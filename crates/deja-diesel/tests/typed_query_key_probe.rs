//! A TYPED diesel query (what hyperswitch actually issues) must come back
//! paired with its own wire rows. This test began life probing the old
//! statement-text join key (whether `debug_query` on the caller's query and on
//! diesel's `as_query()` elaboration agreed — they happened to, which is the
//! only reason that design ever passed a test). No statement text takes part
//! in the pairing any more: the helper takes the capture from the query's own
//! connection in the same closure that ran it, for exactly the query shapes
//! the vendor's generic helpers build (a filtered typed query via
//! `get_result_captured`, a primary-key find via `first_captured` — the
//! `LIMIT 1` the old join key had to normalize away).
//!
//! ```text
//! DEJA_DIESEL_TEST_DATABASE_URL=postgres://postgres:probe@127.0.0.1:5599/probe \
//!     cargo test -p deja-diesel --test typed_query_key_probe -- --ignored --nocapture
//! ```

use deja_diesel::{first_captured, get_result_captured, DejaLoadConnection};
use deja_runtime::{RecordingHook, RuntimeHook};
use diesel::pg::PgConnection;
use diesel::prelude::*;
use std::sync::Arc;

diesel::table! {
    probe_attempt (attempt_id) {
        attempt_id -> Text,
        payment_id -> Text,
        amount -> BigInt,
    }
}

#[derive(Queryable, QueryableByName, Debug)]
#[diesel(table_name = probe_attempt)]
struct ProbeAttempt {
    attempt_id: String,
    payment_id: String,
    amount: i64,
}

fn open_gate() {
    let dir = tempfile::tempdir().expect("tempdir");
    let hook = RecordingHook::new(dir.path()).expect("recording hook");
    let _ = deja_runtime::set_global_runtime_hook(Some(RuntimeHook::Recording(Arc::new(hook))));
    std::mem::forget(dir);
    assert!(
        !deja_runtime::runtime_mode().is_disabled(),
        "gate must be open"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a reachable postgres; set DEJA_DIESEL_TEST_DATABASE_URL"]
async fn a_typed_query_comes_back_paired_with_its_capture() {
    let url =
        std::env::var("DEJA_DIESEL_TEST_DATABASE_URL").expect("DEJA_DIESEL_TEST_DATABASE_URL");
    open_gate();

    {
        let mut setup =
            DejaLoadConnection::<PgConnection>::establish(&url).expect("establish for setup");
        diesel::sql_query("DROP TABLE IF EXISTS probe_attempt")
            .execute(&mut setup)
            .expect("drop");
        diesel::sql_query(
            "CREATE TABLE probe_attempt (attempt_id text primary key, payment_id text not null, amount int8 not null)",
        )
        .execute(&mut setup)
        .expect("create");
        diesel::sql_query("INSERT INTO probe_attempt VALUES ('a_1','pay_1',6000)")
            .execute(&mut setup)
            .expect("insert");
    }

    // The async connection production wraps in a pool; the helper needs
    // nothing installed on it.
    let conn = async_bb8_diesel::Connection::new(
        DejaLoadConnection::<PgConnection>::establish(&url).expect("establish"),
    );

    // Exactly what generic_find_one_core builds: a filtered typed query.
    let query = probe_attempt::table.filter(probe_attempt::payment_id.eq("pay_1"));
    let (result, wire) = get_result_captured::<PgConnection, _, ProbeAttempt>(&conn, query).await;
    let row = result.expect("get_result");
    assert_eq!(row.attempt_id, "a_1");
    assert_eq!(row.payment_id, "pay_1");
    assert_eq!(row.amount, 6000);

    let wire = wire.expect(
        "a TYPED query must come back paired with its capture — this is what \
         every hyperswitch db read is",
    );
    assert_eq!(wire.len(), 1);
    let names: Vec<&str> = wire[0]
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    assert_eq!(names, vec!["attempt_id", "payment_id", "amount"]);
    assert_eq!(
        wire[0].columns[2].bytes.as_deref(),
        Some(6000i64.to_be_bytes().as_ref()),
        "int8 arrives as 8-byte big-endian binary — the seeding representation"
    );

    // And the find-by-primary-key shape (`first_async` in the vendor, which
    // appends the LIMIT 1 the old join key had to normalize away).
    let query = probe_attempt::table.find("a_1");
    let (result, wire) = first_captured::<PgConnection, _, ProbeAttempt>(&conn, query).await;
    assert_eq!(result.expect("first").amount, 6000);
    let wire = wire.expect("the LIMIT 1 shape pairs like any other");
    assert_eq!(wire.len(), 1);
    assert_eq!(
        wire[0].columns[0].bytes.as_deref(),
        Some(b"a_1".as_ref()),
        "the pk find's own row rides its own pair"
    );
}
