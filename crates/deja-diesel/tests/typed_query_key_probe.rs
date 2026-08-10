//! A TYPED diesel query (what hyperswitch actually issues) must hand its
//! capture to the boundary. This test began as a probe of the old join key: the
//! boundary rendered `debug_query(&query)` on the query as the caller built it,
//! while the wrapper rendered whatever diesel handed to `LoadConnection::load`
//! — and diesel's blanket `LoadQuery` impl loads `self.as_query()`, not `self`.
//! If `as_query()` elaborated the statement, every typed query missed while raw
//! `sql_query` (whose `as_query` is the identity) joined fine.
//!
//! The two renderings turned out to agree, and with the capture living in a slot
//! on the connection they no longer have to: the handoff involves no statement
//! text at all. The renderings are still printed, because that agreement is the
//! only reason the old design worked in the test that passed while production
//! recorded nothing.
//!
//! ```text
//! DEJA_DIESEL_TEST_DATABASE_URL=postgres://postgres:probe@127.0.0.1:5599/probe \
//!     cargo test -p deja-diesel --test typed_query_key_probe -- --ignored --nocapture
//! ```

use deja_diesel::DejaLoadConnection;
use deja_runtime::wire_capture::{self, WireSlot};
use deja_runtime::{RecordingHook, RuntimeHook};
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::query_builder::AsQuery;
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

#[test]
#[ignore = "needs a reachable postgres; set DEJA_DIESEL_TEST_DATABASE_URL"]
fn a_typed_query_hands_its_capture_to_the_boundary() {
    let url =
        std::env::var("DEJA_DIESEL_TEST_DATABASE_URL").expect("DEJA_DIESEL_TEST_DATABASE_URL");
    open_gate();
    let mut conn = DejaLoadConnection::<PgConnection>::establish(&url).expect("establish");

    diesel::sql_query("DROP TABLE IF EXISTS probe_attempt")
        .execute(&mut conn)
        .expect("drop");
    diesel::sql_query(
        "CREATE TABLE probe_attempt (attempt_id text primary key, payment_id text not null, amount int8 not null)",
    )
    .execute(&mut conn)
    .expect("create");
    diesel::sql_query("INSERT INTO probe_attempt VALUES ('a_1','pay_1',6000)")
        .execute(&mut conn)
        .expect("insert");

    // What a host does at checkout.
    let slot = WireSlot::new_shared();
    conn.install_wire_slot(Arc::clone(&slot));

    // Exactly what generic_find_one_core does: build a filtered query, render
    // its sql, then get_result.
    let query = probe_attempt::table.filter(probe_attempt::payment_id.eq("pay_1"));
    let boundary_sql = diesel::debug_query::<diesel::pg::Pg, _>(&query).to_string();
    // What diesel actually hands to load():
    let as_query_sql = diesel::debug_query::<diesel::pg::Pg, _>(
        &probe_attempt::table
            .filter(probe_attempt::payment_id.eq("pay_1"))
            .as_query(),
    )
    .to_string();

    println!("BOUNDARY renders : {boundary_sql}");
    println!("as_query renders : {as_query_sql}");
    println!(
        "SAME? {} (no longer load-bearing)",
        boundary_sql == as_query_sql
    );

    let captured = wire_capture::scope_sync(|| {
        assert!(wire_capture::set_current_slot(Arc::clone(&slot)));
        let row: ProbeAttempt = query.get_result(&mut conn).expect("get_result");
        assert_eq!(row.attempt_id, "a_1");
        assert_eq!(row.payment_id, "pay_1");
        assert_eq!(row.amount, 6000);
        wire_capture::take_current_rows()
    });

    println!(
        "take through the task's slot -> {}",
        if captured.is_some() { "HIT" } else { "MISS" }
    );
    let captured = captured.expect(
        "the boundary must be able to take the capture for a TYPED query — this \
         is what every hyperswitch db read is",
    );
    assert_eq!(captured.len(), 1);
    let names: Vec<&str> = captured[0]
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    assert_eq!(names, vec!["attempt_id", "payment_id", "amount"]);
    assert_eq!(
        captured[0].columns[2].bytes.as_deref(),
        Some(6000i64.to_be_bytes().as_ref()),
        "int8 arrives as 8-byte big-endian binary — the seeding representation"
    );
}
