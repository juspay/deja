//! Probe: for a TYPED diesel query (what hyperswitch actually issues), does the
//! string the boundary renders equal the string the wrapper publishes under?
//!
//! The boundary renders `debug_query(&query)` on the query as the caller built
//! it. The wrapper renders `debug_query(&source)` on whatever diesel hands to
//! `LoadConnection::load` — and diesel's blanket `LoadQuery` impl loads
//! `self.as_query()`, not `self`. If `as_query()` elaborates the statement (for
//! example expanding the select list), the two renderings differ and the join
//! misses 100% of the time for every typed query, while raw `sql_query` (whose
//! `as_query` is an identity) joins fine — which is exactly the pattern the
//! sandbox showed: zero physical images from a real recorder, yet a passing
//! bare-connection test.
//!
//! ```text
//! DEJA_DIESEL_TEST_DATABASE_URL=postgres://postgres:probe@127.0.0.1:5599/probe \
//!     cargo test -p deja-diesel --test typed_query_key_probe -- --ignored --nocapture
//! ```

use deja_diesel::DejaLoadConnection;
use deja_runtime::{wire_capture, RecordingHook, RuntimeHook};
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
fn the_boundarys_sql_must_equal_the_wrappers_published_key_for_a_typed_query() {
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
    println!("SAME? {}", boundary_sql == as_query_sql);

    let row: ProbeAttempt = query.get_result(&mut conn).expect("get_result");
    assert_eq!(row.attempt_id, "a_1");
    assert_eq!(row.payment_id, "pay_1");
    assert_eq!(row.amount, 6000);

    println!("pending_captures = {}", wire_capture::pending_captures());

    // The boundary's take, verbatim.
    let by_boundary_key = wire_capture::take_captured_wire_rows(&boundary_sql);
    println!(
        "take with BOUNDARY sql -> {}",
        if by_boundary_key.is_some() {
            "HIT"
        } else {
            "MISS"
        }
    );

    // If that missed, does the as_query rendering hit? That identifies the
    // mismatch precisely rather than leaving it as "something differs".
    if by_boundary_key.is_none() {
        let by_as_query = wire_capture::take_captured_wire_rows(&as_query_sql);
        println!(
            "take with as_query sql -> {}",
            if by_as_query.is_some() { "HIT" } else { "MISS" }
        );
    }

    assert!(
        by_boundary_key.is_some(),
        "the boundary must be able to take the capture for a TYPED query — this \
         is what every hyperswitch db read is"
    );
}
