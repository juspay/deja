//! Probe: does the wire-capture handoff survive the execution stack hyperswitch
//! actually uses? The existing round-trip test drives a BARE
//! `DejaLoadConnection` and proves the wrapper captures. Production instead
//! goes through `async-bb8-diesel` over a `bb8` pool
//! (`query.get_result_async(conn)` → `asc.run(|c| self.get_result(c))` on a
//! `spawn_blocking` thread), and the boundary takes the capture LATER, on the
//! async task. That crossing is what a real recording exercises and what no
//! test covered — a run against the deployed recorder produced ZERO physical
//! row images while the serde path worked normally.
//!
//! Ignored by default; needs a scratch database:
//!
//! ```text
//! DEJA_DIESEL_TEST_DATABASE_URL=postgres://postgres:probe@127.0.0.1:5599/probe \
//!     cargo test -p deja-diesel --test pool_handoff_probe -- --ignored --nocapture
//! ```

use deja_diesel::DejaLoadConnection;
use deja_runtime::{wire_capture, RecordingHook, RuntimeHook};
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Text};
use std::sync::Arc;

type DejaPgConnection = DejaLoadConnection<PgConnection>;

#[derive(QueryableByName, Debug)]
struct Row {
    #[diesel(sql_type = Text)]
    label: String,
    #[diesel(sql_type = BigInt)]
    amount: i64,
}

fn open_gate() {
    let dir = tempfile::tempdir().expect("tempdir");
    let hook = RecordingHook::new(dir.path()).expect("recording hook");
    let _ = deja_runtime::set_global_runtime_hook(Some(RuntimeHook::Recording(Arc::new(hook))));
    // Leak so the artifact dir outlives the hook that writes into it.
    std::mem::forget(dir);
    assert!(
        !deja_runtime::runtime_mode().is_disabled(),
        "the capture gate must be open, or this probe proves nothing"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a reachable postgres; set DEJA_DIESEL_TEST_DATABASE_URL"]
async fn the_handoff_survives_the_pool_and_the_blocking_thread() {
    let url = std::env::var("DEJA_DIESEL_TEST_DATABASE_URL")
        .expect("DEJA_DIESEL_TEST_DATABASE_URL must point at a scratch database");
    open_gate();

    // The production stack: bb8 pool of async-bb8-diesel connections whose
    // inner diesel connection is the deja wrapper.
    let manager = async_bb8_diesel::ConnectionManager::<DejaPgConnection>::new(url);
    let pool = bb8::Pool::builder()
        .max_size(2)
        .build(manager)
        .await
        .expect("pool");
    let conn = pool.get_owned().await.expect("checkout");

    // Mirror `generic_find_one_core`: render the sql, then execute through
    // `get_result_async`, then take — the same order and the same strings.
    let query = diesel::sql_query("SELECT 'poison'::varchar AS label, 42::int8 AS amount");
    let sql = diesel::debug_query::<diesel::pg::Pg, _>(&query).to_string();
    println!("PROBE sql = {sql}");

    use async_bb8_diesel::AsyncRunQueryDsl;
    let row: Row = query
        .get_result_async(&*conn)
        .await
        .expect("get_result_async");
    assert_eq!(row.label, "poison");
    assert_eq!(row.amount, 42);

    println!(
        "PROBE pending_captures after query = {}",
        wire_capture::pending_captures()
    );
    let captured = wire_capture::take_captured_wire_rows(&sql);
    match &captured {
        Some(rows) => println!(
            "PROBE TAKE OK: {} row(s), {} column(s)",
            rows.len(),
            rows[0].columns.len()
        ),
        None => println!("PROBE TAKE FAILED: nothing matched this sql key"),
    }
    assert!(
        captured.is_some(),
        "the pool path must publish a capture the boundary can take — this is \
         the crossing production exercises"
    );
}

/// Second probe: the same query executed twice concurrently on two pooled
/// connections. Byte-identical sql means one registry key, so this is the
/// collision the query-text join admits by design.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a reachable postgres; set DEJA_DIESEL_TEST_DATABASE_URL"]
async fn concurrent_identical_statements_share_one_key() {
    let url = std::env::var("DEJA_DIESEL_TEST_DATABASE_URL")
        .expect("DEJA_DIESEL_TEST_DATABASE_URL must point at a scratch database");
    open_gate();

    let manager = async_bb8_diesel::ConnectionManager::<DejaPgConnection>::new(url);
    let pool = bb8::Pool::builder()
        .max_size(4)
        .build(manager)
        .await
        .expect("pool");

    use async_bb8_diesel::AsyncRunQueryDsl;
    let mut handles = Vec::new();
    for _ in 0..4 {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            let conn = pool.get_owned().await.expect("checkout");
            let q = diesel::sql_query("SELECT 'poison'::varchar AS label, 42::int8 AS amount");
            let _: Row = q.get_result_async(&*conn).await.expect("query");
        }));
    }
    for h in handles {
        h.await.expect("join");
    }
    println!(
        "PROBE pending_captures after 4 concurrent identical queries = {}",
        wire_capture::pending_captures()
    );
}
