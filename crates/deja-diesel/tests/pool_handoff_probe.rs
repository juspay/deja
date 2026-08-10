//! Does the in-band pairing survive the execution stack hyperswitch actually
//! uses? Production drives diesel through `async-bb8-diesel` over a `bb8`
//! pool: the query runs inside `conn.run(|c| …)` on a `spawn_blocking`
//! thread. The captured helpers take the wire rows in that SAME closure and
//! return them paired with the result — these tests pin that the pair
//! arrives intact on the async side, through the real pool, and that each
//! concurrent request's pair carries its own rows by construction.
//!
//! Two ambient predecessors of this handoff passed their tests and failed in
//! production (a keyed global queue: evicted under load; a per-checkout slot
//! handle in a task-local scope: zero captures in the deployed router). The
//! in-band pair has no ambient half to go wrong, and the concurrency test
//! below asserts the pairing on CONTENT — each pair's wire bytes must decode
//! to the very result they rode with — not merely on presence.
//!
//! Ignored by default; needs a scratch database:
//!
//! ```text
//! DEJA_DIESEL_TEST_DATABASE_URL=postgres://postgres:probe@127.0.0.1:5599/probe \
//!     cargo test -p deja-diesel --test pool_handoff_probe -- --ignored --nocapture
//! ```

use deja_diesel::{get_result_captured, get_results_captured, DejaLoadConnection};
use deja_runtime::{RecordingHook, RuntimeHook};
use diesel::pg::PgConnection;
use diesel::sql_types::{BigInt, Text};
use diesel::QueryableByName;
use std::sync::Arc;

type DejaPgConnection = DejaLoadConnection<PgConnection>;
type DejaPool = bb8::Pool<async_bb8_diesel::ConnectionManager<DejaPgConnection>>;

#[derive(QueryableByName, Debug)]
struct Row {
    #[diesel(sql_type = Text)]
    label: String,
    #[diesel(sql_type = BigInt)]
    amount: i64,
}

#[derive(QueryableByName, Debug)]
struct PidRow {
    #[diesel(sql_type = BigInt)]
    pid: i64,
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

async fn pool(url: String, size: u32) -> DejaPool {
    let manager = async_bb8_diesel::ConnectionManager::<DejaPgConnection>::new(url);
    bb8::Pool::builder()
        .max_size(size)
        .build(manager)
        .await
        .expect("pool")
}

fn wire_bytes<'a>(
    rows: &'a [deja_runtime::wire_capture::WireRow],
    column: &str,
) -> Option<&'a [u8]> {
    rows.first()?
        .columns
        .iter()
        .find(|c| c.name == column)?
        .bytes
        .as_deref()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a reachable postgres; set DEJA_DIESEL_TEST_DATABASE_URL"]
async fn the_pair_survives_the_pool_and_the_blocking_thread() {
    let url = std::env::var("DEJA_DIESEL_TEST_DATABASE_URL")
        .expect("DEJA_DIESEL_TEST_DATABASE_URL must point at a scratch database");
    open_gate();

    // The production stack: bb8 pool of async-bb8-diesel connections whose
    // inner diesel connection is the deja wrapper. Nothing is installed at
    // checkout; the helper pairs inside the run closure.
    let pool = pool(url, 2).await;
    let conn = pool.get().await.expect("checkout");

    let query = diesel::sql_query("SELECT 'poison'::varchar AS label, 42::int8 AS amount");
    let (result, wire) = get_result_captured::<PgConnection, _, Row>(&conn, query).await;
    let row = result.expect("get_result");
    assert_eq!(row.label, "poison");
    assert_eq!(row.amount, 42);

    let wire = wire.expect(
        "the pair must carry the capture across the pool and the blocking \
         thread — this is the crossing production exercises",
    );
    println!(
        "PROBE PAIR OK: {} row(s), {} column(s)",
        wire.len(),
        wire[0].columns.len()
    );
    assert_eq!(wire_bytes(&wire, "label"), Some(b"poison".as_ref()));
    assert_eq!(
        wire_bytes(&wire, "amount"),
        Some(42i64.to_be_bytes().as_ref()),
        "int8 arrives as 8-byte big-endian binary — the seeding representation"
    );
}

/// The collision the old query-text join admitted by design, and the pairing
/// the task-local design could only promise: the same statement,
/// byte-identical, executed concurrently by four requests. The statement
/// returns per-connection state (`pg_backend_pid()`), so each pair's wire
/// bytes must decode to the SAME pid its own serde result carries — pairing
/// asserted on content, not presence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a reachable postgres; set DEJA_DIESEL_TEST_DATABASE_URL"]
async fn concurrent_identical_statements_each_pair_with_their_own_rows() {
    let url = std::env::var("DEJA_DIESEL_TEST_DATABASE_URL")
        .expect("DEJA_DIESEL_TEST_DATABASE_URL must point at a scratch database");
    open_gate();

    let pool = pool(url, 4).await;

    let mut handles = Vec::new();
    for request in 0..4 {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            let conn = pool.get().await.expect("checkout");
            // Byte-identical across all four requests, binds included.
            let query = diesel::sql_query("SELECT pg_backend_pid()::int8 AS pid");
            let (result, wire) = get_result_captured::<PgConnection, _, PidRow>(&conn, query).await;
            let row = result.expect("query");
            let wire = wire.expect("each request's pair carries a capture");
            let captured_pid = wire_bytes(&wire, "pid").expect("pid bytes captured");
            println!(
                "PROBE request {request}: pid {} / wire {:?}",
                row.pid, captured_pid
            );
            assert_eq!(
                captured_pid,
                row.pid.to_be_bytes().as_ref(),
                "the wire rows must belong to the very result they rode with"
            );
        }));
    }
    for handle in handles {
        handle.await.expect("join");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a reachable postgres; set DEJA_DIESEL_TEST_DATABASE_URL"]
async fn zero_rows_yield_no_wire() {
    let url = std::env::var("DEJA_DIESEL_TEST_DATABASE_URL")
        .expect("DEJA_DIESEL_TEST_DATABASE_URL must point at a scratch database");
    open_gate();

    let pool = pool(url, 1).await;
    let conn = pool.get().await.expect("checkout");

    let query = diesel::sql_query("SELECT 'x'::varchar AS label, 1::int8 AS amount WHERE false");
    let (result, wire) = get_results_captured::<PgConnection, _, Row>(&conn, query).await;
    assert_eq!(result.expect("empty result set").len(), 0);
    assert!(
        wire.is_none(),
        "no rows, no physical image — the boundary keeps the semantic path"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a reachable postgres; set DEJA_DIESEL_TEST_DATABASE_URL"]
async fn an_err_result_returns_the_pair_without_panicking() {
    let url = std::env::var("DEJA_DIESEL_TEST_DATABASE_URL")
        .expect("DEJA_DIESEL_TEST_DATABASE_URL must point at a scratch database");
    open_gate();

    let pool = pool(url, 1).await;
    let conn = pool.get().await.expect("checkout");

    // NotFound: get_result over an empty result set.
    let query = diesel::sql_query("SELECT 'x'::varchar AS label, 1::int8 AS amount WHERE false");
    let (result, wire) = get_result_captured::<PgConnection, _, Row>(&conn, query).await;
    assert!(matches!(result, Err(diesel::result::Error::NotFound)));
    assert!(wire.is_none());

    // A statement postgres rejects outright: the load fails before any cursor
    // exists; the pair still comes back, Err plus nothing.
    let query = diesel::sql_query("SELECT no_such_column FROM no_such_table");
    let (result, wire) = get_result_captured::<PgConnection, _, Row>(&conn, query).await;
    assert!(result.is_err());
    assert!(wire.is_none());

    // And the connection is still usable for the next captured query.
    let query = diesel::sql_query("SELECT 'after'::varchar AS label, 7::int8 AS amount");
    let (result, wire) = get_result_captured::<PgConnection, _, Row>(&conn, query).await;
    assert_eq!(result.expect("recovered").label, "after");
    assert_eq!(
        wire_bytes(&wire.expect("captured"), "label"),
        Some(b"after".as_ref())
    );
}
