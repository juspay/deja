//! The failures the in-band design deletes, turned into tests the design
//! cannot exhibit them.
//!
//! The first ambient shape of this handoff (a process-global 64-entry deque
//! keyed on statement text) lost a recorded request's capture to the 64th
//! competing statement — measured in PR #51, fatal on any loaded pod. The
//! second (a per-checkout slot handle in a tokio task-local) had nothing to
//! evict but could silently be out of scope. In-band there is no window
//! between capture and take at all: the take happens inside the same
//! `conn.run` closure that executed the statement, so competing traffic has
//! nowhere to intrude and no scope has to be active.
//!
//! The second test pins the one residual hazard the wrapper must own: a
//! pooled connection lives across requests, and a statement whose caller
//! never takes (a plain combinator) leaves rows in the connection's slot.
//! The wrapper empties the slot at the start of every load, so the next
//! captured query on that same connection pairs with ITS OWN rows, never a
//! predecessor's.
//!
//! Ignored by default; needs a scratch database:
//!
//! ```text
//! DEJA_DIESEL_TEST_DATABASE_URL=postgres://postgres:probe@127.0.0.1:5599/probe \
//!     cargo test -p deja-diesel --test no_eviction -- --ignored --nocapture
//! ```

use deja_diesel::{get_result_captured, DejaLoadConnection};
use deja_runtime::{RecordingHook, RuntimeHook};
use diesel::pg::PgConnection;
use diesel::sql_types::Text;
use diesel::QueryableByName;
use std::sync::Arc;

type DejaPgConnection = DejaLoadConnection<PgConnection>;
type DejaPool = bb8::Pool<async_bb8_diesel::ConnectionManager<DejaPgConnection>>;

/// An order of magnitude past the old registry's bound (lost at 64).
const COMPETING_STATEMENTS: usize = 70;

#[derive(QueryableByName, Debug)]
struct Row {
    #[diesel(sql_type = Text)]
    label: String,
}

fn open_gate() {
    let dir = tempfile::tempdir().expect("tempdir");
    let hook = RecordingHook::new(dir.path()).expect("recording hook");
    let _ = deja_runtime::set_global_runtime_hook(Some(RuntimeHook::Recording(Arc::new(hook))));
    std::mem::forget(dir);
    assert!(
        !deja_runtime::runtime_mode().is_disabled(),
        "the capture gate must be open, or this test proves nothing"
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

fn label_bytes(rows: &[deja_runtime::wire_capture::WireRow]) -> Option<Vec<u8>> {
    rows.first()?
        .columns
        .iter()
        .find(|column| column.name == "label")?
        .bytes
        .clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a reachable postgres; set DEJA_DIESEL_TEST_DATABASE_URL"]
async fn competing_traffic_cannot_touch_anyones_pair() {
    let url = std::env::var("DEJA_DIESEL_TEST_DATABASE_URL")
        .expect("DEJA_DIESEL_TEST_DATABASE_URL must point at a scratch database");
    open_gate();

    let pool = pool(url, 4).await;

    // Every request is simultaneously the "recorded" one and everyone else's
    // competing traffic — an order of magnitude more than the load that used
    // to evict every capture. Each asserts its pair carries its OWN label.
    let mut handles = Vec::new();
    for index in 0..COMPETING_STATEMENTS {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            let conn = pool.get().await.expect("checkout");
            let label = format!("request_{index}");
            let query = diesel::sql_query(format!("SELECT '{label}'::varchar AS label"));
            let (result, wire) = get_result_captured::<PgConnection, _, Row>(&conn, query).await;
            assert_eq!(result.expect("query").label, label);
            let wire = wire.expect("every pair carries its capture");
            assert_eq!(
                label_bytes(&wire).as_deref(),
                Some(label.as_bytes()),
                "there is no shared capacity, and no window between capture \
                 and take for anything to intrude on"
            );
        }));
    }
    for handle in handles {
        handle.await.expect("join");
    }
    println!("{COMPETING_STATEMENTS} concurrent captured statements: every pair its own");
}

/// A pooled connection outlives the request that leased it. A statement whose
/// caller never takes — a plain async-bb8-diesel combinator, exactly what the
/// write paths still use — leaves its capture in the connection's slot. The
/// wrapper empties the slot at the start of every load, so the NEXT captured
/// query on the same connection can only pair with its own rows; and if that
/// next query produces nothing to capture, the taker gets nothing, never the
/// predecessor's rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a reachable postgres; set DEJA_DIESEL_TEST_DATABASE_URL"]
async fn a_previous_statements_untaken_capture_never_leaks_into_the_next_pair() {
    let url = std::env::var("DEJA_DIESEL_TEST_DATABASE_URL")
        .expect("DEJA_DIESEL_TEST_DATABASE_URL must point at a scratch database");
    open_gate();

    // One connection, so every statement below is guaranteed to share a slot.
    let pool = pool(url, 1).await;
    let conn = pool.get().await.expect("checkout");

    // Statement 1: a captured load whose caller does NOT take — the plain
    // combinator path. Its rows stay in the connection's slot.
    use async_bb8_diesel::AsyncRunQueryDsl;
    let untaken: Row = diesel::sql_query("SELECT 'untaken'::varchar AS label")
        .get_result_async(&*conn)
        .await
        .expect("plain combinator");
    assert_eq!(untaken.label, "untaken");

    // Statement 2, same connection, through the captured helper: the pair
    // must carry statement 2's rows.
    let query = diesel::sql_query("SELECT 'mine'::varchar AS label");
    let (result, wire) = get_result_captured::<PgConnection, _, Row>(&conn, query).await;
    assert_eq!(result.expect("query").label, "mine");
    assert_eq!(
        label_bytes(&wire.expect("captured")).as_deref(),
        Some(b"mine".as_ref()),
        "the load must have displaced the untaken capture before executing"
    );

    // Statement 3 leaves another untaken capture; statement 4 returns zero
    // rows — its pair must carry NOTHING, not statement 3's leftovers.
    let _: Row = diesel::sql_query("SELECT 'leftover'::varchar AS label")
        .get_result_async(&*conn)
        .await
        .expect("plain combinator");
    let query = diesel::sql_query("SELECT 'never'::varchar AS label WHERE false");
    let (result, wire) = get_result_captured::<PgConnection, _, Row>(&conn, query).await;
    assert!(matches!(result, Err(diesel::result::Error::NotFound)));
    assert!(
        wire.is_none(),
        "an empty result must never inherit an earlier statement's rows"
    );
}
