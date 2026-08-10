//! The failure this design deletes, turned into a test that the design cannot
//! exhibit it.
//!
//! This replaces `registry_eviction_probe.rs` (PR #51), which measured the
//! defect: the capture handoff used to be a process-global 64-entry deque with
//! oldest-first eviction, while the capture gate was process-level — so every
//! statement the process ran published, including the majority from requests
//! the sampler excluded, which never take. A recorded request's image survived
//! 63 competing statements in its window and was lost at 64, which on a pod
//! serving live traffic is every recorded request.
//!
//! There is nothing to evict any more: the capture lives in a slot on the
//! connection that produced it. The test below runs an order of magnitude more
//! competing traffic than the old bound, on other connections, in other tasks,
//! while a recorded request's capture waits — and the request still takes its
//! own rows.
//!
//! Ignored by default; needs a scratch database:
//!
//! ```text
//! DEJA_DIESEL_TEST_DATABASE_URL=postgres://postgres:probe@127.0.0.1:5599/probe \
//!     cargo test -p deja-diesel --test no_eviction -- --ignored --nocapture
//! ```

use deja_diesel::DejaLoadConnection;
use deja_runtime::wire_capture::{self, WireSlot};
use deja_runtime::{RecordingHook, RuntimeHook};
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::sql_types::Text;
use std::sync::Arc;

type DejaPgConnection = DejaLoadConnection<PgConnection>;
type DejaPool = bb8::Pool<async_bb8_diesel::ConnectionManager<DejaPgConnection>>;
type PooledDejaConnection =
    bb8::PooledConnection<'static, async_bb8_diesel::ConnectionManager<DejaPgConnection>>;

/// How much competing traffic to run while the recorded capture waits. The old
/// registry lost the capture at 64.
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

/// The vendor's checkout hook in shape: one slot, installed on the leased
/// connection and registered as the current slot for this task.
async fn checkout_and_install(pool: &DejaPool) -> PooledDejaConnection {
    use async_bb8_diesel::AsyncConnection;
    let conn = pool.get_owned().await.expect("checkout");
    let slot = WireSlot::new_shared();
    let for_connection = Arc::clone(&slot);
    conn.run(move |c| {
        c.install_wire_slot(for_connection);
        Ok::<(), diesel::result::Error>(())
    })
    .await
    .expect("install the slot");
    wire_capture::set_current_slot(slot);
    conn
}

async fn read_one(conn: &PooledDejaConnection, label: &str) {
    use async_bb8_diesel::AsyncRunQueryDsl;
    let query = diesel::sql_query(format!("SELECT '{label}'::varchar AS label"));
    let row: Row = query.get_result_async(&**conn).await.expect("query");
    assert_eq!(row.label, label);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a reachable postgres; set DEJA_DIESEL_TEST_DATABASE_URL"]
async fn competing_traffic_on_other_connections_cannot_displace_this_capture() {
    let url = std::env::var("DEJA_DIESEL_TEST_DATABASE_URL")
        .expect("DEJA_DIESEL_TEST_DATABASE_URL must point at a scratch database");
    open_gate();

    let manager = async_bb8_diesel::ConnectionManager::<DejaPgConnection>::new(url);
    let pool: DejaPool = bb8::Pool::builder()
        .max_size(4)
        .build(manager)
        .await
        .expect("pool");

    let before = wire_capture::counts();

    wire_capture::scope(async {
        // The recorded request reads, and its capture waits in its own slot
        // while the boundary's result producer has not run yet.
        let conn = checkout_and_install(&pool).await;
        read_one(&conn, "recorded").await;

        // Competing traffic: other requests, on other pooled connections, in
        // other tasks, each capturing and none of them taking — the shape that
        // used to flood the shared queue.
        let mut handles = Vec::new();
        for _ in 0..COMPETING_STATEMENTS {
            let pool = pool.clone();
            handles.push(tokio::spawn(wire_capture::scope(async move {
                let other = checkout_and_install(&pool).await;
                read_one(&other, "ambient").await;
                // Deliberately no take: an unclaimed capture.
            })));
        }
        for handle in handles {
            handle.await.expect("join");
        }

        let captured = wire_capture::take_current_rows();
        println!(
            "after {COMPETING_STATEMENTS} competing statements: recorded capture {}",
            if captured.is_some() {
                "SURVIVED"
            } else {
                "LOST"
            }
        );
        let captured = captured.expect(
            "a capture on this connection cannot be displaced by traffic on \
             other connections — there is no shared queue to overflow",
        );
        assert_eq!(captured.len(), 1);
        assert_eq!(
            captured[0]
                .columns
                .iter()
                .find(|column| column.name == "label")
                .and_then(|column| column.bytes.as_deref()),
            Some(b"recorded".as_ref()),
            "and the rows taken are this request's own, not a neighbour's"
        );
    })
    .await;

    // Diagnostics only — the harness may run other tests in this binary at the
    // same time, so these deltas are not exclusively ours. The claim above is
    // local to the recorded request's own slot, which is what makes it sound.
    let after = wire_capture::counts();
    println!(
        "counts delta: put={} taken={} overwritten_unclaimed={} takes_without_slot={}",
        after.put - before.put,
        after.taken - before.taken,
        after.overwritten_unclaimed - before.overwritten_unclaimed,
        after.takes_without_slot - before.takes_without_slot,
    );
}

/// The adoption requirement, pinned. A pooled connection outlives the request
/// that leased it, so a checkout that does NOT install a slot leaves the
/// connection pointing at the previous request's slot, and its statement lands
/// there. Nobody takes it here, so nothing is misattributed — but if the earlier
/// request were still alive and took afterwards, it would receive rows from a
/// statement it never issued. Hence: every checkout site installs, which is what
/// makes the previous slot unreachable before the next statement runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a reachable postgres; set DEJA_DIESEL_TEST_DATABASE_URL"]
async fn a_checkout_that_skips_the_install_inherits_the_previous_slot() {
    let url = std::env::var("DEJA_DIESEL_TEST_DATABASE_URL")
        .expect("DEJA_DIESEL_TEST_DATABASE_URL must point at a scratch database");
    open_gate();

    // One connection, so the second checkout is guaranteed to be the same one.
    let manager = async_bb8_diesel::ConnectionManager::<DejaPgConnection>::new(url);
    let pool: DejaPool = bb8::Pool::builder()
        .max_size(1)
        .build(manager)
        .await
        .expect("pool");

    let slot = WireSlot::new_shared();
    wire_capture::scope(async {
        use async_bb8_diesel::AsyncConnection;
        let conn = pool.get_owned().await.expect("checkout");
        let for_connection = Arc::clone(&slot);
        conn.run(move |c| {
            c.install_wire_slot(for_connection);
            Ok::<(), diesel::result::Error>(())
        })
        .await
        .expect("install the slot");
        read_one(&conn, "first_request").await;
    })
    .await;

    // A second lease of the same connection, with no install: its statement has
    // nowhere of its own to go.
    wire_capture::scope(async {
        let conn = pool.get_owned().await.expect("re-checkout");
        read_one(&conn, "second_request").await;
        assert!(
            wire_capture::take_current_rows().is_none(),
            "this request installed no slot, so it takes nothing"
        );
    })
    .await;

    let inherited = slot
        .take()
        .expect("the first request's slot holds a capture");
    assert_eq!(
        inherited[0]
            .columns
            .iter()
            .find(|column| column.name == "label")
            .and_then(|column| column.bytes.as_deref()),
        Some(b"second_request".as_ref()),
        "an uninstalled checkout publishes into the previous request's slot — \
         which is why every checkout site must install one"
    );
}
