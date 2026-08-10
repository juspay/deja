//! Does the wire-capture handoff survive the execution stack hyperswitch
//! actually uses? The round-trip test drives a BARE `DejaLoadConnection`.
//! Production instead goes through `async-bb8-diesel` over a `bb8` pool
//! (`query.get_result_async(conn)` → `asc.run(|c| self.get_result(c))` on a
//! `spawn_blocking` thread), and the boundary takes the capture LATER, on the
//! async task. That crossing is what a real recording exercises, and a run
//! against the deployed recorder produced ZERO physical row images while the
//! serde path worked normally.
//!
//! With the capture slot living on the connection, the crossing is exactly
//! what these tests must pin: the cursor puts through an `Arc` on a blocking
//! thread, the boundary takes through the async task's current slot, and the
//! two are the same slot because the checkout installed both halves. The
//! checkout sequence below is the one the vendor hook performs.
//!
//! Ignored by default; needs a scratch database:
//!
//! ```text
//! DEJA_DIESEL_TEST_DATABASE_URL=postgres://postgres:probe@127.0.0.1:5599/probe \
//!     cargo test -p deja-diesel --test pool_handoff_probe -- --ignored --nocapture
//! ```

use deja_diesel::DejaLoadConnection;
use deja_runtime::wire_capture::{self, WireSlot};
use deja_runtime::{RecordingHook, RuntimeHook};
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Text};
use std::sync::Arc;

type DejaPgConnection = DejaLoadConnection<PgConnection>;
type PooledDejaConnection =
    bb8::PooledConnection<'static, async_bb8_diesel::ConnectionManager<DejaPgConnection>>;

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

async fn pool(
    url: String,
    size: u32,
) -> bb8::Pool<async_bb8_diesel::ConnectionManager<DejaPgConnection>> {
    let manager = async_bb8_diesel::ConnectionManager::<DejaPgConnection>::new(url);
    bb8::Pool::builder()
        .max_size(size)
        .build(manager)
        .await
        .expect("pool")
}

/// The vendor's checkout hook, verbatim in shape: create one slot, install it
/// on the leased connection inside `conn.run` (the only place `&mut` on the
/// wrapped connection is reachable), and register the same slot as the current
/// one for the async task that will run the boundary.
async fn checkout_and_install(
    pool: &bb8::Pool<async_bb8_diesel::ConnectionManager<DejaPgConnection>>,
) -> PooledDejaConnection {
    use async_bb8_diesel::AsyncConnection;
    let conn = pool.get_owned().await.expect("checkout");
    let slot = WireSlot::new_shared();
    let for_connection = Arc::clone(&slot);
    conn.run(move |c| {
        c.install_wire_slot(for_connection);
        Ok::<(), diesel::result::Error>(())
    })
    .await
    .expect("install the slot on the leased connection");
    assert!(
        wire_capture::set_current_slot(slot),
        "the request must be inside a wire-capture scope"
    );
    conn
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a reachable postgres; set DEJA_DIESEL_TEST_DATABASE_URL"]
async fn the_handoff_survives_the_pool_and_the_blocking_thread() {
    let url = std::env::var("DEJA_DIESEL_TEST_DATABASE_URL")
        .expect("DEJA_DIESEL_TEST_DATABASE_URL must point at a scratch database");
    open_gate();

    // The production stack: bb8 pool of async-bb8-diesel connections whose
    // inner diesel connection is the deja wrapper.
    let pool = pool(url, 2).await;

    wire_capture::scope(async move {
        let conn = checkout_and_install(&pool).await;

        use async_bb8_diesel::AsyncRunQueryDsl;
        let query = diesel::sql_query("SELECT 'poison'::varchar AS label, 42::int8 AS amount");
        let row: Row = query
            .get_result_async(&*conn)
            .await
            .expect("get_result_async");
        assert_eq!(row.label, "poison");
        assert_eq!(row.amount, 42);

        // The boundary's take, on the async task, after the blocking thread put.
        let captured = wire_capture::take_current_rows();
        match &captured {
            Some(rows) => println!(
                "PROBE TAKE OK: {} row(s), {} column(s)",
                rows.len(),
                rows[0].columns.len()
            ),
            None => println!("PROBE TAKE FAILED: the task's slot was empty"),
        }
        assert!(
            captured.is_some(),
            "the pool path must carry a capture the boundary can take — this is \
             the crossing production exercises"
        );
    })
    .await;
}

/// The collision the old query-text join admitted by design: the same statement,
/// byte-identical, executed concurrently by four requests. Each request now has
/// its own slot on its own connection, so each takes its own rows and no take
/// can reach another request's capture.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a reachable postgres; set DEJA_DIESEL_TEST_DATABASE_URL"]
async fn concurrent_identical_statements_each_take_their_own_capture() {
    let url = std::env::var("DEJA_DIESEL_TEST_DATABASE_URL")
        .expect("DEJA_DIESEL_TEST_DATABASE_URL must point at a scratch database");
    open_gate();

    let pool = pool(url, 4).await;

    let mut handles = Vec::new();
    for request in 0..4 {
        let pool = pool.clone();
        handles.push(tokio::spawn(wire_capture::scope(async move {
            use async_bb8_diesel::AsyncRunQueryDsl;
            let conn = checkout_and_install(&pool).await;
            // Byte-identical across all four requests, binds included.
            let query = diesel::sql_query("SELECT 'poison'::varchar AS label, 42::int8 AS amount");
            let _: Row = query.get_result_async(&*conn).await.expect("query");
            let captured = wire_capture::take_current_rows();
            println!(
                "PROBE request {request}: {}",
                if captured.is_some() { "HIT" } else { "MISS" }
            );
            captured.is_some()
        })));
    }
    for handle in handles {
        assert!(
            handle.await.expect("join"),
            "every concurrent request must take its OWN capture — identical \
             statement text is no longer part of the handoff"
        );
    }
}
