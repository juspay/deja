//! Probe the hypothesis that explains a 100% loss in production while every
//! isolated test passes: the capture registry is PROCESS-GLOBAL, bounded at
//! `MAX_PENDING = 64`, and evicts oldest-first — while the capture gate is
//! process-level, so EVERY query the pod runs publishes, including the
//! overwhelming majority belonging to requests the sampler excluded from
//! recording (those never run a boundary result producer, so nobody takes
//! their capture).
//!
//! Publish happens on a blocking thread; the take happens later, after the
//! await returns on the async task. Any traffic in that window pushes the
//! recorded request's capture out of a 64-slot queue. A local test has no
//! competing traffic; a sandbox pod serving ambient traffic has plenty.
//!
//! No database needed — this is a pure registry-behavior probe.

use deja_runtime::wire_capture::{
    pending_captures, publish_captured_wire_rows, take_captured_wire_rows, WireColumn, WireRow,
};
use std::sync::Mutex;

/// The registry these tests probe is process-global, so the harness running
/// them in parallel would have them evict each other's captures — the very
/// defect under test. Serialize, as `wire_capture`'s own tests do.
static SERIALIZE: Mutex<()> = Mutex::new(());

fn row(tag: &str) -> Vec<WireRow> {
    vec![WireRow {
        columns: vec![WireColumn {
            name: tag.to_string(),
            type_oid: Some(1043),
            bytes: Some(tag.as_bytes().to_vec()),
        }],
    }]
}

/// Drain whatever earlier tests left parked so the counts below are ours.
fn drain() {
    for i in 0..4096 {
        if pending_captures() == 0 {
            return;
        }
        // Take by a key that cannot match, then fall back to draining via
        // publishes if needed; simplest reliable drain is to publish nothing
        // and rely on the bound.
        let _ = take_captured_wire_rows(&format!("__drain_{i}"));
        if i > 200 {
            break;
        }
    }
}

#[test]
fn a_recorded_requests_capture_survives_a_quiet_process() {
    let _guard = SERIALIZE.lock().unwrap_or_else(|p| p.into_inner());
    drain();
    let sql = "SELECT * FROM payment_attempt WHERE payment_id = $1 -- binds: [\"pay_quiet\"]";
    publish_captured_wire_rows(sql, row("quiet"));
    assert!(
        take_captured_wire_rows(sql).is_some(),
        "with no competing traffic the handoff works — this is what every \
         existing test exercises"
    );
}

#[test]
fn competing_traffic_evicts_it_before_the_boundary_can_take_it() {
    let _guard = SERIALIZE.lock().unwrap_or_else(|p| p.into_inner());
    drain();
    let sql = "SELECT * FROM payment_attempt WHERE payment_id = $1 -- binds: [\"pay_recorded\"]";

    // The recorded request's read completes and parks its capture.
    publish_captured_wire_rows(sql, row("recorded"));
    assert_eq!(pending_captures(), 1, "our capture is parked");

    // Ambient traffic: other requests' queries, none of them recorded, so
    // nobody takes their captures. 70 > MAX_PENDING (64).
    for i in 0..70 {
        publish_captured_wire_rows(
            &format!("SELECT * FROM other_table WHERE id = $1 -- binds: [{i}]"),
            row("ambient"),
        );
    }

    let taken = take_captured_wire_rows(sql);
    println!(
        "after 70 ambient publishes: pending={} recorded_capture={}",
        pending_captures(),
        if taken.is_some() {
            "SURVIVED"
        } else {
            "EVICTED"
        }
    );
    assert!(
        taken.is_none(),
        "documents the failure mode: the recorded request's physical image is \
         silently evicted by unrelated traffic, and the seeder falls back to \
         the serde path with no signal"
    );
}

/// How little traffic is needed? Binary-search-ish: the capture is lost as soon
/// as MAX_PENDING other statements land in the window.
#[test]
fn the_eviction_threshold_is_the_registry_bound() {
    let _guard = SERIALIZE.lock().unwrap_or_else(|p| p.into_inner());
    for ambient in [1usize, 16, 32, 63, 64, 65] {
        drain();
        let sql = format!("SELECT recorded_{ambient} -- binds: []");
        publish_captured_wire_rows(&sql, row("recorded"));
        for i in 0..ambient {
            publish_captured_wire_rows(
                &format!("SELECT ambient_{ambient}_{i} -- binds: []"),
                row("a"),
            );
        }
        let survived = take_captured_wire_rows(&sql).is_some();
        println!(
            "ambient publishes in the window = {ambient:>3} -> capture {}",
            if survived { "survives" } else { "LOST" }
        );
    }
}
