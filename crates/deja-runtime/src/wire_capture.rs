//! Types for wire-faithful DB row capture (issues #35 / #50, design:
//! docs/design/wire-faithful-seeding.md).
//!
//! The diesel connection wrapper (`deja-diesel`) observes each result row's
//! per-column binary wire value + type OID before `FromSql` consumes them and
//! puts the capture into the [`WireSlot`] the connection owns. The handoff to
//! the recording boundary is IN-BAND: the same `conn.run` closure that executes
//! the query — on the blocking thread, on the very connection that captured —
//! takes the rows back out (`DejaLoadConnection::take_captured_rows`) and
//! returns them PAIRED with the query result as the closure's value. The pair
//! rides the future to the boundary; nothing ambient connects the two halves.
//!
//! # Why in-band, and not ambient state
//!
//! Two ambient shapes of this handoff shipped and failed in production:
//!
//! 1. A process-global queue keyed on the rendered statement, bounded at 64
//!    entries with oldest-first eviction. The capture gate is process-level, so
//!    every statement the process ran published — including the majority from
//!    requests the sampler excluded, which never take — and those unclaimed
//!    captures evicted the recorded request's image. Measured exactly: a
//!    capture survived 63 competing statements and was lost at 64, which on a
//!    live pod is every recorded request.
//! 2. A slot on the connection, with the TAKE side routed through a tokio
//!    task-local installed per checkout and a scope wrapped around each request
//!    by actix middleware. Locally correct on both halves, and it produced zero
//!    captures in the deployed router — most plausibly a scope that was never
//!    active around the paths that mattered, and undiagnosable in the field
//!    because its counters were never exported.
//!
//! Both failures are the same defect: the producer and the consumer agreed on a
//! contract carried by AMBIENT state (a shared queue; a task-local handle), and
//! ambient state can silently be wrong — evicted, or never in scope — while
//! both halves keep passing their own tests. The in-band pair deletes the
//! contract instead of hardening it: the take happens in the same lexical scope
//! as the query, on the same connection, and the rows travel to the boundary as
//! part of the value the boundary already receives. Mispairing is not
//! mitigated; it is unrepresentable.
//!
//! # What lives here
//!
//! The value types the capture produces ([`WireColumn`], [`WireRow`]) and the
//! single-occupant [`WireSlot`] the connection wrapper uses INTERNALLY to move
//! a capture from its cursor (which cannot borrow the connection it is
//! reading) back to the connection's `take_captured_rows`. The slot is an
//! implementation detail of `deja-diesel`; no host installs, scopes, or reads
//! one.

use std::sync::{Arc, Mutex};

/// One column of one captured result row: the column name postgres reported,
/// the value's type OID, and the verbatim binary wire bytes (`typsend`
/// output — diesel hardcodes result format 1). `bytes: None` is SQL NULL;
/// a NULL value carries no OID either (libpq reports the OID with the value).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireColumn {
    /// Column name as reported by the result set (`PQfname`).
    pub name: String,
    /// The value's type OID as reported with the result. `None` iff `bytes`
    /// is `None`.
    pub type_oid: Option<u32>,
    /// Verbatim binary wire bytes; `None` is SQL NULL.
    pub bytes: Option<Vec<u8>>,
}

/// One captured result row.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WireRow {
    pub columns: Vec<WireColumn>,
}

/// A single-occupant holder for one connection's most recent capture.
///
/// Interior-mutable and `Arc`-shared because the wrapper's cursor cannot
/// borrow a field of the connection it is reading (the cursor holds the
/// connection's `&mut` for its whole life): the connection and its cursors
/// share the one slot, the cursor puts on drop, and the connection's
/// `take_captured_rows` empties it — all inside the same blocking closure
/// that ran the query.
///
/// One occupant is the whole point: a connection executes one statement at a
/// time, so there is never a second capture to queue, key, or age out. A put
/// onto an occupied slot replaces the previous capture — that capture belonged
/// to a statement whose caller did not take (a plain combinator, a write), and
/// the replacement guarantees a taker can only ever receive rows from the
/// statement it just ran.
#[derive(Debug, Default)]
pub struct WireSlot {
    rows: Mutex<Option<Vec<WireRow>>>,
}

impl WireSlot {
    /// An empty slot.
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty slot, shared: the shape the connection wrapper and its cursors
    /// hold.
    pub fn new_shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// Put one executed statement's captured rows. Called by the `deja-diesel`
    /// cursor when it is dropped (i.e. once the result set has been handed to
    /// the application), on the thread that executed the load. Empty captures
    /// are dropped rather than stored — an occupied slot always holds
    /// something worth attaching.
    pub fn put(&self, rows: Vec<WireRow>) {
        if rows.is_empty() {
            return;
        }
        // SHADOW GUARANTEE: never panic on a recording path — recover a
        // poisoned lock instead.
        let mut slot = self
            .rows
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *slot = Some(rows);
    }

    /// Take the captured rows, emptying the slot. `None` when nothing was
    /// captured since the last take — a statement that returned no rows, a
    /// write, or capture gated off. Callers treat `None` as "no physical
    /// image" and keep the semantic path.
    pub fn take(&self) -> Option<Vec<WireRow>> {
        self.rows
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
    }

    /// Whether the slot currently holds a capture (diagnostics/tests).
    pub fn is_occupied(&self) -> bool {
        self.rows
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(name: &str, oid: u32, bytes: &[u8]) -> WireRow {
        WireRow {
            columns: vec![WireColumn {
                name: name.to_string(),
                type_oid: Some(oid),
                bytes: Some(bytes.to_vec()),
            }],
        }
    }

    #[test]
    fn a_slot_holds_one_capture_and_take_consumes_it() {
        let slot = WireSlot::new_shared();
        assert!(!slot.is_occupied());
        slot.put(vec![row("a", 1043, b"x")]);
        assert!(slot.is_occupied());
        assert_eq!(slot.take(), Some(vec![row("a", 1043, b"x")]));
        assert_eq!(slot.take(), None, "take consumes");
    }

    #[test]
    fn empty_captures_are_not_stored() {
        let slot = WireSlot::new_shared();
        slot.put(Vec::new());
        assert!(!slot.is_occupied());
    }

    #[test]
    fn an_untaken_capture_is_replaced_by_the_next_statement() {
        // One connection, two statements, no take in between (the first caller
        // used a plain combinator): the slot holds the LATEST capture, so a
        // taker can only receive rows from the statement it just ran.
        let slot = WireSlot::new_shared();
        slot.put(vec![row("a", 1043, b"first")]);
        slot.put(vec![row("a", 1043, b"second")]);
        assert_eq!(slot.take(), Some(vec![row("a", 1043, b"second")]));
    }
}
