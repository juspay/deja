//! Per-connection handoff for wire-faithful DB row capture (issues #35 / #50,
//! design: docs/design/wire-faithful-seeding.md).
//!
//! The diesel connection wrapper (`deja-diesel`) observes each result row's
//! per-column binary wire value + type OID before `FromSql` consumes them and
//! PUTS the result into the slot installed on the connection that executed the
//! statement; the boundary's explicit result producer
//! (`deja::db::recorded_output`) TAKES it from the slot the request installed
//! and attaches the physical image to the recorded event next to the serde
//! value.
//!
//! # Why a slot on the connection, and not a keyed global queue
//!
//! The first shape of this handoff was a process-global `VecDeque` keyed on the
//! rendered statement, bounded at 64 entries with oldest-first eviction. It does
//! not work under load, and the failure was measured rather than argued: the
//! wrapper's capture gate is PROCESS-level, so every statement the process runs
//! publishes — including the majority belonging to requests the sampler excluded
//! from recording, which never run a boundary result producer and therefore
//! never take. Those unclaimed captures fill the queue and evict the image the
//! recorded request is about to claim. The threshold was exact: a capture
//! survived 63 competing statements in its window and was lost at 64. On a pod
//! serving live traffic that is every recorded request, so a sandbox recording
//! produced zero physical row images and seeding silently fell back to the
//! serde path (45 `payment_attempt` refusals).
//!
//! A connection executes one statement at a time. So a slot ON the connection
//! has exactly one occupant, and the shared bounded registry disappears with all
//! of its apparatus: no join key, no key search, no FIFO tie-break, no capacity
//! bound, no aging — and no `LIMIT $n` normalization tracking a diesel
//! implementation detail. Overflow is not mitigated; it is impossible.
//!
//! # How the take side finds the slot
//!
//! The instrumented boundary receives the query FUTURE, not the connection
//! (`execute_generic_find_one(fut, table, sql, inputs)`), and the pooled
//! connection's inner handle is not reachable synchronously
//! (`async_bb8_diesel::Connection::inner` is `pub(crate)`). So the slot handle
//! is ambient WITHIN the request's async task: the host creates a slot at
//! connection checkout, installs one `Arc` on the connection and registers the
//! other as the task's CURRENT slot ([`set_current_slot`]); the boundary reads
//! the current slot and takes from it ([`take_current_rows`]).
//!
//! The carrier is a tokio task-local, deliberately, and neither of the two
//! alternatives fits:
//!
//! - A plain thread-local cannot carry it. There is an await between install
//!   and take (the query itself), so the task can migrate to another worker
//!   thread and another task can run on this one in the meantime. A second task
//!   reading the first task's handle would take a capture belonging to a
//!   different request — the silent-wrong-value class this whole effort exists
//!   to remove, since the shape check at attach time cannot distinguish two
//!   concurrent reads of the same table.
//! - `deja-context` (the crate that carries correlation) does not fit either.
//!   Its snapshot is cloned into a task registry at spawn and restored from the
//!   span tree, so one slot would be shared by a parent and every task it
//!   spawns — the same cross-request misattachment, plus an `Arc` retained in a
//!   process-global map. Correlation is a value that SHOULD be inherited;
//!   a capture slot is an exclusive resource that must not be.
//!
//! A tokio task-local is exclusive by construction: [`tokio::task_local`]'s
//! future swaps the value in for the duration of each poll and out again when
//! the poll returns, so no other task can observe it, and it survives task
//! migration because it lives in the future rather than on a thread.
//!
//! # Known limitation: last install wins, per task
//!
//! The current-slot handle is per async task and last-installed-wins. A task
//! that holds two pooled connections at once and queries the FIRST after
//! checking out the second reads the wrong (second) slot, finds it empty, and
//! records no physical image — the entry falls back to the serde path.
//! Hyperswitch checks out immediately before querying, so this is rare, and it
//! is a coverage loss, never a wrong value: an empty slot yields nothing to
//! attach. [`counts`] makes it visible if it stops being rare
//! (`takes_without_slot`, and `overwritten_unclaimed` for the mirror case where
//! a capture nobody claimed is displaced by the connection's next statement).
//!
//! # Inertness
//!
//! Putting is gated by the CALLER (the wrapper checks the process-level runtime
//! mode AND requires an installed slot; see `deja-diesel`), so a process with
//! no host wiring captures nothing and allocates nothing. Taking has an
//! atomic-load fast path: until something has actually been captured, a db
//! boundary pays one relaxed load and never touches the task-local.

use std::cell::Cell;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
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

// ---------------------------------------------------------------------------
// Observability. A silent coverage regression is the failure class this design
// exists to eliminate, so the handoff counts itself: every put becomes a take,
// an overwrite, or a slot still holding it at teardown.
// ---------------------------------------------------------------------------

static PUT: AtomicU64 = AtomicU64::new(0);
static TAKEN: AtomicU64 = AtomicU64::new(0);
static OVERWRITTEN_UNCLAIMED: AtomicU64 = AtomicU64::new(0);
static TAKES_WITHOUT_SLOT: AtomicU64 = AtomicU64::new(0);
static INSTALLS_WITHOUT_SCOPE: AtomicU64 = AtomicU64::new(0);

/// A snapshot of the handoff's process-wide counters. Monotonic since process
/// start; read it as a delta across a run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WireCaptureCounts {
    /// Captures put into a slot by the connection wrapper.
    pub put: u64,
    /// Captures a boundary claimed and attached.
    pub taken: u64,
    /// Puts onto an already-occupied slot: a capture nobody took, displaced by
    /// that connection's next statement. Expected for statements no recorded
    /// boundary claims (a sampled-out request, a query outside an instrumented
    /// boundary); a large share against `taken` means recorded reads are
    /// missing their physical image.
    pub overwritten_unclaimed: u64,
    /// Boundaries that asked for a capture while no slot was current for their
    /// task: the host has not installed one (no wiring, or a checkout path that
    /// skips it), or the task holds a second connection whose install replaced
    /// this one's. Coverage loss, never a wrong value.
    pub takes_without_slot: u64,
    /// Slots offered as current with no task-local scope established around the
    /// request. A wiring defect: without the scope no boundary can ever find a
    /// slot, and this is the only signal that says so.
    pub installs_without_scope: u64,
}

/// Read the handoff's counters. See [`WireCaptureCounts`].
pub fn counts() -> WireCaptureCounts {
    WireCaptureCounts {
        put: PUT.load(Ordering::Relaxed),
        taken: TAKEN.load(Ordering::Relaxed),
        overwritten_unclaimed: OVERWRITTEN_UNCLAIMED.load(Ordering::Relaxed),
        takes_without_slot: TAKES_WITHOUT_SLOT.load(Ordering::Relaxed),
        installs_without_scope: INSTALLS_WITHOUT_SCOPE.load(Ordering::Relaxed),
    }
}

// ---------------------------------------------------------------------------
// The slot
// ---------------------------------------------------------------------------

/// A single-occupant holder for one connection's most recent capture.
///
/// Interior-mutable so the connection wrapper can put through a shared handle
/// (the cursor cannot borrow a field of the connection it is reading), and
/// shared as an `Arc` because the same slot is held by the connection and by
/// the async task that checked the connection out.
///
/// One occupant is the whole point: a connection executes one statement at a
/// time, so there is never a second capture to queue, key, or age out. A put
/// onto an occupied slot means the previous capture was never claimed; it is
/// replaced and counted ([`WireCaptureCounts::overwritten_unclaimed`]).
#[derive(Debug, Default)]
pub struct WireSlot {
    rows: Mutex<Option<Vec<WireRow>>>,
}

impl WireSlot {
    /// An empty slot.
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty slot, shared: the shape both the connection and the async task
    /// hold.
    pub fn new_shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// Put one executed statement's captured rows. Called by the `deja-diesel`
    /// cursor when it is dropped (i.e. once the result set has been handed to
    /// the application), on the blocking thread that executed the load. Empty
    /// captures are dropped rather than stored — an occupied slot always holds
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
        if slot.is_some() {
            OVERWRITTEN_UNCLAIMED.fetch_add(1, Ordering::Relaxed);
        }
        *slot = Some(rows);
        drop(slot);
        // Release, paired with the Acquire in `take_current_rows`: a boundary
        // that observes a nonzero count also observes the stored rows.
        PUT.fetch_add(1, Ordering::Release);
    }

    /// Take the captured rows, emptying the slot. `None` when nothing was
    /// captured for this connection since the last take — an old build without
    /// the wrapper, a statement that returned no rows, a write. Callers treat
    /// `None` as "no physical image" and keep the semantic path.
    pub fn take(&self) -> Option<Vec<WireRow>> {
        let rows = self
            .rows
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if rows.is_some() {
            TAKEN.fetch_add(1, Ordering::Relaxed);
        }
        rows
    }

    /// Whether the slot currently holds a capture (diagnostics/tests).
    pub fn is_occupied(&self) -> bool {
        self.rows
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_some()
    }
}

// ---------------------------------------------------------------------------
// The ambient handle: which slot belongs to the work running right now
// ---------------------------------------------------------------------------

tokio::task_local! {
    /// The slot installed by the most recent connection checkout on THIS async
    /// task. A `Cell` (not a `RefCell`) so reading and replacing it cannot
    /// panic on a recording path.
    static CURRENT_SLOT: Cell<Option<Arc<WireSlot>>>;
}

/// Establish the per-task current-slot carrier around a unit of work — one
/// request, in a host. Installing a slot outside such a scope cannot be
/// observed by anything, so the host wraps its request future here once and
/// every checkout inside it can hand its slot to the boundaries that follow.
pub fn scope<F: Future>(future: F) -> impl Future<Output = F::Output> {
    CURRENT_SLOT.scope(Cell::new(None), future)
}

/// [`scope`] for synchronous work: the carrier lives for the duration of `f`.
/// Needs no runtime, which is what makes the handoff testable without one.
pub fn scope_sync<R>(f: impl FnOnce() -> R) -> R {
    CURRENT_SLOT.sync_scope(Cell::new(None), f)
}

/// Register `slot` as the current slot for the running task. Returns `false`
/// when no [`scope`] is established (nothing can read the handle, so the
/// capture is unreachable) and counts it as a wiring defect.
pub fn set_current_slot(slot: Arc<WireSlot>) -> bool {
    match CURRENT_SLOT.try_with(|cell| cell.set(Some(slot))) {
        Ok(()) => true,
        Err(_) => {
            INSTALLS_WITHOUT_SCOPE.fetch_add(1, Ordering::Relaxed);
            false
        }
    }
}

/// The current slot for the running task, if one was installed.
pub fn current_slot() -> Option<Arc<WireSlot>> {
    CURRENT_SLOT
        .try_with(|cell| {
            // Read-through: take the handle out to clone it, then put it back.
            let slot = cell.take();
            cell.set(slot.clone());
            slot
        })
        .ok()
        .flatten()
}

/// Take the capture belonging to the statement this task just executed: the
/// boundary side of the handoff. `None` when no slot is current for this task,
/// or the current slot is empty.
pub fn take_current_rows() -> Option<Vec<WireRow>> {
    // Fast path: nothing has ever been captured in this process (no wrapper in
    // the connection stack, or capture gated off), so there is nothing to find.
    if PUT.load(Ordering::Acquire) == 0 {
        return None;
    }
    match current_slot() {
        Some(slot) => slot.take(),
        None => {
            TAKES_WITHOUT_SLOT.fetch_add(1, Ordering::Relaxed);
            None
        }
    }
}

#[cfg(test)]
// Test-only serialization: the guard is held across awaits on purpose (see
// COUNTERS below); each async test runs its own single-threaded runtime.
#[allow(clippy::await_holding_lock)]
mod tests {
    use super::*;

    /// The counters are process-global and the harness runs tests in parallel,
    /// so EVERY test here takes this lock — every put and take bumps a
    /// counter, and the tests that assert counter deltas must not race the
    /// tests that merely exercise slot semantics. The async tests hold the
    /// guard across their awaits deliberately: each `#[tokio::test]` runs its
    /// own single-threaded runtime, so nothing else on that runtime contends
    /// for the lock.
    static COUNTERS: Mutex<()> = Mutex::new(());

    fn exclusive() -> std::sync::MutexGuard<'static, ()> {
        COUNTERS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

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
        let _guard = exclusive();
        let slot = WireSlot::new_shared();
        assert!(!slot.is_occupied());
        slot.put(vec![row("a", 1043, b"x")]);
        assert!(slot.is_occupied());
        assert_eq!(slot.take(), Some(vec![row("a", 1043, b"x")]));
        assert_eq!(slot.take(), None, "take consumes");
    }

    #[test]
    fn empty_captures_are_not_stored() {
        let _guard = exclusive();
        let slot = WireSlot::new_shared();
        slot.put(Vec::new());
        assert!(!slot.is_occupied());
    }

    #[test]
    fn an_unclaimed_capture_is_overwritten_and_counted() {
        let _guard = exclusive();
        let before = counts();

        let slot = WireSlot::new_shared();
        slot.put(vec![row("a", 1043, b"first")]);
        // Nobody took the first capture; the connection's next statement lands
        // on an occupied slot.
        slot.put(vec![row("a", 1043, b"second")]);

        let after = counts();
        assert_eq!(after.put - before.put, 2);
        assert_eq!(
            after.overwritten_unclaimed - before.overwritten_unclaimed,
            1,
            "a put onto an occupied slot is a capture nobody took"
        );
        assert_eq!(
            slot.take(),
            Some(vec![row("a", 1043, b"second")]),
            "the newest capture is the one that survives"
        );
        assert_eq!(counts().taken - before.taken, 1);
    }

    #[tokio::test]
    async fn the_current_slot_is_the_one_the_task_installed() {
        let _guard = exclusive();
        let slot = WireSlot::new_shared();
        let installed = Arc::clone(&slot);
        scope(async move {
            assert!(set_current_slot(installed));
            // The connection publishes into the slot it was given...
            slot.put(vec![row("id", 1043, b"pay_1")]);
            // ...and the boundary takes it back through the ambient handle.
            assert_eq!(take_current_rows(), Some(vec![row("id", 1043, b"pay_1")]));
            assert_eq!(take_current_rows(), None, "one capture, one take");
        })
        .await;
    }

    #[tokio::test]
    async fn a_second_install_replaces_the_first() {
        let _guard = exclusive();
        // The documented limitation, pinned: a task holding two connections
        // reads the LAST one installed. Coverage loss (an empty slot), never
        // another request's rows.
        let first = WireSlot::new_shared();
        let second = WireSlot::new_shared();
        let (a, b) = (Arc::clone(&first), Arc::clone(&second));
        scope(async move {
            set_current_slot(a);
            set_current_slot(b);
            // A statement on the FIRST connection now publishes where nobody
            // is looking.
            first.put(vec![row("id", 1043, b"lost")]);
            assert_eq!(take_current_rows(), None);
            second.put(vec![row("id", 1043, b"found")]);
            assert_eq!(take_current_rows(), Some(vec![row("id", 1043, b"found")]));
        })
        .await;
    }

    #[tokio::test]
    async fn one_tasks_slot_is_invisible_to_another() {
        let _guard = exclusive();
        // The property a thread-local could not give: two tasks on the same
        // runtime, each with its own slot, never see each other's captures.
        let mine = WireSlot::new_shared();
        let theirs = WireSlot::new_shared();
        let mine_installed = Arc::clone(&mine);
        let theirs_installed = Arc::clone(&theirs);

        let other = tokio::spawn(scope(async move {
            set_current_slot(theirs_installed);
            theirs.put(vec![row("id", 1043, b"other_request")]);
            // Even though this task ran on the same runtime, its take can only
            // reach its own slot.
            assert_eq!(
                take_current_rows(),
                Some(vec![row("id", 1043, b"other_request")])
            );
        }));
        other.await.expect("joined");

        scope(async move {
            set_current_slot(mine_installed);
            mine.put(vec![row("id", 1043, b"my_request")]);
            assert_eq!(
                take_current_rows(),
                Some(vec![row("id", 1043, b"my_request")])
            );
        })
        .await;
    }

    #[tokio::test]
    async fn spawned_work_does_not_inherit_the_slot() {
        let _guard = exclusive();
        // Exclusive by construction: a child task cannot claim its parent's
        // capture (which would be another request's rows once the parent's
        // connection is reused).
        let slot = WireSlot::new_shared();
        let installed = Arc::clone(&slot);
        scope(async move {
            set_current_slot(installed);
            slot.put(vec![row("id", 1043, b"parent")]);
            let child = tokio::spawn(async { current_slot().is_none() });
            assert!(child.await.expect("joined"), "no inheritance");
            assert_eq!(take_current_rows(), Some(vec![row("id", 1043, b"parent")]));
        })
        .await;
    }

    #[tokio::test]
    async fn unrelated_traffic_cannot_displace_a_waiting_capture() {
        let _guard = exclusive();
        // The measured failure, as a property test of the new shape. The old
        // registry was one process-global 64-entry deque: 64 captures published
        // by other requests in the window evicted this one. Ten times that much
        // traffic now goes into ten times that many slots, none of them ours.
        let mine = WireSlot::new_shared();
        let installed = Arc::clone(&mine);
        scope(async move {
            set_current_slot(installed);
            mine.put(vec![row("id", 1043, b"recorded")]);

            let strangers: Vec<Arc<WireSlot>> = (0..640).map(|_| WireSlot::new_shared()).collect();
            for stranger in &strangers {
                stranger.put(vec![row("id", 1043, b"ambient")]);
            }

            assert_eq!(
                take_current_rows(),
                Some(vec![row("id", 1043, b"recorded")]),
                "there is no shared capacity for other requests to consume"
            );
        })
        .await;
    }

    #[test]
    fn a_take_with_no_slot_is_counted_not_guessed() {
        let _guard = exclusive();
        // Something must have been captured in this process, or the fast path
        // short-circuits before the accounting.
        WireSlot::new_shared().put(vec![row("a", 1043, b"x")]);
        let before = counts();
        scope_sync(|| {
            assert_eq!(take_current_rows(), None);
        });
        assert_eq!(counts().takes_without_slot - before.takes_without_slot, 1);
    }

    #[test]
    fn installing_outside_a_scope_reports_the_wiring_defect() {
        let _guard = exclusive();
        let before = counts();
        assert!(
            !set_current_slot(WireSlot::new_shared()),
            "no scope, no handle"
        );
        assert_eq!(
            counts().installs_without_scope - before.installs_without_scope,
            1
        );
        assert!(current_slot().is_none());
    }
}
