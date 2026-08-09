//! Ambient handoff for wire-faithful DB row capture (issue #35, design:
//! docs/design/wire-faithful-seeding.md).
//!
//! The diesel connection wrapper (`deja-diesel`) observes each result row's
//! per-column binary wire value + type OID before `FromSql` consumes them,
//! and PUBLISHES the result here; the boundary's explicit result producer
//! (`deja::db::recorded_output`) TAKES it and attaches the physical image to
//! the recorded event next to the serde value.
//!
//! # Why a keyed process-global registry and not a thread-local
//!
//! The obvious slot — a thread-local set inside the diesel call and read after
//! it returns — does NOT survive the execution model this capture runs under.
//! Hyperswitch drives diesel through `async-bb8-diesel`, whose every query
//! method runs the sync diesel closure via `tokio::task::spawn_blocking`
//! (`async_traits.rs`: `run_with_connection`), i.e. on a blocking-pool thread.
//! The boundary macro's `result = deja::db::recorded_output(..)` expression is
//! evaluated by `dispatch_async` on the async task AFTER the query future
//! resolves — a tokio worker thread, never the blocking thread that executed
//! the load. Task-locals do not cross `spawn_blocking` either, and the
//! blocking thread has no deja correlation context (verified: the recording
//! hook's `capture_verdict` reads `recording_decision_for_current()`, which is
//! absent there). So the handoff is a bounded process-global queue, keyed by
//! the one identity both sides possess: the `diesel::debug_query` rendering of
//! the executed statement, binds included.
//!
//! # Join-key discipline
//!
//! Both sides render the SAME string with the SAME diesel function
//! (`debug_query::<Pg, _>(&query).to_string()`), so the key match is exact —
//! including bind values — except for one systematic difference:
//! `first()`/`first_async` apply `LIMIT 1` inside the load call AFTER the
//! caller rendered its sql (hyperswitch's `generic_find_by_id` renders
//! `table.find(id)` and then executes `first_async`). [`normalized_key`]
//! therefore strips ONE trailing `LIMIT $n` clause (and the limit bind diesel
//! appends last) from both sides before comparing.
//!
//! Two boundaries whose statements differ in any bind byte can never exchange
//! captures. The residual ambiguity — two byte-identical statements in flight
//! concurrently whose result sets differ because a write landed between them —
//! is resolved FIFO per key and further shape-checked at attach time
//! (`deja::db::row_image_payload_with_wire`: row count, column-name coverage,
//! NULL alignment). On any doubt the physical image is dropped and the entry
//! falls back to the semantic path; the handoff fails open, never misattaches
//! silently.
//!
//! # Inertness
//!
//! Publishing is gated by the CALLER (the wrapper checks the process-level
//! runtime mode; see `deja-diesel`). Taking has an atomic-load fast path so a
//! process that never captures pays one relaxed load per db boundary and no
//! lock.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

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

struct PendingCapture {
    key: String,
    rows: Vec<WireRow>,
    published_at: Instant,
}

/// Upper bound on parked captures. Captures published for boundaries that
/// never take them (sampled-out requests observe nothing, so their
/// `recorded_output` never runs) sit here until evicted; the bound caps the
/// memory at "results in flight", not traffic volume.
const MAX_PENDING: usize = 64;

/// Captures older than this are dead — the matching take happens within the
/// same boundary dispatch, microseconds after the load returns.
const MAX_AGE: Duration = Duration::from_secs(10);

static PENDING_LEN: AtomicUsize = AtomicUsize::new(0);
static REGISTRY: Mutex<VecDeque<PendingCapture>> = Mutex::new(VecDeque::new());

/// Normalize a `debug_query` rendering into the registry join key: strip ONE
/// trailing ` LIMIT $n` clause and its bind (always the last bind — diesel
/// appends the limit bind after the caller's binds). Applied to BOTH the
/// publish side and the take side, so statements that agree modulo the
/// `first()`-applied limit share a key while everything else must match
/// byte-for-byte, binds included.
fn normalized_key(sql: &str) -> String {
    let Some(binds_at) = sql.rfind(" -- binds: ") else {
        return sql.to_string();
    };
    let (query, binds_section) = sql.split_at(binds_at);
    let binds = &binds_section[" -- binds: ".len()..];

    let stripped_query = query.rfind(" LIMIT $").and_then(|pos| {
        let digits = &query[pos + " LIMIT $".len()..];
        (!digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())).then_some(&query[..pos])
    });
    let Some(stripped_query) = stripped_query else {
        return sql.to_string();
    };

    // Drop the final bind entry (the limit value). Binds render as a debug
    // list: `[v1, v2, 1]`. If the list shape is unexpected, fall back to the
    // un-normalized string — a lost join is a lost physical image, never a
    // wrong one.
    let binds = binds.trim();
    let Some(inner) = binds
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
    else {
        return sql.to_string();
    };
    let trimmed_inner = match inner.rfind(", ") {
        Some(comma) => &inner[..comma],
        None => "",
    };
    format!("{stripped_query} -- binds: [{trimmed_inner}]")
}

/// Publish one executed statement's captured rows for the enclosing boundary
/// to take. Called by the `deja-diesel` cursor when it is dropped (i.e. when
/// the result set has been fully handed to the application), on the blocking
/// thread that executed the load.
pub fn publish_captured_wire_rows(sql: &str, rows: Vec<WireRow>) {
    if rows.is_empty() {
        return;
    }
    let capture = PendingCapture {
        key: normalized_key(sql),
        rows,
        published_at: Instant::now(),
    };
    // SHADOW GUARANTEE: never panic on a recording path — recover a poisoned
    // lock instead.
    let mut registry = REGISTRY
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let now = Instant::now();
    registry.retain(|entry| now.duration_since(entry.published_at) < MAX_AGE);
    while registry.len() >= MAX_PENDING {
        registry.pop_front();
    }
    registry.push_back(capture);
    PENDING_LEN.store(registry.len(), Ordering::Release);
}

/// Take the captured rows for one executed statement, matching on the
/// normalized statement key (FIFO among identical keys). Returns `None` when
/// nothing matching was published — an old build without the wrapper, a
/// statement that returned no rows, or a boundary whose statement the wrapper
/// never saw. Callers treat `None` as "no physical image" and keep the
/// semantic path.
pub fn take_captured_wire_rows(sql: &str) -> Option<Vec<WireRow>> {
    // Fast path: a process with no wrapper in the connection stack (or capture
    // gated off) never publishes; don't touch the lock.
    if PENDING_LEN.load(Ordering::Acquire) == 0 {
        return None;
    }
    let key = normalized_key(sql);
    let mut registry = REGISTRY
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let position = registry.iter().position(|entry| entry.key == key)?;
    let capture = registry.remove(position)?;
    PENDING_LEN.store(registry.len(), Ordering::Release);
    Some(capture.rows)
}

/// Number of parked captures (diagnostics/tests).
pub fn pending_captures() -> usize {
    PENDING_LEN.load(Ordering::Acquire)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The registry is process-global and the harness runs tests in parallel;
    /// every test in this module takes this lock, drains, and then owns the
    /// registry for its scenario.
    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn exclusive() -> std::sync::MutexGuard<'static, ()> {
        let guard = TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        drain();
        guard
    }

    fn drain() {
        let mut registry = REGISTRY
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        registry.clear();
        PENDING_LEN.store(0, Ordering::Release);
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

    // The registry is process-global; tests share it, so each test runs its
    // scenario under unique statement keys and drains at the start.

    #[test]
    fn take_matches_exact_statement_and_consumes() {
        let _guard = exclusive();
        let sql = r#"SELECT "t_exact"."a" FROM "t_exact" WHERE "t_exact"."a" = $1 -- binds: ["x"]"#;
        publish_captured_wire_rows(sql, vec![row("a", 1043, b"x")]);
        assert_eq!(pending_captures(), 1);
        let taken = take_captured_wire_rows(sql).expect("published capture");
        assert_eq!(taken, vec![row("a", 1043, b"x")]);
        assert_eq!(pending_captures(), 0);
        assert!(take_captured_wire_rows(sql).is_none(), "take consumes");
    }

    #[test]
    fn different_binds_never_match() {
        let _guard = exclusive();
        let published =
            r#"SELECT "t_binds"."a" FROM "t_binds" WHERE "t_binds"."a" = $1 -- binds: ["x"]"#;
        let asked =
            r#"SELECT "t_binds"."a" FROM "t_binds" WHERE "t_binds"."a" = $1 -- binds: ["y"]"#;
        publish_captured_wire_rows(published, vec![row("a", 1043, b"x")]);
        assert!(take_captured_wire_rows(asked).is_none());
    }

    #[test]
    fn first_applied_limit_joins_with_unlimited_rendering() {
        let _guard = exclusive();
        // The boundary rendered the statement WITHOUT the limit
        // (generic_find_by_id renders `table.find(id)`)...
        let boundary =
            r#"SELECT "t_lim"."id" FROM "t_lim" WHERE "t_lim"."id" = $1 -- binds: ["pay_1"]"#;
        // ...while the wrapper saw the executed statement, where `first_async`
        // appended `LIMIT $2` and its bind.
        let executed = r#"SELECT "t_lim"."id" FROM "t_lim" WHERE "t_lim"."id" = $1 LIMIT $2 -- binds: ["pay_1", 1]"#;
        publish_captured_wire_rows(executed, vec![row("id", 1043, b"pay_1")]);
        let taken = take_captured_wire_rows(boundary).expect("limit-normalized join");
        assert_eq!(taken, vec![row("id", 1043, b"pay_1")]);
    }

    #[test]
    fn explicit_limit_rendered_on_both_sides_still_joins() {
        let _guard = exclusive();
        let sql = r#"SELECT "t_flt"."a" FROM "t_flt" WHERE "t_flt"."x" = $1 LIMIT $2 -- binds: ["m", 10]"#;
        publish_captured_wire_rows(sql, vec![row("a", 1043, b"v")]);
        assert!(take_captured_wire_rows(sql).is_some());
    }

    #[test]
    fn limit_only_bind_normalizes_to_empty_list() {
        let _guard = exclusive();
        let boundary = r#"SELECT "t_all"."a" FROM "t_all" -- binds: []"#;
        let executed = r#"SELECT "t_all"."a" FROM "t_all" LIMIT $1 -- binds: [1]"#;
        publish_captured_wire_rows(executed, vec![row("a", 1043, b"v")]);
        assert!(take_captured_wire_rows(boundary).is_some());
    }

    #[test]
    fn identical_statements_resolve_fifo() {
        let _guard = exclusive();
        let sql = r#"SELECT "t_fifo"."a" FROM "t_fifo" WHERE "t_fifo"."a" = $1 -- binds: ["k"]"#;
        publish_captured_wire_rows(sql, vec![row("a", 1043, b"first")]);
        publish_captured_wire_rows(sql, vec![row("a", 1043, b"second")]);
        assert_eq!(
            take_captured_wire_rows(sql).expect("first"),
            vec![row("a", 1043, b"first")]
        );
        assert_eq!(
            take_captured_wire_rows(sql).expect("second"),
            vec![row("a", 1043, b"second")]
        );
    }

    #[test]
    fn empty_publishes_are_dropped_and_capacity_is_bounded() {
        let _guard = exclusive();
        publish_captured_wire_rows("SELECT 1 -- binds: []", Vec::new());
        assert_eq!(pending_captures(), 0);

        for i in 0..(MAX_PENDING + 8) {
            publish_captured_wire_rows(
                &format!(r#"SELECT "t_cap"."a" FROM "t_cap" -- binds: [{i}]"#),
                vec![row("a", 23, &(i as u32).to_be_bytes())],
            );
        }
        assert_eq!(pending_captures(), MAX_PENDING);
        // The oldest entries were evicted, the newest survive.
        assert!(
            take_captured_wire_rows(r#"SELECT "t_cap"."a" FROM "t_cap" -- binds: [0]"#).is_none()
        );
        assert!(take_captured_wire_rows(&format!(
            r#"SELECT "t_cap"."a" FROM "t_cap" -- binds: [{}]"#,
            MAX_PENDING + 7
        ))
        .is_some());
    }

    #[test]
    fn normalized_key_is_conservative_on_odd_shapes() {
        // No binds comment: untouched.
        assert_eq!(normalized_key("SELECT 1"), "SELECT 1");
        // LIMIT with a non-digit suffix: untouched.
        let odd = "SELECT a FROM t LIMIT $x -- binds: []";
        assert_eq!(normalized_key(odd), odd);
        // LIMIT mid-statement (OFFSET follows): untouched.
        let offset = r#"SELECT a FROM t LIMIT $1 OFFSET $2 -- binds: [10, 5]"#;
        assert_eq!(normalized_key(offset), offset);
    }
}
