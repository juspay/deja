//! `DejaLoadConnection<C>` — a diesel connection wrapper that captures each
//! result row's per-column BINARY wire value + type OID verbatim, BEFORE
//! `FromSql` consumes them (issue #35; design:
//! docs/design/wire-faithful-seeding.md).
//!
//! # What it does
//!
//! Diesel 2.x's postgres path receives results in binary format, always
//! (`Statement::execute` hardcodes result format `1`), so the bytes flowing
//! past this wrapper are `typsend` output — the exact representation binary
//! `COPY` consumes on the seeding side. The wrapper delegates everything to
//! the wrapped connection; its one addition is a cursor adapter that walks
//! each row's columns through diesel's generic row API (`Row`/`Field`,
//! yielding `PgValue::as_bytes()` + `get_oid()`) and puts the captured rows
//! into the slot every `DejaLoadConnection` owns. The row is handed onward
//! untouched — a pure observer.
//!
//! # The handoff is IN-BAND
//!
//! The capture leaves the connection through [`take_captured_rows`]
//! (`DejaLoadConnection::take_captured_rows`), called in the SAME closure that
//! ran the query, on the same blocking thread, on the same connection — the
//! async pool helpers below do exactly that and return the query result PAIRED
//! with its own wire rows. The pair rides the future's value to the enclosing
//! `#[deja::boundary]`, whose result producer attaches the physical image
//! (`deja::db::recorded_output_with_wire`). Nothing ambient — no registry, no
//! task-local, no per-checkout install — connects the two halves; the pairing
//! is lexical, so cross-statement or cross-request misattachment is
//! unrepresentable. (Two ambient predecessors of this handoff failed in
//! production; `deja_runtime::wire_capture` records the history.)
//!
//! The slot invariant the wrapper maintains: [`load`](LoadConnection::load)
//! empties the slot BEFORE executing, and the cursor fills it as the result
//! streams. A take after a load therefore yields that load's rows or nothing —
//! never a leftover from an earlier statement on the same (pooled, long-lived)
//! connection.
//!
//! # Adoption
//!
//! One swap at pool construction covers every query on every pooled
//! connection (design doc, "adoption inversion"). In hyperswitch:
//!
//! ```text
//! // crates/storage_impl/src/database/store.rs
//! pub type PgPool =
//!     bb8::Pool<async_bb8_diesel::ConnectionManager<DejaLoadConnection<PgConnection>>>;
//! pub type PgPooledConn =
//!     async_bb8_diesel::Connection<DejaLoadConnection<PgConnection>>;
//! ```
//!
//! Result-returning query sites then call [`get_result_captured`] /
//! [`get_results_captured`] / [`first_captured`] instead of async-bb8-diesel's
//! `get_result_async` / `get_results_async` / `first_async`, and receive
//! `(result, wire rows)` instead of `result`.
//!
//! The wrapper implements exactly the trait surface that pool stack needs —
//! [`Connection`], [`LoadConnection`] (generic over the loading mode),
//! [`SimpleConnection`], and `diesel::r2d2::R2D2Connection` (what
//! `r2d2::ConnectionManager`/`async_bb8_diesel::ConnectionManager` require) —
//! not a whole third-party backend.
//!
//! # Inertness, and the accepted waste
//!
//! Capture is gated on the process-level deja runtime mode, resolved through
//! the SAME installed hook the boundary macros use
//! ([`deja_runtime::runtime_mode`]). The gate is process-level rather than
//! per-correlation by necessity: the load executes on a blocking-pool thread
//! where the ambient correlation (and with it the per-request sampling
//! decision) does not exist. So while the mode is Record, capture runs for
//! sampled-out requests too; their rows are taken in the helper and dropped
//! immediately when the boundary declines to record — bounded, short-lived
//! waste (one result set's bytes, freed within the same call), the same waste
//! every earlier shape of this capture paid, now with the simplest possible
//! lifecycle. With no hook installed (mode Disabled — the
//! feature-off/observation-off posture), `load` captures nothing and allocates
//! nothing beyond the wrapped cursor.

use std::sync::Arc;

use diesel::connection::{
    AnsiTransactionManager, ConnectionSealed, Instrumentation, LoadConnection, SimpleConnection,
};
use diesel::pg::{GetPgMetadataCache, Pg, PgMetadataCache};
use diesel::query_builder::{Query, QueryFragment, QueryId};
use diesel::query_dsl::methods::{LimitDsl, LoadQuery};
use diesel::row::{Field as _, Row};
use diesel::{Connection, ConnectionResult, QueryResult, RunQueryDsl};

use deja_runtime::wire_capture::{WireColumn, WireRow, WireSlot};

/// One row of `deja::TABLE_IDENTITY_SQL`: a table and one of its primary-key
/// columns, in key order.
///
/// Row identity is a fact about the SCHEMA, so a recorder reads it from the
/// schema rather than carrying a list of table names. This type lives here
/// because it needs diesel's row derive; the statement and the registry it
/// feeds live in the runtime, so every populator asks the same question.
#[derive(diesel::QueryableByName)]
pub struct TableIdentityRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub table_name: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub column_name: String,
}

/// Connection wrapper capturing result rows' binary wire values for
/// wire-faithful seeding. See the crate docs.
pub struct DejaLoadConnection<C> {
    inner: C,
    /// Where this connection's captures go. Always present, created with the
    /// connection; `Arc`-shared INTERNALLY because the cursor cannot borrow a
    /// field of the connection it is reading (the cursor holds the
    /// connection's `&mut` for its whole life). The `Arc` never leaves the
    /// wrapper.
    wire_slot: Arc<WireSlot>,
}

impl<C> DejaLoadConnection<C> {
    /// Wrap an established connection.
    pub fn from_inner(inner: C) -> Self {
        Self {
            inner,
            wire_slot: WireSlot::new_shared(),
        }
    }

    /// Take the rows the most recent load on this connection captured,
    /// emptying the slot. `None` when there is nothing to hand over: the
    /// statement returned no rows, the capture could not be read faithfully,
    /// or capture is gated off.
    ///
    /// Call this in the same closure that ran the query (the pool helpers
    /// below do) — that lexical adjacency IS the pairing.
    pub fn take_captured_rows(&mut self) -> Option<Vec<WireRow>> {
        // Always empty the slot; only hand rows out under an open gate. After
        // a mode flip a lingering capture from the active era must not attach
        // to a statement executed while capture was off.
        let rows = self.wire_slot.take();
        if capture_is_active() {
            rows
        } else {
            None
        }
    }

    /// The wrapped connection, immutably. Diesel needs `&mut` to execute
    /// anything, so this cannot be used to run a query around the capture.
    pub fn inner(&self) -> &C {
        &self.inner
    }

    /// Unwrap. Takes the wrapper by value, so nothing that reaches a pooled
    /// connection can use it to escape the capture.
    pub fn into_inner(self) -> C {
        self.inner
    }
}

/// Delegating the metadata cache is what makes the wrapper transparent for
/// diesel's *non-query* pg capability: diesel auto-implements
/// [`diesel::pg::PgMetadataLookup`] for any
/// `Connection<Backend = Pg> + GetPgMetadataCache + LoadConnection`, so with
/// this one method the wrapper satisfies the bound that user-defined type
/// lookups (and therefore `QueryFragment::collect_binds`) require.
///
/// This exists to close a hole rather than to add a feature. Without it a
/// caller needing the lookup — hyperswitch's KV bind collection is the one in
/// practice — had to reach past the wrapper for the concrete connection, and
/// the accessor that allowed it (`inner_mut`) was an unenforced way to execute
/// a query with no capture at all: silent, because a missing physical image is
/// indistinguishable from a tape that never had one. The accessor is gone; the
/// capability it was reached for is delegated here instead. Compare the tape
/// seam, where the same "no raw handle out" rule is enforced mechanically.
impl<C> GetPgMetadataCache for DejaLoadConnection<C>
where
    C: GetPgMetadataCache,
{
    fn get_metadata_cache(&mut self) -> &mut PgMetadataCache {
        self.inner.get_metadata_cache()
    }
}

/// Whether this process observes at all (record or replay). Process-level by
/// design — see the crate docs' inertness section.
fn capture_is_active() -> bool {
    !deja_runtime::runtime_mode().is_disabled()
}

impl<C> SimpleConnection for DejaLoadConnection<C>
where
    C: SimpleConnection,
{
    fn batch_execute(&mut self, query: &str) -> QueryResult<()> {
        self.inner.batch_execute(query)
    }
}

impl<C> ConnectionSealed for DejaLoadConnection<C> {}

impl<C> Connection for DejaLoadConnection<C>
where
    C: Connection<Backend = Pg, TransactionManager = AnsiTransactionManager>,
{
    type Backend = Pg;
    // Delegating to the wrapped connection's ANSI manager keeps transaction
    // semantics byte-identical: `AnsiTransactionManager`'s blanket impl works
    // against any `Connection<TransactionManager = Self>`, driving the
    // wrapper's `batch_execute`/`transaction_state`, both of which delegate.
    type TransactionManager = AnsiTransactionManager;

    fn establish(database_url: &str) -> ConnectionResult<Self> {
        C::establish(database_url).map(Self::from_inner)
    }

    fn execute_returning_count<T>(&mut self, source: &T) -> QueryResult<usize>
    where
        T: QueryFragment<Self::Backend> + QueryId,
    {
        self.inner.execute_returning_count(source)
    }

    fn transaction_state(&mut self) -> &mut AnsiTransactionManager {
        self.inner.transaction_state()
    }

    fn instrumentation(&mut self) -> &mut dyn Instrumentation {
        self.inner.instrumentation()
    }

    fn set_instrumentation(&mut self, instrumentation: impl Instrumentation) {
        self.inner.set_instrumentation(instrumentation)
    }
}

impl<C, B> LoadConnection<B> for DejaLoadConnection<C>
where
    C: LoadConnection<B> + Connection<Backend = Pg, TransactionManager = AnsiTransactionManager>,
{
    type Cursor<'conn, 'query>
        = DejaCursor<C::Cursor<'conn, 'query>, C::Row<'conn, 'query>>
    where
        Self: 'conn;
    type Row<'conn, 'query>
        = C::Row<'conn, 'query>
    where
        Self: 'conn;

    fn load<'conn, 'query, T>(
        &'conn mut self,
        source: T,
    ) -> QueryResult<Self::Cursor<'conn, 'query>>
    where
        T: Query + QueryFragment<Self::Backend> + QueryId + 'query,
        Self::Backend: diesel::expression::QueryMetadata<T::SqlType>,
    {
        // Where this statement's capture goes. Cloned out of the connection
        // BEFORE the load borrows it mutably — the cursor holds `&'conn mut
        // self` for its whole life and so cannot borrow a field of the
        // connection; the slot is an `Arc` for exactly this reason.
        let slot = capture_is_active().then(|| Arc::clone(&self.wire_slot));
        // SLOT INVARIANT: empty the slot before executing, so whatever a take
        // finds afterwards belongs to THIS statement. A pooled connection
        // lives across requests, and a previous statement whose caller never
        // took (a write, a plain combinator) must not leak its capture into
        // the next taker's pair.
        if let Some(slot) = &slot {
            drop(slot.take());
        }
        let inner = self.inner.load(source)?;
        Ok(DejaCursor::new(inner, slot, capture_wire_row))
    }
}

impl<C> diesel::r2d2::R2D2Connection for DejaLoadConnection<C>
where
    C: diesel::r2d2::R2D2Connection
        + Connection<Backend = Pg, TransactionManager = AnsiTransactionManager>,
{
    fn ping(&mut self) -> QueryResult<()> {
        self.inner.ping()
    }

    fn is_broken(&mut self) -> bool {
        self.inner.is_broken()
    }
}

// ---------------------------------------------------------------------------
// Async pool helpers: the in-band pairing point
// ---------------------------------------------------------------------------
// async-bb8-diesel's combinators (`get_result_async` and friends) each expand
// to `conn.run(|c| query.method(c))` — the query executes inside a
// `spawn_blocking` closure holding `&mut` on the wrapped connection. That
// closure is the ONE place where the statement's result and the connection
// that captured its wire rows exist in the same lexical scope, so these
// helpers do the take right there and return the pair. Bounds mirror
// async-bb8-diesel 0.2.1's `AsyncRunQueryDsl` methods exactly, narrowed to the
// instrumented connection type (only wrapped pools have anything to take).
//
// Error shape: `run`'s own error type folds into the returned `Result` — the
// caller sees one `Result` plus one `Option`, and an `Err` still carries
// whatever the take found (dropped by the boundary, which attaches images only
// to `Ok` results).

/// Fold `conn.run`'s outer (connection-level) error into the inner query
/// result, so callers destructure one `(Result, Option)` pair.
fn fold_run_outcome<U>(
    outcome: Result<(QueryResult<U>, Option<Vec<WireRow>>), diesel::result::Error>,
) -> (QueryResult<U>, Option<Vec<WireRow>>) {
    match outcome {
        Ok(pair) => pair,
        Err(error) => (Err(error), None),
    }
}

/// `get_result` on the query's own connection, returning the result PAIRED
/// with the wire rows that same statement produced. Mirrors
/// `async_bb8_diesel::AsyncRunQueryDsl::get_result_async`.
pub async fn get_result_captured<C, Q, U>(
    conn: &async_bb8_diesel::Connection<DejaLoadConnection<C>>,
    query: Q,
) -> (QueryResult<U>, Option<Vec<WireRow>>)
where
    C: diesel::r2d2::R2D2Connection
        + Connection<Backend = Pg, TransactionManager = AnsiTransactionManager>
        + 'static,
    Q: RunQueryDsl<DejaLoadConnection<C>>
        + LoadQuery<'static, DejaLoadConnection<C>, U>
        + Send
        + 'static,
    U: Send + 'static,
{
    use async_bb8_diesel::AsyncConnection;
    fold_run_outcome(
        conn.run(move |c| {
            let result = query.get_result(c);
            let wire = c.take_captured_rows();
            Ok::<_, diesel::result::Error>((result, wire))
        })
        .await,
    )
}

/// `get_results` on the query's own connection, returning the row vector
/// PAIRED with the wire rows that same statement produced. Mirrors
/// `async_bb8_diesel::AsyncRunQueryDsl::get_results_async`.
pub async fn get_results_captured<C, Q, U>(
    conn: &async_bb8_diesel::Connection<DejaLoadConnection<C>>,
    query: Q,
) -> (QueryResult<Vec<U>>, Option<Vec<WireRow>>)
where
    C: diesel::r2d2::R2D2Connection
        + Connection<Backend = Pg, TransactionManager = AnsiTransactionManager>
        + 'static,
    Q: RunQueryDsl<DejaLoadConnection<C>>
        + LoadQuery<'static, DejaLoadConnection<C>, U>
        + Send
        + 'static,
    U: Send + 'static,
{
    use async_bb8_diesel::AsyncConnection;
    fold_run_outcome(
        conn.run(move |c| {
            let result = query.get_results(c);
            let wire = c.take_captured_rows();
            Ok::<_, diesel::result::Error>((result, wire))
        })
        .await,
    )
}

/// `first` (applies `LIMIT 1`) on the query's own connection, returning the
/// result PAIRED with the wire rows that same statement produced. Mirrors
/// `async_bb8_diesel::AsyncRunQueryDsl::first_async`.
pub async fn first_captured<C, Q, U>(
    conn: &async_bb8_diesel::Connection<DejaLoadConnection<C>>,
    query: Q,
) -> (QueryResult<U>, Option<Vec<WireRow>>)
where
    C: diesel::r2d2::R2D2Connection
        + Connection<Backend = Pg, TransactionManager = AnsiTransactionManager>
        + 'static,
    Q: RunQueryDsl<DejaLoadConnection<C>> + LimitDsl + Send + 'static,
    diesel::dsl::Limit<Q>: LoadQuery<'static, DejaLoadConnection<C>, U>,
    U: Send + 'static,
{
    use async_bb8_diesel::AsyncConnection;
    fold_run_outcome(
        conn.run(move |c| {
            let result = query.first(c);
            let wire = c.take_captured_rows();
            Ok::<_, diesel::result::Error>((result, wire))
        })
        .await,
    )
}

/// Read one row's columns through the generic row API: `(name, type OID,
/// verbatim wire bytes)` per column, `None` bytes for SQL NULL. Returns
/// `None` when the row cannot be read faithfully (an unnamed column) — the
/// cursor then poisons the whole capture rather than publish a partial row.
fn capture_wire_row<'a, R>(row: &R) -> Option<WireRow>
where
    R: Row<'a, Pg>,
{
    let mut columns = Vec::with_capacity(row.field_count());
    for index in 0..row.field_count() {
        let field = row.get(index)?;
        let name = field.field_name()?.to_string();
        let (type_oid, bytes) = match field.value() {
            Some(value) => (Some(value.get_oid().get()), Some(value.as_bytes().to_vec())),
            None => (None, None),
        };
        columns.push(WireColumn {
            name,
            type_oid,
            bytes,
        });
    }
    Some(WireRow { columns })
}

struct CursorCapture<R> {
    slot: Arc<WireSlot>,
    rows: Vec<WireRow>,
    /// Set when any row could not be captured faithfully (a mid-stream load
    /// error, an unreadable row). A poisoned capture is discarded wholesale —
    /// storing a subset would pair wrong wire rows with serde rows.
    poisoned: bool,
    extract: fn(&R) -> Option<WireRow>,
}

/// Pass-through cursor teeing each yielded row into a [`CursorCapture`]. On
/// drop — i.e. once the result set has been handed to the application, still
/// inside the blocking closure that ran the load — the capture is put into the
/// connection's slot for the same closure's take to collect.
pub struct DejaCursor<I, R> {
    inner: I,
    capture: Option<CursorCapture<R>>,
}

impl<I, R> DejaCursor<I, R> {
    fn new(inner: I, slot: Option<Arc<WireSlot>>, extract: fn(&R) -> Option<WireRow>) -> Self {
        Self {
            inner,
            capture: slot.map(|slot| CursorCapture {
                slot,
                rows: Vec::new(),
                poisoned: false,
                extract,
            }),
        }
    }
}

impl<I, R> Iterator for DejaCursor<I, R>
where
    I: Iterator<Item = QueryResult<R>>,
{
    type Item = QueryResult<R>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next();
        if let Some(capture) = self.capture.as_mut() {
            match &item {
                Some(Ok(row)) => match (capture.extract)(row) {
                    Some(wire_row) => capture.rows.push(wire_row),
                    None => capture.poisoned = true,
                },
                Some(Err(_)) => capture.poisoned = true,
                None => {}
            }
        }
        item
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<I, R> Drop for DejaCursor<I, R> {
    fn drop(&mut self) {
        if let Some(capture) = self.capture.take() {
            if !capture.poisoned && !capture.rows.is_empty() {
                capture.slot.put(capture.rows);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use diesel::row::{PartialRow, RowIndex, RowSealed};
    use std::num::NonZeroU32;

    // -----------------------------------------------------------------
    // A minimal fake Pg row: enough of diesel's row API to exercise
    // capture_wire_row and the cursor tee without a live database. The
    // third-party-backend opt-in feature makes exactly this implementable.
    // -----------------------------------------------------------------

    struct FakeColumn {
        name: Option<&'static str>,
        oid: Option<NonZeroU32>,
        bytes: Option<Vec<u8>>,
    }

    struct FakeRow {
        columns: Vec<FakeColumn>,
    }

    struct FakeField<'f> {
        column: &'f FakeColumn,
    }

    impl<'f> diesel::row::Field<'f, Pg> for FakeField<'f> {
        fn field_name(&self) -> Option<&str> {
            self.column.name
        }

        fn value(&self) -> Option<diesel::pg::PgValue<'_>> {
            match (&self.column.bytes, &self.column.oid) {
                (Some(bytes), Some(oid)) => Some(diesel::pg::PgValue::new(bytes, oid)),
                _ => None,
            }
        }
    }

    impl RowSealed for FakeRow {}

    impl RowIndex<usize> for FakeRow {
        fn idx(&self, idx: usize) -> Option<usize> {
            (idx < self.columns.len()).then_some(idx)
        }
    }

    impl<'b> RowIndex<&'b str> for FakeRow {
        fn idx(&self, name: &'b str) -> Option<usize> {
            self.columns
                .iter()
                .position(|column| column.name == Some(name))
        }
    }

    impl<'a> Row<'a, Pg> for FakeRow {
        type Field<'f>
            = FakeField<'f>
        where
            'a: 'f,
            Self: 'f;
        type InnerPartialRow = Self;

        fn field_count(&self) -> usize {
            self.columns.len()
        }

        fn get<'b, I>(&'b self, idx: I) -> Option<Self::Field<'b>>
        where
            'a: 'b,
            Self: RowIndex<I>,
        {
            let idx = RowIndex::idx(self, idx)?;
            self.columns.get(idx).map(|column| FakeField { column })
        }

        fn partial_row(
            &self,
            range: std::ops::Range<usize>,
        ) -> PartialRow<'_, Self::InnerPartialRow> {
            PartialRow::new::<Pg>(self, range)
        }
    }

    fn oid(value: u32) -> NonZeroU32 {
        NonZeroU32::new(value).expect("nonzero oid")
    }

    fn varchar_column(name: &'static str, value: &str) -> FakeColumn {
        FakeColumn {
            name: Some(name),
            oid: Some(oid(1043)),
            bytes: Some(value.as_bytes().to_vec()),
        }
    }

    fn null_column(name: &'static str) -> FakeColumn {
        FakeColumn {
            name: Some(name),
            oid: None,
            bytes: None,
        }
    }

    #[test]
    fn capture_reads_names_oids_bytes_and_nulls() {
        let row = FakeRow {
            columns: vec![
                varchar_column("attempt_id", "att_1"),
                FakeColumn {
                    name: Some("amount"),
                    oid: Some(oid(20)),
                    bytes: Some(42i64.to_be_bytes().to_vec()),
                },
                null_column("connector_transaction_id"),
            ],
        };
        let captured = capture_wire_row(&row).expect("capturable row");
        assert_eq!(
            captured.columns,
            vec![
                WireColumn {
                    name: "attempt_id".into(),
                    type_oid: Some(1043),
                    bytes: Some(b"att_1".to_vec()),
                },
                WireColumn {
                    name: "amount".into(),
                    type_oid: Some(20),
                    bytes: Some(42i64.to_be_bytes().to_vec()),
                },
                WireColumn {
                    name: "connector_transaction_id".into(),
                    type_oid: None,
                    bytes: None,
                },
            ]
        );
    }

    #[test]
    fn capture_refuses_unnamed_columns() {
        let row = FakeRow {
            columns: vec![FakeColumn {
                name: None,
                oid: Some(oid(1043)),
                bytes: Some(b"v".to_vec()),
            }],
        };
        assert!(capture_wire_row(&row).is_none());
    }

    // -----------------------------------------------------------------
    // Cursor tee semantics, against the connection's own slot. No process
    // state is involved, so these tests cannot interfere with each other.
    // -----------------------------------------------------------------

    fn cursor_with_capture(
        items: Vec<QueryResult<FakeRow>>,
        slot: Option<Arc<WireSlot>>,
    ) -> DejaCursor<std::vec::IntoIter<QueryResult<FakeRow>>, FakeRow> {
        DejaCursor::new(items.into_iter(), slot, capture_wire_row)
    }

    #[test]
    fn cursor_puts_captured_rows_in_the_slot_on_drop() {
        let slot = WireSlot::new_shared();
        let cursor = cursor_with_capture(
            vec![
                Ok(FakeRow {
                    columns: vec![varchar_column("a", "one")],
                }),
                Ok(FakeRow {
                    columns: vec![varchar_column("a", "two")],
                }),
            ],
            Some(Arc::clone(&slot)),
        );
        let yielded: Vec<_> = cursor.collect();
        assert_eq!(yielded.len(), 2, "rows pass through untouched");
        let taken = slot.take().expect("put on drop");
        assert_eq!(taken.len(), 2);
        assert_eq!(taken[0].columns[0].bytes.as_deref(), Some(b"one".as_ref()));
        assert_eq!(taken[1].columns[0].bytes.as_deref(), Some(b"two".as_ref()));
    }

    #[test]
    fn cursor_without_a_slot_captures_nothing() {
        let cursor = cursor_with_capture(
            vec![Ok(FakeRow {
                columns: vec![varchar_column("a", "one")],
            })],
            None,
        );
        let yielded: Vec<_> = cursor.collect();
        assert_eq!(yielded.len(), 1, "rows still pass through");
    }

    #[test]
    fn mid_stream_error_poisons_the_capture() {
        let slot = WireSlot::new_shared();
        let cursor = cursor_with_capture(
            vec![
                Ok(FakeRow {
                    columns: vec![varchar_column("a", "one")],
                }),
                Err(diesel::result::Error::BrokenTransactionManager),
            ],
            Some(Arc::clone(&slot)),
        );
        let _ = cursor.count();
        assert!(
            slot.take().is_none(),
            "a partial capture must be discarded, not stored"
        );
    }

    #[test]
    fn empty_result_leaves_the_slot_empty() {
        let slot = WireSlot::new_shared();
        let cursor = cursor_with_capture(Vec::new(), Some(Arc::clone(&slot)));
        let _ = cursor.count();
        assert!(slot.take().is_none());
    }

    #[test]
    fn partial_drain_captures_the_consumed_prefix() {
        let slot = WireSlot::new_shared();
        let mut cursor = cursor_with_capture(
            vec![
                Ok(FakeRow {
                    columns: vec![varchar_column("a", "one")],
                }),
                Ok(FakeRow {
                    columns: vec![varchar_column("a", "two")],
                }),
            ],
            Some(Arc::clone(&slot)),
        );
        let first = cursor.next();
        assert!(first.is_some());
        drop(cursor);
        let taken = slot.take().expect("prefix captured");
        assert_eq!(
            taken.len(),
            1,
            "only rows the application consumed are captured — matching what serde saw"
        );
    }

    // -----------------------------------------------------------------
    // Error folding in the pool helpers: the run's outer error becomes the
    // pair's inner Err, wire None — one Result, one Option, no third channel.
    // -----------------------------------------------------------------

    #[test]
    fn fold_run_outcome_passes_the_pair_through() {
        let pair: (QueryResult<u64>, _) = fold_run_outcome(Ok((
            Ok(7),
            Some(vec![WireRow {
                columns: vec![WireColumn {
                    name: "n".into(),
                    type_oid: Some(20),
                    bytes: Some(7i64.to_be_bytes().to_vec()),
                }],
            }]),
        )));
        assert_eq!(pair.0, Ok(7));
        assert!(pair.1.is_some());
    }

    #[test]
    fn fold_run_outcome_folds_the_outer_error_into_the_result() {
        let (result, wire): (QueryResult<u64>, _) =
            fold_run_outcome(Err(diesel::result::Error::BrokenTransactionManager));
        assert!(matches!(
            result,
            Err(diesel::result::Error::BrokenTransactionManager)
        ));
        assert!(
            wire.is_none(),
            "a connection-level failure captures nothing"
        );
    }

    #[test]
    fn an_inner_err_still_carries_the_take() {
        // The pair shape holds for Err results too: the take ran in the same
        // closure and its outcome rides along; the boundary decides what to do
        // with it (attach nothing).
        let (result, wire): (QueryResult<u64>, _) =
            fold_run_outcome(Ok((Err(diesel::result::Error::NotFound), None)));
        assert!(matches!(result, Err(diesel::result::Error::NotFound)));
        assert!(wire.is_none());
    }
}
