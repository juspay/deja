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
//! yielding `PgValue::as_bytes()` + `get_oid()`) and publishes the captured
//! rows to [`deja_runtime::wire_capture`], keyed by the executed statement's
//! `debug_query` rendering. The row is handed onward untouched — a pure
//! observer.
//!
//! The enclosing `#[deja::boundary]` result producer
//! (`deja::db::recorded_output`) takes the capture by the same key and
//! attaches it to the recorded event as the physical row image. The handoff
//! is a keyed process-global registry, NOT a thread-local: the wrapped load
//! runs on a `spawn_blocking` thread (async-bb8-diesel) while the take runs
//! on the async task — see `deja_runtime::wire_capture` for the verified
//! execution-model analysis.
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
//! The wrapper implements exactly the trait surface that pool stack needs —
//! [`Connection`], [`LoadConnection`] (generic over the loading mode),
//! [`SimpleConnection`], and `diesel::r2d2::R2D2Connection` (what
//! `r2d2::ConnectionManager`/`async_bb8_diesel::ConnectionManager` require) —
//! not a whole third-party backend.
//!
//! # Inertness
//!
//! Capture is gated on the process-level deja runtime mode, resolved through
//! the SAME installed hook the boundary macros use
//! ([`deja_runtime::runtime_mode`]). The gate is process-level rather than
//! per-correlation by necessity: the load executes on a blocking-pool thread
//! where the ambient correlation (and with it the per-request sampling
//! decision) does not exist. Sampling still applies at the TAKE side — a
//! sampled-out request's boundary never runs its result producer, so the
//! unclaimed capture ages out of the bounded registry. With no hook installed
//! (mode Disabled — the feature-off/observation-off posture) `load` renders
//! nothing, captures nothing, and allocates nothing beyond the wrapped
//! cursor.

use diesel::connection::{
    AnsiTransactionManager, ConnectionSealed, Instrumentation, LoadConnection, SimpleConnection,
};
use diesel::pg::Pg;
use diesel::query_builder::{Query, QueryFragment, QueryId};
use diesel::row::{Field as _, Row};
use diesel::{Connection, ConnectionResult, QueryResult};

use deja_runtime::wire_capture::{self, WireColumn, WireRow};

/// Connection wrapper capturing result rows' binary wire values for
/// wire-faithful seeding. See the crate docs.
pub struct DejaLoadConnection<C> {
    inner: C,
}

impl<C> DejaLoadConnection<C> {
    /// Wrap an established connection.
    pub fn from_inner(inner: C) -> Self {
        Self { inner }
    }

    /// The wrapped connection.
    pub fn inner(&self) -> &C {
        &self.inner
    }

    /// The wrapped connection, mutably.
    pub fn inner_mut(&mut self) -> &mut C {
        &mut self.inner
    }

    /// Unwrap.
    pub fn into_inner(self) -> C {
        self.inner
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
        C::establish(database_url).map(|inner| Self { inner })
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
        // Render the statement key BEFORE executing (the source is consumed by
        // the inner load). Same rendering function the boundary uses for its
        // `sql` argument, so the two sides join exactly (modulo the
        // `first()`-applied LIMIT, normalized inside wire_capture).
        let sql = capture_is_active().then(|| diesel::debug_query::<Pg, _>(&source).to_string());
        let inner = self.inner.load(source)?;
        Ok(DejaCursor::new(inner, sql, capture_wire_row))
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
    sql: String,
    rows: Vec<WireRow>,
    /// Set when any row could not be captured faithfully (a mid-stream load
    /// error, an unreadable row). A poisoned capture is discarded wholesale —
    /// publishing a subset would pair wrong wire rows with serde rows.
    poisoned: bool,
    extract: fn(&R) -> Option<WireRow>,
}

/// Pass-through cursor teeing each yielded row into a [`CursorCapture`]. On
/// drop — i.e. once the result set has been handed to the application, still
/// inside the blocking closure that ran the load — the capture is published
/// for the enclosing boundary to take.
pub struct DejaCursor<I, R> {
    inner: I,
    capture: Option<CursorCapture<R>>,
}

impl<I, R> DejaCursor<I, R> {
    fn new(inner: I, sql: Option<String>, extract: fn(&R) -> Option<WireRow>) -> Self {
        Self {
            inner,
            capture: sql.map(|sql| CursorCapture {
                sql,
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
                wire_capture::publish_captured_wire_rows(&capture.sql, capture.rows);
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
    // Cursor tee semantics, against the process-global registry.
    // -----------------------------------------------------------------

    fn cursor_with_capture(
        items: Vec<QueryResult<FakeRow>>,
        sql: Option<&str>,
    ) -> DejaCursor<std::vec::IntoIter<QueryResult<FakeRow>>, FakeRow> {
        DejaCursor::new(items.into_iter(), sql.map(str::to_string), |row| {
            capture_wire_row(row)
        })
    }

    #[test]
    fn cursor_publishes_captured_rows_on_drop() {
        let sql = r#"SELECT "w_pub"."a" FROM "w_pub" -- binds: []"#;
        let cursor = cursor_with_capture(
            vec![
                Ok(FakeRow {
                    columns: vec![varchar_column("a", "one")],
                }),
                Ok(FakeRow {
                    columns: vec![varchar_column("a", "two")],
                }),
            ],
            Some(sql),
        );
        let yielded: Vec<_> = cursor.collect();
        assert_eq!(yielded.len(), 2, "rows pass through untouched");
        let taken = wire_capture::take_captured_wire_rows(sql).expect("published on drop");
        assert_eq!(taken.len(), 2);
        assert_eq!(taken[0].columns[0].bytes.as_deref(), Some(b"one".as_ref()));
        assert_eq!(taken[1].columns[0].bytes.as_deref(), Some(b"two".as_ref()));
    }

    #[test]
    fn cursor_without_capture_publishes_nothing() {
        let sql = r#"SELECT "w_off"."a" FROM "w_off" -- binds: []"#;
        let cursor = cursor_with_capture(
            vec![Ok(FakeRow {
                columns: vec![varchar_column("a", "one")],
            })],
            None,
        );
        let _ = cursor.count();
        assert!(wire_capture::take_captured_wire_rows(sql).is_none());
    }

    #[test]
    fn mid_stream_error_poisons_the_capture() {
        let sql = r#"SELECT "w_err"."a" FROM "w_err" -- binds: []"#;
        let cursor = cursor_with_capture(
            vec![
                Ok(FakeRow {
                    columns: vec![varchar_column("a", "one")],
                }),
                Err(diesel::result::Error::BrokenTransactionManager),
            ],
            Some(sql),
        );
        let _ = cursor.count();
        assert!(
            wire_capture::take_captured_wire_rows(sql).is_none(),
            "a partial capture must be discarded, not published"
        );
    }

    #[test]
    fn empty_result_publishes_nothing() {
        let sql = r#"SELECT "w_empty"."a" FROM "w_empty" -- binds: []"#;
        let cursor = cursor_with_capture(Vec::new(), Some(sql));
        let _ = cursor.count();
        assert!(wire_capture::take_captured_wire_rows(sql).is_none());
    }

    #[test]
    fn partial_drain_publishes_the_consumed_prefix() {
        let sql = r#"SELECT "w_part"."a" FROM "w_part" -- binds: []"#;
        let mut cursor = cursor_with_capture(
            vec![
                Ok(FakeRow {
                    columns: vec![varchar_column("a", "one")],
                }),
                Ok(FakeRow {
                    columns: vec![varchar_column("a", "two")],
                }),
            ],
            Some(sql),
        );
        let first = cursor.next();
        assert!(first.is_some());
        drop(cursor);
        let taken = wire_capture::take_captured_wire_rows(sql).expect("prefix published");
        assert_eq!(
            taken.len(),
            1,
            "only rows the application consumed are captured — matching what serde saw"
        );
    }
}
