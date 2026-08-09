//! Real-postgres round trip for `DejaLoadConnection<PgConnection>`: proves the
//! wrapper satisfies diesel's connection stack against the real backend and
//! captures verbatim binary wire values under `DefaultLoadingMode` (the mode
//! async-bb8-diesel drives — its `load_async` calls `RunQueryDsl::load`).
//!
//! Ignored by default: needs a reachable database. Run with
//!
//! ```text
//! DEJA_DIESEL_TEST_DATABASE_URL=postgres://user:pass@localhost/db \
//!     cargo test -p deja-diesel -- --ignored
//! ```

use deja_diesel::DejaLoadConnection;
use deja_runtime::{wire_capture, RecordingHook, RuntimeHook};
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Nullable, Text};
use std::sync::Arc;

#[derive(QueryableByName, Debug)]
struct WireProbe {
    #[diesel(sql_type = Text)]
    label: String,
    #[diesel(sql_type = BigInt)]
    amount: i64,
    #[diesel(sql_type = Nullable<Text>)]
    missing: Option<String>,
}

#[test]
#[ignore = "needs a reachable postgres; set DEJA_DIESEL_TEST_DATABASE_URL"]
fn wrapper_captures_binary_wire_values_from_a_real_result() {
    let url = std::env::var("DEJA_DIESEL_TEST_DATABASE_URL")
        .expect("DEJA_DIESEL_TEST_DATABASE_URL must point at a scratch database");

    // Install a recording hook so the wrapper's process-level capture gate is
    // open (the same installed hook the boundary macros resolve).
    let artifact_dir = tempfile::tempdir().expect("tempdir");
    let hook = RecordingHook::new(artifact_dir.path()).expect("recording hook");
    // Another test in this binary may have installed it already; both install
    // a recorder, so either outcome opens the gate.
    let _ = deja_runtime::set_global_runtime_hook(Some(RuntimeHook::Recording(Arc::new(hook))));

    let mut conn =
        DejaLoadConnection::<PgConnection>::establish(&url).expect("establish through wrapper");

    let query = diesel::sql_query(
        "SELECT 'poison'::varchar AS label, 42::int8 AS amount, NULL::text AS missing",
    );
    let sql = diesel::debug_query::<diesel::pg::Pg, _>(&query).to_string();

    let rows: Vec<WireProbe> = query.load(&mut conn).expect("load through wrapper");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].label, "poison");
    assert_eq!(rows[0].amount, 42);
    assert!(rows[0].missing.is_none());

    let captured = wire_capture::take_captured_wire_rows(&sql)
        .expect("wrapper published the result's wire rows");
    assert_eq!(captured.len(), 1);
    let columns = &captured[0].columns;
    assert_eq!(columns.len(), 3);

    // varchar (OID 1043): binary wire form is the text bytes.
    assert_eq!(columns[0].name, "label");
    assert_eq!(columns[0].type_oid, Some(1043));
    assert_eq!(columns[0].bytes.as_deref(), Some(b"poison".as_ref()));

    // int8 (OID 20): 8-byte big-endian.
    assert_eq!(columns[1].name, "amount");
    assert_eq!(columns[1].type_oid, Some(20));
    assert_eq!(
        columns[1].bytes.as_deref(),
        Some(42i64.to_be_bytes().as_ref())
    );

    // SQL NULL: no bytes, no OID.
    assert_eq!(columns[2].name, "missing");
    assert_eq!(columns[2].bytes, None);
}

/// The wrapper must be transparent for diesel's pg metadata lookup, not just
/// for queries. `PgMetadataLookup` is blanket-implemented for any
/// `Connection<Backend = Pg> + GetPgMetadataCache + LoadConnection`, and
/// `QueryFragment::collect_binds` requires it — so this bound holding is what
/// lets a host serialize a query's binds through the wrapper instead of
/// reaching past it for the concrete connection. Compile-time: if the
/// delegation is dropped, this stops building.
#[test]
fn the_wrapper_satisfies_the_pg_metadata_lookup_bound() {
    fn assert_lookup<T: diesel::pg::PgMetadataLookup>() {}
    assert_lookup::<DejaLoadConnection<PgConnection>>();
}
