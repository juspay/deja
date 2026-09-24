//! Capture a query's bind values as structure rather than as diesel's
//! `-- binds: [...]` rendering.
//!
//! `debug_query` renders each bind with its Rust `Debug`, so a map bound as
//! jsonb reaches the tape as text in its iteration order, and two builds of one
//! value compare as two different strings. Here the binds are collected as the
//! bytes diesel sends to Postgres and decoded by type: json and jsonb become
//! JSON documents, arrays become JSON arrays, and every scalar leaf is replaced
//! by a keyed digest, so the structure reaches the tape and the contents do not.
//!
//! Without a capture key nothing structured is written: the capture is the
//! debug rendering, masked as it always was.

use diesel::debug_query;
use diesel::pg::{Pg, PgMetadataLookup, PgQueryBuilder, PgTypeMetadata};
use diesel::query_builder::bind_collector::RawBytesBindCollector;
use diesel::query_builder::{QueryBuilder as _, QueryFragment};

use deja_runtime::capture_key::CaptureKey;

/// What a db boundary records about the query it ran.
#[derive(Debug, Clone, PartialEq)]
pub struct CapturedQuery {
    /// The statement. With a key, the statement alone, never its operands;
    /// without one, diesel's debug rendering, binds included.
    pub sql: String,
    /// The bind image `{"key_id", "binds": {"$1": .., "$2": ..}}`, keyed by the
    /// placeholder each value fills,
    /// or `{"key_id", "capture_failed"}` naming the step that failed. `None`
    /// without a key.
    pub binds: Option<serde_json::Value>,
}

/// Capture `query` under the process's installed capture key.
pub fn capture_query<Q: QueryFragment<Pg>>(query: &Q) -> CapturedQuery {
    capture_query_with(deja_runtime::capture_key::capture_key(), query)
}

/// Capture `query` under an explicit key.
pub fn capture_query_with<Q: QueryFragment<Pg>>(
    key: Option<&CaptureKey>,
    query: &Q,
) -> CapturedQuery {
    let Some(key) = key else {
        return CapturedQuery {
            sql: debug_rendering(query),
            binds: None,
        };
    };
    match statement_and_binds(query) {
        Ok((sql, collector)) => CapturedQuery {
            sql,
            binds: Some(serde_json::json!({
                "key_id": key.id(),
                // Keyed by placeholder, not listed: a bind's position is part
                // of what the statement means, and an object is compared and
                // hashed by key, so no rule that forgives array order can make
                // `$1`/`$2` swapped read as the same query.
                "binds": collector
                    .binds
                    .iter()
                    .zip(&collector.metadata)
                    .enumerate()
                    .map(|(index, (bytes, metadata))| {
                        (
                            format!("${}", index + 1),
                            decode_bind(key, bytes.as_deref(), metadata),
                        )
                    })
                    .collect::<serde_json::Map<_, _>>(),
            })),
        },
        // Capture never fails the query, and a failure writes no operands:
        // the failed step is named, not the error's text, which can carry a
        // value, and the debug rendering is never the fallback here.
        Err(failure) => CapturedQuery {
            sql: failure.statement,
            binds: Some(serde_json::json!({
                "key_id": key.id(),
                "capture_failed": failure.step,
            })),
        },
    }
}

/// Written in place of a statement diesel could not build.
pub const UNBUILDABLE_STATEMENT: &str = "<statement did not build>";

struct CaptureFailure {
    step: &'static str,
    statement: String,
}

fn statement_and_binds<Q: QueryFragment<Pg>>(
    query: &Q,
) -> Result<(String, RawBytesBindCollector<Pg>), CaptureFailure> {
    let mut builder = PgQueryBuilder::default();
    query
        .to_sql(&mut builder, &Pg)
        .map_err(|_| CaptureFailure {
            step: "statement",
            statement: UNBUILDABLE_STATEMENT.to_owned(),
        })?;
    let statement = builder.finish();
    let mut collector = RawBytesBindCollector::<Pg>::new();
    match query.collect_binds(&mut collector, &mut NoConnectionLookup, &Pg) {
        Ok(()) => Ok((statement, collector)),
        Err(_) => Err(CaptureFailure {
            step: "binds",
            statement,
        }),
    }
}

/// diesel's debug rendering, without its panic: `debug_query`'s `Display`
/// reports a statement that does not build (an empty changeset) as a format
/// error, which `to_string` turns into a panic.
fn debug_rendering<Q: QueryFragment<Pg>>(query: &Q) -> String {
    use std::fmt::Write as _;
    let mut rendered = String::new();
    match write!(rendered, "{}", debug_query::<Pg, _>(query)) {
        Ok(()) => rendered,
        Err(_) => UNBUILDABLE_STATEMENT.to_owned(),
    }
}

/// Stand-ins for the OIDs of host-defined types. A real lookup would query the
/// database; capture has no connection and needs none, because a custom type's
/// bytes are captured as an opaque leaf either way. The array stand-in is only
/// ever read back as an array's element type.
const CUSTOM_TYPE_OID: u32 = u32::MAX - 1;
const CUSTOM_ARRAY_OID: u32 = u32::MAX;

struct NoConnectionLookup;

impl PgMetadataLookup for NoConnectionLookup {
    fn lookup_type(&mut self, _type_name: &str, _schema: Option<&str>) -> PgTypeMetadata {
        PgTypeMetadata::new(CUSTOM_TYPE_OID, CUSTOM_ARRAY_OID)
    }
}

const JSON_OID: u32 = 114;
const JSONB_OID: u32 = 3802;

fn decode_bind(
    key: &CaptureKey,
    bytes: Option<&[u8]>,
    metadata: &PgTypeMetadata,
) -> serde_json::Value {
    let Some(bytes) = bytes else {
        return serde_json::Value::Null;
    };
    // diesel describes an array bind as `(array oid, 0)`: an array type has no
    // array type of its own. Every scalar type here has one.
    let is_array = matches!(metadata.array_oid(), Ok(0));
    if is_array {
        return decode_array(key, bytes).unwrap_or_else(|| opaque(key, bytes));
    }
    decode_scalar(key, bytes, metadata.oid().ok())
}

fn decode_scalar(key: &CaptureKey, bytes: &[u8], oid: Option<u32>) -> serde_json::Value {
    let document = match oid {
        Some(JSON_OID) => Some(bytes),
        // jsonb's binary form is a version byte, then the JSON text.
        Some(JSONB_OID) => bytes.strip_prefix(&[1]),
        _ => None,
    };
    document
        .and_then(|text| serde_json::from_slice::<serde_json::Value>(text).ok())
        .map_or_else(|| opaque(key, bytes), |value| key.digest_leaves(&value))
}

fn opaque(key: &CaptureKey, bytes: &[u8]) -> serde_json::Value {
    serde_json::Value::String(key.digest(b"b", bytes))
}

/// Postgres' binary array: dimensions, flags, element oid, then per dimension a
/// length and lower bound, then each element as a length (`-1` for null) and its
/// bytes. One dimension is decoded element by element; anything else, or
/// anything that does not parse exactly, is captured whole as one opaque leaf.
fn decode_array(key: &CaptureKey, bytes: &[u8]) -> Option<serde_json::Value> {
    let mut reader = Reader(bytes);
    let dimensions = reader.i32()?;
    let _flags = reader.i32()?;
    let element_oid = reader.u32()?;
    if dimensions == 0 {
        return reader
            .is_empty()
            .then(|| serde_json::Value::Array(Vec::new()));
    }
    if dimensions != 1 {
        return None;
    }
    let length = usize::try_from(reader.i32()?).ok()?;
    let _lower_bound = reader.i32()?;
    // Every element takes at least its 4-byte length, so the bytes left bound
    // the count; a header's own claim is not trusted with an allocation.
    let mut items = Vec::with_capacity(length.min(reader.remaining() / 4));
    for _ in 0..length {
        let size = reader.i32()?;
        items.push(if size < 0 {
            serde_json::Value::Null
        } else {
            decode_scalar(
                key,
                reader.take(usize::try_from(size).ok()?)?,
                Some(element_oid),
            )
        });
    }
    reader.is_empty().then_some(serde_json::Value::Array(items))
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, count: usize) -> Option<&'a [u8]> {
        let (head, rest) = (self.0.get(..count)?, self.0.get(count..)?);
        self.0 = rest;
        Some(head)
    }
    fn i32(&mut self) -> Option<i32> {
        Some(i32::from_be_bytes(self.take(4)?.try_into().ok()?))
    }
    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_be_bytes(self.take(4)?.try_into().ok()?))
    }
    fn remaining(&self) -> usize {
        self.0.len()
    }
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
