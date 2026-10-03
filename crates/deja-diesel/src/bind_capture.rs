//! Capture a query's bind values as structure rather than as diesel's
//! `-- binds: [...]` rendering.
//!
//! `debug_query` renders each bind with its Rust `Debug`, so a map bound as
//! jsonb reaches the tape as text in its iteration order, and two builds of one
//! value compare as two different strings. Here the binds are collected as the
//! bytes diesel sends to Postgres and decoded by type into plain JSON: json and
//! jsonb become documents, arrays become arrays, and scalars become the numbers,
//! strings and booleans they are.

use diesel::pg::{Pg, PgMetadataLookup, PgQueryBuilder, PgTypeMetadata};
use diesel::query_builder::bind_collector::RawBytesBindCollector;
use diesel::query_builder::{QueryBuilder as _, QueryFragment};

/// What a db boundary records about the query it ran.
#[derive(Debug, Clone, PartialEq)]
pub struct CapturedQuery {
    /// The statement alone, never its operands.
    pub sql: String,
    /// `{"$1": .., "$2": ..}`, each bind keyed by the placeholder it fills, or
    /// `{"capture_failed": step}` naming the step that failed.
    pub binds: serde_json::Value,
}

/// Capture `query` as its statement and its bind values.
pub fn capture_query<Q: QueryFragment<Pg>>(query: &Q) -> CapturedQuery {
    match statement_and_binds(query) {
        Ok((sql, collector)) => CapturedQuery {
            sql,
            // Keyed by placeholder, not listed: a bind's position is part of
            // what the statement means, and an object is compared and hashed by
            // key, so no rule that forgives array order can make `$1`/`$2`
            // traded read as the same query.
            binds: serde_json::Value::Object(
                collector
                    .binds
                    .iter()
                    .zip(&collector.metadata)
                    .enumerate()
                    .map(|(index, (bytes, metadata))| {
                        (
                            format!("${}", index + 1),
                            decode_bind(bytes.as_deref(), metadata),
                        )
                    })
                    .collect(),
            ),
        },
        // Capture never fails the query, and a failure writes no operand: the
        // failed step is named, not the error's text, which can carry a value.
        Err(failure) => CapturedQuery {
            sql: failure.statement,
            binds: serde_json::json!({ "capture_failed": failure.step }),
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

/// Stand-ins for the OIDs of host-defined types. A real lookup would query the
/// database; capture has no connection and needs none. A host enum's binary
/// form is its label, so it decodes as text either way. The array stand-in has
/// to be non-zero only because a zero array oid is how diesel marks a bind that
/// IS an array; a host scalar must not read as one.
const HOST_TYPE_OID: u32 = u32::MAX - 1;
const HOST_ARRAY_OID: u32 = u32::MAX;

struct NoConnectionLookup;

impl PgMetadataLookup for NoConnectionLookup {
    fn lookup_type(&mut self, _type_name: &str, _schema: Option<&str>) -> PgTypeMetadata {
        PgTypeMetadata::new(HOST_TYPE_OID, HOST_ARRAY_OID)
    }
}

mod oid {
    pub const BOOL: u32 = 16;
    pub const NAME: u32 = 19;
    pub const INT8: u32 = 20;
    pub const INT2: u32 = 21;
    pub const INT4: u32 = 23;
    pub const TEXT: u32 = 25;
    pub const JSON: u32 = 114;
    pub const FLOAT4: u32 = 700;
    pub const FLOAT8: u32 = 701;
    pub const BPCHAR: u32 = 1042;
    pub const VARCHAR: u32 = 1043;
    pub const DATE: u32 = 1082;
    pub const TIMESTAMP: u32 = 1114;
    pub const TIMESTAMPTZ: u32 = 1184;
    pub const JSONB: u32 = 3802;
}

fn decode_bind(bytes: Option<&[u8]>, metadata: &PgTypeMetadata) -> serde_json::Value {
    let Some(bytes) = bytes else {
        return serde_json::Value::Null;
    };
    // diesel describes an array bind as `(array oid, 0)`: an array type has no
    // array type of its own. Every scalar type here has one.
    if matches!(metadata.array_oid(), Ok(0)) {
        return decode_array(bytes).unwrap_or_else(|| opaque(bytes));
    }
    decode_scalar(bytes, metadata.oid().ok())
}

/// A scalar in Postgres' binary form, as the plain JSON value it is. Anything
/// that does not decode exactly is kept as its bytes, never guessed at.
fn decode_scalar(bytes: &[u8], oid: Option<u32>) -> serde_json::Value {
    use serde_json::Value;
    let decoded = match oid {
        Some(oid::JSON) => serde_json::from_slice(bytes).ok(),
        // jsonb's binary form is a version byte, then the JSON text.
        Some(oid::JSONB) => bytes
            .strip_prefix(&[1])
            .and_then(|text| serde_json::from_slice(text).ok()),
        Some(oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME | HOST_TYPE_OID) => {
            std::str::from_utf8(bytes)
                .ok()
                .map(|text| Value::String(text.to_owned()))
        }
        Some(oid::BOOL) => match bytes {
            [0] => Some(Value::Bool(false)),
            [1] => Some(Value::Bool(true)),
            _ => None,
        },
        Some(oid::INT2) => fixed(bytes).map(|b| Value::from(i16::from_be_bytes(b))),
        Some(oid::INT4) => fixed(bytes).map(|b| Value::from(i32::from_be_bytes(b))),
        Some(oid::INT8) => fixed(bytes).map(|b| Value::from(i64::from_be_bytes(b))),
        Some(oid::FLOAT4) => fixed(bytes).map(|b| float(f64::from(f32::from_be_bytes(b)))),
        Some(oid::FLOAT8) => fixed(bytes).map(|b| float(f64::from_be_bytes(b))),
        Some(oid::DATE) => fixed(bytes).map(|b| Value::String(date(i32::from_be_bytes(b)))),
        Some(oid::TIMESTAMP) => {
            fixed(bytes).map(|b| Value::String(timestamp(i64::from_be_bytes(b), "")))
        }
        Some(oid::TIMESTAMPTZ) => {
            fixed(bytes).map(|b| Value::String(timestamp(i64::from_be_bytes(b), "Z")))
        }
        // bytea, numeric, and any type not listed: its bytes, faithfully.
        _ => None,
    };
    decoded.unwrap_or_else(|| opaque(bytes))
}

fn fixed<const N: usize>(bytes: &[u8]) -> Option<[u8; N]> {
    bytes.try_into().ok()
}

/// A float as a JSON number, or its name when JSON has no number for it.
fn float(value: f64) -> serde_json::Value {
    serde_json::Number::from_f64(value).map_or_else(
        || serde_json::Value::String(value.to_string()),
        serde_json::Value::Number,
    )
}

/// Bytes kept as bytes, in Postgres' own hex notation.
fn opaque(bytes: &[u8]) -> serde_json::Value {
    use std::fmt::Write as _;
    let mut text = String::with_capacity(2 + bytes.len() * 2);
    text.push_str("\\x");
    for byte in bytes {
        let _ = write!(text, "{byte:02x}");
    }
    serde_json::Value::String(text)
}

/// Postgres dates and timestamps count from 2000-01-01, and mark the two
/// infinities with the extreme values of their integer.
const DAYS_FROM_UNIX_EPOCH_TO_2000: i64 = 10_957;
const MICROS_PER_DAY: i64 = 86_400_000_000;

fn date(days: i32) -> String {
    match days {
        i32::MAX => "infinity".to_owned(),
        i32::MIN => "-infinity".to_owned(),
        days => civil_date(i64::from(days) + DAYS_FROM_UNIX_EPOCH_TO_2000),
    }
}

fn timestamp(micros: i64, zone: &str) -> String {
    match micros {
        i64::MAX => "infinity".to_owned(),
        i64::MIN => "-infinity".to_owned(),
        micros => {
            let days = micros.div_euclid(MICROS_PER_DAY);
            let of_day = micros.rem_euclid(MICROS_PER_DAY);
            format!(
                "{}T{}{zone}",
                civil_date(days + DAYS_FROM_UNIX_EPOCH_TO_2000),
                time_of_day(of_day)
            )
        }
    }
}

fn time_of_day(micros: i64) -> String {
    let (seconds, fraction) = (micros.div_euclid(1_000_000), micros.rem_euclid(1_000_000));
    format!(
        "{:02}:{:02}:{:02}.{fraction:06}",
        seconds / 3600,
        seconds / 60 % 60,
        seconds % 60
    )
}

/// `YYYY-MM-DD` for a count of days since 1970-01-01, proleptic Gregorian.
fn civil_date(days_since_unix_epoch: i64) -> String {
    // Howard Hinnant's days-to-civil.
    let z = days_since_unix_epoch + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * shifted_month + 2) / 5 + 1;
    let month = if shifted_month < 10 {
        shifted_month + 3
    } else {
        shifted_month - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02}")
}

/// Postgres' binary array: dimensions, flags, element oid, then per dimension a
/// length and lower bound, then each element as a length (`-1` for null) and its
/// bytes. One dimension is decoded element by element; anything else, or
/// anything that does not parse exactly, is captured whole as its bytes.
fn decode_array(bytes: &[u8]) -> Option<serde_json::Value> {
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
            decode_scalar(reader.take(usize::try_from(size).ok()?)?, Some(element_oid))
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
