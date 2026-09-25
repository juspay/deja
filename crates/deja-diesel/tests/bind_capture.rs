//! A query's bind values are captured as structure, as plain JSON.
//!
//! Before this, a db boundary recorded diesel's `debug_query` string, whose
//! `-- binds: [...]` tail is each bind's Rust `Debug`. A map bound as jsonb was
//! rendered in its iteration order, so one value built twice produced two
//! strings, two args hashes and a blocking divergence; the comparison could not
//! see that the two were the same object.

use std::io::Write as _;

use deja_diesel::{capture_query, UNBUILDABLE_STATEMENT};
use deja_runtime::replay::canonical_args_hash;
use diesel::pg::data_types::{PgDate, PgTimestamp};
use diesel::pg::Pg;
use diesel::serialize::{self, IsNull, Output, ToSql};
use diesel::{debug_query, ExpressionMethods, QueryDsl};
use serde_json::json;

#[derive(diesel::sql_types::SqlType, diesel::query_builder::QueryId)]
#[diesel(postgres_type(name = "attempt_status"))]
pub struct AttemptStatus;

/// A host enum. Postgres' binary form of an enum is its label.
#[derive(Debug, Clone, Copy, diesel::AsExpression)]
#[diesel(sql_type = AttemptStatus)]
enum Status {
    Charged,
    Failed,
}

impl ToSql<AttemptStatus, Pg> for Status {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        out.write_all(match self {
            Self::Charged => b"charged",
            Self::Failed => b"failed",
        })?;
        Ok(IsNull::No)
    }
}

/// A host type whose bytes happen to form a valid, empty Postgres array header.
#[derive(diesel::sql_types::SqlType, diesel::query_builder::QueryId)]
#[diesel(postgres_type(name = "opaque_blob"))]
pub struct OpaqueBlob;

#[derive(Debug, Clone, Copy, diesel::AsExpression)]
#[diesel(sql_type = OpaqueBlob)]
struct ZeroHeader;

impl ToSql<OpaqueBlob, Pg> for ZeroHeader {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        out.write_all(&[0; 12])?;
        Ok(IsNull::No)
    }
}

/// A host value that refuses to serialize, with a secret in its error.
#[derive(Debug, Clone, Copy, diesel::AsExpression)]
#[diesel(sql_type = OpaqueBlob)]
struct Unserializable;

impl ToSql<OpaqueBlob, Pg> for Unserializable {
    fn to_sql<'b>(&'b self, _out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        Err("cannot write secret-value-9".into())
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::{AttemptStatus, OpaqueBlob};

    attempt (id) {
        id -> Text,
        algo -> Nullable<Jsonb>,
        tags -> Array<Text>,
        status -> AttemptStatus,
        statuses -> Array<AttemptStatus>,
        email -> Text,
        blob -> OpaqueBlob,
        docs -> Array<Nullable<Jsonb>>,
        raw -> Json,
        amount -> BigInt,
        attempts -> Integer,
        retries -> SmallInt,
        flag -> Bool,
        ratio -> Double,
        created -> Timestamp,
        settled -> Timestamptz,
        weight -> Float,
        due -> Date,
        bytes -> Binary,
    }
}

/// `{"pre_routing_results": {first: .., second: ..}}` with its inner keys in the
/// given order, the way an iterated HashMap serializes.
fn routing(order: [&str; 2]) -> serde_json::Value {
    let mut inner = serde_json::Map::new();
    for method in order {
        inner.insert(method.to_owned(), json!({ "connector": "adyen" }));
    }
    json!({ "algorithm": null, "pre_routing_results": inner })
}

macro_rules! update {
    ($assignments:expr) => {
        diesel::update(attempt::table.filter(attempt::id.eq("a_1"))).set($assignments)
    };
}

/// The defect and its fix in one fixture. The premise is asserted first, so the
/// test cannot pass vacuously: the two debug renderings really do differ.
#[test]
fn one_map_built_in_two_orders_captures_as_one_value() {
    let forward = update!(attempt::algo.eq(Some(routing(["ach", "eft"]))));
    let backward = update!(attempt::algo.eq(Some(routing(["eft", "ach"]))));
    assert_ne!(
        debug_query::<Pg, _>(&forward).to_string(),
        debug_query::<Pg, _>(&backward).to_string(),
        "premise: the rendered binds carry the map's order"
    );

    let (left, right) = (capture_query(&forward), capture_query(&backward));
    assert_eq!(left.sql, right.sql);
    assert_eq!(left.binds, right.binds);
    let args = |sql: &str, binds: &serde_json::Value| json!({ "sql": sql, "binds": binds });
    assert_eq!(
        canonical_args_hash(&args(&left.sql, &left.binds)),
        canonical_args_hash(&args(&right.sql, &right.binds)),
        "the address must not depend on the order either"
    );
}

/// The statement no longer carries its operands.
#[test]
fn the_capture_records_the_statement_without_its_binds() {
    let captured = capture_query(&update!(attempt::email.eq("a@b.co")));
    assert!(!captured.sql.contains("-- binds"), "{}", captured.sql);
    assert!(
        captured.sql.starts_with("UPDATE \"attempt\""),
        "{}",
        captured.sql
    );
}

/// Scalars are the values they are: integers and floats as numbers, booleans,
/// text and host enums as strings, dates and timestamps as ISO text, and a type
/// with no faithful JSON form as its bytes.
#[test]
fn each_scalar_captures_as_its_plain_value() {
    let query = update!((
        attempt::amount.eq(6000_i64),
        attempt::attempts.eq(-3),
        attempt::retries.eq(7_i16),
        attempt::flag.eq(true),
        attempt::ratio.eq(0.5),
        attempt::email.eq("a@b.co"),
        attempt::status.eq(Status::Charged),
        attempt::created.eq(PgTimestamp(86_400_000_001)),
        attempt::due.eq(PgDate(59)),
        attempt::bytes.eq(vec![0_u8, 255]),
        attempt::settled.eq(PgTimestamp(0)),
        attempt::weight.eq(0.25_f32),
    ));
    let binds = capture_query(&query).binds;
    assert_eq!(
        binds,
        json!({
            "$1": 6000,
            "$2": -3,
            "$3": 7,
            "$4": true,
            "$5": 0.5,
            "$6": "a@b.co",
            "$7": "charged",
            "$8": "2000-01-02T00:00:00.000001",
            "$9": "2000-02-29",
            "$10": "\\x00ff",
            "$11": "2000-01-01T00:00:00.000000Z",
            "$12": 0.25,
            "$13": "a_1",
        })
    );
}

/// Dates before Postgres' epoch and the two infinities.
#[test]
fn dates_and_timestamps_render_across_their_whole_range() {
    let due = |days| capture_query(&update!(attempt::due.eq(PgDate(days)))).binds["$1"].clone();
    assert_eq!(due(0), json!("2000-01-01"));
    assert_eq!(due(-1), json!("1999-12-31"));
    assert_eq!(due(i32::MAX), json!("infinity"));
    assert_eq!(due(i32::MIN), json!("-infinity"));
    // A century year is a leap year only when divisible by 400.
    assert_eq!(due(36_584), json!("2100-03-01"));
    assert_eq!(due(-36_465), json!("1900-03-01"));
    let created = |micros| {
        capture_query(&update!(attempt::created.eq(PgTimestamp(micros)))).binds["$1"].clone()
    };
    assert_eq!(created(-1), json!("1999-12-31T23:59:59.999999"));
    assert_eq!(created(i64::MIN), json!("-infinity"));
}

/// Swapping values between keys is a real difference and must stay one.
#[test]
fn a_value_swap_inside_a_bound_object_is_not_absorbed() {
    let left = capture_query(&update!(attempt::algo.eq(Some(json!({ "a": 1, "b": 2 })))));
    let right = capture_query(&update!(attempt::algo.eq(Some(json!({ "a": 2, "b": 1 })))));
    assert_ne!(left.binds, right.binds);
}

/// A bound array keeps its order, so a reordered set is still a permutation the
/// comparison can see and name, not a silent equality.
#[test]
fn a_bound_array_keeps_its_order_as_an_array() {
    let tags = |items: [&str; 2]| update!(attempt::tags.eq(items.map(str::to_owned).to_vec()));
    let left = capture_query(&tags(["x", "y"])).binds;
    let right = capture_query(&tags(["y", "x"])).binds;
    assert_eq!(left["$1"], json!(["x", "y"]));
    assert_eq!(right["$1"], json!(["y", "x"]));
}

/// An integer IN-list stays a list of numbers, in the order it was bound.
#[test]
fn an_integer_list_stays_numbers_in_order() {
    let query = attempt::table.filter(attempt::attempts.eq_any(vec![3, 1, 2]));
    assert_eq!(capture_query(&query).binds["$1"], json!([3, 1, 2]));
}

/// The webhook status sets: a `HashSet` of enum variants inside a struct bound
/// as json, which serde writes as an array of strings in the set's iteration
/// order. Two orders are the same members in different positions, and every
/// member is a string, so the array is not all-numeric: it is the shape the
/// scorer compares as a multiset rather than in order.
#[test]
fn a_status_set_captures_as_an_array_of_strings_in_either_order() {
    let details = |statuses: [&str; 2]| json!({ "webhook_version": "1.0.1", "payment_statuses_enabled": statuses });
    let left = capture_query(&update!(attempt::raw.eq(details(["failed", "succeeded"])))).binds;
    let right = capture_query(&update!(attempt::raw.eq(details(["succeeded", "failed"])))).binds;
    let set = |image: &serde_json::Value| image["$1"]["payment_statuses_enabled"].clone();
    assert_eq!(set(&left), json!(["failed", "succeeded"]));
    assert_eq!(set(&right), json!(["succeeded", "failed"]));
    assert_eq!(sort_every_array(&left), sort_every_array(&right));
    assert!(set(&left)
        .as_array()
        .is_some_and(|members| members.iter().all(serde_json::Value::is_string)));
}

/// A host enum, and an array of one, capture without a connection to look their
/// type up, as the labels Postgres receives.
#[test]
fn a_custom_enum_and_an_array_of_one_capture_as_their_labels() {
    let query = update!((
        attempt::status.eq(Status::Charged),
        attempt::statuses.eq(vec![Status::Charged, Status::Failed]),
    ));
    let binds = capture_query(&query).binds;
    assert_eq!(binds["$1"], json!("charged"));
    assert_eq!(binds["$2"], json!(["charged", "failed"]));
}

/// Both booleans, since a decoder that reads any byte as true would pass on one.
#[test]
fn both_booleans_capture_as_themselves() {
    let flag = |value| capture_query(&update!(attempt::flag.eq(value))).binds["$1"].clone();
    assert_eq!(flag(true), json!(true));
    assert_eq!(flag(false), json!(false));
}

/// A null bind is a null: absence stays visible.
#[test]
fn a_null_bind_captures_as_null() {
    let binds = capture_query(&update!(attempt::algo.eq(None::<serde_json::Value>))).binds;
    assert!(binds["$1"].is_null(), "{binds}");
    assert_eq!(binds["$2"], json!("a_1"));
}

/// Whether a bind is an array comes from its type, never from its bytes. A host
/// scalar whose bytes parse as an empty array is still a scalar.
#[test]
fn a_host_scalar_is_never_decoded_as_an_array() {
    let binds = capture_query(&update!(attempt::blob.eq(ZeroHeader))).binds;
    assert!(binds["$1"].is_string(), "{binds}");
}

/// The real shape of `order_details` and `frm_config`: an array of nullable
/// jsonb. Each document keeps its structure, a null element stays null in its
/// position, and a map's order inside an element is not a difference.
#[test]
fn an_array_of_nullable_jsonb_keeps_each_document_and_each_null() {
    let docs = |items: Vec<Option<serde_json::Value>>| update!(attempt::docs.eq(items));
    let image = capture_query(&docs(vec![Some(routing(["ach", "eft"])), None])).binds;
    let items = image["$1"].as_array().expect("an array");
    assert_eq!(items.len(), 2);
    assert!(
        items[0]["pre_routing_results"]["ach"].is_object(),
        "{image}"
    );
    assert!(items[1].is_null(), "{image}");
    let reordered = capture_query(&docs(vec![Some(routing(["eft", "ach"])), None])).binds;
    assert_eq!(image, reordered);
}

/// `json`, not only `jsonb`, is decoded as a document, and its numbers stay numbers.
#[test]
fn a_json_bind_is_decoded_as_a_document() {
    let binds = capture_query(&update!(attempt::raw.eq(json!({ "k": [1, 2] })))).binds;
    assert_eq!(binds["$1"], json!({ "k": [1, 2] }));
}

#[derive(diesel::AsChangeset)]
#[diesel(table_name = attempt)]
struct EmailPatch {
    email: Option<String>,
}

/// A statement diesel cannot build (an empty changeset) is captured, not
/// panicked on, and names its failed step.
#[test]
fn a_statement_that_does_not_build_is_captured_without_panicking() {
    let captured = capture_query(&update!(EmailPatch { email: None }));
    assert_eq!(captured.sql, UNBUILDABLE_STATEMENT);
    assert_eq!(captured.binds, json!({ "capture_failed": "statement" }));
}

/// A bind that fails to serialize leaves the statement, which built, and names
/// the failed step. The error's text, which can carry a value, is not captured.
#[test]
fn a_bind_that_does_not_serialize_keeps_the_statement_and_names_the_step() {
    let captured = capture_query(&update!(attempt::blob.eq(Unserializable)));
    assert!(
        captured.sql.starts_with("UPDATE \"attempt\""),
        "{}",
        captured.sql
    );
    assert_eq!(captured.binds, json!({ "capture_failed": "binds" }));
}

/// Which placeholder a value fills is part of the query. The binds are keyed by
/// placeholder rather than listed, so an array rule that forgives order can
/// never make two values traded between placeholders read as one query.
#[test]
fn values_traded_between_placeholders_are_a_different_query() {
    let set = |email: &str, id: &str| {
        diesel::update(attempt::table.filter(attempt::id.eq(id.to_owned())))
            .set(attempt::email.eq(email.to_owned()))
    };
    let left = capture_query(&set("x", "y")).binds;
    let right = capture_query(&set("y", "x")).binds;
    // Read the values whatever the binds' shape, so the property below, not a
    // shape check, is what fails if the binds go back to being a list.
    let values = |image: &serde_json::Value| -> Vec<String> {
        let mut values: Vec<String> = match image {
            serde_json::Value::Object(map) => map.values().map(ToString::to_string).collect(),
            serde_json::Value::Array(items) => items.iter().map(ToString::to_string).collect(),
            other => panic!("binds is neither an object nor a list: {other}"),
        };
        values.sort();
        values
    };
    assert_eq!(
        values(&left),
        values(&right),
        "premise: the same values, traded"
    );
    // The scorer forgives array order by sorting every array before it compares.
    // The two captures must still differ under that rule.
    assert_ne!(sort_every_array(&left), sort_every_array(&right));
}

fn sort_every_array(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(items) => {
            let mut items: Vec<_> = items.iter().map(sort_every_array).collect();
            items.sort_by_key(ToString::to_string);
            serde_json::Value::Array(items)
        }
        serde_json::Value::Object(map) => serde_json::Value::Object(
            map.iter()
                .map(|(key, item)| (key.clone(), sort_every_array(item)))
                .collect(),
        ),
        scalar => scalar.clone(),
    }
}
