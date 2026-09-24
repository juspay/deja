//! A query's bind values are captured as structure, with every scalar digested.
//!
//! Before this, a db boundary recorded diesel's `debug_query` string, whose
//! `-- binds: [...]` tail is each bind's Rust `Debug`. A map bound as jsonb was
//! rendered in its iteration order, so one value built twice produced two
//! strings, two args hashes and a blocking divergence; the comparison could not
//! see that the two were the same object.

use std::io::Write as _;

use deja_diesel::{capture_query_with, UNBUILDABLE_STATEMENT};
use deja_runtime::capture_key::{CaptureKey, DIGEST_PREFIX};
use deja_runtime::replay::canonical_args_hash;
use diesel::pg::Pg;
use diesel::serialize::{self, IsNull, Output, ToSql};
use diesel::{debug_query, ExpressionMethods, QueryDsl};

#[derive(diesel::sql_types::SqlType, diesel::query_builder::QueryId)]
#[diesel(postgres_type(name = "attempt_status"))]
pub struct AttemptStatus;

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
    }
}

fn key() -> CaptureKey {
    CaptureKey::new(b"test-capture-key").expect("non-empty")
}

/// `{"pre_routing_results": {first: .., second: ..}}` with its inner keys in the
/// given order, the way an iterated HashMap serializes.
fn routing(order: [&str; 2]) -> serde_json::Value {
    let mut inner = serde_json::Map::new();
    for method in order {
        inner.insert(
            method.to_owned(),
            serde_json::json!({ "connector": "adyen" }),
        );
    }
    serde_json::json!({ "algorithm": null, "pre_routing_results": inner })
}

macro_rules! update_algo {
    ($value:expr) => {
        diesel::update(attempt::table.filter(attempt::id.eq("a_1")))
            .set(attempt::algo.eq(Some($value)))
    };
}

/// The defect and its fix in one fixture. The premise is asserted first, so the
/// test cannot pass vacuously: the two debug renderings really do differ.
#[test]
fn one_map_built_in_two_orders_captures_as_one_value() {
    let forward = update_algo!(routing(["ach", "eft"]));
    let backward = update_algo!(routing(["eft", "ach"]));
    assert_ne!(
        debug_query::<Pg, _>(&forward).to_string(),
        debug_query::<Pg, _>(&backward).to_string(),
        "premise: the rendered binds carry the map's order"
    );

    let key = key();
    let (left, right) = (
        capture_query_with(Some(&key), &forward),
        capture_query_with(Some(&key), &backward),
    );
    assert_eq!(left.sql, right.sql);
    let (left_binds, right_binds) = (left.binds.expect("keyed"), right.binds.expect("keyed"));
    assert_eq!(left_binds, right_binds);
    let args =
        |sql: &str, binds: &serde_json::Value| serde_json::json!({ "sql": sql, "binds": binds });
    assert_eq!(
        canonical_args_hash(&args(&left.sql, &left_binds)),
        canonical_args_hash(&args(&right.sql, &right_binds)),
        "the address must not depend on the order either"
    );
}

/// The statement no longer carries its operands.
#[test]
fn a_keyed_capture_records_the_statement_without_its_binds() {
    let captured = capture_query_with(Some(&key()), &update_algo!(routing(["ach", "eft"])));
    assert!(!captured.sql.contains("-- binds"), "{}", captured.sql);
    assert!(
        captured.sql.starts_with("UPDATE \"attempt\""),
        "{}",
        captured.sql
    );
}

/// Swapping values between keys is a real difference and must stay one.
#[test]
fn a_value_swap_inside_a_bound_object_is_not_absorbed() {
    let key = key();
    let left = capture_query_with(
        Some(&key),
        &update_algo!(serde_json::json!({ "a": 1, "b": 2 })),
    );
    let right = capture_query_with(
        Some(&key),
        &update_algo!(serde_json::json!({ "a": 2, "b": 1 })),
    );
    assert_ne!(left.binds, right.binds);
}

/// A bound array keeps its order, so a reordered set is still a permutation the
/// comparison can see and name, not a silent equality.
#[test]
fn a_bound_array_keeps_its_order_as_an_array() {
    let key = key();
    let tags = |items: [&str; 2]| {
        diesel::update(attempt::table.filter(attempt::id.eq("a_1")))
            .set(attempt::tags.eq(items.map(str::to_owned).to_vec()))
    };
    let left = capture_query_with(Some(&key), &tags(["x", "y"]))
        .binds
        .expect("keyed");
    let right = capture_query_with(Some(&key), &tags(["y", "x"]))
        .binds
        .expect("keyed");
    assert_ne!(left, right);
    let items = |image: &serde_json::Value| {
        let mut items: Vec<String> = image["binds"]["$1"]
            .as_array()
            .expect("an array bind captures as an array")
            .iter()
            .map(ToString::to_string)
            .collect();
        items.sort();
        items
    };
    assert_eq!(items(&left), items(&right), "same members, different order");
}

/// A host enum, and an array of one, capture without a connection to look their
/// type up, and still as a scalar and an array.
#[test]
fn a_custom_enum_and_an_array_of_one_capture_without_a_connection() {
    let key = key();
    let query = diesel::update(attempt::table.filter(attempt::id.eq("a_1"))).set((
        attempt::status.eq(Status::Charged),
        attempt::statuses.eq(vec![Status::Charged, Status::Failed]),
    ));
    let image = capture_query_with(Some(&key), &query).binds.expect("keyed");
    let binds = &image["binds"];
    assert!(binds["$1"]
        .as_str()
        .is_some_and(|leaf| leaf.starts_with(DIGEST_PREFIX)));
    assert_eq!(binds["$2"].as_array().map(Vec::len), Some(2));
    let other = capture_query_with(
        Some(&key),
        &diesel::update(attempt::table.filter(attempt::id.eq("a_1"))).set((
            attempt::status.eq(Status::Failed),
            attempt::statuses.eq(vec![Status::Charged, Status::Failed]),
        )),
    );
    assert_ne!(
        Some(image),
        other.binds,
        "a different enum value is a different image"
    );
}

/// No bound value reaches the image in the clear, and the image names its key.
#[test]
fn no_bound_value_reaches_the_image_in_plaintext() {
    let key = key();
    let query = diesel::update(attempt::table.filter(attempt::id.eq("pay_secret_1"))).set((
        attempt::email.eq("a@b.co"),
        attempt::algo.eq(Some(serde_json::json!({ "zip": "560001" }))),
    ));
    let image = capture_query_with(Some(&key), &query).binds.expect("keyed");
    let text = image.to_string();
    for plain in ["a@b.co", "560001", "pay_secret_1"] {
        assert!(!text.contains(plain), "{plain} in {text}");
    }
    assert_eq!(image["key_id"].as_str(), Some(key.id()));
}

/// Without a key the capture is exactly today's: the full debug rendering, whose
/// masking is intact, and no structured binds. A misconfigured recorder must not
/// write structure without digests.
#[test]
fn without_a_key_the_capture_is_the_debug_rendering_and_nothing_else() {
    let query = update_algo!(routing(["ach", "eft"]));
    let captured = capture_query_with(None, &query);
    assert_eq!(captured.sql, debug_query::<Pg, _>(&query).to_string());
    assert!(captured.binds.is_none());
}

/// A null bind is a null, not a digest of nothing: absence stays visible.
#[test]
fn a_null_bind_captures_as_null() {
    let query = diesel::update(attempt::table.filter(attempt::id.eq("a_1")))
        .set(attempt::algo.eq(None::<serde_json::Value>));
    let image = capture_query_with(Some(&key()), &query)
        .binds
        .expect("keyed");
    assert!(image["binds"]["$1"].is_null(), "{image}");
    assert!(
        image["binds"]["$2"].is_string(),
        "the id bind is still captured: {image}"
    );
}

/// Whether a bind is an array comes from its type, never from its bytes. A host
/// scalar whose bytes parse as an empty array is still one opaque leaf.
#[test]
fn a_host_scalar_is_never_decoded_as_an_array() {
    let query = diesel::update(attempt::table.filter(attempt::id.eq("a_1")))
        .set(attempt::blob.eq(ZeroHeader));
    let image = capture_query_with(Some(&key()), &query)
        .binds
        .expect("keyed");
    assert!(
        image["binds"]["$1"]
            .as_str()
            .is_some_and(|leaf| leaf.starts_with(DIGEST_PREFIX)),
        "{image}"
    );
}

/// The real shape of `order_details` and `frm_config`: an array of nullable
/// jsonb. Each document keeps its structure, a null element stays null in its
/// position, and the same documents in another order are another array.
#[test]
fn an_array_of_nullable_jsonb_keeps_each_document_and_each_null() {
    let key = key();
    let docs = |items: Vec<Option<serde_json::Value>>| {
        diesel::update(attempt::table.filter(attempt::id.eq("a_1"))).set(attempt::docs.eq(items))
    };
    let image = capture_query_with(Some(&key), &docs(vec![Some(routing(["ach", "eft"])), None]))
        .binds
        .expect("keyed");
    let items = image["binds"]["$1"].as_array().expect("an array");
    assert_eq!(items.len(), 2);
    assert!(
        items[0]["pre_routing_results"]["ach"].is_object(),
        "{image}"
    );
    assert!(items[1].is_null(), "{image}");
    let reordered =
        capture_query_with(Some(&key), &docs(vec![Some(routing(["eft", "ach"])), None])).binds;
    assert_eq!(
        Some(image),
        reordered,
        "a map's order inside an element is not a difference"
    );
}

/// `json`, not only `jsonb`, is decoded as a document.
#[test]
fn a_json_bind_is_decoded_as_a_document() {
    let query = diesel::update(attempt::table.filter(attempt::id.eq("a_1")))
        .set(attempt::raw.eq(serde_json::json!({ "k": [1, 2] })));
    let image = capture_query_with(Some(&key()), &query)
        .binds
        .expect("keyed");
    assert_eq!(
        image["binds"]["$1"]["k"].as_array().map(Vec::len),
        Some(2),
        "{image}"
    );
}

#[derive(diesel::AsChangeset)]
#[diesel(table_name = attempt)]
struct EmailPatch {
    email: Option<String>,
}

/// A statement diesel cannot build (an empty changeset) is captured, not
/// panicked on, with or without a key, and the keyed failure names its step and
/// writes no operand.
#[test]
fn a_statement_that_does_not_build_is_captured_without_panicking() {
    let query = diesel::update(attempt::table.filter(attempt::id.eq("pay_secret_1")))
        .set(EmailPatch { email: None });
    let keyed = capture_query_with(Some(&key()), &query);
    assert_eq!(keyed.sql, UNBUILDABLE_STATEMENT);
    let image = keyed.binds.expect("keyed");
    assert_eq!(image["capture_failed"].as_str(), Some("statement"));
    assert!(!image.to_string().contains("pay_secret_1"));
    assert_eq!(capture_query_with(None, &query).sql, UNBUILDABLE_STATEMENT);
}

/// A bind that fails to serialize leaves the statement, which built, and names
/// the failed step. Neither the debug rendering nor the error's text, both of
/// which can carry a value, reaches the capture.
#[test]
fn a_bind_that_does_not_serialize_keeps_the_statement_and_no_operand() {
    let query = diesel::update(attempt::table.filter(attempt::id.eq("pay_secret_1")))
        .set(attempt::blob.eq(Unserializable));
    let captured = capture_query_with(Some(&key()), &query);
    assert!(
        captured.sql.starts_with("UPDATE \"attempt\""),
        "{}",
        captured.sql
    );
    assert!(!captured.sql.contains("-- binds"), "{}", captured.sql);
    let image = captured.binds.expect("keyed");
    assert_eq!(image["capture_failed"].as_str(), Some("binds"));
    let text = image.to_string();
    assert!(
        !text.contains("secret-value-9") && !text.contains("pay_secret_1"),
        "{text}"
    );
}

/// Which placeholder a value fills is part of the query. The binds are keyed by
/// placeholder rather than listed, so an array rule that forgives order can
/// never make two values traded between placeholders read as one query.
#[test]
fn values_traded_between_placeholders_are_a_different_query() {
    let key = key();
    let set = |email: &str, id: &str| {
        diesel::update(attempt::table.filter(attempt::id.eq(id.to_owned())))
            .set(attempt::email.eq(email.to_owned()))
    };
    let left = capture_query_with(Some(&key), &set("x", "y"))
        .binds
        .expect("keyed");
    let right = capture_query_with(Some(&key), &set("y", "x"))
        .binds
        .expect("keyed");
    let binds = |image: &serde_json::Value| image["binds"].as_object().cloned().expect("an object");
    assert_eq!(
        binds(&left).keys().collect::<Vec<_>>(),
        ["$1", "$2"],
        "one key per placeholder, in placeholder order"
    );
    let mut left_values: Vec<_> = binds(&left).values().map(ToString::to_string).collect();
    let mut right_values: Vec<_> = binds(&right).values().map(ToString::to_string).collect();
    left_values.sort();
    right_values.sort();
    assert_eq!(
        left_values, right_values,
        "premise: the same values, traded"
    );
    assert_ne!(left, right);
}
