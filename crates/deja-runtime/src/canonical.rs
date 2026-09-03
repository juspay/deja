//! Type-directed capture canonicalisation.
//!
//! # Why this exists
//!
//! A collection that is unordered where it is built — a `HashSet`, a `HashMap` —
//! is serialised into a JSON array or object that carries no record of whether
//! its order ever meant anything. Rust seeds its hasher per PROCESS, so the same
//! members come out in a different order between two runs of the same binary,
//! and every consumer downstream of the bytes — the args hash, the comparator,
//! the dashboard — is left working around a fact that was lost before they saw
//! it. The only fix available to a consumer is to stop trusting array order
//! everywhere, which trades a class of false positives for a class of false
//! negatives: an INTENDED ordering change becomes invisible too.
//!
//! So the order is canonicalised HERE, at capture, while the Rust type is still
//! in hand. Afterwards every array in a tape is genuinely a sequence, and an
//! order difference means what it should mean — somebody changed it.
//!
//! # How the type survives to this point
//!
//! Serde's data model cannot distinguish a set from a sequence: `HashSet` and
//! `Vec` both call `serialize_seq(Some(len))`, and unlike `serialize_struct`
//! that call carries no type name. What it DOES carry is a generic parameter:
//! `SerializeStruct::serialize_field<T>`, `SerializeSeq::serialize_element<T>`,
//! `SerializeMap::serialize_value<T>` and `Serializer::serialize_some<T>` are
//! each generic over the concrete type at that position, and
//! `std::any::type_name::<T>()` requires no bounds at all. So the static type of
//! every nested value is readable here, at arbitrary depth, INSIDE a
//! `#[derive(Serialize)]` impl this crate never sees.
//!
//! # What it is deliberately NOT
//!
//! It is not "sort every array". Sorting an array whose order carries meaning
//! DESTROYS that meaning rather than preserving it: a removed sort on a ranked
//! list would sort back to the same array, invisible in the verdict and
//! unrecoverable from the tape. The type check is exactly what bounds the loss
//! to collections that had no order to lose — which is also why the sort is
//! round-trip safe, since a collection whose order carries nothing deserialises
//! the same from any order.
//!
//! # What it cannot reach, stated so nobody re-derives it
//!
//! Only a collection whose static type is still a set AT THE BOUNDARY. A
//! producer that iterates a `HashMap` into a `Vec` before returning has already
//! lost the fact, and a `Vec<Row>` from a `SELECT` with no `ORDER BY` is
//! unordered in the database's contract while the Rust type says otherwise. Both
//! are producer-side problems and this module correctly leaves both alone.

use serde::ser::{self, Serialize, Serializer};
use serde_json::Value;

/// Type paths whose SEQUENCE serialisation carries no order.
///
/// Matched on the path before the generic arguments, so `HashSet<T>` and
/// `HashSet<T, S>` both hit. `HashMap` is deliberately absent: it serialises
/// through `serialize_map` into a JSON OBJECT, and object key order is
/// insignificant by JSON's own rules (`canonical_args_hash` already sorts keys,
/// and this module emits them in `serde_json::Map` order either way).
///
/// `std::any::type_name` promises no stable format, so
/// [`tests::type_name_strings_are_what_this_module_matches_on`] pins every entry
/// against the real type. If a toolchain ever renames one, that test fails
/// loudly rather than this quietly reverting to "normalise nothing" — and note
/// which way the failure falls: a missed match costs canonicalisation, never a
/// sorted array that should have kept its order.
const UNORDERED_SEQUENCE_TYPES: &[&str] = &[
    "std::collections::hash::set::HashSet",
    // hashbrown is std's own hash table and appears in dependency trees under
    // its own path; the same argument applies to it verbatim.
    "hashbrown::set::HashSet",
];

/// Does the static type at this position serialise as an UNORDERED sequence?
///
/// Public so the pinning test can call it, and so a reader can see that the
/// whole type decision is one string comparison over a fixed list.
#[must_use]
pub fn is_unordered_sequence(type_name: &str) -> bool {
    let name = type_name.trim_start_matches('&');
    let path = name.split('<').next().unwrap_or(name);
    UNORDERED_SEQUENCE_TYPES.contains(&path)
}

/// The canonical order for a multiset of JSON values: by serialised form.
///
/// A SORT, never a dedup, so two collections agree only when their members agree
/// WITH MULTIPLICITY — losing one of two identical members stays a difference.
/// Same rule and same key as the scorer's `sort_as_bag`, deliberately: a tape
/// canonicalised here is already in the order the comparator's `bag_canon` would
/// have put it in, so no third notion of "canonical" enters the system.
fn sort_canonically(items: &mut [Value]) {
    items.sort_by_cached_key(|item| serde_json::to_string(item).unwrap_or_default());
}

/// Serialise `value` to JSON, recording every collection whose static type says
/// its order carries no information in a canonical order.
///
/// Identical to [`serde_json::to_value`] for every value that contains no such
/// collection — asserted by
/// [`tests::a_payload_without_an_unordered_collection_is_byte_identical`], not
/// argued.
///
/// # Errors
///
/// The same failures `serde_json::to_value` reports, and only those: a shape
/// this serialiser cannot express falls back to `serde_json::to_value` rather
/// than failing, so a capture is never worse than it was before this existed.
pub fn to_value<T>(value: &T) -> Result<Value, serde_json::Error>
where
    T: ?Sized + Serialize,
{
    match value.serialize(Canonical::for_type::<T>()) {
        Ok(value) => Ok(value),
        Err(_) => serde_json::to_value(value),
    }
}

/// [`to_value`], with the same never-panic fallback every capture site in this
/// crate already applies. This is the drop-in for a bare
/// `serde_json::to_value(..).unwrap_or(Value::Null)`.
#[must_use]
pub fn to_value_or_null<T>(value: &T) -> Value
where
    T: ?Sized + Serialize,
{
    to_value(value).unwrap_or(Value::Null)
}

/// The serialiser. Carries one bit — whether the value it is about to serialise
/// is, BY ITS STATIC TYPE, an unordered sequence — decided by the position above
/// it, because that is the only place the concrete type is visible.
struct Canonical {
    unordered: bool,
}

impl Canonical {
    fn for_type<T: ?Sized>() -> Self {
        Self {
            unordered: is_unordered_sequence(std::any::type_name::<T>()),
        }
    }

    /// A position that is positional by construction — a tuple, a tuple struct,
    /// a map key — and therefore never canonicalised whatever its type says.
    fn positional() -> Self {
        Self { unordered: false }
    }
}

fn error(message: &str) -> serde_json::Error {
    <serde_json::Error as ser::Error>::custom(message)
}

impl Serializer for Canonical {
    type Ok = Value;
    type Error = serde_json::Error;
    type SerializeSeq = SeqBuilder;
    type SerializeTuple = SeqBuilder;
    type SerializeTupleStruct = SeqBuilder;
    type SerializeTupleVariant = VariantSeqBuilder;
    type SerializeMap = MapBuilder;
    type SerializeStruct = MapBuilder;
    type SerializeStructVariant = VariantMapBuilder;

    fn serialize_bool(self, value: bool) -> Result<Value, Self::Error> {
        Ok(Value::Bool(value))
    }
    fn serialize_i8(self, value: i8) -> Result<Value, Self::Error> {
        Ok(Value::from(value))
    }
    fn serialize_i16(self, value: i16) -> Result<Value, Self::Error> {
        Ok(Value::from(value))
    }
    fn serialize_i32(self, value: i32) -> Result<Value, Self::Error> {
        Ok(Value::from(value))
    }
    fn serialize_i64(self, value: i64) -> Result<Value, Self::Error> {
        Ok(Value::from(value))
    }
    fn serialize_i128(self, value: i128) -> Result<Value, Self::Error> {
        // 128-bit integers only become a `Value` under serde_json's
        // arbitrary_precision feature. Delegate rather than reimplement the
        // feature-conditional path.
        serde_json::to_value(value)
    }
    fn serialize_u8(self, value: u8) -> Result<Value, Self::Error> {
        Ok(Value::from(value))
    }
    fn serialize_u16(self, value: u16) -> Result<Value, Self::Error> {
        Ok(Value::from(value))
    }
    fn serialize_u32(self, value: u32) -> Result<Value, Self::Error> {
        Ok(Value::from(value))
    }
    fn serialize_u64(self, value: u64) -> Result<Value, Self::Error> {
        Ok(Value::from(value))
    }
    fn serialize_u128(self, value: u128) -> Result<Value, Self::Error> {
        serde_json::to_value(value)
    }
    fn serialize_f32(self, value: f32) -> Result<Value, Self::Error> {
        Ok(Value::from(value))
    }
    fn serialize_f64(self, value: f64) -> Result<Value, Self::Error> {
        Ok(Value::from(value))
    }
    fn serialize_char(self, value: char) -> Result<Value, Self::Error> {
        Ok(Value::String(value.to_string()))
    }
    fn serialize_str(self, value: &str) -> Result<Value, Self::Error> {
        Ok(Value::String(value.to_owned()))
    }
    fn serialize_bytes(self, value: &[u8]) -> Result<Value, Self::Error> {
        Ok(Value::Array(
            value.iter().copied().map(Value::from).collect(),
        ))
    }
    fn serialize_none(self) -> Result<Value, Self::Error> {
        Ok(Value::Null)
    }
    fn serialize_some<T>(self, value: &T) -> Result<Value, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        // The `Option` is transparent in JSON, so the type that decides is the
        // one INSIDE it: `Option<HashSet<_>>` canonicalises.
        value.serialize(Self::for_type::<T>())
    }
    fn serialize_unit(self) -> Result<Value, Self::Error> {
        Ok(Value::Null)
    }
    fn serialize_unit_struct(self, _name: &'static str) -> Result<Value, Self::Error> {
        Ok(Value::Null)
    }
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
    ) -> Result<Value, Self::Error> {
        Ok(Value::String(variant.to_owned()))
    }
    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Value, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(Self::for_type::<T>())
    }
    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Value, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        let inner = value.serialize(Self::for_type::<T>())?;
        let mut map = serde_json::Map::with_capacity(1);
        map.insert(variant.to_owned(), inner);
        Ok(Value::Object(map))
    }
    fn serialize_seq(self, len: Option<usize>) -> Result<SeqBuilder, Self::Error> {
        Ok(SeqBuilder {
            items: Vec::with_capacity(len.unwrap_or(0)),
            unordered: self.unordered,
        })
    }
    fn serialize_tuple(self, len: usize) -> Result<SeqBuilder, Self::Error> {
        Ok(SeqBuilder {
            items: Vec::with_capacity(len),
            unordered: false,
        })
    }
    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<SeqBuilder, Self::Error> {
        Ok(SeqBuilder {
            items: Vec::with_capacity(len),
            unordered: false,
        })
    }
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<VariantSeqBuilder, Self::Error> {
        Ok(VariantSeqBuilder {
            variant,
            items: Vec::with_capacity(len),
        })
    }
    fn serialize_map(self, len: Option<usize>) -> Result<MapBuilder, Self::Error> {
        Ok(MapBuilder {
            entries: serde_json::Map::with_capacity(len.unwrap_or(0)),
            pending_key: None,
        })
    }
    fn serialize_struct(self, _name: &'static str, len: usize) -> Result<MapBuilder, Self::Error> {
        Ok(MapBuilder {
            entries: serde_json::Map::with_capacity(len),
            pending_key: None,
        })
    }
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<VariantMapBuilder, Self::Error> {
        Ok(VariantMapBuilder {
            variant,
            entries: serde_json::Map::with_capacity(len),
        })
    }
}

struct SeqBuilder {
    items: Vec<Value>,
    /// Decided by the position ABOVE this sequence, where the concrete type was
    /// visible. `false` for a tuple, which is positional whatever it holds.
    unordered: bool,
}

impl SeqBuilder {
    fn push<T>(&mut self, value: &T) -> Result<(), serde_json::Error>
    where
        T: ?Sized + Serialize,
    {
        self.items
            .push(value.serialize(Canonical::for_type::<T>())?);
        Ok(())
    }

    fn finish(mut self) -> Value {
        if self.unordered {
            sort_canonically(&mut self.items);
        }
        Value::Array(self.items)
    }
}

impl ser::SerializeSeq for SeqBuilder {
    type Ok = Value;
    type Error = serde_json::Error;
    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.push(value)
    }
    fn end(self) -> Result<Value, Self::Error> {
        Ok(self.finish())
    }
}

impl ser::SerializeTuple for SeqBuilder {
    type Ok = Value;
    type Error = serde_json::Error;
    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.push(value)
    }
    fn end(self) -> Result<Value, Self::Error> {
        Ok(self.finish())
    }
}

impl ser::SerializeTupleStruct for SeqBuilder {
    type Ok = Value;
    type Error = serde_json::Error;
    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.push(value)
    }
    fn end(self) -> Result<Value, Self::Error> {
        Ok(self.finish())
    }
}

struct VariantSeqBuilder {
    variant: &'static str,
    items: Vec<Value>,
}

impl ser::SerializeTupleVariant for VariantSeqBuilder {
    type Ok = Value;
    type Error = serde_json::Error;
    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.items
            .push(value.serialize(Canonical::for_type::<T>())?);
        Ok(())
    }
    fn end(self) -> Result<Value, Self::Error> {
        let mut map = serde_json::Map::with_capacity(1);
        map.insert(self.variant.to_owned(), Value::Array(self.items));
        Ok(Value::Object(map))
    }
}

struct MapBuilder {
    entries: serde_json::Map<String, Value>,
    pending_key: Option<String>,
}

/// A map key, by the same rules `serde_json` applies: strings and the scalars
/// that have one unambiguous string form. Anything else is not a JSON object
/// key, and saying so lets [`to_value`] fall back to `serde_json::to_value`,
/// which reports the identical refusal.
fn map_key<T>(key: &T) -> Result<String, serde_json::Error>
where
    T: ?Sized + Serialize,
{
    match key.serialize(Canonical::positional())? {
        Value::String(key) => Ok(key),
        Value::Number(key) => Ok(key.to_string()),
        Value::Bool(key) => Ok(key.to_string()),
        _ => Err(error("key must be a string")),
    }
}

impl ser::SerializeMap for MapBuilder {
    type Ok = Value;
    type Error = serde_json::Error;
    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.pending_key = Some(map_key(key)?);
        Ok(())
    }
    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        let key = self
            .pending_key
            .take()
            .ok_or_else(|| error("value serialized before key"))?;
        self.entries
            .insert(key, value.serialize(Canonical::for_type::<T>())?);
        Ok(())
    }
    fn end(self) -> Result<Value, Self::Error> {
        Ok(Value::Object(self.entries))
    }
}

impl ser::SerializeStruct for MapBuilder {
    type Ok = Value;
    type Error = serde_json::Error;
    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.entries
            .insert(key.to_owned(), value.serialize(Canonical::for_type::<T>())?);
        Ok(())
    }
    fn end(self) -> Result<Value, Self::Error> {
        Ok(Value::Object(self.entries))
    }
}

struct VariantMapBuilder {
    variant: &'static str,
    entries: serde_json::Map<String, Value>,
}

impl ser::SerializeStructVariant for VariantMapBuilder {
    type Ok = Value;
    type Error = serde_json::Error;
    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.entries
            .insert(key.to_owned(), value.serialize(Canonical::for_type::<T>())?);
        Ok(())
    }
    fn end(self) -> Result<Value, Self::Error> {
        let mut map = serde_json::Map::with_capacity(1);
        map.insert(self.variant.to_owned(), Value::Object(self.entries));
        Ok(Value::Object(map))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

    /// The whole type decision is a string comparison against
    /// [`UNORDERED_SEQUENCE_TYPES`], and `std::any::type_name` promises no stable
    /// format. Pin the real strings so a toolchain that renames one fails HERE,
    /// loudly, instead of this module quietly reverting to "normalise nothing".
    #[test]
    fn type_name_strings_are_what_this_module_matches_on() {
        assert_eq!(
            std::any::type_name::<HashSet<String>>(),
            "std::collections::hash::set::HashSet<alloc::string::String>",
            "the HashSet path this module matches on has moved"
        );
        assert!(is_unordered_sequence(std::any::type_name::<HashSet<u8>>()));
        assert!(is_unordered_sequence(std::any::type_name::<
            HashSet<String, std::collections::hash_map::RandomState>,
        >()));
        // A reference is what a nested element position hands us.
        assert!(is_unordered_sequence(
            "&std::collections::hash::set::HashSet<u8>"
        ));

        // Ordered by type, all of them. A false positive here would sort an
        // array whose order is its meaning, which is the one outcome this
        // module must never produce.
        assert!(!is_unordered_sequence(std::any::type_name::<Vec<u8>>()));
        assert!(!is_unordered_sequence(std::any::type_name::<BTreeSet<u8>>()));
        assert!(!is_unordered_sequence(std::any::type_name::<
            BTreeMap<String, u8>,
        >()));
        assert!(!is_unordered_sequence(std::any::type_name::<
            HashMap<String, u8>,
        >()));
        assert!(!is_unordered_sequence("alloc::vec::Vec<MyHashSet>"));
    }

    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    struct Nested {
        tags: HashSet<String>,
        steps: Vec<String>,
        labels: HashMap<String, u8>,
        ranked: BTreeMap<String, u8>,
    }

    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    enum Shape {
        Plain,
        Tagged(u8, u8),
        Named { inner: Vec<u8> },
    }

    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    struct Wrapper(u32);

    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    struct Payload {
        nested: Vec<Nested>,
        maybe: Option<Nested>,
        shape: Shape,
        wrapper: Wrapper,
        pair: (u8, String),
        nothing: Option<u8>,
        text: String,
        number: f64,
        flag: bool,
    }

    fn nested(tags: &[&str], steps: &[&str]) -> Nested {
        Nested {
            tags: tags.iter().map(|t| (*t).to_owned()).collect(),
            steps: steps.iter().map(|s| (*s).to_owned()).collect(),
            labels: [("b".to_owned(), 2u8), ("a".to_owned(), 1)]
                .into_iter()
                .collect(),
            ranked: [("y".to_owned(), 9u8), ("x".to_owned(), 8)]
                .into_iter()
                .collect(),
        }
    }

    fn payload(tags: &[&str], steps: &[&str]) -> Payload {
        Payload {
            nested: vec![nested(tags, steps)],
            maybe: Some(nested(tags, steps)),
            shape: Shape::Named {
                inner: vec![3, 1, 2],
            },
            wrapper: Wrapper(7),
            pair: (1, "one".to_owned()),
            nothing: None,
            text: "t".to_owned(),
            number: 1.5,
            flag: true,
        }
    }

    /// PROPERTY (§8.2): a value carrying no unordered collection captures
    /// EXACTLY as it did before this module existed. Asserted on the rendered
    /// bytes, not on `Value` equality, because `serde_json::Map` compares
    /// order-insensitively and would hide a key-order change.
    #[test]
    fn a_payload_without_an_unordered_collection_is_byte_identical() {
        #[derive(serde::Serialize)]
        struct NoSets {
            steps: Vec<String>,
            labels: HashMap<String, u8>,
            ranked: BTreeMap<String, u8>,
            shape: Shape,
            wrapper: Wrapper,
            pair: (u8, String),
            bytes: Vec<u8>,
            nothing: Option<u8>,
        }
        let value = NoSets {
            steps: vec!["z".into(), "a".into(), "m".into()],
            labels: [("b".to_owned(), 2u8), ("a".to_owned(), 1)]
                .into_iter()
                .collect(),
            ranked: [("y".to_owned(), 9u8)].into_iter().collect(),
            shape: Shape::Tagged(2, 1),
            wrapper: Wrapper(7),
            pair: (1, "one".to_owned()),
            bytes: vec![3, 1, 2],
            nothing: None,
        };
        let before = serde_json::to_value(&value).expect("serde_json");
        let after = to_value(&value).expect("canonical");
        assert_eq!(
            serde_json::to_string(&before).expect("render"),
            serde_json::to_string(&after).expect("render"),
        );
    }

    /// PROPERTY (§8.2 again, the half that matters): a `Vec` keeps its order
    /// even when its members would sort differently. This is the assertion that
    /// separates this design from "sort every array", and it must fail if
    /// anybody ever widens the type list to cover sequences generally.
    #[test]
    fn a_sequence_keeps_its_order() {
        let steps = vec!["z".to_owned(), "a".to_owned(), "m".to_owned()];
        assert_eq!(
            to_value(&steps).expect("canonical"),
            serde_json::json!(["z", "a", "m"]),
        );
    }

    /// The point of the module: two equal sets built in different orders capture
    /// identically, whatever this process's hash seed did.
    #[test]
    fn an_unordered_collection_is_recorded_in_a_canonical_order() {
        let forward: HashSet<String> = ["visa", "mastercard", "amex", "discover"]
            .iter()
            .map(|s| (*s).to_owned())
            .collect();
        let backward: HashSet<String> = ["discover", "amex", "mastercard", "visa"]
            .iter()
            .map(|s| (*s).to_owned())
            .collect();
        assert_eq!(
            to_value(&forward).expect("canonical"),
            serde_json::json!(["amex", "discover", "mastercard", "visa"]),
        );
        assert_eq!(
            to_value(&forward).expect("canonical"),
            to_value(&backward).expect("canonical"),
        );
    }

    /// Nesting is the whole reason this is a `Serializer` and not an autoref arm
    /// on the outermost value: the set here is four levels down, inside a
    /// derived `Serialize` impl this crate never sees.
    #[test]
    fn a_nested_unordered_collection_is_canonicalised_at_its_own_depth() {
        let one = to_value(&payload(&["c", "a", "b"], &["z", "a"])).expect("canonical");
        let two = to_value(&payload(&["b", "c", "a"], &["z", "a"])).expect("canonical");
        assert_eq!(one, two, "same members, different insertion order");
        assert_eq!(one["nested"][0]["tags"], serde_json::json!(["a", "b", "c"]));
        assert_eq!(one["maybe"]["tags"], serde_json::json!(["a", "b", "c"]));
        // …and the `Vec` beside it is untouched at the same depth.
        assert_eq!(one["nested"][0]["steps"], serde_json::json!(["z", "a"]));
    }

    /// PROPERTY (§8.1): reconstruct fidelity. Canonicalising is only safe
    /// because we sort exactly the collections whose deserialization ignores
    /// order — so the capture still rebuilds the value the boundary returned.
    #[test]
    fn a_canonicalised_capture_round_trips_to_the_same_value() {
        let original = payload(&["c", "a", "b"], &["z", "a"]);
        let captured = to_value(&original).expect("canonical");
        let rebuilt: Payload = serde_json::from_value(captured).expect("reconstruct");
        assert_eq!(rebuilt, original);
    }

    /// PROPERTY (§8.5): a SORT, never a dedup. Two DISTINCT members that
    /// serialize to the same JSON must both survive, or a collection that lost
    /// one of them would compare equal to one that did not.
    #[test]
    fn members_that_serialize_alike_are_both_kept() {
        #[derive(PartialEq, Eq, Hash)]
        enum Collapsing {
            First,
            Second,
        }
        // Deliberately lossy: both variants render as the same string, which is
        // what makes multiplicity observable at all.
        impl serde::Serialize for Collapsing {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_str("same")
            }
        }
        let set: HashSet<Collapsing> = [Collapsing::First, Collapsing::Second]
            .into_iter()
            .collect();
        assert_eq!(
            to_value(&set).expect("canonical"),
            serde_json::json!(["same", "same"]),
        );
    }

    /// THE PAYOFF, asserted directly rather than described: the args hash puts
    /// array elements in the key positionally (`hash_value`), so before this
    /// module two equal sets produced two different lookup keys and the call
    /// missed at every address rank. After it they agree.
    #[test]
    fn two_equal_sets_now_hash_to_one_lookup_key() {
        let forward: HashSet<u32> = (0..24).collect();
        let backward: HashSet<u32> = (0..24).rev().collect();
        let plain_forward = serde_json::to_value(&forward).expect("serde_json");
        let plain_backward = serde_json::to_value(&backward).expect("serde_json");
        let canon_forward = to_value(&forward).expect("canonical");
        let canon_backward = to_value(&backward).expect("canonical");

        assert_eq!(
            crate::replay::canonical_args_hash(&canon_forward),
            crate::replay::canonical_args_hash(&canon_backward),
            "canonicalised captures of one set must produce one key"
        );
        // The old behaviour, kept in the test so the property is not vacuous:
        // if the two plain captures happened to agree, the assertion above
        // proved nothing about ordering, and this says so.
        if plain_forward != plain_backward {
            assert_ne!(
                crate::replay::canonical_args_hash(&plain_forward),
                crate::replay::canonical_args_hash(&plain_backward),
                "an uncanonicalised permutation is what moved the key"
            );
        }
    }

    /// PROPERTY (§8, fail-open): a shape this serializer cannot express falls
    /// back to `serde_json::to_value` and reports the same refusal, so a capture
    /// is never worse than it was before this module existed — and never panics.
    #[test]
    fn a_shape_this_cannot_express_reports_what_serde_json_reports() {
        let map: BTreeMap<(u8, u8), u8> = [((1, 2), 3)].into_iter().collect();
        assert!(
            serde_json::to_value(&map).is_err(),
            "precondition: serde_json refuses a non-string map key"
        );
        assert!(to_value(&map).is_err(), "and so does this, by falling back");
        assert_eq!(to_value_or_null(&map), serde_json::Value::Null);
    }

    /// Integer and boolean map keys are accepted, exactly as `serde_json`
    /// accepts them — the fallback must not be reached for shapes that already
    /// worked, or a capture would pay a double serialization on a normal path.
    #[test]
    fn scalar_map_keys_capture_as_serde_json_captures_them() {
        let ints: BTreeMap<u32, &str> = [(2, "b"), (1, "a")].into_iter().collect();
        assert_eq!(
            to_value(&ints).expect("canonical"),
            serde_json::to_value(&ints).expect("serde_json"),
        );
        let bools: BTreeMap<bool, u8> = [(true, 1), (false, 0)].into_iter().collect();
        assert_eq!(
            to_value(&bools).expect("canonical"),
            serde_json::to_value(&bools).expect("serde_json"),
        );
    }
}
