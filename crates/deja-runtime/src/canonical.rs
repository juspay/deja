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

use serde::de::{self, DeserializeOwned, Deserializer, IntoDeserializer, Visitor};
use serde::ser::{self, Serialize, Serializer};
use serde_json::Value;

/// Type paths whose SEQUENCE serialisation carries no order.
///
/// Matched on the path before the generic arguments, so `HashSet<T>` and
/// `HashSet<T, S>` both hit. `HashMap` is not a sequence; its keys are handled
/// by [`is_unordered_map`].
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

/// Type paths whose MAP serialisation carries no key order.
///
/// An ordered map is absent on purpose. `serde_json::Map` built with
/// `preserve_order`, or an `IndexMap`, holds an order its builder chose, and a
/// recorded value is handed back to the service on replay: sorting it here
/// would return something other than what was recorded.
const UNORDERED_MAP_TYPES: &[&str] = &[
    "std::collections::hash::map::HashMap",
    "hashbrown::map::HashMap",
];

/// Does the static type at this position serialise as a map with no key order?
#[must_use]
pub fn is_unordered_map(type_name: &str) -> bool {
    let name = type_name.trim_start_matches('&');
    let path = name.split('<').next().unwrap_or(name);
    UNORDERED_MAP_TYPES.contains(&path)
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
/// It also records a present `Option` whose contents serialise to `null` — a
/// cache hit holding `None` — under [`PRESENT_KEY`], so replay can tell
/// `Some(None)` from `None`. Arguments and results share this one encoding.
///
/// Identical to [`serde_json::to_value`] for every value that contains neither
/// such a collection nor such an `Option` — asserted by
/// [`tests::a_payload_without_an_unordered_collection_is_byte_identical`] and
/// `tests::a_present_value_that_is_not_null_records_as_it_always_did`, not
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

/// The serialiser. Carries whether the value it is about to serialise is, BY
/// ITS STATIC TYPE, an unordered collection — decided by the position above it,
/// because that is the only place the concrete type is visible.
struct Canonical {
    unordered: bool,
    unordered_map: bool,
}

impl Canonical {
    fn for_type<T: ?Sized>() -> Self {
        let type_name = std::any::type_name::<T>();
        Self {
            unordered: is_unordered_sequence(type_name),
            unordered_map: is_unordered_map(type_name),
        }
    }

    /// A position that is positional by construction — a tuple, a tuple struct,
    /// a map key — and therefore never canonicalised whatever its type says.
    fn positional() -> Self {
        Self {
            unordered: false,
            unordered_map: false,
        }
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
    type SerializeStruct = StructBuilder;
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
        let inner = value.serialize(Self::for_type::<T>())?;
        Ok(mark_if_ambiguous(inner))
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
            entries: Vec::with_capacity(len.unwrap_or(0)),
            pending_key: None,
            sort_keys: self.unordered_map,
        })
    }
    fn serialize_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<StructBuilder, Self::Error> {
        Ok(StructBuilder {
            entries: serde_json::Map::with_capacity(len),
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

/// The builder for a MAP. A map that is unordered BY ITS STATIC TYPE is emitted
/// in key order: a consumer may build `serde_json` with `preserve_order`, and
/// then a `HashMap`'s per-process hash order would reach the tape as object key
/// order. A map whose type keeps an order is emitted as it arrived, because the
/// recorded value is what replay hands back. Structs do not go through this:
/// their field order is declaration order, so [`StructBuilder`] inserts directly.
struct MapBuilder {
    entries: Vec<(String, Value)>,
    pending_key: Option<String>,
    sort_keys: bool,
}

/// The builder for a STRUCT. Field names are static and arrive in declaration
/// order, so the key order on the tape is the same on every run whether or not
/// `serde_json::Map` preserves insertion order.
struct StructBuilder {
    entries: serde_json::Map<String, Value>,
}

/// Put map entries in key order. `serde_json` keeps the LAST value for a
/// duplicate key; the stable sort keeps duplicates in arrival order so the
/// insert loop in [`finish_map`] does the same.
fn sort_map_entries(entries: &mut [(String, Value)]) {
    entries.sort_by(|a, b| a.0.cmp(&b.0));
}

fn finish_map(mut entries: Vec<(String, Value)>, sort_keys: bool) -> Value {
    if sort_keys {
        sort_map_entries(&mut entries);
    }
    let mut map = serde_json::Map::with_capacity(entries.len());
    for (key, value) in entries {
        map.insert(key, value);
    }
    Value::Object(map)
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
            .push((key, value.serialize(Canonical::for_type::<T>())?));
        Ok(())
    }
    fn end(self) -> Result<Value, Self::Error> {
        Ok(finish_map(self.entries, self.sort_keys))
    }
}

impl ser::SerializeStruct for StructBuilder {
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

/// The key of the one-entry object that records a PRESENT `Option` whose
/// contents serialise to `null`.
///
/// JSON has one `null`, and serde's `Option` spends it on `None`, so
/// `Some(None)` — a cache hit holding "this does not exist" — would record the
/// same byte as `None`, a miss, and replay would take the wrong arm. A `Some`
/// is therefore recorded as `{"deja:some": inner}` exactly when its inner value
/// is `null` or is itself such an object; every other `Some(x)` records as `x`,
/// as it always has. The second condition is what makes the encoding
/// unambiguous at any depth: `Some(Some(None))` nests the marker, and a
/// genuine map that happens to look like one is escaped rather than misread.
pub const PRESENT_KEY: &str = "deja:some";

fn is_present_marker(value: &Value) -> bool {
    matches!(value, Value::Object(map) if map.len() == 1 && map.contains_key(PRESENT_KEY))
}

fn mark_if_ambiguous(inner: Value) -> Value {
    if inner.is_null() || is_present_marker(&inner) {
        let mut map = serde_json::Map::with_capacity(1);
        map.insert(PRESENT_KEY.to_owned(), inner);
        Value::Object(map)
    } else {
        inner
    }
}

fn carries_present_marker(value: &Value) -> bool {
    match value {
        Value::Array(items) => items.iter().any(carries_present_marker),
        Value::Object(map) => is_present_marker(value) || map.values().any(carries_present_marker),
        _ => false,
    }
}

/// Rebuild a recorded value, reading the present-`Option` marker that
/// [`to_value`] writes.
///
/// A value with no marker anywhere — every tape recorded before the marker
/// existed — goes straight to [`serde_json::from_value`], so a bare `null`
/// still decodes as `None` and an old tape reads exactly as it did.
///
/// # Errors
///
/// Whatever the target type's `Deserialize` rejects.
pub fn from_value<T>(value: Value) -> Result<T, serde_json::Error>
where
    T: DeserializeOwned,
{
    if carries_present_marker(&value) {
        T::deserialize(Recorded(value))
    } else {
        serde_json::from_value(value)
    }
}

/// A `serde_json::Value` deserializer that answers `deserialize_option` from
/// the marker and hands every nested value to another `Recorded`, so an
/// `Option` at any depth is reached. Scalars defer to `Value`'s own impl.
struct Recorded(Value);

macro_rules! defer_scalar {
    ($($method:ident)*) => {$(
        fn $method<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
            match self.0 {
                Value::Array(_) | Value::Object(_) => self.deserialize_any(visitor),
                scalar => scalar.$method(visitor),
            }
        }
    )*};
}

impl<'de> Deserializer<'de> for Recorded {
    type Error = serde_json::Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.0 {
            Value::Array(items) => visitor.visit_seq(RecordedSeq(items.into_iter())),
            Value::Object(map) => visitor.visit_map(RecordedMap {
                entries: map.into_iter(),
                pending: None,
            }),
            scalar => scalar.deserialize_any(visitor),
        }
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.0 {
            Value::Null => visitor.visit_none(),
            Value::Object(mut map) if map.len() == 1 && map.contains_key(PRESENT_KEY) => {
                let inner = map.remove(PRESENT_KEY).unwrap_or(Value::Null);
                visitor.visit_some(Recorded(inner))
            }
            other => visitor.visit_some(Recorded(other)),
        }
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        match self.0 {
            Value::Object(map) if map.len() == 1 => {
                let (variant, value) = map.into_iter().next().unwrap_or_default();
                visitor.visit_enum(RecordedEnum { variant, value })
            }
            other => other.deserialize_enum(name, variants, visitor),
        }
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.0.deserialize_unit_struct(name, visitor)
    }

    fn deserialize_tuple<V: Visitor<'de>>(
        self,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_unit()
    }

    defer_scalar! {
        deserialize_bool deserialize_i8 deserialize_i16 deserialize_i32 deserialize_i64
        deserialize_i128 deserialize_u8 deserialize_u16 deserialize_u32 deserialize_u64
        deserialize_u128 deserialize_f32 deserialize_f64 deserialize_char deserialize_str
        deserialize_string deserialize_bytes deserialize_byte_buf deserialize_unit
        deserialize_identifier
    }
}

struct RecordedSeq(std::vec::IntoIter<Value>);

impl<'de> de::SeqAccess<'de> for RecordedSeq {
    type Error = serde_json::Error;

    fn next_element_seed<S: de::DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> Result<Option<S::Value>, Self::Error> {
        self.0
            .next()
            .map(|item| seed.deserialize(Recorded(item)))
            .transpose()
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.0.len())
    }
}

struct RecordedMap {
    entries: serde_json::map::IntoIter,
    pending: Option<Value>,
}

impl<'de> de::MapAccess<'de> for RecordedMap {
    type Error = serde_json::Error;

    fn next_key_seed<S: de::DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> Result<Option<S::Value>, Self::Error> {
        match self.entries.next() {
            Some((key, value)) => {
                self.pending = Some(value);
                seed.deserialize(RecordedKey(key)).map(Some)
            }
            None => Ok(None),
        }
    }

    fn next_value_seed<S: de::DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> Result<S::Value, Self::Error> {
        let value = self
            .pending
            .take()
            .ok_or_else(|| <serde_json::Error as de::Error>::custom("value before key"))?;
        seed.deserialize(Recorded(value))
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.entries.len())
    }
}

/// An object key, read the way `serde_json` reads one: a string, or a scalar
/// written as its string form (`HashMap<u64, _>` keys arrive as `"7"`).
struct RecordedKey(String);

macro_rules! parse_key {
    ($($method:ident => $visit:ident: $ty:ty),*) => {$(
        fn $method<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
            match self.0.parse::<$ty>() {
                Ok(parsed) => visitor.$visit(parsed),
                Err(_) => visitor.visit_string(self.0),
            }
        }
    )*};
}

impl<'de> Deserializer<'de> for RecordedKey {
    type Error = serde_json::Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_string(self.0)
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_some(self)
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        visitor.visit_enum(self.0.into_deserializer())
    }

    parse_key! {
        deserialize_bool => visit_bool: bool,
        deserialize_i8 => visit_i8: i8,
        deserialize_i16 => visit_i16: i16,
        deserialize_i32 => visit_i32: i32,
        deserialize_i64 => visit_i64: i64,
        deserialize_i128 => visit_i128: i128,
        deserialize_u8 => visit_u8: u8,
        deserialize_u16 => visit_u16: u16,
        deserialize_u32 => visit_u32: u32,
        deserialize_u64 => visit_u64: u64,
        deserialize_u128 => visit_u128: u128,
        deserialize_f32 => visit_f32: f32,
        deserialize_f64 => visit_f64: f64
    }

    serde::forward_to_deserialize_any! {
        char str string bytes byte_buf unit unit_struct seq tuple tuple_struct map
        struct identifier ignored_any
    }
}

struct RecordedEnum {
    variant: String,
    value: Value,
}

impl<'de> de::EnumAccess<'de> for RecordedEnum {
    type Error = serde_json::Error;
    type Variant = Recorded;

    fn variant_seed<S: de::DeserializeSeed<'de>>(
        self,
        seed: S,
    ) -> Result<(S::Value, Recorded), Self::Error> {
        let name: de::value::StringDeserializer<serde_json::Error> =
            self.variant.into_deserializer();
        let variant = seed.deserialize(name)?;
        Ok((variant, Recorded(self.value)))
    }
}

impl<'de> de::VariantAccess<'de> for Recorded {
    type Error = serde_json::Error;

    fn unit_variant(self) -> Result<(), Self::Error> {
        <() as de::Deserialize>::deserialize(self)
    }

    fn newtype_variant_seed<S: de::DeserializeSeed<'de>>(
        self,
        seed: S,
    ) -> Result<S::Value, Self::Error> {
        seed.deserialize(self)
    }

    fn tuple_variant<V: Visitor<'de>>(
        self,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.deserialize_any(visitor)
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
        assert_eq!(
            std::any::type_name::<HashMap<String, u8>>(),
            "std::collections::hash::map::HashMap<alloc::string::String, u8>",
            "the HashMap path this module matches on has moved"
        );
        assert!(is_unordered_map(
            std::any::type_name::<HashMap<String, u8>>()
        ));
        assert!(!is_unordered_map(std::any::type_name::<
            serde_json::Map<String, Value>,
        >()));
        assert!(!is_unordered_map(std::any::type_name::<Value>()));
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
            // A BTreeMap, not a HashMap: a HashMap IS one of the unordered
            // collections this test is meant to exclude, and it iterates in a
            // per-process random order. The assertion below compares RENDERED
            // BYTES, so one would make this test fail whenever the random order
            // is not sorted order.
            labels: BTreeMap<String, u8>,
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

    /// A MAP's keys are emitted in key order whatever `serde_json::Map` does with
    /// insertion order. Asserted on the entry list the builder sorts, not on the
    /// finished `Map` — on a `BTreeMap`-backed build the finished map is sorted
    /// whether or not anybody sorted it, and a test on it would pass for that
    /// reason alone. The consumer this exists for (`preserve_order` on) is the
    /// one whose `Map` would not have sorted them.
    #[test]
    fn map_entries_are_emitted_in_key_order() {
        let mut entries = vec![
            ("zeta".to_owned(), Value::from(1)),
            ("alpha".to_owned(), Value::from(2)),
            ("mid".to_owned(), Value::from(3)),
        ];
        sort_map_entries(&mut entries);
        let keys: Vec<&str> = entries.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["alpha", "mid", "zeta"]);

        let map: HashMap<String, u8> = [("zeta", 1u8), ("alpha", 2), ("mid", 3)]
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v))
            .collect();
        let value = to_value(&map).expect("canonical");
        let emitted: Vec<&String> = value.as_object().expect("object").keys().collect();
        assert_eq!(emitted, ["alpha", "mid", "zeta"]);
        assert_eq!(value, serde_json::to_value(&map).expect("serde_json"));
    }

    /// An ordered map reaches the tape in the order it was built. The recorded
    /// value is what replay hands back, so sorting it would return something
    /// other than what was recorded.
    #[test]
    fn an_ordered_map_keeps_the_order_it_was_built_in() {
        let mut headers = serde_json::Map::new();
        for name in ["date", "content-type", "accept-ranges"] {
            headers.insert(name.to_owned(), Value::from(1));
        }
        let built: Vec<&String> = headers.keys().collect();
        assert_eq!(
            built,
            ["date", "content-type", "accept-ranges"],
            "this build's serde_json::Map does not keep insertion order, so the \
             assertion below could not fail"
        );

        let value = to_value(&Value::Object(headers)).expect("canonical");
        let emitted: Vec<&String> = value.as_object().expect("object").keys().collect();
        assert_eq!(emitted, ["date", "content-type", "accept-ranges"]);
    }

    /// The same keys through a `HashMap` are sorted: its order is this process's
    /// hash order and means nothing.
    #[test]
    fn an_unordered_map_nested_in_an_ordered_one_is_still_sorted() {
        #[derive(serde::Serialize)]
        struct Payload {
            ordered: serde_json::Map<String, Value>,
            unordered: HashMap<String, u8>,
        }
        let mut ordered = serde_json::Map::new();
        ordered.insert("zeta".to_owned(), Value::from(1));
        ordered.insert("alpha".to_owned(), Value::from(2));
        let unordered = [("zeta", 1u8), ("alpha", 2), ("mid", 3)]
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v))
            .collect();

        let value = to_value(&Payload { ordered, unordered }).expect("canonical");
        let keys = |field: &str| -> Vec<String> {
            value[field]
                .as_object()
                .expect("object")
                .keys()
                .cloned()
                .collect()
        };
        assert_eq!(keys("ordered"), ["zeta", "alpha"]);
        assert_eq!(keys("unordered"), ["alpha", "mid", "zeta"]);
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

    fn round_trip<T>(value: &T) -> T
    where
        T: Serialize + DeserializeOwned,
    {
        from_value(to_value(value).expect("canonical")).expect("decode")
    }

    /// A tape recorded before the marker holds a bare `null` for both arms, and
    /// must keep reading as it always did: `None`.
    #[test]
    fn a_bare_null_still_decodes_as_absent() {
        let decoded: Option<Option<String>> = from_value(Value::Null).expect("decode");
        assert_eq!(decoded, None);
    }

    /// Every depth of a nested `Option` records distinctly and comes back as
    /// itself, which is what the marker nesting exists for.
    #[test]
    fn each_depth_of_a_nested_option_is_its_own_value() {
        type Deep = Option<Option<Option<String>>>;
        let cases: [Deep; 4] = [
            None,
            Some(None),
            Some(Some(None)),
            Some(Some(Some("x".into()))),
        ];
        let captured: Vec<Value> = cases
            .iter()
            .map(|case| to_value(case).expect("canonical"))
            .collect();
        for (i, a) in captured.iter().enumerate() {
            for b in &captured[i + 1..] {
                assert_ne!(a, b, "two depths captured as one value");
            }
        }
        for case in &cases {
            assert_eq!(&round_trip(case), case);
        }
    }

    /// The marker is spent only where `null` would be ambiguous. A `Some` whose
    /// contents are anything else records exactly as `serde_json` records it.
    #[test]
    fn a_present_value_that_is_not_null_records_as_it_always_did() {
        #[derive(serde::Serialize)]
        struct Row {
            name: Option<String>,
            nested: Option<Option<u8>>,
            missing: Option<u8>,
        }
        let row = Row {
            name: Some("n".into()),
            nested: Some(Some(3)),
            missing: None,
        };
        assert_eq!(
            to_value(&row).expect("canonical"),
            serde_json::to_value(&row).expect("serde_json"),
        );
    }

    /// A genuine map that happens to have the marker's shape is escaped when it
    /// sits in an `Option`, and read as a map where no `Option` is asked for.
    #[test]
    fn a_map_shaped_like_the_marker_is_not_mistaken_for_one() {
        let lookalike: BTreeMap<String, Option<u8>> =
            [(PRESENT_KEY.to_owned(), None)].into_iter().collect();
        assert_eq!(round_trip(&lookalike), lookalike);
        assert_eq!(round_trip(&Some(lookalike.clone())), Some(lookalike));
    }

    /// The marker is read wherever an `Option` sits: a struct field, a sequence
    /// element, a map value under an integer key, and an enum variant's payload.
    #[test]
    fn a_present_none_survives_at_any_position() {
        #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
        enum Reply {
            Cached(Option<Option<String>>),
            Rows { rows: Vec<Option<Option<u8>>> },
        }
        #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
        struct Holder {
            field: Option<Option<String>>,
            by_id: BTreeMap<u64, Option<Option<u8>>>,
            replies: Vec<Reply>,
            unit: Option<()>,
        }
        let holder = Holder {
            field: Some(None),
            by_id: [(7, Some(None)), (9, None)].into_iter().collect(),
            replies: vec![
                Reply::Cached(Some(None)),
                Reply::Rows {
                    rows: vec![Some(None), None, Some(Some(1))],
                },
            ],
            unit: Some(()),
        };
        assert_eq!(round_trip(&holder), holder);
    }
}
