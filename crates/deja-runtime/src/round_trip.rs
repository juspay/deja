//! Does a boundary's recorded value come back as itself?
//!
//! A codec is a pair — `capture` writes the tape, `reconstruct` reads it back —
//! and replay is only faithful if `reconstruct(capture(v))` IS `v`. Nothing about
//! the pair's shape guarantees that: `Some(None)` and `None` once captured to the
//! same JSON and replayed the wrong arm for weeks. So the recorder checks it, on
//! every recorded call, by rebuilding the value it just captured and comparing.
//!
//! Comparing the two JSON captures cannot work: a lossy capture loses the
//! difference before the second capture runs, so the two agree. The comparison
//! has to be of VALUES, and the strongest one available is chosen per type by
//! autoref specialisation at the call site ([`compare!`](crate::compare)):
//!
//! 1. `PartialEq` — the type's own equality, which can see fields serialisation
//!    does not carry.
//! 2. `Serialize` — the serde data model, which is finer than JSON: it keeps
//!    `Some` apart from its contents, a unit apart from `None`, a newtype apart
//!    from its field and every integer width apart. [`fingerprint`] renders that
//!    model exactly, maps in key order and sets sorted, so two equal values
//!    always render alike. This is the tier a generic boundary reaches, since
//!    its codec already requires `Serialize` — and it cannot see what
//!    serialisation drops (`#[serde(skip)]`, a value that serialises masked).
//! 3. A `Result` whose error offers neither — an `error_stack::Report`, which
//!    most fallible boundaries return — compared by its `Ok` arm's serde image.
//!    The recorder only checks values that are not errors, so the `Ok` arm is
//!    the whole of what it compares.
//! 4. None of these — the check cannot be made and says so.
//!
//! Which tier a type reaches is known where the type is concrete, so
//! [`round_trip!`](crate::round_trip) decides it there: a type with nothing to
//! compare by is never rebuilt, and pays nothing for the check.
//!
//! `Debug` is deliberately not a tier: a `HashMap` renders in hash order, so a
//! difference would not mean the value changed.

use serde::ser::{self, Serialize, Serializer};

use crate::canonical::is_unordered_sequence;

/// The outcome of comparing a value with its rebuilt copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Comparison {
    Same,
    Different,
    /// The type offers neither `PartialEq` nor `Serialize`, or one side could
    /// not be serialised.
    Incomparable,
}

impl Comparison {
    fn from_equal(equal: bool) -> Self {
        if equal {
            Self::Same
        } else {
            Self::Different
        }
    }
}

/// Receiver for the autoref-specialised comparison: a pair of values to
/// compare, or none, which asks only whether the type can be compared at all.
/// Use [`compare!`](crate::compare) and [`round_trip!`](crate::round_trip).
///
/// Each tier is implemented one reference level below the one above it, and
/// both macros call through four references, so the first applicable tier in
/// the order the module documents is the one method lookup reaches first.
pub struct Compare<'a, T: ?Sized>(pub Option<(&'a T, &'a T)>);

impl<T: ?Sized> Compare<'_, T> {
    /// The pair-less receiver [`round_trip!`](crate::round_trip) probes with.
    #[must_use]
    pub const fn probe() -> Self {
        Self(None)
    }
}

pub trait CompareByEq {
    fn deja_comparable(&self) -> bool;
    fn deja_compare(&self) -> Comparison;
}
impl<T: PartialEq + ?Sized> CompareByEq for &&&Compare<'_, T> {
    fn deja_comparable(&self) -> bool {
        true
    }
    fn deja_compare(&self) -> Comparison {
        match self.0 {
            Some((a, b)) => Comparison::from_equal(a == b),
            None => Comparison::Incomparable,
        }
    }
}

pub trait CompareBySerde {
    fn deja_comparable(&self) -> bool;
    fn deja_compare(&self) -> Comparison;
}
impl<T: Serialize + ?Sized> CompareBySerde for &&Compare<'_, T> {
    fn deja_comparable(&self) -> bool {
        true
    }
    fn deja_compare(&self) -> Comparison {
        match self.0 {
            Some((a, b)) => by_fingerprint(a, b),
            None => Comparison::Incomparable,
        }
    }
}

pub trait CompareOkArm {
    fn deja_comparable(&self) -> bool;
    fn deja_compare(&self) -> Comparison;
}
impl<T: Serialize, E> CompareOkArm for &Compare<'_, Result<T, E>> {
    fn deja_comparable(&self) -> bool {
        true
    }
    fn deja_compare(&self) -> Comparison {
        match self.0 {
            Some((Ok(a), Ok(b))) => by_fingerprint(a, b),
            _ => Comparison::Incomparable,
        }
    }
}

fn by_fingerprint<T: Serialize + ?Sized>(a: &T, b: &T) -> Comparison {
    match (fingerprint(a), fingerprint(b)) {
        (Some(a), Some(b)) => Comparison::from_equal(a == b),
        _ => Comparison::Incomparable,
    }
}

pub trait CompareNothing {
    fn deja_comparable(&self) -> bool;
    fn deja_compare(&self) -> Comparison;
}
impl<T: ?Sized> CompareNothing for Compare<'_, T> {
    fn deja_comparable(&self) -> bool {
        false
    }
    fn deja_compare(&self) -> Comparison {
        Comparison::Incomparable
    }
}

/// Compare two values of one type by the strongest means the type offers.
#[macro_export]
macro_rules! compare {
    ($a:expr, $b:expr) => {{
        #[allow(unused_imports)]
        use $crate::round_trip::{
            CompareByEq as _, CompareBySerde as _, CompareNothing as _, CompareOkArm as _,
        };
        (&&&&$crate::round_trip::Compare(::core::option::Option::Some(($a, $b)))).deja_compare()
    }};
}

/// What a site offers the recorder's round-trip check, decided where the
/// site's type is concrete.
pub enum RoundTrip<S> {
    /// The site declares no replay codec: its capture is not meant to rebuild.
    RecordOnly,
    /// A codec, but a type with nothing to compare by: rebuilding would cost a
    /// decode and learn nothing, so the check is skipped and says so.
    Unverifiable,
    /// Rebuild the recorded value and compare it with this.
    Check(S),
}

/// The [`RoundTrip`] for a site returning `$ty` that declares a replay codec.
#[macro_export]
macro_rules! round_trip {
    ($ty:ty) => {{
        #[allow(unused_imports)]
        use $crate::round_trip::{
            CompareByEq as _, CompareBySerde as _, CompareNothing as _, CompareOkArm as _,
        };
        if (&&&&$crate::round_trip::Compare::<$ty>::probe()).deja_comparable() {
            $crate::round_trip::RoundTrip::Check(|a: &$ty, b: &$ty| $crate::compare!(a, b))
        } else {
            $crate::round_trip::RoundTrip::Unverifiable
        }
    }};
}

/// An exact rendering of `value`'s serde data model: two values render alike
/// exactly when they serialise through the same calls with the same arguments,
/// up to the order of collections whose static type carries none. `None` when
/// the value refuses to serialise.
#[must_use]
pub fn fingerprint<T: Serialize + ?Sized>(value: &T) -> Option<String> {
    value.serialize(Fingerprint::for_type::<T>()).ok()
}

/// Length-prefix a rendered part, so a composite is unambiguous whatever its
/// parts contain.
fn part(rendered: &str) -> String {
    format!("{}:{rendered}", rendered.len())
}

fn join(tag: &str, mut parts: Vec<String>, unordered: bool) -> String {
    if unordered {
        parts.sort();
    }
    let mut out = format!("{tag}{};", parts.len());
    for p in &parts {
        out.push_str(&part(p));
    }
    out
}

/// Carries whether the sequence about to be serialised is a set. A map needs
/// no such bit: its entries are keyed, so every map renders in key order and
/// equal maps render alike behind any wrapper — `Box`, `Arc`, `Secret`,
/// `#[serde(flatten)]`, `serialize_with` — where a type name cannot be seen.
struct Fingerprint {
    unordered: bool,
}

impl Fingerprint {
    fn for_type<T: ?Sized>() -> Self {
        Self {
            unordered: is_set_behind_wrappers(std::any::type_name::<T>()),
        }
    }
}

/// A set, seen through the pointers that serialise as their contents. Unlike
/// a map, a sequence cannot be sorted blind — a `Vec`'s order is its value —
/// so a set behind any other wrapper still renders in its own order.
fn is_set_behind_wrappers(type_name: &str) -> bool {
    const WRAPPERS: &[&str] = &[
        "alloc::boxed::Box<",
        "alloc::sync::Arc<",
        "alloc::rc::Rc<",
        "alloc::borrow::Cow<",
    ];
    let mut name = type_name.trim_start_matches('&');
    while let Some(inner) = WRAPPERS.iter().find_map(|w| name.strip_prefix(w)) {
        name = inner.trim_start_matches('&');
    }
    is_unordered_sequence(name)
}

type Error = serde_json::Error;

impl Serializer for Fingerprint {
    type Ok = String;
    type Error = Error;
    type SerializeSeq = Parts;
    type SerializeTuple = Parts;
    type SerializeTupleStruct = Parts;
    type SerializeTupleVariant = Parts;
    type SerializeMap = Parts;
    type SerializeStruct = Parts;
    type SerializeStructVariant = Parts;

    fn serialize_bool(self, v: bool) -> Result<String, Error> {
        Ok(format!("b{}", u8::from(v)))
    }
    fn serialize_i8(self, v: i8) -> Result<String, Error> {
        Ok(format!("i8:{v}"))
    }
    fn serialize_i16(self, v: i16) -> Result<String, Error> {
        Ok(format!("i16:{v}"))
    }
    fn serialize_i32(self, v: i32) -> Result<String, Error> {
        Ok(format!("i32:{v}"))
    }
    fn serialize_i64(self, v: i64) -> Result<String, Error> {
        Ok(format!("i64:{v}"))
    }
    fn serialize_i128(self, v: i128) -> Result<String, Error> {
        Ok(format!("i128:{v}"))
    }
    fn serialize_u8(self, v: u8) -> Result<String, Error> {
        Ok(format!("u8:{v}"))
    }
    fn serialize_u16(self, v: u16) -> Result<String, Error> {
        Ok(format!("u16:{v}"))
    }
    fn serialize_u32(self, v: u32) -> Result<String, Error> {
        Ok(format!("u32:{v}"))
    }
    fn serialize_u64(self, v: u64) -> Result<String, Error> {
        Ok(format!("u64:{v}"))
    }
    fn serialize_u128(self, v: u128) -> Result<String, Error> {
        Ok(format!("u128:{v}"))
    }
    fn serialize_f32(self, v: f32) -> Result<String, Error> {
        Ok(format!("f32:{:x}", v.to_bits()))
    }
    fn serialize_f64(self, v: f64) -> Result<String, Error> {
        Ok(format!("f64:{:x}", v.to_bits()))
    }
    fn serialize_char(self, v: char) -> Result<String, Error> {
        Ok(format!("c{}", part(&v.to_string())))
    }
    fn serialize_str(self, v: &str) -> Result<String, Error> {
        Ok(format!("s{}", part(v)))
    }
    fn serialize_bytes(self, v: &[u8]) -> Result<String, Error> {
        let hex: String = v.iter().map(|b| format!("{b:02x}")).collect();
        Ok(format!("y{}", part(&hex)))
    }
    fn serialize_none(self) -> Result<String, Error> {
        Ok("N".to_owned())
    }
    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<String, Error> {
        Ok(format!(
            "S{}",
            part(&value.serialize(Self::for_type::<T>())?)
        ))
    }
    fn serialize_unit(self) -> Result<String, Error> {
        Ok("U".to_owned())
    }
    fn serialize_unit_struct(self, name: &'static str) -> Result<String, Error> {
        Ok(format!("us{}", part(name)))
    }
    fn serialize_unit_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
    ) -> Result<String, Error> {
        Ok(format!("uv{}{index};{}", part(name), part(variant)))
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<String, Error> {
        let inner = value.serialize(Self::for_type::<T>())?;
        Ok(format!("ns{}{}", part(name), part(&inner)))
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<String, Error> {
        let inner = value.serialize(Self::for_type::<T>())?;
        Ok(format!(
            "nv{}{index};{}{}",
            part(name),
            part(variant),
            part(&inner)
        ))
    }
    fn serialize_seq(self, _len: Option<usize>) -> Result<Parts, Error> {
        Ok(Parts::new("q".to_owned(), self.unordered))
    }
    fn serialize_tuple(self, _len: usize) -> Result<Parts, Error> {
        Ok(Parts::new("t".to_owned(), false))
    }
    fn serialize_tuple_struct(self, name: &'static str, _len: usize) -> Result<Parts, Error> {
        Ok(Parts::new(format!("ts{}", part(name)), false))
    }
    fn serialize_tuple_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Parts, Error> {
        Ok(Parts::new(
            format!("tv{}{index};{}", part(name), part(variant)),
            false,
        ))
    }
    fn serialize_map(self, _len: Option<usize>) -> Result<Parts, Error> {
        Ok(Parts::new("m".to_owned(), true))
    }
    fn serialize_struct(self, name: &'static str, _len: usize) -> Result<Parts, Error> {
        Ok(Parts::new(format!("st{}", part(name)), false))
    }
    fn serialize_struct_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Parts, Error> {
        Ok(Parts::new(
            format!("sv{}{index};{}", part(name), part(variant)),
            false,
        ))
    }
}

/// A composite under construction. A map entry is one part (key and value
/// together), so sorting an unordered map's parts sorts its entries.
struct Parts {
    tag: String,
    parts: Vec<String>,
    unordered: bool,
    pending_key: Option<String>,
}

impl Parts {
    fn new(tag: String, unordered: bool) -> Self {
        Self {
            tag,
            parts: Vec::new(),
            unordered,
            pending_key: None,
        }
    }

    fn push<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        self.parts
            .push(value.serialize(Fingerprint::for_type::<T>())?);
        Ok(())
    }

    fn push_field<T: Serialize + ?Sized>(&mut self, key: &str, value: &T) -> Result<(), Error> {
        let value = value.serialize(Fingerprint::for_type::<T>())?;
        self.parts.push(format!("{}{}", part(key), part(&value)));
        Ok(())
    }

    fn finish(self) -> String {
        join(&self.tag, self.parts, self.unordered)
    }
}

impl ser::SerializeSeq for Parts {
    type Ok = String;
    type Error = Error;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        self.push(value)
    }
    fn end(self) -> Result<String, Error> {
        Ok(self.finish())
    }
}

impl ser::SerializeTuple for Parts {
    type Ok = String;
    type Error = Error;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        self.push(value)
    }
    fn end(self) -> Result<String, Error> {
        Ok(self.finish())
    }
}

impl ser::SerializeTupleStruct for Parts {
    type Ok = String;
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        self.push(value)
    }
    fn end(self) -> Result<String, Error> {
        Ok(self.finish())
    }
}

impl ser::SerializeTupleVariant for Parts {
    type Ok = String;
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        self.push(value)
    }
    fn end(self) -> Result<String, Error> {
        Ok(self.finish())
    }
}

impl ser::SerializeMap for Parts {
    type Ok = String;
    type Error = Error;
    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), Error> {
        self.pending_key = Some(key.serialize(Fingerprint { unordered: false })?);
        Ok(())
    }
    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        let key = self
            .pending_key
            .take()
            .ok_or_else(|| <Error as ser::Error>::custom("value serialized before key"))?;
        self.push_field(&key, value)
    }
    fn end(self) -> Result<String, Error> {
        Ok(self.finish())
    }
}

impl ser::SerializeStruct for Parts {
    type Ok = String;
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Error> {
        self.push_field(key, value)
    }
    fn skip_field(&mut self, key: &'static str) -> Result<(), Error> {
        self.parts.push(format!("{}-", part(key)));
        Ok(())
    }
    fn end(self) -> Result<String, Error> {
        Ok(self.finish())
    }
}

impl ser::SerializeStructVariant for Parts {
    type Ok = String;
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Error> {
        self.push_field(key, value)
    }
    fn skip_field(&mut self, key: &'static str) -> Result<(), Error> {
        self.parts.push(format!("{}-", part(key)));
        Ok(())
    }
    fn end(self) -> Result<String, Error> {
        Ok(self.finish())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, HashMap, HashSet};

    #[test]
    fn the_serde_tier_tells_a_present_none_from_an_absent_one() {
        let present: Option<Option<String>> = Some(None);
        let absent: Option<Option<String>> = None;
        assert_ne!(fingerprint(&present), fingerprint(&absent));
        // JSON cannot, which is why the check does not compare JSON.
        assert_eq!(
            serde_json::to_value(present).expect("json"),
            serde_json::to_value(absent).expect("json"),
        );
    }

    #[test]
    fn the_serde_tier_keeps_apart_what_json_merges() {
        assert_ne!(fingerprint(&()), fingerprint(&None::<u8>));
        assert_ne!(fingerprint(&1_u32), fingerprint(&1_u64));
        assert_ne!(fingerprint(&1_u64), fingerprint(&1.0_f64));
        assert_ne!(fingerprint(&'a'), fingerprint(&"a"));
        assert_ne!(fingerprint(&vec!["a,b"]), fingerprint(&vec!["a", "b"]));
    }

    #[test]
    fn equal_values_render_alike_whatever_order_their_hash_collections_iterate_in() {
        let mut left: HashMap<String, HashSet<u32>> = HashMap::new();
        let mut right: HashMap<String, HashSet<u32>> = HashMap::new();
        for i in 0..64_u32 {
            left.entry(format!("k{}", i % 7)).or_default().insert(i);
        }
        for i in (0..64_u32).rev() {
            right.entry(format!("k{}", i % 7)).or_default().insert(i);
        }
        assert_eq!(left, right);
        assert_eq!(fingerprint(&left), fingerprint(&right));
        let ordered: BTreeMap<u8, u8> = [(2, 0), (1, 0)].into_iter().collect();
        assert!(fingerprint(&ordered).is_some());
    }

    /// Every part is length-prefixed; without it a key and its value could
    /// trade characters and two different maps would render alike.
    #[test]
    fn keys_and_values_cannot_trade_characters() {
        let a: BTreeMap<&str, &str> = [("a", "sb")].into_iter().collect();
        let b: BTreeMap<&str, &str> = [("as", "b")].into_iter().collect();
        assert_ne!(fingerprint(&a), fingerprint(&b));
    }

    #[test]
    fn a_sequence_keeps_its_order() {
        assert_ne!(fingerprint(&vec![1, 2]), fingerprint(&vec![2, 1]));
    }

    /// The tier each type reaches. A generic `T: Serialize` reaches the serde
    /// tier, which is the case that matters: generic boundaries have no other.
    #[test]
    fn each_type_is_compared_by_the_strongest_means_it_offers() {
        #[derive(PartialEq)]
        struct EqOnly(u8);
        struct Neither;
        fn generic<T: Serialize>(a: &T, b: &T) -> Comparison {
            crate::compare!(a, b)
        }
        fn fallible<T: Serialize>(a: &Result<T, Neither>, b: &Result<T, Neither>) -> Comparison {
            crate::compare!(a, b)
        }

        assert_eq!(
            crate::compare!(&EqOnly(1), &EqOnly(2)),
            Comparison::Different
        );
        assert_eq!(
            crate::compare!(&Neither, &Neither),
            Comparison::Incomparable
        );
        assert_eq!(generic(&Some(None::<u8>), &None), Comparison::Different);
        assert_eq!(generic(&Some(Some(1_u8)), &Some(Some(1))), Comparison::Same);
        assert_eq!(
            fallible(&Ok(Some(None::<u8>)), &Ok(None)),
            Comparison::Different
        );
        assert_eq!(fallible(&Ok(Some(1_u8)), &Ok(Some(1))), Comparison::Same);
        assert_eq!(
            fallible::<u8>(&Err(Neither), &Err(Neither)),
            Comparison::Incomparable
        );
    }

    /// A type with both is compared by its own equality, not by its serde
    /// image: `PartialEq` is the stronger statement, since it can see fields
    /// serialisation does not carry.
    #[test]
    fn partial_eq_outranks_the_serde_image() {
        #[derive(serde::Serialize)]
        struct AlwaysEqual(u8);
        impl PartialEq for AlwaysEqual {
            fn eq(&self, _: &Self) -> bool {
                true
            }
        }
        assert_eq!(
            crate::compare!(&AlwaysEqual(1), &AlwaysEqual(2)),
            Comparison::Same,
            "PartialEq decided, although the serde images differ"
        );
        assert_eq!(crate::compare!(&-0.0_f64, &0.0_f64), Comparison::Same);
    }

    fn checks<S>(round_trip: RoundTrip<S>) -> bool {
        matches!(round_trip, RoundTrip::Check(_))
    }

    /// Whether a type can be compared is decided where it is concrete, so a
    /// site with nothing to compare by never pays for a rebuild.
    #[test]
    fn a_type_with_nothing_to_compare_by_is_never_rebuilt() {
        #[derive(PartialEq)]
        struct EqOnly;
        struct Neither;
        fn generic<T: Serialize>() -> bool {
            checks(crate::round_trip!(T))
        }
        fn unbounded<T>() -> bool {
            checks(crate::round_trip!(T))
        }
        assert!(checks(crate::round_trip!(EqOnly)));
        assert!(checks(crate::round_trip!(Result<u8, Neither>)));
        assert!(generic::<Neither2>());
        assert!(!checks(crate::round_trip!(Neither)));
        assert!(!checks(crate::round_trip!(Result<Neither, u8>)));
        assert!(!unbounded::<u8>(), "an unbounded generic offers nothing");

        #[derive(serde::Serialize)]
        struct Neither2;
    }

    /// A map renders in key order behind any wrapper, and a set behind the
    /// pointers that serialise as their contents, so two equal values can
    /// never render differently and fail a debug build for nothing.
    #[test]
    fn equal_values_render_alike_behind_wrappers() {
        #[derive(serde::Serialize)]
        struct Flat {
            #[serde(flatten)]
            extra: HashMap<String, u32>,
        }
        let build = |forward: bool| -> (HashMap<String, u32>, HashSet<u32>) {
            let mut order: Vec<u32> = (0..64).collect();
            if !forward {
                order.reverse();
            }
            (
                order.iter().map(|i| (format!("k{i}"), *i)).collect(),
                order.iter().copied().collect(),
            )
        };
        let (map_a, set_a) = build(true);
        let (map_b, set_b) = build(false);
        assert_eq!(
            fingerprint(&Box::new(map_a.clone())),
            fingerprint(&Box::new(map_b.clone()))
        );
        assert_eq!(
            fingerprint(&Flat { extra: map_a }),
            fingerprint(&Flat { extra: map_b })
        );
        assert_eq!(
            fingerprint(&Box::new(set_a.clone())),
            fingerprint(&Box::new(set_b.clone()))
        );
        assert_eq!(
            fingerprint(&Some(Box::new(set_a))),
            fingerprint(&Some(Box::new(set_b)))
        );
    }
}
