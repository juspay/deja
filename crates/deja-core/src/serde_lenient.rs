//! Lenient numeric deserializers for Deja wire types.
//!
//! String-preserving JSON pipelines (notably Vector) stringify unsigned
//! integers above `i64::MAX` in transit. These helpers accept both JSON
//! numbers and numeric strings so the canonical types parse tapes from
//! either path. Null/missing handling stays with `#[serde(default)]` on the
//! field; the `opt_*` variants additionally map an explicit `null` to `None`.

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer};

fn u64_from_value<E: DeError>(value: serde_json::Value) -> Result<u64, E> {
    match value {
        serde_json::Value::Number(n) => n
            .as_u64()
            .ok_or_else(|| E::custom(format!("expected u64, got {n}"))),
        serde_json::Value::String(s) => s.parse::<u64>().map_err(E::custom),
        other => Err(E::custom(format!(
            "expected u64 number or string, got {other}"
        ))),
    }
}

pub fn u64_lenient<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
    u64_from_value(serde_json::Value::deserialize(d)?)
}

pub fn u32_lenient<'de, D: Deserializer<'de>>(d: D) -> Result<u32, D::Error> {
    let n = u64_lenient(d)?;
    u32::try_from(n).map_err(|_| D::Error::custom(format!("value {n} out of range for u32")))
}

pub fn u16_lenient<'de, D: Deserializer<'de>>(d: D) -> Result<u16, D::Error> {
    let n = u64_lenient(d)?;
    u16::try_from(n).map_err(|_| D::Error::custom(format!("value {n} out of range for u16")))
}

pub fn opt_u64_lenient<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
    match Option::<serde_json::Value>::deserialize(d)? {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(value) => u64_from_value(value).map(Some),
    }
}

pub fn opt_u32_lenient<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u32>, D::Error> {
    match opt_u64_lenient(d)? {
        None => Ok(None),
        Some(n) => u32::try_from(n)
            .map(Some)
            .map_err(|_| D::Error::custom(format!("value {n} out of range for u32"))),
    }
}

pub fn vec_u64_lenient<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u64>, D::Error> {
    Vec::<serde_json::Value>::deserialize(d)?
        .into_iter()
        .map(u64_from_value)
        .collect()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use serde::Deserialize;

    #[derive(Deserialize)]
    struct Probe {
        #[serde(deserialize_with = "super::u64_lenient")]
        big: u64,
        #[serde(deserialize_with = "super::u32_lenient")]
        small: u32,
        #[serde(deserialize_with = "super::u16_lenient")]
        tiny: u16,
        #[serde(default, deserialize_with = "super::opt_u64_lenient")]
        maybe: Option<u64>,
        #[serde(default, deserialize_with = "super::opt_u32_lenient")]
        maybe_small: Option<u32>,
        #[serde(default, deserialize_with = "super::vec_u64_lenient")]
        many: Vec<u64>,
    }

    #[test]
    fn accepts_numbers_and_numeric_strings() {
        let p: Probe = serde_json::from_str(
            r#"{"big":"13069351011358544953","small":"7","tiny":8,
                "maybe":"42","maybe_small":9,"many":["1","6",3]}"#,
        )
        .unwrap();
        assert_eq!(p.big, 13_069_351_011_358_544_953);
        assert_eq!(p.small, 7);
        assert_eq!(p.tiny, 8);
        assert_eq!(p.maybe, Some(42));
        assert_eq!(p.maybe_small, Some(9));
        assert_eq!(p.many, vec![1, 6, 3]);
    }

    #[test]
    fn null_options_deserialize_to_none() {
        let p: Probe = serde_json::from_str(
            r#"{"big":1,"small":1,"tiny":1,"maybe":null,"maybe_small":null,"many":[]}"#,
        )
        .unwrap();
        assert_eq!(p.maybe, None);
        assert_eq!(p.maybe_small, None);
    }

    #[test]
    fn rejects_garbage() {
        assert!(serde_json::from_str::<Probe>(
            r#"{"big":"not-a-number","small":1,"tiny":1,"many":[]}"#
        )
        .is_err());
        assert!(
            serde_json::from_str::<Probe>(r#"{"big":true,"small":1,"tiny":1,"many":[]}"#).is_err()
        );
        // u32 overflow via string must error, not wrap.
        assert!(serde_json::from_str::<Probe>(
            r#"{"big":1,"small":"4294967296","tiny":1,"many":[]}"#
        )
        .is_err());
    }

    #[test]
    fn survives_serde_content_buffering() {
        // #[serde(flatten)] siblings route fields through serde's private
        // Content buffer; the lenient fns must work through that path too.
        #[derive(Deserialize)]
        struct Flat {
            #[serde(deserialize_with = "super::u64_lenient")]
            n: u64,
            #[serde(flatten)]
            rest: serde_json::Map<String, serde_json::Value>,
        }
        let f: Flat = serde_json::from_str(r#"{"n":"99","other":"x"}"#).unwrap();
        assert_eq!(f.n, 99);
        assert_eq!(f.rest["other"], "x");
    }
}
