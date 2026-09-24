//! The capture key: a deployment secret that turns captured values into keyed
//! digests, so a structured capture can reach the tape without its contents.
//!
//! A digest is deterministic — the same value under the same key always yields
//! the same digest — so the recorder and the replay candidate, configured with
//! one key, produce comparable images, and the address a digest feeds survives.
//! It is keyed because many captured values are low-entropy (an email, a zip
//! code, a flag): an unkeyed hash of them is reversed by guessing.
//!
//! The key never leaves this module. A capture names it only by its id, an HMAC
//! of a fixed label, which identifies the key without revealing it.

use std::sync::OnceLock;

use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Label the key id is derived from. Changing it changes every key id.
const KEY_ID_LABEL: &[u8] = b"deja-capture-key-id";

/// Prefix of a digested leaf, so a digest is never mistaken for a captured value.
pub const DIGEST_PREFIX: &str = "h:";

/// A configured capture key.
#[derive(Clone)]
pub struct CaptureKey {
    key: Vec<u8>,
    id: String,
}

impl std::fmt::Debug for CaptureKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CaptureKey")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl CaptureKey {
    /// A key from its secret bytes. An empty secret is no key at all.
    pub fn new(secret: &[u8]) -> Option<Self> {
        if secret.is_empty() {
            return None;
        }
        let key = secret.to_vec();
        let id = hex::encode(&mac(&key, KEY_ID_LABEL, &[])[..8]);
        Some(Self { key, id })
    }

    /// The key's public identity, stamped on every image it produced.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Digest one scalar. `tag` separates kinds, so the string `"1"` and the
    /// number `1` digest differently.
    pub fn digest(&self, tag: &[u8], bytes: &[u8]) -> String {
        format!(
            "{DIGEST_PREFIX}{}",
            hex::encode(&mac(&self.key, tag, bytes)[..16])
        )
    }

    /// Replace every scalar leaf of a JSON document with its digest, keeping
    /// every object key and every array position. `null` stays `null`.
    pub fn digest_leaves(&self, value: &serde_json::Value) -> serde_json::Value {
        use serde_json::Value;
        match value {
            Value::Null => Value::Null,
            Value::Bool(flag) => Value::String(self.digest(b"t", &[u8::from(*flag)])),
            Value::Number(number) => {
                Value::String(self.digest(b"n", number.to_string().as_bytes()))
            }
            Value::String(text) => Value::String(self.digest(b"s", text.as_bytes())),
            Value::Array(items) => {
                Value::Array(items.iter().map(|item| self.digest_leaves(item)).collect())
            }
            Value::Object(map) => Value::Object(
                map.iter()
                    .map(|(key, item)| (key.clone(), self.digest_leaves(item)))
                    .collect(),
            ),
        }
    }
}

fn mac(key: &[u8], tag: &[u8], bytes: &[u8]) -> Vec<u8> {
    // HMAC accepts a key of any length, so construction cannot fail.
    let Ok(mut mac) = HmacSha256::new_from_slice(key) else {
        unreachable!("HMAC accepts keys of any length")
    };
    mac.update(tag);
    mac.update(&[0]);
    mac.update(bytes);
    mac.finalize().into_bytes().to_vec()
}

static CAPTURE_KEY: OnceLock<Option<CaptureKey>> = OnceLock::new();

/// Install the process's capture key, once, at boot. `None` or an empty secret
/// means no key: captures then fall back to their masked form.
pub fn install_capture_key(secret: Option<&[u8]>) -> Result<(), &'static str> {
    CAPTURE_KEY
        .set(secret.and_then(CaptureKey::new))
        .map_err(|_| "capture key already installed")
}

/// The installed capture key, if any.
pub fn capture_key() -> Option<&'static CaptureKey> {
    CAPTURE_KEY.get().and_then(Option::as_ref)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(secret: &str) -> CaptureKey {
        CaptureKey::new(secret.as_bytes()).expect("non-empty")
    }

    #[test]
    fn an_empty_secret_is_no_key() {
        assert!(CaptureKey::new(b"").is_none());
    }

    /// The whole design rests on this: recorder and candidate, holding one key,
    /// must produce the same digest for the same value.
    #[test]
    fn a_digest_is_deterministic_under_one_key() {
        assert_eq!(key("k").digest(b"s", b"x"), key("k").digest(b"s", b"x"));
    }

    #[test]
    fn a_digest_depends_on_the_key_the_tag_and_the_value() {
        let base = key("k").digest(b"s", b"x");
        assert_ne!(base, key("other").digest(b"s", b"x"));
        assert_ne!(base, key("k").digest(b"n", b"x"));
        assert_ne!(base, key("k").digest(b"s", b"y"));
    }

    /// The id names the key without being derivable into it: two keys, two ids;
    /// one key, one id; and the id is not the secret.
    #[test]
    fn the_key_id_identifies_the_key_without_carrying_it() {
        assert_eq!(key("secret").id(), key("secret").id());
        assert_ne!(key("secret").id(), key("secret2").id());
        assert!(!key("secret").id().contains("secret"));
        assert_eq!(key("secret").id().len(), 16);
    }

    /// Structure survives; no scalar does. Keys are kept, positions are kept,
    /// and the plaintext appears nowhere in the image.
    #[test]
    fn digesting_keeps_the_shape_and_drops_every_scalar() {
        let value = serde_json::json!({
            "email": "a@b.co",
            "flags": [true, 7, "x"],
            "nested": { "zip": "560001", "none": null }
        });
        let image = key("k").digest_leaves(&value);
        fn every_leaf_is_a_digest_or_null(value: &serde_json::Value) -> bool {
            match value {
                serde_json::Value::Null => true,
                serde_json::Value::String(leaf) => leaf.starts_with(DIGEST_PREFIX),
                serde_json::Value::Array(items) => items.iter().all(every_leaf_is_a_digest_or_null),
                serde_json::Value::Object(map) => map.values().all(every_leaf_is_a_digest_or_null),
                serde_json::Value::Bool(_) | serde_json::Value::Number(_) => false,
            }
        }
        assert!(every_leaf_is_a_digest_or_null(&image), "{image}");
        let text = image.to_string();
        assert!(
            !text.contains("a@b.co") && !text.contains("560001"),
            "{text}"
        );
        assert_eq!(image["flags"].as_array().map(Vec::len), Some(3));
        assert!(image["nested"]["none"].is_null());
        assert!(image["email"]
            .as_str()
            .is_some_and(|d| d.starts_with(DIGEST_PREFIX)));
    }

    /// A value swap must stay visible: `{a:1,b:2}` and `{a:2,b:1}` hold the same
    /// scalars in different places, and their images must differ.
    #[test]
    fn a_value_swap_between_keys_is_not_hidden() {
        let k = key("k");
        let left = k.digest_leaves(&serde_json::json!({ "a": 1, "b": 2 }));
        let right = k.digest_leaves(&serde_json::json!({ "a": 2, "b": 1 }));
        assert_ne!(left, right);
    }

    /// The string `"1"` and the number `1` are different claims.
    #[test]
    fn a_string_and_a_number_with_the_same_text_digest_differently() {
        let k = key("k");
        assert_ne!(
            k.digest_leaves(&serde_json::json!("1")),
            k.digest_leaves(&serde_json::json!(1))
        );
    }

    /// Digesting does not reorder: an array keeps its positions, so a permuted
    /// set stays a permutation the order rule can name.
    #[test]
    fn digesting_keeps_array_order() {
        let k = key("k");
        let forward = k.digest_leaves(&serde_json::json!(["x", "y"]));
        let backward = k.digest_leaves(&serde_json::json!(["y", "x"]));
        assert_ne!(forward, backward);
        assert_eq!(forward[0], backward[1]);
    }
}
