//! The two cookies, signed the same way: `base64url(json) . base64url(hmac)`.
//!
//! A session is identity only: email, name, picture, expiry. Roles are never
//! in it, so a change to the lists applies on the next request instead of
//! on the next login. The state cookie carries the OAuth nonce and the return
//! URL for the ten minutes a person may sit on Google's consent screen.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub const STATE_MAX_AGE: Duration = Duration::from_secs(10 * 60);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Session {
    /// The issuer's stable id for the account (Google's `sub`). Empty on a
    /// cookie minted before it was carried.
    #[serde(default)]
    pub sub: String,
    pub email: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub picture: String,
    /// Unix seconds.
    pub exp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct State {
    /// The `state` query parameter: the CSRF check.
    nonce: String,
    /// The `nonce` bound into the ID token: the replay check.
    id_nonce: String,
    return_url: String,
    exp: u64,
}

pub struct Signer {
    key: Vec<u8>,
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub fn random_nonce() -> String {
    let mut bytes = [0u8; 24];
    rand::thread_rng().fill_bytes(&mut bytes);
    B64.encode(bytes)
}

impl Signer {
    /// An empty secret gets a random key, and sessions end with the process.
    pub fn new(secret: &[u8]) -> Self {
        if secret.is_empty() {
            let mut key = vec![0u8; 32];
            rand::thread_rng().fill_bytes(&mut key);
            eprintln!(
                "deja-orchestrator: auth.session_secret is not set; sessions will not survive a restart"
            );
            return Self { key };
        }
        Self {
            key: secret.to_vec(),
        }
    }

    /// A random key without the warning, for a deployment with sign-in off.
    pub fn quiet_random() -> Self {
        let mut key = vec![0u8; 32];
        rand::thread_rng().fill_bytes(&mut key);
        Self { key }
    }

    fn sign(&self, payload: &[u8]) -> String {
        let mut mac = HmacSha256::new_from_slice(&self.key).expect("hmac accepts any key length");
        mac.update(payload);
        format!(
            "{}.{}",
            B64.encode(payload),
            B64.encode(mac.finalize().into_bytes())
        )
    }

    fn verify(&self, token: &str) -> Result<Vec<u8>, String> {
        let (payload, sig) = token.split_once('.').ok_or("malformed signed token")?;
        let payload = B64.decode(payload).map_err(|_| "malformed signed token")?;
        let sig = B64.decode(sig).map_err(|_| "malformed signed token")?;
        let mut mac = HmacSha256::new_from_slice(&self.key).expect("hmac accepts any key length");
        mac.update(&payload);
        mac.verify_slice(&sig).map_err(|_| "invalid signature")?;
        Ok(payload)
    }

    pub fn sign_session(&self, session: &Session) -> String {
        self.sign(&serde_json::to_vec(session).unwrap_or_default())
    }

    pub fn verify_session(&self, token: &str) -> Result<Session, String> {
        let payload = self.verify(token)?;
        let s: Session = serde_json::from_slice(&payload).map_err(|_| "malformed session")?;
        if now() >= s.exp {
            return Err("session expired".to_owned());
        }
        Ok(s)
    }

    pub fn new_session(
        &self,
        sub: &str,
        email: &str,
        name: &str,
        picture: &str,
        ttl: Duration,
    ) -> Session {
        Session {
            sub: sub.to_owned(),
            email: email.to_owned(),
            name: name.to_owned(),
            picture: picture.to_owned(),
            exp: now() + ttl.as_secs(),
        }
    }

    pub fn sign_state(&self, nonce: &str, id_nonce: &str, return_url: &str) -> String {
        let st = State {
            nonce: nonce.to_owned(),
            id_nonce: id_nonce.to_owned(),
            return_url: return_url.to_owned(),
            exp: now() + STATE_MAX_AGE.as_secs(),
        };
        self.sign(&serde_json::to_vec(&st).unwrap_or_default())
    }

    /// The return URL and the ID-token nonce, when the cookie is ours,
    /// unexpired, and names the `state` Google sent back.
    pub fn verify_state(&self, cookie: &str, state: &str) -> Result<(String, String), String> {
        let payload = self.verify(cookie)?;
        let st: State = serde_json::from_slice(&payload).map_err(|_| "malformed state")?;
        if now() >= st.exp {
            return Err("sign-in took too long; start again".to_owned());
        }
        if st.nonce != state {
            return Err("state mismatch".to_owned());
        }
        Ok((st.return_url, st.id_nonce))
    }
}

/// The value of one cookie in a `Cookie:` header.
pub fn cookie_value<'a>(header: &'a str, name: &str) -> Option<&'a str> {
    header.split(';').find_map(|part| {
        let (k, v) = part.trim().split_once('=')?;
        (k.trim() == name).then_some(v.trim())
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn a_session_round_trips_and_a_tampered_one_does_not() {
        let s = Signer::new(b"k");
        let ses = s.new_session(
            "sub-1",
            "asha@juspay.in",
            "Asha",
            "",
            Duration::from_secs(60),
        );
        let tok = s.sign_session(&ses);
        assert_eq!(s.verify_session(&tok).unwrap(), ses);
        let mut bad = tok.clone();
        bad.replace_range(0..1, if tok.starts_with('A') { "B" } else { "A" });
        assert!(s.verify_session(&bad).is_err());
        assert!(Signer::new(b"other").verify_session(&tok).is_err());
    }

    #[test]
    fn an_expired_session_is_refused() {
        let s = Signer::new(b"k");
        let ses = Session {
            sub: String::new(),
            email: "a@juspay.in".into(),
            name: String::new(),
            picture: String::new(),
            exp: now() - 1,
        };
        assert!(s.verify_session(&s.sign_session(&ses)).is_err());
    }

    #[test]
    fn the_state_cookie_names_the_state_google_sends_back() {
        let s = Signer::new(b"k");
        let nonce = random_nonce();
        let c = s.sign_state(&nonce, "idn", "/r/x");
        assert_eq!(
            s.verify_state(&c, &nonce).unwrap(),
            ("/r/x".to_owned(), "idn".to_owned())
        );
        assert!(s.verify_state(&c, "someone-elses").is_err());
    }

    #[test]
    fn a_cookie_is_found_by_name_in_the_header() {
        assert_eq!(
            cookie_value("a=1; deja.session=abc.def; b=2", "deja.session"),
            Some("abc.def")
        );
        assert_eq!(cookie_value("a=1", "deja.session"), None);
    }
}
