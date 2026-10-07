//! Google as the OpenID Connect provider: the authorization URL, the code
//! exchange, and verifying the ID token against Google's published keys.
//!
//! The token exchange and the key fetch are blocking HTTP on the client the
//! crate already has; the caller runs them off the async runtime. Keys are
//! cached by key id with a lifetime; an unknown key id refetches once, since
//! Google rotates keys.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde::Deserialize;

pub const ISSUERS: [&str; 2] = ["https://accounts.google.com", "accounts.google.com"];
const JWKS_LIFETIME: Duration = Duration::from_secs(60 * 60);

#[derive(Debug, Clone)]
pub struct Endpoints {
    pub authorization: String,
    pub token: String,
    pub jwks: String,
}

impl Default for Endpoints {
    fn default() -> Self {
        Self {
            authorization: "https://accounts.google.com/o/oauth2/v2/auth".to_owned(),
            token: "https://oauth2.googleapis.com/token".to_owned(),
            jwks: "https://www.googleapis.com/oauth2/v3/certs".to_owned(),
        }
    }
}

/// What the ID token says about the person.
#[derive(Debug, Clone, Deserialize)]
pub struct Claims {
    pub email: Option<String>,
    #[serde(default)]
    pub email_verified: bool,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub picture: String,
    #[serde(default)]
    pub nonce: Option<String>,
}

fn q(s: &str) -> String {
    url_escape(s)
}

/// Percent-encode a query value: unreserved characters pass, the rest is
/// `%XX`. Enough for ids, URLs and nonces; no dependency for it.
fn url_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// The `nonce` is bound into the ID token by Google and checked on the way
/// back, so a token minted for another login cannot be replayed into this one.
pub fn authorization_url(
    e: &Endpoints,
    client_id: &str,
    redirect_uri: &str,
    state: &str,
    nonce: &str,
) -> String {
    format!(
        "{}?client_id={}&redirect_uri={}&response_type=code&scope={}&state={}&nonce={}&prompt=select_account",
        e.authorization,
        q(client_id),
        q(redirect_uri),
        q("openid email profile"),
        q(state),
        q(nonce)
    )
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    id_token: Option<String>,
    error: Option<String>,
    error_description: Option<String>,
}

/// The ID token for an authorization code. Blocking.
pub fn exchange_code(
    e: &Endpoints,
    client_id: &str,
    client_secret: &str,
    code: &str,
    redirect_uri: &str,
) -> Result<String, String> {
    let body = format!(
        "code={}&client_id={}&client_secret={}&redirect_uri={}&grant_type=authorization_code",
        q(code),
        q(client_id),
        q(client_secret),
        q(redirect_uri)
    );
    let resp = ureq::post(&e.token)
        .set("content-type", "application/x-www-form-urlencoded")
        .timeout(Duration::from_secs(10))
        .send_string(&body);
    let text = match resp {
        Ok(r) => r
            .into_string()
            .map_err(|e| format!("token response: {e}"))?,
        Err(ureq::Error::Status(_, r)) => r.into_string().unwrap_or_default(),
        Err(e) => return Err(format!("token exchange: {e}")),
    };
    let t: TokenResponse = serde_json::from_str(&text)
        .map_err(|_| "token response is not the expected JSON".to_owned())?;
    if let Some(err) = t.error {
        return Err(format!(
            "{err}: {}",
            t.error_description.unwrap_or_default()
        ));
    }
    t.id_token
        .ok_or_else(|| "no id_token in the token response".to_owned())
}

pub struct Verifier {
    client_id: String,
    jwks_url: String,
    cache: Mutex<Option<(JwkSet, Instant)>>,
    /// Set for tests: keys that are never refetched.
    fixed: bool,
}

impl Verifier {
    pub fn new(client_id: String, e: Endpoints) -> Self {
        Self {
            client_id,
            jwks_url: e.jwks,
            cache: Mutex::new(None),
            fixed: false,
        }
    }

    /// A verifier over keys given directly, for tests and for a stub issuer.
    pub fn with_keys(client_id: String, keys: JwkSet) -> Self {
        Self {
            client_id,
            jwks_url: String::new(),
            cache: Mutex::new(Some((keys, Instant::now()))),
            fixed: true,
        }
    }

    fn fetch(&self) -> Result<JwkSet, String> {
        let text = ureq::get(&self.jwks_url)
            .timeout(Duration::from_secs(10))
            .call()
            .map_err(|e| format!("fetching Google's signing keys: {e}"))?
            .into_string()
            .map_err(|e| format!("reading Google's signing keys: {e}"))?;
        serde_json::from_str(&text).map_err(|e| format!("Google's signing keys did not parse: {e}"))
    }

    fn key_for(&self, kid: &str, allow_refetch: bool) -> Result<Option<DecodingKey>, String> {
        let mut guard = self
            .cache
            .lock()
            .map_err(|_| "key cache poisoned".to_owned())?;
        let stale = guard
            .as_ref()
            .map(|(_, at)| at.elapsed() > JWKS_LIFETIME)
            .unwrap_or(true);
        let missing = guard
            .as_ref()
            .map(|(set, _)| set.find(kid).is_none())
            .unwrap_or(true);
        if !self.fixed && allow_refetch && (stale || missing) {
            *guard = Some((self.fetch()?, Instant::now()));
        }
        let Some((set, _)) = guard.as_ref() else {
            return Ok(None);
        };
        match set.find(kid) {
            Some(jwk) => DecodingKey::from_jwk(jwk)
                .map(Some)
                .map_err(|e| format!("Google's signing key {kid} is unusable: {e}")),
            None => Ok(None),
        }
    }

    /// The claims of a valid ID token: our client as the audience, Google as
    /// the issuer, unexpired, signed by a published key, with a verified
    /// email.
    pub fn verify(&self, id_token: &str, expected_nonce: Option<&str>) -> Result<Claims, String> {
        let header = decode_header(id_token).map_err(|e| format!("id_token header: {e}"))?;
        if header.alg != Algorithm::RS256 {
            return Err(format!("id_token algorithm {:?} is not RS256", header.alg));
        }
        let kid = header.kid.ok_or("id_token names no key")?;
        let key = self
            .key_for(&kid, true)?
            .ok_or_else(|| format!("id_token signed by an unknown key {kid}"))?;
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_audience(&[self.client_id.as_str()]);
        validation.set_issuer(&ISSUERS);
        let data =
            decode::<Claims>(id_token, &key, &validation).map_err(|e| format!("id_token: {e}"))?;
        let claims = data.claims;
        if !claims.email_verified {
            return Err("Google has not verified this email".to_owned());
        }
        if claims.email.as_deref().unwrap_or("").is_empty() {
            return Err("id_token carries no email".to_owned());
        }
        if let Some(n) = expected_nonce {
            if claims.nonce.as_deref() != Some(n) {
                return Err("id_token nonce does not match this login".to_owned());
            }
        }
        Ok(claims)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, EncodingKey, Header};
    use serde::Serialize;

    use rsa::pkcs1::EncodeRsaPrivateKey;
    use rsa::traits::PublicKeyParts;
    use std::sync::OnceLock;

    /// A key pair made once per test run, standing in for Google's: the
    /// private half signs test tokens, the public half is served as a JWKS.
    fn test_key() -> &'static (String, String) {
        static KEY: OnceLock<(String, String)> = OnceLock::new();
        KEY.get_or_init(|| {
            use base64::Engine as _;
            let key = rsa::RsaPrivateKey::new(&mut rand::thread_rng(), 2048).unwrap();
            let pem = key
                .to_pkcs1_pem(rsa::pkcs1::LineEnding::LF)
                .unwrap()
                .to_string();
            let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
            let jwks = serde_json::json!({ "keys": [{
                "kty": "RSA", "alg": "RS256", "use": "sig", "kid": "test-key-1",
                "n": b64.encode(key.n().to_bytes_be()),
                "e": b64.encode(key.e().to_bytes_be()),
            }] })
            .to_string();
            (pem, jwks)
        })
    }

    #[derive(Serialize)]
    struct TestClaims<'a> {
        iss: &'a str,
        aud: &'a str,
        sub: &'a str,
        email: &'a str,
        email_verified: bool,
        name: &'a str,
        exp: u64,
        iat: u64,
        nonce: &'a str,
    }

    fn token(
        iss: &str,
        aud: &str,
        email: &str,
        verified: bool,
        exp_offset: i64,
        kid: &str,
    ) -> String {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let claims = TestClaims {
            iss,
            aud,
            sub: "123",
            email,
            email_verified: verified,
            name: "Asha",
            exp: (now + exp_offset) as u64,
            iat: now as u64,
            nonce: "n0nce",
        };
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(kid.to_owned());
        encode(
            &header,
            &claims,
            &EncodingKey::from_rsa_pem(test_key().0.as_bytes()).unwrap(),
        )
        .unwrap()
    }

    fn verifier() -> Verifier {
        Verifier::with_keys("cid".into(), serde_json::from_str(&test_key().1).unwrap())
    }

    #[test]
    fn a_token_from_google_for_us_with_a_verified_email_passes() {
        let c = verifier()
            .verify(
                &token(
                    "https://accounts.google.com",
                    "cid",
                    "asha@juspay.in",
                    true,
                    300,
                    "test-key-1",
                ),
                Some("n0nce"),
            )
            .unwrap();
        assert_eq!(c.email.as_deref(), Some("asha@juspay.in"));
        assert_eq!(c.name, "Asha");
    }

    #[test]
    fn the_wrong_audience_issuer_key_or_an_expired_token_is_refused() {
        let v = verifier();
        let good = |iss: &str, aud: &str, ok: bool, off: i64, kid: &str| {
            token(iss, aud, "a@juspay.in", ok, off, kid)
        };
        assert!(v
            .verify(
                &good(
                    "https://accounts.google.com",
                    "other",
                    true,
                    300,
                    "test-key-1"
                ),
                None
            )
            .is_err());
        assert!(v
            .verify(
                &good("https://evil.example", "cid", true, 300, "test-key-1"),
                None
            )
            .is_err());
        assert!(v
            .verify(
                &good(
                    "https://accounts.google.com",
                    "cid",
                    true,
                    -300,
                    "test-key-1"
                ),
                None
            )
            .is_err());
        assert!(v
            .verify(
                &good(
                    "https://accounts.google.com",
                    "cid",
                    true,
                    300,
                    "no-such-key"
                ),
                None
            )
            .is_err());
        assert!(v
            .verify(
                &good(
                    "https://accounts.google.com",
                    "cid",
                    false,
                    300,
                    "test-key-1"
                ),
                None
            )
            .is_err());
        assert!(v
            .verify(
                &good(
                    "https://accounts.google.com",
                    "cid",
                    true,
                    300,
                    "test-key-1"
                ),
                Some("other-nonce")
            )
            .is_err());
    }

    #[test]
    fn the_authorization_url_names_our_client_and_the_callback() {
        let u = authorization_url(
            &Endpoints::default(),
            "cid",
            "https://deja.example/auth/callback",
            "st4te",
            "n0nce",
        );
        assert!(u.starts_with("https://accounts.google.com/o/oauth2/v2/auth?client_id=cid&redirect_uri=https%3A%2F%2Fdeja.example%2Fauth%2Fcallback"));
        assert!(
            u.contains("state=st4te")
                && u.contains("nonce=n0nce")
                && u.contains("scope=openid%20email%20profile")
        );
    }
}
