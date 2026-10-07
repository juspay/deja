//! Sign-in for the people behind the actions that need one: acknowledging a
//! divergence now, promoting a version later.
//!
//! The shape is hyperops' (juspay/hyperops, `internal/authn`): Google's
//! OpenID Connect code flow inside the server, an identity-only session
//! cookie signed with HMAC, a domain allowlist at login, and roles resolved
//! on every request from the configuration rather than frozen into the
//! cookie. What differs is the scope: only the gated routes need a person;
//! reading, creating runs and the pipeline's callbacks stay as they were.
//!
//! Three parts, each testable alone: [`session`] signs and verifies the two
//! cookies; [`google`] builds the authorization URL, exchanges the code and
//! verifies the ID token against Google's published keys; this module holds
//! the configuration snapshot and answers who may do what. The HTTP routes
//! and the middleware live in the binary, next to the other routes.

pub mod google;
pub mod session;

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use deja_compactor::settings::{self, AuthSettings};

pub const SESSION_COOKIE: &str = "deja.session";
pub const STATE_COOKIE: &str = "deja.oauth_state";
pub const ROLE_MAINTAINER: &str = "maintainer";
pub const DEFAULT_DOMAIN: &str = "@juspay.in";
pub const DEFAULT_ROLE: &str = "viewer";
pub const DEFAULT_SESSION: Duration = Duration::from_secs(24 * 60 * 60);
pub const RELOAD_EVERY: Duration = Duration::from_secs(15);

/// The declared configuration, resolved: what the routes and the gate read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthConfig {
    pub enabled: bool,
    pub client_id: String,
    pub client_secret: String,
    /// Signs the cookies. Required when enabled.
    pub session_secret: String,
    pub session_duration: Duration,
    /// Whether the cookies carry `Secure`. Default true.
    pub cookie_secure: bool,
    /// Lowercased suffixes.
    pub domains: Vec<String>,
    pub default_role: String,
    /// Role to lowercased emails.
    pub roles: BTreeMap<String, Vec<String>>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            client_id: String::new(),
            client_secret: String::new(),
            session_secret: String::new(),
            session_duration: DEFAULT_SESSION,
            cookie_secure: true,
            domains: vec![DEFAULT_DOMAIN.to_owned()],
            default_role: DEFAULT_ROLE.to_owned(),
            roles: BTreeMap::new(),
        }
    }
}

impl AuthConfig {
    pub fn from_settings(s: &AuthSettings) -> Result<Self, String> {
        let session_duration = match s.session_duration.as_deref().map(str::trim) {
            None | Some("") => DEFAULT_SESSION,
            Some(d) => parse_duration(d)?,
        };
        let domains: Vec<String> = s
            .domain_allowlist
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(|d| d.trim().to_ascii_lowercase())
            .filter(|d| !d.is_empty())
            .collect();
        // A suffix without its `@` is a different rule from the one the key
        // names: `juspay.in` would admit `x@notjuspay.in`.
        if let Some(bad) = domains.iter().find(|d| !d.starts_with('@')) {
            return Err(format!(
                "auth.domain_allowlist entries are email suffixes starting with '@', got {bad:?}"
            ));
        }
        let roles = s
            .roles
            .iter()
            .map(|(role, who)| {
                (
                    role.trim().to_ascii_lowercase(),
                    who.0
                        .iter()
                        .map(|e| e.trim().to_ascii_lowercase())
                        .collect(),
                )
            })
            .collect();
        let cfg = Self {
            enabled: s.enabled,
            client_id: s.client_id.clone().unwrap_or_default().trim().to_owned(),
            client_secret: s
                .client_secret
                .clone()
                .unwrap_or_default()
                .trim()
                .to_owned(),
            session_secret: s
                .session_secret
                .clone()
                .unwrap_or_default()
                .trim()
                .to_owned(),
            session_duration,
            cookie_secure: s.cookie_secure.unwrap_or(true),
            domains: if domains.is_empty() {
                vec![DEFAULT_DOMAIN.to_owned()]
            } else {
                domains
            },
            default_role: s
                .default_role
                .clone()
                .filter(|r| !r.trim().is_empty())
                .unwrap_or_else(|| DEFAULT_ROLE.to_owned()),
            roles,
        };
        if cfg.enabled
            && (cfg.client_id.is_empty()
                || cfg.client_secret.is_empty()
                || cfg.session_secret.is_empty())
        {
            return Err(
                "auth.enabled needs auth.client_id, auth.client_secret and auth.session_secret"
                    .to_owned(),
            );
        }
        Ok(cfg)
    }

    /// Whether an email may sign in: a case-insensitive suffix match, since
    /// email domains are case-insensitive and Google may vary the casing.
    pub fn domain_allowed(&self, email: &str) -> bool {
        let lower = email.trim().to_ascii_lowercase();
        self.domains.iter().any(|d| lower.ends_with(d.as_str()))
    }

    /// The roles an email holds: the default one plus every list naming it.
    pub fn roles_for(&self, email: &str) -> Vec<String> {
        let lower = email.trim().to_ascii_lowercase();
        let mut out = vec![self.default_role.clone()];
        for (role, who) in &self.roles {
            if who.contains(&lower) && !out.contains(role) {
                out.push(role.clone());
            }
        }
        out
    }

    pub fn has_role(&self, email: &str, role: &str) -> bool {
        self.roles_for(email).iter().any(|r| r == role)
    }
}

/// `24h`, `30m`, `7d`, `900s`.
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    let (num, unit) = s.split_at(s.trim_end_matches(|c: char| c.is_ascii_alphabetic()).len());
    let n: u64 = num.parse().map_err(|_| {
        format!("auth.session_duration: {s:?} is not a number followed by s, m, h or d")
    })?;
    let secs = match unit {
        "s" => n,
        "m" => n * 60,
        "h" => n * 3600,
        "d" => n * 86_400,
        _ => {
            return Err(format!(
                "auth.session_duration: {s:?} needs a unit: s, m, h or d"
            ))
        }
    };
    Ok(Duration::from_secs(secs))
}

/// What the server holds: the configuration, re-read on a timer so a change
/// to the lists applies on the next request; the cookie signer; the verifier
/// for Google's ID tokens.
pub struct AuthState {
    config: RwLock<AuthConfig>,
    pub signer: session::Signer,
    pub verifier: google::Verifier,
    pub endpoints: google::Endpoints,
}

impl AuthState {
    /// From the declared settings. No `[auth]` block is sign-in off. A block
    /// that is declared and does not resolve — a secret that failed to mount,
    /// a duration that does not parse — is an error the caller must refuse
    /// to start on: silently running without the gate the deployment asked
    /// for would reopen every route to a typed name.
    pub fn from_settings() -> Result<Arc<Self>, String> {
        let s = settings::load().map_err(|e| format!("settings: {e}"))?;
        let cfg = match s.auth.as_ref() {
            None => AuthConfig::default(),
            Some(a) => AuthConfig::from_settings(a).map_err(|e| format!("[auth]: {e}"))?,
        };
        let signer = if cfg.enabled {
            session::Signer::new(cfg.session_secret.as_bytes())
        } else {
            session::Signer::quiet_random()
        };
        let verifier = google::Verifier::new(cfg.client_id.clone(), google::Endpoints::default());
        Ok(Arc::new(Self {
            config: RwLock::new(cfg),
            signer,
            verifier,
            endpoints: google::Endpoints::default(),
        }))
    }

    /// Sign-in off, with nothing configured: what a deployment without an
    /// `[auth]` block gets, and what tests start from.
    pub fn disabled() -> Arc<Self> {
        Arc::new(Self {
            config: RwLock::new(AuthConfig::default()),
            signer: session::Signer::quiet_random(),
            verifier: google::Verifier::new(String::new(), google::Endpoints::default()),
            endpoints: google::Endpoints::default(),
        })
    }

    /// From parts already built: tests, and a stub issuer in development.
    pub fn with_parts(
        cfg: AuthConfig,
        signer: session::Signer,
        verifier: google::Verifier,
    ) -> Arc<Self> {
        Arc::new(Self {
            config: RwLock::new(cfg),
            signer,
            verifier,
            endpoints: google::Endpoints::default(),
        })
    }

    pub fn config(&self) -> AuthConfig {
        self.config.read().map(|c| c.clone()).unwrap_or_default()
    }

    pub fn enabled(&self) -> bool {
        self.config.read().map(|c| c.enabled).unwrap_or(false)
    }

    /// Re-read the lists and the switch. The client id is not swapped, since
    /// the verifier was built for it; a changed client id needs a restart,
    /// which a changed secret does anyway. A document that no longer carries
    /// the block while sign-in is on is refused, the last good configuration
    /// kept: a gate is never lowered by a half-written file or a remount.
    pub fn reload(&self) -> Result<(), String> {
        let s = settings::load()?;
        let next = match s.auth.as_ref() {
            Some(a) => AuthConfig::from_settings(a)?,
            None if self.enabled() => {
                return Err(
                    "the [auth] block is gone from the settings while sign-in is on".to_owned(),
                )
            }
            None => AuthConfig::default(),
        };
        if let Ok(mut cur) = self.config.write() {
            cur.enabled = next.enabled;
            cur.domains = next.domains;
            cur.default_role = next.default_role;
            cur.roles = next.roles;
            cur.session_duration = next.session_duration;
            cur.cookie_secure = next.cookie_secure;
        }
        Ok(())
    }
}

/// A return URL the login may send a browser back to: a path on this origin,
/// never a scheme or a protocol-relative `//host`. Anything else becomes `/`.
pub fn safe_return_url(raw: Option<&str>) -> String {
    match raw.map(str::trim) {
        Some(p)
            if p.starts_with('/')
                && !p.starts_with("//")
                && !p.contains('\\')
                && !p.contains("://") =>
        {
            p.to_owned()
        }
        _ => "/".to_owned(),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn settings(roles: &[(&str, &[&str])]) -> AuthSettings {
        AuthSettings {
            enabled: true,
            client_id: Some("cid".into()),
            client_secret: Some("sec".into()),
            session_secret: Some("k".into()),
            session_duration: Some("12h".into()),
            cookie_secure: None,
            domain_allowlist: Some(vec!["@juspay.in".into()]),
            default_role: None,
            roles: roles
                .iter()
                .map(|(r, who)| {
                    (
                        (*r).to_owned(),
                        deja_compactor::settings::Emails(
                            who.iter().map(|s| (*s).to_owned()).collect(),
                        ),
                    )
                })
                .collect(),
        }
    }

    #[test]
    fn the_domain_gate_is_a_case_insensitive_suffix() {
        let cfg = AuthConfig::from_settings(&settings(&[])).unwrap();
        assert!(cfg.domain_allowed("Asha@Juspay.IN"));
        assert!(!cfg.domain_allowed("asha@juspay.in.evil.com"));
        assert!(!cfg.domain_allowed("asha@example.com"));
    }

    #[test]
    fn roles_are_the_default_plus_every_list_naming_the_email() {
        let cfg =
            AuthConfig::from_settings(&settings(&[("Maintainer", &["Ravi@juspay.in"])])).unwrap();
        assert_eq!(
            cfg.roles_for("ravi@juspay.in"),
            vec!["viewer", "maintainer"]
        );
        assert_eq!(cfg.roles_for("asha@juspay.in"), vec!["viewer"]);
        assert!(cfg.has_role("RAVI@juspay.in", ROLE_MAINTAINER));
        assert!(!cfg.has_role("asha@juspay.in", ROLE_MAINTAINER));
    }

    #[test]
    fn an_allowlist_entry_is_a_suffix_with_its_at_sign() {
        let mut s = settings(&[]);
        s.domain_allowlist = Some(vec!["juspay.in".into()]);
        let err = AuthConfig::from_settings(&s).err().unwrap_or_default();
        assert!(err.contains("'@'"), "{err}");
        s.domain_allowlist = Some(vec!["@juspay.in".into(), " @Example.com ".into()]);
        let cfg = AuthConfig::from_settings(&s).unwrap();
        assert!(cfg.domain_allowed("x@example.com"));
        assert!(!cfg.domain_allowed("x@notjuspay.in"));
    }

    #[test]
    fn enabling_needs_a_session_secret_and_the_cookie_is_secure_unless_said() {
        let mut s = settings(&[]);
        assert!(AuthConfig::from_settings(&s).unwrap().cookie_secure);
        s.cookie_secure = Some(false);
        assert!(!AuthConfig::from_settings(&s).unwrap().cookie_secure);
        s.session_secret = Some("  ".into());
        let err = AuthConfig::from_settings(&s).err().unwrap_or_default();
        assert!(err.contains("session_secret"), "{err}");
    }

    #[test]
    fn enabling_without_a_client_is_refused_and_durations_parse() {
        let mut s = settings(&[]);
        s.client_secret = None;
        assert!(AuthConfig::from_settings(&s).is_err());
        assert_eq!(
            parse_duration("12h").unwrap(),
            Duration::from_secs(12 * 3600)
        );
        assert_eq!(
            parse_duration("7d").unwrap(),
            Duration::from_secs(7 * 86_400)
        );
        assert!(parse_duration("12").is_err());
        assert!(parse_duration("soon").is_err());
    }

    #[test]
    fn a_return_url_is_a_path_on_this_origin_or_nothing() {
        assert_eq!(safe_return_url(Some("/r/abc?x=1")), "/r/abc?x=1");
        assert_eq!(safe_return_url(Some("//evil.example/x")), "/");
        assert_eq!(safe_return_url(Some("https://evil.example/")), "/");
        assert_eq!(safe_return_url(Some("/a\\b")), "/");
        assert_eq!(safe_return_url(None), "/");
        assert_eq!(safe_return_url(Some("relative")), "/");
    }
}
