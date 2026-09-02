//! What the orchestrator knows about a system under test, resolved in one place.
//!
//! Per-system facts used to be nine independent `env::var` calls spread across
//! three files, each with its own fallback decided at its own call site. Nothing
//! could answer "what is prism" — only "what is prism's bucket", separately from
//! "does prism manage stores", separately again from "which job template does
//! prism use". That is the producer/consumer split this codebase keeps paying
//! for: each lookup was locally correct, and the set of them was never checked
//! against anything, so `main.rs` grew a two-system `if/else` labelling
//! recordings by name and nothing was in a position to notice.
//!
//! [`SystemConfig`] is the single answer. Resolution rules are unchanged — every
//! field below documents the behaviour it preserves — but they are applied once,
//! from one struct, so a new field is a member rather than a tenth lookup and a
//! new system is an environment block rather than a code change.
//!
//! # A registry, not an enum
//!
//! Systems stay free-form data. An enum would mean a system's own pull request
//! had to add a variant here first, which is the coupling this exists to remove.
//! [`registry`] enumerates what a deployment has declared, for callers that need
//! to LIST systems (the dashboard's picker, `GET /systems`); resolving a single
//! system by name never consults it, so a caller naming a system the registry
//! has not heard of still resolves exactly as it did before.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{default_system, is_default_system};

/// The five candidate-binding slots: which env var name carries each half of
/// the replay contract into the candidate container. Named here so the profile
/// is a table rather than a `match` arm per slot.
pub const CANDIDATE_ENV_SLOTS: [&str; 5] = [
    "MODE_ENV",
    "RUN_ID_ENV",
    "SOURCE_ENV",
    "OBSERVED_ENV",
    "CODE_SHA_ENV",
];

/// The key each slot occupies inside a candidate's configuration, without the
/// prefix that says whose configuration it is.
///
/// These halves are DEJA'S contract, identical for every system, which is what
/// makes the prefix below sufficient. A deployment naming all five in full was
/// restating deja's own convention four more times than necessary, in a form
/// where four could be wrong while the fifth looked right.
const CANDIDATE_ENV_KEYS: [(&str, &str); 5] = [
    ("MODE_ENV", "DEJA__MODE"),
    ("RUN_ID_ENV", "DEJA__RUN_ID"),
    ("SOURCE_ENV", "DEJA__REPLAY__SOURCE"),
    ("OBSERVED_ENV", "DEJA__REPLAY__OBSERVED_SINK"),
    ("CODE_SHA_ENV", "DEJA__IDENTITY__CODE_SHA"),
];

// How a system is declared — the file, the inline document, and the `DEJA__`
// environment convention — is documented on `crate::settings`. There is
// deliberately no table of built-in profiles here: a service's facts (its pod
// names, its span prefixes, its configuration prefix) belong to the deployment
// that runs it, not to the instrument that observes it.
// DEJA_<SYSTEM>_S3_BUCKET=its-recording-bucket
// DEJA_<SYSTEM>_MANAGES_STORES=false
// DEJA_<SYSTEM>_HAS_CODE_BUNDLE=false
// DEJA_<SYSTEM>_INSTANCE_PATTERN=a-substring-of-its-pod-names
// DEJA_<SYSTEM>_SCORED_SPAN_NAMESPACES=its::,span::prefixes::
// DEJA_<SYSTEM>_CANDIDATE_ENV_PREFIX=ITS__
// ```

/// Everything the orchestrator can say about one system, resolved together.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SystemConfig {
    /// The name a caller sends as `system_under_test` / `?system=`.
    pub name: String,
    /// Whether this is the system a caller gets by naming nothing.
    pub is_default: bool,
    /// Where this system's recordings land. `None` means UNDECLARED and the
    /// caller refuses by name — for EVERY system including the default, which
    /// is declared like any other. This deliberately drops the old fallback to
    /// the deployment's own `DEJA_S3_BUCKET`: the default system no longer has
    /// an implicit bucket, so a deployment that declares no systems lists
    /// nothing rather than silently listing the orchestrator's own bucket.
    /// The deploy consequence: the document must be present BEFORE a build
    /// carrying this is rolled out, or the listing endpoint refuses everything.
    pub s3_bucket: Option<String>,
    /// `DEJA_<SYSTEM>_RECORDING_ROOT`. `None` = the deployment-wide root. The
    /// key LAYOUT is shared across systems; only the bucket is per-system.
    pub recording_root: Option<String>,
    /// Whether the harness migrates this system's postgres, gates its schema
    /// fingerprint, flushes redis and materialises the seed plan.
    pub manages_stores: bool,
    /// Whether `manages_stores` was declared or inherited, so the lifecycle can
    /// report which source decided.
    pub manages_stores_declared: Option<bool>,
    /// Whether this system publishes a CodeBundle (the `migrations/` tarball P1
    /// stages). Was `system == DEFAULT_SYSTEM_UNDER_TEST` at the call site,
    /// which says the wrong thing for the same reason `manages_stores` did.
    pub has_code_bundle: bool,
    /// The job-template ConfigMap key. `None` for the default system, which
    /// keeps the executor's base key; any other system resolves
    /// `job.<system>.json` unless it declares otherwise.
    pub job_template_key: Option<String>,
    /// Registry to qualify a bare candidate image reference against.
    pub candidate_image_repo: Option<String>,
    /// A substring of this system's pod names, used ONLY to guess which system
    /// minted a recording when the source bucket does not say. `None` means no
    /// guess is possible, which must read as "unknown", never as "the default".
    pub instance_pattern: Option<String>,
    /// Span-name prefixes this system's instrumentation contract declares as
    /// scored. Deja does not know these; the system does, so it declares them.
    pub scored_span_namespaces: Vec<String>,
    /// Reply canons declared per boundary, in the recorder's own grammar. See
    /// `SystemDeclaration::reply_canons`.
    pub reply_canons: std::collections::BTreeMap<String, String>,
    /// Response paths whose array order is asserted. Empty = order carries no
    /// meaning anywhere; see `SystemDeclaration::ordered_response_paths`.
    pub ordered_response_paths: Vec<String>,
    /// Repo-relative config files its CodeBundle carries besides migrations.
    /// `None` on a system with no bundle, or the default system with nothing
    /// declared (the executor's base list applies).
    pub candidate_config_files: Option<Vec<String>>,
    /// The variable the bundle URI is handed to the init containers in; `None`
    /// means the executor's base name.
    pub code_bundle_uri_env: Option<String>,
    /// Where to copy rendered config from. Absent means NO copy rather than
    /// borrowing the default system's, which would boot a candidate off another
    /// system's environment.
    pub config_source_deployment: Option<String>,
    /// Container within [`Self::config_source_deployment`].
    pub config_source_container: Option<String>,
    /// Declared candidate-binding env var names, keyed by [`CANDIDATE_ENV_SLOTS`].
    /// A slot absent here is not configured for this system and the caller keeps
    /// its base binding's name for that slot.
    pub candidate_env: BTreeMap<String, String>,
    /// Variables this system declared that could not be used, each naming the
    /// variable and what was wrong with it.
    ///
    /// Taking a default on an unparseable value is the right RUNTIME behaviour —
    /// a recording concern must not fail a run, and guessing at `MANAGES_STORES
    /// = perhaps` is worse than inheriting. But silently taking it makes a typo
    /// indistinguishable from an absence, so the deployment gets the safe
    /// behaviour and no way to learn its declaration was ignored. This is the
    /// property worth taking from how the router parses its own config: it
    /// deserializes through `serde_path_to_error` precisely so a bad value names
    /// its own field rather than failing as "invalid type". Same idea, kept as
    /// data rather than an error, and surfaced on `GET /systems`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    /// Set when this system's declaration could not be parsed, naming the
    /// variable and the value. A system carrying one must not be run: its
    /// fields are what it would have had with nothing declared, which is not
    /// what the deployment asked for.
    ///
    /// Distinct from [`Self::warnings`], which are declarations that parsed and
    /// are simply not used. A warning is untidiness; an error means the
    /// deployment stated something the orchestrator could not honour.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

use crate::settings::{self, SystemDeclaration};

/// The declared candidate-binding slot names, keyed by [`CANDIDATE_ENV_SLOTS`].
///
/// A free function rather than a method: `SystemDeclaration` is defined in
/// `deja-compactor` now, so an inherent impl here is not allowed — and it should
/// not be. Which slots a CANDIDATE binds is the resolver's concern, and the
/// sealer that shares the declaration has no candidates to bind.
fn candidate_slots(d: &SystemDeclaration) -> [(&'static str, Option<&String>); 5] {
    [
        ("MODE_ENV", d.candidate_mode_env.as_ref()),
        ("RUN_ID_ENV", d.candidate_run_id_env.as_ref()),
        ("SOURCE_ENV", d.candidate_source_env.as_ref()),
        ("OBSERVED_ENV", d.candidate_observed_env.as_ref()),
        ("CODE_SHA_ENV", d.candidate_code_sha_env.as_ref()),
    ]
}

/// The default the declared configuration names, if it names one.
pub(crate) fn declared_default() -> Option<String> {
    settings::load()
        .ok()
        .and_then(|s| s.default_system)
        .map(|d| d.trim().to_owned())
        .filter(|d| !d.is_empty())
}

/// One system's declaration, from the configuration alone. A system not
/// declared there is undeclared — there is no other place it could be. `Err`
/// is a configuration that could not be read at all, which the caller carries
/// on every system it resolves, since none of them can be trusted.
fn declared(system: &str) -> Result<Option<SystemDeclaration>, String> {
    settings::load().map(|s| s.systems.get(system).cloned())
}

/// Resolve one system. Works for ANY name, registered or not: this is the same
/// resolution the individual lookups did, so a caller naming an unregistered
/// system gets exactly what it got before — which for an undeclared bucket is a
/// refusal naming the variable to set.
#[must_use]
pub fn system_config(name: &str) -> SystemConfig {
    /// What the DOCUMENT is allowed to contribute to a boundary's reply canon, and
    /// why anything else is an error rather than a silent no-op.
    ///
    /// The document is a second contributor of clauses, not a second place to state
    /// a whole-body canon. Only `bag:<one or more paths>` can take effect from here:
    /// a whole-body preset (`bag` alone, `sequence`, `final_state`) or a `project:`
    /// clause would parse, appear on `/systems`, and never be consulted, because
    /// whole-body absorption reads the recorder's declaration alone. A clause that
    /// cannot be honoured is named at resolution, when the deployment applies the
    /// document, rather than discovered after a run absorbs nothing.
    ///
    /// Deliberately strict, and deliberately NOT the parser the recorder's
    /// declarations go through: that one is lenient about unknown presets, which is
    /// pre-existing behaviour for strings already recorded and not this seam's to
    /// change.
    fn document_reply_canon_error(
        reply_canons: &std::collections::BTreeMap<String, String>,
    ) -> Option<String> {
        for (boundary, declaration) in reply_canons {
            let clauses: Vec<&str> = declaration
                .split(';')
                .map(str::trim)
                .filter(|c| !c.is_empty())
                .collect();
            if clauses.is_empty() {
                return Some(format!(
                    "reply canon for boundary `{boundary}` declares no clauses: \
                 remove the entry or give it a `bag:<paths>` clause"
                ));
            }
            for clause in clauses {
                let Some(paths) = clause.strip_prefix("bag:") else {
                    return Some(format!(
                    "reply canon for boundary `{boundary}` has clause `{clause}`: document reply \
                     canons can only contribute `bag:<paths>` clauses, and a whole-body preset \
                     is one only the recorder's own declaration can state"
                ));
                };
                if paths.split(',').map(str::trim).all(str::is_empty) {
                    return Some(format!(
                    "reply canon for boundary `{boundary}` has clause `{clause}`: a `bag:` clause \
                     from the document must name at least one path, since bare `bag` is the \
                     whole-body preset"
                ));
                }
            }
        }
        None
    }

    let is_default = is_default_system(name);
    let warnings: Vec<String> = Vec::new();

    // Undeclared is empty, not defaulted. A configuration that could not be
    // read is carried as `error` on every system — unusable, and saying why.
    let (d, error) = match declared(name) {
        Ok(Some(d)) => (d, None),
        Ok(None) => (SystemDeclaration::default(), None),
        Err(e) => (SystemDeclaration::default(), Some(e)),
    };

    let reply_canons_resolved = d.reply_canons.clone().unwrap_or_default();

    // A prefix names all five slots at once; an explicit per-slot name still
    // wins, for a system whose keys do not follow the convention.
    let prefix = d
        .candidate_env_prefix
        .as_deref()
        .map(str::trim)
        .filter(|p| !p.is_empty());
    let mut candidate_env = BTreeMap::new();
    for (slot, declared_name) in candidate_slots(&d) {
        let value = declared_name.cloned().or_else(|| {
            prefix.map(|p| {
                let key = CANDIDATE_ENV_KEYS
                    .iter()
                    .find(|(s, _)| *s == slot)
                    .map(|(_, k)| *k)
                    .unwrap_or_default();
                format!("{p}{key}")
            })
        });
        if let Some(value) = value {
            candidate_env.insert(slot.to_owned(), value);
        }
    }

    let clean = |v: Option<String>| v.map(|v| v.trim().to_owned()).filter(|v| !v.is_empty());

    SystemConfig {
        name: name.to_owned(),
        is_default,
        s3_bucket: clean(d.s3_bucket),
        recording_root: clean(d.recording_root),
        manages_stores: d.manages_stores.unwrap_or(is_default),
        manages_stores_declared: d.manages_stores,
        has_code_bundle: d.has_code_bundle.unwrap_or(is_default),
        // `None` for the default means the executor's base key; any other
        // system resolves `job.<name>.json` by convention so a launch fails
        // naming the key it wanted rather than borrowing another system's.
        job_template_key: clean(d.job_template_key)
            .or_else(|| (!is_default).then(|| format!("job.{name}.json"))),
        candidate_image_repo: clean(d.candidate_image_repo)
            .map(|v| v.trim_end_matches('/').to_owned()),
        instance_pattern: clean(d.instance_pattern),
        scored_span_namespaces: d.scored_span_namespaces.unwrap_or_default(),
        reply_canons: reply_canons_resolved.clone(),
        ordered_response_paths: d.ordered_response_paths.unwrap_or_default(),
        candidate_config_files: d.candidate_config_files,
        code_bundle_uri_env: clean(d.code_bundle_uri_env),
        config_source_deployment: clean(d.config_source_deployment),
        config_source_container: clean(d.config_source_container),
        candidate_env,
        warnings,
        // A declaration that could not be READ wins; otherwise a clause the
        // document is not allowed to state is itself unhonourable.
        error: error.or_else(|| document_reply_canon_error(&reply_canons_resolved)),
    }
}

/// Every system this deployment has declared, default first.
///
/// `DEJA_SYSTEMS` is authoritative when set — a comma-separated list, which is
/// the only way to state a name's exact spelling, since a hyphenated system
/// folds to an underscore in its variable names and cannot be recovered from
/// them. When unset the registry is DERIVED from the environment, so a
/// deployment that predates this and only set `DEJA_PRISM_S3_BUCKET` still
/// lists prism without being changed.
///
/// The default system is always present, whether or not anything declared it:
/// it needs no configuration to exist, and a picker that could omit it would be
/// offering a choice the orchestrator does not actually have.
/// Where a system's recordings live: its bucket and its key root.
///
/// THE resolution, for every reader — the listing endpoints, the correlation
/// endpoint, and the replay pull path alike. A recording is in one place, so
/// the question "where" must have one answer; the pull path asking the
/// environment while the listing asked the registry is how a system could be
/// listed, offered, and then not found.
///
/// No fallback to the deployment's own bucket. A system that has not declared
/// where its recordings are is refused BY NAME, because scanning somebody
/// else's bucket under this system's label does not fail — it answers wrongly.
pub fn recording_scope(system: &str) -> Result<(String, String), String> {
    let profile = system_config(system);
    let bucket = profile.s3_bucket.ok_or_else(|| {
        format!(
            "system '{system}' has no recording bucket declared: set systems.{system}.s3_bucket in the deja configuration"
        )
    })?;
    let root = profile
        .recording_root
        .filter(|r| !r.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_RECORDING_ROOT.to_owned());
    Ok((bucket, root))
}

/// The key root a system falls back to when it declares none.
pub const DEFAULT_RECORDING_ROOT: &str = "landing/v1";

#[must_use]
pub fn registry() -> Vec<SystemConfig> {
    let mut names: Vec<String> = vec![default_system().to_owned()];
    // The configuration's tables are the roster. One that could not be read
    // yields the default system alone, carrying the error, rather than a
    // half-parsed list: the caller asked for what was declared and gets one
    // entry that says why that is not available.
    match settings::load() {
        Ok(cfg) => {
            for name in cfg.systems.keys() {
                if !names.contains(name) {
                    names.push(name.clone());
                }
            }
            names.iter().map(|n| system_config(n)).collect()
        }
        Err(e) => {
            let mut only = system_config(&names[0]);
            only.error.get_or_insert(e);
            vec![only]
        }
    }
}

/// Which registered system minted a recording, judged by pod names.
///
/// Returns `None` when nothing matches, and that is the whole point. The
/// previous form was a two-system `if/else` whose `else` arm returned the
/// default system for any pod name it did not recognise — so a third system was
/// not unconfigured there, it was silently labelled hyperswitch. Its own comment
/// said "ambiguous stays null; a wrong label is worse than none", which the
/// arm above it did not do. A wrong label is what once sent a router tape to a
/// prism candidate and reset every request's connection.
#[must_use]
pub fn system_from_instances(instances: &[String]) -> Option<String> {
    match_instances(&registry(), instances)
}

/// The decision alone, over a registry the caller supplies.
///
/// Split from [`system_from_instances`] so the rule can be exercised without
/// touching process-global environment — which is not a testing convenience but
/// the reason the first version of these tests raced: several had to set and
/// unset `DEJA_SYSTEMS` to build a registry, and cargo runs tests on parallel
/// threads sharing one environment.
///
/// The default system never matches. It is the answer when nothing else fits,
/// and letting a pattern select it would reintroduce the arm this replaced.
#[must_use]
pub fn match_instances(registry: &[SystemConfig], instances: &[String]) -> Option<String> {
    registry
        .iter()
        .filter(|s| !s.is_default)
        .find(|s| {
            s.instance_pattern
                .as_deref()
                .is_some_and(|p| instances.iter().any(|i| i.contains(p)))
        })
        .map(|s| s.name.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_env::env_guard;
    use crate::DEFAULT_SYSTEM_UNDER_TEST;

    // Every test here reads the one process-global declaration, so every test
    // holds the lock and leaves the environment as it found it.
    fn toml(doc: &str) {
        std::env::set_var("DEJA_CONFIG_TOML", doc);
    }
    fn clear() {
        std::env::remove_var("DEJA_CONFIG_TOML");
    }
    /// A registry entry built in memory, for rules that need no declaration.
    fn config(name: &str, pattern: Option<&str>) -> SystemConfig {
        SystemConfig {
            instance_pattern: pattern.map(str::to_owned),
            ..system_config(name)
        }
    }

    /// The deployment's declaration, resolved by the code that reads it.
    ///
    /// A verbatim copy of what sandbox sets in DEJA_CONFIG_TOML (infra:
    /// deployment-configs/replay-orchestrator/sandbox-values-dep.yaml), so the
    /// producer and the consumer of this document are checked against each
    /// other rather than each being separately plausible. Copies drift, and a
    /// drifted copy here is a failing test rather than a silent misconfiguration.
    #[test]
    fn sbx_deployment_values_resolve_as_intended() {
        let _lock = env_guard();
        toml(INFRA_TOML);
        let reg = registry();
        clear();

        let names: Vec<&str> = reg.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["hyperswitch", "prism"],
            "the tables are the roster"
        );

        let h = reg
            .iter()
            .find(|s| s.is_default)
            .expect("a default is marked");
        assert_eq!(h.name, "hyperswitch");
        assert_eq!(h.error, None, "{:?}", h.error);
        assert_eq!(h.s3_bucket.as_deref(), Some("hyperswitch-art"));
        assert!(h.manages_stores && h.has_code_bundle);
        assert_eq!(
            h.manages_stores_declared,
            Some(true),
            "declared, not inherited"
        );
        assert_eq!(h.job_template_key.as_deref(), Some("job.json"));
        // The document and the comparator are checked against each other here:
        // this is the exact string a deployment writes, in the exact grammar the
        // recorder mints, so the move to a vendor declaration is a copy.
        assert_eq!(
            h.reply_canons.get("http_incoming").map(String::as_str),
            Some("bag:$.payment_methods_enabled[],$.payment_methods_enabled[].card_networks[]"),
            "the payment-methods list is declared unordered for the router's ingress"
        );
        assert_eq!(
            h.candidate_env.get("MODE_ENV").map(String::as_str),
            Some("ROUTER__DEJA__MODE")
        );
        assert_eq!(
            h.candidate_config_files.as_ref().map(Vec::len),
            Some(4),
            "the bundle's config files are declared on the system"
        );

        let p = reg
            .iter()
            .find(|s| s.name == "prism")
            .expect("prism is declared");
        assert_eq!(p.error, None, "{:?}", p.error);
        assert!(!p.is_default);
        assert_eq!(p.s3_bucket.as_deref(), Some("ucs-deja"));
        assert!(!p.manages_stores && !p.has_code_bundle);
        assert_eq!(p.instance_pattern.as_deref(), Some("ucs"));
        assert_eq!(p.scored_span_namespaces, vec!["ucs::", "connector::"]);
        assert!(
            p.reply_canons.is_empty(),
            "a canon declared for one system must not reach another"
        );
        assert_eq!(p.job_template_key.as_deref(), Some("job.prism.json"));
        assert_eq!(p.candidate_env.len(), 5, "one prefix, five names");
        assert_eq!(
            p.candidate_env.get("RUN_ID_ENV").map(String::as_str),
            Some("CS__DEJA__RUN_ID")
        );
        assert_eq!(p.candidate_config_files, None, "no bundle, no files");

        // The attribution that used to mislabel: a router pod is now unknown.
        assert_eq!(
            match_instances(&reg, &["inst=ucs-api-7d9".to_owned()]).as_deref(),
            Some("prism")
        );
        assert_eq!(
            match_instances(&reg, &["inst=hyperswitch-router-abc".to_owned()]),
            None
        );
    }

    /// Verbatim from infra. If this drifts from what is deployed, the test
    /// above is checking a document nobody ships.
    const INFRA_TOML: &str = r#"default_system = "hyperswitch"

# The payment router. Records into hyperswitch-art. The harness owns its
# postgres and redis, and it publishes a CodeBundle the replay Job stages
# before boot; the files that bundle carries besides migrations are the
# router's own deployment configs and the offline Superposition fallback
# (see the note under DEJA_CANDIDATE_CONFIG_FILES below for why
# docker_compose.toml is deliberately not among them).
[systems.hyperswitch]
s3_bucket = "hyperswitch-art"
candidate_image_repo = "223655089699.dkr.ecr.ap-south-1.amazonaws.com/hyperswitch-router"
candidate_env_prefix = "ROUTER__"
manages_stores = true
has_code_bundle = true
job_template_key = "job.json"
candidate_config_files = [
  "config/deployments/sandbox.toml",
  "config/deployments/production.toml",
  "config/development.toml",
  "config/superposition_seed.toml",
]

# UCS, the unified connector service. Records into its own bucket
# (vector-ucs-deja unit, 30-day retention), never mixed into
# hyperswitch-art; the recordings page's prism filter scans it, which needs
# the read whitelist on the replay-orchestrator and replay-job IRSA roles.
# No harness-managed stores and no CodeBundle: its job template
# (job.prism.json in the replay-env chart) has neither postgres nor
# migrations, and a bundle resolved for it would arm a patch against an
# init container that template deliberately does not have. Candidates come
# from a different image repo — bare refs qualify against it, and without
# it the orchestrator refuses bare prism refs by name, because a prism sha
# qualified against the router repo is a guaranteed dead pull (it happened,
# twice).
# The payment-methods list is emitted from an unordered source, so the same
# methods come back in a different order run to run. Declared in the recorder's
# own grammar: when the router's declaration at request_id.rs grows this clause,
# this line is deleted — and until it does, the scorecard reports the path as
# governed by "both", which is how a deployment knows it is safe to delete.
[systems.hyperswitch.reply_canons]
http_incoming = "bag:$.payment_methods_enabled[],$.payment_methods_enabled[].card_networks[]"

[systems.prism]
s3_bucket = "ucs-deja"
candidate_image_repo = "223655089699.dkr.ecr.ap-south-1.amazonaws.com/connector-service"
candidate_env_prefix = "CS__"
manages_stores = false
has_code_bundle = false
job_template_key = "job.prism.json"
instance_pattern = "ucs"
scored_span_namespaces = ["ucs::", "connector::"]"#;

    #[test]
    fn a_declared_system_resolves_every_field() {
        let _lock = env_guard();
        toml(
            r#"
[systems.regsys-full]
s3_bucket = "regsys-bucket"
recording_root = "landing/v9"
candidate_image_repo = "registry.example/repo/"
candidate_env_prefix = "X__"
candidate_run_id_env = "LEGACY_RUN"
manages_stores = true
has_code_bundle = true
candidate_config_files = ["a.toml", "b.toml"]
code_bundle_uri_env = "X_BUNDLE"
job_template_key = "job.custom.json"
config_source_deployment = "dep"
config_source_container = "ctr"
instance_pattern = "regsys"
scored_span_namespaces = ["a::", "b::"]
"#,
        );
        let c = system_config("regsys-full");
        clear();
        assert_eq!(c.error, None, "{:?}", c.error);
        assert!(!c.is_default);
        assert_eq!(c.s3_bucket.as_deref(), Some("regsys-bucket"));
        assert_eq!(c.recording_root.as_deref(), Some("landing/v9"));
        // Trailing slash stripped, so joining a reference cannot double it.
        assert_eq!(
            c.candidate_image_repo.as_deref(),
            Some("registry.example/repo")
        );
        assert_eq!(c.manages_stores_declared, Some(true));
        assert!(c.manages_stores && c.has_code_bundle);
        assert_eq!(
            c.candidate_config_files.as_deref(),
            Some(&["a.toml".to_owned(), "b.toml".to_owned()][..])
        );
        assert_eq!(c.code_bundle_uri_env.as_deref(), Some("X_BUNDLE"));
        assert_eq!(c.job_template_key.as_deref(), Some("job.custom.json"));
        assert_eq!(c.config_source_deployment.as_deref(), Some("dep"));
        assert_eq!(c.config_source_container.as_deref(), Some("ctr"));
        assert_eq!(c.instance_pattern.as_deref(), Some("regsys"));
        assert_eq!(c.scored_span_namespaces, vec!["a::", "b::"]);
        // The prefix names four slots; the explicit name wins the fifth.
        assert_eq!(
            c.candidate_env.get("MODE_ENV").map(String::as_str),
            Some("X__DEJA__MODE")
        );
        assert_eq!(
            c.candidate_env.get("RUN_ID_ENV").map(String::as_str),
            Some("LEGACY_RUN")
        );
    }

    /// The replay pull path resolves a recording the same way the listing does.
    /// It did not: the pull read the deployment's own bucket, so a run whose
    /// recording lives in another system's bucket failed "no landing objects"
    /// for a recording the listing had just offered — before admission, which
    /// is why admission's ingress gap was never the first obstacle.
    ///
    /// Deployment values, so this fails if the document changes shape.
    #[test]
    fn a_recording_resolves_to_its_own_systems_bucket() {
        let _lock = env_guard();
        toml(
            "default_system = \"hyperswitch\"\n\
             [systems.hyperswitch]\ns3_bucket = \"hyperswitch-art\"\n\
             [systems.prism]\ns3_bucket = \"ucs-deja\"\n",
        );
        let prism = recording_scope("prism");
        let hyperswitch = recording_scope("hyperswitch");
        let undeclared = recording_scope("zzz");
        clear();

        assert_eq!(
            prism.as_ref().map(|(b, _)| b.as_str()),
            Ok("ucs-deja"),
            "a prism run pulls from prism's bucket"
        );
        assert_eq!(
            hyperswitch.as_ref().map(|(b, _)| b.as_str()),
            Ok("hyperswitch-art")
        );
        assert_ne!(
            prism.as_ref().map(|(b, _)| b.as_str()),
            hyperswitch.as_ref().map(|(b, _)| b.as_str()),
            "and not from each other's"
        );
        assert_eq!(
            prism.map(|(_, r)| r),
            Ok(DEFAULT_RECORDING_ROOT.to_owned()),
            "the LAYOUT is shared; only the bucket is per system"
        );
        let refusal = undeclared.expect_err("an undeclared system is refused, never defaulted");
        assert!(
            refusal.contains("systems.zzz"),
            "and the refusal names what to declare: {refusal}"
        );
    }

    /// A document clause that cannot take effect is named where the deployment
    /// applies it, not discovered after a run absorbs nothing. Each case here
    /// would otherwise parse, appear on `/systems`, and be silently ignored.
    #[test]
    fn a_document_clause_that_cannot_be_honoured_is_an_error() {
        let _lock = env_guard();
        for (declaration, expected) in [
            // Does not parse at all.
            ("bagg:$.a[]", "can only contribute `bag:<paths>`"),
            ("bag :$.a[]", "can only contribute `bag:<paths>`"),
            // Parses, but is a whole-body preset only the recorder can state —
            // accepted here it would never be consulted.
            ("bag", "can only contribute `bag:<paths>`"),
            // A `bag:` that names nothing is the whole-body preset by another spelling.
            ("bag:", "must name at least one path"),
            ("bag: , ", "must name at least one path"),
            ("sequence", "can only contribute `bag:<paths>`"),
            ("project:!created_at", "can only contribute `bag:<paths>`"),
            // A valid clause beside an impossible one does not rescue it.
            ("bag:$.a[];sequence", "can only contribute `bag:<paths>`"),
            // Present but empty.
            ("", "declares no clauses"),
            ("   ;  ", "declares no clauses"),
        ] {
            toml(&format!(
                "default_system = \"hyperswitch\"\n\
                 [systems.regsys-canon]\n\
                 s3_bucket = \"b\"\n\
                 [systems.regsys-canon.reply_canons]\n\
                 http_incoming = \"{declaration}\"\n"
            ));
            let c = system_config("regsys-canon");
            clear();
            let error = c.error.unwrap_or_else(|| {
                panic!("`{declaration}` must be an error, not silently ignored")
            });
            assert!(
                error.contains(expected) && error.contains("http_incoming"),
                "`{declaration}` must name the boundary and the reason; got: {error}"
            );
        }
    }

    #[test]
    fn a_bag_clause_naming_paths_resolves_without_error() {
        let _lock = env_guard();
        toml(
            "default_system = \"hyperswitch\"\n\
             [systems.regsys-canon-ok]\n\
             s3_bucket = \"b\"\n\
             [systems.regsys-canon-ok.reply_canons]\n\
             http_incoming = \"bag:$.a[],$.b[].c[]\"\n",
        );
        let c = system_config("regsys-canon-ok");
        clear();
        assert_eq!(c.error, None, "{:?}", c.error);
        assert_eq!(
            c.reply_canons.get("http_incoming").map(String::as_str),
            Some("bag:$.a[],$.b[].c[]"),
            "and it is carried through to the comparator"
        );
    }

    /// Undeclared is empty, never borrowed from another system. The template
    /// key alone follows a convention, so a launch fails naming the key it
    /// wanted rather than silently using the default system's.
    #[test]
    fn an_undeclared_system_is_unconfigured_rather_than_defaulted() {
        let _lock = env_guard();
        clear();
        let c = system_config("regsys-absent");
        assert_eq!(c.error, None);
        assert!(!c.is_default);
        assert_eq!(c.s3_bucket, None);
        assert!(!c.manages_stores && !c.has_code_bundle);
        assert_eq!(c.instance_pattern, None);
        assert!(c.scored_span_namespaces.is_empty());
        assert!(c.candidate_env.is_empty(), "no built-in binding");
        assert_eq!(c.candidate_config_files, None);
        assert_eq!(
            c.job_template_key.as_deref(),
            Some("job.regsys-absent.json")
        );
    }

    /// The default system is declared exactly like any other. Being the
    /// default decides only what a caller gets by naming nothing, and what an
    /// undeclared capability means — it does not conjure a bucket.
    #[test]
    fn the_default_system_is_declared_like_any_other() {
        let _lock = env_guard();
        let name = DEFAULT_SYSTEM_UNDER_TEST;
        clear();
        let d = system_config(name);
        assert!(d.is_default);
        assert_eq!(
            d.s3_bucket, None,
            "undeclared is undeclared, even for the default"
        );
        assert!(
            d.manages_stores && d.has_code_bundle,
            "capabilities default to true"
        );
        assert_eq!(
            d.job_template_key, None,
            "None means the executor's base key"
        );

        toml(&format!(
            "[systems.{name}]\ns3_bucket = \"declared-bucket\"\nmanages_stores = false\n"
        ));
        let d = system_config(name);
        clear();
        assert_eq!(d.s3_bucket.as_deref(), Some("declared-bucket"));
        assert!(
            !d.manages_stores,
            "a declaration on the default is honoured, not ignored"
        );
    }

    /// A configuration that does not parse is a deployment error, reported on
    /// the one entry the registry still returns, naming the field rather than
    /// a line and column. Nothing half-parsed is offered.
    #[test]
    fn a_malformed_configuration_is_reported_not_swallowed() {
        let _lock = env_guard();
        toml("[systems.prism]\nmanages_stores = \"nope\"\n");
        let reg = registry();
        clear();
        assert_eq!(reg.len(), 1);
        let err = reg[0]
            .error
            .as_deref()
            .expect("the first entry carries the error");
        assert!(
            err.contains("systems.prism.manages_stores"),
            "names the field: {err}"
        );
    }

    /// The type is the parser, with the `config` crate's own coercion, which
    /// is the router's rule too: a quoted "true" is accepted as a boolean, and
    /// anything that is not a boolean under that rule is refused by field.
    #[test]
    fn a_capability_is_a_boolean_under_the_config_crates_rule() {
        let _lock = env_guard();
        toml("[systems.regsys-spell]\nmanages_stores = \"true\"\n");
        let c = system_config("regsys-spell");
        assert_eq!(c.error, None, "{:?}", c.error);
        assert_eq!(
            c.manages_stores_declared,
            Some(true),
            "coerced, as the router does"
        );
        toml("[systems.regsys-spell]\nmanages_stores = \"nope\"\n");
        let err = system_config("regsys-spell").error;
        clear();
        let err = err.expect("a non-boolean is refused");
        assert!(err.contains("regsys-spell.manages_stores"), "{err}");
    }

    /// The prefix is the fact; the five names follow from it. Declaring them
    /// one by one restated deja's own convention four extra times, in a form
    /// where four could drift while the fifth still looked right.
    #[test]
    fn a_prefix_names_all_five_slots_and_an_explicit_name_still_wins() {
        let _lock = env_guard();
        toml("[systems.regsys-prefix]\ns3_bucket = \"b\"\ncandidate_env_prefix = \"CS__\"\n");
        let c = system_config("regsys-prefix");
        assert_eq!(c.error, None);
        assert_eq!(c.candidate_env.len(), 5, "one declaration, five names");
        assert_eq!(
            c.candidate_env.get("MODE_ENV").map(String::as_str),
            Some("CS__DEJA__MODE")
        );
        assert_eq!(
            c.candidate_env.get("OBSERVED_ENV").map(String::as_str),
            Some("CS__DEJA__REPLAY__OBSERVED_SINK")
        );
        assert_eq!(
            c.candidate_env.get("CODE_SHA_ENV").map(String::as_str),
            Some("CS__DEJA__IDENTITY__CODE_SHA")
        );
        toml("[systems.regsys-prefix]\ns3_bucket = \"b\"\ncandidate_env_prefix = \"CS__\"\ncandidate_run_id_env = \"LEGACY_RUN\"\n");
        let o = system_config("regsys-prefix");
        clear();
        assert_eq!(
            o.candidate_env.get("RUN_ID_ENV").map(String::as_str),
            Some("LEGACY_RUN")
        );
        assert_eq!(
            o.candidate_env.get("MODE_ENV").map(String::as_str),
            Some("CS__DEJA__MODE"),
            "overriding one slot must not disturb the others"
        );
    }

    /// deja knows no system by name. A name it has seen before still resolves
    /// to nothing until the deployment declares it — the property that makes
    /// `GET /systems` honest, because every value it reports is one somebody
    /// chose rather than one deja assumed on their behalf.
    #[test]
    fn a_name_deja_has_seen_before_still_gets_nothing_for_free() {
        let _lock = env_guard();
        clear();
        let c = system_config("prism");
        assert_eq!(c.instance_pattern, None, "no built-in pod-name pattern");
        assert!(
            c.scored_span_namespaces.is_empty(),
            "no built-in span namespaces"
        );
        assert!(c.candidate_env.is_empty(), "no built-in candidate binding");
        assert_eq!(c.s3_bucket, None);
    }

    #[test]
    fn declared_systems_are_the_roster_and_the_default_leads() {
        let _lock = env_guard();
        toml("[systems.regsys-y]\ns3_bucket = \"y\"\n[systems.regsys-x]\ns3_bucket = \"x\"\n");
        let names: Vec<String> = registry().into_iter().map(|s| s.name).collect();
        clear();
        assert_eq!(
            names[0], DEFAULT_SYSTEM_UNDER_TEST,
            "default first, declared or not"
        );
        assert!(names.contains(&"regsys-x".to_owned()) && names.contains(&"regsys-y".to_owned()));
    }

    #[test]
    fn the_default_system_is_listed_even_when_declared_nowhere() {
        let _lock = env_guard();
        toml("[systems.regsys-only]\ns3_bucket = \"b\"\n");
        let names: Vec<String> = registry().into_iter().map(|s| s.name).collect();
        clear();
        assert_eq!(
            names.first().map(String::as_str),
            Some(DEFAULT_SYSTEM_UNDER_TEST),
            "a picker that could omit the default would offer a choice we do not have"
        );
    }

    /// The default system's NAME is read once for the life of the process, so
    /// the override can only be exercised in a process started with it set;
    /// what is checked here is that the fallback holds and the two ways of
    /// asking agree.
    #[test]
    fn the_default_system_name_falls_back_to_the_compiled_in_one() {
        let _lock = env_guard();
        assert_eq!(crate::default_system(), DEFAULT_SYSTEM_UNDER_TEST);
        assert!(is_default_system(crate::default_system()));
        assert!(!is_default_system("regsys-not-default"));
    }

    #[test]
    fn an_unrecognised_pod_name_is_unknown_and_not_the_default_system() {
        let _lock = env_guard();
        clear();
        let reg = vec![
            config(DEFAULT_SYSTEM_UNDER_TEST, None),
            config("regsys-pat", Some("regpod")),
        ];
        assert_eq!(
            match_instances(&reg, &["inst=regpod-abc".to_owned()]).as_deref(),
            Some("regsys-pat")
        );
        // The regression this exists for: the previous form's else-arm returned
        // the default system for any pod name it did not recognise, so a third
        // system was mislabelled rather than merely unconfigured — and a wrong
        // label once sent a router tape to a prism candidate.
        assert_eq!(
            match_instances(&reg, &["inst=something-else".to_owned()]),
            None,
            "an unmatched pod name must be unknown, never the default system"
        );
    }

    #[test]
    fn a_system_declaring_no_pattern_never_matches() {
        let _lock = env_guard();
        clear();
        let reg = vec![
            config(DEFAULT_SYSTEM_UNDER_TEST, None),
            config("regsys-nopat", None),
        ];
        assert_eq!(match_instances(&reg, &["inst=anything".to_owned()]), None);
    }

    /// The default is the answer when nothing else fits; letting a pattern
    /// select it would reintroduce the arm this replaced.
    #[test]
    fn the_default_system_is_never_matched_by_pattern() {
        let _lock = env_guard();
        clear();
        let reg = vec![config(DEFAULT_SYSTEM_UNDER_TEST, Some("router"))];
        assert_eq!(match_instances(&reg, &["inst=router-7".to_owned()]), None);
    }
}
