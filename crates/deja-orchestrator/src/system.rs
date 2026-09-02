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

use crate::{is_default_system, system_env_var, DEFAULT_SYSTEM_UNDER_TEST};

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

// The five candidate-binding slots are declared per system, like everything
// else about it. There is deliberately no table of built-in profiles here.
//
// One used to exist, carrying prism's real values: the substring of its pod
// names, the span prefixes its instrumentation declares, and the `CS__DEJA__*`
// variable names its candidate reads. Those are facts about another service,
// and a service's facts do not belong in the instrument that observes it. Deja
// cannot verify them, cannot notice when that service changes them, and a
// deployment reading `GET /systems` could not tell a value it had chosen from
// one deja had assumed on its behalf — the two rendered identically.
//
// So every value comes from the environment, and a system that declares
// nothing is unconfigured rather than quietly complete. A full declaration
// looks like the following, and lives in the deployment rather than here:
//
// ```text
// DEJA_<SYSTEM>_S3_BUCKET=its-recording-bucket
// DEJA_<SYSTEM>_MANAGES_STORES=false
// DEJA_<SYSTEM>_HAS_CODE_BUNDLE=false
// DEJA_<SYSTEM>_INSTANCE_PATTERN=a-substring-of-its-pod-names
// DEJA_<SYSTEM>_SCORED_SPAN_NAMESPACES=its::,span::prefixes::
// DEJA_<SYSTEM>_CANDIDATE_MODE_ENV=ITS__MODE
// DEJA_<SYSTEM>_CANDIDATE_RUN_ID_ENV=ITS__RUN_ID
// DEJA_<SYSTEM>_CANDIDATE_SOURCE_ENV=ITS__REPLAY__SOURCE
// DEJA_<SYSTEM>_CANDIDATE_OBSERVED_ENV=ITS__REPLAY__OBSERVED_SINK
// DEJA_<SYSTEM>_CANDIDATE_CODE_SHA_ENV=ITS__IDENTITY__CODE_SHA
// ```

/// Everything the orchestrator can say about one system, resolved together.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SystemConfig {
    /// The name a caller sends as `system_under_test` / `?system=`.
    pub name: String,
    /// Whether this is the system a caller gets by naming nothing.
    pub is_default: bool,
    /// `DEJA_<SYSTEM>_S3_BUCKET`. `None` for the default system, which reads the
    /// deployment's own `DEJA_S3_BUCKET` — deliberately, so the default can
    /// never be made to depend on a per-system variable existing. `None` for
    /// any other system means UNDECLARED, and the caller refuses by name.
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

/// What a deployment DECLARED for one system, before any default is applied.
///
/// Deserialized rather than read variable by variable, so each field's TYPE is
/// its parser: a `bool` field accepts `true` and `false` and nothing else, a
/// `Vec<String>` splits on commas, and a value that does not parse names both
/// the variable and the value it found. Adding a field is a member here and
/// nothing else; the shape this replaced needed a member, a line in the
/// resolver, and a hand-written parse — three places to forget one.
///
/// Strict on purpose. The reader this replaced also accepted `1`, `yes` and
/// `on`, which is the kind of latitude that survives into a typed design out of
/// habit and then has to be carried forever. Nothing in any deployment uses
/// those spellings, and a system that would rather guess than refuse is the
/// thing being removed here, so the accepted set is the type's own.
///
/// The router parses its own configuration this way too, through
/// `serde_path_to_error`, for the same reason. It can use `config`'s
/// environment source because it separates levels with `__`, leaving `_` free
/// inside a leaf name (`ROUTER__SECRETS__KMS_ENCRYPTED_JWT_SECRET`). Deja's
/// names are flat and single-underscore, so that source would read
/// `DEJA_PRISM_S3_BUCKET` as `prism.s3.bucket`; a prefix deserializer has no
/// separator to be ambiguous about and fits exactly.
#[derive(Debug, Default, Deserialize)]
struct Declared {
    s3_bucket: Option<String>,
    recording_root: Option<String>,
    manages_stores: Option<bool>,
    has_code_bundle: Option<bool>,
    job_template_key: Option<String>,
    candidate_image_repo: Option<String>,
    instance_pattern: Option<String>,
    scored_span_namespaces: Option<Vec<String>>,
    config_source_deployment: Option<String>,
    config_source_container: Option<String>,
    candidate_mode_env: Option<String>,
    candidate_run_id_env: Option<String>,
    candidate_source_env: Option<String>,
    candidate_observed_env: Option<String>,
    candidate_code_sha_env: Option<String>,
}

impl Declared {
    /// The declared candidate-binding slot names, keyed by [`CANDIDATE_ENV_SLOTS`].
    fn candidate_slots(&self) -> [(&'static str, Option<&String>); 5] {
        [
            ("MODE_ENV", self.candidate_mode_env.as_ref()),
            ("RUN_ID_ENV", self.candidate_run_id_env.as_ref()),
            ("SOURCE_ENV", self.candidate_source_env.as_ref()),
            ("OBSERVED_ENV", self.candidate_observed_env.as_ref()),
            ("CODE_SHA_ENV", self.candidate_code_sha_env.as_ref()),
        ]
    }
}

/// Read one system's declaration. `Err` names the variable and the value.
///
/// All-or-nothing PER SYSTEM, which is the blast radius that matches what a bad
/// value means. Quietly defaulting a misparsed capability gives a system that
/// runs with the WRONG one — a `MANAGES_STORES` typo produces a harness that
/// skips migrations, the schema gate, the flush and the seeding, and says
/// nothing. Refusing that system cannot run wrong. Refusing the whole PROCESS
/// would be the other extreme: one system's typo must not stop another
/// system's replays, and the prefix scopes the deserialize so it does not.
fn declared(system: &str) -> Result<Declared, String> {
    let prefix = system_env_var(system, "");
    envy::prefixed(&prefix)
        .from_env::<Declared>()
        .map_err(|e| format!("{e} (system '{system}', variables {prefix}*)"))
}

/// Resolve one system. Works for ANY name, registered or not: this is the same
/// resolution the individual lookups did, so a caller naming an unregistered
/// system gets exactly what it got before — which for an undeclared bucket is a
/// refusal naming the variable to set.
#[must_use]
pub fn system_config(name: &str) -> SystemConfig {
    let is_default = is_default_system(name);
    let mut warnings = Vec::new();

    // A declaration that does not parse makes this system unusable rather than
    // quietly default. Every field then takes the value it would have had with
    // nothing declared, so the struct stays complete and reportable — `error`
    // is what says not to run it.
    let (d, error) = match declared(name) {
        Ok(d) => (d, None),
        Err(e) => (Declared::default(), Some(e)),
    };

    let mut candidate_env = BTreeMap::new();
    for (slot, declared_name) in d.candidate_slots() {
        if let Some(value) = declared_name.cloned() {
            candidate_env.insert(slot.to_owned(), value);
        }
    }

    let clean = |v: Option<String>| v.map(|v| v.trim().to_owned()).filter(|v| !v.is_empty());

    SystemConfig {
        name: name.to_owned(),
        is_default,
        s3_bucket: if is_default {
            if let Some(ignored) = clean(d.s3_bucket) {
                warnings.push(format!(
                    "{} = '{ignored}' is set but the default system reads DEJA_S3_BUCKET and ignores it; \
                     setting it does not change which bucket is scanned",
                    system_env_var(name, "S3_BUCKET")
                ));
            }
            None
        } else {
            clean(d.s3_bucket)
        },
        recording_root: clean(d.recording_root),
        manages_stores: d.manages_stores.unwrap_or(is_default),
        manages_stores_declared: d.manages_stores,
        has_code_bundle: d.has_code_bundle.unwrap_or(is_default),
        job_template_key: if is_default {
            None
        } else {
            Some(clean(d.job_template_key).unwrap_or_else(|| format!("job.{name}.json")))
        },
        candidate_image_repo: if is_default {
            clean(std::env::var("DEJA_CANDIDATE_IMAGE_REPO").ok())
        } else {
            clean(d.candidate_image_repo)
        }
        .map(|v| v.trim_end_matches('/').to_owned()),
        instance_pattern: clean(d.instance_pattern),
        scored_span_namespaces: d
            .scored_span_namespaces
            .map(|v| {
                v.into_iter()
                    .map(|s| s.trim().to_owned())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default(),
        config_source_deployment: if is_default {
            None
        } else {
            clean(d.config_source_deployment)
        },
        config_source_container: if is_default {
            None
        } else {
            clean(d.config_source_container)
        },
        candidate_env,
        warnings,
        error,
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
#[must_use]
pub fn registry() -> Vec<SystemConfig> {
    let mut names: Vec<String> = vec![DEFAULT_SYSTEM_UNDER_TEST.to_owned()];

    match std::env::var("DEJA_SYSTEMS") {
        Ok(raw) if !raw.trim().is_empty() => {
            for name in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                if !names.iter().any(|n| n == name) {
                    names.push(name.to_owned());
                }
            }
        }
        _ => {
            // Derived: a system exists here if it has a bucket, because that is
            // the one variable it cannot work without.
            let mut derived: Vec<String> = std::env::vars()
                .filter_map(|(k, _)| {
                    let body = k.strip_prefix("DEJA_")?.strip_suffix("_S3_BUCKET")?;
                    // `DEJA_S3_BUCKET` leaves an empty body: that is the
                    // deployment's own bucket, not a system named "".
                    (!body.is_empty()).then(|| body.to_lowercase())
                })
                .filter(|n| !names.contains(n))
                .collect();
            derived.sort();
            derived.dedup();
            names.extend(derived);
        }
    }

    names.iter().map(|n| system_config(n)).collect()
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

    // Tests here mutate process-global environment, so each uses a system name
    // of its own. `registry()` additionally SCANS the environment when
    // `DEJA_SYSTEMS` is unset, which no per-name discipline can isolate — so
    // every test that calls it either takes `env_guard` or asserts containment
    // rather than an exact list.

    /// Set one per-system variable, spelled the way the resolver reads it.
    fn set(system: &str, suffix: &str, value: &str) {
        std::env::set_var(system_env_var(system, suffix), value);
    }

    /// A registry built in memory, for rules that do not need the environment.
    fn config(name: &str, pattern: Option<&str>) -> SystemConfig {
        SystemConfig {
            instance_pattern: pattern.map(str::to_owned),
            ..system_config(name)
        }
    }

    #[test]
    fn the_default_system_needs_no_configuration_to_be_complete() {
        let _lock = env_guard();
        // Set the variable the default system must NOT consult. Asserting None
        // with it unset proves only that nothing was there to read; the
        // invariant is that the default ignores it even when it is present.
        //
        // This is not hypothetical. An orchestrator without the short-circuit
        // refuses `?system=hyperswitch` with an error naming this exact
        // variable, so an operator following that advice sets it — and the
        // default system must keep reading the deployment's own bucket rather
        // than silently repointing at whatever was set to quiet the error.
        std::env::set_var(
            system_env_var(DEFAULT_SYSTEM_UNDER_TEST, "S3_BUCKET"),
            "wrong-bucket",
        );
        let d = system_config(DEFAULT_SYSTEM_UNDER_TEST);
        std::env::remove_var(system_env_var(DEFAULT_SYSTEM_UNDER_TEST, "S3_BUCKET"));

        assert!(d.is_default);
        // None means "reads the deployment's own DEJA_S3_BUCKET", never
        // "unconfigured": the default must not depend on a per-system variable.
        assert_eq!(
            d.s3_bucket, None,
            "the default system must ignore its per-system bucket"
        );
        assert_eq!(d.job_template_key, None);
        assert!(d.manages_stores);
        assert!(d.has_code_bundle);
        assert_eq!(d.manages_stores_declared, None);
    }

    #[test]
    fn a_declared_system_resolves_every_field_from_its_own_variables() {
        let s = "regsys-full";
        set(s, "S3_BUCKET", "regsys-bucket");
        set(s, "RECORDING_ROOT", "landing/v9");
        set(s, "MANAGES_STORES", "true");
        set(s, "HAS_CODE_BUNDLE", "true");
        set(s, "JOB_TEMPLATE_KEY", "job.custom.json");
        set(s, "CANDIDATE_IMAGE_REPO", "registry.example/repo/");
        set(s, "INSTANCE_PATTERN", "regsys");
        set(s, "SCORED_SPAN_NAMESPACES", "a::, b:: ,,c::");
        set(s, "CONFIG_SOURCE_DEPLOYMENT", "dep");
        set(s, "CONFIG_SOURCE_CONTAINER", "ctr");
        set(s, "CANDIDATE_MODE_ENV", "X__MODE");

        let c = system_config(s);
        assert!(!c.is_default);
        assert_eq!(c.s3_bucket.as_deref(), Some("regsys-bucket"));
        assert_eq!(c.recording_root.as_deref(), Some("landing/v9"));
        assert!(c.manages_stores);
        assert_eq!(c.manages_stores_declared, Some(true));
        assert!(c.has_code_bundle);
        assert_eq!(c.job_template_key.as_deref(), Some("job.custom.json"));
        // Trailing slash stripped, so joining a reference cannot double it.
        assert_eq!(
            c.candidate_image_repo.as_deref(),
            Some("registry.example/repo")
        );
        assert_eq!(c.instance_pattern.as_deref(), Some("regsys"));
        assert_eq!(c.scored_span_namespaces, vec!["a::", "b::", "c::"]);
        assert_eq!(c.config_source_deployment.as_deref(), Some("dep"));
        assert_eq!(
            c.candidate_env.get("MODE_ENV").map(String::as_str),
            Some("X__MODE")
        );
        // An undeclared slot is absent rather than empty, so the caller can tell
        // "not configured" from "configured to the empty string".
        assert!(!c.candidate_env.contains_key("RUN_ID_ENV"));
    }

    #[test]
    fn an_undeclared_system_is_unconfigured_rather_than_defaulted() {
        let c = system_config("regsys-absent");
        assert!(!c.is_default);
        assert_eq!(c.s3_bucket, None);
        // Every capability degrades to the safe answer: not the default system,
        // so no stores and no code bundle.
        assert!(!c.manages_stores);
        assert!(!c.has_code_bundle);
        assert_eq!(c.instance_pattern, None);
        assert!(c.scored_span_namespaces.is_empty());
        // The template key is still resolved by convention, so a launch fails
        // naming the key it wanted rather than borrowing another system's.
        assert_eq!(
            c.job_template_key.as_deref(),
            Some("job.regsys-absent.json")
        );
    }

    /// deja knows no system by name. A name it has seen before still resolves to
    /// nothing until the deployment declares it — which is the property that
    /// makes `GET /systems` honest, because every value it reports is one
    /// somebody chose rather than one deja assumed on their behalf.
    #[test]
    fn a_name_deja_has_seen_before_still_gets_nothing_for_free() {
        let c = system_config("prism");
        assert_eq!(c.instance_pattern, None, "no built-in pod-name pattern");
        assert!(
            c.scored_span_namespaces.is_empty(),
            "no built-in span namespaces"
        );
        assert!(c.candidate_env.is_empty(), "no built-in candidate binding");
        assert_eq!(c.s3_bucket, None);
        // Declared, it resolves — the only way anything resolves.
        set("prism", "INSTANCE_PATTERN", "ucs");
        set("prism", "CANDIDATE_MODE_ENV", "CS__DEJA__MODE");
        let d = system_config("prism");
        std::env::remove_var(system_env_var("prism", "INSTANCE_PATTERN"));
        std::env::remove_var(system_env_var("prism", "CANDIDATE_MODE_ENV"));
        assert_eq!(d.instance_pattern.as_deref(), Some("ucs"));
        assert_eq!(
            d.candidate_env.get("MODE_ENV").map(String::as_str),
            Some("CS__DEJA__MODE")
        );
    }

    /// The accepted set is the type's own. The reader this replaced also took
    /// `1`, `yes` and `on`; nothing in any deployment used them, and carrying
    /// that latitude into a typed design is how it becomes permanent.
    #[test]
    fn a_capability_accepts_the_types_spellings_and_no_others() {
        let s = "regsys-spell";
        set(s, "S3_BUCKET", "b");
        for (raw, want) in [("true", true), ("false", false)] {
            set(s, "MANAGES_STORES", raw);
            let c = system_config(s);
            assert_eq!(c.error, None, "'{raw}' must parse");
            assert_eq!(c.manages_stores_declared, Some(want));
        }
        for raw in ["1", "yes", "on", "perhaps", ""] {
            set(s, "MANAGES_STORES", raw);
            assert!(
                system_config(s).error.is_some(),
                "'{raw}' must be refused rather than guessed at"
            );
        }
    }

    #[test]
    fn declared_systems_are_authoritative_and_the_default_leads() {
        let _lock = env_guard();
        std::env::set_var("DEJA_SYSTEMS", "regsys-x, regsys-y ,,regsys-x");
        let names: Vec<String> = registry().into_iter().map(|s| s.name).collect();
        std::env::remove_var("DEJA_SYSTEMS");
        assert_eq!(
            names,
            vec![DEFAULT_SYSTEM_UNDER_TEST, "regsys-x", "regsys-y"],
            "default first, blanks dropped, duplicates collapsed"
        );
    }

    #[test]
    fn the_default_system_is_listed_even_when_declared_nowhere() {
        let _lock = env_guard();
        std::env::set_var("DEJA_SYSTEMS", "regsys-only");
        let names: Vec<String> = registry().into_iter().map(|s| s.name).collect();
        std::env::remove_var("DEJA_SYSTEMS");
        assert!(
            names.contains(&DEFAULT_SYSTEM_UNDER_TEST.to_owned()),
            "a picker that could omit the default would offer a choice we do not have"
        );
    }

    #[test]
    fn an_undeclared_registry_is_derived_from_the_buckets_present() {
        let _lock = env_guard();
        std::env::remove_var("DEJA_SYSTEMS");
        std::env::set_var("DEJA_REGSYS_DERIVED_S3_BUCKET", "b");
        // The deployment's own bucket must not read as a system named "".
        std::env::set_var("DEJA_S3_BUCKET", "hyperswitch-art");
        let names: Vec<String> = registry().into_iter().map(|s| s.name).collect();
        std::env::remove_var("DEJA_REGSYS_DERIVED_S3_BUCKET");
        assert!(names.contains(&"regsys_derived".to_owned()));
        assert!(!names.iter().any(String::is_empty));
    }

    #[test]
    fn an_unrecognised_pod_name_is_unknown_and_not_the_default_system() {
        let reg = vec![
            config(DEFAULT_SYSTEM_UNDER_TEST, None),
            config("regsys-pat", Some("regpod")),
        ];
        let matched = match_instances(&reg, &["inst=regpod-abc".to_owned()]);
        let unmatched = match_instances(&reg, &["inst=something-else".to_owned()]);

        assert_eq!(matched.as_deref(), Some("regsys-pat"));
        // The regression this exists for: the previous form's else-arm returned
        // the default system for any pod name it did not recognise, so a third
        // system was mislabelled rather than merely unconfigured — and a wrong
        // label once sent a router tape to a prism candidate.
        assert_eq!(
            unmatched, None,
            "an unmatched pod name must be unknown, never the default system"
        );
    }

    #[test]
    fn a_system_declaring_no_pattern_never_matches() {
        let reg = vec![
            config(DEFAULT_SYSTEM_UNDER_TEST, None),
            config("regsys-nopat", None),
        ];
        let got = match_instances(&reg, &["inst=anything".to_owned()]);
        assert_eq!(
            got, None,
            "no declared pattern means no evidence, not a match"
        );
    }

    #[test]
    fn the_default_system_is_never_matched_by_pattern() {
        let reg = vec![config(DEFAULT_SYSTEM_UNDER_TEST, Some("router"))];
        assert_eq!(match_instances(&reg, &["inst=router-7".to_owned()]), None);
    }

    #[test]
    fn a_declaration_that_does_not_parse_makes_that_system_unusable() {
        let s = "regsys-bad";
        set(s, "S3_BUCKET", "b");
        set(s, "MANAGES_STORES", "perhaps");
        let c = system_config(s);

        // Not a warning, and not a silent inherit. Defaulting here would give a
        // harness that skips migrations, the schema gate, the flush and the
        // seeding because of a typo, and say nothing about it.
        let err = c.error.expect("an unparseable declaration is an error");
        assert!(
            err.contains("MANAGES_STORES") && err.contains("perhaps"),
            "the error must name the variable and the value: {err}"
        );
    }

    #[test]
    fn one_systems_bad_declaration_does_not_disturb_another() {
        let a = "regsys-iso-bad";
        let b = "regsys-iso-good";
        set(a, "MANAGES_STORES", "perhaps");
        set(b, "S3_BUCKET", "fine");
        set(b, "MANAGES_STORES", "true");

        assert!(system_config(a).error.is_some());
        let good = system_config(b);
        assert!(
            good.error.is_none(),
            "the blast radius of a typo is one system, not the process: {:?}",
            good.error
        );
        assert!(good.manages_stores);
        assert_eq!(good.s3_bucket.as_deref(), Some("fine"));
    }

    #[test]
    fn a_per_system_bucket_on_the_default_system_is_reported_as_ignored() {
        let _lock = env_guard();
        std::env::set_var(
            system_env_var(DEFAULT_SYSTEM_UNDER_TEST, "S3_BUCKET"),
            "wrong-bucket",
        );
        let d = system_config(DEFAULT_SYSTEM_UNDER_TEST);
        std::env::remove_var(system_env_var(DEFAULT_SYSTEM_UNDER_TEST, "S3_BUCKET"));

        assert_eq!(d.s3_bucket, None, "still ignored");
        // This is the trap an unreconciled orchestrator walks an operator into:
        // its 400 names this variable, setting it quiets the error without
        // deploying anything, and nothing afterwards says the value is unused.
        assert!(
            d.warnings.iter().any(|w| w.contains("wrong-bucket")),
            "the variable the pre-reconciliation error tells you to set must not \
             be silently unused: {:?}",
            d.warnings
        );
    }

    #[test]
    fn a_correctly_configured_system_warns_about_nothing() {
        let s = "regsys-clean";
        set(s, "S3_BUCKET", "b");
        set(s, "MANAGES_STORES", "false");
        assert!(system_config(s).warnings.is_empty());
    }

    #[test]
    /// The deployment's declared values, resolved by the code that reads them.
    ///
    /// A copy of what sandbox actually sets (infra:
    /// deployment-configs/replay-orchestrator/sandbox-values-dep.yaml), so the
    /// producer and the consumer of these variables are checked against each
    /// other rather than each being separately plausible. Copies drift, and a
    /// drifted copy here is a failing test rather than a silent misconfiguration
    /// — which is the direction that costs least.
    fn sbx_deployment_values_resolve_as_intended() {
        let _lock = env_guard();
        // Verbatim from deployment-configs/replay-orchestrator/sandbox-values-dep.yaml
        std::env::set_var("DEJA_S3_BUCKET", "hyperswitch-art");
        std::env::set_var("DEJA_PRISM_S3_BUCKET", "ucs-deja");
        std::env::set_var(
            "DEJA_PRISM_CANDIDATE_IMAGE_REPO",
            "223655089699.dkr.ecr.ap-south-1.amazonaws.com/connector-service",
        );
        std::env::set_var("DEJA_PRISM_MANAGES_STORES", "false");
        std::env::set_var("DEJA_PRISM_HAS_CODE_BUNDLE", "false");
        std::env::set_var("DEJA_PRISM_INSTANCE_PATTERN", "ucs");
        std::env::set_var("DEJA_PRISM_SCORED_SPAN_NAMESPACES", "ucs::,connector::");
        std::env::set_var("DEJA_PRISM_CANDIDATE_MODE_ENV", "CS__DEJA__MODE");
        std::env::set_var("DEJA_PRISM_CANDIDATE_RUN_ID_ENV", "CS__DEJA__RUN_ID");
        std::env::set_var(
            "DEJA_PRISM_CANDIDATE_SOURCE_ENV",
            "CS__DEJA__REPLAY__SOURCE",
        );
        std::env::set_var(
            "DEJA_PRISM_CANDIDATE_OBSERVED_ENV",
            "CS__DEJA__REPLAY__OBSERVED_SINK",
        );
        std::env::set_var(
            "DEJA_PRISM_CANDIDATE_CODE_SHA_ENV",
            "CS__DEJA__IDENTITY__CODE_SHA",
        );
        std::env::remove_var("DEJA_SYSTEMS");

        let reg = registry();
        let names: Vec<&str> = reg.iter().map(|s| s.name.as_str()).collect();
        assert!(
            names.contains(&"prism"),
            "prism must be discovered from its bucket alone, with no list to maintain: {names:?}"
        );

        let p = reg
            .iter()
            .find(|s| s.name == "prism")
            .expect("prism is in the registry");
        assert_eq!(p.s3_bucket.as_deref(), Some("ucs-deja"));
        assert!(!p.manages_stores);
        assert!(!p.has_code_bundle);
        assert_eq!(p.instance_pattern.as_deref(), Some("ucs"));
        assert_eq!(p.scored_span_namespaces, vec!["ucs::", "connector::"]);
        // The candidate binding is declared too, now that deja ships none. If
        // these are ever dropped from the deployment a prism candidate boots
        // reading the DEFAULT system's variable names and never sees its run.
        assert_eq!(p.candidate_env.len(), 5, "all five slots declared");
        assert_eq!(
            p.candidate_env.get("MODE_ENV").map(String::as_str),
            Some("CS__DEJA__MODE")
        );
        assert_eq!(p.job_template_key.as_deref(), Some("job.prism.json"));
        assert!(
            p.warnings.is_empty(),
            "no ignored declarations: {:?}",
            p.warnings
        );

        let h = reg
            .iter()
            .find(|s| s.is_default)
            .expect("the default system is always listed");
        assert_eq!(h.s3_bucket, None, "default reads DEJA_S3_BUCKET");
        assert!(h.manages_stores && h.has_code_bundle);
        assert!(h.warnings.is_empty());

        // Leave the environment as found: another module's tests assert that
        // prism declares nothing, and a leaked variable here fails them from a
        // distance, in whichever order cargo happens to schedule them.
        for v in [
            "DEJA_PRISM_S3_BUCKET",
            "DEJA_PRISM_CANDIDATE_IMAGE_REPO",
            "DEJA_PRISM_MANAGES_STORES",
            "DEJA_PRISM_HAS_CODE_BUNDLE",
            "DEJA_PRISM_INSTANCE_PATTERN",
            "DEJA_PRISM_SCORED_SPAN_NAMESPACES",
            "DEJA_PRISM_CANDIDATE_MODE_ENV",
            "DEJA_PRISM_CANDIDATE_RUN_ID_ENV",
            "DEJA_PRISM_CANDIDATE_SOURCE_ENV",
            "DEJA_PRISM_CANDIDATE_OBSERVED_ENV",
            "DEJA_PRISM_CANDIDATE_CODE_SHA_ENV",
        ] {
            std::env::remove_var(v);
        }

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
}
