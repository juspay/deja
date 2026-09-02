//! The orchestrator's declared configuration, loaded the way the router and
//! UCS load theirs: a structured file, overridden by environment variables that
//! follow one convention.
//!
//! The file is TOML. The environment convention is the `config` crate's, the
//! same one `ROUTER__*` and `CS__*` use: the prefix `DEJA`, `__` between
//! nesting levels, `_` inside a name. So `[systems.prism] s3_bucket = "x"` in
//! the file and `DEJA__SYSTEMS__PRISM__S3_BUCKET=x` in the environment name the
//! same value, and anyone who can read one can write the other.
//!
//! The single-underscore variables the orchestrator has always read
//! (`DEJA_S3_BUCKET`, `DEJA_JOBS_NAMESPACE`, ...) are NOT this. The prefix here
//! is `DEJA__`, two underscores, so nothing that exists today is re-read under
//! a different meaning; those variables keep their readers and this adds a
//! structured layer alongside them.
//!
//! Sources, lowest precedence first:
//!
//! 1. A file, at `DEJA_CONFIG_FILE` or `/etc/deja/deja.toml`, if present.
//! 2. `DEJA_CONFIG_TOML`, the same document inline, for a deployment whose
//!    chart carries environment but not files.
//! 3. `DEJA__*` environment variables.
//!
//! A value that does not parse names its field — `systems.prism.manages_stores`
//! — not a line and column.

use std::collections::BTreeMap;

use config::{Config, Environment, File, FileFormat};
use serde::{Deserialize, Deserializer};

/// The declared configuration: which system is default, and one declaration
/// per system.
#[derive(Debug, Default, Clone, Deserialize)]
pub struct Settings {
    /// Which system a run gets by naming nothing.
    #[serde(default)]
    pub default_system: Option<String>,
    /// Every system, keyed by the name a caller sends. The keys are the roster.
    #[serde(default)]
    pub systems: BTreeMap<String, SystemDeclaration>,
}

/// What a deployment declares for one system. Every field is optional, and the
/// resolver decides what an absent one means for the default system (the
/// executor's base value) versus any other (unconfigured, or a convention).
///
/// One struct for every system. Adding a fact is a member here and nothing
/// else, and a third system is one more `[systems.<name>]` table.
#[derive(Debug, Default, Clone, Deserialize)]
pub struct SystemDeclaration {
    // ---- where its recordings are ----
    /// The bucket its recordings land in.
    pub s3_bucket: Option<String>,
    /// Key prefix under the bucket; the deployment-wide root when unset.
    pub recording_root: Option<String>,

    // ---- what a candidate is ----
    /// Registry a bare candidate reference is qualified against.
    pub candidate_image_repo: Option<String>,
    /// The system's own configuration prefix. Its candidate reads
    /// `<prefix>DEJA__MODE`, `<prefix>DEJA__RUN_ID` and so on; `ROUTER__` for
    /// the router, `CS__` for UCS. One fact from which all five names follow.
    pub candidate_env_prefix: Option<String>,
    /// Per-slot overrides, for a system whose keys do not follow the convention.
    pub candidate_mode_env: Option<String>,
    pub candidate_run_id_env: Option<String>,
    pub candidate_source_env: Option<String>,
    pub candidate_observed_env: Option<String>,
    pub candidate_code_sha_env: Option<String>,

    // ---- what the harness does for it ----
    /// The harness owns its postgres and redis: migrates, gates the schema
    /// fingerprint, flushes, seeds.
    pub manages_stores: Option<bool>,
    /// It publishes a CodeBundle — its `migrations/` and config at the
    /// candidate's ref — which the replay Job stages before boot.
    pub has_code_bundle: Option<bool>,
    /// Repo-relative config files the CodeBundle carries besides migrations.
    #[serde(default, deserialize_with = "list_or_csv")]
    pub candidate_config_files: Option<Vec<String>>,
    /// The variable the bundle URI is handed to the init containers in.
    pub code_bundle_uri_env: Option<String>,

    // ---- how it is launched ----
    /// Key in the job-template ConfigMap; `job.<name>.json` by convention.
    pub job_template_key: Option<String>,
    /// Where rendered config is copied from. Absent on a non-default system
    /// means NO copy, never the default's environment.
    pub config_source_deployment: Option<String>,
    pub config_source_container: Option<String>,

    // ---- how its recordings are recognised and scored ----
    /// A substring of its pod names, for attributing a recording whose bucket
    /// does not say which system minted it.
    pub instance_pattern: Option<String>,
    /// Span prefixes its instrumentation declares as scored.
    #[serde(default, deserialize_with = "list_or_csv")]
    pub scored_span_namespaces: Option<Vec<String>>,
    /// Reply canons this system declares per BOUNDARY, in the same grammar the
    /// recorder mints — so moving one to the recorder later is a copy of the
    /// string and a deletion of the line, not a translation.
    ///
    /// A declaration is clauses separated by `;`:
    /// `project:!created_at,!last_synced;bag:$.a[],$.b[]`. A `bag` clause naming
    /// paths says those collections carry no order; bare `bag` is the whole
    /// body, which is what every declaration written before clauses existed
    /// means. Only a permutation is absorbed — the comparator proves the two
    /// sides hold the identical multiset first — so an added, removed or
    /// altered element still differs and still blocks.
    ///
    /// These COMPOSE with the recorder's declaration for the same boundary
    /// rather than deferring to it. A path both sources name identically is
    /// reported redundant (the document line can go); a path they describe
    /// differently is reported as a conflict and absorbed by neither.
    ///
    /// Keyed by boundary, and for an HTTP reply there is exactly one ingress
    /// boundary for every route (`http_incoming`, minted by one middleware), so
    /// a `bag` clause here applies to EVERY route this deployment serves. There
    /// is no route dimension in either source today; if one is ever needed it
    /// has to arrive in both at once.
    ///
    /// hyperswitch declares:
    /// `http_incoming = "bag:$.payment_methods_enabled[],$.payment_methods_enabled[].card_networks[]"`
    #[serde(default)]
    pub reply_canons: Option<std::collections::BTreeMap<String, String>>,

    /// Response paths whose array order IS asserted, in the same `$.a.b[]` form
    /// the classifier emits. Empty by default, which is the point.
    ///
    /// Order is treated as carrying NO meaning everywhere unless a path is
    /// named here. That is the deliberate default: the collections a service
    /// serialises into JSON arrays are mostly sets and maps whose iteration
    /// order is seeded per process, so an order-only difference is almost
    /// always noise, and adjudicating it site by site costs more than it
    /// returns. A path listed here goes back to blocking — for a routing
    /// priority list, a retry ladder, a paginated page, anything whose order a
    /// client is entitled to rely on.
    ///
    /// The cost of the default, stated plainly: the comparator can no longer
    /// tell an INTENDED order change from a hash-seed permutation unless the
    /// path is named here. A removed sort at an undeclared path will not block.
    #[serde(default, deserialize_with = "list_or_csv")]
    pub ordered_response_paths: Option<Vec<String>>,
}

/// A list, from either a TOML array or a comma-separated string.
///
/// In the file a list is a list. In the environment there is only a string, and
/// `config`'s own comma splitting has to be told the exact key of every list
/// field in advance — which for a map keyed by system name would mean
/// enumerating systems before reading which systems exist. Accepting both
/// shapes here removes that.
fn list_or_csv<'de, D>(d: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Either {
        List(Vec<String>),
        Csv(String),
    }
    Ok(match Option::<Either>::deserialize(d)? {
        None => None,
        Some(Either::List(v)) => Some(v),
        Some(Either::Csv(s)) => Some(
            s.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .collect(),
        ),
    })
}

/// The file consulted when `DEJA_CONFIG_FILE` is unset.
pub const DEFAULT_CONFIG_FILE: &str = "/etc/deja/deja.toml";

/// Load the declared configuration from every source, in precedence order.
///
/// `Ok(Settings::default())` when nothing is declared anywhere — that is the
/// deployed state today and is not an error. `Err` names what was wrong and
/// where, and a caller must treat it as "the deployment stated something the
/// orchestrator could not honour" rather than as "nothing was declared".
pub fn load() -> Result<Settings, String> {
    let file = std::env::var("DEJA_CONFIG_FILE")
        .ok()
        .map(|p| p.trim().to_owned())
        .filter(|p| !p.is_empty())
        .unwrap_or_else(|| DEFAULT_CONFIG_FILE.to_owned());

    let mut builder = Config::builder().add_source(File::with_name(&file).required(false));
    if let Ok(inline) = std::env::var("DEJA_CONFIG_TOML") {
        if !inline.trim().is_empty() {
            builder = builder.add_source(File::from_str(&inline, FileFormat::Toml));
        }
    }
    builder = builder.add_source(
        Environment::with_prefix("DEJA")
            .prefix_separator("__")
            .separator("__")
            .try_parsing(true),
    );

    let cfg = builder
        .build()
        .map_err(|e| format!("deja configuration could not be read: {e}"))?;
    serde_path_to_error::deserialize::<_, Settings>(cfg).map_err(|e| {
        format!(
            "deja configuration is invalid at `{}`: {}",
            e.path(),
            e.inner()
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_env::env_guard;

    fn clear() {
        for (k, _) in std::env::vars() {
            if k.starts_with("DEJA__") || k == "DEJA_CONFIG_TOML" || k == "DEJA_CONFIG_FILE" {
                std::env::remove_var(k);
            }
        }
    }

    /// The prefix is `DEJA__`, two underscores. Every single-underscore
    /// variable the orchestrator already reads must be invisible here, or
    /// this layer would silently re-read them under a different meaning.
    #[test]
    fn the_single_underscore_variables_are_not_this_layer() {
        let _lock = env_guard();
        clear();
        std::env::set_var("DEJA_S3_BUCKET", "not-a-system");
        std::env::set_var("DEJA_SYSTEMS", "not-a-map");
        let s = load().expect("loads");
        assert!(
            s.systems.is_empty(),
            "{:?}",
            s.systems.keys().collect::<Vec<_>>()
        );
        assert_eq!(s.default_system, None);
        std::env::remove_var("DEJA_SYSTEMS");
    }

    #[test]
    fn a_file_a_string_and_the_environment_layer_in_that_order() {
        let _lock = env_guard();
        clear();
        std::env::set_var(
            "DEJA_CONFIG_TOML",
            r#"
default_system = "hyperswitch"
[systems.hyperswitch]
s3_bucket = "from-toml"
manages_stores = true
scored_span_namespaces = ["a::", "b::"]
[systems.prism]
s3_bucket = "ucs-deja"
"#,
        );
        // The environment overrides one value and leaves the rest of the table.
        std::env::set_var("DEJA__SYSTEMS__HYPERSWITCH__S3_BUCKET", "from-env");
        // And can add a list as a comma string.
        std::env::set_var(
            "DEJA__SYSTEMS__PRISM__SCORED_SPAN_NAMESPACES",
            "ucs::, connector::",
        );
        let s = load().expect("loads");
        clear();

        assert_eq!(s.default_system.as_deref(), Some("hyperswitch"));
        let h = &s.systems["hyperswitch"];
        assert_eq!(h.s3_bucket.as_deref(), Some("from-env"), "env beats file");
        assert_eq!(
            h.manages_stores,
            Some(true),
            "untouched file values survive"
        );
        assert_eq!(
            h.scored_span_namespaces.as_deref(),
            Some(&["a::".to_owned(), "b::".to_owned()][..])
        );
        let p = &s.systems["prism"];
        assert_eq!(p.s3_bucket.as_deref(), Some("ucs-deja"));
        assert_eq!(
            p.scored_span_namespaces.as_deref(),
            Some(&["ucs::".to_owned(), "connector::".to_owned()][..]),
            "a comma string in the environment is a list"
        );
    }

    #[test]
    fn a_bad_value_names_its_field_not_a_line_and_column() {
        let _lock = env_guard();
        clear();
        std::env::set_var(
            "DEJA_CONFIG_TOML",
            "[systems.prism]\nmanages_stores = \"nope\"\n",
        );
        let err = load().expect_err("does not parse");
        clear();
        assert!(err.contains("systems.prism.manages_stores"), "{err}");
    }

    #[test]
    fn nothing_declared_is_empty_not_an_error() {
        let _lock = env_guard();
        clear();
        let s = load().expect("loads");
        assert!(s.systems.is_empty());
    }

    /// The reason this module lives in THIS crate rather than the orchestrator.
    ///
    /// The sealer takes a system name and has to find that system's recordings.
    /// Before the declaration existed it re-derived the answer from a flat
    /// `DEJA_<SYSTEM>_S3_BUCKET` variable — a second spelling of one convention,
    /// which went stale the moment the deployment moved to a document. It can
    /// read the document only if the document is defined at or below it, and
    /// `deja-orchestrator` depends on `deja-compactor`, never the reverse.
    ///
    /// So this test is the seam: if it stops compiling, the sealer has lost its
    /// ability to resolve a system and is back to guessing from variables.
    #[test]
    fn the_sealer_can_resolve_a_systems_bucket_from_the_declaration() {
        let _lock = env_guard();
        clear();
        std::env::set_var(
            "DEJA_CONFIG_TOML",
            "[systems.prism]\ns3_bucket = \"ucs-deja\"\n[systems.hyperswitch]\ns3_bucket = \"hyperswitch-art\"\n",
        );
        let s = load().expect("loads");
        clear();

        let bucket = |name: &str| s.systems.get(name).and_then(|d| d.s3_bucket.as_deref());
        assert_eq!(bucket("prism"), Some("ucs-deja"));
        assert_eq!(bucket("hyperswitch"), Some("hyperswitch-art"));
        // An undeclared system resolves to nothing — which a caller must report
        // as "not declared", never by falling back to another system's bucket.
        // Borrowing one system's recordings under another's name would not fail;
        // it would answer confidently and wrongly.
        assert_eq!(bucket("nope"), None);
    }
}
