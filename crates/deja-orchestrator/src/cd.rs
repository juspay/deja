//! Continuous deployment: what runs where, derived from what the pipeline
//! posts and what Argo is observed running.
//!
//! The store keeps two things: versions (a merge or a release, with the pull
//! requests it carries) and an append-only deployment log per environment.
//! Jenkins writes both — a version when it builds one, a `requested` /
//! `committed` / `failed` row when it acts — and a reconcile job posts what
//! Argo reports every few minutes. This module holds the one decision in
//! that: given an environment's newest row and an observation, what row, if
//! any, is appended. It is a pure function so the cases are tested here and
//! the handler only runs it.
//!
//! Observations are facts, never names: the reconcile job posts the version
//! Argo's chart resolves to, and this never parses a chart or image name.

use serde::{Deserialize, Serialize};

use crate::settings::{self, CdSettings};

/// `[cd]`, resolved: nothing optional left.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CdConfig {
    pub systems: Vec<String>,
    pub environments: Vec<String>,
    pub promotable: Vec<String>,
}

impl CdConfig {
    /// From the declared block. `None` when no `[cd]` block was declared: the
    /// pages and endpoints answer that deployment tracking is off, rather
    /// than inventing an environment list.
    pub fn from_settings(s: &settings::Settings) -> Option<Self> {
        let cd: &CdSettings = s.cd.as_ref()?;
        let systems = cd
            .systems
            .clone()
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| vec![crate::default_system().to_owned()]);
        let environments = cd
            .environments
            .clone()
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| ["integ", "sandbox", "prod"].map(str::to_owned).to_vec());
        Some(Self {
            systems,
            environments,
            promotable: cd.promotable.clone().unwrap_or_default(),
        })
    }

    pub fn knows_system(&self, system: &str) -> bool {
        self.systems.iter().any(|s| s == system)
    }

    pub fn knows_environment(&self, environment: &str) -> bool {
        self.environments.iter().any(|e| e == environment)
    }
}

/// Whether a version name is a CalVer release (`2026.10.10.0`, or a hotfix
/// of one, `2026.09.28.1-hotfix1`) rather than a merge's short sha. Only for
/// a version first seen on an environment, which the pipeline never posted a
/// kind for.
pub fn kind_of_version_name(version: &str) -> &'static str {
    let base = version.split_once('-').map_or(version, |(b, _)| b);
    let parts: Vec<&str> = base.split('.').collect();
    let calver = parts.len() == 4
        && parts
            .iter()
            .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
        && parts[0].len() == 4;
    if calver {
        "release"
    } else {
        "merge"
    }
}

/// What Argo reports for one environment, as the reconcile job posts it.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Observation {
    pub system: String,
    /// What the Argo application is asked to run.
    pub desired_version: String,
    /// What it is running: `None` while nothing has synced yet.
    #[serde(default)]
    pub synced_version: Option<String>,
    /// Argo's sync status, `Synced` / `OutOfSync` / …
    pub sync: String,
    /// Argo's health, `Healthy` / `Progressing` / `Degraded` / …
    pub health: String,
    /// When the job read Argo, as it wrote it; kept on the row's detail.
    #[serde(default)]
    pub observed_at: Option<String>,
    /// The raw chart and image, kept on the row for the record.
    #[serde(default)]
    pub chart: Option<String>,
    #[serde(default)]
    pub image_tag: Option<String>,
}

impl Observation {
    pub fn is_live(&self) -> bool {
        self.sync == "Synced"
            && self.health == "Healthy"
            && self.synced_version.as_deref() == Some(self.desired_version.as_str())
    }
}

/// The newest row of an environment, reduced to what the decision reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Latest {
    pub version: String,
    pub state: String,
}

/// What to append for an observation: the version, its state, and whether
/// the version was observed without anyone asking for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transition {
    pub version: String,
    pub state: String,
    pub out_of_band: bool,
}

/// The decision. `latest` is the environment's newest row of any state.
/// `asked` answers whether a job asked for a version on this environment
/// (as opposed to it turning up); `was_live_before` whether the observed
/// synced version has been live here earlier (a return to it is a rollback,
/// a first arrival is a deploy). Both are store questions, asked ahead so
/// this stays a pure function.
///
/// Nothing is appended when the observation says what the newest row
/// already says, so a reconcile every five minutes writes only on change.
pub fn transition(
    latest: Option<&Latest>,
    obs: &Observation,
    asked: impl Fn(&str) -> bool,
    was_live_before: impl Fn(&str) -> bool,
) -> Option<Transition> {
    if obs.is_live() {
        let v = obs.desired_version.as_str();
        return match latest {
            // Already recorded as running this.
            Some(l) if l.version == v && (l.state == "live" || l.state == "rolled_back") => None,
            // What a job asked for arrived, or something nobody asked for.
            _ => Some(Transition {
                version: v.to_owned(),
                state: if !asked(v) && was_live_before(v) {
                    "rolled_back".to_owned()
                } else {
                    "live".to_owned()
                },
                out_of_band: !asked(v),
            }),
        };
    }
    // Not live yet: Argo is moving to `desired`, or is stuck there. Record
    // `syncing` for the desired version once; a later observation that still
    // is not live appends nothing, and the one that is appends `live`.
    let v = obs.desired_version.as_str();
    match latest {
        Some(l) if l.version == v && l.state != "live" && l.state != "rolled_back" => None,
        // Desired is what we last had live: Argo reports it degraded or out
        // of sync, which is a health fact, not a deployment. Nothing to append.
        Some(l) if l.version == v => None,
        _ => Some(Transition {
            version: v.to_owned(),
            state: "syncing".to_owned(),
            out_of_band: !asked(v),
        }),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn obs(desired: &str, synced: Option<&str>, sync: &str, health: &str) -> Observation {
        Observation {
            system: "prism".into(),
            desired_version: desired.into(),
            synced_version: synced.map(str::to_owned),
            sync: sync.into(),
            health: health.into(),
            observed_at: None,
            chart: None,
            image_tag: None,
        }
    }

    fn latest(version: &str, state: &str) -> Latest {
        Latest {
            version: version.into(),
            state: state.into(),
        }
    }

    #[test]
    fn a_requested_version_that_synced_goes_live() {
        let t = transition(
            Some(&latest("8eaddb0b85", "committed")),
            &obs("8eaddb0b85", Some("8eaddb0b85"), "Synced", "Healthy"),
            |_| true,
            |_| false,
        )
        .unwrap();
        assert_eq!(t.state, "live");
        assert!(!t.out_of_band);
    }

    #[test]
    fn the_same_observation_twice_appends_nothing() {
        let live = latest("8eaddb0b85", "live");
        let o = obs("8eaddb0b85", Some("8eaddb0b85"), "Synced", "Healthy");
        assert_eq!(transition(Some(&live), &o, |_| false, |_| false), None);
        let sync = latest("8eaddb0b85", "syncing");
        let o = obs("8eaddb0b85", Some("efdd705fea"), "OutOfSync", "Progressing");
        assert_eq!(transition(Some(&sync), &o, |_| true, |_| false), None);
    }

    #[test]
    fn a_version_nobody_asked_for_is_recorded_out_of_band() {
        let t = transition(
            Some(&latest("2026.10.09.1", "live")),
            &obs("2026.10.10.0", Some("2026.10.10.0"), "Synced", "Healthy"),
            |_| false,
            |_| false,
        )
        .unwrap();
        assert_eq!((t.state.as_str(), t.out_of_band), ("live", true));
    }

    #[test]
    fn a_return_to_an_earlier_live_version_is_a_rollback() {
        let t = transition(
            Some(&latest("2026.10.12.1", "live")),
            &obs("2026.10.12.0", Some("2026.10.12.0"), "Synced", "Healthy"),
            |_| false,
            |v| v == "2026.10.12.0",
        )
        .unwrap();
        assert_eq!(t.state, "rolled_back");
    }

    #[test]
    fn a_requested_return_is_a_deploy_not_a_rollback_in_name() {
        // The job that asked for it said `rolled_back` itself when it did.
        let t = transition(
            Some(&latest("2026.10.12.0", "committed")),
            &obs("2026.10.12.0", Some("2026.10.12.0"), "Synced", "Healthy"),
            |_| true,
            |_| true,
        )
        .unwrap();
        assert_eq!(t.state, "live");
    }

    #[test]
    fn a_new_desired_version_is_syncing_until_it_arrives() {
        let t = transition(
            Some(&latest("8eaddb0b85", "live")),
            &obs("efdd705fea", Some("8eaddb0b85"), "OutOfSync", "Healthy"),
            |_| false,
            |_| false,
        )
        .unwrap();
        assert_eq!((t.state.as_str(), t.out_of_band), ("syncing", true));
        // Once requested by a job, the same move is in band.
        let t = transition(
            Some(&latest("efdd705fea", "committed")),
            &obs("efdd705fea", Some("8eaddb0b85"), "OutOfSync", "Progressing"),
            |_| true,
            |_| false,
        );
        assert_eq!(t, None, "committed already says a move is under way");
    }

    #[test]
    fn a_degraded_live_version_is_not_a_deployment() {
        let t = transition(
            Some(&latest("8eaddb0b85", "live")),
            &obs("8eaddb0b85", Some("8eaddb0b85"), "Synced", "Degraded"),
            |_| false,
            |_| false,
        );
        assert_eq!(t, None);
    }

    #[test]
    fn the_first_observation_of_an_environment_is_recorded() {
        let t = transition(
            None,
            &obs("2026.10.09.1", Some("2026.10.09.1"), "Synced", "Healthy"),
            |_| false,
            |_| false,
        )
        .unwrap();
        assert_eq!((t.state.as_str(), t.out_of_band), ("live", true));
    }

    #[test]
    fn version_kind_from_the_name() {
        assert_eq!(kind_of_version_name("2026.10.10.0"), "release");
        assert_eq!(kind_of_version_name("2026.10.12.11"), "release");
        assert_eq!(kind_of_version_name("2026.09.28.1-hotfix1"), "release");
        assert_eq!(kind_of_version_name("8eaddb0b85"), "merge");
        assert_eq!(kind_of_version_name("1.2.3.4"), "merge");
        assert_eq!(kind_of_version_name("8ead-db0b"), "merge");
    }
}
