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
    pub soak_hours: u32,
    /// Whether a promotion needs the version's promotion replay to have come back.
    pub promotion_replay_required: bool,
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
            soak_hours: cd.soak_hours.unwrap_or(0),
            promotion_replay_required: cd.promotion_replay_required.as_deref().map(str::trim)
                != Some("none"),
        })
    }

    /// Where a promotable environment is promoted from: the one before it.
    /// None for an environment that is not promotable, or the first.
    pub fn source_of(&self, target: &str) -> Option<&str> {
        if !self.promotable.iter().any(|p| p == target) {
            return None;
        }
        let i = self.environments.iter().position(|e| e == target)?;
        i.checked_sub(1).map(|j| self.environments[j].as_str())
    }

    pub fn knows_system(&self, system: &str) -> bool {
        self.systems.iter().any(|s| s == system)
    }

    pub fn knows_environment(&self, environment: &str) -> bool {
        self.environments.iter().any(|e| e == environment)
    }
}

/// What the promotion checks read, gathered by the handler. Kept apart from
/// the reading so every rule has a test.
#[derive(Debug, Clone, Default)]
pub struct PromotionFacts {
    /// Why the person may not promote, when they may not.
    pub refused: Option<String>,
    pub source: String,
    pub target: String,
    /// The version asked for: its kind and merge time; None when unknown.
    pub version_kind: Option<String>,
    pub version_merged_at: Option<chrono::DateTime<chrono::Utc>>,
    /// When the version first went live on the source; None when never.
    pub live_on_source_since: Option<chrono::DateTime<chrono::Utc>>,
    /// What the target runs now, and the merge time of what it carries.
    pub target_live: Option<String>,
    pub target_merged_at: Option<chrono::DateTime<chrono::Utc>>,
    /// The version's newest promotion replay: (state, against).
    pub promotion_replay: Option<(deja_store::PromotionReplayState, String)>,
    /// A request for the target that has not ended.
    pub in_flight: Option<i64>,
    /// The pause switch's reason, when paused.
    pub paused: Option<String>,
    pub now: chrono::DateTime<chrono::Utc>,
}

/// One rule and how the version fared against it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PromotionCheck {
    pub id: &'static str,
    pub ok: bool,
    pub why: String,
}

/// The rules a promotion is accepted under, in the order the dialog shows
/// them. All must pass.
pub fn promotion_checks(cfg: &CdConfig, f: &PromotionFacts) -> Vec<PromotionCheck> {
    let c = |id, ok, why: String| PromotionCheck { id, ok, why };
    let mut out = vec![];
    out.push(match &f.refused {
        None => c("person", true, "you may promote".into()),
        Some(why) => c("person", false, why.clone()),
    });
    out.push(match &f.paused {
        None => c("running", true, "the pipeline is running".into()),
        Some(why) => c("running", false, format!("the pipeline is paused: {why}")),
    });
    out.push(match f.version_kind.as_deref() {
        Some("merge") => c(
            "merge",
            true,
            "a merge, so the release is a tag on exactly it".into(),
        ),
        Some(k) => c(
            "merge",
            false,
            format!("a {k}; promote a merge, which gets its own release"),
        ),
        None => c("merge", false, "deja does not know this version".into()),
    });
    out.push(match f.live_on_source_since {
        Some(at) => c(
            "ran",
            true,
            format!(
                "ran on {} since {}",
                f.source,
                at.format("%Y-%m-%d %H:%M UTC")
            ),
        ),
        None => c("ran", false, format!("never went live on {}", f.source)),
    });
    out.push(match (f.version_merged_at, f.target_merged_at, &f.target_live) {
        (_, _, None) => c("newer", true, format!("{} runs nothing recorded", f.target)),
        (Some(v), Some(t), Some(live)) if v > t => c("newer", true, format!("newer than {live}, which {} runs", f.target)),
        (Some(_), Some(_), Some(live)) => c("newer", false, format!("not newer than {live}, which {} runs; going back is a rollback, not a promotion", f.target)),
        // A release first seen by reconcile has no merge time in deja. The
        // job checks ancestry on GitHub before tagging, so this is not the
        // last word; say so rather than refuse.
        (_, _, Some(live)) => c("newer", true, format!("deja has no merge time for {live}; the promote job checks on GitHub that this comes after it")),
    });
    out.push(if !cfg.promotion_replay_required {
        c(
            "promotion_replay",
            true,
            "no promotion replay required".into(),
        )
    } else {
        match (&f.promotion_replay, &f.target_live) {
            (_, None) => c(
                "promotion_replay",
                true,
                format!("{} runs nothing to replay against", f.target),
            ),
            (Some((state, against)), Some(live))
                if *state == deja_store::PromotionReplayState::Done && against == live =>
            {
                c(
                    "promotion_replay",
                    true,
                    format!("promotion replay against {live} came back"),
                )
            }
            (Some((state, against)), Some(live)) if against == live => c(
                "promotion_replay",
                false,
                format!("promotion replay against {live} is {state}"),
            ),
            (_, Some(live)) => c(
                "promotion_replay",
                false,
                format!("no promotion replay against {live} yet"),
            ),
        }
    });
    out.push(match f.in_flight {
        None => c(
            "one",
            true,
            format!("no other promotion to {} in flight", f.target),
        ),
        Some(id) => c(
            "one",
            false,
            format!("promotion #{id} to {} is still in flight", f.target),
        ),
    });
    if cfg.soak_hours > 0 {
        let need = chrono::Duration::hours(i64::from(cfg.soak_hours));
        out.push(match f.live_on_source_since {
            Some(at) if f.now - at >= need => c(
                "soak",
                true,
                format!("live on {} for {}h or more", f.source, cfg.soak_hours),
            ),
            Some(at) => c(
                "soak",
                false,
                format!(
                    "live on {} for {}h; {}h needed",
                    f.source,
                    (f.now - at).num_hours(),
                    cfg.soak_hours
                ),
            ),
            None => c("soak", false, format!("not live on {}", f.source)),
        });
    }
    out
}

/// Whether a version name is a release (a CalVer, `2026.10.10.0`, a hotfix
/// of one, `2026.09.28.1-hotfix1`, or the pipeline's internal deploy tag on
/// a promoted merge, `cd-2026.10.22.3`) rather than a merge's short sha.
/// Only for a version first seen on an environment, which the pipeline never
/// posted a kind for.
pub fn kind_of_version_name(version: &str) -> &'static str {
    let version = version.strip_prefix("cd-").unwrap_or(version);
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
/// `asked` answers whether an unfailed job request remains eligible;
/// `was_live_before` whether the observed synced version has been live here
/// earlier, and `last_confirmed_version` is the newest version ever confirmed
/// live here. Both are store questions, asked ahead so this stays pure.
///
/// A failed latest row does not imply a new deploy merely because Argo is
/// healthy at that same version: it is an observation of previously confirmed
/// state, not evidence that the failed intent succeeded.
pub fn transition(
    latest: Option<&Latest>,
    obs: &Observation,
    asked: impl Fn(&str) -> bool,
    was_live_before: impl Fn(&str) -> bool,
    last_confirmed_version: Option<&str>,
) -> Option<Transition> {
    if obs.is_live() {
        let v = obs.desired_version.as_str();
        return match latest {
            Some(l) if l.version == v && (l.state == "live" || l.state == "rolled_back") => None,
            Some(l)
                if l.version == v && l.state == "failed" && last_confirmed_version == Some(v) =>
            {
                None
            }
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
    let v = obs.desired_version.as_str();
    match latest {
        Some(l) if l.version == v && l.state != "failed" => None,
        Some(l) if l.version == v && l.state == "failed" && !asked(v) => None,
        _ => Some(Transition {
            version: v.to_owned(),
            state: "syncing".to_owned(),
            out_of_band: !asked(v),
        }),
    }
}

// ---- a promotion preview: what rides along, and whose change each
//      divergence is -----------------------------------------------------------

/// How the pipeline marks a merge-queue run today: its label starts with
/// this. Mirrors `QUEUE_LABEL` in `web/src/lib/scope.ts`, the only other
/// place the label is read rather than displayed. When the pipeline sends a
/// structured field, test it first and keep this as the fallback for the
/// runs already stored.
pub const QUEUE_LABEL: &str = "MQ · ";

/// The merges a promotion of `picked` carries to an environment running
/// `target_live`: `picked` and every merge before it, back to (not
/// including) the merge the target's release was cut on, newest first.
/// `None` when that cannot be known: the target runs nothing, or a release
/// that names no merge (one first seen by reconcile), or a merge déjà has no
/// merge time for, or `picked` is not after it.
pub fn carried_by<'a>(
    versions: &'a [deja_store::CdVersionRow],
    picked: &str,
    target_live: Option<&deja_store::CdVersionRow>,
) -> Option<Vec<&'a deja_store::CdVersionRow>> {
    let base_name = target_live?.released_from.as_deref()?;
    let since = versions
        .iter()
        .find(|v| v.version == base_name)?
        .merged_at?;
    let until = versions.iter().find(|v| v.version == picked)?.merged_at?;
    if until <= since {
        return None;
    }
    let mut out: Vec<&deja_store::CdVersionRow> = versions
        .iter()
        .filter(|v| v.kind == "merge" && v.merged_at.is_some_and(|m| m > since && m <= until))
        .collect();
    out.sort_by_key(|v| std::cmp::Reverse(v.merged_at));
    Some(out)
}

/// One divergence of the promotion delta, laid on the merge that first
/// showed it.
#[derive(Debug, Clone, Serialize)]
pub struct AttributedRow {
    #[serde(flatten)]
    pub row: crate::divergence::delta::Row,
    /// Later merges whose queue run showed the same shape: they changed it
    /// again on top of the first.
    pub also_changed_by: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AttributedGroup {
    pub version: String,
    pub rows: Vec<AttributedRow>,
}

/// The promotion delta's rows, by whose change they are.
#[derive(Debug, Default, Serialize)]
pub struct Attribution {
    /// One group per merge, in the order given (oldest first), holding the
    /// charged rows first seen on its queue run.
    pub groups: Vec<AttributedGroup>,
    /// Charged rows no queue run showed: several merges together, or a
    /// merge without a queue run.
    pub unattributed: Vec<crate::divergence::delta::Row>,
    /// Rows the target has and this version does not: a promotion removes
    /// them.
    pub fixed: Vec<crate::divergence::delta::Row>,
}

/// A row's shape: its lane and its address with the request-specific parts
/// dropped, the key acknowledgements are matched on too.
pub fn shape_of(row: &crate::divergence::delta::Row) -> crate::divergence::acknowledgement::Key {
    crate::divergence::acknowledgement::Key {
        lane: row.lane.clone(),
        pattern: crate::divergence::acknowledgement::Pattern::of(&row.address),
    }
}

/// Lay the promotion delta's rows on the merges that ride along. `queues`
/// is each merge with its merge-queue run's rows, OLDEST FIRST: a shape two
/// queue runs show is the older merge's, since the newer one's base already
/// carried it and it changed it again. A matched row takes the queue row's
/// acknowledgement, because the promotion replay names no pull request and
/// so has none of its own.
pub fn attribute(
    promotion_rows: Vec<crate::divergence::delta::Row>,
    queues: &[(String, Vec<crate::divergence::delta::Row>)],
) -> Attribution {
    use std::collections::HashMap;
    let index: Vec<
        HashMap<crate::divergence::acknowledgement::Key, &crate::divergence::delta::Row>,
    > = queues
        .iter()
        .map(|(_, rows)| {
            rows.iter()
                .filter(|r| r.bucket.charges_y())
                .map(|r| (shape_of(r), r))
                .collect()
        })
        .collect();
    let mut groups: Vec<AttributedGroup> = queues
        .iter()
        .map(|(version, _)| AttributedGroup {
            version: version.clone(),
            rows: Vec::new(),
        })
        .collect();
    let mut out = Attribution::default();
    for mut row in promotion_rows {
        if row.bucket.family() == "resolved" {
            out.fixed.push(row);
            continue;
        }
        if !row.bucket.charges_y() {
            continue;
        }
        let key = shape_of(&row);
        let hits: Vec<usize> = index
            .iter()
            .enumerate()
            .filter(|(_, m)| m.contains_key(&key))
            .map(|(i, _)| i)
            .collect();
        match hits.split_first() {
            None => out.unattributed.push(row),
            Some((&first, rest)) => {
                if row.acknowledgement.is_none() {
                    row.acknowledgement = index[first][&key].acknowledgement.clone();
                }
                let also_changed_by = rest.iter().map(|&i| queues[i].0.clone()).collect();
                groups[first].rows.push(AttributedRow {
                    row,
                    also_changed_by,
                });
            }
        }
    }
    out.groups = groups;
    out
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
            None,
        )
        .unwrap();
        assert_eq!(t.state, "live");
        assert!(!t.out_of_band);
    }

    #[test]
    fn the_same_observation_twice_appends_nothing() {
        let live = latest("8eaddb0b85", "live");
        let o = obs("8eaddb0b85", Some("8eaddb0b85"), "Synced", "Healthy");
        assert_eq!(
            transition(Some(&live), &o, |_| false, |_| false, Some("8eaddb0b85")),
            None
        );
        let sync = latest("8eaddb0b85", "syncing");
        let o = obs("8eaddb0b85", Some("efdd705fea"), "OutOfSync", "Progressing");
        assert_eq!(
            transition(Some(&sync), &o, |_| true, |_| false, Some("8eaddb0b85")),
            None
        );
    }

    #[test]
    fn a_version_nobody_asked_for_is_recorded_out_of_band() {
        let t = transition(
            Some(&latest("2026.10.09.1", "live")),
            &obs("2026.10.10.0", Some("2026.10.10.0"), "Synced", "Healthy"),
            |_| false,
            |_| false,
            None,
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
            Some("2026.10.12.1"),
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
            Some("2026.10.12.1"),
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
            Some("8eaddb0b85"),
        )
        .unwrap();
        assert_eq!((t.state.as_str(), t.out_of_band), ("syncing", true));
        // Once requested by a job, the same move is in band.
        let t = transition(
            Some(&latest("efdd705fea", "committed")),
            &obs("efdd705fea", Some("8eaddb0b85"), "OutOfSync", "Progressing"),
            |_| true,
            |_| false,
            Some("8eaddb0b85"),
        );
        assert_eq!(t, None, "committed already says a move is under way");
    }

    #[test]
    fn failed_new_version_observation_is_out_of_band_not_stale_request() {
        let t = transition(
            Some(&latest("B", "failed")),
            &obs("B", Some("B"), "Synced", "Healthy"),
            |_| false,
            |_| false,
            None,
        )
        .unwrap();
        assert_eq!(
            (t.version.as_str(), t.state.as_str(), t.out_of_band),
            ("B", "live", true)
        );
    }

    #[test]
    fn failed_retry_preserves_last_confirmed_version_and_retry_can_complete() {
        let o = obs("B", Some("B"), "Synced", "Healthy");
        assert_eq!(
            transition(
                Some(&latest("B", "failed")),
                &o,
                |_| false,
                |v| v == "B",
                Some("B")
            ),
            None
        );
        let t = transition(
            Some(&latest("B", "requested")),
            &o,
            |v| v == "B",
            |v| v == "B",
            Some("A"),
        )
        .unwrap();
        assert_eq!((t.state.as_str(), t.out_of_band), ("live", false));
    }

    #[test]
    fn failed_observation_of_superseded_historically_live_version_is_rollback() {
        let t = transition(
            Some(&latest("A", "failed")),
            &obs("A", Some("A"), "Synced", "Healthy"),
            |_| false,
            |v| v == "A",
            Some("B"),
        )
        .unwrap();
        assert_eq!(
            (t.version.as_str(), t.state.as_str(), t.out_of_band),
            ("A", "rolled_back", true)
        );

        assert_eq!(
            transition(
                Some(&latest("B", "failed")),
                &obs("B", Some("B"), "Synced", "Healthy"),
                |_| false,
                |v| v == "B",
                Some("B"),
            ),
            None,
            "a failed attempt at the currently confirmed version is not duplicated"
        );

        let retry = transition(
            Some(&latest("A", "failed")),
            &obs("A", Some("A"), "Synced", "Healthy"),
            |v| v == "A",
            |v| v == "A",
            Some("B"),
        )
        .unwrap();
        assert_eq!((retry.state.as_str(), retry.out_of_band), ("live", false));
    }

    #[test]
    fn a_degraded_live_version_is_not_a_deployment() {
        let t = transition(
            Some(&latest("8eaddb0b85", "live")),
            &obs("8eaddb0b85", Some("8eaddb0b85"), "Synced", "Degraded"),
            |_| false,
            |_| false,
            Some("8eaddb0b85"),
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
            None,
        )
        .unwrap();
        assert_eq!((t.state.as_str(), t.out_of_band), ("live", true));
    }

    #[test]
    fn version_kind_from_the_name() {
        assert_eq!(kind_of_version_name("2026.10.10.0"), "release");
        assert_eq!(kind_of_version_name("2026.10.12.11"), "release");
        assert_eq!(kind_of_version_name("2026.09.28.1-hotfix1"), "release");
        assert_eq!(kind_of_version_name("cd-2026.10.22.3"), "release");
        assert_eq!(kind_of_version_name("cd-2026.10.22.3-hotfix1"), "release");
        assert_eq!(kind_of_version_name("cd-"), "merge");
        assert_eq!(kind_of_version_name("cd-8eaddb0b85"), "merge");
        assert_eq!(kind_of_version_name("8eaddb0b85"), "merge");
        assert_eq!(kind_of_version_name("1.2.3.4"), "merge");
        assert_eq!(kind_of_version_name("8ead-db0b"), "merge");
    }
}

#[cfg(test)]
mod promotion {
    use super::*;
    use chrono::{Duration, TimeZone, Utc};

    fn cfg(soak: u32, promotion_replay: bool) -> CdConfig {
        CdConfig {
            systems: vec!["prism".into()],
            environments: vec!["integ".into(), "sandbox".into(), "prod".into()],
            promotable: vec!["sandbox".into()],
            soak_hours: soak,
            promotion_replay_required: promotion_replay,
        }
    }

    fn good() -> PromotionFacts {
        let now = Utc.with_ymd_and_hms(2026, 10, 14, 12, 0, 0).unwrap();
        PromotionFacts {
            refused: None,
            source: "integ".into(),
            target: "sandbox".into(),
            version_kind: Some("merge".into()),
            version_merged_at: Some(now - Duration::hours(30)),
            live_on_source_since: Some(now - Duration::hours(29)),
            target_live: Some("2026.10.09.1".into()),
            target_merged_at: Some(now - Duration::days(5)),
            promotion_replay: Some((
                deja_store::PromotionReplayState::Done,
                "2026.10.09.1".into(),
            )),
            in_flight: None,
            paused: None,
            now,
        }
    }

    fn failing(c: &CdConfig, f: &PromotionFacts) -> Vec<&'static str> {
        promotion_checks(c, f)
            .into_iter()
            .filter(|c| !c.ok)
            .map(|c| c.id)
            .collect()
    }

    #[test]
    fn the_source_is_the_environment_before_a_promotable_one() {
        let c = cfg(0, true);
        assert_eq!(c.source_of("sandbox"), Some("integ"));
        assert_eq!(c.source_of("prod"), None);
        assert_eq!(c.source_of("integ"), None);
    }

    #[test]
    fn a_version_that_ran_and_has_its_promotion_replay_passes_every_check() {
        assert!(failing(&cfg(0, true), &good()).is_empty());
    }

    #[test]
    fn each_rule_fails_on_its_own() {
        let c = cfg(0, true);
        let mut f = good();
        f.refused = Some("no".into());
        assert_eq!(failing(&c, &f), vec!["person"]);
        let mut f = good();
        f.paused = Some("integ broken".into());
        assert_eq!(failing(&c, &f), vec!["running"]);
        let mut f = good();
        f.version_kind = Some("release".into());
        assert_eq!(failing(&c, &f), vec!["merge"]);
        let mut f = good();
        f.live_on_source_since = None;
        assert_eq!(failing(&c, &f), vec!["ran"]);
        let mut f = good();
        f.version_merged_at = Some(f.now - Duration::days(9));
        assert_eq!(failing(&c, &f), vec!["newer"]);
        let mut f = good();
        f.promotion_replay = Some((
            deja_store::PromotionReplayState::Requested,
            "2026.10.09.1".into(),
        ));
        assert_eq!(failing(&c, &f), vec!["promotion_replay"]);
        let mut f = good();
        f.promotion_replay = Some((
            deja_store::PromotionReplayState::Done,
            "2026.10.01.0".into(),
        ));
        assert_eq!(
            failing(&c, &f),
            vec!["promotion_replay"],
            "a promotion replay against another release is not this one"
        );
        let mut f = good();
        f.in_flight = Some(7);
        assert_eq!(failing(&c, &f), vec!["one"]);
    }

    #[test]
    fn an_unknown_merge_time_on_the_target_is_left_to_the_job() {
        let mut f = good();
        f.target_merged_at = None;
        assert!(failing(&cfg(0, true), &f).is_empty());
    }

    #[test]
    fn nothing_live_on_the_target_needs_no_promotion_replay_and_is_newer() {
        let mut f = good();
        f.target_live = None;
        f.promotion_replay = None;
        assert!(failing(&cfg(0, true), &f).is_empty());
    }

    #[test]
    fn promotion_replay_none_and_soak_are_settings() {
        let mut f = good();
        f.promotion_replay = None;
        assert_eq!(failing(&cfg(0, true), &f), vec!["promotion_replay"]);
        assert!(failing(&cfg(0, false), &f).is_empty());
        assert!(failing(&cfg(24, true), &good()).is_empty());
        assert_eq!(failing(&cfg(48, true), &good()), vec!["soak"]);
    }
}

#[cfg(test)]
mod preview_tests {
    use super::*;
    use crate::divergence::acknowledgement::{RowAcknowledgement, State};
    use crate::divergence::behaviour_tree::{Address, Lane};
    use crate::divergence::delta::{Bucket, Row, Side};
    use chrono::{TimeZone, Utc};

    fn version(
        name: &str,
        kind: &str,
        released_from: Option<&str>,
        merged_hour: Option<u32>,
    ) -> deja_store::CdVersionRow {
        deja_store::CdVersionRow {
            system: "prism".into(),
            version: name.into(),
            kind: kind.into(),
            commit_sha: None,
            image_tag: None,
            chart: None,
            released_from: released_from.map(str::to_owned),
            merged_at: merged_hour.map(|h| Utc.with_ymd_and_hms(2026, 10, 23, h, 0, 0).unwrap()),
            created_at: Utc.with_ymd_and_hms(2026, 10, 23, 0, 0, 0).unwrap(),
            prs: vec![],
        }
    }

    #[test]
    fn carried_is_the_picked_merge_and_the_ones_back_to_the_release_s_merge() {
        let versions = vec![
            version("e5", "merge", None, Some(15)),
            version("d4", "merge", None, Some(14)),
            version("c3", "merge", None, Some(13)),
            version("cd-2026.10.23.1", "release", Some("b2"), Some(12)),
            version("b2", "merge", None, Some(12)),
            version("a1", "merge", None, Some(11)),
        ];
        let live = version("cd-2026.10.23.1", "release", Some("b2"), Some(12));
        let got: Vec<&str> = carried_by(&versions, "d4", Some(&live))
            .expect("the release names its merge, so what rides along is known")
            .iter()
            .map(|v| v.version.as_str())
            .collect();
        assert_eq!(
            got,
            vec!["d4", "c3"],
            "newest first, e5 stays on integ, b2 is the release's own merge"
        );
    }

    #[test]
    fn carried_is_unknown_without_a_merge_to_count_from() {
        let versions = vec![
            version("d4", "merge", None, Some(14)),
            version("2026.10.16.2", "release", None, None),
        ];
        assert!(
            carried_by(
                &versions,
                "d4",
                Some(&version("2026.10.16.2", "release", None, None))
            )
            .is_none(),
            "a release first seen on the environment names no merge"
        );
        assert!(carried_by(&versions, "d4", None).is_none(), "nothing live");
        let v2 = vec![
            version("d4", "merge", None, Some(14)),
            version("r", "release", Some("x"), Some(15)),
            version("x", "merge", None, Some(15)),
        ];
        assert!(
            carried_by(&v2, "d4", Some(&v2[1])).is_none(),
            "picked is not after the release's merge"
        );
    }

    fn row(connector: &str, path: &str, bucket: Bucket, ack: Option<&str>) -> Row {
        Row {
            address: Address::Body {
                correlation: "corr-1".into(),
                json_path: path.into(),
            },
            bucket,
            m: Side::Tape,
            y: Side::Hash("h1".into()),
            blocking: true,
            lane: Some(Lane {
                connector: connector.into(),
                flow: "authorize".into(),
            }),
            acknowledgement: ack.map(|by| RowAcknowledgement {
                id: 1,
                state: State::Acknowledged,
                by: by.into(),
                note: "intended".into(),
                value_changed: false,
            }),
        }
    }

    #[test]
    fn a_shape_goes_to_the_oldest_merge_that_showed_it_and_names_the_later_ones() {
        let promotion = vec![
            row("ilixium", "$.error_reason", Bucket::Introduced, None),
            row("tamara", "$.order.tax", Bucket::Introduced, None),
            row("jpmorgan", "$.initiator", Bucket::Changed, None),
            row("adyen", "$.status", Bucket::Resolved, None),
            row("adyen", "$.inherited", Bucket::Inherited, None),
        ];
        let queues = vec![
            (
                "older".to_owned(),
                vec![row(
                    "tamara",
                    "$.order.tax",
                    Bucket::Introduced,
                    Some("dev-a"),
                )],
            ),
            (
                "newer".to_owned(),
                vec![
                    row("tamara", "$.order.tax", Bucket::Changed, None),
                    row(
                        "ilixium",
                        "$.error_reason",
                        Bucket::Introduced,
                        Some("dev-b"),
                    ),
                ],
            ),
        ];
        let a = attribute(promotion, &queues);
        assert_eq!(a.groups[0].version, "older");
        assert_eq!(a.groups[0].rows.len(), 1);
        assert_eq!(
            a.groups[0].rows[0].also_changed_by,
            vec!["newer".to_owned()]
        );
        assert_eq!(
            a.groups[0].rows[0]
                .row
                .acknowledgement
                .as_ref()
                .map(|k| k.by.as_str()),
            Some("dev-a"),
            "the matched queue row's acknowledgement rides along"
        );
        assert_eq!(a.groups[1].rows.len(), 1);
        assert!(a.groups[1].rows[0].also_changed_by.is_empty());
        assert_eq!(a.unattributed.len(), 1, "jpmorgan: no queue run showed it");
        assert_eq!(
            a.fixed.len(),
            1,
            "resolved rows are what a promotion removes"
        );
    }

    #[test]
    fn no_queue_runs_leaves_every_charged_row_unattributed() {
        let a = attribute(vec![row("x", "$.a", Bucket::Introduced, None)], &[]);
        assert!(a.groups.is_empty());
        assert_eq!(a.unattributed.len(), 1);
    }
}
