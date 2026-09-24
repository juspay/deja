//! Every artifact a replay run publishes, in one table: its name in the
//! artifact index, the object it is published as, where it sits on the host
//! that wrote or serves it, and what bounds that local copy.
//!
//! The publish loop, hydration, the detail endpoints and the artifact-cache
//! sweep all read this table. Two lists used to disagree about what a run
//! publishes (`record_graph` was in one, `seed_certificate` in the other), and
//! reasoning about S3 from either alone gave the wrong answer.

use std::path::PathBuf;

use crate::HarnessRoot;

/// How a kind reaches the store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Publish {
    /// By the lifecycle's publish loop, for every scored run.
    Stream,
    /// By its own step, and only when that step produced it.
    OwnStep,
}

/// What keeps a kind's local copy from growing without bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bound {
    /// Hydrated onto the orchestrator and evicted by its artifact-cache budget.
    CacheBudget,
    /// Never hydrated: on k8s it exists only in the ephemeral runner pod, and
    /// the published object is the record.
    RunnerPod,
}

pub struct RunArtifactKind {
    /// The `artifacts.kind` value, constrained by the store's migrations.
    pub name: &'static str,
    /// The object name it is published under, beside the run's other objects.
    pub object: &'static str,
    pub local_path: fn(&HarnessRoot, &str) -> PathBuf,
    pub publish: Publish,
    /// Hydrated from the store and served by the API.
    pub served: bool,
    pub bound: Bound,
}

impl RunArtifactKind {
    pub fn path(&self, root: &HarnessRoot, run_id: &str) -> PathBuf {
        (self.local_path)(root, run_id)
    }
}

/// Read only by scoring, in the replay pod. The API streams the published
/// object by URI and never reads a local copy, so it is not hydrated.
pub const LOOKUP_TABLE: RunArtifactKind = RunArtifactKind {
    name: "lookup_table",
    object: "lookup_table.json",
    local_path: HarnessRoot::lookup_table_path,
    publish: Publish::Stream,
    served: false,
    bound: Bound::RunnerPod,
};
pub const OBSERVED: RunArtifactKind = RunArtifactKind {
    name: "observed",
    object: "observed.jsonl",
    local_path: HarnessRoot::observed_path,
    publish: Publish::Stream,
    served: true,
    bound: Bound::CacheBudget,
};
pub const HTTP_DIFFS: RunArtifactKind = RunArtifactKind {
    name: "http_diffs",
    object: "http_diffs.jsonl",
    local_path: HarnessRoot::http_diff_path,
    publish: Publish::Stream,
    served: true,
    bound: Bound::CacheBudget,
};
pub const SCORECARD: RunArtifactKind = RunArtifactKind {
    name: "scorecard",
    object: "scorecard.json",
    local_path: HarnessRoot::scorecard_path,
    publish: Publish::Stream,
    served: true,
    bound: Bound::CacheBudget,
};
pub const CALL_LEDGER: RunArtifactKind = RunArtifactKind {
    name: "call_ledger",
    object: "call_ledger.jsonl",
    local_path: HarnessRoot::call_ledger_path,
    publish: Publish::Stream,
    served: true,
    bound: Bound::CacheBudget,
};
/// The run's own account of what seeding did, per entry, with readback. It was
/// once registered under the pod's local path and never uploaded, so on k8s it
/// was unreadable the moment the pod died. It publishes beside the full lookup
/// table, so it adds no new kind of egress. The scorer reads the local copy
/// while scoring; nothing in the API reads the published one back. It is kept
/// for the reader and for audit.
pub const SEED_CERTIFICATE: RunArtifactKind = RunArtifactKind {
    name: "seed_certificate",
    object: "seed-certificate.json",
    local_path: HarnessRoot::seed_certificate_path,
    publish: Publish::Stream,
    served: false,
    bound: Bound::RunnerPod,
};
/// Record-side span structure, extracted in the pod so the recording tape never
/// leaves it. Absent when extraction refused, so it is published by its own step.
pub const RECORD_GRAPH: RunArtifactKind = RunArtifactKind {
    name: "record_graph",
    object: "record_graph.jsonl",
    local_path: HarnessRoot::record_graph_path,
    publish: Publish::OwnStep,
    served: true,
    bound: Bound::CacheBudget,
};

/// Every kind, in publish order.
pub const RUN_ARTIFACT_KINDS: [RunArtifactKind; 7] = [
    LOOKUP_TABLE,
    OBSERVED,
    HTTP_DIFFS,
    SCORECARD,
    CALL_LEDGER,
    SEED_CERTIFICATE,
    RECORD_GRAPH,
];

/// The kinds the lifecycle's publish loop publishes.
pub fn streamed() -> impl Iterator<Item = &'static RunArtifactKind> {
    RUN_ARTIFACT_KINDS
        .iter()
        .filter(|kind| kind.publish == Publish::Stream)
}

/// The content type the raw endpoint serves an artifact as.
///
/// Decided by its kind, not by the URI it was registered under: a compose run
/// registers the local path, and runs published before a rename keep their old
/// object name, so the extension on the URI can say the wrong thing.
pub fn served_content_type(kind: &str, uri: &str) -> &'static str {
    let name = RUN_ARTIFACT_KINDS
        .iter()
        .find(|k| k.name == kind)
        .map_or(uri, |k| k.object);
    if kind == "visualization_html" {
        "text/html; charset=utf-8"
    } else if name.ends_with(".json") {
        "application/json"
    } else {
        "application/x-ndjson"
    }
}

/// The kinds hydrated onto the orchestrator and served by the API.
pub fn served() -> impl Iterator<Item = &'static RunArtifactKind> {
    RUN_ARTIFACT_KINDS.iter().filter(|kind| kind.served)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A served kind is hydrated onto the orchestrator, so the cache budget is
    /// what bounds it; a kind that is not served never reaches that volume.
    #[test]
    fn a_kind_is_bounded_by_the_cache_exactly_when_it_is_served() {
        for kind in &RUN_ARTIFACT_KINDS {
            assert_eq!(
                kind.served,
                kind.bound == Bound::CacheBudget,
                "{}: served and bound disagree",
                kind.name
            );
        }
    }

    /// The lifecycle has exactly one publish step of its own, for the record
    /// graph; every other kind goes through the loop. A kind on both paths is
    /// uploaded and registered twice, and one on neither is never published.
    #[test]
    fn each_kind_is_published_by_exactly_one_path() {
        for kind in &RUN_ARTIFACT_KINDS {
            assert_eq!(
                kind.publish == Publish::OwnStep,
                kind.name == RECORD_GRAPH.name,
                "{}: publish path does not match the lifecycle's steps",
                kind.name
            );
        }
    }

    /// The lookup table is one document on one line. Served as NDJSON, a line
    /// reader gets one plausible record, the envelope, instead of an error;
    /// that must hold for a compose run's local path and an old `.jsonl`
    /// object as much as for a new one.
    #[test]
    fn the_lookup_table_is_served_as_a_json_document_whatever_its_uri() {
        for uri in [
            "s3://bucket/replay-runs/run-1/lookup_table.json",
            "s3://bucket/replay-runs/run-1/lookup_table.jsonl",
            "/workspace/state/lookup-tables/run-1.jsonl",
        ] {
            assert_eq!(
                served_content_type(LOOKUP_TABLE.name, uri),
                "application/json",
                "{uri}"
            );
        }
        assert_eq!(
            served_content_type(CALL_LEDGER.name, "/state/ledgers/run-1.jsonl"),
            "application/x-ndjson",
            "a line stream stays NDJSON"
        );
        assert_eq!(
            served_content_type("some_future_kind", "s3://b/x.json"),
            "application/json",
            "a kind outside the table falls back to its URI"
        );
    }

    #[test]
    fn names_and_objects_are_unique() {
        let mut names: Vec<_> = RUN_ARTIFACT_KINDS.iter().map(|k| k.name).collect();
        let mut objects: Vec<_> = RUN_ARTIFACT_KINDS.iter().map(|k| k.object).collect();
        names.sort_unstable();
        names.dedup();
        objects.sort_unstable();
        objects.dedup();
        assert_eq!(names.len(), RUN_ARTIFACT_KINDS.len());
        assert_eq!(objects.len(), RUN_ARTIFACT_KINDS.len());
    }
}
