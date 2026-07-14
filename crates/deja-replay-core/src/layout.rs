//! Every S3 key template in the replay platform lives here — nowhere else.
//!
//! All functions take the configured key `prefix` explicitly; an empty
//! prefix yields exactly today's bucket-root layout, so existing recordings
//! remain addressable.

fn join(prefix: &str, rest: &str) -> String {
    let p = prefix.trim_matches('/');
    if p.is_empty() {
        rest.to_owned()
    } else {
        format!("{p}/{rest}")
    }
}

/// Root of a sealed recording session: `{prefix}/sessions/v1/{recording_id}`.
/// Delegates the un-prefixed tail to `deja_compactor::layout` so the two
/// crates cannot disagree about the session shape.
pub fn recording_session_root(prefix: &str, recording_id: &str) -> String {
    join(prefix, &deja_compactor::layout::session_root(recording_id))
}

/// A run artifact object: `{prefix}/runs/{run_id}/{name}`.
pub fn run_artifact(prefix: &str, run_id: &str, name: &str) -> String {
    join(prefix, &format!("runs/{run_id}/{name}"))
}

/// A published candidate router binary: `{prefix}/builds/{build_ref}/router`.
pub fn candidate_build(prefix: &str, build_ref: &str) -> String {
    join(prefix, &format!("builds/{build_ref}/router"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // tests panic on failure by design
mod tests {
    use super::*;

    #[test]
    fn empty_prefix_matches_legacy_layout() {
        assert_eq!(recording_session_root("", "rec-1"), "sessions/v1/rec-1");
        assert_eq!(
            run_artifact("", "run-1", "scorecard.json"),
            "runs/run-1/scorecard.json"
        );
        assert_eq!(candidate_build("", "abc123"), "builds/abc123/router");
    }

    #[test]
    fn prefix_is_joined_and_slash_trimmed() {
        assert_eq!(
            recording_session_root("/deja/v1/", "rec-1"),
            "deja/v1/sessions/v1/rec-1"
        );
        assert_eq!(
            run_artifact("deja/v1", "run-1", "agent.log"),
            "deja/v1/runs/run-1/agent.log"
        );
        assert_eq!(
            candidate_build("deja/v1", "sha-9f"),
            "deja/v1/builds/sha-9f/router"
        );
    }
}
