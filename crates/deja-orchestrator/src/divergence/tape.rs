//! Whether two runs scored the same tape.
//!
//! A recording is sealed on quiet and re-sealed as it grows, so two runs that
//! name the same recording or group can have read different content: two
//! runs of one group have read 2 and 78 correlations. A delta between them
//! would be decided by the tape rather than the candidates, so it must be
//! refused, and a run whose tape cannot be established refuses too.
//!
//! The evidence is each run's `ingest_report`: `member_seals` when the runner
//! recorded them, otherwise `members` and `correlations`.

use serde_json::Value;

/// `Ok` when `y` and `m` scored the same tape, `Err` naming why not.
///
/// Accepts every pair for now, which is what the delta does today: it
/// compares recording names only.
pub fn same_tape(
    _y_id: &str,
    _y_report: Option<&Value>,
    _m_id: &str,
    _m_report: Option<&Value>,
) -> Result<(), String> {
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sealed(members: &[(&str, &str)], correlations: usize) -> Value {
        json!({
            "members": members.iter().map(|(r, _)| *r).collect::<Vec<_>>(),
            "correlations": correlations,
            "member_seals": members
                .iter()
                .map(|(r, s)| json!({"recording_id": r, "seal_id": s}))
                .collect::<Vec<_>>(),
        })
    }

    fn counted(members: &[&str], correlations: usize) -> Value {
        json!({"members": members, "correlations": correlations})
    }

    #[test]
    fn two_seals_of_one_recording_are_different_tapes() {
        let early = sealed(&[("rec-d2", "aaaa")], 2);
        let late = sealed(&[("rec-d2", "bbbb")], 78);
        let err = same_tape("y", Some(&late), "m", Some(&early)).unwrap_err();
        assert!(err.contains("aaaa") && err.contains("bbbb"), "{err}");
    }

    /// What the counts cannot see: same members, same count, different content.
    #[test]
    fn different_seals_refuse_even_when_the_counts_agree() {
        let a = sealed(&[("rec-a", "s1")], 10);
        let b = sealed(&[("rec-a", "s2")], 10);
        assert!(same_tape("y", Some(&a), "m", Some(&b)).is_err());
    }

    /// A manifest from before seals were addressed carries an empty id; two
    /// empty ids are not a match, so the counts decide.
    #[test]
    fn an_empty_seal_id_is_no_seal() {
        let a = sealed(&[("rec-a", "")], 10);
        let b = sealed(&[("rec-a", "")], 11);
        assert!(same_tape("y", Some(&a), "m", Some(&b)).is_err());
    }

    #[test]
    fn one_seal_is_one_tape() {
        let a = sealed(&[("rec-a", "s1"), ("rec-b", "s2")], 40);
        let b = sealed(&[("rec-b", "s2"), ("rec-a", "s1")], 40);
        assert_eq!(same_tape("y", Some(&a), "m", Some(&b)), Ok(()));
    }

    #[test]
    fn a_member_added_to_the_group_is_a_different_tape() {
        let before = sealed(&[("rec-a", "s1")], 10);
        let after = sealed(&[("rec-a", "s1"), ("rec-b", "s2")], 30);
        assert!(same_tape("y", Some(&after), "m", Some(&before)).is_err());
    }

    #[test]
    fn a_run_with_no_report_refuses_on_either_side() {
        let a = sealed(&[("rec-a", "s1")], 10);
        let err = same_tape("y", None, "m", Some(&a)).unwrap_err();
        assert!(err.contains("y"), "{err}");
        let err = same_tape("y", Some(&a), "m", None).unwrap_err();
        assert!(err.contains("m"), "{err}");
        assert!(same_tape("y", None, "m", None).is_err());
    }

    /// Two reports that both name no members and agree on the count must still
    /// refuse: nothing says what either run read.
    #[test]
    fn a_report_without_members_refuses() {
        let bare = json!({"correlations": 10});
        let err = same_tape("y", Some(&bare), "m", Some(&bare)).unwrap_err();
        assert!(err.contains("names no members"), "{err}");
    }

    #[test]
    fn without_seals_the_counts_decide() {
        let a = counted(&["rec-a", "rec-b"], 1341);
        let b = counted(&["rec-b", "rec-a"], 1341);
        assert_eq!(same_tape("y", Some(&a), "m", Some(&b)), Ok(()));
        let short = counted(&["rec-a", "rec-b"], 1137);
        assert!(same_tape("y", Some(&a), "m", Some(&short)).is_err());
        let other = counted(&["rec-a", "rec-c"], 1341);
        assert!(same_tape("y", Some(&a), "m", Some(&other)).is_err());
    }

    #[test]
    fn a_sealed_run_against_an_unsealed_one_falls_back_to_counts() {
        let with_seals = sealed(&[("rec-a", "s1")], 10);
        let same_counts = counted(&["rec-a"], 10);
        assert_eq!(
            same_tape("y", Some(&with_seals), "m", Some(&same_counts)),
            Ok(())
        );
        let other_counts = counted(&["rec-a"], 9);
        assert!(same_tape("y", Some(&with_seals), "m", Some(&other_counts)).is_err());
    }
}
