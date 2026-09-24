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
/// A missing report, or one without `members`, refuses: the tape it scored
/// cannot be established, and an unestablished tape must not pass for a
/// matching one. When both runs recorded seals, the seals decide — a
/// `seal_id` addresses the sealed content, so equal ids are equal tapes.
/// Otherwise the members and the correlation count decide, which cannot see a
/// re-seal that changed content without changing the count.
pub fn same_tape(
    y_id: &str,
    y_report: Option<&Value>,
    m_id: &str,
    m_report: Option<&Value>,
) -> Result<(), String> {
    let y = Tape::of(y_id, y_report)?;
    let m = Tape::of(m_id, m_report)?;
    // Deliberately not a refusal when only one side has seals. Every run from
    // before seals were recorded has none, so refusing here would refuse every
    // delta against existing work. Such a pair falls back to members and
    // count, which misses one case: equal members and equal counts over
    // different content. Absence is still refused above, in `Tape::of`.
    if let (Some(y_seals), Some(m_seals)) = (&y.seals, &m.seals) {
        if y_seals != m_seals {
            return Err(format!(
                "the runs scored different seals: {y_id} read {}, {m_id} read {}",
                describe(y_seals),
                describe(m_seals)
            ));
        }
        return Ok(());
    }
    if y.members != m.members || y.correlations != m.correlations {
        return Err(format!(
            "the runs scored different tapes: {y_id} read {} correlation(s) of [{}], \
             {m_id} read {} of [{}]",
            y.correlations,
            y.members.join(", "),
            m.correlations,
            m.members.join(", ")
        ));
    }
    Ok(())
}

/// What a run's ingest report says it read.
struct Tape {
    /// Sorted, so member order does not decide equality.
    members: Vec<String>,
    correlations: u64,
    /// Sorted `(recording_id, seal_id)`, or `None` when the report predates
    /// seal recording or a member carries no seal.
    seals: Option<Vec<(String, String)>>,
}

impl Tape {
    fn of(run_id: &str, report: Option<&Value>) -> Result<Self, String> {
        let report = report.ok_or_else(|| {
            format!("run {run_id} has no ingest report, so the tape it scored is unknown")
        })?;
        let mut members: Vec<String> = report
            .get("members")
            .and_then(Value::as_array)
            .ok_or_else(|| format!("run {run_id}'s ingest report names no members"))?
            .iter()
            .map(|m| m.as_str().map(str::to_owned))
            .collect::<Option<_>>()
            .ok_or_else(|| format!("run {run_id}'s ingest report has a non-string member"))?;
        members.sort();
        let correlations = report
            .get("correlations")
            .and_then(Value::as_u64)
            .ok_or_else(|| format!("run {run_id}'s ingest report has no correlation count"))?;
        let seals = report
            .get("member_seals")
            .and_then(Value::as_array)
            .filter(|seals| !seals.is_empty())
            .and_then(|seals| {
                seals
                    .iter()
                    .map(|s| {
                        let recording = s.get("recording_id")?.as_str()?;
                        let seal = s.get("seal_id")?.as_str().filter(|id| !id.is_empty())?;
                        Some((recording.to_owned(), seal.to_owned()))
                    })
                    .collect::<Option<Vec<_>>>()
            })
            .map(|mut seals| {
                seals.sort();
                seals
            })
            // Seals vouch for the tape only if they name every member and no
            // other; a partial list is not trusted as a whole one.
            .filter(|seals: &Vec<(String, String)>| {
                seals
                    .iter()
                    .map(|(recording, _)| recording)
                    .eq(members.iter())
            });
        Ok(Self {
            members,
            correlations,
            seals,
        })
    }
}

fn describe(seals: &[(String, String)]) -> String {
    seals
        .iter()
        .map(|(recording, seal)| format!("{recording}@{seal}"))
        .collect::<Vec<_>>()
        .join(", ")
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

    /// Seals that do not cover exactly the members cannot vouch for the tape:
    /// equal seals over different member lists must not pass.
    #[test]
    fn seals_that_do_not_cover_the_members_are_not_trusted() {
        let y = json!({
            "members": ["rec-a", "rec-b"], "correlations": 10,
            "member_seals": [{"recording_id": "rec-a", "seal_id": "s1"}],
        });
        let m = json!({
            "members": ["rec-a", "rec-c"], "correlations": 10,
            "member_seals": [{"recording_id": "rec-a", "seal_id": "s1"}],
        });
        assert!(same_tape("run-Y", Some(&y), "run-M", Some(&m)).is_err());
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
        let err = same_tape("run-Y", None, "run-M", Some(&a)).unwrap_err();
        assert!(err.contains("run run-Y has no ingest report"), "{err}");
        let err = same_tape("run-Y", Some(&a), "run-M", None).unwrap_err();
        assert!(err.contains("run run-M has no ingest report"), "{err}");
        assert!(same_tape("run-Y", None, "run-M", None).is_err());
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
