-- An acknowledgement: one divergence a pull request introduces, accepted as
-- intended. Proposed by one person with a note, confirmed by another;
-- withdrawn rather than deleted so the trail survives.
--
-- Keyed to outlive the run it was proposed on: by the pull request, by the
-- lane (connector and flow) and by the address with its recording-specific
-- parts removed (the `pattern`), never by the run or the recording. A later
-- replay of the same pull request on a newer recording matches on that key.
--
-- `change_id` is the patch id of the pull request's own diff (merge-base to
-- head) when the acknowledgement was proposed. A run whose change id differs
-- shows the acknowledgement as stale: the author changed the change, so it is
-- reviewed again. A merge of main into the branch leaves the patch id alone.
--
-- `value_hash` is the candidate's value at proposal, kept for display; a
-- different value later is flagged on the row, not treated as uncovered.
CREATE TABLE acknowledgements (
  id              bigserial PRIMARY KEY,
  repo            text NOT NULL,
  pr_number       bigint NOT NULL,
  change_id       text NOT NULL,
  lane            jsonb,
  pattern         jsonb NOT NULL,
  value_hash      text,
  note            text NOT NULL,
  origin_run_id   text NOT NULL,
  proposed_by     text NOT NULL,
  proposed_at     timestamptz NOT NULL DEFAULT now(),
  acknowledged_by text,
  acknowledged_at timestamptz,
  withdrawn_by    text,
  withdrawn_at    timestamptz
);
CREATE INDEX acknowledgements_pr ON acknowledgements (repo, pr_number, id);

-- Runs by pull request, for re-stating every run's delta verdict when an
-- acknowledgement is confirmed or withdrawn. Text on both sides: the params
-- are JSON and the number is compared as the text it is stored as.
CREATE INDEX replay_runs_pull_request
  ON replay_runs ((params -> 'github' ->> 'repo'), (params -> 'github' ->> 'pr_number'));
