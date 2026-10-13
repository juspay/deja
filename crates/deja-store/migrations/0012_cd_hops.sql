-- Hop replays: a version replayed against the release the next environment
-- runs, three-way with a recording as the common ancestor, so a promotion
-- can be judged on what the version changes in behaviour rather than on the
-- whole distance from the recording (which carries every change since it).
--
-- One hop is a pair of deja runs: the candidate (the version) and the
-- baseline (the release), both driving the same tape; the verdict is the
-- candidate run's delta against the baseline run. The runs are linked by id
-- and the verdict copied here as the job read it, but the run row's own
-- `delta_verdict` stays the word of record: an acknowledgement given after
-- the hop changes what the run row says, not this row.
--
-- Append-only, like cd_deployments: the job writes `requested` when it
-- starts and `done`, `failed` (the replay could not be run or did not finish)
-- or `skipped` (nothing to measure against: the next environment runs
-- nothing recorded, or already runs this version) when it ends, each with a
-- reason. A version's newest row is its hop as things stand.
CREATE TABLE cd_hops (
  id              bigserial PRIMARY KEY,
  system          text NOT NULL,
  version         text NOT NULL,
  against_version text NOT NULL,
  against_commit  text,
  state           text NOT NULL,
  candidate_run   text,
  baseline_run    text,
  verdict         text,
  reason          text,
  requested_by    text NOT NULL,
  jenkins_build   text,
  detail          jsonb NOT NULL DEFAULT '{}'::jsonb,
  at              timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (system, version) REFERENCES cd_versions (system, version)
);
CREATE INDEX cd_hops_version ON cd_hops (system, version, id DESC);
