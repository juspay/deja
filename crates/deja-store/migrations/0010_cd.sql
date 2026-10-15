-- Continuous deployment: what version runs where, and how it got there.
--
-- A VERSION is one thing a system can run. Two kinds: a `merge`, one squashed
-- pull request on main, named by its short sha and deployed to integ as it
-- lands; a `release`, a CalVer tag placed on one of those commits, named by
-- that commit's merge date, which sandbox and prod run. A release records the
-- merge version it was tagged on in `released_from`.
--
-- A version's pull requests are its contents: one for a merge, every merge
-- since the previous release for a release. Written with the version and
-- replaced with it, so a re-post is idempotent.
--
-- `merged_at` is the commit's merge time, which the release name is derived
-- from and the picker orders by. It is what the pipeline knew when it posted;
-- a version observed on an environment before anyone posted it has none.
CREATE TABLE cd_versions (
  system        text NOT NULL,
  version       text NOT NULL,
  kind          text NOT NULL,
  commit_sha    text,
  image_tag     text,
  chart         text,
  released_from text,
  merged_at     timestamptz,
  created_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (system, version)
);

CREATE TABLE cd_version_prs (
  system     text NOT NULL,
  version    text NOT NULL,
  repo       text NOT NULL,
  pr_number  bigint NOT NULL,
  title      text,
  author     text,
  merged_at  timestamptz,
  PRIMARY KEY (system, version, repo, pr_number),
  FOREIGN KEY (system, version) REFERENCES cd_versions (system, version)
);
-- A pull request's versions: the merge it is, and every release carrying it.
CREATE INDEX cd_version_prs_pr ON cd_version_prs (repo, pr_number);

-- Append-only. Every change to what an environment is asked to run or is
-- observed running is one row, newest last; the environment's current state
-- is its newest row. States: `requested` (a job asked for it), `committed`
-- (the job wrote the infra change), `syncing` (Argo is moving to it), `live`
-- (Argo reports it synced and healthy), `failed` (the job gave up),
-- `rolled_back` (an older version went live again).
--
-- `requested_by` is who asked, as the job reported it: a person's email for a
-- promotion, `merge #N` for integ. `requested_by_sub` is the sign-in subject
-- beside it, null when the job did not have one. Rows written from an
-- observation name `observed` and say so in `detail.out_of_band` when no job
-- asked for that version: a deploy made by hand is recorded, not hidden.
CREATE TABLE cd_deployments (
  id               bigserial PRIMARY KEY,
  system           text NOT NULL,
  environment      text NOT NULL,
  version          text NOT NULL,
  state            text NOT NULL,
  requested_by     text NOT NULL,
  requested_by_sub text,
  request_id       bigint,
  jenkins_build    text,
  detail           jsonb NOT NULL DEFAULT '{}'::jsonb,
  at               timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (system, version) REFERENCES cd_versions (system, version)
);
CREATE INDEX cd_deployments_env ON cd_deployments (system, environment, id DESC);
CREATE INDEX cd_deployments_version ON cd_deployments (system, version, id DESC);
