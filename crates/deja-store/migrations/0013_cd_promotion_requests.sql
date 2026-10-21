-- Promotions: a signed-in person asks for a version to go to a promotable
-- environment; the promote job (Jenkins) picks the request up by polling
-- and walks it through its steps, posting each one. deja never calls
-- Jenkins: the request row is the hand-off.
--
-- States, in order: `pending` (accepted, waiting for the job), `picked`
-- (the job has it; set once, by a conditional update), `tagged` (the
-- release is named, `release` set), `built` (its image and chart exist),
-- `committed` (deploy-apps wrote the infra change), `live` (Argo runs it,
-- Synced and Healthy). Or it ends `failed` (with the step and reason) or
-- `cancelled` (by its requester, or a maintainer, while pending).
--
-- One request per environment at a time: the unique index admits a second
-- only once the first has ended.
CREATE TABLE cd_promotion_requests (
  id               bigserial PRIMARY KEY,
  system           text NOT NULL,
  environment      text NOT NULL,
  from_version     text NOT NULL,
  release          text,
  state            text NOT NULL DEFAULT 'pending',
  requested_by     text NOT NULL,
  requested_by_sub text,
  note             text,
  evidence         jsonb NOT NULL DEFAULT '{}'::jsonb,
  step_detail      jsonb NOT NULL DEFAULT '{}'::jsonb,
  picked_by_build  text,
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now(),
  finished_at      timestamptz,
  FOREIGN KEY (system, from_version) REFERENCES cd_versions (system, version)
);
CREATE UNIQUE INDEX cd_promotion_requests_one_in_flight
  ON cd_promotion_requests (system, environment)
  WHERE state NOT IN ('live', 'failed', 'cancelled');
CREATE INDEX cd_promotion_requests_recent
  ON cd_promotion_requests (system, environment, id DESC);
