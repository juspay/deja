-- Phase 1 schema slice (docs/PHASE1_WEB_MATRIX_PLAN.md §W3). The master
-- design's remaining tables (runners, schedules, recording_window_*) land in
-- later migrations in this same crate — the single migration set.

CREATE TABLE recordings (
  recording_id      text PRIMARY KEY,
  kind              text NOT NULL DEFAULT 'session' CHECK (kind IN ('session','window')),
  source_path       text,
  event_count       bigint,
  correlation_count bigint,
  byte_size         bigint,
  status            text NOT NULL DEFAULT 'ready' CHECK (status IN ('ready','failed','deleted')),
  created_by        text NOT NULL DEFAULT 'unknown',
  created_at        timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX recordings_browse ON recordings (kind, created_at DESC);

CREATE TABLE replay_runs (
  run_id           text PRIMARY KEY,             -- legacy run-<hexnanos> ids stay valid
  mode             text NOT NULL CHECK (mode IN ('record','replay')),
  recording_id     text,
  candidate        jsonb NOT NULL DEFAULT '{}',
  candidate_sha256 text,
  params           jsonb NOT NULL DEFAULT '{}',
  config_snapshot  jsonb NOT NULL DEFAULT '{}',
  state            text NOT NULL DEFAULT 'pending',
  verdict          text CHECK (verdict IN ('pass','fail','inconclusive')),
  scorecard        jsonb,
  failure          jsonb,
  expectation      text,
  created_by       text NOT NULL DEFAULT 'unknown',
  created_at       timestamptz NOT NULL DEFAULT now(),
  started_at       timestamptz,
  finished_at      timestamptz
);
CREATE INDEX replay_runs_list ON replay_runs (created_at DESC);
CREATE INDEX replay_runs_recording ON replay_runs (recording_id, created_at DESC);

CREATE TABLE run_stages (
  id          bigserial PRIMARY KEY,
  run_id      text NOT NULL REFERENCES replay_runs(run_id) ON DELETE CASCADE,
  stage       text NOT NULL,
  status      text NOT NULL DEFAULT 'running' CHECK (status IN ('running','ok','failed')),
  step        int,
  steps_total int,
  started_at  timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  detail      jsonb NOT NULL DEFAULT '{}'
);
CREATE INDEX run_stages_run ON run_stages (run_id, id);

CREATE TABLE run_log_chunks (
  run_id text   NOT NULL,
  stage  text   NOT NULL,
  seq    bigint NOT NULL,
  lines  text   NOT NULL,
  ts     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, stage, seq)
);

CREATE TABLE artifacts (
  id         bigserial PRIMARY KEY,
  run_id     text REFERENCES replay_runs(run_id) ON DELETE CASCADE,
  recording_id text,
  kind       text NOT NULL CHECK (kind IN
    ('events','lookup_table','observed','http_diffs','scorecard',
     'graph','graph_replay','visualization_html','log')),
  uri        text NOT NULL,
  bytes      bigint,
  sha256     text,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX artifacts_run ON artifacts (run_id, kind);
CREATE INDEX artifacts_recording ON artifacts (recording_id, kind);

CREATE TABLE audit_events (
  id          bigserial PRIMARY KEY,
  ts          timestamptz NOT NULL DEFAULT now(),
  actor       text NOT NULL,
  action      text NOT NULL,
  object_type text NOT NULL,
  object_id   text NOT NULL,
  params      jsonb NOT NULL DEFAULT '{}'
);
CREATE INDEX audit_object ON audit_events (object_type, object_id, ts);
CREATE INDEX audit_actor ON audit_events (actor, ts);

-- Append-only: even the owning role cannot UPDATE/DELETE through these rules.
CREATE RULE audit_no_update AS ON UPDATE TO audit_events DO INSTEAD NOTHING;
CREATE RULE audit_no_delete AS ON DELETE TO audit_events DO INSTEAD NOTHING;
