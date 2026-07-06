-- Phase 2.3: the compactor seals sessions with a manifest; the catalog row
-- carries it (coverage/seal badges in the UI), and the local manifest copy
-- registers as an artifact.
ALTER TABLE recordings ADD COLUMN manifest jsonb;
ALTER TABLE artifacts DROP CONSTRAINT artifacts_kind_check;
ALTER TABLE artifacts ADD CONSTRAINT artifacts_kind_check CHECK (kind IN
  ('events','lookup_table','observed','http_diffs','scorecard',
   'graph','graph_replay','visualization_html','log','ingest_report',
   'manifest'));
