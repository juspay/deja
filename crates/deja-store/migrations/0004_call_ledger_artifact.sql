-- Phase 2.x: the divergence detector now persists a per-call ledger sidecar
-- (recorded-vs-observed detail) registered as an artifact for the diff UI.
ALTER TABLE artifacts DROP CONSTRAINT artifacts_kind_check;
ALTER TABLE artifacts ADD CONSTRAINT artifacts_kind_check CHECK (kind IN
  ('events','lookup_table','observed','http_diffs','scorecard',
   'graph','graph_replay','visualization_html','log','ingest_report',
   'manifest','call_ledger'));
