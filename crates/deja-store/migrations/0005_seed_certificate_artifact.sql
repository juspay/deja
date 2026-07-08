-- Phase D: replay seeding now emits a seed/readback certificate artifact so
-- failed materialization is visible instead of only best-effort stderr.
ALTER TABLE artifacts DROP CONSTRAINT artifacts_kind_check;
ALTER TABLE artifacts ADD CONSTRAINT artifacts_kind_check CHECK (kind IN
  ('events','lookup_table','observed','http_diffs','scorecard',
   'graph','graph_replay','visualization_html','log','ingest_report',
   'manifest','call_ledger','seed_certificate'));
