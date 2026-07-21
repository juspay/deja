-- The in-pod runner emits the recorded run's execution-graph STRUCTURE (span
-- nodes only — never the boundary payloads) as a `record_graph` artifact, so
-- the dashboard's /graph record side renders for k8s runs without the recording
-- tape ever reaching the orchestrator. Monotonic superset of 0005.
ALTER TABLE artifacts DROP CONSTRAINT artifacts_kind_check;
ALTER TABLE artifacts ADD CONSTRAINT artifacts_kind_check CHECK (kind IN
  ('events','lookup_table','observed','http_diffs','scorecard',
   'graph','graph_replay','visualization_html','log','ingest_report',
   'manifest','call_ledger','seed_certificate','record_graph'));
