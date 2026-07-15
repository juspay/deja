-- The replay agent now renders a self-contained HTML diff report
-- (diff-report.html) next to http-diffs/call-ledger; allow its kind so the
-- verdict callback can register it instead of silently skipping it.
ALTER TABLE artifacts DROP CONSTRAINT artifacts_kind_check;
ALTER TABLE artifacts ADD CONSTRAINT artifacts_kind_check CHECK (kind IN
  ('events','lookup_table','observed','http_diffs','scorecard',
   'graph','graph_replay','visualization_html','log','ingest_report',
   'manifest','call_ledger','seed_certificate','diff_report'));
