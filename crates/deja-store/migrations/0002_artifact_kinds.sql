-- Phase 2.1: the native S3 pull registers its accounting (objects, lines,
-- duplicates dropped) as an `ingest_report` artifact.
ALTER TABLE artifacts DROP CONSTRAINT artifacts_kind_check;
ALTER TABLE artifacts ADD CONSTRAINT artifacts_kind_check CHECK (kind IN
  ('events','lookup_table','observed','http_diffs','scorecard',
   'graph','graph_replay','visualization_html','log','ingest_report'));
