-- Incremental source-object/request index and immutable exact day views.
-- Scope is deliberately explicit: the same session identifier may exist in
-- different systems, buckets, or landing roots.
CREATE TABLE landing_sessions (
  system text NOT NULL,
  bucket text NOT NULL,
  root text NOT NULL,
  session_id text NOT NULL,
  generation bigint NOT NULL DEFAULT 0,
  source_incomplete boolean NOT NULL DEFAULT false,
  PRIMARY KEY (system, bucket, root, session_id)
);

CREATE TABLE landing_objects (
  system text NOT NULL,
  bucket text NOT NULL,
  root text NOT NULL,
  session_id text NOT NULL,
  object_key text NOT NULL,
  fingerprint text NOT NULL,
  indexed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (system, bucket, root, session_id, object_key),
  FOREIGN KEY (system, bucket, root, session_id)
    REFERENCES landing_sessions(system, bucket, root, session_id) ON DELETE CASCADE
);

CREATE TABLE landing_requests (
  system text NOT NULL,
  bucket text NOT NULL,
  root text NOT NULL,
  session_id text NOT NULL,
  correlation_id text NOT NULL,
  day date NOT NULL,
  PRIMARY KEY (system, bucket, root, session_id, correlation_id),
  FOREIGN KEY (system, bucket, root, session_id)
    REFERENCES landing_sessions(system, bucket, root, session_id) ON DELETE CASCADE
);
CREATE INDEX landing_requests_by_day
  ON landing_requests (system, bucket, root, session_id, day, correlation_id);

CREATE TABLE landing_object_requests (
  system text NOT NULL,
  bucket text NOT NULL,
  root text NOT NULL,
  session_id text NOT NULL,
  object_key text NOT NULL,
  correlation_id text NOT NULL,
  PRIMARY KEY (system, bucket, root, session_id, object_key, correlation_id),
  FOREIGN KEY (system, bucket, root, session_id, object_key)
    REFERENCES landing_objects(system, bucket, root, session_id, object_key) ON DELETE CASCADE
);
CREATE INDEX landing_object_requests_by_request
  ON landing_object_requests (system, bucket, root, session_id, correlation_id, object_key);
CREATE TABLE landing_request_ingresses (
  system text NOT NULL,
  bucket text NOT NULL,
  root text NOT NULL,
  session_id text NOT NULL,
  object_key text NOT NULL,
  correlation_id text NOT NULL,
  PRIMARY KEY (system, bucket, root, session_id, object_key, correlation_id),
  FOREIGN KEY (system, bucket, root, session_id, object_key)
    REFERENCES landing_objects(system, bucket, root, session_id, object_key) ON DELETE CASCADE
);
CREATE INDEX landing_request_ingresses_by_request
  ON landing_request_ingresses (system, bucket, root, session_id, correlation_id);


CREATE TABLE landing_object_days (
  system text NOT NULL,
  bucket text NOT NULL,
  root text NOT NULL,
  session_id text NOT NULL,
  object_key text NOT NULL,
  day date NOT NULL,
  PRIMARY KEY (system, bucket, root, session_id, object_key, day),
  FOREIGN KEY (system, bucket, root, session_id, object_key)
    REFERENCES landing_objects(system, bucket, root, session_id, object_key) ON DELETE CASCADE
);
CREATE INDEX landing_object_days_by_day
  ON landing_object_days (system, bucket, root, session_id, day, object_key);

CREATE TABLE day_views (
  id bigserial PRIMARY KEY,
  system text NOT NULL,
  bucket text NOT NULL,
  root text NOT NULL,
  session_id text NOT NULL,
  day date NOT NULL,
  generation bigint NOT NULL,
  dependency_token text NOT NULL,
  manifest jsonb NOT NULL,
  objects jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (system, bucket, root, session_id, day, dependency_token)
);
CREATE TABLE day_view_heads (
  system text NOT NULL,
  bucket text NOT NULL,
  root text NOT NULL,
  session_id text NOT NULL,
  day date NOT NULL,
  view_id bigint NOT NULL REFERENCES day_views(id),
  generation bigint NOT NULL,
  dependency_token text NOT NULL,
  PRIMARY KEY (system, bucket, root, session_id, day)
);
