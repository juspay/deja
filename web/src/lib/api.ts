// Thin API client. Every mutating request carries X-Deja-Actor (decision 8:
// auth-light but audit-ready); the actor name persists in localStorage.

// The serialized `CandidateSpec` enum (crates/deja-orchestrator/src/lib.rs:28).
// Live runs carry 28 `prebuilt_image` and 1 `repo_sha`; the other three arms are
// declared server-side and unreached so far.
export type CandidateSpecRow =
  | { kind: "prebuilt_image"; image: string }
  | { kind: "local_path"; binary_or_source: string }
  | { kind: "repo_sha"; repo: string; sha: string }
  | { kind: "repo_branch"; repo: string; branch: string }
  | { kind: "repo_pr"; repo: string; pr: number }
  | { kind: string; [k: string]: unknown };

export type RunRow = {
  run_id: string;
  mode: "record" | "replay";
  recording_id: string | null;
  candidate: CandidateSpecRow;
  candidate_sha256: string | null;
  // Serialized RunSpec params persisted by the API.
  params: { workload?: unknown; [k: string]: unknown };
  state: string;
  verdict: "pass" | "fail" | "inconclusive" | null;
  scorecard: Scorecard | null;
  failure: { message?: string } | null;
  expectation: string | null;
  created_by: string;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  live?: {
    status: string;
    stage: string | null;
    step: number;
    steps_total: number;
    stage_updated_ms: number;
    failure_reason: string | null;
    candidate_image: { docker_image: string; source_ref: string } | null;
  };
};

export type SessionManifest = {
  status: string;
  counts: {
    landing_objects: number;
    lines_in: number;
    events: number;
    duplicates_dropped: number;
    correlations: number;
  };
  instances: { instance_id: string; gaps: [number, number][]; duplicates_dropped: number }[];
  code: { sha: string | null; deja_version: string | null }[];
};

export type RecordingRow = {
  recording_id: string;
  kind: string;
  source_path: string | null;
  event_count: number | null;
  correlation_count: number | null;
  byte_size: number | null;
  status: string;
  created_by: string;
  created_at: string;
  manifest: SessionManifest | null;
};

export type StageRow = {
  id: number;
  stage: string;
  status: "running" | "ok" | "failed";
  step: number | null;
  steps_total: number | null;
  started_at: string;
  finished_at: string | null;
};

export type ArtifactRow = {
  id: number;
  run_id: string | null;
  recording_id: string | null;
  kind: string;
  uri: string;
  bytes: number | null;
  created_at: string;
};

export type AuditRow = {
  id: number;
  ts: string;
  actor: string;
  action: string;
  object_type: string;
  object_id: string;
  params: Record<string, unknown>;
};

export type Scorecard = {
  verdict: { pass: boolean; inconclusive: boolean; reason: string };
  summary: {
    matched_correlations: number;
    total_correlations: number;
    http_status_mismatches: number;
    http_body_mismatches: number;
    side_effect_divergences: number;
    omitted_calls?: number;
    novel_calls?: number;
    // A non-zero value_divergences means an Execute boundary ran during replay
    // and produced a value differing from the recorded baseline. Lookup /
    // Substitute boundaries serve recorded values.
    value_divergences?: number;
    inconclusive_seed_gaps?: number;
    environmental_misses?: number;
    recovered_rank5_calls?: number;
    matched_side_effect_calls?: number;
    resolved_by_rank: Record<string, number>;
  };
  // The correlations this run was scoped to. On the live recording that is 3 of
  // 42,310 — a fact the trust strip states rather than leaving the reader to
  // read "0/3 matched" as "0 of everything".
  correlation_scope?: string[];
  // The scorer's own caveats, e.g. "correlation scope: 3 id(s) driven; excluded
  // 42307 recorded events outside the subset". Rendered nowhere before this.
  warnings?: string[];
  per_boundary?: Record<
    string,
    {
      matched?: number;
      diverged?: number;
      tier?: string;
      kinds?: Record<string, number>;
      [k: string]: unknown;
    }
  >;
  per_correlation?: {
    correlation_id: string;
    passed?: boolean;
    http_status_match?: boolean;
    http_body_match?: boolean;
    side_effect_divergences?: number;
  }[];
};

// One side (recorded or observed) of a reconciled boundary call.
export type CallSide = {
  args?: unknown;
  result?: unknown;
  is_error?: boolean;
  call_file?: string;
  call_line?: number;
  call_column?: number;
  // The wire name. `logical_span_path` was never emitted by any producer:
  // measured 535 occurrences of `span_path` and 0 of `logical_span_path` in the
  // live /calls payload for run-18c89f82e67728b9.
  span_path?: string;
  graph_node_id?: number;
};

// A reconciled side-effect call: identity + classification + both sides.
export type CallRecord = {
  correlation_id?: string;
  source_event_global_sequence?: number;
  boundary: string;
  trait_name: string;
  method_name: string;
  // matched | recovered | novel | omitted | environmental | deterministic |
  // value_diverged
  kind: string;
  blocking: boolean;
  // For a value_diverged row: true on the ORIGIN (executed read whose real value
  // differed — the cause), false on the CONSEQUENCE (downstream write). Absent
  // on every other kind.
  origin?: boolean;
  resolved_rank?: number;
  recorded?: CallSide;
  observed?: CallSide;
};

export type JsonFieldDiff = { json_path: string; baseline: unknown; candidate: unknown };

export type HttpDiff = {
  correlation_id: string;
  request_sequence: number;
  request_path: string;
  status_baseline: number;
  status_candidate: number;
  status_match: boolean;
  body_diff: JsonFieldDiff[];
  // Full recorded + replayed bodies (present once the kernel persists them) —
  // enables a true side-by-side before/after with unchanged context.
  baseline_body?: unknown;
  candidate_body?: unknown;
};

// Raw execution-graph node (record or replay side).
export type GraphNode = {
  node_id: number;
  parent_id: number | null;
  causal_parent_ids: number[];
  sequence: number;
  span_name: string;
  target: string;
  level: string;
  fields: Record<string, unknown>;
  started_ns: number;
  closed_ns: number | null;
};

export type RunGraph = { record: GraphNode[]; replay: GraphNode[] };

export function actor(): string {
  return localStorage.getItem("deja-actor") || "";
}

export function setActor(name: string) {
  localStorage.setItem("deja-actor", name);
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const resp = await fetch(path, init);
  if (!resp.ok) {
    let detail = `${resp.status}`;
    try {
      const body = (await resp.json()) as { error?: string };
      if (body.error) detail = body.error;
    } catch {
      /* non-JSON error */
    }
    throw new Error(detail);
  }
  return (await resp.json()) as T;
}

export const api = {
  recordings: () => request<RecordingRow[]>("/api/v1/recordings"),
  runs: () => request<RunRow[]>("/api/v1/runs"),
  run: (id: string) => request<RunRow>(`/api/v1/runs/${id}`),
  stages: (id: string) => request<StageRow[]>(`/api/v1/runs/${id}/stages`),
  logs: (id: string, afterSeq = -1) =>
    request<{ stage: string; seq: number; lines: string }[]>(
      `/api/v1/runs/${id}/logs?after_seq=${afterSeq}`,
    ),
  artifacts: (id: string) => request<ArtifactRow[]>(`/api/v1/runs/${id}/artifacts`),
  // DO NOT use this to decide a run's result — use `resultOf(run)` over the
  // scorecard the run row embeds. This endpoint SYNTHESISES a scorecard when no
  // artifact exists: on a run that died in stage 1 it returns 200 with
  // `correlation_scope: [3 ids]`, `total_correlations: 0` and
  // `inconclusive: true`, which reads as "3 correlations matched 0 of 42,310".
  // No consumer in this app today; kept as raw API surface.
  scorecard: (id: string) => request<Scorecard>(`/api/v1/runs/${id}/scorecard`),
  calls: (id: string) => request<CallRecord[]>(`/api/v1/runs/${id}/calls`),
  httpDiffs: (id: string) => request<HttpDiff[]>(`/api/v1/runs/${id}/http-diffs`),
  graph: (id: string) => request<RunGraph>(`/api/v1/runs/${id}/graph`),
  audit: () => request<AuditRow[]>("/api/v1/audit"),

  createRun: (spec: Record<string, unknown>) => {
    const who = actor();
    if (!who) throw new Error("set your actor name first (top right)");
    return request<{ run_id: string; status: string }>("/api/v1/runs", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "X-Deja-Actor": who,
      },
      body: JSON.stringify(spec),
    });
  },
};

// ===========================================================================
// THE BUCKET INDEX — appended, so nothing above moves.
//
// `GET /api/v1/recordings` above lists the CATALOG: recordings that have been
// pulled, which is a property of what has been replayed rather than of what
// exists. A recording made an hour ago is absent from it until something drives
// it. `GET /api/v1/recordings/available` lists the landing area itself. Live,
// the first returns 2 and the second returns 7.
// ===========================================================================

/**
 * What a recording's id claims about its own provenance, parsed server-side so
 * two readers cannot disagree about the shape.
 *
 * Null on every recording made before ids carried a revision — the normal past,
 * not an error. When present the facts also live authoritatively in the
 * recording's envelopes (`code.sha`, `instance_id`).
 */
export type RecordingIdentity = {
  /** Short git sha of the recorded system. */
  revision: string;
  /** `MMDDhhmm` UTC — when recording began. No year. */
  recorded_at: string;
  /** Discriminator, so two pods starting in the same minute stay distinct. */
  instance: string;
};

/** One session found in the bucket. */
export type AvailableRecording = {
  /** The session id, minted ONCE PER ROUTER PROCESS — so this names a pod's
   *  entire lifetime, not a bounded window of traffic. */
  recording_id: string;
  /** Daily partitions holding objects, sorted ascending. */
  dates: string[];
  latest_date: string | null;
  /** LANDING OBJECTS — gzipped envelope batches, not requests. */
  objects: number;
  /** Whether the catalog already holds it. Degrades to false (never an error)
   *  when the catalog itself cannot be read, so the bucket stays visible. */
  pulled: boolean;
  identity: RecordingIdentity | null;
  /** The prefix the orchestrator would ingest from — reported so a run can be
   *  reproduced by hand, NOT so a caller has to supply it. Bucket-relative: a
   *  `s3_source.path` needs `bucket/` in front of it. */
  prefix: string;
};

export type AvailableRecordingsPage = {
  recordings: AvailableRecording[];
  /** Sessions in the bucket, before paging — compare against the page length. */
  total: number;
  offset: number;
  limit: number;
};

/**
 * List the bucket, newest first (by last partition, then by id, so the order is
 * total). The server clamps `limit` to 1..200.
 *
 * This LISTS S3, so it is slow (~1.8s against the live bucket) and it can fail
 * with a 502 — which a caller must render as a failure to look, never as an
 * empty list, because an empty list reads as "no recordings exist".
 */
export const availableRecordings = (limit = 200, offset = 0) =>
  request<AvailableRecordingsPage>(
    `/api/v1/recordings/available?limit=${limit}&offset=${offset}`,
  );
