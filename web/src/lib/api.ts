// Thin API client. Every mutating request carries X-Deja-Actor (decision 8:
// auth-light but audit-ready); the actor name persists in localStorage.

export type RunRow = {
  run_id: string;
  mode: "record" | "replay";
  recording_id: string | null;
  candidate: Record<string, unknown>;
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
    resolved_by_rank: Record<string, number>;
  };
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
  logical_span_path?: string;
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

export function serviceToken(): string {
  return localStorage.getItem("deja-service-token") || "";
}

export function setServiceToken(token: string) {
  localStorage.setItem("deja-service-token", token);
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

function mutationHeaders(): Record<string, string> {
  const who = actor();
  if (!who) throw new Error("set your actor name first (top right)");
  const token = serviceToken();
  const headers: Record<string, string> = {
    "content-type": "application/json",
    "X-Deja-Actor": who,
  };
  if (token) headers.Authorization = `Bearer ${token}`;
  return headers;
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
  scorecard: (id: string) => request<Scorecard>(`/api/v1/runs/${id}/scorecard`),
  calls: (id: string) => request<CallRecord[]>(`/api/v1/runs/${id}/calls`),
  httpDiffs: (id: string) => request<HttpDiff[]>(`/api/v1/runs/${id}/http-diffs`),
  graph: (id: string) => request<RunGraph>(`/api/v1/runs/${id}/graph`),
  audit: () => request<AuditRow[]>("/api/v1/audit"),

  createRun: (spec: Record<string, unknown>) => {
    return request<{ run_id: string; status: string }>("/api/v1/runs", {
      method: "POST",
      headers: mutationHeaders(),
      body: JSON.stringify(spec),
    });
  },
  killRun: (id: string) =>
    request<{
      ok: boolean;
      run_id: string;
      namespace: string;
      terminated: boolean;
      message: string;
      details: string[];
    }>(`/api/v1/runs/${id}/kill`, {
      method: "POST",
      headers: mutationHeaders(),
    }),
};
