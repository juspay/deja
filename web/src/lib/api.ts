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

// What a run was asked to do, as persisted on the run row
// (`RunParams`, crates/deja-orchestrator/src/lib.rs). Values are RESOLVED: the
// correlation filter as normalized, the workload with its defaults applied, and
// the recording id as the concrete session once an s3_source prefix resolved
// one. `null` for rows created before the request was persisted — those carry
// `{"workload": null}` and no request at all.
export type RunParams = {
  mode: "record" | "replay";
  candidate_spec: CandidateSpecRow;
  /** Which system the run drives. Absent means the deployment's DEFAULT system,
   *  which this app deliberately does not name: the set of systems and which of
   *  them is default are the orchestrator's to state, and it states them at
   *  `/api/v1/systems`. Hard-coding a name here made adding a system a change in
   *  two repositories and two languages. */
  system_under_test?: string;
  candidate_repo?: string;
  recording_id: string | null;
  s3_source?: { path: string; region?: string; endpoint?: string };
  /** Absent = the entire session was driven, which is a real answer. */
  correlation_filter?: string[];
  workload?: unknown;
  expectation?: string;
};

/** The request a row carries, or null when it predates the record. */
export function runParams(run: RunRow): RunParams | null {
  const p = run.params as Partial<RunParams> | null | undefined;
  return p && p.candidate_spec ? (p as RunParams) : null;
}

export type RunRow = {
  run_id: string;
  mode: "record" | "replay";
  recording_id: string | null;
  candidate: CandidateSpecRow;
  candidate_sha256: string | null;
  params: Partial<RunParams> & { [k: string]: unknown };
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

/**
 * What killing a run actually removed (`POST /runs/{id}/kill`).
 *
 * The server reports rather than claims: `job_deleted` is null when the Job was
 * already gone, which is SUCCESS and not a failure — the endpoint is idempotent.
 * `problems` holds what it could not remove, non-fatally, so one stuck pod never
 * blocks reclaiming the rest. A caller must render `problems` — a 200 with
 * problems is a partial kill, not a clean one.
 */
export type KillReport = {
  run_id: string;
  /** The Job name, when one was still there. Null = already gone. */
  job_deleted: string | null;
  /** Pods deleted directly — normally empty, since deleting the Job takes them. */
  pods_deleted: string[];
  /** Non-fatal problems. Never swallowed. */
  problems: string[];
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
    // BLOCKING omissions/novel calls — what the verdict acts on, and a fold of
    // per_boundary's `OmittedCall` / `NovelCall`. The `_tolerated` counterparts
    // are the rest: uncorrelated background work and non-blocking boundaries.
    // The `/calls` ledger's `omitted` rows are the two added together, so a
    // count taken there will not match the headline unless it filters
    // `blocking`.
    omitted_calls?: number;
    omitted_calls_tolerated?: number;
    novel_calls?: number;
    novel_calls_tolerated?: number;
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
    // The scored-span shape section: present only when the run declared
    // `scored_span_namespaces` and this correlation carries namespaced spans.
    span_shape?: CorrelationSpanShape;
  }[];
};

/** One scored span occurrence from the span-shape check (see the scorer's
 * `span_shape` module): the run's declared instrumentation contract, compared
 * recorded-vs-replayed outside the event-bearing skeleton. */
export type SpanShapeOutcome = {
  path: string;
  span_name: string;
  k: number;
  status: "matched" | "missing" | "novel" | "field_diverged";
  record_node_id?: number;
  replay_node_id?: number;
  field_diffs?: { key: string; recorded?: unknown; replayed?: unknown }[];
};

export type CorrelationSpanShape = {
  matched: number;
  missing: number;
  novel: number;
  field_diverged: number;
  outcomes: SpanShapeOutcome[];
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

export type RunGraph = {
  record: GraphNode[];
  replay: GraphNode[];
  /** Why the record side is empty, when it is empty for a stated reason (the
   * tape refused to scope) rather than because the run had no cascade. */
  record_note?: string | null;
};

export function actor(): string {
  return localStorage.getItem("deja-actor") || "";
}

export function setActor(name: string) {
  localStorage.setItem("deja-actor", name);
}

/**
 * A failed request, carrying the HTTP status alongside the server's message.
 *
 * It is a plain `Error` with a field, not a subclass: every existing consumer
 * renders `String(error)`, and a subclass would change that string everywhere by
 * changing `name`. `status` lets a caller tell "no such thing" (404) from "could
 * not look" (5xx / network) — a distinction this app treats as load-bearing.
 */
export type ApiError = Error & { status?: number };

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
    const err: ApiError = new Error(detail);
    err.status = resp.status;
    throw err;
  }
  // AN UNROUTED /api/v1 PATH IS NOT A 404 HERE. The SPA fallback owns every URL
  // the API router does not claim, and it answers index.html with 200 — so an
  // endpoint this build knows about and the deployed orchestrator does not comes
  // back as HTML, and `resp.json()` would fail with "Unexpected token '<'".
  // Naming it is the difference between "this orchestrator has no such endpoint"
  // and an unreadable parse error. No `status` is set: the route being absent
  // says nothing about the thing that was asked for.
  const ctype = resp.headers.get("content-type") ?? "";
  if (!ctype.includes("json")) {
    throw new Error(
      `${path} is not served by this orchestrator (answered ${ctype || "no content-type"}, not JSON)`,
    );
  }
  return (await resp.json()) as T;
}

/** One system this orchestrator can replay, as it resolved from the deployment.
 *
 * Every field here is something the environment declared. `configured` false
 * means the system is named but cannot run — reported rather than hidden,
 * because a missing row is indistinguishable from a system nobody has heard of.
 * `error` is set when a declaration could not be parsed, and such a system must
 * not be offered as a choice. */
export type SystemRow = {
  name: string;
  is_default: boolean;
  configured: boolean;
  s3_bucket?: string | null;
  recording_root?: string | null;
  manages_stores: boolean;
  manages_stores_declared?: boolean | null;
  has_code_bundle: boolean;
  job_template_key?: string | null;
  candidate_image_repo?: string | null;
  instance_pattern?: string | null;
  scored_span_namespaces: string[];
  /** Slot name → the env var the candidate reads it from, derived from
   *  candidate_env_prefix or overridden per slot. */
  candidate_env?: Record<string, string>;
  candidate_config_files?: string[] | null;
  code_bundle_uri_env?: string | null;
  warnings?: string[];
  error?: string | null;
};

export const api = {
  systems: () => request<{ systems: SystemRow[] }>("/api/v1/systems"),
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

  /**
   * Stop a run and reclaim its pod. Mutating, so it carries the actor header
   * like `createRun`.
   *
   * Fails with 400 "kill is only supported for the k8s executor" on a compose
   * deployment. That is a real answer and is shown as one — the caller must not
   * present a refusal as a kill.
   */
  killRun: (id: string) => {
    const who = actor();
    if (!who) throw new Error("set your actor name first (top right)");
    return request<KillReport>(`/api/v1/runs/${id}/kill`, {
      method: "POST",
      headers: { "X-Deja-Actor": who },
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
  /** Short git sha of the recorded system. (hyperswitch `rec-…` shape) */
  revision?: string;
  /** `MMDDhhmm` UTC — when recording began. No year. */
  recorded_at?: string;
  /** Discriminator, so two pods starting in the same minute stay distinct. */
  instance?: string;
  /** Nanoseconds since epoch at recorder boot (the UCS `run-<nanos>` shape). */
  booted_at_nanos?: string;
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
  /** Which recorded system minted the session. Derived from the `inst=` pod
   *  names when the scan captured any (authoritative); the id shape decides
   *  only the unambiguous rec-<sha>-… case. Null = genuinely unknown — the
   *  run-<nanos> shape alone is ambiguous (old router recorders minted it
   *  too), and a wrong badge once sent a router tape into a prism replay. */
  system?: string | null;
  /** The bucket the session was found in. With a `?system=` scoped listing
   *  this differs from the default bucket, and a replay needs it to build
   *  `s3_source` (`s3://{bucket}/{prefix}`). */
  bucket?: string;
  /** `inst=` discriminators under the session — for a UCS session this is the
   *  recorder's pod name, the only identity its id does not carry. */
  instances?: string[];
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
export const availableRecordings = (limit = 200, offset = 0, system?: string) =>
  request<AvailableRecordingsPage>(
    `/api/v1/recordings/available?limit=${limit}&offset=${offset}` +
      (system ? `&system=${encodeURIComponent(system)}` : ""),
  );

// ===========================================================================
// THE CORRELATIONS INDEX — appended, so nothing above moves.
//
// `GET /api/v1/recordings/{id}/correlations` answers "which test cases does
// this recording hold", from the sealed index (manifest + sidecar) rather than
// from the tape. It is the ONLY endpoint that can name a recording's
// correlations; the catalog carries a count and no ids, and the bucket listing
// carries neither.
// ===========================================================================

/**
 * The recording's correlation ids, in the recording's own order.
 *
 * THREE ANSWERS, and they must stay three. A recording can be sealed (the index
 * is final and `correlations` is authoritative), present but not yet sealed (the
 * manifest is written last, so its absence IS "not sealed" — the ids are not
 * knowable cheaply and `correlations` is empty), or unknown (404). The middle
 * one is not "no correlations" and must never be rendered as one.
 *
 * `status` mirrors `SessionManifest.status`, whose sealed value is the literal
 * `"sealed"`. `sealed` is accepted as well because the ingest path spells the
 * same fact as a bool (`s3::IngestReport.sealed`); either is enough.
 *
 * ORDER IS THE SERVER'S. Correlation ids are time-ordered, so the index's own
 * ascending order is arrival order — the first entry is the earliest request.
 * A client must not re-sort: it could only disagree with the index.
 */
export type RecordingCorrelations = {
  recording_id: string;
  /** `"sealed"` when the index is final. */
  status?: string;
  /** The same fact as a bool, if the server spells it that way. */
  sealed?: boolean;
  /** How many the recording holds. Null when that is not known yet. */
  total: number | null;
  /** A PAGE of the index, earliest first. Empty when the recording is unsealed. */
  correlations: string[];
};

/**
 * Page the correlations index, from the start.
 *
 * The page exists because the index is not always small — the live catalog has
 * recordings holding 42,310 and 170,568 correlations, and shipping 170k ids to a
 * browser to populate a picker would be absurd. Offset 0 with the server's order
 * means the head of the page is the head of the index, which is what makes "the
 * first N" answerable without reading the whole thing.
 */
export const recordingCorrelations = (id: string, limit = 1000, offset = 0) =>
  request<RecordingCorrelations>(
    `/api/v1/recordings/${encodeURIComponent(id)}/correlations?limit=${limit}&offset=${offset}`,
  );
