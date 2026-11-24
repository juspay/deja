import type { CallRecord, ClauseSource, Delta, DeltaBucket, DeltaFamily, DeltaRow, HttpBodyClassification, HttpComparison, HttpDiff, JsonFieldDiff, Scorecard, ValueAbsorption } from "./api";
import { deltaFamily } from "./api";
import { capturedResponseFieldValue, present, responseFieldValue, unavailable, type EvidenceField } from "./fieldEvidence";

/** Keys identify published responses; they do not compare captured values. */
export function responseKey(correlation: string, sequence: number): string {
  return JSON.stringify([correlation, sequence]);
}

export type HttpJudgment =
  | { kind: "published"; body: HttpBodyClassification }
  | { kind: "historical"; accepted?: boolean }
  | { kind: "unavailable"; reason: string };

/** Validate publication shape, never the meaning of captured values. */
function validBody(body: HttpBodyClassification): boolean {
  if (!body || !Array.isArray(body.blocking)) return false;
  if (!Number.isSafeInteger(body.blocking_leaf_count) || body.blocking_leaf_count < 0 || body.blocking_leaf_count !== body.blocking.length) return false;
  const paths = new Set<string>();
  for (const field of body.blocking) {
    if (!field || typeof field.json_path !== "string" || !field.json_path || paths.has(field.json_path)
      || typeof field.candidate_identity !== "string" || !field.candidate_identity.trim()
      || !Object.prototype.hasOwnProperty.call(field, "baseline") || !Object.prototype.hasOwnProperty.call(field, "candidate")) return false;
    for (const side of ["baseline", "candidate"] as const) {
      const presence = field[`${side}_presence`];
      if (presence !== undefined && !["present", "missing", "unavailable"].includes(presence)) return false;
      if (presence === "missing" && field[side] !== null) return false;
    }
    paths.add(field.json_path);
  }
  const strings = (values: unknown) => Array.isArray(values) && values.every((value) => typeof value === "string");
  if (![body.schema_derived_paths, body.order_only_paths, body.canon_conflicts, body.kernel_equivalent_paths, body.embedded_document_paths, body.document_text_paths].every(strings)) return false;
  if ([body.race_absorbed_paths, body.hidden_form_paths].some((values) => values !== undefined && !strings(values))) return false;
  return Array.isArray(body.canon_absorbed) && body.canon_absorbed.every((entry) => Array.isArray(entry) && entry.length === 2 && typeof entry[0] === "string" && ["default", "recorder", "document", "both"].includes(entry[1]));
}

type PreparedComparison =
  | { kind: "indexed"; responses: ReadonlyMap<string, HttpBodyClassification> }
  | Extract<HttpJudgment, { kind: "unavailable" }>;

// API publications are immutable React Query JSON snapshots. Cache validation
// and indexing together, including rejection; a replacement object is checked
// afresh. Weak keys let discarded query snapshots be collected.
const preparedComparisons = new WeakMap<HttpComparison, PreparedComparison>();
const historicalCorrelations = new WeakMap<NonNullable<Scorecard["per_correlation"]>, ReadonlyMap<string, boolean | undefined>>();

function prepareComparison(published: HttpComparison): PreparedComparison {
  const cached = preparedComparisons.get(published);
  if (cached) return cached;
  const reject = (reason: string): PreparedComparison => {
    const result = { kind: "unavailable" as const, reason };
    preparedComparisons.set(published, result);
    return result;
  };
  const version = published.version;
  if (version !== 1) return reject(`Response comparison version ${version} is not supported by this dashboard.`);
  const rows = published.responses;
  const inconsistent = "Published response comparison details are inconsistent or incomplete.";
  if (!Array.isArray(rows)) return reject(inconsistent);
  const responses = new Map<string, HttpBodyClassification>();
  for (const response of rows) {
    if (!response) return reject(inconsistent);
    const { correlation_id, request_sequence, body } = response;
    if (typeof correlation_id !== "string" || !Number.isSafeInteger(request_sequence) || request_sequence < 0 || !validBody(body)) return reject(inconsistent);
    const unavailable = body.unavailable_reason;
    if (unavailable !== undefined) {
      if (typeof unavailable !== "string") return reject(inconsistent);
      return reject(unavailable.trim() ? `Response comparison unavailable: ${unavailable}` : "The scorer marked response comparison unavailable.");
    }
    const key = responseKey(correlation_id, request_sequence);
    if (responses.has(key)) return reject(inconsistent);
    responses.set(key, body);
  }
  const result = { kind: "indexed" as const, responses };
  preparedComparisons.set(published, result);
  return result;
}

function historicalAcceptance(rows: Scorecard["per_correlation"], correlation: string): boolean | undefined {
  if (!rows) return undefined;
  let index = historicalCorrelations.get(rows);
  if (!index) {
    const prepared = new Map<string, boolean | undefined>();
    for (const row of rows) {
      // Preserve the former Array.find behavior if a historical payload carries
      // duplicate correlations, including an undefined first judgment.
      if (!prepared.has(row.correlation_id)) prepared.set(row.correlation_id, row.http_body_match);
    }
    historicalCorrelations.set(rows, prepared);
    index = prepared;
  }
  return index.get(correlation);
}

export function httpJudgment(scorecard: Scorecard | null | undefined, diff: HttpDiff): HttpJudgment {
  const published = scorecard?.http_comparison;
  if (published) {
    if (typeof published !== "object") return { kind: "unavailable", reason: "Published response comparison details are inconsistent or incomplete." };
    const prepared = prepareComparison(published);
    if (prepared.kind === "unavailable") return prepared;
    const body = prepared.responses.get(responseKey(diff.correlation_id, diff.request_sequence));
    return body
      ? { kind: "published", body }
      : { kind: "unavailable", reason: "The scorer did not publish comparison details for this response." };
  }
  return { kind: "historical", accepted: historicalAcceptance(scorecard?.per_correlation, diff.correlation_id) };
}

/** Default findings follow the scorer; raw differences are only a legacy view. */
export function responseFinding(scorecard: Scorecard | null | undefined, diff: HttpDiff): { label: string; unavailable?: boolean } | null {
  const judgment = httpJudgment(scorecard, diff);
  if (!diff.status_match) return { label: `${diff.status_baseline} → ${diff.status_candidate}` };
  if (judgment.kind === "unavailable") return { label: judgment.reason, unavailable: true };
  if (judgment.kind === "published") {
    const n = judgment.body.blocking.length;
    return n ? { label: `${n} blocking response difference${n === 1 ? "" : "s"}` } : null;
  }
  if (judgment.accepted === true || diff.body_diff.length === 0) return null;
  return { label: `historical comparison · ${diff.body_diff.length} raw field difference${diff.body_diff.length === 1 ? "" : "s"}; classified details unavailable` };
}

export function responseMark(scorecard: Scorecard | null | undefined, diff: HttpDiff): "matched" | "response" | "unknown" {
  const finding = responseFinding(scorecard, diff);
  return finding?.unavailable ? "unknown" : finding ? "response" : "matched";
}

export function sourceLabel(source: ClauseSource): string {
  return source === "both" ? "recorder and system declaration" : source === "document" ? "system declaration" : source;
}

export function acceptedBodyReasons(body: HttpBodyClassification): { path: string; reason: string }[] {
  return [
    ...body.canon_absorbed.map(([path, source]) => ({ path, reason: `ordering accepted by ${sourceLabel(source)}` })),
    ...body.schema_derived_paths.map((path) => ({ path, reason: "schema-derived value accepted" })),
    ...body.kernel_equivalent_paths.map((path) => ({ path, reason: "accepted by the kernel comparison" })),
    ...body.embedded_document_paths.map((path) => ({ path, reason: "compared as embedded documents" })),
    ...body.document_text_paths.map((path) => ({ path, reason: "document rendering accepted" })),
    ...(body.race_absorbed_paths ?? []).map((path) => ({ path, reason: "race-attributed difference set aside by the scorer" })),
    ...(body.hidden_form_paths ?? []).map((path) => ({ path, reason: "hidden form comparison accepted" })),
  ];
}

function absorptionLabel(reason: ValueAbsorption): string {
  switch (reason.kind) {
    case "args_identity": return "same semantic argument identity";
    case "approved_noise": return "accepted by the run's maintainer policy";
    case "db_infrastructure": return "replay database infrastructure difference";
    case "served_recorded_error": return "served the recorded error";
    case "canon": return `accepted by ${reason.source} canonicalization`;
    case "document_text": return `document rendering accepted${reason.source ? ` by ${reason.source}` : ""}`;
    default: return "the scorer's published allowance";
  }
}

export function callComparisonText(call: CallRecord): string {
  const comparison = call.comparison;
  if (!comparison) return "Comparison details were not published for this historical call.";
  if (comparison.version !== 1) return `Call comparison version ${comparison.version} is not supported by this dashboard.`;
  const subject = comparison.subject === "arguments" ? "Arguments" : comparison.subject === "result" ? "Result" : null;
  if (!subject) return "The published comparison subject is not supported by this dashboard.";
  switch (comparison.verdict.kind) {
    case "equal": return `${subject} compared equal by the scorer.`;
    case "absorbed": return `${subject} accepted by the scorer: ${absorptionLabel(comparison.verdict.reason)}.`;
    case "diverged": return `${subject} ${comparison.subject === "result" ? "differs" : "differ"} under the scorer's comparison.`;
    default: return "The published call verdict is not supported by this dashboard.";
  }
}

/** Runtime observations never follow from scorer categories or origin flags. */
export function callRuntimeText(call: CallRecord): string {
  const runtime = call.observed?.runtime;
  if (!runtime) return "Runtime evidence is unavailable for this call; this response does not establish its outcome or value provenance.";
  if (runtime.version !== 1) return `Runtime observation version ${runtime.version} is not supported by this dashboard.`;
  if (runtime.cancelled) return "This replay call was cancelled; completed execution or substitution is not established.";
  if (runtime.provenance === "execute_shadow") return "The real boundary executed during replay (execute-shadow provenance).";
  if (runtime.outcome === "stopped") return "Replay stopped at this call without returning a substituted response.";
  if (runtime.provenance === "served_recorded_error") return "This call served its recorded error instead of executing the real boundary.";
  if (runtime.provenance === "recorded" && runtime.outcome === "substituted") return "Recorded response substituted. Its source is independent of the argument comparison.";
  if (runtime.provenance === "recorded" && runtime.outcome === "synthesized") return "The replay site synthesized a response; it did not come from the recording.";
  return "The published runtime outcome or provenance is unsupported; execution and substitution are not established.";
}

/** A transport fact, never a semantic provider identity. No path/query/credentials. */
export function callTransportHost(call: CallRecord): string | null {
  const args = call.observed?.args ?? call.recorded?.args;
  if (!args || typeof args !== "object" || !("url" in args) || typeof args.url !== "string") return null;
  try { const url = new URL(args.url); return ["https:", "http:"].includes(url.protocol) ? url.host : null; } catch { return null; }
}

export const SAME_AS_RECORDING = Symbol("same as recording");
export type FieldSide = unknown | typeof SAME_AS_RECORDING;
export type DeltaOutcome = "conflict" | "new" | "restored" | "same" | "clean";
export type DeltaField = { path: string; recording: unknown; main: FieldSide; pr: FieldSide; sameAsMain: boolean; bucket: DeltaBucket; evidence?: EvidenceField };
export type DeltaRequest = {
  correlation: string;
  sequence: number;
  method: string;
  lane: string;
  outcome: DeltaOutcome;
  status: [number, number | null, number];
  statusEvidence?: EvidenceField;
  fields: DeltaField[];
  notes: string[];
};

function outcomeOf(families: Set<DeltaFamily>): DeltaOutcome {
  if (families.has("changed")) return "conflict";
  if (families.has("introduced")) return "new";
  if (families.has("resolved")) return "restored";
  if (families.has("inherited")) return "same";
  return "clean";
}

/** Request counts come from published buckets, independent of HTTP row count. */
export function deltaRequestCounts(delta: Delta) {
  const families = new Map<string, Set<DeltaFamily>>();
  for (const row of delta.rows) {
    const set = families.get(row.address.correlation) ?? new Set<DeltaFamily>();
    set.add(deltaFamily(row.bucket));
    families.set(row.address.correlation, set);
  }
  const outcomes = [...families.values()].map(outcomeOf);
  const n = (outcome: DeltaOutcome) => outcome === "clean"
    ? Math.max(0, delta.covered_correlations - outcomes.filter((o) => o !== "clean").length)
    : outcomes.filter((o) => o === outcome).length;
  return { n, recVsMain: n("same") + n("conflict") + n("restored"), recVsPr: n("same") + n("conflict") + n("new"), mainVsPr: n("new") + n("conflict") + n("restored") };
}

/** Counts observations without treating informational rows as review requirements. */
export function deltaReviewCounts(delta: Delta) {
  const specific = delta.rows.filter((row) => ["introduced", "changed"].includes(deltaFamily(row.bucket)));
  const blocking = specific.filter((row) => row.blocking);
  return {
    blocking: blocking.length,
    informational: specific.length - blocking.length,
    affectedRequests: new Set(specific.map((row) => row.address.correlation)).size,
    blockingRequests: new Set(blocking.map((row) => row.address.correlation)).size,
    shared: delta.rows.filter((row) => deltaFamily(row.bucket) === "inherited").length,
  };
}

function responseFields(mode: string, scorecard: Scorecard | null | undefined, diff: HttpDiff | undefined): { fields: JsonFieldDiff[]; note?: string } {
  if (!diff) return { fields: [], note: "This response's captured values are unavailable." };
  if (mode === "legacy") {
    if (diff.body_diff_address_error !== undefined) return { fields: [], note: `Response field addresses are unavailable: ${diff.body_diff_address_error || "the server could not safely address these rows."}` };
    if (diff.body_diff_address_version !== undefined && diff.body_diff_address_version !== 1) return { fields: [], note: `Response field address version ${diff.body_diff_address_version} is not supported by this dashboard.` };
    const paths = new Set<string>();
    for (const field of diff.body_diff) {
      if (paths.has(field.json_path)) return { fields: [], note: "Response field values are unavailable because raw diff paths are duplicated and ambiguous." };
      paths.add(field.json_path);
    }
    return { fields: diff.body_diff };
  }
  if (mode !== "classified") return { fields: [], note: `Response comparison mode ${mode} is not supported by this dashboard.` };
  const judgment = httpJudgment(scorecard, diff);
  return judgment.kind === "published"
    ? { fields: judgment.body.blocking }
    : { fields: [], note: judgment.kind === "unavailable" ? judgment.reason : "Classified response values were not published for this run." };
}

/** Only backend body addresses supply fields. Residue indices are never read
 * from the raw JSON body, and equal-looking values never decide a bucket. */
export function joinDeltaResponses(delta: Delta, prDiffs: HttpDiff[], mainDiffs: HttpDiff[], prScorecard?: Scorecard | null, mainScorecard?: Scorecard | null): DeltaRequest[] {
  const rowsBy = new Map<string, DeltaRow[]>();
  const laneBy = new Map(Object.entries(delta.request_lanes ?? {}).map(([c, l]) => [c, `${l.connector} · ${l.flow}`]));
  for (const row of delta.rows) {
    if (row.lane) laneBy.set(row.address.correlation, `${row.lane.connector} · ${row.lane.flow}`);
    // Call addresses have no response sequence. They are listed separately,
    // never attributed to every HTTP response in their correlation.
    if (row.address.kind === "call") continue;
    const key = responseKey(row.address.correlation, row.address.request_sequence ?? 0);
    rowsBy.set(key, [...(rowsBy.get(key) ?? []), row]);
  }
  const index = (diffs: HttpDiff[]) => {
    const by = new Map<string, { diff: HttpDiff; count: number }>();
    for (const diff of diffs) {
      const key = responseKey(diff.correlation_id, diff.request_sequence);
      const entry = by.get(key);
      if (entry) entry.count += 1;
      else by.set(key, { diff, count: 1 });
    }
    return by;
  };
  const mainBy = index(mainDiffs);
  const prBy = index(prDiffs);
  const mode = delta.http_comparison_mode ?? "legacy";
  return [...prBy.values()].map(({ diff, count }): DeltaRequest => {
    const key = responseKey(diff.correlation_id, diff.request_sequence);
    const rows = rowsBy.get(key) ?? [];
    const entry = mainBy.get(key);
    const main = entry?.count === 1 ? entry.diff : undefined;
    const p = responseFields(mode, prScorecard, count === 1 ? diff : undefined);
    const m = responseFields(mode, mainScorecard, main);
    const fields = rows.flatMap((row): DeltaField[] => {
      if (row.address.kind !== "body") return [];
      const path = row.address.json_path;
      const prField = p.fields.find((f) => f.json_path === path);
      const mainField = m.fields.find((f) => f.json_path === path);
      const side = (value: DeltaRow["m"], field: JsonFieldDiff | undefined): FieldSide => value === "tape" ? SAME_AS_RECORDING : field?.candidate;
      const recording = responseFieldValue(prField ?? mainField, "baseline", prField ? diff : main);
      const evidence: EvidenceField = {
        path,
        recording,
        main: row.m === "absent" ? unavailable("The baseline did not reach this field.") : mainField ? responseFieldValue(mainField, "candidate", main) : row.m === "tape" ? capturedResponseFieldValue(path, "candidate", main) : responseFieldValue(undefined, "candidate"),
        pr: row.y === "absent" ? unavailable("The candidate did not reach this field.") : prField ? responseFieldValue(prField, "candidate", diff) : row.y === "tape" && count === 1 ? capturedResponseFieldValue(path, "candidate", diff) : responseFieldValue(undefined, "candidate"),
      };
      return [{ path, recording: (prField ?? mainField)?.baseline, main: side(row.m, mainField), pr: side(row.y, prField), sameAsMain: row.bucket === "inherited" || row.bucket === "clean", bucket: row.bucket, evidence }];
    });
    const notes = [...new Set([p.note, m.note].filter((note): note is string => !!note))];
    if (count > 1 || (entry?.count ?? 0) > 1) notes.push("Duplicate response correlation/sequence addresses make captured evidence ambiguous; those values are unavailable.");
    if (mode === "legacy") notes.unshift("Historical response comparison: raw field values are shown for the backend's delta addresses.");
    if (fields.some((field) => field.main === undefined || field.pr === undefined)) notes.push("Some judged field values were not published; their delta classifications remain shown.");
    return {
      correlation: diff.correlation_id,
      sequence: diff.request_sequence,
      method: diff.request_path.split("/").pop() ?? diff.request_path,
      lane: laneBy.get(diff.correlation_id) ?? `no connector call · ${(diff.request_path.split("/").pop() ?? "").toLowerCase()}`,
      outcome: outcomeOf(new Set(rows.map((row) => deltaFamily(row.bucket)))),
      status: [diff.status_baseline, main?.status_candidate ?? null, diff.status_candidate],
      statusEvidence: { path: "$status", recording: count === 1 ? present(diff.status_baseline) : unavailable("Duplicate captured response addresses."), main: main ? present(main.status_candidate) : unavailable("Baseline response status was not uniquely published."), pr: count === 1 ? present(diff.status_candidate) : unavailable("Duplicate captured response addresses.") },
      fields,
      notes,
    };
  }).sort((a, b) => a.sequence - b.sequence);
}
