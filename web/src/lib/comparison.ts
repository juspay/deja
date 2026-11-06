import type { CallRecord, ClauseSource, Delta, DeltaBucket, DeltaFamily, DeltaRow, HttpBodyClassification, HttpComparison, HttpDiff, JsonFieldDiff, Scorecard, ValueAbsorption } from "./api";
import { deltaFamily } from "./api";

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

export const SAME_AS_RECORDING = Symbol("same as recording");
export type FieldSide = unknown | typeof SAME_AS_RECORDING;
export type DeltaOutcome = "conflict" | "new" | "restored" | "same" | "clean";
export type DeltaField = { path: string; recording: unknown; main: FieldSide; pr: FieldSide; sameAsMain: boolean; bucket: DeltaBucket };
export type DeltaRequest = {
  correlation: string;
  sequence: number;
  method: string;
  lane: string;
  outcome: DeltaOutcome;
  status: [number, number | null, number];
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
  const mainBy = new Map(mainDiffs.map((diff) => [responseKey(diff.correlation_id, diff.request_sequence), diff]));
  const mode = delta.http_comparison_mode ?? "legacy";
  return prDiffs.map((diff): DeltaRequest => {
    const key = responseKey(diff.correlation_id, diff.request_sequence);
    const rows = rowsBy.get(key) ?? [];
    const main = mainBy.get(key);
    const p = responseFields(mode, prScorecard, diff);
    const m = responseFields(mode, mainScorecard, main);
    const fields = rows.flatMap((row): DeltaField[] => {
      if (row.address.kind !== "body") return [];
      const path = row.address.json_path;
      const prField = p.fields.find((f) => f.json_path === path);
      const mainField = m.fields.find((f) => f.json_path === path);
      const side = (value: DeltaRow["m"], field: JsonFieldDiff | undefined): FieldSide => value === "tape" ? SAME_AS_RECORDING : field?.candidate;
      return [{ path, recording: (prField ?? mainField)?.baseline, main: side(row.m, mainField), pr: side(row.y, prField), sameAsMain: row.bucket === "inherited" || row.bucket === "clean", bucket: row.bucket }];
    });
    const notes = [...new Set([p.note, m.note].filter((note): note is string => !!note))];
    if (mode === "legacy") notes.unshift("Historical response comparison: raw field values are shown for the backend's delta addresses.");
    if (fields.some((field) => field.main === undefined || field.pr === undefined)) notes.push("Some judged field values were not published; their delta classifications remain shown.");
    return {
      correlation: diff.correlation_id,
      sequence: diff.request_sequence,
      method: diff.request_path.split("/").pop() ?? diff.request_path,
      lane: laneBy.get(diff.correlation_id) ?? `no connector call · ${(diff.request_path.split("/").pop() ?? "").toLowerCase()}`,
      outcome: outcomeOf(new Set(rows.map((row) => deltaFamily(row.bucket)))),
      status: [diff.status_baseline, main?.status_candidate ?? null, diff.status_candidate],
      fields,
      notes,
    };
  }).sort((a, b) => a.sequence - b.sequence);
}
