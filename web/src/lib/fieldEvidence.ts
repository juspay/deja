import type { CallRecord, DeltaRow, HttpDiff, JsonFieldDiff } from "./api";
import { callAtAddress } from "./acknowledge";

export type EvidenceValue =
  | { state: "present"; value: unknown; documents?: Record<string, unknown> }
  | { state: "missing" }
  | { state: "unavailable"; reason: string };
export type EvidenceField = { path: string; matchPath?: string; recording: EvidenceValue; main: EvidenceValue; pr: EvidenceValue };
export const unavailable = (reason: string): EvidenceValue => ({ state: "unavailable", reason });
export const present = (value: unknown, documents?: Record<string, unknown>): EvidenceValue => documents ? ({ state: "present", value, documents }) : ({ state: "present", value });
const missing: EvidenceValue = { state: "missing" };
type Segment = string | number;

function document(value: unknown): unknown {
  if (typeof value !== "string" || !/^[\s]*[\[{]/.test(value)) return value;
  try { const parsed: unknown = JSON.parse(value); return parsed && typeof parsed === "object" ? parsed : value; } catch { return value; }
}
function object(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
function pathOf(segments: Segment[]): string {
  return "$" + segments.map((segment) => typeof segment === "number" ? `[${segment}]` : segment && !/[.\[\]"\\]/.test(segment) ? `.${segment}` : `[${JSON.stringify(segment)}]`).join("");
}
function valueAt(root: EvidenceValue, segments: Segment[], decode = document): EvidenceValue {
  if (root.state !== "present") return root;
  let value = root.value;
  const projected = (value: unknown, path: Segment[]) => root.documents && Object.prototype.hasOwnProperty.call(root.documents, pathOf(path)) ? root.documents[pathOf(path)] : decode(value);
  for (let i = 0; i < segments.length; i++) {
    const segment = segments[i];
    value = projected(value, segments.slice(0, i));
    if (typeof segment === "number") {
      if (!Array.isArray(value)) return missing;
    } else if (!object(value)) return missing;
    if (!Object.prototype.hasOwnProperty.call(value, segment)) return missing;
    value = (value as Record<string | number, unknown>)[segment];
  }
  return present(projected(value, segments));
}
function equal(a: EvidenceValue, b: EvidenceValue): boolean {
  return a.state === b.state && (a.state === "present" && b.state === "present" ? JSON.stringify(a.value) === JSON.stringify(b.value) : a.state !== "unavailable");
}

/** A raw inspection projection, not a scorer: arrays retain captured positions.
 * Embedded JSON objects/arrays are opened only for navigation, never tolerance. */
export function capturedFields(recording: EvidenceValue, main: EvidenceValue, pr: EvidenceValue): { fields: EvidenceField[]; truncated: boolean } {
  const paths: Segment[][] = [];
  const documents = new Map<string, unknown>();
  const decode = (value: unknown): unknown => {
    if (typeof value !== "string") return value;
    if (!documents.has(value)) documents.set(value, document(value));
    return documents.get(value);
  };
  let visited = 0;
  let truncated = false;
  const walk = (segments: Segment[], depth: number) => {
    if (++visited > 20000 || paths.length >= 300) { truncated = true; return; }
    const sides = [recording, main, pr].map((root) => valueAt(root, segments, decode));
    if (sides.every((side) => side.state === "unavailable")) return;
    if (equal(sides[0], sides[1]) && equal(sides[1], sides[2])) return;
    const values = sides.filter((side): side is Extract<EvidenceValue, { state: "present" }> => side.state === "present").map((side) => side.value);
    const objects = values.filter(object);
    if (depth < 24 && objects.length > 0 && values.every(object)) {
      const keys = [...new Set(objects.flatMap(Object.keys))].sort();
      if (keys.length) { for (const key of keys) { if (truncated) break; walk([...segments, key], depth + 1); } return; }
    }
    const arrays = values.filter(Array.isArray);
    if (depth < 24 && arrays.length > 0 && values.every(Array.isArray)) {
      const length = Math.max(...arrays.map((array) => array.length));
      if (length) { for (let i = 0; i < length; i++) { if (truncated) break; walk([...segments, i], depth + 1); } return; }
    }
    paths.push(segments);
  };
  walk([], 0);
  return { fields: paths.map((segments) => ({ path: pathOf(segments), matchPath: typeof segments.at(-1) === "string" ? pathOf([segments.at(-1)!]) : undefined, recording: valueAt(recording, segments, decode), main: valueAt(main, segments, decode), pr: valueAt(pr, segments, decode) })), truncated };
}

function subjectValue(call: CallRecord | null, recorded: boolean, subject: "arguments" | "result"): EvidenceValue {
  if (!call) return unavailable("Exact call evidence is unavailable.");
  const side = recorded ? call.recorded : call.observed;
  const key = subject === "arguments" ? "args" : "result";
  const documents = side?.inspection?.version === 1 ? side.inspection[subject] : undefined;
  return side && Object.prototype.hasOwnProperty.call(side, key) ? present(side[key], documents) : unavailable(`Captured ${subject} were not published.`);
}

export function callFieldEvidence(row: DeltaRow, candidateCalls: CallRecord[], baselineCalls: CallRecord[]) {
  const candidate = callAtAddress(row.address, candidateCalls);
  const baseline = callAtAddress(row.address, baselineCalls);
  const published = candidate?.comparison ?? baseline?.comparison;
  if (published?.version !== 1 || !["arguments", "result"].includes(published.subject)) return { fields: [], note: "Comparison subject was not published. Inspect the full call evidence." };
  const subject = published.subject;
  const recordedCall = candidate?.recorded ? candidate : baseline;
  const recording = subjectValue(recordedCall, true, subject);
  const main = row.m === "absent" ? unavailable("The baseline did not reach this call.") : subjectValue(baseline, false, subject);
  const pr = row.y === "absent" ? unavailable("The candidate did not reach this call.") : subjectValue(candidate, false, subject);
  const projection = capturedFields(recording, main, pr);
  return { fields: projection.fields, note: `Raw captured ${subject}; inline JSON and producer-published document projections are opened structurally. Array indices are captured positions. These differences do not add scorer findings.${baseline?.comparison?.subject !== subject ? " The baseline's published comparison used a different or unpublished subject; its column is the captured value of the selected candidate subject, not a new judgment." : ""}${projection.truncated ? " Inspection is limited to 300 leaves / 20,000 visited values; open exact call evidence for the complete capture." : ""}` };
}

/** Historical body null is ambiguous unless the exact full captured parent
 * proves presence. Canonical residue indices are not captured array indices. */
export function responseFieldValue(field: JsonFieldDiff | undefined, side: "baseline" | "candidate", diff?: HttpDiff): EvidenceValue {
  if (!field) return unavailable("The judged field value was not published.");
  const presence = (field as JsonFieldDiff & { baseline_presence?: string; candidate_presence?: string })[`${side}_presence`];
  if (presence === "missing") return missing;
  // A non-null published operand cannot be the historical missing sentinel,
  // even when its canonical address cannot be traversed in the raw body.
  if (presence === "present" || field[side] !== null) return present(field[side]);
  if (presence === "unavailable") return unavailable("The producer could not prove field presence at this comparison address.");
  const root = side === "baseline" ? diff?.baseline_body : diff?.candidate_body;
  const segments = parseObjectPath(field.json_path);
  if (root !== undefined && segments) {
    const captured = responseValueAt(root, segments);
    if (captured.state === "missing" || (captured.state === "present" && captured.value === null)) return captured;
  }
  return unavailable("Historical null: missing versus explicit null was not published.");
}

/** Inspect an exact object path in a full captured body. A scorer-equivalent
 * side is not a copy of the recording: accepted values may still differ. */
export function capturedResponseFieldValue(path: string, side: "baseline" | "candidate", diff?: HttpDiff): EvidenceValue {
  const root = side === "baseline" ? diff?.baseline_body : diff?.candidate_body;
  const segments = parseObjectPath(path);
  return root !== undefined && segments ? responseValueAt(root, segments) : unavailable("Exact captured field presence/value is unavailable; scorer equivalence does not imply identical captured values.");
}

/** Only an accessible object's absent own member proves a missing response
 * field. Opaque strings/scalars cannot prove absence inside their contents. */
function responseValueAt(root: unknown, segments: string[]): EvidenceValue {
  let value = root;
  for (const segment of segments) {
    value = document(value);
    if (!object(value)) return unavailable("The captured parent is opaque or not an accessible object.");
    if (!Object.prototype.hasOwnProperty.call(value, segment)) return missing;
    value = value[segment];
  }
  return present(value);
}

// Only object-member paths are safe to traverse in historical response bodies:
// an index in a classified path can name a canonical array residue.
function parseObjectPath(path: string): string[] | null {
  if (!path.startsWith("$")) return null;
  const segments: string[] = [];
  let rest = path.slice(1);
  while (rest) {
    const dot = /^\.([^.[\]]+)/.exec(rest);
    if (dot) { segments.push(dot[1]); rest = rest.slice(dot[0].length); continue; }
    const bracket = /^\[("(?:[^"\\]|\\.)*")\]/.exec(rest);
    if (!bracket) return null;
    try { segments.push(JSON.parse(bracket[1]) as string); } catch { return null; }
    rest = rest.slice(bracket[0].length);
  }
  return segments;
}

/** Matching leaf evidence is a navigation relationship, never a causal claim.
 * Unknown sides cannot establish a match. Correlation alone never groups rows. */
export function matchingFieldObservations(rows: DeltaRow[], candidateCalls: CallRecord[], baselineCalls: CallRecord[]) {
  const by = new Map<string, { field: EvidenceField; rows: DeltaRow[]; locations: { row: DeltaRow; path: string }[] }>();
  for (const row of rows) {
    for (const field of callFieldEvidence(row, candidateCalls, baselineCalls).fields) {
      const sides = [field.recording, field.main, field.pr];
      if (!field.matchPath || sides.some((side) => side.state === "unavailable") || equal(field.main, field.pr)) continue;
      const key = JSON.stringify([row.address.correlation, field.matchPath, ...sides]);
      const group = by.get(key) ?? { field: { ...field, path: field.matchPath }, rows: [], locations: [] };
      if (!group.rows.includes(row)) group.rows.push(row);
      group.locations.push({ row, path: field.path });
      by.set(key, group);
    }
  }
  return [...by.values()].filter((group) => group.rows.length > 1);
}
