// Deep argument diff: find the leaves that changed between a recorded call's
// args and the candidate's args, so a MODIFIED call shows *what* changed — not
// just "args differ". Handles the common Hyperswitch shape where the meaningful
// value is buried inside a Rust `Debug` blob string (e.g. PaymentAttemptNew),
// by windowing the changed region of long strings.
//
// Arrays are walked, not treated as one leaf. A header list is `[[name, value],
// …]` in whatever order the sending process's map iterated, so as one leaf it
// rendered both whole lists — thirty lines each, bearer token included — for a
// single changed header, and the reader had to find it by eye. Header pairs are
// compared by NAME, a string that parses as a JSON document is compared as that
// document, and equal-length arrays are compared element by element.

export type LeafDiff = {
  path: string;
  recorded: unknown;
  candidate: unknown;
  // For changed string leaves: a windowed highlight of the differing span.
  highlight?: StringHighlight;
};

export type StringHighlight = {
  before: string; // common prefix (windowed, with leading … if clipped)
  recordedMid: string; // the part only in recorded
  candidateMid: string; // the part only in candidate
  after: string; // common suffix (windowed, with trailing … if clipped)
};

function isObj(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

const WINDOW = 48;

/* Windowed common-prefix/suffix diff for two strings — surfaces a single
   changed value inside a long blob without dumping the whole blob. */
export function highlightString(a: string, b: string): StringHighlight {
  let p = 0;
  const max = Math.min(a.length, b.length);
  while (p < max && a[p] === b[p]) p++;
  let s = 0;
  while (s < max - p && a[a.length - 1 - s] === b[b.length - 1 - s]) s++;
  const before = a.slice(0, p);
  const after = a.slice(a.length - s);
  return {
    before: (before.length > WINDOW ? "…" : "") + before.slice(-WINDOW),
    recordedMid: a.slice(p, a.length - s),
    candidateMid: b.slice(p, b.length - s),
    after: after.slice(0, WINDOW) + (after.length > WINDOW ? "…" : ""),
  };
}

/** `[[name, value], …]` — the wire shape of an HTTP header list. */
function isPairList(v: unknown): v is [string, unknown][] {
  return (
    Array.isArray(v) &&
    v.length > 0 &&
    v.every((e) => Array.isArray(e) && e.length === 2 && typeof e[0] === "string")
  );
}

/* A header list keyed by lower-cased name. A name that repeats (set-cookie)
   keeps every value, in arrival order, so multiplicity still compares. */
function pairsByName(pairs: [string, unknown][]): Record<string, unknown> {
  const out: Record<string, unknown[]> = {};
  for (const [name, value] of pairs) (out[name.toLowerCase()] ??= []).push(value);
  const flat: Record<string, unknown> = {};
  for (const [k, vs] of Object.entries(out)) flat[k] = vs.length === 1 ? vs[0] : vs;
  return flat;
}

/* A string that carries a JSON document (a body sent as text). Only objects
   and arrays: a bare number or string is a value, not a document. */
function parsedDocument(v: unknown): unknown | undefined {
  if (typeof v !== "string") return undefined;
  const t = v.trimStart();
  if (!(t.startsWith("{") || t.startsWith("["))) return undefined;
  try {
    return JSON.parse(v);
  } catch {
    return undefined;
  }
}

function walk(rec: unknown, cand: unknown, path: string, out: LeafDiff[]) {
  if (JSON.stringify(rec) === JSON.stringify(cand)) return;
  if (isObj(rec) && isObj(cand)) {
    for (const k of new Set([...Object.keys(rec), ...Object.keys(cand)])) {
      walk(rec[k], cand[k], path ? `${path}.${k}` : k, out);
    }
    return;
  }
  // An EMPTY list is a header list too, when the other side is one. Requiring
  // length > 0 on both meant adding the first header, or removing the last,
  // fell through to the whole-array leaf and rendered both lists in full —
  // the rendering this module exists to avoid, in the case where the list is
  // shortest. Two empty lists are equal and never reach here.
  const pairish = (v: unknown, other: unknown): v is [string, unknown][] =>
    isPairList(v) || (Array.isArray(v) && v.length === 0 && isPairList(other));
  if (pairish(rec, cand) && pairish(cand, rec)) {
    walk(pairsByName(rec), pairsByName(cand), path, out);
    return;
  }
  if (Array.isArray(rec) && Array.isArray(cand) && rec.length === cand.length) {
    for (let i = 0; i < rec.length; i++) walk(rec[i], cand[i], `${path}[${i}]`, out);
    return;
  }
  const recDoc = parsedDocument(rec);
  const candDoc = parsedDocument(cand);
  if (recDoc !== undefined && candDoc !== undefined) {
    walk(recDoc, candDoc, path ? `${path}(json)` : "(json)", out);
    return;
  }
  // a changed leaf (or array, or shape change)
  const diff: LeafDiff = { path, recorded: rec, candidate: cand };
  if (typeof rec === "string" && typeof cand === "string") {
    diff.highlight = highlightString(rec, cand);
  }
  out.push(diff);
}

/* Changed leaves between recorded and candidate argument objects. */
export function diffArgs(recorded: unknown, candidate: unknown): LeafDiff[] {
  const out: LeafDiff[] = [];
  walk(recorded, candidate, "", out);
  return out;
}

const SHOWN = 40;

/* Strings render bare so a header value reads as itself, not as a quoted blob.
   That hides a type change: `short(5)` and `short("5")` are both `5`, so a leaf
   that really did change renders as `amount: 5 -> 5` and reads as no change at
   all. When the two sides are not the same type — the case where it matters —
   quote the string side so the difference is on screen. */
function short(v: unknown, quoteStrings = false): string {
  const s =
    typeof v === "string"
      ? quoteStrings
        ? JSON.stringify(v)
        : v
      : v === undefined
        ? "∅"
        : JSON.stringify(v);
  return s.length > SHOWN ? `${s.slice(0, SHOWN - 1)}…` : s;
}

/* How much unchanged text to keep either side of the differing region when the
   two sides are too long to tell apart at SHOWN characters. */
const CONTEXT = 12;

/* `short` cuts at SHOWN characters, so two long values that share a prefix
   render identically — the same "a real change reads as no change" failure the
   type quoting above fixes, reached by length instead of by type, and the
   likelier one on a Rust `Debug` blob where the change is buried deep. When the
   two renderings collide, show the region that actually differs instead of the
   prefix they have in common. */
function differingRegion(rec: unknown, cand: unknown): [string, string] | null {
  const a = typeof rec === "string" ? rec : JSON.stringify(rec) ?? "";
  const b = typeof cand === "string" ? cand : JSON.stringify(cand) ?? "";
  if (a === b) return null;
  const h = highlightString(a, b);
  const pre = h.before.slice(-CONTEXT);
  const post = h.after.slice(0, CONTEXT);
  const lead = h.before.length > pre.length ? "…" : "";
  const tail = h.after.length > post.length ? "…" : "";
  const mid = (m: string) => (m.length > SHOWN ? `${m.slice(0, SHOWN - 1)}…` : m);
  return [
    `${lead}${pre}${mid(h.recordedMid)}${post}${tail}`,
    `${lead}${pre}${mid(h.candidateMid)}${post}${tail}`,
  ];
}

/** One changed leaf, said in a line: `path: recorded → candidate`. */
export type LeafSummary = { path: string; recorded: string; candidate: string };

/* The first few changed leaves as one-liners, so a row can say what changed
   before anyone opens it. `more` is how many it did not fit. */
export function summarizeLeaves(
  leaves: LeafDiff[],
  max = 2,
): { shown: LeafSummary[]; more: number } {
  const shown = leaves.slice(0, max).map((d) => {
    // `null` is typeof "object" and `undefined` its own type, so this catches
    // null-vs-"null" and ∅-vs-"" as well as 5-vs-"5".
    const mixed = typeof d.recorded !== typeof d.candidate;
    let recorded = short(d.recorded, mixed);
    let candidate = short(d.candidate, mixed);
    if (recorded === candidate) {
      // Same rendering for two values the walk says differ: truncation ate the
      // difference. Fall back to the region that actually changed.
      const win = differingRegion(d.recorded, d.candidate);
      if (win) [recorded, candidate] = win;
    }
    return { path: d.path || "(value)", recorded, candidate };
  });
  return { shown, more: Math.max(0, leaves.length - shown.length) };
}
