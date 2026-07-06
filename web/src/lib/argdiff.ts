// Deep argument diff: find the leaves that changed between a recorded call's
// args and the candidate's args, so a MODIFIED call shows *what* changed — not
// just "args differ". Handles the common Hyperswitch shape where the meaningful
// value is buried inside a Rust `Debug` blob string (e.g. PaymentAttemptNew),
// by windowing the changed region of long strings.

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

function walk(rec: unknown, cand: unknown, path: string, out: LeafDiff[]) {
  if (JSON.stringify(rec) === JSON.stringify(cand)) return;
  if (isObj(rec) && isObj(cand)) {
    for (const k of new Set([...Object.keys(rec), ...Object.keys(cand)])) {
      walk(rec[k], cand[k], path ? `${path}.${k}` : k, out);
    }
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
