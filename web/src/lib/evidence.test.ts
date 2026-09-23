// A view built from several API reads must not render a failed read as an
// empty one. The API answers an absent artifact with a named error; before
// this, both evidence views read `data ?? []` and showed zero findings.

import { describe, expect, it } from "vitest";
import { emptyFindingsText, evidenceOf } from "./evidence";

const failed = (msg: string) => ({ data: undefined, error: new Error(msg) });
const read = <T>(data: T) => ({ data, error: null });

describe("evidenceOf", () => {
  it("names a failed ledger read as blocking, with the server's reason", () => {
    const e = evidenceOf(failed("no call_ledger artifact for run-1: the run never registered one"), read([]));
    expect(e.blocking).toContain("call ledger");
    expect(e.blocking).toContain("the run never registered one");
    expect(e.calls).toEqual([]);
  });

  it("names a failed http-diffs read as a note, and keeps the ledger", () => {
    const row = { correlation_id: "c1" } as never;
    const e = evidenceOf(read([row]), failed("no http_diffs artifact for run-1"));
    expect(e.blocking).toBeNull();
    expect(e.calls).toEqual([row]);
    expect(e.notes.join(" ")).toContain("no http_diffs artifact for run-1");
  });

  it("two successful empty reads are a fact, and say nothing is missing", () => {
    const e = evidenceOf(read([]), read([]));
    expect(e.blocking).toBeNull();
    expect(e.notes).toEqual([]);
    expect(e.complete).toBe(true);
  });

  it("a view with any failed read is not complete", () => {
    expect(evidenceOf(read([]), failed("x")).complete).toBe(false);
    expect(evidenceOf(failed("x"), read([])).complete).toBe(false);
  });

  it("uses the error's message, not its String() form", () => {
    const e = evidenceOf(failed("boom"), read([]));
    expect(e.blocking).not.toContain("Error: ");
  });

  it("an empty list claims nothing was published only when every read succeeded", () => {
    expect(emptyFindingsText(evidenceOf(read([]), read([])))).toContain("were published");
    const partial = emptyFindingsText(evidenceOf(read([]), failed("x")));
    expect(partial).not.toContain("were published");
    expect(partial).toContain("could not be read");
  });
});
