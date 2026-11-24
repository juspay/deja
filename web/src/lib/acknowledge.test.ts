import { describe, expect, it } from "vitest";
import type { CallRecord, Delta, DeltaAddress, DeltaRow } from "./api";
import { badgeOf, callAtAddress, callEvidenceHref, groupCharged, requestsOf } from "./acknowledge";

const address: DeltaAddress = { kind: "call", correlation: "request-a", boundary: "db", operation: "update", span_path: "request>update", recorded_event: 849, occurrence: 0 };
const row = (over: Partial<DeltaRow> = {}): DeltaRow => ({ address, bucket: "introduced", m: "tape", y: { hash: "semantic:changed" }, blocking: true, ...over });
const delta = (rows: DeltaRow[]): Delta => ({ rows, verdict: { pass: false } } as Delta);
const call = (over: Partial<CallRecord> = {}): CallRecord => ({
  correlation_id: "request-a", boundary: "db", method_name: "update", trait_name: "Store", kind: "value_diverged", blocking: true,
  source_event_global_sequence: 849, recorded: { span_path: "request>update", graph_node_id: 12 }, observed: { span_path: "request>update", graph_node_id: 24 }, ...over,
});

describe("blocking acknowledgment scope", () => {
  it("requires only blocking introduced and changed rows, including novel/omitted variants", () => {
    const d = delta([
      row(), row({ bucket: "changed", blocking: false }), row({ bucket: "inherited" }), row({ bucket: "resolved" }),
      row({ bucket: "introduced_novel", address: { ...address, operation: "insert" } }),
      row({ bucket: "introduced_omission", address: { ...address, operation: "send" } }),
    ]);
    const groups = groupCharged(d);
    expect(groups).toHaveLength(3);
    expect(groups.flatMap((g) => g.rows)).toHaveLength(3);
    expect(requestsOf(groups)).toBe(1);
    expect(badgeOf(groups, d, "reviewer")?.label).toBe("3 to mark");
  });

  it("does not make informational proposals or their stale state require a decision", () => {
    const d = delta([row({ blocking: false, acknowledgement: { id: 1, state: "stale", by: "author", note: "old", value_changed: true } })]);
    expect(groupCharged(d)).toEqual([]);
    expect(badgeOf(groupCharged(d), d, "reviewer")).toBeNull();
  });

  it("retains the server's shape grouping across requests and value changes", () => {
    const groups = groupCharged(delta([row(), row({ address: { ...address, correlation: "request-b", recorded_event: 999 } })]));
    expect(groups).toHaveLength(1);
    expect(requestsOf(groups)).toBe(2);
  });
});

describe("exact call evidence navigation", () => {
  it("uses the recorded event as well as correlation, method, boundary and recorded path", () => {
    const target = call();
    const neighbors = [
      call({ source_event_global_sequence: 850 }), call({ correlation_id: "request-b" }), call({ boundary: "km" }),
      call({ method_name: "insert" }), call({ recorded: { span_path: "request>other" } }),
    ];
    expect(callAtAddress(address, [...neighbors, target])).toBe(target);
    expect(callEvidenceHref("candidate", address, [target])).toBe("/r/candidate?case=request-a&node=24&side=rep");
    expect(callAtAddress(address, [target, call()])).toBeNull();
  });

  it("links an omitted call to the recorded node, with no invented node fallback", () => {
    const omitted = call({ kind: "omitted", observed: undefined });
    expect(callEvidenceHref("baseline", address, [omitted])).toContain("node=12&side=rec");
    expect(callEvidenceHref("baseline", address, [call({ recorded: {}, observed: {} })])).toBeNull();
  });

  it("does not resolve repeated novel occurrences by a similar-looking call site", () => {
    const novelAddress = { ...address, recorded_event: undefined };
    const novel = call({ kind: "novel", source_event_global_sequence: undefined, recorded: undefined });
    expect(callAtAddress(novelAddress, [call(), novel])).toBe(novel);
    expect(callAtAddress(novelAddress, [novel, { ...novel }])).toBeNull();
    expect(callAtAddress(novelAddress, [novel, call({ source_event_global_sequence: undefined, kind: "future-kind" })])).toBeNull();
    expect(callAtAddress({ ...novelAddress, occurrence: 1 }, [novel])).toBeNull();
    expect(callAtAddress({ kind: "status", correlation: "request-a", request_sequence: 0 }, [novel])).toBeNull();
  });
});
