import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import type { CallRecord, Delta, DeltaRow, HttpDiff, JsonFieldDiff, Scorecard } from "./api";
import { capturedFields, capturedResponseFieldValue, callFieldEvidence, matchingFieldObservations, present, responseFieldValue, unavailable } from "./fieldEvidence";
import { EvidenceValueView, FieldInspector } from "../components/FieldInspector";
import { httpJudgment, joinDeltaResponses } from "./comparison";

const call = (args: unknown, over: Partial<CallRecord> = {}): CallRecord => ({ correlation_id: "request", boundary: "db", trait_name: "Store", method_name: "update", kind: "value_diverged", blocking: true,
  source_event_global_sequence: 1, recorded: { args: { optional: "recorded" }, span_path: "root>update" }, observed: { args, span_path: "root>update" },
  comparison: { version: 1, subject: "arguments", verdict: { kind: "diverged" } }, ...over });
const row = (over: Partial<DeltaRow> = {}): DeltaRow => ({ address: { kind: "call", correlation: "request", boundary: "db", operation: "update", span_path: "root>update", recorded_event: 1, occurrence: 0 }, bucket: "changed", blocking: true, m: { hash: "baseline" }, y: { hash: "candidate" }, ...over });
const field = (over: Partial<JsonFieldDiff> = {}): JsonFieldDiff => ({ json_path: "$.optional", baseline: null, candidate: 1, ...over });

describe("captured field inspection", () => {
  it("renders captured markup as text and reads prototype-named keys only as own JSON members", () => {
    const payload = '<img src=x onerror="alert(1)"><script>alert(2)</script>';
    const captured = JSON.parse(JSON.stringify({ value: payload, constructor: "captured" })) as Record<string, unknown>;
    const evidence = capturedFields(present({}), present({}), present(captured));
    const html = renderToStaticMarkup(<><FieldInspector fields={evidence.fields} note={payload} /><EvidenceValueView evidence={present(payload)} /></>);
    expect(html).not.toContain("<img");
    expect(html).not.toContain("<script");
    expect(html).toContain("&lt;img");
    expect(evidence.fields.find((entry) => entry.path === "$.constructor")?.recording).toEqual({ state: "missing" });
    const parent = JSON.parse('{"__proto__":{"polluted":"captured"}}') as Record<string, unknown>;
    expect(capturedResponseFieldValue("$.__proto__.polluted", "candidate", { candidate_body: parent } as HttpDiff)).toEqual({ state: "present", value: "captured" });
    expect(capturedResponseFieldValue("$.__proto__.polluted", "candidate", { candidate_body: {} } as HttpDiff)).toEqual({ state: "missing" });
    expect(Object.prototype).not.toHaveProperty("polluted");
  });
  it("distinguishes missing, explicit null and unavailable on one selected leaf", () => {
    const { fields } = capturedFields(present({}), present({ optional: null }), unavailable("capture not published"));
    expect(fields[0]).toMatchObject({ path: "$.optional", recording: { state: "missing" }, main: { state: "present", value: null }, pr: { state: "unavailable" } });
    const html = renderToStaticMarkup(<FieldInspector fields={fields} note="raw inspection, not new findings" />);
    expect(html).toContain("missing field"); expect(html).toContain("explicit null"); expect(html).toContain("capture not published");
  });
  it("keeps literal punctuation keys distinct from nested members and preserves captured array indices", () => {
    const { fields } = capturedFields(present({ "a.b": 1, a: { b: 1 }, "x/y~z": [1] }), present({ "a.b": 1, a: { b: 1 }, "x/y~z": [1] }), present({ "a.b": 2, a: { b: 3 }, "x/y~z": [2] }));
    expect(fields.map((entry) => entry.path)).toEqual(["$.a.b", '$["a.b"]', "$.x/y~z[0]"]);
  });
  it("uses published parser projections for encoded documents and never decodes opaque base64 locally", () => {
    const opaque = "eyJvcHRpb25hbCI6bnVsbH0=";
    const c = call({ data: opaque }, { observed: { args: { data: opaque }, inspection: { version: 1, arguments: { "$.data": { optional: null } } } } });
    const m = call({ data: "encoded-before" }, { observed: { args: { data: "encoded-before" }, inspection: { version: 1, arguments: { "$.data": {} } } }, recorded: { args: { data: "encoded-recording" }, span_path: "root>update", inspection: { version: 1, arguments: { "$.data": {} } } } });
    c.recorded = m.recorded;
    expect(callFieldEvidence(row(), [c], [m]).fields.find((entry) => entry.path === "$.data.optional")?.main).toEqual({ state: "missing" });
    const raw = capturedFields(present({ data: "before" }), present({ data: "before" }), present({ data: opaque }));
    expect(raw.fields.map((entry) => entry.path)).toEqual(["$.data"]);
  });
  it("rejects unsupported comparison subjects and bounds large evidence with an explicit limit note", () => {
    expect(callFieldEvidence(row(), [call({}, { comparison: undefined })], []).fields).toEqual([]);
    const wide = Object.fromEntries(Array.from({ length: 500 }, (_, i) => [`field${i}`, i]));
    const bounded = callFieldEvidence(row(), [call(wide)], [call({})]);
    expect(bounded.fields).toHaveLength(300);
    expect(bounded.note).toContain("limited to 300 leaves");
  });
  it("shows raw changes without making a new scorer verdict", () => {
    const c = call({ optional: "candidate" }, { comparison: { version: 1, subject: "arguments", verdict: { kind: "absorbed", reason: { kind: "args_identity" } } } });
    const evidence = callFieldEvidence(row(), [c], [call({ optional: "baseline" })]);
    expect(evidence.fields).toHaveLength(1);
    expect(evidence.note).toContain("do not add scorer findings");
    expect(c.comparison?.verdict.kind).toBe("absorbed");
  });
});

describe("response presence evidence", () => {
  it.each(["eyJvcHRpb25hbCI6bnVsbH0=", "opaque text", 17, null, [null]])("does not infer missing response members beneath opaque or non-object parents: %j", (parent) => {
    const response = { baseline_body: { data: parent }, candidate_body: { data: parent } } as HttpDiff;
    const residual = field({ json_path: "$.data.optional", candidate: null });
    expect(responseFieldValue(residual, "baseline", response).state).toBe("unavailable");
    expect(responseFieldValue(residual, "candidate", response).state).toBe("unavailable");
    expect(capturedResponseFieldValue(residual.json_path, "baseline", response).state).toBe("unavailable");
    expect(capturedResponseFieldValue(residual.json_path, "candidate", response).state).toBe("unavailable");
  });
  it("proves absence only from accessible object members, including ordinary inline JSON", () => {
    const path = "$.data.optional";
    for (const data of [{}, "{}"] ) {
      const response = { baseline_body: { data } } as HttpDiff;
      expect(capturedResponseFieldValue(path, "baseline", response)).toEqual({ state: "missing" });
      expect(responseFieldValue(field({ json_path: path }), "baseline", response)).toEqual({ state: "missing" });
    }
    const captured = { baseline_body: { data: { optional: null } } } as HttpDiff;
    expect(capturedResponseFieldValue(path, "baseline", captured)).toEqual({ state: "present", value: null });
  });
  it("does not replace a scorer-equivalent captured baseline with the recording value", () => {
    const base = { correlation_id: "request", request_sequence: 0, request_path: "/response", status_baseline: 200, status_candidate: 200, status_match: true, body_diff: [], baseline_body: { id: "recorded" }, candidate_body: { id: "baseline-accepted" } } as HttpDiff;
    const candidate = { ...base, candidate_body: { id: "candidate-change" }, body_diff: [{ json_path: "$.id", baseline: "recorded", candidate: "candidate-change" }] };
    const r = { ...row({ m: "tape" }), address: { kind: "body", correlation: "request", request_sequence: 0, json_path: "$.id" } } as DeltaRow;
    const d = { rows: [r], http_comparison_mode: "legacy" } as Delta;
    const evidence = joinDeltaResponses(d, [candidate], [base])[0].fields[0].evidence!;
    expect(evidence.recording).toEqual({ state: "present", value: "recorded" });
    expect(evidence.main).toEqual({ state: "present", value: "baseline-accepted" });
    expect(evidence.pr).toEqual({ state: "present", value: "candidate-change" });
    const unavailable = joinDeltaResponses(d, [candidate], [{ ...base, candidate_body: undefined }])[0].fields[0].evidence!;
    expect(unavailable.main.state).toBe("unavailable");
    const residue = { ...r, address: { ...r.address, json_path: "$.items[0].id" } } as DeltaRow;
    expect(joinDeltaResponses({ ...d, rows: [residue] }, [candidate], [base])[0].fields[0].evidence?.main.state).toBe("unavailable");
  });
  it("fails closed for duplicate response addresses while retaining the published delta bucket", () => {
    const response = { correlation_id: "request", request_sequence: 0, request_path: "/response", status_baseline: 200, status_candidate: 200, status_match: true, body_diff: [field()] } as HttpDiff;
    const d = { rows: [{ ...row(), address: { kind: "body", correlation: "request", request_sequence: 0, json_path: "$.optional" } }], http_comparison_mode: "legacy" } as Delta;
    const joined = joinDeltaResponses(d, [response, { ...response, status_candidate: 500 }], [response]);
    expect(joined).toHaveLength(1);
    expect(joined[0].fields[0]).toMatchObject({ bucket: "changed", evidence: { pr: { state: "unavailable" } } });
    expect(joined[0].statusEvidence?.pr.state).toBe("unavailable");
    expect(joined[0].notes.join(" ")).toContain("Duplicate response");
  });
  it("rejects malformed producer presence rather than treating it as missing or accepted", () => {
    const response = { correlation_id: "request", request_sequence: 0 } as HttpDiff;
    const classification = { blocking_leaf_count: 1, blocking: [{ ...field(), candidate_identity: "unchanged", candidate_presence: "missing" }], schema_derived_paths: [], order_only_paths: [], canon_absorbed: [], canon_conflicts: [], kernel_equivalent_paths: [], embedded_document_paths: [], document_text_paths: [] };
    const sc = { http_comparison: { version: 1, responses: [{ correlation_id: "request", request_sequence: 0, body: classification }] } } as unknown as Scorecard;
    expect(httpJudgment(sc, response).kind).toBe("unavailable");
  });
  it("accepts explicit producer presence without inferring from null", () => {
    expect(responseFieldValue({ ...field(), baseline_presence: "missing" } as JsonFieldDiff, "baseline")).toEqual({ state: "missing" });
    expect(responseFieldValue({ ...field(), baseline_presence: "present" } as JsonFieldDiff, "baseline")).toEqual({ state: "present", value: null });
    expect(responseFieldValue(field(), "baseline").state).toBe("unavailable");
    expect(responseFieldValue({ ...field(), candidate_presence: "unavailable" } as JsonFieldDiff, "candidate")).toEqual({ state: "present", value: 1 });
  });
  it("proves historical null/missing only through the exact captured object parent", () => {
    expect(responseFieldValue(field(), "baseline", { baseline_body: {} } as HttpDiff)).toEqual({ state: "missing" });
    expect(responseFieldValue(field(), "baseline", { baseline_body: { optional: null } } as HttpDiff)).toEqual({ state: "present", value: null });
    expect(responseFieldValue(field({ json_path: "$.items[0]" }), "baseline", { baseline_body: { items: [null] } } as HttpDiff).state).toBe("unavailable");
    const literal = field({ json_path: '$["a.b"]' });
    expect(responseFieldValue(literal, "baseline", { baseline_body: { "a.b": null, a: { b: 1 } } } as HttpDiff)).toEqual({ state: "present", value: null });
  });
  it("does not display unavailable evidence as a missing value", () => {
    const html = renderToStaticMarkup(<EvidenceValueView evidence={unavailable("historical null presence unknown")} />);
    expect(html).toContain("unavailable"); expect(html).not.toContain("missing field");
  });
});

describe("related observation evidence", () => {
  it("groups matching named changes across structural locations while retaining both exact paths", () => {
    const a = row();
    const b = row({ address: { kind: "call", correlation: "request", boundary: "http_outgoing", operation: "send", span_path: "root>send", recorded_event: 2, occurrence: 0 } });
    const c1 = call({ binds: { optional: null } }, { recorded: { args: { binds: {} }, span_path: "root>update" } });
    const m1 = call({ binds: {} }, { recorded: c1.recorded });
    const c2 = call({ body: { metadata: { optional: null } } }, { boundary: "http_outgoing", method_name: "send", source_event_global_sequence: 2, recorded: { args: { body: { metadata: {} } }, span_path: "root>send" } });
    const m2 = call({ body: { metadata: {} } }, { ...c2, observed: { args: { body: { metadata: {} } } } });
    const groups = matchingFieldObservations([a, b], [c1, c2], [m1, m2]);
    expect(groups).toHaveLength(1);
    expect(groups[0].field.path).toBe("$.optional");
    expect(groups[0].locations.map((location) => location.path)).toEqual(["$.binds.optional", "$.body.metadata.optional"]);
    expect(matchingFieldObservations([a, { ...b, address: { ...b.address, correlation: "other" } }], [c1, c2], [m1, m2])).toEqual([]);
  });
  it("uses fully captured baseline arguments when its separate scorer judgment concerned a result", () => {
    const c = call({ optional: null }, { recorded: { args: {}, span_path: "root>update" } });
    const baseline = call({}, { recorded: c.recorded, comparison: { version: 1, subject: "result", verdict: { kind: "equal" } } });
    const evidence = callFieldEvidence(row(), [c], [baseline]);
    expect(evidence.fields[0].main).toEqual({ state: "missing" });
    expect(evidence.note).toContain("not a new judgment");
  });
});
