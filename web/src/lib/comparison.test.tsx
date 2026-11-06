import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";
import type { CallRecord, Delta, DeltaRow, HttpBodyClassification, HttpComparison, HttpDiff, RunRow, Scorecard } from "./api";
import { acceptedBodyReasons, callComparisonText, deltaRequestCounts, httpJudgment, joinDeltaResponses, responseFinding, responseMark, SAME_AS_RECORDING } from "./comparison";
import { CallComparison, CorrelationOutcome, ResponseEvidence } from "../components/Comparison";
import { FieldTable, Headline, SideCell } from "../components/DeltaSummary";
import { buildSpine } from "./spine";
import { resultOf } from "./result";

// The third-party raw viewer requires a browser. Keep the actual response
// panel under server-rendered tests and inspect what it passes to that viewer.
vi.mock("../components/JsonDiff", () => ({
  JsonDiff: ({ before, after }: { before: unknown; after: unknown }) => <pre data-raw-inspection>{JSON.stringify({ before, after })}</pre>,
}));

const body = (over: Partial<HttpBodyClassification> = {}): HttpBodyClassification => ({
  blocking: [], blocking_leaf_count: over.blocking?.length ?? 0, schema_derived_paths: [], order_only_paths: [], canon_absorbed: [], canon_conflicts: [],
  kernel_equivalent_paths: [], embedded_document_paths: [], document_text_paths: [], ...over,
});
const diff = (over: Partial<HttpDiff> = {}): HttpDiff => ({
  correlation_id: "request-a", request_sequence: 0, request_path: "/example/choices", status_baseline: 200,
  status_candidate: 200, status_match: true, body_diff: [], baseline_body: { choices: ["a", "b"] },
  candidate_body: { choices: ["b", "a"] }, ...over,
});
const scorecard = (responses: { diff: HttpDiff; body: HttpBodyClassification }[], over: Partial<Scorecard> = {}): Scorecard => ({
  verdict: { pass: true, inconclusive: false, reason: "comparison accepted" },
  summary: { matched_correlations: 1, total_correlations: 1, http_status_mismatches: 0, http_body_mismatches: 0, side_effect_divergences: 0, resolved_by_rank: {} },
  http_comparison: { version: 1, responses: responses.map((r) => ({ correlation_id: r.diff.correlation_id, request_sequence: r.diff.request_sequence, body: r.body })) }, ...over,
});
const field = (path: string, baseline: unknown, candidate: unknown) => ({ json_path: path, baseline, candidate, candidate_identity: "published-identity" });
const deltaRow = (path: string, bucket: DeltaRow["bucket"] = "changed", sequence = 0): DeltaRow => ({
  address: { kind: "body", correlation: "request-a", request_sequence: sequence, json_path: path },
  bucket, m: { hash: "main-identity" }, y: { hash: "candidate-identity" }, blocking: true,
});
const delta = (rows: DeltaRow[], over: Partial<Delta> = {}): Delta => ({
  m_run: "main", y_run: "candidate", canon_version: 9, http_comparison_mode: "classified",
  verdict: { pass: false, introduced: 0, changed: rows.length, inherited: 0, resolved: 0, reason: "published delta" },
  buckets: {}, lanes: [], rows, clean: 0, covered_correlations: 1, uncovered: { m_only: [], y_only: [], addresses: 0 },
  tape: "recording", sides: { m: { run: "main", tape_verdict: null, candidate: null }, y: { run: "candidate", tape_verdict: null, candidate: null } }, ...over,
});
const call = (over: Partial<CallRecord> = {}): CallRecord => ({
  correlation_id: "request-a", boundary: "api", trait_name: "Client", method_name: "send", kind: "matched", blocking: false,
  recorded: { args: { document: "first rendering" } }, observed: { args: { document: "second rendering" } }, ...over,
});

describe("published response presentation", () => {
  it.each(["stored response paths are ambiguous without full bodies", ""])("keeps the entire publication unavailable when the scorer publishes a reason: %s", (reason) => {
    const safe = diff({ request_sequence: 1 });
    const unsafe = diff({ request_sequence: 2, baseline_body: undefined, candidate_body: undefined });
    const publication = scorecard([{ diff: safe, body: body() }, { diff: unsafe, body: body({ unavailable_reason: reason }) }]).http_comparison!;
    let reads = 0;
    const cached: HttpComparison = { version: 1, get responses() { reads += 1; return publication.responses; } };
    const sc = scorecard([], { http_comparison: cached });
    const rejected = httpJudgment(sc, safe);
    expect(rejected.kind).toBe("unavailable");
    if (rejected.kind === "unavailable" && reason) expect(rejected.reason).toContain(reason);
    for (const raw of [safe, unsafe, safe]) {
      expect(httpJudgment(sc, raw)).toBe(rejected);
      expect(responseFinding(sc, raw)?.unavailable).toBe(true);
      expect(responseMark(sc, raw)).toBe("unknown");
      const html = renderToStaticMarkup(<ResponseEvidence diff={raw} scorecard={sc} />);
      expect(html).not.toContain("Body accepted");
      expect(html).toContain("unavailable");
    }
    expect(reads).toBe(1);
    const joined = joinDeltaResponses(delta([deltaRow("$.total", "changed", 1)]), [safe], [safe], sc, sc)[0];
    expect(joined.fields[0].pr).toBeUndefined();
    expect(joined.notes.join(" ")).toContain("unavailable");
  });

  it("validates and indexes one immutable publication once across report consumers", () => {
    const raws = Array.from({ length: 32 }, (_, index) => diff({ correlation_id: `request-${index % 4}`, request_sequence: Math.floor(index / 4) }));
    const bodyReads = raws.map(() => 0);
    const rows = raws.map((raw, index) => {
      const classified = Object.freeze(body());
      return Object.freeze({
        correlation_id: raw.correlation_id,
        request_sequence: raw.request_sequence,
        get body() { bodyReads[index] += 1; return classified; },
      });
    });
    const iterate = rows[Symbol.iterator].bind(rows);
    let traversals = 0;
    Object.defineProperty(rows, Symbol.iterator, { value: () => { traversals += 1; return iterate(); } });
    Object.freeze(rows);
    let versionReads = 0;
    let responseReads = 0;
    const published: HttpComparison = Object.freeze({
      get version() { versionReads += 1; return 1; },
      get responses() { responseReads += 1; return rows; },
    });
    const sc = scorecard([], { http_comparison: published });
    // The findings list, tree, detail pane, and delta join share this JSON
    // snapshot. Repeated consumers must not rescan or rebuild its index.
    for (let render = 0; render < 3; render += 1) {
      for (const raw of raws) {
        expect(httpJudgment(sc, raw).kind).toBe("published");
        expect(responseFinding(sc, raw)).toBeNull();
        expect(responseMark(sc, raw)).toBe("matched");
        expect(renderToStaticMarkup(<ResponseEvidence diff={raw} scorecard={sc} />)).toContain("Body accepted");
      }
      buildSpine([], undefined, raws, undefined, { scorecard: sc });
      joinDeltaResponses(delta([]), raws, raws, sc, sc);
    }
    expect(versionReads).toBe(1);
    expect(responseReads).toBe(1);
    expect(traversals).toBe(1);
    expect(bodyReads).toEqual(raws.map(() => 1));
  });

  it.each(["malformed", "future"] as const)("caches %s rejection and validates a replacement publication afresh", (kind) => {
    const raw = diff();
    let versionReads = 0;
    let responseReads = 0;
    const rows = [{ correlation_id: raw.correlation_id, request_sequence: raw.request_sequence, body: body({ blocking_leaf_count: 1 }) }];
    const published: HttpComparison = Object.freeze({
      get version() { versionReads += 1; return kind === "future" ? 2 : 1; },
      get responses() { responseReads += 1; return rows; },
    });
    const sc = scorecard([], { http_comparison: published });
    const rejected = httpJudgment(sc, raw);
    expect(rejected.kind).toBe("unavailable");
    for (let render = 0; render < 5; render += 1) {
      expect(httpJudgment(sc, raw)).toBe(rejected);
      expect(responseMark(sc, raw)).toBe("unknown");
    }
    expect(versionReads).toBe(1);
    expect(responseReads).toBe(kind === "future" ? 0 : 1);

    const replacement = scorecard([{ diff: raw, body: body() }]);
    expect(httpJudgment({ ...sc, http_comparison: replacement.http_comparison }, raw).kind).toBe("published");
    // Replacing a snapshot neither mutates nor rehabilitates the old one.
    expect(httpJudgment(sc, raw)).toBe(rejected);
    expect(versionReads).toBe(1);
  });

  it("does not reuse a valid publication's index for a malformed replacement object", () => {
    const raw = diff();
    const sc = scorecard([{ diff: raw, body: body() }]);
    expect(httpJudgment(sc, raw).kind).toBe("published");
    const replacement = scorecard([{ diff: raw, body: body() }, { diff: raw, body: body() }]);
    expect(httpJudgment({ ...sc, http_comparison: replacement.http_comparison }, raw).kind).toBe("unavailable");
    expect(httpJudgment(sc, raw).kind).toBe("published");
  });

  it("indexes immutable historical correlations once and preserves the first stored judgment", () => {
    const raws = Array.from({ length: 16 }, (_, index) => diff({ correlation_id: `legacy-${index}` }));
    const reads = raws.map(() => 0);
    const rows = raws.map((raw, index) => ({
      correlation_id: raw.correlation_id,
      get http_body_match() { reads[index] += 1; return index !== 0; },
    }));
    rows.push({ correlation_id: raws[0].correlation_id, http_body_match: true });
    Object.freeze(rows);
    const sc = scorecard([], { http_comparison: undefined, per_correlation: rows });
    for (let render = 0; render < 4; render += 1) {
      for (const [index, raw] of raws.entries()) expect(httpJudgment(sc, raw)).toEqual({ kind: "historical", accepted: index !== 0 });
    }
    expect(reads).toEqual(raws.map(() => 1));
    expect(httpJudgment({ ...sc, per_correlation: [{ correlation_id: raws[0].correlation_id, http_body_match: true }] }, raws[0])).toEqual({ kind: "historical", accepted: true });
  });

  it("keeps accepted rendering differences out of findings and tree marks, with reasons and raw inspection still visible", () => {
    const raw = diff({ body_diff: Array.from({ length: 20 }, (_, i) => field(`$.choices[${i}]`, i, 19 - i)) });
    const sc = scorecard([{ diff: raw, body: body({ canon_absorbed: [["$.choices", "default"]] }) }]);
    expect(responseFinding(sc, raw)).toBeNull();
    expect(buildSpine([], undefined, [raw], undefined, { scorecard: sc }).cases[0].root.mark).toBe("matched");
    const html = renderToStaticMarkup(<ResponseEvidence diff={raw} scorecard={sc} />);
    const defaultPanel = html.split("<details")[0];
    expect(defaultPanel).toContain("Body accepted by the scorer");
    expect(defaultPanel).toContain("$.choices");
    expect(defaultPanel).toContain("ordering accepted by default");
    expect(defaultPanel).not.toContain("20 blocking");
    expect(html).toContain("Raw captured response");
    expect(html).toContain("20 raw diff rows");
  });

  it("shows only the published blocking residue beside accepted paths", () => {
    const raw = diff({ body_diff: [field("$.choices[0]", "a", "b"), field("$.total", 10, 11)] });
    const sc = scorecard([{ diff: raw, body: body({ blocking: [field("$.total", 10, 11)], canon_absorbed: [["$.choices", "document"]] }) }]);
    expect(responseFinding(sc, raw)?.label).toBe("1 blocking response difference");
    const html = renderToStaticMarkup(<ResponseEvidence diff={raw} scorecard={sc} />).split("<details")[0];
    expect(html).toContain("$.total");
    expect(html).toContain("ordering accepted by system declaration");
    expect(html).not.toContain("$.choices[0]");
  });

  it("keeps responses in one correlation independently judged", () => {
    const first = diff({ request_sequence: 4 });
    const second = diff({ request_sequence: 5 });
    const sc = scorecard([{ diff: first, body: body() }, { diff: second, body: body({ blocking: [field("$.total", 1, 2)] }) }]);
    expect(responseMark(sc, first)).toBe("matched");
    expect(responseMark(sc, second)).toBe("response");
  });

  it("does not turn an inconclusive run into a pass when its response is accepted", () => {
    const raw = diff();
    const sc = scorecard([{ diff: raw, body: body() }], { verdict: { pass: false, inconclusive: true, reason: "recording tail unavailable" } });
    const run = { state: "completed", mode: "replay", verdict: "inconclusive", scorecard: sc, failure: null } as RunRow;
    expect(responseFinding(sc, raw)).toBeNull();
    expect(resultOf(run).state).toBe("INCONCLUSIVE");
    expect(sc.verdict.inconclusive).toBe(true);
  });

  it("honors the stored historical acceptance without inventing a reason", () => {
    const raw = diff({ body_diff: [field("$.choices[0]", "a", "b")] });
    const sc = scorecard([], { http_comparison: undefined, per_correlation: [{ correlation_id: "request-a", http_body_match: true }] });
    expect(responseFinding(sc, raw)).toBeNull();
    const html = renderToStaticMarkup(<ResponseEvidence diff={raw} scorecard={sc} />);
    expect(html).toContain("accepted by the stored scorer judgment");
    expect(html).toContain("not published for this historical run");
    expect(html).not.toContain("ordering accepted");
  });

  it("labels historical raw differences honestly when detailed judgments are missing", () => {
    const raw = diff({ body_diff: [field("$.total", 1, 2)] });
    const sc = scorecard([], { http_comparison: undefined, per_correlation: [{ correlation_id: "request-a", http_body_match: false }] });
    expect(responseFinding(sc, raw)?.label).toContain("historical comparison");
    expect(responseFinding(undefined, raw)?.label).toContain("classified details unavailable");
  });

  it("keeps future or missing published response details unavailable instead of claiming clean", () => {
    const raw = diff();
    for (const sc of [scorecard([], { http_comparison: { version: 2, responses: [] } }), scorecard([])]) {
      expect(httpJudgment(sc, raw).kind).toBe("unavailable");
      expect(responseMark(sc, raw)).toBe("unknown");
      expect(responseFinding(sc, raw)?.unavailable).toBe(true);
    }
  });

  it("refuses contradictory or ambiguous version-one publications instead of displaying accepted", () => {
    const raw = diff();
    const duplicate = field("$.total", 1, 2);
    const invalid = [
      scorecard([{ diff: raw, body: body({ blocking_leaf_count: 1 }) }]),
      scorecard([{ diff: raw, body: { ...body(), blocking_leaf_count: undefined } as unknown as HttpBodyClassification }]),
      scorecard([{ diff: raw, body: body() }, { diff: raw, body: body() }]),
      scorecard([{ diff: raw, body: body({ blocking: [duplicate, duplicate] }) }]),
      scorecard([{ diff: raw, body: body({ blocking: [{ ...duplicate, candidate_identity: "" }] }) }]),
    ];
    for (const sc of invalid) {
      expect(httpJudgment(sc, raw).kind).toBe("unavailable");
      expect(responseMark(sc, raw)).toBe("unknown");
      expect(renderToStaticMarkup(<ResponseEvidence diff={raw} scorecard={sc} />)).not.toContain("Body accepted");
    }
  });

  it("shows race and hidden-form reasons and preserves each correlation's own outcome", () => {
    const raw = diff();
    const sc = scorecard([{ diff: raw, body: body({ race_absorbed_paths: ["$.token"], hidden_form_paths: ["$"] }) }]);
    const html = renderToStaticMarkup(<ResponseEvidence diff={raw} scorecard={sc} />);
    expect(html).toContain("race-attributed difference set aside");
    expect(html).toContain("hidden form comparison accepted");
    expect(html).not.toContain("Body accepted");
    expect(renderToStaticMarkup(<CorrelationOutcome row={{ correlation_id: "uncertain", passed: false, inconclusive: true }} />)).toContain("inconclusive");
    expect(renderToStaticMarkup(<CorrelationOutcome row={{ correlation_id: "accepted", passed: true, inconclusive: false }} />)).toContain("accepted");
  });

  it("does not recalculate a backend finding from equal-looking or permuted captured arrays", () => {
    const raw = diff();
    const sc = scorecard([{ diff: raw, body: body({ blocking: [field("$.choices", ["a", "b"], ["b", "a"])] }) }]);
    expect(responseFinding(sc, raw)?.label).toBe("1 blocking response difference");
  });
});

describe("published call comparison", () => {
  it("shows the explicit subject and allowance without changing the call's structural classification", () => {
    const row = call({ comparison: { version: 1, subject: "arguments", verdict: { kind: "absorbed", reason: { kind: "args_identity" } }, candidate_identity: "semantic:v3:example" } });
    expect(renderToStaticMarkup(<CallComparison call={row} />)).toContain("Arguments accepted by the scorer: same semantic argument identity");
    expect(row.kind).toBe("matched");
    expect(row.blocking).toBe(false);
    expect(callComparisonText(call({ kind: "value_diverged", blocking: true, comparison: { version: 1, subject: "result", verdict: { kind: "diverged" } } }))).toContain("Result differs under the scorer");
  });

  it("does not compare a historical call's raw values to manufacture agreement", () => {
    expect(callComparisonText(call())).toContain("not published for this historical call");
    expect(callComparisonText(call({ comparison: { version: 2, subject: "arguments", verdict: { kind: "equal" } } }))).toContain("not supported");
  });

  it("renders published reasons without requiring a particular system or path", () => {
    const reasons = acceptedBodyReasons(body({ schema_derived_paths: ["$.generated"], document_text_paths: ["$.echo"], canon_absorbed: [["$.items", "both"]] }));
    expect(reasons).toContainEqual({ path: "$.items", reason: "ordering accepted by recorder and system declaration" });
    expect(reasons.map((r) => r.path)).toEqual(["$.items", "$.generated", "$.echo"]);
  });
});

describe("delta joins published comparison addresses", () => {
  it("retains literal dotted keys and nested paths with identical values as separate published findings", () => {
    const literal = '$["a.b"]';
    const nested = "$.a.b";
    const raw = diff({
      baseline_body: { "a.b": 1, a: { b: 1 } },
      candidate_body: { "a.b": 2, a: { b: 2 } },
      // The raw artifact's historical path format is unchanged and ambiguous;
      // the published residual addresses are the only ones used for this join.
      body_diff: [field(nested, 1, 2), field(nested, 1, 2)],
    });
    const mainRaw = diff({ baseline_body: raw.baseline_body, candidate_body: raw.baseline_body });
    const pr = scorecard([{ diff: raw, body: body({ blocking: [field(literal, 1, 2), field(nested, 1, 2)] }) }]);
    const main = scorecard([{ diff: mainRaw, body: body() }]);
    const rows = [literal, nested].map((path) => ({ ...deltaRow(path, "introduced"), m: "tape" as const }));
    const joined = joinDeltaResponses(delta(rows), [raw], [mainRaw], pr, main);
    expect(joined[0].fields).toEqual([literal, nested].map((path) => ({ path, recording: 1, main: SAME_AS_RECORDING, pr: 2, sameAsMain: false, bucket: "introduced" })));
    expect(responseFinding(pr, raw)?.label).toBe("2 blocking response differences");
    const response = renderToStaticMarkup(<ResponseEvidence diff={raw} scorecard={pr} />).split("<details")[0];
    const table = renderToStaticMarkup(<FieldTable reqs={joined} mainSha="main" />);
    for (const html of [response, table]) {
      expect(html).toContain("$[&quot;a.b&quot;]");
      expect(html).toContain(nested);
    }
  });

  it("still refuses duplicate published quoted paths instead of merging them", () => {
    const raw = diff();
    const duplicated = field('$["a.b"]', 1, 2);
    const sc = scorecard([{ diff: raw, body: body({ blocking: [duplicated, duplicated] }) }]);
    expect(httpJudgment(sc, raw).kind).toBe("unavailable");
    expect(responseFinding(sc, raw)?.unavailable).toBe(true);
  });

  it.each(["resolved", "introduced"] as const)("does not describe a passing %s delta as exact tape agreement", (bucket) => {
    const row: DeltaRow = { ...deltaRow("$.total", bucket), blocking: bucket === "resolved", m: bucket === "resolved" ? { hash: "main" } : "tape", y: bucket === "resolved" ? "tape" : { hash: "candidate" } };
    const d = delta([row], { verdict: { pass: true, introduced: 0, changed: 0, inherited: 0, resolved: bucket === "resolved" ? 1 : 0, reason: "no new blocking difference" } });
    const html = renderToStaticMarkup(<Headline d={d} />);
    expect(html).toContain("The delta attributes no new blocking difference to this PR.");
    expect(html).not.toContain("Every one");
    expect(html).not.toContain("differ the same way on main");
    if (bucket === "resolved") expect(html).toContain("Main has findings relative to the recording on 1 request.");
  });

  it("uses classified residue values at the backend address rather than a raw positional path", () => {
    const raw = diff({ body_diff: [field("$.items[7].total", 6, 9)] });
    const row = deltaRow("$.items[0].total");
    const pr = scorecard([{ diff: raw, body: body({ blocking: [field("$.items[0].total", 10, 20)] }) }]);
    const main = scorecard([{ diff: raw, body: body({ blocking: [field("$.items[0].total", 10, 15)] }) }]);
    const joined = joinDeltaResponses(delta([row]), [raw], [raw], pr, main);
    expect(joined[0].fields).toEqual([{ path: "$.items[0].total", recording: 10, main: 15, pr: 20, sameAsMain: false, bucket: "changed" }]);
  });

  it("does not add raw accepted paths to the backend's field list", () => {
    const raw = diff({ body_diff: [field("$.choices[0]", "a", "b")] });
    const sc = scorecard([{ diff: raw, body: body({ canon_absorbed: [["$.choices", "default"]] }) }]);
    expect(joinDeltaResponses(delta([]), [raw], [raw], sc, sc)[0].fields).toEqual([]);
  });

  it("uses backend buckets for agreement even when raw renderings differ or look identical", () => {
    const raw = diff();
    const pr = scorecard([{ diff: raw, body: body({ blocking: [field("$.echo", "recorded", "second rendering")] }) }]);
    const main = scorecard([{ diff: raw, body: body({ blocking: [field("$.echo", "recorded", "first rendering")] }) }]);
    expect(joinDeltaResponses(delta([deltaRow("$.echo", "inherited")]), [raw], [raw], pr, main)[0].fields[0].sameAsMain).toBe(true);
    expect(joinDeltaResponses(delta([deltaRow("$.echo", "changed")]), [raw], [raw], pr, pr)[0].fields[0].sameAsMain).toBe(false);
  });

  it("joins sequences independently and never assigns a correlation-only call change to every response", () => {
    const first = diff({ request_sequence: 1 });
    const second = diff({ request_sequence: 2 });
    const mainFirst = diff({ request_sequence: 1, status_candidate: 201 });
    const mainSecond = diff({ request_sequence: 2, status_candidate: 202 });
    const rows: DeltaRow[] = [deltaRow("$.total", "inherited", 1), deltaRow("$.total", "changed", 2), { address: { kind: "call", correlation: "request-a", boundary: "db", span_path: "request", operation: "read", occurrence: 0 }, bucket: "introduced", m: "tape", y: { hash: "new" }, blocking: true }];
    const sc = scorecard([{ diff: first, body: body({ blocking: [field("$.total", 0, 1)] }) }, { diff: second, body: body({ blocking: [field("$.total", 0, 2)] }) }]);
    const joined = joinDeltaResponses(delta(rows), [first, second], [mainFirst, mainSecond], sc, sc);
    expect(joined.map((r) => [r.sequence, r.status[1], r.outcome])).toEqual([[1, 201, "same"], [2, 202, "conflict"]]);
    expect(deltaRequestCounts(delta(rows)).n("conflict")).toBe(1);
    expect(deltaRequestCounts(delta(rows)).n("new")).toBe(0);
    expect(deltaRequestCounts(delta(rows)).n("clean")).toBe(0);
  });

  it("retains the backend's overall call change when the response itself has no delta difference", () => {
    const raw = diff();
    const row: DeltaRow = { address: { kind: "call", correlation: "request-a", boundary: "api", span_path: "request", operation: "send", occurrence: 0 }, bucket: "introduced", m: "tape", y: { hash: "changed" }, blocking: true };
    const d = delta([row]);
    expect(joinDeltaResponses(d, [raw], [raw])[0].outcome).toBe("clean");
    expect(deltaRequestCounts(d).n("new")).toBe(1);
    expect(deltaRequestCounts(d).n("clean")).toBe(0);
  });

  it("keeps backend tape sides and unsupported detail separate from missing values", () => {
    const raw = diff();
    const row = { ...deltaRow("$.total", "introduced"), m: "tape" as const };
    const joined = joinDeltaResponses(delta([row]), [raw], [raw], scorecard([]), scorecard([]))[0];
    expect(joined.fields[0].main).toBe(SAME_AS_RECORDING);
    expect(joined.fields[0].pr).toBeUndefined();
    expect(joined.notes.join(" ")).toContain("not publish comparison details");
    expect(joined.outcome).toBe("new");
  });

  it("uses historical raw values only when the backend chooses legacy mode", () => {
    const raw = diff({ body_diff: [field("$.items[7]", 1, 2)] });
    const sc = scorecard([{ diff: raw, body: body({ blocking: [field("$.items[7]", 10, 20)] }) }]);
    const old = delta([deltaRow("$.items[7]")], { http_comparison_mode: undefined });
    const joined = joinDeltaResponses(old, [raw], [raw], sc, sc)[0];
    expect(joined.fields[0].pr).toBe(2);
    expect(joined.notes[0]).toContain("Historical response comparison");
  });

  it("joins the API's addressed literal and nested legacy fields without decoding either path", () => {
    const paths = ['$["a.b"]', "$.a.b"];
    const raw = diff({ body_diff_address_version: 1, body_diff: paths.map((path) => field(path, 1, 2)) });
    const main = diff({ body_diff_address_version: 1, body_diff: paths.map((path) => field(path, 1, 3)) });
    const joined = joinDeltaResponses(delta(paths.map((path) => deltaRow(path)), { http_comparison_mode: "legacy" }), [raw], [main]);
    expect(joined[0].fields).toEqual(paths.map((path) => ({ path, recording: 1, main: 3, pr: 2, sameAsMain: false, bucket: "changed" })));
    const html = renderToStaticMarkup(<FieldTable reqs={joined} mainSha="main" />);
    expect(html).toContain("$[&quot;a.b&quot;]");
    expect(html).toContain("$.a.b");
  });

  it("keeps a legacy delta's bucket but refuses values from duplicate raw paths without address metadata", () => {
    const raw = diff({ body_diff: [field("$.a.b", 1, 2), field("$.a.b", 3, 4)] });
    const joined = joinDeltaResponses(delta([deltaRow("$.a.b")], { http_comparison_mode: "legacy" }), [raw], [raw]);
    expect(joined[0].fields).toEqual([{ path: "$.a.b", recording: undefined, main: undefined, pr: undefined, sameAsMain: false, bucket: "changed" }]);
    expect(joined[0].outcome).toBe("conflict");
    expect(joined[0].notes.join(" ")).toContain("duplicated and ambiguous");
    expect(renderToStaticMarkup(<FieldTable reqs={joined} mainSha="main" />)).toContain("not published");
  });

  it.each([
    { metadata: { body_diff_address_version: 2 }, reason: "address version 2 is not supported" },
    { metadata: { body_diff_address_version: 1, body_diff_address_error: "stored paths collide" }, reason: "stored paths collide" },
    { metadata: { body_diff_address_error: "" }, reason: "the server could not safely address these rows" },
  ])("keeps legacy values unavailable when the API reports $reason", ({ metadata, reason }) => {
    const raw = diff({ body_diff: [field("$.total", 1, 2)], ...metadata });
    const joined = joinDeltaResponses(delta([deltaRow("$.total")], { http_comparison_mode: "legacy" }), [raw], [raw])[0];
    expect(joined.fields[0].pr).toBeUndefined();
    expect(joined.fields[0].main).toBeUndefined();
    expect(joined.notes.join(" ")).toContain(reason);
    expect(joined.outcome).toBe("conflict");
  });

  it("keeps classified values authoritative while explaining a raw addressing error in raw inspection", () => {
    const raw = diff({ body_diff_address_error: "stored paths collide", body_diff: [field("$.a.b", 1, 2), field("$.a.b", 1, 2)] });
    const sc = scorecard([{ diff: raw, body: body({ blocking: [field('$["a.b"]', 1, 2)] }) }]);
    const joined = joinDeltaResponses(delta([deltaRow('$["a.b"]')]), [raw], [raw], sc, sc)[0];
    expect(joined.fields[0].pr).toBe(2);
    expect(joined.notes).toEqual([]);
    const html = renderToStaticMarkup(<ResponseEvidence diff={raw} scorecard={sc} />);
    expect(html.split("<details")[0]).toContain("1 blocking response difference published by the scorer.");
    expect(html.split("<details")[1]).toContain("Raw field addresses are unavailable: stored paths collide");
  });

  it("does not use raw values as a fallback for an unknown modern comparison mode", () => {
    const raw = diff({ body_diff: [field("$.total", 1, 2)] });
    const joined = joinDeltaResponses(delta([deltaRow("$.total")], { http_comparison_mode: "future" }), [raw], [raw])[0];
    expect(joined.fields[0].pr).toBeUndefined();
    expect(joined.notes.join(" ")).toContain("not supported");
    expect(joined.outcome).toBe("conflict");
  });

  it("renders missing classified payloads as not published, never as an absent or equal value", () => {
    const raw = diff();
    const reqs = joinDeltaResponses(delta([deltaRow("$.total")]), [raw], [raw], scorecard([]), scorecard([]));
    const html = renderToStaticMarkup(<FieldTable reqs={reqs} mainSha="main" />);
    expect(html).toContain("not published");
    expect(html).not.toContain(">absent<");
    const cell = renderToStaticMarkup(<table><tbody><tr><SideCell v={undefined} sameAsMain /></tr></tbody></table>);
    expect(cell).toContain("not published");
    expect(cell).not.toContain("= main");
  });

  it("does not infer field presence from a captured JSON null", () => {
    const html = renderToStaticMarkup(<table><tbody><tr><SideCell v={null} /></tr></tbody></table>);
    expect(html).toContain("null / absent");
    expect(html).not.toContain(">absent<");
  });
});
