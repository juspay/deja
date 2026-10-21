import { describe, expect, test } from "vitest";
import { ago, agoText, argoOf, carries, promotionReplayWord, howOf, inFlight, mergesSince, placesOf, promotionCandidates, promotionProgress, stateTone } from "./cd";
import type { CdVersionRow } from "./api";

function v(partial: Partial<CdVersionRow> & { version: string; kind: string }): CdVersionRow {
  return {
    system: "prism",
    commit_sha: null,
    image_tag: null,
    chart: null,
    released_from: null,
    merged_at: null,
    created_at: "2026-10-12T00:00:00Z",
    prs: [],
    ...partial,
  };
}

const merge = (version: string, merged_at: string, pr: number) =>
  v({ version, kind: "merge", merged_at, prs: [{ repo: "juspay/hyperswitch-prism", pr_number: pr, title: `pr ${pr}`, author: "a", merged_at }] });

describe("mergesSince", () => {
  const versions = [
    merge("9d8050b234", "2026-10-12T10:35:28Z", 2501),
    merge("8eaddb0b85", "2026-10-12T06:54:18Z", 2488),
    v({ version: "2026.10.09.1", kind: "release", released_from: "6aa4b50944", merged_at: "2026-10-09T14:00:00Z" }),
    merge("efdd705fea", "2026-10-10T09:36:40Z", 2490),
    merge("6aa4b50944", "2026-10-09T13:00:00Z", 2480),
    merge("d4e7e1cc8d", "2026-10-08T09:00:00Z", 2483),
  ];

  test("every merge newer than the release's base, newest first", () => {
    const got = mergesSince(versions, versions[2]);
    expect(got?.map((x) => x.version)).toEqual(["9d8050b234", "8eaddb0b85", "efdd705fea"]);
  });

  test("unknown when the live release names no base", () => {
    expect(mergesSince(versions, v({ version: "2026.09.28.1-hotfix1", kind: "release" }))).toBeNull();
  });

  test("unknown when the base is not a version we know", () => {
    expect(mergesSince(versions, v({ version: "2026.10.01.0", kind: "release", released_from: "0000000000" }))).toBeNull();
  });

  test("unknown with nothing live", () => {
    expect(mergesSince(versions, null)).toBeNull();
  });
});

describe("howOf", () => {
  test("a job's row names the job", () => {
    expect(howOf({ requested_by: "merge #2501", jenkins_build: "http://j/1", detail: {} })).toEqual({ kind: "job", by: "merge #2501", build: "http://j/1" });
  });
  test("an observation says whether anyone asked", () => {
    expect(howOf({ requested_by: "observed", jenkins_build: null, detail: { out_of_band: true } })).toEqual({ kind: "observed", outOfBand: true });
    expect(howOf({ requested_by: "observed", jenkins_build: null, detail: { out_of_band: false } })).toEqual({ kind: "observed", outOfBand: false });
    expect(howOf({ requested_by: "observed", jenkins_build: null, detail: {} })).toEqual({ kind: "observed", outOfBand: false });
  });
  test("an arrival somebody asked for names them", () => {
    expect(howOf({ requested_by: "observed", jenkins_build: "http://j/5", detail: { out_of_band: false, asked_by: "merge #2501" } })).toEqual({ kind: "job", by: "merge #2501", build: "http://j/5" });
    expect(howOf({ requested_by: "observed", jenkins_build: null, detail: { out_of_band: false, asked_by: null } })).toEqual({ kind: "observed", outOfBand: false });
  });
});

describe("argoOf", () => {
  test("only an observation row carries Argo's status", () => {
    expect(argoOf({ detail: { sync: "Synced", health: "Healthy" } })).toEqual({ sync: "Synced", health: "Healthy" });
    expect(argoOf({ detail: { commit: "abc" } })).toBeNull();
    expect(argoOf(null)).toBeNull();
  });
});

describe("carries / placesOf / tone / ago", () => {
  test("a merge is its pull request, a release a count", () => {
    expect(carries(merge("8eaddb0b85", "2026-10-12T06:54:18Z", 2488))).toBe("#2488 pr 2488");
    expect(carries(v({ version: "2026.10.12.0", kind: "release", released_from: "8eaddb0b85", prs: [{ repo: "r", pr_number: 1, title: null, author: null, merged_at: null }] }))).toBe("1 pull request, from 8eaddb0b85");
    expect(carries(v({ version: "x", kind: "merge" }))).toBe("—");
    expect(carries(v({ version: "2026.09.28.1-hotfix1", kind: "release" }))).toBe("contents not recorded");
  });

  test("only the observed live version is highlighted across deployment transitions", () => {
    const row = (id: number, version: string, state: string) => ({
      id, system: "prism", environment: "integ", version, state, requested_by: "pipeline", request_id: null,
      jenkins_build: null, detail: {}, at: `2026-10-1${id}T00:00:00Z`,
    });
    const place = (rows: { id: number; system: string; environment: string; version: string; state: string; requested_by: string; request_id: null; jenkins_build: null; detail: {}; at: string }[], liveVersion: string, version: string) =>
      placesOf(rows.filter((r) => r.version === version), ["integ"], [{ environment: "integ", version: liveVersion }])[0];

    const aLive = [row(1, "A", "live")];
    expect(place([...aLive, row(2, "B", "live")], "B", "A")).toMatchObject({ state: "live", present: false, lastConfirmed: false });
    expect(place([...aLive, row(2, "B", "syncing")], "A", "A")).toMatchObject({ present: true, lastConfirmed: true });
    expect(place([row(2, "A", "failed"), ...aLive], "A", "A")).toMatchObject({ state: "failed", present: true, lastConfirmed: true });
    expect(place([row(2, "A", "rolled_back")], "A", "A")).toMatchObject({ present: true, lastConfirmed: true });
  });

  test("tones", () => {
    expect(stateTone("live")).toBe("ok");
    expect(stateTone("failed")).toBe("failed");
    expect(stateTone("syncing")).toBe("pending");
    expect(stateTone("rolled_back")).toBe("inconclusive");
  });

  test("ago", () => {
    const now = Date.parse("2026-10-12T12:00:00Z");
    expect(ago("2026-10-12T11:59:40Z", now)).toBe("just now");
    expect(ago("2026-10-12T11:19:00Z", now)).toBe("41 min");
    expect(ago("2026-10-12T07:00:00Z", now)).toBe("5 h");
    expect(ago("2026-10-09T12:00:00Z", now)).toBe("3 days");
    expect(agoText("2026-10-12T11:59:40Z", now)).toBe("just now");
    expect(agoText("2026-10-12T07:00:00Z", now)).toBe("5 h ago");
  });
});

describe("promotionReplayWord", () => {
  const h = (p: { state: string; verdict?: string | null; run_state?: string | null; run_delta_verdict?: string | null }) => ({
    verdict: null,
    run_state: null,
    run_delta_verdict: null,
    ...p,
  });
  test("the candidate run's own word is current and wins", () => {
    expect(promotionReplayWord(h({ state: "done", verdict: "fail", run_delta_verdict: "acknowledged" }))).toEqual({ word: "acknowledged", tone: "ok" });
    expect(promotionReplayWord(h({ state: "done", verdict: "pass", run_delta_verdict: "fail" }))).toEqual({ word: "fail", tone: "failed" });
  });
  test("a promotion replay still requested is running, unless its run died", () => {
    expect(promotionReplayWord(h({ state: "requested", run_state: "running" }))).toEqual({ word: "running", tone: "pending" });
    expect(promotionReplayWord(h({ state: "requested" }))).toEqual({ word: "running", tone: "pending" });
    expect(promotionReplayWord(h({ state: "requested", run_state: "failed" }))).toEqual({ word: "failed", tone: "failed" });
  });
  test("skipped and failed say so", () => {
    expect(promotionReplayWord(h({ state: "skipped" }))).toEqual({ word: "skipped", tone: "muted" });
    expect(promotionReplayWord(h({ state: "failed" }))).toEqual({ word: "failed", tone: "failed" });
  });
  test("a replay that failed, was skipped or is still running never borrows the candidate run's verdict", () => {
    expect(promotionReplayWord(h({ state: "failed", verdict: "unknown", run_delta_verdict: "pass" }))).toEqual({ word: "failed", tone: "failed" });
    expect(promotionReplayWord(h({ state: "skipped", run_delta_verdict: "pass" }))).toEqual({ word: "skipped", tone: "muted" });
    expect(promotionReplayWord(h({ state: "requested", run_delta_verdict: "pass" }))).toEqual({ word: "running", tone: "pending" });
    expect(promotionReplayWord(h({ state: "requested", run_state: "failed", run_delta_verdict: "pass" }))).toEqual({ word: "failed", tone: "failed" });
  });
  test("a finished replay whose run was re-pointed keeps its recorded verdict (the store sends no live one)", () => {
    expect(promotionReplayWord(h({ state: "done", verdict: "fail", run_delta_verdict: null }))).toEqual({ word: "fail", tone: "failed" });
  });
  test("a done promotion replay without a run word falls back to what the job read", () => {
    expect(promotionReplayWord(h({ state: "done", verdict: "pass", run_delta_verdict: "pending" }))).toEqual({ word: "pass", tone: "ok" });
    expect(promotionReplayWord(h({ state: "done", verdict: "inconclusive" }))).toEqual({ word: "inconclusive", tone: "inconclusive" });
    expect(promotionReplayWord(h({ state: "done" }))).toEqual({ word: "unknown", tone: "muted" });
  });
});

describe("promotion", () => {
  const w = (environment: string, state: string, version: string) => ({ id: 1, system: "prism", environment, version, state, requested_by: "x", request_id: null, jenkins_build: null, detail: {}, at: "2026-10-12T00:00:00Z" });
  const rows = [
    { version: merge("9d8050b234", "2026-10-12T10:35:28Z", 2501), where: [w("integ", "committed", "9d8050b234")] },
    { version: merge("8eaddb0b85", "2026-10-12T06:54:18Z", 2488), where: [w("integ", "live", "8eaddb0b85")] },
    { version: v({ version: "2026.10.09.1", kind: "release", released_from: "6aa4b50944", merged_at: "2026-10-09T13:00:00Z" }), where: [w("sandbox", "live", "2026.10.09.1")] },
    { version: merge("efdd705fea", "2026-10-10T09:36:40Z", 2490), where: [w("integ", "live", "efdd705fea")] },
    { version: merge("6aa4b50944", "2026-10-09T13:00:00Z", 2480), where: [w("integ", "live", "6aa4b50944")] },
  ];
  test("candidates: merges that reached integ after what sandbox runs, newest first", () => {
    expect(promotionCandidates(rows, "integ", rows[2].version).map((x) => x.version)).toEqual(["9d8050b234", "8eaddb0b85", "efdd705fea"]);
  });
  test("candidates when sandbox's base is unknown: every merge that reached integ", () => {
    expect(promotionCandidates(rows, "integ", null).map((x) => x.version)).toEqual(["9d8050b234", "8eaddb0b85", "efdd705fea", "6aa4b50944"]);
  });
  test("progress and in flight", () => {
    expect(promotionProgress({ state: "built" }).map((s) => s.at)).toEqual(["done", "done", "done", "now", "todo", "todo"]);
    expect(promotionProgress({ state: "live" }).every((s) => s.at === "done")).toBe(true);
    expect(inFlight({ state: "picked" })).toBe(true);
    expect(inFlight({ state: "cancelled" })).toBe(false);
  });
});
