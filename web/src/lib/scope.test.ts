import { describe, expect, test } from "vitest";
import { MERGE_QUEUE, changeOf, reposOf, scopesOf } from "./scope";
import type { RunSummaryRow } from "./api";

const run = (run_id: string, params: Record<string, unknown>): RunSummaryRow =>
  ({ run_id, params: { candidate_spec: { kind: "prebuilt_image", image: run_id }, ...params } }) as unknown as RunSummaryRow;

const gh = (n: number) => ({ repo: "juspay/hyperswitch-prism", pr_number: n, head_sha: "h" });

describe("scopesOf", () => {
  const runs = [
    run("pr", { system_under_test: "prism", label: "PR #2520 · fix", github: gh(2520), delta_against: "pr-base" }),
    run("pr-base", { system_under_test: "prism", purpose: "baseline", label: "main at 5beabfb324, merge-base of PR #2520" }),
    run("mq", { system_under_test: "prism", label: "MQ · PR #2432 · fix", github: gh(2432), delta_against: "mq-base" }),
    run("mq-base", { system_under_test: "prism", purpose: "baseline", label: "main at 090efd2e18, queue base of PR #2432" }),
    run("hs", { candidate_repo: "juspay/hyperswitch" }),
  ];
  const s = scopesOf(runs, "hyperswitch");

  test("a queue run and the baseline it named go to the merge queue", () => {
    expect(s.get("mq")).toBe(MERGE_QUEUE);
    expect(s.get("mq-base")).toBe(MERGE_QUEUE);
  });
  test("a PR run and its baseline stay with their system", () => {
    expect(s.get("pr")).toBe("prism");
    expect(s.get("pr-base")).toBe("prism");
  });
  test("a run that names no system lands on the default", () => {
    expect(s.get("hs")).toBe("hyperswitch");
  });
  test("a baseline shared with a plain PR run stays on the system", () => {
    const shared = scopesOf([...runs, run("pr2", { system_under_test: "prism", label: "PR #9", delta_against: "mq-base" })], "hyperswitch");
    expect(shared.get("mq-base")).toBe("prism");
    expect(shared.get("mq")).toBe(MERGE_QUEUE);
  });
});

test("reposOf names each system by the repository its runs build from", () => {
  const r = reposOf([run("a", { system_under_test: "prism", github: gh(1) }), run("b", { candidate_repo: "juspay/hyperswitch" })], "hyperswitch");
  expect(r.get("prism")).toBe("hyperswitch-prism");
  expect(r.get("hyperswitch")).toBe("hyperswitch");
});

test("changeOf shows the label, or the PR number when there is no label", () => {
  expect(changeOf(run("a", { label: "PR #2520 · fix" }))).toBe("PR #2520 · fix");
  expect(changeOf(run("b", { github: gh(7) }))).toBe("PR #7");
  expect(changeOf(run("c", {}))).toBeNull();
});
