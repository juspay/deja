import { describe, expect, it } from "vitest";
import { AvailableRecordingsPage } from "./api";
import { recommendedGroup } from "./recommendation";

const page = (
  state: AvailableRecordingsPage["recommendation_state"]["state"],
  recommended_group: string | null,
): AvailableRecordingsPage => ({
  recordings: [
    {
      recording_id: "locally-newest",
      dates: [],
      latest_date: null,
      objects: 1,
      pulled: false,
      identity: null,
      prefix: "local-prefix",
      group: "locally-newest",
    },
  ],
  total: 1,
  offset: 0,
  limit: 200,
  recommendation_state: { state, reason: state === "ready" ? null : "workload is unresolved" },
  recommended_group,
});

describe("recommendedGroup", () => {
  it("uses the server's ready recommendation, not bucket row order", () => {
    expect(recommendedGroup(page("ready", "server-group"))).toBe("server-group");
  });

  it.each(["unavailable", "ambiguous"] as const)("does not choose a local default when %s", (state) => {
    expect(recommendedGroup(page(state, null))).toBeNull();
  });

  it.each([
    ["current deployment", "sha-current-1010"],
    ["rollback deployment", "sha-active-older"],
  ] as const)("uses the active-workload recommendation for a %s", (_case, group) => {
    expect(recommendedGroup(page("ready", group))).toBe(group);
  });

  it("does not fall back to a locally newest group during rollback resolution", () => {
    expect(recommendedGroup(page("unavailable", null))).toBeNull();
    expect(recommendedGroup(page("ambiguous", null))).toBeNull();
  });

  it("supports Prism recommendations using the same system-independent response contract", () => {
    expect(recommendedGroup(page("ready", "prism-deployment-day"))).toBe("prism-deployment-day");
  });

  it("does not infer a group from rows when a ready answer has no group", () => {
    expect(recommendedGroup(page("ready", null))).toBeNull();
  });

  it("does not recommend before the response arrives", () => {
    expect(recommendedGroup(undefined)).toBeNull();
  });
});
