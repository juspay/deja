import { describe, expect, it } from "vitest";
import { deltaUnavailable } from "./api";

describe("deltaUnavailable", () => {
  it("is pending only when the server says so", () => {
    expect(deltaUnavailable({ unavailable: "baseline still running", unavailable_kind: "pending" })).toEqual({
      why: "baseline still running",
      pending: true,
    });
  });

  it("reads a refusal, and an answer with no kind, as final", () => {
    expect(deltaUnavailable({ unavailable: "different tapes", unavailable_kind: "refused" })?.pending).toBe(false);
    expect(deltaUnavailable({ unavailable: "from an older server" })?.pending).toBe(false);
  });

  it("is nothing for a computed delta or no answer yet", () => {
    expect(deltaUnavailable(undefined)).toBeNull();
    expect(deltaUnavailable({ rows: [] } as never)).toBeNull();
  });
});
