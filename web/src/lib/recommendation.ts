import { AvailableRecordingsPage } from "./api";

/** The server's recommendation is the only automatic default for a run. */
export function recommendedGroup(page: AvailableRecordingsPage | undefined): string | null {
  if (page?.recommendation_state.state !== "ready") return null;
  return page.recommended_group;
}
