import { useSearchParams } from "react-router-dom";

// `?debug=1` reveals the raw material: logs, stages, artifacts, the execution
// graph, the trigger curl.
//
// READ FROM THE URL ONLY — deliberately not localStorage. A sticky flag means a
// user who typed `?debug=1` once sees a different application forever, and can
// no longer reproduce what a colleague is looking at. A URL is shareable and
// self-describing; a stored flag is neither.
//
// It never hides evidence: the scorecard, the divergence list and the trust
// counters are in the report for everyone. Debug only adds raw sources.
export function useDebug(): boolean {
  const [params] = useSearchParams();
  return params.get("debug") === "1";
}

/** Preserve `?debug=1` across an internal link. */
export function withDebug(path: string, debug: boolean): string {
  if (!debug) return path;
  return path + (path.includes("?") ? "&" : "?") + "debug=1";
}
