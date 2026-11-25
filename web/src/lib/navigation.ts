import { useEffect, useRef } from "react";
import { useLocation } from "react-router-dom";

export type ReviewNavigation = { runs: string; trail: string[] };
const origin = "https://deja.invalid";

function localPath(value: unknown, kind: "runs" | "report" | "review"): string | null {
  if (typeof value !== "string" || value.length > 12000 || !value.startsWith("/") || value.startsWith("//") || value.includes("\\")) return null;
  try {
    const url = new URL(value, origin);
    const valid = kind === "runs" ? url.pathname === "/runs"
      : kind === "report" ? /^\/r\/[\w.-]+$/.test(url.pathname)
        : /^\/r\/[\w.-]+(?:\/(?:delta|acknowledge))?$/.test(url.pathname);
    if (url.origin !== origin || !valid) return null;
    // Context is held once, not nested inside every previous report URL.
    url.searchParams.delete("nav");
    return `${url.pathname}${url.search}${url.hash}`;
  } catch { return null; }
}

export function readReviewNavigation(search: string, legacyState?: unknown): ReviewNavigation {
  try {
    const raw = new URLSearchParams(search).get("nav");
    if (raw && raw.length < 20000) {
      const parsed = JSON.parse(raw) as Partial<ReviewNavigation>;
      const runs = localPath(parsed.runs, "runs");
      if (runs && Array.isArray(parsed.trail) && parsed.trail.length <= 12) {
        const trail = parsed.trail.map((path) => localPath(path, "report"));
        if (trail.every((path): path is string => path !== null)) return { runs, trail };
      }
    }
  } catch { /* A malformed link is a fresh navigation, never a redirect. */ }
  const previous = legacyState as { runsUrl?: unknown } | null;
  return { runs: localPath(previous?.runsUrl, "runs") ?? "/runs", trail: [] };
}

export function reviewHref(target: string, current: string, context: ReviewNavigation): string {
  const cleanTarget = localPath(target, "review");
  if (!cleanTarget) return target;
  const trail = [...context.trail];
  const report = localPath(current, "report");
  if (report && trail.at(-1) !== report) trail.push(report);
  const targetIndex = trail.findIndex((path) => path === cleanTarget);
  const nextTrail = targetIndex >= 0 ? trail.slice(0, targetIndex) : trail.slice(-12);
  const url = new URL(cleanTarget, origin);
  url.searchParams.set("nav", JSON.stringify({ runs: context.runs, trail: nextTrail }));
  return `${url.pathname}${url.search}${url.hash}`;
}

export function useReviewNavigation() {
  const location = useLocation();
  const current = `${location.pathname}${location.search}${location.hash}`;
  const context = readReviewNavigation(location.search, location.state);
  const href = (target: string) => reviewHref(target, current, context);
  const currentReport = localPath(current, "report");
  const report = currentReport ?? context.trail.at(-1) ?? location.pathname.replace(/\/(?:delta|acknowledge)$/, "");
  const previous = context.trail.at(-1);
  return {
    href,
    runsHref: context.runs,
    reportHref: href(report),
    previousReportHref: previous && previous !== currentReport ? href(previous) : null,
  };
}

/** The row anchor survives sharing/reloading; session storage restores the
 * exact scroll position for a reader returning to their own filtered page. */
export function useRunListScroll(ready: boolean) {
  const location = useLocation();
  const key = `${location.pathname}${location.search}`;
  const restored = useRef<string | null>(null);
  useEffect(() => {
    const save = () => {
      // A navigation may clamp the new document before React runs cleanup.
      // Save while this history page owns the viewport, never on unmount.
      if (!ready || restored.current !== key || `${window.location.pathname}${window.location.search}` !== key) return;
      try { sessionStorage.setItem(`deja-runs-scroll:${key}`, String(window.scrollY)); } catch { /* Storage may be disabled. */ }
    };
    window.addEventListener("scroll", save, { passive: true });
    return () => window.removeEventListener("scroll", save);
  }, [key, ready]);
  useEffect(() => {
    if (!ready || restored.current === key) return;
    restored.current = key;
    let position: string | null = null;
    try { position = sessionStorage.getItem(`deja-runs-scroll:${key}`); } catch { /* Fall back to the shareable row anchor. */ }
    if (position !== null && Number.isFinite(Number(position))) window.scrollTo({ top: Math.max(0, Number(position)) });
    else if (location.hash) {
      try { document.getElementById(decodeURIComponent(location.hash.slice(1)))?.scrollIntoView({ block: "center" }); } catch { /* A malformed anchor is not executable navigation. */ }
    }
    else window.scrollTo({ top: 0 });
  }, [ready, key, location.hash]);
}
