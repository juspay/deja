import { GraphNode } from "./api";

// Deja's record-mode ingress wrapper span. Replay mode never enters it, so it
// must not participate in the record-vs-replay span-name merge (same
// asymmetry the rank-2 address trim fixes in deja-runtime).
const INGRESS_WRAPPER = "deja::http_incoming";

// Trace/debug spans are runtime plumbing (tokio codec polls, connection-pool
// frames), not application structure. Record environments often capture them
// while replay routers run at INFO — so they render as thousands of
// meaningless record-only "skipped" rows that bury the real divergences.
const INTERNAL_LEVELS = new Set(["TRACE", "DEBUG"]);

export type FilteredGraph = {
  nodes: GraphNode[];
  // Map any original node id to its nearest KEPT self-or-ancestor, so
  // divergence badges anchored to a dropped span re-attach to the visible
  // tree instead of vanishing.
  resolve: (id: number | undefined) => number | undefined;
};

export function keepAll(nodes: GraphNode[]): FilteredGraph {
  return { nodes, resolve: (id) => id };
}

// Drop trace/debug spans, re-parenting children (transitively) to the nearest
// kept ancestor.
export function dropInternalSpans(nodes: GraphNode[]): FilteredGraph {
  const dropped = new Set<number>();
  const parentOf = new Map<number, number | null>();
  for (const n of nodes) {
    parentOf.set(n.node_id, n.parent_id);
    if (INTERNAL_LEVELS.has((n.level ?? "").toUpperCase())) dropped.add(n.node_id);
  }
  if (dropped.size === 0) return keepAll(nodes);
  const nearestKept = (id: number | null | undefined): number | undefined => {
    let cur: number | null | undefined = id;
    while (cur != null && dropped.has(cur)) cur = parentOf.get(cur) ?? null;
    return cur ?? undefined;
  };
  const kept = nodes
    .filter((n) => !dropped.has(n.node_id))
    .map((n) =>
      n.parent_id != null && dropped.has(n.parent_id)
        ? { ...n, parent_id: nearestKept(n.parent_id) ?? null }
        : n,
    );
  return {
    nodes: kept,
    resolve: (id) => (id == null ? undefined : dropped.has(id) ? nearestKept(id) : id),
  };
}

// Drop health-probe request trees (kube-probe /health hits): the replay
// kernel filters health correlations from the driven set, so their record
// and replay trees are pure harness noise that also skews root pairing.
export function dropHealthTrees(nodes: GraphNode[]): GraphNode[] {
  const doomed = new Set<number>();
  for (const n of nodes) {
    const route = n.fields?.["http.route"];
    if (typeof route === "string" && route.replace(/\/+$/, "") === "/health") {
      doomed.add(n.node_id);
    }
  }
  if (doomed.size === 0) return nodes;
  // Remove the flagged roots and every descendant (transitively).
  const parentOf = new Map<number, number | null>();
  for (const n of nodes) parentOf.set(n.node_id, n.parent_id);
  const underDoomed = (id: number): boolean => {
    let cur: number | null | undefined = id;
    while (cur != null) {
      if (doomed.has(cur)) return true;
      cur = parentOf.get(cur) ?? null;
    }
    return false;
  };
  return nodes.filter((n) => !underDoomed(n.node_id));
}

// Remove wrapper nodes, re-parenting their children to the wrapper's parent
// (usually none -> the child becomes a root, aligning with the replay tree's
// "HTTP request" roots).
export function unwrapIngress(nodes: GraphNode[]): GraphNode[] {
  const wrappers = new Map<number, number | null>();
  for (const n of nodes) {
    if (n.span_name === INGRESS_WRAPPER) wrappers.set(n.node_id, n.parent_id);
  }
  if (wrappers.size === 0) return nodes;
  return nodes
    .filter((n) => !wrappers.has(n.node_id))
    .map((n) =>
      n.parent_id != null && wrappers.has(n.parent_id)
        ? { ...n, parent_id: wrappers.get(n.parent_id)! }
        : n,
    );
}
