import { GraphNode } from "./api";

// Deja's record-mode ingress wrapper span. Replay mode never enters it, so it
// must not participate in the record-vs-replay span-name merge (same
// asymmetry the rank-2 address trim fixes in deja-runtime).
const INGRESS_WRAPPER = "deja::http_incoming";

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
