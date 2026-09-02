import { useQuery } from "@tanstack/react-query";

import { api, type SystemRow } from "./api";

/**
 * The systems this orchestrator can replay, as the deployment declared them.
 *
 * The list used to live in this app, three times over: a two-option `<select>`,
 * a `"" | "prism"` state type that could not represent a third system, and a
 * comparison against the literal `"prism"` that decided which span namespaces a
 * run scored. Adding a system therefore meant editing the browser as well as the
 * orchestrator, in a language whose compiler enforced the omission.
 *
 * None of that is the client's to know. The orchestrator resolves each system
 * from the environment and reports the result; this reads it.
 *
 * Cached for the session: the registry is deployment configuration and does not
 * change while a page is open, so refetching it per view would be noise.
 */
export function useSystems() {
  const query = useQuery({
    queryKey: ["systems"],
    queryFn: () => api.systems(),
    staleTime: Infinity,
  });

  const all: SystemRow[] = query.data?.systems ?? [];
  // A system that cannot run is not a choice. It is still worth reporting
  // elsewhere — `configured: false` says which of its causes applies — but
  // offering it in a picker would produce a run that fails at launch.
  const selectable = all.filter((s) => s.configured && !s.error);
  const fallback = selectable[0] ?? all[0];

  return {
    ...query,
    all,
    selectable,
    /** The system a caller gets by naming nothing. Undefined until loaded — a
     *  caller that needs a name should wait rather than guess one. */
    defaultSystem: all.find((s) => s.is_default) ?? fallback,
    /** Whether `name` is the default, without this app knowing which name that
     *  is. Absent (a legacy row, or a caller that named nothing) counts as the
     *  default, which is exactly what the wire contract says. */
    isDefault: (name?: string | null) =>
      !name || all.some((s) => s.is_default && s.name === name),
  };
}
