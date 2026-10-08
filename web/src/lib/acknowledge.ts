// Acknowledging a divergence: the client-side helpers shared by the report
// header's button, the one-line summary in the delta panel, and the
// acknowledge page. The server decides everything (which rows may be
// proposed, who may confirm, the effective verdict); these only group rows
// for display and call the four endpoints.

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ApiError, Delta, DeltaAddress, DeltaRow, EffectiveVerdict, actor, api, deltaFamily } from "./api";

/** The server's key, recomputed only to group rows for display. */
export function shapeOf(a: DeltaAddress): string {
  switch (a.kind) {
    case "call":
      return `call ${a.boundary} ${a.operation} ${a.span_path}`;
    case "status":
      return "status";
    case "body":
      return `body ${a.json_path}`;
  }
}

export function describe(a: DeltaAddress): { what: string; where: string } {
  switch (a.kind) {
    case "call": {
      const tail = a.span_path.split(">").slice(-2).join(" › ");
      return { what: `${a.operation} on ${a.boundary}`, where: tail };
    }
    case "status":
      return { what: "response status", where: "" };
    case "body":
      return { what: "response field", where: a.json_path };
  }
}

export type Group = {
  key: string;
  lane: string;
  what: string;
  where: string;
  rows: DeltaRow[];
  family: "introduced" | "changed";
  ack: DeltaRow["acknowledgement"] | null;
  /** No row in the group still carries the value that was acknowledged. A
   *  call's value differs per request by nature, so one matching row means
   *  the shape is the one accepted. */
  valueChanged: boolean;
};

export function groupCharged(d: Delta): Group[] {
  const by = new Map<string, Group>();
  for (const r of d.rows) {
    const family = deltaFamily(r.bucket);
    if (family !== "introduced" && family !== "changed") continue;
    const lane = r.lane ? `${r.lane.connector} · ${r.lane.flow}` : "no connector call";
    const key = `${lane} | ${shapeOf(r.address)}`;
    const g = by.get(key);
    if (g) {
      g.rows.push(r);
      g.valueChanged &&= !!r.acknowledgement?.value_changed;
      continue;
    }
    const { what, where } = describe(r.address);
    by.set(key, {
      key,
      lane,
      what,
      where,
      rows: [r],
      family,
      ack: r.acknowledgement ?? null,
      valueChanged: !!r.acknowledgement?.value_changed,
    });
  }
  return [...by.values()].sort((a, b) => b.rows.length - a.rows.length || a.key.localeCompare(b.key));
}

export function effectiveOf(d: Delta): EffectiveVerdict {
  return d.verdict.effective ?? (d.verdict.pass ? "pass" : "fail");
}

export function requestsOf(groups: Group[]): number {
  return new Set(groups.flatMap((g) => g.rows.map((r) => r.address.correlation))).size;
}

/** What the button on the report says, for the person looking at it. */
export function badgeOf(groups: Group[], d: Delta, me: string): { label: string; tone: "ok" | "todo" | "wait" } | null {
  if (groups.length === 0) return null;
  if (effectiveOf(d) === "acknowledged") return { label: "all acknowledged", tone: "ok" };
  const mine = (g: Group) => !!g.ack && g.ack.by.trim().toLowerCase() === me.trim().toLowerCase();
  const toConfirm = groups.filter((g) => g.ack?.state === "proposed" && !mine(g)).length;
  const toMark = groups.filter((g) => !g.ack || g.ack.state === "stale").length;
  if (toConfirm > 0) return { label: `${toConfirm} to acknowledge`, tone: "todo" };
  if (toMark > 0) return { label: `${toMark} to mark`, tone: "todo" };
  return { label: "waiting for a second person", tone: "wait" };
}

/** The pull request's acknowledgements; `null` data when the run names no
 *  pull request (the endpoint answers 404). */
export function useAcknowledgements(runId: string) {
  return useQuery({
    queryKey: ["acks", runId],
    queryFn: async () => {
      try {
        return await api.acknowledgements(runId);
      } catch (e) {
        if ((e as ApiError).status === 404) return null;
        throw e;
      }
    },
  });
}

export function useAckActions(runId: string, against: string, onError: (msg: string) => void, onDone?: () => void) {
  const qc = useQueryClient();
  const refresh = () => {
    qc.invalidateQueries({ queryKey: ["delta", runId, against] });
    qc.invalidateQueries({ queryKey: ["acks", runId] });
    qc.invalidateQueries({ queryKey: ["runs"] });
    qc.invalidateQueries({ queryKey: ["run", runId] });
    onDone?.();
  };
  const fail = (e: unknown) => onError(String((e as Error).message));
  const propose = useMutation({
    mutationFn: ({ rows, note }: { rows: DeltaAddress[]; note: string }) => api.proposeAcknowledgements(runId, rows, note),
    onSuccess: refresh,
    onError: fail,
  });
  const confirm = useMutation({ mutationFn: (id: number) => api.confirmAcknowledgement(id), onSuccess: refresh, onError: fail });
  const withdraw = useMutation({ mutationFn: (id: number) => api.withdrawAcknowledgement(id), onSuccess: refresh, onError: fail });
  return { propose, confirm, withdraw, me: actor() };
}
