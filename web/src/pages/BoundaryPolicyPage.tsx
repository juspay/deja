import React from "react";
import { Link, useParams } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { BoundaryPolicyPreview, BoundaryPolicyRevision, BoundaryRule, BoundaryTreatment, boundaryPolicyApi, api } from "../lib/api";
import { useMe } from "../lib/me";
import { useSystems } from "../lib/systems";

const treatments: { value: BoundaryTreatment; label: string }[] = [
  { value: "plain_base64_json_order", label: "Equal parsed plain JSON (base64 JSON order)" },
  { value: "http_response_header_groups", label: "Unsigned HTTP response header groups" },
  { value: "signed_body_derived_signature", label: "Signature derived from changed signed-body bytes" },
];

export default function BoundaryPolicyPage() {
  const { system = "" } = useParams();
  const me = useMe();
  const systems = useSystems();
  const client = useQueryClient();
  const boundary = "km";
  const trait = "common_utils::keymanager";
  const method = "call_encryption_service";
  const [selected, setSelected] = React.useState<BoundaryTreatment[]>([]);
  const [reason, setReason] = React.useState("");
  const [runId, setRunId] = React.useState("");
  const [message, setMessage] = React.useState("");
  const [preview, setPreview] = React.useState<BoundaryPolicyPreview | null>(null);
  const allowed = !!me.data?.authenticated && (me.data.roles ?? []).includes("maintainer");
  const policy = useQuery({ queryKey: ["boundary-policy", system], queryFn: () => boundaryPolicyApi.get(system), enabled: !!system && allowed });
  const runs = useQuery({ queryKey: ["runs", system], queryFn: api.runs, enabled: !!system && allowed });
  const activeId = policy.data?.active?.id ?? null;
  const rules: BoundaryRule[] = selected.length ? [{ boundary, trait_name: trait, method_name: method, treatments: selected }] : [];
  const refresh = () => client.invalidateQueries({ queryKey: ["boundary-policy", system] });
  const previewMutation = useMutation({ mutationFn: () => boundaryPolicyApi.preview(system, runId, rules), onSuccess: setPreview, onError: (e) => setMessage(String(e)) });
  const proposeMutation = useMutation({ mutationFn: () => boundaryPolicyApi.propose(system, rules, reason.trim(), activeId), onSuccess: async () => { setMessage("Proposal created; a different maintainer must activate it."); setReason(""); await refresh(); }, onError: (e) => setMessage(`Conflict or proposal refused: ${String(e)}`) });
  const activateMutation = useMutation({ mutationFn: ({ revision, expected }: { revision: number; expected: number | null }) => boundaryPolicyApi.activate(system, revision, expected), onSuccess: async () => { setMessage("Policy activation recorded."); await refresh(); }, onError: (e) => setMessage(`Stale policy or activation refused: ${String(e)}`) });

  if (me.isLoading || systems.isLoading) return <p className="hint">loading…</p>;
  if (!allowed) return <><h1>Boundary policy</h1><p className="err">Maintainer sign-in is required.</p></>;
  if (!systems.all.some((s) => s.name === system)) return <><h1>Boundary policy</h1><p className="err">Unknown system.</p></>;
  if (policy.isLoading) return <p className="hint">loading policy…</p>;
  if (policy.error) return <><h1>Boundary policy</h1><p className="err">{String(policy.error)}</p></>;
  const runOptions = (runs.data ?? []).filter((r) => r.mode === "replay" && (r.params.system_under_test ?? systems.defaultSystem?.name) === system);
  const ownPending = (revision: BoundaryPolicyRevision) => revision.proposed_by === (me.data?.email ?? "");
  const pending = (policy.data?.revisions ?? []).filter((r) => !r.activated_at);
  return <>
    <h1>Maintainer boundary policy · {system}</h1>
    <p className="hint">Applies to new runs only. This is system-owned replay argument classification policy—not an author acknowledgement or PR approval. Policy activation affects future runs; it does not rewrite past runs.</p>
    <p role="note"><strong>Safety:</strong> signed-body-derived signature treatment declares the signature derived; it does not verify HMAC or other cryptographic authenticity. Preview replays historical run data for comparison; it is not a live simulation and reports only the counts and examples below.</p>
    <section>
      <h2>Effective policy</h2>
      {policy.data?.active ? <><p>Revision {policy.data.active.id} · digest <code>{policy.data.active.snapshot.digest}</code></p><p>{policy.data.active.reason}</p></> : <p>No active revision. New runs use strict boundary classification; no legacy policy is applied.</p>}
    </section>
    <section>
      <h2>Propose a revision</h2>
      <p><strong>Selector:</strong> boundary <code>{boundary}</code>, trait <code>{trait}</code>, method <code>{method}</code></p>
      <fieldset><legend>Typed treatments</legend>{treatments.map((t) => <label key={t.value} style={{ display: "block" }}><input type="checkbox" checked={selected.includes(t.value)} onChange={(e) => { setPreview(null); setSelected((old) => e.target.checked ? [...old, t.value] : old.filter((x) => x !== t.value)); }} /> {t.label}</label>)}</fieldset>
      <label htmlFor="policy-reason">Reason</label>
      <textarea id="policy-reason" value={reason} onChange={(e) => setReason(e.target.value)} />
      <label htmlFor="policy-run">Preview against replay run for {system}</label>
      <select id="policy-run" value={runId} onChange={(e) => { setRunId(e.target.value); setPreview(null); }}><option value="">Choose run</option>{runOptions.map((r) => <option key={r.run_id} value={r.run_id}>{r.run_id} · {r.recording_id ?? "no recording"}</option>)}</select>
      <button className="btn" disabled={!runId || previewMutation.isPending} onClick={() => { setMessage(""); previewMutation.mutate(); }}>Preview</button>
      {preview && <div aria-live="polite">
        <p>Historical run <code>{preview.run_id}</code>: {preview.total_km_rows} differing KM argument pairs; {preview.would_absorb} would be absorbed and {preview.would_remain_blocking} would remain blocking under this draft.</p>
        {preview.examples.length > 0 && <ul>{preview.examples.map((example, index) =>
          <li key={`${example.correlation_id ?? "uncorrelated"}-${index}`}><code>{example.correlation_id ?? "uncorrelated"}</code>: {example.classification}</li>
        )}</ul>}
      </div>}
      {runs.isError && <p role="alert" className="err">Could not load replay runs: {String(runs.error)}</p>}
      <p><button className="btn" disabled={!reason.trim() || proposeMutation.isPending} onClick={() => { setMessage(""); proposeMutation.mutate(); }}>Propose immutable revision{selected.length === 0 ? " (strict: no treatments)" : ""}</button></p>
      {message && <p role="status">{message}</p>}
    </section>
    <section>
      <h2>Revision history</h2>
      {(policy.data?.revisions ?? []).length === 0 ? <p>No revisions.</p> : (
        <table>
          <thead><tr><th>revision</th><th>status</th><th>treatments</th><th>digest</th><th>reason</th><th>proposed by</th><th>confirmed by</th><th>action</th></tr></thead>
          <tbody>{policy.data?.revisions.map((r) => (
            <tr key={r.id}>
              <td>{r.id}</td>
              <td>{r.id === activeId ? "active" : r.activated_at ? "superseded" : "proposed"}</td>
              <td>{r.snapshot.rules.flatMap((rule) => rule.treatments).join(", ") || "strict"}</td>
              <td><code title={r.snapshot.digest}>{r.snapshot.digest.slice(0, 21)}…</code></td>
              <td>{r.reason}</td>
              <td>{r.proposed_by}<br /><small className="hint">{new Date(r.created_at).toLocaleString()}</small></td>
              <td>{r.confirmed_by ?? "—"}{r.activated_at && <><br /><small className="hint">{new Date(r.activated_at).toLocaleString()}</small></>}</td>
              <td>{r.id !== activeId && (ownPending(r) ?
                <span className="hint">Awaiting a different maintainer</span> :
                <button className="btn" disabled={activateMutation.isPending} onClick={() => { setMessage(""); activateMutation.mutate({ revision: r.id, expected: activeId }); }}>
                  {r.activated_at ? "Rollback to revision" : "Confirm and activate"}
                </button>
              )}</td>
            </tr>
          ))}</tbody>
        </table>
      )}
    </section>
    {pending.length > 0 && <p className="hint">Pending proposals: {pending.map((r) => `#${r.id}`).join(", ")}. Activation is guarded by the current active revision; stale conflicts require refresh before retry.</p>}
    <p><Link to="/runs">Review runs</Link></p>
  </>;
}
