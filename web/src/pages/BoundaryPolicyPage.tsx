import React from "react";
import { Link, useParams } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  api, boundaryPolicyApi, type BoundaryPolicyPreview,
  type BoundaryRule, type NoiseTarget,
} from "../lib/api";
import { useMe } from "../lib/me";
import { useSystems } from "../lib/systems";

type TargetKind = NoiseTarget["kind"];

function ruleDescription(rule: BoundaryRule): string {
  const source = rule.source_path ? `decoded ${rule.source_path}` : "arguments";
  const target = rule.target.kind === "header_value"
    ? `header ${rule.target.name} at ${rule.target.json_pointer}`
    : `${rule.target.kind.replace("_", " ")} at ${rule.target.json_pointer}`;
  const { boundary, trait_name, method_name } = rule.selector;
  return `${boundary} / ${trait_name} / ${method_name} · ${source} · ${target}`;
}

function sameRule(a: BoundaryRule, b: BoundaryRule): boolean {
  return a.selector.boundary === b.selector.boundary
    && a.selector.trait_name === b.selector.trait_name
    && a.selector.method_name === b.selector.method_name
    && (a.source_path ?? null) === (b.source_path ?? null)
    && a.target.kind === b.target.kind
    && a.target.json_pointer === b.target.json_pointer
    && (a.target.kind !== "header_value" || (b.target.kind === "header_value" && a.target.name.toLowerCase() === b.target.name.toLowerCase()));
}

export default function BoundaryPolicyPage() {
  const { system = "" } = useParams();
  const me = useMe();
  const systems = useSystems();
  const client = useQueryClient();
  const allowed = !!me.data?.authenticated && (me.data.roles ?? []).includes("maintainer");
  const policy = useQuery({
    queryKey: ["boundary-policy", system],
    queryFn: () => boundaryPolicyApi.get(system),
    enabled: !!system && allowed,
  });
  const runs = useQuery({ queryKey: ["runs", system], queryFn: api.runs, enabled: !!system && allowed });
  const activeId = policy.data?.active?.id ?? null;
  const activeRules = policy.data?.active?.snapshot.rules ?? [];
  const [draft, setDraft] = React.useState<BoundaryRule[]>([]);
  const [boundary, setBoundary] = React.useState("");
  const [traitName, setTraitName] = React.useState("");
  const [methodName, setMethodName] = React.useState("");
  const [sourcePath, setSourcePath] = React.useState("");
  const [targetKind, setTargetKind] = React.useState<TargetKind>("scalar");
  const [jsonPointer, setJsonPointer] = React.useState("");
  const [headerName, setHeaderName] = React.useState("");
  const [reason, setReason] = React.useState("");
  const [runId, setRunId] = React.useState("");
  const [preview, setPreview] = React.useState<BoundaryPolicyPreview | null>(null);
  const [message, setMessage] = React.useState("");
  const additions = draft.filter((rule) => !activeRules.some((active) => sameRule(rule, active)));
  const removals = activeRules.filter((rule) => !draft.some((item) => sameRule(rule, item)));
  const action = additions.length === 1 && removals.length === 0 ? "append"
    : additions.length === 0 && removals.length === 1 ? "revoke" : null;
  const changedRule = action === "append" ? additions[0] : action === "revoke" ? removals[0] : null;

  // A revision is immutable; editing always starts from the current active
  // snapshot. A new activation refreshes the draft instead of silently
  // carrying old allowances into a future proposal.
  React.useEffect(() => {
    setDraft(policy.data?.active?.snapshot.rules ?? []);
    setPreview(null);
  }, [system, activeId]);

  const previewMutation = useMutation({
    mutationFn: () => boundaryPolicyApi.preview(system, runId, draft),
    onSuccess: setPreview,
    onError: (error) => setMessage(String(error)),
  });
  const proposeMutation = useMutation({
    mutationFn: () => {
      if (!action || !changedRule) throw new Error("Propose one appended or revoked rule at a time.");
      return boundaryPolicyApi.propose(system, action, changedRule, reason.trim(), activeId);
    },
    onSuccess: async () => {
      setMessage("One-rule change proposed. Another signed-in maintainer must confirm it.");
      setReason("");
      await client.invalidateQueries({ queryKey: ["boundary-policy", system] });
    },
    onError: (error) => setMessage(`Proposal refused: ${String(error)}`),
  });
  const activateMutation = useMutation({
    mutationFn: (id: number) => boundaryPolicyApi.activate(system, id, activeId),
    onSuccess: async () => {
      setMessage("Allowance change activated for future replays; existing run snapshots are unchanged.");
      await client.invalidateQueries({ queryKey: ["boundary-policy", system] });
    },
    onError: (error) => setMessage(`Activation refused; refresh if the active revision changed: ${String(error)}`),
  });

  const addRule = () => {
    if (action) {
      setMessage("Propose or cancel the current single-rule change before adding another.");
      return;
    }
    if (!boundary.trim() || !traitName.trim() || !methodName.trim() || !jsonPointer.startsWith("/")) {
      setMessage("Provide an exact boundary, trait, method, and a JSON Pointer beginning with /.");
      return;
    }
    if (sourcePath && !sourcePath.startsWith("/")) {
      setMessage("Encoded source path must be a JSON Pointer beginning with /.");
      return;
    }
    if (targetKind === "header_value" && !headerName.trim()) {
      setMessage("Name the one header value to approve as noise.");
      return;
    }
    const target: NoiseTarget = targetKind === "scalar"
      ? { kind: "scalar", json_pointer: jsonPointer.trim() }
      : targetKind === "header_group"
        ? { kind: "header_group", json_pointer: jsonPointer.trim() }
        : { kind: "header_value", json_pointer: jsonPointer.trim(), name: headerName.trim() };
    const next: BoundaryRule = {
      selector: { boundary: boundary.trim(), trait_name: traitName.trim(), method_name: methodName.trim() },
      ...(sourcePath.trim() ? { source_path: sourcePath.trim() } : {}),
      target,
    };
    if (draft.some((rule) => sameRule(rule, next))) {
      setMessage("This exact allowance is already in the effective set or draft.");
      return;
    }
    setDraft((previous) => [...previous, next]);
    setPreview(null);
    setMessage("");
  };

  if (me.isLoading || systems.isLoading) return <p className="hint">Loading…</p>;
  if (!allowed) return <><h1>Boundary noise policy</h1><p className="err">Maintainer sign-in is required.</p></>;
  if (!systems.all.some((item) => item.name === system)) return <><h1>Boundary noise policy</h1><p className="err">Unknown system.</p></>;
  if (policy.isLoading) return <p className="hint">Loading policy…</p>;
  if (policy.error) return <><h1>Boundary noise policy</h1><p className="err">{String(policy.error)}</p></>;
  const runOptions = (runs.data ?? []).filter((run) => run.mode === "replay"
    && (run.params.system_under_test ?? systems.defaultSystem?.name) === system);

  return <>
    <h1>Maintainer replay allowances · {system}</h1>
    <p className="hint">Semantic JSON decoding is automatic. Append one exact noise allowance, or revoke one once its cause is fixed. Old runs keep the rules they were scored with.</p>
    <p role="note"><strong>Safety:</strong> approving a scalar or header value is a maintainer declaration, not proof that a signature is valid. Every other argument remains compared. Raw captured arguments stay in the call ledger, not this preview.</p>
    <section>
      <h2>Effective allowances</h2>
      {policy.data?.active ? <>
        <p>Last change #{policy.data.active.id} · digest <code>{policy.data.active.snapshot.digest}</code></p>
        <p>{policy.data.active.reason}</p>
        {policy.data.active.snapshot.rules.length === 0
          ? <p>No approved noise.</p>
          : <ul>{policy.data.active.snapshot.rules.map((rule, index) => <li key={index}>{ruleDescription(rule)}</li>)}</ul>}
      </> : <p>No allowances appended. Existing replay admission remains unchanged.</p>}
    </section>
    <section>
      <h2>Append or revoke one allowance</h2>
      <p className="hint">Each action proposes one immutable, audited change. A different maintainer confirms it. The effective set changes only after confirmation; old runs retain their pinned rules.</p>
      <label>Boundary <input type="text" value={boundary} onChange={(event) => setBoundary(event.target.value)} placeholder="exact boundary name" /></label>{" "}
      <label>Trait <input type="text" value={traitName} onChange={(event) => setTraitName(event.target.value)} placeholder="exact trait name" /></label>{" "}
      <label>Method <input type="text" value={methodName} onChange={(event) => setMethodName(event.target.value)} placeholder="exact method name" /></label>
      <p><label>Encoded JSON source (optional) <input type="text" value={sourcePath} onChange={(event) => setSourcePath(event.target.value)} placeholder="/request/data" /></label></p>
      <label>Approved difference <select value={targetKind} onChange={(event) => setTargetKind(event.target.value as TargetKind)}>
        <option value="scalar">One scalar value</option>
        <option value="header_group">Header-name group presentation</option>
        <option value="header_value">One named header value</option>
      </select></label>{" "}
      <label>JSON Pointer <input type="text" value={jsonPointer} onChange={(event) => setJsonPointer(event.target.value)} placeholder="/headers" /></label>{" "}
      {targetKind === "header_value" && <label>Header name <input type="text" value={headerName} onChange={(event) => setHeaderName(event.target.value)} /></label>}
      <button className="btn" onClick={addRule} disabled={action !== null}>Add one allowance</button>
      <h3>Proposed effective rules</h3>
      {draft.length === 0 ? <p>No approved noise.</p> :
        <ul>{draft.map((rule, index) => <li key={index}>{ruleDescription(rule)}{" "}
          <button className="btn" disabled={action !== null && (action !== "append" || !changedRule || !sameRule(rule, changedRule))}
            onClick={() => { setDraft((items) => items.filter((_, i) => i !== index)); setPreview(null); }}>
            {activeRules.some((active) => sameRule(active, rule)) ? "Revoke for future runs" : "Cancel addition"}
          </button>
        </li>)}</ul>}
      {action && <button className="btn" onClick={() => {
        setDraft(activeRules);
        setPreview(null);
        setMessage("");
      }}>Cancel draft change</button>}
      <label htmlFor="noise-reason">Reason for this one-rule change</label>
      <textarea id="noise-reason" value={reason} onChange={(event) => setReason(event.target.value)} />
      <label htmlFor="noise-preview-run">Preview against a replay run</label>
      <select id="noise-preview-run" value={runId} onChange={(event) => { setRunId(event.target.value); setPreview(null); }}>
        <option value="">Choose run</option>
        {runOptions.map((run) => <option key={run.run_id} value={run.run_id}>{run.run_id} · {run.recording_id ?? "no recording"}</option>)}
      </select>{" "}
      <button className="btn" disabled={!runId || previewMutation.isPending} onClick={() => { setMessage(""); previewMutation.mutate(); }}>Preview draft</button>
      {runs.error && <p role="alert" className="err">Could not load replay runs: {String(runs.error)}</p>}
      {preview && <div aria-live="polite">
        <p>Historical run <code>{preview.run_id}</code>: {preview.total_rows} differing argument pairs; {preview.would_absorb} would be treated as equivalent; {preview.would_remain_blocking} remain blocking under this draft.</p>
        <ul>{preview.examples.map((example, index) => <li key={`${example.correlation_id}-${index}`}>
          <code>{example.correlation_id ?? "uncorrelated"}</code> · {example.boundary}/{example.method_name}: {example.classification}
          {example.classification === "untrusted_pair" && " (unpaired or independently reordered)"}
        </li>)}</ul>
      </div>}
      <p><button className="btn" disabled={!action || !reason.trim() || proposeMutation.isPending}
        onClick={() => { setMessage(""); proposeMutation.mutate(); }}>
        {action === "append" ? "Propose append" : action === "revoke" ? "Propose revocation" : "Choose one change"}
      </button></p>
      {message && <p role="status">{message}</p>}
    </section>
    <section>
      <h2>Revision history</h2>
      {(policy.data?.revisions ?? []).length === 0 ? <p>No revisions.</p> : <table>
        <thead><tr><th>Revision</th><th>Status</th><th>Rules</th><th>Reason</th><th>Proposed by</th><th>Confirmed by</th><th>Action</th></tr></thead>
        <tbody>{policy.data?.revisions.map((revision) => <tr key={revision.id}>
          <td>{revision.id}</td>
          <td>{revision.id === activeId ? "active" : revision.activated_at ? "superseded" : revision.base_revision_id !== activeId ? "stale proposal" : "proposed"}</td>
          <td>{revision.snapshot.rules.length === 0 ? "none" : revision.snapshot.rules.map(ruleDescription).join("; ")}</td>
          <td>{revision.reason}</td>
          <td>{revision.proposed_by}</td>
          <td>{revision.confirmed_by ?? "—"}</td>
          <td>{!revision.activated_at && revision.base_revision_id === activeId && (
            revision.proposed_by === (me.data?.email ?? "")
              ? <span className="hint">Awaiting another maintainer</span>
              : <button className="btn" disabled={activateMutation.isPending}
                  onClick={() => { setMessage(""); activateMutation.mutate(revision.id); }}>
                  Confirm this change
                </button>
          )}</td>
        </tr>)}</tbody>
      </table>}
    </section>
    <p><Link to="/runs">Review runs</Link></p>
  </>;
}
