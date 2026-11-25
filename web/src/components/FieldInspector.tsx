import React from "react";
import type { EvidenceField, EvidenceValue } from "../lib/fieldEvidence";
import { JsonView } from "./JsonView";

export function EvidenceValueView({ evidence }: { evidence: EvidenceValue }) {
  if (evidence.state === "missing") return <span className="hint">missing field</span>;
  if (evidence.state === "unavailable") return <span className="hint" title={evidence.reason}>unavailable · {evidence.reason}</span>;
  if (evidence.value === null) return <code>explicit null</code>;
  return <JsonView value={evidence.value} />;
}

/** A shared, selected-leaf three-way view; classifications remain upstream. */
export function FieldInspector({ fields, note, title = "Field evidence" }: { fields: EvidenceField[]; note: string; title?: string }) {
  const [selected, setSelected] = React.useState("");
  const field = fields.find((candidate) => candidate.path === selected) ?? fields[0];
  const id = React.useId();
  return <section className="field-inspector">
    <h4>{title}</h4>
    <p className="hint">{note}</p>
    {!field ? <p className="hint">No differing captured leaf is available for this observation. This does not override its published classification.</p> : <>
      <label htmlFor={id}>Inspect field </label>
      <select id={id} value={field.path} onChange={(event) => setSelected(event.target.value)}>
        {fields.map((candidate) => <option key={candidate.path} value={candidate.path}>{candidate.path}</option>)}
      </select>
      <table className="delta-vals"><thead><tr><th>recording</th><th>baseline</th><th>candidate</th></tr></thead>
        <tbody><tr>{(["recording", "main", "pr"] as const).map((side) => <td key={side}><EvidenceValueView evidence={field[side]} /></td>)}</tr></tbody>
      </table>
    </>}
  </section>;
}
