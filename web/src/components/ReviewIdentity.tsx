import { Link } from "react-router-dom";
import type { IdentityDimension, IdentityPublication, ReviewIdentities } from "../lib/api";
import { useReviewNavigation } from "../lib/navigation";

/** Treat stable dimension/request IDs as data, including prototype-named keys. */
function own<T>(map: Record<string, T> | undefined, key: string): T | undefined {
  return map && Object.prototype.hasOwnProperty.call(map, key) ? map[key] : undefined;
}

function Dimension({ dimension, runId, correlation }: { dimension?: IdentityDimension; runId: string; correlation: string }) {
  const nav = useReviewNavigation();
  if (!dimension || dimension.status === "unknown") return <span className="hint">unknown · selected evidence unavailable</span>;
  if (dimension.status !== "known" && dimension.status !== "ambiguous") return <span className="hint">unsupported identity status</span>;
  const value = dimension.status === "known" && typeof dimension.value === "string" ? dimension.value : null;
  return <>
    <span>{value ?? (dimension.status === "ambiguous" ? "ambiguous · declared sources disagree" : "unknown · published identity is incomplete")}</span>
    <details><summary>Dimension source evidence</summary><ul>
      {(dimension.evidence ?? []).map((entry, index) => {
        const node = entry.graph_node_id;
        const params = new URLSearchParams({ case: correlation });
        const addressable = node != null && Number.isSafeInteger(node) && node >= 0;
        if (addressable) { params.set("node", String(node)); params.set("side", "rep"); }
        return <li key={index}><code>{entry.value}</code> from <code>{entry.location}</code> · selector {entry.selector + 1} · {entry.boundary}·{entry.operation}
          {entry.recorded_event != null && <> · recorded event {entry.recorded_event}</>}
          {addressable && <> · <Link to={nav.href(`/r/${encodeURIComponent(runId)}?${params}`)}>source node</Link></>}
        </li>;
      })}
    </ul></details>
  </>;
}

function IdentitySide({ publication, runId, correlations, label }: { publication?: IdentityPublication; runId: string; correlations: string[]; label: string }) {
  if (!publication) return <p className="hint">{label}: review dimensions were not published for this report.</p>;
  const notes = (publication.notes ?? []).map((note, index) => <p className="hint" key={index}>{note}</p>);
  if (publication.version !== 1 && publication.version !== 2) return <>{notes}<p className="hint">{label}: identity publication version {publication.version} is unsupported.</p></>;
  if (!publication.declaration) return <>{notes}<p className="hint">{label}: no review dimension selectors are declared. No dimension values are inferred.</p></>;
  if (publication.declaration.source !== "current_deployment") return <>{notes}<p className="hint">{label}: identity declaration source is unsupported.</p></>;
  // Version 1's two fixed fields remain readable, without imposing those fields
  // on a current declaration or inventing them when configuration is absent.
  const legacy = publication.version === 1;
  const dimensions = legacy
    ? [{ id: "connector", label: "Legacy connector" }, { id: "flow", label: "Legacy flow" }]
    : Object.entries(publication.dimensions ?? {}).map(([id, definition]) => ({ id, label: definition.label || id }));
  return <div>
    <p className="hint">{label} · current deployment declaration <code title={publication.declaration.digest}>{publication.declaration.digest.slice(0, 12)}</code>. Review evidence, not a run-pinned scoring policy or acknowledgment scope.</p>
    {legacy && <p className="hint">Historical version 1 publication: connector and flow are displayed through a legacy read adapter.</p>}
    {notes}
    {dimensions.length === 0 ? <p className="hint">{label}: this declaration publishes no review dimensions.</p> :
      <table><thead><tr><th>request</th>{dimensions.map(({ id, label: dimensionLabel }) => <th key={id} title={id}>{dimensionLabel}</th>)}</tr></thead><tbody>
        {correlations.map((correlation) => {
          const request = own(publication.requests, correlation);
          return <tr key={correlation}>
            <td className="mono">{correlation}</td>
            {dimensions.map(({ id }) => <td key={id}><Dimension dimension={legacy ? id === "connector" ? request?.connector : request?.flow : own(request?.dimensions, id)} runId={runId} correlation={correlation} /></td>)}
          </tr>;
        })}
      </tbody></table>}
  </div>;
}

/** Display declared values with evidence; legacy lanes remain separate scope hints. */
export function ReviewIdentityEvidence({ identities, correlations, candidateRun, baselineRun }: { identities?: ReviewIdentities; correlations: string[]; candidateRun: string; baselineRun: string }) {
  const requests = [...new Set(correlations)];
  return <details className="delta-box">
    <summary className="head"><span className="t">Declared review dimensions and their evidence</span></summary>
    <div className="body">
      <IdentitySide publication={identities?.candidate} runId={candidateRun} correlations={requests} label="Candidate" />
      <IdentitySide publication={identities?.baseline} runId={baselineRun} correlations={requests} label="Baseline" />
      <p className="hint">Transport destinations are shown separately on each call. A captured hostname does not establish a declared review dimension. Legacy lane labels remain grouping hints and do not gain new authority from this projection.</p>
    </div>
  </details>;
}
