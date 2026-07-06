import { useQuery } from "@tanstack/react-query";
import { api } from "../lib/api";

export default function AuditPage() {
  const audit = useQuery({ queryKey: ["audit"], queryFn: api.audit });

  if (audit.isLoading) return <p className="hint">loading…</p>;
  if (audit.error) return <p className="err">{String(audit.error)}</p>;

  return (
    <>
      <h1>Audit log</h1>
      <p className="hint">append-only; every mutation with its actor and full parameters</p>
      <table>
        <thead>
          <tr>
            <th>when</th>
            <th>actor</th>
            <th>action</th>
            <th>object</th>
            <th>params</th>
          </tr>
        </thead>
        <tbody>
          {audit.data?.map((a) => (
            <tr key={a.id}>
              <td>{new Date(a.ts).toLocaleString()}</td>
              <td>{a.actor}</td>
              <td>{a.action}</td>
              <td>
                {a.object_type}/{a.object_id.slice(0, 20)}
              </td>
              <td className="hint" title={JSON.stringify(a.params, null, 2)}>
                {JSON.stringify(a.params).slice(0, 80)}…
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </>
  );
}
