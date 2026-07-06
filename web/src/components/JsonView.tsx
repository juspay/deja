import React from "react";

/* Compact, collapsible JSON value renderer. Big values (SQL, request bodies)
   are clamped to a few lines with a click-to-expand. Pure presentational. */
export function JsonView({ value, clamp = true }: { value: unknown; clamp?: boolean }) {
  const [open, setOpen] = React.useState(!clamp);
  if (value === undefined) return <span className="hint">—</span>;
  const text =
    typeof value === "string" ? value : JSON.stringify(value, null, 2);
  const big = text.length > 160 || text.includes("\n");
  if (!big) return <code className="jsonline">{text}</code>;
  return (
    <pre
      className={`jsonblock ${open ? "" : "clamped"}`}
      onClick={() => setOpen((o) => !o)}
      title={open ? "click to collapse" : "click to expand"}
    >
      {text}
    </pre>
  );
}

/* Side-by-side recorded → candidate value cell used by the HTTP body diff. */
export function ValuePair({ baseline, candidate }: { baseline: unknown; candidate: unknown }) {
  return (
    <div className="valuepair">
      <div className="side recorded">
        <span className="sidelbl">recorded</span>
        <JsonView value={baseline} />
      </div>
      <div className="arrow">→</div>
      <div className="side candidate">
        <span className="sidelbl">replayed</span>
        <JsonView value={candidate} />
      </div>
    </div>
  );
}
