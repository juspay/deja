import React from "react";
import { CorrelationSource } from "../lib/correlations";

/**
 * CHOOSE THE TEST CASES, WITHOUT ALREADY KNOWING THEIR IDS.
 *
 * This replaces a bare text field that asked for comma-separated correlation
 * ids — which only worked if you had them in another window. It is a search over
 * the recording's own correlations index, multi-select by click, range-select by
 * dragging (or shift-clicking) from the last row you touched, and a paste box
 * that still accepts ids typed by hand.
 *
 * THE CAP REFUSES, IT DOES NOT TRUNCATE. Every write goes through `propose`,
 * which compares the WHOLE resulting selection against the cap and, when it is
 * over, changes nothing and says so. Trimming a 400-correlation drag to its
 * first 100 would produce a scorecard that looks complete and is not — the one
 * outcome this control exists to prevent. A drag says its size while it is being
 * dragged, so a refusal is never a surprise.
 *
 * THAT IS NOT THE SAME THING AS THE DEFAULT. Selecting nothing is not a
 * selection that got trimmed; it is declining to make one, and it takes the
 * first `cap` correlations. The two are kept apart in state as well as in
 * wording — the default lives in `source.defaultScope` and never touches
 * `value` unless the caller materialises it deliberately. `.corrrow.dflt` marks
 * which rows the default covers, so "the first 100" is visible rather than
 * asserted.
 *
 * WHAT IT WILL NOT PRETEND. Three answers stay three: sealed, unsealed, and
 * unknown. An unsealed recording has correlations that cannot be listed cheaply
 * yet, and it renders as exactly that — never as an empty picker, which would
 * read as "this recording has no correlations".
 */
export function CorrelationPicker({
  source,
  cap,
  value,
  onChange,
}: {
  source: CorrelationSource;
  /** Hard limit on the resulting selection. */
  cap: number;
  value: string[];
  onChange: (ids: string[]) => void;
}) {
  const [q, setQ] = React.useState("");
  const [paste, setPaste] = React.useState("");
  const [refusal, setRefusal] = React.useState<string | null>(null);
  /** The row a range extends FROM: the last row committed by click. */
  const [anchor, setAnchor] = React.useState<number | null>(null);
  const [drag, setDrag] = React.useState<{ from: number; to: number } | null>(null);
  const dragRef = React.useRef<{ from: number; to: number } | null>(null);
  const listId = React.useId();

  const selected = React.useMemo(() => new Set(value), [value]);
  const known = React.useMemo(
    () => new Set(source.candidates.map((c) => c.id)),
    [source.candidates],
  );

  // The default only describes what will happen while nothing is picked. One
  // explicit pick and it stops applying, so it stops being shown.
  const showDefault = value.length === 0 && source.state === "sealed";

  const needle = q.trim().toLowerCase();
  const rows = React.useMemo(
    () =>
      needle
        ? source.candidates.filter((c) => c.id.toLowerCase().includes(needle))
        : source.candidates,
    [source.candidates, needle],
  );

  /**
   * THE ONLY WRITE PATH, so the cap has exactly one place to hold.
   *
   * Refusal is whole: the proposed selection is discarded and the existing one
   * is left untouched, and the message states both numbers so it is obvious
   * nothing was quietly kept.
   */
  const propose = React.useCallback(
    (next: string[], what: string) => {
      const uniq = [...new Set(next)];
      if (uniq.length > cap) {
        setRefusal(
          `Refused: ${what} would put the selection at ${uniq.length.toLocaleString()} ` +
            `correlations, over the limit of ${cap} per run. Nothing was selected — the ` +
            `selection is still ${value.length}. Narrow it and try again; it is not truncated ` +
            `to the first ${cap}, because a run that drove ${cap} of ${uniq.length.toLocaleString()} ` +
            `would still score as if it had driven them all.`,
        );
        return;
      }
      setRefusal(null);
      onChange(uniq);
    },
    [cap, onChange, value.length],
  );

  const remove = React.useCallback(
    (id: string) => {
      setRefusal(null);
      onChange(value.filter((v) => v !== id));
    },
    [onChange, value],
  );

  const toggle = React.useCallback(
    (id: string) => {
      if (selected.has(id)) {
        remove(id);
        return;
      }
      propose([...value, id], "adding this correlation");
    },
    [propose, remove, selected, value],
  );

  const applyRange = React.useCallback(
    (a: number, b: number) => {
      const lo = Math.min(a, b);
      const hi = Math.max(a, b);
      const ids = rows.slice(lo, hi + 1).map((r) => r.id);
      propose(
        [...value, ...ids],
        `that range of ${ids.length.toLocaleString()} row${ids.length === 1 ? "" : "s"}`,
      );
    },
    [propose, rows, value],
  );

  // Held in a ref so the document-level pointerup listener below always sees the
  // current selection without resubscribing on every pointer move.
  const commitRef = React.useRef<(d: { from: number; to: number }) => void>(() => {});
  commitRef.current = (d) => {
    setAnchor(d.from);
    if (d.from === d.to) {
      const row = rows[d.from];
      if (row) toggle(row.id);
    } else {
      applyRange(d.from, d.to);
    }
  };

  // A drag can end anywhere, including outside the list and outside the window,
  // so the release is watched on the document. Ending off-list still commits the
  // range that was swept, which is what a release means everywhere else.
  const dragging = drag !== null;
  React.useEffect(() => {
    if (!dragging) return;
    const up = () => {
      const d = dragRef.current;
      dragRef.current = null;
      setDrag(null);
      if (d) commitRef.current(d);
    };
    document.addEventListener("pointerup", up);
    document.addEventListener("pointercancel", up);
    return () => {
      document.removeEventListener("pointerup", up);
      document.removeEventListener("pointercancel", up);
    };
  }, [dragging]);

  const onRowPointerDown = (i: number, e: React.PointerEvent) => {
    if (e.button !== 0) return;
    if (e.shiftKey && anchor != null) {
      applyRange(anchor, i);
      return;
    }
    dragRef.current = { from: i, to: i };
    setDrag({ from: i, to: i });
  };

  const onRowPointerEnter = (i: number) => {
    if (!dragRef.current) return;
    dragRef.current = { from: dragRef.current.from, to: i };
    setDrag(dragRef.current);
  };

  const lo = drag ? Math.min(drag.from, drag.to) : -1;
  const hi = drag ? Math.max(drag.from, drag.to) : -2;
  const dragWouldBe = drag
    ? new Set([...value, ...rows.slice(lo, hi + 1).map((r) => r.id)]).size
    : 0;
  const dragOver = dragWouldBe > cap;

  const addPasted = () => {
    const ids = paste.split(/[\s,]+/).map((s) => s.trim()).filter(Boolean);
    if (ids.length === 0) return;
    propose(
      [...value, ...ids],
      `pasting ${ids.length.toLocaleString()} id${ids.length === 1 ? "" : "s"}`,
    );
    setPaste("");
  };

  return (
    <div className="corrpick">
      <div className="corrhead">
        <input
          className="corrsearch"
          type="search"
          placeholder="search correlation ids…"
          value={q}
          onChange={(e) => setQ(e.target.value)}
          aria-label="search correlation ids"
          aria-controls={listId}
        />
        <span className={value.length >= cap ? "corrcount full" : "corrcount"}>
          {value.length} / {cap} selected
        </span>
        <span className="hint corrcap">hard limit {cap} correlations per run</span>
      </div>

      {/* PROVENANCE. What this list is, said before it is offered. */}
      {source.state === "sealed" && (
        <p className="hint corrsrc">
          {source.complete
            ? `All ${source.candidates.length.toLocaleString()} correlation${source.candidates.length === 1 ? "" : "s"} in this recording, earliest request first.`
            : `The first ${source.candidates.length.toLocaleString()} correlations in this recording, earliest request first — search and selection reach these; anything further in can still be pasted below.`}
          {source.total != null && !source.complete
            ? ` The recording holds ${source.total.toLocaleString()} in all.`
            : null}
        </p>
      )}

      {/* A FAILURE TO LOOK IS NOT AN ABSENCE — the same rule the recording
          picker follows for the bucket listing. */}
      {source.error && (
        <p className="err corrfail">
          <b>Could not read this recording's correlations</b>: {source.error}. This is a failure to
          look, not a recording without correlations. The run can still be launched — it drives the
          first {cap} — and ids can be pasted below.
        </p>
      )}

      {source.state === "pending" && !source.error && (
        <p className="hint corrsrc">
          {source.loading
            ? "reading this recording's correlations index…"
            : "choose a recording above to list its correlations."}
        </p>
      )}

      {/* NOT SEALED IS NOT EMPTY. The manifest is written last, so its absence
          means the ids are not knowable cheaply — not that there are none. This
          does not block a launch: the orchestrator applies the same limit. */}
      {source.state === "unsealed" && (
        <div className="corrnone">
          <p>
            <b>This recording is still open — its correlations cannot be listed yet.</b>
          </p>
          <p className="hint">
            A recording's index is written when the recording is sealed, and this one has not been.
            That is why there is nothing to pick from, and it is <b>not</b> a claim that the
            recording holds no correlations
            {source.total != null ? ` — it holds ${source.total.toLocaleString()}` : ""}. The run
            still launches: with nothing named, the orchestrator drives the first {cap} correlations
            itself. This page cannot say in advance which {cap} those are. Paste ids below to choose
            them yourself.
          </p>
        </div>
      )}

      {source.state === "unknown" && (
        <div className="corrnone">
          <p>
            <b>No recording under this id.</b>
          </p>
          <p className="hint">
            The correlations index has no entry for it. A recording that has landed in the bucket
            but has never been ingested is not in the index yet, so this can mean "not ingested"
            rather than "does not exist". Paste ids below to scope a run anyway.
          </p>
        </div>
      )}

      {rows.length > 0 && (
        <>
          <div className="corrtools">
            <button
              type="button"
              className="btn"
              onClick={() =>
                propose(
                  [...value, ...rows.map((r) => r.id)],
                  `selecting all ${rows.length.toLocaleString()} match${rows.length === 1 ? "" : "es"}`,
                )
              }
            >
              select all {rows.length.toLocaleString()} shown
            </button>
            {value.length > 0 && (
              <button
                type="button"
                className="btn"
                onClick={() => {
                  setRefusal(null);
                  onChange([]);
                }}
              >
                clear selection
              </button>
            )}
            <span className="hint">
              click to pick · drag across rows, or shift-click, to take a range
            </span>
          </div>

          {drag && (
            <p className={dragOver ? "corrdrag over" : "corrdrag"} role="status">
              {hi - lo + 1} row{hi - lo === 0 ? "" : "s"} swept — selection would be {dragWouldBe} /{" "}
              {cap}
              {dragOver ? " · over the limit, this range will be refused" : ""}
            </p>
          )}

          <ul className="corrlist" id={listId}>
            {rows.map((c, i) => {
              const on = selected.has(c.id);
              const inDrag = i >= lo && i <= hi;
              // Which rows the default covers, shown only while it is what
              // would actually run. The moment anything is picked explicitly
              // the default no longer applies, so the marking goes.
              const isDefault = showDefault && c.ordinal <= cap;
              return (
                <li key={c.id}>
                  <button
                    type="button"
                    className={[
                      "corrrow",
                      isDefault ? "dflt" : "",
                      on ? "on" : "",
                      inDrag ? (dragOver ? "sweep over" : "sweep") : "",
                    ]
                      .filter(Boolean)
                      .join(" ")}
                    aria-pressed={on}
                    onPointerDown={(e) => onRowPointerDown(i, e)}
                    onPointerEnter={() => onRowPointerEnter(i)}
                    onKeyDown={(e) => {
                      if (e.key !== "Enter" && e.key !== " ") return;
                      e.preventDefault();
                      setAnchor(i);
                      toggle(c.id);
                    }}
                  >
                    <span className="corrbox" aria-hidden="true">
                      {on ? "✓" : ""}
                    </span>
                    <span className="corrord">#{c.ordinal.toLocaleString()}</span>
                    <span className="corrid mono">{c.id}</span>
                    <span className="corrmark">{isDefault ? "in the default" : ""}</span>
                  </button>
                </li>
              );
            })}
          </ul>
        </>
      )}

      {source.candidates.length > 0 && rows.length === 0 && (
        <p className="hint corrnomatch">
          no id here contains “{q.trim()}”.
          {source.complete
            ? " Nothing in this recording matches."
            : ` Only the first ${source.candidates.length.toLocaleString()} of the recording's ${(source.total ?? 0).toLocaleString()} are loaded, so a match further in would not be found — paste it below.`}
        </p>
      )}

      {refusal && (
        <p className="err corrrefusal" role="alert">
          {refusal}
        </p>
      )}

      {value.length > 0 && (
        <div className="corrchosen">
          <span className="hint">
            driving {value.length} test case{value.length === 1 ? "" : "s"} — everything outside this
            list is excluded from the verdict, not counted omitted.
          </span>
          <ul>
            {value.map((id) => (
              <li key={id}>
                <button
                  type="button"
                  className={known.has(id) ? "corrchip" : "corrchip unknown"}
                  onClick={() => remove(id)}
                  title={known.has(id) ? `remove ${id}` : `${id} — not in the candidate list; remove`}
                >
                  <span className="mono">{id}</span>
                  {!known.has(id) && <em>unlisted</em>}
                  <span aria-hidden="true">×</span>
                </button>
              </li>
            ))}
          </ul>
        </div>
      )}

      <div className="corrpaste">
        <label htmlFor={`${listId}-paste`}>
          paste ids <span className="hint">(comma, space or newline separated)</span>
        </label>
        <textarea
          id={`${listId}-paste`}
          rows={2}
          placeholder="019fae07-b8d4-7080-aa62-9ecc4f816dd8, 019fae07-…"
          value={paste}
          onChange={(e) => setPaste(e.target.value)}
        />
        <button type="button" className="btn" onClick={addPasted} disabled={!paste.trim()}>
          add to selection
        </button>
      </div>
    </div>
  );
}
