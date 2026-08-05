import React from "react";
import { AvailableRecording, RecordingRow } from "../lib/api";
import { catalogById, identityText, scaleText, spanOf } from "../lib/recordings";

/**
 * CHOOSE A RECORDING BY NAME, NOT BY PATH.
 *
 * Where recordings land and how the keys are partitioned belong to the
 * deployment. A caller names a recording; the orchestrator resolves the rest.
 * So this replaces the two path fields the form used to open with.
 *
 * WHY THIS IS NOT A `<select>`. A recording id is minted once per router
 * process, so one id is a pod's entire lifetime — the live index holds a single
 * session spanning seven daily partitions and 42,310 correlations, next to
 * others holding nine objects and one day. Those two are the same shape of
 * thing in a dropdown and are wildly different things to replay. A native
 * option is one line of unstyled text; the difference has to be visible at the
 * moment of choosing, so each row carries its span, its size, and whether it is
 * already pulled, and the choice is restated underneath in full.
 *
 * It is still a dropdown: closed by default, one line when closed, keyboard
 * driven (Arrow/Home/End/Enter/Escape), dismissed by clicking away.
 */
export function RecordingPicker({
  recordings,
  catalog,
  value,
  onChange,
  truncated,
}: {
  recordings: AvailableRecording[];
  catalog: RecordingRow[] | undefined;
  value: string;
  onChange: (id: string) => void;
  /** The bucket holds more than this page — say so rather than imply a full list. */
  truncated: number;
}) {
  const [open, setOpen] = React.useState(false);
  const [active, setActive] = React.useState(0);
  const rootRef = React.useRef<HTMLDivElement>(null);
  const listRef = React.useRef<HTMLUListElement>(null);
  // The visible label is a heading over a group, not a <label for>, so the
  // control names itself.
  const listId = React.useId();
  const byId = React.useMemo(() => catalogById(catalog), [catalog]);

  const index = recordings.findIndex((r) => r.recording_id === value);
  const picked = index >= 0 ? recordings[index] : undefined;

  // Clicking away closes. Pointerdown rather than click, so the menu is gone
  // before whatever was clicked reacts.
  React.useEffect(() => {
    if (!open) return;
    const away = (e: PointerEvent) => {
      if (!rootRef.current?.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("pointerdown", away);
    return () => document.removeEventListener("pointerdown", away);
  }, [open]);

  React.useEffect(() => {
    if (open) setActive(index >= 0 ? index : 0);
  }, [open, index]);

  React.useEffect(() => {
    if (!open) return;
    listRef.current?.children[active]?.scrollIntoView({ block: "nearest" });
  }, [open, active]);

  const commit = (i: number) => {
    const r = recordings[i];
    if (r) onChange(r.recording_id);
    setOpen(false);
  };

  const onKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Escape") {
      setOpen(false);
      return;
    }
    if (!open && (e.key === "ArrowDown" || e.key === "ArrowUp" || e.key === "Enter")) {
      e.preventDefault();
      setOpen(true);
      return;
    }
    if (!open) return;
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setActive((a) => Math.min(a + 1, recordings.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setActive((a) => Math.max(a - 1, 0));
    } else if (e.key === "Home") {
      e.preventDefault();
      setActive(0);
    } else if (e.key === "End") {
      e.preventDefault();
      setActive(recordings.length - 1);
    } else if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      commit(active);
    }
  };

  return (
    <div className="recpick" ref={rootRef} onKeyDown={onKeyDown}>
      <button
        type="button"
        className={open ? "recpick-trigger open" : "recpick-trigger"}
        aria-haspopup="listbox"
        aria-expanded={open}
        aria-controls={open ? listId : undefined}
        aria-label={`recording to replay${value ? `: ${value}` : " — none chosen"}`}
        onClick={() => setOpen((o) => !o)}
      >
        <span className="recpick-trigger-main">
          {picked ? (
            <>
              <span className="recpick-id">{picked.recording_id}</span>
              <span className="recpick-sub">
                {spanOf(picked.dates).text} · {scaleText(picked, byId.get(picked.recording_id))}
              </span>
            </>
          ) : value ? (
            // A recording named in the URL that the bucket listing does not
            // hold. Show it as chosen and say what is odd about it, rather
            // than silently resetting the caller's selection.
            <>
              <span className="recpick-id">{value}</span>
              <span className="recpick-sub warn">not in the bucket listing</span>
            </>
          ) : (
            <span className="recpick-sub">choose a recording</span>
          )}
        </span>
        <span className="recpick-caret" aria-hidden="true">
          ▾
        </span>
      </button>

      {open && (
        <div className="recpick-pop">
          <ul className="recpick-list" id={listId} role="listbox" ref={listRef} tabIndex={-1}>
            {recordings.map((r, i) => {
              const span = spanOf(r.dates);
              const ident = identityText(r.identity);
              return (
                <li
                  key={r.recording_id}
                  role="option"
                  aria-selected={r.recording_id === value}
                  className={[
                    "recpick-row",
                    i === active ? "active" : "",
                    r.recording_id === value ? "chosen" : "",
                  ]
                    .filter(Boolean)
                    .join(" ")}
                  onPointerEnter={() => setActive(i)}
                  onClick={() => commit(i)}
                >
                  <div className="recpick-row-head">
                    <span className="recpick-id">{r.recording_id}</span>
                    <span className="recpick-chips">
                      {span.multiDay && (
                        <span className="chip modified" title="one id, many days of traffic">
                          {span.partitions} days
                        </span>
                      )}
                      <span className={r.pulled ? "chip pass" : "chip muted"}>
                        {r.pulled ? "pulled" : "in bucket"}
                      </span>
                    </span>
                  </div>
                  <div className="recpick-row-sub">
                    {span.range} · {scaleText(r, byId.get(r.recording_id))}
                  </div>
                  {ident && <div className="recpick-row-ident">{ident}</div>}
                </li>
              );
            })}
          </ul>
          {/* Outside the listbox: a listbox's children must all be options, and
              this is a statement about the list rather than a thing to pick. */}
          {truncated > 0 && (
            <p className="recpick-more">
              showing {recordings.length} of {truncated} in the bucket
            </p>
          )}
        </div>
      )}
    </div>
  );
}

/**
 * WHAT YOU ARE ABOUT TO REPLAY, restated under the closed dropdown.
 *
 * The dropdown's job is to be small; this one's is to leave nobody able to say
 * they did not know a week of production traffic was behind a one-line control.
 */
export function RecordingSummary({
  rec,
  catalog,
}: {
  rec: AvailableRecording | undefined;
  catalog: RecordingRow | undefined;
}) {
  if (!rec) return null;
  const span = spanOf(rec.dates);
  const ident = identityText(rec.identity);
  return (
    <div className="recsummary">
      <dl>
        <div>
          <dt>span</dt>
          <dd>{span.text}</dd>
        </div>
        <div>
          <dt>size</dt>
          <dd>{scaleText(rec, catalog)}</dd>
        </div>
        <div>
          <dt>state</dt>
          <dd>
            {rec.pulled
              ? "already in the catalog — the run reuses the ingested tape"
              : "in the bucket only — the run's first stage fetches and ingests it"}
          </dd>
        </div>
        {/* Identity renders only when the id carries it. Its absence means the
            recording predates ids naming a revision, which is not a defect and
            is not reported as one. */}
        {ident && (
          <div>
            <dt>identity</dt>
            <dd className="mono">{ident}</dd>
          </div>
        )}
      </dl>
      {span.multiDay && (
        <p className="recsummary-note">
          <b>One id, {span.partitions} days.</b> A recording id is minted once per router process,
          so this is a pod's entire lifetime rather than a window of traffic — everything it
          recorded between {span.from} and {span.to} is one item here, and there is no way to
          replay a single day of it from this form. Narrow the run with the correlation filter
          below.
        </p>
      )}
    </div>
  );
}
