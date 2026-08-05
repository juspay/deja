import React from "react";
import { Differ, Viewer } from "json-diff-kit";

// LCS array diffing + modification coalescing = remove+add at the same path
// collapse into one row instead of two.
const differ = new Differ({
  detectCircular: true,
  maxDepth: Infinity,
  showModifications: true,
  arrayDiffMethod: "lcs",
});

// Line backgrounds, as an INLINE STYLE.
//
// Import order does not decide this — specificity does, and the old comment here
// claimed the opposite ("viewer.css is imported once in main.tsx before
// styles.css so our dark overrides win"). It is false, and it is how the diff
// shipped unreadable: json-diff-kit's own `.json-diff-viewer tr .line-remove
// { background: #ef9a9a }` has specificity (0,2,1) and beats our
// `.jdk .line-remove` at (0,2,0), so the pale light-theme backgrounds survive
// under `color: var(--text-muted)` #aeb6c2 — measured 1.05:1 on removed lines,
// 1.24:1 add, 1.58:1 modify, against WCAG AA's 4.5:1.
//
// The Viewer emits `bgColour[type]` as `style={{ backgroundColor }}` on all four
// <td>s (line-number and content, both sides — verified in the shipped
// dist/viewer.js), and an inline style beats every stylesheet regardless of
// specificity or import order. It is also immune to the library's CSS churn.
// Measured against --text-muted: add 8.07:1, remove 8.49:1, modify 8.29:1.
const BG_COLOUR = { add: "#0f2317", remove: "#2a1416", modify: "#211c10" };

/** A GitHub-style split before/after diff of two JSON values. */
export function JsonDiff({ before, after, split = true }: { before: unknown; after: unknown; split?: boolean }) {
  const diff = React.useMemo(
    () => differ.diff(before ?? null, after ?? null),
    [before, after],
  );
  return (
    <div className="jdk">
      {split && (
        <div className="splithdr">
          <div className="rec">recorded — what it used to be</div>
          <div className="rep">replayed — what it is now</div>
        </div>
      )}
      <Viewer
        diff={diff}
        bgColour={BG_COLOUR}
        indent={2}
        lineNumbers
        highlightInlineDiff
        inlineDiffOptions={{ mode: "word", wordSeparator: " " }}
        hideUnchangedLines={{ threshold: 6, margin: 3 }}
      />
    </div>
  );
}
