import React from "react";
import { Differ, Viewer } from "json-diff-kit";

// LCS array diffing + modification coalescing = remove+add at the same path
// collapse into one row instead of two. (viewer.css is imported once in main.tsx
// before styles.css so our dark overrides win.)
const differ = new Differ({
  detectCircular: true,
  maxDepth: Infinity,
  showModifications: true,
  arrayDiffMethod: "lcs",
});

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
        indent={2}
        lineNumbers
        highlightInlineDiff
        inlineDiffOptions={{ mode: "word", wordSeparator: " " }}
        hideUnchangedLines={{ threshold: 6, margin: 3 }}
      />
    </div>
  );
}
