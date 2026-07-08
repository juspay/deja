// The confidence ladder — deja's moat. Address rank maps to how trustworthy a
// match is; the run's overall confidence is the *weakest* level it relied on.
//   rank 1+2 → CERTAIN · 3 → HIGH · 4 → MEDIUM · 5 → LOW · 6/unmatched → UNMATCHED

const LEVELS = ["certain", "high", "medium", "low", "unmatched"] as const;
type Level = (typeof LEVELS)[number];

const RANK_LEVEL: Record<string, Level> = {
  rank_1: "certain", rank_2: "certain", rank_3: "high",
  rank_4: "medium", rank_5: "low", rank_6: "unmatched",
};
const LABEL: Record<Level, string> = {
  certain: "CERTAIN", high: "HIGH", medium: "MEDIUM", low: "LOW", unmatched: "UNMATCHED",
};
const RANK_DESC: Record<string, string> = {
  rank_1: "explicit", rank_2: "logical span-path", rank_3: "syntactic hash",
  rank_4: "lexical path", rank_5: "source location", rank_6: "positional ⚠",
};

export function levelForRank(rank?: number): Level {
  return rank ? RANK_LEVEL[`rank_${rank}`] ?? "unmatched" : "unmatched";
}

/** Overall = the weakest (highest-rank) level the run actually used. */
export function overallConfidence(byRank: Record<string, number>): Level {
  let worst = 0;
  for (const [k, n] of Object.entries(byRank || {})) {
    if (n > 0) {
      const r = parseInt(k.replace("rank_", ""), 10);
      if (r > worst) worst = r;
    }
  }
  return levelForRank(worst || undefined);
}

export function ConfidenceBadge({ level, title }: { level: Level; title?: string }) {
  return <span className={`confbadge conf-${level}`} title={title}>{LABEL[level]}</span>;
}

/** Histogram of resolved calls by rank, as a labeled confidence ladder. */
export function ConfidenceLadder({ byRank }: { byRank: Record<string, number> }) {
  const ranks = Object.entries(byRank || {}).sort();
  const max = Math.max(1, ...ranks.map(([, n]) => n));
  if (ranks.length === 0) return <p className="hint">no substitutions recorded</p>;
  // A single populated rank makes the bar meaningless (fill == 100%); say it plainly.
  if (ranks.length === 1) {
    const [rank, n] = ranks[0];
    const lvl = RANK_LEVEL[rank] ?? "unmatched";
    return (
      <div className="ladderrow">
        <span style={{ color: "var(--text-muted)" }}>
          all <b>{n}</b> matches resolved at <b>{RANK_DESC[rank] ?? rank}</b> (rank {rank.replace("rank_", "")})
        </span>
        <ConfidenceBadge level={lvl} />
      </div>
    );
  }
  return (
    <div className="ladder">
      {ranks.map(([rank, n]) => {
        const lvl = RANK_LEVEL[rank] ?? "unmatched";
        return (
          <div className="ladderrow" key={rank}>
            <span className="lbl">{RANK_DESC[rank] ?? rank}</span>
            <div className={`bar conf-${lvl}`} style={{ width: `${(n / max) * 60}%`, background: `var(--conf-${lvl})` }} />
            <span className="ct">{n}</span>
            <ConfidenceBadge level={lvl} />
          </div>
        );
      })}
    </div>
  );
}
