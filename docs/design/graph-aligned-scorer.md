# Graph-aligned scoring — classification as alignment of two execution trees

## The claim

A replay produces two execution graphs: the record graph on the tape and the
replay graph in the observed stream. Both are already constructed, both carry
boundary calls attached to their nodes, and the verdict uses neither — the
scorer classifies over flat call streams, and every identity it consults
(`span_path` strings, FIFO occurrence) is a lossy projection of the trees it
declined to look at. The claim of this design: **the comparison object is the
aligned pair of graphs.** Classification is what falls out of alignment, not a
separate machine that re-derives structure from strings.

This is not a new capability so much as a stopped evasion. Diagnosing the
sandbox run `rp-…-0810210113104` by hand — response body → first divergent
call → its span path → the vendor source — WAS graph alignment, performed
manually, and it collapsed 2,099 omitted calls, 57 novel calls, and 24
fabricated divergences into two facts. The scorer should have handed us those
two facts.

## What already exists (verified, that run)

- **Record graph**: `graph_node` records on the tape — `node_id`,
  `parent_id`, `span_name`, and (since the correlation-stamping change) the
  owning correlation. Extracted per run through `ScopedRecording`, gated by
  the capture-gap refusal.
- **Replay graph**: 24,451 `graph_node` records in `observed.jsonl`, same
  shape, produced by the candidate.
- **The join**: every observed call carries `span_path` (wire name
  `logical_context`) and `graph_node_id` — 1,588 of 1,588 in that run.
  Recorded events join through their rank-2 `SpanPath` lookup addresses and
  their own `graph_node_id`.
- **What the verdict does with all this today: nothing.** The record graph
  feeds one warning; `graph_node_id` feeds the UI. `divergence/mod.rs` says
  it in its own comment: "the verdict below is unaffected."

## Alignment

Node identity is position in structure, not a string:

```
align(record_node, replay_node) when
  their parents are aligned (roots align per correlation),
  span_name matches,
  and they are the k-th same-named child of their parents respectively
```

Three properties, each load-bearing:

1. **Order-strict along ancestry.** A call under `get_trackers >
   find_business_profile` is not the same call under `server_wrap >
   release_lock`, whatever its method name. This is the identity that the
   flat scorer's method-name pairing threw away, and that the span-scoped
   pairing (the flat fix preceding this design) restores as full-path
   equality.
2. **Order-free among siblings.** The recording ran concurrent traffic;
   replay may serialize it differently. Sibling sets compare as multisets
   grouped by name; wall-clock interleaving carries no meaning. A flat
   stream cannot make this distinction — it conflates causal order with
   temporal order, which is why concurrency demanded special-case demotion
   rules.
3. **Occurrence scoped to the parent — a declared trade, not a free lunch.**
   Same-named repeats (a loop, a retry) disambiguate by index *within their
   aligned parent*, not globally. Inside one same-named group, though, the
   k-th↔k-th pairing reuses each side's local creation order — the very
   temporal signal property 2 discards across names. Optimal within-group
   matching (an assignment problem over subtree similarity) is deliberately
   not attempted in v1; that omission is what makes the O(nodes) claim
   true. Where the group's nodes carry boundary events, the events'
   task-lineage fields (`bucket_id`, `fork_seq`) discriminate before
   occurrence does — but a bare span node has no lineage of its own
   (`ExecutionGraphNode` carries none today), so a wrong k-pairing inside a
   concurrent same-named group is possible and would surface as a paired
   `ValueDiverged` across two subtrees. The report must therefore present
   same-named sibling groups with more than one member as lower-confidence
   alignments, named as such.

Alignment is a single top-down pass, O(nodes), no search: align roots, then
recursively align same-named children in order, and everything that fails to
align is *itself* the finding.

## The object being aligned: an activation forest

Worth stating precisely, because every guarantee above rests on it — and
because "execution graph" invites the wrong intuition (the natural read is
enter-a-span-then-pop-back, which sounds like a cycle).

**Acyclic by construction.** Nodes are ACTIVATIONS, not code sites. A static
call graph — nodes = functions — is genuinely cyclic under recursion
(`A → B → A`). Here the graph layer mints a fresh `node_id` per span
instance, so recursion produces a new node under the previous one and
unrolls into a chain. Each node carries exactly one structural edge,
`parent_id`, pointing at the span it was created under; a parent must exist
before its child, so creation order is a topological order and a cycle is
unrepresentable. The enter–descend–exit pop-back lives in the WALK — the
Euler tour a flamegraph draws — not in the structure being walked. Async
does not change this: a tracing span enters and exits once per poll across
awaits, and all of it collapses into one node with one
first-enter-to-last-close interval. The structure is creation-ancestry,
never time-nesting — a span can even close after its correlation scope
exited.

**`causal_parent_ids` makes it a DAG; alignment ignores those edges.** The
follows-from edges cross subtrees and exist for race attribution. Alignment
walks `parent_id` only — causal edges are annotations carried through to the
ported race-evidence rules, never alignment structure — so aligned-parent
occurrence scoping stays well-defined on a tree.

**Forests, not trees.** The scoped reader promotes a node whose parent the
tape does not carry to a root (`scope.rs`; the replay side of the evidence
run carried 175 promoted roots), so a correlation is a forest. Roots align
as the sibling set of a virtual top node — the same (span_name, occurrence)
rule — and an unaligned root is a pruned or novel subtree like any other.

**A malformed tape can still fabricate a cycle, and the reader already
defends against it.** `parent_id` is a u64 that must survive a JSON pipeline
known to mangle large integers; `root_for` carries an explicit cycle-break
(an entry point on a cycle becomes its own root) — semantically unreachable,
defensively necessary. The aligner adopts the same guard, because
`PrunedSubtree`/`NovelSubtree` are only well-defined over acyclic structure:
a detected cycle demotes the correlation to the flat tier, declared in the
scorecard like every other degradation.

## Serving identity vs scoring identity

Replay SERVES results through the lookup ladder — `LookupKey::occurrence` is
FIFO per (correlation, bucket, address, args) — while the aligner SCORES by
k-th-child-of-aligned-parent. Two different occurrence schemes, and nothing
forces them to agree: on a loop of same-named calls the ladder may bind
recorded result *n* to the node alignment calls *k*. Scoring a node with a
value the server bound crosswise would manufacture exactly the
plausible-and-wrong finding this design exists to kill — the repo's
canonical producer/consumer split, one layer up.

The reconciliation is evidence, not assumption: every observed call already
records which recorded event the ladder actually served
(`source_event_global_sequence`). The aligner joins through that field. When
the served event is attached to the very node the alignment paired, the two
identities agree and the comparison is anchored; when they disagree, the
node classifies as `IdentitySkew` — counted, non-silent, listed with both
bindings — rather than being scored as a value divergence. Migration step
2's golden tests must include a same-named loop where the two schemes
disagree, asserting the skew is reported, not scored.

## Classification falls out

For aligned node pairs carrying boundary events, compare the enrichment —
the same value comparison the scorer performs today, now anchored:

| structure | classification |
| --- | --- |
| aligned node, values equal | `Matched` |
| aligned node, values differ | `ValueDiverged` — and it is the **origin** iff no ancestor diverged |
| record subtree with no replay counterpart | `PrunedSubtree(root)` — ONE finding covering every call under it |
| replay subtree with no record counterpart | `NovelSubtree(root)` — ONE finding covering every call under it |

Two consequences worth naming:

**Cause and consequence become ancestry.** Today `ValueDivergedOrigin` vs
`ValueDiverged` is a heuristic split between two pairing arms. Under
alignment it is a structural fact: the first divergent node on a root-to-leaf
path is the cause; everything below it is consequence. The total-derivative
cascade the system exists to catch becomes a path you can walk.

**Cascades collapse to their roots.** Run 0810 under this scorer: each of
the 86 walled correlations reports `NovelSubtree` rooted at
`find_business_profile > get_or_populate_redis` (the cause, named) and
`PrunedSubtree` rooted at the point the flow died (the consequence, sized).
Not 2,099 omitted calls. The scorecard's summary counts survive as
projections — a pruned subtree still *contains* N omitted calls — but the
report leads with the two facts, not the two thousand.

## The double-claim class dies structurally

The flat scorer needed a two-pass restructure and an accounting assertion to
stop one recorded event being classified twice. Under alignment the property
is not enforced, it is unrepresentable: a node aligns with at most one
counterpart, every node in either graph lands in exactly one row of the table
above, and every boundary event inherits its node's classification. The
accounting identity — every input becomes exactly one output — is the shape
of the algorithm, not a check bolted onto it.

## Concurrency evidence, ported

The existing demotion rules are flat-stream approximations of graph facts,
and each ports to where it natively lives:

- **Rule A (order-nondeterministic same-row UPDATE-RETURNING)**: a
  sibling-order artifact — two aligned writes whose *sibling order* swapped.
  Becomes a property of the aligned parent, not a span-string comparison.
- **Rule B (idempotent redis DELETE)**: a node-local value rule, unchanged,
  just anchored.
- **Race evidence (`inconclusive_race`)**: overlapping lineages touching the
  same row are *cross-subtree* facts; the graph carries the lineage
  boundaries the current code reconstructs from span strings.

## Degraded modes are declared, never silent

- **No graph on either side** (old tape, capture gap): the correlation
  scores through the flat span-scoped scorer — the fix that precedes this
  design is the fallback tier, not throwaway work — and the scorecard names
  the degradation per correlation.
- **Partial graph**: align what exists; nodes outside the graph score flat;
  the boundary between the two is reported.
- **Request-only cases** (validation rejections): an empty tree aligned with
  an empty tree. Trivially matched, no warning — the graph-note fix already
  encodes this judgment.
- **Attachment gaps**: `graph_node_id` is optional on events. An event
  inside a graphed correlation that carries no node id joins by span path
  only when that join is unambiguous — exactly one candidate node —
  otherwise the event scores through the flat tier and the correlation's
  report says how many events did. Never a silent guess among the
  occurrence-siblings alignment exists to disambiguate.
- **Span-rename drift is a known blind spot — detected, not solved.** The
  record graph is the baseline's code and the replay graph is the
  candidate's, different by design. A renamed ancestor span makes its whole
  subtree unalignable and reads as a Pruned+Novel pair, indistinguishable
  from a real wall. The v1 mitigation is honest rather than clever: when a
  pruned and a novel subtree under the same aligned parent have matching
  shape (same child-name multiset, same boundary-event count), the report
  flags the pair as a suspected rename instead of two independent findings.
  A real name-mapping story — declared renames, or content-based matching —
  is future work, and this document says so instead of pretending the case
  cannot arise.

## Accounting

Asserted, not logged, same standard as the flat scorer's new backstop:

```
|record nodes| = aligned + pruned-under-some-root
|replay nodes| = aligned + novel-under-some-root
aligned = matched + value-diverged + identity-skew
every boundary event maps to exactly one node outcome
  (flat-tier events counted and named per correlation)
summary counters are projections of the node table — never a second tally
```

## Migration

1. The flat fix (span-scoped pairing, two-pass, assertion) ships first and
   remains as the degraded tier. Nothing here reverts it.
2. The aligner lands as a pure module over `(record graph, replay graph,
   events, observed)` — the same inputs `detect()` already receives — with
   golden tests built from the run-0810 shapes: the wall correlation (novel +
   pruned subtree), the clean correlation (full alignment), the 400 case
   (empty trees).
3. `detect()` switches per correlation: graph-aligned when both trees exist,
   flat otherwise. Scorecard schema gains `NovelSubtree`/`PrunedSubtree`
   kinds and a per-correlation `scoring_mode`; existing counters keep their
   meanings as projections. Per the no-legacy-compat policy this is a clean
   cutover for new runs — old scorecards are not rewritten, old tapes simply
   take the flat tier.
4. `docs/design/unified-execution-graph.md` (in flight in a sibling
   worktree) covers graph *construction*; this document covers graph
   *comparison*. Reconcile at review — the constructed graph this design
   consumes must be the one that design produces.

## What this deliberately does not do

- No graph *diffing* beyond alignment — no edit-distance, no reordering
  recovery. A moved subtree reports as pruned+novel; if that proves noisy in
  practice, the evidence will say so before any cleverness is bought.
- No cross-correlation alignment. Correlations stay independent test cases;
  session-scoped state questions live elsewhere.
- No change to WHAT is compared at a node (codecs, value canon, tolerated
  tiers all unchanged) — only to how the two sides find each other.
