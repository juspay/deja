# The partial-function replay model — fail-stop on a Substitute-miss, no policy

**Status: implemented.**
This is the model the team converged on after [execute-substitute-declaration.md](execute-substitute-declaration.md)
(#28, the Channel/Effect → one-knob collapse). The global policy axis is gone:
runtime routing is explicit `RuntimeMode` plus per-boundary `ReplayStrategy`, and
a missed Substitute lookup is an honest blocking divergence instead of a silently
served lie.

## The thesis

The recording tape is a **partial function** `f: (call-site, args) → result`. At
each instrumented boundary, replay queries `f`. The lookup either **hits** its
domain (this exact call-site + args was recorded) or **misses** (args diverged, or
a novel call-site). What to do is decided by **one per-site mode**, honored as an
intermix across the call graph. **There is no global policy** — `AllLookup` only
ever masked via the serve-stale fallback (now deleted), so it collapsed to "all
sites Substitute," which is degenerate; `SelectiveExecute` was just "honor the
declarations," which is the *only* way to replay. So the policy axis is gone.

## The model — two modes × {hit, miss}

`ReplayStrategy { Execute, Substitute }` (default `Substitute`), per site.

| | **HIT** (call-site **and** args match) | **MISS** (args diverged or novel) |
|---|---|---|
| **Substitute** *(default)* — *trust the tape; value matters* | serve the recorded result; do **not** run (for a value-less sink this is a noop) | **flag + FAIL-STOP** |
| **Execute** *(opt-in; only where re-running is safe)* — *verify against reality* | run the real fn, diff result vs recorded (`ValueDiverged`) | run the real fn (recompute for the new args), `NovelCall` if no baseline |

The fourth combination — *serve recorded on a miss and continue* — is the
serve-stale lie (it poisons downstream args; see below). It is rejected, so the
model is exactly these three live cells.

### Why fail-stop, not serve-stale, on a Substitute-miss

The detection is identical either way (the divergence is flagged from the
**arguments** — a miss *is* an argument divergence). So the choice is "which
continuation never corrupts," and serve-stale corrupts three ways:

1. **Dataflow poisoning is unbounded.** deja substitutes *results*, never *args*.
   A served-stale value flows by ordinary dataflow into every downstream
   boundary's live args — corrupting their args-hashes (cascading spurious misses)
   and their outputs. A downstream `Execute` does **not** heal it (it recomputes
   against real state but inherits the poisoned argument).
2. **Shared substrate.** The replay run shares one pg + one redis across all
   parallel test-cases; serving stale (or falling through to `run()`) mutates the
   shared store siblings read.
3. **Detection honesty.** Today's serve-stale path forces `observed == recorded`,
   laundering the divergence into `kind=matched` — a false PASS.

Fail-stop (panic-unwind at the seam) discards the entire in-process downstream
subtree **by construction** — descendants never execute, so nothing corrupt is
computed, served, or written. That is exact transitive "subtree compromised" with
zero graph-marking machinery.

### The scope is not free — the host must contain the unwind

This section originally ended "…scoped to the one correlation (each request is an
isolated test case)", treating per-request isolation as a property the host
provides. **It is not.** There is no `catch_unwind` anywhere in actix-web 4.11 /
actix-http 3.11 / actix-server 2.6: the panic unwinds out of the connection task
and the worker writes no response at all. The kernel sees `server closed the
connection without writing a response (0 bytes read)` and scores the correlation
as a transport fault, so the divergence that was already emitted *before* the
panic is buried, and every remaining call of that request is reported as an
anonymous omission. Measured on `rp-sbx-bb148328f7-…-0812141147885`: 8 of 73
correlations, and 178 of the run's 182 omitted calls.

The containment is therefore explicit — `deja::catch_fail_stop_async` wraps the
host's request future, classifies a caught payload by `deja::FAIL_STOP_SENTINEL`,
re-raises everything else untouched, and hands the host a `FailStop` whose message
becomes the 5xx body. Two properties are deliberate: the guard is inert outside
replay (a fail-stop is replay-only, so record-mode and deja-off traffic keep
byte-identical panic semantics), and it does **not** resume the request. The
stopped request's remaining boundaries stay unexecuted — that is this model's
point, and continuing past a miss would need exactly the fabricated value
rejected above. What the guard buys is a correlation that scores as the boundary
divergence it is, instead of as a hangup.

### Why no third mode (Skip/Noop)

Considered and rejected. A fire-and-forget sink's divergence is *also* an argument
divergence, which Substitute already catches; on a HIT Substitute already noops
the side effect (it serves the recorded result instead of running). The only thing
a `Skip` (flag-but-continue) mode would buy is collecting several non-load-bearing
sink divergences in one run instead of fail-stopping at the first — a marginal
ergonomic win, and a purely additive opt-in if it ever turns real. YAGNI.

## The return-type problem — solved by type erasure

Fail-stop = **panic-unwind at the dispatch seam**, not a synthesized error:
- `replay_ok` boundaries never name `E` (it may be non-serde, e.g.
  `error_stack::Report`), so an `Err` cannot be constructed.
- A **true miss returns `None` from the lookup before any attempt to build `T`**,
  so the panic is fully type-erased — it works for `u64`, `String`, structs, `()`,
  `Result`, streams alike. No `Default` bound; `Default::default()` is rejected
  (fabricating a value launders the divergence into a false pass).

### `on_miss` — a declared miss value, where one is honest

The type-erasure argument above says deja cannot build an `Err` for an `E` a
`replay_ok` site never names. That is true of *deja*; it is not true of the
site. `on_miss = <expr>` on `#[deja::boundary]` inverts the construction: the
declaration, which knows its own return type, supplies the value; the seam
(`dispatch_or_miss` / `dispatch_async_or_miss`) just returns it instead of
panicking. Deja contributes `deja::SubstituteMiss` — boundary, component,
method, args image — which the expression may name as `__deja_miss` so a host
error enum takes it with `#[from]` and the degraded path stays attributable. No
host error type appears in deja.

Why the fail-stop default is wrong for *some* boundaries: a candidate under
review adds boundary calls the recording never made — that is what a change is.
Answering the first one with an unwind means actix writes nothing, the kernel
reads a 500, and one added cache read censors every other signal in the run (a
measured sandbox run: 45 of 52 correlations at `500 vs 200`, 1/52 matched). The
miss is still scored either way — the blocking NovelCall divergence is emitted
by the lookup *before* `on_miss` runs — so the choice is only about the
continuation, and a returned miss lets the divergence localise to the subtree
that actually needed the value.

The distinction that keeps this from being the rejected `Default::default()`:

- **Honest.** `Cache::get_val` returns `Option<T>`; `None` means *not in cache*,
  which is TRUE under replay, and the caller's fallback is separately
  instrumented, so the fallback's own boundary calls are scored. Nothing untrue
  is asserted.
- **Fabrication.** A synthesized egress response claims a third party answered
  when none did, and every downstream boundary consumes the lie as an argument.
  That is the false pass `Default::default()` was rejected for. Egress
  (http/grpc) keeps the fail-stop default, per Legality below.

`on_miss` on a site that resolves to `replay = Execute` is a build error: the
Execute branch never reaches the Substitute-miss arm, so the declaration would
be dead code reading as protection.

## Legality — `Execute` is not valid everywhere

`Execute` re-runs the real boundary, so it is illegal where re-running is unsafe:
- **Egress** (http/grpc to a third party): would hit prod. Must stay Substitute.
- **Accumulative RMW** (`INCR`/`count`/`delete`): would double-apply. Must stay
  Substitute. (Blind last-write-wins `SET` is fine — re-running with the same args
  reaches the same state.)

This is a per-site **declaration-legality lint** (reject `Execute` on those at
build), not a runtime branch — the runtime stays the pure 2×2.

## Implementation — four seams

A "miss" reaches `run()` (a live boundary call) at **four** fall-through points;
all four get the same gate: **in Lookup mode, a `None` from the lookup → emit the
already-recorded blocking divergence (done by `try_replay`/`resolve`) + fail-stop;
never fall through to `run()`**.

1. `crates/deja-record/src/lib.rs` — `dispatch` (sync macro seam).
2. `crates/deja-record/src/lib.rs` — `dispatch_async` (async macro seam).
3. `crates/deja-record/src/lib.rs` — `dispatch_with_hook` / `dispatch_async_with_hook`
   (the trait-delegate seam).
4. `crates/deja/src/lib.rs` — `record_query_async` (the hand-written DB seam; DB
   does **not** use the macro dispatch).

Reaching the Lookup branch already implies non-Execute (the Execute branch either
runs with an execute-shadow token or fail-stops before the real boundary), so a
`None` there is always a Substitute-miss.

### Deferred (not in this change)

- **HIT-but-unreconstructable** (a recorded value that fails to deserialize) is
  *ambiguous* with an honestly-recorded `Err` skipped under the V1 "skip error
  arms" policy — the macro's reconstruct closure collapses both to `None`. So the
  HIT-unreconstructable fail-stop is **deferred** until the closure carries a
  tri-state (`Some(T) | ReconstructFailed | RecordedErrSkip`); for now that case
  keeps the V1 fall-through. Only the **true miss** (`replay_boundary` returned
  `None`) fail-stops now. The DB seam already distinguishes a faithful recorded
  `Err` (`recover_err`) — that stays a faithful HIT, not a miss.
- The old `Policy` enum / `DEJA_POLICY` / `DEJA_EXECUTE_OPS` demo and API
  plumbing is deleted; routing is policy-free.

## Routing — policy-free

`boundary_execute_mode_for` no longer consults a policy: a site maps its
`ReplayStrategy` directly (`Execute → Execute`, `Substitute → Lookup`), and an
undeclared site defaults to `Lookup` (Substitute). State seeding no longer reads
boundary/method-name heuristics; it consumes the explicit `read_set`/`write_set`
captured on the tape. The hand-written DB seam declares `Execute` and records
explicit state keys through `QuerySpec`.

## Demo

No A/B columns (no policy). One intermix run per candidate, checked against the
expected call graph + replay report: `self`/`benign` PASS; `real`/`earlier-fork`/
`extra-call` caught; `eu-overcharge` caught at the re-keyed Substitute read
(fail-stop before the stale rate can poison the write); `dropped-write`/
`response-only` caught by omitted-call / HTTP-body detectors.
