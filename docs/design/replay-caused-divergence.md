# Divergences the replay process causes itself

A same-image replay — the recording's own image replayed against the recording —
must match every correlation. Every divergence it reports is therefore a real
property of the environment, or a defect in deja, or — the case the gate section
below turned up — a latent defect in the candidate that only shows once the same
value is rendered twice and the two are compared. This document is the second
list: the diffs deja manufactures, grouped by root cause rather than by symptom,
because the symptoms fan out and the causes do not.

Measured on `rp-sbx-bb148328f7-bb14832-08121241-ki-0812141147885`
(candidate `bb148328f7-release-fast`, recording `rec-bb14832-08121241-ki`,
73 correlations, 33 passed).

## The 40 failures, attributed

| count | cause | owner |
| --- | --- | --- |
| 11 | recorder-schema drift only (`cards_info` migration, `business_label` default) | environment |
| 9 | candidate fail-stopped: connection closed with no response | **RC3** |
| 9 | `payment_attempt` row compared against a different statement's row | **RC2** |
| 4 | body differs with `side_effect_divergences = 0`, all `/payments/redirect/…` | **RC5** |
| 4 | `modified_at` freshness (recorded `13:42:58` → observed `14:13:37`) | **RC5** |
| 2 | novel `km` decrypt call | **RC1** |
| 1 | misc | — |

178 of the 182 omitted calls lie inside the 9 fail-stopped correlations.
Omission is not a class; it is the fail-stop's cascade. Counting it as a class is
what made this residue look four times larger than it is.

## The root causes

### RC1 — a call's replay address is not a faithful identity

The lookup addresses a call by `(correlation, bucket, span path, args hash,
occurrence)`. Any drift in any component is a miss. Measured at
`update_payment_attempt_with_attempt_id`: the candidate makes its normal 2–3
updates and 10 of them do not resolve (`resolved=false`, `rank=None`). The one
that resolves is a 3-column update; the two that miss are an 18-column confirm
and a connector-response update whose args carry connector-derived values.

Because db generics declare `replay = Execute`, a miss there is harmless to
behaviour — the statement runs against the seeded schema. At a `Substitute`
boundary the same miss is fatal (RC3). **Not yet fixed**; the drifting component
is not yet proven, and guessing it is how two earlier cycles were lost.

### RC2 — an unpaired event is re-partnered instead of reported

**Fixed.** The args-free pairing existed so a write whose operand diverged still
pairs with its recorded twin instead of splitting into `OmittedCall` +
`NovelCall`. Dropping args *entirely* to achieve that made the pool too wide:
every call through one kit function at one span shared one FIFO queue. With a
tape order of `[confirm, small, small, connector]` and a candidate order of
`[confirm, connector, small]`, an ordered assignment yields exactly
`matched, value_diverged, value_diverged, omitted` — the ten fabricated
`payment_attempt` divergences, including one whose "observed" operand was a
`payment_intent` row.

The fix pairs on the **statement**, not on its operands. `pairing_shape` derives
the part of the args a value divergence cannot change: for SQL, the text before
diesel's ` -- binds: [...]` tail; otherwise the structural skeleton plus the args
fields deja's own contract defines as identity (`operation`, `table`, `cache`,
`endpoint`). `key` is deliberately excluded — a re-keyed write is the very
divergence this pairing must still recover. A shape we cannot see stays a
wildcard and pairs as before, so an event with no `BoundaryEvent` is not forced
into a false mismatch.

### RC3 — a codec failure is anonymous, and fatal

**Partly fixed.** An unreconstructable `Substitute` hit calls
`fail_stop_substitute_unreconstructable`, which panics; the worker unwinds and
the kernel records `server closed the connection without writing a response`.
`Reconstructed::Failed` was a unit variant, so the serde error and the concrete
type were discarded at the seam. A codec incompatibility was therefore
indistinguishable from a network fault, in a pod whose logs are dropped.

That anonymity cost three cycles: `DirValue::Connector`'s `skip_deserializing`
twice, then `Encryptable`'s two-halves change, which made every pre-change tape
unreadable and produced a 77 % crash rate read as a scoring regression.

`Reconstructed::Failed` now carries the reason, built on the cold path, and every
generated codec names the type it failed on plus the serde error. Remaining gap:
`ReplayCodec::reconstruct` returns `Option`, so a *custom* codec still has no
channel for a reason. Closing it means widening the trait's return type.

The second half of RC3 was the fail-stop's *scope*, not its anonymity. The model
assumed the host isolated a per-request panic; actix does not (no `catch_unwind`
in actix-web 4.11 / actix-http 3.11 / actix-server 2.6), so the reason — however
well named — never left the process, because no response was written and this
pod's logs are dropped. `deja::catch_fail_stop_async` now contains the unwind at
the request boundary and puts the message in a 5xx body, which turns these 9
correlations from transport faults into scored divergences. It is opt-in by
construction: deja cannot wrap a request it does not own, so until the ingress
middleware calls it the fail-stop still escapes to actix. Whether it is wired is
readable off the artifacts — a fail-stopped correlation with `transport_error`
set means the guard is absent; one with a 5xx and the sentinel in its body means
it is present.

### RC4 — state-key precision degrades silently

The seeder needs a name for the state a read observed. A **row key**
(`payment_intent` where `payment_id=X` and `merchant_id=Y`) names one row; a
**query fingerprint** names a result. The RMW pre-image rule — "the precondition
is the row's last observed state" — can only be asked of a row.

`payment_intent` and `payment_attempt` record **no** row keys, while `customers`
and `address` do. Two independent reasons, both verified:

- The boot-time `pg_index` read fails open by design, so a table missing from the
  registry silently yields fingerprints.
- `binds_read_keys` looks for the literal `"merchant_id" = $`, and the payment
  tables' predicate is `"processor_merchant_id" = $12`, which cannot match.

Seeding is unaffected — all 556 rows landed through binary COPY, none via insert,
`readback_missing: 0`. So this costs zero divergences today and blocks the
pre-image work indefinitely. **Not fixed**; the decision is to make the
degradation counted and named rather than to fail loud in the recording path.

### RC5 — values that never crossed an instrumented boundary

Four correlations differ in the response body with **every** boundary matched and
status equal. Four more differ only in `modified_at`, where the observed value is
replay wall-clock. Both mean something outside the instrumented set reached
observable state — a database-side `now()` is the leading candidate for the
second, since deja can substitute the application's clock but not the store's.
**Not fixed**; needs diagnosis before code.

## The standing property

Four of these five are the same failure shape: a stage cannot answer, fails open,
and a later stage invents an answer instead of reporting the gap. The repo's own
discipline already names the defence —

> Accounting that must balance: every input line becomes an output or a named
> drop, and the totals are asserted, not logged.

Applied here: every tape event resolves to exactly one verdict or one named drop;
every observed call is claimed once or named novel; every state key is row-exact
or degraded **with a reason**; every reconstruction failure names its type. The
scorer already asserts the first of these
(`consumed ∩ paired_consumed` must be empty). That assertion held through this
run — the double-claimed rows were in the emitted ledger, not the tally, so the
artifact humans read disagreed with the verdict by two rows. An accounting rule
that binds the summary but not the artifact leaves the diagnosis unguarded.

## Gate

The conformance gate tested that a recorded value *parses*. It now tests that it
*survives*: reconstruct, re-capture, and prove no value the tape carried was
dropped. `Serialize` and `Deserialize` are independent impls, and every codec
defect above parsed cleanly while losing data. The check is one-directional —
additions lose nothing — so it does not fire on benign asymmetry.

Calibrating it against a real tape mattered more than writing it. The first
version compared arrays position-wise and reported **53** losses; 48 were its own
false alarms, because `ConstraintGraph`'s `InAggregator` is an `FxHashSet` and its
JSON order is therefore nondeterministic. Arrays now compare as multisets — a
dropped or altered element still changes one, reordering does not — and the
failure message carries both sides, so the next reader is not left guessing
between precision, format and ordering.

That false alarm exposed a real property worth stating on its own: **a captured
constraint graph does not serialize deterministically.** Two captures of the same
graph differ in the order of every `InAggregator`, so any byte-comparison of graph
values is order-sensitive by accident. It costs nothing today (the graph
substitutes and `imc` reports 3 divergences, all novel) and it would silently
defeat content-addressed payload dedup, which the IMC design doc proposes as the
honest fix for those values being ~60 % of imc bytes.

Five failures survived the first calibration run, all `PaymentIntent.created_at`,
and once the message carried both sides it named itself:

```
the tape has "2026-08-12T13:33:02.938Z", the round trip produced "2026-08-12T13:33:02.937Z"
```

**A millisecond, lost downward — in SERIALIZATION.** The first reading of this
blamed the parse ("`.938` deserializes to marginally under 938 ms"). It is the
other half. `time` 0.3.41 renders an ISO 8601 subsecond by building
`seconds + nanoseconds / 1e9` as an `f64` and truncating it
(`src/formatting/iso8601.rs:108`), so a value whose fractional part is not
exactly representable as a double renders one millisecond EARLY. Deserialization
is exact; `Se(De(x)) ≠ x` because `Se` is wrong, and asking for more decimal
digits does not help — it just exposes the `.937999999` underneath.

That distinction is the whole point, because it moves the defect out of this
document's subject. **This is not a divergence replay caused.** It is a
correctness bug in every API response hyperswitch has ever emitted carrying an
affected timestamp; replay is merely the only thing that ever rendered the same
instant twice and compared the two. The tape did not lie — the recording and the
replay were both handed a value the formatter had already corrupted.

**No single percentage describes its incidence**, which is the part the first
write-up of this got wrong in a second way. The whole-seconds term shifts where
the sum lands in the double, so the set of broken milliseconds moves with it.
Measured against the old formatter across five seconds × 1000 milliseconds:
**318 of 5000** pairs render early — none at `:00`, 86 at `:01`, 47 at `:02`, 185
at `:32`, none at `:59`. The `:02` figure is the one that was first quoted, as
though 4.7 % were the global rate; it is near the best case. Three of 138 values
happened to be affected on this tape.

That also made the first regression test too weak to be a guard: it pinned the
`:02` fixture, so it caught 47 of those 318 and would have passed with the bug
live at a six-times-worse second — and had it pinned `:59` it would have reported
the formatter as clean. The test now sweeps `:00`, `:01`, `:02`, `:32`, `:59`,
and was checked by reverting the fix under it: 318 failures, then zero.

Fixed in `common_utils::custom_serde::iso8601` by rendering the subsecond field
from the integer nanosecond count through an explicit `format_description!` item
list, which is exact for all 1000 values and byte-identical to the well-known
formatter's output wherever that formatter was already right — so no API response
shape changes. Not fixed by upgrading `time`: upstream corrected this by 0.3.55,
and every release carrying the fix requires Rust 1.88 while this workspace
declares 1.85, so the bump would trade a correctness bug for an MSRV failure.

**The sibling module `iso8601custom` was never affected, for a reason worth
writing down** — it reads like it should have been, and the next person to check
will reach the wrong conclusion twice. It configures `decimal_digits: None`,
which looks like "variable precision" and therefore like the same `f64` render at
full width. It is not: `format_float`'s `None` arm is
`let value = value.trunc() as u64` (`src/formatting/mod.rs:93`), which discards
the fraction rather than rendering it. That format emits whole seconds, so it had
no subsecond to get wrong, and the integer part of `seconds + nanoseconds / 1e9`
is exact for every input because the addend is always in `[0, 1)`. It was
rewritten onto an explicit item list anyway — the `f64` path is one
`decimal_digits` edit from acquiring the defect, and the old code derived its
`YYYY-MM-DD HH:MM:SS` wire shape by running the ISO 8601 formatter and then
string-replacing `T` and `Z`. A rewrite with no bug to fix has to prove it
changed nothing, so its test keeps the old formatter as an oracle and compares
byte for byte.

This is also the best current explanation for RC5's four `/payments/redirect/…`
failures, which mismatch on the response body with **zero** side-effect
divergences, and the formatting attribution makes it a better fit than the
parse one did: the substituted row equals the recorded row at the boundary, so
nothing is scored, and the corruption is introduced when that row is re-serialized
into the body. Not yet proven — the next step is to confirm a lost millisecond
appears in one of those four bodies.

Separately and structurally: postgres stores microseconds and this codec emits
milliseconds, so the serde image of a timestamp is lossy against the physical row
**by construction**. That is the standing argument for seeding from the wire image
rather than the serde image wherever both exist.

One reporting gap remains. When a multiset element has no match, the message names
the element (`(root)[0]`) rather than the differing leaf inside it, because the
search cannot know which candidate it "should" have matched. Recursing into the
nearest candidate to name the leaf would close it.

Still open on the gate: polymorphic caches are probed against a candidate list
and pass if ANY type parses, while the runtime demands one exact type. `imc`
values carry no `type_name` on the tape (db and km do), which is why the guess
exists. Stamping the captured type at capture time would make the gate exact and
delete the guess.

## What a green gate does and does not say

The conformance gate asserts that every recorded value reconstructs — that
`Se(De(x))` equals `x` for each value the tape carries. That is worth asserting,
because a type that serialises without deserialising fails at replay time and
nowhere earlier, where no compiler and no write-only test can see it. But the
property is narrower than it looks, and the natural misreading is expensive.

The gate compares the tape against itself. It cannot compare the tape against
the world. If a value was already wrong when it was written, both sides of the
round trip agree on the wrong value and the check passes — not by accident, but
because the question it asks does not reach that far.

This is not hypothetical. `time` 0.3.41 renders an ISO 8601 subsecond by
building `seconds + nanoseconds/1e9` as an `f64` and truncating, so a fraction
that is not exactly representable renders one millisecond early: a true `.939`
is written `.938`. Every tape recorded before that fix carries the error baked
into its timestamps, and every one of them passes the gate on exactly those
values, because `.938` reconstructs to `.938` faithfully. Replay was simply the
first thing that ever rendered the same instant twice and compared the results,
which is why the defect surfaced here rather than in the years of production
responses that had been carrying it.

So a green gate says the tape is SELF-CONSISTENT: replay will substitute what
the recording holds. It does not say the recording holds what the system
actually did. Two things follow. The first genuinely correct timestamps arrive
only with the first recording made after the fix. And any comparison spanning
that boundary sets a wrong-but-consistent tape against a right one, which will
present as divergence and is not.

Surfaced by the conformance gate on `rec-bb14832-08121241-ki`.
