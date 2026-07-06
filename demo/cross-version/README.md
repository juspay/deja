# Cross-version candidate patches

These are **V2 candidate** patches for the cross-version mode of
`demo/run-deja-demo.sh`:

```
demo/run-deja-demo.sh --cross-version benign-line-shift   # expect PASS  (no false divergence)
demo/run-deja-demo.sh --cross-version real-change         # expect DIVERGENCE (the gate works)
# or:  --candidate-patch demo/cross-version/<file>.patch
```

## How it works

The demo records on **V1** (the current source), then — between record and replay —
applies one of these patches to `vendor/hyperswitch-deja-clean`, rebuilds the host
`router` binary, and replays **that V2 binary** against the V1 recording. The
Dockerfile bakes the host binary, so the replay container runs V2 while the still-
running V1 record container stays pinned to its V1 image. The patch is reverted on
every exit (success / failure / Ctrl-C).

## Constraints (enforced by the script)

- **Vendor-only.** A patch must NOT touch the parent `crates/deja*` instrumentation —
  it must be byte-identical across V1 and V2, or a divergence would be an
  instrumentation artifact rather than a real version diff. The script rejects any
  patch whose diff headers reference `crates/deja`.
- **Must change the binary.** The script asserts the rebuilt router's sha256 differs
  from V1; a no-op patch (which would cache-hit the Docker `COPY` layer and silently
  replay V1) fails loudly.
- Patches are generated against a CLEAN target file (`git -C vendor diff -- <file>`)
  so they apply additively on top of the dirty vendor tree and reverse cleanly.

## The patches

- **`benign-line-shift.patch`** — inserts a comment block above the `PaymentCreate`
  operation in `payment_create.rs`. Every `#[track_caller]` boundary line below it
  shifts, so the rank-5 `SourceLocation` address differs between V1 and V2 — yet every
  boundary still resolves via the version-stable rank-2 `LogicalContext` / rank-3
  `SyntacticHash`. No args change → **no false divergence**.
- **`real-change.patch`** — changes the `payment_attempt` insert's `updated_by`
  column from `""` to `"v2-candidate"`. That value flows into the recorded `db`
  insert, so the candidate's DB write no longer byte-matches V1. Current scoring
  reports this as a DB value divergence with the HTTP response still byte-identical.
- **`transitive-chain.patch`** — mutates the known `bank_code` leaf inside the
  `ConfirmUpdate` `payment_attempt.payment_method_data` JSON. The live row returned
  from that DB Execute becomes the in-memory attempt used by the later response
  tracker, so the downstream `ResponseUpdate` update args should diverge even
  though `payment_response.rs` itself is unpatched.

## The active default matrix (`run-deja-matrix.sh`)

The default matrix records ONE golden V1 baseline and replays the active
candidates that still apply to the current vendor source. Beyond
`self`/`benign`/`real`, five scenarios cover distinct detector shapes
(classification × boundary, plus a live transitive Execute chain), proving the
gate catches more than one regression class. (Each correlation is an independent test case;
see the platform design on per-case isolation / parallel replay.)

| patch | change | divergence cell | current signature |
|---|---|---|---|
| `real-change` | arg into the **attempt** insert | value-diverged · db | `matched=8/9`, `db 67/68`, one DB value divergence, no HTTP diff |
| `earlier-fork` | arg into the **intent** insert (fires *before* the attempt) | value-diverged · db | same shape as `real-change`, but fork origin is earlier than the attempt write |
| `dropped-write` | candidate skips a fire-and-forget redis cache populate (`if false`) | **omitted-only** · redis | 10 omitted `set_key` calls, **0 novel, 0 HTTP diff** — a silent lost write |
| `response-only` | overrides one response field (`amount`), no db/redis call touched by the patch | **HTTP body** · http_incoming | 2 body mismatches plus one value divergence; db/id/time match and redis has only the existing idempotent-value warning |
| `extra-call` | candidate issues a `db` find V1 never made | **novel-only** · db | 1 novel DB call, no omitted calls, no HTTP diff |
| `transitive-chain` | `bank_code` changed inside `payment_method_data` at **ConfirmUpdate DB2**, then consumed by **ResponseUpdate DB3** | value-diverged · db | `matched=8/9`, `db 65/68`, three DB value divergences (ConfirmUpdate origin, ResponseUpdate consequence, final attempt-write consequence), **0 novel, 0 omitted, 0 HTTP diff** |

The `dropped-write`, `response-only`, and `transitive-chain` cases are the
important coverage anchors: a regression that's **invisible in the HTTP response**
(a dropped side-effect), one that's **invisible in the side-effects** (a wrong
response value), and one that proves a live 3-node chain where one Execute DB
update returns mutated data that changes a later Execute DB update's args without
patching the later code path.

`transitive-chain` deliberately writes `bank_code = "deja-confirm-update-v2"`
inside the existing externally tagged `AdditionalPaymentData::Card` payload, not
as an unknown JSON key. `payment_response.rs` parses and re-encodes the row
through `payment_attempt.get_payment_method_data()` →
`update_additional_payment_data_with_connector_response_pm_data()` →
`parse_value::<AdditionalPaymentData>()`, and `add_connector_response_to_additional_payment_data`
preserves non-overwritten card fields via `..*additional_card_data.clone()`. The
candidate is proven only if the downstream ResponseUpdate DB call diverges; the
current clean signature is three DB value divergences with no novel/omitted calls
and no HTTP diff.

## Retired/stale candidate: `retired/eu-overcharge.patch`

`eu-overcharge.patch` is kept only under `demo/cross-version/retired/`, not in
the active patch glob checked by the matrix preflight. It targeted the old Deja
demo-only `eu_settlement_read` / `eu_settlement_write` injection in
`payment_create.rs`; that injection has been removed from the vendor-thin source,
so the patch no longer applies cleanly to the current default vendor tree.

Do not count `eu-overcharge` as a passing matrix candidate. Its intended coverage
is still valuable: a data-driven Substitute READ whose result flows into a
fire-and-forget Execute WRITE, producing a write-only divergence with an identical
HTTP body. A future transitive read→write candidate should be rebased or
reintroduced separately against current vendor source before being restored to the
default matrix.
