# Seeding audit — what the mechanism does, measured on a real run

Run: `rp-sbx-adf4845317-adf4845-08062232-85-0807082100244` — recording
`rec-adf4845-08062232-85` replayed against the image that recorded it
(`adf4845317`), on the instrumented stack (deja `195940b817`). Every number
below came from the run's own artifacts over plain GETs: seed certificate,
lookup table, observed stream, call ledger, scorecard.

## The accounting (seed certificate summary)

| counter | value | meaning |
|---|---|---|
| planned | 937 | seed entries the planner produced |
| materialized | 621 | landed in the correlation's store and (mostly) read back |
| failed | **153** | renderer refused — **every one `payment_attempt`**, message `could not render an insert` |
| skipped | 163 | nothing to seed (Err/miss results; non-scalar redis values) |
| not_preconditions | 219 | reads concluded not to be preconditions (read-after-write / self-created table), now on the record — named `masked` on this run's certificate, renamed since (#40) |
| readback_matched / missing | 528 / 93 | seeded rows verified / not found on readback |

`planned + not_preconditions` now accounts for every state-read the planner was
shown — the identity that used to be unverifiable.

## Verdict trajectory

`matched_correlations: 10/100`, unchanged from the pre-audit run — as expected:
this phase changed the *instrument*, not the seeding. The scorecard now carries
the framing inline:

> 153 seed entries FAILED to materialize (tables: payment_attempt) — reads of
> those rows replay against an empty table, so their divergences describe the
> missing seed, not the candidate

## Family A — CONFIRMED (issue #35)

Recorded `payment_attempt` rows carry `connector_transaction_id: {"TxnId": …}`
(externally tagged enum, no serde attr) into a `Nullable<Varchar>` column. The
fail-closed renderer arm refuses; the whole entry fails before SQL; the
candidate's `find_one(payment_attempt)` executes against an empty table →
NotFound → 404 where the recording answered 200. 153 failed entries; the 70
entries whose value is `null` materialize — the exact mixed behaviour observed.

Fix fork (user decision, see #35): (a) revive the dead producer-metadata path
(`row_image_payload_with_metadata`) so typed column info travels with rows —
recommended; (b) vendor serde change on `ConnectorTransactionId`; (c) renderer
unwrap heuristic — recommend against (fail-closed is load-bearing).

## Family B — OPEN (issue #36)

24 correlations: one novel `find_one(business_profile)` each, 108 recorded imc
hits unconsumed, whole flow truncated. imc is declared Substitute
(docs/design/cache-isolation.md); the chain did not engage. Next analysis:
observed × lookup imc-entry join to separate identity-miss from call-never-made.

## Family C — likely A's mechanism, verify after the A fix

payment_intent (4) and merchant_key_store (1) NotFound forks: payment_intent
materializes 174 entries in this run, so these are either different columns or
family-B cascades. Re-measure after the A fix; the certificate now names either
outcome.

## Family D — bounded (issue #37)

4 deterministic `no header/body separator` transport errors (same 4
correlations both runs): the router closed with zero bytes; the kernel's
minimal client surfaces it, named per row since #33.

## Also observed, parked

- `readback_missing: 93` on materialized entries — seeded rows a readback query
  did not find. Quantified but not yet classified; candidates: readback query
  shape vs multi-row entries. Gets its own pass after A.
- `skipped` concentrates on `incremental_authorization` (54), `dispute` (27),
  `refund` (27) — recorded miss/Err results with no rows to seed; correct
  behaviour, listed for completeness.

## Method note

No tape export was needed: the replay pipeline is the tape reader. Wider tape
coverage = more runs over different correlation windows. Analysis reused
production readers throughout; predictions were cross-checked against the call
ledger before being written down.
