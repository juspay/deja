# Déjà

Déjà makes a service run deterministically replayable: it **records** every
interaction with an external system (database, Redis, HTTP, crypto/id/time
entropy) during a live run, then **replays** that run byte-exactly —
substituting the recorded values in place of the live systems. The same
recording then acts as a regression gate: replay it against a *changed* build
and score the divergence.

The reference integration is [Hyperswitch](https://github.com/juspay/hyperswitch)
(vendored at `vendor/hyperswitch-deja-clean` with the integration patch on a
branch). The full architecture and technical reference lives at
[docs/DEJA_RECORDING_ARCHITECTURE.md](docs/DEJA_RECORDING_ARCHITECTURE.md).

## How it works

- **Annotation macros** — boundaries are instrumented with feature-gated
  attribute macros (`#[cfg_attr(feature = "deja", deja::redis(...))]`,
  `deja::id`, `deja::time`, `deja::http`, `deja::boundary`, plus a
  `#[deja::recordable]` trait decorator and a db query macro). With the feature
  off, the integration compiles to the unpatched upstream code.
- **Record** — macro-generated code emits one `SemanticEvent` per boundary call
  (args, result, correlation id, callsite identity) through a `RuntimeHook` to
  JSONL and/or Kafka sinks.
- **Replay** — an orchestrator renders a **lookup table** from the recording;
  the candidate boots with `DEJA_MODE=replay` and a `LookupTableHook` that
  substitutes recorded results per call. Every lookup emits an `ObservedCall`;
  a post-hoc divergence detector scores the run into a verdict + scorecard.
- **Cross-version addressing** — lookups resolve through a 6-rank address
  ladder (explicit · logical span-path · syntactic hash · lexical path · source
  location · positional). Rank 2, the span-path, survives line shifts and
  disambiguates concurrent same-callsite calls; see
  [docs/LOGICAL_CONTEXT_ADDRESSING.md](docs/LOGICAL_CONTEXT_ADDRESSING.md).

## Crate map

| Crate | Role |
|---|---|
| `deja` | Facade: macro re-exports, payload helpers, `__private` macro support |
| `deja-derive` | The attribute macros (`boundary`/`instrument` family, `recordable`) |
| `deja-record` | Recording + replay runtime: `SemanticEvent`, hooks, sinks, writer, the address ladder |
| `deja-core` | Event schema + artifact validation |
| `deja-context` | Correlation context (thread/task-local snapshots) |
| `replay-harness-api` | Orchestrator: run lifecycle, lookup-table renderer, divergence scorecard |
| `replay-harness-kernel` | Replay driver: re-drives the recorded ingress requests |
| `deja-tui` | Interactive record→replay substitution explorer |

## Demo (record → Kafka → MinIO → replay → verdict)

```sh
# requires docker (+compose), cargo, curl, jq; put a Stripe TEST key in demo/.env
demo/run-deja-demo.sh                      # record + self-replay, PASS/FAIL verdict
demo/run-deja-demo.sh --cross-version benign-line-shift   # V2 replay vs V1 recording
demo/run-deja-matrix.sh                    # one recording, three candidates (self/benign/real)
```

## Development

```sh
just verify    # fmt-check + clippy -D warnings + tests
```

## History

Déjà began as a zero-code, syscall-level (`LD_PRELOAD`) capture/replay
experiment before pivoting to the current semantic, annotation-based
architecture. The design documents from those earlier tracks are preserved
under [docs/history/](docs/history/).
