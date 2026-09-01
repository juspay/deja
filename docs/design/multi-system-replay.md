# Multi-system replay: one déjà deployment for hyperswitch AND hyperswitch-prism

Status: implemented (working tree), validated end-to-end against a live
hyperswitch-prism (UCS) server in both its gRPC and HTTP modes.

## The problem

Déjà was built around one recorded system. Three of its layers hardcoded that
system's shape:

1. **Tapes** — "the ingress event" was identified by the literal boundary name
   `http_incoming` at every consumer (kernel, renderer, replay-hook allowlist,
   scorer ×6, scope index, S3 admission, compactor, TUI). A system whose
   ingress is gRPC (`grpc_incoming`) needed a relabel shim before rendering.
2. **The driver** — deja-kernel reconstructs and re-drives HTTP/1.1 only. A
   gRPC recording had no production driver.
3. **Runs** — the replay lifecycle bakes in hyperswitch's boot contract
   (`ROUTER__DEJA__*` env binding, hyperswitch compose service, pg/redis
   seed stages). There was no way to say which system a run drives.

## D-1 — self-describing tapes: `role: "ingress"`

`BoundaryEvent` (and `ObservedCall`) gain an optional `role` field; ingress
recorders stamp `ROLE_INGRESS` via `EventBuilder::with_role`. THE membership
test is `BoundaryEvent::is_ingress()`:

```text
role == "ingress"  ||  boundary == "http_incoming"   // legacy-name fallback
```

Every load-bearing consumer now goes through it (or its observed/probe-struct
equivalents): kernel reconstruct, lookup renderer skip, `ReplayHook::record`
allowlist, scorer (`is_nonblocking_boundary`/`omission_is_blocking` take the
row's role; ingress-map; observed-row skips; undeclared-concurrency
finalization), scope's `TapeIndex`, S3 admission (`TerminalResponse`),
compactor `has_ingress`, TUI. The graph-side ingress-root check generalizes to
the span-name convention `deja::<boundary>` + `_incoming` suffix.

The field is additive within event-schema v8: absent on old tapes, defaulted
on read, never required. Old tapes, old observed artifacts, and the derive
macro's `http(incoming)` preset are untouched — zero hyperswitch change needed
now; when hyperswitch bumps its pin its recorder MAY add `.with_role(...)`,
and until then the name fallback covers it forever.

## D-2 — the gRPC drive path, inside deja-kernel

One driver binary, one `KERNEL_*` contract, dispatch per correlation on the
recorded ingress shape: `method`/`path` → the existing HTTP/1.1 client;
`rpc` → `deja_kernel::grpc` (new module):

- request bytes from `request.request.raw_b64` (transport bytes verbatim,
  already gRPC-framed), or re-encoded from `request.request.decoded`
  (proto3-JSON) through an optional descriptor pool;
- driven over HTTP/2 (`h2` + a per-drive current-thread tokio runtime — the
  kernel's thread-pool concurrency model is untouched); grpc-status read from
  trailers, or headers on trailers-only responses;
- outcome baseline: recorded `response.grpc_status` when the recorder stamped
  it, else the event's `is_error`; statuses map onto the HTTP driver's
  symmetric 200/500 contract;
- body diff: field-level proto3-JSON via `KERNEL_DESCRIPTOR_SET` (a compiled
  `FileDescriptorSet`, e.g. the server's own build artifact), byte-exact
  otherwise — a byte mismatch without descriptors is one opaque root diff,
  never silently dropped;
- rows are the same `HttpDiff` shape (request_path = the rpc), so the sink,
  scorer, and dashboard need nothing new.

New env: `KERNEL_DESCRIPTOR_SET` (optional). New deps (kernel only): tokio/h2/
http/bytes/base64/prost/prost-reflect — none pull url/idna (the icu ≥ rustc
1.86 hazard the HTTP driver hand-rolls around).

## D-3 — `system_under_test` on the run

`RunSpec.system_under_test: Option<String>` (serde-default; absent =
`hyperswitch`, so every existing caller, stored row, and curl recipe keeps its
meaning; read via `RunSpec::system()`). Mirrored on `RunParams`, so it rides
the existing `params` jsonb — no store migration.

- **k8s executor**: the candidate env binding and config-copy source resolve
  per run. The default system keeps the base profile; others read
  `DEJA_<SYSTEM>_CANDIDATE_{MODE,RUN_ID,SOURCE,OBSERVED,CODE_SHA}_ENV` and
  `DEJA_<SYSTEM>_CONFIG_SOURCE_{DEPLOYMENT,CONTAINER}`, with shipped defaults
  for `prism` (`CS__DEJA__*`) and config-copy off unless configured.
- **compose lifecycle**: the replay service is `<system>-replay` (the default
  system resolves to the existing `hyperswitch-replay`), and the pg/redis
  migrate/schema-gate/flush/seed stages run only for the default system —
  prism is stateless in UCS (every store crossing is a recorded boundary).
- **dashboard**: a system picker on New Run (only non-default values are
  sent), the system in the runs-list row/search, and in the client-side
  attempt-subject key (same recording driven against two systems = two
  experiments).

## What the recorded system must do (prism-side follow-ups)

- stamp `.with_role(deja::ROLE_INGRESS)` on both its ingress layers, after
  bumping its deja pin;
- record `raw_b64` alongside `decoded` on gRPC ingress (drives without
  descriptors), and stamp `response.grpc_status` — today prism records
  `is_error: false` + an empty response partial even for failed rpcs, which
  reads as a false 200 baseline (the validation run surfaced exactly this);
- ship its `FileDescriptorSet` where the kernel can read it
  (`KERNEL_DESCRIPTOR_SET`) for field-level response diffs.

## Validation

- Full workspace: fmt clean, clippy clean, all tests pass (incl. new tests
  pinning: role-based renderer skip + legacy fallback, kernel role/gRPC
  reconstruct + dispatch, `grpc_status` baseline precedence, byte-exact gate,
  metadata filter, `with_role` stamping).
- MSRV job (`cargo +1.85.0 check -p deja -p deja-context -p deja-core -p
  deja-derive`) passes.
- Live E2E vs hyperswitch-prism: a real gRPC-mode tape (role injected at
  staging, simulating the post-pin-bump recorder) rendered with NO relabel,
  driven by this kernel over h2 with descriptor re-encode, egress substituted
  (rank 2) with the connector dead — scorer verdict PASS 1/1, 0 body diffs.
  The same binary then drove a legacy HTTP-mode tape (no `role` anywhere)
  through the HTTP path: PASS 1/1, 0 diffs — the no-regression proof.
