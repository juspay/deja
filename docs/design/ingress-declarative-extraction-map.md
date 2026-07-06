# Ingress + correlation/graph layers — extraction map (input to OMP's DSL layer)

**Status: DESIGN INPUT, no code changed.** User decisions (2026-07-02): (1) compose/vector stay in
the vendor (the record→Kafka→Vector→S3 push path is the real pipeline the hosted replay
orchestrator will fetch from; the overlay is how HS carries it for local dev — matches
`packaging.md:192`). (2) The `request_id.rs` ingress capture + correlation/span-path layers should
move to the library behind a **declarative generic mechanism** — OMP is building a DSL layer for
exactly this, so this document maps what exists and what the DSL must express, instead of
hand-rolling an extraction that the DSL would supersede.

## 1. What the vendor currently carries (the extraction inventory)

### `router_env/src/logger/setup.rs` (+38 — already thin)
Both layers are LIBRARY types (`deja::ExecutionGraphLayer` from `deja-record/src/graph.rs`,
`deja::DejaCorrelationLayer` from `deja-record/src/correlation_layer.rs`). Vendor only holds two
env-gated installer fns:
- `deja_layer()` — installs `ExecutionGraphLayer` iff `DEJA_GRAPH_DIR` set.
- `deja_correlation_layer()` — installs `DejaCorrelationLayer` iff `DEJA_MODE ∈ {record, replay}`.

Quick win (defer to DSL owner so the API shape matches the DSL): library constructors
`deja::graph_layer_from_env()` / `deja::correlation_layer_from_env()` so the install POLICY (which
mode gets which layer) lives in the library; vendor becomes two `.with(...)` one-liners.

### `router_env/src/request_id.rs` `semantic_boundary` module (+560 — the real target)
Four distinct pieces, currently interleaved:

**(a) Hook plumbing (generic, duplicated per seam).** `HOOK: OnceLock<Option<Arc<RuntimeHook>>>`,
`hook()`, `is_active()` — every hand-rolled seam re-implements this. DSL should provide ambient
hook access so no seam ever writes this again.

**(b) Id seam (`record_id_generation` / `replay_id_generation`)** — the uuid_v7 request-id
boundary. Hand-rolled (not `#[deja::boundary]`) because it runs in MIDDLEWARE, pre-span, where the
macro doesn't fit, and because the generated id **becomes** the correlation (there is no
correlation yet to inherit). Replay substitutes the recorded id (byte-exact reproduction) or
falls through to live generation on miss (scored). This is task #25's subject — fold into the DSL.

**(c) Ingress capture core (framework-agnostic once de-actix'd).**
- `IncomingHttpRecord` — method/path/query/request_id/headers/content_type/content_length/body.
- JSON shaping: `headers_json`, `body_json` (captured:false + error on extract failure),
  `partial_result_json`, `error_result_json`.
- Event lifecycle: `EventBuilder::start` at request arrival → **`deja::LazyEventFinalizer`
  finishes the event only when the RESPONSE BODY completes streaming** (not when the handler
  returns). Error arm records `error_result_json`.
All of this speaks only library primitives + serde_json — it belongs in `deja-record` verbatim.

**(d) Actix adapter surface (framework-typed, thin).**
- `capture_incoming_request(ServiceRequest, request_id)` — `request.extract::<Bytes>()` then
  `request.set_payload(...)` (body tee on the request side).
- `RecordingBody<B>: MessageBody` — tees the response body, finalizes the event at EOF/error.
- `RecordedIncomingFuture<B>` — wraps the service future; starts the event, attaches the
  finalizer to the response body, handles the error arm.
- Middleware wiring: `EitherBody`, `scope_correlation(request_id, fut).in_current_span()`.

### Correlation anchoring (two cooperating mechanisms — keep both in the design)
1. Middleware wraps the request future in `scope_correlation` (task-local for the request tree).
2. `DejaCorrelationLayer` mirrors the span's `request_id` FIELD into deja-context thread-local —
   for spawned tasks that escape the future wrapper but carry the span via `.in_current_span()`.
The DSL must preserve both, or spawned-task boundary events lose correlation (this exact gap
caused the confirm-404 class of bugs).

## 2. What the DSL declaration for INGRESS must be able to express

1. `boundary = http_incoming`, component, operation — as today's `#[deja::boundary]`.
2. **Request capture spec**: method/path/query/headers/body (today: full body, no size cap —
   DSL should make caps/redaction declarable; tape currently leaks secrets per the
   instrumentation audit).
3. **Deferred finalization semantics**: event finishes when the response body FINISHES STREAMING
   (LazyEventFinalizer), with a declared partial-result shape if the body never completes and an
   error-arm shape on handler error. This is the one semantic no current macro expresses.
4. **Correlation ESTABLISHMENT (not inheritance)**: this boundary *creates* the correlation from
   the request id (header reuse per `IdReuse::UseIncoming` or generated uuid_v7), then scopes it
   (task-local + span field). Distinct from every other boundary, which inherits.
5. **Driver semantics on replay**: http_incoming is the replay DRIVER — never substituted; the
   classifier already treats it as the request tier (non-blocking). The declaration should say so
   (`role = driver` vs `role = substitutable`).
6. **Sub-boundary**: the id-generation record/replay-substitute (b) with "output becomes
   correlation".
7. **Framework adapters**: generic core + per-framework glue. Actix is the only adapter today;
   the adapter surface is exactly (d): request-body tee, response-body tee w/ finalizer, future
   wrapper, correlation scoping.

## 3. Recommended target shape (for the DSL work to confirm)

- `deja-record::ingress` — core: record struct, JSON shaping, finalizer policy, hook plumbing (c+a).
- `deja-record::ingress::actix` (feature `actix`) — `RecordingBody`, `RecordedIncomingFuture`,
  `capture_incoming_request` (d).
- Vendor `request_id.rs` shrinks to: its stock RequestId middleware + ONE declarative install
  (e.g. `deja::ingress::actix::capture(request_id)` around the service call) + the id-seam
  declaration. Estimated residual: ~50–80 lines from today's ~560.
- `setup.rs` shrinks to two `.with(deja::…_from_env())` one-liners.

## 4. Verification contract for whoever lands this
Behavior-affecting (feeds recording): compile both feature sets, then full self-check must stay
**pass=true 9/9**, and the recording must still contain http_incoming events with the same shape
(the kernel drives replay from them) + id_generation events replay byte-exact.

## 5. Related
- Task #25 (request_id replay_id_generation decoupling) — same surface, fold into DSL.
- `docs/design/recording-capture-decoupled.md` (observations-not-verdicts, one dispatch seam) and
  `docs/design/declarative-boundary-model.md` — the DSL is the natural continuation of both.
- Instrumentation audit: tape leaks secrets → capture-spec redaction belongs in the declaration.
