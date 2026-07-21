# gRPC egress boundary — recording + substitution for outbound gRPC clients

**Status: PLAN-ONLY (design v3; nothing built). Decisions below are PROPOSED unless marked DECIDED or inherited-LOCKED.**

Scope: the vendor tree's outbound gRPC clients (`crates/external_services/src/grpc_client/`).
Target branch (**corrected 2026-07-13, user**): `juspay/hyperswitch:deja-pr` — the
canonical dev branch backing PR #13237 (worktree `vendor/hyperswitch`). NOT `deja-lean`
(the pre-re-home habit; its deja builds are also broken by outer tracing-substrate
drift, whereas deja-pr pins the published `juspay/deja@075a614`). Pushes to origin
remain approval-gated.

Revision history:
- v1: per-RPC macro boundaries (37 attribute sites, redis pattern) — REJECTED (user):
  wrong precedent; gRPC ops are uniform egress, not per-op semantics like redis.
- v2: single typed seam `deja_boundary::unary()` (DB-generics pattern) — REJECTED
  (user): still a wrapper convention; authors must route through it; drift risk on
  future methods.
- **v3 (this doc): transport-layer tower `Service` wrapper — the definition site is
  wrapped once; zero authoring/call-site changes, current AND future methods covered
  structurally.**

---

## 1. Why

A large share of hyperswitch's outbound calls are gRPC, and today they are a recording
blind spot — **zero** deja instrumentation under `grpc_client/` (the `deja` optional dep
exists in `external_services` but only `http_client` uses it).

Two of the three client families are *decision-relevant*:

- **Dynamic routing** (success-rate / elimination / contract clients) returns scores that
  pick the connector. Unrecorded, replay either hits a live scorer (forbidden egress) or
  the client is absent and the routing decision silently diverges from the recording.
- **UCS (unified connector service)** carries the actual payment operations
  (authorize/capture/refund/payouts/webhooks) when the UCS path is active — the gRPC
  analogue of the already-instrumented HTTP `send_request` chokepoint.

The `"grpc"` boundary tag is already pre-plumbed downstream: orchestrator tiers it
`Environmental` (`deja-orchestrator/src/divergence/mod.rs:44`), the TUI parses its args
like `http_outgoing` (`deja-tui/src/lib.rs:947,1009`), and
`partial-function-replay.md:79` locks the routing rule. Only the instrumentation itself
is missing.

## 2. Surface inventory (verified 2026-07-13, vendor `deja-lean` @ b3f0021d28)

| Family | Transport | RPCs | Router call path |
|---|---|---|---|
| UCS (`unified_connector_service.rs`) | `tonic::transport::Channel`, **eager** `connect()` + timeout at boot via `build_grpc_client!` (10×); `None` on failure | 26 wrapper methods over 10 service clients (all unary) | direct method calls from ~20 gateway files |
| Dynamic routing (`dynamic_routing/*.rs`) | shared `hyper_util` http2 pool (`type Client`, `grpc_client.rs:43-45`) + `with_origin` (lazy) | 10 trait methods: SR 4, elimination 3, contract 3 (all unary) | behind `SuccessBasedDynamicRouting` / `EliminationBasedRouting` / `ContractBasedDynamicRouting` traits |
| Recovery decider (`revenue_recovery/`) | same pool + `with_origin` | 1 (`decide`, unary) | `Box<dyn RecoveryDeciderClientInterface>`, one call site |
| Health check (`health_check_client.rs`) | same pool + `with_origin` | 1 (`grpc.health.v1/Check`) | health endpoint only; replay overlay disables deep health checks |

Facts that shape the design:

- **Every RPC is unary.** 14 local-proto rpcs + 43 UCS rpcs — zero `stream`. The
  deferred-but-reserved Source/Sink/Session shapes are NOT needed for v1.
- **The transports are constructed at exactly 2 definition sites**: the shared pool
  `Client` (once, `grpc_client.rs:82-142`) and `build_grpc_client!` (the one macro all
  10 UCS clients go through). Generated tonic clients are generic over the transport
  (`T: GrpcService<BoxBody>`), so wrapping at these 2 sites covers every RPC —
  **including methods and whole service clients added in the future** — with zero
  authoring discipline.
- **No tonic interceptors / tower layers exist today**; metadata is attached per-request
  by mutating `request.metadata_mut()`.
- **UCS metadata carries plaintext connector secrets** (`api_key`/`key1`/`api_secret`
  `.peek()`'d into `MetadataMap`, `unified_connector_service.rs:1084-1233`).
- **Descriptors are available for everything** (verified):
  `grpc_api_types::FILE_DESCRIPTOR_SET` is a **public const** at the pinned rev 71fcc81
  (`crates/types-traits/grpc-api-types/src/lib.rs:5-6` — `include_file_descriptor_set!`,
  emitted by its build.rs). Local protos: we own `external_services/build.rs`; add
  `.file_descriptor_set_path()` (one line). Note: the vendor's dep
  `unified-connector-service-client` re-exports only `{health_check, payments}` — the
  FDS needs a direct `grpc-api-types` git dep at the **same rev** (or a 1-line upstream
  re-export; see D2).
- Versions: tonic 0.14.5 / prost 0.14.3 / tonic-prost-build 0.14 in `external_services`.

## 3. Inherited constraints (LOCKED elsewhere, restated)

1. **Egress is always Substitute** — never re-issued (`partial-function-replay.md:79`).
   The transport wrapper hardcodes it; there is no knob to misuse.
2. `kind` is a free-text, non-routing label (#28) — `"grpc"` is already understood
   downstream.
3. Args must serialize **canonically** — `args_hash` is a lookup-key component with no
   arg-tolerant fallback.
4. Substitute miss = blocking `NovelCall` + fail-stop — the mechanism that *catches a
   candidate changing its outbound gRPC request*.
5. v1 boundary shape is unary `Call`. All 57 RPCs in the tree are unary; streaming
   arrives additively via the reserved shapes if ever needed.

## 4. Design (v3): wrap the transport, decode by descriptor

### 4.1 `DejaGrpcTransport<S>` — one tower `Service` wrapper (vendor-side)

New module `grpc_client/deja_transport.rs` (+ semantic companion), feature-gated:

```rust
pub struct DejaGrpcTransport<S> { inner: S, /* descriptor pool handle */ }

impl<S> tower::Service<http::Request<TonicBody>> for DejaGrpcTransport<S>
where S: tower::Service<http::Request<TonicBody>, Response = http::Response<S::Body>> ...
```

Installed at the 2 definition sites, under `feature = "deja"`:

```rust
// grpc_client.rs — covers dyn routing + health + recovery + anything future on the pool
#[cfg(not(feature = "deja"))]
pub type Client = hyper_util::client::legacy::Client<HttpConnector, Body>;
#[cfg(feature = "deja")]
pub type Client = DejaGrpcTransport<hyper_util::client::legacy::Client<HttpConnector, Body>>;
// (all generated-client struct fields / trait impls use the alias → follow automatically)

// unified_connector_service.rs — build_grpc_client! wraps each Channel the same way
```

**No wrapper convention, no call-site changes, no per-RPC anything.** A new UCS method,
or a whole new service client, flows through `Service::call` because that is how tonic
works — coverage is structural, not conventional.

### 4.2 The boundary inside `call()` — hand-written dispatch, runtime identity

Precedent: the DB seam (`record_deja_db_query!`) and the actix ingress seam — deja's
hand-written-seam pattern for places where a macro can't sit.

- **Identity** (refined 2026-07-13, user): the full Address ladder, with the rpc path as
  the rank-1 primary. The wrapper stamps `CallsiteIdentity { source: Explicit,
  id: Some(rpc_path), span_path: current_span_path(), syntax_hash:
  stable_callsite_hash("grpc::" + path), .. }`:
  - **rank 1 `Explicit(rpc_path)`** — the interface operation itself
    (`/types.PaymentService/Authorize`): the most version-independent identity an
    egress call has (changes only when the `.proto` contract changes). Verified
    settable by hand-written seams (`addresses_for`, `replay.rs:1372-1377`).
  - **rank 2 `SpanPath`** — automatic (`current_span_path()`, propagates through the
    polled future chain at the transport layer): the *calling-context* disambiguator —
    same RPC from different flows (e.g. `payment_authorize` from `authorize_gateway`
    vs `complete_authorize_gateway`) gets distinct rank-2 addresses, and concurrent
    same-callsite calls can't swap occurrences under async interleaving.
  - **rank 3 `SyntacticHash`** — runtime hash of `"grpc::" + path` (DB-seam precedent);
    an expansion-time hash would collapse all RPCs onto one value since the wrapper is
    a single lexical site.
  - ranks 5–6 (source location / sequence) automatic.
  Span-path is deliberately NOT the primary: it names the calling context (drifts with
  `#[instrument]` renames — survivable by ladder design), while the rpc path names the
  operation — identity is "what the interface operation is", context is the refinement.
- **Correlation**: `x-request-id` request header (attached by all three families; at
  this layer metadata IS http headers).
- **Semantics**: `BoundarySemantics { replay_strategy: Substitute (hardcoded),
  kind: Some("grpc"), declaration: effect=Grpc, op=ExternalCall }`.
- **Record mode**: buffer the request body (unary ⇒ bounded), forward to `inner`,
  buffer the response body + trailers, emit the observation, hand the buffered response
  up (same eager-buffer-and-rebuild trick as `http_client.rs:172-208`).
- **Replay (Substitute)**: never dial. Reconstruct `http::Response` from the recorded
  envelope and return it — **tonic's own decode path turns it back into the typed
  response or `Status`**. Miss ⇒ NovelCall + fail-stop, per the model.

### 4.3 Capture form: bytes are truth, descriptor-decoded JSON is the projection

The recorded envelope per call:

```jsonc
// args (canonical):
{ "rpc": "/types.PaymentService/Authorize",
  "authority": "ucs.internal:8000",
  "metadata": [["x-merchant-id","m_123"], ["x-request-id","req_9"], ...],  // sorted, WHITELIST
  "request": { /* descriptor-decoded proto3-JSON of the message */ } }

// result:
{ "http_status": 200,
  "grpc_status": 0,                    // from trailers (or headers-only error response)
  "trailers": [["grpc-status","0"]],
  "body_b64": "AAAAAScK...",           // raw frames — the substitution source of truth
  "response": { /* decoded projection, for humans/diffs */ } }
```

- **Substitution replays the recorded BYTES** through tonic's real decoder — maximal
  fidelity, and full Ok+**Err** replay for free: a recorded decline is just recorded
  trailers (`grpc-status: N`), which tonic surfaces as the same typed `Status`. (HTTP
  egress is Ok-only; gRPC leapfrogs it with no custom error codec at all.)
- **`args_hash` is computed over the decoded JSON**, never raw bytes:
  `canonical_args_hash` sorts object keys, so prost's map-field encode-order
  nondeterminism (std `HashMap` iteration order is per-process) cannot re-key the
  lookup. Hashing raw bytes would be unsound for any message with a `map<...>` field.
- **Decoding — local protos only (D2 DECIDED 2026-07-13: UCS descriptors deferred)**:
  `prost_reflect::DescriptorPool` built from the local-proto FDS (one line in our
  `build.rs`); rpc path → input/output `MessageDescriptor` → `DynamicMessage::decode`
  → serde. No serde derives on generated types needed (v1's `type_attribute` plan is
  obsolete). **UCS entries stay opaque** (`undecoded: true`, bytes only): arg
  divergences for UCS report at hash/tag level, not field level — accepted trade-off;
  re-enable field-level UCS diffs later by adding the `grpc-api-types` dep (its public
  `FILE_DESCRIPTOR_SET` is verified available at rev 71fcc81).
- **Undecoded args_hash MUST NOT hash raw bytes** — verified: `payment.proto` has
  **32 `map<>` fields**; prost encodes map entries in per-process `HashMap` iteration
  order, so the same logical request produces different bytes across record and
  candidate processes → spurious blocking `NovelCall`. Guard: a schema-free
  **canonical wire-hash** — parse the protobuf wire format generically into
  `(field_number, wire_type, payload)` items (no schema needed; recursive parse
  attempts need only be *deterministic*, not correct) and sort repeated occurrences of
  the same field number by encoded bytes before hashing. Both renderer and hook share
  the function ⇒ stable keys. Known cost (flagged): order-only changes in genuinely
  ordered repeated fields no longer re-key (they substitute). Responses are never
  hashed — replayed verbatim, map order irrelevant.
- **Streaming tripwire**: descriptors carry the streaming flag; if a streaming RPC ever
  appears — warn loudly in record, fail loud in replay (reserved shapes not built).

**Reconstruction mechanics (the "codec" — hand-written seam closures, no `ReplayCodec`
impl; that trait is the macro path's plug-point):**

- Record: buffer request frames (unary ⇒ bounded), forward rebuilt request; buffer
  response data frames **verbatim** (5-byte prefixes, flags — never parsed) + the
  trailers frame; hand tonic a response rebuilt over the *same buffer* — tape and
  production observation are byte-identical by construction.
- Result envelope: Ok-shaped `{http_status, headers (verbatim, sorted), body_b64,
  trailers | null, is_err: grpc_status != 0}`; transport-error-shaped
  `{transport_error: "<display chain>", is_err: true}` when `inner.call()` itself
  fails.
- Substitute: build `http::Response` from recorded status/headers; body =
  `BufferedBody: http_body::Body` yielding `Frame::data(bytes)` → `Frame::trailers` →
  end; **tonic's own decoder** consumes it. Consequences, all by construction:
  - typed `Status` fidelity including `grpc-status-details-bin` (recorded declines
    replay as the identical error — no error codec, no sentinel);
  - trailers-only error shape preserved (`trailers: null`, status in headers) so
    `Status::metadata()` partitions identically;
  - compression-proof (recorded bytes + recorded `grpc-encoding` header travel
    together; hyperswitch doesn't enable compression today);
  - one concrete `BufferedBody` type serves record-rebuild AND replay-reconstruct,
    satisfying the `GrpcService` bounds once.
  - transport-error arm replays as a wrapper error type carrying the recorded message
    → tonic maps via `Status::from_error`. Approximate (message, not original struct)
    — flagged; substitutes rather than fail-stops.
- Dispatch value `T = Result<http::Response<BufferedBody>, WrapErr>`; reconstruct =
  envelope → `T`; extract = `T` → envelope JSON. Miss ⇒ NovelCall + fail-stop,
  unchanged.

Secrets posture (D6 — **OVERRULED by user 2026-07-15: full fidelity**): ALL request
metadata is captured, auth included — the capture layer never drops data; tape
protection/redaction is a separate deferred workstream. This matches the HTTP
boundary, which records every request header. Replay stability holds because
candidates rebuild requests from SEEDED state (recorded values reproduce); any
metadata change re-keys the lookup and surfaces as an honest divergence.
Empirical capture set at this layer = app metadata + tonic's per-call constants
(`content-type: application/grpc`, `te: trailers`); no user-agent below the wrap.

### 4.4 Replay boot: UCS must construct lazily

UCS is the only client with an **eager** `connect()` at boot. In a replay pod there is
no UCS server → connect fails → `unified_connector_service_client = None` →
`should_call_unified_connector_service` silently takes the non-UCS code path —
structural divergence before any boundary fires. Fix: under active deja replay,
`Endpoint::connect_lazy()`; substituted calls never touch the channel, so it never
dials. Corollary: the replay pod's config must keep UCS + dynamic-routing sections
present/enabled with dummy URIs (config parity — same trap class as
`replay-env-contract.md`).

### 4.5 UCS diff footprint (exact, from `unified_connector_service.rs` @ deja-lean)

- 10 struct fields (`:28-48`): `XServiceClient<tonic::transport::Channel>` →
  `XServiceClient<UcsTransport>` where `UcsTransport` is a cfg-swapped alias
  (`Channel` / `DejaGrpcTransport<Channel>`). Mechanical type-token change.
- `build_grpc_client!` (`:112-137`): today `<T>::connect(uri)` builds a *private*
  Channel (nothing to wrap). Deja path: explicit `Endpoint` → `connect()` (same
  timeout/None-on-fail semantics) → wrap once → `XServiceClient::new(transport.clone())`.
  Feature-off keeps the existing path byte-identical. (Note: current macro dials 10
  separate connections to one URI; the deja path shares one multiplexed Channel — keep
  per-service under `cfg(not(deja))` to stay behavior-neutral in the vendor diff.)
- `build_connections`: replay-active ⇒ `connect_lazy()` (§4.4).
- **Zero changes**: all 26 wrapper methods, `build_unified_connector_service_grpc_headers`
  (secrets still attached to real requests; the tape whitelist lives in the transport
  module), all router call sites.
- `external_services/Cargo.toml`: optional-under-`deja` deps `grpc-api-types`
  (same git rev — `FILE_DESCRIPTOR_SET`) + `prost-reflect`.

### 4.6 Library side (outer repo): nearly zero

- `EffectKind::Grpc` variant (metadata-only) — see D3. Nothing else: no macro entry, no
  new codec trait, no dispatch changes. The wire (v8) gains one effect-kind string —
  no-legacy-compat policy applies (re-record, no compat readers).
- The transport wrapper lives **vendor-side** (it's tonic/hyper-specific plumbing; deja
  crates stay transport-free — same reasoning that put `HttpResponseCodec` vendor-side).
  Extract a generic `deja-tonic` helper crate later only if a second consumer appears
  (prove-first).

## 5. What replay catches after this lands

- Candidate builds a different outbound gRPC request → decoded-JSON `args_hash` re-keys
  → blocking `NovelCall` at the exact rpc path.
- Candidate stops making a recorded call → `OmittedCall`.
- Routing decisions (SR/elimination/contract scores) substituted from tape → replay
  selects the same connector as the recording.
- Recorded UCS declines/errors replay as the same typed `Status` through tonic's own
  decoder — error paths exercised, not fail-stopped.
- Health check: flows through the same wrapped pool → recorded (correlation-less,
  Environmental noise at worst); replay pods already disable deep health checks. No
  special-casing (generic, no hardcode).

## 6. Out of scope / deferred

- **Streaming RPCs** — none exist; descriptor-flag tripwire + reserved shapes.
- **Tape secret redaction** — cross-channel workstream; this design only guarantees no
  *new* secret class (metadata whitelist).
- **Export to PR #13237** — dev-only on `deja-lean` first. Layout-drift warning: the PR
  base (newer main) may have restructured `grpc_client/` (redis precedent:
  flat `commands.rs` → `module/{fred,redis_rs}` split). Audit at export time.
- **`deja-tonic` extraction** — only on a second consumer.

## 7. Implementation order (each step demo-gated, per consolidated-validation policy)

1. **L1 (outer)**: `EffectKind::Grpc` variant (if D3 = add). Nothing else.
2. **V1 (vendor)**: descriptor plumbing — local-proto FDS in `build.rs`, direct
   `grpc-api-types` git dep @ 71fcc81 (D2), `DescriptorPool` assembly + decode-by-path
   unit tests (incl. a map-field hash-stability test).
3. **V2**: `DejaGrpcTransport` — record path (buffer/tee/observe) on the shared-pool
   `Client` alias; record a routing-enabled flow, verify tape (decoded projections,
   whitelisted metadata, distinct per-rpc addresses).
4. **V3**: replay path (synthesize response from envelope) + wrap `build_grpc_client!`
   (UCS); replay gates: substitution hit, NovelCall on mutated request, Err-replay of a
   recorded decline.
5. **V4**: UCS `connect_lazy` under replay + config-parity note in
   `replay-env-contract.md`.
6. Full matrix gate on the final tree (expensive battery once, at the end).

## 8. Decisions

| # | Question | Status |
|---|---|---|
| D1 | Where the boundary lives | **DECIDED (user-directed 2026-07-13)**: transport-layer tower `Service` wrapper at the 2 construction sites; zero authoring/call-site changes, structural coverage of future methods. (v1 per-RPC attributes and v2 seam-convention REJECTED.) |
| D2 | UCS descriptor access | **DECIDED (user 2026-07-13): deferred** — UCS tape entries are opaque bytes (hash/tag-level diff only, no field-level arg diffs); local protos still decode (we own build.rs). Consequence: UCS args_hash uses the schema-free canonical wire-hash (§4.3 — raw-bytes hashing is unsound, 32 map<> fields in payment.proto). Field-level UCS diffs re-enabled later via the verified-public `FILE_DESCRIPTOR_SET` @ 71fcc81. |
| D3 | Effect metadata | PROPOSED: add `EffectKind::Grpc` variant (one enum variant, metadata-only), not reuse `Http` + kind label. |
| D4 | Scope | DISSOLVED by D1: coverage is structural (all four families incl. health flow through the wrapped transports). No skip-list unless health noise proves annoying. |
| D5 | Error fidelity | DISSOLVED by D1: byte-level replay through tonic's decoder gives full Ok+Err fidelity automatically. |
| D6 | Secrets in args | **OVERRULED (user 2026-07-15)**: full-fidelity capture — ALL metadata recorded incl. auth; protection/redaction deferred as its own workstream (vendor 8c321035d6). Whitelist deleted. |
| D7 | Envelope stores bytes + decoded projection (both) | PROPOSED: bytes = substitution truth, JSON = human/hash projection. Rejected: JSON-only with re-encode (re-encode not guaranteed byte-identical; unknown-field loss). Cost: larger gRPC tape entries. |
