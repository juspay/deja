> **Archived.** This document records the preload-era socket-replay plan, which was never completed (superseded by lookup-table replay). It is kept for historical context and no longer matches the shipped system; the current reference is [DEJA_RECORDING_ARCHITECTURE.md](../DEJA_RECORDING_ARCHITECTURE.md).

# Complete Replay Pipeline for Async Rust Services

## Context

Déjà can now **record** all outbound socket calls (Redis, PostgreSQL, HTTP) from a production Rust service (tested with Hyperswitch — 239MB binary, tokio multi-threaded runtime, actix-web, 12 workers) transparently via `LD_PRELOAD` with zero code changes. 51 structured events were captured from a live instance including Redis RESP protocol exchanges and PostgreSQL wire protocol queries.

**Recording works. Replay does not — yet.**

This issue tracks the three components needed to close the loop and deliver AREX-style zero-code regression testing for Rust services.

## Problem

During recording, our libc hooks (`connect`, `send`, `recv`, `read`, `write`, `close`) observe real I/O and append captured bytes to `events.jsonl`. The application runs normally because real syscalls proceed.

During replay, the hooks need to **replace** real I/O with recorded data. This fails for async services because:

1. **`connect()` returns synthetic success but the fd is dead** — tokio registers fds with `epoll`. A fake-connected fd never becomes "ready" so `epoll_wait` hangs forever and `recv()` is never called.

2. **No automated request driver** — the recording captures outbound dependency calls (Redis, PG) but the inbound HTTP requests that triggered them must be re-sent by something. Currently that "something" is a human running curl.

3. **No response comparison** — even if replay worked, we don't capture the server's HTTP response during replay to compare against the recorded response.

## Solution — Four Components

### Component 1: Socketpair-Based Replay

**The core fix for async/epoll compatibility.**

When `connect()` is called in replay mode, instead of returning 0 on a dead fd:

```
1. Create socketpair(AF_UNIX, SOCK_STREAM) → [app_fd, agent_fd]
2. dup2(app_fd, original_fd) — replace the socket with our pipe end
3. Return 0 (success) — the fd is now a real, connected Unix socket
4. On recv(original_fd): read from the pipe (agent writes recorded data into agent_fd)
5. On send(original_fd): read from pipe, advance replay cursor, discard
```

Because the socketpair is a real kernel object, `epoll_wait` reports readiness naturally when data is written to the agent end. No epoll hooking needed.

**Implementation:**

In `crates/deja-preload/src/agent.rs`:
- Add `replay_pipes: Mutex<HashMap<i32, RawFd>>` to `AgentRuntime` — maps app fd → agent-side fd
- `before_connect()` in replay mode:
  - Call real `socketpair(AF_UNIX, SOCK_STREAM, 0, fds)`
  - Call real `dup2(fds[0], sockfd)` to replace the original fd
  - Store `fds[1]` in `replay_pipes`
  - Load all recorded `Receive` events for this connection into a buffer
  - Spawn a thread that writes the buffered data into `fds[1]` on demand
  - Return `HookAction::Synthetic(ConnectOk)`
- `before_recv()` in replay mode: remove the synthetic data injection (the pipe handles it naturally now)
- `before_send()` in replay mode: let the `write()` go through (it writes to the pipe, agent-side discards it)

**Acceptance criteria:**
- [ ] `demo_gateway` (blocking I/O) replay still works
- [ ] Hyperswitch with tokio starts in replay mode without hanging
- [ ] Redis connections return recorded PONG/OK/data
- [ ] PostgreSQL connections return recorded auth + query results

### Component 2: Request Replay Driver

**`deja replay-traffic --artifact <PATH> --target <URL>`**

Reads the recorded artifact, extracts inbound HTTP requests (identified as traffic on the server's listening socket), and sends them to the target server.

The recording doesn't currently capture inbound requests (accepted connections aren't tracked by the fd tracker since `accept()` isn't hooked). Two approaches:

**Approach A — Hook `accept()` + capture inbound traffic:**
- Add `#[no_mangle] pub unsafe extern "C" fn accept(...)` / `accept4(...)` hooks
- Register accepted fds in the fd tracker as "inbound" connections
- Record inbound `recv` (request) and `send` (response) events
- Tag inbound events with a flag (`direction: inbound`) to distinguish from outbound
- The request driver reads inbound recv events and replays them as HTTP requests

**Approach B — External traffic capture (simpler, less integrated):**
- Use a separate mechanism to capture inbound traffic (tcpdump, mitmproxy, or application-level logging)
- Store captured requests in a sidecar file (`requests.jsonl`)
- The request driver reads this file and sends the requests

**Recommended: Approach A** — it keeps everything in the LD_PRELOAD agent, maintains zero-code property.

**Implementation for Approach A:**

In `crates/deja-preload/src/hooks.rs`:
```rust
#[no_mangle]
pub unsafe extern "C" fn accept(
    sockfd: libc::c_int,
    addr: *mut libc::sockaddr,
    addrlen: *mut libc::socklen_t,
) -> libc::c_int {
    let real = ensure_real_functions();
    let fd = (real.accept)(sockfd, addr, addrlen);
    if fd >= 0 {
        if let Some(ag) = agent() {
            ag.on_accept(fd, sockfd); // Register as inbound connection
        }
    }
    fd
}
```

In `crates/deja-preload/src/fd_tracker.rs`:
- Add `FdKind::InboundTcpSocket` variant
- Track inbound connections separately from outbound

In `crates/deja-core/src/lib.rs`:
- Add `direction: EventDirection` field to `SocketBoundaryEvent` (or use a new `InboundSocketBoundaryEvent`)

In `crates/deja-cli/src/main.rs`:
- Add `deja replay-traffic --artifact <PATH> --target http://localhost:8080` command
- Read events where direction=inbound and operation=receive (these are the original requests)
- Send each as an HTTP request to the target
- Capture responses for comparison

**Acceptance criteria:**
- [ ] `accept()` hook captures inbound connections
- [ ] Inbound recv events contain the raw HTTP request bytes
- [ ] Inbound send events contain the raw HTTP response bytes
- [ ] `deja replay-traffic` sends recorded requests and collects responses

### Component 3: Response Comparison

**Automatic regression detection: compare recorded vs replayed responses.**

During replay:
1. The request driver sends recorded inbound requests to the new server
2. The server processes them, calling mocked outbound dependencies
3. The server sends back a response
4. We capture this response (via inbound send events from the `accept()` hook)
5. We compare it to the original recorded response

**Implementation:**

- Extend `deja replay-traffic` to:
  - Capture the response from each replayed request
  - Store in a second artifact (`replay-artifact/`)
  - Run `deja regress --baseline <recording> --candidate <replay-artifact>`
- The comparison engine (`deja-compare`) already handles JSON diff with noise rules

Or, simpler single-command approach:

```
deja regress-live \
  --recording /path/to/recorded-artifact \
  --target http://localhost:8080 \
  --config noise-rules.json
```

This command:
1. Reads the recording
2. Starts the request driver
3. Captures responses
4. Compares in real-time
5. Outputs regression report

**Acceptance criteria:**
- [ ] Replayed responses are captured
- [ ] `deja regress` correctly identifies identical responses
- [ ] `deja regress` correctly identifies regressions (different status code, different body)
- [ ] Noise rules filter out timestamps, request IDs, etc.

### Component 4: State-Seeded Owned Dependency Replay

**For isolated request replay of stateful DB/Redis.**

Socketpair replay can substitute recorded dependency bytes, but owned mutable dependencies such as Redis and Postgres have a stronger replay option: reconstruct the request's pre-state, seed isolated infrastructure, and let candidate code hit real DB/Redis.

This avoids the central ambiguity of signature/cursor replay:

```text
GET k -> v1
SET k -> v2
GET k -> v2
```

The second `GET k` should not be seeded as initial state. It should be produced by the candidate's own `SET`. Therefore the fixture builder must use request correlation and local operation order to distinguish:

```text
pre-write reads       -> initial fixture facts
post-write reads      -> validation observations
writes/deletes        -> ordered side-effect log
post-request snapshot -> final-state expectations
```

**Implementation:**

- Add `deja inspect --state-fixture <artifact>` to infer fixture plans.
- For Redis, seed strings/hashes and negative facts into isolated Redis.
- For SQL, start with high-confidence full-row primary-key reads and classify underdetermined queries.
- Extend `deja regress-live` with owned dependency configuration:

```
deja regress-live \
  --recording ./recording \
  --target http://localhost:8080 \
  --owned-dep redis=redis://127.0.0.1:6379 \
  --owned-dep postgres=postgres://127.0.0.1/deja_replay
```

**Acceptance criteria:**
- [ ] Fixture plan lists pre-write facts, post-write observations, ordered writes, and confidence warnings per request
- [ ] Redis string/hash fixtures can be seeded and replayed against real Redis
- [ ] Candidate Redis writes are compared against recorded ordered write log
- [ ] Final touched Redis state is compared after replay
- [ ] SQL exact-row fixtures are seeded into isolated Postgres
- [ ] SQL aggregate/join/projection cases emit explicit confidence warnings

See `docs/STATE_SEEDED_REPLAY.md`.

## Execution Order

```
Component 1 (socketpair replay)   ← MUST be first, unblocks everything
    ↓
Component 2 (request driver)      ← needs Component 1 to work
    ↓
Component 3 (response comparison) ← needs Component 2 to produce data
    ↓
Component 4 (state-seeded owned deps) ← upgrades DB/Redis from response mocking to isolated real-state replay
```

## Definition of Done

The following end-to-end flow works with Hyperswitch:

```bash
# 1. Record: run server with real Redis + PG, send test traffic
LD_PRELOAD=libdeja_preload.so ./hyperswitch-router &
curl -X POST http://localhost:8080/user/signin ...
kill -TERM %1

# 2. Replay: run server against mocked externals and/or isolated seeded Redis/PG, send same traffic
LD_PRELOAD=libdeja_preload.so DEJA_MODE=replay ./hyperswitch-router &
deja replay-traffic --artifact ./recording --target http://localhost:8080
# or, for isolated owned-state replay:
deja regress-live --recording ./recording --target http://localhost:8080 \
  --owned-dep redis=redis://127.0.0.1:6379 \
  --owned-dep postgres=postgres://127.0.0.1/deja_replay
kill -TERM %1

# 3. Compare: detect regressions
deja regress --baseline ./recording --candidate ./replay-artifact
# Output: regress.result=pass (or fail with specific diffs)
```

Zero code changes to Hyperswitch. Basic replay requires only LD_PRELOAD env vars; state-seeded DB/Redis replay additionally requires isolated dependency endpoints or connection rewriting.

## Related

- [Observation Journal](demo/JOURNAL.md) — full experiment results from Hyperswitch recording
- Current recording proof: 51 events captured (7 Redis connections, 1 PG connection with auth + queries)
- Known gap: `send()` events on Redis connections still sparse (reentrance guard timing under high concurrency)
