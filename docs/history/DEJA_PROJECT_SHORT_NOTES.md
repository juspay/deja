> **Archived.** This document records project notes from the zero-code/preload era. It is kept for historical context and no longer matches the shipped system; the current reference is [DEJA_RECORDING_ARCHITECTURE.md](../DEJA_RECORDING_ARCHITECTURE.md).

# Déjà — Short Project Notes

## 1) What Déjà is

Déjà is a **zero-code, boundary-first capture / replay / analysis system** for Rust services.

Core idea:

```text
Most application nondeterminism comes from external boundaries:
- network I/O
- time
- randomness
- environment
```

If Déjà can observe these boundaries accurately, it can:
- explain what a request touched
- replay and compare behavior
- detect regressions and divergence
- measure production-readiness of an instrumentation approach

---

## 2) What problem we are solving

For backend systems, debugging and regression detection are hard because the same request can depend on:
- PostgreSQL
- Redis
- HTTP calls to external providers
- async scheduling
- timing/randomness

Traditional logs are incomplete.
Traditional replay systems often match by signatures rather than true causality.

Déjà’s goal is stronger:

```text
capture real boundary behavior,
preserve causal attribution,
and detect meaningful divergence
```

---

## 3) Current project direction

Déjà has evolved into two connected tracks:

### A. Zero-code boundary capture
Using `LD_PRELOAD` interception to capture live behavior from real binaries with no source-code changes.

### B. Causal correlation across async Rust
Using runtime-level context propagation so syscall/socket events can be tied back to the request that caused them.

This second part is what makes Déjà more than just packet capture.

---

## 4) Why this is different

Déjà is not trying to be:
- a ptrace/rr clone
- a proxy-only capture system
- just an eBPF network sniffer
- just a signature-based replay system

The intended differentiator is:

```text
causal correctness
```

Examples of what should matter to Déjà:
- request A touched Redis, request B did not
- DB/Redis ordering changed
- side effects happened in the wrong sequence
- replay looks “similar” but is causally wrong

---

## 5) Why we chose this approach

### LD_PRELOAD / hook-based capture
Chosen because it gives:
- zero-code integration
- direct syscall/socket visibility
- works on real production binaries
- no need to recompile or rewrite the target app

### Tokio-based context propagation
Chosen because many Rust services and libraries depend on Tokio.
Patching a **foundational runtime** scales much better than patching every library independently.

This lets downstream libraries inherit request context automatically through:
- task spawn
- task poll
- `spawn_blocking`
- Tokio channels / work-item transfer

---

## 6) Alternatives considered

### eBPF-only
Pros:
- broad deployment potential
- kernel-level visibility

Why not enough alone:
- sees I/O, but not business/request ownership by itself
- cannot solve causal attribution without runtime/application hints

### Proxy-based capture
Pros:
- protocol-aware
- useful for HTTP replay

Why not enough:
- misses non-proxied internal boundaries
- changes network topology
- not truly zero-code

### ptrace / ltrace style tracing
Why rejected:
- too slow
- privileged
- operationally poor for production-style usage

### Patch every client library
Example: patch `fred`, patch DB clients, etc.

Why rejected as long-term strategy:
- does not scale
- high maintenance burden
- too dependent on internal library structure

---

## 7) What we have proven so far

### Real demo against Hyperswitch
We built a full Docker-based experiment around a real Rust payment router with:
- PostgreSQL
- Redis
- external Stripe calls
- concurrent request traffic
- end-to-end scorecard output

### Fidelity proof
Independent pcap verification has shown:
- **100% A9 fidelity** in validated runs
- captured bytes match real network traffic at the service level

### Correlation proof
With the current Tokio context work:
- request grouping works well
- PostgreSQL attribution is strong
- Redis send-side attribution improved significantly
- real concurrent requests can be tracked by request ID

### Operational proof
The full demo pipeline now:
- completes end-to-end
- writes logs and artifacts
- produces correlation, fidelity, and benchmark outputs

---

## 8) Key technical lesson so far

There are really **two different problems**:

### Problem 1: context propagation
How request context survives async boundaries.

Solved well at the runtime level with the Tokio work.

### Problem 2: semantic ownership
What a given low-level event actually belongs to.

This is harder for shared/multiplexed systems like Redis clients.
A raw `recv()` on a shared Redis connection may not cleanly belong to exactly one request.

This means:

```text
runtime propagation is necessary,
but not always sufficient
```

---

## 9) Redis / fred conclusion

What we learned from `fred`:
- request task sends logical command into internal routing machinery
- long-lived reader/writer tasks operate on shared connections
- raw recv-side ownership is ambiguous on multiplexed connections

So the long-term answer is **not** “keep forking Redis libraries”.

Better direction:
- keep runtime-level propagation in Tokio
- add semantic library integration only where needed
- prefer integration at **consumer-owned wrapper layers**

For Hyperswitch, that wrapper is:

```text
vendor/hyperswitch/crates/redis_interface/
```

---

## 10) Current recommendation

### Keep active
- Tokio automatic context propagation
- zero-code LD_PRELOAD capture path
- benchmark/scorecard framework

### Do not make mainline
- library-specific forks as the default strategy
- forcing raw recv attribution where ownership is fundamentally ambiguous

### Build next
- a small semantic operation-attribution layer
- consumer-side wrapper integrations where needed
- continued focus on performance reduction

---

## 11) Current demo status

As of the latest working state:
- Stripe payment confirm succeeds in the demo
- concurrent request burst completes reliably
- Redis traffic is still present
- correlation still passes (~95%)
- fidelity still passes (100%)

That means the system is already good for demonstrating:
- zero-code capture
- causal request grouping
- real boundary observation
- independent fidelity validation

Main remaining concern is **performance overhead**, not demo correctness.

---

## 12) Main open issues

### Performance
The instrumented path is still too expensive in benchmark mode.
This is the biggest productization issue.

### Redis recv-side semantics
We need to decide what to attribute:
- raw recv syscall
- logical Redis response
- ambiguous/multi-owner event model

### Broader replay maturity
Replay/comparison flows exist conceptually, but the strongest current proof is still on capture/fidelity/correlation.

---

## 13) One-line senior summary

```text
Déjà is a zero-code causal observability and replay project for Rust services: it captures real boundary behavior from production binaries, validates fidelity independently, propagates request context across async runtime boundaries, and is now moving from raw capture toward semantically correct attribution and scalable integration surfaces.
```
