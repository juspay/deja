# Ticket: @Architect review — capture correctness vs parser correctness

## Status
Open

## Why this ticket exists
We need an architecture-level answer to this question:

> If Déjà records all bytes crossing supported socket boundaries, is proving data correctness basically just proving the protocol parsers are correct?

Short answer: **no**.

Parser correctness is only one layer. For a zero-code `LD_PRELOAD` recorder/replayer, the bigger question is whether the recorder captured the **right bytes, in the right order, on the right connection, with no gaps, duplicates, or corruption**.

## Proposed framing
The problem should be split into at least these layers:

### 1. Capture coverage
Did we intercept every supported application-visible I/O path that can carry bytes?

If not, we can lose data even with perfect parsers.

### 2. Raw-byte fidelity
For each supported syscall/event, did we persist the exact bytes the app sent/received?

This is where partial reads/writes, vectored I/O, `EAGAIN`, EOF, and concurrency matter.

### 3. Connection / request correlation
Did bytes get attached to the correct logical connection/request?

If bytes from two concurrent sockets are mixed, semantic parsing can still appear plausible while being wrong.

### 4. Parser correctness
Given a correct raw byte stream, do Redis / PostgreSQL / HTTP parsers decode it correctly and without loss?

This is important for watch/diff UX, but it is **not sufficient** to prove recording correctness.

### 5. Replay correctness
During replay, do we:
- verify outbound bytes from the app against the recording,
- return inbound bytes exactly as recorded,
- and detect leftovers / unexpected bytes?

## Stronger statement
A better correctness claim is:

> For every supported connection and direction, Déjà preserves a lossless byte stream equivalent to what the process observed at the syscall boundary; protocol parsers are then a secondary semantic interpretation layer over that verified stream.

## Current repo context
Today the repo already hooks:
- `socket`
- `connect`
- `accept`
- `accept4`
- `send`
- `recv`
- `read`
- `write`
- `writev`
- `sendmsg`
- `recvmsg`
- `close`

Known major caveats still include:
- no TLS plaintext interception yet,
- replay matching still needs stronger logical connection identity,
- replay correctness needs stricter byte-level validation,
- unsupported surfaces are broader than users may assume.

## Current risks / gaps to review

### A. Outbound replay validation is too weak
Replay currently advances through recorded events, but this ticket should require that replay-time outbound bytes be compared against the recorded bytes, not just cursor-advanced.

**Risk:** divergence can be missed.

### B. Connection identity is too weak under concurrency
Matching by peer address alone is not enough when multiple concurrent connections hit the same host:port.

**Risk:** one connection can consume another connection's recorded stream.

### C. `recvmsg` / vectored replay fidelity is incomplete
Vectored receive replay must populate the full iovec layout correctly, not only a simplified buffer path.

**Risk:** partial or malformed application-visible reads.

### D. Coverage is not the same as “all socket bytes everywhere”
Even with current hooks, we still need to be explicit about unsupported or partially supported surfaces:
- TLS plaintext
- UDP
- Unix domain sockets
- ancillary / control messages
- `sendmmsg` / `recvmmsg`
- `sendfile` / `splice`
- shutdown semantics

**Risk:** silent blind spots.

### E. Artifact integrity is not yet strongly proven
Append-only JSONL is useful, but the design should define how we detect truncation/corruption and prove stream completeness.

**Risk:** bad artifact accepted as valid evidence.

## What should count as “correctly and fully recorded”
For supported scope, we should define correctness in terms of **per-connection, per-direction byte-stream equivalence**.

### Suggested invariants
For every supported connection + direction:
- the concatenated recorded bytes equal the bytes actually observed at the syscall boundary,
- no gaps,
- no overlaps,
- no duplicates,
- no cross-connection mixing,
- no unconsumed recorded bytes after replay,
- no unexpected replay-time bytes from the app.

### Suggested metrics / evidence
Per connection and direction:
- `byte_count`
- `chunk_count`
- `stream_offset` per chunk
- `stream_hash` for the full concatenated byte stream
- optional per-chunk hash
- counts of short reads/writes
- counts of `EAGAIN`, EOF, and unexpected events

## Recommended architectural position
The architecture should explicitly separate:

### Transport-fidelity proof
"Did we capture the byte stream correctly?"

### Semantic-parser proof
"Did we decode the byte stream correctly?"

A parser can be correct while capture is incomplete. A capture can be correct while a parser is buggy. These must be measured independently.

## Suggested acceptance criteria for a milestone

### Scope contract
- Supported surfaces are explicitly documented.
- Unsupported surfaces fail loudly or are clearly marked unsupported.

### Recorder correctness
- Recorded chunk length equals syscall-visible byte length.
- Stream offsets are contiguous per connection and direction.
- Stream hashes are stable and verifiable.
- Artifact truncation/corruption is detectable.

### Replay correctness
- Every replay-time outbound write is byte-compared to the recorded stream.
- Every replay-time inbound read is sourced only from the recorded stream.
- End-of-run checks report:
  - leftover recorded bytes/events,
  - unexpected app bytes,
  - unmatched connections,
  - ordering mismatches.

### Parser correctness stays separate
- Parsers are validated with parser-specific tests and round-trip/fixture checks.
- Raw byte/hash validation must still pass even if semantic parsing is disabled.

## Suggested experiments
1. **Synthetic fragmentation matrix**
   - partial `send` / `recv`
   - `writev` / `sendmsg` / `recvmsg`
   - nonblocking + `EAGAIN`
   - EOF boundaries

2. **Concurrent same-peer sockets**
   - multiple connections to same host:port
   - verify no cross-stream mixing

3. **Independent ground truth comparison**
   - compare Déjà-captured raw bytes against a second source in controlled tests
   - e.g. syscall tracing / harness-level echo verification

4. **Negative tests**
   - inject missing, extra, reordered, truncated, or corrupted chunks
   - verify replay detects the issue instead of silently continuing

5. **Parser-independent verification**
   - disable protocol decoding and ensure raw stream invariants still prove correctness

## Questions for @Architect
1. What exact boundary claim do we want to make in v1?
   - plaintext TCP only?
   - only syscall-visible bytes?
   - explicitly excluding TLS plaintext?

2. Do we want `connection_id + stream_offset + stream_hash` as a first-class artifact contract?

3. Should unsupported surfaces be hard errors, warnings, or metadata markers?

4. What is the preferred ground-truth strategy for proving raw-byte fidelity in CI?

5. Should parser correctness be tracked as a separate milestone from transport-fidelity correctness?

## Proposed answer to the original question
A refined answer to the user's hypothesis is:

> It is close, but not complete. The proof has two major parts: (1) transport-fidelity / raw-byte correctness and completeness, and (2) parser correctness. If the goal is to prove there is no partial, missing, or corrupted data, parser correctness alone is not enough.

## Recommended next milestone after architect review
- add stable logical `connection_id`,
- add stream offsets + stream hashes,
- enforce outbound replay byte-compare,
- tighten vectored-I/O replay fidelity,
- define explicit supported/unsupported surface contract,
- keep parser validation as a separate semantic milestone.
