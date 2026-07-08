# Archived Spike: fred Redis recv attribution

## Status

This spike is **archived**.

It is **not part of the active main intent anymore**.

Active state of the repo after cleanup:

```text
Tokio patch: removed from active workspace
fred patch: removed from active workspace
upstream Tokio hooks: active direction
```

Reason:

```text
The fred recv-attribution spike was not successful enough to justify keeping it
in the active codebase.
```

The patch set is preserved separately for future discussion/replay:

```text
docs/patches/fred-recv-attribution-spike-manifest.patch
docs/patches/fred-recv-attribution-spike-fred-deja.patch
```

---

## Why this spike existed

The original intended design was:

```text
patch downstream Tokio only
```

That worked for the important send-side path:

```text
request task
-> tokio mpsc carrying fred RouterCommand
-> long-lived router task
-> Redis socket send
```

However, Redis **receive** events remained largely untagged in real pipeline validation.

So this spike asked a narrower question:

```text
If we explicitly carry Déjà context into fred's response-reader side,
will raw Redis recv attribution become correct?
```

---

## What the fred spike changed

The archived spike added a vendored fred fork:

```text
vendor/fred-deja
```

and temporarily patched workspaces to use it.

The spike changed the following areas:

### 1. Command-level context capture

`RedisCommand` captured `deja-context` at creation time.

Intent:

```text
bind each outstanding Redis command to the request context that created it
```

### 2. Command duplication preserved context

fred duplicates commands during some retry/buffering flows.

Intent:

```text
preserve request context across retries / duplicated command state
```

### 3. Shared in-flight buffer became peekable

The fred shared response buffer was changed from a non-peekable queue to a peekable queue.

Intent:

```text
let the reader inspect the oldest in-flight command before reading a response
```

### 4. Reader loop was scoped with oldest in-flight context

The centralized/clustered fred reader loops wrapped:

```text
next_frame(...)
process_response_frame(...)
```

inside the context of the oldest in-flight command.

Intent:

```text
make raw Redis recv syscalls run under the request id of the response being read
```

---

## Archived patch set

### Manifest wiring patch

```text
docs/patches/fred-recv-attribution-spike-manifest.patch
```

This shows the temporary workspace wiring that pointed both main and vendored Hyperswitch to `vendor/fred-deja`.

### fred crate patch

```text
docs/patches/fred-recv-attribution-spike-fred-deja.patch
```

This is the actual fred 8.0.6 diff against upstream source for the spike.

Notes:

- It is a patch **against upstream fred 8.0.6 source**, not a full vendored crate dump.
- Re-applying it later requires vendoring or unpacking upstream `fred-8.0.6` first.

---

## Validation performed

### Focused compile/test validation

Passed during the spike:

```bash
cd vendor/hyperswitch
cargo check -p redis_interface
cargo build -p router --release

cargo test --manifest-path vendor/fred-deja/Cargo.toml duplicate_preserves_deja_context --lib
cargo test --manifest-path vendor/fred-deja/Cargo.toml shared_buffer_exposes_oldest_command_context --lib
```

These verified only that:

```text
command context was preserved
reader could see oldest in-flight command context
```

They did not prove real recv-side syscall attribution.

### Earlier larger pipeline result

Saved artifact:

```text
/tmp/deja-savepoints/consolidate-20260506-155659/deja-pipeline-live
```

Result before fred spike:

```text
Redis total:           1509
Redis strictly tagged: 925

send:    925 / 949 tagged
receive:   0 / 553 tagged
connect:   0 / 7 tagged
```

Interpretation:

```text
Tokio-only patch strongly improved send-side attribution.
Recv-side remained unresolved.
```

### Reduced manual Docker run after fred spike

Saved artifact:

```text
/tmp/deja-savepoints/manual-fred-recv-20260506-163519/deja-manual-fred-recv
```

Run shape:

```text
4 concurrent merchant creates
4 concurrent org creates
all returned HTTP 200
```

Result:

```text
Redis total:           110
Redis strictly tagged: 25

send:    24 / 48 tagged
receive:  1 / 55 tagged
connect:  0 / 7 tagged
```

Per-request Redis totals showed send-side tags for merchant requests, but almost no recv-side tags.

---

## Strongest failure signal: marker responses

The clearest test was the explicit Redis validation marker payloads.

Marker **sends**:

```text
8 / 8 tagged correctly
```

Marker **receives**:

```text
0 / 8 tagged correctly
```

Example send payload:

```text
*2\r\n$4\r\nECHO\r\n$87\r\ndeja_expected_request_id=deja-manual-8-...;deja_backend=redis;deja_operation=del
```

Example matching receive payload:

```text
$87\r\ndeja_expected_request_id=deja-manual-8-...;deja_backend=redis;deja_operation=del
request_id = null
```

This is the main reason the spike is considered unsuccessful.

---

## Additional debug evidence

A later `RUST_LOG=debug` run showed that higher-level application Redis logs still had the correct request id during the same merchant-create flows.

Saved artifact:

```text
/tmp/deja-savepoints/manual-fred-debuglog-20260506-165733/deja-manual-fred-debuglog
```

This suggests:

```text
request identity still exists at higher application layers
but raw recv-side syscall capture still does not reliably expose it
```

---

## Why this spike is considered unsuccessful

Because the question was not:

```text
can we stuff more context into fred?
```

The real question was:

```text
can we make Redis recv attribution reliable enough to justify shipping the patch?
```

Current answer:

```text
no, not with confidence
```

The spike improved or partially influenced some recv observations, but it did **not** solve the critical validation case:

```text
matching Redis marker responses still recorded with request_id = null
```

So keeping the fred patch active would create complexity without giving a trustworthy causal result.

---

## What needs discussion: what does it mean to attribute a recv?

This spike exposed an important modeling question.

For Redis on a shared connection, a single raw socket `recv` may contain:

- bytes for exactly one logical response
- bytes for multiple queued responses
- bytes that are only partially consumed now and decoded later in user space
- responses whose ownership is only known after higher-level protocol parsing

That means a syscall-level event like:

```text
socket receive(fd=..., connection_id=...)
```

may not have a uniquely correct single `request_id`.

### Possible attribution models

#### Model A: single request id on raw recv syscall

```text
simple
but may be incorrect on shared/pipelined connections
```

#### Model B: logical Redis response attribution

```text
attribute decoded RESP frames / logical command completions
instead of raw recv syscalls
```

This is much closer to business causality.

#### Model C: multi-request candidate attribution on recv

```text
one recv event may carry candidate_request_ids = [A, B, C]
```

This preserves ambiguity honestly, but complicates downstream tooling.

---

## Recommendation after this spike

1. Keep upstream Tokio hooks as the active task-propagation direction.
2. Keep the fred spike **archived only**.
3. Do not ship a fred patch as active code until attribution semantics and an upstream/wrapper path are decided.
4. Discuss whether Déjà wants:

```text
raw syscall attribution
logical response attribution
or explicit ambiguous/multi-owner attribution for shared connections
```

5. Prefer a solution that preserves causal honesty over forcing a possibly-wrong single request id onto recv syscalls.

---

## Related artifacts and safety notes

Original consolidation backup:

```text
/tmp/deja-savepoints/consolidate-20260506-155659
```

Later validation artifacts:

```text
/tmp/deja-savepoints/manual-fred-recv-20260506-163519/deja-manual-fred-recv
/tmp/deja-savepoints/manual-fred-debug-20260506-165612/deja-manual-fred-debug
/tmp/deja-savepoints/manual-fred-debuglog-20260506-165733/deja-manual-fred-debuglog
```

Useful context tags:

```text
before-removing-active-fred-patch
fred-reader-context-spike-and-manual-validation
fred-recv-debug-evidence-captured
```
