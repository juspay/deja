> **Archived.** This document records async runtime attribution notes from the preload/FFI era. It is kept for historical context and no longer matches the shipped system; the current reference is [DEJA_RECORDING_ARCHITECTURE.md](../DEJA_RECORDING_ARCHITECTURE.md).

# Async Runtime Attribution Notes

## Purpose

This file is for later parallel discussion/worktree exploration.

The immediate implementation target remains Tokio + fred:

```text
Tokio task hooks + Tokio mpsc/unbounded mpsc context propagation
```

This note captures the broader idea so it can be explored separately without slowing down the current spike.

---

## Central question

Can Déjà build a generic accurate-attribution layer for async runtimes?

Déjà needs to answer:

```text
Which business/request scope caused this syscall or outbound I/O?
```

This is stronger than signature matching.

It requires causal context to survive across async boundaries.

---

## Common runtime fundamentals

Most async runtimes appear to have the same causal boundaries:

```text
1. Business scope boundary
2. Task/fiber spawn boundary
3. Task/fiber poll or run boundary
4. Work-item/channel boundary
5. Blocking thread-pool boundary
6. I/O/syscall boundary
```

The exact APIs differ, but the conceptual model is common.

---

## Proposed abstraction names

### ContextCarrier

Stores, captures, restores, and exposes the current business context.

Responsibilities:

```text
capture current context
enter context for current execution
clear context after execution
expose context to LD_PRELOAD/eBPF/syscall hook layer
```

### TaskHookDriver

Runtime-specific task lifecycle integration.

Responsibilities:

```text
on task spawn: capture parent context
before task poll/run: enter task context
after task poll/run: clear thread-visible context
on task terminate: cleanup task context
```

### ChannelAttributor

Message/work-item propagation.

Responsibilities:

```text
send captures context with message
recv adopts message context into receiving task/fiber
```

This is essential for long-lived routers/workers, such as fred Redis router tasks.

### BlockingBridge

Context propagation across blocking thread pools.

Responsibilities:

```text
capture context before spawn_blocking/block_in_place/etc.
enter context inside blocking closure
clear context on closure exit
```

### RuntimeAdapter

Runtime-specific implementation of the above pieces.

Examples:

```text
TokioRuntimeAdapter
AsyncStdRuntimeAdapter
SmolRuntimeAdapter
GlommioRuntimeAdapter
MonoioRuntimeAdapter
```

---

## Runtime-specific thoughts

### Tokio

Best first target.

Reasons:

```text
Tokio has task hooks behind tokio_unstable.
Hyperswitch uses Tokio.
fred uses tokio::sync::mpsc::unbounded_channel for router commands.
Tokio mpsc internals are patchable for a spike.
```

Near-term solution:

```text
TaskHookDriver via Tokio Builder hooks
ChannelAttributor via patched tokio::sync::mpsc
BlockingBridge via spawn_blocking wrapper/patch
```

### async-std / smol

Likely share lower-level `async-task` concepts.

Potential route:

```text
wrap spawn APIs
or patch async-task internals
or instrument executor poll path
```

Needs separate investigation.

### glommio

Different thread-per-core runtime model.

May reduce cross-thread context movement, but still has task/work-item boundaries.

Needs separate investigation.

### monoio / io_uring runtimes

May require ring-level attribution for submitted operations.

Potentially needs a shadow map from io_uring submission entries to context.

More complex and not near-term.

---

## More generic/new ideas to explore later

### Compiler/future poll wrapping

Instrument futures at construction/poll boundaries rather than patching runtimes.

Potential forms:

```text
attribute macro around handlers
middleware wrapping returned futures
proc-macro rewriting spawn/channel calls
```

Pros:

```text
runtime-independent in principle
less dependency patching
```

Cons:

```text
does not automatically cover third-party internal queues
can miss work created outside instrumented futures
```

### Universal channel envelope

Create a trait-level pattern:

```rust
Contextual<T> = { context, value }
```

and push libraries toward carrying it explicitly.

Pros:

```text
clear and safe
no runtime patching
```

Cons:

```text
requires library/application code changes
not low-code for existing stacks
```

### Syscall-side inference plus hints

Combine syscall hooks/eBPF with runtime hints.

```text
runtime emits task/context transitions
syscall layer samples current context
post-processor reconstructs causality
```

This may be useful for production hardening later.

It does not replace runtime/application hints.

---

## Current decision

Do not build multi-runtime support now.

Use this file for later parallel exploration.

Main work now:

```text
Fix fred attribution through Tokio hooks + Tokio unbounded mpsc patch.
```
