> **Archived.** This document records fred/tokio correlation analysis from the preload/FFI era. It is kept for historical context and no longer matches the shipped system; the current reference is [DEJA_RECORDING_ARCHITECTURE.md](../DEJA_RECORDING_ARCHITECTURE.md).

# Redis correlation status: upstream Tokio hooks, no Tokio fork

## Active intent

The active implementation direction is now:

```text
use upstream Tokio runtime task hooks for task-context propagation
avoid patched/forked library dependencies
```

Removed from the active dependency path:

```text
vendored Tokio fork
Cargo patch override for Tokio
patched Tokio mpsc adoption
vendored fred fork
workspace patching to vendored fred
```

Kept active:

```text
crates/deja-context/       # runtime-independent context store
crates/deja-tokio/         # task-local bridge + upstream Builder hook extension
crates/deja-actix/         # request-scope middleware
```

## What upstream Tokio hooks solve

When the application builds its runtime with Déjà hooks installed:

```rust
use deja_tokio::RuntimeBuilderExt;

let mut builder = tokio::runtime::Builder::new_multi_thread();
builder.enable_all().enable_deja_context_hooks();
let runtime = builder.build()?;
```

Déjà can propagate context across ordinary Tokio task boundaries without a Tokio fork:

```text
request scope
-> raw tokio::spawn / Handle::spawn
-> child task poll
-> LD_PRELOAD hook sees request context
```

In Tokio 1.48 this hook API is gated by `--cfg tokio_unstable`. That is not a patched dependency, but it is a build configuration requirement for hook-based propagation.

## What hooks do not solve

Runtime task hooks do not automatically solve work-item ownership inside a long-lived shared driver task.

fred-style Redis still has this shape:

```text
startup:
  spawn redis router task with no request context

request req-123:
  enqueue Redis command into driver channel

router task:
  later receives command
  writes Redis bytes to socket
```

At the socket write, the active task is the router task. Runtime hooks can restore the router task's own context, but they cannot infer which request owns a queued command unless that context travels with the command.

So the remaining Redis/fred issue is not Tokio spawning. It is command-boundary propagation:

```text
send captures current context
queued command carries context
recv/driver processing adopts context while writing/parsing that command
```

This should be solved with explicit command envelopes, driver wrappers, upstream extension points, or in-band validation markers — not a long-lived fork of Tokio or fred.

## Current recommendation

1. Keep the Tokio fork and Cargo patch override removed.
2. Use upstream Tokio task hooks for raw spawned-task propagation.
3. Keep `deja_tokio::spawn_blocking` as an explicit wrapper because task-poll hooks do not model arbitrary blocking closures.
4. Treat Redis/fred multiplexing as a separate command-envelope problem.
5. Do not reintroduce patched dependencies unless there is no upstream/wrapper extension point and the exact ownership requirement is proven critical.
