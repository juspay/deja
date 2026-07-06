> **Archived.** This document records the tokio task-hook correlation plan from the preload/FFI era. It is kept for historical context and no longer matches the shipped system; the current reference is [DEJA_RECORDING_ARCHITECTURE.md](../DEJA_RECORDING_ARCHITECTURE.md).

# Tokio Runtime Correlation Plan — hook-only, no patched dependency

## Purpose

Déjà needs accurate attribution of outbound I/O to the business/request scope that caused it.

Target invariant:

```text
If work originates inside business scope req-123,
then syscalls caused by request-owned async work should be tagged req-123,
even if the async task later migrates to another worker thread.
```

The active constraint is now:

```text
Do not depend on a patched/forked Tokio or a patched/forked driver crate.
```

---

## 1. Core mental model

A business scope is the logical unit of causality:

```text
HTTP request
message consumer callback
scheduled job execution
workflow step
```

For Actix/Hyperswitch-style services, `deja-actix::DejaScope` creates the request scope and calls:

```rust
deja_tokio::scope(request_id, handler_future).await
```

That scope sets both:

```text
1. Tokio task-local context for direct task-local reads.
2. deja-context thread-visible context for runtime hooks and LD_PRELOAD bridge reads.
```

---

## 2. What upstream Tokio task hooks solve

Tokio tasks can yield, resume, and migrate across worker threads. A plain OS-thread-local is therefore unsafe.

Tokio's runtime task hooks let Déjà maintain a task-id -> context map without patching Tokio source:

```text
on_task_spawn(task_id):
  capture current deja-context snapshot
  TASK_CONTEXTS[task_id] = snapshot

on_before_task_poll(task_id):
  enter TASK_CONTEXTS[task_id]

on_after_task_poll(task_id):
  restore previous thread-visible context

on_task_terminate(task_id):
  remove TASK_CONTEXTS[task_id]
```

This covers ordinary task boundaries:

```text
request task
-> raw tokio::spawn / Handle::spawn
-> child task
-> socket write/read inside child task
```

No application-wide `tokio::spawn` find/replace is required when the runtime is built with the hook extension.

### Tokio build caveat

In Tokio 1.48 these APIs are gated by:

```text
RUSTFLAGS="--cfg tokio_unstable"
```

This is a build configuration requirement, not a patched dependency.

---

## 3. Runtime integration API

`crates/deja-tokio` exposes, when compiled with `tokio_unstable`:

```rust
use deja_tokio::RuntimeBuilderExt;

fn main() {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all().enable_deja_context_hooks();
    let runtime = builder.build().unwrap();
    runtime.block_on(async_main());
}
```

Hook-only integration requires the application to own runtime construction. If the service uses `#[tokio::main]`, replace it with explicit builder construction so hooks can be installed.

---

## 4. What hooks do not solve

Task hooks solve task inheritance. They do not automatically solve work-item ownership inside shared worker/driver tasks.

Example: fred Redis.

```text
startup:
  tokio::spawn(redis_router_task())  # no request context

request req-123:
  redis.get(key).await
  -> enqueue RouterCommand into internal channel

router task:
  receives RouterCommand later
  writes Redis bytes to socket
```

At syscall time, the active Tokio task is the router task. Hooks can restore the router task's context, but the router task was spawned at startup and has no request context to inherit.

Therefore the missing boundary is:

```text
request task -> command enqueue -> shared driver task
```

This is not a Tokio spawn problem. It is a command/work-item propagation problem.

---

## 5. Command-boundary propagation without forks

Preferred design:

```rust
struct ContextualCommand<C> {
    command: C,
    context: deja_context::ContextSnapshot,
}
```

At command creation/enqueue:

```rust
let context = deja_context::capture_current();
queue.send(ContextualCommand { command, context });
```

At driver processing:

```rust
let item = queue.recv().await;
deja_context::scope_sync(item.context, || {
    write_command_to_socket(item.command)
});
```

For third-party drivers, use this preference order:

1. Public wrapper that captures context before enqueue.
2. Upstream-supported metadata/hook extension point.
3. In-band validation marker for observability and drift detection.
4. Fork/patch only as a temporary spike, not as mainline architecture.

---

## 6. spawn_blocking policy

Tokio task-poll hooks do not model arbitrary blocking closures in the same way as async task polls.

Keep the explicit wrapper:

```rust
deja_tokio::spawn_blocking(move || {
    // closure runs under captured deja-context
})
```

This is not a patched dependency. It is a narrow application/library helper for a boundary that Tokio task hooks do not fully cover.

---

## 7. CI and enforcement

Hook-only CI should verify:

```text
cargo test -p deja-tokio
RUSTFLAGS="--cfg tokio_unstable" cargo test -p deja-tokio runtime_hooks
cargo tree -i tokio   # confirms registry Tokio, no path patch
rg "\[patch.crates-io\]|vendor/tokio-deja" Cargo.toml crates docs
```

Optional linting:

```text
Do not lint against raw tokio::spawn when runtime hooks are mandatory.
Continue documenting spawn_blocking as a wrapper-required boundary.
```

---

## 8. Decision

The mainline architecture is:

```text
upstream Tokio runtime hooks
+ deja-context task map
+ explicit command-envelope propagation for shared drivers
+ no patched Tokio/fred/redis dependency in the active tree
```

This is less magical than patching Tokio mpsc, but it scales better operationally and keeps Déjà from owning long-lived forks of foundational libraries.
