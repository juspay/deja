# Behavior = a pure function of a handler

> The conceptual foundation of Déjà, written to be read on its own. It explains
> *what* a record/replay regression gate fundamentally is, in one model, and shows
> how Déjà's concrete machinery — and its hard design tradeoffs — are instances of
> that model. If you remember one thing: **record/replay is the business of
> capturing an effect handler and re-running a program against it.**

---

## The one idea

Every program's observable behavior can be written as a **pure function of its
inputs and a handler**:

```
imp(x)  ≡  pur(x, h)
```

- `imp` is the real, effectful program (it reads a DB, calls `now()`, hits a
  connector, writes rows).
- `x` is the explicit input (an HTTP request).
- `h` is the **handler**: the thing that answers every effectful question the
  program asks the world — *what does `db.get(k)` return? what is `now()`? what's
  the next id? which connector response comes back?*
- `pur` is what's left once you've named `h`: a deterministic function. Give it the
  same `x` and the same `h` and it produces the same result, every time.

Nothing about the effects disappears — they become **explicit data**: the answers
flow *in* through `h`, and the program's mutations flow *out* as part of the result.
This is the standard "reification of effects" / state-passing transformation from
programming-language theory; the handler form is the algebraic-effects view of it.

**A record/replay system is a machine for capturing `h` from one execution and
re-supplying it to another.** Recording observes `h`. Replay evaluates `pur(x, h)`
for a *different* program. Everything else — full-mock vs state-seeding, the blind
spots, the noise, the cost — is a consequence of *how you represent `h` and how
complete it is*.

---

## Part I — the model

### 1. What is in `h`

`h` is the program's entire interface to the world. It has an **input side** (every
value the program reads / the *read-set*) and an **output side** (every value the
program mutates / the *write-set*). The channels:

| Channel | Examples | Role in `h` |
|---|---|---|
| **State** (read-write) | DB rows, cache, files | read-set in, write-set out |
| **Entropy / oracle** | `now()`, RNG, `uuid()` | a stream of draws (input) |
| **Order / schedule** | concurrent interleaving | the chosen schedule (input) |
| **Timing** | latencies, timeouts firing | observed durations (input) |
| **External I/O** | connector / network responses | the environment's answers (input) |
| **Config / environment** | flags, env vars | ambient constants (input) |
| **Implicit context** | thread-locals, ambient auth, locale | hidden parameters (input) |

The special cases are the familiar monads: a reads-only `h` is the **Reader**
(`pur : (X, Env) → Y`); read-write state is the **State** monad
(`pur : (X, S) → (Y, S)`); outputs are the **Writer**. The general case is a
**handler** that interprets each effect operation.

### 2. Exactness condition

`pur(x, h) = imp(x)` **exactly, iff `h` is total and faithful** — it answers *every*
operation the program actually invokes, with the value the real world would have
given. Hold onto this "iff": it is where every interesting limitation comes from.

A handler captured from one run answers the questions *that run asked*. It purifies
the program **at that trace** — not universally. Run a *different* program and it may
ask questions the captured `h` can't answer. That gap is the whole game.

---

## Part II — Déjà as one instantiation

### 3. The cut

You don't reify the *entire* world — you choose a **cut**: a set of boundaries below
which effects are captured into `h`, and above which code is treated as pure. Déjà's
cut is a fixed set of instrumented seams:

```
db (generic queries) · redis (commands) · crypto · id-gen · time · http (outgoing)
```

Everything at those seams is captured into `h`. Everything between them — the
candidate's business logic — is the `pur` part that runs for real. The incoming HTTP
response is not in `h`; it is the *output* being compared.

**The cut defines `h`'s granularity, and it is a design parameter, not a given.** Cut
deeper (e.g. at the syscall level, as an `LD_PRELOAD` tracer would) and more of the
world lands in `h` — more faithful, more to capture. Cut shallower (library calls, as
Déjà does) and there's less to capture but more is assumed-pure.

### 4. Record = capture `h`; replay = evaluate `pur(x', h)`

- **Recording** runs `imp(x)` and observes the cut: for each boundary operation it
  stores the question (args + a stable identity for the call site) and the answer
  (the result), in order. It also captures the **write-set** — the effects the run
  produced — which becomes the *oracle* replay is graded against.
- **Replay** takes a *candidate* program `pur'` (a code change) and evaluates
  `pur'(x, h)` using the recorded `h`. Divergence = where `pur'`'s behavior, run
  against the same `h`, differs from the recorded write-set / response.

The recorded `h` is used twice: as the **handler** that drives replay, and as the
**expected output** to diff against.

---

## Part III — the design space (the part that generalizes)

This is the transferable insight: the big architectural choices are all **"how do
you represent `h`, and how complete can you keep it when the program changes?"**

### 5. Two representations of the same `h`

Full-mock and state-seeding are **not two models** — they are two *implementations of
the same handler*:

| | Full-mock | State-seeding |
|---|---|---|
| How `h` answers a read | from a **memo table** keyed by the call (the recorded result) | from a **materialized world** (a live, seeded DB) the program queries |
| Representation | operational — supplied *at the boundary* | denotational — supplied *as state* |
| Cost | cheap, deterministic, no live backend | seed + real I/O + teardown per case |
| Completability under a changed program | low — only answers calls the original run made | high — a full snapshot answers new queries too |

Other points on the same axis: a **copy-on-read overlay** (reads fall through to a
reference snapshot, writes go to a per-case overlay — a "branch" of `h` per request),
and **live fallback** (`h` fetches from the real world on a miss). All four are just
strategies for *representing and completing the same `h`*.

### 6. The candidate changes the program → `h` under change

The developer perturbs the program: `pur → pur'`. The effect on the output has two
parts — the direct change in `pur'`, and the change because `pur'` *asks `h`
different questions*. How your `h`-representation answers those new questions decides
what you can see:

- A **memo-table `h`** can only answer the original run's questions. When `pur'` asks
  a changed question, it returns the *stale* answer at the old call point. So you
  measure the change **holding `h`'s responses fixed** — the **partial derivative**.
- A **complete materialized `h`** answers the new questions for real, so consequences
  propagate — the **total derivative**.

Worked example. A change makes `db1` query a different key, whose result feeds `db2`:

```
plan = db.get_plan(region, plan_id)      // db1 — args change: region "US" → "EU"
db.insert_invoice({ region,              //   direct field        (caught either way)
                    rate:   plan.rate,   //   ← db1's result      (transitive)
                    amount: total*plan.rate }) // ← db1's result  (transitive)
```

- **Memo-table `h` (full-mock):** `db1`'s result is the recorded `{rate: 0.10}`; the
  EU plan's real `{rate: 0.20}` is never consulted. The invoice replays clean — the
  2× overcharge is **masked** (it rode `db1`'s result, a question `h` answered
  stale). You catch the *direct* `region` change and miss the *transitive* one.
- **Complete materialized `h` (state-seed):** `db1` re-reads the seeded EU row, the
  real rate flows into the invoice, and the overcharge **materializes** and is caught
  — the total derivative.

### 7. One blind spot, two faces

Both representations have a blind spot, and it's *the same* blind spot:
**`h` is incomplete for the perturbed program.**

- Full-mock's "transitive masking" = `pur'` asked a changed question; the memo `h`
  answered it stale.
- State-seeding's "un-seeded read" = `pur'` asked a question whose answer wasn't in
  the materialized `h` (the recording only seeded rows the original run touched).

They are duals. Full-mock answers *everything the original run asked* but can't
propagate change; state-seeding *propagates change* but can't answer outside the
seed. Neither is complete; completeness of `h` is the single underlying limit.

Two tools that fall out of this framing:
- **Taint** is an *estimate of the reach of `h`-incompleteness*: which downstream
  calls/fields a stale answer could have affected. It gives the **scope** of the
  masked region, never the **values** (those need a complete `h`, i.e. execution).
- A **control / self-replay run** (replay the recording against an *unchanged*
  program) detects `h`-incompleteness as noise — anything that diverges there is a
  handler gap, not a real change, and gets subtracted.

---

## Part IV — honest limits

The transform is a theorem, but "*any* `imp`" carries five conditions worth stating:

1. **`h` is generally an oracle, often unbounded.** Purifying for *all* inputs needs
   `h` to answer *every possible* operation (`db.get(k)` for all `k`). You can only
   capture a finite, run-specific `h`. So `pur(x, h) = imp(x)` **at the captured
   trace**, not universally. This is the mathematical root of every blind spot.
2. **Order and timing are higher-order.** The schedule isn't a free parameter — the
   program's own actions influence the interleaving (a fixpoint). You can
   record-and-replay the order that happened; you can't freely *choose* one for a
   changed program.
3. **External writes can't truly be "returned as a value."** Reifying the write-set
   is faithful for *modeling and comparison*, but a real email sent or payment
   captured has escaped. This is exactly why live execution must **sandbox egress**:
   you're coercing the external write-set back into `h`'s output instead of letting
   it happen.
4. **Fundamental nondeterminism is capturable, not predictable.** You can record what
   a hardware RNG or a race produced; you can't compute it for a changed program.
   Capture works; synthesis doesn't.
5. **Completeness is undecidable.** You can't statically prove you captured the full
   read-set (hidden globals, implicit context). You discover gaps when replay
   diverges — which is *why the control run exists*.

---

## Part V — how to use this model

When you face a record/replay design decision, translate it into the handler frame:

- **"Full-mock or state-seed?"** → *Which representation of `h`, and how complete do
  I need it for the programs I'll test?* Use the cheap memo representation for breadth
  (find and scope every divergence over all traffic); use a materialized,
  more-complete representation for depth (true consequences on the cases that matter).
  The hybrid — memo-`h` triage + materialized-`h` on flagged cases — is "two
  representations of one handler," not two systems.
- **"Why did we miss this regression?"** → *`h` answered a changed question stale, or
  lacked the answer entirely.* The fix is always a more complete `h` along some
  channel.
- **"Where do we instrument?"** → *Where do we cut?* Deeper cut = more in `h` = more
  faithful + more to capture. It's a dial.
- **"Why is the gate noisy?"** → *`h` is incomplete along the entropy/order/timing
  channels.* Either capture those channels too (put them in `h`) or subtract them
  with a control run.
- **"Egress / secrets / DB seeding problems"** → these are all *handler-completion
  strategies and their costs*: making `h` answer external I/O (sandbox/stub),
  protecting the captured `h` (it holds secrets), and materializing the state channel
  of `h` (seeding).

---

## Appendix — abstract ↔ Déjà glossary

| Model term | Déjà term |
|---|---|
| `imp(x)` | the instrumented service handling a request |
| `pur` | the candidate build's business logic (between boundaries) |
| `x` | the recorded request (replayed unchanged) |
| `h` (handler) | the recording — boundary answers + the seeded state |
| the cut | the boundary set (db / redis / crypto / id / time / http) |
| read-set | recorded boundary args + results |
| write-set | recorded side effects + response (the oracle) |
| memo-table `h` | full-mock substitution from the lookup table |
| materialized `h` | state-seeded-per-correlation live execution |
| `h` incomplete under change | the masking / un-seeded-read blind spots |
| reach of `h`-incompleteness | taint / blast-radius scope |
| detecting `h`-incompleteness | the control / self-replay run |
