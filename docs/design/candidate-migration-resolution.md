# Candidate migration resolution — migrations are a function of the candidate ref

**The rule:** a replay run is a function of two code refs — the recording's
(`sha_R`, carried in the tape envelope) and the candidate's (`sha_C`, the thing
under test). The schema the candidate runs against must be **the candidate's own**.
Nothing in the harness may impose a schema — not the runner image, not a pinned
vendor tree, not a constant. This document is how `sha_C` becomes a live,
verified schema without a single hardcode.

`[BUILT]` = landed in code · `[SEAM]` = interface exists, producer pending ·
`[DECISION]` = needs a call before it can land.

---

## 0. Why this exists

The candidate is a **parameter** (`CandidateSpec`): an image ref *or* a code ref,
different every run. The recording is a **separate parameter** — data (an S3
path / id), decoupled from any image; it may have been produced by an entirely
different `sha_R`, and it does not need an image at all.

The frozen sandbox candidate image copies only the router binary and one config
TOML — **it carries no `migrations/`**. So the candidate's schema cannot come
from the image. It has to be resolved from `sha_C` at run time. The failure this
prevents (A1): the runner image happens to bundle *some* Hyperswitch tree, that
tree's migrations get applied, and the candidate runs against a schema that is
neither the recording's nor its own. Every resulting difference then reads as a
candidate regression — a **wrong verdict**, silently.

---

## 1. The resolution chain (generic — one derivation for every candidate kind)

```
CandidateSpec ─▶ sha_C ─▶ migrations(sha_C) ─▶ expected fingerprint
                                             └▶ (applied to sidecar pg by the runner's diesel)
```

| `CandidateSpec` | how `sha_C` resolves |
|---|---|
| `RepoSha { sha }` | `sha_C = sha` directly |
| `RepoBranch { branch }` | git resolve `branch` → `sha_C` |
| `RepoPr { pr }` | git resolve the PR head → `sha_C` |
| `PrebuiltImage { image }` | read the image's `org.opencontainers.image.revision` label → `sha_C` |
| `LocalPath { .. }` | the local checkout's `git rev-parse HEAD` |

Then, uniformly:

- `migrations(sha_C)` = the `migrations/` tree **at that ref**.
- `expected fingerprint` = the set of migration version directories in that tree
  (diesel's version = the timestamp prefix of each `migrations/<version>_*/`).

No branch of this table reads a schema from the runner, the recording, or a
constant. The candidate ref is the sole input.

---

## 2. What is already built

The **verification half** is landed and does not depend on the fork in §3:

- `SchemaFingerprint` (`deja-orchestrator/src/lib.rs`) — the applied migration
  versions, with order-independent-but-exact `matches` and a `diff` that names
  the drift. `[BUILT]`
- `read_schema_fingerprint` (`lifecycle/mod.rs`) — reads the live set back from
  `__diesel_schema_migrations`, guarded for an unmigrated store. `[BUILT]`
- `InPodOptions.expected_schema` + the P1 gate: after migrating, before seeding,
  the runner refuses (fail-closed) unless the live schema is **exactly** the
  candidate's expected set, naming what is missing/extra. `None` = record-only.
  `[BUILT]`
- `RUNNER_EXPECTED_MIGRATIONS` — the runner reads the expected set from env, so
  it is a per-run **parameter** the executor supplies, never a harness constant.
  `[BUILT]`

So the moment the expected set and the applied migrations both come from `sha_C`,
a stale or foreign schema is a **loud refusal**, not a false verdict. What remains
is producing those two things from `sha_C`.

The **resolution half** is the seam:

- `CandidateSpec` already models every candidate kind (§1). `[BUILT]`
- `sha_C` resolution + `migrations(sha_C)` staging + computing the expected set
  from the staged tree — the executor's job. `[SEAM]` (this is task #27; the
  executor sets `RUNNER_MIGRATE_CMD` to apply the staged tree and
  `RUNNER_EXPECTED_MIGRATIONS` to its version list.)

---

## 3. `[DECISION]` How migrations physically reach the Job pod

The frozen image lacks them; they must arrive at run time. Three ways, one call:

### Option A — git-fetch initContainer
A small `git` initContainer checks out `migrations/` at `sha_C` onto the shared
volume.
- **For:** no new artifact store; always fresh from the source of truth.
- **Against:** needs git **and repo credentials in every replay pod**; adds
  egress to the git host — which **fights the default-deny egress seal** (#32);
  a clone per run.

### Option B — orchestrator pre-stages a CodeBundle to S3, runner pulls it  ⟵ recommended for the MVP
At run creation the control-plane orchestrator ensures a CodeBundle for `sha_C`
exists in S3 (fetch `migrations/` from git **once**, cache by sha), and the
runner pulls it by sha exactly the way it already pulls the recording.
- **For:** reuses the runner's existing S3 pull path; git access is confined to
  the **control plane** (one place, not every pod); cached per sha (fetch once,
  not per run); the pod only ever talks to S3, so it stays inside the egress seal.
- **Against:** needs a CodeBundle producer in the orchestrator (`git archive` →
  S3) and a small S3 layout (`codebundles/<sha_C>/migrations.tar`).

### Option C — CI publishes a migrations bundle per image  ⟵ eventual home
When CI builds the candidate image, it also uploads `migrations/` (and config
fingerprints) to S3 keyed by `sha_C`. The runner pulls by sha.
- **For:** zero runtime git; the bundle is produced in the **same CI run** as the
  image, so provenance is strongest; nothing to fetch lazily.
- **Against:** a change to Hyperswitch's image pipeline (cross-team, longer lead);
  needs a fallback until CI does it.

**Recommendation:** ship **B** now (reuses the S3 pull, confines git to the
control plane, egress-seal compatible), treat **C** as the destination once the
image pipeline can add the upload, and avoid **A** because it directly undercuts
the egress seal and sprays git credentials across every Job.

The choice affects only *delivery*: the resolution (§1) and the verification (§2)
are identical under all three. It does shape the infra PR — B/C add an S3
read-only grant + a codebundle prefix to the replay-job IRSA role; A instead adds
a git secret + an egress exception. That is why it is a call to make before the
Monday infra ask, not after.

---

## 4. Config, not just schema

`migrations(sha_C)` is the first CodeBundle member; the same bundle is the right
home for the other `sha_C`-derived facts the frozen image also drops — the config
TOML deltas, `redis_key_prefix`, `crypto_epoch`, `deja_rev`. They resolve by the
identical chain and travel in the identical bundle. This doc scopes the decision
to migrations because they are the one with a landed fail-closed gate; the rest
follow the chosen delivery path without a new decision.
