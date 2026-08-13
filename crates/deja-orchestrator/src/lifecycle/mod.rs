//! Run lifecycle worker (Phase B of the capstone demo).
//!
//! `api::runs::create` persists a Pending run and spawns [`drive`] on a
//! background thread. The worker advances the run's status and orchestrates the
//! demo by shelling out to `docker compose` (which builds the candidate image),
//! pulling the recording back out of MinIO (the full Kafka→S3→replay loop), and
//! calling the in-process lookup renderer + divergence detector.
//!
//! It reuses Hyperswitch's OWN compose (`vendor/.../docker-compose.yml`) plus a
//! thin overlay (`docker-compose.deja.yml`) that swaps the router to a local
//! deja build and adds MinIO + a replay service; HS's kafka0 and vector are
//! reused as-is. Profiled services (kafka0, vector) are started BY NAME so the
//! heavy olap stack (opensearch/clickhouse) is not pulled in. The worker does
//! NOT tear the stack down; the one-click script owns teardown so MinIO persists
//! between the record run and the replay run.
//!
//! Runtime config (env, with demo defaults):
//!   DEMO_COMPOSE_BASE    HS compose (default vendor/hyperswitch-deja-clean/docker-compose.yml)
//!   DEMO_COMPOSE_OVERLAY deja overlay (default demo/overlays/hyperswitch/docker-compose.deja.yml)
//!   DEMO_PROJECT         docker compose project name (default deja-demo)
//!   DEMO_REPLAY_PORT     host port for the replay candidate (default 8090; the
//!                        only host-published port — the host kernel hits it)
//!   DEMO_KERNEL_BIN      deja-kernel binary (default target/release/deja-kernel)
//!   DEMO_KAFKA_TOPIC     recording topic (default hyperswitch-deja-recording-events)
//!   STRIPE_API_KEY       forwarded to the record workload (steps 7 & 9)

use std::collections::BTreeMap;
use std::io::BufRead;
use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use crate::{
    read_json, write_json, CandidateSpec, HarnessRoot, Run, RunMode, RunStatus, SchemaFingerprint,
};

pub mod store_ctx;
pub use store_ctx::StoreCtx;
pub(crate) mod store_exec;
use store_exec::StoreExec;

/// Resolved runtime configuration for the demo orchestration.
#[derive(Clone)]
struct Demo {
    compose_base: String,
    compose_overlay: String,
    project: String,
    replay_port: u16,
    kernel_bin: String,
    topic: String,
    harness_state: String,
    /// Image tag for the candidate services; defaults to the overlay's local
    /// build, overridden when a `local_binary` candidate is baked per-run.
    candidate_image: Option<String>,
    /// Whether the `ucs` compose profile is active (DEMO_UCS=1 → lib.sh exports
    /// `COMPOSE_PROFILES=ucs`, which this process inherits). Named-service
    /// `compose up` does NOT pull in a profiled service unless it is named
    /// explicitly, so the RECORD path must add `ucs` to its service list; the
    /// env alone is not enough. The REPLAY path never lists it — replay
    /// substitutes the gRPC egress from the tape, no live UCS server.
    ucs_profile: bool,
}

impl Demo {
    fn from_env(root: &HarnessRoot) -> Self {
        let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_owned());
        Self {
            compose_base: env(
                "DEMO_COMPOSE_BASE",
                "vendor/hyperswitch-deja-clean/docker-compose.yml",
            ),
            // The out-of-tree overlay (W4) is canonical: it carries the TYPED
            // ROUTER__DEJA__* env the current router reads. The vendor tree
            // still holds a stale pre-typed copy — with that one, a candidate
            // silently boots with deja disabled.
            compose_overlay: env(
                "DEMO_COMPOSE_OVERLAY",
                "demo/overlays/hyperswitch/docker-compose.deja.yml",
            ),
            project: env("DEMO_PROJECT", "deja-demo"),
            replay_port: env("DEMO_REPLAY_PORT", "8090").parse().unwrap_or(8090),
            kernel_bin: env("DEMO_KERNEL_BIN", "target/release/deja-kernel"),
            topic: env("DEMO_KAFKA_TOPIC", "hyperswitch-deja-recording-events"),
            harness_state: root.root.display().to_string(),
            candidate_image: None,
            ucs_profile: std::env::var("COMPOSE_PROFILES")
                .map(|p| p.split(',').any(|s| s.trim() == "ucs"))
                .unwrap_or(false),
        }
    }

    /// `docker compose -p <project> -f <base> -f <overlay>` prefix.
    fn compose_base_args(&self) -> Vec<String> {
        vec![
            "compose".into(),
            "-p".into(),
            self.project.clone(),
            "-f".into(),
            self.compose_base.clone(),
            "-f".into(),
            self.compose_overlay.clone(),
        ]
    }

    /// Common env every compose invocation needs for `${VAR}` interpolation.
    fn compose_env(&self, recording_id: &str, run_id: &str) -> Vec<(String, String)> {
        vec![
            ("RUN_ID".into(), run_id.to_owned()),
            ("RECORDING_ID".into(), recording_id.to_owned()),
            ("HARNESS_STATE".into(), self.harness_state.clone()),
            ("DEJA_RECORDING_TOPIC".into(), self.topic.clone()),
            ("REPLAY_HOST_PORT".into(), self.replay_port.to_string()),
            (
                "STRIPE_API_KEY".into(),
                std::env::var("STRIPE_API_KEY").unwrap_or_default(),
            ),
            (
                "CANDIDATE_IMAGE".into(),
                self.candidate_image
                    .clone()
                    .unwrap_or_else(|| "deja-router-local:latest".to_owned()),
            ),
            // Code identity for the envelope's `code.sha` (resolved by the
            // demo script from the vendor git head; empty when unknown). The
            // router reads it through the standard Vergen identity env, per
            // the typed `deja.identity.git_sha_env` setting.
            (
                "VERGEN_GIT_SHA".into(),
                std::env::var("VERGEN_GIT_SHA").unwrap_or_default(),
            ),
        ]
    }

    /// Derive a PER-RUN-ISOLATED clone of this config for a REPLAY run, so many
    /// candidates can replay the ONE shared recording concurrently without
    /// colliding on the docker project (→ its pg/redis/superposition/replay
    /// stack) or the host replay port.
    ///
    /// - project  → `deja-run-<last 8 alnum of run_id>`: a distinct compose
    ///   project. The LOW-order (fast-changing) end of the id is used — run ids
    ///   are `run-<nanos_hex>`, whose HIGH digits barely move between runs
    ///   submitted seconds apart, so taking the TAIL avoids project-name
    ///   collisions for near-simultaneous parallel submissions. A distinct
    ///   project means `up` brings up a distinct stack:
    ///   an OWN pg + redis-standalone + migration_runner + superposition(+init)
    ///   plus hyperswitch-replay — a fresh, migrated DB + empty redis per run. The
    ///   shared deja-demo project (record-side: kafka0, vector, minio, the
    ///   recording) is untouched.
    /// - replay_port → a free host TCP port (bind :0 to claim one): the only
    ///   host-published port the replay stack exposes, hit by the host kernel.
    ///
    /// Record runs do NOT call this — they keep the shared project + MinIO so the
    /// recording lands in the one shared bucket the orchestrator pulls from.
    ///
    /// Opt out (force the legacy shared project/port, e.g. for a strictly
    /// sequential single-run debug) with `DEMO_REPLAY_SHARED=1`.
    fn isolated_for_replay(&self, run_id: &str) -> Self {
        if std::env::var("DEMO_REPLAY_SHARED").is_ok() {
            return self.clone();
        }
        // Take the TAIL of the alphanumeric id (the low-order, fast-changing
        // nanos hex), not the head — see the doc comment. Reverse, take 8, unreverse.
        let alnum: Vec<char> = run_id
            .chars()
            .filter(|c| c.is_ascii_alphanumeric())
            .collect();
        let short: String = if alnum.is_empty() {
            "run".to_owned()
        } else {
            let start = alnum.len().saturating_sub(8);
            alnum[start..].iter().collect()
        };
        let mut out = self.clone();
        out.project = format!("deja-run-{short}");
        out.replay_port = alloc_free_port().unwrap_or(self.replay_port);
        eprintln!(
            "lifecycle: replay run {run_id} isolated → project={} replay_port={}",
            out.project, out.replay_port
        );
        out
    }
}

/// Claim a free host TCP port by binding `:0` and reading back the OS-assigned
/// port, then releasing it. There is an inherent (small) TOCTOU window between
/// release and the container's `-p <port>:8080` bind; per-run ports drawn this
/// way are spread across the ephemeral range so concurrent replays rarely
/// collide, and a bind failure surfaces as a normal compose-up error.
fn alloc_free_port() -> Option<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").ok()?;
    let port = listener.local_addr().ok()?.port();
    drop(listener);
    Some(port)
}

/// Entry point spawned by the run-creation handler on a background thread.
pub fn drive(root: &HarnessRoot, run_id: &str, ctx: &StoreCtx) {
    let mut run = match read_json::<Run>(&root.run_path(run_id)) {
        Ok(run) => run,
        Err(e) => {
            eprintln!("lifecycle: cannot read run {run_id}: {e}");
            return;
        }
    };
    let mut demo = Demo::from_env(root);
    if let Err(e) = resolve_candidate(&mut demo, root, &mut run, ctx) {
        eprintln!("lifecycle: run {run_id} failed: {e}");
        ctx.finish(false, Some(&e));
        set_status(root, &mut run, RunStatus::Failed, Some(e));
        return;
    }
    let outcome = match run.spec.mode {
        RunMode::Record => drive_record(root, &demo, &mut run, ctx),
        RunMode::Replay => {
            // Per-run isolation: a replay run gets its OWN docker compose project
            // (→ own pg/redis/superposition/replay stack) and its OWN host replay
            // port, so N candidates can replay the shared recording in parallel.
            let demo = demo.isolated_for_replay(run_id);
            let result = drive_replay(root, &demo, &mut run, ctx);
            // Tear the per-run stack down so parallel runs never leak ~5-container
            // stacks. Only for an ISOLATED project (never the shared deja-demo,
            // which holds the record-side recording other runs still pull).
            teardown_if_isolated(&demo, run_id);
            result
        }
    };
    match outcome {
        Ok(()) => {
            ctx.finish(true, None);
            set_status(root, &mut run, RunStatus::Completed, None);
        }
        Err(e) => {
            eprintln!("lifecycle: run {run_id} failed: {e}");
            ctx.log("failure", &e);
            ctx.finish(false, Some(&e));
            set_status(root, &mut run, RunStatus::Failed, Some(e));
        }
    }
}

// ---------------------------------------------------------------------------
// Candidate resolution
// ---------------------------------------------------------------------------

/// Resolve the run's `CandidateSpec` into the image tag compose will use.
///
/// - `PrebuiltImage`: a deployed image ref (e.g. the Jenkins ECR build) —
///   pre-pull it (fail fast on auth/typo, streamed into the run log), then
///   point compose at it via `${CANDIDATE_IMAGE}`; no local build. The host's
///   docker must be logged into the registry (bring-up runbook).
/// - `LocalPath` ("paste a router binary path" — the Phase 1 web-matrix form):
///   validate the binary, sha256 it (the UI's compile-neutral signal), stage a
///   minimal docker context, bake `deja-candidate:<run8>`, and point compose at
///   it (the overlay's `image: ${CANDIDATE_IMAGE:-…}`). Build-from-ref
///   variants land with M3.
fn resolve_candidate(
    demo: &mut Demo,
    root: &HarnessRoot,
    run: &mut Run,
    ctx: &StoreCtx,
) -> Result<(), String> {
    let binary = match &run.spec.candidate_spec {
        CandidateSpec::PrebuiltImage { image } => {
            let declared = image.trim().to_owned();
            // "deja-demo" is the SPA's historical no-candidate default: the
            // legacy compose self-build (overlay default image, `--build`).
            // An empty ref means the same.
            if declared.is_empty() || declared == "deja-demo" {
                return Ok(());
            }
            // The SAME resolution the k8s executor uses (a bare ref qualifies
            // against DEJA_CANDIDATE_IMAGE_REPO). One spec must not read as two
            // different images depending on which executor picked it up.
            let (image, _tag) = crate::executor::resolve_candidate_image(&run.spec.candidate_spec)
                .map_err(|e| e.to_string())?;
            ctx.stage("pulling candidate image", 0, 0);
            let mut cmd = Command::new("docker");
            cmd.args(["pull", &image]);
            let status = run_streamed(cmd, ctx, "pulling candidate image", "docker pull")?;
            if !status.success() {
                return Err(format!(
                    "docker pull {image} failed (status {status}) — is the host logged into the registry?"
                ));
            }
            run.candidate_image = Some(crate::CandidateImage {
                docker_image: image.clone(),
                // What the run DECLARED, kept apart from what it resolved to —
                // the declared ref is what the run id and the spec carry.
                source_ref: declared,
            });
            write_json(&root.run_path(&run.run_id), run)
                .map_err(|e| format!("persist run: {e}"))?;
            demo.candidate_image = Some(image);
            return Ok(());
        }
        CandidateSpec::LocalPath { binary_or_source } => binary_or_source.clone(),
        _ => return Ok(()), // build-from-ref variants land with M3
    };
    ctx.stage("resolving candidate binary", 0, 0);

    let bytes = std::fs::read(&binary)
        .map_err(|e| format!("candidate binary {}: {e}", binary.display()))?;
    if bytes.len() < 20 || &bytes[0..4] != b"\x7fELF" {
        return Err(format!(
            "candidate {} is not an ELF executable",
            binary.display()
        ));
    }
    // e_machine (offset 18, LE): 62 = x86-64 — the demo stack is linux/amd64.
    let e_machine = u16::from_le_bytes([bytes[18], bytes[19]]);
    if e_machine != 62 {
        return Err(format!(
            "candidate {} is not x86_64 (e_machine={e_machine})",
            binary.display()
        ));
    }
    let sha256 = {
        use sha2::{Digest, Sha256};
        let mut h = Sha256::new();
        h.update(&bytes);
        hex::encode(h.finalize())
    };
    ctx.candidate_sha(&sha256);
    let msg = format!(
        "candidate binary {} ({} bytes, sha256 {})",
        binary.display(),
        bytes.len(),
        &sha256[..12]
    );
    eprintln!("lifecycle: {msg}");
    ctx.log("resolving candidate binary", &msg);

    // Stage a minimal, self-contained build context (no repo-root context, no
    // .dockerignore coupling): the candidate Dockerfile pattern of
    // demo/Dockerfile.hyperswitch-semantic with the binary COPY'd in place.
    let stage_dir = root.candidate_stage_dir(&run.run_id);
    std::fs::create_dir_all(&stage_dir).map_err(|e| format!("stage dir: {e}"))?;
    std::fs::write(stage_dir.join("router"), &bytes).map_err(|e| format!("stage binary: {e}"))?;
    for (src, name) in [
        ("demo/workload.sh", "workload.sh"),
        ("demo/superposition_seed.toml", "superposition_seed.toml"),
    ] {
        std::fs::copy(src, stage_dir.join(name))
            .map_err(|e| format!("stage {name} (run from the repo root): {e}"))?;
    }
    std::fs::write(stage_dir.join("Dockerfile"), CANDIDATE_DOCKERFILE)
        .map_err(|e| format!("stage Dockerfile: {e}"))?;

    let short = run.run_id.rsplit('-').next().unwrap_or("cand");
    let tag = format!("deja-candidate:{short}");
    let mut cmd = Command::new("docker");
    cmd.args(["build", "-t", &tag, "."]).current_dir(&stage_dir);
    let status = run_streamed(cmd, ctx, "resolving candidate binary", "docker build")?;
    if !status.success() {
        return Err(format!("candidate image build failed (status {status})"));
    }
    run.candidate_image = Some(crate::CandidateImage {
        docker_image: tag.clone(),
        source_ref: binary.display().to_string(),
    });
    write_json(&root.run_path(&run.run_id), run).map_err(|e| format!("persist run: {e}"))?;
    demo.candidate_image = Some(tag);
    Ok(())
}

const CANDIDATE_DOCKERFILE: &str = r#"FROM --platform=linux/amd64 debian:trixie-slim
RUN apt-get update     && apt-get install -y --no-install-recommends        libpq5 libssl3 zlib1g ca-certificates curl jq bc procps openssl     && rm -rf /var/lib/apt/lists/*
COPY router /local/bin/router
RUN chmod +x /local/bin/router
COPY workload.sh /workload.sh
RUN chmod +x /workload.sh
COPY superposition_seed.toml /local/config/superposition_seed.toml
WORKDIR /local
ENTRYPOINT ["/local/bin/router"]
CMD ["-f", "/local/config/docker_compose.toml"]
"#;

fn set_status(root: &HarnessRoot, run: &mut Run, status: RunStatus, failure: Option<String>) {
    run.status = status;
    run.failure_reason = failure;
    if let Err(e) = write_json(&root.run_path(&run.run_id), run) {
        eprintln!(
            "lifecycle: failed to persist status for {}: {e}",
            run.run_id
        );
    }
}

/// Update the human-facing progress (step `step`/`total`, labelled `label`) and
/// persist it so `GET /runs/{id}` clients can render a live progress bar.
fn set_stage(
    root: &HarnessRoot,
    run: &mut Run,
    ctx: &StoreCtx,
    step: u32,
    total: u32,
    label: &str,
) {
    run.step = step;
    run.steps_total = total;
    run.stage = Some(label.to_owned());
    run.stage_updated_ms = crate::now_ms();
    eprintln!("lifecycle: [{step}/{total}] {label}");
    ctx.stage(label, step, total);
    if let Err(e) = write_json(&root.run_path(&run.run_id), run) {
        eprintln!("lifecycle: failed to persist stage for {}: {e}", run.run_id);
    }
}

/// Run a child process streaming its stdout+stderr line-by-line to BOTH the
/// console (live script UX preserved) and the run's persisted log chunks
/// (batched 25 lines per row to keep insert volume sane on docker builds).
fn run_streamed(
    mut cmd: Command,
    ctx: &StoreCtx,
    stage: &str,
    label: &str,
) -> Result<std::process::ExitStatus, String> {
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd.spawn().map_err(|e| format!("spawn {label}: {e}"))?;

    let mut readers = Vec::new();
    for pipe in [
        child
            .stdout
            .take()
            .map(|p| Box::new(p) as Box<dyn std::io::Read + Send>),
        child
            .stderr
            .take()
            .map(|p| Box::new(p) as Box<dyn std::io::Read + Send>),
    ]
    .into_iter()
    .flatten()
    {
        let ctx = ctx.clone();
        let stage = stage.to_owned();
        readers.push(thread::spawn(move || {
            let reader = std::io::BufReader::new(pipe);
            let mut batch: Vec<String> = Vec::with_capacity(25);
            for line in reader.lines().map_while(Result::ok) {
                eprintln!("{line}");
                batch.push(line);
                if batch.len() >= 25 {
                    ctx.log(&stage, &batch.join("\n"));
                    batch.clear();
                }
            }
            if !batch.is_empty() {
                ctx.log(&stage, &batch.join("\n"));
            }
        }));
    }
    let status = child.wait().map_err(|e| format!("wait {label}: {e}"))?;
    for r in readers {
        let _ = r.join();
    }
    Ok(status)
}

// ---------------------------------------------------------------------------
// Record: bring up the stack, drive the workload, pull the recording from MinIO
// ---------------------------------------------------------------------------

fn drive_record(
    root: &HarnessRoot,
    demo: &Demo,
    run: &mut Run,
    ctx: &StoreCtx,
) -> Result<(), String> {
    let recording_id = run
        .spec
        .recording_id
        .clone()
        .or_else(|| run.recording_id.clone())
        .unwrap_or_else(|| run.run_id.clone());
    run.recording_id = Some(recording_id.clone());
    ctx.run_recording(&recording_id);

    let total = 6;
    set_status(root, run, RunStatus::Building, None);
    ctx.run_state("building");
    // Kafka FIRST and wait until it actually accepts connections: HS's event
    // handler (events.source=kafka) connects at boot and aborts the router if the
    // broker isn't ready. (A compose depends_on can't be used — kafka0 is in the
    // olap profile, which a non-profiled service may not depend on.)
    set_stage(
        root,
        run,
        ctx,
        1,
        total,
        "building images + starting kafka/minio",
    );
    // With DEMO_UCS the Unified Connector Service comes up in THIS infra `up`
    // (before the router), so the record router's eager UCS connect at boot
    // finds a listening host and the outbound gRPC egress is exercised + taped.
    // Named-service `up` won't start `ucs` from COMPOSE_PROFILES alone — it has
    // to be listed. Replay never lists it (egress is substituted from the tape).
    let mut infra: Vec<&str> = vec!["kafka0", "minio", "minio-setup"];
    if demo.ucs_profile {
        infra.push("ucs");
    }
    compose_up(
        demo,
        ctx,
        "building images + starting kafka/minio",
        &recording_id,
        &run.run_id,
        &infra,
        run.candidate_image.is_none(),
        &[],
    )?;

    set_stage(
        root,
        run,
        ctx,
        2,
        total,
        "waiting for kafka broker to be ready",
    );
    wait_kafka_ready(demo, &recording_id, Duration::from_secs(150))?;

    set_stage(
        root,
        run,
        ctx,
        3,
        total,
        "starting record router (DEJA_MODE=record)",
    );
    compose_up(
        demo,
        ctx,
        "starting record router (DEJA_MODE=record)",
        &recording_id,
        &run.run_id,
        &["vector", "hyperswitch-server"],
        run.candidate_image.is_none(),
        &[],
    )?;
    set_status(root, run, RunStatus::Running, None);
    ctx.run_state("running");
    // record candidate isn't published to the host; check health from inside.
    wait_health_exec(
        demo,
        &recording_id,
        "hyperswitch-server",
        Duration::from_secs(240),
    )?;

    set_stage(
        root,
        run,
        ctx,
        4,
        total,
        "driving payment workload (recording to the configured sink)",
    );
    // EU-settlement demo: the settlement READ is now a RAW fred GET against
    // redis, so seed the default rate in the record container's redis (not pg)
    // BEFORE the workload — V1 then records reading 0.10 and writing it (the
    // recorded twin). Best-effort.
    seed_redis(
        &StoreExec::compose(
            demo.compose_base_args(),
            demo.compose_env(&recording_id, &run.run_id),
        ),
        "settlement_rate_default",
        "0.10",
    );
    run_workload(demo, ctx, &recording_id, run_iterations(run))?;

    // Graceful stop of the record router BEFORE the landing wait: SIGTERM →
    // hook drop → writer shutdown flush → producer drain → `eof` sink marker.
    // Without this the eof only fires at compose-down, after the seal.
    set_stage(
        root,
        run,
        ctx,
        5,
        total,
        "stopping record router (flush + eof)",
    );
    stop_service(demo, &recording_id, "hyperswitch-server");

    set_stage(
        root,
        run,
        ctx,
        5,
        total,
        "waiting for the recording to land",
    );
    // The full 9-step Stripe workload keeps producing events while this stage is
    // already counting down, then the router→Kafka→Vector→S3 drain adds a tail
    // (Vector batches every 5s). Observed first-object latency is ~60s, so give
    // a comfortable budget; the stable-count check returns early once the flush
    // settles, so a healthy run does NOT wait the whole window.
    wait_s3_objects(&recording_id, Duration::from_secs(180))?;

    set_stage(
        root,
        run,
        ctx,
        6,
        total,
        "compacting + pulling session from S3",
    );
    pull_recording(root, ctx, &recording_id)?;

    // Register what this run produced. Execution-graph nodes ride the tape
    // itself as `DejaRecord::GraphNode` lines, so the events artifact is the
    // whole recording.
    ctx.artifact(
        Some(&recording_id),
        "events",
        &crate::scope::TapeSlot::for_write(root, &recording_id),
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Replay: pull recording from MinIO, render lookup table, drive kernel, score
// ---------------------------------------------------------------------------

fn drive_replay(
    root: &HarnessRoot,
    demo: &Demo,
    run: &mut Run,
    ctx: &StoreCtx,
) -> Result<(), String> {
    let total = 6;
    set_status(root, run, RunStatus::Resolving, None);
    ctx.run_state("resolving");
    let recording_id = stage_resolve_recording(root, run, ctx, total)?;
    // ONE scope for the whole run: the same object renders the lookup table,
    // seeds the stores, drives the kernel and scores the result, so the four
    // stages cannot disagree about which test cases this run covers.
    let scope = crate::scope::RunScope::of(run);
    let recording = crate::scope::ScopedRecording::open(root, &recording_id, scope.clone())
        .map_err(|e| format!("open recording {recording_id}: {e}"))?;

    // Render the lookup table (whole-document JSON; round-trips through both the
    // candidate's LocalFileLookupSource and the divergence detector).
    set_stage(root, run, ctx, 2, total, "rendering lookup table");
    let table = crate::lookup::render_lookup_table(&recording, &recording_id, 1)
        .map_err(|e| format!("render lookup table: {e}"))?;
    write_json(&root.lookup_table_path(&run.run_id), &table)
        .map_err(|e| format!("write lookup table: {e}"))?;
    if table.entries.is_empty() {
        return Err("rendered lookup table is empty".to_string());
    }

    set_status(root, run, RunStatus::Building, None);
    ctx.run_state("building");
    // Replay candidate; pg/redis/migration/superposition-init come up as deps.
    set_stage(
        root,
        run,
        ctx,
        3,
        total,
        "starting replay router (DEJA_MODE=replay)",
    );
    // `--build` defaults on for the legacy compose-build candidate (no baked
    // image). For PARALLEL replays this is a hazard: every per-run project would
    // concurrently rebuild the SAME `deja-router-local:latest` tag, racing the
    // build cache. The parallel runner builds the replay image ONCE up front and
    // sets DEMO_REPLAY_NO_BUILD=1, so isolated runs reuse it instead of rebuilding.
    let build = run.candidate_image.is_none() && std::env::var("DEMO_REPLAY_NO_BUILD").is_err();
    compose_up(
        demo,
        ctx,
        "starting replay router (DEJA_MODE=replay)",
        &recording_id,
        &run.run_id,
        &["hyperswitch-replay"],
        build,
        &[],
    )?;

    set_status(root, run, RunStatus::Running, None);
    ctx.run_state("running");
    set_stage(root, run, ctx, 4, total, "waiting for replay router");
    wait_health(demo.replay_port, Duration::from_secs(240))?;

    set_stage(
        root,
        run,
        ctx,
        5,
        total,
        "driving recorded requests (kernel)",
    );
    // Reset redis to the empty state the record run started from (post `down -v`).
    // Replay routing is selected by each boundary's explicit declaration plus
    // DEJA_MODE=replay, so the harness prepares concrete store state instead of
    // adding process-level overrides. Some cache keys the record run wrote carry
    // no TTL (e.g. `merchant_key_store_*`); without this flush, the FIRST replayed
    // request whose recording observed a cache MISS instead reads a STALE HIT and
    // diverges (signup's merchant-existence check finds the key store the record
    // run wrote → short-circuits → "merchant already exists" / UR_15). The
    // in-memory moka cache is already fresh per replay process; only redis carries
    // record's writes over.
    // Store transport for this run (S1 seam): compose here; the in-pod k8s
    // runner builds a StoreExec::direct against its sidecars instead.
    let store = StoreExec::compose(
        demo.compose_base_args(),
        demo.compose_env(&recording_id, &run.run_id),
    );
    flush_redis(&store)?;
    // GENERAL SEEDING (replay precondition materialization).
    // Replay routing is driven by the candidate's explicit per-boundary
    // declarations plus DEJA_MODE=replay. Seed materialization restores the
    // recorded preconditions into concrete stores before the replay workload
    // runs; materialization remains best-effort because scoring can still report
    // the replay outcome when store seeding is unavailable.
    let seed_certificate = materialize_seed_plan(&store, &recording, &run.run_id);
    let seed_certificate_path = root.seed_certificate_path(&run.run_id);
    // Written here (the file is stage-4 evidence), REGISTERED at stage 6 by the
    // publish loop like every other stream artifact. Registering here as well
    // stored the runner pod's local path verbatim — a row that answered every
    // read with ENOENT once the pod died — and would now duplicate the row.
    if let Err(e) = write_json(&seed_certificate_path, &seed_certificate) {
        eprintln!("lifecycle: seed certificate write failed: {e}; continuing");
    }
    run_kernel(
        &demo.kernel_bin,
        demo.replay_port,
        root,
        ctx,
        &recording_id,
        &run.run_id,
        &scope,
    )?;

    // Compose: the orchestrator serves artifacts from its own state dir.
    score_and_register(root, run, ctx, &recording_id, total, &ArtifactSink::Local)
}

/// Stage 1 (shared by the compose worker and the in-pod runner): resolve the
/// recording identity, materialize `events.jsonl`, and settle which
/// correlations the run drives.
///
/// With an `s3_source` the spec's recording id is a SESSION FILTER and may be
/// unset (the scan auto-resolves it when the prefix holds exactly one
/// session); the session-layout path requires it up front.
fn stage_resolve_recording(
    root: &HarnessRoot,
    run: &mut Run,
    ctx: &StoreCtx,
    total: u32,
) -> Result<String, String> {
    let wanted = run
        .spec
        .recording_id
        .clone()
        .or_else(|| run.recording_id.clone());
    let s3_source = run.spec.s3_source.clone();
    let recording_id = match &s3_source {
        // Deployed aggregator layout: scan the given bucket/prefix, resolve
        // the session, materialize events.jsonl.
        Some(source) => {
            set_stage(
                root,
                run,
                ctx,
                1,
                total,
                "scanning S3 source (aggregator layout)",
            );
            resolve_recording_from_source(root, ctx, source, wanted.as_deref())?
        }
        // Session layout: the recording comes back out of the deja bucket.
        // (If a prior run on this host already pulled it to disk, reuse that.)
        None => {
            let recording_id =
                wanted.ok_or_else(|| "replay run requires recording_id".to_string())?;
            // Name the SOURCE, not the store that happens to back it. MinIO is
            // the local demo's object store; a deployed run reads real S3, and a
            // stage label that says otherwise tells an operator to go looking in
            // the wrong place.
            set_stage(
                root,
                run,
                ctx,
                1,
                total,
                &format!("fetching recording {recording_id}"),
            );
            if !crate::scope::TapeSlot::is_materialized(root, &recording_id) {
                pull_recording(root, ctx, &recording_id)?;
            }
            recording_id
        }
    };
    run.recording_id = Some(recording_id.clone());
    ctx.run_recording(&recording_id);
    if !crate::scope::TapeSlot::is_materialized(root, &recording_id) {
        return Err(format!(
            "recording {recording_id} not found in S3 or on disk"
        ));
    }
    admit_against_recording(root, ctx, &recording_id)?;
    resolve_correlation_filter(root, run, ctx, &recording_id)?;
    Ok(recording_id)
}

/// What the tape's own ingest report says about the recording having stopped
/// growing before it was read.
///
/// Three states, not two, because "the report says it was incomplete" and "there
/// is no report" are different facts and refusing on both would be wrong.
#[derive(Debug, Clone, PartialEq, Eq)]
enum TapeCompleteness {
    /// Sealed, or every producer instance emitted its `eof` marker.
    Complete(String),
    /// The recording was demonstrably still being written when this was read.
    Incomplete(String),
    /// No ingest report beside the tape — a tape materialized by an older
    /// build. Absence of the audit trail is not evidence the recording was
    /// short, so this is said out loud and not treated as a refusal.
    Unproven(String),
}

impl TapeCompleteness {
    /// The reason a verdict from this tape cannot be read as complete, if any.
    fn provisional_reason(&self) -> Option<&str> {
        match self {
            TapeCompleteness::Incomplete(reason) => Some(reason),
            TapeCompleteness::Complete(_) | TapeCompleteness::Unproven(_) => None,
        }
    }
}

/// Read the completeness the ingest already established, from beside the tape.
///
/// The report is the seam. A run that reuses an already-materialized tape never
/// re-ingests it, so the fact cannot ride in memory from the pull to the
/// verdict — it rides on disk with the thing it describes, and both the
/// admission gate and the scoring stamp read it from there.
fn tape_completeness(root: &HarnessRoot, recording_id: &str) -> TapeCompleteness {
    let path = crate::scope::TapeSlot::ingest_report_path(root, recording_id);
    let Ok(body) = std::fs::read_to_string(&path) else {
        return TapeCompleteness::Unproven(format!(
            "no ingest report beside the tape ({}) — this recording's completeness was never \
             recorded",
            path.display()
        ));
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&body) else {
        return TapeCompleteness::Unproven(format!(
            "ingest report at {} is not readable JSON",
            path.display()
        ));
    };
    // Read defensively: a report written by an older build has no
    // `completeness` at all, which is Unproven rather than a failure to parse.
    let Some(state) = value
        .pointer("/completeness/state")
        .and_then(|v| v.as_str())
    else {
        return TapeCompleteness::Unproven(format!(
            "ingest report at {} predates completeness accounting",
            path.display()
        ));
    };
    match state {
        "still_landing" => TapeCompleteness::Incomplete(
            value
                .pointer("/completeness/reason")
                .and_then(|v| v.as_str())
                .unwrap_or("the recording had not stopped growing when it was read")
                .to_owned(),
        ),
        "sealed" => TapeCompleteness::Complete("sealed session".to_owned()),
        "eof_observed" => TapeCompleteness::Complete(format!(
            "{} producer instance(s) emitted their `eof` marker",
            value
                .pointer("/completeness/instances")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default()
        )),
        other => TapeCompleteness::Unproven(format!("unrecognized completeness state `{other}`")),
    }
}

/// Refuse — or explicitly admit — a replay against a recording that cannot be
/// shown to have finished.
///
/// The ingest has computed this fact for a while and nobody acted on it. Acting
/// on it is the whole point: run
/// `rp-sbx-1868bb92e9-1868bb9-08120623-rk-0812064511654` scored 9/44 against a
/// prefix of `rec-1868bb9-08120623-rk`, a recording that later reported 87
/// correlations, and the verdict was shaped exactly like a complete one. Three
/// analyses were built on it.
///
/// Refusal is the default because the failure it prevents is silent and the
/// failure it causes is not: a refused run says why, immediately, where a
/// prefix-scored run says nothing and is believed. It is not unconditional
/// because a caller may genuinely want a partial read — a recording still in
/// flight, deliberately sampled early. That caller sets
/// [`ALLOW_INCOMPLETE_RECORDING`], and the run then reports `inconclusive`
/// instead of pass/fail, because a prefix cannot answer the question asked.
fn admit_against_recording(
    root: &HarnessRoot,
    ctx: &StoreCtx,
    recording_id: &str,
) -> Result<(), String> {
    admit_with_policy(
        tape_completeness(root, recording_id),
        ctx,
        recording_id,
        incomplete_recordings_allowed(),
    )
}

/// The admission decision itself, with the opt-in passed in rather than read.
/// Split out so the policy is testable without a process-wide env var — and so
/// the rule is one expression a reader can check against the doc above it.
fn admit_with_policy(
    completeness: TapeCompleteness,
    ctx: &StoreCtx,
    recording_id: &str,
    allow_incomplete: bool,
) -> Result<(), String> {
    match completeness {
        TapeCompleteness::Complete(how) => {
            ctx.log(
                "ingest",
                &format!("recording {recording_id} is complete ({how})"),
            );
            Ok(())
        }
        TapeCompleteness::Unproven(why) => {
            // Loud, but not a refusal. Nothing here says the recording was
            // short; refusing would strand every tape pulled by an older build.
            let line = format!(
                "recording {recording_id}: completeness UNPROVEN — {why}. Re-pull it to get a \
                 delivery certificate; this run proceeds on an unverified tape."
            );
            eprintln!("lifecycle: {line}");
            ctx.log("ingest", &line);
            Ok(())
        }
        TapeCompleteness::Incomplete(reason) => {
            if allow_incomplete {
                let line = format!(
                    "recording {recording_id} is INCOMPLETE — {reason}. Admitted by \
                     {ALLOW_INCOMPLETE_RECORDING}: this run scores a PREFIX of the recording and \
                     its verdict is reported inconclusive."
                );
                eprintln!("lifecycle: {line}");
                ctx.log("ingest", &line);
                return Ok(());
            }
            Err(format!(
                "recording {recording_id} is not complete — {reason}. A replay scored against a \
                 recording that is still being written scores a PREFIX of it, and the verdict is \
                 indistinguishable from a complete one (a real run reported 9/44 on a recording \
                 that later held 87 correlations). Wait for the recorder to finish and re-pull, \
                 or set {ALLOW_INCOMPLETE_RECORDING}=1 to score the prefix deliberately — that \
                 run reports inconclusive."
            ))
        }
    }
}

/// Opt in to scoring a recording that cannot be shown to have finished.
///
/// This is a caller's intent and its natural home is the run request. It is an
/// environment knob instead because `RunSpec` is constructed by struct literal
/// in modules this change may not touch; moving it onto the request is a
/// follow-up, and the only thing it changes is the granularity (process-wide
/// here, per-run there). Either way the decision is recorded where it matters:
/// the run's own log line, and the `recording_incomplete` stamp on its verdict.
const ALLOW_INCOMPLETE_RECORDING: &str = "DEJA_ALLOW_INCOMPLETE_RECORDING";

fn incomplete_recordings_allowed() -> bool {
    matches!(
        std::env::var(ALLOW_INCOMPLETE_RECORDING)
            .unwrap_or_default()
            .trim(),
        "1" | "true" | "yes"
    )
}

/// Settle the run's correlation filter into a CONCRETE list of ids, here, once,
/// before any stage reads the scope.
///
/// An unbounded replay must not be reachable. Driving a whole session is not a
/// choice anyone makes on purpose — the one run that did it drove 455
/// correlations, took 439.8s and died in the scorer — so an absent filter
/// resolves to the first [`crate::scope::MAX_CORRELATIONS_PER_RUN`] in tape
/// order rather than to everything.
///
/// The resolution is written back into `run.spec.correlation_filter`, so what
/// the run carries of what it was asked to do is the 100 concrete ids that ran,
/// not an empty filter plus a rule someone has to know about. Everything
/// downstream already reads the spec: `RunScope::of(run)` in both drivers, the
/// run manifest, the kernel's env filter, the scorer, and the run json on disk
/// that the API and the divergence detector re-read afterwards.
///
/// Record mode is untouched. A recording has nothing to filter against, and
/// `RunScope`'s `EntireSession` stays exactly what an absent filter means as a
/// value — the ceiling is a property of ADMITTING a replay run, not of the type.
fn resolve_correlation_filter(
    root: &HarnessRoot,
    run: &mut Run,
    ctx: &StoreCtx,
    recording_id: &str,
) -> Result<(), String> {
    if run.spec.mode != crate::RunMode::Replay {
        return Ok(());
    }
    let requested = run.spec.correlation_filter.clone();
    // Only read the tape when the answer depends on it: an explicit filter is
    // the caller's, and a recording of 171,234 events should not be re-read to
    // confirm what the caller already named.
    let tape_order =
        if crate::scope::RunScope::from_filter(requested.as_deref()).is_entire_session() {
            crate::scope::TapeSlot::correlations_in_tape_order(root, recording_id)
                .map_err(|e| format!("read correlations of {recording_id}: {e}"))?
        } else {
            Vec::new()
        };
    let resolved = crate::scope::resolve_run_correlations(requested.as_deref(), &tape_order)?;
    if resolved.is_empty() {
        return Err(format!(
            "recording {recording_id} has no correlations to drive"
        ));
    }

    if requested.is_none() || requested.as_deref().is_some_and(<[String]>::is_empty) {
        let line = format!(
            "no correlations chosen — driving the first {} of {} in recording order",
            resolved.len(),
            tape_order.len()
        );
        eprintln!("lifecycle: {line}");
        ctx.log("scope", &line);
    }
    // Persist at the moment the decision is made rather than relying on the
    // next stage's write: the resolved list is this run's record of what it was
    // asked to do, and a run that fails before the next stage would otherwise
    // show an empty filter and no sign of which correlations it had picked.
    run.spec.correlation_filter = Some(resolved);
    if let Err(e) = write_json(&root.run_path(&run.run_id), run) {
        eprintln!("lifecycle: failed to persist the resolved correlation filter: {e}");
    }
    Ok(())
}

/// Final stage (shared): score the run, report the verdict, register the
/// The replay-run stream artifacts [`score_and_register`] publishes, as
/// `(kind, s3-filename)`. One list so the sink loop and the DB-constraint
/// coverage test stay in agreement. `observed` also carries the replay-side
/// execution-graph nodes (`DejaRecord::GraphNode`).
const REPLAY_STREAM_ARTIFACTS: [(&str, &str); 6] = [
    ("lookup_table", "lookup_table.jsonl"),
    ("observed", "observed.jsonl"),
    ("http_diffs", "http_diffs.jsonl"),
    ("scorecard", "scorecard.json"),
    ("call_ledger", "call_ledger.jsonl"),
    // The seed certificate is the run's own account of what seeding did —
    // planned/materialized/failed per entry, with readback. It used to be
    // registered with the runner pod's LOCAL path (never uploaded), so on k8s
    // the one artifact that explains a seed-shaped divergence was unreadable
    // the moment the pod died. It publishes like every other stream; the same
    // prefix already carries the full lookup table, so this adds no new kind
    // of egress.
    ("seed_certificate", "seed-certificate.json"),
];

/// Where a run's replay artifacts are published so the dashboard can read them
/// back AFTER the run. Compose/local: the orchestrator serves them from its own
/// state dir, so the local path is enough. In-pod: the runner pod is ephemeral,
/// so each artifact is uploaded to S3 under `<prefix>/<run_id>/` and the durable
/// `s3://` URI is registered — the orchestrator hydrates from there on demand.
/// The RAW streams (`observed`, `http_diffs`, `lookup_table`) are kept, so any
/// current or future visualization derives from them, not just today's cards.
pub enum ArtifactSink {
    Local,
    S3 {
        cfg: crate::s3::S3Config,
        prefix: String,
    },
}

impl ArtifactSink {
    /// The in-pod runner selects the S3 sink via `DEJA_RUN_ARTIFACT_S3=1` (set on
    /// the Job's runner container ONLY, never the orchestrator), using the
    /// runner's S3 config and `DEJA_RUN_ARTIFACT_PREFIX` (default `replay-runs`).
    /// Anything else → Local (the compose worker's orchestrator serves artifacts
    /// itself). A misconfigured Job (flag unset) degrades to pod-local artifacts,
    /// which the dashboard simply won't show — never a failed run.
    pub fn from_env() -> Self {
        if std::env::var("DEJA_RUN_ARTIFACT_S3").ok().as_deref() == Some("1") {
            let prefix = std::env::var("DEJA_RUN_ARTIFACT_PREFIX")
                .ok()
                .map(|s| s.trim().trim_matches('/').to_owned())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "replay-runs".to_owned());
            ArtifactSink::S3 {
                cfg: crate::s3::S3Config::from_env(),
                prefix,
            }
        } else {
            ArtifactSink::Local
        }
    }

    /// The durable object key for a run artifact: `<prefix>/<run_id>/<filename>`.
    pub fn key(prefix: &str, run_id: &str, filename: &str) -> String {
        format!("{prefix}/{run_id}/{filename}")
    }

    /// Publish one artifact file and return `(uri, bytes)` to register, or None
    /// if the file is absent (best-effort — some artifacts are optional) or the
    /// upload failed (logged; the run still finishes, the dashboard just lacks
    /// that one artifact). S3: upload under the run prefix, return the `s3://`
    /// URI. Local: return the local path unchanged.
    pub fn publish(
        &self,
        run_id: &str,
        filename: &str,
        local: &std::path::Path,
    ) -> Option<(String, i64)> {
        let bytes = std::fs::metadata(local).ok()?.len() as i64;
        match self {
            ArtifactSink::Local => Some((local.display().to_string(), bytes)),
            ArtifactSink::S3 { cfg, prefix } => {
                let data = std::fs::read(local)
                    .map_err(|e| eprintln!("artifact: read {}: {e}", local.display()))
                    .ok()?;
                let key = Self::key(prefix, run_id, filename);
                match deja_compactor::put_object(cfg, &key, data) {
                    Ok(()) => Some((format!("s3://{}/{}", cfg.bucket, key), bytes)),
                    Err(e) => {
                        eprintln!("artifact: upload {key} failed: {e}");
                        None
                    }
                }
            }
        }
    }
}

/// Extract the record-side execution-graph nodes (`DejaRecord::GraphNode`) for
/// THIS RUN'S correlations into a compact `record_graph.jsonl`: the STRUCTURE
/// of the recorded run's cascade (span ids, parents, names, level, timing, span
/// fields), NOT its boundary payloads (args/results). The in-pod runner already
/// holds the recording locally (it drove replay off it); emitting just the graph
/// nodes as a run artifact lets the record side reach the dashboard through the
/// SAME S3 sink as the replay side, WITHOUT copying the sensitive recording tape
/// off the pod. Returns the node count (0 ⇒ no nodes ⇒ nothing to publish).
///
/// This took a raw `&Path` and emitted EVERY node on the tape: 86,204 nodes /
/// 29 MB against three driven correlations. Nodes carry `fields` (`key`,
/// `table`, `value`) and a `golden_log_line` on all 42,310 `ROOT_SPAN`s, so
/// that published the span structure and field values of every request in a
/// production session to S3 and out through an unauthenticated `GET /graph`.
/// That is a containment problem, not a size one — hence a `&ScopedRecording`,
/// which cannot be constructed without saying which cases the run covers.
fn write_record_graph_nodes(
    nodes: &[deja_core::ExecutionGraphNode],
    dest: &std::path::Path,
) -> Result<usize, String> {
    let mut out = String::new();
    for node in nodes {
        // Round-trip through the wire enum so the artifact keeps the tagged
        // one-stream shape the dashboard's reader expects.
        let line = serde_json::to_string(&deja::DejaRecord::GraphNode(Box::new(node.clone())))
            .map_err(|e| format!("encode graph node {}: {e}", node.node_id))?;
        out.push_str(&line);
        out.push('\n');
    }
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
    }
    std::fs::write(dest, out).map_err(|e| format!("write {}: {e}", dest.display()))?;
    Ok(nodes.len())
}

/// Derive the record-side execution graph for this run and leave it at
/// `record_graph_path`, ready to publish. Split out of `score_and_register` and
/// called BEFORE anything scores so a run record never carries a verdict it
/// then disowns.
///
/// `Ok(Some(n))` — n nodes extracted, publish them. `Ok(None)` — no graph, for
/// a reason written to `record_graph_note_path` (and stderr) rather than only
/// logged: the scorer folds that note into the scorecard's warnings, so the
/// reason travels with the verdict. `Err` — a real I/O failure.
///
/// The tape's scoping refusals — no anchors at all, or an in-scope case that
/// reaches no graph root — used to be `Err`, failing the whole run. That
/// coupling destroyed the scorecard over an artifact it does not read:
/// divergence detection never consults the graph, and on tapes recorded before
/// graph nodes carried a correlation, the refusal mostly means "this tape
/// cannot say", not "this run is invalid". The refusal's PURPOSE — never
/// publish a partial graph as if it were whole — is kept: nothing is
/// published, and the absence explains itself in the run's own record instead
/// of on a dead stderr.
fn extract_record_graph(
    root: &HarnessRoot,
    run: &Run,
    recording_id: &str,
) -> Result<Option<usize>, String> {
    // Record-side execution graph: the recorded run's cascade STRUCTURE (graph
    // nodes only — never the boundary payloads), derived from the recording the
    // runner already holds locally, scoped to the run's own correlations.
    let scope = crate::scope::RunScope::of(run);
    let recording = match crate::scope::ScopedRecording::open(root, recording_id, scope) {
        Ok(recording) => recording,
        // No tape on this node: a different problem from a tape without a graph,
        // and not one the record-graph step can decide anything about.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            eprintln!(
                "lifecycle: no recording {recording_id} on disk — record graph not extracted ({e})"
            );
            return Ok(None);
        }
        Err(e) => return Err(format!("open recording {recording_id}: {e}")),
    };
    let nodes = match recording.graph_nodes() {
        Ok(nodes) => nodes,
        // The scoping refusal (InvalidData is the kind both refusals carry).
        // Publish NOTHING, and leave the reason where the scorer will pick it
        // up — a run whose graph cannot be built still gets its verdict, with
        // the gap stated in the same record.
        Err(e) if e.kind() == std::io::ErrorKind::InvalidData => {
            let note = format!("record graph unavailable: {e}");
            eprintln!("lifecycle: {note}");
            let note_path = root.record_graph_note_path(&run.run_id);
            if let Some(parent) = note_path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            if let Err(write_err) = std::fs::write(&note_path, &note) {
                // The note is the reason's ride into the scorecard; losing it
                // silently would recreate the exact failure mode this exists
                // to end.
                return Err(format!(
                    "record-graph note write failed ({write_err}) while degrading: {note}"
                ));
            }
            return Ok(None);
        }
        Err(e) => return Err(format!("record-graph extract: {e}")),
    };
    if nodes.is_empty() {
        // Under EntireSession this means the tape truly carries no graph.
        // Staying quiet here is what let an entire missing record-side graph
        // look like an ordinary run for weeks — the dashboard renders the
        // absence as "skipped" on every row, which reads as a finding.
        eprintln!(
            "lifecycle: recording {recording_id} carries NO graph nodes — the record side \
             of the execution graph will be empty and no comparison is possible"
        );
        return Ok(None);
    }
    let node_count = write_record_graph_nodes(&nodes, &root.record_graph_path(&run.run_id))
        .map_err(|e| format!("record-graph extract: {e}"))?;
    Ok(Some(node_count))
}

/// replay artifacts (best-effort; absent files are skipped).
fn score_and_register(
    root: &HarnessRoot,
    run: &mut Run,
    ctx: &StoreCtx,
    recording_id: &str,
    total: u32,
    sink: &ArtifactSink,
) -> Result<(), String> {
    set_stage(
        root,
        run,
        ctx,
        total,
        total,
        "scoring divergence (byte-exact)",
    );
    // BEFORE the verdict, deliberately. This step can refuse the run, and a run
    // that is going to be refused must not first write a scorecard and push a
    // result the caller then marks failed. `set_stage` above is a label, not a
    // verdict, so it is safe on this side of the refusal.
    let record_graph_nodes = extract_record_graph(root, run, recording_id)?;

    let card = crate::divergence::detect_and_score(root, &run.run_id)
        .map_err(|e| format!("score: {e}"))?;

    // The recording this verdict is ABOUT. A run admitted against a recording
    // that was still being written scored a prefix of it, and the scorer has no
    // way to know that — it sees a tape and scores the tape. So the stamp is
    // applied here, where the run's own record is written, and it is applied to
    // the verdict itself rather than to a note beside it: `inconclusive` is
    // what a prefix score is, and it is the one value no reader mistakes for a
    // clean result.
    let provisional = tape_completeness(root, recording_id)
        .provisional_reason()
        .map(str::to_owned);

    let verdict_line = format!(
        "run {} verdict pass={} ({}){}",
        run.run_id,
        card.verdict.pass,
        card.verdict.reason,
        match &provisional {
            Some(reason) => format!(
                " — REPORTED INCONCLUSIVE: scored against an incomplete recording ({reason})"
            ),
            None => String::new(),
        }
    );
    eprintln!("lifecycle: {verdict_line}");
    ctx.log("scoring divergence (byte-exact)", &verdict_line);
    let verdict = if provisional.is_some() || card.verdict.inconclusive {
        "inconclusive"
    } else if card.verdict.pass {
        "pass"
    } else {
        "fail"
    };
    // Stamp the stored scorecard payload too: the dashboard reads this value,
    // and a verdict field alone leaves a reader to wonder which of the two
    // kinds of inconclusive this was.
    let mut result_value = serde_json::to_value(&card).ok();
    if let (Some(serde_json::Value::Object(map)), Some(reason)) =
        (result_value.as_mut(), provisional.as_ref())
    {
        map.insert(
            "recording_incomplete".to_owned(),
            serde_json::json!({
                "recording_id": recording_id,
                "reason": reason,
                "effect": "this run scored a PREFIX of the recording; the verdict is inconclusive",
            }),
        );
    }
    ctx.result(Some(verdict), result_value.as_ref());

    // Publish the raw replay streams + computed cards through the sink. In-pod
    // these upload to S3 (durable past the ephemeral pod); compose registers
    // local paths. The RAW streams (observed, http_diffs, lookup_table) are kept
    // so any current or future view derives from them. Each entry also lands in
    // the run manifest index below. `observed` carries the replay-side
    // execution-graph nodes (`DejaRecord::GraphNode`) — no separate artifact.
    let mut index = serde_json::Map::new();
    for (kind, filename) in REPLAY_STREAM_ARTIFACTS {
        let path = match kind {
            "lookup_table" => root.lookup_table_path(&run.run_id),
            "observed" => root.observed_path(&run.run_id),
            "http_diffs" => root.http_diff_path(&run.run_id),
            "scorecard" => root.scorecard_path(&run.run_id),
            "call_ledger" => root.call_ledger_path(&run.run_id),
            "seed_certificate" => root.seed_certificate_path(&run.run_id),
            _ => continue,
        };
        if let Some((uri, bytes)) = sink.publish(&run.run_id, filename, &path) {
            ctx.artifact_uri(Some(recording_id), kind, &uri, Some(bytes));
            index.insert(
                kind.to_owned(),
                serde_json::json!({ "uri": uri, "bytes": bytes }),
            );
        }
    }

    // Record-side execution graph: PUBLISH only — it was extracted (and could
    // have refused the run) before scoring. Published through the SAME sink as
    // the replay side so the dashboard's `/graph` record side renders for in-pod
    // runs too, WITHOUT the sensitive recording tape ever leaving the pod.
    // (Compose registers the local path; the orchestrator also reads the
    // recording directly there, so this is belt-and-suspenders — but it keeps
    // both modes on one artifact contract.)
    if let Some(node_count) = record_graph_nodes {
        let record_graph_path = root.record_graph_path(&run.run_id);
        if let Some((uri, bytes)) =
            sink.publish(&run.run_id, "record_graph.jsonl", &record_graph_path)
        {
            ctx.artifact_uri(Some(recording_id), "record_graph", &uri, Some(bytes));
            index.insert(
                "record_graph".to_owned(),
                serde_json::json!({ "uri": uri, "bytes": bytes, "nodes": node_count }),
            );
        }
    }

    // Static HTML visualization (the demo's existing visualize-replay.py);
    // best-effort — python3 may be absent.
    let viz = root
        .root
        .join(format!("replay-visualization-{}.html", run.run_id));
    let state_dir = root.root.display().to_string();
    let viz_ok = Command::new("python3")
        .args([
            "demo/visualize-replay.py",
            state_dir.as_str(),
            "--run",
            run.run_id.as_str(),
        ])
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if viz_ok {
        if let Some((uri, bytes)) = sink.publish(&run.run_id, "visualization.html", &viz) {
            ctx.artifact_uri(Some(recording_id), "visualization_html", &uri, Some(bytes));
            index.insert(
                "visualization_html".to_owned(),
                serde_json::json!({ "uri": uri, "bytes": bytes }),
            );
        }
    }

    // Run manifest: one object that indexes every artifact + the recording
    // pointer + candidate + correlation subset + verdict, so the dashboard (and
    // any future view) can find the whole run from a single key.
    let manifest = serde_json::json!({
        "schema": "deja.replay-run/v1",
        "run_id": run.run_id,
        "recording_id": recording_id,
        "candidate_image": run.candidate_image,
        "correlation_filter": run.spec.correlation_filter,
        "verdict": verdict,
        // Travels with the verdict, in the one object that indexes the whole
        // run: a reader who finds this key is looking at a prefix score.
        "recording_incomplete": provisional,
        "artifacts": index,
    });
    let manifest_path = root
        .root
        .join("runs")
        .join(format!("{}.manifest.json", run.run_id));
    if let Ok(bytes) = serde_json::to_vec_pretty(&manifest) {
        if std::fs::write(&manifest_path, bytes).is_ok() {
            if let Some((uri, bytes)) = sink.publish(&run.run_id, "manifest.json", &manifest_path) {
                ctx.artifact_uri(Some(recording_id), "manifest", &uri, Some(bytes));
            }
        }
    }
    Ok(())
}

/// The in-pod (k8s Job) replay driver: `drive_replay` minus the compose
/// stages. The candidate service is a POD CONTAINER k8s started (not ours to
/// start or tear down), the stores are sidecars reached directly
/// ([`StoreExec::Direct`]), and progress flows back through the caller's
/// `StoreCtx` (the Http transport in a Job). Sequencing:
///
///   1. resolve + pull the recording (shared stage)
///   2. render the lookup table into the SHARED workspace volume at the
///      contract path ([`HarnessRoot::replay_contract`]); the candidate loads
///      it eagerly at boot (its own env binds the path — for the Hyperswitch
///      router, `ROUTER__DEJA__REPLAY__SOURCE`)
///   3. migrate the sidecar pg (operator-supplied command; the per-corr
///      schema clone in seeding needs a migrated `public`)
///   4. flush + seed the sidecar stores (idempotent across Job retries), then
///      publish the readiness sentinel — the candidate's boot guard blocks on
///      it, so it can never serve against an unseeded store (A2)
///   5. wait for candidate health, drive the kernel
///   6. score + register artifacts (shared stage)
pub struct InPodOptions {
    /// Sidecar redis, e.g. ("127.0.0.1", 6379).
    pub redis_host: String,
    pub redis_port: u16,
    /// Sidecar pg conninfo URL (also what seeding's psql uses via `-d`).
    pub database_url: String,
    /// The router container's health/traffic port (pod-shared netns).
    pub router_port: u16,
    /// deja-kernel binary path inside the runner container.
    pub kernel_bin: String,
    /// Migration command (argv) run at stage 3 with DATABASE_URL set; None
    /// logs the stage as skipped (a pre-migrated store, e.g. an initContainer).
    ///
    /// The migration CONTENT this command applies must be the CANDIDATE's
    /// (its `migrations/` tree at its code sha), staged onto the shared volume
    /// by the candidate — never the harness runner's own baked migrations. The
    /// runner owns the migration TOOL (diesel), not the schema. `expected_schema`
    /// below is what enforces that the right content actually ran.
    pub migrate_cmd: Option<Vec<String>>,
    /// The CANDIDATE's expected schema fingerprint — its own migration versions,
    /// derived by the executor from the candidate ref, NOT a harness constant.
    /// After migrating, the runner reads the live schema back and refuses
    /// (fail-closed, P1) unless it is exactly this set, so a stale or foreign
    /// migration set becomes a loud refusal instead of a false verdict (A1).
    /// None = no candidate schema supplied: record the live fingerprint, no gate.
    pub expected_schema: Option<SchemaFingerprint>,
}

pub fn drive_replay_in_pod(
    root: &HarnessRoot,
    run: &mut Run,
    ctx: &StoreCtx,
    opts: &InPodOptions,
) -> Result<(), String> {
    let total = 6;
    set_status(root, run, RunStatus::Resolving, None);
    ctx.run_state("resolving");
    let recording_id = stage_resolve_recording(root, run, ctx, total)?;
    // ONE scope for the whole run: the same object renders the lookup table,
    // seeds the stores, drives the kernel and scores the result, so the four
    // stages cannot disagree about which test cases this run covers.
    let scope = crate::scope::RunScope::of(run);
    let recording = crate::scope::ScopedRecording::open(root, &recording_id, scope.clone())
        .map_err(|e| format!("open recording {recording_id}: {e}"))?;

    set_stage(root, run, ctx, 2, total, "rendering lookup table");
    let table = crate::lookup::render_lookup_table(&recording, &recording_id, 1)
        .map_err(|e| format!("render lookup table: {e}"))?;
    write_json(&root.lookup_table_path(&run.run_id), &table)
        .map_err(|e| format!("write lookup table: {e}"))?;
    if table.entries.is_empty() {
        return Err("rendered lookup table is empty".to_string());
    }

    set_status(root, run, RunStatus::Building, None);
    ctx.run_state("building");
    set_stage(root, run, ctx, 3, total, "migrating sidecar pg");
    match &opts.migrate_cmd {
        Some(argv) if !argv.is_empty() => {
            let mut cmd = Command::new(&argv[0]);
            cmd.args(&argv[1..]).env("DATABASE_URL", &opts.database_url);
            let status = run_streamed(cmd, ctx, "migrating sidecar pg", "migrate")?;
            if !status.success() {
                return Err(format!("migration command failed (status {status})"));
            }
        }
        _ => ctx.log(
            "migrating sidecar pg",
            "no migrate command configured — assuming a pre-migrated store",
        ),
    }

    set_status(root, run, RunStatus::Running, None);
    ctx.run_state("running");
    set_stage(root, run, ctx, 4, total, "seeding sidecar stores");
    let store = StoreExec::direct(
        opts.redis_host.clone(),
        opts.redis_port,
        opts.database_url.clone(),
    );

    // A1/P1: verify the migrated schema is EXACTLY the candidate's BEFORE seeding
    // into it. The live fingerprint is read back from the store; the expected set
    // is the candidate's own migration versions (supplied by the executor, a
    // function of the candidate ref — never a harness constant). A mismatch is a
    // fail-closed refusal that names the drift, not a silent seed-into-wrong-
    // schema that resurfaces later as a phantom candidate regression.
    let live_schema = read_schema_fingerprint(&store)?;
    ctx.log(
        "seeding sidecar stores",
        &format!(
            "live schema: {} migrations applied (head {})",
            live_schema.count(),
            live_schema.head().unwrap_or("none"),
        ),
    );
    if let Some(expected) = &opts.expected_schema {
        if !live_schema.matches(expected) {
            let (missing, extra) = live_schema.diff(expected);
            return Err(format!(
                "schema fingerprint mismatch (P1): candidate expects {} migrations (head {}), \
                 store has {} (head {}); missing {} [{}], extra {} [{}]. The applied migration set \
                 is not the candidate's — refusing rather than emit a false verdict.",
                expected.count(),
                expected.head().unwrap_or("none"),
                live_schema.count(),
                live_schema.head().unwrap_or("none"),
                missing.len(),
                sample_versions(&missing),
                extra.len(),
                sample_versions(&extra),
            ));
        }
        ctx.log(
            "seeding sidecar stores",
            "schema fingerprint matches the candidate (P1 pass)",
        );
    }

    flush_redis(&store)?;
    let seed_certificate = materialize_seed_plan(&store, &recording, &run.run_id);
    let seed_certificate_path = root.seed_certificate_path(&run.run_id);
    // Written here (the file is stage-4 evidence), REGISTERED at stage 6 by the
    // publish loop like every other stream artifact. Registering here as well
    // stored the runner pod's local path verbatim — a row that answered every
    // read with ENOENT once the pod died — and would now duplicate the row.
    if let Err(e) = write_json(&seed_certificate_path, &seed_certificate) {
        eprintln!("lifecycle: seed certificate write failed: {e}; continuing");
    }

    // A2: the candidate boots as a pod sibling with no ordering guarantee vs
    // this runner. It aborts loudly if the lookup table (stage 2) is missing,
    // but nothing otherwise stops it serving traffic against a store this runner
    // has not yet seeded — a between-stages boot yields a FALSE divergence.
    // Publish the readiness sentinel now, only after seeding; the candidate's
    // boot command blocks on it (`ReplayContract::wait_for_seed_snippet`). Fatal
    // on failure: a missing sentinel would hang the candidate until the Job times
    // out. Idempotent across Job retries (overwrite).
    let ready = root.ready_sentinel_path(&run.run_id);
    std::fs::write(&ready, run.run_id.as_bytes())
        .map_err(|e| format!("publish readiness sentinel {}: {e}", ready.display()))?;
    ctx.log(
        "seeding sidecar stores",
        &format!(
            "stores seeded; readiness sentinel published at {}",
            ready.display()
        ),
    );

    set_stage(
        root,
        run,
        ctx,
        5,
        total,
        "driving recorded requests (kernel)",
    );
    wait_health(opts.router_port, Duration::from_secs(240))?;
    run_kernel(
        &opts.kernel_bin,
        opts.router_port,
        root,
        ctx,
        &recording_id,
        &run.run_id,
        &scope,
    )?;

    // In-pod: DEJA_RUN_ARTIFACT_S3=1 (Job template) uploads artifacts to S3 so
    // they survive the ephemeral pod and the dashboard can hydrate them.
    score_and_register(
        root,
        run,
        ctx,
        &recording_id,
        total,
        &ArtifactSink::from_env(),
    )
}

// ---------------------------------------------------------------------------
// Shell-out helpers
// ---------------------------------------------------------------------------

/// How many times this run drives the workload. The default lives on
/// [`crate::RunSpec::iterations`] because the run's persisted record reads it
/// from there too — two copies of it could name two different numbers.
fn run_iterations(run: &Run) -> u64 {
    run.spec.iterations()
}

#[allow(clippy::too_many_arguments)] // worker plumbing, internal
fn compose_up(
    demo: &Demo,
    ctx: &StoreCtx,
    stage: &str,
    recording_id: &str,
    run_id: &str,
    services: &[&str],
    build: bool,
    extra_env: &[(&str, String)],
) -> Result<(), String> {
    let mut args = demo.compose_base_args();
    args.extend(["up".into(), "-d".into()]);
    // A baked `local_binary` candidate image must NOT be rebuilt by compose:
    // `--build` would re-run the overlay's build context and re-tag over it.
    if build {
        args.push("--build".into());
    }
    args.extend(services.iter().map(|s| s.to_string()));
    let cmdline = format!("docker {}", args.join(" "));
    eprintln!("lifecycle: {cmdline}");
    ctx.log(stage, &cmdline);
    let mut cmd = Command::new("docker");
    cmd.args(&args).envs(demo.compose_env(recording_id, run_id));
    cmd.envs(extra_env.iter().map(|(k, v)| (k.to_string(), v.clone())));
    let status = run_streamed(cmd, ctx, stage, "docker compose up")?;
    if !status.success() {
        return Err(format!("docker compose up failed (status {status})"));
    }
    Ok(())
}

/// Tear down a PER-RUN-ISOLATED replay project with `docker compose down -v`
/// (drop containers + the named volumes = its pg/redis data), so concurrent
/// replays don't leak stacks. A no-op when the project is the shared `deja-demo`
/// (the record-side project that holds the recording + MinIO other runs pull
/// from — only the one-click script tears THAT down). Best-effort: a teardown
/// failure is logged, never fatal (the verdict already stands).
fn teardown_if_isolated(demo: &Demo, run_id: &str) {
    if !demo.project.starts_with("deja-run-") {
        return; // shared project — leave it for the owning script's teardown
    }
    let mut args = demo.compose_base_args();
    args.extend(["down".into(), "-v".into(), "--remove-orphans".into()]);
    eprintln!(
        "lifecycle: tearing down isolated replay project {}",
        demo.project
    );
    match Command::new("docker")
        .args(&args)
        .envs(demo.compose_env(run_id, run_id))
        .output()
    {
        Ok(o) if o.status.success() => {}
        Ok(o) => eprintln!(
            "lifecycle: down {} failed (continuing): {}",
            demo.project,
            String::from_utf8_lossy(&o.stderr)
        ),
        Err(e) => eprintln!("lifecycle: down {} failed (continuing): {e}", demo.project),
    }
}

/// `docker compose exec -T redis-standalone redis-cli FLUSHALL` — wipe the
/// candidate's redis so the replay run begins from the same empty cache the
/// record run started with. See the call site in `drive_replay` for why this is
/// required for byte-exact self-replay. Best-effort: if redis isn't reachable
/// (e.g. a deployment without the standalone service) the flush is skipped
/// rather than failing the whole replay.
/// Read the applied migration versions back from the store's diesel bookkeeping
/// table — the ground truth of which schema is live (A1/P1). Guarded for an
/// unmigrated store: a missing `__diesel_schema_migrations` yields an EMPTY
/// fingerprint (not a hard error), which the P1 gate then reports as a mismatch
/// if a candidate schema was expected. A genuine connection/read failure is
/// fatal (fail-loud), so an unreadable store never masquerades as unmigrated.
fn read_schema_fingerprint(store: &StoreExec) -> Result<SchemaFingerprint, String> {
    let exists = store
        .psql(
            &["-A", "-t"],
            true,
            "SELECT to_regclass('__diesel_schema_migrations') IS NOT NULL",
        )
        .output()
        .map_err(|e| format!("probe schema migrations table: {e}"))?;
    if !exists.status.success() {
        return Err(format!(
            "probe schema migrations table failed: {}",
            String::from_utf8_lossy(&exists.stderr).trim()
        ));
    }
    if String::from_utf8_lossy(&exists.stdout).trim() != "t" {
        return Ok(SchemaFingerprint::new(Vec::new()));
    }
    let out = store
        .psql(
            &["-A", "-t"],
            true,
            "SELECT version FROM __diesel_schema_migrations ORDER BY version",
        )
        .output()
        .map_err(|e| format!("read schema migrations: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "read schema migrations failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    let applied = String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    Ok(SchemaFingerprint::new(applied))
}

/// Render the first few versions of a drift set for a refusal message (the full
/// set can be hundreds of entries).
fn sample_versions(v: &[String]) -> String {
    let shown: Vec<&str> = v.iter().take(5).map(String::as_str).collect();
    if v.len() > 5 {
        format!("{}, …", shown.join(", "))
    } else {
        shown.join(", ")
    }
}

fn flush_redis(store: &StoreExec) -> Result<(), String> {
    let mut cmd = store.redis_cli(&["FLUSHALL"]);
    eprintln!("lifecycle: {}", store_exec::describe(&cmd));
    match cmd.status() {
        Ok(status) if status.success() => Ok(()),
        Ok(status) => {
            eprintln!("lifecycle: redis FLUSHALL exited {status}; continuing (best-effort)");
            Ok(())
        }
        Err(e) => {
            eprintln!("lifecycle: could not run redis FLUSHALL: {e}; continuing (best-effort)");
            Ok(())
        }
    }
}

/// Seed a single redis STRING key (the EU-settlement demo's record-side seed,
/// and the ambient-template scalar path). The settlement READ is a RAW fred GET
/// (leaf boundary) against redis, so the seed lives in redis, not pg.
/// Best-effort: a failure logs and continues.
fn seed_redis(
    store: &StoreExec,
    key: &str,
    value: &str,
) -> (SeedMaterializationStatus, SeedReadback) {
    seed_redis_image(
        store,
        &RedisSeedImage {
            physical_key: key.to_string(),
            payload: RedisSeedPayload::String(value.to_string()),
            ttl_seconds: None,
        },
    )
}

/// The typed write-shape a redis seed materializes as — the backward half of
/// the [`deja::value::RedisWireValue`] transform (#39). One variant per value
/// family the wire type carries; each maps to the native write command whose
/// type-appropriate read returns the recorded value. Members are already
/// rendered to the raw strings redis holds (via
/// [`deja::value::RedisWireValue::to_redis_string`]).
#[derive(Debug, Clone, PartialEq, Eq)]
enum RedisSeedPayload {
    /// Scalar → `SET`; read back with `GET`.
    String(String),
    /// Map → `HSET` field/value pairs in recorded order; read back with
    /// `HGETALL`.
    Hash(Vec<(String, String)>),
    /// Array → `RPUSH` members in recorded order; read back with `LRANGE`.
    List(Vec<String>),
    /// Array recorded by a sorted-set method → `ZADD`; read back with
    /// `ZRANGE`. A range read without `WITHSCORES` records member ORDER but no
    /// scores, so each member's index is its score — that reproduces exactly
    /// what the recorded read observed (the order), fabricating nothing the
    /// replay can see.
    SortedSet(Vec<String>),
    /// Set → `SADD`; read back with `SMEMBERS` (unordered).
    Set(Vec<String>),
}

impl RedisSeedPayload {
    /// The `redis-cli` argv that writes this payload under `key`.
    fn write_args(&self, key: &str) -> Vec<String> {
        let mut args: Vec<String>;
        match self {
            Self::String(value) => {
                args = vec!["SET".into(), key.into(), value.clone()];
            }
            Self::Hash(pairs) => {
                args = Vec::with_capacity(2 + pairs.len() * 2);
                args.push("HSET".into());
                args.push(key.into());
                for (field, value) in pairs {
                    args.push(field.clone());
                    args.push(value.clone());
                }
            }
            Self::List(items) => {
                args = Vec::with_capacity(2 + items.len());
                args.push("RPUSH".into());
                args.push(key.into());
                args.extend(items.iter().cloned());
            }
            Self::SortedSet(members) => {
                args = Vec::with_capacity(2 + members.len() * 2);
                args.push("ZADD".into());
                args.push(key.into());
                for (index, member) in members.iter().enumerate() {
                    args.push(index.to_string());
                    args.push(member.clone());
                }
            }
            Self::Set(members) => {
                args = Vec::with_capacity(2 + members.len());
                args.push("SADD".into());
                args.push(key.into());
                args.extend(members.iter().cloned());
            }
        }
        args
    }

    /// The `redis-cli` argv of the type-appropriate readback READ for `key`.
    fn readback_args(&self, key: &str) -> Vec<String> {
        match self {
            Self::String(_) => vec!["GET".into(), key.into()],
            Self::Hash(_) => vec!["HGETALL".into(), key.into()],
            Self::List(_) => vec!["LRANGE".into(), key.into(), "0".into(), "-1".into()],
            Self::SortedSet(_) => vec!["ZRANGE".into(), key.into(), "0".into(), "-1".into()],
            Self::Set(_) => vec!["SMEMBERS".into(), key.into()],
        }
    }

    /// The `--raw` line image the readback READ must print if the seed landed:
    /// one element per line, in command order (`HGETALL` alternates
    /// field/value). `SMEMBERS` order is not defined — [`Self::compare`]
    /// handles that, not this image.
    fn expected_readback_lines(&self) -> Vec<String> {
        match self {
            Self::String(value) => vec![value.clone()],
            Self::Hash(pairs) => pairs
                .iter()
                .flat_map(|(field, value)| [field.clone(), value.clone()])
                .collect(),
            Self::List(items) => items.clone(),
            Self::SortedSet(members) => members.clone(),
            Self::Set(members) => members.clone(),
        }
    }

    /// Compare a `--raw` readback against this payload. Line-oriented, like the
    /// `redis-cli` transport itself: an element containing a newline cannot be
    /// distinguished from two elements (a pre-existing transport limitation,
    /// #26 owns the transport). `Set` compares order-insensitively; everything
    /// else is order-exact.
    fn compare(&self, observed: &[u8]) -> Result<(), (Vec<String>, Vec<String>)> {
        let mut expected = self.expected_readback_lines();
        let observed_text = String::from_utf8_lossy(observed);
        let mut observed_lines: Vec<String> = if observed_text.is_empty() {
            Vec::new()
        } else {
            observed_text.split('\n').map(str::to_owned).collect()
        };
        if matches!(self, Self::Set(_)) {
            expected.sort();
            observed_lines.sort();
        }
        if expected == observed_lines {
            Ok(())
        } else {
            Err((expected, observed_lines))
        }
    }

    /// The command verb, for log/error messages.
    fn write_verb(&self) -> &'static str {
        match self {
            Self::String(_) => "SET",
            Self::Hash(_) => "HSET",
            Self::List(_) => "RPUSH",
            Self::SortedSet(_) => "ZADD",
            Self::Set(_) => "SADD",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RedisSeedImage {
    physical_key: String,
    payload: RedisSeedPayload,
    ttl_seconds: Option<i64>,
}

/// Materialize one typed redis seed and read it back. Best-effort: a failure
/// logs, is recorded on the certificate, and never fails the replay.
fn seed_redis_image(
    store: &StoreExec,
    image: &RedisSeedImage,
) -> (SeedMaterializationStatus, SeedReadback) {
    let write_args = image.payload.write_args(&image.physical_key);
    let write_refs: Vec<&str> = write_args.iter().map(String::as_str).collect();
    let mut cmd = store.redis_cli(&write_refs);
    eprintln!(
        "lifecycle: {} (redis key {} byte(s), write {}, ttl {:?})",
        store_exec::describe(&cmd),
        image.physical_key.len(),
        image.payload.write_verb(),
        image.ttl_seconds
    );
    match cmd.status() {
        Ok(status) if status.success() => (
            SeedMaterializationStatus::Materialized,
            readback_redis(store, image),
        ),
        Ok(status) => {
            let message = format!("seed_redis {} exited {status}", image.payload.write_verb());
            eprintln!("lifecycle: {message}; continuing (best-effort)");
            (
                SeedMaterializationStatus::Failed,
                SeedReadback::error(message),
            )
        }
        Err(e) => {
            let message = format!(
                "could not run seed_redis {}: {e}",
                image.payload.write_verb()
            );
            eprintln!("lifecycle: {message}; continuing (best-effort)");
            (
                SeedMaterializationStatus::Failed,
                SeedReadback::error(message),
            )
        }
    }
}

fn readback_redis(store: &StoreExec, image: &RedisSeedImage) -> SeedReadback {
    let key = image.physical_key.as_str();
    let expected_lines = image.payload.expected_readback_lines();
    let exists = match redis_cli_output(store, &["EXISTS", key]) {
        Ok(output) if output.status.success() => {
            String::from_utf8_lossy(&output.stdout).trim().to_owned()
        }
        Ok(output) => {
            return SeedReadback::error(format!(
                "redis EXISTS readback exited {}; stderr='{}'",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        Err(message) => return SeedReadback::error(message),
    };
    if exists != "1" {
        return SeedReadback::missing(
            serde_json::json!(expected_lines),
            format!(
                "redis EXISTS returned {exists:?} after {}",
                image.payload.write_verb()
            ),
        );
    }

    let read_args = image.payload.readback_args(key);
    let mut raw_args: Vec<&str> = vec!["--raw"];
    raw_args.extend(read_args.iter().map(String::as_str));
    let output = match redis_cli_output(store, &raw_args) {
        Ok(output) if output.status.success() => output,
        Ok(output) => {
            return SeedReadback::error(format!(
                "redis {} readback exited {}; stderr='{}'",
                read_args[0],
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        Err(message) => return SeedReadback::error(message),
    };
    let observed_bytes = strip_redis_cli_terminator(&output.stdout);
    match image.payload.compare(observed_bytes) {
        Ok(()) => SeedReadback::matched(
            serde_json::json!(expected_lines),
            serde_json::json!(String::from_utf8_lossy(observed_bytes)
                .split('\n')
                .collect::<Vec<_>>()),
        ),
        Err((expected, observed)) => SeedReadback::mismatched(
            serde_json::json!({
                "lines": expected,
                "len": expected.len(),
            }),
            serde_json::json!({
                "lines": observed,
                "len": observed.len(),
            }),
            format!(
                "redis {} returned a different value after {}",
                read_args[0],
                image.payload.write_verb()
            ),
        ),
    }
}

fn redis_cli_output(
    store: &StoreExec,
    redis_args: &[&str],
) -> Result<std::process::Output, String> {
    store
        .redis_cli(redis_args)
        .output()
        .map_err(|e| format!("could not run redis readback: {e}"))
}

fn strip_redis_cli_terminator(bytes: &[u8]) -> &[u8] {
    match bytes.split_last() {
        Some((last, rest)) if *last == b'\n' => rest,
        _ => bytes,
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
struct SeedCertificate {
    schema_version: u16,
    #[serde(rename = "type")]
    kind: String,
    recording_id: String,
    run_id: String,
    seed_db_enabled: bool,
    summary: SeedCertificateSummary,
    entries: Vec<SeedCertificateEntry>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct SeedCertificateSummary {
    planned: usize,
    materialized: usize,
    skipped: usize,
    failed: usize,
    unsupported: usize,
    /// Reads the planner concluded are not preconditions (read-after-write /
    /// self-created table); disjoint from `planned`. `#[serde(default)]` so
    /// certificates written before the field parse as zero.
    #[serde(default)]
    not_preconditions: usize,
    readback_matched: usize,
    readback_missing: usize,
    readback_mismatched: usize,
    readback_errors: usize,
    readback_not_run: usize,
    /// Which mechanism actually materialized each table's DB rows, and how many
    /// entries reached the seeder with no physical row image to use.
    ///
    /// Without this the two mechanisms are indistinguishable from outside the
    /// pod: a certificate showing `failed: 35` said nothing about whether the
    /// tape carried wire images, whether they were rejected, or which column
    /// refused to render — and the tape itself is deliberately not exposed
    /// through any API, so answering it meant reading pod logs (or a Kafka
    /// topic) that outlive nothing. The counts belong next to the outcome they
    /// explain.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    db_rows_by_table: BTreeMap<String, SeedTableMechanism>,
}

/// Per-table materialization accounting: `via_copy + via_insert` is every row
/// the seeder rendered for the table.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct SeedTableMechanism {
    /// Rows materialized from their verbatim physical wire image (binary COPY).
    via_copy: usize,
    /// Rows materialized through the serde INSERT renderer.
    via_insert: usize,
    /// Entries where every row seeded from its own physical image.
    entries_with_physical_image: usize,
    /// Entries where at least one row had no usable physical image — the
    /// population exposed to the fail-closed renderer. `with + without` is
    /// every entry the seeder rendered for the table.
    entries_without_physical_image: usize,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
struct SeedCertificateEntry {
    correlation_id: Option<String>,
    boundary: String,
    logical_key: String,
    physical_key: Option<String>,
    db_schema: Option<String>,
    origin: deja::SeedOrigin,
    materialization: SeedMaterializationStatus,
    readback: SeedReadback,
    /// Which mechanism materialized this DB entry's rows. Absent for redis
    /// entries and for entries the seeder never got as far as rendering.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    mechanism: Option<SeedEntryMechanism>,
}

/// How one DB seed entry's rows were materialized. A `Failed` entry carries
/// this too: the split is exactly what says whether the tape's physical image
/// was available and unused, or never captured in the first place.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct SeedEntryMechanism {
    table: String,
    rows: usize,
    /// Rows materialized from their verbatim physical wire image (binary COPY).
    via_copy: usize,
    /// Rows materialized through the serde INSERT renderer.
    via_insert: usize,
    /// Why the insert-path rows did not use a physical image — the tape carried
    /// none, or it carried one that is not portable across clusters. `None`
    /// when every row seeded from its wire image.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    physical_image_gap: Option<String>,
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum SeedMaterializationStatus {
    Materialized,
    Skipped,
    Failed,
    Unsupported,
    /// The planner saw the read and concluded it is not a precondition (a
    /// prior write in the correlation, or a table the correlation itself
    /// creates). Recorded so `planned + not_preconditions` accounts for every
    /// state-read the tape showed the planner — such a read used to leave no
    /// trace, making the under-count indistinguishable from a planner that
    /// never saw the read. Distinct from `Skipped` (planned, but the
    /// materializer had nothing to do): this was never planned at all.
    NotAPrecondition,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
struct SeedReadback {
    status: SeedReadbackStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expected: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    observed: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum SeedReadbackStatus {
    Matched,
    Missing,
    Mismatched,
    Error,
    NotRun,
    Unsupported,
}

impl SeedCertificate {
    const SCHEMA_VERSION: u16 = 1;
    const KIND: &'static str = "seed_certificate";

    fn new(recording_id: &str, run_id: &str, seed_db_enabled: bool) -> Self {
        Self {
            schema_version: Self::SCHEMA_VERSION,
            kind: Self::KIND.to_owned(),
            recording_id: recording_id.to_owned(),
            run_id: run_id.to_owned(),
            seed_db_enabled,
            summary: SeedCertificateSummary::default(),
            entries: Vec::new(),
        }
    }

    fn push(&mut self, entry: SeedCertificateEntry) {
        if let Some(mechanism) = &entry.mechanism {
            let table = self
                .summary
                .db_rows_by_table
                .entry(mechanism.table.clone())
                .or_default();
            table.via_copy += mechanism.via_copy;
            table.via_insert += mechanism.via_insert;
            if mechanism.physical_image_gap.is_some() {
                table.entries_without_physical_image += 1;
            } else {
                table.entries_with_physical_image += 1;
            }
        }
        // A not-a-precondition read was never planned: it has no
        // materialization outcome and no readback to account. It is carried
        // for the accounting identity (`planned + not_preconditions` = every
        // state-read shown to the planner), not as work.
        if matches!(
            entry.materialization,
            SeedMaterializationStatus::NotAPrecondition
        ) {
            self.summary.not_preconditions += 1;
            self.entries.push(entry);
            return;
        }
        self.summary.planned += 1;
        match entry.materialization {
            SeedMaterializationStatus::Materialized => self.summary.materialized += 1,
            SeedMaterializationStatus::Skipped => self.summary.skipped += 1,
            SeedMaterializationStatus::Failed => self.summary.failed += 1,
            SeedMaterializationStatus::Unsupported => self.summary.unsupported += 1,
            SeedMaterializationStatus::NotAPrecondition => unreachable!("handled above"),
        }
        match entry.readback.status {
            SeedReadbackStatus::Matched => self.summary.readback_matched += 1,
            SeedReadbackStatus::Missing => self.summary.readback_missing += 1,
            SeedReadbackStatus::Mismatched => self.summary.readback_mismatched += 1,
            SeedReadbackStatus::Error => self.summary.readback_errors += 1,
            SeedReadbackStatus::NotRun | SeedReadbackStatus::Unsupported => {
                self.summary.readback_not_run += 1;
            }
        }
        self.entries.push(entry);
    }
}

impl SeedCertificateEntry {
    fn new(
        correlation_id: &Option<String>,
        entry: &deja::SeedEntry,
        physical_key: Option<String>,
        db_schema: Option<String>,
        materialization: SeedMaterializationStatus,
        readback: SeedReadback,
    ) -> Self {
        Self {
            correlation_id: correlation_id.clone(),
            boundary: entry.boundary.clone(),
            logical_key: entry.key.clone(),
            physical_key,
            db_schema,
            origin: entry.origin,
            materialization,
            readback,
            mechanism: None,
        }
    }

    /// Attach the DB materialization split (see [`SeedEntryMechanism`]).
    fn with_mechanism(mut self, mechanism: Option<SeedEntryMechanism>) -> Self {
        self.mechanism = mechanism;
        self
    }

    /// A read the planner concluded is not a precondition. Carried in the
    /// certificate so the accounting closes; the readback names the rule.
    fn not_a_precondition(
        correlation_id: &Option<String>,
        boundary: &str,
        key: &str,
        reason: deja::NotPreconditionReason,
    ) -> Self {
        let why = match reason {
            deja::NotPreconditionReason::ReadAfterWrite => {
                "not a precondition: the read observes this correlation's own prior write of the key"
            }
            deja::NotPreconditionReason::SelfCreatedTable => {
                "not a precondition: this correlation creates the table's rows itself on replay"
            }
        };
        Self {
            correlation_id: correlation_id.clone(),
            boundary: boundary.to_owned(),
            logical_key: key.to_owned(),
            physical_key: None,
            db_schema: None,
            origin: deja::SeedOrigin::Recording,
            materialization: SeedMaterializationStatus::NotAPrecondition,
            readback: SeedReadback::not_run(why),
            mechanism: None,
        }
    }
}

impl SeedReadback {
    fn matched(expected: serde_json::Value, observed: serde_json::Value) -> Self {
        Self {
            status: SeedReadbackStatus::Matched,
            expected: Some(expected),
            observed: Some(observed),
            message: None,
        }
    }

    fn missing(expected: serde_json::Value, message: impl Into<String>) -> Self {
        Self {
            status: SeedReadbackStatus::Missing,
            expected: Some(expected),
            observed: None,
            message: Some(message.into()),
        }
    }

    fn mismatched(
        expected: serde_json::Value,
        observed: serde_json::Value,
        message: impl Into<String>,
    ) -> Self {
        Self {
            status: SeedReadbackStatus::Mismatched,
            expected: Some(expected),
            observed: Some(observed),
            message: Some(message.into()),
        }
    }

    fn error(message: impl Into<String>) -> Self {
        Self {
            status: SeedReadbackStatus::Error,
            expected: None,
            observed: None,
            message: Some(message.into()),
        }
    }

    fn not_run(message: impl Into<String>) -> Self {
        Self {
            status: SeedReadbackStatus::NotRun,
            expected: None,
            observed: None,
            message: Some(message.into()),
        }
    }

    fn unsupported(message: impl Into<String>) -> Self {
        Self {
            status: SeedReadbackStatus::Unsupported,
            expected: None,
            observed: None,
            message: Some(message.into()),
        }
    }
}

/// Build the total-derivative [`SeedPlan`](deja::SeedPlan) from the recording
/// and materialize its preconditions into the (just-flushed) replay store.
///
/// This GENERALIZES the old hand-coded `redis-cli SET settlement_rate_*` seeds:
/// instead of literal keys, the preconditions are DERIVED from the recording's
/// recorded RESULTS (one [`build_seed_plan`](deja::build_seed_plan) per
/// correlation, unioned across the tape), then merged with a static ambient
/// template (config keys a re-keyed/diverged read reaches for). The pure plan
/// logic lives in `deja-record` and is unit-tested without docker; this function
/// is the thin I/O wiring that walks the plan.
///
/// Two boundary arms: `redis` entries materialize via [`seed_redis`]; `db`
/// entries (seed-from-result-by-PK rows) materialize via [`seed_db`] into the
/// correlation's schema. Both are ON by default; `DEJA_SEED_DB=0` kill-switches
/// the db arm (falls back to the shared-pg self-rebuild).
///
/// Best-effort throughout: a missing/unparseable recording, an unmapped row, or
/// an unreachable store logs and continues rather than failing the replay
/// (matching the prior hand-coded seeds' best-effort behavior).
fn materialize_seed_plan(
    store: &StoreExec,
    recording: &crate::scope::ScopedRecording,
    run_id: &str,
) -> SeedCertificate {
    let recording_id = recording.recording_id();
    // Scoped at the READER, not after: the filter used to be re-applied here to
    // a Vec of every correlation in the session (~42,310 deduped ids compared
    // against the run's 3), and any future seeding path that forgot the
    // parameter would silently seed the whole session.
    let events = read_scoped_events(recording);
    // PER-CORRELATION ISOLATION (R1). Each request is an independent test case;
    // its preconditions are seeded into ITS OWN namespace, NOT a shared/unioned
    // store, so cases can't collide and read-modify-write can't double-apply —
    // which is what makes it safe to Execute stateful ops against the seeded
    // store. Redis keys get a `{correlation}:` prefix that mirrors the redis seam's
    // `add_prefix` during replay; db rows (when enabled) go into the correlation's
    // pg schema (the router sets `search_path` to that schema per connection). A
    // `None` correlation (uncorrelated event) seeds the bare key, matching the
    // seam returning `None` from `replay_key_namespace()`.
    let mut correlations: Vec<Option<String>> =
        events.iter().map(|e| e.correlation_id.clone()).collect();
    correlations.sort();
    correlations.dedup();
    // Test-case subset (R1): `recording` already carries the run's scope, so
    // these are exactly the correlations the kernel drives (`run_kernel`) and
    // scoring scopes to (`divergence`). Without it, seeding clones a full
    // public-schema per correlation for EVERY recorded correlation — including
    // the many single-event health checks that are never driven — which is
    // O(correlations x schema) and dominates runtime on a real recording.

    // DB isolation + seeding is ON by default (R1: real seeding). `DEJA_SEED_DB=0`
    // is a kill-switch that falls back to the old shared-pg self-rebuild. When on,
    // each correlation gets its own pg schema (full structural clone of public)
    // that the router routes to via `search_path`, and its seed rows land there.
    let seed_db_enabled = std::env::var("DEJA_SEED_DB")
        .ok()
        .map(|v| v.trim() != "0")
        .unwrap_or(true);

    let db_catalog = if seed_db_enabled {
        load_db_catalog(store)
    } else {
        DbCatalog::default()
    };

    let ambient = load_ambient_template();
    let mut certificate = SeedCertificate::new(recording_id, run_id, seed_db_enabled);
    for corr in &correlations {
        // One plan per case, merged with the static ambient/config template
        // (config keys the recording never observed, e.g. `settlement_rate_premium`,
        // that a diverged read reaches for — ambient never clobbers a
        // recording-derived precondition). Each case gets its own copy in its
        // namespace, since reads are isolated per correlation.
        let plan = deja::build_seed_plan(&events, corr.as_deref()).with_ambient(&ambient);
        // The per-correlation pg schema (DB isolation): same derivation the router
        // uses for `search_path`, so seeded rows land where replay reads them.
        let db_schema = corr.as_deref().map(deja::db_schema_for);

        // GRANULAR SCHEMA CLONING: clone ONLY the tables this correlation touches.
        // Read-precondition tables (where seed rows land) ∪ write-target tables
        // (writes must isolate here, not leak to the shared `public` base).
        // Untouched tables are NOT cloned — they resolve to the empty,
        // freshly-migrated `public` via `search_path` fallback as a correct miss.
        // This is O(touched) DDL per correlation, not O(all-tables). A correlation
        // with no db entries but with writes still gets its write-target tables
        // cloned (write isolation), so the plan.is_empty() skip below applies only
        // to SEED materialization, not to schema provisioning.
        if seed_db_enabled {
            if let Some(schema) = db_schema.as_deref() {
                let mut needed = plan.touched_db_tables();
                needed.extend(deja::build_write_target_tables(&events, corr.as_deref()));
                create_db_schema(store, schema, &needed);
            }
        }

        // Account for the reads the planner declined BEFORE the empty-plan
        // skip: a correlation can plan nothing and still have not-a-precondition
        // reads, and dropping them here would reopen the silent under-count.
        for (boundary, key, reason) in plan.non_precondition_reads() {
            certificate.push(SeedCertificateEntry::not_a_precondition(
                corr, boundary, key, reason,
            ));
        }

        if plan.is_empty() {
            continue;
        }
        let mut entries = plan.iter().collect::<Vec<_>>();
        // Chronology FIRST: a tape can carry two versions of one row under
        // different state keys (an early read's image, a post-update read's
        // image); one store holds one, and `ON CONFLICT DO NOTHING` keeps
        // whichever lands first. First must mean FIRST OBSERVED — key-string
        // order let a later version land before an earlier read's, and every
        // read of the early state diverged by the update's delta. The
        // row-before-query priority survives as the tiebreak WITHIN one
        // source event (its row-key and query-key entries share a sequence).
        entries.sort_by_key(|entry| (entry.source_sequence, seed_materialization_priority(entry)));
        for entry in entries {
            match entry.boundary.as_str() {
                // REDIS — render the recorded wire value to its typed write
                // (scalar → SET, Map → HSET, Array → RPUSH/ZADD, Set → SADD),
                // then write it under the per-correlation namespace. A shape
                // with no faithful write command is skipped LOUDLY (an explicit
                // certificate entry with the named reason), never seeded as
                // wrapper text.
                "redis" => {
                    let key = match corr {
                        Some(c) => format!("{c}:{}", entry.key),
                        None => entry.key.clone(),
                    };
                    let (materialization, readback) =
                        match render_redis_seed_payload(&entry.value, entry.method.as_deref()) {
                            RedisSeedRender::Payload(payload) => seed_redis_image(
                                store,
                                &RedisSeedImage {
                                    physical_key: key.clone(),
                                    payload,
                                    ttl_seconds: None,
                                },
                            ),
                            RedisSeedRender::Absent(reason) => (
                                SeedMaterializationStatus::Skipped,
                                SeedReadback::not_run(reason),
                            ),
                            RedisSeedRender::Unrepresentable(reason) => (
                                SeedMaterializationStatus::Skipped,
                                SeedReadback::not_run(reason),
                            ),
                        };
                    certificate.push(SeedCertificateEntry::new(
                        corr,
                        entry,
                        Some(key),
                        None,
                        materialization,
                        readback,
                    ));
                }
                // DB seed-from-result-by-PK, into the correlation's schema. ON by
                // default; DEJA_SEED_DB=0 is the kill-switch.
                "db" if seed_db_enabled => {
                    let outcome = seed_db(
                        store,
                        db_schema.as_deref(),
                        &db_catalog,
                        &entry.key,
                        entry.image.as_ref(),
                        &entry.value,
                    );
                    certificate.push(
                        SeedCertificateEntry::new(
                            corr,
                            entry,
                            None,
                            db_schema.clone(),
                            outcome.status,
                            outcome.readback,
                        )
                        .with_mechanism(outcome.mechanism),
                    );
                }
                "db" => certificate.push(SeedCertificateEntry::new(
                    corr,
                    entry,
                    None,
                    db_schema.clone(),
                    SeedMaterializationStatus::Skipped,
                    SeedReadback::not_run("db seeding disabled by DEJA_SEED_DB=0"),
                )),
                _ => certificate.push(SeedCertificateEntry::new(
                    corr,
                    entry,
                    None,
                    None,
                    SeedMaterializationStatus::Unsupported,
                    SeedReadback::unsupported(
                        "seed materialization only supports redis and db boundaries",
                    ),
                )),
            }
        }
    }
    eprintln!(
        "lifecycle: materialized {} of {} seed preconditions across {} correlation(s) for recording {recording_id}; readback matched {}, missing {}, mismatched {}, errored {}",
        certificate.summary.materialized,
        certificate.summary.planned,
        correlations.len(),
        certificate.summary.readback_matched,
        certificate.summary.readback_missing,
        certificate.summary.readback_mismatched,
        certificate.summary.readback_errors
    );
    for (table, mechanism) in &certificate.summary.db_rows_by_table {
        eprintln!(
            "lifecycle: seed mechanism {table}: {} row(s) via binary COPY, {} via insert; {} entr(ies) had a physical image, {} did not",
            mechanism.via_copy,
            mechanism.via_insert,
            mechanism.entries_with_physical_image,
            mechanism.entries_without_physical_image
        );
    }
    certificate
}

fn seed_materialization_priority(entry: &deja::SeedEntry) -> u8 {
    if entry.boundary != "db" {
        return 0;
    }
    match deja::StateKey::parse(&entry.key) {
        Ok(deja::StateKey::DbRow { .. }) => 0,
        Ok(deja::StateKey::DbQuery { .. }) => 1,
        _ => 2,
    }
}

/// Seed the row(s) a recorded `boundary="db"` READ returned, into the
/// correlation's schema — so that read reproduces against the isolated store.
///
/// The table comes from typed v1 [`deja::StateKey`] state keys. Opaque/legacy DB
/// keys are intentionally skipped instead of being parsed with string splits:
/// lookup identity and state identity are separate, and DB key grammar belongs
/// to the typed API. The value may be either a typed row payload (new row-key
/// path) or the legacy database-result envelope (query fallback). Row-key seeds
/// filter a multi-row envelope down to the keyed row before rendering; query
/// fallback seeds materialize the full returned row set once.
///
/// Best-effort: a malformed row / unreachable pg logs + continues, NEVER fails the
/// replay.
// Harness-internal call with one call path; slated for the runner extraction,
// where the shared (demo, ids, schema, catalog) context becomes a struct.
#[allow(clippy::too_many_arguments)]
fn seed_db(
    store: &StoreExec,
    schema: Option<&str>,
    catalog: &DbCatalog,
    key: &str,
    image: Option<&serde_json::Value>,
    envelope: &serde_json::Value,
) -> SeedDbOutcome {
    let target = match db_seed_target_from_key(key) {
        Some(target) => target,
        None => {
            return SeedDbOutcome::unrendered(
                SeedMaterializationStatus::Unsupported,
                SeedReadback::unsupported("unsupported or opaque db state key"),
            );
        }
    };
    let rows = image
        .and_then(|image| db_row_images_from_typed_payload(&target.table, image, catalog))
        .unwrap_or_else(|| {
            db_seed_value(envelope)
                .map(|value| target.filter_rows(db_row_images(&target.table, &value, catalog)))
                .unwrap_or_default()
        });
    if rows.is_empty() {
        let message = format!(
            "seed_db {} key {} carried no seedable row payload; skipping",
            target.kind, key
        );
        eprintln!("lifecycle: {message}");
        return SeedDbOutcome::unrendered(
            SeedMaterializationStatus::Skipped,
            SeedReadback::not_run(message),
        );
    }

    // Split rows by seeding mechanism: a row carrying a COPY-eligible
    // physical wire image seeds through binary COPY (the store parses its own
    // `typsend` output — no type-shape knowledge here); everything else
    // renders through the serde INSERT path exactly as before, fail-closed
    // arm and all. Old tapes carry no wire images, so they take the insert
    // path byte-identically.
    let mut copy_rows: Vec<&DbRowImage> = Vec::new();
    let mut insert_rows: Vec<&DbRowImage> = Vec::new();
    let mut physical_image_gap: Option<String> = None;
    for row in &rows {
        match wire_copy_eligibility(row) {
            Ok(()) => copy_rows.push(row),
            Err(reason) => {
                if row.wire.is_some() {
                    eprintln!(
                        "lifecycle: seed_db {} {}: physical image not COPY-eligible ({reason}); using the serde insert path for this row",
                        target.kind, target.table
                    );
                }
                if physical_image_gap.is_none() {
                    physical_image_gap = Some(reason);
                }
                insert_rows.push(row);
            }
        }
    }
    let mechanism = SeedEntryMechanism {
        table: target.table.clone(),
        rows: rows.len(),
        via_copy: copy_rows.len(),
        via_insert: insert_rows.len(),
        physical_image_gap,
    };

    // Log the split BEFORE rendering: a row the renderer refuses is exactly
    // when knowing whether a physical image existed matters most.
    eprintln!(
        "lifecycle: seed_db {} {} ({} row(s): {} via binary COPY, {} via insert{})",
        target.kind,
        target.table,
        mechanism.rows,
        mechanism.via_copy,
        mechanism.via_insert,
        match &mechanism.physical_image_gap {
            Some(reason) => format!(" — no physical image used: {reason}"),
            None => String::new(),
        }
    );
    if seed_contains_null_column(&rows, "totp_secret") {
        eprintln!(
            "lifecycle: seed_db {} {} NULL columns: totp_secret=NULL",
            target.kind, target.table
        );
    }

    let mut sql = String::new();
    for row in &insert_rows {
        let stmt = match build_insert_sql(schema, row) {
            Ok(stmt) => stmt,
            Err(reason) => {
                let message = format!(
                    "seed_db {} {} could not render an insert for a seedable row: {reason}",
                    target.kind, target.table
                );
                eprintln!("lifecycle: {message}; skipping this seed entry");
                return SeedDbOutcome::rendered(
                    SeedMaterializationStatus::Failed,
                    SeedReadback::error(message),
                    mechanism,
                );
            }
        };
        sql.push_str(&stmt);
        sql.push('\n');
    }

    if !copy_rows.is_empty() {
        if let Err(message) = seed_db_wire_copy(store, schema, &target.table, &copy_rows) {
            eprintln!("lifecycle: {message}; continuing (best-effort)");
            return SeedDbOutcome::rendered(
                SeedMaterializationStatus::Failed,
                SeedReadback::error(message),
                mechanism,
            );
        }
    }

    if sql.is_empty() {
        return SeedDbOutcome::rendered(
            SeedMaterializationStatus::Materialized,
            readback_db(store, schema, &target, &rows),
            mechanism,
        );
    }
    match store.psql(&[], true, &sql).output() {
        Ok(output) if output.status.success() => SeedDbOutcome::rendered(
            SeedMaterializationStatus::Materialized,
            readback_db(store, schema, &target, &rows),
            mechanism,
        ),
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            let message = format!(
                "seed_db {} exited {}; stderr='{}' stdout='{}'",
                target.table,
                output.status,
                stderr.trim(),
                stdout.trim()
            );
            eprintln!("lifecycle: {message}; continuing (best-effort)");
            SeedDbOutcome::rendered(
                SeedMaterializationStatus::Failed,
                SeedReadback::error(message),
                mechanism,
            )
        }
        Err(e) => {
            let message = format!("could not run seed_db {}: {e}", target.table);
            eprintln!("lifecycle: {message}; continuing (best-effort)");
            SeedDbOutcome::rendered(
                SeedMaterializationStatus::Failed,
                SeedReadback::error(message),
                mechanism,
            )
        }
    }
}

/// What one [`seed_db`] call concluded: the outcome, its readback, and — once
/// rows were actually rendered — which mechanism rendered them.
struct SeedDbOutcome {
    status: SeedMaterializationStatus,
    readback: SeedReadback,
    mechanism: Option<SeedEntryMechanism>,
}

impl SeedDbOutcome {
    /// An outcome reached before any row was rendered (an opaque key, or a
    /// payload with no rows in it).
    fn unrendered(status: SeedMaterializationStatus, readback: SeedReadback) -> Self {
        Self {
            status,
            readback,
            mechanism: None,
        }
    }

    fn rendered(
        status: SeedMaterializationStatus,
        readback: SeedReadback,
        mechanism: SeedEntryMechanism,
    ) -> Self {
        Self {
            status,
            readback,
            mechanism: Some(mechanism),
        }
    }
}

/// Materialize rows that carry COPY-eligible physical wire images: stream the
/// captured `typsend` bytes back through `COPY ... WITH (FORMAT binary)`, so
/// the only parser involved is the paired `typreceive` of the store itself
/// (docs/design/wire-faithful-seeding.md, the binary-format decision).
///
/// Mechanics: rows are grouped by column list; each group COPYs into a
/// session-temporary staging table (`CREATE TEMP TABLE ... AS SELECT cols ...
/// WITH NO DATA` — exact column types, none of the target's NOT NULL/identity
/// baggage) and lands with `INSERT ... SELECT ... ON CONFLICT DO NOTHING`,
/// preserving the insert path's duplicate tolerance (a bare COPY into the
/// target would abort the whole batch on one duplicate key). The stream
/// travels on psql's stdin (`\copy ... from pstdin`), which both StoreExec
/// transports forward; the three commands share one psql session so the temp
/// table survives across them.
fn seed_db_wire_copy(
    store: &StoreExec,
    schema: Option<&str>,
    table: &str,
    rows: &[&DbRowImage],
) -> Result<(), String> {
    let mut groups: Vec<(Vec<&str>, Vec<&DbRowImage>)> = Vec::new();
    for row in rows {
        let names: Vec<&str> = row
            .columns
            .iter()
            .map(|column| column.metadata.name.as_str())
            .collect();
        match groups.iter_mut().find(|(group, _)| *group == names) {
            Some((_, group_rows)) => group_rows.push(row),
            None => groups.push((names, vec![row])),
        }
    }
    let qualified = qualified_table(schema, table);
    for (names, group_rows) in groups {
        let stream = build_binary_copy_stream(&group_rows)?;
        let commands = wire_copy_commands(&qualified, &names);
        run_psql_script_with_stdin(store, &commands, &stream)
            .map_err(|e| format!("seed_db binary copy into {qualified} failed: {e}"))?;
    }
    Ok(())
}

/// The three-command psql session that lands one binary COPY group: stage
/// into a session-temp table with the target's exact column types (`CREATE
/// TEMP TABLE ... AS SELECT ... WITH NO DATA` copies types but none of the
/// target's NOT NULL/identity constraints), stream the captured bytes into it
/// (`\copy ... from pstdin`), then land with the insert path's duplicate
/// tolerance. Pure — unit-tested.
fn wire_copy_commands(qualified_table: &str, column_names: &[&str]) -> Vec<String> {
    let column_list = column_names
        .iter()
        .map(|name| quote_ident(name))
        .collect::<Vec<_>>()
        .join(", ");
    let staging = "_deja_wire_seed";
    vec![
        format!(
            "CREATE TEMP TABLE {staging} AS SELECT {column_list} FROM {qualified_table} WITH NO DATA"
        ),
        format!("\\copy {staging} from pstdin with (format binary)"),
        format!(
            "INSERT INTO {qualified_table} ({column_list}) SELECT {column_list} FROM {staging} ON CONFLICT DO NOTHING"
        ),
    ]
}

/// Construct a binary COPY data stream from captured wire images: the fixed
/// 19-byte header (`PGCOPY\n\xff\r\n\0` signature + int32 flags + int32
/// extension length), then per row an int16 field count and per field an
/// int32 byte length (`-1` for NULL) followed by the verbatim captured bytes,
/// closed by the int16 `-1` trailer. Pure — golden-tested against the
/// documented format.
fn build_binary_copy_stream(rows: &[&DbRowImage]) -> Result<Vec<u8>, String> {
    const SIGNATURE: &[u8] = b"PGCOPY\n\xff\r\n\0";
    let mut out = Vec::new();
    out.extend_from_slice(SIGNATURE);
    out.extend_from_slice(&0_i32.to_be_bytes()); // flags
    out.extend_from_slice(&0_i32.to_be_bytes()); // header extension length
    for row in rows {
        let wire = row.wire.as_ref().ok_or_else(|| {
            "binary copy stream requested for a row without a wire image".to_string()
        })?;
        let field_count = i16::try_from(row.columns.len())
            .map_err(|_| format!("row has too many columns ({})", row.columns.len()))?;
        out.extend_from_slice(&field_count.to_be_bytes());
        for column in &row.columns {
            let name = &column.metadata.name;
            let bytes = wire
                .columns
                .get(name)
                .ok_or_else(|| format!("wire image missing column {name}"))?;
            match bytes {
                None => out.extend_from_slice(&(-1_i32).to_be_bytes()),
                Some(bytes) => {
                    let length = i32::try_from(bytes.len())
                        .map_err(|_| format!("wire value for column {name} is too large"))?;
                    out.extend_from_slice(&length.to_be_bytes());
                    out.extend_from_slice(bytes);
                }
            }
        }
    }
    out.extend_from_slice(&(-1_i16).to_be_bytes());
    Ok(out)
}

/// Run a multi-command psql session with the given bytes on stdin. The stream
/// is staged in a temp file so the child reads a real file descriptor (no
/// pipe-writer bookkeeping); the file is removed afterwards, best-effort.
fn run_psql_script_with_stdin(
    store: &StoreExec,
    commands: &[String],
    stdin_bytes: &[u8],
) -> Result<(), String> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEED_STREAM_SEQ: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "deja-wire-seed-{}-{}.pgcopy",
        std::process::id(),
        SEED_STREAM_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::write(&path, stdin_bytes)
        .map_err(|e| format!("could not stage the copy stream at {}: {e}", path.display()))?;
    let result = (|| {
        let file = std::fs::File::open(&path)
            .map_err(|e| format!("could not reopen the staged copy stream: {e}"))?;
        let mut cmd = store.psql_script(true, commands);
        cmd.stdin(std::process::Stdio::from(file));
        let output = cmd
            .output()
            .map_err(|e| format!("could not run psql: {e}"))?;
        if output.status.success() {
            Ok(())
        } else {
            Err(format!(
                "psql exited {}; stderr='{}' stdout='{}'",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim(),
                String::from_utf8_lossy(&output.stdout).trim()
            ))
        }
    })();
    let _ = std::fs::remove_file(&path);
    result
}
/// Outcome of one wire-faithful row readback.
enum WireReadback {
    Matched,
    Missing,
    Mismatched { column: String },
}

/// The pragmatic-PK predicate for reading one seeded row back, when the row
/// image carries a known primary-key column. PK values are plain identifier
/// strings — the one column class whose serde rendering IS faithful, which is
/// why row-state keys exist at all.
fn wire_readback_row_predicate(row: &DbRowImage) -> Option<Vec<(String, String)>> {
    let object: serde_json::Map<String, serde_json::Value> = row
        .columns
        .iter()
        .map(|column| (column.metadata.name.clone(), column.value.clone()))
        .collect();
    match deja::db::row_state_key(&row.table, &serde_json::Value::Object(object)) {
        Some(deja::StateKey::DbRow { key, .. }) => Some(key),
        _ => None,
    }
}

/// Render the COPY-OUT command that reads one seeded row back in binary —
/// the exact inverse of the seed path, so the only codec involved is the
/// store's own `typsend`.
fn wire_copy_out_command(
    schema: Option<&str>,
    table: &str,
    column_names: &[&str],
    key: &[(String, String)],
) -> String {
    let column_list = column_names
        .iter()
        .map(|name| quote_ident(name))
        .collect::<Vec<_>>()
        .join(", ");
    let qualified = qualified_table(schema, table);
    // Every key column, ANDed: a composite key addresses one row only when all
    // of its columns are constrained.
    let predicate = key
        .iter()
        .map(|(column, value)| {
            format!(
                "{} = {}",
                quote_ident(column),
                sql_literal(&serde_json::Value::String(value.clone()))
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    format!(
        "\\copy (SELECT {column_list} FROM {qualified} WHERE {predicate}) TO PSTDOUT WITH (FORMAT binary)"
    )
}

/// Parse a binary COPY stream back into per-row, per-field bytes — the
/// inverse of [`build_binary_copy_stream`], golden-tested against it.
fn parse_binary_copy_stream(buf: &[u8]) -> Result<Vec<Vec<Option<Vec<u8>>>>, String> {
    const SIGNATURE: &[u8] = b"PGCOPY\n\xff\r\n\0";
    let mut at = 0usize;
    let take = |at: &mut usize, n: usize| -> Result<&[u8], String> {
        let slice = buf
            .get(*at..*at + n)
            .ok_or_else(|| format!("copy stream truncated at byte {at}"))?;
        *at += n;
        Ok(slice)
    };
    if take(&mut at, SIGNATURE.len())? != SIGNATURE {
        return Err("copy stream does not start with the PGCOPY signature".to_string());
    }
    take(&mut at, 4)?; // flags
    let ext_len = i32::from_be_bytes(take(&mut at, 4)?.try_into().expect("4 bytes"));
    take(&mut at, usize::try_from(ext_len).unwrap_or(0))?;
    let mut rows = Vec::new();
    loop {
        let field_count = i16::from_be_bytes(take(&mut at, 2)?.try_into().expect("2 bytes"));
        if field_count == -1 {
            return Ok(rows); // trailer
        }
        let mut row = Vec::with_capacity(field_count.max(0) as usize);
        for _ in 0..field_count {
            let len = i32::from_be_bytes(take(&mut at, 4)?.try_into().expect("4 bytes"));
            if len == -1 {
                row.push(None);
            } else {
                row.push(Some(
                    take(
                        &mut at,
                        usize::try_from(len).map_err(|_| "negative field length")?,
                    )?
                    .to_vec(),
                ));
            }
        }
        rows.push(row);
    }
}

/// Verify one wire-seeded row through the seed's exact inverse: COPY it back
/// OUT in binary and byte-compare against the captured `typsend` bytes.
fn readback_db_wire_row(
    store: &StoreExec,
    schema: Option<&str>,
    row: &DbRowImage,
) -> Result<WireReadback, String> {
    let wire = row
        .wire
        .as_ref()
        .ok_or("wire readback requested for a row without a physical image")?;
    let key = wire_readback_row_predicate(row)
        .ok_or("wire readback requested for a row whose schema key is not fully present")?;
    let names: Vec<&str> = row
        .columns
        .iter()
        .map(|column| column.metadata.name.as_str())
        .collect();
    let command = wire_copy_out_command(schema, &row.table, &names, &key);
    let output = store
        .psql(&["-q"], true, &command)
        .output()
        .map_err(|e| format!("could not run wire readback: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "wire readback exited {}; stderr='{}'",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let observed_rows = parse_binary_copy_stream(&output.stdout)?;
    let Some(observed) = observed_rows.first() else {
        return Ok(WireReadback::Missing);
    };
    for (index, name) in names.iter().enumerate() {
        let expected_bytes = wire.columns.get(*name).cloned().flatten();
        let observed_bytes = observed.get(index).cloned().flatten();
        if expected_bytes != observed_bytes {
            return Ok(WireReadback::Mismatched {
                column: (*name).to_owned(),
            });
        }
    }
    Ok(WireReadback::Matched)
}

fn readback_db(
    store: &StoreExec,
    schema: Option<&str>,
    target: &DbSeedTarget,
    rows: &[DbRowImage],
) -> SeedReadback {
    // Wire-seeded rows verify through the seed's exact inverse — binary COPY
    // OUT, byte-compared against the captured `typsend` bytes. The serde path
    // below renders recorded values as SQL literals and compares those; a
    // serde shape that cannot faithfully render is the REASON wire seeding
    // exists, so it can never satisfy a literal-equality predicate — which is
    // how correctly-seeded rows scored as 365 missing/mismatched readback
    // entries (the codec-mismatch theorem, applied to verification).
    let mut wire_rows: Vec<&DbRowImage> = Vec::new();
    let mut serde_rows: Vec<&DbRowImage> = Vec::new();
    for row in rows {
        if wire_copy_eligibility(row).is_ok() && wire_readback_row_predicate(row).is_some() {
            wire_rows.push(row);
        } else {
            serde_rows.push(row);
        }
    }
    let expected = serde_json::json!({
        "rows": rows.len(),
        "table": target.table,
        "kind": target.kind,
    });
    for row in &wire_rows {
        match readback_db_wire_row(store, schema, row) {
            Ok(WireReadback::Matched) => {}
            Ok(WireReadback::Missing) => {
                return SeedReadback::missing(
                    expected,
                    "db seed readback found no row at the seeded primary key",
                );
            }
            Ok(WireReadback::Mismatched { column }) => {
                return SeedReadback::mismatched(
                    expected,
                    serde_json::json!({"wire_mismatched_column": column, "table": row.table}),
                    format!(
                        "db row exists by key after seed, but column {column} differs from the captured wire bytes"
                    ),
                );
            }
            Err(message) => return SeedReadback::error(message),
        }
    }
    if serde_rows.is_empty() {
        return SeedReadback::matched(
            expected,
            serde_json::json!({"wire_row_matches": wire_rows.len()}),
        );
    }

    let mut full_sql = String::new();
    for row in &serde_rows {
        let Some(stmt) = build_count_sql(schema, row, None) else {
            return SeedReadback::error("cannot render db readback full-row predicate");
        };
        full_sql.push_str(&stmt);
        full_sql.push('\n');
    }
    let full_counts = match run_db_readback_counts(store, &full_sql, serde_rows.len()) {
        Ok(counts) => counts,
        Err(message) => return SeedReadback::error(message),
    };
    let mut observed = serde_json::json!({
        "wire_row_matches": wire_rows.len(),
        "full_row_matches": full_counts.clone(),
    });
    if full_counts.iter().all(|count| *count > 0) {
        return SeedReadback::matched(expected, observed);
    }

    if let Some(filter) = &target.row_filter {
        let mut key_sql = String::new();
        for row in &serde_rows {
            let Some(stmt) = build_count_sql(schema, row, Some(filter)) else {
                return SeedReadback::error("cannot render db readback key predicate");
            };
            key_sql.push_str(&stmt);
            key_sql.push('\n');
        }
        let key_counts = match run_db_readback_counts(store, &key_sql, serde_rows.len()) {
            Ok(counts) => counts,
            Err(message) => return SeedReadback::error(message),
        };
        if let Some(map) = observed.as_object_mut() {
            map.insert(
                "key_matches".to_owned(),
                serde_json::json!(key_counts.clone()),
            );
        }
        if key_counts.iter().any(|count| *count > 0) {
            return SeedReadback::mismatched(
                expected,
                observed,
                "db row exists by key after seed, but at least one column differs from the seed image",
            );
        }
    }

    SeedReadback::missing(
        expected,
        "db seed readback found no row matching the materialized seed image",
    )
}

fn run_db_readback_counts(
    store: &StoreExec,
    sql: &str,
    expected_lines: usize,
) -> Result<Vec<u64>, String> {
    let output = store
        .psql(&["-A", "-t"], true, sql)
        .output()
        .map_err(|e| format!("could not run db seed readback: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "db seed readback exited {}; stderr='{}'",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let counts = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            line.trim()
                .parse::<u64>()
                .map_err(|e| format!("db seed readback count '{line}' was not numeric: {e}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if counts.len() != expected_lines {
        return Err(format!(
            "db seed readback returned {} count line(s), expected {expected_lines}",
            counts.len()
        ));
    }
    Ok(counts)
}

fn build_count_sql(
    schema: Option<&str>,
    row: &DbRowImage,
    filter: Option<&DbRowFilter>,
) -> Option<String> {
    let qualified_table = qualified_table(schema, &row.table);
    let predicates = match filter {
        Some(filter) => vec![db_filter_predicate(row, filter)?],
        None => {
            let mut predicates = Vec::with_capacity(row.columns.len());
            for column in &row.columns {
                match db_comparison_predicate(&column.metadata.name, column) {
                    Some(predicate) => predicates.push(predicate),
                    // A wire-seeded column the renderer cannot express (the
                    // exact case binary-COPY seeding exists for, e.g. the
                    // tagged-enum-in-varchar poison value) is excluded from
                    // the full-row readback predicate: presence is verified
                    // through the remaining columns and the key predicate.
                    // Rows without a wire image keep the old all-or-nothing
                    // behavior.
                    None if row
                        .wire
                        .as_ref()
                        .is_some_and(|wire| wire.columns.contains_key(&column.metadata.name)) =>
                    {
                        continue;
                    }
                    None => return None,
                }
            }
            if predicates.is_empty() {
                return None;
            }
            predicates
        }
    };
    Some(format!(
        "SELECT COUNT(*) FROM {qualified_table} WHERE {};",
        predicates.join(" AND ")
    ))
}

fn db_filter_predicate(row: &DbRowImage, filter: &DbRowFilter) -> Option<String> {
    let mut predicates = Vec::with_capacity(filter.key.len());
    for (key_column, key_value) in &filter.key {
        // Prefer the row's own typed column: its catalog metadata renders the
        // comparison correctly (a json column compares as jsonb, and so on).
        // Absent that, fall back to the key's value as an untyped literal.
        let predicate = match row
            .columns
            .iter()
            .find(|column| &column.metadata.name == key_column)
        {
            Some(column) => db_comparison_predicate(&column.metadata.name, column)?,
            None => {
                let column = DbColumnImage {
                    metadata: DbColumnMetadata::unknown(key_column),
                    value: serde_json::Value::String(key_value.clone()),
                };
                db_comparison_predicate(key_column, &column)?
            }
        };
        predicates.push(predicate);
    }
    (!predicates.is_empty()).then(|| predicates.join(" AND "))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DbSeedTarget {
    table: String,
    kind: &'static str,
    row_filter: Option<DbRowFilter>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DbRowFilter {
    /// The schema key's columns and values, in key order. All of them must
    /// match: a composite key identifies a row only as a whole.
    key: Vec<(String, String)>,
}

impl DbSeedTarget {
    fn filter_rows(&self, rows: Vec<DbRowImage>) -> Vec<DbRowImage> {
        let Some(filter) = &self.row_filter else {
            return rows;
        };
        rows.into_iter()
            .filter(|row| db_row_matches_filter(row, filter))
            .collect()
    }
}

fn db_seed_target_from_key(key: &str) -> Option<DbSeedTarget> {
    let state_key = match deja::StateKey::parse(key) {
        Ok(state_key) => state_key,
        Err(err) => {
            eprintln!("lifecycle: seed_db: opaque/unknown db state key '{key}': {err}; skipping");
            return None;
        }
    };
    let Some(table) = state_key.db_table().map(str::to_owned) else {
        eprintln!(
            "lifecycle: seed_db: typed state key '{}' has no db table; skipping",
            state_key.to_wire()
        );
        return None;
    };
    match &state_key {
        deja::StateKey::DbRow { key, .. } => Some(DbSeedTarget {
            table,
            kind: "row",
            row_filter: Some(DbRowFilter { key: key.clone() }),
        }),
        deja::StateKey::DbQuery { .. } => Some(DbSeedTarget {
            table,
            kind: "query-fallback",
            row_filter: None,
        }),
        _ => {
            eprintln!(
                "lifecycle: seed_db: typed state key '{}' is not a db row/query key; skipping",
                state_key.to_wire()
            );
            None
        }
    }
}

fn db_row_matches_filter(row: &DbRowImage, filter: &DbRowFilter) -> bool {
    filter.key.iter().all(|(key_column, key_value)| {
        row.columns.iter().any(|column| {
            &column.metadata.name == key_column && &db_seed_wire_value(&column.value) == key_value
        })
    })
}

fn db_seed_wire_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(value) => value.clone(),
        _ => serde_json::to_string(value).unwrap_or_else(|_| "null".to_owned()),
    }
}

fn db_seed_value(envelope: &serde_json::Value) -> Option<serde_json::Value> {
    use deja::value::{DejaDatabaseResult, DejaDatabaseResultPayload};

    match serde_json::from_value::<DejaDatabaseResult>(envelope.clone()) {
        Ok(DejaDatabaseResult {
            payload: DejaDatabaseResultPayload::Ok { value, .. },
            ..
        }) => Some(value),
        Ok(DejaDatabaseResult {
            payload: DejaDatabaseResultPayload::Err { .. },
            ..
        }) => None,
        Err(_) => Some(envelope.clone()),
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct DbCatalog {
    columns_by_table: BTreeMap<String, BTreeMap<String, DbColumnMetadata>>,
}

impl DbCatalog {
    fn insert(&mut self, table: String, column: DbColumnMetadata) {
        self.columns_by_table
            .entry(table)
            .or_default()
            .insert(column.name.clone(), column);
    }

    fn metadata_for(&self, table: &str, column: &str) -> DbColumnMetadata {
        self.columns_by_table
            .get(table)
            .and_then(|cols| cols.get(column))
            .cloned()
            .unwrap_or_else(|| DbColumnMetadata::unknown(column))
    }

    fn column_count(&self) -> usize {
        self.columns_by_table.values().map(BTreeMap::len).sum()
    }
}

/// Teach the replay side which columns identify a row, by asking the database
/// that holds the schema.
///
/// Row identity is a fact about the SCHEMA, so it is read from the schema.
/// Both halves of deja used to consult a hardcoded `table → column` map — the
/// tables of one particular application listed inside a generic engine, and a
/// shape that cannot describe a composite key at all. The statement lives in
/// the runtime (`TABLE_IDENTITY_SQL`) so every populator asks the same
/// question; this one runs it against the replay store.
fn register_table_identity(store: &StoreExec) {
    let mut command = store.psql(&["-A", "-t", "-F", "\t"], false, deja::TABLE_IDENTITY_SQL);
    let Ok(output) = command.output() else {
        eprintln!(
            "lifecycle: could not read row identity from the schema; row keys will fall back to query fingerprints"
        );
        return;
    };
    if !output.status.success() {
        eprintln!(
            "lifecycle: row identity read exited {}; row keys will fall back to query fingerprints",
            output.status
        );
        return;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut by_table: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for line in stdout.lines().filter(|line| !line.trim().is_empty()) {
        let mut parts = line.split('\t');
        let (Some(table), Some(column)) = (parts.next(), parts.next()) else {
            continue;
        };
        by_table
            .entry(table.to_owned())
            .or_default()
            .push(column.to_owned());
    }
    let composite = by_table
        .values()
        .filter(|columns| columns.len() > 1)
        .count();
    eprintln!(
        "lifecycle: row identity read from the schema: {} table(s), {composite} with composite keys",
        by_table.len()
    );
    deja::register_table_identity(by_table);
}

fn load_db_catalog(store: &StoreExec) -> DbCatalog {
    let sql =
        "SELECT cls.relname, attr.attname, typ.oid::int4, typ.typname, (NOT attr.attnotnull) \
               FROM pg_catalog.pg_attribute attr \
               JOIN pg_catalog.pg_class cls ON cls.oid = attr.attrelid \
               JOIN pg_catalog.pg_namespace ns ON ns.oid = cls.relnamespace \
               JOIN pg_catalog.pg_type typ ON typ.oid = attr.atttypid \
               WHERE ns.nspname = 'public' \
                 AND attr.attnum > 0 \
                 AND NOT attr.attisdropped \
                 AND cls.relkind IN ('r', 'p') \
               ORDER BY cls.relname, attr.attnum";
    match store.psql(&["-A", "-t", "-F", "\t"], false, sql).output() {
        Ok(output) if output.status.success() => {
            let mut catalog = DbCatalog::default();
            let stdout = String::from_utf8_lossy(&output.stdout);
            for line in stdout.lines().filter(|line| !line.trim().is_empty()) {
                let parts: Vec<&str> = line.split('\t').collect();
                if parts.len() != 5 {
                    eprintln!("lifecycle: skipping malformed db catalog row '{line}'");
                    continue;
                }
                catalog.insert(
                    parts[0].to_string(),
                    DbColumnMetadata {
                        name: parts[1].to_string(),
                        type_oid: parts[2].parse().ok(),
                        type_name: nonempty(parts[3]),
                        nullable: parse_pg_bool(parts[4]),
                    },
                );
            }
            eprintln!(
                "lifecycle: loaded db catalog metadata for {} table(s), {} column(s)",
                catalog.columns_by_table.len(),
                catalog.column_count()
            );
            if catalog.columns_by_table.is_empty() {
                // psql succeeded and returned nothing: the query ran against a
                // database whose `public` schema has no tables. Seeding will then
                // render every column untyped and quietly mis-materialize the
                // typed ones, so say so here rather than let it look like a row
                // problem later.
                eprintln!(
                    "lifecycle: db catalog is EMPTY — every seeded column will be rendered without type information"
                );
            }
            register_table_identity(store);
            catalog
        }
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            eprintln!(
                "lifecycle: db catalog load exited {}; using unknown column metadata fallback: {}",
                output.status,
                stderr.trim()
            );
            DbCatalog::default()
        }
        Err(e) => {
            eprintln!(
                "lifecycle: could not load db catalog metadata: {e}; using unknown column metadata fallback"
            );
            DbCatalog::default()
        }
    }
}

fn nonempty(value: &str) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

fn parse_pg_bool(value: &str) -> Option<bool> {
    match value {
        "t" | "true" | "TRUE" => Some(true),
        "f" | "false" | "FALSE" => Some(false),
        _ => None,
    }
}

/// Create the per-correlation isolation schema (R1) and clone ONLY the tables
/// the correlation actually touches — its read-precondition tables (rows the
/// seed materializes) and its write-target tables (writes must isolate here, not
/// leak to the shared `public` base). Untouched tables are NOT cloned: they
/// resolve to the (empty, freshly-migrated) `public` via `search_path` fallback
/// as a correct miss, so cloning them is pure waste. `tables` is the union of
/// [`deja::SeedPlan::touched_db_tables`] and [`deja::build_write_target_tables`].
///
/// Each cloned table is `CREATE TABLE … (LIKE public.t INCLUDING DEFAULTS
/// INCLUDING CONSTRAINTS INCLUDING INDEXES)`. `LIKE` never copies FOREIGN KEYS —
/// deliberate: we seed only a subset of rows (read-before-write preconditions),
/// so FK refs would otherwise dangle. `INCLUDING INDEXES` brings the PK/unique
/// indexes the seed UPSERT's `ON CONFLICT` needs; `INCLUDING DEFAULTS` keeps
/// SERIAL/sequence defaults so the router's own inserts (which omit the serial
/// id) still work. Best-effort: a failure logs + continues.
///
/// If `tables` is empty, only the bare schema is created (the router still emits
/// `SET search_path TO "<schema>", public` — a schema with no tables is safe and
/// falls through to `public`).
/// Build the SQL for [`create_db_schema`]: `CREATE SCHEMA` + a `DO` block that
/// `LIKE`-clones ONLY the named tables from `public` (not every public table).
/// Empty `tables` → bare schema only. Pure (no store) so it is unit-testable.
fn build_create_schema_sql(schema: &str, tables: &[String]) -> String {
    // De-duplicate + quote. Empty → bare schema (search_path falls through to public).
    let table_list: Vec<String> = {
        let mut t = tables.to_vec();
        t.sort();
        t.dedup();
        t.into_iter()
            .map(|name| format!("'{}'", name.replace('\'', "''")))
            .collect()
    };
    let clone_sql = if table_list.is_empty() {
        String::new()
    } else {
        format!(
            "DO $deja$ DECLARE r record; BEGIN \
               FOR r IN SELECT tablename FROM pg_tables WHERE schemaname = 'public' \
                 AND tablename = ANY(ARRAY[{}]) LOOP \
                 EXECUTE format('CREATE TABLE IF NOT EXISTS \"{schema}\".%I \
                   (LIKE public.%I INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES)', \
                   r.tablename, r.tablename); \
               END LOOP; \
             END $deja$;",
            table_list.join(", ")
        )
    };
    format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\"; {clone_sql}")
}

/// Create the per-correlation isolation schema (R1) and clone ONLY the tables
/// the correlation actually touches — its read-precondition tables (rows the
/// seed materializes) and its write-target tables (writes must isolate here, not
/// leak to the shared `public` base). Untouched tables are NOT cloned: they
/// resolve to the (empty, freshly-migrated) `public` via `search_path` fallback
/// as a correct miss, so cloning them is pure waste. `tables` is the union of
/// [`deja::SeedPlan::touched_db_tables`] and [`deja::build_write_target_tables`].
///
/// Each cloned table is `CREATE TABLE … (LIKE public.t INCLUDING DEFAULTS
/// INCLUDING CONSTRAINTS INCLUDING INDEXES)`. `LIKE` never copies FOREIGN KEYS —
/// deliberate: we seed only a subset of rows (read-before-write preconditions),
/// so FK refs would otherwise dangle. `INCLUDING INDEXES` brings the PK/unique
/// indexes the seed UPSERT's `ON CONFLICT` needs; `INCLUDING DEFAULTS` keeps
/// SERIAL/sequence defaults so the router's own inserts (which omit the serial
/// id) still work. Best-effort: a failure logs + continues.
///
/// If `tables` is empty, only the bare schema is created (the router still emits
/// `SET search_path TO "<schema>", public` — a schema with no tables is safe and
/// falls through to `public`).
fn create_db_schema(store: &StoreExec, schema: &str, tables: &[String]) {
    let sql = build_create_schema_sql(schema, tables);
    let table_count = tables
        .iter()
        .collect::<std::collections::HashSet<_>>()
        .len();
    eprintln!(
        "lifecycle: create_db_schema {schema} (clone of {} table(s): {})",
        table_count,
        if tables.is_empty() {
            "none — bare schema, untouched tables fall through to public".to_string()
        } else {
            tables.join(",")
        }
    );
    match store.psql(&[], false, &sql).status() {
        Ok(status) if status.success() => {}
        Ok(status) => {
            eprintln!(
                "lifecycle: create_db_schema {schema} exited {status}; continuing (best-effort)"
            );
        }
        Err(e) => {
            eprintln!(
                "lifecycle: could not create_db_schema {schema}: {e}; continuing (best-effort)"
            );
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DbColumnMetadata {
    name: String,
    type_oid: Option<u32>,
    type_name: Option<String>,
    nullable: Option<bool>,
}

impl DbColumnMetadata {
    fn unknown(name: &str) -> Self {
        Self {
            name: name.to_string(),
            type_oid: None,
            type_name: None,
            nullable: None,
        }
    }

    /// Classify this column into the cast the seed renderer emits, using the
    /// always-available pg catalog metadata (`type_oid` and/or `type_name`).
    /// Implemented as a `TryFrom` so precedence (array spellings bind before
    /// scalar json/jsonb) and the untyped fallthrough live in one place.
    fn cast_kind(&self) -> SqlCastKind {
        SqlCastKind::try_from(self).unwrap_or(SqlCastKind::Untyped)
    }
    fn merge_typed(&self, typed: &deja::db::DbColumnImage) -> Self {
        Self {
            name: typed.name.clone(),
            type_oid: typed.type_oid.or(self.type_oid),
            type_name: typed.type_name.clone().or_else(|| self.type_name.clone()),
            nullable: typed.nullable.or(self.nullable),
        }
    }
}

/// The pg cast the seed renderer applies to a column's quoted value, or
/// `Untyped` when the plain-quote path is correct. Derived purely from pg
/// catalog metadata — a fallible mapping of the `(type_oid, type_name)` key,
/// so precedence (arrays before scalars) and the untyped fallthrough are
/// explicit in one place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqlCastKind {
    Bytea,
    /// scalar `json` (oid 114)
    Json,
    /// scalar `jsonb` (oid 3802)
    Jsonb,
    /// array-of-json (oid 199, pg name `_json` / `json[]`)
    JsonArray,
    /// array-of-jsonb (oid 3807, pg name `_jsonb` / `jsonb[]`)
    JsonbArray,
    /// No metadata, or a collected type we don't cast (text, numeric, uuid,
    /// timestamptz, text[], …) — renderer falls through to plain quoting.
    Untyped,
}

impl TryFrom<&DbColumnMetadata> for SqlCastKind {
    type Error = ();

    /// Array spellings (`_json`, `json[]`, `_jsonb`, `jsonb[]`) bind BEFORE the
    /// scalar json/jsonb names so the array casts are never shadowed by a
    /// scalar-name match. Both oid and name are honored since either may be the
    /// only metadata present.
    fn try_from(md: &DbColumnMetadata) -> Result<Self, Self::Error> {
        let key = (md.type_oid, md.type_name.as_deref());
        match key {
            (Some(17), _) | (_, Some("bytea")) => Ok(SqlCastKind::Bytea),
            (Some(199), _) | (_, Some("_json")) | (_, Some("json[]")) => Ok(SqlCastKind::JsonArray),
            (Some(3807), _) | (_, Some("_jsonb")) | (_, Some("jsonb[]")) => {
                Ok(SqlCastKind::JsonbArray)
            }
            (Some(114), _) | (_, Some("json")) => Ok(SqlCastKind::Json),
            (Some(3802), _) | (_, Some("jsonb")) => Ok(SqlCastKind::Jsonb),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct DbColumnImage {
    metadata: DbColumnMetadata,
    value: serde_json::Value,
}

/// The PHYSICAL half of a typed row image, when the tape carried one: per
/// column (keyed by name), the verbatim binary wire bytes (`typsend` output)
/// the recording store sent, hex-decoded; `None` is SQL NULL. Produced by the
/// `deja-diesel` connection wrapper and carried on the row image next to the
/// serde values (`deja::db::DbColumnImage::wire`). Consumed by seeding ONLY —
/// never diffed, never substituted.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct DbRowWireImage {
    columns: BTreeMap<String, Option<Vec<u8>>>,
}

#[derive(Debug, Clone, PartialEq)]
struct DbRowImage {
    table: String,
    columns: Vec<DbColumnImage>,
    /// `Some` iff the typed payload declared `wire_format: "binary"` and every
    /// column's wire hex decoded — the row is a candidate for binary-COPY
    /// seeding (see [`wire_copy_eligibility`]).
    wire: Option<DbRowWireImage>,
}

impl DbRowImage {
    fn from_json_object(
        table: &str,
        row: &serde_json::Map<String, serde_json::Value>,
        catalog: &DbCatalog,
    ) -> Option<Self> {
        if row.is_empty() {
            return None;
        }
        let columns = row
            .iter()
            .map(|(name, value)| DbColumnImage {
                metadata: catalog.metadata_for(table, name),
                value: value.clone(),
            })
            .collect();
        Some(Self {
            table: table.to_string(),
            columns,
            wire: None,
        })
    }
}

fn seed_contains_null_column(rows: &[DbRowImage], column_name: &str) -> bool {
    rows.iter().any(|row| {
        row.columns
            .iter()
            .any(|column| column.metadata.name == column_name && column.value.is_null())
    })
}

fn db_row_images(table: &str, value: &serde_json::Value, catalog: &DbCatalog) -> Vec<DbRowImage> {
    match value {
        serde_json::Value::Object(map) => DbRowImage::from_json_object(table, map, catalog)
            .into_iter()
            .collect(),
        serde_json::Value::Array(items) => items
            .iter()
            .filter_map(|value| {
                value
                    .as_object()
                    .and_then(|map| DbRowImage::from_json_object(table, map, catalog))
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn db_row_images_from_typed_payload(
    expected_table: &str,
    image: &serde_json::Value,
    catalog: &DbCatalog,
) -> Option<Vec<DbRowImage>> {
    let typed_rows = match image {
        serde_json::Value::Array(values) => values
            .iter()
            .filter_map(|value| typed_db_row_image(expected_table, value, catalog))
            .collect::<Vec<_>>(),
        _ => typed_db_row_image(expected_table, image, catalog)
            .into_iter()
            .collect(),
    };

    if typed_rows.is_empty() {
        return None;
    }
    if !typed_rows
        .iter()
        .any(|(_, has_producer_metadata)| *has_producer_metadata)
    {
        eprintln!(
            "lifecycle: typed db row image for {expected_table} carried only unknown producer metadata; falling back to legacy seed value"
        );
        return None;
    }
    Some(typed_rows.into_iter().map(|(row, _)| row).collect())
}

fn typed_column_has_metadata(column: &deja::db::DbColumnImage) -> bool {
    column.type_oid.is_some() || column.type_name.is_some() || column.nullable.is_some()
}

fn typed_db_row_image(
    expected_table: &str,
    value: &serde_json::Value,
    catalog: &DbCatalog,
) -> Option<(DbRowImage, bool)> {
    let payload: deja::db::DbRowImage = serde_json::from_value(value.clone()).ok()?;
    if payload.deja_image != deja::db::DbRowImage::KIND
        || payload.version != deja::db::DbRowImage::VERSION
        || payload.table != expected_table
        || payload.columns.is_empty()
    {
        return None;
    }
    let has_producer_metadata = payload.columns.iter().any(typed_column_has_metadata);
    let wire = wire_image_from_typed(&payload);
    let columns = payload
        .columns
        .iter()
        .map(|column| DbColumnImage {
            metadata: catalog
                .metadata_for(&payload.table, &column.name)
                .merge_typed(column),
            value: column.value.clone(),
        })
        .collect();
    Some((
        DbRowImage {
            table: payload.table,
            columns,
            wire,
        },
        has_producer_metadata,
    ))
}

/// Decode the physical wire image off a typed row payload. `None` when the
/// producer attached none (old tape / unjoined capture) or when any column's
/// hex fails to decode or repeats — a physical image that cannot be trusted
/// wholesale is ignored wholesale, and the row seeds through the serde path.
fn wire_image_from_typed(payload: &deja::db::DbRowImage) -> Option<DbRowWireImage> {
    if payload.wire_format.as_deref() != Some(deja::db::DbRowImage::WIRE_FORMAT_BINARY) {
        return None;
    }
    let mut columns = BTreeMap::new();
    for column in &payload.columns {
        let bytes = match &column.wire {
            Some(wire_hex) => match hex::decode(wire_hex) {
                Ok(bytes) => Some(bytes),
                Err(_) => {
                    eprintln!(
                        "lifecycle: undecodable wire hex for column {} of {}; ignoring the row's physical image",
                        column.name, payload.table
                    );
                    return None;
                }
            },
            None => None,
        };
        if columns.insert(column.name.clone(), bytes).is_some() {
            eprintln!(
                "lifecycle: duplicate column {} in wire image for {}; ignoring the row's physical image",
                column.name, payload.table
            );
            return None;
        }
    }
    Some(DbRowWireImage { columns })
}

/// First OID postgres hands out for user-defined objects is 16384, and
/// everything below 10000 is a hand-assigned built-in pinned at initdb —
/// identical on every cluster of every supported version. (10000–16383 are
/// initdb-time leftovers no application column type ever gets.)
const FIRST_NON_BUILTIN_OID: u32 = 10_000;

/// The v1 wire-seeding boundary (the design doc's "container types embed
/// element OIDs" spike, resolved): a row seeds via binary COPY only when
/// EVERY column is provably portable across clusters —
///
/// - SQL NULL: no bytes to interpret;
/// - built-in type (captured OID < 10000): built-in OIDs are pinned and
///   cluster-stable, and any OID a built-in CONTAINER value embeds (the
///   element OID in a binary array header, e.g. `json[]` embedding 114) is
///   itself built-in and equally stable;
/// - user-defined type whose wire bytes EQUAL the semantic string's UTF-8:
///   the value is then plain label text with no embedded OIDs — the pg enum
///   case (`enum_send` output IS the label, `enum_recv` resolves it by name
///   on the target cluster). Proven per VALUE, byte-for-byte, not assumed
///   per type.
///
/// Everything else — user-defined composites, arrays of user-defined
/// elements (their binary header embeds the recording cluster's element
/// OID), any user-defined value without textual evidence — sends the WHOLE
/// row down the serde INSERT path (a row lands in one statement). That
/// fallback keeps the fail-closed renderer: a poison column in a fallback
/// row still refuses loudly rather than guessing.
fn wire_copy_eligibility(row: &DbRowImage) -> Result<(), String> {
    let Some(wire) = row.wire.as_ref() else {
        return Err("no physical image".to_string());
    };
    for column in &row.columns {
        let name = &column.metadata.name;
        let Some(bytes) = wire.columns.get(name) else {
            return Err(format!("column {name} has no wire capture"));
        };
        match bytes {
            None => {
                if !column.value.is_null() {
                    return Err(format!(
                        "column {name}: wire value is NULL but the semantic value is not"
                    ));
                }
            }
            Some(bytes) => {
                let Some(type_oid) = column.metadata.type_oid else {
                    return Err(format!("column {name} carries wire bytes but no type oid"));
                };
                if type_oid < FIRST_NON_BUILTIN_OID {
                    continue;
                }
                let is_label_text = column
                    .value
                    .as_str()
                    .is_some_and(|text| text.as_bytes() == bytes.as_slice());
                if !is_label_text {
                    return Err(format!(
                        "column {name}: user-defined type oid {type_oid} without label-text evidence"
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Build `INSERT INTO <table> (cols...) VALUES (...) ON CONFLICT DO NOTHING`
/// from a typed row image. Values are rendered according to column metadata when
/// available; unknown metadata falls back to generic JSON-as-SQL-literal
/// rendering. `bytea` handling is gated solely by the column type metadata, not
/// by guessing object shapes globally.
fn build_insert_sql(schema: Option<&str>, row: &DbRowImage) -> Result<String, String> {
    if row.columns.is_empty() {
        return Err("row image has no columns".to_string());
    }
    let col_list = row
        .columns
        .iter()
        .map(|column| quote_ident(&column.metadata.name))
        .collect::<Vec<_>>()
        .join(", ");
    let mut values = Vec::with_capacity(row.columns.len());
    for column in &row.columns {
        // Name the column that refused. A bare "could not render" makes every
        // cause look alike from outside the pod, which is how a serde-shaped
        // value in a scalar column (`{"TxnId": …}` in a varchar) read as a
        // generic seeding failure for a full sandbox cycle.
        // The column name and its type are the whole diagnosis; the value
        // itself stays out of the message (it is row data, and the certificate
        // is served over the API).
        let literal = sql_literal_for_column(column).ok_or_else(|| {
            format!(
                "column {} (type oid {:?}, name {:?}) holds a {} value the fail-closed renderer refuses to guess at",
                column.metadata.name,
                column.metadata.type_oid,
                column.metadata.type_name,
                match &column.value {
                    serde_json::Value::Object(_) => "structured object",
                    serde_json::Value::Array(_) => "array",
                    _ => "scalar",
                },
            )
        })?;
        values.push(literal);
    }
    let value_list = values.join(", ");
    // Qualify the target with the per-correlation schema when isolating (R1), so
    // the row lands in that case's schema — the one the router's `search_path`
    // selects during replay. `ON CONFLICT DO NOTHING` (no target) needs no PK
    // knowledge: the cloned schema starts empty, so this only no-ops on the rare
    // intra-seed duplicate. Unqualified (→ search_path/public) when no schema.
    let qualified_table = qualified_table(schema, &row.table);
    Ok(format!(
        "INSERT INTO {qualified_table} ({col_list}) VALUES ({value_list}) ON CONFLICT DO NOTHING;"
    ))
}

fn qualified_table(schema: Option<&str>, table: &str) -> String {
    match schema {
        Some(s) => format!("{}.{}", quote_ident(s), quote_ident(table)),
        None => quote_ident(table),
    }
}

/// Double-quote a SQL identifier, escaping embedded double-quotes.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Render a recorded JSON array into a Postgres array of `json`/`jsonb`.
///
/// `ARRAY['<elem>', …]::json[]` — each element rendered as its own json literal.
/// A recorded value that is not an array cannot describe an array column, so it
/// is refused rather than guessed at.
fn pg_json_array_literal(
    value: &serde_json::Value,
    element_type: &str,
    column_name: &str,
) -> Option<String> {
    let Some(items) = value.as_array() else {
        eprintln!(
            "lifecycle: seed value for {element_type}[] column {column_name} is not an array; skipping row"
        );
        return None;
    };
    let elements = items.iter().map(sql_literal).collect::<Vec<_>>().join(", ");
    Some(format!("ARRAY[{elements}]::{element_type}[]"))
}

/// One `column IS NOT DISTINCT FROM value` predicate for the seed readback.
///
/// Postgres `json` carries no equality operator — only `jsonb` does — so a
/// readback that compares a `json` or `json[]` column directly fails with
/// "operator does not exist: json = json" and the verification is lost rather
/// than answered. Both sides are compared as `jsonb`, which is equality on the
/// parsed document: what the readback is asking in the first place, and it does
/// not depend on key order or whitespace surviving a round trip.
fn db_comparison_predicate(column_name: &str, column: &DbColumnImage) -> Option<String> {
    let literal = sql_literal_for_column(column)?;
    let ident = quote_ident(column_name);
    Some(match column.metadata.cast_kind() {
        SqlCastKind::Json => format!("{ident}::jsonb IS NOT DISTINCT FROM ({literal})::jsonb"),
        SqlCastKind::JsonArray => {
            format!("{ident}::jsonb[] IS NOT DISTINCT FROM ({literal})::jsonb[]")
        }
        _ => format!("{ident} IS NOT DISTINCT FROM {literal}"),
    })
}

fn sql_literal_for_column(column: &DbColumnImage) -> Option<String> {
    if column.value.is_null() {
        return Some("NULL".to_string());
    }
    let md = &column.metadata;
    // Classify once via the pg catalog metadata; the match is exhaustive over
    // the cast families the renderer understands, so an unhandled typed
    // non-scalar (the json[] "malformed array literal" class of bug) can't slip
    // through a quote-and-hope fallthrough.
    match md.cast_kind() {
        SqlCastKind::Bytea => bytea_bytes_from_typed_value(&column.value).map_or_else(
            || {
                eprintln!(
                    "lifecycle: cannot render bytea seed value for column {}; skipping row",
                    md.name
                );
                None
            },
            |bytes| Some(bytea_hex_literal(&bytes)),
        ),
        // Scalar json/jsonb: the recorded value is one document, and a quoted
        // JSON literal with an explicit cast is exactly that.
        SqlCastKind::Json => Some(format!("{}::json", sql_literal(&column.value))),
        SqlCastKind::Jsonb => Some(format!("{}::jsonb", sql_literal(&column.value))),
        // Array-of-json: the recorded value is a JSON array, and its ELEMENTS are
        // the Postgres array's elements. Casting the array's text is not a way to
        // say that — Postgres writes array literals as `{…}`, so it reads the
        // leading `[` as a malformed one and rejects the value whatever the cast
        // says. An array constructor states the structure instead of hoping the
        // text is read as one.
        SqlCastKind::JsonArray => pg_json_array_literal(&column.value, "json", &md.name),
        SqlCastKind::JsonbArray => pg_json_array_literal(&column.value, "jsonb", &md.name),
        // Fail-closed for a typed column we haven't been taught: if it carries
        // authoritative pg catalog metadata but is neither bytea nor the json
        // family AND holds a non-scalar object/array, the plain-quote path
        // could silently mis-materialize (the array case). Surface it rather
        // than guess. Scalar values in unknown columns are safe to quote.
        SqlCastKind::Untyped => {
            let has_type_metadata = md.type_oid.is_some() || md.type_name.is_some();
            let is_non_scalar = column.value.is_object() || column.value.is_array();
            if has_type_metadata && is_non_scalar {
                eprintln!(
                    "lifecycle: cannot render non-scalar seed value for column {} of type {:?}/{:?}; skipping row",
                    md.name, md.type_oid, md.type_name
                );
                return None;
            }
            // No catalog metadata AND a non-scalar value: the plain-quote path is
            // a coin flip decided by the column's real type. Postgres coerces an
            // unquoted-type literal into json/jsonb happily, so an object or array
            // in a json column still lands; the same literal in an ARRAY column is
            // read as an array constructor and fails ("malformed array literal").
            // Rendering it anyway is the pre-existing behaviour and is right more
            // often than not, but a column reaching here at all means the catalog
            // did not cover it — name it, because that gap is the actual bug and
            // it is otherwise invisible.
            if is_non_scalar {
                eprintln!(
                    "lifecycle: no pg catalog metadata for column {}; rendering its {} seed value as a plain literal, which pg will reject if the column is array-typed",
                    md.name,
                    if column.value.is_array() {
                        "array"
                    } else {
                        "object"
                    },
                );
            }
            Some(sql_literal(&column.value))
        }
    }
}

/// Render a JSON value as a SQL literal with no column-type assumptions:
/// `null` → `NULL`; strings → quoted literals; numbers/bools → their text;
/// objects/arrays → quoted compact JSON text. Single quotes are SQL-escaped by
/// doubling. `bytea` is intentionally NOT inferred here.
fn sql_literal(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        other => format!("'{}'", other.to_string().replace('\'', "''")),
    }
}

fn bytea_hex_literal(bytes: &[u8]) -> String {
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    format!("'\\x{hex}'::bytea")
}

fn bytea_bytes_from_typed_value(value: &serde_json::Value) -> Option<Vec<u8>> {
    match value {
        serde_json::Value::Object(map) => bytea_from_inner_array(map),
        serde_json::Value::Array(values) => bytea_from_array(values),
        serde_json::Value::String(s) => {
            if let Some(hex) = s.strip_prefix("\\x") {
                decode_hex(hex)
            } else if s.len() % 2 == 0 && s.bytes().all(|b| b.is_ascii_hexdigit()) {
                decode_hex(s)
            } else {
                Some(s.as_bytes().to_vec())
            }
        }
        _ => None,
    }
}

fn bytea_from_inner_array(map: &serde_json::Map<String, serde_json::Value>) -> Option<Vec<u8>> {
    if map.len() != 1 {
        return None;
    }
    bytea_from_array(map.get("inner")?.as_array()?)
}

fn bytea_from_array(values: &[serde_json::Value]) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        let n = value.as_u64()?;
        if n > 255 {
            return None;
        }
        out.push(n as u8);
    }
    Some(out)
}

fn decode_hex(hex: &str) -> Option<Vec<u8>> {
    if hex.len() % 2 != 0 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for chunk in hex.as_bytes().chunks_exact(2) {
        let pair = std::str::from_utf8(chunk).ok()?;
        bytes.push(u8::from_str_radix(pair, 16).ok()?);
    }
    Some(bytes)
}

/// Outcome of rendering a recorded redis seed value into a write payload.
/// The three arms are the three honest answers, and each maps to a distinct
/// certificate story: write it, correctly write nothing, or refuse loudly.
#[derive(Debug, Clone, PartialEq)]
enum RedisSeedRender {
    /// A faithful write exists: materialize it.
    Payload(RedisSeedPayload),
    /// The recorded value IS absence (a `Null` miss, an empty collection):
    /// seeding nothing reproduces it. Skipped on the certificate, reason named.
    Absent(&'static str),
    /// No faithful write command exists for this shape. Skipped LOUDLY on the
    /// certificate with the named reason — never stringified into a `SET`.
    Unrepresentable(String),
}

/// Reconstruct the typed write a recorded redis read implies — the backward
/// half of the wire transform (#39; the wire-faithful-seeding scaling rule:
/// per-protocol adapters, never per-type patches).
///
/// Two genuinely different sources feed this, and the branch reflects that:
///
/// - **Recording-derived** — a serialized [`deja::value::RedisWireValue`], the
///   canonical shared type. Decoded FIRST, then mapped by an exhaustive match
///   (no wildcard): scalars → `SET`, `Map` → `HSET`, `Array` → `RPUSH` (or
///   `ZADD` when `method` names a sorted-set read — the value alone cannot
///   distinguish a list range from a zset range), `Set` → `SADD`, `Null` →
///   nothing, and the non-value shapes (status replies, attribute/verbatim/
///   push frames, the unsupported catch) → a named skip. A new wire variant
///   is a compile error here, not a silent replay gap.
/// - **Ambient/config template** — a bare JSON string (e.g. `"0.20"`) that is
///   *already* the raw text redis holds. Passed through byte-for-byte.
///
/// Anything else — a wire shape with no faithful write command
/// (push/verbatim/attribute/status replies; modelled by the canonical type
/// since #45 so the client adapters stay total, but still not seedable), or a
/// structure with members that are not scalars — is
/// [`RedisSeedRender::Unrepresentable`]: the caller records an explicit skip
/// in the seed certificate instead of writing garbage. This is what replaced
/// the old `to_string()` fallback, which wrote the enum wrapper text
/// (`{"BulkString":[…]}`) into redis and made the replayed router branch on
/// garbage — a false divergence.
fn render_redis_seed_payload(value: &serde_json::Value, method: Option<&str>) -> RedisSeedRender {
    // Recording-derived: decode into the canonical shared type FIRST. This also
    // correctly treats a bare-string unit variant (`"Null"`, a miss that leaked
    // past the upstream filter) as "nothing to seed" rather than the literal
    // text "Null".
    if let Ok(v) = serde_json::from_value::<deja::value::RedisWireValue>(value.clone()) {
        return wire_value_seed_render(&v, method);
    }
    // Ambient/config template: a bare string that is NOT a `RedisWireValue`
    // (e.g. `"0.20"`) is already the raw text redis holds. Preserved
    // byte-for-byte, exactly as before.
    if let serde_json::Value::String(s) = value {
        return RedisSeedRender::Payload(RedisSeedPayload::String(s.clone()));
    }
    // A wire shape the canonical type does not model: an explicit, named skip,
    // never a silent stringify of the wrapper.
    let tag = value
        .as_object()
        .filter(|object| object.len() == 1)
        .and_then(|object| object.keys().next().cloned());
    RedisSeedRender::Unrepresentable(match tag {
        Some(tag) => {
            format!("redis wire variant `{tag}` has no faithful write command; refusing to guess")
        }
        None => "redis value is neither a RedisWireValue nor an ambient string; refusing to guess"
            .to_owned(),
    })
}

/// Map one decoded wire value to its write payload. Exhaustive over
/// [`deja::value::RedisWireValue`] — the compiler enforces the seeder answers
/// for every variant the codec can carry.
fn wire_value_seed_render(
    value: &deja::value::RedisWireValue,
    method: Option<&str>,
) -> RedisSeedRender {
    use deja::value::RedisWireValue as Wire;
    match value {
        Wire::Null => RedisSeedRender::Absent(
            "recorded redis miss (Null): absence is the correct seed, nothing written",
        ),
        Wire::Int(_)
        | Wire::BulkString(_)
        | Wire::SimpleString(_)
        | Wire::Double(_)
        | Wire::Boolean(_) => match value.to_redis_string() {
            Some(rendered) => RedisSeedRender::Payload(RedisSeedPayload::String(rendered)),
            // Unreachable for the scalar variants matched above; stated rather
            // than unwrapped so a rendering change cannot panic the seeder.
            None => RedisSeedRender::Unrepresentable(
                "scalar redis value did not render to a string".to_owned(),
            ),
        },
        Wire::Map(pairs) => {
            if pairs.is_empty() {
                return RedisSeedRender::Absent(
                    "recorded empty hash: redis holds no key for it; absence is the correct seed",
                );
            }
            let mut rendered = Vec::with_capacity(pairs.len());
            for (field, field_value) in pairs {
                match (field.to_redis_string(), field_value.to_redis_string()) {
                    (Some(field), Some(field_value)) => rendered.push((field, field_value)),
                    _ => {
                        return RedisSeedRender::Unrepresentable(
                            "redis Map carries a non-scalar field or value; HSET cannot represent it"
                                .to_owned(),
                        );
                    }
                }
            }
            RedisSeedRender::Payload(RedisSeedPayload::Hash(rendered))
        }
        Wire::Array(items) => {
            if items.is_empty() {
                return RedisSeedRender::Absent(
                    "recorded empty list/range: redis holds no key for it; absence is the correct seed",
                );
            }
            let mut members = Vec::with_capacity(items.len());
            for item in items {
                match item.to_redis_string() {
                    Some(member) => members.push(member),
                    None => {
                        return RedisSeedRender::Unrepresentable(
                            "redis Array carries a non-scalar member; RPUSH/ZADD cannot represent it"
                                .to_owned(),
                        );
                    }
                }
            }
            if method.is_some_and(method_names_sorted_set) {
                RedisSeedRender::Payload(RedisSeedPayload::SortedSet(members))
            } else {
                RedisSeedRender::Payload(RedisSeedPayload::List(members))
            }
        }
        Wire::Set(items) => {
            if items.is_empty() {
                return RedisSeedRender::Absent(
                    "recorded empty set: redis holds no key for it; absence is the correct seed",
                );
            }
            let mut members = Vec::with_capacity(items.len());
            for item in items {
                match item.to_redis_string() {
                    Some(member) => members.push(member),
                    None => {
                        return RedisSeedRender::Unrepresentable(
                            "redis Set carries a non-scalar member; SADD cannot represent it"
                                .to_owned(),
                        );
                    }
                }
            }
            RedisSeedRender::Payload(RedisSeedPayload::Set(members))
        }
        // Status acknowledgements (`+OK`, `+QUEUED`): command outcomes, not
        // stored state — there is nothing a write could faithfully reproduce.
        // Before #45 these bare-string tags slipped past the wire decode into
        // the ambient-string branch and were seeded as the literal text
        // "Okay"/"Queued"; the exhaustive arm turns that silent garbage into a
        // named skip.
        Wire::Okay => RedisSeedRender::Unrepresentable(
            "redis wire variant `Okay` is a status reply, not stored state; refusing to seed it"
                .to_owned(),
        ),
        Wire::Queued => RedisSeedRender::Unrepresentable(
            "redis wire variant `Queued` is a status reply, not stored state; refusing to seed it"
                .to_owned(),
        ),
        // Wire shapes with no faithful write command. Modelled since #45 (the
        // client adapters must be total over both client enums), so the loud
        // skip moved from "deserializing fails" to this exhaustive match —
        // same certificate entry, now compiler-enforced.
        Wire::Attribute { .. } => RedisSeedRender::Unrepresentable(
            "redis wire variant `Attribute` has no faithful write command; refusing to guess"
                .to_owned(),
        ),
        Wire::VerbatimString { .. } => RedisSeedRender::Unrepresentable(
            "redis wire variant `VerbatimString` has no faithful write command; refusing to guess"
                .to_owned(),
        ),
        Wire::Push { .. } => RedisSeedRender::Unrepresentable(
            "redis wire variant `Push` has no faithful write command; refusing to guess".to_owned(),
        ),
        Wire::UnsupportedSuccessfulValue { variant } => RedisSeedRender::Unrepresentable(format!(
            "redis value recorded as unsupported ({variant}); refusing to guess"
        )),
    }
}

/// Whether a recorded method name identifies a SORTED-SET read — the one case
/// where the wire value (`Array`) does not determine the write command. The
/// stems cover the redis Z-command family; matched as substrings so wrapper
/// names (`get_sorted_set_range`, `zrange_withscores_raw`) resolve too.
/// Conservative default: no match → list (`RPUSH`), and a wrongly-listed zset
/// surfaces as a readback/replay divergence, not silent corruption.
fn method_names_sorted_set(method: &str) -> bool {
    const SORTED_SET_STEMS: [&str; 12] = [
        "zadd",
        "zrange",
        "zscore",
        "zincr",
        "zrem",
        "zcard",
        "zcount",
        "zrank",
        "zscan",
        "zpop",
        "zrandmember",
        "sorted_set",
    ];
    let method = method.to_ascii_lowercase();
    SORTED_SET_STEMS.iter().any(|stem| method.contains(stem))
}

/// Read a recording's boundary events JSONL, tolerating non-event records from
/// the shared `DejaRecord` stream exactly like the lookup renderer does.
/// Returns an empty vec on any I/O failure (best-effort seeding).
/// Drain a scoped recording into memory for seed planning, which needs random
/// access per correlation. Materializing is acceptable HERE and only here
/// because the scope has already cut the stream down to the run's own cases; on
/// a whole session this is the 361 MB the streaming readers exist to avoid.
/// Best-effort: an unreadable tape logs and yields nothing rather than failing
/// the replay, matching the prior hand-coded seeds' behavior.
fn read_scoped_events(recording: &crate::scope::ScopedRecording) -> Vec<deja::BoundaryEvent> {
    let stream = match recording.events() {
        Ok(stream) => stream,
        Err(e) => {
            eprintln!(
                "lifecycle: seed plan: recording {} not readable ({e}); skipping seeding",
                recording.recording_id()
            );
            return Vec::new();
        }
    };
    stream
        .filter_map(|item| match item {
            crate::scope::TapeItem::Event(event) => Some(*event),
            crate::scope::TapeItem::Malformed { .. } => None,
        })
        .collect()
}

/// Load the ambient/config template for seed materialization (deliverable 4).
///
/// If `DEJA_AMBIENT_TEMPLATE` points at a `boundary\tkey\tvalue` TSV file, it is
/// parsed from there; otherwise the built-in EU-settlement
/// [`demo_defaults`](deja::AmbientTemplate::demo_defaults) supply the premium
/// rate — replacing the hand-coded `redis-cli SET settlement_rate_premium 0.20`.
fn load_ambient_template() -> deja::AmbientTemplate {
    if let Ok(path) = std::env::var("DEJA_AMBIENT_TEMPLATE") {
        if !path.trim().is_empty() {
            match std::fs::read_to_string(&path) {
                Ok(text) => {
                    let template = deja::AmbientTemplate::from_tsv(&text);
                    eprintln!(
                        "lifecycle: loaded ambient template from {path} ({} entries)",
                        template.entries().len()
                    );
                    return template;
                }
                Err(e) => {
                    eprintln!(
                        "lifecycle: could not read DEJA_AMBIENT_TEMPLATE={path}: {e}; \
                         falling back to demo defaults"
                    );
                }
            }
        }
    }
    deja::AmbientTemplate::demo_defaults()
}

fn run_workload(
    demo: &Demo,
    ctx: &StoreCtx,
    recording_id: &str,
    iterations: u64,
) -> Result<(), String> {
    let mut args = demo.compose_base_args();
    args.extend(
        [
            "exec",
            "-T",
            "-e",
            "BASE_URL=http://127.0.0.1:8080",
            "-e",
            "ADMIN_API_KEY=test_admin",
            "-e",
            "WORKLOAD_REQUIRE_CONFIRM_SUCCESS=true",
            "-e",
            "WORKLOAD_FAIL_ON_ANY_ERROR=true",
            "hyperswitch-server",
            "/workload.sh",
        ]
        .iter()
        .map(|s| s.to_string()),
    );
    args.push(iterations.to_string());
    let mut cmd = Command::new("docker");
    cmd.args(&args).envs(demo.compose_env(recording_id, ""));
    let status = run_streamed(
        cmd,
        ctx,
        "driving payment workload (recording to the configured sink)",
        "workload",
    )?;
    if !status.success() {
        return Err(format!("workload failed (status {status})"));
    }
    Ok(())
}

/// Graceful `docker compose stop <service>` (best-effort): the router's
/// SIGTERM handler drops the recording hook, whose writer shutdown flushes
/// the Kafka producer and emits the `eof` sink marker.
fn stop_service(demo: &Demo, recording_id: &str, service: &str) {
    let mut args = demo.compose_base_args();
    args.extend(
        ["stop", "--timeout", "30", service]
            .iter()
            .map(|s| s.to_string()),
    );
    match Command::new("docker")
        .args(&args)
        .envs(demo.compose_env(recording_id, ""))
        .output()
    {
        Ok(o) if o.status.success() => eprintln!("lifecycle: stopped {service}"),
        Ok(o) => eprintln!(
            "lifecycle: stop {service} failed (continuing): {}",
            String::from_utf8_lossy(&o.stderr)
        ),
        Err(e) => eprintln!("lifecycle: stop {service} failed (continuing): {e}"),
    }
}

fn run_kernel(
    kernel_bin: &str,
    target_port: u16,
    root: &HarnessRoot,
    ctx: &StoreCtx,
    recording_id: &str,
    run_id: &str,
    scope: &crate::scope::RunScope,
) -> Result<(), String> {
    let diff_sink = root.http_diff_path(run_id);
    let mut cmd = Command::new(kernel_bin);
    // The scope crosses a PROCESS boundary here, so the tape path and the
    // correlation filter are emitted by ONE call and cannot be set apart: a
    // caller that set the path and forgot the filter pointed the kernel at the
    // whole session.
    cmd.envs(crate::scope::TapeSlot::subprocess(
        root,
        recording_id,
        scope,
    ))
    .env("KERNEL_TARGET_HOST", "127.0.0.1")
    .env("KERNEL_TARGET_PORT", target_port.to_string())
    .env("KERNEL_HTTP_DIFF_SINK", &diff_sink);
    // empty allowlist by default = byte-exact gate; override via
    // KERNEL_BODY_ALLOWLIST on the harness-api process during bring-up.
    let status = run_streamed(cmd, ctx, "driving recorded requests (kernel)", "kernel")?;
    if !status.success() {
        return Err(format!("kernel failed (status {status})"));
    }
    Ok(())
}

/// Poll a candidate's `/health` from INSIDE the container via `docker compose
/// exec` — for services not published to the host (the record candidate). Fails
/// FAST (with container logs) if the container has exited, instead of spinning
/// until the timeout.
fn wait_health_exec(
    demo: &Demo,
    recording_id: &str,
    service: &str,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut args = demo.compose_base_args();
        args.extend(
            [
                "exec",
                "-T",
                service,
                "curl",
                "-fsS",
                "-o",
                "/dev/null",
                "--max-time",
                "3",
                "http://localhost:8080/health",
            ]
            .iter()
            .map(|s| s.to_string()),
        );
        match Command::new("docker")
            .args(&args)
            .envs(demo.compose_env(recording_id, ""))
            .output()
        {
            Ok(o) if o.status.success() => return Ok(()),
            Ok(o) => {
                let err = String::from_utf8_lossy(&o.stderr);
                // Container exited → no point waiting; surface the crash logs now.
                if err.contains("is not running") || err.contains("no such service") {
                    return Err(format!(
                        "{service} exited during boot. Recent logs:\n{}",
                        tail_logs(demo, service)
                    ));
                }
                // otherwise: still booting (connection refused) — keep waiting
            }
            Err(_) => {}
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{service} not healthy within timeout. Recent logs:\n{}",
                tail_logs(demo, service)
            ));
        }
        thread::sleep(Duration::from_secs(2));
    }
}

/// Wait until kafka0 actually accepts connections (cp-kafka logs "Started" well
/// before it is ready). Uses the broker's own CLI over the internal listener.
fn wait_kafka_ready(demo: &Demo, recording_id: &str, timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut args = demo.compose_base_args();
        args.extend(
            [
                "exec",
                "-T",
                // Blank JMX for the CLI: the image sets JMX_PORT=9997 for the
                // BROKER, but every kafka CLI is also a JVM that would try to
                // re-bind 9997 (already held by the broker) and die before
                // contacting it. These overrides apply only to this process.
                "-e",
                "JMX_PORT=",
                "-e",
                "KAFKA_JMX_OPTS=",
                "kafka0",
                "kafka-topics",
                "--bootstrap-server",
                // PLAINTEXT_HOST listener binds 0.0.0.0:9092 → reachable via
                // loopback inside the container (the 29092 listener is bound to
                // the kafka0 interface, not localhost).
                "localhost:9092",
                "--list",
            ]
            .iter()
            .map(|s| s.to_string()),
        );
        let ok = Command::new("docker")
            .args(&args)
            .envs(demo.compose_env(recording_id, ""))
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);
        if ok {
            eprintln!("lifecycle: kafka0 ready");
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("kafka0 not ready within timeout".to_string());
        }
        thread::sleep(Duration::from_secs(3));
    }
}

/// Last ~60 log lines for a service (used to surface boot crashes in the
/// run's failure_reason so the next iteration doesn't need a manual `logs`).
fn tail_logs(demo: &Demo, service: &str) -> String {
    let mut args = demo.compose_base_args();
    args.extend(
        ["logs", "--tail=60", "--no-color", service]
            .iter()
            .map(|s| s.to_string()),
    );
    match Command::new("docker").args(&args).output() {
        Ok(o) => {
            let mut s = String::from_utf8_lossy(&o.stdout).into_owned();
            s.push_str(&String::from_utf8_lossy(&o.stderr));
            s
        }
        Err(e) => format!("(could not fetch logs: {e})"),
    }
}

/// Poll the candidate's `/health` on a host-published port until 200 or timeout.
fn wait_health(port: u16, timeout: Duration) -> Result<(), String> {
    let url = format!("http://127.0.0.1:{port}/health");
    let deadline = Instant::now() + timeout;
    loop {
        let ok = Command::new("curl")
            .args(["-fsS", "-o", "/dev/null", "--max-time", "3", &url])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if ok {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("candidate at {url} not healthy within timeout"));
        }
        thread::sleep(Duration::from_secs(2));
    }
}

/// Wait until at least one object exists under the session's landing prefix
/// and the count stops growing (Vector batch flush settled). Native S3 list —
/// no `mc` container round-trips.
fn wait_s3_objects(recording_id: &str, timeout: Duration) -> Result<(), String> {
    let cfg = crate::s3::S3Config::from_env();
    let deadline = Instant::now() + timeout;
    let mut last = 0usize;
    let mut stable = 0u8;
    loop {
        let count = crate::s3::count_session_objects(&cfg, recording_id).unwrap_or(0);
        if count > 0 && count == last {
            stable += 1;
            if stable >= 2 {
                eprintln!("lifecycle: S3 has {count} landing object(s) for {recording_id}");
                return Ok(());
            }
        } else {
            stable = 0;
        }
        last = count;
        if Instant::now() >= deadline {
            if last > 0 {
                return Ok(());
            }
            return Err(format!(
                "no recording objects appeared in S3 for {recording_id} within timeout"
            ));
        }
        thread::sleep(Duration::from_secs(3));
    }
}

/// Pull the session out of S3 into the canonical
/// `{root}/recordings/{id}/events.jsonl` slot the kernel + renderer read.
/// Compacts the session first if it isn't sealed (manifest absent), then
/// streams the data parts (see `deja-compactor`). The ingest report and the
/// sealing manifest are persisted next to the events file and registered as
/// artifacts; the recording catalog row upserts from the manifest.
fn pull_recording(root: &HarnessRoot, ctx: &StoreCtx, recording_id: &str) -> Result<(), String> {
    let cfg = crate::s3::S3Config::from_env();
    // A recording named by id alone still has to be FOUND. The compactor looks
    // under its own flat layout; the deployed aggregator partitions by date
    // first, so a session it wrote is not there and the pull failed with "no
    // landing objects" for a recording plainly present in the bucket — which is
    // every recording the index offers but has not yet ingested.
    //
    // An unsealed session therefore goes through the prefix scan, which filters
    // by the session id carried in each envelope. That matters beyond finding
    // it: a session spanning two dates is addressed from the shared parent, so
    // the prefix holds other sessions too, and only content can separate them.
    if deja_compactor::read_manifest(&cfg, recording_id)?.is_none() {
        let root_prefix =
            std::env::var("DEJA_RECORDING_ROOT").unwrap_or_else(|_| "landing/v1".to_owned());
        let prefix = deja_compactor::locate_landing_prefix(&cfg, recording_id, &root_prefix)?
            .ok_or_else(|| {
                format!(
                    "recording {recording_id} is not in s3://{}/{root_prefix} — it was never                      landed, or it landed under a different root",
                    cfg.bucket
                )
            })?;
        let source = crate::S3Source {
            path: format!("s3://{}/{prefix}", cfg.bucket),
            region: Some(cfg.region.clone()),
            endpoint: (!cfg.endpoint.trim().is_empty()).then(|| cfg.endpoint.clone()),
        };
        resolve_recording_from_source(root, ctx, &source, Some(recording_id))?;
        return Ok(());
    }
    let dest = crate::scope::TapeSlot::for_write(root, recording_id);
    let (report, manifest) = crate::s3::pull_recording(&cfg, recording_id, &dest)?;
    let gaps: usize = manifest.instances.iter().map(|i| i.gaps.len()).sum();
    let line = format!(
        "ingested {recording_id}: {} landing object(s), {} line(s), {} duplicate(s) dropped → \
         {} event(s), {} correlation(s), {} gap(s), sealed",
        report.landing_objects,
        report.lines_in,
        report.duplicates_dropped,
        report.events_out,
        report.correlations,
        gaps,
    );
    eprintln!("lifecycle: {line}");
    ctx.log("ingest", &line);
    if report.events_out == 0 {
        return Err(format!("recording {recording_id} pulled empty from S3"));
    }
    // Consumer shim: deja-tui / deja-semantic-metrics historically read the
    // JSONL primary at {root}/recording/semantic-events.jsonl. Kafka is the
    // only sink now, so materialize the pulled copy there too.
    let legacy_copy = root.root.join("recording").join("semantic-events.jsonl");
    if let Some(parent) = legacy_copy.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Err(e) = std::fs::copy(&dest, &legacy_copy) {
        eprintln!("lifecycle: semantic-events.jsonl shim copy failed: {e}");
    }
    // Published through the sink, not registered by local path: on k8s this
    // code runs in the Job pod, and a row holding the pod's path answers every
    // later read with ENOENT. The sink uploads in-pod and degrades to the local
    // path under compose — the same contract as every stream artifact.
    let sink = ArtifactSink::from_env();
    let report_path = dest.with_file_name("ingest-report.json");
    if let Err(e) = write_json(&report_path, &report) {
        eprintln!("lifecycle: ingest report write failed: {e}");
    }
    if let Some((uri, bytes)) = sink.publish(ctx.run_id(), "ingest-report.json", &report_path) {
        ctx.artifact_uri(Some(recording_id), "ingest_report", &uri, Some(bytes));
    }
    let manifest_path = dest.with_file_name("manifest.json");
    if let Err(e) = write_json(&manifest_path, &manifest) {
        eprintln!("lifecycle: manifest copy write failed: {e}");
    }
    if let Some((uri, bytes)) =
        sink.publish(ctx.run_id(), "recording-manifest.json", &manifest_path)
    {
        ctx.artifact_uri(Some(recording_id), "manifest", &uri, Some(bytes));
    }
    let bytes = std::fs::metadata(&dest).ok().map(|m| m.len() as i64);
    ctx.recording(
        recording_id,
        dest.to_str(),
        Some(report.events_out as i64),
        Some(report.correlations as i64),
        bytes,
        serde_json::to_value(&manifest).ok().as_ref(),
    );
    Ok(())
}

/// Pull a replay's recording out of an arbitrary bucket/prefix in the DEPLOYED
/// aggregator layout (see `s3::pull_recording_from_prefix`) and register it
/// exactly like the session-layout pull. The scan seals what it read, so the
/// next pull of the same recording takes the manifest fast path instead of
/// repeating it. Returns the resolved recording (session) id.
///
/// An explicit, already-ingested session reuses the on-disk events file; a
/// filterless spec always scans (the session isn't known until then).
fn resolve_recording_from_source(
    root: &HarnessRoot,
    ctx: &StoreCtx,
    source: &crate::S3Source,
    wanted: Option<&str>,
) -> Result<String, String> {
    if let Some(id) = wanted {
        if crate::scope::TapeSlot::is_materialized(root, id) {
            eprintln!("lifecycle: recording {id} already ingested; reusing");
            return Ok(id.to_owned());
        }
    }
    let (cfg, prefix) = source.to_config()?;
    let (report, resolved, seen) =
        crate::s3::pull_recording_from_prefix(&cfg, &prefix, wanted, |sid| {
            crate::scope::TapeSlot::for_write(root, sid)
        })?;
    if seen.len() > 1 {
        let others = seen
            .iter()
            .filter(|(sid, _)| sid != &resolved)
            .map(|(sid, n)| format!("{sid} ({n})"))
            .collect::<Vec<_>>()
            .join(", ");
        ctx.log(
            "ingest",
            &format!("other sessions under this prefix: {others}"),
        );
    }
    let line = format!(
        "ingested {resolved} from {}: {} object(s), {} line(s), {} duplicate(s) dropped → \
         {} event(s), {} correlation(s) (prefix scan, {})",
        report.prefix,
        report.landing_objects,
        report.lines_in,
        report.duplicates_dropped,
        report.events_out,
        report.correlations,
        if report.sealed {
            "sealed for the next run"
        } else {
            "NOT sealed — the next run rescans"
        },
    );
    eprintln!("lifecycle: {line}");
    ctx.log("ingest", &line);
    if report.events_out == 0 {
        return Err(format!(
            "session {resolved} pulled empty from {}",
            report.prefix
        ));
    }
    let dest = crate::scope::TapeSlot::for_write(root, &resolved);
    // Same consumer shim as the session-layout pull (deja-tui / metrics).
    let legacy_copy = root.root.join("recording").join("semantic-events.jsonl");
    if let Some(parent) = legacy_copy.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Err(e) = std::fs::copy(&dest, &legacy_copy) {
        eprintln!("lifecycle: semantic-events.jsonl shim copy failed: {e}");
    }
    let report_path = dest.with_file_name("ingest-report.json");
    if let Err(e) = write_json(&report_path, &report) {
        eprintln!("lifecycle: ingest report write failed: {e}");
    }
    // Sink-published for the same reason as the session-layout pull above: a
    // pod-local path in the artifacts table is a read that can never succeed.
    if let Some((uri, bytes)) =
        ArtifactSink::from_env().publish(ctx.run_id(), "ingest-report.json", &report_path)
    {
        ctx.artifact_uri(Some(&resolved), "ingest_report", &uri, Some(bytes));
    }
    let bytes = std::fs::metadata(&dest).ok().map(|m| m.len() as i64);
    ctx.recording(
        &resolved,
        dest.to_str(),
        Some(report.events_out as i64),
        Some(report.correlations as i64),
        bytes,
        None,
    );
    Ok(resolved)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // tests panic on failure by design
mod tests {
    use super::*;
    use crate::scope::{RunScope, ScopedRecording, TapeSlot};
    use crate::{CandidateSpec, RunSpec};

    /// Row identity is read from the schema at run time (see
    /// `register_table_identity`); tests stand in for that read with the same
    /// shape the statement returns. Idempotent, so every test that needs row
    /// keys can call it without ordering assumptions.
    fn register_test_schema_identity() {
        deja::register_table_identity([
            // The real schema keys: migration 2024-06-03-090859 made both of
            // these composite, which is why the row-key grammar has to carry
            // several columns.
            (
                "payment_attempt".to_owned(),
                vec!["attempt_id".to_owned(), "merchant_id".to_owned()],
            ),
            (
                "payment_intent".to_owned(),
                vec!["payment_id".to_owned(), "merchant_id".to_owned()],
            ),
            (
                "payment_methods".to_owned(),
                vec!["payment_method_id".to_owned()],
            ),
            (
                "merchant_account".to_owned(),
                vec!["merchant_id".to_owned()],
            ),
            (
                "merchant_key_store".to_owned(),
                vec!["merchant_id".to_owned()],
            ),
            ("business_profile".to_owned(), vec!["profile_id".to_owned()]),
            (
                "merchant_connector_account".to_owned(),
                vec!["merchant_connector_id".to_owned()],
            ),
            ("customers".to_owned(), vec!["customer_id".to_owned()]),
            ("users".to_owned(), vec!["user_id".to_owned()]),
            ("address".to_owned(), vec!["address_id".to_owned()]),
            ("configs".to_owned(), vec!["key".to_owned()]),
        ]);
    }
    /// Write a tape into the canonical slot and open it at `scope`.
    fn recording(body: &str, scope: RunScope) -> (tempfile::TempDir, ScopedRecording) {
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        let path = TapeSlot::for_write(&root, "rec-1");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, body).unwrap();
        let opened = ScopedRecording::open(&root, "rec-1", scope).unwrap();
        (dir, opened)
    }

    /// Seeding scopes to the run's test cases and always keeps ambient
    /// (uncorrelated) preconditions, which are shared across cases. This used to
    /// be a `select_seed_correlations` pass over every correlation in the
    /// session; the scope now applies at the reader, so the property lives here.
    #[test]
    fn seed_scope_keeps_the_listed_cases_and_all_ambient_preconditions() {
        let unfiltered = RunScope::from_filter(None);
        for id in ["pay-1", "health-1"] {
            assert!(unfiltered.contains(Some(id)), "no filter seeds everything");
        }
        assert!(RunScope::from_filter(Some(&[])).contains(Some("health-1")));

        let scope = RunScope::from_filter(Some(&["pay-1".to_string(), "pay-2".to_string()]));
        assert!(scope.contains(Some("pay-1")));
        assert!(scope.contains(Some("pay-2")));
        assert!(!scope.contains(Some("health-1")), "health checks drop out");
        assert!(
            scope.contains(None),
            "ambient/uncorrelated preconditions are shared and always seeded"
        );
    }

    fn extract_ctx_artifact_kinds(source: &str) -> std::collections::BTreeSet<String> {
        let mut kinds = std::collections::BTreeSet::new();
        // Both StoreCtx registration forms — the local path form and the
        // sink-published uri form — pass the kind as the 2nd arg. A call whose
        // kind is a VARIABLE (the REPLAY_STREAM_ARTIFACTS loop) is skipped here;
        // those kinds are added from the const below. (Markers are built with
        // concat! so this scanner never matches its own source text.)
        for marker in [
            concat!("ctx", ".artifact("),
            concat!("ctx", ".artifact_uri("),
        ] {
            for call in source.split(marker).skip(1) {
                let Some(first_comma) = call.find(',') else {
                    continue;
                };
                let after = call[first_comma + 1..].trim_start();
                if !after.starts_with('"') {
                    continue; // variable kind (loop) — not a literal
                }
                let after_quote = &after[1..];
                let Some(end) = after_quote.find('"') else {
                    continue;
                };
                kinds.insert(after_quote[..end].to_owned());
            }
        }
        // The stream artifacts are published from the const via a variable kind.
        kinds.extend(REPLAY_STREAM_ARTIFACTS.iter().map(|(k, _)| (*k).to_owned()));
        kinds
    }

    fn extract_artifact_constraint_kinds(sql: &str) -> std::collections::BTreeSet<String> {
        let artifact_scope = sql
            .find("CREATE TABLE artifacts")
            .or_else(|| sql.find("ADD CONSTRAINT artifacts_kind_check"))
            .expect("migration should define or replace the artifact kind constraint");
        let scoped_sql = &sql[artifact_scope..];
        let kind_in = scoped_sql
            .find("kind IN")
            .expect("artifact migration should constrain artifact kind IN");
        let after_kind_in = &scoped_sql[kind_in..];
        let open = after_kind_in
            .find('(')
            .expect("artifact kind constraint should open literal list")
            + 1;
        let after_open = &after_kind_in[open..];
        let close = after_open
            .find(')')
            .expect("artifact kind constraint should close literal list");
        let literal_list = &after_open[..close];
        let mut kinds = std::collections::BTreeSet::new();
        for (idx, part) in literal_list.split('\'').enumerate() {
            if idx % 2 == 1 {
                kinds.insert(part.to_owned());
            }
        }
        kinds
    }

    #[test]
    fn artifact_kind_constraints_cover_lifecycle_registrations() {
        let lifecycle_source = include_str!("mod.rs");
        let registered = extract_ctx_artifact_kinds(lifecycle_source);
        assert_eq!(
            registered,
            [
                "call_ledger",
                "events",
                "http_diffs",
                "ingest_report",
                "lookup_table",
                "manifest",
                "observed",
                "record_graph",
                "scorecard",
                "seed_certificate",
                "visualization_html",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect::<std::collections::BTreeSet<_>>(),
            "test must track every StoreCtx::artifact kind literal the lifecycle can write",
        );

        let migrations = [
            include_str!("../../../deja-store/migrations/0001_init.sql"),
            include_str!("../../../deja-store/migrations/0002_artifact_kinds.sql"),
            include_str!("../../../deja-store/migrations/0003_session_manifests.sql"),
            include_str!("../../../deja-store/migrations/0004_call_ledger_artifact.sql"),
            include_str!("../../../deja-store/migrations/0005_seed_certificate_artifact.sql"),
            include_str!("../../../deja-store/migrations/0006_record_graph_artifact.sql"),
        ];
        let allowed_by_step = migrations
            .into_iter()
            .map(extract_artifact_constraint_kinds)
            .collect::<Vec<_>>();
        for window in allowed_by_step.windows(2) {
            assert!(
                window[1].is_superset(&window[0]),
                "artifact kind migrations must be monotonic so upgraded DBs keep accepting existing rows",
            );
        }
        let final_allowed = allowed_by_step
            .last()
            .expect("migration set should include a final artifact kind constraint");
        assert!(
            registered.is_subset(final_allowed),
            "final artifact kind constraint must accept all lifecycle-registered artifact kinds; missing {:?}",
            registered.difference(final_allowed).collect::<Vec<_>>()
        );
    }

    fn certificate_seed_entry(boundary: &str, key: &str) -> deja::SeedEntry {
        deja::SeedEntry {
            boundary: boundary.to_owned(),
            key: key.to_owned(),
            value: serde_json::json!({"seed": key}),
            image: None,
            method: None,
            origin: deja::SeedOrigin::Recording,
            source_sequence: 0,
        }
    }

    #[test]
    fn seed_certificate_summarizes_materialized_skipped_failed_and_readback_states() {
        let corr = Some("cycle36b".to_owned());
        let redis = certificate_seed_entry("redis", "settlement_rate_default");
        let db = certificate_seed_entry(
            "db",
            &deja::StateKey::DbRow {
                table: "users".to_owned(),
                key: vec![("user_id".to_owned(), "user_123".to_owned())],
            }
            .to_wire(),
        );
        let storage = certificate_seed_entry("storage", "object://unsupported");
        let mut certificate = SeedCertificate::new("rec-1", "run-1", true);

        certificate.push(SeedCertificateEntry::new(
            &corr,
            &redis,
            Some("cycle36b:settlement_rate_default".to_owned()),
            None,
            SeedMaterializationStatus::Materialized,
            SeedReadback::matched(serde_json::json!("0.10"), serde_json::json!("0.10")),
        ));
        certificate.push(SeedCertificateEntry::new(
            &corr,
            &redis,
            Some("cycle36b:settlement_rate_premium".to_owned()),
            None,
            SeedMaterializationStatus::Materialized,
            SeedReadback::mismatched(
                serde_json::json!({"utf8": "0.20", "len": 4}),
                serde_json::json!({"utf8": "0.30", "len": 4}),
                "redis GET returned a different value after SET",
            ),
        ));
        certificate.push(SeedCertificateEntry::new(
            &corr,
            &db,
            None,
            Some(deja::db_schema_for("cycle36b")),
            SeedMaterializationStatus::Skipped,
            SeedReadback::not_run("db seeding disabled by DEJA_SEED_DB=0"),
        ));
        certificate.push(SeedCertificateEntry::new(
            &corr,
            &db,
            None,
            Some(deja::db_schema_for("cycle36b")),
            SeedMaterializationStatus::Failed,
            SeedReadback::error("seed_db users exited 1"),
        ));
        certificate.push(SeedCertificateEntry::new(
            &corr,
            &db,
            None,
            Some(deja::db_schema_for("cycle36b")),
            SeedMaterializationStatus::Materialized,
            SeedReadback::missing(
                serde_json::json!({"rows": 1, "table": "users", "kind": "row"}),
                "db seed readback found no row matching the materialized seed image",
            ),
        ));
        certificate.push(SeedCertificateEntry::new(
            &corr,
            &storage,
            None,
            None,
            SeedMaterializationStatus::Unsupported,
            SeedReadback::unsupported("seed materialization only supports redis and db boundaries"),
        ));

        // A not-a-precondition read joins the entries but NOT `planned`: it was
        // never work, it is accounting — planned + not_preconditions = every
        // state-read the planner saw.
        certificate.push(SeedCertificateEntry::not_a_precondition(
            &Some("c1".to_owned()),
            "db",
            "deja:state:v1:db_row:7573657273:6964:31",
            deja::NotPreconditionReason::ReadAfterWrite,
        ));

        assert_eq!(
            certificate.summary,
            SeedCertificateSummary {
                planned: 6,
                materialized: 3,
                skipped: 1,
                failed: 1,
                unsupported: 1,
                not_preconditions: 1,
                readback_matched: 1,
                readback_missing: 1,
                readback_mismatched: 1,
                readback_errors: 1,
                readback_not_run: 2,
                // These entries were pushed without a mechanism (the split is
                // attached by `seed_db` itself); the per-table block is
                // exercised by `seed_certificate_accounts_the_materialization_mechanism_per_table`.
                db_rows_by_table: BTreeMap::new(),
            },
            "the certificate summary must distinguish materialization outcomes and readback evidence"
        );
        let json = serde_json::to_value(&certificate).expect("certificate serializes");
        assert_eq!(json["type"], SeedCertificate::KIND);
        assert_eq!(json["entries"][0]["materialization"], "materialized");
        assert_eq!(json["entries"][1]["readback"]["status"], "mismatched");
        assert_eq!(json["entries"][2]["materialization"], "skipped");
        assert_eq!(json["entries"][3]["readback"]["status"], "error");
    }

    /// The per-table mechanism block answers, from the artifact alone, whether
    /// the tape's physical row images reached the seeder and which rows they
    /// materialized. That question previously had no answer outside the pod —
    /// the recording is deliberately not exposed through any API — so a
    /// certificate reporting `failed` could not say whether the wire image was
    /// missing, rejected, or simply unused. A FAILED entry carries its split
    /// too, which is the case that matters.
    #[test]
    fn seed_certificate_accounts_the_materialization_mechanism_per_table() {
        let corr = Some("c1".to_owned());
        let db = certificate_seed_entry(
            "db",
            &deja::StateKey::DbQuery {
                table: "payment_attempt".to_owned(),
                fingerprint: "f439efeb0ad8b2f4".to_owned(),
            }
            .to_wire(),
        );
        let mut certificate = SeedCertificate::new("rec-1", "run-1", true);

        certificate.push(
            SeedCertificateEntry::new(
                &corr,
                &db,
                None,
                None,
                SeedMaterializationStatus::Materialized,
                SeedReadback::matched(
                    serde_json::json!({"rows": 2}),
                    serde_json::json!({"rows": 2}),
                ),
            )
            .with_mechanism(Some(SeedEntryMechanism {
                table: "payment_attempt".to_owned(),
                rows: 2,
                via_copy: 2,
                via_insert: 0,
                physical_image_gap: None,
            })),
        );
        certificate.push(
            SeedCertificateEntry::new(
                &corr,
                &db,
                None,
                None,
                SeedMaterializationStatus::Failed,
                SeedReadback::error("could not render an insert"),
            )
            .with_mechanism(Some(SeedEntryMechanism {
                table: "payment_attempt".to_owned(),
                rows: 1,
                via_copy: 0,
                via_insert: 1,
                physical_image_gap: Some("no physical image".to_owned()),
            })),
        );

        let table = certificate
            .summary
            .db_rows_by_table
            .get("payment_attempt")
            .expect("the failing table must appear in the per-table accounting");
        assert_eq!(
            table,
            &SeedTableMechanism {
                via_copy: 2,
                via_insert: 1,
                entries_with_physical_image: 1,
                entries_without_physical_image: 1,
            },
            "rows must be attributed to the mechanism that rendered them, and entries counted by whether a physical image was available"
        );
        let json = serde_json::to_value(&certificate).expect("certificate serializes");
        assert_eq!(
            json["summary"]["db_rows_by_table"]["payment_attempt"]["via_copy"],
            2
        );
        assert_eq!(
            json["entries"][1]["mechanism"]["physical_image_gap"], "no physical image",
            "a failed entry must still name why it had no physical image to use"
        );
    }

    #[test]
    fn seed_certificate_preserves_db_row_and_query_seed_entries_when_db_seeding_is_skipped() {
        let corr = Some("cycle36b".to_owned());
        let user_id = "user_123";
        let query_key = deja::StateKey::DbQuery {
            table: "users".to_owned(),
            fingerprint: "find-user-by-email".to_owned(),
        }
        .to_wire();
        let row_key = deja::StateKey::DbRow {
            table: "users".to_owned(),
            key: vec![("user_id".to_owned(), user_id.to_owned())],
        }
        .to_wire();
        let query_result_image = deja::db::DbRowImage::new(
            "users",
            vec![
                deja::db::DbColumnImage {
                    wire: None,
                    name: "user_id".into(),
                    type_oid: Some(25),
                    type_name: Some("text".into()),
                    nullable: Some(false),
                    value: serde_json::json!(user_id),
                },
                deja::db::DbColumnImage {
                    wire: None,
                    name: "email".into(),
                    type_oid: Some(25),
                    type_name: Some("text".into()),
                    nullable: Some(false),
                    value: serde_json::json!("alice@example.com"),
                },
            ],
        )
        .to_value();
        let rmw_pre_image = deja::db::DbRowImage::new(
            "users",
            vec![
                deja::db::DbColumnImage {
                    wire: None,
                    name: "user_id".into(),
                    type_oid: Some(25),
                    type_name: Some("text".into()),
                    nullable: Some(false),
                    value: serde_json::json!(user_id),
                },
                deja::db::DbColumnImage {
                    wire: None,
                    name: "name".into(),
                    type_oid: Some(25),
                    type_name: Some("text".into()),
                    nullable: Some(false),
                    value: serde_json::json!("before-rmw"),
                },
            ],
        )
        .to_value();
        let rmw_post_image = deja::db::DbRowImage::new(
            "users",
            vec![
                deja::db::DbColumnImage {
                    wire: None,
                    name: "user_id".into(),
                    type_oid: Some(25),
                    type_name: Some("text".into()),
                    nullable: Some(false),
                    value: serde_json::json!(user_id),
                },
                deja::db::DbColumnImage {
                    wire: None,
                    name: "name".into(),
                    type_oid: Some(25),
                    type_name: Some("text".into()),
                    nullable: Some(false),
                    value: serde_json::json!("after-rmw"),
                },
            ],
        )
        .to_value();
        let query_envelope = serde_json::json!({
            "version": 1,
            "result": "Ok",
            "value": {
                "user_id": user_id,
                "merchant_id": "merch_456",
                "email": "alice@example.com"
            },
            "type_name": "diesel_models::user::User"
        });
        let query_event: deja::BoundaryEvent = serde_json::from_value(serde_json::json!({
            "global_sequence": 50,
            "request_sequence": 1,
            "correlation_id": corr.as_deref().unwrap(),
            "timestamp_ns": 1783029410812345678_u64,
            "boundary": "db",
            "trait_name": "diesel_models::query::generics",
            "method_name": "generic_find_one_core",
            "call_file": "crates/diesel_models/src/query/generics.rs",
            "call_line": 767,
            "call_column": 25,
            "request": {
                "operation": "generic_find_one_core",
                "table": "users",
                "sql": "SELECT * FROM \"users\" WHERE \"email\" = $1",
                "inputs": ["alice@example.com"]
            },
            "args": {
                "operation": "generic_find_one_core",
                "table": "users",
                "sql": "SELECT * FROM \"users\" WHERE \"email\" = $1",
                "inputs": ["alice@example.com"]
            },
            "result": query_envelope,
            "response": query_envelope,
            "result_image": query_result_image.clone(),
            "is_error": false,
            "duration_us": 0,
            "event_schema_version": deja::CURRENT_EVENT_SCHEMA_VERSION,
            "provenance": "recorded",
            "recon": "lossless",
            "replay_strategy": "execute",
            "read_set": [query_key.clone()],
            "write_set": []
        }))
        .expect("db read event parses");
        let rmw_event: deja::BoundaryEvent = serde_json::from_value(serde_json::json!({
            "global_sequence": 51,
            "request_sequence": 2,
            "correlation_id": corr.as_deref().unwrap(),
            "timestamp_ns": 1783029410812345679_u64,
            "boundary": "db",
            "trait_name": "diesel_models::query::generics",
            "method_name": "generic_update_with_results",
            "call_file": "crates/diesel_models/src/query/generics.rs",
            "call_line": 900,
            "call_column": 25,
            "request": {
                "operation": "generic_update_with_results",
                "table": "users",
                "sql": "UPDATE \"users\" SET \"name\" = $1 WHERE \"user_id\" = $2 RETURNING *",
                "inputs": ["after-rmw", user_id]
            },
            "args": {
                "operation": "generic_update_with_results",
                "table": "users",
                "sql": "UPDATE \"users\" SET \"name\" = $1 WHERE \"user_id\" = $2 RETURNING *",
                "inputs": ["after-rmw", user_id]
            },
            "result": {
                "version": 1,
                "result": "Ok",
                "value": {
                    "user_id": user_id,
                    "name": "after-rmw"
                },
                "type_name": "diesel_models::user::User"
            },
            "response": {
                "version": 1,
                "result": "Ok",
                "value": {
                    "user_id": user_id,
                    "name": "after-rmw"
                },
                "type_name": "diesel_models::user::User"
            },
            "result_image": rmw_post_image,
            "pre_image": rmw_pre_image.clone(),
            "is_error": false,
            "duration_us": 0,
            "event_schema_version": deja::CURRENT_EVENT_SCHEMA_VERSION,
            "provenance": "recorded",
            "recon": "lossless",
            "replay_strategy": "execute",
            "read_set": [row_key.clone()],
            "write_set": [row_key.clone()]
        }))
        .expect("db read-modify-write event parses");
        let plan = deja::build_seed_plan(&[query_event, rmw_event], corr.as_deref());
        let mut entries = plan.iter().collect::<Vec<_>>();
        entries.sort_by_key(|entry| seed_materialization_priority(entry));
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.key.as_str())
                .collect::<Vec<_>>(),
            vec![row_key.as_str(), query_key.as_str()],
            "the source seed plan must keep RMW row images ahead of read query images"
        );
        assert_eq!(
            entries[0].image.as_ref(),
            Some(&rmw_pre_image),
            "self-RMW seeds must certify the pre-image, not the post-write result image"
        );
        assert_eq!(
            entries[1].image.as_ref(),
            Some(&query_result_image),
            "plain DB read seeds must carry the producer result_image into the certificate path"
        );

        let schema = corr.as_deref().map(deja::db_schema_for);
        let mut certificate = SeedCertificate::new("rec-1", "run-1", false);
        for entry in entries {
            certificate.push(SeedCertificateEntry::new(
                &corr,
                entry,
                None,
                schema.clone(),
                SeedMaterializationStatus::Skipped,
                SeedReadback::not_run("db seeding disabled by DEJA_SEED_DB=0"),
            ));
        }

        assert_eq!(certificate.summary.planned, 2);
        assert_eq!(certificate.summary.skipped, 2);
        assert_eq!(certificate.summary.readback_not_run, 2);
        assert_eq!(
            certificate
                .entries
                .iter()
                .map(|entry| entry.logical_key.as_str())
                .collect::<Vec<_>>(),
            vec![row_key.as_str(), query_key.as_str()],
            "certificates must keep exact DB row preconditions ahead of query fallback snapshots"
        );
        for entry in &certificate.entries {
            assert_eq!(entry.correlation_id, corr);
            assert_eq!(entry.boundary, "db");
            assert_eq!(entry.physical_key, None);
            assert_eq!(entry.db_schema, schema);
            assert_eq!(entry.origin, deja::SeedOrigin::Recording);
            assert_eq!(entry.materialization, SeedMaterializationStatus::Skipped);
            assert_eq!(entry.readback.status, SeedReadbackStatus::NotRun);
        }
    }

    #[test]
    fn seed_certificate_redis_readback_strips_only_the_cli_transport_linefeed() {
        assert_eq!(strip_redis_cli_terminator(b"0.10\n"), b"0.10");
        assert_eq!(strip_redis_cli_terminator(b"line\n\n"), b"line\n");
        assert_eq!(strip_redis_cli_terminator(b"already-raw"), b"already-raw");
        assert_eq!(strip_redis_cli_terminator(b"binary\0\n"), b"binary\0");
        assert_eq!(strip_redis_cli_terminator(b""), b"");
    }

    #[test]
    fn seed_certificate_db_readback_sql_separates_full_row_and_key_match_predicates() {
        let row: serde_json::Map<String, serde_json::Value> =
            serde_json::from_value(serde_json::json!({
                "user_id": "user_123",
                "email": "alice@example.com",
                "merchant_id": "merch_456"
            }))
            .expect("row object");
        let image =
            DbRowImage::from_json_object("users", &row, &DbCatalog::default()).expect("row image");
        let key_filter = DbRowFilter {
            key: vec![("user_id".to_owned(), "user_123".to_owned())],
        };

        let full_row_sql =
            build_count_sql(Some("deja_cycle36b"), &image, None).expect("full-row count SQL");
        assert!(full_row_sql.starts_with("SELECT COUNT(*) FROM \"deja_cycle36b\".\"users\""));
        assert!(full_row_sql.contains("\"user_id\" IS NOT DISTINCT FROM 'user_123'"));
        assert!(full_row_sql.contains("\"email\" IS NOT DISTINCT FROM 'alice@example.com'"));
        assert!(full_row_sql.contains("\"merchant_id\" IS NOT DISTINCT FROM 'merch_456'"));

        let key_sql = build_count_sql(Some("deja_cycle36b"), &image, Some(&key_filter))
            .expect("key count SQL");
        assert!(key_sql.contains("\"user_id\" IS NOT DISTINCT FROM 'user_123'"));
        assert!(
            !key_sql.contains("alice@example.com") && !key_sql.contains("merch_456"),
            "the key readback query must isolate key-exists evidence from full-row equality; got: {key_sql}"
        );
    }

    static DEMO_REPLAY_SHARED_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    struct EnvVarRestore {
        key: &'static str,
        original: Option<std::ffi::OsString>,
    }

    impl EnvVarRestore {
        fn unset(key: &'static str) -> Self {
            let original = std::env::var_os(key);
            std::env::remove_var(key);
            Self { key, original }
        }
    }

    impl Drop for EnvVarRestore {
        fn drop(&mut self) {
            match &self.original {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }

    fn run_with_workload(workload: serde_json::Value) -> Run {
        Run {
            run_id: "r1".into(),
            spec: RunSpec {
                mode: RunMode::Record,
                candidate_spec: CandidateSpec::PrebuiltImage { image: "x".into() },
                candidate_repo: None,
                recording_id: None,
                s3_source: None,
                correlation_filter: None,
                workload,
            },
            status: RunStatus::Pending,
            recording_id: None,
            candidate_image: None,
            failure_reason: None,
            stage: None,
            step: 0,
            steps_total: 0,
            stage_updated_ms: 0,
        }
    }

    #[test]
    fn isolated_parallel_replays_use_tail_ids_and_preserve_shared_opt_out() {
        let _env_lock = DEMO_REPLAY_SHARED_ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _env_restore = EnvVarRestore::unset("DEMO_REPLAY_SHARED");

        let shared_port_guard = TcpListener::bind("127.0.0.1:0").unwrap();
        let shared_port = shared_port_guard.local_addr().unwrap().port();
        let demo = Demo {
            compose_base: "compose.yml".into(),
            compose_overlay: "compose.deja.yml".into(),
            project: "deja-demo-shared".into(),
            replay_port: shared_port,
            kernel_bin: "deja-kernel".into(),
            topic: "recording-events".into(),
            harness_state: "/tmp/deja-state".into(),
            candidate_image: None,
            ucs_profile: false,
        };

        let replay_a = demo.isolated_for_replay("run-20260702feedface00000001");
        let replay_b = demo.isolated_for_replay("run-20260702feedface00000002");

        assert_eq!(replay_a.project, "deja-run-00000001");
        assert_eq!(replay_b.project, "deja-run-00000002");
        assert_ne!(replay_a.project, replay_b.project);
        assert_ne!(replay_a.project, demo.project);
        assert_ne!(replay_b.project, demo.project);
        assert_ne!(replay_a.replay_port, demo.replay_port);
        assert_ne!(replay_b.replay_port, demo.replay_port);
        assert_ne!(
            replay_a.replay_port, replay_b.replay_port,
            "successful per-run allocations must not collapse parallel replays onto one host port"
        );

        std::env::set_var("DEMO_REPLAY_SHARED", "1");
        let shared_replay = demo.isolated_for_replay("run-20260702feedface00000003");

        assert_eq!(shared_replay.project, demo.project);
        assert_eq!(shared_replay.replay_port, demo.replay_port);
    }

    // -- the correlation backstop --------------------------------------------

    /// A replay run of `recording_id`, with whatever filter the caller sent.
    fn replay_run(recording_id: &str, filter: Option<Vec<String>>) -> Run {
        Run {
            run_id: "run-backstop".into(),
            spec: RunSpec {
                mode: RunMode::Replay,
                candidate_spec: CandidateSpec::PrebuiltImage { image: "x".into() },
                candidate_repo: None,
                recording_id: Some(recording_id.to_owned()),
                s3_source: None,
                correlation_filter: filter,
                workload: serde_json::Value::Null,
            },
            status: RunStatus::Pending,
            recording_id: Some(recording_id.to_owned()),
            candidate_image: None,
            failure_reason: None,
            stage: None,
            step: 0,
            steps_total: 0,
            stage_updated_ms: 0,
        }
    }

    /// A tape of `n` correlations, one boundary event each, in the order given.
    /// Written through the same door an ingest writes through.
    fn tape_of(root: &HarnessRoot, recording_id: &str, correlations: &[String]) {
        let path = crate::scope::TapeSlot::for_write(root, recording_id);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let lines: Vec<String> = correlations
            .iter()
            .enumerate()
            .map(|(i, correlation)| {
                serde_json::json!({
                    "record_kind": "boundary_event",
                    "global_sequence": i as u64 + 1,
                    "request_sequence": 0,
                    "correlation_id": correlation,
                    "timestamp_ns": 0,
                    "recording_run_id": recording_id,
                    "boundary": "http_incoming",
                    "trait_name": "T",
                    "method_name": "m",
                    "call_file": "x.rs",
                    "call_line": 1,
                    "call_column": 0,
                    "request": null,
                    "args": {},
                    "response": null,
                    "result": "v",
                    "is_error": false,
                    "duration_us": 0,
                    "event_schema_version": deja::CURRENT_EVENT_SCHEMA_VERSION,
                    "provenance": "recorded",
                    "recon": "lossless",
                    "replay_strategy": "substitute",
                    "graph_node_id": null,
                })
                .to_string()
            })
            .collect();
        std::fs::write(&path, lines.join("\n") + "\n").unwrap();
    }

    /// Ids that sort the OPPOSITE way to the traffic, so "the first hundred"
    /// cannot accidentally pass by sorting.
    fn descending_ids(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("c-{:04}", n - i)).collect()
    }

    // -- the recording-completeness gate ------------------------------------

    /// Write an ingest report beside the tape, exactly where a pull puts it.
    fn ingest_report_of(root: &HarnessRoot, recording_id: &str, completeness: serde_json::Value) {
        let path = crate::scope::TapeSlot::ingest_report_path(root, recording_id);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            serde_json::to_vec(&serde_json::json!({
                "prefix": "s3://b/p",
                "lines_in": 16258,
                "events_out": 16175,
                "correlations": 73,
                "sealed": false,
                "markers_dropped": 83,
                "completeness": completeness,
            }))
            .unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn a_replay_against_a_still_landing_recording_is_refused() {
        // THE defect. Run rp-sbx-1868bb92e9-…-0812064511654 scored 9/44 against
        // a prefix of a recording that later held 87 correlations, and nothing
        // in the verdict said so. The ingest already knew — it reported
        // `sealed: false` with 83 markers it threw away — and nobody acted.
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        ingest_report_of(
            &root,
            "rec",
            serde_json::json!({
                "state": "still_landing",
                "reason": "instance(s) router-h-1 emitted checkpoints but no `eof`",
            }),
        );

        let err = admit_against_recording(&root, &StoreCtx::disabled("run-1"), "rec")
            .expect_err("a run against a recording still being written must not be admitted");
        assert!(err.contains("not complete"), "{err}");
        assert!(err.contains("router-h-1"), "names the cause: {err}");
        assert!(
            err.contains("DEJA_ALLOW_INCOMPLETE_RECORDING"),
            "a refusal has to say how to proceed on purpose: {err}"
        );
    }

    #[test]
    fn an_explicit_opt_in_admits_the_prefix_run() {
        // Refusing is safer but must not be a wall: a caller who KNOWS the
        // recording is still in flight can ask for the partial read.
        let ctx = StoreCtx::disabled("run-1");
        admit_with_policy(
            TapeCompleteness::Incomplete("no `eof` yet".to_owned()),
            &ctx,
            "rec",
            true,
        )
        .expect("the opt-in admits the run");
        admit_with_policy(
            TapeCompleteness::Incomplete("no `eof` yet".to_owned()),
            &ctx,
            "rec",
            false,
        )
        .expect_err("without it, the same recording is refused");
    }

    #[test]
    fn a_prefix_run_reports_inconclusive_rather_than_a_clean_verdict() {
        // The other half of the opt-in: admitted is not the same as believed.
        // `provisional_reason` is what turns the verdict into `inconclusive`
        // and stamps the run's stored result — a pass/fail here would be the
        // original bug wearing a permission slip.
        assert_eq!(
            TapeCompleteness::Incomplete("no `eof` yet".to_owned()).provisional_reason(),
            Some("no `eof` yet"),
        );
        assert_eq!(
            TapeCompleteness::Complete("sealed session".to_owned()).provisional_reason(),
            None,
        );
    }

    #[test]
    fn a_complete_recording_is_admitted_without_a_stamp() {
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();

        ingest_report_of(
            &root,
            "sealed-rec",
            serde_json::json!({ "state": "sealed" }),
        );
        assert_eq!(
            tape_completeness(&root, "sealed-rec"),
            TapeCompleteness::Complete("sealed session".to_owned()),
        );
        admit_against_recording(&root, &StoreCtx::disabled("run-1"), "sealed-rec").unwrap();

        ingest_report_of(
            &root,
            "eof-rec",
            serde_json::json!({ "state": "eof_observed", "instances": 2 }),
        );
        let completeness = tape_completeness(&root, "eof-rec");
        assert!(completeness.provisional_reason().is_none());
        assert!(matches!(completeness, TapeCompleteness::Complete(ref how) if how.contains('2')));
        admit_against_recording(&root, &StoreCtx::disabled("run-1"), "eof-rec").unwrap();
    }

    #[test]
    fn a_tape_with_no_ingest_report_is_unproven_not_refused() {
        // A tape pulled by an older build carries no completeness. That is the
        // ABSENCE of evidence, not evidence the recording was short — refusing
        // would strand every already-materialized tape on the box. It is said
        // out loud and the run proceeds.
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        let completeness = tape_completeness(&root, "rec");
        assert!(matches!(completeness, TapeCompleteness::Unproven(_)));
        assert!(
            completeness.provisional_reason().is_none(),
            "unproven must not masquerade as proven-incomplete"
        );
        admit_against_recording(&root, &StoreCtx::disabled("run-1"), "rec")
            .expect("an unverified tape is not a refusal");

        // A report from before completeness accounting lands in the same place.
        let path = crate::scope::TapeSlot::ingest_report_path(&root, "old");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, br#"{"lines_in":10,"sealed":false}"#).unwrap();
        assert!(matches!(
            tape_completeness(&root, "old"),
            TapeCompleteness::Unproven(_)
        ));
    }

    #[test]
    fn the_gate_reads_the_completeness_the_ingest_actually_writes() {
        // The two halves have to agree on the wire shape, or this whole gate is
        // a producer/consumer split of its own. So the fixture here is the real
        // `IngestReport`, serialized — not a hand-written JSON blob.
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        for (recording_id, completeness, expect_refusal) in [
            ("sealed", crate::s3::Completeness::Sealed, false),
            (
                "eof",
                crate::s3::Completeness::EofObserved { instances: 1 },
                false,
            ),
            (
                "landing",
                crate::s3::Completeness::StillLanding {
                    reason: "no `eof` from router-h-1".to_owned(),
                },
                true,
            ),
        ] {
            let report = crate::s3::IngestReport {
                prefix: "s3://b/p".into(),
                landing_objects: 1,
                lines_in: 1,
                duplicates_dropped: 0,
                events_out: 1,
                correlations: 1,
                sealed: false,
                markers_dropped: 0,
                non_envelope_dropped: 0,
                unparseable_dropped: 0,
                completeness,
                delivery: crate::s3::DeliveryCertificate::default(),
            };
            let path = crate::scope::TapeSlot::ingest_report_path(&root, recording_id);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, serde_json::to_vec(&report).unwrap()).unwrap();

            let admitted =
                admit_against_recording(&root, &StoreCtx::disabled("run-1"), recording_id);
            assert_eq!(
                admitted.is_err(),
                expect_refusal,
                "recording {recording_id}: {admitted:?}"
            );
        }
    }

    #[test]
    fn a_run_that_chose_nothing_is_bounded_to_the_first_hundred() {
        // An unbounded replay must not be reachable: the one run that drove a
        // whole session took 439.8s and died in the scorer. So "no choice" is
        // resolved here, before any stage reads the scope.
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        let recorded = descending_ids(455);
        tape_of(&root, "rec", &recorded);
        let mut run = replay_run("rec", None);

        resolve_correlation_filter(&root, &mut run, &StoreCtx::disabled("run-backstop"), "rec")
            .unwrap();

        let resolved = run.spec.correlation_filter.clone().expect(
            "the run must carry the concrete ids it will drive, not an empty filter and a rule",
        );
        assert_eq!(resolved.len(), crate::scope::MAX_CORRELATIONS_PER_RUN);
        assert_eq!(
            resolved,
            recorded[..crate::scope::MAX_CORRELATIONS_PER_RUN].to_vec(),
            "the earliest hundred requests, in recording order"
        );
        // And the scope every stage builds from the spec now covers exactly them.
        let scope = crate::scope::RunScope::of(&run);
        assert!(!scope.is_entire_session());
        assert_eq!(scope.ids().map(|ids| ids.len()), Some(100));

        // The decision is durable at the moment it is made, not at the next stage.
        let persisted: Run = crate::read_json(&root.run_path("run-backstop")).unwrap();
        assert_eq!(persisted.spec.correlation_filter, Some(resolved));
    }

    #[test]
    fn a_recording_with_fewer_than_a_hundred_correlations_runs_all_of_them() {
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        let recorded = descending_ids(40);
        tape_of(&root, "rec", &recorded);
        let mut run = replay_run("rec", None);

        resolve_correlation_filter(&root, &mut run, &StoreCtx::disabled("run-backstop"), "rec")
            .unwrap();

        assert_eq!(
            run.spec.correlation_filter,
            Some(recorded),
            "the cap is a ceiling, not a target"
        );
    }

    #[test]
    fn an_oversized_explicit_filter_fails_the_run_instead_of_being_trimmed() {
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        tape_of(&root, "rec", &descending_ids(500));
        let asked: Vec<String> = (0..101).map(|i| format!("c-{i:04}")).collect();
        let mut run = replay_run("rec", Some(asked.clone()));

        let err =
            resolve_correlation_filter(&root, &mut run, &StoreCtx::disabled("run-backstop"), "rec")
                .unwrap_err();

        assert!(err.contains("101"), "{err}");
        assert_eq!(
            run.spec.correlation_filter,
            Some(asked),
            "a refused run keeps what was asked for — it is not quietly rewritten to 100"
        );
    }

    #[test]
    fn an_explicit_filter_is_left_exactly_as_the_caller_named_it() {
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        tape_of(&root, "rec", &descending_ids(455));
        let mut run = replay_run("rec", Some(vec!["c-0400".to_owned(), "c-0002".to_owned()]));

        resolve_correlation_filter(&root, &mut run, &StoreCtx::disabled("run-backstop"), "rec")
            .unwrap();

        assert_eq!(
            run.spec.correlation_filter,
            Some(vec!["c-0002".to_owned(), "c-0400".to_owned()]),
            "a caller that named two correlations drives two, not the first hundred"
        );
    }

    #[test]
    fn a_record_run_is_not_given_a_correlation_filter() {
        // A recording has nothing to filter against, and record mode's absent
        // filter is a real answer rather than an unmade choice.
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        let mut run = run_with_workload(serde_json::json!({}));
        assert_eq!(run.spec.mode, RunMode::Record);

        resolve_correlation_filter(&root, &mut run, &StoreCtx::disabled("r1"), "rec").unwrap();

        assert_eq!(run.spec.correlation_filter, None);
    }

    #[test]
    fn iterations_defaults_to_one() {
        assert_eq!(run_iterations(&run_with_workload(serde_json::json!({}))), 1);
    }

    #[test]
    fn iterations_read_from_workload() {
        assert_eq!(
            run_iterations(&run_with_workload(serde_json::json!({ "iterations": 25 }))),
            25
        );
    }

    // -----------------------------------------------------------------------
    // Seed-plan materialization wiring (deliverable 5) — the docker `seed_redis`
    // shell is not exercised; the plan-build + ambient-merge + value-rendering
    // pipeline that drives it is.
    // -----------------------------------------------------------------------

    /// A minimal recorded State READ event as JSONL (uses serde defaults for the
    /// many additive fields, so the test only states what it cares about).
    fn settlement_read_event_jsonl(correlation: &str, key: &str, value: &str) -> String {
        serde_json::json!({
            "record_kind": "boundary_event",
            "global_sequence": 0,
            "request_sequence": 0,
            "correlation_id": correlation,
            "timestamp_ns": 0,
            "boundary": "redis",
            "trait_name": "RedisStore",
            "method_name": "get",
            "call_file": "x.rs",
            "call_line": 1,
            "call_column": 1,
            "request": [key],
            "args": [key],
            "result": value,
            "response": value,
            "is_error": false,
            "duration_us": 0,
            "event_schema_version": deja::CURRENT_EVENT_SCHEMA_VERSION,
            "provenance": "recorded",
            "recon": "lossless",
            "replay_strategy": "substitute",
            "read_set": [key]
        })
        .to_string()
    }

    #[test]
    fn read_scoped_events_tolerates_non_event_lines() {
        let body = format!(
            "{}\n# a header / non-event line\n\n{}\n{}\n",
            settlement_read_event_jsonl("c1", "settlement_rate_default", "0.10"),
            "{not json at all}",
            // graph nodes ride the same tape; they are not seed-plan inputs
            r#"{"record_kind":"graph_node","node_id":1,"global_sequence":1,"sequence":1,"span_name":"root","target":"router","level":"INFO","fields":{},"started_ns":1}"#,
        );
        let (_dir, rec) = recording(&body, RunScope::entire_session());
        let events = read_scoped_events(&rec);
        assert_eq!(events.len(), 1, "only the one valid event parses");
        assert_eq!(events[0].read_set, vec!["settlement_rate_default"]);
    }

    /// Seeding must materialize preconditions for the run's cases only: a
    /// whole-session seed clones a pg schema per recorded correlation, which is
    /// O(correlations x schema) against the 42,310 on a production tape.
    #[test]
    fn read_scoped_events_yields_only_the_run_s_cases() {
        let body = format!(
            "{}\n{}\n",
            settlement_read_event_jsonl("c1", "settlement_rate_default", "0.10"),
            settlement_read_event_jsonl("c2", "settlement_rate_default", "0.99"),
        );
        let (_dir, rec) = recording(&body, RunScope::from_filter(Some(&["c1".to_string()])));
        let events = read_scoped_events(&rec);
        assert_eq!(
            events
                .iter()
                .map(|e| e.correlation_id.clone())
                .collect::<Vec<_>>(),
            [Some("c1".to_string())],
            "an undriven correlation must not be seeded"
        );
    }

    #[test]
    fn record_graph_extract_keeps_only_graph_nodes_and_drops_payloads() {
        // A tape interleaving graph-node STRUCTURE with a boundary event that
        // carries a payment payload — the exact thing that must NOT leave the pod.
        let secret_value = "SECRET_SETTLEMENT_PAYLOAD";
        let body = format!(
            "{}\n{}\n{}\n",
            r#"{"record_kind":"graph_node","node_id":1,"global_sequence":1,"sequence":1,"span_name":"root","target":"router","level":"INFO","fields":{"request_id":"req-1"},"started_ns":1}"#,
            settlement_read_event_jsonl("c1", "settlement_rate_default", secret_value),
            r#"{"record_kind":"graph_node","node_id":2,"global_sequence":3,"sequence":2,"parent_id":1,"span_name":"charge","target":"router","level":"INFO","fields":{},"started_ns":2}"#,
        );
        let (dir, rec) = recording(&body, RunScope::entire_session());

        let dest = dir.path().join("record-graph.jsonl");
        let n = write_record_graph_nodes(&rec.graph_nodes().unwrap(), &dest).unwrap();
        assert_eq!(
            n, 2,
            "both graph nodes extracted, the boundary event dropped"
        );

        let out = std::fs::read_to_string(&dest).unwrap();
        assert_eq!(out.lines().count(), 2);
        assert!(
            !out.contains(secret_value),
            "boundary payloads must never appear in the record-graph artifact"
        );
        // Every emitted line is a GraphNode DejaRecord the /graph reader accepts.
        for line in out.lines() {
            assert!(matches!(
                serde_json::from_str::<deja::DejaRecord>(line),
                Ok(deja::DejaRecord::GraphNode(_))
            ));
        }
    }

    /// The reported bug: a run driving three correlations published the span
    /// structure and field values of EVERY request in the recorded session —
    /// 86,204 nodes / 29 MB — to S3 and out through an unauthenticated
    /// `GET /graph`.
    #[test]
    fn record_graph_extract_publishes_only_the_run_s_own_correlations() {
        let mut driven = serde_json::from_str::<serde_json::Value>(&settlement_read_event_jsonl(
            "c-driven",
            "settlement_rate_default",
            "0.10",
        ))
        .unwrap();
        driven["graph_node_id"] = serde_json::json!(2);
        let mut foreign = serde_json::from_str::<serde_json::Value>(&settlement_read_event_jsonl(
            "c-foreign",
            "settlement_rate_default",
            "0.99",
        ))
        .unwrap();
        foreign["graph_node_id"] = serde_json::json!(11);

        let body = format!(
            "{}\n{}\n{}\n{}\n{}\n",
            r#"{"record_kind":"graph_node","node_id":1,"global_sequence":1,"sequence":1,"span_name":"ROOT_SPAN","target":"router","level":"INFO","fields":{},"started_ns":1}"#,
            r#"{"record_kind":"graph_node","node_id":2,"global_sequence":2,"sequence":2,"parent_id":1,"span_name":"charge","target":"router","level":"INFO","fields":{},"started_ns":2}"#,
            r#"{"record_kind":"graph_node","node_id":10,"global_sequence":10,"sequence":10,"span_name":"ROOT_SPAN","target":"router","level":"INFO","fields":{"golden_log_line":"OTHER_TENANT_REQUEST"},"started_ns":10}"#,
            r#"{"record_kind":"graph_node","node_id":11,"global_sequence":11,"sequence":11,"parent_id":10,"span_name":"charge","target":"router","level":"INFO","fields":{},"started_ns":11}"#,
            [driven.to_string(), foreign.to_string()].join("\n"),
        );
        let (dir, rec) = recording(
            &body,
            RunScope::from_filter(Some(&["c-driven".to_string()])),
        );
        let dest = dir.path().join("record-graph.jsonl");
        let n = write_record_graph_nodes(&rec.graph_nodes().unwrap(), &dest).unwrap();

        let out = std::fs::read_to_string(&dest).unwrap();
        assert_eq!(n, 2, "only the driven request's span tree: {out}");
        assert!(
            !out.contains("OTHER_TENANT_REQUEST"),
            "another request's span fields reached the published artifact: {out}"
        );
    }

    #[test]
    fn record_graph_extract_absent_recording_is_a_noop() {
        // No recording on disk (compose without ingest): opening refuses rather
        // than quietly reporting an empty graph, and score_and_register treats
        // that NotFound as "nothing to extract" instead of a run failure.
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        assert!(!TapeSlot::is_materialized(&root, "missing"));
        let err = ScopedRecording::open(&root, "missing", RunScope::entire_session()).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }

    /// Which cases on the staged tape name a graph node — i.e. which of the two
    /// refusals, if any, the record-graph extract will raise.
    enum Anchoring {
        /// Both driven cases anchor: nothing refuses.
        All,
        /// No case anchors: the "tape carries no `graph_node_id`" refusal.
        None,
        /// One case anchors and one does not: the "reaches no root" refusal,
        /// which must NAME the case that would have vanished.
        Partial,
    }

    /// Stage the on-disk state a replay run reaches `score_and_register` with:
    /// a two-case tape in the canonical slot and the run record beside it.
    fn staged_replay_run(anchoring: &Anchoring) -> (tempfile::TempDir, HarnessRoot, Run) {
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        let run_id = "run-order";
        let recording_id = "rec-order";

        let case = |correlation: &str, anchor: Option<u64>| {
            let mut event = serde_json::from_str::<serde_json::Value>(
                &settlement_read_event_jsonl(correlation, "settlement_rate_default", "0.10"),
            )
            .unwrap();
            event["graph_node_id"] = serde_json::json!(anchor);
            event.to_string()
        };
        let (first, second) = match anchoring {
            Anchoring::All => (Some(2), Some(4)),
            Anchoring::None => (None, None),
            Anchoring::Partial => (Some(2), None),
        };
        let body = [
            r#"{"record_kind":"graph_node","node_id":1,"global_sequence":1,"sequence":1,"span_name":"ROOT_SPAN","target":"router","level":"INFO","fields":{},"started_ns":1}"#.to_owned(),
            r#"{"record_kind":"graph_node","node_id":2,"global_sequence":2,"sequence":2,"parent_id":1,"span_name":"charge","target":"router","level":"INFO","fields":{},"started_ns":2}"#.to_owned(),
            r#"{"record_kind":"graph_node","node_id":3,"global_sequence":3,"sequence":3,"span_name":"ROOT_SPAN","target":"router","level":"INFO","fields":{},"started_ns":3}"#.to_owned(),
            r#"{"record_kind":"graph_node","node_id":4,"global_sequence":4,"sequence":4,"parent_id":3,"span_name":"charge","target":"router","level":"INFO","fields":{},"started_ns":4}"#.to_owned(),
            case("c-driven-1", first),
            case("c-driven-2", second),
        ]
        .join("\n");
        let tape = TapeSlot::for_write(&root, recording_id);
        std::fs::create_dir_all(tape.parent().unwrap()).unwrap();
        std::fs::write(&tape, body + "\n").unwrap();

        let run = Run {
            run_id: run_id.to_owned(),
            spec: RunSpec {
                mode: crate::RunMode::Replay,
                candidate_spec: CandidateSpec::PrebuiltImage {
                    image: "deja-demo".to_owned(),
                },
                candidate_repo: None,
                recording_id: Some(recording_id.to_owned()),
                s3_source: None,
                correlation_filter: Some(vec!["c-driven-1".to_owned(), "c-driven-2".to_owned()]),
                workload: serde_json::Value::Null,
            },
            status: RunStatus::Running,
            recording_id: Some(recording_id.to_owned()),
            candidate_image: None,
            failure_reason: None,
            stage: None,
            step: 0,
            steps_total: 0,
            stage_updated_ms: 0,
        };
        write_json(&root.run_path(run_id), &run).unwrap();
        (dir, root, run)
    }

    /// The graph refusal DEGRADES the run instead of destroying it. It used to
    /// be fatal — refuse before scoring so a run never carried a verdict it
    /// then disowned — but the scorer does not read the graph, and on tapes
    /// recorded before `graph_node_id` the refusal mostly means "this tape
    /// cannot say". So the run keeps its verdict, publishes NO graph (the
    /// refusal's actual point), and the reason rides the scorecard's own
    /// warnings instead of dying on stderr.
    #[test]
    fn a_refused_record_graph_degrades_to_a_scored_run_that_states_the_gap() {
        let (_dir, root, mut run) = staged_replay_run(&Anchoring::None);
        let ctx = StoreCtx::disabled(&run.run_id);
        score_and_register(&root, &mut run, &ctx, "rec-order", 6, &ArtifactSink::Local)
            .expect("a tape that cannot scope its graph still scores");

        assert!(
            root.scorecard_path(&run.run_id).exists(),
            "the verdict is persisted — the graph is not evidence scoring reads"
        );
        assert!(
            !root.record_graph_path(&run.run_id).exists(),
            "and NOTHING is published as the graph — a partial or empty graph \
             pretending to be whole is the failure mode the old refusal existed for"
        );
        let note = std::fs::read_to_string(root.record_graph_note_path(&run.run_id))
            .expect("the reason is written where the scorer picks it up");
        assert!(
            note.contains("graph_node_id"),
            "and it carries the actionable reason: {note}"
        );
        let card: serde_json::Value = crate::read_json(&root.scorecard_path(&run.run_id)).unwrap();
        let warnings = card["warnings"].to_string();
        assert!(
            warnings.contains("record graph unavailable"),
            "the gap is stated in the verdict's own record, not only on stderr: {warnings}"
        );
    }

    /// The other refusal, end to end: when only SOME cases anchor, no graph is
    /// published — a graph silently missing one case's spans is the exact thing
    /// the refusal exists to prevent — and the warning names the case whose
    /// spans would have vanished. The name has to survive into the scorecard's
    /// warnings, which is where an operator will actually read it.
    #[test]
    fn a_partially_anchored_tape_names_the_case_that_would_have_vanished() {
        let (_dir, root, mut run) = staged_replay_run(&Anchoring::Partial);
        let ctx = StoreCtx::disabled(&run.run_id);
        score_and_register(&root, &mut run, &ctx, "rec-order", 6, &ArtifactSink::Local)
            .expect("a partially anchored tape still scores");

        assert!(
            !root.record_graph_path(&run.run_id).exists(),
            "no graph is published when a case's spans would silently vanish from it"
        );
        let note = std::fs::read_to_string(root.record_graph_note_path(&run.run_id)).unwrap();
        assert!(
            note.contains("c-driven-2"),
            "the warning must name the unreachable case: {note}"
        );
        assert!(
            !note.contains("(c-driven-1"),
            "and only that one — the anchored case is fine: {note}"
        );
        assert!(
            root.scorecard_path(&run.run_id).exists(),
            "while the verdict is still persisted"
        );
    }

    /// A request the candidate never answered is one stated fact, not
    /// fifty-four field diffs. The kernel now carries the transport error on
    /// the diff row; scoring folds it into the scorecard's warnings, naming
    /// the correlation and the reason — the line that was missing every time
    /// someone asked "why is every status 0".
    #[test]
    fn a_candidate_that_never_answered_is_named_in_the_scorecards_warnings() {
        let (_dir, root, mut run) = staged_replay_run(&Anchoring::All);
        let diff = serde_json::json!({
            "correlation_id": "c-driven-1",
            "request_sequence": 0,
            "request_path": "/payments",
            "status_baseline": 200,
            "status_candidate": 0,
            "status_match": false,
            "body_diff": [],
            "transport_error": "read /payments: Resource temporarily unavailable (os error 11)",
        });
        std::fs::write(root.http_diff_path(&run.run_id), format!("{diff}\n")).unwrap();

        let ctx = StoreCtx::disabled(&run.run_id);
        score_and_register(&root, &mut run, &ctx, "rec-order", 6, &ArtifactSink::Local).unwrap();
        let card: serde_json::Value = crate::read_json(&root.scorecard_path(&run.run_id)).unwrap();
        let warnings = card["warnings"].to_string();
        assert!(
            warnings.contains("no response from the candidate")
                && warnings.contains("c-driven-1")
                && warnings.contains("os error 11"),
            "the reason travels with the verdict: {warnings}"
        );
    }

    /// A seed that failed to materialize re-frames every divergence downstream
    /// of it: the candidate read an empty table, not different behaviour. The
    /// certificate knows; this asserts the fact reaches the scorecard's own
    /// warnings, naming the table — the page where someone reads the verdict.
    #[test]
    fn a_failed_seed_is_named_in_the_scorecards_warnings() {
        let (_dir, root, mut run) = staged_replay_run(&Anchoring::All);
        // hex("payment_attempt") / hex("attempt_id") / hex("x") — a real v1
        // db_row wire key, so the warning derives the table the way readers do.
        let key = "deja:state:v1:db_row:7061796d656e745f617474656d7074:617474656d70745f6964:78";
        let cert = serde_json::json!({
            "schema_version": 1, "type": "seed_certificate",
            "recording_id": "rec-order", "run_id": run.run_id, "seed_db_enabled": true,
            "summary": { "planned": 2, "materialized": 0, "skipped": 0, "failed": 2,
                          "unsupported": 0, "readback_matched": 0, "readback_missing": 0,
                          "readback_mismatched": 0, "readback_errors": 2, "readback_not_run": 0 },
            "entries": [
                { "correlation_id": "c-driven-1", "boundary": "db", "logical_key": key,
                  "origin": "recording", "materialization": "failed",
                  "readback": { "status": "error", "message": "could not render an insert" } },
                { "correlation_id": "c-driven-2", "boundary": "db", "logical_key": key,
                  "origin": "recording", "materialization": "failed",
                  "readback": { "status": "error", "message": "could not render an insert" } }
            ]
        });
        crate::write_json(&root.seed_certificate_path(&run.run_id), &cert).unwrap();

        let ctx = StoreCtx::disabled(&run.run_id);
        score_and_register(&root, &mut run, &ctx, "rec-order", 6, &ArtifactSink::Local).unwrap();
        let card: serde_json::Value = crate::read_json(&root.scorecard_path(&run.run_id)).unwrap();
        let warnings = card["warnings"].to_string();
        assert!(
            warnings.contains("2 seed entries FAILED") && warnings.contains("payment_attempt"),
            "the verdict's own record names the seed failure and its table: {warnings}"
        );
    }

    /// The positive control: the same path on a fully ANCHORED tape must still
    /// score and write the verdict, or the assertions above pass for the wrong
    /// reason.
    #[test]
    fn an_anchored_record_graph_still_scores_and_writes_the_verdict() {
        let (_dir, root, mut run) = staged_replay_run(&Anchoring::All);
        let ctx = StoreCtx::disabled(&run.run_id);
        score_and_register(&root, &mut run, &ctx, "rec-order", 6, &ArtifactSink::Local)
            .expect("an anchored tape scores normally");
        assert!(
            root.scorecard_path(&run.run_id).exists(),
            "the verdict is persisted when nothing refuses"
        );
        let nodes = std::fs::read_to_string(root.record_graph_path(&run.run_id)).unwrap();
        assert_eq!(
            nodes.lines().count(),
            4,
            "and both driven cases' span trees are extracted: {nodes}"
        );
    }

    /// The full replay-side wiring: derive the default rate from the recording's
    /// read-set, supply the premium rate from the ambient template, and render
    /// both to the byte-identical redis values the old hand-coded seeds wrote.
    #[test]
    fn seed_plan_yields_settlement_rates_from_recording_and_template() {
        let (_dir, rec) = recording(
            &settlement_read_event_jsonl("c1", "settlement_rate_default", "0.10"),
            RunScope::entire_session(),
        );
        let events = read_scoped_events(&rec);

        // Build the plan exactly as materialize_seed_plan does (per-correlation,
        // unioned, then ambient-merged).
        let mut plan = deja::SeedPlan::new();
        for entry in deja::build_seed_plan(&events, Some("c1")).iter() {
            plan.upsert(entry.clone());
        }
        let plan = plan.with_ambient(&deja::AmbientTemplate::demo_defaults());

        // default rate is RECORDING-derived; premium rate is AMBIENT-derived.
        let default = plan
            .resolve("redis", "settlement_rate_default")
            .expect("default seeded from recording");
        assert_eq!(default.origin, deja::SeedOrigin::Recording);
        assert_eq!(rendered_string(&default.value).as_deref(), Some("0.10"));

        let premium = plan
            .resolve("redis", "settlement_rate_premium")
            .expect("premium seeded from ambient template");
        assert_eq!(premium.origin, deja::SeedOrigin::Ambient);
        assert_eq!(
            rendered_string(&premium.value).as_deref(),
            Some("0.20"),
            "premium rate renders byte-identically to the old `redis-cli SET ... 0.20`"
        );
    }

    /// Scalar rendering shorthand: the `SET` payload's value, when the render
    /// is one.
    fn rendered_string(value: &serde_json::Value) -> Option<String> {
        match render_redis_seed_payload(value, None) {
            RedisSeedRender::Payload(RedisSeedPayload::String(s)) => Some(s),
            _ => None,
        }
    }

    #[test]
    fn ambient_template_defaults_to_demo_premium_rate() {
        // No DEJA_AMBIENT_TEMPLATE set in test → demo defaults.
        let template = load_ambient_template();
        assert!(!template.is_empty());
        let plan = deja::SeedPlan::new().with_ambient(&template);
        assert_eq!(
            rendered_string(
                &plan
                    .resolve("redis", "settlement_rate_premium")
                    .unwrap()
                    .value
            )
            .as_deref(),
            Some("0.20")
        );
    }

    #[test]
    fn redis_seed_decodes_dejarredisvalue_wrapper_to_raw_value() {
        // V1 regression: a recorded redis GET hit is an externally-tagged
        // DejaRedisValue, not a bare string. The seeder must write the DECODED
        // value redis returns, never the enum wrapper text.

        // Golden case — the exact bytes of a real recorded API_LOCK GET hit from
        // demo/harness-state/1783513055 (redis_rs backend, `BulkString`). These
        // bytes are the UTF-8 of a UUID; the old `to_string()` seeded the literal
        // `{"BulkString":[48,49,...]}` text and the router read back garbage.
        let bulk = serde_json::json!({
            "BulkString": [
                48, 49, 57, 102, 52, 49, 98, 49, 45, 50, 50, 48, 101, 45, 55, 50,
                56, 50, 45, 56, 48, 101, 51, 45, 53, 56, 55, 54, 100, 97, 56, 99,
                100, 101, 57, 99
            ]
        });
        assert_eq!(
            rendered_string(&bulk).as_deref(),
            Some("019f41b1-220e-7282-80e3-5876da8cde9c"),
            "BulkString must decode to the raw UUID redis holds, not the wrapper"
        );

        // fred backend uses different variant names for the same shapes; the
        // shared type's serde aliases fold both dialects into one decode.
        assert_eq!(
            rendered_string(&serde_json::json!({ "Bytes": [104, 105] })).as_deref(),
            Some("hi"),
        );
        assert_eq!(
            rendered_string(&serde_json::json!({ "String": "merchant_xyz" })).as_deref(),
            Some("merchant_xyz"),
        );
        // redis_rs scalar variants.
        assert_eq!(
            rendered_string(&serde_json::json!({ "SimpleString": "OK" })).as_deref(),
            Some("OK"),
        );
        assert_eq!(
            rendered_string(&serde_json::json!({ "Int": 42 })).as_deref(),
            Some("42"),
        );

        // Backwards-compat: an ambient/template bare string is preserved
        // byte-for-byte (this is the path the demo relied on).
        assert_eq!(
            rendered_string(&serde_json::Value::String("0.20".to_owned())).as_deref(),
            Some("0.20"),
        );

        // A miss is correct ABSENCE — seed nothing, named on the certificate.
        assert!(matches!(
            render_redis_seed_payload(&serde_json::json!("Null"), None),
            RedisSeedRender::Absent(_)
        ));
    }

    /// #39: a recorded hash read (`Map`, HGETALL-family) seeds via `HSET` with
    /// field/value pairs in recorded order, and reads back with `HGETALL`.
    #[test]
    fn redis_map_seed_renders_hset_and_reads_back() {
        // Mixed dialects/scalar kinds inside one map, as the wire allows.
        let map = serde_json::json!({ "Map": [
            [{ "BulkString": [102, 49] }, { "BulkString": [118, 49] }],
            [{ "SimpleString": "f2" }, { "Int": 7 }],
        ]});
        let payload = match render_redis_seed_payload(&map, Some("get_hash_fields")) {
            RedisSeedRender::Payload(payload) => payload,
            other => panic!("a Map must render a write payload, got {other:?}"),
        };
        assert_eq!(
            payload.write_args("k"),
            ["HSET", "k", "f1", "v1", "f2", "7"],
            "HSET carries the pairs in recorded order"
        );
        assert_eq!(payload.readback_args("k"), ["HGETALL", "k"]);
        assert!(
            payload.compare(b"f1\nv1\nf2\n7").is_ok(),
            "the --raw HGETALL line image reads back"
        );
        assert!(
            payload.compare(b"f1\nv1\nf2\n8").is_err(),
            "a changed field value is a readback mismatch, not a pass"
        );
    }

    /// #39: a recorded list range (`Array`) seeds via `RPUSH` in recorded
    /// order — order is part of the value a list read observed.
    #[test]
    fn redis_array_seed_renders_rpush_preserving_order() {
        let array = serde_json::json!({ "Array": [
            { "SimpleString": "a" }, { "SimpleString": "c" }, { "SimpleString": "b" },
        ]});
        let payload = match render_redis_seed_payload(&array, Some("get_list_elements")) {
            RedisSeedRender::Payload(payload) => payload,
            other => panic!("an Array must render a write payload, got {other:?}"),
        };
        assert_eq!(
            payload.write_args("k"),
            ["RPUSH", "k", "a", "c", "b"],
            "RPUSH pushes members in recorded order"
        );
        assert_eq!(payload.readback_args("k"), ["LRANGE", "k", "0", "-1"]);
        assert!(payload.compare(b"a\nc\nb").is_ok());
        assert!(
            payload.compare(b"a\nb\nc").is_err(),
            "a reordered list is a readback mismatch — order is recorded state"
        );
    }

    /// #39: the SAME `Array` wire shape recorded by a sorted-set method seeds
    /// via `ZADD` — the value alone cannot distinguish a list range from a
    /// zset range; the recorded method name can. Index-as-score reproduces the
    /// member order the (score-less) range read observed.
    #[test]
    fn redis_array_from_sorted_set_method_renders_zadd() {
        let array = serde_json::json!({ "Array": [
            { "SimpleString": "m1" }, { "SimpleString": "m2" },
        ]});
        let payload = match render_redis_seed_payload(&array, Some("zrange_by_rank")) {
            RedisSeedRender::Payload(payload) => payload,
            other => panic!("a zset Array must render a write payload, got {other:?}"),
        };
        assert_eq!(payload.write_args("k"), ["ZADD", "k", "0", "m1", "1", "m2"]);
        assert_eq!(payload.readback_args("k"), ["ZRANGE", "k", "0", "-1"]);
        assert!(payload.compare(b"m1\nm2").is_ok());
        assert!(
            !method_names_sorted_set("get_list_elements"),
            "a plain list method must NOT trip the zset detection"
        );
    }

    /// #39: a recorded set (`Set`, `redis_rs` dialect) seeds via `SADD`;
    /// SMEMBERS has no defined order, so its readback compares as a set.
    #[test]
    fn redis_set_seed_renders_sadd_with_unordered_readback() {
        let set = serde_json::json!({ "Set": [
            { "SimpleString": "b" }, { "SimpleString": "a" },
        ]});
        let payload = match render_redis_seed_payload(&set, Some("smembers")) {
            RedisSeedRender::Payload(payload) => payload,
            other => panic!("a Set must render a write payload, got {other:?}"),
        };
        assert_eq!(payload.write_args("k"), ["SADD", "k", "b", "a"]);
        assert_eq!(payload.readback_args("k"), ["SMEMBERS", "k"]);
        assert!(
            payload.compare(b"a\nb").is_ok(),
            "SMEMBERS order is not part of the value"
        );
        assert!(payload.compare(b"a\nc").is_err());
    }

    /// #39: a shape with no faithful write command stays a LOUD skip with a
    /// named reason — never a stringified `SET` of the wrapper text.
    #[test]
    fn redis_unsupported_wire_shapes_stay_loud_skips() {
        // A wire variant with no faithful write command (modelled since #45,
        // still refused — now by the exhaustive match instead of at decode).
        match render_redis_seed_payload(
            &serde_json::json!({ "VerbatimString": { "format": "txt", "text": "x" } }),
            None,
        ) {
            RedisSeedRender::Unrepresentable(reason) => assert!(
                reason.contains("VerbatimString"),
                "the refusal names the variant: {reason}"
            ),
            other => panic!("an unmodelled variant must be Unrepresentable, got {other:?}"),
        }
        // Status acknowledgements are not stored state. Before the wire type
        // modelled them (#45), these bare-string tags slipped into the
        // ambient-string branch and seeded the LITERAL TEXT "Okay"/"Queued".
        for tag in ["Okay", "Queued"] {
            match render_redis_seed_payload(&serde_json::json!(tag), None) {
                RedisSeedRender::Unrepresentable(reason) => assert!(
                    reason.contains(tag),
                    "the refusal names the status reply: {reason}"
                ),
                other => panic!("a status reply must be Unrepresentable, got {other:?}"),
            }
        }
        // A recording the capture side flagged as unsupported stays named.
        match render_redis_seed_payload(
            &serde_json::json!({ "UnsupportedSuccessfulValue": { "variant": "BigNumber" } }),
            None,
        ) {
            RedisSeedRender::Unrepresentable(reason) => assert!(
                reason.contains("BigNumber"),
                "the refusal names what was recorded: {reason}"
            ),
            other => panic!("an unsupported recording must be Unrepresentable, got {other:?}"),
        }
        // A structure whose members are not scalars cannot flatten into HSET.
        match render_redis_seed_payload(
            &serde_json::json!({ "Map": [[{ "SimpleString": "f" }, { "Array": [] }]] }),
            None,
        ) {
            RedisSeedRender::Unrepresentable(reason) => assert!(
                reason.contains("HSET"),
                "the refusal names the command that cannot represent it: {reason}"
            ),
            other => panic!("a nested non-scalar must be Unrepresentable, got {other:?}"),
        }
        // An empty collection is ABSENCE (redis holds no key for it), not work
        // and not a refusal.
        assert!(matches!(
            render_redis_seed_payload(&serde_json::json!({ "Array": [] }), None),
            RedisSeedRender::Absent(_)
        ));
    }

    #[test]
    fn db_query_seed_plan_materializes_users_ok_envelope_into_insert_sql() {
        let query_key = deja::StateKey::DbQuery {
            table: "users".to_owned(),
            fingerprint: "find-user-123".to_owned(),
        }
        .to_wire();
        let users_row = serde_json::json!({
            "user_id": "user_123",
            "merchant_id": "merch_456",
            "email": "alice@example.com"
        });
        let envelope = serde_json::json!({
            "version": 1,
            "result": "Ok",
            "value": users_row,
            "type_name": "User"
        });
        let event: deja::BoundaryEvent = serde_json::from_value(serde_json::json!({
            "global_sequence": 0,
            "request_sequence": 0,
            "correlation_id": "cycle36b",
            "timestamp_ns": 0,
            "boundary": "db",
            "trait_name": "Execute",
            "method_name": "generic_find_one_core",
            "call_file": "x.rs",
            "call_line": 1,
            "call_column": 1,
            "request": {
                "operation": "generic_find_one_core",
                "table": "users",
                "sql": "SELECT * FROM \"users\" WHERE \"user_id\" = $1",
                "inputs": ["user_123"]
            },
            "args": {
                "operation": "generic_find_one_core",
                "table": "users",
                "sql": "SELECT * FROM \"users\" WHERE \"user_id\" = $1",
                "inputs": ["user_123"]
            },
            "result": envelope,
            "response": envelope,
            "is_error": false,
            "duration_us": 0,
            "event_schema_version": deja::CURRENT_EVENT_SCHEMA_VERSION,
            "provenance": "recorded",
            "recon": "lossless",
            "replay_strategy": "execute",
            "read_set": [query_key]
        }))
        .unwrap();

        let plan = deja::build_seed_plan(&[event], Some("cycle36b"));
        let seed = plan
            .resolve("db", &query_key)
            .expect("DbQuery read must seed from the recorded result envelope");
        assert_eq!(seed.origin, deja::SeedOrigin::Recording);
        let target = db_seed_target_from_key(&seed.key).expect("DbQuery key is seedable");
        assert_eq!(target.table, "users");
        assert_eq!(target.kind, "query-fallback");

        let value = db_seed_value(&seed.value).expect("Ok envelope exposes row payload");
        let rows = db_row_images(&target.table, &value, &DbCatalog::default());
        assert_eq!(rows.len(), 1, "one users row image should be materialized");
        let sql = build_insert_sql(Some("deja_cycle36b"), &rows[0]).expect("insert SQL");

        assert!(
            sql.starts_with("INSERT INTO \"deja_cycle36b\".\"users\""),
            "query-fallback DB seeds must materialize into the correlation schema; got: {sql}"
        );
        assert!(
            sql.contains("\"user_id\"") && sql.contains("'user_123'"),
            "the users primary-key column and value must be present in the row image; got: {sql}"
        );
        assert!(
            sql.contains("\"merchant_id\"") && sql.contains("'merch_456'"),
            "non-PK account data from the recorded row must remain in the INSERT image; got: {sql}"
        );
    }

    #[test]
    fn db_row_seed_filters_multi_row_envelope_to_keyed_row() {
        let row_key = deja::StateKey::DbRow {
            table: "users".to_owned(),
            key: vec![("user_id".to_owned(), "user_123".to_owned())],
        }
        .to_wire();
        let target = db_seed_target_from_key(&row_key).expect("DbRow key is seedable");
        assert_eq!(target.kind, "row");

        let envelope = serde_json::json!({
            "version": 1,
            "result": "Ok",
            "value": [
                {
                    "user_id": "user_123",
                    "merchant_id": "merch_456",
                    "email": "alice@example.com"
                },
                {
                    "user_id": "user_999",
                    "merchant_id": "merch_999",
                    "email": "mallory@example.com"
                }
            ],
            "type_name": "Vec<User>"
        });

        let value = db_seed_value(&envelope).expect("Ok envelope exposes row payload");
        let rows = target.filter_rows(db_row_images(&target.table, &value, &DbCatalog::default()));
        assert_eq!(rows.len(), 1, "DbRow seeds must render only the keyed row");

        let query_key = deja::StateKey::DbQuery {
            table: "users".to_owned(),
            fingerprint: "multi-user-query".to_owned(),
        }
        .to_wire();
        let query_target = db_seed_target_from_key(&query_key).expect("DbQuery key is seedable");
        let query_rows = query_target.filter_rows(db_row_images(
            &query_target.table,
            &value,
            &DbCatalog::default(),
        ));
        assert_eq!(
            query_rows.len(),
            2,
            "DbQuery fallback seeds still materialize the complete result set once"
        );

        let sql = build_insert_sql(Some("deja_cycle36b"), &rows[0]).expect("insert SQL");
        assert!(
            sql.contains("'user_123'") && sql.contains("'alice@example.com'"),
            "the keyed row must be rendered; got: {sql}"
        );
        assert!(
            !sql.contains("user_999") && !sql.contains("mallory@example.com"),
            "other rows from the same result envelope must not be rendered for a DbRow seed; got: {sql}"
        );
    }

    #[test]
    fn db_row_seeds_materialize_before_query_fallback_for_same_payment_intent() {
        let payment_id = "pay_precondition_123";
        let row_key = deja::StateKey::DbRow {
            table: "payment_intent".to_owned(),
            key: vec![("payment_id".to_owned(), payment_id.to_owned())],
        }
        .to_wire();
        let query_key = deja::StateKey::DbQuery {
            table: "payment_intent".to_owned(),
            fingerprint: "confirm-status-query".to_owned(),
        }
        .to_wire();

        let row_precondition = serde_json::json!({
            "version": 1,
            "result": "Ok",
            "value": {
                "payment_id": payment_id,
                "status": "requires_confirmation"
            },
            "type_name": "diesel_models::payments::payment_intent::PaymentIntent"
        });
        let query_fallback = serde_json::json!({
            "version": 1,
            "result": "Ok",
            "value": {
                "payment_id": payment_id,
                "status": "succeeded"
            },
            "type_name": "diesel_models::payments::payment_intent::PaymentIntent"
        });

        let mut plan = deja::SeedPlan::new();
        plan.upsert(deja::SeedEntry {
            boundary: "db".to_owned(),
            key: query_key.clone(),
            value: query_fallback,
            image: None,
            method: None,
            origin: deja::SeedOrigin::Recording,
            source_sequence: 0,
        });
        plan.upsert(deja::SeedEntry {
            boundary: "db".to_owned(),
            key: row_key.clone(),
            value: row_precondition,
            image: None,
            method: None,
            origin: deja::SeedOrigin::Recording,
            source_sequence: 0,
        });

        let query_seed = plan.resolve("db", &query_key).expect("query seed present");
        let row_seed = plan.resolve("db", &row_key).expect("row seed present");
        assert!(
            seed_materialization_priority(row_seed) < seed_materialization_priority(query_seed),
            "exact DbRow preconditions must be ranked before DbQuery fallback snapshots"
        );

        let mut entries = plan.iter().collect::<Vec<_>>();
        entries.sort_by_key(|entry| seed_materialization_priority(entry));

        assert_eq!(
            entries.iter().map(|entry| entry.key.as_str()).collect::<Vec<_>>(),
            vec![row_key.as_str(), query_key.as_str()],
            "materialization must insert the exact row first so the later query fallback no-ops on conflict"
        );

        let first_target = db_seed_target_from_key(&entries[0].key).expect("first seed target");
        let first_value = db_seed_value(&entries[0].value).expect("first seed has Ok row payload");
        let first_rows = first_target.filter_rows(db_row_images(
            &first_target.table,
            &first_value,
            &DbCatalog::default(),
        ));
        let first_sql =
            build_insert_sql(Some("deja_confirm"), &first_rows[0]).expect("first insert sql");
        assert!(
            first_sql.contains("'requires_confirmation'") && !first_sql.contains("'succeeded'"),
            "the row precondition, not the final query snapshot, must be the first INSERT; got: {first_sql}"
        );

        let second_target = db_seed_target_from_key(&entries[1].key).expect("second seed target");
        let second_value =
            db_seed_value(&entries[1].value).expect("second seed has Ok row payload");
        let second_rows = second_target.filter_rows(db_row_images(
            &second_target.table,
            &second_value,
            &DbCatalog::default(),
        ));
        let second_sql =
            build_insert_sql(Some("deja_confirm"), &second_rows[0]).expect("second insert sql");
        assert!(
            second_sql.contains("'succeeded'"),
            "the query fallback snapshot is still materialized after the exact row; got: {second_sql}"
        );
    }

    /// A tape can carry TWO versions of one row under different state keys —
    /// an early read's image and a post-update read's. One store holds one,
    /// and `ON CONFLICT DO NOTHING` keeps whichever lands first, so first
    /// must mean FIRST OBSERVED: key-string order let a later version land
    /// before an earlier read's, and every read of the early state diverged
    /// by the update's delta (13 payment_methods timestamp divergences plus
    /// 13 readback misses on run-0811). Within one source event, the
    /// row-before-query priority survives as the tiebreak.
    #[test]
    fn seed_materialization_orders_by_source_sequence_before_kind() {
        let entry = |key: &str, seq: u64| deja::SeedEntry {
            boundary: "db".to_owned(),
            key: key.to_owned(),
            value: serde_json::json!({}),
            image: None,
            method: None,
            origin: deja::SeedOrigin::Recording,
            source_sequence: seq,
        };
        let row_key = |table: &str, val: &str| {
            deja::StateKey::DbRow {
                table: table.to_owned(),
                key: vec![("id".to_owned(), val.to_owned())],
            }
            .to_wire()
        };
        let query_key = |table: &str, fp: &str| {
            deja::StateKey::DbQuery {
                table: table.to_owned(),
                fingerprint: fp.to_owned(),
            }
            .to_wire()
        };
        // Deliberately key-string-inverted: the LATER event's keys sort first
        // alphabetically ("aaa" < "zzz").
        let late_row = entry(&row_key("aaa_table", "1"), 20);
        let early_query = entry(&query_key("zzz_table", "ff"), 10);
        let early_row = entry(&row_key("zzz_table", "1"), 10);
        let mut entries = [&late_row, &early_query, &early_row];
        entries.sort_by_key(|entry| (entry.source_sequence, seed_materialization_priority(entry)));
        let order: Vec<u64> = entries.iter().map(|e| e.source_sequence).collect();
        assert_eq!(order, [10, 10, 20], "earliest observed version lands first");
        assert_eq!(
            seed_materialization_priority(entries[0]),
            0,
            "within one source event the exact row still precedes the query snapshot"
        );
    }

    #[test]
    fn signin_users_tape_entry_materializes_dbquery_and_dbrow_seed_sql() {
        let corr = "019f24d5-ac02-79d1-8e13-5ee04f51c8a1";
        let user_id = "a4db0a28-55db-412a-a57b-657c4dbd5504";
        let query_key = deja::StateKey::DbQuery {
            table: "users".to_owned(),
            fingerprint: "9cbd90c8d72d18b3".to_owned(),
        }
        .to_wire();
        let row_key = deja::StateKey::DbRow {
            table: "users".to_owned(),
            key: vec![("user_id".to_owned(), user_id.to_owned())],
        }
        .to_wire();
        let users_row = serde_json::json!({
            "created_at": "2026-07-02 21:56:50.798726",
            "email": "user_8ab3599a75a5b997@deja.dev",
            "is_active": true,
            "is_verified": false,
            "last_modified_at": "2026-07-02 21:56:50.798726",
            "last_password_modified_at": "2026-07-02 21:56:50.798726",
            "lineage_context": null,
            "name": "user_8ab3599a75a5b997",
            "password": "$argon2id$v=19$m=19456,t=2,p=1$hash",
            "totp_recovery_codes": null,
            "totp_secret": null,
            "totp_status": "not_set",
            "user_id": user_id
        });
        let envelope = serde_json::json!({
            "version": 1,
            "result": "Ok",
            "value": users_row,
            "type_name": "diesel_models::user::User"
        });
        let event: deja::BoundaryEvent = serde_json::from_value(serde_json::json!({
            "global_sequence": 50,
            "request_sequence": 1,
            "correlation_id": corr,
            "timestamp_ns": 1783029410812345678_u64,
            "boundary": "db",
            "trait_name": "diesel_models::query::generics",
            "method_name": "generic_find_one_core",
            "call_file": "crates/diesel_models/src/query/generics.rs",
            "call_line": 767,
            "call_column": 25,
            "request": {
                "operation": "generic_find_one_core",
                "table": "users",
                "sql": "SELECT \"users\".\"user_id\" FROM \"users\" WHERE \"users\".\"email\" = $1",
                "inputs": {
                    "predicate": {
                        "type": "diesel::expression::grouped::Grouped<users::email>"
                    }
                }
            },
            "args": {
                "operation": "generic_find_one_core",
                "table": "users",
                "sql": "SELECT \"users\".\"user_id\" FROM \"users\" WHERE \"users\".\"email\" = $1",
                "inputs": {
                    "predicate": {
                        "type": "diesel::expression::grouped::Grouped<users::email>"
                    }
                }
            },
            "result": envelope,
            "response": envelope,
            "is_error": false,
            "duration_us": 0,
            "event_schema_version": deja::CURRENT_EVENT_SCHEMA_VERSION,
            "provenance": "recorded",
            "recon": "lossless",
            "replay_strategy": "execute",
            "read_set": [query_key.clone(), row_key.clone()],
            "write_set": []
        }))
        .unwrap();
        let plan = deja::build_seed_plan(&[event], Some(corr));
        let mut catalog = DbCatalog::default();
        catalog.insert(
            "users".into(),
            DbColumnMetadata {
                name: "totp_secret".into(),
                type_oid: Some(17),
                type_name: Some("bytea".into()),
                nullable: Some(true),
            },
        );

        for key in [&query_key, &row_key] {
            let seed = plan
                .resolve("db", key)
                .expect("signin users read key must produce a DB seed entry");
            let target = db_seed_target_from_key(&seed.key).expect("typed users key is seedable");
            let value = db_seed_value(&seed.value).expect("Ok envelope exposes users row");
            let rows = target.filter_rows(db_row_images(&target.table, &value, &catalog));
            assert_eq!(
                rows.len(),
                1,
                "{key} must materialize exactly the signin user row"
            );
            let sql = build_insert_sql(Some(&deja::db_schema_for(corr)), &rows[0])
                .expect("signin users row must build INSERT SQL");
            assert!(sql.contains("\"totp_secret\"") && sql.contains("NULL"));
            assert!(sql.contains("'user_8ab3599a75a5b997@deja.dev'"));
            assert!(sql.contains(user_id));
        }
    }

    #[test]
    fn seed_db_renders_encrypted_bytea_key_as_hex_literal_from_metadata() {
        // merchant_key_store row exactly as recorded: `key` is the `Encryption`
        // serde shape {"inner":[<u8>...]}; it is treated as bytea only because
        // catalog metadata says that column is bytea.
        let row: serde_json::Map<String, serde_json::Value> =
            serde_json::from_value(serde_json::json!({
                "merchant_id": "merch_753c6e4d26d2323a",
                "key": {"inner": [225, 127, 0, 255, 16]},
                "created_at": "2026-07-02T07:04:03.613Z"
            }))
            .unwrap();
        let mut catalog = DbCatalog::default();
        catalog.insert(
            "merchant_key_store".into(),
            DbColumnMetadata {
                name: "merchant_id".into(),
                type_oid: Some(25),
                type_name: Some("text".into()),
                nullable: Some(false),
            },
        );
        catalog.insert(
            "merchant_key_store".into(),
            DbColumnMetadata {
                name: "key".into(),
                type_oid: Some(17),
                type_name: Some("bytea".into()),
                nullable: Some(false),
            },
        );
        catalog.insert(
            "merchant_key_store".into(),
            DbColumnMetadata {
                name: "created_at".into(),
                type_oid: Some(1184),
                type_name: Some("timestamptz".into()),
                nullable: Some(false),
            },
        );

        let image = DbRowImage::from_json_object("merchant_key_store", &row, &catalog)
            .expect("row image built");
        let sql = build_insert_sql(Some("deja_4d2c"), &image).expect("insert sql built");
        assert!(
            image
                .columns
                .iter()
                .any(|column| column.metadata.name == "key"
                    && column.metadata.type_oid == Some(17)
                    && column.metadata.type_name.as_deref() == Some("bytea")
                    && column.metadata.nullable == Some(false)),
            "row image must carry typed column metadata"
        );
        // The encrypted key must be a bytea hex literal (e1 7f 00 ff 10), NOT JSON.
        assert!(
            sql.contains("'\\xe17f00ff10'::bytea"),
            "key must render as bytea hex; got: {sql}"
        );
        assert!(
            !sql.contains("{\"inner\""),
            "bytea metadata must drive rendering away from JSON text; got: {sql}"
        );
        // Plain columns still render as quoted literals, into the corr schema.
        assert!(sql.contains("INSERT INTO \"deja_4d2c\".\"merchant_key_store\""));
        assert!(sql.contains("'merch_753c6e4d26d2323a'"));
    }

    #[test]
    fn typed_db_image_metadata_is_preferred_and_all_unknown_image_falls_back() {
        let typed_image = deja::db::DbRowImage::new(
            "merchant_key_store",
            vec![
                deja::db::DbColumnImage {
                    wire: None,
                    name: "merchant_id".into(),
                    type_oid: Some(25),
                    type_name: Some("text".into()),
                    nullable: Some(false),
                    value: serde_json::json!("merch_typed"),
                },
                deja::db::DbColumnImage {
                    wire: None,
                    name: "key".into(),
                    type_oid: Some(17),
                    type_name: Some("bytea".into()),
                    nullable: Some(false),
                    value: serde_json::json!({"inner": [1, 2, 3]}),
                },
            ],
        )
        .to_value();
        let raw_envelope = serde_json::json!({
            "version": 1,
            "result": "Ok",
            "value": {
                "merchant_id": "merch_raw",
                "key": {"inner": [9, 9, 9]}
            },
            "type_name": "MerchantKeyStore"
        });

        let rows = db_row_images_from_typed_payload(
            "merchant_key_store",
            &typed_image,
            &DbCatalog::default(),
        )
        .expect("typed image with producer metadata is seedable");
        let typed_sql = build_insert_sql(Some("deja_typed"), &rows[0]).expect("typed insert SQL");
        assert!(
            typed_sql.contains("'\\x010203'::bytea"),
            "producer bytea metadata must drive typed-image rendering; got: {typed_sql}"
        );
        assert!(
            !typed_sql.contains("'\\x090909'::bytea") && !typed_sql.contains("merch_raw"),
            "typed image must be preferred over the raw envelope fallback; got: {typed_sql}"
        );

        let all_unknown_image = deja::db::DbRowImage::new(
            "merchant_key_store",
            vec![
                deja::db::DbColumnImage {
                    wire: None,
                    name: "merchant_id".into(),
                    type_oid: None,
                    type_name: None,
                    nullable: None,
                    value: serde_json::json!("merch_typed"),
                },
                deja::db::DbColumnImage {
                    wire: None,
                    name: "key".into(),
                    type_oid: None,
                    type_name: None,
                    nullable: None,
                    value: serde_json::json!({"inner": [1, 2, 3]}),
                },
            ],
        )
        .to_value();
        let mut catalog = DbCatalog::default();
        catalog.insert(
            "merchant_key_store".into(),
            DbColumnMetadata {
                name: "key".into(),
                type_oid: Some(17),
                type_name: Some("bytea".into()),
                nullable: Some(false),
            },
        );
        assert!(
            db_row_images_from_typed_payload("merchant_key_store", &all_unknown_image, &catalog)
                .is_none(),
            "an all-unknown typed image must not count as a metadata-backed image success"
        );

        let unknown_rows = db_row_images(
            "merchant_key_store",
            &serde_json::json!({
                "merchant_id": "merch_unknown",
                "key": {"inner": [1, 2, 3]}
            }),
            &DbCatalog::default(),
        );
        let unknown_sql =
            build_insert_sql(Some("deja_typed"), &unknown_rows[0]).expect("unknown insert SQL");
        assert!(
            unknown_sql.contains("{\"inner\":[1,2,3]}") && !unknown_sql.contains("::bytea"),
            "unknown metadata must render the JSON object literally, never guess bytea; got: {unknown_sql}"
        );

        let fallback_value = db_seed_value(&raw_envelope).expect("legacy Ok envelope has value");
        let fallback_rows = db_row_images("merchant_key_store", &fallback_value, &catalog);
        let fallback_sql =
            build_insert_sql(Some("deja_typed"), &fallback_rows[0]).expect("fallback insert SQL");
        assert!(
            fallback_sql.contains("'\\x090909'::bytea") && fallback_sql.contains("merch_raw"),
            "legacy raw envelope + catalog fallback must still materialize; got: {fallback_sql}"
        );
    }

    #[test]
    fn nullable_bytea_column_renders_null_instead_of_skipping_row() {
        let row: serde_json::Map<String, serde_json::Value> =
            serde_json::from_value(serde_json::json!({
                "user_id": "a4db0a28-55db-412a-a57b-657c4dbd5504",
                "email": "user_8ab3599a75a5b997@deja.dev",
                "name": "user_8ab3599a75a5b997",
                "password": "$argon2id$v=19$m=19456,t=2,p=1$hash",
                "is_verified": false,
                "created_at": "2026-07-02 21:56:50.798726",
                "last_modified_at": "2026-07-02 21:56:50.798726",
                "totp_status": "not_set",
                "totp_secret": null,
                "totp_recovery_codes": null,
                "last_password_modified_at": "2026-07-02 21:56:50.798726",
                "lineage_context": null,
                "is_active": true
            }))
            .unwrap();
        let mut catalog = DbCatalog::default();
        catalog.insert(
            "users".into(),
            DbColumnMetadata {
                name: "totp_secret".into(),
                type_oid: Some(17),
                type_name: Some("bytea".into()),
                nullable: Some(true),
            },
        );

        let image = DbRowImage::from_json_object("users", &row, &catalog)
            .expect("users row image built even with nullable bytea");
        let sql = build_insert_sql(Some("deja_signin"), &image)
            .expect("nullable bytea NULL must not skip the users row");

        assert!(
            sql.contains("\"totp_secret\"") && sql.contains("NULL"),
            "nullable bytea columns must render NULL; got: {sql}"
        );
        assert!(
            sql.contains("'user_8ab3599a75a5b997@deja.dev'"),
            "the exact signin user seed row must still materialize; got: {sql}"
        );
    }

    #[test]
    fn sql_literal_does_not_guess_bytea_from_json_shape() {
        // Without bytea column metadata, even an Encryption-looking object is
        // rendered as JSON text. Shape detection is only used after metadata says
        // the target column is bytea.
        assert!(sql_literal(&serde_json::json!({"inner": [222, 173, 190, 239]})).starts_with("'{"));
        assert!(sql_literal(&serde_json::json!({"inner": [0, 15, 255]})).starts_with("'{"));
        assert_eq!(sql_literal(&serde_json::json!("usd")), "'usd'");
        assert!(sql_literal(&serde_json::json!({"a": 1})).starts_with("'{"));
    }

    #[test]
    fn bytea_column_renderer_accepts_typed_byte_values() {
        let metadata = DbColumnMetadata {
            name: "encrypted".into(),
            type_oid: Some(17),
            type_name: Some("bytea".into()),
            nullable: Some(true),
        };
        let column = |value| DbColumnImage {
            metadata: metadata.clone(),
            value,
        };

        assert_eq!(
            sql_literal_for_column(&column(serde_json::json!({"inner": [222, 173, 190, 239]}))),
            Some("'\\xdeadbeef'::bytea".to_string())
        );
        assert_eq!(
            sql_literal_for_column(&column(serde_json::json!([0, 15, 255]))),
            Some("'\\x000fff'::bytea".to_string())
        );
        assert_eq!(
            sql_literal_for_column(&column(serde_json::json!("\\x0102ff"))),
            Some("'\\x0102ff'::bytea".to_string())
        );
        assert_eq!(
            sql_literal_for_column(&column(serde_json::json!({"inner": [300]}))),
            None
        );
    }

    #[test]
    fn json_family_columns_render_with_explicit_pg_cast() {
        let mk = |name: &str, type_oid: Option<u32>, type_name: &str| DbColumnImage {
            metadata: DbColumnMetadata {
                name: name.into(),
                type_oid,
                type_name: Some(type_name.into()),
                nullable: Some(true),
            },
            value: serde_json::json!([{"pm": "card", "types": ["credit"]}]),
        };
        // json (oid 114)
        assert_eq!(
            sql_literal_for_column(&mk("j", Some(114), "json")),
            Some("'[{\"pm\":\"card\",\"types\":[\"credit\"]}]'::json".to_string())
        );
        // jsonb (oid 3802)
        assert_eq!(
            sql_literal_for_column(&mk("jb", Some(3802), "jsonb")),
            Some("'[{\"pm\":\"card\",\"types\":[\"credit\"]}]'::jsonb".to_string())
        );
        // json[] array (oid 199, pg name `_json`). An ARRAY constructor, NOT a
        // cast of the array's text: `'[…]'::json[]` is rejected by Postgres as a
        // malformed array literal, because array literals are written `{…}`.
        assert_eq!(
            sql_literal_for_column(&mk("ja", Some(199), "_json")),
            Some("ARRAY['{\"pm\":\"card\",\"types\":[\"credit\"]}']::json[]".to_string())
        );
        // jsonb[] array (oid 3807, pg name `_jsonb`)
        assert_eq!(
            sql_literal_for_column(&mk("jba", Some(3807), "_jsonb")),
            Some("ARRAY['{\"pm\":\"card\",\"types\":[\"credit\"]}']::jsonb[]".to_string())
        );
        // type_name alone (no oid) also detects: `json[]`
        assert_eq!(
            sql_literal_for_column(&mk("jad", None, "json[]")),
            Some("ARRAY['{\"pm\":\"card\",\"types\":[\"credit\"]}']::json[]".to_string())
        );
        // Multi-element and empty arrays both stay well-formed.
        let multi = DbColumnImage {
            metadata: DbColumnMetadata {
                name: "pme".into(),
                type_oid: Some(199),
                type_name: Some("_json".into()),
                nullable: Some(true),
            },
            value: serde_json::json!([{"a": 1}, {"b": 2}]),
        };
        assert_eq!(
            sql_literal_for_column(&multi),
            Some("ARRAY['{\"a\":1}', '{\"b\":2}']::json[]".to_string())
        );
        let empty = DbColumnImage {
            metadata: multi.metadata.clone(),
            value: serde_json::json!([]),
        };
        assert_eq!(
            sql_literal_for_column(&empty),
            Some("ARRAY[]::json[]".to_string())
        );
    }

    #[test]
    fn json_columns_are_compared_as_jsonb_in_readback() {
        // Postgres `json` has no equality operator, so comparing it directly in
        // the readback fails with "operator does not exist: json = json" and the
        // verification is lost. Both sides go through jsonb.
        let scalar = DbColumnImage {
            metadata: DbColumnMetadata {
                name: "cfg".into(),
                type_oid: Some(114),
                type_name: Some("json".into()),
                nullable: Some(true),
            },
            value: serde_json::json!({"a": 1}),
        };
        assert_eq!(
            db_comparison_predicate("cfg", &scalar),
            Some("\"cfg\"::jsonb IS NOT DISTINCT FROM ('{\"a\":1}'::json)::jsonb".to_string())
        );

        let array = DbColumnImage {
            metadata: DbColumnMetadata {
                name: "pme".into(),
                type_oid: Some(199),
                type_name: Some("_json".into()),
                nullable: Some(true),
            },
            value: serde_json::json!([{"a": 1}]),
        };
        assert_eq!(
            db_comparison_predicate("pme", &array),
            Some(
                "\"pme\"::jsonb[] IS NOT DISTINCT FROM (ARRAY['{\"a\":1}']::json[])::jsonb[]"
                    .to_string()
            )
        );

        // jsonb already compares, and non-json columns are untouched.
        let plain = DbColumnImage {
            metadata: DbColumnMetadata::unknown("profile_name"),
            value: serde_json::json!("US_default"),
        };
        assert_eq!(
            db_comparison_predicate("profile_name", &plain),
            Some("\"profile_name\" IS NOT DISTINCT FROM 'US_default'".to_string())
        );
    }

    #[test]
    fn sql_cast_kind_classifier_precedence() {
        // Precedence lock: the TryFrom classifier must bind array spellings
        // BEFORE scalar json/jsonb, else `_json`/`json[]` would shadow to Json.
        assert_eq!(
            SqlCastKind::try_from(&DbColumnMetadata {
                name: "pme".into(),
                type_oid: Some(199),
                type_name: Some("_json".into()),
                nullable: Some(true),
            }),
            Ok(SqlCastKind::JsonArray)
        );
        // type_name alone (no oid) still classifies — recorder may supply only one.
        assert_eq!(
            SqlCastKind::try_from(&DbColumnMetadata {
                name: "x".into(),
                type_oid: None,
                type_name: Some("jsonb[]".into()),
                nullable: None,
            }),
            Ok(SqlCastKind::JsonbArray)
        );
        // Unknown type is Err (renderer maps to Untyped -> plain quote or fail-closed).
        assert_eq!(
            SqlCastKind::try_from(&DbColumnMetadata {
                name: "x".into(),
                type_oid: Some(1009), // _text
                type_name: Some("_text".into()),
                nullable: Some(true),
            }),
            Err(())
        );
    }

    #[test]
    fn uncatalogued_nonscalar_column_still_renders() {
        // The complement of the fail-closed case: NO type metadata at all. The
        // plain-quote path stays (pg coerces an unknown-type literal into a
        // json/jsonb column, so skipping would lose rows that seed correctly
        // today) — but this is the shape that produces "malformed array literal"
        // when the column turns out to be array-typed, so the renderer logs it.
        // A column reaching here means the pg catalog did not cover it.
        let column = DbColumnImage {
            metadata: DbColumnMetadata::unknown("payment_methods_enabled"),
            value: serde_json::json!([{"payment_method": "card"}]),
        };
        assert_eq!(
            sql_literal_for_column(&column),
            Some("'[{\"payment_method\":\"card\"}]'".to_string())
        );

        // Scalars in an uncatalogued column are unambiguous and stay quiet.
        let scalar = DbColumnImage {
            metadata: DbColumnMetadata::unknown("profile_name"),
            value: serde_json::json!("US_default"),
        };
        assert_eq!(
            sql_literal_for_column(&scalar),
            Some("'US_default'".to_string())
        );
    }

    #[test]
    fn unhandled_typed_nonscalar_column_fails_closed() {
        // A column with authoritative type metadata that is neither bytea nor
        // the json family must NOT silently wrap a non-scalar value in quotes.
        let metadata = DbColumnMetadata {
            name: "tags".into(),
            type_oid: Some(1009), // text[]
            type_name: Some("_text".into()),
            nullable: Some(true),
        };
        let column = DbColumnImage {
            metadata: metadata.clone(),
            value: serde_json::json!(["a", "b"]),
        };
        assert_eq!(sql_literal_for_column(&column), None);
    }

    #[test]
    fn scalar_value_in_typed_still_quotes_without_cast() {
        // Scalars (strings/numbers/bools) in typed columns keep the legacy
        // quote path — no cast appended, no regression on existing seeds.
        let metadata = DbColumnMetadata {
            name: "amount".into(),
            type_oid: Some(1700), // numeric
            type_name: Some("numeric".into()),
            nullable: Some(true),
        };
        let column = DbColumnImage {
            metadata,
            value: serde_json::json!("0.20"),
        };
        assert_eq!(sql_literal_for_column(&column), Some("'0.20'".to_string()));
    }

    #[test]
    fn redis_seed_image_keeps_physical_key_typed_payload_and_ttl_advisory() {
        let image = RedisSeedImage {
            physical_key: "corr:settlement_rate_default".to_owned(),
            payload: RedisSeedPayload::String("0.10".to_owned()),
            ttl_seconds: None,
        };

        assert_eq!(image.physical_key, "corr:settlement_rate_default");
        assert_eq!(
            image.payload.write_args(&image.physical_key),
            ["SET", "corr:settlement_rate_default", "0.10"]
        );
        assert_eq!(image.ttl_seconds, None);
    }

    #[test]
    fn create_schema_sql_clones_only_named_tables_not_all_public() {
        // The granular contract: only the touched tables are LIKE-cloned, via an
        // ARRAY filter — NOT the old blanket `pg_tables WHERE schemaname='public'` scan.
        let sql =
            build_create_schema_sql("deja_corr_1", &["users".into(), "payment_intent".into()]);
        assert!(sql.contains("CREATE SCHEMA IF NOT EXISTS \"deja_corr_1\";"));
        assert!(
            sql.contains("tablename = ANY(ARRAY['payment_intent', 'users'])"),
            "clone list must be scoped to the touched tables via ARRAY filter; got: {sql}"
        );
        assert!(
            !sql.contains("WHERE schemaname = 'public' LOOP"),
            "must NOT use the old blanket scan that clones every public table; got: {sql}"
        );
        assert!(sql.contains("INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES"));
    }

    #[test]
    fn create_schema_sql_empty_tables_is_bare_schema_only() {
        // A correlation that touches no db tables (read or write) gets a bare
        // schema — no DO block, no clone. Untouched tables fall through to public.
        let sql = build_create_schema_sql("deja_empty", &[]);
        assert_eq!(sql, "CREATE SCHEMA IF NOT EXISTS \"deja_empty\"; ");
        assert!(!sql.contains("DO $deja$"));
    }

    #[test]
    fn create_schema_sql_deduplicates_and_quotes_tables() {
        // Duplicate table names collapse; quotes are SQL-escaped.
        let sql = build_create_schema_sql(
            "deja_d",
            &["users".into(), "users".into(), "weird'name".into()],
        );
        assert!(
            sql.contains("ARRAY['users', 'weird''name']"),
            "dedup + single-quote escape; got: {sql}"
        );
    }

    // ------------------------------------------------------------------
    // Wire-faithful seeding: eligibility boundary, binary COPY stream,
    // typed-payload decode, readback predicate (issue #35).
    // ------------------------------------------------------------------

    /// One column of a [`wire_row_image`] fixture:
    /// `(name, type_oid, semantic value, wire bytes)`.
    type WireColumnFixture<'a> = (&'a str, Option<u32>, serde_json::Value, Option<Vec<u8>>);

    /// Row fixture with the wire half mirrored into the row's physical image.
    fn wire_row_image(table: &str, columns: Vec<WireColumnFixture<'_>>) -> DbRowImage {
        let wire = DbRowWireImage {
            columns: columns
                .iter()
                .map(|(name, _, _, bytes)| ((*name).to_string(), bytes.clone()))
                .collect(),
        };
        DbRowImage {
            table: table.to_string(),
            columns: columns
                .into_iter()
                .map(|(name, type_oid, value, _)| DbColumnImage {
                    metadata: DbColumnMetadata {
                        name: name.to_string(),
                        type_oid,
                        type_name: None,
                        nullable: None,
                    },
                    value,
                })
                .collect(),
            wire: Some(wire),
        }
    }

    #[test]
    fn binary_copy_stream_golden_bytes() {
        // Hand-assembled from the documented COPY binary format: 11-byte
        // signature, int32 flags 0, int32 extension length 0; per row an
        // int16 field count and length-prefixed fields (-1 = NULL); int16 -1
        // trailer.
        let row = wire_row_image(
            "t",
            vec![
                ("id", Some(25), serde_json::json!("a"), Some(b"a".to_vec())),
                (
                    "amount",
                    Some(20),
                    serde_json::json!(42),
                    Some(42_i64.to_be_bytes().to_vec()),
                ),
                ("gone", Some(1043), serde_json::Value::Null, None),
            ],
        );
        let stream = build_binary_copy_stream(&[&row]).expect("stream builds");

        let mut expected: Vec<u8> = Vec::new();
        expected.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
        expected.extend_from_slice(&[0, 0, 0, 0]); // flags
        expected.extend_from_slice(&[0, 0, 0, 0]); // extension length
        expected.extend_from_slice(&[0, 3]); // field count
        expected.extend_from_slice(&[0, 0, 0, 1]); // len("a")
        expected.push(b'a');
        expected.extend_from_slice(&[0, 0, 0, 8]); // len(int8)
        expected.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 42]);
        expected.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]); // NULL
        expected.extend_from_slice(&[0xff, 0xff]); // trailer
        assert_eq!(stream, expected);
    }

    /// The readback parser is the builder's exact inverse: parsing a built
    /// stream returns the captured field bytes, byte for byte, NULLs
    /// preserved. This round trip is what makes wire readback the seed's
    /// inverse rather than a second, disagreeing codec.
    #[test]
    fn binary_copy_stream_parse_is_the_builders_inverse() {
        let row = wire_row_image(
            "t",
            vec![
                ("id", Some(25), serde_json::json!("a"), Some(b"a".to_vec())),
                (
                    "amount",
                    Some(20),
                    serde_json::json!(42),
                    Some(42_i64.to_be_bytes().to_vec()),
                ),
                ("gone", Some(1043), serde_json::Value::Null, None),
            ],
        );
        let stream = build_binary_copy_stream(&[&row]).expect("stream builds");
        let parsed = parse_binary_copy_stream(&stream).expect("stream parses");
        assert_eq!(
            parsed,
            vec![vec![
                Some(b"a".to_vec()),
                Some(42_i64.to_be_bytes().to_vec()),
                None,
            ]]
        );
        // Zero tuples (header + trailer only) parses to an empty set — the
        // "row not found" signal.
        let empty = build_binary_copy_stream(&[]).expect("empty stream builds");
        assert_eq!(
            parse_binary_copy_stream(&empty).expect("empty parses"),
            Vec::<Vec<Option<Vec<u8>>>>::new()
        );
        // A truncated stream refuses loudly instead of returning partial rows.
        assert!(parse_binary_copy_stream(&stream[..stream.len() - 3]).is_err());
    }

    /// The COPY-OUT readback command selects the image's columns in capture
    /// order, keyed by EVERY column of the row's schema key, quoted like every
    /// other identifier on the seed path. `payment_attempt` is keyed by
    /// `(attempt_id, merchant_id)`, so a one-column predicate would read back
    /// the wrong row — or several.
    #[test]
    fn wire_copy_out_command_renders_quoted_and_keyed() {
        let command = wire_copy_out_command(
            Some("deja_c1"),
            "payment_attempt",
            &["attempt_id", "connector_transaction_id"],
            &[
                ("attempt_id".to_owned(), "pay_x_1".to_owned()),
                ("merchant_id".to_owned(), "merch_1".to_owned()),
            ],
        );
        assert_eq!(
            command,
            "\\copy (SELECT \"attempt_id\", \"connector_transaction_id\" FROM \"deja_c1\".\"payment_attempt\" \
             WHERE \"attempt_id\" = 'pay_x_1' AND \"merchant_id\" = 'merch_1') TO PSTDOUT WITH (FORMAT binary)"
        );
    }

    /// The wire readback predicate comes from the row image itself, and carries
    /// EVERY column of the schema key — in key order. A row missing part of its
    /// key yields no predicate at all rather than one that addresses a prefix:
    /// reading back "some row with this payment_id" would pass against a
    /// different merchant's row.
    #[test]
    fn wire_readback_predicate_carries_the_whole_schema_key() {
        register_test_schema_identity();
        let payment_id = (
            "payment_id",
            Some(1043),
            serde_json::json!("pay_1"),
            Some(b"pay_1".to_vec()),
        );
        let merchant_id = (
            "merchant_id",
            Some(1043),
            serde_json::json!("merch_1"),
            Some(b"merch_1".to_vec()),
        );
        let status = (
            "status",
            Some(19465),
            serde_json::json!("succeeded"),
            Some(b"succeeded".to_vec()),
        );

        let whole = wire_row_image(
            "payment_intent",
            vec![payment_id.clone(), merchant_id, status.clone()],
        );
        assert_eq!(
            wire_readback_row_predicate(&whole),
            Some(vec![
                ("payment_id".to_owned(), "pay_1".to_owned()),
                ("merchant_id".to_owned(), "merch_1".to_owned()),
            ]),
            "both key columns, in schema key order"
        );

        let partial = wire_row_image("payment_intent", vec![payment_id, status]);
        assert_eq!(
            wire_readback_row_predicate(&partial),
            None,
            "half a composite key identifies no row"
        );
    }

    #[test]
    fn binary_copy_stream_passes_every_type_family_verbatim() {
        // The builder interprets NOTHING: whatever `typsend` produced at
        // record time is emitted length-prefixed, byte for byte. One field
        // per family, fixtures written out from the documented binary forms.
        let families: Vec<(&str, u32, serde_json::Value, Vec<u8>)> = vec![
            ("c_text", 25, serde_json::json!("abc"), b"abc".to_vec()),
            ("c_varchar", 1043, serde_json::json!("v"), b"v".to_vec()),
            (
                "c_int8",
                20,
                serde_json::json!(7),
                7_i64.to_be_bytes().to_vec(),
            ),
            ("c_bool", 16, serde_json::json!(true), vec![0x01]),
            // numeric 1.5: ndigits=2? No — opaque fixture: the builder must
            // not care whether these bytes are even valid numeric internals.
            (
                "c_numeric",
                1700,
                serde_json::json!("1.5"),
                vec![0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01],
            ),
            // timestamptz: microseconds since 2000-01-01 as int8.
            (
                "c_timestamptz",
                1184,
                serde_json::json!("2000-01-01T00:00:01Z"),
                1_000_000_i64.to_be_bytes().to_vec(),
            ),
            // jsonb: version byte 0x01 + document text.
            ("c_jsonb", 3802, serde_json::json!({"a": 1}), {
                let mut bytes = vec![0x01];
                bytes.extend_from_slice(br#"{"a": 1}"#);
                bytes
            }),
            ("c_bytea", 17, serde_json::json!([0, 255]), vec![0x00, 0xff]),
        ];
        let row = wire_row_image(
            "t",
            families
                .iter()
                .map(|(name, oid, value, bytes)| {
                    (*name, Some(*oid), value.clone(), Some(bytes.clone()))
                })
                .collect(),
        );
        let stream = build_binary_copy_stream(&[&row]).expect("stream builds");

        let mut expected: Vec<u8> = Vec::new();
        expected.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
        expected.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]);
        expected.extend_from_slice(&(families.len() as i16).to_be_bytes());
        for (_, _, _, bytes) in &families {
            expected.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            expected.extend_from_slice(bytes);
        }
        expected.extend_from_slice(&[0xff, 0xff]);
        assert_eq!(stream, expected);

        // And the whole row is COPY-eligible: every family above is built-in.
        assert_eq!(wire_copy_eligibility(&row), Ok(()));
    }

    #[test]
    fn eligibility_accepts_builtin_oids_and_nulls() {
        let row = wire_row_image(
            "payment_attempt",
            vec![
                (
                    "attempt_id",
                    Some(1043),
                    serde_json::json!("att_1"),
                    Some(b"att_1".to_vec()),
                ),
                // The poison column: tagged-enum SEMANTIC value in a varchar
                // column; the PHYSICAL bytes are the raw text. Covered —
                // varchar is OID 1043, built-in.
                (
                    "connector_transaction_id",
                    Some(1043),
                    serde_json::json!({"TxnId": "txn_raw"}),
                    Some(b"txn_raw".to_vec()),
                ),
                ("capture_on", Some(1114), serde_json::Value::Null, None),
            ],
        );
        assert_eq!(wire_copy_eligibility(&row), Ok(()));
    }

    #[test]
    fn eligibility_accepts_user_defined_types_only_with_label_text_evidence() {
        // pg enum: wire bytes ARE the label text — provably portable.
        let enum_row = wire_row_image(
            "payment_attempt",
            vec![(
                "status",
                Some(16385),
                serde_json::json!("charged"),
                Some(b"charged".to_vec()),
            )],
        );
        assert_eq!(wire_copy_eligibility(&enum_row), Ok(()));

        // Same OID, but bytes differ from the semantic string (a serde rename,
        // or a non-textual user-defined type): no evidence, no COPY.
        let renamed = wire_row_image(
            "payment_attempt",
            vec![(
                "status",
                Some(16385),
                serde_json::json!("Charged"),
                Some(b"charged".to_vec()),
            )],
        );
        assert!(wire_copy_eligibility(&renamed).is_err());

        // Non-string semantic value in a user-defined column (composite,
        // array of enums): never eligible.
        let composite = wire_row_image(
            "t",
            vec![(
                "extra",
                Some(16400),
                serde_json::json!({"a": 1}),
                Some(vec![0x00, 0x01]),
            )],
        );
        assert!(wire_copy_eligibility(&composite).is_err());
    }

    #[test]
    fn eligibility_rejects_unsound_rows() {
        // No physical image at all (old tape).
        let mut plain = wire_row_image(
            "t",
            vec![("id", Some(25), serde_json::json!("a"), Some(b"a".to_vec()))],
        );
        plain.wire = None;
        assert_eq!(
            wire_copy_eligibility(&plain),
            Err("no physical image".to_string())
        );

        // Wire NULL under a non-null semantic value.
        let misaligned = wire_row_image("t", vec![("id", Some(25), serde_json::json!("a"), None)]);
        assert!(wire_copy_eligibility(&misaligned).is_err());

        // Wire bytes without a type oid.
        let no_oid = wire_row_image(
            "t",
            vec![("id", None, serde_json::json!("a"), Some(b"a".to_vec()))],
        );
        assert!(wire_copy_eligibility(&no_oid).is_err());

        // A column missing from the wire map.
        let mut missing = wire_row_image(
            "t",
            vec![("id", Some(25), serde_json::json!("a"), Some(b"a".to_vec()))],
        );
        if let Some(wire) = missing.wire.as_mut() {
            wire.columns.clear();
        }
        assert!(wire_copy_eligibility(&missing).is_err());
    }

    #[test]
    fn typed_payload_wire_half_decodes_onto_the_row() {
        let payload = serde_json::json!({
            "deja_image": "db_row",
            "version": 1,
            "table": "payment_attempt",
            "wire_format": "binary",
            "columns": [
                {"name": "attempt_id", "type_oid": 1043, "value": "att_1", "wire": "6174745f31"},
                {"name": "capture_on", "value": null},
            ],
        });
        let catalog = DbCatalog::default();
        let rows = db_row_images_from_typed_payload("payment_attempt", &payload, &catalog)
            .expect("typed rows");
        assert_eq!(rows.len(), 1);
        let wire = rows[0].wire.as_ref().expect("wire image decoded");
        assert_eq!(
            wire.columns.get("attempt_id"),
            Some(&Some(b"att_1".to_vec()))
        );
        assert_eq!(wire.columns.get("capture_on"), Some(&None));
        assert_eq!(wire_copy_eligibility(&rows[0]), Ok(()));
    }

    #[test]
    fn typed_payload_bad_hex_or_missing_flag_yields_no_wire_image() {
        let bad_hex = serde_json::json!({
            "deja_image": "db_row",
            "version": 1,
            "table": "t",
            "wire_format": "binary",
            "columns": [
                {"name": "id", "type_oid": 25, "value": "a", "wire": "zz"},
            ],
        });
        let catalog = DbCatalog::default();
        let rows = db_row_images_from_typed_payload("t", &bad_hex, &catalog).expect("typed rows");
        assert_eq!(rows[0].wire, None, "undecodable hex drops the wire half");

        let no_flag = serde_json::json!({
            "deja_image": "db_row",
            "version": 1,
            "table": "t",
            "columns": [
                {"name": "id", "type_oid": 25, "value": "a", "wire": "61"},
            ],
        });
        let rows = db_row_images_from_typed_payload("t", &no_flag, &catalog).expect("typed rows");
        assert_eq!(
            rows[0].wire, None,
            "wire bytes without the row-level binary flag are not a physical image"
        );
    }

    #[test]
    fn readback_predicate_skips_unrenderable_wire_seeded_columns_only() {
        // Wire-seeded row with the poison column (typed varchar + tagged
        // object → fail-closed renderer refuses a literal): the full-row
        // readback predicate is built WITHOUT that column instead of failing.
        let wire_row = wire_row_image(
            "payment_attempt",
            vec![
                (
                    "attempt_id",
                    Some(1043),
                    serde_json::json!("att_1"),
                    Some(b"att_1".to_vec()),
                ),
                (
                    "connector_transaction_id",
                    Some(1043),
                    serde_json::json!({"TxnId": "txn_raw"}),
                    Some(b"txn_raw".to_vec()),
                ),
            ],
        );
        let sql = build_count_sql(Some("corr_1"), &wire_row, None).expect("readback renders");
        assert!(sql.contains("\"attempt_id\""));
        assert!(
            !sql.contains("connector_transaction_id"),
            "unrenderable wire column excluded from the predicate: {sql}"
        );

        // Control: the same row WITHOUT a wire image keeps the old
        // all-or-nothing behavior.
        let mut plain = wire_row;
        plain.wire = None;
        assert!(build_count_sql(Some("corr_1"), &plain, None).is_none());
    }

    #[test]
    fn wire_copy_commands_stage_copy_and_land_with_conflict_tolerance() {
        let commands = wire_copy_commands(
            "\"corr_1\".\"payment_attempt\"",
            &["attempt_id", "connector_transaction_id"],
        );
        assert_eq!(
            commands,
            vec![
                "CREATE TEMP TABLE _deja_wire_seed AS SELECT \"attempt_id\", \"connector_transaction_id\" FROM \"corr_1\".\"payment_attempt\" WITH NO DATA".to_string(),
                "\\copy _deja_wire_seed from pstdin with (format binary)".to_string(),
                "INSERT INTO \"corr_1\".\"payment_attempt\" (\"attempt_id\", \"connector_transaction_id\") SELECT \"attempt_id\", \"connector_transaction_id\" FROM _deja_wire_seed ON CONFLICT DO NOTHING".to_string(),
            ]
        );
    }
}
