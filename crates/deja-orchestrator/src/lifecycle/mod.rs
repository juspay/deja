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
            let image = image.trim().to_owned();
            // "deja-demo" is the SPA's historical no-candidate default: the
            // legacy compose self-build (overlay default image, `--build`).
            // An empty ref means the same.
            if image.is_empty() || image == "deja-demo" {
                return Ok(());
            }
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
                source_ref: image.clone(),
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
        "driving payment workload (HS → Kafka → Vector → MinIO)",
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
        "waiting for recording to land in MinIO (S3)",
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
        &root.recording_events_path(&recording_id),
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
    let recording_path = root.recording_events_path(&recording_id);

    // Render the lookup table (whole-document JSON; round-trips through both the
    // candidate's LocalFileLookupSource and the divergence detector).
    set_stage(root, run, ctx, 2, total, "rendering lookup table");
    let table = crate::lookup::render_lookup_table(&recording_path, &recording_id, 1)
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
    let seed_certificate = materialize_seed_plan(&store, root, &recording_id, &run.run_id);
    let seed_certificate_path = root.seed_certificate_path(&run.run_id);
    match write_json(&seed_certificate_path, &seed_certificate) {
        Ok(()) => ctx.artifact(
            Some(&recording_id),
            "seed_certificate",
            &seed_certificate_path,
        ),
        Err(e) => eprintln!("lifecycle: seed certificate write failed: {e}; continuing"),
    }
    run_kernel(
        &demo.kernel_bin,
        demo.replay_port,
        root,
        ctx,
        &recording_id,
        &run.run_id,
        run.spec.correlation_filter.as_deref(),
    )?;

    // Compose: the orchestrator serves artifacts from its own state dir.
    score_and_register(root, run, ctx, &recording_id, total, &ArtifactSink::Local)
}

/// Stage 1 (shared by the compose worker and the in-pod runner): resolve the
/// recording identity and materialize `events.jsonl`.
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
            set_stage(
                root,
                run,
                ctx,
                1,
                total,
                "pulling recording from MinIO (S3)",
            );
            if !root.recording_events_path(&recording_id).exists() {
                pull_recording(root, ctx, &recording_id)?;
            }
            recording_id
        }
    };
    run.recording_id = Some(recording_id.clone());
    ctx.run_recording(&recording_id);
    if !root.recording_events_path(&recording_id).exists() {
        return Err(format!(
            "recording {recording_id} not found in S3 or on disk"
        ));
    }
    Ok(recording_id)
}

/// Final stage (shared): score the run, report the verdict, register the
/// The replay-run stream artifacts [`score_and_register`] publishes, as
/// `(kind, s3-filename)`. One list so the sink loop and the DB-constraint
/// coverage test stay in agreement. `observed` also carries the replay-side
/// execution-graph nodes (`DejaRecord::GraphNode`).
const REPLAY_STREAM_ARTIFACTS: [(&str, &str); 5] = [
    ("lookup_table", "lookup_table.jsonl"),
    ("observed", "observed.jsonl"),
    ("http_diffs", "http_diffs.jsonl"),
    ("scorecard", "scorecard.json"),
    ("call_ledger", "call_ledger.jsonl"),
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

/// Extract the record-side execution-graph nodes (`DejaRecord::GraphNode`) from
/// a recording's events tape into a compact `record_graph.jsonl`: the STRUCTURE
/// of the recorded run's cascade (span ids, parents, names, level, timing, span
/// fields), NOT its boundary payloads (args/results). The in-pod runner already
/// holds the recording locally (it drove replay off it); emitting just the graph
/// nodes as a run artifact lets the record side reach the dashboard through the
/// SAME S3 sink as the replay side, WITHOUT copying the sensitive recording tape
/// off the pod. Returns the node count (0 ⇒ no local recording / no nodes ⇒
/// nothing to publish).
fn write_record_graph_nodes(
    recording_path: &std::path::Path,
    dest: &std::path::Path,
) -> Result<usize, String> {
    let Ok(file) = std::fs::File::open(recording_path) else {
        return Ok(0); // no recording on disk (nothing to derive the record graph from)
    };
    let mut out = String::new();
    let mut nodes = 0usize;
    for line in BufRead::lines(std::io::BufReader::new(file)).map_while(Result::ok) {
        if line.trim().is_empty() {
            continue;
        }
        // Keep ONLY graph nodes — boundary events (the payloads) never leave.
        if let Ok(deja::DejaRecord::GraphNode(_)) = serde_json::from_str::<deja::DejaRecord>(&line) {
            out.push_str(&line);
            out.push('\n');
            nodes += 1;
        }
    }
    if nodes == 0 {
        return Ok(0);
    }
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
    }
    std::fs::write(dest, out).map_err(|e| format!("write {}: {e}", dest.display()))?;
    Ok(nodes)
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
    set_stage(root, run, ctx, total, total, "scoring divergence (byte-exact)");
    let card = crate::divergence::detect_and_score(root, &run.run_id)
        .map_err(|e| format!("score: {e}"))?;
    let verdict_line = format!(
        "run {} verdict pass={} ({})",
        run.run_id, card.verdict.pass, card.verdict.reason
    );
    eprintln!("lifecycle: {verdict_line}");
    ctx.log("scoring divergence (byte-exact)", &verdict_line);
    let verdict = if card.verdict.inconclusive {
        "inconclusive"
    } else if card.verdict.pass {
        "pass"
    } else {
        "fail"
    };
    ctx.result(Some(verdict), serde_json::to_value(&card).ok().as_ref());

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

    // Record-side execution graph: the recorded run's cascade STRUCTURE (graph
    // nodes only — never the boundary payloads), derived from the recording the
    // runner already holds locally. Published through the SAME sink so the
    // dashboard's `/graph` record side renders for in-pod runs too, WITHOUT the
    // sensitive recording tape ever leaving the pod. (Compose registers the local
    // path; the orchestrator also reads the recording directly there, so this is
    // belt-and-suspenders — but it keeps both modes on one artifact contract.)
    let record_graph_path = root.record_graph_path(&run.run_id);
    match write_record_graph_nodes(&root.recording_events_path(recording_id), &record_graph_path) {
        Ok(0) => {} // no recording on disk / no graph nodes — nothing to publish
        Ok(node_count) => {
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
        Err(e) => eprintln!("lifecycle: record-graph extract failed: {e}"),
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
    let recording_path = root.recording_events_path(&recording_id);

    set_stage(root, run, ctx, 2, total, "rendering lookup table");
    let table = crate::lookup::render_lookup_table(&recording_path, &recording_id, 1)
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
            cmd.args(&argv[1..])
                .env("DATABASE_URL", &opts.database_url);
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
    let seed_certificate = materialize_seed_plan(&store, root, &recording_id, &run.run_id);
    let seed_certificate_path = root.seed_certificate_path(&run.run_id);
    match write_json(&seed_certificate_path, &seed_certificate) {
        Ok(()) => ctx.artifact(
            Some(&recording_id),
            "seed_certificate",
            &seed_certificate_path,
        ),
        Err(e) => eprintln!("lifecycle: seed certificate write failed: {e}; continuing"),
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
        &format!("stores seeded; readiness sentinel published at {}", ready.display()),
    );

    set_stage(root, run, ctx, 5, total, "driving recorded requests (kernel)");
    wait_health(opts.router_port, Duration::from_secs(240))?;
    run_kernel(
        &opts.kernel_bin,
        opts.router_port,
        root,
        ctx,
        &recording_id,
        &run.run_id,
        run.spec.correlation_filter.as_deref(),
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

fn run_iterations(run: &Run) -> u64 {
    run.spec
        .workload
        .get("iterations")
        .and_then(|v| v.as_u64())
        .unwrap_or(1)
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

/// Seed a single redis key the EU-settlement demo reads. The settlement READ is
/// now a RAW fred GET (leaf boundary) against redis, so the seed lives in redis,
/// not pg. Mirrors `flush_redis`'s `docker compose exec -T redis-standalone
/// redis-cli ...` pattern. Best-effort: a failure logs and continues.
fn seed_redis(
    store: &StoreExec,
    key: &str,
    value: &str,
) -> (SeedMaterializationStatus, SeedReadback) {
    let image = RedisSeedImage::string(key, value);
    match seed_redis_image(store, &image) {
        Ok(()) => (
            SeedMaterializationStatus::Materialized,
            readback_redis(store, key, value),
        ),
        Err(message) => (
            SeedMaterializationStatus::Failed,
            SeedReadback::error(message),
        ),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RedisSeedValueType {
    String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RedisSeedImage {
    physical_key: String,
    physical_key_bytes: Vec<u8>,
    value_type: RedisSeedValueType,
    raw_value: String,
    raw_value_bytes: Vec<u8>,
    ttl_seconds: Option<i64>,
}

impl RedisSeedImage {
    fn string(key: &str, value: &str) -> Self {
        Self {
            physical_key: key.to_string(),
            physical_key_bytes: key.as_bytes().to_vec(),
            value_type: RedisSeedValueType::String,
            raw_value: value.to_string(),
            raw_value_bytes: value.as_bytes().to_vec(),
            ttl_seconds: None,
        }
    }
}

fn seed_redis_image(store: &StoreExec, image: &RedisSeedImage) -> Result<(), String> {
    let mut cmd = store.redis_cli(&[
        "SET",
        image.physical_key.as_str(),
        image.raw_value.as_str(),
    ]);
    eprintln!(
        "lifecycle: {} (redis key {} byte(s), value {:?}, ttl {:?})",
        store_exec::describe(&cmd),
        image.physical_key_bytes.len(),
        image.value_type,
        image.ttl_seconds
    );
    match cmd.status() {
        Ok(status) if status.success() => Ok(()),
        Ok(status) => {
            let message = format!("seed_redis exited {status}");
            eprintln!("lifecycle: {message}; continuing (best-effort)");
            Err(message)
        }
        Err(e) => {
            let message = format!("could not run seed_redis: {e}");
            eprintln!("lifecycle: {message}; continuing (best-effort)");
            Err(message)
        }
    }
}

fn readback_redis(store: &StoreExec, key: &str, expected: &str) -> SeedReadback {
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
            serde_json::json!(expected),
            format!("redis EXISTS returned {exists:?} after SET"),
        );
    }

    let output = match redis_cli_output(store, &["--raw", "GET", key]) {
        Ok(output) if output.status.success() => output,
        Ok(output) => {
            return SeedReadback::error(format!(
                "redis GET readback exited {}; stderr='{}'",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        Err(message) => return SeedReadback::error(message),
    };
    let observed_bytes = strip_redis_cli_terminator(&output.stdout);
    let expected_bytes = expected.as_bytes();
    if observed_bytes == expected_bytes {
        SeedReadback::matched(
            serde_json::json!(expected),
            serde_json::json!(String::from_utf8_lossy(observed_bytes).to_string()),
        )
    } else {
        SeedReadback::mismatched(
            serde_json::json!({
                "utf8": expected,
                "len": expected_bytes.len(),
            }),
            serde_json::json!({
                "utf8": String::from_utf8_lossy(observed_bytes).to_string(),
                "len": observed_bytes.len(),
            }),
            "redis GET returned a different value after SET",
        )
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
    readback_matched: usize,
    readback_missing: usize,
    readback_mismatched: usize,
    readback_errors: usize,
    readback_not_run: usize,
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
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum SeedMaterializationStatus {
    Materialized,
    Skipped,
    Failed,
    Unsupported,
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
        self.summary.planned += 1;
        match entry.materialization {
            SeedMaterializationStatus::Materialized => self.summary.materialized += 1,
            SeedMaterializationStatus::Skipped => self.summary.skipped += 1,
            SeedMaterializationStatus::Failed => self.summary.failed += 1,
            SeedMaterializationStatus::Unsupported => self.summary.unsupported += 1,
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
    root: &HarnessRoot,
    recording_id: &str,
    run_id: &str,
) -> SeedCertificate {
    let recording_path = root.recording_events_path(recording_id);
    let events = read_recording_events(&recording_path);
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

    // DB isolation + seeding is ON by default (R1: real seeding). `DEJA_SEED_DB=0`
    // is a kill-switch that falls back to the old shared-pg self-rebuild. When on,
    // each correlation gets its own pg schema (full structural clone of public)
    // that the router routes to via `search_path`, and its seed rows land there.
    let seed_db_enabled = std::env::var("DEJA_SEED_DB")
        .ok()
        .map(|v| v.trim() != "0")
        .unwrap_or(true);

    // Create one isolated schema per correlation BEFORE seeding — every correlation
    // that replays needs its schema to exist (the router routes ALL its queries
    // there, not just seeded tables), so this is independent of whether a
    // correlation has seed entries.
    if seed_db_enabled {
        for corr in correlations.iter().filter_map(|c| c.as_deref()) {
            create_db_schema(store, &deja::db_schema_for(corr));
        }
    }

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
        if plan.is_empty() {
            continue;
        }
        // The per-correlation pg schema (DB isolation): same derivation the router
        // uses for `search_path`, so seeded rows land where replay reads them.
        let db_schema = corr.as_deref().map(deja::db_schema_for);
        let mut entries = plan.iter().collect::<Vec<_>>();
        entries.sort_by_key(|entry| seed_materialization_priority(entry));
        for entry in entries {
            match entry.boundary.as_str() {
                // REDIS — render the value to the raw string redis holds (a JSON
                // string becomes its inner text, so "0.20" not "\"0.20\""), then
                // write it under the per-correlation namespace.
                "redis" => {
                    let key = match corr {
                        Some(c) => format!("{c}:{}", entry.key),
                        None => entry.key.clone(),
                    };
                    // A non-scalar RESP3 value the string `SET` seeder can't
                    // represent is skipped LOUDLY (an explicit certificate
                    // entry), never seeded as wrapper text.
                    let (materialization, readback) = match render_redis_seed_value(&entry.value) {
                        Some(value) => seed_redis(store, &key, &value),
                        None => (
                            SeedMaterializationStatus::Skipped,
                            SeedReadback::not_run(
                                "redis value is not a scalar string SET can materialize",
                            ),
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
                    let (materialization, readback) = seed_db(
                        store,
                        db_schema.as_deref(),
                        &db_catalog,
                        &entry.key,
                        entry.image.as_ref(),
                        &entry.value,
                    );
                    certificate.push(SeedCertificateEntry::new(
                        corr,
                        entry,
                        None,
                        db_schema.clone(),
                        materialization,
                        readback,
                    ));
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
) -> (SeedMaterializationStatus, SeedReadback) {
    let target = match db_seed_target_from_key(key) {
        Some(target) => target,
        None => {
            return (
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
        return (
            SeedMaterializationStatus::Skipped,
            SeedReadback::not_run(message),
        );
    }

    let mut sql = String::new();
    for row in &rows {
        let Some(stmt) = build_insert_sql(schema, row) else {
            let message = format!(
                "seed_db {} {} could not render an insert for a seedable row",
                target.kind, target.table
            );
            eprintln!("lifecycle: {message}; skipping this seed entry");
            return (
                SeedMaterializationStatus::Failed,
                SeedReadback::error(message),
            );
        };
        sql.push_str(&stmt);
        sql.push('\n');
    }
    if sql.is_empty() {
        return (
            SeedMaterializationStatus::Skipped,
            SeedReadback::not_run("seed_db rendered no insert SQL"),
        );
    }
    let row_count = sql.lines().count();

    eprintln!(
        "lifecycle: seed_db {} {} ({row_count} row(s))",
        target.kind, target.table
    );
    if seed_contains_null_column(&rows, "totp_secret") {
        eprintln!(
            "lifecycle: seed_db {} {} NULL columns: totp_secret=NULL",
            target.kind, target.table
        );
    }
    match store.psql(&[], true, &sql).output() {
        Ok(output) if output.status.success() => (
            SeedMaterializationStatus::Materialized,
            readback_db(store, schema, &target, &rows),
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
            (
                SeedMaterializationStatus::Failed,
                SeedReadback::error(message),
            )
        }
        Err(e) => {
            let message = format!("could not run seed_db {}: {e}", target.table);
            eprintln!("lifecycle: {message}; continuing (best-effort)");
            (
                SeedMaterializationStatus::Failed,
                SeedReadback::error(message),
            )
        }
    }
}
fn readback_db(
    store: &StoreExec,
    schema: Option<&str>,
    target: &DbSeedTarget,
    rows: &[DbRowImage],
) -> SeedReadback {
    let mut full_sql = String::new();
    for row in rows {
        let Some(stmt) = build_count_sql(schema, row, None) else {
            return SeedReadback::error("cannot render db readback full-row predicate");
        };
        full_sql.push_str(&stmt);
        full_sql.push('\n');
    }
    let full_counts = match run_db_readback_counts(store, &full_sql, rows.len()) {
        Ok(counts) => counts,
        Err(message) => return SeedReadback::error(message),
    };
    let expected = serde_json::json!({
        "rows": rows.len(),
        "table": target.table,
        "kind": target.kind,
    });
    let mut observed = serde_json::json!({
        "full_row_matches": full_counts.clone(),
    });
    if full_counts.iter().all(|count| *count > 0) {
        return SeedReadback::matched(expected, observed);
    }

    if let Some(filter) = &target.row_filter {
        let mut key_sql = String::new();
        for row in rows {
            let Some(stmt) = build_count_sql(schema, row, Some(filter)) else {
                return SeedReadback::error("cannot render db readback key predicate");
            };
            key_sql.push_str(&stmt);
            key_sql.push('\n');
        }
        let key_counts = match run_db_readback_counts(store, &key_sql, rows.len()) {
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
                predicates.push(format!(
                    "{} IS NOT DISTINCT FROM {}",
                    quote_ident(&column.metadata.name),
                    sql_literal_for_column(column)?
                ));
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
    if let Some(column) = row
        .columns
        .iter()
        .find(|column| column.metadata.name == filter.pk_column)
    {
        return Some(format!(
            "{} IS NOT DISTINCT FROM {}",
            quote_ident(&column.metadata.name),
            sql_literal_for_column(column)?
        ));
    }
    let column = DbColumnImage {
        metadata: DbColumnMetadata::unknown(&filter.pk_column),
        value: serde_json::Value::String(filter.pk_value.clone()),
    };
    Some(format!(
        "{} IS NOT DISTINCT FROM {}",
        quote_ident(&filter.pk_column),
        sql_literal_for_column(&column)?
    ))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DbSeedTarget {
    table: String,
    kind: &'static str,
    row_filter: Option<DbRowFilter>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DbRowFilter {
    pk_column: String,
    pk_value: String,
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
        deja::StateKey::DbRow {
            pk_column,
            pk_value,
            ..
        } => Some(DbSeedTarget {
            table,
            kind: "row",
            row_filter: Some(DbRowFilter {
                pk_column: pk_column.clone(),
                pk_value: pk_value.clone(),
            }),
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
    row.columns.iter().any(|column| {
        column.metadata.name == filter.pk_column
            && db_seed_wire_value(&column.value) == filter.pk_value
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

/// Create the per-correlation isolation schema (R1) as a FULL structural clone of
/// `public`: `CREATE SCHEMA` + one `CREATE TABLE … (LIKE public.t INCLUDING
/// DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES)` per public table. The
/// router routes this correlation's queries here via `search_path`, so EVERY table
/// must exist (writes resolve to the schema first → isolation). `LIKE` never
/// copies FOREIGN KEYS — deliberate: we seed only a subset of rows (read-before-
/// write preconditions), so FK refs would otherwise dangle. `INCLUDING INDEXES`
/// brings the PK/unique indexes that the seed UPSERT's `ON CONFLICT` needs;
/// `INCLUDING DEFAULTS` keeps SERIAL/sequence defaults so the router's own inserts
/// (which omit the serial id) still work. Best-effort: a failure logs + continues.
fn create_db_schema(store: &StoreExec, schema: &str) {
    let sql = format!(
        "CREATE SCHEMA IF NOT EXISTS \"{schema}\"; \
         DO $deja$ DECLARE r record; BEGIN \
           FOR r IN SELECT tablename FROM pg_tables WHERE schemaname = 'public' LOOP \
             EXECUTE format('CREATE TABLE IF NOT EXISTS \"{schema}\".%I \
               (LIKE public.%I INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES)', \
               r.tablename, r.tablename); \
           END LOOP; \
         END $deja$;"
    );
    eprintln!("lifecycle: create_db_schema {schema} (clone of public)");
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

    fn is_bytea(&self) -> bool {
        self.type_oid == Some(17) || self.type_name.as_deref() == Some("bytea")
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

#[derive(Debug, Clone, PartialEq)]
struct DbColumnImage {
    metadata: DbColumnMetadata,
    value: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq)]
struct DbRowImage {
    table: String,
    columns: Vec<DbColumnImage>,
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
        },
        has_producer_metadata,
    ))
}

/// Build `INSERT INTO <table> (cols...) VALUES (...) ON CONFLICT DO NOTHING`
/// from a typed row image. Values are rendered according to column metadata when
/// available; unknown metadata falls back to generic JSON-as-SQL-literal
/// rendering. `bytea` handling is gated solely by the column type metadata, not
/// by guessing object shapes globally.
fn build_insert_sql(schema: Option<&str>, row: &DbRowImage) -> Option<String> {
    if row.columns.is_empty() {
        return None;
    }
    let col_list = row
        .columns
        .iter()
        .map(|column| quote_ident(&column.metadata.name))
        .collect::<Vec<_>>()
        .join(", ");
    let mut values = Vec::with_capacity(row.columns.len());
    for column in &row.columns {
        values.push(sql_literal_for_column(column)?);
    }
    let value_list = values.join(", ");
    // Qualify the target with the per-correlation schema when isolating (R1), so
    // the row lands in that case's schema — the one the router's `search_path`
    // selects during replay. `ON CONFLICT DO NOTHING` (no target) needs no PK
    // knowledge: the cloned schema starts empty, so this only no-ops on the rare
    // intra-seed duplicate. Unqualified (→ search_path/public) when no schema.
    let qualified_table = qualified_table(schema, &row.table);
    Some(format!(
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

fn sql_literal_for_column(column: &DbColumnImage) -> Option<String> {
    if column.value.is_null() {
        return Some("NULL".to_string());
    }
    if column.metadata.is_bytea() {
        let Some(bytes) = bytea_bytes_from_typed_value(&column.value) else {
            eprintln!(
                "lifecycle: cannot render bytea seed value for column {}; skipping row",
                column.metadata.name
            );
            return None;
        };
        return Some(bytea_hex_literal(&bytes));
    }
    Some(sql_literal(&column.value))
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

/// Reconstruct the raw value redis holds from a recorded redis result, or
/// `None` when the value cannot be materialized as a string `SET`.
///
/// Two genuinely different sources feed this, and the branch reflects that:
///
/// - **Ambient/config template** — a bare JSON string (e.g. `"0.20"`) that is
///   *already* the raw text redis holds. Passed through byte-for-byte.
/// - **Recording-derived** — a serialized [`deja::value::RedisWireValue`], the
///   canonical shared type. We deserialize into it and let
///   [`RedisWireValue::to_redis_string`] produce the value redis returns. The
///   match there is exhaustive with no wildcard, so a new scalar variant is a
///   compile error rather than a silent fall-through to corruption.
///
/// `None` (a non-scalar RESP3 shape, or a `Null` miss) means "do not SET this":
/// the caller records an explicit skip in the seed certificate instead of
/// writing garbage. This is what replaced the old `to_string()` fallback, which
/// wrote the enum wrapper text (`{"BulkString":[…]}`) into redis and made the
/// replayed router branch on garbage — a false divergence.
fn render_redis_seed_value(value: &serde_json::Value) -> Option<String> {
    // Recording-derived: decode into the canonical shared type FIRST. This also
    // correctly treats a bare-string unit variant (`"Null"`, a miss that leaked
    // past the upstream filter) as "nothing to seed" rather than the literal
    // text "Null".
    if let Ok(v) = serde_json::from_value::<deja::value::RedisWireValue>(value.clone()) {
        return v.to_redis_string();
    }
    // Ambient/config template: a bare string that is NOT a `RedisWireValue`
    // (e.g. `"0.20"`) is already the raw text redis holds. Preserved
    // byte-for-byte, exactly as before.
    if let serde_json::Value::String(s) = value {
        return Some(s.clone());
    }
    // A non-scalar RESP3 shape (Array/Map/Set/…) the string `SET` seeder cannot
    // represent: an explicit skip, never a silent stringify of the wrapper.
    None
}

/// Read a recording's boundary events JSONL, tolerating non-event records from
/// the shared `DejaRecord` stream exactly like the lookup renderer does.
/// Returns an empty vec on any I/O failure (best-effort seeding).
fn read_recording_events(path: &std::path::Path) -> Vec<deja::BoundaryEvent> {
    let Ok(file) = std::fs::File::open(path) else {
        eprintln!(
            "lifecycle: seed plan: recording {} not readable; skipping seeding",
            path.display()
        );
        return Vec::new();
    };
    let reader = std::io::BufReader::new(file);
    let mut events = Vec::new();
    for line in reader.lines() {
        let Ok(line) = line else { continue };
        if line.trim().is_empty() {
            continue;
        }
        // The current wire format is a single `DejaRecord` stream. Boundary
        // events feed seed planning; graph nodes and replay observations do not.
        if let Ok(deja::DejaRecord::BoundaryEvent(ev)) =
            serde_json::from_str::<deja::DejaRecord>(&line)
        {
            events.push(*ev);
        }
    }
    events
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
        "driving payment workload (HS → Kafka → Vector → MinIO)",
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
    correlation_filter: Option<&[String]>,
) -> Result<(), String> {
    let recording_path = root.recording_events_path(recording_id);
    let diff_sink = root.http_diff_path(run_id);
    let mut cmd = Command::new(kernel_bin);
    cmd.env("KERNEL_RECORDING_PATH", &recording_path)
        .env("KERNEL_TARGET_HOST", "127.0.0.1")
        .env("KERNEL_TARGET_PORT", target_port.to_string())
        .env("KERNEL_HTTP_DIFF_SINK", &diff_sink);
    // Test-case subset: the kernel drives only these correlations; scoring
    // scopes to the same list (load_artifacts reads it off the run spec).
    if let Some(filter) = correlation_filter.filter(|f| !f.is_empty()) {
        cmd.env("KERNEL_CORRELATION_FILTER", filter.join(","));
    }
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
    let dest = root.recording_events_path(recording_id);
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
    let report_path = dest.with_file_name("ingest-report.json");
    if let Err(e) = write_json(&report_path, &report) {
        eprintln!("lifecycle: ingest report write failed: {e}");
    }
    ctx.artifact(Some(recording_id), "ingest_report", &report_path);
    let manifest_path = dest.with_file_name("manifest.json");
    if let Err(e) = write_json(&manifest_path, &manifest) {
        eprintln!("lifecycle: manifest copy write failed: {e}");
    }
    ctx.artifact(Some(recording_id), "manifest", &manifest_path);
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
/// exactly like the session-layout pull — minus the manifest, which a raw
/// prefix doesn't have. Returns the resolved recording (session) id.
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
        if root.recording_events_path(id).exists() {
            eprintln!("lifecycle: recording {id} already ingested; reusing");
            return Ok(id.to_owned());
        }
    }
    let (cfg, prefix) = source.to_config()?;
    let (report, resolved, seen) =
        crate::s3::pull_recording_from_prefix(&cfg, &prefix, wanted, |sid| {
            root.recording_events_path(sid)
        })?;
    if seen.len() > 1 {
        let others = seen
            .iter()
            .filter(|(sid, _)| sid != &resolved)
            .map(|(sid, n)| format!("{sid} ({n})"))
            .collect::<Vec<_>>()
            .join(", ");
        ctx.log("ingest", &format!("other sessions under this prefix: {others}"));
    }
    let line = format!(
        "ingested {resolved} from {}: {} object(s), {} line(s), {} duplicate(s) dropped → \
         {} event(s), {} correlation(s) (unsealed prefix scan)",
        report.prefix,
        report.landing_objects,
        report.lines_in,
        report.duplicates_dropped,
        report.events_out,
        report.correlations,
    );
    eprintln!("lifecycle: {line}");
    ctx.log("ingest", &line);
    if report.events_out == 0 {
        return Err(format!(
            "session {resolved} pulled empty from {}",
            report.prefix
        ));
    }
    let dest = root.recording_events_path(&resolved);
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
    ctx.artifact(Some(&resolved), "ingest_report", &report_path);
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
    use crate::{CandidateSpec, RunSpec};

    fn extract_ctx_artifact_kinds(source: &str) -> std::collections::BTreeSet<String> {
        let mut kinds = std::collections::BTreeSet::new();
        // Both StoreCtx registration forms — the local path form and the
        // sink-published uri form — pass the kind as the 2nd arg. A call whose
        // kind is a VARIABLE (the REPLAY_STREAM_ARTIFACTS loop) is skipped here;
        // those kinds are added from the const below. (Markers are built with
        // concat! so this scanner never matches its own source text.)
        for marker in [concat!("ctx", ".artifact("), concat!("ctx", ".artifact_uri(")] {
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
            origin: deja::SeedOrigin::Recording,
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
                pk_column: "user_id".to_owned(),
                pk_value: "user_123".to_owned(),
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

        assert_eq!(
            certificate.summary,
            SeedCertificateSummary {
                planned: 6,
                materialized: 3,
                skipped: 1,
                failed: 1,
                unsupported: 1,
                readback_matched: 1,
                readback_missing: 1,
                readback_mismatched: 1,
                readback_errors: 1,
                readback_not_run: 2,
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
            pk_column: "user_id".to_owned(),
            pk_value: user_id.to_owned(),
        }
        .to_wire();
        let query_result_image = deja::db::DbRowImage::new(
            "users",
            vec![
                deja::db::DbColumnImage {
                    name: "user_id".into(),
                    type_oid: Some(25),
                    type_name: Some("text".into()),
                    nullable: Some(false),
                    value: serde_json::json!(user_id),
                },
                deja::db::DbColumnImage {
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
                    name: "user_id".into(),
                    type_oid: Some(25),
                    type_name: Some("text".into()),
                    nullable: Some(false),
                    value: serde_json::json!(user_id),
                },
                deja::db::DbColumnImage {
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
                    name: "user_id".into(),
                    type_oid: Some(25),
                    type_name: Some("text".into()),
                    nullable: Some(false),
                    value: serde_json::json!(user_id),
                },
                deja::db::DbColumnImage {
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
            pk_column: "user_id".to_owned(),
            pk_value: "user_123".to_owned(),
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
    fn read_recording_events_tolerates_non_event_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let body = format!(
            "{}\n# a header / non-event line\n\n{}\n{}\n",
            settlement_read_event_jsonl("c1", "settlement_rate_default", "0.10"),
            "{not json at all}",
            // graph nodes ride the same tape; they are not seed-plan inputs
            r#"{"record_kind":"graph_node","node_id":1,"global_sequence":1,"sequence":1,"span_name":"root","target":"router","level":"INFO","fields":{},"started_ns":1}"#,
        );
        std::fs::write(&path, body).unwrap();
        let events = read_recording_events(&path);
        assert_eq!(events.len(), 1, "only the one valid event parses");
        assert_eq!(events[0].read_set, vec!["settlement_rate_default"]);
    }

    #[test]
    fn record_graph_extract_keeps_only_graph_nodes_and_drops_payloads() {
        let dir = tempfile::tempdir().unwrap();
        let recording = dir.path().join("events.jsonl");
        // A tape interleaving graph-node STRUCTURE with a boundary event that
        // carries a payment payload — the exact thing that must NOT leave the pod.
        let secret_value = "SECRET_SETTLEMENT_PAYLOAD";
        let body = format!(
            "{}\n{}\n{}\n",
            r#"{"record_kind":"graph_node","node_id":1,"global_sequence":1,"sequence":1,"span_name":"root","target":"router","level":"INFO","fields":{"request_id":"req-1"},"started_ns":1}"#,
            settlement_read_event_jsonl("c1", "settlement_rate_default", secret_value),
            r#"{"record_kind":"graph_node","node_id":2,"global_sequence":3,"sequence":2,"parent_id":1,"span_name":"charge","target":"router","level":"INFO","fields":{},"started_ns":2}"#,
        );
        std::fs::write(&recording, body).unwrap();

        let dest = dir.path().join("record-graph.jsonl");
        let n = write_record_graph_nodes(&recording, &dest).unwrap();
        assert_eq!(n, 2, "both graph nodes extracted, the boundary event dropped");

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

    #[test]
    fn record_graph_extract_absent_recording_is_a_noop() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("record-graph.jsonl");
        // No recording on disk (compose without ingest) → 0 nodes, no file written.
        let n = write_record_graph_nodes(&dir.path().join("missing.jsonl"), &dest).unwrap();
        assert_eq!(n, 0);
        assert!(!dest.exists(), "nothing to publish → no empty artifact left behind");
    }

    /// The full replay-side wiring: derive the default rate from the recording's
    /// read-set, supply the premium rate from the ambient template, and render
    /// both to the byte-identical redis values the old hand-coded seeds wrote.
    #[test]
    fn seed_plan_yields_settlement_rates_from_recording_and_template() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        std::fs::write(
            &path,
            settlement_read_event_jsonl("c1", "settlement_rate_default", "0.10"),
        )
        .unwrap();
        let events = read_recording_events(&path);

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
        assert_eq!(render_redis_seed_value(&default.value).as_deref(), Some("0.10"));

        let premium = plan
            .resolve("redis", "settlement_rate_premium")
            .expect("premium seeded from ambient template");
        assert_eq!(premium.origin, deja::SeedOrigin::Ambient);
        assert_eq!(
            render_redis_seed_value(&premium.value).as_deref(),
            Some("0.20"),
            "premium rate renders byte-identically to the old `redis-cli SET ... 0.20`"
        );
    }

    #[test]
    fn ambient_template_defaults_to_demo_premium_rate() {
        // No DEJA_AMBIENT_TEMPLATE set in test → demo defaults.
        let template = load_ambient_template();
        assert!(!template.is_empty());
        let plan = deja::SeedPlan::new().with_ambient(&template);
        assert_eq!(
            render_redis_seed_value(
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
            render_redis_seed_value(&bulk).as_deref(),
            Some("019f41b1-220e-7282-80e3-5876da8cde9c"),
            "BulkString must decode to the raw UUID redis holds, not the wrapper"
        );

        // fred backend uses different variant names for the same shapes; the
        // shared type's serde aliases fold both dialects into one decode.
        assert_eq!(
            render_redis_seed_value(&serde_json::json!({ "Bytes": [104, 105] })).as_deref(),
            Some("hi"),
        );
        assert_eq!(
            render_redis_seed_value(&serde_json::json!({ "String": "merchant_xyz" })).as_deref(),
            Some("merchant_xyz"),
        );
        // redis_rs scalar variants.
        assert_eq!(
            render_redis_seed_value(&serde_json::json!({ "SimpleString": "OK" })).as_deref(),
            Some("OK"),
        );
        assert_eq!(
            render_redis_seed_value(&serde_json::json!({ "Int": 42 })).as_deref(),
            Some("42"),
        );

        // Backwards-compat: an ambient/template bare string is preserved
        // byte-for-byte (this is the path the demo relied on).
        assert_eq!(
            render_redis_seed_value(&serde_json::Value::String("0.20".to_owned())).as_deref(),
            Some("0.20"),
        );

        // A miss and a non-scalar RESP3 shape both decline to seed (loud skip at
        // the call site) rather than writing garbage.
        assert_eq!(render_redis_seed_value(&serde_json::json!("Null")), None);
        assert_eq!(
            render_redis_seed_value(&serde_json::json!({ "Array": [{ "Int": 1 }] })),
            None,
            "a non-scalar value must be skipped, never stringified into redis"
        );
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
            pk_column: "user_id".to_owned(),
            pk_value: "user_123".to_owned(),
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
            pk_column: "payment_id".to_owned(),
            pk_value: payment_id.to_owned(),
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
            origin: deja::SeedOrigin::Recording,
        });
        plan.upsert(deja::SeedEntry {
            boundary: "db".to_owned(),
            key: row_key.clone(),
            value: row_precondition,
            image: None,
            origin: deja::SeedOrigin::Recording,
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
            pk_column: "user_id".to_owned(),
            pk_value: user_id.to_owned(),
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
                    name: "merchant_id".into(),
                    type_oid: Some(25),
                    type_name: Some("text".into()),
                    nullable: Some(false),
                    value: serde_json::json!("merch_typed"),
                },
                deja::db::DbColumnImage {
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
                    name: "merchant_id".into(),
                    type_oid: None,
                    type_name: None,
                    nullable: None,
                    value: serde_json::json!("merch_typed"),
                },
                deja::db::DbColumnImage {
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
    fn redis_seed_image_keeps_physical_key_raw_value_and_ttl_advisory() {
        let image = RedisSeedImage::string("corr:settlement_rate_default", "0.10");

        assert_eq!(image.physical_key, "corr:settlement_rate_default");
        assert_eq!(image.physical_key_bytes, b"corr:settlement_rate_default");
        assert_eq!(image.value_type, RedisSeedValueType::String);
        assert_eq!(image.raw_value, "0.10");
        assert_eq!(image.raw_value_bytes, b"0.10");
        assert_eq!(image.ttl_seconds, None);
    }
}
