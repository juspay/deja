//! Thin CLI over the compactor lib — and the entry point a scheduled sealing
//! job calls.
//!
//!   deja-compactor pass                      → seal every declared system, with a ledger
//!   deja-compactor systems                   → every system the document declares
//!   deja-compactor list      [system]        → what is landed, and what is sealed
//!   deja-compactor readiness <id> [system]   → is this session finished?
//!   deja-compactor seal      <id> [system]   → seal it, but ONLY if it is finished
//!   deja-compactor compact   <id> [system]   → seal it regardless (manual override)
//!   deja-compactor manifest  <id> [system]   → print the manifest if sealed
//!
//! `pass` is the one a CronJob runs: it walks every declared system, isolates
//! them from each other, and accounts for every recording it considered. `seal`
//! is the same decision for one named recording. `compact` is that work without
//! the readiness gate, kept for the operator who has already decided.
//!
//! Connection comes from the environment (`DEJA_S3_ENDPOINT`, `DEJA_S3_BUCKET`,
//! `DEJA_S3_ACCESS_KEY`, `DEJA_S3_SECRET_KEY`); `DEJA_RECORDING_ROOT` says where
//! recordings land. The optional `system` argument selects a non-default system's
//! BUCKET through the deployment's `DEJA_<SYSTEM>_S3_BUCKET` convention — the key
//! layout is the same for every system, only the bucket differs.

use deja_compactor::{S3Config, SealReadiness};

/// How long a session must go unwritten before silence is taken for an ending.
/// Must exceed the aggregator's flush interval, or the gap between two flushes
/// of a live workload reads as a finished recording.
const DEFAULT_QUIET_SECS: u64 = 900;

fn quiet_secs() -> u64 {
    std::env::var("DEJA_SEAL_QUIET_SECS")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(DEFAULT_QUIET_SECS)
}

/// The most DECOMPRESSED landing a single compaction may hold before it refuses
/// the recording instead of loading it.
///
/// Compaction holds the whole landing, then its lines, then its collated
/// events, so peak resident memory is a multiple of this — 512 MiB is chosen
/// against a 2 GiB container, not as a limit in its own right. A deployment
/// that changes the container's memory changes this with it.
///
/// It exists because the alternative to refusing is being killed. A landing
/// past the container's memory takes the whole cgroup, including the pass, and
/// no line survives to say which recording did it; refusing costs one named
/// drop and lets every recording behind it seal.
const DEFAULT_MAX_LANDING_BYTES: u64 = 512 * 1024 * 1024;

/// `DEJA_SEAL_MAX_LANDING_BYTES`, where `0` means no ceiling.
///
/// Unbounded is spelled explicitly rather than by unsetting the variable, so a
/// deployment that means "load whatever it takes" reads differently from one
/// that never configured a sealer.
fn max_landing_bytes() -> Option<u64> {
    match std::env::var("DEJA_SEAL_MAX_LANDING_BYTES") {
        Ok(raw) => match raw.trim().parse::<u64>() {
            Ok(0) => None,
            Ok(bytes) => Some(bytes),
            Err(_) => Some(DEFAULT_MAX_LANDING_BYTES),
        },
        Err(_) => Some(DEFAULT_MAX_LANDING_BYTES),
    }
}

/// Where `system`'s recordings are: its bucket and its key root, both from the
/// declared document.
///
/// Naming no system means the document's `default_system`, which is declared
/// like any other and has no implicit bucket. A system the document does not
/// declare fails HERE, naming the table to add, rather than silently reading
/// some other system's recordings — the sealer would then write a seal into one
/// system's `sessions/v1` from another system's landing, and nothing downstream
/// could tell.
///
/// Same precedence as the orchestrator's `scan_scope`: bucket required, root
/// defaulted. The two cannot share code — `deja-orchestrator` depends on this
/// crate — but they read the same document.
fn scope_for(system: Option<&str>) -> Result<(S3Config, String, String), String> {
    let declared = deja_compactor::settings::load()?;
    let system = match system {
        Some(s) if !s.trim().is_empty() => s.trim().to_owned(),
        _ => declared.default_system.clone().unwrap_or_default(),
    };
    if system.is_empty() {
        return Err(
            "no system named and the deja configuration declares no default_system; \
             pass one, or set default_system in the document"
                .to_owned(),
        );
    }
    let (cfg, root) = deja_compactor::pass::scope_for_system(&system)?;
    Ok((cfg, root, system))
}

fn print_json<T: serde::Serialize>(value: &T) {
    println!(
        "{}",
        serde_json::to_string_pretty(value).unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}"))
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let cmd = args.get(1).map(String::as_str).unwrap_or("");
    // `systems` takes nothing, `list` takes the system in the id slot, and
    // everything else takes an id first.
    let (session_id, system) = match cmd {
        "systems" | "pass" => ("", None),
        "list" => ("", args.get(2).map(String::as_str)),
        _ => (
            args.get(2).map(String::as_str).unwrap_or(""),
            args.get(3).map(String::as_str),
        ),
    };
    let needs_id = !matches!(cmd, "systems" | "list" | "pass");
    if cmd.is_empty() || (needs_id && session_id.is_empty()) {
        eprintln!(
            "usage: deja-compactor <pass|systems|list|readiness|seal|compact|manifest> \
             [session_id] [system]"
        );
        std::process::exit(2);
    }

    if let Err(e) = run(cmd, session_id, system) {
        eprintln!("deja-compactor: {e}");
        std::process::exit(1);
    }
}

fn run(cmd: &str, session_id: &str, system: Option<&str>) -> Result<(), String> {
    // Answered before any scope is resolved: the roster is a fact about the
    // document, not about one system, and asking for it must not fail because
    // some unrelated system has no bucket declared. It is also what a caller
    // runs FIRST, to find out which systems to resolve at all.
    if cmd == "systems" {
        for name in deja_compactor::declared_systems()? {
            println!("{name}");
        }
        return Ok(());
    }
    // Resolved per system INSIDE the pass, because a system whose bucket is
    // undeclared has to become a named drop rather than the reason every other
    // system goes unsealed.
    if cmd == "pass" {
        return run_sealing_pass();
    }
    let (cfg, root, system) = scope_for(system)?;
    match cmd {
        "list" => {
            let found = deja_compactor::list_landed_recordings(&cfg, &root)?;
            let rows: Vec<serde_json::Value> = found
                .iter()
                .map(|r| {
                    // Sealed state is one small GET per recording. It is the
                    // whole point of the listing for a sealing job: the ones
                    // already sealed are the ones it must not re-read.
                    let manifest = deja_compactor::read_manifest(&cfg, &r.session_id)
                        .ok()
                        .flatten();
                    serde_json::json!({
                        "recording_id": r.session_id,
                        "dates": r.dates,
                        "objects": r.objects,
                        "prefix": r.prefix,
                        "sealed": manifest.is_some(),
                        "correlations": manifest.as_ref().map(|m| m.counts.correlations),
                    })
                })
                .collect();
            print_json(&serde_json::json!({
                "system": system,
                "bucket": cfg.bucket,
                "root": root,
                "recordings": rows,
            }));
            Ok(())
        }
        "readiness" => {
            let readiness = deja_compactor::seal_readiness(&cfg, session_id, &root, quiet_secs())?;
            print_json(&readiness);
            Ok(())
        }
        "seal" => {
            let existing = deja_compactor::read_manifest(&cfg, session_id)?;
            let readiness = deja_compactor::seal_readiness(&cfg, session_id, &root, quiet_secs())?;
            match deja_compactor::seal_decision(existing.as_ref(), &readiness) {
                // Nothing new since the seal. A SUCCESS, not a no-op to report as
                // failure: a cron that treats its own steady state as an error
                // alerts every time it runs.
                deja_compactor::SealDecision::AlreadyCurrent { sealed_objects } => {
                    eprintln!(
                        "deja-compactor: {session_id} is sealed and current ({sealed_objects} \
                         landing object(s) covered); nothing to do"
                    );
                    if let Some(manifest) = existing {
                        print_json(&manifest);
                    }
                    return Ok(());
                }
                deja_compactor::SealDecision::NotReady => {
                    // Not an error. The session is fine; it is just not finished.
                    eprintln!("deja-compactor: {session_id} is not ready to seal");
                    print_json(&readiness);
                    return Ok(());
                }
                deja_compactor::SealDecision::Seal { resealing } => {
                    if resealing {
                        // Worth saying: the recording resumed after it was
                        // sealed, and the manifest about to be replaced was a
                        // statement about a shorter recording.
                        eprintln!(
                            "deja-compactor: {session_id} grew after it was sealed ({} object(s) \
                             covered, {} now) — re-sealing to supersede that manifest",
                            existing
                                .as_ref()
                                .map(|m| m.counts.landing_objects)
                                .unwrap_or(0),
                            readiness.objects()
                        );
                    }
                }
            }
            if let SealReadiness::Quiesced {
                instances_without_eof,
                quiet_for_secs,
                ..
            } = &readiness
            {
                if !instances_without_eof.is_empty() {
                    // Said out loud because it changes what the seal MEANS: the
                    // recording is whatever reached the bucket, and one producer
                    // never confirmed it had finished sending.
                    eprintln!(
                        "deja-compactor: sealing {session_id} after {quiet_for_secs}s quiet, but \
                         instance(s) {} never wrote an end-of-stream marker — the seal covers what \
                         landed, which may be short of what was recorded",
                        instances_without_eof.join(", ")
                    );
                }
            }
            let manifest = deja_compactor::compact_session(&cfg, session_id, &root)?;
            print_json(&manifest);
            Ok(())
        }
        "compact" => {
            let manifest = deja_compactor::compact_session(&cfg, session_id, &root)?;
            print_json(&manifest);
            Ok(())
        }
        "manifest" => match deja_compactor::read_manifest(&cfg, session_id)? {
            Some(manifest) => {
                print_json(&manifest);
                Ok(())
            }
            None => Err(format!("session {session_id} is not sealed (no manifest)")),
        },
        other => Err(format!("unknown command: {other}")),
    }
}

/// One scheduled sealing pass over every declared system.
///
/// The two output streams are the point. Every row goes to STDOUT as one JSON
/// object the moment it is decided, and Rust's stdout is line buffered, so the
/// row is flushed before the next recording is touched. That is what survives
/// the container being OOM-killed mid-pass: the ledger up to the death, naming
/// the recording that was being compacted when it happened. The end-of-pass
/// summary is a convenience for a pass that finishes; it is never the only
/// record. Human lines go to stderr beside it.
///
/// Everything on stdout is one JSON object per line, summary included, so the
/// whole stream parses as JSONL without special-casing the last line.
fn run_sealing_pass() -> Result<(), String> {
    let emit = |value: &serde_json::Value| println!("{value}");
    let mut ledger =
        deja_compactor::pass::run_pass(quiet_secs(), max_landing_bytes(), &mut |row| {
            match serde_json::to_value(row) {
                Ok(value) => emit(&value),
                // A row that will not serialise is still a row. Dropping it here
                // would be the silent loss this command exists to remove.
                Err(e) => emit(&serde_json::json!({
                    "outcome": "unreportable",
                    "recording_id": row.recording_id,
                    "system": row.system,
                    "error": e.to_string(),
                })),
            }
            eprintln!("sealer: {row}");
        });

    if let Some(error) = ledger.roster_error.take() {
        return Err(error);
    }
    for system in &ledger.systems {
        match &system.unreachable {
            // Said at pass level as well as in the ledger: a system nobody can
            // reach seals nothing, and its recordings are not in any row.
            Some(why) => eprintln!("sealer: system {} UNREACHABLE — {why}", system.system),
            None => eprintln!(
                "sealer: system {}: {} recording(s) considered",
                system.system,
                system.planned.len()
            ),
        }
    }
    let disagreements = ledger.disagreements();
    for disagreement in &disagreements {
        eprintln!("sealer: ACCOUNTING — {disagreement}");
    }

    let totals = ledger.totals();
    emit(&serde_json::json!({
        "outcome": "pass_summary",
        "totals": totals,
        "unreachable_systems": ledger
            .systems
            .iter()
            .filter(|s| s.unreachable.is_some())
            .map(|s| s.system.clone())
            .collect::<Vec<_>>(),
        "dropped": ledger
            .drops()
            .iter()
            .map(|r| format!("{}/{}", r.system, r.recording_id))
            .collect::<Vec<_>>(),
        "disagreements": disagreements,
    }));

    if ledger.clean() {
        eprintln!(
            "sealer: pass complete — {} sealed, {} already current, {} not ready",
            totals.sealed, totals.already_current, totals.not_ready
        );
        return Ok(());
    }
    Err(format!(
        "pass incomplete — {} recording(s) dropped, {} system(s) unreachable, {} accounting \
         disagreement(s); the rows above name every one",
        totals.dropped,
        totals.systems_unreachable,
        disagreements.len()
    ))
}
