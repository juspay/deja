//! Thin CLI over the compactor lib — and the entry point a scheduled sealing
//! job calls.
//!
//!   deja-compactor list      [system]        → what is landed, and what is sealed
//!   deja-compactor readiness <id> [system]   → is this session finished?
//!   deja-compactor seal      <id> [system]   → seal it, but ONLY if it is finished
//!   deja-compactor compact   <id> [system]   → seal it regardless (manual override)
//!   deja-compactor manifest  <id> [system]   → print the manifest if sealed
//!
//! `seal` is the one a CronJob runs. `compact` is the same work without the
//! readiness gate, kept for the operator who has already decided.
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
    let mut cfg = S3Config::from_env();
    cfg.bucket = deja_compactor::bucket_for_system(&system)?;
    let root = deja_compactor::recording_root_for(&system)?;
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
    // `list` takes the system in the id slot; everything else takes an id first.
    let (session_id, system) = match cmd {
        "list" => ("", args.get(2).map(String::as_str)),
        _ => (
            args.get(2).map(String::as_str).unwrap_or(""),
            args.get(3).map(String::as_str),
        ),
    };
    if cmd.is_empty() || (cmd != "list" && session_id.is_empty()) {
        eprintln!(
            "usage: deja-compactor <list|readiness|seal|compact|manifest> [session_id] [system]"
        );
        std::process::exit(2);
    }

    if let Err(e) = run(cmd, session_id, system) {
        eprintln!("deja-compactor: {e}");
        std::process::exit(1);
    }
}

fn run(cmd: &str, session_id: &str, system: Option<&str>) -> Result<(), String> {
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
