//! Thin CLI over the compactor lib.
//!
//!   deja-compactor compact <session_id>   → compact + print the manifest
//!   deja-compactor manifest <session_id>  → print the manifest if sealed
//!
//! Connection via DEJA_S3_ENDPOINT / DEJA_S3_BUCKET / DEJA_S3_ACCESS_KEY /
//! DEJA_S3_SECRET_KEY (defaults match the demo MinIO).

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (cmd, session_id) = match (args.get(1), args.get(2)) {
        (Some(c), Some(s)) => (c.as_str(), s.as_str()),
        _ => {
            eprintln!("usage: deja-compactor <compact|manifest> <session_id>");
            std::process::exit(2);
        }
    };
    let cfg = deja_compactor::S3Config::from_env();
    let result = match cmd {
        "compact" => deja_compactor::compact_session(&cfg, session_id).map(Some),
        "manifest" => deja_compactor::read_manifest(&cfg, session_id),
        other => {
            eprintln!("unknown command: {other}");
            std::process::exit(2);
        }
    };
    match result {
        Ok(Some(manifest)) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&manifest).unwrap_or_default()
            );
        }
        Ok(None) => {
            eprintln!("session {session_id} is not sealed (no manifest)");
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("deja-compactor: {e}");
            std::process::exit(1);
        }
    }
}
