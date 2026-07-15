use std::path::PathBuf;
use std::process::ExitCode;

fn config_path(arg: Option<std::ffi::OsString>) -> PathBuf {
    arg.map(PathBuf::from)
        .or_else(|| std::env::var_os("DEJA_AGENT_CONFIG").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("agent.toml"))
}

fn print_summary(summary: &deja_replay_agent::AgentSummary) {
    match serde_json::to_string_pretty(summary) {
        Ok(json) => println!("{json}"),
        Err(err) => eprintln!("deja-replay-agent: summary serialization failed: {err}"),
    }
}

/// `report <http-diffs> <call-ledger> <out.html> [run-id] [recording-id]`:
/// rebuild the human-readable diff report offline from downloaded artifacts
/// (accepts dashboard JSON-array exports or agent JSONL).
fn run_offline_report(args: &mut impl Iterator<Item = std::ffi::OsString>) -> Result<(), String> {
    let mut path_arg = |name: &str| {
        args.next()
            .map(PathBuf::from)
            .ok_or_else(|| format!("report: missing {name} argument"))
    };
    let diffs = path_arg("http-diffs")?;
    let ledger = path_arg("call-ledger")?;
    let out = path_arg("output")?;
    let run_id = args
        .next()
        .and_then(|a| a.into_string().ok())
        .unwrap_or_else(|| "offline".to_owned());
    let recording_id = args
        .next()
        .and_then(|a| a.into_string().ok())
        .unwrap_or_else(|| "unknown".to_owned());
    deja_replay_agent::report::write_report(&run_id, &recording_id, &diffs, &ledger, &out)?;
    eprintln!("deja-replay-agent: wrote {}", out.display());
    Ok(())
}

fn main() -> ExitCode {
    let mut args = std::env::args_os().skip(1);
    let first = args.next();
    let result = match first.as_ref().and_then(|a| a.to_str()) {
        // `prepare <config>`: pull + render the lookup table, then exit
        // (init-container mode; the router boots only after this succeeds).
        Some("prepare") => {
            deja_replay_agent::prepare_from_config_path(&config_path(args.next())).map(|()| None)
        }
        // `drive <config>`: drive an already-prepared run (main container).
        Some("drive") => {
            deja_replay_agent::drive_from_config_path(&config_path(args.next())).map(Some)
        }
        // `report <http-diffs> <call-ledger> <out.html> [run-id] [recording-id]`:
        // rebuild the human-readable diff report offline from downloaded
        // artifacts (accepts dashboard JSON-array exports or agent JSONL).
        Some("report") => run_offline_report(&mut args)
            .map(|()| None)
            .map_err(deja_replay_agent::AgentError::from_message),
        // legacy: bare config path (or nothing) = prepare + drive in one process
        _ => deja_replay_agent::run_from_config_path(&config_path(first)).map(Some),
    };
    match result {
        Ok(summary) => {
            if let Some(summary) = summary {
                print_summary(&summary);
            }
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("deja-replay-agent: {err}");
            ExitCode::FAILURE
        }
    }
}
