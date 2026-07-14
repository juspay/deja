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
