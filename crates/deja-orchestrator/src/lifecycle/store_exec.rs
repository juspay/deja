//! Store CLI transport seam (S1 of the k8s-executor design,
//! docs/design/replay-orchestrator-k8s-executor.md).
//!
//! Every replay-side store interaction is a `redis-cli`/`psql` invocation; the
//! seeding/readback/schema logic depends only on the CLI protocol (args in,
//! stdout/status out), never on WHERE the store lives. This type owns the
//! "where": the local-dev compose stack reaches stores through
//! `docker compose exec`, the in-pod (k8s Job) runner reaches its sidecar
//! stores through the CLIs directly. One value is built at the top of a
//! record/replay drive and threaded to every store call, so those call sites
//! are identical across executors.

use std::process::Command;

/// Which store a command targets — used only for log/describe purposes.
#[derive(Clone)]
pub(crate) enum StoreExec {
    /// Local-dev compose: `docker compose -p … -f … -f … exec -T <service> <cli> …`.
    /// `base_args`/`env` are the rendered compose prefix + `${VAR}` interpolation
    /// env (computed once from the demo config; keeps this module decoupled from
    /// the `Demo` type).
    Compose {
        base_args: Vec<String>,
        env: Vec<(String, String)>,
    },
    /// Direct CLIs against sidecar stores (the in-pod k8s Job runner). `psql`
    /// receives `database_url` via `-d` (a conninfo URL carries user/password/db).
    Direct {
        redis_host: String,
        redis_port: u16,
        database_url: String,
    },
}

impl StoreExec {
    pub(crate) fn compose(base_args: Vec<String>, env: Vec<(String, String)>) -> Self {
        Self::Compose { base_args, env }
    }

    #[allow(dead_code)] // wired up by the in-pod runner (design S1/#21)
    pub(crate) fn direct(redis_host: String, redis_port: u16, database_url: String) -> Self {
        Self::Direct {
            redis_host,
            redis_port,
            database_url,
        }
    }

    /// Prepared `redis-cli <args…>` against the replay redis. Args exclude the
    /// binary name.
    pub(crate) fn redis_cli(&self, args: &[&str]) -> Command {
        match self {
            Self::Compose { base_args, env } => {
                let mut cmd = Command::new("docker");
                cmd.args(base_args)
                    .args(["exec", "-T", "redis-standalone", "redis-cli"])
                    .args(args)
                    .envs(env.iter().cloned());
                cmd
            }
            Self::Direct {
                redis_host,
                redis_port,
                ..
            } => {
                let mut cmd = Command::new("redis-cli");
                cmd.args(["-h", redis_host, "-p", &redis_port.to_string()])
                    .args(args);
                cmd
            }
        }
    }

    /// Prepared `psql <shape_flags…> -v ON_ERROR_STOP=<0|1> … -c <sql>` against
    /// the replay pg. `shape_flags` are output-shape flags only (`-A -t -F \t`);
    /// connection identity (user/db/password) belongs to the transport.
    pub(crate) fn psql(&self, shape_flags: &[&str], on_error_stop: bool, sql: &str) -> Command {
        let stop = if on_error_stop {
            "ON_ERROR_STOP=1"
        } else {
            "ON_ERROR_STOP=0"
        };
        match self {
            Self::Compose { base_args, env } => {
                let mut cmd = Command::new("docker");
                cmd.args(base_args)
                    .args(["exec", "-T", "pg", "psql"])
                    .args(shape_flags)
                    .args(["-v", stop, "-U", "db_user", "-d", "hyperswitch_db", "-c", sql])
                    .envs(env.iter().cloned())
                    .env("PGPASSWORD", "db_pass");
                cmd
            }
            Self::Direct { database_url, .. } => {
                let mut cmd = Command::new("psql");
                cmd.args(shape_flags)
                    .args(["-v", stop, "-d", database_url, "-c", sql]);
                cmd
            }
        }
    }
}

/// One-line `program arg1 arg2 …` rendering for worker logs (what the compose
/// path used to print as `docker {args}`).
pub(crate) fn describe(cmd: &Command) -> String {
    let mut out = cmd.get_program().to_string_lossy().into_owned();
    for arg in cmd.get_args() {
        out.push(' ');
        out.push_str(&arg.to_string_lossy());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(cmd: &Command) -> Vec<String> {
        std::iter::once(cmd.get_program())
            .chain(cmd.get_args())
            .map(|s| s.to_string_lossy().into_owned())
            .collect()
    }

    #[test]
    fn compose_redis_cli_matches_legacy_shape() {
        let exec = StoreExec::compose(
            vec!["compose".into(), "-p".into(), "proj".into()],
            vec![("RUN_ID".into(), "r1".into())],
        );
        let cmd = exec.redis_cli(&["--raw", "GET", "k"]);
        assert_eq!(
            argv(&cmd),
            [
                "docker", "compose", "-p", "proj", "exec", "-T", "redis-standalone",
                "redis-cli", "--raw", "GET", "k"
            ]
        );
    }

    #[test]
    fn compose_psql_matches_legacy_shape() {
        let exec = StoreExec::compose(vec!["compose".into()], vec![]);
        let cmd = exec.psql(&["-A", "-t", "-F", "\t"], false, "SELECT 1");
        assert_eq!(
            argv(&cmd),
            [
                "docker", "compose", "exec", "-T", "pg", "psql", "-A", "-t", "-F", "\t",
                "-v", "ON_ERROR_STOP=0", "-U", "db_user", "-d", "hyperswitch_db", "-c",
                "SELECT 1"
            ]
        );
        let has_pgpassword = cmd
            .get_envs()
            .any(|(k, v)| k == "PGPASSWORD" && v == Some("db_pass".as_ref()));
        assert!(has_pgpassword);
    }

    #[test]
    fn direct_variants_target_sidecars() {
        let exec = StoreExec::direct("127.0.0.1".into(), 6379, "postgres://u:p@localhost/db".into());
        assert_eq!(
            argv(&exec.redis_cli(&["EXISTS", "k"])),
            ["redis-cli", "-h", "127.0.0.1", "-p", "6379", "EXISTS", "k"]
        );
        assert_eq!(
            argv(&exec.psql(&[], true, "SELECT 1")),
            [
                "psql", "-v", "ON_ERROR_STOP=1", "-d", "postgres://u:p@localhost/db",
                "-c", "SELECT 1"
            ]
        );
    }
}
