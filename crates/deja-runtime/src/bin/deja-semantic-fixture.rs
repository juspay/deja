use std::collections::HashMap;
use std::env;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use deja_context::ContextSnapshot;
use deja_runtime::RecordingHook;

#[deja_derive::recordable]
#[async_trait::async_trait]
trait PaymentStore {
    async fn find_merchant_by_id(&self, merchant_id: &str) -> Result<String, String>;
    async fn find_payment_intent_by_id(&self, payment_id: &str) -> Result<String, String>;
    async fn insert_payment_attempt(
        &self,
        payment_id: String,
        amount: u64,
    ) -> Result<String, String>;
}

#[deja_derive::recordable]
#[async_trait::async_trait]
trait ConfigStore {
    type Error;

    async fn find_config_by_key(&self, key: &str) -> Result<String, Self::Error>;
}

#[derive(Clone)]
struct RealPaymentStore;

#[async_trait::async_trait]
impl PaymentStore for RealPaymentStore {
    async fn find_merchant_by_id(&self, merchant_id: &str) -> Result<String, String> {
        Ok(format!("merchant:{merchant_id}"))
    }

    async fn find_payment_intent_by_id(&self, payment_id: &str) -> Result<String, String> {
        Ok(format!("payment_intent:{payment_id}"))
    }

    async fn insert_payment_attempt(
        &self,
        payment_id: String,
        amount: u64,
    ) -> Result<String, String> {
        Ok(format!("attempt:{payment_id}:{amount}"))
    }
}

#[derive(Clone)]
struct RealConfigStore;

#[async_trait::async_trait]
impl ConfigStore for RealConfigStore {
    type Error = String;

    async fn find_config_by_key(&self, key: &str) -> Result<String, Self::Error> {
        Ok(format!("config:{key}=enabled"))
    }
}

struct DejaPaymentStore {
    inner: Box<dyn PaymentStore + Send + Sync>,
    hook: Arc<RecordingHook>,
}

delegate_payment_store!(DejaPaymentStore, inner, hook, "storage");

struct DejaConfigStore {
    inner: Box<dyn ConfigStore<Error = String> + Send + Sync>,
    hook: Arc<RecordingHook>,
}

delegate_config_store!(DejaConfigStore, inner, hook, "storage", {
    type Error = String;
});

#[derive(Clone)]
struct AppState {
    payment: Arc<dyn PaymentStore + Send + Sync>,
    config: Arc<dyn ConfigStore<Error = String> + Send + Sync>,
}

fn main() -> std::io::Result<()> {
    let mut mode = String::from("baseline");
    let mut port = 18080u16;
    let mut artifact_dir = env::var_os("DEJA_ARTIFACT_DIR").map(PathBuf::from);

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--mode" => mode = args.next().unwrap_or_else(|| mode.clone()),
            "--port" => {
                port = args
                    .next()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(port);
            }
            "--artifact-dir" => artifact_dir = args.next().map(PathBuf::from),
            "--help" | "-h" => {
                print_help();
                return Ok(());
            }
            _ => {}
        }
    }

    let state = if mode == "record" {
        let dir =
            artifact_dir.unwrap_or_else(|| PathBuf::from("/tmp/deja-semantic-harness/recording"));
        match RecordingHook::new(&dir) {
            Ok(hook) => {
                let hook = Arc::new(hook);
                AppState {
                    payment: Arc::new(DejaPaymentStore {
                        inner: Box::new(RealPaymentStore),
                        hook: hook.clone(),
                    }),
                    config: Arc::new(DejaConfigStore {
                        inner: Box::new(RealConfigStore),
                        hook,
                    }),
                }
            }
            Err(error) => {
                eprintln!(
                    "deja-semantic-fixture failed to initialize recorder at {}: {error}; continuing without recording",
                    dir.display()
                );
                baseline_state()
            }
        }
    } else {
        baseline_state()
    };

    let listener = TcpListener::bind(("127.0.0.1", port))?;
    eprintln!("deja-semantic-fixture mode={mode} listening on 127.0.0.1:{port}");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let state = state.clone();
                thread::spawn(move || {
                    let _ = handle_connection(stream, state);
                });
            }
            Err(error) => eprintln!("accept error: {error}"),
        }
    }

    Ok(())
}

fn baseline_state() -> AppState {
    AppState {
        payment: Arc::new(RealPaymentStore),
        config: Arc::new(RealConfigStore),
    }
}

fn print_help() {
    eprintln!(
        "Usage: deja-semantic-fixture [--mode baseline|record] [--port PORT] [--artifact-dir DIR]"
    );
}

fn handle_connection(mut stream: TcpStream, state: AppState) -> std::io::Result<()> {
    let mut buffer = [0_u8; 8192];
    let n = stream.read(&mut buffer)?;
    if n == 0 {
        return Ok(());
    }

    let request = String::from_utf8_lossy(&buffer[..n]);
    let mut lines = request.lines();
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let target = parts.next().unwrap_or("/");

    let mut headers = HashMap::new();
    for line in lines {
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }

    if method != "GET" {
        return write_response(&mut stream, 405, r#"{"error":"method_not_allowed"}"#);
    }

    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    match path {
        "/health" => write_response(&mut stream, 200, r#"{"status":"ok"}"#),
        "/payment-flow" => {
            let request_id = headers
                .get("x-request-id")
                .cloned()
                .unwrap_or_else(|| "semantic-fixture-uncorrelated".to_string());
            let query = parse_query(query);
            let payment_id = query
                .get("payment_id")
                .map(String::as_str)
                .unwrap_or("pay_demo");
            let amount = query
                .get("amount")
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(100);

            let result = deja_context::scope_sync(ContextSnapshot::new(request_id.clone()), || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .map_err(|error| error.to_string())?;
                runtime.block_on(run_payment_flow(&state, payment_id, amount))
            });

            match result {
                Ok(body) => write_response(
                    &mut stream,
                    200,
                    &format!(r#"{{"request_id":"{request_id}","result":"{body}"}}"#),
                ),
                Err(error) => write_response(
                    &mut stream,
                    500,
                    &format!(r#"{{"request_id":"{request_id}","error":"{error}"}}"#),
                ),
            }
        }
        _ => write_response(&mut stream, 404, r#"{"error":"not_found"}"#),
    }
}

async fn run_payment_flow(
    state: &AppState,
    payment_id: &str,
    amount: u64,
) -> Result<String, String> {
    let config = state.config.find_config_by_key("routing.default").await?;
    let merchant = state.payment.find_merchant_by_id("merchant_demo").await?;
    let intent = state.payment.find_payment_intent_by_id(payment_id).await?;
    let attempt = state
        .payment
        .insert_payment_attempt(payment_id.to_string(), amount)
        .await?;

    Ok(format!("{config}|{merchant}|{intent}|{attempt}"))
}

fn parse_query(query: &str) -> HashMap<String, String> {
    query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

fn write_response(stream: &mut TcpStream, status: u16, body: &str) -> std::io::Result<()> {
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "Internal Server Error",
    };
    write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    )
}
