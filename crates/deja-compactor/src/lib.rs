//! Compacts deja landing objects into the durable session layout (Phase 2.3).
//!
//! Vector lands `deja.artifact_record/v2` envelopes as small per-batch
//! objects under `landing/v1/session={id}/inst={instance_id}/`. The compactor
//! turns one session's landing into the canonical store form:
//!
//!   sessions/v1/{id}/data/part-NNNNN.ndjsonl.zst   ← full envelope lines,
//!                                                    deduped + sorted
//!   sessions/v1/{id}/index/correlations.ndjson.zst ← per-correlation summary
//!   sessions/v1/{id}/manifest.json                 ← written LAST = the seal
//!
//! The manifest records per-instance `global_sequence` coverage (ranges,
//! gaps, duplicates dropped), schema versions, code provenance, and counts —
//! everything the catalog row and replay prep need without re-reading data.
//! Its existence is the seal: readers treat a session without a manifest as
//! still landing.
//!
//! Dedup is per `(instance_id, global_sequence)` — gseq is a per-process
//! counter, so two producers may legitimately share gseq values. (Session
//! mode runs single-instance today; the key is already multi-producer-safe
//! for the Phase 3 window mode.)
//!
//! Sync API over a current-thread runtime: the orchestrator lifecycle calls
//! this in-process from a plain worker thread, and the bin wraps the same
//! entry points.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use futures::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};

type DynStore = Arc<dyn ObjectStore>;

/// Events per data part before rotating to the next object.
const PART_MAX_EVENTS: usize = 50_000;
const ZSTD_LEVEL: i32 = 3;

/// Connection settings; defaults match the demo overlay's MinIO
/// (host-published on 9100, minioadmin credentials, bucket created by
/// minio-setup).
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
}

impl S3Config {
    pub fn from_env() -> Self {
        let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_owned());
        Self {
            endpoint: env("DEJA_S3_ENDPOINT", "http://127.0.0.1:9100"),
            bucket: env("DEJA_S3_BUCKET", "deja-recordings"),
            access_key: env("DEJA_S3_ACCESS_KEY", "minioadmin"),
            secret_key: env("DEJA_S3_SECRET_KEY", "minioadmin"),
        }
    }

    pub fn build(&self) -> Result<DynStore, String> {
        let store = AmazonS3Builder::new()
            .with_endpoint(&self.endpoint)
            .with_bucket_name(&self.bucket)
            .with_access_key_id(&self.access_key)
            .with_secret_access_key(&self.secret_key)
            .with_region("us-east-1")
            .with_allow_http(true)
            .build()
            .map_err(|e| format!("s3 client: {e}"))?;
        Ok(Arc::new(store))
    }
}

/// Key layout for one session in the bucket.
pub mod layout {
    pub fn landing_prefix(session_id: &str) -> String {
        format!("landing/v1/session={session_id}")
    }
    pub fn session_root(session_id: &str) -> String {
        format!("sessions/v1/{session_id}")
    }
    pub fn part_key(session_id: &str, n: usize) -> String {
        format!("{}/data/part-{n:05}.ndjsonl.zst", session_root(session_id))
    }
    pub fn correlations_key(session_id: &str) -> String {
        format!("{}/index/correlations.ndjson.zst", session_root(session_id))
    }
    pub fn manifest_key(session_id: &str) -> String {
        format!("{}/manifest.json", session_root(session_id))
    }
}

// -- manifest shapes ---------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionManifest {
    pub manifest_version: u32,
    pub session_id: String,
    /// "sealed" — the manifest is written last, so its presence IS the seal.
    pub status: String,
    pub capture_mode: String,
    pub envelope_schema_versions: Vec<u32>,
    pub event_schema_versions: Vec<u32>,
    /// Distinct code identities seen across the session's envelopes.
    pub code: Vec<CodeRef>,
    pub instances: Vec<InstanceCoverage>,
    pub counts: Counts,
    pub data_parts: Vec<DataPart>,
    pub created_unix_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeRef {
    pub sha: Option<String>,
    pub deja_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceCoverage {
    pub instance_id: String,
    pub gseq_min: u64,
    pub gseq_max: u64,
    pub events: u64,
    /// Inclusive `[from, to]` ranges missing between gseq_min and gseq_max.
    pub gaps: Vec<[u64; 2]>,
    pub duplicates_dropped: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Counts {
    pub landing_objects: usize,
    pub lines_in: usize,
    pub events: usize,
    pub duplicates_dropped: usize,
    pub correlations: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataPart {
    pub key: String,
    pub events: usize,
    pub bytes: usize,
}

/// Per-correlation row of the `index/correlations` sidecar.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorrelationSummary {
    pub correlation_id: Option<String>,
    pub events: u64,
    pub gseq_min: u64,
    pub gseq_max: u64,
    /// Whether the correlation has an `http_incoming` event — i.e. replay
    /// can re-drive it from the recording alone.
    pub has_ingress: bool,
}

// -- envelope probing --------------------------------------------------------

#[derive(Deserialize)]
struct EnvelopeProbe<'a> {
    #[serde(default)]
    schema_version: Option<u32>,
    #[serde(default)]
    artifact_type: Option<String>,
    #[serde(default)]
    instance_id: Option<String>,
    #[serde(default)]
    capture: Option<CaptureProbe>,
    #[serde(default)]
    code: Option<CodeRef>,
    #[serde(borrow)]
    event: Option<&'a serde_json::value::RawValue>,
}

#[derive(Deserialize)]
struct CaptureProbe {
    #[serde(default)]
    mode: Option<String>,
}

#[derive(Deserialize)]
struct EventProbe {
    #[serde(default)]
    global_sequence: u64,
    #[serde(default)]
    correlation_id: Option<String>,
    #[serde(default)]
    boundary: String,
    #[serde(default)]
    event_schema_version: Option<u32>,
}

fn runtime() -> Result<tokio::runtime::Runtime, String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("compactor runtime: {e}"))
}

async fn list_keys(
    store: &DynStore,
    prefix: &str,
) -> Result<Vec<object_store::path::Path>, String> {
    let prefix_path = object_store::path::Path::from(prefix);
    let mut keys: Vec<_> = store
        .list(Some(&prefix_path))
        .map_ok(|meta| meta.location)
        .try_collect()
        .await
        .map_err(|e| format!("s3 list {prefix}: {e}"))?;
    keys.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
    Ok(keys)
}

async fn get_decoded(store: &DynStore, key: &object_store::path::Path) -> Result<Vec<u8>, String> {
    let bytes = store
        .get(key)
        .await
        .map_err(|e| format!("s3 get {key}: {e}"))?
        .bytes()
        .await
        .map_err(|e| format!("s3 read {key}: {e}"))?;
    if key.as_ref().ends_with(".zst") {
        zstd::stream::decode_all(std::io::Cursor::new(&bytes[..]))
            .map_err(|e| format!("zstd {key}: {e}"))
    } else {
        Ok(bytes.to_vec())
    }
}

async fn put(store: &DynStore, key: &str, bytes: Vec<u8>) -> Result<(), String> {
    store
        .put(&object_store::path::Path::from(key), bytes.into())
        .await
        .map(|_| ())
        .map_err(|e| format!("s3 put {key}: {e}"))
}

fn zstd_encode(bytes: &[u8]) -> Result<Vec<u8>, String> {
    zstd::stream::encode_all(std::io::Cursor::new(bytes), ZSTD_LEVEL)
        .map_err(|e| format!("zstd encode: {e}"))
}

/// One accepted envelope line plus the probed identity the compactor sorts,
/// dedups, and summarizes by.
struct Accepted {
    instance_id: String,
    gseq: u64,
    correlation_id: Option<String>,
    boundary: String,
    raw_line: String,
}

/// Count landing objects for a session (the lifecycle's quiesce poll).
pub fn count_landing_objects(cfg: &S3Config, session_id: &str) -> Result<usize, String> {
    let store = cfg.build()?;
    let rt = runtime()?;
    rt.block_on(async {
        Ok(list_keys(&store, &layout::landing_prefix(session_id))
            .await?
            .len())
    })
}

/// Read the manifest if the session is sealed.
pub fn read_manifest(cfg: &S3Config, session_id: &str) -> Result<Option<SessionManifest>, String> {
    let store = cfg.build()?;
    let rt = runtime()?;
    rt.block_on(async {
        match store
            .get(&object_store::path::Path::from(layout::manifest_key(
                session_id,
            )))
            .await
        {
            Ok(obj) => {
                let bytes = obj
                    .bytes()
                    .await
                    .map_err(|e| format!("manifest read: {e}"))?;
                serde_json::from_slice(&bytes)
                    .map(Some)
                    .map_err(|e| format!("manifest parse: {e}"))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(format!("manifest get: {e}")),
        }
    })
}

/// Stream every envelope line out of a sealed session's data parts, in the
/// compacted (deduped, sorted) order.
pub fn read_session_lines(
    cfg: &S3Config,
    manifest: &SessionManifest,
) -> Result<Vec<String>, String> {
    let store = cfg.build()?;
    let rt = runtime()?;
    rt.block_on(async {
        let mut lines = Vec::new();
        for part in &manifest.data_parts {
            let data =
                get_decoded(&store, &object_store::path::Path::from(part.key.as_str())).await?;
            for line in data.split(|&b| b == b'\n') {
                if line.iter().all(|b| b.is_ascii_whitespace()) {
                    continue;
                }
                lines.push(String::from_utf8_lossy(line).into_owned());
            }
        }
        Ok(lines)
    })
}

/// Compact one session: landing objects → data parts + correlations index,
/// manifest written last (the seal). Idempotent — recompacting overwrites the
/// same keys from the same landing data.
pub fn compact_session(cfg: &S3Config, session_id: &str) -> Result<SessionManifest, String> {
    let store = cfg.build()?;
    let rt = runtime()?;
    rt.block_on(compact_session_inner(&store, session_id))
}

async fn compact_session_inner(
    store: &DynStore,
    session_id: &str,
) -> Result<SessionManifest, String> {
    let landing = layout::landing_prefix(session_id);
    let keys = list_keys(store, &landing).await?;
    if keys.is_empty() {
        return Err(format!("no landing objects under {landing}"));
    }
    let mut chunks = Vec::with_capacity(keys.len());
    for key in &keys {
        chunks.push(get_decoded(store, key).await?);
    }

    let collated = collate(&chunks);

    // Data parts: full envelope lines, rotated.
    let mut data_parts = Vec::new();
    for (n, window) in collated.events.chunks(PART_MAX_EVENTS).enumerate() {
        let mut buf = Vec::new();
        for acc in window {
            buf.extend_from_slice(acc.raw_line.as_bytes());
            buf.push(b'\n');
        }
        let compressed = zstd_encode(&buf)?;
        let key = layout::part_key(session_id, n);
        let bytes = compressed.len();
        put(store, &key, compressed).await?;
        data_parts.push(DataPart {
            key,
            events: window.len(),
            bytes,
        });
    }

    // Correlations index.
    let mut corr_buf = Vec::new();
    for row in &collated.correlations {
        corr_buf.extend_from_slice(
            serde_json::to_string(row)
                .map_err(|e| format!("correlation row: {e}"))?
                .as_bytes(),
        );
        corr_buf.push(b'\n');
    }
    put(
        store,
        &layout::correlations_key(session_id),
        zstd_encode(&corr_buf)?,
    )
    .await?;

    // Manifest LAST — its presence is the seal.
    let manifest = SessionManifest {
        manifest_version: 1,
        session_id: session_id.to_owned(),
        status: "sealed".to_owned(),
        capture_mode: collated.capture_mode,
        envelope_schema_versions: collated.envelope_schema_versions,
        event_schema_versions: collated.event_schema_versions,
        code: collated.code,
        instances: collated.instances,
        counts: Counts {
            landing_objects: keys.len(),
            lines_in: collated.lines_in,
            events: collated.events.len(),
            duplicates_dropped: collated.duplicates_dropped,
            correlations: collated.correlations.len(),
        },
        data_parts,
        created_unix_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0),
    };
    let manifest_bytes =
        serde_json::to_vec_pretty(&manifest).map_err(|e| format!("manifest encode: {e}"))?;
    put(store, &layout::manifest_key(session_id), manifest_bytes).await?;
    Ok(manifest)
}

struct Collated {
    events: Vec<Accepted>,
    lines_in: usize,
    duplicates_dropped: usize,
    capture_mode: String,
    envelope_schema_versions: Vec<u32>,
    event_schema_versions: Vec<u32>,
    code: Vec<CodeRef>,
    instances: Vec<InstanceCoverage>,
    correlations: Vec<CorrelationSummary>,
}

/// Parse landing chunks into deduped, sorted envelope lines plus the
/// coverage/summary facts the manifest records.
fn collate(chunks: &[Vec<u8>]) -> Collated {
    let mut seen: BTreeSet<(String, u64)> = BTreeSet::new();
    let mut dupes_by_instance: BTreeMap<String, u64> = BTreeMap::new();
    let mut events: Vec<Accepted> = Vec::new();
    let mut lines_in = 0usize;
    let mut duplicates = 0usize;
    let mut envelope_versions: BTreeSet<u32> = BTreeSet::new();
    let mut event_versions: BTreeSet<u32> = BTreeSet::new();
    let mut codes: Vec<CodeRef> = Vec::new();
    let mut capture_mode = String::from("session");

    for chunk in chunks {
        for line in chunk.split(|&b| b == b'\n') {
            if line.iter().all(|b| b.is_ascii_whitespace()) {
                continue;
            }
            lines_in += 1;
            let line_str = String::from_utf8_lossy(line);
            let env: EnvelopeProbe = match serde_json::from_str(&line_str) {
                Ok(e) => e,
                Err(_) => {
                    eprintln!("compactor: dropping non-envelope line");
                    continue;
                }
            };
            if env.artifact_type.as_deref() == Some("deja_sink_marker") {
                continue; // loss accounting, not session data (P2.4)
            }
            let Some(event) = env.event else {
                eprintln!("compactor: dropping envelope without event payload");
                continue;
            };
            let probe: EventProbe = match serde_json::from_str(event.get()) {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("compactor: dropping unparseable event ({e})");
                    continue;
                }
            };
            let instance_id = env.instance_id.unwrap_or_else(|| "unknown".to_owned());
            if !seen.insert((instance_id.clone(), probe.global_sequence)) {
                duplicates += 1;
                *dupes_by_instance.entry(instance_id).or_default() += 1;
                continue;
            }
            if let Some(v) = env.schema_version {
                envelope_versions.insert(v);
            }
            if let Some(v) = probe.event_schema_version {
                event_versions.insert(v);
            }
            if let Some(code) = env.code {
                if !codes.contains(&code) {
                    codes.push(code);
                }
            }
            if let Some(mode) = env.capture.and_then(|c| c.mode) {
                capture_mode = mode;
            }
            events.push(Accepted {
                instance_id,
                gseq: probe.global_sequence,
                correlation_id: probe.correlation_id,
                boundary: probe.boundary,
                raw_line: line_str.into_owned(),
            });
        }
    }
    events.sort_by(|a, b| (&a.instance_id, a.gseq).cmp(&(&b.instance_id, b.gseq)));

    // Per-instance coverage (events are sorted, so gaps fall out of one scan).
    let mut instances: Vec<InstanceCoverage> = Vec::new();
    for acc in &events {
        match instances.last_mut() {
            Some(cov) if cov.instance_id == acc.instance_id => {
                if acc.gseq > cov.gseq_max + 1 {
                    cov.gaps.push([cov.gseq_max + 1, acc.gseq - 1]);
                }
                cov.gseq_max = acc.gseq;
                cov.events += 1;
            }
            _ => instances.push(InstanceCoverage {
                instance_id: acc.instance_id.clone(),
                gseq_min: acc.gseq,
                gseq_max: acc.gseq,
                events: 1,
                gaps: Vec::new(),
                duplicates_dropped: 0,
            }),
        }
    }
    for cov in &mut instances {
        cov.duplicates_dropped = dupes_by_instance
            .get(&cov.instance_id)
            .copied()
            .unwrap_or(0);
    }

    // Per-correlation summary.
    let mut corr: BTreeMap<Option<String>, CorrelationSummary> = BTreeMap::new();
    for acc in &events {
        let row = corr
            .entry(acc.correlation_id.clone())
            .or_insert_with(|| CorrelationSummary {
                correlation_id: acc.correlation_id.clone(),
                events: 0,
                gseq_min: acc.gseq,
                gseq_max: acc.gseq,
                has_ingress: false,
            });
        row.events += 1;
        row.gseq_min = row.gseq_min.min(acc.gseq);
        row.gseq_max = row.gseq_max.max(acc.gseq);
        row.has_ingress |= acc.boundary == "http_incoming";
    }

    Collated {
        events,
        lines_in,
        duplicates_dropped: duplicates,
        capture_mode,
        envelope_schema_versions: envelope_versions.into_iter().collect(),
        event_schema_versions: event_versions.into_iter().collect(),
        code: codes,
        instances,
        correlations: corr.into_values().collect(),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn envelope(inst: &str, gseq: u64, corr: Option<&str>, boundary: &str) -> String {
        let corr_json = match corr {
            Some(c) => format!(r#""{c}""#),
            None => "null".to_owned(),
        };
        format!(
            r#"{{"schema_version":2,"artifact_type":"deja_artifact_record","instance_id":"{inst}","capture":{{"mode":"session","session_id":"s1"}},"code":{{"sha":"abc","deja_version":"0.1.0"}},"event":{{"recording_run_id":"s1","global_sequence":{gseq},"correlation_id":{corr_json},"boundary":"{boundary}","event_schema_version":1}}}}"#
        )
    }

    #[test]
    fn collate_builds_coverage_and_correlations() {
        let chunk = [
            envelope("i1", 1, Some("c1"), "http_incoming"),
            envelope("i1", 2, Some("c1"), "db"),
            envelope("i1", 2, Some("c1"), "db"),    // duplicate
            envelope("i1", 5, Some("c2"), "redis"), // gap 3-4
            envelope("i1", 4, None, "id_generation"),
        ]
        .join("\n")
        .into_bytes();
        let c = collate(&[chunk]);
        assert_eq!(c.lines_in, 5);
        assert_eq!(c.duplicates_dropped, 1);
        assert_eq!(c.events.len(), 4);
        assert_eq!(c.instances.len(), 1);
        let cov = &c.instances[0];
        assert_eq!((cov.gseq_min, cov.gseq_max), (1, 5));
        assert_eq!(cov.gaps, vec![[3, 3]]);
        assert_eq!(cov.duplicates_dropped, 1);
        assert_eq!(c.envelope_schema_versions, vec![2]);
        assert_eq!(c.event_schema_versions, vec![1]);
        assert_eq!(c.code.len(), 1);
        // correlations: None, c1, c2
        assert_eq!(c.correlations.len(), 3);
        let c1 = c
            .correlations
            .iter()
            .find(|r| r.correlation_id.as_deref() == Some("c1"))
            .unwrap();
        assert!(c1.has_ingress);
        assert_eq!(c1.events, 2);
        let c2 = c
            .correlations
            .iter()
            .find(|r| r.correlation_id.as_deref() == Some("c2"))
            .unwrap();
        assert!(!c2.has_ingress);
    }

    #[test]
    fn collate_separates_instances() {
        let chunk = [
            envelope("i2", 1, Some("c1"), "db"),
            envelope("i1", 1, Some("c1"), "db"), // same gseq, different instance: NOT a dupe
        ]
        .join("\n")
        .into_bytes();
        let c = collate(&[chunk]);
        assert_eq!(c.duplicates_dropped, 0);
        assert_eq!(c.events.len(), 2);
        assert_eq!(c.instances.len(), 2);
        assert_eq!(c.events[0].instance_id, "i1"); // sorted by (instance, gseq)
    }
}
