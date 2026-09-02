//! Compacts deja landing objects into the durable session layout (Phase 2.3).
//!
//! Vector lands `deja.artifact_record/v2` envelopes as small per-batch
//! objects under `landing/v1/session={id}/inst={instance_id}/`. The compactor
//! turns one session's landing into the canonical store form:
//!
//!   sessions/v1/{id}/data/{seal}/part-NNNNN.ndjsonl.zst
//!                                    ← full envelope lines, deduped + sorted
//!   sessions/v1/{id}/index/{seal}/correlations.ndjson.zst
//!                                    ← per-correlation summary
//!   sessions/v1/{id}/manifest.json   ← written LAST = the seal
//!
//! The manifest records per-instance `global_sequence` coverage (ranges,
//! gaps, duplicates dropped), schema versions, code provenance, and counts —
//! everything the catalog row and replay prep need without re-reading data.
//! Its existence is the seal: readers treat a session without a manifest as
//! still landing.
//!
//! Two things follow from "the manifest is the seal", and both are load-bearing
//! because a recording is sealed by whichever run happens to ingest it first —
//! concurrently, from several runs, with no lock between them:
//!
//! - Write order is data parts, then index, then manifest. A seal interrupted
//!   anywhere before the last PUT leaves no manifest at all, so the session is
//!   indistinguishable from one still landing and the caller rescans. There is
//!   no state in which a reader can be handed a manifest naming an object that
//!   is not yet durable.
//! - Part and index keys are content-addressed by [`seal_id`]. Sealers that read
//!   the same landing write byte-identical objects to identical keys; sealers
//!   that read DIFFERENT landings (one saw more of a still-flushing session)
//!   write disjoint key sets, so neither can truncate the other's parts. The
//!   manifest key is shared and its PUT is atomic, so the last writer wins — and
//!   the manifest that wins names only its own, complete, immutable parts.
//!
//! Dedup is per `(instance_id, record kind, global_sequence)` — gseq is a
//! per-process counter, so two producers may legitimately share gseq values,
//! and each record kind is numbered in a sequence space of its own, so a gseq
//! identifies a record only within its kind. (Session mode runs single-instance
//! today; the key is already multi-producer-safe for the Phase 3 window mode.)
//!
//! Sync API over a current-thread runtime: the orchestrator lifecycle calls
//! this in-process from a plain worker thread, and the bin wraps the same
//! entry points.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub mod settings;

/// One lock for every test that touches the declared configuration.
///
/// The declaration is process-global — `std::env` — and `cargo test` runs a
/// crate's tests on parallel threads sharing it. WRITERS hold this so they do
/// not clobber each other; READERS hold it too, because a reader that loads
/// while a writer has a different document in place gets a torn answer and
/// fails a test that had nothing to do with it.
///
/// This is a SECOND lock, not a copy of the orchestrator's, and that is correct
/// rather than duplication: `cargo test` builds one test binary per crate and
/// runs them as separate processes, so there is no environment shared between
/// them for a single lock to guard. A lock reached across the crate boundary
/// would suggest a coordination that does not exist.
#[cfg(test)]
pub(crate) mod test_env {
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Hold this as the FIRST statement of any test that reads or writes the
    /// declaration. Placed after a cleanup line, that cleanup runs unlocked and
    /// produces a flake that looks like it belongs to whichever test lost.
    pub(crate) fn env_guard() -> std::sync::MutexGuard<'static, ()> {
        // A panicking test must not wedge every later one.
        ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }
}

use futures::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::{ObjectStore, WriteMultipart};
use serde::{Deserialize, Serialize};

type DynStore = Arc<dyn ObjectStore>;

/// Events per data part before rotating to the next object.
const PART_MAX_EVENTS: usize = 50_000;
const ZSTD_LEVEL: i32 = 3;

/// Bodies up to this size go up as one PUT; anything larger is uploaded as a
/// multipart in [`UPLOAD_CHUNK_BYTES`] pieces.
///
/// A single-shot PUT sends the whole body in ONE request, which has to complete
/// within the HTTP client's per-request timeout (30s by default in
/// `object_store`). A data part is as large as the recording it holds — the
/// first part of a session with 22,936 landing objects is tens of megabytes —
/// so the object's size, not any transient condition, decided whether the
/// request could finish, and `s3 put sessions/v1/…/part-00000` failed every
/// time on the recordings that most needed sealing. Chunking makes each request
/// a fixed size, independent of how big the recording is.
///
/// 8 MiB clears S3's 5 MiB minimum for non-final multipart parts.
const SINGLE_PUT_MAX_BYTES: usize = 8 * 1024 * 1024;
const UPLOAD_CHUNK_BYTES: usize = 8 * 1024 * 1024;
/// Parts in flight at once. Bounded so a large upload's memory is a few chunks
/// rather than the whole body a second time.
const UPLOAD_CONCURRENCY: usize = 4;

/// Connection settings. An EMPTY `endpoint` targets real AWS S3: object_store
/// derives the virtual-hosted endpoint from `region` and uses the AWS
/// credential chain (IRSA/web-identity) when no static credentials are set — so
/// an in-cluster pod needs only region + bucket. A non-empty `endpoint` (the
/// demo overlay's MinIO, host-published on 9100 with minioadmin credentials)
/// overrides that for a self-hosted/S3-compatible store.
pub struct S3Config {
    /// Explicit S3 endpoint, or empty to derive the AWS endpoint from `region`.
    pub endpoint: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
    pub allow_http: bool,
}

impl S3Config {
    pub fn from_env() -> Self {
        let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_owned());
        Self {
            // Empty by default → real AWS (endpoint derived from region). The
            // demo overlay sets DEJA_S3_ENDPOINT explicitly for its MinIO.
            endpoint: env("DEJA_S3_ENDPOINT", ""),
            bucket: env("DEJA_S3_BUCKET", "deja-recordings"),
            access_key: env("DEJA_S3_ACCESS_KEY", "minioadmin"),
            secret_key: env("DEJA_S3_SECRET_KEY", "minioadmin"),
            region: env("DEJA_S3_REGION", "us-east-1"),
            // Default true for the demo MinIO (plaintext); a real S3 endpoint
            // sets DEJA_S3_ALLOW_HTTP=false to require TLS.
            allow_http: env("DEJA_S3_ALLOW_HTTP", "true") != "false",
        }
    }

    fn has_static_credentials(&self) -> bool {
        !self.access_key.trim().is_empty() && !self.secret_key.trim().is_empty()
    }

    pub fn build(&self) -> Result<DynStore, String> {
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&self.bucket)
            .with_region(&self.region)
            .with_allow_http(self.allow_http);

        // Empty endpoint → let object_store derive the AWS virtual-hosted
        // endpoint from the region (real S3). A non-empty endpoint overrides it
        // (MinIO / S3-compatible). Passing an empty endpoint would break the
        // derived URL, so only set it when present.
        if !self.endpoint.trim().is_empty() {
            builder = builder.with_endpoint(&self.endpoint);
        }

        if self.has_static_credentials() {
            builder = builder
                .with_access_key_id(&self.access_key)
                .with_secret_access_key(&self.secret_key);
        }

        let store = builder.build().map_err(|e| format!("s3 client: {e}"))?;
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

    /// The directory component that scopes one seal's objects, or nothing for
    /// an empty id. Manifests written before seals were content-addressed carry
    /// no id and name their parts at the unscoped keys; reading them is
    /// unaffected, since a reader takes part keys from the manifest, and the
    /// correlations index is the one key a reader has to derive.
    fn seal_scope(seal_id: &str) -> String {
        if seal_id.is_empty() {
            String::new()
        } else {
            format!("{seal_id}/")
        }
    }

    pub fn part_key(session_id: &str, seal_id: &str, n: usize) -> String {
        format!(
            "{}/data/{}part-{n:05}.ndjsonl.zst",
            session_root(session_id),
            seal_scope(seal_id)
        )
    }
    pub fn correlations_key(session_id: &str, seal_id: &str) -> String {
        format!(
            "{}/index/{}correlations.ndjson.zst",
            session_root(session_id),
            seal_scope(seal_id)
        )
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
    /// Content address of this seal's data (see [`seal_id`]), and the directory
    /// component its parts and index live under. Empty on manifests written
    /// before seals were addressed.
    #[serde(default)]
    pub seal_id: String,
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
    /// Boundary events. Graph nodes are counted separately — see below.
    pub events: usize,
    pub duplicates_dropped: usize,
    /// Correlations — test cases — in the recording. This is the count a caller
    /// gets from the seal instead of pulling the tape.
    ///
    /// The index sidecar carries one MORE row than this when the recording has
    /// uncorrelated events: ambient background traffic is shared across cases
    /// and gets a row of its own so its events are accounted somewhere, but it
    /// is not a correlation and counting it would make the same recording
    /// report a different number here than a scan of its landing does.
    pub correlations: usize,
    /// Execution-graph nodes carried in the data parts.
    ///
    /// They are recording data, but they are not boundary events: they are
    /// numbered in a sequence space of their own and carry no correlation, so
    /// they are excluded from `events`, from the per-instance coverage, and
    /// from the correlation index. They are counted here because a seal that
    /// dropped them would take the whole record side of the execution graph
    /// with it, and nothing would say so.
    #[serde(default)]
    pub graph_nodes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataPart {
    pub key: String,
    pub events: usize,
    pub bytes: usize,
}

/// Per-correlation row of the `index/correlations` sidecar. Rows are in TAPE
/// ORDER — each correlation's first appearance in the recording — so "the first
/// N" means the earliest N requests.
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

/// The wire `artifact_type` values the landing carries. An unset type is the
/// legacy boundary-event envelope.
const ARTIFACT_TYPE_MARKER: &str = "deja_sink_marker";
const ARTIFACT_TYPE_GRAPH_NODE: &str = "deja_graph_node";

/// Which record an envelope carries. It decides two things at once: where the
/// payload is nested, and which `global_sequence` space the record is numbered
/// in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RecordKind {
    BoundaryEvent,
    GraphNode,
}

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
    /// An execution-graph node is nested under `node`, not `event`, because it
    /// carries its own `recording_run_id` and `global_sequence` that flattening
    /// would collide with the envelope's. A probe that knows only `event`
    /// therefore reads every graph envelope as an envelope with no payload —
    /// which is how the compactor used to drop the entire record-side graph
    /// while reporting a clean seal.
    #[serde(borrow)]
    node: Option<&'a serde_json::value::RawValue>,
}

impl<'a> EnvelopeProbe<'a> {
    /// The record this envelope declares, and the raw payload under whichever
    /// key that record uses. `None` when the declared payload is absent.
    fn payload(&self) -> Option<(RecordKind, &'a serde_json::value::RawValue)> {
        match self.artifact_type.as_deref() {
            Some(ARTIFACT_TYPE_GRAPH_NODE) => self.node.map(|n| (RecordKind::GraphNode, n)),
            _ => self.event.map(|e| (RecordKind::BoundaryEvent, e)),
        }
    }

    fn is_marker(&self) -> bool {
        self.artifact_type.as_deref() == Some(ARTIFACT_TYPE_MARKER)
    }
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
    role: Option<String>,
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
    decode_object(key.as_ref(), &bytes)
}

/// Decode an object's compression by extension, with a magic-byte fallback:
/// `.zst` = the deja session layout; `.gz`/gzip-magic = deployed Vector
/// aggregators configured with `compression: gzip` (whose extension is a
/// Vector default we don't control).
fn decode_object(key: &str, bytes: &[u8]) -> Result<Vec<u8>, String> {
    if key.ends_with(".zst") {
        return zstd::stream::decode_all(std::io::Cursor::new(bytes))
            .map_err(|e| format!("zstd {key}: {e}"));
    }
    if key.ends_with(".gz") || bytes.starts_with(&[0x1f, 0x8b]) {
        let mut out = Vec::new();
        let mut decoder = flate2::read::MultiGzDecoder::new(bytes);
        std::io::Read::read_to_end(&mut decoder, &mut out)
            .map_err(|e| format!("gzip {key}: {e}"))?;
        return Ok(out);
    }
    Ok(bytes.to_vec())
}

/// Upload one object. Small bodies go up as a single PUT; large ones are
/// chunked into a multipart upload — see [`SINGLE_PUT_MAX_BYTES`] for why the
/// size of the body, on its own, used to decide whether the write succeeded.
///
/// Either way the object becomes visible in one step: S3 publishes a multipart
/// upload only on `complete`, so a reader never sees a half-written part.
async fn put(store: &DynStore, key: &str, bytes: Vec<u8>) -> Result<(), String> {
    let path = object_store::path::Path::from(key);
    if bytes.len() <= SINGLE_PUT_MAX_BYTES {
        return store
            .put(&path, bytes.into())
            .await
            .map(|_| ())
            .map_err(|e| format!("s3 put {key}: {e}"));
    }
    put_chunked(store, &path, &bytes, UPLOAD_CHUNK_BYTES)
        .await
        .map_err(|e| format!("s3 put {key} ({} bytes): {e}", bytes.len()))
}

/// Multipart upload of `bytes` in `chunk_size` pieces, at most
/// [`UPLOAD_CONCURRENCY`] in flight.
///
/// A failed upload is aborted rather than left behind: S3 keeps the parts of an
/// abandoned multipart upload (and bills for them) until a lifecycle rule
/// expires them.
async fn put_chunked(
    store: &DynStore,
    path: &object_store::path::Path,
    bytes: &[u8],
    chunk_size: usize,
) -> Result<(), String> {
    let upload = store
        .put_multipart(path)
        .await
        .map_err(|e| format!("begin multipart: {e}"))?;
    let mut writer = WriteMultipart::new_with_chunk_size(upload, chunk_size);
    for chunk in bytes.chunks(chunk_size) {
        if let Err(e) = writer.wait_for_capacity(UPLOAD_CONCURRENCY).await {
            let _ = writer.abort().await;
            return Err(format!("upload part: {e}"));
        }
        writer.write(chunk);
    }
    // `finish` flushes the tail, waits for every part, and aborts the upload
    // itself if completion fails.
    writer
        .finish()
        .await
        .map(|_| ())
        .map_err(|e| format!("complete multipart: {e}"))
}

fn zstd_encode(bytes: &[u8]) -> Result<Vec<u8>, String> {
    zstd::stream::encode_all(std::io::Cursor::new(bytes), ZSTD_LEVEL)
        .map_err(|e| format!("zstd encode: {e}"))
}

/// One accepted envelope line plus the probed identity the compactor sorts,
/// dedups, and summarizes by.
struct Accepted {
    instance_id: String,
    kind: RecordKind,
    gseq: u64,
    correlation_id: Option<String>,
    boundary: String,
    role: Option<String>,
    raw_line: String,
}

/// Content address of a seal: sha256 over exactly the bytes the data parts
/// hold, in the order they are written, truncated to 16 hex characters.
///
/// This is what makes concurrent sealing safe. Two sealers that read the same
/// landing produce the same id, so they write the same keys with identical
/// bytes and overwriting is a no-op in content. Two that read different
/// landings — one caught a session mid-flush — produce different ids and
/// therefore disjoint key sets, so a shorter seal cannot truncate a longer
/// one's parts underneath the manifest that names them.
///
/// The uncompressed lines are hashed rather than the zstd frames, so the
/// address does not move when the compressor's output does.
fn seal_id(events: &[Accepted]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    for acc in events {
        hasher.update(acc.raw_line.as_bytes());
        hasher.update(b"\n");
    }
    hasher
        .finalize()
        .iter()
        .take(8)
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// List object keys under an ARBITRARY prefix (sorted). This is the scan
/// primitive for ingesting non-session layouts — e.g. the deployed Vector
/// aggregator's date-partitioned `%Y/%m/%d/` objects — where the recording is
/// identified by envelope content (`capture.session_id`), not by key layout.
pub fn list_objects(cfg: &S3Config, prefix: &str) -> Result<Vec<String>, String> {
    let store = cfg.build()?;
    let rt = runtime()?;
    rt.block_on(async {
        Ok(list_keys(&store, prefix)
            .await?
            .into_iter()
            .map(|p| p.as_ref().to_owned())
            .collect())
    })
}

/// One recording as it exists in the landing area, discovered by listing.
///
/// Held so a caller can name a recording instead of constructing a path to it.
/// The partitions a session spans are kept because they are a property of when
/// the aggregator flushed, not of the recording: a session that runs across
/// midnight lands under two dates, and a prefix naming only one of them
/// silently ingests part of it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LandedRecording {
    pub session_id: String,
    /// Partition dates this session appears under, earliest first. Empty for a
    /// layout that carries no date partition.
    pub dates: Vec<String>,
    /// The prefix to ingest from — the shared parent when a session spans more
    /// than one partition, so the scan sees all of it.
    pub prefix: String,
    pub objects: usize,
    /// `inst=` discriminators seen under the session, sorted. For a session
    /// whose id names no provenance (the UCS `run-<nanos>` shape) this is the
    /// only identity the LISTING can offer — it is the recorder's instance id
    /// (in k8s, the pod name) — and it costs nothing: the segments are already
    /// in the keys the scan lists.
    #[serde(default)]
    pub instances: Vec<String>,
}

impl LandedRecording {
    /// The date a listing sorts by: the last partition the session appears
    /// under, so "latest" means most recently written.
    pub fn latest_date(&self) -> Option<&str> {
        self.dates.last().map(String::as_str)
    }
}

/// Enumerate the recordings present under `root` (default `landing/v1`),
/// newest first.
///
/// The deployed aggregator writes `landing/v1/dt=<date>/session=<id>/…` while
/// the session layout writes `landing/v1/session=<id>/…`; both are read here,
/// because which one a bucket uses is a property of the deployment and not
/// something a caller should have to know. Keys that match neither are ignored
/// rather than guessed at.
pub fn list_landed_recordings(cfg: &S3Config, root: &str) -> Result<Vec<LandedRecording>, String> {
    Ok(index_landed_keys(root, &list_objects(cfg, root)?))
}

/// The listing's parsing half, separated from its IO so the key shapes it must
/// understand can be stated as examples rather than discovered in production.
pub fn index_landed_keys(root: &str, keys: &[String]) -> Vec<LandedRecording> {
    let root = root.trim_end_matches('/');
    // (session, (dates, objects, instances)) — BTreeMaps/Sets so everything
    // comes out sorted without a pass.
    #[allow(clippy::type_complexity)]
    let mut found: BTreeMap<String, (BTreeSet<String>, usize, BTreeSet<String>)> = BTreeMap::new();
    for key in keys {
        let Some(rest) = key.strip_prefix(root).map(|r| r.trim_start_matches('/')) else {
            continue;
        };
        let mut date: Option<&str> = None;
        let mut session: Option<&str> = None;
        let mut instance: Option<&str> = None;
        for segment in rest.split('/') {
            if let Some(d) = segment.strip_prefix("dt=") {
                date = Some(d);
            } else if let Some(s) = segment.strip_prefix("session=") {
                session = Some(s);
            } else if session.is_some() {
                // One level below the session: the writer's `inst=` partition.
                // Anything else there (an object, a different layout) is not an
                // instance; stop either way — deeper segments are the object's.
                instance = segment.strip_prefix("inst=");
                break;
            }
        }
        let Some(session) = session else { continue };
        let entry = found.entry(session.to_owned()).or_default();
        if let Some(d) = date {
            entry.0.insert(d.to_owned());
        }
        entry.1 += 1;
        if let Some(inst) = instance {
            entry.2.insert(inst.to_owned());
        }
    }

    let mut out: Vec<LandedRecording> = found
        .into_iter()
        .map(|(session_id, (dates, objects, instances))| {
            let dates: Vec<String> = dates.into_iter().collect();
            // One partition: address it directly. Several (or none): address the
            // root, so a straddling session is ingested whole.
            let prefix = match dates.as_slice() {
                [only] => format!("{root}/dt={only}/session={session_id}"),
                [] => format!("{root}/session={session_id}"),
                _ => root.to_owned(),
            };
            LandedRecording {
                session_id,
                dates,
                prefix,
                objects,
                instances: instances.into_iter().collect(),
            }
        })
        .collect();
    // Newest first: by last partition, then by id so the order is total.
    out.sort_by(|a, b| {
        b.latest_date()
            .cmp(&a.latest_date())
            .then_with(|| b.session_id.cmp(&a.session_id))
    });
    out
}

/// Fetch one object and decode its compression (`.zst`, `.gz`, or plain).
/// Companion to [`list_objects`] for arbitrary-prefix ingest.
pub fn get_object_decoded(cfg: &S3Config, key: &str) -> Result<Vec<u8>, String> {
    let store = cfg.build()?;
    let rt = runtime()?;
    rt.block_on(get_decoded(&store, &object_store::path::Path::from(key)))
}

/// Upload raw bytes to `key` (no compression). Used to stage a candidate
/// CodeBundle (a migrations tar) that a Job initContainer later pulls.
pub fn put_object(cfg: &S3Config, key: &str, bytes: Vec<u8>) -> Result<(), String> {
    let store = cfg.build()?;
    let rt = runtime()?;
    rt.block_on(put(&store, key, bytes))
}

/// Whether an object exists at `key` (HEAD-equivalent). Lets a producer skip
/// re-staging a bundle already present for a sha.
pub fn object_exists(cfg: &S3Config, key: &str) -> Result<bool, String> {
    let store = cfg.build()?;
    let rt = runtime()?;
    rt.block_on(async {
        match store.head(&object_store::path::Path::from(key)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(format!("s3 head {key}: {e}")),
        }
    })
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
    rt.block_on(manifest_of(&store, session_id))
}

async fn manifest_of(
    store: &DynStore,
    session_id: &str,
) -> Result<Option<SessionManifest>, String> {
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
}

/// How many correlations a sealed recording holds, WITHOUT pulling the tape.
///
/// One small GET: the count is a fact the seal already recorded, and reading it
/// off the manifest costs the same whether the recording is 3 correlations or
/// 42,310. `None` means the recording is not sealed yet, which is a different
/// answer from zero and must not be reported as one.
pub fn correlation_count(cfg: &S3Config, session_id: &str) -> Result<Option<usize>, String> {
    Ok(read_manifest(cfg, session_id)?.map(|m| m.counts.correlations))
}

/// The per-correlation index of a sealed recording: one row per correlation
/// with its event count, `global_sequence` span, and whether replay can
/// re-drive it. One manifest GET plus one sidecar GET — the data parts are
/// never touched. `None` if the recording is not sealed.
pub fn read_correlation_index(
    cfg: &S3Config,
    session_id: &str,
) -> Result<Option<Vec<CorrelationSummary>>, String> {
    let store = cfg.build()?;
    let rt = runtime()?;
    rt.block_on(async {
        let Some(manifest) = manifest_of(&store, session_id).await? else {
            return Ok(None);
        };
        correlation_index_of(&store, &manifest).await.map(Some)
    })
}

async fn correlation_index_of(
    store: &DynStore,
    manifest: &SessionManifest,
) -> Result<Vec<CorrelationSummary>, String> {
    let key = layout::correlations_key(&manifest.session_id, &manifest.seal_id);
    let data = get_decoded(store, &object_store::path::Path::from(key.as_str())).await?;
    let mut rows = Vec::with_capacity(manifest.counts.correlations);
    for line in data.split(|&b| b == b'\n') {
        if line.iter().all(|b| b.is_ascii_whitespace()) {
            continue;
        }
        rows.push(
            serde_json::from_slice(line).map_err(|e| format!("correlation row in {key}: {e}"))?,
        );
    }
    Ok(rows)
}

/// Stream every envelope line out of a sealed session's data parts, in the
/// compacted (deduped, sorted) order.
pub fn read_session_lines(
    cfg: &S3Config,
    manifest: &SessionManifest,
) -> Result<Vec<String>, String> {
    let store = cfg.build()?;
    let rt = runtime()?;
    rt.block_on(session_lines(&store, manifest))
}

async fn session_lines(
    store: &DynStore,
    manifest: &SessionManifest,
) -> Result<Vec<String>, String> {
    let mut lines = Vec::new();
    for part in &manifest.data_parts {
        let data = get_decoded(store, &object_store::path::Path::from(part.key.as_str())).await?;
        for line in data.split(|&b| b == b'\n') {
            if line.iter().all(|b| b.is_ascii_whitespace()) {
                continue;
            }
            lines.push(String::from_utf8_lossy(line).into_owned());
        }
    }
    Ok(lines)
}

/// Compact one session: landing objects → data parts + correlations index,
/// manifest written last (the seal). Idempotent — recompacting the same landing
/// data writes identical bytes to identical keys.
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

    let collated = collate(chunk_lines(&chunks));
    write_seal(store, session_id, collated, keys.len()).await
}

/// Seal a recording from envelope lines a caller has ALREADY read.
///
/// The ingest that scans a raw landing prefix ends up holding exactly what a
/// seal is made of, and until it wrote that back every replay run re-listed,
/// re-fetched and re-collated the same landing objects — 8 to 17 seconds per
/// run, forever, for a recording that cannot change. Sealing here is what makes
/// the next pull of the same recording take the manifest fast path.
///
/// `landing_objects` is the number of source objects these lines came from,
/// recorded in the manifest for the same accounting the landing-driven compact
/// reports.
pub fn seal_session(
    cfg: &S3Config,
    session_id: &str,
    envelope_lines: &[String],
    landing_objects: usize,
) -> Result<SessionManifest, String> {
    let store = cfg.build()?;
    let rt = runtime()?;
    rt.block_on(async {
        let collated = collate(envelope_lines.iter().map(|l| Cow::Borrowed(l.as_str())));
        write_seal(&store, session_id, collated, landing_objects).await
    })
}

/// Write the sealed form: data parts, then the correlations index, then the
/// manifest LAST.
///
/// The ordering is the whole safety property. Every object the manifest names
/// is durable before the manifest exists, so an interrupted seal leaves a
/// session that reads as unsealed and is rescanned, never a manifest promising
/// parts that are not there. See the module header for why the part and index
/// keys are content-addressed.
async fn write_seal(
    store: &DynStore,
    session_id: &str,
    collated: Collated,
    landing_objects: usize,
) -> Result<SessionManifest, String> {
    // A seal claims this is the whole recording, and it is permanent in the
    // sense that matters: every later run takes the manifest fast path and
    // stops looking at the landing. An empty one cannot be told apart from a
    // landing that was scanned too early or read through the wrong prefix, so
    // it would turn a recoverable mistake into a recording that is empty
    // forever. Leave the session unsealed and say why.
    if collated.events.is_empty() {
        return Err(format!(
            "refusing to seal {session_id} as an empty recording: {} line(s) yielded no records",
            collated.lines_in
        ));
    }
    let seal = seal_id(&collated.events);

    // Data parts: full envelope lines, rotated. One part is built, compressed
    // and uploaded at a time — the session's lines are already in memory, and
    // holding every part's buffers as well is what turns a large recording into
    // a memory problem.
    let mut data_parts = Vec::new();
    for (n, window) in collated.events.chunks(PART_MAX_EVENTS).enumerate() {
        let compressed = {
            let mut buf = Vec::new();
            for acc in window {
                buf.extend_from_slice(acc.raw_line.as_bytes());
                buf.push(b'\n');
            }
            zstd_encode(&buf)?
        };
        let key = layout::part_key(session_id, &seal, n);
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
        &layout::correlations_key(session_id, &seal),
        zstd_encode(&corr_buf)?,
    )
    .await?;

    // Manifest LAST — its presence is the seal.
    let manifest = SessionManifest {
        manifest_version: 1,
        session_id: session_id.to_owned(),
        status: "sealed".to_owned(),
        seal_id: seal,
        capture_mode: collated.capture_mode,
        envelope_schema_versions: collated.envelope_schema_versions,
        event_schema_versions: collated.event_schema_versions,
        code: collated.code,
        instances: collated.instances,
        counts: Counts {
            landing_objects,
            lines_in: collated.lines_in,
            events: collated.events.len() - collated.graph_nodes,
            duplicates_dropped: collated.duplicates_dropped,
            correlations: collated
                .correlations
                .iter()
                .filter(|row| row.correlation_id.is_some())
                .count(),
            graph_nodes: collated.graph_nodes,
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
    /// Every accepted record, boundary events and graph nodes together — the
    /// data parts carry the recording whole.
    events: Vec<Accepted>,
    /// How many of `events` are graph nodes.
    graph_nodes: usize,
    lines_in: usize,
    duplicates_dropped: usize,
    capture_mode: String,
    envelope_schema_versions: Vec<u32>,
    event_schema_versions: Vec<u32>,
    code: Vec<CodeRef>,
    instances: Vec<InstanceCoverage>,
    correlations: Vec<CorrelationSummary>,
}

/// Split landing objects into envelope lines. Blank lines are not lines.
fn chunk_lines(chunks: &[Vec<u8>]) -> impl Iterator<Item = Cow<'_, str>> {
    chunks
        .iter()
        .flat_map(|chunk| chunk.split(|&b| b == b'\n'))
        .map(String::from_utf8_lossy)
}

/// Parse envelope lines into deduped, sorted records plus the coverage/summary
/// facts the manifest records.
///
/// The record kind is dispatched from `artifact_type` BEFORE the payload is
/// looked for, because which key holds the payload depends on the kind (see
/// [`EnvelopeProbe`]), and it is part of the dedup key, because
/// `global_sequence` is only unique WITHIN a kind: graph nodes are numbered in
/// a sequence space of their own so that boundary-event numbering is identical
/// whether or not graph capture is on. Both spaces start at zero in one
/// recording, so a key without the kind makes graph node N collide with
/// boundary event N and drops one of them.
///
/// Coverage and the correlation index describe the BOUNDARY EVENT stream only.
/// Graph nodes have their own numbering and no correlation, so folding them in
/// would invent gaps and an uncorrelated bucket; they still ride in the data
/// parts, which is what the recording is.
fn collate<'a>(lines: impl Iterator<Item = Cow<'a, str>>) -> Collated {
    let mut seen: BTreeSet<(String, RecordKind, u64)> = BTreeSet::new();
    let mut dupes_by_instance: BTreeMap<String, u64> = BTreeMap::new();
    let mut events: Vec<Accepted> = Vec::new();
    let mut lines_in = 0usize;
    let mut duplicates = 0usize;
    let mut graph_nodes = 0usize;
    let mut envelope_versions: BTreeSet<u32> = BTreeSet::new();
    let mut event_versions: BTreeSet<u32> = BTreeSet::new();
    let mut codes: Vec<CodeRef> = Vec::new();
    let mut capture_mode = String::from("session");

    for line_str in lines {
        if line_str.trim().is_empty() {
            continue;
        }
        lines_in += 1;
        let env: EnvelopeProbe = match serde_json::from_str(&line_str) {
            Ok(e) => e,
            Err(_) => {
                eprintln!("compactor: dropping non-envelope line");
                continue;
            }
        };
        if env.is_marker() {
            continue; // loss accounting, not session data (P2.4)
        }
        let Some((kind, payload)) = env.payload() else {
            eprintln!("compactor: dropping envelope without a payload");
            continue;
        };
        let probe: EventProbe = match serde_json::from_str(payload.get()) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("compactor: dropping unparseable event ({e})");
                continue;
            }
        };
        let instance_id = env.instance_id.unwrap_or_else(|| "unknown".to_owned());
        if !seen.insert((instance_id.clone(), kind, probe.global_sequence)) {
            duplicates += 1;
            *dupes_by_instance.entry(instance_id).or_default() += 1;
            continue;
        }
        // The two version fields describe the boundary-event stream. A graph
        // envelope carries its own, unrelated `schema_version`, and folding it
        // in here would read as the recording spanning two envelope versions.
        if kind == RecordKind::BoundaryEvent {
            if let Some(v) = env.schema_version {
                envelope_versions.insert(v);
            }
            if let Some(v) = probe.event_schema_version {
                event_versions.insert(v);
            }
        } else {
            graph_nodes += 1;
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
            kind,
            gseq: probe.global_sequence,
            correlation_id: probe.correlation_id,
            boundary: probe.boundary,
            role: probe.role,
            raw_line: line_str.into_owned(),
        });
    }
    // Sequence before kind, so each kind keeps its own order and the kind only
    // breaks the tie between the two spaces.
    events.sort_by(|a, b| (&a.instance_id, a.gseq, a.kind).cmp(&(&b.instance_id, b.gseq, b.kind)));

    // Per-instance coverage (events are sorted, so gaps fall out of one scan).
    let mut instances: Vec<InstanceCoverage> = Vec::new();
    for acc in events
        .iter()
        .filter(|a| a.kind == RecordKind::BoundaryEvent)
    {
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

    // Per-correlation summary, in TAPE ORDER: rows come out in the order each
    // correlation first appears in the data parts, not sorted by id.
    //
    // The order is what a caller reads "the first N correlations" from, so it
    // has to mean something about the recording. First appearance is the
    // recording's own order — the earliest requests — and it stays that whatever
    // the ids look like. Today's ids are UUIDv7, which sort chronologically by
    // construction, so sorting by id would give the same answer; that is a
    // property of the current id scheme, not of the recording, and it is not
    // something a reader should have to know or a future id scheme has to keep.
    let mut order: Vec<Option<String>> = Vec::new();
    let mut corr: BTreeMap<Option<String>, CorrelationSummary> = BTreeMap::new();
    for acc in events
        .iter()
        .filter(|a| a.kind == RecordKind::BoundaryEvent)
    {
        let row = corr.entry(acc.correlation_id.clone()).or_insert_with(|| {
            order.push(acc.correlation_id.clone());
            CorrelationSummary {
                correlation_id: acc.correlation_id.clone(),
                events: 0,
                gseq_min: acc.gseq,
                gseq_max: acc.gseq,
                has_ingress: false,
            }
        });
        row.events += 1;
        row.gseq_min = row.gseq_min.min(acc.gseq);
        row.gseq_max = row.gseq_max.max(acc.gseq);
        row.has_ingress |=
            acc.role.as_deref() == Some("ingress") || acc.boundary == "http_incoming";
    }
    let correlations: Vec<CorrelationSummary> = order
        .into_iter()
        .filter_map(|id| corr.remove(&id))
        .collect();

    Collated {
        events,
        graph_nodes,
        lines_in,
        duplicates_dropped: duplicates,
        capture_mode,
        envelope_schema_versions: envelope_versions.into_iter().collect(),
        event_schema_versions: event_versions.into_iter().collect(),
        code: codes,
        instances,
        correlations,
    }
}

/// Where a session's landing objects actually are, whatever layout wrote them.
///
/// The compactor's own layout puts them under `landing/v1/session={id}`. The
/// deployed aggregator partitions by date first —
/// `landing/v1/dt=<date>/session={id}/` — so a session it wrote is not under
/// the prefix the compactor looks in, and a caller naming a recording by id
/// alone was told there were no landing objects for a recording sitting right
/// there. Both layouts are read, because which one a bucket uses belongs to the
/// deployment.
///
/// `None` means the session is not in the bucket at all, which is a different
/// answer from "not where I looked" and deserves a different message.
pub fn locate_landing_prefix(
    cfg: &S3Config,
    session_id: &str,
    root: &str,
) -> Result<Option<String>, String> {
    // The flat layout first: one cheap list, and it is exact when it hits.
    let flat = layout::landing_prefix(session_id);
    if !list_objects(cfg, &flat)?.is_empty() {
        return Ok(Some(flat));
    }
    Ok(list_landed_recordings(cfg, root)?
        .into_iter()
        .find(|found| found.session_id == session_id)
        .map(|found| found.prefix))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn keys(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| (*s).to_owned()).collect()
    }

    #[test]
    fn the_index_reads_both_layouts_a_bucket_may_use() {
        // The deployed aggregator partitions by date; the session layout does
        // not. Which one a bucket uses belongs to the deployment, so a caller
        // naming a recording must get the same answer either way.
        let found = index_landed_keys(
            "landing/v1",
            &keys(&[
                "landing/v1/dt=2026-07-29/session=run-a/inst=r1/0.log.gz",
                "landing/v1/dt=2026-07-29/session=run-a/inst=r1/1.log.gz",
                "landing/v1/session=run-b/inst=r1/0.log.gz",
            ]),
        );
        let a = found.iter().find(|r| r.session_id == "run-a").unwrap();
        assert_eq!(a.dates, vec!["2026-07-29"]);
        assert_eq!(a.objects, 2);
        assert_eq!(a.prefix, "landing/v1/dt=2026-07-29/session=run-a");

        let b = found.iter().find(|r| r.session_id == "run-b").unwrap();
        assert!(b.dates.is_empty());
        assert_eq!(b.prefix, "landing/v1/session=run-b");
    }

    #[test]
    fn a_session_spanning_midnight_is_addressed_whole() {
        // The partition is when the aggregator flushed, not when the recording
        // happened. A prefix naming one of the two dates would ingest half a
        // recording and report it as a whole one.
        let found = index_landed_keys(
            "landing/v1",
            &keys(&[
                "landing/v1/dt=2026-07-29/session=run-x/inst=r1/9.log.gz",
                "landing/v1/dt=2026-07-30/session=run-x/inst=r1/0.log.gz",
            ]),
        );
        assert_eq!(found.len(), 1);
        let x = &found[0];
        assert_eq!(x.dates, vec!["2026-07-29", "2026-07-30"]);
        assert_eq!(x.objects, 2);
        assert_eq!(
            x.prefix, "landing/v1",
            "a straddling session must be scanned from the shared parent"
        );
        assert_eq!(x.latest_date(), Some("2026-07-30"));
    }

    #[test]
    fn instance_discriminators_ride_the_listing() {
        // For a boot-derived session id the `inst=` segment is the only
        // identity the listing can offer (in k8s it is the pod name). Captured
        // from the keys the scan already lists — never from object content.
        let found = index_landed_keys(
            "landing/v1",
            &keys(&[
                "landing/v1/dt=2026-08-26/session=run-1/inst=sbx-custom-hyperswitch-ucs-a/0.gz",
                "landing/v1/dt=2026-08-26/session=run-1/inst=sbx-custom-hyperswitch-ucs-b/1.gz",
                "landing/v1/dt=2026-08-26/session=run-1/inst=sbx-custom-hyperswitch-ucs-a/2.gz",
                // An object directly under the session (no inst partition)
                // contributes no instance — and must not invent one.
                "landing/v1/dt=2026-08-26/session=run-2/manifest.json",
            ]),
        );
        assert_eq!(found.len(), 2);
        let one = found.iter().find(|r| r.session_id == "run-1").unwrap();
        assert_eq!(
            one.instances,
            vec![
                "sbx-custom-hyperswitch-ucs-a".to_owned(),
                "sbx-custom-hyperswitch-ucs-b".to_owned()
            ]
        );
        let two = found.iter().find(|r| r.session_id == "run-2").unwrap();
        assert!(two.instances.is_empty());
    }

    #[test]
    fn the_index_is_newest_first() {
        let found = index_landed_keys(
            "landing/v1",
            &keys(&[
                "landing/v1/dt=2026-07-28/session=run-old/inst=r1/0.log.gz",
                "landing/v1/dt=2026-08-04/session=run-new/inst=r1/0.log.gz",
                "landing/v1/dt=2026-07-29/session=run-mid/inst=r1/0.log.gz",
            ]),
        );
        let order: Vec<&str> = found.iter().map(|r| r.session_id.as_str()).collect();
        assert_eq!(order, vec!["run-new", "run-mid", "run-old"]);
    }

    #[test]
    fn keys_that_name_no_session_are_ignored_not_guessed_at() {
        let found = index_landed_keys(
            "landing/v1",
            &keys(&[
                "landing/v1/dt=2026-07-29/stray.log.gz",
                "landing/v1/_SUCCESS",
                "landing/v1/dt=2026-07-29/session=run-a/inst=r1/0.log.gz",
            ]),
        );
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].session_id, "run-a");
        assert_eq!(found[0].objects, 1);
    }

    #[test]
    fn the_flat_layout_is_preferred_and_the_index_is_the_fallback() {
        // Two layouts exist and a caller naming a recording by id knows neither.
        // The flat one is exact when it hits, so it is tried first; the index
        // reads both and is what finds a date-partitioned session.
        let flat = index_landed_keys(
            "landing/v1",
            &keys(&["landing/v1/session=run-a/inst=r1/0.log.gz"]),
        );
        assert_eq!(flat[0].prefix, "landing/v1/session=run-a");

        // The case that was failing: the aggregator partitions by date, so the
        // flat prefix holds nothing and the recording is nonetheless present.
        let dated = index_landed_keys(
            "landing/v1",
            &keys(&["landing/v1/dt=2026-08-04/session=run-b/inst=r1/0.log.gz"]),
        );
        assert_eq!(dated[0].prefix, "landing/v1/dt=2026-08-04/session=run-b");

        // And a session spanning two dates resolves to the shared parent, which
        // holds OTHER sessions — so whoever ingests it must filter by the
        // session id in each envelope rather than trusting the prefix.
        let spanning = index_landed_keys(
            "landing/v1",
            &keys(&[
                "landing/v1/dt=2026-08-04/session=run-c/inst=r1/9.log.gz",
                "landing/v1/dt=2026-08-05/session=run-c/inst=r1/0.log.gz",
                "landing/v1/dt=2026-08-05/session=run-d/inst=r1/0.log.gz",
            ]),
        );
        let c = spanning.iter().find(|r| r.session_id == "run-c").unwrap();
        assert_eq!(c.prefix, "landing/v1");
        assert!(
            spanning.iter().any(|r| r.session_id == "run-d"),
            "the shared parent holds another session — content, not the prefix, separates them"
        );
    }

    #[test]
    fn decode_object_handles_gzip_zstd_and_plain() {
        let payload = b"{\"a\":1}\n{\"b\":2}\n";

        // gzip by extension AND by magic bytes (deployed Vector aggregators
        // use compression: gzip with a default extension we don't control).
        let mut gz = Vec::new();
        {
            use std::io::Write as _;
            let mut enc = flate2::write::GzEncoder::new(&mut gz, flate2::Compression::default());
            enc.write_all(payload).unwrap();
            enc.finish().unwrap();
        }
        assert_eq!(
            decode_object("2026/07/10/x.log.gz", &gz).unwrap(),
            payload.to_vec()
        );
        assert_eq!(
            decode_object("2026/07/10/x.log", &gz).unwrap(),
            payload.to_vec(),
            "gzip magic sniff must decode extension-less keys"
        );

        let zst = zstd_encode(payload).unwrap();
        assert_eq!(
            decode_object("sessions/v1/s/data/part-00000.ndjsonl.zst", &zst).unwrap(),
            payload.to_vec()
        );

        assert_eq!(
            decode_object("plain.ndjson", payload).unwrap(),
            payload.to_vec()
        );
    }

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
    fn s3_config_requires_both_static_credentials() {
        let mut cfg = S3Config {
            endpoint: "http://127.0.0.1:9100".to_owned(),
            bucket: "deja-recordings".to_owned(),
            access_key: String::new(),
            secret_key: String::new(),
            region: "us-east-1".to_owned(),
            allow_http: true,
        };

        assert!(!cfg.has_static_credentials());
        cfg.access_key = "access".to_owned();
        assert!(!cfg.has_static_credentials());
        cfg.secret_key = "secret".to_owned();
        assert!(cfg.has_static_credentials());
        cfg.access_key = " ".to_owned();
        assert!(!cfg.has_static_credentials());
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
        let c = collate(chunk_lines(&[chunk]));
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
    fn the_correlation_index_is_in_tape_order_not_id_order() {
        // A caller reads "the first N correlations" off this list, so its order
        // has to be a fact about the recording. Ids here sort the OPPOSITE way
        // to the traffic, which is the only way to tell the two orders apart.
        let chunk = [
            envelope("i1", 1, Some("c-z"), "http_incoming"),
            envelope("i1", 2, Some("c-m"), "http_incoming"),
            envelope("i1", 3, Some("c-z"), "db"), // c-z reappears; it is not new
            envelope("i1", 4, Some("c-a"), "http_incoming"),
        ]
        .join("\n")
        .into_bytes();
        let c = collate(chunk_lines(&[chunk]));
        let order: Vec<&str> = c
            .correlations
            .iter()
            .filter_map(|r| r.correlation_id.as_deref())
            .collect();
        assert_eq!(
            order,
            vec!["c-z", "c-m", "c-a"],
            "rows follow first appearance in the recording, not the ids' sort order"
        );
    }

    #[test]
    fn collate_separates_instances() {
        let chunk = [
            envelope("i2", 1, Some("c1"), "db"),
            envelope("i1", 1, Some("c1"), "db"), // same gseq, different instance: NOT a dupe
        ]
        .join("\n")
        .into_bytes();
        let c = collate(chunk_lines(&[chunk]));
        assert_eq!(c.duplicates_dropped, 0);
        assert_eq!(c.events.len(), 2);
        assert_eq!(c.instances.len(), 2);
        assert_eq!(c.events[0].instance_id, "i1"); // sorted by (instance, gseq)
    }

    // -- sealing ------------------------------------------------------------

    /// A graph-node envelope as the record sink emits it: schema version 1 and
    /// the payload under `node`, not `event` — the node carries its own
    /// `recording_run_id` and `global_sequence` that flattening into the
    /// envelope would collide with.
    ///
    /// Copied from the shape the ingest side pins
    /// (`deja-orchestrator/src/s3`), which took it from the producer. It is
    /// spelled out rather than described because a fixture that wrote `event`
    /// here — a shape nothing emits — is exactly how a green test suite once
    /// coexisted with every graph node being discarded on the way in.
    fn graph_envelope(inst: &str, gseq: u64) -> String {
        format!(
            r#"{{"schema_version":1,"artifact_type":"deja_graph_node","instance_id":"{inst}","recording_run_id":"s1","capture":{{"mode":"session","session_id":"s1"}},"node":{{"recording_run_id":"s1","global_sequence":{gseq},"node_id":{gseq},"span_name":"payments_create"}}}}"#
        )
    }

    fn memory() -> DynStore {
        Arc::new(object_store::memory::InMemory::new())
    }

    fn block<T>(f: impl std::future::Future<Output = T>) -> T {
        runtime().unwrap().block_on(f)
    }

    fn seal(store: &DynStore, session_id: &str, lines: &[String]) -> SessionManifest {
        block(async {
            let collated = collate(lines.iter().map(|l| Cow::Borrowed(l.as_str())));
            write_seal(store, session_id, collated, lines.len()).await
        })
        .unwrap()
    }

    fn object_keys(store: &DynStore) -> Vec<String> {
        block(async {
            list_keys(store, "sessions")
                .await
                .unwrap()
                .into_iter()
                .map(|p| p.as_ref().to_owned())
                .collect()
        })
    }

    #[test]
    fn a_seal_writes_the_manifest_after_every_object_it_names() {
        // The manifest is the seal, so it may not exist until everything it
        // declares does. Any other order hands a reader a manifest pointing at
        // an object that is not there yet — a short read that looks like a
        // complete recording.
        let log = Arc::new(WriteLog::new());
        let store: DynStore = log.clone();
        let lines = vec![
            envelope("i1", 1, Some("c1"), "http_incoming"),
            envelope("i1", 2, Some("c1"), "db"),
        ];
        let manifest = seal(&store, "s1", &lines);

        let writes = log.writes();
        let manifest_key = layout::manifest_key("s1");
        assert_eq!(
            writes.last().map(String::as_str),
            Some(manifest_key.as_str()),
            "the manifest must be the last write: {writes:?}"
        );
        let sealed_at = writes.iter().position(|k| *k == manifest_key).unwrap();
        for part in &manifest.data_parts {
            let at = writes.iter().position(|k| k == &part.key).unwrap();
            assert!(
                at < sealed_at,
                "{} is declared complete before it is written",
                part.key
            );
        }
        let index = layout::correlations_key("s1", &manifest.seal_id);
        assert!(writes.iter().position(|k| *k == index).unwrap() < sealed_at);
    }

    #[test]
    fn an_interrupted_seal_reads_as_a_session_that_is_still_landing() {
        // Everything but the last PUT landed. A reader must not be able to tell
        // this from a session Vector is still flushing into, because both
        // answers are "rescan the landing" — and the alternative, reading the
        // parts that happen to be there, is a silently short recording.
        let store = memory();
        let lines = vec![envelope("i1", 1, Some("c1"), "http_incoming")];
        let manifest = seal(&store, "s1", &lines);
        block(async {
            store
                .delete(&object_store::path::Path::from(layout::manifest_key("s1")))
                .await
        })
        .unwrap();

        assert!(
            !object_keys(&store).is_empty(),
            "the data parts are still there — that is the point"
        );
        assert!(
            block(manifest_of(&store, "s1")).unwrap().is_none(),
            "no manifest, no seal, whatever is under data/"
        );
        // And a re-seal of the same content rewrites the same objects rather
        // than adding a second copy beside them.
        assert_eq!(seal(&store, "s1", &lines).seal_id, manifest.seal_id);
    }

    #[test]
    fn two_sealers_of_different_snapshots_cannot_truncate_each_others_parts() {
        // Two runs ingest one recording at once, and one caught the session
        // mid-flush so it has less of it. Content-addressed part keys make
        // their writes disjoint, so whichever manifest wins the shared key
        // names its own complete parts — rather than the longer seal's
        // manifest pointing at parts the shorter one overwrote.
        let store = memory();
        let long: Vec<String> = (1..=4)
            .map(|n| envelope("i1", n, Some("c1"), "db"))
            .collect();
        let short = long[..2].to_vec();

        let sealed_long = seal(&store, "s1", &long);
        let sealed_short = seal(&store, "s1", &short);

        assert_ne!(sealed_long.seal_id, sealed_short.seal_id);
        let long_keys: BTreeSet<&str> = sealed_long
            .data_parts
            .iter()
            .map(|p| p.key.as_str())
            .collect();
        let short_keys: BTreeSet<&str> = sealed_short
            .data_parts
            .iter()
            .map(|p| p.key.as_str())
            .collect();
        assert!(
            long_keys.is_disjoint(&short_keys),
            "different content must not share object keys: {long_keys:?} vs {short_keys:?}"
        );

        // The manifest that won names the two lines its sealer read...
        let winner = block(manifest_of(&store, "s1")).unwrap().unwrap();
        assert_eq!(winner.seal_id, sealed_short.seal_id);
        assert_eq!(block(session_lines(&store, &winner)).unwrap().len(), 2);
        // ...and the other seal's parts are untouched, not truncated to two.
        assert_eq!(block(session_lines(&store, &sealed_long)).unwrap().len(), 4);
    }

    #[test]
    fn sealing_the_same_content_twice_writes_the_same_objects() {
        let store = memory();
        let lines = vec![
            envelope("i1", 1, Some("c1"), "http_incoming"),
            envelope("i1", 2, Some("c1"), "db"),
        ];
        let first = seal(&store, "s1", &lines);
        let after_first = object_keys(&store);
        let second = seal(&store, "s1", &lines);

        assert_eq!(first.seal_id, second.seal_id);
        assert_eq!(
            first.data_parts.iter().map(|p| &p.key).collect::<Vec<_>>(),
            second.data_parts.iter().map(|p| &p.key).collect::<Vec<_>>(),
        );
        assert_eq!(
            after_first,
            object_keys(&store),
            "a re-seal of unchanged content adds no objects"
        );
    }

    #[test]
    fn a_seal_carries_the_graph_nodes_the_recording_captured() {
        // The compactor used to look only for `event`, so every graph envelope
        // read as an envelope with no payload and was dropped. Sealing on
        // ingest makes that fatal rather than wasteful: the sealed session is
        // what every later run reads, so a graph the seal discards is gone even
        // though the landing still has it.
        let store = memory();
        let lines = vec![
            envelope("i1", 0, Some("c1"), "http_incoming"),
            envelope("i1", 1, Some("c1"), "db"),
            // The same sequence numbers as the events above: graph nodes are
            // numbered in a space of their own, so these are not duplicates.
            graph_envelope("i1", 0),
            graph_envelope("i1", 1),
        ];
        let manifest = seal(&store, "s1", &lines);

        assert_eq!(manifest.counts.duplicates_dropped, 0);
        assert_eq!(manifest.counts.events, 2);
        assert_eq!(manifest.counts.graph_nodes, 2);
        // Coverage and the correlation index describe the boundary events; the
        // graph's own numbering must not invent gaps or an extra correlation.
        assert_eq!(manifest.instances.len(), 1);
        assert_eq!(manifest.instances[0].events, 2);
        assert!(manifest.instances[0].gaps.is_empty());
        assert_eq!(manifest.counts.correlations, 1);
        assert_eq!(manifest.envelope_schema_versions, vec![2]);

        let read_back = block(session_lines(&store, &manifest)).unwrap();
        assert_eq!(read_back.len(), 4);
        assert_eq!(
            read_back
                .iter()
                .filter(|l| l.contains("deja_graph_node"))
                .count(),
            2,
            "the graph rides in the data parts or the record side of it is lost"
        );
    }

    #[test]
    fn a_sealed_session_reads_back_the_lines_it_was_sealed_from() {
        // The seal's whole purpose is that the next run reads the same
        // recording without rescanning, so what comes out of the parts has to
        // be what went in — minus the duplicates and markers a rescan would
        // have dropped too.
        let store = memory();
        let lines = vec![
            envelope("i1", 2, Some("c1"), "db"),
            envelope("i1", 1, Some("c1"), "http_incoming"),
            envelope("i1", 2, Some("c1"), "db"), // duplicate
            r#"{"artifact_type":"deja_sink_marker","instance_id":"i1"}"#.to_owned(),
        ];
        let manifest = seal(&store, "s1", &lines);
        assert_eq!(manifest.counts.lines_in, 4);
        assert_eq!(manifest.counts.duplicates_dropped, 1);

        let read_back = block(session_lines(&store, &manifest)).unwrap();
        assert_eq!(read_back, vec![lines[1].clone(), lines[0].clone()]);
    }

    #[test]
    fn the_correlation_count_comes_off_the_seal_without_reading_the_tape() {
        // A caller asking how many correlations a recording has should not pay
        // for the recording. The manifest holds the count and the index sidecar
        // holds the rows; neither read touches a data part.
        let log = Arc::new(WriteLog::new());
        let store: DynStore = log.clone();
        let lines = vec![
            envelope("i1", 1, Some("c1"), "http_incoming"),
            envelope("i1", 2, Some("c1"), "db"),
            envelope("i1", 3, Some("c2"), "redis"),
            // Ambient traffic: shared across cases, so it is not a case.
            envelope("i1", 4, None, "id_generation"),
        ];
        let sealed = seal(&store, "s1", &lines);
        log.clear_reads();

        let manifest = block(manifest_of(&store, "s1")).unwrap().unwrap();
        assert_eq!(
            manifest.counts.correlations, 2,
            "the count is correlations, not buckets — the uncorrelated one is not a case"
        );

        let rows = block(correlation_index_of(&store, &manifest)).unwrap();
        assert_eq!(
            rows.len(),
            3,
            "the index still accounts for the uncorrelated events"
        );
        assert_eq!(
            rows.iter()
                .filter(|r| r.correlation_id.is_none())
                .map(|r| r.events)
                .sum::<u64>(),
            1
        );
        let c1 = rows
            .iter()
            .find(|r| r.correlation_id.as_deref() == Some("c1"))
            .unwrap();
        assert_eq!(c1.events, 2);
        assert!(
            c1.has_ingress,
            "c1 has an http_incoming, so replay can re-drive it"
        );
        assert!(
            !rows
                .iter()
                .find(|r| r.correlation_id.as_deref() == Some("c2"))
                .unwrap()
                .has_ingress
        );

        let reads = log.reads();
        let part_keys: Vec<&String> = sealed.data_parts.iter().map(|p| &p.key).collect();
        assert!(
            !reads.iter().any(|k| part_keys.contains(&k)),
            "the count and the index must not pull the tape: {reads:?}"
        );
    }

    #[test]
    fn an_unsealed_recording_has_no_manifest_to_answer_from() {
        let store = memory();
        assert!(block(manifest_of(&store, "never-sealed"))
            .unwrap()
            .is_none());
    }

    #[test]
    fn a_scan_that_found_nothing_is_not_sealed_as_an_empty_recording() {
        // Sealing is one-way as far as readers are concerned: once a manifest
        // exists nothing looks at the landing again. So a scan that produced no
        // records — run too early, pointed at the wrong prefix, or a landing of
        // nothing but markers — must leave the session unsealed rather than
        // freeze "this recording is empty" in place.
        let store = memory();
        let lines = [
            r#"{"artifact_type":"deja_sink_marker","instance_id":"i1"}"#.to_owned(),
            "not-an-envelope".to_owned(),
        ];
        let err = block(async {
            let collated = collate(lines.iter().map(|l| Cow::Borrowed(l.as_str())));
            write_seal(&store, "s1", collated, lines.len()).await
        })
        .unwrap_err();
        assert!(err.contains("refusing to seal"), "{err}");
        assert!(block(manifest_of(&store, "s1")).unwrap().is_none());
        assert!(
            object_keys(&store).is_empty(),
            "and nothing was written under the session root"
        );
    }

    #[test]
    fn a_body_larger_than_one_chunk_round_trips_through_a_multipart_upload() {
        // The failure this fixes: a data part is as big as the recording, and a
        // single-shot PUT sends it in one request that has to finish inside the
        // client's per-request timeout. Chunking makes each request a fixed
        // size — but only if the chunking itself is exact, including the case
        // where the body divides evenly and there is no tail.
        let store = memory();
        for len in [3_500usize, 4_096, 1] {
            let body: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
            let path = object_store::path::Path::from(format!("chunked/{len}"));
            block(put_chunked(&store, &path, &body, 1024)).unwrap();
            let got = block(async { store.get(&path).await.unwrap().bytes().await.unwrap() });
            assert_eq!(got.as_ref(), body.as_slice(), "len {len}");
        }
    }

    #[test]
    fn a_body_over_the_single_put_limit_is_uploaded_in_parts() {
        let log = Arc::new(WriteLog::new());
        let store: DynStore = log.clone();
        let key = "sessions/v1/s1/data/big";
        block(put(&store, key, vec![7u8; SINGLE_PUT_MAX_BYTES + 1])).unwrap();
        assert_eq!(
            log.multiparts(),
            vec![key.to_owned()],
            "a body past the single-PUT limit must not go up as one request"
        );

        block(put(&store, "sessions/v1/s1/data/small", vec![7u8; 1024])).unwrap();
        assert_eq!(
            log.multiparts().len(),
            1,
            "a small body still goes up as one PUT"
        );
    }

    /// An `ObjectStore` that records what was written, in the order the objects
    /// became visible, and what was read — so the seal's ordering can be
    /// asserted instead of assumed.
    #[derive(Debug)]
    struct WriteLog {
        inner: object_store::memory::InMemory,
        writes: Arc<std::sync::Mutex<Vec<String>>>,
        multiparts: std::sync::Mutex<Vec<String>>,
        reads: std::sync::Mutex<Vec<String>>,
    }

    impl WriteLog {
        fn new() -> Self {
            Self {
                inner: object_store::memory::InMemory::new(),
                writes: Default::default(),
                multiparts: Default::default(),
                reads: Default::default(),
            }
        }

        fn writes(&self) -> Vec<String> {
            self.writes.lock().unwrap().clone()
        }

        fn multiparts(&self) -> Vec<String> {
            self.multiparts.lock().unwrap().clone()
        }

        fn reads(&self) -> Vec<String> {
            self.reads.lock().unwrap().clone()
        }

        fn clear_reads(&self) {
            self.reads.lock().unwrap().clear();
        }
    }

    impl std::fmt::Display for WriteLog {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "WriteLog({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for WriteLog {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            let result = self.inner.put_opts(location, payload, opts).await?;
            self.writes
                .lock()
                .unwrap()
                .push(location.as_ref().to_owned());
            Ok(result)
        }

        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::PutMultipartOpts,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.multiparts
                .lock()
                .unwrap()
                .push(location.as_ref().to_owned());
            let upload = self.inner.put_multipart_opts(location, opts).await?;
            Ok(Box::new(LoggedUpload {
                inner: upload,
                key: location.as_ref().to_owned(),
                writes: Arc::clone(&self.writes),
            }))
        }

        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.reads
                .lock()
                .unwrap()
                .push(location.as_ref().to_owned());
            self.inner.get_opts(location, options).await
        }

        async fn delete(&self, location: &object_store::path::Path) -> object_store::Result<()> {
            self.inner.delete(location).await
        }

        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'_, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
        ) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
        ) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    /// Records the key when the upload COMPLETES, because that — not the first
    /// part — is the moment the object exists.
    #[derive(Debug)]
    struct LoggedUpload {
        inner: Box<dyn object_store::MultipartUpload>,
        key: String,
        writes: Arc<std::sync::Mutex<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl object_store::MultipartUpload for LoggedUpload {
        fn put_part(&mut self, data: object_store::PutPayload) -> object_store::UploadPart {
            self.inner.put_part(data)
        }

        async fn complete(&mut self) -> object_store::Result<object_store::PutResult> {
            let result = self.inner.complete().await?;
            self.writes.lock().unwrap().push(self.key.clone());
            Ok(result)
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            self.inner.abort().await
        }
    }
}
