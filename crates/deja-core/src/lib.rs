use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::io::{BufRead, BufReader, ErrorKind, Read as _, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const ARTIFACT_SCHEMA_VERSION_V1: ArtifactSchemaVersion = ArtifactSchemaVersion::V1;
pub const ARTIFACT_STORAGE_KIND_DIRECTORY: ArtifactStorageKind = ArtifactStorageKind::Directory;
pub const METADATA_FILE_NAME: &str = "metadata.json";
pub const EVENTS_FILE_NAME: &str = "events.jsonl";
pub const EXECUTION_GRAPH_FILE_NAME: &str = "execution-graph.jsonl";
pub const MANIFEST_FILE_NAME: &str = "manifest.json";
pub const INSPECTION_SUMMARY_FILE_NAME: &str = "inspection-summary.json";
// Legacy v1 (preload-era) artifact-manifest protocol. The semantic pipeline
// does not produce these manifests; the types remain so existing v1 artifacts
// keep (de)serializing and validating.
pub const PRELOAD_BOOTSTRAP_PROTOCOL_V1: &str = "deja.preload-bootstrap/v1";
pub const LD_PRELOAD_ENV_VAR: &str = "LD_PRELOAD";
pub const PRELOAD_PROTOCOL_ENV_VAR: &str = "DEJA_PRELOAD_PROTOCOL";
pub const PRELOAD_TRANSPORT_ENV_VAR: &str = "DEJA_PRELOAD_TRANSPORT";
pub const PRELOAD_MODE_ENV_VAR: &str = "DEJA_PRELOAD_MODE";
pub const PRELOAD_ARTIFACT_ROOT_ENV_VAR: &str = "DEJA_PRELOAD_ARTIFACT_ROOT";
pub const PRELOAD_LIBRARY_PATH_ENV_VAR: &str = "DEJA_PRELOAD_LIBRARY_PATH";
pub const REGRESSION_REPORT_FILE_NAME: &str = "regression-report.json";
pub const BEHAVIOR_DIFF_FILE_NAME: &str = "behavior-diff.json";
pub const DEJA_GRAPH_DIR_ENV_VAR: &str = "DEJA_GRAPH_DIR";

pub const EXECUTION_GRAPH_SELECTED_FIELDS: &[&str] = &[
    "request_id",
    "flow",
    "payment_id",
    "merchant_id",
    "connector_name",
    "payment_method",
    "tenant_id",
    "http.status_code",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ArtifactSchemaVersion {
    #[serde(rename = "deja.artifact/v1")]
    V1,
}

impl ArtifactSchemaVersion {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::V1 => "deja.artifact/v1",
        }
    }
}

impl fmt::Display for ArtifactSchemaVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for ArtifactSchemaVersion {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "deja.artifact/v1" => Ok(Self::V1),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactStorageKind {
    Directory,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactLayoutDescriptor {
    pub kind: ArtifactStorageKind,
    pub metadata_file: String,
    pub events_file: String,
    pub inspection_summary_file: String,
}

impl Default for ArtifactLayoutDescriptor {
    fn default() -> Self {
        Self {
            kind: ARTIFACT_STORAGE_KIND_DIRECTORY,
            metadata_file: METADATA_FILE_NAME.to_owned(),
            events_file: EVENTS_FILE_NAME.to_owned(),
            inspection_summary_file: INSPECTION_SUMMARY_FILE_NAME.to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactLayout {
    pub root: PathBuf,
    pub metadata_path: PathBuf,
    pub events_path: PathBuf,
    pub inspection_summary_path: PathBuf,
}

impl ArtifactLayout {
    pub fn from_root(root: impl AsRef<Path>) -> Self {
        Self::from_descriptor(root, &ArtifactLayoutDescriptor::default())
    }

    pub fn from_descriptor(root: impl AsRef<Path>, descriptor: &ArtifactLayoutDescriptor) -> Self {
        let root = root.as_ref().to_path_buf();

        Self {
            metadata_path: root.join(&descriptor.metadata_file),
            events_path: root.join(&descriptor.events_file),
            inspection_summary_path: root.join(&descriptor.inspection_summary_file),
            root,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionGraphRecord {
    #[serde(flatten)]
    pub node: ExecutionGraphNode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionGraphNode {
    pub node_id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<u64>,
    #[serde(default)]
    pub causal_parent_ids: Vec<u64>,
    pub sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recording_run_id: Option<String>,
    pub span_name: String,
    pub target: String,
    pub level: String,
    #[serde(default)]
    pub fields: BTreeMap<String, serde_json::Value>,
    pub started_ns: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closed_ns: Option<u64>,
}

impl ExecutionGraphNode {
    pub fn request_id(&self) -> Option<&str> {
        self.fields
            .get("request_id")
            .and_then(serde_json::Value::as_str)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionGraphEdge {
    pub parent_id: u64,
    pub child_id: u64,
    pub kind: ExecutionGraphEdgeKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionGraphEdgeKind {
    Parent,
    Causal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BehaviorDiff {
    pub baseline_path: String,
    pub candidate_path: String,
    pub request_diffs: Vec<BehaviorRequestDiff>,
}

impl BehaviorDiff {
    pub fn is_empty(&self) -> bool {
        self.request_diffs.iter().all(|diff| diff.diffs.is_empty())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BehaviorRequestDiff {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    pub diffs: Vec<BehaviorDiffEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BehaviorDiffEntry {
    pub kind: BehaviorDiffKind,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub baseline: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate: Option<serde_json::Value>,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BehaviorDiffKind {
    InsertedSpan,
    RemovedSpan,
    RenamedSpan,
    ChangedFields,
    ChangedParent,
    ChangedCausalParents,
    OrderChanged,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactDescriptor {
    pub artifact_id: String,
    pub created_at: String,
    pub producer: ProducerMetadata,
    pub layout: ArtifactLayoutDescriptor,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerMetadata {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactMetadataDocument {
    pub schema_version: ArtifactSchemaVersion,
    pub artifact: ArtifactDescriptor,
    pub session: SessionMetadata,
    pub support_matrix: SupportMatrixMetadata,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preload: Option<PreloadBootstrap>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RawArtifactMetadataDocument {
    pub schema_version: String,
    pub artifact: ArtifactDescriptor,
    pub session: SessionMetadata,
    pub support_matrix: SupportMatrixMetadata,
    #[serde(default)]
    pub preload: Option<PreloadBootstrap>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionMetadata {
    pub session_id: String,
    pub recorded_at: String,
    pub command: Vec<String>,
    pub working_directory: String,
    pub target: TargetMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetMetadata {
    pub os: String,
    pub arch: String,
    pub libc: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreloadTransport {
    LdPreload,
}

impl PreloadTransport {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LdPreload => "ld_preload",
        }
    }
}

impl fmt::Display for PreloadTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for PreloadTransport {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "ld_preload" => Ok(Self::LdPreload),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreloadMode {
    Record,
    Replay,
}

impl PreloadMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Record => "record",
            Self::Replay => "replay",
        }
    }
}

impl fmt::Display for PreloadMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for PreloadMode {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "record" => Ok(Self::Record),
            "replay" => Ok(Self::Replay),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreloadBootstrap {
    pub protocol_version: String,
    pub transport: PreloadTransport,
    pub mode: PreloadMode,
    pub artifact_root: PathBuf,
    pub library_path: PathBuf,
}

impl PreloadBootstrap {
    pub fn new(
        mode: PreloadMode,
        artifact_root: impl AsRef<Path>,
        library_path: impl AsRef<Path>,
    ) -> Self {
        Self {
            protocol_version: PRELOAD_BOOTSTRAP_PROTOCOL_V1.to_owned(),
            transport: PreloadTransport::LdPreload,
            mode,
            artifact_root: artifact_root.as_ref().to_path_buf(),
            library_path: library_path.as_ref().to_path_buf(),
        }
    }

    pub fn environment_overrides(
        &self,
        inherited_ld_preload: Option<&str>,
    ) -> Vec<(String, String)> {
        let library_path = self.library_path.to_string_lossy().into_owned();
        let ld_preload = match inherited_ld_preload {
            Some(existing) if !existing.trim().is_empty() => {
                format!("{library_path}:{existing}")
            }
            _ => library_path.clone(),
        };

        vec![
            (LD_PRELOAD_ENV_VAR.to_owned(), ld_preload),
            (
                PRELOAD_PROTOCOL_ENV_VAR.to_owned(),
                self.protocol_version.clone(),
            ),
            (
                PRELOAD_TRANSPORT_ENV_VAR.to_owned(),
                self.transport.to_string(),
            ),
            (PRELOAD_MODE_ENV_VAR.to_owned(), self.mode.to_string()),
            (
                PRELOAD_ARTIFACT_ROOT_ENV_VAR.to_owned(),
                self.artifact_root.to_string_lossy().into_owned(),
            ),
            (PRELOAD_LIBRARY_PATH_ENV_VAR.to_owned(), library_path),
        ]
    }

    pub fn from_current_environment() -> Result<Option<Self>, PreloadBootstrapError> {
        Self::from_environment(|key| std::env::var(key).ok())
    }

    pub fn from_environment<F>(mut lookup: F) -> Result<Option<Self>, PreloadBootstrapError>
    where
        F: FnMut(&str) -> Option<String>,
    {
        let protocol = lookup(PRELOAD_PROTOCOL_ENV_VAR);
        let transport = lookup(PRELOAD_TRANSPORT_ENV_VAR);
        let mode = lookup(PRELOAD_MODE_ENV_VAR);
        let artifact_root = lookup(PRELOAD_ARTIFACT_ROOT_ENV_VAR);
        let library_path = lookup(PRELOAD_LIBRARY_PATH_ENV_VAR);

        if protocol.is_none()
            && transport.is_none()
            && mode.is_none()
            && artifact_root.is_none()
            && library_path.is_none()
        {
            return Ok(None);
        }

        let protocol = protocol.ok_or(PreloadBootstrapError::MissingVariable {
            name: PRELOAD_PROTOCOL_ENV_VAR,
        })?;
        if protocol != PRELOAD_BOOTSTRAP_PROTOCOL_V1 {
            return Err(PreloadBootstrapError::UnsupportedProtocol { found: protocol });
        }

        let transport = transport.ok_or(PreloadBootstrapError::MissingVariable {
            name: PRELOAD_TRANSPORT_ENV_VAR,
        })?;
        let transport = PreloadTransport::from_str(&transport)
            .map_err(|_| PreloadBootstrapError::UnsupportedTransport { found: transport })?;

        let mode = mode.ok_or(PreloadBootstrapError::MissingVariable {
            name: PRELOAD_MODE_ENV_VAR,
        })?;
        let mode = PreloadMode::from_str(&mode)
            .map_err(|_| PreloadBootstrapError::UnsupportedMode { found: mode })?;

        let artifact_root = artifact_root.ok_or(PreloadBootstrapError::MissingVariable {
            name: PRELOAD_ARTIFACT_ROOT_ENV_VAR,
        })?;
        let library_path = library_path.ok_or(PreloadBootstrapError::MissingVariable {
            name: PRELOAD_LIBRARY_PATH_ENV_VAR,
        })?;

        Ok(Some(Self {
            protocol_version: PRELOAD_BOOTSTRAP_PROTOCOL_V1.to_owned(),
            transport,
            mode,
            artifact_root: PathBuf::from(artifact_root),
            library_path: PathBuf::from(library_path),
        }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupportMatrixMetadata {
    pub launched_child_only: bool,
    pub supported_boundaries: Vec<SupportedBoundary>,
    pub unsupported_notes: Vec<String>,
}

pub fn v1_supported_boundaries() -> Vec<SupportedBoundary> {
    vec![
        SupportedBoundary::Time,
        SupportedBoundary::Random,
        SupportedBoundary::Environment,
        SupportedBoundary::Http,
    ]
}

pub fn v1_unsupported_notes() -> Vec<String> {
    vec![
        "No live attach support in v1".to_owned(),
        "No TLS HTTP capture in v1".to_owned(),
        "No deterministic scheduling or ptrace-based replay in v1".to_owned(),
        "No signal interception in v1".to_owned(),
        "No mmap or memory-mapped I/O replay in v1".to_owned(),
        "No shared memory replay in v1".to_owned(),
        "No vDSO bypass or interception in v1".to_owned(),
        "No io_uring replay in v1".to_owned(),
        "No container or namespace-aware replay in v1".to_owned(),
        "No cross-machine replay in v1".to_owned(),
        "Bootstrap artifacts may exist before boundary hooks record events".to_owned(),
    ]
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostEnvironment {
    pub operating_system: String,
    pub architecture: String,
    pub libc: String,
}

impl HostEnvironment {
    pub fn current() -> Self {
        Self {
            operating_system: std::env::consts::OS.to_owned(),
            architecture: std::env::consts::ARCH.to_owned(),
            libc: current_libc_name(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryLinkage {
    Dynamic,
    Static,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestrictedExecutionReason {
    SetUserId,
    SetGroupId,
}

impl fmt::Display for RestrictedExecutionReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SetUserId => formatter.write_str("setuid binary"),
            Self::SetGroupId => formatter.write_str("setgid binary"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetBinaryMetadata {
    pub path: PathBuf,
    pub architecture: String,
    pub linkage: BinaryLinkage,
    pub restricted_execution: Option<RestrictedExecutionReason>,
}

impl TargetBinaryMetadata {
    pub fn inspect(path: impl AsRef<Path>) -> Result<Self, UnsupportedEnvironmentError> {
        let path = path.as_ref().to_path_buf();

        let map_io = |source: std::io::Error| UnsupportedEnvironmentError::TargetInspectionFailed {
            path: path.clone(),
            reason: source.to_string(),
        };

        let mut file = fs::File::open(&path).map_err(map_io)?;

        // Read only the 64-byte ELF header first, not the entire binary.
        let mut header = [0u8; 64];
        file.read_exact(&mut header).map_err(|source| {
            if source.kind() == ErrorKind::UnexpectedEof {
                UnsupportedEnvironmentError::UnsupportedTargetBinary {
                    path: path.clone(),
                    detail: "file is too small to be a supported ELF executable".to_owned(),
                }
            } else {
                map_io(source)
            }
        })?;

        if &header[..4] != b"\x7fELF" {
            return Err(UnsupportedEnvironmentError::UnsupportedTargetBinary {
                path,
                detail: "binary is not an ELF executable".to_owned(),
            });
        }

        if header[4] != 2 {
            return Err(UnsupportedEnvironmentError::UnsupportedTargetBinary {
                path,
                detail: "only 64-bit ELF executables are supported in v1".to_owned(),
            });
        }

        if header[5] != 1 {
            return Err(UnsupportedEnvironmentError::UnsupportedTargetBinary {
                path,
                detail: "only little-endian ELF executables are supported in v1".to_owned(),
            });
        }

        let machine = read_u16_le(&header, 18).ok_or_else(|| {
            UnsupportedEnvironmentError::UnsupportedTargetBinary {
                path: path.clone(),
                detail: "ELF header is truncated".to_owned(),
            }
        })?;
        let architecture = elf_machine_name(machine);

        let program_header_offset = read_u64_le(&header, 32).ok_or_else(|| {
            UnsupportedEnvironmentError::UnsupportedTargetBinary {
                path: path.clone(),
                detail: "ELF program header table is missing".to_owned(),
            }
        })? as usize;
        let program_header_entry_size = read_u16_le(&header, 54).ok_or_else(|| {
            UnsupportedEnvironmentError::UnsupportedTargetBinary {
                path: path.clone(),
                detail: "ELF program header entry size is missing".to_owned(),
            }
        })? as usize;
        let program_header_count = read_u16_le(&header, 56).ok_or_else(|| {
            UnsupportedEnvironmentError::UnsupportedTargetBinary {
                path: path.clone(),
                detail: "ELF program header count is missing".to_owned(),
            }
        })? as usize;

        if program_header_entry_size < 4 || program_header_count == 0 {
            return Err(UnsupportedEnvironmentError::UnsupportedTargetBinary {
                path,
                detail: "ELF program header table is incomplete".to_owned(),
            });
        }

        // Read only the program header table (typically a few hundred bytes).
        let phdr_table_size = program_header_count * program_header_entry_size;
        let mut phdr_bytes = vec![0u8; phdr_table_size];
        use std::io::Seek;
        file.seek(std::io::SeekFrom::Start(program_header_offset as u64))
            .map_err(map_io)?;
        file.read_exact(&mut phdr_bytes).map_err(|source| {
            if source.kind() == ErrorKind::UnexpectedEof {
                UnsupportedEnvironmentError::UnsupportedTargetBinary {
                    path: path.clone(),
                    detail: "ELF program headers are truncated".to_owned(),
                }
            } else {
                map_io(source)
            }
        })?;

        let mut has_interp_segment = false;
        for index in 0..program_header_count {
            let offset = index * program_header_entry_size;
            let p_type = read_u32_le(&phdr_bytes, offset).ok_or_else(|| {
                UnsupportedEnvironmentError::UnsupportedTargetBinary {
                    path: path.clone(),
                    detail: "ELF program headers are truncated".to_owned(),
                }
            })?;

            if p_type == 3 {
                has_interp_segment = true;
                break;
            }
        }

        Ok(Self {
            path: path.clone(),
            architecture,
            linkage: if has_interp_segment {
                BinaryLinkage::Dynamic
            } else {
                BinaryLinkage::Static
            },
            restricted_execution: restricted_execution_reason(&path)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedLaunch {
    pub target_path: PathBuf,
    pub bootstrap: PreloadBootstrap,
}

impl PreparedLaunch {
    pub fn environment_overrides(
        &self,
        inherited_ld_preload: Option<&str>,
    ) -> Vec<(String, String)> {
        self.bootstrap.environment_overrides(inherited_ld_preload)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PreloadBootstrapError {
    #[error("missing required preload bootstrap variable {name}")]
    MissingVariable { name: &'static str },
    #[error("unsupported preload bootstrap protocol `{found}`")]
    UnsupportedProtocol { found: String },
    #[error("unsupported preload bootstrap transport `{found}`")]
    UnsupportedTransport { found: String },
    #[error("unsupported preload bootstrap mode `{found}`")]
    UnsupportedMode { found: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum UnsupportedEnvironmentError {
    #[error("v1 execution is Linux-first; current platform `{actual}` is unsupported")]
    OperatingSystem { actual: String },
    #[error("v1 execution currently supports x86_64 only; current architecture `{actual}` is unsupported")]
    Architecture { actual: String },
    #[error("v1 execution currently supports glibc only; current libc `{actual}` is unsupported")]
    Libc { actual: String },
    #[error("failed to inspect target binary {path}: {reason}")]
    TargetInspectionFailed { path: PathBuf, reason: String },
    #[error("target binary {path} is not a supported launched-child executable: {detail}")]
    UnsupportedTargetBinary { path: PathBuf, detail: String },
    #[error("target binary {path} is not x86_64 (found `{actual}`)")]
    TargetArchitecture { path: PathBuf, actual: String },
    #[error("target binary {path} appears to be statically linked; LD_PRELOAD requires a dynamically linked glibc executable")]
    StaticTargetBinary { path: PathBuf },
    #[error("target binary {path} cannot use LD_PRELOAD in v1 because secure-execution restrictions apply ({reason})")]
    RestrictedTargetBinary {
        path: PathBuf,
        reason: RestrictedExecutionReason,
    },
}

pub fn validate_supported_host_environment(
    host: &HostEnvironment,
) -> Result<(), UnsupportedEnvironmentError> {
    if host.operating_system != "linux" {
        return Err(UnsupportedEnvironmentError::OperatingSystem {
            actual: host.operating_system.clone(),
        });
    }

    if host.architecture != "x86_64" {
        return Err(UnsupportedEnvironmentError::Architecture {
            actual: host.architecture.clone(),
        });
    }

    if host.libc != "glibc" {
        return Err(UnsupportedEnvironmentError::Libc {
            actual: host.libc.clone(),
        });
    }

    Ok(())
}

pub fn validate_supported_execution_environment(
    host: &HostEnvironment,
    target: &TargetBinaryMetadata,
) -> Result<(), UnsupportedEnvironmentError> {
    validate_supported_host_environment(host)?;

    if target.architecture != "x86_64" {
        return Err(UnsupportedEnvironmentError::TargetArchitecture {
            path: target.path.clone(),
            actual: target.architecture.clone(),
        });
    }

    if target.linkage != BinaryLinkage::Dynamic {
        return Err(UnsupportedEnvironmentError::StaticTargetBinary {
            path: target.path.clone(),
        });
    }

    if let Some(reason) = target.restricted_execution {
        return Err(UnsupportedEnvironmentError::RestrictedTargetBinary {
            path: target.path.clone(),
            reason,
        });
    }

    Ok(())
}

pub fn prepare_launched_child_execution(
    mode: PreloadMode,
    host: &HostEnvironment,
    target: &TargetBinaryMetadata,
    artifact_root: impl AsRef<Path>,
    preload_library_path: impl AsRef<Path>,
) -> Result<PreparedLaunch, UnsupportedEnvironmentError> {
    validate_supported_execution_environment(host, target)?;

    Ok(PreparedLaunch {
        target_path: target.path.clone(),
        bootstrap: PreloadBootstrap::new(mode, artifact_root, preload_library_path),
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SupportedBoundary {
    Time,
    Random,
    Environment,
    Http,
    Socket,
    Dns,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectionSummaryDocument {
    pub schema_version: ArtifactSchemaVersion,
    pub total_records: u64,
    pub counts: EventCounts,
    pub fidelity: ArtifactFidelitySummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RawInspectionSummaryDocument {
    pub schema_version: String,
    pub total_records: u64,
    pub counts: EventCounts,
    pub fidelity: ArtifactFidelitySummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventCounts {
    pub time: u64,
    pub random: u64,
    pub environment: u64,
    pub http: u64,
    #[serde(default)]
    pub socket: u64,
    #[serde(default)]
    pub dns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactFidelitySummary {
    pub exact_records: u64,
    pub semantic_records: u64,
    pub divergence_markers: Vec<DivergenceMarker>,
}

// ---------------------------------------------------------------------------
// Correlation health — validates that event tagging is correct
// ---------------------------------------------------------------------------

/// Health report for correlation integrity across the recorded event stream.
///
/// Detects two failure modes:
/// - **Contamination**: a single connection's events carry different request IDs,
///   meaning events from one request leaked into another's scope.
/// - **Orphans**: events with no `request_id` that fall between the start and end
///   of a known request, meaning they should have been tagged but weren't.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CorrelationHealth {
    /// Total events examined.
    pub total_events: u64,
    /// Events that have a `request_id` set.
    pub correlated_events: u64,
    /// Events without a `request_id`.
    pub uncorrelated_events: u64,
    /// Fraction of events that are correlated (0.0–1.0). Computed as
    /// `correlated_events / total_events`. Stored as parts-per-thousand to
    /// avoid floating point in serialized JSON.
    pub coverage_per_mille: u64,
    /// Connections where all events share the same `request_id`.
    pub clean_connections: u64,
    /// Connections whose events carry more than one distinct `request_id`.
    /// Non-zero means cross-request contamination occurred.
    pub contaminated_connections: u64,
    /// Details of each contaminated connection: which connection_id, which
    /// request_ids were seen, and how many events per request_id.
    pub contamination_details: Vec<ContaminationDetail>,
    /// Events that fall inside a request's sequence range but lack a
    /// `request_id` — they should have been tagged but weren't.
    pub orphaned_events: u64,
    /// Sequence ranges of detected orphans for debugging.
    pub orphan_details: Vec<OrphanDetail>,
    /// Overall verdict.
    pub status: CorrelationStatus,
}

impl Default for CorrelationHealth {
    fn default() -> Self {
        Self {
            total_events: 0,
            correlated_events: 0,
            uncorrelated_events: 0,
            coverage_per_mille: 0,
            clean_connections: 0,
            contaminated_connections: 0,
            contamination_details: Vec::new(),
            orphaned_events: 0,
            orphan_details: Vec::new(),
            status: CorrelationStatus::NoCorrelation,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CorrelationStatus {
    /// Every event is correlated, no contamination detected.
    Healthy,
    /// Correlation is present but some events are uncorrelated or orphans exist.
    Degraded,
    /// Cross-request contamination detected — correlation data is unreliable.
    Contaminated,
    /// No `request_id` found on any event (correlation not in use).
    NoCorrelation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContaminationDetail {
    pub connection_id: u64,
    pub request_ids: Vec<String>,
    pub events_per_request_id: Vec<(String, u64)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrphanDetail {
    pub sequence: u64,
    pub event_id: String,
    pub event_type: String,
}

/// Validate correlation integrity across the entire event stream.
///
/// Checks:
/// 1. **Connection–request consistency**: every `connection_id` should map to
///    at most one `request_id`. If events on connection 42 carry request_id "A"
///    and later request_id "B", that's contamination.
/// 2. **Orphan detection**: events that fall within the sequence range of a
///    request (between first and last event of a request_id) but have no
///    `request_id` themselves are orphans.
/// 3. **Coverage**: fraction of events that carry a `request_id`.
pub fn validate_correlation_health(events: &[EventRecord]) -> CorrelationHealth {
    use std::collections::BTreeMap;

    let total_events = events.len() as u64;
    let mut correlated_events: u64 = 0;
    let mut uncorrelated_events: u64 = 0;

    // connection_id → set of (request_id, count)
    let mut conn_requests: BTreeMap<u64, BTreeMap<String, u64>> = BTreeMap::new();

    // Track per-request sequence ranges for orphan detection
    let mut request_first_seq: BTreeMap<String, u64> = BTreeMap::new();
    let mut request_last_seq: BTreeMap<String, u64> = BTreeMap::new();

    for record in events {
        match &record.request_id {
            Some(id) => {
                correlated_events += 1;

                // Track connection → request_id mapping
                let conn_id = connection_id_from_event(&record.event);
                if conn_id > 0 {
                    conn_requests
                        .entry(conn_id)
                        .or_default()
                        .entry(id.clone())
                        .and_modify(|c| *c += 1)
                        .or_insert(1);
                }

                // Track sequence range for this request_id
                let seq = record.metadata.sequence;
                request_first_seq
                    .entry(id.clone())
                    .and_modify(|s| *s = (*s).min(seq))
                    .or_insert(seq);
                request_last_seq
                    .entry(id.clone())
                    .and_modify(|s| *s = (*s).max(seq))
                    .or_insert(seq);
            }
            None => {
                uncorrelated_events += 1;
            }
        }
    }

    // Check 1: contamination — connections with multiple request_ids
    let mut clean_connections: u64 = 0;
    let mut contaminated_connections: u64 = 0;
    let mut contamination_details: Vec<ContaminationDetail> = Vec::new();

    for (conn_id, req_map) in &conn_requests {
        if req_map.len() > 1 {
            contaminated_connections += 1;
            contamination_details.push(ContaminationDetail {
                connection_id: *conn_id,
                request_ids: req_map.keys().cloned().collect(),
                events_per_request_id: req_map.iter().map(|(k, v)| (k.clone(), *v)).collect(),
            });
        } else {
            clean_connections += 1;
        }
    }

    // Check 2: orphan detection — events without request_id that fall inside
    // a request's sequence range
    let mut orphaned_events: u64 = 0;
    let mut orphan_details: Vec<OrphanDetail> = Vec::new();

    for record in events {
        if record.request_id.is_none() {
            let seq = record.metadata.sequence;
            // Is this event's sequence inside any request's range?
            let inside_any_request = request_first_seq.iter().any(|(req_id, &first)| {
                let last = request_last_seq.get(req_id).copied().unwrap_or(first);
                seq >= first && seq <= last
            });
            if inside_any_request {
                orphaned_events += 1;
                orphan_details.push(OrphanDetail {
                    sequence: seq,
                    event_id: record.metadata.event_id.clone(),
                    event_type: event_kind_name(&record.event).to_owned(),
                });
            }
        }
    }

    // Compute coverage
    let coverage_per_mille = (correlated_events * 1000)
        .checked_div(total_events)
        .unwrap_or(0);

    // Determine status
    let status = if correlated_events == 0 {
        CorrelationStatus::NoCorrelation
    } else if contaminated_connections > 0 {
        CorrelationStatus::Contaminated
    } else if orphaned_events > 0 || uncorrelated_events > 0 {
        CorrelationStatus::Degraded
    } else {
        CorrelationStatus::Healthy
    };

    CorrelationHealth {
        total_events,
        correlated_events,
        uncorrelated_events,
        coverage_per_mille,
        clean_connections,
        contaminated_connections,
        contamination_details,
        orphaned_events,
        orphan_details,
        status,
    }
}

/// Extract connection_id from a BoundaryEvent if it's a socket event.
fn connection_id_from_event(event: &BoundaryEvent) -> u64 {
    match event {
        BoundaryEvent::Socket(se) => se.connection_id,
        _ => 0,
    }
}

/// Return the kind name for a boundary event (for diagnostics).
fn event_kind_name(event: &BoundaryEvent) -> &'static str {
    match event {
        BoundaryEvent::Time(_) => "time",
        BoundaryEvent::Random(_) => "random",
        BoundaryEvent::Environment(_) => "environment",
        BoundaryEvent::Http(_) => "http",
        BoundaryEvent::Socket(_) => "socket",
        BoundaryEvent::Dns(_) => "dns",
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventRecord {
    pub metadata: RecordMetadata,
    pub event: BoundaryEvent,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

pub fn inspection_summary_from_events(events: &[EventRecord]) -> InspectionSummaryDocument {
    use std::collections::BTreeSet;

    let mut counts = EventCounts {
        time: 0,
        random: 0,
        environment: 0,
        http: 0,
        socket: 0,
        dns: 0,
    };
    let mut exact_records = 0;
    let mut semantic_records = 0;
    let mut divergence_set = BTreeSet::new();

    for record in events {
        match &record.event {
            BoundaryEvent::Time(_) => counts.time += 1,
            BoundaryEvent::Random(_) => counts.random += 1,
            BoundaryEvent::Environment(_) => counts.environment += 1,
            BoundaryEvent::Http(_) => counts.http += 1,
            BoundaryEvent::Socket(_) => counts.socket += 1,
            BoundaryEvent::Dns(_) => counts.dns += 1,
        }

        match record.metadata.capture_fidelity {
            CaptureFidelity::Exact => exact_records += 1,
            CaptureFidelity::Semantic => semantic_records += 1,
        }

        for marker in &record.metadata.divergence_markers {
            divergence_set.insert(*marker);
        }
    }

    InspectionSummaryDocument {
        schema_version: ARTIFACT_SCHEMA_VERSION_V1,
        total_records: events.len() as u64,
        counts,
        fidelity: ArtifactFidelitySummary {
            exact_records,
            semantic_records,
            divergence_markers: divergence_set.into_iter().collect(),
        },
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordMetadata {
    pub event_id: String,
    pub sequence: u64,
    pub capture_fidelity: CaptureFidelity,
    pub replay_classification: ReplayClassification,
    pub divergence_markers: Vec<DivergenceMarker>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaptureFidelity {
    Exact,
    Semantic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayClassification {
    DeterministicEquivalent,
    SemanticallyEquivalent,
    Divergent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DivergenceMarker {
    ExternalStateOmitted,
    HttpResponseBodyTruncated,
    NonReplayableInputObserved,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum BoundaryEvent {
    Time(TimeBoundaryEvent),
    Random(RandomBoundaryEvent),
    Environment(EnvironmentBoundaryEvent),
    Http(HttpExchangeEvent),
    Socket(SocketBoundaryEvent),
    Dns(DnsBoundaryEvent),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeBoundaryEvent {
    pub source: TimeSource,
    pub seconds: i64,
    pub nanos: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeSource {
    SystemTime,
    Monotonic,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RandomBoundaryEvent {
    pub source: RandomSource,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RandomSource {
    Getrandom,
    DevUrandom,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentBoundaryEvent {
    pub operation: EnvironmentOperation,
    pub key: String,
    pub value: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvironmentOperation {
    Get,
    Set,
    Remove,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpExchangeEvent {
    pub request: HttpRequestRecord,
    pub response: HttpResponseRecord,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpProtocolVersion {
    Http11,
}

impl HttpProtocolVersion {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Http11 => "HTTP/1.1",
        }
    }
}

impl fmt::Display for HttpProtocolVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpSemanticExchangeEvent {
    pub protocol: HttpProtocolVersion,
    pub request: HttpSemanticRequest,
    pub response: HttpSemanticResponse,
    pub fidelity: HttpSemanticFidelity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpSemanticFidelity {
    pub capture_fidelity: CaptureFidelity,
    pub replay_classification: ReplayClassification,
    pub divergence_markers: Vec<DivergenceMarker>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpSemanticRequest {
    pub method: String,
    pub url: String,
    pub headers: Vec<HttpHeader>,
    pub body: HttpBodySemantic,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpSemanticResponse {
    pub status: u16,
    pub reason_phrase: String,
    pub headers: Vec<HttpHeader>,
    pub body: HttpBodySemantic,
    pub body_truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpBodySemantic {
    pub content_type: Option<String>,
    pub byte_len: usize,
}

pub fn normalize_http_exchange_event(
    exchange: &HttpExchangeEvent,
    metadata: &RecordMetadata,
) -> HttpSemanticExchangeEvent {
    let request = normalize_http_request_record(&exchange.request);
    let response = normalize_http_response_record(
        &exchange.response,
        metadata
            .divergence_markers
            .contains(&DivergenceMarker::HttpResponseBodyTruncated),
    );

    HttpSemanticExchangeEvent {
        protocol: HttpProtocolVersion::Http11,
        request,
        response,
        fidelity: HttpSemanticFidelity {
            capture_fidelity: metadata.capture_fidelity,
            replay_classification: metadata.replay_classification,
            divergence_markers: metadata.divergence_markers.clone(),
        },
    }
}

fn normalize_http_request_record(request: &HttpRequestRecord) -> HttpSemanticRequest {
    let method = request.method.trim().to_ascii_uppercase();
    let scheme = request.scheme.trim().to_ascii_lowercase();
    let authority = request.authority.trim();
    let raw_path_and_query = request.path_and_query.trim();
    let path_and_query = if raw_path_and_query.is_empty() {
        "/".to_owned()
    } else if raw_path_and_query.starts_with('/') {
        raw_path_and_query.to_owned()
    } else {
        format!("/{raw_path_and_query}")
    };
    let url = format!("{scheme}://{authority}{path_and_query}");

    HttpSemanticRequest {
        method,
        url,
        headers: normalize_http_headers(&request.headers),
        body: normalize_http_body_record(&request.body),
    }
}

fn normalize_http_response_record(
    response: &HttpResponseRecord,
    body_truncated: bool,
) -> HttpSemanticResponse {
    HttpSemanticResponse {
        status: response.status,
        reason_phrase: response.reason_phrase.trim().to_owned(),
        headers: normalize_http_headers(&response.headers),
        body: normalize_http_body_record(&response.body),
        body_truncated,
    }
}

fn normalize_http_body_record(body: &HttpBodyRecord) -> HttpBodySemantic {
    let content_type = body
        .content_type
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);

    HttpBodySemantic {
        content_type,
        byte_len: body.bytes.len(),
    }
}

fn normalize_http_headers(headers: &[HttpHeader]) -> Vec<HttpHeader> {
    let mut normalized = headers
        .iter()
        .map(|header| HttpHeader {
            name: header.name.trim().to_ascii_lowercase(),
            value: header.value.trim().to_owned(),
        })
        .collect::<Vec<_>>();

    normalized.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.value.cmp(&b.value)));
    normalized
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpRequestRecord {
    pub method: String,
    pub scheme: String,
    pub authority: String,
    pub path_and_query: String,
    pub headers: Vec<HttpHeader>,
    pub body: HttpBodyRecord,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpResponseRecord {
    pub status: u16,
    pub reason_phrase: String,
    pub headers: Vec<HttpHeader>,
    pub body: HttpBodyRecord,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpHeader {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpBodyRecord {
    pub content_type: Option<String>,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactBundle {
    pub metadata: ArtifactMetadataDocument,
    pub events: Vec<EventRecord>,
    pub inspection_summary: InspectionSummaryDocument,
}

impl ArtifactBundle {
    pub fn write_to_directory(
        &self,
        root: impl AsRef<Path>,
    ) -> Result<ArtifactLayout, ArtifactError> {
        let layout = ArtifactLayout::from_descriptor(root, &self.metadata.artifact.layout);

        fs::create_dir_all(&layout.root).map_err(|source| ArtifactError::Io {
            path: layout.root.clone(),
            source,
        })?;

        write_json_file(&layout.metadata_path, &self.metadata)?;
        write_json_lines_file(&layout.events_path, &self.events)?;
        write_json_file(&layout.inspection_summary_path, &self.inspection_summary)?;

        Ok(layout)
    }

    pub fn read_from_directory(root: impl AsRef<Path>) -> Result<Self, ArtifactError> {
        let inspection = ArtifactInspector::read(root)?;
        let layout = inspection.layout;
        let events = read_event_stream(&layout.events_path)?;

        // Recompute summary from actual events — the on-disk summary may be
        // stale if the writer uses append-only persistence.
        let inspection_summary = inspection_summary_from_events(&events);

        Ok(Self {
            metadata: inspection.metadata,
            events,
            inspection_summary,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactInspection {
    pub layout: ArtifactLayout,
    pub metadata: ArtifactMetadataDocument,
    pub inspection_summary: InspectionSummaryDocument,
}

pub struct ArtifactInspector;

impl ArtifactInspector {
    pub fn read(root: impl AsRef<Path>) -> Result<ArtifactInspection, ArtifactError> {
        let initial_layout = ArtifactLayout::from_root(root);
        let metadata = read_metadata_document(&initial_layout.metadata_path)?;

        let layout =
            ArtifactLayout::from_descriptor(&initial_layout.root, &metadata.artifact.layout);
        let summary = read_summary_document(&layout.inspection_summary_path)?;
        ensure_supported_schema(
            &layout.inspection_summary_path,
            summary.schema_version,
            metadata.schema_version,
        )?;

        Ok(ArtifactInspection {
            layout,
            metadata,
            inspection_summary: summary,
        })
    }
}

pub fn read_artifact_metadata(root: impl AsRef<Path>) -> Result<ArtifactInspection, ArtifactError> {
    ArtifactInspector::read(root)
}

#[derive(Debug, Error)]
pub enum ArtifactError {
    #[error("artifact path does not exist: {path}")]
    MissingPath { path: PathBuf },
    #[error("failed to read artifact path {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("artifact file {path} is corrupt: {message}")]
    CorruptArtifact { path: PathBuf, message: String },
    #[error("artifact schema mismatch in {path}: expected {expected}, found {found}")]
    SchemaVersionMismatch {
        path: PathBuf,
        expected: ArtifactSchemaVersion,
        found: String,
    },
}

fn ensure_supported_schema(
    path: &Path,
    found: ArtifactSchemaVersion,
    expected: ArtifactSchemaVersion,
) -> Result<(), ArtifactError> {
    if found == expected {
        Ok(())
    } else {
        Err(ArtifactError::SchemaVersionMismatch {
            path: path.to_path_buf(),
            expected,
            found: found.to_string(),
        })
    }
}

fn parse_schema_version(
    path: &Path,
    raw_version: String,
    expected: ArtifactSchemaVersion,
) -> Result<ArtifactSchemaVersion, ArtifactError> {
    ArtifactSchemaVersion::from_str(&raw_version).map_err(|_| {
        ArtifactError::SchemaVersionMismatch {
            path: path.to_path_buf(),
            expected,
            found: raw_version,
        }
    })
}

fn read_metadata_document(path: &Path) -> Result<ArtifactMetadataDocument, ArtifactError> {
    let raw: RawArtifactMetadataDocument = read_json_file(path)?;
    let schema_version =
        parse_schema_version(path, raw.schema_version, ARTIFACT_SCHEMA_VERSION_V1)?;
    ensure_supported_schema(path, schema_version, ARTIFACT_SCHEMA_VERSION_V1)?;

    Ok(ArtifactMetadataDocument {
        schema_version,
        artifact: raw.artifact,
        session: raw.session,
        support_matrix: raw.support_matrix,
        preload: raw.preload,
    })
}

fn read_summary_document(path: &Path) -> Result<InspectionSummaryDocument, ArtifactError> {
    let raw: RawInspectionSummaryDocument = read_json_file(path)?;
    let schema_version =
        parse_schema_version(path, raw.schema_version, ARTIFACT_SCHEMA_VERSION_V1)?;

    Ok(InspectionSummaryDocument {
        schema_version,
        total_records: raw.total_records,
        counts: raw.counts,
        fidelity: raw.fidelity,
    })
}

fn write_json_file<T: Serialize>(path: &Path, value: &T) -> Result<(), ArtifactError> {
    let bytes =
        serde_json::to_vec_pretty(value).map_err(|error| ArtifactError::CorruptArtifact {
            path: path.to_path_buf(),
            message: error.to_string(),
        })?;

    fs::write(path, bytes).map_err(|source| ArtifactError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn write_json_lines_file<T: Serialize>(path: &Path, values: &[T]) -> Result<(), ArtifactError> {
    let mut file = fs::File::create(path).map_err(|source| ArtifactError::Io {
        path: path.to_path_buf(),
        source,
    })?;

    for value in values {
        let line =
            serde_json::to_string(value).map_err(|error| ArtifactError::CorruptArtifact {
                path: path.to_path_buf(),
                message: error.to_string(),
            })?;

        file.write_all(line.as_bytes())
            .and_then(|_| file.write_all(b"\n"))
            .map_err(|source| ArtifactError::Io {
                path: path.to_path_buf(),
                source,
            })?;
    }

    Ok(())
}

fn read_json_file<T>(path: &Path) -> Result<T, ArtifactError>
where
    T: for<'de> Deserialize<'de>,
{
    let bytes = fs::read(path).map_err(|source| match source.kind() {
        ErrorKind::NotFound => ArtifactError::MissingPath {
            path: path.to_path_buf(),
        },
        _ => ArtifactError::Io {
            path: path.to_path_buf(),
            source,
        },
    })?;

    serde_json::from_slice(&bytes).map_err(|error| ArtifactError::CorruptArtifact {
        path: path.to_path_buf(),
        message: error.to_string(),
    })
}

fn read_event_stream(path: &Path) -> Result<Vec<EventRecord>, ArtifactError> {
    let file = fs::File::open(path).map_err(|source| match source.kind() {
        ErrorKind::NotFound => ArtifactError::MissingPath {
            path: path.to_path_buf(),
        },
        _ => ArtifactError::Io {
            path: path.to_path_buf(),
            source,
        },
    })?;

    let reader = BufReader::new(file);
    let mut events = Vec::new();

    for (line_index, line) in reader.lines().enumerate() {
        let line = line.map_err(|source| ArtifactError::Io {
            path: path.to_path_buf(),
            source,
        })?;

        if line.trim().is_empty() {
            continue;
        }

        let event = serde_json::from_str::<EventRecord>(&line).map_err(|error| {
            ArtifactError::CorruptArtifact {
                path: path.to_path_buf(),
                message: format!("invalid event record on line {}: {error}", line_index + 1),
            }
        })?;

        events.push(event);
    }

    Ok(events)
}

fn current_libc_name() -> String {
    if cfg!(all(target_os = "linux", target_env = "gnu")) {
        "glibc".to_owned()
    } else if cfg!(all(target_os = "linux", target_env = "musl")) {
        "musl".to_owned()
    } else if cfg!(target_os = "macos") {
        "libSystem".to_owned()
    } else {
        "unknown".to_owned()
    }
}

fn elf_machine_name(machine: u16) -> String {
    match machine {
        62 => "x86_64".to_owned(),
        183 => "aarch64".to_owned(),
        3 => "x86".to_owned(),
        other => format!("elf-machine-{other}"),
    }
}

fn restricted_execution_reason(
    path: &Path,
) -> Result<Option<RestrictedExecutionReason>, UnsupportedEnvironmentError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mode = fs::metadata(path)
            .map_err(
                |source| UnsupportedEnvironmentError::TargetInspectionFailed {
                    path: path.to_path_buf(),
                    reason: source.to_string(),
                },
            )?
            .permissions()
            .mode();

        if mode & 0o4000 != 0 {
            return Ok(Some(RestrictedExecutionReason::SetUserId));
        }

        if mode & 0o2000 != 0 {
            return Ok(Some(RestrictedExecutionReason::SetGroupId));
        }
    }

    Ok(None)
}

fn read_u16_le(bytes: &[u8], offset: usize) -> Option<u16> {
    let slice = bytes.get(offset..(offset + 2))?;
    Some(u16::from_le_bytes([slice[0], slice[1]]))
}

fn read_u32_le(bytes: &[u8], offset: usize) -> Option<u32> {
    let slice = bytes.get(offset..(offset + 4))?;
    Some(u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]))
}

fn read_u64_le(bytes: &[u8], offset: usize) -> Option<u64> {
    let slice = bytes.get(offset..(offset + 8))?;
    Some(u64::from_le_bytes([
        slice[0], slice[1], slice[2], slice[3], slice[4], slice[5], slice[6], slice[7],
    ]))
}

// ---------------------------------------------------------------------------
// Socket boundary events (v2)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SocketOperation {
    Connect,
    Send,
    Receive,
    Close,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventDirection {
    /// Connection initiated by this process (outbound: connect())
    Outbound,
    /// Connection accepted by this process (inbound: accept())
    Inbound,
}

fn default_direction() -> EventDirection {
    EventDirection::Outbound
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolHint {
    Http11,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SocketBoundaryEvent {
    pub operation: SocketOperation,
    /// Whether this socket is inbound (accepted) or outbound (connected).
    #[serde(default = "default_direction")]
    pub direction: EventDirection,
    pub peer_address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_address: Option<String>,
    pub protocol_hint: ProtocolHint,
    pub data: Vec<u8>,
    pub fd: i64,
    /// Stable connection identifier assigned on connect()/accept().
    /// Survives fd reuse. Zero means unassigned (legacy artifacts).
    #[serde(default)]
    pub connection_id: u64,
    /// Byte offset within this connection+direction stream.
    /// Cumulative count of data bytes seen so far.
    #[serde(default)]
    pub stream_offset: u64,
}

// ---------------------------------------------------------------------------
// DNS boundary events (v2)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AddressFamily {
    Ipv4,
    Ipv6,
    Unspecified,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DnsBoundaryEvent {
    pub hostname: String,
    pub resolved_addresses: Vec<String>,
    pub address_family: AddressFamily,
}

pub fn v2_supported_boundaries() -> Vec<SupportedBoundary> {
    vec![
        SupportedBoundary::Time,
        SupportedBoundary::Random,
        SupportedBoundary::Environment,
        SupportedBoundary::Http,
        SupportedBoundary::Socket,
        SupportedBoundary::Dns,
    ]
}

// ---------------------------------------------------------------------------
// Request-case correlation model
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestCase {
    pub case_id: String,
    pub inbound: RequestCaseInbound,
    /// Non-deterministic inputs captured during this request (Time, Random, Environment).
    pub recorded_inputs: Vec<EventRecord>,
    /// External dependencies called during this request (Socket, DNS).
    pub recorded_outputs: Vec<EventRecord>,
    /// The HTTP response sent back to the caller.
    pub response: RequestCaseOutbound,
    /// If this request was spawned by another, the parent's case_id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_case_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestCaseInbound {
    pub method: String,
    pub path: String,
    pub headers: Vec<HttpHeader>,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestCaseOutbound {
    pub status: u16,
    pub headers: Vec<HttpHeader>,
    pub body: Vec<u8>,
}

/// Groups a flat event stream into request cases.
///
/// If any event has a `request_id`, uses request-ID-based grouping.
/// Otherwise falls back to the legacy HTTP-boundary heuristic.
pub fn group_events_into_cases(events: &[EventRecord]) -> Vec<RequestCase> {
    let has_request_ids = events.iter().any(|e| e.request_id.is_some());

    if has_request_ids {
        group_by_request_id(events)
    } else {
        group_by_http_boundaries(events)
    }
}

fn group_by_request_id(events: &[EventRecord]) -> Vec<RequestCase> {
    use std::collections::BTreeMap;

    // Use BTreeMap for deterministic ordering
    let mut groups: BTreeMap<String, Vec<&EventRecord>> = BTreeMap::new();
    let mut ungrouped: Vec<&EventRecord> = Vec::new();

    for record in events {
        match &record.request_id {
            Some(id) => {
                groups.entry(id.clone()).or_default().push(record);
            }
            None => ungrouped.push(record),
        }
    }

    let mut cases = Vec::new();

    // Collect background events into __background__ pseudo-case
    if !ungrouped.is_empty() {
        let mut inputs = Vec::new();
        let mut outputs = Vec::new();
        for record in &ungrouped {
            match &record.event {
                BoundaryEvent::Time(_)
                | BoundaryEvent::Random(_)
                | BoundaryEvent::Environment(_) => inputs.push((*record).clone()),
                _ => outputs.push((*record).clone()),
            }
        }
        cases.push(RequestCase {
            case_id: "__background__".to_owned(),
            inbound: RequestCaseInbound {
                method: "BACKGROUND".to_owned(),
                path: "/__startup__".to_owned(),
                headers: vec![],
                body: vec![],
            },
            recorded_inputs: inputs,
            recorded_outputs: outputs,
            response: RequestCaseOutbound {
                status: 0,
                headers: vec![],
                body: vec![],
            },
            parent_case_id: None,
        });
    }

    for (req_id, group_events) in &groups {
        let mut inbound: Option<RequestCaseInbound> = None;
        let mut response: Option<RequestCaseOutbound> = None;
        let mut recorded_inputs = Vec::new();
        let mut recorded_outputs = Vec::new();

        for record in group_events {
            match &record.event {
                // Non-deterministic inputs → recorded_inputs
                BoundaryEvent::Time(_)
                | BoundaryEvent::Random(_)
                | BoundaryEvent::Environment(_) => {
                    recorded_inputs.push((*record).clone());
                }

                // Inbound socket events with direction == Inbound
                BoundaryEvent::Socket(se)
                    if se.direction == EventDirection::Inbound
                        && se.operation == SocketOperation::Receive =>
                {
                    // Raw inbound request bytes — create a placeholder inbound
                    // Full HTTP parsing will be added when the raw socket pipeline is complete
                    if inbound.is_none() {
                        inbound = Some(RequestCaseInbound {
                            method: "RAW".to_owned(),
                            path: "/".to_owned(),
                            headers: vec![],
                            body: se.data.clone(),
                        });
                    }
                }
                BoundaryEvent::Socket(se)
                    if se.direction == EventDirection::Inbound
                        && se.operation == SocketOperation::Send =>
                {
                    // Raw outbound response bytes
                    if response.is_none() {
                        response = Some(RequestCaseOutbound {
                            status: 0,
                            headers: vec![],
                            body: se.data.clone(),
                        });
                    }
                }

                // Outbound socket events → recorded_outputs (dependency calls)
                BoundaryEvent::Socket(_) => {
                    recorded_outputs.push((*record).clone());
                }

                // DNS → recorded_outputs
                BoundaryEvent::Dns(_) => {
                    recorded_outputs.push((*record).clone());
                }

                // v1 HTTP exchange → split into inbound + response
                BoundaryEvent::Http(exchange) => {
                    inbound = Some(RequestCaseInbound {
                        method: exchange.request.method.clone(),
                        path: exchange.request.path_and_query.clone(),
                        headers: exchange.request.headers.clone(),
                        body: exchange.request.body.bytes.clone(),
                    });
                    response = Some(RequestCaseOutbound {
                        status: exchange.response.status,
                        headers: exchange.response.headers.clone(),
                        body: exchange.response.body.bytes.clone(),
                    });
                }
            }
        }

        cases.push(RequestCase {
            case_id: req_id.clone(),
            inbound: inbound.unwrap_or_else(|| RequestCaseInbound {
                method: "UNKNOWN".to_owned(),
                path: "/".to_owned(),
                headers: vec![],
                body: vec![],
            }),
            recorded_inputs,
            recorded_outputs,
            response: response.unwrap_or_else(|| RequestCaseOutbound {
                status: 0,
                headers: vec![],
                body: vec![],
            }),
            parent_case_id: None,
        });
    }

    cases
}

/// Legacy fallback: group by HTTP event boundaries when no request IDs are present.
fn group_by_http_boundaries(events: &[EventRecord]) -> Vec<RequestCase> {
    let mut cases = Vec::new();
    let mut pending_deps: Vec<EventRecord> = Vec::new();
    let mut http_events: Vec<&EventRecord> = Vec::new();

    for record in events {
        if matches!(&record.event, BoundaryEvent::Http(_)) {
            http_events.push(record);

            if http_events.len() >= 2 {
                let inbound_rec = http_events[http_events.len() - 2];
                let outbound_rec = http_events[http_events.len() - 1];
                if let (BoundaryEvent::Http(req_exchange), BoundaryEvent::Http(resp_exchange)) =
                    (&inbound_rec.event, &outbound_rec.event)
                {
                    // Split pending_deps into inputs and outputs
                    let (inputs, outputs) = split_deps_by_type(&pending_deps);
                    cases.push(RequestCase {
                        case_id: format!("case-{}", cases.len() + 1),
                        inbound: RequestCaseInbound {
                            method: req_exchange.request.method.clone(),
                            path: req_exchange.request.path_and_query.clone(),
                            headers: req_exchange.request.headers.clone(),
                            body: req_exchange.request.body.bytes.clone(),
                        },
                        recorded_inputs: inputs,
                        recorded_outputs: outputs,
                        response: RequestCaseOutbound {
                            status: resp_exchange.response.status,
                            headers: resp_exchange.response.headers.clone(),
                            body: resp_exchange.response.body.bytes.clone(),
                        },
                        parent_case_id: None,
                    });
                    pending_deps.clear();
                }
            }
        } else {
            pending_deps.push(record.clone());
        }
    }

    // If only one HTTP event, treat everything as a single case
    if cases.is_empty() {
        if let Some(http_record) = http_events.first() {
            if let BoundaryEvent::Http(exchange) = &http_record.event {
                let (inputs, outputs) = split_deps_by_type(&pending_deps);
                cases.push(RequestCase {
                    case_id: "case-1".to_owned(),
                    inbound: RequestCaseInbound {
                        method: exchange.request.method.clone(),
                        path: exchange.request.path_and_query.clone(),
                        headers: exchange.request.headers.clone(),
                        body: exchange.request.body.bytes.clone(),
                    },
                    recorded_inputs: inputs,
                    recorded_outputs: outputs,
                    response: RequestCaseOutbound {
                        status: exchange.response.status,
                        headers: exchange.response.headers.clone(),
                        body: exchange.response.body.bytes.clone(),
                    },
                    parent_case_id: None,
                });
            }
        }
    }

    cases
}

/// Helper to split legacy dependencies into inputs vs outputs.
fn split_deps_by_type(deps: &[EventRecord]) -> (Vec<EventRecord>, Vec<EventRecord>) {
    let mut inputs = Vec::new();
    let mut outputs = Vec::new();
    for dep in deps {
        match &dep.event {
            BoundaryEvent::Time(_) | BoundaryEvent::Random(_) | BoundaryEvent::Environment(_) => {
                inputs.push(dep.clone())
            }
            _ => outputs.push(dep.clone()),
        }
    }
    (inputs, outputs)
}

// ---------------------------------------------------------------------------
// Comparison configuration and result types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ComparisonConfig {
    #[serde(default)]
    pub noise_rules: Vec<NoiseRule>,
    #[serde(default)]
    pub float_tolerance: Option<f64>,
    #[serde(default)]
    pub ignore_array_order: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoiseRule {
    pub path_pattern: String,
    pub rule_type: NoiseRuleType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NoiseRuleType {
    /// Ignore this field entirely during comparison.
    Ignore,
    /// Treat as noise if the value looks like an ISO-8601 timestamp.
    TimestampIso8601,
    /// Treat as noise if the value looks like a UUID.
    Uuid,
    /// Treat as noise if the value is a numeric ID (any integer).
    NumericId,
    /// Treat as noise if the value contains this substring.
    Contains(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaseComparisonResult {
    pub case_id: String,
    pub baseline_case_id: String,
    pub classification: ComparisonClassification,
    pub field_diffs: Vec<FieldDiff>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonClassification {
    Identical,
    NoiseOnly,
    Regression,
}

impl fmt::Display for ComparisonClassification {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identical => formatter.write_str("identical"),
            Self::NoiseOnly => formatter.write_str("noise_only"),
            Self::Regression => formatter.write_str("regression"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldDiff {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub baseline_value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_value: Option<String>,
    pub kind: FieldDiffKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldDiffKind {
    Identical,
    Noise,
    Regression,
    BaselineOnly,
    CandidateOnly,
}

impl fmt::Display for FieldDiffKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identical => formatter.write_str("identical"),
            Self::Noise => formatter.write_str("noise"),
            Self::Regression => formatter.write_str("regression"),
            Self::BaselineOnly => formatter.write_str("baseline_only"),
            Self::CandidateOnly => formatter.write_str("candidate_only"),
        }
    }
}

/// Protocol-level summary for a regression report.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ProtocolSummary {
    /// Protocol label (REDIS, PG, HTTP, GRPC, HTTP2, UNKNOWN).
    pub protocol: String,
    /// Number of connections for this protocol.
    pub connection_count: u64,
    /// Number of matched cases with identical responses.
    pub identical_count: u64,
    /// Number of cases with only noise differences.
    pub noise_only_count: u64,
    /// Number of cases with regressions.
    pub regression_count: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RegressionReport {
    pub schema_version: ArtifactSchemaVersion,
    pub baseline_artifact_id: String,
    pub candidate_artifact_id: String,
    pub total_cases: u64,
    pub identical_count: u64,
    pub noise_only_count: u64,
    pub regression_count: u64,
    pub unmatched_baseline_count: u64,
    pub unmatched_candidate_count: u64,
    pub case_results: Vec<CaseComparisonResult>,
    /// Per-protocol breakdown of regression results.
    #[serde(default)]
    pub protocol_summaries: Vec<ProtocolSummary>,
}

impl RegressionReport {
    pub fn has_regressions(&self) -> bool {
        self.regression_count > 0 || self.unmatched_baseline_count > 0
    }
}

// ---------------------------------------------------------------------------
// Connection summary and artifact manifest for integrity verification
// ---------------------------------------------------------------------------

/// Summary of a single connection's recorded data, used for integrity checks.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ConnectionSummary {
    /// Stable connection identifier.
    pub connection_id: u64,
    /// File descriptor at time of recording.
    pub fd: i64,
    /// Peer address (e.g. "192.168.1.3:6379").
    pub peer_address: String,
    /// Direction of this connection.
    pub direction: EventDirection,
    /// Total bytes sent in this direction.
    pub byte_count: u64,
    /// Number of data chunks (events) recorded.
    pub chunk_count: u64,
    /// SHA-256 hash of the concatenated data bytes.
    pub stream_hash: String,
}

/// Artifact manifest — written at the end of recording for integrity verification.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ArtifactManifest {
    /// Schema version for the manifest itself.
    pub schema_version: String,
    /// Total number of events in the artifact.
    pub total_events: u64,
    /// Total bytes of data across all connections.
    pub total_data_bytes: u64,
    /// Per-connection summaries.
    pub connections: Vec<ConnectionSummary>,
    /// SHA-256 hash of the entire manifest (excluding this field).
    pub manifest_hash: String,
}

impl ArtifactManifest {
    pub const SCHEMA_VERSION: &'static str = "deja.manifest/v1";
}
