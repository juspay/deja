use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

use deja_core::{
    ExecutionGraphNode, ExecutionGraphRecord, DEJA_GRAPH_DIR_ENV_VAR, EXECUTION_GRAPH_FILE_NAME,
};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::Subscriber;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

use crate::{
    current_recording_run_id, now_ns, AsyncRecordWriter, JsonlSink, WriterConfig,
    WriterStatsSnapshot,
};

static GRAPH_NODE_BY_TRACING_SPAN_ID: OnceLock<Mutex<HashMap<u64, u64>>> = OnceLock::new();

fn graph_node_map() -> &'static Mutex<HashMap<u64, u64>> {
    GRAPH_NODE_BY_TRACING_SPAN_ID.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Return the active tracing span id and matching execution-graph node id.
///
/// This is populated by [`ExecutionGraphLayer`]. When the graph layer is not installed,
/// or the current span was not observed by the layer, both values may be absent.
pub fn current_execution_graph_context() -> (Option<u64>, Option<u64>) {
    let tracing_span_id = tracing::Span::current().id().map(|id| id.into_u64());
    let graph_node_id = tracing_span_id.and_then(|id| {
        graph_node_map()
            .lock()
            .ok()
            .and_then(|map| map.get(&id).copied())
    });
    (tracing_span_id, graph_node_id)
}

/// Tracing subscriber layer that records span lifecycle data as an execution graph.
pub struct ExecutionGraphLayer {
    writer: AsyncRecordWriter<ExecutionGraphRecord>,
    node_ids: AtomicU64,
    sequence: AtomicU64,
}

impl ExecutionGraphLayer {
    /// Create a graph layer writing `execution-graph.jsonl` under `artifact_dir`.
    pub fn new(artifact_dir: impl AsRef<Path>) -> std::io::Result<Self> {
        Self::new_with_writer_config(artifact_dir, WriterConfig::from_env())
    }

    /// Create a graph layer with caller-supplied async writer settings.
    pub fn new_with_writer_config(
        artifact_dir: impl AsRef<Path>,
        writer_config: WriterConfig,
    ) -> std::io::Result<Self> {
        std::fs::create_dir_all(artifact_dir.as_ref())?;
        let sink = JsonlSink::new(&execution_graph_path(artifact_dir.as_ref()))?;

        Ok(Self {
            writer: AsyncRecordWriter::new(sink, writer_config),
            node_ids: AtomicU64::new(0),
            sequence: AtomicU64::new(0),
        })
    }

    /// Create a graph layer from `DEJA_GRAPH_DIR`.
    ///
    /// Returns `None` when the environment variable is unset.
    pub fn from_env() -> Option<std::io::Result<Self>> {
        std::env::var(DEJA_GRAPH_DIR_ENV_VAR)
            .ok()
            .map(|dir| Self::new(Path::new(&dir)))
    }

    fn next_node_id(&self) -> u64 {
        self.node_ids.fetch_add(1, Ordering::SeqCst)
    }

    fn next_sequence(&self) -> u64 {
        self.sequence.fetch_add(1, Ordering::SeqCst)
    }

    fn write_record(&self, node: ExecutionGraphNode) {
        let _ = self.writer.record(ExecutionGraphRecord { node });
    }

    /// Flush queued graph records through the configured sink.
    pub fn flush(&self) -> std::io::Result<()> {
        self.writer.flush()
    }

    /// Snapshot health counters for graph recording.
    pub fn writer_stats(&self) -> WriterStatsSnapshot {
        self.writer.stats()
    }
}

impl<S> Layer<S> for ExecutionGraphLayer
where
    S: Subscriber,
    S: for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let parent_id = graph_parent_id(attrs, &ctx);
        let metadata = attrs.metadata();
        let mut fields = BTreeMap::new();
        attrs.record(&mut JsonFieldVisitor::new(&mut fields));

        if let Some(span) = ctx.span(id) {
            let node_id = self.next_node_id();
            if let Ok(mut map) = graph_node_map().lock() {
                map.insert(id.into_u64(), node_id);
            }

            span.extensions_mut().insert(GraphSpanState {
                node_id,
                parent_id,
                causal_parent_ids: Vec::new(),
                sequence: self.next_sequence(),
                span_name: metadata.name().to_owned(),
                target: metadata.target().to_owned(),
                level: metadata.level().to_string(),
                fields,
                started_ns: now_ns(),
            });
        }
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            if let Some(state) = span.extensions_mut().get_mut::<GraphSpanState>() {
                values.record(&mut JsonFieldVisitor::new(&mut state.fields));
            }
        }
    }

    fn on_follows_from(&self, id: &Id, follows: &Id, ctx: Context<'_, S>) {
        let Some(causal_parent_id) = ctx.span(follows).and_then(|span| {
            span.extensions()
                .get::<GraphSpanState>()
                .map(|state| state.node_id)
        }) else {
            return;
        };

        if let Some(span) = ctx.span(id) {
            if let Some(state) = span.extensions_mut().get_mut::<GraphSpanState>() {
                if !state.causal_parent_ids.contains(&causal_parent_id) {
                    state.causal_parent_ids.push(causal_parent_id);
                }
            }
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else {
            return;
        };
        let Some(state) = span.extensions_mut().remove::<GraphSpanState>() else {
            return;
        };
        if let Ok(mut map) = graph_node_map().lock() {
            map.remove(&id.into_u64());
        }

        let closed_ns = now_ns().max(state.started_ns);
        self.write_record(state.into_node(Some(closed_ns)));
    }
}

fn graph_parent_id<S>(attrs: &Attributes<'_>, ctx: &Context<'_, S>) -> Option<u64>
where
    S: Subscriber,
    S: for<'lookup> LookupSpan<'lookup>,
{
    attrs
        .parent()
        .and_then(|parent| node_id_for_span(parent, ctx))
        .or_else(|| {
            attrs
                .is_contextual()
                .then(|| {
                    ctx.current_span()
                        .id()
                        .and_then(|id| node_id_for_span(id, ctx))
                })
                .flatten()
        })
}

fn node_id_for_span<S>(id: &Id, ctx: &Context<'_, S>) -> Option<u64>
where
    S: Subscriber,
    S: for<'lookup> LookupSpan<'lookup>,
{
    ctx.span(id).and_then(|span| {
        span.extensions()
            .get::<GraphSpanState>()
            .map(|state| state.node_id)
    })
}

/// Path to the execution graph file within an artifact directory.
pub fn execution_graph_path(artifact_dir: &Path) -> PathBuf {
    artifact_dir.join(EXECUTION_GRAPH_FILE_NAME)
}

/// Read all execution graph records from the JSONL graph file.
pub fn read_execution_graph_records(
    artifact_dir: &Path,
) -> std::io::Result<Vec<ExecutionGraphRecord>> {
    let content = std::fs::read_to_string(execution_graph_path(artifact_dir))?;
    let mut records = Vec::new();
    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(record) = serde_json::from_str::<ExecutionGraphRecord>(line) {
            records.push(record);
        }
    }
    Ok(records)
}

#[derive(Debug)]
struct GraphSpanState {
    node_id: u64,
    parent_id: Option<u64>,
    causal_parent_ids: Vec<u64>,
    sequence: u64,
    span_name: String,
    target: String,
    level: String,
    fields: BTreeMap<String, serde_json::Value>,
    started_ns: u64,
}

impl GraphSpanState {
    fn into_node(self, closed_ns: Option<u64>) -> ExecutionGraphNode {
        ExecutionGraphNode {
            node_id: self.node_id,
            // Stamped by the recording hook when the node is tape-carried;
            // the file-sink path predating graph-as-events leaves it 0.
            global_sequence: 0,
            parent_id: self.parent_id,
            causal_parent_ids: self.causal_parent_ids,
            sequence: self.sequence,
            recording_run_id: current_recording_run_id(),
            span_name: self.span_name,
            target: self.target,
            level: self.level,
            fields: self.fields,
            started_ns: self.started_ns,
            closed_ns,
        }
    }
}

struct JsonFieldVisitor<'a> {
    fields: &'a mut BTreeMap<String, serde_json::Value>,
}

impl<'a> JsonFieldVisitor<'a> {
    fn new(fields: &'a mut BTreeMap<String, serde_json::Value>) -> Self {
        Self { fields }
    }

    fn insert(&mut self, field: &Field, value: serde_json::Value) {
        self.fields.insert(field.name().to_owned(), value);
    }
}

impl Visit for JsonFieldVisitor<'_> {
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.insert(field, serde_json::Value::Bool(value));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.insert(field, serde_json::Value::from(value));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.insert(field, serde_json::Value::from(value));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.insert(field, serde_json::Value::String(value.to_owned()));
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.insert(field, serde_json::Value::String(value.to_string()));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.insert(field, serde_json::Value::String(format!("{value:?}")));
    }
}
