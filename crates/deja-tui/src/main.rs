use regex::Regex;
use std::cmp::min;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{self, Stdout};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use crossterm::event::{self, Event, KeyCode, KeyEvent};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use deja_core::ExecutionGraphNode;
use deja_runtime::BoundaryEvent;
use deja_tui::{
    boundary_substitution_counts, build_diff_rows, graph_record_has_error, graph_record_text,
    graph_records_for_request, graph_request_id, load_artifacts, novel_observed_calls,
    rank_histogram, request_outcomes, semantic_event_request_id, semantic_event_text,
    substitution_status, summarize, unique_boundaries, BoundaryStat, DiffKind, DiffRow, FieldDiff,
    JsonlStats, LoadedArtifacts, RequestOutcome, Scorecard, Substitution, Summary,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, Borders, Cell, Clear, Paragraph, Row, Table, TableState, Tabs, Wrap,
};
use ratatui::{Frame, Terminal};

const HELP: &str = "q quit | 1-7/tab switch tab | / search | b boundary | r request(regex) | e errors | g graph-visual | up/down select | pgup/pgdn scroll detail | enter drill in";
const USAGE: &str = "USAGE:
    deja-tui [--summary] <ARTIFACT_PATH>

ARTIFACT_PATH may be a run dir (recording + observed/ + runs/ + http-diffs/)
or a parent of run dirs (e.g. demo/harness-state) — the newest run is picked.

EXAMPLES:
    cargo run -p deja-tui -- demo/harness-state
    cargo run -p deja-tui -- --summary demo/harness-state/<run-tag>
";

fn main() -> Result<()> {
    let args = Args::parse(std::env::args().skip(1).collect())?;
    let artifacts = load_artifacts(&args.path)?;
    if args.summary {
        print_summary(&artifacts);
        return Ok(());
    }

    run_tui(artifacts)
}

#[derive(Debug)]
struct Args {
    path: PathBuf,
    summary: bool,
}

impl Args {
    fn parse(raw: Vec<String>) -> Result<Self> {
        if raw.iter().any(|arg| arg == "-h" || arg == "--help") {
            println!("{USAGE}");
            std::process::exit(0);
        }

        let mut summary = false;
        let mut path = None;
        for arg in raw {
            match arg.as_str() {
                "--summary" => summary = true,
                flag if flag.starts_with('-') => bail!("unknown option: {flag}\n\n{USAGE}"),
                _ => {
                    if path.replace(PathBuf::from(arg)).is_some() {
                        bail!("expected exactly one artifact path\n\n{USAGE}");
                    }
                }
            }
        }

        let path = path.context(USAGE)?;
        Ok(Self { path, summary })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tab {
    Run,
    Requests,
    Timeline,
    Semantic,
    Http,
    Divergences,
    Graph,
}

impl Tab {
    const ALL: [Self; 7] = [
        Self::Run,
        Self::Requests,
        Self::Timeline,
        Self::Semantic,
        Self::Http,
        Self::Divergences,
        Self::Graph,
    ];

    fn title(self) -> &'static str {
        match self {
            Self::Run => "1 Run",
            Self::Requests => "2 Requests",
            Self::Timeline => "3 Timeline",
            Self::Semantic => "4 Events",
            Self::Http => "5 HTTP Diff",
            Self::Divergences => "6 Divergences",
            Self::Graph => "7 Graph",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InputMode {
    Normal,
    Search,
    Boundary,
    Request,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestFocus {
    Requests,
    Tree,
}

/// Which pane has focus in the Divergences tab.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DivFocus {
    Selector, // the request list (top) is being navigated
    Diff,     // the split diff body is being navigated
}

/// Divergences diff layout: side-by-side or unified single column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DivView {
    Split,
    Inline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ViewMode {
    Normal,
    GraphVisual,
}

#[derive(Debug, Clone)]
struct GraphNode {
    node_id: u64,
    label: String,
    depth: usize,
    level: String,
    #[allow(dead_code)]
    children: Vec<usize>,
    #[allow(dead_code)]
    parent: Option<usize>,
    record_index: usize,
}

#[derive(Debug, Clone)]
struct RequestTreeItem {
    label: String,
    kind: RequestTreeItemKind,
}

#[derive(Debug, Clone, Copy)]
enum RequestTreeItemKind {
    Header,
    Semantic(usize),
    Graph(usize),
}

struct App {
    artifacts: LoadedArtifacts,
    summary: Summary,
    substitution: HashMap<u64, Substitution>,
    boundary_substitution: Vec<(String, usize, usize)>,
    request_rows: Vec<RequestOutcome>,
    tab_index: usize,
    input_mode: InputMode,
    input_buffer: String,
    search: String,
    boundary_filter: String,
    request_filter: String,
    error_only: bool,
    request_focus: RequestFocus,
    semantic_table: TableState,
    graph_table: TableState,
    request_table: TableState,
    tree_table: TableState,
    replay_table: TableState,
    http_table: TableState,
    divergence_table: TableState,
    // Record↔replay split diff state for the selected request.
    diff_rows: Vec<DiffRow>,
    diff_cursor: usize, // shared logical-row cursor
    diff_scroll: u16,   // shared vertical scroll for BOTH panes (lockstep)
    diff_expanded: std::collections::HashSet<usize>,
    diff_focus: DivFocus,
    div_view: DivView,
    // Shared scroll offset for the non-Divergences detail panes (Run "Recording",
    // Requests "Selected Record", HTTP "Body Diff Detail"). PgUp/PgDn drives it.
    detail_scroll: u16,
    view_mode: ViewMode,
    graph_nodes: Vec<GraphNode>,
    graph_selected: usize,
    graph_detail_open: bool,
    graph_detail_scroll: u16,
}

impl App {
    fn new(artifacts: LoadedArtifacts) -> Self {
        let summary = summarize(&artifacts);
        let (substitution, boundary_substitution) = match &artifacts.replay {
            Some(replay) => (
                substitution_status(&artifacts.semantic_events, replay),
                boundary_substitution_counts(&artifacts.semantic_events, replay),
            ),
            None => (HashMap::new(), Vec::new()),
        };
        let mut semantic_table = TableState::default();
        semantic_table.select(Some(0));
        let mut graph_table = TableState::default();
        graph_table.select(Some(0));
        let mut request_table = TableState::default();
        request_table.select(Some(0));
        let mut tree_table = TableState::default();
        tree_table.select(Some(0));
        let mut replay_table = TableState::default();
        replay_table.select(Some(0));
        let mut http_table = TableState::default();
        http_table.select(Some(0));
        let mut divergence_table = TableState::default();
        divergence_table.select(Some(0));
        let request_rows = request_outcomes(&artifacts);

        let mut app = Self {
            artifacts,
            summary,
            substitution,
            boundary_substitution,
            request_rows,
            tab_index: 0,
            input_mode: InputMode::Normal,
            input_buffer: String::new(),
            search: String::new(),
            boundary_filter: String::new(),
            request_filter: String::new(),
            error_only: false,
            request_focus: RequestFocus::Requests,
            semantic_table,
            graph_table,
            request_table,
            divergence_table,
            diff_rows: Vec::new(),
            diff_cursor: 0,
            diff_scroll: 0,
            diff_expanded: std::collections::HashSet::new(),
            diff_focus: DivFocus::Selector,
            div_view: DivView::Split,
            detail_scroll: 0,
            tree_table,
            replay_table,
            http_table,
            view_mode: ViewMode::Normal,
            graph_nodes: Vec::new(),
            graph_selected: 0,
            graph_detail_open: false,
            graph_detail_scroll: 0,
        };
        app.rebuild_graph_nodes();
        app.sync_diff_rows();
        app
    }

    /// Recorded events sorted by `global_sequence`, honoring the active
    /// search/boundary/request/error filters (the Replay timeline view).
    fn replay_timeline_events(&self) -> Vec<&BoundaryEvent> {
        let mut events = self.filtered_semantic_events();
        events.sort_by_key(|event| event.global_sequence);
        events
    }

    fn active_tab(&self) -> Tab {
        Tab::ALL[self.tab_index]
    }

    fn next_tab(&mut self) {
        self.detail_scroll = 0;
        self.tab_index = (self.tab_index + 1) % Tab::ALL.len();
        self.repair_selection();
    }

    fn request_filter_matches(&self, id: &str) -> bool {
        if self.request_filter.is_empty() {
            return true;
        }
        Regex::new(&self.request_filter)
            .map(|re| re.is_match(id))
            .unwrap_or_else(|_| *id == self.request_filter)
    }

    fn filtered_semantic_events(&self) -> Vec<&BoundaryEvent> {
        let search = self.search.to_ascii_lowercase();
        let boundary = self.boundary_filter.to_ascii_lowercase();
        self.artifacts
            .semantic_events
            .iter()
            .filter(|event| {
                (search.is_empty()
                    || semantic_event_text(event)
                        .to_ascii_lowercase()
                        .contains(search.as_str()))
                    && (boundary.is_empty()
                        || event
                            .boundary
                            .to_ascii_lowercase()
                            .contains(boundary.as_str()))
                    && (self.request_filter.is_empty()
                        || semantic_event_request_id(event)
                            .map(|id| self.request_filter_matches(id))
                            .unwrap_or(false))
                    && (!self.error_only || event.is_error)
            })
            .collect()
    }

    fn filtered_graph_records(&self) -> Vec<&ExecutionGraphNode> {
        let search = self.search.to_ascii_lowercase();
        self.artifacts
            .graph_records
            .iter()
            .filter(|record| {
                (search.is_empty()
                    || graph_record_text(record)
                        .to_ascii_lowercase()
                        .contains(search.as_str()))
                    && (self.request_filter.is_empty()
                        || graph_request_id(record)
                            .map(|id| self.request_filter_matches(id))
                            .unwrap_or(false))
                    && (!self.error_only || graph_record_has_error(record))
            })
            .collect()
    }

    /// Owned copy of the filtered rows, for render paths that also need
    /// `&mut` access to a `TableState` (the request set is tiny).
    fn filtered_request_rows_owned(&self) -> Vec<RequestOutcome> {
        self.filtered_request_rows().into_iter().cloned().collect()
    }

    /// Driven requests in recorded order, honoring search/request filters
    /// (search matches the correlation id, method or path).
    fn filtered_request_rows(&self) -> Vec<&RequestOutcome> {
        let search = self.search.to_ascii_lowercase();
        self.request_rows
            .iter()
            .filter(|row| {
                (search.is_empty()
                    || row
                        .correlation_id
                        .to_ascii_lowercase()
                        .contains(search.as_str())
                    || row.path.to_ascii_lowercase().contains(search.as_str())
                    || row.method.to_ascii_lowercase().contains(search.as_str()))
                    && (self.request_filter.is_empty()
                        || self.request_filter_matches(&row.correlation_id))
            })
            .collect()
    }

    fn selected_semantic(&self) -> Option<&BoundaryEvent> {
        let events = self.filtered_semantic_events();
        self.semantic_table
            .selected()
            .and_then(|index| events.get(index).copied())
    }

    fn selected_replay_event(&self) -> Option<&BoundaryEvent> {
        let events = self.replay_timeline_events();
        self.replay_table
            .selected()
            .and_then(|index| events.get(index).copied())
    }

    fn selected_graph(&self) -> Option<&ExecutionGraphNode> {
        let records = self.filtered_graph_records();
        self.graph_table
            .selected()
            .and_then(|index| records.get(index).copied())
    }

    fn selected_request_id(&self) -> Option<String> {
        let requests = self.filtered_request_rows();
        self.request_table
            .selected()
            .and_then(|index| requests.get(index).map(|row| row.correlation_id.clone()))
    }

    fn selected_http_diff_row(&self) -> Option<&RequestOutcome> {
        let rows = self.filtered_request_rows();
        self.http_table
            .selected()
            .and_then(|index| rows.get(index).copied())
    }

    /// Requests that diverged on replay: an HTTP status/body mismatch, or a
    /// side-effect divergence (omitted/novel call), or an overall fail.
    fn diverged_request_rows(&self) -> Vec<RequestOutcome> {
        self.filtered_request_rows_owned()
            .into_iter()
            .filter(request_has_divergence)
            .collect()
    }

    fn selected_divergence_row(&self) -> Option<RequestOutcome> {
        let rows = self.diverged_request_rows();
        self.divergence_table
            .selected()
            .and_then(|index| rows.get(index).cloned())
    }

    /// Rebuild the split-diff rows for the currently-selected diverging request.
    /// Called whenever the selection (or the diverged set) changes.
    fn sync_diff_rows(&mut self) {
        // Pull the correlation + its http diff into locals first to avoid borrow
        // conflicts while we mutate self.
        let row = self.selected_divergence_row();
        self.diff_rows = match row {
            Some(r) => build_diff_rows(
                &self.artifacts.semantic_events,
                self.artifacts.replay.as_ref(),
                r.http_diff.as_ref(),
                &r.correlation_id,
            ),
            None => Vec::new(),
        };
        self.diff_cursor = 0;
        self.diff_scroll = 0;
        self.diff_expanded.clear();
        // Land the cursor on the first actual divergence (skip leading Matched).
        if let Some(idx) = self.diff_rows.iter().position(DiffRow::is_divergence) {
            self.diff_cursor = idx;
        }
    }

    /// Move the diff cursor to the next/previous divergence row (skipping Matched).
    fn jump_divergence(&mut self, forward: bool) {
        if self.diff_rows.is_empty() {
            return;
        }
        let n = self.diff_rows.len();
        let mut i = self.diff_cursor;
        for _ in 0..n {
            i = if forward {
                (i + 1) % n
            } else {
                (i + n - 1) % n
            };
            if self.diff_rows[i].is_divergence() {
                self.diff_cursor = i;
                return;
            }
        }
    }

    fn current_request_id(&self) -> Option<String> {
        self.selected_request_id()
            .or_else(|| (!self.request_filter.is_empty()).then(|| self.request_filter.clone()))
    }

    fn filtered_request_tree(&self, request_id: &str) -> Vec<RequestTreeItem> {
        let mut items = Vec::new();
        let semantic_events = self
            .artifacts
            .semantic_events
            .iter()
            .enumerate()
            .filter(|(_, event)| semantic_event_request_id(event) == Some(request_id))
            .filter(|(_, event)| self.semantic_event_visible(event))
            .collect::<Vec<_>>();
        let graph_records = graph_records_for_request(&self.artifacts.graph_records, request_id)
            .into_iter()
            .filter(|record| self.graph_record_visible(record))
            .collect::<Vec<_>>();

        items.push(RequestTreeItem {
            label: format!("request {request_id}"),
            kind: RequestTreeItemKind::Header,
        });
        items.push(RequestTreeItem {
            label: format!(
                "  semantic events: {}  graph spans: {}",
                semantic_events.len(),
                graph_records.len()
            ),
            kind: RequestTreeItemKind::Header,
        });
        append_semantic_tree_items(&mut items, semantic_events);
        append_graph_tree_items(&mut items, &self.artifacts.graph_records, graph_records);
        items
    }

    fn selected_tree_item(&self) -> Option<RequestTreeItem> {
        let request_id = self.current_request_id()?;
        let items = self.filtered_request_tree(&request_id);
        self.tree_table
            .selected()
            .and_then(|index| items.get(index).cloned())
    }

    fn semantic_event_visible(&self, event: &BoundaryEvent) -> bool {
        let search = self.search.to_ascii_lowercase();
        let boundary = self.boundary_filter.to_ascii_lowercase();
        (search.is_empty()
            || semantic_event_text(event)
                .to_ascii_lowercase()
                .contains(search.as_str()))
            && (boundary.is_empty()
                || event
                    .boundary
                    .to_ascii_lowercase()
                    .contains(boundary.as_str()))
            && (!self.error_only || event.is_error)
    }

    fn graph_record_visible(&self, record: &ExecutionGraphNode) -> bool {
        let search = self.search.to_ascii_lowercase();
        (search.is_empty()
            || graph_record_text(record)
                .to_ascii_lowercase()
                .contains(search.as_str()))
            && (!self.error_only || graph_record_has_error(record))
    }

    fn move_selection(&mut self, down: bool) {
        self.detail_scroll = 0; // new selection → detail back to top
        match self.active_tab() {
            Tab::Semantic => {
                let len = self.filtered_semantic_events().len();
                move_table_selection(&mut self.semantic_table, len, down);
            }
            Tab::Timeline => {
                let len = self.replay_timeline_events().len();
                move_table_selection(&mut self.replay_table, len, down);
            }
            Tab::Graph => {
                let len = self.filtered_graph_records().len();
                move_table_selection(&mut self.graph_table, len, down);
            }
            Tab::Http => {
                let len = self.filtered_request_rows().len();
                move_table_selection(&mut self.http_table, len, down);
            }
            Tab::Divergences => {
                // Generic up/down moves the request selector; rebuild its diff.
                let len = self.diverged_request_rows().len();
                move_table_selection(&mut self.divergence_table, len, down);
                self.sync_diff_rows();
            }
            Tab::Requests => {
                if self.request_focus == RequestFocus::Requests {
                    let len = self.filtered_request_rows().len();
                    move_table_selection(&mut self.request_table, len, down);
                    self.tree_table.select(Some(0));
                } else {
                    let len = self
                        .current_request_id()
                        .map(|request_id| self.filtered_request_tree(&request_id).len())
                        .unwrap_or(0);
                    move_table_selection(&mut self.tree_table, len, down);
                }
            }
            Tab::Run => {}
        }
    }

    fn drilldown(&mut self) {
        let request = match self.active_tab() {
            Tab::Semantic => self
                .selected_semantic()
                .and_then(semantic_event_request_id)
                .map(str::to_owned),
            Tab::Timeline => self
                .selected_replay_event()
                .and_then(semantic_event_request_id)
                .map(str::to_owned),
            Tab::Graph => self
                .selected_graph()
                .and_then(graph_request_id)
                .map(str::to_owned),
            Tab::Http => self
                .selected_http_diff_row()
                .map(|row| row.correlation_id.clone()),
            Tab::Divergences => self
                .selected_divergence_row()
                .map(|row| row.correlation_id.clone()),
            Tab::Requests | Tab::Run => self.selected_request_id(),
        };

        if let Some(request) = request {
            self.request_filter = request;
            self.request_focus = RequestFocus::Tree;
            self.tab_index = Tab::ALL
                .iter()
                .position(|tab| *tab == Tab::Requests)
                .unwrap_or(self.tab_index);
            self.repair_selection();
        }
    }

    fn start_input(&mut self, mode: InputMode) {
        self.input_mode = mode;
        self.input_buffer = match mode {
            InputMode::Search => self.search.clone(),
            InputMode::Boundary => self.boundary_filter.clone(),
            InputMode::Request => self.request_filter.clone(),
            InputMode::Normal => String::new(),
        };
    }

    fn commit_input(&mut self) {
        match self.input_mode {
            InputMode::Search => self.search = self.input_buffer.clone(),
            InputMode::Boundary => self.boundary_filter = self.input_buffer.clone(),
            InputMode::Request => self.request_filter = self.input_buffer.clone(),
            InputMode::Normal => {}
        }
        self.input_mode = InputMode::Normal;
        self.input_buffer.clear();
        self.repair_selection();
    }

    fn cancel_input(&mut self) {
        self.input_mode = InputMode::Normal;
        self.input_buffer.clear();
    }

    fn repair_selection(&mut self) {
        let semantic_len = self.filtered_semantic_events().len();
        let graph_len = self.filtered_graph_records().len();
        let request_len = self.filtered_request_rows().len();
        let tree_len = self
            .current_request_id()
            .map(|request_id| self.filtered_request_tree(&request_id).len())
            .unwrap_or(0);
        let replay_len = self.replay_timeline_events().len();
        repair_table_selection(&mut self.semantic_table, semantic_len);
        repair_table_selection(&mut self.graph_table, graph_len);
        repair_table_selection(&mut self.request_table, request_len);
        repair_table_selection(&mut self.tree_table, tree_len);
        repair_table_selection(&mut self.replay_table, replay_len);
        repair_table_selection(&mut self.http_table, request_len);
        let diverged_len = self.diverged_request_rows().len();
        repair_table_selection(&mut self.divergence_table, diverged_len);
        self.sync_diff_rows();
        // Also repair graph visual selection
        if !self.graph_nodes.is_empty() {
            self.graph_selected = self.graph_selected.min(self.graph_nodes.len() - 1);
        }
    }

    fn rebuild_graph_nodes(&mut self) {
        self.graph_nodes = self.build_graph_tree();
    }

    fn build_graph_tree(&self) -> Vec<GraphNode> {
        let records: Vec<&ExecutionGraphNode> = self.artifacts.graph_records.iter().collect();
        if records.is_empty() {
            return Vec::new();
        }
        let in_request: HashSet<u64> = records.iter().map(|r| r.node_id).collect();

        // Map node_id -> (original artifact index, record ref)
        let mut id_to_info: HashMap<u64, (usize, &ExecutionGraphNode)> = HashMap::new();
        let mut children: HashMap<Option<u64>, Vec<u64>> = HashMap::new();

        for (art_idx, record) in self.artifacts.graph_records.iter().enumerate() {
            id_to_info.insert(record.node_id, (art_idx, record));
            children
                .entry(record.parent_id)
                .or_default()
                .push(record.node_id);
        }

        // Sort children by sequence (but we'll filter by in_request later)
        for child_ids in children.values_mut() {
            child_ids.sort_by_key(|id| id_to_info.get(id).map(|(_, r)| r.sequence).unwrap_or(0));
        }

        // Find roots: parent is None OR parent is NOT in the current request's records
        let mut root_ids: Vec<u64> = records
            .iter()
            .filter(|r| {
                r.parent_id.is_none()
                    || r.parent_id
                        .map(|pid| !in_request.contains(&pid))
                        .unwrap_or(true)
            })
            .map(|r| r.node_id)
            .collect();
        // If no roots found (e.g. all records have parents inside the set),
        // treat every record as a disconnected root so we still render something.
        if root_ids.is_empty() {
            root_ids = records.iter().map(|r| r.node_id).collect();
        }
        root_ids.sort_by_key(|id| id_to_info.get(id).map(|(_, r)| r.sequence).unwrap_or(0));

        let mut result = Vec::new();
        let mut emitted = 0;
        for root_id in root_ids {
            if emitted >= 200 {
                break;
            }
            self.build_graph_subtree(
                &id_to_info,
                &children,
                &in_request,
                root_id,
                0,
                &mut result,
                &mut emitted,
            );
        }
        result
    }

    #[allow(clippy::too_many_arguments)] // recursive tree renderer threads its display state
    #[allow(clippy::only_used_in_recursion)]
    fn build_graph_subtree(
        &self,
        id_to_info: &HashMap<u64, (usize, &ExecutionGraphNode)>,
        children: &HashMap<Option<u64>, Vec<u64>>,
        in_request: &HashSet<u64>,
        node_id: u64,
        depth: usize,
        result: &mut Vec<GraphNode>,
        emitted: &mut usize,
    ) {
        if *emitted >= 200 {
            return;
        }
        let Some(&(art_idx, record)) = id_to_info.get(&node_id) else {
            return;
        };
        let node = record;

        let level_icon = match node.level.to_ascii_lowercase().as_str() {
            "error" => "[ERR]",
            "warn" => "[WRN]",
            "debug" => "[DBG]",
            "trace" => "[TRC]",
            _ => "[INF]",
        };

        let label = format!(
            "#{:>5}.{:>3} {} {}",
            node.sequence,
            node.node_id,
            level_icon,
            short(&node.span_name, 32),
        );

        let parent = node.parent_id.and_then(|pid| {
            id_to_info
                .get(&pid)
                .and_then(|_| result.iter().position(|gn| gn.node_id == pid))
        });

        let child_ids: Vec<usize> = children
            .get(&Some(node_id))
            .map(|ids| {
                ids.iter()
                    .filter_map(|cid| {
                        if in_request.contains(cid) {
                            Some(*cid as usize)
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        result.push(GraphNode {
            node_id,
            label,
            depth,
            level: node.level.clone(),
            children: child_ids,
            parent,
            record_index: art_idx,
        });
        *emitted += 1;

        if let Some(all_children) = children.get(&Some(node_id)) {
            for &child_id in all_children.iter().filter(|c| in_request.contains(c)) {
                if *emitted >= 200 {
                    break;
                }
                self.build_graph_subtree(
                    id_to_info,
                    children,
                    in_request,
                    child_id,
                    depth + 1,
                    result,
                    emitted,
                );
            }
        }
    }

    fn selected_graph_record(&self) -> Option<&ExecutionGraphNode> {
        self.graph_nodes
            .get(self.graph_selected)
            .and_then(|node| self.artifacts.graph_records.get(node.record_index))
    }

    fn move_graph_selection(&mut self, down: bool) {
        if self.graph_nodes.is_empty() {
            return;
        }
        if down {
            self.graph_selected = (self.graph_selected + 1).min(self.graph_nodes.len() - 1);
        } else {
            self.graph_selected = self.graph_selected.saturating_sub(1);
        }
    }
}

fn run_tui(artifacts: LoadedArtifacts) -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    let result = run_app(&mut terminal, App::new(artifacts));
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    result
}

fn run_app(terminal: &mut Terminal<CrosstermBackend<Stdout>>, mut app: App) -> Result<()> {
    loop {
        terminal.draw(|frame| render(frame, &mut app))?;
        if event::poll(Duration::from_millis(200))? {
            if let Event::Key(key) = event::read()? {
                if handle_key(&mut app, key) {
                    break;
                }
            }
        }
    }
    Ok(())
}

fn handle_key(app: &mut App, key: KeyEvent) -> bool {
    if app.input_mode != InputMode::Normal {
        match key.code {
            KeyCode::Esc => app.cancel_input(),
            KeyCode::Enter => app.commit_input(),
            KeyCode::Backspace => {
                app.input_buffer.pop();
            }
            KeyCode::Char(ch) => app.input_buffer.push(ch),
            _ => {}
        }
        return false;
    }

    // Graph detail popup: scroll with ↑↓, close with esc/enter/q
    if app.graph_detail_open {
        match key.code {
            KeyCode::Esc | KeyCode::Enter | KeyCode::Char('q') => {
                app.graph_detail_open = false;
                app.graph_detail_scroll = 0;
                false
            }
            KeyCode::Up => {
                app.graph_detail_scroll = app.graph_detail_scroll.saturating_sub(1);
                false
            }
            KeyCode::Down => {
                app.graph_detail_scroll = app.graph_detail_scroll.saturating_add(1);
                false
            }
            _ => false,
        }
    }
    // Graph visual mode intercepts navigation keys
    else if app.view_mode == ViewMode::GraphVisual {
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => {
                app.view_mode = ViewMode::Normal;
                false
            }
            KeyCode::Down => {
                app.move_graph_selection(true);
                false
            }
            KeyCode::Up => {
                app.move_graph_selection(false);
                false
            }
            KeyCode::Enter => {
                app.graph_detail_open = true;
                false
            }
            _ => false,
        }
    } else {
        match key.code {
            KeyCode::Char('q') => true,
            KeyCode::Char('/') => {
                app.start_input(InputMode::Search);
                false
            }
            KeyCode::Char('b') => {
                app.start_input(InputMode::Boundary);
                false
            }
            KeyCode::Char('r') => {
                app.start_input(InputMode::Request);
                false
            }
            KeyCode::Char('e') => {
                app.error_only = !app.error_only;
                app.repair_selection();
                false
            }
            KeyCode::Char('g') | KeyCode::Char('G') => {
                app.rebuild_graph_nodes();
                app.view_mode = ViewMode::GraphVisual;
                app.graph_selected = 0;
                false
            }
            KeyCode::Tab => {
                app.next_tab();
                false
            }
            KeyCode::Char(digit @ '1'..='9') => {
                app.detail_scroll = 0;
                app.tab_index = (digit as usize - '1' as usize).min(Tab::ALL.len() - 1);
                app.repair_selection();
                false
            }
            // ── Divergences tab: split-diff navigation (gated, before the generic arms) ──
            KeyCode::Char('j') | KeyCode::Down if app.active_tab() == Tab::Divergences => {
                if app.diff_focus == DivFocus::Selector {
                    app.move_selection(true); // moves selector + rebuilds diff
                } else if !app.diff_rows.is_empty() {
                    app.diff_cursor = (app.diff_cursor + 1).min(app.diff_rows.len() - 1);
                }
                false
            }
            KeyCode::Char('k') | KeyCode::Up if app.active_tab() == Tab::Divergences => {
                if app.diff_focus == DivFocus::Selector {
                    app.move_selection(false);
                } else {
                    app.diff_cursor = app.diff_cursor.saturating_sub(1);
                }
                false
            }
            KeyCode::Enter if app.active_tab() == Tab::Divergences => {
                if app.diff_focus == DivFocus::Selector {
                    app.diff_focus = DivFocus::Diff; // dive into the diff
                } else if app.diff_cursor < app.diff_rows.len() {
                    // toggle expand on the cursor row
                    if !app.diff_expanded.remove(&app.diff_cursor) {
                        app.diff_expanded.insert(app.diff_cursor);
                    }
                }
                false
            }
            KeyCode::Esc
                if app.active_tab() == Tab::Divergences && app.diff_focus == DivFocus::Diff =>
            {
                app.diff_focus = DivFocus::Selector;
                false
            }
            KeyCode::Char('[') if app.active_tab() == Tab::Divergences => {
                let len = app.diverged_request_rows().len();
                move_table_selection(&mut app.divergence_table, len, false);
                app.sync_diff_rows();
                false
            }
            KeyCode::Char(']') if app.active_tab() == Tab::Divergences => {
                let len = app.diverged_request_rows().len();
                move_table_selection(&mut app.divergence_table, len, true);
                app.sync_diff_rows();
                false
            }
            KeyCode::Char('n') if app.active_tab() == Tab::Divergences => {
                app.diff_focus = DivFocus::Diff;
                app.jump_divergence(true);
                false
            }
            KeyCode::Char('N') if app.active_tab() == Tab::Divergences => {
                app.diff_focus = DivFocus::Diff;
                app.jump_divergence(false);
                false
            }
            KeyCode::Char('s') if app.active_tab() == Tab::Divergences => {
                app.div_view = match app.div_view {
                    DivView::Split => DivView::Inline,
                    DivView::Inline => DivView::Split,
                };
                false
            }
            KeyCode::PageDown if app.active_tab() == Tab::Divergences => {
                app.diff_scroll = app.diff_scroll.saturating_add(8);
                false
            }
            KeyCode::PageUp if app.active_tab() == Tab::Divergences => {
                app.diff_scroll = app.diff_scroll.saturating_sub(8);
                false
            }
            // General detail-pane scroll for the other tabs (Run "Recording",
            // Requests "Selected Record", HTTP "Body Diff Detail").
            KeyCode::PageDown => {
                app.detail_scroll = app.detail_scroll.saturating_add(4);
                false
            }
            KeyCode::PageUp => {
                app.detail_scroll = app.detail_scroll.saturating_sub(4);
                false
            }
            KeyCode::Right => {
                if app.active_tab() == Tab::Requests {
                    app.request_focus = RequestFocus::Tree;
                }
                false
            }
            KeyCode::Left => {
                if app.active_tab() == Tab::Requests {
                    app.request_focus = RequestFocus::Requests;
                }
                false
            }
            KeyCode::Down => {
                app.move_selection(true);
                false
            }
            KeyCode::Up => {
                app.move_selection(false);
                false
            }
            KeyCode::Enter => {
                if app.active_tab() == Tab::Requests {
                    app.request_focus = RequestFocus::Tree;
                } else {
                    app.drilldown();
                }
                false
            }
            _ => false,
        }
    }
}

fn render(frame: &mut Frame<'_>, app: &mut App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Min(8),
            Constraint::Length(2),
        ])
        .split(frame.size());

    render_header(frame, chunks[0], app);
    render_tabs(frame, chunks[1], app);
    match app.active_tab() {
        Tab::Run => render_run(frame, chunks[2], app),
        Tab::Requests => render_requests(frame, chunks[2], app),
        Tab::Timeline => render_timeline(frame, chunks[2], app),
        Tab::Semantic => render_semantic(frame, chunks[2], app),
        Tab::Http => render_http(frame, chunks[2], app),
        Tab::Divergences => render_divergences(frame, chunks[2], app),
        Tab::Graph => render_graph(frame, chunks[2], app),
    }
    render_footer(frame, chunks[3], app);

    if app.input_mode != InputMode::Normal {
        render_input(frame, app);
    }

    if app.view_mode == ViewMode::GraphVisual {
        render_graph_visual(frame, app);
        if app.graph_detail_open {
            render_graph_detail_popup(frame, app);
        }
    }
}

fn render_header(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let semantic_count = app.artifacts.semantic_events.len();
    let graph_count = app.artifacts.graph_records.len();
    let filter = format!(
        "search='{}' boundary='{}' request='{}' errors={}",
        blank_filter(&app.search),
        blank_filter(&app.boundary_filter),
        blank_filter(&app.request_filter),
        if app.error_only { "on" } else { "off" }
    );
    let line = Line::from(vec![
        Span::styled(
            "Deja/Hyperswitch Artifacts",
            Style::default().fg(Color::Cyan),
        ),
        Span::raw(format!(
            "  semantic={} graph={}  {}",
            semantic_count, graph_count, filter
        )),
    ]);
    frame.render_widget(
        Paragraph::new(line).block(
            Block::default()
                .borders(Borders::ALL)
                .title(app.artifacts.paths.root.display().to_string()),
        ),
        area,
    );
}

fn render_tabs(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let titles = Tab::ALL
        .iter()
        .map(|tab| Line::from(tab.title()))
        .collect::<Vec<_>>();
    frame.render_widget(
        Tabs::new(titles)
            .select(app.tab_index)
            .style(Style::default().fg(Color::Gray))
            .highlight_style(
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            )
            .block(Block::default().borders(Borders::ALL)),
        area,
    );
}

/// The "Run" dashboard: verdict banner, divergence summary, per-boundary
/// substitution bars, the address-rank histogram and recording metadata.
fn render_run(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let scorecard = app
        .artifacts
        .replay
        .as_ref()
        .and_then(|replay| replay.scorecard.as_ref());

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(2),
            Constraint::Min(8),
        ])
        .split(area);

    render_replay_verdict(frame, chunks[0], scorecard);
    render_replay_summary(frame, chunks[1], scorecard);

    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(42),
            Constraint::Percentage(29),
            Constraint::Percentage(29),
        ])
        .split(chunks[2]);

    render_replay_boundaries(frame, columns[0], app);
    render_rank_histogram(frame, columns[1], scorecard);
    frame.render_widget(
        Paragraph::new(run_meta_text(app))
            .block(panel("Recording  ·  PgUp/PgDn scroll"))
            .wrap(Wrap { trim: false })
            .scroll((app.detail_scroll, 0)),
        columns[2],
    );
}

/// How and how strongly replay matched calls to the recording: rank_1
/// explicit … rank_6 sequence-of-last-resort. A ranks-1–4-heavy histogram
/// (content/identity: logical-context, syntax-hash, lexical-path) means matching
/// is content-addressed and robust to reordering.
fn render_rank_histogram(frame: &mut Frame<'_>, area: Rect, scorecard: Option<&Scorecard>) {
    let block = panel("Match Strength (resolved_by_rank)");
    let Some(card) = scorecard else {
        frame.render_widget(Paragraph::new("no scorecard").block(block), area);
        return;
    };
    let histogram = rank_histogram(card);
    if histogram.is_empty() {
        frame.render_widget(Paragraph::new("no resolved calls").block(block), area);
        return;
    }
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let max = histogram.iter().map(|(_, count)| *count).max().unwrap_or(1);
    let bar_width = (inner.width as usize).saturating_sub(22).max(8);
    let mut lines = Vec::new();
    for (rank, count) in &histogram {
        let filled = (*count as usize).saturating_mul(bar_width) / max as usize;
        // Post-P3 rank ladder: 1 Explicit · 2 SpanPath · 3 SyntacticHash ·
        // 4 LexicalPath (all version-stable content/identity) · 5 SourceLocation
        // (location) · 6 Sequence (positional/fragile).
        let color = match rank.as_str() {
            "rank_1" | "rank_2" | "rank_3" | "rank_4" => Color::Green,
            "rank_5" => Color::Cyan,
            _ => Color::Yellow,
        };
        lines.push(Line::from(vec![
            Span::styled(format!("{:<8}", rank), Style::default().fg(color)),
            Span::styled(format!("{:>5} ", count), Style::default().fg(Color::White)),
            Span::styled("█".repeat(filled.max(1)), Style::default().fg(color)),
        ]));
    }
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "rank 1-4 content/identity · 5 location · 6 sequence",
        Style::default().fg(Color::DarkGray),
    )));
    frame.render_widget(Paragraph::new(lines), inner);
}

fn run_meta_text(app: &App) -> String {
    let mut out = overview_text(app);
    if let Some(replay) = &app.artifacts.replay {
        if let Some(card) = &replay.scorecard {
            out.push_str(&format!(
                "\n\nrun: {}\nrecording: {}",
                card.run_id,
                card.recording_id.as_deref().unwrap_or("-"),
            ));
            if card.summary.uncorrelated_events_seen > 0 {
                out.push_str(&format!(
                    "\nuncorrelated events: {} ({})",
                    card.summary.uncorrelated_events_seen,
                    if card.summary.uncorrelated_events_tolerated {
                        "tolerated"
                    } else {
                        "blocking"
                    }
                ));
            }
            for warning in &card.warnings {
                out.push_str(&format!("\n⚠ {warning}"));
            }
        }
        out.push_str(&format!("\nobserved calls: {}", replay.observed.len()));
        out.push_str(&format!("\nhttp comparisons: {}", replay.http_diffs.len()));
    }
    out
}

fn render_semantic(frame: &mut Frame<'_>, area: Rect, app: &mut App) {
    let events = app.filtered_semantic_events();
    let title = format!("Semantic Events ({})", events.len());
    let rows = events.iter().map(|event| {
        Row::new(vec![
            Cell::from(event.global_sequence.to_string()),
            Cell::from(short(event.correlation_id.as_deref().unwrap_or("-"), 16)),
            Cell::from(event.boundary.clone()),
            Cell::from(short(
                &format!("{}::{}", event.trait_name, event.method_name),
                42,
            )),
            Cell::from(format!("{}us", event.duration_us)),
            Cell::from(
                event
                    .graph_node_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "-".to_owned()),
            ),
            Cell::from(if event.is_error { "yes" } else { "" }),
            Cell::from(short(
                &format!("{}:{}", event.call_file, event.call_line),
                44,
            )),
        ])
    });

    let table = Table::new(
        rows,
        [
            Constraint::Length(6),
            Constraint::Length(18),
            Constraint::Length(16),
            Constraint::Percentage(28),
            Constraint::Length(10),
            Constraint::Length(8),
            Constraint::Length(5),
            Constraint::Percentage(30),
        ],
    )
    .header(header_row([
        "seq",
        "request",
        "boundary",
        "operation",
        "duration",
        "graph",
        "err",
        "callsite",
    ]))
    .block(panel(&title))
    .highlight_style(Style::default().fg(Color::Black).bg(Color::Yellow));
    frame.render_stateful_widget(table, area, &mut app.semantic_table);
}

fn render_graph(frame: &mut Frame<'_>, area: Rect, app: &mut App) {
    let records = app.filtered_graph_records();
    let title = format!("Graph Spans ({})", records.len());
    let rows = records.iter().map(|record| {
        let node = record;
        Row::new(vec![
            Cell::from(node.sequence.to_string()),
            Cell::from(node.node_id.to_string()),
            Cell::from(short(graph_request_id(record).unwrap_or("-"), 16)),
            Cell::from(short(&node.span_name, 30)),
            Cell::from(short(&node.target, 28)),
            Cell::from(node.level.clone()),
            Cell::from(node.parent_id.map(|id| id.to_string()).unwrap_or_default()),
            Cell::from(duration_label(node.started_ns, node.closed_ns)),
        ])
    });

    let table = Table::new(
        rows,
        [
            Constraint::Length(6),
            Constraint::Length(6),
            Constraint::Length(18),
            Constraint::Percentage(24),
            Constraint::Percentage(24),
            Constraint::Length(7),
            Constraint::Length(7),
            Constraint::Length(10),
        ],
    )
    .header(header_row([
        "seq", "node", "request", "span", "target", "level", "parent", "dur",
    ]))
    .block(panel(&title))
    .highlight_style(Style::default().fg(Color::Black).bg(Color::Cyan));
    frame.render_stateful_widget(table, area, &mut app.graph_table);
}

fn render_graph_visual(frame: &mut Frame<'_>, app: &App) {
    let area = centered_rect(90, 80, frame.size());
    frame.render_widget(Clear, area);

    let title = "Graph Visual ──↑↓ navigate | enter detail | q/esc close──";
    let block = Block::default()
        .borders(Borders::ALL)
        .title(title.to_owned())
        .border_style(Style::default().fg(Color::Magenta));

    let inner = block.inner(area);
    frame.render_widget(block, area);

    let height = inner.height as usize;
    if height == 0 {
        return;
    }

    if app.graph_nodes.is_empty() {
        let msg = Paragraph::new("No graph records to display.\n\nPress q or esc to close.")
            .style(Style::default().fg(Color::Yellow));
        frame.render_widget(msg, inner);
        return;
    }

    // Compute viewport offset centered on selected node
    let selected = app.graph_selected;
    let row = selected.min(app.graph_nodes.len() - 1);
    let offset = if row + height / 2 < app.graph_nodes.len() {
        row.saturating_sub(height / 2)
    } else {
        app.graph_nodes.len().saturating_sub(height)
    };

    let visible_range = offset..(offset + height).min(app.graph_nodes.len());
    let mut lines: Vec<Line<'_>> = Vec::with_capacity(height);

    for idx in visible_range {
        let node = &app.graph_nodes[idx];
        let is_selected = idx == app.graph_selected;
        let node_color = if is_selected {
            Color::Black
        } else {
            match node.depth {
                0 => Color::Yellow,
                1 => Color::Cyan,
                _ => Color::White,
            }
        };
        let bg_color = if is_selected {
            Color::Yellow
        } else {
            Color::Reset
        };

        let style = Style::default().fg(node_color).bg(bg_color);
        let spans = build_graph_line_spans(node, style);
        lines.push(Line::from(spans));
    }

    // Pad remaining lines
    while lines.len() < height {
        lines.push(Line::from(""));
    }

    frame.render_widget(Paragraph::new(lines), inner);
}

fn build_graph_line_spans(node: &GraphNode, base_style: Style) -> Vec<Span<'_>> {
    let mut spans = Vec::new();

    // Connector drawing
    for d in 0..node.depth {
        let connector = if d == node.depth.saturating_sub(1) {
            "╰─ "
        } else {
            "│  "
        };
        spans.push(Span::styled(
            connector.to_string(),
            base_style.fg(Color::DarkGray),
        ));
    }

    // Level marker with color
    let (marker_fg, marker) = match node.level.to_ascii_lowercase().as_str() {
        "error" => (Color::Red, "●"),
        "warn" => (Color::Yellow, "◐"),
        "debug" => (Color::Blue, "◯"),
        "trace" => (Color::Magenta, "∙"),
        _ => (Color::Green, "◎"),
    };
    spans.push(Span::styled(format!("{marker} "), base_style.fg(marker_fg)));

    // Label body
    spans.push(Span::styled(node.label.clone(), base_style));

    spans
}

fn render_graph_detail_popup(frame: &mut Frame<'_>, app: &App) {
    let area = centered_rect(80, 70, frame.size());
    frame.render_widget(Clear, area);

    let detail = app
        .selected_graph_record()
        .map(|record| {
            let node = record;
            let pretty = serde_json::to_string_pretty(record)
                .unwrap_or_else(|e| format!("failed to render JSON: {e}"));
            let semantic_pay = semantic_payloads_for_graph_span(app, record);
            format!(
                "graph span detail\n\n\
                 span:      {}\n\
                 target:    {}\n\
                 level:     {}\n\
                 node_id:   {}\n\
                 parent_id: {}\n\
                 request:   {}\n\
                 duration:  {}\n\n\
                 linked semantic events:\n{}\n\n\
                 full record:\n{}",
                node.span_name,
                node.target,
                node.level,
                node.node_id,
                node.parent_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "-".to_owned()),
                graph_request_id(record).unwrap_or("-"),
                duration_label(node.started_ns, node.closed_ns),
                semantic_pay,
                pretty,
            )
        })
        .unwrap_or_else(|| "No graph node selected".to_owned());

    let paragraph = Paragraph::new(detail)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Magenta))
                .title("Node Detail ──↑↓ scroll | enter/esc/q close ──"),
        )
        .wrap(Wrap { trim: false })
        .scroll((app.graph_detail_scroll, 0));
    frame.render_widget(paragraph, area);
}

fn render_requests(frame: &mut Frame<'_>, area: Rect, app: &mut App) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(40),
            Constraint::Percentage(30),
            Constraint::Percentage(30),
        ])
        .split(area);
    let requests = app.filtered_request_rows_owned();
    let passed = requests
        .iter()
        .filter(|row| {
            row.outcome
                .as_ref()
                .map(|outcome| outcome.passed)
                .unwrap_or(false)
        })
        .count();
    let scored = requests.iter().filter(|row| row.outcome.is_some()).count();
    let title = if scored > 0 {
        format!(
            "Requests ({}) — {passed}/{scored} replayed clean",
            requests.len()
        )
    } else {
        format!("Requests ({})", requests.len())
    };
    let rows = requests
        .iter()
        .map(|request| request_row_cells(request))
        .collect::<Vec<_>>();
    let table = Table::new(
        rows,
        [
            Constraint::Length(2),
            Constraint::Percentage(52),
            Constraint::Length(6),
            Constraint::Length(11),
            Constraint::Percentage(30),
        ],
    )
    .header(header_row([
        "",
        "request",
        "events",
        "status",
        "correlation",
    ]))
    .block(focused_panel(
        &title,
        app.request_focus == RequestFocus::Requests,
    ))
    .highlight_style(Style::default().add_modifier(Modifier::REVERSED | Modifier::BOLD));
    frame.render_stateful_widget(table, chunks[0], &mut app.request_table);

    let selected_request = app.current_request_id();
    let tree_items = selected_request
        .as_deref()
        .map(|request| app.filtered_request_tree(request))
        .unwrap_or_default();
    let tree_rows = tree_items.iter().map(|item| {
        let style = match item.kind {
            RequestTreeItemKind::Header => Style::default().fg(Color::Yellow),
            RequestTreeItemKind::Semantic(_) => Style::default().fg(Color::White),
            RequestTreeItemKind::Graph(_) => Style::default().fg(Color::Cyan),
        };
        Row::new(vec![Cell::from(item.label.clone()).style(style)])
    });
    let tree = Table::new(tree_rows, [Constraint::Percentage(100)])
        .block(focused_panel(
            "Nested Request",
            app.request_focus == RequestFocus::Tree,
        ))
        .highlight_style(Style::default().fg(Color::Black).bg(Color::Yellow));
    frame.render_stateful_widget(tree, chunks[1], &mut app.tree_table);

    let detail = app
        .selected_tree_item()
        .map(|item| selected_item_detail(app, item))
        .unwrap_or_else(|| "No request selected".to_owned());
    frame.render_widget(
        Paragraph::new(detail)
            .block(panel("Selected Record  ·  PgUp/PgDn scroll"))
            .wrap(Wrap { trim: false })
            .scroll((app.detail_scroll, 0)),
        chunks[2],
    );
}

fn render_timeline(frame: &mut Frame<'_>, area: Rect, app: &mut App) {
    let Some(replay) = app.artifacts.replay.clone() else {
        frame.render_widget(
            Paragraph::new(
                "no replay artifacts found — record+replay first.\n\n\
                 Expected:\n  <state-dir>/observed/<run>.jsonl\n  <state-dir>/runs/<run>.scorecard.json",
            )
            .block(panel("Substitution Timeline"))
            .style(Style::default().fg(Color::Yellow))
            .wrap(Wrap { trim: false }),
            area,
        );
        return;
    };

    let novel = novel_observed_calls(&replay);
    let novel_height = if novel.is_empty() {
        0
    } else {
        (novel.len().min(4) as u16).saturating_add(2)
    };
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(6), Constraint::Length(novel_height)])
        .split(area);

    render_replay_timeline(frame, chunks[0], app);

    if !novel.is_empty() {
        let lines = novel
            .iter()
            .take(4)
            .map(|call| {
                Line::from(Span::styled(
                    format!(
                        "novel  {}  {}::{}  corr={}",
                        call.boundary,
                        short(&call.trait_name, 24),
                        short(&call.method_name, 32),
                        short(call.correlation_id.as_deref().unwrap_or("-"), 14),
                    ),
                    Style::default().fg(Color::Red),
                ))
            })
            .collect::<Vec<_>>();
        frame.render_widget(
            Paragraph::new(lines).block(panel(&format!(
                "Novel Calls — replay executed, recording never saw ({})",
                novel.len()
            ))),
            chunks[1],
        );
    }
}

fn render_replay_verdict(frame: &mut Frame<'_>, area: Rect, scorecard: Option<&Scorecard>) {
    let (text, color) = match scorecard.map(|card| &card.verdict) {
        Some(verdict) if verdict.pass => (
            "✅ SELF-REPLAY PASS — same code, zero divergence".to_owned(),
            Color::Green,
        ),
        Some(verdict) if verdict.inconclusive => (
            format!("⚠ INCONCLUSIVE — {}", verdict.reason),
            Color::Yellow,
        ),
        Some(verdict) => (format!("❌ FAIL — {}", verdict.reason), Color::Red),
        None => (
            "⚠ no scorecard — observed calls only".to_owned(),
            Color::Yellow,
        ),
    };
    let line = Line::from(Span::styled(
        text,
        Style::default().fg(color).add_modifier(Modifier::BOLD),
    ));
    frame.render_widget(
        Paragraph::new(line)
            .block(panel("Verdict"))
            .style(Style::default().fg(color)),
        area,
    );
}

fn render_replay_summary(frame: &mut Frame<'_>, area: Rect, scorecard: Option<&Scorecard>) {
    let text = match scorecard.map(|card| &card.summary) {
        Some(summary) => format!(
            "requests {}/{} matched · http status {} / body {} mismatches\n\
             side-effects: {} substituted · {} diverged ({} omitted, {} novel, {} environmental, {} inconclusive_race)",
            summary.matched_correlations,
            summary.total_correlations,
            summary.http_status_mismatches,
            summary.http_body_mismatches,
            summary.matched_side_effect_calls,
            summary.side_effect_divergences,
            summary.omitted_calls,
            summary.novel_calls,
            summary.environmental_misses,
            summary.inconclusive_races,
        ),
        None => "scorecard summary unavailable".to_owned(),
    };
    frame.render_widget(
        Paragraph::new(text).style(Style::default().fg(Color::Gray)),
        area,
    );
}

fn render_replay_boundaries(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let block = panel("Per-Boundary Substitution");
    if app.boundary_substitution.is_empty() {
        frame.render_widget(
            Paragraph::new("no boundary substitution data").block(block),
            area,
        );
        return;
    }
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let rows = inner.height as usize;
    let mut lines: Vec<Line<'_>> = Vec::with_capacity(rows);
    for (boundary, substituted, total) in app.boundary_substitution.iter().take(rows) {
        let bar_width = 24usize;
        let filled = if *total == 0 {
            0
        } else {
            substituted.saturating_mul(bar_width) / total
        };
        let tier = boundary_tier(app, boundary);
        let all_substituted = total > &0 && substituted == total;
        let bar_color = if all_substituted {
            Color::Green
        } else if *substituted == 0 {
            Color::Red
        } else {
            Color::Yellow
        };
        let label_color = if matches!(tier.as_deref(), Some("pure") | Some("excluded")) {
            Color::DarkGray
        } else {
            Color::White
        };
        let mut spans = vec![
            Span::styled(
                format!("{:<16}", short(boundary, 16)),
                Style::default().fg(label_color),
            ),
            Span::styled(
                format!("{:>4}/{:<4} ", substituted, total),
                Style::default().fg(label_color),
            ),
            Span::styled("█".repeat(filled), Style::default().fg(bar_color)),
            Span::styled(
                "░".repeat(bar_width.saturating_sub(filled)),
                Style::default().fg(Color::DarkGray),
            ),
        ];
        if let Some(tier) = tier {
            spans.push(Span::styled(
                format!(" [{tier}]"),
                Style::default().fg(Color::DarkGray),
            ));
        }
        lines.push(Line::from(spans));
    }
    frame.render_widget(Paragraph::new(lines), inner);
}

fn render_replay_timeline(frame: &mut Frame<'_>, area: Rect, app: &mut App) {
    let events = app.replay_timeline_events();
    let substituted_count = events
        .iter()
        .filter(|event| {
            matches!(
                app.substitution.get(&event.global_sequence),
                Some(Substitution::Substituted { .. })
            )
        })
        .count();
    let title = format!(
        "Substitution Timeline ({} substituted / {} recorded)",
        substituted_count,
        events.len()
    );

    let rows = events.iter().map(|event| {
        let status = app.substitution.get(&event.global_sequence).copied();
        let tier = boundary_tier(app, &event.boundary);
        let (status_label, row_color) = match status {
            Some(Substitution::Substituted { rank }) => {
                let label = match rank {
                    Some(rank) => format!("✓ substituted (rank {rank})"),
                    None => "✓ substituted".to_owned(),
                };
                // Dim substitutions on pure/excluded boundaries — they are not
                // material side-effects.
                let color = if matches!(tier.as_deref(), Some("pure") | Some("excluded")) {
                    Color::DarkGray
                } else {
                    Color::Green
                };
                (label, color)
            }
            // Red is reserved for true omissions: a recorded side-effect the
            // replay never asked for. Anchored/pure/environmental events are
            // expected to skip substitution and must not read as failures.
            Some(Substitution::NotReplayed) | None => match tier.as_deref() {
                _ if event.boundary == "http_incoming" => {
                    ("⚓ anchored (kernel-driven)".to_owned(), Color::Cyan)
                }
                Some("pure") | Some("excluded") => ("· live (pure)".to_owned(), Color::DarkGray),
                Some("environmental") => ("≈ environmental".to_owned(), Color::Yellow),
                _ => ("✗ omitted".to_owned(), Color::Red),
            },
        };
        Row::new(vec![
            Cell::from(event.global_sequence.to_string()),
            Cell::from(short(event.correlation_id.as_deref().unwrap_or("-"), 8)),
            Cell::from(short(&event.boundary, 14)),
            Cell::from(short(
                &format!("{}::{}", event.trait_name, event.method_name),
                40,
            )),
            Cell::from(status_label),
        ])
        .style(Style::default().fg(row_color))
    });

    let table = Table::new(
        rows,
        [
            Constraint::Length(6),
            Constraint::Length(9),
            Constraint::Length(15),
            Constraint::Percentage(44),
            Constraint::Percentage(28),
        ],
    )
    .header(header_row([
        "seq",
        "corr",
        "boundary",
        "trait::method",
        "status",
    ]))
    .block(panel(&title))
    .highlight_style(Style::default().add_modifier(Modifier::REVERSED | Modifier::BOLD));
    frame.render_stateful_widget(table, area, &mut app.replay_table);
}

/// Look up the scorecard tier ("pure", "stateful", "excluded", …) for a boundary.
fn boundary_tier(app: &App, boundary: &str) -> Option<String> {
    app.artifacts
        .replay
        .as_ref()?
        .scorecard
        .as_ref()?
        .per_boundary
        .get(boundary)
        .map(|stat: &BoundaryStat| stat.tier.clone())
        .filter(|tier| !tier.is_empty())
}

/// One row of the Requests table: pass icon, METHOD /path, event count,
/// recorded→replayed status, correlation id.
fn request_row_cells(request: &RequestOutcome) -> Row<'_> {
    let (icon, icon_color) = match &request.outcome {
        Some(outcome) if outcome.passed => ("✓", Color::Green),
        Some(_) => ("✗", Color::Red),
        None => ("·", Color::DarkGray),
    };
    let status = request
        .http_diff
        .as_ref()
        .map(|diff| {
            if diff.status_match {
                format!("{} = {}", diff.status_baseline, diff.status_candidate)
            } else {
                format!("{} → {}", diff.status_baseline, diff.status_candidate)
            }
        })
        .unwrap_or_else(|| "-".to_owned());
    let status_color = match request.http_diff.as_ref() {
        Some(diff) if diff.status_match && diff.body_diff.is_empty() => Color::Green,
        Some(_) => Color::Red,
        None => Color::DarkGray,
    };
    Row::new(vec![
        Cell::from(icon).style(Style::default().fg(icon_color)),
        Cell::from(format!("{} {}", request.method, short(&request.path, 40))),
        Cell::from(request.event_count.to_string()),
        Cell::from(status).style(Style::default().fg(status_color)),
        Cell::from(short(&request.correlation_id, 26)),
    ])
}

/// Recorded vs replayed HTTP responses, the byte-exact comparison the verdict
/// is built on: per-request status line plus every `json_path` body diff.
fn render_http(frame: &mut Frame<'_>, area: Rect, app: &mut App) {
    let rows_data = app.filtered_request_rows_owned();
    if rows_data.is_empty() {
        frame.render_widget(
            Paragraph::new("no requests loaded — is this a record/replay state dir?")
                .block(panel("HTTP Diff"))
                .style(Style::default().fg(Color::Yellow)),
            area,
        );
        return;
    }

    let compared = rows_data
        .iter()
        .filter(|row| row.http_diff.is_some())
        .count();
    let exact = rows_data
        .iter()
        .filter_map(|row| row.http_diff.as_ref())
        .filter(|diff| diff.status_match && diff.body_diff.is_empty())
        .count();

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(45), Constraint::Percentage(55)])
        .split(area);

    let title = format!("Recorded vs Replayed Responses — {exact}/{compared} byte-exact");
    let rows = rows_data
        .iter()
        .map(|request| {
            let diffs = request
                .http_diff
                .as_ref()
                .map(|diff| diff.body_diff.len().to_string())
                .unwrap_or_else(|| "-".to_owned());
            let (icon, color) = match request.http_diff.as_ref() {
                Some(diff) if diff.status_match && diff.body_diff.is_empty() => ("✓", Color::Green),
                Some(_) => ("✗", Color::Red),
                None => ("·", Color::DarkGray),
            };
            let status = request
                .http_diff
                .as_ref()
                .map(|diff| format!("{} vs {}", diff.status_baseline, diff.status_candidate))
                .unwrap_or_else(|| "-".to_owned());
            Row::new(vec![
                Cell::from(icon),
                Cell::from(format!("{} {}", request.method, short(&request.path, 44))),
                Cell::from(status),
                Cell::from(diffs),
            ])
            .style(Style::default().fg(color))
        })
        .collect::<Vec<_>>();
    let table = Table::new(
        rows,
        [
            Constraint::Length(2),
            Constraint::Percentage(60),
            Constraint::Length(12),
            Constraint::Length(10),
        ],
    )
    .header(header_row(["", "request", "status", "body diffs"]))
    .block(panel(&title))
    .highlight_style(Style::default().add_modifier(Modifier::REVERSED | Modifier::BOLD));
    frame.render_stateful_widget(table, chunks[0], &mut app.http_table);

    let detail = app
        .selected_http_diff_row()
        .map(http_diff_detail)
        .unwrap_or_else(|| "select a request".to_owned());
    frame.render_widget(
        Paragraph::new(detail)
            .block(panel(
                "Body Diff Detail (recorded vs replayed)  ·  PgUp/PgDn scroll",
            ))
            .wrap(Wrap { trim: false })
            .scroll((app.detail_scroll, 0)),
        chunks[1],
    );
}

/// True when a request diverged on replay: an HTTP status/body mismatch, a
/// side-effect divergence (omitted/novel call), or an overall failed correlation.
fn request_has_divergence(row: &RequestOutcome) -> bool {
    let http = row
        .http_diff
        .as_ref()
        .is_some_and(|diff| !diff.status_match || !diff.body_diff.is_empty());
    let side_effect = row
        .outcome
        .as_ref()
        .is_some_and(|o| o.side_effect_divergences > 0 || !o.passed);
    http || side_effect
}

/// One-line summary of WHAT diverged for a request, e.g.
/// `status 200→500 · body 1 · side-fx 11`.
fn divergence_summary(row: &RequestOutcome) -> String {
    let mut parts = Vec::new();
    if let Some(diff) = row.http_diff.as_ref() {
        if !diff.status_match {
            parts.push(format!(
                "status {}→{}",
                diff.status_baseline, diff.status_candidate
            ));
        }
        if !diff.body_diff.is_empty() {
            parts.push(format!("body {}", diff.body_diff.len()));
        }
    }
    if let Some(o) = row.outcome.as_ref() {
        if o.side_effect_divergences > 0 {
            parts.push(format!("side-fx {}", o.side_effect_divergences));
        }
    }
    if parts.is_empty() {
        "diverged".to_owned()
    } else {
        parts.join(" · ")
    }
}

/// The Divergences tab: ONLY the requests that diverged, with a detail pane
/// showing what differs (HTTP status/body diff + the side-effect divergence
/// count). `Enter` drills into that correlation in the Requests tab.
fn render_divergences(frame: &mut Frame<'_>, area: Rect, app: &mut App) {
    let rows_data = app.diverged_request_rows();
    if rows_data.is_empty() {
        let msg = if app.request_rows.is_empty() {
            "no requests loaded — is this a record/replay state dir?"
        } else {
            "✓ no divergences — every request replayed byte-exact with all \
             side-effects substituted"
        };
        frame.render_widget(
            Paragraph::new(msg)
                .block(panel("Divergences"))
                .style(Style::default().fg(Color::Green))
                .wrap(Wrap { trim: false }),
            area,
        );
        return;
    }
    if app.artifacts.replay.is_none() {
        frame.render_widget(
            Paragraph::new(
                "no replay run loaded — a record AND replay are required for a divergence diff",
            )
            .block(panel("Divergences"))
            .style(Style::default().fg(Color::Yellow)),
            area,
        );
        return;
    }

    // Three zones: request selector · split diff body · key hint.
    let sel_h = (rows_data.len() as u16 + 3).min(8);
    let zones = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(sel_h),
            Constraint::Min(6),
            Constraint::Length(1),
        ])
        .split(area);

    // --- zone 0: request selector ---
    let sel_focused = app.diff_focus == DivFocus::Selector;
    let title = format!(
        "Diverging Requests — {} of {}",
        rows_data.len(),
        app.request_rows.len()
    );
    let trows = rows_data
        .iter()
        .map(|request| {
            let (vmark, vcol) = match request.outcome.as_ref().map(|o| o.passed) {
                Some(true) => ("✓ pass", Color::Green),
                _ => ("✗ FAIL", Color::Red),
            };
            Row::new(vec![
                Cell::from(format!("{} {}", request.method, short(&request.path, 34))),
                Cell::from(vmark).style(Style::default().fg(vcol)),
                Cell::from(divergence_summary(request)),
            ])
        })
        .collect::<Vec<_>>();
    let table = Table::new(
        trows,
        [
            Constraint::Percentage(45),
            Constraint::Length(8),
            Constraint::Percentage(45),
        ],
    )
    .header(header_row(["request", "verdict", "what diverged"]))
    .block(focused_panel(&title, sel_focused))
    .highlight_style(Style::default().add_modifier(Modifier::REVERSED | Modifier::BOLD));
    frame.render_stateful_widget(table, zones[0], &mut app.divergence_table);

    // --- zone 1: the record↔replay diff body ---
    let diff_focused = app.diff_focus == DivFocus::Diff;
    if app.diff_rows.is_empty() {
        frame.render_widget(
            Paragraph::new(
                "↑/↓ pick a diverging request, then Enter to open its record↔replay diff",
            )
            .block(panel("Diff"))
            .wrap(Wrap { trim: false }),
            zones[1],
        );
    } else if app.div_view == DivView::Inline || zones[1].width < 100 {
        // Unified single-column (narrow terminals or `s` toggle).
        let width = zones[1].width.saturating_sub(2) as usize;
        let (lines, cursor_line) = build_diff_lines_inline(app, width);
        autoscroll(app, cursor_line, zones[1].height);
        frame.render_widget(
            Paragraph::new(lines)
                .block(focused_panel(
                    "RECORD → REPLAY  (unified · s for split)",
                    diff_focused,
                ))
                .wrap(Wrap { trim: false })
                .scroll((app.diff_scroll, 0)),
            zones[1],
        );
    } else {
        let cols = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(zones[1]);
        let width = cols[0].width.saturating_sub(2) as usize;
        let (left, right, cursor_line) = build_diff_lines(app, width);
        autoscroll(app, cursor_line, cols[0].height);
        frame.render_widget(
            Paragraph::new(left)
                .block(focused_panel("RECORD (expected)", diff_focused))
                .wrap(Wrap { trim: false })
                .scroll((app.diff_scroll, 0)),
            cols[0],
        );
        frame.render_widget(
            Paragraph::new(right)
                .block(panel("REPLAY (candidate)"))
                .wrap(Wrap { trim: false })
                .scroll((app.diff_scroll, 0)),
            cols[1],
        );
    }

    // --- zone 2: key hint ---
    let hint = if app.diff_focus == DivFocus::Selector {
        "↑/↓ pick request · Enter/n open diff · [ ] prev/next request · s split/inline"
    } else {
        "j/k move · n/N next/prev divergence · Enter expand · Esc back · [ ] request · s view · PgUp/PgDn scroll"
    };
    frame.render_widget(
        Paragraph::new(hint).style(Style::default().fg(Color::DarkGray)),
        zones[2],
    );
}

/// Keep the cursor row visible by nudging the shared scroll offset.
fn autoscroll(app: &mut App, cursor_line: Option<usize>, pane_height: u16) {
    let Some(cl) = cursor_line else { return };
    let cl = cl as u16;
    let inner = pane_height.saturating_sub(2); // borders
    if cl < app.diff_scroll {
        app.diff_scroll = cl;
    } else if inner > 0 && cl >= app.diff_scroll + inner {
        app.diff_scroll = cl - inner + 1;
    }
}

fn row_base_style(kind: &DiffKind) -> Style {
    match kind {
        DiffKind::Matched => Style::default().add_modifier(Modifier::DIM),
        DiffKind::Omitted => Style::default().fg(Color::Red),
        DiffKind::Novel => Style::default().fg(Color::Green),
        _ => Style::default().fg(Color::Yellow),
    }
}

fn gutter_for(kind: &DiffKind) -> &'static str {
    match kind {
        DiffKind::Matched => "·",
        DiffKind::Omitted => "-",
        DiffKind::Novel => "+",
        _ => "~",
    }
}

/// Build the left (record) and right (replay) line vectors for the split diff,
/// kept index-aligned so a row occupies the same vertical band on both panes.
/// Returns the first line index of the cursor row for autoscroll.
fn build_diff_lines(
    app: &App,
    width: usize,
) -> (Vec<Line<'static>>, Vec<Line<'static>>, Option<usize>) {
    let mut left: Vec<Line> = Vec::new();
    let mut right: Vec<Line> = Vec::new();
    let mut cursor_line = None;
    let rows = &app.diff_rows;
    let mut i = 0;
    while i < rows.len() {
        // Fold a run of ≥4 matched rows that doesn't contain the cursor.
        if matches!(rows[i].kind, DiffKind::Matched) {
            let mut j = i;
            while j < rows.len() && matches!(rows[j].kind, DiffKind::Matched) {
                j += 1;
            }
            let run = j - i;
            let cursor_in =
                app.diff_focus == DivFocus::Diff && app.diff_cursor >= i && app.diff_cursor < j;
            if run >= 4 && !cursor_in {
                let txt = format!("  ⋯ {run} matched events ⋯");
                let s = Style::default().fg(Color::DarkGray);
                left.push(Line::from(Span::styled(txt.clone(), s)));
                right.push(Line::from(Span::styled(txt, s)));
                i = j;
                continue;
            }
        }
        if i == app.diff_cursor {
            cursor_line = Some(left.len());
        }
        let (mut lb, mut rb) = diff_row_lines(app, i, width);
        let h = lb.len().max(rb.len());
        while lb.len() < h {
            lb.push(Line::from(""));
        }
        while rb.len() < h {
            rb.push(Line::from(""));
        }
        left.extend(lb);
        right.extend(rb);
        i += 1;
    }
    (left, right, cursor_line)
}

/// The line(s) for one diff row on each pane.
fn diff_row_lines(app: &App, i: usize, width: usize) -> (Vec<Line<'static>>, Vec<Line<'static>>) {
    let row = &app.diff_rows[i];
    let cursor = app.diff_focus == DivFocus::Diff && i == app.diff_cursor;
    let base = row_base_style(&row.kind);
    let style = if cursor {
        base.add_modifier(Modifier::REVERSED | Modifier::BOLD)
    } else {
        base
    };
    let expandable = matches!(
        row.kind,
        DiffKind::Changed { .. } | DiffKind::HttpBody { .. }
    );
    let expanded = app.diff_expanded.contains(&i);
    let marker = if expandable {
        if expanded {
            " [-]"
        } else {
            " [+]"
        }
    } else {
        ""
    };
    let g = gutter_for(&row.kind);
    let label = format!("{g} {}{marker}", short(&row.label, width.saturating_sub(6)));
    let mut left = Vec::new();
    let mut right = Vec::new();
    match &row.kind {
        DiffKind::Omitted => {
            left.push(Line::from(Span::styled(label, style)));
            right.push(Line::from(""));
        }
        DiffKind::Novel => {
            left.push(Line::from(""));
            right.push(Line::from(Span::styled(label, style)));
        }
        DiffKind::Changed { field_diffs } | DiffKind::HttpBody { field_diffs } => {
            let line = Line::from(Span::styled(label, style));
            left.push(line.clone());
            right.push(line);
            if expanded {
                let (ls, rs) = render_field_diffs(field_diffs, width);
                left.extend(ls);
                right.extend(rs);
            }
        }
        // Matched + HttpStatus: same label on both sides (context).
        _ => {
            let line = Line::from(Span::styled(label, style));
            left.push(line.clone());
            right.push(line);
        }
    }
    (left, right)
}

/// Expanded field-level diff lines for both panes. String leaves are
/// prefix/suffix-trimmed so only the changed run is emphasized.
fn render_field_diffs(
    diffs: &[FieldDiff],
    width: usize,
) -> (Vec<Line<'static>>, Vec<Line<'static>>) {
    let mut left = Vec::new();
    let mut right = Vec::new();
    let dim = Style::default().add_modifier(Modifier::DIM);
    for d in diffs.iter().take(30) {
        let path = d.json_path.clone();
        match (&d.baseline, &d.candidate) {
            (serde_json::Value::String(b), serde_json::Value::String(c)) => {
                let (lspans, rspans) = string_leaf_spans(b, c);
                let mut ll = vec![Span::styled(format!("    {path}: "), dim)];
                ll.extend(lspans);
                left.push(Line::from(ll));
                let mut rl = vec![Span::styled(format!("    {path}: "), dim)];
                rl.extend(rspans);
                right.push(Line::from(rl));
            }
            _ => {
                left.push(Line::from(Span::styled(
                    format!(
                        "    {path}: {}",
                        short(&d.baseline.to_string(), width.saturating_sub(8))
                    ),
                    Style::default().fg(Color::Red),
                )));
                right.push(Line::from(Span::styled(
                    format!(
                        "    {path}: {}",
                        short(&d.candidate.to_string(), width.saturating_sub(8))
                    ),
                    Style::default().fg(Color::Green),
                )));
            }
        }
    }
    if diffs.len() > 30 {
        let more = format!("    …+{} more fields", diffs.len() - 30);
        left.push(Line::from(Span::styled(more.clone(), dim)));
        right.push(Line::from(Span::styled(more, dim)));
    }
    (left, right)
}

/// Split two strings into (common-prefix DIM, changed-middle colored, common-suffix
/// DIM) spans, windowing long common runs to ~24 chars around the change.
fn string_leaf_spans(base: &str, cand: &str) -> (Vec<Span<'static>>, Vec<Span<'static>>) {
    let b: Vec<char> = base.chars().collect();
    let c: Vec<char> = cand.chars().collect();
    let mut p = 0;
    while p < b.len() && p < c.len() && b[p] == c[p] {
        p += 1;
    }
    let mut s = 0;
    while s < b.len() - p && s < c.len() - p && b[b.len() - 1 - s] == c[c.len() - 1 - s] {
        s += 1;
    }
    let dim = Style::default().add_modifier(Modifier::DIM);
    const W: usize = 24;
    let span_for = |chars: &[char], color: Color| -> Vec<Span<'static>> {
        let n = chars.len();
        let mid_start = p.min(n);
        let mid_end = n.saturating_sub(s).max(mid_start);
        let pre_full: String = chars[..mid_start].iter().collect();
        let mid: String = chars[mid_start..mid_end].iter().collect();
        let suf_full: String = chars[mid_end..].iter().collect();
        let pre = if pre_full.chars().count() > W {
            let tail: String = pre_full
                .chars()
                .rev()
                .take(W)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect();
            format!("…{tail}")
        } else {
            pre_full
        };
        let suf = if suf_full.chars().count() > W {
            format!("{}…", suf_full.chars().take(W).collect::<String>())
        } else {
            suf_full
        };
        let mid_disp = if mid.is_empty() {
            "∅".to_owned()
        } else {
            mid
        };
        vec![
            Span::styled(pre, dim),
            Span::styled(
                mid_disp,
                Style::default().fg(color).add_modifier(Modifier::BOLD),
            ),
            Span::styled(suf, dim),
        ]
    };
    (span_for(&b, Color::Red), span_for(&c, Color::Green))
}

/// Unified single-column diff (narrow terminals or `s` toggle). Returns the lines
/// and the cursor row's first line index.
fn build_diff_lines_inline(app: &App, width: usize) -> (Vec<Line<'static>>, Option<usize>) {
    let mut out: Vec<Line> = Vec::new();
    let mut cursor_line = None;
    let rows = &app.diff_rows;
    let mut i = 0;
    while i < rows.len() {
        if matches!(rows[i].kind, DiffKind::Matched) {
            let mut j = i;
            while j < rows.len() && matches!(rows[j].kind, DiffKind::Matched) {
                j += 1;
            }
            let run = j - i;
            let cursor_in =
                app.diff_focus == DivFocus::Diff && app.diff_cursor >= i && app.diff_cursor < j;
            if run >= 4 && !cursor_in {
                out.push(Line::from(Span::styled(
                    format!("  ⋯ {run} matched events ⋯"),
                    Style::default().fg(Color::DarkGray),
                )));
                i = j;
                continue;
            }
        }
        if i == app.diff_cursor {
            cursor_line = Some(out.len());
        }
        let row = &rows[i];
        let cursor = app.diff_focus == DivFocus::Diff && i == app.diff_cursor;
        let base = row_base_style(&row.kind);
        let style = if cursor {
            base.add_modifier(Modifier::REVERSED | Modifier::BOLD)
        } else {
            base
        };
        let g = gutter_for(&row.kind);
        let expandable = matches!(
            row.kind,
            DiffKind::Changed { .. } | DiffKind::HttpBody { .. }
        );
        let expanded = app.diff_expanded.contains(&i);
        let marker = if expandable {
            if expanded {
                " [-]"
            } else {
                " [+]"
            }
        } else {
            ""
        };
        out.push(Line::from(Span::styled(
            format!("{g} {}{marker}", short(&row.label, width.saturating_sub(4))),
            style,
        )));
        if expanded {
            if let DiffKind::Changed { field_diffs } | DiffKind::HttpBody { field_diffs } =
                &row.kind
            {
                for d in field_diffs.iter().take(30) {
                    out.push(Line::from(Span::styled(
                        format!(
                            "    {}: {} → {}",
                            d.json_path,
                            short(&d.baseline.to_string(), 28),
                            short(&d.candidate.to_string(), 28)
                        ),
                        Style::default().fg(Color::Yellow),
                    )));
                }
            }
        }
        i += 1;
    }
    (out, cursor_line)
}

/// Detail text for one request's HTTP comparison (used by the HTTP Diff tab).
fn http_diff_detail(request: &RequestOutcome) -> String {
    let Some(diff) = request.http_diff.as_ref() else {
        return format!(
            "{} {}\n\nno HTTP comparison recorded for this correlation \
             (not driven by the kernel, e.g. a health probe).",
            request.method, request.path
        );
    };
    let mut out = format!(
        "{} {}\ncorrelation: {}\nstatus: recorded {}  replayed {}  ({})\n",
        request.method,
        request.path,
        request.correlation_id,
        diff.status_baseline,
        diff.status_candidate,
        if diff.status_match {
            "match"
        } else {
            "MISMATCH"
        },
    );
    if diff.body_diff.is_empty() {
        out.push_str("\nbody: byte-exact — recorded and replayed responses are identical");
        return out;
    }
    out.push_str(&format!("\nbody diffs ({}):\n", diff.body_diff.len()));
    for entry in &diff.body_diff {
        out.push_str(&format!(
            "\n--- {} ---\nrecorded: {}\nreplayed: {}\n",
            entry.json_path,
            pretty_json_value(&entry.baseline),
            pretty_json_value(&entry.candidate),
        ));
    }
    out
}

fn render_footer(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let mode = match app.input_mode {
        InputMode::Normal => HELP.to_owned(),
        InputMode::Search => "search: enter commit | esc cancel".to_owned(),
        InputMode::Boundary => "boundary: enter commit | esc cancel".to_owned(),
        InputMode::Request => {
            "request: regex supported (e.g. deja-.+) | enter commit | esc cancel".to_owned()
        }
    };
    frame.render_widget(
        Paragraph::new(mode).style(Style::default().fg(Color::Gray)),
        area,
    );
}

fn render_input(frame: &mut Frame<'_>, app: &App) {
    let area = centered_rect(60, 20, frame.size());
    let title = match app.input_mode {
        InputMode::Search => "Search",
        InputMode::Boundary => "Boundary Filter",
        InputMode::Request => "Request/Correlation ID",
        InputMode::Normal => "",
    };
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(app.input_buffer.as_str())
            .block(Block::default().borders(Borders::ALL).title(title))
            .style(Style::default().fg(Color::Yellow)),
        area,
    );
}

fn print_summary(artifacts: &LoadedArtifacts) {
    let summary = summarize(artifacts);
    println!(
        "Deja/Hyperswitch artifacts: {}",
        artifacts.paths.root.display()
    );
    print_file_stat("semantic", artifacts.semantic_stats.as_ref());
    print_file_stat("graph", artifacts.graph_stats.as_ref());
    println!(
        "semantic_events={} semantic_errors={} graph_spans={} graph_errors={}",
        artifacts.semantic_events.len(),
        summary.semantic_errors,
        artifacts.graph_records.len(),
        summary.graph_errors
    );
    print_counts("boundaries", &summary.boundary_counts, 12);
    print_counts("top_operations", &summary.top_operations, 12);
    print_counts("graph_spans", &summary.span_counts, 12);
    println!("requests:");
    for (id, semantic_count, graph_count) in summary.request_counts.iter().take(20) {
        println!("  semantic={semantic_count:>5} graph={graph_count:>5} {id}");
    }
    for boundary in unique_boundaries(&artifacts.semantic_events) {
        println!("boundary={boundary}");
    }
    print_replay_summary(artifacts);
}

fn print_replay_summary(artifacts: &LoadedArtifacts) {
    let Some(replay) = &artifacts.replay else {
        println!("replay: <no observed/scorecard artifacts>");
        return;
    };
    println!(
        "replay: observed_calls={} observed_path={} scorecard_path={}",
        replay.observed.len(),
        replay
            .observed_path
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "<missing>".to_owned()),
        replay
            .scorecard_path
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "<missing>".to_owned()),
    );
    if let Some(card) = &replay.scorecard {
        println!(
            "replay_verdict pass={} inconclusive={} reason={:?}",
            card.verdict.pass, card.verdict.inconclusive, card.verdict.reason
        );
        println!(
            "replay_summary matched={}/{} side_effect_divergences={} omitted={} novel={} environmental={} inconclusive_races={} http_status_mismatches={} http_body_mismatches={}",
            card.summary.matched_correlations,
            card.summary.total_correlations,
            card.summary.side_effect_divergences,
            card.summary.omitted_calls,
            card.summary.novel_calls,
            card.summary.environmental_misses,
            card.summary.inconclusive_races,
            card.summary.http_status_mismatches,
            card.summary.http_body_mismatches,
        );
    }
    let status = substitution_status(&artifacts.semantic_events, replay);
    let substituted = status
        .values()
        .filter(|status| matches!(status, Substitution::Substituted { .. }))
        .count();
    println!(
        "replay_substitution substituted={}/{} recorded events",
        substituted,
        artifacts.semantic_events.len()
    );
    for (boundary, sub, total) in boundary_substitution_counts(&artifacts.semantic_events, replay) {
        println!("  {boundary:<16} {sub:>5}/{total:<5} substituted");
    }
}

fn print_file_stat(label: &str, stats: Option<&JsonlStats>) {
    match stats {
        Some(stats) => println!(
            "{label}_path={} {label}_lines={} {label}_skipped={}",
            stats.path.display(),
            stats.lines,
            stats.skipped
        ),
        None => println!("{label}_path=<missing> {label}_lines=0 {label}_skipped=0"),
    }
}

fn print_counts(label: &str, counts: &[(String, usize)], limit: usize) {
    println!("{label}:");
    for (name, count) in counts.iter().take(limit) {
        println!("  {count:>6} {name}");
    }
}

fn overview_text(app: &App) -> String {
    let semantic = file_line("semantic", app.artifacts.semantic_stats.as_ref());
    let graph = file_line("graph", app.artifacts.graph_stats.as_ref());
    format!(
        "{semantic}\n{graph}\n\nsemantic events: {}\nsemantic errors: {}\ngraph spans: {}\ngraph errors: {}\nrequests/correlations: {}",
        app.artifacts.semantic_events.len(),
        app.summary.semantic_errors,
        app.artifacts.graph_records.len(),
        app.summary.graph_errors,
        app.summary.request_counts.len(),
    )
}

fn file_line(label: &str, stats: Option<&JsonlStats>) -> String {
    match stats {
        Some(stats) => format!(
            "{label}: {}\n  lines={} skipped={}",
            stats.path.display(),
            stats.lines,
            stats.skipped
        ),
        None => format!("{label}: <missing>"),
    }
}

fn append_semantic_tree_items(
    items: &mut Vec<RequestTreeItem>,
    events: Vec<(usize, &BoundaryEvent)>,
) {
    items.push(RequestTreeItem {
        label: "semantic".to_owned(),
        kind: RequestTreeItemKind::Header,
    });
    if events.is_empty() {
        items.push(RequestTreeItem {
            label: "  <none>".to_owned(),
            kind: RequestTreeItemKind::Header,
        });
        return;
    }

    let mut by_boundary: BTreeMap<&str, Vec<(usize, &BoundaryEvent)>> = BTreeMap::new();
    for (index, event) in events {
        by_boundary
            .entry(event.boundary.as_str())
            .or_default()
            .push((index, event));
    }

    for (boundary, events) in by_boundary {
        items.push(RequestTreeItem {
            label: format!("  {boundary} ({})", events.len()),
            kind: RequestTreeItemKind::Header,
        });
        for (index, event) in events {
            items.push(RequestTreeItem {
                label: format!(
                    "    - #{:>5}.{:>3} {}::{} {}us{}",
                    event.global_sequence,
                    event.request_sequence,
                    short(&event.trait_name, 24),
                    short(&event.method_name, 30),
                    event.duration_us,
                    if event.is_error { " ERROR" } else { "" }
                ),
                kind: RequestTreeItemKind::Semantic(index),
            });
        }
    }
}

fn append_graph_tree_items(
    items: &mut Vec<RequestTreeItem>,
    all_records: &[ExecutionGraphNode],
    records: Vec<&ExecutionGraphNode>,
) {
    items.push(RequestTreeItem {
        label: "graph".to_owned(),
        kind: RequestTreeItemKind::Header,
    });
    if records.is_empty() {
        items.push(RequestTreeItem {
            label: "  <none>".to_owned(),
            kind: RequestTreeItemKind::Header,
        });
        return;
    }

    let mut by_id = HashMap::new();
    let mut children: BTreeMap<Option<u64>, Vec<&ExecutionGraphNode>> = BTreeMap::new();
    for record in &records {
        by_id.insert(record.node_id, *record);
    }
    for record in &records {
        let parent = record
            .parent_id
            .filter(|parent_id| by_id.contains_key(parent_id));
        children.entry(parent).or_default().push(*record);
    }
    for records in children.values_mut() {
        records.sort_by_key(|record| record.sequence);
    }

    let mut emitted = 0;
    append_graph_child_items(items, all_records, &children, None, 1, &mut emitted);
    if emitted == 0 {
        let mut records = records;
        records.sort_by_key(|record| record.sequence);
        for record in records {
            append_graph_item(items, all_records, record, 1);
        }
    }
}

fn append_graph_child_items(
    items: &mut Vec<RequestTreeItem>,
    all_records: &[ExecutionGraphNode],
    children: &BTreeMap<Option<u64>, Vec<&ExecutionGraphNode>>,
    parent: Option<u64>,
    depth: usize,
    emitted: &mut usize,
) {
    if *emitted >= 160 {
        return;
    }
    let Some(records) = children.get(&parent) else {
        return;
    };
    for record in records {
        if *emitted >= 160 {
            items.push(RequestTreeItem {
                label: "  ... truncated ...".to_owned(),
                kind: RequestTreeItemKind::Header,
            });
            return;
        }
        append_graph_item(items, all_records, record, depth);
        *emitted += 1;
        append_graph_child_items(
            items,
            all_records,
            children,
            Some(record.node_id),
            depth + 1,
            emitted,
        );
    }
}

fn append_graph_item(
    items: &mut Vec<RequestTreeItem>,
    all_records: &[ExecutionGraphNode],
    record: &ExecutionGraphNode,
    depth: usize,
) {
    let index = all_records
        .iter()
        .position(|candidate| {
            candidate.node_id == record.node_id && candidate.sequence == record.sequence
        })
        .unwrap_or(0);
    let indent = "  ".repeat(depth);
    let node = record;
    items.push(RequestTreeItem {
        label: format!(
            "{indent}+-- #{:>5}.{:>3} {} [{}] {}",
            node.sequence,
            node.node_id,
            short(&node.span_name, 28),
            node.level,
            duration_label(node.started_ns, node.closed_ns)
        ),
        kind: RequestTreeItemKind::Graph(index),
    });
}

fn selected_item_detail(app: &App, item: RequestTreeItem) -> String {
    match item.kind {
        RequestTreeItemKind::Header => item.label,
        RequestTreeItemKind::Semantic(index) => {
            let Some(event) = app.artifacts.semantic_events.get(index) else {
                return "semantic event is no longer available".to_owned();
            };
            let pretty = serde_json::to_string_pretty(event)
                .unwrap_or_else(|error| format!("failed to render event JSON: {error}"));
            format!(
                "semantic event\n\noperation: {}::{}\nboundary: {}\nrequest_id: {}\ngraph_node_id: {}\ntracing_span_id: {}\nerror: {}\nduration: {}us\ncallsite: {}:{}:{}\n\nfull record:\n{}",
                event.trait_name,
                event.method_name,
                event.boundary,
                event.correlation_id.as_deref().unwrap_or("-"),
                event
                    .graph_node_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "-".to_owned()),
                event
                    .tracing_span_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "-".to_owned()),
                event.is_error,
                event.duration_us,
                event.call_file,
                event.call_line,
                event.call_column,
                pretty
            )
        }
        RequestTreeItemKind::Graph(index) => {
            let Some(record) = app.artifacts.graph_records.get(index) else {
                return "graph span is no longer available".to_owned();
            };
            let node = record;
            let pretty = serde_json::to_string_pretty(record)
                .unwrap_or_else(|error| format!("failed to render span JSON: {error}"));
            let semantic_payloads = semantic_payloads_for_graph_span(app, record);
            format!(
                "graph span\n\nspan: {}\ntarget: {}\nlevel: {}\nrequest_id: {}\nnode_id: {}\nparent_id: {}\nduration: {}\n\nlinked semantic-events.jsonl payloads:\n{}\n\nfull graph record:\n{}",
                node.span_name,
                node.target,
                node.level,
                graph_request_id(record).unwrap_or("-"),
                node.node_id,
                node.parent_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "-".to_owned()),
                duration_label(node.started_ns, node.closed_ns),
                semantic_payloads,
                pretty
            )
        }
    }
}

fn semantic_payloads_for_graph_span(app: &App, record: &ExecutionGraphNode) -> String {
    let exact_matches = app
        .artifacts
        .semantic_events
        .iter()
        .filter(|event| event.graph_node_id == Some(record.node_id))
        .collect::<Vec<_>>();

    if !exact_matches.is_empty() {
        return format_semantic_payload_matches("matched graph_node_id exactly", exact_matches);
    }

    let request_id = graph_request_id(record)
        .map(str::to_owned)
        .or_else(|| app.current_request_id());
    let Some(request_id) = request_id else {
        return "<no request_id available for this graph span>".to_owned();
    };

    let inferred_boundary = infer_semantic_boundary_for_graph(record);
    let mut matches = app
        .artifacts
        .semantic_events
        .iter()
        .filter(|event| semantic_event_request_id(event) == Some(request_id.as_str()))
        .filter(|event| {
            inferred_boundary
                .map(|boundary| event.boundary == boundary)
                .unwrap_or(true)
        })
        .collect::<Vec<_>>();

    matches.sort_by_key(|event| (event.timestamp_ns, event.global_sequence));

    if matches.is_empty() && inferred_boundary.is_some() {
        matches = app
            .artifacts
            .semantic_events
            .iter()
            .filter(|event| semantic_event_request_id(event) == Some(request_id.as_str()))
            .collect::<Vec<_>>();
    }

    if matches.is_empty() {
        return format!("<no semantic-events.jsonl records found for request {request_id}>");
    }

    let reason = if let Some(boundary) = inferred_boundary {
        format!("matched request_id={request_id}, inferred boundary={boundary}")
    } else {
        format!("matched request_id={request_id}")
    };

    format_semantic_payload_matches(&reason, matches)
}

fn format_semantic_payload_matches(reason: &str, mut matches: Vec<&BoundaryEvent>) -> String {
    matches.sort_by_key(|event| (event.timestamp_ns, event.global_sequence));
    let mut output = format!("{reason}\n");
    for event in matches.into_iter().take(6) {
        output.push_str(&format!(
            "\n--- {}::{} boundary={} graph_node={} seq={}.{} duration={}us error={} ---\n",
            event.trait_name,
            event.method_name,
            event.boundary,
            event
                .graph_node_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            event.global_sequence,
            event.request_sequence,
            event.duration_us,
            event.is_error
        ));
        output.push_str("request:\n");
        output.push_str(&pretty_json_value(&event.request));
        output.push_str("\nargs:\n");
        output.push_str(&pretty_json_value(&event.args));
        output.push_str("\nresponse:\n");
        output.push_str(&pretty_json_value(&event.response));
        output.push_str("\nresult:\n");
        output.push_str(&pretty_json_value(&event.result));
        output.push('\n');
    }

    output
}

fn infer_semantic_boundary_for_graph(record: &ExecutionGraphNode) -> Option<&'static str> {
    let haystack = graph_record_text(record).to_ascii_lowercase();
    if haystack.contains("redis") || haystack.contains("fred::") || haystack.contains("api_lock") {
        Some("redis")
    } else if haystack.contains("postgres")
        || haystack.contains("diesel")
        || haystack.contains("database")
        || haystack.contains("storage_impl")
    {
        Some("db")
    } else if haystack.contains("http")
        || haystack.contains("request")
        || haystack.contains("actix")
    {
        Some("http_incoming")
    } else if haystack.contains("time") || haystack.contains("date_time") {
        Some("time")
    } else if haystack.contains("id_generation") || haystack.contains("generate_id") {
        Some("id_generation")
    } else {
        None
    }
}

fn pretty_json_value(value: &serde_json::Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|error| format!("<invalid json: {error}>"))
}

fn header_row<const N: usize>(cells: [&str; N]) -> Row<'_> {
    Row::new(cells)
        .style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
        .bottom_margin(1)
}

fn panel(title: &str) -> Block<'_> {
    Block::default()
        .borders(Borders::ALL)
        .title(title.to_owned())
}

fn focused_panel(title: &str, focused: bool) -> Block<'_> {
    let style = if focused {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default()
    };
    Block::default()
        .borders(Borders::ALL)
        .border_style(style)
        .title(title.to_owned())
}

fn move_table_selection(state: &mut TableState, len: usize, down: bool) {
    if len == 0 {
        state.select(None);
        return;
    }
    let selected = state.selected().unwrap_or(0);
    let next = if down {
        min(selected + 1, len.saturating_sub(1))
    } else {
        selected.saturating_sub(1)
    };
    state.select(Some(next));
}

fn repair_table_selection(state: &mut TableState, len: usize) {
    if len == 0 {
        state.select(None);
        return;
    }
    let selected = state.selected().unwrap_or(0);
    state.select(Some(min(selected, len - 1)));
}

fn blank_filter(value: &str) -> &str {
    if value.is_empty() {
        "-"
    } else {
        value
    }
}

fn short(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let head = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{head}...")
    } else {
        head
    }
}

fn duration_label(started_ns: u64, closed_ns: Option<u64>) -> String {
    closed_ns
        .and_then(|closed_ns| closed_ns.checked_sub(started_ns))
        .map(|duration_ns| format!("{}us", duration_ns / 1_000))
        .unwrap_or_else(|| "open".to_owned())
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}
