use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use dioxus::prelude::*;
use serde_json::{Map, Value};
use tokio::runtime::Handle;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use ariadne_core::graph_backend::GraphBackend;
use ariadne_core::state::{ClusterState, SharedClusterState};
use ariadne_core::types::ResourceType;
use strum::IntoEnumIterator;

use crate::agent::{AnalysisResult, Analyst, ConversationTurn, LlmUsage, Translator};
use crate::error::CliResult;
use crate::validation::validate_cypher;

const SHORT_TERM_CONTEXT_LIMIT: usize = 4;
const COMPACT_CONTEXT_LIMIT: usize = 12;
const CONTEXT_RESERVED_TOKENS: usize = 2048;
const CONTEXT_MIN_TOKENS: usize = 512;
const LLM_MAX_RETRIES: usize = 1;

const APP_CSS: &str = r#"
:root {
  color-scheme: dark;
  font-family: "IBM Plex Mono", "JetBrains Mono", ui-monospace, SFMono-Regular, Menlo, Monaco, "Courier New", monospace;
  background-color: #0f141b;
  color: #e5ecf2;
}
html, body {
  height: 100%;
  overflow: hidden;
}
body {
  margin: 0;
  background-color: #0f141b;
  color: #e5ecf2;
}
.app {
  display: flex;
  flex-direction: column;
  min-height: 100vh;
  height: 100vh;
  background: #0f141b;
}
.header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  padding: 16px 24px;
  background: #141c24;
  border-bottom: 1px solid #2c3846;
  position: sticky;
  top: 0;
  z-index: 20;
}
.header h1 {
  margin: 0;
  font-size: 18px;
  letter-spacing: 0.04em;
}
.header-left {
  display: flex;
  align-items: center;
  gap: 16px;
}
.meta {
  font-size: 12px;
  color: #9aa8b7;
}
.pulse {
  display: flex;
  gap: 12px;
  font-size: 11px;
  color: #8aa0b2;
}
.layout {
  flex: 1;
  display: flex;
  min-height: 0;
}
.sidebar {
  width: 72px;
  background: #121a23;
  border-right: 1px solid #2c3846;
  display: flex;
  flex-direction: column;
  align-items: center;
  padding: 16px 0;
  gap: 12px;
}
.nav-btn {
  width: 40px;
  height: 40px;
  border-radius: 12px;
  background: #1b2530;
  border: 1px solid #2c3846;
  color: #9aa8b7;
  display: grid;
  place-items: center;
  font-size: 12px;
}
.main {
  flex: 1;
  display: flex;
  flex-direction: column;
  min-width: 0;
}
.context-bar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 8px 24px;
  background: #10151c;
  border-bottom: 1px solid #1b2530;
  font-size: 11px;
  color: #8aa0b2;
}
.context-actions {
  display: flex;
  gap: 8px;
}
.context-btn {
  background: #1b2530;
  border: 1px solid #2c3846;
  color: #c7d1dc;
  padding: 4px 8px;
  border-radius: 6px;
  font-size: 11px;
  cursor: pointer;
}
.feed {
  flex: 1;
  overflow: auto;
  padding: 24px;
  display: flex;
  flex-direction: column;
  gap: 16px;
}
.card {
  background: #1b2530;
  border: 1px solid #2c3846;
  border-radius: 10px;
  padding: 16px;
  display: flex;
  flex-direction: column;
  gap: 12px;
}
.card-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  font-size: 13px;
}
.question {
  color: #e5ecf2;
  font-size: 14px;
}
.state {
  font-size: 11px;
  color: #8aa0b2;
}
.cypher {
  background: #121a23;
  border-radius: 8px;
  padding: 12px;
  font-size: 12px;
  white-space: pre-wrap;
  border: 1px solid #2c3846;
}
.cypher-block {
  display: grid;
  gap: 8px;
}
.cypher-block summary {
  cursor: pointer;
  font-size: 12px;
  color: #8aa0b2;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  list-style: none;
}
.cypher-block summary::-webkit-details-marker {
  display: none;
}
.cypher-keyword {
  color: #e6a36c;
  font-weight: 600;
}
.cypher-string {
  color: #8bd3ff;
}
.cypher-text {
  color: #e5ecf2;
}
.cypher-actions {
  display: flex;
  gap: 8px;
}
.result {
  background: #10151c;
  border-radius: 8px;
  padding: 12px;
  border: 1px solid #2c3846;
}
.error {
  font-size: 12px;
  color: #e76f51;
}
.analysis {
  background: #0f141b;
  border: 1px solid #2c3846;
  border-radius: 8px;
  padding: 12px;
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.analysis-title {
  font-size: 12px;
  color: #8aa0b2;
  text-transform: uppercase;
  letter-spacing: 0.06em;
}
.metric-card {
  background: #0f141b;
  border: 1px solid #2c3846;
  border-radius: 10px;
  padding: 16px;
  text-align: center;
}
.metric-value {
  font-size: 36px;
  color: #e6a36c;
}
.metric-label {
  font-size: 12px;
  color: #8aa0b2;
}
.table-wrap {
  overflow-x: auto;
  overflow-y: auto;
  max-height: 260px;
}
.result-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 12px;
}
.result-table th,
.result-table td {
  border-bottom: 1px solid #2c3846;
  padding: 8px 10px;
  text-align: left;
  white-space: nowrap;
}
.result-table th {
  color: #8aa0b2;
  font-weight: 600;
}
.result-table tr:hover td {
  background: #121a23;
}
.graph-block {
  display: grid;
  gap: 10px;
  font-size: 12px;
}
.graph-list {
  background: #121a23;
  border-radius: 8px;
  padding: 10px;
  border: 1px solid #2c3846;
}
.footer {
  padding: 16px 24px;
  background: #141c24;
  border-top: 1px solid #2c3846;
  display: flex;
  gap: 12px;
  align-items: center;
}
.input {
  flex: 1;
  background: #1b2530;
  border: 1px solid #2c3846;
  border-radius: 8px;
  padding: 10px 12px;
  color: #e5ecf2;
  font-size: 13px;
  resize: none;
  line-height: 1.4;
  width: 100%;
  min-height: 56px;
  box-sizing: border-box;
}
.button {
  background: #4f9bd9;
  border: none;
  border-radius: 8px;
  padding: 10px 16px;
  color: #0f141b;
  font-weight: 600;
  cursor: pointer;
}
.button.secondary {
  background: #1b2530;
  color: #e5ecf2;
  border: 1px solid #2c3846;
}
.button:disabled {
  opacity: 0.5;
  cursor: default;
}
.suggestions {
  position: relative;
  flex: 1;
}
.suggestion-list {
  position: absolute;
  bottom: 54px;
  left: 0;
  right: 0;
  background: #121a23;
  border: 1px solid #2c3846;
  border-radius: 8px;
  padding: 6px;
  display: grid;
  gap: 6px;
  z-index: 10;
}
.suggestion-item {
  background: #1b2530;
  border: 1px solid #2c3846;
  border-radius: 6px;
  padding: 6px 10px;
  font-size: 12px;
  cursor: pointer;
}
.inspector {
  width: 320px;
  background: #121a23;
  border-left: 1px solid #2c3846;
  padding: 16px;
  display: flex;
  flex-direction: column;
  gap: 12px;
  overflow: auto;
}
.inspector.hidden {
  display: none;
}
.inspector-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
}
.inspector-title {
  font-size: 16px;
  font-weight: 700;
}
.inspector-section {
  font-size: 12px;
  color: #8aa0b2;
  text-transform: uppercase;
  letter-spacing: 0.06em;
}
.inspector-block {
  background: #0f141b;
  border: 1px solid #2c3846;
  border-radius: 8px;
  padding: 10px;
  font-size: 12px;
  white-space: pre-wrap;
}
.skeleton {
  height: 10px;
  border-radius: 6px;
  background: linear-gradient(90deg, #121a23, #1b2530, #121a23);
  background-size: 200% 100%;
  animation: shimmer 1.4s ease infinite;
}
@keyframes shimmer {
  0% { background-position: 0% 50%; }
  100% { background-position: 100% 50%; }
}
"#;

#[derive(Debug, Clone, Copy)]
pub enum DioxusRenderer {
    Desktop,
    Native,
}

#[derive(Clone)]
struct AppContext {
    runtime: Handle,
    backend: Arc<dyn GraphBackend>,
    translator: Arc<dyn Translator>,
    analyst: Arc<dyn Analyst>,
    cluster_state: SharedClusterState,
    shared: Arc<Mutex<SharedState>>,
    notify_tx: watch::Sender<u64>,
    notify_rx: watch::Receiver<u64>,
    cluster_label: String,
    backend_label: String,
    context_window_tokens: Option<usize>,
}

#[derive(Default, Clone)]
struct SharedState {
    feed: Vec<FeedItem>,
    next_id: u64,
    inspector: InspectorState,
    context_cutoff_id: u64,
    context_compact_summary: Option<String>,
    context_compact_usage: Option<LlmUsage>,
    context_compact_duration_ms: Option<u128>,
    context_compact_error: Option<String>,
    context_compacting: bool,
    table_sort: Option<TableSort>,
}

#[derive(Debug, Clone)]
struct FeedItem {
    id: u64,
    user_text: String,
    cypher: Option<String>,
    result: ResultPayload,
    state: FeedState,
    llm_usage: Option<LlmUsage>,
    llm_duration_ms: Option<u128>,
    exec_duration_ms: Option<u128>,
    analysis: Option<AnalysisResult>,
    analysis_duration_ms: Option<u128>,
    analysis_error: Option<String>,
    analysis_pending: bool,
    context_summary: Option<String>,
}

impl FeedItem {
    fn new(id: u64, user_text: String) -> Self {
        Self {
            id,
            user_text,
            cypher: None,
            result: ResultPayload::Empty,
            state: FeedState::Translating,
            llm_usage: None,
            llm_duration_ms: None,
            exec_duration_ms: None,
            analysis: None,
            analysis_duration_ms: None,
            analysis_error: None,
            analysis_pending: false,
            context_summary: None,
        }
    }
}

#[derive(Debug, Clone)]
enum FeedState {
    Translating,
    Validating,
    Running,
    Ready,
    Error(String),
}

#[derive(Debug, Clone)]
enum ResultPayload {
    Empty,
    Metric {
        label: String,
        value: String,
        unit: Option<String>,
    },
    List {
        rows: Vec<RowCard>,
    },
    Graph {
        nodes: Vec<GraphNode>,
        edges: Vec<GraphEdge>,
    },
    Raw {
        text: String,
    },
}

#[derive(Debug, Clone)]
struct RowCard {
    title: String,
    subtitle: Option<String>,
    status: Option<String>,
    fields: Vec<(String, String)>,
    raw_fields: Vec<(String, Value)>,
}

#[derive(Debug, Clone)]
struct GraphNode {
    label: String,
}

#[derive(Debug, Clone)]
struct GraphEdge {
    from: usize,
    to: usize,
    label: Option<String>,
}

#[derive(Default, Clone)]
struct InspectorState {
    is_open: bool,
    node_type: Option<String>,
    node_id: Option<String>,
    properties: Vec<InspectorProperty>,
    relationships: Vec<(String, String)>,
}

#[derive(Clone, Debug)]
struct InspectorProperty {
    key: String,
    value: InspectorValue,
}

#[derive(Clone, Debug)]
enum InspectorValue {
    Text(String),
    Json(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TableSort {
    item_id: u64,
    column: String,
    direction: SortDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortDirection {
    Asc,
    Desc,
}

impl SortDirection {
    fn toggle(self) -> Self {
        match self {
            SortDirection::Asc => SortDirection::Desc,
            SortDirection::Desc => SortDirection::Asc,
        }
    }
}

static APP_CONTEXT: OnceLock<AppContext> = OnceLock::new();

#[allow(clippy::too_many_arguments)]
pub fn run_gui_dioxus(
    runtime: &tokio::runtime::Runtime,
    renderer: DioxusRenderer,
    backend: Arc<dyn GraphBackend>,
    translator: Arc<dyn Translator>,
    analyst: Arc<dyn Analyst>,
    cluster_state: SharedClusterState,
    _token: CancellationToken,
    cluster_label: String,
    backend_label: String,
    context_window_tokens: Option<usize>,
) -> CliResult<()> {
    let (notify_tx, notify_rx) = watch::channel(0u64);
    let context = AppContext {
        runtime: runtime.handle().clone(),
        backend,
        translator,
        analyst,
        cluster_state,
        shared: Arc::new(Mutex::new(SharedState {
            feed: vec![],
            next_id: 1,
            inspector: InspectorState::default(),
            context_cutoff_id: 0,
            context_compact_summary: None,
            context_compact_usage: None,
            context_compact_duration_ms: None,
            context_compact_error: None,
            context_compacting: false,
            table_sort: None,
        })),
        notify_tx,
        notify_rx,
        cluster_label,
        backend_label,
        context_window_tokens,
    };
    APP_CONTEXT
        .set(context)
        .map_err(|_| std::io::Error::other("Dioxus app already initialized"))?;

    match renderer {
        DioxusRenderer::Desktop => {
            dioxus_desktop::launch::launch(app, Vec::new(), Vec::new());
        }
        DioxusRenderer::Native => {
            dioxus_native::launch(app);
        }
    }

    Ok(())
}

fn app() -> Element {
    let context = APP_CONTEXT
        .get()
        .expect("Dioxus context not initialized")
        .clone();
    use_context_provider(|| context);

    rsx! { AppShell {} }
}

#[component]
fn AppShell() -> Element {
    let context = use_context::<AppContext>();
    let refresh = use_signal(|| 0u64);

    use_hook({
        let mut rx = context.notify_rx.clone();
        let mut refresh = refresh;
        move || {
            spawn(async move {
                loop {
                    if rx.changed().await.is_err() {
                        break;
                    }
                    refresh.set(*rx.borrow());
                }
            });
        }
    });

    let mut input = use_signal(String::new);
    let _search = use_signal(String::new);

    let _ = *refresh.read();
    let snapshot = read_shared(&context);

    let input_value = input.read().clone();
    let suggestions = build_suggestions();
    let filtered_suggestions = filter_suggestions(&input_value, &suggestions);

    let counts = cluster_counts(&context.cluster_state);

    let (context_turns, _context_tokens, context_label) = build_context_stats(
        &snapshot,
        context.context_window_tokens,
        snapshot.context_compact_summary.as_deref(),
    );

    let context_can_compact = !snapshot.context_compacting && !context_turns.is_empty();

    let on_submit = {
        let context = context.clone();
        let mut input = input;
        move |_| {
            let question = input.read().trim().to_string();
            if question.is_empty() {
                return;
            }
            input.set(String::new());
            submit_question(&context, question);
        }
    };

    let on_keydown = {
        let context = context.clone();
        let mut input = input;
        move |evt: KeyboardEvent| {
            if evt.key() == Key::Enter && !evt.modifiers().shift() {
                evt.prevent_default();
                let question = input.read().trim().to_string();
                if question.is_empty() {
                    return;
                }
                input.set(String::new());
                submit_question(&context, question);
            }
        }
    };

    let input_rows = input_value.lines().count().clamp(2, 5).to_string();

    let on_reset_context = {
        let context = context.clone();
        move |_| reset_context(&context)
    };

    let on_compact_context = {
        let context = context.clone();
        move |_| start_context_compaction(&context)
    };

    rsx! {
        style { "{APP_CSS}" }
        div { class: "app",
            header { class: "header",
                div { class: "header-left",
                    h1 { "KubeGraph Ops (Dioxus)" }
                    div { class: "meta", "{context.cluster_label}" }
                    div { class: "meta", "Backend: {context.backend_label}" }
                }
                div { class: "pulse",
                    span { "Nodes {format_count(counts.node_count)}" }
                    span { "Props {format_count(counts.prop_count)}" }
                    span { "Pods {format_count(counts.pod_count)}" }
                    span { "Services {format_count(counts.service_count)}" }
                    span { "Namespaces {format_count(counts.namespace_count)}" }
                }
            }
            div { class: "layout",
                div { class: "sidebar",
                    div { class: "nav-btn", "H" }
                    div { class: "nav-btn", "S" }
                    div { class: "nav-btn", "A" }
                }
                div { class: "main",
                    div { class: "context-bar",
                        span { "{context_label}" }
                        div { class: "context-actions",
                            button {
                                class: "context-btn",
                                onclick: on_reset_context,
                                "Reset"
                            }
                            button {
                                class: "context-btn",
                                onclick: on_compact_context,
                                disabled: !context_can_compact,
                                if snapshot.context_compacting { "Compacting..." } else { "Compact" }
                            }
                        }
                    }
                    div { class: "feed",
                        if snapshot.feed.is_empty() {
                            div { class: "meta", "Ask a question to get started." }
                        }
                        for item in snapshot.feed.iter() {
                            {render_feed_card(item, &context)}
                        }
                    }
                    div { class: "footer",
                        div { class: "suggestions", style: "flex:1",
                            textarea {
                                class: "input",
                                placeholder: "Show me the services connected to these OOMing pods...",
                                value: "{input_value}",
                                oninput: move |evt| input.set(evt.value()),
                                onkeydown: on_keydown,
                                rows: "{input_rows}",
                            }
                            if !filtered_suggestions.is_empty() {
                                div { class: "suggestion-list",
                                    for suggestion in filtered_suggestions.iter() {
                                        div {
                                            class: "suggestion-item",
                                            onclick: {
                                                let mut input = input;
                                                let suggestion = suggestion.clone();
                                                move |_| {
                                                    let updated = replace_last_token(&input.read(), &suggestion);
                                                    input.set(updated);
                                                }
                                            },
                                            "{suggestion}"
                                        }
                                    }
                                }
                            }
                        }
                        button { class: "button", onclick: on_submit, disabled: input_value.trim().is_empty(), "RUN QUERY" }
                    }
                }
                div { class: if snapshot.inspector.is_open { "inspector" } else { "inspector hidden" },
                    {render_inspector_panel(&snapshot.inspector, &context)}
                }
            }
        }
    }
}

fn render_feed_card(item: &FeedItem, context: &AppContext) -> Element {
    let item = item.clone();
    let context = context.clone();
    let id = item.id;
    let cypher_block = item.cypher.as_ref().map(|cypher| {
        let spans = highlight_cypher_spans(cypher);
        let cypher_text = cypher.to_string();
        let context_for_run = context.clone();
        let run_action = move |_| {
            rerun_cypher(&context_for_run, id, cypher_text.clone());
        };
        rsx! {
            details { class: "cypher-block", open: true,
                summary { "Planned Query" }
                div { class: "cypher",
                    {spans.into_iter()}
                }
                div { class: "cypher-actions",
                    button { class: "button secondary", onclick: run_action, "Run" }
                }
            }
        }
    });

    let render_state = match &item.state {
        FeedState::Translating => "Translating...",
        FeedState::Validating => "Validating...",
        FeedState::Running => "Running...",
        FeedState::Ready => "Ready",
        FeedState::Error(_) => "Error",
    };

    rsx! {
        div { class: "card",
            div { class: "card-header",
                div { class: "question", "{item.user_text}" }
                div { class: "state", "{render_state}" }
            }
            {cypher_block}
            if item.llm_duration_ms.is_some() || item.exec_duration_ms.is_some() || item.llm_usage.is_some() {
                div { class: "meta",
                    if let Some(ms) = item.llm_duration_ms { span { "llm {format_duration(ms)}" } }
                    if let Some(ms) = item.exec_duration_ms { span { " · query {format_duration(ms)}" } }
                    if let Some(usage) = item.llm_usage.as_ref() {
                        span { " · tokens {usage.prompt_tokens}/{usage.completion_tokens}/{usage.total_tokens}" }
                    }
                }
            }
            match &item.state {
                FeedState::Translating | FeedState::Validating | FeedState::Running => rsx! {
                    div { class: "result",
                        div { class: "skeleton", style: "width: 60%" }
                        div { class: "skeleton", style: "width: 80%; margin-top: 8px" }
                        div { class: "skeleton", style: "width: 50%; margin-top: 8px" }
                    }
                },
                FeedState::Error(err) => rsx! { div { class: "error", "Error: {err}" } },
                FeedState::Ready => rsx! {
                    {render_analysis_block(&item)}
                    {render_result_block(&item, &context, item.id)}
                },
            }
        }
    }
}

fn render_analysis_block(item: &FeedItem) -> Element {
    if !item.analysis_pending && item.analysis.is_none() && item.analysis_error.is_none() {
        return rsx! {};
    }

    rsx! {
        div { class: "analysis",
            div { class: "analysis-title", "SRE Answer" }
            if item.analysis_pending {
                div { class: "meta", "Analyzing results..." }
                div { class: "skeleton", style: "width: 70%" }
                div { class: "skeleton", style: "width: 55%" }
            }
            if let Some(error) = item.analysis_error.as_ref() {
                div { class: "error", "Analysis error: {error}" }
            }
            if let Some(analysis) = item.analysis.as_ref() {
                div { class: "analysis-title", "{analysis.title}" }
                div { class: "question", "{analysis.summary}" }
                if !analysis.bullets.is_empty() {
                    for bullet in analysis.bullets.iter() {
                        div { class: "question", "• {bullet}" }
                    }
                }
                if !analysis.rows.is_empty() {
                    div { class: "analysis-title", "Highlights" }
                    {render_analysis_rows(&analysis.rows)}
                }
                if !analysis.follow_ups.is_empty() {
                    div { class: "analysis-title", "Follow-ups" }
                    for follow in analysis.follow_ups.iter() {
                        div { class: "question", "• {follow}" }
                    }
                }
                if item.analysis_duration_ms.is_some() || analysis.usage.is_some() || !analysis.confidence.is_empty() {
                    div { class: "meta",
                        if let Some(ms) = item.analysis_duration_ms { span { "analysis {format_duration(ms)}" } }
                        if !analysis.confidence.is_empty() { span { " · confidence {analysis.confidence}" } }
                        if let Some(usage) = analysis.usage.as_ref() {
                            span { " · tokens {usage.prompt_tokens}/{usage.completion_tokens}/{usage.total_tokens}" }
                            if let Some(cached) = usage.cached_tokens { span { " · cached {cached}" } }
                            if let Some(reasoning) = usage.reasoning_tokens { span { " · reasoning {reasoning}" } }
                        }
                    }
                }
            }
        }
    }
}

fn render_analysis_rows(rows: &[Value]) -> Element {
    let objects: Vec<&Map<String, Value>> = rows.iter().filter_map(|row| row.as_object()).collect();
    if objects.is_empty() {
        return rsx! { div { class: "meta", "No structured rows to display." } };
    }

    let mut columns: Vec<String> = objects[0].keys().cloned().collect();
    columns.sort();
    let max_rows = 10usize;
    let row_count = objects.len().min(max_rows);

    let header_nodes: Vec<Element> = columns
        .iter()
        .map(|label| rsx! { th { "{label}" } })
        .collect();

    let row_nodes: Vec<Element> = objects
        .iter()
        .take(row_count)
        .map(|row| {
            let values: Vec<String> = columns
                .iter()
                .map(|key| {
                    row.get(key)
                        .map(format_value)
                        .unwrap_or_else(|| "-".to_string())
                })
                .collect();
            rsx! {
                tr {
                    for value in values.iter() {
                        td { "{value}" }
                    }
                }
            }
        })
        .collect();

    rsx! {
        div { class: "result table-wrap",
            table { class: "result-table",
                thead { tr { {header_nodes.into_iter()} } }
                tbody { {row_nodes.into_iter()} }
            }
            if objects.len() > row_count {
                div { class: "meta", "Showing {row_count} of {objects.len()} rows." }
            }
        }
    }
}

fn render_result_block(item: &FeedItem, context: &AppContext, item_id: u64) -> Element {
    match &item.result {
        ResultPayload::Empty => rsx! { div { class: "meta", "No results returned." } },
        ResultPayload::Metric { label, value, unit } => {
            let mut text = value.clone();
            if let Some(unit) = unit {
                text = format!("{text} {unit}");
            }
            rsx! {
                div { class: "metric-card",
                    div { class: "metric-value", "{text}" }
                    div { class: "metric-label", "{label}" }
                }
            }
        }
        ResultPayload::List { rows } => render_table_block(rows, context, item_id),
        ResultPayload::Graph { nodes, edges } => {
            let edge_rows: Vec<Element> = edges
                .iter()
                .map(|edge| {
                    let label = edge.label.as_deref().unwrap_or("link");
                    let text = format!("{} -> {} ({})", edge.from, edge.to, label);
                    rsx! { div { class: "question", "{text}" } }
                })
                .collect();
            rsx! {
                div { class: "result graph-block",
                    div { class: "graph-list",
                        div { class: "meta", "Nodes" }
                        for (idx, node) in nodes.iter().enumerate() {
                            div { class: "question", "{idx}: {node.label}" }
                        }
                    }
                    div { class: "graph-list",
                        div { class: "meta", "Edges" }
                        {edge_rows.into_iter()}
                    }
                }
            }
        }
        ResultPayload::Raw { text } => rsx! { div { class: "result", pre { "{text}" } } },
    }
}

fn render_table_block(rows: &[RowCard], context: &AppContext, item_id: u64) -> Element {
    let spec = table_spec(rows);
    let extra_keys = spec.extra_keys.clone();
    let context = context.clone();
    let sort_state = {
        let shared = context.shared.lock().expect("shared state lock poisoned");
        shared
            .table_sort
            .as_ref()
            .filter(|sort| sort.item_id == item_id)
            .cloned()
    };
    let sorted_rows = sort_rows(rows, sort_state.as_ref());

    let header_defs = build_header_defs(&spec, &extra_keys, sort_state.as_ref());
    let header_nodes: Vec<Element> = header_defs
        .iter()
        .map(|header| {
            let context = context.clone();
            let column = header.column.clone();
            let label = header.label.clone();
            let on_click = move |_| toggle_table_sort(&context, item_id, column.clone());
            rsx! { th { onclick: on_click, "{label}" } }
        })
        .collect();

    let row_nodes: Vec<Element> = sorted_rows
        .iter()
        .map(|row| {
            let row_clone = row.clone();
            let row_fields = row.fields.clone();
            let extra_values: Vec<String> = extra_keys
                .iter()
                .map(|key| find_field(&row_fields, key).unwrap_or("-").to_string())
                .collect();
            let row_for_click = row_clone.clone();
            let on_click = {
                let context = context.clone();
                move |_| open_inspector_from_row(&context, &row_for_click)
            };
            let title = row_clone.title.clone();
            let namespace = row_clone
                .subtitle
                .clone()
                .unwrap_or_else(|| "-".to_string());
            let status = row_clone.status.clone().unwrap_or_else(|| "-".to_string());
            rsx! {
                tr { onclick: on_click,
                    if spec.show_title { td { "{title}" } }
                    if spec.show_namespace { td { "{namespace}" } }
                    if spec.show_status { td { "{status}" } }
                    for value in extra_values.iter() {
                        td { "{value}" }
                    }
                }
            }
        })
        .collect();

    rsx! {
        div { class: "result table-wrap",
            table { class: "result-table",
                thead {
                    tr {
                        {header_nodes.into_iter()}
                    }
                }
                tbody {
                    {row_nodes.into_iter()}
                }
            }
        }
    }
}

fn render_inspector_panel(inspector: &InspectorState, context: &AppContext) -> Element {
    if !inspector.is_open {
        return rsx! {};
    }

    let context = context.clone();
    let close = move |_| {
        update_shared(&context, |shared| shared.inspector.is_open = false);
    };

    rsx! {
        div { class: "inspector",
            div { class: "inspector-header",
                div { class: "inspector-title", "Node Inspector" }
                button { class: "button secondary", onclick: close, "X" }
            }
            if let Some(node_id) = inspector.node_id.as_ref() {
                div { class: "question", "{node_id}" }
            }
            if let Some(node_type) = inspector.node_type.as_ref() {
                div { class: "meta", "{node_type}" }
            }
            div { class: "inspector-section", "Properties" }
            if inspector.properties.is_empty() {
                div { class: "meta", "No properties loaded" }
            }
            for property in inspector.properties.iter() {
                match &property.value {
                    InspectorValue::Text(value) => rsx! {
                        div { class: "question", "{property.key}: {value}" }
                    },
                    InspectorValue::Json(value) => rsx! {
                        div { class: "meta", "{property.key}" }
                        div { class: "inspector-block", "{value}" }
                    },
                }
            }
            div { class: "inspector-section", "Relationships" }
            if inspector.relationships.is_empty() {
                div { class: "meta", "No relationships loaded" }
            }
            for (label, target) in inspector.relationships.iter() {
                div { class: "question", "→ {label} ({target})" }
            }
        }
    }
}

fn submit_question(context: &AppContext, question: String) {
    let id = {
        let mut shared = context.shared.lock().expect("shared state lock poisoned");
        let id = shared.next_id;
        shared.next_id += 1;
        shared.feed.push(FeedItem::new(id, question.clone()));
        id
    };
    notify(context);

    let context = context.clone();
    let runtime = context.runtime.clone();
    let backend = context.backend.clone();
    let translator = context.translator.clone();
    let analyst = context.analyst.clone();
    let analysis_context = build_context_with_budget(&context, &read_shared(&context));
    let analysis_summary = read_shared(&context).context_compact_summary.clone();

    runtime.spawn(async move {
        let mut attempt = 0usize;
        let mut feedback: Option<String> = None;

        loop {
            attempt += 1;
            update_feed_item(&context, id, |item| {
                item.state = FeedState::Translating;
                item.analysis = None;
                item.analysis_error = None;
                item.analysis_pending = false;
            });
            notify(&context);

            let llm_start = Instant::now();
            let result = translator
                .translate(&question, &[], None, feedback.as_deref())
                .await;
            let llm_ms = llm_start.elapsed().as_millis();

            let result = match result {
                Ok(result) => result,
                Err(err) => {
                    update_feed_item(&context, id, |item| {
                        item.state = FeedState::Error(err.to_string());
                        item.llm_duration_ms = Some(llm_ms);
                    });
                    notify(&context);
                    return;
                }
            };
            log_llm_call("translator", llm_ms, result.usage.as_ref());

            update_feed_item(&context, id, |item| {
                item.cypher = Some(result.cypher.clone());
                item.llm_usage = result.usage.clone();
                item.llm_duration_ms = Some(llm_ms);
                item.state = FeedState::Validating;
            });
            notify(&context);

            match validate_cypher(&result.cypher) {
                Ok(()) => {
                    let cypher = result.cypher.clone();
                    update_feed_item(&context, id, |item| {
                        item.state = FeedState::Running;
                    });
                    notify(&context);

                    let exec_start = Instant::now();
                    match backend.execute_query(cypher.clone()).await {
                        Ok(records) => {
                            let exec_ms = exec_start.elapsed().as_millis();
                            let summary = summarize_records(&records);
                            let classified = classify_result(&records);
                            update_feed_item(&context, id, |item| {
                                item.state = FeedState::Ready;
                                item.result = classified;
                                item.exec_duration_ms = Some(exec_ms);
                                item.context_summary = Some(summary.clone());
                            });
                            notify(&context);

                            update_feed_item(&context, id, |item| {
                                item.analysis_pending = true;
                            });
                            notify(&context);

                            let analysis_start = Instant::now();
                            match analyst
                                .analyze(
                                    &question,
                                    &cypher,
                                    &records,
                                    &summary,
                                    &analysis_context,
                                    analysis_summary.as_deref(),
                                )
                                .await
                            {
                                Ok(analysis) => {
                                    let analysis_ms = analysis_start.elapsed().as_millis();
                                    log_llm_call("analysis", analysis_ms, analysis.usage.as_ref());
                                    update_feed_item(&context, id, |item| {
                                        item.analysis = Some(analysis);
                                        item.analysis_duration_ms = Some(analysis_ms);
                                        item.analysis_pending = false;
                                        item.analysis_error = None;
                                    });
                                }
                                Err(err) => {
                                    let analysis_ms = analysis_start.elapsed().as_millis();
                                    update_feed_item(&context, id, |item| {
                                        item.analysis_error = Some(err.to_string());
                                        item.analysis_duration_ms = Some(analysis_ms);
                                        item.analysis_pending = false;
                                    });
                                }
                            }
                            notify(&context);
                        }
                        Err(err) => {
                            let exec_ms = exec_start.elapsed().as_millis();
                            update_feed_item(&context, id, |item| {
                                item.state = FeedState::Error(err.to_string());
                                item.exec_duration_ms = Some(exec_ms);
                            });
                            notify(&context);
                        }
                    }
                    return;
                }
                Err(issue) => {
                    if attempt <= LLM_MAX_RETRIES && issue.retriable() {
                        feedback = Some(issue.feedback());
                        continue;
                    }
                    update_feed_item(&context, id, |item| {
                        item.state = FeedState::Error(issue.to_string());
                    });
                    notify(&context);
                    return;
                }
            }
        }
    });
}

fn rerun_cypher(context: &AppContext, id: u64, cypher: String) {
    let context = context.clone();
    let runtime = context.runtime.clone();
    let backend = context.backend.clone();
    let analyst = context.analyst.clone();
    let question = {
        let shared = context.shared.lock().expect("shared state lock poisoned");
        shared
            .feed
            .iter()
            .find(|item| item.id == id)
            .map(|item| item.user_text.clone())
            .unwrap_or_default()
    };
    let analysis_context = build_context_with_budget(&context, &read_shared(&context));
    let analysis_summary = read_shared(&context).context_compact_summary.clone();

    runtime.spawn(async move {
        match validate_cypher(&cypher) {
            Ok(()) => {
                update_feed_item(&context, id, |item| {
                    item.state = FeedState::Running;
                    item.analysis = None;
                    item.analysis_error = None;
                    item.analysis_pending = false;
                });
                notify(&context);

                let exec_start = Instant::now();
                match backend.execute_query(cypher.clone()).await {
                    Ok(records) => {
                        let exec_ms = exec_start.elapsed().as_millis();
                        let summary = summarize_records(&records);
                        let classified = classify_result(&records);
                        update_feed_item(&context, id, |item| {
                            item.state = FeedState::Ready;
                            item.result = classified;
                            item.exec_duration_ms = Some(exec_ms);
                            item.context_summary = Some(summary.clone());
                        });
                        notify(&context);

                        update_feed_item(&context, id, |item| {
                            item.analysis_pending = true;
                        });
                        notify(&context);

                        let analysis_start = Instant::now();
                        match analyst
                            .analyze(
                                &question,
                                &cypher,
                                &records,
                                &summary,
                                &analysis_context,
                                analysis_summary.as_deref(),
                            )
                            .await
                        {
                            Ok(analysis) => {
                                let analysis_ms = analysis_start.elapsed().as_millis();
                                log_llm_call("analysis", analysis_ms, analysis.usage.as_ref());
                                update_feed_item(&context, id, |item| {
                                    item.analysis = Some(analysis);
                                    item.analysis_duration_ms = Some(analysis_ms);
                                    item.analysis_pending = false;
                                    item.analysis_error = None;
                                });
                            }
                            Err(err) => {
                                let analysis_ms = analysis_start.elapsed().as_millis();
                                update_feed_item(&context, id, |item| {
                                    item.analysis_error = Some(err.to_string());
                                    item.analysis_duration_ms = Some(analysis_ms);
                                    item.analysis_pending = false;
                                });
                            }
                        }
                        notify(&context);
                    }
                    Err(err) => {
                        let exec_ms = exec_start.elapsed().as_millis();
                        update_feed_item(&context, id, |item| {
                            item.state = FeedState::Error(err.to_string());
                            item.exec_duration_ms = Some(exec_ms);
                        });
                        notify(&context);
                    }
                }
            }
            Err(err) => {
                update_feed_item(&context, id, |item| {
                    item.state = FeedState::Error(err.to_string());
                });
                notify(&context);
            }
        }
    });
}

fn reset_context(context: &AppContext) {
    update_shared(context, |shared| {
        shared.context_cutoff_id = shared.next_id;
        shared.context_compact_summary = None;
        shared.context_compact_usage = None;
        shared.context_compact_duration_ms = None;
        shared.context_compact_error = None;
        shared.context_compacting = false;
    });
}

fn start_context_compaction(context: &AppContext) {
    let context = context.clone();
    let compact_context = {
        let mut shared = context.shared.lock().expect("shared state lock poisoned");
        if shared.context_compacting {
            return;
        }
        let context_turns = build_context(&shared, COMPACT_CONTEXT_LIMIT);
        if context_turns.is_empty() {
            shared.context_compact_error = Some("No context to compact.".to_string());
            notify(&context);
            return;
        }
        shared.context_compacting = true;
        shared.context_compact_error = None;
        context_turns
    };
    notify(&context);

    let runtime = context.runtime.clone();
    let analyst = context.analyst.clone();
    runtime.spawn(async move {
        let start = Instant::now();
        match analyst.compact_context(&compact_context).await {
            Ok(result) => {
                let duration_ms = start.elapsed().as_millis();
                update_shared(&context, |shared| {
                    shared.context_compacting = false;
                    shared.context_compact_summary = Some(result.summary);
                    shared.context_compact_usage = result.usage;
                    shared.context_compact_duration_ms = Some(duration_ms);
                    shared.context_compact_error = None;
                });
            }
            Err(err) => {
                update_shared(&context, |shared| {
                    shared.context_compacting = false;
                    shared.context_compact_error = Some(err.to_string());
                });
            }
        }
    });
}

fn open_inspector_from_row(context: &AppContext, row: &RowCard) {
    update_shared(context, |shared| {
        shared.inspector.is_open = true;
        shared.inspector.node_type = row
            .raw_fields
            .iter()
            .find(|(key, _)| key == "kind")
            .and_then(|(_, value)| value.as_str())
            .map(|value| value.to_string());
        shared.inspector.node_id = Some(row.title.clone());
        shared.inspector.properties = row
            .raw_fields
            .iter()
            .map(|(key, value)| InspectorProperty {
                key: key.clone(),
                value: inspector_value(value),
            })
            .collect();
        shared.inspector.relationships = vec![];
    });
}

fn notify(context: &AppContext) {
    context.notify_tx.send_modify(|value| *value += 1);
}

fn update_shared(context: &AppContext, update: impl FnOnce(&mut SharedState)) {
    let mut shared = context.shared.lock().expect("shared state lock poisoned");
    update(&mut shared);
    drop(shared);
    notify(context);
}

fn update_feed_item(context: &AppContext, id: u64, update: impl FnOnce(&mut FeedItem)) {
    let mut shared = context.shared.lock().expect("shared state lock poisoned");
    if let Some(item) = shared.feed.iter_mut().find(|item| item.id == id) {
        update(item);
    }
}

fn read_shared(context: &AppContext) -> SharedState {
    context
        .shared
        .lock()
        .expect("shared state lock poisoned")
        .clone()
}

fn build_context_stats(
    shared: &SharedState,
    context_window_tokens: Option<usize>,
    summary: Option<&str>,
) -> (Vec<ConversationTurn>, usize, String) {
    let turns = build_context_with_budget_shared(shared, context_window_tokens);
    let tokens = estimate_context_tokens(&turns, summary);
    let label = if let Some(budget) = context_budget_tokens(context_window_tokens) {
        format!("Context: {} • ~{} / ~{} tok", turns.len(), tokens, budget)
    } else {
        format!(
            "Context: {}/{} • ~{} tok",
            turns.len(),
            SHORT_TERM_CONTEXT_LIMIT,
            tokens
        )
    };
    (turns, tokens, label)
}

fn build_context_with_budget(context: &AppContext, shared: &SharedState) -> Vec<ConversationTurn> {
    build_context_with_budget_shared(shared, context.context_window_tokens)
}

fn build_context_with_budget_shared(
    shared: &SharedState,
    context_window_tokens: Option<usize>,
) -> Vec<ConversationTurn> {
    let Some(budget) = context_budget_tokens(context_window_tokens) else {
        return build_context(shared, SHORT_TERM_CONTEXT_LIMIT);
    };

    let summary_tokens = shared
        .context_compact_summary
        .as_deref()
        .map(estimate_text_tokens)
        .unwrap_or(0);
    let mut remaining = budget.saturating_sub(summary_tokens);
    let mut turns = Vec::new();

    for item in shared.feed.iter().rev() {
        if item.id < shared.context_cutoff_id {
            continue;
        }
        if !matches!(item.state, FeedState::Ready) {
            continue;
        }
        let Some(cypher) = &item.cypher else {
            continue;
        };
        let turn = ConversationTurn {
            question: item.user_text.clone(),
            cypher: cypher.clone(),
            result_summary: item.context_summary.clone(),
        };
        let turn_tokens = estimate_turn_tokens(&turn);
        if turn_tokens > remaining && !turns.is_empty() {
            break;
        }
        if turn_tokens <= remaining || turns.is_empty() {
            remaining = remaining.saturating_sub(turn_tokens);
            turns.push(turn);
        }
    }
    turns.reverse();
    turns
}

fn build_context(shared: &SharedState, limit: usize) -> Vec<ConversationTurn> {
    let mut turns = Vec::new();
    for item in shared.feed.iter().rev() {
        if turns.len() >= limit {
            break;
        }
        if item.id < shared.context_cutoff_id {
            continue;
        }
        if !matches!(item.state, FeedState::Ready) {
            continue;
        }
        let Some(cypher) = &item.cypher else {
            continue;
        };
        turns.push(ConversationTurn {
            question: item.user_text.clone(),
            cypher: cypher.clone(),
            result_summary: item.context_summary.clone(),
        });
    }
    turns.reverse();
    turns
}

fn context_budget_tokens(context_window_tokens: Option<usize>) -> Option<usize> {
    let total = context_window_tokens?;
    let budget = total.saturating_sub(CONTEXT_RESERVED_TOKENS);
    Some(budget.max(CONTEXT_MIN_TOKENS).min(total))
}

#[derive(Default, Clone)]
struct ClusterCounts {
    node_count: usize,
    prop_count: usize,
    pod_count: usize,
    service_count: usize,
    namespace_count: usize,
}

fn cluster_counts(state: &SharedClusterState) -> ClusterCounts {
    let guard = state.lock().expect("cluster state lock poisoned");
    let node_count = guard.get_node_count();
    let prop_count = estimate_property_count(&guard, node_count);
    let pod_count = guard.get_nodes_by_type(&ResourceType::Pod).count();
    let service_count = guard.get_nodes_by_type(&ResourceType::Service).count();
    let namespace_count = guard.get_nodes_by_type(&ResourceType::Namespace).count();
    ClusterCounts {
        node_count,
        prop_count,
        pod_count,
        service_count,
        namespace_count,
    }
}

fn classify_result(records: &[Value]) -> ResultPayload {
    if records.is_empty() {
        return ResultPayload::Empty;
    }

    if let Some(graph) = parse_graph_payload(records) {
        return graph;
    }

    if records.len() == 1 {
        if let Some(obj) = records[0].as_object() {
            if obj.len() == 1 {
                if let Some((label, value)) = obj.iter().next() {
                    let value_str = format_value(value);
                    return ResultPayload::Metric {
                        label: label.clone(),
                        value: value_str,
                        unit: None,
                    };
                }
            }
        }
    }

    if records.iter().all(|value| value.is_object()) {
        let rows = records
            .iter()
            .filter_map(|value| value.as_object())
            .map(summarize_row)
            .collect();
        return ResultPayload::List { rows };
    }

    ResultPayload::Raw {
        text: serde_json::to_string_pretty(records).unwrap_or_else(|_| "[]".to_string()),
    }
}

fn summarize_records(records: &[Value]) -> String {
    if records.is_empty() {
        return "rows=0".to_string();
    }

    let rows = records.len();
    let mut columns: Vec<String> = records
        .first()
        .and_then(|v| v.as_object())
        .map(|obj| obj.keys().cloned().collect())
        .unwrap_or_default();
    columns.sort();

    let mut summary = format!("rows={rows}");
    if !columns.is_empty() {
        summary.push_str(", columns=");
        summary.push_str(&columns.join(","));
    }

    let samples: Vec<String> = records
        .iter()
        .take(2)
        .map(|value| summarize_record(value, &columns))
        .collect();
    if !samples.is_empty() {
        summary.push_str("; sample=");
        summary.push_str(&samples.join(" | "));
    }

    truncate_text(&summary, 400)
}

fn summarize_record(value: &Value, columns: &[String]) -> String {
    if let Some(obj) = value.as_object() {
        let keys: Vec<String> = if columns.is_empty() {
            let mut keys: Vec<String> = obj.keys().cloned().collect();
            keys.sort();
            keys
        } else {
            columns.to_vec()
        };
        let mut parts = Vec::new();
        for key in keys.into_iter().take(6) {
            let entry = obj
                .get(&key)
                .map(format_value)
                .unwrap_or_else(|| "null".to_string());
            parts.push(format!("{key}={}", truncate_text(&entry, 60)));
        }
        return parts.join(", ");
    }

    truncate_text(&format_value(value), 120)
}

fn truncate_text(text: &str, max_len: usize) -> String {
    if text.len() <= max_len {
        return text.to_string();
    }
    let mut trimmed = text[..max_len.saturating_sub(3)].to_string();
    trimmed.push_str("...");
    trimmed
}

fn parse_graph_payload(records: &[Value]) -> Option<ResultPayload> {
    if records.len() != 1 {
        return None;
    }
    let obj = records[0].as_object()?;
    let nodes_val = obj.get("nodes")?;
    let edges_val = obj.get("edges")?;
    let nodes_arr = nodes_val.as_array()?;
    let edges_arr = edges_val.as_array()?;

    let nodes: Vec<GraphNode> = nodes_arr
        .iter()
        .enumerate()
        .map(|(idx, value)| {
            let label = value
                .get("label")
                .and_then(|v| v.as_str())
                .map(|v| v.to_string())
                .or_else(|| {
                    value
                        .get("name")
                        .and_then(|v| v.as_str())
                        .map(|v| v.to_string())
                })
                .unwrap_or_else(|| format!("Node {idx}"));
            GraphNode { label }
        })
        .collect();

    let edges: Vec<GraphEdge> = edges_arr
        .iter()
        .filter_map(|value| {
            let from = value.get("from")?.as_u64()? as usize;
            let to = value.get("to")?.as_u64()? as usize;
            let label = value
                .get("label")
                .and_then(|v| v.as_str())
                .map(|v| v.to_string());
            Some(GraphEdge { from, to, label })
        })
        .collect();

    Some(ResultPayload::Graph { nodes, edges })
}

fn summarize_row(obj: &Map<String, Value>) -> RowCard {
    let title = obj
        .get("metadata_name")
        .and_then(|v| v.as_str())
        .or_else(|| obj.get("name").and_then(|v| v.as_str()))
        .or_else(|| obj.get("namespace").and_then(|v| v.as_str()))
        .unwrap_or("Row")
        .to_string();
    let subtitle = obj
        .get("metadata_namespace")
        .and_then(|v| v.as_str())
        .map(|v| v.to_string());
    let status = obj
        .get("status")
        .and_then(|v| v.as_str())
        .or_else(|| obj.get("phase").and_then(|v| v.as_str()))
        .or_else(|| {
            obj.get("status")
                .and_then(|v| v.as_object())
                .and_then(|status| status.get("phase"))
                .and_then(|v| v.as_str())
        })
        .or_else(|| {
            obj.get("status")
                .and_then(|v| v.as_object())
                .and_then(|status| status.get("reason"))
                .and_then(|v| v.as_str())
        })
        .map(|v| v.to_string());

    let mut raw_fields: Vec<(String, Value)> =
        obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    raw_fields.sort_by(|a, b| a.0.cmp(&b.0));

    let fields: Vec<(String, String)> = raw_fields
        .iter()
        .map(|(k, v)| (k.clone(), format_value(v)))
        .collect();

    RowCard {
        title,
        subtitle,
        status,
        fields,
        raw_fields,
    }
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(v) => v.to_string(),
        Value::Number(v) => v.to_string(),
        Value::String(v) => v.clone(),
        Value::Array(arr) => format_array_value(arr),
        Value::Object(obj) => format!("object({})", obj.len()),
    }
}

fn format_array_value(arr: &[Value]) -> String {
    if arr.is_empty() {
        return "[]".to_string();
    }

    let max_items = 6usize;
    let mut parts = Vec::new();
    for value in arr.iter().take(max_items) {
        parts.push(format_array_item(value));
    }

    let mut out = parts.join(", ");
    if arr.len() > max_items {
        out.push_str(&format!(", ... (+{})", arr.len() - max_items));
    }

    format!("[{out}]")
}

fn format_array_item(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(v) => v.to_string(),
        Value::Number(v) => v.to_string(),
        Value::String(v) => v.clone(),
        Value::Array(arr) => format!("array({})", arr.len()),
        Value::Object(obj) => format!("object({})", obj.len()),
    }
}

fn inspector_value(value: &Value) -> InspectorValue {
    match value {
        Value::Array(_) | Value::Object(_) => {
            let pretty =
                serde_json::to_string_pretty(value).unwrap_or_else(|_| format_value(value));
            InspectorValue::Json(pretty)
        }
        _ => InspectorValue::Text(format_value(value)),
    }
}

fn find_field<'a>(fields: &'a [(String, String)], key: &str) -> Option<&'a str> {
    fields
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

fn build_suggestions() -> Vec<String> {
    let mut suggestions: Vec<String> = ResourceType::iter().map(|r| r.to_string()).collect();
    suggestions.extend(vec![
        "OOMKilledState".to_string(),
        "Pod".to_string(),
        "Container".to_string(),
        "Namespace".to_string(),
        "Deployment".to_string(),
        "ReplicaSet".to_string(),
        "DaemonSet".to_string(),
        "Service".to_string(),
        "Ingress".to_string(),
    ]);
    suggestions.sort();
    suggestions.dedup();
    suggestions
}

fn current_token(input: &str) -> String {
    input
        .split(|c: char| c.is_whitespace() || c == ',' || c == '(' || c == ')' || c == ':')
        .next_back()
        .unwrap_or("")
        .to_string()
}

fn replace_last_token(input: &str, suggestion: &str) -> String {
    let mut parts: Vec<&str> = input
        .split(|c: char| c.is_whitespace() || c == ',' || c == '(' || c == ')' || c == ':')
        .collect();
    if parts.is_empty() {
        return suggestion.to_string();
    }
    let last_token = parts.pop().unwrap_or("");
    let prefix_len = input.len().saturating_sub(last_token.len());
    let prefix = &input[..prefix_len];
    format!("{prefix}{suggestion} ")
}

fn filter_suggestions(input: &str, suggestions: &[String]) -> Vec<String> {
    let token = current_token(input);
    if token.is_empty() {
        return Vec::new();
    }
    let token_lower = token.to_lowercase();
    suggestions
        .iter()
        .filter(|suggestion| suggestion.to_lowercase().starts_with(&token_lower))
        .take(6)
        .cloned()
        .collect()
}

fn estimate_property_count(state: &ClusterState, node_count: usize) -> usize {
    let sample_size = 200usize.min(node_count.max(1));
    let mut total = 0usize;
    let mut count = 0usize;
    for node in state.get_nodes().take(sample_size) {
        if let Ok(value) = serde_json::to_value(node) {
            total += count_json_properties(&value);
            count += 1;
        }
    }
    if count == 0 {
        return 0;
    }
    let avg = total as f64 / count as f64;
    (avg * node_count as f64) as usize
}

fn count_json_properties(value: &Value) -> usize {
    match value {
        Value::Object(map) => map.len() + map.values().map(count_json_properties).sum::<usize>(),
        Value::Array(arr) => arr.iter().map(count_json_properties).sum(),
        _ => 0,
    }
}

fn format_count(value: usize) -> String {
    let digits = value.to_string();
    let mut out = String::new();
    for (idx, ch) in digits.chars().rev().enumerate() {
        if idx > 0 && idx % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out.chars().rev().collect()
}

fn table_spec(rows: &[RowCard]) -> TableSpec {
    let mut extra_keys: Vec<String> = Vec::new();
    if let Some(first) = rows.first() {
        for (key, _) in &first.fields {
            if key == "metadata_name"
                || key == "metadata_namespace"
                || key == "status"
                || key == "phase"
                || key == "kind"
            {
                continue;
            }
            extra_keys.push(key.clone());
        }
    }

    let show_title = rows.iter().any(|r| r.title != "Row");
    let show_namespace = rows.iter().any(|r| r.subtitle.is_some());
    let show_status = rows.iter().any(|r| r.status.is_some());

    TableSpec {
        show_title,
        show_namespace,
        show_status,
        extra_keys,
    }
}

struct TableSpec {
    show_title: bool,
    show_namespace: bool,
    show_status: bool,
    extra_keys: Vec<String>,
}

#[derive(Clone)]
struct HeaderDef {
    column: String,
    label: String,
}

fn build_header_defs(
    spec: &TableSpec,
    extra_keys: &[String],
    sort: Option<&TableSort>,
) -> Vec<HeaderDef> {
    let mut headers = Vec::new();
    if spec.show_title {
        headers.push(header_def("Name", sort));
    }
    if spec.show_namespace {
        headers.push(header_def("Namespace", sort));
    }
    if spec.show_status {
        headers.push(header_def("Status", sort));
    }
    for key in extra_keys {
        headers.push(header_def(key, sort));
    }
    headers
}

fn header_def(column: &str, sort: Option<&TableSort>) -> HeaderDef {
    let label = if let Some(sort) = sort {
        if sort.column == column {
            match sort.direction {
                SortDirection::Asc => format!("{column} ▲"),
                SortDirection::Desc => format!("{column} ▼"),
            }
        } else {
            column.to_string()
        }
    } else {
        column.to_string()
    };
    HeaderDef {
        column: column.to_string(),
        label,
    }
}

fn toggle_table_sort(context: &AppContext, item_id: u64, column: String) {
    update_shared(context, |shared| {
        let next = match &shared.table_sort {
            Some(sort) if sort.item_id == item_id && sort.column == column => TableSort {
                item_id,
                column: column.clone(),
                direction: sort.direction.toggle(),
            },
            _ => TableSort {
                item_id,
                column: column.clone(),
                direction: SortDirection::Asc,
            },
        };
        shared.table_sort = Some(next);
    });
}

fn sort_rows(rows: &[RowCard], sort: Option<&TableSort>) -> Vec<RowCard> {
    let mut rows = rows.to_vec();
    if let Some(sort) = sort {
        rows.sort_by(|a, b| {
            let left = row_sort_value(a, &sort.column);
            let right = row_sort_value(b, &sort.column);
            let cmp = left.cmp(&right);
            match sort.direction {
                SortDirection::Asc => cmp,
                SortDirection::Desc => cmp.reverse(),
            }
        });
    }
    rows
}

fn row_sort_value(row: &RowCard, column: &str) -> String {
    let value = match column {
        "Name" => row.title.clone(),
        "Namespace" => row.subtitle.clone().unwrap_or_default(),
        "Status" => row.status.clone().unwrap_or_default(),
        _ => find_field(&row.fields, column).unwrap_or("").to_string(),
    };
    value.to_lowercase()
}

fn format_duration(ms: u128) -> String {
    if ms >= 1000 {
        format!("{:.2}s", ms as f64 / 1000.0)
    } else {
        format!("{ms} ms")
    }
}

fn highlight_cypher_spans(text: &str) -> Vec<Element> {
    let keywords = [
        "MATCH", "RETURN", "WHERE", "AND", "OR", "AS", "IN", "LIMIT", "ORDER", "BY", "SKIP",
        "WITH", "UNWIND", "CALL", "YIELD", "CREATE", "DELETE", "SET", "REMOVE", "MERGE",
        "DISTINCT", "COUNT",
    ];
    text.split_inclusive(|c: char| !c.is_alphanumeric() && c != '_')
        .map(|part| {
            let trimmed = part.trim();
            let word = trimmed.trim_matches(|c: char| !c.is_alphanumeric() && c != '_');
            let upper = word.to_uppercase();
            let class = if !word.is_empty() && keywords.contains(&upper.as_str()) {
                "cypher-keyword"
            } else if trimmed.contains('"') || trimmed.contains('\'') {
                "cypher-string"
            } else {
                "cypher-text"
            };
            let content = part.to_string();
            rsx! { span { class: "{class}", "{content}" } }
        })
        .collect()
}

fn log_llm_call(label: &str, duration_ms: u128, usage: Option<&LlmUsage>) {
    if let Some(usage) = usage {
        tracing::info!(
            "{label} LLM ({duration_ms} ms) tokens prompt={} completion={} total={} cached={:?} reasoning={:?}",
            usage.prompt_tokens,
            usage.completion_tokens,
            usage.total_tokens,
            usage.cached_tokens,
            usage.reasoning_tokens
        );
    } else {
        tracing::info!("{label} LLM ({duration_ms} ms)");
    }
}

fn estimate_text_tokens(text: &str) -> usize {
    let chars = text.len();
    if chars == 0 {
        0
    } else {
        (chars / 4).max(1)
    }
}

fn estimate_turn_tokens(turn: &ConversationTurn) -> usize {
    let mut tokens = estimate_text_tokens(&turn.question);
    tokens += estimate_text_tokens(&turn.cypher);
    if let Some(summary) = &turn.result_summary {
        tokens += estimate_text_tokens(summary);
    }
    tokens
}

fn estimate_context_tokens(turns: &[ConversationTurn], summary: Option<&str>) -> usize {
    let mut tokens = 0usize;
    for turn in turns {
        tokens += estimate_turn_tokens(turn);
    }
    if let Some(summary) = summary {
        tokens += estimate_text_tokens(summary);
    }
    tokens
}
