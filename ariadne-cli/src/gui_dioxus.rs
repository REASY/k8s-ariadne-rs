use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use dioxus::prelude::*;
use serde_json::{Map, Value};
use tokio::runtime::Handle;
use tokio::sync::watch;

use ariadne_core::cypher_validation::validate_cypher;
use ariadne_core::graph_backend::GraphBackend;
use ariadne_core::query_issue::classify_ariadne_error;
use ariadne_core::state::SharedClusterState;
use ariadne_core::types::ResourceType;

use crate::agent::{
    Agentic, Analyst, ConversationTurn, LlmUsage, RouteDecision, Router, Translator,
};
use crate::error::CliResult;
use crate::gui_results::{
    build_suggestions, classify_result, current_token, estimate_property_count,
    extract_context_bindings, find_field, format_count, format_value, inspector_value,
    merge_params, replace_last_token, summarize_records,
};
use crate::gui_shared::{
    FeedItem, FeedState, InspectorProperty, InspectorState, InspectorValue, ResultPayload, RowCard,
    UsageAccumulator, estimate_context_tokens, estimate_text_tokens, estimate_turn_tokens,
    format_duration, log_llm_call,
};

#[path = "gui_dioxus/components.rs"]
mod components;
use components::{render_feed_card, render_inspector_panel};
#[path = "gui_dioxus/workflow.rs"]
mod workflow;
use workflow::{
    open_inspector_from_row, rerun_cypher, reset_context, start_context_compaction, submit_question,
};

const SHORT_TERM_CONTEXT_LIMIT: usize = 4;
const COMPACT_CONTEXT_LIMIT: usize = 12;
const CONTEXT_RESERVED_TOKENS: usize = 2048;
const CONTEXT_MIN_TOKENS: usize = 512;
const LLM_MAX_RETRIES: usize = 1;

const APP_CSS: &str = include_str!("gui_dioxus/style.css");

#[derive(Debug, Clone, Copy)]
pub enum DioxusRenderer {
    Desktop,
    Native,
}

pub struct DioxusGuiArgs {
    pub runtime_handle: tokio::runtime::Handle,
    pub renderer: DioxusRenderer,
    pub backend: Arc<dyn GraphBackend>,
    pub translator: Arc<dyn Translator>,
    pub router: Arc<dyn Router>,
    pub agentic: Arc<dyn Agentic>,
    pub analyst: Arc<dyn Analyst>,
    pub cluster_state: SharedClusterState,
    pub cluster_label: String,
    pub backend_label: String,
    pub context_window_tokens: Option<usize>,
}

#[derive(Clone)]
struct AppContext {
    runtime: Handle,
    backend: Arc<dyn GraphBackend>,
    translator: Arc<dyn Translator>,
    router: Arc<dyn Router>,
    agentic: Arc<dyn Agentic>,
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

pub fn run_gui_dioxus(args: DioxusGuiArgs) -> CliResult<()> {
    let (notify_tx, notify_rx) = watch::channel(0u64);
    let context = AppContext {
        runtime: args.runtime_handle.clone(),
        backend: args.backend,
        translator: args.translator,
        router: args.router,
        agentic: args.agentic,
        analyst: args.analyst,
        cluster_state: args.cluster_state,
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
        cluster_label: args.cluster_label,
        backend_label: args.backend_label,
        context_window_tokens: args.context_window_tokens,
    };
    APP_CONTEXT
        .set(context)
        .map_err(|_| std::io::Error::other("Dioxus app already initialized"))?;

    match args.renderer {
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
        document::Title { "Ariadne " }
        style { "{APP_CSS}" }
        div { class: "app",
            header { class: "header",
                div { class: "header-left",
                    h1 { "Ariadne (Dioxus)" }
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
            bindings: item.context_bindings.clone(),
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
            bindings: item.context_bindings.clone(),
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
