use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use eframe::egui;
use eframe::egui::{
    text::LayoutJob, Align, Align2, Color32, CornerRadius, FontFamily, FontId, Frame, Layout,
    Margin, RichText, ScrollArea, Stroke, TextEdit, TextFormat, TextStyle, Vec2,
};
use egui_extras::{Column, TableBuilder};
use serde_json::{Map, Value};
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;

use ariadne_core::graph_backend::GraphBackend;
use ariadne_core::state::SharedClusterState;
use ariadne_core::types::ResourceType;
use strum::IntoEnumIterator;

use crate::agent::{AnalysisResult, Analyst, ConversationTurn, LlmUsage, Translator};
use crate::error::CliResult;
use crate::validation::validate_cypher;

const SHORT_TERM_CONTEXT_LIMIT: usize = 4;
const COMPACT_CONTEXT_LIMIT: usize = 12;
const CONTEXT_RESERVED_TOKENS: usize = 2048;
const CONTEXT_MIN_TOKENS: usize = 512;
const GRAPH_PULSE_HEIGHT: f32 = 40.0;
const LLM_MAX_RETRIES: usize = 1;

pub struct GuiArgs {
    pub runtime_handle: tokio::runtime::Handle,
    pub backend: Arc<dyn GraphBackend>,
    pub translator: Arc<dyn Translator>,
    pub analyst: Arc<dyn Analyst>,
    pub cluster_state: SharedClusterState,
    pub token: CancellationToken,
    pub cluster_label: String,
    pub backend_label: String,
    pub context_window_tokens: Option<usize>,
}

pub fn run_gui(args: GuiArgs) -> CliResult<()> {
    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([1400.0, 900.0]),
        ..Default::default()
    };
    let runtime_handle = args.runtime_handle.clone();
    let backend = args.backend.clone();
    let translator = args.translator.clone();
    let cluster_state = args.cluster_state.clone();
    let token = args.token.clone();
    let cluster_label = args.cluster_label.clone();
    eframe::run_native(
        "Ariadne",
        native_options,
        Box::new(|cc| {
            let palette = Palette::default();
            setup_style(&cc.egui_ctx, &palette);
            Ok(Box::new(GuiApp::new(
                runtime_handle.clone(),
                backend.clone(),
                translator.clone(),
                args.analyst.clone(),
                cluster_state.clone(),
                token.clone(),
                cluster_label.clone(),
                args.backend_label.clone(),
                args.context_window_tokens,
                cc.egui_ctx.clone(),
            )))
        }),
    )
    .map_err(|err| std::io::Error::other(err.to_string()))?;
    Ok(())
}

#[derive(Clone)]
struct ClusterMeta {
    label: String,
    connected: bool,
    backend_label: String,
}

#[derive(Clone)]
struct Palette {
    bg_primary: Color32,
    bg_panel: Color32,
    bg_elevated: Color32,
    accent: Color32,
    accent_warm: Color32,
    success: Color32,
    danger: Color32,
    text_primary: Color32,
    text_muted: Color32,
    border: Color32,
    keyword: Color32,
    string: Color32,
    spark_nodes: Color32,
    spark_props: Color32,
    spark_pods: Color32,
    spark_services: Color32,
    spark_namespaces: Color32,
}

impl Default for Palette {
    fn default() -> Self {
        Self {
            bg_primary: Color32::from_rgb(0x0F, 0x14, 0x1B),
            bg_panel: Color32::from_rgb(0x14, 0x1C, 0x24),
            bg_elevated: Color32::from_rgb(0x1B, 0x25, 0x30),
            accent: Color32::from_rgb(0x4F, 0x9B, 0xD9),
            accent_warm: Color32::from_rgb(0xE6, 0xA3, 0x6C),
            success: Color32::from_rgb(0x6A, 0xD3, 0x9F),
            danger: Color32::from_rgb(0xE7, 0x6F, 0x51),
            text_primary: Color32::from_rgb(0xE5, 0xEC, 0xF2),
            text_muted: Color32::from_rgb(0x9A, 0xA8, 0xB7),
            border: Color32::from_rgb(0x2C, 0x38, 0x46),
            keyword: Color32::from_rgb(0xE6, 0xA3, 0x6C),
            string: Color32::from_rgb(0x8B, 0xD3, 0xFF),
            spark_nodes: Color32::from_rgb(0xE2, 0x8B, 0x8B),
            spark_props: Color32::from_rgb(0xB8, 0x8B, 0xF5),
            spark_pods: Color32::from_rgb(0x6B, 0xB5, 0xF5),
            spark_services: Color32::from_rgb(0x7A, 0xD9, 0xA5),
            spark_namespaces: Color32::from_rgb(0x7D, 0xC4, 0xFF),
        }
    }
}

// ... (FeedState, ResultPayload, etc. unchanged) ...

fn lighten_color(color: Color32, factor: f32) -> Color32 {
    color.gamma_multiply(factor)
}

fn setup_style(ctx: &egui::Context, palette: &Palette) {
    let mut visuals = egui::Visuals::dark();
    visuals.panel_fill = palette.bg_panel;
    visuals.window_fill = palette.bg_primary;
    visuals.faint_bg_color = lighten_color(palette.bg_panel, 1.04);
    visuals.extreme_bg_color = palette.bg_elevated; // Inputs background
    visuals.text_edit_bg_color = Some(palette.bg_elevated);
    visuals.code_bg_color = lighten_color(palette.bg_primary, 1.08);
    visuals.widgets.noninteractive.bg_fill = palette.bg_panel;
    visuals.widgets.noninteractive.bg_stroke = Stroke::new(1.0, palette.border);
    visuals.widgets.inactive.bg_fill = palette.bg_elevated; // Buttons/Cards default
    visuals.widgets.active.bg_fill = lighten_color(palette.bg_elevated, 1.08);
    visuals.widgets.hovered.bg_fill = lighten_color(palette.bg_elevated, 1.06);
    visuals.selection.bg_fill = palette.accent.gamma_multiply(0.3);
    visuals.selection.stroke = Stroke::new(1.0, palette.accent);
    visuals.override_text_color = Some(palette.text_primary);
    visuals.weak_text_color = Some(palette.text_muted);
    visuals.hyperlink_color = palette.accent;
    visuals.warn_fg_color = palette.accent_warm;
    visuals.error_fg_color = palette.danger;
    visuals.striped = true;

    // Borders & Rounding
    visuals.widgets.inactive.bg_stroke = Stroke::new(1.0, palette.border);
    visuals.widgets.active.bg_stroke = Stroke::new(1.0, palette.accent);
    visuals.widgets.hovered.bg_stroke = Stroke::new(1.5, palette.text_muted);
    visuals.widgets.noninteractive.corner_radius = CornerRadius::same(6);
    visuals.widgets.inactive.corner_radius = CornerRadius::same(6);
    visuals.widgets.hovered.corner_radius = CornerRadius::same(6);
    visuals.widgets.active.corner_radius = CornerRadius::same(6);
    visuals.window_corner_radius = CornerRadius::same(10);
    visuals.window_stroke = Stroke::new(1.0, palette.border);
    visuals.window_shadow = egui::Shadow {
        offset: [10, 18],
        blur: 16,
        spread: 0,
        color: Color32::from_black_alpha(110),
    };
    visuals.popup_shadow = egui::Shadow {
        offset: [6, 10],
        blur: 10,
        spread: 0,
        color: Color32::from_black_alpha(110),
    };
    visuals.menu_corner_radius = CornerRadius::same(8);
    visuals.button_frame = true;
    visuals.collapsing_header_frame = true;
    visuals.indent_has_left_vline = false;

    ctx.set_visuals(visuals);

    let mut style = (*ctx.style()).clone();
    style.text_styles.insert(
        TextStyle::Heading,
        FontId::new(18.0, FontFamily::Proportional),
    );
    style
        .text_styles
        .insert(TextStyle::Body, FontId::new(13.0, FontFamily::Proportional));
    style.text_styles.insert(
        TextStyle::Small,
        FontId::new(11.0, FontFamily::Proportional),
    );
    style.text_styles.insert(
        TextStyle::Monospace,
        FontId::new(12.0, FontFamily::Monospace),
    );
    style.text_styles.insert(
        TextStyle::Button,
        FontId::new(13.0, FontFamily::Proportional),
    );
    style.spacing.item_spacing = Vec2::new(10.0, 10.0);
    style.spacing.window_margin = Margin::same(12);
    style.spacing.button_padding = Vec2::new(8.0, 5.0);
    ctx.set_style(style);
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

enum AppEvent {
    TranslationStarted {
        id: u64,
    },
    TranslationCompleted {
        id: u64,
        cypher: String,
        usage: Option<LlmUsage>,
        duration_ms: u128,
    },
    TranslationFailed {
        id: u64,
        error: String,
    },
    ValidationFailed {
        id: u64,
        error: String,
        cypher: String,
    },
    QueryStarted {
        id: u64,
        cypher: String,
    },
    QueryCompleted {
        id: u64,
        cypher: String,
        records: Vec<Value>,
        duration_ms: u128,
    },
    QueryFailed {
        id: u64,
        error: String,
        cypher: String,
        duration_ms: u128,
    },
    AnalysisStarted {
        id: u64,
    },
    AnalysisCompleted {
        id: u64,
        analysis: AnalysisResult,
        duration_ms: u128,
    },
    AnalysisFailed {
        id: u64,
        error: String,
        duration_ms: u128,
    },
    ContextCompactionStarted,
    ContextCompactionCompleted {
        summary: String,
        usage: Option<LlmUsage>,
        duration_ms: u128,
    },
    ContextCompactionFailed {
        error: String,
    },
}

pub struct GuiApp {
    runtime: Handle,
    backend: Arc<dyn GraphBackend>,
    translator: Arc<dyn Translator>,
    analyst: Arc<dyn Analyst>,
    cluster_state: SharedClusterState,
    cluster_meta: ClusterMeta,
    token: CancellationToken,
    egui_ctx: egui::Context,
    palette: Palette,
    feed: Vec<FeedItem>,
    next_id: u64,
    input: String,
    search: String,
    input_rect: Option<egui::Rect>,
    suggestions: Vec<String>,
    filtered_suggestions: Vec<String>,
    events_tx: mpsc::Sender<AppEvent>,
    events_rx: mpsc::Receiver<AppEvent>,
    inspector: InspectorState,
    pulse_nodes: Vec<f64>,
    pulse_props: Vec<f64>,
    pulse_pods: Vec<f64>,
    pulse_services: Vec<f64>,
    pulse_namespaces: Vec<f64>,
    last_pulse_update: Instant,
    context_cutoff_id: u64,
    context_compact_summary: Option<String>,
    context_compact_usage: Option<LlmUsage>,
    context_compact_duration_ms: Option<u128>,
    context_compact_error: Option<String>,
    context_compacting: bool,
    context_window_tokens: Option<usize>,
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

impl GuiApp {
    #[allow(clippy::too_many_arguments)]
    fn new(
        runtime: Handle,
        backend: Arc<dyn GraphBackend>,
        translator: Arc<dyn Translator>,
        analyst: Arc<dyn Analyst>,
        cluster_state: SharedClusterState,
        token: CancellationToken,
        cluster_label: String,
        backend_label: String,
        context_window_tokens: Option<usize>,
        egui_ctx: egui::Context,
    ) -> Self {
        let (events_tx, events_rx) = mpsc::channel();
        let suggestions = build_suggestions();
        let palette = Palette::default();
        Self {
            runtime,
            backend,
            translator,
            analyst,
            cluster_state,
            cluster_meta: ClusterMeta {
                label: cluster_label,
                connected: true,
                backend_label,
            },
            token,
            egui_ctx,
            palette,
            feed: Vec::new(),
            next_id: 1,
            input: String::new(),
            search: String::new(),
            input_rect: None,
            suggestions,
            filtered_suggestions: Vec::new(),
            events_tx,
            events_rx,
            inspector: InspectorState::default(),
            pulse_nodes: vec![],
            pulse_props: vec![],
            pulse_pods: vec![],
            pulse_services: vec![],
            pulse_namespaces: vec![],
            last_pulse_update: Instant::now() - Duration::from_secs(10),
            context_cutoff_id: 0,
            context_compact_summary: None,
            context_compact_usage: None,
            context_compact_duration_ms: None,
            context_compact_error: None,
            context_compacting: false,
            context_window_tokens,
        }
    }

    fn submit_question(&mut self) {
        let question = self.input.trim().to_string();
        if question.is_empty() {
            return;
        }

        if self.handle_slash_command(&question) {
            self.input.clear();
            return;
        }

        let id = self.next_id;
        self.next_id += 1;
        self.feed.push(FeedItem::new(id, question.clone()));
        self.input.clear();

        let tx = self.events_tx.clone();
        let translator = self.translator.clone();
        let analyst = self.analyst.clone();
        let backend = self.backend.clone();
        let runtime = self.runtime.clone();
        let analysis_context = self.build_context_with_budget();
        let analysis_summary = self.context_compact_summary.clone();
        let ctx = self.egui_ctx.clone();

        runtime.spawn(async move {
            let send_event = |event| {
                let _ = tx.send(event);
                ctx.request_repaint();
            };
            let mut attempt = 0usize;
            let mut feedback: Option<String> = None;

            loop {
                attempt += 1;
                send_event(AppEvent::TranslationStarted { id });
                let llm_start = Instant::now();
                let result = translator
                    .translate(&question, &[], None, feedback.as_deref())
                    .await;
                let llm_ms = llm_start.elapsed().as_millis();

                let result = match result {
                    Ok(result) => result,
                    Err(err) => {
                        tracing::error!("Translation failed: {err}");
                        send_event(AppEvent::TranslationFailed {
                            id,
                            error: err.to_string(),
                        });
                        return;
                    }
                };
                log_llm_call("translator", llm_ms, result.usage.as_ref());

                send_event(AppEvent::TranslationCompleted {
                    id,
                    cypher: result.cypher.clone(),
                    usage: result.usage.clone(),
                    duration_ms: llm_ms,
                });

                match validate_cypher(&result.cypher) {
                    Ok(()) => {
                        let cypher = result.cypher.clone();
                        send_event(AppEvent::QueryStarted {
                            id,
                            cypher: cypher.clone(),
                        });
                        let exec_start = Instant::now();
                        match backend.execute_query(cypher.clone()).await {
                            Ok(records) => {
                                let exec_ms = exec_start.elapsed().as_millis();
                                let summary = summarize_records(&records);
                                send_event(AppEvent::QueryCompleted {
                                    id,
                                    cypher: cypher.clone(),
                                    records: records.clone(),
                                    duration_ms: exec_ms,
                                });
                                send_event(AppEvent::AnalysisStarted { id });
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
                                        log_llm_call(
                                            "analysis",
                                            analysis_ms,
                                            analysis.usage.as_ref(),
                                        );
                                        send_event(AppEvent::AnalysisCompleted {
                                            id,
                                            analysis,
                                            duration_ms: analysis_ms,
                                        });
                                    }
                                    Err(err) => {
                                        let analysis_ms = analysis_start.elapsed().as_millis();
                                        tracing::error!("Analysis failed: {err}");
                                        send_event(AppEvent::AnalysisFailed {
                                            id,
                                            error: err.to_string(),
                                            duration_ms: analysis_ms,
                                        });
                                    }
                                }
                            }
                            Err(err) => {
                                let exec_ms = exec_start.elapsed().as_millis();
                                tracing::error!("Query failed: {err}");
                                send_event(AppEvent::QueryFailed {
                                    id,
                                    error: err.to_string(),
                                    cypher,
                                    duration_ms: exec_ms,
                                });
                            }
                        }
                        return;
                    }
                    Err(issue) => {
                        tracing::error!("Validation failed: {issue}");
                        if attempt <= LLM_MAX_RETRIES && issue.retriable() {
                            feedback = Some(issue.feedback());
                            continue;
                        }
                        send_event(AppEvent::ValidationFailed {
                            id,
                            error: issue.to_string(),
                            cypher: result.cypher,
                        });
                        return;
                    }
                }
            }
        });
    }

    fn rerun_cypher(&mut self, id: u64, cypher: String) {
        let tx = self.events_tx.clone();
        let backend = self.backend.clone();
        let analyst = self.analyst.clone();
        let runtime = self.runtime.clone();
        let ctx = self.egui_ctx.clone();
        let question = self
            .feed
            .iter()
            .find(|item| item.id == id)
            .map(|item| item.user_text.clone())
            .unwrap_or_default();
        let analysis_context = self.build_context_with_budget();
        let analysis_summary = self.context_compact_summary.clone();

        runtime.spawn(async move {
            let send_event = |event| {
                let _ = tx.send(event);
                ctx.request_repaint();
            };
            match validate_cypher(&cypher) {
                Ok(()) => {
                    send_event(AppEvent::QueryStarted {
                        id,
                        cypher: cypher.clone(),
                    });
                    let exec_start = Instant::now();
                    match backend.execute_query(cypher.clone()).await {
                        Ok(records) => {
                            let exec_ms = exec_start.elapsed().as_millis();
                            let summary = summarize_records(&records);
                            send_event(AppEvent::QueryCompleted {
                                id,
                                cypher: cypher.clone(),
                                records: records.clone(),
                                duration_ms: exec_ms,
                            });
                            send_event(AppEvent::AnalysisStarted { id });
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
                                    send_event(AppEvent::AnalysisCompleted {
                                        id,
                                        analysis,
                                        duration_ms: analysis_ms,
                                    });
                                }
                                Err(err) => {
                                    let analysis_ms = analysis_start.elapsed().as_millis();
                                    tracing::error!("Analysis failed: {err}");
                                    send_event(AppEvent::AnalysisFailed {
                                        id,
                                        error: err.to_string(),
                                        duration_ms: analysis_ms,
                                    });
                                }
                            }
                        }
                        Err(err) => {
                            let exec_ms = exec_start.elapsed().as_millis();
                            tracing::error!("Query failed: {err}");
                            send_event(AppEvent::QueryFailed {
                                id,
                                error: err.to_string(),
                                cypher: cypher.clone(),
                                duration_ms: exec_ms,
                            });
                        }
                    }
                }
                Err(err) => {
                    tracing::error!("Validation failed: {err}");
                    send_event(AppEvent::ValidationFailed {
                        id,
                        error: err.to_string(),
                        cypher,
                    });
                }
            }
        });
    }

    fn handle_slash_command(&mut self, input: &str) -> bool {
        if input.starts_with("/history") {
            let id = self.next_id;
            self.next_id += 1;
            let mut item = FeedItem::new(id, input.to_string());
            item.state = FeedState::Ready;
            item.result = ResultPayload::Raw {
                text: "History is not implemented yet.".to_string(),
            };
            self.feed.push(item);
            return true;
        }
        if input.starts_with("/explain") {
            let id = self.next_id;
            self.next_id += 1;
            let mut item = FeedItem::new(id, input.to_string());
            item.state = FeedState::Ready;
            item.result = ResultPayload::Raw {
                text: "Explain mode is not implemented yet.".to_string(),
            };
            self.feed.push(item);
            return true;
        }
        false
    }

    fn drain_events(&mut self) -> bool {
        let mut handled = false;
        while let Ok(event) = self.events_rx.try_recv() {
            handled = true;
            match event {
                AppEvent::TranslationStarted { id } => {
                    if let Some(item) = self.feed_item_mut(id) {
                        item.state = FeedState::Translating;
                    }
                }
                AppEvent::TranslationCompleted {
                    id,
                    cypher,
                    usage,
                    duration_ms,
                } => {
                    if let Some(item) = self.feed_item_mut(id) {
                        item.cypher = Some(cypher);
                        item.state = FeedState::Validating;
                        item.llm_usage = usage;
                        item.llm_duration_ms = Some(duration_ms);
                    }
                }
                AppEvent::TranslationFailed { id, error } => {
                    if let Some(item) = self.feed_item_mut(id) {
                        item.state = FeedState::Error(error);
                    }
                }
                AppEvent::ValidationFailed { id, error, cypher } => {
                    if let Some(item) = self.feed_item_mut(id) {
                        item.cypher = Some(cypher);
                        item.state = FeedState::Error(error);
                    }
                }
                AppEvent::QueryStarted { id, cypher } => {
                    if let Some(item) = self.feed_item_mut(id) {
                        item.cypher = Some(cypher);
                        item.state = FeedState::Running;
                        item.analysis = None;
                        item.analysis_error = None;
                        item.analysis_pending = false;
                        item.analysis_duration_ms = None;
                    }
                }
                AppEvent::QueryCompleted {
                    id,
                    cypher,
                    records,
                    duration_ms,
                } => {
                    if let Some(item) = self.feed_item_mut(id) {
                        item.cypher = Some(cypher);
                        item.result = classify_result(&records);
                        item.state = FeedState::Ready;
                        item.exec_duration_ms = Some(duration_ms);
                        item.context_summary = Some(summarize_records(&records));
                    }
                }
                AppEvent::QueryFailed {
                    id,
                    error,
                    cypher,
                    duration_ms,
                } => {
                    if let Some(item) = self.feed_item_mut(id) {
                        item.cypher = Some(cypher);
                        item.state = FeedState::Error(error);
                        item.exec_duration_ms = Some(duration_ms);
                        item.analysis = None;
                        item.analysis_error = None;
                        item.analysis_pending = false;
                        item.analysis_duration_ms = None;
                    }
                }
                AppEvent::AnalysisStarted { id } => {
                    if let Some(item) = self.feed_item_mut(id) {
                        item.analysis_pending = true;
                        item.analysis_error = None;
                    }
                }
                AppEvent::AnalysisCompleted {
                    id,
                    analysis,
                    duration_ms,
                } => {
                    if let Some(item) = self.feed_item_mut(id) {
                        item.analysis = Some(analysis);
                        item.analysis_duration_ms = Some(duration_ms);
                        item.analysis_pending = false;
                        item.analysis_error = None;
                    }
                }
                AppEvent::AnalysisFailed {
                    id,
                    error,
                    duration_ms,
                } => {
                    if let Some(item) = self.feed_item_mut(id) {
                        item.analysis_error = Some(error);
                        item.analysis_duration_ms = Some(duration_ms);
                        item.analysis_pending = false;
                    }
                }
                AppEvent::ContextCompactionStarted => {
                    self.context_compacting = true;
                    self.context_compact_error = None;
                }
                AppEvent::ContextCompactionCompleted {
                    summary,
                    usage,
                    duration_ms,
                } => {
                    self.context_compacting = false;
                    self.context_compact_summary = Some(summary);
                    self.context_compact_usage = usage;
                    self.context_compact_duration_ms = Some(duration_ms);
                    self.context_compact_error = None;
                    self.context_cutoff_id = self.next_id;
                }
                AppEvent::ContextCompactionFailed { error } => {
                    self.context_compacting = false;
                    self.context_compact_error = Some(error);
                }
            }
        }
        handled
    }

    fn feed_item_mut(&mut self, id: u64) -> Option<&mut FeedItem> {
        self.feed.iter_mut().find(|item| item.id == id)
    }

    fn context_budget_tokens(&self) -> Option<usize> {
        let total = self.context_window_tokens?;
        let budget = total.saturating_sub(CONTEXT_RESERVED_TOKENS);
        Some(budget.max(CONTEXT_MIN_TOKENS).min(total))
    }

    fn build_context_with_budget(&self) -> Vec<ConversationTurn> {
        let Some(budget) = self.context_budget_tokens() else {
            return self.build_context(SHORT_TERM_CONTEXT_LIMIT);
        };

        let summary_tokens = self
            .context_compact_summary
            .as_deref()
            .map(estimate_text_tokens)
            .unwrap_or(0);
        let mut remaining = budget.saturating_sub(summary_tokens);
        let mut turns = Vec::new();

        for item in self.feed.iter().rev() {
            if item.id < self.context_cutoff_id {
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

    fn build_context(&self, limit: usize) -> Vec<ConversationTurn> {
        let mut turns = Vec::new();
        for item in self.feed.iter().rev() {
            if turns.len() >= limit {
                break;
            }
            if item.id < self.context_cutoff_id {
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

    fn build_context_for_compaction(&self, limit: usize) -> Vec<ConversationTurn> {
        self.build_context(limit)
    }

    fn reset_context(&mut self) {
        self.context_cutoff_id = self.next_id;
        self.context_compact_summary = None;
        self.context_compact_usage = None;
        self.context_compact_duration_ms = None;
        self.context_compact_error = None;
        self.context_compacting = false;
    }

    fn start_context_compaction(&mut self) {
        if self.context_compacting {
            return;
        }
        let context = self.build_context_for_compaction(COMPACT_CONTEXT_LIMIT);
        if context.is_empty() {
            self.context_compact_error = Some("No context to compact.".to_string());
            return;
        }

        let tx = self.events_tx.clone();
        let analyst = self.analyst.clone();
        let runtime = self.runtime.clone();
        let ctx = self.egui_ctx.clone();

        self.context_compacting = true;
        self.context_compact_error = None;

        runtime.spawn(async move {
            let send_event = |event| {
                let _ = tx.send(event);
                ctx.request_repaint();
            };
            send_event(AppEvent::ContextCompactionStarted);
            let start = Instant::now();
            match analyst.compact_context(&context).await {
                Ok(result) => {
                    let duration_ms = start.elapsed().as_millis();
                    send_event(AppEvent::ContextCompactionCompleted {
                        summary: result.summary,
                        usage: result.usage,
                        duration_ms,
                    });
                }
                Err(err) => {
                    send_event(AppEvent::ContextCompactionFailed {
                        error: err.to_string(),
                    });
                }
            }
        });
    }

    fn update_pulse(&mut self) {
        let interval = Duration::from_secs(5);
        if self.last_pulse_update.elapsed() < interval {
            return;
        }
        let (node_count, prop_count, pod_count, service_count, namespace_count) = {
            let guard = self
                .cluster_state
                .lock()
                .expect("cluster state lock poisoned");
            let node_count = guard.get_node_count();
            let prop_count = estimate_property_count(&guard, node_count);
            let pod_count = guard.get_nodes_by_type(&ResourceType::Pod).count();
            let service_count = guard.get_nodes_by_type(&ResourceType::Service).count();
            let namespace_count = guard.get_nodes_by_type(&ResourceType::Namespace).count();
            (
                node_count,
                prop_count,
                pod_count,
                service_count,
                namespace_count,
            )
        };
        push_sparkline(&mut self.pulse_nodes, node_count as f64);
        push_sparkline(&mut self.pulse_props, prop_count as f64);
        push_sparkline(&mut self.pulse_pods, pod_count as f64);
        push_sparkline(&mut self.pulse_services, service_count as f64);
        push_sparkline(&mut self.pulse_namespaces, namespace_count as f64);
        self.last_pulse_update = Instant::now();
    }

    fn update_autocomplete(&mut self) {
        let token = current_token(&self.input);
        if token.is_empty() {
            self.filtered_suggestions.clear();
            return;
        }
        let token_lower = token.to_lowercase();
        self.filtered_suggestions = self
            .suggestions
            .iter()
            .filter(|suggestion| suggestion.to_lowercase().starts_with(&token_lower))
            .take(6)
            .cloned()
            .collect();
    }

    fn apply_suggestion(&mut self, suggestion: &str) {
        let replaced = replace_last_token(&self.input, suggestion);
        self.input = replaced;
        self.filtered_suggestions.clear();
    }

    fn open_inspector_from_row(&mut self, row: &RowCard) {
        self.inspector.is_open = true;
        self.inspector.node_type = row
            .raw_fields
            .iter()
            .find(|(key, _)| key == "kind")
            .and_then(|(_, value)| value.as_str())
            .map(|value| value.to_string());
        self.inspector.node_id = Some(row.title.clone());
        self.inspector.properties = row
            .raw_fields
            .iter()
            .map(|(key, value)| InspectorProperty {
                key: key.clone(),
                value: inspector_value(value),
            })
            .collect();
        self.inspector.relationships = vec![];
    }
}

impl eframe::App for GuiApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        if self.drain_events() {
            ctx.request_repaint();
        }
        self.update_pulse();

        let screen_width = ctx.available_rect().width();
        let inspector_width = if screen_width < 1100.0 { 0.0 } else { 320.0 };

        // HEADER
        egui::TopBottomPanel::top("header")
            .exact_height(56.0)
            .frame(
                Frame::new()
                    .fill(self.palette.bg_panel)
                    .stroke(Stroke::new(1.0, self.palette.border))
                    .shadow(egui::Shadow {
                        offset: [0, 6],
                        blur: 12,
                        spread: 0,
                        color: Color32::from_black_alpha(80),
                    }),
            )
            .show(ctx, |ui| {
                ui.set_height(56.0);
                ui.horizontal(|ui| {
                    ui.add_space(16.0);
                    ui.label(
                        RichText::new("Ariadne (egui)")
                            .color(self.palette.text_primary)
                            .size(18.0)
                            .strong(),
                    );

                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        ui.add_space(16.0);
                        ui.add_sized(
                            [28.0, 28.0],
                            egui::Button::new(
                                RichText::new("J")
                                    .color(self.palette.text_primary)
                                    .size(12.0)
                                    .strong(),
                            )
                            .fill(self.palette.bg_elevated)
                            .stroke(Stroke::new(1.0, self.palette.border))
                            .corner_radius(CornerRadius::same(14)),
                        );
                        ui.add_space(6.0);
                        let _ = ui.add_sized(
                            [28.0, 28.0],
                            egui::Button::new(
                                RichText::new("?").color(self.palette.text_muted).size(12.0),
                            )
                            .fill(self.palette.bg_elevated)
                            .stroke(Stroke::new(1.0, self.palette.border))
                            .corner_radius(CornerRadius::same(14)),
                        );
                        ui.add_space(8.0);
                        let search_width = ui.available_width().clamp(180.0, 320.0);
                        ui.add_sized(
                            [search_width, 30.0],
                            TextEdit::singleline(&mut self.search)
                                .hint_text("Search")
                                .font(TextStyle::Body)
                                .background_color(self.palette.bg_elevated)
                                .margin(Margin::symmetric(10, 6)),
                        );
                    });
                });
            });

        // FOOTER
        egui::TopBottomPanel::bottom("footer")
            .exact_height(74.0)
            .frame(
                Frame::new()
                    .fill(self.palette.bg_panel)
                    .stroke(Stroke::new(1.0, self.palette.border))
                    .shadow(egui::Shadow {
                        offset: [0, -4],
                        blur: 10,
                        spread: 0,
                        color: Color32::from_black_alpha(80),
                    }),
            )
            .show(ctx, |ui| {
                ui.add_space(10.0);
                let mut has_focus = false;
                let mut input_id: Option<egui::Id> = None;
                ui.horizontal(|ui| {
                    ui.add_space(16.0);

                    let buttons_width = 140.0;
                    let available = ui.available_width() - buttons_width;

                    let response = ui.add_sized(
                        [available.max(220.0), 40.0],
                        TextEdit::singleline(&mut self.input)
                            .hint_text("Show me the services connected to these OOMing pods...")
                            .font(TextStyle::Monospace)
                            .background_color(self.palette.bg_elevated)
                            .margin(Margin::symmetric(12, 8)),
                    );
                    self.input_rect = Some(response.rect);
                    has_focus = response.has_focus();
                    input_id = Some(response.id);

                    if response.lost_focus() && ctx.input(|i| i.key_pressed(egui::Key::Enter)) {
                        self.submit_question();
                    }

                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        ui.add_space(16.0);
                        if ui
                            .add_sized(
                                [120.0, 40.0],
                                egui::Button::new(
                                    RichText::new("RUN QUERY")
                                        .color(self.palette.text_primary)
                                        .strong(),
                                )
                                .fill(self.palette.accent)
                                .stroke(Stroke::new(1.0, self.palette.accent))
                                .corner_radius(CornerRadius::same(6)),
                            )
                            .clicked()
                        {
                            self.submit_question();
                        }
                    });
                });
                self.update_autocomplete();
                if has_focus
                    && !self.filtered_suggestions.is_empty()
                    && ctx.input(|i| i.key_pressed(egui::Key::Tab))
                {
                    if let Some(first) = self.filtered_suggestions.first().cloned() {
                        self.apply_suggestion(&first);
                        ctx.input_mut(|i| i.consume_key(egui::Modifiers::NONE, egui::Key::Tab));
                        if let Some(id) = input_id {
                            ctx.memory_mut(|mem| mem.request_focus(id));
                        }
                    }
                }
                let mut show_autocomplete = has_focus;
                let mut autocomplete_rect = None;
                if let Some(rect) = self.input_rect {
                    let row_height = 24.0;
                    let height = row_height * self.filtered_suggestions.len() as f32 + 18.0;
                    let pos = rect.left_top() - Vec2::new(0.0, height + 10.0);
                    autocomplete_rect = Some(egui::Rect::from_min_size(
                        pos,
                        Vec2::new(rect.width(), height),
                    ));
                }
                if let Some(rect) = autocomplete_rect {
                    if ctx.input(|i| i.pointer.hover_pos().is_some_and(|p| rect.contains(p))) {
                        show_autocomplete = true;
                    }
                }
                if show_autocomplete && !self.filtered_suggestions.is_empty() {
                    if let Some(rect) = self.input_rect {
                        let row_height = 24.0;
                        let height = row_height * self.filtered_suggestions.len() as f32 + 18.0;
                        let pos = rect.left_top() - Vec2::new(0.0, height + 10.0);
                        egui::Area::new(egui::Id::new("autocomplete"))
                            .order(egui::Order::Foreground)
                            .fixed_pos(pos)
                            .show(ctx, |ui| {
                                Frame::new()
                                    .fill(self.palette.bg_elevated)
                                    .stroke(Stroke::new(1.0, self.palette.border))
                                    .corner_radius(CornerRadius::same(8))
                                    .inner_margin(Margin::same(8))
                                    .show(ui, |ui| {
                                        ui.set_width(rect.width());
                                        let suggestions = self.filtered_suggestions.clone();
                                        for (idx, suggestion) in suggestions.iter().enumerate() {
                                            let button = egui::Button::new(
                                                RichText::new(suggestion)
                                                    .color(self.palette.text_primary)
                                                    .size(12.0),
                                            )
                                            .fill(self.palette.bg_primary)
                                            .stroke(Stroke::new(1.0, self.palette.border))
                                            .corner_radius(CornerRadius::same(6));
                                            if ui
                                                .add_sized([rect.width() - 4.0, 28.0], button)
                                                .clicked()
                                            {
                                                self.apply_suggestion(suggestion);
                                            }
                                            if idx + 1 < suggestions.len() {
                                                ui.add_space(4.0);
                                            }
                                        }
                                    });
                            });
                    }
                }
            });

        if self.inspector.is_open && inspector_width > 0.0 {
            egui::SidePanel::right("inspector")
                .exact_width(inspector_width)
                .frame(
                    Frame::new()
                        .fill(self.palette.bg_panel)
                        .stroke(Stroke::new(1.0, self.palette.border)),
                )
                .show(ctx, |ui| {
                    ui.add_space(8.0);
                    Frame::new()
                        .fill(self.palette.bg_panel)
                        .stroke(Stroke::new(1.0, self.palette.border))
                        .corner_radius(CornerRadius::same(0)) // Panel fills side
                        .inner_margin(Margin::same(16))
                        .show(ui, |ui| {
                            ui.horizontal(|ui| {
                                ui.label(
                                    RichText::new("Node Inspector")
                                        .color(self.palette.text_primary)
                                        .size(16.0)
                                        .strong(),
                                );
                                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                                    if ui.button("X").clicked() {
                                        self.inspector.is_open = false;
                                    }
                                });
                            });
                            ui.add_space(16.0);

                            // Header
                            if let Some(node_id) = &self.inspector.node_id {
                                ui.label(
                                    RichText::new(node_id)
                                        .color(self.palette.text_primary)
                                        .size(18.0)
                                        .strong(),
                                );
                            }
                            if let Some(node_type) = &self.inspector.node_type {
                                ui.label(
                                    RichText::new(node_type)
                                        .color(self.palette.accent)
                                        .size(13.0),
                                );
                            }

                            ui.add_space(16.0);
                            ui.separator();

                            ui.label(
                                RichText::new("Properties")
                                    .color(self.palette.text_muted)
                                    .size(12.0)
                                    .strong(),
                            );
                            ui.add_space(4.0);
                            ScrollArea::vertical().max_height(240.0).show(ui, |ui| {
                                for property in &self.inspector.properties {
                                    match &property.value {
                                        InspectorValue::Text(value) => {
                                            ui.horizontal_wrapped(|ui| {
                                                ui.label(
                                                    RichText::new(format!("{}:", property.key))
                                                        .color(self.palette.text_muted)
                                                        .size(13.0),
                                                );
                                                ui.label(
                                                    RichText::new(value)
                                                        .color(self.palette.text_primary)
                                                        .size(13.0),
                                                );
                                            });
                                            ui.add_space(2.0);
                                        }
                                        InspectorValue::Json(value) => {
                                            ui.label(
                                                RichText::new(format!("{}:", property.key))
                                                    .color(self.palette.text_muted)
                                                    .size(13.0),
                                            );
                                            ui.add_space(4.0);
                                            let lines = value.lines().count().clamp(3, 10);
                                            let height = (lines as f32) * 16.0 + 12.0;
                                            Frame::new()
                                                .fill(self.palette.bg_primary)
                                                .stroke(Stroke::new(1.0, self.palette.border))
                                                .corner_radius(CornerRadius::same(6))
                                                .inner_margin(Margin::same(6))
                                                .show(ui, |ui| {
                                                    let mut display = value.clone();
                                                    ui.add_sized(
                                                        [ui.available_width(), height],
                                                        TextEdit::multiline(&mut display)
                                                            .font(TextStyle::Monospace)
                                                            .interactive(false)
                                                            .desired_width(f32::INFINITY),
                                                    );
                                                });
                                            ui.add_space(6.0);
                                        }
                                    }
                                }
                            });

                            ui.add_space(16.0);
                            ui.separator();
                            ui.label(
                                RichText::new("Relationships")
                                    .color(self.palette.text_muted)
                                    .size(12.0)
                                    .strong(),
                            );
                            ui.add_space(4.0);
                            if self.inspector.relationships.is_empty() {
                                ui.label(
                                    RichText::new("No relationships loaded")
                                        .color(self.palette.text_muted)
                                        .italics(),
                                );
                            } else {
                                for (label, target) in &self.inspector.relationships {
                                    ui.horizontal(|ui| {
                                        ui.label(RichText::new("→").color(self.palette.accent));
                                        ui.label(label.to_string());
                                        ui.label(
                                            RichText::new(format!("({target})"))
                                                .color(self.palette.text_muted),
                                        );
                                    });
                                }
                            }
                        });
                });
        }

        egui::CentralPanel::default()
            .frame(Frame::new().fill(self.palette.bg_primary))
            .show(ctx, |ui| {
                ui.add_space(12.0);
                let context_turns = self.build_context_with_budget();
                let context_tokens = estimate_context_tokens(
                    &context_turns,
                    self.context_compact_summary.as_deref(),
                );
                let context_budget = self.context_budget_tokens();
                let context_label = if let Some(budget) = context_budget {
                    format!(
                        "Context: {} • ~{} / ~{} tok",
                        context_turns.len(),
                        context_tokens,
                        budget
                    )
                } else {
                    format!(
                        "Context: {}/{} • ~{} tok",
                        context_turns.len(),
                        SHORT_TERM_CONTEXT_LIMIT,
                        context_tokens
                    )
                };
                let context_can_compact = !self.context_compacting && !context_turns.is_empty();

                let mut reset_clicked = false;
                let mut compact_clicked = false;
                ui.allocate_ui(Vec2::new(ui.available_width(), GRAPH_PULSE_HEIGHT), |ui| {
                    let (reset, compact) = render_graph_pulse(
                        ui,
                        &self.palette,
                        &mut self.cluster_meta,
                        &self.pulse_nodes,
                        &self.pulse_props,
                        &self.pulse_pods,
                        &self.pulse_services,
                        &self.pulse_namespaces,
                        &context_label,
                        self.context_compact_summary.is_some(),
                        self.context_compacting,
                        context_can_compact,
                    );
                    reset_clicked = reset;
                    compact_clicked = compact;
                });
                if reset_clicked {
                    self.reset_context();
                }
                if compact_clicked {
                    self.start_context_compaction();
                }

                ui.add_space(12.0);

                ScrollArea::vertical()
                    .auto_shrink([false; 2])
                    .show(ui, |ui| {
                        Frame::new()
                            .fill(self.palette.bg_panel)
                            .stroke(Stroke::new(1.0, self.palette.border))
                            .corner_radius(CornerRadius::same(12))
                            .inner_margin(Margin::same(14))
                            .shadow(egui::Shadow {
                                offset: [0, 6],
                                blur: 12,
                                spread: 0,
                                color: Color32::from_black_alpha(80),
                            })
                            .show(ui, |ui| {
                                ui.horizontal(|ui| {
                                    ui.label(
                                        RichText::new("Investigation Feed")
                                            .color(self.palette.text_primary)
                                            .size(14.0)
                                            .strong(),
                                    );
                                });

                                ui.add_space(10.0);

                                if let Some(error) = &self.context_compact_error {
                                    ui.label(
                                        RichText::new(error).color(self.palette.danger).size(11.0),
                                    );
                                    ui.add_space(6.0);
                                }
                                if self.context_compact_summary.is_some() {
                                    if let Some(ms) = self.context_compact_duration_ms {
                                        let token_hint = self
                                            .context_compact_usage
                                            .as_ref()
                                            .map(|usage| usage.total_tokens);
                                        let meta = if let Some(tokens) = token_hint {
                                            format!(
                                                "Compacted in {} • {} tokens",
                                                format_duration(ms),
                                                tokens
                                            )
                                        } else {
                                            format!("Compacted in {}", format_duration(ms))
                                        };
                                        ui.label(
                                            RichText::new(meta)
                                                .color(self.palette.text_muted)
                                                .size(11.0),
                                        );
                                        ui.add_space(6.0);
                                    }
                                }

                                if self.feed.is_empty() {
                                    ui.label(
                                        RichText::new("No investigations yet.")
                                            .color(self.palette.text_muted)
                                            .italics(),
                                    );
                                }

                                let mut run_request: Option<(u64, String)> = None;
                                let mut select_request: Option<RowCard> = None;
                                for item in &self.feed {
                                    render_feed_item(
                                        ui,
                                        item,
                                        &self.palette,
                                        |id, cypher| {
                                            run_request = Some((id, cypher));
                                        },
                                        |row| {
                                            select_request = Some(row.clone());
                                        },
                                    );
                                }
                                if let Some((id, cypher)) = run_request {
                                    self.rerun_cypher(id, cypher);
                                }
                                if let Some(row) = select_request {
                                    self.open_inspector_from_row(&row);
                                }
                            });

                        // Pad bottom to not be hidden behind footer
                        ui.add_space(24.0);
                    });
            });
    }

    fn on_exit(&mut self, _gl: Option<&eframe::glow::Context>) {
        self.token.cancel();
    }
}

#[allow(clippy::too_many_arguments)]
fn render_graph_pulse(
    ui: &mut egui::Ui,
    palette: &Palette,
    cluster_meta: &mut ClusterMeta,
    nodes: &[f64],
    props: &[f64],
    pods: &[f64],
    services: &[f64],
    namespaces: &[f64],
    context_label: &str,
    context_has_summary: bool,
    context_compacting: bool,
    context_can_compact: bool,
) -> (bool, bool) {
    let mut reset_clicked = false;
    let mut compact_clicked = false;
    Frame::new()
        .fill(palette.bg_panel)
        .stroke(Stroke::new(1.0, palette.border))
        .corner_radius(CornerRadius::same(12))
        .inner_margin(Margin::same(14))
        .shadow(egui::Shadow {
            offset: [0, 6],
            blur: 12,
            spread: 0,
            color: Color32::from_black_alpha(80),
        })
        .show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.label(
                    RichText::new("Graph Pulse")
                        .color(palette.text_primary)
                        .size(14.0)
                        .strong(),
                );
                ui.label(RichText::new("v").color(palette.text_muted).size(12.0));
                let status = if cluster_meta.connected {
                    "Connected"
                } else {
                    "Disconnected"
                };
                let status_color = if cluster_meta.connected {
                    palette.success
                } else {
                    palette.danger
                };
                ui.add_space(10.0);
                ui.label(RichText::new(status).color(status_color).size(11.0));
                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                    let mut selected = cluster_meta.label.clone();
                    egui::ComboBox::from_id_salt("cluster-selector")
                        .selected_text(selected.clone())
                        .width(200.0)
                        .show_ui(ui, |ui| {
                            ui.selectable_value(
                                &mut selected,
                                cluster_meta.label.clone(),
                                cluster_meta.label.clone(),
                            );
                        });
                    cluster_meta.label = selected;
                    ui.add_space(6.0);
                    ui.label(
                        RichText::new("Cluster:")
                            .color(palette.text_muted)
                            .size(12.0),
                    );
                    ui.add_space(10.0);
                    let backend_text =
                        truncate_text(&format!("Backend: {}", cluster_meta.backend_label), 48);
                    Frame::new()
                        .fill(palette.bg_elevated)
                        .stroke(Stroke::new(1.0, palette.border))
                        .corner_radius(CornerRadius::same(6))
                        .inner_margin(Margin::symmetric(10, 4))
                        .show(ui, |ui| {
                            ui.label(
                                RichText::new(backend_text)
                                    .color(palette.text_muted)
                                    .size(11.0),
                            );
                        });
                });
            });

            ui.add_space(6.0);

            ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                let reset = ui.add_enabled(
                    !context_compacting,
                    egui::Button::new(
                        RichText::new("Reset Context")
                            .color(palette.text_primary)
                            .size(11.0),
                    )
                    .fill(palette.bg_panel)
                    .stroke(Stroke::new(1.0, palette.border))
                    .corner_radius(CornerRadius::same(6)),
                );
                if reset.clicked() {
                    reset_clicked = true;
                }

                ui.add_space(6.0);

                let compact_label = if context_compacting {
                    "Compacting..."
                } else {
                    "Compact Context"
                };
                let compact = ui.add_enabled(
                    context_can_compact,
                    egui::Button::new(
                        RichText::new(compact_label)
                            .color(palette.text_primary)
                            .size(11.0),
                    )
                    .fill(palette.accent)
                    .stroke(Stroke::new(1.0, palette.accent))
                    .corner_radius(CornerRadius::same(6)),
                );
                if compact.clicked() {
                    compact_clicked = true;
                }

                ui.add_space(12.0);
                ui.label(
                    RichText::new(context_label)
                        .color(palette.text_muted)
                        .size(11.0),
                );
                if context_has_summary {
                    ui.add_space(6.0);
                    ui.label(RichText::new("summary").color(palette.accent).size(11.0));
                }
            });

            ui.add_space(8.0);
            ui.separator();
            ui.add_space(8.0);

            ui.horizontal(|ui| {
                pulse_metric_cell(ui, "Nodes", nodes, palette, palette.spark_nodes);
                ui.add(egui::Separator::default().vertical());
                ui.add_space(6.0);
                pulse_metric_cell(ui, "Properties", props, palette, palette.spark_props);
                ui.add(egui::Separator::default().vertical());
                ui.add_space(6.0);
                pulse_metric_cell(ui, "Pods", pods, palette, palette.spark_pods);
                ui.add(egui::Separator::default().vertical());
                ui.add_space(6.0);
                pulse_metric_cell(ui, "Services", services, palette, palette.spark_services);
                ui.add(egui::Separator::default().vertical());
                ui.add_space(6.0);
                pulse_metric_cell(
                    ui,
                    "Namespaces",
                    namespaces,
                    palette,
                    palette.spark_namespaces,
                );
            });
        });
    (reset_clicked, compact_clicked)
}

fn pulse_metric_cell(
    ui: &mut egui::Ui,
    label: &str,
    series: &[f64],
    palette: &Palette,
    spark_color: Color32,
) {
    let count = series.last().copied().unwrap_or(0.0) as usize;
    ui.vertical(|ui| {
        ui.label(
            RichText::new(format!("{label}:"))
                .color(palette.text_muted)
                .size(12.0),
        );
        ui.horizontal(|ui| {
            ui.label(
                RichText::new(format_count(count))
                    .color(palette.text_primary)
                    .size(18.0)
                    .strong(),
            );
            if series.len() >= 2 {
                let delta = series[series.len() - 1] - series[series.len() - 2];
                if delta > 0.0 {
                    ui.label(RichText::new("^").color(palette.success).size(12.0));
                } else if delta < 0.0 {
                    ui.label(RichText::new("v").color(palette.danger).size(12.0));
                }
            }
        });
        let spark_size = Vec2::new(140.0, 24.0);
        let (response, painter) = ui.allocate_painter(spark_size, egui::Sense::hover());
        draw_sparkline(painter, response.rect, series, spark_color);
    });
}

fn draw_sparkline(painter: egui::Painter, rect: egui::Rect, series: &[f64], color: Color32) {
    if series.len() < 2 {
        return;
    }
    let min = series.iter().copied().fold(f64::INFINITY, |a, b| a.min(b));
    let max = series
        .iter()
        .copied()
        .fold(f64::NEG_INFINITY, |a, b| a.max(b));
    let range = (max - min).max(1.0);

    let points: Vec<egui::Pos2> = series
        .iter()
        .enumerate()
        .map(|(idx, value)| {
            let t = idx as f32 / (series.len().saturating_sub(1)) as f32;
            let x = rect.left() + rect.width() * t;
            let norm = ((*value - min) / range) as f32;
            let y = rect.bottom() - rect.height() * norm;
            egui::pos2(x, y)
        })
        .collect();

    for window in points.windows(2) {
        painter.line_segment([window[0], window[1]], Stroke::new(1.5, color));
    }
}

fn skeleton_line(ui: &mut egui::Ui, width: f32, palette: &Palette) {
    let height = 8.0;
    let width = width.max(40.0);
    let (rect, _) = ui.allocate_exact_size(Vec2::new(width, height), egui::Sense::hover());
    ui.painter().rect_filled(
        rect,
        CornerRadius::same(4),
        lighten_color(palette.bg_elevated, 1.08),
    );
}

fn render_item_stats(ui: &mut egui::Ui, item: &FeedItem, palette: &Palette) {
    if item.llm_duration_ms.is_none()
        && item.exec_duration_ms.is_none()
        && item.llm_usage.is_none()
        && item.analysis_duration_ms.is_none()
        && item
            .analysis
            .as_ref()
            .and_then(|a| a.usage.as_ref())
            .is_none()
    {
        return;
    }

    ui.add_space(8.0);
    ui.horizontal_wrapped(|ui| {
        if let Some(ms) = item.llm_duration_ms {
            ui.label(
                RichText::new(format!("LLM {}", format_duration(ms)))
                    .color(palette.text_muted)
                    .size(11.0),
            );
        }
        if let Some(usage) = &item.llm_usage {
            ui.label(
                RichText::new(format!(
                    "tokens {}/{}/{}",
                    usage.prompt_tokens, usage.completion_tokens, usage.total_tokens
                ))
                .color(palette.text_muted)
                .size(11.0),
            );
            if let Some(cached) = usage.cached_tokens {
                ui.label(
                    RichText::new(format!("cached {cached}"))
                        .color(palette.text_muted)
                        .size(11.0),
                );
            }
            if let Some(reasoning) = usage.reasoning_tokens {
                ui.label(
                    RichText::new(format!("reasoning {reasoning}"))
                        .color(palette.text_muted)
                        .size(11.0),
                );
            }
        }
        if let Some(ms) = item.exec_duration_ms {
            ui.label(
                RichText::new(format!("exec {}", format_duration(ms)))
                    .color(palette.text_muted)
                    .size(11.0),
            );
        }
        if let Some(ms) = item.analysis_duration_ms {
            ui.label(
                RichText::new(format!("analysis {}", format_duration(ms)))
                    .color(palette.text_muted)
                    .size(11.0),
            );
        }
        if let Some(usage) = item
            .analysis
            .as_ref()
            .and_then(|analysis| analysis.usage.as_ref())
        {
            ui.label(
                RichText::new(format!(
                    "analysis tokens {}/{}/{}",
                    usage.prompt_tokens, usage.completion_tokens, usage.total_tokens
                ))
                .color(palette.text_muted)
                .size(11.0),
            );
            if let Some(cached) = usage.cached_tokens {
                ui.label(
                    RichText::new(format!("analysis cached {cached}"))
                        .color(palette.text_muted)
                        .size(11.0),
                );
            }
            if let Some(reasoning) = usage.reasoning_tokens {
                ui.label(
                    RichText::new(format!("analysis reasoning {reasoning}"))
                        .color(palette.text_muted)
                        .size(11.0),
                );
            }
        }
    });
}

fn format_duration(ms: u128) -> String {
    if ms >= 1000 {
        format!("{:.2}s", ms as f64 / 1000.0)
    } else {
        format!("{ms} ms")
    }
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

fn render_feed_item(
    ui: &mut egui::Ui,
    item: &FeedItem,
    palette: &Palette,
    mut on_run: impl FnMut(u64, String),
    mut on_select: impl FnMut(&RowCard),
) {
    ui.add_space(10.0);
    Frame::new()
        .fill(palette.bg_elevated)
        .stroke(Stroke::new(1.0, palette.border))
        .corner_radius(CornerRadius::same(10))
        .inner_margin(Margin::same(12))
        .show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.label(RichText::new("User").color(palette.accent).size(12.0));
                ui.label(RichText::new("> ").color(palette.text_muted).size(12.0));
                ui.label(
                    RichText::new(&item.user_text)
                        .color(palette.text_primary)
                        .size(14.0),
                );
                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                    ui.label(RichText::new("...").color(palette.text_muted));
                });
            });

            if let Some(cypher) = &item.cypher {
                ui.add_space(8.0);
                let id = ui.make_persistent_id(format!("cypher-header-{}", item.id));
                egui::collapsing_header::CollapsingState::load_with_default_open(
                    ui.ctx(),
                    id,
                    true,
                )
                .show_header(ui, |ui| {
                    ui.label(
                        RichText::new("Planned Query")
                            .size(12.0)
                            .color(palette.text_muted)
                            .strong(),
                    );
                })
                .body(|ui| {
                    Frame::new()
                        .fill(palette.bg_panel)
                        .stroke(Stroke::new(1.0, palette.border))
                        .corner_radius(CornerRadius::same(8))
                        .inner_margin(Margin::same(10))
                        .show(ui, |ui| {
                            ui.horizontal(|ui| {
                                let mut job = highlight_cypher(cypher, palette);
                                job.wrap.max_width = ui.available_width() - 84.0;
                                job.wrap.break_anywhere = true;
                                ui.add(egui::Label::new(job).wrap());

                                ui.with_layout(Layout::right_to_left(Align::Min), |ui| {
                                    if ui
                                        .add(
                                            egui::Button::new(
                                                RichText::new("Run")
                                                    .color(palette.text_primary)
                                                    .strong(),
                                            )
                                            .fill(palette.accent)
                                            .stroke(Stroke::new(1.0, palette.accent))
                                            .corner_radius(CornerRadius::same(6)),
                                        )
                                        .clicked()
                                    {
                                        on_run(item.id, cypher.clone());
                                    }
                                    if ui
                                        .add(
                                            egui::Button::new(
                                                RichText::new("Copy").color(palette.text_primary),
                                            )
                                            .fill(palette.bg_elevated)
                                            .stroke(Stroke::new(1.0, palette.border))
                                            .corner_radius(CornerRadius::same(6)),
                                        )
                                        .clicked()
                                    {
                                        ui.ctx().copy_text(cypher.clone());
                                    }
                                });
                            });
                        });
                });
            }

            render_item_stats(ui, item, palette);
            if item.llm_duration_ms.is_some()
                || item.exec_duration_ms.is_some()
                || item.llm_usage.is_some()
            {
                ui.add_space(8.0);
            }

            ui.add_space(10.0);

            Frame::new()
                .fill(palette.bg_panel)
                .stroke(Stroke::new(1.0, palette.border))
                .corner_radius(CornerRadius::same(8))
                .inner_margin(Margin::same(12))
                .show(ui, |ui| match &item.state {
                    FeedState::Translating => {
                        ui.label(
                            RichText::new("Translating...")
                                .color(palette.text_muted)
                                .italics(),
                        );
                        ui.add_space(8.0);
                        let width = ui.available_width();
                        skeleton_line(ui, width, palette);
                        ui.add_space(6.0);
                        skeleton_line(ui, width * 0.92, palette);
                        ui.add_space(6.0);
                        skeleton_line(ui, width * 0.7, palette);
                    }
                    FeedState::Validating => {
                        ui.label(
                            RichText::new("Validating...")
                                .color(palette.text_muted)
                                .italics(),
                        );
                        ui.add_space(8.0);
                        let width = ui.available_width();
                        skeleton_line(ui, width, palette);
                        ui.add_space(6.0);
                        skeleton_line(ui, width * 0.75, palette);
                    }
                    FeedState::Running => {
                        ui.label(
                            RichText::new("Running...")
                                .color(palette.text_muted)
                                .italics(),
                        );
                        ui.add_space(8.0);
                        let width = ui.available_width();
                        skeleton_line(ui, width, palette);
                        ui.add_space(6.0);
                        skeleton_line(ui, width * 0.6, palette);
                    }
                    FeedState::Error(err) => {
                        ui.colored_label(palette.danger, format!("Error: {err}"));
                    }
                    FeedState::Ready => {
                        if render_analysis(ui, item, palette) {
                            ui.add_space(10.0);
                        }
                        render_result(ui, item, palette, &mut on_select);
                    }
                });
        });
}

fn highlight_cypher(text: &str, palette: &Palette) -> LayoutJob {
    let mut job = LayoutJob::default();
    let keywords = [
        "MATCH", "RETURN", "WHERE", "AND", "OR", "AS", "IN", "LIMIT", "ORDER BY", "SKIP", "WITH",
        "UNWIND", "CALL", "YIELD", "CREATE", "DELETE", "SET", "REMOVE", "MERGE", "DISTINCT",
        "COUNT",
    ];

    // Simple approach: split by whitespace
    // This is not a perfect lexer but suffices for "basic"
    for part in text.split_inclusive(|c: char| !c.is_alphanumeric() && c != '_') {
        // part contains the word and maybe a delimiter
        let trimmed = part.trim_matches(|c: char| !c.is_alphanumeric() && c != '_');
        let upper = trimmed.to_uppercase();

        let color = if keywords.contains(&upper.as_str()) {
            palette.keyword
        } else if trimmed.starts_with('"') || trimmed.starts_with("'") {
            palette.string
        } else {
            palette.text_primary
        };

        job.append(
            part,
            0.0,
            TextFormat {
                font_id: FontId::new(13.0, FontFamily::Monospace),
                color,
                ..Default::default()
            },
        );
    }
    job
}

fn render_result(
    ui: &mut egui::Ui,
    item: &FeedItem,
    palette: &Palette,
    on_select: &mut impl FnMut(&RowCard),
) {
    match &item.result {
        ResultPayload::Empty => {
            ui.label(
                RichText::new("No results returned.")
                    .color(palette.text_muted)
                    .italics(),
            );
        }
        ResultPayload::Metric { label, value, unit } => {
            Frame::new()
                .fill(palette.bg_primary)
                .stroke(Stroke::new(1.0, palette.border))
                .corner_radius(CornerRadius::same(10))
                .inner_margin(Margin::same(16))
                .show(ui, |ui| {
                    ui.vertical_centered(|ui| {
                        ui.add_space(6.0);
                        ui.horizontal(|ui| {
                            let mut text = value.clone();
                            if let Some(unit) = unit {
                                text = format!("{text} {unit}");
                            }
                            ui.label(
                                RichText::new(text)
                                    .size(42.0)
                                    .color(palette.accent_warm)
                                    .strong(),
                            );
                        });
                        ui.label(RichText::new(label).size(16.0).color(palette.text_muted));
                        ui.add_space(6.0);
                    });
                });
        }
        ResultPayload::List { rows } => {
            // "VISUAL RESULT AREA" header is already rendered by render_feed_item, so we remove it here.

            ui.add_space(6.0);
            let frame = Frame::new()
                .fill(palette.bg_primary)
                .stroke(Stroke::new(1.0, palette.border))
                .corner_radius(CornerRadius::same(8))
                .inner_margin(Margin::same(8))
                .shadow(egui::Shadow {
                    offset: [0, 6],
                    blur: 12,
                    spread: 0,
                    color: Color32::from_black_alpha(80),
                });
            frame.show(ui, |ui| {
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
                let mut column_labels = Vec::new();
                if show_title {
                    column_labels.push("Name".to_string());
                }
                if show_namespace {
                    column_labels.push("Namespace".to_string());
                }
                if show_status {
                    column_labels.push("Status".to_string());
                }
                column_labels.extend(extra_keys.clone());

                let mut column_defs = Vec::new();
                if show_title {
                    column_defs.push(Column::initial(220.0).at_least(140.0).resizable(true));
                }
                if show_namespace {
                    column_defs.push(Column::initial(160.0).at_least(120.0).resizable(true));
                }
                if show_status {
                    column_defs.push(Column::initial(120.0).at_least(100.0).resizable(true));
                }
                for _ in &extra_keys {
                    column_defs.push(Column::initial(160.0).at_least(120.0).resizable(true));
                }

                ScrollArea::horizontal()
                    .auto_shrink([false; 2])
                    .show(ui, |ui| {
                        let mut table = TableBuilder::new(ui)
                            .id_salt(format!("result-table-{}", item.id))
                            .striped(true)
                            .resizable(true)
                            .cell_layout(Layout::left_to_right(Align::Center))
                            .min_scrolled_height(120.0)
                            .max_scroll_height(260.0);
                        for col in &column_defs {
                            table = table.column(*col);
                        }
                        table
                            .header(22.0, |mut header| {
                                for header_label in &column_labels {
                                    header.col(|ui| {
                                        ui.label(
                                            RichText::new(header_label)
                                                .color(palette.text_muted)
                                                .size(11.0)
                                                .strong(),
                                        );
                                    });
                                }
                            })
                            .body(|body| {
                                let row_height = 26.0;
                                body.rows(row_height, rows.len(), |mut row| {
                                    let row_index = row.index();
                                    let row_data = &rows[row_index];

                                    if show_title {
                                        row.col(|ui| {
                                            let response =
                                                ui.selectable_label(false, &row_data.title);
                                            if response.clicked() {
                                                on_select(row_data);
                                            }
                                        });
                                    }
                                    if show_namespace {
                                        let namespace = row_data.subtitle.as_deref().unwrap_or("-");
                                        row.col(|ui| {
                                            let response = ui.selectable_label(false, namespace);
                                            if response.clicked() {
                                                on_select(row_data);
                                            }
                                        });
                                    }
                                    if show_status {
                                        let status = row_data.status.as_deref().unwrap_or("-");
                                        row.col(|ui| {
                                            let response = ui.selectable_label(false, status);
                                            if response.clicked() {
                                                on_select(row_data);
                                            }
                                        });
                                    }
                                    for key in &extra_keys {
                                        row.col(|ui| {
                                            let value =
                                                find_field(&row_data.fields, key).unwrap_or("-");
                                            let response = ui.selectable_label(false, value);
                                            if response.clicked() {
                                                on_select(row_data);
                                            }
                                        });
                                    }
                                });
                            });
                    });
            });
        }
        ResultPayload::Graph { nodes, edges } => {
            Frame::new()
                .fill(palette.bg_primary)
                .stroke(Stroke::new(1.0, palette.border))
                .corner_radius(CornerRadius::same(8))
                .inner_margin(Margin::same(0))
                .shadow(egui::Shadow {
                    offset: [0, 6],
                    blur: 12,
                    spread: 0,
                    color: Color32::from_black_alpha(80),
                })
                .show(ui, |ui| {
                    let size = Vec2::new(ui.available_width(), 300.0);
                    let (response, painter) = ui.allocate_painter(size, egui::Sense::click());
                    draw_graph(painter, response.rect, nodes, edges, palette);
                });
        }
        ResultPayload::Raw { text } => {
            let mut display = text.clone();
            ui.add_sized(
                [ui.available_width(), 120.0],
                TextEdit::multiline(&mut display)
                    .font(TextStyle::Monospace)
                    .interactive(false),
            );
        }
    }
}

fn render_analysis(ui: &mut egui::Ui, item: &FeedItem, palette: &Palette) -> bool {
    if !item.analysis_pending && item.analysis.is_none() && item.analysis_error.is_none() {
        return false;
    }

    Frame::new()
        .fill(palette.bg_primary)
        .stroke(Stroke::new(1.0, palette.border))
        .corner_radius(CornerRadius::same(8))
        .inner_margin(Margin::same(12))
        .show(ui, |ui| {
            ui.label(
                RichText::new("SRE Answer")
                    .color(palette.text_muted)
                    .size(12.0)
                    .strong(),
            );
            ui.add_space(8.0);

            if item.analysis_pending {
                ui.label(
                    RichText::new("Analyzing results...")
                        .color(palette.text_muted)
                        .italics(),
                );
                ui.add_space(8.0);
                let width = ui.available_width();
                skeleton_line(ui, width, palette);
                ui.add_space(6.0);
                skeleton_line(ui, width * 0.85, palette);
                ui.add_space(6.0);
                skeleton_line(ui, width * 0.65, palette);
                return;
            }

            if let Some(error) = &item.analysis_error {
                ui.colored_label(palette.danger, format!("Analysis error: {error}"));
                return;
            }

            if let Some(analysis) = &item.analysis {
                ui.label(
                    RichText::new(&analysis.title)
                        .color(palette.text_primary)
                        .size(14.0)
                        .strong(),
                );
                ui.add_space(4.0);
                ui.label(
                    RichText::new(&analysis.summary)
                        .color(palette.text_primary)
                        .size(13.0),
                );

                if !analysis.bullets.is_empty() {
                    ui.add_space(8.0);
                    for bullet in &analysis.bullets {
                        ui.label(
                            RichText::new(format!("• {bullet}"))
                                .color(palette.text_primary)
                                .size(12.0),
                        );
                    }
                }

                if !analysis.rows.is_empty() {
                    ui.add_space(10.0);
                    ui.label(
                        RichText::new("Highlights")
                            .color(palette.text_muted)
                            .size(12.0)
                            .strong(),
                    );
                    ui.add_space(4.0);
                    render_analysis_rows(ui, &analysis.rows, palette);
                }

                if !analysis.follow_ups.is_empty() {
                    ui.add_space(10.0);
                    ui.label(
                        RichText::new("Follow-ups")
                            .color(palette.text_muted)
                            .size(12.0)
                            .strong(),
                    );
                    ui.add_space(4.0);
                    for follow in &analysis.follow_ups {
                        ui.label(
                            RichText::new(format!("• {follow}"))
                                .color(palette.text_primary)
                                .size(12.0),
                        );
                    }
                }

                if item.analysis_duration_ms.is_some()
                    || analysis.usage.is_some()
                    || !analysis.confidence.is_empty()
                {
                    ui.add_space(10.0);
                    ui.horizontal_wrapped(|ui| {
                        if let Some(ms) = item.analysis_duration_ms {
                            ui.label(
                                RichText::new(format!("analysis {}", format_duration(ms)))
                                    .color(palette.text_muted)
                                    .size(11.0),
                            );
                        }
                        if !analysis.confidence.is_empty() {
                            ui.label(
                                RichText::new(format!("confidence {}", analysis.confidence))
                                    .color(palette.text_muted)
                                    .size(11.0),
                            );
                        }
                        if let Some(usage) = analysis.usage.as_ref() {
                            ui.label(
                                RichText::new(format!(
                                    "tokens {}/{}/{}",
                                    usage.prompt_tokens,
                                    usage.completion_tokens,
                                    usage.total_tokens
                                ))
                                .color(palette.text_muted)
                                .size(11.0),
                            );
                            if let Some(cached) = usage.cached_tokens {
                                ui.label(
                                    RichText::new(format!("cached {cached}"))
                                        .color(palette.text_muted)
                                        .size(11.0),
                                );
                            }
                            if let Some(reasoning) = usage.reasoning_tokens {
                                ui.label(
                                    RichText::new(format!("reasoning {reasoning}"))
                                        .color(palette.text_muted)
                                        .size(11.0),
                                );
                            }
                        }
                    });
                }
            }
        });
    true
}

fn render_analysis_rows(ui: &mut egui::Ui, rows: &[Value], palette: &Palette) {
    let objects: Vec<&Map<String, Value>> = rows.iter().filter_map(|row| row.as_object()).collect();
    if objects.is_empty() {
        ui.label(
            RichText::new("No structured rows to display.")
                .color(palette.text_muted)
                .size(11.0),
        );
        return;
    }

    let mut columns: Vec<String> = objects[0].keys().cloned().collect();
    columns.sort();

    let max_rows = 10usize;
    let row_count = objects.len().min(max_rows);
    ScrollArea::horizontal()
        .auto_shrink([false; 2])
        .show(ui, |ui| {
            let mut table = TableBuilder::new(ui)
                .id_salt("analysis-rows")
                .striped(true)
                .resizable(true)
                .cell_layout(Layout::left_to_right(Align::Center))
                .min_scrolled_height(80.0)
                .max_scroll_height(180.0);
            for _ in &columns {
                table = table.column(Column::initial(150.0).at_least(120.0).resizable(true));
            }
            table
                .header(20.0, |mut header| {
                    for label in &columns {
                        header.col(|ui| {
                            ui.label(
                                RichText::new(label)
                                    .color(palette.text_muted)
                                    .size(11.0)
                                    .strong(),
                            );
                        });
                    }
                })
                .body(|body| {
                    let row_height = 24.0;
                    body.rows(row_height, row_count, |mut row| {
                        let row_index = row.index();
                        let row_data = objects[row_index];
                        for key in &columns {
                            row.col(|ui| {
                                let value = row_data
                                    .get(key)
                                    .map(format_value)
                                    .unwrap_or_else(|| "-".to_string());
                                ui.label(value);
                            });
                        }
                    });
                });
        });

    if objects.len() > max_rows {
        ui.add_space(4.0);
        ui.label(
            RichText::new(format!("Showing {row_count} of {} rows.", objects.len()))
                .color(palette.text_muted)
                .size(10.0),
        );
    }
}

fn draw_graph(
    painter: egui::Painter,
    rect: egui::Rect,
    nodes: &[GraphNode],
    edges: &[GraphEdge],
    palette: &Palette,
) {
    if nodes.is_empty() {
        painter.text(
            rect.center(),
            Align2::CENTER_CENTER,
            "No graph data",
            FontId::new(12.0, FontFamily::Proportional),
            palette.text_muted,
        );
        return;
    }
    let center = rect.center();
    // Use a slightly better layout: distribute on circle
    let radius = rect.width().min(rect.height()) * 0.35;

    // Draw background grid (subtle cyber aesthetic)
    let grid_step = 30.0;
    let grid_color = palette.bg_elevated.gamma_multiply(0.4);
    let mut x = rect.left();
    while x < rect.right() {
        painter.line_segment(
            [egui::pos2(x, rect.top()), egui::pos2(x, rect.bottom())],
            Stroke::new(0.5, grid_color),
        );
        x += grid_step;
    }
    let mut y = rect.top();
    while y < rect.bottom() {
        painter.line_segment(
            [egui::pos2(rect.left(), y), egui::pos2(rect.right(), y)],
            Stroke::new(0.5, grid_color),
        );
        y += grid_step;
    }

    let positions: Vec<egui::Pos2> = (0..nodes.len())
        .map(|idx| {
            let angle = idx as f32 / nodes.len() as f32 * std::f32::consts::TAU;
            egui::pos2(
                center.x + radius * angle.cos(),
                center.y + radius * angle.sin(),
            )
        })
        .collect();

    for edge in edges {
        if let (Some(from), Some(to)) = (positions.get(edge.from), positions.get(edge.to)) {
            // Glowing Edges
            painter.line_segment(
                [*from, *to],
                Stroke::new(2.0, lighten_color(palette.border, 1.12)),
            );
            painter.line_segment([*from, *to], Stroke::new(1.0, palette.text_muted));

            if let Some(label) = &edge.label {
                let mid = egui::pos2((from.x + to.x) * 0.5, (from.y + to.y) * 0.5);
                // Draw label background pill (Estimated size to avoid galley complexity)
                let text_width = label.len() as f32 * 6.0;
                let rect_width = text_width + 12.0;
                let text_rect = egui::Rect::from_center_size(mid, Vec2::new(rect_width, 16.0));

                painter.rect(
                    text_rect,
                    CornerRadius::same(6),
                    palette.bg_primary,
                    Stroke::new(1.0, palette.border),
                    egui::StrokeKind::Middle,
                );

                painter.text(
                    mid,
                    Align2::CENTER_CENTER,
                    label,
                    FontId::new(10.0, FontFamily::Proportional),
                    palette.text_muted,
                );
            }
        }
    }

    for (idx, node) in nodes.iter().enumerate() {
        if let Some(pos) = positions.get(idx) {
            // Cyber Node: Glow + Core
            // Outer Glow
            painter.circle_filled(*pos, 14.0, palette.accent.gamma_multiply(0.1));
            painter.circle_stroke(
                *pos,
                12.0,
                Stroke::new(1.0, palette.accent.gamma_multiply(0.5)),
            );

            // Core
            painter.circle_filled(*pos, 5.0, palette.accent);

            // Label
            painter.text(
                *pos + Vec2::new(0.0, 18.0),
                Align2::CENTER_TOP,
                &node.label,
                FontId::new(12.0, FontFamily::Proportional),
                palette.text_primary,
            );
        }
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

fn push_sparkline(series: &mut Vec<f64>, value: f64) {
    if series.is_empty() {
        // Pre-fill history so it shows a flat line immediately
        for _ in 0..12 {
            series.push(value);
        }
    } else {
        series.push(value);
        if series.len() > 12 {
            series.remove(0);
        }
    }
}

fn estimate_property_count(state: &ariadne_core::state::ClusterState, node_count: usize) -> usize {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_count_adds_commas() {
        assert_eq!(format_count(0), "0");
        assert_eq!(format_count(1000), "1,000");
        assert_eq!(format_count(1200300), "1,200,300");
    }

    #[test]
    fn current_token_picks_last_word() {
        assert_eq!(current_token("MATCH (p:Pod"), "Pod");
    }

    #[test]
    fn push_sparkline_prefills_empty() {
        let mut series = vec![];
        push_sparkline(&mut series, 42.0);
        assert_eq!(series.len(), 12);
        for val in series {
            assert_eq!(val, 42.0);
        }
    }

    #[test]
    fn push_sparkline_maintains_size_and_shifts() {
        let mut series = vec![
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0,
        ];
        push_sparkline(&mut series, 13.0);
        assert_eq!(series.len(), 12);
        assert_eq!(series[0], 2.0);
        assert_eq!(series[11], 13.0);
    }
}
