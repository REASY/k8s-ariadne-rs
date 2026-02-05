use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use eframe::egui;
use eframe::egui::{
    text::LayoutJob, Align, Align2, Color32, CornerRadius, FontFamily, FontId, Frame, Layout,
    Margin, RichText, ScrollArea, Stroke, TextEdit, TextFormat, TextStyle, Vec2,
};
use serde_json::{Map, Value};
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;

use ariadne_core::graph_backend::GraphBackend;
use ariadne_core::state::SharedClusterState;
use ariadne_core::types::ResourceType;
use strum::IntoEnumIterator;

use crate::error::CliResult;
use crate::llm::Translator;
use crate::validation::validate_cypher;

pub fn run_gui(
    runtime: &tokio::runtime::Runtime,
    backend: Arc<dyn GraphBackend>,
    translator: Arc<dyn Translator>,
    cluster_state: SharedClusterState,
    token: CancellationToken,
    cluster_label: String,
) -> CliResult<()> {
    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([1400.0, 900.0]),
        ..Default::default()
    };
    let app = GuiApp::new(
        runtime.handle().clone(),
        backend,
        translator,
        cluster_state,
        token,
        cluster_label,
    );
    eframe::run_native(
        "KubeGraph Ops",
        native_options,
        Box::new(|cc| {
            let palette = Palette::default();
            setup_style(&cc.egui_ctx, &palette);
            Ok(Box::new(app))
        }),
    )
    .map_err(|err| std::io::Error::other(err.to_string()))?;
    Ok(())
}

#[derive(Clone)]
struct ClusterMeta {
    label: String,
    connected: bool,
}

#[derive(Clone)]
struct Palette {
    bg_primary: Color32,
    bg_panel: Color32,
    bg_elevated: Color32,
    accent: Color32,
    accent_warm: Color32,
    danger: Color32,
    text_primary: Color32,
    text_muted: Color32,
    border: Color32,
    keyword: Color32,
    string: Color32,
}

impl Default for Palette {
    fn default() -> Self {
        Self {
            bg_primary: Color32::from_rgb(0x0B, 0x11, 0x17),
            bg_panel: Color32::from_rgb(0x12, 0x1A, 0x23),
            bg_elevated: Color32::from_rgb(0x1B, 0x24, 0x30),
            accent: Color32::from_rgb(0x4C, 0xC9, 0xF0),
            accent_warm: Color32::from_rgb(0xF4, 0xA2, 0x61),
            danger: Color32::from_rgb(0xE7, 0x6F, 0x51),
            text_primary: Color32::from_rgb(0xE6, 0xED, 0xF3),
            text_muted: Color32::from_rgb(0x8A, 0xA0, 0xB2),
            border: Color32::from_rgb(0x26, 0x32, 0x41),
            keyword: Color32::from_rgb(0xF9, 0x75, 0x83), // Pinkish for keywords
            string: Color32::from_rgb(0xA5, 0xD6, 0xFF),  // Light blue for strings
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
}

impl FeedItem {
    fn new(id: u64, user_text: String) -> Self {
        Self {
            id,
            user_text,
            cypher: None,
            result: ResultPayload::Empty,
            state: FeedState::Translating,
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
    },
    QueryFailed {
        id: u64,
        error: String,
        cypher: String,
    },
}

pub struct GuiApp {
    runtime: Handle,
    backend: Arc<dyn GraphBackend>,
    translator: Arc<dyn Translator>,
    cluster_state: SharedClusterState,
    cluster_meta: ClusterMeta,
    token: CancellationToken,
    palette: Palette,
    feed: Vec<FeedItem>,
    next_id: u64,
    input: String,
    input_rect: Option<egui::Rect>,
    suggestions: Vec<String>,
    filtered_suggestions: Vec<String>,
    events_tx: mpsc::Sender<AppEvent>,
    events_rx: mpsc::Receiver<AppEvent>,
    inspector: InspectorState,
    pulse_nodes: Vec<f64>,
    pulse_props: Vec<f64>,
    last_pulse_update: Instant,
}

#[derive(Default, Clone)]
struct InspectorState {
    is_open: bool,
    node_type: Option<String>,
    node_id: Option<String>,
    properties: Vec<(String, String)>,
    relationships: Vec<(String, String)>,
}

impl GuiApp {
    fn new(
        runtime: Handle,
        backend: Arc<dyn GraphBackend>,
        translator: Arc<dyn Translator>,
        cluster_state: SharedClusterState,
        token: CancellationToken,
        cluster_label: String,
    ) -> Self {
        let (events_tx, events_rx) = mpsc::channel();
        let suggestions = build_suggestions();
        let palette = Palette::default();
        Self {
            runtime,
            backend,
            translator,
            cluster_state,
            cluster_meta: ClusterMeta {
                label: cluster_label,
                connected: true,
            },
            token,
            palette,
            feed: Vec::new(),
            next_id: 1,
            input: String::new(),
            input_rect: None,
            suggestions,
            filtered_suggestions: Vec::new(),
            events_tx,
            events_rx,
            inspector: InspectorState::default(),
            pulse_nodes: vec![],
            pulse_props: vec![],
            last_pulse_update: Instant::now() - Duration::from_secs(10),
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
        let backend = self.backend.clone();
        let runtime = self.runtime.clone();

        runtime.spawn(async move {
            let _ = tx.send(AppEvent::TranslationStarted { id });
            match translator.translate(&question).await {
                Ok(cypher) => {
                    let _ = tx.send(AppEvent::TranslationCompleted {
                        id,
                        cypher: cypher.clone(),
                    });
                    match validate_cypher(&cypher) {
                        Ok(()) => {
                            let _ = tx.send(AppEvent::QueryStarted {
                                id,
                                cypher: cypher.clone(),
                            });
                            match backend.execute_query(cypher.clone()).await {
                                Ok(records) => {
                                    let _ = tx.send(AppEvent::QueryCompleted {
                                        id,
                                        cypher,
                                        records,
                                    });
                                }
                                Err(err) => {
                                    tracing::error!("Query failed: {err}");
                                    let _ = tx.send(AppEvent::QueryFailed {
                                        id,
                                        error: err.to_string(),
                                        cypher,
                                    });
                                }
                            }
                        }
                        Err(err) => {
                            tracing::error!("Validation failed: {err}");
                            let _ = tx.send(AppEvent::ValidationFailed {
                                id,
                                error: err.to_string(),
                                cypher,
                            });
                        }
                    }
                }
                Err(err) => {
                    tracing::error!("Translation failed: {err}");
                    let _ = tx.send(AppEvent::TranslationFailed {
                        id,
                        error: err.to_string(),
                    });
                }
            }
        });
    }

    fn rerun_cypher(&mut self, id: u64, cypher: String) {
        let tx = self.events_tx.clone();
        let backend = self.backend.clone();
        let runtime = self.runtime.clone();

        runtime.spawn(async move {
            match validate_cypher(&cypher) {
                Ok(()) => {
                    let _ = tx.send(AppEvent::QueryStarted {
                        id,
                        cypher: cypher.clone(),
                    });
                    match backend.execute_query(cypher.clone()).await {
                        Ok(records) => {
                            let _ = tx.send(AppEvent::QueryCompleted {
                                id,
                                cypher,
                                records,
                            });
                        }
                        Err(err) => {
                            tracing::error!("Query failed: {err}");
                            let _ = tx.send(AppEvent::QueryFailed {
                                id,
                                error: err.to_string(),
                                cypher,
                            });
                        }
                    }
                }
                Err(err) => {
                    tracing::error!("Validation failed: {err}");
                    let _ = tx.send(AppEvent::ValidationFailed {
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

    fn drain_events(&mut self) {
        while let Ok(event) = self.events_rx.try_recv() {
            match event {
                AppEvent::TranslationStarted { id } => {
                    if let Some(item) = self.feed_item_mut(id) {
                        item.state = FeedState::Translating;
                    }
                }
                AppEvent::TranslationCompleted { id, cypher } => {
                    if let Some(item) = self.feed_item_mut(id) {
                        item.cypher = Some(cypher);
                        item.state = FeedState::Validating;
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
                    }
                }
                AppEvent::QueryCompleted {
                    id,
                    cypher,
                    records,
                } => {
                    if let Some(item) = self.feed_item_mut(id) {
                        item.cypher = Some(cypher);
                        item.result = classify_result(&records);
                        item.state = FeedState::Ready;
                    }
                }
                AppEvent::QueryFailed { id, error, cypher } => {
                    if let Some(item) = self.feed_item_mut(id) {
                        item.cypher = Some(cypher);
                        item.state = FeedState::Error(error);
                    }
                }
            }
        }
    }

    fn feed_item_mut(&mut self, id: u64) -> Option<&mut FeedItem> {
        self.feed.iter_mut().find(|item| item.id == id)
    }

    fn update_pulse(&mut self) {
        let interval = Duration::from_secs(5);
        if self.last_pulse_update.elapsed() < interval {
            return;
        }
        let (node_count, prop_count) = {
            let guard = self
                .cluster_state
                .lock()
                .expect("cluster state lock poisoned");
            let node_count = guard.get_node_count();
            let prop_count = estimate_property_count(&guard, node_count);
            (node_count, prop_count)
        };
        push_sparkline(&mut self.pulse_nodes, node_count as f64);
        push_sparkline(&mut self.pulse_props, prop_count as f64);
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
            .fields
            .iter()
            .find(|(key, _)| key == "kind")
            .map(|(_, value)| value.clone());
        self.inspector.node_id = Some(row.title.clone());
        self.inspector.properties = row.fields.clone();
        self.inspector.relationships = vec![];
    }
}

impl eframe::App for GuiApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.drain_events();
        self.update_pulse();

        let screen_width = ctx.available_rect().width();
        let nav_width = if screen_width < 1100.0 { 72.0 } else { 96.0 };
        let inspector_width = if screen_width < 1000.0 { 0.0 } else { 340.0 };

        // HEADER
        egui::TopBottomPanel::top("header")
            .exact_height(56.0)
            .frame(
                Frame::new()
                    .fill(self.palette.bg_panel)
                    .stroke(Stroke::new(1.0, self.palette.border)),
            )
            .show(ctx, |ui| {
                ui.set_height(56.0);
                ui.horizontal(|ui| {
                    ui.add_space(16.0);
                    ui.label(
                        RichText::new("KubeGraph Ops")
                            .color(self.palette.text_primary)
                            .size(18.0)
                            .strong(),
                    );
                    ui.add_space(16.0);
                    let status = if self.cluster_meta.connected {
                        format!("Connected to: {}", self.cluster_meta.label)
                    } else {
                        "DISCONNECTED".to_string()
                    };
                    ui.label(RichText::new(status).color(self.palette.text_muted));

                    // Right aligned Pulse
                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        ui.add_space(16.0);
                        pulse_metric(
                            ui,
                            "Nodes",
                            self.pulse_nodes.last().copied().unwrap_or(0.0) as usize,
                            &self.pulse_nodes,
                            &self.palette,
                        );
                        ui.add_space(16.0);
                        pulse_metric(
                            ui,
                            "Properties",
                            self.pulse_props.last().copied().unwrap_or(0.0) as usize,
                            &self.pulse_props,
                            &self.palette,
                        );
                    });
                });
            });

        // FOOTER
        egui::TopBottomPanel::bottom("footer")
            .exact_height(72.0)
            .frame(
                Frame::new()
                    .fill(self.palette.bg_panel)
                    .stroke(Stroke::new(1.0, self.palette.border)),
            )
            .show(ctx, |ui| {
                ui.add_space(12.0);
                let mut has_focus = false;
                ui.horizontal(|ui| {
                    ui.add_space(16.0);
                    ui.label(RichText::new(">_").color(self.palette.accent).size(18.0));
                    ui.add_space(8.0);

                    let buttons_width = 240.0;
                    let available = ui.available_width() - buttons_width;

                    let response = ui.add_sized(
                        [available.max(200.0), 40.0],
                        TextEdit::singleline(&mut self.input)
                            .hint_text("Ask about your cluster...")
                            .font(TextStyle::Monospace)
                            .margin(Margin::same(10)),
                    );
                    self.input_rect = Some(response.rect);
                    has_focus = response.has_focus();

                    if response.lost_focus() && ctx.input(|i| i.key_pressed(egui::Key::Enter)) {
                        self.submit_question();
                    }

                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        ui.add_space(16.0);
                        if ui
                            .add_sized([92.0, 36.0], egui::Button::new("Run Query"))
                            .clicked()
                        {
                            self.submit_question();
                        }
                        ui.add_space(8.0);
                        let _ = ui.add_sized([60.0, 32.0], egui::Button::new("Help"));
                        ui.add_space(4.0);
                        let _ = ui.add_sized([72.0, 32.0], egui::Button::new("History"));
                    });
                });
                self.update_autocomplete();
                if has_focus && !self.filtered_suggestions.is_empty() {
                    if let Some(rect) = self.input_rect {
                        let row_height = 24.0;
                        let height = row_height * self.filtered_suggestions.len() as f32 + 12.0;
                        let pos = rect.left_top() - Vec2::new(0.0, height + 8.0);
                        egui::Area::new(egui::Id::new("autocomplete"))
                            .order(egui::Order::Foreground)
                            .fixed_pos(pos)
                            .show(ctx, |ui| {
                                Frame::new()
                                    .fill(self.palette.bg_elevated)
                                    .stroke(Stroke::new(1.0, self.palette.border))
                                    .corner_radius(CornerRadius::same(6))
                                    .inner_margin(Margin::same(6))
                                    .show(ui, |ui| {
                                        ui.set_width(rect.width());
                                        let suggestions = self.filtered_suggestions.clone();
                                        for suggestion in suggestions {
                                            if ui
                                                .button(
                                                    RichText::new(&suggestion)
                                                        .color(self.palette.text_primary),
                                                )
                                                .clicked()
                                            {
                                                self.apply_suggestion(&suggestion);
                                            }
                                        }
                                    });
                            });
                    }
                }
            });

        egui::SidePanel::left("nav")
            .exact_width(nav_width)
            .frame(
                Frame::new()
                    .fill(self.palette.bg_panel)
                    .stroke(Stroke::new(1.0, self.palette.border)),
            )
            .show(ctx, |ui| {
                ui.add_space(16.0);
                ui.vertical_centered(|ui| {
                    nav_button(ui, "H", "History");
                    nav_button(ui, "S", "Saved");
                    nav_button(ui, "!", "Alerts");
                });
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
                                for (key, value) in &self.inspector.properties {
                                    ui.horizontal_wrapped(|ui| {
                                        ui.label(
                                            RichText::new(format!("{key}:"))
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
                ui.add_space(8.0);
                ScrollArea::vertical()
                    .auto_shrink([false; 2])
                    .stick_to_bottom(true)
                    .show(ui, |ui| {
                        if self.feed.is_empty() {
                            ui.allocate_ui(
                                Vec2::new(ui.available_width(), ui.available_height() * 0.8),
                                |ui| {
                                    ui.centered_and_justified(|ui| {
                                        ui.label(
                                            RichText::new("Ask about your cluster...")
                                                .color(self.palette.text_muted)
                                                .size(20.0),
                                        );
                                    });
                                },
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

                        // Pad bottom to not be hidden behind footer
                        ui.add_space(20.0);
                    });
            });
    }

    fn on_exit(&mut self, _gl: Option<&eframe::glow::Context>) {
        self.token.cancel();
    }
}

fn setup_style(ctx: &egui::Context, palette: &Palette) {
    let mut visuals = egui::Visuals::dark();
    visuals.panel_fill = palette.bg_panel;
    visuals.window_fill = palette.bg_primary;
    visuals.extreme_bg_color = palette.bg_primary;
    visuals.widgets.noninteractive.bg_fill = palette.bg_panel;
    visuals.widgets.noninteractive.bg_stroke = Stroke::new(1.0, palette.border);
    visuals.widgets.inactive.bg_fill = palette.bg_elevated;
    visuals.widgets.active.bg_fill = palette.bg_elevated;
    visuals.widgets.hovered.bg_fill = palette.bg_elevated;
    visuals.selection.bg_fill = palette.accent;
    visuals.override_text_color = Some(palette.text_primary);

    // Borders
    visuals.widgets.inactive.bg_stroke = Stroke::new(1.0, palette.border);
    visuals.widgets.active.bg_stroke = Stroke::new(1.0, palette.accent);
    visuals.widgets.hovered.bg_stroke = Stroke::new(1.0, palette.accent);

    ctx.set_visuals(visuals);

    let mut style = (*ctx.style()).clone();
    style.text_styles.insert(
        TextStyle::Heading,
        FontId::new(18.0, FontFamily::Proportional),
    );
    style
        .text_styles
        .insert(TextStyle::Body, FontId::new(14.0, FontFamily::Proportional));
    style.text_styles.insert(
        TextStyle::Small,
        FontId::new(12.0, FontFamily::Proportional),
    );
    style.text_styles.insert(
        TextStyle::Monospace,
        FontId::new(13.0, FontFamily::Monospace),
    );
    style.text_styles.insert(
        TextStyle::Button,
        FontId::new(13.0, FontFamily::Proportional),
    );
    style.spacing.item_spacing = Vec2::new(8.0, 8.0);
    style.spacing.window_margin = Margin::same(8);
    ctx.set_style(style);
}

fn nav_button(ui: &mut egui::Ui, label: &str, tooltip: &str) {
    ui.add_space(8.0);
    ui.add_sized(
        [48.0, 48.0],
        egui::Button::new(RichText::new(label).size(16.0)),
    )
    .on_hover_text(tooltip);
}

fn pulse_metric(ui: &mut egui::Ui, label: &str, count: usize, series: &[f64], palette: &Palette) {
    let desired = Vec2::new(176.0, 40.0);
    ui.allocate_ui_with_layout(desired, Layout::left_to_right(Align::Center), |ui| {
        Frame::new()
            .fill(palette.bg_elevated)
            .stroke(Stroke::new(1.0, palette.border))
            .corner_radius(CornerRadius::same(6))
            .inner_margin(Margin::same(6))
            .show(ui, |ui| {
                ui.set_min_size(desired);
                ui.horizontal(|ui| {
                    ui.vertical(|ui| {
                        ui.add(
                            egui::Label::new(
                                RichText::new(label).color(palette.text_muted).size(10.0),
                            )
                            .truncate(),
                        );
                        ui.add(
                            egui::Label::new(
                                RichText::new(format_count(count))
                                    .color(palette.text_primary)
                                    .size(14.0)
                                    .strong(),
                            )
                            .truncate(),
                        );
                    });
                    ui.add_space(6.0);
                    let spark_size = Vec2::new(60.0, 24.0);
                    let (response, painter) = ui.allocate_painter(spark_size, egui::Sense::hover());
                    draw_sparkline(painter, response.rect, series, palette.accent);
                });
            });
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

fn render_feed_item(
    ui: &mut egui::Ui,
    item: &FeedItem,
    palette: &Palette,
    mut on_run: impl FnMut(u64, String),
    mut on_select: impl FnMut(&RowCard),
) {
    ui.add_space(16.0);

    // User Question Bubble
    ui.horizontal(|ui| {
        let max_width = ui.available_width() * 0.7;

        Frame::new()
            .fill(palette.bg_elevated)
            .stroke(Stroke::new(1.0, palette.border))
            .corner_radius(CornerRadius::same(16))
            .inner_margin(Margin::same(14))
            .show(ui, |ui| {
                ui.set_max_width(max_width);
                ui.horizontal(|ui| {
                    ui.label(
                        RichText::new("User > ")
                            .color(palette.text_muted)
                            .size(12.0),
                    );
                    ui.label(
                        RichText::new(&item.user_text)
                            .color(palette.text_primary)
                            .size(15.0),
                    );
                });
            });
    });

    ui.add_space(8.0);

    // System Response Block
    Frame::new().fill(Color32::TRANSPARENT).show(ui, |ui| {
        // 1. Cypher Layer (if present)
        if let Some(cypher) = &item.cypher {
            let id = ui.make_persistent_id(format!("cypher-header-{}", item.id));
            egui::collapsing_header::CollapsingState::load_with_default_open(ui.ctx(), id, true)
                .show_header(ui, |ui| {
                    ui.label(
                        RichText::new("GENERATED CYPHER")
                            .size(12.0)
                            .color(palette.accent),
                    );
                })
                .body(|ui| {
                    Frame::new()
                        .fill(palette.bg_primary)
                        .stroke(Stroke::new(1.0, palette.border))
                        .corner_radius(CornerRadius::same(6))
                        .inner_margin(Margin::same(8))
                        .show(ui, |ui| {
                            ui.horizontal(|ui| {
                                let mut job = highlight_cypher(cypher, palette);
                                job.wrap.max_width = ui.available_width() - 80.0;
                                ui.label(job);

                                ui.with_layout(Layout::right_to_left(Align::Min), |ui| {
                                    if ui.small_button("Run").clicked() {
                                        on_run(item.id, cypher.clone());
                                    }
                                    if ui.small_button("Copy").clicked() {
                                        ui.ctx().copy_text(cypher.clone());
                                    }
                                });
                            });
                        });
                });
        }

        ui.add_space(8.0);

        // 2. Result / Status Layer
        Frame::new()
            .fill(palette.bg_elevated)
            .stroke(Stroke::new(1.0, palette.border))
            .corner_radius(CornerRadius::same(12))
            .inner_margin(Margin::same(16))
            .show(ui, |ui| {
                // Header for result area
                ui.horizontal(|ui| {
                    ui.label(
                        RichText::new("VISUAL RESULT AREA")
                            .size(10.0)
                            .color(palette.text_muted),
                    );
                });
                ui.add_space(4.0);

                match &item.state {
                    FeedState::Translating => {
                        ui.horizontal(|ui| {
                            ui.add(egui::Spinner::new());
                            ui.label(RichText::new("Translating to Cypher...").italics());
                        });
                    }
                    FeedState::Validating => {
                        ui.horizontal(|ui| {
                            ui.add(egui::Spinner::new());
                            ui.label(RichText::new("Validating query...").italics());
                        });
                    }
                    FeedState::Running => {
                        ui.horizontal(|ui| {
                            ui.add(egui::Spinner::new());
                            ui.label(RichText::new("Executing query on graph...").italics());
                        });
                    }
                    FeedState::Error(err) => {
                        ui.colored_label(palette.danger, format!("Error: {err}"));
                    }
                    FeedState::Ready => {
                        render_result(ui, item, palette, &mut on_select);
                    }
                }

                ui.add_space(12.0);
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
            ui.vertical_centered(|ui| {
                ui.add_space(10.0);
                ui.horizontal(|ui| {
                    // Skull icon placeholder
                    ui.label(RichText::new("💀").size(32.0));
                    ui.add_space(10.0);
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
                ui.add_space(10.0);
            });
        }
        ResultPayload::List { rows } => {
            // "VISUAL RESULT AREA" header is already rendered by render_feed_item, so we remove it here.

            ui.add_space(6.0);
            let frame = Frame::new()
                .fill(palette.bg_panel)
                .stroke(Stroke::new(1.0, palette.border))
                .corner_radius(CornerRadius::same(8))
                .inner_margin(Margin::same(8));
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

                let show_namespace = rows.iter().any(|r| r.subtitle.is_some());
                let show_status = rows.iter().any(|r| r.status.is_some());

                let mut columns = vec!["Name".to_string()];
                if show_namespace {
                    columns.push("Namespace".to_string());
                }
                if show_status {
                    columns.push("Status".to_string());
                }
                columns.extend(extra_keys.clone());

                egui::Grid::new("result_table")
                    .striped(true)
                    .min_col_width(120.0)
                    .show(ui, |ui| {
                        for header in &columns {
                            ui.label(
                                RichText::new(header)
                                    .color(palette.text_muted)
                                    .size(11.0)
                                    .strong(),
                            );
                        }
                        ui.end_row();

                        for (idx, row) in rows.iter().enumerate() {
                            let clicked = ui.selectable_label(false, &row.title).clicked();

                            if show_namespace {
                                let namespace = row.subtitle.as_deref().unwrap_or("-");
                                ui.label(namespace);
                            }
                            if show_status {
                                let status = row.status.as_deref().unwrap_or("-");
                                ui.label(status);
                            }

                            for key in &extra_keys {
                                ui.label(find_field(&row.fields, key).unwrap_or("-"));
                            }
                            ui.end_row();
                            if clicked {
                                on_select(row);
                            }
                            let _ = idx;
                        }
                    });
            });
        }
        ResultPayload::Graph { nodes, edges } => {
            Frame::new()
                .fill(palette.bg_primary) // contrast
                .stroke(Stroke::new(1.0, palette.border))
                .corner_radius(CornerRadius::same(8))
                .inner_margin(Margin::same(0))
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

    // Draw background grid (subtle)
    let grid_step = 40.0;
    let mut x = rect.left();
    while x < rect.right() {
        painter.line_segment(
            [egui::pos2(x, rect.top()), egui::pos2(x, rect.bottom())],
            Stroke::new(0.5, palette.bg_elevated),
        );
        x += grid_step;
    }
    let mut y = rect.top();
    while y < rect.bottom() {
        painter.line_segment(
            [egui::pos2(rect.left(), y), egui::pos2(rect.right(), y)],
            Stroke::new(0.5, palette.bg_elevated),
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
            painter.line_segment([*from, *to], Stroke::new(1.0, palette.text_muted));
            if let Some(label) = &edge.label {
                let mid = egui::pos2((from.x + to.x) * 0.5, (from.y + to.y) * 0.5);
                // Draw label background
                painter.rect_filled(
                    egui::Rect::from_center_size(
                        mid,
                        Vec2::new(label.len() as f32 * 6.0 + 8.0, 14.0),
                    ),
                    CornerRadius::same(4),
                    palette.bg_primary,
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
            // Node glow
            painter.circle_filled(*pos, 10.0, palette.accent.gamma_multiply(0.2));
            painter.circle_filled(*pos, 6.0, palette.accent);

            // Label
            painter.text(
                *pos + Vec2::new(0.0, 14.0),
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

    let mut fields: Vec<(String, String)> = obj
        .iter()
        .map(|(k, v)| (k.clone(), format_value(v)))
        .collect();
    fields.sort_by(|a, b| a.0.cmp(&b.0));

    RowCard {
        title,
        subtitle,
        status,
        fields,
    }
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(v) => v.to_string(),
        Value::Number(v) => v.to_string(),
        Value::String(v) => v.clone(),
        Value::Array(arr) => format!("array({})", arr.len()),
        Value::Object(obj) => format!("object({})", obj.len()),
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
