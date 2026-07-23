use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use eframe::egui;
use eframe::egui::{
    Align, Align2, Color32, CornerRadius, FontFamily, FontId, Frame, Layout, Margin, RichText,
    ScrollArea, Stroke, TextEdit, TextFormat, TextStyle, Vec2, text::LayoutJob,
};
use egui_extras::{Column, TableBuilder};
use serde_json::{Map, Value};
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;

use ariadne_core::graph_backend::GraphBackend;
use ariadne_core::state::SharedClusterState;
use ariadne_core::types::ResourceType;

use crate::agent::{Agentic, Analyst, ConversationTurn, LlmUsage, Router, Translator};
use crate::error::CliResult;
use crate::gui_context::{
    COMPACT_CONTEXT_LIMIT, SHORT_TERM_CONTEXT_LIMIT, build_context as select_context,
    build_context_with_budget as select_context_with_budget, filter_suggestions,
};
use crate::gui_results::{
    build_suggestions, estimate_property_count, find_field, format_count, format_value,
    inspector_value, replace_last_token, truncate_text,
};
use crate::gui_shared::{
    FeedItem, FeedState, GraphEdge, GraphNode, InspectorProperty, InspectorState, InspectorValue,
    ResultPayload, RowCard, estimate_context_tokens, format_duration,
};
use crate::gui_workflow::FeedPatch;

#[path = "gui/render.rs"]
mod render;
use render::{push_sparkline, render_feed_item, render_graph_pulse};
#[path = "gui/controller.rs"]
mod controller;
#[path = "gui/shell.rs"]
mod shell;

const GRAPH_PULSE_HEIGHT: f32 = 40.0;

pub struct GuiArgs {
    pub runtime_handle: tokio::runtime::Handle,
    pub backend: Arc<dyn GraphBackend>,
    pub translator: Arc<dyn Translator>,
    pub router: Arc<dyn Router>,
    pub agentic: Arc<dyn Agentic>,
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
    let router = args.router.clone();
    let agentic = args.agentic.clone();
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
                router.clone(),
                agentic.clone(),
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

    let mut style = (*ctx.global_style()).clone();
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
    ctx.set_global_style(style);
}

enum AppEvent {
    FeedPatch {
        id: u64,
        patch: FeedPatch,
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
    router: Arc<dyn Router>,
    agentic: Arc<dyn Agentic>,
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
        assert_eq!(crate::gui_results::current_token("MATCH (p:Pod"), "Pod");
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
