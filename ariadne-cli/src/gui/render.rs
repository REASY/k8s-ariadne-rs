//! Stateless egui rendering for feed cards, results, analysis, and graphs.
//!
//! Renderers communicate actions through callbacks and never schedule backend
//! or LLM work directly.

use super::{
    Align, Align2, Color32, Column, CornerRadius, FeedItem, FeedState, FontFamily, FontId, Frame,
    GraphEdge, GraphNode, Layout, LayoutJob, Map, Margin, Palette, ResultPayload, RichText,
    RowCard, ScrollArea, Stroke, TableBuilder, TextEdit, TextFormat, TextStyle, Value, Vec2, egui,
    find_field, format_duration, format_value, lighten_color,
};

#[path = "render/pulse.rs"]
mod pulse;
pub(super) use pulse::render_graph_pulse;

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
        && item.route.is_none()
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
        if let Some(route) = item.route {
            let mut label = format!("route {}", route.as_str());
            if let Some(steps) = item.agent_steps {
                label.push_str(&format!(" steps {steps}"));
            }
            ui.label(RichText::new(label).color(palette.text_muted).size(11.0));
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

pub(super) fn render_feed_item(
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

pub(super) fn push_sparkline(series: &mut Vec<f64>, value: f64) {
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
