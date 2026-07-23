use super::*;

#[allow(clippy::too_many_arguments)]
pub(crate) fn render_graph_pulse(
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
