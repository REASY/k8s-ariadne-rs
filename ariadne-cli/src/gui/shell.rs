//! eframe shell for arranging panels and dispatching user interactions.
//!
//! The shell may call controller methods but must not duplicate query,
//! context-selection, or result-classification policy.

use super::{
    Align, Color32, CornerRadius, Frame, GRAPH_PULSE_HEIGHT, GuiApp, InspectorValue, Layout,
    Margin, RichText, RowCard, SHORT_TERM_CONTEXT_LIMIT, ScrollArea, Stroke, TextEdit, TextStyle,
    Vec2, egui, estimate_context_tokens, format_duration, render_feed_item, render_graph_pulse,
};

impl eframe::App for GuiApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = ui.ctx().clone();
        if self.drain_events() {
            ctx.request_repaint();
        }
        self.update_pulse();

        let screen_width = ctx.content_rect().width();
        let inspector_width = if screen_width < 1100.0 { 0.0 } else { 320.0 };

        // HEADER
        egui::Panel::top("header")
            .exact_size(56.0)
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
            .show(ui, |ui| {
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
        egui::Panel::bottom("footer")
            .exact_size(74.0)
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
            .show(ui, |ui| {
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
                    && let Some(first) = self.filtered_suggestions.first().cloned()
                {
                    self.apply_suggestion(&first);
                    ctx.input_mut(|i| i.consume_key(egui::Modifiers::NONE, egui::Key::Tab));
                    if let Some(id) = input_id {
                        ctx.memory_mut(|mem| mem.request_focus(id));
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
                if let Some(rect) = autocomplete_rect
                    && ctx.input(|i| i.pointer.hover_pos().is_some_and(|p| rect.contains(p)))
                {
                    show_autocomplete = true;
                }
                if show_autocomplete
                    && !self.filtered_suggestions.is_empty()
                    && let Some(rect) = self.input_rect
                {
                    let row_height = 24.0;
                    let height = row_height * self.filtered_suggestions.len() as f32 + 18.0;
                    let pos = rect.left_top() - Vec2::new(0.0, height + 10.0);
                    egui::Area::new(egui::Id::new("autocomplete"))
                        .order(egui::Order::Foreground)
                        .fixed_pos(pos)
                        .show(&ctx, |ui| {
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
            });

        if self.inspector.is_open && inspector_width > 0.0 {
            egui::Panel::right("inspector")
                .exact_size(inspector_width)
                .frame(
                    Frame::new()
                        .fill(self.palette.bg_panel)
                        .stroke(Stroke::new(1.0, self.palette.border)),
                )
                .show(ui, |ui| {
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
            .show(ui, |ui| {
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
                                if self.context_compact_summary.is_some()
                                    && let Some(ms) = self.context_compact_duration_ms
                                {
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

    fn on_exit(&mut self) {
        self.token.cancel();
    }
}
