//! egui application state transitions and asynchronous workflow adapter.
//!
//! Rendering belongs in `render`/`shell`; this module owns mutation of
//! `GuiApp` and translation of asynchronous outcomes into app events.

use super::{
    Agentic, Analyst, AppEvent, Arc, COMPACT_CONTEXT_LIMIT, CancellationToken, ClusterMeta,
    ConversationTurn, Duration, FeedItem, FeedState, GraphBackend, GuiApp, Handle,
    InspectorProperty, InspectorState, Instant, Palette, ResourceType, ResultPayload, Router,
    RowCard, SharedClusterState, Translator, build_suggestions, estimate_property_count,
    filter_suggestions, inspector_value, push_sparkline, replace_last_token, select_context,
    select_context_with_budget,
};
use crate::gui_workflow::{WorkflowServices, rerun_query, run_question};
use std::sync::mpsc;

impl GuiApp {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        runtime: Handle,
        backend: Arc<dyn GraphBackend>,
        translator: Arc<dyn Translator>,
        router: Arc<dyn Router>,
        agentic: Arc<dyn Agentic>,
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
            router,
            agentic,
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

    pub(super) fn submit_question(&mut self) {
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

        let services = WorkflowServices::new(
            self.backend.clone(),
            self.translator.clone(),
            self.router.clone(),
            self.agentic.clone(),
            self.analyst.clone(),
        );
        let context = self.build_context_with_budget();
        let context_summary = self.context_compact_summary.clone();
        let tx = self.events_tx.clone();
        let egui_ctx = self.egui_ctx.clone();
        self.runtime.spawn(async move {
            run_question(services, question, context, context_summary, |patch| {
                let _ = tx.send(AppEvent::FeedPatch { id, patch });
                egui_ctx.request_repaint();
            })
            .await;
        });
    }

    pub(super) fn rerun_cypher(&mut self, id: u64, cypher: String) {
        let Some(item) = self.feed.iter().find(|item| item.id == id) else {
            return;
        };
        let question = item.user_text.clone();
        let params = item.params.clone();
        let services = WorkflowServices::new(
            self.backend.clone(),
            self.translator.clone(),
            self.router.clone(),
            self.agentic.clone(),
            self.analyst.clone(),
        );
        let context = self.build_context_with_budget();
        let context_summary = self.context_compact_summary.clone();
        let tx = self.events_tx.clone();
        let egui_ctx = self.egui_ctx.clone();
        self.runtime.spawn(async move {
            rerun_query(
                services,
                question,
                cypher,
                params,
                context,
                context_summary,
                |patch| {
                    let _ = tx.send(AppEvent::FeedPatch { id, patch });
                    egui_ctx.request_repaint();
                },
            )
            .await;
        });
    }

    pub(super) fn handle_slash_command(&mut self, input: &str) -> bool {
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

    pub(super) fn drain_events(&mut self) -> bool {
        let mut handled = false;
        while let Ok(event) = self.events_rx.try_recv() {
            handled = true;
            match event {
                AppEvent::FeedPatch { id, patch } => {
                    if let Some(item) = self.feed_item_mut(id) {
                        patch.apply(item);
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

    pub(super) fn feed_item_mut(&mut self, id: u64) -> Option<&mut FeedItem> {
        self.feed.iter_mut().find(|item| item.id == id)
    }

    pub(super) fn context_budget_tokens(&self) -> Option<usize> {
        crate::gui_context::context_budget_tokens(self.context_window_tokens)
    }

    pub(super) fn build_context_with_budget(&self) -> Vec<ConversationTurn> {
        select_context_with_budget(
            &self.feed,
            self.context_cutoff_id,
            self.context_compact_summary.as_deref(),
            self.context_window_tokens,
        )
    }

    pub(super) fn build_context(&self, limit: usize) -> Vec<ConversationTurn> {
        select_context(&self.feed, self.context_cutoff_id, limit)
    }

    pub(super) fn build_context_for_compaction(&self, limit: usize) -> Vec<ConversationTurn> {
        self.build_context(limit)
    }

    pub(super) fn reset_context(&mut self) {
        self.context_cutoff_id = self.next_id;
        self.context_compact_summary = None;
        self.context_compact_usage = None;
        self.context_compact_duration_ms = None;
        self.context_compact_error = None;
        self.context_compacting = false;
    }

    pub(super) fn start_context_compaction(&mut self) {
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

    pub(super) fn update_pulse(&mut self) {
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

    pub(super) fn update_autocomplete(&mut self) {
        self.filtered_suggestions = filter_suggestions(&self.input, &self.suggestions);
    }

    pub(super) fn apply_suggestion(&mut self, suggestion: &str) {
        let replaced = replace_last_token(&self.input, suggestion);
        self.input = replaced;
        self.filtered_suggestions.clear();
    }

    pub(super) fn open_inspector_from_row(&mut self, row: &RowCard) {
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
