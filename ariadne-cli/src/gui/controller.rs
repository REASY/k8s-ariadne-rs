use super::*;

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

        let tx = self.events_tx.clone();
        let translator = self.translator.clone();
        let router = self.router.clone();
        let agentic = self.agentic.clone();
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
            let mut usage_acc = UsageAccumulator::default();

            send_event(AppEvent::TranslationStarted { id });

            let mut route = RouteDecision::OneShot;
            let route_start = Instant::now();
            match router.classify(&question).await {
                Ok(route_result) => {
                    let route_ms = route_start.elapsed().as_millis();
                    log_llm_call("router", route_ms, route_result.usage.as_ref());
                    usage_acc.add(route_result.usage.as_ref());
                    route = route_result.decision;
                    let steps = if route == RouteDecision::OneShot {
                        Some(0)
                    } else {
                        None
                    };
                    send_event(AppEvent::RouteDecided { id, route, steps });
                }
                Err(err) => {
                    tracing::warn!("Router failed, falling back to one-shot: {err}");
                    send_event(AppEvent::RouteDecided {
                        id,
                        route: RouteDecision::OneShot,
                        steps: Some(0),
                    });
                }
            }

            if route == RouteDecision::MultiTurn {
                let plan_start = Instant::now();
                match agentic
                    .plan(
                        &question,
                        &analysis_context,
                        analysis_summary.as_deref(),
                        backend.as_ref(),
                    )
                    .await
                {
                    Ok(plan) => {
                        let plan_ms = plan_start.elapsed().as_millis();
                        log_llm_call("agentic", plan_ms, plan.usage.as_ref());
                        usage_acc.add(plan.usage.as_ref());
                        let params = merge_params(plan.params.clone(), &analysis_context);
                        send_event(AppEvent::RouteDecided {
                            id,
                            route: RouteDecision::MultiTurn,
                            steps: Some(plan.steps.len()),
                        });

                        send_event(AppEvent::TranslationCompleted {
                            id,
                            cypher: plan.cypher.clone(),
                            params: params.clone(),
                            usage: usage_acc.build(),
                            duration_ms: plan_ms,
                        });

                        match validate_cypher(&plan.cypher) {
                            Ok(()) => {
                                let cypher = plan.cypher.clone();
                                send_event(AppEvent::QueryStarted {
                                    id,
                                    cypher: cypher.clone(),
                                    params: params.clone(),
                                });
                                let exec_start = Instant::now();
                                match backend.execute_query(cypher.clone(), params.clone()).await {
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
                                                let analysis_ms =
                                                    analysis_start.elapsed().as_millis();
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
                                                let analysis_ms =
                                                    analysis_start.elapsed().as_millis();
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
                                        let issue = classify_ariadne_error(&err);
                                        tracing::error!("Query failed: {err}");
                                        send_event(AppEvent::QueryFailed {
                                            id,
                                            error: issue.to_string(),
                                            cypher,
                                            duration_ms: exec_ms,
                                        });
                                    }
                                }
                                return;
                            }
                            Err(issue) => {
                                tracing::error!("Validation failed: {issue}");
                                send_event(AppEvent::ValidationFailed {
                                    id,
                                    error: issue.to_string(),
                                    cypher: plan.cypher,
                                });
                                return;
                            }
                        }
                    }
                    Err(err) => {
                        tracing::warn!("Agentic planning failed, falling back to one-shot: {err}");
                        send_event(AppEvent::RouteDecided {
                            id,
                            route: RouteDecision::OneShot,
                            steps: Some(0),
                        });
                    }
                }
            }

            let mut attempt = 0usize;
            let mut feedback: Option<String> = None;

            loop {
                attempt += 1;
                send_event(AppEvent::TranslationStarted { id });
                let llm_start = Instant::now();
                let result = translator
                    .translate(
                        &question,
                        &analysis_context,
                        analysis_summary.as_deref(),
                        feedback.as_deref(),
                    )
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
                usage_acc.add(result.usage.as_ref());

                let params = merge_params(result.params.clone(), &analysis_context);

                send_event(AppEvent::TranslationCompleted {
                    id,
                    cypher: result.cypher.clone(),
                    params: params.clone(),
                    usage: usage_acc.build(),
                    duration_ms: llm_ms,
                });

                match validate_cypher(&result.cypher) {
                    Ok(()) => {
                        let cypher = result.cypher.clone();
                        send_event(AppEvent::QueryStarted {
                            id,
                            cypher: cypher.clone(),
                            params: params.clone(),
                        });
                        let exec_start = Instant::now();
                        match backend.execute_query(cypher.clone(), params.clone()).await {
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
                                let issue = classify_ariadne_error(&err);
                                if attempt <= LLM_MAX_RETRIES && issue.repairable() {
                                    feedback = Some(issue.feedback());
                                    continue;
                                }
                                tracing::error!("Query failed: {err}");
                                send_event(AppEvent::QueryFailed {
                                    id,
                                    error: issue.to_string(),
                                    cypher,
                                    duration_ms: exec_ms,
                                });
                            }
                        }
                        return;
                    }
                    Err(issue) => {
                        tracing::error!("Validation failed: {issue}");
                        if attempt <= LLM_MAX_RETRIES && issue.repairable() {
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

    pub(super) fn rerun_cypher(&mut self, id: u64, cypher: String) {
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
        let params = self
            .feed
            .iter()
            .find(|item| item.id == id)
            .and_then(|item| item.params.clone());
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
                        params: params.clone(),
                    });
                    let exec_start = Instant::now();
                    match backend.execute_query(cypher.clone(), params.clone()).await {
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
                            let issue = classify_ariadne_error(&err);
                            tracing::error!("Query failed: {err}");
                            send_event(AppEvent::QueryFailed {
                                id,
                                error: issue.to_string(),
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
                AppEvent::RouteDecided { id, route, steps } => {
                    if let Some(item) = self.feed_item_mut(id) {
                        item.route = Some(route);
                        if let Some(steps) = steps {
                            item.agent_steps = Some(steps);
                        }
                    }
                }
                AppEvent::TranslationStarted { id } => {
                    if let Some(item) = self.feed_item_mut(id) {
                        item.state = FeedState::Translating;
                    }
                }
                AppEvent::TranslationCompleted {
                    id,
                    cypher,
                    params,
                    usage,
                    duration_ms,
                } => {
                    if let Some(item) = self.feed_item_mut(id) {
                        item.cypher = Some(cypher);
                        item.params = params;
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
                AppEvent::QueryStarted { id, cypher, params } => {
                    if let Some(item) = self.feed_item_mut(id) {
                        item.cypher = Some(cypher);
                        item.params = params;
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
                        item.context_bindings = extract_context_bindings(&records);
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

    pub(super) fn feed_item_mut(&mut self, id: u64) -> Option<&mut FeedItem> {
        self.feed.iter_mut().find(|item| item.id == id)
    }

    pub(super) fn context_budget_tokens(&self) -> Option<usize> {
        let total = self.context_window_tokens?;
        let budget = total.saturating_sub(CONTEXT_RESERVED_TOKENS);
        Some(budget.max(CONTEXT_MIN_TOKENS).min(total))
    }

    pub(super) fn build_context_with_budget(&self) -> Vec<ConversationTurn> {
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

    pub(super) fn build_context(&self, limit: usize) -> Vec<ConversationTurn> {
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
                bindings: item.context_bindings.clone(),
            });
        }
        turns.reverse();
        turns
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
