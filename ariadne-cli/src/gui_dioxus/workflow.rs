use super::*;

pub(super) fn submit_question(context: &AppContext, question: String) {
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
    let router = context.router.clone();
    let agentic = context.agentic.clone();
    let analyst = context.analyst.clone();
    let analysis_context = build_context_with_budget(&context, &read_shared(&context));
    let analysis_summary = read_shared(&context).context_compact_summary.clone();

    runtime.spawn(async move {
        let mut usage_acc = UsageAccumulator::default();

        update_feed_item(&context, id, |item| {
            item.state = FeedState::Translating;
            item.analysis = None;
            item.analysis_error = None;
            item.analysis_pending = false;
        });
        notify(&context);

        let mut route = RouteDecision::OneShot;
        let route_start = Instant::now();
        match router.classify(&question).await {
            Ok(route_result) => {
                let route_ms = route_start.elapsed().as_millis();
                log_llm_call("router", route_ms, route_result.usage.as_ref());
                usage_acc.add(route_result.usage.as_ref());
                route = route_result.decision;
                update_feed_item(&context, id, |item| {
                    item.route = Some(route);
                    if route == RouteDecision::OneShot {
                        item.agent_steps = Some(0);
                    }
                });
                notify(&context);
            }
            Err(err) => {
                tracing::warn!("Router failed, falling back to one-shot: {err}");
                update_feed_item(&context, id, |item| {
                    item.route = Some(RouteDecision::OneShot);
                    item.agent_steps = Some(0);
                });
                notify(&context);
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
                    update_feed_item(&context, id, |item| {
                        item.route = Some(RouteDecision::MultiTurn);
                        item.agent_steps = Some(plan.steps.len());
                    });
                    notify(&context);

                    update_feed_item(&context, id, |item| {
                        item.cypher = Some(plan.cypher.clone());
                        item.params = params.clone();
                        item.llm_usage = usage_acc.build();
                        item.llm_duration_ms = Some(plan_ms);
                        item.state = FeedState::Validating;
                    });
                    notify(&context);

                    match validate_cypher(&plan.cypher) {
                        Ok(()) => {
                            let cypher = plan.cypher.clone();
                            update_feed_item(&context, id, |item| {
                                item.state = FeedState::Running;
                                item.params = params.clone();
                            });
                            notify(&context);

                            let exec_start = Instant::now();
                            match backend.execute_query(cypher.clone(), params.clone()).await {
                                Ok(records) => {
                                    let exec_ms = exec_start.elapsed().as_millis();
                                    let summary = summarize_records(&records);
                                    let classified = classify_result(&records);
                                    update_feed_item(&context, id, |item| {
                                        item.state = FeedState::Ready;
                                        item.result = classified;
                                        item.exec_duration_ms = Some(exec_ms);
                                        item.context_summary = Some(summary.clone());
                                        item.context_bindings = extract_context_bindings(&records);
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
                                            log_llm_call(
                                                "analysis",
                                                analysis_ms,
                                                analysis.usage.as_ref(),
                                            );
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
                                    let issue = classify_ariadne_error(&err);
                                    update_feed_item(&context, id, |item| {
                                        item.state = FeedState::Error(issue.to_string());
                                        item.exec_duration_ms = Some(exec_ms);
                                    });
                                    notify(&context);
                                }
                            }
                            return;
                        }
                        Err(issue) => {
                            update_feed_item(&context, id, |item| {
                                item.state = FeedState::Error(issue.to_string());
                            });
                            notify(&context);
                            return;
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!("Agentic planning failed, falling back to one-shot: {err}");
                    update_feed_item(&context, id, |item| {
                        item.route = Some(RouteDecision::OneShot);
                        item.agent_steps = Some(0);
                    });
                    notify(&context);
                }
            }
        }

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
                    update_feed_item(&context, id, |item| {
                        item.state = FeedState::Error(err.to_string());
                        item.llm_duration_ms = Some(llm_ms);
                    });
                    notify(&context);
                    return;
                }
            };
            log_llm_call("translator", llm_ms, result.usage.as_ref());
            usage_acc.add(result.usage.as_ref());

            let params = merge_params(result.params.clone(), &analysis_context);

            update_feed_item(&context, id, |item| {
                item.cypher = Some(result.cypher.clone());
                item.params = params.clone();
                item.llm_usage = usage_acc.build();
                item.llm_duration_ms = Some(llm_ms);
                item.state = FeedState::Validating;
            });
            notify(&context);

            match validate_cypher(&result.cypher) {
                Ok(()) => {
                    let cypher = result.cypher.clone();
                    update_feed_item(&context, id, |item| {
                        item.state = FeedState::Running;
                        item.params = params.clone();
                    });
                    notify(&context);

                    let exec_start = Instant::now();
                    match backend.execute_query(cypher.clone(), params.clone()).await {
                        Ok(records) => {
                            let exec_ms = exec_start.elapsed().as_millis();
                            let summary = summarize_records(&records);
                            let classified = classify_result(&records);
                            update_feed_item(&context, id, |item| {
                                item.state = FeedState::Ready;
                                item.result = classified;
                                item.exec_duration_ms = Some(exec_ms);
                                item.context_summary = Some(summary.clone());
                                item.context_bindings = extract_context_bindings(&records);
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
                            let issue = classify_ariadne_error(&err);
                            if attempt <= LLM_MAX_RETRIES && issue.repairable() {
                                feedback = Some(issue.feedback());
                                continue;
                            }
                            update_feed_item(&context, id, |item| {
                                item.state = FeedState::Error(issue.to_string());
                                item.exec_duration_ms = Some(exec_ms);
                            });
                            notify(&context);
                        }
                    }
                    return;
                }
                Err(issue) => {
                    if attempt <= LLM_MAX_RETRIES && issue.repairable() {
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

pub(super) fn rerun_cypher(context: &AppContext, id: u64, cypher: String) {
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
    let params = {
        let shared = context.shared.lock().expect("shared state lock poisoned");
        shared
            .feed
            .iter()
            .find(|item| item.id == id)
            .and_then(|item| item.params.clone())
    };
    let analysis_context = build_context_with_budget(&context, &read_shared(&context));
    let analysis_summary = read_shared(&context).context_compact_summary.clone();

    runtime.spawn(async move {
        match validate_cypher(&cypher) {
            Ok(()) => {
                update_feed_item(&context, id, |item| {
                    item.state = FeedState::Running;
                    item.params = params.clone();
                    item.analysis = None;
                    item.analysis_error = None;
                    item.analysis_pending = false;
                });
                notify(&context);

                let exec_start = Instant::now();
                match backend.execute_query(cypher.clone(), params.clone()).await {
                    Ok(records) => {
                        let exec_ms = exec_start.elapsed().as_millis();
                        let summary = summarize_records(&records);
                        let classified = classify_result(&records);
                        update_feed_item(&context, id, |item| {
                            item.state = FeedState::Ready;
                            item.result = classified;
                            item.exec_duration_ms = Some(exec_ms);
                            item.context_summary = Some(summary.clone());
                            item.context_bindings = extract_context_bindings(&records);
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
                        let issue = classify_ariadne_error(&err);
                        update_feed_item(&context, id, |item| {
                            item.state = FeedState::Error(issue.to_string());
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

pub(super) fn reset_context(context: &AppContext) {
    update_shared(context, |shared| {
        shared.context_cutoff_id = shared.next_id;
        shared.context_compact_summary = None;
        shared.context_compact_usage = None;
        shared.context_compact_duration_ms = None;
        shared.context_compact_error = None;
        shared.context_compacting = false;
    });
}

pub(super) fn start_context_compaction(context: &AppContext) {
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

pub(super) fn open_inspector_from_row(context: &AppContext, row: &RowCard) {
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
