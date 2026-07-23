use super::*;

pub(super) fn render_feed_card(item: &FeedItem, context: &AppContext) -> Element {
    let item = item.clone();
    let context = context.clone();
    let id = item.id;
    let cypher_block = item.cypher.as_ref().map(|cypher| {
        let spans = highlight_cypher_spans(cypher);
        let cypher_text = cypher.to_string();
        let context_for_run = context.clone();
        let run_action = move |_| {
            rerun_cypher(&context_for_run, id, cypher_text.clone());
        };
        rsx! {
            details { class: "cypher-block", open: true,
                summary { "Planned Query" }
                div { class: "cypher",
                    {spans.into_iter()}
                }
                div { class: "cypher-actions",
                    button { class: "button secondary", onclick: run_action, "Run" }
                }
            }
        }
    });

    let render_state = match &item.state {
        FeedState::Translating => "Translating...",
        FeedState::Validating => "Validating...",
        FeedState::Running => "Running...",
        FeedState::Ready => "Ready",
        FeedState::Error(_) => "Error",
    };

    rsx! {
        div { class: "card",
            div { class: "card-header",
                div { class: "question", "{item.user_text}" }
                div { class: "state", "{render_state}" }
            }
            {cypher_block}
            if item.llm_duration_ms.is_some()
                || item.exec_duration_ms.is_some()
                || item.llm_usage.is_some()
                || item.route.is_some()
            {
                div { class: "meta",
                    if let Some(ms) = item.llm_duration_ms { span { "llm {format_duration(ms)}" } }
                    if let Some(ms) = item.exec_duration_ms { span { " · query {format_duration(ms)}" } }
                    if let Some(usage) = item.llm_usage.as_ref() {
                        span { " · tokens {usage.prompt_tokens}/{usage.completion_tokens}/{usage.total_tokens}" }
                    }
                    if let Some(route) = item.route {
                        if let Some(steps) = item.agent_steps {
                            span { " · route {route.as_str()} steps {steps}" }
                        } else {
                            span { " · route {route.as_str()}" }
                        }
                    }
                }
            }
            match &item.state {
                FeedState::Translating | FeedState::Validating | FeedState::Running => rsx! {
                    div { class: "result",
                        div { class: "skeleton", style: "width: 60%" }
                        div { class: "skeleton", style: "width: 80%; margin-top: 8px" }
                        div { class: "skeleton", style: "width: 50%; margin-top: 8px" }
                    }
                },
                FeedState::Error(err) => rsx! { div { class: "error", "Error: {err}" } },
                FeedState::Ready => rsx! {
                    {render_analysis_block(&item)}
                    {render_result_block(&item, &context, item.id)}
                },
            }
        }
    }
}

fn render_analysis_block(item: &FeedItem) -> Element {
    if !item.analysis_pending && item.analysis.is_none() && item.analysis_error.is_none() {
        return rsx! {};
    }

    rsx! {
        div { class: "analysis",
            div { class: "analysis-title", "SRE Answer" }
            if item.analysis_pending {
                div { class: "meta", "Analyzing results..." }
                div { class: "skeleton", style: "width: 70%" }
                div { class: "skeleton", style: "width: 55%" }
            }
            if let Some(error) = item.analysis_error.as_ref() {
                div { class: "error", "Analysis error: {error}" }
            }
            if let Some(analysis) = item.analysis.as_ref() {
                div { class: "analysis-title", "{analysis.title}" }
                div { class: "question", "{analysis.summary}" }
                if !analysis.bullets.is_empty() {
                    for bullet in analysis.bullets.iter() {
                        div { class: "question", "• {bullet}" }
                    }
                }
                if !analysis.rows.is_empty() {
                    div { class: "analysis-title", "Highlights" }
                    {render_analysis_rows(&analysis.rows)}
                }
                if !analysis.follow_ups.is_empty() {
                    div { class: "analysis-title", "Follow-ups" }
                    for follow in analysis.follow_ups.iter() {
                        div { class: "question", "• {follow}" }
                    }
                }
                if item.analysis_duration_ms.is_some() || analysis.usage.is_some() || !analysis.confidence.is_empty() {
                    div { class: "meta",
                        if let Some(ms) = item.analysis_duration_ms { span { "analysis {format_duration(ms)}" } }
                        if !analysis.confidence.is_empty() { span { " · confidence {analysis.confidence}" } }
                        if let Some(usage) = analysis.usage.as_ref() {
                            span { " · tokens {usage.prompt_tokens}/{usage.completion_tokens}/{usage.total_tokens}" }
                            if let Some(cached) = usage.cached_tokens { span { " · cached {cached}" } }
                            if let Some(reasoning) = usage.reasoning_tokens { span { " · reasoning {reasoning}" } }
                        }
                    }
                }
            }
        }
    }
}

fn render_analysis_rows(rows: &[Value]) -> Element {
    let objects: Vec<&Map<String, Value>> = rows.iter().filter_map(|row| row.as_object()).collect();
    if objects.is_empty() {
        return rsx! { div { class: "meta", "No structured rows to display." } };
    }

    let mut columns: Vec<String> = objects[0].keys().cloned().collect();
    columns.sort();
    let max_rows = 10usize;
    let row_count = objects.len().min(max_rows);

    let header_nodes: Vec<Element> = columns
        .iter()
        .map(|label| rsx! { th { "{label}" } })
        .collect();

    let row_nodes: Vec<Element> = objects
        .iter()
        .take(row_count)
        .map(|row| {
            let values: Vec<String> = columns
                .iter()
                .map(|key| {
                    row.get(key)
                        .map(format_value)
                        .unwrap_or_else(|| "-".to_string())
                })
                .collect();
            rsx! {
                tr {
                    for value in values.iter() {
                        td { "{value}" }
                    }
                }
            }
        })
        .collect();

    rsx! {
        div { class: "result table-wrap",
            table { class: "result-table",
                thead { tr { {header_nodes.into_iter()} } }
                tbody { {row_nodes.into_iter()} }
            }
            if objects.len() > row_count {
                div { class: "meta", "Showing {row_count} of {objects.len()} rows." }
            }
        }
    }
}

fn render_result_block(item: &FeedItem, context: &AppContext, item_id: u64) -> Element {
    match &item.result {
        ResultPayload::Empty => rsx! { div { class: "meta", "No results returned." } },
        ResultPayload::Metric { label, value, unit } => {
            let mut text = value.clone();
            if let Some(unit) = unit {
                text = format!("{text} {unit}");
            }
            rsx! {
                div { class: "metric-card",
                    div { class: "metric-value", "{text}" }
                    div { class: "metric-label", "{label}" }
                }
            }
        }
        ResultPayload::List { rows } => render_table_block(rows, context, item_id),
        ResultPayload::Graph { nodes, edges } => {
            let edge_rows: Vec<Element> = edges
                .iter()
                .map(|edge| {
                    let label = edge.label.as_deref().unwrap_or("link");
                    let text = format!("{} -> {} ({})", edge.from, edge.to, label);
                    rsx! { div { class: "question", "{text}" } }
                })
                .collect();
            rsx! {
                div { class: "result graph-block",
                    div { class: "graph-list",
                        div { class: "meta", "Nodes" }
                        for (idx, node) in nodes.iter().enumerate() {
                            div { class: "question", "{idx}: {node.label}" }
                        }
                    }
                    div { class: "graph-list",
                        div { class: "meta", "Edges" }
                        {edge_rows.into_iter()}
                    }
                }
            }
        }
        ResultPayload::Raw { text } => rsx! { div { class: "result", pre { "{text}" } } },
    }
}

fn render_table_block(rows: &[RowCard], context: &AppContext, item_id: u64) -> Element {
    let spec = table_spec(rows);
    let extra_keys = spec.extra_keys.clone();
    let context = context.clone();
    let sort_state = {
        let shared = context.shared.lock().expect("shared state lock poisoned");
        shared
            .table_sort
            .as_ref()
            .filter(|sort| sort.item_id == item_id)
            .cloned()
    };
    let sorted_rows = sort_rows(rows, sort_state.as_ref());

    let header_defs = build_header_defs(&spec, &extra_keys, sort_state.as_ref());
    let header_nodes: Vec<Element> = header_defs
        .iter()
        .map(|header| {
            let context = context.clone();
            let column = header.column.clone();
            let label = header.label.clone();
            let on_click = move |_| toggle_table_sort(&context, item_id, column.clone());
            rsx! { th { onclick: on_click, "{label}" } }
        })
        .collect();

    let row_nodes: Vec<Element> = sorted_rows
        .iter()
        .map(|row| {
            let row_clone = row.clone();
            let row_fields = row.fields.clone();
            let extra_values: Vec<String> = extra_keys
                .iter()
                .map(|key| find_field(&row_fields, key).unwrap_or("-").to_string())
                .collect();
            let row_for_click = row_clone.clone();
            let on_click = {
                let context = context.clone();
                move |_| open_inspector_from_row(&context, &row_for_click)
            };
            let title = row_clone.title.clone();
            let namespace = row_clone
                .subtitle
                .clone()
                .unwrap_or_else(|| "-".to_string());
            let status = row_clone.status.clone().unwrap_or_else(|| "-".to_string());
            rsx! {
                tr { onclick: on_click,
                    if spec.show_title { td { "{title}" } }
                    if spec.show_namespace { td { "{namespace}" } }
                    if spec.show_status { td { "{status}" } }
                    for value in extra_values.iter() {
                        td { "{value}" }
                    }
                }
            }
        })
        .collect();

    rsx! {
        div { class: "result table-wrap",
            table { class: "result-table",
                thead {
                    tr {
                        {header_nodes.into_iter()}
                    }
                }
                tbody {
                    {row_nodes.into_iter()}
                }
            }
        }
    }
}

pub(super) fn render_inspector_panel(inspector: &InspectorState, context: &AppContext) -> Element {
    if !inspector.is_open {
        return rsx! {};
    }

    let context = context.clone();
    let close = move |_| {
        update_shared(&context, |shared| shared.inspector.is_open = false);
    };

    rsx! {
        div { class: "inspector",
            div { class: "inspector-header",
                div { class: "inspector-title", "Node Inspector" }
                button { class: "button secondary", onclick: close, "X" }
            }
            if let Some(node_id) = inspector.node_id.as_ref() {
                div { class: "question", "{node_id}" }
            }
            if let Some(node_type) = inspector.node_type.as_ref() {
                div { class: "meta", "{node_type}" }
            }
            div { class: "inspector-section", "Properties" }
            if inspector.properties.is_empty() {
                div { class: "meta", "No properties loaded" }
            }
            for property in inspector.properties.iter() {
                match &property.value {
                    InspectorValue::Text(value) => rsx! {
                        div { class: "question", "{property.key}: {value}" }
                    },
                    InspectorValue::Json(value) => rsx! {
                        div { class: "meta", "{property.key}" }
                        div { class: "inspector-block", "{value}" }
                    },
                }
            }
            div { class: "inspector-section", "Relationships" }
            if inspector.relationships.is_empty() {
                div { class: "meta", "No relationships loaded" }
            }
            for (label, target) in inspector.relationships.iter() {
                div { class: "question", "→ {label} ({target})" }
            }
        }
    }
}
