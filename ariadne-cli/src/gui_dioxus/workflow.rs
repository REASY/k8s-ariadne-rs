//! Dioxus adapter for asynchronous query and context-compaction workflows.
//!
//! Shared policy is delegated to framework-neutral modules; this file owns
//! signal notification and mutation of Dioxus `SharedState`.

use super::{
    AppContext, COMPACT_CONTEXT_LIMIT, FeedItem, InspectorProperty, Instant, RowCard,
    build_context, build_context_with_budget, inspector_value, notify, read_shared,
    update_feed_item, update_shared,
};
use crate::gui_workflow::{WorkflowServices, rerun_query, run_question};

pub(super) fn submit_question(context: &AppContext, question: String) {
    let id = {
        let mut shared = context.shared.lock().expect("shared state lock poisoned");
        let id = shared.next_id;
        shared.next_id += 1;
        shared.feed.push(FeedItem::new(id, question.clone()));
        id
    };
    notify(context);

    let services = WorkflowServices::new(
        context.backend.clone(),
        context.translator.clone(),
        context.router.clone(),
        context.agentic.clone(),
        context.analyst.clone(),
    );
    let analysis_context = build_context_with_budget(context, &read_shared(context));
    let analysis_summary = read_shared(context).context_compact_summary;
    let context = context.clone();
    let runtime = context.runtime.clone();
    runtime.spawn(async move {
        run_question(
            services,
            question,
            analysis_context,
            analysis_summary,
            |patch| {
                update_feed_item(&context, id, |item| patch.apply(item));
                notify(&context);
            },
        )
        .await;
    });
}

pub(super) fn rerun_cypher(context: &AppContext, id: u64, cypher: String) {
    let item = {
        let shared = context.shared.lock().expect("shared state lock poisoned");
        shared.feed.iter().find(|item| item.id == id).cloned()
    };
    let Some(item) = item else {
        return;
    };
    let services = WorkflowServices::new(
        context.backend.clone(),
        context.translator.clone(),
        context.router.clone(),
        context.agentic.clone(),
        context.analyst.clone(),
    );
    let analysis_context = build_context_with_budget(context, &read_shared(context));
    let analysis_summary = read_shared(context).context_compact_summary;
    let context = context.clone();
    let runtime = context.runtime.clone();
    runtime.spawn(async move {
        rerun_query(
            services,
            item.user_text,
            cypher,
            item.params,
            analysis_context,
            analysis_summary,
            |patch| {
                update_feed_item(&context, id, |feed_item| patch.apply(feed_item));
                notify(&context);
            },
        )
        .await;
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
