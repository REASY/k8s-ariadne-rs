//! Framework-neutral query, validation, execution, and analysis state machine.
//!
//! Frontends provide an event sink and decide how to repaint or notify. This
//! module is the single owner of retry, routing, and feed-transition semantics.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use ariadne_core::cypher_validation::validate_cypher;
use ariadne_core::graph_backend::GraphBackend;
use ariadne_core::query_issue::{QueryIssue, classify_ariadne_error};
use serde_json::Value;

use crate::agent::{
    Agentic, AnalysisResult, Analyst, ConversationTurn, LlmUsage, RouteDecision, Router, Translator,
};
use crate::gui_results::{
    classify_result, extract_context_bindings, merge_params, summarize_records,
};
use crate::gui_shared::{FeedItem, FeedState, ResultPayload, UsageAccumulator, log_llm_call};

const LLM_MAX_RETRIES: usize = 1;

#[derive(Clone)]
pub(crate) struct WorkflowServices {
    backend: Arc<dyn GraphBackend>,
    translator: Arc<dyn Translator>,
    router: Arc<dyn Router>,
    agentic: Arc<dyn Agentic>,
    analyst: Arc<dyn Analyst>,
}

impl WorkflowServices {
    pub(crate) fn new(
        backend: Arc<dyn GraphBackend>,
        translator: Arc<dyn Translator>,
        router: Arc<dyn Router>,
        agentic: Arc<dyn Agentic>,
        analyst: Arc<dyn Analyst>,
    ) -> Self {
        Self {
            backend,
            translator,
            router,
            agentic,
            analyst,
        }
    }
}

pub(crate) enum FeedPatch {
    Preparing,
    Route {
        decision: RouteDecision,
        steps: Option<usize>,
    },
    Translated {
        cypher: String,
        params: Option<HashMap<String, Value>>,
        usage: Option<LlmUsage>,
        duration_ms: u128,
    },
    Running {
        cypher: Option<String>,
        params: Option<HashMap<String, Value>>,
    },
    Ready {
        result: ResultPayload,
        duration_ms: u128,
        summary: String,
        bindings: Option<HashMap<String, Value>>,
    },
    AnalysisPending,
    AnalysisCompleted {
        analysis: AnalysisResult,
        duration_ms: u128,
    },
    AnalysisFailed {
        error: String,
        duration_ms: u128,
    },
    Failed {
        error: String,
        llm_duration_ms: Option<u128>,
        exec_duration_ms: Option<u128>,
    },
}

impl FeedPatch {
    pub(crate) fn apply(self, item: &mut FeedItem) {
        match self {
            Self::Preparing => {
                item.state = FeedState::Translating;
                item.analysis = None;
                item.analysis_error = None;
                item.analysis_pending = false;
            }
            Self::Route { decision, steps } => {
                item.route = Some(decision);
                item.agent_steps = steps;
            }
            Self::Translated {
                cypher,
                params,
                usage,
                duration_ms,
            } => {
                item.cypher = Some(cypher);
                item.params = params;
                item.llm_usage = usage;
                item.llm_duration_ms = Some(duration_ms);
                item.state = FeedState::Validating;
            }
            Self::Running { cypher, params } => {
                if let Some(cypher) = cypher {
                    item.cypher = Some(cypher);
                }
                item.params = params;
                item.state = FeedState::Running;
                item.analysis = None;
                item.analysis_error = None;
                item.analysis_pending = false;
            }
            Self::Ready {
                result,
                duration_ms,
                summary,
                bindings,
            } => {
                item.state = FeedState::Ready;
                item.result = result;
                item.exec_duration_ms = Some(duration_ms);
                item.context_summary = Some(summary);
                item.context_bindings = bindings;
            }
            Self::AnalysisPending => item.analysis_pending = true,
            Self::AnalysisCompleted {
                analysis,
                duration_ms,
            } => {
                item.analysis = Some(analysis);
                item.analysis_duration_ms = Some(duration_ms);
                item.analysis_pending = false;
                item.analysis_error = None;
            }
            Self::AnalysisFailed { error, duration_ms } => {
                item.analysis_error = Some(error);
                item.analysis_duration_ms = Some(duration_ms);
                item.analysis_pending = false;
            }
            Self::Failed {
                error,
                llm_duration_ms,
                exec_duration_ms,
            } => {
                item.state = FeedState::Error(error);
                if let Some(duration_ms) = llm_duration_ms {
                    item.llm_duration_ms = Some(duration_ms);
                }
                if let Some(duration_ms) = exec_duration_ms {
                    item.exec_duration_ms = Some(duration_ms);
                }
            }
        }
    }
}

pub(crate) async fn run_question(
    services: WorkflowServices,
    question: String,
    context: Vec<ConversationTurn>,
    context_summary: Option<String>,
    emit: impl Fn(FeedPatch),
) {
    let mut usage = UsageAccumulator::default();
    emit(FeedPatch::Preparing);

    let route_start = Instant::now();
    let route = match services.router.classify(&question).await {
        Ok(result) => {
            let duration_ms = route_start.elapsed().as_millis();
            log_llm_call("router", duration_ms, result.usage.as_ref());
            usage.add(result.usage.as_ref());
            emit(FeedPatch::Route {
                decision: result.decision,
                steps: (result.decision == RouteDecision::OneShot).then_some(0),
            });
            result.decision
        }
        Err(error) => {
            tracing::warn!("Router failed, falling back to one-shot: {error}");
            emit(FeedPatch::Route {
                decision: RouteDecision::OneShot,
                steps: Some(0),
            });
            RouteDecision::OneShot
        }
    };

    if route == RouteDecision::MultiTurn {
        let plan_start = Instant::now();
        match services
            .agentic
            .plan(
                &question,
                &context,
                context_summary.as_deref(),
                services.backend.as_ref(),
            )
            .await
        {
            Ok(plan) => {
                let duration_ms = plan_start.elapsed().as_millis();
                log_llm_call("agentic", duration_ms, plan.usage.as_ref());
                usage.add(plan.usage.as_ref());
                let params = merge_params(plan.params, &context);
                emit(FeedPatch::Route {
                    decision: RouteDecision::MultiTurn,
                    steps: Some(plan.steps.len()),
                });
                emit(FeedPatch::Translated {
                    cypher: plan.cypher.clone(),
                    params: params.clone(),
                    usage: usage.build(),
                    duration_ms,
                });
                if let Err(issue) = validate_cypher(&plan.cypher) {
                    emit(failed(issue, None, None));
                    return;
                }
                if let Err(failure) = execute_and_analyze(
                    &services,
                    &question,
                    plan.cypher,
                    params,
                    &context,
                    context_summary.as_deref(),
                    false,
                    &emit,
                )
                .await
                {
                    emit(failed(failure.issue, None, Some(failure.duration_ms)));
                }
                return;
            }
            Err(error) => {
                tracing::warn!("Agentic planning failed, falling back to one-shot: {error}");
                emit(FeedPatch::Route {
                    decision: RouteDecision::OneShot,
                    steps: Some(0),
                });
            }
        }
    }

    let mut feedback = None;
    for attempt in 0..=LLM_MAX_RETRIES {
        emit(FeedPatch::Preparing);
        let translation_start = Instant::now();
        let translation = match services
            .translator
            .translate(
                &question,
                &context,
                context_summary.as_deref(),
                feedback.as_deref(),
            )
            .await
        {
            Ok(translation) => translation,
            Err(error) => {
                emit(FeedPatch::Failed {
                    error: error.to_string(),
                    llm_duration_ms: Some(translation_start.elapsed().as_millis()),
                    exec_duration_ms: None,
                });
                return;
            }
        };
        let duration_ms = translation_start.elapsed().as_millis();
        log_llm_call("translator", duration_ms, translation.usage.as_ref());
        usage.add(translation.usage.as_ref());
        let params = merge_params(translation.params, &context);
        emit(FeedPatch::Translated {
            cypher: translation.cypher.clone(),
            params: params.clone(),
            usage: usage.build(),
            duration_ms,
        });

        if let Err(issue) = validate_cypher(&translation.cypher) {
            if attempt < LLM_MAX_RETRIES && issue.repairable() {
                feedback = Some(issue.feedback());
                continue;
            }
            emit(failed(issue, None, None));
            return;
        }

        match execute_and_analyze(
            &services,
            &question,
            translation.cypher,
            params,
            &context,
            context_summary.as_deref(),
            false,
            &emit,
        )
        .await
        {
            Ok(()) => return,
            Err(failure) if attempt < LLM_MAX_RETRIES && failure.issue.repairable() => {
                feedback = Some(failure.issue.feedback());
            }
            Err(failure) => {
                emit(failed(failure.issue, None, Some(failure.duration_ms)));
                return;
            }
        }
    }
}

pub(crate) async fn rerun_query(
    services: WorkflowServices,
    question: String,
    cypher: String,
    params: Option<HashMap<String, Value>>,
    context: Vec<ConversationTurn>,
    context_summary: Option<String>,
    emit: impl Fn(FeedPatch),
) {
    if let Err(issue) = validate_cypher(&cypher) {
        emit(failed(issue, None, None));
        return;
    }
    if let Err(failure) = execute_and_analyze(
        &services,
        &question,
        cypher,
        params,
        &context,
        context_summary.as_deref(),
        true,
        &emit,
    )
    .await
    {
        emit(failed(failure.issue, None, Some(failure.duration_ms)));
    }
}

struct ExecutionFailure {
    issue: QueryIssue,
    duration_ms: u128,
}

#[allow(clippy::too_many_arguments)]
async fn execute_and_analyze(
    services: &WorkflowServices,
    question: &str,
    cypher: String,
    params: Option<HashMap<String, Value>>,
    context: &[ConversationTurn],
    context_summary: Option<&str>,
    update_cypher: bool,
    emit: &impl Fn(FeedPatch),
) -> Result<(), ExecutionFailure> {
    emit(FeedPatch::Running {
        cypher: update_cypher.then(|| cypher.clone()),
        params: params.clone(),
    });
    let execution_start = Instant::now();
    let records = services
        .backend
        .execute_query(cypher.clone(), params)
        .await
        .map_err(|error| ExecutionFailure {
            issue: classify_ariadne_error(&error),
            duration_ms: execution_start.elapsed().as_millis(),
        })?;
    let duration_ms = execution_start.elapsed().as_millis();
    let summary = summarize_records(&records);
    emit(FeedPatch::Ready {
        result: classify_result(&records),
        duration_ms,
        summary: summary.clone(),
        bindings: extract_context_bindings(&records),
    });
    emit(FeedPatch::AnalysisPending);

    let analysis_start = Instant::now();
    match services
        .analyst
        .analyze(
            question,
            &cypher,
            &records,
            &summary,
            context,
            context_summary,
        )
        .await
    {
        Ok(analysis) => {
            let duration_ms = analysis_start.elapsed().as_millis();
            log_llm_call("analysis", duration_ms, analysis.usage.as_ref());
            emit(FeedPatch::AnalysisCompleted {
                analysis,
                duration_ms,
            });
        }
        Err(error) => emit(FeedPatch::AnalysisFailed {
            error: error.to_string(),
            duration_ms: analysis_start.elapsed().as_millis(),
        }),
    }
    Ok(())
}

fn failed(
    issue: QueryIssue,
    llm_duration_ms: Option<u128>,
    exec_duration_ms: Option<u128>,
) -> FeedPatch {
    FeedPatch::Failed {
        error: issue.to_string(),
        llm_duration_ms,
        exec_duration_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feed_patches_apply_a_successful_query_lifecycle() {
        let mut item = FeedItem::new(1, "question".to_string());
        FeedPatch::Translated {
            cypher: "RETURN 1".to_string(),
            params: None,
            usage: None,
            duration_ms: 12,
        }
        .apply(&mut item);
        FeedPatch::Running {
            cypher: None,
            params: None,
        }
        .apply(&mut item);
        FeedPatch::Ready {
            result: ResultPayload::Metric {
                label: "count".to_string(),
                value: "1".to_string(),
                unit: None,
            },
            duration_ms: 3,
            summary: "one row".to_string(),
            bindings: None,
        }
        .apply(&mut item);

        assert!(matches!(item.state, FeedState::Ready));
        assert_eq!(item.cypher.as_deref(), Some("RETURN 1"));
        assert_eq!(item.context_summary.as_deref(), Some("one row"));
        assert_eq!(item.llm_duration_ms, Some(12));
        assert_eq!(item.exec_duration_ms, Some(3));
    }

    #[test]
    fn failure_patch_preserves_stage_durations() {
        let mut item = FeedItem::new(1, "question".to_string());
        FeedPatch::Failed {
            error: "backend unavailable".to_string(),
            llm_duration_ms: Some(8),
            exec_duration_ms: Some(5),
        }
        .apply(&mut item);

        assert!(matches!(
            item.state,
            FeedState::Error(ref error) if error == "backend unavailable"
        ));
        assert_eq!(item.llm_duration_ms, Some(8));
        assert_eq!(item.exec_duration_ms, Some(5));
    }
}
