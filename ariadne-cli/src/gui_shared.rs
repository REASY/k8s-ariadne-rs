use std::collections::HashMap;

use serde_json::Value;

use crate::agent::{AnalysisResult, ConversationTurn, LlmUsage, RouteDecision};

#[derive(Debug, Clone)]
pub(crate) enum FeedState {
    Translating,
    Validating,
    Running,
    Ready,
    Error(String),
}

#[derive(Debug, Clone)]
pub(crate) enum ResultPayload {
    Empty,
    Metric {
        label: String,
        value: String,
        unit: Option<String>,
    },
    List {
        rows: Vec<RowCard>,
    },
    Graph {
        nodes: Vec<GraphNode>,
        edges: Vec<GraphEdge>,
    },
    Raw {
        text: String,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct RowCard {
    pub(crate) title: String,
    pub(crate) subtitle: Option<String>,
    pub(crate) status: Option<String>,
    pub(crate) fields: Vec<(String, String)>,
    pub(crate) raw_fields: Vec<(String, Value)>,
}

#[derive(Debug, Clone)]
pub(crate) struct GraphNode {
    pub(crate) label: String,
}

#[derive(Debug, Clone)]
pub(crate) struct GraphEdge {
    pub(crate) from: usize,
    pub(crate) to: usize,
    pub(crate) label: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct FeedItem {
    pub(crate) id: u64,
    pub(crate) user_text: String,
    pub(crate) cypher: Option<String>,
    pub(crate) params: Option<HashMap<String, Value>>,
    pub(crate) result: ResultPayload,
    pub(crate) state: FeedState,
    pub(crate) llm_usage: Option<LlmUsage>,
    pub(crate) llm_duration_ms: Option<u128>,
    pub(crate) exec_duration_ms: Option<u128>,
    pub(crate) analysis: Option<AnalysisResult>,
    pub(crate) analysis_duration_ms: Option<u128>,
    pub(crate) analysis_error: Option<String>,
    pub(crate) analysis_pending: bool,
    pub(crate) context_summary: Option<String>,
    pub(crate) context_bindings: Option<HashMap<String, Value>>,
    pub(crate) route: Option<RouteDecision>,
    pub(crate) agent_steps: Option<usize>,
}

impl FeedItem {
    pub(crate) fn new(id: u64, user_text: String) -> Self {
        Self {
            id,
            user_text,
            cypher: None,
            params: None,
            result: ResultPayload::Empty,
            state: FeedState::Translating,
            llm_usage: None,
            llm_duration_ms: None,
            exec_duration_ms: None,
            analysis: None,
            analysis_duration_ms: None,
            analysis_error: None,
            analysis_pending: false,
            context_summary: None,
            context_bindings: None,
            route: None,
            agent_steps: None,
        }
    }
}

#[derive(Default, Clone)]
pub(crate) struct InspectorState {
    pub(crate) is_open: bool,
    pub(crate) node_type: Option<String>,
    pub(crate) node_id: Option<String>,
    pub(crate) properties: Vec<InspectorProperty>,
    pub(crate) relationships: Vec<(String, String)>,
}

#[derive(Clone, Debug)]
pub(crate) struct InspectorProperty {
    pub(crate) key: String,
    pub(crate) value: InspectorValue,
}

#[derive(Clone, Debug)]
pub(crate) enum InspectorValue {
    Text(String),
    Json(String),
}

pub(crate) fn format_duration(milliseconds: u128) -> String {
    if milliseconds >= 1_000 {
        format!("{:.2}s", milliseconds as f64 / 1_000.0)
    } else {
        format!("{milliseconds} ms")
    }
}

#[derive(Default)]
pub(crate) struct UsageAccumulator {
    prompt_tokens: u32,
    completion_tokens: u32,
    total_tokens: u32,
    reasoning_tokens: Option<u32>,
    cached_tokens: Option<u32>,
    seen: bool,
    reasoning_complete: bool,
    cached_complete: bool,
}

impl UsageAccumulator {
    pub(crate) fn add(&mut self, usage: Option<&LlmUsage>) {
        let Some(usage) = usage else {
            return;
        };
        if !self.seen {
            self.seen = true;
            self.reasoning_complete = true;
            self.cached_complete = true;
            self.reasoning_tokens = Some(0);
            self.cached_tokens = Some(0);
        }
        self.prompt_tokens = self.prompt_tokens.saturating_add(usage.prompt_tokens);
        self.completion_tokens = self
            .completion_tokens
            .saturating_add(usage.completion_tokens);
        self.total_tokens = self.total_tokens.saturating_add(usage.total_tokens);
        if self.reasoning_complete {
            if let Some(tokens) = usage.reasoning_tokens {
                self.reasoning_tokens = Some(self.reasoning_tokens.unwrap_or(0) + tokens);
            } else {
                self.reasoning_tokens = None;
                self.reasoning_complete = false;
            }
        }
        if self.cached_complete {
            if let Some(tokens) = usage.cached_tokens {
                self.cached_tokens = Some(self.cached_tokens.unwrap_or(0) + tokens);
            } else {
                self.cached_tokens = None;
                self.cached_complete = false;
            }
        }
    }

    pub(crate) fn build(&self) -> Option<LlmUsage> {
        self.seen.then_some(LlmUsage {
            prompt_tokens: self.prompt_tokens,
            completion_tokens: self.completion_tokens,
            total_tokens: self.total_tokens,
            reasoning_tokens: self.reasoning_tokens,
            cached_tokens: self.cached_tokens,
        })
    }
}

pub(crate) fn log_llm_call(label: &str, duration_ms: u128, usage: Option<&LlmUsage>) {
    if let Some(usage) = usage {
        tracing::info!(
            "{label} LLM ({duration_ms} ms) tokens prompt={} completion={} total={} cached={:?} reasoning={:?}",
            usage.prompt_tokens,
            usage.completion_tokens,
            usage.total_tokens,
            usage.cached_tokens,
            usage.reasoning_tokens
        );
    } else {
        tracing::info!("{label} LLM ({duration_ms} ms)");
    }
}

pub(crate) fn estimate_text_tokens(text: &str) -> usize {
    if text.is_empty() {
        0
    } else {
        (text.len() / 4).max(1)
    }
}

pub(crate) fn estimate_turn_tokens(turn: &ConversationTurn) -> usize {
    let mut tokens = estimate_text_tokens(&turn.question) + estimate_text_tokens(&turn.cypher);
    if let Some(summary) = &turn.result_summary {
        tokens += estimate_text_tokens(summary);
    }
    if let Some(bindings) = &turn.bindings {
        tokens += estimate_text_tokens(&serde_json::to_string(bindings).unwrap_or_default());
    }
    tokens
}

pub(crate) fn estimate_context_tokens(turns: &[ConversationTurn], summary: Option<&str>) -> usize {
    turns.iter().map(estimate_turn_tokens).sum::<usize>()
        + summary.map(estimate_text_tokens).unwrap_or(0)
}
