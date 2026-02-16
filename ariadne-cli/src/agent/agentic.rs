use ::llm::builder::LLMBuilder;
use ::llm::chat::{ChatMessage, StructuredOutputFormat};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::env;

use ariadne_core::graph_backend::GraphBackend;

use crate::agent::prompts::agentic_prompt;
use crate::agent::types::{
    AgentAction, AgentPlan, AgentStep, ConversationTurn, LlmConfig, LlmUsage,
};
use crate::agent::util::{clean_json_response, extract_cypher, map_llm_error};
use crate::error::CliResult;
use crate::validation::validate_cypher;

const DEFAULT_MAX_STEPS: usize = 3;
const DEFAULT_MAX_RETRIES: usize = 1;

#[async_trait]
pub trait Agentic: Send + Sync {
    async fn plan(
        &self,
        question: &str,
        context: &[ConversationTurn],
        context_summary: Option<&str>,
        backend: &dyn GraphBackend,
    ) -> CliResult<AgentPlan>;
}

pub struct LlmAgentic {
    llm: Box<dyn ::llm::LLMProvider>,
    structured_output: bool,
    max_steps: usize,
    max_retries: usize,
}

impl LlmAgentic {
    pub fn try_new(config: LlmConfig) -> CliResult<Self> {
        if config.base_url.trim().is_empty() {
            return Err("LLM base URL is empty".into());
        }
        if config.model.trim().is_empty() {
            return Err("LLM model is empty".into());
        }

        let mut builder = LLMBuilder::new()
            .backend(config.backend.clone())
            .model(config.model.clone())
            .timeout_seconds(config.timeout_secs)
            .normalize_response(true)
            .system(agentic_prompt(config.structured_output));

        if config.structured_output {
            builder = builder.schema(agent_step_schema());
        }

        if !config.base_url.trim().is_empty() {
            builder = builder.base_url(config.base_url.clone());
        }
        if let Some(api_key) = &config.api_key {
            builder = builder.api_key(api_key.clone());
        }

        let llm = builder.build()?;
        Ok(Self {
            llm,
            structured_output: config.structured_output,
            max_steps: read_max_steps(),
            max_retries: read_max_retries(),
        })
    }
}

#[async_trait]
impl Agentic for LlmAgentic {
    async fn plan(
        &self,
        question: &str,
        context: &[ConversationTurn],
        context_summary: Option<&str>,
        backend: &dyn GraphBackend,
    ) -> CliResult<AgentPlan> {
        let mut steps: Vec<AgentStep> = Vec::new();
        let mut usage = UsageAccumulator::default();

        for step_index in 0..self.max_steps {
            let mut attempt = 0usize;
            let mut feedback: Option<String> = None;

            loop {
                attempt += 1;
                let messages = build_agent_messages(
                    question,
                    context,
                    context_summary,
                    &steps,
                    feedback.as_deref(),
                );
                let response = match self.llm.chat(&messages).await {
                    Ok(response) => response,
                    Err(err) => return Err(map_llm_error(err, self.structured_output)),
                };
                if let Some(usage_details) = response.usage() {
                    usage.add(&LlmUsage::from(usage_details));
                }
                let text = response
                    .text()
                    .ok_or_else(|| "LLM response missing text".to_string())?;

                let mut step = if self.structured_output {
                    parse_structured_step(&text)?
                } else {
                    parse_unstructured_step(&text)
                };
                step.usage = response.usage().map(LlmUsage::from);

                if step.cypher.trim().is_empty() {
                    if attempt <= self.max_retries {
                        feedback =
                            Some("Cypher was empty. Provide a valid Cypher query.".to_string());
                        continue;
                    }
                    return Err("Agent returned empty Cypher".into());
                }

                if let Err(issue) = validate_cypher(&step.cypher) {
                    if attempt <= self.max_retries && issue.retriable() {
                        feedback = Some(issue.feedback());
                        continue;
                    }
                    return Err(issue.into());
                }

                let merged_params = merge_params(step.params.clone(), context);
                step.params = merged_params.clone();

                if step.action == AgentAction::Final {
                    return Ok(AgentPlan {
                        cypher: step.cypher,
                        params: step.params,
                        steps,
                        usage: usage.build(),
                    });
                }

                if step_index + 1 >= self.max_steps {
                    tracing::warn!("Agentic max steps reached; using last query as final response");
                    return Ok(AgentPlan {
                        cypher: step.cypher,
                        params: step.params,
                        steps,
                        usage: usage.build(),
                    });
                }

                match backend
                    .execute_query(step.cypher.clone(), step.params.clone())
                    .await
                {
                    Ok(records) => {
                        step.result_summary = Some(summarize_records_for_agent(&records));
                        steps.push(step);
                        break;
                    }
                    Err(err) => {
                        return Err(err.into());
                    }
                }
            }
        }

        Err("Agentic loop did not return a final query".into())
    }
}

#[derive(Debug, Deserialize)]
struct AgentStepPayload {
    action: String,
    cypher: String,
    #[serde(default)]
    params: Option<HashMap<String, Value>>,
}

fn parse_structured_step(text: &str) -> CliResult<AgentStep> {
    let cleaned = clean_json_response(text);
    let payload: AgentStepPayload =
        serde_json::from_str(&cleaned).map_err(|e| format!("Invalid JSON response: {e}"))?;
    Ok(AgentStep {
        action: parse_action(&payload.action),
        cypher: extract_cypher(&payload.cypher),
        params: payload.params,
        result_summary: None,
        usage: None,
    })
}

fn parse_unstructured_step(text: &str) -> AgentStep {
    let mut action = AgentAction::Final;
    let mut cypher_line: Option<String> = None;
    for line in text.lines() {
        let trimmed = line.trim();
        let lower = trimmed.to_lowercase();
        if lower.starts_with("action:") {
            if lower.contains("query") {
                action = AgentAction::Query;
            } else if lower.contains("final") {
                action = AgentAction::Final;
            }
        }
        if lower.starts_with("cypher:") {
            if let Some((_, value)) = trimmed.split_once(':') {
                let value = value.trim();
                if !value.is_empty() {
                    cypher_line = Some(value.to_string());
                }
            }
        }
    }
    let cypher = cypher_line.unwrap_or_else(|| extract_cypher(text));
    AgentStep {
        action,
        cypher,
        params: None,
        result_summary: None,
        usage: None,
    }
}

fn parse_action(raw: &str) -> AgentAction {
    match raw.trim().to_lowercase().as_str() {
        "query" => AgentAction::Query,
        _ => AgentAction::Final,
    }
}

fn build_agent_messages(
    question: &str,
    context: &[ConversationTurn],
    context_summary: Option<&str>,
    steps: &[AgentStep],
    feedback: Option<&str>,
) -> Vec<ChatMessage> {
    let mut messages = Vec::new();
    if let Some(summary) = context_summary {
        let summary = summary.trim();
        if !summary.is_empty() {
            messages.push(
                ChatMessage::assistant()
                    .content(format!("Context summary:\n{summary}"))
                    .build(),
            );
        }
    }
    for turn in context {
        if turn.question.trim().is_empty() || turn.cypher.trim().is_empty() {
            continue;
        }
        messages.push(ChatMessage::user().content(turn.question.trim()).build());
        let mut assistant = format!("Cypher:\n{}", turn.cypher.trim());
        if let Some(summary) = &turn.result_summary {
            if !summary.trim().is_empty() {
                assistant.push_str("\nResult summary:\n");
                assistant.push_str(summary.trim());
            }
        }
        if let Some(bindings) = &turn.bindings {
            let formatted = format_bindings(bindings);
            if !formatted.is_empty() {
                assistant.push_str("\nBindings:\n");
                assistant.push_str(&formatted);
            }
        }
        messages.push(ChatMessage::assistant().content(assistant).build());
    }

    if !steps.is_empty() {
        let mut history = String::new();
        for (index, step) in steps.iter().enumerate() {
            history.push_str(&format!(
                "Step {}: action={}\nCypher:\n{}\n",
                index + 1,
                step.action.as_str(),
                step.cypher.trim()
            ));
            if let Some(summary) = &step.result_summary {
                if !summary.trim().is_empty() {
                    history.push_str("Result summary: ");
                    history.push_str(summary.trim());
                    history.push('\n');
                }
            }
            if let Some(params) = &step.params {
                let formatted = format_bindings(params);
                if !formatted.is_empty() {
                    history.push_str("Params:\n");
                    history.push_str(&formatted);
                    history.push('\n');
                }
            }
        }
        messages.push(
            ChatMessage::assistant()
                .content(format!("Agent history:\n{history}"))
                .build(),
        );
    }

    if let Some(feedback) = feedback {
        let feedback = feedback.trim();
        if !feedback.is_empty() {
            messages.push(
                ChatMessage::user()
                    .content(format!(
                        "Previous Cypher failed validation: {feedback}\n\
Please correct it and return a valid JSON action."
                    ))
                    .build(),
            );
        }
    }

    messages.push(ChatMessage::user().content(question.trim()).build());
    messages
}

fn format_bindings(bindings: &HashMap<String, Value>) -> String {
    let mut entries: Vec<String> = bindings
        .iter()
        .map(|(key, value)| {
            let value = serde_json::to_string(value).unwrap_or_else(|_| "null".to_string());
            format!("{key} = {value}")
        })
        .collect();
    entries.sort();
    entries.join("\n")
}

fn merge_params(
    params: Option<HashMap<String, Value>>,
    context: &[ConversationTurn],
) -> Option<HashMap<String, Value>> {
    let mut merged = params.unwrap_or_default();
    if let Some(turn) = context.last() {
        if let Some(bindings) = &turn.bindings {
            for (key, value) in bindings {
                merged.entry(key.clone()).or_insert_with(|| value.clone());
            }
        }
    }
    if merged.is_empty() {
        None
    } else {
        Some(merged)
    }
}

fn summarize_records_for_agent(records: &[Value]) -> String {
    if records.is_empty() {
        return "rows=0".to_string();
    }

    let rows = records.len();
    let mut columns: Vec<String> = records
        .first()
        .and_then(|v| v.as_object())
        .map(|obj| obj.keys().cloned().collect())
        .unwrap_or_default();
    columns.sort();

    let mut summary = format!("rows={rows}");
    if !columns.is_empty() {
        summary.push_str(", columns=");
        summary.push_str(&columns.join(","));
    }

    let samples: Vec<String> = records
        .iter()
        .take(2)
        .map(|value| summarize_record(value, &columns))
        .collect();
    if !samples.is_empty() {
        summary.push_str("; sample=");
        summary.push_str(&samples.join(" | "));
    }

    truncate_text(&summary, 400)
}

fn summarize_record(value: &Value, columns: &[String]) -> String {
    let Some(obj) = value.as_object() else {
        return truncate_text(&value.to_string(), 120);
    };
    let mut entries = Vec::new();
    for key in columns.iter().take(4) {
        if let Some(val) = obj.get(key) {
            let rendered = if val.is_string() {
                val.as_str().unwrap_or("").to_string()
            } else {
                truncate_text(&val.to_string(), 60)
            };
            entries.push(format!("{key}={rendered}"));
        }
    }
    let rendered = entries.join(", ");
    truncate_text(&rendered, 200)
}

fn truncate_text(text: &str, max_len: usize) -> String {
    if text.len() <= max_len {
        return text.to_string();
    }
    let mut clipped = text[..max_len].to_string();
    clipped.push_str("...");
    clipped
}

fn read_max_steps() -> usize {
    env::var("ARIADNE_AGENT_MAX_STEPS")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_MAX_STEPS)
}

fn read_max_retries() -> usize {
    env::var("ARIADNE_AGENT_MAX_RETRIES")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_MAX_RETRIES)
}

fn agent_step_schema() -> StructuredOutputFormat {
    const SCHEMA: &str = r#"
    {
        "name": "AgentStep",
        "description": "Agentic step for multi-turn graph query planning",
        "strict": true,
        "schema": {
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "action": { "type": "string", "enum": ["query", "final"] },
                "cypher": { "type": "string" },
                "params": { "type": "object", "additionalProperties": true }
            },
            "required": ["action", "cypher"]
        }
    }
    "#;
    serde_json::from_str(SCHEMA).expect("invalid AgentStep schema JSON")
}

struct UsageAccumulator {
    prompt_tokens: u32,
    completion_tokens: u32,
    total_tokens: u32,
    reasoning_tokens: Option<u32>,
    cached_tokens: Option<u32>,
    seen: bool,
    reasoning_complete: bool,
    cached_complete: bool,
}

impl Default for UsageAccumulator {
    fn default() -> Self {
        Self {
            prompt_tokens: 0,
            completion_tokens: 0,
            total_tokens: 0,
            reasoning_tokens: Some(0),
            cached_tokens: Some(0),
            seen: false,
            reasoning_complete: true,
            cached_complete: true,
        }
    }
}

impl UsageAccumulator {
    fn add(&mut self, usage: &LlmUsage) {
        self.prompt_tokens = self.prompt_tokens.saturating_add(usage.prompt_tokens);
        self.completion_tokens = self
            .completion_tokens
            .saturating_add(usage.completion_tokens);
        self.total_tokens = self.total_tokens.saturating_add(usage.total_tokens);
        self.seen = true;

        if !self.reasoning_complete {
            // Already missing in a prior call; keep None.
        } else if let Some(tokens) = usage.reasoning_tokens {
            self.reasoning_tokens = Some(self.reasoning_tokens.unwrap_or(0) + tokens);
        } else {
            self.reasoning_tokens = None;
            self.reasoning_complete = false;
        }

        if !self.cached_complete {
            // Already missing in a prior call; keep None.
        } else if let Some(tokens) = usage.cached_tokens {
            self.cached_tokens = Some(self.cached_tokens.unwrap_or(0) + tokens);
        } else {
            self.cached_tokens = None;
            self.cached_complete = false;
        }
    }

    fn build(&self) -> Option<LlmUsage> {
        if !self.seen {
            return None;
        }
        Some(LlmUsage {
            prompt_tokens: self.prompt_tokens,
            completion_tokens: self.completion_tokens,
            total_tokens: self.total_tokens,
            reasoning_tokens: self.reasoning_tokens,
            cached_tokens: self.cached_tokens,
        })
    }
}
