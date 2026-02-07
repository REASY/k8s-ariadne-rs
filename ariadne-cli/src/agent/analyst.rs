use ::llm::builder::LLMBuilder;
use ::llm::chat::{ChatMessage, StructuredOutputFormat};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;

use crate::agent::prompts::{analysis_compaction_prompt, analysis_prompt};
use crate::agent::types::{
    AnalysisResult, ContextCompaction, ConversationTurn, LlmConfig, LlmUsage,
};
use crate::agent::util::{clean_json_response, map_llm_error};
use crate::error::CliResult;

#[async_trait]
pub trait Analyst: Send + Sync {
    async fn analyze(
        &self,
        question: &str,
        cypher: &str,
        records: &[Value],
        summary: &str,
        context: &[ConversationTurn],
        context_summary: Option<&str>,
    ) -> CliResult<AnalysisResult>;

    async fn compact_context(&self, context: &[ConversationTurn]) -> CliResult<ContextCompaction>;
}

pub struct SreAnalyst {
    llm: Box<dyn ::llm::LLMProvider>,
    structured_output: bool,
    config: LlmConfig,
}

impl SreAnalyst {
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
            .system(analysis_prompt(config.structured_output));

        if config.structured_output {
            builder = builder.schema(analysis_schema());
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
            config,
        })
    }
}

#[async_trait]
impl Analyst for SreAnalyst {
    async fn analyze(
        &self,
        question: &str,
        cypher: &str,
        records: &[Value],
        summary: &str,
        context: &[ConversationTurn],
        context_summary: Option<&str>,
    ) -> CliResult<AnalysisResult> {
        let messages =
            build_analysis_messages(question, cypher, records, summary, context, context_summary);
        let response = match self.llm.chat(&messages).await {
            Ok(response) => response,
            Err(err) => return Err(map_llm_error(err, self.structured_output)),
        };
        let usage = response.usage().map(LlmUsage::from);
        let text = response
            .text()
            .ok_or_else(|| "LLM response missing text".to_string())?;
        let mut result = if self.structured_output {
            parse_structured_analysis(&text)?
        } else {
            parse_unstructured_analysis(&text)
        };
        result.usage = usage;
        Ok(result)
    }

    async fn compact_context(&self, context: &[ConversationTurn]) -> CliResult<ContextCompaction> {
        let provider = build_compaction_provider(&self.config)?;
        let messages = build_compaction_messages(context);
        let response = match provider.chat(&messages).await {
            Ok(response) => response,
            Err(err) => return Err(map_llm_error(err, false)),
        };
        let usage = response.usage().map(LlmUsage::from);
        let text = response
            .text()
            .ok_or_else(|| "LLM response missing text".to_string())?;
        Ok(ContextCompaction {
            summary: text.trim().to_string(),
            usage,
        })
    }
}

#[derive(Debug, Deserialize)]
struct AnalysisPayload {
    answer: String,
    #[serde(default)]
    follow_ups: Vec<String>,
    confidence: Option<String>,
}

fn parse_structured_analysis(text: &str) -> CliResult<AnalysisResult> {
    let cleaned = clean_json_response(text);
    let payload: AnalysisPayload =
        serde_json::from_str(&cleaned).map_err(|e| format!("Invalid JSON response: {e}"))?;
    Ok(AnalysisResult {
        answer: payload.answer.trim().to_string(),
        follow_ups: payload.follow_ups,
        confidence: payload.confidence.map(|c| c.trim().to_string()),
        usage: None,
    })
}

fn parse_unstructured_analysis(text: &str) -> AnalysisResult {
    let mut answer_lines = Vec::new();
    let mut follow_ups = Vec::new();
    let mut confidence: Option<String> = None;
    let mut in_followups = false;

    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            if !in_followups {
                answer_lines.push(String::new());
            }
            continue;
        }
        let lower = trimmed.to_lowercase();
        if lower.starts_with("follow-up") || lower.starts_with("follow ups") {
            in_followups = true;
            continue;
        }
        if lower.starts_with("confidence") {
            if let Some((_, value)) = trimmed.split_once(':') {
                confidence = Some(value.trim().to_string());
            }
            continue;
        }
        if in_followups {
            let item = trimmed.trim_start_matches(['-', '•', '*', ' ']).trim();
            if !item.is_empty() {
                follow_ups.push(item.to_string());
            }
        } else {
            answer_lines.push(trimmed.to_string());
        }
    }

    let answer = if follow_ups.is_empty() && confidence.is_none() {
        text.trim().to_string()
    } else {
        answer_lines.join("\n").trim().to_string()
    };

    AnalysisResult {
        answer,
        follow_ups,
        confidence,
        usage: None,
    }
}

fn build_analysis_messages(
    question: &str,
    cypher: &str,
    records: &[Value],
    summary: &str,
    context: &[ConversationTurn],
    context_summary: Option<&str>,
) -> Vec<ChatMessage> {
    let records_json = serde_json::to_string(records).unwrap_or_else(|_| "[]".to_string());
    let mut body = String::new();
    if let Some(summary) = context_summary {
        let summary = summary.trim();
        if !summary.is_empty() {
            body.push_str("Context summary:\n");
            body.push_str(summary);
            body.push_str("\n\n");
        }
    }
    if !context.is_empty() {
        body.push_str("Recent context:\n");
        for (idx, turn) in context.iter().enumerate() {
            body.push_str(&format!("Turn {}:\n", idx + 1));
            body.push_str("User: ");
            body.push_str(turn.question.trim());
            body.push('\n');
            if let Some(summary) = &turn.result_summary {
                if !summary.trim().is_empty() {
                    body.push_str("Result summary: ");
                    body.push_str(summary.trim());
                    body.push('\n');
                }
            }
            body.push('\n');
        }
    }
    body.push_str("Question:\n");
    body.push_str(question.trim());
    body.push_str("\n\nCypher:\n");
    body.push_str(cypher.trim());
    body.push_str("\n\nResult summary:\n");
    body.push_str(summary.trim());
    body.push_str("\n\nRows (JSON):\n");
    body.push_str(&records_json);
    vec![ChatMessage::user().content(body).build()]
}

fn build_compaction_provider(config: &LlmConfig) -> CliResult<Box<dyn ::llm::LLMProvider>> {
    let mut builder = LLMBuilder::new()
        .backend(config.backend.clone())
        .model(config.model.clone())
        .timeout_seconds(config.timeout_secs)
        .normalize_response(true)
        .system(analysis_compaction_prompt());

    if !config.base_url.trim().is_empty() {
        builder = builder.base_url(config.base_url.clone());
    }
    if let Some(api_key) = &config.api_key {
        builder = builder.api_key(api_key.clone());
    }

    let llm = builder.build()?;
    Ok(llm)
}

fn build_compaction_messages(context: &[ConversationTurn]) -> Vec<ChatMessage> {
    let mut body = String::from(
        "Summarize the following conversation context for reuse in future SRE answers.\n",
    );
    for (idx, turn) in context.iter().enumerate() {
        body.push_str(&format!("\nTurn {}:\n", idx + 1));
        body.push_str("User: ");
        body.push_str(turn.question.trim());
        body.push('\n');
        if let Some(summary) = &turn.result_summary {
            if !summary.trim().is_empty() {
                body.push_str("Result summary: ");
                body.push_str(summary.trim());
                body.push('\n');
            }
        }
    }
    vec![ChatMessage::user().content(body).build()]
}

fn analysis_schema() -> StructuredOutputFormat {
    StructuredOutputFormat {
        name: "SreAnalysis".to_string(),
        description: Some("SRE analysis of Cypher results".to_string()),
        schema: Some(serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "answer": { "type": "string" },
                "follow_ups": { "type": "array", "items": { "type": "string" } },
                "confidence": { "type": "string", "enum": ["low", "medium", "high"] }
            },
            "required": ["answer", "follow_ups", "confidence"]
        })),
        strict: Some(true),
    }
}
