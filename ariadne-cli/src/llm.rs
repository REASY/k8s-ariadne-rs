use ::llm::builder::{LLMBackend, LLMBuilder};
use ::llm::chat::{ChatMessage, StructuredOutputFormat};
use ::llm::error::LLMError;
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::sync::OnceLock;
use tracing::{error, warn};

use crate::error::CliResult;

#[derive(Debug, Clone)]
pub struct LlmUsage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
    pub reasoning_tokens: Option<u32>,
    pub cached_tokens: Option<u32>,
}

impl From<::llm::chat::Usage> for LlmUsage {
    fn from(value: ::llm::chat::Usage) -> Self {
        let reasoning_tokens = value
            .completion_tokens_details
            .and_then(|details| details.reasoning_tokens);
        let cached_tokens = value
            .prompt_tokens_details
            .and_then(|details| details.cached_tokens);
        Self {
            prompt_tokens: value.prompt_tokens,
            completion_tokens: value.completion_tokens,
            total_tokens: value.total_tokens,
            reasoning_tokens,
            cached_tokens,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TranslationResult {
    pub cypher: String,
    pub usage: Option<LlmUsage>,
}

#[derive(Debug, Clone)]
pub struct ConversationTurn {
    pub question: String,
    pub cypher: String,
    pub result_summary: Option<String>,
}

#[derive(Debug, Clone)]
pub struct LlmConfig {
    pub backend: LLMBackend,
    pub base_url: String,
    pub model: String,
    pub api_key: Option<String>,
    pub timeout_secs: u64,
    pub structured_output: bool,
}

#[derive(Debug, Deserialize)]
struct ContextWindowConfig {
    #[serde(default)]
    models: HashMap<String, usize>,
    #[serde(default)]
    providers: HashMap<String, HashMap<String, usize>>,
}

pub fn context_window_tokens_for_model(model: &str) -> Option<usize> {
    if let Ok(raw) = env::var("LLM_CONTEXT_WINDOW_TOKENS") {
        match raw.trim().parse::<usize>() {
            Ok(tokens) if tokens > 0 => return Some(tokens),
            _ => warn!("LLM_CONTEXT_WINDOW_TOKENS is not a valid positive integer: {raw}"),
        }
    }

    let config = context_window_config()?;
    resolve_context_window_tokens(model, config)
}

fn context_window_config() -> Option<&'static ContextWindowConfig> {
    static CONFIG: OnceLock<Option<ContextWindowConfig>> = OnceLock::new();
    CONFIG.get_or_init(|| read_context_window_config()).as_ref()
}

fn read_context_window_config() -> Option<ContextWindowConfig> {
    let path = locate_context_window_config_path()?;
    let contents = match std::fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(err) => {
            warn!(
                "Failed to read context window config at {}: {err}",
                path.display()
            );
            return None;
        }
    };
    match toml::from_str::<ContextWindowConfig>(&contents) {
        Ok(config) => Some(config),
        Err(err) => {
            warn!(
                "Failed to parse context window config at {}: {err}",
                path.display()
            );
            None
        }
    }
}

fn locate_context_window_config_path() -> Option<PathBuf> {
    if let Ok(path) = env::var("LLM_CONTEXT_WINDOW_CONFIG") {
        let path = PathBuf::from(path);
        if path.exists() {
            return Some(path);
        }
        warn!(
            "LLM_CONTEXT_WINDOW_CONFIG points to a missing file: {}",
            path.display()
        );
    }

    let cwd = env::current_dir().ok()?;
    let candidates = [
        cwd.join("config/model_context_windows.toml"),
        cwd.join("ariadne-cli/config/model_context_windows.toml"),
    ];
    for path in candidates {
        if path.exists() {
            return Some(path);
        }
    }
    None
}

fn resolve_context_window_tokens(model: &str, config: &ContextWindowConfig) -> Option<usize> {
    if let Some(tokens) = config.models.get(model).copied() {
        return Some(tokens);
    }
    for models in config.providers.values() {
        if let Some(tokens) = models.get(model).copied() {
            return Some(tokens);
        }
    }
    None
}

#[derive(Debug, Clone)]
pub struct ContextCompaction {
    pub summary: String,
    pub usage: Option<LlmUsage>,
}

#[async_trait]
pub trait Translator: Send + Sync {
    async fn translate(
        &self,
        question: &str,
        context: &[ConversationTurn],
        context_summary: Option<&str>,
    ) -> CliResult<TranslationResult>;

    async fn compact_context(&self, context: &[ConversationTurn]) -> CliResult<ContextCompaction>;
}

pub struct LlmTranslator {
    llm: Box<dyn ::llm::LLMProvider>,
    structured_output: bool,
    config: LlmConfig,
}

impl LlmTranslator {
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
            .system(base_prompt(config.structured_output));

        if config.structured_output {
            builder = builder.schema(cypher_schema());
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
impl Translator for LlmTranslator {
    async fn translate(
        &self,
        question: &str,
        context: &[ConversationTurn],
        context_summary: Option<&str>,
    ) -> CliResult<TranslationResult> {
        let messages = build_messages(question, context, context_summary);
        let response = match self.llm.chat(&messages).await {
            Ok(response) => response,
            Err(err) => return Err(map_llm_error(err, self.structured_output)),
        };
        let usage = response.usage().map(LlmUsage::from);
        let text = response
            .text()
            .ok_or_else(|| "LLM response missing text".to_string())?;
        let cypher = if self.structured_output {
            parse_structured_cypher(&text)?
        } else {
            extract_cypher(&text)
        };
        Ok(TranslationResult { cypher, usage })
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

fn base_prompt(structured: bool) -> String {
    let prompt = ariadne_tools::full_prompt();
    if structured {
        format!(
            "{prompt}\n\nReturn JSON with a single key `cypher` and no extra fields. Do not include explanations or code fences."
        )
    } else {
        format!("{prompt}\n\nReturn only Cypher. Do not include explanations or code fences.")
    }
}

fn build_messages(
    question: &str,
    context: &[ConversationTurn],
    context_summary: Option<&str>,
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
        messages.push(ChatMessage::assistant().content(assistant).build());
    }
    messages.push(ChatMessage::user().content(question.trim()).build());
    messages
}

fn build_compaction_provider(config: &LlmConfig) -> CliResult<Box<dyn ::llm::LLMProvider>> {
    let mut builder = LLMBuilder::new()
        .backend(config.backend.clone())
        .model(config.model.clone())
        .timeout_seconds(config.timeout_secs)
        .normalize_response(true)
        .system(compaction_prompt());

    if !config.base_url.trim().is_empty() {
        builder = builder.base_url(config.base_url.clone());
    }
    if let Some(api_key) = &config.api_key {
        builder = builder.api_key(api_key.clone());
    }

    let llm = builder.build()?;
    Ok(llm)
}

fn compaction_prompt() -> String {
    "You summarize short-term investigation context for future Cypher generation. \
Return a concise, plain-text summary with key entities, filters, assumptions, and results. \
Keep it under 1200 characters. Do not return Cypher."
        .to_string()
}

fn build_compaction_messages(context: &[ConversationTurn]) -> Vec<ChatMessage> {
    let mut body = String::from(
        "Summarize the following conversation context for reuse in future questions.\n",
    );
    for (idx, turn) in context.iter().enumerate() {
        body.push_str(&format!("\nTurn {}:\n", idx + 1));
        body.push_str("User: ");
        body.push_str(turn.question.trim());
        body.push('\n');
        body.push_str("Cypher: ");
        body.push_str(turn.cypher.trim());
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

fn cypher_schema() -> StructuredOutputFormat {
    StructuredOutputFormat {
        name: "CypherQuery".to_string(),
        description: Some("Cypher query result".to_string()),
        schema: Some(serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "cypher": { "type": "string" }
            },
            "required": ["cypher"]
        })),
        strict: Some(true),
    }
}

fn extract_cypher(text: &str) -> String {
    let trimmed = text.trim();
    if let Some(stripped) = trimmed.strip_prefix("cypher:") {
        return stripped.trim().to_string();
    }
    if let Some(start) = trimmed.find("```cypher") {
        let rest = &trimmed[start + "```cypher".len()..];
        if let Some(end) = rest.find("```") {
            return rest[..end].trim().to_string();
        }
    }
    if let Some(start) = trimmed.find("```") {
        let rest = &trimmed[start + 3..];
        if let Some(end) = rest.find("```") {
            return rest[..end].trim().to_string();
        }
    }
    trimmed.to_string()
}

fn parse_structured_cypher(text: &str) -> CliResult<String> {
    let cleaned = clean_json_response(text);
    let payload: Value =
        serde_json::from_str(&cleaned).map_err(|e| format!("Invalid JSON response: {e}"))?;
    let cypher = payload
        .get("cypher")
        .and_then(|value| value.as_str())
        .ok_or_else(|| "JSON response missing 'cypher' field".to_string())?;
    Ok(extract_cypher(cypher))
}

fn clean_json_response(response_text: &str) -> String {
    let text = response_text.trim();

    if text.starts_with("```json") && text.ends_with("```") {
        let start = text.find("```json").unwrap() + 7;
        let end = text.rfind("```").unwrap();
        return text[start..end].trim().to_string();
    }

    if text.starts_with("```") && text.ends_with("```") {
        let start = text.find("```").unwrap() + 3;
        let end = text.rfind("```").unwrap();
        return text[start..end].trim().to_string();
    }

    text.to_string()
}

fn map_llm_error(err: LLMError, structured: bool) -> Box<dyn std::error::Error + Send + Sync> {
    if structured {
        match err {
            LLMError::ResponseFormatError {
                message,
                raw_response,
            } => {
                error!("LLM structured output failed: {message}. Raw response: {raw_response}");
                let msg = format!(
                    "Structured output failed: {message}. \
This provider/model may not support structured output. \
Set LLM_STRUCTURED_OUTPUT=0 to disable."
                );
                return std::io::Error::other(msg).into();
            }
            LLMError::InvalidRequest(message) => {
                let msg = format!(
                    "LLM request rejected: {message}. \
If this is due to response_format, set LLM_STRUCTURED_OUTPUT=0."
                );
                return std::io::Error::other(msg).into();
            }
            other => return Box::new(other),
        }
    }
    Box::new(err)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_cypher_from_fence() {
        let input = "```cypher\nMATCH (n) RETURN n\n```";
        assert_eq!(extract_cypher(input), "MATCH (n) RETURN n");
    }

    #[test]
    fn extract_cypher_from_prefix() {
        let input = "cypher: MATCH (n) RETURN n";
        assert_eq!(extract_cypher(input), "MATCH (n) RETURN n");
    }

    #[test]
    fn parse_structured_cypher_from_json() {
        let input = r#"{"cypher":"MATCH (n) RETURN n"}"#;
        let parsed = parse_structured_cypher(input).unwrap();
        assert_eq!(parsed, "MATCH (n) RETURN n");
    }

    #[test]
    fn parse_structured_cypher_from_fenced_json() {
        let input = "```json\n{\"cypher\":\"MATCH (n) RETURN n\"}\n```";
        let parsed = parse_structured_cypher(input).unwrap();
        assert_eq!(parsed, "MATCH (n) RETURN n");
    }

    #[test]
    fn context_window_parses_providers() {
        let input = r#"
[providers.anthropic]
"claude-sonnet-4-5-20250929" = 200000

[providers.openai]
"openai/gpt-5-mini-2025-08-07" = 400000
"#;
        let config: ContextWindowConfig = toml::from_str(input).unwrap();
        assert_eq!(
            resolve_context_window_tokens("claude-sonnet-4-5-20250929", &config),
            Some(200000)
        );
        assert_eq!(
            resolve_context_window_tokens("openai/gpt-5-mini-2025-08-07", &config),
            Some(400000)
        );
    }

    #[test]
    fn context_window_parses_models() {
        let input = r#"
[models]
"gemini-2.5-flash" = 1048576
"#;
        let config: ContextWindowConfig = toml::from_str(input).unwrap();
        assert_eq!(
            resolve_context_window_tokens("gemini-2.5-flash", &config),
            Some(1048576)
        );
    }

    #[test]
    fn context_window_models_take_precedence() {
        let input = r#"
[models]
"deepseek-r1" = 64000

[providers.deepseek]
"deepseek-r1" = 128000
"#;
        let config: ContextWindowConfig = toml::from_str(input).unwrap();
        assert_eq!(
            resolve_context_window_tokens("deepseek-r1", &config),
            Some(64000)
        );
    }
}
