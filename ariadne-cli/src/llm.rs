use ::llm::builder::{LLMBackend, LLMBuilder};
use ::llm::chat::{ChatMessage, StructuredOutputFormat};
use ::llm::error::LLMError;
use async_trait::async_trait;
use serde_json::Value;
use tracing::error;

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
pub struct LlmConfig {
    pub backend: LLMBackend,
    pub base_url: String,
    pub model: String,
    pub api_key: Option<String>,
    pub timeout_secs: u64,
    pub structured_output: bool,
}

#[async_trait]
pub trait Translator: Send + Sync {
    async fn translate(&self, question: &str) -> CliResult<TranslationResult>;
}

pub struct LlmTranslator {
    llm: Box<dyn ::llm::LLMProvider>,
    structured_output: bool,
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
            .backend(config.backend)
            .model(config.model)
            .timeout_seconds(config.timeout_secs)
            .normalize_response(true)
            .system(base_prompt(config.structured_output));

        if config.structured_output {
            builder = builder.schema(cypher_schema());
        }

        if !config.base_url.trim().is_empty() {
            builder = builder.base_url(config.base_url);
        }
        if let Some(api_key) = config.api_key {
            builder = builder.api_key(api_key);
        }

        let llm = builder.build()?;
        Ok(Self {
            llm,
            structured_output: config.structured_output,
        })
    }
}

#[async_trait]
impl Translator for LlmTranslator {
    async fn translate(&self, question: &str) -> CliResult<TranslationResult> {
        let messages = build_messages(question);
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

fn build_messages(question: &str) -> Vec<ChatMessage> {
    vec![ChatMessage::user().content(question.trim()).build()]
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
}
