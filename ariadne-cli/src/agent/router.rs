use ::llm::builder::LLMBuilder;
use ::llm::chat::{ChatMessage, StructuredOutputFormat};
use async_trait::async_trait;
use serde::Deserialize;

use crate::agent::prompts::router_prompt;
use crate::agent::types::{LlmConfig, LlmUsage, RouteDecision, RouteResult};
use crate::agent::util::{clean_json_response, map_llm_error};
use crate::error::CliResult;

#[async_trait]
pub trait Router: Send + Sync {
    async fn classify(&self, question: &str) -> CliResult<RouteResult>;
}

pub struct LlmRouter {
    llm: Box<dyn ::llm::LLMProvider>,
    structured_output: bool,
}

impl LlmRouter {
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
            .system(router_prompt());

        if config.structured_output {
            builder = builder.schema(router_schema());
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
        })
    }
}

#[async_trait]
impl Router for LlmRouter {
    async fn classify(&self, question: &str) -> CliResult<RouteResult> {
        let messages = vec![ChatMessage::user().content(question.trim()).build()];
        let response = match self.llm.chat(&messages).await {
            Ok(response) => response,
            Err(err) => return Err(map_llm_error(err, self.structured_output)),
        };
        let usage = response.usage().map(LlmUsage::from);
        let text = response
            .text()
            .ok_or_else(|| "LLM response missing text".to_string())?;

        let decision = if self.structured_output {
            parse_structured_route(&text)?
        } else {
            parse_unstructured_route(&text)
        };

        Ok(RouteResult { decision, usage })
    }
}

#[derive(Debug, Deserialize)]
struct RoutePayload {
    route: String,
}

fn parse_structured_route(text: &str) -> CliResult<RouteDecision> {
    let cleaned = clean_json_response(text);
    let payload: RoutePayload =
        serde_json::from_str(&cleaned).map_err(|e| format!("Invalid JSON response: {e}"))?;
    Ok(parse_route(&payload.route))
}

fn parse_unstructured_route(text: &str) -> RouteDecision {
    let lower = text.to_lowercase();
    if lower.contains("multi_turn") || lower.contains("multiturn") || lower.contains("multi-turn") {
        return RouteDecision::MultiTurn;
    }
    RouteDecision::OneShot
}

fn parse_route(raw: &str) -> RouteDecision {
    match raw.trim().to_lowercase().as_str() {
        "multi_turn" | "multi-turn" | "multiturn" => RouteDecision::MultiTurn,
        _ => RouteDecision::OneShot,
    }
}

fn router_schema() -> StructuredOutputFormat {
    const SCHEMA: &str = r#"
    {
        "name": "RouteDecision",
        "description": "Routing decision for agentic query planning",
        "strict": true,
        "schema": {
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "route": { "type": "string", "enum": ["one_shot", "multi_turn"] }
            },
            "required": ["route"]
        }
    }
    "#;
    serde_json::from_str(SCHEMA).expect("invalid RouteDecision schema JSON")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_structured_route_payload() {
        let input = r#"{"route":"multi_turn"}"#;
        let decision = parse_structured_route(input).unwrap();
        assert_eq!(decision, RouteDecision::MultiTurn);
    }
}
