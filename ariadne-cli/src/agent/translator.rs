use ::llm::builder::LLMBuilder;
use ::llm::chat::{ChatMessage, StructuredOutputFormat};
use async_trait::async_trait;

use crate::agent::prompts::base_prompt;
use crate::agent::types::{ConversationTurn, LlmConfig, LlmUsage, TranslationResult};
use crate::agent::util::{extract_cypher, map_llm_error, parse_structured_cypher};
use crate::error::CliResult;

#[async_trait]
pub trait Translator: Send + Sync {
    async fn translate(
        &self,
        question: &str,
        context: &[ConversationTurn],
        context_summary: Option<&str>,
        feedback: Option<&str>,
    ) -> CliResult<TranslationResult>;
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
        feedback: Option<&str>,
    ) -> CliResult<TranslationResult> {
        let messages = build_messages(question, context, context_summary, feedback);
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

fn build_messages(
    question: &str,
    context: &[ConversationTurn],
    context_summary: Option<&str>,
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
        messages.push(ChatMessage::assistant().content(assistant).build());
    }
    if let Some(feedback) = feedback {
        let feedback = feedback.trim();
        if !feedback.is_empty() {
            messages.push(
                ChatMessage::user()
                    .content(format!(
                        "Previous Cypher failed validation: {feedback}\n\
Please correct the Cypher. Return only the fixed query."
                    ))
                    .build(),
            );
        }
    }
    messages.push(ChatMessage::user().content(question.trim()).build());
    messages
}

fn cypher_schema() -> StructuredOutputFormat {
    const SCHEMA: &str = r#"
    {
        "name": "CypherQuery",
        "description": "Cypher query result",
        "strict": true,
        "schema": {
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "cypher": { "type": "string" }
            },
            "required": ["cypher"]
        }
    }
    "#;
    serde_json::from_str(SCHEMA).expect("invalid CypherQuery schema JSON")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cypher_schema_parses() {
        let schema = cypher_schema();
        assert_eq!(schema.name, "CypherQuery");
    }
}
