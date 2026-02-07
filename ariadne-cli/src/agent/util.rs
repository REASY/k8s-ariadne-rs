use ::llm::error::LLMError;
use serde_json::Value;
use tracing::error;

use crate::error::CliResult;

pub fn extract_cypher(text: &str) -> String {
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

pub fn parse_structured_cypher(text: &str) -> CliResult<String> {
    let cleaned = clean_json_response(text);
    let payload: Value =
        serde_json::from_str(&cleaned).map_err(|e| format!("Invalid JSON response: {e}"))?;
    let cypher = payload
        .get("cypher")
        .and_then(|value| value.as_str())
        .ok_or_else(|| "JSON response missing 'cypher' field".to_string())?;
    Ok(extract_cypher(cypher))
}

pub fn clean_json_response(response_text: &str) -> String {
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

pub fn map_llm_error(err: LLMError, structured: bool) -> Box<dyn std::error::Error + Send + Sync> {
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
