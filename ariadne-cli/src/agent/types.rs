use ::llm::builder::LLMBackend;

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

#[derive(Debug, Clone)]
pub struct ContextCompaction {
    pub summary: String,
    pub usage: Option<LlmUsage>,
}

#[derive(Debug, Clone)]
pub struct AnalysisResult {
    pub answer: String,
    pub follow_ups: Vec<String>,
    pub confidence: Option<String>,
    pub usage: Option<LlmUsage>,
}
