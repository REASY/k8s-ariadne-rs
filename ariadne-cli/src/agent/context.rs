use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::sync::OnceLock;
use tracing::warn;

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
    CONFIG.get_or_init(read_context_window_config).as_ref()
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
    candidates.into_iter().find(|path| path.exists())
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

#[cfg(test)]
mod tests {
    use super::*;

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
