use crate::errors::AriadneError;
use serde::Serialize;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryIssueKind {
    Parse,
    Semantic,
    Schema,
    Scope,
    Parameter,
    EngineLimitation,
    Backend,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryIssueSource {
    Validator,
    Backend,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueryIssue {
    pub kind: QueryIssueKind,
    pub message: String,
    pub source: QueryIssueSource,
    pub retryable: bool,
    pub repairable: bool,
}

impl QueryIssue {
    pub fn validation(kind: QueryIssueKind, message: impl Into<String>) -> Self {
        debug_assert!(matches!(
            kind,
            QueryIssueKind::Parse | QueryIssueKind::Semantic | QueryIssueKind::Schema
        ));
        Self {
            kind,
            message: message.into(),
            source: QueryIssueSource::Validator,
            retryable: false,
            repairable: true,
        }
    }

    pub fn backend(
        kind: QueryIssueKind,
        message: impl Into<String>,
        retryable: bool,
        repairable: bool,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            source: QueryIssueSource::Backend,
            retryable,
            repairable,
        }
    }

    pub fn retryable(&self) -> bool {
        self.retryable
    }

    pub fn repairable(&self) -> bool {
        self.repairable
    }

    pub fn invalid_params(&self) -> bool {
        matches!(
            self.kind,
            QueryIssueKind::Parse
                | QueryIssueKind::Semantic
                | QueryIssueKind::Schema
                | QueryIssueKind::Scope
                | QueryIssueKind::Parameter
                | QueryIssueKind::EngineLimitation
        )
    }

    pub fn kind_code(&self) -> &'static str {
        match self.kind {
            QueryIssueKind::Parse if self.source == QueryIssueSource::Backend => "syntax_error",
            QueryIssueKind::Parse => "parse_error",
            QueryIssueKind::Semantic => "semantic_error",
            QueryIssueKind::Schema => "schema_error",
            QueryIssueKind::Scope => "scope_error",
            QueryIssueKind::Parameter => "parameter_error",
            QueryIssueKind::EngineLimitation => "engine_limitation",
            QueryIssueKind::Backend => "backend_error",
        }
    }

    pub fn source_code(&self) -> &'static str {
        match self.source {
            QueryIssueSource::Validator => "validator",
            QueryIssueSource::Backend => "backend",
        }
    }

    pub fn feedback(&self) -> String {
        if self.source == QueryIssueSource::Backend
            && self.kind == QueryIssueKind::Semantic
            && self
                .message
                .to_ascii_lowercase()
                .contains("multiple results with the same name")
        {
            return format!(
                "Query execution failed ({}): {}. Give each RETURN expression a unique AS alias before retrying.",
                self.kind_code(),
                self.message
            );
        }
        match self.source {
            QueryIssueSource::Validator => format!(
                "Validation failed ({}): {}. Fix the Cypher to match the schema and syntax.",
                self.kind_code(),
                self.message
            ),
            QueryIssueSource::Backend => format!(
                "Query execution failed ({}): {}. Fix the Cypher and parameters before retrying.",
                self.kind_code(),
                self.message
            ),
        }
    }
}

impl fmt::Display for QueryIssue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for QueryIssue {}

pub fn classify_ariadne_error(error: &AriadneError) -> QueryIssue {
    classify_backend_error(error.to_string())
}

pub fn classify_backend_error(message: impl Into<String>) -> QueryIssue {
    let message = message.into();
    let message_lower = message.to_ascii_lowercase();
    if message_lower.contains("unbound variable") {
        return QueryIssue::backend(QueryIssueKind::Scope, message, false, true);
    }
    if message_lower.contains("parameter $") && message_lower.contains("not provided") {
        return QueryIssue::backend(QueryIssueKind::Parameter, message, false, true);
    }
    if message_lower.contains("multiple results with the same name") {
        return QueryIssue::backend(QueryIssueKind::Semantic, message, false, true);
    }
    if message_lower.contains("not yet implemented") {
        return QueryIssue::backend(QueryIssueKind::EngineLimitation, message, false, true);
    }
    if message_lower.contains("parsing error")
        || message_lower.contains("no viable alternative")
        || message_lower.contains("syntax")
    {
        return QueryIssue::backend(QueryIssueKind::Parse, message, false, true);
    }
    if message_lower.contains("rate limit")
        || message_lower.contains("too many tokens")
        || message_lower.contains("timeout")
        || message_lower.contains("temporar")
        || message_lower.contains("no current transaction to commit")
    {
        return QueryIssue::backend(QueryIssueKind::Backend, message, true, false);
    }
    QueryIssue::backend(QueryIssueKind::Backend, message, false, false)
}

#[cfg(test)]
mod tests {
    use super::{QueryIssueKind, QueryIssueSource, classify_backend_error};

    #[test]
    fn classifies_scope_errors_as_repairable() {
        let issue = classify_backend_error(
            "MemgraphError: QueryError: Query execution error: Unbound variable: ns.",
        );
        assert_eq!(issue.kind, QueryIssueKind::Scope);
        assert_eq!(issue.source, QueryIssueSource::Backend);
        assert!(!issue.retryable());
        assert!(issue.repairable());
        assert_eq!(issue.kind_code(), "scope_error");
    }

    #[test]
    fn classifies_semantic_backend_errors_as_repairable() {
        let issue = classify_backend_error(
            "MemgraphError: QueryError: Query execution error: Multiple results with the same name 'x' are not allowed.",
        );
        assert_eq!(issue.kind, QueryIssueKind::Semantic);
        assert!(!issue.retryable());
        assert!(issue.repairable());
        assert_eq!(issue.kind_code(), "semantic_error");
    }

    #[test]
    fn classifies_transient_backend_errors_as_retryable() {
        let issue = classify_backend_error(
            "MemgraphError: CommitError: Query execution error: No current transaction to commit.",
        );
        assert_eq!(issue.kind, QueryIssueKind::Backend);
        assert!(issue.retryable());
        assert!(!issue.repairable());
        assert_eq!(issue.kind_code(), "backend_error");
    }

    #[test]
    fn classifies_missing_parameters_as_repairable() {
        let issue = classify_backend_error(
            "MemgraphError: QueryError: Query execution error: Parameter $node not provided.",
        );
        assert_eq!(issue.kind, QueryIssueKind::Parameter);
        assert!(!issue.retryable());
        assert!(issue.repairable());
        assert_eq!(issue.kind_code(), "parameter_error");
    }

    #[test]
    fn semantic_duplicate_column_feedback_mentions_aliases() {
        let issue = classify_backend_error(
            "MemgraphError: QueryError: Query execution error: Multiple results with the same name 'x' are not allowed.",
        );
        assert!(
            issue
                .feedback()
                .contains("Give each RETURN expression a unique AS alias")
        );
    }
}
