mod ast;
mod parser;
mod validate;

pub use ast::*;
pub use parser::parse_query;
pub use validate::{validate_query, ValidationMode};

use thiserror::Error;
use tree_sitter::{Parser, Tree};

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("Cypher parser failed to initialize")]
    Language,
    #[error("Cypher parse returned no tree")]
    ParseFailed,
    #[error("Cypher syntax error in parse tree")]
    Syntax,
}

#[derive(Debug, Error)]
pub enum CypherError {
    #[error("{0}")]
    Parse(#[from] ParseError),
    #[error("Unsupported syntax: {message} at {span}")]
    Unsupported { message: String, span: String },
    #[error("Semantic error: {message} at {span}")]
    Semantic { message: String, span: String },
    #[error("Invalid text at {span}")]
    InvalidText { span: String },
    #[error("Invalid literal {kind}: {text}")]
    InvalidLiteral { kind: String, text: String },
}

impl CypherError {
    pub(crate) fn unsupported(message: impl Into<String>, span: Span) -> Self {
        CypherError::Unsupported {
            message: message.into(),
            span: span.display(),
        }
    }

    pub(crate) fn semantic(message: impl Into<String>, span: Span) -> Self {
        CypherError::Semantic {
            message: message.into(),
            span: span.display(),
        }
    }

    pub(crate) fn missing(message: impl Into<String>, span: Span) -> Self {
        CypherError::Semantic {
            message: message.into(),
            span: span.display(),
        }
    }

    pub(crate) fn invalid_text(span: Span) -> Self {
        CypherError::InvalidText {
            span: span.display(),
        }
    }

    pub(crate) fn invalid_literal(kind: impl Into<String>, text: String) -> Self {
        CypherError::InvalidLiteral {
            kind: kind.into(),
            text,
        }
    }
}

pub fn parse_cypher(input: &str) -> Result<Tree, ParseError> {
    let mut parser = Parser::new();
    let language = tree_sitter::Language::new(tree_sitter_cypher::LANGUAGE);
    parser
        .set_language(&language)
        .map_err(|_| ParseError::Language)?;
    let tree = parser.parse(input, None).ok_or(ParseError::ParseFailed)?;
    if tree.root_node().has_error() {
        return Err(ParseError::Syntax);
    }
    Ok(tree)
}
