use crate::error::CliResult;
use ariadne_cypher::{parse_query, validate_query, ValidationMode};

pub fn validate_cypher(cypher: &str) -> CliResult<()> {
    let query = parse_query(cypher)?;
    validate_query(&query, ValidationMode::ReadOnly)?;
    Ok(())
}
