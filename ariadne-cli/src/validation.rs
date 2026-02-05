use crate::error::CliResult;
use ariadne_cypher::{parse_query, validate_query, ValidationMode};

pub fn validate_cypher(cypher: &str) -> CliResult<()> {
    let query = match parse_query(cypher) {
        Ok(query) => query,
        Err(err) => {
            tracing::error!(error = %err, cypher = %cypher, "Cypher parse failed");
            return Err(err.into());
        }
    };
    if let Err(err) = validate_query(&query, ValidationMode::ReadOnly) {
        tracing::error!(error = %err, cypher = %cypher, "Cypher validation failed");
        return Err(err.into());
    }
    Ok(())
}
