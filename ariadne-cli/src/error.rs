#[allow(dead_code)]
pub type CliResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
