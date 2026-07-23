use crate::logger::setup;
use ariadne_tools::{full_prompt, schema_prompt};
use clap::Parser;
#[cfg(feature = "build-info")]
use shadow_rs::shadow;
use tracing::info;

pub mod logger;

#[cfg(feature = "build-info")]
shadow!(build);

#[cfg(feature = "build-info")]
pub const APP_VERSION: &str = shadow_rs::formatcp!(
    "{} ({} {}), build_env: {}, {}, {}",
    build::PKG_VERSION,
    build::SHORT_COMMIT,
    build::BUILD_TIME,
    build::RUST_VERSION,
    build::RUST_CHANNEL,
    build::CARGO_VERSION
);

#[cfg(not(feature = "build-info"))]
pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser, Debug, Clone)]
#[clap(author, version = APP_VERSION, about, long_about = None)]
struct AppArgs {
    #[arg(
        long,
        help = "Print the full prompt template with schema and relationships"
    )]
    full_prompt: bool,
}

fn main() {
    setup("ariadne_tools", "debug");
    let args = AppArgs::parse();
    info!("Received args: {:?}", args);

    let prompt = if args.full_prompt {
        full_prompt()
    } else {
        schema_prompt()
    };
    println!("{prompt}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_schema() {
        let schema = schema_prompt();

        assert!(schema.starts_with("Node properties:\n"));
        assert!(schema.contains("Container: 6 properties (container_type: string"));
        assert!(schema.contains(
            "PersistentVolumeClaim: 5 properties (apiVersion: string, kind: string, metadata:"
        ));
        assert!(schema.contains("Referenced types (used via `#/$defs/`):\n"));
    }
}
