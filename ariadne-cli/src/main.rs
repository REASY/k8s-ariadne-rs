mod error;
mod llm;
mod tui;
mod validation;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use ::llm::builder::LLMBackend;
use clap::Parser;
use kube::config::KubeConfigOptions;
use tokio_util::sync::CancellationToken;

use ariadne_core::graph_backend::GraphBackend;
use ariadne_core::in_memory::InMemoryBackend;
use ariadne_core::kube_client::SnapshotKubeClient;
use ariadne_core::state_resolver::ClusterStateResolver;

use crate::error::CliResult;
use crate::llm::{LlmConfig, LlmTranslator, Translator};
use crate::tui::run_tui;

#[derive(Parser, Debug)]
#[command(name = "ariadne-cli")]
#[command(about = "Interactive TUI for querying Kubernetes graphs", long_about = None)]
struct Cli {
    #[arg(long, env = "CLUSTER")]
    cluster: String,
    #[arg(long, env = "KUBE_CONTEXT")]
    kube_context: Option<String>,
    #[arg(long, env = "KUBE_NAMESPACE")]
    kube_namespace: Option<String>,
    #[arg(long, env = "KUBE_SNAPSHOT_DIR")]
    snapshot_dir: Option<String>,
    #[arg(long, env = "LLM_BACKEND", default_value = "openai")]
    llm_backend: LLMBackend,
    #[arg(long, env = "LLM_BASE_URL")]
    llm_base_url: String,
    #[arg(long, env = "LLM_MODEL")]
    llm_model: String,
    #[arg(long, env = "LLM_API_KEY")]
    llm_api_key: Option<String>,
    #[arg(long, env = "LLM_TIMEOUT_SECS", default_value_t = 60)]
    llm_timeout_secs: u64,
    #[arg(long, env = "LLM_STRUCTURED_OUTPUT", default_value_t = true)]
    llm_structured_output: bool,
}

fn main() -> CliResult<()> {
    init_logging()?;

    let cli = Cli::parse();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let backend: Arc<dyn GraphBackend> = Arc::new(InMemoryBackend::new());

    let kube_opts = KubeConfigOptions {
        context: cli.kube_context.clone(),
        cluster: None,
        user: None,
    };

    let resolver = runtime.block_on(async {
        if let Some(snapshot_dir) = &cli.snapshot_dir {
            let snapshot_client = SnapshotKubeClient::from_dir(snapshot_dir.clone())?;
            ClusterStateResolver::new_with_kube_client(
                cli.cluster.clone(),
                Box::new(snapshot_client),
            )
            .await
        } else {
            ClusterStateResolver::new(
                cli.cluster.clone(),
                &kube_opts,
                cli.kube_namespace.as_deref(),
            )
            .await
        }
    })?;

    let cluster_state = runtime.block_on(async { resolver.resolve().await })?;
    if let Err(err) = runtime.block_on(async { backend.create(cluster_state.clone()).await }) {
        tracing::error!("Graph backend initialization failed: {err}");
        return Err(err.into());
    }

    let token = CancellationToken::new();
    runtime.block_on(async {
        resolver.start_diff_loop(backend.clone(), token.clone());
    });

    let translator: Arc<dyn Translator> = Arc::new(LlmTranslator::try_new(LlmConfig {
        backend: cli.llm_backend,
        base_url: cli.llm_base_url,
        model: cli.llm_model,
        api_key: cli.llm_api_key,
        timeout_secs: cli.llm_timeout_secs,
        structured_output: cli.llm_structured_output,
    })?);

    let tui_result = run_tui(&runtime, backend.clone(), translator, token);

    runtime.block_on(async { backend.shutdown().await });

    tui_result
}

fn init_logging() -> CliResult<()> {
    let log_target = std::env::var("ARIADNE_CLI_LOG").ok();
    match log_target.as_deref() {
        Some("stderr") => {
            tracing_subscriber::fmt()
                .with_env_filter("INFO")
                .with_writer(std::io::stderr)
                .init();
        }
        Some("stdout") => {
            tracing_subscriber::fmt()
                .with_env_filter("INFO")
                .with_writer(std::io::stdout)
                .init();
        }
        Some(path) => {
            let file = open_log_file(Path::new(path))?;
            tracing_subscriber::fmt()
                .with_env_filter("INFO")
                .with_writer(file)
                .with_ansi(false)
                .init();
        }
        None => {
            if let Some(path) = default_log_path() {
                if let Ok(file) = open_log_file(&path) {
                    tracing_subscriber::fmt()
                        .with_env_filter("INFO")
                        .with_writer(file)
                        .with_ansi(false)
                        .init();
                    return Ok(());
                }
            }
            tracing_subscriber::fmt()
                .with_env_filter("INFO")
                .with_writer(std::io::sink)
                .init();
        }
    }
    Ok(())
}

fn default_log_path() -> Option<PathBuf> {
    if let Ok(path) = std::env::var("XDG_STATE_HOME") {
        return Some(
            PathBuf::from(path)
                .join("ariadne-cli")
                .join("ariadne-cli.log"),
        );
    }
    let home = std::env::var("HOME").ok()?;
    if cfg!(target_os = "macos") {
        Some(PathBuf::from(home).join("Library/Logs/ariadne-cli.log"))
    } else {
        Some(
            PathBuf::from(home)
                .join(".local/state")
                .join("ariadne-cli")
                .join("ariadne-cli.log"),
        )
    }
}

fn open_log_file(path: &Path) -> CliResult<std::fs::File> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?)
}
