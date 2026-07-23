use ariadne_core::errors::AriadneError;
use ariadne_core::graph_backend::GraphBackend;
use ariadne_core::in_memory::InMemoryBackend;
use ariadne_core::kube_client::SnapshotKubeClient;
use ariadne_core::memgraph_async::MemgraphAsync;
use ariadne_core::state_resolver::{
    ClusterStateResolver, RebuildStage, SourceSyncStage, StateDiffSummary,
    configured_source_sync_poll_interval,
};
use ariadne_mcp::health::{
    DiffSummary, GraphScope, HealthError, RebuildHealth, SharedCoverage, SnapshotManifest,
    SyncHealth, format_timestamp, now,
};
use ariadne_mcp::{
    APP_VERSION, errors, logger, read_snapshot_manifest, routes, write_snapshot_manifest,
};
use axum::Router;
use axum::http::header;
use axum::middleware::map_response;
use axum::response::Response;
use axum::routing::get;
use axum_prometheus::PrometheusMetricLayer;
use clap::{Parser, Subcommand};
use kube::config::KubeConfigOptions;
use metrics::{
    Unit, counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram,
};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, Once};
use std::time::{Duration, SystemTime};
use tokio::signal;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::propagate_header::PropagateHeaderLayer;
use tower_http::sensitive_headers::SetSensitiveHeadersLayer;
use tower_http::trace;
use tracing::{info, warn};

mod runtime_sync;
use runtime_sync::{describe_source_sync_metrics, run_rebuild_loop, run_source_sync_loop};

const SOURCE_SYNC_ATTEMPTS_TOTAL: &str = "ariadne_source_sync_attempts_total";
const SOURCE_SYNC_ATTEMPT_DURATION_MS: &str = "ariadne_source_sync_attempt_duration_ms";
const SOURCE_SYNC_FETCH_DURATION_MS: &str = "ariadne_source_sync_fetch_duration_ms";
const SOURCE_SYNC_DIFF_DURATION_MS: &str = "ariadne_source_sync_diff_duration_ms";
const SOURCE_SYNC_WRITE_DURATION_MS: &str = "ariadne_source_sync_write_duration_ms";
const SOURCE_SYNC_LAST_ATTEMPT_DURATION_MS: &str = "ariadne_source_sync_last_attempt_duration_ms";
const SOURCE_SYNC_LAST_FETCH_DURATION_MS: &str = "ariadne_source_sync_last_fetch_duration_ms";
const SOURCE_SYNC_LAST_DIFF_DURATION_MS: &str = "ariadne_source_sync_last_diff_duration_ms";
const SOURCE_SYNC_LAST_WRITE_DURATION_MS: &str = "ariadne_source_sync_last_write_duration_ms";
const DEFAULT_POLL_INTERVAL_SECONDS: u64 = 30;
static SOURCE_SYNC_METRICS_INIT: Once = Once::new();

#[derive(Parser)]
#[command(name = "ariadne-mcp")]
#[command(about = "Kubernetes graph service and snapshot tools", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
    #[arg(long, env = "CLUSTER")]
    cluster: String,
    #[arg(long, env = "KUBE_CONTEXT")]
    kube_context: Option<String>,
    #[arg(long, env = "KUBE_NAMESPACE")]
    kube_namespace: Option<String>,
}

#[derive(Subcommand)]
enum Command {
    Snapshot {
        #[command(subcommand)]
        command: SnapshotCommand,
    },
}

#[derive(Subcommand)]
enum SnapshotCommand {
    Export {
        #[arg(long, env = "SNAPSHOT_EXPORT_DIR")]
        output_dir: String,
    },
}

async fn set_version_header<B>(mut res: Response<B>) -> Response<B> {
    match APP_VERSION.parse() {
        Ok(value) => {
            res.headers_mut().insert("x-version-id", value);
        }
        Err(err) => {
            warn!("Failed to parse x-version-id header value: {err}");
        }
    }
    res
}

fn normalize_backend_kind(raw: &str) -> String {
    match raw {
        "in-memory" | "inmemory" | "memory" => "in-memory".to_string(),
        _ => "memgraph".to_string(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeMode {
    Live,
    Snapshot,
}

impl RuntimeMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Snapshot => "snapshot",
        }
    }
}

fn runtime_mode(snapshot_dir: Option<&str>) -> RuntimeMode {
    match snapshot_dir {
        Some(_) => RuntimeMode::Snapshot,
        None => RuntimeMode::Live,
    }
}

fn resolve_backend_kind(
    ariadne_graph_backend: Option<&str>,
    graph_backend: Option<&str>,
) -> String {
    let raw_backend = ariadne_graph_backend
        .or(graph_backend)
        .unwrap_or("memgraph");
    normalize_backend_kind(raw_backend)
}

fn runtime_scope(namespace: Option<&str>) -> GraphScope {
    match namespace {
        Some(namespace) => GraphScope::namespace(namespace),
        None => GraphScope::cluster(),
    }
}

fn parse_poll_interval(raw: Option<&str>) -> Duration {
    let seconds = raw
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_POLL_INTERVAL_SECONDS);
    Duration::from_secs(seconds)
}

fn parse_enable_full_rebuild_loop(raw: Option<&str>) -> bool {
    raw.map(|value| matches_ignore_ascii_case(value, &["1", "true", "yes"]))
        .unwrap_or(false)
}

fn resolve_runtime_scope(
    mode: RuntimeMode,
    snapshot_manifest: Option<&SnapshotManifest>,
    live_scope: &GraphScope,
) -> Option<GraphScope> {
    match mode {
        RuntimeMode::Snapshot => snapshot_manifest.map(|manifest| manifest.scope.clone()),
        RuntimeMode::Live => Some(live_scope.clone()),
    }
}

fn snapshot_manifest_captured_at(snapshot_manifest: Option<&SnapshotManifest>) -> Option<String> {
    snapshot_manifest.map(|manifest| manifest.captured_at.clone())
}

fn select_snapshot_export_manifest(
    snapshot_manifest: Option<&SnapshotManifest>,
    live_scope: &GraphScope,
    captured_at: String,
) -> SnapshotManifest {
    snapshot_manifest
        .cloned()
        .unwrap_or_else(|| SnapshotManifest {
            captured_at,
            scope: live_scope.clone(),
        })
}

fn parse_http_addr(http_host: &str, http_port: &str) -> errors::Result<SocketAddr> {
    let addr: SocketAddr = format!("{http_host}:{http_port}").parse().map_err(|err| {
        AriadneError::from(std::io::Error::new(std::io::ErrorKind::InvalidInput, err))
    })?;
    Ok(addr)
}

fn sum_durations(parts: &[Option<Duration>]) -> Option<Duration> {
    let mut total = Duration::ZERO;
    let mut found = false;
    for part in parts.iter().flatten() {
        total += *part;
        found = true;
    }
    found.then_some(total)
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis() as u64
}

fn duration_ms_f64(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> errors::Result<()> {
    logger::setup("INFO");

    let cli = Cli::parse();
    let cluster_name: String = cli.cluster;
    info!("CLUSTER: {}", cluster_name);

    let memgraph_uri: String = std::env::var("MEMGRAPH_URI")
        .ok()
        .unwrap_or_else(|| "bolt://localhost:7687".to_string());
    let ariadne_graph_backend = std::env::var("ARIADNE_GRAPH_BACKEND").ok();
    let graph_backend = std::env::var("GRAPH_BACKEND").ok();
    let backend_kind =
        resolve_backend_kind(ariadne_graph_backend.as_deref(), graph_backend.as_deref());

    let kube_context: Option<String> = cli.kube_context;
    let kube_namespace: Option<String> = cli.kube_namespace;
    info!("KUBE_CONTEXT: {kube_context:?}, KUBE_NAMESPACE: {kube_namespace:?}");
    info!("GRAPH_BACKEND: {backend_kind}");

    let kube_opts = KubeConfigOptions {
        context: kube_context,
        cluster: None,
        user: None,
    };

    let live_scope = runtime_scope(kube_namespace.as_deref());

    if let Some(Command::Snapshot {
        command: SnapshotCommand::Export { output_dir },
    }) = cli.command
    {
        let resolver =
            ClusterStateResolver::new(cluster_name.clone(), &kube_opts, kube_namespace.as_deref())
                .await?;
        resolver.export_observed_snapshot_dir(&output_dir)?;
        write_snapshot_manifest(
            Path::new(&output_dir),
            &SnapshotManifest {
                captured_at: format_timestamp(now()),
                scope: live_scope,
            },
        )?;
        info!("Snapshot export complete");
        return Ok(());
    }

    let graph: Arc<dyn GraphBackend> = match backend_kind.as_str() {
        "in-memory" => Arc::new(InMemoryBackend::new()),
        _ => Arc::new(MemgraphAsync::try_new_from_url(memgraph_uri.as_str())?),
    };

    let snapshot_dir: Option<String> = std::env::var("KUBE_SNAPSHOT_DIR").ok();
    let mode = runtime_mode(snapshot_dir.as_deref());
    let snapshot_manifest = if let Some(snapshot_dir) = snapshot_dir.as_deref() {
        let dir = Path::new(snapshot_dir);
        info!("Loading snapshot from directory: {snapshot_dir}");
        read_snapshot_manifest(dir)?
    } else {
        None
    };

    let initial_load_started = std::time::Instant::now();
    let resolver_started = std::time::Instant::now();
    let resolver = if let Some(snapshot_dir) = snapshot_dir.as_deref() {
        let snapshot_client = SnapshotKubeClient::from_dir(snapshot_dir)?;
        ClusterStateResolver::new_with_kube_client(cluster_name.clone(), Box::new(snapshot_client))
            .await?
    } else {
        ClusterStateResolver::new(cluster_name.clone(), &kube_opts, kube_namespace.as_deref())
            .await?
    };
    let resolver_duration = resolver_started.elapsed();
    let resolver = Arc::new(resolver);
    let cluster_state = resolver.resolve().await?;
    let graph_create_started = std::time::Instant::now();
    graph.create(cluster_state.clone()).await?;
    let graph_create_duration = graph_create_started.elapsed();
    let initial_load_duration = initial_load_started.elapsed();
    info!(
        mode = mode.as_str(),
        resolver_ms = duration_ms(resolver_duration),
        graph_create_ms = duration_ms(graph_create_duration),
        total_initial_load_ms = duration_ms(initial_load_duration),
        "Initial cluster state loaded"
    );

    let initial_load_succeeded = Arc::new(AtomicBool::new(true));
    let coverage: SharedCoverage = resolver.degraded_resource_kinds_handle();
    let initial_success_at = now();
    let source_sync_poll_interval = configured_source_sync_poll_interval();
    let source_sync = Arc::new(Mutex::new(if mode == RuntimeMode::Live {
        let mut sync = SyncHealth::bootstrap(initial_success_at);
        sync.poll_interval_seconds = source_sync_poll_interval.as_secs();
        sync
    } else {
        SyncHealth::default()
    }));

    let poll_interval = parse_poll_interval(std::env::var("POLL_INTERVAL_SECONDS").ok().as_deref());
    let enable_full_rebuild_loop =
        parse_enable_full_rebuild_loop(std::env::var("ENABLE_FULL_REBUILD_LOOP").ok().as_deref());
    let rebuild = Arc::new(Mutex::new(enable_full_rebuild_loop.then_some(
        RebuildHealth {
            poll_interval_seconds: poll_interval.as_secs(),
            ..Default::default()
        },
    )));

    let scope = resolve_runtime_scope(mode, snapshot_manifest.as_ref(), &live_scope);
    let snapshot_captured_at = snapshot_manifest_captured_at(snapshot_manifest.as_ref());

    if let Ok(export_dir) = std::env::var("SNAPSHOT_EXPORT_DIR") {
        info!("Exporting snapshot to directory: {export_dir}");
        resolver.export_observed_snapshot_dir(&export_dir)?;
        let manifest = select_snapshot_export_manifest(
            snapshot_manifest.as_ref(),
            &live_scope,
            format_timestamp(now()),
        );
        write_snapshot_manifest(Path::new(&export_dir), &manifest)?;
    }

    let token: CancellationToken = CancellationToken::new();

    let source_sync_handle: Option<JoinHandle<errors::Result<()>>> = if mode == RuntimeMode::Live {
        let resolver = resolver.clone();
        let graph = graph.clone();
        let source_sync = source_sync.clone();
        let token = token.clone();
        let poll_interval = source_sync_poll_interval;
        Some(tokio::spawn(async move {
            run_source_sync_loop(resolver, graph, source_sync, token, poll_interval).await
        }))
    } else {
        info!("Source sync loop disabled in snapshot mode");
        None
    };

    let rebuild_handle: Option<JoinHandle<errors::Result<()>>> = if enable_full_rebuild_loop {
        info!("Full rebuild fallback loop enabled");
        let resolver = resolver.clone();
        let graph = graph.clone();
        let rebuild = rebuild.clone();
        let token = token.clone();
        Some(tokio::spawn(async move {
            run_rebuild_loop(resolver, graph, rebuild, token, poll_interval).await
        }))
    } else {
        info!("Full rebuild fallback loop disabled");
        None
    };

    let main_router = routes::create_route(
        cluster_name.clone(),
        backend_kind.clone(),
        mode.as_str().to_string(),
        scope.clone(),
        snapshot_captured_at.clone(),
        cluster_state.clone(),
        graph.clone(),
        initial_load_succeeded.clone(),
        source_sync.clone(),
        rebuild.clone(),
        coverage.clone(),
        token.clone(),
    )
    .await?;

    let (prometheus_layer, metric_handle) = PrometheusMetricLayer::pair();
    describe_source_sync_metrics();
    let route = Router::new()
        .merge(main_router)
        .route(
            "/render/metrics",
            get(|| async move { metric_handle.render() }),
        )
        .layer(prometheus_layer)
        .layer(map_response(set_version_header))
        .layer(
            trace::TraceLayer::new_for_http()
                .make_span_with(trace::DefaultMakeSpan::new().include_headers(true))
                .on_request(trace::DefaultOnRequest::new().level(tracing::Level::DEBUG))
                .on_response(trace::DefaultOnResponse::new().level(tracing::Level::DEBUG)),
        )
        .layer(SetSensitiveHeadersLayer::new(std::iter::once(
            header::AUTHORIZATION,
        )))
        .layer(CompressionLayer::new())
        .layer(PropagateHeaderLayer::new(header::HeaderName::from_static(
            "x-request-id",
        )))
        .layer(CorsLayer::permissive());

    let http_host = std::env::var("HTTP_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let http_port = std::env::var("HTTP_PORT").unwrap_or_else(|_| "8080".to_string());
    let http_addr = parse_http_addr(&http_host, &http_port)?;
    let svc = route.into_make_service_with_connect_info::<SocketAddr>();
    let http_listener = tokio::net::TcpListener::bind(http_addr)
        .await
        .map_err(AriadneError::from)?;
    let shutdown_token = token.clone();
    let svc_clone = svc.clone();
    let server_handle: JoinHandle<errors::Result<()>> = tokio::spawn(async move {
        axum::serve(http_listener, svc_clone)
            .with_graceful_shutdown(shutdown_signal(shutdown_token))
            .await
            .map_err(|err| AriadneError::from(std::io::Error::other(err)))?;
        Ok(())
    });

    info!(
        "Ariadne is running on http://{} with index page on http://{}/render/index.html",
        &http_addr, &http_addr
    );

    server_handle
        .await
        .map_err(|err| AriadneError::from(std::io::Error::other(err)))??;
    token.cancel();

    if let Some(source_sync_handle) = source_sync_handle {
        source_sync_handle
            .await
            .map_err(|err| AriadneError::from(std::io::Error::other(err)))??;
    }

    if let Some(rebuild_handle) = rebuild_handle {
        rebuild_handle
            .await
            .map_err(|err| AriadneError::from(std::io::Error::other(err)))??;
    }

    graph.shutdown().await;

    info!("Server shutdown");
    Ok(())
}

fn matches_ignore_ascii_case(value: &str, truthy: &[&str]) -> bool {
    truthy
        .iter()
        .any(|candidate| value.eq_ignore_ascii_case(candidate))
}

async fn shutdown_signal(token: CancellationToken) {
    let ctrl_c = async {
        if let Err(err) = signal::ctrl_c().await {
            warn!("failed to install Ctrl+C handler: {err}");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(err) => {
                warn!("failed to install signal handler: {err}");
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            token.cancel()
        },
        _ = terminate => {
            token.cancel()
        },
    }

    println!("signal received, starting graceful shutdown");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_kind_prefers_ariadne_env_then_graph_backend() {
        assert_eq!(resolve_backend_kind(None, None), "memgraph");
        assert_eq!(
            resolve_backend_kind(Some("inmemory"), Some("memgraph")),
            "in-memory"
        );
        assert_eq!(resolve_backend_kind(None, Some("memory")), "in-memory");
        assert_eq!(
            resolve_backend_kind(Some("unknown-backend"), Some("in-memory")),
            "memgraph"
        );
    }

    #[test]
    fn runtime_mode_scope_and_snapshot_timestamp_are_derived_correctly() {
        let live_scope = GraphScope::namespace("team-a");
        let snapshot_manifest = SnapshotManifest {
            captured_at: "2026-03-29T00:00:00Z".to_string(),
            scope: GraphScope::cluster(),
        };

        assert_eq!(runtime_mode(None), RuntimeMode::Live);
        assert_eq!(runtime_mode(Some("/tmp/snapshot")), RuntimeMode::Snapshot);
        assert_eq!(RuntimeMode::Live.as_str(), "live");
        assert_eq!(RuntimeMode::Snapshot.as_str(), "snapshot");

        assert_eq!(
            resolve_runtime_scope(RuntimeMode::Live, Some(&snapshot_manifest), &live_scope),
            Some(live_scope.clone())
        );
        assert_eq!(
            resolve_runtime_scope(RuntimeMode::Snapshot, Some(&snapshot_manifest), &live_scope),
            Some(snapshot_manifest.scope.clone())
        );
        assert_eq!(
            resolve_runtime_scope(RuntimeMode::Snapshot, None, &live_scope),
            None
        );
        assert_eq!(
            snapshot_manifest_captured_at(Some(&snapshot_manifest)),
            Some(snapshot_manifest.captured_at.clone())
        );
        assert_eq!(snapshot_manifest_captured_at(None), None);
    }

    #[test]
    fn rebuild_flags_and_poll_interval_use_expected_parsing() {
        assert_eq!(
            parse_poll_interval(None),
            Duration::from_secs(DEFAULT_POLL_INTERVAL_SECONDS)
        );
        assert_eq!(parse_poll_interval(Some("45")), Duration::from_secs(45));
        assert_eq!(
            parse_poll_interval(Some("not-a-number")),
            Duration::from_secs(DEFAULT_POLL_INTERVAL_SECONDS)
        );

        assert!(parse_enable_full_rebuild_loop(Some("1")));
        assert!(parse_enable_full_rebuild_loop(Some("TRUE")));
        assert!(parse_enable_full_rebuild_loop(Some("yes")));
        assert!(!parse_enable_full_rebuild_loop(Some("false")));
        assert!(!parse_enable_full_rebuild_loop(None));
    }

    #[test]
    fn http_address_parsing_accepts_valid_values_and_rejects_invalid_ones() {
        let addr = parse_http_addr("127.0.0.1", "8080").expect("valid addr should parse");
        assert_eq!(addr, "127.0.0.1:8080".parse().expect("hardcoded parse"));
        assert!(parse_http_addr("127.0.0.1", "not-a-port").is_err());
    }

    #[test]
    fn snapshot_export_manifest_prefers_loaded_manifest_and_falls_back_to_live_scope() {
        let live_scope = GraphScope::namespace("platform");
        let existing = SnapshotManifest {
            captured_at: "2026-03-20T10:11:12Z".to_string(),
            scope: GraphScope::cluster(),
        };

        assert_eq!(
            select_snapshot_export_manifest(Some(&existing), &live_scope, "ignored".to_string()),
            existing
        );

        let fallback =
            select_snapshot_export_manifest(None, &live_scope, "2026-03-29T03:04:05Z".to_string());
        assert_eq!(fallback.captured_at, "2026-03-29T03:04:05Z");
        assert_eq!(fallback.scope, live_scope);
    }
}
