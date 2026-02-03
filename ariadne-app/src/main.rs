use ariadne_core::errors::AriadneError;
use ariadne_core::kube_client::SnapshotKubeClient;
use ariadne_core::memgraph_async::MemgraphAsync;
use ariadne_core::state_resolver::ClusterStateResolver;
use axum::http::header;
use axum::middleware::map_response;
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use axum_prometheus::PrometheusMetricLayer;
use clap::{Parser, Subcommand};
use kube::config::KubeConfigOptions;
use shadow_rs::shadow;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::signal;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::propagate_header::PropagateHeaderLayer;
use tower_http::sensitive_headers::SetSensitiveHeadersLayer;
use tower_http::trace;
use tracing::{info, warn};

pub mod errors;
mod kube_tool;
pub mod logger;
mod routes;

shadow!(build);

#[derive(Parser)]
#[command(name = "ariadne-app")]
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

pub const APP_VERSION: &str = shadow_rs::formatcp!(
    "{} ({} {}), build_env: {}, {}, {}",
    build::PKG_VERSION,
    build::SHORT_COMMIT,
    build::BUILD_TIME,
    build::RUST_VERSION,
    build::RUST_CHANNEL,
    build::CARGO_VERSION
);

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

async fn fetch_state(
    resolver: ClusterStateResolver,
    memgraph: MemgraphAsync,
    token: CancellationToken,
    poll_interval: Duration,
) -> errors::Result<()> {
    info!("Starting fetch_state with poll_interval {poll_interval:?}");
    let mut id: usize = 0;

    let fetch_and_save_fn = || async {
        let new_state = resolver.resolve().await?;
        memgraph.create(new_state.clone()).await?;

        errors::Result::Ok(())
    };
    loop {
        tokio::select! {
            _ = token.cancelled() => {
                break;
            },
            _ = sleep(poll_interval) => {
                match fetch_and_save_fn().await {
                    Ok(_) => {}
                    Err(err) => {
                        warn!("Error in fetch_and_save_fn at iteration {id}: {:?}", err);
                    }
                }
                id += 1;
            },
        }
    }
    info!("Stopped fetch_state, number of loops {id}");
    Ok(())
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

    let kube_context: Option<String> = cli.kube_context;
    let kube_namespace: Option<String> = cli.kube_namespace;
    info!("KUBE_CONTEXT: {kube_context:?}, KUBE_NAMESPACE: {kube_namespace:?}");

    let kube_opts = KubeConfigOptions {
        context: kube_context,
        cluster: None,
        user: None,
    };

    if let Some(Command::Snapshot {
        command: SnapshotCommand::Export { output_dir },
    }) = cli.command
    {
        let resolver =
            ClusterStateResolver::new(cluster_name.clone(), &kube_opts, kube_namespace.as_deref())
                .await?;
        resolver.export_observed_snapshot_dir(output_dir)?;
        info!("Snapshot export complete");
        return Ok(());
    }

    let memgraph: MemgraphAsync = MemgraphAsync::try_new_from_url(memgraph_uri.as_str())?;

    let snapshot_dir: Option<String> = std::env::var("KUBE_SNAPSHOT_DIR").ok();
    let resolver = if let Some(snapshot_dir) = snapshot_dir {
        info!("Loading snapshot from directory: {snapshot_dir}");
        let snapshot_client = SnapshotKubeClient::from_dir(snapshot_dir)?;
        ClusterStateResolver::new_with_kube_client(cluster_name.clone(), Box::new(snapshot_client))
            .await?
    } else {
        ClusterStateResolver::new(cluster_name.clone(), &kube_opts, kube_namespace.as_deref())
            .await?
    };
    let cluster_state = resolver.resolve().await?;
    memgraph.create(cluster_state.clone()).await?;

    if let Ok(export_dir) = std::env::var("SNAPSHOT_EXPORT_DIR") {
        info!("Exporting snapshot to directory: {export_dir}");
        resolver.export_observed_snapshot_dir(export_dir)?;
    }

    let token: CancellationToken = CancellationToken::new();

    resolver.start_diff_loop(memgraph.clone(), token.clone());

    let main_router =
        routes::create_route(cluster_name, cluster_state.clone(), memgraph.clone()).await?;
    let (prometheus_layer, metric_handle) = PrometheusMetricLayer::pair();
    let route = Router::new()
        .merge(main_router)
        .route(
            "/render/metrics",
            get(|| async move { metric_handle.render() }),
        )
        .layer(prometheus_layer)
        .layer(map_response(set_version_header))
        // High level logging of requests and responses
        .layer(
            trace::TraceLayer::new_for_http()
                .make_span_with(trace::DefaultMakeSpan::new().include_headers(true))
                .on_request(trace::DefaultOnRequest::new().level(tracing::Level::DEBUG))
                .on_response(trace::DefaultOnResponse::new().level(tracing::Level::DEBUG)),
        )
        // Mark the `Authorization` request header as sensitive, so it doesn't
        // show in logs.
        .layer(SetSensitiveHeadersLayer::new(std::iter::once(
            header::AUTHORIZATION,
        )))
        // Compress responses
        .layer(CompressionLayer::new())
        // Propagate `x-request-id`s from requests to responses
        .layer(PropagateHeaderLayer::new(header::HeaderName::from_static(
            "x-request-id",
        )))
        // CORS configuration. This should probably be more restrictive in
        // production.
        .layer(CorsLayer::permissive());

    let http_host = std::env::var("HTTP_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let http_port = std::env::var("HTTP_PORT").unwrap_or_else(|_| "8080".to_string());
    let http_addr: SocketAddr = format!("{}:{}", http_host, http_port)
        .parse()
        .map_err(|err| {
            AriadneError::from(std::io::Error::new(std::io::ErrorKind::InvalidInput, err))
        })?;
    let svc = route.into_make_service_with_connect_info::<SocketAddr>();
    let http_listener = tokio::net::TcpListener::bind(http_addr)
        .await
        .map_err(AriadneError::from)?;
    let shutdown_token = token.clone();
    let svc_clone = svc.clone();
    let f: tokio::task::JoinHandle<errors::Result<()>> = tokio::spawn(async move {
        axum::serve(http_listener, svc_clone)
            .with_graceful_shutdown(shutdown_signal(shutdown_token))
            .await
            .map_err(|err| {
                AriadneError::from(std::io::Error::new(std::io::ErrorKind::Other, err))
            })?;
        Ok(())
    });

    info!(
        "Ariadne is running on http://{} with index page on http://{}/render/index.html",
        &http_addr, &http_addr
    );

    let poll_interval = Duration::from_secs(
        std::env::var("POLL_INTERVAL_SECONDS")
            .iter()
            .flat_map(|s| s.parse())
            .next()
            .unwrap_or(30),
    );

    let enable_full_rebuild_loop = std::env::var("ENABLE_FULL_REBUILD_LOOP")
        .map(|value| matches_ignore_ascii_case(&value, &["1", "true", "yes"]))
        .unwrap_or(false);

    let fetch_state_handle = if enable_full_rebuild_loop {
        info!("Full rebuild fallback loop enabled");
        let resolver_for_fallback = resolver;
        let memgraph_for_fallback = memgraph.clone();
        let t0 = token.clone();
        Some(tokio::spawn(async move {
            fetch_state(
                resolver_for_fallback,
                memgraph_for_fallback,
                t0,
                poll_interval,
            )
            .await
        }))
    } else {
        info!("Full rebuild fallback loop disabled");
        None
    };

    if let Some(fetch_state_handle) = fetch_state_handle {
        let (server_result, fallback_result) = tokio::join!(f, fetch_state_handle);
        server_result.map_err(|err| {
            AriadneError::from(std::io::Error::new(std::io::ErrorKind::Other, err))
        })??;
        fallback_result.map_err(|err| {
            AriadneError::from(std::io::Error::new(std::io::ErrorKind::Other, err))
        })??;
    } else {
        f.await.map_err(|err| {
            AriadneError::from(std::io::Error::new(std::io::ErrorKind::Other, err))
        })??;
    }
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
