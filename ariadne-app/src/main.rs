use ariadne_core::memgraph;
use ariadne_core::prelude::*;
use ariadne_core::state::{ClusterState, SharedClusterState};
use ariadne_core::state_resolver::ClusterStateResolver;
use axum::http::header;
use axum::middleware::map_response;
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use axum_prometheus::PrometheusMetricLayer;
use kube::config::KubeConfigOptions;
use shadow_rs::shadow;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::time::Duration;
use tokio::signal;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::propagate_header::PropagateHeaderLayer;
use tower_http::sensitive_headers::SetSensitiveHeadersLayer;
use tower_http::trace;
use tracing::info;

pub mod errors;
mod kube_tool;
pub mod logger;
mod routes;

shadow!(build);

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
    res.headers_mut()
        .insert("x-version-id", APP_VERSION.parse().unwrap());
    res
}

async fn fetch_state(
    memgraph_uri: String,
    resolver: ClusterStateResolver,
    cluster_state: SharedClusterState,
    token: CancellationToken,
) -> errors::Result<()> {
    info!("Starting fetch_state");
    let mut id: usize = 0;
    loop {
        tokio::select! {
            _ = token.cancelled() => {
                break;
            },
            _ = sleep(Duration::from_secs(10)) => {
                let new_state = resolver.resolve().await?;
                create_db_state(memgraph_uri.clone(), &new_state).await?;

                {
                    let mut old_locked_state = cluster_state.lock().unwrap();
                    *old_locked_state = new_state;
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

    let cluster_name: String =
        std::env::var("CLUSTER").expect("Env variable `CLUSTER` is required but not present");
    info!("CLUSTER: {}", cluster_name);

    let memgraph_uri: String = std::env::var("MEMGRAPH_URI")
        .ok()
        .unwrap_or_else(|| "bolt://localhost:7687".to_string());

    let kube_context: Option<String> = std::env::var("KUBE_CONTEXT").ok();
    let kube_namespace: Option<String> = std::env::var("KUBE_NAMESPACE").ok();
    info!("KUBE_CONTEXT: {kube_context:?}, KUBE_NAMESPACE: {kube_namespace:?}");

    let kube_opts = KubeConfigOptions {
        context: kube_context,
        cluster: None,
        user: None,
    };
    let resolver =
        ClusterStateResolver::new(cluster_name.clone(), &kube_opts, kube_namespace.as_deref())
            .await?;
    let init_state = resolver.resolve().await?;
    create_db_state(memgraph_uri.clone(), &init_state).await?;
    let cluster_state: SharedClusterState = SharedClusterState::new(Mutex::new(init_state));

    let token = CancellationToken::new();

    let c0 = cluster_state.clone();
    let t0 = token.clone();
    let memgraph_uri_clone = memgraph_uri.clone();

    let main_router =
        routes::create_route(cluster_name, cluster_state.clone(), memgraph_uri).await?;
    let (prometheus_layer, metric_handle) = PrometheusMetricLayer::pair();
    let route = Router::new()
        .merge(main_router)
        .route("/metrics", get(|| async move { metric_handle.render() }))
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
    let http_addr: SocketAddr = format!("{}:{}", http_host, http_port).parse().unwrap();
    let svc = route.into_make_service_with_connect_info::<SocketAddr>();
    let http_listener = tokio::net::TcpListener::bind(http_addr).await.unwrap();
    let f = tokio::spawn(async move {
        axum::serve(http_listener, svc.clone())
            .with_graceful_shutdown(shutdown_signal(token.clone()))
            .await
            .expect("Failed to start server")
    });

    info!(
        "Ariadne is running on http://{} with index page on http://{}/render/index.html",
        &http_addr, &http_addr
    );

    let fetch_state_handle = tokio::spawn(async move {
        fetch_state(memgraph_uri_clone, resolver, c0, t0)
            .await
            .unwrap()
    });
    info!("Created fetch_state_handle");

    let (f0, f1) = tokio::join!(f, fetch_state_handle);
    f0.unwrap();
    f1.unwrap();
    info!("Server shutdown");

    Ok(())
}

async fn create_db_state(memgraph_uri: String, new_state: &ClusterState) -> Result<()> {
    let mut mem_graph = memgraph::Memgraph::try_new_from_url(memgraph_uri.as_str())?;
    mem_graph.create(new_state)?;
    Ok(())
}

async fn shutdown_signal(token: CancellationToken) {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
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
