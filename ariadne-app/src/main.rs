use ariadne_core::memgraph;
use ariadne_core::state::{ClusterState, SharedClusterState};
use ariadne_core::state_resolver::ClusterStateResolver;
use axum::http::header;
use axum::middleware::map_response;
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use axum_prometheus::PrometheusMetricLayer;
use kube::config::KubeConfigOptions;
use rsmgclient::ConnectParams;
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
    context: Option<String>,
    namespace: Option<String>,
    cluster_state: SharedClusterState,
    token: CancellationToken,
) -> errors::Result<()> {
    info!("Starting fetch_state");
    let mut id: usize = 0;
    let kube_opts = KubeConfigOptions {
        context,
        cluster: None,
        user: None,
    };
    let resolver = ClusterStateResolver::new(&kube_opts, namespace.as_deref()).await?;
    loop {
        if token.is_cancelled() {
            break;
        }
        let new_state = resolver.resolve().await?;

        {
            let mut mem_graph = memgraph::Memgraph::try_new(ConnectParams {
                host: Some(String::from("localhost")),
                ..Default::default()
            })?;

            mem_graph.create(&new_state)?;
        }

        {
            let mut old_locked_state = cluster_state.lock().unwrap();
            *old_locked_state = new_state;
        }

        sleep(Duration::from_millis(400000)).await;
        id += 1;
    }
    info!("Stopped fetch_state, number of loops {id}");
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> errors::Result<()> {
    logger::setup("INFO");

    let context: Option<String> = std::env::var("KUBE_CONTEXT").ok();
    let namespace: Option<String> = std::env::var("KUBE_NAMESPACE").ok();
    info!("context: {context:?}, namespace: {namespace:?}");

    let token = CancellationToken::new();

    let cluster_state: SharedClusterState =
        SharedClusterState::new(Mutex::new(ClusterState::new()));

    let c0 = cluster_state.clone();
    let t0 = token.clone();
    let fetch_state_handle =
        tokio::spawn(async move { fetch_state(context, namespace, c0, t0).await.unwrap() });
    info!("Created fetch_state_handle");

    let (prometheus_layer, metric_handle) = PrometheusMetricLayer::pair();
    let route = Router::new()
        .merge(routes::create_route(cluster_state.clone()).await)
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

    let http_addr: SocketAddr = format!("{}:{}", "127.0.0.1", "18080").parse().unwrap();

    info!("Server listening for HTTP on http://{}", &http_addr);
    info!("Index page is on http://{}/index.html", &http_addr);
    let svc = route.into_make_service_with_connect_info::<SocketAddr>();
    let http_listener = tokio::net::TcpListener::bind(http_addr).await.unwrap();
    let f = tokio::spawn(async move {
        axum::serve(http_listener, svc.clone())
            .with_graceful_shutdown(shutdown_signal(token.clone()))
            .await
            .expect("Failed to start server")
    });
    let (f0, f1) = tokio::join!(f, fetch_state_handle);
    f0.unwrap();
    f1.unwrap();
    info!("Server shutdown");

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
