use crate::cluster_state::{ClusterState, Label, Node, NodeType, SharedClusterState};
use axum::http::header;
use axum::middleware::map_response;
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use axum_prometheus::PrometheusMetricLayer;
use k8s_openapi::api::core::v1::Pod;
use kube::api::ListParams;
use kube::{Api, Client, ResourceExt};
use shadow_rs::shadow;
use std::net::SocketAddr;
use std::sync::Mutex;
use tokio::signal;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::propagate_header::PropagateHeaderLayer;
use tower_http::sensitive_headers::SetSensitiveHeadersLayer;
use tower_http::trace;
use tracing::info;

mod cluster_state;
mod errors;
mod id_gen;
mod logger;
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

#[tokio::main]
async fn main() -> errors::Result<()> {
    logger::setup("INFO");

    let mut cluster_state: SharedClusterState = SharedClusterState::new(Mutex::new(ClusterState::new()));

    let pods: Api<Pod> = Api::default_namespaced(Client::try_default().await?);
    let lp = ListParams::default();
    let pods = pods.list(&lp).await?;

    {
        // FIXME
        let mut locked_cluster_state = cluster_state.lock().unwrap();
        for p in pods {
            println!("Found Pod: {}", p.name_any());
            let labels: Vec<Label> = p
                .labels()
                .iter()
                .map(|(k, v)| Label::new(k.clone(), v.clone()))
                .collect();
            let node = Node {
                uid: p.uid().unwrap(),
                name: p.name_any(),
                namespace: p.namespace().unwrap(),
                version: p.resource_version().unwrap(),
                node_type: NodeType::Pod,
                labels,
            };
            locked_cluster_state.add_node(node);
        }
    }

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

    info!("Server listening for HTTP on {}", &http_addr);
    let svc = route.into_make_service_with_connect_info::<SocketAddr>();
    let http_listener = tokio::net::TcpListener::bind(http_addr).await.unwrap();
    let f = tokio::spawn(async move {
        axum::serve(http_listener, svc.clone())
            .with_graceful_shutdown(shutdown_signal())
            .await
            .expect("Failed to start server")
    });
    f.await.expect("Failed to get the server running");
    info!("Server shutdown");

    Ok(())
}

async fn shutdown_signal() {
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
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    println!("signal received, starting graceful shutdown");
}
