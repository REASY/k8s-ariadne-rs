use crate::health::{GraphScope, RebuildHealth, SharedCoverage, SyncHealth};
use crate::kube_tool::KubeTool;
use ariadne_core::graph_backend::GraphBackend;
use ariadne_core::prelude::*;
use ariadne_core::state::{DirectedGraph, SharedClusterState};
use ariadne_core::types::{Cluster, Edge, ResourceType};
use axum::extract::State;
use axum::response::Html;
use axum::routing::get;
use axum::{Json, Router};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use serde::{Deserialize, Serialize};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use strum::IntoEnumIterator;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
struct AppState {
    cluster_state: SharedClusterState,
}

#[allow(clippy::too_many_arguments)]
pub async fn create_route(
    cluster_name: String,
    backend_kind: String,
    mode: String,
    scope: Option<GraphScope>,
    snapshot_captured_at: Option<String>,
    cluster_state: SharedClusterState,
    graph: Arc<dyn GraphBackend>,
    initial_load_succeeded: Arc<AtomicBool>,
    source_sync: Arc<Mutex<SyncHealth>>,
    rebuild: Arc<Mutex<Option<RebuildHealth>>>,
    coverage: SharedCoverage,
    cancellation_token: CancellationToken,
) -> Result<Router> {
    let mcp_cluster_state = cluster_state.clone();
    let service = StreamableHttpService::new(
        move || {
            Ok(KubeTool::new_tool(
                cluster_name.clone(),
                backend_kind.clone(),
                mode.clone(),
                scope.clone(),
                snapshot_captured_at.clone(),
                mcp_cluster_state.clone(),
                graph.clone(),
                initial_load_succeeded.clone(),
                source_sync.clone(),
                rebuild.clone(),
                coverage.clone(),
            ))
        },
        LocalSessionManager::default().into(),
        StreamableHttpServerConfig::default().with_cancellation_token(cancellation_token),
    );

    let state = AppState { cluster_state };
    let get_layer_route = Router::new()
        .route("/render/index.html", get(html))
        .route("/render/v1/graph", get(get_graph))
        .route("/render/v1/metadata", get(get_metadata))
        .nest_service("/mcp", service)
        .with_state(state);
    Ok(Router::new().merge(get_layer_route))
}

#[tracing::instrument(level = "INFO")]
async fn get_graph(State(state): State<AppState>) -> Json<DirectedGraph> {
    let lock = state.cluster_state.lock().unwrap();
    Json(lock.to_directed_graph())
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GraphMetadata {
    resource_types: Vec<ResourceType>,
    edge_types: Vec<Edge>,
    cluster: Cluster,
}

#[tracing::instrument(level = "INFO")]
async fn get_metadata(State(state): State<AppState>) -> Json<GraphMetadata> {
    let resource_types: Vec<ResourceType> = ResourceType::iter().collect();
    let edge_types: Vec<Edge> = Edge::iter().collect();

    let cluster = {
        let lock = state.cluster_state.lock().unwrap();
        lock.cluster.clone()
    };

    Json(GraphMetadata {
        cluster,
        resource_types,
        edge_types,
    })
}

async fn html() -> Html<&'static str> {
    Html(include_str!("index.html"))
}
