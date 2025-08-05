use crate::kube_tool::KubeTool;
use ariadne_core::prelude::*;
use ariadne_core::state::{DirectedGraph, SharedClusterState};
use ariadne_core::types::{Edge, ResourceType};
use axum::extract::State;
use axum::response::Html;
use axum::routing::get;
use axum::{Json, Router};
use rmcp::transport::streamable_http_server::{
    session::local::LocalSessionManager, StreamableHttpService,
};
use serde::{Deserialize, Serialize};
use strum::IntoEnumIterator;

#[derive(Debug, Clone)]
struct AppState {
    cluster_state: SharedClusterState,
}

pub async fn create_route(
    cluster_name: String,
    cluster_state: SharedClusterState,
    memgraph_uri: String,
) -> Result<Router> {
    let service = StreamableHttpService::new(
        move || {
            Ok(KubeTool::new_tool(
                cluster_name.clone(),
                memgraph_uri.clone(),
            ))
        },
        LocalSessionManager::default().into(),
        Default::default(),
    );

    let state = AppState { cluster_state };
    let get_layer_route = Router::new()
        .route("/index.html", get(html))
        .route("/v1/graph", get(get_graph))
        .route("/v1/metadata", get(get_metadata))
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
}

#[tracing::instrument(level = "INFO")]
async fn get_metadata(State(state): State<AppState>) -> Json<GraphMetadata> {
    let resource_types: Vec<ResourceType> = ResourceType::iter().collect();
    let edge_types: Vec<Edge> = Edge::iter().collect();

    Json(GraphMetadata {
        resource_types,
        edge_types,
    })
}

async fn html() -> Html<&'static str> {
    Html(include_str!("index.html"))
}
