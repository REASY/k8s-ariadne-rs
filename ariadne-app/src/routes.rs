use ariadne_core::state::{DirectedGraph, SharedClusterState};
use ariadne_core::types::{Edge, ResourceType};
use axum::extract::State;
use axum::response::Html;
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use strum::IntoEnumIterator;

#[derive(Debug, Clone)]
struct AppState {
    cluster_state: SharedClusterState,
}

pub async fn create_route(cluster_state: SharedClusterState) -> Router {
    let state = AppState { cluster_state };
    let get_layer_route = Router::new()
        .route("/index.html", get(html))
        .route("/v1/graph", get(get_graph))
        .route("/v1/metadata", get(get_metadata))
        .with_state(state);
    Router::new().merge(get_layer_route)
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
