use ariadne_core::cluster_state::{DirectedGraph, SharedClusterState};
use axum::extract::State;
use axum::response::Html;
use axum::routing::get;
use axum::{Json, Router};

#[derive(Debug, Clone)]
struct AppState {
    cluster_state: SharedClusterState,
}

pub async fn create_route(cluster_state: SharedClusterState) -> Router {
    let state = AppState { cluster_state };
    let get_layer_route = Router::new()
        .route("/index.html", get(html))
        .route("/v1/graph", get(get_graph))
        .with_state(state);
    Router::new().merge(get_layer_route)
}

#[tracing::instrument(level = "INFO")]
async fn get_graph(State(state): State<AppState>) -> Json<DirectedGraph> {
    let lock = state.cluster_state.lock().unwrap();
    Json(lock.to_directed_graph())
}

async fn html() -> Html<&'static str> {
    Html(include_str!("index.html"))
}
