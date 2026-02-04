pub mod errors;
pub(crate) mod graph_actor;
pub mod graph_backend;
pub mod graph_schema;
mod kube_access;
pub(crate) mod tls;

pub mod prelude {
    use crate::errors;
    pub type Result<T> = std::result::Result<T, errors::AriadneError>;
}

mod diff;
pub mod graphqlite;
pub mod id_gen;
pub mod kube_client;
pub mod memgraph;
pub mod memgraph_async;
pub mod snapshot;
pub mod state;
pub mod state_resolver;
pub mod types;
