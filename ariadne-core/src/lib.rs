pub mod errors;
#[path = "graph/actor.rs"]
pub(crate) mod graph_actor;
#[path = "graph/backend.rs"]
pub mod graph_backend;
#[path = "graph/schema.rs"]
pub mod graph_schema;
#[path = "kube/access.rs"]
mod kube_access;
pub(crate) mod tls;

pub mod prelude {
    use crate::errors;
    pub type Result<T> = std::result::Result<T, errors::AriadneError>;
}

#[path = "state/diff.rs"]
mod diff;
#[path = "state/id_gen.rs"]
pub mod id_gen;
#[path = "backends/in_memory.rs"]
pub mod in_memory;
#[path = "kube/client.rs"]
pub mod kube_client;
#[path = "backends/memgraph.rs"]
pub mod memgraph;
#[path = "backends/memgraph_async.rs"]
pub mod memgraph_async;
#[path = "kube/snapshot.rs"]
pub mod snapshot;
#[path = "state/mod.rs"]
pub mod state;
#[path = "kube/state_resolver.rs"]
pub mod state_resolver;
pub mod types;
