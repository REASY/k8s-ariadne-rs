pub mod errors;

pub mod prelude {
    use crate::errors;
    pub type Result<T> = std::result::Result<T, errors::AriadneError>;
}

pub mod id_gen;
pub mod kube_client;
pub mod memgraph;
pub mod memgraph_async;
pub mod state;
pub mod state_resolver;
pub mod types;
