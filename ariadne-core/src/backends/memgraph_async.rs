use crate::graph_actor::{GraphActor, GraphConnection};
use crate::graph_backend::GraphBackend;
use crate::memgraph::Memgraph;
use crate::prelude::*;
use crate::state::{ClusterStateDiff, SharedClusterState};
use rsmgclient::ConnectParams;
use serde_json::Value;

impl GraphConnection for Memgraph {
    fn create_from_snapshot(
        &mut self,
        nodes: &[crate::types::GenericObject],
        edges: &[crate::state::GraphEdge],
    ) -> Result<()> {
        Memgraph::create_from_snapshot(self, nodes, edges)
    }

    fn update_from_diff(&mut self, diff: &ClusterStateDiff) -> Result<()> {
        Memgraph::update_from_diff(self, diff)
    }

    fn execute_query(&mut self, query: &str) -> Result<Vec<Value>> {
        Memgraph::execute_query(self, query)
    }
}

/// Async handle for interacting with Memgraph via message passing.
#[derive(Clone, Debug)]
pub struct MemgraphAsync {
    actor: GraphActor,
}

impl MemgraphAsync {
    /// Start the actor by connecting from a URL.
    pub fn try_new_from_url(url: &str) -> Result<Self> {
        let url = url.to_string();
        Self::spawn_with(move || Memgraph::try_new_from_url(&url))
    }

    /// Start the actor by connecting from ConnectParams.
    pub fn try_new(params: ConnectParams) -> Result<Self> {
        let host = params.host.clone();
        let port = params.port;
        let address = params.address;
        let username = params.username;
        let password = params.password;
        let client_name = params.client_name;
        let sslmode = params.sslmode;
        let sslcert = params.sslcert;
        let sslkey = params.sslkey;
        let lazy = params.lazy;
        let autocommit = params.autocommit;

        Self::spawn_with(move || {
            let rebuilt = ConnectParams {
                host,
                port,
                address,
                username,
                password,
                client_name,
                sslmode,
                sslcert,
                sslkey,
                lazy,
                autocommit,
                ..Default::default()
            };
            Memgraph::try_new(rebuilt)
        })
    }

    fn spawn_with<F>(connect_fn: F) -> Result<Self>
    where
        F: FnOnce() -> Result<Memgraph> + Send + 'static,
    {
        let actor = GraphActor::spawn("memgraph", connect_fn)?;
        Ok(Self { actor })
    }

    pub async fn create(&self, cluster_state: SharedClusterState) -> Result<()> {
        self.actor.create(cluster_state).await
    }

    pub async fn update(&self, diff: ClusterStateDiff) -> Result<()> {
        self.actor.update(diff).await
    }

    pub async fn execute_query(&self, query: impl Into<String>) -> Result<Vec<Value>> {
        self.actor.execute_query(query).await
    }

    pub async fn shutdown(&self) {
        self.actor.shutdown().await;
    }
}

#[async_trait::async_trait]
impl GraphBackend for MemgraphAsync {
    async fn create(&self, cluster_state: SharedClusterState) -> Result<()> {
        MemgraphAsync::create(self, cluster_state).await
    }

    async fn update(&self, diff: ClusterStateDiff) -> Result<()> {
        MemgraphAsync::update(self, diff).await
    }

    async fn execute_query(&self, query: String) -> Result<Vec<Value>> {
        MemgraphAsync::execute_query(self, query).await
    }

    async fn shutdown(&self) {
        MemgraphAsync::shutdown(self).await
    }
}
