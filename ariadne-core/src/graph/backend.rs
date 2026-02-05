use async_trait::async_trait;
use serde_json::Value;

use crate::prelude::Result;
use crate::state::{ClusterStateDiff, SharedClusterState};

#[async_trait]
pub trait GraphBackend: Send + Sync + std::fmt::Debug {
    async fn create(&self, cluster_state: SharedClusterState) -> Result<()>;
    async fn update(&self, diff: ClusterStateDiff) -> Result<()>;
    async fn execute_query(&self, query: String) -> Result<Vec<Value>>;
    async fn shutdown(&self);
}
