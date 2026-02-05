use std::sync::mpsc::{self, Sender};
use std::thread;
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use crate::prelude::*;
use crate::state::{ClusterStateDiff, GraphEdge, SharedClusterState};
use crate::types::GenericObject;
use serde_json::Value;
use tracing::{error, info};

pub(crate) trait GraphConnection {
    fn create_from_snapshot(&mut self, nodes: &[GenericObject], edges: &[GraphEdge]) -> Result<()>;
    fn update_from_diff(&mut self, diff: &ClusterStateDiff) -> Result<()>;
    fn execute_query(&mut self, query: &str) -> Result<Vec<Value>>;
}

enum Command {
    Create {
        lock: SharedClusterState,
        resp: oneshot::Sender<Result<()>>,
    },
    Update {
        diff: ClusterStateDiff,
        resp: oneshot::Sender<Result<()>>,
    },
    ExecuteQuery {
        query: String,
        resp: oneshot::Sender<Result<Vec<Value>>>,
    },
    Shutdown {
        resp: oneshot::Sender<()>,
    },
}

#[derive(Debug)]
pub(crate) struct GraphActor {
    tx: Sender<Command>,
    label: &'static str,
}

impl Clone for GraphActor {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            label: self.label,
        }
    }
}

impl GraphActor {
    pub(crate) fn spawn<C, F>(label: &'static str, connect_fn: F) -> Result<Self>
    where
        C: GraphConnection,
        F: FnOnce() -> Result<C> + Send + 'static,
    {
        let (tx, rx) = mpsc::channel::<Command>();
        let (ready_tx, ready_rx) = mpsc::channel::<Result<()>>();

        thread::Builder::new()
            .name(format!("{label}-actor"))
            .spawn(move || {
                let mut connection = match connect_fn() {
                    Ok(connection) => connection,
                    Err(err) => {
                        error!("{label}: failed to connect: {err:?}");
                        let _ = ready_tx.send(Err(err));
                        return;
                    }
                };
                let _ = ready_tx.send(Ok(()));

                while let Ok(cmd) = rx.recv() {
                    match cmd {
                        Command::Create { lock, resp } => {
                            info!("{label}: create");
                            let (nodes, edges) = {
                                let state = lock.lock().expect("Failed to lock cluster state");
                                let nodes = state.get_nodes().cloned().collect::<Vec<_>>();
                                let edges = state.get_edges().collect::<Vec<_>>();
                                (nodes, edges)
                            };
                            let res = connection.create_from_snapshot(&nodes, &edges);
                            if let Err(err) = &res {
                                error!("{label}: create failed: {err}");
                            }
                            let _ = resp.send(res);
                        }
                        Command::Update { diff, resp } => {
                            info!(
                                "{label}: update (+{} nodes, -{} nodes, ~{} nodes, +{} edges, -{} edges)",
                                diff.added_nodes.len(),
                                diff.removed_nodes.len(),
                                diff.modified_nodes.len(),
                                diff.added_edges.len(),
                                diff.removed_edges.len()
                            );
                            let res = connection.update_from_diff(&diff);
                            if let Err(err) = &res {
                                error!("{label}: update failed: {err}");
                            }
                            let _ = resp.send(res);
                        }
                        Command::ExecuteQuery { query, resp } => {
                            let started = Instant::now();
                            let res = connection.execute_query(&query);
                            let elapsed_ms = started.elapsed().as_millis();
                            info!("{label}: execute_query ({elapsed_ms} ms): {query}");
                            if let Err(err) = &res {
                                error!("{label}: execute_query failed: {err}");
                            }
                            let _ = resp.send(res);
                        }
                        Command::Shutdown { resp } => {
                            let _ = resp.send(());
                            break;
                        }
                    }
                }
            })
            .map_err(|e| std::io::Error::other(format!("Failed to spawn {label} actor: {e}")))?;

        match ready_rx.recv() {
            Ok(Ok(())) => Ok(Self { tx, label }),
            Ok(Err(err)) => Err(err),
            Err(err) => Err(std::io::Error::other(format!(
                "{label} actor failed to signal readiness: {err}"
            ))
            .into()),
        }
    }

    pub(crate) async fn create(&self, cluster_state: SharedClusterState) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(Command::Create {
                lock: cluster_state,
                resp: resp_tx,
            })
            .map_err(|e| {
                std::io::Error::other(format!(
                    "{label} actor is not available: {e}",
                    label = self.label
                ))
            })?;
        resp_rx.await.map_err(|e| {
            std::io::Error::other(format!(
                "{label} actor response dropped: {e}",
                label = self.label
            ))
        })?
    }

    pub(crate) async fn update(&self, diff: ClusterStateDiff) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(Command::Update {
                diff,
                resp: resp_tx,
            })
            .map_err(|e| {
                std::io::Error::other(format!(
                    "{label} actor is not available: {e}",
                    label = self.label
                ))
            })?;
        resp_rx.await.map_err(|e| {
            std::io::Error::other(format!(
                "{label} actor response dropped: {e}",
                label = self.label
            ))
        })?
    }

    pub(crate) async fn execute_query(&self, query: impl Into<String>) -> Result<Vec<Value>> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(Command::ExecuteQuery {
                query: query.into(),
                resp: resp_tx,
            })
            .map_err(|e| {
                std::io::Error::other(format!(
                    "{label} actor is not available: {e}",
                    label = self.label
                ))
            })?;
        resp_rx.await.map_err(|e| {
            std::io::Error::other(format!(
                "{label} actor response dropped: {e}",
                label = self.label
            ))
        })?
    }

    pub(crate) async fn shutdown(&self) {
        let (resp_tx, resp_rx) = oneshot::channel();
        if self.tx.send(Command::Shutdown { resp: resp_tx }).is_ok() {
            let _ = tokio::time::timeout(Duration::from_secs(5), resp_rx).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{ClusterState, GraphEdge};
    use crate::types::{Cluster, Edge, GenericObject, ObjectIdentifier, ResourceType};
    use k8s_openapi::apimachinery::pkg::version::Info;
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct TestState {
        calls: Vec<String>,
        create_nodes: usize,
        create_edges: usize,
        update_added: usize,
        update_removed: usize,
        update_modified: usize,
        update_edges_added: usize,
        update_edges_removed: usize,
        last_query: Option<String>,
    }

    enum FailMode {
        None,
        Create,
        Update,
        Query,
    }

    struct TestConnection {
        state: Arc<Mutex<TestState>>,
        fail: FailMode,
    }

    impl TestConnection {
        fn new(state: Arc<Mutex<TestState>>, fail: FailMode) -> Self {
            Self { state, fail }
        }
    }

    impl GraphConnection for TestConnection {
        fn create_from_snapshot(
            &mut self,
            nodes: &[GenericObject],
            edges: &[GraphEdge],
        ) -> Result<()> {
            if matches!(self.fail, FailMode::Create) {
                return Err(std::io::Error::other("create failed").into());
            }
            let mut state = self.state.lock().unwrap();
            state.calls.push("create".to_string());
            state.create_nodes = nodes.len();
            state.create_edges = edges.len();
            Ok(())
        }

        fn update_from_diff(&mut self, diff: &ClusterStateDiff) -> Result<()> {
            if matches!(self.fail, FailMode::Update) {
                return Err(std::io::Error::other("update failed").into());
            }
            let mut state = self.state.lock().unwrap();
            state.calls.push("update".to_string());
            state.update_added = diff.added_nodes.len();
            state.update_removed = diff.removed_nodes.len();
            state.update_modified = diff.modified_nodes.len();
            state.update_edges_added = diff.added_edges.len();
            state.update_edges_removed = diff.removed_edges.len();
            Ok(())
        }

        fn execute_query(&mut self, query: &str) -> Result<Vec<Value>> {
            if matches!(self.fail, FailMode::Query) {
                return Err(std::io::Error::other("query failed").into());
            }
            let mut state = self.state.lock().unwrap();
            state.calls.push("query".to_string());
            state.last_query = Some(query.to_string());
            Ok(vec![json!({ "ok": true })])
        }
    }

    fn build_cluster(uid: &str, name: &str) -> Cluster {
        let id = ObjectIdentifier {
            uid: uid.to_string(),
            name: name.to_string(),
            namespace: None,
            resource_version: None,
        };
        Cluster::new(id, "https://example.test", Info::default())
    }

    fn build_object(uid: &str, name: &str, resource_type: ResourceType) -> GenericObject {
        GenericObject {
            id: ObjectIdentifier {
                uid: uid.to_string(),
                name: name.to_string(),
                namespace: None,
                resource_version: None,
            },
            resource_type,
            attributes: None,
        }
    }

    #[tokio::test]
    async fn actor_happy_path() {
        let state = Arc::new(Mutex::new(TestState::default()));
        let state_clone = state.clone();
        let actor = GraphActor::spawn("test", move || {
            Ok(TestConnection::new(state_clone, FailMode::None))
        })
        .unwrap();

        let cluster = build_cluster("cluster-uid", "demo");
        let mut cluster_state = ClusterState::new(cluster);
        let cluster_obj = build_object("cluster-uid", "demo", ResourceType::Cluster);
        let ns_obj = build_object("ns-uid", "default", ResourceType::Namespace);
        cluster_state.add_node(cluster_obj);
        cluster_state.add_node(ns_obj);
        cluster_state.add_edge(
            "ns-uid",
            ResourceType::Namespace,
            "cluster-uid",
            ResourceType::Cluster,
            Edge::PartOf,
        );

        let shared_state = Arc::new(Mutex::new(cluster_state));
        actor.create(shared_state).await.unwrap();

        let mut diff = ClusterStateDiff::default();
        diff.added_nodes = vec![build_object("pod-1", "pod", ResourceType::Pod)];
        diff.removed_edges = vec![GraphEdge {
            source: "a".to_string(),
            source_type: ResourceType::Pod,
            target: "b".to_string(),
            target_type: ResourceType::Node,
            edge_type: Edge::RunsOn,
        }];
        actor.update(diff).await.unwrap();

        let results = actor.execute_query("MATCH (n) RETURN n").await.unwrap();
        assert_eq!(results, vec![json!({ "ok": true })]);

        actor.shutdown().await;

        let recorded = state.lock().unwrap();
        assert_eq!(recorded.calls, vec!["create", "update", "query"]);
        assert_eq!(recorded.create_nodes, 2);
        assert_eq!(recorded.create_edges, 1);
        assert_eq!(recorded.update_added, 1);
        assert_eq!(recorded.update_edges_removed, 1);
        assert_eq!(recorded.last_query.as_deref(), Some("MATCH (n) RETURN n"));
    }

    #[tokio::test]
    async fn actor_propagates_create_errors() {
        let state = Arc::new(Mutex::new(TestState::default()));
        let state_clone = state.clone();
        let actor = GraphActor::spawn("test", move || {
            Ok(TestConnection::new(state_clone, FailMode::Create))
        })
        .unwrap();

        let cluster = build_cluster("cluster-uid", "demo");
        let cluster_state = ClusterState::new(cluster);
        let shared_state = Arc::new(Mutex::new(cluster_state));
        assert!(actor.create(shared_state).await.is_err());
    }

    #[tokio::test]
    async fn actor_propagates_update_errors() {
        let state = Arc::new(Mutex::new(TestState::default()));
        let state_clone = state.clone();
        let actor = GraphActor::spawn("test", move || {
            Ok(TestConnection::new(state_clone, FailMode::Update))
        })
        .unwrap();

        let diff = ClusterStateDiff::default();
        assert!(actor.update(diff).await.is_err());
    }

    #[tokio::test]
    async fn actor_propagates_query_errors() {
        let state = Arc::new(Mutex::new(TestState::default()));
        let state_clone = state.clone();
        let actor = GraphActor::spawn("test", move || {
            Ok(TestConnection::new(state_clone, FailMode::Query))
        })
        .unwrap();

        assert!(actor.execute_query("MATCH (n)").await.is_err());
    }

    #[tokio::test]
    async fn actor_rejects_after_shutdown() {
        let state = Arc::new(Mutex::new(TestState::default()));
        let state_clone = state.clone();
        let actor = GraphActor::spawn("test", move || {
            Ok(TestConnection::new(state_clone, FailMode::None))
        })
        .unwrap();
        actor.shutdown().await;
        assert!(actor.execute_query("MATCH (n)").await.is_err());
    }

    #[test]
    fn actor_reports_connect_failures() {
        let actor = GraphActor::spawn::<TestConnection, _>("test", move || {
            Err(std::io::Error::other("connect failed").into())
        });
        assert!(actor.is_err());
    }
}
