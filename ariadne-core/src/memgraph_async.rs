use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use tokio::sync::oneshot;

use crate::memgraph;
use crate::memgraph::Memgraph;
use crate::prelude::*;
use crate::state::{ClusterState, ClusterStateDiff};
use rsmgclient::ConnectParams;
use serde_json::Value;
use tracing::error;
use tracing::info;

/// Commands sent to the worker thread that owns the Memgraph connection.
enum Command {
    Create {
        lock: Arc<Mutex<ClusterState>>,
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

/// Async handle for interacting with Memgraph via message passing.
#[derive(Clone, Debug)]
pub struct MemgraphAsync {
    tx: Sender<Command>,
}

impl MemgraphAsync {
    /// Start the actor by connecting from a URL.
    pub fn try_new_from_url(url: &str) -> Result<Self> {
        // We'll connect inside the worker thread to keep all Memgraph usage confined to it.
        let url = url.to_string();
        Self::spawn_with(move || Memgraph::try_new_from_url(&url))
    }

    /// Start the actor by connecting from ConnectParams.
    pub fn try_new(params: ConnectParams) -> Result<Self> {
        // Extract only Send-safe fields.
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

        // If you need more fields (e.g., username/password/TLS), extract them here as well,
        // ensuring they are Send (Strings, numbers, simple enums), then reassemble below.
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
        let (tx, rx) = mpsc::channel::<Command>();
        let (ready_tx, ready_rx) = mpsc::channel::<Result<()>>();

        // Spawn a dedicated OS thread that owns the Memgraph connection
        // and handles all requests synchronously and exclusively.
        thread::Builder::new()
            .name("memgraph-actor".to_string())
            .spawn(move || {
                // Backoff retries on connect might be useful in certain deployments.
                // Here we connect once; you can extend with retries if desired.
                let mut mg = match connect_fn() {
                    Ok(mg) => mg,
                    Err(err) => {
                        error!("MemgraphAsync: failed to connect: {err:?}");
                        let _ = ready_tx.send(Err(err));
                        return;
                    }
                };
                let _ = ready_tx.send(Ok(()));

                // Process requests until Shutdown is received or all senders are dropped
                while let Ok(cmd) = rx.recv() {
                    match cmd {
                        Command::Create { lock, resp } => {
                            info!("memgraph: create");
                            let (nodes, edges) = {
                                let state = lock.lock().expect("Failed to lock cluster state");
                                let nodes = state.get_nodes().cloned().collect::<Vec<_>>();
                                let edges = state.get_edges().collect::<Vec<_>>();
                                (nodes, edges)
                            };
                            let res = mg.create_from_snapshot(&nodes, &edges);
                            if let Err(err) = &res {
                                error!("memgraph: create failed: {err}");
                            }
                            let _ = resp.send(res);
                        }
                        Command::Update { diff, resp } => {
                            info!(
                                "memgraph: update (+{} nodes, -{} nodes, ~{} nodes, +{} edges, -{} edges)",
                                diff.added_nodes.len(),
                                diff.removed_nodes.len(),
                                diff.modified_nodes.len(),
                                diff.added_edges.len(),
                                diff.removed_edges.len()
                            );
                            let res = mg.update_from_diff(&diff);
                            if let Err(err) = &res {
                                error!("memgraph: update failed: {err}");
                            }
                            let _ = resp.send(res);
                        }
                        Command::ExecuteQuery { query, resp } => {
                            info!("memgraph: execute_query");
                            let res = mg.execute_query(&query);
                            if let Err(err) = &res {
                                error!("memgraph: execute_query failed: {err}");
                            }
                            let _ = resp.send(res);
                        }
                        Command::Shutdown { resp } => {
                            // Drop mg by letting it go out of scope. If you need an explicit close, add it here.
                            let _ = resp.send(());
                            break;
                        }
                    }
                }
            })
            .map_err(|e| {
                memgraph::MemgraphError::ConnectionError(format!(
                    "Failed to spawn memgraph actor thread: {e}"
                ))
            })?;

        match ready_rx.recv() {
            Ok(Ok(())) => Ok(Self { tx }),
            Ok(Err(err)) => Err(err),
            Err(err) => Err(memgraph::MemgraphError::ConnectionError(format!(
                "Memgraph actor failed to signal readiness: {err}"
            ))
            .into()),
        }
    }

    /// Asynchronously create the graph from the provided ClusterState.
    /// Pass an Arc to avoid cloning large states and to ensure Send + 'static across the thread boundary.
    pub async fn create(&self, cluster_state: Arc<Mutex<ClusterState>>) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(Command::Create {
                lock: cluster_state.clone(),
                resp: resp_tx,
            })
            .map_err(|e| {
                memgraph::MemgraphError::ConnectionError(format!(
                    "Memgraph actor is not available: {e}"
                ))
            })?;
        resp_rx.await.map_err(|e| {
            memgraph::MemgraphError::ConnectionError(format!(
                "Memgraph actor response dropped: {e}"
            ))
        })?
    }

    /// Incrementally update the graph using a diff.
    pub async fn update(&self, diff: ClusterStateDiff) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(Command::Update {
                diff,
                resp: resp_tx,
            })
            .map_err(|e| {
                memgraph::MemgraphError::ConnectionError(format!(
                    "Memgraph actor is not available: {e}"
                ))
            })?;
        resp_rx.await.map_err(|e| {
            memgraph::MemgraphError::ConnectionError(format!(
                "Memgraph actor response dropped: {e}"
            ))
        })?
    }

    /// Asynchronously execute a query and return JSON-serializable values.
    pub async fn execute_query(&self, query: impl Into<String>) -> Result<Vec<Value>> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(Command::ExecuteQuery {
                query: query.into(),
                resp: resp_tx,
            })
            .map_err(|e| {
                memgraph::MemgraphError::ConnectionError(format!(
                    "Memgraph actor is not available: {e}"
                ))
            })?;
        resp_rx.await.map_err(|e| {
            memgraph::MemgraphError::ConnectionError(format!(
                "Memgraph actor response dropped: {e}"
            ))
        })?
    }

    /// Gracefully stop the worker thread.
    pub async fn shutdown(&self) {
        let (resp_tx, resp_rx) = oneshot::channel();
        if self.tx.send(Command::Shutdown { resp: resp_tx }).is_ok() {
            // Best-effort: wait a short time for the worker to confirm shutdown.
            // If the worker is blocked or already gone, this will just time out or err.
            let _ = tokio::time::timeout(Duration::from_secs(5), resp_rx).await;
        }
    }
}
