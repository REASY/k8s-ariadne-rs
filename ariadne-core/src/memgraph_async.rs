use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use tokio::sync::oneshot;

use crate::memgraph;
use crate::memgraph::Memgraph;
use crate::prelude::*;
use crate::state::ClusterState;
use rsmgclient::ConnectParams;
use serde_json::Value;
use tracing::error;

/// Commands sent to the worker thread that owns the Memgraph connection.
enum Command {
    Create {
        lock: Arc<Mutex<ClusterState>>,
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
                        // If we fail to connect up front, we still need to drain incoming
                        // messages and reply with the same error, so callers aren't left hanging.
                        // However, since no sender is available to create requests yet, this
                        // scenario mainly affects construction time; best we can do is exit.
                        // Returning here drops rx and causes senders to fail immediately.
                        error!("MemgraphAsync: failed to connect: {err:?}");
                        return;
                    }
                };

                // Process requests until Shutdown is received or all senders are dropped
                while let Ok(cmd) = rx.recv() {
                    match cmd {
                        Command::Create { lock, resp } => {
                            let state = lock.lock().expect("Failed to lock cluster state");
                            let res = mg.create(&state);
                            let _ = resp.send(res);
                        }
                        Command::ExecuteQuery { query, resp } => {
                            let res = mg.execute_query(&query);
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

        Ok(Self { tx })
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
