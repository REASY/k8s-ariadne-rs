use ariadne_core::graph_backend::GraphBackend;
use ariadne_core::in_memory::InMemoryBackend;
use ariadne_core::kube_client::SnapshotKubeClient;
use ariadne_core::snapshot::{
    SNAPSHOT_CLUSTER_FILE, SNAPSHOT_CONFIG_MAPS_FILE, SNAPSHOT_DAEMON_SETS_FILE,
    SNAPSHOT_DEPLOYMENTS_FILE, SNAPSHOT_ENDPOINT_SLICES_FILE, SNAPSHOT_EVENTS_FILE,
    SNAPSHOT_INGRESSES_FILE, SNAPSHOT_JOBS_FILE, SNAPSHOT_NAMESPACES_FILE,
    SNAPSHOT_NETWORK_POLICIES_FILE, SNAPSHOT_NODES_FILE, SNAPSHOT_PERSISTENT_VOLUME_CLAIMS_FILE,
    SNAPSHOT_PERSISTENT_VOLUMES_FILE, SNAPSHOT_PODS_FILE, SNAPSHOT_REPLICA_SETS_FILE,
    SNAPSHOT_SERVICE_ACCOUNTS_FILE, SNAPSHOT_SERVICES_FILE, SNAPSHOT_STATEFUL_SETS_FILE,
    SNAPSHOT_STORAGE_CLASSES_FILE, write_json_to_dir, write_list_to_dir,
};
use ariadne_core::state_resolver::{
    ClusterStateResolver, DEFAULT_SOURCE_SYNC_POLL_INTERVAL_SECONDS, SOURCE_SYNC_POLL_INTERVAL_ENV,
    configured_source_sync_poll_interval,
};
use ariadne_core::types::{Cluster, ObjectIdentifier};
use ariadne_mcp::health::{
    GraphHealthCompactResponse, GraphHealthDetail, GraphHealthResponse, GraphScope, GraphScopeKind,
    SNAPSHOT_MANIFEST_FILE, SnapshotManifest, SyncHealth,
};
use ariadne_mcp::{read_snapshot_manifest, write_snapshot_manifest};
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{
    ConfigMap, Namespace, Node, PersistentVolume, PersistentVolumeClaim, Pod, Service,
    ServiceAccount,
};
use k8s_openapi::api::discovery::v1::EndpointSlice;
use k8s_openapi::api::events::v1::Event;
use k8s_openapi::api::networking::v1::{Ingress, NetworkPolicy};
use k8s_openapi::api::storage::v1::StorageClass;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use k8s_openapi::apimachinery::pkg::version::Info;
use rmcp::ServiceExt;
use rmcp::model::CallToolRequestParams;
use rmcp::transport::{
    StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
};
use serde::de::DeserializeOwned;
use serde_json::json;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(prefix: &str) -> TestResult<Self> {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        path.push(format!("{prefix}_{}_{}", std::process::id(), nanos));
        fs::create_dir_all(&path)?;
        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

struct RunningServer {
    url: String,
    shutdown: CancellationToken,
    handle: JoinHandle<()>,
}

impl RunningServer {
    async fn stop(self) {
        self.shutdown.cancel();
        let _ = self.handle.await;
    }
}

fn test_cluster(name: &str) -> Cluster {
    Cluster::new(
        ObjectIdentifier {
            uid: format!("Cluster:{name}"),
            name: name.to_string(),
            namespace: None,
            resource_version: None,
        },
        "https://example.test",
        Info {
            major: "1".to_string(),
            minor: "32".to_string(),
            ..Default::default()
        },
    )
}

fn test_namespace(name: &str) -> Arc<Namespace> {
    Arc::new(Namespace {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            uid: Some(format!("namespace:{name}")),
            ..Default::default()
        },
        ..Default::default()
    })
}

fn write_snapshot_fixture(dir: &Path) -> TestResult<()> {
    write_json_to_dir(dir, SNAPSHOT_CLUSTER_FILE, &test_cluster("test-cluster"))?;

    let namespaces = vec![test_namespace("team-a")];
    write_list_to_dir(dir, SNAPSHOT_NAMESPACES_FILE, &namespaces)?;

    let pods: Vec<Arc<Pod>> = Vec::new();
    let deployments: Vec<Arc<Deployment>> = Vec::new();
    let stateful_sets: Vec<Arc<StatefulSet>> = Vec::new();
    let replica_sets: Vec<Arc<ReplicaSet>> = Vec::new();
    let daemon_sets: Vec<Arc<DaemonSet>> = Vec::new();
    let jobs: Vec<Arc<Job>> = Vec::new();
    let ingresses: Vec<Arc<Ingress>> = Vec::new();
    let services: Vec<Arc<Service>> = Vec::new();
    let endpoint_slices: Vec<Arc<EndpointSlice>> = Vec::new();
    let network_policies: Vec<Arc<NetworkPolicy>> = Vec::new();
    let config_maps: Vec<Arc<ConfigMap>> = Vec::new();
    let storage_classes: Vec<Arc<StorageClass>> = Vec::new();
    let persistent_volumes: Vec<Arc<PersistentVolume>> = Vec::new();
    let persistent_volume_claims: Vec<Arc<PersistentVolumeClaim>> = Vec::new();
    let nodes: Vec<Arc<Node>> = Vec::new();
    let service_accounts: Vec<Arc<ServiceAccount>> = Vec::new();
    let events: Vec<Arc<Event>> = Vec::new();

    write_list_to_dir(dir, SNAPSHOT_PODS_FILE, &pods)?;
    write_list_to_dir(dir, SNAPSHOT_DEPLOYMENTS_FILE, &deployments)?;
    write_list_to_dir(dir, SNAPSHOT_STATEFUL_SETS_FILE, &stateful_sets)?;
    write_list_to_dir(dir, SNAPSHOT_REPLICA_SETS_FILE, &replica_sets)?;
    write_list_to_dir(dir, SNAPSHOT_DAEMON_SETS_FILE, &daemon_sets)?;
    write_list_to_dir(dir, SNAPSHOT_JOBS_FILE, &jobs)?;
    write_list_to_dir(dir, SNAPSHOT_INGRESSES_FILE, &ingresses)?;
    write_list_to_dir(dir, SNAPSHOT_SERVICES_FILE, &services)?;
    write_list_to_dir(dir, SNAPSHOT_ENDPOINT_SLICES_FILE, &endpoint_slices)?;
    write_list_to_dir(dir, SNAPSHOT_NETWORK_POLICIES_FILE, &network_policies)?;
    write_list_to_dir(dir, SNAPSHOT_CONFIG_MAPS_FILE, &config_maps)?;
    write_list_to_dir(dir, SNAPSHOT_STORAGE_CLASSES_FILE, &storage_classes)?;
    write_list_to_dir(dir, SNAPSHOT_PERSISTENT_VOLUMES_FILE, &persistent_volumes)?;
    write_list_to_dir(
        dir,
        SNAPSHOT_PERSISTENT_VOLUME_CLAIMS_FILE,
        &persistent_volume_claims,
    )?;
    write_list_to_dir(dir, SNAPSHOT_NODES_FILE, &nodes)?;
    write_list_to_dir(dir, SNAPSHOT_SERVICE_ACCOUNTS_FILE, &service_accounts)?;
    write_list_to_dir(dir, SNAPSHOT_EVENTS_FILE, &events)?;
    Ok(())
}

fn parse_structured<T: DeserializeOwned>(result: rmcp::model::CallToolResult) -> TestResult<T> {
    let structured = result
        .structured_content
        .ok_or_else(|| "missing structured tool response".to_string())?;
    Ok(serde_json::from_value(structured)?)
}

async fn start_server(
    mode: &str,
    scope: Option<GraphScope>,
    snapshot_captured_at: Option<String>,
    source_sync: SyncHealth,
    snapshot_dir: &Path,
) -> TestResult<RunningServer> {
    let resolver = Arc::new(
        ClusterStateResolver::new_with_kube_client(
            "test-cluster".to_string(),
            Box::new(SnapshotKubeClient::from_dir(snapshot_dir)?),
        )
        .await?,
    );
    let cluster_state = resolver.resolve().await?;

    let graph: Arc<dyn GraphBackend> = Arc::new(InMemoryBackend::new());
    graph.create(cluster_state.clone()).await?;

    let shutdown = CancellationToken::new();
    let router = ariadne_mcp::routes::create_route(
        "test-cluster".to_string(),
        "in-memory".to_string(),
        mode.to_string(),
        scope,
        snapshot_captured_at,
        cluster_state,
        graph,
        Arc::new(AtomicBool::new(true)),
        Arc::new(Mutex::new(source_sync)),
        Arc::new(Mutex::new(None)),
        resolver.degraded_resource_kinds_handle(),
        shutdown.clone(),
    )
    .await?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let handle = tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            let _ = axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    shutdown.cancelled_owned().await;
                })
                .await;
        }
    });

    Ok(RunningServer {
        url: format!("http://{addr}/mcp"),
        shutdown,
        handle,
    })
}

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[test]
fn snapshot_manifest_read_semantics() -> TestResult<()> {
    let dir = TempDir::new("ariadne_mcp_manifest_read_semantics")?;
    write_snapshot_fixture(dir.path())?;
    assert!(read_snapshot_manifest(dir.path())?.is_none());

    let manifest = SnapshotManifest {
        captured_at: "2026-03-28T00:00:00Z".to_string(),
        scope: GraphScope::namespace("team-a"),
    };
    write_snapshot_manifest(dir.path(), &manifest)?;

    let loaded = read_snapshot_manifest(dir.path())?;
    assert_eq!(loaded, Some(manifest.clone()));

    fs::write(dir.path().join(SNAPSHOT_MANIFEST_FILE), "{invalid-json")?;
    assert!(read_snapshot_manifest(dir.path()).is_err());
    Ok(())
}

#[test]
fn source_sync_poll_interval_env_parsing() {
    let _guard = env_lock().lock().expect("env lock poisoned");
    let original = std::env::var(SOURCE_SYNC_POLL_INTERVAL_ENV).ok();

    unsafe {
        std::env::remove_var(SOURCE_SYNC_POLL_INTERVAL_ENV);
    }
    assert_eq!(
        configured_source_sync_poll_interval(),
        Duration::from_secs(DEFAULT_SOURCE_SYNC_POLL_INTERVAL_SECONDS)
    );

    unsafe {
        std::env::set_var(SOURCE_SYNC_POLL_INTERVAL_ENV, "7");
    }
    assert_eq!(
        configured_source_sync_poll_interval(),
        Duration::from_secs(7)
    );

    unsafe {
        std::env::set_var(SOURCE_SYNC_POLL_INTERVAL_ENV, "0");
    }
    assert_eq!(
        configured_source_sync_poll_interval(),
        Duration::from_secs(DEFAULT_SOURCE_SYNC_POLL_INTERVAL_SECONDS)
    );

    unsafe {
        std::env::set_var(SOURCE_SYNC_POLL_INTERVAL_ENV, "not-a-number");
    }
    assert_eq!(
        configured_source_sync_poll_interval(),
        Duration::from_secs(DEFAULT_SOURCE_SYNC_POLL_INTERVAL_SECONDS)
    );

    if let Some(original) = original {
        unsafe {
            std::env::set_var(SOURCE_SYNC_POLL_INTERVAL_ENV, original);
        }
    } else {
        unsafe {
            std::env::remove_var(SOURCE_SYNC_POLL_INTERVAL_ENV);
        }
    }
}

#[tokio::test]
async fn mcp_live_vs_snapshot_modes_over_http() -> TestResult<()> {
    let snapshot_dir = TempDir::new("ariadne_mcp_lifecycle_modes")?;
    write_snapshot_fixture(snapshot_dir.path())?;
    write_snapshot_manifest(
        snapshot_dir.path(),
        &SnapshotManifest {
            captured_at: "2026-03-28T00:00:00Z".to_string(),
            scope: GraphScope::namespace("team-a"),
        },
    )?;
    let manifest = read_snapshot_manifest(snapshot_dir.path())?
        .ok_or_else(|| "snapshot manifest should exist".to_string())?;

    let snapshot_server = start_server(
        "snapshot",
        Some(manifest.scope.clone()),
        Some(manifest.captured_at.clone()),
        SyncHealth::default(),
        snapshot_dir.path(),
    )
    .await?;
    let snapshot_transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(snapshot_server.url.clone()),
    );
    let snapshot_client = ().serve(snapshot_transport).await?;

    let mut tool_names = snapshot_client
        .list_all_tools()
        .await?
        .into_iter()
        .map(|tool| tool.name.to_string())
        .collect::<Vec<_>>();
    tool_names.sort();
    assert_eq!(
        tool_names,
        vec![
            "graph_health".to_string(),
            "graph_query".to_string(),
            "graph_schema".to_string(),
        ]
    );

    let snapshot_health: GraphHealthCompactResponse = parse_structured(
        snapshot_client
            .call_tool(CallToolRequestParams::new("graph_health"))
            .await?,
    )?;
    assert_eq!(snapshot_health.detail, GraphHealthDetail::Compact);
    assert_eq!(snapshot_health.mode, "snapshot");
    assert_eq!(
        snapshot_health.data_as_of.as_deref(),
        Some("2026-03-28T00:00:00Z")
    );
    assert!(snapshot_health.sync_lag_ms.is_none());
    let scope = snapshot_health
        .scope
        .expect("snapshot mode should preserve snapshot scope");
    assert_eq!(scope.kind, GraphScopeKind::Namespace);
    assert_eq!(scope.namespace.as_deref(), Some("team-a"));

    let _ = snapshot_client.cancel().await;
    snapshot_server.stop().await;

    let mut live_sync = SyncHealth::bootstrap(SystemTime::now() - Duration::from_secs(1));
    live_sync.poll_interval_seconds = 7;
    let live_server = start_server(
        "live",
        Some(GraphScope::cluster()),
        None,
        live_sync,
        snapshot_dir.path(),
    )
    .await?;
    let live_transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(live_server.url.clone()),
    );
    let live_client = ().serve(live_transport).await?;

    let live_health_compact: GraphHealthCompactResponse = parse_structured(
        live_client
            .call_tool(CallToolRequestParams::new("graph_health"))
            .await?,
    )?;
    assert_eq!(live_health_compact.mode, "live");
    assert!(live_health_compact.data_as_of.is_some());
    assert!(live_health_compact.sync_lag_ms.is_some());
    let live_scope = live_health_compact
        .scope
        .expect("live mode should expose runtime scope");
    assert_eq!(live_scope.kind, GraphScopeKind::Cluster);
    assert!(live_scope.namespace.is_none());

    let live_health_full: GraphHealthResponse = parse_structured(
        live_client
            .call_tool(
                CallToolRequestParams::new("graph_health").with_arguments(
                    json!({"detail": "full"})
                        .as_object()
                        .expect("graph_health arguments should be an object")
                        .clone(),
                ),
            )
            .await?,
    )?;
    assert_eq!(live_health_full.detail, GraphHealthDetail::Full);
    assert_eq!(live_health_full.mode, "live");
    assert_eq!(
        live_health_full
            .sync
            .as_ref()
            .expect("live mode should include sync")
            .poll_interval_seconds,
        7
    );

    let _ = live_client.cancel().await;
    live_server.stop().await;
    Ok(())
}

#[tokio::test]
async fn graceful_shutdown_cancels_mcp_transport() -> TestResult<()> {
    let snapshot_dir = TempDir::new("ariadne_mcp_shutdown")?;
    write_snapshot_fixture(snapshot_dir.path())?;

    let server = start_server(
        "snapshot",
        Some(GraphScope::namespace("team-a")),
        Some("2026-03-28T00:00:00Z".to_string()),
        SyncHealth::default(),
        snapshot_dir.path(),
    )
    .await?;
    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(server.url.clone()),
    );
    let client = ().serve(transport).await?;

    let _: GraphHealthCompactResponse = parse_structured(
        client
            .call_tool(CallToolRequestParams::new("graph_health"))
            .await?,
    )?;
    server.stop().await;

    let after_shutdown = timeout(
        Duration::from_secs(3),
        client.call_tool(CallToolRequestParams::new("graph_health")),
    )
    .await;
    assert!(
        after_shutdown.is_ok(),
        "request timed out instead of failing after shutdown"
    );
    assert!(
        after_shutdown.expect("timeout already checked").is_err(),
        "expected transport error after shutdown"
    );

    let _ = client.cancel().await;
    Ok(())
}
