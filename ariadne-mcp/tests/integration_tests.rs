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
use ariadne_core::state_resolver::ClusterStateResolver;
use ariadne_core::types::{Cluster, ObjectIdentifier};
use ariadne_mcp::GraphSchemaFormat;
use ariadne_mcp::health::{
    GraphHealthCompactResponse, GraphHealthDetail, GraphHealthResponse, GraphScope, GraphScopeKind,
    SNAPSHOT_MANIFEST_FILE, SnapshotManifest, SyncHealth,
};
use ariadne_mcp::read_snapshot_manifest;
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
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Debug, Deserialize)]
struct GraphQueryResponse {
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
    row_count: usize,
    truncated: bool,
    duration_ms: u64,
}

#[derive(Debug, Deserialize)]
struct SchemaProperty {
    name: String,
    #[serde(rename = "type")]
    property_type: String,
}

#[derive(Debug, Deserialize)]
struct NodeLabelSchema {
    label: String,
    properties: Vec<SchemaProperty>,
}

#[derive(Debug, Deserialize)]
struct GraphSchemaResponse {
    format: GraphSchemaFormat,
    schema_version: String,
    server_version: String,
    node_labels: Vec<NodeLabelSchema>,
}

#[derive(Debug, Deserialize)]
struct GraphSchemaCompactResponse {
    format: GraphSchemaFormat,
    schema_version: String,
    schema_text: String,
    example_patterns: Vec<String>,
}

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

    write_json_to_dir(
        dir,
        SNAPSHOT_MANIFEST_FILE,
        &SnapshotManifest {
            captured_at: "2026-03-28T00:00:00Z".to_string(),
            scope: GraphScope::namespace("team-a"),
        },
    )?;
    Ok(())
}

async fn start_snapshot_server(snapshot_dir: &Path) -> TestResult<RunningServer> {
    let manifest = read_snapshot_manifest(snapshot_dir)?
        .ok_or_else(|| "snapshot manifest should exist".to_string())?;
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

    let router = ariadne_mcp::routes::create_route(
        "test-cluster".to_string(),
        "in-memory".to_string(),
        "snapshot".to_string(),
        Some(manifest.scope.clone()),
        Some(manifest.captured_at.clone()),
        cluster_state,
        graph,
        Arc::new(AtomicBool::new(true)),
        Arc::new(Mutex::new(SyncHealth::default())),
        Arc::new(Mutex::new(None)),
        resolver.degraded_resource_kinds_handle(),
        CancellationToken::new(),
    )
    .await?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let shutdown = CancellationToken::new();
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

fn parse_structured<T: DeserializeOwned>(result: rmcp::model::CallToolResult) -> TestResult<T> {
    let structured = result
        .structured_content
        .ok_or_else(|| "missing structured tool response".to_string())?;
    Ok(serde_json::from_value(structured)?)
}

#[tokio::test]
async fn mcp_snapshot_tools_round_trip_over_http() -> TestResult<()> {
    let snapshot_dir = TempDir::new("ariadne_mcp_snapshot")?;
    write_snapshot_fixture(snapshot_dir.path())?;
    let server = start_snapshot_server(snapshot_dir.path()).await?;

    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(server.url.clone()),
    );
    let client = ().serve(transport).await?;

    let mut tool_names = client
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

    let schema_payload: GraphSchemaCompactResponse = parse_structured(
        client
            .call_tool(CallToolRequestParams::new("graph_schema"))
            .await?,
    )?;
    assert_eq!(schema_payload.format, GraphSchemaFormat::Compact);
    assert!(schema_payload.schema_version.starts_with("sha256:"));
    assert!(schema_payload.schema_text.starts_with("# Nodes\n"));
    assert!(schema_payload.schema_text.contains("\n## Logical Nodes\n"));
    assert!(
        schema_payload
            .schema_text
            .contains("\n## K8s Native Nodes\n")
    );
    assert!(schema_payload.schema_text.contains("\n# Edges\n"));
    assert!(schema_payload.schema_text.contains("Namespace("));
    assert!(
        schema_payload
            .schema_text
            .contains("spec:io.k8s.api.core.v1.Container")
    );
    assert!(
        schema_payload
            .example_patterns
            .iter()
            .all(|pattern| pattern.contains("LIMIT"))
    );

    let structured_schema_payload: GraphSchemaResponse = parse_structured(
        client
            .call_tool(
                CallToolRequestParams::new("graph_schema").with_arguments(
                    json!({"format": "structured"})
                        .as_object()
                        .expect("graph_schema arguments should be an object")
                        .clone(),
                ),
            )
            .await?,
    )?;
    assert_eq!(
        structured_schema_payload.format,
        GraphSchemaFormat::Structured
    );
    assert!(
        structured_schema_payload
            .schema_version
            .starts_with("sha256:")
    );
    assert_eq!(
        structured_schema_payload.server_version,
        ariadne_mcp::APP_VERSION
    );
    let namespace = structured_schema_payload
        .node_labels
        .iter()
        .find(|label| label.label == "Namespace")
        .expect("Namespace should be present in graph_schema");
    assert!(
        namespace
            .properties
            .iter()
            .any(|property| property.name == "metadata"
                && property.property_type == "io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta")
    );
    let pod = structured_schema_payload
        .node_labels
        .iter()
        .find(|label| label.label == "Pod")
        .expect("Pod should be present in graph_schema");
    assert!(pod.properties.iter().any(|property| property.name == "spec"
        && property.property_type == "io.k8s.api.core.v1.PodSpec"));

    let query_payload: GraphQueryResponse = parse_structured(
        client
            .call_tool(
                CallToolRequestParams::new("graph_query").with_arguments(
                    json!({
                        "query": "MATCH (n:Namespace) RETURN n['metadata']['name'] AS name LIMIT 5",
                        "limit": 1
                    })
                    .as_object()
                    .expect("graph_query arguments should be an object")
                    .clone(),
                ),
            )
            .await?,
    )?;
    assert_eq!(query_payload.columns, vec!["name"]);
    assert_eq!(query_payload.rows, vec![vec![json!("team-a")]]);
    assert_eq!(query_payload.row_count, 1);
    assert!(!query_payload.truncated);
    assert!(query_payload.duration_ms < 5_000);

    let health_payload: GraphHealthCompactResponse = parse_structured(
        client
            .call_tool(CallToolRequestParams::new("graph_health"))
            .await?,
    )?;
    assert_eq!(health_payload.detail, GraphHealthDetail::Compact);
    assert_eq!(health_payload.cluster, "test-cluster");
    assert_eq!(health_payload.mode, "snapshot");
    assert!(health_payload.ready);
    assert_eq!(
        health_payload.data_as_of.as_deref(),
        Some("2026-03-28T00:00:00Z")
    );
    assert_eq!(health_payload.node_count, 2);
    assert_eq!(health_payload.edge_count, 1);
    assert!(health_payload.sync_lag_ms.is_none());
    let scope = health_payload
        .scope
        .expect("snapshot scope should be present");
    assert_eq!(scope.kind, GraphScopeKind::Namespace);
    assert_eq!(scope.namespace.as_deref(), Some("team-a"));
    assert!(health_payload.coverage.degraded_resource_kinds.is_empty());
    assert!(!health_payload.observed_at.is_empty());

    let full_health_payload: GraphHealthResponse = parse_structured(
        client
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
    assert_eq!(full_health_payload.detail, GraphHealthDetail::Full);
    assert_eq!(full_health_payload.backend, "in-memory");
    assert_eq!(full_health_payload.version, ariadne_mcp::APP_VERSION);
    assert!(full_health_payload.sync.is_none());
    assert!(full_health_payload.rebuild.is_none());

    let _ = client.cancel().await;
    server.stop().await;
    Ok(())
}
