use super::*;
use crate::health::GraphScopeKind;
use ariadne_core::query_issue::QueryIssueKind;
use ariadne_core::state::ClusterState;
use ariadne_core::types::{Cluster, ObjectIdentifier};
use k8s_openapi::apimachinery::pkg::version::Info;
use std::time::Duration;

#[derive(Debug, Default)]
struct MockBackend {
    columns: Vec<String>,
    rows: Vec<Value>,
    fail: Option<String>,
}

#[async_trait::async_trait]
impl GraphBackend for MockBackend {
    async fn create(
        &self,
        _cluster_state: SharedClusterState,
    ) -> ariadne_core::prelude::Result<()> {
        Ok(())
    }

    async fn update(
        &self,
        _diff: ariadne_core::state::ClusterStateDiff,
    ) -> ariadne_core::prelude::Result<()> {
        Ok(())
    }

    async fn execute_query(
        &self,
        _query: String,
        _params: Option<HashMap<String, Value>>,
    ) -> ariadne_core::prelude::Result<Vec<Value>> {
        if let Some(message) = &self.fail {
            return Err(std::io::Error::other(message.clone()).into());
        }
        Ok(self.rows.clone())
    }

    async fn execute_query_with_columns(
        &self,
        _query: String,
        _params: Option<HashMap<String, Value>>,
    ) -> ariadne_core::prelude::Result<(Vec<String>, Vec<Value>)> {
        if let Some(message) = &self.fail {
            return Err(std::io::Error::other(message.clone()).into());
        }
        Ok((self.columns.clone(), self.rows.clone()))
    }

    async fn shutdown(&self) {}
}

fn test_cluster() -> Cluster {
    Cluster::new(
        ObjectIdentifier {
            uid: "Cluster:test".to_string(),
            name: "test".to_string(),
            namespace: None,
            resource_version: None,
        },
        "https://example.invalid",
        Info::default(),
    )
}

fn test_state() -> SharedClusterState {
    Arc::new(Mutex::new(ClusterState::new(test_cluster())))
}

#[allow(clippy::too_many_arguments)]
fn test_tool_with(
    graph: Arc<dyn GraphBackend>,
    backend_kind: &str,
    mode: &str,
    scope: Option<GraphScope>,
    snapshot_captured_at: Option<&str>,
    initial_load_succeeded: bool,
    sync: SyncHealth,
    rebuild: Option<RebuildHealth>,
    coverage: &[&str],
) -> KubeTool {
    KubeTool::new_tool(
        "test-cluster".to_string(),
        backend_kind.to_string(),
        mode.to_string(),
        scope,
        snapshot_captured_at.map(ToOwned::to_owned),
        test_state(),
        graph,
        Arc::new(AtomicBool::new(initial_load_succeeded)),
        Arc::new(Mutex::new(sync)),
        Arc::new(Mutex::new(rebuild)),
        Arc::new(Mutex::new(
            coverage
                .iter()
                .map(|kind| kind.to_string())
                .collect::<std::collections::BTreeSet<String>>(),
        )),
    )
}

fn test_tool(graph: Arc<dyn GraphBackend>) -> KubeTool {
    let mut sync = SyncHealth::bootstrap(SystemTime::UNIX_EPOCH);
    sync.poll_interval_seconds = 5;
    test_tool_with(
        graph,
        "in-memory",
        "live",
        Some(GraphScope::namespace("kube-system")),
        None,
        true,
        sync,
        Some(RebuildHealth {
            loop_alive: true,
            poll_interval_seconds: 30,
            ..Default::default()
        }),
        &["Node"],
    )
}

#[tokio::test]
async fn graph_query_returns_structured_rows_and_truncation() {
    let graph = Arc::new(MockBackend {
        columns: vec!["pod".to_string(), "namespace".to_string()],
        rows: vec![
            json!({"pod": "a", "namespace": "default"}),
            json!({"pod": "b", "namespace": "kube-system"}),
        ],
        fail: None,
    });
    let tool = test_tool(graph);

    let result = tool
        .graph_query(Parameters(GraphQueryRequest {
            query: "MATCH (p:Pod) RETURN p['metadata']['name'] AS pod, p['metadata']['namespace'] AS namespace".to_string(),
            params: None,
            limit: Some(1),
        }))
        .await
        .expect("graph_query should succeed");
    let payload: GraphQueryResponse =
        serde_json::from_value(result.structured_content.expect("structured response"))
            .expect("query payload");

    assert_eq!(payload.columns, vec!["pod", "namespace"]);
    assert_eq!(payload.rows, vec![vec![json!("a"), json!("default")]]);
    assert_eq!(payload.row_count, 1);
    assert!(payload.truncated);
}

#[tokio::test]
async fn graph_query_in_memory_rejects_parser_unsupported_query() {
    let graph = Arc::new(MockBackend {
        columns: vec!["service_name".to_string()],
        rows: vec![json!({"service_name": "kubernetes"})],
        fail: None,
    });
    let tool = test_tool(graph);

    let err = tool
        .graph_query(Parameters(GraphQueryRequest {
            query: "MATCH (s:Service)-[:BelongsTo]->(ns:Namespace) WHERE ns['metadata']['name'] = 'default' AND NOT EXISTS { MATCH (s)-[:Manages]->(:EndpointSlice) } RETURN s['metadata']['name'] AS service_name".to_string(),
            params: None,
            limit: None,
        }))
        .await
        .expect_err("in-memory backend should still reject parser-unsupported syntax");

    assert_eq!(err.code, rmcp::model::ErrorCode::INVALID_PARAMS);
    let data = err.data.expect("data");
    assert_eq!(data["kind"], "parse_error");
    assert_eq!(data["source"], "validator");
}

#[tokio::test]
async fn graph_query_memgraph_allows_parser_unsupported_read_only_query() {
    let graph = Arc::new(MockBackend {
        columns: vec!["service_name".to_string()],
        rows: vec![json!({"service_name": "kubernetes"})],
        fail: None,
    });
    let mut sync = SyncHealth::bootstrap(SystemTime::UNIX_EPOCH);
    sync.poll_interval_seconds = 5;
    let tool = test_tool_with(
        graph,
        "memgraph",
        "snapshot",
        Some(GraphScope::cluster()),
        Some("2026-04-08T00:00:00Z"),
        true,
        sync,
        None,
        &[],
    );

    let result = tool
        .graph_query(Parameters(GraphQueryRequest {
            query: "MATCH (s:Service)-[:BelongsTo]->(ns:Namespace) WHERE ns['metadata']['name'] = 'default' AND NOT EXISTS { MATCH (s)-[:Manages]->(:EndpointSlice) } RETURN s['metadata']['name'] AS service_name".to_string(),
            params: None,
            limit: None,
        }))
        .await
        .expect("memgraph backend should allow parser-divergent read-only syntax");
    let payload: GraphQueryResponse =
        serde_json::from_value(result.structured_content.expect("structured response"))
            .expect("query payload");

    assert_eq!(payload.columns, vec!["service_name"]);
    assert_eq!(payload.rows, vec![vec![json!("kubernetes")]]);
}

#[tokio::test]
async fn graph_query_memgraph_still_rejects_schema_errors() {
    let graph = Arc::new(MockBackend::default());
    let mut sync = SyncHealth::bootstrap(SystemTime::UNIX_EPOCH);
    sync.poll_interval_seconds = 5;
    let tool = test_tool_with(
        graph,
        "memgraph",
        "snapshot",
        Some(GraphScope::cluster()),
        Some("2026-04-08T00:00:00Z"),
        true,
        sync,
        None,
        &[],
    );

    let err = tool
        .graph_query(Parameters(GraphQueryRequest {
            query: "MATCH (p:Pod)-[:BelongsTo]->(c:Cluster) RETURN p".to_string(),
            params: None,
            limit: None,
        }))
        .await
        .expect_err("schema mismatch should still be rejected");

    assert_eq!(err.code, rmcp::model::ErrorCode::INVALID_PARAMS);
    let data = err.data.expect("data");
    assert_eq!(data["kind"], "schema_error");
    assert_eq!(data["source"], "validator");
}

#[test]
fn memgraph_fallback_still_blocks_non_read_only_statement() {
    let issue = validate_graph_query_for_backend("memgraph", "CREATE INDEX ON :Pod(name)")
        .expect_err("fallback guard should reject CREATE INDEX");

    assert_eq!(issue.kind, QueryIssueKind::Semantic);
    assert_eq!(issue.source_code(), "validator");
}

#[tokio::test]
async fn graph_schema_defaults_to_compact_text() {
    let tool = test_tool(Arc::new(MockBackend::default()));
    let result = tool
        .graph_schema(Parameters(GraphSchemaRequest::default()))
        .await
        .expect("graph_schema should succeed");
    let payload: GraphSchemaCompactResponse =
        serde_json::from_value(result.structured_content.expect("structured response"))
            .expect("compact schema payload");

    assert_eq!(payload.format, GraphSchemaFormat::Compact);
    assert!(payload.schema_version.starts_with("sha256:"));
    assert!(payload.schema_text.starts_with("# Nodes\n"));
    assert!(payload.schema_text.contains("\n## Logical Nodes\n"));
    assert!(payload.schema_text.contains("\n## K8s Native Nodes\n"));
    assert!(payload.schema_text.contains("\n# Edges\n"));
    assert!(payload.schema_text.contains("Namespace("));
    assert!(payload.schema_text.contains("PersistentVolumeClaim("));
    assert!(
        payload
            .schema_text
            .contains("Container(container_type:string")
    );
    assert!(
        payload
            .schema_text
            .contains("spec:io.k8s.api.core.v1.Container")
    );
    assert!(
        payload
            .schema_text
            .contains("spec:io.k8s.api.core.v1.PodSpec")
    );
    assert!(payload.schema_text.contains("Pod-[BelongsTo]->Namespace"));
    assert!(
        payload
            .schema_text
            .contains("Deployment-[Manages]->[DaemonSet, Deployment, ReplicaSet]")
    );
    assert!(!payload.schema_text.contains("AWX-["));
    assert!(!payload.schema_text.contains("ConfigMap(data:"));
    assert!(!payload.schema_text.contains("binaryData:"));
    assert!(!payload.schema_text.contains("apiVersion:"));
    assert!(!payload.schema_text.contains("kind:"));
    assert!(
        payload
            .schema_text
            .contains("imagePullSecrets:[io.k8s.api.core.v1.LocalObjectReference]")
    );
    assert!(
        payload
            .example_patterns
            .iter()
            .all(|pattern| pattern.contains("LIMIT"))
    );
}

#[tokio::test]
async fn graph_schema_structured_mode_filters_stripped_configmap_fields() {
    let tool = test_tool(Arc::new(MockBackend::default()));
    let result = tool
        .graph_schema(Parameters(GraphSchemaRequest {
            format: GraphSchemaFormat::Structured,
        }))
        .await
        .expect("graph_schema should succeed");
    let payload: GraphSchemaResponse =
        serde_json::from_value(result.structured_content.expect("structured response"))
            .expect("schema payload");

    assert_eq!(payload.format, GraphSchemaFormat::Structured);
    assert!(payload.schema_version.starts_with("sha256:"));
    assert!(
        payload
            .example_patterns
            .iter()
            .all(|pattern| pattern.contains("LIMIT"))
    );
    assert!(
        payload
            .example_patterns
            .iter()
            .all(|pattern| pattern.contains(" AS "))
    );

    let config_map = payload
        .node_labels
        .iter()
        .find(|label| label.label == "ConfigMap")
        .expect("ConfigMap label should exist");
    assert!(
        !config_map
            .properties
            .iter()
            .any(|property| property.name == "data" || property.name == "binaryData")
    );
    let pod = payload
        .node_labels
        .iter()
        .find(|label| label.label == "Pod")
        .expect("Pod label should exist");
    assert!(pod.properties.iter().any(|property| {
        property.name == "spec" && property.property_type == "io.k8s.api.core.v1.PodSpec"
    }));
    let container = payload
        .node_labels
        .iter()
        .find(|label| label.label == "Container")
        .expect("Container label should exist");
    assert!(container.properties.iter().any(|property| {
        property.name == "spec" && property.property_type == "io.k8s.api.core.v1.Container"
    }));
    let service_account = payload
        .node_labels
        .iter()
        .find(|label| label.label == "ServiceAccount")
        .expect("ServiceAccount label should exist");
    assert!(service_account.properties.iter().any(|property| {
        property.name == "imagePullSecrets"
            && property.property_type == "[io.k8s.api.core.v1.LocalObjectReference]"
    }));
}

#[test]
fn schema_version_is_order_invariant() {
    let left = vec![
        NodeLabelSchema {
            label: "Pod".to_string(),
            properties: vec![
                SchemaProperty {
                    name: "status".to_string(),
                    property_type: "object".to_string(),
                },
                SchemaProperty {
                    name: "metadata".to_string(),
                    property_type: "ObjectMeta".to_string(),
                },
            ],
        },
        NodeLabelSchema {
            label: "Namespace".to_string(),
            properties: vec![SchemaProperty {
                name: "metadata".to_string(),
                property_type: "ObjectMeta".to_string(),
            }],
        },
    ];
    let right = vec![
        NodeLabelSchema {
            label: "Namespace".to_string(),
            properties: vec![SchemaProperty {
                name: "metadata".to_string(),
                property_type: "ObjectMeta".to_string(),
            }],
        },
        NodeLabelSchema {
            label: "Pod".to_string(),
            properties: vec![
                SchemaProperty {
                    name: "metadata".to_string(),
                    property_type: "ObjectMeta".to_string(),
                },
                SchemaProperty {
                    name: "status".to_string(),
                    property_type: "object".to_string(),
                },
            ],
        },
    ];

    let relationships_a = vec![
        ariadne_core::graph_schema::GraphRelationship {
            from: "Pod".to_string(),
            edge: "BelongsTo".to_string(),
            to: "Namespace".to_string(),
        },
        ariadne_core::graph_schema::GraphRelationship {
            from: "Pod".to_string(),
            edge: "PartOf".to_string(),
            to: "Cluster".to_string(),
        },
    ];
    let relationships_b = vec![
        ariadne_core::graph_schema::GraphRelationship {
            from: "Pod".to_string(),
            edge: "PartOf".to_string(),
            to: "Cluster".to_string(),
        },
        ariadne_core::graph_schema::GraphRelationship {
            from: "Pod".to_string(),
            edge: "BelongsTo".to_string(),
            to: "Namespace".to_string(),
        },
    ];

    assert_eq!(
        compute_schema_version(&left, &relationships_a),
        compute_schema_version(&right, &relationships_b)
    );
}

#[tokio::test]
async fn graph_health_defaults_to_compact_summary() {
    let tool = test_tool(Arc::new(MockBackend::default()));
    let result = tool
        .graph_health(Parameters(GraphHealthRequest::default()))
        .await
        .expect("graph_health should succeed");
    let payload: GraphHealthCompactResponse =
        serde_json::from_value(result.structured_content.expect("structured response"))
            .expect("health payload");

    assert_eq!(payload.detail, GraphHealthDetail::Compact);
    assert_eq!(payload.cluster, "test-cluster");
    assert_eq!(payload.mode, "live");
    assert!(payload.ready);
    assert!(payload.sync_lag_ms.is_some());
    assert_eq!(payload.coverage.degraded_resource_kinds, vec!["Node"]);
    assert_eq!(
        payload.scope.as_ref().expect("scope").kind,
        GraphScopeKind::Namespace
    );
    assert_eq!(
        payload.scope.as_ref().expect("scope").namespace.as_deref(),
        Some("kube-system")
    );
}

#[tokio::test]
async fn graph_health_full_mode_reports_live_state_and_coverage() {
    let tool = test_tool(Arc::new(MockBackend::default()));
    let result = tool
        .graph_health(Parameters(GraphHealthRequest {
            detail: GraphHealthRequestDetail::Full,
        }))
        .await
        .expect("graph_health should succeed");
    let payload: GraphHealthResponse =
        serde_json::from_value(result.structured_content.expect("structured response"))
            .expect("health payload");

    assert_eq!(payload.detail, GraphHealthDetail::Full);
    assert_eq!(payload.cluster, "test-cluster");
    assert_eq!(payload.mode, "live");
    assert!(payload.ready);
    assert_eq!(
        payload.scope.as_ref().expect("scope").kind,
        GraphScopeKind::Namespace
    );
    assert_eq!(
        payload.scope.as_ref().expect("scope").namespace.as_deref(),
        Some("kube-system")
    );
    assert!(payload.sync.is_some());
    assert!(payload.rebuild.is_some());
    assert_eq!(
        payload.sync.as_ref().expect("sync").poll_interval_seconds,
        5
    );
    assert_eq!(payload.coverage.degraded_resource_kinds, vec!["Node"]);
}

#[tokio::test]
async fn graph_health_debug_mode_uses_most_recent_live_data_timestamp() {
    let sync_success = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
    let rebuild_success = SystemTime::UNIX_EPOCH + Duration::from_secs(20);
    let mut sync = SyncHealth::bootstrap(sync_success);
    sync.poll_interval_seconds = 7;

    let tool = test_tool_with(
        Arc::new(MockBackend::default()),
        "in-memory",
        "live",
        Some(GraphScope::namespace("team-a")),
        None,
        true,
        sync,
        Some(RebuildHealth {
            last_success_at: Some(rebuild_success),
            ..Default::default()
        }),
        &["Pod", "ConfigMap", "Namespace"],
    );

    let result = tool
        .graph_health(Parameters(GraphHealthRequest {
            detail: GraphHealthRequestDetail::Debug,
        }))
        .await
        .expect("graph_health should succeed");
    let payload: GraphHealthResponse =
        serde_json::from_value(result.structured_content.expect("structured response"))
            .expect("health payload");

    assert_eq!(payload.detail, GraphHealthDetail::Debug);
    assert_eq!(payload.mode, "live");
    assert!(payload.ready);
    assert_eq!(payload.data_as_of, Some(format_timestamp(rebuild_success)));
    assert_eq!(
        payload.coverage.degraded_resource_kinds,
        vec!["ConfigMap", "Namespace", "Pod"]
    );
}

#[tokio::test]
async fn graph_health_live_without_success_reports_not_ready_and_no_lag() {
    let sync = SyncHealth {
        poll_interval_seconds: 5,
        ..Default::default()
    };
    let tool = test_tool_with(
        Arc::new(MockBackend::default()),
        "in-memory",
        "live",
        Some(GraphScope::cluster()),
        None,
        true,
        sync,
        None,
        &[],
    );

    let result = tool
        .graph_health(Parameters(GraphHealthRequest::default()))
        .await
        .expect("graph_health should succeed");
    let payload: GraphHealthCompactResponse =
        serde_json::from_value(result.structured_content.expect("structured response"))
            .expect("health payload");

    assert!(!payload.ready);
    assert!(payload.data_as_of.is_none());
    assert!(payload.sync_lag_ms.is_none());
}

#[tokio::test]
async fn graph_health_snapshot_uses_manifest_provenance() {
    let sync = SyncHealth::bootstrap(SystemTime::UNIX_EPOCH + Duration::from_secs(5));
    let tool = test_tool_with(
        Arc::new(MockBackend::default()),
        "in-memory",
        "snapshot",
        Some(GraphScope::namespace("team-a")),
        Some("2026-03-28T00:00:00Z"),
        true,
        sync,
        None,
        &["Node"],
    );

    let result = tool
        .graph_health(Parameters(GraphHealthRequest {
            detail: GraphHealthRequestDetail::Full,
        }))
        .await
        .expect("graph_health should succeed");
    let payload: GraphHealthResponse =
        serde_json::from_value(result.structured_content.expect("structured response"))
            .expect("health payload");

    assert_eq!(payload.mode, "snapshot");
    assert!(payload.ready);
    assert_eq!(payload.data_as_of.as_deref(), Some("2026-03-28T00:00:00Z"));
    assert_eq!(
        payload.scope.as_ref().expect("scope").kind,
        GraphScopeKind::Namespace
    );
    assert_eq!(
        payload.scope.as_ref().expect("scope").namespace.as_deref(),
        Some("team-a")
    );
    assert!(payload.sync.is_none());
}

#[tokio::test]
async fn graph_health_snapshot_requires_backend_probe_for_readiness() {
    let sync = SyncHealth::bootstrap(SystemTime::UNIX_EPOCH);
    let tool = test_tool_with(
        Arc::new(MockBackend {
            fail: Some("probe failure".to_string()),
            ..Default::default()
        }),
        "memgraph",
        "snapshot",
        Some(GraphScope::cluster()),
        Some("2026-03-28T00:00:00Z"),
        true,
        sync,
        None,
        &[],
    );

    let result = tool
        .graph_health(Parameters(GraphHealthRequest {
            detail: GraphHealthRequestDetail::Full,
        }))
        .await
        .expect("graph_health should succeed");
    let payload: GraphHealthResponse =
        serde_json::from_value(result.structured_content.expect("structured response"))
            .expect("health payload");

    assert!(!payload.backend_probe_ok);
    assert!(!payload.ready);
    assert_eq!(payload.data_as_of.as_deref(), Some("2026-03-28T00:00:00Z"));
}

#[test]
fn classifies_backend_scope_error() {
    let err = query_issue_to_mcp(
        ariadne_core::query_issue::classify_backend_error(
            "MemgraphError: QueryError: Query execution error: Unbound variable: ns.",
        ),
        "RETURN 1".to_string(),
    );
    assert_eq!(err.code, rmcp::model::ErrorCode::INVALID_PARAMS);
    let data = err.data.expect("data");
    assert_eq!(data["kind"], "scope_error");
    assert_eq!(data["retryable"], false);
    assert_eq!(data["repairable"], true);
    assert_eq!(data["source"], "backend");
}

#[test]
fn validator_errors_are_invalid_params_with_metadata() {
    let err = query_issue_to_mcp(
        QueryIssue::validation(
            QueryIssueKind::Schema,
            "Relationship BelongsTo not allowed".to_string(),
        ),
        "MATCH ...".to_string(),
    );
    assert_eq!(err.code, rmcp::model::ErrorCode::INVALID_PARAMS);
    let data = err.data.expect("data");
    assert_eq!(data["kind"], "schema_error");
    assert_eq!(data["retryable"], false);
    assert_eq!(data["repairable"], true);
    assert_eq!(data["source"], "validator");
}
