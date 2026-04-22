use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ariadne_core::memgraph::Memgraph;
use ariadne_core::memgraph_async::MemgraphAsync;
use ariadne_core::query_issue::{QueryIssueKind, classify_ariadne_error};
use ariadne_core::state::{ClusterState, ClusterStateDiff, GraphEdge};
use ariadne_core::types::{
    Cluster, Edge, GenericObject, ObjectIdentifier, ResourceAttributes, ResourceType,
};
use k8s_openapi::api::core::v1::Namespace;
use k8s_openapi::apimachinery::pkg::version::Info;
use rsmgclient::ConnectParams;
use serde_json::Value;
use testcontainers::core::ContainerPort;
use testcontainers::runners::{AsyncRunner, SyncRunner};
use testcontainers::{Container, ContainerAsync, GenericImage};

const MEMGRAPH_PORT: u16 = 7687;

fn docker_available() -> bool {
    if std::env::var("ARIADNE_RUN_DOCKER_TESTS")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
    {
        return true;
    }
    if let Ok(host) = std::env::var("DOCKER_HOST") {
        if !host.trim().is_empty() {
            return true;
        }
    }
    std::fs::metadata("/var/run/docker.sock").is_ok()
}

fn memgraph_image() -> GenericImage {
    GenericImage::new("memgraph/memgraph-mage", "3.7.2")
        .with_exposed_port(ContainerPort::Tcp(MEMGRAPH_PORT))
}

fn start_memgraph_sync() -> Container<GenericImage> {
    if !docker_available() {
        panic!("Docker not available; set ARIADNE_RUN_DOCKER_TESTS=1 to force");
    }
    SyncRunner::start(memgraph_image()).expect("failed to start memgraph container")
}

async fn start_memgraph_async() -> ContainerAsync<GenericImage> {
    if !docker_available() {
        panic!("Docker not available; set ARIADNE_RUN_DOCKER_TESTS=1 to force");
    }
    AsyncRunner::start(memgraph_image())
        .await
        .expect("failed to start memgraph container")
}

fn memgraph_params(host_port: u16) -> ConnectParams {
    ConnectParams {
        host: Some("127.0.0.1".to_string()),
        port: host_port,
        autocommit: true,
        ..Default::default()
    }
}

fn wait_for_memgraph(mut make_params: impl FnMut() -> ConnectParams) -> Memgraph {
    let mut last_err = None;
    for _ in 0..30 {
        match Memgraph::try_new(make_params()) {
            Ok(mg) => return mg,
            Err(err) => {
                last_err = Some(err);
                std::thread::sleep(Duration::from_millis(500));
            }
        }
    }
    panic!("memgraph did not become ready: {last_err:?}");
}

async fn wait_for_memgraph_async(mut make_params: impl FnMut() -> ConnectParams) -> MemgraphAsync {
    let mut last_err = None;
    for _ in 0..30 {
        match MemgraphAsync::try_new(make_params()) {
            Ok(mg) => return mg,
            Err(err) => {
                last_err = Some(err);
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    panic!("memgraph did not become ready: {last_err:?}");
}

fn build_cluster(uid: &str, name: &str) -> (Cluster, GenericObject) {
    let id = ObjectIdentifier {
        uid: uid.to_string(),
        name: name.to_string(),
        namespace: None,
        resource_version: None,
    };
    let info = Info {
        major: "1".to_string(),
        minor: "27".to_string(),
        ..Default::default()
    };
    let cluster = Cluster::new(id.clone(), "https://example.test", info);
    let obj = GenericObject {
        id,
        resource_type: ResourceType::Cluster,
        attributes: Some(Box::new(ResourceAttributes::Cluster {
            cluster: Box::new(cluster.clone()),
        })),
    };
    (cluster, obj)
}

fn build_namespace(uid: &str, name: &str) -> GenericObject {
    let mut namespace = Namespace::default();
    namespace.metadata.name = Some(name.to_string());
    namespace.metadata.uid = Some(uid.to_string());
    let id = ObjectIdentifier {
        uid: uid.to_string(),
        name: name.to_string(),
        namespace: None,
        resource_version: None,
    };
    GenericObject {
        id,
        resource_type: ResourceType::Namespace,
        attributes: Some(Box::new(ResourceAttributes::Namespace {
            namespace: Arc::new(namespace),
        })),
    }
}

fn build_namespace_edge(namespace_uid: &str, cluster_uid: &str) -> GraphEdge {
    GraphEdge {
        source: namespace_uid.to_string(),
        source_type: ResourceType::Namespace,
        target: cluster_uid.to_string(),
        target_type: ResourceType::Cluster,
        edge_type: Edge::PartOf,
    }
}

fn extract_count(results: &[Value], key: &str) -> i64 {
    let Value::Object(map) = &results[0] else {
        panic!("expected object result, got {results:?}");
    };
    map.get(key)
        .and_then(Value::as_i64)
        .unwrap_or_else(|| panic!("missing numeric key {key} in {map:?}"))
}

fn extract_first_object<'a>(results: &'a [Value]) -> &'a serde_json::Map<String, Value> {
    let Value::Object(map) = &results[0] else {
        panic!("expected object result, got {results:?}");
    };
    map
}

#[test]
fn memgraph_create_from_snapshot_and_query() {
    if !docker_available() {
        eprintln!("Skipping memgraph integration test; Docker not available");
        return;
    }
    let container = start_memgraph_sync();
    let host_port = container
        .get_host_port_ipv4(ContainerPort::Tcp(MEMGRAPH_PORT))
        .expect("failed to map memgraph port");
    let mut mg = wait_for_memgraph(|| memgraph_params(host_port));

    let (_cluster, cluster_obj) = build_cluster("cluster-uid", "test-cluster");
    let namespace_obj = build_namespace("ns-uid", "test-namespace");
    let edge = build_namespace_edge("ns-uid", "cluster-uid");

    mg.create_from_snapshot(&[cluster_obj, namespace_obj], &[edge])
        .expect("create_from_snapshot failed");

    let results = mg
        .execute_query("MATCH (n:Namespace)-[:PartOf]->(c:Cluster) RETURN count(n) AS cnt")
        .expect("execute_query failed");

    assert_eq!(extract_count(&results, "cnt"), 1);
}

#[test]
fn memgraph_update_from_diff_applies_changes() {
    if !docker_available() {
        eprintln!("Skipping memgraph integration test; Docker not available");
        return;
    }
    let container = start_memgraph_sync();
    let host_port = container
        .get_host_port_ipv4(ContainerPort::Tcp(MEMGRAPH_PORT))
        .expect("failed to map memgraph port");
    let mut mg = wait_for_memgraph(|| memgraph_params(host_port));

    let (_cluster, cluster_obj) = build_cluster("cluster-uid", "test-cluster");
    mg.create_from_snapshot(&[cluster_obj], &[])
        .expect("initial create_from_snapshot failed");

    let namespace_obj = build_namespace("ns-uid", "test-namespace");
    let edge = build_namespace_edge("ns-uid", "cluster-uid");
    let diff = ClusterStateDiff {
        added_nodes: vec![namespace_obj],
        removed_nodes: vec![],
        modified_nodes: vec![],
        added_edges: vec![edge],
        removed_edges: vec![],
    };

    mg.update_from_diff(&diff).expect("update_from_diff failed");

    let results = mg
        .execute_query("MATCH (n:Namespace)-[:PartOf]->(c:Cluster) RETURN count(n) AS cnt")
        .expect("execute_query failed");

    assert_eq!(extract_count(&results, "cnt"), 1);
}

#[test]
fn memgraph_update_from_diff_materializes_add_update_delete_flow() {
    if !docker_available() {
        eprintln!("Skipping memgraph integration test; Docker not available");
        return;
    }
    let container = start_memgraph_sync();
    let host_port = container
        .get_host_port_ipv4(ContainerPort::Tcp(MEMGRAPH_PORT))
        .expect("failed to map memgraph port");
    let mut mg = wait_for_memgraph(|| memgraph_params(host_port));

    let (_cluster, cluster_obj) = build_cluster("cluster-uid", "test-cluster");
    let namespace_old = build_namespace("ns-old", "old-namespace");
    let old_edge = build_namespace_edge("ns-old", "cluster-uid");
    mg.create_from_snapshot(&[cluster_obj, namespace_old], &[old_edge])
        .expect("initial create_from_snapshot failed");

    let namespace_new = build_namespace("ns-new", "new-namespace");
    let add_edge = build_namespace_edge("ns-new", "cluster-uid");
    let remove_edge = build_namespace_edge("ns-old", "cluster-uid");
    let diff = ClusterStateDiff {
        added_nodes: vec![namespace_new.clone()],
        removed_nodes: vec![build_namespace("ns-old", "old-namespace")],
        modified_nodes: vec![],
        added_edges: vec![add_edge],
        removed_edges: vec![remove_edge],
    };
    mg.update_from_diff(&diff).expect("update_from_diff failed");

    let old_results = mg
        .execute_query("MATCH (n:Namespace) WHERE n.metadata.uid = 'ns-old' RETURN count(n) AS cnt")
        .expect("query for old namespace failed");
    assert_eq!(extract_count(&old_results, "cnt"), 0);

    let new_results = mg
        .execute_query("MATCH (n:Namespace) WHERE n.metadata.uid = 'ns-new' RETURN count(n) AS cnt")
        .expect("query for new namespace failed");
    assert_eq!(extract_count(&new_results, "cnt"), 1);

    let edge_results = mg
        .execute_query("MATCH (:Namespace)-[r:PartOf]->(:Cluster) RETURN count(r) AS cnt")
        .expect("query for edge count failed");
    assert_eq!(extract_count(&edge_results, "cnt"), 1);

    let delete_diff = ClusterStateDiff {
        added_nodes: vec![],
        removed_nodes: vec![namespace_new],
        modified_nodes: vec![],
        added_edges: vec![],
        removed_edges: vec![build_namespace_edge("ns-new", "cluster-uid")],
    };
    mg.update_from_diff(&delete_diff)
        .expect("delete update_from_diff failed");

    let remaining_results = mg
        .execute_query("MATCH (n:Namespace) RETURN count(n) AS cnt")
        .expect("query for remaining namespaces failed");
    assert_eq!(extract_count(&remaining_results, "cnt"), 0);

    let probe = mg
        .execute_query("RETURN 1 AS ok")
        .expect("backend probe-style query failed");
    assert_eq!(extract_count(&probe, "ok"), 1);
}

#[test]
fn memgraph_sync_execute_query_with_columns_preserves_projection_order_and_values() {
    if !docker_available() {
        eprintln!("Skipping memgraph integration test; Docker not available");
        return;
    }
    let container = start_memgraph_sync();
    let host_port = container
        .get_host_port_ipv4(ContainerPort::Tcp(MEMGRAPH_PORT))
        .expect("failed to map memgraph port");
    let mut mg = wait_for_memgraph(|| memgraph_params(host_port));

    let mut params = HashMap::new();
    params.insert("marker".to_string(), Value::String("m".to_string()));
    let (columns, rows) = mg
        .execute_query_with_params_and_columns(
            "RETURN $marker AS marker, 42 AS answer, toString(7) AS text",
            Some(&params),
        )
        .expect("execute_query_with_params_and_columns failed");

    assert_eq!(
        columns,
        vec![
            "marker".to_string(),
            "answer".to_string(),
            "text".to_string()
        ]
    );
    assert_eq!(rows.len(), 1);
    let row = extract_first_object(&rows);
    assert_eq!(row.get("marker"), Some(&Value::String("m".to_string())));
    assert_eq!(row.get("answer"), Some(&Value::from(42)));
    assert_eq!(row.get("text"), Some(&Value::String("7".to_string())));
}

#[test]
fn memgraph_real_backend_errors_classify_to_query_issue_kinds() {
    if !docker_available() {
        eprintln!("Skipping memgraph integration test; Docker not available");
        return;
    }
    let container = start_memgraph_sync();
    let host_port = container
        .get_host_port_ipv4(ContainerPort::Tcp(MEMGRAPH_PORT))
        .expect("failed to map memgraph port");
    let mut mg = wait_for_memgraph(|| memgraph_params(host_port));

    let scope_err = mg
        .execute_query("MATCH (n:Namespace) RETURN ns")
        .expect_err("query should fail with unbound variable");
    let scope_issue = classify_ariadne_error(&scope_err);
    assert_eq!(scope_issue.kind, QueryIssueKind::Scope);
    assert!(!scope_issue.retryable());
    assert!(scope_issue.repairable());

    let param_err = mg
        .execute_query("RETURN $missing AS value")
        .expect_err("query should fail with missing parameter");
    let param_issue = classify_ariadne_error(&param_err);
    assert_eq!(param_issue.kind, QueryIssueKind::Parameter);
    assert!(!param_issue.retryable());
    assert!(param_issue.repairable());
}

#[tokio::test]
async fn memgraph_async_create_and_query() {
    if !docker_available() {
        eprintln!("Skipping memgraph integration test; Docker not available");
        return;
    }
    let container = start_memgraph_async().await;
    let host_port = container
        .get_host_port_ipv4(ContainerPort::Tcp(MEMGRAPH_PORT))
        .await
        .expect("failed to map memgraph port");
    let mg = wait_for_memgraph_async(|| memgraph_params(host_port)).await;

    let (cluster, cluster_obj) = build_cluster("cluster-uid", "test-cluster");
    let namespace_obj = build_namespace("ns-uid", "test-namespace");
    let mut state = ClusterState::new(cluster);
    state.add_node(cluster_obj);
    state.add_node(namespace_obj);
    state.add_edge(
        "ns-uid",
        ResourceType::Namespace,
        "cluster-uid",
        ResourceType::Cluster,
        Edge::PartOf,
    );

    mg.create(Arc::new(Mutex::new(state)))
        .await
        .expect("memgraph_async create failed");

    let results = mg
        .execute_query(
            "MATCH (n:Namespace)-[:PartOf]->(c:Cluster) RETURN count(n) AS cnt",
            None,
        )
        .await
        .expect("memgraph_async execute_query failed");

    assert_eq!(extract_count(&results, "cnt"), 1);

    mg.shutdown().await;
}

#[tokio::test]
async fn memgraph_async_execute_query_with_columns_preserves_projection_order() {
    if !docker_available() {
        eprintln!("Skipping memgraph integration test; Docker not available");
        return;
    }
    let container = start_memgraph_async().await;
    let host_port = container
        .get_host_port_ipv4(ContainerPort::Tcp(MEMGRAPH_PORT))
        .await
        .expect("failed to map memgraph port");
    let mg = wait_for_memgraph_async(|| memgraph_params(host_port)).await;

    let (cluster, cluster_obj) = build_cluster("cluster-uid", "test-cluster");
    let namespace_obj = build_namespace("ns-uid", "test-namespace");
    let mut state = ClusterState::new(cluster);
    state.add_node(cluster_obj);
    state.add_node(namespace_obj);

    mg.create(Arc::new(Mutex::new(state)))
        .await
        .expect("memgraph_async create failed");

    let (columns, rows) = mg
        .execute_query_with_columns(
            "MATCH (n:Namespace) RETURN count(n) AS cnt, 'x' AS marker",
            None,
        )
        .await
        .expect("memgraph_async execute_query_with_columns failed");

    assert_eq!(columns, vec!["cnt".to_string(), "marker".to_string()]);
    assert_eq!(rows.len(), 1);
    let row = extract_first_object(&rows);
    assert_eq!(row.get("cnt"), Some(&Value::from(1)));
    assert_eq!(row.get("marker"), Some(&Value::String("x".to_string())));

    mg.shutdown().await;
}
