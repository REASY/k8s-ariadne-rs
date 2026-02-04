use std::sync::{Arc, Mutex};
use std::time::Duration;

use ariadne_core::memgraph::Memgraph;
use ariadne_core::memgraph_async::MemgraphAsync;
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
        .execute_query("MATCH (n:Namespace)-[:PartOf]->(c:Cluster) RETURN count(n) AS cnt")
        .await
        .expect("memgraph_async execute_query failed");

    assert_eq!(extract_count(&results, "cnt"), 1);

    mg.shutdown().await;
}
