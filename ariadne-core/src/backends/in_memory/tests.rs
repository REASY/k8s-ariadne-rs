use super::*;
use crate::state::ClusterState;
use crate::types::{Cluster, Edge, ObjectIdentifier};
use k8s_openapi::api::apps::v1::{Deployment, ReplicaSet};
use k8s_openapi::api::core::v1::{
    ConfigMap, ContainerState, ContainerStateTerminated, ContainerStatus, Pod, PodStatus,
};
use k8s_openapi::apimachinery::pkg::version::Info;
use std::sync::{Arc, Mutex};

fn dummy_cluster() -> Cluster {
    let id = ObjectIdentifier {
        uid: "cluster-uid".to_string(),
        name: "test".to_string(),
        namespace: None,
        resource_version: None,
    };
    Cluster::new(id, "https://example.invalid", Info::default())
}

fn pod(uid: &str, name: &str, namespace: &str) -> GenericObject {
    let pod = Pod {
        metadata: ObjectMeta {
            uid: Some(uid.to_string()),
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        ..Default::default()
    };
    GenericObject {
        id: ObjectIdentifier {
            uid: uid.to_string(),
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            resource_version: None,
        },
        resource_type: ResourceType::Pod,
        attributes: Some(Box::new(ResourceAttributes::Pod { pod: Arc::new(pod) })),
    }
}

#[test]
fn node_serialization_redacts_config_map_payloads() {
    let config_map = ConfigMap {
        metadata: ObjectMeta {
            name: Some("settings".to_string()),
            namespace: Some("team-a".to_string()),
            uid: Some("config-map-1".to_string()),
            annotations: Some(std::collections::BTreeMap::from([(
                "credential".to_string(),
                "secret".to_string(),
            )])),
            ..Default::default()
        },
        data: Some(std::collections::BTreeMap::from([(
            "password".to_string(),
            "secret".to_string(),
        )])),
        ..Default::default()
    };
    let object = GenericObject {
        id: ObjectIdentifier {
            uid: "config-map-1".to_string(),
            name: "settings".to_string(),
            namespace: Some("team-a".to_string()),
            resource_version: None,
        },
        resource_type: ResourceType::ConfigMap,
        attributes: Some(Box::new(ResourceAttributes::ConfigMap {
            config_map: Arc::new(config_map),
        })),
    };

    let value = node_to_value(&object).expect("ConfigMap should serialize");
    assert!(value.get("data").is_none());
    assert!(value.pointer("/metadata/annotations").is_none());
    assert_eq!(
        value.get("metadata_name"),
        Some(&Value::String("settings".to_string()))
    );
}

fn pod_with_container_status(
    uid: &str,
    name: &str,
    namespace: &str,
    reason: &str,
) -> GenericObject {
    let mut pod = Pod {
        metadata: ObjectMeta {
            uid: Some(uid.to_string()),
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        ..Default::default()
    };
    let terminated = ContainerStateTerminated {
        reason: Some(reason.to_string()),
        exit_code: 137,
        message: Some("terminated".to_string()),
        ..Default::default()
    };
    let last_state = ContainerState {
        terminated: Some(terminated),
        ..Default::default()
    };
    let status = ContainerStatus {
        name: "main".to_string(),
        last_state: Some(last_state),
        ..Default::default()
    };
    pod.status = Some(PodStatus {
        container_statuses: Some(vec![status]),
        ..Default::default()
    });
    GenericObject {
        id: ObjectIdentifier {
            uid: uid.to_string(),
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            resource_version: None,
        },
        resource_type: ResourceType::Pod,
        attributes: Some(Box::new(ResourceAttributes::Pod { pod: Arc::new(pod) })),
    }
}

fn deployment(uid: &str, name: &str, namespace: &str) -> GenericObject {
    let dep = Deployment {
        metadata: ObjectMeta {
            uid: Some(uid.to_string()),
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        ..Default::default()
    };
    GenericObject {
        id: ObjectIdentifier {
            uid: uid.to_string(),
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            resource_version: None,
        },
        resource_type: ResourceType::Deployment,
        attributes: Some(Box::new(ResourceAttributes::Deployment {
            deployment: Arc::new(dep),
        })),
    }
}

fn replica_set(uid: &str, name: &str, namespace: &str) -> GenericObject {
    let rs = ReplicaSet {
        metadata: ObjectMeta {
            uid: Some(uid.to_string()),
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        ..Default::default()
    };
    GenericObject {
        id: ObjectIdentifier {
            uid: uid.to_string(),
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            resource_version: None,
        },
        resource_type: ResourceType::ReplicaSet,
        attributes: Some(Box::new(ResourceAttributes::ReplicaSet {
            replica_set: Arc::new(rs),
        })),
    }
}

#[test]
fn executes_match_where_return() {
    let mut state = ClusterState::new(dummy_cluster());
    state.add_node(pod("p1", "pod-one", "ns1"));
    state.add_node(pod("p2", "pod-two", "ns2"));

    let query = parse_query(
        "MATCH (p:Pod) WHERE p.metadata.name = 'pod-one' RETURN p.metadata.name AS name",
    )
    .unwrap();
    validate_query(&query, ValidationMode::Engine).unwrap();

    let mut stats = QueryStats::default();
    let results = execute_query_ast(&query, &state, &HashMap::new(), &mut stats).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("name").and_then(|v| v.as_str()),
        Some("pod-one")
    );
}

#[test]
fn executes_count() {
    let mut state = ClusterState::new(dummy_cluster());
    state.add_node(pod("p1", "pod-one", "ns1"));
    state.add_node(pod("p2", "pod-two", "ns2"));

    let query = parse_query("MATCH (p:Pod) RETURN count(p) AS total").unwrap();
    validate_query(&query, ValidationMode::Engine).unwrap();

    let mut stats = QueryStats::default();
    let results = execute_query_ast(&query, &state, &HashMap::new(), &mut stats).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("total").and_then(|v| v.as_i64()), Some(2));
}

#[test]
fn executes_relationship_match() {
    let mut state = ClusterState::new(dummy_cluster());
    let dep = deployment("d1", "deploy", "ns1");
    let rs = replica_set("r1", "rs", "ns1");
    state.add_node(dep);
    state.add_node(rs);
    state.add_edge(
        "d1",
        ResourceType::Deployment,
        "r1",
        ResourceType::ReplicaSet,
        Edge::Manages,
    );

    let query = parse_query(
        "MATCH (d:Deployment)-[:Manages]->(r:ReplicaSet) RETURN r.metadata.name AS name",
    )
    .unwrap();
    validate_query(&query, ValidationMode::Engine).unwrap();

    let mut stats = QueryStats::default();
    let results = execute_query_ast(&query, &state, &HashMap::new(), &mut stats).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("name").and_then(|v| v.as_str()), Some("rs"));
}

#[test]
fn executes_multi_hop_relationship_match() {
    let mut state = ClusterState::new(dummy_cluster());
    let dep = deployment("d1", "deploy", "ns1");
    let rs1 = replica_set("r1", "rs1", "ns1");
    let rs2 = replica_set("r2", "rs2", "ns1");
    let pod1 = pod("p1", "pod1", "ns1");
    let pod2 = pod("p2", "pod2", "ns1");
    state.add_node(dep);
    state.add_node(rs1);
    state.add_node(rs2);
    state.add_node(pod1);
    state.add_node(pod2);
    state.add_edge(
        "d1",
        ResourceType::Deployment,
        "r1",
        ResourceType::ReplicaSet,
        Edge::Manages,
    );
    state.add_edge(
        "r1",
        ResourceType::ReplicaSet,
        "p1",
        ResourceType::Pod,
        Edge::Manages,
    );
    state.add_edge(
        "r2",
        ResourceType::ReplicaSet,
        "p2",
        ResourceType::Pod,
        Edge::Manages,
    );

    let query = parse_query(
        "MATCH (d:Deployment)-[:Manages]->(:ReplicaSet)-[:Manages]->(p:Pod) RETURN p.metadata.name AS name",
    )
    .unwrap();
    validate_query(&query, ValidationMode::Engine).unwrap();

    let mut stats = QueryStats::default();
    let results = execute_query_ast(&query, &state, &HashMap::new(), &mut stats).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("name").and_then(|v| v.as_str()),
        Some("pod1")
    );
}

#[test]
fn executes_relationship_variable() {
    let mut state = ClusterState::new(dummy_cluster());
    let dep = deployment("d1", "deploy", "ns1");
    let rs = replica_set("r1", "rs", "ns1");
    state.add_node(dep);
    state.add_node(rs);
    state.add_edge(
        "d1",
        ResourceType::Deployment,
        "r1",
        ResourceType::ReplicaSet,
        Edge::Manages,
    );

    let query =
        parse_query("MATCH (d:Deployment)-[r:Manages]->(s:ReplicaSet) RETURN r.type AS kind")
            .unwrap();
    validate_query(&query, ValidationMode::Engine).unwrap();

    let mut stats = QueryStats::default();
    let results = execute_query_ast(&query, &state, &HashMap::new(), &mut stats).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("kind").and_then(|v| v.as_str()),
        Some("Manages")
    );
}

#[test]
fn executes_unwind_with_aggregate() {
    let state = ClusterState::new(dummy_cluster());
    let query =
        parse_query("UNWIND [1,2,3] AS x WITH x RETURN sum(x) AS total, collect(x) AS items")
            .unwrap();
    validate_query(&query, ValidationMode::Engine).unwrap();

    let mut stats = QueryStats::default();
    let results = execute_query_ast(&query, &state, &HashMap::new(), &mut stats).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("total").and_then(|v| v.as_f64()), Some(6.0));
    let items = results[0]
        .get("items")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap();
    assert_eq!(items.len(), 3);
}

#[test]
fn executes_multi_match() {
    let mut state = ClusterState::new(dummy_cluster());
    state.add_node(pod("p1", "pod-one", "ns1"));
    state.add_node(pod("p2", "pod-two", "ns1"));

    let query = parse_query("MATCH (p:Pod) MATCH (q:Pod) RETURN count(*) AS total").unwrap();
    validate_query(&query, ValidationMode::Engine).unwrap();

    let mut stats = QueryStats::default();
    let results = execute_query_ast(&query, &state, &HashMap::new(), &mut stats).unwrap();
    assert_eq!(results[0].get("total").and_then(|v| v.as_i64()), Some(4));
}

#[test]
fn backend_executes_query() {
    let mut state = ClusterState::new(dummy_cluster());
    state.add_node(pod("p1", "pod-one", "ns1"));
    let shared = Arc::new(Mutex::new(state));

    let backend = InMemoryBackend::new();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        backend.create(shared.clone()).await.unwrap();
        let results = backend
            .execute_query("MATCH (p:Pod) RETURN count(p) AS total".to_string(), None)
            .await
            .unwrap();
        assert_eq!(results[0].get("total").and_then(|v| v.as_i64()), Some(1));
    });
}

#[test]
fn executes_string_predicate() {
    let mut state = ClusterState::new(dummy_cluster());
    state.add_node(pod("p1", "pod-one", "ns1"));
    state.add_node(pod("p2", "pod-two", "ns1"));

    let query = parse_query(
        "MATCH (p:Pod) WHERE p.metadata.name ENDS WITH 'one' RETURN p.metadata.name AS name",
    )
    .unwrap();
    validate_query(&query, ValidationMode::Engine).unwrap();

    let mut stats = QueryStats::default();
    let results = execute_query_ast(&query, &state, &HashMap::new(), &mut stats).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("name").and_then(|v| v.as_str()),
        Some("pod-one")
    );
}

#[test]
fn executes_case_expression() {
    let state = ClusterState::new(dummy_cluster());
    let query = parse_query("UNWIND [1] AS x WITH CASE WHEN x = 1 THEN 5 ELSE 0 END AS v RETURN v")
        .unwrap();
    validate_query(&query, ValidationMode::Engine).unwrap();

    let mut stats = QueryStats::default();
    let results = execute_query_ast(&query, &state, &HashMap::new(), &mut stats).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("v").and_then(|v| v.as_i64()), Some(5));
}

#[test]
fn executes_replace_function() {
    let state = ClusterState::new(dummy_cluster());
    let query = parse_query("RETURN replace('250m','m','') AS v").unwrap();
    validate_query(&query, ValidationMode::Engine).unwrap();

    let mut stats = QueryStats::default();
    let results = execute_query_ast(&query, &state, &HashMap::new(), &mut stats).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("v").and_then(|v| v.as_str()), Some("250"));
}

#[test]
fn executes_labels_function() {
    let mut state = ClusterState::new(dummy_cluster());
    state.add_node(pod("p1", "pod-one", "ns1"));

    let query = parse_query("MATCH (p:Pod) RETURN labels(p) AS labels").unwrap();
    validate_query(&query, ValidationMode::Engine).unwrap();

    let mut stats = QueryStats::default();
    let results = execute_query_ast(&query, &state, &HashMap::new(), &mut stats).unwrap();
    let labels = results[0].get("labels").and_then(|v| v.as_array()).cloned();
    assert_eq!(labels, Some(vec![Value::String("Pod".to_string())]));
}

#[test]
fn executes_mixed_multiplicative_expression() {
    let state = ClusterState::new(dummy_cluster());
    let query = parse_query("RETURN 1000 / 1024 / 1024 AS v").unwrap();
    validate_query(&query, ValidationMode::Engine).unwrap();

    let mut stats = QueryStats::default();
    let results = execute_query_ast(&query, &state, &HashMap::new(), &mut stats).unwrap();
    let v = results[0].get("v").and_then(|v| v.as_f64()).unwrap();
    let expected = 1000.0 / 1024.0 / 1024.0;
    assert!((v - expected).abs() < 1e-9, "expected {expected}, got {v}");
}

#[test]
fn executes_label_predicate_filter() {
    let mut state = ClusterState::new(dummy_cluster());
    state.add_node(pod("p1", "pod-one", "ns1"));
    state.add_node(deployment("d1", "deploy", "ns1"));

    let query = parse_query("MATCH (n) WHERE n:Pod RETURN count(n) AS total").unwrap();
    validate_query(&query, ValidationMode::Engine).unwrap();

    let mut stats = QueryStats::default();
    let results = execute_query_ast(&query, &state, &HashMap::new(), &mut stats).unwrap();
    assert_eq!(results[0].get("total").and_then(|v| v.as_i64()), Some(1));
}

#[test]
fn executes_label_predicate_with_or() {
    let mut state = ClusterState::new(dummy_cluster());
    state.add_node(pod("p1", "pod-one", "ns1"));
    state.add_node(pod("p2", "pod-two", "ns1"));
    state.add_node(deployment("d1", "deploy", "ns1"));

    let query =
        parse_query("MATCH (n) WHERE n:Pod OR n:Deployment RETURN count(n) AS total").unwrap();
    validate_query(&query, ValidationMode::Engine).unwrap();

    let mut stats = QueryStats::default();
    let results = execute_query_ast(&query, &state, &HashMap::new(), &mut stats).unwrap();
    assert_eq!(results[0].get("total").and_then(|v| v.as_i64()), Some(3));
}

#[test]
fn executes_exists_subquery() {
    let mut state = ClusterState::new(dummy_cluster());
    state.add_node(deployment("d1", "deploy", "ns1"));
    state.add_node(replica_set("r1", "rs", "ns1"));
    state.add_edge(
        "d1",
        ResourceType::Deployment,
        "r1",
        ResourceType::ReplicaSet,
        Edge::Manages,
    );

    let query = parse_query(
        "MATCH (d:Deployment) WHERE exists { (d)-[:Manages]->(r:ReplicaSet) } RETURN count(d) AS total",
    )
    .unwrap();
    validate_query(&query, ValidationMode::Engine).unwrap();

    let mut stats = QueryStats::default();
    let results = execute_query_ast(&query, &state, &HashMap::new(), &mut stats).unwrap();
    assert_eq!(results[0].get("total").and_then(|v| v.as_i64()), Some(1));
}

#[test]
fn executes_not_exists_subquery() {
    let mut state = ClusterState::new(dummy_cluster());
    state.add_node(deployment("d1", "deploy-a", "ns1"));
    state.add_node(deployment("d2", "deploy-b", "ns1"));
    state.add_node(replica_set("r1", "rs", "ns1"));
    state.add_edge(
        "d1",
        ResourceType::Deployment,
        "r1",
        ResourceType::ReplicaSet,
        Edge::Manages,
    );

    let query = parse_query(
        "MATCH (d:Deployment) WHERE NOT exists { (d)-[:Manages]->(r:ReplicaSet) } RETURN count(d) AS total",
    )
    .unwrap();
    validate_query(&query, ValidationMode::Engine).unwrap();

    let mut stats = QueryStats::default();
    let results = execute_query_ast(&query, &state, &HashMap::new(), &mut stats).unwrap();
    assert_eq!(results[0].get("total").and_then(|v| v.as_i64()), Some(1));
}

#[test]
fn executes_exists_subquery_with_where() {
    let mut state = ClusterState::new(dummy_cluster());
    state.add_node(deployment("d1", "deploy", "ns1"));
    state.add_node(replica_set("r1", "rs", "ns1"));
    state.add_edge(
        "d1",
        ResourceType::Deployment,
        "r1",
        ResourceType::ReplicaSet,
        Edge::Manages,
    );

    let query = parse_query(
        "MATCH (d:Deployment) WHERE exists { (d)-[:Manages]->(r:ReplicaSet) WHERE r.metadata.name = 'rs' } RETURN count(d) AS total",
    )
    .unwrap();
    validate_query(&query, ValidationMode::Engine).unwrap();

    let mut stats = QueryStats::default();
    let results = execute_query_ast(&query, &state, &HashMap::new(), &mut stats).unwrap();
    assert_eq!(results[0].get("total").and_then(|v| v.as_i64()), Some(1));

    let query = parse_query(
        "MATCH (d:Deployment) WHERE exists { (d)-[:Manages]->(r:ReplicaSet) WHERE r.metadata.name = 'nope' } RETURN count(d) AS total",
    )
    .unwrap();
    validate_query(&query, ValidationMode::Engine).unwrap();
    let results = execute_query_ast(&query, &state, &HashMap::new(), &mut stats).unwrap();
    assert!(results.is_empty());
}

#[test]
fn multi_hop_match_first_match_filter() {
    let mut state = ClusterState::new(dummy_cluster());
    state.add_node(deployment("d1", "deploy", "ns1"));
    state.add_node(deployment("d2", "deploy-2", "ns1"));
    state.add_node(replica_set("r1", "rs", "ns1"));
    state.add_node(pod("p1", "pod-one", "ns1"));
    state.add_edge(
        "d1",
        ResourceType::Deployment,
        "r1",
        ResourceType::ReplicaSet,
        Edge::Manages,
    );
    state.add_edge(
        "r1",
        ResourceType::ReplicaSet,
        "p1",
        ResourceType::Pod,
        Edge::Manages,
    );

    let query = parse_query(
        "MATCH (d:Deployment) MATCH (d)-[:Manages]->(:ReplicaSet)-[:Manages]->(:Pod) RETURN d.metadata.name AS name",
    )
    .unwrap();
    validate_query(&query, ValidationMode::Engine).unwrap();

    let mut stats = QueryStats::default();
    let results = execute_query_ast(&query, &state, &HashMap::new(), &mut stats).unwrap();
    let names: Vec<_> = results
        .into_iter()
        .filter_map(|row| {
            row.get("name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        })
        .collect();
    assert_eq!(names, vec!["deploy".to_string()]);
}

#[test]
fn executes_quantifiers() {
    let state = ClusterState::new(dummy_cluster());
    let query = parse_query(
        "RETURN any(x IN [1,2,3] WHERE x = 2) AS any, \
         all(x IN [1,2,3] WHERE x > 0) AS all, \
         none(x IN [1,2,3] WHERE x < 0) AS none, \
         single(x IN [1,2,3] WHERE x = 2) AS single",
    )
    .unwrap();
    validate_query(&query, ValidationMode::Engine).unwrap();

    let mut stats = QueryStats::default();
    let results = execute_query_ast(&query, &state, &HashMap::new(), &mut stats).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("any").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(results[0].get("all").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(results[0].get("none").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(
        results[0].get("single").and_then(|v| v.as_bool()),
        Some(true)
    );
}

#[test]
fn executes_quantifier_with_list_comprehension_smoke() {
    let mut state = ClusterState::new(dummy_cluster());
    state.add_node(pod_with_container_status(
        "p1",
        "oom-pod",
        "ns1",
        "OOMKilled",
    ));
    state.add_node(pod_with_container_status(
        "p2",
        "ok-pod",
        "ns1",
        "Completed",
    ));

    let query = parse_query(
        "MATCH (p:Pod)\n\
         WHERE ANY(cs IN p['status']['containerStatuses'] WHERE cs['lastState']['terminated']['reason'] = 'OOMKilled')\n\
         RETURN p['metadata']['namespace'] AS namespace,\n\
                p['metadata']['name'] AS pod,\n\
                [cs IN p['status']['containerStatuses'] WHERE cs['lastState']['terminated']['reason'] = 'OOMKilled' | {\n\
                  container: cs['name'],\n\
                  exitCode: cs['lastState']['terminated']['exitCode'],\n\
                  finishedAt: cs['lastState']['terminated']['finishedAt'],\n\
                  message: cs['lastState']['terminated']['message']\n\
                }] AS oom_killed_containers\n\
         ORDER BY namespace, pod",
    )
    .unwrap();
    validate_query(&query, ValidationMode::Engine).unwrap();

    let mut stats = QueryStats::default();
    let results = execute_query_ast(&query, &state, &HashMap::new(), &mut stats).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("pod").and_then(|v| v.as_str()),
        Some("oom-pod")
    );
    let containers = results[0]
        .get("oom_killed_containers")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert_eq!(containers.len(), 1);
    let container = containers[0].as_object().cloned().unwrap_or_default();
    assert_eq!(
        container.get("container").and_then(|v| v.as_str()),
        Some("main")
    );
    assert_eq!(
        container.get("exitCode").and_then(|v| v.as_i64()),
        Some(137)
    );
}

#[test]
fn executes_collect_slice_and_index() {
    let mut state = ClusterState::new(dummy_cluster());
    state.add_node(pod("p1", "alpha", "ns1"));
    state.add_node(pod("p2", "beta", "ns1"));
    state.add_node(pod("p3", "gamma", "ns1"));

    let query = parse_query(
        "MATCH (p:Pod)\n\
         WITH p ORDER BY p.metadata.name\n\
         RETURN collect(p.metadata.name)[0..2] AS names, collect(p.metadata.name)[1] AS second",
    )
    .unwrap();
    validate_query(&query, ValidationMode::Engine).unwrap();

    let mut stats = QueryStats::default();
    let results = execute_query_ast(&query, &state, &HashMap::new(), &mut stats).unwrap();
    assert_eq!(results.len(), 1);
    let names = results[0]
        .get("names")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        names,
        vec![
            Value::String("alpha".to_string()),
            Value::String("beta".to_string())
        ]
    );
    assert_eq!(
        results[0].get("second").and_then(|v| v.as_str()),
        Some("beta")
    );
}

#[test]
fn executes_aggregate_arithmetic() {
    let state = ClusterState::new(dummy_cluster());
    let query =
        parse_query("UNWIND [1024, 2048] AS x RETURN sum(x) AS total, sum(x) / 1024 AS gib")
            .unwrap();
    validate_query(&query, ValidationMode::Engine).unwrap();

    let mut stats = QueryStats::default();
    let results = execute_query_ast(&query, &state, &HashMap::new(), &mut stats).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("total").and_then(|v| v.as_f64()),
        Some(3072.0)
    );
    assert_eq!(results[0].get("gib").and_then(|v| v.as_f64()), Some(3.0));
}

#[test]
fn executes_keys_function() {
    let state = ClusterState::new(dummy_cluster());
    let query = parse_query("RETURN keys({b: 1, a: 2}) AS ks").unwrap();
    validate_query(&query, ValidationMode::Engine).unwrap();

    let mut stats = QueryStats::default();
    let results = execute_query_ast(&query, &state, &HashMap::new(), &mut stats).unwrap();
    let keys = results[0]
        .get("ks")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        keys,
        vec![
            Value::String("a".to_string()),
            Value::String("b".to_string())
        ]
    );
}
