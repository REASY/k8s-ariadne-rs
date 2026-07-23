use super::ClusterState;
use crate::types::{Cluster, Edge, GenericObject, ObjectIdentifier, ResourceType};
use k8s_openapi::apimachinery::pkg::version::Info;

fn cluster_state() -> ClusterState {
    ClusterState::new(Cluster::new(
        ObjectIdentifier {
            uid: "cluster-uid".to_string(),
            name: "test-cluster".to_string(),
            namespace: None,
            resource_version: None,
        },
        "https://example.invalid",
        Info::default(),
    ))
}

fn node(uid: &str, resource_type: ResourceType) -> GenericObject {
    GenericObject {
        id: ObjectIdentifier {
            uid: uid.to_string(),
            name: uid.to_string(),
            namespace: Some("default".to_string()),
            resource_version: None,
        },
        resource_type,
        attributes: None,
    }
}

fn assert_no_edges(state: &ClusterState) {
    assert_eq!(state.get_edge_count(), 0);
    assert_eq!(state.get_edges().count(), 0);
    assert_eq!(state.get_edges_by_type(&Edge::Manages).count(), 0);
}

#[test]
fn add_edge_rejects_declared_types_that_do_not_match_resolved_nodes() {
    for (actual_source, actual_target) in [
        (ResourceType::Pod, ResourceType::ReplicaSet),
        (ResourceType::Deployment, ResourceType::Pod),
        (ResourceType::Pod, ResourceType::Pod),
    ] {
        let mut state = cluster_state();
        state.add_node(node("source", actual_source));
        state.add_node(node("target", actual_target));

        state.add_edge(
            "source",
            ResourceType::Deployment,
            "target",
            ResourceType::ReplicaSet,
            Edge::Manages,
        );

        assert_no_edges(&state);
    }
}

#[test]
fn add_edge_keeps_missing_nodes_out_of_both_edge_indexes() {
    let mut state = cluster_state();
    state.add_node(node("source", ResourceType::Deployment));

    state.add_edge(
        "source",
        ResourceType::Deployment,
        "missing-target",
        ResourceType::ReplicaSet,
        Edge::Manages,
    );
    state.add_edge(
        "missing-source",
        ResourceType::Deployment,
        "source",
        ResourceType::ReplicaSet,
        Edge::Manages,
    );

    assert_no_edges(&state);
}

#[test]
fn add_edge_preserves_valid_distinct_edges_and_ignores_duplicates() {
    let mut state = cluster_state();
    state.add_node(node("pod", ResourceType::Pod));
    state.add_node(node("config", ResourceType::ConfigMap));

    for edge in [Edge::MountsConfig, Edge::InjectsConfig, Edge::MountsConfig] {
        state.add_edge(
            "pod",
            ResourceType::Pod,
            "config",
            ResourceType::ConfigMap,
            edge,
        );
    }

    assert_eq!(state.get_edge_count(), 2);
    assert_eq!(state.get_edges().count(), 2);
    assert_eq!(state.get_edges_by_type(&Edge::MountsConfig).count(), 1);
    assert_eq!(state.get_edges_by_type(&Edge::InjectsConfig).count(), 1);
}
