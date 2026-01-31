use crate::types::{Edge, ResourceType, LOGICAL_RESOURCE_TYPES};
use serde::Serialize;
use std::collections::HashSet;
use std::sync::OnceLock;
use strum::IntoEnumIterator;

type EdgeKey = (ResourceType, Edge, ResourceType);

#[derive(Debug, Clone, Serialize)]
pub struct GraphRelationship {
    #[serde(rename = "from")]
    pub from: String,
    pub edge: String,
    pub to: String,
}

impl GraphRelationship {
    fn new(from: ResourceType, edge: Edge, to: ResourceType) -> Self {
        Self {
            from: from.to_string(),
            edge: edge.to_string(),
            to: to.to_string(),
        }
    }
}

const BASE_RELATIONSHIPS: &[EdgeKey] = &[
    (ResourceType::ConfigMap, Edge::PartOf, ResourceType::Cluster),
    (
        ResourceType::ConfigMap,
        Edge::BelongsTo,
        ResourceType::Namespace,
    ),
    (ResourceType::Container, Edge::PartOf, ResourceType::Cluster),
    (ResourceType::Container, Edge::HasLogs, ResourceType::Logs),
    (
        ResourceType::Container,
        Edge::BelongsTo,
        ResourceType::Namespace,
    ),
    (ResourceType::Container, Edge::Runs, ResourceType::Pod),
    (ResourceType::DaemonSet, Edge::PartOf, ResourceType::Cluster),
    (
        ResourceType::DaemonSet,
        Edge::BelongsTo,
        ResourceType::Namespace,
    ),
    (ResourceType::DaemonSet, Edge::Manages, ResourceType::Pod),
    (
        ResourceType::Deployment,
        Edge::PartOf,
        ResourceType::Cluster,
    ),
    (
        ResourceType::Deployment,
        Edge::BelongsTo,
        ResourceType::Namespace,
    ),
    (
        ResourceType::Deployment,
        Edge::Manages,
        ResourceType::ReplicaSet,
    ),
    (
        ResourceType::Endpoint,
        Edge::HasAddress,
        ResourceType::EndpointAddress,
    ),
    (
        ResourceType::EndpointAddress,
        Edge::ListedIn,
        ResourceType::EndpointSlice,
    ),
    (
        ResourceType::EndpointAddress,
        Edge::IsAddressOf,
        ResourceType::Pod,
    ),
    (
        ResourceType::EndpointSlice,
        Edge::PartOf,
        ResourceType::Cluster,
    ),
    (
        ResourceType::EndpointSlice,
        Edge::ContainsEndpoint,
        ResourceType::Endpoint,
    ),
    (
        ResourceType::EndpointSlice,
        Edge::BelongsTo,
        ResourceType::Namespace,
    ),
    (ResourceType::Host, Edge::IsClaimedBy, ResourceType::Ingress),
    (ResourceType::Ingress, Edge::PartOf, ResourceType::Cluster),
    (
        ResourceType::Ingress,
        Edge::DefinesBackend,
        ResourceType::IngressServiceBackend,
    ),
    (
        ResourceType::Ingress,
        Edge::BelongsTo,
        ResourceType::Namespace,
    ),
    (
        ResourceType::IngressServiceBackend,
        Edge::TargetsService,
        ResourceType::Service,
    ),
    (ResourceType::Job, Edge::PartOf, ResourceType::Cluster),
    (ResourceType::Job, Edge::BelongsTo, ResourceType::Namespace),
    (ResourceType::Job, Edge::Manages, ResourceType::Pod),
    (ResourceType::Logs, Edge::PartOf, ResourceType::Cluster),
    (ResourceType::Logs, Edge::BelongsTo, ResourceType::Namespace),
    (ResourceType::Namespace, Edge::PartOf, ResourceType::Cluster),
    (ResourceType::Node, Edge::PartOf, ResourceType::Cluster),
    (ResourceType::Node, Edge::Manages, ResourceType::Pod),
    (
        ResourceType::PersistentVolume,
        Edge::PartOf,
        ResourceType::Cluster,
    ),
    (
        ResourceType::PersistentVolume,
        Edge::UsesStorageClass,
        ResourceType::StorageClass,
    ),
    (
        ResourceType::PersistentVolumeClaim,
        Edge::PartOf,
        ResourceType::Cluster,
    ),
    (
        ResourceType::PersistentVolumeClaim,
        Edge::BelongsTo,
        ResourceType::Namespace,
    ),
    (
        ResourceType::PersistentVolumeClaim,
        Edge::BoundTo,
        ResourceType::PersistentVolume,
    ),
    (ResourceType::Pod, Edge::PartOf, ResourceType::Cluster),
    (ResourceType::Pod, Edge::BelongsTo, ResourceType::Namespace),
    (ResourceType::Pod, Edge::RunsOn, ResourceType::Node),
    (
        ResourceType::Pod,
        Edge::ClaimsVolume,
        ResourceType::PersistentVolumeClaim,
    ),
    (
        ResourceType::Provisioner,
        Edge::PartOf,
        ResourceType::Cluster,
    ),
    (
        ResourceType::ReplicaSet,
        Edge::PartOf,
        ResourceType::Cluster,
    ),
    (
        ResourceType::ReplicaSet,
        Edge::BelongsTo,
        ResourceType::Namespace,
    ),
    (ResourceType::ReplicaSet, Edge::Manages, ResourceType::Pod),
    (ResourceType::Service, Edge::PartOf, ResourceType::Cluster),
    (
        ResourceType::Service,
        Edge::Manages,
        ResourceType::EndpointSlice,
    ),
    (
        ResourceType::Service,
        Edge::BelongsTo,
        ResourceType::Namespace,
    ),
    (
        ResourceType::ServiceAccount,
        Edge::PartOf,
        ResourceType::Cluster,
    ),
    (
        ResourceType::ServiceAccount,
        Edge::BelongsTo,
        ResourceType::Namespace,
    ),
    (
        ResourceType::StatefulSet,
        Edge::PartOf,
        ResourceType::Cluster,
    ),
    (
        ResourceType::StatefulSet,
        Edge::BelongsTo,
        ResourceType::Namespace,
    ),
    (ResourceType::StatefulSet, Edge::Manages, ResourceType::Pod),
    (
        ResourceType::StorageClass,
        Edge::PartOf,
        ResourceType::Cluster,
    ),
    (
        ResourceType::StorageClass,
        Edge::UsesProvisioner,
        ResourceType::Provisioner,
    ),
];

pub fn graph_relationship_specs() -> Vec<EdgeKey> {
    let mut relationships: Vec<EdgeKey> = BASE_RELATIONSHIPS
        .iter()
        .map(|(from, edge, to)| (from.clone(), edge.clone(), to.clone()))
        .collect();
    for resource_type in ResourceType::iter() {
        if resource_type == ResourceType::Event || LOGICAL_RESOURCE_TYPES.contains(&resource_type) {
            continue;
        }
        relationships.push((ResourceType::Event, Edge::Concerns, resource_type));
    }
    relationships
}

pub fn graph_relationships() -> Vec<GraphRelationship> {
    graph_relationship_specs()
        .into_iter()
        .map(|(from, edge, to)| GraphRelationship::new(from, edge, to))
        .collect()
}

pub fn is_known_edge(source: &ResourceType, edge: &Edge, target: &ResourceType) -> bool {
    static EDGE_SET: OnceLock<HashSet<EdgeKey>> = OnceLock::new();
    let set = EDGE_SET.get_or_init(|| graph_relationship_specs().into_iter().collect());
    set.contains(&(source.clone(), edge.clone(), target.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn graph_relationships_are_known() {
        for (source, edge, target) in graph_relationship_specs() {
            assert!(is_known_edge(&source, &edge, &target));
        }
    }
}
