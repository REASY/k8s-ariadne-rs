use crate::diff::{Diff, ObservedClusterSnapshotDiff};
use crate::graph_schema;
use crate::id_gen::{GetNextIdResult, IdGen};
use crate::state_resolver::ObservedClusterSnapshot;
use crate::types::{Cluster, Edge, GenericObject, ResourceType};
use kube::ResourceExt;
use petgraph::graphmap::DiGraphMap;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};
use tracing::warn;

pub type NodeId = u32;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct GraphVertex {
    id: String,
    name: String,
    namespace: Option<String>,
    version: Option<String>,
    node_type: ResourceType,
}

impl GraphVertex {
    pub fn new(node: &GenericObject) -> Self {
        GraphVertex {
            id: node.id.uid.clone(),
            name: node.id.name.clone(),
            namespace: node.id.namespace.clone(),
            version: node.id.resource_version.clone(),
            node_type: node.resource_type.clone(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct GraphEdge {
    pub source: String,
    pub source_type: ResourceType,
    pub target: String,
    pub target_type: ResourceType,
    pub edge_type: Edge,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DirectedGraph {
    vertices: Vec<GraphVertex>,
    edges: Vec<GraphEdge>,
}

#[derive(Debug, Default, Clone)]
pub struct ClusterStateDiff {
    pub added_nodes: Vec<GenericObject>,
    pub removed_nodes: Vec<GenericObject>,
    pub modified_nodes: Vec<GenericObject>,
    pub added_edges: Vec<GraphEdge>,
    pub removed_edges: Vec<GraphEdge>,
}

impl ClusterStateDiff {
    pub fn is_empty(&self) -> bool {
        self.added_nodes.is_empty()
            && self.removed_nodes.is_empty()
            && self.modified_nodes.is_empty()
            && self.added_edges.is_empty()
            && self.removed_edges.is_empty()
    }
}

#[macro_export]
macro_rules! create_generic_object {
    ($item:expr, $resource_type:ident, $variant:ident, $field:ident) => {
        GenericObject {
            id: ObjectIdentifier {
                uid: $item.uid().unwrap().to_string(),
                name: $item.name_any(),
                namespace: $item.namespace(),
                resource_version: $item.resource_version(),
            },
            resource_type: ResourceType::$resource_type,
            attributes: Some(Box::new(ResourceAttributes::$variant {
                $field: $item.clone(),
            })),
        }
    };
}

#[derive(Debug)]
pub struct ClusterState {
    pub cluster: Cluster,
    graph: DiGraphMap<NodeId, Edge>,
    id_gen: IdGen,
    id_to_node: HashMap<NodeId, GenericObject>,
}

type EdgeKey = (ResourceType, Edge, ResourceType);

fn should_log_unknown_edge(source: &ResourceType, edge: &Edge, target: &ResourceType) -> bool {
    static UNKNOWN_EDGES: OnceLock<Mutex<HashSet<EdgeKey>>> = OnceLock::new();
    let set = UNKNOWN_EDGES.get_or_init(|| Mutex::new(HashSet::new()));
    let mut guard = set.lock().expect("Unknown edge log guard poisoned");
    guard.insert((source.clone(), edge.clone(), target.clone()))
}

impl ClusterState {
    pub fn new(cluster: Cluster) -> Self {
        ClusterState {
            cluster,
            graph: DiGraphMap::new(),
            id_gen: IdGen::new(),
            id_to_node: HashMap::new(),
        }
    }

    pub fn add_node(&mut self, node: GenericObject) {
        match self.id_gen.get_next_id(&node.id.uid) {
            GetNextIdResult::Existing(id) => {
                self.id_to_node.insert(id, node);
            }
            GetNextIdResult::New(new_id) => {
                self.id_to_node.insert(new_id, node);
                self.graph.add_node(new_id);
            }
        }
    }

    pub fn add_edge(
        &mut self,
        source: &str,
        source_type: ResourceType,
        target: &str,
        target_type: ResourceType,
        edge: Edge,
    ) {
        if !graph_schema::is_known_edge(&source_type, &edge, &target_type) {
            if should_log_unknown_edge(&source_type, &edge, &target_type) {
                warn!("Unknown edge: {source_type:?}-[:{edge:?}]->{target_type:?} (skipping)");
            }
            return;
        }
        let maybe_source = self.get_node(source);
        let maybe_target = self.get_node(target);

        match (maybe_source, maybe_target) {
            (Some(from), Some(to)) => {
                self.graph.add_edge(from, to, edge);
            }
            (from_id, to_id) => {
                warn!("Node(s) do not exist, source: {source} [{source_type}], from_id: {from_id:?}, target: {target} [{target_type}], to_id: {to_id:?}, edge: {edge:?}")
            }
        }
    }

    pub fn to_directed_graph(&self) -> DirectedGraph {
        let mut vertices: Vec<GraphVertex> = self.get_nodes().map(GraphVertex::new).collect();
        vertices.sort_by_key(|v| v.id.clone());

        let mut edges: Vec<GraphEdge> = self.get_edges().collect();
        edges.sort_by(|a, b| {
            let key_a = (a.source.as_str(), a.target.as_str(), a.edge_type.clone());
            let key_b = (b.source.as_str(), b.target.as_str(), b.edge_type.clone());
            key_a.cmp(&key_b)
        });

        DirectedGraph { vertices, edges }
    }

    fn get_node(&mut self, uid: &str) -> Option<u32> {
        self.id_gen.get_id(uid)
    }

    pub fn get_nodes(&self) -> impl Iterator<Item = &GenericObject> {
        self.graph
            .nodes()
            .map(|id| self.id_to_node.get(&id).unwrap())
    }

    pub fn get_edges(&self) -> impl Iterator<Item = GraphEdge> + use<'_> {
        self.graph.all_edges().map(|(from, to, t)| {
            let source = self.id_gen.get_by_id(from).unwrap();
            let source_resource_type = self.id_to_node.get(&from).unwrap().resource_type.clone();
            let target = self.id_gen.get_by_id(to).unwrap();
            let target_resource_type = self.id_to_node.get(&to).unwrap().resource_type.clone();
            GraphEdge {
                source,
                source_type: source_resource_type,
                target,
                target_type: target_resource_type,
                edge_type: t.clone(),
            }
        })
    }

    pub fn get_node_count(&self) -> usize {
        self.graph.node_count()
    }

    pub fn get_edge_count(&self) -> usize {
        self.graph.edge_count()
    }

    pub fn node_by_uid(&self, uid: &str) -> Option<&GenericObject> {
        self.id_gen
            .get_id(uid)
            .and_then(|node_id| self.id_to_node.get(&node_id))
    }

    fn node_map(&self) -> HashMap<String, &GenericObject> {
        let mut map = HashMap::with_capacity(self.id_to_node.len());
        for (node_id, node) in &self.id_to_node {
            if let Some(uid) = self.id_gen.get_by_id(*node_id) {
                map.insert(uid, node);
            }
        }
        map
    }

    pub fn diff(
        &self,
        new_state: &ClusterState,
        prev_snapshot: &ObservedClusterSnapshot,
        new_snapshot: &ObservedClusterSnapshot,
    ) -> ClusterStateDiff {
        let mut diff = ClusterStateDiff::default();
        let mut processed_uids: HashSet<String> = HashSet::new();

        let snapshot_diff = ObservedClusterSnapshotDiff::new(new_snapshot, prev_snapshot);

        fn apply_resource_diff<'a, T: ResourceExt>(
            resource_diff: &Diff<'a, T>,
            prev_state: &ClusterState,
            new_state: &ClusterState,
            out: &mut ClusterStateDiff,
            processed: &mut HashSet<String>,
        ) {
            for item in &resource_diff.added {
                if let Some(uid) = item.meta().uid.as_deref() {
                    if processed.insert(uid.to_string()) {
                        match new_state.node_by_uid(uid) {
                            Some(node) => out.added_nodes.push(node.clone()),
                            None => warn!("Added resource {uid} missing from new state"),
                        }
                    }
                }
            }

            for item in &resource_diff.removed {
                if let Some(uid) = item.meta().uid.as_deref() {
                    if processed.insert(uid.to_string()) {
                        match prev_state.node_by_uid(uid) {
                            Some(node) => out.removed_nodes.push(node.clone()),
                            None => warn!("Removed resource {uid} missing from previous state"),
                        }
                    }
                }
            }

            for item in &resource_diff.modified {
                if let Some(uid) = item.meta().uid.as_deref() {
                    if processed.insert(uid.to_string()) {
                        match new_state.node_by_uid(uid) {
                            Some(node) => out.modified_nodes.push(node.clone()),
                            None => warn!("Modified resource {uid} missing from new state"),
                        }
                    }
                }
            }
        }

        apply_resource_diff(
            &snapshot_diff.namespaces,
            self,
            new_state,
            &mut diff,
            &mut processed_uids,
        );
        apply_resource_diff(
            &snapshot_diff.pods,
            self,
            new_state,
            &mut diff,
            &mut processed_uids,
        );
        apply_resource_diff(
            &snapshot_diff.deployments,
            self,
            new_state,
            &mut diff,
            &mut processed_uids,
        );
        apply_resource_diff(
            &snapshot_diff.stateful_sets,
            self,
            new_state,
            &mut diff,
            &mut processed_uids,
        );
        apply_resource_diff(
            &snapshot_diff.replica_sets,
            self,
            new_state,
            &mut diff,
            &mut processed_uids,
        );
        apply_resource_diff(
            &snapshot_diff.daemon_sets,
            self,
            new_state,
            &mut diff,
            &mut processed_uids,
        );
        apply_resource_diff(
            &snapshot_diff.jobs,
            self,
            new_state,
            &mut diff,
            &mut processed_uids,
        );
        apply_resource_diff(
            &snapshot_diff.ingresses,
            self,
            new_state,
            &mut diff,
            &mut processed_uids,
        );
        apply_resource_diff(
            &snapshot_diff.services,
            self,
            new_state,
            &mut diff,
            &mut processed_uids,
        );
        apply_resource_diff(
            &snapshot_diff.endpoint_slices,
            self,
            new_state,
            &mut diff,
            &mut processed_uids,
        );
        apply_resource_diff(
            &snapshot_diff.network_policies,
            self,
            new_state,
            &mut diff,
            &mut processed_uids,
        );
        apply_resource_diff(
            &snapshot_diff.config_maps,
            self,
            new_state,
            &mut diff,
            &mut processed_uids,
        );
        apply_resource_diff(
            &snapshot_diff.storage_classes,
            self,
            new_state,
            &mut diff,
            &mut processed_uids,
        );
        apply_resource_diff(
            &snapshot_diff.persistent_volumes,
            self,
            new_state,
            &mut diff,
            &mut processed_uids,
        );
        apply_resource_diff(
            &snapshot_diff.persistent_volume_claims,
            self,
            new_state,
            &mut diff,
            &mut processed_uids,
        );
        apply_resource_diff(
            &snapshot_diff.nodes,
            self,
            new_state,
            &mut diff,
            &mut processed_uids,
        );
        apply_resource_diff(
            &snapshot_diff.service_accounts,
            self,
            new_state,
            &mut diff,
            &mut processed_uids,
        );
        apply_resource_diff(
            &snapshot_diff.events,
            self,
            new_state,
            &mut diff,
            &mut processed_uids,
        );

        let old_nodes = self.node_map();
        let new_nodes = new_state.node_map();

        for (uid, node) in &new_nodes {
            if processed_uids.contains(uid) {
                continue;
            }
            match old_nodes.get(uid) {
                None => {
                    diff.added_nodes.push((*node).clone());
                    processed_uids.insert(uid.clone());
                }
                Some(previous) => {
                    if *previous != *node {
                        diff.modified_nodes.push((*node).clone());
                        processed_uids.insert(uid.clone());
                    }
                }
            }
        }

        for (uid, node) in &old_nodes {
            if processed_uids.contains(uid) {
                continue;
            }
            if !new_nodes.contains_key(uid) {
                diff.removed_nodes.push((*node).clone());
                processed_uids.insert(uid.clone());
            }
        }

        let old_edges: HashSet<GraphEdge> = self.get_edges().collect();
        let new_edges: HashSet<GraphEdge> = new_state.get_edges().collect();

        for edge in new_edges.difference(&old_edges) {
            diff.added_edges.push(edge.clone());
        }
        for edge in old_edges.difference(&new_edges) {
            diff.removed_edges.push(edge.clone());
        }

        diff
    }
}

pub type SharedClusterState = Arc<Mutex<ClusterState>>;
