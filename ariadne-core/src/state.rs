use crate::id_gen::{GetNextIdResult, IdGen};
use crate::types::{GenericObject, ResourceType};
use petgraph::graphmap::DiGraphMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::warn;

#[derive(Debug, Serialize, Deserialize, Clone, Eq, Ord, PartialEq, PartialOrd)]
pub enum Edge {
    Owns,       // e.g., Deployment → Pod
    Selects,    // e.g., Service → Pod via labels
    Hosts,      // e.g., Node → Pod
    Claims,     // e.g., Pod → PersistentVolumeClaim
    Binds,      // e.g., PersistentVolumeClaim → PersistentVolume
    References, // e.g., Pod → ConfigMap/Secret
}

pub type NodeId = u32;

#[derive(Serialize, Deserialize, Debug)]
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

#[derive(Serialize, Deserialize, Debug)]
pub struct GraphEdge {
    source: String,
    target: String,
    edge_type: Edge,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DirectedGraph {
    vertices: Vec<GraphVertex>,
    edges: Vec<GraphEdge>,
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
            attributes: Box::new(ResourceAttributes::$variant {
                $field: $item.clone(),
            }),
        }
    };
}

#[derive(Debug, Default)]
pub struct ClusterState {
    graph: DiGraphMap<NodeId, Edge>,
    id_gen: IdGen,
    id_to_node: HashMap<NodeId, GenericObject>,
}

impl ClusterState {
    pub fn new() -> Self {
        ClusterState {
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

    pub fn add_edge(&mut self, source: &str, target: &str, edge: Edge) {
        let maybe_source = self.get_node(source);
        let maybe_target = self.get_node(target);

        match (maybe_source, maybe_target) {
            (Some(from), Some(to)) => {
                self.graph.add_edge(from, to, edge);
            }
            (from_id, to_id) => {
                warn!("Node(s) do not exist, source: {source}, from_id: {from_id:?}, target: {target}, to_id: {to_id:?}, edge: {edge:?}")
            }
        }
    }

    pub fn to_directed_graph(&self) -> DirectedGraph {
        let mut vertices: Vec<GraphVertex> = Vec::with_capacity(self.graph.node_count());
        self.graph.nodes().for_each(|vertex_id| {
            let node = self.id_to_node.get(&vertex_id).unwrap();
            vertices.push(GraphVertex::new(node));
        });
        vertices.sort_by_key(|v| v.id.clone());

        let mut edges: Vec<GraphEdge> = Vec::with_capacity(self.graph.edge_count());
        self.graph.all_edges().for_each(|(from, to, t)| {
            let from = String::from(self.id_gen.get_by_id(from).unwrap());
            let to = String::from(self.id_gen.get_by_id(to).unwrap());
            edges.push(GraphEdge {
                source: from,
                target: to,
                edge_type: t.clone(),
            });
        });
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
}

pub type SharedClusterState = Arc<Mutex<ClusterState>>;
