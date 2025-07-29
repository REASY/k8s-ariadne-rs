use crate::id_gen::{GetNextIdResult, IdGen};
use crate::types::{Edge, GenericObject, ResourceType};
use petgraph::graphmap::DiGraphMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::warn;

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
        let mut vertices: Vec<GraphVertex> = self
            .get_nodes()
            .map(|node| GraphVertex::new(node))
            .collect();
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
            let source = String::from(self.id_gen.get_by_id(from).unwrap());
            let source_resource_type = self.id_to_node.get(&from).unwrap().resource_type.clone();
            let target = String::from(self.id_gen.get_by_id(to).unwrap());
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
}

pub type SharedClusterState = Arc<Mutex<ClusterState>>;
