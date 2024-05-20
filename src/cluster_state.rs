use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::{BTreeMap, HashMap};

use crate::id_gen::{GetNextIdResult, IdGen};
use petgraph::graphmap::DiGraphMap;
use std::sync::{Arc, Mutex};
use tracing::{info, warn};

#[derive(Debug, Serialize, Deserialize, Default, PartialEq, Clone)]
pub enum NodeType {
    #[default]
    None,
    Service,
    Pod,
    Deployment,
}

#[derive(Debug, Serialize, Deserialize, Default, PartialEq, Clone)]
pub struct Label {
    pub name: String,
    pub value: String,
}

impl Label {
    pub fn new(name: String, value: String) -> Self {
        Label { name, value }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct Node {
    pub uid: String,
    pub name: String,
    pub namespace: String,
    pub version: String,
    pub node_type: NodeType,
    pub labels: Vec<Label>,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub enum Edge {
    #[default]
    None,
    Use,
    Own,
}

pub type NodeId = u32;

#[derive(Serialize, Deserialize, Debug)]
pub struct GraphVertex {
    uid: String,
    name: String,
    namespace: String,
    version: String,
    node_type: NodeType,
    labels: Vec<Label>,
}

impl GraphVertex {
    pub fn new(node: &Node) -> Self {
        GraphVertex {
            uid: node.uid.clone(),
            name: node.name.clone(),
            namespace: node.namespace.clone(),
            version: node.version.clone(),
            node_type: node.node_type.clone(),
            labels: node.labels.to_vec(),
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

#[derive(Debug, Default)]
pub struct ClusterState {
    graph: DiGraphMap<NodeId, Edge>,
    id_gen: IdGen,
    id_to_node: HashMap<NodeId, Node>,
}

impl ClusterState {
    pub fn new() -> Self {
        ClusterState {
            graph: DiGraphMap::new(),
            id_gen: IdGen::new(),
            id_to_node: HashMap::new(),
        }
    }

    pub fn add_node(&mut self, node: Node) {
        match self.id_gen.get_next_id(&node.uid) {
            GetNextIdResult::Existing(id) => {
                self.id_to_node.insert(id, node);
            }
            GetNextIdResult::New(new_id) => {
                self.id_to_node.insert(new_id, node);
                self.graph.add_node(new_id);
            }
        }
    }

    pub fn add_edge(&mut self, source: String, target: String, edge: Edge) {
        let maybe_source = self.get_node(&source);
        let maybe_target = self.get_node(&target);

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
        DirectedGraph { vertices, edges }
    }

    fn get_node(&mut self, uid: &str) -> Option<u32> {
        self.id_gen.get_id(uid)
    }
}

pub type SharedClusterState = Arc<Mutex<ClusterState>>;
