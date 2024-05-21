use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;

use crate::errors;
use crate::id_gen::{GetNextIdResult, IdGen};
use k8s_openapi::api::apps::v1::{Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::core::v1::{Pod, Service};
use k8s_openapi::{kind, Resource};
use kube::api::ListParams;
use kube::{Api, Client, ResourceExt};
use petgraph::graphmap::DiGraphMap;
use serde::de::DeserializeOwned;
use std::sync::{Arc, Mutex};
use async_openai::config::OpenAIConfig;
use tracing::warn;

#[derive(Debug, Serialize, Deserialize, Default, PartialEq, Clone)]
pub enum NodeType {
    #[default]
    None,
    Service,
    Pod,
    Deployment,
    ReplicaSet,
    StatefulSet,
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
    pub status: String,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone, Eq, Ord, PartialEq, PartialOrd)]
pub enum Edge {
    #[default]
    None,
    Connect,
    Own,
}

pub type NodeId = u32;

#[derive(Serialize, Deserialize, Debug)]
pub struct GraphVertex {
    id: String,
    name: String,
    namespace: String,
    version: String,
    node_type: NodeType,
    labels: Vec<Label>,
    status: String,
}

impl GraphVertex {
    pub fn new(node: &Node) -> Self {
        GraphVertex {
            id: node.uid.clone(),
            name: node.name.clone(),
            namespace: node.namespace.clone(),
            version: node.version.clone(),
            node_type: node.node_type.clone(),
            labels: node.labels.to_vec(),
            status: node.status.clone(),
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

pub struct ClusterStateResolver {
    pods: Api<Pod>,
    services: Api<Service>,
    replica_sets: Api<ReplicaSet>,
    stateful_set: Api<StatefulSet>,
    deployments: Api<Deployment>,
    openai_client: async_openai::Client<OpenAIConfig>
}

impl ClusterStateResolver {
    pub async fn new() -> errors::Result<Self> {
        let client = Client::try_default().await?;
        let pods: Api<Pod> = Api::default_namespaced(client.clone());
        let services: Api<Service> = Api::default_namespaced(client.clone());
        let replica_sets: Api<ReplicaSet> = Api::default_namespaced(client.clone());
        let stateful_set: Api<StatefulSet> = Api::default_namespaced(client.clone());
        let deployments: Api<Deployment> = Api::default_namespaced(client.clone());
        let openai_client = async_openai::Client::new();
        Ok(ClusterStateResolver {
            pods,
            services,
            replica_sets,
            stateful_set,
            deployments,
            openai_client
        })
    }

    pub async fn resolve(&self) -> errors::Result<ClusterState> {
        let pods: Vec<Pod> = Self::get_object(&self.pods).await?;
        let services = Self::get_object(&self.services).await?;
        let replica_sets = Self::get_object(&self.replica_sets).await?;
        let stateful_sets = Self::get_object(&self.stateful_set).await?;
        let deployments = Self::get_object(&self.deployments).await?;
        let mut state = ClusterState::new();

        {
            for item in &pods {
                let status = item
                    .status
                    .as_ref()
                    .map(|x| x.phase.clone())
                    .flatten()
                    .unwrap_or_default()
                    .clone();
                let node = Self::create_node(item, status.clone());
                state.add_node(node);
            }

            for item in &services {
                let status = item
                    .spec
                    .as_ref()
                    .map(|s| {
                        format!(
                            "{} {}",
                            s.type_.clone().unwrap_or_default(),
                            s.cluster_ip.clone().unwrap_or_default()
                        )
                    })
                    .unwrap_or_default();
                let node = Self::create_node(item, status);
                state.add_node(node);
            }

            for item in &replica_sets {
                let status = item
                    .status
                    .as_ref()
                    .map(|x| format!("{}/{}", x.ready_replicas.unwrap_or_default(), x.replicas))
                    .unwrap_or_default();
                let node = Self::create_node(item, status);
                state.add_node(node);
            }

            for item in &stateful_sets {
                let status = item
                    .status
                    .as_ref()
                    .map(|x| format!("{}/{}", x.current_replicas.unwrap_or_default(), x.replicas))
                    .unwrap_or_default();
                let node = Self::create_node(item, status);
                state.add_node(node);
            }

            for item in &deployments {
                let status = item
                    .status
                    .as_ref()
                    .map(|x| x.conditions.as_ref().map(|t| t.last()))
                    .flatten()
                    .flatten()
                    .map(|c| c.type_.clone())
                    .unwrap_or_default();
                let node = Self::create_node(item, status);
                state.add_node(node);
            }

            Self::add_owner_edges(&pods, &mut state);
            Self::add_owner_edges(&replica_sets, &mut state);
            Self::add_owner_edges(&stateful_sets, &mut state);
            Self::add_owner_edges(&deployments, &mut state);
            Self::add_owner_edges(&services, &mut state);

            for item in &services {
                let maybe_selector = item.spec.as_ref().map(|s| s.selector.as_ref()).flatten();
                match maybe_selector {
                    None => {}
                    Some(selector) => {
                        for pod in &pods {
                            match pod.metadata.labels.as_ref() {
                                None => {}
                                Some(pod_selector) => {
                                    let is_connected = selector.iter().all(|(name, value)| {
                                        pod_selector.get(name).map(|v| v == value).unwrap_or(false)
                                    });
                                    if is_connected {
                                        let svc_uid = item.metadata.uid.clone().unwrap_or_default();
                                        let pod_uid = pod.metadata.uid.clone().unwrap_or_default();
                                        state.add_edge(svc_uid, pod_uid, Edge::Connect);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(state)
    }

    fn add_owner_edges<T: Resource + k8s_openapi::Resource + ResourceExt>(
        objs: &Vec<T>,
        cluster_state: &mut ClusterState,
    ) {
        for item in objs {
            for owner in item.owner_references() {
                cluster_state.add_edge(owner.uid.clone(), item.uid().unwrap(), Edge::Own);
            }
        }
    }

    async fn get_object<T: Clone + DeserializeOwned + Debug>(
        api: &Api<T>,
    ) -> errors::Result<Vec<T>> {
        let mut r: Vec<T> = Vec::new();
        let mut continue_token: Option<String> = None;
        loop {
            let lp = match continue_token {
                None => ListParams::default(),
                Some(t) => ListParams::default().continue_token(&t),
            };
            let pods = api.list(&lp).await?;
            continue_token = pods.metadata.continue_.clone();

            for p in pods {
                r.push(p)
            }
            if continue_token.is_none() {
                break;
            }
        }
        Ok(r)
    }

    fn create_node<T: Resource + k8s_openapi::Resource + ResourceExt>(
        p: &T,
        status: String,
    ) -> Node {
        let node_type = match kind(p) {
            Pod::KIND => NodeType::Pod,
            Service::KIND => NodeType::Service,
            Deployment::KIND => NodeType::Deployment,
            ReplicaSet::KIND => NodeType::ReplicaSet,
            StatefulSet::KIND => NodeType::StatefulSet,
            x => {
                warn!("Do not know how to map {x} to NodeType");
                NodeType::None
            }
        };
        let labels: Vec<Label> = p
            .labels()
            .iter()
            .map(|(k, v)| Label::new(k.clone(), v.clone()))
            .collect();
        Node {
            uid: p.uid().unwrap(),
            name: p.name_any(),
            namespace: p.namespace().unwrap(),
            version: p.resource_version().unwrap(),
            node_type: node_type,
            labels,
            status,
        }
    }
}
