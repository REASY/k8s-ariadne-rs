use crate::prelude::*;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;

use crate::id_gen::{GetNextIdResult, IdGen};
use crate::types::*;
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::core::v1::{
    Endpoints, Node, PersistentVolume, PersistentVolumeClaim, Pod, Service,
};
use k8s_openapi::api::networking::v1::Ingress;
use k8s_openapi::Resource;
use kube::api::ListParams;
use kube::{Api, Client, ResourceExt};
use petgraph::graphmap::DiGraphMap;
use serde::de::DeserializeOwned;
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

pub struct ClusterStateResolver {
    node_api: Api<Node>,
    pod_api: Api<Pod>,
    deployment_api: Api<Deployment>,
    stateful_set_api: Api<StatefulSet>,
    replica_set_api: Api<ReplicaSet>,
    daemon_set_api: Api<DaemonSet>,
    persistent_volume_api: Api<PersistentVolume>,
    persistent_volume_claim_api: Api<PersistentVolumeClaim>,
    ingress_api: Api<Ingress>,
    service_api: Api<Service>,
    endpoints_api: Api<Endpoints>,
}

impl ClusterStateResolver {
    pub async fn new() -> Result<Self> {
        let client = Client::try_default().await?;
        Ok(ClusterStateResolver {
            node_api: Api::all(client.clone()),
            pod_api: Api::default_namespaced(client.clone()),
            deployment_api: Api::default_namespaced(client.clone()),
            stateful_set_api: Api::default_namespaced(client.clone()),
            replica_set_api: Api::default_namespaced(client.clone()),
            daemon_set_api: Api::all(client.clone()),
            persistent_volume_api: Api::all(client.clone()),
            persistent_volume_claim_api: Api::all(client.clone()),
            ingress_api: Api::default_namespaced(client.clone()),
            service_api: Api::default_namespaced(client.clone()),
            endpoints_api: Api::default_namespaced(client.clone()),
        })
    }

    pub async fn resolve(&self) -> Result<ClusterState> {
        let nodes: Vec<Node> = Self::get_object(&self.node_api).await?;
        let pods: Vec<Pod> = Self::get_object(&self.pod_api).await?;
        let deployments: Vec<Deployment> = Self::get_object(&self.deployment_api).await?;
        let stateful_sets: Vec<StatefulSet> = Self::get_object(&self.stateful_set_api).await?;
        let replica_sets: Vec<ReplicaSet> = Self::get_object(&self.replica_set_api).await?;
        let daemon_sets: Vec<DaemonSet> = Self::get_object(&self.daemon_set_api).await?;

        let persistent_volumes: Vec<PersistentVolume> =
            Self::get_object(&self.persistent_volume_api).await?;
        let persistent_volume_claims: Vec<PersistentVolumeClaim> =
            Self::get_object(&self.persistent_volume_claim_api).await?;

        let services: Vec<Service> = Self::get_object(&self.service_api).await?;
        let ingresses: Vec<Ingress> = Self::get_object(&self.ingress_api).await?;
        let endpoints: Vec<Endpoints> = Self::get_object(&self.endpoints_api).await?;

        let mut state = ClusterState::new();
        {
            for item in &nodes {
                let node = create_generic_object!(item.clone(), Node, Node, node);
                state.add_node(node);
            }

            for item in &pods {
                let node = create_generic_object!(item.clone(), Pod, Pod, pod);
                state.add_node(node);
            }

            for item in &deployments {
                let node = create_generic_object!(item.clone(), Deployment, Deployment, deployment);
                state.add_node(node);
            }

            for item in &stateful_sets {
                let node =
                    create_generic_object!(item.clone(), StatefulSet, StatefulSet, stateful_set);
                state.add_node(node);
            }

            for item in &replica_sets {
                let node =
                    create_generic_object!(item.clone(), ReplicaSet, ReplicaSet, replica_set);
                state.add_node(node);
            }

            for item in &daemon_sets {
                let node = create_generic_object!(item.clone(), DaemonSet, DaemonSet, daemon_set);
                state.add_node(node);
            }

            for item in &persistent_volumes {
                let node =
                    create_generic_object!(item.clone(), PersistentVolume, PersistentVolume, pv);
                state.add_node(node);
            }

            for item in &persistent_volume_claims {
                let node = create_generic_object!(
                    item.clone(),
                    PersistentVolumeClaim,
                    PersistentVolumeClaim,
                    pvc
                );
                state.add_node(node);
            }

            for item in &ingresses {
                let node = create_generic_object!(item.clone(), Ingress, Ingress, ingress);
                state.add_node(node);
            }

            for item in &services {
                let node = create_generic_object!(item.clone(), Service, Service, service);
                state.add_node(node);
            }

            for item in &endpoints {
                let node = create_generic_object!(item.clone(), Endpoints, Endpoints, endpoints);
                state.add_node(node);
            }

            Self::add_owner_edges(&pods, &mut state);
            Self::add_owner_edges(&replica_sets, &mut state);
            Self::add_owner_edges(&stateful_sets, &mut state);
            Self::add_owner_edges(&daemon_sets, &mut state);
            Self::add_owner_edges(&deployments, &mut state);
            Self::add_owner_edges(&endpoints, &mut state);
            Self::add_owner_edges(&persistent_volume_claims, &mut state);
            Self::add_owner_edges(&ingresses, &mut state);

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
                                        state.add_edge(
                                            svc_uid.as_str(),
                                            pod_uid.as_str(),
                                            Edge::Selects,
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }

            let node_name_to_node = nodes
                .iter()
                .map(|n| {
                    (
                        n.metadata.name.as_ref().unwrap().as_str(),
                        n.metadata.uid.as_ref().unwrap().as_str(),
                    )
                })
                .collect::<HashMap<&str, &str>>();
            for pod in &pods {
                let node_uid = pod
                    .spec
                    .as_ref()
                    .map(|s| s.node_name.as_ref().map(|x| x.as_str()))
                    .flatten();
                match node_uid {
                    None => {}
                    Some(node_name) => {
                        let node_uid = node_name_to_node.get(node_name).unwrap();
                        pod.metadata
                            .uid
                            .as_ref()
                            .map(|x| x.as_str())
                            .iter()
                            .for_each(|pod_uid| {
                                state.add_edge(node_uid, pod_uid, Edge::Hosts);
                            });
                    }
                }
            }
        }

        Ok(state)
    }

    fn add_owner_edges<T: Resource + ResourceExt>(objs: &Vec<T>, cluster_state: &mut ClusterState) {
        for item in objs {
            for owner in item.owner_references() {
                item.uid().iter().for_each(|uid| {
                    cluster_state.add_edge(owner.uid.as_ref(), uid, Edge::Owns);
                });
            }
        }
    }

    async fn get_object<T: Clone + DeserializeOwned + Debug>(api: &Api<T>) -> Result<Vec<T>> {
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
}
