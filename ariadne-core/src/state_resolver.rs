use crate::prelude::*;

use crate::create_generic_object;
use crate::state::{ClusterState, Edge};
use crate::types::*;
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::core::v1::{
    Endpoints, Node, PersistentVolume, PersistentVolumeClaim, Pod, Service,
};
use k8s_openapi::api::networking::v1::Ingress;
use k8s_openapi::Resource;
use kube::api::ListParams;
use kube::config::KubeConfigOptions;
use kube::{Api, Client, Config, ResourceExt};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs;
use std::sync::LazyLock;

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
    should_export_snapshot: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct ClusterSnapshot {
    nodes: Vec<Node>,
    pods: Vec<Pod>,
    deployments: Vec<Deployment>,
    stateful_sets: Vec<StatefulSet>,
    replica_sets: Vec<ReplicaSet>,
    daemon_sets: Vec<DaemonSet>,
    persistent_volumes: Vec<PersistentVolume>,
    persistent_volume_claims: Vec<PersistentVolumeClaim>,
    services: Vec<Service>,
    ingresses: Vec<Ingress>,
    endpoints: Vec<Endpoints>,
}

static CLUSTER_STATE: LazyLock<ClusterSnapshot> = LazyLock::new(|| {
    if false {
        let bytes = fs::read("/home/user/Downloads/snapshot_1751206570696.json").unwrap();
        serde_json::from_slice::<ClusterSnapshot>(&bytes).unwrap()
    } else {
        ClusterSnapshot {
            nodes: vec![],
            pods: vec![],
            deployments: vec![],
            stateful_sets: vec![],
            replica_sets: vec![],
            daemon_sets: vec![],
            persistent_volumes: vec![],
            persistent_volume_claims: vec![],
            services: vec![],
            ingresses: vec![],
            endpoints: vec![],
        }
    }
});

impl ClusterStateResolver {
    pub async fn new(options: &KubeConfigOptions, namespace: &str) -> Result<Self> {
        let cfg = Config::from_kubeconfig(options).await?;
        let client = Client::try_from(cfg)?;
        Ok(ClusterStateResolver {
            node_api: Api::all(client.clone()),
            pod_api: Api::namespaced(client.clone(), namespace),
            deployment_api: Api::namespaced(client.clone(), namespace),
            stateful_set_api: Api::namespaced(client.clone(), namespace),
            replica_set_api: Api::namespaced(client.clone(), namespace),
            daemon_set_api: Api::all(client.clone()),
            persistent_volume_api: Api::all(client.clone()),
            persistent_volume_claim_api: Api::all(client.clone()),
            ingress_api: Api::namespaced(client.clone(), namespace),
            service_api: Api::namespaced(client.clone(), namespace),
            endpoints_api: Api::namespaced(client.clone(), namespace),
            should_export_snapshot: false,
        })
    }

    async fn get_snapshot(&self) -> Result<ClusterSnapshot> {
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
        let snapshot = ClusterSnapshot {
            nodes: nodes,
            pods: pods,
            deployments: deployments,
            stateful_sets: stateful_sets,
            replica_sets: replica_sets,
            daemon_sets: daemon_sets,
            persistent_volumes: persistent_volumes,
            persistent_volume_claims: persistent_volume_claims,
            services: services,
            ingresses: ingresses,
            endpoints: endpoints,
        };
        Ok(snapshot)
    }

    pub async fn resolve(&self) -> Result<ClusterState> {
        let snapshot = self.get_snapshot().await?;
        let state = Self::create_state(&snapshot);
        Ok(state)
    }

    fn create_state(snapshot: &ClusterSnapshot) -> ClusterState {
        let mut state = ClusterState::new();
        {
            for item in &snapshot.nodes {
                let node = create_generic_object!(item.clone(), Node, Node, node);
                state.add_node(node);
            }

            for item in &snapshot.pods {
                let node = create_generic_object!(item.clone(), Pod, Pod, pod);
                state.add_node(node);
            }

            for item in &snapshot.deployments {
                let node = create_generic_object!(item.clone(), Deployment, Deployment, deployment);
                state.add_node(node);
            }

            for item in &snapshot.stateful_sets {
                let node =
                    create_generic_object!(item.clone(), StatefulSet, StatefulSet, stateful_set);
                state.add_node(node);
            }

            for item in &snapshot.replica_sets {
                let node =
                    create_generic_object!(item.clone(), ReplicaSet, ReplicaSet, replica_set);
                state.add_node(node);
            }

            for item in &snapshot.daemon_sets {
                let node = create_generic_object!(item.clone(), DaemonSet, DaemonSet, daemon_set);
                state.add_node(node);
            }

            for item in &snapshot.persistent_volumes {
                let node =
                    create_generic_object!(item.clone(), PersistentVolume, PersistentVolume, pv);
                state.add_node(node);
            }

            for item in &snapshot.persistent_volume_claims {
                let node = create_generic_object!(
                    item.clone(),
                    PersistentVolumeClaim,
                    PersistentVolumeClaim,
                    pvc
                );
                state.add_node(node);
            }

            for item in &snapshot.ingresses {
                let node = create_generic_object!(item.clone(), Ingress, Ingress, ingress);
                state.add_node(node);
            }

            for item in &snapshot.services {
                let node = create_generic_object!(item.clone(), Service, Service, service);
                state.add_node(node);
            }

            for item in &snapshot.endpoints {
                let node = create_generic_object!(item.clone(), Endpoints, Endpoints, endpoints);
                state.add_node(node);
            }

            Self::owner_edges(&snapshot, &mut state);

            let mut with_selectors: Vec<(&str, &std::collections::BTreeMap<String, String>)> =
                Vec::new();
            for item in &snapshot.services {
                item.metadata.uid.as_ref().iter().for_each(|uid| {
                    let maybe_selector = item.spec.as_ref().map(|s| s.selector.as_ref()).flatten();
                    maybe_selector.iter().for_each(|tree| {
                        with_selectors.push((uid.as_str(), tree));
                    });
                });
            }

            let pvc_name_to_uid: HashMap<&str, &str> = snapshot
                .persistent_volume_claims
                .iter()
                .filter(|pvc| pvc.metadata.name.is_some() && pvc.metadata.uid.is_some())
                .map(|pvc| {
                    (
                        pvc.metadata.name.as_ref().unwrap().as_str(),
                        pvc.metadata.uid.as_ref().unwrap().as_str(),
                    )
                })
                .collect();

            for pod in &snapshot.pods {
                pod.metadata
                    .uid
                    .clone()
                    .as_ref()
                    .iter()
                    .for_each(|pod_uid| {
                        pod.spec
                            .as_ref()
                            .map(|s| s.volumes.as_ref())
                            .iter()
                            .flatten()
                            .for_each(|volumes| {
                                volumes.iter().for_each(|v| {
                                    v.persistent_volume_claim.as_ref().iter().for_each(|pvc| {
                                        let pvc_uid =
                                            pvc_name_to_uid.get(pvc.claim_name.as_str()).unwrap();
                                        state.add_edge(*pod_uid, pvc_uid, Edge::Claims);
                                    });
                                });
                            });

                        match pod.metadata.labels.as_ref() {
                            None => {}
                            Some(pod_selector) => {
                                for (uid, selector) in &with_selectors {
                                    let is_connected = (*selector).iter().all(|(name, value)| {
                                        pod_selector.get(name).map(|v| v == value).unwrap_or(false)
                                    });
                                    if is_connected {
                                        state.add_edge(*uid, pod_uid.as_str(), Edge::Selects);
                                    }
                                }
                            }
                        }
                    });
            }
            Self::nodes_host_pods(&snapshot.nodes, &snapshot.pods, &mut state);

            Self::pvc_to_pv(&snapshot.persistent_volumes, &mut state);
        }
        state
    }

    fn owner_edges(snapshot: &ClusterSnapshot, mut state: &mut ClusterState) {
        Self::add_owner_edges(&snapshot.pods, &mut state);
        Self::add_owner_edges(&snapshot.replica_sets, &mut state);
        Self::add_owner_edges(&snapshot.stateful_sets, &mut state);
        Self::add_owner_edges(&snapshot.daemon_sets, &mut state);
        Self::add_owner_edges(&snapshot.deployments, &mut state);
        Self::add_owner_edges(&snapshot.endpoints, &mut state);
        Self::add_owner_edges(&snapshot.persistent_volume_claims, &mut state);
        Self::add_owner_edges(&snapshot.ingresses, &mut state);
    }

    fn nodes_host_pods(nodes: &[Node], pods: &[Pod], state: &mut ClusterState) {
        let node_name_to_node = nodes
            .iter()
            .map(|n| {
                (
                    n.metadata.name.as_ref().unwrap().as_str(),
                    n.metadata.uid.as_ref().unwrap().as_str(),
                )
            })
            .collect::<HashMap<&str, &str>>();
        for pod in pods {
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

    fn pvc_to_pv(pvs: &[PersistentVolume], state: &mut ClusterState) {
        for pv in pvs {
            pv.spec.iter().for_each(|spec| {
                spec.claim_ref.iter().for_each(|claim_ref| {
                    claim_ref.uid.iter().for_each(|pvc_id| {
                        state.add_edge(pvc_id, pv.metadata.uid.as_ref().unwrap(), Edge::Binds);
                    });
                });
            });
        }
    }
}
