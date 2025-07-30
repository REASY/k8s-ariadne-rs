use crate::prelude::*;

use crate::create_generic_object;
use crate::kube_client::{KubeClient, KubeClientImpl};
use crate::state::ClusterState;
use crate::types::*;
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{
    ConfigMap, Endpoints, Node, PersistentVolume, PersistentVolumeClaim, Pod, Service,
    ServiceAccount,
};
use k8s_openapi::api::networking::v1::{Ingress, NetworkPolicy};
use k8s_openapi::api::storage::v1::StorageClass;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use k8s_openapi::Resource;
use kube::config::KubeConfigOptions;
use kube::ResourceExt;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use tracing::log;

pub struct ClusterStateResolver {
    kube_client: Box<dyn KubeClient>,
    #[allow(unused)]
    should_export_snapshot: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct ClusterSnapshot {
    pods: Vec<Pod>,
    deployments: Vec<Deployment>,
    stateful_sets: Vec<StatefulSet>,
    replica_sets: Vec<ReplicaSet>,
    daemon_sets: Vec<DaemonSet>,
    jobs: Vec<Job>,
    ingresses: Vec<Ingress>,
    services: Vec<Service>,
    endpoints: Vec<Endpoints>,
    network_policies: Vec<NetworkPolicy>,
    config_maps: Vec<ConfigMap>,
    storage_classes: Vec<StorageClass>,
    persistent_volumes: Vec<PersistentVolume>,
    persistent_volume_claims: Vec<PersistentVolumeClaim>,
    nodes: Vec<Node>,
    service_accounts: Vec<ServiceAccount>,
}

#[allow(unused)]
static CLUSTER_STATE: std::sync::LazyLock<ClusterSnapshot> = std::sync::LazyLock::new(|| {
    if true {
        let bytes = std::fs::read("/home/user/Downloads/snapshot.json").unwrap();
        serde_json::from_slice::<ClusterSnapshot>(&bytes).unwrap()
    } else {
        ClusterSnapshot {
            pods: vec![],
            deployments: vec![],
            stateful_sets: vec![],
            replica_sets: vec![],
            daemon_sets: vec![],
            jobs: vec![],
            ingresses: vec![],
            services: vec![],
            endpoints: vec![],
            network_policies: vec![],
            config_maps: vec![],
            storage_classes: vec![],
            persistent_volumes: vec![],
            persistent_volume_claims: vec![],
            nodes: vec![],
            service_accounts: vec![],
        }
    }
});

impl ClusterStateResolver {
    pub async fn new(options: &KubeConfigOptions, namespace: &str) -> Result<Self> {
        let kube_client = KubeClientImpl::new(options, namespace).await?;
        Ok(ClusterStateResolver {
            kube_client: Box::new(kube_client),
            should_export_snapshot: false,
        })
    }

    async fn get_snapshot(&self) -> Result<ClusterSnapshot> {
        let nodes: Vec<Node> = self.kube_client.get_nodes().await.or_else(|err| {
            log::error!("Failed to get nodes: {}", err);
            Result::Ok(vec![])
        })?;
        let pods: Vec<Pod> = self.kube_client.get_pods().await?;
        let deployments: Vec<Deployment> = self.kube_client.get_deployments().await?;
        let stateful_sets: Vec<StatefulSet> = self.kube_client.get_stateful_sets().await?;
        let replica_sets: Vec<ReplicaSet> = self.kube_client.get_replica_sets().await?;
        let daemon_sets: Vec<DaemonSet> = self.kube_client.get_daemon_sets().await?;
        let jobs: Vec<Job> = self.kube_client.get_jobs().await?;

        let ingresses: Vec<Ingress> = self.kube_client.get_ingresses().await?;
        let services: Vec<Service> = self.kube_client.get_services().await?;
        let endpoints: Vec<Endpoints> = self.kube_client.get_endpoints().await?;
        let network_policies: Vec<NetworkPolicy> = self.kube_client.get_network_policies().await?;

        let config_maps: Vec<ConfigMap> = self.kube_client.get_config_maps().await?;

        let storage_classes: Vec<StorageClass> = self.kube_client.get_storage_classes().await?;
        let persistent_volumes: Vec<PersistentVolume> = self
            .kube_client
            .get_persistent_volumes()
            .await
            .or_else(|err| {
                log::error!("Failed to get PVs: {}", err);
                Result::Ok(vec![])
            })?;
        let persistent_volume_claims: Vec<PersistentVolumeClaim> =
            self.kube_client.get_persistent_volume_claims().await?;

        let service_accounts: Vec<ServiceAccount> = self.kube_client.get_service_accounts().await?;

        let snapshot = ClusterSnapshot {
            pods,
            deployments,
            stateful_sets,
            replica_sets,
            daemon_sets,
            jobs,
            ingresses,
            services,
            endpoints,
            network_policies,
            config_maps,
            storage_classes,
            persistent_volumes,
            persistent_volume_claims,
            nodes,
            service_accounts,
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
        // Core Workloads
        for item in &snapshot.pods {
            let node = create_generic_object!(item.clone(), Pod, Pod, pod);
            state.add_node(node);
        }
        for item in &snapshot.deployments {
            let node = create_generic_object!(item.clone(), Deployment, Deployment, deployment);
            state.add_node(node);
        }
        for item in &snapshot.stateful_sets {
            let node = create_generic_object!(item.clone(), StatefulSet, StatefulSet, stateful_set);
            state.add_node(node);
        }
        for item in &snapshot.replica_sets {
            let node = create_generic_object!(item.clone(), ReplicaSet, ReplicaSet, replica_set);
            state.add_node(node);
        }
        for item in &snapshot.daemon_sets {
            let node = create_generic_object!(item.clone(), DaemonSet, DaemonSet, daemon_set);
            state.add_node(node);
        }
        for item in &snapshot.jobs {
            let node = create_generic_object!(item.clone(), Job, Job, job);
            state.add_node(node);
        }

        // Networking & Discovery
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
        for item in &snapshot.network_policies {
            let node =
                create_generic_object!(item.clone(), NetworkPolicy, NetworkPolicy, network_policy);
            state.add_node(node);
        }

        // Configuration
        for item in &snapshot.config_maps {
            let node = create_generic_object!(item.clone(), ConfigMap, ConfigMap, config_map);
            state.add_node(node);
        }

        let mut unique_provisoners: HashSet<&str> = HashSet::new();
        // Storage
        for item in &snapshot.storage_classes {
            let provisoner = &item.provisioner;
            if unique_provisoners.insert(&item.provisioner) {
                let obj_id = ObjectIdentifier {
                    uid: provisoner.clone(),
                    name: provisoner.clone(),
                    namespace: item.metadata.namespace.clone(),
                    resource_version: None,
                };
                state.add_node(GenericObject {
                    id: obj_id.clone(),
                    resource_type: ResourceType::Provisioner,
                    attributes: Some(Box::new(ResourceAttributes::Provisioner {
                        provisioner: Provisioner::new(&obj_id, provisoner.as_str()),
                    })),
                });
            }
            let node =
                create_generic_object!(item.clone(), StorageClass, StorageClass, storage_class);
            state.add_node(node);

            state.add_edge(
                item.metadata.uid.as_ref().unwrap(),
                provisoner,
                Edge::UsesProvisioner,
            );
        }
        for item in &snapshot.persistent_volumes {
            let node = create_generic_object!(item.clone(), PersistentVolume, PersistentVolume, pv);
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

        // Cluster Infrastructure
        for item in &snapshot.nodes {
            let node = create_generic_object!(item.clone(), Node, Node, node);
            state.add_node(node);
        }

        // Identity & Access Control
        for item in &snapshot.service_accounts {
            let node = create_generic_object!(
                item.clone(),
                ServiceAccount,
                ServiceAccount,
                service_account
            );
            state.add_node(node);
        }

        Self::set_manages_edge_all(&snapshot, &mut state);

        let mut service_selectors: Vec<(&str, &std::collections::BTreeMap<String, String>)> =
            Vec::new();
        for item in &snapshot.services {
            item.metadata.uid.as_ref().inspect(|uid| {
                let maybe_selector = item.spec.as_ref().map(|s| s.selector.as_ref()).flatten();
                maybe_selector.inspect(|tree| {
                    service_selectors.push((uid.as_str(), tree));
                });
            });
        }

        let pvc_name_to_uid: HashMap<&str, &str> = Self::name_to_uid(
            snapshot
                .persistent_volume_claims
                .iter()
                .map(|x| &x.metadata),
        );

        for pod in &snapshot.pods {
            pod.metadata.uid.as_ref().inspect(|pod_uid| {
                pod.spec
                    .as_ref()
                    .map(|s| s.volumes.as_ref())
                    .iter()
                    .flatten()
                    .for_each(|volumes| {
                        volumes.iter().for_each(|v| {
                            v.persistent_volume_claim.as_ref().inspect(|pvc| {
                                let claim_name = pvc.claim_name.as_str();
                                let pvc_uid = pvc_name_to_uid
                                    .get(claim_name)
                                    .expect(format!("PVC `{}` not found", claim_name).as_str());
                                state.add_edge(*pod_uid, pvc_uid, Edge::ClaimsVolume);
                            });
                        });
                    });

                match pod.metadata.labels.as_ref() {
                    None => {}
                    Some(pod_selector) => {
                        for (uid, selector) in &service_selectors {
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
        Self::set_runs_on_edge(&snapshot.nodes, &snapshot.pods, &mut state);

        let storage_class_name_to_uid: HashMap<&str, &str> =
            Self::name_to_uid(snapshot.storage_classes.iter().map(|x| &x.metadata));
        Self::pvc_to_pv(
            &snapshot.persistent_volumes,
            &storage_class_name_to_uid,
            &mut state,
        );

        Self::ingress_to_service(&snapshot.ingresses, &snapshot.services, &mut state);

        Self::endpoint_to_pod(&snapshot.endpoints, &mut state);
        state
    }

    fn set_manages_edge_all(snapshot: &ClusterSnapshot, mut state: &mut ClusterState) {
        Self::set_manages_edge(&snapshot.pods, &mut state);
        Self::set_manages_edge(&snapshot.replica_sets, &mut state);
        Self::set_manages_edge(&snapshot.stateful_sets, &mut state);
        Self::set_manages_edge(&snapshot.daemon_sets, &mut state);
        Self::set_manages_edge(&snapshot.deployments, &mut state);
        Self::set_manages_edge(&snapshot.endpoints, &mut state);
        Self::set_manages_edge(&snapshot.persistent_volume_claims, &mut state);
        Self::set_manages_edge(&snapshot.ingresses, &mut state);
    }

    fn set_runs_on_edge(nodes: &[Node], pods: &[Pod], state: &mut ClusterState) {
        let node_name_to_node = Self::name_to_uid(nodes.iter().map(|n| &n.metadata));
        for pod in pods {
            let node_uid = pod
                .spec
                .as_ref()
                .map(|s| s.node_name.as_ref().map(|x| x.as_str()))
                .flatten();
            match node_uid {
                None => {}
                Some(node_name) => {
                    node_name_to_node
                        .get(node_name)
                        .as_ref()
                        .inspect(|node_uid| {
                            pod.metadata
                                .uid
                                .as_ref()
                                .map(|x| x.as_str())
                                .inspect(|pod_uid| {
                                    state.add_edge(pod_uid, node_uid, Edge::RunsOn);
                                });
                        });
                }
            }
        }
    }

    fn set_manages_edge<T: Resource + ResourceExt>(
        objs: &Vec<T>,
        cluster_state: &mut ClusterState,
    ) {
        for item in objs {
            for owner in item.owner_references() {
                item.uid().inspect(|uid| {
                    cluster_state.add_edge(owner.uid.as_ref(), uid, Edge::Manages);
                });
            }
        }
    }

    fn pvc_to_pv(
        pvs: &[PersistentVolume],
        storage_class_name_to_uid: &HashMap<&str, &str>,
        state: &mut ClusterState,
    ) {
        for pv in pvs {
            pv.spec.as_ref().inspect(|spec| {
                pv.metadata.uid.as_ref().inspect(|pv_id| {
                    spec.storage_class_name.as_ref().inspect(|sc_name| {
                        storage_class_name_to_uid
                            .get(sc_name.as_str())
                            .inspect(|sc_id| {
                                state.add_edge(pv_id, sc_id, Edge::UsesStorageClass);
                            });
                    });

                    spec.claim_ref.as_ref().inspect(|claim_ref| {
                        claim_ref.uid.as_ref().inspect(|pvc_id| {
                            state.add_edge(pvc_id, pv_id, Edge::BoundTo);
                        });
                    });
                });
            });
        }
    }

    fn ingress_to_service(ingresses: &[Ingress], services: &[Service], state: &mut ClusterState) {
        let service_name_to_id = Self::name_to_uid(services.iter().map(|s| &s.metadata));
        ingresses.iter().for_each(|ingress| {
            ingress.metadata.uid.as_ref().inspect(|ingress_id| {
                ingress.spec.as_ref().inspect(|spec| {
                    spec.rules.as_ref().inspect(|rules| {
                        rules.iter().for_each(|rule| {
                            rule.host.as_ref().inspect(|host| {
                                let host_uid = format!("{}_{}", ingress_id, host);
                                let obj_id = ObjectIdentifier {
                                    uid: host_uid.clone(),
                                    name: (*host).clone(),
                                    namespace: ingress.metadata.namespace.clone(),
                                    resource_version: None,
                                };
                                state.add_node(GenericObject {
                                    id: obj_id.clone(),
                                    resource_type: ResourceType::Host,
                                    attributes: Some(Box::new(ResourceAttributes::Host {
                                        host: Host::new(&obj_id, host),
                                    })),
                                });
                                state.add_edge(&host_uid, ingress_id, Edge::IsClaimedBy);
                            });

                            rule.http.as_ref().inspect(|http| {
                                http.paths.iter().for_each(|p| {
                                    p.backend.service.as_ref().inspect(|s| {
                                        let service_name = s.name.as_str();
                                        let ingress_svc_backend_uid =
                                            format!("{}_{}", ingress_id, service_name);
                                        // Prepare for the edges:
                                        // 1. (Ingress) -[:DefinesBackend]-> (IngressBackend)
                                        // 2. (IngressBackend) [:TargetsService]-> Service
                                        let obj_id = ObjectIdentifier {
                                            uid: ingress_svc_backend_uid.clone(),
                                            name: service_name.to_string(),
                                            namespace: ingress.metadata.namespace.clone(),
                                            resource_version: None,
                                        };

                                        state.add_node(GenericObject {
                                            id: obj_id.clone(),
                                            resource_type: ResourceType::IngressServiceBackend,
                                            attributes: Some(Box::new(
                                                ResourceAttributes::IngressServiceBackend {
                                                    ingress_service_backend:
                                                        IngressServiceBackend::new(&obj_id, &s),
                                                },
                                            )),
                                        });
                                        state.add_edge(
                                            &ingress_id,
                                            &ingress_svc_backend_uid,
                                            Edge::DefinesBackend,
                                        );

                                        service_name_to_id.get(service_name).inspect(|svc_id| {
                                            state.add_edge(
                                                &ingress_svc_backend_uid,
                                                svc_id,
                                                Edge::TargetsService,
                                            );
                                        });
                                    });
                                });
                            });
                        })
                    });
                });
            });
        });
    }

    fn endpoint_to_pod(endpoints_slice: &[Endpoints], state: &mut ClusterState) {
        endpoints_slice.iter().for_each(|endpoints| {
            endpoints.metadata.uid.as_ref().inspect(|endpoints_id| {
                endpoints.subsets.as_ref().inspect(|subsets| {
                    subsets.iter().for_each(|subset| {
                        subset.addresses.iter().for_each(|addresses| {
                            addresses.iter().for_each(|address| {
                                let endpoint_address_uid =
                                    format!("{}_{}", endpoints_id, address.ip.as_str());
                                let obj_id = ObjectIdentifier {
                                    uid: endpoint_address_uid.clone(),
                                    name: address.ip.clone(),
                                    namespace: endpoints.metadata.namespace.clone(),
                                    resource_version: None,
                                };
                                state.add_node(GenericObject {
                                    id: obj_id.clone(),
                                    resource_type: ResourceType::EndpointAddress,
                                    attributes: Some(Box::new(
                                        ResourceAttributes::EndpointAddress {
                                            endpoint_address: EndpointAddress::new(
                                                &obj_id, &address,
                                            ),
                                        },
                                    )),
                                });
                                state.add_edge(&endpoint_address_uid, endpoints_id, Edge::ListedIn);

                                address.target_ref.as_ref().inspect(|target_ref| {
                                    target_ref.uid.as_ref().inspect(|target_id| {
                                        state.add_edge(
                                            &endpoint_address_uid,
                                            target_id,
                                            Edge::IsAddressOf,
                                        );
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });
    }

    fn name_to_uid<'a, I>(items: I) -> HashMap<&'a str, &'a str>
    where
        I: Iterator<Item = &'a ObjectMeta>,
    {
        items
            .filter_map(|n| {
                let name = n.name.as_ref()?.as_str();
                let uid = n.uid.as_ref()?.as_str();
                Some((name, uid))
            })
            .collect()
    }

    #[allow(unused)]
    fn uid_to_name<'a, I>(items: I) -> HashMap<&'a str, &'a str>
    where
        I: Iterator<Item = &'a ObjectMeta>,
    {
        items
            .filter_map(|n| {
                let uid = n.uid.as_ref()?.as_str();
                let name = n.name.as_ref()?.as_str();
                Some((uid, name))
            })
            .collect()
    }
}
