use crate::prelude::*;

use crate::create_generic_object;
use crate::kube_client::{KubeClient, KubeClientImpl};
use crate::state::ClusterState;
use crate::types::*;
use chrono::Utc;
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{
    ConfigMap, Endpoints, Namespace, Node, PersistentVolume, PersistentVolumeClaim, Pod, Service,
    ServiceAccount,
};
use k8s_openapi::api::events::v1::Event;
use k8s_openapi::api::networking::v1::{Ingress, NetworkPolicy};
use k8s_openapi::api::storage::v1::StorageClass;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use k8s_openapi::Resource;
use kube::config::KubeConfigOptions;
use kube::ResourceExt;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, warn};

pub struct ClusterStateResolver {
    cluster_name: String,
    kube_client: Arc<Box<dyn KubeClient>>,
    #[allow(unused)]
    should_export_snapshot: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct ClusterSnapshot {
    cluster: Cluster,
    namespaces: Vec<Namespace>,
    pods: Vec<Pod>,
    pods_logs: Vec<Option<Logs>>,
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
    events: Vec<Event>,
}

#[allow(unused)]
static CLUSTER_STATE: std::sync::LazyLock<ClusterSnapshot> = std::sync::LazyLock::new(|| {
    if true {
        let bytes = std::fs::read("/home/user/Downloads/snapshot.json").unwrap();
        serde_json::from_slice::<ClusterSnapshot>(&bytes).unwrap()
    } else {
        ClusterSnapshot {
            cluster: Cluster {
                metadata: Default::default(),
                name: "".to_string(),
                cluster_url: "".to_string(),
                info: Default::default(),
                retrieved_at: Default::default(),
            },
            namespaces: vec![],
            pods: vec![],
            pods_logs: vec![],
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
            events: vec![],
        }
    }
});

impl ClusterStateResolver {
    pub async fn new(
        cluster_name: String,
        options: &KubeConfigOptions,
        maybe_ns: Option<&str>,
    ) -> Result<Self> {
        let kube_client = KubeClientImpl::new(options, maybe_ns).await?;
        Ok(ClusterStateResolver {
            cluster_name,
            kube_client: Arc::new(Box::new(kube_client)),
            should_export_snapshot: false,
        })
    }

    async fn get_snapshot(&self) -> Result<ClusterSnapshot> {
        let client = self.kube_client.clone();
        let namespaces: Vec<Namespace> = client.get_namespaces().await?;
        let events: Vec<Event> = Self::get_events(&client, namespaces.as_slice()).await;
        let nodes: Vec<Node> = client
            .get_nodes()
            .await
            .or_else(|_err| Result::Ok(vec![]))?;
        let pods: Vec<Pod> = client.get_pods().await?;
        let logs: Vec<Option<Logs>> = Self::get_logs(&client, &pods).await;
        let deployments: Vec<Deployment> = client.get_deployments().await?;
        let stateful_sets: Vec<StatefulSet> = client.get_stateful_sets().await?;
        let replica_sets: Vec<ReplicaSet> = client.get_replica_sets().await?;
        let daemon_sets: Vec<DaemonSet> = client.get_daemon_sets().await?;
        let jobs: Vec<Job> = client.get_jobs().await?;

        let ingresses: Vec<Ingress> = client.get_ingresses().await?;
        let services: Vec<Service> = client.get_services().await?;
        let endpoints: Vec<Endpoints> = client.get_endpoints().await?;
        let network_policies: Vec<NetworkPolicy> = client.get_network_policies().await?;

        let config_maps: Vec<ConfigMap> = client.get_config_maps().await?;

        let storage_classes: Vec<StorageClass> = self
            .kube_client
            .get_storage_classes()
            .await
            .or_else(|_err| Result::Ok(vec![]))?;
        let persistent_volumes: Vec<PersistentVolume> = self
            .kube_client
            .get_persistent_volumes()
            .await
            .or_else(|_err| Result::Ok(vec![]))?;
        let persistent_volume_claims: Vec<PersistentVolumeClaim> = self
            .kube_client
            .get_persistent_volume_claims()
            .await
            .or_else(|_err| Result::Ok(vec![]))?;

        let service_accounts: Vec<ServiceAccount> = client.get_service_accounts().await?;
        let cluster_url = client.get_cluster_url().await?;
        let info = client.apiserver_version().await?;
        let retrieved_at = Utc::now();
        let cluster: Cluster = Cluster::new(
            ObjectIdentifier {
                uid: format!("cluster_{}", self.cluster_name),
                name: self.cluster_name.to_string(),
                namespace: None,
                resource_version: None,
            },
            cluster_url.as_ref(),
            info,
            retrieved_at,
        );

        let snapshot = ClusterSnapshot {
            cluster,
            namespaces,
            pods,
            pods_logs: logs,
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
            events,
        };
        Ok(snapshot)
    }

    async fn get_logs(client: &Arc<Box<dyn KubeClient>>, pods: &[Pod]) -> Vec<Option<Logs>> {
        let mut logs: Vec<Option<Logs>> = Vec::with_capacity(pods.len());
        let mut handles = Vec::new();

        for p in pods {
            if let (Some(ns), Some(name)) =
                (p.metadata.namespace.as_deref(), p.metadata.name.as_deref())
            {
                let ns = ns.to_string();
                let name = name.to_string();
                let pod_uid = p.metadata.uid.as_ref().unwrap().to_string();

                let client = client.clone();
                handles.push(tokio::spawn(async move {
                    match client.get_pod_logs(&ns, &name, None).await {
                        Ok(content) => Some(Logs::new(&ns, &name, &pod_uid, content)),
                        Err(err) => {
                            warn!("Unable to fetch the logs for pod {ns}/{name}: {}", err);
                            None
                        }
                    }
                }));
            }
        }
        for handle in handles {
            logs.push(handle.await.unwrap_or(None));
        }
        logs
    }

    async fn get_events(client: &Arc<Box<dyn KubeClient>>, namespaces: &[Namespace]) -> Vec<Event> {
        let mut events: Vec<Event> = Vec::with_capacity(namespaces.len());
        let mut handles = Vec::new();

        for p in namespaces {
            if let Some(ns) = p.metadata.name.as_deref() {
                let ns = ns.to_string();
                let client = client.clone();
                handles.push(tokio::spawn(
                    async move { (client.get_events(&ns).await).ok() },
                ));
            }
        }
        for handle in handles {
            match handle.await.unwrap_or(None) {
                None => {}
                Some(mut this_events) => {
                    events.append(&mut this_events);
                }
            }
        }
        events
    }

    pub async fn resolve(&self) -> Result<ClusterState> {
        let s = Instant::now();
        let snapshot = self.get_snapshot().await?;
        info!("Retrieved snapshot in {}ms", s.elapsed().as_millis());

        let state = Self::create_state(&snapshot);
        Ok(state)
    }

    fn create_state(snapshot: &ClusterSnapshot) -> ClusterState {
        let mut state = ClusterState::new();
        let cluster_uid: String = {
            let obj_id = ObjectIdentifier {
                uid: snapshot.cluster.metadata.uid.as_ref().unwrap().to_string(),
                name: snapshot.cluster.metadata.name.as_ref().unwrap().to_string(),
                namespace: None,
                resource_version: None,
            };
            let cluster_node = GenericObject {
                id: obj_id.clone(),
                resource_type: ResourceType::Cluster,
                attributes: Some(Box::new(ResourceAttributes::Cluster {
                    cluster: snapshot.cluster.clone(),
                })),
            };
            state.add_node(cluster_node);
            obj_id.uid.clone()
        };

        // Namespaces
        for item in &snapshot.namespaces {
            let node = create_generic_object!(item.clone(), Namespace, Namespace, namespace);
            state.add_node(node);

            state.add_edge(
                item.metadata.uid.as_ref().unwrap(),
                cluster_uid.as_str(),
                Edge::PartOf,
            );
        }
        let namespace_name_to_uid: HashMap<&str, &str> =
            Self::name_to_uid(snapshot.namespaces.iter().map(|x| &x.metadata));

        // Core Workloads
        for item in &snapshot.pods {
            let node = create_generic_object!(item.clone(), Pod, Pod, pod);
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                item.metadata.namespace.as_deref(),
            );
        }

        for logs in snapshot.pods_logs.clone().into_iter().flatten() {
            let obj_id = ObjectIdentifier {
                uid: logs.metadata.uid.as_ref().unwrap().clone(),
                name: logs.metadata.name.as_ref().unwrap().clone(),
                namespace: logs.metadata.namespace.clone(),
                resource_version: None,
            };
            let pod_uid = logs.pod_uid.clone();
            state.add_node(GenericObject {
                id: obj_id.clone(),
                resource_type: ResourceType::Logs,
                attributes: Some(Box::new(ResourceAttributes::Logs { logs })),
            });
            state.add_edge(pod_uid.as_str(), obj_id.uid.as_str(), Edge::HasLogs);
        }

        for item in &snapshot.deployments {
            let node = create_generic_object!(item.clone(), Deployment, Deployment, deployment);
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                item.metadata.namespace.as_deref(),
            );
        }
        for item in &snapshot.stateful_sets {
            let node = create_generic_object!(item.clone(), StatefulSet, StatefulSet, stateful_set);
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                item.metadata.namespace.as_deref(),
            );
        }
        for item in &snapshot.replica_sets {
            let node = create_generic_object!(item.clone(), ReplicaSet, ReplicaSet, replica_set);
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                item.metadata.namespace.as_deref(),
            );
        }
        for item in &snapshot.daemon_sets {
            let node = create_generic_object!(item.clone(), DaemonSet, DaemonSet, daemon_set);
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                item.metadata.namespace.as_deref(),
            );
        }
        for item in &snapshot.jobs {
            let node = create_generic_object!(item.clone(), Job, Job, job);
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                item.metadata.namespace.as_deref(),
            );
        }

        // Networking & Discovery
        for item in &snapshot.ingresses {
            let node = create_generic_object!(item.clone(), Ingress, Ingress, ingress);
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                item.metadata.namespace.as_deref(),
            );
        }
        for item in &snapshot.services {
            let node = create_generic_object!(item.clone(), Service, Service, service);
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                item.metadata.namespace.as_deref(),
            );
        }
        for item in &snapshot.endpoints {
            let node = create_generic_object!(item.clone(), Endpoints, Endpoints, endpoints);
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                item.metadata.namespace.as_deref(),
            );
        }
        for item in &snapshot.network_policies {
            let node =
                create_generic_object!(item.clone(), NetworkPolicy, NetworkPolicy, network_policy);
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                item.metadata.namespace.as_deref(),
            );
        }

        // Configuration
        for item in &snapshot.config_maps {
            let node = create_generic_object!(item.clone(), ConfigMap, ConfigMap, config_map);
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                item.metadata.namespace.as_deref(),
            );
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

                Self::connect_part_of_and_belongs_to(
                    &mut state,
                    &namespace_name_to_uid,
                    cluster_uid.as_str(),
                    obj_id.uid.as_str(),
                    obj_id.namespace.as_deref(),
                );
            }
            let node =
                create_generic_object!(item.clone(), StorageClass, StorageClass, storage_class);
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                item.metadata.namespace.as_deref(),
            );

            state.add_edge(
                item.metadata.uid.as_ref().unwrap(),
                provisoner,
                Edge::UsesProvisioner,
            );
        }
        for item in &snapshot.persistent_volumes {
            let node = create_generic_object!(item.clone(), PersistentVolume, PersistentVolume, pv);
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                item.metadata.namespace.as_deref(),
            );
        }
        for item in &snapshot.persistent_volume_claims {
            let node = create_generic_object!(
                item.clone(),
                PersistentVolumeClaim,
                PersistentVolumeClaim,
                pvc
            );
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                item.metadata.namespace.as_deref(),
            );
        }

        // Cluster Infrastructure
        for item in &snapshot.nodes {
            let node = create_generic_object!(item.clone(), Node, Node, node);
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                item.metadata.namespace.as_deref(),
            );
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

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                item.metadata.namespace.as_deref(),
            );
        }

        Self::set_manages_edge_all(snapshot, &mut state);

        let mut service_selectors: Vec<(&str, &std::collections::BTreeMap<String, String>)> =
            Vec::new();
        for item in &snapshot.services {
            item.metadata.uid.as_ref().inspect(|uid| {
                let maybe_selector = item.spec.as_ref().and_then(|s| s.selector.as_ref());
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
                                    .unwrap_or_else(|| panic!("PVC `{claim_name}` not found"));
                                state.add_edge(pod_uid, pvc_uid, Edge::ClaimsVolume);
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
                                state.add_edge(uid, pod_uid.as_str(), Edge::Selects);
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

        for item in &snapshot.events {
            item.metadata.uid.as_ref().inspect(|uid| {
                state.add_node(GenericObject {
                    id: ObjectIdentifier {
                        uid: uid.to_string(),
                        name: item.metadata.name.as_ref().unwrap().clone(),
                        namespace: item.metadata.namespace.clone(),
                        resource_version: None,
                    },
                    resource_type: ResourceType::Event,
                    attributes: Some(Box::new(ResourceAttributes::Event {
                        event: item.clone(),
                    })),
                })
            });

            let uid = item.metadata.uid.as_ref().unwrap();
            item.regarding.as_ref().inspect(|regarding| {
                regarding.uid.as_ref().inspect(|regarding_uid| {
                    state.add_edge(uid, regarding_uid, Edge::Concerns);
                });
            });
        }

        state
    }

    fn set_manages_edge_all(snapshot: &ClusterSnapshot, state: &mut ClusterState) {
        Self::set_manages_edge(&snapshot.pods, state);
        Self::set_manages_edge(&snapshot.replica_sets, state);
        Self::set_manages_edge(&snapshot.stateful_sets, state);
        Self::set_manages_edge(&snapshot.daemon_sets, state);
        Self::set_manages_edge(&snapshot.deployments, state);
        Self::set_manages_edge(&snapshot.endpoints, state);
        Self::set_manages_edge(&snapshot.persistent_volume_claims, state);
        Self::set_manages_edge(&snapshot.ingresses, state);
    }

    fn set_runs_on_edge(nodes: &[Node], pods: &[Pod], state: &mut ClusterState) {
        let node_name_to_node = Self::name_to_uid(nodes.iter().map(|n| &n.metadata));
        for pod in pods {
            let node_uid = pod.spec.as_ref().and_then(|s| s.node_name.as_deref());
            match node_uid {
                None => {}
                Some(node_name) => {
                    node_name_to_node
                        .get(node_name)
                        .as_ref()
                        .inspect(|node_uid| {
                            pod.metadata.uid.as_deref().inspect(|pod_uid| {
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
                                let host_uid = format!("{ingress_id}_{host}");
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
                                            format!("{ingress_id}_{service_name}");
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
                                                        IngressServiceBackend::new(&obj_id, s),
                                                },
                                            )),
                                        });
                                        state.add_edge(
                                            ingress_id,
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
                                                &obj_id, address,
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

    fn connect_part_of_and_belongs_to(
        state: &mut ClusterState,
        namespace_name_to_uid: &HashMap<&str, &str>,
        cluster_uid: &str,
        item_uid: &str,
        namespace: Option<&str>,
    ) {
        state.add_edge(item_uid, cluster_uid, Edge::PartOf);

        namespace.inspect(|ns| {
            namespace_name_to_uid.get(*ns).inspect(|ns_uid| {
                state.add_edge(item_uid, ns_uid, Edge::BelongsTo);
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
