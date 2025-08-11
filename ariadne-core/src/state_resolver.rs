use crate::prelude::*;

use crate::create_generic_object;
use crate::kube_client::{CachedKubeClient, KubeClient};
use crate::state::ClusterState;
use crate::types::*;
use chrono::Utc;
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{
    ConfigMap, Namespace, Node, PersistentVolume, PersistentVolumeClaim, Pod, Service,
    ServiceAccount,
};
use k8s_openapi::api::discovery::v1::EndpointSlice;
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
use std::fs;
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, trace, warn};

pub struct ClusterStateResolver {
    cluster_name: String,
    kube_client: Arc<Box<dyn KubeClient>>,
    #[allow(unused)]
    should_export_snapshot: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ClusterSnapshot {
    cluster: Cluster,
    namespaces: Vec<Arc<Namespace>>,
    pods: Vec<Arc<Pod>>,
    containers: Vec<Container>,
    container_logs: Vec<Logs>,
    deployments: Vec<Arc<Deployment>>,
    stateful_sets: Vec<Arc<StatefulSet>>,
    replica_sets: Vec<Arc<ReplicaSet>>,
    daemon_sets: Vec<Arc<DaemonSet>>,
    jobs: Vec<Arc<Job>>,
    ingresses: Vec<Arc<Ingress>>,
    services: Vec<Arc<Service>>,
    endpoint_slices: Vec<Arc<EndpointSlice>>,
    network_policies: Vec<Arc<NetworkPolicy>>,
    config_maps: Vec<Arc<ConfigMap>>,
    storage_classes: Vec<Arc<StorageClass>>,
    persistent_volumes: Vec<Arc<PersistentVolume>>,
    persistent_volume_claims: Vec<Arc<PersistentVolumeClaim>>,
    nodes: Vec<Arc<Node>>,
    service_accounts: Vec<Arc<ServiceAccount>>,
    events: Vec<Arc<Event>>,
}

#[allow(unused)]
static CLUSTER_STATE: std::sync::LazyLock<ClusterSnapshot> = std::sync::LazyLock::new(|| {
    if false {
        let bytes = fs::read("/tmp/snapshot.json").unwrap();
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
            containers: vec![],
            pods: vec![],
            container_logs: vec![],
            deployments: vec![],
            stateful_sets: vec![],
            replica_sets: vec![],
            daemon_sets: vec![],
            jobs: vec![],
            ingresses: vec![],
            services: vec![],
            endpoint_slices: vec![],
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
        let kube_client = CachedKubeClient::new(options, maybe_ns).await?;
        Ok(ClusterStateResolver {
            cluster_name,
            kube_client: Arc::new(Box::new(kube_client)),
            should_export_snapshot: false,
        })
    }

    async fn get_snapshot(&self) -> Result<ClusterSnapshot> {
        let client = self.kube_client.clone();
        let namespaces = client.get_namespaces().await?;
        let events = Self::get_events(&client, namespaces.as_slice()).await;
        let nodes = client
            .get_nodes()
            .await
            .or_else(|_err| Result::Ok(vec![]))?;
        let pods = client.get_pods().await?;
        let containers = Self::get_containers(&pods)?;
        let logs = Self::get_logs(&client, &containers).await;
        let deployments = client.get_deployments().await?;
        let stateful_sets = client.get_stateful_sets().await?;
        let replica_sets = client.get_replica_sets().await?;
        let daemon_sets = client.get_daemon_sets().await?;
        let jobs = client.get_jobs().await?;

        let ingresses = client.get_ingresses().await?;
        let services = client.get_services().await?;
        let endpoint_slices = client.get_endpoint_slices().await?;
        let network_policies = client.get_network_policies().await?;

        let config_maps = client.get_config_maps().await?;

        let storage_classes = self
            .kube_client
            .get_storage_classes()
            .await
            .or_else(|_err| Result::Ok(vec![]))?;
        let persistent_volumes = self
            .kube_client
            .get_persistent_volumes()
            .await
            .or_else(|_err| Result::Ok(vec![]))?;
        let persistent_volume_claims = self
            .kube_client
            .get_persistent_volume_claims()
            .await
            .or_else(|_err| Result::Ok(vec![]))?;

        let service_accounts = client.get_service_accounts().await?;
        let cluster_url = client.get_cluster_url().await?;
        let info = client.apiserver_version().await?;
        let retrieved_at = Utc::now();
        let cluster: Cluster = Cluster::new(
            ObjectIdentifier {
                uid: format!("Cluster:{}", self.cluster_name),
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
            containers,
            container_logs: logs,
            deployments,
            stateful_sets,
            replica_sets,
            daemon_sets,
            jobs,
            ingresses,
            services,
            endpoint_slices,
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

    async fn get_logs(client: &Arc<Box<dyn KubeClient>>, containers: &[Container]) -> Vec<Logs> {
        let mut all_logs: Vec<Logs> = Vec::with_capacity(containers.len());
        let mut handles = Vec::new();

        for c in containers {
            if let (Some(ns), Some(name)) =
                (c.metadata.namespace.as_deref(), c.metadata.name.as_deref())
            {
                let ns = ns.to_string();
                let pod_name = c.pod_name.to_string();
                let container_name = name.to_string();
                let container_uid = c.metadata.uid.as_ref().unwrap().to_string();

                let client = client.clone();
                handles.push(tokio::spawn(async move {
                    match client.get_pod_logs(&ns, pod_name.as_str(), Some(container_name.clone())).await {
                        Ok(content) => Some(Logs::new(&ns, &container_name, &container_uid, content)),
                        Err(err) => {
                            trace!("Unable to fetch the logs for pod {ns}/{pod_name} and container {container_name}: {}", err);
                            None
                        }
                    }
                }));
            }
        }
        for handle in handles {
            if let Ok(Some(logs)) = handle.await {
                all_logs.push(logs);
            }
        }
        all_logs
    }

    async fn get_events(
        client: &Arc<Box<dyn KubeClient>>,
        namespaces: &[Arc<Namespace>],
    ) -> Vec<Arc<Event>> {
        let mut events: Vec<Arc<Event>> = Vec::with_capacity(namespaces.len());
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
        if self.should_export_snapshot {
            let v = serde_json::to_value(&snapshot)?;
            let json = serde_json::to_string_pretty(&v)?;
            let path = "/tmp/snapshot.json".to_string();
            fs::write(path, json)?;
        }

        let state = Self::create_state(&snapshot);
        Ok(state)
    }

    fn create_state(snapshot: &ClusterSnapshot) -> ClusterState {
        let mut state = ClusterState::new(snapshot.cluster.clone());
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
                ResourceType::Namespace,
                cluster_uid.as_str(),
                ResourceType::Cluster,
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
                ResourceType::Pod,
                item.metadata.namespace.as_deref(),
            );
        }
        for item in &snapshot.containers {
            let obj_id = ObjectIdentifier {
                uid: item.metadata.uid.as_ref().unwrap().clone(),
                name: item.metadata.name.as_ref().unwrap().clone(),
                namespace: item.metadata.namespace.clone(),
                resource_version: None,
            };
            state.add_node(GenericObject {
                id: obj_id.clone(),
                resource_type: ResourceType::Container,
                attributes: Some(Box::new(ResourceAttributes::Container {
                    container: item.clone(),
                })),
            });

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                ResourceType::Container,
                item.metadata.namespace.as_deref(),
            );

            let container_uid = item.metadata.uid.as_ref().unwrap().to_string();
            state.add_edge(
                container_uid.as_str(),
                ResourceType::Container,
                item.pod_uid.as_str(),
                ResourceType::Pod,
                Edge::Runs,
            );
        }
        for logs in &snapshot.container_logs {
            let obj_id = ObjectIdentifier {
                uid: logs.metadata.uid.as_ref().unwrap().clone(),
                name: logs.metadata.name.as_ref().unwrap().clone(),
                namespace: logs.metadata.namespace.clone(),
                resource_version: None,
            };
            let container_uid = logs.container_uid.clone();
            state.add_node(GenericObject {
                id: obj_id.clone(),
                resource_type: ResourceType::Logs,
                attributes: Some(Box::new(ResourceAttributes::Logs { logs: logs.clone() })),
            });

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                obj_id.uid.as_str(),
                ResourceType::Logs,
                obj_id.namespace.as_deref(),
            );

            state.add_edge(
                container_uid.as_str(),
                ResourceType::Container,
                obj_id.uid.as_str(),
                ResourceType::Logs,
                Edge::HasLogs,
            );
        }

        for item in &snapshot.deployments {
            let node = create_generic_object!(item.clone(), Deployment, Deployment, deployment);
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                ResourceType::Deployment,
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
                ResourceType::StatefulSet,
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
                ResourceType::ReplicaSet,
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
                ResourceType::DaemonSet,
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
                ResourceType::Job,
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
                ResourceType::Ingress,
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
                ResourceType::Service,
                item.metadata.namespace.as_deref(),
            );
        }
        for item in &snapshot.endpoint_slices {
            let node =
                create_generic_object!(item.clone(), EndpointSlice, EndpointSlice, endpoint_slice);
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                ResourceType::EndpointSlice,
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
                ResourceType::NetworkPolicy,
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
                ResourceType::ConfigMap,
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
                    ResourceType::Provisioner,
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
                ResourceType::StorageClass,
                item.metadata.namespace.as_deref(),
            );

            state.add_edge(
                item.metadata.uid.as_ref().unwrap(),
                ResourceType::StorageClass,
                provisoner,
                ResourceType::Provisioner,
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
                ResourceType::PersistentVolume,
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
                ResourceType::PersistentVolumeClaim,
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
                ResourceType::Node,
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
                ResourceType::ServiceAccount,
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
                                state.add_edge(
                                    pod_uid,
                                    ResourceType::Pod,
                                    pvc_uid,
                                    ResourceType::PersistentVolumeClaim,
                                    Edge::ClaimsVolume,
                                );
                            });
                        });
                    });
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

        Self::endpoint_to_pod(&snapshot.endpoint_slices, &mut state);

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
                    if let Some(kind) = &regarding.kind {
                        match ResourceType::try_new(kind.as_str()) {
                            Ok(regarding_resource_type) => {
                                state.add_edge(
                                    uid,
                                    ResourceType::Event,
                                    regarding_uid,
                                    regarding_resource_type,
                                    Edge::Concerns,
                                );
                            }
                            Err(err) => {
                                warn!(
                                    "Failed to parse resource type from event regarding {:?}: {}",
                                    regarding, err
                                );
                            }
                        }
                    }
                });
            });
        }

        state
    }

    fn set_manages_edge_all(snapshot: &ClusterSnapshot, state: &mut ClusterState) {
        Self::set_manages_edge(&snapshot.pods, ResourceType::Pod, state);
        Self::set_manages_edge(&snapshot.replica_sets, ResourceType::ReplicaSet, state);
        Self::set_manages_edge(&snapshot.stateful_sets, ResourceType::StatefulSet, state);
        Self::set_manages_edge(&snapshot.daemon_sets, ResourceType::DaemonSet, state);
        Self::set_manages_edge(&snapshot.deployments, ResourceType::Deployment, state);
        Self::set_manages_edge(
            &snapshot.endpoint_slices,
            ResourceType::EndpointSlice,
            state,
        );
        Self::set_manages_edge(
            &snapshot.persistent_volume_claims,
            ResourceType::PersistentVolumeClaim,
            state,
        );
        Self::set_manages_edge(&snapshot.ingresses, ResourceType::Ingress, state);
    }

    fn set_runs_on_edge(nodes: &[Arc<Node>], pods: &[Arc<Pod>], state: &mut ClusterState) {
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
                                state.add_edge(
                                    pod_uid,
                                    ResourceType::Pod,
                                    node_uid,
                                    ResourceType::Node,
                                    Edge::RunsOn,
                                );
                            });
                        });
                }
            }
        }
    }

    fn set_manages_edge<T: Resource + ResourceExt>(
        objs: &Vec<Arc<T>>,
        resource_type: ResourceType,
        cluster_state: &mut ClusterState,
    ) {
        for item in objs {
            if let Some(item_uid) = item.uid() {
                for owner in item.owner_references() {
                    match ResourceType::try_new(owner.kind.as_str()) {
                        Ok(owner_resource_type) => {
                            cluster_state.add_edge(
                                owner.uid.as_ref(),
                                owner_resource_type,
                                item_uid.as_ref(),
                                resource_type.clone(),
                                Edge::Manages,
                            );
                        }
                        Err(err) => {
                            warn!(
                                "Unable to parse resource type of {:?} from owner reference: {}",
                                owner, err
                            );
                        }
                    }
                }
            }
        }
    }

    fn pvc_to_pv(
        pvs: &[Arc<PersistentVolume>],
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
                                state.add_edge(
                                    pv_id,
                                    ResourceType::PersistentVolume,
                                    sc_id,
                                    ResourceType::StorageClass,
                                    Edge::UsesStorageClass,
                                );
                            });
                    });

                    spec.claim_ref.as_ref().inspect(|claim_ref| {
                        claim_ref.uid.as_ref().inspect(|pvc_id| {
                            state.add_edge(
                                pvc_id,
                                ResourceType::PersistentVolumeClaim,
                                pv_id,
                                ResourceType::PersistentVolume,
                                Edge::BoundTo,
                            );
                        });
                    });
                });
            });
        }
    }

    fn ingress_to_service(
        ingresses: &[Arc<Ingress>],
        services: &[Arc<Service>],
        state: &mut ClusterState,
    ) {
        let service_name_to_id = Self::name_to_uid(services.iter().map(|s| &s.metadata));
        ingresses.iter().for_each(|ingress| {
            ingress.metadata.uid.as_ref().inspect(|ingress_id| {
                ingress.spec.as_ref().inspect(|spec| {
                    spec.rules.as_ref().inspect(|rules| {
                        rules.iter().for_each(|rule| {
                            rule.host.as_ref().inspect(|host| {
                                let host_uid = format!("Host:{ingress_id}:{host}");
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
                                state.add_edge(
                                    &host_uid,
                                    ResourceType::Host,
                                    ingress_id,
                                    ResourceType::Ingress,
                                    Edge::IsClaimedBy,
                                );
                            });

                            rule.http.as_ref().inspect(|http| {
                                http.paths.iter().for_each(|p| {
                                    p.backend.service.as_ref().inspect(|s| {
                                        let service_name = s.name.as_str();
                                        let ingress_svc_backend_uid = format!(
                                            "IngressServiceBackend:{ingress_id}:{service_name}"
                                        );
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
                                            ResourceType::Ingress,
                                            &ingress_svc_backend_uid,
                                            ResourceType::IngressServiceBackend,
                                            Edge::DefinesBackend,
                                        );

                                        service_name_to_id.get(service_name).inspect(|svc_id| {
                                            state.add_edge(
                                                &ingress_svc_backend_uid,
                                                ResourceType::IngressServiceBackend,
                                                svc_id,
                                                ResourceType::Service,
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

    fn endpoint_to_pod(endpoints_slices: &[Arc<EndpointSlice>], state: &mut ClusterState) {
        endpoints_slices.iter().for_each(|slice| {
            if let Some(endpoint_slice_id) = slice.metadata.uid.as_ref() {
                slice.endpoints.iter().for_each(|endpoint| {
                    let obj_hash = endpoint.get_hash();
                    let endpoint_uid = format!(
                        "Endpoint:{}:{}:{}",
                        endpoint_slice_id, slice.address_type, obj_hash
                    );
                    let endpoint_id = ObjectIdentifier {
                        uid: endpoint_uid.clone(),
                        name: "".to_string(),
                        namespace: slice.metadata.namespace.clone(),
                        resource_version: None,
                    };
                    state.add_node(GenericObject {
                        id: endpoint_id.clone(),
                        resource_type: ResourceType::Endpoint,
                        attributes: Some(Box::new(ResourceAttributes::Endpoint {
                            endpoint: Endpoint::new(&endpoint_id, endpoint.clone()),
                        })),
                    });
                    // (EndpointSlice) -[:ContainsEndpoint]-> (Endpoint)
                    state.add_edge(
                        endpoint_slice_id.as_str(),
                        ResourceType::EndpointSlice,
                        endpoint_uid.as_str(),
                        ResourceType::Endpoint,
                        Edge::ContainsEndpoint,
                    );

                    endpoint.addresses.iter().for_each(|address| {
                        let endpoint_address_uid =
                            format!("EndpointAddress:{endpoint_uid}:{address}");
                        let obj_id = ObjectIdentifier {
                            uid: endpoint_address_uid.clone(),
                            name: address.clone(),
                            namespace: slice.metadata.namespace.clone(),
                            resource_version: None,
                        };
                        state.add_node(GenericObject {
                            id: obj_id.clone(),
                            resource_type: ResourceType::EndpointAddress,
                            attributes: Some(Box::new(ResourceAttributes::EndpointAddress {
                                endpoint_address: EndpointAddress::new(&obj_id, address.clone()),
                            })),
                        });

                        // (Endpoint) -[:HasAddress]-> (EndpointAddress)
                        state.add_edge(
                            endpoint_uid.as_str(),
                            ResourceType::Endpoint,
                            endpoint_address_uid.as_str(),
                            ResourceType::EndpointAddress,
                            Edge::HasAddress,
                        );

                        // (EndpointAddress) -[:ListedIn]-> (EndpointSlice)
                        state.add_edge(
                            endpoint_address_uid.as_str(),
                            ResourceType::EndpointAddress,
                            endpoint_slice_id.as_str(),
                            ResourceType::EndpointSlice,
                            Edge::ListedIn,
                        );

                        endpoint.target_ref.as_ref().inspect(|target_ref| {
                            if let Some(kind) = target_ref.kind.as_ref() {
                                if let Some(pod_uid) = target_ref.uid.as_ref() {
                                    match ResourceType::try_new(kind) {
                                        Ok(resource_type) => {
                                            match resource_type {
                                                ResourceType::Pod => {
                                                    // (EndpointAddress) -[:IsAddressOf]-> (Pod)
                                                    state.add_edge(
                                                        endpoint_address_uid.as_str(),
                                                        ResourceType::EndpointAddress,
                                                        pod_uid,
                                                        ResourceType::Pod,
                                                        Edge::IsAddressOf,
                                                    );
                                                }
                                                resource_type => {
                                                    warn!("Unknown endpoint target kind {} for EndpointSlice [{}]: {}",
                                                        resource_type,
                                                        target_ref.kind.as_deref().unwrap_or(""),
                                                        endpoint_slice_id
                                                    );
                                                }
                                            }
                                        }
                                        Err(err) => {
                                            warn!(
                                                "Failed to parse resource type from endpoint target {:?}: {}",
                                                target_ref, err
                                            );
                                        }
                                    }
                                }
                            }
                        });
                    });
                });
            };
        });
    }

    fn connect_part_of_and_belongs_to(
        state: &mut ClusterState,
        namespace_name_to_uid: &HashMap<&str, &str>,
        cluster_uid: &str,
        item_uid: &str,
        item_resource_type: ResourceType,
        namespace: Option<&str>,
    ) {
        state.add_edge(
            item_uid,
            item_resource_type.clone(),
            cluster_uid,
            ResourceType::Cluster,
            Edge::PartOf,
        );

        namespace.inspect(|ns| {
            namespace_name_to_uid.get(*ns).inspect(|ns_uid| {
                state.add_edge(
                    item_uid,
                    item_resource_type,
                    ns_uid,
                    ResourceType::Namespace,
                    Edge::BelongsTo,
                );
            });
        });
    }

    fn get_containers(pods: &[Arc<Pod>]) -> Result<Vec<Container>> {
        let mut containers: Vec<Container> = Vec::new();
        for pod in pods {
            if let Some(name) = pod.metadata.name.as_ref() {
                if let Some(ns) = pod.metadata.namespace.as_ref() {
                    if let Some(uid) = pod.metadata.uid.as_ref() {
                        if let Some(spec) = pod.spec.as_ref() {
                            if let Some(inits) = spec.init_containers.as_ref() {
                                for c in inits {
                                    let container = Container::new(
                                        ns,
                                        name,
                                        uid,
                                        c.clone(),
                                        ContainerType::Init,
                                    );
                                    containers.push(container);
                                }
                            }
                            for c in &spec.containers {
                                let container = Container::new(
                                    ns,
                                    name,
                                    uid,
                                    c.clone(),
                                    ContainerType::Standard,
                                );
                                containers.push(container);
                            }
                        }
                    }
                }
            }
        }
        Ok(containers)
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
