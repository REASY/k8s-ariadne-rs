use crate::prelude::*;

use crate::create_generic_object;
use crate::graph_backend::GraphBackend;
use crate::kube_client::{CachedKubeClient, KubeClient};
use crate::snapshot::{
    write_json_to_dir, write_list_to_dir, SNAPSHOT_CLUSTER_FILE, SNAPSHOT_CONFIG_MAPS_FILE,
    SNAPSHOT_DAEMON_SETS_FILE, SNAPSHOT_DEPLOYMENTS_FILE, SNAPSHOT_ENDPOINT_SLICES_FILE,
    SNAPSHOT_EVENTS_FILE, SNAPSHOT_INGRESSES_FILE, SNAPSHOT_JOBS_FILE, SNAPSHOT_NAMESPACES_FILE,
    SNAPSHOT_NETWORK_POLICIES_FILE, SNAPSHOT_NODES_FILE, SNAPSHOT_PERSISTENT_VOLUMES_FILE,
    SNAPSHOT_PERSISTENT_VOLUME_CLAIMS_FILE, SNAPSHOT_PODS_FILE, SNAPSHOT_REPLICA_SETS_FILE,
    SNAPSHOT_SERVICES_FILE, SNAPSHOT_SERVICE_ACCOUNTS_FILE, SNAPSHOT_STATEFUL_SETS_FILE,
    SNAPSHOT_STORAGE_CLASSES_FILE,
};
use crate::state::ClusterState;
use crate::types::*;
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
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{info, trace, warn};

type IngressDerived = (Vec<Arc<Host>>, Vec<Arc<IngressServiceBackend>>);
type EndpointSliceDerived = (Vec<Arc<Endpoint>>, Vec<Arc<EndpointAddress>>);

pub struct ClusterStateResolver {
    cluster: Cluster,
    kube_client: Arc<Box<dyn KubeClient>>,
    last_snapshot: Arc<Mutex<AugmentedClusterSnapshot>>,
    last_state: Arc<Mutex<ClusterState>>,
    #[allow(unused)]
    should_export_snapshot: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ObservedClusterSnapshot {
    pub cluster: Cluster,
    pub namespaces: Vec<Arc<Namespace>>,
    pub pods: Vec<Arc<Pod>>,
    pub deployments: Vec<Arc<Deployment>>,
    pub stateful_sets: Vec<Arc<StatefulSet>>,
    pub replica_sets: Vec<Arc<ReplicaSet>>,
    pub daemon_sets: Vec<Arc<DaemonSet>>,
    pub jobs: Vec<Arc<Job>>,
    pub ingresses: Vec<Arc<Ingress>>,
    pub services: Vec<Arc<Service>>,
    pub endpoint_slices: Vec<Arc<EndpointSlice>>,
    pub network_policies: Vec<Arc<NetworkPolicy>>,
    pub config_maps: Vec<Arc<ConfigMap>>,
    pub storage_classes: Vec<Arc<StorageClass>>,
    pub persistent_volumes: Vec<Arc<PersistentVolume>>,
    pub persistent_volume_claims: Vec<Arc<PersistentVolumeClaim>>,
    pub nodes: Vec<Arc<Node>>,
    pub service_accounts: Vec<Arc<ServiceAccount>>,
    pub events: Vec<Arc<Event>>,
}

impl ObservedClusterSnapshot {
    fn empty() -> Self {
        ObservedClusterSnapshot {
            cluster: Cluster {
                metadata: Default::default(),
                name: "".to_string(),
                cluster_url: "".to_string(),
                info: Default::default(),
            },
            namespaces: vec![],
            pods: vec![],
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
}

pub struct DerivedClusterSnapshot {
    pub containers: Vec<Arc<Container>>,
    pub hosts: Vec<Arc<Host>>,
    pub ingress_service_backends: Vec<Arc<IngressServiceBackend>>,
    pub endpoints: Vec<Arc<Endpoint>>,
    pub endpoint_addresses: Vec<Arc<EndpointAddress>>,
}

pub struct AugmentedClusterSnapshot {
    pub observed: ObservedClusterSnapshot,
    pub derived: DerivedClusterSnapshot,
}

#[allow(unused)]
static CLUSTER_STATE: std::sync::LazyLock<ObservedClusterSnapshot> =
    std::sync::LazyLock::new(|| {
        if false {
            let bytes = fs::read("/tmp/snapshot.json").unwrap();
            serde_json::from_slice::<ObservedClusterSnapshot>(&bytes).unwrap()
        } else {
            ObservedClusterSnapshot::empty()
        }
    });

impl ClusterStateResolver {
    pub async fn new(
        cluster_name: String,
        options: &KubeConfigOptions,
        maybe_ns: Option<&str>,
    ) -> Result<Self> {
        let kube_client = CachedKubeClient::new(options, maybe_ns).await?;
        Self::new_with_kube_client(cluster_name, Box::new(kube_client)).await
    }

    pub async fn new_with_kube_client(
        cluster_name: String,
        kube_client: Box<dyn KubeClient>,
    ) -> Result<Self> {
        let cluster_url = kube_client.get_cluster_url().await?;
        let info = kube_client.apiserver_version().await?;
        let cluster: Cluster = Cluster::new(
            ObjectIdentifier {
                uid: format!("Cluster:{cluster_name}"),
                name: cluster_name.to_string(),
                namespace: None,
                resource_version: None,
            },
            cluster_url.as_ref(),
            info,
        );
        let kube_client: Arc<Box<dyn KubeClient>> = Arc::new(kube_client);
        let augmented = Self::get_augmented_snapshot(&cluster, kube_client.clone()).await?;

        let last_state = Arc::new(Mutex::new(Self::create_state(&augmented)));
        Ok(ClusterStateResolver {
            cluster,
            kube_client,
            last_snapshot: Arc::new(Mutex::new(augmented)),
            last_state,
            should_export_snapshot: false,
        })
    }

    async fn get_augmented_snapshot(
        cluster: &Cluster,
        kube_client: Arc<Box<dyn KubeClient>>,
    ) -> Result<AugmentedClusterSnapshot> {
        let last_snapshot =
            Self::get_observed_snapshot(cluster.clone(), kube_client.clone()).await?;
        let derived_snapshot = Self::get_derived_snapshot(&last_snapshot)?;
        let augmented = AugmentedClusterSnapshot {
            observed: last_snapshot,
            derived: derived_snapshot,
        };
        Ok(augmented)
    }

    async fn get_observed_snapshot(
        cluster: Cluster,
        client: Arc<Box<dyn KubeClient>>,
    ) -> Result<ObservedClusterSnapshot> {
        let namespaces = client.get_namespaces().await?;
        let events: Vec<Arc<Event>> = client.get_events().await?;
        let nodes = client
            .get_nodes()
            .await
            .or_else(|_err| Result::Ok(vec![]))?;
        let pods = client.get_pods().await?;
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

        let storage_classes = client
            .get_storage_classes()
            .await
            .or_else(|_err| Result::Ok(vec![]))?;
        let persistent_volumes = client
            .get_persistent_volumes()
            .await
            .or_else(|_err| Result::Ok(vec![]))?;
        let persistent_volume_claims = client
            .get_persistent_volume_claims()
            .await
            .or_else(|_err| Result::Ok(vec![]))?;

        let service_accounts = client.get_service_accounts().await?;

        let snapshot = ObservedClusterSnapshot {
            cluster,
            namespaces,
            pods,
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

    fn get_derived_snapshot(snapshot: &ObservedClusterSnapshot) -> Result<DerivedClusterSnapshot> {
        let containers: Vec<Arc<Container>> = Self::get_containers(&snapshot.pods)?;
        let (hosts, ingress_service_backends) =
            Self::get_derived_from_ingress(snapshot.ingresses.as_slice())?;

        let (endpoints, endpoint_addresses) =
            Self::get_derived_from_endpoints_slices(&snapshot.endpoint_slices)?;

        Ok(DerivedClusterSnapshot {
            containers,
            hosts,
            ingress_service_backends,
            endpoints,
            endpoint_addresses,
        })
    }

    pub fn start_diff_loop(
        &self,
        backend: Arc<dyn GraphBackend>,
        token: CancellationToken,
    ) -> JoinHandle<()> {
        let cluster = self.cluster.clone();
        let kube_client = self.kube_client.clone();
        let last_snapshot: Arc<Mutex<AugmentedClusterSnapshot>> = self.last_snapshot.clone();
        let last_state: Arc<Mutex<ClusterState>> = self.last_state.clone();
        let task = tokio::spawn(async move {
            Self::diff_loop(
                cluster,
                kube_client,
                last_snapshot,
                last_state,
                backend,
                token,
            )
            .await
            .expect("Diff loop failed");
        });

        task
    }

    async fn diff_loop(
        cluster: Cluster,
        kube_client: Arc<Box<dyn KubeClient>>,
        last_snapshot: Arc<Mutex<AugmentedClusterSnapshot>>,
        last_state: Arc<Mutex<ClusterState>>,
        backend: Arc<dyn GraphBackend>,
        token: CancellationToken,
    ) -> Result<()> {
        let poll_interval: Duration = Duration::from_secs(5);
        let mut id: usize = 0;
        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    break;
                },
                _ = sleep(poll_interval) => {

                    let current_snapshot =
                        Self::get_augmented_snapshot(&cluster, kube_client.clone()).await?;

                    let new_cluster_state = Self::create_state(&current_snapshot);

                    let previous_snapshot = {
                        let last_snapshot_guard = last_snapshot
                            .lock()
                            .expect("Failed to lock last_snapshot for diff computation");
                        last_snapshot_guard.observed.clone()
                    };

                    let state_diff = {
                        let last_state_guard = last_state
                            .lock()
                            .expect("Failed to lock last_state for diff computation");
                        last_state_guard.diff(
                            &new_cluster_state,
                            &previous_snapshot,
                            &current_snapshot.observed,
                        )
                    };

                    if !state_diff.is_empty() {
                        info!(
                            "Applying diff loop iteration {id}: +{} nodes, -{} nodes, ~{} nodes, +{} edges, -{} edges",
                            state_diff.added_nodes.len(),
                            state_diff.removed_nodes.len(),
                            state_diff.modified_nodes.len(),
                            state_diff.added_edges.len(),
                            state_diff.removed_edges.len(),
                        );
                        backend.update(state_diff).await?;
                    } else {
                        trace!("Diff loop iteration {id}: no changes detected");
                    }

                    {
                        let mut last_state_guard = last_state
                            .lock()
                            .expect("Failed to lock last_state for update");
                        *last_state_guard = new_cluster_state;
                    }

                    {
                        let mut last_snapshot_guard = last_snapshot
                            .lock()
                            .expect("Failed to lock last_snapshot for update");
                        *last_snapshot_guard = current_snapshot;
                    }

                    id += 1;
                },
            }
        }
        info!("Stopped diff_loop, number of loops {id}");
        Ok(())
    }

    #[allow(dead_code)]
    async fn get_logs(
        client: &Arc<Box<dyn KubeClient>>,
        containers: &[Arc<Container>],
    ) -> Vec<Logs> {
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

    pub async fn resolve(&self) -> Result<Arc<Mutex<ClusterState>>> {
        Ok(self.last_state.clone())
    }

    pub fn export_observed_snapshot_dir(&self, dir: impl AsRef<Path>) -> Result<()> {
        let dir = dir.as_ref();
        fs::create_dir_all(dir)?;
        let snapshot = {
            let last_snapshot_guard = self
                .last_snapshot
                .lock()
                .expect("Failed to lock last_snapshot for export");
            last_snapshot_guard.observed.clone()
        };

        write_json_to_dir(dir, SNAPSHOT_CLUSTER_FILE, &snapshot.cluster)?;
        write_list_to_dir(dir, SNAPSHOT_NAMESPACES_FILE, &snapshot.namespaces)?;
        write_list_to_dir(dir, SNAPSHOT_PODS_FILE, &snapshot.pods)?;
        write_list_to_dir(dir, SNAPSHOT_DEPLOYMENTS_FILE, &snapshot.deployments)?;
        write_list_to_dir(dir, SNAPSHOT_STATEFUL_SETS_FILE, &snapshot.stateful_sets)?;
        write_list_to_dir(dir, SNAPSHOT_REPLICA_SETS_FILE, &snapshot.replica_sets)?;
        write_list_to_dir(dir, SNAPSHOT_DAEMON_SETS_FILE, &snapshot.daemon_sets)?;
        write_list_to_dir(dir, SNAPSHOT_JOBS_FILE, &snapshot.jobs)?;
        write_list_to_dir(dir, SNAPSHOT_INGRESSES_FILE, &snapshot.ingresses)?;
        write_list_to_dir(dir, SNAPSHOT_SERVICES_FILE, &snapshot.services)?;
        write_list_to_dir(
            dir,
            SNAPSHOT_ENDPOINT_SLICES_FILE,
            &snapshot.endpoint_slices,
        )?;
        write_list_to_dir(
            dir,
            SNAPSHOT_NETWORK_POLICIES_FILE,
            &snapshot.network_policies,
        )?;
        write_list_to_dir(dir, SNAPSHOT_CONFIG_MAPS_FILE, &snapshot.config_maps)?;
        write_list_to_dir(
            dir,
            SNAPSHOT_STORAGE_CLASSES_FILE,
            &snapshot.storage_classes,
        )?;
        write_list_to_dir(
            dir,
            SNAPSHOT_PERSISTENT_VOLUMES_FILE,
            &snapshot.persistent_volumes,
        )?;
        write_list_to_dir(
            dir,
            SNAPSHOT_PERSISTENT_VOLUME_CLAIMS_FILE,
            &snapshot.persistent_volume_claims,
        )?;
        write_list_to_dir(dir, SNAPSHOT_NODES_FILE, &snapshot.nodes)?;
        write_list_to_dir(
            dir,
            SNAPSHOT_SERVICE_ACCOUNTS_FILE,
            &snapshot.service_accounts,
        )?;
        write_list_to_dir(dir, SNAPSHOT_EVENTS_FILE, &snapshot.events)?;
        Ok(())
    }

    fn create_state(augmented: &AugmentedClusterSnapshot) -> ClusterState {
        let snapshot = &augmented.observed;
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
                    cluster: Box::new(snapshot.cluster.clone()),
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
        for item in &augmented.derived.containers {
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
                        provisioner: Box::new(Provisioner::new(&obj_id, provisoner.as_str())),
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

        Self::ingress_to_service(
            &snapshot.services,
            &augmented.derived.ingress_service_backends,
            &mut state,
        );
        Self::connect_hosts(&augmented.derived.hosts, &mut state);

        Self::endpoint_to_pod(
            &snapshot.endpoint_slices,
            &augmented.derived.endpoints,
            &augmented.derived.endpoint_addresses,
            &mut state,
        );

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

    fn set_manages_edge_all(snapshot: &ObservedClusterSnapshot, state: &mut ClusterState) {
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
        services: &[Arc<Service>],
        ingress_service_backends: &[Arc<IngressServiceBackend>],
        state: &mut ClusterState,
    ) {
        let service_name_to_id = Self::name_to_uid(services.iter().map(|s| &s.metadata));
        for ingress_service_backend in ingress_service_backends {
            // Prepare for the edges:
            // 1. (Ingress) -[:DefinesBackend]-> (IngressBackend)
            // 2. (IngressBackend) [:TargetsService]-> Service
            let obj_id = ObjectIdentifier {
                uid: ingress_service_backend
                    .metadata
                    .uid
                    .as_ref()
                    .unwrap()
                    .clone(),
                name: ingress_service_backend.name.to_string(),
                namespace: ingress_service_backend.metadata.namespace.clone(),
                resource_version: ingress_service_backend.metadata.resource_version.clone(),
            };

            state.add_node(GenericObject {
                id: obj_id.clone(),
                resource_type: ResourceType::IngressServiceBackend,
                attributes: Some(Box::new(ResourceAttributes::IngressServiceBackend {
                    ingress_service_backend: ingress_service_backend.clone(),
                })),
            });
            state.add_edge(
                ingress_service_backend.ingress_uid.as_ref(),
                ResourceType::Ingress,
                &obj_id.uid,
                ResourceType::IngressServiceBackend,
                Edge::DefinesBackend,
            );

            service_name_to_id
                .get(ingress_service_backend.name.as_str())
                .inspect(|svc_id| {
                    state.add_edge(
                        &obj_id.uid,
                        ResourceType::IngressServiceBackend,
                        svc_id,
                        ResourceType::Service,
                        Edge::TargetsService,
                    );
                });
        }
    }

    fn connect_hosts(hosts: &Vec<Arc<Host>>, state: &mut ClusterState) {
        for host in hosts {
            let obj_id = ObjectIdentifier {
                uid: host.metadata.uid.as_ref().unwrap().clone(),
                name: host.name.to_string(),
                namespace: host.metadata.namespace.clone(),
                resource_version: None,
            };
            state.add_node(GenericObject {
                id: obj_id.clone(),
                resource_type: ResourceType::Host,
                attributes: Some(Box::new(ResourceAttributes::Host { host: host.clone() })),
            });
            state.add_edge(
                &obj_id.uid,
                ResourceType::Host,
                host.ingress_uid.as_ref(),
                ResourceType::Ingress,
                Edge::IsClaimedBy,
            );
        }
    }

    fn endpoint_to_pod(
        _endpoints_slices: &[Arc<EndpointSlice>],
        endpoints: &[Arc<Endpoint>],
        endpoint_addresses: &[Arc<EndpointAddress>],
        state: &mut ClusterState,
    ) {
        for endpoint in endpoints {
            let endpoint_uid = endpoint.metadata.uid.as_ref().unwrap().to_string();
            let obj_id = ObjectIdentifier {
                uid: endpoint_uid.clone(),
                name: endpoint.metadata.name.as_ref().unwrap().to_string(),
                namespace: endpoint.metadata.namespace.clone(),
                resource_version: endpoint.metadata.resource_version.clone(),
            };
            state.add_node(GenericObject {
                id: obj_id,
                resource_type: ResourceType::Endpoint,
                attributes: Some(Box::new(ResourceAttributes::Endpoint {
                    endpoint: endpoint.clone(),
                })),
            });
            // (EndpointSlice) -[:ContainsEndpoint]-> (Endpoint)
            state.add_edge(
                endpoint.endpoint_slice_id.as_str(),
                ResourceType::EndpointSlice,
                endpoint_uid.as_str(),
                ResourceType::Endpoint,
                Edge::ContainsEndpoint,
            );
        }

        for endpoint_address in endpoint_addresses {
            let obj_id = ObjectIdentifier {
                uid: endpoint_address.metadata.uid.as_ref().unwrap().to_string(),
                name: endpoint_address.metadata.name.as_ref().unwrap().to_string(),
                namespace: endpoint_address.metadata.namespace.clone(),
                resource_version: endpoint_address.metadata.resource_version.clone(),
            };
            state.add_node(GenericObject {
                id: obj_id.clone(),
                resource_type: ResourceType::EndpointAddress,
                attributes: Some(Box::new(ResourceAttributes::EndpointAddress {
                    endpoint_address: endpoint_address.clone(),
                })),
            });

            let endpoint_address_uid = endpoint_address.metadata.uid.as_ref().unwrap().as_str();

            // (Endpoint) -[:HasAddress]-> (EndpointAddress)
            state.add_edge(
                endpoint_address.endpoint_uid.as_str(),
                ResourceType::Endpoint,
                endpoint_address_uid,
                ResourceType::EndpointAddress,
                Edge::HasAddress,
            );

            // (EndpointAddress) -[:ListedIn]-> (EndpointSlice)
            state.add_edge(
                endpoint_address_uid,
                ResourceType::EndpointAddress,
                endpoint_address.endpoint_slice_uid.as_str(),
                ResourceType::EndpointSlice,
                Edge::ListedIn,
            );

            if let Some(pod_uid) = endpoint_address.pod_uid.as_ref() {
                // (EndpointAddress) -[:IsAddressOf]-> (Pod)
                state.add_edge(
                    endpoint_address_uid,
                    ResourceType::EndpointAddress,
                    pod_uid.as_str(),
                    ResourceType::Pod,
                    Edge::IsAddressOf,
                );
            };
        }
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

    fn get_containers(pods: &[Arc<Pod>]) -> Result<Vec<Arc<Container>>> {
        let mut containers: Vec<Arc<Container>> = Vec::new();
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
                                    containers.push(Arc::new(container));
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
                                containers.push(Arc::new(container));
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

    fn get_derived_from_ingress(ingresses: &[Arc<Ingress>]) -> Result<IngressDerived> {
        let mut hosts: Vec<Arc<Host>> = Vec::new();
        let mut ingress_service_backends: Vec<Arc<IngressServiceBackend>> = Vec::new();

        for ingress in ingresses {
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
                                hosts.push(Arc::new(Host::new(&obj_id, host, ingress_id.as_ref())));
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

                                        ingress_service_backends.push(Arc::new(
                                            IngressServiceBackend::new(
                                                &obj_id,
                                                s,
                                                ingress_id.as_str(),
                                            ),
                                        ));
                                    });
                                });
                            });
                        })
                    });
                });
            });
        }

        Ok((hosts, ingress_service_backends))
    }

    fn get_derived_from_endpoints_slices(
        endpoints_slices: &[Arc<EndpointSlice>],
    ) -> Result<EndpointSliceDerived> {
        let mut endpoints: Vec<Arc<Endpoint>> = Vec::new();
        let mut endpoint_addresss: Vec<Arc<EndpointAddress>> = Vec::new();

        for slice in endpoints_slices {
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
                    endpoints.push(Arc::new(Endpoint::new(
                        &endpoint_id,
                        endpoint.clone(),
                        endpoint_slice_id.as_str(),
                    )));

                    let pod_uid = endpoint.target_ref.as_ref().and_then(|target_ref| {
                        if let (Some(kind), Some(uid)) = (target_ref.kind.as_ref(), target_ref.uid.as_ref()) {
                            match ResourceType::try_new(kind) {
                                Ok(resource_type) => {
                                    match resource_type {
                                        ResourceType::Pod => {
                                            Some(uid.clone())
                                        }
                                        resource_type => {
                                            warn!("Unknown endpoint target kind {} for EndpointSlice [{}]: {}",
                                                        resource_type,
                                                        target_ref.kind.as_deref().unwrap_or(""),
                                                        endpoint_slice_id
                                                    );
                                            None
                                        }
                                    }
                                }
                                Err(err) => {
                                    warn!(
                                                "Failed to parse resource type from endpoint target {:?}: {}",
                                                target_ref, err
                                            );
                                    None
                                }
                            }
                        }
                        else {
                            None
                        }
                    });

                    endpoint.addresses.iter().for_each(|address| {
                        let endpoint_address_uid =
                            format!("EndpointAddress:{endpoint_uid}:{address}");
                        let endpoint_address_id = ObjectIdentifier {
                            uid: endpoint_address_uid.clone(),
                            name: address.clone(),
                            namespace: slice.metadata.namespace.clone(),
                            resource_version: None,
                        };
                        endpoint_addresss.push(Arc::new(EndpointAddress::new(
                            &endpoint_address_id,
                            address.clone(),
                            endpoint_uid.as_str(),
                            endpoint_slice_id.as_str(),
                            pod_uid.clone()
                        )));
                    });
                });
            };
        }

        Ok((endpoints, endpoint_addresss))
    }
}
