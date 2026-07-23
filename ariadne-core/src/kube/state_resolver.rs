use crate::prelude::*;

use crate::graph_backend::GraphBackend;
use crate::kube_client::{CachedKubeClient, KubeClient};
use crate::snapshot::{
    SNAPSHOT_CLUSTER_FILE, SNAPSHOT_CONFIG_MAPS_FILE, SNAPSHOT_DAEMON_SETS_FILE,
    SNAPSHOT_DEPLOYMENTS_FILE, SNAPSHOT_ENDPOINT_SLICES_FILE, SNAPSHOT_EVENTS_FILE,
    SNAPSHOT_INGRESSES_FILE, SNAPSHOT_JOBS_FILE, SNAPSHOT_NAMESPACES_FILE,
    SNAPSHOT_NETWORK_POLICIES_FILE, SNAPSHOT_NODES_FILE, SNAPSHOT_PERSISTENT_VOLUME_CLAIMS_FILE,
    SNAPSHOT_PERSISTENT_VOLUMES_FILE, SNAPSHOT_PODS_FILE, SNAPSHOT_REPLICA_SETS_FILE,
    SNAPSHOT_SERVICE_ACCOUNTS_FILE, SNAPSHOT_SERVICES_FILE, SNAPSHOT_STATEFUL_SETS_FILE,
    SNAPSHOT_STORAGE_CLASSES_FILE, write_json_to_dir, write_redacted_list_to_dir,
};
use crate::state::{ClusterState, ClusterStateDiff};
use crate::types::*;
use k8s_openapi::Resource;
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
use kube::ResourceExt;
use kube::config::KubeConfigOptions;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{info, trace, warn};

#[path = "state_resolver/nodes.rs"]
mod nodes;
#[path = "state_resolver/relationships.rs"]
mod relationships;

type IngressDerived = (Vec<Arc<Host>>, Vec<Arc<IngressServiceBackend>>);
type EndpointSliceDerived = (Vec<Arc<Endpoint>>, Vec<Arc<EndpointAddress>>);

pub const SOURCE_SYNC_POLL_INTERVAL_ENV: &str = "SOURCE_SYNC_POLL_INTERVAL_SECONDS";
pub const DEFAULT_SOURCE_SYNC_POLL_INTERVAL_SECONDS: u64 = 2;

pub fn configured_source_sync_poll_interval() -> Duration {
    match std::env::var(SOURCE_SYNC_POLL_INTERVAL_ENV) {
        Ok(raw) => match raw.parse::<u64>() {
            Ok(0) => {
                warn!(
                    env_var = SOURCE_SYNC_POLL_INTERVAL_ENV,
                    value = %raw,
                    default_seconds = DEFAULT_SOURCE_SYNC_POLL_INTERVAL_SECONDS,
                    "Invalid source sync poll interval; expected a positive integer, using default"
                );
                Duration::from_secs(DEFAULT_SOURCE_SYNC_POLL_INTERVAL_SECONDS)
            }
            Ok(seconds) => Duration::from_secs(seconds),
            Err(err) => {
                warn!(
                    env_var = SOURCE_SYNC_POLL_INTERVAL_ENV,
                    value = %raw,
                    error = %err,
                    default_seconds = DEFAULT_SOURCE_SYNC_POLL_INTERVAL_SECONDS,
                    "Failed to parse source sync poll interval; using default"
                );
                Duration::from_secs(DEFAULT_SOURCE_SYNC_POLL_INTERVAL_SECONDS)
            }
        },
        Err(_) => Duration::from_secs(DEFAULT_SOURCE_SYNC_POLL_INTERVAL_SECONDS),
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StateDiffSummary {
    pub added_nodes: usize,
    pub removed_nodes: usize,
    pub modified_nodes: usize,
    pub added_edges: usize,
    pub removed_edges: usize,
}

impl StateDiffSummary {
    fn from_diff(diff: &ClusterStateDiff) -> Self {
        Self {
            added_nodes: diff.added_nodes.len(),
            removed_nodes: diff.removed_nodes.len(),
            modified_nodes: diff.modified_nodes.len(),
            added_edges: diff.added_edges.len(),
            removed_edges: diff.removed_edges.len(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceSyncStage {
    KubeFetch,
    Diff,
    GraphWrite,
}

#[derive(Debug, Clone)]
pub struct SourceSyncOutcome {
    pub fetch_duration: Duration,
    pub diff_duration: Duration,
    pub write_duration: Option<Duration>,
    pub diff: StateDiffSummary,
}

#[derive(Debug, Clone)]
pub struct SourceSyncError {
    pub stage: SourceSyncStage,
    pub message: String,
    pub fetch_duration: Option<Duration>,
    pub diff_duration: Option<Duration>,
    pub write_duration: Option<Duration>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebuildStage {
    StateRead,
    GraphWrite,
}

#[derive(Debug, Clone)]
pub struct RebuildOutcome {
    pub fetch_duration: Duration,
    pub write_duration: Duration,
}

#[derive(Debug, Clone)]
pub struct RebuildError {
    pub stage: RebuildStage,
    pub message: String,
    pub fetch_duration: Option<Duration>,
    pub write_duration: Option<Duration>,
}

pub struct ClusterStateResolver {
    cluster: Cluster,
    kube_client: Arc<Box<dyn KubeClient>>,
    last_snapshot: Arc<Mutex<AugmentedClusterSnapshot>>,
    last_state: Arc<Mutex<ClusterState>>,
    refresh_lock: Arc<AsyncMutex<()>>,
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
        let bootstrap_started = Instant::now();
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
        info!(
            bootstrap_ms = bootstrap_started.elapsed().as_millis(),
            "Initialized cluster state resolver"
        );
        Ok(ClusterStateResolver {
            cluster,
            kube_client,
            last_snapshot: Arc::new(Mutex::new(augmented)),
            last_state,
            refresh_lock: Arc::new(AsyncMutex::new(())),
            should_export_snapshot: false,
        })
    }

    async fn get_augmented_snapshot(
        cluster: &Cluster,
        kube_client: Arc<Box<dyn KubeClient>>,
    ) -> Result<AugmentedClusterSnapshot> {
        let observed_started = Instant::now();
        let last_snapshot =
            Self::get_observed_snapshot(cluster.clone(), kube_client.clone()).await?;
        let observed_duration = observed_started.elapsed();
        let derived_started = Instant::now();
        let derived_snapshot = Self::get_derived_snapshot(&last_snapshot)?;
        let derived_duration = derived_started.elapsed();
        let augmented = AugmentedClusterSnapshot {
            observed: last_snapshot,
            derived: derived_snapshot,
        };
        info!(
            snapshot_read_ms = observed_duration.as_millis(),
            derive_ms = derived_duration.as_millis(),
            containers = augmented.derived.containers.len(),
            hosts = augmented.derived.hosts.len(),
            ingress_service_backends = augmented.derived.ingress_service_backends.len(),
            endpoints = augmented.derived.endpoints.len(),
            endpoint_addresses = augmented.derived.endpoint_addresses.len(),
            "Built augmented cluster snapshot"
        );
        Ok(augmented)
    }

    async fn get_observed_snapshot(
        cluster: Cluster,
        client: Arc<Box<dyn KubeClient>>,
    ) -> Result<ObservedClusterSnapshot> {
        let fetch_started = Instant::now();
        let namespaces = client.get_namespaces().await?;
        let events: Vec<Arc<Event>> = client.get_events().await?;
        let nodes = client.get_nodes().await?;
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

        let storage_classes = client.get_storage_classes().await?;
        let persistent_volumes = client.get_persistent_volumes().await?;
        let persistent_volume_claims = client.get_persistent_volume_claims().await?;

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
        info!(
            snapshot_read_ms = fetch_started.elapsed().as_millis(),
            namespaces = snapshot.namespaces.len(),
            pods = snapshot.pods.len(),
            deployments = snapshot.deployments.len(),
            stateful_sets = snapshot.stateful_sets.len(),
            replica_sets = snapshot.replica_sets.len(),
            daemon_sets = snapshot.daemon_sets.len(),
            jobs = snapshot.jobs.len(),
            ingresses = snapshot.ingresses.len(),
            services = snapshot.services.len(),
            endpoint_slices = snapshot.endpoint_slices.len(),
            network_policies = snapshot.network_policies.len(),
            config_maps = snapshot.config_maps.len(),
            storage_classes = snapshot.storage_classes.len(),
            persistent_volumes = snapshot.persistent_volumes.len(),
            persistent_volume_claims = snapshot.persistent_volume_claims.len(),
            nodes = snapshot.nodes.len(),
            service_accounts = snapshot.service_accounts.len(),
            events = snapshot.events.len(),
            "Fetched observed cluster snapshot from source"
        );
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
        let refresh_lock = self.refresh_lock.clone();

        tokio::spawn(async move {
            Self::diff_loop(
                cluster,
                kube_client,
                last_snapshot,
                last_state,
                refresh_lock,
                backend,
                token,
            )
            .await
            .expect("Diff loop failed");
        })
    }

    async fn diff_loop(
        cluster: Cluster,
        kube_client: Arc<Box<dyn KubeClient>>,
        last_snapshot: Arc<Mutex<AugmentedClusterSnapshot>>,
        last_state: Arc<Mutex<ClusterState>>,
        refresh_lock: Arc<AsyncMutex<()>>,
        backend: Arc<dyn GraphBackend>,
        token: CancellationToken,
    ) -> Result<()> {
        let poll_interval = configured_source_sync_poll_interval();
        let mut id: usize = 0;
        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    break;
                },
                _ = sleep(poll_interval) => {
                    Self::sync_from_source_impl(
                        cluster.clone(),
                        kube_client.clone(),
                        last_snapshot.clone(),
                        last_state.clone(),
                        refresh_lock.clone(),
                        backend.clone(),
                    )
                    .await
                    .map_err(|err| std::io::Error::other(err.message))?;
                    id += 1;
                },
            }
        }
        info!("Stopped diff_loop, number of loops {id}");
        Ok(())
    }

    pub async fn sync_from_source(
        &self,
        backend: Arc<dyn GraphBackend>,
    ) -> std::result::Result<SourceSyncOutcome, SourceSyncError> {
        Self::sync_from_source_impl(
            self.cluster.clone(),
            self.kube_client.clone(),
            self.last_snapshot.clone(),
            self.last_state.clone(),
            self.refresh_lock.clone(),
            backend,
        )
        .await
    }

    async fn sync_from_source_impl(
        cluster: Cluster,
        kube_client: Arc<Box<dyn KubeClient>>,
        last_snapshot: Arc<Mutex<AugmentedClusterSnapshot>>,
        last_state: Arc<Mutex<ClusterState>>,
        refresh_lock: Arc<AsyncMutex<()>>,
        backend: Arc<dyn GraphBackend>,
    ) -> std::result::Result<SourceSyncOutcome, SourceSyncError> {
        let _refresh_guard = refresh_lock.lock().await;

        let fetch_started = Instant::now();
        let current_snapshot = Self::get_augmented_snapshot(&cluster, kube_client.clone())
            .await
            .map_err(|err| SourceSyncError {
                stage: SourceSyncStage::KubeFetch,
                message: err.to_string(),
                fetch_duration: Some(fetch_started.elapsed()),
                diff_duration: None,
                write_duration: None,
            })?;
        let fetch_duration = fetch_started.elapsed();

        let diff_started = Instant::now();
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
        let diff_duration = diff_started.elapsed();
        let diff_summary = StateDiffSummary::from_diff(&state_diff);

        let write_duration = if !state_diff.is_empty() {
            info!(
                "Applying source sync diff: +{} nodes, -{} nodes, ~{} nodes, +{} edges, -{} edges",
                diff_summary.added_nodes,
                diff_summary.removed_nodes,
                diff_summary.modified_nodes,
                diff_summary.added_edges,
                diff_summary.removed_edges,
            );
            let write_started = Instant::now();
            backend
                .update(state_diff)
                .await
                .map_err(|err| SourceSyncError {
                    stage: SourceSyncStage::GraphWrite,
                    message: err.to_string(),
                    fetch_duration: Some(fetch_duration),
                    diff_duration: Some(diff_duration),
                    write_duration: Some(write_started.elapsed()),
                })?;
            Some(write_started.elapsed())
        } else {
            trace!("Source sync: no changes detected");
            None
        };

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

        Ok(SourceSyncOutcome {
            fetch_duration,
            diff_duration,
            write_duration,
            diff: diff_summary,
        })
    }

    pub async fn rebuild_from_source(
        &self,
        backend: Arc<dyn GraphBackend>,
    ) -> std::result::Result<RebuildOutcome, RebuildError> {
        let _refresh_guard = self.refresh_lock.lock().await;
        let fetch_started = Instant::now();
        let current_snapshot =
            Self::get_augmented_snapshot(&self.cluster, self.kube_client.clone())
                .await
                .map_err(|err| RebuildError {
                    stage: RebuildStage::StateRead,
                    message: err.to_string(),
                    fetch_duration: Some(fetch_started.elapsed()),
                    write_duration: None,
                })?;
        let fetch_duration = fetch_started.elapsed();

        // Build Memgraph from a fresh source snapshot, not the cached shared state.
        let rebuild_state = Arc::new(Mutex::new(Self::create_state(&current_snapshot)));
        let write_started = Instant::now();
        backend
            .create(rebuild_state)
            .await
            .map_err(|err| RebuildError {
                stage: RebuildStage::GraphWrite,
                message: err.to_string(),
                fetch_duration: Some(fetch_duration),
                write_duration: Some(write_started.elapsed()),
            })?;
        let write_duration = write_started.elapsed();

        let refreshed_state = Self::create_state(&current_snapshot);
        {
            let mut last_state_guard = self
                .last_state
                .lock()
                .expect("Failed to lock last_state for full rebuild");
            *last_state_guard = refreshed_state;
        }

        {
            let mut last_snapshot_guard = self
                .last_snapshot
                .lock()
                .expect("Failed to lock last_snapshot for full rebuild");
            *last_snapshot_guard = current_snapshot;
        }

        Ok(RebuildOutcome {
            fetch_duration,
            write_duration,
        })
    }

    pub fn degraded_resource_kinds_handle(&self) -> Arc<Mutex<std::collections::BTreeSet<String>>> {
        self.kube_client.degraded_resource_kinds_handle()
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
        write_redacted_list_to_dir(
            dir,
            SNAPSHOT_NAMESPACES_FILE,
            &snapshot.namespaces,
            ResourceType::Namespace,
        )?;
        write_redacted_list_to_dir(dir, SNAPSHOT_PODS_FILE, &snapshot.pods, ResourceType::Pod)?;
        write_redacted_list_to_dir(
            dir,
            SNAPSHOT_DEPLOYMENTS_FILE,
            &snapshot.deployments,
            ResourceType::Deployment,
        )?;
        write_redacted_list_to_dir(
            dir,
            SNAPSHOT_STATEFUL_SETS_FILE,
            &snapshot.stateful_sets,
            ResourceType::StatefulSet,
        )?;
        write_redacted_list_to_dir(
            dir,
            SNAPSHOT_REPLICA_SETS_FILE,
            &snapshot.replica_sets,
            ResourceType::ReplicaSet,
        )?;
        write_redacted_list_to_dir(
            dir,
            SNAPSHOT_DAEMON_SETS_FILE,
            &snapshot.daemon_sets,
            ResourceType::DaemonSet,
        )?;
        write_redacted_list_to_dir(dir, SNAPSHOT_JOBS_FILE, &snapshot.jobs, ResourceType::Job)?;
        write_redacted_list_to_dir(
            dir,
            SNAPSHOT_INGRESSES_FILE,
            &snapshot.ingresses,
            ResourceType::Ingress,
        )?;
        write_redacted_list_to_dir(
            dir,
            SNAPSHOT_SERVICES_FILE,
            &snapshot.services,
            ResourceType::Service,
        )?;
        write_redacted_list_to_dir(
            dir,
            SNAPSHOT_ENDPOINT_SLICES_FILE,
            &snapshot.endpoint_slices,
            ResourceType::EndpointSlice,
        )?;
        write_redacted_list_to_dir(
            dir,
            SNAPSHOT_NETWORK_POLICIES_FILE,
            &snapshot.network_policies,
            ResourceType::NetworkPolicy,
        )?;
        write_redacted_list_to_dir(
            dir,
            SNAPSHOT_CONFIG_MAPS_FILE,
            &snapshot.config_maps,
            ResourceType::ConfigMap,
        )?;
        write_redacted_list_to_dir(
            dir,
            SNAPSHOT_STORAGE_CLASSES_FILE,
            &snapshot.storage_classes,
            ResourceType::StorageClass,
        )?;
        write_redacted_list_to_dir(
            dir,
            SNAPSHOT_PERSISTENT_VOLUMES_FILE,
            &snapshot.persistent_volumes,
            ResourceType::PersistentVolume,
        )?;
        write_redacted_list_to_dir(
            dir,
            SNAPSHOT_PERSISTENT_VOLUME_CLAIMS_FILE,
            &snapshot.persistent_volume_claims,
            ResourceType::PersistentVolumeClaim,
        )?;
        write_redacted_list_to_dir(
            dir,
            SNAPSHOT_NODES_FILE,
            &snapshot.nodes,
            ResourceType::Node,
        )?;
        write_redacted_list_to_dir(
            dir,
            SNAPSHOT_SERVICE_ACCOUNTS_FILE,
            &snapshot.service_accounts,
            ResourceType::ServiceAccount,
        )?;
        write_redacted_list_to_dir(
            dir,
            SNAPSHOT_EVENTS_FILE,
            &snapshot.events,
            ResourceType::Event,
        )?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "state_resolver/tests.rs"]
mod tests;
