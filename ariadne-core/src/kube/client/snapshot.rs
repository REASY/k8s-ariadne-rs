//! Read-only Kubernetes client backed by an exported snapshot directory.
//!
//! Construction eagerly validates and loads the snapshot so trait methods can
//! return stable, immutable resource collections without filesystem I/O.

use super::{
    Arc, BTreeSet, Cluster, ConfigMap, DaemonSet, Deployment, EndpointSlice, Event, Info, Ingress,
    Job, KubeClient, Mutex, Namespace, NetworkPolicy, Node, PersistentVolume,
    PersistentVolumeClaim, Pod, ReplicaSet, Result, SNAPSHOT_CLUSTER_FILE,
    SNAPSHOT_CONFIG_MAPS_FILE, SNAPSHOT_DAEMON_SETS_FILE, SNAPSHOT_DEPLOYMENTS_FILE,
    SNAPSHOT_ENDPOINT_SLICES_FILE, SNAPSHOT_EVENTS_FILE, SNAPSHOT_INGRESSES_FILE,
    SNAPSHOT_JOBS_FILE, SNAPSHOT_NAMESPACES_FILE, SNAPSHOT_NETWORK_POLICIES_FILE,
    SNAPSHOT_NODES_FILE, SNAPSHOT_PERSISTENT_VOLUME_CLAIMS_FILE, SNAPSHOT_PERSISTENT_VOLUMES_FILE,
    SNAPSHOT_PODS_FILE, SNAPSHOT_REPLICA_SETS_FILE, SNAPSHOT_SERVICE_ACCOUNTS_FILE,
    SNAPSHOT_SERVICES_FILE, SNAPSHOT_STATEFUL_SETS_FILE, SNAPSHOT_STORAGE_CLASSES_FILE, Service,
    ServiceAccount, StatefulSet, StorageClass, read_json_from_dir, read_list_from_dir,
};
use async_trait::async_trait;
use kube::Resource;
use std::path::Path;

pub struct SnapshotKubeClient {
    cluster: Cluster,
    degraded_resource_kinds: Arc<Mutex<BTreeSet<String>>>,
    namespaces: Vec<Arc<Namespace>>,
    pods: Vec<Arc<Pod>>,
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

impl SnapshotKubeClient {
    pub fn from_dir(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref();
        let cluster: Cluster = read_json_from_dir(dir, SNAPSHOT_CLUSTER_FILE)?;
        validate_cluster_metadata(&cluster)?;
        let namespaces = read_list_from_dir(dir, SNAPSHOT_NAMESPACES_FILE)?;
        let pods = read_list_from_dir(dir, SNAPSHOT_PODS_FILE)?;
        let deployments = read_list_from_dir(dir, SNAPSHOT_DEPLOYMENTS_FILE)?;
        let stateful_sets = read_list_from_dir(dir, SNAPSHOT_STATEFUL_SETS_FILE)?;
        let replica_sets = read_list_from_dir(dir, SNAPSHOT_REPLICA_SETS_FILE)?;
        let daemon_sets = read_list_from_dir(dir, SNAPSHOT_DAEMON_SETS_FILE)?;
        let jobs = read_list_from_dir(dir, SNAPSHOT_JOBS_FILE)?;
        let ingresses = read_list_from_dir(dir, SNAPSHOT_INGRESSES_FILE)?;
        let services = read_list_from_dir(dir, SNAPSHOT_SERVICES_FILE)?;
        let endpoint_slices = read_list_from_dir(dir, SNAPSHOT_ENDPOINT_SLICES_FILE)?;
        let network_policies = read_list_from_dir(dir, SNAPSHOT_NETWORK_POLICIES_FILE)?;
        let config_maps = read_list_from_dir(dir, SNAPSHOT_CONFIG_MAPS_FILE)?;
        let storage_classes = read_list_from_dir(dir, SNAPSHOT_STORAGE_CLASSES_FILE)?;
        let persistent_volumes = read_list_from_dir(dir, SNAPSHOT_PERSISTENT_VOLUMES_FILE)?;
        let persistent_volume_claims =
            read_list_from_dir(dir, SNAPSHOT_PERSISTENT_VOLUME_CLAIMS_FILE)?;
        let nodes = read_list_from_dir(dir, SNAPSHOT_NODES_FILE)?;
        let service_accounts = read_list_from_dir(dir, SNAPSHOT_SERVICE_ACCOUNTS_FILE)?;
        let events = read_list_from_dir(dir, SNAPSHOT_EVENTS_FILE)?;

        validate_resource_metadata("Namespace", &namespaces)?;
        validate_resource_metadata("Pod", &pods)?;
        validate_resource_metadata("Deployment", &deployments)?;
        validate_resource_metadata("StatefulSet", &stateful_sets)?;
        validate_resource_metadata("ReplicaSet", &replica_sets)?;
        validate_resource_metadata("DaemonSet", &daemon_sets)?;
        validate_resource_metadata("Job", &jobs)?;
        validate_resource_metadata("Ingress", &ingresses)?;
        validate_resource_metadata("Service", &services)?;
        validate_resource_metadata("EndpointSlice", &endpoint_slices)?;
        validate_resource_metadata("NetworkPolicy", &network_policies)?;
        validate_resource_metadata("ConfigMap", &config_maps)?;
        validate_resource_metadata("StorageClass", &storage_classes)?;
        validate_resource_metadata("PersistentVolume", &persistent_volumes)?;
        validate_resource_metadata("PersistentVolumeClaim", &persistent_volume_claims)?;
        validate_resource_metadata("Node", &nodes)?;
        validate_resource_metadata("ServiceAccount", &service_accounts)?;
        validate_resource_metadata("Event", &events)?;

        Ok(SnapshotKubeClient {
            cluster,
            degraded_resource_kinds: Arc::new(Mutex::new(BTreeSet::new())),
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
        })
    }
}

fn validate_cluster_metadata(cluster: &Cluster) -> Result<()> {
    validate_metadata_values(
        "Cluster",
        None,
        cluster.metadata.name.as_deref(),
        cluster.metadata.uid.as_deref(),
    )
}

fn validate_resource_metadata<T: Resource>(kind: &str, resources: &[Arc<T>]) -> Result<()> {
    for (index, resource) in resources.iter().enumerate() {
        let metadata = resource.meta();
        validate_metadata_values(
            kind,
            Some(index),
            metadata.name.as_deref(),
            metadata.uid.as_deref(),
        )?;
    }
    Ok(())
}

fn validate_metadata_values(
    kind: &str,
    index: Option<usize>,
    name: Option<&str>,
    uid: Option<&str>,
) -> Result<()> {
    let location = index
        .map(|index| format!(" entry {index}"))
        .unwrap_or_default();
    for (field, value) in [("name", name), ("uid", uid)] {
        if value.is_none_or(str::is_empty) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("{kind} snapshot{location} is missing non-empty metadata.{field}"),
            )
            .into());
        }
    }
    Ok(())
}

#[async_trait]
impl KubeClient for SnapshotKubeClient {
    async fn get_namespaces(&self) -> Result<Vec<Arc<Namespace>>> {
        Ok(self.namespaces.clone())
    }

    async fn get_pods(&self) -> Result<Vec<Arc<Pod>>> {
        Ok(self.pods.clone())
    }

    async fn get_deployments(&self) -> Result<Vec<Arc<Deployment>>> {
        Ok(self.deployments.clone())
    }

    async fn get_stateful_sets(&self) -> Result<Vec<Arc<StatefulSet>>> {
        Ok(self.stateful_sets.clone())
    }

    async fn get_replica_sets(&self) -> Result<Vec<Arc<ReplicaSet>>> {
        Ok(self.replica_sets.clone())
    }

    async fn get_daemon_sets(&self) -> Result<Vec<Arc<DaemonSet>>> {
        Ok(self.daemon_sets.clone())
    }

    async fn get_jobs(&self) -> Result<Vec<Arc<Job>>> {
        Ok(self.jobs.clone())
    }

    async fn get_ingresses(&self) -> Result<Vec<Arc<Ingress>>> {
        Ok(self.ingresses.clone())
    }

    async fn get_services(&self) -> Result<Vec<Arc<Service>>> {
        Ok(self.services.clone())
    }

    async fn get_endpoint_slices(&self) -> Result<Vec<Arc<EndpointSlice>>> {
        Ok(self.endpoint_slices.clone())
    }

    async fn get_network_policies(&self) -> Result<Vec<Arc<NetworkPolicy>>> {
        Ok(self.network_policies.clone())
    }

    async fn get_config_maps(&self) -> Result<Vec<Arc<ConfigMap>>> {
        Ok(self.config_maps.clone())
    }

    async fn get_storage_classes(&self) -> Result<Vec<Arc<StorageClass>>> {
        Ok(self.storage_classes.clone())
    }

    async fn get_persistent_volumes(&self) -> Result<Vec<Arc<PersistentVolume>>> {
        Ok(self.persistent_volumes.clone())
    }

    async fn get_persistent_volume_claims(&self) -> Result<Vec<Arc<PersistentVolumeClaim>>> {
        Ok(self.persistent_volume_claims.clone())
    }

    async fn get_nodes(&self) -> Result<Vec<Arc<Node>>> {
        Ok(self.nodes.clone())
    }

    async fn get_service_accounts(&self) -> Result<Vec<Arc<ServiceAccount>>> {
        Ok(self.service_accounts.clone())
    }

    async fn apiserver_version(&self) -> Result<Info> {
        Ok(self.cluster.info.clone())
    }

    async fn get_cluster_url(&self) -> Result<String> {
        Ok(self.cluster.cluster_url.clone())
    }

    async fn get_events(&self) -> Result<Vec<Arc<Event>>> {
        Ok(self.events.clone())
    }

    fn degraded_resource_kinds_handle(&self) -> Arc<Mutex<BTreeSet<String>>> {
        self.degraded_resource_kinds.clone()
    }
}
