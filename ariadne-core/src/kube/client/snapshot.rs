use super::*;

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
        Ok(SnapshotKubeClient {
            cluster,
            degraded_resource_kinds: Arc::new(Mutex::new(BTreeSet::new())),
            namespaces: read_list_from_dir(dir, SNAPSHOT_NAMESPACES_FILE)?,
            pods: read_list_from_dir(dir, SNAPSHOT_PODS_FILE)?,
            deployments: read_list_from_dir(dir, SNAPSHOT_DEPLOYMENTS_FILE)?,
            stateful_sets: read_list_from_dir(dir, SNAPSHOT_STATEFUL_SETS_FILE)?,
            replica_sets: read_list_from_dir(dir, SNAPSHOT_REPLICA_SETS_FILE)?,
            daemon_sets: read_list_from_dir(dir, SNAPSHOT_DAEMON_SETS_FILE)?,
            jobs: read_list_from_dir(dir, SNAPSHOT_JOBS_FILE)?,
            ingresses: read_list_from_dir(dir, SNAPSHOT_INGRESSES_FILE)?,
            services: read_list_from_dir(dir, SNAPSHOT_SERVICES_FILE)?,
            endpoint_slices: read_list_from_dir(dir, SNAPSHOT_ENDPOINT_SLICES_FILE)?,
            network_policies: read_list_from_dir(dir, SNAPSHOT_NETWORK_POLICIES_FILE)?,
            config_maps: read_list_from_dir(dir, SNAPSHOT_CONFIG_MAPS_FILE)?,
            storage_classes: read_list_from_dir(dir, SNAPSHOT_STORAGE_CLASSES_FILE)?,
            persistent_volumes: read_list_from_dir(dir, SNAPSHOT_PERSISTENT_VOLUMES_FILE)?,
            persistent_volume_claims: read_list_from_dir(
                dir,
                SNAPSHOT_PERSISTENT_VOLUME_CLAIMS_FILE,
            )?,
            nodes: read_list_from_dir(dir, SNAPSHOT_NODES_FILE)?,
            service_accounts: read_list_from_dir(dir, SNAPSHOT_SERVICE_ACCOUNTS_FILE)?,
            events: read_list_from_dir(dir, SNAPSHOT_EVENTS_FILE)?,
        })
    }
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
