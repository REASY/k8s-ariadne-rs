//! Reflector-backed Kubernetes client for continuously refreshed live state.
//!
//! Denied resource kinds start absent and are recorded as degraded coverage,
//! while their reflectors keep retrying so later RBAC grants take effect.
//! Stores that have ever initialized must remain ready before their contents
//! are returned.

use super::{
    AccessChecker, AccessDecision, Api, Arc, BTreeSet, Client, Config, ConfigMap, DaemonSet,
    Deployment, EndpointSlice, Event, Info, Ingress, Job, JoinHandle, KubeClient,
    KubeConfigOptions, Mutex, Namespace, NetworkPolicy, Node, PersistentVolume,
    PersistentVolumeClaim, Pod, RESOURCE_CONFIG_MAP, RESOURCE_DAEMON_SET, RESOURCE_DEPLOYMENT,
    RESOURCE_ENDPOINT_SLICE, RESOURCE_EVENT, RESOURCE_INGRESS, RESOURCE_JOB, RESOURCE_NAMESPACE,
    RESOURCE_NETWORK_POLICY, RESOURCE_NODE, RESOURCE_PERSISTENT_VOLUME,
    RESOURCE_PERSISTENT_VOLUME_CLAIM, RESOURCE_POD, RESOURCE_REPLICA_SET, RESOURCE_SERVICE,
    RESOURCE_SERVICE_ACCOUNT, RESOURCE_STATEFUL_SET, RESOURCE_STORAGE_CLASS, ReplicaSet, Resource,
    ResourceDescriptor, Result, STORE_READY_TIMEOUT_SECONDS, Service, ServiceAccount, StatefulSet,
    StorageClass, Store, WatchHealth, event_api, install_rustls_provider, load_kube_config,
    make_store_and_watch, watcher,
};
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use std::future::Future;
use std::time::Duration;
use tokio::time::timeout;
use tracing::warn;

const ACCESS_CHECK_CONCURRENCY: usize = 6;
const WATCHED_RESOURCE_DESCRIPTORS: [ResourceDescriptor; 18] = [
    RESOURCE_NAMESPACE,
    RESOURCE_POD,
    RESOURCE_DEPLOYMENT,
    RESOURCE_STATEFUL_SET,
    RESOURCE_REPLICA_SET,
    RESOURCE_DAEMON_SET,
    RESOURCE_JOB,
    RESOURCE_INGRESS,
    RESOURCE_SERVICE,
    RESOURCE_ENDPOINT_SLICE,
    RESOURCE_NETWORK_POLICY,
    RESOURCE_CONFIG_MAP,
    RESOURCE_STORAGE_CLASS,
    RESOURCE_PERSISTENT_VOLUME,
    RESOURCE_PERSISTENT_VOLUME_CLAIM,
    RESOURCE_NODE,
    RESOURCE_SERVICE_ACCOUNT,
    RESOURCE_EVENT,
];

pub struct CachedKubeClient {
    config: Config,
    client: Client,
    watch_health: WatchHealth,
    _reflector_tasks: ReflectorTasks,
    namespace_store: Option<Store<Namespace>>,
    pod_store: Option<Store<Pod>>,
    deployment_store: Option<Store<Deployment>>,
    stateful_set_store: Option<Store<StatefulSet>>,
    replica_set_store: Option<Store<ReplicaSet>>,
    daemon_set_store: Option<Store<DaemonSet>>,
    job_store: Option<Store<Job>>,
    ingress_store: Option<Store<Ingress>>,
    service_store: Option<Store<Service>>,
    endpoint_slice_store: Option<Store<EndpointSlice>>,
    network_policy_store: Option<Store<NetworkPolicy>>,
    config_map_store: Option<Store<ConfigMap>>,
    storage_class_store: Option<Store<StorageClass>>,
    persistent_volume_store: Option<Store<PersistentVolume>>,
    persistent_volume_claim_store: Option<Store<PersistentVolumeClaim>>,
    node_store: Option<Store<Node>>,
    service_account_store: Option<Store<ServiceAccount>>,
    event_store: Option<Store<Event>>,
}

pub(super) struct ReflectorTasks {
    handles: Vec<JoinHandle<()>>,
}

impl ReflectorTasks {
    pub(super) fn new(handles: impl IntoIterator<Item = Option<JoinHandle<()>>>) -> Self {
        Self {
            handles: handles.into_iter().flatten().collect(),
        }
    }
}

impl Drop for ReflectorTasks {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}

#[async_trait]
impl KubeClient for CachedKubeClient {
    async fn get_namespaces(&self) -> Result<Vec<Arc<Namespace>>> {
        store_state_or_empty(&self.namespace_store, "Namespace", &self.watch_health).await
    }

    async fn get_pods(&self) -> Result<Vec<Arc<Pod>>> {
        store_state_or_empty(&self.pod_store, "Pod", &self.watch_health).await
    }

    async fn get_deployments(&self) -> Result<Vec<Arc<Deployment>>> {
        store_state_or_empty(&self.deployment_store, "Deployment", &self.watch_health).await
    }

    async fn get_stateful_sets(&self) -> Result<Vec<Arc<StatefulSet>>> {
        store_state_or_empty(&self.stateful_set_store, "StatefulSet", &self.watch_health).await
    }

    async fn get_replica_sets(&self) -> Result<Vec<Arc<ReplicaSet>>> {
        store_state_or_empty(&self.replica_set_store, "ReplicaSet", &self.watch_health).await
    }

    async fn get_daemon_sets(&self) -> Result<Vec<Arc<DaemonSet>>> {
        store_state_or_empty(&self.daemon_set_store, "DaemonSet", &self.watch_health).await
    }

    async fn get_jobs(&self) -> Result<Vec<Arc<Job>>> {
        store_state_or_empty(&self.job_store, "Job", &self.watch_health).await
    }

    async fn get_ingresses(&self) -> Result<Vec<Arc<Ingress>>> {
        store_state_or_empty(&self.ingress_store, "Ingress", &self.watch_health).await
    }

    async fn get_services(&self) -> Result<Vec<Arc<Service>>> {
        store_state_or_empty(&self.service_store, "Service", &self.watch_health).await
    }

    async fn get_endpoint_slices(&self) -> Result<Vec<Arc<EndpointSlice>>> {
        store_state_or_empty(
            &self.endpoint_slice_store,
            "EndpointSlice",
            &self.watch_health,
        )
        .await
    }

    async fn get_network_policies(&self) -> Result<Vec<Arc<NetworkPolicy>>> {
        store_state_or_empty(
            &self.network_policy_store,
            "NetworkPolicy",
            &self.watch_health,
        )
        .await
    }

    async fn get_config_maps(&self) -> Result<Vec<Arc<ConfigMap>>> {
        store_state_or_empty(&self.config_map_store, "ConfigMap", &self.watch_health).await
    }

    async fn get_storage_classes(&self) -> Result<Vec<Arc<StorageClass>>> {
        store_state_or_empty(
            &self.storage_class_store,
            "StorageClass",
            &self.watch_health,
        )
        .await
    }

    async fn get_persistent_volumes(&self) -> Result<Vec<Arc<PersistentVolume>>> {
        store_state_or_empty(
            &self.persistent_volume_store,
            "PersistentVolume",
            &self.watch_health,
        )
        .await
    }

    async fn get_persistent_volume_claims(&self) -> Result<Vec<Arc<PersistentVolumeClaim>>> {
        store_state_or_empty(
            &self.persistent_volume_claim_store,
            "PersistentVolumeClaim",
            &self.watch_health,
        )
        .await
    }

    async fn get_nodes(&self) -> Result<Vec<Arc<Node>>> {
        store_state_or_empty(&self.node_store, "Node", &self.watch_health).await
    }

    async fn get_service_accounts(&self) -> Result<Vec<Arc<ServiceAccount>>> {
        store_state_or_empty(
            &self.service_account_store,
            "ServiceAccount",
            &self.watch_health,
        )
        .await
    }

    async fn apiserver_version(&self) -> Result<Info> {
        let r = self.client.apiserver_version().await?;
        Ok(r)
    }

    async fn get_cluster_url(&self) -> Result<String> {
        Ok(self.config.cluster_url.to_string())
    }

    async fn get_events(&self) -> Result<Vec<Arc<Event>>> {
        store_state_or_empty_with_timeout(
            &self.event_store,
            "Event",
            event_store_ready_timeout(),
            &self.watch_health,
        )
        .await
    }

    fn degraded_resource_kinds_handle(&self) -> Arc<Mutex<BTreeSet<String>>> {
        self.watch_health.degraded_handle()
    }
}

struct StoreStarter {
    watch_health: WatchHealth,
}

impl StoreStarter {
    fn start<T>(&self, api: Api<T>) -> (Option<Store<T>>, Option<JoinHandle<()>>)
    where
        T: Resource + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
        T::DynamicType: Default + Clone + Eq + std::hash::Hash + Send + Sync + 'static,
    {
        self.start_with_config(api, watcher::Config::default())
    }

    fn start_with_config<T>(
        &self,
        api: Api<T>,
        watcher_config: watcher::Config,
    ) -> (Option<Store<T>>, Option<JoinHandle<()>>)
    where
        T: Resource + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
        T::DynamicType: Default + Clone + Eq + std::hash::Hash + Send + Sync + 'static,
    {
        let watch_health = self.watch_health.clone();
        start_store_with_factory(true, || {
            make_store_and_watch(api, watcher_config, watch_health)
        })
    }
}

pub(super) fn start_store_with_factory<T, F, Fut>(
    allowed: bool,
    factory: F,
) -> (Option<Store<T>>, Option<JoinHandle<()>>)
where
    T: Resource + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
    T::DynamicType: Default + Clone + Eq + std::hash::Hash + Send + Sync + 'static,
    F: FnOnce() -> (Store<T>, Fut),
    Fut: Future<Output = ()> + Send + 'static,
{
    if allowed {
        let (store, watch) = factory();
        (Some(store), Some(tokio::spawn(watch)))
    } else {
        (None, None)
    }
}

pub(super) async fn store_state_or_empty<T>(
    store: &Option<Store<T>>,
    kind: &'static str,
    watch_health: &WatchHealth,
) -> Result<Vec<Arc<T>>>
where
    T: Resource + Clone + Debug + Send + Sync + 'static,
    T::DynamicType: Default + Clone + Eq + std::hash::Hash + Send + Sync + 'static,
{
    store_state_or_empty_with_timeout(store, kind, store_ready_timeout(), watch_health).await
}

pub(super) async fn store_state_or_empty_with_timeout<T>(
    store: &Option<Store<T>>,
    kind: &'static str,
    timeout_duration: Duration,
    watch_health: &WatchHealth,
) -> Result<Vec<Arc<T>>>
where
    T: Resource + Clone + Debug + Send + Sync + 'static,
    T::DynamicType: Default + Clone + Eq + std::hash::Hash + Send + Sync + 'static,
{
    if watch_health.is_awaiting_initial_access(kind) {
        return Ok(Vec::new());
    }
    match store {
        Some(store) => {
            wait_for_store_readiness(
                store.wait_until_ready(),
                || store.state(),
                kind,
                timeout_duration,
            )
            .await
        }
        None => Ok(Vec::new()),
    }
}

pub(super) async fn wait_for_store_readiness<T, Fut, E, F>(
    wait_ready: Fut,
    state: F,
    kind: &'static str,
    timeout_duration: Duration,
) -> Result<Vec<Arc<T>>>
where
    Fut: Future<Output = std::result::Result<(), E>>,
    E: std::fmt::Display,
    F: FnOnce() -> Vec<Arc<T>>,
{
    match timeout(timeout_duration, wait_ready).await {
        Ok(wait_result) => {
            if let Err(err) = wait_result {
                return Err(
                    std::io::Error::other(format!("{kind} store is not ready: {err}")).into(),
                );
            }
            Ok(state())
        }
        Err(_elapsed) => {
            warn!("Timed out waiting for {kind} store after {timeout_duration:?}",);
            Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("Timed out waiting for {kind} store readiness"),
            )
            .into())
        }
    }
}

pub(super) fn store_ready_timeout() -> Duration {
    std::env::var("KUBE_STORE_READY_TIMEOUT_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(STORE_READY_TIMEOUT_SECONDS))
}

pub(super) fn event_store_ready_timeout() -> Duration {
    std::env::var("KUBE_EVENT_STORE_READY_TIMEOUT_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(4))
}

pub(super) fn namespace_watcher_config(maybe_ns: Option<&str>) -> watcher::Config {
    maybe_ns
        .map(|namespace| watcher::Config::default().fields(&format!("metadata.name={namespace}")))
        .unwrap_or_default()
}

pub(super) fn update_degraded_resource_kinds(
    watch_health: &WatchHealth,
    access: &[(AccessDecision, &'static str)],
) {
    let mut kinds = watch_health
        .degraded_resource_kinds
        .lock()
        .expect("degraded_resource_kinds lock poisoned");
    let mut awaiting = watch_health
        .awaiting_initial_access
        .lock()
        .expect("awaiting_initial_access lock poisoned");
    for (decision, kind) in access {
        if decision.is_denied() {
            kinds.insert((*kind).to_string());
            awaiting.insert((*kind).to_string());
        }
    }
}

impl CachedKubeClient {
    pub async fn new(options: &KubeConfigOptions, maybe_ns: Option<&str>) -> Result<Self> {
        install_rustls_provider();
        let cfg = load_kube_config(options).await?;
        let client = Client::try_from(cfg.clone())?;

        let namespace_api: Api<Namespace> = Api::all(client.clone());

        let pod_api: Api<Pod> = maybe_ns
            .map(|ns| Api::namespaced(client.clone(), ns))
            .unwrap_or_else(|| Api::all(client.clone()));

        let deployment_api: Api<Deployment> = maybe_ns
            .map(|ns| Api::namespaced(client.clone(), ns))
            .unwrap_or_else(|| Api::all(client.clone()));

        let stateful_set_api: Api<StatefulSet> = maybe_ns
            .map(|ns| Api::namespaced(client.clone(), ns))
            .unwrap_or_else(|| Api::all(client.clone()));

        let replica_set_api: Api<ReplicaSet> = maybe_ns
            .map(|ns| Api::namespaced(client.clone(), ns))
            .unwrap_or_else(|| Api::all(client.clone()));
        let daemon_set_api: Api<DaemonSet> = maybe_ns
            .map(|ns| Api::namespaced(client.clone(), ns))
            .unwrap_or_else(|| Api::all(client.clone()));
        let job_api: Api<Job> = maybe_ns
            .map(|ns| Api::namespaced(client.clone(), ns))
            .unwrap_or_else(|| Api::all(client.clone()));
        let ingress_api: Api<Ingress> = maybe_ns
            .map(|ns| Api::namespaced(client.clone(), ns))
            .unwrap_or_else(|| Api::all(client.clone()));
        let service_api: Api<Service> = maybe_ns
            .map(|ns| Api::namespaced(client.clone(), ns))
            .unwrap_or_else(|| Api::all(client.clone()));
        let endpoint_slices_api: Api<EndpointSlice> = maybe_ns
            .map(|ns| Api::namespaced(client.clone(), ns))
            .unwrap_or_else(|| Api::all(client.clone()));
        let network_policy_api: Api<NetworkPolicy> = maybe_ns
            .map(|ns| Api::namespaced(client.clone(), ns))
            .unwrap_or_else(|| Api::all(client.clone()));
        let config_map_api: Api<ConfigMap> = maybe_ns
            .map(|ns| Api::namespaced(client.clone(), ns))
            .unwrap_or_else(|| Api::all(client.clone()));
        let storage_class_api: Api<StorageClass> = Api::all(client.clone());
        let persistent_volume_api: Api<PersistentVolume> = Api::all(client.clone());
        let persistent_volume_claim_api: Api<PersistentVolumeClaim> = maybe_ns
            .map(|ns| Api::namespaced(client.clone(), ns))
            .unwrap_or_else(|| Api::all(client.clone()));
        let node_api: Api<Node> = Api::all(client.clone());
        let service_account_api: Api<ServiceAccount> = maybe_ns
            .map(|ns| Api::namespaced(client.clone(), ns))
            .unwrap_or_else(|| Api::all(client.clone()));

        let event_api = event_api(client.clone(), maybe_ns);

        let access = AccessChecker::new(client.clone(), maybe_ns);
        let watch_health = WatchHealth::new();

        let access_decisions = access
            .can_read_all(&WATCHED_RESOURCE_DESCRIPTORS, ACCESS_CHECK_CONCURRENCY)
            .await;
        update_degraded_resource_kinds(&watch_health, &access_decisions);

        let stores = StoreStarter {
            watch_health: watch_health.clone(),
        };
        let (pod_store, pod_watch) = stores.start(pod_api);
        let (deployment_store, deployment_watch) = stores.start(deployment_api);
        let (stateful_set_store, stateful_set_watch) = stores.start(stateful_set_api);
        let (replica_set_store, replica_set_watch) = stores.start(replica_set_api);
        let (daemon_set_store, daemon_set_watch) = stores.start(daemon_set_api);
        let (job_store, job_watch) = stores.start(job_api);
        let (ingress_store, ingress_watch) = stores.start(ingress_api);
        let (service_store, service_watch) = stores.start(service_api);
        let (endpoint_slice_store, endpoint_slice_watch) = stores.start(endpoint_slices_api);
        let (network_policy_store, network_policy_watch) = stores.start(network_policy_api);
        let (config_map_store, config_map_watch) = stores.start(config_map_api);
        let (storage_class_store, storage_class_watch) = stores.start(storage_class_api);
        let (persistent_volume_store, persistent_volume_watch) =
            stores.start(persistent_volume_api);
        let (persistent_volume_claim_store, persistent_volume_claim_watch) =
            stores.start(persistent_volume_claim_api);
        let (node_store, node_watch) = stores.start(node_api);
        let (service_account_store, service_account_watch) = stores.start(service_account_api);
        let namespace_watcher_config = namespace_watcher_config(maybe_ns);
        let (namespace_store, namespace_watch) =
            stores.start_with_config(namespace_api, namespace_watcher_config);
        let (event_store, event_store_watch) = stores.start(event_api);
        let reflector_tasks = ReflectorTasks::new([
            namespace_watch,
            pod_watch,
            deployment_watch,
            stateful_set_watch,
            replica_set_watch,
            daemon_set_watch,
            job_watch,
            ingress_watch,
            service_watch,
            endpoint_slice_watch,
            network_policy_watch,
            config_map_watch,
            storage_class_watch,
            persistent_volume_watch,
            persistent_volume_claim_watch,
            node_watch,
            service_account_watch,
            event_store_watch,
        ]);

        Ok(Self {
            config: cfg.clone(),
            client: client.clone(),
            watch_health,
            _reflector_tasks: reflector_tasks,
            namespace_store,
            pod_store,
            deployment_store,
            stateful_set_store,
            replica_set_store,
            daemon_set_store,
            job_store,
            ingress_store,
            service_store,
            endpoint_slice_store,
            network_policy_store,
            config_map_store,
            storage_class_store,
            persistent_volume_store,
            persistent_volume_claim_store,
            node_store,
            service_account_store,
            event_store,
        })
    }
}
