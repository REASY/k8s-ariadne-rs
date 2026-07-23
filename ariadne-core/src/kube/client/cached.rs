//! Reflector-backed Kubernetes client for continuously refreshed live state.
//!
//! Denied resource kinds remain absent and are recorded as degraded coverage;
//! allowed stores must become ready before their contents are returned.

use super::{
    AccessChecker, Api, Arc, BTreeSet, Client, Config, ConfigMap, DaemonSet, Deployment,
    EndpointSlice, Event, Info, Ingress, Job, JoinHandle, KubeClient, KubeConfigOptions, Mutex,
    Namespace, NetworkPolicy, Node, PersistentVolume, PersistentVolumeClaim, Pod,
    RESOURCE_CONFIG_MAP, RESOURCE_DAEMON_SET, RESOURCE_DEPLOYMENT, RESOURCE_ENDPOINT_SLICE,
    RESOURCE_EVENT, RESOURCE_INGRESS, RESOURCE_JOB, RESOURCE_NAMESPACE, RESOURCE_NETWORK_POLICY,
    RESOURCE_NODE, RESOURCE_PERSISTENT_VOLUME, RESOURCE_PERSISTENT_VOLUME_CLAIM, RESOURCE_POD,
    RESOURCE_REPLICA_SET, RESOURCE_SERVICE, RESOURCE_SERVICE_ACCOUNT, RESOURCE_STATEFUL_SET,
    RESOURCE_STORAGE_CLASS, ReplicaSet, Resource, Result, STORE_READY_TIMEOUT_SECONDS, Service,
    ServiceAccount, StatefulSet, StorageClass, Store, install_rustls_provider, load_kube_config,
    make_store_and_watch,
};
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use std::future::Future;
use std::time::Duration;
use tokio::time::timeout;
use tracing::warn;

pub struct CachedKubeClient {
    config: Config,
    client: Client,
    degraded_resource_kinds: Arc<Mutex<BTreeSet<String>>>,
    namespace_store: Option<Store<Namespace>>,
    #[allow(unused)]
    namespace_watch: Option<JoinHandle<()>>,
    pod_store: Option<Store<Pod>>,
    #[allow(unused)]
    pod_watch: Option<JoinHandle<()>>,
    deployment_store: Option<Store<Deployment>>,
    #[allow(unused)]
    deployment_watch: Option<JoinHandle<()>>,
    stateful_set_store: Option<Store<StatefulSet>>,
    #[allow(unused)]
    stateful_set_watch: Option<JoinHandle<()>>,
    replica_set_store: Option<Store<ReplicaSet>>,
    #[allow(unused)]
    replica_set_watch: Option<JoinHandle<()>>,
    daemon_set_store: Option<Store<DaemonSet>>,
    #[allow(unused)]
    daemon_set_watch: Option<JoinHandle<()>>,
    job_store: Option<Store<Job>>,
    #[allow(unused)]
    job_watch: Option<JoinHandle<()>>,
    ingress_store: Option<Store<Ingress>>,
    #[allow(unused)]
    ingress_watch: Option<JoinHandle<()>>,
    service_store: Option<Store<Service>>,
    #[allow(unused)]
    service_watch: Option<JoinHandle<()>>,
    endpoint_slice_store: Option<Store<EndpointSlice>>,
    #[allow(unused)]
    endpoint_slice_watch: Option<JoinHandle<()>>,
    network_policy_store: Option<Store<NetworkPolicy>>,
    #[allow(unused)]
    network_policy_watch: Option<JoinHandle<()>>,
    config_map_store: Option<Store<ConfigMap>>,
    #[allow(unused)]
    config_map_watch: Option<JoinHandle<()>>,
    storage_class_store: Option<Store<StorageClass>>,
    #[allow(unused)]
    storage_class_watch: Option<JoinHandle<()>>,
    persistent_volume_store: Option<Store<PersistentVolume>>,
    #[allow(unused)]
    persistent_volume_watch: Option<JoinHandle<()>>,
    persistent_volume_claim_store: Option<Store<PersistentVolumeClaim>>,
    #[allow(unused)]
    persistent_volume_claim_watch: Option<JoinHandle<()>>,
    node_store: Option<Store<Node>>,
    #[allow(unused)]
    node_watch: Option<JoinHandle<()>>,
    service_account_store: Option<Store<ServiceAccount>>,
    #[allow(unused)]
    service_account_watch: Option<JoinHandle<()>>,
    event_store: Option<Store<Event>>,
    #[allow(unused)]
    event_store_watch: Option<JoinHandle<()>>,
}

#[async_trait]
impl KubeClient for CachedKubeClient {
    async fn get_namespaces(&self) -> Result<Vec<Arc<Namespace>>> {
        store_state_or_empty(&self.namespace_store, "Namespace").await
    }

    async fn get_pods(&self) -> Result<Vec<Arc<Pod>>> {
        store_state_or_empty(&self.pod_store, "Pod").await
    }

    async fn get_deployments(&self) -> Result<Vec<Arc<Deployment>>> {
        store_state_or_empty(&self.deployment_store, "Deployment").await
    }

    async fn get_stateful_sets(&self) -> Result<Vec<Arc<StatefulSet>>> {
        store_state_or_empty(&self.stateful_set_store, "StatefulSet").await
    }

    async fn get_replica_sets(&self) -> Result<Vec<Arc<ReplicaSet>>> {
        store_state_or_empty(&self.replica_set_store, "ReplicaSet").await
    }

    async fn get_daemon_sets(&self) -> Result<Vec<Arc<DaemonSet>>> {
        store_state_or_empty(&self.daemon_set_store, "DaemonSet").await
    }

    async fn get_jobs(&self) -> Result<Vec<Arc<Job>>> {
        store_state_or_empty(&self.job_store, "Job").await
    }

    async fn get_ingresses(&self) -> Result<Vec<Arc<Ingress>>> {
        store_state_or_empty(&self.ingress_store, "Ingress").await
    }

    async fn get_services(&self) -> Result<Vec<Arc<Service>>> {
        store_state_or_empty(&self.service_store, "Service").await
    }

    async fn get_endpoint_slices(&self) -> Result<Vec<Arc<EndpointSlice>>> {
        store_state_or_empty(&self.endpoint_slice_store, "EndpointSlice").await
    }

    async fn get_network_policies(&self) -> Result<Vec<Arc<NetworkPolicy>>> {
        store_state_or_empty(&self.network_policy_store, "NetworkPolicy").await
    }

    async fn get_config_maps(&self) -> Result<Vec<Arc<ConfigMap>>> {
        store_state_or_empty(&self.config_map_store, "ConfigMap").await
    }

    async fn get_storage_classes(&self) -> Result<Vec<Arc<StorageClass>>> {
        store_state_or_empty(&self.storage_class_store, "StorageClass").await
    }

    async fn get_persistent_volumes(&self) -> Result<Vec<Arc<PersistentVolume>>> {
        store_state_or_empty(&self.persistent_volume_store, "PersistentVolume").await
    }

    async fn get_persistent_volume_claims(&self) -> Result<Vec<Arc<PersistentVolumeClaim>>> {
        store_state_or_empty(&self.persistent_volume_claim_store, "PersistentVolumeClaim").await
    }

    async fn get_nodes(&self) -> Result<Vec<Arc<Node>>> {
        store_state_or_empty(&self.node_store, "Node").await
    }

    async fn get_service_accounts(&self) -> Result<Vec<Arc<ServiceAccount>>> {
        store_state_or_empty(&self.service_account_store, "ServiceAccount").await
    }

    async fn apiserver_version(&self) -> Result<Info> {
        let r = self.client.apiserver_version().await?;
        Ok(r)
    }

    async fn get_cluster_url(&self) -> Result<String> {
        Ok(self.config.cluster_url.to_string())
    }

    async fn get_events(&self) -> Result<Vec<Arc<Event>>> {
        let Some(store) = &self.event_store else {
            return Ok(Vec::new());
        };
        let timeout_duration = event_store_ready_timeout();
        match timeout(timeout_duration, store.wait_until_ready()).await {
            Ok(wait_result) => {
                wait_result.expect("Event store is not ready");
                Ok(store.state())
            }
            Err(_elapsed) => {
                warn!(
                    "Timed out waiting for events after {timeout_duration:?}; returning empty list",
                );
                Ok(Vec::new())
            }
        }
    }

    fn degraded_resource_kinds_handle(&self) -> Arc<Mutex<BTreeSet<String>>> {
        self.degraded_resource_kinds.clone()
    }
}

struct StoreStarter {
    degraded_resource_kinds: Arc<Mutex<BTreeSet<String>>>,
}

impl StoreStarter {
    fn start<T>(&self, api: Api<T>, allowed: bool) -> (Option<Store<T>>, Option<JoinHandle<()>>)
    where
        T: Resource + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
        T::DynamicType: Default + Clone + Eq + std::hash::Hash + Send + Sync + 'static,
    {
        let degraded_resource_kinds = self.degraded_resource_kinds.clone();
        start_store_with_factory(allowed, || {
            make_store_and_watch(api, degraded_resource_kinds)
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
) -> Result<Vec<Arc<T>>>
where
    T: Resource + Clone + Debug + Send + Sync + 'static,
    T::DynamicType: Default + Clone + Eq + std::hash::Hash + Send + Sync + 'static,
{
    match store {
        Some(store) => {
            let timeout_duration = store_ready_timeout();
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

pub(super) fn update_degraded_resource_kinds(
    degraded_resource_kinds: &Arc<Mutex<BTreeSet<String>>>,
    access: &[(bool, &'static str)],
) {
    let mut kinds = degraded_resource_kinds
        .lock()
        .expect("degraded_resource_kinds lock poisoned");
    for (allowed, kind) in access {
        if !allowed {
            kinds.insert((*kind).to_string());
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

        let event_api: Api<Event> = maybe_ns
            .map(|ns| Api::namespaced(client.clone(), ns))
            .unwrap_or_else(|| Api::all(client.clone()));

        let access = AccessChecker::new(client.clone(), maybe_ns);
        let degraded_resource_kinds = Arc::new(Mutex::new(BTreeSet::new()));

        let namespace_allowed = access.can_read(RESOURCE_NAMESPACE).await;
        let pod_allowed = access.can_read(RESOURCE_POD).await;
        let deployment_allowed = access.can_read(RESOURCE_DEPLOYMENT).await;
        let stateful_set_allowed = access.can_read(RESOURCE_STATEFUL_SET).await;
        let replica_set_allowed = access.can_read(RESOURCE_REPLICA_SET).await;
        let daemon_set_allowed = access.can_read(RESOURCE_DAEMON_SET).await;
        let job_allowed = access.can_read(RESOURCE_JOB).await;
        let ingress_allowed = access.can_read(RESOURCE_INGRESS).await;
        let service_allowed = access.can_read(RESOURCE_SERVICE).await;
        let endpoint_slice_allowed = access.can_read(RESOURCE_ENDPOINT_SLICE).await;
        let network_policy_allowed = access.can_read(RESOURCE_NETWORK_POLICY).await;
        let config_map_allowed = access.can_read(RESOURCE_CONFIG_MAP).await;
        let storage_class_allowed = access.can_read(RESOURCE_STORAGE_CLASS).await;
        let persistent_volume_allowed = access.can_read(RESOURCE_PERSISTENT_VOLUME).await;
        let persistent_volume_claim_allowed =
            access.can_read(RESOURCE_PERSISTENT_VOLUME_CLAIM).await;
        let node_allowed = access.can_read(RESOURCE_NODE).await;
        let service_account_allowed = access.can_read(RESOURCE_SERVICE_ACCOUNT).await;
        let event_allowed = access.can_read(RESOURCE_EVENT).await;

        update_degraded_resource_kinds(
            &degraded_resource_kinds,
            &[
                (namespace_allowed, "Namespace"),
                (pod_allowed, "Pod"),
                (deployment_allowed, "Deployment"),
                (stateful_set_allowed, "StatefulSet"),
                (replica_set_allowed, "ReplicaSet"),
                (daemon_set_allowed, "DaemonSet"),
                (job_allowed, "Job"),
                (ingress_allowed, "Ingress"),
                (service_allowed, "Service"),
                (endpoint_slice_allowed, "EndpointSlice"),
                (network_policy_allowed, "NetworkPolicy"),
                (config_map_allowed, "ConfigMap"),
                (storage_class_allowed, "StorageClass"),
                (persistent_volume_allowed, "PersistentVolume"),
                (persistent_volume_claim_allowed, "PersistentVolumeClaim"),
                (node_allowed, "Node"),
                (service_account_allowed, "ServiceAccount"),
                (event_allowed, "Event"),
            ],
        );

        let stores = StoreStarter {
            degraded_resource_kinds: degraded_resource_kinds.clone(),
        };
        let (pod_store, pod_watch) = stores.start(pod_api, pod_allowed);
        let (deployment_store, deployment_watch) = stores.start(deployment_api, deployment_allowed);
        let (stateful_set_store, stateful_set_watch) =
            stores.start(stateful_set_api, stateful_set_allowed);
        let (replica_set_store, replica_set_watch) =
            stores.start(replica_set_api, replica_set_allowed);
        let (daemon_set_store, daemon_set_watch) = stores.start(daemon_set_api, daemon_set_allowed);
        let (job_store, job_watch) = stores.start(job_api, job_allowed);
        let (ingress_store, ingress_watch) = stores.start(ingress_api, ingress_allowed);
        let (service_store, service_watch) = stores.start(service_api, service_allowed);
        let (endpoint_slice_store, endpoint_slice_watch) =
            stores.start(endpoint_slices_api, endpoint_slice_allowed);
        let (network_policy_store, network_policy_watch) =
            stores.start(network_policy_api, network_policy_allowed);
        let (config_map_store, config_map_watch) = stores.start(config_map_api, config_map_allowed);
        let (storage_class_store, storage_class_watch) =
            stores.start(storage_class_api, storage_class_allowed);
        let (persistent_volume_store, persistent_volume_watch) =
            stores.start(persistent_volume_api, persistent_volume_allowed);
        let (persistent_volume_claim_store, persistent_volume_claim_watch) =
            stores.start(persistent_volume_claim_api, persistent_volume_claim_allowed);
        let (node_store, node_watch) = stores.start(node_api, node_allowed);
        let (service_account_store, service_account_watch) =
            stores.start(service_account_api, service_account_allowed);
        let (namespace_store, namespace_watch) = stores.start(namespace_api, namespace_allowed);
        let (event_store, event_store_watch) = stores.start(event_api, event_allowed);

        Ok(Self {
            config: cfg.clone(),
            client: client.clone(),
            degraded_resource_kinds,
            namespace_store,
            namespace_watch,
            pod_store,
            pod_watch,
            deployment_store,
            deployment_watch,
            stateful_set_store,
            stateful_set_watch,
            replica_set_store,
            replica_set_watch,
            daemon_set_store,
            daemon_set_watch,
            job_store,
            job_watch,
            ingress_store,
            ingress_watch,
            service_store,
            service_watch,
            endpoint_slice_store,
            endpoint_slice_watch,
            network_policy_store,
            network_policy_watch,
            config_map_store,
            config_map_watch,
            storage_class_store,
            storage_class_watch,
            persistent_volume_store,
            persistent_volume_watch,
            persistent_volume_claim_store,
            persistent_volume_claim_watch,
            node_store,
            node_watch,
            service_account_store,
            service_account_watch,
            event_store,
            event_store_watch,
        })
    }
}
