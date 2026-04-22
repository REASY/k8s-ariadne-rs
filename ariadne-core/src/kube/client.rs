use crate::prelude::*;
use crate::snapshot::{
    SNAPSHOT_CLUSTER_FILE, SNAPSHOT_CONFIG_MAPS_FILE, SNAPSHOT_DAEMON_SETS_FILE,
    SNAPSHOT_DEPLOYMENTS_FILE, SNAPSHOT_ENDPOINT_SLICES_FILE, SNAPSHOT_EVENTS_FILE,
    SNAPSHOT_INGRESSES_FILE, SNAPSHOT_JOBS_FILE, SNAPSHOT_NAMESPACES_FILE,
    SNAPSHOT_NETWORK_POLICIES_FILE, SNAPSHOT_NODES_FILE, SNAPSHOT_PERSISTENT_VOLUME_CLAIMS_FILE,
    SNAPSHOT_PERSISTENT_VOLUMES_FILE, SNAPSHOT_PODS_FILE, SNAPSHOT_REPLICA_SETS_FILE,
    SNAPSHOT_SERVICE_ACCOUNTS_FILE, SNAPSHOT_SERVICES_FILE, SNAPSHOT_STATEFUL_SETS_FILE,
    SNAPSHOT_STORAGE_CLASSES_FILE, read_json_from_dir, read_list_from_dir,
};
use crate::tls::install_rustls_provider;
use crate::types::Cluster;
use std::any::type_name;

use async_trait::async_trait;
use futures::{StreamExt, future};
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
use k8s_openapi::apimachinery::pkg::version::Info;
use kube::api::ListParams;
use kube::config::KubeConfigOptions;
use kube::runtime::reflector::Store;
use kube::runtime::{WatchStreamExt, reflector, watcher};
use kube::{Api, Client, Config, Resource, ResourceExt};
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::{info, warn};

use crate::kube_access::{
    AccessChecker, RESOURCE_CONFIG_MAP, RESOURCE_DAEMON_SET, RESOURCE_DEPLOYMENT,
    RESOURCE_ENDPOINT_SLICE, RESOURCE_EVENT, RESOURCE_INGRESS, RESOURCE_JOB, RESOURCE_NAMESPACE,
    RESOURCE_NETWORK_POLICY, RESOURCE_NODE, RESOURCE_PERSISTENT_VOLUME,
    RESOURCE_PERSISTENT_VOLUME_CLAIM, RESOURCE_POD, RESOURCE_REPLICA_SET, RESOURCE_SERVICE,
    RESOURCE_SERVICE_ACCOUNT, RESOURCE_STATEFUL_SET, RESOURCE_STORAGE_CLASS,
};
use std::collections::BTreeSet;
use std::sync::Mutex;

#[async_trait]
pub trait KubeClient: Sync + Send {
    async fn get_namespaces(&self) -> Result<Vec<Arc<Namespace>>>;
    async fn get_pods(&self) -> Result<Vec<Arc<Pod>>>;
    async fn get_deployments(&self) -> Result<Vec<Arc<Deployment>>>;
    async fn get_stateful_sets(&self) -> Result<Vec<Arc<StatefulSet>>>;
    async fn get_replica_sets(&self) -> Result<Vec<Arc<ReplicaSet>>>;
    async fn get_daemon_sets(&self) -> Result<Vec<Arc<DaemonSet>>>;
    async fn get_jobs(&self) -> Result<Vec<Arc<Job>>>;
    async fn get_ingresses(&self) -> Result<Vec<Arc<Ingress>>>;
    async fn get_services(&self) -> Result<Vec<Arc<Service>>>;
    async fn get_endpoint_slices(&self) -> Result<Vec<Arc<EndpointSlice>>>;
    async fn get_network_policies(&self) -> Result<Vec<Arc<NetworkPolicy>>>;
    async fn get_config_maps(&self) -> Result<Vec<Arc<ConfigMap>>>;
    async fn get_storage_classes(&self) -> Result<Vec<Arc<StorageClass>>>;
    async fn get_persistent_volumes(&self) -> Result<Vec<Arc<PersistentVolume>>>;
    async fn get_persistent_volume_claims(&self) -> Result<Vec<Arc<PersistentVolumeClaim>>>;
    async fn get_nodes(&self) -> Result<Vec<Arc<Node>>>;
    async fn get_service_accounts(&self) -> Result<Vec<Arc<ServiceAccount>>>;
    async fn apiserver_version(&self) -> Result<Info>;
    async fn get_cluster_url(&self) -> Result<String>;
    async fn get_events(&self) -> Result<Vec<Arc<k8s_openapi::api::events::v1::Event>>>;
    fn degraded_resource_kinds_handle(&self) -> Arc<Mutex<BTreeSet<String>>>;
}

pub struct KubeClientImpl {
    config: Config,
    client: Client,
    degraded_resource_kinds: Arc<Mutex<BTreeSet<String>>>,
    namespace_api: Api<Namespace>,
    pod_api: Api<Pod>,
    deployment_api: Api<Deployment>,
    stateful_set_api: Api<StatefulSet>,
    replica_set_api: Api<ReplicaSet>,
    daemon_set_api: Api<DaemonSet>,
    job_api: Api<Job>,
    ingress_api: Api<Ingress>,
    service_api: Api<Service>,
    endpoint_slices_api: Api<EndpointSlice>,
    network_policy_api: Api<NetworkPolicy>,
    config_map_api: Api<ConfigMap>,
    storage_class_api: Api<StorageClass>,
    persistent_volume_api: Api<PersistentVolume>,
    persistent_volume_claim_api: Api<PersistentVolumeClaim>,
    node_api: Api<Node>,
    service_account_api: Api<ServiceAccount>,
}

impl KubeClientImpl {
    pub async fn new(options: &KubeConfigOptions, maybe_ns: Option<&str>) -> Result<Self> {
        install_rustls_provider();
        let cfg = match Config::from_kubeconfig(options).await {
            Ok(cfg) => {
                info!(
                    "Successfully loaded kubeconfig using KubeConfigOptions(context: {:?}, cluster: {:?}, user: {:?}), cluster_url: {}",
                    options.context, options.cluster, options.user, cfg.cluster_url
                );
                cfg
            }
            Err(err) => {
                info!(
                    "Failed to load kubeconfig using KubeConfigOptions(context: {:?}, cluster: {:?}, user: {:?}), falling back to local in-cluster config. The error was: {err:?}",
                    options.context, options.cluster, options.user
                );
                let in_cluster_cfg = Config::incluster()?;
                info!(
                    "Successfully loaded in-cluster config, cluster_url: {}",
                    in_cluster_cfg.cluster_url
                );
                in_cluster_cfg
            }
        };
        let client = Client::try_from(cfg.clone())?;

        Ok(KubeClientImpl {
            config: cfg,
            client: client.clone(),
            degraded_resource_kinds: Arc::new(Mutex::new(BTreeSet::new())),
            namespace_api: Api::all(client.clone()),
            pod_api: maybe_ns
                .map(|ns| Api::namespaced(client.clone(), ns))
                .unwrap_or_else(|| Api::all(client.clone())),
            deployment_api: maybe_ns
                .map(|ns| Api::namespaced(client.clone(), ns))
                .unwrap_or_else(|| Api::all(client.clone())),
            stateful_set_api: maybe_ns
                .map(|ns| Api::namespaced(client.clone(), ns))
                .unwrap_or_else(|| Api::all(client.clone())),
            replica_set_api: maybe_ns
                .map(|ns| Api::namespaced(client.clone(), ns))
                .unwrap_or_else(|| Api::all(client.clone())),
            daemon_set_api: maybe_ns
                .map(|ns| Api::namespaced(client.clone(), ns))
                .unwrap_or_else(|| Api::all(client.clone())),
            job_api: maybe_ns
                .map(|ns| Api::namespaced(client.clone(), ns))
                .unwrap_or_else(|| Api::all(client.clone())),
            ingress_api: maybe_ns
                .map(|ns| Api::namespaced(client.clone(), ns))
                .unwrap_or_else(|| Api::all(client.clone())),
            service_api: maybe_ns
                .map(|ns| Api::namespaced(client.clone(), ns))
                .unwrap_or_else(|| Api::all(client.clone())),
            endpoint_slices_api: maybe_ns
                .map(|ns| Api::namespaced(client.clone(), ns))
                .unwrap_or_else(|| Api::all(client.clone())),
            network_policy_api: maybe_ns
                .map(|ns| Api::namespaced(client.clone(), ns))
                .unwrap_or_else(|| Api::all(client.clone())),
            config_map_api: maybe_ns
                .map(|ns| Api::namespaced(client.clone(), ns))
                .unwrap_or_else(|| Api::all(client.clone())),
            storage_class_api: Api::all(client.clone()),
            persistent_volume_api: Api::all(client.clone()),
            persistent_volume_claim_api: maybe_ns
                .map(|ns| Api::namespaced(client.clone(), ns))
                .unwrap_or_else(|| Api::all(client.clone())),
            node_api: Api::all(client.clone()),
            service_account_api: maybe_ns
                .map(|ns| Api::namespaced(client.clone(), ns))
                .unwrap_or_else(|| Api::all(client.clone())),
        })
    }
}

const STORE_READY_TIMEOUT_SECONDS: u64 = 10;

#[async_trait]
impl KubeClient for KubeClientImpl {
    async fn get_namespaces(&self) -> Result<Vec<Arc<Namespace>>> {
        get_object(&self.namespace_api).await
    }

    async fn get_pods(&self) -> Result<Vec<Arc<Pod>>> {
        get_object(&self.pod_api).await
    }

    async fn get_deployments(&self) -> Result<Vec<Arc<Deployment>>> {
        get_object(&self.deployment_api).await
    }

    async fn get_stateful_sets(&self) -> Result<Vec<Arc<StatefulSet>>> {
        get_object(&self.stateful_set_api).await
    }

    async fn get_replica_sets(&self) -> Result<Vec<Arc<ReplicaSet>>> {
        get_object(&self.replica_set_api).await
    }

    async fn get_daemon_sets(&self) -> Result<Vec<Arc<DaemonSet>>> {
        get_object(&self.daemon_set_api).await
    }

    async fn get_jobs(&self) -> Result<Vec<Arc<Job>>> {
        get_object(&self.job_api).await
    }

    async fn get_ingresses(&self) -> Result<Vec<Arc<Ingress>>> {
        get_object(&self.ingress_api).await
    }

    async fn get_services(&self) -> Result<Vec<Arc<Service>>> {
        get_object(&self.service_api).await
    }

    async fn get_endpoint_slices(&self) -> Result<Vec<Arc<EndpointSlice>>> {
        get_object(&self.endpoint_slices_api).await
    }

    async fn get_network_policies(&self) -> Result<Vec<Arc<NetworkPolicy>>> {
        get_object(&self.network_policy_api).await
    }

    async fn get_config_maps(&self) -> Result<Vec<Arc<ConfigMap>>> {
        get_object(&self.config_map_api).await
    }

    async fn get_storage_classes(&self) -> Result<Vec<Arc<StorageClass>>> {
        get_object(&self.storage_class_api).await
    }

    async fn get_persistent_volumes(&self) -> Result<Vec<Arc<PersistentVolume>>> {
        get_object(&self.persistent_volume_api).await
    }

    async fn get_persistent_volume_claims(&self) -> Result<Vec<Arc<PersistentVolumeClaim>>> {
        get_object(&self.persistent_volume_claim_api).await
    }

    async fn get_nodes(&self) -> Result<Vec<Arc<Node>>> {
        get_object(&self.node_api).await
    }

    async fn get_service_accounts(&self) -> Result<Vec<Arc<ServiceAccount>>> {
        get_object(&self.service_account_api).await
    }

    async fn apiserver_version(&self) -> Result<Info> {
        let r = self.client.apiserver_version().await?;
        Ok(r)
    }

    async fn get_cluster_url(&self) -> Result<String> {
        Ok(self.config.cluster_url.to_string())
    }

    async fn get_events(&self) -> Result<Vec<Arc<k8s_openapi::api::events::v1::Event>>> {
        let api: Api<k8s_openapi::api::events::v1::Event> = Api::all(self.client.clone());
        get_object(&api).await
    }

    fn degraded_resource_kinds_handle(&self) -> Arc<Mutex<BTreeSet<String>>> {
        self.degraded_resource_kinds.clone()
    }
}

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

fn start_store_if_allowed<T>(
    api: Api<T>,
    allowed: bool,
) -> (Option<Store<T>>, Option<JoinHandle<()>>)
where
    T: Resource + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
    T::DynamicType: Default + Clone + Eq + std::hash::Hash + Send + Sync + 'static,
{
    start_store_with_factory(allowed, || make_store_and_watch(api))
}

fn start_store_with_factory<T, F, Fut>(
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

async fn store_state_or_empty<T>(
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

async fn wait_for_store_readiness<T, Fut, E, F>(
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

fn store_ready_timeout() -> Duration {
    std::env::var("KUBE_STORE_READY_TIMEOUT_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(STORE_READY_TIMEOUT_SECONDS))
}

fn event_store_ready_timeout() -> Duration {
    std::env::var("KUBE_EVENT_STORE_READY_TIMEOUT_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(4))
}

fn update_degraded_resource_kinds(
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
        let cfg = match Config::from_kubeconfig(options).await {
            Ok(cfg) => {
                info!(
                    "Successfully loaded kubeconfig using KubeConfigOptions(context: {:?}, cluster: {:?}, user: {:?}), cluster_url: {}",
                    options.context, options.cluster, options.user, cfg.cluster_url
                );
                cfg
            }
            Err(err) => {
                info!(
                    "Failed to load kubeconfig using KubeConfigOptions(context: {:?}, cluster: {:?}, user: {:?}), falling back to local in-cluster config. The error was: {err:?}",
                    options.context, options.cluster, options.user
                );
                let in_cluster_cfg = Config::incluster()?;
                info!(
                    "Successfully loaded in-cluster config, cluster_url: {}",
                    in_cluster_cfg.cluster_url
                );
                in_cluster_cfg
            }
        };
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

        let (pod_store, pod_watch) = start_store_if_allowed(pod_api, pod_allowed);
        let (deployment_store, deployment_watch) =
            start_store_if_allowed(deployment_api, deployment_allowed);
        let (stateful_set_store, stateful_set_watch) =
            start_store_if_allowed(stateful_set_api, stateful_set_allowed);
        let (replica_set_store, replica_set_watch) =
            start_store_if_allowed(replica_set_api, replica_set_allowed);
        let (daemon_set_store, daemon_set_watch) =
            start_store_if_allowed(daemon_set_api, daemon_set_allowed);
        let (job_store, job_watch) = start_store_if_allowed(job_api, job_allowed);
        let (ingress_store, ingress_watch) = start_store_if_allowed(ingress_api, ingress_allowed);
        let (service_store, service_watch) = start_store_if_allowed(service_api, service_allowed);
        let (endpoint_slice_store, endpoint_slice_watch) =
            start_store_if_allowed(endpoint_slices_api, endpoint_slice_allowed);
        let (network_policy_store, network_policy_watch) =
            start_store_if_allowed(network_policy_api, network_policy_allowed);
        let (config_map_store, config_map_watch) =
            start_store_if_allowed(config_map_api, config_map_allowed);
        let (storage_class_store, storage_class_watch) =
            start_store_if_allowed(storage_class_api, storage_class_allowed);
        let (persistent_volume_store, persistent_volume_watch) =
            start_store_if_allowed(persistent_volume_api, persistent_volume_allowed);
        let (persistent_volume_claim_store, persistent_volume_claim_watch) =
            start_store_if_allowed(persistent_volume_claim_api, persistent_volume_claim_allowed);
        let (node_store, node_watch) = start_store_if_allowed(node_api, node_allowed);
        let (service_account_store, service_account_watch) =
            start_store_if_allowed(service_account_api, service_account_allowed);
        let (namespace_store, namespace_watch) =
            start_store_if_allowed(namespace_api, namespace_allowed);

        let (event_store, event_store_watch) = start_store_if_allowed(event_api, event_allowed);

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

fn make_store_and_watch<T>(
    api: Api<T>,
) -> (Store<T>, impl future::Future<Output = ()> + Send + 'static)
where
    T: Resource + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
    T::DynamicType: Default + Clone + Eq + std::hash::Hash + Send + Sync + 'static,
{
    let (reader, writer) = reflector::store();
    let fut = reflector(writer, watcher(api, Default::default()))
        .modify(|item| {
            item.managed_fields_mut().clear();
        })
        .for_each(|x| {
            let _ = x.inspect_err(|err| {
                let resource_type = type_name::<T>();
                let dynamic_type = type_name::<T::DynamicType>();
                warn!("Error in watch loop for the type [{resource_type}:{dynamic_type}] {err:?}");
            });
            future::ready(())
        });
    (reader, fut)
}

async fn get_object<T: Clone + DeserializeOwned + Debug>(api: &Api<T>) -> Result<Vec<Arc<T>>> {
    let mut r: Vec<Arc<T>> = Vec::new();
    let mut continue_token: Option<String> = None;
    loop {
        let lp = match continue_token {
            None => ListParams::default(),
            Some(t) => ListParams::default().continue_token(&t),
        };
        let pods = api.list(&lp).await?;
        continue_token = pods.metadata.continue_.clone();

        for p in pods {
            r.push(Arc::new(p))
        }
        if continue_token.is_none() {
            break;
        }
    }
    Ok(r)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::write_json_to_dir;
    use crate::snapshot::write_list_to_dir;
    use crate::types::ObjectIdentifier;
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
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(prefix: &str) -> std::result::Result<Self, Box<dyn std::error::Error>> {
            let mut path = std::env::temp_dir();
            let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
            path.push(format!("{prefix}_{}_{}", std::process::id(), nanos));
            fs::create_dir_all(&path)?;
            Ok(Self { path })
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn test_namespace(name: &str) -> Arc<Namespace> {
        Arc::new(Namespace {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                uid: Some(format!("namespace:{name}")),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    fn test_cluster(name: &str) -> Cluster {
        Cluster::new(
            ObjectIdentifier {
                uid: format!("Cluster:{name}"),
                name: name.to_string(),
                namespace: None,
                resource_version: None,
            },
            "https://example.test",
            Info {
                major: "1".to_string(),
                minor: "32".to_string(),
                ..Default::default()
            },
        )
    }

    fn write_snapshot_fixture(dir: &Path) {
        write_json_to_dir(dir, SNAPSHOT_CLUSTER_FILE, &test_cluster("test-cluster"))
            .expect("write cluster snapshot");

        let namespaces = vec![test_namespace("team-a")];
        let pods: Vec<Arc<Pod>> = Vec::new();
        let deployments: Vec<Arc<Deployment>> = Vec::new();
        let stateful_sets: Vec<Arc<StatefulSet>> = Vec::new();
        let replica_sets: Vec<Arc<ReplicaSet>> = Vec::new();
        let daemon_sets: Vec<Arc<DaemonSet>> = Vec::new();
        let jobs: Vec<Arc<Job>> = Vec::new();
        let ingresses: Vec<Arc<Ingress>> = Vec::new();
        let services: Vec<Arc<Service>> = Vec::new();
        let endpoint_slices: Vec<Arc<EndpointSlice>> = Vec::new();
        let network_policies: Vec<Arc<NetworkPolicy>> = Vec::new();
        let config_maps: Vec<Arc<ConfigMap>> = Vec::new();
        let storage_classes: Vec<Arc<StorageClass>> = Vec::new();
        let persistent_volumes: Vec<Arc<PersistentVolume>> = Vec::new();
        let persistent_volume_claims: Vec<Arc<PersistentVolumeClaim>> = Vec::new();
        let nodes: Vec<Arc<Node>> = Vec::new();
        let service_accounts: Vec<Arc<ServiceAccount>> = Vec::new();
        let events: Vec<Arc<Event>> = Vec::new();

        write_list_to_dir(dir, SNAPSHOT_NAMESPACES_FILE, &namespaces).expect("write namespaces");
        write_list_to_dir(dir, SNAPSHOT_PODS_FILE, &pods).expect("write pods");
        write_list_to_dir(dir, SNAPSHOT_DEPLOYMENTS_FILE, &deployments).expect("write deployments");
        write_list_to_dir(dir, SNAPSHOT_STATEFUL_SETS_FILE, &stateful_sets)
            .expect("write stateful sets");
        write_list_to_dir(dir, SNAPSHOT_REPLICA_SETS_FILE, &replica_sets)
            .expect("write replica sets");
        write_list_to_dir(dir, SNAPSHOT_DAEMON_SETS_FILE, &daemon_sets).expect("write daemon sets");
        write_list_to_dir(dir, SNAPSHOT_JOBS_FILE, &jobs).expect("write jobs");
        write_list_to_dir(dir, SNAPSHOT_INGRESSES_FILE, &ingresses).expect("write ingresses");
        write_list_to_dir(dir, SNAPSHOT_SERVICES_FILE, &services).expect("write services");
        write_list_to_dir(dir, SNAPSHOT_ENDPOINT_SLICES_FILE, &endpoint_slices)
            .expect("write endpoint slices");
        write_list_to_dir(dir, SNAPSHOT_NETWORK_POLICIES_FILE, &network_policies)
            .expect("write network policies");
        write_list_to_dir(dir, SNAPSHOT_CONFIG_MAPS_FILE, &config_maps).expect("write config maps");
        write_list_to_dir(dir, SNAPSHOT_STORAGE_CLASSES_FILE, &storage_classes)
            .expect("write storage classes");
        write_list_to_dir(dir, SNAPSHOT_PERSISTENT_VOLUMES_FILE, &persistent_volumes)
            .expect("write persistent volumes");
        write_list_to_dir(
            dir,
            SNAPSHOT_PERSISTENT_VOLUME_CLAIMS_FILE,
            &persistent_volume_claims,
        )
        .expect("write persistent volume claims");
        write_list_to_dir(dir, SNAPSHOT_NODES_FILE, &nodes).expect("write nodes");
        write_list_to_dir(dir, SNAPSHOT_SERVICE_ACCOUNTS_FILE, &service_accounts)
            .expect("write service accounts");
        write_list_to_dir(dir, SNAPSHOT_EVENTS_FILE, &events).expect("write events");
    }

    #[test]
    fn update_degraded_resource_kinds_marks_denied_resources_only() {
        let degraded = Arc::new(Mutex::new(BTreeSet::new()));
        update_degraded_resource_kinds(
            &degraded,
            &[
                (true, "Pod"),
                (false, "Node"),
                (false, "Service"),
                (true, "Deployment"),
            ],
        );
        let values = degraded
            .lock()
            .expect("degraded lock poisoned")
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(values, vec!["Node".to_string(), "Service".to_string()]);
    }

    #[tokio::test]
    async fn start_store_with_factory_respects_allowed_flag() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_clone = calls.clone();
        let (store_none, watch_none) = start_store_with_factory::<Namespace, _, _>(false, || {
            calls_clone.fetch_add(1, Ordering::SeqCst);
            let (store, _writer) = reflector::store();
            (store, future::ready(()))
        });
        assert!(store_none.is_none());
        assert!(watch_none.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        let calls_clone = calls.clone();
        let (store_some, watch_some) = start_store_with_factory::<Namespace, _, _>(true, || {
            calls_clone.fetch_add(1, Ordering::SeqCst);
            let (store, _writer) = reflector::store();
            (store, future::ready(()))
        });
        assert!(store_some.is_some());
        assert!(watch_some.is_some());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        watch_some
            .expect("missing watch handle")
            .await
            .expect("watch task failed");
    }

    #[tokio::test]
    async fn store_state_or_empty_returns_empty_when_store_absent() {
        let state = store_state_or_empty::<Namespace>(&None, "Namespace")
            .await
            .expect("state fetch should succeed");
        assert!(state.is_empty());
    }

    #[tokio::test]
    async fn wait_for_store_readiness_covers_success_error_and_timeout() {
        let ready_state = wait_for_store_readiness(
            future::ready(Ok::<(), std::io::Error>(())),
            || vec![test_namespace("team-a")],
            "Namespace",
            Duration::from_millis(20),
        )
        .await
        .expect("ready path should return state");
        assert_eq!(ready_state.len(), 1);
        assert_eq!(ready_state[0].metadata.name.as_deref(), Some("team-a"));

        let err = wait_for_store_readiness::<Namespace, _, _, _>(
            future::ready(Err::<(), _>(std::io::Error::other("boom"))),
            Vec::new,
            "Namespace",
            Duration::from_millis(20),
        )
        .await
        .expect_err("error path should fail");
        assert!(err.to_string().contains("Namespace store is not ready"));

        let timeout_err = wait_for_store_readiness::<Namespace, _, _, _>(
            future::pending::<std::result::Result<(), std::io::Error>>(),
            Vec::new,
            "Namespace",
            Duration::from_millis(1),
        )
        .await
        .expect_err("timeout path should fail");
        assert!(
            timeout_err
                .to_string()
                .contains("Timed out waiting for Namespace store readiness")
        );
    }

    #[test]
    fn timeout_env_parsing_uses_defaults_and_valid_values() {
        let _guard = env_lock().lock().expect("env lock poisoned");
        let original_store = std::env::var("KUBE_STORE_READY_TIMEOUT_SECONDS").ok();
        let original_event = std::env::var("KUBE_EVENT_STORE_READY_TIMEOUT_SECONDS").ok();

        unsafe {
            std::env::remove_var("KUBE_STORE_READY_TIMEOUT_SECONDS");
            std::env::remove_var("KUBE_EVENT_STORE_READY_TIMEOUT_SECONDS");
        }
        assert_eq!(store_ready_timeout(), Duration::from_secs(10));
        assert_eq!(event_store_ready_timeout(), Duration::from_secs(4));

        unsafe {
            std::env::set_var("KUBE_STORE_READY_TIMEOUT_SECONDS", "17");
            std::env::set_var("KUBE_EVENT_STORE_READY_TIMEOUT_SECONDS", "9");
        }
        assert_eq!(store_ready_timeout(), Duration::from_secs(17));
        assert_eq!(event_store_ready_timeout(), Duration::from_secs(9));

        unsafe {
            std::env::set_var("KUBE_STORE_READY_TIMEOUT_SECONDS", "invalid");
            std::env::set_var("KUBE_EVENT_STORE_READY_TIMEOUT_SECONDS", "invalid");
        }
        assert_eq!(store_ready_timeout(), Duration::from_secs(10));
        assert_eq!(event_store_ready_timeout(), Duration::from_secs(4));

        match original_store {
            Some(value) => unsafe { std::env::set_var("KUBE_STORE_READY_TIMEOUT_SECONDS", value) },
            None => unsafe { std::env::remove_var("KUBE_STORE_READY_TIMEOUT_SECONDS") },
        }
        match original_event {
            Some(value) => unsafe {
                std::env::set_var("KUBE_EVENT_STORE_READY_TIMEOUT_SECONDS", value)
            },
            None => unsafe { std::env::remove_var("KUBE_EVENT_STORE_READY_TIMEOUT_SECONDS") },
        }
    }

    #[tokio::test]
    async fn snapshot_kube_client_from_dir_loads_state_and_has_shared_degraded_handle() {
        let dir = TempDir::new("snapshot_kube_client").expect("temp dir creation should succeed");
        write_snapshot_fixture(dir.path());

        let client =
            SnapshotKubeClient::from_dir(dir.path()).expect("snapshot client should load fixture");
        let namespaces = client
            .get_namespaces()
            .await
            .expect("namespaces should be available");
        assert_eq!(namespaces.len(), 1);
        assert_eq!(namespaces[0].metadata.name.as_deref(), Some("team-a"));
        assert_eq!(
            client
                .get_cluster_url()
                .await
                .expect("cluster url should be present"),
            "https://example.test"
        );

        let degraded_a = client.degraded_resource_kinds_handle();
        let degraded_b = client.degraded_resource_kinds_handle();
        assert!(Arc::ptr_eq(&degraded_a, &degraded_b));
        assert!(
            degraded_a
                .lock()
                .expect("degraded lock poisoned")
                .is_empty()
        );
    }
}
