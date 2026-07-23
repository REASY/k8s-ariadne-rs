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
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::kube_access::{
    AccessChecker, AccessDecision, RESOURCE_CONFIG_MAP, RESOURCE_DAEMON_SET, RESOURCE_DEPLOYMENT,
    RESOURCE_ENDPOINT_SLICE, RESOURCE_EVENT, RESOURCE_INGRESS, RESOURCE_JOB, RESOURCE_NAMESPACE,
    RESOURCE_NETWORK_POLICY, RESOURCE_NODE, RESOURCE_PERSISTENT_VOLUME,
    RESOURCE_PERSISTENT_VOLUME_CLAIM, RESOURCE_POD, RESOURCE_REPLICA_SET, RESOURCE_SERVICE,
    RESOURCE_SERVICE_ACCOUNT, RESOURCE_STATEFUL_SET, RESOURCE_STORAGE_CLASS,
};
use std::collections::BTreeSet;
use std::sync::Mutex;

#[path = "client/cached.rs"]
mod cached;
#[path = "client/snapshot.rs"]
mod snapshot;

pub use cached::CachedKubeClient;
#[cfg(test)]
use cached::{
    ReflectorTasks, event_store_ready_timeout, namespace_watcher_config, start_store_with_factory,
    store_ready_timeout, store_state_or_empty, store_state_or_empty_with_timeout,
    update_degraded_resource_kinds, wait_for_store_readiness,
};
pub use snapshot::SnapshotKubeClient;

#[derive(Clone)]
struct WatchHealth {
    degraded_resource_kinds: Arc<Mutex<BTreeSet<String>>>,
    awaiting_initial_access: Arc<Mutex<BTreeSet<String>>>,
}

impl WatchHealth {
    fn new() -> Self {
        Self {
            degraded_resource_kinds: Arc::new(Mutex::new(BTreeSet::new())),
            awaiting_initial_access: Arc::new(Mutex::new(BTreeSet::new())),
        }
    }

    fn degraded_handle(&self) -> Arc<Mutex<BTreeSet<String>>> {
        self.degraded_resource_kinds.clone()
    }

    fn is_awaiting_initial_access(&self, resource_kind: &str) -> bool {
        self.awaiting_initial_access
            .lock()
            .expect("awaiting_initial_access lock poisoned")
            .contains(resource_kind)
    }
}

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
    event_api: Api<Event>,
}

impl KubeClientImpl {
    pub async fn new(options: &KubeConfigOptions, maybe_ns: Option<&str>) -> Result<Self> {
        install_rustls_provider();
        let cfg = load_kube_config(options).await?;
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
            event_api: event_api(client, maybe_ns),
        })
    }
}

fn event_api(client: Client, maybe_ns: Option<&str>) -> Api<Event> {
    maybe_ns
        .map(|namespace| Api::namespaced(client.clone(), namespace))
        .unwrap_or_else(|| Api::all(client))
}

pub(super) async fn load_kube_config(options: &KubeConfigOptions) -> Result<Config> {
    match Config::from_kubeconfig(options).await {
        Ok(cfg) => {
            info!(
                "Successfully loaded kubeconfig using KubeConfigOptions(context: {:?}, cluster: {:?}, user: {:?}), cluster_url: {}",
                options.context, options.cluster, options.user, cfg.cluster_url
            );
            Ok(cfg)
        }
        Err(err)
            if explicit_kubeconfig_requested(options, std::env::var_os("KUBECONFIG").is_some()) =>
        {
            warn!(
                "Failed to load explicitly requested kubeconfig using KubeConfigOptions(context: {:?}, cluster: {:?}, user: {:?}): {err:?}",
                options.context, options.cluster, options.user
            );
            Err(err.into())
        }
        Err(err) => {
            info!(
                "Failed to load default kubeconfig; attempting in-cluster configuration. The error was: {err:?}"
            );
            let in_cluster_cfg = Config::incluster()?;
            info!(
                "Successfully loaded in-cluster config, cluster_url: {}",
                in_cluster_cfg.cluster_url
            );
            Ok(in_cluster_cfg)
        }
    }
}

fn explicit_kubeconfig_requested(options: &KubeConfigOptions, kubeconfig_env_is_set: bool) -> bool {
    kubeconfig_env_is_set
        || options.context.is_some()
        || options.cluster.is_some()
        || options.user.is_some()
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
        get_object(&self.event_api).await
    }

    fn degraded_resource_kinds_handle(&self) -> Arc<Mutex<BTreeSet<String>>> {
        self.degraded_resource_kinds.clone()
    }
}

fn make_store_and_watch<T>(
    api: Api<T>,
    watcher_config: watcher::Config,
    watch_health: WatchHealth,
) -> (Store<T>, impl future::Future<Output = ()> + Send + 'static)
where
    T: Resource + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
    T::DynamicType: Default + Clone + Eq + std::hash::Hash + Send + Sync + 'static,
{
    let (reader, writer) = reflector::store();
    let resource_kind = T::kind(&T::DynamicType::default()).into_owned();
    let fut = reflector(writer, watcher(api, watcher_config).default_backoff())
        .modify(|item| {
            item.managed_fields_mut().clear();
        })
        .for_each(move |result| {
            match &result {
                Ok(_) => update_watch_health(&watch_health, &resource_kind, true),
                Err(err) => {
                    update_watch_health(&watch_health, &resource_kind, false);
                    warn!("Error in watch loop for {resource_kind}: {err:?}");
                }
            }
            future::ready(())
        });
    (reader, fut)
}

fn update_watch_health(watch_health: &WatchHealth, resource_kind: &str, healthy: bool) {
    let mut degraded = watch_health
        .degraded_resource_kinds
        .lock()
        .expect("degraded_resource_kinds lock poisoned");
    if healthy {
        degraded.remove(resource_kind);
        watch_health
            .awaiting_initial_access
            .lock()
            .expect("awaiting_initial_access lock poisoned")
            .remove(resource_kind);
    } else {
        degraded.insert(resource_kind.to_string());
    }
}

async fn get_object<T: Clone + DeserializeOwned + Debug>(api: &Api<T>) -> Result<Vec<Arc<T>>> {
    let mut objects = Vec::new();
    let mut continue_token: Option<String> = None;
    loop {
        let params = match continue_token {
            None => ListParams::default(),
            Some(token) => ListParams::default().continue_token(&token),
        };
        let page = api.list(&params).await?;
        continue_token = page.metadata.continue_.clone();
        objects.extend(page.into_iter().map(Arc::new));
        if continue_token.is_none() {
            break;
        }
    }
    Ok(objects)
}

#[cfg(test)]
#[path = "client/tests.rs"]
mod tests;
