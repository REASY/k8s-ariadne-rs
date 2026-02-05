use crate::prelude::*;
use crate::snapshot::{
    read_json_from_dir, read_list_from_dir, SNAPSHOT_CLUSTER_FILE, SNAPSHOT_CONFIG_MAPS_FILE,
    SNAPSHOT_DAEMON_SETS_FILE, SNAPSHOT_DEPLOYMENTS_FILE, SNAPSHOT_ENDPOINT_SLICES_FILE,
    SNAPSHOT_EVENTS_FILE, SNAPSHOT_INGRESSES_FILE, SNAPSHOT_JOBS_FILE, SNAPSHOT_NAMESPACES_FILE,
    SNAPSHOT_NETWORK_POLICIES_FILE, SNAPSHOT_NODES_FILE, SNAPSHOT_PERSISTENT_VOLUMES_FILE,
    SNAPSHOT_PERSISTENT_VOLUME_CLAIMS_FILE, SNAPSHOT_PODS_FILE, SNAPSHOT_REPLICA_SETS_FILE,
    SNAPSHOT_SERVICES_FILE, SNAPSHOT_SERVICE_ACCOUNTS_FILE, SNAPSHOT_STATEFUL_SETS_FILE,
    SNAPSHOT_STORAGE_CLASSES_FILE,
};
use crate::tls::install_rustls_provider;
use crate::types::Cluster;
use std::any::type_name;

use async_trait::async_trait;
use futures::{future, StreamExt};
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
use kube::api::{ListParams, LogParams};
use kube::config::KubeConfigOptions;
use kube::runtime::reflector::Store;
use kube::runtime::{reflector, watcher, WatchStreamExt};
use kube::{Api, Client, Config, Resource, ResourceExt};
use serde::de::DeserializeOwned;
use std::fmt::Debug;
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
    async fn get_pod_logs(
        &self,
        namespace: &str,
        pod_name: &str,
        container: Option<String>,
    ) -> Result<String>;
    async fn get_events(&self) -> Result<Vec<Arc<k8s_openapi::api::events::v1::Event>>>;
}

pub struct KubeClientImpl {
    config: Config,
    client: Client,
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
                info!("Successfully loaded kubeconfig using KubeConfigOptions(context: {:?}, cluster: {:?}, user: {:?}), cluster_url: {}", options.context, options.cluster, options.user, cfg.cluster_url);
                cfg
            }
            Err(err) => {
                info!("Failed to load kubeconfig using KubeConfigOptions(context: {:?}, cluster: {:?}, user: {:?}), falling back to local in-cluster config. The error was: {err:?}", options.context, options.cluster, options.user);
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

const LAST_N_LOG_LINES: i64 = 50;
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

    async fn get_pod_logs(
        &self,
        namespace: &str,
        pod_name: &str,
        container: Option<String>,
    ) -> Result<String> {
        let api: Api<Pod> = Api::namespaced(self.client.clone(), namespace);
        let log_params = LogParams {
            container,
            follow: false,
            limit_bytes: None,
            pretty: false,
            previous: false,
            since_seconds: None,
            since_time: None,
            tail_lines: Some(LAST_N_LOG_LINES),
            timestamps: true,
        };

        let logs = api.logs(pod_name, &log_params).await?;
        Ok(logs)
    }

    async fn get_events(&self) -> Result<Vec<Arc<k8s_openapi::api::events::v1::Event>>> {
        let api: Api<k8s_openapi::api::events::v1::Event> = Api::all(self.client.clone());
        get_object(&api).await
    }
}

pub struct CachedKubeClient {
    config: Config,
    client: Client,
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

    async fn get_pod_logs(
        &self,
        namespace: &str,
        pod_name: &str,
        container: Option<String>,
    ) -> Result<String> {
        let api: Api<Pod> = Api::namespaced(self.client.clone(), namespace);
        let log_params = LogParams {
            container,
            follow: false,
            limit_bytes: None,
            pretty: false,
            previous: false,
            since_seconds: None,
            since_time: None,
            tail_lines: Some(LAST_N_LOG_LINES),
            timestamps: true,
        };

        let logs = api.logs(pod_name, &log_params).await?;
        Ok(logs)
    }

    async fn get_events(&self) -> Result<Vec<Arc<Event>>> {
        let Some(store) = &self.event_store else {
            return Ok(Vec::new());
        };
        match timeout(Duration::from_secs(2), store.wait_until_ready()).await {
            Ok(wait_result) => {
                wait_result.expect("Event store is not ready");
                Ok(store.state())
            }
            Err(_elapsed) => {
                warn!("Timed out waiting for events after 2s; returning empty list",);
                Ok(Vec::new())
            }
        }
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
    if allowed {
        let (store, watch) = make_store_and_watch(api);
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
            match timeout(timeout_duration, store.wait_until_ready()).await {
                Ok(wait_result) => {
                    if let Err(err) = wait_result {
                        return Err(std::io::Error::other(format!(
                            "{kind} store is not ready: {err}"
                        ))
                        .into());
                    }
                    Ok(store.state())
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
        None => Ok(Vec::new()),
    }
}

fn store_ready_timeout() -> Duration {
    std::env::var("KUBE_STORE_READY_TIMEOUT_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(STORE_READY_TIMEOUT_SECONDS))
}

impl CachedKubeClient {
    pub async fn new(options: &KubeConfigOptions, maybe_ns: Option<&str>) -> Result<Self> {
        install_rustls_provider();
        let cfg = match Config::from_kubeconfig(options).await {
            Ok(cfg) => {
                info!("Successfully loaded kubeconfig using KubeConfigOptions(context: {:?}, cluster: {:?}, user: {:?}), cluster_url: {}", options.context, options.cluster, options.user, cfg.cluster_url);
                cfg
            }
            Err(err) => {
                info!("Failed to load kubeconfig using KubeConfigOptions(context: {:?}, cluster: {:?}, user: {:?}), falling back to local in-cluster config. The error was: {err:?}", options.context, options.cluster, options.user);
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

    async fn get_pod_logs(
        &self,
        _namespace: &str,
        _pod_name: &str,
        _container: Option<String>,
    ) -> Result<String> {
        warn!("SnapshotKubeClient does not support pod logs");
        Ok(String::new())
    }

    async fn get_events(&self) -> Result<Vec<Arc<Event>>> {
        Ok(self.events.clone())
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
