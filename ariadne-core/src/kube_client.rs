use crate::prelude::*;
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
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::{info, warn};

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
    namespace_store: Store<Namespace>,
    #[allow(unused)]
    namespace_watch: JoinHandle<()>,
    pod_store: Store<Pod>,
    #[allow(unused)]
    pod_watch: JoinHandle<()>,
    deployment_store: Store<Deployment>,
    #[allow(unused)]
    deployment_watch: JoinHandle<()>,
    stateful_set_store: Store<StatefulSet>,
    #[allow(unused)]
    stateful_set_watch: JoinHandle<()>,
    replica_set_store: Store<ReplicaSet>,
    #[allow(unused)]
    replica_set_watch: JoinHandle<()>,
    daemon_set_store: Store<DaemonSet>,
    #[allow(unused)]
    daemon_set_watch: JoinHandle<()>,
    job_store: Store<Job>,
    #[allow(unused)]
    job_watch: JoinHandle<()>,
    ingress_store: Store<Ingress>,
    #[allow(unused)]
    ingress_watch: JoinHandle<()>,
    service_store: Store<Service>,
    #[allow(unused)]
    service_watch: JoinHandle<()>,
    endpoint_slice_store: Store<EndpointSlice>,
    #[allow(unused)]
    endpoint_slice_watch: JoinHandle<()>,
    network_policy_store: Store<NetworkPolicy>,
    #[allow(unused)]
    network_policy_watch: JoinHandle<()>,
    config_map_store: Store<ConfigMap>,
    #[allow(unused)]
    config_map_watch: JoinHandle<()>,
    storage_class_store: Store<StorageClass>,
    #[allow(unused)]
    storage_class_watch: JoinHandle<()>,
    persistent_volume_store: Store<PersistentVolume>,
    #[allow(unused)]
    persistent_volume_watch: JoinHandle<()>,
    persistent_volume_claim_store: Store<PersistentVolumeClaim>,
    #[allow(unused)]
    persistent_volume_claim_watch: JoinHandle<()>,
    node_store: Store<Node>,
    #[allow(unused)]
    node_watch: JoinHandle<()>,
    service_account_store: Store<ServiceAccount>,
    #[allow(unused)]
    service_account_watch: JoinHandle<()>,
    event_store: Store<Event>,
    #[allow(unused)]
    event_store_watch: JoinHandle<()>,
}

#[async_trait]
impl KubeClient for CachedKubeClient {
    async fn get_namespaces(&self) -> Result<Vec<Arc<Namespace>>> {
        let store = &self.namespace_store;
        store
            .wait_until_ready()
            .await
            .expect("Namespace store is not ready");
        Ok(store.state())
    }

    async fn get_pods(&self) -> Result<Vec<Arc<Pod>>> {
        let store = &self.pod_store;
        store
            .wait_until_ready()
            .await
            .expect("Pod store is not ready");
        Ok(store.state())
    }

    async fn get_deployments(&self) -> Result<Vec<Arc<Deployment>>> {
        let store = &self.deployment_store;
        store
            .wait_until_ready()
            .await
            .expect("Deployment store is not ready");
        Ok(store.state())
    }

    async fn get_stateful_sets(&self) -> Result<Vec<Arc<StatefulSet>>> {
        let store = &self.stateful_set_store;
        store
            .wait_until_ready()
            .await
            .expect("StatefulSet store is not ready");
        Ok(store.state())
    }

    async fn get_replica_sets(&self) -> Result<Vec<Arc<ReplicaSet>>> {
        let store = &self.replica_set_store;
        store
            .wait_until_ready()
            .await
            .expect("ReplicaSet store is not ready");
        Ok(store.state())
    }

    async fn get_daemon_sets(&self) -> Result<Vec<Arc<DaemonSet>>> {
        let store = &self.daemon_set_store;
        store
            .wait_until_ready()
            .await
            .expect("DaemonSet store is not ready");
        Ok(store.state())
    }

    async fn get_jobs(&self) -> Result<Vec<Arc<Job>>> {
        let store = &self.job_store;
        store
            .wait_until_ready()
            .await
            .expect("Job store is not ready");
        Ok(store.state())
    }

    async fn get_ingresses(&self) -> Result<Vec<Arc<Ingress>>> {
        let store = &self.ingress_store;
        store
            .wait_until_ready()
            .await
            .expect("Ingress store is not ready");
        Ok(store.state())
    }

    async fn get_services(&self) -> Result<Vec<Arc<Service>>> {
        let store = &self.service_store;
        store
            .wait_until_ready()
            .await
            .expect("Service store is not ready");
        Ok(store.state())
    }

    async fn get_endpoint_slices(&self) -> Result<Vec<Arc<EndpointSlice>>> {
        let store = &self.endpoint_slice_store;
        store
            .wait_until_ready()
            .await
            .expect("EndpointSlice store is not ready");
        Ok(store.state())
    }

    async fn get_network_policies(&self) -> Result<Vec<Arc<NetworkPolicy>>> {
        let store = &self.network_policy_store;
        store
            .wait_until_ready()
            .await
            .expect("NetworkPolicy store is not ready");
        Ok(store.state())
    }

    async fn get_config_maps(&self) -> Result<Vec<Arc<ConfigMap>>> {
        let store = &self.config_map_store;
        store
            .wait_until_ready()
            .await
            .expect("ConfigMap store is not ready");
        Ok(store.state())
    }

    async fn get_storage_classes(&self) -> Result<Vec<Arc<StorageClass>>> {
        let store = &self.storage_class_store;
        store
            .wait_until_ready()
            .await
            .expect("StorageClass store is not ready");
        Ok(store.state())
    }

    async fn get_persistent_volumes(&self) -> Result<Vec<Arc<PersistentVolume>>> {
        let store = &self.persistent_volume_store;
        store
            .wait_until_ready()
            .await
            .expect("PersistentVolume store is not ready");
        Ok(store.state())
    }

    async fn get_persistent_volume_claims(&self) -> Result<Vec<Arc<PersistentVolumeClaim>>> {
        let store = &self.persistent_volume_claim_store;
        store
            .wait_until_ready()
            .await
            .expect("PersistentVolumeClaim store is not ready");
        Ok(store.state())
    }

    async fn get_nodes(&self) -> Result<Vec<Arc<Node>>> {
        let store = &self.node_store;
        store
            .wait_until_ready()
            .await
            .expect("Node store is not ready");
        Ok(store.state())
    }

    async fn get_service_accounts(&self) -> Result<Vec<Arc<ServiceAccount>>> {
        let store = &self.service_account_store;
        store
            .wait_until_ready()
            .await
            .expect("ServiceAccount store is not ready");
        Ok(store.state())
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
        let store = &self.event_store;
        match timeout(Duration::from_secs(2), store.wait_until_ready()).await {
            Ok(wait_result) => {
                wait_result.expect("Event store is not ready");
                let state = store.state();
                Ok(state)
            }
            Err(_elapsed) => {
                warn!("Timed out waiting for events after 2s; returning empty list",);
                Ok(Vec::new())
            }
        }
    }
}
impl CachedKubeClient {
    pub async fn new(options: &KubeConfigOptions, maybe_ns: Option<&str>) -> Result<Self> {
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

        let event_api: Api<Event> = Api::all(client.clone());

        let (pod_store, pod_watch) = make_store_and_watch(pod_api);
        let (deployment_store, deployment_watch) = make_store_and_watch(deployment_api);
        let (stateful_set_store, stateful_set_watch) = make_store_and_watch(stateful_set_api);
        let (replica_set_store, replica_set_watch) = make_store_and_watch(replica_set_api);
        let (daemon_set_store, daemon_set_watch) = make_store_and_watch(daemon_set_api);
        let (job_store, job_watch) = make_store_and_watch(job_api);
        let (ingress_store, ingress_watch) = make_store_and_watch(ingress_api);
        let (service_store, service_watch) = make_store_and_watch(service_api);
        let (endpoint_slice_store, endpoint_slice_watch) =
            make_store_and_watch(endpoint_slices_api);
        let (network_policy_store, network_policy_watch) = make_store_and_watch(network_policy_api);
        let (config_map_store, config_map_watch) = make_store_and_watch(config_map_api);
        let (storage_class_store, storage_class_watch) = make_store_and_watch(storage_class_api);
        let (persistent_volume_store, persistent_volume_watch) =
            make_store_and_watch(persistent_volume_api);
        let (persistent_volume_claim_store, persistent_volume_claim_watch) =
            make_store_and_watch(persistent_volume_claim_api);
        let (node_store, node_watch) = make_store_and_watch(node_api);
        let (service_account_store, service_account_watch) =
            make_store_and_watch(service_account_api);
        let (namespace_store, namespace_watch) = make_store_and_watch(namespace_api);

        let (event_store, event_watch) = make_store_and_watch(event_api);

        Ok(Self {
            config: cfg.clone(),
            client: client.clone(),
            namespace_store,
            namespace_watch: tokio::spawn(namespace_watch),
            pod_store,
            pod_watch: tokio::spawn(pod_watch),
            deployment_store,
            deployment_watch: tokio::spawn(deployment_watch),
            stateful_set_store,
            stateful_set_watch: tokio::spawn(stateful_set_watch),
            replica_set_store,
            replica_set_watch: tokio::spawn(replica_set_watch),
            daemon_set_store,
            daemon_set_watch: tokio::spawn(daemon_set_watch),
            job_store,
            job_watch: tokio::spawn(job_watch),
            ingress_store,
            ingress_watch: tokio::spawn(ingress_watch),
            service_store,
            service_watch: tokio::spawn(service_watch),
            endpoint_slice_store,
            endpoint_slice_watch: tokio::spawn(endpoint_slice_watch),
            network_policy_store,
            network_policy_watch: tokio::spawn(network_policy_watch),
            config_map_store,
            config_map_watch: tokio::spawn(config_map_watch),
            storage_class_store,
            storage_class_watch: tokio::spawn(storage_class_watch),
            persistent_volume_store,
            persistent_volume_watch: tokio::spawn(persistent_volume_watch),
            persistent_volume_claim_store,
            persistent_volume_claim_watch: tokio::spawn(persistent_volume_claim_watch),
            node_store,
            node_watch: tokio::spawn(node_watch),
            service_account_store,
            service_account_watch: tokio::spawn(service_account_watch),
            event_store,
            event_store_watch: tokio::spawn(event_watch),
        })
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
