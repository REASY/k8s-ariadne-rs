use crate::prelude::*;

use async_trait::async_trait;
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{
    ConfigMap, Endpoints, Node, PersistentVolume, PersistentVolumeClaim, Pod, Service,
    ServiceAccount,
};
use k8s_openapi::api::networking::v1::{Ingress, NetworkPolicy};
use k8s_openapi::api::storage::v1::StorageClass;
use kube::api::ListParams;
use kube::config::KubeConfigOptions;
use kube::{Api, Client, Config};
use serde::de::DeserializeOwned;
use std::fmt::Debug;

#[async_trait]
pub trait KubeClient: Sync + Send {
    async fn get_pods(&self) -> Result<Vec<Pod>>;
    async fn get_deployments(&self) -> Result<Vec<Deployment>>;
    async fn get_stateful_sets(&self) -> Result<Vec<StatefulSet>>;
    async fn get_replica_sets(&self) -> Result<Vec<ReplicaSet>>;
    async fn get_daemon_sets(&self) -> Result<Vec<DaemonSet>>;
    async fn get_jobs(&self) -> Result<Vec<Job>>;
    async fn get_ingresses(&self) -> Result<Vec<Ingress>>;
    async fn get_services(&self) -> Result<Vec<Service>>;
    async fn get_endpoints(&self) -> Result<Vec<Endpoints>>;
    async fn get_network_policies(&self) -> Result<Vec<NetworkPolicy>>;
    async fn get_config_maps(&self) -> Result<Vec<ConfigMap>>;
    async fn get_storage_classes(&self) -> Result<Vec<StorageClass>>;
    async fn get_persistent_volumes(&self) -> Result<Vec<PersistentVolume>>;
    async fn get_persistent_volume_claims(&self) -> Result<Vec<PersistentVolumeClaim>>;
    async fn get_nodes(&self) -> Result<Vec<Node>>;
    async fn get_service_accounts(&self) -> Result<Vec<ServiceAccount>>;
}

pub struct KubeClientImpl {
    pod_api: Api<Pod>,
    deployment_api: Api<Deployment>,
    stateful_set_api: Api<StatefulSet>,
    replica_set_api: Api<ReplicaSet>,
    daemon_set_api: Api<DaemonSet>,
    job_api: Api<Job>,
    ingress_api: Api<Ingress>,
    service_api: Api<Service>,
    endpoints_api: Api<Endpoints>,
    network_policy_api: Api<NetworkPolicy>,
    config_map_api: Api<ConfigMap>,
    storage_class_api: Api<StorageClass>,
    persistent_volume_api: Api<PersistentVolume>,
    persistent_volume_claim_api: Api<PersistentVolumeClaim>,
    node_api: Api<Node>,
    service_account_api: Api<ServiceAccount>,
}

impl KubeClientImpl {
    pub async fn new(options: &KubeConfigOptions, namespace: &str) -> Result<Self> {
        let cfg = Config::from_kubeconfig(options).await?;
        let client = Client::try_from(cfg)?;
        Ok(KubeClientImpl {
            pod_api: Api::namespaced(client.clone(), namespace),
            deployment_api: Api::namespaced(client.clone(), namespace),
            stateful_set_api: Api::namespaced(client.clone(), namespace),
            replica_set_api: Api::namespaced(client.clone(), namespace),
            daemon_set_api: Api::namespaced(client.clone(), namespace),
            job_api: Api::namespaced(client.clone(), namespace),
            ingress_api: Api::namespaced(client.clone(), namespace),
            service_api: Api::namespaced(client.clone(), namespace),
            endpoints_api: Api::namespaced(client.clone(), namespace),
            network_policy_api: Api::namespaced(client.clone(), namespace),
            config_map_api: Api::namespaced(client.clone(), namespace),
            storage_class_api: Api::all(client.clone()),
            persistent_volume_api: Api::all(client.clone()),
            persistent_volume_claim_api: Api::namespaced(client.clone(), namespace),
            node_api: Api::all(client.clone()),
            service_account_api: Api::namespaced(client.clone(), namespace),
        })
    }

    async fn get_object<T: Clone + DeserializeOwned + Debug>(api: &Api<T>) -> Result<Vec<T>> {
        let mut r: Vec<T> = Vec::new();
        let mut continue_token: Option<String> = None;
        loop {
            let lp = match continue_token {
                None => ListParams::default(),
                Some(t) => ListParams::default().continue_token(&t),
            };
            let pods = api.list(&lp).await?;
            continue_token = pods.metadata.continue_.clone();

            for p in pods {
                r.push(p)
            }
            if continue_token.is_none() {
                break;
            }
        }
        Ok(r)
    }
}

#[async_trait]
impl KubeClient for KubeClientImpl {
    async fn get_pods(&self) -> Result<Vec<Pod>> {
        Self::get_object(&self.pod_api).await
    }

    async fn get_deployments(&self) -> Result<Vec<Deployment>> {
        Self::get_object(&self.deployment_api).await
    }

    async fn get_stateful_sets(&self) -> Result<Vec<StatefulSet>> {
        Self::get_object(&self.stateful_set_api).await
    }

    async fn get_replica_sets(&self) -> Result<Vec<ReplicaSet>> {
        Self::get_object(&self.replica_set_api).await
    }

    async fn get_daemon_sets(&self) -> Result<Vec<DaemonSet>> {
        Self::get_object(&self.daemon_set_api).await
    }

    async fn get_jobs(&self) -> Result<Vec<Job>> {
        Self::get_object(&self.job_api).await
    }

    async fn get_ingresses(&self) -> Result<Vec<Ingress>> {
        Self::get_object(&self.ingress_api).await
    }

    async fn get_services(&self) -> Result<Vec<Service>> {
        Self::get_object(&self.service_api).await
    }

    async fn get_endpoints(&self) -> Result<Vec<Endpoints>> {
        Self::get_object(&self.endpoints_api).await
    }

    async fn get_network_policies(&self) -> Result<Vec<NetworkPolicy>> {
        Self::get_object(&self.network_policy_api).await
    }

    async fn get_config_maps(&self) -> Result<Vec<ConfigMap>> {
        Self::get_object(&self.config_map_api).await
    }

    async fn get_storage_classes(&self) -> Result<Vec<StorageClass>> {
        Self::get_object(&self.storage_class_api).await
    }

    async fn get_persistent_volumes(&self) -> Result<Vec<PersistentVolume>> {
        Self::get_object(&self.persistent_volume_api).await
    }

    async fn get_persistent_volume_claims(&self) -> Result<Vec<PersistentVolumeClaim>> {
        Self::get_object(&self.persistent_volume_claim_api).await
    }

    async fn get_nodes(&self) -> Result<Vec<Node>> {
        Self::get_object(&self.node_api).await
    }

    async fn get_service_accounts(&self) -> Result<Vec<ServiceAccount>> {
        Self::get_object(&self.service_account_api).await
    }
}
