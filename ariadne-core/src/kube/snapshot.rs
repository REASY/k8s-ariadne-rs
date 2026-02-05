use crate::prelude::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub const SNAPSHOT_CLUSTER_FILE: &str = "cluster.json";
pub const SNAPSHOT_NAMESPACES_FILE: &str = "namespaces.json";
pub const SNAPSHOT_PODS_FILE: &str = "pods.json";
pub const SNAPSHOT_DEPLOYMENTS_FILE: &str = "deployments.json";
pub const SNAPSHOT_STATEFUL_SETS_FILE: &str = "statefulsets.json";
pub const SNAPSHOT_REPLICA_SETS_FILE: &str = "replicasets.json";
pub const SNAPSHOT_DAEMON_SETS_FILE: &str = "daemonsets.json";
pub const SNAPSHOT_JOBS_FILE: &str = "jobs.json";
pub const SNAPSHOT_INGRESSES_FILE: &str = "ingresses.json";
pub const SNAPSHOT_SERVICES_FILE: &str = "services.json";
pub const SNAPSHOT_ENDPOINT_SLICES_FILE: &str = "endpointslices.json";
pub const SNAPSHOT_NETWORK_POLICIES_FILE: &str = "networkpolicies.json";
pub const SNAPSHOT_CONFIG_MAPS_FILE: &str = "configmaps.json";
pub const SNAPSHOT_STORAGE_CLASSES_FILE: &str = "storageclasses.json";
pub const SNAPSHOT_PERSISTENT_VOLUMES_FILE: &str = "persistentvolumes.json";
pub const SNAPSHOT_PERSISTENT_VOLUME_CLAIMS_FILE: &str = "persistentvolumeclaims.json";
pub const SNAPSHOT_NODES_FILE: &str = "nodes.json";
pub const SNAPSHOT_SERVICE_ACCOUNTS_FILE: &str = "serviceaccounts.json";
pub const SNAPSHOT_EVENTS_FILE: &str = "events.json";

pub fn read_json_from_dir<T>(dir: &Path, filename: &str) -> Result<T>
where
    T: DeserializeOwned,
{
    let path = dir.join(filename);
    let bytes = fs::read(&path)?;
    Ok(serde_json::from_slice(&bytes)?)
}

pub fn read_list_from_dir<T>(dir: &Path, filename: &str) -> Result<Vec<Arc<T>>>
where
    T: DeserializeOwned,
{
    let items: Vec<T> = read_json_from_dir(dir, filename)?;
    Ok(items.into_iter().map(Arc::new).collect())
}

pub fn write_json_to_dir<T>(dir: &Path, filename: &str, value: &T) -> Result<PathBuf>
where
    T: Serialize,
{
    let path = dir.join(filename);
    let file = fs::File::create(&path)?;
    serde_json::to_writer_pretty(file, value)?;
    Ok(path)
}

pub fn write_list_to_dir<T>(dir: &Path, filename: &str, items: &[Arc<T>]) -> Result<PathBuf>
where
    T: Serialize,
{
    let view: Vec<&T> = items.iter().map(|item| item.as_ref()).collect();
    write_json_to_dir(dir, filename, &view)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kube_client::{KubeClient, SnapshotKubeClient};
    use crate::state_resolver::ClusterStateResolver;
    use crate::types::{Cluster, ObjectIdentifier};
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
    use k8s_openapi::apimachinery::pkg::version::Info;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(prefix: &str) -> Self {
            let mut path = std::env::temp_dir();
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            path.push(format!("{}_{}_{}", prefix, std::process::id(), nanos));
            fs::create_dir_all(&path).expect("Failed to create temp dir");
            TempDir { path }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn write_empty_lists(dir: &Path) -> Result<()> {
        write_list_to_dir::<Namespace>(dir, SNAPSHOT_NAMESPACES_FILE, &Vec::new())?;
        write_list_to_dir::<Pod>(dir, SNAPSHOT_PODS_FILE, &Vec::new())?;
        write_list_to_dir::<Deployment>(dir, SNAPSHOT_DEPLOYMENTS_FILE, &Vec::new())?;
        write_list_to_dir::<StatefulSet>(dir, SNAPSHOT_STATEFUL_SETS_FILE, &Vec::new())?;
        write_list_to_dir::<ReplicaSet>(dir, SNAPSHOT_REPLICA_SETS_FILE, &Vec::new())?;
        write_list_to_dir::<DaemonSet>(dir, SNAPSHOT_DAEMON_SETS_FILE, &Vec::new())?;
        write_list_to_dir::<Job>(dir, SNAPSHOT_JOBS_FILE, &Vec::new())?;
        write_list_to_dir::<Ingress>(dir, SNAPSHOT_INGRESSES_FILE, &Vec::new())?;
        write_list_to_dir::<Service>(dir, SNAPSHOT_SERVICES_FILE, &Vec::new())?;
        write_list_to_dir::<EndpointSlice>(dir, SNAPSHOT_ENDPOINT_SLICES_FILE, &Vec::new())?;
        write_list_to_dir::<NetworkPolicy>(dir, SNAPSHOT_NETWORK_POLICIES_FILE, &Vec::new())?;
        write_list_to_dir::<ConfigMap>(dir, SNAPSHOT_CONFIG_MAPS_FILE, &Vec::new())?;
        write_list_to_dir::<StorageClass>(dir, SNAPSHOT_STORAGE_CLASSES_FILE, &Vec::new())?;
        write_list_to_dir::<PersistentVolume>(dir, SNAPSHOT_PERSISTENT_VOLUMES_FILE, &Vec::new())?;
        write_list_to_dir::<PersistentVolumeClaim>(
            dir,
            SNAPSHOT_PERSISTENT_VOLUME_CLAIMS_FILE,
            &Vec::new(),
        )?;
        write_list_to_dir::<Node>(dir, SNAPSHOT_NODES_FILE, &Vec::new())?;
        write_list_to_dir::<ServiceAccount>(dir, SNAPSHOT_SERVICE_ACCOUNTS_FILE, &Vec::new())?;
        write_list_to_dir::<Event>(dir, SNAPSHOT_EVENTS_FILE, &Vec::new())?;
        Ok(())
    }

    fn test_cluster() -> Cluster {
        Cluster::new(
            ObjectIdentifier {
                uid: "Cluster:test".to_string(),
                name: "test".to_string(),
                namespace: None,
                resource_version: None,
            },
            "https://example.invalid",
            Info::default(),
        )
    }

    #[tokio::test]
    async fn snapshot_client_reads_expected_data() -> Result<()> {
        let temp = TempDir::new("ariadne_snapshot_client");
        let dir = temp.path.as_path();

        write_json_to_dir(dir, SNAPSHOT_CLUSTER_FILE, &test_cluster())?;
        write_empty_lists(dir)?;

        let namespace = Arc::new(Namespace {
            metadata: ObjectMeta {
                name: Some("default".to_string()),
                uid: Some("ns-uid".to_string()),
                ..Default::default()
            },
            ..Default::default()
        });
        write_list_to_dir(dir, SNAPSHOT_NAMESPACES_FILE, &vec![namespace])?;

        let client = SnapshotKubeClient::from_dir(dir)?;
        let namespaces = client.get_namespaces().await?;
        assert_eq!(namespaces.len(), 1);
        assert_eq!(namespaces[0].metadata.name.as_deref(), Some("default"));
        assert_eq!(client.get_cluster_url().await?, "https://example.invalid");

        Ok(())
    }

    #[tokio::test]
    async fn export_snapshot_dir_roundtrip() -> Result<()> {
        let seed = TempDir::new("ariadne_snapshot_seed");
        let seed_dir = seed.path.as_path();
        write_json_to_dir(seed_dir, SNAPSHOT_CLUSTER_FILE, &test_cluster())?;
        write_empty_lists(seed_dir)?;

        let namespace = Arc::new(Namespace {
            metadata: ObjectMeta {
                name: Some("kube-system".to_string()),
                uid: Some("ns-uid-2".to_string()),
                ..Default::default()
            },
            ..Default::default()
        });
        write_list_to_dir(seed_dir, SNAPSHOT_NAMESPACES_FILE, &vec![namespace])?;

        let client = SnapshotKubeClient::from_dir(seed_dir)?;
        let resolver =
            ClusterStateResolver::new_with_kube_client("test".to_string(), Box::new(client))
                .await?;

        let out = TempDir::new("ariadne_snapshot_out");
        resolver.export_observed_snapshot_dir(&out.path)?;

        let exported_namespaces: Vec<Namespace> =
            read_json_from_dir(&out.path, SNAPSHOT_NAMESPACES_FILE)?;
        assert_eq!(exported_namespaces.len(), 1);
        assert_eq!(
            exported_namespaces[0].metadata.name.as_deref(),
            Some("kube-system")
        );

        Ok(())
    }
}
