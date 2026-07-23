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
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn explicit_kubeconfig_selection_disables_incluster_fallback() {
    for options in [
        KubeConfigOptions {
            context: Some("production".to_string()),
            ..Default::default()
        },
        KubeConfigOptions {
            cluster: Some("production".to_string()),
            ..Default::default()
        },
        KubeConfigOptions {
            user: Some("operator".to_string()),
            ..Default::default()
        },
    ] {
        assert!(explicit_kubeconfig_requested(&options, false));
    }
}

#[test]
fn kubeconfig_environment_disables_incluster_fallback() {
    assert!(explicit_kubeconfig_requested(
        &KubeConfigOptions::default(),
        true
    ));
}

#[test]
fn unspecified_kubeconfig_allows_incluster_fallback() {
    assert!(!explicit_kubeconfig_requested(
        &KubeConfigOptions::default(),
        false
    ));
}

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
    write_list_to_dir(dir, SNAPSHOT_REPLICA_SETS_FILE, &replica_sets).expect("write replica sets");
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
            (AccessDecision::Allowed, "Pod"),
            (AccessDecision::Denied, "Node"),
            (AccessDecision::Denied, "Service"),
            (AccessDecision::Indeterminate, "Deployment"),
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

#[test]
fn watch_health_marks_errors_and_clears_recovered_kinds() {
    let degraded = Arc::new(Mutex::new(BTreeSet::from(["Node".to_string()])));

    update_watch_health(&degraded, "Pod", false);
    assert_eq!(
        degraded
            .lock()
            .expect("degraded lock poisoned")
            .iter()
            .cloned()
            .collect::<Vec<_>>(),
        vec!["Node".to_string(), "Pod".to_string()]
    );

    update_watch_health(&degraded, "Pod", true);
    assert_eq!(
        degraded
            .lock()
            .expect("degraded lock poisoned")
            .iter()
            .cloned()
            .collect::<Vec<_>>(),
        vec!["Node".to_string()]
    );
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
async fn event_store_timeout_and_failure_are_errors_not_empty_state() {
    let (pending_store, _writer) = reflector::store::<Event>();
    let timeout_err =
        store_state_or_empty_with_timeout(&Some(pending_store), "Event", Duration::from_millis(1))
            .await
            .expect_err("an unready Event store must fail");
    assert!(
        timeout_err
            .to_string()
            .contains("Timed out waiting for Event store readiness")
    );

    let (failed_store, writer) = reflector::store::<Event>();
    drop(writer);
    let readiness_err =
        store_state_or_empty_with_timeout(&Some(failed_store), "Event", Duration::from_millis(20))
            .await
            .expect_err("a failed Event store must fail without panicking");
    assert!(
        readiness_err
            .to_string()
            .contains("Event store is not ready")
    );
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
