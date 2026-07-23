use super::*;
use async_trait::async_trait;
use k8s_openapi::api::core::v1::{
    ConfigMapEnvSource, ConfigMapKeySelector, ConfigMapProjection, ConfigMapVolumeSource,
    Container as KubernetesContainer, EnvFromSource, EnvVar, EnvVarSource, EphemeralContainer,
    PersistentVolumeClaimVolumeSource, PodSpec, ProjectedVolumeSource, Volume, VolumeProjection,
};
use k8s_openapi::api::discovery::v1::{Endpoint as KubernetesEndpoint, EndpointConditions};
use k8s_openapi::api::networking::v1::{
    HTTPIngressPath, HTTPIngressRuleValue, IngressBackend, IngressRule,
    IngressServiceBackend as KubeIngressServiceBackend, IngressSpec, ServiceBackendPort,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use k8s_openapi::apimachinery::pkg::version::Info;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Clone, Default)]
struct SnapshotFrame {
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

#[derive(Debug, Clone, Copy, Default)]
struct FailureConfig {
    namespaces: bool,
    namespaces_after_first_round: bool,
    nodes_after_first_round: bool,
    storage_classes_after_first_round: bool,
    persistent_volumes_after_first_round: bool,
    persistent_volume_claims_after_first_round: bool,
    events_after_first_round: bool,
}

#[derive(Debug)]
struct MockKubeClient {
    snapshots: Vec<SnapshotFrame>,
    round_index: Mutex<usize>,
    failures: FailureConfig,
    degraded_resource_kinds: Arc<Mutex<BTreeSet<String>>>,
}

impl MockKubeClient {
    fn new(
        snapshots: Vec<SnapshotFrame>,
        failures: FailureConfig,
        degraded_resource_kinds: Arc<Mutex<BTreeSet<String>>>,
    ) -> Self {
        assert!(
            !snapshots.is_empty(),
            "MockKubeClient requires at least one snapshot"
        );
        Self {
            snapshots,
            round_index: Mutex::new(0),
            failures,
            degraded_resource_kinds,
        }
    }

    fn snapshot_for_round(&self, round: usize) -> SnapshotFrame {
        self.snapshots
            .get(round)
            .cloned()
            .unwrap_or_else(|| self.snapshots.last().expect("snapshot exists").clone())
    }

    fn begin_round(&self) -> SnapshotFrame {
        let mut round = self.round_index.lock().expect("round_index lock poisoned");
        let snapshot = self.snapshot_for_round(*round);
        *round += 1;
        snapshot
    }

    fn current_round_snapshot(&self) -> SnapshotFrame {
        let round = *self.round_index.lock().expect("round_index lock poisoned");
        let index = round.saturating_sub(1);
        self.snapshot_for_round(index)
    }
}

#[async_trait]
impl KubeClient for MockKubeClient {
    async fn get_namespaces(&self) -> Result<Vec<Arc<Namespace>>> {
        let snapshot = self.begin_round();
        let round = *self.round_index.lock().expect("round_index lock poisoned");
        if self.failures.namespaces || (self.failures.namespaces_after_first_round && round > 1) {
            return Err(std::io::Error::other("mock namespace failure").into());
        }
        Ok(snapshot.namespaces)
    }

    async fn get_pods(&self) -> Result<Vec<Arc<Pod>>> {
        Ok(self.current_round_snapshot().pods)
    }

    async fn get_deployments(&self) -> Result<Vec<Arc<Deployment>>> {
        Ok(self.current_round_snapshot().deployments)
    }

    async fn get_stateful_sets(&self) -> Result<Vec<Arc<StatefulSet>>> {
        Ok(self.current_round_snapshot().stateful_sets)
    }

    async fn get_replica_sets(&self) -> Result<Vec<Arc<ReplicaSet>>> {
        Ok(self.current_round_snapshot().replica_sets)
    }

    async fn get_daemon_sets(&self) -> Result<Vec<Arc<DaemonSet>>> {
        Ok(self.current_round_snapshot().daemon_sets)
    }

    async fn get_jobs(&self) -> Result<Vec<Arc<Job>>> {
        Ok(self.current_round_snapshot().jobs)
    }

    async fn get_ingresses(&self) -> Result<Vec<Arc<Ingress>>> {
        Ok(self.current_round_snapshot().ingresses)
    }

    async fn get_services(&self) -> Result<Vec<Arc<Service>>> {
        Ok(self.current_round_snapshot().services)
    }

    async fn get_endpoint_slices(&self) -> Result<Vec<Arc<EndpointSlice>>> {
        Ok(self.current_round_snapshot().endpoint_slices)
    }

    async fn get_network_policies(&self) -> Result<Vec<Arc<NetworkPolicy>>> {
        Ok(self.current_round_snapshot().network_policies)
    }

    async fn get_config_maps(&self) -> Result<Vec<Arc<ConfigMap>>> {
        Ok(self.current_round_snapshot().config_maps)
    }

    async fn get_storage_classes(&self) -> Result<Vec<Arc<StorageClass>>> {
        let round = *self.round_index.lock().expect("round_index lock poisoned");
        if self.failures.storage_classes_after_first_round && round > 1 {
            return Err(std::io::Error::other("mock storage class failure").into());
        }
        Ok(self.current_round_snapshot().storage_classes)
    }

    async fn get_persistent_volumes(&self) -> Result<Vec<Arc<PersistentVolume>>> {
        let round = *self.round_index.lock().expect("round_index lock poisoned");
        if self.failures.persistent_volumes_after_first_round && round > 1 {
            return Err(std::io::Error::other("mock persistent volume failure").into());
        }
        Ok(self.current_round_snapshot().persistent_volumes)
    }

    async fn get_persistent_volume_claims(&self) -> Result<Vec<Arc<PersistentVolumeClaim>>> {
        let round = *self.round_index.lock().expect("round_index lock poisoned");
        if self.failures.persistent_volume_claims_after_first_round && round > 1 {
            return Err(std::io::Error::other("mock persistent volume claim failure").into());
        }
        Ok(self.current_round_snapshot().persistent_volume_claims)
    }

    async fn get_nodes(&self) -> Result<Vec<Arc<Node>>> {
        let round = *self.round_index.lock().expect("round_index lock poisoned");
        if self.failures.nodes_after_first_round && round > 1 {
            return Err(std::io::Error::other("mock node failure").into());
        }
        Ok(self.current_round_snapshot().nodes)
    }

    async fn get_service_accounts(&self) -> Result<Vec<Arc<ServiceAccount>>> {
        Ok(self.current_round_snapshot().service_accounts)
    }

    async fn apiserver_version(&self) -> Result<Info> {
        Ok(Info {
            major: "1".to_string(),
            minor: "32".to_string(),
            ..Default::default()
        })
    }

    async fn get_cluster_url(&self) -> Result<String> {
        Ok("https://example.test".to_string())
    }

    async fn get_events(&self) -> Result<Vec<Arc<Event>>> {
        let round = *self.round_index.lock().expect("round_index lock poisoned");
        if self.failures.events_after_first_round && round > 1 {
            return Err(std::io::Error::other("mock event failure").into());
        }
        Ok(self.current_round_snapshot().events)
    }

    fn degraded_resource_kinds_handle(&self) -> Arc<Mutex<BTreeSet<String>>> {
        self.degraded_resource_kinds.clone()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CapturedDiffSummary {
    added_nodes: usize,
    removed_nodes: usize,
    modified_nodes: usize,
    added_edges: usize,
    removed_edges: usize,
}

impl From<&ClusterStateDiff> for CapturedDiffSummary {
    fn from(diff: &ClusterStateDiff) -> Self {
        Self {
            added_nodes: diff.added_nodes.len(),
            removed_nodes: diff.removed_nodes.len(),
            modified_nodes: diff.modified_nodes.len(),
            added_edges: diff.added_edges.len(),
            removed_edges: diff.removed_edges.len(),
        }
    }
}

#[derive(Debug, Default)]
struct MockGraphBackend {
    fail_create: bool,
    fail_update: bool,
    fail_updates_remaining: AtomicUsize,
    update_attempts: AtomicUsize,
    create_counts: Mutex<Vec<(usize, usize)>>,
    update_summaries: Mutex<Vec<CapturedDiffSummary>>,
}

impl MockGraphBackend {
    fn fail_update() -> Self {
        Self {
            fail_update: true,
            ..Default::default()
        }
    }

    fn fail_create() -> Self {
        Self {
            fail_create: true,
            ..Default::default()
        }
    }

    fn fail_update_once() -> Self {
        Self {
            fail_updates_remaining: AtomicUsize::new(1),
            ..Default::default()
        }
    }

    fn update_attempts(&self) -> usize {
        self.update_attempts.load(Ordering::SeqCst)
    }

    fn create_counts(&self) -> Vec<(usize, usize)> {
        self.create_counts
            .lock()
            .expect("create_counts lock poisoned")
            .clone()
    }

    fn update_summaries(&self) -> Vec<CapturedDiffSummary> {
        self.update_summaries
            .lock()
            .expect("update_summaries lock poisoned")
            .clone()
    }
}

#[async_trait]
impl GraphBackend for MockGraphBackend {
    async fn create(&self, cluster_state: Arc<Mutex<ClusterState>>) -> Result<()> {
        if self.fail_create {
            return Err(std::io::Error::other("mock create failure").into());
        }
        let state = cluster_state.lock().expect("cluster_state lock poisoned");
        self.create_counts
            .lock()
            .expect("create_counts lock poisoned")
            .push((state.get_node_count(), state.get_edge_count()));
        Ok(())
    }

    async fn update(&self, diff: ClusterStateDiff) -> Result<()> {
        self.update_attempts.fetch_add(1, Ordering::SeqCst);
        let injected_failure = self
            .fail_updates_remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok();
        if self.fail_update || injected_failure {
            return Err(std::io::Error::other("mock update failure").into());
        }
        self.update_summaries
            .lock()
            .expect("update_summaries lock poisoned")
            .push(CapturedDiffSummary::from(&diff));
        Ok(())
    }

    async fn execute_query(
        &self,
        _query: String,
        _params: Option<HashMap<String, Value>>,
    ) -> Result<Vec<Value>> {
        Ok(Vec::new())
    }

    async fn shutdown(&self) {}
}

fn owner_reference(kind: &str, name: &str, uid: &str) -> OwnerReference {
    OwnerReference {
        api_version: "apps/v1".to_string(),
        block_owner_deletion: None,
        controller: Some(true),
        kind: kind.to_string(),
        name: name.to_string(),
        uid: uid.to_string(),
    }
}

fn namespace(name: &str, uid: &str) -> Arc<Namespace> {
    Arc::new(Namespace {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            uid: Some(uid.to_string()),
            ..Default::default()
        },
        ..Default::default()
    })
}

fn pod(name: &str, namespace: &str, uid: &str, owners: Vec<OwnerReference>) -> Arc<Pod> {
    Arc::new(Pod {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            uid: Some(uid.to_string()),
            owner_references: (!owners.is_empty()).then_some(owners),
            ..Default::default()
        },
        ..Default::default()
    })
}

fn pod_with_pvc(name: &str, namespace: &str, uid: &str, claim_name: &str) -> Arc<Pod> {
    Arc::new(Pod {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            uid: Some(uid.to_string()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: Vec::new(),
            volumes: Some(vec![Volume {
                name: "data".to_string(),
                persistent_volume_claim: Some(PersistentVolumeClaimVolumeSource {
                    claim_name: claim_name.to_string(),
                    read_only: None,
                }),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    })
}

fn pod_with_service_account(
    name: &str,
    namespace: &str,
    uid: &str,
    service_account_name: Option<&str>,
    deprecated_service_account: Option<&str>,
) -> Arc<Pod> {
    Arc::new(Pod {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            uid: Some(uid.to_string()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: Vec::new(),
            service_account_name: service_account_name.map(str::to_string),
            service_account: deprecated_service_account.map(str::to_string),
            ..Default::default()
        }),
        ..Default::default()
    })
}

fn service_account(name: &str, namespace: &str, uid: &str) -> Arc<ServiceAccount> {
    Arc::new(ServiceAccount {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            uid: Some(uid.to_string()),
            ..Default::default()
        },
        ..Default::default()
    })
}

fn config_map(name: &str, namespace: &str, uid: &str) -> Arc<ConfigMap> {
    Arc::new(ConfigMap {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            uid: Some(uid.to_string()),
            ..Default::default()
        },
        ..Default::default()
    })
}

fn config_map_env_from(name: &str, optional: bool) -> EnvFromSource {
    EnvFromSource {
        config_map_ref: Some(ConfigMapEnvSource {
            name: name.to_string(),
            optional: Some(optional),
        }),
        ..Default::default()
    }
}

fn config_map_env_var(name: &str, optional: bool) -> EnvVar {
    EnvVar {
        name: format!("FROM_{}", name.replace('-', "_").to_uppercase()),
        value_from: Some(EnvVarSource {
            config_map_key_ref: Some(ConfigMapKeySelector {
                key: "value".to_string(),
                name: name.to_string(),
                optional: Some(optional),
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn persistent_volume_claim(name: &str, namespace: &str, uid: &str) -> Arc<PersistentVolumeClaim> {
    Arc::new(PersistentVolumeClaim {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            uid: Some(uid.to_string()),
            ..Default::default()
        },
        ..Default::default()
    })
}

fn service(name: &str, namespace: &str, uid: &str) -> Arc<Service> {
    Arc::new(Service {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            uid: Some(uid.to_string()),
            ..Default::default()
        },
        ..Default::default()
    })
}

fn endpoint_slice(
    uid: &str,
    resource_version: &str,
    addresses: &[&str],
    ready: bool,
) -> Arc<EndpointSlice> {
    Arc::new(EndpointSlice {
        metadata: ObjectMeta {
            name: Some("api-endpoints".to_string()),
            namespace: Some("default".to_string()),
            uid: Some(uid.to_string()),
            resource_version: Some(resource_version.to_string()),
            ..Default::default()
        },
        address_type: "IPv4".to_string(),
        endpoints: vec![KubernetesEndpoint {
            addresses: addresses
                .iter()
                .map(|address| (*address).to_string())
                .collect(),
            conditions: Some(EndpointConditions {
                ready: Some(ready),
                ..Default::default()
            }),
            ..Default::default()
        }],
        ..Default::default()
    })
}

fn endpoint_slice_for_service(
    uid: &str,
    namespace: &str,
    service_name: &str,
    owner_references: Vec<OwnerReference>,
) -> Arc<EndpointSlice> {
    let mut endpoint_slice = endpoint_slice(uid, "1", &["10.0.0.1"], true)
        .as_ref()
        .clone();
    endpoint_slice.metadata.namespace = Some(namespace.to_string());
    endpoint_slice.metadata.labels = Some(BTreeMap::from([(
        "kubernetes.io/service-name".to_string(),
        service_name.to_string(),
    )]));
    endpoint_slice.metadata.owner_references =
        (!owner_references.is_empty()).then_some(owner_references);
    Arc::new(endpoint_slice)
}

fn ingress_with_service(
    name: &str,
    namespace: &str,
    uid: &str,
    service_name: &str,
) -> Arc<Ingress> {
    ingress_with_service_ports(
        name,
        namespace,
        uid,
        service_name,
        vec![ServiceBackendPort {
            number: Some(80),
            ..Default::default()
        }],
    )
}

fn ingress_with_service_ports(
    name: &str,
    namespace: &str,
    uid: &str,
    service_name: &str,
    ports: Vec<ServiceBackendPort>,
) -> Arc<Ingress> {
    Arc::new(Ingress {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            uid: Some(uid.to_string()),
            ..Default::default()
        },
        spec: Some(IngressSpec {
            rules: Some(vec![IngressRule {
                http: Some(HTTPIngressRuleValue {
                    paths: ports
                        .into_iter()
                        .map(|port| HTTPIngressPath {
                            backend: IngressBackend {
                                service: Some(KubeIngressServiceBackend {
                                    name: service_name.to_string(),
                                    port: Some(port),
                                }),
                                ..Default::default()
                            },
                            path_type: "Prefix".to_string(),
                            ..Default::default()
                        })
                        .collect(),
                }),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    })
}

fn base_snapshot() -> SnapshotFrame {
    SnapshotFrame {
        namespaces: vec![namespace("team-a", "namespace-team-a")],
        replica_sets: vec![Arc::new(ReplicaSet {
            metadata: ObjectMeta {
                name: Some("web-rs".to_string()),
                namespace: Some("team-a".to_string()),
                uid: Some("replicaset-1".to_string()),
                ..Default::default()
            },
            ..Default::default()
        })],
        pods: vec![
            pod(
                "managed",
                "team-a",
                "pod-managed",
                vec![owner_reference("ReplicaSet", "web-rs", "replicaset-1")],
            ),
            pod(
                "unsupported-owner",
                "team-a",
                "pod-unsupported",
                vec![owner_reference("UnsupportedKind", "ghost", "ghost-owner")],
            ),
            pod("orphan", "team-missing", "pod-orphan", vec![]),
        ],
        ..Default::default()
    }
}

fn changed_snapshot() -> SnapshotFrame {
    let mut snapshot = base_snapshot();
    snapshot
        .namespaces
        .push(namespace("team-b", "namespace-team-b"));
    snapshot.pods.push(pod("new", "team-b", "pod-new", vec![]));
    snapshot
}

fn has_edge(state: &ClusterState, edge_type: Edge, source: &str, target: &str) -> bool {
    state
        .get_edges_by_type(&edge_type)
        .any(|edge| edge.source == source && edge.target == target)
}

#[tokio::test]
async fn endpoint_state_change_modifies_stable_derived_node() {
    let initial = SnapshotFrame {
        endpoint_slices: vec![endpoint_slice(
            "endpoint-slice-api",
            "1",
            &["10.0.0.1"],
            true,
        )],
        ..Default::default()
    };
    let changed = SnapshotFrame {
        endpoint_slices: vec![endpoint_slice(
            "endpoint-slice-api",
            "2",
            &["10.0.0.1"],
            false,
        )],
        ..Default::default()
    };
    let resolver = ClusterStateResolver::new_with_kube_client(
        "test-cluster".to_string(),
        Box::new(MockKubeClient::new(
            vec![initial, changed],
            FailureConfig::default(),
            Arc::new(Mutex::new(BTreeSet::new())),
        )),
    )
    .await
    .expect("resolver should initialize from first EndpointSlice snapshot");
    let initial_endpoint_uid = {
        let state_handle = resolver.resolve().await.expect("state should resolve");
        let state = state_handle.lock().expect("state lock poisoned");
        state
            .get_nodes_by_type(&ResourceType::Endpoint)
            .next()
            .expect("derived Endpoint should exist")
            .id
            .uid
            .clone()
    };
    let backend = Arc::new(MockGraphBackend::default());

    let outcome = resolver
        .sync_from_source(backend.clone())
        .await
        .expect("Endpoint readiness change should sync");

    assert_eq!(outcome.diff.added_nodes, 0);
    assert_eq!(outcome.diff.removed_nodes, 0);
    assert_eq!(
        outcome.diff.modified_nodes, 2,
        "the EndpointSlice and stable derived Endpoint should be modified"
    );
    assert_eq!(outcome.diff.added_edges, 0);
    assert_eq!(outcome.diff.removed_edges, 0);
    let state_handle = resolver.resolve().await.expect("state should resolve");
    let state = state_handle.lock().expect("state lock poisoned");
    let current_endpoint = state
        .get_nodes_by_type(&ResourceType::Endpoint)
        .next()
        .expect("derived Endpoint should remain");
    assert_eq!(current_endpoint.id.uid, initial_endpoint_uid);
    let Some(ResourceAttributes::Endpoint { endpoint }) = current_endpoint.attributes.as_deref()
    else {
        panic!("derived node should contain Endpoint attributes");
    };
    assert_eq!(
        endpoint
            .conditions
            .as_ref()
            .and_then(|conditions| conditions.ready),
        Some(false)
    );
}

#[tokio::test]
async fn endpoint_address_change_replaces_derived_endpoint_identity() {
    let initial = SnapshotFrame {
        endpoint_slices: vec![endpoint_slice(
            "endpoint-slice-api",
            "1",
            &["10.0.0.1"],
            true,
        )],
        ..Default::default()
    };
    let changed = SnapshotFrame {
        endpoint_slices: vec![endpoint_slice(
            "endpoint-slice-api",
            "2",
            &["10.0.0.2"],
            true,
        )],
        ..Default::default()
    };
    let resolver = ClusterStateResolver::new_with_kube_client(
        "test-cluster".to_string(),
        Box::new(MockKubeClient::new(
            vec![initial, changed],
            FailureConfig::default(),
            Arc::new(Mutex::new(BTreeSet::new())),
        )),
    )
    .await
    .expect("resolver should initialize from first EndpointSlice snapshot");
    let backend = Arc::new(MockGraphBackend::default());

    let outcome = resolver
        .sync_from_source(backend)
        .await
        .expect("Endpoint address change should sync");

    assert_eq!(outcome.diff.added_nodes, 2);
    assert_eq!(outcome.diff.removed_nodes, 2);
    assert_eq!(outcome.diff.modified_nodes, 1);
    assert_eq!(outcome.diff.added_edges, 3);
    assert_eq!(outcome.diff.removed_edges, 3);
}

#[tokio::test]
async fn ownerless_endpoint_slice_resolves_service_label_within_namespace() {
    let snapshot = SnapshotFrame {
        services: vec![
            service("api", "team-a", "service-team-a"),
            service("api", "team-b", "service-team-b"),
        ],
        endpoint_slices: vec![endpoint_slice_for_service(
            "endpoint-slice-api",
            "team-b",
            "api",
            Vec::new(),
        )],
        ..Default::default()
    };
    let resolver = ClusterStateResolver::new_with_kube_client(
        "test-cluster".to_string(),
        Box::new(MockKubeClient::new(
            vec![snapshot],
            FailureConfig::default(),
            Arc::new(Mutex::new(BTreeSet::new())),
        )),
    )
    .await
    .expect("ownerless custom EndpointSlice should resolve its labeled Service");
    let state_handle = resolver.resolve().await.expect("state should resolve");
    let state = state_handle.lock().expect("state lock poisoned");

    assert!(has_edge(
        &state,
        Edge::Manages,
        "service-team-b",
        "endpoint-slice-api"
    ));
    assert!(!has_edge(
        &state,
        Edge::Manages,
        "service-team-a",
        "endpoint-slice-api"
    ));
}

#[tokio::test]
async fn endpoint_slice_service_label_and_owner_reference_share_one_edge() {
    let mut service_owner = owner_reference("Service", "api", "service-api");
    service_owner.api_version = "v1".to_string();
    let snapshot = SnapshotFrame {
        services: vec![service("api", "default", "service-api")],
        endpoint_slices: vec![endpoint_slice_for_service(
            "endpoint-slice-api",
            "default",
            "api",
            vec![service_owner],
        )],
        ..Default::default()
    };
    let resolver = ClusterStateResolver::new_with_kube_client(
        "test-cluster".to_string(),
        Box::new(MockKubeClient::new(
            vec![snapshot],
            FailureConfig::default(),
            Arc::new(Mutex::new(BTreeSet::new())),
        )),
    )
    .await
    .expect("EndpointSlice owner and label should resolve");
    let state_handle = resolver.resolve().await.expect("state should resolve");
    let state = state_handle.lock().expect("state lock poisoned");

    assert_eq!(
        state
            .get_edges_by_type(&Edge::Manages)
            .filter(|edge| { edge.source == "service-api" && edge.target == "endpoint-slice-api" })
            .count(),
        1
    );
}

#[tokio::test]
async fn missing_labeled_service_does_not_abort_endpoint_slice_graph_build() {
    let snapshot = SnapshotFrame {
        endpoint_slices: vec![endpoint_slice_for_service(
            "endpoint-slice-orphan",
            "default",
            "missing-api",
            Vec::new(),
        )],
        ..Default::default()
    };
    let resolver = ClusterStateResolver::new_with_kube_client(
        "test-cluster".to_string(),
        Box::new(MockKubeClient::new(
            vec![snapshot],
            FailureConfig::default(),
            Arc::new(Mutex::new(BTreeSet::new())),
        )),
    )
    .await
    .expect("unresolved EndpointSlice Service must not abort graph construction");
    let state_handle = resolver.resolve().await.expect("state should resolve");
    let state = state_handle.lock().expect("state lock poisoned");

    assert!(
        state
            .node_by_uid("endpoint-slice-orphan")
            .is_some_and(|node| node.resource_type == ResourceType::EndpointSlice)
    );
    assert!(
        !state
            .get_edges_by_type(&Edge::Manages)
            .any(|edge| edge.target == "endpoint-slice-orphan")
    );
}

#[tokio::test]
async fn pod_service_accounts_resolve_canonical_default_and_deprecated_names_within_namespace() {
    let snapshot = SnapshotFrame {
        pods: vec![
            pod_with_service_account("explicit", "team-b", "pod-explicit", Some("api"), None),
            pod_with_service_account("defaulted", "team-a", "pod-defaulted", None, None),
            pod_with_service_account("legacy", "team-a", "pod-legacy", None, Some("legacy")),
            pod_with_service_account(
                "canonical-wins",
                "team-a",
                "pod-canonical-wins",
                Some("api"),
                Some("legacy"),
            ),
        ],
        service_accounts: vec![
            service_account("api", "team-a", "service-account-api-team-a"),
            service_account("api", "team-b", "service-account-api-team-b"),
            service_account("default", "team-a", "service-account-default-team-a"),
            service_account("legacy", "team-a", "service-account-legacy-team-a"),
        ],
        ..Default::default()
    };
    let resolver = ClusterStateResolver::new_with_kube_client(
        "test-cluster".to_string(),
        Box::new(MockKubeClient::new(
            vec![snapshot],
            FailureConfig::default(),
            Arc::new(Mutex::new(BTreeSet::new())),
        )),
    )
    .await
    .expect("Pod ServiceAccounts should resolve");
    let state_handle = resolver.resolve().await.expect("state should resolve");
    let state = state_handle.lock().expect("state lock poisoned");

    assert!(has_edge(
        &state,
        Edge::UsesIdentity,
        "pod-explicit",
        "service-account-api-team-b"
    ));
    assert!(!has_edge(
        &state,
        Edge::UsesIdentity,
        "pod-explicit",
        "service-account-api-team-a"
    ));
    assert!(has_edge(
        &state,
        Edge::UsesIdentity,
        "pod-defaulted",
        "service-account-default-team-a"
    ));
    assert!(has_edge(
        &state,
        Edge::UsesIdentity,
        "pod-legacy",
        "service-account-legacy-team-a"
    ));
    assert!(has_edge(
        &state,
        Edge::UsesIdentity,
        "pod-canonical-wins",
        "service-account-api-team-a"
    ));
    assert!(!has_edge(
        &state,
        Edge::UsesIdentity,
        "pod-canonical-wins",
        "service-account-legacy-team-a"
    ));
}

#[tokio::test]
async fn missing_pod_service_account_does_not_abort_graph_build() {
    let snapshot = SnapshotFrame {
        pods: vec![pod_with_service_account(
            "orphan",
            "default",
            "pod-orphan-identity",
            Some("missing"),
            None,
        )],
        ..Default::default()
    };
    let resolver = ClusterStateResolver::new_with_kube_client(
        "test-cluster".to_string(),
        Box::new(MockKubeClient::new(
            vec![snapshot],
            FailureConfig::default(),
            Arc::new(Mutex::new(BTreeSet::new())),
        )),
    )
    .await
    .expect("unresolved ServiceAccount must not abort graph construction");
    let state_handle = resolver.resolve().await.expect("state should resolve");
    let state = state_handle.lock().expect("state lock poisoned");

    assert!(
        state
            .node_by_uid("pod-orphan-identity")
            .is_some_and(|node| node.resource_type == ResourceType::Pod)
    );
    assert!(
        !state
            .get_edges_by_type(&Edge::UsesIdentity)
            .any(|edge| edge.source == "pod-orphan-identity")
    );
}

#[tokio::test]
async fn pod_config_map_edges_cover_volume_and_all_container_reference_forms() {
    let pod = Arc::new(Pod {
        metadata: ObjectMeta {
            name: Some("configured".to_string()),
            namespace: Some("team-a".to_string()),
            uid: Some("pod-configured".to_string()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![KubernetesContainer {
                name: "main".to_string(),
                env_from: Some(vec![
                    config_map_env_from("regular-env-from", false),
                    config_map_env_from("shared", false),
                ]),
                env: Some(vec![
                    config_map_env_var("regular-key", false),
                    config_map_env_var("shared", false),
                    config_map_env_var("shared", false),
                ]),
                ..Default::default()
            }],
            init_containers: Some(vec![KubernetesContainer {
                name: "init".to_string(),
                env_from: Some(vec![config_map_env_from("init-env-from", false)]),
                env: Some(vec![config_map_env_var("init-key", false)]),
                ..Default::default()
            }]),
            ephemeral_containers: Some(vec![EphemeralContainer {
                name: "debug".to_string(),
                env_from: Some(vec![config_map_env_from("ephemeral-env-from", false)]),
                env: Some(vec![config_map_env_var("ephemeral-key", false)]),
                ..Default::default()
            }]),
            volumes: Some(vec![
                Volume {
                    name: "direct".to_string(),
                    config_map: Some(ConfigMapVolumeSource {
                        name: "mount-direct".to_string(),
                        optional: Some(false),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                Volume {
                    name: "projected".to_string(),
                    projected: Some(ProjectedVolumeSource {
                        sources: Some(vec![VolumeProjection {
                            config_map: Some(ConfigMapProjection {
                                name: "mount-projected".to_string(),
                                optional: Some(false),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }]),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                Volume {
                    name: "shared".to_string(),
                    config_map: Some(ConfigMapVolumeSource {
                        name: "shared".to_string(),
                        optional: Some(false),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                Volume {
                    name: "shared-duplicate".to_string(),
                    config_map: Some(ConfigMapVolumeSource {
                        name: "shared".to_string(),
                        optional: Some(false),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        }),
        ..Default::default()
    });
    let config_map_names = [
        "mount-direct",
        "mount-projected",
        "shared",
        "regular-env-from",
        "regular-key",
        "init-env-from",
        "init-key",
        "ephemeral-env-from",
        "ephemeral-key",
    ];
    let mut config_maps: Vec<Arc<ConfigMap>> = config_map_names
        .into_iter()
        .map(|name| config_map(name, "team-a", &format!("config-map-{name}")))
        .collect();
    config_maps.push(config_map("shared", "team-b", "config-map-shared-team-b"));
    let snapshot = SnapshotFrame {
        pods: vec![pod],
        config_maps,
        ..Default::default()
    };
    let resolver = ClusterStateResolver::new_with_kube_client(
        "test-cluster".to_string(),
        Box::new(MockKubeClient::new(
            vec![snapshot],
            FailureConfig::default(),
            Arc::new(Mutex::new(BTreeSet::new())),
        )),
    )
    .await
    .expect("Pod ConfigMap references should resolve");
    let state_handle = resolver.resolve().await.expect("state should resolve");
    let state = state_handle.lock().expect("state lock poisoned");

    for name in ["mount-direct", "mount-projected", "shared"] {
        assert!(has_edge(
            &state,
            Edge::MountsConfig,
            "pod-configured",
            &format!("config-map-{name}")
        ));
    }
    for name in [
        "shared",
        "regular-env-from",
        "regular-key",
        "init-env-from",
        "init-key",
        "ephemeral-env-from",
        "ephemeral-key",
    ] {
        assert!(has_edge(
            &state,
            Edge::InjectsConfig,
            "pod-configured",
            &format!("config-map-{name}")
        ));
    }
    assert!(!has_edge(
        &state,
        Edge::MountsConfig,
        "pod-configured",
        "config-map-shared-team-b"
    ));
    assert!(!has_edge(
        &state,
        Edge::InjectsConfig,
        "pod-configured",
        "config-map-shared-team-b"
    ));
    assert_eq!(
        state
            .get_edges()
            .filter(|edge| {
                edge.source == "pod-configured"
                    && edge.target == "config-map-shared"
                    && matches!(edge.edge_type, Edge::MountsConfig | Edge::InjectsConfig)
            })
            .count(),
        2,
        "mounting and injecting the same ConfigMap must preserve both edge types"
    );
}

#[tokio::test]
async fn missing_optional_config_maps_do_not_abort_graph_build() {
    let pod = Arc::new(Pod {
        metadata: ObjectMeta {
            name: Some("optional-config".to_string()),
            namespace: Some("default".to_string()),
            uid: Some("pod-optional-config".to_string()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![KubernetesContainer {
                name: "main".to_string(),
                env_from: Some(vec![config_map_env_from("missing-env", true)]),
                env: Some(vec![config_map_env_var("missing-key", true)]),
                ..Default::default()
            }],
            volumes: Some(vec![Volume {
                name: "optional".to_string(),
                config_map: Some(ConfigMapVolumeSource {
                    name: "missing-volume".to_string(),
                    optional: Some(true),
                    ..Default::default()
                }),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    });
    let snapshot = SnapshotFrame {
        pods: vec![pod],
        ..Default::default()
    };
    let resolver = ClusterStateResolver::new_with_kube_client(
        "test-cluster".to_string(),
        Box::new(MockKubeClient::new(
            vec![snapshot],
            FailureConfig::default(),
            Arc::new(Mutex::new(BTreeSet::new())),
        )),
    )
    .await
    .expect("missing optional ConfigMaps must not abort graph construction");
    let state_handle = resolver.resolve().await.expect("state should resolve");
    let state = state_handle.lock().expect("state lock poisoned");

    assert!(state.node_by_uid("pod-optional-config").is_some());
    assert!(
        !state
            .get_edges_by_type(&Edge::MountsConfig)
            .any(|edge| edge.source == "pod-optional-config")
    );
    assert!(
        !state
            .get_edges_by_type(&Edge::InjectsConfig)
            .any(|edge| edge.source == "pod-optional-config")
    );
}

#[tokio::test]
async fn pod_pvc_edges_are_resolved_within_the_pod_namespace() {
    let snapshot = SnapshotFrame {
        namespaces: vec![
            namespace("team-a", "namespace-team-a"),
            namespace("team-b", "namespace-team-b"),
        ],
        pods: vec![
            pod_with_pvc("app", "team-a", "pod-team-a", "data"),
            pod_with_pvc("app", "team-b", "pod-team-b", "data"),
        ],
        persistent_volume_claims: vec![
            persistent_volume_claim("data", "team-a", "pvc-team-a"),
            persistent_volume_claim("data", "team-b", "pvc-team-b"),
        ],
        ..Default::default()
    };
    let resolver = ClusterStateResolver::new_with_kube_client(
        "test-cluster".to_string(),
        Box::new(MockKubeClient::new(
            vec![snapshot],
            FailureConfig::default(),
            Arc::new(Mutex::new(BTreeSet::new())),
        )),
    )
    .await
    .expect("resolver should build namespace-qualified PVC relationships");
    let state_handle = resolver.resolve().await.expect("state should resolve");
    let state = state_handle.lock().expect("state lock poisoned");

    assert!(has_edge(
        &state,
        Edge::ClaimsVolume,
        "pod-team-a",
        "pvc-team-a"
    ));
    assert!(has_edge(
        &state,
        Edge::ClaimsVolume,
        "pod-team-b",
        "pvc-team-b"
    ));
    assert!(!has_edge(
        &state,
        Edge::ClaimsVolume,
        "pod-team-a",
        "pvc-team-b"
    ));
    assert!(!has_edge(
        &state,
        Edge::ClaimsVolume,
        "pod-team-b",
        "pvc-team-a"
    ));
}

#[tokio::test]
async fn missing_pvc_reference_does_not_abort_graph_build() {
    let snapshot = SnapshotFrame {
        namespaces: vec![namespace("team-a", "namespace-team-a")],
        pods: vec![pod_with_pvc("app", "team-a", "pod-missing-pvc", "missing")],
        ..Default::default()
    };
    let resolver = ClusterStateResolver::new_with_kube_client(
        "test-cluster".to_string(),
        Box::new(MockKubeClient::new(
            vec![snapshot],
            FailureConfig::default(),
            Arc::new(Mutex::new(BTreeSet::new())),
        )),
    )
    .await
    .expect("an unresolved PVC must not abort graph construction");
    let state_handle = resolver.resolve().await.expect("state should resolve");
    let state = state_handle.lock().expect("state lock poisoned");

    assert!(state.node_by_uid("pod-missing-pvc").is_some());
    assert!(
        state
            .get_edges_by_type(&Edge::ClaimsVolume)
            .all(|edge| edge.source != "pod-missing-pvc")
    );
}

#[tokio::test]
async fn ingress_service_edges_are_resolved_within_the_ingress_namespace() {
    let snapshot = SnapshotFrame {
        namespaces: vec![
            namespace("team-a", "namespace-team-a"),
            namespace("team-b", "namespace-team-b"),
        ],
        ingresses: vec![
            ingress_with_service("web", "team-a", "ingress-team-a", "api"),
            ingress_with_service("web", "team-b", "ingress-team-b", "api"),
        ],
        services: vec![
            service("api", "team-a", "service-team-a"),
            service("api", "team-b", "service-team-b"),
        ],
        ..Default::default()
    };
    let resolver = ClusterStateResolver::new_with_kube_client(
        "test-cluster".to_string(),
        Box::new(MockKubeClient::new(
            vec![snapshot],
            FailureConfig::default(),
            Arc::new(Mutex::new(BTreeSet::new())),
        )),
    )
    .await
    .expect("resolver should build namespace-qualified Service relationships");
    let state_handle = resolver.resolve().await.expect("state should resolve");
    let state = state_handle.lock().expect("state lock poisoned");
    let backend_team_a = "IngressServiceBackend:ingress-team-a:api:number:80";
    let backend_team_b = "IngressServiceBackend:ingress-team-b:api:number:80";

    assert!(has_edge(
        &state,
        Edge::TargetsService,
        backend_team_a,
        "service-team-a"
    ));
    assert!(has_edge(
        &state,
        Edge::TargetsService,
        backend_team_b,
        "service-team-b"
    ));
    assert!(!has_edge(
        &state,
        Edge::TargetsService,
        backend_team_a,
        "service-team-b"
    ));
    assert!(!has_edge(
        &state,
        Edge::TargetsService,
        backend_team_b,
        "service-team-a"
    ));
}

#[tokio::test]
async fn missing_ingress_service_does_not_abort_graph_build() {
    let snapshot = SnapshotFrame {
        namespaces: vec![namespace("team-a", "namespace-team-a")],
        ingresses: vec![ingress_with_service(
            "web",
            "team-a",
            "ingress-missing-service",
            "missing",
        )],
        ..Default::default()
    };
    let resolver = ClusterStateResolver::new_with_kube_client(
        "test-cluster".to_string(),
        Box::new(MockKubeClient::new(
            vec![snapshot],
            FailureConfig::default(),
            Arc::new(Mutex::new(BTreeSet::new())),
        )),
    )
    .await
    .expect("an unresolved Service must not abort graph construction");
    let state_handle = resolver.resolve().await.expect("state should resolve");
    let state = state_handle.lock().expect("state lock poisoned");
    let backend_uid = "IngressServiceBackend:ingress-missing-service:missing:number:80";

    assert!(state.node_by_uid(backend_uid).is_some());
    assert!(
        state
            .get_edges_by_type(&Edge::TargetsService)
            .all(|edge| edge.source != backend_uid)
    );
}

#[tokio::test]
async fn ingress_backend_identity_includes_port_and_deduplicates_repeated_paths() {
    let snapshot = SnapshotFrame {
        namespaces: vec![namespace("team-a", "namespace-team-a")],
        ingresses: vec![ingress_with_service_ports(
            "web",
            "team-a",
            "ingress-multi-port",
            "api",
            vec![
                ServiceBackendPort {
                    number: Some(80),
                    ..Default::default()
                },
                ServiceBackendPort {
                    number: Some(8080),
                    ..Default::default()
                },
                ServiceBackendPort {
                    name: Some("http".to_string()),
                    ..Default::default()
                },
                ServiceBackendPort {
                    name: Some("http".to_string()),
                    ..Default::default()
                },
            ],
        )],
        services: vec![service("api", "team-a", "service-api")],
        ..Default::default()
    };
    let resolver = ClusterStateResolver::new_with_kube_client(
        "test-cluster".to_string(),
        Box::new(MockKubeClient::new(
            vec![snapshot],
            FailureConfig::default(),
            Arc::new(Mutex::new(BTreeSet::new())),
        )),
    )
    .await
    .expect("resolver should preserve distinct Ingress backend ports");
    let state_handle = resolver.resolve().await.expect("state should resolve");
    let state = state_handle.lock().expect("state lock poisoned");
    let backend_uids = [
        "IngressServiceBackend:ingress-multi-port:api:number:80",
        "IngressServiceBackend:ingress-multi-port:api:number:8080",
        "IngressServiceBackend:ingress-multi-port:api:name:http",
    ];

    for backend_uid in backend_uids {
        assert!(state.node_by_uid(backend_uid).is_some());
        assert!(has_edge(
            &state,
            Edge::TargetsService,
            backend_uid,
            "service-api"
        ));
    }
    assert_eq!(
        state
            .get_nodes_by_type(&ResourceType::IngressServiceBackend)
            .count(),
        3,
        "duplicate paths to the same Service and port must share one backend node"
    );
}

#[tokio::test]
async fn resolver_sync_and_rebuild_cover_live_sync_paths() {
    let initial = base_snapshot();
    let unchanged = initial.clone();
    let changed = changed_snapshot();
    let degraded = Arc::new(Mutex::new(BTreeSet::new()));

    let resolver = ClusterStateResolver::new_with_kube_client(
        "test-cluster".to_string(),
        Box::new(MockKubeClient::new(
            vec![initial, unchanged, changed.clone(), changed],
            FailureConfig::default(),
            degraded,
        )),
    )
    .await
    .expect("resolver should initialize from first snapshot");

    let state_handle = resolver.resolve().await.expect("state should resolve");
    {
        let state = state_handle.lock().expect("state lock poisoned");
        assert!(state.node_by_uid("replicaset-1").is_some());
        assert!(state.node_by_uid("pod-managed").is_some());
        assert!(state.node_by_uid("pod-unsupported").is_some());
        assert!(state.node_by_uid("pod-orphan").is_some());
        assert!(has_edge(
            &state,
            Edge::Manages,
            "replicaset-1",
            "pod-managed"
        ));
        assert!(!has_edge(
            &state,
            Edge::Manages,
            "ghost-owner",
            "pod-unsupported"
        ));
        assert!(has_edge(
            &state,
            Edge::BelongsTo,
            "pod-managed",
            "namespace-team-a"
        ));
        assert!(!has_edge(
            &state,
            Edge::BelongsTo,
            "pod-orphan",
            "namespace-team-a"
        ));
    }

    let backend = Arc::new(MockGraphBackend::default());

    let no_change = resolver
        .sync_from_source(backend.clone())
        .await
        .expect("no-change sync should succeed");
    assert_eq!(no_change.diff, StateDiffSummary::default());
    assert!(no_change.write_duration.is_none());
    assert!(backend.update_summaries().is_empty());

    let changed_sync = resolver
        .sync_from_source(backend.clone())
        .await
        .expect("changed sync should succeed");
    assert!(changed_sync.write_duration.is_some());
    assert!(changed_sync.diff.added_nodes > 0 || changed_sync.diff.modified_nodes > 0);
    let update_summaries = backend.update_summaries();
    assert_eq!(update_summaries.len(), 1);
    assert!(
        update_summaries[0].added_nodes > 0
            || update_summaries[0].modified_nodes > 0
            || update_summaries[0].removed_nodes > 0
    );

    {
        let state = state_handle.lock().expect("state lock poisoned");
        assert!(state.node_by_uid("namespace-team-b").is_some());
        assert!(state.node_by_uid("pod-new").is_some());
        assert!(has_edge(
            &state,
            Edge::BelongsTo,
            "pod-new",
            "namespace-team-b"
        ));
    }

    resolver
        .rebuild_from_source(backend.clone())
        .await
        .expect("rebuild should succeed");
    let create_counts = backend.create_counts();
    assert_eq!(create_counts.len(), 1);
    assert!(create_counts[0].0 > 0);
    assert!(create_counts[0].1 > 0);
}

#[tokio::test]
async fn diff_loop_recovers_after_transient_update_failure_and_stops_on_cancellation() {
    let initial = base_snapshot();
    let changed = changed_snapshot();
    let resolver = ClusterStateResolver::new_with_kube_client(
        "test-cluster".to_string(),
        Box::new(MockKubeClient::new(
            vec![initial, changed.clone(), changed],
            FailureConfig::default(),
            Arc::new(Mutex::new(BTreeSet::new())),
        )),
    )
    .await
    .expect("resolver should initialize from first snapshot");
    let backend = Arc::new(MockGraphBackend::fail_update_once());
    let token = CancellationToken::new();
    let handle = resolver.start_diff_loop_with_interval(
        backend.clone(),
        token.clone(),
        Duration::from_millis(5),
    );

    tokio::time::timeout(Duration::from_millis(500), async {
        while backend.update_summaries().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("diff loop should retry and recover after the injected failure");

    token.cancel();
    tokio::time::timeout(Duration::from_millis(100), handle)
        .await
        .expect("diff loop should respond to cancellation")
        .expect("diff loop task should not panic");

    assert!(backend.update_attempts() >= 2);
    assert_eq!(backend.update_summaries().len(), 1);
    let state_handle = resolver.resolve().await.expect("state should resolve");
    assert!(
        state_handle
            .lock()
            .expect("state lock poisoned")
            .node_by_uid("pod-new")
            .is_some(),
        "the recovered sync must publish the changed snapshot"
    );
}

#[tokio::test]
async fn degraded_resource_kinds_propagate_from_kube_client_handle() {
    let degraded = Arc::new(Mutex::new(BTreeSet::from([
        "Node".to_string(),
        "PersistentVolume".to_string(),
        "StorageClass".to_string(),
    ])));
    let resolver = ClusterStateResolver::new_with_kube_client(
        "test-cluster".to_string(),
        Box::new(MockKubeClient::new(
            vec![SnapshotFrame::default()],
            FailureConfig::default(),
            degraded.clone(),
        )),
    )
    .await
    .expect("resolver should initialize with degraded resources");

    let resolver_handle = resolver.degraded_resource_kinds_handle();
    assert!(Arc::ptr_eq(&resolver_handle, &degraded));
    let degraded_set = resolver_handle
        .lock()
        .expect("degraded_resource_kinds lock poisoned");
    assert!(degraded_set.contains("Node"));
    assert!(degraded_set.contains("StorageClass"));
    assert!(degraded_set.contains("PersistentVolume"));
}

#[test]
fn configured_source_sync_poll_interval_parsing() {
    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let _guard = ENV_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("env lock poisoned");
    let original = std::env::var(SOURCE_SYNC_POLL_INTERVAL_ENV).ok();

    unsafe {
        std::env::remove_var(SOURCE_SYNC_POLL_INTERVAL_ENV);
    }
    assert_eq!(
        configured_source_sync_poll_interval(),
        Duration::from_secs(DEFAULT_SOURCE_SYNC_POLL_INTERVAL_SECONDS)
    );

    unsafe {
        std::env::set_var(SOURCE_SYNC_POLL_INTERVAL_ENV, "0");
    }
    assert_eq!(
        configured_source_sync_poll_interval(),
        Duration::from_secs(DEFAULT_SOURCE_SYNC_POLL_INTERVAL_SECONDS)
    );

    unsafe {
        std::env::set_var(SOURCE_SYNC_POLL_INTERVAL_ENV, "invalid");
    }
    assert_eq!(
        configured_source_sync_poll_interval(),
        Duration::from_secs(DEFAULT_SOURCE_SYNC_POLL_INTERVAL_SECONDS)
    );

    unsafe {
        std::env::set_var(SOURCE_SYNC_POLL_INTERVAL_ENV, "7");
    }
    assert_eq!(
        configured_source_sync_poll_interval(),
        Duration::from_secs(7)
    );

    match original {
        Some(value) => unsafe {
            std::env::set_var(SOURCE_SYNC_POLL_INTERVAL_ENV, value);
        },
        None => unsafe {
            std::env::remove_var(SOURCE_SYNC_POLL_INTERVAL_ENV);
        },
    }
}

#[tokio::test]
async fn sync_from_source_reports_kube_fetch_failure_with_metadata() {
    let resolver = ClusterStateResolver::new_with_kube_client(
        "test-cluster".to_string(),
        Box::new(MockKubeClient::new(
            vec![base_snapshot()],
            FailureConfig {
                namespaces_after_first_round: true,
                ..Default::default()
            },
            Arc::new(Mutex::new(BTreeSet::new())),
        )),
    )
    .await
    .expect("resolver should initialize from first snapshot");

    let state_handle = resolver.resolve().await.expect("state should resolve");
    let err = resolver
        .sync_from_source(Arc::new(MockGraphBackend::default()))
        .await
        .expect_err("sync should fail on kube fetch");

    assert_eq!(err.stage, SourceSyncStage::KubeFetch);
    assert!(err.fetch_duration.is_some());
    assert!(err.diff_duration.is_none());
    assert!(err.write_duration.is_none());

    let state = state_handle.lock().expect("state lock poisoned");
    assert!(state.node_by_uid("pod-managed").is_some());
    assert!(state.node_by_uid("namespace-team-b").is_none());
}

#[tokio::test]
async fn cluster_resource_read_failures_preserve_last_known_good_state() {
    let mut initial = base_snapshot();
    initial.nodes.push(Arc::new(Node {
        metadata: ObjectMeta {
            name: Some("worker-1".to_string()),
            uid: Some("node-1".to_string()),
            ..Default::default()
        },
        ..Default::default()
    }));
    initial.storage_classes.push(Arc::new(StorageClass {
        metadata: ObjectMeta {
            name: Some("standard".to_string()),
            uid: Some("storage-class-1".to_string()),
            ..Default::default()
        },
        ..Default::default()
    }));
    initial.persistent_volumes.push(Arc::new(PersistentVolume {
        metadata: ObjectMeta {
            name: Some("pv-1".to_string()),
            uid: Some("persistent-volume-1".to_string()),
            ..Default::default()
        },
        ..Default::default()
    }));
    initial
        .persistent_volume_claims
        .push(Arc::new(PersistentVolumeClaim {
            metadata: ObjectMeta {
                name: Some("data".to_string()),
                namespace: Some("team-a".to_string()),
                uid: Some("persistent-volume-claim-1".to_string()),
                ..Default::default()
            },
            ..Default::default()
        }));

    let failure_cases = [
        FailureConfig {
            nodes_after_first_round: true,
            ..Default::default()
        },
        FailureConfig {
            storage_classes_after_first_round: true,
            ..Default::default()
        },
        FailureConfig {
            persistent_volumes_after_first_round: true,
            ..Default::default()
        },
        FailureConfig {
            persistent_volume_claims_after_first_round: true,
            ..Default::default()
        },
    ];

    for failures in failure_cases {
        let resolver = ClusterStateResolver::new_with_kube_client(
            "test-cluster".to_string(),
            Box::new(MockKubeClient::new(
                vec![initial.clone()],
                failures,
                Arc::new(Mutex::new(BTreeSet::new())),
            )),
        )
        .await
        .expect("resolver should initialize from the complete first snapshot");
        let state_handle = resolver.resolve().await.expect("state should resolve");
        let backend = Arc::new(MockGraphBackend::default());

        let err = resolver
            .sync_from_source(backend.clone())
            .await
            .expect_err("partial Kubernetes read failure must abort the snapshot refresh");

        assert_eq!(err.stage, SourceSyncStage::KubeFetch);
        assert!(
            backend.update_summaries().is_empty(),
            "an incomplete snapshot must not be written"
        );
        let state = state_handle.lock().expect("state lock poisoned");
        for uid in [
            "node-1",
            "storage-class-1",
            "persistent-volume-1",
            "persistent-volume-claim-1",
        ] {
            assert!(
                state.node_by_uid(uid).is_some(),
                "last-known-good resource {uid} must be preserved"
            );
        }
    }
}

#[tokio::test]
async fn event_read_failure_preserves_last_known_good_events() {
    let mut initial = base_snapshot();
    initial.events.push(Arc::new(Event {
        metadata: ObjectMeta {
            name: Some("pod-warning".to_string()),
            namespace: Some("team-a".to_string()),
            uid: Some("event-1".to_string()),
            ..Default::default()
        },
        ..Default::default()
    }));
    let resolver = ClusterStateResolver::new_with_kube_client(
        "test-cluster".to_string(),
        Box::new(MockKubeClient::new(
            vec![initial],
            FailureConfig {
                events_after_first_round: true,
                ..Default::default()
            },
            Arc::new(Mutex::new(BTreeSet::new())),
        )),
    )
    .await
    .expect("resolver should initialize with the first Event snapshot");
    let state_handle = resolver.resolve().await.expect("state should resolve");
    let backend = Arc::new(MockGraphBackend::default());

    let err = resolver
        .sync_from_source(backend.clone())
        .await
        .expect_err("Event read failure must abort the snapshot refresh");

    assert_eq!(err.stage, SourceSyncStage::KubeFetch);
    assert!(backend.update_summaries().is_empty());
    assert!(
        state_handle
            .lock()
            .expect("state lock poisoned")
            .node_by_uid("event-1")
            .is_some(),
        "last-known-good Event must remain in the graph"
    );
}

#[tokio::test]
async fn sync_from_source_reports_graph_write_failure_and_preserves_cached_state() {
    let changed = changed_snapshot();
    let resolver = ClusterStateResolver::new_with_kube_client(
        "test-cluster".to_string(),
        Box::new(MockKubeClient::new(
            vec![base_snapshot(), changed.clone(), changed],
            FailureConfig::default(),
            Arc::new(Mutex::new(BTreeSet::new())),
        )),
    )
    .await
    .expect("resolver should initialize from first snapshot");

    let state_handle = resolver.resolve().await.expect("state should resolve");
    let err = resolver
        .sync_from_source(Arc::new(MockGraphBackend::fail_update()))
        .await
        .expect_err("sync should fail on graph write");

    assert_eq!(err.stage, SourceSyncStage::GraphWrite);
    assert!(err.fetch_duration.is_some());
    assert!(err.diff_duration.is_some());
    assert!(err.write_duration.is_some());

    {
        let state = state_handle.lock().expect("state lock poisoned");
        assert!(state.node_by_uid("namespace-team-b").is_none());
        assert!(state.node_by_uid("pod-new").is_none());
    }

    let outcome = resolver
        .sync_from_source(Arc::new(MockGraphBackend::default()))
        .await
        .expect("follow-up sync should still observe pending diff");
    assert!(outcome.write_duration.is_some());
    assert!(outcome.diff.added_nodes > 0 || outcome.diff.modified_nodes > 0);

    let state = state_handle.lock().expect("state lock poisoned");
    assert!(state.node_by_uid("namespace-team-b").is_some());
    assert!(state.node_by_uid("pod-new").is_some());
}

#[tokio::test]
async fn rebuild_from_source_reports_state_read_failure_with_metadata() {
    let resolver = ClusterStateResolver::new_with_kube_client(
        "test-cluster".to_string(),
        Box::new(MockKubeClient::new(
            vec![base_snapshot()],
            FailureConfig {
                namespaces_after_first_round: true,
                ..Default::default()
            },
            Arc::new(Mutex::new(BTreeSet::new())),
        )),
    )
    .await
    .expect("resolver should initialize from first snapshot");

    let state_handle = resolver.resolve().await.expect("state should resolve");
    let err = resolver
        .rebuild_from_source(Arc::new(MockGraphBackend::default()))
        .await
        .expect_err("rebuild should fail while reading source state");

    assert_eq!(err.stage, RebuildStage::StateRead);
    assert!(err.fetch_duration.is_some());
    assert!(err.write_duration.is_none());

    let state = state_handle.lock().expect("state lock poisoned");
    assert!(state.node_by_uid("pod-managed").is_some());
    assert!(state.node_by_uid("namespace-team-b").is_none());
}

#[tokio::test]
async fn rebuild_from_source_reports_graph_write_failure_and_preserves_cached_state() {
    let changed = changed_snapshot();
    let resolver = ClusterStateResolver::new_with_kube_client(
        "test-cluster".to_string(),
        Box::new(MockKubeClient::new(
            vec![base_snapshot(), changed.clone(), changed],
            FailureConfig::default(),
            Arc::new(Mutex::new(BTreeSet::new())),
        )),
    )
    .await
    .expect("resolver should initialize from first snapshot");

    let state_handle = resolver.resolve().await.expect("state should resolve");
    let err = resolver
        .rebuild_from_source(Arc::new(MockGraphBackend::fail_create()))
        .await
        .expect_err("rebuild should fail while writing graph");

    assert_eq!(err.stage, RebuildStage::GraphWrite);
    assert!(err.fetch_duration.is_some());
    assert!(err.write_duration.is_some());

    {
        let state = state_handle.lock().expect("state lock poisoned");
        assert!(state.node_by_uid("namespace-team-b").is_none());
        assert!(state.node_by_uid("pod-new").is_none());
    }

    resolver
        .rebuild_from_source(Arc::new(MockGraphBackend::default()))
        .await
        .expect("follow-up rebuild should still refresh from changed snapshot");
    let state = state_handle.lock().expect("state lock poisoned");
    assert!(state.node_by_uid("namespace-team-b").is_some());
    assert!(state.node_by_uid("pod-new").is_some());
}
