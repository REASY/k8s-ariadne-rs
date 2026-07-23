use crate::errors::{AriadneError, ErrorKind};
use crate::prelude::*;
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
use schemars;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use strum::IntoEnumIterator;
use strum_macros::{Display, EnumIter};

pub static LOGICAL_RESOURCE_TYPES: &[ResourceType] = &[
    ResourceType::IngressServiceBackend,
    ResourceType::EndpointAddress,
    ResourceType::Endpoint,
    ResourceType::Host,
    ResourceType::Cluster,
    ResourceType::Container,
];

#[derive(
    Debug, Display, Serialize, Deserialize, PartialOrd, Ord, Eq, Hash, PartialEq, Clone, EnumIter,
)]
pub enum ResourceType {
    // Core Workloads
    Pod,
    Deployment,
    StatefulSet,
    ReplicaSet,
    DaemonSet,
    Job,

    // Networking & Discovery
    Ingress,
    Service,
    EndpointSlice,
    NetworkPolicy,

    // Configuration
    ConfigMap,

    // Storage
    Provisioner,
    StorageClass,
    PersistentVolumeClaim,
    PersistentVolume,

    // Cluster Infrastructure
    Node,
    Namespace,

    // Identity & Access Control
    ServiceAccount,

    // Event
    Event,

    // Ansible
    AWX,

    // Logical resource types
    IngressServiceBackend, //  Represents a backend in an Ingress spec
    EndpointAddress,       // Represents a single IP address in an Endpoints object
    Endpoint,              //
    Host,                  // Represents a hostname claimed by an Ingress
    Cluster,               // Represents a cluster in which K8s objects exist
    Container,             // Represents a container of a pod
}

impl ResourceType {
    pub fn try_new(kind: &str) -> Result<Self> {
        if let Some(resource_type) =
            ResourceType::iter().find(|candidate| candidate.to_string() == kind)
        {
            return Ok(resource_type);
        }
        Err(AriadneError::from(ErrorKind::InvalidResourceTypeError(
            kind.to_string(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::ObjectReference;
    use k8s_openapi::api::discovery::v1::{
        Endpoint as KubernetesEndpoint, EndpointConditions, EndpointHints, ForZone,
    };

    #[test]
    fn resource_type_try_new_accepts_all_variants() {
        for resource in ResourceType::iter() {
            let parsed = ResourceType::try_new(&resource.to_string());
            assert!(parsed.is_ok(), "missing ResourceType mapping: {resource}");
        }
    }

    #[test]
    fn endpoint_identity_is_stable_across_mutable_state_changes() {
        let original = KubernetesEndpoint {
            addresses: vec!["10.0.0.1".to_string()],
            conditions: Some(EndpointConditions {
                ready: Some(true),
                serving: Some(true),
                terminating: Some(false),
            }),
            target_ref: Some(ObjectReference {
                kind: Some("Pod".to_string()),
                name: Some("api".to_string()),
                namespace: Some("default".to_string()),
                resource_version: Some("1".to_string()),
                uid: Some("pod-api".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let changed = KubernetesEndpoint {
            conditions: Some(EndpointConditions {
                ready: Some(false),
                serving: Some(false),
                terminating: Some(true),
            }),
            hints: Some(EndpointHints {
                for_zones: Some(vec![ForZone {
                    name: "zone-b".to_string(),
                }]),
            }),
            hostname: Some("api-0".to_string()),
            node_name: Some("node-b".to_string()),
            target_ref: original.target_ref.clone().map(|mut target_ref| {
                target_ref.resource_version = Some("2".to_string());
                target_ref
            }),
            zone: Some("zone-b".to_string()),
            ..original.clone()
        };

        assert_ne!(original, changed);
        assert_eq!(original.identity_digest(), changed.identity_digest());
    }

    #[test]
    fn endpoint_identity_canonicalizes_and_structurally_encodes_addresses() {
        let ordered = KubernetesEndpoint {
            addresses: vec!["10.0.0.1".to_string(), "10.0.0.22".to_string()],
            ..Default::default()
        };
        let reversed = KubernetesEndpoint {
            addresses: vec!["10.0.0.22".to_string(), "10.0.0.1".to_string()],
            ..Default::default()
        };
        let ambiguous_without_lengths_a = KubernetesEndpoint {
            addresses: vec!["a".to_string(), "bc".to_string()],
            ..Default::default()
        };
        let ambiguous_without_lengths_b = KubernetesEndpoint {
            addresses: vec!["ab".to_string(), "c".to_string()],
            ..Default::default()
        };

        assert_eq!(ordered.identity_digest(), reversed.identity_digest());
        assert_ne!(
            ambiguous_without_lengths_a.identity_digest(),
            ambiguous_without_lengths_b.identity_digest()
        );
    }
}

#[derive(
    Debug, Display, Serialize, Deserialize, Clone, Eq, Ord, PartialEq, PartialOrd, Hash, EnumIter,
)]
pub enum Edge {
    PartOf,    // e.g. Node -> Cluster
    BelongsTo, // e.g. Pod -> Namespace

    // Workload Management
    Manages, // e.g., Deployment -> ReplicaSet -> Pod

    // Pod & Node
    RunsOn, // e.g., Pod -> Node
    Runs,   // e.g., Pod -> Container

    // Networking & Routing
    DefinesBackend, // e.g., Ingress -> IngressBackend
    TargetsService, // e.g., IngressBackend -> Service
    IsClaimedBy,    // e.g., Host -> Ingress
    ListedIn,       // e.g., EndpointAddress -> EndpointSlice
    IsAddressOf,    // e.g., EndpointAddress -> Pod

    // Configuration
    MountsConfig,  // e.g., Pod -> ConfigMap (as volume)
    InjectsConfig, // e.g., Pod -> ConfigMap (as env)

    // Identity
    UsesIdentity, // e.g., Pod -> ServiceAccount

    // Storage
    ClaimsVolume,     // e.g., Pod → PersistentVolumeClaim
    BoundTo,          // e.g., PersistentVolumeClaim → PersistentVolume
    UsesProvisioner,  // e.g., StorageClass -> Provisioner
    UsesStorageClass, // e.g., PersistentVolume -> StorageClass

    // Policy
    AppliesTo, // e.g., NetworkPolicy -> Pod

    // Events
    Concerns, // e.g. Event -> Pod

    ContainsEndpoint, // EndpointSlice -> Endpoint
    HasAddress,       // Endpoint -> EndpointAddress
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum ResourceAttributes {
    Namespace {
        namespace: Arc<Namespace>,
    },
    Node {
        node: Arc<Node>,
    },
    Pod {
        pod: Arc<Pod>,
    },
    Deployment {
        deployment: Arc<Deployment>,
    },
    StatefulSet {
        stateful_set: Arc<StatefulSet>,
    },
    ReplicaSet {
        replica_set: Arc<ReplicaSet>,
    },
    DaemonSet {
        daemon_set: Arc<DaemonSet>,
    },
    Job {
        job: Arc<Job>,
    },
    Ingress {
        ingress: Arc<Ingress>,
    },
    Service {
        service: Arc<Service>,
    },
    Endpoint {
        endpoint: Arc<Endpoint>,
    },
    NetworkPolicy {
        network_policy: Arc<NetworkPolicy>,
    },
    ConfigMap {
        config_map: Arc<ConfigMap>,
    },
    Provisioner {
        provisioner: Box<Provisioner>,
    },
    StorageClass {
        storage_class: Arc<StorageClass>,
    },
    PersistentVolume {
        pv: Arc<PersistentVolume>,
    },
    PersistentVolumeClaim {
        pvc: Arc<PersistentVolumeClaim>,
    },
    ServiceAccount {
        service_account: Arc<ServiceAccount>,
    },
    IngressServiceBackend {
        ingress_service_backend: Arc<IngressServiceBackend>,
    },
    EndpointSlice {
        endpoint_slice: Arc<EndpointSlice>,
    },
    EndpointAddress {
        endpoint_address: Arc<EndpointAddress>,
    },
    Host {
        host: Arc<Host>,
    },
    Cluster {
        cluster: Box<Cluster>,
    },
    Container {
        container: Arc<Container>,
    },
    Event {
        event: Arc<Event>,
    },
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq, Hash, Ord, PartialOrd)]
pub struct ObjectIdentifier {
    pub uid: String,
    pub name: String,
    pub namespace: Option<String>,
    pub resource_version: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct GenericObject {
    pub id: ObjectIdentifier,
    pub resource_type: ResourceType,
    pub attributes: Option<Box<ResourceAttributes>>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, schemars::    JsonSchema)]
pub struct Cluster {
    pub metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    pub name: String,
    pub cluster_url: String,
    pub info: k8s_openapi::apimachinery::pkg::version::Info,
}
impl Cluster {
    pub fn new(
        id: ObjectIdentifier,
        server: &str,
        info: k8s_openapi::apimachinery::pkg::version::Info,
    ) -> Self {
        let md = k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            annotations: None,
            creation_timestamp: None,
            deletion_grace_period_seconds: None,
            deletion_timestamp: None,
            finalizers: None,
            generate_name: None,
            generation: None,
            labels: None,
            managed_fields: None,
            name: Some(id.name.to_string()),
            namespace: None,
            owner_references: None,
            resource_version: id.resource_version.clone(),
            self_link: None,
            uid: Some(id.uid.clone()),
        };
        Self {
            metadata: md,
            name: id.name.clone(),
            cluster_url: server.to_string(),
            info,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, schemars::JsonSchema)]
pub struct Provisioner {
    pub metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    pub name: String,
}

impl Provisioner {
    pub fn new(id: &ObjectIdentifier, name: &str) -> Self {
        let md = k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            annotations: None,
            creation_timestamp: None,
            deletion_grace_period_seconds: None,
            deletion_timestamp: None,
            finalizers: None,
            generate_name: None,
            generation: None,
            labels: None,
            managed_fields: None,
            name: Some(id.name.to_string()),
            namespace: id.namespace.clone(),
            owner_references: None,
            resource_version: id.resource_version.clone(),
            self_link: None,
            uid: Some(id.uid.clone()),
        };
        Self {
            metadata: md,
            name: name.to_string(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, schemars::JsonSchema)]
pub struct IngressServiceBackend {
    pub metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    pub name: String,
    pub port: Option<k8s_openapi::api::networking::v1::ServiceBackendPort>,

    #[serde(skip)]
    pub ingress_uid: String,
}

impl IngressServiceBackend {
    pub fn new(
        id: &ObjectIdentifier,
        backend: &k8s_openapi::api::networking::v1::IngressServiceBackend,
        ingress_uid: &str,
    ) -> Self {
        let md = k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            annotations: None,
            creation_timestamp: None,
            deletion_grace_period_seconds: None,
            deletion_timestamp: None,
            finalizers: None,
            generate_name: None,
            generation: None,
            labels: None,
            managed_fields: None,
            name: Some(id.name.to_string()),
            namespace: id.namespace.clone(),
            owner_references: None,
            resource_version: id.resource_version.clone(),
            self_link: None,
            uid: Some(id.uid.clone()),
        };
        Self {
            metadata: md,
            name: backend.name.clone(),
            port: backend.port.clone(),
            ingress_uid: ingress_uid.to_string(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, schemars::JsonSchema)]
pub struct EndpointAddress {
    pub metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    pub address: String,

    #[serde(skip)]
    pub endpoint_uid: String,

    #[serde(skip)]
    pub endpoint_slice_uid: String,

    #[serde(skip)]
    pub pod_uid: Option<String>,
}

impl EndpointAddress {
    pub fn new(
        id: &ObjectIdentifier,
        address: String,
        endpoint_uid: &str,
        endpoint_slice_uid: &str,
        pod_uid: Option<String>,
    ) -> Self {
        Self {
            metadata: as_object_meta(id),
            address,
            endpoint_uid: endpoint_uid.to_string(),
            endpoint_slice_uid: endpoint_slice_uid.to_string(),
            pod_uid,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, schemars::JsonSchema)]
pub struct Host {
    pub metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    pub name: String,

    #[serde(skip)]
    pub ingress_uid: String,
}
impl Host {
    pub fn new(id: &ObjectIdentifier, host: &str, ingress_uid: &str) -> Self {
        Self {
            metadata: as_object_meta(id),
            name: host.to_string(),
            ingress_uid: ingress_uid.to_string(),
        }
    }
}

#[derive(
    Debug,
    Serialize,
    Deserialize,
    PartialOrd,
    Ord,
    Eq,
    Hash,
    PartialEq,
    Clone,
    EnumIter,
    Display,
    schemars::JsonSchema,
)]
#[strum(serialize_all = "snake_case")]
pub enum ContainerType {
    Standard,
    Init,
    Ephemeral,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, schemars::JsonSchema)]
pub struct Container {
    pub pod_name: String,
    pub pod_uid: String,
    pub container_type: ContainerType,
    pub metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    pub spec: k8s_openapi::api::core::v1::Container,
}

impl Container {
    pub fn new(
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        spec: k8s_openapi::api::core::v1::Container,
        container_type: ContainerType,
    ) -> Self {
        let uid = format!("Container:{}:{}:{}", pod_uid, container_type, spec.name);
        Self {
            pod_name: pod_name.to_string(),
            pod_uid: pod_uid.to_string(),
            container_type,
            metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
                uid: Some(uid),
                name: Some(spec.name.clone()),
                namespace: Some(namespace.to_string()),
                ..Default::default()
            },
            spec,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, schemars::JsonSchema)]
pub struct Endpoint {
    pub metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    pub addresses: Vec<String>,
    pub conditions: Option<k8s_openapi::api::discovery::v1::EndpointConditions>,
    pub hints: Option<k8s_openapi::api::discovery::v1::EndpointHints>,
    pub hostname: Option<String>,
    pub node_name: Option<String>,
    pub target_ref: Option<k8s_openapi::api::core::v1::ObjectReference>,
    pub zone: Option<String>,

    #[serde(skip)]
    pub endpoint_slice_id: String,
}

impl Endpoint {
    pub fn new(
        id: &ObjectIdentifier,
        endpoint: k8s_openapi::api::discovery::v1::Endpoint,
        endpoint_slice_id: &str,
    ) -> Self {
        Self {
            metadata: as_object_meta(id),
            addresses: endpoint.addresses,
            conditions: endpoint.conditions,
            hints: endpoint.hints,
            hostname: endpoint.hostname,
            node_name: endpoint.node_name,
            target_ref: endpoint.target_ref,
            zone: endpoint.zone,
            endpoint_slice_id: endpoint_slice_id.to_string(),
        }
    }
}

fn as_object_meta(
    id: &ObjectIdentifier,
) -> k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
    k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
        annotations: None,
        creation_timestamp: None,
        deletion_grace_period_seconds: None,
        deletion_timestamp: None,
        finalizers: None,
        generate_name: None,
        generation: None,
        labels: None,
        managed_fields: None,
        name: Some(id.name.to_string()),
        namespace: id.namespace.clone(),
        owner_references: None,
        resource_version: id.resource_version.clone(),
        self_link: None,
        uid: Some(id.uid.clone()),
    }
}

pub(crate) trait EndpointIdentity {
    fn identity_digest(&self) -> String;
}

impl EndpointIdentity for k8s_openapi::api::discovery::v1::Endpoint {
    fn identity_digest(&self) -> String {
        endpoint_identity_digest(self)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }
}

#[doc(hidden)]
pub trait ObjectHasher {
    fn get_hash(&self) -> u64;
}

impl ObjectHasher for k8s_openapi::api::discovery::v1::Endpoint {
    fn get_hash(&self) -> u64 {
        let digest = endpoint_identity_digest(self);
        u64::from_be_bytes(
            digest[..size_of::<u64>()]
                .try_into()
                .expect("SHA-256 digest contains eight bytes"),
        )
    }
}

fn endpoint_identity_digest(endpoint: &k8s_openapi::api::discovery::v1::Endpoint) -> [u8; 32] {
    let mut addresses = endpoint.addresses.clone();
    addresses.sort();
    let mut hasher = Sha256::new();
    hasher.update(b"ariadne:endpoint-identity:v1");
    hasher.update((addresses.len() as u64).to_be_bytes());
    for address in addresses {
        let bytes = address.as_bytes();
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
    }
    hasher.finalize().into()
}
