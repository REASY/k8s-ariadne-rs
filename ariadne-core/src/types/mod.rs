use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{
    ConfigMap, Endpoints, Node, PersistentVolume, PersistentVolumeClaim, Pod, Service,
    ServiceAccount,
};
use k8s_openapi::api::networking::v1::{Ingress, NetworkPolicy};
use k8s_openapi::api::storage::v1::StorageClass;
use serde::{Deserialize, Serialize};
use strum_macros::EnumIter;

#[derive(Debug, Serialize, Deserialize, Eq, Hash, PartialEq, Clone, EnumIter)]
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
    Endpoints,
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

    // Identity & Access Control
    ServiceAccount,

    // Logical resource types
    IngressServiceBackend, //  Represents a backend in an Ingress spec
    EndpointAddress,       // Represents a single IP address in an Endpoints object
    Host,                  // Represents a hostname claimed by an Ingress
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, Ord, PartialEq, PartialOrd, Hash, EnumIter)]
pub enum Edge {
    // Workload Management
    Manages, // e.g., Deployment -> ReplicaSet -> Pod

    // Pod & Node
    RunsOn, // e.g., Pod -> Node

    // Networking & Routing
    Selects,        // e.g., Service -> Pod
    DefinesBackend, // e.g., Ingress -> IngressBackend
    TargetsService, // e.g., IngressBackend -> Service
    IsClaimedBy,    // e.g., Host -> Ingress
    ListedIn,       // e.g., EndpointAddress -> Endpoints
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
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum ResourceAttributes {
    Node {
        node: Node,
    },
    Pod {
        pod: Pod,
    },
    Deployment {
        deployment: Deployment,
    },
    StatefulSet {
        stateful_set: StatefulSet,
    },
    ReplicaSet {
        replica_set: ReplicaSet,
    },
    DaemonSet {
        daemon_set: DaemonSet,
    },
    Job {
        job: Job,
    },
    Ingress {
        ingress: Ingress,
    },
    Service {
        service: Service,
    },
    Endpoints {
        endpoints: Endpoints,
    },
    NetworkPolicy {
        network_policy: NetworkPolicy,
    },
    ConfigMap {
        config_map: ConfigMap,
    },
    Provisioner {
        provisioner: Provisioner,
    },
    StorageClass {
        storage_class: StorageClass,
    },
    PersistentVolume {
        pv: PersistentVolume,
    },
    PersistentVolumeClaim {
        pvc: PersistentVolumeClaim,
    },
    ServiceAccount {
        service_account: ServiceAccount,
    },
    IngressServiceBackend {
        ingress_service_backend: IngressServiceBackend,
    },
    EndpointAddress {
        endpoint_address: EndpointAddress,
    },
    Host {
        host: Host,
    },
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq, Hash, Ord, PartialOrd)]
pub struct ObjectIdentifier {
    pub uid: String,
    pub name: String,
    pub namespace: Option<String>,
    pub resource_version: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct GenericObject {
    pub id: ObjectIdentifier,
    pub resource_type: ResourceType,
    pub attributes: Option<Box<ResourceAttributes>>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
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

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct IngressServiceBackend {
    pub metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    pub name: String,
    pub port: Option<k8s_openapi::api::networking::v1::ServiceBackendPort>,
}

impl IngressServiceBackend {
    pub fn new(
        id: &ObjectIdentifier,
        backend: &k8s_openapi::api::networking::v1::IngressServiceBackend,
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
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct EndpointAddress {
    pub metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    pub hostname: Option<String>,
    pub ip: String,
    pub node_name: Option<String>,
    pub target_ref: Option<k8s_openapi::api::core::v1::ObjectReference>,
}

impl EndpointAddress {
    pub fn new(
        id: &ObjectIdentifier,
        backend: &k8s_openapi::api::core::v1::EndpointAddress,
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
            hostname: backend.hostname.clone(),
            ip: backend.ip.clone(),
            node_name: backend.node_name.clone(),
            target_ref: backend.target_ref.clone(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Host {
    pub metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    pub host: String,
}
impl Host {
    pub fn new(id: &ObjectIdentifier, host: &str) -> Self {
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
            host: host.to_string(),
        }
    }
}
