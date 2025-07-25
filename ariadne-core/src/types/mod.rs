use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{
    ConfigMap, EndpointAddress, Endpoints, Node, PersistentVolume, PersistentVolumeClaim, Pod,
    Service, ServiceAccount,
};
use k8s_openapi::api::networking::v1::{Ingress, IngressServiceBackend, NetworkPolicy};
use k8s_openapi::api::storage::v1::StorageClass;
use serde::{Deserialize, Serialize};
use strum_macros::EnumIter;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, EnumIter)]
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

#[derive(Debug, Serialize, Deserialize, Clone, Eq, Ord, PartialEq, PartialOrd, EnumIter)]
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
    ClaimsVolume, // e.g., Pod → PersistentVolumeClaim
    BoundTo,      // e.g., PersistentVolumeClaim → PersistentVolume

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
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
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
