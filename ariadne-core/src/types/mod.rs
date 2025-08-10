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
use serde::{Deserialize, Serialize};
use std::hash::{DefaultHasher, Hash, Hasher};
use strum_macros::{Display, EnumIter};

pub static LOGICAL_RESOURCE_TYPES: &[ResourceType] = &[
    ResourceType::IngressServiceBackend,
    ResourceType::EndpointAddress,
    ResourceType::Endpoint,
    ResourceType::Host,
    ResourceType::Cluster,
    ResourceType::Logs,
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

    // Logical resource types
    IngressServiceBackend, //  Represents a backend in an Ingress spec
    EndpointAddress,       // Represents a single IP address in an Endpoints object
    Endpoint,              //
    Host,                  // Represents a hostname claimed by an Ingress
    Cluster,               // Represents a cluster in which K8s objects exist
    Logs,                  // Represents logs of a pod
    Container,             // Represents a container of a pod
}

impl ResourceType {
    pub fn try_new(kind: &str) -> Result<Self> {
        match kind {
            "Pod" => Ok(ResourceType::Pod),
            "Deployment" => Ok(ResourceType::Deployment),
            "StatefulSet" => Ok(ResourceType::StatefulSet),
            "ReplicaSet" => Ok(ResourceType::ReplicaSet),
            "DaemonSet" => Ok(ResourceType::DaemonSet),
            "Job" => Ok(ResourceType::Job),
            "Ingress" => Ok(ResourceType::Ingress),
            "Service" => Ok(ResourceType::Service),
            "EndpointSlice" => Ok(ResourceType::EndpointSlice),
            "NetworkPolicy" => Ok(ResourceType::NetworkPolicy),
            "ConfigMap" => Ok(ResourceType::ConfigMap),
            "StorageClass" => Ok(ResourceType::StorageClass),
            "PersistentVolumeClaim" => Ok(ResourceType::PersistentVolumeClaim),
            "PersistentVolume" => Ok(ResourceType::PersistentVolume),
            "Node" => Ok(ResourceType::Node),
            "Namespace" => Ok(ResourceType::Namespace),
            "ServiceAccount" => Ok(ResourceType::ServiceAccount),
            "Event" => Ok(ResourceType::Event),
            _ => Err(AriadneError::from(ErrorKind::InvalidResourceTypeError(
                kind.to_string(),
            ))),
        }
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
    RunsOn,  // e.g., Pod -> Node
    HasLogs, // e.g., Container -> Logs
    Runs,    // e.g., Pod -> Container

    // Networking & Routing
    Selects,        // e.g., Service -> Pod
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

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum ResourceAttributes {
    Namespace {
        namespace: Namespace,
    },
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
    Endpoint {
        endpoint: Endpoint,
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
    EndpointSlice {
        endpoint_slice: EndpointSlice,
    },
    EndpointAddress {
        endpoint_address: EndpointAddress,
    },
    Host {
        host: Host,
    },
    Cluster {
        cluster: Cluster,
    },
    Logs {
        logs: Logs,
    },
    Container {
        container: Container,
    },
    Event {
        event: Event,
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

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Cluster {
    pub metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    pub name: String,
    pub cluster_url: String,
    pub info: k8s_openapi::apimachinery::pkg::version::Info,
    pub retrieved_at: chrono::DateTime<chrono::Utc>,
}
impl Cluster {
    pub fn new(
        id: ObjectIdentifier,
        server: &str,
        info: k8s_openapi::apimachinery::pkg::version::Info,
        retrieved_at: chrono::DateTime<chrono::Utc>,
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
            retrieved_at,
        }
    }
}

impl k8s_openapi::schemars::JsonSchema for Cluster {
    fn schema_name() -> String {
        "Cluster".into()
    }

    fn json_schema(
        __gen: &mut k8s_openapi::schemars::gen::SchemaGenerator,
    ) -> k8s_openapi::schemars::schema::Schema {
        k8s_openapi::schemars::schema::Schema::Object(k8s_openapi::schemars::schema::SchemaObject {
            instance_type: Some(k8s_openapi::schemars::schema::SingleOrVec::Single(
                Box::new(k8s_openapi::schemars::schema::InstanceType::Object),
            )),
            object: Some(Box::new(k8s_openapi::schemars::schema::ObjectValidation {
                properties: [
                    (
                        "metadata".into(),
                        {
                            let mut schema_obj = __gen.subschema_for::<k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta>().into_object();
                            schema_obj.metadata = Some(Box::new(k8s_openapi::schemars::schema::Metadata {
                                description: Some("Standard object's metadata. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata".into()),
                                ..Default::default()
                            }));
                            k8s_openapi::schemars::schema::Schema::Object(schema_obj)
                        },
                    ),
                    (
                        "name".into(),
                        k8s_openapi::schemars::schema::Schema::Object(
                            k8s_openapi::schemars::schema::SchemaObject {
                                instance_type: Some(
                                    k8s_openapi::schemars::schema::SingleOrVec::Single(Box::new(
                                        k8s_openapi::schemars::schema::InstanceType::String,
                                    )),
                                ),
                                ..Default::default()
                            },
                        ),
                    ),
                    (
                        "cluster_url".into(),
                        k8s_openapi::schemars::schema::Schema::Object(
                            k8s_openapi::schemars::schema::SchemaObject {
                                instance_type: Some(
                                    k8s_openapi::schemars::schema::SingleOrVec::Single(Box::new(
                                        k8s_openapi::schemars::schema::InstanceType::String,
                                    )),
                                ),
                                ..Default::default()
                            },
                        ),
                    ),
                    (
                        "info".into(),
                        {
                            let schema_obj = __gen
                                .subschema_for::<k8s_openapi::apimachinery::pkg::version::Info>()
                                .into_object();
                            k8s_openapi::schemars::schema::Schema::Object(schema_obj)
                        }
                    ),
                    (
                        "retrieved_at".into(),
                        {
                            let schema_obj = __gen.subschema_for::<k8s_openapi::apimachinery::pkg::apis::meta::v1::Time>().into_object();
                            k8s_openapi::schemars::schema::Schema::Object(schema_obj)
                        }
                    ),
                ]
                .into(),
                ..Default::default()
            })),
            ..Default::default()
        })
    }
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

impl k8s_openapi::schemars::JsonSchema for Provisioner {
    fn schema_name() -> String {
        "Provisioner".into()
    }

    fn json_schema(
        __gen: &mut k8s_openapi::schemars::gen::SchemaGenerator,
    ) -> k8s_openapi::schemars::schema::Schema {
        k8s_openapi::schemars::schema::Schema::Object(k8s_openapi::schemars::schema::SchemaObject {
            instance_type: Some(k8s_openapi::schemars::schema::SingleOrVec::Single(Box::new(k8s_openapi::schemars::schema::InstanceType::Object))),
            object: Some(Box::new(k8s_openapi::schemars::schema::ObjectValidation {
                properties: [
                    (
                        "name".into(),
                        k8s_openapi::schemars::schema::Schema::Object(k8s_openapi::schemars::schema::SchemaObject {
                            instance_type: Some(k8s_openapi::schemars::schema::SingleOrVec::Single(std::boxed::Box::new(k8s_openapi::schemars::schema::InstanceType::String))),
                            ..Default::default()
                        }),
                    ),
                    (
                        "metadata".into(),
                        {
                            let mut schema_obj = __gen.subschema_for::<k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta>().into_object();
                            schema_obj.metadata = Some(Box::new(k8s_openapi::schemars::schema::Metadata {
                                description: Some("Standard object's metadata. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata".into()),
                                ..Default::default()
                            }));
                            k8s_openapi::schemars::schema::Schema::Object(schema_obj)
                        },
                    ),
                ].into(),
                required: [
                    "metadata".into(),
                    "name".into(),
                ].into(),
                ..Default::default()
            })),
            ..Default::default()
        })
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

impl k8s_openapi::schemars::JsonSchema for IngressServiceBackend {
    fn schema_name() -> String {
        "IngressServiceBackend".into()
    }

    fn json_schema(
        __gen: &mut k8s_openapi::schemars::gen::SchemaGenerator,
    ) -> k8s_openapi::schemars::schema::Schema {
        k8s_openapi::schemars::schema::Schema::Object(k8s_openapi::schemars::schema::SchemaObject {
            metadata: Some(Box::new(k8s_openapi::schemars::schema::Metadata {
                description: Some("IngressServiceBackend references a Kubernetes Service as a Backend.".into()),
                ..Default::default()
            })),
            instance_type: Some(k8s_openapi::schemars::schema::SingleOrVec::Single(Box::new(k8s_openapi::schemars::schema::InstanceType::Object))),
            object: Some(Box::new(k8s_openapi::schemars::schema::ObjectValidation {
                properties: [
                    (
                        "name".into(),
                        k8s_openapi::schemars::schema::Schema::Object(k8s_openapi::schemars::schema::SchemaObject {
                            metadata: Some(Box::new(k8s_openapi::schemars::schema::Metadata {
                                description: Some("name is the referenced service. The service must exist in the same namespace as the Ingress object.".into()),
                                ..Default::default()
                            })),
                            instance_type: Some(k8s_openapi::schemars::schema::SingleOrVec::Single(std::boxed::Box::new(k8s_openapi::schemars::schema::InstanceType::String))),
                            ..Default::default()
                        }),
                    ),
                    (
                        "port".into(),
                        {
                            let mut schema_obj = __gen.subschema_for::<k8s_openapi::api::networking::v1::ServiceBackendPort>().into_object();
                            schema_obj.metadata = Some(std::boxed::Box::new(k8s_openapi::schemars::schema::Metadata {
                                description: Some("port of the referenced service. A port name or port number is required for a IngressServiceBackend.".into()),
                                ..Default::default()
                            }));
                            k8s_openapi::schemars::schema::Schema::Object(schema_obj)
                        },
                    ),
                    (
                        "metadata".into(),
                        {
                            let mut schema_obj = __gen.subschema_for::<k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta>().into_object();
                            schema_obj.metadata = Some(Box::new(k8s_openapi::schemars::schema::Metadata {
                                description: Some("Standard object's metadata. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata".into()),
                                ..Default::default()
                            }));
                            k8s_openapi::schemars::schema::Schema::Object(schema_obj)
                        },
                    )
                ].into(),
                ..Default::default()
            })),
            ..Default::default()
        })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct EndpointAddress {
    pub metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    pub address: String,
}

impl EndpointAddress {
    pub fn new(id: &ObjectIdentifier, address: String) -> Self {
        Self {
            metadata: as_object_meta(id),
            address,
        }
    }
}

impl k8s_openapi::schemars::JsonSchema for EndpointAddress {
    fn schema_name() -> String {
        "EndpointAddress".into()
    }

    fn json_schema(
        __gen: &mut k8s_openapi::schemars::gen::SchemaGenerator,
    ) -> k8s_openapi::schemars::schema::Schema {
        k8s_openapi::schemars::schema::Schema::Object(k8s_openapi::schemars::schema::SchemaObject {
            instance_type: Some(k8s_openapi::schemars::schema::SingleOrVec::Single(Box::new(k8s_openapi::schemars::schema::InstanceType::Object))),
            object: Some(Box::new(k8s_openapi::schemars::schema::ObjectValidation {
                properties: [
                    (
                        "metadata".into(),
                        {
                            let mut schema_obj = __gen.subschema_for::<k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta>().into_object();
                            schema_obj.metadata = Some(Box::new(k8s_openapi::schemars::schema::Metadata {
                                description: Some("Standard object's metadata. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata".into()),
                                ..Default::default()
                            }));
                            k8s_openapi::schemars::schema::Schema::Object(schema_obj)
                        },
                    ),
                    (
                        "address".into(),
                        k8s_openapi::schemars::schema::Schema::Object(k8s_openapi::schemars::schema::SchemaObject {
                            instance_type: Some(k8s_openapi::schemars::schema::SingleOrVec::Single(Box::new(k8s_openapi::schemars::schema::InstanceType::String))),
                            ..Default::default()
                        }),
                    )

                ].into(),
                ..Default::default()
            })),
            ..Default::default()
        })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Host {
    pub metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    pub name: String,
}
impl Host {
    pub fn new(id: &ObjectIdentifier, host: &str) -> Self {
        Self {
            metadata: as_object_meta(id),
            name: host.to_string(),
        }
    }
}

impl k8s_openapi::schemars::JsonSchema for Host {
    fn schema_name() -> String {
        "Host".into()
    }

    fn json_schema(
        __gen: &mut k8s_openapi::schemars::gen::SchemaGenerator,
    ) -> k8s_openapi::schemars::schema::Schema {
        k8s_openapi::schemars::schema::Schema::Object(k8s_openapi::schemars::schema::SchemaObject {
            instance_type: Some(k8s_openapi::schemars::schema::SingleOrVec::Single(Box::new(k8s_openapi::schemars::schema::InstanceType::Object))),
            object: Some(Box::new(k8s_openapi::schemars::schema::ObjectValidation {
                properties: [
                    (
                        "name".into(),
                        k8s_openapi::schemars::schema::Schema::Object(k8s_openapi::schemars::schema::SchemaObject {
                            instance_type: Some(k8s_openapi::schemars::schema::SingleOrVec::Single(Box::new(k8s_openapi::schemars::schema::InstanceType::String))),
                            ..Default::default()
                        }),
                    ),
                    (
                        "metadata".into(),
                        {
                            let mut schema_obj = __gen.subschema_for::<k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta>().into_object();
                            schema_obj.metadata = Some(Box::new(k8s_openapi::schemars::schema::Metadata {
                                description: Some("Standard object's metadata. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata".into()),
                                ..Default::default()
                            }));
                            k8s_openapi::schemars::schema::Schema::Object(schema_obj)
                        },
                    )
                ].into(),
                ..Default::default()
            })),
            ..Default::default()
        })
    }
}

#[derive(
    Debug, Serialize, Deserialize, PartialOrd, Ord, Eq, Hash, PartialEq, Clone, EnumIter, Display,
)]
#[strum(serialize_all = "snake_case")]
pub enum ContainerType {
    Standard,
    Init,
    Ephemeral,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
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
        let uid = format!("{}_{}_{}", pod_uid, container_type, &spec.name);
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

impl k8s_openapi::schemars::JsonSchema for Container {
    fn schema_name() -> String {
        "Container".into()
    }

    fn json_schema(
        __gen: &mut k8s_openapi::schemars::gen::SchemaGenerator,
    ) -> k8s_openapi::schemars::schema::Schema {
        k8s_openapi::schemars::schema::Schema::Object(k8s_openapi::schemars::schema::SchemaObject {
            instance_type: Some(k8s_openapi::schemars::schema::SingleOrVec::Single(Box::new(k8s_openapi::schemars::schema::InstanceType::Object))),
            object: Some(Box::new(k8s_openapi::schemars::schema::ObjectValidation {
                properties: [
                    (
                        "pod_name".into(),
                        k8s_openapi::schemars::schema::Schema::Object(k8s_openapi::schemars::schema::SchemaObject {
                            instance_type: Some(k8s_openapi::schemars::schema::SingleOrVec::Single(Box::new(k8s_openapi::schemars::schema::InstanceType::String))),
                            ..Default::default()
                        }),
                    ),
                    (
                        "pod_uid".into(),
                        k8s_openapi::schemars::schema::Schema::Object(k8s_openapi::schemars::schema::SchemaObject {
                            instance_type: Some(k8s_openapi::schemars::schema::SingleOrVec::Single(Box::new(k8s_openapi::schemars::schema::InstanceType::String))),
                            ..Default::default()
                        }),
                    ),
                    (
                        "container_type".into(),
                        k8s_openapi::schemars::schema::Schema::Object(k8s_openapi::schemars::schema::SchemaObject {
                            instance_type: Some(k8s_openapi::schemars::schema::SingleOrVec::Single(Box::new(k8s_openapi::schemars::schema::InstanceType::String))),
                            ..Default::default()
                        }),
                    ),
                    (
                        "metadata".into(),
                        {
                            let mut schema_obj = __gen.subschema_for::<k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta>().into_object();
                            schema_obj.metadata = Some(Box::new(k8s_openapi::schemars::schema::Metadata {
                                description: Some("Standard object's metadata. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata".into()),
                                ..Default::default()
                            }));
                            k8s_openapi::schemars::schema::Schema::Object(schema_obj)
                        },
                    ),
                    (
                        "spec".into(),
                        {
                            let schema_obj = __gen.subschema_for::<k8s_openapi::api::core::v1::Container>().into_object();
                            k8s_openapi::schemars::schema::Schema::Object(schema_obj)
                        }
                    ),
                ].into(),
                ..Default::default()
            })),
            ..Default::default()
        })
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Logs {
    pub metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    pub container_uid: String,
    pub content: String,
}

impl Logs {
    pub fn new(namespace: &str, name: &str, container_uid: &str, content: String) -> Self {
        let uid = format!("logs_{container_uid}");
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
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            owner_references: None,
            resource_version: None,
            self_link: None,
            uid: Some(uid),
        };
        Self {
            metadata: md,
            container_uid: container_uid.to_string(),
            content,
        }
    }
}

impl k8s_openapi::schemars::JsonSchema for Logs {
    fn schema_name() -> String {
        "Logs".into()
    }

    fn json_schema(
        __gen: &mut k8s_openapi::schemars::gen::SchemaGenerator,
    ) -> k8s_openapi::schemars::schema::Schema {
        k8s_openapi::schemars::schema::Schema::Object(k8s_openapi::schemars::schema::SchemaObject {
            instance_type: Some(k8s_openapi::schemars::schema::SingleOrVec::Single(
                Box::new(k8s_openapi::schemars::schema::InstanceType::Object),
            )),
            object: Some(Box::new(k8s_openapi::schemars::schema::ObjectValidation {
                properties: [
                    (
                        "content".into(),
                        k8s_openapi::schemars::schema::Schema::Object(
                            k8s_openapi::schemars::schema::SchemaObject {
                                instance_type: Some(
                                    k8s_openapi::schemars::schema::SingleOrVec::Single(Box::new(
                                        k8s_openapi::schemars::schema::InstanceType::String,
                                    )),
                                ),
                                ..Default::default()
                            },
                        ),
                    ),
                    (
                        "container_uid".into(),
                        k8s_openapi::schemars::schema::Schema::Object(
                            k8s_openapi::schemars::schema::SchemaObject {
                                instance_type: Some(
                                    k8s_openapi::schemars::schema::SingleOrVec::Single(Box::new(
                                        k8s_openapi::schemars::schema::InstanceType::String,
                                    )),
                                ),
                                ..Default::default()
                            },
                        ),
                    ),
                    (
                        "metadata".into(),
                        {
                            let mut schema_obj = __gen.subschema_for::<k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta>().into_object();
                            schema_obj.metadata = Some(Box::new(k8s_openapi::schemars::schema::Metadata {
                                description: Some("Standard object's metadata. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata".into()),
                                ..Default::default()
                            }));
                            k8s_openapi::schemars::schema::Schema::Object(schema_obj)
                        },
                    )]
                .into(),
                ..Default::default()
            })),
            ..Default::default()
        })
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Endpoint {
    pub metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    pub addresses: Vec<String>,
    pub conditions: Option<k8s_openapi::api::discovery::v1::EndpointConditions>,
    pub hints: Option<k8s_openapi::api::discovery::v1::EndpointHints>,
    pub hostname: Option<String>,
    pub node_name: Option<String>,
    pub target_ref: Option<k8s_openapi::api::core::v1::ObjectReference>,
    pub zone: Option<String>,
}

impl Endpoint {
    pub fn new(id: &ObjectIdentifier, endpoint: k8s_openapi::api::discovery::v1::Endpoint) -> Self {
        Self {
            metadata: as_object_meta(id),
            addresses: endpoint.addresses,
            conditions: endpoint.conditions,
            hints: endpoint.hints,
            hostname: endpoint.hostname,
            node_name: endpoint.node_name,
            target_ref: endpoint.target_ref,
            zone: endpoint.zone,
        }
    }
}

impl k8s_openapi::schemars::JsonSchema for Endpoint {
    fn schema_name() -> String {
        "Endpoint".into()
    }

    fn json_schema(
        __gen: &mut k8s_openapi::schemars::gen::SchemaGenerator,
    ) -> k8s_openapi::schemars::schema::Schema {
        k8s_openapi::schemars::schema::Schema::Object(k8s_openapi::schemars::schema::SchemaObject {
            metadata: Some(std::boxed::Box::new(k8s_openapi::schemars::schema::Metadata {
                description: Some("Endpoint represents a single logical \"backend\" implementing a service.".into()),
                ..Default::default()
            })),
            instance_type: Some(k8s_openapi::schemars::schema::SingleOrVec::Single(std::boxed::Box::new(k8s_openapi::schemars::schema::InstanceType::Object))),
            object: Some(std::boxed::Box::new(k8s_openapi::schemars::schema::ObjectValidation {
                properties: [
                    (
                        "addresses".into(),
                        k8s_openapi::schemars::schema::Schema::Object(k8s_openapi::schemars::schema::SchemaObject {
                            metadata: Some(std::boxed::Box::new(k8s_openapi::schemars::schema::Metadata {
                                description: Some("addresses of this endpoint. The contents of this field are interpreted according to the corresponding EndpointSlice addressType field. Consumers must handle different types of addresses in the context of their own capabilities. This must contain at least one address but no more than 100. These are all assumed to be fungible and clients may choose to only use the first element. Refer to: https://issue.k8s.io/106267".into()),
                                ..Default::default()
                            })),
                            instance_type: Some(k8s_openapi::schemars::schema::SingleOrVec::Single(std::boxed::Box::new(k8s_openapi::schemars::schema::InstanceType::Array))),
                            array: Some(std::boxed::Box::new(k8s_openapi::schemars::schema::ArrayValidation {
                                items: Some(k8s_openapi::schemars::schema::SingleOrVec::Single(std::boxed::Box::new(
                                    k8s_openapi::schemars::schema::Schema::Object(k8s_openapi::schemars::schema::SchemaObject {
                                        instance_type: Some(k8s_openapi::schemars::schema::SingleOrVec::Single(std::boxed::Box::new(k8s_openapi::schemars::schema::InstanceType::String))),
                                        ..Default::default()
                                    })
                                ))),
                                ..Default::default()
                            })),
                            ..Default::default()
                        }),
                    ),
                    (
                        "conditions".into(),
                        {
                            let mut schema_obj = __gen.subschema_for::<k8s_openapi::api::discovery::v1::EndpointConditions>().into_object();
                            schema_obj.metadata = Some(std::boxed::Box::new(k8s_openapi::schemars::schema::Metadata {
                                description: Some("conditions contains information about the current status of the endpoint.".into()),
                                ..Default::default()
                            }));
                            k8s_openapi::schemars::schema::Schema::Object(schema_obj)
                        },
                    ),
                    (
                        "deprecatedTopology".into(),
                        k8s_openapi::schemars::schema::Schema::Object(k8s_openapi::schemars::schema::SchemaObject {
                            metadata: Some(std::boxed::Box::new(k8s_openapi::schemars::schema::Metadata {
                                description: Some("deprecatedTopology contains topology information part of the v1beta1 API. This field is deprecated, and will be removed when the v1beta1 API is removed (no sooner than kubernetes v1.24).  While this field can hold values, it is not writable through the v1 API, and any attempts to write to it will be silently ignored. Topology information can be found in the zone and nodeName fields instead.".into()),
                                ..Default::default()
                            })),
                            instance_type: Some(k8s_openapi::schemars::schema::SingleOrVec::Single(std::boxed::Box::new(k8s_openapi::schemars::schema::InstanceType::Object))),
                            object: Some(std::boxed::Box::new(k8s_openapi::schemars::schema::ObjectValidation {
                                additional_properties: Some(std::boxed::Box::new(
                                    k8s_openapi::schemars::schema::Schema::Object(k8s_openapi::schemars::schema::SchemaObject {
                                        instance_type: Some(k8s_openapi::schemars::schema::SingleOrVec::Single(std::boxed::Box::new(k8s_openapi::schemars::schema::InstanceType::String))),
                                        ..Default::default()
                                    })
                                )),
                                ..Default::default()
                            })),
                            ..Default::default()
                        }),
                    ),
                    (
                        "hints".into(),
                        {
                            let mut schema_obj = __gen.subschema_for::<k8s_openapi::api::discovery::v1::EndpointHints>().into_object();
                            schema_obj.metadata = Some(std::boxed::Box::new(k8s_openapi::schemars::schema::Metadata {
                                description: Some("hints contains information associated with how an endpoint should be consumed.".into()),
                                ..Default::default()
                            }));
                            k8s_openapi::schemars::schema::Schema::Object(schema_obj)
                        },
                    ),
                    (
                        "hostname".into(),
                        k8s_openapi::schemars::schema::Schema::Object(k8s_openapi::schemars::schema::SchemaObject {
                            metadata: Some(std::boxed::Box::new(k8s_openapi::schemars::schema::Metadata {
                                description: Some("hostname of this endpoint. This field may be used by consumers of endpoints to distinguish endpoints from each other (e.g. in DNS names). Multiple endpoints which use the same hostname should be considered fungible (e.g. multiple A values in DNS). Must be lowercase and pass DNS Label (RFC 1123) validation.".into()),
                                ..Default::default()
                            })),
                            instance_type: Some(k8s_openapi::schemars::schema::SingleOrVec::Single(std::boxed::Box::new(k8s_openapi::schemars::schema::InstanceType::String))),
                            ..Default::default()
                        }),
                    ),
                    (
                        "nodeName".into(),
                        k8s_openapi::schemars::schema::Schema::Object(k8s_openapi::schemars::schema::SchemaObject {
                            metadata: Some(std::boxed::Box::new(k8s_openapi::schemars::schema::Metadata {
                                description: Some("nodeName represents the name of the Node hosting this endpoint. This can be used to determine endpoints local to a Node.".into()),
                                ..Default::default()
                            })),
                            instance_type: Some(k8s_openapi::schemars::schema::SingleOrVec::Single(std::boxed::Box::new(k8s_openapi::schemars::schema::InstanceType::String))),
                            ..Default::default()
                        }),
                    ),
                    (
                        "targetRef".into(),
                        {
                            let mut schema_obj = __gen.subschema_for::<k8s_openapi::api::core::v1::ObjectReference>().into_object();
                            schema_obj.metadata = Some(std::boxed::Box::new(k8s_openapi::schemars::schema::Metadata {
                                description: Some("targetRef is a reference to a Kubernetes object that represents this endpoint.".into()),
                                ..Default::default()
                            }));
                            k8s_openapi::schemars::schema::Schema::Object(schema_obj)
                        },
                    ),
                    (
                        "zone".into(),
                        k8s_openapi::schemars::schema::Schema::Object(k8s_openapi::schemars::schema::SchemaObject {
                            metadata: Some(std::boxed::Box::new(k8s_openapi::schemars::schema::Metadata {
                                description: Some("zone is the name of the Zone this endpoint exists in.".into()),
                                ..Default::default()
                            })),
                            instance_type: Some(k8s_openapi::schemars::schema::SingleOrVec::Single(std::boxed::Box::new(k8s_openapi::schemars::schema::InstanceType::String))),
                            ..Default::default()
                        }),
                    ),
                    (
                        "metadata".into(),
                        {
                            let mut schema_obj = __gen.subschema_for::<k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta>().into_object();
                            schema_obj.metadata = Some(Box::new(k8s_openapi::schemars::schema::Metadata {
                                description: Some("Standard object's metadata. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata".into()),
                                ..Default::default()
                            }));
                            k8s_openapi::schemars::schema::Schema::Object(schema_obj)
                        },
                    )
                ].into(),
                required: [
                    "addresses".into(),
                ].into(),
                ..Default::default()
            })),
            ..Default::default()
        })
    }
}

fn as_object_meta(
    id: &ObjectIdentifier,
) -> k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
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
    md
}

pub trait ObjectHasher {
    fn get_hash(&self) -> u64;
}

impl ObjectHasher for k8s_openapi::api::discovery::v1::Endpoint {
    fn get_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();

        let mut addresses = self.addresses.clone();
        addresses.sort();
        addresses
            .iter()
            .for_each(|addr| hasher.write(addr.as_bytes()));
        self.conditions.as_ref().inspect(|c| {
            c.ready.inspect(|b| hasher.write_i8(*b as i8));
            c.serving.inspect(|b| hasher.write_i8(*b as i8));
            c.terminating.inspect(|b| hasher.write_i8(*b as i8));
        });

        self.hints.as_ref().inspect(|h| {
            h.for_zones.as_ref().inspect(|zones| {
                zones.iter().for_each(|zone| {
                    hasher.write(zone.name.as_bytes());
                })
            });
        });
        self.hostname
            .as_ref()
            .inspect(|h| hasher.write(h.as_bytes()));
        self.node_name
            .as_ref()
            .inspect(|n| hasher.write(n.as_bytes()));
        self.target_ref.as_ref().inspect(|t| {
            t.kind.as_ref().inspect(|k| hasher.write(k.as_bytes()));
            t.name.as_ref().inspect(|n| hasher.write(n.as_bytes()));
            t.namespace.as_ref().inspect(|n| hasher.write(n.as_bytes()));
            t.resource_version
                .as_ref()
                .inspect(|n| hasher.write(n.as_bytes()));
            t.uid.as_ref().inspect(|u| hasher.write(u.as_bytes()));
        });
        self.zone.as_ref().inspect(|z| hasher.write(z.as_bytes()));

        let hash = hasher.finish();
        hash
    }
}
