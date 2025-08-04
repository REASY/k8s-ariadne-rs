use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{
    ConfigMap, Endpoints, Namespace, Node, PersistentVolume, PersistentVolumeClaim, Pod, Service,
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
    Namespace,

    // Identity & Access Control
    ServiceAccount,

    // Logical resource types
    IngressServiceBackend, //  Represents a backend in an Ingress spec
    EndpointAddress,       // Represents a single IP address in an Endpoints object
    Host,                  // Represents a hostname claimed by an Ingress
    Cluster,               // Represents a cluster in which K8s objects exist
    Logs,                  // Represents logs of a pod
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, Ord, PartialEq, PartialOrd, Hash, EnumIter)]
pub enum Edge {
    PartOf,    // e.g. Node -> Cluster
    BelongsTo, // e.g. Pod -> Namespace

    // Workload Management
    Manages, // e.g., Deployment -> ReplicaSet -> Pod

    // Pod & Node
    RunsOn,  // e.g., Pod -> Node
    HasLogs, // e.g., Pod -> Logs

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
    Cluster {
        cluster: Cluster,
    },
    Logs {
        logs: Logs,
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
                    ("info".into(), {
                        let schema_obj = __gen
                            .subschema_for::<k8s_openapi::apimachinery::pkg::version::Info>()
                            .into_object();
                        k8s_openapi::schemars::schema::Schema::Object(schema_obj)
                    }),
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
                        "hostname".into(),
                        k8s_openapi::schemars::schema::Schema::Object(k8s_openapi::schemars::schema::SchemaObject {
                            metadata: Some(Box::new(k8s_openapi::schemars::schema::Metadata {
                                description: Some("The Hostname of this endpoint".into()),
                                ..Default::default()
                            })),
                            instance_type: Some(k8s_openapi::schemars::schema::SingleOrVec::Single(Box::new(k8s_openapi::schemars::schema::InstanceType::String))),
                            ..Default::default()
                        }),
                    ),
                    (
                        "ip".into(),
                        k8s_openapi::schemars::schema::Schema::Object(k8s_openapi::schemars::schema::SchemaObject {
                            metadata: Some(Box::new(k8s_openapi::schemars::schema::Metadata {
                                description: Some("The IP of this endpoint. May not be loopback (127.0.0.0/8 or ::1), link-local (169.254.0.0/16 or fe80::/10), or link-local multicast (224.0.0.0/24 or ff02::/16).".into()),
                                ..Default::default()
                            })),
                            instance_type: Some(k8s_openapi::schemars::schema::SingleOrVec::Single(Box::new(k8s_openapi::schemars::schema::InstanceType::String))),
                            ..Default::default()
                        }),
                    ),
                    (
                        "nodeName".into(),
                        k8s_openapi::schemars::schema::Schema::Object(k8s_openapi::schemars::schema::SchemaObject {
                            metadata: Some(Box::new(k8s_openapi::schemars::schema::Metadata {
                                description: Some("Optional: Node hosting this endpoint. This can be used to determine endpoints local to a node.".into()),
                                ..Default::default()
                            })),
                            instance_type: Some(k8s_openapi::schemars::schema::SingleOrVec::Single(Box::new(k8s_openapi::schemars::schema::InstanceType::String))),
                            ..Default::default()
                        }),
                    ),
                    (
                        "targetRef".into(),
                        {
                            let mut schema_obj = __gen.subschema_for::<k8s_openapi::api::core::v1::ObjectReference>().into_object();
                            schema_obj.metadata = Some(Box::new(k8s_openapi::schemars::schema::Metadata {
                                description: Some("Reference to object providing the endpoint.".into()),
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

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Logs {
    pub metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    pub pod_uid: String,
    pub content: String,
}

impl Logs {
    pub fn new(namespace: &str, name: &str, pod_uid: &str, content: String) -> Self {
        let uid = format!("logs_{}", pod_uid);
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
            pod_uid: pod_uid.to_string(),
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
                properties: [(
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
