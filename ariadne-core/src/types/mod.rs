use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::core::v1::{
    Endpoints, Node, PersistentVolume, PersistentVolumeClaim, Pod, Service,
};
use k8s_openapi::api::networking::v1::Ingress;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum ResourceType {
    Node,
    Pod,
    Deployment,
    StatefulSet,
    ReplicaSet,
    DaemonSet,
    PersistentVolume,
    PersistentVolumeClaim,
    Ingress,
    Service,
    Endpoints,
    Host,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum ResourceAttributes {
    Node { node: Node },
    Pod { pod: Pod },
    Deployment { deployment: Deployment },
    StatefulSet { stateful_set: StatefulSet },
    ReplicaSet { replica_set: ReplicaSet },
    DaemonSet { daemon_set: DaemonSet },
    PersistentVolume { pv: PersistentVolume },
    PersistentVolumeClaim { pvc: PersistentVolumeClaim },
    Ingress { ingress: Ingress },
    Service { service: Service },
    Endpoints { endpoints: Endpoints },
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
