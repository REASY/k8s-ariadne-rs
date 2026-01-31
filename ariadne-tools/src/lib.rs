use ariadne_core::types::{
    Cluster, Container, Endpoint, EndpointAddress, Host, IngressServiceBackend, Logs, Provisioner,
};
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{
    ConfigMap, Namespace, Node, PersistentVolume, Pod, Service, ServiceAccount,
};
use k8s_openapi::api::discovery::v1::EndpointSlice;
use k8s_openapi::api::events::v1::Event;
use k8s_openapi::api::networking::v1::{Ingress, NetworkPolicy};
use k8s_openapi::api::storage::v1::StorageClass;
use k8s_openapi::schemars::schema_for;
use schemars::Schema;

pub mod schema;

pub use ariadne_core::graph_schema::{graph_relationships, GraphRelationship};
pub use schema::SchemaInfo;

const PROMPT_TEMPLATE: &str = include_str!("../../prompt.txt");
const SCHEMA_PLACEHOLDER: &str = "{{SCHEMA}}";
const RELATIONSHIPS_PLACEHOLDER: &str = "{{RELATIONSHIPS}}";

pub fn schema_prompt() -> String {
    let derived_schema = generate_schema();
    schema::write_schema_prompt(derived_schema)
}

pub fn graph_relationships_prompt() -> String {
    let mut output = String::new();
    for relationship in graph_relationships() {
        output.push_str(&format!(
            "(:{})-[:{}]->(:{})\n",
            relationship.from, relationship.edge, relationship.to
        ));
    }
    output
}

pub fn full_prompt() -> String {
    let schema = schema_prompt();
    let relationships = graph_relationships_prompt();
    PROMPT_TEMPLATE
        .replace(SCHEMA_PLACEHOLDER, schema.trim_end())
        .replace(RELATIONSHIPS_PLACEHOLDER, relationships.trim_end())
}

fn generate_schema() -> Vec<SchemaInfo> {
    let logical_types: Vec<Schema> = vec![
        schema_for!(Cluster),
        schema_for!(Container),
        schema_for!(Endpoint),
        schema_for!(EndpointAddress),
        schema_for!(Host),
        schema_for!(IngressServiceBackend),
        schema_for!(Logs),
        schema_for!(Provisioner),
    ];
    let k8s_types: Vec<Schema> = vec![
        schema_for!(ConfigMap),
        schema_for!(DaemonSet),
        schema_for!(Deployment),
        schema_for!(EndpointSlice),
        schema_for!(Event),
        schema_for!(Ingress),
        schema_for!(Job),
        schema_for!(Namespace),
        schema_for!(NetworkPolicy),
        schema_for!(Node),
        schema_for!(PersistentVolume),
        schema_for!(Pod),
        schema_for!(ReplicaSet),
        schema_for!(Service),
        schema_for!(ServiceAccount),
        schema_for!(StatefulSet),
        schema_for!(StorageClass),
    ];
    let mut all_types = logical_types;
    all_types.extend(k8s_types);
    let mut derived_schema: Vec<SchemaInfo> = Vec::new();
    for schema in all_types {
        derived_schema.push(schema::get_schema(&schema));
    }
    derived_schema.sort_by_key(|x| x.root_type.name.clone());
    derived_schema
}
