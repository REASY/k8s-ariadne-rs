use crate::logger::setup;
use clap::Parser;
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{
    ConfigMap, Endpoints, Namespace, Node, PersistentVolume, Pod, Service, ServiceAccount,
};
use k8s_openapi::api::networking::v1::{Ingress, NetworkPolicy};
use k8s_openapi::api::storage::v1::StorageClass;
use k8s_openapi::schemars::schema::RootSchema;
use shadow_rs::shadow;
use tracing::info;
pub mod logger;
mod schema;

use crate::schema::{get_schema, write_schema_prompt, SchemaInfo};
use ariadne_core::types::{
    Cluster, EndpointAddress, Host, IngressServiceBackend, Logs, Provisioner,
};
use k8s_openapi::schemars::schema_for;

shadow!(build);

pub const APP_VERSION: &str = shadow_rs::formatcp!(
    "{} ({} {}), build_env: {}, {}, {}",
    build::PKG_VERSION,
    build::SHORT_COMMIT,
    build::BUILD_TIME,
    build::RUST_VERSION,
    build::RUST_CHANNEL,
    build::CARGO_VERSION
);

#[derive(Parser, Debug, Clone)]
#[clap(author, version = APP_VERSION, about, long_about = None)]
struct AppArgs {
    /// Connection string to GraphDB
    #[clap(long)]
    graph_url: String,
}

fn main() {
    setup("ariadne_tools", "debug");
    let args = AppArgs::parse();
    info!("Received args: {:?}", args);

    let logical_types: Vec<RootSchema> = vec![
        schema_for!(Provisioner),
        schema_for!(IngressServiceBackend),
        schema_for!(EndpointAddress),
        schema_for!(Host),
        schema_for!(Cluster),
        schema_for!(Logs),
    ];
    let k8s_types: Vec<RootSchema> = vec![
        schema_for!(Pod),
        schema_for!(Deployment),
        schema_for!(StatefulSet),
        schema_for!(ReplicaSet),
        schema_for!(DaemonSet),
        schema_for!(Job),
        schema_for!(Ingress),
        schema_for!(Service),
        schema_for!(Endpoints),
        schema_for!(NetworkPolicy),
        schema_for!(ConfigMap),
        schema_for!(StorageClass),
        schema_for!(PersistentVolume),
        schema_for!(Node),
        schema_for!(Namespace),
        schema_for!(ServiceAccount),
    ];
    let mut all_types = logical_types;
    all_types.extend(k8s_types);
    let mut derived_schema: Vec<SchemaInfo> = Vec::new();
    for schema in all_types {
        derived_schema.push(get_schema(&schema));
    }
    derived_schema.sort_by_key(|x| x.root_type.name.clone());

    let prompt = write_schema_prompt(derived_schema);
    println!("{prompt}");
}
