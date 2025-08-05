use crate::prelude::*;
use crate::state::ClusterState;
use crate::types::{Edge, GenericObject, ResourceAttributes, ResourceType};
use k8s_openapi::Metadata;
use rsmgclient::{ConnectParams, Connection, ConnectionStatus};
use serde_json::Value;
use std::collections::HashSet;
use std::time::Instant;
use thiserror::Error;
use tracing::{info, trace};

#[derive(Error, Debug)]
pub enum MemgraphError {
    #[error("ConnectionError: {0}")]
    ConnectionError(String),
    #[error("QueryError: {0}")]
    QueryError(String),
    #[error("CommitError: {0}")]
    CommitError(String),
}

pub struct Memgraph {
    connection: Connection,
}

impl Memgraph {
    pub fn try_new(params: ConnectParams) -> Result<Self> {
        let mut connection: Connection = Connection::connect(&params)
            .map_err(|e| MemgraphError::ConnectionError(e.to_string()))?;
        let status = connection.status();
        if status != ConnectionStatus::Ready {
            println!("Connection failed with status: {:?}", status);
            return Err(MemgraphError::ConnectionError(format!(
                "Connection status {:?}",
                status
            )))?;
        }
        // Clear the graph.
        connection
            .execute_without_results("MATCH (n) DETACH DELETE n;")
            .map_err(|e| MemgraphError::QueryError(e.to_string()))?;
        connection
            .commit()
            .map_err(|e| MemgraphError::CommitError(e.to_string()))?;

        Ok(Self { connection })
    }

    pub fn create(&mut self, cluster_state: &ClusterState) -> Result<()> {
        let s = Instant::now();
        // Create nodes
        let mut unique_types: HashSet<ResourceType> = HashSet::new();
        for node in cluster_state.get_nodes() {
            let create_query = Self::get_create_query(&node)?;
            trace!("{}", create_query);
            self.connection
                .execute_without_results(&create_query)
                .map_err(|e| MemgraphError::QueryError(e.to_string()))?;

            unique_types.insert(node.resource_type.clone());
        }

        // Create indices
        for resource_type in unique_types {
            for create_index_query in Self::get_create_indices_query(&resource_type) {
                trace!("{}", create_index_query);
                self.connection
                    .execute_without_results(&create_index_query)
                    .map_err(|e| MemgraphError::QueryError(e.to_string()))?;
            }
        }
        // Create edges
        let mut unique_edges: HashSet<(ResourceType, ResourceType, Edge)> = HashSet::new();
        for edge in cluster_state.get_edges() {
            let create_edge_query = format!("MATCH (u:{:?}), (v:{:?}) WHERE u.metadata.uid = '{}' AND v.metadata.uid = '{}' CREATE (u)-[:{:?}]->(v);",  edge.source_type, edge.target_type, edge.source, edge.target, edge.edge_type);
            trace!("{}", create_edge_query);
            unique_edges.insert((edge.source_type, edge.target_type, edge.edge_type));
            self.connection
                .execute_without_results(&create_edge_query)
                .map_err(|e| MemgraphError::QueryError(e.to_string()))?;
        }
        self.connection
            .commit()
            .map_err(|e| MemgraphError::CommitError(e.to_string()))?;
        info!(
            "Created a memgraph with {} nodes and {} edges in {}ms",
            cluster_state.get_node_count(),
            cluster_state.get_edge_count(),
            s.elapsed().as_millis()
        );
        info!("There are {} edges in this graph", unique_edges.len());
        for (source_type, target_type, edge_type) in &unique_edges {
            info!(
                "(:{:?})-[:{:?}]->(:{:?})",
                source_type, edge_type, target_type
            );
        }

        for (source_type, target_type, edge_type) in unique_edges {
            println!(
                "(:{:?})-[:{:?}]->(:{:?})",
                source_type, edge_type, target_type
            );
        }

        Result::Ok(())
    }

    fn get_create_query(obj: &GenericObject) -> Result<String> {
        let r = match obj.resource_type {
            ResourceType::Pod => {
                format!(
                    r#"CREATE (n:Pod {});"#,
                    Self::json_to_cypher(&Self::get_as_json(&obj)?)
                )
            }
            ResourceType::Deployment => {
                format!(
                    r#"CREATE (n:Deployment {})"#,
                    Self::json_to_cypher(&Self::get_as_json(&obj)?)
                )
            }
            ResourceType::StatefulSet => {
                format!(
                    r#"CREATE (n:StatefulSet {})"#,
                    Self::json_to_cypher(&Self::get_as_json(&obj)?)
                )
            }
            ResourceType::ReplicaSet => {
                format!(
                    r#"CREATE (n:ReplicaSet {})"#,
                    Self::json_to_cypher(&Self::get_as_json(&obj)?)
                )
            }
            ResourceType::DaemonSet => {
                format!(
                    r#"CREATE (n:DaemonSet {})"#,
                    Self::json_to_cypher(&Self::get_as_json(&obj)?)
                )
            }
            ResourceType::Job => {
                format!(
                    r#"CREATE (n:Job {})"#,
                    Self::json_to_cypher(&Self::get_as_json(&obj)?)
                )
            }
            ResourceType::Ingress => {
                format!(
                    r#"CREATE (n:Ingress {})"#,
                    Self::json_to_cypher(&Self::get_as_json(&obj)?)
                )
            }
            ResourceType::Service => {
                format!(
                    r#"CREATE (n:Service {})"#,
                    Self::json_to_cypher(&Self::get_as_json(&obj)?)
                )
            }
            ResourceType::Endpoints => {
                format!(
                    r#"CREATE (n:Endpoints {})"#,
                    Self::json_to_cypher(&Self::get_as_json(&obj)?)
                )
            }
            ResourceType::NetworkPolicy => {
                format!(
                    r#"CREATE (n:NetworkPolicy {})"#,
                    Self::json_to_cypher(&Self::get_as_json(&obj)?)
                )
            }
            ResourceType::ConfigMap => {
                format!(
                    r#"CREATE (n:ConfigMap {})"#,
                    Self::json_to_cypher(&Self::get_as_json(&obj)?)
                )
            }
            ResourceType::Provisioner => {
                format!(
                    r#"CREATE (n:Provisioner {})"#,
                    Self::json_to_cypher(&Self::get_as_json(&obj)?)
                )
            }
            ResourceType::StorageClass => {
                format!(
                    r#"CREATE (n:StorageClass {})"#,
                    Self::json_to_cypher(&Self::get_as_json(&obj)?)
                )
            }
            ResourceType::PersistentVolumeClaim => {
                format!(
                    r#"CREATE (n:PersistentVolumeClaim {})"#,
                    Self::json_to_cypher(&Self::get_as_json(&obj)?)
                )
            }
            ResourceType::PersistentVolume => {
                format!(
                    r#"CREATE (n:PersistentVolume {})"#,
                    Self::json_to_cypher(&Self::get_as_json(&obj)?)
                )
            }
            ResourceType::Node => {
                format!(
                    r#"CREATE (n:Node {})"#,
                    Self::json_to_cypher(&Self::get_as_json(&obj)?)
                )
            }

            ResourceType::Namespace => {
                format!(
                    r#"CREATE (n:Namespace {})"#,
                    Self::json_to_cypher(&Self::get_as_json(&obj)?)
                )
            }
            ResourceType::ServiceAccount => {
                format!(
                    r#"CREATE (n:ServiceAccount {})"#,
                    Self::json_to_cypher(&Self::get_as_json(&obj)?)
                )
            }
            ResourceType::IngressServiceBackend => {
                format!(
                    r#"CREATE (n:IngressServiceBackend {})"#,
                    Self::json_to_cypher(&Self::get_as_json(&obj)?)
                )
            }
            ResourceType::EndpointAddress => {
                format!(
                    r#"CREATE (n:EndpointAddress {})"#,
                    Self::json_to_cypher(&Self::get_as_json(&obj)?)
                )
            }
            ResourceType::Host => {
                format!(
                    r#"CREATE (n:Host {})"#,
                    Self::json_to_cypher(&Self::get_as_json(&obj)?)
                )
            }
            ResourceType::Cluster => {
                format!(
                    r#"CREATE (n:Cluster {})"#,
                    Self::json_to_cypher(&Self::get_as_json(&obj)?)
                )
            }

            ResourceType::Logs => {
                format!(
                    r#"CREATE (n:Logs {})"#,
                    Self::json_to_cypher(&Self::get_as_json(&obj)?)
                )
            }
        };
        Ok(r)
    }

    fn get_as_json(obj: &GenericObject) -> Result<Value> {
        let Some(attributes) = &obj.attributes else {
            return Ok(Value::Null);
        };
        let v = match attributes.as_ref() {
            ResourceAttributes::Node { node: value } => {
                let mut fixed = value.clone();
                Self::cleanup_metadata(&mut fixed);
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::Namespace { namespace: value } => {
                let mut fixed = value.clone();
                Self::cleanup_metadata(&mut fixed);
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::Pod { pod: value } => {
                let mut fixed = value.clone();
                Self::cleanup_metadata(&mut fixed);
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::Deployment { deployment: value } => {
                let mut fixed = value.clone();
                Self::cleanup_metadata(&mut fixed);
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::StatefulSet {
                stateful_set: value,
            } => {
                let mut fixed = value.clone();
                Self::cleanup_metadata(&mut fixed);
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::ReplicaSet { replica_set: value } => {
                let mut fixed = value.clone();
                Self::cleanup_metadata(&mut fixed);
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::DaemonSet { daemon_set: value } => {
                let mut fixed = value.clone();
                Self::cleanup_metadata(&mut fixed);
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::Job { job: value } => {
                let mut fixed = value.clone();
                Self::cleanup_metadata(&mut fixed);
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::Ingress { ingress: value } => {
                let mut fixed = value.clone();
                Self::cleanup_metadata(&mut fixed);
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::Service { service: value } => {
                let mut fixed = value.clone();
                Self::cleanup_metadata(&mut fixed);
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::Endpoints { endpoints: value } => {
                let mut fixed = value.clone();
                Self::cleanup_metadata(&mut fixed);
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::NetworkPolicy {
                network_policy: value,
            } => {
                let mut fixed = value.clone();
                Self::cleanup_metadata(&mut fixed);
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::ConfigMap { config_map } => {
                let mut fixed = config_map.clone();
                Self::cleanup_metadata(&mut fixed);
                fixed.data = None;
                fixed.binary_data = None;
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::Provisioner { provisioner } => serde_json::to_value(provisioner)?,
            ResourceAttributes::StorageClass {
                storage_class: value,
            } => {
                let mut fixed = value.clone();
                Self::cleanup_metadata(&mut fixed);
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::PersistentVolume { pv: value } => {
                let mut fixed = value.clone();
                Self::cleanup_metadata(&mut fixed);
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::PersistentVolumeClaim { pvc: value } => {
                let mut fixed = value.clone();
                Self::cleanup_metadata(&mut fixed);
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::ServiceAccount {
                service_account: value,
            } => {
                let mut fixed = value.clone();
                Self::cleanup_metadata(&mut fixed);
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::IngressServiceBackend {
                ingress_service_backend,
            } => serde_json::to_value(ingress_service_backend)?,
            ResourceAttributes::EndpointAddress { endpoint_address } => {
                serde_json::to_value(endpoint_address)?
            }
            ResourceAttributes::Host { host } => serde_json::to_value(host)?,
            ResourceAttributes::Cluster { cluster: context } => serde_json::to_value(context)?,
            ResourceAttributes::Logs { logs: context } => serde_json::to_value(context)?,
        };

        Ok(v)
    }

    fn cleanup_metadata<T>(fixed: &mut T)
    where
        T: Metadata<Ty = k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta>,
    {
        let md = fixed.metadata_mut();
        md.managed_fields = None;
        if let Some(map) = md.annotations.as_mut() {
            // The following annotations are quite complicated to escape properly, we just remove them for now ;)
            map.remove("kubectl.kubernetes.io/last-applied-configuration");
            map.remove("kapp.k14s.io/original");
        }
    }

    fn get_create_indices_query(rt: &ResourceType) -> Vec<String> {
        match rt {
            _ => vec![
                format!("CREATE INDEX ON :{rt:?}(metadata.name)"),
                format!("CREATE INDEX ON :{rt:?}(metadata.uid)"),
                format!("CREATE INDEX ON :{rt:?}(metadata.namespace)"),
            ],
        }
    }

    fn json_to_cypher(value: &Value) -> String {
        let mut cypher_data = String::new();
        fn to_cypher_data0(value: &Value, cypher_data: &mut String) {
            match value {
                Value::Null => {
                    cypher_data.push_str("NULL");
                }
                Value::Bool(v) => {
                    if *v {
                        cypher_data.push_str("true");
                    } else {
                        cypher_data.push_str("false");
                    }
                }
                Value::Number(n) => {
                    cypher_data.push_str(&n.to_string());
                }
                Value::String(s) => {
                    cypher_data.push_str("\"");
                    let escaped = s.replace("\\", "\\\\").replace("\"", "\\\"");
                    cypher_data.push_str(escaped.as_str());
                    cypher_data.push_str("\"");
                }
                Value::Array(xs) => {
                    cypher_data.push_str("[");
                    for (idx, x) in xs.iter().enumerate() {
                        to_cypher_data0(x, cypher_data);
                        if idx != xs.len() - 1 {
                            cypher_data.push_str(", ");
                        }
                    }
                    cypher_data.push_str("]");
                }
                Value::Object(obj) => {
                    cypher_data.push_str("{");
                    for (idx, (k, v)) in obj.iter().enumerate() {
                        let must_escape = k.contains(".")
                            || k.contains("-")
                            || k.contains(":")
                            || k.contains("/");
                        if must_escape {
                            cypher_data.push_str("`");
                        }
                        cypher_data.push_str(k);
                        if must_escape {
                            cypher_data.push_str("`");
                        }

                        cypher_data.push_str(": ");
                        to_cypher_data0(v, cypher_data);
                        if idx != obj.len() - 1 {
                            cypher_data.push_str(", ");
                        }
                    }
                    cypher_data.push_str("}");
                }
            }
        }
        to_cypher_data0(value, &mut cypher_data);
        cypher_data
    }
}
