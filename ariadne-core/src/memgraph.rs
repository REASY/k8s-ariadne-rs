use crate::prelude::*;
use crate::state::ClusterState;
use crate::types::{GenericObject, ObjectIdentifier, ResourceType};
use k8s_openapi::DeepMerge;
use rsmgclient::{ConnectParams, Connection, ConnectionStatus};
use serde_json::{Map, Value};
use std::collections::HashSet;
use std::time::Instant;
use thiserror::Error;
use tracing::info;

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
            let create_query = Self::get_create_query(&node);
            println!("{}", create_query);
            self.connection
                .execute_without_results(&create_query)
                .map_err(|e| MemgraphError::QueryError(e.to_string()))?;

            unique_types.insert(node.resource_type.clone());
        }
        self.connection
            .commit()
            .map_err(|e| MemgraphError::CommitError(e.to_string()))?;

        // Create indices
        for resource_type in unique_types {
            let create_index_query = Self::get_create_index_query(&resource_type);
            self.connection
                .execute_without_results(&create_index_query)
                .map_err(|e| MemgraphError::QueryError(e.to_string()))?;
        }
        self.connection
            .commit()
            .map_err(|e| MemgraphError::CommitError(e.to_string()))?;

        // Create edges
        for edge in cluster_state.get_edges() {
            let create_edge_query = format!("MATCH (u:{:?}), (v:{:?}) WHERE u.metadata.uid = '{}' AND v.metadata.uid = '{}' CREATE (u)-[:{:?}]->(v);",  edge.source_type, edge.target_type, edge.source, edge.target, edge.edge_type);
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

        Result::Ok(())
    }

    fn get_create_query(obj: &GenericObject) -> String {
        match obj.resource_type {
            ResourceType::Pod => {
                format!(
                    r#"CREATE (n:Pod {});"#,
                    Self::json_to_cypher(&Self::get_metadata_v2(&obj.id))
                )
            }
            ResourceType::Deployment => {
                format!(
                    r#"CREATE (n:Deployment {})"#,
                    Self::json_to_cypher(&Self::get_metadata_v2(&obj.id))
                )
            }
            ResourceType::StatefulSet => {
                format!(
                    r#"CREATE (n:StatefulSet {})"#,
                    Self::json_to_cypher(&Self::get_metadata_v2(&obj.id))
                )
            }
            ResourceType::ReplicaSet => {
                format!(
                    r#"CREATE (n:ReplicaSet {})"#,
                    Self::json_to_cypher(&Self::get_metadata_v2(&obj.id))
                )
            }
            ResourceType::DaemonSet => {
                format!(
                    r#"CREATE (n:DaemonSet {})"#,
                    Self::json_to_cypher(&Self::get_metadata_v2(&obj.id))
                )
            }
            ResourceType::Job => {
                format!(
                    r#"CREATE (n:Job {})"#,
                    Self::json_to_cypher(&Self::get_metadata_v2(&obj.id))
                )
            }
            ResourceType::Ingress => {
                format!(
                    r#"CREATE (n:Ingress {})"#,
                    Self::json_to_cypher(&Self::get_metadata_v2(&obj.id))
                )
            }
            ResourceType::Service => {
                format!(
                    r#"CREATE (n:Service {})"#,
                    Self::json_to_cypher(&Self::get_metadata_v2(&obj.id))
                )
            }
            ResourceType::Endpoints => {
                format!(
                    r#"CREATE (n:Endpoints {})"#,
                    Self::json_to_cypher(&Self::get_metadata_v2(&obj.id))
                )
            }
            ResourceType::NetworkPolicy => {
                format!(
                    r#"CREATE (n:NetworkPolicy {})"#,
                    Self::json_to_cypher(&Self::get_metadata_v2(&obj.id))
                )
            }
            ResourceType::ConfigMap => {
                format!(
                    r#"CREATE (n:ConfigMap {})"#,
                    Self::json_to_cypher(&Self::get_metadata_v2(&obj.id))
                )
            }
            ResourceType::Provisioner => {
                format!(
                    r#"CREATE (n:Provisioner {})"#,
                    Self::json_to_cypher(&Self::get_metadata_v2(&obj.id))
                )
            }
            ResourceType::StorageClass => {
                format!(
                    r#"CREATE (n:StorageClass {})"#,
                    Self::json_to_cypher(&Self::get_metadata_v2(&obj.id))
                )
            }
            ResourceType::PersistentVolumeClaim => {
                format!(
                    r#"CREATE (n:PersistentVolumeClaim {})"#,
                    Self::json_to_cypher(&Self::get_metadata_v2(&obj.id))
                )
            }
            ResourceType::PersistentVolume => {
                format!(
                    r#"CREATE (n:PersistentVolume {})"#,
                    Self::json_to_cypher(&Self::get_metadata_v2(&obj.id))
                )
            }
            ResourceType::Node => {
                format!(
                    r#"CREATE (n:Node {})"#,
                    Self::json_to_cypher(&Self::get_metadata_v2(&obj.id))
                )
            }
            ResourceType::ServiceAccount => {
                format!(
                    r#"CREATE (n:ServiceAccount {})"#,
                    Self::json_to_cypher(&Self::get_metadata_v2(&obj.id))
                )
            }
            ResourceType::IngressServiceBackend => {
                format!(
                    r#"CREATE (n:IngressServiceBackend {})"#,
                    Self::json_to_cypher(&Self::get_metadata_v2(&obj.id))
                )
            }
            ResourceType::EndpointAddress => {
                format!(
                    r#"CREATE (n:EndpointAddress {})"#,
                    Self::json_to_cypher(&Self::get_metadata_v2(&obj.id))
                )
            }
            ResourceType::Host => {
                let mut host_json = Value::Object(Map::from_iter([(
                    "value".to_string(),
                    Value::String(obj.id.name.clone()),
                )]));
                host_json.merge_from(Self::get_metadata_v2(&obj.id));
                format!(r#"CREATE (n:Host {})"#, Self::json_to_cypher(&host_json))
            }
        }
    }

    fn get_metadata_v2(obj_id: &ObjectIdentifier) -> Value {
        let mut map = Map::new();
        map.insert("uid".to_string(), Value::String(obj_id.uid.clone()));
        map.insert("name".to_string(), Value::String(obj_id.name.clone()));
        match obj_id.namespace.as_ref() {
            None => {
                map.insert("namespace".to_string(), Value::Null);
            }
            Some(namespace) => {
                map.insert("namespace".to_string(), Value::String(namespace.clone()));
            }
        }
        match obj_id.resource_version.as_ref() {
            None => {
                map.insert("resource_version".to_string(), Value::Null);
            }
            Some(resource_version) => {
                map.insert(
                    "resource_version".to_string(),
                    Value::String(resource_version.clone()),
                );
            }
        }
        Value::Object(Map::from_iter([(
            "metadata".to_string(),
            Value::Object(map),
        )]))
    }

    #[allow(unused)]
    fn get_metadata(obj_id: &ObjectIdentifier) -> String {
        let mut cypher_query = String::new();
        cypher_query.push_str("{uid: '");
        cypher_query.push_str(&obj_id.uid);
        cypher_query.push_str("', ");

        cypher_query.push_str("name: '");
        cypher_query.push_str(&obj_id.name);
        cypher_query.push_str("', ");

        cypher_query.push_str("namespace: ");
        match obj_id.namespace.as_ref() {
            None => {
                cypher_query.push_str("NULL");
            }
            Some(namespace) => {
                cypher_query.push_str("'");
                cypher_query.push_str(namespace);
                cypher_query.push_str("'");
            }
        }
        cypher_query.push_str(", resource_version: ");
        match obj_id.resource_version.as_ref() {
            None => {
                cypher_query.push_str("NULL");
            }
            Some(resource_version) => {
                cypher_query.push_str("'");
                cypher_query.push_str(resource_version);
                cypher_query.push_str("'");
            }
        }
        cypher_query.push_str("}");

        cypher_query
    }

    fn get_create_index_query(rt: &ResourceType) -> String {
        match rt {
            _ => format!(
                r#"CREATE INDEX ON :{rt:?}(metadata.uid, metadata.name, metadata.namespace);"#,
            ), // ResourceType::Pod => {
               //     format!(
               //         r#"CREATE INDEX ON :Pod(metadata.uid, metadata.name, metadata.namespace)"#,
               //     )
               // }
               // ResourceType::Deployment => {}
               // ResourceType::StatefulSet => {}
               // ResourceType::ReplicaSet => {}
               // ResourceType::DaemonSet => {}
               // ResourceType::Job => {}
               // ResourceType::Ingress => {}
               // ResourceType::Service => {}
               // ResourceType::Endpoints => {}
               // ResourceType::NetworkPolicy => {}
               // ResourceType::ConfigMap => {}
               // ResourceType::Provisioner => {}
               // ResourceType::StorageClass => {}
               // ResourceType::PersistentVolumeClaim => {}
               // ResourceType::PersistentVolume => {}
               // ResourceType::Node => {}
               // ResourceType::ServiceAccount => {}
               // ResourceType::IngressServiceBackend => {}
               // ResourceType::EndpointAddress => {}
               // ResourceType::Host => {}
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
                    cypher_data.push_str("'");
                    cypher_data.push_str(s);
                    cypher_data.push_str("'");
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
                        cypher_data.push_str(k);
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
