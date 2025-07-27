use crate::prelude::*;
use crate::state::ClusterState;
use crate::types::{GenericObject, ObjectIdentifier, ResourceType};
use rsmgclient::{ConnectParams, Connection, ConnectionStatus};
use std::collections::HashSet;
use thiserror::Error;

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
        // Create nodes
        let mut unique_types: HashSet<ResourceType> = HashSet::new();
        for node in cluster_state.get_nodes() {
            let create_query = Self::get_create_query(&node);
            // println!("{}", create_query);
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

        Result::Ok(())
    }

    fn get_create_query(obj: &GenericObject) -> String {
        match obj.resource_type {
            ResourceType::Pod => {
                format!(
                    r#"CREATE (n:Pod {{ metadata: {} }});"#,
                    Self::get_metadata(&obj.id)
                )
            }
            ResourceType::Deployment => {
                format!(
                    r#"CREATE (n:Deployment {{ metadata: {} }})"#,
                    Self::get_metadata(&obj.id)
                )
            }
            ResourceType::StatefulSet => {
                format!(
                    r#"CREATE (n:StatefulSet {{ metadata: {} }})"#,
                    Self::get_metadata(&obj.id)
                )
            }
            ResourceType::ReplicaSet => {
                format!(
                    r#"CREATE (n:ReplicaSet {{ metadata: {} }})"#,
                    Self::get_metadata(&obj.id)
                )
            }
            ResourceType::DaemonSet => {
                format!(
                    r#"CREATE (n:DaemonSet {{ metadata: {} }})"#,
                    Self::get_metadata(&obj.id)
                )
            }
            ResourceType::Job => {
                format!(
                    r#"CREATE (n:Job {{ metadata: {} }})"#,
                    Self::get_metadata(&obj.id)
                )
            }
            ResourceType::Ingress => {
                format!(
                    r#"CREATE (n:Ingress {{ metadata: {} }})"#,
                    Self::get_metadata(&obj.id)
                )
            }
            ResourceType::Service => {
                format!(
                    r#"CREATE (n:Service {{ metadata: {} }})"#,
                    Self::get_metadata(&obj.id)
                )
            }
            ResourceType::Endpoints => {
                format!(
                    r#"CREATE (n:Endpoints {{ metadata: {} }})"#,
                    Self::get_metadata(&obj.id)
                )
            }
            ResourceType::NetworkPolicy => {
                format!(
                    r#"CREATE (n:NetworkPolicy {{ metadata: {} }})"#,
                    Self::get_metadata(&obj.id)
                )
            }
            ResourceType::ConfigMap => {
                format!(
                    r#"CREATE (n:ConfigMap {{ metadata: {} }})"#,
                    Self::get_metadata(&obj.id)
                )
            }
            ResourceType::Provisioner => {
                format!(
                    r#"CREATE (n:Provisioner {{ metadata: {} }})"#,
                    Self::get_metadata(&obj.id)
                )
            }
            ResourceType::StorageClass => {
                format!(
                    r#"CREATE (n:StorageClass {{ metadata: {} }})"#,
                    Self::get_metadata(&obj.id)
                )
            }
            ResourceType::PersistentVolumeClaim => {
                format!(
                    r#"CREATE (n:PersistentVolumeClaim {{ metadata: {} }})"#,
                    Self::get_metadata(&obj.id)
                )
            }
            ResourceType::PersistentVolume => {
                format!(
                    r#"CREATE (n:PersistentVolume {{ metadata: {} }})"#,
                    Self::get_metadata(&obj.id)
                )
            }
            ResourceType::Node => {
                format!(
                    r#"CREATE (n:Node {{ metadata: {} }})"#,
                    Self::get_metadata(&obj.id)
                )
            }
            ResourceType::ServiceAccount => {
                format!(
                    r#"CREATE (n:ServiceAccount {{ metadata: {} }})"#,
                    Self::get_metadata(&obj.id)
                )
            }
            ResourceType::IngressServiceBackend => {
                format!(
                    r#"CREATE (n:IngressServiceBackend {{ metadata: {} }})"#,
                    Self::get_metadata(&obj.id)
                )
            }
            ResourceType::EndpointAddress => {
                format!(
                    r#"CREATE (n:EndpointAddress {{ metadata: {} }})"#,
                    Self::get_metadata(&obj.id)
                )
            }
            ResourceType::Host => {
                format!(
                    r#"CREATE (n:Host {{ metadata: {} }})"#,
                    Self::get_metadata(&obj.id)
                )
            }
        }
    }

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
}
