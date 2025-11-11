use crate::prelude::*;
use crate::state::{ClusterState, ClusterStateDiff, GraphEdge};
use crate::types::{Edge, GenericObject, ResourceAttributes, ResourceType, LOGICAL_RESOURCE_TYPES};
use k8s_openapi::Metadata;
use rsmgclient::{ConnectParams, Connection, ConnectionStatus, Record};
use serde::Serialize;
use serde_json::{Number, Value};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::time::Instant;
use strum::IntoEnumIterator;
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
    pub fn try_new_from_url(url: &str) -> Result<Self> {
        let binding = url.replace("bolt://", "");
        let vec = binding.split(":").collect::<Vec<_>>();
        assert_eq!(vec.len(), 2);
        let host = vec[0].to_string();
        let port: u16 = vec[1].parse().map_err(|err| {
            MemgraphError::ConnectionError(format!("Failed to parse port from url: {err:?}"))
        })?;

        info!("Connecting to memgraph at {}:{}", host, port);

        let params = ConnectParams {
            port,
            host: Some(host),
            ..Default::default()
        };
        Self::try_new(params)
    }
    pub fn try_new(params: ConnectParams) -> Result<Self> {
        let connection: Connection = Connection::connect(&params)
            .map_err(|e| MemgraphError::ConnectionError(e.to_string()))?;
        let status = connection.status();
        if status != ConnectionStatus::Ready {
            println!("Connection failed with status: {status:?}");
            return Err(MemgraphError::ConnectionError(format!(
                "Connection status {status:?}"
            )))?;
        }

        Ok(Self { connection })
    }

    pub fn create(&mut self, cluster_state: &ClusterState) -> Result<()> {
        let s = Instant::now();

        // Clear the graph.
        self.connection
            .execute_without_results("MATCH (n) DETACH DELETE n;")
            .map_err(|e| MemgraphError::QueryError(e.to_string()))?;

        // Create nodes
        let mut unique_types: HashSet<ResourceType> = HashSet::new();
        for node in cluster_state.get_nodes() {
            let create_query = Self::get_create_query(node)?;
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
            let create_edge_query = format!("MATCH (u:{:?}), (v:{:?}) WHERE u.metadata.uid = '{}' AND v.metadata.uid = '{}' CREATE (u)-[:{:?}]->(v);", edge.source_type, edge.target_type, edge.source, edge.target, edge.edge_type);
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

        fn is_logical_type(rt: &ResourceType) -> bool {
            LOGICAL_RESOURCE_TYPES.contains(rt)
        }

        let all_types_that_can_have_events =
            ResourceType::iter().filter(|rt| rt != &ResourceType::Event && !is_logical_type(rt));
        for rt in all_types_that_can_have_events {
            unique_edges.insert((rt, ResourceType::Event, Edge::Concerns));
        }

        let mut unique_edges: Vec<(ResourceType, ResourceType, Edge)> =
            unique_edges.into_iter().collect::<Vec<_>>();

        unique_edges.sort_by(|a, b| {
            a.0.to_string()
                .cmp(&b.0.to_string())
                .then(a.1.to_string().cmp(&b.1.to_string()))
                .then(a.2.to_string().cmp(&b.2.to_string()))
        });

        info!("There are {} edges in this graph", unique_edges.len());
        for (source_type, target_type, edge_type) in &unique_edges {
            trace!(
                "(:{:?})-[:{:?}]->(:{:?})",
                source_type,
                edge_type,
                target_type
            );
        }
        Result::Ok(())
    }

    pub fn update_from_diff(&mut self, diff: &ClusterStateDiff) -> Result<()> {
        if diff.is_empty() {
            return Ok(());
        }
        let s = Instant::now();

        let mut changed = false;

        for edge in &diff.removed_edges {
            let query = Self::get_delete_edge_query(edge);
            self.connection
                .execute_without_results(&query)
                .map_err(|e| {
                    MemgraphError::QueryError(format!(
                        "Failed to delete {:?}: {}",
                        edge,
                        e.to_string()
                    ))
                })?;
            changed = true;
        }

        for node in &diff.removed_nodes {
            let query = Self::get_delete_node_query(node);
            self.connection
                .execute_without_results(&query)
                .map_err(|e| {
                    MemgraphError::QueryError(format!(
                        "Failed to delete the node with id {:?} and type {}: {}",
                        node.id,
                        node.resource_type,
                        e.to_string()
                    ))
                })?;
            changed = true;
        }

        for node in &diff.added_nodes {
            let create_query = Self::get_create_query(node)?;
            self.connection
                .execute_without_results(&create_query)
                .map_err(|e| {
                    MemgraphError::QueryError(format!(
                        "Failed to create the node with id {:?} and type {}: {}",
                        node.id,
                        node.resource_type,
                        e.to_string()
                    ))
                })?;
            changed = true;
        }

        for node in &diff.modified_nodes {
            let update_query = Self::get_update_query(node)?;
            self.connection
                .execute_without_results(&update_query)
                .map_err(|e| {
                    MemgraphError::QueryError(format!(
                        "Failed to update the node with id {:?} and type {}: {}",
                        node.id,
                        node.resource_type,
                        e.to_string()
                    ))
                })?;
            changed = true;
        }

        for edge in &diff.added_edges {
            let query = Self::get_merge_edge_query(edge);
            self.connection
                .execute_without_results(&query)
                .map_err(|e| {
                    MemgraphError::QueryError(format!(
                        "Failed to merge {:?}: {}",
                        edge,
                        e.to_string()
                    ))
                })?;
            changed = true;
        }

        if changed {
            self.connection
                .commit()
                .map_err(|e| MemgraphError::CommitError(e.to_string()))?;
        }

        info!(
            "Applied diff in {} ms: +{} nodes, -{} nodes, ~{} nodes, +{} edges, -{} edges",
            s.elapsed().as_millis(),
            diff.added_nodes.len(),
            diff.removed_nodes.len(),
            diff.modified_nodes.len(),
            diff.added_edges.len(),
            diff.removed_edges.len(),
        );
        Ok(())
    }

    pub fn execute_query(&mut self, query: &str) -> Result<Vec<Value>> {
        let cols = self
            .connection
            .execute(query, None)
            .map_err(|e| MemgraphError::QueryError(e.to_string()))?;
        let records = self
            .connection
            .fetchall()
            .map_err(|e| MemgraphError::QueryError(e.to_string()))?;
        let mut result: Vec<Value> = Vec::with_capacity(records.len());
        for records in records {
            result.push(Self::record_to_json(cols.as_slice(), &records)?);
        }
        Ok(result)
    }

    fn get_create_query(obj: &GenericObject) -> Result<String> {
        let r = match obj.resource_type {
            ResourceType::Pod => {
                format!(
                    r#"CREATE (n:Pod {});"#,
                    Self::json_to_cypher(&Self::get_as_json(obj)?)
                )
            }
            ResourceType::Deployment => {
                format!(
                    r#"CREATE (n:Deployment {})"#,
                    Self::json_to_cypher(&Self::get_as_json(obj)?)
                )
            }
            ResourceType::StatefulSet => {
                format!(
                    r#"CREATE (n:StatefulSet {})"#,
                    Self::json_to_cypher(&Self::get_as_json(obj)?)
                )
            }
            ResourceType::ReplicaSet => {
                format!(
                    r#"CREATE (n:ReplicaSet {})"#,
                    Self::json_to_cypher(&Self::get_as_json(obj)?)
                )
            }
            ResourceType::DaemonSet => {
                format!(
                    r#"CREATE (n:DaemonSet {})"#,
                    Self::json_to_cypher(&Self::get_as_json(obj)?)
                )
            }
            ResourceType::Job => {
                format!(
                    r#"CREATE (n:Job {})"#,
                    Self::json_to_cypher(&Self::get_as_json(obj)?)
                )
            }
            ResourceType::Ingress => {
                format!(
                    r#"CREATE (n:Ingress {})"#,
                    Self::json_to_cypher(&Self::get_as_json(obj)?)
                )
            }
            ResourceType::Service => {
                format!(
                    r#"CREATE (n:Service {})"#,
                    Self::json_to_cypher(&Self::get_as_json(obj)?)
                )
            }
            ResourceType::EndpointSlice => {
                format!(
                    r#"CREATE (n:EndpointSlice {})"#,
                    Self::json_to_cypher(&Self::get_as_json(obj)?)
                )
            }
            ResourceType::NetworkPolicy => {
                format!(
                    r#"CREATE (n:NetworkPolicy {})"#,
                    Self::json_to_cypher(&Self::get_as_json(obj)?)
                )
            }
            ResourceType::ConfigMap => {
                format!(
                    r#"CREATE (n:ConfigMap {})"#,
                    Self::json_to_cypher(&Self::get_as_json(obj)?)
                )
            }
            ResourceType::Provisioner => {
                format!(
                    r#"CREATE (n:Provisioner {})"#,
                    Self::json_to_cypher(&Self::get_as_json(obj)?)
                )
            }
            ResourceType::StorageClass => {
                format!(
                    r#"CREATE (n:StorageClass {})"#,
                    Self::json_to_cypher(&Self::get_as_json(obj)?)
                )
            }
            ResourceType::PersistentVolumeClaim => {
                format!(
                    r#"CREATE (n:PersistentVolumeClaim {})"#,
                    Self::json_to_cypher(&Self::get_as_json(obj)?)
                )
            }
            ResourceType::PersistentVolume => {
                format!(
                    r#"CREATE (n:PersistentVolume {})"#,
                    Self::json_to_cypher(&Self::get_as_json(obj)?)
                )
            }
            ResourceType::Node => {
                format!(
                    r#"CREATE (n:Node {})"#,
                    Self::json_to_cypher(&Self::get_as_json(obj)?)
                )
            }

            ResourceType::Namespace => {
                format!(
                    r#"CREATE (n:Namespace {})"#,
                    Self::json_to_cypher(&Self::get_as_json(obj)?)
                )
            }
            ResourceType::ServiceAccount => {
                format!(
                    r#"CREATE (n:ServiceAccount {})"#,
                    Self::json_to_cypher(&Self::get_as_json(obj)?)
                )
            }
            ResourceType::Event => {
                format!(
                    r#"CREATE (n:Event {})"#,
                    Self::json_to_cypher(&Self::get_as_json(obj)?)
                )
            }
            ResourceType::IngressServiceBackend => {
                format!(
                    r#"CREATE (n:IngressServiceBackend {})"#,
                    Self::json_to_cypher(&Self::get_as_json(obj)?)
                )
            }
            ResourceType::EndpointAddress => {
                format!(
                    r#"CREATE (n:EndpointAddress {})"#,
                    Self::json_to_cypher(&Self::get_as_json(obj)?)
                )
            }
            ResourceType::Endpoint => {
                format!(
                    r#"CREATE (n:Endpoint {})"#,
                    Self::json_to_cypher(&Self::get_as_json(obj)?)
                )
            }
            ResourceType::Host => {
                format!(
                    r#"CREATE (n:Host {})"#,
                    Self::json_to_cypher(&Self::get_as_json(obj)?)
                )
            }
            ResourceType::Cluster => {
                format!(
                    r#"CREATE (n:Cluster {})"#,
                    Self::json_to_cypher(&Self::get_as_json(obj)?)
                )
            }
            ResourceType::Container => {
                format!(
                    r#"CREATE (n:Container {})"#,
                    Self::json_to_cypher(&Self::get_as_json(obj)?)
                )
            }
            ResourceType::Logs => {
                format!(
                    r#"CREATE (n:Logs {})"#,
                    Self::json_to_cypher(&Self::get_as_json(obj)?)
                )
            }
        };
        Ok(r)
    }

    fn get_update_query(obj: &GenericObject) -> Result<String> {
        let properties = Self::json_to_cypher(&Self::get_as_json(obj)?);
        Ok(format!(
            "MATCH (n:{label:?}) WHERE n.metadata.uid = '{uid}' SET n = {properties} ",
            label = obj.resource_type,
            uid = Self::escape_identifier(&obj.id.uid),
            properties = properties
        ))
    }

    fn get_as_json(obj: &GenericObject) -> Result<Value> {
        let Some(attributes) = &obj.attributes else {
            return Ok(Value::Null);
        };
        let v = match attributes.as_ref() {
            ResourceAttributes::Node { node: value } => {
                let mut fixed = value.as_ref().clone();
                Self::cleanup_metadata(&mut fixed);
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::Namespace { namespace: value } => {
                let mut fixed = value.as_ref().clone();
                Self::cleanup_metadata(&mut fixed);
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::Pod { pod: value } => {
                let mut fixed = value.as_ref().clone();
                Self::cleanup_metadata(&mut fixed);
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::Deployment { deployment: value } => {
                let mut fixed = value.as_ref().clone();
                Self::cleanup_metadata(&mut fixed);
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::StatefulSet {
                stateful_set: value,
            } => {
                let mut fixed = value.as_ref().clone();
                Self::cleanup_metadata(&mut fixed);
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::ReplicaSet { replica_set: value } => {
                let mut fixed = value.as_ref().clone();
                Self::cleanup_metadata(&mut fixed);
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::DaemonSet { daemon_set: value } => {
                let mut fixed = value.as_ref().clone();
                Self::cleanup_metadata(&mut fixed);
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::Job { job: value } => {
                let mut fixed = value.as_ref().clone();
                Self::cleanup_metadata(&mut fixed);
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::Ingress { ingress: value } => {
                let mut fixed = value.as_ref().clone();
                Self::cleanup_metadata(&mut fixed);
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::Service { service: value } => {
                let mut fixed = value.as_ref().clone();
                Self::cleanup_metadata(&mut fixed);
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::EndpointSlice {
                endpoint_slice: value,
            } => {
                let mut fixed = value.as_ref().clone();
                Self::cleanup_metadata(&mut fixed);
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::NetworkPolicy {
                network_policy: value,
            } => {
                let mut fixed = value.as_ref().clone();
                Self::cleanup_metadata(&mut fixed);
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::ConfigMap { config_map } => {
                let mut fixed = config_map.as_ref().clone();
                Self::cleanup_metadata(&mut fixed);
                fixed.data = None;
                fixed.binary_data = None;
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::Provisioner { provisioner } => serde_json::to_value(provisioner)?,
            ResourceAttributes::StorageClass {
                storage_class: value,
            } => {
                let mut fixed = value.as_ref().clone();
                Self::cleanup_metadata(&mut fixed);
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::PersistentVolume { pv: value } => {
                let mut fixed = value.as_ref().clone();
                Self::cleanup_metadata(&mut fixed);
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::PersistentVolumeClaim { pvc: value } => {
                let mut fixed = value.as_ref().clone();
                Self::cleanup_metadata(&mut fixed);
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::ServiceAccount {
                service_account: value,
            } => {
                let mut fixed = value.as_ref().clone();
                Self::cleanup_metadata(&mut fixed);
                serde_json::to_value(fixed)?
            }
            ResourceAttributes::Logs { logs: context } => serde_json::to_value(context)?,
            ResourceAttributes::Event { event: context } => serde_json::to_value(context.as_ref())?,
            ResourceAttributes::IngressServiceBackend {
                ingress_service_backend,
            } => serde_json::to_value(ingress_service_backend)?,
            ResourceAttributes::EndpointAddress { endpoint_address } => {
                serde_json::to_value(endpoint_address)?
            }
            ResourceAttributes::Host { host } => serde_json::to_value(host)?,
            ResourceAttributes::Cluster { cluster: context } => serde_json::to_value(context)?,
            ResourceAttributes::Container { container: context } => serde_json::to_value(context)?,
            ResourceAttributes::Endpoint { endpoint: context } => serde_json::to_value(context)?,
        };

        Ok(v)
    }

    fn cleanup_metadata<T>(fixed: &mut T)
    where
        T: Metadata<Ty = k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta>,
    {
        let md = fixed.metadata_mut();
        if md.managed_fields.is_some() {
            md.managed_fields = None;
        }
        if let Some(map) = md.annotations.as_mut() {
            // The following annotations are quite complicated to escape properly, we just remove them for now ;)
            map.remove("kubectl.kubernetes.io/last-applied-configuration");
            map.remove("kapp.k14s.io/original");
        }
    }

    fn get_create_indices_query(rt: &ResourceType) -> Vec<String> {
        vec![
            format!("CREATE INDEX ON :{rt:?}(metadata.name)"),
            format!("CREATE INDEX ON :{rt:?}(metadata.uid)"),
            format!("CREATE INDEX ON :{rt:?}(metadata.namespace)"),
        ]
    }

    fn get_delete_node_query(obj: &GenericObject) -> String {
        format!(
            "MATCH (n:{label:?}) WHERE n.metadata.uid = '{uid}' DETACH DELETE n ",
            label = obj.resource_type,
            uid = Self::escape_identifier(&obj.id.uid)
        )
    }

    fn get_delete_edge_query(edge: &GraphEdge) -> String {
        format!(
            "MATCH (u:{source_type:?})-[r:{edge_type:?}]->(v:{target_type:?}) WHERE u.metadata.uid = '{source}' AND v.metadata.uid = '{target}' DELETE r",
            source_type = edge.source_type,
            source = Self::escape_identifier(&edge.source),
            edge_type = edge.edge_type,
            target_type = edge.target_type,
            target = Self::escape_identifier(&edge.target),
        )
    }

    fn get_merge_edge_query(edge: &GraphEdge) -> String {
        format!(
            "MATCH (u:{source_type:?} ), (v:{target_type:?}) WHERE u.metadata.uid = '{source}' AND v.metadata.uid = '{target}' MERGE (u)-[:{edge_type:?}]->(v)",
            source_type = edge.source_type,
            source = Self::escape_identifier(&edge.source),
            target_type = edge.target_type,
            target = Self::escape_identifier(&edge.target),
            edge_type = edge.edge_type,
        )
    }

    fn escape_identifier(value: &str) -> String {
        value.replace('\\', "\\\\").replace('\'', "\\'")
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
                    cypher_data.push('"');
                    let escaped = s
                        .replace("\\", "\\\\")
                        .replace("\"", "\\\"")
                        .replace("\n", "\\n");
                    cypher_data.push_str(escaped.as_str());
                    cypher_data.push('"');
                }
                Value::Array(xs) => {
                    cypher_data.push('[');
                    for (idx, x) in xs.iter().enumerate() {
                        to_cypher_data0(x, cypher_data);
                        if idx != xs.len() - 1 {
                            cypher_data.push_str(", ");
                        }
                    }
                    cypher_data.push(']');
                }
                Value::Object(obj) => {
                    cypher_data.push('{');
                    for (idx, (k, v)) in obj.iter().enumerate() {
                        let must_escape = k.contains(".")
                            || k.contains("-")
                            || k.contains(":")
                            || k.contains("/");
                        if must_escape {
                            cypher_data.push('`');
                        }
                        cypher_data.push_str(k);
                        if must_escape {
                            cypher_data.push('`');
                        }

                        cypher_data.push_str(": ");
                        to_cypher_data0(v, cypher_data);
                        if idx != obj.len() - 1 {
                            cypher_data.push_str(", ");
                        }
                    }
                    cypher_data.push('}');
                }
            }
        }
        to_cypher_data0(value, &mut cypher_data);
        cypher_data
    }

    fn record_to_json(columns: &[String], value: &Record) -> Result<Value> {
        let mut map = serde_json::Map::new();
        for (col, value) in columns.iter().zip(value.values.as_slice()) {
            map.insert(col.to_string(), record_to_json0(value)?);
        }
        Ok(Value::Object(map))
    }
}

fn record_to_json0(value: &rsmgclient::Value) -> Result<Value> {
    let r = match value {
        rsmgclient::Value::Null => Value::Null,
        rsmgclient::Value::Bool(v) => Value::Bool(*v),
        rsmgclient::Value::Int(n) => Value::Number(Number::from(*n)),
        rsmgclient::Value::Float(n) => Value::Number(Number::from_f64(*n).unwrap()),
        rsmgclient::Value::String(s) => Value::String(s.clone()),
        rsmgclient::Value::List(xs) => {
            let mut v = Vec::new();
            for x in xs {
                v.push(record_to_json0(x)?);
            }
            Value::Array(v)
        }
        rsmgclient::Value::Date(d) => Value::String(d.format("%Y-%m-%d").to_string()),
        rsmgclient::Value::LocalTime(lt) => Value::String(lt.format("%H:%M:%S").to_string()),
        rsmgclient::Value::LocalDateTime(dt) => Value::String(dt.and_utc().to_rfc3339()),
        rsmgclient::Value::Duration(d) => Value::String(d.to_string()),
        rsmgclient::Value::Map(m) => {
            let mut map = serde_json::Map::new();
            for (k, v) in m {
                map.insert(k.clone(), record_to_json0(v)?);
            }
            Value::Object(map)
        }
        rsmgclient::Value::Node(n) => serde_json::to_value(Node::try_new(n)?)?,
        rsmgclient::Value::Relationship(rel) => serde_json::to_value(Relationship::try_new(rel)?)?,
        rsmgclient::Value::UnboundRelationship(rel) => {
            serde_json::to_value(UnboundRelationship::try_new(rel)?)?
        }
        rsmgclient::Value::Path(path) => serde_json::to_value(Path::try_new(path)?)?,
        rsmgclient::Value::DateTime(_) => unimplemented!("Value::DateTime"),
        rsmgclient::Value::Point2D(_) => unimplemented!("Value::Point2D"),
        rsmgclient::Value::Point3D(_) => unimplemented!("Value::Point3D"),
    };
    Ok(r)
}

#[derive(Debug, PartialEq, Clone, Serialize)]
struct Node {
    pub id: i64,
    pub label_count: u32,
    pub labels: Vec<String>,
    pub properties: HashMap<String, Value>,
    #[serde(rename = "type")]
    pub type_: String,
}

impl Node {
    pub fn try_new(n: &rsmgclient::Node) -> Result<Self> {
        let properties = {
            let mut map = HashMap::new();
            for (k, v) in &n.properties {
                map.insert(k.clone(), record_to_json0(v)?);
            }
            map
        };
        Ok(Self {
            id: n.id,
            label_count: n.label_count,
            labels: n.labels.clone(),
            properties,
            type_: "node".to_string(),
        })
    }
}

#[derive(Debug, PartialEq, Clone, Serialize)]
struct Relationship {
    pub id: i64,
    pub start_id: i64,
    pub end_id: i64,
    pub label: String,
    #[serde(rename = "type")]
    pub type_: String,
    pub properties: HashMap<String, Value>,
}
impl Relationship {
    fn try_new(r: &rsmgclient::Relationship) -> Result<Self> {
        let properties = {
            let mut map = HashMap::new();
            for (k, v) in &r.properties {
                map.insert(k.clone(), record_to_json0(v)?);
            }
            map
        };
        Ok(Self {
            id: r.id,
            start_id: r.start_id,
            end_id: r.end_id,
            label: r.type_.clone(),
            type_: "relationship".to_string(),
            properties,
        })
    }
}

#[derive(Debug, PartialEq, Clone, Serialize)]
struct UnboundRelationship {
    pub id: i64,
    pub label: String,
    #[serde(rename = "type")]
    pub type_: String,
    pub properties: HashMap<String, Value>,
}

impl UnboundRelationship {
    fn try_new(r: &rsmgclient::UnboundRelationship) -> Result<Self> {
        let properties = {
            let mut map = HashMap::new();
            for (k, v) in &r.properties {
                map.insert(k.clone(), record_to_json0(v)?);
            }
            map
        };
        Ok(Self {
            id: r.id,
            label: r.type_.clone(),
            type_: "unbound_relationship".to_string(),
            properties,
        })
    }
}

#[derive(Debug, PartialEq, Clone, Serialize)]
struct Path {
    pub node_count: u32,
    pub relationship_count: u32,
    pub nodes: Vec<Node>,
    pub relationships: Vec<UnboundRelationship>,
}

impl Path {
    pub fn try_new(p: &rsmgclient::Path) -> Result<Self> {
        let nodes = {
            let mut vec = Vec::new();
            for n in &p.nodes {
                vec.push(Node::try_new(n)?);
            }
            vec
        };
        let relationships = {
            let mut vec = Vec::new();
            for r in &p.relationships {
                vec.push(UnboundRelationship::try_new(r)?);
            }
            vec
        };
        Ok(Self {
            node_count: p.node_count,
            relationship_count: p.relationship_count,
            nodes,
            relationships,
        })
    }
}
