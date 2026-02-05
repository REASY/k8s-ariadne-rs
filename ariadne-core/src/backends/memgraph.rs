use crate::prelude::*;
use crate::state::{ClusterState, ClusterStateDiff, GraphEdge};
use crate::types::{Edge, GenericObject, ResourceAttributes, ResourceType, LOGICAL_RESOURCE_TYPES};
use k8s_openapi::Metadata;
use rsmgclient::{
    ConnectParams, Connection, ConnectionStatus, QueryParam, Record, SSLMode, TrustCallback,
};
use serde::Serialize;
use serde_json::{Number, Value};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::time::Instant;
use strum::IntoEnumIterator;
use thiserror::Error;
use tracing::{info, trace, warn};

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
    connect_params: ConnectParamsSnapshot,
}

struct ConnectParamsSnapshot {
    port: u16,
    host: Option<String>,
    address: Option<String>,
    username: Option<String>,
    password: Option<String>,
    client_name: String,
    sslmode: SSLMode,
    sslcert: Option<String>,
    sslkey: Option<String>,
    trust_callback: Option<TrustCallback>,
    lazy: bool,
    autocommit: bool,
}

fn clone_sslmode(mode: &SSLMode) -> SSLMode {
    match mode {
        SSLMode::Disable => SSLMode::Disable,
        SSLMode::Require => SSLMode::Require,
    }
}

impl ConnectParamsSnapshot {
    fn from_params(params: &ConnectParams) -> Self {
        Self {
            port: params.port,
            host: params.host.clone(),
            address: params.address.clone(),
            username: params.username.clone(),
            password: params.password.clone(),
            client_name: params.client_name.clone(),
            sslmode: clone_sslmode(&params.sslmode),
            sslcert: params.sslcert.clone(),
            sslkey: params.sslkey.clone(),
            trust_callback: params.trust_callback,
            lazy: params.lazy,
            autocommit: params.autocommit,
        }
    }

    fn to_params(&self) -> ConnectParams {
        ConnectParams {
            port: self.port,
            host: self.host.clone(),
            address: self.address.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            client_name: self.client_name.clone(),
            sslmode: clone_sslmode(&self.sslmode),
            sslcert: self.sslcert.clone(),
            sslkey: self.sslkey.clone(),
            trust_callback: self.trust_callback,
            lazy: self.lazy,
            autocommit: self.autocommit,
        }
    }
}

pub(crate) struct QuerySpec {
    query: String,
    params: HashMap<String, QueryParam>,
}

impl QuerySpec {
    pub(crate) fn new(query: String) -> Self {
        Self {
            query,
            params: HashMap::new(),
        }
    }

    pub(crate) fn with_params(query: String, params: HashMap<String, QueryParam>) -> Self {
        Self { query, params }
    }

    pub(crate) fn params(&self) -> Option<&HashMap<String, QueryParam>> {
        if self.params.is_empty() {
            None
        } else {
            Some(&self.params)
        }
    }

    #[allow(dead_code)]
    pub(crate) fn query(&self) -> &str {
        &self.query
    }

    #[allow(dead_code)]
    pub(crate) fn params_map(&self) -> &HashMap<String, QueryParam> {
        &self.params
    }
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
        let connect_params = ConnectParamsSnapshot::from_params(&params);
        let connection: Connection = Connection::connect(&params)
            .map_err(|e| MemgraphError::ConnectionError(e.to_string()))?;
        let status = connection.status();
        if status != ConnectionStatus::Ready {
            println!("Connection failed with status: {status:?}");
            return Err(MemgraphError::ConnectionError(format!(
                "Connection status {status:?}"
            )))?;
        }

        Ok(Self {
            connection,
            connect_params,
        })
    }

    fn ensure_connected(&mut self) -> Result<()> {
        let status = self.connection.status();
        if status == ConnectionStatus::Bad || status == ConnectionStatus::Closed {
            self.reconnect()?;
        }
        Ok(())
    }

    fn reconnect(&mut self) -> Result<()> {
        info!("Reconnecting to memgraph");
        let params = self.connect_params.to_params();
        let connection: Connection = Connection::connect(&params)
            .map_err(|e| MemgraphError::ConnectionError(e.to_string()))?;
        let status = connection.status();
        if status != ConnectionStatus::Ready {
            return Err(
                MemgraphError::ConnectionError(format!("Connection status {status:?}")).into(),
            );
        }
        self.connection = connection;
        Ok(())
    }

    fn reconnect_if_bad(&mut self) {
        let status = self.connection.status();
        if status == ConnectionStatus::Bad || status == ConnectionStatus::Closed {
            if let Err(err) = self.reconnect() {
                warn!("Failed to reconnect memgraph after bad connection: {err}");
            }
        }
    }

    fn execute_query_spec(&mut self, spec: &QuerySpec) -> Result<()> {
        self.connection
            .execute(&spec.query, spec.params())
            .map_err(|e| MemgraphError::QueryError(e.to_string()))?;
        self.connection
            .fetchall()
            .map_err(|e| MemgraphError::QueryError(e.to_string()))?;
        Ok(())
    }

    pub fn create(&mut self, cluster_state: &ClusterState) -> Result<()> {
        let nodes = cluster_state.get_nodes().cloned().collect::<Vec<_>>();
        let edges = cluster_state.get_edges().collect::<Vec<_>>();
        self.create_from_snapshot(&nodes, &edges)
    }

    pub fn create_from_snapshot(
        &mut self,
        nodes: &[GenericObject],
        edges: &[GraphEdge],
    ) -> Result<()> {
        self.ensure_connected()?;
        let s = Instant::now();

        // Clear the graph.
        self.connection
            .execute_without_results("MATCH (n) DETACH DELETE n;")
            .map_err(|e| MemgraphError::QueryError(e.to_string()))?;

        // Create nodes first (faster bulk load), then build indices.
        let mut unique_types: HashSet<ResourceType> = HashSet::new();
        for node in nodes {
            let create_spec = Self::get_create_query(node)?;
            trace!("{}", create_spec.query);
            self.execute_query_spec(&create_spec)?;
            unique_types.insert(node.resource_type.clone());
        }

        if !nodes.is_empty() {
            self.connection
                .commit()
                .map_err(|e| MemgraphError::CommitError(e.to_string()))?;
        }

        // Create indices after nodes to keep index build efficient.
        for resource_type in &unique_types {
            for create_index_query in Self::get_create_indices_query(resource_type) {
                trace!("{}", create_index_query);
                self.connection
                    .execute_without_results(&create_index_query)
                    .map_err(|e| MemgraphError::QueryError(e.to_string()))?;
            }
        }
        // Create edges
        let mut unique_edges: HashSet<(ResourceType, ResourceType, Edge)> = HashSet::new();
        for edge in edges {
            let create_edge_spec = Self::get_create_edge_query(edge);
            trace!("{}", create_edge_spec.query);
            unique_edges.insert((
                edge.source_type.clone(),
                edge.target_type.clone(),
                edge.edge_type.clone(),
            ));
            self.execute_query_spec(&create_edge_spec)?;
        }
        if !edges.is_empty() {
            self.connection
                .commit()
                .map_err(|e| MemgraphError::CommitError(e.to_string()))?;
        }
        info!(
            "Created a memgraph with {} nodes and {} edges in {}ms",
            nodes.len(),
            edges.len(),
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

        info!("There are {} edge types in this graph", unique_edges.len());
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
        self.ensure_connected()?;
        let s = Instant::now();

        let mut changed = false;

        for edge in &diff.removed_edges {
            let query = Self::get_delete_edge_query(edge);
            self.execute_query_spec(&query).map_err(|e| {
                MemgraphError::QueryError(format!("Failed to delete {edge:?}: {e}"))
            })?;
            changed = true;
        }

        for node in &diff.removed_nodes {
            let query = Self::get_delete_node_query(node);
            self.execute_query_spec(&query).map_err(|e| {
                MemgraphError::QueryError(format!(
                    "Failed to delete the node with id {:?} and type {}: {}",
                    node.id, node.resource_type, e
                ))
            })?;
            changed = true;
        }

        for node in &diff.added_nodes {
            let create_query = Self::get_create_query(node)?;
            self.execute_query_spec(&create_query).map_err(|e| {
                MemgraphError::QueryError(format!(
                    "Failed to create the node with id {:?} and type {}: {}",
                    node.id, node.resource_type, e
                ))
            })?;
            changed = true;
        }

        for node in &diff.modified_nodes {
            let update_query = Self::get_update_query(node)?;
            self.execute_query_spec(&update_query).map_err(|e| {
                MemgraphError::QueryError(format!(
                    "Failed to update the node with id {:?} and type {}: {}",
                    node.id, node.resource_type, e
                ))
            })?;
            changed = true;
        }

        for edge in &diff.added_edges {
            let query = Self::get_merge_edge_query(edge);
            self.execute_query_spec(&query)
                .map_err(|e| MemgraphError::QueryError(format!("Failed to merge {edge:?}: {e}")))?;
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
        self.ensure_connected()?;
        let cols = self.connection.execute(query, None);
        let cols = match cols {
            Ok(cols) => cols,
            Err(err) => {
                let msg = err.to_string();
                self.reconnect_if_bad();
                return Err(MemgraphError::QueryError(msg).into());
            }
        };
        let records = self.connection.fetchall().map_err(|e| {
            let msg = e.to_string();
            self.reconnect_if_bad();
            MemgraphError::QueryError(msg)
        })?;
        let mut result: Vec<Value> = Vec::with_capacity(records.len());
        for records in records {
            result.push(Self::record_to_json(cols.as_slice(), &records)?);
        }
        self.connection.commit().map_err(|e| {
            let msg = e.to_string();
            self.reconnect_if_bad();
            MemgraphError::CommitError(msg)
        })?;
        Ok(result)
    }

    pub(crate) fn get_create_query(obj: &GenericObject) -> Result<QuerySpec> {
        let properties = Self::get_properties_param(obj)?;
        let label = &obj.resource_type;
        match properties {
            Some(props) => {
                let mut params = HashMap::new();
                params.insert("props".to_string(), props);
                Ok(QuerySpec::with_params(
                    format!("CREATE (n:{label:?} $props)"),
                    params,
                ))
            }
            None => Ok(QuerySpec::new(format!("CREATE (n:{label:?})"))),
        }
    }

    pub(crate) fn get_update_query(obj: &GenericObject) -> Result<QuerySpec> {
        let properties = Self::get_properties_param(obj)?.unwrap_or(QueryParam::Null);
        let mut params = HashMap::new();
        params.insert("uid".to_string(), QueryParam::String(obj.id.uid.clone()));
        params.insert("props".to_string(), properties);
        Ok(QuerySpec::with_params(
            format!(
                "MATCH (n:{:?}) WHERE n.metadata.uid = $uid SET n = $props",
                obj.resource_type
            ),
            params,
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
            ResourceAttributes::Provisioner { provisioner } => {
                serde_json::to_value(provisioner.as_ref())?
            }
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
            ResourceAttributes::Logs { logs: context } => serde_json::to_value(context.as_ref())?,
            ResourceAttributes::Event { event: context } => serde_json::to_value(context.as_ref())?,
            ResourceAttributes::IngressServiceBackend {
                ingress_service_backend,
            } => serde_json::to_value(ingress_service_backend)?,
            ResourceAttributes::EndpointAddress { endpoint_address } => {
                serde_json::to_value(endpoint_address)?
            }
            ResourceAttributes::Host { host } => serde_json::to_value(host)?,
            ResourceAttributes::Cluster { cluster: context } => {
                serde_json::to_value(context.as_ref())?
            }
            ResourceAttributes::Container { container: context } => serde_json::to_value(context)?,
            ResourceAttributes::Endpoint { endpoint: context } => serde_json::to_value(context)?,
        };

        Ok(v)
    }

    pub(crate) fn get_properties_param(obj: &GenericObject) -> Result<Option<QueryParam>> {
        let json = Self::get_as_json(obj)?;
        if json.is_null() {
            return Ok(None);
        }
        Ok(Some(Self::json_to_query_param(&json)))
    }

    fn json_to_query_param(value: &Value) -> QueryParam {
        match value {
            Value::Null => QueryParam::Null,
            Value::Bool(v) => QueryParam::Bool(*v),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    QueryParam::Int(i)
                } else if let Some(u) = n.as_u64() {
                    if u <= i64::MAX as u64 {
                        QueryParam::Int(u as i64)
                    } else {
                        QueryParam::Float(u as f64)
                    }
                } else if let Some(f) = n.as_f64() {
                    QueryParam::Float(f)
                } else {
                    QueryParam::Null
                }
            }
            Value::String(s) => QueryParam::String(s.clone()),
            Value::Array(xs) => {
                QueryParam::List(xs.iter().map(Self::json_to_query_param).collect())
            }
            Value::Object(map) => QueryParam::Map(
                map.iter()
                    .map(|(k, v)| (k.clone(), Self::json_to_query_param(v)))
                    .collect(),
            ),
        }
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

    pub(crate) fn get_create_indices_query(rt: &ResourceType) -> Vec<String> {
        vec![
            format!("CREATE INDEX ON :{rt:?}(metadata.name)"),
            format!("CREATE INDEX ON :{rt:?}(metadata.uid)"),
            format!("CREATE INDEX ON :{rt:?}(metadata.namespace)"),
        ]
    }

    pub(crate) fn get_delete_node_query(obj: &GenericObject) -> QuerySpec {
        let mut params = HashMap::new();
        params.insert("uid".to_string(), QueryParam::String(obj.id.uid.clone()));
        QuerySpec::with_params(
            format!(
                "MATCH (n:{label:?}) WHERE n.metadata.uid = $uid DETACH DELETE n ",
                label = obj.resource_type
            ),
            params,
        )
    }

    pub(crate) fn get_delete_edge_query(edge: &GraphEdge) -> QuerySpec {
        let mut params = HashMap::new();
        params.insert(
            "source".to_string(),
            QueryParam::String(edge.source.clone()),
        );
        params.insert(
            "target".to_string(),
            QueryParam::String(edge.target.clone()),
        );
        QuerySpec::with_params(
            format!(
                "MATCH (u:{source_type:?})-[r:{edge_type:?}]->(v:{target_type:?}) WHERE u.metadata.uid = $source AND v.metadata.uid = $target DELETE r",
                source_type = edge.source_type,
                edge_type = edge.edge_type,
                target_type = edge.target_type,
            ),
            params,
        )
    }

    pub(crate) fn get_create_edge_query(edge: &GraphEdge) -> QuerySpec {
        let mut params = HashMap::new();
        params.insert(
            "source".to_string(),
            QueryParam::String(edge.source.clone()),
        );
        params.insert(
            "target".to_string(),
            QueryParam::String(edge.target.clone()),
        );
        QuerySpec::with_params(
            format!(
                "MATCH (u:{source_type:?}), (v:{target_type:?}) WHERE u.metadata.uid = $source AND v.metadata.uid = $target CREATE (u)-[:{edge_type:?}]->(v)",
                source_type = edge.source_type,
                target_type = edge.target_type,
                edge_type = edge.edge_type,
            ),
            params,
        )
    }

    pub(crate) fn get_merge_edge_query(edge: &GraphEdge) -> QuerySpec {
        let mut params = HashMap::new();
        params.insert(
            "source".to_string(),
            QueryParam::String(edge.source.clone()),
        );
        params.insert(
            "target".to_string(),
            QueryParam::String(edge.target.clone()),
        );
        QuerySpec::with_params(
            format!(
                "MATCH (u:{source_type:?} ), (v:{target_type:?}) WHERE u.metadata.uid = $source AND v.metadata.uid = $target MERGE (u)-[:{edge_type:?}]->(v)",
                source_type = edge.source_type,
                target_type = edge.target_type,
                edge_type = edge.edge_type,
            ),
            params,
        )
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
