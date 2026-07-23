use crate::prelude::*;
use crate::state::{ClusterState, ClusterStateDiff, GraphEdge};
use crate::types::{Edge, GenericObject, LOGICAL_RESOURCE_TYPES, ResourceAttributes, ResourceType};
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
    #[error("ValueConversionError: {0}")]
    ValueConversionError(String),
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
        let address = url.strip_prefix("bolt://").unwrap_or(url);
        let (host, port) = address.rsplit_once(':').ok_or_else(|| {
            MemgraphError::ConnectionError(format!(
                "Invalid Memgraph URL {url:?}; expected bolt://host:port"
            ))
        })?;
        if host.is_empty() {
            return Err(MemgraphError::ConnectionError(format!(
                "Invalid Memgraph URL {url:?}; host is empty"
            ))
            .into());
        }
        let port: u16 = port.parse().map_err(|err| {
            MemgraphError::ConnectionError(format!("Failed to parse port from url: {err:?}"))
        })?;

        info!("Connecting to memgraph at {}:{}", host, port);

        let params = ConnectParams {
            port,
            host: Some(host.to_string()),
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
            return Err(
                MemgraphError::ConnectionError(format!("Connection status {status:?}")).into(),
            );
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
        if (status == ConnectionStatus::Bad || status == ConnectionStatus::Closed)
            && let Err(err) = self.reconnect()
        {
            warn!("Failed to reconnect memgraph after bad connection: {err}");
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
                source_type, edge_type, target_type
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
        self.execute_query_with_params(query, None)
    }

    pub fn execute_query_with_params(
        &mut self,
        query: &str,
        params: Option<&HashMap<String, Value>>,
    ) -> Result<Vec<Value>> {
        let (_, rows) = self.execute_query_with_params_and_columns(query, params)?;
        Ok(rows)
    }

    pub fn execute_query_with_params_and_columns(
        &mut self,
        query: &str,
        params: Option<&HashMap<String, Value>>,
    ) -> Result<(Vec<String>, Vec<Value>)> {
        self.ensure_connected()?;
        let query_params = params.map(Self::json_params_to_query_params);
        let cols = self.connection.execute(query, query_params.as_ref());
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
        Ok((cols, result))
    }

    fn json_params_to_query_params(params: &HashMap<String, Value>) -> HashMap<String, QueryParam> {
        let mut mapped = HashMap::new();
        for (key, value) in params {
            mapped.insert(key.clone(), Self::json_to_query_param(value));
        }
        mapped
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
        if columns.len() != value.values.len() {
            return Err(MemgraphError::ValueConversionError(format!(
                "record contains {} values for {} columns",
                value.values.len(),
                columns.len()
            ))
            .into());
        }
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
        rsmgclient::Value::Float(n) => Value::Number(json_number(*n, "float")?),
        rsmgclient::Value::String(s) => Value::String(s.clone()),
        rsmgclient::Value::List(xs) => {
            let mut v = Vec::new();
            for x in xs {
                v.push(record_to_json0(x)?);
            }
            Value::Array(v)
        }
        rsmgclient::Value::Date(d) => Value::String(d.to_string()),
        rsmgclient::Value::LocalTime(time) => Value::String(format_local_time(time)),
        rsmgclient::Value::LocalDateTime(date_time) => {
            Value::String(format_local_date_time(date_time))
        }
        rsmgclient::Value::DateTime(date_time) => date_time_to_json(date_time),
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
        rsmgclient::Value::Point2D(point) => point_2d_to_json(point)?,
        rsmgclient::Value::Point3D(point) => point_3d_to_json(point)?,
    };
    Ok(r)
}

fn json_number(value: f64, field: &str) -> Result<Number> {
    Number::from_f64(value).ok_or_else(|| {
        MemgraphError::ValueConversionError(format!("{field} is not a finite JSON number")).into()
    })
}

fn push_fraction(value: &mut String, nanosecond: u32) {
    if nanosecond != 0 {
        let fraction = format!("{nanosecond:09}");
        value.push('.');
        value.push_str(fraction.trim_end_matches('0'));
    }
}

fn format_year(year: i32) -> String {
    if year < 0 {
        format!("-{:04}", year.unsigned_abs())
    } else if year <= 9999 {
        format!("{year:04}")
    } else {
        format!("+{year}")
    }
}

fn format_local_time(time: &rsmgclient::LocalTime) -> String {
    let mut value = format!(
        "{:02}:{:02}:{:02}",
        time.hour(),
        time.minute(),
        time.second()
    );
    push_fraction(&mut value, time.nanosecond() as u32);
    value
}

fn format_local_date_time(date_time: &rsmgclient::LocalDateTime) -> String {
    let mut value = format!(
        "{}-{:02}-{:02}T{:02}:{:02}:{:02}",
        format_year(i32::from(date_time.year())),
        date_time.month(),
        date_time.day(),
        date_time.hour(),
        date_time.minute(),
        date_time.second()
    );
    push_fraction(&mut value, date_time.nanosecond() as u32);
    value
}

fn format_offset(offset_seconds: i32) -> String {
    let sign = if offset_seconds < 0 { '-' } else { '+' };
    let offset = offset_seconds.unsigned_abs();
    let hours = offset / 3_600;
    let minutes = (offset % 3_600) / 60;
    let seconds = offset % 60;
    if seconds == 0 {
        format!("{sign}{hours:02}:{minutes:02}")
    } else {
        format!("{sign}{hours:02}:{minutes:02}:{seconds:02}")
    }
}

fn date_time_to_json(date_time: &rsmgclient::DateTime) -> Value {
    let mut value = format!(
        "{}-{:02}-{:02}T{:02}:{:02}:{:02}",
        format_year(date_time.year),
        date_time.month,
        date_time.day,
        date_time.hour,
        date_time.minute,
        date_time.second
    );
    push_fraction(&mut value, date_time.nanosecond);
    value.push_str(&format_offset(date_time.time_zone_offset_seconds));

    let mut result = serde_json::Map::new();
    result.insert("type".to_string(), Value::String("datetime".to_string()));
    result.insert("value".to_string(), Value::String(value));
    if let Some(time_zone_id) = &date_time.time_zone_id {
        result.insert(
            "timezone_id".to_string(),
            Value::String(time_zone_id.clone()),
        );
    }
    Value::Object(result)
}

fn point_2d_to_json(point: &rsmgclient::Point2D) -> Result<Value> {
    Ok(Value::Object(point_json_fields(
        point.srid,
        point.x_longitude,
        point.y_latitude,
    )?))
}

fn point_3d_to_json(point: &rsmgclient::Point3D) -> Result<Value> {
    let mut result = point_json_fields(point.srid, point.x_longitude, point.y_latitude)?;
    result.insert(
        "z".to_string(),
        Value::Number(json_number(point.z_height, "point.z")?),
    );
    Ok(Value::Object(result))
}

fn point_json_fields(srid: u16, x: f64, y: f64) -> Result<serde_json::Map<String, Value>> {
    let mut result = serde_json::Map::new();
    result.insert("type".to_string(), Value::String("point".to_string()));
    result.insert("srid".to_string(), Value::Number(srid.into()));
    result.insert("x".to_string(), Value::Number(json_number(x, "point.x")?));
    result.insert("y".to_string(), Value::Number(json_number(y, "point.y")?));
    Ok(result)
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

#[cfg(test)]
mod tests {
    use super::{Memgraph, record_to_json0};
    use rsmgclient::Record;
    use serde_json::json;

    #[test]
    fn converts_memgraph_temporal_values_to_json_strings() {
        let date = rsmgclient::Date::new(2026, 7, 23).unwrap();
        let time = rsmgclient::LocalTime::new(14, 5, 9, 123_000_000).unwrap();
        let date_time = rsmgclient::LocalDateTime::new(2026, 7, 23, 14, 5, 9, 123_000_000).unwrap();
        let offset_date_time = rsmgclient::DateTime {
            year: 2026,
            month: 7,
            day: 23,
            hour: 14,
            minute: 5,
            second: 9,
            nanosecond: 123_000_000,
            time_zone_offset_seconds: 19_800,
            time_zone_id: Some("Asia/Kolkata".to_string()),
        };

        assert_eq!(
            record_to_json0(&rsmgclient::Value::Date(date)).unwrap(),
            json!("2026-07-23")
        );
        assert_eq!(
            record_to_json0(&rsmgclient::Value::LocalTime(time)).unwrap(),
            json!("14:05:09.123")
        );
        assert_eq!(
            record_to_json0(&rsmgclient::Value::LocalDateTime(date_time)).unwrap(),
            json!("2026-07-23T14:05:09.123")
        );
        assert_eq!(
            record_to_json0(&rsmgclient::Value::DateTime(offset_date_time)).unwrap(),
            json!({
                "type": "datetime",
                "value": "2026-07-23T14:05:09.123+05:30",
                "timezone_id": "Asia/Kolkata"
            })
        );
    }

    #[test]
    fn converts_memgraph_points_to_json_objects() {
        let point_2d = rsmgclient::Point2D {
            srid: 4_326,
            x_longitude: 103.851_959,
            y_latitude: 1.290_27,
        };
        let point_3d = rsmgclient::Point3D {
            srid: 4_979,
            x_longitude: 103.851_959,
            y_latitude: 1.290_27,
            z_height: 15.5,
        };

        assert_eq!(
            record_to_json0(&rsmgclient::Value::Point2D(point_2d)).unwrap(),
            json!({
                "type": "point",
                "srid": 4326,
                "x": 103.851959,
                "y": 1.29027
            })
        );
        assert_eq!(
            record_to_json0(&rsmgclient::Value::Point3D(point_3d)).unwrap(),
            json!({
                "type": "point",
                "srid": 4979,
                "x": 103.851959,
                "y": 1.29027,
                "z": 15.5
            })
        );
    }

    #[test]
    fn rejects_non_finite_numbers_and_mismatched_records() {
        assert!(record_to_json0(&rsmgclient::Value::Float(f64::NAN)).is_err());
        assert!(
            record_to_json0(&rsmgclient::Value::Point2D(rsmgclient::Point2D {
                srid: 4_326,
                x_longitude: f64::INFINITY,
                y_latitude: 1.0,
            }))
            .is_err()
        );

        let record = Record {
            values: vec![rsmgclient::Value::Int(1)],
        };
        assert!(
            Memgraph::record_to_json(&["first".to_string(), "second".to_string()], &record)
                .is_err()
        );
    }

    #[test]
    fn rejects_malformed_memgraph_urls_without_panicking() {
        assert!(Memgraph::try_new_from_url("not-a-url").is_err());
        assert!(Memgraph::try_new_from_url("bolt://:7687").is_err());
        assert!(Memgraph::try_new_from_url("bolt://localhost:not-a-port").is_err());
    }

    #[test]
    fn formats_zero_fraction_and_offset_seconds_without_losing_information() {
        let local_time = rsmgclient::LocalTime::new(0, 0, 0, 0).unwrap();
        let offset_date_time = rsmgclient::DateTime {
            year: -1,
            month: 1,
            day: 2,
            hour: 3,
            minute: 4,
            second: 5,
            nanosecond: 1,
            time_zone_offset_seconds: -3_661,
            time_zone_id: None,
        };

        assert_eq!(
            record_to_json0(&rsmgclient::Value::LocalTime(local_time)).unwrap(),
            json!("00:00:00")
        );
        assert_eq!(
            record_to_json0(&rsmgclient::Value::DateTime(offset_date_time)).unwrap(),
            json!({
                "type": "datetime",
                "value": "-0001-01-02T03:04:05.000000001-01:01:01"
            })
        );
    }
}
