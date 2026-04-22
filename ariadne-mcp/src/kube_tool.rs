use crate::APP_VERSION;
#[cfg(feature = "build-info")]
use crate::build::PROJECT_NAME;
use crate::health::{
    GraphHealthCompactResponse, GraphHealthDetail, GraphHealthResponse, GraphScope, RebuildHealth,
    SharedCoverage, SyncHealth, coverage_response, format_timestamp, now, rebuild_response,
    sync_response,
};
use ariadne_core::cypher_validation::{
    parse_cypher, validate_cypher, validate_read_only_query, validate_read_only_text,
    validate_schema_query,
};
use ariadne_core::graph_backend::GraphBackend;
use ariadne_core::query_issue::{QueryIssue, classify_ariadne_error};
use ariadne_core::state::SharedClusterState;
use ariadne_tools::{generate_schema, graph_relationships};
use rmcp::handler::server::{router::tool::ToolRouter, wrapper::Parameters};
use rmcp::model::{
    CallToolResult, Implementation, InitializeRequestParams, InitializeResult, ProtocolVersion,
};
use rmcp::service::RequestContext;
use rmcp::{
    ErrorData, RoleServer, ServerHandler,
    model::{ServerCapabilities, ServerInfo},
    schemars, tool, tool_handler, tool_router,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Instant, SystemTime};

#[cfg(not(feature = "build-info"))]
const PROJECT_NAME: &str = env!("CARGO_PKG_NAME");

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct GraphQueryRequest {
    pub query: String,
    #[serde(default)]
    pub params: Option<HashMap<String, serde_json::Value>>,
    #[serde(default)]
    pub limit: Option<u64>,
}

#[derive(
    Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum GraphSchemaFormat {
    #[default]
    Compact,
    Structured,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema, Default)]
pub struct GraphSchemaRequest {
    #[serde(default)]
    pub format: GraphSchemaFormat,
}

#[derive(
    Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum GraphHealthRequestDetail {
    #[default]
    Compact,
    Full,
    Debug,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema, Default)]
pub struct GraphHealthRequest {
    #[serde(default)]
    pub detail: GraphHealthRequestDetail,
}

#[derive(Debug, Clone)]
pub struct KubeTool {
    cluster_name: String,
    backend_kind: String,
    mode: String,
    scope: Option<GraphScope>,
    snapshot_captured_at: Option<String>,
    cluster_state: SharedClusterState,
    graph: Arc<dyn GraphBackend>,
    initial_load_succeeded: Arc<AtomicBool>,
    source_sync: Arc<Mutex<SyncHealth>>,
    rebuild: Arc<Mutex<Option<RebuildHealth>>>,
    coverage: SharedCoverage,
    tool_router: ToolRouter<Self>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct GraphQueryResponse {
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
    row_count: usize,
    truncated: bool,
    duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct SchemaProperty {
    name: String,
    #[serde(rename = "type")]
    property_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct NodeLabelSchema {
    label: String,
    properties: Vec<SchemaProperty>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GraphSchemaResponse {
    format: GraphSchemaFormat,
    schema_version: String,
    server_version: String,
    node_labels: Vec<NodeLabelSchema>,
    relationship_types: Vec<ariadne_core::graph_schema::GraphRelationship>,
    example_patterns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GraphSchemaCompactResponse {
    format: GraphSchemaFormat,
    schema_version: String,
    schema_text: String,
    example_patterns: Vec<String>,
}

#[tool_router]
impl KubeTool {
    #[allow(clippy::too_many_arguments)]
    pub fn new_tool(
        cluster_name: String,
        backend_kind: String,
        mode: String,
        scope: Option<GraphScope>,
        snapshot_captured_at: Option<String>,
        cluster_state: SharedClusterState,
        graph: Arc<dyn GraphBackend>,
        initial_load_succeeded: Arc<AtomicBool>,
        source_sync: Arc<Mutex<SyncHealth>>,
        rebuild: Arc<Mutex<Option<RebuildHealth>>>,
        coverage: SharedCoverage,
    ) -> Self {
        Self {
            cluster_name,
            backend_kind,
            mode,
            scope,
            snapshot_captured_at,
            cluster_state,
            graph,
            initial_load_succeeded,
            source_sync,
            rebuild,
            coverage,
            tool_router: Self::tool_router(),
        }
    }

    #[tool(
        name = "graph_query",
        description = "Execute a read-only Cypher query against the Kubernetes graph. Returns rows, columns, row count, truncation status, and execution time. The optional top-level `limit` caps response size after execution (default: 100, max: 1000) for transport/context safety only. Prefer narrow queries and add Cypher `LIMIT` when exploring large result sets. Errors include structured classification with `kind` and `repairable`/`retryable` flags. Use `graph_schema` only when labels, properties, or traversal directions are uncertain."
    )]
    async fn graph_query(
        &self,
        Parameters(GraphQueryRequest {
            query,
            params,
            limit,
        }): Parameters<GraphQueryRequest>,
    ) -> Result<CallToolResult, ErrorData> {
        tracing::info!(cypher = %query, "graph_query");
        validate_graph_query_for_backend(&self.backend_kind, &query).map_err(|issue| {
            tracing::error!(cypher = %query, error = %issue, "graph_query validation failed");
            query_issue_to_mcp(issue, query.clone())
        })?;

        let response_limit = validate_response_limit(limit)?;
        let started = Instant::now();
        let (mut columns, raw_rows) = self
            .graph
            .execute_query_with_columns(query.clone(), params)
            .await
            .map_err(|err| {
                let issue = classify_ariadne_error(&err);
                tracing::error!(
                    cypher = %query,
                    error = %err,
                    issue_kind = issue.kind_code(),
                    "graph_query failed"
                );
                query_issue_to_mcp(issue, query.clone())
            })?;
        let duration_ms = started.elapsed().as_millis() as u64;

        if columns.is_empty() && raw_rows.iter().any(|row| !row.is_object()) {
            columns.push("value".to_string());
        }

        let rows = to_columnar_rows(&columns, &raw_rows);
        let truncated = rows.len() > response_limit;
        let rows: Vec<Vec<Value>> = rows.into_iter().take(response_limit).collect();
        structured_response(GraphQueryResponse {
            row_count: rows.len(),
            columns,
            rows,
            truncated,
            duration_ms,
        })
    }

    #[tool(
        name = "graph_schema",
        description = "Return the Kubernetes graph schema. By default this returns a compact text view optimized for model consumption. Pass `format = \"structured\"` for the full machine-readable schema with node labels, properties, and relationship types."
    )]
    async fn graph_schema(
        &self,
        Parameters(GraphSchemaRequest { format }): Parameters<GraphSchemaRequest>,
    ) -> Result<CallToolResult, ErrorData> {
        match format {
            GraphSchemaFormat::Compact => structured_response(compact_schema_response().clone()),
            GraphSchemaFormat::Structured => {
                structured_response(structured_schema_response().clone())
            }
        }
    }

    #[tool(
        name = "graph_health",
        description = "Return Kubernetes graph health. By default this returns a compact freshness/status summary optimized for model consumption. Pass `detail = \"full\"` or `detail = \"debug\"` for the full diagnostic payload with backend probe details, sync and rebuild state, and version."
    )]
    async fn graph_health(
        &self,
        Parameters(GraphHealthRequest { detail }): Parameters<GraphHealthRequest>,
    ) -> Result<CallToolResult, ErrorData> {
        let observed_at = now();
        let (backend_probe_ok, backend_probe_duration_ms) = self.probe_backend().await;
        let sync = self
            .source_sync
            .lock()
            .expect("source_sync lock poisoned")
            .clone();
        let rebuild = self.rebuild.lock().expect("rebuild lock poisoned").clone();
        let ready = if self.mode == "snapshot" {
            self.initial_load_succeeded.load(Ordering::Relaxed) && backend_probe_ok
        } else {
            sync.last_success_at.is_some() && backend_probe_ok
        };

        let (node_count, edge_count) = {
            let state = self
                .cluster_state
                .lock()
                .expect("cluster_state lock poisoned");
            (state.get_node_count(), state.get_edge_count())
        };

        let data_as_of = if self.mode == "snapshot" {
            self.snapshot_captured_at.clone()
        } else {
            max_timestamp(
                sync.last_success_at,
                rebuild.as_ref().and_then(|state| state.last_success_at),
            )
            .map(format_timestamp)
        };

        let lag_ms = (self.mode == "live")
            .then(|| crate::health::lag_ms(observed_at, sync.last_success_at))
            .flatten();
        let coverage = coverage_response(&self.coverage);

        match detail {
            GraphHealthRequestDetail::Compact => structured_response(GraphHealthCompactResponse {
                detail: GraphHealthDetail::Compact,
                cluster: self.cluster_name.clone(),
                mode: self.mode.clone(),
                scope: self.scope.clone(),
                observed_at: format_timestamp(observed_at),
                ready,
                data_as_of,
                node_count,
                edge_count,
                sync_lag_ms: lag_ms,
                coverage,
            }),
            GraphHealthRequestDetail::Full => structured_response(GraphHealthResponse {
                detail: GraphHealthDetail::Full,
                cluster: self.cluster_name.clone(),
                backend: self.backend_kind.clone(),
                mode: self.mode.clone(),
                scope: self.scope.clone(),
                observed_at: format_timestamp(observed_at),
                ready,
                backend_probe_ok,
                backend_probe_duration_ms,
                data_as_of,
                node_count,
                edge_count,
                version: APP_VERSION.to_string(),
                sync: (self.mode == "live").then(|| sync_response(&sync, observed_at)),
                rebuild: rebuild.as_ref().map(rebuild_response),
                coverage,
            }),
            GraphHealthRequestDetail::Debug => structured_response(GraphHealthResponse {
                detail: GraphHealthDetail::Debug,
                cluster: self.cluster_name.clone(),
                backend: self.backend_kind.clone(),
                mode: self.mode.clone(),
                scope: self.scope.clone(),
                observed_at: format_timestamp(observed_at),
                ready,
                backend_probe_ok,
                backend_probe_duration_ms,
                data_as_of,
                node_count,
                edge_count,
                version: APP_VERSION.to_string(),
                sync: (self.mode == "live").then(|| sync_response(&sync, observed_at)),
                rebuild: rebuild.as_ref().map(rebuild_response),
                coverage,
            }),
        }
    }

    async fn probe_backend(&self) -> (bool, u64) {
        if self.backend_kind == "in-memory" {
            return (true, 0);
        }

        let started = Instant::now();
        let probe_result = self
            .graph
            .execute_query("RETURN 1 AS ok".to_string(), None)
            .await;
        let duration_ms = started.elapsed().as_millis() as u64;
        if let Err(err) = &probe_result {
            tracing::warn!(error = %err, "graph_health backend probe failed");
        }
        (probe_result.is_ok(), duration_ms)
    }
}

fn structured_response<T>(value: T) -> Result<CallToolResult, ErrorData>
where
    T: Serialize,
{
    let value = serde_json::to_value(value)
        .map_err(|err| ErrorData::internal_error(err.to_string(), None))?;
    Ok(CallToolResult::structured(value))
}

fn validate_response_limit(limit: Option<u64>) -> Result<usize, ErrorData> {
    match limit {
        None => Ok(100),
        Some(value @ 1..=1000) => Ok(value as usize),
        Some(_) => Err(ErrorData::invalid_params(
            "limit must be between 1 and 1000",
            None,
        )),
    }
}

fn to_columnar_rows(columns: &[String], rows: &[Value]) -> Vec<Vec<Value>> {
    rows.iter()
        .map(|row| match row.as_object() {
            Some(map) => columns
                .iter()
                .map(|column| map.get(column).cloned().unwrap_or(Value::Null))
                .collect(),
            None if columns.len() == 1 => vec![row.clone()],
            None => columns.iter().map(|_| Value::Null).collect(),
        })
        .collect()
}

fn query_issue_to_mcp(issue: QueryIssue, cypher: String) -> ErrorData {
    let data = Some(json!({
        "kind": issue.kind_code(),
        "retryable": issue.retryable(),
        "repairable": issue.repairable(),
        "source": issue.source_code(),
        "cypher": cypher,
    }));
    if issue.invalid_params() {
        ErrorData::invalid_params(issue.to_string(), data)
    } else {
        ErrorData::internal_error(issue.to_string(), data)
    }
}

fn validate_graph_query_for_backend(backend_kind: &str, cypher: &str) -> Result<(), QueryIssue> {
    if backend_kind == "in-memory" {
        return validate_cypher(cypher);
    }

    match parse_cypher(cypher) {
        Ok(query) => {
            validate_read_only_query(&query)?;
            validate_schema_query(&query)
        }
        Err(parse_issue) => {
            validate_read_only_text(cypher)?;
            tracing::warn!(
                backend_kind,
                cypher = %cypher,
                parse_error = %parse_issue,
                "graph_query skipping parser-specific validation for backend"
            );
            Ok(())
        }
    }
}

fn max_timestamp(left: Option<SystemTime>, right: Option<SystemTime>) -> Option<SystemTime> {
    match (left, right) {
        (Some(left), Some(right)) => {
            if left >= right {
                Some(left)
            } else {
                Some(right)
            }
        }
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

fn structured_schema_response() -> &'static GraphSchemaResponse {
    static SCHEMA_CACHE: OnceLock<GraphSchemaResponse> = OnceLock::new();
    SCHEMA_CACHE.get_or_init(build_schema_response)
}

fn compact_schema_response() -> &'static GraphSchemaCompactResponse {
    static SCHEMA_CACHE: OnceLock<GraphSchemaCompactResponse> = OnceLock::new();
    SCHEMA_CACHE.get_or_init(build_compact_schema_response)
}

fn build_schema_response() -> GraphSchemaResponse {
    let mut node_labels: Vec<NodeLabelSchema> = generate_schema()
        .into_iter()
        .map(|schema| {
            let label = schema.root_type.name;
            let properties = schema
                .root_type
                .properties
                .into_iter()
                .filter(|property| !is_stripped_property(&label, &property.name))
                .map(|property| SchemaProperty {
                    name: property.name,
                    property_type: normalize_schema_type(&property.data_type),
                })
                .collect();
            NodeLabelSchema { label, properties }
        })
        .collect();
    node_labels.sort_by(|left, right| left.label.cmp(&right.label));

    let available_labels: BTreeSet<String> = node_labels
        .iter()
        .map(|label| label.label.clone())
        .collect();
    let relationship_types = graph_relationships()
        .into_iter()
        .filter(|relationship| {
            available_labels.contains(&relationship.from)
                && available_labels.contains(&relationship.to)
        })
        .collect::<Vec<_>>();
    let schema_version = compute_schema_version(&node_labels, &relationship_types);

    GraphSchemaResponse {
        format: GraphSchemaFormat::Structured,
        schema_version,
        server_version: APP_VERSION.to_string(),
        node_labels,
        relationship_types,
        example_patterns: vec![
            "MATCH (d:Deployment)-[:Manages]->(rs:ReplicaSet)-[:Manages]->(p:Pod) WHERE d['metadata']['name'] = $name RETURN p['metadata']['name'] AS pod_name LIMIT 25".to_string(),
            "MATCH (h:Host)-[:IsClaimedBy]->(i:Ingress)-[:DefinesBackend]->(b:IngressServiceBackend)-[:TargetsService]->(s:Service) WHERE h['name'] = $hostname RETURN s['metadata']['name'] AS service_name, s['metadata']['namespace'] AS service_namespace LIMIT 25".to_string(),
            "MATCH (p:Pod)-[:RunsOn]->(n:Node) WHERE p['metadata']['namespace'] = $ns RETURN n['metadata']['name'] AS node_name, count(p) AS pod_count ORDER BY pod_count DESC LIMIT 25".to_string(),
            "MATCH (svc:Service)-[:Manages]->(es:EndpointSlice)-[:ContainsEndpoint]->(ep:Endpoint)-[:HasAddress]->(ea:EndpointAddress)-[:IsAddressOf]->(p:Pod) WHERE svc['metadata']['name'] = $svc AND svc['metadata']['namespace'] = $ns RETURN p['metadata']['name'] AS pod_name LIMIT 25".to_string(),
            "MATCH (ns:Namespace) OPTIONAL MATCH (p:Pod)-[:BelongsTo]->(ns) RETURN ns['metadata']['name'] AS namespace, count(p) AS pods ORDER BY pods DESC LIMIT 25".to_string(),
        ],
    }
}

fn build_compact_schema_response() -> GraphSchemaCompactResponse {
    let structured = structured_schema_response();
    GraphSchemaCompactResponse {
        format: GraphSchemaFormat::Compact,
        schema_version: structured.schema_version.clone(),
        schema_text: build_compact_schema_text(
            &structured.node_labels,
            &structured.relationship_types,
        ),
        example_patterns: structured.example_patterns.clone(),
    }
}

fn build_compact_schema_text(
    node_labels: &[NodeLabelSchema],
    relationship_types: &[ariadne_core::graph_schema::GraphRelationship],
) -> String {
    let mut lines = vec!["# Nodes".to_string()];
    let (logical_labels, native_labels): (Vec<_>, Vec<_>) = node_labels
        .iter()
        .partition(|label| is_logical_node_label(&label.label));
    push_compact_node_group(&mut lines, "## Logical Nodes", &logical_labels);
    lines.push(String::new());
    push_compact_node_group(&mut lines, "## K8s Native Nodes", &native_labels);

    lines.push(String::new());
    lines.push("# Edges".to_string());
    lines.push("Each bracket item expands to an independent directed edge.".to_string());

    let mut grouped_edges: BTreeMap<(String, String), BTreeSet<String>> = BTreeMap::new();
    for relationship in relationship_types {
        grouped_edges
            .entry((relationship.from.clone(), relationship.edge.clone()))
            .or_default()
            .insert(relationship.to.clone());
    }

    for ((from, edge), targets) in grouped_edges {
        let targets = targets.into_iter().collect::<Vec<_>>();
        if targets.len() == 1 {
            lines.push(format!("{from}-[{edge}]->{}", targets[0]));
        } else {
            lines.push(format!("{from}-[{edge}]->[{}]", targets.join(", ")));
        }
    }

    lines.join("\n")
}

fn push_compact_node_group(
    lines: &mut Vec<String>,
    heading: &str,
    node_labels: &[&NodeLabelSchema],
) {
    if node_labels.is_empty() {
        return;
    }
    lines.push(heading.to_string());
    for label in node_labels {
        let properties = label
            .properties
            .iter()
            .filter(|property| !is_low_value_compact_property(&property.name))
            .map(|property| format!("{}:{}", property.name, property.property_type))
            .collect::<Vec<_>>()
            .join(", ");
        if properties.is_empty() {
            lines.push(label.label.clone());
        } else {
            lines.push(format!("{}({})", label.label, properties));
        }
    }
}

fn compute_schema_version(
    node_labels: &[NodeLabelSchema],
    relationship_types: &[ariadne_core::graph_schema::GraphRelationship],
) -> String {
    let mut canonical_node_labels = node_labels.to_vec();
    canonical_node_labels.sort_by(|left, right| left.label.cmp(&right.label));
    for node_label in &mut canonical_node_labels {
        node_label.properties.sort_by(|left, right| {
            left.name
                .cmp(&right.name)
                .then_with(|| left.property_type.cmp(&right.property_type))
        });
    }

    let mut canonical_relationship_types = relationship_types.to_vec();
    canonical_relationship_types.sort_by(|left, right| {
        left.from
            .cmp(&right.from)
            .then_with(|| left.edge.cmp(&right.edge))
            .then_with(|| left.to.cmp(&right.to))
    });

    let canonical = serde_json::to_vec(&json!({
        "node_labels": canonical_node_labels,
        "relationship_types": canonical_relationship_types,
    }))
    .expect("schema serialization should succeed");
    let digest = Sha256::digest(canonical);
    let hex = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("sha256:{}", &hex[..12])
}

fn is_stripped_property(label: &str, property_name: &str) -> bool {
    matches!(
        (label, property_name),
        ("ConfigMap", "data") | ("ConfigMap", "binaryData")
    )
}

fn is_low_value_compact_property(property_name: &str) -> bool {
    matches!(property_name, "apiVersion" | "kind")
}

fn is_logical_node_label(label: &str) -> bool {
    matches!(
        label,
        "Cluster"
            | "Container"
            | "Endpoint"
            | "EndpointAddress"
            | "Host"
            | "IngressServiceBackend"
            | "Provisioner"
    )
}

fn normalize_schema_type(data_type: &str) -> String {
    if let Some(inner) = data_type
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
    {
        return format!("[{}]", normalize_schema_type(inner));
    }

    match data_type {
        "#/$defs/io.k8s.apimachinery.pkg.apis.meta.v1.Time"
        | "#/$defs/io.k8s.apimachinery.pkg.apis.meta.v1.MicroTime"
        | "#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.Time"
        | "#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.MicroTime" => "datetime".to_string(),
        value if value.starts_with("#/$defs/") => value.trim_start_matches("#/$defs/").to_string(),
        value if value.starts_with("#/definitions/") => {
            value.trim_start_matches("#/definitions/").to_string()
        }
        value => value.to_string(),
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for KubeTool {
    async fn initialize(
        &self,
        _request: InitializeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<InitializeResult, ErrorData> {
        if let Some(http_request_part) = context.extensions.get::<axum::http::request::Parts>() {
            let initialize_headers = &http_request_part.headers;
            let initialize_uri = &http_request_part.uri;
            tracing::info!(?initialize_headers, %initialize_uri, "initialize from http server");
        }
        Ok(self.get_info())
    }

    fn get_info(&self) -> ServerInfo {
        let instruction = format!(
            "Read-only MCP server for Kubernetes cluster {}.\nThree tools: graph_query, graph_schema, graph_health.\nUse graph_query directly when the query shape is already known. graph_schema defaults to a compact text schema; request format=structured only when full machine-readable details are needed. graph_health defaults to a compact freshness/status summary; request detail=full or detail=debug only when the extra diagnostics matter.\nAll queries are read-only Cypher. Prefer LIMIT in exploratory queries. Use parameterized queries ($var) when filtering. Alias non-trivial RETURN expressions with AS to keep result columns unique and stable.\nErrors include structured classification; check `repairable` to decide whether to fix and retry.",
            self.cluster_name
        );
        let capabilities = ServerCapabilities::builder().enable_tools().build();
        let server_info = Implementation::new(PROJECT_NAME, APP_VERSION)
            .with_title(PROJECT_NAME)
            .with_description(format!(
                "Read-only MCP server exposing a Cypher graph view of Kubernetes cluster {}",
                self.cluster_name
            ));
        ServerInfo::new(capabilities)
            .with_protocol_version(ProtocolVersion::V_2025_03_26)
            .with_server_info(server_info)
            .with_instructions(instruction)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health::GraphScopeKind;
    use ariadne_core::query_issue::QueryIssueKind;
    use ariadne_core::state::ClusterState;
    use ariadne_core::types::{Cluster, ObjectIdentifier};
    use k8s_openapi::apimachinery::pkg::version::Info;
    use std::time::Duration;

    #[derive(Debug, Default)]
    struct MockBackend {
        columns: Vec<String>,
        rows: Vec<Value>,
        fail: Option<String>,
    }

    #[async_trait::async_trait]
    impl GraphBackend for MockBackend {
        async fn create(
            &self,
            _cluster_state: SharedClusterState,
        ) -> ariadne_core::prelude::Result<()> {
            Ok(())
        }

        async fn update(
            &self,
            _diff: ariadne_core::state::ClusterStateDiff,
        ) -> ariadne_core::prelude::Result<()> {
            Ok(())
        }

        async fn execute_query(
            &self,
            _query: String,
            _params: Option<HashMap<String, Value>>,
        ) -> ariadne_core::prelude::Result<Vec<Value>> {
            if let Some(message) = &self.fail {
                return Err(std::io::Error::other(message.clone()).into());
            }
            Ok(self.rows.clone())
        }

        async fn execute_query_with_columns(
            &self,
            _query: String,
            _params: Option<HashMap<String, Value>>,
        ) -> ariadne_core::prelude::Result<(Vec<String>, Vec<Value>)> {
            if let Some(message) = &self.fail {
                return Err(std::io::Error::other(message.clone()).into());
            }
            Ok((self.columns.clone(), self.rows.clone()))
        }

        async fn shutdown(&self) {}
    }

    fn test_cluster() -> Cluster {
        Cluster::new(
            ObjectIdentifier {
                uid: "Cluster:test".to_string(),
                name: "test".to_string(),
                namespace: None,
                resource_version: None,
            },
            "https://example.invalid",
            Info::default(),
        )
    }

    fn test_state() -> SharedClusterState {
        Arc::new(Mutex::new(ClusterState::new(test_cluster())))
    }

    #[allow(clippy::too_many_arguments)]
    fn test_tool_with(
        graph: Arc<dyn GraphBackend>,
        backend_kind: &str,
        mode: &str,
        scope: Option<GraphScope>,
        snapshot_captured_at: Option<&str>,
        initial_load_succeeded: bool,
        sync: SyncHealth,
        rebuild: Option<RebuildHealth>,
        coverage: &[&str],
    ) -> KubeTool {
        KubeTool::new_tool(
            "test-cluster".to_string(),
            backend_kind.to_string(),
            mode.to_string(),
            scope,
            snapshot_captured_at.map(ToOwned::to_owned),
            test_state(),
            graph,
            Arc::new(AtomicBool::new(initial_load_succeeded)),
            Arc::new(Mutex::new(sync)),
            Arc::new(Mutex::new(rebuild)),
            Arc::new(Mutex::new(
                coverage
                    .iter()
                    .map(|kind| kind.to_string())
                    .collect::<std::collections::BTreeSet<String>>(),
            )),
        )
    }

    fn test_tool(graph: Arc<dyn GraphBackend>) -> KubeTool {
        let mut sync = SyncHealth::bootstrap(SystemTime::UNIX_EPOCH);
        sync.poll_interval_seconds = 5;
        test_tool_with(
            graph,
            "in-memory",
            "live",
            Some(GraphScope::namespace("kube-system")),
            None,
            true,
            sync,
            Some(RebuildHealth {
                loop_alive: true,
                poll_interval_seconds: 30,
                ..Default::default()
            }),
            &["Node"],
        )
    }

    #[tokio::test]
    async fn graph_query_returns_structured_rows_and_truncation() {
        let graph = Arc::new(MockBackend {
            columns: vec!["pod".to_string(), "namespace".to_string()],
            rows: vec![
                json!({"pod": "a", "namespace": "default"}),
                json!({"pod": "b", "namespace": "kube-system"}),
            ],
            fail: None,
        });
        let tool = test_tool(graph);

        let result = tool
            .graph_query(Parameters(GraphQueryRequest {
                query: "MATCH (p:Pod) RETURN p['metadata']['name'] AS pod, p['metadata']['namespace'] AS namespace".to_string(),
                params: None,
                limit: Some(1),
            }))
            .await
            .expect("graph_query should succeed");
        let payload: GraphQueryResponse =
            serde_json::from_value(result.structured_content.expect("structured response"))
                .expect("query payload");

        assert_eq!(payload.columns, vec!["pod", "namespace"]);
        assert_eq!(payload.rows, vec![vec![json!("a"), json!("default")]]);
        assert_eq!(payload.row_count, 1);
        assert!(payload.truncated);
    }

    #[tokio::test]
    async fn graph_query_in_memory_rejects_parser_unsupported_query() {
        let graph = Arc::new(MockBackend {
            columns: vec!["service_name".to_string()],
            rows: vec![json!({"service_name": "kubernetes"})],
            fail: None,
        });
        let tool = test_tool(graph);

        let err = tool
            .graph_query(Parameters(GraphQueryRequest {
                query: "MATCH (s:Service)-[:BelongsTo]->(ns:Namespace) WHERE ns['metadata']['name'] = 'default' AND NOT EXISTS { MATCH (s)-[:Manages]->(:EndpointSlice) } RETURN s['metadata']['name'] AS service_name".to_string(),
                params: None,
                limit: None,
            }))
            .await
            .expect_err("in-memory backend should still reject parser-unsupported syntax");

        assert_eq!(err.code, rmcp::model::ErrorCode::INVALID_PARAMS);
        let data = err.data.expect("data");
        assert_eq!(data["kind"], "parse_error");
        assert_eq!(data["source"], "validator");
    }

    #[tokio::test]
    async fn graph_query_memgraph_allows_parser_unsupported_read_only_query() {
        let graph = Arc::new(MockBackend {
            columns: vec!["service_name".to_string()],
            rows: vec![json!({"service_name": "kubernetes"})],
            fail: None,
        });
        let mut sync = SyncHealth::bootstrap(SystemTime::UNIX_EPOCH);
        sync.poll_interval_seconds = 5;
        let tool = test_tool_with(
            graph,
            "memgraph",
            "snapshot",
            Some(GraphScope::cluster()),
            Some("2026-04-08T00:00:00Z"),
            true,
            sync,
            None,
            &[],
        );

        let result = tool
            .graph_query(Parameters(GraphQueryRequest {
                query: "MATCH (s:Service)-[:BelongsTo]->(ns:Namespace) WHERE ns['metadata']['name'] = 'default' AND NOT EXISTS { MATCH (s)-[:Manages]->(:EndpointSlice) } RETURN s['metadata']['name'] AS service_name".to_string(),
                params: None,
                limit: None,
            }))
            .await
            .expect("memgraph backend should allow parser-divergent read-only syntax");
        let payload: GraphQueryResponse =
            serde_json::from_value(result.structured_content.expect("structured response"))
                .expect("query payload");

        assert_eq!(payload.columns, vec!["service_name"]);
        assert_eq!(payload.rows, vec![vec![json!("kubernetes")]]);
    }

    #[tokio::test]
    async fn graph_query_memgraph_still_rejects_schema_errors() {
        let graph = Arc::new(MockBackend::default());
        let mut sync = SyncHealth::bootstrap(SystemTime::UNIX_EPOCH);
        sync.poll_interval_seconds = 5;
        let tool = test_tool_with(
            graph,
            "memgraph",
            "snapshot",
            Some(GraphScope::cluster()),
            Some("2026-04-08T00:00:00Z"),
            true,
            sync,
            None,
            &[],
        );

        let err = tool
            .graph_query(Parameters(GraphQueryRequest {
                query: "MATCH (p:Pod)-[:BelongsTo]->(c:Cluster) RETURN p".to_string(),
                params: None,
                limit: None,
            }))
            .await
            .expect_err("schema mismatch should still be rejected");

        assert_eq!(err.code, rmcp::model::ErrorCode::INVALID_PARAMS);
        let data = err.data.expect("data");
        assert_eq!(data["kind"], "schema_error");
        assert_eq!(data["source"], "validator");
    }

    #[test]
    fn memgraph_fallback_still_blocks_non_read_only_statement() {
        let issue = validate_graph_query_for_backend("memgraph", "CREATE INDEX ON :Pod(name)")
            .expect_err("fallback guard should reject CREATE INDEX");

        assert_eq!(issue.kind, QueryIssueKind::Semantic);
        assert_eq!(issue.source_code(), "validator");
    }

    #[tokio::test]
    async fn graph_schema_defaults_to_compact_text() {
        let tool = test_tool(Arc::new(MockBackend::default()));
        let result = tool
            .graph_schema(Parameters(GraphSchemaRequest::default()))
            .await
            .expect("graph_schema should succeed");
        let payload: GraphSchemaCompactResponse =
            serde_json::from_value(result.structured_content.expect("structured response"))
                .expect("compact schema payload");

        assert_eq!(payload.format, GraphSchemaFormat::Compact);
        assert!(payload.schema_version.starts_with("sha256:"));
        assert!(payload.schema_text.starts_with("# Nodes\n"));
        assert!(payload.schema_text.contains("\n## Logical Nodes\n"));
        assert!(payload.schema_text.contains("\n## K8s Native Nodes\n"));
        assert!(payload.schema_text.contains("\n# Edges\n"));
        assert!(payload.schema_text.contains("Namespace("));
        assert!(payload.schema_text.contains("PersistentVolumeClaim("));
        assert!(
            payload
                .schema_text
                .contains("Container(container_type:string")
        );
        assert!(
            payload
                .schema_text
                .contains("spec:io.k8s.api.core.v1.Container")
        );
        assert!(
            payload
                .schema_text
                .contains("spec:io.k8s.api.core.v1.PodSpec")
        );
        assert!(payload.schema_text.contains("Pod-[BelongsTo]->Namespace"));
        assert!(
            payload
                .schema_text
                .contains("Deployment-[Manages]->[DaemonSet, Deployment, ReplicaSet]")
        );
        assert!(!payload.schema_text.contains("AWX-["));
        assert!(!payload.schema_text.contains("ConfigMap(data:"));
        assert!(!payload.schema_text.contains("binaryData:"));
        assert!(!payload.schema_text.contains("apiVersion:"));
        assert!(!payload.schema_text.contains("kind:"));
        assert!(
            payload
                .schema_text
                .contains("imagePullSecrets:[io.k8s.api.core.v1.LocalObjectReference]")
        );
        assert!(
            payload
                .example_patterns
                .iter()
                .all(|pattern| pattern.contains("LIMIT"))
        );
    }

    #[tokio::test]
    async fn graph_schema_structured_mode_filters_stripped_configmap_fields() {
        let tool = test_tool(Arc::new(MockBackend::default()));
        let result = tool
            .graph_schema(Parameters(GraphSchemaRequest {
                format: GraphSchemaFormat::Structured,
            }))
            .await
            .expect("graph_schema should succeed");
        let payload: GraphSchemaResponse =
            serde_json::from_value(result.structured_content.expect("structured response"))
                .expect("schema payload");

        assert_eq!(payload.format, GraphSchemaFormat::Structured);
        assert!(payload.schema_version.starts_with("sha256:"));
        assert!(
            payload
                .example_patterns
                .iter()
                .all(|pattern| pattern.contains("LIMIT"))
        );
        assert!(
            payload
                .example_patterns
                .iter()
                .all(|pattern| pattern.contains(" AS "))
        );

        let config_map = payload
            .node_labels
            .iter()
            .find(|label| label.label == "ConfigMap")
            .expect("ConfigMap label should exist");
        assert!(
            !config_map
                .properties
                .iter()
                .any(|property| property.name == "data" || property.name == "binaryData")
        );
        let pod = payload
            .node_labels
            .iter()
            .find(|label| label.label == "Pod")
            .expect("Pod label should exist");
        assert!(pod.properties.iter().any(|property| {
            property.name == "spec" && property.property_type == "io.k8s.api.core.v1.PodSpec"
        }));
        let container = payload
            .node_labels
            .iter()
            .find(|label| label.label == "Container")
            .expect("Container label should exist");
        assert!(container.properties.iter().any(|property| {
            property.name == "spec" && property.property_type == "io.k8s.api.core.v1.Container"
        }));
        let service_account = payload
            .node_labels
            .iter()
            .find(|label| label.label == "ServiceAccount")
            .expect("ServiceAccount label should exist");
        assert!(service_account.properties.iter().any(|property| {
            property.name == "imagePullSecrets"
                && property.property_type == "[io.k8s.api.core.v1.LocalObjectReference]"
        }));
    }

    #[test]
    fn schema_version_is_order_invariant() {
        let left = vec![
            NodeLabelSchema {
                label: "Pod".to_string(),
                properties: vec![
                    SchemaProperty {
                        name: "status".to_string(),
                        property_type: "object".to_string(),
                    },
                    SchemaProperty {
                        name: "metadata".to_string(),
                        property_type: "ObjectMeta".to_string(),
                    },
                ],
            },
            NodeLabelSchema {
                label: "Namespace".to_string(),
                properties: vec![SchemaProperty {
                    name: "metadata".to_string(),
                    property_type: "ObjectMeta".to_string(),
                }],
            },
        ];
        let right = vec![
            NodeLabelSchema {
                label: "Namespace".to_string(),
                properties: vec![SchemaProperty {
                    name: "metadata".to_string(),
                    property_type: "ObjectMeta".to_string(),
                }],
            },
            NodeLabelSchema {
                label: "Pod".to_string(),
                properties: vec![
                    SchemaProperty {
                        name: "metadata".to_string(),
                        property_type: "ObjectMeta".to_string(),
                    },
                    SchemaProperty {
                        name: "status".to_string(),
                        property_type: "object".to_string(),
                    },
                ],
            },
        ];

        let relationships_a = vec![
            ariadne_core::graph_schema::GraphRelationship {
                from: "Pod".to_string(),
                edge: "BelongsTo".to_string(),
                to: "Namespace".to_string(),
            },
            ariadne_core::graph_schema::GraphRelationship {
                from: "Pod".to_string(),
                edge: "PartOf".to_string(),
                to: "Cluster".to_string(),
            },
        ];
        let relationships_b = vec![
            ariadne_core::graph_schema::GraphRelationship {
                from: "Pod".to_string(),
                edge: "PartOf".to_string(),
                to: "Cluster".to_string(),
            },
            ariadne_core::graph_schema::GraphRelationship {
                from: "Pod".to_string(),
                edge: "BelongsTo".to_string(),
                to: "Namespace".to_string(),
            },
        ];

        assert_eq!(
            compute_schema_version(&left, &relationships_a),
            compute_schema_version(&right, &relationships_b)
        );
    }

    #[tokio::test]
    async fn graph_health_defaults_to_compact_summary() {
        let tool = test_tool(Arc::new(MockBackend::default()));
        let result = tool
            .graph_health(Parameters(GraphHealthRequest::default()))
            .await
            .expect("graph_health should succeed");
        let payload: GraphHealthCompactResponse =
            serde_json::from_value(result.structured_content.expect("structured response"))
                .expect("health payload");

        assert_eq!(payload.detail, GraphHealthDetail::Compact);
        assert_eq!(payload.cluster, "test-cluster");
        assert_eq!(payload.mode, "live");
        assert!(payload.ready);
        assert!(payload.sync_lag_ms.is_some());
        assert_eq!(payload.coverage.degraded_resource_kinds, vec!["Node"]);
        assert_eq!(
            payload.scope.as_ref().expect("scope").kind,
            GraphScopeKind::Namespace
        );
        assert_eq!(
            payload.scope.as_ref().expect("scope").namespace.as_deref(),
            Some("kube-system")
        );
    }

    #[tokio::test]
    async fn graph_health_full_mode_reports_live_state_and_coverage() {
        let tool = test_tool(Arc::new(MockBackend::default()));
        let result = tool
            .graph_health(Parameters(GraphHealthRequest {
                detail: GraphHealthRequestDetail::Full,
            }))
            .await
            .expect("graph_health should succeed");
        let payload: GraphHealthResponse =
            serde_json::from_value(result.structured_content.expect("structured response"))
                .expect("health payload");

        assert_eq!(payload.detail, GraphHealthDetail::Full);
        assert_eq!(payload.cluster, "test-cluster");
        assert_eq!(payload.mode, "live");
        assert!(payload.ready);
        assert_eq!(
            payload.scope.as_ref().expect("scope").kind,
            GraphScopeKind::Namespace
        );
        assert_eq!(
            payload.scope.as_ref().expect("scope").namespace.as_deref(),
            Some("kube-system")
        );
        assert!(payload.sync.is_some());
        assert!(payload.rebuild.is_some());
        assert_eq!(
            payload.sync.as_ref().expect("sync").poll_interval_seconds,
            5
        );
        assert_eq!(payload.coverage.degraded_resource_kinds, vec!["Node"]);
    }

    #[tokio::test]
    async fn graph_health_debug_mode_uses_most_recent_live_data_timestamp() {
        let sync_success = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let rebuild_success = SystemTime::UNIX_EPOCH + Duration::from_secs(20);
        let mut sync = SyncHealth::bootstrap(sync_success);
        sync.poll_interval_seconds = 7;

        let tool = test_tool_with(
            Arc::new(MockBackend::default()),
            "in-memory",
            "live",
            Some(GraphScope::namespace("team-a")),
            None,
            true,
            sync,
            Some(RebuildHealth {
                last_success_at: Some(rebuild_success),
                ..Default::default()
            }),
            &["Pod", "ConfigMap", "Namespace"],
        );

        let result = tool
            .graph_health(Parameters(GraphHealthRequest {
                detail: GraphHealthRequestDetail::Debug,
            }))
            .await
            .expect("graph_health should succeed");
        let payload: GraphHealthResponse =
            serde_json::from_value(result.structured_content.expect("structured response"))
                .expect("health payload");

        assert_eq!(payload.detail, GraphHealthDetail::Debug);
        assert_eq!(payload.mode, "live");
        assert!(payload.ready);
        assert_eq!(payload.data_as_of, Some(format_timestamp(rebuild_success)));
        assert_eq!(
            payload.coverage.degraded_resource_kinds,
            vec!["ConfigMap", "Namespace", "Pod"]
        );
    }

    #[tokio::test]
    async fn graph_health_live_without_success_reports_not_ready_and_no_lag() {
        let sync = SyncHealth {
            poll_interval_seconds: 5,
            ..Default::default()
        };
        let tool = test_tool_with(
            Arc::new(MockBackend::default()),
            "in-memory",
            "live",
            Some(GraphScope::cluster()),
            None,
            true,
            sync,
            None,
            &[],
        );

        let result = tool
            .graph_health(Parameters(GraphHealthRequest::default()))
            .await
            .expect("graph_health should succeed");
        let payload: GraphHealthCompactResponse =
            serde_json::from_value(result.structured_content.expect("structured response"))
                .expect("health payload");

        assert!(!payload.ready);
        assert!(payload.data_as_of.is_none());
        assert!(payload.sync_lag_ms.is_none());
    }

    #[tokio::test]
    async fn graph_health_snapshot_uses_manifest_provenance() {
        let sync = SyncHealth::bootstrap(SystemTime::UNIX_EPOCH + Duration::from_secs(5));
        let tool = test_tool_with(
            Arc::new(MockBackend::default()),
            "in-memory",
            "snapshot",
            Some(GraphScope::namespace("team-a")),
            Some("2026-03-28T00:00:00Z"),
            true,
            sync,
            None,
            &["Node"],
        );

        let result = tool
            .graph_health(Parameters(GraphHealthRequest {
                detail: GraphHealthRequestDetail::Full,
            }))
            .await
            .expect("graph_health should succeed");
        let payload: GraphHealthResponse =
            serde_json::from_value(result.structured_content.expect("structured response"))
                .expect("health payload");

        assert_eq!(payload.mode, "snapshot");
        assert!(payload.ready);
        assert_eq!(payload.data_as_of.as_deref(), Some("2026-03-28T00:00:00Z"));
        assert_eq!(
            payload.scope.as_ref().expect("scope").kind,
            GraphScopeKind::Namespace
        );
        assert_eq!(
            payload.scope.as_ref().expect("scope").namespace.as_deref(),
            Some("team-a")
        );
        assert!(payload.sync.is_none());
    }

    #[tokio::test]
    async fn graph_health_snapshot_requires_backend_probe_for_readiness() {
        let sync = SyncHealth::bootstrap(SystemTime::UNIX_EPOCH);
        let tool = test_tool_with(
            Arc::new(MockBackend {
                fail: Some("probe failure".to_string()),
                ..Default::default()
            }),
            "memgraph",
            "snapshot",
            Some(GraphScope::cluster()),
            Some("2026-03-28T00:00:00Z"),
            true,
            sync,
            None,
            &[],
        );

        let result = tool
            .graph_health(Parameters(GraphHealthRequest {
                detail: GraphHealthRequestDetail::Full,
            }))
            .await
            .expect("graph_health should succeed");
        let payload: GraphHealthResponse =
            serde_json::from_value(result.structured_content.expect("structured response"))
                .expect("health payload");

        assert!(!payload.backend_probe_ok);
        assert!(!payload.ready);
        assert_eq!(payload.data_as_of.as_deref(), Some("2026-03-28T00:00:00Z"));
    }

    #[test]
    fn classifies_backend_scope_error() {
        let err = query_issue_to_mcp(
            ariadne_core::query_issue::classify_backend_error(
                "MemgraphError: QueryError: Query execution error: Unbound variable: ns.",
            ),
            "RETURN 1".to_string(),
        );
        assert_eq!(err.code, rmcp::model::ErrorCode::INVALID_PARAMS);
        let data = err.data.expect("data");
        assert_eq!(data["kind"], "scope_error");
        assert_eq!(data["retryable"], false);
        assert_eq!(data["repairable"], true);
        assert_eq!(data["source"], "backend");
    }

    #[test]
    fn validator_errors_are_invalid_params_with_metadata() {
        let err = query_issue_to_mcp(
            QueryIssue::validation(
                QueryIssueKind::Schema,
                "Relationship BelongsTo not allowed".to_string(),
            ),
            "MATCH ...".to_string(),
        );
        assert_eq!(err.code, rmcp::model::ErrorCode::INVALID_PARAMS);
        let data = err.data.expect("data");
        assert_eq!(data["kind"], "schema_error");
        assert_eq!(data["retryable"], false);
        assert_eq!(data["repairable"], true);
        assert_eq!(data["source"], "validator");
    }
}
