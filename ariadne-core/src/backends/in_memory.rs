use crate::graph_backend::GraphBackend;
use crate::kube_redaction::redact_kubernetes_value;
use crate::prelude::Result;
use crate::state::{ClusterState, ClusterStateDiff, SharedClusterState};
use crate::types::{Edge, GenericObject, ResourceAttributes, ResourceType};
use ariadne_cypher::{
    Clause, Expr, Literal, MatchClause, OrderBy, PathPattern, Pattern, ProjectionItem, Query,
    RelationshipDirection, RelationshipPattern, ReturnClause, ValidationMode, parse_query,
    validate_query,
};
use k8s_openapi::Metadata;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

#[path = "in_memory/expression.rs"]
mod expression;
use expression::{eval_bool, eval_expr, literal_to_value, value_to_string};
#[path = "in_memory/matching.rs"]
mod matching;
use matching::{apply_match, eval_exists};
#[path = "in_memory/projection.rs"]
mod projection;
use projection::{
    apply_skip_limit, compare_values, distinct_rows, eval_list_slice, project_rows_internal,
    sort_rows,
};

#[derive(Debug, Default)]
struct QueryStats {
    parse_ms: u128,
    validate_ms: u128,
    lock_ms: u128,
    exec_ms: u128,
    match_ms: u128,
    unwind_ms: u128,
    with_ms: u128,
    return_ms: u128,
    with_project_ms: u128,
    with_filter_ms: u128,
    with_sort_ms: u128,
    with_distinct_ms: u128,
    with_skip_limit_ms: u128,
    return_project_ms: u128,
    return_sort_ms: u128,
    return_distinct_ms: u128,
    return_skip_limit_ms: u128,
    rows_peak: usize,
    rows_final: usize,
    nodes_scanned: usize,
    nodes_indexed: usize,
    edges_scanned: usize,
    edges_indexed: usize,
    match_clauses: usize,
    unwind_clauses: usize,
    with_clauses: usize,
    return_clauses: usize,
}

#[derive(Debug, Default)]
pub struct InMemoryBackend {
    state: Mutex<Option<SharedClusterState>>,
}

impl InMemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }

    fn state(&self) -> Result<SharedClusterState> {
        let guard = self.state.lock().expect("state lock poisoned");
        guard
            .as_ref()
            .cloned()
            .ok_or_else(|| std::io::Error::other("in-memory backend not initialized").into())
    }
}

#[async_trait::async_trait]
impl GraphBackend for InMemoryBackend {
    async fn create(&self, cluster_state: SharedClusterState) -> Result<()> {
        let mut guard = self.state.lock().expect("state lock poisoned");
        *guard = Some(cluster_state);
        Ok(())
    }

    async fn update(&self, _diff: ClusterStateDiff) -> Result<()> {
        Ok(())
    }

    async fn execute_query(
        &self,
        query: String,
        params: Option<HashMap<String, Value>>,
    ) -> Result<Vec<Value>> {
        let started = Instant::now();
        let mut stats = QueryStats::default();
        let result: Result<Vec<Value>> = (|| {
            let parse_start = Instant::now();
            let query_ast =
                parse_query(&query).map_err(|err| std::io::Error::other(err.to_string()))?;
            stats.parse_ms = parse_start.elapsed().as_millis();
            let validate_start = Instant::now();
            validate_query(&query_ast, ValidationMode::Engine)
                .map_err(|err| std::io::Error::other(err.to_string()))?;
            stats.validate_ms = validate_start.elapsed().as_millis();
            let state = self.state()?;
            let lock_start = Instant::now();
            let guard = state.lock().expect("cluster state lock poisoned");
            stats.lock_ms = lock_start.elapsed().as_millis();
            let params = params.unwrap_or_default();
            let exec_start = Instant::now();
            let output = execute_query_ast(&query_ast, &guard, &params, &mut stats);
            stats.exec_ms = exec_start.elapsed().as_millis();
            output
        })();

        let elapsed_ms = started.elapsed().as_millis();
        tracing::info!("in_memory: execute_query ({elapsed_ms} ms): {query}");
        if let Err(err) = &result {
            tracing::error!("in_memory: execute_query failed: {err}");
        }
        tracing::info!(
            "in_memory: execute_query stats nodes_scanned={} nodes_indexed={} edges_scanned={} edges_indexed={} match_clauses={} unwind_clauses={} with_clauses={} return_clauses={}",
            stats.nodes_scanned,
            stats.nodes_indexed,
            stats.edges_scanned,
            stats.edges_indexed,
            stats.match_clauses,
            stats.unwind_clauses,
            stats.with_clauses,
            stats.return_clauses
        );
        tracing::info!(
            "in_memory: execute_query timings parse={}ms validate={}ms lock={}ms exec={}ms match={}ms unwind={}ms with={}ms return={}ms with_project={}ms with_filter={}ms with_sort={}ms with_distinct={}ms with_skip={}ms return_project={}ms return_sort={}ms return_distinct={}ms return_skip={}ms rows_peak={} rows_final={}",
            stats.parse_ms,
            stats.validate_ms,
            stats.lock_ms,
            stats.exec_ms,
            stats.match_ms,
            stats.unwind_ms,
            stats.with_ms,
            stats.return_ms,
            stats.with_project_ms,
            stats.with_filter_ms,
            stats.with_sort_ms,
            stats.with_distinct_ms,
            stats.with_skip_limit_ms,
            stats.return_project_ms,
            stats.return_sort_ms,
            stats.return_distinct_ms,
            stats.return_skip_limit_ms,
            stats.rows_peak,
            stats.rows_final
        );
        result
    }

    async fn shutdown(&self) {
        let mut guard = self.state.lock().expect("state lock poisoned");
        *guard = None;
    }
}

type Row = HashMap<String, Value>;

fn execute_query_ast(
    query: &Query,
    state: &ClusterState,
    params: &HashMap<String, Value>,
    stats: &mut QueryStats,
) -> Result<Vec<Value>> {
    let mut rows = vec![Row::new()];
    stats.rows_peak = stats.rows_peak.max(rows.len());
    for clause in &query.clauses {
        match clause {
            Clause::Match(m) => {
                stats.match_clauses += 1;
                let clause_start = Instant::now();
                rows = apply_match(rows, m, state, params, stats)?;
                stats.match_ms += clause_start.elapsed().as_millis();
                stats.rows_peak = stats.rows_peak.max(rows.len());
            }
            Clause::Unwind(u) => {
                stats.unwind_clauses += 1;
                let clause_start = Instant::now();
                rows = apply_unwind(rows, u, state, params, stats)?;
                stats.unwind_ms += clause_start.elapsed().as_millis();
                stats.rows_peak = stats.rows_peak.max(rows.len());
            }
            Clause::With(w) => {
                stats.with_clauses += 1;
                let clause_start = Instant::now();
                rows = apply_with(rows, w, state, params, stats)?;
                stats.with_ms += clause_start.elapsed().as_millis();
                stats.rows_peak = stats.rows_peak.max(rows.len());
            }
            Clause::Return(r) => {
                stats.return_clauses += 1;
                let clause_start = Instant::now();
                let output = finalize_return(rows, r, state, params, stats);
                stats.return_ms += clause_start.elapsed().as_millis();
                return output;
            }
            _ => {
                return Err(std::io::Error::other("unsupported clause for engine").into());
            }
        }
    }

    Err(std::io::Error::other("query must include RETURN for in-memory engine").into())
}

fn apply_unwind(
    rows: Vec<Row>,
    clause: &ariadne_cypher::UnwindClause,
    state: &ClusterState,
    params: &HashMap<String, Value>,
    stats: &mut QueryStats,
) -> Result<Vec<Row>> {
    let mut output = Vec::new();
    for row in rows {
        let value = eval_expr(&clause.expression, &row, state, params, stats)?;
        match value {
            Value::Array(items) => {
                for item in items {
                    let mut new_row = row.clone();
                    new_row.insert(clause.variable.clone(), item);
                    output.push(new_row);
                }
            }
            Value::Null => {}
            other => {
                let mut new_row = row.clone();
                new_row.insert(clause.variable.clone(), other);
                output.push(new_row);
            }
        }
    }
    Ok(output)
}

fn apply_with(
    rows: Vec<Row>,
    clause: &ariadne_cypher::WithClause,
    state: &ClusterState,
    params: &HashMap<String, Value>,
    stats: &mut QueryStats,
) -> Result<Vec<Row>> {
    let project_start = Instant::now();
    let mut projected = project_rows_internal(rows, &clause.items, state, params, stats)?;
    stats.with_project_ms += project_start.elapsed().as_millis();

    if clause.distinct {
        let distinct_start = Instant::now();
        projected = distinct_rows(projected);
        stats.with_distinct_ms += distinct_start.elapsed().as_millis();
    }

    if let Some(where_clause) = &clause.where_clause {
        let filter_start = Instant::now();
        projected = projected
            .into_iter()
            .filter_map(
                |row| match eval_bool(where_clause, &row, state, params, stats) {
                    Ok(true) => Some(Ok(row)),
                    Ok(false) => None,
                    Err(err) => Some(Err(err)),
                },
            )
            .collect::<Result<Vec<_>>>()?;
        stats.with_filter_ms += filter_start.elapsed().as_millis();
    }

    if let Some(order) = &clause.order {
        let sort_start = Instant::now();
        projected = sort_rows(projected, order, state, params, stats)?;
        stats.with_sort_ms += sort_start.elapsed().as_millis();
    }

    let skip_start = Instant::now();
    projected = apply_skip_limit(
        projected,
        clause.skip.as_ref(),
        clause.limit.as_ref(),
        state,
        params,
        stats,
    )?;
    stats.with_skip_limit_ms += skip_start.elapsed().as_millis();

    Ok(projected)
}

fn finalize_return(
    rows: Vec<Row>,
    clause: &ReturnClause,
    state: &ClusterState,
    params: &HashMap<String, Value>,
    stats: &mut QueryStats,
) -> Result<Vec<Value>> {
    let project_start = Instant::now();
    let mut projected = project_rows_internal(rows, &clause.items, state, params, stats)?;
    stats.return_project_ms += project_start.elapsed().as_millis();
    if clause.distinct {
        let distinct_start = Instant::now();
        projected = distinct_rows(projected);
        stats.return_distinct_ms += distinct_start.elapsed().as_millis();
    }
    if let Some(order) = &clause.order {
        let sort_start = Instant::now();
        projected = sort_rows(projected, order, state, params, stats)?;
        stats.return_sort_ms += sort_start.elapsed().as_millis();
    }
    let skip_start = Instant::now();
    projected = apply_skip_limit(
        projected,
        clause.skip.as_ref(),
        clause.limit.as_ref(),
        state,
        params,
        stats,
    )?;
    stats.return_skip_limit_ms += skip_start.elapsed().as_millis();
    let out: Vec<Value> = projected
        .into_iter()
        .map(|row| Value::Object(row.into_iter().collect()))
        .collect();
    stats.rows_final = out.len();
    Ok(out)
}

fn node_to_value(obj: &GenericObject) -> Result<Value> {
    let Some(attributes) = &obj.attributes else {
        return Ok(Value::Null);
    };
    let mut value = match attributes.as_ref() {
        ResourceAttributes::Node { node } => {
            let mut fixed = node.as_ref().clone();
            cleanup_metadata(&mut fixed);
            serde_json::to_value(fixed)?
        }
        ResourceAttributes::Namespace { namespace } => {
            let mut fixed = namespace.as_ref().clone();
            cleanup_metadata(&mut fixed);
            serde_json::to_value(fixed)?
        }
        ResourceAttributes::Pod { pod } => {
            let mut fixed = pod.as_ref().clone();
            cleanup_metadata(&mut fixed);
            serde_json::to_value(fixed)?
        }
        ResourceAttributes::Deployment { deployment } => {
            let mut fixed = deployment.as_ref().clone();
            cleanup_metadata(&mut fixed);
            serde_json::to_value(fixed)?
        }
        ResourceAttributes::StatefulSet { stateful_set } => {
            let mut fixed = stateful_set.as_ref().clone();
            cleanup_metadata(&mut fixed);
            serde_json::to_value(fixed)?
        }
        ResourceAttributes::ReplicaSet { replica_set } => {
            let mut fixed = replica_set.as_ref().clone();
            cleanup_metadata(&mut fixed);
            serde_json::to_value(fixed)?
        }
        ResourceAttributes::DaemonSet { daemon_set } => {
            let mut fixed = daemon_set.as_ref().clone();
            cleanup_metadata(&mut fixed);
            serde_json::to_value(fixed)?
        }
        ResourceAttributes::Job { job } => {
            let mut fixed = job.as_ref().clone();
            cleanup_metadata(&mut fixed);
            serde_json::to_value(fixed)?
        }
        ResourceAttributes::Ingress { ingress } => {
            let mut fixed = ingress.as_ref().clone();
            cleanup_metadata(&mut fixed);
            serde_json::to_value(fixed)?
        }
        ResourceAttributes::Service { service } => {
            let mut fixed = service.as_ref().clone();
            cleanup_metadata(&mut fixed);
            serde_json::to_value(fixed)?
        }
        ResourceAttributes::EndpointSlice { endpoint_slice } => {
            let mut fixed = endpoint_slice.as_ref().clone();
            cleanup_metadata(&mut fixed);
            serde_json::to_value(fixed)?
        }
        ResourceAttributes::NetworkPolicy { network_policy } => {
            let mut fixed = network_policy.as_ref().clone();
            cleanup_metadata(&mut fixed);
            serde_json::to_value(fixed)?
        }
        ResourceAttributes::ConfigMap { config_map } => {
            let mut fixed = config_map.as_ref().clone();
            cleanup_metadata(&mut fixed);
            serde_json::to_value(fixed)?
        }
        ResourceAttributes::StorageClass { storage_class } => {
            let mut fixed = storage_class.as_ref().clone();
            cleanup_metadata(&mut fixed);
            serde_json::to_value(fixed)?
        }
        ResourceAttributes::PersistentVolumeClaim { pvc } => {
            let mut fixed = pvc.as_ref().clone();
            cleanup_metadata(&mut fixed);
            serde_json::to_value(fixed)?
        }
        ResourceAttributes::PersistentVolume { pv } => {
            let mut fixed = pv.as_ref().clone();
            cleanup_metadata(&mut fixed);
            serde_json::to_value(fixed)?
        }
        ResourceAttributes::ServiceAccount { service_account } => {
            let mut fixed = service_account.as_ref().clone();
            cleanup_metadata(&mut fixed);
            serde_json::to_value(fixed)?
        }
        ResourceAttributes::Event { event } => {
            let mut fixed = event.as_ref().clone();
            cleanup_metadata(&mut fixed);
            serde_json::to_value(fixed)?
        }
        ResourceAttributes::Provisioner { provisioner } => {
            serde_json::to_value(provisioner.as_ref())?
        }
        ResourceAttributes::IngressServiceBackend {
            ingress_service_backend,
        } => serde_json::to_value(ingress_service_backend.as_ref())?,
        ResourceAttributes::EndpointAddress { endpoint_address } => {
            serde_json::to_value(endpoint_address.as_ref())?
        }
        ResourceAttributes::Endpoint { endpoint } => serde_json::to_value(endpoint.as_ref())?,
        ResourceAttributes::Host { host } => serde_json::to_value(host.as_ref())?,
        ResourceAttributes::Cluster { cluster } => serde_json::to_value(cluster.as_ref())?,
        ResourceAttributes::Container { container } => serde_json::to_value(container.as_ref())?,
    };

    redact_kubernetes_value(&obj.resource_type, &mut value);

    if let Value::Object(map) = &mut value {
        let (uid, name, ns) = if let Some(Value::Object(metadata)) = map.get("metadata") {
            (
                metadata
                    .get("uid")
                    .and_then(|v| v.as_str())
                    .map(|v| v.to_string()),
                metadata
                    .get("name")
                    .and_then(|v| v.as_str())
                    .map(|v| v.to_string()),
                metadata
                    .get("namespace")
                    .and_then(|v| v.as_str())
                    .map(|v| v.to_string()),
            )
        } else {
            (None, None, None)
        };

        if let Some(uid) = uid {
            map.insert("metadata_uid".to_string(), Value::String(uid));
        }
        if let Some(name) = name {
            map.insert("metadata_name".to_string(), Value::String(name));
        }
        if let Some(ns) = ns {
            map.insert("metadata_namespace".to_string(), Value::String(ns));
        }
    }

    Ok(value)
}

fn cleanup_metadata<T>(fixed: &mut T)
where
    T: Metadata<Ty = ObjectMeta>,
{
    let md = fixed.metadata_mut();
    if md.managed_fields.is_some() {
        md.managed_fields = None;
    }
    if let Some(map) = md.annotations.as_mut() {
        map.remove("kubectl.kubernetes.io/last-applied-configuration");
        map.remove("kapp.k14s.io/original");
    }
}

#[cfg(test)]
#[path = "in_memory/tests.rs"]
mod tests;
