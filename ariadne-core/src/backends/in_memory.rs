use crate::graph_backend::GraphBackend;
use crate::prelude::Result;
use crate::state::{ClusterState, ClusterStateDiff, SharedClusterState};
use crate::types::{Edge, GenericObject, ResourceAttributes, ResourceType};
use ariadne_cypher::{
    parse_query, validate_query, Clause, Expr, Literal, MatchClause, OrderBy, PathPattern, Pattern,
    ProjectionItem, Query, RelationshipDirection, RelationshipPattern, ReturnClause,
    ValidationMode,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use k8s_openapi::Metadata;
use serde_json::{Map, Value};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::Instant;
use strum::IntoEnumIterator;

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

    async fn execute_query(&self, query: String) -> Result<Vec<Value>> {
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
            let exec_start = Instant::now();
            let output = execute_query_ast(&query_ast, &guard, &mut stats);
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
    stats: &mut QueryStats,
) -> Result<Vec<Value>> {
    let mut rows = vec![Row::new()];
    stats.rows_peak = stats.rows_peak.max(rows.len());
    for clause in &query.clauses {
        match clause {
            Clause::Match(m) => {
                stats.match_clauses += 1;
                let clause_start = Instant::now();
                rows = apply_match(rows, m, state, stats)?;
                stats.match_ms += clause_start.elapsed().as_millis();
                stats.rows_peak = stats.rows_peak.max(rows.len());
            }
            Clause::Unwind(u) => {
                stats.unwind_clauses += 1;
                let clause_start = Instant::now();
                rows = apply_unwind(rows, u, state, stats)?;
                stats.unwind_ms += clause_start.elapsed().as_millis();
                stats.rows_peak = stats.rows_peak.max(rows.len());
            }
            Clause::With(w) => {
                stats.with_clauses += 1;
                let clause_start = Instant::now();
                rows = apply_with(rows, w, state, stats)?;
                stats.with_ms += clause_start.elapsed().as_millis();
                stats.rows_peak = stats.rows_peak.max(rows.len());
            }
            Clause::Return(r) => {
                stats.return_clauses += 1;
                let clause_start = Instant::now();
                let output = finalize_return(rows, r, state, stats);
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

fn apply_match(
    rows: Vec<Row>,
    clause: &MatchClause,
    state: &ClusterState,
    stats: &mut QueryStats,
) -> Result<Vec<Row>> {
    let mut output = Vec::new();
    let vars = pattern_variables(&clause.pattern);

    for row in rows {
        let can_first_match = matches!(&clause.pattern, Pattern::Path(path) if path.segments.len() > 1)
            && vars.iter().all(|var| row.contains_key(var));
        if can_first_match {
            let matched = eval_exists(
                &row,
                &clause.pattern,
                clause.where_clause.as_ref(),
                state,
                stats,
            )?;
            if matched || clause.optional {
                output.push(row);
            }
            continue;
        }

        let matches = match_pattern(&row, &clause.pattern, state, stats)?;
        if matches.is_empty() {
            if clause.optional {
                let mut expanded = row.clone();
                for var in &vars {
                    expanded.entry(var.clone()).or_insert(Value::Null);
                }
                output.push(expanded);
            }
            continue;
        }

        for binding in matches {
            if let Some(merged) = merge_rows(&row, &binding) {
                output.push(merged);
            }
        }
    }

    if let Some(where_clause) = &clause.where_clause {
        output = output
            .into_iter()
            .filter_map(|row| match eval_bool(where_clause, &row, state, stats) {
                Ok(true) => Some(Ok(row)),
                Ok(false) => None,
                Err(err) => Some(Err(err)),
            })
            .collect::<Result<Vec<_>>>()?;
    }

    Ok(output)
}

fn apply_unwind(
    rows: Vec<Row>,
    clause: &ariadne_cypher::UnwindClause,
    state: &ClusterState,
    stats: &mut QueryStats,
) -> Result<Vec<Row>> {
    let mut output = Vec::new();
    for row in rows {
        let value = eval_expr(&clause.expression, &row, state, stats)?;
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
    stats: &mut QueryStats,
) -> Result<Vec<Row>> {
    let project_start = Instant::now();
    let mut projected = project_rows_internal(rows, &clause.items, state, stats)?;
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
            .filter_map(|row| match eval_bool(where_clause, &row, state, stats) {
                Ok(true) => Some(Ok(row)),
                Ok(false) => None,
                Err(err) => Some(Err(err)),
            })
            .collect::<Result<Vec<_>>>()?;
        stats.with_filter_ms += filter_start.elapsed().as_millis();
    }

    if let Some(order) = &clause.order {
        let sort_start = Instant::now();
        projected = sort_rows(projected, order, state, stats)?;
        stats.with_sort_ms += sort_start.elapsed().as_millis();
    }

    let skip_start = Instant::now();
    projected = apply_skip_limit(
        projected,
        clause.skip.as_ref(),
        clause.limit.as_ref(),
        state,
        stats,
    )?;
    stats.with_skip_limit_ms += skip_start.elapsed().as_millis();

    Ok(projected)
}

fn finalize_return(
    rows: Vec<Row>,
    clause: &ReturnClause,
    state: &ClusterState,
    stats: &mut QueryStats,
) -> Result<Vec<Value>> {
    let project_start = Instant::now();
    let mut projected = project_rows_internal(rows, &clause.items, state, stats)?;
    stats.return_project_ms += project_start.elapsed().as_millis();
    if clause.distinct {
        let distinct_start = Instant::now();
        projected = distinct_rows(projected);
        stats.return_distinct_ms += distinct_start.elapsed().as_millis();
    }
    if let Some(order) = &clause.order {
        let sort_start = Instant::now();
        projected = sort_rows(projected, order, state, stats)?;
        stats.return_sort_ms += sort_start.elapsed().as_millis();
    }
    let skip_start = Instant::now();
    projected = apply_skip_limit(
        projected,
        clause.skip.as_ref(),
        clause.limit.as_ref(),
        state,
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

fn pattern_variables(pattern: &Pattern) -> Vec<String> {
    let mut vars = Vec::new();
    match pattern {
        Pattern::Node(node) => {
            if let Some(var) = &node.variable {
                vars.push(var.clone());
            }
        }
        Pattern::Relationship(rel) => {
            if let Some(var) = &rel.left.variable {
                vars.push(var.clone());
            }
            if let Some(var) = &rel.right.variable {
                vars.push(var.clone());
            }
            if let Some(var) = &rel.rel.variable {
                vars.push(var.clone());
            }
        }
        Pattern::Path(path) => {
            if let Some(var) = &path.start.variable {
                vars.push(var.clone());
            }
            for segment in &path.segments {
                if let Some(var) = &segment.node.variable {
                    vars.push(var.clone());
                }
                if let Some(var) = &segment.rel.variable {
                    vars.push(var.clone());
                }
            }
        }
    }
    vars.sort();
    vars.dedup();
    vars
}

fn match_pattern(
    row: &Row,
    pattern: &Pattern,
    state: &ClusterState,
    stats: &mut QueryStats,
) -> Result<Vec<Row>> {
    match pattern {
        Pattern::Node(node) => match_node_pattern(row, node, state, stats),
        Pattern::Relationship(rel) => match_relationship_pattern(row, rel, state, stats),
        Pattern::Path(path) => match_path_pattern(row, path, state, stats),
    }
}

fn match_node_pattern(
    row: &Row,
    pattern: &ariadne_cypher::NodePattern,
    state: &ClusterState,
    stats: &mut QueryStats,
) -> Result<Vec<Row>> {
    let var = pattern.variable.as_ref();
    if let Some(name) = var {
        if let Some(bound) = row.get(name) {
            if let Some(uid) = node_uid_from_value(bound) {
                if let Some(node) = state.node_by_uid(uid) {
                    if matches_labels(node, &pattern.labels)? {
                        return Ok(vec![Row::new()]);
                    }
                }
            }
            return Ok(Vec::new());
        }
    }

    let mut results = Vec::new();
    let label_type =
        if pattern.labels.len() == 1 {
            Some(ResourceType::try_new(&pattern.labels[0]).map_err(|_| {
                std::io::Error::other(format!("unknown label: {}", pattern.labels[0]))
            })?)
        } else {
            None
        };
    let candidates: Box<dyn Iterator<Item = &GenericObject>> =
        if let Some(ref expected) = label_type {
            Box::new(state.get_nodes_by_type(expected))
        } else {
            Box::new(state.get_nodes())
        };
    for node in candidates {
        if label_type.is_some() {
            stats.nodes_indexed += 1;
        } else {
            stats.nodes_scanned += 1;
        }
        if label_type.is_none() && !matches_labels(node, &pattern.labels)? {
            continue;
        }
        let mut binding = Row::new();
        if let Some(name) = var {
            binding.insert(name.clone(), node_to_value(node)?);
        }
        results.push(binding);
    }

    Ok(results)
}

fn match_relationship_pattern(
    row: &Row,
    pattern: &RelationshipPattern,
    state: &ClusterState,
    stats: &mut QueryStats,
) -> Result<Vec<Row>> {
    let mut results = Vec::new();
    let rel_types = &pattern.rel.types;
    let dir = &pattern.rel.direction;

    let left_label_type = if pattern.left.labels.len() == 1 {
        Some(ResourceType::try_new(&pattern.left.labels[0]).map_err(|_| {
            std::io::Error::other(format!("unknown label: {}", pattern.left.labels[0]))
        })?)
    } else {
        None
    };
    let right_label_type = if pattern.right.labels.len() == 1 {
        Some(
            ResourceType::try_new(&pattern.right.labels[0]).map_err(|_| {
                std::io::Error::other(format!("unknown label: {}", pattern.right.labels[0]))
            })?,
        )
    } else {
        None
    };
    if rel_types.is_empty() {
        for edge in state.get_edges() {
            stats.edges_scanned += 1;
            if let Some(rows) = match_edge_row(
                row,
                pattern,
                &edge,
                state,
                dir,
                left_label_type.as_ref(),
                right_label_type.as_ref(),
            )? {
                results.extend(rows);
            }
        }
    } else {
        let mut seen = std::collections::HashSet::new();
        for rel_type in rel_types {
            if let Some(edge_type) = edge_type_from_str(rel_type) {
                if !seen.insert(edge_type.clone()) {
                    continue;
                }
                for edge in state.get_edges_by_type(&edge_type) {
                    stats.edges_indexed += 1;
                    if let Some(rows) = match_edge_row(
                        row,
                        pattern,
                        &edge,
                        state,
                        dir,
                        left_label_type.as_ref(),
                        right_label_type.as_ref(),
                    )? {
                        results.extend(rows);
                    }
                }
            }
        }
    }

    Ok(results)
}

fn match_path_pattern(
    row: &Row,
    pattern: &PathPattern,
    state: &ClusterState,
    stats: &mut QueryStats,
) -> Result<Vec<Row>> {
    let (relationships, internal_vars) = path_relationships_with_internal_vars(pattern, row);
    let mut bindings = vec![Row::new()];

    for rel_pattern in relationships {
        let mut next = Vec::new();
        for binding in bindings {
            let combined = combine_row_for_match(row, &binding);
            let matches = match_relationship_pattern(&combined, &rel_pattern, state, stats)?;
            for new_binding in matches {
                let mut merged = binding.clone();
                for (key, value) in new_binding {
                    merged.insert(key, value);
                }
                next.push(merged);
            }
        }
        bindings = next;
        if bindings.is_empty() {
            break;
        }
    }

    if !internal_vars.is_empty() {
        let internal: HashSet<String> = internal_vars.into_iter().collect();
        for binding in &mut bindings {
            for key in &internal {
                binding.remove(key);
            }
        }
    }

    Ok(bindings)
}

fn combine_row_for_match(base: &Row, binding: &Row) -> Row {
    let mut combined = base.clone();
    for (key, value) in binding {
        if !combined.contains_key(key) {
            combined.insert(key.clone(), value.clone());
        }
    }
    combined
}

fn path_relationships_with_internal_vars(
    pattern: &PathPattern,
    row: &Row,
) -> (Vec<RelationshipPattern>, Vec<String>) {
    let mut used = HashSet::new();
    for key in row.keys() {
        used.insert(key.clone());
    }
    if let Some(var) = &pattern.start.variable {
        used.insert(var.clone());
    }
    for segment in &pattern.segments {
        if let Some(var) = &segment.node.variable {
            used.insert(var.clone());
        }
        if let Some(var) = &segment.rel.variable {
            used.insert(var.clone());
        }
    }

    let mut nodes = Vec::with_capacity(pattern.segments.len() + 1);
    nodes.push(pattern.start.clone());
    for segment in &pattern.segments {
        nodes.push(segment.node.clone());
    }

    let mut internal_vars = Vec::new();
    if nodes.len() > 2 {
        for idx in 1..nodes.len() - 1 {
            if nodes[idx].variable.is_none() {
                let name = unique_internal_var(&mut used, idx);
                nodes[idx].variable = Some(name.clone());
                internal_vars.push(name);
            }
        }
    }

    let mut relationships = Vec::with_capacity(pattern.segments.len());
    for (idx, segment) in pattern.segments.iter().enumerate() {
        relationships.push(RelationshipPattern {
            left: nodes[idx].clone(),
            rel: segment.rel.clone(),
            right: nodes[idx + 1].clone(),
            span: segment.span,
        });
    }

    (relationships, internal_vars)
}

fn unique_internal_var(used: &mut HashSet<String>, mut index: usize) -> String {
    loop {
        let candidate = format!("__ariadne_internal_path_node_{index}");
        if used.insert(candidate.clone()) {
            return candidate;
        }
        index += 1;
    }
}

fn edge_type_from_str(name: &str) -> Option<Edge> {
    Edge::iter().find(|edge| edge.to_string().eq_ignore_ascii_case(name))
}

fn match_edge_row(
    row: &Row,
    pattern: &RelationshipPattern,
    edge: &crate::state::GraphEdge,
    state: &ClusterState,
    dir: &RelationshipDirection,
    left_label_type: Option<&ResourceType>,
    right_label_type: Option<&ResourceType>,
) -> Result<Option<Vec<Row>>> {
    let pairs: Vec<(String, String)> = match dir {
        RelationshipDirection::LeftToRight => vec![(edge.source.clone(), edge.target.clone())],
        RelationshipDirection::RightToLeft => vec![(edge.target.clone(), edge.source.clone())],
        RelationshipDirection::Undirected => vec![
            (edge.source.clone(), edge.target.clone()),
            (edge.target.clone(), edge.source.clone()),
        ],
    };

    let mut results = Vec::new();
    for (left_uid, right_uid) in pairs {
        let left_node = match state.node_by_uid(&left_uid) {
            Some(node) => node,
            None => continue,
        };
        let right_node = match state.node_by_uid(&right_uid) {
            Some(node) => node,
            None => continue,
        };

        if let Some(expected) = left_label_type {
            if left_node.resource_type != *expected {
                continue;
            }
        } else if !pattern.left.labels.is_empty()
            && !matches_labels(left_node, &pattern.left.labels)?
        {
            continue;
        }

        if let Some(expected) = right_label_type {
            if right_node.resource_type != *expected {
                continue;
            }
        } else if !pattern.right.labels.is_empty()
            && !matches_labels(right_node, &pattern.right.labels)?
        {
            continue;
        }

        if let Some(var) = &pattern.left.variable {
            if let Some(bound) = row.get(var) {
                if !node_value_matches(bound, left_node) {
                    continue;
                }
            }
        }
        if let Some(var) = &pattern.right.variable {
            if let Some(bound) = row.get(var) {
                if !node_value_matches(bound, right_node) {
                    continue;
                }
            }
        }

        if let Some(rel_var) = &pattern.rel.variable {
            if let Some(bound) = row.get(rel_var) {
                if !relationship_value_matches(bound, edge, &left_uid, &right_uid) {
                    continue;
                }
            }
        }

        let mut binding = Row::new();
        if let Some(var) = &pattern.left.variable {
            if !row.contains_key(var) {
                binding.insert(var.clone(), node_to_value(left_node)?);
            }
        }
        if let Some(var) = &pattern.right.variable {
            if !row.contains_key(var) {
                binding.insert(var.clone(), node_to_value(right_node)?);
            }
        }
        if let Some(rel_var) = &pattern.rel.variable {
            if !row.contains_key(rel_var) {
                binding.insert(
                    rel_var.clone(),
                    relationship_to_value(edge, &left_uid, &right_uid),
                );
            }
        }

        results.push(binding);
    }

    if results.is_empty() {
        Ok(None)
    } else {
        Ok(Some(results))
    }
}

fn merge_rows(base: &Row, binding: &Row) -> Option<Row> {
    let mut merged = base.clone();
    for (key, value) in binding {
        if let Some(existing) = merged.get(key) {
            if existing.is_null() {
                if !value.is_null() {
                    merged.insert(key.clone(), value.clone());
                }
                continue;
            }
            if value.is_null() {
                continue;
            }
            if existing != value {
                return None;
            }
        } else {
            merged.insert(key.clone(), value.clone());
        }
    }
    Some(merged)
}

fn node_uid_from_value(value: &Value) -> Option<&str> {
    let obj = value.as_object()?;
    if let Some(uid) = obj.get("metadata_uid").and_then(|v| v.as_str()) {
        return Some(uid);
    }
    if let Some(Value::Object(metadata)) = obj.get("metadata") {
        if let Some(uid) = metadata.get("uid").and_then(|v| v.as_str()) {
            return Some(uid);
        }
    }
    None
}

fn node_value_matches(value: &Value, node: &GenericObject) -> bool {
    node_uid_from_value(value)
        .map(|uid| uid == node.id.uid)
        .unwrap_or(false)
}

fn relationship_to_value(edge: &crate::state::GraphEdge, left_uid: &str, right_uid: &str) -> Value {
    let mut map = Map::new();
    map.insert(
        "type".to_string(),
        Value::String(format!("{:?}", edge.edge_type)),
    );
    map.insert("source".to_string(), Value::String(left_uid.to_string()));
    map.insert("target".to_string(), Value::String(right_uid.to_string()));
    map.insert(
        "source_type".to_string(),
        Value::String(format!("{:?}", edge.source_type)),
    );
    map.insert(
        "target_type".to_string(),
        Value::String(format!("{:?}", edge.target_type)),
    );
    Value::Object(map)
}

fn relationship_value_matches(
    value: &Value,
    edge: &crate::state::GraphEdge,
    left_uid: &str,
    right_uid: &str,
) -> bool {
    let obj = match value.as_object() {
        Some(obj) => obj,
        None => return false,
    };
    if let Some(edge_type) = obj.get("type").and_then(|v| v.as_str()) {
        if !edge_type.eq_ignore_ascii_case(&format!("{:?}", edge.edge_type)) {
            return false;
        }
    }
    if let Some(source) = obj.get("source").and_then(|v| v.as_str()) {
        if source != left_uid {
            return false;
        }
    }
    if let Some(target) = obj.get("target").and_then(|v| v.as_str()) {
        if target != right_uid {
            return false;
        }
    }
    true
}

fn matches_labels(node: &GenericObject, labels: &[String]) -> Result<bool> {
    if labels.is_empty() {
        return Ok(true);
    }
    if labels.len() > 1 {
        return Ok(false);
    }
    let label = labels[0].as_str();
    let expected = ResourceType::try_new(label)
        .map_err(|_| std::io::Error::other(format!("unknown label: {label}")))?;
    Ok(node.resource_type == expected)
}

fn project_rows_internal(
    rows: Vec<Row>,
    items: &[ProjectionItem],
    state: &ClusterState,
    stats: &mut QueryStats,
) -> Result<Vec<Row>> {
    let has_agg = items.iter().any(|item| is_aggregate_expr(&item.expr));

    if has_agg {
        return project_rows_aggregate(rows, items, state, stats);
    }

    let mut output = Vec::with_capacity(rows.len());
    for row in rows {
        let mut record = Row::new();
        for (idx, item) in items.iter().enumerate() {
            match &item.expr {
                Expr::Star => {
                    if item.alias.is_some() {
                        return Err(std::io::Error::other("cannot alias RETURN *").into());
                    }
                    for (k, v) in &row {
                        record.insert(k.clone(), v.clone());
                    }
                }
                _ => {
                    let key = projection_label(item, idx);
                    let value = eval_expr(&item.expr, &row, state, stats)?;
                    record.insert(key, value);
                }
            }
        }
        output.push(record);
    }
    Ok(output)
}

fn project_rows_aggregate(
    rows: Vec<Row>,
    items: &[ProjectionItem],
    state: &ClusterState,
    stats: &mut QueryStats,
) -> Result<Vec<Row>> {
    let mut non_agg_indices = Vec::new();
    for (idx, item) in items.iter().enumerate() {
        if !is_aggregate_expr(&item.expr) {
            if matches!(item.expr, Expr::Star) {
                return Err(std::io::Error::other("cannot aggregate with RETURN *").into());
            }
            non_agg_indices.push(idx);
        }
    }

    let mut groups: HashMap<String, (Vec<Value>, Vec<Row>)> = HashMap::new();
    for row in rows {
        let mut key_values = Vec::new();
        for idx in &non_agg_indices {
            let value = eval_expr(&items[*idx].expr, &row, state, stats)?;
            key_values.push(value);
        }
        let key = group_key(&key_values);
        groups
            .entry(key)
            .or_insert_with(|| (key_values.clone(), Vec::new()))
            .1
            .push(row);
    }

    let mut output = Vec::new();
    for (_, (key_values, group_rows)) in groups {
        let mut record = Row::new();
        let mut key_iter = key_values.into_iter();
        for (idx, item) in items.iter().enumerate() {
            let value = if is_aggregate_expr(&item.expr) {
                eval_aggregate(&item.expr, &group_rows, state, stats)?
            } else {
                key_iter
                    .next()
                    .ok_or_else(|| std::io::Error::other("missing group key"))?
            };
            let key = projection_label(item, idx);
            record.insert(key, value);
        }
        output.push(record);
    }
    Ok(output)
}

fn eval_aggregate(
    expr: &Expr,
    rows: &[Row],
    state: &ClusterState,
    stats: &mut QueryStats,
) -> Result<Value> {
    match expr {
        Expr::CountStar => Ok(Value::from(rows.len() as i64)),
        Expr::FunctionCall { name, args } => match name.to_ascii_lowercase().as_str() {
            "count" => {
                let target = args
                    .first()
                    .ok_or_else(|| std::io::Error::other("count requires one argument"))?;
                let mut count = 0i64;
                for row in rows {
                    let value = eval_expr(target, row, state, stats)?;
                    if !value.is_null() {
                        count += 1;
                    }
                }
                Ok(Value::from(count))
            }
            "sum" => {
                let target = args
                    .first()
                    .ok_or_else(|| std::io::Error::other("sum requires one argument"))?;
                let mut total = 0.0;
                let mut seen = false;
                for row in rows {
                    if let Some(v) = eval_expr(target, row, state, stats)?.as_f64() {
                        total += v;
                        seen = true;
                    }
                }
                if seen {
                    Ok(Value::from(total))
                } else {
                    Ok(Value::Null)
                }
            }
            "avg" => {
                let target = args
                    .first()
                    .ok_or_else(|| std::io::Error::other("avg requires one argument"))?;
                let mut total = 0.0;
                let mut count = 0.0;
                for row in rows {
                    if let Some(v) = eval_expr(target, row, state, stats)?.as_f64() {
                        total += v;
                        count += 1.0;
                    }
                }
                if count == 0.0 {
                    Ok(Value::Null)
                } else {
                    Ok(Value::from(total / count))
                }
            }
            "min" | "max" => {
                let target = args
                    .first()
                    .ok_or_else(|| std::io::Error::other("min/max require one argument"))?;
                let mut current: Option<Value> = None;
                for row in rows {
                    let value = eval_expr(target, row, state, stats)?;
                    if value.is_null() {
                        continue;
                    }
                    current = match current.take() {
                        None => Some(value),
                        Some(existing) => {
                            let ord = compare_values(&existing, &value).unwrap_or(Ordering::Equal);
                            let choose = if name.eq_ignore_ascii_case("min") {
                                ord != Ordering::Greater
                            } else {
                                ord != Ordering::Less
                            };
                            Some(if choose { existing } else { value })
                        }
                    };
                }
                Ok(current.unwrap_or(Value::Null))
            }
            "collect" => {
                let target = args
                    .first()
                    .ok_or_else(|| std::io::Error::other("collect requires one argument"))?;
                let mut values = Vec::new();
                for row in rows {
                    values.push(eval_expr(target, row, state, stats)?);
                }
                Ok(Value::Array(values))
            }
            _ => Err(std::io::Error::other("unsupported aggregate function").into()),
        },
        Expr::IndexAccess { expr, index } => {
            let base = eval_aggregate(expr, rows, state, stats)?;
            let sample = rows.first().cloned().unwrap_or_default();
            let idx = eval_expr(index, &sample, state, stats)?;
            match (base, idx) {
                (Value::Array(items), Value::Number(n)) => {
                    let i = n.as_i64().unwrap_or(-1);
                    if i < 0 {
                        Ok(Value::Null)
                    } else {
                        Ok(items.get(i as usize).cloned().unwrap_or(Value::Null))
                    }
                }
                _ => Ok(Value::Null),
            }
        }
        Expr::ListSlice { expr, start, end } => {
            let base = eval_aggregate(expr, rows, state, stats)?;
            let sample = rows.first().cloned().unwrap_or_default();
            eval_list_slice(
                base,
                start.as_deref(),
                end.as_deref(),
                &sample,
                state,
                stats,
            )
        }
        _ => Err(std::io::Error::other("unsupported aggregate expression").into()),
    }
}

fn projection_label(item: &ProjectionItem, idx: usize) -> String {
    if let Some(alias) = &item.alias {
        return alias.clone();
    }
    match &item.expr {
        Expr::Variable(name) => name.clone(),
        Expr::PropertyAccess { key, .. } => key.clone(),
        Expr::CountStar => "count".to_string(),
        Expr::FunctionCall { name, .. } => name.clone(),
        Expr::Star => "*".to_string(),
        _ => format!("expr_{idx}"),
    }
}

fn is_aggregate_expr(expr: &Expr) -> bool {
    match expr {
        Expr::CountStar => true,
        Expr::FunctionCall { name, .. } => matches!(
            name.to_ascii_lowercase().as_str(),
            "count" | "sum" | "avg" | "min" | "max" | "collect"
        ),
        Expr::IndexAccess { expr, .. } => is_aggregate_expr(expr),
        Expr::ListSlice { expr, .. } => is_aggregate_expr(expr),
        _ => false,
    }
}

fn eval_list_slice(
    base: Value,
    start: Option<&Expr>,
    end: Option<&Expr>,
    row: &Row,
    state: &ClusterState,
    stats: &mut QueryStats,
) -> Result<Value> {
    let items = match base {
        Value::Array(items) => items,
        _ => return Ok(Value::Null),
    };
    let len = items.len() as i64;
    let mut start_idx = 0i64;
    let mut end_idx = len;

    if let Some(start_expr) = start {
        let value = eval_expr(start_expr, row, state, stats)?;
        if value.is_null() {
            start_idx = 0;
        } else if let Some(v) = value.as_i64() {
            start_idx = v;
        } else {
            return Ok(Value::Null);
        }
    }
    if let Some(end_expr) = end {
        let value = eval_expr(end_expr, row, state, stats)?;
        if value.is_null() {
            end_idx = len;
        } else if let Some(v) = value.as_i64() {
            end_idx = v;
        } else {
            return Ok(Value::Null);
        }
    }

    if start_idx < 0 {
        start_idx = 0;
    }
    if end_idx < 0 {
        end_idx = 0;
    }
    if start_idx > len {
        start_idx = len;
    }
    if end_idx > len {
        end_idx = len;
    }
    if end_idx < start_idx {
        end_idx = start_idx;
    }

    let start_usize = start_idx as usize;
    let end_usize = end_idx as usize;
    Ok(Value::Array(items[start_usize..end_usize].to_vec()))
}

fn group_key(values: &[Value]) -> String {
    serde_json::to_string(values).unwrap_or_default()
}

fn distinct_rows(rows: Vec<Row>) -> Vec<Row> {
    let mut seen = std::collections::HashSet::new();
    let mut output = Vec::new();
    for row in rows {
        let key = row_fingerprint(&row);
        if seen.insert(key) {
            output.push(row);
        }
    }
    output
}

fn row_fingerprint(row: &Row) -> String {
    let mut keys: Vec<_> = row.keys().cloned().collect();
    keys.sort();
    let mut map = Map::new();
    for key in keys {
        if let Some(value) = row.get(&key) {
            map.insert(key, value.clone());
        }
    }
    serde_json::to_string(&Value::Object(map)).unwrap_or_default()
}

fn apply_skip_limit(
    mut rows: Vec<Row>,
    skip: Option<&Expr>,
    limit: Option<&Expr>,
    state: &ClusterState,
    stats: &mut QueryStats,
) -> Result<Vec<Row>> {
    if let Some(skip_expr) = skip {
        let skip_count = eval_expr(skip_expr, &Row::new(), state, stats)?
            .as_i64()
            .unwrap_or(0)
            .max(0) as usize;
        if skip_count < rows.len() {
            rows = rows.split_off(skip_count);
        } else {
            rows.clear();
        }
    }

    if let Some(limit_expr) = limit {
        let limit_count = eval_expr(limit_expr, &Row::new(), state, stats)?
            .as_i64()
            .unwrap_or(0)
            .max(0) as usize;
        rows.truncate(limit_count);
    }

    Ok(rows)
}

fn sort_rows(
    rows: Vec<Row>,
    order: &OrderBy,
    state: &ClusterState,
    stats: &mut QueryStats,
) -> Result<Vec<Row>> {
    let mut rows_with_keys = Vec::with_capacity(rows.len());
    for row in rows {
        let mut keys = Vec::new();
        for item in &order.items {
            keys.push(eval_expr(&item.expr, &row, state, stats)?);
        }
        rows_with_keys.push((row, keys));
    }

    rows_with_keys.sort_by(|a, b| compare_keys(&a.1, &b.1, &order.items));

    Ok(rows_with_keys.into_iter().map(|(row, _)| row).collect())
}

fn compare_keys(a: &[Value], b: &[Value], order: &[ariadne_cypher::OrderItem]) -> Ordering {
    for (idx, (left, right)) in a.iter().zip(b.iter()).enumerate() {
        let dir = order.get(idx).map(|o| &o.direction);
        let ord = compare_values(left, right).unwrap_or(Ordering::Equal);
        if ord != Ordering::Equal {
            return match dir {
                Some(ariadne_cypher::SortDirection::Desc) => ord.reverse(),
                _ => ord,
            };
        }
    }
    Ordering::Equal
}

fn compare_values(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::Null, Value::Null) => Some(Ordering::Equal),
        (Value::Null, _) => Some(Ordering::Less),
        (_, Value::Null) => Some(Ordering::Greater),
        (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
        (Value::Number(a), Value::Number(b)) => {
            let la = a.as_f64()?;
            let lb = b.as_f64()?;
            la.partial_cmp(&lb)
        }
        (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

fn eval_exists(
    row: &Row,
    pattern: &Pattern,
    where_clause: Option<&Expr>,
    state: &ClusterState,
    stats: &mut QueryStats,
) -> Result<bool> {
    match pattern {
        Pattern::Node(node) => exists_node_pattern(row, node, where_clause, state, stats),
        Pattern::Relationship(rel) => {
            exists_relationship_pattern(row, rel, where_clause, state, stats)
        }
        Pattern::Path(path) => exists_path_pattern(row, path, where_clause, state, stats),
    }
}

fn exists_node_pattern(
    row: &Row,
    pattern: &ariadne_cypher::NodePattern,
    where_clause: Option<&Expr>,
    state: &ClusterState,
    stats: &mut QueryStats,
) -> Result<bool> {
    let var = pattern.variable.as_ref();
    if let Some(name) = var {
        if let Some(bound) = row.get(name) {
            if let Some(uid) = node_uid_from_value(bound) {
                if let Some(node) = state.node_by_uid(uid) {
                    if matches_labels(node, &pattern.labels)? {
                        return exists_binding(row, Row::new(), where_clause, state, stats);
                    }
                }
            }
            return Ok(false);
        }
    }

    let label_type =
        if pattern.labels.len() == 1 {
            Some(ResourceType::try_new(&pattern.labels[0]).map_err(|_| {
                std::io::Error::other(format!("unknown label: {}", pattern.labels[0]))
            })?)
        } else {
            None
        };
    let candidates: Box<dyn Iterator<Item = &GenericObject>> =
        if let Some(ref expected) = label_type {
            Box::new(state.get_nodes_by_type(expected))
        } else {
            Box::new(state.get_nodes())
        };
    for node in candidates {
        if label_type.is_some() {
            stats.nodes_indexed += 1;
        } else {
            stats.nodes_scanned += 1;
        }
        if label_type.is_none() && !matches_labels(node, &pattern.labels)? {
            continue;
        }
        let mut binding = Row::new();
        if let Some(name) = var {
            binding.insert(name.clone(), node_to_value(node)?);
        }
        if exists_binding(row, binding, where_clause, state, stats)? {
            return Ok(true);
        }
    }

    Ok(false)
}

fn exists_relationship_pattern(
    row: &Row,
    pattern: &RelationshipPattern,
    where_clause: Option<&Expr>,
    state: &ClusterState,
    stats: &mut QueryStats,
) -> Result<bool> {
    let rel_types = &pattern.rel.types;
    let dir = &pattern.rel.direction;

    let left_label_type = if pattern.left.labels.len() == 1 {
        Some(ResourceType::try_new(&pattern.left.labels[0]).map_err(|_| {
            std::io::Error::other(format!("unknown label: {}", pattern.left.labels[0]))
        })?)
    } else {
        None
    };
    let right_label_type = if pattern.right.labels.len() == 1 {
        Some(
            ResourceType::try_new(&pattern.right.labels[0]).map_err(|_| {
                std::io::Error::other(format!("unknown label: {}", pattern.right.labels[0]))
            })?,
        )
    } else {
        None
    };

    if rel_types.is_empty() {
        for edge in state.get_edges() {
            stats.edges_scanned += 1;
            if let Some(rows) = match_edge_row(
                row,
                pattern,
                &edge,
                state,
                dir,
                left_label_type.as_ref(),
                right_label_type.as_ref(),
            )? {
                for binding in rows {
                    if exists_binding(row, binding, where_clause, state, stats)? {
                        return Ok(true);
                    }
                }
            }
        }
    } else {
        let mut seen = std::collections::HashSet::new();
        for rel_type in rel_types {
            if let Some(edge_type) = edge_type_from_str(rel_type) {
                if !seen.insert(edge_type.clone()) {
                    continue;
                }
                for edge in state.get_edges_by_type(&edge_type) {
                    stats.edges_indexed += 1;
                    if let Some(rows) = match_edge_row(
                        row,
                        pattern,
                        &edge,
                        state,
                        dir,
                        left_label_type.as_ref(),
                        right_label_type.as_ref(),
                    )? {
                        for binding in rows {
                            if exists_binding(row, binding, where_clause, state, stats)? {
                                return Ok(true);
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(false)
}

fn exists_path_pattern(
    row: &Row,
    pattern: &PathPattern,
    where_clause: Option<&Expr>,
    state: &ClusterState,
    stats: &mut QueryStats,
) -> Result<bool> {
    let (relationships, _internal_vars) = path_relationships_with_internal_vars(pattern, row);
    let mut bindings = vec![Row::new()];

    for (idx, rel_pattern) in relationships.iter().enumerate() {
        let is_last = idx + 1 == relationships.len();
        let mut next = Vec::new();
        for binding in bindings {
            let combined = combine_row_for_match(row, &binding);
            let matches = match_relationship_pattern(&combined, rel_pattern, state, stats)?;
            for new_binding in matches {
                let mut merged = binding.clone();
                for (key, value) in new_binding {
                    merged.insert(key, value);
                }
                if is_last {
                    if exists_binding(row, merged, where_clause, state, stats)? {
                        return Ok(true);
                    }
                } else {
                    next.push(merged);
                }
            }
        }
        if is_last {
            return Ok(false);
        }
        bindings = next;
        if bindings.is_empty() {
            return Ok(false);
        }
    }

    Ok(false)
}

fn exists_binding(
    base: &Row,
    binding: Row,
    where_clause: Option<&Expr>,
    state: &ClusterState,
    stats: &mut QueryStats,
) -> Result<bool> {
    let Some(merged) = merge_rows(base, &binding) else {
        return Ok(false);
    };
    if let Some(where_clause) = where_clause {
        eval_bool(where_clause, &merged, state, stats)
    } else {
        Ok(true)
    }
}

fn eval_bool(expr: &Expr, row: &Row, state: &ClusterState, stats: &mut QueryStats) -> Result<bool> {
    match eval_expr(expr, row, state, stats)? {
        Value::Bool(b) => Ok(b),
        _ => Ok(false),
    }
}

fn eval_expr(
    expr: &Expr,
    row: &Row,
    state: &ClusterState,
    stats: &mut QueryStats,
) -> Result<Value> {
    match expr {
        Expr::Literal(lit) => literal_to_value(lit, row, state, stats),
        Expr::Variable(name) => Ok(row.get(name).cloned().unwrap_or(Value::Null)),
        Expr::Star => Ok(Value::Null),
        Expr::PropertyAccess { expr, key } => {
            let base = eval_expr(expr, row, state, stats)?;
            Ok(base
                .as_object()
                .and_then(|obj| obj.get(key))
                .cloned()
                .unwrap_or(Value::Null))
        }
        Expr::IndexAccess { expr, index } => {
            let base = eval_expr(expr, row, state, stats)?;
            let idx = eval_expr(index, row, state, stats)?;
            match (base, idx) {
                (Value::Array(items), Value::Number(n)) => {
                    let i = n.as_i64().unwrap_or(-1);
                    if i < 0 {
                        Ok(Value::Null)
                    } else {
                        Ok(items.get(i as usize).cloned().unwrap_or(Value::Null))
                    }
                }
                (Value::Object(map), Value::String(key)) => {
                    Ok(map.get(&key).cloned().unwrap_or(Value::Null))
                }
                _ => Ok(Value::Null),
            }
        }
        Expr::ListSlice { expr, start, end } => {
            let base = eval_expr(expr, row, state, stats)?;
            eval_list_slice(base, start.as_deref(), end.as_deref(), row, state, stats)
        }
        Expr::UnaryOp { op, expr } => {
            let value = eval_expr(expr, row, state, stats)?;
            match op {
                ariadne_cypher::UnaryOp::Not => Ok(Value::Bool(!value.as_bool().unwrap_or(false))),
                ariadne_cypher::UnaryOp::Neg => Ok(Value::from(-value.as_f64().unwrap_or(0.0))),
                ariadne_cypher::UnaryOp::Pos => Ok(Value::from(value.as_f64().unwrap_or(0.0))),
            }
        }
        Expr::BinaryOp { op, left, right } => eval_binary(op, left, right, row, state, stats),
        Expr::IsNull { expr, negated } => {
            let value = eval_expr(expr, row, state, stats)?;
            let is_null = value.is_null();
            Ok(Value::Bool(if *negated { !is_null } else { is_null }))
        }
        Expr::In { expr, list } => {
            let value = eval_expr(expr, row, state, stats)?;
            let list_value = eval_expr(list, row, state, stats)?;
            let contains = match list_value {
                Value::Array(items) => items.iter().any(|item| item == &value),
                _ => false,
            };
            Ok(Value::Bool(contains))
        }
        Expr::HasLabel { expr, labels } => {
            let value = eval_expr(expr, row, state, stats)?;
            let label = match value {
                Value::Object(map) => map
                    .get("kind")
                    .and_then(|v| v.as_str())
                    .or_else(|| map.get("resource_type").and_then(|v| v.as_str()))
                    .map(|v| v.to_string()),
                _ => None,
            };
            let matches = if let Some(label) = label {
                labels.iter().all(|l| l == &label)
            } else {
                false
            };
            Ok(Value::Bool(matches))
        }
        Expr::Exists {
            pattern,
            where_clause,
        } => {
            let exists = eval_exists(row, pattern, where_clause.as_deref(), state, stats)?;
            Ok(Value::Bool(exists))
        }
        Expr::ListComprehension {
            variable,
            list,
            where_clause,
            map,
        } => eval_list_comprehension(
            variable,
            list,
            where_clause.as_deref(),
            map,
            row,
            state,
            stats,
        ),
        Expr::Quantifier {
            kind,
            variable,
            list,
            where_clause,
        } => eval_quantifier(
            kind,
            variable,
            list,
            where_clause.as_deref(),
            row,
            state,
            stats,
        ),
        Expr::Case {
            base,
            alternatives,
            else_expr,
        } => {
            if let Some(base) = base {
                let base_value = eval_expr(base, row, state, stats)?;
                for (when_expr, then_expr) in alternatives {
                    let when_value = eval_expr(when_expr, row, state, stats)?;
                    let matches = compare_values(&base_value, &when_value)
                        .map(|ord| ord == Ordering::Equal)
                        .unwrap_or(false);
                    if matches {
                        return eval_expr(then_expr, row, state, stats);
                    }
                }
            } else {
                for (when_expr, then_expr) in alternatives {
                    if eval_bool(when_expr, row, state, stats)? {
                        return eval_expr(then_expr, row, state, stats);
                    }
                }
            }
            if let Some(else_expr) = else_expr {
                eval_expr(else_expr, row, state, stats)
            } else {
                Ok(Value::Null)
            }
        }
        Expr::FunctionCall { name, args } => eval_function(name, args, row, state, stats),
        Expr::CountStar => Err(std::io::Error::other("count(*) not valid here").into()),
        Expr::Parameter(name) => Err(std::io::Error::other(format!(
            "parameters not supported in engine: ${name}"
        ))
        .into()),
    }
}

fn eval_list_comprehension(
    variable: &str,
    list_expr: &Expr,
    where_clause: Option<&Expr>,
    map_expr: &Expr,
    row: &Row,
    state: &ClusterState,
    stats: &mut QueryStats,
) -> Result<Value> {
    let list_value = eval_expr(list_expr, row, state, stats)?;
    let items = match list_value {
        Value::Array(items) => items,
        _ => return Ok(Value::Array(Vec::new())),
    };
    let mut output = Vec::new();
    for item in items {
        let mut scoped = row.clone();
        scoped.insert(variable.to_string(), item);
        if let Some(where_clause) = where_clause {
            if !eval_bool(where_clause, &scoped, state, stats)? {
                continue;
            }
        }
        output.push(eval_expr(map_expr, &scoped, state, stats)?);
    }
    Ok(Value::Array(output))
}

fn eval_quantifier(
    kind: &ariadne_cypher::QuantifierKind,
    variable: &str,
    list_expr: &Expr,
    where_clause: Option<&Expr>,
    row: &Row,
    state: &ClusterState,
    stats: &mut QueryStats,
) -> Result<Value> {
    let list_value = eval_expr(list_expr, row, state, stats)?;
    let items = match list_value {
        Value::Array(items) => items,
        _ => return Ok(Value::Bool(false)),
    };

    let mut matches = 0usize;
    for item in items {
        let mut scoped = row.clone();
        scoped.insert(variable.to_string(), item.clone());
        let passed = if let Some(where_clause) = where_clause {
            eval_bool(where_clause, &scoped, state, stats)?
        } else {
            item.as_bool().unwrap_or(false)
        };

        match kind {
            ariadne_cypher::QuantifierKind::Any => {
                if passed {
                    return Ok(Value::Bool(true));
                }
            }
            ariadne_cypher::QuantifierKind::All => {
                if !passed {
                    return Ok(Value::Bool(false));
                }
            }
            ariadne_cypher::QuantifierKind::None => {
                if passed {
                    return Ok(Value::Bool(false));
                }
            }
            ariadne_cypher::QuantifierKind::Single => {
                if passed {
                    matches += 1;
                    if matches > 1 {
                        return Ok(Value::Bool(false));
                    }
                }
            }
        }
    }

    let result = match kind {
        ariadne_cypher::QuantifierKind::Any => false,
        ariadne_cypher::QuantifierKind::All => true,
        ariadne_cypher::QuantifierKind::None => true,
        ariadne_cypher::QuantifierKind::Single => matches == 1,
    };
    Ok(Value::Bool(result))
}

fn eval_function(
    name: &str,
    args: &[Expr],
    row: &Row,
    state: &ClusterState,
    stats: &mut QueryStats,
) -> Result<Value> {
    let lower = name.to_ascii_lowercase();
    match lower.as_str() {
        "size" => {
            let target = args
                .first()
                .ok_or_else(|| std::io::Error::other("size requires one argument"))?;
            let value = eval_expr(target, row, state, stats)?;
            let size = match value {
                Value::Array(items) => items.len() as i64,
                Value::String(s) => s.chars().count() as i64,
                Value::Object(map) => map.len() as i64,
                _ => 0,
            };
            Ok(Value::from(size))
        }
        "lower" | "upper" => {
            let target = args
                .first()
                .ok_or_else(|| std::io::Error::other("lower/upper require one argument"))?;
            let value = eval_expr(target, row, state, stats)?;
            let text = value.as_str().unwrap_or_default();
            let out = if lower == "lower" {
                text.to_ascii_lowercase()
            } else {
                text.to_ascii_uppercase()
            };
            Ok(Value::String(out))
        }
        "coalesce" => {
            for arg in args {
                let value = eval_expr(arg, row, state, stats)?;
                if !value.is_null() {
                    return Ok(value);
                }
            }
            Ok(Value::Null)
        }
        "tostring" => {
            let target = args
                .first()
                .ok_or_else(|| std::io::Error::other("toString requires one argument"))?;
            let value = eval_expr(target, row, state, stats)?;
            Ok(Value::String(match value {
                Value::String(s) => s,
                other => other.to_string(),
            }))
        }
        "tointeger" | "toint" => {
            let target = args
                .first()
                .ok_or_else(|| std::io::Error::other("toInteger requires one argument"))?;
            let value = eval_expr(target, row, state, stats)?;
            let num = match value {
                Value::Number(n) => n.as_i64().unwrap_or(0),
                Value::String(s) => s.parse::<i64>().unwrap_or(0),
                Value::Bool(b) => {
                    if b {
                        1
                    } else {
                        0
                    }
                }
                _ => 0,
            };
            Ok(Value::from(num))
        }
        "tofloat" => {
            let target = args
                .first()
                .ok_or_else(|| std::io::Error::other("toFloat requires one argument"))?;
            let value = eval_expr(target, row, state, stats)?;
            let num = match value {
                Value::Number(n) => n.as_f64().unwrap_or(0.0),
                Value::String(s) => s.parse::<f64>().unwrap_or(0.0),
                Value::Bool(b) => {
                    if b {
                        1.0
                    } else {
                        0.0
                    }
                }
                _ => 0.0,
            };
            Ok(Value::from(num))
        }
        "labels" => {
            let target = args
                .first()
                .ok_or_else(|| std::io::Error::other("labels requires one argument"))?;
            let value = eval_expr(target, row, state, stats)?;
            match value {
                Value::Object(map) => {
                    if let Some(label) = map
                        .get("kind")
                        .and_then(|v| v.as_str())
                        .map(|v| v.to_string())
                        .or_else(|| {
                            map.get("resource_type")
                                .and_then(|v| v.as_str())
                                .map(|v| v.to_string())
                        })
                    {
                        Ok(Value::Array(vec![Value::String(label)]))
                    } else {
                        Ok(Value::Array(vec![]))
                    }
                }
                Value::Null => Ok(Value::Array(vec![])),
                _ => Ok(Value::Array(vec![])),
            }
        }
        "replace" => {
            if args.len() < 3 {
                return Err(std::io::Error::other("replace requires three arguments").into());
            }
            let value = eval_expr(&args[0], row, state, stats)?;
            let search = eval_expr(&args[1], row, state, stats)?;
            let replacement = eval_expr(&args[2], row, state, stats)?;
            if value.is_null() || search.is_null() || replacement.is_null() {
                return Ok(Value::Null);
            }
            let source = value_to_string(&value);
            let needle = value_to_string(&search);
            let repl = value_to_string(&replacement);
            Ok(Value::String(source.replace(&needle, &repl)))
        }
        "count" | "sum" | "avg" | "min" | "max" | "collect" => {
            Err(std::io::Error::other("aggregate functions must appear in projection").into())
        }
        _ => Err(std::io::Error::other(format!("unsupported function in engine: {name}")).into()),
    }
}

fn eval_binary(
    op: &ariadne_cypher::BinaryOp,
    left: &Expr,
    right: &Expr,
    row: &Row,
    state: &ClusterState,
    stats: &mut QueryStats,
) -> Result<Value> {
    use ariadne_cypher::BinaryOp::*;
    match op {
        Or => Ok(Value::Bool(
            eval_bool(left, row, state, stats)? || eval_bool(right, row, state, stats)?,
        )),
        And => Ok(Value::Bool(
            eval_bool(left, row, state, stats)? && eval_bool(right, row, state, stats)?,
        )),
        Xor => Ok(Value::Bool(
            eval_bool(left, row, state, stats)? ^ eval_bool(right, row, state, stats)?,
        )),
        Eq | Neq | Lt | Gt | Lte | Gte => {
            let l = eval_expr(left, row, state, stats)?;
            let r = eval_expr(right, row, state, stats)?;
            let cmp = compare_values(&l, &r);
            let result = match op {
                Eq => cmp.map(|c| c == Ordering::Equal).unwrap_or(false),
                Neq => cmp.map(|c| c != Ordering::Equal).unwrap_or(true),
                Lt => cmp.map(|c| c == Ordering::Less).unwrap_or(false),
                Gt => cmp.map(|c| c == Ordering::Greater).unwrap_or(false),
                Lte => cmp.map(|c| c != Ordering::Greater).unwrap_or(false),
                Gte => cmp.map(|c| c != Ordering::Less).unwrap_or(false),
                _ => false,
            };
            Ok(Value::Bool(result))
        }
        StartsWith | EndsWith | Contains => {
            let l = eval_expr(left, row, state, stats)?;
            let r = eval_expr(right, row, state, stats)?;
            if l.is_null() || r.is_null() {
                return Ok(Value::Bool(false));
            }
            let left_str = value_to_string(&l);
            let right_str = value_to_string(&r);
            let result = match op {
                StartsWith => left_str.starts_with(&right_str),
                EndsWith => left_str.ends_with(&right_str),
                Contains => left_str.contains(&right_str),
                _ => false,
            };
            Ok(Value::Bool(result))
        }
        Add | Sub | Mul | Div | Mod | Pow => {
            let l = eval_expr(left, row, state, stats)?.as_f64().unwrap_or(0.0);
            let r = eval_expr(right, row, state, stats)?.as_f64().unwrap_or(0.0);
            let value = match op {
                Add => l + r,
                Sub => l - r,
                Mul => l * r,
                Div => l / r,
                Mod => l % r,
                Pow => l.powf(r),
                _ => 0.0,
            };
            Ok(Value::from(value))
        }
    }
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn literal_to_value(
    lit: &Literal,
    row: &Row,
    state: &ClusterState,
    stats: &mut QueryStats,
) -> Result<Value> {
    match lit {
        Literal::String(s) => Ok(Value::String(s.clone())),
        Literal::Integer(i) => Ok(Value::from(*i)),
        Literal::Float(f) => Ok(Value::from(*f)),
        Literal::Boolean(b) => Ok(Value::from(*b)),
        Literal::Null => Ok(Value::Null),
        Literal::List(items) => {
            let mut values = Vec::new();
            for expr in items {
                values.push(eval_expr(expr, row, state, stats)?);
            }
            Ok(Value::Array(values))
        }
        Literal::Map(entries) => {
            let mut map = Map::new();
            for (k, v) in entries {
                map.insert(k.clone(), eval_expr(v, row, state, stats)?);
            }
            Ok(Value::Object(map))
        }
    }
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
        ResourceAttributes::Logs { logs } => serde_json::to_value(logs.as_ref())?,
        ResourceAttributes::Container { container } => serde_json::to_value(container.as_ref())?,
    };

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
mod tests {
    use super::*;
    use crate::state::ClusterState;
    use crate::types::{Cluster, Edge, ObjectIdentifier};
    use k8s_openapi::api::apps::v1::{Deployment, ReplicaSet};
    use k8s_openapi::api::core::v1::{
        ContainerState, ContainerStateTerminated, ContainerStatus, Pod, PodStatus,
    };
    use k8s_openapi::apimachinery::pkg::version::Info;
    use std::sync::{Arc, Mutex};

    fn dummy_cluster() -> Cluster {
        let id = ObjectIdentifier {
            uid: "cluster-uid".to_string(),
            name: "test".to_string(),
            namespace: None,
            resource_version: None,
        };
        Cluster::new(id, "https://example.invalid", Info::default())
    }

    fn pod(uid: &str, name: &str, namespace: &str) -> GenericObject {
        let mut pod = Pod::default();
        pod.metadata = ObjectMeta {
            uid: Some(uid.to_string()),
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        };
        GenericObject {
            id: ObjectIdentifier {
                uid: uid.to_string(),
                name: name.to_string(),
                namespace: Some(namespace.to_string()),
                resource_version: None,
            },
            resource_type: ResourceType::Pod,
            attributes: Some(Box::new(ResourceAttributes::Pod { pod: Arc::new(pod) })),
        }
    }

    fn pod_with_container_status(
        uid: &str,
        name: &str,
        namespace: &str,
        reason: &str,
    ) -> GenericObject {
        let mut pod = Pod::default();
        pod.metadata = ObjectMeta {
            uid: Some(uid.to_string()),
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        };
        let terminated = ContainerStateTerminated {
            reason: Some(reason.to_string()),
            exit_code: 137,
            message: Some("terminated".to_string()),
            ..Default::default()
        };
        let last_state = ContainerState {
            terminated: Some(terminated),
            ..Default::default()
        };
        let status = ContainerStatus {
            name: "main".to_string(),
            last_state: Some(last_state),
            ..Default::default()
        };
        pod.status = Some(PodStatus {
            container_statuses: Some(vec![status]),
            ..Default::default()
        });
        GenericObject {
            id: ObjectIdentifier {
                uid: uid.to_string(),
                name: name.to_string(),
                namespace: Some(namespace.to_string()),
                resource_version: None,
            },
            resource_type: ResourceType::Pod,
            attributes: Some(Box::new(ResourceAttributes::Pod { pod: Arc::new(pod) })),
        }
    }

    fn deployment(uid: &str, name: &str, namespace: &str) -> GenericObject {
        let mut dep = Deployment::default();
        dep.metadata = ObjectMeta {
            uid: Some(uid.to_string()),
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        };
        GenericObject {
            id: ObjectIdentifier {
                uid: uid.to_string(),
                name: name.to_string(),
                namespace: Some(namespace.to_string()),
                resource_version: None,
            },
            resource_type: ResourceType::Deployment,
            attributes: Some(Box::new(ResourceAttributes::Deployment {
                deployment: Arc::new(dep),
            })),
        }
    }

    fn replica_set(uid: &str, name: &str, namespace: &str) -> GenericObject {
        let mut rs = ReplicaSet::default();
        rs.metadata = ObjectMeta {
            uid: Some(uid.to_string()),
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        };
        GenericObject {
            id: ObjectIdentifier {
                uid: uid.to_string(),
                name: name.to_string(),
                namespace: Some(namespace.to_string()),
                resource_version: None,
            },
            resource_type: ResourceType::ReplicaSet,
            attributes: Some(Box::new(ResourceAttributes::ReplicaSet {
                replica_set: Arc::new(rs),
            })),
        }
    }

    #[test]
    fn executes_match_where_return() {
        let mut state = ClusterState::new(dummy_cluster());
        state.add_node(pod("p1", "pod-one", "ns1"));
        state.add_node(pod("p2", "pod-two", "ns2"));

        let query = parse_query(
            "MATCH (p:Pod) WHERE p.metadata.name = 'pod-one' RETURN p.metadata.name AS name",
        )
        .unwrap();
        validate_query(&query, ValidationMode::Engine).unwrap();

        let mut stats = QueryStats::default();
        let results = execute_query_ast(&query, &state, &mut stats).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].get("name").and_then(|v| v.as_str()),
            Some("pod-one")
        );
    }

    #[test]
    fn executes_count() {
        let mut state = ClusterState::new(dummy_cluster());
        state.add_node(pod("p1", "pod-one", "ns1"));
        state.add_node(pod("p2", "pod-two", "ns2"));

        let query = parse_query("MATCH (p:Pod) RETURN count(p) AS total").unwrap();
        validate_query(&query, ValidationMode::Engine).unwrap();

        let mut stats = QueryStats::default();
        let results = execute_query_ast(&query, &state, &mut stats).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("total").and_then(|v| v.as_i64()), Some(2));
    }

    #[test]
    fn executes_relationship_match() {
        let mut state = ClusterState::new(dummy_cluster());
        let dep = deployment("d1", "deploy", "ns1");
        let rs = replica_set("r1", "rs", "ns1");
        state.add_node(dep);
        state.add_node(rs);
        state.add_edge(
            "d1",
            ResourceType::Deployment,
            "r1",
            ResourceType::ReplicaSet,
            Edge::Manages,
        );

        let query = parse_query(
            "MATCH (d:Deployment)-[:Manages]->(r:ReplicaSet) RETURN r.metadata.name AS name",
        )
        .unwrap();
        validate_query(&query, ValidationMode::Engine).unwrap();

        let mut stats = QueryStats::default();
        let results = execute_query_ast(&query, &state, &mut stats).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("name").and_then(|v| v.as_str()), Some("rs"));
    }

    #[test]
    fn executes_multi_hop_relationship_match() {
        let mut state = ClusterState::new(dummy_cluster());
        let dep = deployment("d1", "deploy", "ns1");
        let rs1 = replica_set("r1", "rs1", "ns1");
        let rs2 = replica_set("r2", "rs2", "ns1");
        let pod1 = pod("p1", "pod1", "ns1");
        let pod2 = pod("p2", "pod2", "ns1");
        state.add_node(dep);
        state.add_node(rs1);
        state.add_node(rs2);
        state.add_node(pod1);
        state.add_node(pod2);
        state.add_edge(
            "d1",
            ResourceType::Deployment,
            "r1",
            ResourceType::ReplicaSet,
            Edge::Manages,
        );
        state.add_edge(
            "r1",
            ResourceType::ReplicaSet,
            "p1",
            ResourceType::Pod,
            Edge::Manages,
        );
        state.add_edge(
            "r2",
            ResourceType::ReplicaSet,
            "p2",
            ResourceType::Pod,
            Edge::Manages,
        );

        let query = parse_query(
            "MATCH (d:Deployment)-[:Manages]->(:ReplicaSet)-[:Manages]->(p:Pod) RETURN p.metadata.name AS name",
        )
        .unwrap();
        validate_query(&query, ValidationMode::Engine).unwrap();

        let mut stats = QueryStats::default();
        let results = execute_query_ast(&query, &state, &mut stats).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].get("name").and_then(|v| v.as_str()),
            Some("pod1")
        );
    }

    #[test]
    fn executes_relationship_variable() {
        let mut state = ClusterState::new(dummy_cluster());
        let dep = deployment("d1", "deploy", "ns1");
        let rs = replica_set("r1", "rs", "ns1");
        state.add_node(dep);
        state.add_node(rs);
        state.add_edge(
            "d1",
            ResourceType::Deployment,
            "r1",
            ResourceType::ReplicaSet,
            Edge::Manages,
        );

        let query =
            parse_query("MATCH (d:Deployment)-[r:Manages]->(s:ReplicaSet) RETURN r.type AS kind")
                .unwrap();
        validate_query(&query, ValidationMode::Engine).unwrap();

        let mut stats = QueryStats::default();
        let results = execute_query_ast(&query, &state, &mut stats).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].get("kind").and_then(|v| v.as_str()),
            Some("Manages")
        );
    }

    #[test]
    fn executes_unwind_with_aggregate() {
        let state = ClusterState::new(dummy_cluster());
        let query =
            parse_query("UNWIND [1,2,3] AS x WITH x RETURN sum(x) AS total, collect(x) AS items")
                .unwrap();
        validate_query(&query, ValidationMode::Engine).unwrap();

        let mut stats = QueryStats::default();
        let results = execute_query_ast(&query, &state, &mut stats).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("total").and_then(|v| v.as_f64()), Some(6.0));
        let items = results[0]
            .get("items")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap();
        assert_eq!(items.len(), 3);
    }

    #[test]
    fn executes_multi_match() {
        let mut state = ClusterState::new(dummy_cluster());
        state.add_node(pod("p1", "pod-one", "ns1"));
        state.add_node(pod("p2", "pod-two", "ns1"));

        let query = parse_query("MATCH (p:Pod) MATCH (q:Pod) RETURN count(*) AS total").unwrap();
        validate_query(&query, ValidationMode::Engine).unwrap();

        let mut stats = QueryStats::default();
        let results = execute_query_ast(&query, &state, &mut stats).unwrap();
        assert_eq!(results[0].get("total").and_then(|v| v.as_i64()), Some(4));
    }

    #[test]
    fn backend_executes_query() {
        let mut state = ClusterState::new(dummy_cluster());
        state.add_node(pod("p1", "pod-one", "ns1"));
        let shared = Arc::new(Mutex::new(state));

        let backend = InMemoryBackend::new();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            backend.create(shared.clone()).await.unwrap();
            let results = backend
                .execute_query("MATCH (p:Pod) RETURN count(p) AS total".to_string())
                .await
                .unwrap();
            assert_eq!(results[0].get("total").and_then(|v| v.as_i64()), Some(1));
        });
    }

    #[test]
    fn executes_string_predicate() {
        let mut state = ClusterState::new(dummy_cluster());
        state.add_node(pod("p1", "pod-one", "ns1"));
        state.add_node(pod("p2", "pod-two", "ns1"));

        let query = parse_query(
            "MATCH (p:Pod) WHERE p.metadata.name ENDS WITH 'one' RETURN p.metadata.name AS name",
        )
        .unwrap();
        validate_query(&query, ValidationMode::Engine).unwrap();

        let mut stats = QueryStats::default();
        let results = execute_query_ast(&query, &state, &mut stats).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].get("name").and_then(|v| v.as_str()),
            Some("pod-one")
        );
    }

    #[test]
    fn executes_case_expression() {
        let state = ClusterState::new(dummy_cluster());
        let query =
            parse_query("UNWIND [1] AS x WITH CASE WHEN x = 1 THEN 5 ELSE 0 END AS v RETURN v")
                .unwrap();
        validate_query(&query, ValidationMode::Engine).unwrap();

        let mut stats = QueryStats::default();
        let results = execute_query_ast(&query, &state, &mut stats).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("v").and_then(|v| v.as_i64()), Some(5));
    }

    #[test]
    fn executes_replace_function() {
        let state = ClusterState::new(dummy_cluster());
        let query = parse_query("RETURN replace('250m','m','') AS v").unwrap();
        validate_query(&query, ValidationMode::Engine).unwrap();

        let mut stats = QueryStats::default();
        let results = execute_query_ast(&query, &state, &mut stats).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("v").and_then(|v| v.as_str()), Some("250"));
    }

    #[test]
    fn executes_labels_function() {
        let mut state = ClusterState::new(dummy_cluster());
        state.add_node(pod("p1", "pod-one", "ns1"));

        let query = parse_query("MATCH (p:Pod) RETURN labels(p) AS labels").unwrap();
        validate_query(&query, ValidationMode::Engine).unwrap();

        let mut stats = QueryStats::default();
        let results = execute_query_ast(&query, &state, &mut stats).unwrap();
        let labels = results[0].get("labels").and_then(|v| v.as_array()).cloned();
        assert_eq!(labels, Some(vec![Value::String("Pod".to_string())]));
    }

    #[test]
    fn executes_mixed_multiplicative_expression() {
        let state = ClusterState::new(dummy_cluster());
        let query = parse_query("RETURN 1000 / 1024 / 1024 AS v").unwrap();
        validate_query(&query, ValidationMode::Engine).unwrap();

        let mut stats = QueryStats::default();
        let results = execute_query_ast(&query, &state, &mut stats).unwrap();
        let v = results[0].get("v").and_then(|v| v.as_f64()).unwrap();
        let expected = 1000.0 / 1024.0 / 1024.0;
        assert!((v - expected).abs() < 1e-9, "expected {expected}, got {v}");
    }

    #[test]
    fn executes_label_predicate_filter() {
        let mut state = ClusterState::new(dummy_cluster());
        state.add_node(pod("p1", "pod-one", "ns1"));
        state.add_node(deployment("d1", "deploy", "ns1"));

        let query = parse_query("MATCH (n) WHERE n:Pod RETURN count(n) AS total").unwrap();
        validate_query(&query, ValidationMode::Engine).unwrap();

        let mut stats = QueryStats::default();
        let results = execute_query_ast(&query, &state, &mut stats).unwrap();
        assert_eq!(results[0].get("total").and_then(|v| v.as_i64()), Some(1));
    }

    #[test]
    fn executes_label_predicate_with_or() {
        let mut state = ClusterState::new(dummy_cluster());
        state.add_node(pod("p1", "pod-one", "ns1"));
        state.add_node(pod("p2", "pod-two", "ns1"));
        state.add_node(deployment("d1", "deploy", "ns1"));

        let query =
            parse_query("MATCH (n) WHERE n:Pod OR n:Deployment RETURN count(n) AS total").unwrap();
        validate_query(&query, ValidationMode::Engine).unwrap();

        let mut stats = QueryStats::default();
        let results = execute_query_ast(&query, &state, &mut stats).unwrap();
        assert_eq!(results[0].get("total").and_then(|v| v.as_i64()), Some(3));
    }

    #[test]
    fn executes_exists_subquery() {
        let mut state = ClusterState::new(dummy_cluster());
        state.add_node(deployment("d1", "deploy", "ns1"));
        state.add_node(replica_set("r1", "rs", "ns1"));
        state.add_edge(
            "d1",
            ResourceType::Deployment,
            "r1",
            ResourceType::ReplicaSet,
            Edge::Manages,
        );

        let query = parse_query(
            "MATCH (d:Deployment) WHERE exists { (d)-[:Manages]->(r:ReplicaSet) } RETURN count(d) AS total",
        )
        .unwrap();
        validate_query(&query, ValidationMode::Engine).unwrap();

        let mut stats = QueryStats::default();
        let results = execute_query_ast(&query, &state, &mut stats).unwrap();
        assert_eq!(results[0].get("total").and_then(|v| v.as_i64()), Some(1));
    }

    #[test]
    fn executes_not_exists_subquery() {
        let mut state = ClusterState::new(dummy_cluster());
        state.add_node(deployment("d1", "deploy-a", "ns1"));
        state.add_node(deployment("d2", "deploy-b", "ns1"));
        state.add_node(replica_set("r1", "rs", "ns1"));
        state.add_edge(
            "d1",
            ResourceType::Deployment,
            "r1",
            ResourceType::ReplicaSet,
            Edge::Manages,
        );

        let query = parse_query(
            "MATCH (d:Deployment) WHERE NOT exists { (d)-[:Manages]->(r:ReplicaSet) } RETURN count(d) AS total",
        )
        .unwrap();
        validate_query(&query, ValidationMode::Engine).unwrap();

        let mut stats = QueryStats::default();
        let results = execute_query_ast(&query, &state, &mut stats).unwrap();
        assert_eq!(results[0].get("total").and_then(|v| v.as_i64()), Some(1));
    }

    #[test]
    fn executes_exists_subquery_with_where() {
        let mut state = ClusterState::new(dummy_cluster());
        state.add_node(deployment("d1", "deploy", "ns1"));
        state.add_node(replica_set("r1", "rs", "ns1"));
        state.add_edge(
            "d1",
            ResourceType::Deployment,
            "r1",
            ResourceType::ReplicaSet,
            Edge::Manages,
        );

        let query = parse_query(
            "MATCH (d:Deployment) WHERE exists { (d)-[:Manages]->(r:ReplicaSet) WHERE r.metadata.name = 'rs' } RETURN count(d) AS total",
        )
        .unwrap();
        validate_query(&query, ValidationMode::Engine).unwrap();

        let mut stats = QueryStats::default();
        let results = execute_query_ast(&query, &state, &mut stats).unwrap();
        assert_eq!(results[0].get("total").and_then(|v| v.as_i64()), Some(1));

        let query = parse_query(
            "MATCH (d:Deployment) WHERE exists { (d)-[:Manages]->(r:ReplicaSet) WHERE r.metadata.name = 'nope' } RETURN count(d) AS total",
        )
        .unwrap();
        validate_query(&query, ValidationMode::Engine).unwrap();
        let results = execute_query_ast(&query, &state, &mut stats).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn multi_hop_match_first_match_filter() {
        let mut state = ClusterState::new(dummy_cluster());
        state.add_node(deployment("d1", "deploy", "ns1"));
        state.add_node(deployment("d2", "deploy-2", "ns1"));
        state.add_node(replica_set("r1", "rs", "ns1"));
        state.add_node(pod("p1", "pod-one", "ns1"));
        state.add_edge(
            "d1",
            ResourceType::Deployment,
            "r1",
            ResourceType::ReplicaSet,
            Edge::Manages,
        );
        state.add_edge(
            "r1",
            ResourceType::ReplicaSet,
            "p1",
            ResourceType::Pod,
            Edge::Manages,
        );

        let query = parse_query(
            "MATCH (d:Deployment) MATCH (d)-[:Manages]->(:ReplicaSet)-[:Manages]->(:Pod) RETURN d.metadata.name AS name",
        )
        .unwrap();
        validate_query(&query, ValidationMode::Engine).unwrap();

        let mut stats = QueryStats::default();
        let results = execute_query_ast(&query, &state, &mut stats).unwrap();
        let names: Vec<_> = results
            .into_iter()
            .filter_map(|row| {
                row.get("name")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect();
        assert_eq!(names, vec!["deploy".to_string()]);
    }

    #[test]
    fn executes_quantifiers() {
        let state = ClusterState::new(dummy_cluster());
        let query = parse_query(
            "RETURN any(x IN [1,2,3] WHERE x = 2) AS any, \
             all(x IN [1,2,3] WHERE x > 0) AS all, \
             none(x IN [1,2,3] WHERE x < 0) AS none, \
             single(x IN [1,2,3] WHERE x = 2) AS single",
        )
        .unwrap();
        validate_query(&query, ValidationMode::Engine).unwrap();

        let mut stats = QueryStats::default();
        let results = execute_query_ast(&query, &state, &mut stats).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("any").and_then(|v| v.as_bool()), Some(true));
        assert_eq!(results[0].get("all").and_then(|v| v.as_bool()), Some(true));
        assert_eq!(results[0].get("none").and_then(|v| v.as_bool()), Some(true));
        assert_eq!(
            results[0].get("single").and_then(|v| v.as_bool()),
            Some(true)
        );
    }

    #[test]
    fn executes_quantifier_with_list_comprehension_smoke() {
        let mut state = ClusterState::new(dummy_cluster());
        state.add_node(pod_with_container_status(
            "p1",
            "oom-pod",
            "ns1",
            "OOMKilled",
        ));
        state.add_node(pod_with_container_status(
            "p2",
            "ok-pod",
            "ns1",
            "Completed",
        ));

        let query = parse_query(
            "MATCH (p:Pod)\n\
             WHERE ANY(cs IN p['status']['containerStatuses'] WHERE cs['lastState']['terminated']['reason'] = 'OOMKilled')\n\
             RETURN p['metadata']['namespace'] AS namespace,\n\
                    p['metadata']['name'] AS pod,\n\
                    [cs IN p['status']['containerStatuses'] WHERE cs['lastState']['terminated']['reason'] = 'OOMKilled' | {\n\
                      container: cs['name'],\n\
                      exitCode: cs['lastState']['terminated']['exitCode'],\n\
                      finishedAt: cs['lastState']['terminated']['finishedAt'],\n\
                      message: cs['lastState']['terminated']['message']\n\
                    }] AS oom_killed_containers\n\
             ORDER BY namespace, pod",
        )
        .unwrap();
        validate_query(&query, ValidationMode::Engine).unwrap();

        let mut stats = QueryStats::default();
        let results = execute_query_ast(&query, &state, &mut stats).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].get("pod").and_then(|v| v.as_str()),
            Some("oom-pod")
        );
        let containers = results[0]
            .get("oom_killed_containers")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert_eq!(containers.len(), 1);
        let container = containers[0].as_object().cloned().unwrap_or_default();
        assert_eq!(
            container.get("container").and_then(|v| v.as_str()),
            Some("main")
        );
        assert_eq!(
            container.get("exitCode").and_then(|v| v.as_i64()),
            Some(137)
        );
    }

    #[test]
    fn executes_collect_slice_and_index() {
        let mut state = ClusterState::new(dummy_cluster());
        state.add_node(pod("p1", "alpha", "ns1"));
        state.add_node(pod("p2", "beta", "ns1"));
        state.add_node(pod("p3", "gamma", "ns1"));

        let query = parse_query(
            "MATCH (p:Pod)\n\
             WITH p ORDER BY p.metadata.name\n\
             RETURN collect(p.metadata.name)[0..2] AS names, collect(p.metadata.name)[1] AS second",
        )
        .unwrap();
        validate_query(&query, ValidationMode::Engine).unwrap();

        let mut stats = QueryStats::default();
        let results = execute_query_ast(&query, &state, &mut stats).unwrap();
        assert_eq!(results.len(), 1);
        let names = results[0]
            .get("names")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert_eq!(
            names,
            vec![
                Value::String("alpha".to_string()),
                Value::String("beta".to_string())
            ]
        );
        assert_eq!(
            results[0].get("second").and_then(|v| v.as_str()),
            Some("beta")
        );
    }
}
