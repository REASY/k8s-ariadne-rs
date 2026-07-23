//! Pure conversion of backend records into GUI result and inspector models.
//!
//! These functions must not mutate frontend state or depend on a rendering
//! framework so both desktop implementations classify results identically.

use std::collections::HashMap;

use serde_json::{Map, Value};
use strum::IntoEnumIterator;

use ariadne_core::types::ResourceType;

use crate::agent::ConversationTurn;
use crate::gui_shared::{GraphEdge, GraphNode, InspectorValue, ResultPayload, RowCard};

pub(crate) fn classify_result(records: &[Value]) -> ResultPayload {
    if records.is_empty() {
        return ResultPayload::Empty;
    }

    if let Some(graph) = parse_graph_payload(records) {
        return graph;
    }

    if records.len() == 1
        && let Some(obj) = records[0].as_object()
        && obj.len() == 1
        && let Some((label, value)) = obj.iter().next()
    {
        let value_str = format_value(value);
        return ResultPayload::Metric {
            label: label.clone(),
            value: value_str,
            unit: None,
        };
    }

    if records.iter().all(|value| value.is_object()) {
        let rows = records
            .iter()
            .filter_map(|value| value.as_object())
            .map(summarize_row)
            .collect();
        return ResultPayload::List { rows };
    }

    ResultPayload::Raw {
        text: serde_json::to_string_pretty(records).unwrap_or_else(|_| "[]".to_string()),
    }
}

pub(crate) fn summarize_records(records: &[Value]) -> String {
    if records.is_empty() {
        return "rows=0".to_string();
    }

    let rows = records.len();
    let mut columns: Vec<String> = records
        .first()
        .and_then(|v| v.as_object())
        .map(|obj| obj.keys().cloned().collect())
        .unwrap_or_default();
    columns.sort();

    let mut summary = format!("rows={rows}");
    if !columns.is_empty() {
        summary.push_str(", columns=");
        summary.push_str(&columns.join(","));
    }

    let samples: Vec<String> = records
        .iter()
        .take(2)
        .map(|value| summarize_record(value, &columns))
        .collect();
    if !samples.is_empty() {
        summary.push_str("; sample=");
        summary.push_str(&samples.join(" | "));
    }

    truncate_text(&summary, 400)
}

pub(crate) fn merge_params(
    params: Option<HashMap<String, Value>>,
    context: &[ConversationTurn],
) -> Option<HashMap<String, Value>> {
    let mut merged = params.unwrap_or_default();
    if let Some(turn) = context.last()
        && let Some(bindings) = &turn.bindings
    {
        for (key, value) in bindings {
            merged.entry(key.clone()).or_insert_with(|| value.clone());
        }
    }
    if merged.is_empty() {
        None
    } else {
        Some(merged)
    }
}

pub(crate) fn extract_context_bindings(records: &[Value]) -> Option<HashMap<String, Value>> {
    if records.is_empty() {
        return None;
    }
    let first = records.first().and_then(|value| value.as_object())?;
    let columns: std::collections::HashSet<String> = first.keys().cloned().collect();
    let has_pod = columns.contains("pod") || columns.contains("pod_name");
    let has_service = columns.contains("service") || columns.contains("service_name");

    let mut bindings = HashMap::new();

    if let Some(value) = extract_uniform_value(records, &["pod", "pod_name"]) {
        bindings.insert("pod_name".to_string(), value);
    }
    if has_pod && let Some(value) = extract_uniform_value(records, &["pod_namespace", "namespace"])
    {
        bindings.insert("pod_namespace".to_string(), value);
    }
    if let Some(value) = extract_uniform_value(records, &["service", "service_name"]) {
        bindings.insert("service_name".to_string(), value);
    }
    if has_service {
        if let Some(value) = extract_uniform_value(records, &["service_namespace"]) {
            bindings.insert("service_namespace".to_string(), value);
        } else if !has_pod && let Some(value) = extract_uniform_value(records, &["namespace"]) {
            bindings.insert("service_namespace".to_string(), value);
        }
    }
    if let Some(value) = extract_uniform_value(records, &["ingress", "ingress_name"]) {
        bindings.insert("ingress_name".to_string(), value);
    }
    if let Some(value) = extract_uniform_value(records, &["ingress_namespace"]) {
        bindings.insert("ingress_namespace".to_string(), value);
    }
    if let Some(value) = extract_uniform_value(records, &["host", "hostname"]) {
        bindings.insert("host".to_string(), value);
    }

    if bindings.is_empty() {
        None
    } else {
        Some(bindings)
    }
}

fn extract_uniform_value(records: &[Value], keys: &[&str]) -> Option<Value> {
    for key in keys {
        let mut value: Option<Value> = None;
        let mut count = 0usize;
        for record in records {
            let obj = record.as_object()?;
            let entry = match obj.get(*key) {
                Some(entry) => entry,
                None => {
                    value = None;
                    count = 0;
                    break;
                }
            };
            count += 1;
            match &value {
                None => value = Some(entry.clone()),
                Some(existing) if existing == entry => {}
                Some(_) => {
                    value = None;
                    break;
                }
            }
        }
        if count == records.len()
            && let Some(found) = value
        {
            return Some(found);
        }
    }
    None
}

fn summarize_record(value: &Value, columns: &[String]) -> String {
    if let Some(obj) = value.as_object() {
        let keys: Vec<String> = if columns.is_empty() {
            let mut keys: Vec<String> = obj.keys().cloned().collect();
            keys.sort();
            keys
        } else {
            columns.to_vec()
        };
        let mut parts = Vec::new();
        for key in keys.into_iter().take(6) {
            let entry = obj
                .get(&key)
                .map(format_value)
                .unwrap_or_else(|| "null".to_string());
            parts.push(format!("{key}={}", truncate_text(&entry, 60)));
        }
        return parts.join(", ");
    }

    truncate_text(&format_value(value), 120)
}

pub(crate) fn truncate_text(text: &str, max_len: usize) -> String {
    if text.len() <= max_len {
        return text.to_string();
    }
    let mut trimmed = text[..max_len.saturating_sub(3)].to_string();
    trimmed.push_str("...");
    trimmed
}

fn parse_graph_payload(records: &[Value]) -> Option<ResultPayload> {
    if records.len() != 1 {
        return None;
    }
    let obj = records[0].as_object()?;
    let nodes_val = obj.get("nodes")?;
    let edges_val = obj.get("edges")?;
    let nodes_arr = nodes_val.as_array()?;
    let edges_arr = edges_val.as_array()?;

    let nodes: Vec<GraphNode> = nodes_arr
        .iter()
        .enumerate()
        .map(|(idx, value)| {
            let label = value
                .get("label")
                .and_then(|v| v.as_str())
                .map(|v| v.to_string())
                .or_else(|| {
                    value
                        .get("name")
                        .and_then(|v| v.as_str())
                        .map(|v| v.to_string())
                })
                .unwrap_or_else(|| format!("Node {idx}"));
            GraphNode { label }
        })
        .collect();

    let edges: Vec<GraphEdge> = edges_arr
        .iter()
        .filter_map(|value| {
            let from = value.get("from")?.as_u64()? as usize;
            let to = value.get("to")?.as_u64()? as usize;
            let label = value
                .get("label")
                .and_then(|v| v.as_str())
                .map(|v| v.to_string());
            Some(GraphEdge { from, to, label })
        })
        .collect();

    Some(ResultPayload::Graph { nodes, edges })
}

fn summarize_row(obj: &Map<String, Value>) -> RowCard {
    let title = obj
        .get("metadata_name")
        .and_then(|v| v.as_str())
        .or_else(|| obj.get("name").and_then(|v| v.as_str()))
        .or_else(|| obj.get("namespace").and_then(|v| v.as_str()))
        .unwrap_or("Row")
        .to_string();
    let subtitle = obj
        .get("metadata_namespace")
        .and_then(|v| v.as_str())
        .map(|v| v.to_string());
    let status = obj
        .get("status")
        .and_then(|v| v.as_str())
        .or_else(|| obj.get("phase").and_then(|v| v.as_str()))
        .or_else(|| {
            obj.get("status")
                .and_then(|v| v.as_object())
                .and_then(|status| status.get("phase"))
                .and_then(|v| v.as_str())
        })
        .or_else(|| {
            obj.get("status")
                .and_then(|v| v.as_object())
                .and_then(|status| status.get("reason"))
                .and_then(|v| v.as_str())
        })
        .map(|v| v.to_string());

    let mut raw_fields: Vec<(String, Value)> =
        obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    raw_fields.sort_by(|a, b| a.0.cmp(&b.0));

    let fields: Vec<(String, String)> = raw_fields
        .iter()
        .map(|(k, v)| (k.clone(), format_value(v)))
        .collect();

    RowCard {
        title,
        subtitle,
        status,
        fields,
        raw_fields,
    }
}

pub(crate) fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(v) => v.to_string(),
        Value::Number(v) => v.to_string(),
        Value::String(v) => v.clone(),
        Value::Array(arr) => format_array_value(arr),
        Value::Object(obj) => format!("object({})", obj.len()),
    }
}

fn format_array_value(arr: &[Value]) -> String {
    if arr.is_empty() {
        return "[]".to_string();
    }

    let max_items = 6usize;
    let mut parts = Vec::new();
    for value in arr.iter().take(max_items) {
        parts.push(format_array_item(value));
    }

    let mut out = parts.join(", ");
    if arr.len() > max_items {
        out.push_str(&format!(", ... (+{})", arr.len() - max_items));
    }

    format!("[{out}]")
}

fn format_array_item(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(v) => v.to_string(),
        Value::Number(v) => v.to_string(),
        Value::String(v) => v.clone(),
        Value::Array(arr) => format!("array({})", arr.len()),
        Value::Object(obj) => format!("object({})", obj.len()),
    }
}

pub(crate) fn inspector_value(value: &Value) -> InspectorValue {
    match value {
        Value::Array(_) | Value::Object(_) => {
            let pretty =
                serde_json::to_string_pretty(value).unwrap_or_else(|_| format_value(value));
            InspectorValue::Json(pretty)
        }
        _ => InspectorValue::Text(format_value(value)),
    }
}

pub(crate) fn find_field<'a>(fields: &'a [(String, String)], key: &str) -> Option<&'a str> {
    fields
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

pub(crate) fn build_suggestions() -> Vec<String> {
    let mut suggestions: Vec<String> = ResourceType::iter().map(|r| r.to_string()).collect();
    suggestions.extend(vec![
        "OOMKilledState".to_string(),
        "Pod".to_string(),
        "Container".to_string(),
        "Namespace".to_string(),
        "Deployment".to_string(),
        "ReplicaSet".to_string(),
        "DaemonSet".to_string(),
        "Service".to_string(),
        "Ingress".to_string(),
    ]);
    suggestions.sort();
    suggestions.dedup();
    suggestions
}

pub(crate) fn current_token(input: &str) -> String {
    input
        .split(|c: char| c.is_whitespace() || c == ',' || c == '(' || c == ')' || c == ':')
        .next_back()
        .unwrap_or("")
        .to_string()
}

pub(crate) fn replace_last_token(input: &str, suggestion: &str) -> String {
    let mut parts: Vec<&str> = input
        .split(|c: char| c.is_whitespace() || c == ',' || c == '(' || c == ')' || c == ':')
        .collect();
    if parts.is_empty() {
        return suggestion.to_string();
    }
    let last_token = parts.pop().unwrap_or("");
    let prefix_len = input.len().saturating_sub(last_token.len());
    let prefix = &input[..prefix_len];
    format!("{prefix}{suggestion} ")
}

pub(crate) fn estimate_property_count(
    state: &ariadne_core::state::ClusterState,
    node_count: usize,
) -> usize {
    let sample_size = 200usize.min(node_count.max(1));
    let mut total = 0usize;
    let mut count = 0usize;
    for node in state.get_nodes().take(sample_size) {
        if let Ok(value) = serde_json::to_value(node) {
            total += count_json_properties(&value);
            count += 1;
        }
    }
    if count == 0 {
        return 0;
    }
    let avg = total as f64 / count as f64;
    (avg * node_count as f64) as usize
}

fn count_json_properties(value: &Value) -> usize {
    match value {
        Value::Object(map) => map.len() + map.values().map(count_json_properties).sum::<usize>(),
        Value::Array(arr) => arr.iter().map(count_json_properties).sum(),
        _ => 0,
    }
}

pub(crate) fn format_count(value: usize) -> String {
    let digits = value.to_string();
    let mut out = String::new();
    for (idx, ch) in digits.chars().rev().enumerate() {
        if idx > 0 && idx % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out.chars().rev().collect()
}
