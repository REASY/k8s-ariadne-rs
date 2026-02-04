use crate::graph_actor::{GraphActor, GraphConnection};
use crate::graph_backend::GraphBackend;
use crate::memgraph::{Memgraph, QuerySpec};
use crate::prelude::*;
use crate::state::{ClusterStateDiff, SharedClusterState};
use graphqlite as gql;
use graphqlite::CYPHER_RESERVED;
use rsmgclient::QueryParam;
use serde_json::Value;
use std::collections::HashMap;
use tracing::error;

#[derive(Debug, Clone)]
pub struct GraphqliteConfig {
    pub db_path: String,
}

impl Default for GraphqliteConfig {
    fn default() -> Self {
        Self {
            db_path: ":memory:".to_string(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct GraphqliteBackend {
    actor: GraphActor,
}

impl GraphqliteBackend {
    pub fn try_new(config: GraphqliteConfig) -> Result<Self> {
        let actor = GraphActor::spawn("graphqlite", move || GraphqliteConnection::try_new(config))?;
        Ok(Self { actor })
    }
}

#[async_trait::async_trait]
impl GraphBackend for GraphqliteBackend {
    async fn create(&self, cluster_state: SharedClusterState) -> Result<()> {
        self.actor.create(cluster_state).await
    }

    async fn update(&self, diff: ClusterStateDiff) -> Result<()> {
        self.actor.update(diff).await
    }

    async fn execute_query(&self, query: String) -> Result<Vec<Value>> {
        self.actor.execute_query(query).await
    }

    async fn shutdown(&self) {
        self.actor.shutdown().await
    }
}

struct GraphqliteConnection {
    conn: gql::Connection,
}

impl GraphqliteConnection {
    fn try_new(config: GraphqliteConfig) -> Result<Self> {
        let conn = gql::Connection::open(&config.db_path)?;
        Ok(Self { conn })
    }

    fn execute_query_raw(&mut self, cypher: &str) -> Result<Vec<Value>> {
        let results = self.conn.cypher(cypher)?;
        cypher_result_to_json(&results)
    }

    fn execute_query(&mut self, query: &str) -> Result<Vec<Value>> {
        let cypher = rewrite_query_for_graphqlite(query);
        match self.execute_query_raw(&cypher) {
            Ok(results) => Ok(results),
            Err(err) => {
                error!("GraphQLite query failed (rewritten): {cypher}");
                Err(err)
            }
        }
    }

    fn execute_query_spec(&mut self, spec: &QuerySpec) -> Result<()> {
        let cypher = rewrite_query_for_graphqlite(&inline_params(spec));
        if let Err(err) = self.execute_query_raw(&cypher) {
            error!("GraphQLite query failed: {cypher}");
            return Err(err);
        }
        Ok(())
    }

    fn create_from_snapshot(
        &mut self,
        nodes: &[crate::types::GenericObject],
        edges: &[crate::state::GraphEdge],
    ) -> Result<()> {
        self.execute_query("MATCH (n) DETACH DELETE n")?;

        for node in nodes {
            let create_spec = Memgraph::get_create_query(node)?;
            self.execute_query_spec(&create_spec)?;
        }

        for edge in edges {
            let create_spec = Memgraph::get_create_edge_query(edge);
            self.execute_query_spec(&create_spec)?;
        }

        Ok(())
    }

    fn update_from_diff(&mut self, diff: &ClusterStateDiff) -> Result<()> {
        for node in diff.removed_nodes.iter() {
            let spec = Memgraph::get_delete_node_query(node);
            self.execute_query_spec(&spec)?;
        }
        for node in diff.modified_nodes.iter() {
            self.update_node_properties(node)?;
        }
        for node in diff.added_nodes.iter() {
            let spec = Memgraph::get_create_query(node)?;
            self.execute_query_spec(&spec)?;
        }
        for edge in diff.removed_edges.iter() {
            let spec = Memgraph::get_delete_edge_query(edge);
            self.execute_query_spec(&spec)?;
        }
        for edge in diff.added_edges.iter() {
            let spec = Memgraph::get_merge_edge_query(edge);
            self.execute_query_spec(&spec)?;
        }
        Ok(())
    }

    fn update_node_properties(&mut self, node: &crate::types::GenericObject) -> Result<()> {
        let Some(props) = Memgraph::get_properties_param(node)? else {
            return Ok(());
        };
        let QueryParam::Map(map) = props else {
            return Ok(());
        };
        let assignments = render_update_assignments(&map);
        if assignments.is_empty() {
            return Ok(());
        }
        let uid = query_param_to_cypher(&QueryParam::String(node.id.uid.clone()));
        let cypher = format!(
            "MATCH (n:{:?}) WHERE n.metadata_uid = {uid} SET {}",
            node.resource_type,
            assignments.join(", ")
        );
        if let Err(err) = self.execute_query(&cypher) {
            error!("GraphQLite query failed: {cypher}");
            return Err(err);
        }
        Ok(())
    }
}

impl GraphConnection for GraphqliteConnection {
    fn create_from_snapshot(
        &mut self,
        nodes: &[crate::types::GenericObject],
        edges: &[crate::state::GraphEdge],
    ) -> Result<()> {
        GraphqliteConnection::create_from_snapshot(self, nodes, edges)
    }

    fn update_from_diff(&mut self, diff: &ClusterStateDiff) -> Result<()> {
        GraphqliteConnection::update_from_diff(self, diff)
    }

    fn execute_query(&mut self, query: &str) -> Result<Vec<Value>> {
        GraphqliteConnection::execute_query(self, query)
    }
}

fn inline_params(spec: &QuerySpec) -> String {
    if spec.params_map().is_empty() {
        return spec.query().to_string();
    }

    let mut query = spec.query().to_string();
    let mut params: Vec<(&String, &QueryParam)> = spec.params_map().iter().collect();
    params.sort_by(|(a, _), (b, _)| b.len().cmp(&a.len()));

    for (key, value) in params {
        let placeholder = format!("${key}");
        let replacement = query_param_to_cypher(value);
        query = query.replace(&placeholder, &replacement);
    }

    query
}

fn query_param_to_cypher(value: &QueryParam) -> String {
    match value {
        QueryParam::Null => "NULL".to_string(),
        QueryParam::Bool(v) => v.to_string(),
        QueryParam::Int(v) => v.to_string(),
        QueryParam::Float(v) => {
            if v.is_finite() {
                v.to_string()
            } else {
                "NULL".to_string()
            }
        }
        QueryParam::String(s) => format!("'{}'", escape_cypher_string(s)),
        QueryParam::Date(d) => format!("'{}'", d.format("%Y-%m-%d")),
        QueryParam::LocalTime(t) => format!("'{}'", t.format("%H:%M:%S")),
        QueryParam::LocalDateTime(dt) => format!("'{}'", dt.format("%Y-%m-%dT%H:%M:%S")),
        QueryParam::Duration(d) => format!("'{d}'"),
        QueryParam::Point2D(p) => format!("'{p}'"),
        QueryParam::Point3D(p) => format!("'{p}'"),
        QueryParam::List(xs) => {
            let rendered: Vec<String> = xs.iter().map(query_param_to_cypher).collect();
            format!("[{}]", rendered.join(", "))
        }
        QueryParam::Map(map) => {
            let rendered: Vec<String> = render_map(map);
            format!("{{{}}}", rendered.join(", "))
        }
    }
}

fn render_map(map: &HashMap<String, QueryParam>) -> Vec<String> {
    let mut entries: Vec<(&String, &QueryParam)> = map.iter().collect();
    entries.sort_by(|(a, _), (b, _)| a.cmp(b));
    let mut rendered: Vec<String> = entries
        .into_iter()
        .map(|(k, v)| format!("{}: {}", format_map_key(k), query_param_to_update_cypher(v)))
        .collect();
    let mut seen: std::collections::HashSet<String> =
        map.keys().map(|k| sanitize_identifier(k)).collect();

    if let Some(QueryParam::Map(metadata)) = map.get("metadata") {
        let extra_keys = [
            ("uid", "metadata_uid"),
            ("name", "metadata_name"),
            ("namespace", "metadata_namespace"),
        ];
        for (meta_key, flattened_key) in extra_keys {
            if map.contains_key(flattened_key) {
                continue;
            }
            if let Some(value) = metadata.get(meta_key) {
                rendered.push(format!(
                    "{}: {}",
                    format_map_key(flattened_key),
                    query_param_to_update_cypher(value)
                ));
                seen.insert(sanitize_identifier(flattened_key));
            }
        }
    }

    let mut flattened = Vec::new();
    collect_flattened_entries(map, None, &mut flattened);
    flattened.sort_by(|(a, _), (b, _)| a.cmp(b));
    for (key, value) in flattened {
        if seen.insert(key.clone()) {
            rendered.push(format!(
                "{}: {}",
                format_map_key(&key),
                query_param_to_update_cypher(value)
            ));
        }
    }

    rendered
}

fn escape_cypher_string(input: &str) -> String {
    gql::escape_string(input)
}

fn format_map_key(key: &str) -> String {
    sanitize_identifier(key)
}

fn rewrite_metadata_access(query: &str) -> String {
    let mut rewritten = query.to_string();
    let replacements = [
        (".metadata.uid", ".metadata_uid"),
        (".metadata.name", ".metadata_name"),
        (".metadata.namespace", ".metadata_namespace"),
        (".metadata['uid']", ".metadata_uid"),
        (".metadata[\"uid\"]", ".metadata_uid"),
        (".metadata['name']", ".metadata_name"),
        (".metadata[\"name\"]", ".metadata_name"),
        (".metadata['namespace']", ".metadata_namespace"),
        (".metadata[\"namespace\"]", ".metadata_namespace"),
    ];
    for (from, to) in replacements {
        rewritten = rewritten.replace(from, to);
    }
    rewritten
}

fn rewrite_query_for_graphqlite(query: &str) -> String {
    let rewritten = rewrite_bracket_property_access(query);
    let rewritten = rewrite_metadata_access(&rewritten);
    let rewritten = rewrite_nested_property_access(&rewritten);
    let rewritten = rewrite_in_list_literals(&rewritten);
    strip_trailing_semicolons(&rewritten)
}

fn rewrite_bracket_property_access(query: &str) -> String {
    let mut output = String::with_capacity(query.len());
    let mut chars = query.chars().peekable();
    let mut in_single = false;
    let mut in_double = false;

    while let Some(ch) = chars.next() {
        if in_single {
            output.push(ch);
            if ch == '\\' {
                if let Some(next) = chars.next() {
                    output.push(next);
                }
                continue;
            }
            if ch == '\'' {
                in_single = false;
            }
            continue;
        }
        if in_double {
            output.push(ch);
            if ch == '\\' {
                if let Some(next) = chars.next() {
                    output.push(next);
                }
                continue;
            }
            if ch == '"' {
                in_double = false;
            }
            continue;
        }

        if ch == '\'' {
            in_single = true;
            output.push(ch);
            continue;
        }
        if ch == '"' {
            in_double = true;
            output.push(ch);
            continue;
        }

        if is_ident_start(ch) {
            let mut ident = String::new();
            ident.push(ch);
            while let Some(&next) = chars.peek() {
                if is_ident_continue(next) {
                    ident.push(next);
                    chars.next();
                } else {
                    break;
                }
            }
            output.push_str(&ident);
            loop {
                if !matches!(chars.peek(), Some('[')) {
                    break;
                }
                match consume_bracket_access(&mut chars) {
                    Ok(access) => output.push_str(&access),
                    Err(raw) => {
                        output.push_str(&raw);
                        break;
                    }
                }
            }
            continue;
        }

        output.push(ch);
    }

    output
}

fn rewrite_nested_property_access(query: &str) -> String {
    let mut output = String::with_capacity(query.len());
    let mut chars = query.chars().peekable();
    let mut in_single = false;
    let mut in_double = false;

    while let Some(ch) = chars.next() {
        if in_single {
            output.push(ch);
            if ch == '\\' {
                if let Some(next) = chars.next() {
                    output.push(next);
                }
                continue;
            }
            if ch == '\'' {
                in_single = false;
            }
            continue;
        }
        if in_double {
            output.push(ch);
            if ch == '\\' {
                if let Some(next) = chars.next() {
                    output.push(next);
                }
                continue;
            }
            if ch == '"' {
                in_double = false;
            }
            continue;
        }

        if ch == '\'' {
            in_single = true;
            output.push(ch);
            continue;
        }
        if ch == '"' {
            in_double = true;
            output.push(ch);
            continue;
        }

        if is_ident_start(ch) {
            let mut ident = String::new();
            ident.push(ch);
            while let Some(&next) = chars.peek() {
                if is_ident_continue(next) {
                    ident.push(next);
                    chars.next();
                } else {
                    break;
                }
            }

            let mut raw = ident.clone();
            let mut properties: Vec<String> = Vec::new();
            let mut failed = false;

            loop {
                if !matches!(chars.peek(), Some('.')) {
                    break;
                }
                chars.next();
                raw.push('.');

                let key = match chars.peek() {
                    Some('`') => {
                        let mut key = String::new();
                        let mut segment_raw = String::new();
                        segment_raw.push('`');
                        chars.next();
                        loop {
                            match chars.next() {
                                Some('`') => {
                                    if matches!(chars.peek(), Some('`')) {
                                        chars.next();
                                        segment_raw.push('`');
                                        segment_raw.push('`');
                                        key.push('`');
                                    } else {
                                        segment_raw.push('`');
                                        break;
                                    }
                                }
                                Some(c) => {
                                    segment_raw.push(c);
                                    key.push(c);
                                }
                                None => {
                                    failed = true;
                                    break;
                                }
                            }
                        }
                        raw.push_str(&segment_raw);
                        key
                    }
                    Some(next) if is_ident_start(*next) => {
                        let mut key = String::new();
                        while let Some(&c) = chars.peek() {
                            if is_ident_continue(c) {
                                key.push(c);
                                chars.next();
                            } else {
                                break;
                            }
                        }
                        raw.push_str(&key);
                        key
                    }
                    _ => {
                        failed = true;
                        break;
                    }
                };

                if failed {
                    break;
                }
                properties.push(key);
            }

            if failed || properties.len() <= 1 {
                output.push_str(&raw);
                continue;
            }

            let mut flattened = sanitize_identifier(&properties[0]);
            for segment in properties.iter().skip(1) {
                flattened.push('_');
                flattened.push_str(&sanitize_identifier(segment));
            }
            output.push_str(&format!("{}.{}", ident, format_map_key(&flattened)));
            continue;
        }

        output.push(ch);
    }

    output
}

fn rewrite_in_list_literals(query: &str) -> String {
    let mut rewritten = String::new();
    for line in query.lines() {
        if let Some(transformed) = rewrite_in_list_literal_line(line) {
            rewritten.push_str(&transformed);
        } else {
            rewritten.push_str(line);
        }
        rewritten.push('\n');
    }
    if !query.ends_with('\n') {
        rewritten.pop();
    }
    rewritten
}

fn rewrite_in_list_literal_line(line: &str) -> Option<String> {
    let lowercase = line.to_ascii_lowercase();
    let needle = " in [";
    let in_pos = lowercase.find(needle)?;
    let (before, _) = line.split_at(in_pos);
    let list_start = in_pos + needle.len() - 1;
    let list_section = &line[list_start..];
    let end_bracket = list_section.find(']')?;
    let list_body = &list_section[1..end_bracket];
    let after_list = &list_section[end_bracket + 1..];

    let mut before_trim = before.trim_end().to_string();
    if before_trim.is_empty() {
        return None;
    }
    let lhs_start = before_trim
        .rfind(|c: char| c.is_whitespace())
        .map(|idx| idx + 1)
        .unwrap_or(0);
    let lhs = before_trim[lhs_start..].to_string();
    if lhs.is_empty() {
        return None;
    }
    before_trim.truncate(lhs_start);
    let before_lhs = before_trim.trim_end();
    let mut not_prefix = false;
    let mut prefix = before_lhs.to_string();
    if before_lhs.to_ascii_lowercase().ends_with("not") {
        let trimmed = before_lhs.trim_end();
        let without_not = trimmed[..trimmed.len() - 3].trim_end();
        prefix = without_not.to_string();
        not_prefix = true;
    }

    let items = parse_list_items(list_body)?;
    if items.is_empty() {
        return None;
    }
    let mut replaced = String::new();
    replaced.push_str(prefix.trim_end());
    if !replaced.is_empty() {
        replaced.push(' ');
    }
    if not_prefix {
        replaced.push_str("NOT (");
    } else {
        replaced.push('(');
    }
    for (idx, item) in items.iter().enumerate() {
        if idx > 0 {
            replaced.push_str(" OR ");
        }
        replaced.push_str(&lhs);
        replaced.push_str(" = ");
        replaced.push_str(item);
    }
    replaced.push(')');
    replaced.push_str(after_list);
    Some(replaced)
}

fn parse_list_items(list_body: &str) -> Option<Vec<String>> {
    let mut items = Vec::new();
    let mut current = String::new();
    let mut chars = list_body.chars().peekable();
    let mut in_single = false;
    let mut in_double = false;

    while let Some(ch) = chars.next() {
        if in_single {
            current.push(ch);
            if ch == '\\' {
                if let Some(next) = chars.next() {
                    current.push(next);
                }
                continue;
            }
            if ch == '\'' {
                in_single = false;
            }
            continue;
        }
        if in_double {
            current.push(ch);
            if ch == '\\' {
                if let Some(next) = chars.next() {
                    current.push(next);
                }
                continue;
            }
            if ch == '"' {
                in_double = false;
            }
            continue;
        }

        match ch {
            '\'' => {
                in_single = true;
                current.push(ch);
            }
            '"' => {
                in_double = true;
                current.push(ch);
            }
            ',' => {
                let item = current.trim();
                if !item.is_empty() {
                    items.push(item.to_string());
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    if in_single || in_double {
        return None;
    }
    let item = current.trim();
    if !item.is_empty() {
        items.push(item.to_string());
    }
    Some(items)
}

fn strip_trailing_semicolons(query: &str) -> String {
    let mut trimmed = query.trim_end().to_string();
    while trimmed.ends_with(';') {
        trimmed.pop();
        trimmed = trimmed.trim_end().to_string();
    }
    trimmed
}

fn consume_bracket_access(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
) -> std::result::Result<String, String> {
    let mut raw = String::new();
    let Some('[') = chars.next() else {
        return Err(raw);
    };
    raw.push('[');

    while let Some(&c) = chars.peek() {
        if c.is_whitespace() {
            raw.push(c);
            chars.next();
        } else {
            break;
        }
    }

    let quote = match chars.next() {
        Some('\'') => '\'',
        Some('"') => '"',
        _ => return Err(raw),
    };
    raw.push(quote);

    let mut key = String::new();
    let mut escaped = false;
    while let Some(c) = chars.next() {
        raw.push(c);
        if escaped {
            key.push(c);
            escaped = false;
            continue;
        }
        if c == '\\' {
            escaped = true;
            continue;
        }
        if c == quote {
            break;
        }
        key.push(c);
    }
    if !raw.ends_with(quote) {
        return Err(raw);
    }

    while let Some(&c) = chars.peek() {
        if c.is_whitespace() {
            raw.push(c);
            chars.next();
        } else {
            break;
        }
    }

    if chars.next() != Some(']') {
        return Err(raw);
    }
    raw.push(']');

    Ok(format!(".{}", format_map_key(&key)))
}

fn is_ident_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
}

fn is_ident_continue(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn format_property_access(key: &str) -> String {
    format!("n.{}", sanitize_identifier(key))
}

fn sanitize_identifier(key: &str) -> String {
    let mut out = String::new();
    for ch in key.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        out.push_str("prop");
    }
    if out.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        out = format!("p_{out}");
    }
    if CYPHER_RESERVED
        .iter()
        .any(|reserved| reserved.eq_ignore_ascii_case(&out))
    {
        out = format!("p_{out}");
    }
    out
}

fn render_update_assignments(map: &HashMap<String, QueryParam>) -> Vec<String> {
    let mut entries: Vec<(&String, &QueryParam)> = map.iter().collect();
    entries.sort_by(|(a, _), (b, _)| a.cmp(b));
    let mut rendered: Vec<String> = entries
        .into_iter()
        .map(|(k, v)| {
            format!(
                "{} = {}",
                format_property_access(k),
                query_param_to_update_cypher(v)
            )
        })
        .collect();
    let mut seen: std::collections::HashSet<String> =
        map.keys().map(|k| sanitize_identifier(k)).collect();

    if let Some(QueryParam::Map(metadata)) = map.get("metadata") {
        let extra_keys = [
            ("uid", "metadata_uid"),
            ("name", "metadata_name"),
            ("namespace", "metadata_namespace"),
        ];
        for (meta_key, flattened_key) in extra_keys {
            if map.contains_key(flattened_key) {
                continue;
            }
            if let Some(value) = metadata.get(meta_key) {
                rendered.push(format!(
                    "{} = {}",
                    format_property_access(flattened_key),
                    query_param_to_update_cypher(value)
                ));
                seen.insert(sanitize_identifier(flattened_key));
            }
        }
    }

    let mut flattened = Vec::new();
    collect_flattened_entries(map, None, &mut flattened);
    flattened.sort_by(|(a, _), (b, _)| a.cmp(b));
    for (key, value) in flattened {
        if seen.insert(key.clone()) {
            rendered.push(format!(
                "{} = {}",
                format_property_access(&key),
                query_param_to_update_cypher(value)
            ));
        }
    }

    rendered
}

fn collect_flattened_entries<'a>(
    map: &'a HashMap<String, QueryParam>,
    prefix: Option<&str>,
    out: &mut Vec<(String, &'a QueryParam)>,
) {
    for (key, value) in map {
        let sanitized = sanitize_identifier(key);
        let flattened_key = match prefix {
            Some(prefix) => format!("{prefix}_{sanitized}"),
            None => sanitized,
        };
        match value {
            QueryParam::Map(inner) => {
                collect_flattened_entries(inner, Some(&flattened_key), out);
            }
            _ => out.push((flattened_key, value)),
        }
    }
}

fn query_param_to_update_cypher(value: &QueryParam) -> String {
    match value {
        QueryParam::Map(_) | QueryParam::List(_) => {
            let json = query_param_to_json(value);
            format!("'{}'", escape_cypher_string(&json.to_string()))
        }
        _ => query_param_to_cypher(value),
    }
}

fn query_param_to_json(value: &QueryParam) -> Value {
    match value {
        QueryParam::Null => Value::Null,
        QueryParam::Bool(v) => Value::Bool(*v),
        QueryParam::Int(v) => Value::Number((*v).into()),
        QueryParam::Float(v) => serde_json::Number::from_f64(*v)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        QueryParam::String(s) => Value::String(s.clone()),
        QueryParam::Date(d) => Value::String(d.format("%Y-%m-%d").to_string()),
        QueryParam::LocalTime(t) => Value::String(t.format("%H:%M:%S").to_string()),
        QueryParam::LocalDateTime(dt) => Value::String(dt.format("%Y-%m-%dT%H:%M:%S").to_string()),
        QueryParam::Duration(d) => Value::String(d.to_string()),
        QueryParam::Point2D(p) => Value::String(p.to_string()),
        QueryParam::Point3D(p) => Value::String(p.to_string()),
        QueryParam::List(xs) => Value::Array(xs.iter().map(query_param_to_json).collect()),
        QueryParam::Map(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), query_param_to_json(v)))
                .collect(),
        ),
    }
}

fn cypher_result_to_json(results: &gql::CypherResult) -> Result<Vec<Value>> {
    let mut output = Vec::with_capacity(results.len());
    for row in results.iter() {
        let mut map = serde_json::Map::new();
        for column in row.columns() {
            let value = row
                .get_value(column)
                .map(serde_json::to_value)
                .transpose()?
                .unwrap_or(Value::Null);
            map.insert(column.clone(), value);
        }
        output.push(Value::Object(map));
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsmgclient::QueryParam;
    use std::collections::HashMap;

    #[test]
    fn inline_params_escapes_strings() {
        let mut params = HashMap::new();
        params.insert(
            "name".to_string(),
            QueryParam::String("O'Reilly".to_string()),
        );
        let spec = QuerySpec::with_params(
            "MATCH (n) WHERE n.name = $name RETURN n".to_string(),
            params,
        );
        let inline = inline_params(&spec);
        assert!(inline.contains("n.name = 'O\\'Reilly'"));
    }

    #[test]
    fn inline_params_handles_maps() {
        let mut inner = HashMap::new();
        inner.insert("name".to_string(), QueryParam::String("pod".to_string()));
        inner.insert("uid".to_string(), QueryParam::String("pod-uid".to_string()));
        let mut props = HashMap::new();
        props.insert("metadata".to_string(), QueryParam::Map(inner));

        let mut params = HashMap::new();
        params.insert("props".to_string(), QueryParam::Map(props));
        let spec = QuerySpec::with_params("CREATE (n:Pod $props)".to_string(), params);
        let inline = inline_params(&spec);
        assert!(inline.contains("CREATE (n:Pod"));
        assert!(inline.contains("metadata:"));
        assert!(inline.contains("name: 'pod'"));
        assert!(inline.contains("metadata_uid: 'pod-uid'"));
    }

    #[test]
    fn render_update_assignments_adds_metadata_uid() {
        let mut metadata = HashMap::new();
        metadata.insert("uid".to_string(), QueryParam::String("pod-uid".to_string()));
        let mut props = HashMap::new();
        props.insert("metadata".to_string(), QueryParam::Map(metadata));
        props.insert("kind".to_string(), QueryParam::String("Pod".to_string()));

        let rendered = render_update_assignments(&props);
        assert!(rendered.iter().any(|s| s == "n.kind = 'Pod'"));
        assert!(rendered.iter().any(|s| s == "n.metadata_uid = 'pod-uid'"));
    }

    #[test]
    fn render_update_assignments_stringifies_maps_and_lists() {
        let mut inner = HashMap::new();
        inner.insert("key".to_string(), QueryParam::String("value".to_string()));
        let mut props = HashMap::new();
        props.insert("spec".to_string(), QueryParam::Map(inner));
        props.insert(
            "tags".to_string(),
            QueryParam::List(vec![QueryParam::String("alpha".to_string())]),
        );

        let rendered = render_update_assignments(&props);
        let spec = rendered
            .iter()
            .find(|s| s.starts_with("n.spec = "))
            .expect("spec assignment");
        assert!(spec.contains("{\\\"key\\\":\\\"value\\\"}"));
        let tags = rendered
            .iter()
            .find(|s| s.starts_with("n.tags = "))
            .expect("tags assignment");
        assert!(tags.contains("[\\\"alpha\\\"]"));
    }

    #[test]
    fn rewrite_bracket_property_access_rewrites_keys() {
        let query = "MATCH (h:Host) WHERE h['name'] = 'x' RETURN h['name']";
        let rewritten = rewrite_bracket_property_access(query);
        assert!(rewritten.contains("h.name"));
        assert!(!rewritten.contains("['name']"));
    }

    #[test]
    fn rewrite_bracket_property_access_quotes_complex_keys() {
        let query = "RETURN p['app.kubernetes.io/name'] AS app";
        let rewritten = rewrite_bracket_property_access(query);
        assert!(rewritten.contains("p.app_kubernetes_io_name"));
    }

    #[test]
    fn rewrite_query_for_graphqlite_flattens_metadata() {
        let query = "RETURN p['metadata']['namespace'] AS ns";
        let rewritten = rewrite_query_for_graphqlite(query);
        assert!(rewritten.contains("p.metadata_namespace"));
    }

    #[test]
    fn rewrite_nested_property_access_to_json_extract() {
        let query = "WHERE p.status.phase = 'Failed'";
        let rewritten = rewrite_nested_property_access(query);
        assert!(rewritten.contains("p.status_phase"));
    }

    #[test]
    fn rewrite_nested_property_access_preserves_backticks() {
        let query = "RETURN p.`app.kubernetes.io/labels`.tier";
        let rewritten = rewrite_nested_property_access(query);
        assert!(rewritten.contains("p.app_kubernetes_io_labels_tier"));
    }

    #[test]
    fn rewrite_in_list_literals_rewrites_to_or() {
        let query = "WHERE p.status_phase IN ['Failed', 'Unknown']";
        let rewritten = rewrite_in_list_literals(query);
        assert!(rewritten.contains("p.status_phase = 'Failed'"));
        assert!(rewritten.contains("OR p.status_phase = 'Unknown'"));
    }

    #[test]
    fn rewrite_in_list_literals_handles_not() {
        let query = "WHERE NOT p.status_phase IN ['Running','Succeeded']";
        let rewritten = rewrite_in_list_literals(query);
        assert!(rewritten.contains("NOT (p.status_phase = 'Running'"));
    }

    // Result conversion relies on GraphQLite's runtime behavior; covered by integration usage.
}
