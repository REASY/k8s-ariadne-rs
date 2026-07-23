use super::*;

pub(super) fn apply_match(
    rows: Vec<Row>,
    clause: &MatchClause,
    state: &ClusterState,
    params: &HashMap<String, Value>,
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
                params,
                stats,
            )?;
            if matched || clause.optional {
                output.push(row);
            }
            continue;
        }

        let matches = match_pattern(&row, &clause.pattern, state, params, stats)?;
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
            .filter_map(
                |row| match eval_bool(where_clause, &row, state, params, stats) {
                    Ok(true) => Some(Ok(row)),
                    Ok(false) => None,
                    Err(err) => Some(Err(err)),
                },
            )
            .collect::<Result<Vec<_>>>()?;
    }

    Ok(output)
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
    params: &HashMap<String, Value>,
    stats: &mut QueryStats,
) -> Result<Vec<Row>> {
    match pattern {
        Pattern::Node(node) => match_node_pattern(row, node, state, params, stats),
        Pattern::Relationship(rel) => match_relationship_pattern(row, rel, state, params, stats),
        Pattern::Path(path) => match_path_pattern(row, path, state, params, stats),
    }
}

fn match_node_pattern(
    row: &Row,
    pattern: &ariadne_cypher::NodePattern,
    state: &ClusterState,
    _params: &HashMap<String, Value>,
    stats: &mut QueryStats,
) -> Result<Vec<Row>> {
    let var = pattern.variable.as_ref();
    if let Some(name) = var
        && let Some(bound) = row.get(name)
    {
        if let Some(uid) = node_uid_from_value(bound)
            && let Some(node) = state.node_by_uid(uid)
            && matches_labels(node, &pattern.labels)?
        {
            return Ok(vec![Row::new()]);
        }
        return Ok(Vec::new());
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
    _params: &HashMap<String, Value>,
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
    params: &HashMap<String, Value>,
    stats: &mut QueryStats,
) -> Result<Vec<Row>> {
    let (relationships, internal_vars) = path_relationships_with_internal_vars(pattern, row);
    let mut bindings = vec![Row::new()];

    for rel_pattern in relationships {
        let mut next = Vec::new();
        for binding in bindings {
            let combined = combine_row_for_match(row, &binding);
            let matches =
                match_relationship_pattern(&combined, &rel_pattern, state, params, stats)?;
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

        if let Some(var) = &pattern.left.variable
            && let Some(bound) = row.get(var)
            && !node_value_matches(bound, left_node)
        {
            continue;
        }
        if let Some(var) = &pattern.right.variable
            && let Some(bound) = row.get(var)
            && !node_value_matches(bound, right_node)
        {
            continue;
        }

        if let Some(rel_var) = &pattern.rel.variable
            && let Some(bound) = row.get(rel_var)
            && !relationship_value_matches(bound, edge, &left_uid, &right_uid)
        {
            continue;
        }

        let mut binding = Row::new();
        if let Some(var) = &pattern.left.variable
            && !row.contains_key(var)
        {
            binding.insert(var.clone(), node_to_value(left_node)?);
        }
        if let Some(var) = &pattern.right.variable
            && !row.contains_key(var)
        {
            binding.insert(var.clone(), node_to_value(right_node)?);
        }
        if let Some(rel_var) = &pattern.rel.variable
            && !row.contains_key(rel_var)
        {
            binding.insert(
                rel_var.clone(),
                relationship_to_value(edge, &left_uid, &right_uid),
            );
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
    if let Some(Value::Object(metadata)) = obj.get("metadata")
        && let Some(uid) = metadata.get("uid").and_then(|v| v.as_str())
    {
        return Some(uid);
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
    if let Some(edge_type) = obj.get("type").and_then(|v| v.as_str())
        && !edge_type.eq_ignore_ascii_case(&format!("{:?}", edge.edge_type))
    {
        return false;
    }
    if let Some(source) = obj.get("source").and_then(|v| v.as_str())
        && source != left_uid
    {
        return false;
    }
    if let Some(target) = obj.get("target").and_then(|v| v.as_str())
        && target != right_uid
    {
        return false;
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

pub(super) fn eval_exists(
    row: &Row,
    pattern: &Pattern,
    where_clause: Option<&Expr>,
    state: &ClusterState,
    params: &HashMap<String, Value>,
    stats: &mut QueryStats,
) -> Result<bool> {
    match pattern {
        Pattern::Node(node) => exists_node_pattern(row, node, where_clause, state, params, stats),
        Pattern::Relationship(rel) => {
            exists_relationship_pattern(row, rel, where_clause, state, params, stats)
        }
        Pattern::Path(path) => exists_path_pattern(row, path, where_clause, state, params, stats),
    }
}

fn exists_node_pattern(
    row: &Row,
    pattern: &ariadne_cypher::NodePattern,
    where_clause: Option<&Expr>,
    state: &ClusterState,
    params: &HashMap<String, Value>,
    stats: &mut QueryStats,
) -> Result<bool> {
    let var = pattern.variable.as_ref();
    if let Some(name) = var
        && let Some(bound) = row.get(name)
    {
        if let Some(uid) = node_uid_from_value(bound)
            && let Some(node) = state.node_by_uid(uid)
            && matches_labels(node, &pattern.labels)?
        {
            return exists_binding(row, Row::new(), where_clause, state, params, stats);
        }
        return Ok(false);
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
        if exists_binding(row, binding, where_clause, state, params, stats)? {
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
    params: &HashMap<String, Value>,
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
                    if exists_binding(row, binding, where_clause, state, params, stats)? {
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
                            if exists_binding(row, binding, where_clause, state, params, stats)? {
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
    params: &HashMap<String, Value>,
    stats: &mut QueryStats,
) -> Result<bool> {
    let (relationships, _internal_vars) = path_relationships_with_internal_vars(pattern, row);
    let mut bindings = vec![Row::new()];

    for (idx, rel_pattern) in relationships.iter().enumerate() {
        let is_last = idx + 1 == relationships.len();
        let mut next = Vec::new();
        for binding in bindings {
            let combined = combine_row_for_match(row, &binding);
            let matches = match_relationship_pattern(&combined, rel_pattern, state, params, stats)?;
            for new_binding in matches {
                let mut merged = binding.clone();
                for (key, value) in new_binding {
                    merged.insert(key, value);
                }
                if is_last {
                    if exists_binding(row, merged, where_clause, state, params, stats)? {
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
    params: &HashMap<String, Value>,
    stats: &mut QueryStats,
) -> Result<bool> {
    let Some(merged) = merge_rows(base, &binding) else {
        return Ok(false);
    };
    if let Some(where_clause) = where_clause {
        eval_bool(where_clause, &merged, state, params, stats)
    } else {
        Ok(true)
    }
}
