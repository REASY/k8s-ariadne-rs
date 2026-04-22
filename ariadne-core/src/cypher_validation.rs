use std::collections::{HashMap, HashSet};

use crate::graph_schema;
use crate::query_issue::{QueryIssue, QueryIssueKind};
use crate::types::{Edge, ResourceType};
use ariadne_cypher::{
    Clause, Expr, MatchClause, NodePattern, Pattern, RelationshipDirection, RelationshipPattern,
    ValidationMode, parse_query, validate_query,
};
use strum::IntoEnumIterator;

pub use crate::query_issue::{
    QueryIssue as ValidationIssue, QueryIssueKind as ValidationIssueKind,
};

pub fn parse_cypher(cypher: &str) -> Result<ariadne_cypher::Query, ValidationIssue> {
    parse_query(cypher).map_err(|err| {
        tracing::error!(error = %err, cypher = %cypher, "Cypher parse failed");
        QueryIssue::validation(QueryIssueKind::Parse, err.to_string())
    })
}

pub fn validate_read_only_query(query: &ariadne_cypher::Query) -> Result<(), ValidationIssue> {
    validate_query(query, ValidationMode::ReadOnly).map_err(|err| {
        tracing::error!(error = %err, "Cypher validation failed");
        QueryIssue::validation(QueryIssueKind::Semantic, err.to_string())
    })
}

pub fn validate_cypher(cypher: &str) -> Result<(), ValidationIssue> {
    let query = parse_cypher(cypher)?;
    validate_read_only_query(&query)?;
    validate_schema_query(&query)?;
    Ok(())
}

pub fn validate_schema_query(query: &ariadne_cypher::Query) -> Result<(), ValidationIssue> {
    let mut var_labels: HashMap<String, HashSet<String>> = HashMap::new();
    let mut patterns: Vec<Pattern> = Vec::new();

    for clause in &query.clauses {
        if let Clause::Match(m) = clause {
            collect_pattern_labels(&m.pattern, &mut var_labels);
            patterns.push(m.pattern.clone());
            if let Some(expr) = &m.where_clause {
                collect_from_expr(expr, &mut var_labels, &mut patterns);
            }
        }
        collect_patterns_from_clause_exprs(clause, &mut var_labels, &mut patterns);
    }

    let mut issues = Vec::new();
    for pattern in patterns {
        for rel in relationships_from_pattern(&pattern) {
            validate_relationship(&rel, &var_labels, &mut issues);
        }
    }

    if issues.is_empty() {
        return Ok(());
    }
    Err(QueryIssue::validation(
        QueryIssueKind::Schema,
        issues.join(" | "),
    ))
}

pub fn validate_read_only_text(cypher: &str) -> Result<(), ValidationIssue> {
    const NON_READ_ONLY_KEYWORDS: &[&str] = &[
        "ALTER",
        "CALL",
        "COPY",
        "CREATE",
        "DELETE",
        "DETACH",
        "DROP",
        "FOREACH",
        "INSTALL",
        "LOAD",
        "MERGE",
        "REMOVE",
        "RENAME",
        "RESTORE",
        "SET",
        "SNAPSHOT",
        "STREAM",
        "TRIGGER",
        "UNINSTALL",
    ];

    let tokens = cypher_tokens(cypher);
    if let Some(keyword) = tokens
        .iter()
        .find(|token| NON_READ_ONLY_KEYWORDS.contains(&token.as_str()))
    {
        return Err(QueryIssue::validation(
            QueryIssueKind::Semantic,
            format!(
                "Unsupported non-read-only statement detected while skipping parser-specific validation: {keyword}"
            ),
        ));
    }

    Ok(())
}

fn cypher_tokens(cypher: &str) -> Vec<String> {
    #[derive(Clone, Copy)]
    enum Mode {
        Normal,
        SingleQuoted,
        DoubleQuoted,
        BacktickQuoted,
        LineComment,
        BlockComment,
    }

    let mut chars = cypher.chars().peekable();
    let mut mode = Mode::Normal;
    let mut current = String::new();
    let mut tokens = Vec::new();

    fn flush(current: &mut String, tokens: &mut Vec<String>) {
        if !current.is_empty() {
            tokens.push(std::mem::take(current));
        }
    }

    while let Some(ch) = chars.next() {
        match mode {
            Mode::Normal => match ch {
                '\'' => {
                    flush(&mut current, &mut tokens);
                    mode = Mode::SingleQuoted;
                }
                '"' => {
                    flush(&mut current, &mut tokens);
                    mode = Mode::DoubleQuoted;
                }
                '`' => {
                    flush(&mut current, &mut tokens);
                    mode = Mode::BacktickQuoted;
                }
                '/' => {
                    if matches!(chars.peek(), Some('/')) {
                        chars.next();
                        flush(&mut current, &mut tokens);
                        mode = Mode::LineComment;
                    } else if matches!(chars.peek(), Some('*')) {
                        chars.next();
                        flush(&mut current, &mut tokens);
                        mode = Mode::BlockComment;
                    } else {
                        flush(&mut current, &mut tokens);
                    }
                }
                c if c.is_ascii_alphanumeric() || c == '_' => current.push(c.to_ascii_uppercase()),
                _ => flush(&mut current, &mut tokens),
            },
            Mode::SingleQuoted => {
                if ch == '\\' {
                    chars.next();
                } else if ch == '\'' {
                    mode = Mode::Normal;
                }
            }
            Mode::DoubleQuoted => {
                if ch == '\\' {
                    chars.next();
                } else if ch == '"' {
                    mode = Mode::Normal;
                }
            }
            Mode::BacktickQuoted => {
                if ch == '`' {
                    mode = Mode::Normal;
                }
            }
            Mode::LineComment => {
                if ch == '\n' {
                    mode = Mode::Normal;
                }
            }
            Mode::BlockComment => {
                if ch == '*' && matches!(chars.peek(), Some('/')) {
                    chars.next();
                    mode = Mode::Normal;
                }
            }
        }
    }
    flush(&mut current, &mut tokens);
    tokens
}

fn collect_patterns_from_clause_exprs(
    clause: &Clause,
    var_labels: &mut HashMap<String, HashSet<String>>,
    patterns: &mut Vec<Pattern>,
) {
    match clause {
        Clause::Match(MatchClause {
            where_clause: Some(expr),
            ..
        }) => collect_from_expr(expr, var_labels, patterns),
        Clause::Unwind(u) => collect_from_expr(&u.expression, var_labels, patterns),
        Clause::With(w) => {
            for item in &w.items {
                collect_from_expr(&item.expr, var_labels, patterns);
            }
            if let Some(expr) = &w.where_clause {
                collect_from_expr(expr, var_labels, patterns);
            }
            if let Some(order) = &w.order {
                for item in &order.items {
                    collect_from_expr(&item.expr, var_labels, patterns);
                }
            }
            if let Some(expr) = &w.skip {
                collect_from_expr(expr, var_labels, patterns);
            }
            if let Some(expr) = &w.limit {
                collect_from_expr(expr, var_labels, patterns);
            }
        }
        Clause::Return(r) => {
            for item in &r.items {
                collect_from_expr(&item.expr, var_labels, patterns);
            }
            if let Some(order) = &r.order {
                for item in &order.items {
                    collect_from_expr(&item.expr, var_labels, patterns);
                }
            }
            if let Some(expr) = &r.skip {
                collect_from_expr(expr, var_labels, patterns);
            }
            if let Some(expr) = &r.limit {
                collect_from_expr(expr, var_labels, patterns);
            }
        }
        _ => {}
    }
}

fn collect_from_expr(
    expr: &Expr,
    var_labels: &mut HashMap<String, HashSet<String>>,
    patterns: &mut Vec<Pattern>,
) {
    match expr {
        Expr::Exists {
            pattern,
            where_clause,
        } => {
            collect_pattern_labels(pattern, var_labels);
            patterns.push(pattern.clone());
            if let Some(expr) = where_clause {
                collect_from_expr(expr, var_labels, patterns);
            }
        }
        Expr::HasLabel { expr, labels } => {
            if let Expr::Variable(name) = expr.as_ref() {
                let entry = var_labels.entry(name.clone()).or_default();
                for label in labels {
                    entry.insert(label.clone());
                }
            }
            collect_from_expr(expr, var_labels, patterns);
        }
        Expr::PropertyAccess { expr, .. } => collect_from_expr(expr, var_labels, patterns),
        Expr::IndexAccess { expr, index } => {
            collect_from_expr(expr, var_labels, patterns);
            collect_from_expr(index, var_labels, patterns);
        }
        Expr::ListSlice { expr, start, end } => {
            collect_from_expr(expr, var_labels, patterns);
            if let Some(start) = start.as_deref() {
                collect_from_expr(start, var_labels, patterns);
            }
            if let Some(end) = end.as_deref() {
                collect_from_expr(end, var_labels, patterns);
            }
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_from_expr(arg, var_labels, patterns);
            }
        }
        Expr::UnaryOp { expr, .. } => collect_from_expr(expr, var_labels, patterns),
        Expr::BinaryOp { left, right, .. } => {
            collect_from_expr(left, var_labels, patterns);
            collect_from_expr(right, var_labels, patterns);
        }
        Expr::IsNull { expr, .. } => collect_from_expr(expr, var_labels, patterns),
        Expr::In { expr, list } => {
            collect_from_expr(expr, var_labels, patterns);
            collect_from_expr(list, var_labels, patterns);
        }
        Expr::Case {
            base,
            alternatives,
            else_expr,
        } => {
            if let Some(base) = base.as_deref() {
                collect_from_expr(base, var_labels, patterns);
            }
            for (when, then) in alternatives {
                collect_from_expr(when, var_labels, patterns);
                collect_from_expr(then, var_labels, patterns);
            }
            if let Some(expr) = else_expr.as_deref() {
                collect_from_expr(expr, var_labels, patterns);
            }
        }
        Expr::ListComprehension {
            list,
            where_clause,
            map,
            ..
        } => {
            collect_from_expr(list, var_labels, patterns);
            if let Some(expr) = where_clause.as_deref() {
                collect_from_expr(expr, var_labels, patterns);
            }
            collect_from_expr(map, var_labels, patterns);
        }
        Expr::Quantifier {
            list, where_clause, ..
        } => {
            collect_from_expr(list, var_labels, patterns);
            if let Some(expr) = where_clause.as_deref() {
                collect_from_expr(expr, var_labels, patterns);
            }
        }
        Expr::Literal(_)
        | Expr::Variable(_)
        | Expr::Star
        | Expr::CountStar
        | Expr::Parameter(_) => {}
    }
}

fn collect_pattern_labels(pattern: &Pattern, var_labels: &mut HashMap<String, HashSet<String>>) {
    match pattern {
        Pattern::Node(node) => collect_node_labels(node, var_labels),
        Pattern::Relationship(rel) => {
            collect_node_labels(&rel.left, var_labels);
            collect_node_labels(&rel.right, var_labels);
        }
        Pattern::Path(path) => {
            collect_node_labels(&path.start, var_labels);
            for segment in &path.segments {
                collect_node_labels(&segment.node, var_labels);
            }
        }
    }
}

fn collect_node_labels(node: &NodePattern, var_labels: &mut HashMap<String, HashSet<String>>) {
    let Some(var) = &node.variable else {
        return;
    };
    if node.labels.is_empty() {
        return;
    }
    let entry = var_labels.entry(var.clone()).or_default();
    for label in &node.labels {
        entry.insert(label.clone());
    }
}

fn relationships_from_pattern(pattern: &Pattern) -> Vec<RelationshipPattern> {
    match pattern {
        Pattern::Relationship(rel) => vec![rel.clone()],
        Pattern::Path(path) => path
            .segments
            .iter()
            .enumerate()
            .map(|(idx, segment)| RelationshipPattern {
                left: if idx == 0 {
                    path.start.clone()
                } else {
                    path.segments[idx - 1].node.clone()
                },
                rel: segment.rel.clone(),
                right: segment.node.clone(),
                span: segment.span,
            })
            .collect(),
        Pattern::Node(_) => Vec::new(),
    }
}

fn validate_relationship(
    rel: &RelationshipPattern,
    var_labels: &HashMap<String, HashSet<String>>,
    issues: &mut Vec<String>,
) {
    if rel.rel.types.is_empty() {
        return;
    }

    let left_labels = resolve_labels(&rel.left, var_labels);
    let right_labels = resolve_labels(&rel.right, var_labels);
    let (Some(left_labels), Some(right_labels)) = (left_labels, right_labels) else {
        return;
    };

    let left_types = match labels_to_types(&left_labels) {
        Ok(types) => types,
        Err(errs) => {
            issues.extend(errs);
            return;
        }
    };
    let right_types = match labels_to_types(&right_labels) {
        Ok(types) => types,
        Err(errs) => {
            issues.extend(errs);
            return;
        }
    };

    for rel_type in &rel.rel.types {
        let Some(edge) = edge_from_str(rel_type) else {
            issues.push(format!("Unknown relationship type: {rel_type}"));
            continue;
        };
        let allowed = is_edge_allowed(&edge, &left_types, &right_types, &rel.rel.direction);
        if !allowed {
            let pairs = allowed_pairs(&edge);
            issues.push(format!(
                "Relationship {rel_type} not allowed between {} and {} ({:?}); allowed: {}",
                label_list(&left_labels),
                label_list(&right_labels),
                rel.rel.direction,
                pairs
            ));
        }
    }
}

fn resolve_labels(
    node: &NodePattern,
    var_labels: &HashMap<String, HashSet<String>>,
) -> Option<HashSet<String>> {
    if !node.labels.is_empty() {
        return Some(node.labels.iter().cloned().collect());
    }
    if let Some(var) = &node.variable
        && let Some(labels) = var_labels.get(var)
    {
        return Some(labels.iter().cloned().collect());
    }
    None
}

fn labels_to_types(labels: &HashSet<String>) -> Result<Vec<ResourceType>, Vec<String>> {
    let mut types = Vec::new();
    let mut issues = Vec::new();
    for label in labels {
        match ResourceType::try_new(label) {
            Ok(kind) => types.push(kind),
            Err(_) => issues.push(format!("Unknown label: {label}")),
        }
    }
    if issues.is_empty() {
        Ok(types)
    } else {
        Err(issues)
    }
}

fn edge_from_str(name: &str) -> Option<Edge> {
    Edge::iter().find(|edge| edge.to_string().eq_ignore_ascii_case(name))
}

fn is_edge_allowed(
    edge: &Edge,
    left_types: &[ResourceType],
    right_types: &[ResourceType],
    direction: &RelationshipDirection,
) -> bool {
    for left in left_types {
        for right in right_types {
            let allowed = match direction {
                RelationshipDirection::LeftToRight => {
                    graph_schema::is_known_edge(left, edge, right)
                }
                RelationshipDirection::RightToLeft => {
                    graph_schema::is_known_edge(right, edge, left)
                }
                RelationshipDirection::Undirected => {
                    graph_schema::is_known_edge(left, edge, right)
                        || graph_schema::is_known_edge(right, edge, left)
                }
            };
            if allowed {
                return true;
            }
        }
    }
    false
}

fn allowed_pairs(edge: &Edge) -> String {
    let mut pairs: Vec<String> = graph_schema::graph_relationship_specs()
        .into_iter()
        .filter(|(_, e, _)| e == edge)
        .map(|(from, _, to)| format!("{from}->{to}"))
        .collect();
    pairs.sort();
    if pairs.is_empty() {
        "none".to_string()
    } else {
        pairs.join(", ")
    }
}

fn label_list(labels: &HashSet<String>) -> String {
    let mut values: Vec<String> = labels.iter().cloned().collect();
    values.sort();
    values.join("|")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_issue::{QueryIssueKind, QueryIssueSource};

    // ── Parse errors ────────────────────────────────────────────────

    #[test]
    fn parse_error_invalid_syntax() {
        let result = validate_cypher("MATCH RETURN");
        let issue = result.expect_err("should fail to parse");
        assert_eq!(issue.kind, QueryIssueKind::Parse);
        assert_eq!(issue.source, QueryIssueSource::Validator);
    }

    #[test]
    fn parse_error_completely_broken_input() {
        let result = validate_cypher("not cypher at all");
        let issue = result.expect_err("should fail to parse");
        assert_eq!(issue.kind, QueryIssueKind::Parse);
        assert_eq!(issue.source, QueryIssueSource::Validator);
    }

    #[test]
    fn parse_stage_returns_query_for_valid_input() {
        let query = parse_cypher("MATCH (p:Pod) RETURN p").expect("query should parse");
        assert_eq!(query.clauses.len(), 2);
    }

    // ── Semantic errors ─────────────────────────────────────────────

    #[test]
    fn semantic_error_create_mutation() {
        let result = validate_cypher("CREATE (n:Pod) RETURN n");
        let issue = result.expect_err("CREATE should be rejected");
        assert_eq!(issue.kind, QueryIssueKind::Semantic);
        assert_eq!(issue.source, QueryIssueSource::Validator);
    }

    #[test]
    fn semantic_error_delete_mutation() {
        let result = validate_cypher("MATCH (n) DELETE n");
        let issue = result.expect_err("DELETE should be rejected");
        assert_eq!(issue.kind, QueryIssueKind::Semantic);
        assert_eq!(issue.source, QueryIssueSource::Validator);
    }

    #[test]
    fn read_only_text_rejects_non_read_only_keyword() {
        let issue = validate_read_only_text("CREATE INDEX ON :Pod(name)")
            .expect_err("CREATE INDEX should be rejected by fallback guard");
        assert_eq!(issue.kind, QueryIssueKind::Semantic);
        assert_eq!(issue.source, QueryIssueSource::Validator);
    }

    #[test]
    fn read_only_text_ignores_keywords_inside_strings_and_comments() {
        let result = validate_read_only_text(
            "// CREATE should be ignored\nMATCH (p:Pod) WHERE p['note'] = 'delete me' RETURN p",
        );
        assert!(
            result.is_ok(),
            "read-only fallback should ignore literals/comments"
        );
    }

    #[test]
    fn semantic_error_no_return_clause() {
        // The parser itself rejects a query without RETURN, producing a Parse error.
        let result = validate_cypher("MATCH (n:Pod)");
        let issue = result.expect_err("missing RETURN should be rejected");
        assert_eq!(issue.kind, QueryIssueKind::Parse);
        assert_eq!(issue.source, QueryIssueSource::Validator);
    }

    // ── Schema errors ───────────────────────────────────────────────

    #[test]
    fn schema_error_unknown_node_label() {
        // Schema validation triggers on relationship patterns, so the unknown
        // label must appear as an endpoint of a relationship.
        let result = validate_cypher("MATCH (n:FakeLabel)-[:RunsOn]->(m:Node) RETURN n");
        let issue = result.expect_err("unknown label should be rejected");
        assert_eq!(issue.kind, QueryIssueKind::Schema);
        assert_eq!(issue.source, QueryIssueSource::Validator);
        assert!(
            issue.message.contains("Unknown label: FakeLabel"),
            "message should mention the bad label, got: {}",
            issue.message,
        );
    }

    #[test]
    fn schema_error_wrong_edge_direction() {
        // RunsOn goes Pod->Node, not Node->Pod
        let result = validate_cypher("MATCH (n:Node)-[:RunsOn]->(p:Pod) RETURN n");
        let issue = result.expect_err("wrong direction should be rejected");
        assert_eq!(issue.kind, QueryIssueKind::Schema);
        assert_eq!(issue.source, QueryIssueSource::Validator);
        assert!(
            issue.message.contains("RunsOn"),
            "message should mention the edge type, got: {}",
            issue.message,
        );
    }

    #[test]
    fn schema_error_unknown_edge_type() {
        let result = validate_cypher("MATCH (p:Pod)-[:FakeEdge]->(n:Node) RETURN p");
        let issue = result.expect_err("unknown edge type should be rejected");
        assert_eq!(issue.kind, QueryIssueKind::Schema);
        assert_eq!(issue.source, QueryIssueSource::Validator);
        assert!(
            issue
                .message
                .contains("Unknown relationship type: FakeEdge"),
            "message should mention the bad edge, got: {}",
            issue.message,
        );
    }

    #[test]
    fn schema_error_edge_between_wrong_node_types() {
        // Pod-Manages->Node does not exist
        let result = validate_cypher("MATCH (p:Pod)-[:Manages]->(n:Node) RETURN p");
        let issue = result.expect_err("edge between wrong node types should be rejected");
        assert_eq!(issue.kind, QueryIssueKind::Schema);
        assert_eq!(issue.source, QueryIssueSource::Validator);
        assert!(
            issue.message.contains("Manages"),
            "message should mention the edge type, got: {}",
            issue.message,
        );
    }

    #[test]
    fn schema_stage_can_be_run_independently() {
        let query = parse_cypher("MATCH (p:Pod)-[:BelongsTo]->(c:Cluster) RETURN p")
            .expect("query should parse");
        let issue = validate_schema_query(&query).expect_err("schema validation should fail");
        assert_eq!(issue.kind, QueryIssueKind::Schema);
        assert_eq!(issue.source, QueryIssueSource::Validator);
    }

    // ── Valid queries ───────────────────────────────────────────────

    #[test]
    fn valid_simple_match_return() {
        let result = validate_cypher("MATCH (p:Pod) RETURN p");
        assert!(
            result.is_ok(),
            "simple MATCH+RETURN should pass: {result:?}"
        );
    }

    #[test]
    fn valid_multi_hop_path() {
        let result = validate_cypher(
            "MATCH (d:Deployment)-[:Manages]->(rs:ReplicaSet)-[:Manages]->(p:Pod) RETURN p",
        );
        assert!(result.is_ok(), "multi-hop path should pass: {result:?}");
    }

    #[test]
    fn valid_query_with_where_and_parameter() {
        let result = validate_cypher("MATCH (p:Pod) WHERE p.name = $name RETURN p");
        assert!(
            result.is_ok(),
            "query with WHERE and parameter should pass: {result:?}",
        );
    }

    #[test]
    fn valid_optional_match() {
        let result = validate_cypher(
            "MATCH (d:Deployment) OPTIONAL MATCH (d)-[:Manages]->(rs:ReplicaSet) RETURN d, rs",
        );
        assert!(result.is_ok(), "OPTIONAL MATCH should pass: {result:?}",);
    }

    #[test]
    fn valid_aggregation_with_count() {
        let result = validate_cypher("MATCH (p:Pod)-[:RunsOn]->(n:Node) RETURN n.name, count(p)");
        assert!(
            result.is_ok(),
            "aggregation query with count should pass: {result:?}",
        );
    }
}
