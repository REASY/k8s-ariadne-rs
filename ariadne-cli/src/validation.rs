use std::collections::{HashMap, HashSet};
use std::fmt;

use ariadne_core::graph_schema;
use ariadne_core::types::{Edge, ResourceType};
use ariadne_cypher::{
    parse_query, validate_query, Clause, Expr, MatchClause, NodePattern, Pattern,
    RelationshipDirection, RelationshipPattern, ValidationMode,
};
use strum::IntoEnumIterator;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationIssueKind {
    Parse,
    Semantic,
    Schema,
}

#[derive(Debug, Clone)]
pub struct ValidationIssue {
    pub kind: ValidationIssueKind,
    pub message: String,
}

impl ValidationIssue {
    pub fn retriable(&self) -> bool {
        matches!(
            self.kind,
            ValidationIssueKind::Parse
                | ValidationIssueKind::Schema
                | ValidationIssueKind::Semantic
        )
    }

    pub fn feedback(&self) -> String {
        format!(
            "Validation failed ({:?}): {}. Fix the Cypher to match the schema and syntax.",
            self.kind, self.message
        )
    }
}

impl fmt::Display for ValidationIssue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ValidationIssue {}

pub fn validate_cypher(cypher: &str) -> Result<(), ValidationIssue> {
    let query = match parse_query(cypher) {
        Ok(query) => query,
        Err(err) => {
            tracing::error!(error = %err, cypher = %cypher, "Cypher parse failed");
            return Err(ValidationIssue {
                kind: ValidationIssueKind::Parse,
                message: err.to_string(),
            });
        }
    };
    if let Err(err) = validate_query(&query, ValidationMode::ReadOnly) {
        tracing::error!(error = %err, cypher = %cypher, "Cypher validation failed");
        return Err(ValidationIssue {
            kind: ValidationIssueKind::Semantic,
            message: err.to_string(),
        });
    }
    if let Err(err) = validate_schema(&query) {
        tracing::error!(error = %err, cypher = %cypher, "Cypher schema validation failed");
        return Err(err);
    }
    Ok(())
}

fn validate_schema(query: &ariadne_cypher::Query) -> Result<(), ValidationIssue> {
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
    Err(ValidationIssue {
        kind: ValidationIssueKind::Schema,
        message: issues.join(" | "),
    })
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
    if let Some(var) = &node.variable {
        if let Some(labels) = var_labels.get(var) {
            return Some(labels.iter().cloned().collect());
        }
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
