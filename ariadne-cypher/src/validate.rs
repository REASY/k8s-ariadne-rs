use crate::ast::*;
use crate::CypherError;

#[derive(Debug, Clone, Copy)]
pub enum ValidationMode {
    ReadOnly,
    Engine,
}

pub fn validate_query(query: &Query, mode: ValidationMode) -> Result<(), CypherError> {
    if query.clauses.is_empty() {
        return Err(CypherError::semantic(
            "query contains no clauses",
            Span {
                start_byte: 0,
                end_byte: 0,
                start_row: 0,
                start_col: 0,
                end_row: 0,
                end_col: 0,
            },
        ));
    }

    if let Some(last) = query.clauses.last() {
        match last {
            Clause::Return(_) => {}
            _ => {
                return Err(CypherError::semantic(
                    "query must end with RETURN",
                    clause_span(last),
                ));
            }
        }
    }

    for clause in &query.clauses {
        match clause {
            Clause::Updating(updating) => {
                return Err(CypherError::semantic(
                    format!("updating clause not supported: {:?}", updating.kind),
                    updating.span,
                ));
            }
            Clause::Call(call) => {
                if matches!(mode, ValidationMode::ReadOnly | ValidationMode::Engine) {
                    return Err(CypherError::semantic(
                        "CALL clauses are not supported",
                        call.span,
                    ));
                }
            }
            Clause::Match(m) => {
                validate_pattern(&m.pattern)?;
            }
            _ => {}
        }
    }

    if matches!(mode, ValidationMode::Engine) {
        for clause in &query.clauses {
            match clause {
                Clause::Match(m) => {
                    validate_engine_pattern(&m.pattern)?;
                    if let Some(where_clause) = &m.where_clause {
                        validate_engine_expr(where_clause)?;
                    }
                }
                Clause::Unwind(u) => {
                    validate_engine_expr(&u.expression)?;
                }
                Clause::With(w) => {
                    validate_projection_items(&w.items)?;
                    if let Some(where_clause) = &w.where_clause {
                        validate_engine_expr(where_clause)?;
                    }
                    if let Some(order) = &w.order {
                        for item in &order.items {
                            validate_engine_expr(&item.expr)?;
                        }
                    }
                    if let Some(skip) = &w.skip {
                        validate_engine_expr(skip)?;
                    }
                    if let Some(limit) = &w.limit {
                        validate_engine_expr(limit)?;
                    }
                }
                Clause::Return(r) => {
                    validate_projection_items(&r.items)?;
                    if let Some(order) = &r.order {
                        for item in &order.items {
                            validate_engine_expr(&item.expr)?;
                        }
                    }
                    if let Some(skip) = &r.skip {
                        validate_engine_expr(skip)?;
                    }
                    if let Some(limit) = &r.limit {
                        validate_engine_expr(limit)?;
                    }
                }
                other => {
                    return Err(CypherError::semantic(
                        "unsupported clause for in-memory engine",
                        clause_span(other),
                    ));
                }
            }
        }
    }

    Ok(())
}

fn validate_pattern(pattern: &Pattern) -> Result<(), CypherError> {
    match pattern {
        Pattern::Node(_) => Ok(()),
        Pattern::Relationship(rel) => {
            if rel.rel.types.len() > 1 {
                return Err(CypherError::semantic(
                    "relationship type unions not supported",
                    rel.span,
                ));
            }
            Ok(())
        }
    }
}

fn validate_engine_pattern(pattern: &Pattern) -> Result<(), CypherError> {
    match pattern {
        Pattern::Node(node) => {
            if node.labels.len() > 1 {
                return Err(CypherError::semantic(
                    "multiple labels are not supported by the in-memory engine",
                    node.span,
                ));
            }
            Ok(())
        }
        Pattern::Relationship(rel) => {
            if rel.left.labels.len() > 1 || rel.right.labels.len() > 1 {
                return Err(CypherError::semantic(
                    "multiple labels are not supported by the in-memory engine",
                    rel.span,
                ));
            }
            Ok(())
        }
    }
}

fn validate_projection_items(items: &[ProjectionItem]) -> Result<(), CypherError> {
    let mut has_agg = false;
    for item in items {
        if is_aggregate_expr(&item.expr) {
            has_agg = true;
            if !is_top_level_aggregate(&item.expr) {
                return Err(CypherError::semantic(
                    "aggregate functions must be top-level expressions",
                    dummy_span(),
                ));
            }
        } else {
            validate_engine_expr(&item.expr)?;
        }
    }

    if has_agg {
        for item in items {
            if matches!(&item.expr, Expr::Star) {
                return Err(CypherError::semantic(
                    "RETURN/WITH * cannot be combined with aggregation",
                    dummy_span(),
                ));
            }
        }
    }

    Ok(())
}

fn validate_engine_expr(expr: &Expr) -> Result<(), CypherError> {
    match expr {
        Expr::Literal(_) => Ok(()),
        Expr::Variable(_) => Ok(()),
        Expr::Star => Ok(()),
        Expr::PropertyAccess { expr, .. } => validate_engine_expr(expr),
        Expr::IndexAccess { expr, index } => {
            validate_engine_expr(expr)?;
            validate_engine_expr(index)
        }
        Expr::FunctionCall { name, args } => {
            if is_aggregate_name(name) {
                return Err(CypherError::semantic(
                    "aggregate functions must appear in projection",
                    dummy_span(),
                ));
            }
            for arg in args {
                validate_engine_expr(arg)?;
            }
            Ok(())
        }
        Expr::CountStar => Err(CypherError::semantic(
            "count(*) must appear in projection",
            dummy_span(),
        )),
        Expr::UnaryOp { expr, .. } => validate_engine_expr(expr),
        Expr::BinaryOp { left, right, .. } => {
            validate_engine_expr(left)?;
            validate_engine_expr(right)
        }
        Expr::HasLabel { expr, .. } => validate_engine_expr(expr),
        Expr::Case {
            base,
            alternatives,
            else_expr,
        } => {
            if let Some(base) = base {
                validate_engine_expr(base)?;
            }
            for (when_expr, then_expr) in alternatives {
                validate_engine_expr(when_expr)?;
                validate_engine_expr(then_expr)?;
            }
            if let Some(else_expr) = else_expr {
                validate_engine_expr(else_expr)?;
            }
            Ok(())
        }
        Expr::IsNull { expr, .. } => validate_engine_expr(expr),
        Expr::In { expr, list } => {
            validate_engine_expr(expr)?;
            validate_engine_expr(list)
        }
        Expr::Parameter(_) => Err(CypherError::semantic(
            "parameters are not supported by the in-memory engine",
            dummy_span(),
        )),
    }
}

fn is_aggregate_expr(expr: &Expr) -> bool {
    matches!(expr, Expr::CountStar)
        || matches!(expr, Expr::FunctionCall { name, .. } if is_aggregate_name(name))
}

fn is_top_level_aggregate(expr: &Expr) -> bool {
    matches!(expr, Expr::CountStar)
        || matches!(expr, Expr::FunctionCall { name, .. } if is_aggregate_name(name))
}

fn is_aggregate_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "count" | "sum" | "avg" | "min" | "max" | "collect"
    )
}

fn dummy_span() -> Span {
    Span {
        start_byte: 0,
        end_byte: 0,
        start_row: 0,
        start_col: 0,
        end_row: 0,
        end_col: 0,
    }
}

fn clause_span(clause: &Clause) -> Span {
    match clause {
        Clause::Match(c) => c.span,
        Clause::Unwind(c) => c.span,
        Clause::With(c) => c.span,
        Clause::Return(c) => c.span,
        Clause::Call(c) => c.span,
        Clause::Updating(c) => c.span,
    }
}
