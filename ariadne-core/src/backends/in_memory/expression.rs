//! Cypher scalar-expression evaluation for the in-memory backend.
//!
//! Evaluation is side-effect free apart from query statistics and must produce
//! the same JSON value semantics used by projection and matching.

use super::{
    ClusterState, Expr, Literal, QueryStats, Result, Row, Value, compare_values, eval_exists,
    eval_list_slice,
};
use serde_json::Map;
use std::cmp::Ordering;
use std::collections::HashMap;

pub(super) fn eval_bool(
    expr: &Expr,
    row: &Row,
    state: &ClusterState,
    params: &HashMap<String, Value>,
    stats: &mut QueryStats,
) -> Result<bool> {
    match eval_expr(expr, row, state, params, stats)? {
        Value::Bool(b) => Ok(b),
        _ => Ok(false),
    }
}

pub(super) fn eval_expr(
    expr: &Expr,
    row: &Row,
    state: &ClusterState,
    params: &HashMap<String, Value>,
    stats: &mut QueryStats,
) -> Result<Value> {
    match expr {
        Expr::Literal(lit) => literal_to_value(lit, row, state, params, stats),
        Expr::Variable(name) => Ok(row.get(name).cloned().unwrap_or(Value::Null)),
        Expr::Star => Ok(Value::Null),
        Expr::PropertyAccess { expr, key } => {
            let base = eval_expr(expr, row, state, params, stats)?;
            Ok(base
                .as_object()
                .and_then(|obj| obj.get(key))
                .cloned()
                .unwrap_or(Value::Null))
        }
        Expr::IndexAccess { expr, index } => {
            let base = eval_expr(expr, row, state, params, stats)?;
            let idx = eval_expr(index, row, state, params, stats)?;
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
            let base = eval_expr(expr, row, state, params, stats)?;
            eval_list_slice(
                base,
                start.as_deref(),
                end.as_deref(),
                row,
                state,
                params,
                stats,
            )
        }
        Expr::UnaryOp { op, expr } => {
            let value = eval_expr(expr, row, state, params, stats)?;
            match op {
                ariadne_cypher::UnaryOp::Not => Ok(Value::Bool(!value.as_bool().unwrap_or(false))),
                ariadne_cypher::UnaryOp::Neg => Ok(Value::from(-value.as_f64().unwrap_or(0.0))),
                ariadne_cypher::UnaryOp::Pos => Ok(Value::from(value.as_f64().unwrap_or(0.0))),
            }
        }
        Expr::BinaryOp { op, left, right } => {
            eval_binary(op, left, right, row, state, params, stats)
        }
        Expr::IsNull { expr, negated } => {
            let value = eval_expr(expr, row, state, params, stats)?;
            let is_null = value.is_null();
            Ok(Value::Bool(if *negated { !is_null } else { is_null }))
        }
        Expr::In { expr, list } => {
            let value = eval_expr(expr, row, state, params, stats)?;
            let list_value = eval_expr(list, row, state, params, stats)?;
            let contains = match list_value {
                Value::Array(items) => items.iter().any(|item| item == &value),
                _ => false,
            };
            Ok(Value::Bool(contains))
        }
        Expr::HasLabel { expr, labels } => {
            let value = eval_expr(expr, row, state, params, stats)?;
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
            let exists = eval_exists(row, pattern, where_clause.as_deref(), state, params, stats)?;
            Ok(Value::Bool(exists))
        }
        Expr::ListComprehension {
            variable,
            list,
            where_clause,
            map,
        } => {
            let mut ctx = EvalContext {
                row,
                state,
                params,
                stats,
            };
            eval_list_comprehension(variable, list, where_clause.as_deref(), map, &mut ctx)
        }
        Expr::Quantifier {
            kind,
            variable,
            list,
            where_clause,
        } => {
            let mut ctx = EvalContext {
                row,
                state,
                params,
                stats,
            };
            eval_quantifier(kind, variable, list, where_clause.as_deref(), &mut ctx)
        }
        Expr::Case {
            base,
            alternatives,
            else_expr,
        } => {
            if let Some(base) = base {
                let base_value = eval_expr(base, row, state, params, stats)?;
                for (when_expr, then_expr) in alternatives {
                    let when_value = eval_expr(when_expr, row, state, params, stats)?;
                    let matches = compare_values(&base_value, &when_value)
                        .map(|ord| ord == Ordering::Equal)
                        .unwrap_or(false);
                    if matches {
                        return eval_expr(then_expr, row, state, params, stats);
                    }
                }
            } else {
                for (when_expr, then_expr) in alternatives {
                    if eval_bool(when_expr, row, state, params, stats)? {
                        return eval_expr(then_expr, row, state, params, stats);
                    }
                }
            }
            if let Some(else_expr) = else_expr {
                eval_expr(else_expr, row, state, params, stats)
            } else {
                Ok(Value::Null)
            }
        }
        Expr::FunctionCall { name, args } => eval_function(name, args, row, state, params, stats),
        Expr::CountStar => Err(std::io::Error::other("count(*) not valid here").into()),
        Expr::Parameter(name) => params.get(name).cloned().ok_or_else(|| {
            std::io::Error::other(format!("parameter not provided: ${name}")).into()
        }),
    }
}

struct EvalContext<'a> {
    row: &'a Row,
    state: &'a ClusterState,
    params: &'a HashMap<String, Value>,
    stats: &'a mut QueryStats,
}

fn eval_list_comprehension(
    variable: &str,
    list_expr: &Expr,
    where_clause: Option<&Expr>,
    map_expr: &Expr,
    ctx: &mut EvalContext<'_>,
) -> Result<Value> {
    let list_value = eval_expr(list_expr, ctx.row, ctx.state, ctx.params, ctx.stats)?;
    let items = match list_value {
        Value::Array(items) => items,
        _ => return Ok(Value::Array(Vec::new())),
    };
    let mut output = Vec::new();
    for item in items {
        let mut scoped = ctx.row.clone();
        scoped.insert(variable.to_string(), item);
        if let Some(where_clause) = where_clause
            && !eval_bool(where_clause, &scoped, ctx.state, ctx.params, ctx.stats)?
        {
            continue;
        }
        output.push(eval_expr(
            map_expr, &scoped, ctx.state, ctx.params, ctx.stats,
        )?);
    }
    Ok(Value::Array(output))
}

fn eval_quantifier(
    kind: &ariadne_cypher::QuantifierKind,
    variable: &str,
    list_expr: &Expr,
    where_clause: Option<&Expr>,
    ctx: &mut EvalContext<'_>,
) -> Result<Value> {
    let list_value = eval_expr(list_expr, ctx.row, ctx.state, ctx.params, ctx.stats)?;
    let items = match list_value {
        Value::Array(items) => items,
        _ => return Ok(Value::Bool(false)),
    };

    let mut matches = 0usize;
    for item in items {
        let mut scoped = ctx.row.clone();
        scoped.insert(variable.to_string(), item.clone());
        let passed = if let Some(where_clause) = where_clause {
            eval_bool(where_clause, &scoped, ctx.state, ctx.params, ctx.stats)?
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
    params: &HashMap<String, Value>,
    stats: &mut QueryStats,
) -> Result<Value> {
    let lower = name.to_ascii_lowercase();
    match lower.as_str() {
        "size" => {
            let target = args
                .first()
                .ok_or_else(|| std::io::Error::other("size requires one argument"))?;
            let value = eval_expr(target, row, state, params, stats)?;
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
            let value = eval_expr(target, row, state, params, stats)?;
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
                let value = eval_expr(arg, row, state, params, stats)?;
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
            let value = eval_expr(target, row, state, params, stats)?;
            Ok(Value::String(match value {
                Value::String(s) => s,
                other => other.to_string(),
            }))
        }
        "tointeger" | "toint" => {
            let target = args
                .first()
                .ok_or_else(|| std::io::Error::other("toInteger requires one argument"))?;
            let value = eval_expr(target, row, state, params, stats)?;
            let num = match value {
                Value::Number(n) => n.as_i64().unwrap_or(0),
                Value::String(s) => s.parse::<i64>().unwrap_or(0),
                Value::Bool(true) => 1,
                Value::Bool(false) => 0,
                _ => 0,
            };
            Ok(Value::from(num))
        }
        "tofloat" => {
            let target = args
                .first()
                .ok_or_else(|| std::io::Error::other("toFloat requires one argument"))?;
            let value = eval_expr(target, row, state, params, stats)?;
            let num = match value {
                Value::Number(n) => n.as_f64().unwrap_or(0.0),
                Value::String(s) => s.parse::<f64>().unwrap_or(0.0),
                Value::Bool(true) => 1.0,
                Value::Bool(false) => 0.0,
                _ => 0.0,
            };
            Ok(Value::from(num))
        }
        "labels" => {
            let target = args
                .first()
                .ok_or_else(|| std::io::Error::other("labels requires one argument"))?;
            let value = eval_expr(target, row, state, params, stats)?;
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
        "keys" => {
            let target = args
                .first()
                .ok_or_else(|| std::io::Error::other("keys requires one argument"))?;
            let value = eval_expr(target, row, state, params, stats)?;
            match value {
                Value::Object(map) => {
                    let mut keys: Vec<String> = map.keys().cloned().collect();
                    keys.sort();
                    Ok(Value::Array(keys.into_iter().map(Value::String).collect()))
                }
                Value::Null => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "replace" => {
            if args.len() < 3 {
                return Err(std::io::Error::other("replace requires three arguments").into());
            }
            let value = eval_expr(&args[0], row, state, params, stats)?;
            let search = eval_expr(&args[1], row, state, params, stats)?;
            let replacement = eval_expr(&args[2], row, state, params, stats)?;
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
    params: &HashMap<String, Value>,
    stats: &mut QueryStats,
) -> Result<Value> {
    use ariadne_cypher::BinaryOp::*;
    match op {
        Or => Ok(Value::Bool(
            eval_bool(left, row, state, params, stats)?
                || eval_bool(right, row, state, params, stats)?,
        )),
        And => Ok(Value::Bool(
            eval_bool(left, row, state, params, stats)?
                && eval_bool(right, row, state, params, stats)?,
        )),
        Xor => Ok(Value::Bool(
            eval_bool(left, row, state, params, stats)?
                ^ eval_bool(right, row, state, params, stats)?,
        )),
        Eq | Neq | Lt | Gt | Lte | Gte => {
            let l = eval_expr(left, row, state, params, stats)?;
            let r = eval_expr(right, row, state, params, stats)?;
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
            let l = eval_expr(left, row, state, params, stats)?;
            let r = eval_expr(right, row, state, params, stats)?;
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
            let l = eval_expr(left, row, state, params, stats)?
                .as_f64()
                .unwrap_or(0.0);
            let r = eval_expr(right, row, state, params, stats)?
                .as_f64()
                .unwrap_or(0.0);
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

pub(super) fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

pub(super) fn literal_to_value(
    lit: &Literal,
    row: &Row,
    state: &ClusterState,
    params: &HashMap<String, Value>,
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
                values.push(eval_expr(expr, row, state, params, stats)?);
            }
            Ok(Value::Array(values))
        }
        Literal::Map(entries) => {
            let mut map = Map::new();
            for (k, v) in entries {
                map.insert(k.clone(), eval_expr(v, row, state, params, stats)?);
            }
            Ok(Value::Object(map))
        }
    }
}
