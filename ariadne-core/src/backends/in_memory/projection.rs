use super::*;

pub(super) fn project_rows_internal(
    rows: Vec<Row>,
    items: &[ProjectionItem],
    state: &ClusterState,
    params: &HashMap<String, Value>,
    stats: &mut QueryStats,
) -> Result<Vec<Row>> {
    let has_agg = items.iter().any(|item| contains_aggregate_expr(&item.expr));

    if has_agg {
        return project_rows_aggregate(rows, items, state, params, stats);
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
                    let value = eval_expr(&item.expr, &row, state, params, stats)?;
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
    params: &HashMap<String, Value>,
    stats: &mut QueryStats,
) -> Result<Vec<Row>> {
    let mut non_agg_indices = Vec::new();
    for (idx, item) in items.iter().enumerate() {
        if !contains_aggregate_expr(&item.expr) {
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
            let value = eval_expr(&items[*idx].expr, &row, state, params, stats)?;
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
            let value = if contains_aggregate_expr(&item.expr) {
                eval_aggregate_expr(&item.expr, &group_rows, state, params, stats)?
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
    params: &HashMap<String, Value>,
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
                    let value = eval_expr(target, row, state, params, stats)?;
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
                    if let Some(v) = eval_expr(target, row, state, params, stats)?.as_f64() {
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
                    if let Some(v) = eval_expr(target, row, state, params, stats)?.as_f64() {
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
                    let value = eval_expr(target, row, state, params, stats)?;
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
                    values.push(eval_expr(target, row, state, params, stats)?);
                }
                Ok(Value::Array(values))
            }
            _ => Err(std::io::Error::other("unsupported aggregate function").into()),
        },
        Expr::IndexAccess { expr, index } => {
            let base = eval_aggregate(expr, rows, state, params, stats)?;
            let sample = rows.first().cloned().unwrap_or_default();
            let idx = eval_expr(index, &sample, state, params, stats)?;
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
            let base = eval_aggregate(expr, rows, state, params, stats)?;
            let sample = rows.first().cloned().unwrap_or_default();
            eval_list_slice(
                base,
                start.as_deref(),
                end.as_deref(),
                &sample,
                state,
                params,
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

fn contains_aggregate_expr(expr: &Expr) -> bool {
    match expr {
        Expr::CountStar => true,
        Expr::FunctionCall { name, .. } => matches!(
            name.to_ascii_lowercase().as_str(),
            "count" | "sum" | "avg" | "min" | "max" | "collect"
        ),
        Expr::UnaryOp { expr, .. } => contains_aggregate_expr(expr),
        Expr::BinaryOp { left, right, .. } => {
            contains_aggregate_expr(left) || contains_aggregate_expr(right)
        }
        Expr::PropertyAccess { expr, .. } => contains_aggregate_expr(expr),
        Expr::IndexAccess { expr, index } => {
            contains_aggregate_expr(expr) || contains_aggregate_expr(index)
        }
        Expr::ListSlice { expr, start, end } => {
            contains_aggregate_expr(expr)
                || start.as_deref().is_some_and(contains_aggregate_expr)
                || end.as_deref().is_some_and(contains_aggregate_expr)
        }
        Expr::IsNull { expr, .. } => contains_aggregate_expr(expr),
        Expr::In { expr, list } => contains_aggregate_expr(expr) || contains_aggregate_expr(list),
        Expr::Case {
            base,
            alternatives,
            else_expr,
        } => {
            base.as_deref().is_some_and(contains_aggregate_expr)
                || alternatives
                    .iter()
                    .any(|(a, b)| contains_aggregate_expr(a) || contains_aggregate_expr(b))
                || else_expr.as_deref().is_some_and(contains_aggregate_expr)
        }
        Expr::ListComprehension {
            list,
            where_clause,
            map,
            ..
        } => {
            contains_aggregate_expr(list)
                || where_clause.as_deref().is_some_and(contains_aggregate_expr)
                || contains_aggregate_expr(map)
        }
        Expr::Quantifier {
            list, where_clause, ..
        } => {
            contains_aggregate_expr(list)
                || where_clause.as_deref().is_some_and(contains_aggregate_expr)
        }
        _ => false,
    }
}

fn eval_aggregate_expr(
    expr: &Expr,
    rows: &[Row],
    state: &ClusterState,
    params: &HashMap<String, Value>,
    stats: &mut QueryStats,
) -> Result<Value> {
    match expr {
        Expr::CountStar
        | Expr::FunctionCall { .. }
        | Expr::IndexAccess { .. }
        | Expr::ListSlice { .. } => eval_aggregate(expr, rows, state, params, stats),
        Expr::Literal(lit) => literal_to_value(lit, &Row::new(), state, params, stats),
        Expr::UnaryOp { op, expr } => {
            let value = eval_aggregate_expr(expr, rows, state, params, stats)?;
            match op {
                ariadne_cypher::UnaryOp::Not => Ok(Value::Bool(!value.as_bool().unwrap_or(false))),
                ariadne_cypher::UnaryOp::Neg => Ok(Value::from(-value.as_f64().unwrap_or(0.0))),
                ariadne_cypher::UnaryOp::Pos => Ok(Value::from(value.as_f64().unwrap_or(0.0))),
            }
        }
        Expr::BinaryOp { op, left, right } => {
            let l = eval_aggregate_expr(left, rows, state, params, stats)?;
            let r = eval_aggregate_expr(right, rows, state, params, stats)?;
            eval_binary_values(op, l, r)
        }
        Expr::Parameter(name) => params.get(name).cloned().ok_or_else(|| {
            std::io::Error::other(format!("parameter not provided: ${name}")).into()
        }),
        _ => Err(std::io::Error::other("unsupported aggregate expression shape").into()),
    }
}

pub(super) fn eval_binary_values(
    op: &ariadne_cypher::BinaryOp,
    left: Value,
    right: Value,
) -> Result<Value> {
    use ariadne_cypher::BinaryOp::*;
    match op {
        Or => Ok(Value::Bool(
            left.as_bool().unwrap_or(false) || right.as_bool().unwrap_or(false),
        )),
        And => Ok(Value::Bool(
            left.as_bool().unwrap_or(false) && right.as_bool().unwrap_or(false),
        )),
        Xor => Ok(Value::Bool(
            left.as_bool().unwrap_or(false) ^ right.as_bool().unwrap_or(false),
        )),
        Eq | Neq | Lt | Gt | Lte | Gte => {
            let cmp = compare_values(&left, &right);
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
            if left.is_null() || right.is_null() {
                return Ok(Value::Bool(false));
            }
            let left_str = value_to_string(&left);
            let right_str = value_to_string(&right);
            let result = match op {
                StartsWith => left_str.starts_with(&right_str),
                EndsWith => left_str.ends_with(&right_str),
                Contains => left_str.contains(&right_str),
                _ => false,
            };
            Ok(Value::Bool(result))
        }
        Add | Sub | Mul | Div | Mod | Pow => {
            let l = left.as_f64().unwrap_or(0.0);
            let r = right.as_f64().unwrap_or(0.0);
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

pub(super) fn eval_list_slice(
    base: Value,
    start: Option<&Expr>,
    end: Option<&Expr>,
    row: &Row,
    state: &ClusterState,
    params: &HashMap<String, Value>,
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
        let value = eval_expr(start_expr, row, state, params, stats)?;
        if value.is_null() {
            start_idx = 0;
        } else if let Some(v) = value.as_i64() {
            start_idx = v;
        } else {
            return Ok(Value::Null);
        }
    }
    if let Some(end_expr) = end {
        let value = eval_expr(end_expr, row, state, params, stats)?;
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

pub(super) fn distinct_rows(rows: Vec<Row>) -> Vec<Row> {
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

pub(super) fn apply_skip_limit(
    mut rows: Vec<Row>,
    skip: Option<&Expr>,
    limit: Option<&Expr>,
    state: &ClusterState,
    params: &HashMap<String, Value>,
    stats: &mut QueryStats,
) -> Result<Vec<Row>> {
    if let Some(skip_expr) = skip {
        let skip_count = eval_expr(skip_expr, &Row::new(), state, params, stats)?
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
        let limit_count = eval_expr(limit_expr, &Row::new(), state, params, stats)?
            .as_i64()
            .unwrap_or(0)
            .max(0) as usize;
        rows.truncate(limit_count);
    }

    Ok(rows)
}

pub(super) fn sort_rows(
    rows: Vec<Row>,
    order: &OrderBy,
    state: &ClusterState,
    params: &HashMap<String, Value>,
    stats: &mut QueryStats,
) -> Result<Vec<Row>> {
    let mut rows_with_keys = Vec::with_capacity(rows.len());
    for row in rows {
        let mut keys = Vec::new();
        for item in &order.items {
            keys.push(eval_expr(&item.expr, &row, state, params, stats)?);
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

pub(super) fn compare_values(left: &Value, right: &Value) -> Option<Ordering> {
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
