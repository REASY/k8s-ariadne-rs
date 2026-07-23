use super::*;

pub(super) fn parse_expression(node: Node, input: &str) -> Result<Expr, CypherError> {
    match node.kind() {
        "expression" => {
            let child = named_children(node)
                .into_iter()
                .next()
                .ok_or_else(|| CypherError::missing("expression", Span::from_node(node)))?;
            parse_expression(child, input)
        }
        "or_expression" => parse_binary(node, input, BinaryOp::Or),
        "xor_expression" => parse_binary(node, input, BinaryOp::Xor),
        "and_expression" => parse_binary(node, input, BinaryOp::And),
        "additive_expression" => parse_additive(node, input),
        "multiplicative_expression" => parse_multiplicative(node, input),
        "exponential_expression" => parse_exponential(node, input),
        "unary_expression" => parse_unary(node, input),
        "not_expression" => {
            let child = named_children(node)
                .into_iter()
                .next()
                .ok_or_else(|| CypherError::missing("not expression", Span::from_node(node)))?;
            Ok(Expr::UnaryOp {
                op: UnaryOp::Not,
                expr: Box::new(parse_expression(child, input)?),
            })
        }
        "comparison_expression" => parse_comparison(node, input),
        "string_list_null_predicate_expression" => parse_predicate(node, input),
        "list_operator_expression" => parse_index_access(node, input),
        "property_or_labels_expression" => parse_property_access(node, input),
        "parenthesized_expression" => parse_parenthesized(node, input),
        "case_expression" => parse_case_expression(node, input),
        "list_comprehension" => parse_list_comprehension(node, input),
        "literal" => parse_literal(node, input),
        "string_literal" | "number_literal" | "boolean_literal" | "null_literal"
        | "list_literal" | "map_literal" => parse_literal(node, input),
        "variable" => Ok(Expr::Variable(parse_identifier(node, input)?)),
        "parameter" => Ok(Expr::Parameter(parse_parameter(node, input)?)),
        "function_invocation" => parse_function(node, input),
        "existential_subquery" => parse_existential_subquery(node, input),
        "quantifier" => parse_quantifier(node, input),
        "atom" => parse_atom(node, input),
        other => Err(CypherError::unsupported(other, Span::from_node(node))),
    }
}

fn parse_existential_subquery(node: Node, input: &str) -> Result<Expr, CypherError> {
    let mut pattern_node = None;
    let mut where_node = None;
    for child in named_children(node) {
        match child.kind() {
            "pattern" => pattern_node = Some(child),
            "where" => where_node = Some(child),
            "regular_query" => {
                return Err(CypherError::unsupported(
                    "exists subquery with regular query",
                    Span::from_node(child),
                ));
            }
            _ => {}
        }
    }
    let pattern_node = pattern_node
        .ok_or_else(|| CypherError::missing("exists pattern", Span::from_node(node)))?;
    let pattern = parse_pattern(pattern_node, input)?;
    let where_clause = if let Some(where_node) = where_node {
        Some(Box::new(parse_where(where_node, input)?))
    } else {
        None
    };
    Ok(Expr::Exists {
        pattern,
        where_clause,
    })
}

fn parse_atom(node: Node, input: &str) -> Result<Expr, CypherError> {
    if node.named_child_count() > 0 {
        let child = named_children(node)
            .into_iter()
            .next()
            .ok_or_else(|| CypherError::missing("atom", Span::from_node(node)))?;
        return parse_expression(child, input);
    }
    let text = node_text(node, input)?;
    if text.trim().eq_ignore_ascii_case("count(*)") {
        return Ok(Expr::CountStar);
    }
    Err(CypherError::unsupported("atom", Span::from_node(node)))
}

fn parse_binary(node: Node, input: &str, op: BinaryOp) -> Result<Expr, CypherError> {
    let mut named = named_children(node).into_iter();
    let left = named
        .next()
        .ok_or_else(|| CypherError::missing("binary left", Span::from_node(node)))?;
    let right = named
        .next()
        .ok_or_else(|| CypherError::missing("binary right", Span::from_node(node)))?;
    Ok(Expr::BinaryOp {
        op,
        left: Box::new(parse_expression(left, input)?),
        right: Box::new(parse_expression(right, input)?),
    })
}

fn parse_additive(node: Node, input: &str) -> Result<Expr, CypherError> {
    let mut named = named_children(node).into_iter();
    let left = named
        .next()
        .ok_or_else(|| CypherError::missing("additive left", Span::from_node(node)))?;
    let right = named
        .next()
        .ok_or_else(|| CypherError::missing("additive right", Span::from_node(node)))?;
    let op_text = find_operator(node, input, &["+", "-"])?;
    let op = if op_text == "-" {
        BinaryOp::Sub
    } else {
        BinaryOp::Add
    };
    Ok(Expr::BinaryOp {
        op,
        left: Box::new(parse_expression(left, input)?),
        right: Box::new(parse_expression(right, input)?),
    })
}

fn parse_multiplicative(node: Node, input: &str) -> Result<Expr, CypherError> {
    let mut named = named_children(node).into_iter();
    let left = named
        .next()
        .ok_or_else(|| CypherError::missing("multiplicative left", Span::from_node(node)))?;
    let right = named
        .next()
        .ok_or_else(|| CypherError::missing("multiplicative right", Span::from_node(node)))?;
    let op_text = find_operator(node, input, &["*", "/", "%"])?;
    let op = match op_text.as_str() {
        "*" => BinaryOp::Mul,
        "/" => BinaryOp::Div,
        "%" => BinaryOp::Mod,
        _ => BinaryOp::Mul,
    };
    Ok(Expr::BinaryOp {
        op,
        left: Box::new(parse_expression(left, input)?),
        right: Box::new(parse_expression(right, input)?),
    })
}

fn parse_exponential(node: Node, input: &str) -> Result<Expr, CypherError> {
    let mut named = named_children(node).into_iter();
    let left = named
        .next()
        .ok_or_else(|| CypherError::missing("exponential left", Span::from_node(node)))?;
    let right = named
        .next()
        .ok_or_else(|| CypherError::missing("exponential right", Span::from_node(node)))?;
    let _ = find_operator(node, input, &["^"])?;
    Ok(Expr::BinaryOp {
        op: BinaryOp::Pow,
        left: Box::new(parse_expression(left, input)?),
        right: Box::new(parse_expression(right, input)?),
    })
}

fn parse_unary(node: Node, input: &str) -> Result<Expr, CypherError> {
    let child = named_children(node)
        .into_iter()
        .next()
        .ok_or_else(|| CypherError::missing("unary expression", Span::from_node(node)))?;
    let op_text = find_operator(node, input, &["+", "-"])?;
    let op = if op_text == "-" {
        UnaryOp::Neg
    } else {
        UnaryOp::Pos
    };
    Ok(Expr::UnaryOp {
        op,
        expr: Box::new(parse_expression(child, input)?),
    })
}

fn find_operator(node: Node, input: &str, ops: &[&str]) -> Result<String, CypherError> {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if !child.is_named() && ops.iter().any(|op| *op == child.kind()) {
            return Ok(child.kind().to_string());
        }
    }
    let text = node_text(node, input)?;
    for op in ops {
        if text.contains(op) {
            return Ok(op.to_string());
        }
    }
    Err(CypherError::unsupported("operator", Span::from_node(node)))
}

fn parse_comparison(node: Node, input: &str) -> Result<Expr, CypherError> {
    let mut named = named_children(node).into_iter();
    let left = named
        .next()
        .ok_or_else(|| CypherError::missing("comparison left", Span::from_node(node)))?;
    let right = named
        .next()
        .ok_or_else(|| CypherError::missing("comparison right", Span::from_node(node)))?;
    let text = node_text(node, input)?;
    let op = if text.contains("<>") {
        BinaryOp::Neq
    } else if text.contains("<=") {
        BinaryOp::Lte
    } else if text.contains(">=") {
        BinaryOp::Gte
    } else if text.contains('=') {
        BinaryOp::Eq
    } else if text.contains('<') {
        BinaryOp::Lt
    } else if text.contains('>') {
        BinaryOp::Gt
    } else {
        return Err(CypherError::unsupported(
            "comparison operator",
            Span::from_node(node),
        ));
    };
    Ok(Expr::BinaryOp {
        op,
        left: Box::new(parse_expression(left, input)?),
        right: Box::new(parse_expression(right, input)?),
    })
}

fn parse_predicate(node: Node, input: &str) -> Result<Expr, CypherError> {
    let mut named = named_children(node).into_iter();
    let left = named
        .next()
        .ok_or_else(|| CypherError::missing("predicate left", Span::from_node(node)))?;
    let predicate = named
        .next()
        .ok_or_else(|| CypherError::missing("predicate", Span::from_node(node)))?;
    match predicate.kind() {
        "list_predicate_expression" => {
            let right = named_children(predicate)
                .into_iter()
                .find(|child| child.kind() == "expression")
                .ok_or_else(|| {
                    CypherError::missing("list predicate expression", Span::from_node(predicate))
                })?;
            Ok(Expr::In {
                expr: Box::new(parse_expression(left, input)?),
                list: Box::new(parse_expression(right, input)?),
            })
        }
        "null_predicate_expression" => {
            let text = node_text(predicate, input)?.to_ascii_lowercase();
            let negated = text.contains("not");
            Ok(Expr::IsNull {
                expr: Box::new(parse_expression(left, input)?),
                negated,
            })
        }
        "string_predicate_expression" => {
            let text = node_text(predicate, input)?.to_ascii_lowercase();
            let right = named_children(predicate)
                .into_iter()
                .find(|child| child.kind() == "expression")
                .ok_or_else(|| {
                    CypherError::missing("string predicate expression", Span::from_node(predicate))
                })?;
            let op = if text.contains("starts with") {
                BinaryOp::StartsWith
            } else if text.contains("ends with") {
                BinaryOp::EndsWith
            } else if text.contains("contains") {
                BinaryOp::Contains
            } else {
                return Err(CypherError::unsupported(
                    "string predicate operator",
                    Span::from_node(predicate),
                ));
            };
            Ok(Expr::BinaryOp {
                op,
                left: Box::new(parse_expression(left, input)?),
                right: Box::new(parse_expression(right, input)?),
            })
        }
        other => Err(CypherError::unsupported(other, Span::from_node(predicate))),
    }
}

fn parse_case_expression(node: Node, input: &str) -> Result<Expr, CypherError> {
    let mut base: Option<Expr> = None;
    let mut alternatives: Vec<(Expr, Expr)> = Vec::new();
    let mut else_expr: Option<Expr> = None;

    for child in named_children(node) {
        match child.kind() {
            "expression" => {
                if base.is_none() && alternatives.is_empty() {
                    base = Some(parse_expression(child, input)?);
                } else {
                    else_expr = Some(parse_expression(child, input)?);
                }
            }
            "case_alternatives" => {
                let mut iter = named_children(child).into_iter();
                let when_expr = iter.next().ok_or_else(|| {
                    CypherError::missing("case when expression", Span::from_node(child))
                })?;
                let then_expr = iter.next().ok_or_else(|| {
                    CypherError::missing("case then expression", Span::from_node(child))
                })?;
                alternatives.push((
                    parse_expression(when_expr, input)?,
                    parse_expression(then_expr, input)?,
                ));
            }
            _ => {}
        }
    }

    Ok(Expr::Case {
        base: base.map(Box::new),
        alternatives,
        else_expr: else_expr.map(Box::new),
    })
}

fn parse_list_comprehension(node: Node, input: &str) -> Result<Expr, CypherError> {
    let filter_node = named_children(node)
        .into_iter()
        .find(|child| child.kind() == "filter_expression")
        .ok_or_else(|| CypherError::missing("list comprehension filter", Span::from_node(node)))?;
    let (variable, list, where_clause) = parse_filter_expression(filter_node, input)?;
    let map_expr = named_children(node)
        .into_iter()
        .find(|child| child.kind() == "expression")
        .map(|expr| parse_expression(expr, input))
        .transpose()?
        .unwrap_or_else(|| Expr::Variable(variable.clone()));

    Ok(Expr::ListComprehension {
        variable,
        list: Box::new(list),
        where_clause: where_clause.map(Box::new),
        map: Box::new(map_expr),
    })
}

fn parse_quantifier(node: Node, input: &str) -> Result<Expr, CypherError> {
    let filter_node = named_children(node)
        .into_iter()
        .find(|child| child.kind() == "filter_expression")
        .ok_or_else(|| CypherError::missing("quantifier filter", Span::from_node(node)))?;
    let (variable, list, where_clause) = parse_filter_expression(filter_node, input)?;
    let keyword = node_text(node, input)?.trim_start().to_ascii_lowercase();
    let kind = if keyword.starts_with("any") {
        QuantifierKind::Any
    } else if keyword.starts_with("all") {
        QuantifierKind::All
    } else if keyword.starts_with("none") {
        QuantifierKind::None
    } else if keyword.starts_with("single") {
        QuantifierKind::Single
    } else {
        return Err(CypherError::unsupported(
            "quantifier keyword",
            Span::from_node(node),
        ));
    };
    Ok(Expr::Quantifier {
        kind,
        variable,
        list: Box::new(list),
        where_clause: where_clause.map(Box::new),
    })
}

fn parse_filter_expression(
    node: Node,
    input: &str,
) -> Result<(String, Expr, Option<Expr>), CypherError> {
    let id_in_coll = named_children(node)
        .into_iter()
        .find(|child| child.kind() == "id_in_coll")
        .ok_or_else(|| CypherError::missing("filter id_in_coll", Span::from_node(node)))?;
    let mut id_named = named_children(id_in_coll).into_iter();
    let var_node = id_named
        .find(|child| child.kind() == "variable")
        .ok_or_else(|| CypherError::missing("filter variable", Span::from_node(id_in_coll)))?;
    let list_node = named_children(id_in_coll)
        .into_iter()
        .find(|child| child.kind() == "expression")
        .ok_or_else(|| CypherError::missing("filter list", Span::from_node(id_in_coll)))?;
    let variable = parse_identifier(var_node, input)?;
    let list = parse_expression(list_node, input)?;
    let where_clause = named_children(node)
        .into_iter()
        .find(|child| child.kind() == "where")
        .map(|where_node| parse_where(where_node, input))
        .transpose()?;
    Ok((variable, list, where_clause))
}

fn parse_index_access(node: Node, input: &str) -> Result<Expr, CypherError> {
    let named = named_children(node);
    let base = named
        .first()
        .ok_or_else(|| CypherError::missing("index base", Span::from_node(node)))?;
    let base_expr = parse_expression(*base, input)?;
    let text = node_text(node, input)?;
    let has_slice = text.contains("..");
    if has_slice {
        let mut start: Option<Expr> = None;
        let mut end: Option<Expr> = None;
        if named.len() >= 2 {
            let first = named[1];
            let first_expr = parse_expression(first, input)?;
            if named.len() >= 3 {
                start = Some(first_expr);
                end = Some(parse_expression(named[2], input)?);
            } else {
                let slice_pos = text
                    .find("..")
                    .ok_or_else(|| CypherError::unsupported("list slice", Span::from_node(node)))?;
                let bound_start = first.start_byte().saturating_sub(node.start_byte());
                if bound_start < slice_pos {
                    start = Some(first_expr);
                } else {
                    end = Some(first_expr);
                }
            }
        }
        return Ok(Expr::ListSlice {
            expr: Box::new(base_expr),
            start: start.map(Box::new),
            end: end.map(Box::new),
        });
    }

    let index = named
        .get(1)
        .ok_or_else(|| CypherError::missing("index expression", Span::from_node(node)))?;
    Ok(Expr::IndexAccess {
        expr: Box::new(base_expr),
        index: Box::new(parse_expression(*index, input)?),
    })
}

fn parse_property_access(node: Node, input: &str) -> Result<Expr, CypherError> {
    let mut named = named_children(node).into_iter();
    let base = named
        .next()
        .ok_or_else(|| CypherError::missing("property base", Span::from_node(node)))?;
    let mut expr = parse_expression(base, input)?;

    for child in named {
        match child.kind() {
            "property_lookup" => {
                let key = named_children(child)
                    .into_iter()
                    .find(|c| c.kind() == "property_key_name")
                    .ok_or_else(|| CypherError::missing("property key", Span::from_node(child)))?;
                expr = Expr::PropertyAccess {
                    expr: Box::new(expr),
                    key: parse_identifier(key, input)?,
                };
            }
            "node_labels" => {
                let labels = parse_node_labels(child, input)?;
                expr = Expr::HasLabel {
                    expr: Box::new(expr),
                    labels,
                };
            }
            _ => {}
        }
    }

    Ok(expr)
}

fn parse_node_labels(node: Node, input: &str) -> Result<Vec<String>, CypherError> {
    let mut labels = Vec::new();
    for child in named_children(node) {
        match child.kind() {
            "node_label" => labels.push(parse_label(child, input)?),
            "label_name" => labels.push(parse_identifier(child, input)?),
            _ => {}
        }
    }
    if labels.is_empty() {
        return Err(CypherError::missing("label name", Span::from_node(node)));
    }
    Ok(labels)
}

fn parse_parenthesized(node: Node, input: &str) -> Result<Expr, CypherError> {
    let child = named_children(node)
        .into_iter()
        .next()
        .ok_or_else(|| CypherError::missing("parenthesized expression", Span::from_node(node)))?;
    parse_expression(child, input)
}

fn parse_function(node: Node, input: &str) -> Result<Expr, CypherError> {
    let name_node = named_children(node)
        .into_iter()
        .find(|child| child.kind() == "function_name")
        .ok_or_else(|| CypherError::missing("function name", Span::from_node(node)))?;
    let name = node_text(name_node, input)?.trim().to_string();

    let mut args = Vec::new();
    for child in named_children(node) {
        if child.kind() == "expression" {
            args.push(parse_expression(child, input)?);
        }
    }
    Ok(Expr::FunctionCall { name, args })
}

fn parse_literal(node: Node, input: &str) -> Result<Expr, CypherError> {
    let kind = node.kind();
    let lit = match kind {
        "literal" => {
            let child = named_children(node)
                .into_iter()
                .next()
                .ok_or_else(|| CypherError::missing("literal", Span::from_node(node)))?;
            return parse_literal(child, input);
        }
        "string_literal" => Literal::String(unescape_string(node_text(node, input)?)),
        "number_literal" => parse_number(node_text(node, input)?)?,
        "boolean_literal" => {
            let text = node_text(node, input)?.to_ascii_lowercase();
            Literal::Boolean(text.trim() == "true")
        }
        "null_literal" => Literal::Null,
        "list_literal" => {
            let mut items = Vec::new();
            for child in named_children(node) {
                if child.kind() == "expression" {
                    items.push(parse_expression(child, input)?);
                }
            }
            Literal::List(items)
        }
        "map_literal" => {
            let mut entries = Vec::new();
            let mut iter = named_children(node).into_iter().peekable();
            while let Some(key_node) = iter.next() {
                if key_node.kind() != "property_key_name" {
                    continue;
                }
                let value_node = iter
                    .next()
                    .ok_or_else(|| CypherError::missing("map value", Span::from_node(node)))?;
                if value_node.kind() != "expression" {
                    return Err(CypherError::missing("map value", Span::from_node(node)));
                }
                let key = parse_identifier(key_node, input)?;
                let value = parse_expression(value_node, input)?;
                entries.push((key, value));
            }
            Literal::Map(entries)
        }
        other => {
            return Err(CypherError::unsupported(other, Span::from_node(node)));
        }
    };
    Ok(Expr::Literal(lit))
}

fn parse_number(text: &str) -> Result<Literal, CypherError> {
    if text.contains('.') || text.contains('e') || text.contains('E') {
        let value = text
            .parse::<f64>()
            .map_err(|_| CypherError::invalid_literal("number literal", text.to_string()))?;
        Ok(Literal::Float(value))
    } else {
        let value = text
            .parse::<i64>()
            .map_err(|_| CypherError::invalid_literal("integer literal", text.to_string()))?;
        Ok(Literal::Integer(value))
    }
}
