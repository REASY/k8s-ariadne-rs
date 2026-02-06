use crate::ast::*;
use crate::CypherError;
use tree_sitter::{Node, Tree};

pub fn parse_query(input: &str) -> Result<Query, CypherError> {
    let tree = crate::parse_cypher(input)?;
    build_query(input, &tree)
}

fn build_query(input: &str, tree: &Tree) -> Result<Query, CypherError> {
    let root = tree.root_node();
    let mut clause_nodes = Vec::new();
    collect_clauses(root, false, &mut clause_nodes);
    clause_nodes.sort_by_key(|node| node.start_byte());

    let mut clauses = Vec::new();
    for node in clause_nodes {
        clauses.push(parse_clause(node, input)?);
    }

    Ok(Query { clauses })
}

fn collect_clauses<'a>(node: Node<'a>, in_expr: bool, out: &mut Vec<Node<'a>>) {
    let kind = node.kind();
    let is_expr = in_expr || is_expression_kind(kind);

    if !is_expr && is_clause_kind(kind) {
        out.push(node);
        return;
    }

    for child in named_children(node) {
        collect_clauses(child, is_expr, out);
    }
}

fn is_clause_kind(kind: &str) -> bool {
    matches!(
        kind,
        "match"
            | "unwind"
            | "with"
            | "return"
            | "in_query_call"
            | "standalone_call"
            | "create"
            | "merge"
            | "delete"
            | "set"
            | "remove"
    )
}

fn is_expression_kind(kind: &str) -> bool {
    kind.ends_with("_expression")
        || matches!(
            kind,
            "expression"
                | "list_comprehension"
                | "pattern_comprehension"
                | "quantifier"
                | "existential_subquery"
                | "case_expression"
                | "function_invocation"
        )
}

fn parse_clause(node: Node, input: &str) -> Result<Clause, CypherError> {
    match node.kind() {
        "match" => Ok(Clause::Match(parse_match(node, input)?)),
        "unwind" => Ok(Clause::Unwind(parse_unwind(node, input)?)),
        "with" => Ok(Clause::With(parse_with(node, input)?)),
        "return" => Ok(Clause::Return(parse_return(node, input)?)),
        "in_query_call" | "standalone_call" => Ok(Clause::Call(parse_call(node, input)?)),
        "create" | "merge" | "delete" | "set" | "remove" => {
            Ok(Clause::Updating(parse_updating_clause(node, input)?))
        }
        other => Err(CypherError::unsupported(other, Span::from_node(node))),
    }
}

fn parse_match(node: Node, input: &str) -> Result<MatchClause, CypherError> {
    let pattern = named_children(node)
        .into_iter()
        .find(|child| child.kind() == "pattern")
        .ok_or_else(|| CypherError::missing("match pattern", Span::from_node(node)))?;
    let where_clause = named_children(node)
        .into_iter()
        .find(|child| child.kind() == "where")
        .map(|where_node| parse_where(where_node, input))
        .transpose()?;

    let text = node_text(node, input)?;
    let optional = text
        .trim_start()
        .to_ascii_lowercase()
        .starts_with("optional");

    Ok(MatchClause {
        optional,
        pattern: parse_pattern(pattern, input)?,
        where_clause,
        span: Span::from_node(node),
    })
}

fn parse_unwind(node: Node, input: &str) -> Result<UnwindClause, CypherError> {
    let mut named = named_children(node).into_iter();
    let expr_node = named
        .find(|child| child.kind() == "expression")
        .ok_or_else(|| CypherError::missing("unwind expression", Span::from_node(node)))?;
    let var_node = named_children(node)
        .into_iter()
        .find(|child| child.kind() == "variable")
        .ok_or_else(|| CypherError::missing("unwind variable", Span::from_node(node)))?;

    Ok(UnwindClause {
        expression: parse_expression(expr_node, input)?,
        variable: parse_identifier(var_node, input)?,
        span: Span::from_node(node),
    })
}

fn parse_with(node: Node, input: &str) -> Result<WithClause, CypherError> {
    let projection = named_children(node)
        .into_iter()
        .find(|child| child.kind() == "projection_body")
        .ok_or_else(|| CypherError::missing("with projection", Span::from_node(node)))?;
    let where_clause = named_children(node)
        .into_iter()
        .find(|child| child.kind() == "where")
        .map(|where_node| parse_where(where_node, input))
        .transpose()?;

    let projection_parts = parse_projection_body(projection, input)?;

    Ok(WithClause {
        distinct: projection_parts.distinct,
        items: projection_parts.items,
        order: projection_parts.order,
        skip: projection_parts.skip,
        limit: projection_parts.limit,
        where_clause,
        span: Span::from_node(node),
    })
}

fn parse_return(node: Node, input: &str) -> Result<ReturnClause, CypherError> {
    let projection = named_children(node)
        .into_iter()
        .find(|child| child.kind() == "projection_body")
        .ok_or_else(|| CypherError::missing("return projection", Span::from_node(node)))?;
    let projection_parts = parse_projection_body(projection, input)?;

    Ok(ReturnClause {
        distinct: projection_parts.distinct,
        items: projection_parts.items,
        order: projection_parts.order,
        skip: projection_parts.skip,
        limit: projection_parts.limit,
        span: Span::from_node(node),
    })
}

fn parse_call(node: Node, input: &str) -> Result<CallClause, CypherError> {
    let invocation = named_children(node)
        .into_iter()
        .find(|child| {
            matches!(
                child.kind(),
                "explicit_procedure_invocation" | "implicit_procedure_invocation"
            )
        })
        .ok_or_else(|| CypherError::missing("call invocation", Span::from_node(node)))?;

    let (name, args) = parse_procedure_invocation(invocation, input)?;
    let yields = named_children(node)
        .into_iter()
        .find(|child| child.kind() == "yield_items")
        .map(|yield_node| parse_yield_items(yield_node, input))
        .transpose()?;

    Ok(CallClause {
        name,
        args,
        yields,
        span: Span::from_node(node),
    })
}

fn parse_updating_clause(node: Node, input: &str) -> Result<UpdatingClause, CypherError> {
    let kind = match node.kind() {
        "create" => UpdatingClauseKind::Create,
        "merge" => UpdatingClauseKind::Merge,
        "delete" => UpdatingClauseKind::Delete,
        "set" => UpdatingClauseKind::Set,
        "remove" => UpdatingClauseKind::Remove,
        other => {
            return Err(CypherError::unsupported(other, Span::from_node(node)));
        }
    };
    Ok(UpdatingClause {
        kind,
        span: Span::from_node(node),
        text: node_text(node, input)?.to_string(),
    })
}

struct ProjectionParts {
    distinct: bool,
    items: Vec<ProjectionItem>,
    order: Option<OrderBy>,
    skip: Option<Expr>,
    limit: Option<Expr>,
}

fn parse_projection_body(node: Node, input: &str) -> Result<ProjectionParts, CypherError> {
    let mut items = Vec::new();
    let mut order = None;
    let mut skip = None;
    let mut limit = None;

    let projection_items = named_children(node)
        .into_iter()
        .find(|child| child.kind() == "projection_items")
        .ok_or_else(|| CypherError::missing("projection items", Span::from_node(node)))?;

    let items_text = node_text(projection_items, input)?.trim();
    if items_text.starts_with('*') {
        items.push(ProjectionItem {
            expr: Expr::Star,
            alias: None,
        });
    }

    for child in named_children(projection_items) {
        if child.kind() == "projection_item" {
            items.push(parse_projection_item(child, input)?);
        }
    }

    for child in named_children(node) {
        match child.kind() {
            "order" => order = Some(parse_order(child, input)?),
            "skip" => {
                let expr = named_children(child)
                    .into_iter()
                    .find(|c| c.kind() == "expression")
                    .ok_or_else(|| {
                        CypherError::missing("skip expression", Span::from_node(child))
                    })?;
                skip = Some(parse_expression(expr, input)?);
            }
            "limit" => {
                let expr = named_children(child)
                    .into_iter()
                    .find(|c| c.kind() == "expression")
                    .ok_or_else(|| {
                        CypherError::missing("limit expression", Span::from_node(child))
                    })?;
                limit = Some(parse_expression(expr, input)?);
            }
            _ => {}
        }
    }

    let text = node_text(node, input)?.to_ascii_lowercase();
    let distinct = text.trim_start().starts_with("distinct");

    Ok(ProjectionParts {
        distinct,
        items,
        order,
        skip,
        limit,
    })
}

fn parse_projection_item(node: Node, input: &str) -> Result<ProjectionItem, CypherError> {
    let mut named = named_children(node).into_iter();
    let expr_node = named
        .next()
        .ok_or_else(|| CypherError::missing("projection expression", Span::from_node(node)))?;
    let expr = parse_expression(expr_node, input)?;

    let alias = named_children(node)
        .into_iter()
        .find(|child| child.kind() == "variable")
        .map(|var| parse_identifier(var, input))
        .transpose()?;

    Ok(ProjectionItem { expr, alias })
}

fn parse_order(node: Node, input: &str) -> Result<OrderBy, CypherError> {
    let mut items = Vec::new();
    for child in named_children(node) {
        if child.kind() == "sort_item" {
            items.push(parse_sort_item(child, input)?);
        }
    }
    Ok(OrderBy { items })
}

fn parse_sort_item(node: Node, input: &str) -> Result<OrderItem, CypherError> {
    let expr_node = named_children(node)
        .into_iter()
        .find(|child| child.kind() == "expression")
        .ok_or_else(|| CypherError::missing("sort expression", Span::from_node(node)))?;
    let expr = parse_expression(expr_node, input)?;
    let text = node_text(node, input)?.to_ascii_lowercase();
    let direction = if text.contains(" desc") || text.ends_with(" desc") {
        SortDirection::Desc
    } else {
        SortDirection::Asc
    };
    Ok(OrderItem { expr, direction })
}

fn parse_yield_items(node: Node, input: &str) -> Result<Vec<YieldItem>, CypherError> {
    let mut items = Vec::new();
    for child in named_children(node) {
        if child.kind() == "yield_item" {
            items.push(parse_yield_item(child, input)?);
        }
    }
    Ok(items)
}

fn parse_yield_item(node: Node, input: &str) -> Result<YieldItem, CypherError> {
    let mut named = named_children(node).into_iter();
    let first = named
        .next()
        .ok_or_else(|| CypherError::missing("yield item", Span::from_node(node)))?;
    let second = named.next();
    let (name, alias) = match (first.kind(), second) {
        ("procedure_result_field", Some(second)) => (
            parse_identifier(first, input)?,
            Some(parse_identifier(second, input)?),
        ),
        _ => (parse_identifier(first, input)?, None),
    };
    Ok(YieldItem { name, alias })
}

fn parse_procedure_invocation(node: Node, input: &str) -> Result<(String, Vec<Expr>), CypherError> {
    let name_node = named_children(node)
        .into_iter()
        .find(|child| child.kind() == "procedure_name")
        .ok_or_else(|| CypherError::missing("procedure name", Span::from_node(node)))?;
    let name = node_text(name_node, input)?.trim().to_string();
    let mut args = Vec::new();
    for child in named_children(node) {
        if child.kind() == "expression" {
            args.push(parse_expression(child, input)?);
        }
    }
    Ok((name, args))
}

fn parse_where(node: Node, input: &str) -> Result<Expr, CypherError> {
    let expr_node = named_children(node)
        .into_iter()
        .find(|child| child.kind() == "expression")
        .ok_or_else(|| CypherError::missing("where expression", Span::from_node(node)))?;
    parse_expression(expr_node, input)
}

fn parse_pattern(node: Node, input: &str) -> Result<Pattern, CypherError> {
    let mut parts = named_children(node)
        .into_iter()
        .filter(|child| child.kind() == "pattern_part");
    let first = parts
        .next()
        .ok_or_else(|| CypherError::missing("pattern part", Span::from_node(node)))?;
    if parts.next().is_some() {
        return Err(CypherError::unsupported(
            "multiple pattern parts",
            Span::from_node(node),
        ));
    }
    parse_pattern_part(first, input)
}

fn parse_pattern_part(node: Node, input: &str) -> Result<Pattern, CypherError> {
    let mut named = named_children(node).into_iter();
    let first = named
        .next()
        .ok_or_else(|| CypherError::missing("pattern element", Span::from_node(node)))?;

    if first.kind() == "variable" {
        return Err(CypherError::unsupported(
            "named pattern assignment",
            Span::from_node(node),
        ));
    }

    parse_pattern_element(first, input)
}

fn parse_pattern_element(node: Node, input: &str) -> Result<Pattern, CypherError> {
    let element = if node.kind() == "pattern_element" {
        node
    } else {
        named_children(node)
            .into_iter()
            .find(|child| child.kind() == "pattern_element")
            .unwrap_or(node)
    };

    let node_pattern = named_children(element)
        .into_iter()
        .find(|child| child.kind() == "node_pattern")
        .ok_or_else(|| CypherError::missing("node pattern", Span::from_node(element)))?;
    let base = parse_node_pattern(node_pattern, input)?;

    let chains: Vec<Node> = named_children(element)
        .into_iter()
        .filter(|child| child.kind() == "pattern_element_chain")
        .collect();

    if chains.is_empty() {
        return Ok(Pattern::Node(base));
    }

    let mut segments = Vec::with_capacity(chains.len());
    for chain in chains {
        let rel = named_children(chain)
            .into_iter()
            .find(|child| child.kind() == "relationship_pattern")
            .ok_or_else(|| CypherError::missing("relationship pattern", Span::from_node(chain)))?;
        let right = named_children(chain)
            .into_iter()
            .find(|child| child.kind() == "node_pattern")
            .ok_or_else(|| CypherError::missing("node pattern", Span::from_node(chain)))?;

        let rel_detail = parse_relationship_pattern(rel, input)?;
        let right_node = parse_node_pattern(right, input)?;

        segments.push(PathSegment {
            rel: rel_detail,
            node: right_node,
            span: Span::from_node(chain),
        });
    }

    if segments.len() == 1 {
        let segment = segments.pop().expect("segment exists");
        return Ok(Pattern::Relationship(RelationshipPattern {
            left: base,
            rel: segment.rel,
            right: segment.node,
            span: Span::from_node(element),
        }));
    }

    Ok(Pattern::Path(PathPattern {
        start: base,
        segments,
        span: Span::from_node(element),
    }))
}

fn parse_node_pattern(node: Node, input: &str) -> Result<NodePattern, CypherError> {
    let mut variable = None;
    let mut labels = Vec::new();
    for child in named_children(node) {
        match child.kind() {
            "variable" => variable = Some(parse_identifier(child, input)?),
            "node_labels" => {
                for label in named_children(child) {
                    if label.kind() == "node_label" {
                        labels.push(parse_label(label, input)?);
                    }
                }
            }
            "properties" => {
                return Err(CypherError::unsupported(
                    "node properties in patterns",
                    Span::from_node(child),
                ));
            }
            _ => {}
        }
    }

    Ok(NodePattern {
        variable,
        labels,
        span: Span::from_node(node),
    })
}

fn parse_relationship_pattern(node: Node, input: &str) -> Result<RelationshipDetail, CypherError> {
    let mut variable = None;
    let mut types = Vec::new();

    for child in named_children(node) {
        if child.kind() == "relationship_detail" {
            for detail in named_children(child) {
                match detail.kind() {
                    "variable" => variable = Some(parse_identifier(detail, input)?),
                    "relationship_types" => {
                        for rel in named_children(detail) {
                            if rel.kind() == "rel_type_name" {
                                types.push(parse_identifier(rel, input)?);
                            }
                        }
                    }
                    "range_literal" | "properties" => {
                        return Err(CypherError::unsupported(
                            "relationship ranges/properties",
                            Span::from_node(detail),
                        ));
                    }
                    _ => {}
                }
            }
        }
    }

    let text = node_text(node, input)?.trim();
    let direction = if text.starts_with('<') {
        RelationshipDirection::RightToLeft
    } else if text.ends_with('>') {
        RelationshipDirection::LeftToRight
    } else {
        RelationshipDirection::Undirected
    };

    Ok(RelationshipDetail {
        variable,
        types,
        direction,
    })
}

fn parse_label(node: Node, input: &str) -> Result<String, CypherError> {
    let name = named_children(node)
        .into_iter()
        .find(|child| child.kind() == "label_name")
        .ok_or_else(|| CypherError::missing("label name", Span::from_node(node)))?;
    parse_identifier(name, input)
}

fn parse_expression(node: Node, input: &str) -> Result<Expr, CypherError> {
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
    let keyword = node_text(node, input)?
        .trim_start()
        .to_ascii_lowercase();
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
    let mut named = named_children(node).into_iter();
    let base = named
        .next()
        .ok_or_else(|| CypherError::missing("index base", Span::from_node(node)))?;
    let index = named
        .next()
        .ok_or_else(|| CypherError::missing("index expression", Span::from_node(node)))?;
    Ok(Expr::IndexAccess {
        expr: Box::new(parse_expression(base, input)?),
        index: Box::new(parse_expression(index, input)?),
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

fn parse_parameter(node: Node, input: &str) -> Result<String, CypherError> {
    let text = node_text(node, input)?;
    Ok(text.trim().trim_start_matches('$').to_string())
}

fn parse_identifier(node: Node, input: &str) -> Result<String, CypherError> {
    let text = node_text(node, input)?.trim().to_string();
    Ok(normalize_identifier(&text))
}

fn normalize_identifier(text: &str) -> String {
    let trimmed = text.trim();
    if trimmed.starts_with('`') && trimmed.ends_with('`') && trimmed.len() >= 2 {
        trimmed[1..trimmed.len() - 1].to_string()
    } else {
        trimmed.to_string()
    }
}

fn unescape_string(text: &str) -> String {
    let trimmed = text.trim();
    let unquoted = if trimmed.len() >= 2
        && ((trimmed.starts_with('\'') && trimmed.ends_with('\''))
            || (trimmed.starts_with('"') && trimmed.ends_with('"')))
    {
        &trimmed[1..trimmed.len() - 1]
    } else {
        trimmed
    };
    let mut out = String::new();
    let mut chars = unquoted.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            match chars.next() {
                Some('n') => out.push('\n'),
                Some('t') => out.push('\t'),
                Some('r') => out.push('\r'),
                Some('"') => out.push('"'),
                Some('\'') => out.push('\''),
                Some('\\') => out.push('\\'),
                Some(other) => out.push(other),
                None => {}
            }
        } else {
            out.push(ch);
        }
    }
    out
}

fn node_text<'a>(node: Node, input: &'a str) -> Result<&'a str, CypherError> {
    node.utf8_text(input.as_bytes())
        .map_err(|_| CypherError::invalid_text(Span::from_node(node)))
}

fn named_children<'a>(node: Node<'a>) -> Vec<Node<'a>> {
    let mut cursor = node.walk();
    node.named_children(&mut cursor).collect()
}

impl Span {
    fn from_node(node: Node) -> Self {
        let range = node.range();
        Span {
            start_byte: range.start_byte,
            end_byte: range.end_byte,
            start_row: range.start_point.row,
            start_col: range.start_point.column,
            end_row: range.end_point.row,
            end_col: range.end_point.column,
        }
    }
}
