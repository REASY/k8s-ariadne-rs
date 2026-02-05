use ariadne_cypher::{
    parse_query, validate_query, BinaryOp, Clause, Expr, Literal, Pattern, RelationshipDirection,
    ValidationMode,
};

#[test]
fn parses_match_where_return_ast() {
    let query = parse_query(
        "MATCH (p:Pod) WHERE p.status.phase IN ['Failed','Unknown'] RETURN count(p) AS total",
    )
    .unwrap();
    assert_eq!(query.clauses.len(), 2);
    match &query.clauses[0] {
        Clause::Match(m) => {
            assert!(matches!(m.pattern, Pattern::Node(_)));
            assert!(m.where_clause.is_some());
        }
        other => panic!("unexpected clause: {other:?}"),
    }
    match &query.clauses[1] {
        Clause::Return(r) => {
            assert_eq!(r.items.len(), 1);
            assert_eq!(r.items[0].alias.as_deref(), Some("total"));
        }
        other => panic!("unexpected clause: {other:?}"),
    }
}

#[test]
fn parses_relationship_pattern() {
    let query = parse_query("MATCH (a:Deployment)-[:MANAGES]->(b:ReplicaSet) RETURN b").unwrap();
    match &query.clauses[0] {
        Clause::Match(m) => match &m.pattern {
            Pattern::Relationship(rel) => {
                assert_eq!(rel.rel.direction, RelationshipDirection::LeftToRight);
                assert_eq!(rel.rel.types, vec!["MANAGES".to_string()]);
            }
            other => panic!("unexpected pattern: {other:?}"),
        },
        other => panic!("unexpected clause: {other:?}"),
    }
}

#[test]
fn parses_bracket_index_access() {
    let query =
        parse_query("MATCH (p:Pod) WHERE p['status']['phase'] = 'Failed' RETURN p").unwrap();
    match &query.clauses[0] {
        Clause::Match(m) => {
            let expr = m.where_clause.as_ref().expect("missing where");
            match expr {
                Expr::BinaryOp {
                    op: BinaryOp::Eq, ..
                } => {}
                other => panic!("unexpected expression: {other:?}"),
            }
        }
        other => panic!("unexpected clause: {other:?}"),
    }
}

#[test]
fn validates_read_only_rejects_updates() {
    let query = parse_query("CREATE (:Pod) RETURN 1").unwrap();
    let err = validate_query(&query, ValidationMode::ReadOnly).unwrap_err();
    assert!(err.to_string().contains("updating clause"));
}

#[test]
fn parses_literals() {
    let query = parse_query("RETURN 1, 2.5, true, null").unwrap();
    match &query.clauses[0] {
        Clause::Return(r) => {
            let kinds: Vec<_> = r
                .items
                .iter()
                .map(|item| match &item.expr {
                    Expr::Literal(Literal::Integer(_)) => "int",
                    Expr::Literal(Literal::Float(_)) => "float",
                    Expr::Literal(Literal::Boolean(_)) => "bool",
                    Expr::Literal(Literal::Null) => "null",
                    _ => "other",
                })
                .collect();
            assert_eq!(kinds, vec!["int", "float", "bool", "null"]);
        }
        other => panic!("unexpected clause: {other:?}"),
    }
}
