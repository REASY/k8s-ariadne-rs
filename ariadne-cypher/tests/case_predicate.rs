use ariadne_cypher::parse_query;

#[test]
fn parses_case_expression() {
    let query = r#"
        MATCH (p:Pod)
        WITH p,
          CASE
            WHEN p['metadata']['name'] = 'foo' THEN 1
            ELSE 0
          END AS flag
        RETURN flag
    "#;
    assert!(parse_query(query).is_ok());
}

#[test]
fn parses_string_predicates() {
    let query = r#"
        MATCH (p:Pod)
        WHERE p['metadata']['name'] ENDS WITH 'server'
        RETURN p
    "#;
    assert!(parse_query(query).is_ok());
}

#[test]
fn parses_label_predicate() {
    let query = r#"
        MATCH (n)
        WHERE n:Pod OR n:Service
        RETURN n
    "#;
    assert!(parse_query(query).is_ok());
}
