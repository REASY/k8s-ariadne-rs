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

#[test]
fn parses_exists_subquery() {
    let query = r#"
        MATCH (n)
        WHERE exists { (n)-->(m) }
        RETURN n
    "#;
    assert!(parse_query(query).is_ok());
}

#[test]
fn parses_quantifier_and_list_comprehension() {
    let query = r#"
        MATCH (p:Pod)
        WHERE ANY(cs IN p['status']['containerStatuses'] WHERE cs['lastState']['terminated']['reason'] = 'OOMKilled')
        RETURN
          p['metadata']['namespace'] AS namespace,
          p['metadata']['name'] AS pod,
          [cs IN p['status']['containerStatuses'] WHERE cs['lastState']['terminated']['reason'] = 'OOMKilled' | {
            container: cs['name'],
            exitCode: cs['lastState']['terminated']['exitCode'],
            finishedAt: cs['lastState']['terminated']['finishedAt'],
            message: cs['lastState']['terminated']['message']
          }] AS oom_killed_containers
        ORDER BY namespace, pod
    "#;
    assert!(parse_query(query).is_ok());
}
