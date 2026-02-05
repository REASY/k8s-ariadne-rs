use ariadne_cypher::{parse_cypher, ParseError};

#[test]
fn parses_simple_match_return() {
    let tree = parse_cypher("MATCH (n) RETURN n").unwrap();
    assert!(!tree.root_node().has_error());
}

#[test]
fn parses_where_in_list() {
    let tree =
        parse_cypher("MATCH (p:Pod) WHERE p.status.phase IN ['Failed', 'Unknown'] RETURN count(p)")
            .unwrap();
    assert!(!tree.root_node().has_error());
}

#[test]
fn parses_with_unwind_labels() {
    let tree = parse_cypher(
        "MATCH (n) WITH labels(n) AS lbls, n UNWIND lbls AS label RETURN label, count(*)",
    )
    .unwrap();
    assert!(!tree.root_node().has_error());
}

#[test]
fn parses_create_and_merge() {
    let tree = parse_cypher(
        "CREATE (a:Person {name: 'Alice'}) MERGE (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();
    assert!(!tree.root_node().has_error());
}

#[test]
fn parses_optional_match_and_order() {
    let tree = parse_cypher(
        "OPTIONAL MATCH (n:Pod) RETURN n.metadata.name AS name ORDER BY name DESC SKIP 5 LIMIT 10",
    )
    .unwrap();
    assert!(!tree.root_node().has_error());
}

#[test]
fn parses_list_comprehension() {
    let tree = parse_cypher("RETURN [x IN [1,2,3] WHERE x > 1 | x * 2] AS xs").unwrap();
    assert!(!tree.root_node().has_error());
}

#[test]
fn parses_pattern_comprehension() {
    let tree = parse_cypher("MATCH (n) RETURN [(n)-[:KNOWS]->(m) | m.name] AS neighbors").unwrap();
    assert!(!tree.root_node().has_error());
}

#[test]
fn parses_exists_subquery() {
    let tree = parse_cypher("MATCH (n) WHERE exists { (n)-->(m) } RETURN n").unwrap();
    assert!(!tree.root_node().has_error());
}

#[test]
fn parses_call_with_yield() {
    let tree = parse_cypher("CALL db.labels() YIELD label RETURN count(label) AS total").unwrap();
    assert!(!tree.root_node().has_error());
}

#[test]
fn parses_backtick_identifier() {
    let tree = parse_cypher("MATCH (n) RETURN n.`app.kubernetes.io/name` AS name").unwrap();
    assert!(!tree.root_node().has_error());
}

#[test]
fn rejects_invalid_query() {
    let err = parse_cypher("MATCH (n RETURN n").unwrap_err();
    assert!(matches!(err, ParseError::Syntax));
}
