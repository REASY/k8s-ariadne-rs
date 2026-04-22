use ariadne_cypher::{Query, ValidationMode, parse_query, validate_query};

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

fn expect_err_contains(cypher: &str, mode: ValidationMode, substring: &str) {
    let query = parse_query(cypher).unwrap();
    let err = validate_query(&query, mode).expect_err(&format!(
        "expected error containing '{substring}' for: {cypher}"
    ));
    let msg = err.to_string();
    assert!(
        msg.contains(substring),
        "error message '{msg}' does not contain '{substring}'"
    );
}

fn expect_ok(cypher: &str, mode: ValidationMode) {
    let query = parse_query(cypher).unwrap();
    validate_query(&query, mode).unwrap_or_else(|e| panic!("expected Ok for '{cypher}', got: {e}"));
}

// ===========================================================================
// ReadOnly-mode rejection tests
// ===========================================================================

#[test]
fn readonly_rejects_empty_query() {
    // parse_query cannot produce an empty Query, so construct one directly.
    let query = Query { clauses: vec![] };
    let err = validate_query(&query, ValidationMode::ReadOnly).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("query contains no clauses"),
        "unexpected error: {msg}"
    );
}

#[test]
fn readonly_rejects_query_not_ending_with_return() {
    // The tree-sitter parser rejects bare "MATCH (n)" or "MATCH (n) WITH n",
    // so build a query from a valid parse and then strip the trailing RETURN.
    let mut query = parse_query("MATCH (n) RETURN n").unwrap();
    // Remove the RETURN clause so the query ends with MATCH.
    query.clauses.pop();
    let err = validate_query(&query, ValidationMode::ReadOnly).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("query must end with RETURN"),
        "unexpected error: {msg}"
    );
}

#[test]
fn readonly_rejects_create() {
    expect_err_contains(
        "CREATE (a:Person {name: 'Alice'}) RETURN a",
        ValidationMode::ReadOnly,
        "updating clause not supported",
    );
}

#[test]
fn readonly_rejects_set() {
    expect_err_contains(
        "MATCH (n:Pod) SET n.foo = 1 RETURN n",
        ValidationMode::ReadOnly,
        "updating clause not supported",
    );
}

#[test]
fn readonly_rejects_delete() {
    expect_err_contains(
        "MATCH (n:Pod) DELETE n RETURN n",
        ValidationMode::ReadOnly,
        "updating clause not supported",
    );
}

#[test]
fn readonly_rejects_merge() {
    expect_err_contains(
        "MERGE (a:Person {name: 'Alice'}) RETURN a",
        ValidationMode::ReadOnly,
        "updating clause not supported",
    );
}

#[test]
fn readonly_rejects_remove() {
    expect_err_contains(
        "MATCH (n:Pod) REMOVE n.foo RETURN n",
        ValidationMode::ReadOnly,
        "updating clause not supported",
    );
}

#[test]
fn readonly_rejects_call() {
    expect_err_contains(
        "CALL db.labels() YIELD label RETURN label",
        ValidationMode::ReadOnly,
        "CALL clauses are not supported",
    );
}

#[test]
fn readonly_rejects_relationship_type_union() {
    expect_err_contains(
        "MATCH (a)-[:KNOWS|LIKES]->(b) RETURN b",
        ValidationMode::ReadOnly,
        "relationship type unions not supported",
    );
}

#[test]
fn readonly_rejects_inline_property_map_in_exists_where_pattern() {
    expect_err_contains(
        "MATCH (n:Pod) WHERE exists { (n)-[:BelongsTo]->(ns:Namespace {name: 'litmus'}) } RETURN n",
        ValidationMode::ReadOnly,
        "inline property maps in MATCH are not supported",
    );
}

#[test]
fn readonly_rejects_inline_property_map_in_exists_return_pattern() {
    expect_err_contains(
        "MATCH (n:Pod) RETURN exists { (ns:Namespace {name: 'litmus'})<-[:BelongsTo]-(n) } AS in_litmus",
        ValidationMode::ReadOnly,
        "inline property maps in MATCH are not supported",
    );
}

// ===========================================================================
// Engine-mode rejection tests (includes all ReadOnly checks)
// ===========================================================================

#[test]
fn engine_rejects_multiple_labels_on_node() {
    expect_err_contains(
        "MATCH (n:Pod:Service) RETURN n",
        ValidationMode::Engine,
        "multiple labels are not supported by the in-memory engine",
    );
}

#[test]
fn engine_rejects_multiple_labels_in_relationship_pattern() {
    expect_err_contains(
        "MATCH (a:Pod:Service)-[:MANAGES]->(b) RETURN b",
        ValidationMode::Engine,
        "multiple labels are not supported by the in-memory engine",
    );
}

#[test]
fn engine_rejects_multiple_labels_on_right_side_of_rel() {
    expect_err_contains(
        "MATCH (a)-[:MANAGES]->(b:Pod:Service) RETURN b",
        ValidationMode::Engine,
        "multiple labels are not supported by the in-memory engine",
    );
}

#[test]
fn engine_rejects_multiple_labels_in_path_pattern() {
    expect_err_contains(
        "MATCH (a:Pod:Service)-[:MANAGES]->(b)-[:OWNS]->(c) RETURN c",
        ValidationMode::Engine,
        "multiple labels are not supported by the in-memory engine",
    );
}

#[test]
fn engine_rejects_aggregate_outside_projection() {
    expect_err_contains(
        "MATCH (n) WHERE count(n) > 1 RETURN n",
        ValidationMode::Engine,
        "aggregate functions must appear in projection",
    );
}

#[test]
fn engine_rejects_count_star_outside_projection() {
    expect_err_contains(
        "MATCH (n) WHERE count(*) > 0 RETURN n",
        ValidationMode::Engine,
        "count(*) must appear in projection",
    );
}

#[test]
fn engine_rejects_return_star_combined_with_aggregation() {
    expect_err_contains(
        "MATCH (n) RETURN *, count(n)",
        ValidationMode::Engine,
        "RETURN/WITH * cannot be combined with aggregation",
    );
}

#[test]
fn engine_rejects_updating_clause() {
    expect_err_contains(
        "CREATE (a:Person {name: 'Alice'}) RETURN a",
        ValidationMode::Engine,
        "updating clause not supported",
    );
}

#[test]
fn engine_rejects_call_clause() {
    expect_err_contains(
        "CALL db.labels() YIELD label RETURN label",
        ValidationMode::Engine,
        "CALL clauses are not supported",
    );
}

#[test]
fn engine_rejects_inline_property_map_in_nested_exists_pattern() {
    expect_err_contains(
        "MATCH (p:Pod) WHERE exists { (p)-[:BelongsTo]->(ns:Namespace) WHERE exists { (ns:Namespace {name: 'litmus'}) } } RETURN p",
        ValidationMode::Engine,
        "inline property maps in MATCH are not supported",
    );
}

// ===========================================================================
// Valid queries -- ReadOnly mode
// ===========================================================================

#[test]
fn readonly_accepts_simple_match_return() {
    expect_ok("MATCH (n) RETURN n", ValidationMode::ReadOnly);
}

#[test]
fn readonly_accepts_match_where_order_limit() {
    expect_ok(
        "MATCH (p:Pod) WHERE p.status.phase = 'Running' RETURN p ORDER BY p.metadata.name LIMIT 10",
        ValidationMode::ReadOnly,
    );
}

#[test]
fn readonly_accepts_optional_match() {
    expect_ok(
        "OPTIONAL MATCH (n:Pod) RETURN n.metadata.name AS name",
        ValidationMode::ReadOnly,
    );
}

#[test]
fn readonly_accepts_unwind() {
    expect_ok("UNWIND [1, 2, 3] AS x RETURN x", ValidationMode::ReadOnly);
}

#[test]
fn readonly_accepts_with_clause() {
    expect_ok("MATCH (n) WITH n RETURN n", ValidationMode::ReadOnly);
}

// ===========================================================================
// Valid queries -- Engine mode
// ===========================================================================

#[test]
fn engine_accepts_case_expression() {
    expect_ok(
        r#"MATCH (p:Pod) RETURN CASE WHEN p.status = 'Running' THEN 'ok' ELSE 'bad' END AS health"#,
        ValidationMode::Engine,
    );
}

#[test]
fn engine_accepts_exists_expression() {
    expect_ok(
        "MATCH (n:Pod) WHERE exists { (n)-->(m) } RETURN n",
        ValidationMode::Engine,
    );
}

#[test]
fn engine_accepts_list_comprehension() {
    expect_ok(
        "MATCH (n) RETURN [x IN [1,2,3] WHERE x > 1 | x * 2] AS doubled",
        ValidationMode::Engine,
    );
}

#[test]
fn engine_accepts_count_in_projection() {
    expect_ok(
        "MATCH (n:Pod) RETURN count(n) AS total",
        ValidationMode::Engine,
    );
}

#[test]
fn engine_accepts_sum_in_projection() {
    expect_ok(
        "MATCH (n:Pod) RETURN sum(n.cpu) AS total_cpu",
        ValidationMode::Engine,
    );
}

#[test]
fn engine_accepts_count_star_in_projection() {
    expect_ok(
        "MATCH (n:Pod) RETURN count(*) AS total",
        ValidationMode::Engine,
    );
}

#[test]
fn engine_accepts_simple_match_return() {
    expect_ok("MATCH (n) RETURN n", ValidationMode::Engine);
}

#[test]
fn engine_accepts_with_and_return() {
    expect_ok(
        "MATCH (n:Pod) WITH n RETURN n.metadata.name AS name",
        ValidationMode::Engine,
    );
}
