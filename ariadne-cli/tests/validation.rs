use ariadne_cli::validation::validate_cypher;

#[test]
fn rejects_updating_clause() {
    let err = validate_cypher("CREATE (:Pod) RETURN 1").unwrap_err();
    assert!(err.to_string().contains("updating"));
}

#[test]
fn rejects_call_clause() {
    let err = validate_cypher("CALL db.labels() YIELD label RETURN label").unwrap_err();
    assert!(err.to_string().contains("CALL"));
}

#[test]
fn accepts_with_unwind() {
    let res = validate_cypher("UNWIND [1,2,3] AS x WITH x RETURN x");
    assert!(res.is_ok());
}
