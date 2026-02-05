use ariadne_cypher::parse_query;

#[test]
fn parser_accepts_corpus() {
    let queries = build_valid_queries();
    for (idx, query) in queries.iter().enumerate() {
        if let Err(err) = parse_query(query) {
            panic!("valid query {idx} failed: {query}\n{err}");
        }
    }
}

#[test]
fn parser_rejects_corpus() {
    let queries = build_invalid_queries();
    for (idx, query) in queries.iter().enumerate() {
        if parse_query(query).is_ok() {
            panic!("invalid query {idx} unexpectedly parsed: {query}");
        }
    }
}

fn build_valid_queries() -> Vec<String> {
    let mut queries = Vec::new();

    let node_patterns = [
        "(n)",
        "(n:Pod)",
        "(n:Namespace)",
        "(n:Node)",
        "(n:ConfigMap)",
    ];
    let node_wheres = [
        "",
        "WHERE n.kind = 'Pod'",
        "WHERE n.kind <> 'Pod'",
        "WHERE n.count >= 1",
        "WHERE n.flag = true",
        "WHERE n.deleted IS NULL",
        "WHERE n.kind IN ['Pod','Node']",
        "WHERE NOT (n.flag = false)",
    ];
    let node_returns = [
        "RETURN n",
        "RETURN n.kind AS kind",
        "RETURN n.metadata_name AS name",
        "RETURN n['metadata_name'] AS name",
        "RETURN count(n) AS c",
        "RETURN count(*) AS total",
        "RETURN DISTINCT n.kind AS kind",
        "RETURN n.kind AS kind ORDER BY kind DESC SKIP 1 LIMIT 3",
    ];

    for pattern in node_patterns {
        for where_clause in node_wheres {
            for ret in node_returns {
                push_query(
                    &mut queries,
                    &[&format!("MATCH {pattern}"), where_clause, ret],
                );
            }
        }
    }

    let rel_patterns = [
        "(a)-[r:MANAGES]->(b)",
        "(a)<-[r:MANAGES]-(b)",
        "(a)-[r]->(b)",
        "(a)-[r:MANAGES]-(b)",
        "(a:Pod)-[r:RUNS_ON]->(b:Node)",
    ];
    let rel_wheres = [
        "",
        "WHERE a.kind = 'Pod'",
        "WHERE b.kind = 'Node'",
        "WHERE r IS NOT NULL",
        "WHERE a.kind = 'Pod' AND b.kind = 'Node'",
        "WHERE NOT (r.kind = 'MANAGES')",
    ];
    let rel_returns = [
        "RETURN a,b",
        "RETURN a,b,r",
        "RETURN r",
        "RETURN count(*) AS total",
        "RETURN type(r) AS rel_type",
        "RETURN a.kind AS ak ORDER BY ak",
    ];

    for pattern in rel_patterns {
        for where_clause in rel_wheres {
            for ret in rel_returns {
                push_query(
                    &mut queries,
                    &[&format!("MATCH {pattern}"), where_clause, ret],
                );
            }
        }
    }

    let extras = [
        "MATCH (n) RETURN *",
        "OPTIONAL MATCH (n) RETURN n",
        "OPTIONAL MATCH (n:Pod) WHERE n.kind = 'Pod' RETURN n",
        "UNWIND [1,2,3] AS x RETURN x",
        "UNWIND [1,2,3] AS x WITH x RETURN x",
        "UNWIND $items AS item RETURN item",
        "MATCH (n) WITH n RETURN n",
        "MATCH (n) WITH n WHERE n.kind = 'Pod' RETURN n",
        "MATCH (n) WITH n.kind AS kind RETURN kind",
        "MATCH (n) WITH n.kind AS kind RETURN count(kind) AS total",
        "MATCH (n) WITH n.kind AS kind RETURN kind ORDER BY kind",
        "MATCH (n) WITH n.kind AS kind RETURN kind SKIP 2 LIMIT 4",
        "CALL db.labels() YIELD label RETURN label",
        "CALL db.labels() YIELD label AS l RETURN l",
        "CALL db.labels($arg) YIELD label RETURN label",
        "CREATE (n) RETURN n",
        "MERGE (n) RETURN n",
        "MATCH (n) SET n.status = 'Running' RETURN n",
        "MATCH (n) REMOVE n.status RETURN n",
        "MATCH (n) DELETE n",
        "MATCH (n) DETACH DELETE n",
        "MATCH (n)\nRETURN n",
        "MATCH (n)\nWHERE n.kind = 'Pod'\nRETURN n.kind AS kind",
    ];
    for query in extras {
        queries.push(query.to_string());
    }

    queries
}

fn build_invalid_queries() -> Vec<&'static str> {
    vec![
        "MATCH (n {name:'x'}) RETURN n",
        "MATCH (n:Pod {name:'x'}) RETURN n",
        "MATCH p = (a)--(b) RETURN p",
        "MATCH (a)-[:REL*1..3]->(b) RETURN a",
        "MATCH (a)-[:REL]->(b)-[:REL]->(c) RETURN a",
        "MATCH (a)--(b), (c) RETURN a",
        "MATCH (n) WHERE n.name STARTS WITH 'k' RETURN n",
        "MATCH (n) WHERE n.name CONTAINS 'k' RETURN n",
        "MATCH (n) WHERE n.name =~ 'k.*' RETURN n",
        "MATCH (n) WHERE n:Label RETURN n",
        "MATCH (n) RETURN n + 1",
        "MATCH (n) RETURN n.age - 1",
        "MATCH (n) RETURN n.age * 2",
        "MATCH (n) RETURN [x IN [1,2,3] | x]",
        "MATCH (n) RETURN CASE WHEN n THEN 1 ELSE 0 END",
        "MATCH (n) RETURN EXISTS { MATCH (n) RETURN n }",
        "MATCH (n) RETURN n ORDER BY",
        "MATCH (n) RETURN",
        "RETURN",
        "MATCH (n",
        "MATCH n RETURN n",
        "MATCH (n) WHERE RETURN n",
        "WITH RETURN n",
        "UNWIND [1,2] RETURN x",
        "UNWIND AS x RETURN x",
        "CALL db.labels() YIELD RETURN label",
        "MATCH (n) SET n = RETURN n",
        "MATCH (n) LIMIT 5 RETURN n",
        "MATCH (n) WITH RETURN n",
        "MATCH (n) RETURN n SKIP",
        "MATCH (n) RETURN n LIMIT",
        "MATCH (n) RETURN n SKIP 1 LIMIT",
    ]
}

fn push_query(out: &mut Vec<String>, parts: &[&str]) {
    let mut query = String::new();
    for part in parts {
        if part.is_empty() {
            continue;
        }
        if !query.is_empty() {
            query.push(' ');
        }
        query.push_str(part);
    }
    out.push(query);
}
