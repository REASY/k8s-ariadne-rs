import pytest

from k8s_graph_agent.cypher_validator import (
    CypherCompatibilityError,
    CypherValidationError,
    CypherSchemaValidator,
    SchemaValidationError,
)
from k8s_graph_agent.graph_schema import GraphSchema


@pytest.fixture()
def validator() -> CypherSchemaValidator:
    edges = [
        ("Host", "IsClaimedBy", "Ingress"),
        ("Ingress", "DefinesBackend", "IngressServiceBackend"),
        ("IngressServiceBackend", "TargetsService", "Service"),
        ("Service", "Manages", "EndpointSlice"),
        ("EndpointSlice", "ContainsEndpoint", "Endpoint"),
        ("Endpoint", "HasAddress", "EndpointAddress"),
        ("EndpointAddress", "IsAddressOf", "Pod"),
        ("EndpointAddress", "ListedIn", "EndpointSlice"),
        ("Pod", "BelongsTo", "Namespace"),
        ("Deployment", "Manages", "ReplicaSet"),
        ("ReplicaSet", "Manages", "Pod"),
        ("StatefulSet", "Manages", "Pod"),
        ("DaemonSet", "Manages", "Pod"),
        ("Job", "Manages", "Pod"),
    ]
    schema = GraphSchema.from_edges(edges)
    return CypherSchemaValidator(schema)


def test_valid_path(validator: CypherSchemaValidator) -> None:
    cypher = (
        "MATCH (h:Host)-[:IsClaimedBy]->(i:Ingress)"
        "-[:DefinesBackend]->(b:IngressServiceBackend)"
        "-[:TargetsService]->(s:Service)"
        "-[:Manages]->(es:EndpointSlice)"
        "-[:ContainsEndpoint]->(e:Endpoint)"
        "-[:HasAddress]->(ea:EndpointAddress)"
        "-[:IsAddressOf]->(p:Pod) "
        "RETURN p"
    )
    validator.validate(cypher)


def test_accepts_multiple_with_clauses(validator: CypherSchemaValidator) -> None:
    cypher = "MATCH (h:Host)-[:IsClaimedBy]->(i:Ingress) WITH h, i WITH h RETURN h"
    validator.validate(cypher)


def test_invalid_edge_direction_and_node(
    validator: CypherSchemaValidator,
) -> None:
    cypher = (
        "MATCH (h:Host)-[:IsClaimedBy]->(i:Ingress)"
        "-[:DefinesBackend]->(b:IngressServiceBackend)"
        "-[:TargetsService]->(s:Service)"
        "-[:Manages]->(es:EndpointSlice)"
        "-[:ContainsEndpoint]->(e:Endpoint)"
        "<-[:ListedIn]-(ea:EndpointAddress)"
        "-[:IsAddressOf]->(p:Pod) "
        "RETURN p"
    )
    with pytest.raises(SchemaValidationError) as context:
        validator.validate(cypher)
    message = str(context.value)
    assert "ListedIn" in message
    assert "Endpoint" in message
    assert "EndpointAddress" in message
    assert "[rule=" in message
    assert "Hint:" in message


def test_rejects_wrong_direction_from_log_example(
    validator: CypherSchemaValidator,
) -> None:
    cypher = (
        "MATCH (h:Host)-[:IsClaimedBy]->(i:Ingress)\n"
        "WHERE h.name = 'litmus.qa.agoda.is'\n"
        "MATCH (i)-[:DefinesBackend]->(b:IngressServiceBackend)-[:TargetsService]->(s:Service)\n"
        "MATCH (s)-[:Manages]->(es:EndpointSlice)-[:ContainsEndpoint]->(e:Endpoint)\n"
        "MATCH (e)<-[:HasAddress]-(ea:EndpointAddress)-[:IsAddressOf]->(p:Pod)\n"
        "RETURN DISTINCT\n"
        "  p['metadata']['namespace'] AS namespace,\n"
        "  p['metadata']['name'] AS pod,\n"
        "  p['status']['podIP'] AS pod_ip,\n"
        "  s['metadata']['name'] AS service,\n"
        "  i['metadata']['name'] AS ingress\n"
        "ORDER BY namespace, pod"
    )
    with pytest.raises(SchemaValidationError) as context:
        validator.validate(cypher)
    message = str(context.value)
    assert "HasAddress" in message
    assert "Endpoint" in message
    assert "EndpointAddress" in message
    assert "[rule=" in message
    assert "Hint:" in message


def test_accepts_valid_query_from_log_example(
    validator: CypherSchemaValidator,
) -> None:
    cypher = (
        "MATCH (h:Host)-[:IsClaimedBy]->(i:Ingress)-[:DefinesBackend]->(b:IngressServiceBackend)"
        "-[:TargetsService]->(s:Service)-[:Manages]->(es:EndpointSlice)-[:ContainsEndpoint]->(e:Endpoint)"
        "-[:HasAddress]->(ea:EndpointAddress)-[:IsAddressOf]->(p:Pod)\n"
        "WHERE h.name = 'litmus.qa.agoda.is'\n"
        "RETURN DISTINCT\n"
        "  p['metadata']['namespace'] AS namespace,\n"
        "  p['metadata']['name'] AS pod,\n"
        "  p['status']['podIP'] AS podIP,\n"
        "  p['status']['phase'] AS phase\n"
        "ORDER BY namespace, pod;"
    )
    validator.validate(cypher)


def test_accepts_exists_subquery_without_return(
    validator: CypherSchemaValidator,
) -> None:
    cypher = (
        "MATCH (s:Service)\n"
        "WHERE NOT EXISTS { MATCH (s)-[:Manages]->(:EndpointSlice) }\n"
        "RETURN s['metadata']['namespace'] AS namespace,\n"
        "       s['metadata']['name'] AS service,\n"
        "       s['spec']['type'] AS type\n"
        "ORDER BY namespace, service"
    )
    validator.validate(cypher)


def test_accepts_exists_pattern_function(
    validator: CypherSchemaValidator,
) -> None:
    cypher = (
        "MATCH (s:Service)\n"
        "WHERE NOT EXISTS((s)-[:Manages]->(:EndpointSlice))\n"
        "RETURN s['metadata']['namespace'] AS namespace,\n"
        "       s['metadata']['name'] AS service,\n"
        "       s['spec']['type'] AS type\n"
        "ORDER BY namespace, service"
    )
    validator.validate(cypher)


def test_accepts_multiple_exists_subqueries_without_return(
    validator: CypherSchemaValidator,
) -> None:
    cypher = (
        "MATCH (ns:Namespace)<-[:BelongsTo]-(p:Pod)\n"
        "WHERE ns['metadata']['name'] = 'litmus'\n"
        "  AND NOT EXISTS { MATCH (d:Deployment)-[:Manages]->(rs:ReplicaSet)-[:Manages]->(p) }\n"
        "  AND NOT EXISTS { MATCH (ss:StatefulSet)-[:Manages]->(p) }\n"
        "  AND NOT EXISTS { MATCH (ds:DaemonSet)-[:Manages]->(p) }\n"
        "  AND NOT EXISTS { MATCH (j:Job)-[:Manages]->(p) }\n"
        "  AND NOT EXISTS { MATCH (rs2:ReplicaSet)-[:Manages]->(p) }\n"
        "RETURN p['metadata']['name'] AS pod,\n"
        "       p['status']['phase'] AS phase,\n"
        "       p['metadata']['uid'] AS uid\n"
        "ORDER BY pod"
    )
    validator.validate(cypher)


def test_rejects_unsupported_function(
    validator: CypherSchemaValidator,
) -> None:
    cypher = "MATCH (n:Pod) RETURN time() AS now"
    with pytest.raises(CypherCompatibilityError) as context:
        validator.validate(cypher)
    assert "time" in str(context.value)


def test_rejects_exists_property_function(
    validator: CypherSchemaValidator,
) -> None:
    cypher = "MATCH (n:Pod) WHERE exists(n.metadata) RETURN n"
    with pytest.raises(CypherValidationError) as context:
        validator.validate(cypher)
    message = str(context.value)
    assert "exists" in message


def test_rejects_inline_property_map_in_match(
    validator: CypherSchemaValidator,
) -> None:
    cypher = (
        "MATCH (p:Pod {metadata: {name: 'pyroscope-compactor-2'}})"
        "-[:HasLogs]->(l:Logs) RETURN l"
    )
    with pytest.raises(CypherCompatibilityError) as context:
        validator.validate(cypher)
    message = str(context.value)
    assert "Inline property maps in MATCH" in message
