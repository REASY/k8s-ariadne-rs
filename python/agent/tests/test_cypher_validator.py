import os
import sys
import unittest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from k8s_graph_agent.cypher_validator import (
    CypherCompatibilityError,
    CypherValidationError,
    CypherSchemaValidator,
    SchemaValidationError,
)
from k8s_graph_agent.graph_schema import GraphSchema


class TestCypherSchemaValidator(unittest.TestCase):
    def setUp(self) -> None:
        edges = [
            ("Host", "IsClaimedBy", "Ingress"),
            ("Ingress", "DefinesBackend", "IngressServiceBackend"),
            ("IngressServiceBackend", "TargetsService", "Service"),
            ("Service", "Manages", "EndpointSlice"),
            ("EndpointSlice", "ContainsEndpoint", "Endpoint"),
            ("Endpoint", "HasAddress", "EndpointAddress"),
            ("EndpointAddress", "IsAddressOf", "Pod"),
            ("EndpointAddress", "ListedIn", "EndpointSlice"),
        ]
        schema = GraphSchema.from_edges(edges)
        self.validator = CypherSchemaValidator(schema)

    def test_valid_path(self) -> None:
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
        self.validator.validate(cypher)

    def test_invalid_edge_direction_and_node(self) -> None:
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
        with self.assertRaises(SchemaValidationError) as context:
            self.validator.validate(cypher)
        message = str(context.exception)
        self.assertIn("ListedIn", message)
        self.assertIn("Endpoint", message)
        self.assertIn("EndpointAddress", message)

    def test_rejects_wrong_direction_from_log_example(self) -> None:
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
        with self.assertRaises(SchemaValidationError) as context:
            self.validator.validate(cypher)
        message = str(context.exception)
        self.assertIn("HasAddress", message)
        self.assertIn("Endpoint", message)
        self.assertIn("EndpointAddress", message)

    def test_accepts_valid_query_from_log_example(self) -> None:
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
        self.validator.validate(cypher)

    def test_rejects_unsupported_function(self) -> None:
        cypher = "MATCH (n:Pod) RETURN time() AS now"
        with self.assertRaises(CypherCompatibilityError) as context:
            self.validator.validate(cypher)
        self.assertIn("time", str(context.exception))

    def test_rejects_exists_property_function(self) -> None:
        cypher = "MATCH (n:Pod) WHERE exists(n.metadata) RETURN n"
        with self.assertRaises(CypherValidationError) as context:
            self.validator.validate(cypher)
        message = str(context.exception)
        self.assertIn("exists", message)


if __name__ == "__main__":
    unittest.main()
