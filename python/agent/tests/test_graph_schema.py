from k8s_graph_agent.graph_schema import GraphSchema


def test_from_payload() -> None:
    payload = {
        "relationships": [
            {"from": "Host", "edge": "IsClaimedBy", "to": "Ingress"},
            {
                "from": "Ingress",
                "edge": "DefinesBackend",
                "to": "IngressServiceBackend",
            },
        ]
    }
    schema = GraphSchema.from_payload(payload)
    assert schema is not None
    assert schema.allows("IsClaimedBy", "Host", "Ingress")
    assert schema.allows("DefinesBackend", "Ingress", "IngressServiceBackend")
    assert not schema.allows("DefinesBackend", "Host", "IngressServiceBackend")
