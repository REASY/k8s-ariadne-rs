from __future__ import annotations

from k8s_graph_agent.eval.traversal_plan import derive_traversal_plan


def test_derive_simple_chain() -> None:
    cypher = (
        "MATCH (s:Service)-[:Manages]->(es:EndpointSlice)-[:ContainsEndpoint]->(e:Endpoint)"
    )
    plan = derive_traversal_plan(cypher)
    assert plan == (
        "Service -[:Manages]-> EndpointSlice; "
        "EndpointSlice -[:ContainsEndpoint]-> Endpoint"
    )


def test_derive_reverse_and_optional_chain() -> None:
    cypher = (
        "MATCH (ns:Namespace)<-[:BelongsTo]-(s:Service)\n"
        "OPTIONAL MATCH (s)-[:Manages]->(es:EndpointSlice)"
    )
    plan = derive_traversal_plan(cypher)
    assert plan.splitlines() == [
        "Service -[:BelongsTo]-> Namespace",
        "OPTIONAL Service -[:Manages]-> EndpointSlice",
    ]


def test_derive_unwind_and_exclude() -> None:
    cypher = (
        "MATCH (p:Pod)-[:BelongsTo]->(ns:Namespace)\n"
        "UNWIND p['spec']['containers'] AS container\n"
        "WHERE NOT EXISTS { MATCH (:Deployment)-[:Manages]->(:ReplicaSet)-[:Manages]->(p) }"
    )
    plan = derive_traversal_plan(cypher)
    assert "Pod -[:BelongsTo]-> Namespace" in plan
    assert "UNWIND Pod.spec.containers AS container" in plan
    assert "EXCLUDE Deployment -[:Manages]-> ReplicaSet; ReplicaSet -[:Manages]-> Pod" in plan


def test_derive_standalone_node() -> None:
    cypher = "MATCH (sc:StorageClass)\nRETURN sc['metadata']['name'] AS storage_class_name"
    plan = derive_traversal_plan(cypher)
    assert plan == "StorageClass"
