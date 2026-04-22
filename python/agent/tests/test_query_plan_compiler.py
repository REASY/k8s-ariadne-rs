from __future__ import annotations

from k8s_graph_agent.graph_schema import GraphSchema
from k8s_graph_agent.query_plan import QueryPlanV1
from k8s_graph_agent.query_plan_compiler import compile_query_plan


def test_compile_simple_namespace_match_reverses_belongs_to() -> None:
    plan = QueryPlanV1.model_validate(
        {
            "$schema": "QueryPlanV1",
            "match": [
                {
                    "entity": "Namespace",
                    "bind": "ns",
                    "filter": [{"property": "name", "op": "eq", "value": "litmus"}],
                },
                {
                    "entity": "Pod",
                    "bind": "p",
                    "from": {"variable": "ns", "relationship": "BelongsTo"},
                    "filter": [],
                },
            ],
            "return": [{"variable": "p", "property": "name", "alias": "pod"}],
            "order_by": [{"column": "pod", "direction": "asc"}],
        }
    )
    compiled = compile_query_plan(plan, schema=GraphSchema.load_default()).cypher
    assert "MATCH (p:Pod)-[:BelongsTo]->(ns:Namespace)" in compiled
    assert 'RETURN p["metadata"]["name"] AS pod' in compiled


def test_compile_sum_memory_mib_uses_case_expression() -> None:
    plan = QueryPlanV1.model_validate(
        {
            "$schema": "QueryPlanV1",
            "match": [
                {"entity": "Namespace", "bind": "ns", "filter": []},
                {
                    "entity": "Pod",
                    "bind": "p",
                    "from": {"variable": "ns", "relationship": "BelongsTo"},
                    "filter": [],
                },
            ],
            "unwind": {
                "source_variable": "p",
                "source_property": "spec.containers",
                "element_type": "k8s_container_spec",
                "as": "container",
            },
            "stages": [
                {
                    "group_by": [{"variable": "ns"}],
                    "compute": [
                        {
                            "fn": "sum_memory_mib",
                            "input": "container",
                            "input_property": "resources.requests.memory",
                            "alias": "total_requested_memory_mib",
                        }
                    ],
                }
            ],
            "return": [
                {"variable": "ns", "property": "name", "alias": "namespace"},
                {"stage_ref": "total_requested_memory_mib"},
            ],
        }
    )
    compiled = compile_query_plan(plan, schema=GraphSchema.load_default()).cypher
    assert 'UNWIND p["spec"]["containers"] AS container' in compiled
    assert "sum(CASE" in compiled
    assert "AS total_requested_memory_mib" in compiled


def test_compile_coalesce_return() -> None:
    plan = QueryPlanV1.model_validate(
        {
            "$schema": "QueryPlanV1",
            "match": [
                {"entity": "PersistentVolumeClaim", "bind": "pvc", "filter": []},
                {
                    "entity": "PersistentVolume",
                    "bind": "pv",
                    "optional": True,
                    "from": {"variable": "pvc", "relationship": "BoundTo"},
                    "filter": [],
                },
            ],
            "return": [
                {
                    "coalesce": [
                        {"variable": "pvc", "property": "storage_class_name"},
                        {"variable": "pv", "property": "storage_class_name"},
                    ],
                    "alias": "storage_class",
                }
            ],
        }
    )
    compiled = compile_query_plan(plan, schema=GraphSchema.load_default()).cypher
    assert "OPTIONAL MATCH (pvc:PersistentVolumeClaim)-[:BoundTo]->(pv:PersistentVolume)" in compiled
    assert 'coalesce(pvc["spec"]["storageClassName"], pv["spec"]["storageClassName"]) AS storage_class' in compiled
