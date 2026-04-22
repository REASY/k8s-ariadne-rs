from __future__ import annotations

from pathlib import Path

import yaml

from k8s_graph_agent.graph_schema import GraphSchema
from k8s_graph_agent.query_plan import (
    QueryPlanV1Lite,
    QueryPlanV1Mid,
    TranslatorOutput,
    upgrade_lite_plan,
    upgrade_mid_plan,
)
from k8s_graph_agent.query_plan_validator import (
    QueryPlanValidationError,
    validate_query_plan,
    validate_translator_output,
)


def _load_examples() -> list[dict]:
    path = (
        Path(__file__).resolve().parents[1]
        / "eval"
        / "query_plan_v1_examples.yaml"
    )
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(payload, list)
    return payload


def test_handwritten_query_plan_examples_validate() -> None:
    schema = GraphSchema.load_default()
    for example in _load_examples():
        output = TranslatorOutput.model_validate(example["output"])
        validate_translator_output(output, schema=schema)


def test_invalid_relationship_is_rejected() -> None:
    schema = GraphSchema.load_default()
    output = TranslatorOutput.model_validate(
        {
            "mode": "plan",
            "plan": {
                "$schema": "QueryPlanV1",
                "match": [
                    {"entity": "Namespace", "bind": "ns"},
                    {
                        "entity": "Pod",
                        "bind": "p",
                        "from": {"variable": "ns", "relationship": "TargetsService"},
                    },
                ],
                "return": [{"variable": "p", "property": "name", "alias": "pod"}],
                "distinct": False,
            },
        }
    )

    try:
        validate_translator_output(output, schema=schema)
    except QueryPlanValidationError as exc:
        assert any(issue.code == "illegal_relationship" for issue in exc.issues)
    else:
        raise AssertionError("expected relationship validation error")


def test_dropped_variable_after_stage_is_rejected() -> None:
    schema = GraphSchema.load_default()
    output = TranslatorOutput.model_validate(
        {
            "mode": "plan",
            "plan": {
                "$schema": "QueryPlanV1",
                "match": [
                    {"entity": "Namespace", "bind": "ns"},
                    {
                        "entity": "Pod",
                        "bind": "p",
                        "from": {"variable": "ns", "relationship": "BelongsTo"},
                    },
                ],
                "stages": [
                    {
                        "group_by": [{"variable": "ns"}],
                        "compute": [{"fn": "count_distinct", "input": "p", "alias": "pod_count"}],
                    }
                ],
                "return": [
                    {"variable": "p", "property": "name", "alias": "pod"},
                    {"stage_ref": "pod_count"},
                ],
                "distinct": False,
            },
        }
    )

    try:
        validate_translator_output(output, schema=schema)
    except QueryPlanValidationError as exc:
        assert any(issue.code == "unknown_variable" for issue in exc.issues)
    else:
        raise AssertionError("expected dropped-variable validation error")


def test_raw_cypher_fallback_rejects_write_operations() -> None:
    output = TranslatorOutput.model_validate(
        {
            "mode": "cypher",
            "cypher": "MATCH (n:Pod) DELETE n",
            "reason": "requires unsupported feature",
        }
    )

    try:
        validate_translator_output(output, schema=GraphSchema.load_default())
    except QueryPlanValidationError as exc:
        assert any(issue.code == "write_operation" for issue in exc.issues)
    else:
        raise AssertionError("expected write-operation rejection")


def test_upgrade_lite_plan_validates_as_full_plan() -> None:
    lite = QueryPlanV1Lite.model_validate(
        {
            "$schema": "QueryPlanV1Lite",
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
            "distinct": False,
        }
    )
    full = upgrade_lite_plan(lite)
    validate_query_plan(full, schema=GraphSchema.load_default())


def test_upgrade_mid_plan_validates_as_full_plan() -> None:
    mid = QueryPlanV1Mid.model_validate(
        {
            "$schema": "QueryPlanV1Mid",
            "match": [
                {
                    "entity": "Namespace",
                    "bind": "ns",
                    "filter": [],
                },
                {
                    "entity": "Container",
                    "bind": "c",
                    "from": {"variable": "ns", "relationship": "BelongsTo"},
                    "filter": [],
                },
                {
                    "entity": "Logs",
                    "bind": "l",
                    "filter": [],
                    "optional": True,
                    "property_join": {
                        "local_property": "container_uid",
                        "remote_variable": "c",
                        "remote_property": "uid",
                    },
                },
            ],
            "where": [
                {
                    "variable": "ns",
                    "property": "name",
                    "op": "eq",
                    "value": "litmus",
                }
            ],
            "return": [
                {"variable": "c", "property": "name", "alias": "container"},
                {"variable": "l", "property": "content", "alias": "logs"},
            ],
            "order_by": [{"column": "container", "direction": "asc"}],
            "distinct": False,
        }
    )
    full = upgrade_mid_plan(mid)
    validate_query_plan(full, schema=GraphSchema.load_default())


def test_negation_can_reference_current_step_bind() -> None:
    plan = TranslatorOutput.model_validate(
        {
            "mode": "plan",
            "plan": {
                "$schema": "QueryPlanV1",
                "match": [
                    {
                        "entity": "PersistentVolumeClaim",
                        "bind": "pvc",
                        "filter": [],
                        "not_exists": [
                            {
                                "match": [
                                    {
                                        "entity": "PersistentVolume",
                                        "from": {
                                            "variable": "pvc",
                                            "relationship": "BoundTo",
                                        },
                                        "filter": [],
                                    }
                                ]
                            }
                        ],
                    }
                ],
                "return": [{"variable": "pvc", "property": "name", "alias": "pvc_name"}],
                "distinct": False,
            },
        }
    )
    validate_translator_output(plan, schema=GraphSchema.load_default())


def test_derived_fn_can_reference_prior_stage_alias() -> None:
    plan = TranslatorOutput.model_validate(
        {
            "mode": "plan",
            "plan": {
                "$schema": "QueryPlanV1",
                "match": [
                    {"entity": "Service", "bind": "s", "filter": []},
                    {
                        "entity": "EndpointSlice",
                        "bind": "es",
                        "from": {"variable": "s", "relationship": "Manages"},
                        "filter": [],
                        "optional": True,
                    },
                    {
                        "entity": "Endpoint",
                        "bind": "e",
                        "from": {"variable": "es", "relationship": "ContainsEndpoint"},
                        "filter": [],
                        "optional": True,
                    },
                    {
                        "entity": "EndpointAddress",
                        "bind": "ea",
                        "from": {"variable": "e", "relationship": "HasAddress"},
                        "filter": [],
                        "optional": True,
                    },
                ],
                "stages": [
                    {
                        "group_by": [{"variable": "s"}],
                        "compute": [
                            {
                                "fn": "collect_distinct",
                                "input": "ea",
                                "input_property": "address",
                                "alias": "pod_ips",
                            }
                        ],
                    },
                    {
                        "group_by": [{"variable": "s"}],
                        "compute": [
                            {
                                "fn": "size",
                                "input": "pod_ips",
                                "alias": "pod_ip_count",
                            }
                        ],
                    },
                ],
                "return": [
                    {"variable": "s", "property": "name", "alias": "service"},
                    {"stage_ref": "pod_ip_count"},
                ],
                "distinct": False,
            },
        }
    )
    validate_translator_output(plan, schema=GraphSchema.load_default())
