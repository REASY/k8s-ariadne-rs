from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import re
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
import uuid

import yaml

from k8s_graph_agent.adk_translate import AdkCypherTranslator, _run_async
from k8s_graph_agent.agent import GraphMcpClient
from k8s_graph_agent.config import AdkConfig, AgentConfig
from k8s_graph_agent.eval.loader import load_dataset
from k8s_graph_agent.eval.matching import evaluate_expected_match
from k8s_graph_agent.graph_schema import GraphSchema
from k8s_graph_agent.mcp_client import StreamableHttpMcpClient, extract_json_content
from k8s_graph_agent.prompting import prompt_sections_from_graph_schema_payload
from k8s_graph_agent.query_plan import TranslatorOutput
from k8s_graph_agent.query_plan_compiler import compile_query_plan
from k8s_graph_agent.query_plan_prompting import build_distilled_ir_prompt_context
from k8s_graph_agent.query_plan_validator import validate_translator_output
from smoke_test_query_plan_generation import _strip_code_fences
from smoke_test_query_plan_hard_generation import _build_runner_for_model, _config_for_model, _run_agent_debug
from smoke_test_query_plan_hard_generation import _OUTPUT_SKELETON, _IR_CONTRACT_SECTION


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare direct Cypher vs QueryPlan->Cypher on the hard subset."
    )
    parser.add_argument(
        "--examples",
        type=Path,
        default=Path("eval/query_plan_v1_hard_examples.yaml"),
    )
    parser.add_argument(
        "--dataset",
        type=Path,
        default=Path("eval/questions_gold_expanded_dense_v2.yaml"),
    )
    parser.add_argument(
        "--models",
        default="openai/gpt-5-mini-2025-08-07,gemini-2.5-flash",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("eval/query_plan_hard_eval"),
    )
    parser.add_argument(
        "--question-parallelism",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--prompt-variant",
        choices=["v6", "distilled-v2"],
        default="v6",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    examples = _load_examples(args.examples)
    dataset = {question.id: question for question in load_dataset(args.dataset)}
    schema = GraphSchema.load_default()
    run_dir = args.output_dir / datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    agent_config = AgentConfig.from_env()
    mcp = StreamableHttpMcpClient(
        base_url=agent_config.mcp_url,
        timeout_seconds=agent_config.request_timeout_seconds,
        client_name=agent_config.client_name,
        client_version=agent_config.client_version,
        auth_token=agent_config.mcp_auth_token,
    )
    try:
        prompt_sections = _load_prompt_sections(mcp, examples)
        distilled_context = build_distilled_ir_prompt_context(
            node_connectivity=prompt_sections.node_connectivity,
            schema=schema,
        )
        manifest = {
            "examples": len(examples),
            "schema_reference_chars": len(prompt_sections.schema_reference),
            "node_connectivity_chars": len(prompt_sections.node_connectivity),
            "distilled_context_chars": len(distilled_context.render()),
            "models": [m.strip() for m in args.models.split(",") if m.strip()],
        }
        (run_dir / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True),
            encoding="utf-8",
        )

        for model in [m.strip() for m in args.models.split(",") if m.strip()]:
            model_dir = run_dir / _slugify(model)
            model_dir.mkdir(parents=True, exist_ok=True)
            report = _run_model(
                model,
                examples,
                dataset,
                schema,
                prompt_sections.schema_reference,
                prompt_sections.node_connectivity,
                distilled_context.render(),
                model_dir,
                args.question_parallelism,
                args.prompt_variant,
            )
            _write_json_atomic(model_dir / "report.json", report)
            ir_summary = report["ir"]["summary"]
            direct_summary = report["direct"]["summary"]
            print(
                f"[hard-eval] {model}: "
                f"ir={ir_summary['matched']}/{ir_summary['total']} "
                f"direct={direct_summary['matched']}/{direct_summary['total']}"
            )
    finally:
        mcp.close()
    return 0


def _load_examples(path: Path) -> list[dict[str, str]]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"{path} must contain a top-level list")
    return [{"id": str(item["id"]), "question": str(item["question"])} for item in payload]


def _run_model(
    model: str,
    examples: list[dict[str, str]],
    dataset: dict,
    schema: GraphSchema,
    schema_reference: str,
    node_connectivity: str,
    distilled_context: str,
    model_dir: Path,
    question_parallelism: int,
    prompt_variant: str,
) -> dict:
    ir_config = _config_for_model(model)
    direct_base = AdkConfig.from_env()
    ir_results: dict[str, dict] = {}
    direct_results: dict[str, dict] = {}
    ir_matched = 0
    direct_matched = 0
    max_workers = max(1, question_parallelism)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_qid = {
            executor.submit(
                _evaluate_question,
                model=model,
                example=example,
                gold=dataset[example["id"]],
                schema=schema,
                schema_reference=schema_reference,
                node_connectivity=node_connectivity,
                distilled_context=distilled_context,
                ir_config=ir_config,
                direct_base=direct_base,
                prompt_variant=prompt_variant,
            ): example["id"]
            for example in examples
        }
        for future in as_completed(future_to_qid):
            question_id = future_to_qid[future]
            ir_entry, direct_entry = future.result()
            ir_results[question_id] = ir_entry
            direct_results[question_id] = direct_entry
            if ir_entry.get("matched"):
                ir_matched += 1
            if direct_entry.get("matched"):
                direct_matched += 1
            _write_checkpoint(
                model_dir=model_dir,
                model=model,
                total=len(examples),
                completed=len(ir_results),
                last_question_id=question_id,
                ir_matched=ir_matched,
                direct_matched=direct_matched,
                ir_results=ir_results,
                direct_results=direct_results,
            )

    return {
        "model": model,
        "ir": {
            "summary": {"matched": ir_matched, "total": len(examples)},
            "results": ir_results,
        },
        "direct": {
            "summary": {"matched": direct_matched, "total": len(examples)},
            "results": direct_results,
        },
    }


def _evaluate_question(
    *,
    model: str,
    example: dict[str, str],
    gold,
    schema: GraphSchema,
    schema_reference: str,
    node_connectivity: str,
    distilled_context: str,
    ir_config: AdkConfig,
    direct_base: AdkConfig,
    prompt_variant: str,
) -> tuple[dict, dict]:
    agent_config = AgentConfig.from_env()
    mcp = StreamableHttpMcpClient(
        base_url=agent_config.mcp_url,
        timeout_seconds=agent_config.request_timeout_seconds,
        client_name=agent_config.client_name,
        client_version=agent_config.client_version,
        auth_token=agent_config.mcp_auth_token,
    )
    graph = GraphMcpClient(mcp=mcp)
    worker_ir_config = AdkConfig(
        model=ir_config.model,
        provider=ir_config.provider,
        base_url=ir_config.base_url,
        api_key=ir_config.api_key,
        app_name=ir_config.app_name,
        user_id=ir_config.user_id,
        session_id=f"query-plan-hard-{uuid.uuid4().hex[:8]}",
        temperature=ir_config.temperature,
        max_output_tokens=ir_config.max_output_tokens,
        use_mcp_prompt=ir_config.use_mcp_prompt,
    )
    ir_runner, ir_types, ir_session_service = _build_runner_for_model(worker_ir_config)
    direct_config = AdkConfig(
        model=model,
        provider=ir_config.provider,
        base_url=ir_config.base_url,
        api_key=ir_config.api_key,
        app_name=direct_base.app_name,
        user_id=direct_base.user_id,
        session_id=f"query-plan-hard-direct-{uuid.uuid4().hex[:8]}",
        temperature=ir_config.temperature,
        max_output_tokens=direct_base.max_output_tokens,
        use_mcp_prompt=direct_base.use_mcp_prompt,
    )
    direct_translator = AdkCypherTranslator(mcp=mcp, config=direct_config)
    try:
        ir_entry = _run_ir_question(
            example=example,
            gold=gold,
            graph=graph,
            schema=schema,
            schema_reference=schema_reference,
            node_connectivity=node_connectivity,
            distilled_context=distilled_context,
            config=worker_ir_config,
            runner=ir_runner,
            types=ir_types,
            session_service=ir_session_service,
            prompt_variant=prompt_variant,
        )
        direct_entry = _run_direct_question(
            question_text=gold.question,
            gold=gold,
            graph=graph,
            translator=direct_translator,
        )
        return ir_entry, direct_entry
    finally:
        mcp.close()


def _run_ir_question(
    *,
    example: dict[str, str],
    gold,
    graph: GraphMcpClient,
    schema: GraphSchema,
    schema_reference: str,
    node_connectivity: str,
    distilled_context: str,
    config: AdkConfig,
    runner,
    types,
    session_service,
    prompt_variant: str,
) -> dict:
    session_id = f"{config.session_id}-{uuid.uuid4().hex}"
    _run_async(
        session_service.create_session(
            app_name=config.app_name,
            user_id=config.user_id,
            session_id=session_id,
        )
    )
    prompt = _build_eval_prompt(
        example["question"],
        schema_reference,
        node_connectivity,
        distilled_context,
        prompt_variant,
    )
    content = types.Content(role="user", parts=[types.Part(text=prompt)])
    try:
        response_text, usage, debug = _run_agent_debug(runner, config, content, session_id)
        parsed = TranslatorOutput.model_validate_json(_strip_code_fences(response_text))
        validate_translator_output(parsed, schema=schema)
        if parsed.mode == "cypher":
            assert parsed.cypher is not None
            compiled_cypher = parsed.cypher
            mode = "cypher"
            reason = parsed.reason
        else:
            assert parsed.plan is not None
            compiled_cypher = compile_query_plan(parsed.plan, schema=schema).cypher
            mode = "plan"
            reason = None
        result = graph.execute_cypher(compiled_cypher)
        match_eval = evaluate_expected_match(result, gold.expected)
        return {
            "parsed": True,
            "mode": mode,
            "fallback_reason": reason,
            "compiled_cypher": compiled_cypher,
            "matched": match_eval.matched,
            "match_type": match_eval.match_type.value,
            "match_details": match_eval.as_dict(),
            "tokens": {
                "prompt": usage.prompt_tokens,
                "output": usage.output_tokens,
                "total": usage.total_tokens,
            },
            "response": parsed.model_dump(mode="json", by_alias=True),
            "debug": debug,
        }
    except Exception as exc:
        return {"parsed": False, "matched": False, "error": str(exc)}


def _run_direct_question(*, question_text: str, gold, graph: GraphMcpClient, translator: AdkCypherTranslator) -> dict:
    try:
        outcome = translator.translate_with_attempts(question_text, max_attempts=2)
        if outcome.cypher is None:
            return {
                "matched": False,
                "cypher": None,
                "error": outcome.error,
                "attempts": [_serialize_attempt(item) for item in outcome.attempts],
            }
        result = graph.execute_cypher(outcome.cypher)
        match_eval = evaluate_expected_match(result, gold.expected)
        return {
            "matched": match_eval.matched,
            "match_type": match_eval.match_type.value,
            "match_details": match_eval.as_dict(),
            "cypher": outcome.cypher,
            "attempts": [_serialize_attempt(item) for item in outcome.attempts],
        }
    except Exception as exc:
        return {"matched": False, "error": str(exc)}


def _serialize_attempt(attempt) -> dict:
    return {
        "attempt": attempt.attempt,
        "cypher": attempt.cypher,
        "valid": attempt.valid,
        "error": attempt.error,
        "usage": {
            "prompt_tokens": attempt.usage.prompt_tokens,
            "output_tokens": attempt.usage.output_tokens,
            "total_tokens": attempt.usage.total_tokens,
        },
    }


def _load_prompt_sections(mcp: StreamableHttpMcpClient, examples: list[dict[str, str]]):
    del examples
    payload = extract_json_content(
        mcp.call_tool("graph_schema", {"format": "structured"})
    )
    if not isinstance(payload, dict):
        raise ValueError("MCP graph_schema did not return a JSON object")
    sections = prompt_sections_from_graph_schema_payload(payload)
    if sections is None:
        raise ValueError("MCP graph_schema payload did not contain schema sections")
    return sections


def _build_eval_prompt(
    question: str,
    schema_reference: str,
    node_connectivity: str,
    distilled_context: str,
    prompt_variant: str = "v6",
) -> str:
    if prompt_variant == "distilled-v2":
        return _build_distilled_v2_prompt(question, distilled_context=distilled_context)
    simplified_schema = _simplify_schema_reference(schema_reference)
    schema_context = f"{simplified_schema}\n\n{node_connectivity}"
    return _build_concise_ir_prompt_v6(question, schema_context=schema_context)


def _build_distilled_v2_prompt(question: str, distilled_context: str = "") -> str:
    parts = []
    parts.append("Return only JSON matching this root schema:")
    parts.append("")
    parts.append(_DISTILLED_V2_OUTPUT_SKELETON)
    parts.append("")
    parts.append("Use EXACT field names from QueryPlanV1. Do not invent fields.")
    parts.append("")
    parts.append("Entities, relationships, and logical properties are defined by the distilled reference below.")
    parts.append("")
    parts.append(_DISTILLED_V2_IMPORTANT_FORMS)
    parts.append("")
    parts.append("Examples:")
    parts.append(_DISTILLED_EXAMPLES)
    parts.append("")
    if distilled_context:
        parts.append("Use the following distilled IR reference as the source of truth for entity types, relationship directions, and logical property names.")
        parts.append("Do not invent entities, relationships, or properties not supported there.")
        parts.append("Do not change the JSON field names from the QueryPlanV1 examples above.")
        parts.append("")
        parts.append(distilled_context.strip())
        parts.append("")
    parts.append("Return only JSON for this question:")
    parts.append(question)
    parts.append("")
    return "\n".join(parts)


_DISTILLED_V2_OUTPUT_SKELETON = """\
{
  "mode": "plan",
  "plan": {
    "$schema": "QueryPlanV1",
    "match": [ ... ],
    "where": [ ... ],
    "unwind": null | {
      "source_variable": "p",
      "source_property": "spec.containers",
      "element_type": "k8s_container_spec",
      "as": "container"
    },
    "stages": [ ... ],
    "return": [ ... ],
    "order_by": [ ... ],
    "limit": null,
    "distinct": false
  }
}"""


_DISTILLED_V2_IMPORTANT_FORMS = """\
Important forms:

1. Aggregation stage
{
  "group_by": [
    {"variable": "ns"},
    {"variable": "p", "property": "phase", "alias": "phase"}
  ],
  "compute": [
    {"fn": "count", "input": "p", "alias": "pod_count"},
    {"fn": "count_distinct", "input": "p", "alias": "pod_count"},
    {"fn": "sum_memory_mib", "input": "container", "input_property": "resources.requests.memory", "alias": "total_requested_memory_mib"},
    {"fn": "collect_distinct", "input": "ea", "alias": "pod_ips"},
    {"fn": "size", "input": "pod_ips", "alias": "pod_ip_count"}
  ],
  "having": [
    {"alias": "pod_count", "op": "gt", "value": 50},
    {"alias": "endpoint_slice_count", "op": "eq", "value": 0},
    {"variable": "s", "op": "is_not_null"}
  ]
}

2. Return expressions
- property return:
  {"variable": "ns", "property": "name", "alias": "namespace"}
- stage ref:
  {"stage_ref": "pod_count"}
- coalesce:
  {
    "coalesce": [
      {"variable": "pvc", "property": "storage_class_name"},
      {"variable": "pv", "property": "storage_class_name"}
    ],
    "alias": "storage_class"
  }

3. Use `unwind` only for `spec.containers`.

4. Deployment to Pod ownership goes through ReplicaSet:
Deployment -[:Manages]-> ReplicaSet -[:Manages]-> Pod

5. Service backing chain:
Service -[:Manages]-> EndpointSlice -[:ContainsEndpoint]-> Endpoint
-[:HasAddress]-> EndpointAddress -[:IsAddressOf]-> Pod

6. EndpointAddress uses property `address`, not `ip`."""


def _build_concise_ir_prompt_v6(question: str, schema_context: str = "") -> str:
    """v6 prompt: v5 structure but with distilled logical properties instead of
    simplified K8s schema. Node connectivity is kept from the schema context."""
    parts = []
    if schema_context:
        # Extract only the node connectivity section, drop the schema reference
        connectivity = _extract_connectivity(schema_context)
        if connectivity:
            parts.append(connectivity)
            parts.append("")
    parts.append("## QueryPlanV1 output format")
    parts.append("")
    parts.append(_OUTPUT_SKELETON)
    parts.append("")
    parts.append(_IR_CONTRACT_SECTION)
    parts.append("")
    parts.append(_IMPORTANT_FORMS)
    parts.append("")
    parts.append(_DISTILLED_LOGICAL_PROPERTIES)
    parts.append("")
    parts.append("Examples:")
    parts.append(_DISTILLED_EXAMPLES)
    parts.append("")
    parts.append(f"Q: {question}")
    return "\n".join(parts)


def _extract_connectivity(schema_context: str) -> str | None:
    """Extract the Node Connectivity section from the schema context."""
    marker = "### Node Connectivity"
    idx = schema_context.find(marker)
    if idx < 0:
        # Try alternate marker
        marker = "Node Connectivity"
        idx = schema_context.find(marker)
    if idx < 0:
        return schema_context  # fallback: use the whole thing
    return schema_context[idx:].strip()


_DISTILLED_LOGICAL_PROPERTIES = """\
Logical entity properties (use these as QueryPlan property names):
- AWX: name, namespace, uid
- Cluster: name, namespace, uid
- ConfigMap: name, namespace, uid
- Container: container_type, container_uid, name, namespace, pod_name, uid
- DaemonSet: name, namespace, uid
- Deployment: name, namespace, ready_replicas, replicas, uid
- Endpoint: name, namespace, uid
- EndpointAddress: address, name, namespace, uid
- EndpointSlice: address_type, name, namespace, uid
- Event: event_time, name, namespace, note, reason, type, uid
- Host: name
- Ingress: name, namespace, uid
- IngressServiceBackend: name, namespace, uid
- Job: name, namespace, uid
- Logs: container_uid, content
- Namespace: name, namespace, uid
- NetworkPolicy: name, namespace, uid
- Node: name, namespace, phase, provider_id, uid
- PersistentVolume: capacity_storage, name, namespace, phase, storage_class_name, uid
- PersistentVolumeClaim: name, namespace, phase, storage_class_name, uid, volume_name
- Pod: name, namespace, phase, spec.containers, uid
- Provisioner: name, namespace, uid
- ReplicaSet: name, namespace, uid
- Service: name, namespace, uid
- ServiceAccount: name, namespace, uid
- StatefulSet: name, namespace, replicas, uid
- StorageClass: name, namespace, uid
- k8s_container_spec: image, name, resources.limits.cpu, resources.limits.memory, resources.requests.cpu, resources.requests.memory"""


def _build_concise_ir_prompt_v4(question: str, schema_context: str = "") -> str:
    """v4 prompt: v3 concise structure + 4 full examples from distilled."""
    parts = []
    if schema_context:
        parts.append(schema_context)
        parts.append("")
    parts.append("## QueryPlanV1 output format")
    parts.append("")
    parts.append(_OUTPUT_SKELETON)
    parts.append("")
    parts.append(_IR_CONTRACT_SECTION)
    parts.append("")
    parts.append("Examples:")
    parts.append(_DISTILLED_EXAMPLES)
    parts.append("")
    parts.append(f"Q: {question}")
    return "\n".join(parts)


def _build_concise_ir_prompt_v5(question: str, schema_context: str = "") -> str:
    """v5 prompt: v4 + Important forms section from distilled-v2."""
    parts = []
    if schema_context:
        parts.append(schema_context)
        parts.append("")
    parts.append("## QueryPlanV1 output format")
    parts.append("")
    parts.append(_OUTPUT_SKELETON)
    parts.append("")
    parts.append(_IR_CONTRACT_SECTION)
    parts.append("")
    parts.append(_IMPORTANT_FORMS)
    parts.append("")
    parts.append("Examples:")
    parts.append(_DISTILLED_EXAMPLES)
    parts.append("")
    parts.append(f"Q: {question}")
    return "\n".join(parts)


_IMPORTANT_FORMS = """\
Important forms:

1. Aggregation stage
{
  "group_by": [
    {"variable": "ns"},
    {"variable": "p", "property": "phase", "alias": "phase"}
  ],
  "compute": [
    {"fn": "count", "input": "p", "alias": "pod_count"},
    {"fn": "count_distinct", "input": "p", "alias": "distinct_pod_count"},
    {"fn": "sum_memory_mib", "input": "container", "input_property": "resources.requests.memory", "alias": "total_requested_memory_mib"},
    {"fn": "collect_distinct", "input": "ea", "input_property": "address", "alias": "pod_ips"},
    {"fn": "size", "input": "pod_ips", "alias": "pod_ip_count"}
  ],
  "having": [
    {"alias": "pod_count", "op": "gt", "value": 50},
    {"alias": "endpoint_slice_count", "op": "eq", "value": 0},
    {"variable": "s", "op": "is_not_null"}
  ]
}

2. Return expressions
- property return:
  {"variable": "ns", "property": "name", "alias": "namespace"}
- stage ref:
  {"stage_ref": "pod_count"}
- coalesce:
  {
    "coalesce": [
      {"variable": "pvc", "property": "storage_class_name"},
      {"variable": "pv", "property": "storage_class_name"}
    ],
    "alias": "storage_class"
  }

3. Use `unwind` only for `spec.containers`.

4. Deployment to Pod ownership goes through ReplicaSet:
Deployment -[:Manages]-> ReplicaSet -[:Manages]-> Pod

5. Service backing chain:
Service -[:Manages]-> EndpointSlice -[:ContainsEndpoint]-> Endpoint
-[:HasAddress]-> EndpointAddress -[:IsAddressOf]-> Pod

6. EndpointAddress uses property `address`, not `ip`."""


_DISTILLED_EXAMPLES = """
Question: For each namespace, sum requested memory in MiB across all pod containers.
JSON:
{
  "mode": "plan",
  "plan": {
    "$schema": "QueryPlanV1",
    "match": [
      {"entity": "Namespace", "bind": "ns", "filter": []},
      {"entity": "Pod", "bind": "p", "from": {"variable": "ns", "relationship": "BelongsTo"}, "filter": []}
    ],
    "where": [],
    "unwind": {
      "source_variable": "p",
      "source_property": "spec.containers",
      "element_type": "k8s_container_spec",
      "as": "container"
    },
    "stages": [
      {
        "group_by": [
          {"variable": "ns"}
        ],
        "compute": [
          {"fn": "sum_memory_mib", "input": "container", "input_property": "resources.requests.memory", "alias": "total_requested_memory_mib"}
        ],
        "having": []
      }
    ],
    "return": [
      {"variable": "ns", "property": "name", "alias": "namespace"},
      {"stage_ref": "total_requested_memory_mib"}
    ],
    "order_by": [
      {"column": "namespace", "direction": "asc"}
    ],
    "limit": null,
    "distinct": false
  }
}

Question: List PVCs in namespace pyroscope with their bound PV name and storage class (if any).
JSON:
{
  "mode": "plan",
  "plan": {
    "$schema": "QueryPlanV1",
    "match": [
      {"entity": "Namespace", "bind": "ns", "filter": [{"property": "name", "op": "eq", "value": "pyroscope"}]},
      {"entity": "PersistentVolumeClaim", "bind": "pvc", "from": {"variable": "ns", "relationship": "BelongsTo"}, "filter": []},
      {"entity": "PersistentVolume", "bind": "pv", "from": {"variable": "pvc", "relationship": "BoundTo"}, "filter": [], "optional": true}
    ],
    "where": [],
    "unwind": null,
    "stages": [],
    "return": [
      {"variable": "pv", "property": "name", "alias": "pv_name"},
      {"variable": "pvc", "property": "name", "alias": "pvc_name"},
      {
        "coalesce": [
          {"variable": "pvc", "property": "storage_class_name"},
          {"variable": "pv", "property": "storage_class_name"}
        ],
        "alias": "storage_class"
      }
    ],
    "order_by": [
      {"column": "pvc_name", "direction": "asc"}
    ],
    "limit": null,
    "distinct": false
  }
}

Question: For each service, count distinct backing pod IPs.
JSON:
{
  "mode": "plan",
  "plan": {
    "$schema": "QueryPlanV1",
    "match": [
      {"entity": "Service", "bind": "s", "filter": []},
      {"entity": "EndpointSlice", "bind": "es", "from": {"variable": "s", "relationship": "Manages"}, "filter": [], "optional": true},
      {"entity": "Endpoint", "bind": "e", "from": {"variable": "es", "relationship": "ContainsEndpoint"}, "filter": [], "optional": true},
      {"entity": "EndpointAddress", "bind": "ea", "from": {"variable": "e", "relationship": "HasAddress"}, "filter": [], "optional": true},
      {"entity": "Pod", "bind": "p", "from": {"variable": "ea", "relationship": "IsAddressOf"}, "filter": [], "optional": true}
    ],
    "where": [],
    "unwind": null,
    "stages": [
      {
        "group_by": [
          {"variable": "s"}
        ],
        "compute": [
          {"fn": "collect_distinct", "input": "ea", "input_property": "address", "alias": "pod_ips"}
        ],
        "having": []
      },
      {
        "group_by": [
          {"variable": "s"}
        ],
        "compute": [
          {"fn": "size", "input": "pod_ips", "alias": "pod_ip_count"}
        ],
        "having": []
      }
    ],
    "return": [
      {"variable": "s", "property": "name", "alias": "service"},
      {"stage_ref": "pod_ip_count"}
    ],
    "order_by": [
      {"column": "service", "direction": "asc"}
    ],
    "limit": null,
    "distinct": false
  }
}

Question: For each service in namespace litmus, count pods by phase.
JSON:
{
  "mode": "plan",
  "plan": {
    "$schema": "QueryPlanV1",
    "match": [
      {"entity": "Namespace", "bind": "ns", "filter": [{"property": "name", "op": "eq", "value": "litmus"}]},
      {"entity": "Service", "bind": "s", "from": {"variable": "ns", "relationship": "BelongsTo"}, "filter": []},
      {"entity": "EndpointSlice", "bind": "es", "from": {"variable": "s", "relationship": "Manages"}, "filter": [], "optional": true},
      {"entity": "Endpoint", "bind": "e", "from": {"variable": "es", "relationship": "ContainsEndpoint"}, "filter": [], "optional": true},
      {"entity": "EndpointAddress", "bind": "ea", "from": {"variable": "e", "relationship": "HasAddress"}, "filter": [], "optional": true},
      {"entity": "Pod", "bind": "p", "from": {"variable": "ea", "relationship": "IsAddressOf"}, "filter": [], "optional": true}
    ],
    "where": [],
    "unwind": null,
    "stages": [
      {
        "group_by": [
          {"variable": "s"},
          {"variable": "p", "property": "phase", "alias": "phase"}
        ],
        "compute": [
          {"fn": "count_distinct", "input": "p", "alias": "pod_count"}
        ],
        "having": []
      }
    ],
    "return": [
      {"variable": "s", "property": "name", "alias": "service"},
      {"stage_ref": "phase"},
      {"stage_ref": "pod_count"}
    ],
    "order_by": [
      {"column": "service", "direction": "asc"},
      {"column": "phase", "direction": "asc"}
    ],
    "limit": null,
    "distinct": false
  }
}"""


def _simplify_schema_reference(schema_ref: str) -> str:
    """Simplify the MCP schema reference for the IR prompt.

    1. Replace #/$defs/... and #/definitions/... type refs with OBJECT
    2. Replace [#/$defs/...] array-of-ref types with [OBJECT]
    3. Drop the entire 'Referenced types (used via `#/$defs/`):' section
    """
    # Drop everything from the referenced types section onward
    ref_section = re.search(
        r"^Referenced types \(used via.*$",
        schema_ref,
        flags=re.MULTILINE,
    )
    if ref_section:
        schema_ref = schema_ref[: ref_section.start()].rstrip()

    # Replace [#/$defs/...] with [OBJECT]
    schema_ref = re.sub(r"\[#/\$?defs/[^\]]+\]", "[OBJECT]", schema_ref)
    schema_ref = re.sub(r"\[#/definitions/[^\]]+\]", "[OBJECT]", schema_ref)

    # Replace #/$defs/... and #/definitions/... with OBJECT
    schema_ref = re.sub(r"#/\$?defs/[A-Za-z0-9_.]+", "OBJECT", schema_ref)
    schema_ref = re.sub(r"#/definitions/[A-Za-z0-9_.]+", "OBJECT", schema_ref)

    return schema_ref


def _slugify(model: str) -> str:
    return model.replace("/", "_").replace(".", "_")


def _write_checkpoint(
    *,
    model_dir: Path,
    model: str,
    total: int,
    completed: int,
    last_question_id: str,
    ir_matched: int,
    direct_matched: int,
    ir_results: dict[str, dict],
    direct_results: dict[str, dict],
) -> None:
    payload = {
        "model": model,
        "progress": {
            "completed": completed,
            "total": total,
            "last_question_id": last_question_id,
        },
        "ir": {
            "summary": {"matched": ir_matched, "total": total},
            "results": ir_results,
        },
        "direct": {
            "summary": {"matched": direct_matched, "total": total},
            "results": direct_results,
        },
    }
    _write_json_atomic(model_dir / "report.partial.json", payload)


def _write_json_atomic(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=path.parent,
        delete=False,
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.flush()
        temp_path = Path(handle.name)
    temp_path.replace(path)


if __name__ == "__main__":
    raise SystemExit(main())
