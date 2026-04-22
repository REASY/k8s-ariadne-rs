from __future__ import annotations

import argparse
import csv
import io
import json
from pathlib import Path
import subprocess

import yaml

from k8s_graph_agent.agent import GraphMcpClient
from k8s_graph_agent.config import AgentConfig
from k8s_graph_agent.eval.loader import load_dataset
from k8s_graph_agent.eval.matching import evaluate_expected_match
from k8s_graph_agent.graph_schema import GraphSchema
from k8s_graph_agent.mcp_client import StreamableHttpMcpClient
from k8s_graph_agent.query_plan import TranslatorOutput
from k8s_graph_agent.query_plan_compiler import compile_query_plan


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compile handwritten QueryPlan examples, execute them, and compare against gold."
    )
    parser.add_argument(
        "--examples",
        type=Path,
        default=Path("eval/query_plan_v1_examples.yaml"),
    )
    parser.add_argument(
        "--dataset",
        type=Path,
        default=Path("eval/questions_gold_expanded_dense_v2.yaml"),
    )
    parser.add_argument(
        "--execution-backend",
        choices=["auto", "mcp", "mgconsole"],
        default="auto",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    examples = yaml.safe_load(args.examples.read_text(encoding="utf-8"))
    if not isinstance(examples, list):
        raise ValueError(f"{args.examples} must contain a top-level list")

    questions = {question.id: question for question in load_dataset(args.dataset)}
    schema = GraphSchema.load_default()
    graph = _build_executor(args.execution_backend)
    ok = 0
    try:
        for item in examples:
            if not isinstance(item, dict):
                raise ValueError("example entry must be a mapping")
            example_id = str(item["id"])
            output = TranslatorOutput.model_validate(item["output"])
            if output.plan is None:
                print(f"[SKIP] {example_id}")
                continue
            compiled = compile_query_plan(output.plan, schema=schema)
            try:
                result = graph.execute(compiled.cypher)
            except Exception as exc:
                print(f"[ERROR] {example_id}")
                print(compiled.cypher)
                print(str(exc))
                continue
            question = questions.get(example_id)
            if question is None or question.expected is None:
                status = "NO_GOLD"
                details = None
            else:
                match_eval = evaluate_expected_match(result, question.expected)
                status = "OK" if match_eval.matched else "FAIL"
                details = match_eval.as_dict()
                if match_eval.matched:
                    ok += 1
            print(f"[{status}] {example_id}")
            print(compiled.cypher)
            if details is not None:
                print(json.dumps(details, indent=2, sort_keys=True))
    finally:
        graph.close()

    if ok:
        print(f"Matched {ok} gold-backed example(s).")
    return 0

class _Executor:
    def execute(self, query: str):
        raise NotImplementedError

    def close(self) -> None:
        return None


class _McpExecutor(_Executor):
    def __init__(self) -> None:
        agent_config = AgentConfig.from_env()
        self._mcp = StreamableHttpMcpClient(
            base_url=agent_config.mcp_url,
            timeout_seconds=agent_config.request_timeout_seconds,
            client_name=agent_config.client_name,
            client_version=agent_config.client_version,
            auth_token=agent_config.mcp_auth_token,
        )
        self._graph = GraphMcpClient(mcp=self._mcp)

    def execute(self, query: str):
        return self._graph.execute_cypher(query)

    def close(self) -> None:
        self._mcp.close()


class _MgconsoleExecutor(_Executor):
    def execute(self, query: str):
        completed = subprocess.run(
            [
                "mgconsole",
                "--host",
                "127.0.0.1",
                "--port",
                "7687",
                "--output_format=csv",
            ],
            input=_terminate_query(query),
            text=True,
            capture_output=True,
            check=True,
        )
        return _parse_mgconsole_csv(completed.stdout)


def _build_executor(mode: str) -> _Executor:
    if mode == "mgconsole":
        return _MgconsoleExecutor()
    if mode == "mcp":
        return _McpExecutor()
    try:
        return _McpExecutor()
    except Exception:
        return _MgconsoleExecutor()


def _parse_mgconsole_csv(payload: str) -> list[dict[str, object]]:
    rows = list(csv.reader(io.StringIO(payload)))
    if not rows:
        return []
    headers = rows[0]
    result: list[dict[str, object]] = []
    for row in rows[1:]:
        record: dict[str, object] = {}
        for index, header in enumerate(headers):
            value = row[index] if index < len(row) else ""
            record[header] = _decode_mgconsole_value(value)
        result.append(record)
    return result


def _decode_mgconsole_value(value: str) -> object:
    if value == "":
        return ""
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _terminate_query(query: str) -> str:
    stripped = query.rstrip()
    if not stripped.endswith(";"):
        stripped += ";"
    return stripped + "\n"


if __name__ == "__main__":
    raise SystemExit(main())
