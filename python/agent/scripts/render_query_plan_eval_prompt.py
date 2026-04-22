from __future__ import annotations

import argparse
from pathlib import Path
import sys

import yaml

sys.path.append(str(Path(__file__).resolve().parent))

from run_query_plan_hard_eval import _build_eval_prompt, _load_prompt_sections
from k8s_graph_agent.config import AgentConfig
from k8s_graph_agent.mcp_client import StreamableHttpMcpClient
from k8s_graph_agent.graph_schema import GraphSchema
from k8s_graph_agent.query_plan_prompting import build_distilled_ir_prompt_context


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Render the current QueryPlan eval prompt for a question/example."
    )
    parser.add_argument(
        "--examples",
        type=Path,
        default=Path("eval/query_plan_full_examples.yaml"),
    )
    parser.add_argument(
        "--question-id",
        default="e01",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
    )
    parser.add_argument(
        "--prompt-variant",
        choices=["v6", "distilled-v2"],
        default="v6",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    examples = yaml.safe_load(args.examples.read_text(encoding="utf-8"))
    if not isinstance(examples, list):
        raise ValueError(f"{args.examples} must contain a top-level list")
    match = next((item for item in examples if item.get("id") == args.question_id), None)
    if match is None:
        raise ValueError(f"question id '{args.question_id}' not found in {args.examples}")
    question = str(match["question"])

    cfg = AgentConfig.from_env()
    mcp = StreamableHttpMcpClient(
        base_url=cfg.mcp_url,
        timeout_seconds=cfg.request_timeout_seconds,
        client_name=cfg.client_name,
        client_version=cfg.client_version,
        auth_token=cfg.mcp_auth_token,
    )
    try:
        sections = _load_prompt_sections(mcp, examples)
        distilled_context = build_distilled_ir_prompt_context(
            node_connectivity=sections.node_connectivity,
            schema=GraphSchema.load_default(),
        )
        rendered = _build_eval_prompt(
            question,
            sections.schema_reference,
            sections.node_connectivity,
            distilled_context.render(),
            args.prompt_variant,
        )
    finally:
        mcp.close()

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rendered, encoding="utf-8")
    print(args.output)
    print(len(rendered))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
