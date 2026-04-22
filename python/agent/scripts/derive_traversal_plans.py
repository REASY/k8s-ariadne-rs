from __future__ import annotations

import argparse
from pathlib import Path

from k8s_graph_agent.agent import GraphMcpClient
from k8s_graph_agent.config import AgentConfig
from k8s_graph_agent.eval.bootstrap import (
    load_dataset_raw,
    select_questions,
    update_question_fields,
    write_dataset_raw,
)
from k8s_graph_agent.eval.traversal_plan import derive_traversal_plan
from k8s_graph_agent.mcp_client import StreamableHttpMcpClient


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Derive traversal_plan labels from reference Cypher and validate them against MCP."
    )
    parser.add_argument(
        "--dataset",
        type=Path,
        default=Path("eval/questions_gold_expanded_dense_v2.yaml"),
        help="Dataset path to read (default: eval/questions_gold_expanded_dense_v2.yaml).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional output path. Defaults to overwriting --dataset.",
    )
    parser.add_argument(
        "--ids",
        help="Comma-separated question ids to include.",
    )
    parser.add_argument(
        "--tags",
        help="Comma-separated tags to include.",
    )
    parser.add_argument(
        "--skip-mcp-validation",
        action="store_true",
        help="Skip executing the reference Cypher via MCP before writing traversal plans.",
    )
    args = parser.parse_args()

    raw = load_dataset_raw(args.dataset)
    question_ids = _split_csv(args.ids)
    tags = _split_csv(args.tags)
    questions = select_questions(
        raw,
        ids=question_ids,
        tags=tags,
        require_reference=True,
    )
    if not questions:
        raise SystemExit("No questions matched the provided filters.")

    mcp: StreamableHttpMcpClient | None = None
    graph: GraphMcpClient | None = None
    if not args.skip_mcp_validation:
        agent_config = AgentConfig.from_env()
        mcp = StreamableHttpMcpClient(
            base_url=agent_config.mcp_url,
            timeout_seconds=agent_config.request_timeout_seconds,
            client_name=agent_config.client_name,
            client_version=agent_config.client_version,
            auth_token=agent_config.mcp_auth_token,
        )
        graph = GraphMcpClient(mcp=mcp)

    try:
        updated = 0
        for question in questions:
            reference_cypher = question.reference_cypher
            if not reference_cypher:
                continue
            if graph is not None:
                graph.execute_cypher(reference_cypher)
            traversal_plan = derive_traversal_plan(reference_cypher)
            changed = update_question_fields(
                raw,
                question.id,
                {
                    "traversal_plan": traversal_plan,
                },
            )
            if not changed:
                raise RuntimeError(f"Question {question.id} not found in dataset.")
            updated += 1
            print(
                f"[traversal] question={question.id} lines={len(traversal_plan.splitlines())}"
            )
    finally:
        if mcp is not None:
            mcp.close()

    output_path = args.output or args.dataset
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_dataset_raw(output_path, raw)
    print(f"Wrote {updated} traversal plan(s) -> {output_path}")


def _split_csv(raw: str | None) -> set[str] | None:
    if raw is None:
        return None
    values = {part.strip() for part in raw.split(",") if part.strip()}
    return values or None


if __name__ == "__main__":
    main()
