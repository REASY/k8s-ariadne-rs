from __future__ import annotations

import argparse
from pathlib import Path

from k8s_graph_agent.agent import GraphMcpClient
from k8s_graph_agent.config import AgentConfig
from k8s_graph_agent.eval.bootstrap import (
    execute_reference_query,
    load_dataset_raw,
    select_questions,
    update_question_fields,
    write_dataset_raw,
)
from k8s_graph_agent.mcp_client import StreamableHttpMcpClient


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Materialize expected result rows from approved reference Cypher."
    )
    parser.add_argument(
        "--dataset",
        type=Path,
        default=Path("eval/questions.yaml"),
        help="Dataset path to update (default: eval/questions.yaml).",
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
        "--include-nondeterministic",
        action="store_true",
        help="Include questions without deterministic=true.",
    )
    parser.add_argument(
        "--ordered",
        action="store_true",
        help="Force ordered=true for all updated expected blocks.",
    )
    args = parser.parse_args()

    raw = load_dataset_raw(args.dataset)
    question_ids = _split_csv(args.ids)
    tags = _split_csv(args.tags)
    questions = select_questions(
        raw,
        ids=question_ids,
        tags=tags,
        deterministic_only=not args.include_nondeterministic,
        require_reference=True,
    )
    if not questions:
        raise SystemExit("No questions matched the provided filters.")

    agent_config = AgentConfig.from_env()
    mcp = StreamableHttpMcpClient(
        base_url=agent_config.mcp_url,
        timeout_seconds=agent_config.request_timeout_seconds,
        client_name=agent_config.client_name,
        client_version=agent_config.client_version,
        auth_token=agent_config.mcp_auth_token,
    )
    try:
        graph = GraphMcpClient(mcp=mcp)
        updated = 0
        for question in questions:
            expected = execute_reference_query(
                graph,
                question,
                ordered=True if args.ordered else None,
            )
            changed = update_question_fields(
                raw,
                question.id,
                {
                    "expected": expected.model_dump(),
                },
            )
            if not changed:
                raise RuntimeError(f"Question {question.id} not found in dataset.")
            updated += 1
            print(
                f"[materialize] question={question.id} "
                f"rows={len(expected.rows)} columns={len(expected.columns)}"
            )
    finally:
        mcp.close()

    output_path = args.output or args.dataset
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_dataset_raw(output_path, raw)
    print(f"Wrote {updated} updated question(s) -> {output_path}")


def _split_csv(raw: str | None) -> set[str] | None:
    if raw is None:
        return None
    values = {part.strip() for part in raw.split(",") if part.strip()}
    return values or None


if __name__ == "__main__":
    main()
