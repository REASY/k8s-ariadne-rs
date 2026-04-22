from __future__ import annotations

import argparse
from pathlib import Path

from k8s_graph_agent.agent import GraphMcpClient
from k8s_graph_agent.config import AgentConfig
from k8s_graph_agent.eval.bootstrap import load_dataset_raw, write_dataset_raw
from k8s_graph_agent.eval.expansion import generate_expanded_dataset
from k8s_graph_agent.mcp_client import StreamableHttpMcpClient


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Expand the gold eval dataset using grounded namespace/host variants."
    )
    parser.add_argument(
        "--dataset",
        type=Path,
        default=Path("eval/questions_gold_full.yaml"),
        help="Source gold dataset (default: eval/questions_gold_full.yaml).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("eval/questions_gold_expanded.yaml"),
        help="Expanded output dataset (default: eval/questions_gold_expanded.yaml).",
    )
    parser.add_argument(
        "--target-total",
        type=int,
        default=180,
        help="Target total number of questions including originals (default: 180).",
    )
    parser.add_argument(
        "--namespace-pool-size",
        type=int,
        default=12,
        help="How many rich namespaces to consider as substitution candidates.",
    )
    parser.add_argument(
        "--max-namespace-variants-per-question",
        type=int,
        default=4,
        help="Maximum namespace variants to keep per source question.",
    )
    parser.add_argument(
        "--max-host-variants-per-question",
        type=int,
        default=4,
        help="Maximum host variants to keep per source question.",
    )
    parser.add_argument(
        "--include-empty-variants",
        action="store_true",
        help="Keep variants whose expected result set is empty.",
    )
    args = parser.parse_args()

    raw = load_dataset_raw(args.dataset)
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
        expanded = generate_expanded_dataset(
            raw,
            graph=graph,
            target_total=args.target_total,
            namespace_pool_size=args.namespace_pool_size,
            max_namespace_variants_per_question=args.max_namespace_variants_per_question,
            max_host_variants_per_question=args.max_host_variants_per_question,
            include_empty_variants=args.include_empty_variants,
        )
    finally:
        mcp.close()

    args.output.parent.mkdir(parents=True, exist_ok=True)
    write_dataset_raw(args.output, expanded)
    generated_count = max(0, len(expanded) - _question_count(raw))
    print(
        f"Wrote {len(expanded)} questions ({generated_count} generated) -> {args.output}"
    )


def _question_count(raw: object) -> int:
    if isinstance(raw, list):
        return len(raw)
    if isinstance(raw, dict):
        return sum(len(items) for items in raw.values() if isinstance(items, list))
    return 0


if __name__ == "__main__":
    main()
