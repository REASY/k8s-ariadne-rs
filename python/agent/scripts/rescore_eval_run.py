"""Re-score an existing eval run against an updated gold dataset.

Reads JSONL result files from a run directory, re-executes each model's
Cypher queries via MCP, and re-evaluates against the (possibly updated)
expected columns/rows from the dataset.

Usage:
    MCP_URL=http://localhost:8080/mcp \
    uv run python scripts/rescore_eval_run.py \
        --dataset eval/questions_gold_expanded_dense_v2.yaml \
        --run-dir eval/runs/20260324_140703 \
        [--output-dir eval/runs/rescored_20260324_140703]
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

from k8s_graph_agent.agent import GraphMcpClient
from k8s_graph_agent.config import AgentConfig
from k8s_graph_agent.eval.bootstrap import collect_questions, load_dataset_raw
from k8s_graph_agent.eval.matching import MatchType, evaluate_expected_match
from k8s_graph_agent.eval.models import ExpectedResult
from k8s_graph_agent.mcp_client import StreamableHttpMcpClient


def main() -> None:
    parser = argparse.ArgumentParser(description="Re-score eval runs against updated gold dataset.")
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--run-dir", type=Path, required=True, action="append",
                        help="Run directory containing results_*.jsonl files (can repeat)")
    parser.add_argument("--output-dir", type=Path, help="Output directory (default: <run-dir>/rescored)")
    args = parser.parse_args()

    raw = load_dataset_raw(args.dataset)
    questions = collect_questions(raw)
    expected_by_id: dict[str, ExpectedResult | None] = {}
    for q in questions:
        expected_by_id[q.id] = q.expected

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
        for run_dir in args.run_dir:
            _rescore_run_dir(run_dir, graph, expected_by_id, args.output_dir)
    finally:
        mcp.close()


def _rescore_run_dir(
    run_dir: Path,
    graph: GraphMcpClient,
    expected_by_id: dict[str, ExpectedResult | None],
    output_dir: Path | None,
) -> None:
    jsonl_files = sorted(run_dir.glob("results_*.jsonl"))
    if not jsonl_files:
        print(f"No results_*.jsonl files in {run_dir}")
        return

    out_dir = output_dir or run_dir / "rescored"
    out_dir.mkdir(parents=True, exist_ok=True)

    for jsonl_path in jsonl_files:
        model_slug = jsonl_path.stem.removeprefix("results_")
        print(f"\n[rescore] {model_slug} from {jsonl_path.name}")

        records = []
        with open(jsonl_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))

        rescored: list[dict[str, Any]] = []
        total = 0
        matched = 0
        exact_col = 0
        projected = 0
        exec_error = 0
        match_type_counts: Counter[str] = Counter()

        for record in records:
            qid = record["question_id"]
            expected = expected_by_id.get(qid)
            final = record.get("final", {})
            cypher = final.get("cypher", "")

            total += 1
            new_final = dict(final)
            for stale_key in (
                "execution_error",
                "validator_error",
                "query_issue_kind",
                "query_issue_source",
            ):
                new_final.pop(stale_key, None)

            if not cypher or not final.get("valid"):
                new_final["result_match"] = False
                new_final["match_type"] = MatchType.INVALID.value
                new_final["match_details"] = None
                match_type_counts[MatchType.INVALID.value] += 1
            elif expected is None or not expected.columns:
                # No expected data for this question, keep original
                mt = final.get("match_type", MatchType.INVALID.value)
                match_type_counts[mt] += 1
                if final.get("result_match"):
                    matched += 1
            else:
                try:
                    result = graph.execute_cypher(cypher)
                    evaluation = evaluate_expected_match(result, expected)
                    new_final["result_match"] = evaluation.matched
                    new_final["match_type"] = evaluation.match_type.value
                    new_final["match_details"] = evaluation.as_dict()
                    new_final["rows"] = len(result) if isinstance(result, list) else None
                    match_type_counts[evaluation.match_type.value] += 1
                    if evaluation.matched:
                        matched += 1
                        if evaluation.match_type is MatchType.EXACT:
                            exact_col += 1
                        elif evaluation.match_type is MatchType.PROJECTED:
                            projected += 1
                except Exception as exc:
                    new_final["result_match"] = False
                    new_final["match_type"] = MatchType.EXECUTION_ERROR.value
                    new_final["match_details"] = {"error": str(exc)}
                    new_final["execution_error"] = str(exc)
                    match_type_counts[MatchType.EXECUTION_ERROR.value] += 1
                    exec_error += 1

            new_record = dict(record)
            new_record["final"] = new_final
            rescored.append(new_record)

        out_path = out_dir / jsonl_path.name
        with open(out_path, "w", encoding="utf-8") as f:
            for rec in rescored:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")

        print(f"  matched={matched}/{total} (exact_col={exact_col}, projected={projected}, exec_error={exec_error})")
        print(f"  match_types: {dict(match_type_counts.most_common())}")
        print(f"  -> {out_path}")

    # Write summary
    summary_path = out_dir / "rescore_summary.txt"
    summary_path.write_text(
        f"Rescored from: {run_dir}\nDataset: see --dataset arg\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
