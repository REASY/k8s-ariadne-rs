from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from pathlib import Path
from typing import Any

from k8s_graph_agent.agent import GraphMcpClient
from k8s_graph_agent.config import AgentConfig
from k8s_graph_agent.eval.loader import load_dataset
from k8s_graph_agent.eval.matching import MatchType, evaluate_expected_match
from k8s_graph_agent.eval.models import EvalQuestion, ExpectedResult
from k8s_graph_agent.mcp_client import StreamableHttpMcpClient


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Re-evaluate saved report/jsonl artifacts with taxonomy-based matching."
    )
    parser.add_argument(
        "--eval-root",
        type=Path,
        default=PROJECT_ROOT / "eval",
        help="Eval root to scan (default: eval).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=6,
        help="Parallel worker count (default: 6).",
    )
    args = parser.parse_args()

    manifests = sorted(args.eval_root.rglob("manifest.json"))
    tasks: list[tuple[str, Path, Path]] = []
    for manifest in manifests:
        run_dir = manifest.parent
        for report in sorted(run_dir.glob("*/report.json")):
            tasks.append(("report", report, manifest))
        for results_file in sorted(run_dir.glob("results_*.jsonl")):
            tasks.append(("jsonl", results_file, manifest))

    if not tasks:
        raise SystemExit(f"No reevaluable artifacts found under {args.eval_root}")

    summary: Counter[str] = Counter()
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        future_map = {
            executor.submit(_reevaluate_artifact, kind, path, manifest): (kind, path)
            for kind, path, manifest in tasks
        }
        for future in as_completed(future_map):
            kind, path = future_map[future]
            result = future.result()
            summary["artifacts"] += 1
            summary[f"{kind}_artifacts"] += 1
            summary["rows"] += result["rows"]
            summary["matched_rows"] += result["matched_rows"]
            print(
                f"[reeval] {kind} {_display_path(path)} "
                f"rows={result['rows']} matched={result['matched_rows']}"
            )

    print(
        json.dumps(
            {
                "artifacts": summary["artifacts"],
                "report_artifacts": summary["report_artifacts"],
                "jsonl_artifacts": summary["jsonl_artifacts"],
                "rows": summary["rows"],
                "matched_rows": summary["matched_rows"],
            },
            indent=2,
        )
    )


def _reevaluate_artifact(kind: str, path: Path, manifest_path: Path) -> dict[str, int]:
    question_map = _load_question_map(manifest_path)
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
        if kind == "report":
            return _reevaluate_report(path, question_map, graph)
        return _reevaluate_jsonl(path, question_map, graph)
    finally:
        mcp.close()


def _reevaluate_report(
    path: Path, question_map: dict[str, EvalQuestion], graph: GraphMcpClient
) -> dict[str, int]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if _is_query_plan_report(payload):
        return _reevaluate_query_plan_report(path, payload, question_map, graph)
    rows_total = 0
    matched_total = 0
    for section_name in ("baseline", "compiled"):
        section = payload.get(section_name)
        if not isinstance(section, dict):
            continue
        rows_by_question = section.get("rows_by_question")
        if not isinstance(rows_by_question, dict):
            continue
        counts = Counter[str]()
        matched = 0
        exact_column = 0
        projected = 0
        rows_total += len(rows_by_question)
        for question_id, row in rows_by_question.items():
            question = question_map.get(question_id)
            if question is None or question.expected is None or not isinstance(row, dict):
                continue
            _reevaluate_row(row, question.expected, graph)
            match_type = row.get("match_type") or "unknown"
            counts[str(match_type)] += 1
            if row.get("result_match") is True:
                matched += 1
                matched_total += 1
            if match_type == MatchType.EXACT.value:
                exact_column += 1
            elif match_type == MatchType.PROJECTED.value:
                projected += 1
        total = len(rows_by_question)
        valid = sum(1 for row in rows_by_question.values() if row.get("valid"))
        exec_error = sum(
            1 for row in rows_by_question.values() if row.get("match_type") == "execution_error"
        )
        invalid = sum(1 for row in rows_by_question.values() if row.get("match_type") == "invalid")
        section["counts"] = {
            "total": total,
            "valid": valid,
            "exec_error": exec_error,
            "matched": matched,
            "exact_match": matched,
            "invalid": invalid,
            "exact_column_match": exact_column,
            "projected_match": projected,
            "match_type_counts": dict(counts),
            "valid_rate": _ratio(valid, total),
            "exec_error_rate": _ratio(exec_error, total),
            "matched_rate": _ratio(matched, total),
            "exact_match_rate": _ratio(matched, total),
            "exact_column_match_rate": _ratio(exact_column, total),
            "projected_match_rate": _ratio(projected, total),
        }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return {"rows": rows_total, "matched_rows": matched_total}


def _is_query_plan_report(payload: dict[str, Any]) -> bool:
    return (
        isinstance(payload.get("direct"), dict)
        and isinstance(payload.get("ir"), dict)
        and isinstance(payload.get("model"), str)
    )


def _reevaluate_query_plan_report(
    path: Path,
    payload: dict[str, Any],
    question_map: dict[str, EvalQuestion],
    graph: GraphMcpClient,
) -> dict[str, int]:
    rows_total = 0
    matched_total = 0
    for section_name, cypher_key in (("direct", "cypher"), ("ir", "compiled_cypher")):
        section = payload.get(section_name)
        if not isinstance(section, dict):
            continue
        rows_by_question = section.get("results")
        if not isinstance(rows_by_question, dict):
            continue
        counts = Counter[str]()
        matched = 0
        exact = 0
        projected = 0
        valid = 0
        exec_error = 0
        invalid = 0
        rows_total += len(rows_by_question)
        for question_id, row in rows_by_question.items():
            question = question_map.get(question_id)
            if question is None or question.expected is None or not isinstance(row, dict):
                continue
            _reevaluate_report_row(row, question.expected, graph, cypher_key=cypher_key)
            match_type = str(row.get("match_type") or "unknown")
            counts[match_type] += 1
            if row.get("matched") is True:
                matched += 1
                matched_total += 1
            if match_type == MatchType.EXACT.value:
                exact += 1
            elif match_type == MatchType.PROJECTED.value:
                projected += 1
            if row.get("execution_error"):
                exec_error += 1
            elif row.get(cypher_key):
                valid += 1
            else:
                invalid += 1
        total = len(rows_by_question)
        section["summary"] = {
            "matched": matched,
            "total": total,
            "exact": exact,
            "projected": projected,
            "valid": valid,
            "exec_error": exec_error,
            "invalid": invalid,
            "match_type_counts": dict(counts),
            "matched_rate": _ratio(matched, total),
            "exact_rate": _ratio(exact, total),
            "projected_rate": _ratio(projected, total),
        }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return {"rows": rows_total, "matched_rows": matched_total}


def _reevaluate_jsonl(
    path: Path, question_map: dict[str, EvalQuestion], graph: GraphMcpClient
) -> dict[str, int]:
    rows_total = 0
    matched_total = 0
    updated: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        question_id = record.get("question_id")
        question = question_map.get(str(question_id))
        final = record.get("final")
        if question and question.expected and isinstance(final, dict):
            _reevaluate_row(final, question.expected, graph)
            if final.get("result_match") is True:
                matched_total += 1
        rows_total += 1
        updated.append(json.dumps(record, default=str))
    path.write_text("\n".join(updated) + "\n", encoding="utf-8")
    return {"rows": rows_total, "matched_rows": matched_total}


def _reevaluate_row(
    row: dict[str, Any], expected: ExpectedResult, graph: GraphMcpClient
) -> None:
    cypher = row.get("cypher")
    if not isinstance(cypher, str) or not cypher.strip():
        row["result_match"] = False
        row["match_type"] = MatchType.INVALID.value
        row.pop("match_details", None)
        row["execution_error"] = row.get("execution_error")
        return
    row["valid"] = True
    try:
        result = graph.execute_cypher(cypher)
    except Exception as exc:
        row["execution_error"] = str(exc)
        row["result_match"] = False
        row["match_type"] = MatchType.EXECUTION_ERROR.value
        row.pop("match_details", None)
        return
    match_eval = evaluate_expected_match(result, expected)
    row["execution_error"] = None
    row["row_count"] = len(result) if isinstance(result, list) else None
    row["result_match"] = match_eval.matched
    row["match_type"] = match_eval.match_type.value
    row["match_details"] = match_eval.as_dict()


def _reevaluate_report_row(
    row: dict[str, Any],
    expected: ExpectedResult,
    graph: GraphMcpClient,
    *,
    cypher_key: str,
) -> None:
    cypher = row.get(cypher_key)
    if not isinstance(cypher, str) or not cypher.strip():
        row["matched"] = False
        row["match_type"] = MatchType.INVALID.value
        row.pop("match_details", None)
        row["execution_error"] = row.get("execution_error")
        return
    try:
        result = graph.execute_cypher(cypher)
    except Exception as exc:
        row["execution_error"] = str(exc)
        row["matched"] = False
        row["match_type"] = MatchType.EXECUTION_ERROR.value
        row.pop("match_details", None)
        return
    match_eval = evaluate_expected_match(result, expected)
    row["execution_error"] = None
    row["matched"] = match_eval.matched
    row["match_type"] = match_eval.match_type.value
    row["match_details"] = match_eval.as_dict()


def _load_question_map(manifest_path: Path) -> dict[str, EvalQuestion]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    dataset_raw = manifest.get("dataset")
    datasets: list[Path] = []
    if isinstance(dataset_raw, str) and dataset_raw:
        candidate = Path(dataset_raw)
        datasets.append(candidate if candidate.is_absolute() else PROJECT_ROOT / candidate)
    datasets.extend(
        [
            PROJECT_ROOT / "eval/questions_gold_expanded_dense_v2.yaml",
            PROJECT_ROOT / "eval/questions_gold_expanded_dense.yaml",
            PROJECT_ROOT / "eval/questions_gold_full.yaml",
            PROJECT_ROOT / "eval/questions_gold.yaml",
            PROJECT_ROOT / "eval/questions.yaml",
        ]
    )
    for dataset in datasets:
        if dataset.exists():
            questions = load_dataset(dataset)
            return {question.id: question for question in questions}
    raise FileNotFoundError(f"Could not resolve dataset for {manifest_path}")


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _display_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError:
        return str(path)


if __name__ == "__main__":
    main()
