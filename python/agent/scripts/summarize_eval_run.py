from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Summarize eval run folder as a markdown table."
    )
    parser.add_argument(
        "--run-dir",
        type=Path,
        required=True,
        help="Run folder containing results_*.jsonl files.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional output markdown file (defaults to stdout).",
    )
    args = parser.parse_args()

    run_dir = args.run_dir
    if not run_dir.exists() or not run_dir.is_dir():
        raise SystemExit(f"Run dir not found: {run_dir}")

    results_files = sorted(run_dir.glob("results_*.jsonl"))
    if not results_files:
        raise SystemExit(f"No results_*.jsonl files found in {run_dir}")

    manifest = _read_manifest(run_dir / "manifest.json")
    summaries = []
    for path in results_files:
        records = list(_load_records(path))
        if not records:
            continue
        summaries.append(_summarize_records(path, records))

    if not summaries:
        raise SystemExit("No records found in results files.")

    summaries.sort(
        key=lambda row: (
            -(row["valid_rate"] or 0.0),
            row["exec_error_rate"] or 0.0,
            row["avg_latency_ms"] or 0.0,
        )
    )

    markdown = _render_markdown(run_dir, manifest, summaries)
    if args.output:
        args.output.write_text(markdown, encoding="utf-8")
    else:
        print(markdown)


def _read_manifest(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def _load_records(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def _summarize_records(path: Path, records: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(records)
    model = _first(records, "model") or path.stem.replace("results_", "")
    question_ids = {rec.get("question_id") for rec in records if rec.get("question_id")}
    valid = sum(1 for rec in records if _truthy(rec, "final", "valid"))
    exec_errors = sum(
        1 for rec in records if rec.get("final", {}).get("execution_error")
    )
    retry = sum(1 for rec in records if len(rec.get("attempts", [])) > 1)
    retry_success = sum(
        1
        for rec in records
        if len(rec.get("attempts", [])) > 1 and _truthy(rec, "final", "valid")
    )

    return {
        "model": model,
        "records": total,
        "unique_questions": len(question_ids),
        "valid_rate": _ratio(valid, total),
        "exec_error_rate": _ratio(exec_errors, total),
        "retry_rate": _ratio(retry, total),
        "retry_success_rate": _ratio(retry_success, retry) if retry else 0.0,
        "avg_attempts": _avg_metric(records, "attempts"),
        "avg_latency_ms": _avg_metric(records, "latency_ms"),
        "avg_total_tokens": _avg_metric(records, "total_tokens"),
        "avg_prompt_tokens": _avg_metric(records, "total_prompt_tokens"),
        "avg_output_tokens": _avg_metric(records, "total_output_tokens"),
    }


def _avg_metric(records: list[dict[str, Any]], key: str) -> float | None:
    values = []
    for rec in records:
        metrics = rec.get("metrics") or {}
        value = metrics.get(key)
        if isinstance(value, (int, float)):
            values.append(float(value))
    if not values:
        return None
    return sum(values) / len(values)


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _truthy(rec: dict[str, Any], *path: str) -> bool:
    current: Any = rec
    for key in path:
        if not isinstance(current, dict):
            return False
        current = current.get(key)
    return bool(current)


def _first(records: list[dict[str, Any]], key: str) -> str | None:
    for rec in records:
        value = rec.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _render_markdown(
    run_dir: Path, manifest: dict[str, Any] | None, summaries: list[dict[str, Any]]
) -> str:
    lines: list[str] = []
    lines.append(f"# Eval Summary ({run_dir.name})")
    if manifest:
        lines.append("")
        lines.append("## Run metadata")
        lines.append("")
        lines.append(f"- Dataset: {manifest.get('dataset')}")
        lines.append(f"- Mode: {manifest.get('mode')}")
        lines.append(f"- Runs: {manifest.get('runs')}")
        lines.append(f"- Models: {len(manifest.get('models', []))}")
        if manifest.get("parallelism"):
            lines.append(f"- Parallelism: {manifest.get('parallelism')}")
    lines.append("")
    lines.append("## Model summary")
    lines.append("")
    headers = [
        "Model",
        "Records",
        "Questions",
        "Valid %",
        "Exec Error %",
        "Retry %",
        "Retry Success %",
        "Avg Attempts",
        "Avg Latency (ms)",
        "Avg Tokens",
        "Avg Prompt",
        "Avg Output",
    ]
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join("---" for _ in headers) + " |")
    for row in summaries:
        lines.append(
            "| "
            + " | ".join(
                [
                    row["model"],
                    str(row["records"]),
                    str(row["unique_questions"]),
                    _pct(row["valid_rate"]),
                    _pct(row["exec_error_rate"]),
                    _pct(row["retry_rate"]),
                    _pct(row["retry_success_rate"]),
                    _fmt_float(row["avg_attempts"]),
                    _fmt_float(row["avg_latency_ms"]),
                    _fmt_float(row["avg_total_tokens"]),
                    _fmt_float(row["avg_prompt_tokens"]),
                    _fmt_float(row["avg_output_tokens"]),
                ]
            )
            + " |"
        )
    return "\n".join(lines)


def _pct(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value * 100:.1f}%"


def _fmt_float(value: float | None) -> str:
    if value is None:
        return "-"
    if value >= 1000:
        return f"{value:.0f}"
    return f"{value:.2f}"


if __name__ == "__main__":
    main()
