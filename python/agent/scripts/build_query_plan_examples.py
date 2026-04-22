from __future__ import annotations

import argparse
from pathlib import Path

import yaml

from k8s_graph_agent.eval.loader import load_dataset


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a simple QueryPlan example manifest from an eval dataset."
    )
    parser.add_argument(
        "--dataset",
        type=Path,
        default=Path("eval/questions_gold_expanded_dense_v2.yaml"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("eval/query_plan_full_examples.yaml"),
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    questions = load_dataset(args.dataset)
    payload = [{"id": question.id, "question": question.question} for question in questions]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    print(f"Wrote {len(payload)} example(s) to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
