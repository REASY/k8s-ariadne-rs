from __future__ import annotations

import argparse
from pathlib import Path

from k8s_graph_agent.eval.bootstrap import (
    load_dataset_raw,
    sample_grouped_dataset,
    write_dataset_raw,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sample a stratified gold-eval subset from the grouped dataset."
    )
    parser.add_argument(
        "--dataset",
        type=Path,
        default=Path("eval/questions.yaml"),
        help="Source grouped dataset (default: eval/questions.yaml).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("eval/questions_gold.yaml"),
        help="Output subset path (default: eval/questions_gold.yaml).",
    )
    parser.add_argument(
        "--total",
        type=int,
        default=20,
        help="Total questions to sample (default: 20).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=20260324,
        help="Random seed for reproducible sampling (default: 20260324).",
    )
    args = parser.parse_args()

    raw = load_dataset_raw(args.dataset)
    sampled = sample_grouped_dataset(raw, total=args.total, seed=args.seed)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    write_dataset_raw(args.output, sampled)

    print(
        f"Wrote stratified subset -> {args.output} "
        f"(total={args.total}, seed={args.seed})"
    )
    for group, items in sampled.items():
        print(f"[{group}] {len(items)} question(s)")
        for item in items:
            print(f"  - {item.get('id')}: {item.get('question')}")


if __name__ == "__main__":
    main()
