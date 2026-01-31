from __future__ import annotations

import argparse
import logging
from pathlib import Path

from .runner import run_evaluation


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate NL → Cypher translation.")
    parser.add_argument(
        "--dataset",
        required=True,
        type=Path,
        help="Path to dataset YAML/JSON file.",
    )
    parser.add_argument(
        "--mode",
        choices=("single-shot", "retry"),
        default="retry",
        help="Evaluation mode.",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=1,
        help="Number of repetitions per question.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write JSONL records to this path (defaults to stdout).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level for the evaluator.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level.upper())
    run_evaluation(
        dataset_path=args.dataset,
        mode=args.mode,
        runs=args.runs,
        output_path=args.output,
    )


if __name__ == "__main__":
    main()
