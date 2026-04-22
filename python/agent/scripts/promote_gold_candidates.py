from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from k8s_graph_agent.eval.bootstrap import (
    choose_consensus_reference,
    load_dataset_raw,
    update_question_fields,
    write_dataset_raw,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Promote safe consensus candidate Cypher into reference_cypher."
    )
    parser.add_argument(
        "--dataset",
        type=Path,
        default=Path("eval/questions_gold.yaml"),
        help="Dataset path to update (default: eval/questions_gold.yaml).",
    )
    parser.add_argument(
        "--candidates",
        type=Path,
        default=Path("eval/gold_candidates.json"),
        help="Candidate JSON path (default: eval/gold_candidates.json).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional output dataset path. Defaults to overwriting --dataset.",
    )
    parser.add_argument(
        "--min-models",
        type=int,
        default=2,
        help="Minimum agreeing models required for promotion (default: 2).",
    )
    args = parser.parse_args()

    raw = load_dataset_raw(args.dataset)
    candidates = json.loads(args.candidates.read_text(encoding="utf-8"))
    if not isinstance(candidates, list):
        raise SystemExit("Candidate file must contain a JSON list.")

    promoted = 0
    skipped = 0
    for item in candidates:
        if not isinstance(item, dict):
            continue
        question_id = item.get("id")
        candidate_runs = item.get("candidates")
        if not isinstance(question_id, str) or not isinstance(candidate_runs, list):
            skipped += 1
            continue
        cypher = choose_consensus_reference(candidate_runs, min_models=args.min_models)
        if not cypher:
            skipped += 1
            print(f"[skip] question={question_id} no safe consensus")
            continue
        changed = update_question_fields(
            raw,
            question_id,
            {"reference_cypher": cypher, "deterministic": True},
        )
        if not changed:
            raise RuntimeError(f"Question {question_id} not found in dataset.")
        promoted += 1
        print(f"[promote] question={question_id}")

    output_path = args.output or args.dataset
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_dataset_raw(output_path, raw)
    print(f"Wrote dataset -> {output_path} (promoted={promoted}, skipped={skipped})")


if __name__ == "__main__":
    main()
