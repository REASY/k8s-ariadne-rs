from __future__ import annotations

import argparse
from pathlib import Path

import yaml

from k8s_graph_agent.graph_schema import GraphSchema
from k8s_graph_agent.query_plan import TranslatorOutput
from k8s_graph_agent.query_plan_validator import QueryPlanValidationError, validate_translator_output


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate handwritten QueryPlan V1 examples against the semantic plan validator."
    )
    parser.add_argument(
        "--examples",
        default="eval/query_plan_v1_examples.yaml",
        help="Path to the handwritten TranslatorOutput examples.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    examples_path = Path(args.examples)
    payload = yaml.safe_load(examples_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"{examples_path} must contain a top-level list")

    schema = GraphSchema.load_default()
    success = 0
    for item in payload:
        if not isinstance(item, dict):
            raise ValueError("each example must be a mapping")
        example_id = item.get("id", "<unknown>")
        output = TranslatorOutput.model_validate(item["output"])
        try:
            validate_translator_output(output, schema=schema)
        except QueryPlanValidationError as exc:
            print(f"[FAIL] {example_id}")
            for issue in exc.issues:
                print(f"  - {issue.path}: {issue.code}: {issue.message}")
            return 1
        print(f"[OK] {example_id}")
        success += 1

    print(f"Validated {success} example(s) from {examples_path}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
