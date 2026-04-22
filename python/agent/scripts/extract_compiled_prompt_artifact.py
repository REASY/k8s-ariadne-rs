from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import cloudpickle


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract readable instructions and demos from a compiled DSPy program."
    )
    parser.add_argument(
        "compiled_program_dir",
        type=Path,
        help="Path to a compiled_program directory containing program.pkl.",
    )
    args = parser.parse_args()

    compiled_dir = args.compiled_program_dir
    program_path = compiled_dir / "program.pkl"
    if not program_path.exists():
        raise SystemExit(f"Missing compiled program: {program_path}")

    with program_path.open("rb") as f:
        program = cloudpickle.load(f)

    predictor = getattr(program, "translate", None)
    if predictor is None:
        raise SystemExit("Compiled program has no `translate` predictor.")

    signature = getattr(predictor, "signature", None)
    instructions = getattr(signature, "instructions", None)
    demos = getattr(predictor, "demos", None) or []

    if not isinstance(instructions, str) or not instructions.strip():
        raise SystemExit("Compiled program did not contain readable instructions.")

    instruction_path = compiled_dir.parent / "compiled_instruction.txt"
    demos_path = compiled_dir.parent / "compiled_demos.json"

    instruction_path.write_text(instructions.strip() + "\n", encoding="utf-8")
    demos_path.write_text(
        json.dumps([_serialize_demo(demo) for demo in demos], indent=2),
        encoding="utf-8",
    )

    print(f"wrote {instruction_path}")
    print(f"wrote {demos_path}")
    print(f"demos={len(demos)}")


def _serialize_demo(demo: Any) -> dict[str, Any]:
    if hasattr(demo, "toDict"):
        return demo.toDict()
    if hasattr(demo, "items"):
        return dict(demo.items())
    if hasattr(demo, "__dict__"):
        return {
            key: value
            for key, value in demo.__dict__.items()
            if not key.startswith("_")
        }
    return {"repr": repr(demo)}


if __name__ == "__main__":
    main()
