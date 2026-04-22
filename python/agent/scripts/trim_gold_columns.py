"""Trim over-specified expected columns in the gold dataset.

For each affected question, reduces expected.columns to only the "core"
columns that directly answer the question, and drops the corresponding
values from expected.rows.
"""
from __future__ import annotations

import sys
from pathlib import Path

import yaml

# Mapping: question base id -> set of core columns to KEEP.
# All variants of the same base id inherit the same core columns.
CORE_COLUMNS: dict[str, list[str]] = {
    "e06": ["endpoint_slice"],
    "e09": ["persistent_volume"],
    "e20": ["container_name", "pod_name"],  # pod_name needed: container names repeat across pods
    "h04": ["service"],
    "h05": ["ingress", "service"],
    "h06": ["pod_name", "service_name"],
    # h07: namespace needed — service names are not unique across namespaces (kept as-is)
    "h19": ["host", "backend_service"],
    # m14: pod needed — container names repeat across pods (kept as-is)
}


def base_id(qid: str) -> str:
    """Extract base question id from a possibly-variant id."""
    for base in CORE_COLUMNS:
        if qid == base or qid.startswith(base + "_"):
            return base
    return ""


def trim_entry(entry: dict) -> bool:
    """Trim expected columns/rows in place. Returns True if modified."""
    qid = entry.get("id", "")
    bid = base_id(qid)
    if not bid:
        return False
    expected = entry.get("expected")
    if not expected:
        return False
    old_columns: list[str] = expected.get("columns", [])
    if not old_columns:
        return False

    core = CORE_COLUMNS[bid]
    # Find which column indices to keep
    keep_indices = []
    for core_col in core:
        for i, col in enumerate(old_columns):
            if col == core_col and i not in keep_indices:
                keep_indices.append(i)
                break

    if sorted(keep_indices) == list(range(len(old_columns))):
        return False  # nothing to trim

    # Update columns
    new_columns = [old_columns[i] for i in keep_indices]
    expected["columns"] = new_columns

    # Update rows
    old_rows = expected.get("rows", [])
    new_rows = []
    for row in old_rows:
        new_rows.append([row[i] for i in keep_indices])
    expected["rows"] = new_rows

    return True


def main() -> None:
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <dataset.yaml> [--output <path>]")
        sys.exit(1)

    dataset_path = Path(sys.argv[1])
    output_path = dataset_path
    if "--output" in sys.argv:
        idx = sys.argv.index("--output")
        output_path = Path(sys.argv[idx + 1])

    with open(dataset_path, encoding="utf-8") as f:
        data = yaml.safe_load(f)

    modified = 0
    # Handle both flat list and difficulty-grouped dict formats
    if isinstance(data, list):
        for entry in data:
            if trim_entry(entry):
                modified += 1
    elif isinstance(data, dict):
        for _difficulty, entries in data.items():
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if trim_entry(entry):
                    modified += 1

    with open(output_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    print(f"Trimmed {modified} entries -> {output_path}")


if __name__ == "__main__":
    main()
