from __future__ import annotations

from typing import Iterable

from .model import SchemaViolation


class CypherValidationError(ValueError):
    pass


class SchemaValidationError(CypherValidationError):
    def __init__(self, violations: Iterable[SchemaViolation]) -> None:
        self.violations = list(violations)
        message = _format_violations(self.violations)
        super().__init__(message)


class CypherCompatibilityError(CypherValidationError):
    def __init__(self, issues: Iterable[str]) -> None:
        self.issues = list(issues)
        message = "Cypher uses constructs not supported by Memgraph:\n" + "\n".join(
            f"- {issue}" for issue in self.issues
        )
        super().__init__(message)


def _format_violations(violations: list[SchemaViolation]) -> str:
    lines = ["Cypher schema validation failed:"]
    for violation in violations:
        if violation.direction == "left_to_right":
            arrow = "->"
        elif violation.direction == "right_to_left":
            arrow = "<-"
        elif violation.direction == "both":
            arrow = "<->"
        else:
            arrow = "-"
        allowed = _format_allowed_pairs(violation.allowed_pairs)
        lines.append(
            "- Invalid relationship: %s %s %s via %s. Allowed: %s. Pattern: %s [rule=%s]"
            % (
                ",".join(violation.left_labels),
                arrow,
                ",".join(violation.right_labels),
                violation.rel_type,
                allowed,
                violation.snippet,
                violation.rule_path,
            )
        )
        lines.append(
            "  Hint: %s is only allowed as %s. Check direction and node labels."
            % (violation.rel_type, allowed)
        )
    return "\n".join(lines)


def _format_allowed_pairs(pairs: tuple[tuple[str, str], ...]) -> str:
    if not pairs:
        return "none"
    return "; ".join(f"{src} -> {dst}" for src, dst in pairs)
