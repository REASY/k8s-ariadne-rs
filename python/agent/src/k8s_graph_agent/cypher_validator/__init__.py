"""Cypher validation utilities for the K8s graph agent.

Validation pipeline:
1) Parse Cypher (ANTLR) with fallback normalization for EXISTS subqueries.
2) Check Memgraph compatibility constraints (unsupported functions/constructs).
3) Visit the parsed AST to extract node/relationship patterns.
4) Resolve labels/variables across patterns.
5) Validate relationship directions and labels against GraphSchema.
6) Raise structured errors with actionable hints.
"""

from .errors import (
    CypherCompatibilityError,
    CypherValidationError,
    SchemaValidationError,
)
from .model import SchemaViolation
from .validator import CypherSchemaValidator

__all__ = [
    "CypherSchemaValidator",
    "CypherValidationError",
    "CypherCompatibilityError",
    "SchemaValidationError",
    "SchemaViolation",
]
