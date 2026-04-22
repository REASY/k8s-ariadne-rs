from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import json
import re
from typing import Any, Iterable

from ..agent import GraphMcpClient
from .bootstrap import build_expected_result, collect_questions
from .models import EvalQuestion, ExpectedResult


_NAMESPACE_PATTERN = re.compile(r"\bnamespace ([a-z0-9][a-z0-9.-]*)\b", re.IGNORECASE)
_HOST_PATTERN = re.compile(r"\bhost ([a-z0-9][a-z0-9.-]*)\b", re.IGNORECASE)


@dataclass(frozen=True)
class GeneratedVariant:
    source_question_id: str
    question: EvalQuestion


def generate_expanded_dataset(
    raw: list[dict[str, Any]] | dict[str, list[dict[str, Any]]],
    *,
    graph: GraphMcpClient,
    target_total: int,
    namespace_pool_size: int = 12,
    max_namespace_variants_per_question: int = 4,
    max_host_variants_per_question: int = 4,
    include_empty_variants: bool = False,
) -> list[dict[str, Any]]:
    base_questions = collect_questions(raw)
    if target_total <= len(base_questions):
        return [question.model_dump(exclude_none=True) for question in base_questions]

    namespace_pool = list_top_namespaces(graph, limit=namespace_pool_size)
    host_pool = list_hosts(graph)
    source_fingerprints = {
        question.id: _expected_fingerprint(question.expected) for question in base_questions
    }

    candidates_by_source: dict[str, list[EvalQuestion]] = defaultdict(list)
    seen_ids = {question.id for question in base_questions}
    seen_question_keys = {
        (question.question.strip(), (question.reference_cypher or "").strip())
        for question in base_questions
    }

    for question in base_questions:
        for variant in iter_namespace_variants(
            question,
            graph=graph,
            namespace_pool=namespace_pool,
            max_variants=max_namespace_variants_per_question,
            include_empty_variants=include_empty_variants,
            source_fingerprint=source_fingerprints.get(question.id),
        ):
            key = (variant.question.strip(), (variant.reference_cypher or "").strip())
            if variant.id in seen_ids or key in seen_question_keys:
                continue
            seen_ids.add(variant.id)
            seen_question_keys.add(key)
            candidates_by_source[question.id].append(variant)

        for variant in iter_host_variants(
            question,
            graph=graph,
            host_pool=host_pool,
            max_variants=max_host_variants_per_question,
            include_empty_variants=include_empty_variants,
            source_fingerprint=source_fingerprints.get(question.id),
        ):
            key = (variant.question.strip(), (variant.reference_cypher or "").strip())
            if variant.id in seen_ids or key in seen_question_keys:
                continue
            seen_ids.add(variant.id)
            seen_question_keys.add(key)
            candidates_by_source[question.id].append(variant)

    additions_needed = target_total - len(base_questions)
    selected = round_robin_variants(candidates_by_source, limit=additions_needed)
    output_questions = base_questions + selected
    return [question.model_dump(exclude_none=True) for question in output_questions]


def list_top_namespaces(graph: GraphMcpClient, *, limit: int = 12) -> list[str]:
    result = graph.execute_cypher(
        """
        MATCH (ns:Namespace)
        OPTIONAL MATCH (ns)<-[:BelongsTo]-(r)
        WITH ns, count(r) AS resources
        WHERE resources > 0
        RETURN ns['metadata']['name'] AS namespace, resources
        ORDER BY resources DESC, namespace
        LIMIT $limit
        """.replace("$limit", str(limit))
    )
    if not isinstance(result, list):
        return []
    namespaces = [row.get("namespace") for row in result if isinstance(row, dict)]
    return [value for value in namespaces if isinstance(value, str) and value]


def list_hosts(graph: GraphMcpClient) -> list[str]:
    result = graph.execute_cypher(
        """
        MATCH (h:Host)
        RETURN h['name'] AS host
        ORDER BY host
        """
    )
    if not isinstance(result, list):
        return []
    hosts = [row.get("host") for row in result if isinstance(row, dict)]
    return [value for value in hosts if isinstance(value, str) and value]


def iter_namespace_variants(
    question: EvalQuestion,
    *,
    graph: GraphMcpClient,
    namespace_pool: Iterable[str],
    max_variants: int,
    include_empty_variants: bool,
    source_fingerprint: tuple[tuple[str, Any], ...] | None,
) -> Iterable[EvalQuestion]:
    if not question.reference_cypher:
        return []
    source_namespace = extract_namespace(question.question, question.reference_cypher)
    if source_namespace is None:
        return []

    generated: list[EvalQuestion] = []
    for namespace in namespace_pool:
        if namespace == source_namespace:
            continue
        question_text = replace_namespace_text(question.question, source_namespace, namespace)
        reference_cypher = replace_quoted_literal(
            question.reference_cypher,
            source_namespace,
            namespace,
        )
        expected = execute_expected_from_reference(
            graph,
            reference_cypher,
            existing_expected=question.expected,
        )
        if not include_empty_variants and not expected.rows:
            continue
        if source_fingerprint is not None and _expected_fingerprint(expected) == source_fingerprint:
            continue
        generated.append(
            build_generated_question(
                source=question,
                variant_key=namespace,
                question_text=question_text,
                reference_cypher=reference_cypher,
                expected=expected,
                family="namespace_variant",
                parameters={"namespace": namespace},
            )
        )
        if len(generated) >= max_variants:
            break
    return generated


def iter_host_variants(
    question: EvalQuestion,
    *,
    graph: GraphMcpClient,
    host_pool: Iterable[str],
    max_variants: int,
    include_empty_variants: bool,
    source_fingerprint: tuple[tuple[str, Any], ...] | None,
) -> Iterable[EvalQuestion]:
    if not question.reference_cypher:
        return []
    source_host = extract_host(question.question, question.reference_cypher)
    if source_host is None:
        return []

    generated: list[EvalQuestion] = []
    for host in host_pool:
        if host == source_host:
            continue
        question_text = question.question.replace(source_host, host)
        reference_cypher = replace_quoted_literal(question.reference_cypher, source_host, host)
        expected = execute_expected_from_reference(
            graph,
            reference_cypher,
            existing_expected=question.expected,
        )
        if not include_empty_variants and not expected.rows:
            continue
        if source_fingerprint is not None and _expected_fingerprint(expected) == source_fingerprint:
            continue
        generated.append(
            build_generated_question(
                source=question,
                variant_key=host,
                question_text=question_text,
                reference_cypher=reference_cypher,
                expected=expected,
                family="host_variant",
                parameters={"host": host},
            )
        )
        if len(generated) >= max_variants:
            break
    return generated


def round_robin_variants(
    variants_by_source: dict[str, list[EvalQuestion]], *, limit: int
) -> list[EvalQuestion]:
    selected: list[EvalQuestion] = []
    queues = {source: list(variants) for source, variants in variants_by_source.items()}
    source_ids = sorted(queues)
    while len(selected) < limit:
        progressed = False
        for source_id in source_ids:
            queue = queues[source_id]
            if not queue:
                continue
            selected.append(queue.pop(0))
            progressed = True
            if len(selected) >= limit:
                break
        if not progressed:
            break
    return selected


def build_generated_question(
    *,
    source: EvalQuestion,
    variant_key: str,
    question_text: str,
    reference_cypher: str,
    expected: ExpectedResult,
    family: str,
    parameters: dict[str, Any],
) -> EvalQuestion:
    slug = slugify(variant_key)
    return EvalQuestion(
        id=f"{source.id}_{family}_{slug}",
        question=question_text,
        tags=list(source.tags) + ["generated", family],
        expected=expected,
        deterministic=True,
        reference_cypher=reference_cypher,
        family=family,
        group_id=source.id,
        source_question_id=source.id,
        generation_type=family,
        parameters=parameters,
    )


def execute_expected_from_reference(
    graph: GraphMcpClient,
    reference_cypher: str,
    *,
    existing_expected: ExpectedResult | None,
) -> ExpectedResult:
    result = graph.execute_cypher(reference_cypher)
    return build_expected_result(
        result,
        columns=existing_expected.columns if existing_expected is not None else None,
        ordered=existing_expected.ordered if existing_expected is not None else False,
    )


def extract_namespace(question_text: str, reference_cypher: str) -> str | None:
    question_match = _NAMESPACE_PATTERN.search(question_text)
    if question_match is None:
        return None
    namespace = question_match.group(1)
    if f"'{namespace}'" not in reference_cypher:
        return None
    return namespace


def extract_host(question_text: str, reference_cypher: str) -> str | None:
    question_match = _HOST_PATTERN.search(question_text)
    if question_match is None:
        return None
    host = question_match.group(1)
    if f"'{host}'" not in reference_cypher:
        return None
    return host


def replace_namespace_text(question_text: str, source_namespace: str, target_namespace: str) -> str:
    return re.sub(
        rf"\bnamespace {re.escape(source_namespace)}\b",
        f"namespace {target_namespace}",
        question_text,
        count=1,
        flags=re.IGNORECASE,
    )


def replace_quoted_literal(text: str, source_value: str, target_value: str) -> str:
    return text.replace(f"'{source_value}'", f"'{target_value}'")


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def _expected_fingerprint(expected: ExpectedResult | None) -> tuple[tuple[str, Any], ...] | None:
    if expected is None:
        return None
    columns = tuple(expected.columns)
    normalized_rows = [tuple(_canonical_cell(value) for value in row) for row in expected.rows]
    if not expected.ordered:
        normalized_rows = sorted(
            normalized_rows,
            key=lambda row: json.dumps(row, sort_keys=True, separators=(",", ":")),
        )
    return (
        ("columns", columns),
        ("rows", tuple(normalized_rows)),
        ("ordered", expected.ordered),
    )


def _canonical_cell(value: Any) -> Any:
    if isinstance(value, list):
        return tuple(_canonical_cell(item) for item in value)
    if isinstance(value, dict):
        return tuple(
            sorted((str(key), _canonical_cell(inner)) for key, inner in value.items())
        )
    return value
