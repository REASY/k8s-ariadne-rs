from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from enum import StrEnum
from itertools import permutations
from typing import Any, Iterable, Mapping, cast

from ..models import JsonValue
from .models import ExpectedResult

class MatchType(StrEnum):
    EXACT = "exact"
    PROJECTED = "projected"
    ORDERING_MISMATCH = "ordering_mismatch"
    EMPTY_RESULT = "empty_result_missing_all_rows"
    INSUFFICIENT_COLUMNS = "insufficient_columns_to_reconstruct"
    EXTRA_ROWS = "extra_rows_overinclusive"
    MISSING_ROWS = "missing_rows_partial"
    EXTRA_AND_MISSING = "extra_and_missing_rows"
    GROUPED = "grouped_or_aggregated_shape"
    WRONG_SEMANTICS = "wrong_filter_or_relation"
    NON_TABULAR = "non_tabular_result"
    INVALID = "invalid"
    EXECUTION_ERROR = "execution_error"


@dataclass(frozen=True)
class MatchEvaluation:
    matched: bool
    match_type: MatchType
    expected_count: int
    result_count: int | None
    best_overlap: int | None = None
    best_projection: list[str] | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "matched": self.matched,
            "match_type": self.match_type.value,
            "expected_count": self.expected_count,
            "result_count": self.result_count,
            "best_overlap": self.best_overlap,
            "best_projection": self.best_projection,
        }


def evaluate_expected_match(
    result: JsonValue | Any, expected: ExpectedResult
) -> MatchEvaluation:
    expected_rows = [tuple(_freeze_value(value) for value in row) for row in expected.rows]
    expected_count = len(expected_rows)
    if not isinstance(result, list):
        return MatchEvaluation(
            matched=False,
            match_type=MatchType.NON_TABULAR,
            expected_count=expected_count,
            result_count=None,
        )
    if not all(isinstance(row, Mapping) for row in result):
        return MatchEvaluation(
            matched=False,
            match_type=MatchType.NON_TABULAR,
            expected_count=expected_count,
            result_count=len(result),
        )

    rows = cast(list[Mapping[str, Any]], result)
    result_count = len(rows)
    direct_rows = _normalize_rows(rows, expected.columns)
    if direct_rows is not None:
        if _rows_match(direct_rows, expected_rows, ordered=expected.ordered):
            return MatchEvaluation(
                matched=True,
                match_type=MatchType.EXACT,
                expected_count=expected_count,
                result_count=result_count,
                best_overlap=_overlap_size(direct_rows, expected_rows),
                best_projection=list(expected.columns),
            )
        if expected.ordered and _multiset_equal(direct_rows, expected_rows):
            return MatchEvaluation(
                matched=False,
                match_type=MatchType.ORDERING_MISMATCH,
                expected_count=expected_count,
                result_count=result_count,
                best_overlap=expected_count,
                best_projection=list(expected.columns),
            )

    projection_rows = _normalize_projected_rows(rows, width=len(expected.columns))
    if projection_rows:
        for projection, candidate in projection_rows:
            if _rows_match(candidate, expected_rows, ordered=expected.ordered):
                return MatchEvaluation(
                    matched=True,
                    match_type=MatchType.PROJECTED,
                    expected_count=expected_count,
                    result_count=result_count,
                    best_overlap=_overlap_size(candidate, expected_rows),
                    best_projection=list(projection),
                )
            if expected.ordered and _multiset_equal(candidate, expected_rows):
                return MatchEvaluation(
                    matched=False,
                    match_type=MatchType.ORDERING_MISMATCH,
                    expected_count=expected_count,
                    result_count=result_count,
                    best_overlap=expected_count,
                    best_projection=list(projection),
                )

    association_match = _match_association_equivalent(
        rows=rows,
        direct_rows=direct_rows,
        projection_rows=projection_rows,
        expected=expected,
        expected_count=expected_count,
        result_count=result_count,
    )
    if association_match is not None:
        return association_match

    zero_count_match = _match_allow_extra_zero_rows(
        direct_rows=direct_rows,
        projection_rows=projection_rows,
        expected=expected,
        expected_rows=expected_rows,
        expected_count=expected_count,
        result_count=result_count,
    )
    if zero_count_match is not None:
        return zero_count_match

    entity_name_match = _match_allow_full_entity_name_projection(
        rows=rows,
        expected=expected,
        expected_rows=expected_rows,
        expected_count=expected_count,
        result_count=result_count,
    )
    if entity_name_match is not None:
        return entity_name_match

    if result_count == 0 and expected_count > 0:
        return MatchEvaluation(
            matched=False,
            match_type=MatchType.EMPTY_RESULT,
            expected_count=expected_count,
            result_count=result_count,
            best_overlap=0,
        )

    if projection_rows is None:
        return MatchEvaluation(
            matched=False,
            match_type=MatchType.INSUFFICIENT_COLUMNS,
            expected_count=expected_count,
            result_count=result_count,
        )

    best_projection, best_candidate, best_overlap = _best_projection(
        projection_rows, expected_rows
    )
    nested_values = _has_nested_values(rows)

    if best_overlap == expected_count and result_count > expected_count:
        return MatchEvaluation(
            matched=False,
            match_type=MatchType.EXTRA_ROWS,
            expected_count=expected_count,
            result_count=result_count,
            best_overlap=best_overlap,
            best_projection=list(best_projection) if best_projection else None,
        )

    if nested_values and _looks_grouped_shape(
        best_candidate, expected_rows, result_count, expected_count
    ):
        return MatchEvaluation(
            matched=False,
            match_type=MatchType.GROUPED,
            expected_count=expected_count,
            result_count=result_count,
            best_overlap=best_overlap,
            best_projection=list(best_projection) if best_projection else None,
        )

    if 0 < best_overlap < expected_count and result_count < expected_count:
        return MatchEvaluation(
            matched=False,
            match_type=MatchType.MISSING_ROWS,
            expected_count=expected_count,
            result_count=result_count,
            best_overlap=best_overlap,
            best_projection=list(best_projection) if best_projection else None,
        )

    if 0 < best_overlap and result_count > expected_count:
        return MatchEvaluation(
            matched=False,
            match_type=MatchType.EXTRA_AND_MISSING,
            expected_count=expected_count,
            result_count=result_count,
            best_overlap=best_overlap,
            best_projection=list(best_projection) if best_projection else None,
        )

    if 0 < best_overlap:
        return MatchEvaluation(
            matched=False,
            match_type=MatchType.MISSING_ROWS,
            expected_count=expected_count,
            result_count=result_count,
            best_overlap=best_overlap,
            best_projection=list(best_projection) if best_projection else None,
        )

    if nested_values:
        return MatchEvaluation(
            matched=False,
            match_type=MatchType.GROUPED,
            expected_count=expected_count,
            result_count=result_count,
            best_overlap=best_overlap,
            best_projection=list(best_projection) if best_projection else None,
        )

    return MatchEvaluation(
        matched=False,
        match_type=MatchType.WRONG_SEMANTICS,
        expected_count=expected_count,
        result_count=result_count,
        best_overlap=best_overlap,
        best_projection=list(best_projection) if best_projection else None,
    )


def match_expected(result: JsonValue | Any, expected: ExpectedResult) -> bool:
    return evaluate_expected_match(result, expected).matched


def _normalize_rows(
    rows: Iterable[Mapping[str, Any]], columns: list[str]
) -> list[tuple[Any, ...]] | None:
    normalized: list[tuple[Any, ...]] = []
    for row in rows:
        if any(column not in row for column in columns):
            return None
        normalized.append(tuple(_freeze_value(row[column]) for column in columns))
    return normalized


def _normalize_projected_rows(
    rows: Iterable[Mapping[str, Any]], width: int
) -> list[tuple[tuple[str, ...], list[tuple[Any, ...]]]] | None:
    if width <= 0:
        return None
    row_list = list(rows)
    if not row_list:
        return [(tuple(), [])]
    first_columns = list(row_list[0].keys())
    shared_columns = [col for col in first_columns if all(col in row for row in row_list)]
    if len(shared_columns) < width:
        return None
    projected: list[tuple[tuple[str, ...], list[tuple[Any, ...]]]] = []
    for columns in permutations(shared_columns, width):
        projected.append(
            (
                columns,
                [tuple(_freeze_value(row[col]) for col in columns) for row in row_list],
            )
        )
    return projected


def _best_projection(
    projection_rows: list[tuple[tuple[str, ...], list[tuple[Any, ...]]]],
    expected_rows: list[tuple[Any, ...]],
) -> tuple[tuple[str, ...] | None, list[tuple[Any, ...]] | None, int]:
    best_projection: tuple[str, ...] | None = None
    best_candidate: list[tuple[Any, ...]] | None = None
    best_overlap = -1
    expected_counter = Counter(expected_rows)
    for projection, candidate in projection_rows:
        overlap = _counter_overlap(Counter(candidate), expected_counter)
        if overlap > best_overlap:
            best_projection = projection
            best_candidate = candidate
            best_overlap = overlap
    return best_projection, best_candidate, max(best_overlap, 0)


def _match_association_equivalent(
    *,
    rows: list[Mapping[str, Any]],
    direct_rows: list[tuple[Any, ...]] | None,
    projection_rows: list[tuple[tuple[str, ...], list[tuple[Any, ...]]]] | None,
    expected: ExpectedResult,
    expected_count: int,
    result_count: int,
) -> MatchEvaluation | None:
    comparison = expected.comparison
    if comparison is None or comparison.shape_policy != "association_equivalent":
        return None

    candidates: list[tuple[tuple[str, ...], list[tuple[Any, ...]]]] = []
    if direct_rows is not None:
        candidates.append((tuple(expected.columns), direct_rows))
    if projection_rows:
        candidates.extend(projection_rows)

    for projection, candidate_rows in candidates:
        grouped_index = _association_shared_grouped_index(
            expected_rows=expected.rows,
            candidate_rows=candidate_rows,
        )
        if grouped_index is None:
            continue

        expected_assoc = _canonicalize_association_rows(expected.rows, grouped_index)
        candidate_assoc = _canonicalize_association_rows(candidate_rows, grouped_index)
        if candidate_assoc is not None and (
            expected_assoc is not None
            and (
                candidate_assoc == expected_assoc
            or _association_matches_with_empty_parent_policy(
                expected_assoc=expected_assoc,
                candidate_assoc=candidate_assoc,
                empty_parent_policy=comparison.empty_parent_policy,
            )
            )
        ):
            best_overlap = expected_count
            if comparison.empty_parent_policy == "allow_extra_empty":
                best_overlap = _association_overlap_count(expected_assoc, candidate_assoc)
            return MatchEvaluation(
                matched=True,
                match_type=MatchType.PROJECTED,
                expected_count=expected_count,
                result_count=result_count,
                best_overlap=best_overlap,
                best_projection=list(projection),
            )

    return None


def _association_shared_grouped_index(
    *,
    expected_rows: list[list[Any]],
    candidate_rows: list[tuple[Any, ...]],
) -> int | None:
    expected_index = _association_grouped_index(expected_rows)
    candidate_index = _association_grouped_index([list(row) for row in candidate_rows])
    if expected_index is not None and candidate_index is not None:
        return expected_index if expected_index == candidate_index else None
    if expected_index is not None:
        return expected_index
    if candidate_index is not None:
        return candidate_index
    return None


def _match_allow_extra_zero_rows(
    *,
    direct_rows: list[tuple[Any, ...]] | None,
    projection_rows: list[tuple[tuple[str, ...], list[tuple[Any, ...]]]] | None,
    expected: ExpectedResult,
    expected_rows: list[tuple[Any, ...]],
    expected_count: int,
    result_count: int,
) -> MatchEvaluation | None:
    comparison = expected.comparison
    if comparison is None or comparison.zero_count_policy != "allow_extra_zero_rows":
        return None

    candidates: list[tuple[tuple[str, ...], list[tuple[Any, ...]]]] = []
    if direct_rows is not None:
        candidates.append((tuple(expected.columns), direct_rows))
    if projection_rows:
        candidates.extend(projection_rows)

    for projection, candidate_rows in candidates:
        filtered = [row for row in candidate_rows if not _row_has_zero_count_tail(row)]
        if _rows_match(filtered, expected_rows, ordered=expected.ordered):
            return MatchEvaluation(
                matched=True,
                match_type=MatchType.PROJECTED,
                expected_count=expected_count,
                result_count=result_count,
                best_overlap=expected_count,
                best_projection=list(projection),
            )
        if expected.ordered and _multiset_equal(filtered, expected_rows):
            return MatchEvaluation(
                matched=False,
                match_type=MatchType.ORDERING_MISMATCH,
                expected_count=expected_count,
                result_count=result_count,
                best_overlap=expected_count,
                best_projection=list(projection),
            )
    return None


def _match_allow_full_entity_name_projection(
    *,
    rows: list[Mapping[str, Any]],
    expected: ExpectedResult,
    expected_rows: list[tuple[Any, ...]],
    expected_count: int,
    result_count: int,
) -> MatchEvaluation | None:
    comparison = expected.comparison
    if (
        comparison is None
        or comparison.entity_name_policy != "allow_full_entity_name_projection"
        or len(expected.columns) != 1
    ):
        return None

    if any(len(row) != 1 for row in rows):
        return None

    shared_columns = list(rows[0].keys()) if rows else []
    for column in shared_columns:
        projected: list[tuple[Any, ...]] = []
        for row in rows:
            name = _extract_entity_name(row.get(column))
            if name is None:
                projected = []
                break
            projected.append((name,))
        if not projected:
            continue
        if _rows_match(projected, expected_rows, ordered=expected.ordered):
            return MatchEvaluation(
                matched=True,
                match_type=MatchType.PROJECTED,
                expected_count=expected_count,
                result_count=result_count,
                best_overlap=expected_count,
                best_projection=[column],
            )
        if expected.ordered and _multiset_equal(projected, expected_rows):
            return MatchEvaluation(
                matched=False,
                match_type=MatchType.ORDERING_MISMATCH,
                expected_count=expected_count,
                result_count=result_count,
                best_overlap=expected_count,
                best_projection=[column],
            )
    return None


def _row_has_zero_count_tail(row: tuple[Any, ...]) -> bool:
    if not row:
        return False
    tail = row[-1]
    return isinstance(tail, (int, float)) and not isinstance(tail, bool) and tail == 0


def _association_matches_with_empty_parent_policy(
    *,
    expected_assoc: dict[tuple[Any, ...], frozenset[Any]],
    candidate_assoc: dict[tuple[Any, ...], frozenset[Any]],
    empty_parent_policy: str | None,
) -> bool:
    if empty_parent_policy == "allow_omitted":
        if not set(candidate_assoc).issubset(expected_assoc):
            return False
        for parent_key, expected_children in expected_assoc.items():
            candidate_children = candidate_assoc.get(parent_key)
            if expected_children:
                if candidate_children != expected_children:
                    return False
                continue
            if candidate_children not in (None, frozenset()):
                return False
        return True

    if empty_parent_policy == "allow_extra_empty":
        if not set(expected_assoc).issubset(candidate_assoc):
            return False
        for parent_key, expected_children in expected_assoc.items():
            if candidate_assoc.get(parent_key) != expected_children:
                return False
        for parent_key, candidate_children in candidate_assoc.items():
            if parent_key in expected_assoc:
                continue
            if candidate_children != frozenset():
                return False
        return True

    return False


def _association_overlap_count(
    expected_assoc: dict[tuple[Any, ...], frozenset[Any]],
    candidate_assoc: dict[tuple[Any, ...], frozenset[Any]],
) -> int:
    total = 0
    for parent_key, expected_children in expected_assoc.items():
        candidate_children = candidate_assoc.get(parent_key)
        if candidate_children == expected_children:
            total += max(len(expected_children), 1)
    return total


def _association_grouped_index(rows: list[list[Any]]) -> int | None:
    if not rows:
        return None
    width = len(rows[0])
    candidate_indices = {
        index
        for index in range(width)
        if any(isinstance(row[index], (list, tuple)) for row in rows if index < len(row))
    }
    if len(candidate_indices) != 1:
        return None
    return next(iter(candidate_indices))


def _canonicalize_association_rows(
    rows: Iterable[Iterable[Any]], grouped_index: int
) -> dict[tuple[Any, ...], frozenset[Any]] | None:
    association: dict[tuple[Any, ...], set[Any]] = {}
    for row in rows:
        values = list(row)
        if grouped_index >= len(values):
            return None
        parent_key = tuple(
            _freeze_value(value)
            for index, value in enumerate(values)
            if index != grouped_index
        )
        if parent_key not in association:
            association[parent_key] = set()

        child_value = values[grouped_index]
        if isinstance(child_value, (list, tuple)):
            for item in child_value:
                frozen = _freeze_value(item)
                if frozen is not None:
                    association[parent_key].add(frozen)
            continue

        frozen = _freeze_value(child_value)
        if frozen is not None:
            association[parent_key].add(frozen)

    return {
        parent_key: frozenset(children)
        for parent_key, children in association.items()
    }


def _has_nested_values(rows: Iterable[Mapping[str, Any]]) -> bool:
    for row in rows:
        for value in row.values():
            if isinstance(value, (list, dict)):
                return True
    return False


def _looks_grouped_shape(
    candidate: list[tuple[Any, ...]] | None,
    expected_rows: list[tuple[Any, ...]],
    result_count: int,
    expected_count: int,
) -> bool:
    if candidate is None:
        return True
    if result_count < expected_count:
        return True
    if expected_count <= 0:
        return False
    return _overlap_size(candidate, expected_rows) < expected_count


def _freeze_value(value: Any) -> Any:
    if isinstance(value, dict):
        return tuple((key, _freeze_value(value[key])) for key in sorted(value))
    if isinstance(value, list):
        return tuple(_freeze_value(item) for item in value)
    return value


def _extract_entity_name(value: Any) -> str | None:
    if not isinstance(value, dict):
        return None
    properties = value.get("properties")
    if isinstance(properties, dict):
        metadata = properties.get("metadata")
        if isinstance(metadata, dict):
            name = metadata.get("name")
            if isinstance(name, str):
                return name
        name = properties.get("name")
        if isinstance(name, str):
            return name
    metadata = value.get("metadata")
    if isinstance(metadata, dict):
        name = metadata.get("name")
        if isinstance(name, str):
            return name
    name = value.get("name")
    if isinstance(name, str):
        return name
    return None


def _rows_match(
    left: list[tuple[Any, ...]], right: list[tuple[Any, ...]], *, ordered: bool
) -> bool:
    if ordered:
        return left == right
    return _multiset_equal(left, right)


def _multiset_equal(left: list[tuple[Any, ...]], right: list[tuple[Any, ...]]) -> bool:
    return Counter(left) == Counter(right)


def _overlap_size(left: list[tuple[Any, ...]], right: list[tuple[Any, ...]]) -> int:
    return _counter_overlap(Counter(left), Counter(right))


def _counter_overlap(left: Counter[tuple[Any, ...]], right: Counter[tuple[Any, ...]]) -> int:
    total = 0
    for key, left_count in left.items():
        total += min(left_count, right.get(key, 0))
    return total
