import pytest
import yaml

from k8s_graph_agent.dspy_experiment import (
    _match_expected,
    _score_match_evaluation,
    build_stratified_split,
)
from k8s_graph_agent.eval.matching import MatchEvaluation, MatchType, evaluate_expected_match
from k8s_graph_agent.eval.models import ExpectedResult


def test_match_expected_handles_nested_values() -> None:
    result = [
        {
            "host": "litmus.qa.agoda.is",
            "backend_services": [
                {
                    "service": "chaos-litmus-server-service",
                    "port": 9002,
                }
            ],
        }
    ]
    expected = ExpectedResult(
        columns=["backend_services", "host"],
        rows=[
            [
                [
                    {
                        "service": "chaos-litmus-server-service",
                        "port": 9002,
                    }
                ],
                "litmus.qa.agoda.is",
            ]
        ],
        ordered=False,
    )
    assert _match_expected(result, expected) is True


def test_match_expected_allows_projection_with_extra_columns() -> None:
    result = [
        {"name": "chaos-litmus-admin-config", "data": {"x": "1"}, "immutable": None},
        {
            "name": "chaos-litmus-frontend-nginx-configuration",
            "data": {"x": "2"},
            "immutable": None,
        },
        {"name": "chaos-mongodb-common-scripts", "data": {"x": "3"}, "immutable": None},
        {"name": "chaos-mongodb-scripts", "data": {"x": "4"}, "immutable": None},
        {"name": "kube-root-ca.crt", "data": {"x": "5"}, "immutable": None},
    ]
    expected = ExpectedResult(
        columns=["config_map"],
        rows=[
            ["chaos-litmus-admin-config"],
            ["chaos-litmus-frontend-nginx-configuration"],
            ["chaos-mongodb-common-scripts"],
            ["chaos-mongodb-scripts"],
            ["kube-root-ca.crt"],
        ],
        ordered=False,
    )
    assert _match_expected(result, expected) is True


def test_match_expected_allows_projection_with_reordered_columns() -> None:
    result = [
        {"b": "svc-a", "a": "ns-1", "extra": 1},
        {"b": "svc-b", "a": "ns-2", "extra": 2},
    ]
    expected = ExpectedResult(
        columns=["namespace", "service"],
        rows=[
            ["ns-1", "svc-a"],
            ["ns-2", "svc-b"],
        ],
        ordered=False,
    )
    assert _match_expected(result, expected) is True


def test_match_taxonomy_reports_projection_match() -> None:
    result = [
        {"name": "cm-a", "namespace": "litmus", "uid": "1"},
        {"name": "cm-b", "namespace": "litmus", "uid": "2"},
    ]
    expected = ExpectedResult(
        columns=["config_map"],
        rows=[["cm-a"], ["cm-b"]],
        ordered=False,
    )
    evaluation = evaluate_expected_match(result, expected)
    assert evaluation.matched is True
    assert evaluation.match_type == "projected"
    assert evaluation.best_projection == ["name"]


def test_match_taxonomy_reports_extra_rows() -> None:
    result = [
        {"service": "svc-a"},
        {"service": "svc-b"},
        {"service": "svc-c"},
    ]
    expected = ExpectedResult(
        columns=["service"],
        rows=[["svc-a"], ["svc-b"]],
        ordered=False,
    )
    evaluation = evaluate_expected_match(result, expected)
    assert evaluation.matched is False
    assert evaluation.match_type == "extra_rows_overinclusive"
    assert evaluation.best_overlap == 2


def test_match_taxonomy_reports_grouped_shape() -> None:
    result = [
        {"statefulset": "mongo", "pods": ["mongo-0", "mongo-1"], "pod_count": 2},
        {"statefulset": "redis", "pods": ["redis-0"], "pod_count": 1},
    ]
    expected = ExpectedResult(
        columns=["pod", "stateful_set"],
        rows=[
            ["mongo-0", "mongo"],
            ["mongo-1", "mongo"],
            ["redis-0", "redis"],
        ],
        ordered=False,
    )
    evaluation = evaluate_expected_match(result, expected)
    assert evaluation.matched is False
    assert evaluation.match_type == "grouped_or_aggregated_shape"


def test_match_allows_association_equivalent_flat_rows_for_grouped_gold() -> None:
    result = [
        {"service": "svc-a", "endpoint_slice": "es-1"},
        {"service": "svc-a", "endpoint_slice": "es-2"},
        {"service": "svc-b", "endpoint_slice": "es-3"},
    ]
    expected = ExpectedResult(
        columns=["endpoint_slices", "service"],
        rows=[
            [["es-1", "es-2"], "svc-a"],
            [["es-3"], "svc-b"],
        ],
        ordered=False,
        comparison={"shape_policy": "association_equivalent"},
    )
    evaluation = evaluate_expected_match(result, expected)
    assert evaluation.matched is True
    assert evaluation.match_type == "projected"
    assert evaluation.best_projection == ["endpoint_slice", "service"]


def test_match_allows_association_equivalent_grouped_rows_for_flat_gold() -> None:
    result = [
        {"deployment_name": "dep-a", "replica_sets": ["rs-1", "rs-2"]},
        {"deployment_name": "dep-b", "replica_sets": ["rs-3"]},
    ]
    expected = ExpectedResult(
        columns=["deployment", "replica_set"],
        rows=[
            ["dep-a", "rs-1"],
            ["dep-a", "rs-2"],
            ["dep-b", "rs-3"],
        ],
        ordered=False,
        comparison={"shape_policy": "association_equivalent"},
    )
    evaluation = evaluate_expected_match(result, expected)
    assert evaluation.matched is True
    assert evaluation.match_type == "projected"
    assert evaluation.best_projection == ["deployment_name", "replica_sets"]


def test_match_allows_association_equivalent_with_extra_columns() -> None:
    result = [
        {
            "endpoint_slice": "es-a",
            "pod": "pod-1",
            "pod_namespace": "ns-a",
            "endpoint_address": "10.0.0.1",
        },
        {
            "endpoint_slice": "es-a",
            "pod": "pod-2",
            "pod_namespace": "ns-a",
            "endpoint_address": "10.0.0.2",
        },
        {
            "endpoint_slice": "es-b",
            "pod": "pod-3",
            "pod_namespace": "ns-a",
            "endpoint_address": "10.0.0.3",
        },
    ]
    expected = ExpectedResult(
        columns=["endpoint_slice", "pods"],
        rows=[
            ["es-a", ["pod-1", "pod-2"]],
            ["es-b", ["pod-3"]],
        ],
        ordered=False,
        comparison={"shape_policy": "association_equivalent"},
    )
    evaluation = evaluate_expected_match(result, expected)
    assert evaluation.matched is True
    assert evaluation.match_type == "projected"
    assert evaluation.best_projection == ["endpoint_slice", "pod"]


def test_match_allows_omitted_empty_parents_for_association_equivalent() -> None:
    result = [
        {"service": "svc-a", "pod": "pod-1"},
        {"service": "svc-a", "pod": "pod-2"},
        {"service": "svc-b", "pod": "pod-3"},
    ]
    expected = ExpectedResult(
        columns=["backing_pods", "service"],
        rows=[
            [["pod-1", "pod-2"], "svc-a"],
            [["pod-3"], "svc-b"],
            [[], "svc-empty"],
        ],
        ordered=False,
        comparison={
            "shape_policy": "association_equivalent",
            "empty_parent_policy": "allow_omitted",
        },
    )
    evaluation = evaluate_expected_match(result, expected)
    assert evaluation.matched is True
    assert evaluation.match_type == "projected"
    assert evaluation.best_projection == ["pod", "service"]


def test_match_allows_empty_result_when_all_association_parents_are_empty_and_omittable() -> None:
    result: list[dict[str, str]] = []
    expected = ExpectedResult(
        columns=["pod", "pvc_claims"],
        rows=[
            ["pod-a", []],
            ["pod-b", []],
        ],
        ordered=False,
        comparison={
            "shape_policy": "association_equivalent",
            "empty_parent_policy": "allow_omitted",
        },
    )
    evaluation = evaluate_expected_match(result, expected)
    assert evaluation.matched is True
    assert evaluation.match_type == "projected"
    assert evaluation.best_projection == ["pod", "pvc_claims"]


def test_match_allows_extra_empty_parents_for_association_equivalent() -> None:
    result = [
        {"replicaset": "rs-a", "pods": ["pod-1", "pod-2"]},
        {"replicaset": "rs-b", "pods": ["pod-3"]},
        {"replicaset": "rs-empty", "pods": []},
    ]
    expected = ExpectedResult(
        columns=["pods", "replicaset"],
        rows=[
            [["pod-1", "pod-2"], "rs-a"],
            [["pod-3"], "rs-b"],
        ],
        ordered=False,
        comparison={
            "shape_policy": "association_equivalent",
            "empty_parent_policy": "allow_extra_empty",
        },
    )
    evaluation = evaluate_expected_match(result, expected)
    assert evaluation.matched is True
    assert evaluation.match_type == "projected"
    assert evaluation.best_projection == ["pods", "replicaset"]


def test_match_allows_grouped_rows_with_extra_empty_parents_for_flat_gold() -> None:
    result = [
        {"statefulset": "ss-a", "pods": ["pod-1", "pod-2"]},
        {"statefulset": "ss-b", "pods": ["pod-3"]},
        {"statefulset": "ss-empty", "pods": []},
    ]
    expected = ExpectedResult(
        columns=["pod", "stateful_set"],
        rows=[
            ["pod-1", "ss-a"],
            ["pod-2", "ss-a"],
            ["pod-3", "ss-b"],
        ],
        ordered=False,
        comparison={
            "shape_policy": "association_equivalent",
            "empty_parent_policy": "allow_extra_empty",
        },
    )
    evaluation = evaluate_expected_match(result, expected)
    assert evaluation.matched is True
    assert evaluation.match_type == "projected"
    assert evaluation.best_projection == ["statefulset", "pods"]


def test_match_allows_extra_zero_count_rows_when_policy_enabled() -> None:
    result = [
        {"namespace": "default", "service_count": 4},
        {"namespace": "litmus", "service_count": 2},
        {"namespace": "empty-a", "service_count": 0},
        {"namespace": "empty-b", "service_count": 0},
    ]
    expected = ExpectedResult(
        columns=["namespace", "service_count"],
        rows=[
            ["default", 4],
            ["litmus", 2],
        ],
        ordered=False,
        comparison={"zero_count_policy": "allow_extra_zero_rows"},
    )
    evaluation = evaluate_expected_match(result, expected)
    assert evaluation.matched is True
    assert evaluation.match_type == "projected"
    assert evaluation.best_projection == ["namespace", "service_count"]


def test_match_allows_full_entity_name_projection_for_single_column_gold() -> None:
    result = [
        {"es": {"metadata": {"name": "slice-a"}, "kind": "EndpointSlice"}},
        {"es": {"metadata": {"name": "slice-b"}, "kind": "EndpointSlice"}},
    ]
    expected = ExpectedResult(
        columns=["endpoint_slice"],
        rows=[["slice-a"], ["slice-b"]],
        ordered=False,
        comparison={"entity_name_policy": "allow_full_entity_name_projection"},
    )
    evaluation = evaluate_expected_match(result, expected)
    assert evaluation.matched is True
    assert evaluation.match_type == "projected"
    assert evaluation.best_projection == ["es"]


def test_match_allows_full_entity_name_projection_for_mcp_node_shape() -> None:
    result = [
        {"cm": {"type": "node", "properties": {"metadata": {"name": "cm-a"}}}},
        {"cm": {"type": "node", "properties": {"metadata": {"name": "cm-b"}}}},
    ]
    expected = ExpectedResult(
        columns=["config_map"],
        rows=[["cm-a"], ["cm-b"]],
        ordered=False,
        comparison={"entity_name_policy": "allow_full_entity_name_projection"},
    )
    evaluation = evaluate_expected_match(result, expected)
    assert evaluation.matched is True
    assert evaluation.match_type == "projected"
    assert evaluation.best_projection == ["cm"]


# ---------------------------------------------------------------------------
# _score_match_evaluation tests
# ---------------------------------------------------------------------------

def _eval(
    match_type: MatchType,
    *,
    matched: bool = False,
    expected_count: int = 10,
    result_count: int | None = 10,
    best_overlap: int | None = None,
) -> MatchEvaluation:
    return MatchEvaluation(
        matched=matched,
        match_type=match_type,
        expected_count=expected_count,
        result_count=result_count,
        best_overlap=best_overlap,
    )


class TestScoreMatchEvaluationBounds:
    """Every match type must produce a score in [0, 1]."""

    @pytest.mark.parametrize("mt", list(MatchType))
    def test_score_in_unit_interval(self, mt: MatchType) -> None:
        matched = mt in {MatchType.EXACT, MatchType.PROJECTED}
        for overlap in [0, 1, 5, 10]:
            for result_count in [0, 1, 10, 1000]:
                score = _score_match_evaluation(
                    _eval(mt, matched=matched, best_overlap=overlap, result_count=result_count)
                )
                assert 0.0 <= score <= 1.0, f"{mt} overlap={overlap} rc={result_count} -> {score}"


class TestScoreMatchEvaluationMonotonicity:
    """Key ordering invariants the optimizer relies on."""

    def test_exact_beats_ordering_mismatch(self) -> None:
        exact = _score_match_evaluation(_eval(MatchType.EXACT, matched=True))
        ordering = _score_match_evaluation(_eval(MatchType.ORDERING_MISMATCH, best_overlap=10))
        assert exact > ordering

    def test_ordering_mismatch_beats_partial(self) -> None:
        ordering = _score_match_evaluation(_eval(MatchType.ORDERING_MISMATCH, best_overlap=10))
        missing_high = _score_match_evaluation(
            _eval(MatchType.MISSING_ROWS, best_overlap=9, result_count=9)
        )
        assert ordering > missing_high

    def test_high_overlap_missing_beats_low_precision_extra(self) -> None:
        # 9/10 correct rows should beat 10/1000 precision.
        missing = _score_match_evaluation(
            _eval(MatchType.MISSING_ROWS, best_overlap=9, result_count=9)
        )
        extra = _score_match_evaluation(
            _eval(MatchType.EXTRA_ROWS, best_overlap=10, result_count=1000)
        )
        assert missing > extra

    def test_high_precision_extra_beats_low_overlap_missing(self) -> None:
        # 10/11 precision should beat 2/10 overlap.
        extra = _score_match_evaluation(
            _eval(MatchType.EXTRA_ROWS, best_overlap=10, result_count=11)
        )
        missing = _score_match_evaluation(
            _eval(MatchType.MISSING_ROWS, best_overlap=2, result_count=2)
        )
        assert extra > missing

    def test_missing_rows_monotonic_in_overlap(self) -> None:
        scores = [
            _score_match_evaluation(
                _eval(MatchType.MISSING_ROWS, best_overlap=i, result_count=i)
            )
            for i in range(11)
        ]
        for i in range(len(scores) - 1):
            assert scores[i] <= scores[i + 1], f"overlap {i} -> {i+1}: {scores[i]} > {scores[i+1]}"

    def test_extra_rows_monotonic_in_precision(self) -> None:
        result_counts = [1000, 100, 50, 20, 11]
        scores = [
            _score_match_evaluation(
                _eval(MatchType.EXTRA_ROWS, best_overlap=10, result_count=rc)
            )
            for rc in result_counts
        ]
        for i in range(len(scores) - 1):
            assert scores[i] <= scores[i + 1], f"rc {result_counts[i]} -> {result_counts[i+1]}: {scores[i]} > {scores[i+1]}"

    def test_partial_beats_wrong_semantics(self) -> None:
        missing = _score_match_evaluation(
            _eval(MatchType.MISSING_ROWS, best_overlap=1, result_count=1)
        )
        wrong = _score_match_evaluation(_eval(MatchType.WRONG_SEMANTICS, best_overlap=0))
        assert missing > wrong

    def test_wrong_semantics_beats_execution_error(self) -> None:
        wrong = _score_match_evaluation(_eval(MatchType.WRONG_SEMANTICS, best_overlap=0))
        error = _score_match_evaluation(_eval(MatchType.EXECUTION_ERROR))
        assert wrong > error


def test_build_stratified_split_keeps_group_variants_together(tmp_path) -> None:
    dataset = [
        {
            "id": "e01",
            "question": "Q1",
            "tags": [],
            "deterministic": True,
            "reference_cypher": "MATCH 1",
            "expected": {"columns": ["x"], "rows": [[1]], "ordered": False},
            "group_id": "base_a",
        },
        {
            "id": "e01_namespace_variant_pyroscope",
            "question": "Q1 variant",
            "tags": ["generated"],
            "deterministic": True,
            "reference_cypher": "MATCH 2",
            "expected": {"columns": ["x"], "rows": [[2]], "ordered": False},
            "group_id": "base_a",
        },
        {
            "id": "m01",
            "question": "Q2",
            "tags": [],
            "deterministic": True,
            "reference_cypher": "MATCH 3",
            "expected": {"columns": ["x"], "rows": [[3]], "ordered": False},
            "group_id": "base_b",
        },
        {
            "id": "m01_namespace_variant_tempo",
            "question": "Q2 variant",
            "tags": ["generated"],
            "deterministic": True,
            "reference_cypher": "MATCH 4",
            "expected": {"columns": ["x"], "rows": [[4]], "ordered": False},
            "group_id": "base_b",
        },
        {
            "id": "h01",
            "question": "Q3",
            "tags": [],
            "deterministic": True,
            "reference_cypher": "MATCH 5",
            "expected": {"columns": ["x"], "rows": [[5]], "ordered": False},
            "group_id": "base_c",
        },
        {
            "id": "h01_namespace_variant_loki",
            "question": "Q3 variant",
            "tags": ["generated"],
            "deterministic": True,
            "reference_cypher": "MATCH 6",
            "expected": {"columns": ["x"], "rows": [[6]], "ordered": False},
            "group_id": "base_c",
        },
    ]
    dataset_path = tmp_path / "dataset.yaml"
    dataset_path.write_text(yaml.safe_dump(dataset, sort_keys=False), encoding="utf-8")

    split = build_stratified_split(dataset_path, train_size=4, seed=7)
    train_ids = {q.id for q in split.train}
    dev_ids = {q.id for q in split.dev}

    assert train_ids.isdisjoint(dev_ids)
    for pair in [
        {"e01", "e01_namespace_variant_pyroscope"},
        {"m01", "m01_namespace_variant_tempo"},
        {"h01", "h01_namespace_variant_loki"},
    ]:
        assert pair <= train_ids or pair <= dev_ids
