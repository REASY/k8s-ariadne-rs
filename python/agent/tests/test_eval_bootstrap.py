from __future__ import annotations

from k8s_graph_agent.eval.bootstrap import (
    build_expected_result,
    choose_consensus_reference,
    collect_questions,
    consensus_fingerprint,
    result_fingerprint,
    sample_grouped_dataset,
    select_questions,
    update_question_fields,
)


def test_collect_questions_adds_group_difficulty_tag() -> None:
    raw = {
        "easy": [
            {
                "id": "e01",
                "question": "List namespaces",
                "tags": ["namespace"],
                "deterministic": True,
            }
        ]
    }

    questions = collect_questions(raw)

    assert len(questions) == 1
    assert "namespace" in questions[0].tags
    assert "difficulty:easy" in questions[0].tags


def test_select_questions_filters_by_reference_and_tags() -> None:
    raw = [
        {
            "id": "q1",
            "question": "List namespaces",
            "tags": ["namespace"],
            "deterministic": True,
            "reference_cypher": "MATCH (n:Namespace) RETURN n['name'] AS name",
        },
        {
            "id": "q2",
            "question": "List pods",
            "tags": ["pod"],
            "deterministic": True,
        },
    ]

    questions = select_questions(
        raw,
        tags={"namespace"},
        deterministic_only=True,
        require_reference=True,
    )

    assert [question.id for question in questions] == ["q1"]


def test_update_question_fields_updates_grouped_dataset() -> None:
    raw = {
        "easy": [
            {
                "id": "e01",
                "question": "List namespaces",
            }
        ]
    }

    changed = update_question_fields(
        raw,
        "e01",
        {
            "reference_cypher": "MATCH (n:Namespace) RETURN n['name'] AS name",
        },
    )

    assert changed is True
    assert raw["easy"][0]["reference_cypher"].startswith("MATCH")


def test_build_expected_result_preserves_existing_columns() -> None:
    result = [
        {"namespace": "default", "count": 3},
        {"namespace": "kube-system", "count": 8},
    ]

    expected = build_expected_result(result, columns=["count", "namespace"], ordered=True)

    assert expected.columns == ["count", "namespace"]
    assert expected.rows == [[3, "default"], [8, "kube-system"]]
    assert expected.ordered is True


def test_build_expected_result_infers_columns_when_existing_columns_are_empty() -> None:
    result = [
        {"daemon_set": "rook-ceph-agent", "pod": "rook-ceph-agent-a1b2c"},
        {"daemon_set": "rook-ceph-agent", "pod": "rook-ceph-agent-d3e4f"},
    ]

    expected = build_expected_result(result, columns=[], ordered=False)

    assert expected.columns == ["daemon_set", "pod"]
    assert expected.rows == [
        ["rook-ceph-agent", "rook-ceph-agent-a1b2c"],
        ["rook-ceph-agent", "rook-ceph-agent-d3e4f"],
    ]
    assert expected.ordered is False


def test_result_fingerprint_ignores_row_order() -> None:
    left = [
        {"namespace": "default", "count": 3},
        {"namespace": "kube-system", "count": 8},
    ]
    right = [
        {"count": 8, "namespace": "kube-system"},
        {"count": 3, "namespace": "default"},
    ]

    assert result_fingerprint(left) == result_fingerprint(right)


def test_consensus_fingerprint_returns_most_common_value() -> None:
    fingerprint = consensus_fingerprint(["abc", "abc", None, "def"])

    assert fingerprint == "abc"


def test_sample_grouped_dataset_stratifies_evenly_and_marks_deterministic() -> None:
    raw = {
        "easy": [
            {"id": "e01", "question": "Q1"},
            {"id": "e02", "question": "Q2"},
            {"id": "e03", "question": "Q3"},
        ],
        "medium": [
            {"id": "m01", "question": "Q1"},
            {"id": "m02", "question": "Q2"},
            {"id": "m03", "question": "Q3"},
        ],
        "hard": [
            {"id": "h01", "question": "Q1"},
            {"id": "h02", "question": "Q2"},
            {"id": "h03", "question": "Q3"},
        ],
    }

    sampled = sample_grouped_dataset(raw, total=5, seed=7)

    assert sum(len(items) for items in sampled.values()) == 5
    assert len(sampled["easy"]) == 2
    assert len(sampled["medium"]) == 2
    assert len(sampled["hard"]) == 1
    assert all(item["deterministic"] is True for items in sampled.values() for item in items)


def test_choose_consensus_reference_requires_same_result_and_same_cypher() -> None:
    candidate_runs = [
        {
            "model": "m1",
            "valid": True,
            "cypher": "MATCH (n) RETURN n",
            "execution_error": None,
            "result_fingerprint": "abc",
        },
        {
            "model": "m2",
            "valid": True,
            "cypher": "MATCH (n) RETURN n",
            "execution_error": None,
            "result_fingerprint": "abc",
        },
        {
            "model": "m3",
            "valid": True,
            "cypher": "MATCH (m) RETURN m",
            "execution_error": None,
            "result_fingerprint": "def",
        },
    ]

    assert choose_consensus_reference(candidate_runs, min_models=2) == "MATCH (n) RETURN n"


def test_choose_consensus_reference_rejects_same_result_with_different_cypher() -> None:
    candidate_runs = [
        {
            "model": "m1",
            "valid": True,
            "cypher": "MATCH (n) RETURN n",
            "execution_error": None,
            "result_fingerprint": "abc",
        },
        {
            "model": "m2",
            "valid": True,
            "cypher": "MATCH (x) RETURN x",
            "execution_error": None,
            "result_fingerprint": "abc",
        },
    ]

    assert choose_consensus_reference(candidate_runs, min_models=2) is None
