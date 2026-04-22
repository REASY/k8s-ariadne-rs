from __future__ import annotations

from k8s_graph_agent.eval.expansion import (
    build_generated_question,
    extract_host,
    extract_namespace,
    replace_namespace_text,
    replace_quoted_literal,
    round_robin_variants,
)
from k8s_graph_agent.eval.models import EvalQuestion, ExpectedResult


def test_extract_namespace_requires_question_and_query_alignment() -> None:
    question = "List all pods in namespace litmus."
    cypher = (
        "MATCH (p:Pod)-[:BelongsTo]->(ns:Namespace) "
        "WHERE ns['metadata']['name'] = 'litmus' "
        "RETURN p['metadata']['name'] AS pod"
    )

    assert extract_namespace(question, cypher) == "litmus"


def test_replace_namespace_text_updates_question_once() -> None:
    question = "For namespace litmus, list services in namespace litmus."

    rewritten = replace_namespace_text(question, "litmus", "pyroscope")

    assert rewritten == "For namespace pyroscope, list services in namespace litmus."


def test_extract_host_uses_literal_present_in_query() -> None:
    question = "For host litmus.qa.agoda.is, list backend services."
    cypher = "MATCH (h:Host) WHERE h['name'] = 'litmus.qa.agoda.is' RETURN h"

    assert extract_host(question, cypher) == "litmus.qa.agoda.is"


def test_round_robin_variants_spreads_across_sources() -> None:
    expected = ExpectedResult(columns=["name"], rows=[["a"]])
    q1 = EvalQuestion(id="e01", question="Q1", expected=expected)
    q2 = EvalQuestion(id="e02", question="Q2", expected=expected)
    variants = {
        "e01": [
            build_generated_question(
                source=q1,
                variant_key="pyroscope",
                question_text="Q1 ns1",
                reference_cypher="MATCH 1",
                expected=expected,
                family="namespace_variant",
                parameters={"namespace": "pyroscope"},
            ),
            build_generated_question(
                source=q1,
                variant_key="tempo",
                question_text="Q1 ns2",
                reference_cypher="MATCH 2",
                expected=expected,
                family="namespace_variant",
                parameters={"namespace": "tempo"},
            ),
        ],
        "e02": [
            build_generated_question(
                source=q2,
                variant_key="storefront",
                question_text="Q2 ns1",
                reference_cypher="MATCH 3",
                expected=expected,
                family="namespace_variant",
                parameters={"namespace": "storefront"},
            )
        ],
    }

    selected = round_robin_variants(variants, limit=3)

    assert [question.source_question_id for question in selected] == ["e01", "e02", "e01"]


def test_replace_quoted_literal_rewrites_exact_literal() -> None:
    text = "WHERE ns['metadata']['name'] = 'litmus' RETURN 'litmus' AS ns"

    rewritten = replace_quoted_literal(text, "litmus", "tempo")

    assert rewritten == "WHERE ns['metadata']['name'] = 'tempo' RETURN 'tempo' AS ns"
