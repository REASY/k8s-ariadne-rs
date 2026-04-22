from k8s_graph_agent.adk_translate import TokenUsage, TranslationAttempt, TranslationOutcome
from k8s_graph_agent.eval.models import EvalQuestion, ExpectedResult
from k8s_graph_agent.eval.runner import _run_question
from k8s_graph_agent.mcp_client import JsonRpcError


class FakeTranslator:
    def __init__(self) -> None:
        self.repair_calls = []

    def translate_with_attempts(self, question: str, max_attempts: int = 2) -> TranslationOutcome:
        return TranslationOutcome(
            cypher="MATCH (bad) RETURN bad",
            attempts=[
                TranslationAttempt(
                    attempt=1,
                    cypher="MATCH (bad) RETURN bad",
                    valid=True,
                    error=None,
                    usage=TokenUsage(),
                )
            ],
            total_usage=TokenUsage(),
            error=None,
        )

    def translate_with_execution_error(
        self, question: str, cypher: str, error: str, max_attempts: int = 1
    ) -> TranslationOutcome:
        self.repair_calls.append((question, cypher, error, max_attempts))
        return TranslationOutcome(
            cypher="MATCH (fixed) RETURN fixed AS value",
            attempts=[
                TranslationAttempt(
                    attempt=1,
                    cypher="MATCH (fixed) RETURN fixed AS value",
                    valid=True,
                    error=None,
                    usage=TokenUsage(),
                )
            ],
            total_usage=TokenUsage(),
            error=None,
        )


class FlakyGraph:
    def __init__(self) -> None:
        self.calls = []

    def execute_cypher(self, query: str):
        self.calls.append(query)
        if len(self.calls) == 1:
            raise RuntimeError(
                "MemgraphError: QueryError: Query execution error: Unbound variable: ns."
            )
        return [{"value": "ok"}]


class ValidatorRejectingGraph:
    def execute_cypher(self, query: str):
        raise JsonRpcError(
            {
                "message": "Cypher syntax error in parse tree",
                "code": -32602,
                "data": {
                    "cypher": query,
                    "kind": "parse_error",
                    "repairable": True,
                    "retryable": False,
                    "source": "validator",
                },
            }
        )


def test_run_question_retries_after_execution_error_in_retry_mode() -> None:
    translator = FakeTranslator()
    graph = FlakyGraph()
    question = EvalQuestion(
        id="q1",
        question="Return fixed value",
        expected=ExpectedResult(columns=["value"], rows=[["ok"]]),
    )

    record = _run_question(
        translator=translator,
        graph=graph,
        question=question,
        mode="retry",
        run_index=1,
        model="gemini-2.5-flash",
    )

    assert len(graph.calls) == 2
    assert translator.repair_calls
    assert "Unbound variable: ns." in translator.repair_calls[0][2]
    assert record.final["cypher"] == "MATCH (fixed) RETURN fixed AS value"
    assert record.final["result_match"] is True
    assert record.metrics["attempts"] == 2


def test_run_question_does_not_retry_after_execution_error_in_single_shot() -> None:
    translator = FakeTranslator()
    graph = FlakyGraph()
    question = EvalQuestion(id="q1", question="Return fixed value")

    record = _run_question(
        translator=translator,
        graph=graph,
        question=question,
        mode="single-shot",
        run_index=1,
        model="gemini-2.5-flash",
    )

    assert len(graph.calls) == 1
    assert translator.repair_calls == []
    assert "execution_error" in record.final


def test_run_question_classifies_validator_failures_separately() -> None:
    translator = FakeTranslator()
    graph = ValidatorRejectingGraph()
    question = EvalQuestion(id="q1", question="Return fixed value")

    record = _run_question(
        translator=translator,
        graph=graph,
        question=question,
        mode="single-shot",
        run_index=1,
        model="gemini-2.5-flash",
    )

    assert translator.repair_calls == []
    assert record.final["match_type"] == "validator_parse_error"
    assert record.final["validator_error"] == "Cypher syntax error in parse tree"
    assert record.final["query_issue_kind"] == "parse_error"
    assert record.final["query_issue_source"] == "validator"
    assert "execution_error" not in record.final
