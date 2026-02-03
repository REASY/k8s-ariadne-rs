from k8s_graph_agent.synthesize import SreResponseSynthesizer


def test_empty_result() -> None:
    synthesizer = SreResponseSynthesizer()
    response = synthesizer.synthesize("why?", "MATCH (n) RETURN n", [])
    assert "Rows returned: 0" in response
    assert "Next steps:" in response
