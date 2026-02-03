from k8s_graph_agent.prompting import extract_prompt_text


def test_extract_prompt_text() -> None:
    prompt_result = {
        "messages": [
            {"content": {"type": "text", "text": "hello"}},
            {"content": {"text": "world"}},
        ]
    }
    text = extract_prompt_text(prompt_result)
    assert text == "hello\nworld"
