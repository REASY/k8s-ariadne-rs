from typing import cast

from k8s_graph_agent.mcp_client import (
    _parse_sse_messages,
    _pick_response,
    extract_json_content,
)


def test_parse_sse_messages() -> None:
    body = 'data: {"jsonrpc":"2.0","id":1,"result":{}}\n\n'
    messages = _parse_sse_messages(body)
    assert len(messages) == 1
    assert messages[0]["id"] == 1


def test_pick_response() -> None:
    responses = [
        {"id": 1, "result": {"value": "a"}},
        {"id": 2, "result": {"value": "b"}},
    ]
    picked = _pick_response(responses, 2)
    assert picked is not None
    result = picked.get("result")
    assert isinstance(result, dict)
    result_dict = cast(dict[str, object], result)
    assert result_dict.get("value") == "b"


def test_extract_json_content() -> None:
    tool_result = {
        "content": [
            {"type": "text", "text": '[{"pod": "a"}]'},
        ]
    }
    extracted = extract_json_content(tool_result)
    assert isinstance(extracted, list)
    extracted_list = cast(list[object], extracted)
    assert extracted_list
    first = extracted_list[0]
    assert isinstance(first, dict)
    first_dict = cast(dict[str, object], first)
    assert first_dict.get("pod") == "a"
