import os

from k8s_graph_agent.logging_utils import format_java_like


def _raise_error() -> None:
    raise ValueError("boom")


def test_format_java_like_includes_thread_and_location() -> None:
    try:
        _raise_error()
    except Exception as exc:
        output = format_java_like(exc, thread_name="worker-1")
    assert 'Exception in thread "worker-1"' in output
    assert "ValueError" in output
    filename = os.path.basename(__file__)
    assert f"{filename}:" in output
    assert "at _raise_error" in output


def test_format_java_like_without_thread_name() -> None:
    try:
        _raise_error()
    except Exception as exc:
        output = format_java_like(exc)
    assert output.startswith("Exception (ValueError):")
