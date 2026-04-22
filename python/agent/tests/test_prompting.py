from k8s_graph_agent.prompting import (
    extract_prompt_text,
    render_prompt_bundle,
    split_prompt_sections,
)


def test_extract_prompt_text() -> None:
    prompt_result = {
        "messages": [
            {"content": {"type": "text", "text": "hello"}},
            {"content": {"text": "world"}},
        ]
    }
    text = extract_prompt_text(prompt_result)
    assert text == "hello\nworld"


def test_split_prompt_sections() -> None:
    prompt_text = """You are Ariadne.

# Important considerations

## Rules for Cypher query generation
Rule A
Rule B

## Definitive Graph Schema Reference
Schema block

### Node Connectivity
Connectivity block

Before you output the final query, do one last check.

User question: 'Which pods are failing?'"""
    sections = split_prompt_sections(prompt_text)
    assert sections.instruction == "You are Ariadne.\n\n# Important considerations"
    assert sections.rules == "## Rules for Cypher query generation\nRule A\nRule B"
    assert sections.schema_reference == "## Definitive Graph Schema Reference\nSchema block"
    assert sections.node_connectivity == "### Node Connectivity\nConnectivity block"
    assert sections.footer == "Before you output the final query, do one last check."


def test_render_prompt_bundle_appends_question() -> None:
    prompt = render_prompt_bundle("Compiled prompt bundle", "What's failing?")
    assert prompt == "Compiled prompt bundle\n\nUser question: 'What\\'s failing?'"
