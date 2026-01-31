# K8s Graph Agent (Python)

This is a typed Python scaffold for the agent layer that talks to the MCP server exposed by the Rust app.
It supports direct Cypher execution via the MCP tool `execute_cypher_query` and includes an ADK
translator that uses LiteLLM for provider-agnostic LLM access.

## Setup (uv)

```bash
cd python/agent
uv venv
uv pip install -e .
```

Set your model credentials (examples):
```bash
export GOOGLE_API_KEY="..."
```

## Configuration

- `MCP_URL`: MCP endpoint (default: `http://localhost:8080/mcp`)
- `MCP_AUTH_TOKEN`: Optional bearer token for MCP
- `LLM_MODEL`: LLM model name (default: `gemini-2.0-flash`)
- `LLM_PROVIDER`: LLM provider (`openai`, `google`, `gemini`, etc.)
- `LLM_BASE_URL`: Override base URL (used as LiteLLM `api_base`)
- `OPENAI_API_KEY` / `OPENAI_BASE_URL`: OpenAI credentials and optional base URL
- `GEMINI_API_KEY` / `GOOGLE_GEMINI_BASE_URL`: Gemini credentials and optional base URL
- `ADK_MODEL`: Legacy alias for `LLM_MODEL` (still supported)

Precedence:
1) `LLM_MODEL`, `LLM_PROVIDER`, `LLM_BASE_URL` (explicit overrides)
2) Provider-specific vars (`OPENAI_*`, `GEMINI_*`, `GOOGLE_GEMINI_*`)
3) Provider inference from `LLM_MODEL` prefix/name

## Quick Run

```bash
MCP_URL=http://localhost:8080/mcp \
  k8s-graph-agent "cypher: MATCH (n) RETURN n LIMIT 5"
```

### ADK translation

```bash
MCP_URL=http://localhost:8080/mcp \
LLM_MODEL=google/gemini-2.0-flash \
  k8s-graph-agent --use-adk "Which pods are failing in kube-system?"
```

### Show result rows

```bash
LLM_MODEL=openai/gpt-5.2 \
  k8s-graph-agent --use-adk --rows 20 "Show services in namespace pyroscope"
```

## Notes
- The current CLI expects a Cypher query prefixed with `cypher:`.
- The MCP protocol uses JSON-RPC over streamable HTTP; this client handles JSON and SSE responses.
- The ADK translator fetches the `analyze_question` prompt from MCP and uses it as the model input.
- If `LLM_PROVIDER` is omitted, the provider is inferred from `LLM_MODEL` (prefix or name).

## ADK web (config-based)

Config agents live in `python/agent/adk_config/`. Each agent is a folder with `root_agent.yaml`.

To sync the core prompt into the config agent:

```bash
python python/agent/scripts/sync_adk_prompt.py
```

```bash
cd python/agent/adk_config/k8s_graph_agent
adk web --port 8000
```

Open http://localhost:8000 and select `k8s_graph_agent`.

Note: ADK config agents currently support Gemini models only. If you need OpenAI via LiteLLM,
use the code-based translator (`k8s-graph-agent --use-adk`).

## Tests

```bash
python -m unittest discover -s python/agent/tests
```

## Evaluation harness

Run NL → Cypher evaluation against a dataset (YAML or JSON):
```bash
MCP_URL=http://localhost:8080/mcp \
LLM_MODEL=openai/gpt-5.2 \
  k8s-graph-eval --dataset ./eval/questions.yaml --mode retry --runs 3 --output ./eval/results.jsonl
```

Dataset entry example:
```yaml
- id: q001
  question: "What are the pods backing DNS name litmus.qa.agoda.is?"
  deterministic: true
  expected:
    columns: [namespace, pod_name]
    rows:
      - ["litmus", "chaos-litmus-frontend-..."]
  tags: [dns, ingress, endpointslice, pod]
```
