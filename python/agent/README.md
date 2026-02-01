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

## Open WebUI bridge (OpenAI-compatible)

Run a lightweight OpenAI-compatible bridge that wraps the ADK agent:

```bash
uv run k8s-graph-openai-bridge --port 8001
```

Then run Open WebUI (recommended in a separate Python 3.11 environment):

```bash
uvx --python 3.11 open-webui@latest serve
```

In Open WebUI: Admin → Settings → Connections → add an OpenAI connection:
- Base URL: `http://localhost:8001/v1`
- API key: any non-empty string (not used by the bridge)

Bridge config:
- `K8S_GRAPH_BRIDGE_HOST` / `K8S_GRAPH_BRIDGE_PORT`: bind address (defaults: `0.0.0.0:8001`)
- `K8S_GRAPH_BRIDGE_MODEL_ID`: model id shown in the UI (default: `k8s-graph-agent`)
- `K8S_GRAPH_BRIDGE_STYLE`: `ui` (default), `simple`, or `sre`
- `K8S_GRAPH_BRIDGE_USE_ADK`: set `false` to use the prefix translator
- `K8S_GRAPH_BRIDGE_LOG_LEVEL`: logging level (default: `INFO`)
- `K8S_GRAPH_BRIDGE_CORS_ORIGINS`: comma-separated origins (default: `*`)
- `K8S_GRAPH_BRIDGE_MAX_ROWS`: max rows to render in tables (default: `25`)
- `K8S_GRAPH_BRIDGE_INCLUDE_CYPHER`: `true` to include the Cypher in the UI output
- `K8S_GRAPH_BRIDGE_CYPHER_FENCE`: `true` to wrap Cypher in ```cypher code fences (default: `true`)
- `K8S_GRAPH_BRIDGE_CYPHER_FORMAT`: `pretty` to format Cypher (default: `pretty`), `none` to keep original
- `K8S_GRAPH_BRIDGE_MAX_CELL_CHARS`: max characters per table cell (default: `120`)
- `K8S_GRAPH_BRIDGE_COMPACT_VALUES`: `true` to summarize large objects in tables (default: `true`)

Quick launch script (bridge + WebUI):
```bash
python/agent/scripts/run_webui.sh
```

Override ports:
```bash
WEBUI_PORT=3001 BRIDGE_PORT=8002 python/agent/scripts/run_webui.sh
```

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

Control parallelism with env vars (useful for high-latency models):
```bash
K8S_GRAPH_EVAL_PARALLELISM=4 \
  k8s-graph-eval --dataset ./eval/questions.yaml --mode retry --runs 3 --output ./eval/results.jsonl
```

Write logs to a file (useful for debugging concurrency issues):
```bash
K8S_GRAPH_LOG_FILE=./eval/eval_debug.log \
  k8s-graph-eval --dataset ./eval/questions.yaml --mode retry --runs 3 --output ./eval/results.jsonl
```

Or log to a directory (filename auto-generated with timestamp + pid):
```bash
K8S_GRAPH_LOG_DIR=./eval/logs \
  k8s-graph-eval --dataset ./eval/questions.yaml --mode retry --runs 3 --output ./eval/results.jsonl
```

Tune file log verbosity (defaults to INFO):
```bash
K8S_GRAPH_LOG_FILE_LEVEL=WARNING \
  K8S_GRAPH_LOG_FILE=./eval/eval_debug.log \
  k8s-graph-eval --dataset ./eval/questions.yaml --mode retry --runs 3 --output ./eval/results.jsonl
```

Script multi-model runs into a timestamped folder:
```bash
uv run python scripts/run_evals.py \
  --dataset ./eval/questions.yaml \
  --mode retry \
  --runs 3 \
  --models openai/gpt-5.2-2025-12-11,gemini-3-pro-preview
```
The script writes results to `eval/runs/<timestamp>/results_<model>.jsonl`, a `manifest.json`,
and per-model logs (`eval_<model>.log`) unless you override `K8S_GRAPH_LOG_FILE`/`K8S_GRAPH_LOG_DIR`.

Use the built-in full list:
```bash
uv run python scripts/run_evals.py --preset all
```

Summarize a run folder as markdown:
```bash
uv run python scripts/summarize_eval_run.py --run-dir eval/runs/<timestamp>
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
