# K8s Graph Agent (Python)

This is a typed Python scaffold for the agent layer that talks to the MCP server exposed by the Rust app.
It supports direct Cypher execution via the MCP tool `graph_query` and includes an ADK
translator that uses LiteLLM for provider-agnostic LLM access.

## Demo

[ariadne.webm](https://github.com/user-attachments/assets/97b6a741-5f4c-4d01-bb22-0e5d1c6019bd)

[ariadne.webm](../../docs/demo/ariadne.webm)

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
- The ADK translator no longer depends on MCP prompts; it derives prompt context from `graph_schema` or a local prompt bundle override.
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

Bootstrap gold evals with stronger models as candidate generators:
```bash
uv run python scripts/generate_gold_candidates.py \
  --dataset eval/questions.yaml \
  --output eval/gold_candidates.json \
  --models openai/gpt-5.2-2025-12-11,gemini-3-pro-preview,claude-opus-4-5-20251101 \
  --ids e01,e02,m01,h04
```
This writes per-question candidate Cypher plus execution fingerprints so you can review consensus cases first.
Treat these model outputs as proposals, not gold truth.

After you approve and copy `reference_cypher` onto deterministic questions, materialize exact expected rows:
```bash
uv run python scripts/materialize_expected.py \
  --dataset eval/questions.yaml \
  --ids e01,e02,m01,h04
```
This executes each approved `reference_cypher` via MCP and stores `expected.columns` / `expected.rows`
back into the dataset. Use `--output` to write to a separate file instead of overwriting.

Expand the gold dataset with grounded namespace/host variants from the live graph:
```bash
uv run python scripts/expand_gold_dataset.py \
  --dataset eval/questions_gold_full.yaml \
  --output eval/questions_gold_expanded.yaml \
  --target-total 180 \
  --namespace-pool-size 20 \
  --max-namespace-variants-per-question 6 \
  --include-empty-variants
```
This writes generated questions with `group_id`, `family`, `source_question_id`, and
`generation_type` metadata so later train/dev splits can keep related variants together.
If you want a denser set with fewer empty-result variants, omit `--include-empty-variants`.

## DSPy spike

The DSPy experiment keeps the live MCP-rendered schema/connectivity fixed and optimizes the
instruction + rules layer for cheaper models.

Run a DSPy prompt-optimization pass against the gold dataset:
```bash
MCP_URL=http://127.0.0.1:8080/mcp \
OPENAI_API_KEY=... \
OPENAI_BASE_URL=... \
GEMINI_API_KEY=... \
GOOGLE_GEMINI_BASE_URL=... \
uv run python scripts/run_dspy_experiment.py \
  --dataset eval/questions_gold_full.yaml \
  --models openai/gpt-5-mini-2025-08-07,gemini-2.5-flash \
  --train-size 40 \
  --auto light \
  --num-threads 4 \
  --max-bootstrapped-demos 2 \
  --max-labeled-demos 2
```

Artifacts are written to `eval/dspy_runs/<timestamp>/` and include:
- `manifest.json` with train/dev ids and the base tunable instruction
- `<model>/report.json` with baseline vs compiled exact-match metrics
- `<model>/compiled_program/` with the serialized DSPy program

For a cheap smoke run:
```bash
uv run python scripts/run_dspy_experiment.py \
  --dataset eval/questions_gold.yaml \
  --models openai/gpt-5-mini-2025-08-07 \
  --train-size 12 \
  --num-trials 2 \
  --minibatch-size 6 \
  --output-dir eval/dspy_smoke
```

Dataset entry example:
```yaml
- id: q001
  question: "What are the pods backing DNS name litmus.qa.agoda.is?"
  deterministic: true
  reference_cypher: |
    MATCH (h:Host)-[:IsClaimedBy]->(ing:Ingress)-[:DefinesBackend]->(:IngressServiceBackend)-[:TargetsService]->(svc:Service)
    WHERE h['name'] = 'litmus.qa.agoda.is'
    RETURN svc['metadata']['name'] AS service
  expected:
    columns: [service]
    rows:
      - ["frontend"]
  tags: [dns, ingress, endpointslice, pod]
```
