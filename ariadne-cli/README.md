# ariadne-cli

Interactive GUI for querying Kubernetes cluster graphs with natural language.

`ariadne-cli` connects to a live cluster or a local snapshot, builds an
in-memory graph from Kubernetes objects, and lets you ask questions in English.
The app calls an LLM to translate your question into Cypher, runs it against the
embedded engine, and renders results in an immediate GUI powered by egui.

## Features

- **In-memory graph engine**: no external database required.
- **Immediate GUI (egui)**: ask questions and browse results in one interactive session.
- **Live or snapshot mode**: connect to a real cluster or a snapshot directory.
- **Structured LLM output**: enforces JSON output with a single `cypher` field.
- **Log hygiene**: logs go to a file by default so the UI stays clean.
- **Short-term context**: recent queries + summaries are fed back to the LLM for follow-ups.

## Install / Build

From the repo root:

```bash
cargo build -p ariadne-cli
```

## Quick Start

### Snapshot mode (recommended for local testing)

```bash
LLM_BASE_URL=... \
LLM_MODEL=... \
LLM_API_KEY=... \
cargo run -p ariadne-cli -- --cluster demo --snapshot-dir snapshot
```

### Live cluster mode

```bash
LLM_BASE_URL=... \
LLM_MODEL=... \
LLM_API_KEY=... \
cargo run -p ariadne-cli -- --cluster demo
```

## Usage

```
ariadne-cli [OPTIONS] --cluster <CLUSTER>

Options:
  --cluster <CLUSTER>             Cluster name (required)
  --kube-context <KUBE_CONTEXT>   kubeconfig context name
  --kube-namespace <NAMESPACE>    namespace filter
  --snapshot-dir <DIR>            read from snapshot directory (offline mode)
  --llm-backend <BACKEND>         LLM backend (default: openai)
  --llm-base-url <URL>            LLM base URL
  --llm-model <MODEL>             LLM model name
  --llm-api-key <KEY>             LLM API key
  --llm-timeout-secs <SECS>       LLM request timeout (default: 60)
  --llm-structured-output <BOOL>  enforce JSON schema output (default: true)
```

## GUI controls

- **Type** to enter a question.
- **Enter** or **Run Query** to run.
- **Esc** to clear the input.

## Environment variables

All flags can be provided via env vars:

```
CLUSTER
KUBE_CONTEXT
KUBE_NAMESPACE
KUBE_SNAPSHOT_DIR
KUBE_EVENT_STORE_READY_TIMEOUT_SECONDS

LLM_BACKEND
LLM_BASE_URL
LLM_MODEL
LLM_API_KEY
LLM_TIMEOUT_SECS
LLM_STRUCTURED_OUTPUT
```

### LLM backends

The CLI uses the `llm` crate. Set `LLM_BACKEND` to match your provider:

- `openai` (default)
- `anthropic`
- `ollama`
- `deepseek`
- `xai`
- `google`
- `groq`
- `aws` (Bedrock)

> Tip: some backends ignore `LLM_BASE_URL` or require their own endpoint format.

### Structured output

The CLI enforces a JSON schema response:

```json
{
  "cypher": "MATCH (n) RETURN n"
}
```

If the model does not support structured output, you will see a JSON parse error.
Use a model/backend that supports structured output, or set `LLM_STRUCTURED_OUTPUT=0`.

## Logging

By default, logs are written to a file so they do not break the GUI.

Default locations:

- **macOS**: `~/Library/Logs/ariadne-cli.log`
- **Linux**: `~/.local/state/ariadne-cli/ariadne-cli.log`

Override with:

```
ARIADNE_CLI_LOG=stderr  # log to stderr
ARIADNE_CLI_LOG=stdout  # log to stdout
ARIADNE_CLI_LOG=/path/to/file.log
```

## Snapshot mode

Use `--snapshot-dir` to load a cluster snapshot from disk. Snapshots can be
exported using the `ariadne-app` tooling (see repo docs), or you can point to
the `snapshot/` directory in this repo.

## Troubleshooting

**GUI shows garbled output / log spam**

- Make sure `ARIADNE_CLI_LOG` is not set to `stdout` or `stderr`.
- By default, logs go to a file to keep the GUI clean.

**JSON parse error from the LLM**

- Your model may not support structured output.
- Try a different model or backend (e.g., OpenAI GPT-4.1 or newer).

**Kube events missing early on**

- The event store has a startup timeout; if it is too low for your cluster, set
  `KUBE_EVENT_STORE_READY_TIMEOUT_SECONDS` (default: 4).

## Development

Run tests:

```bash
cargo test -p ariadne-cli
```

Clippy:

```bash
cargo clippy -p ariadne-cli -- -D warnings
```

Format:

```bash
cargo fmt
```
