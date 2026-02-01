# Ariadne

Ariadne turns Kubernetes state into a Memgraph property graph and lets you query it
with Cypher or natural language. This repo includes the Rust ingester + MCP server,
a Python NL -> Cypher agent with AST validation, and an eval harness.

> Ariadne is a Greek mythology nod to finding a path through complex systems.

---

## Quick start

### 1) Start Memgraph (local)

```bash
docker compose up -d
```

Memgraph listens on `localhost:7687` and Memgraph Lab on `localhost:3000`.

### 2) Run the Rust app (graph + MCP server)

```bash
CLUSTER=<cluster> \
KUBE_CONTEXT=<context> \
cargo run --release -p ariadne-app
```

The app:
- builds the graph in Memgraph
- exposes HTTP endpoints (including MCP)

### 3) Ask questions with the Python agent

```bash
cd python/agent
uv venv
uv pip install -e .
```

```bash
MCP_URL=http://localhost:8080/mcp \
LLM_MODEL=openai/gpt-5.2 \
k8s-graph-agent --use-adk "What are the pods backing DNS name litmus.qa.agoda.is?"
```

---

## Docs

- Architecture: [docs/architecture.md](docs/architecture.md)
- Development & build: [docs/development.md](docs/development.md)
- Snapshots: [docs/snapshots.md](docs/snapshots.md)
- Python agent + eval harness: [python/agent/README.md](python/agent/README.md)

## Repo structure

- `ariadne-core/` - core graph + Memgraph integration
- `ariadne-app/` - K8s ingestion + MCP + HTTP server
- `ariadne-tools/` - schema generation tooling
- `python/agent/` - ADK agent, AST validator, eval harness

---

## License

See [LICENSE](LICENSE).
