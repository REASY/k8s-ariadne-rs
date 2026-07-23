# Ariadne

Ariadne is a Kubernetes graph MCP for coding agents. It ingests the cluster state into a property graph and exposes three deterministic tools:

- `graph_query` for read-only Cypher
- `graph_schema` for schema discovery
- `graph_health` for freshness and readiness

The Rust MCP server is the primary product surface. The Rust CLI and Python agent in this repo are local harnesses for debugging, evaluation, and experimentation.

> Short version: `kubectl` lists objects. Ariadne answers relationship questions.

---

## TL;DR

- **MCP-first**: Ariadne is designed to plug into coding agents, not replace them.
- **Topology over inventory**: Ariadne is strongest on multi-hop Kubernetes questions such as `Host -> Ingress -> IngressServiceBackend -> Service -> EndpointSlice -> Endpoint -> EndpointAddress -> Pod`.
- **Deterministic execution**: the model generates Cypher; the graph executes modeled relationships.
- **Compact defaults**: `graph_schema()` and `graph_health()` default to compact, model-friendly responses to reduce token overhead.
- **Shared validation**: `ariadne-core` owns Cypher validation and shared query-issue classification used by both the MCP server and local tooling.

---

## Demo

https://github.com/user-attachments/assets/c73a521e-612d-4cad-9dc2-a22acd431baf

[mcp.mp4](docs/demo/mcp.mp4)

## Why Ariadne

Most "LLM + Kubernetes" workflows make the model reason over raw JSON or YAML:

```text
Kubernetes API -> giant object dump -> LLM context -> ad hoc joins in code
```

That works for direct inventory, but it breaks down on relationship-heavy questions:

- which pods back a host?
- which services have no endpoint slices?
- which pods claim PVCs whose PV has no storage class?
- which deployment owns these pods through replica sets?

Those are graph questions. Ariadne gives the agent explicit edges instead of forcing it to reconstruct joins with Python, shell, or `jq`.

## When Ariadne Wins

Ariadne is strongest when the answer depends on multi-hop traversals across resource kinds:

- `Host -> Ingress -> IngressServiceBackend -> Service -> EndpointSlice -> Endpoint -> EndpointAddress -> Pod`
- `Service -> EndpointSlice -> Endpoint -> EndpointAddress -> Pod`
- `Deployment -> ReplicaSet -> Pod`
- `Pod -> PersistentVolumeClaim -> PersistentVolume -> StorageClass`
- negative graph queries such as "resources with no backing edges"

These are literal graph paths. Ariadne derives helper nodes such as `Host`, `IngressServiceBackend`,
`Endpoint`, `EndpointAddress`, and `Container` from raw Kubernetes objects during graph construction.

For these questions, Ariadne helps with:

- **correctness**: joins are encoded as graph edges, not improvised by the agent
- **lower agent-side complexity**: one declarative query replaces bespoke glue code
- **scaling with hop depth**: adding another relationship hop extends the traversal instead of rewriting the whole approach

## When `kubectl` Is Fine

Ariadne is not meant to replace `kubectl` for every cluster question. Plain read-only `kubectl` is often enough for:

- listing pods, services, ingresses, or PVCs
- top namespaces by pod count
- straightforward spec/status checks
- direct inventory dumps with little or no joining

The goal is not to out-`kubectl` `kubectl`. The goal is to give agents a safer query substrate for relationship-heavy Kubernetes questions.

## Core flow

```text
User question
     ↓
Coding agent
     ↓
Query issue loop
(static validation + repairable execution feedback)
     ↓
GraphDB (Memgraph)
     ↓
Deterministic execution over the current graph state
```

Key idea:

- the model does not need raw cluster state in context
- it generates a query against a modeled graph
- Ariadne validates the query and returns structured results or structured repair feedback

---

## Quick start

### 1) Start Memgraph

```bash
docker compose up -d
```

Memgraph listens on `localhost:7687` and Memgraph Lab on `localhost:3000`.

### 2) Run the MCP server

```bash
CLUSTER=<cluster> \
KUBE_CONTEXT=<context> \
cargo run --release -p ariadne-mcp
```

Literal Kubernetes environment-variable values are redacted from graphs and exported snapshots by
default. To retain them for configuration queries, explicitly opt out:

```bash
ARIADNE_REDACT_ENV_VALUES=false \
CLUSTER=<cluster> \
KUBE_CONTEXT=<context> \
cargo run --release -p ariadne-mcp
```

Environment-variable names and `valueFrom` references remain queryable in both modes. Other
sensitive-field redaction is unaffected by this setting.

By default this starts an HTTP MCP server on:

```text
http://127.0.0.1:8080/mcp
```

The main tools are:

- `graph_query`: execute read-only Cypher
- `graph_schema`: compact schema by default; request `format = "structured"` for full machine-readable details
- `graph_health`: compact freshness/status by default; request `detail = "full"` or `detail = "debug"` for full diagnostics

### 3) Connect a coding agent

Point your coding agent at the MCP endpoint above. If you want reusable agent guidance, use [AGENTS.template.md](AGENTS.template.md) as a template in the consuming workspace, not as an active instruction file in this repo.

### 4) Optional local tooling

The repo also includes local harnesses:

- `ariadne-cli`: local GUI/debug client
- `python/agent`: evaluation and experimentation layer for NL -> Cypher workflows

Example Python agent setup:

```bash
cd python/agent
uv venv
uv sync
```

```bash
MCP_URL=http://localhost:8080/mcp \
LLM_MODEL=openai/gpt-5.2 \
k8s-graph-agent --use-adk "What are the pods backing DNS name litmus.qa.agoda.is?"
```

---

## Docs

- Architecture: [docs/architecture.md](docs/architecture.md)
- MCP tool contract: [docs/specs/mcp_tools_v1.md](docs/specs/mcp_tools_v1.md)
- Development & build: [docs/development.md](docs/development.md)
- Snapshots: [docs/snapshots.md](docs/snapshots.md)
- Python agent + eval harness: [python/agent/README.md](python/agent/README.md)
- CLI: [ariadne-cli/README.md](ariadne-cli/README.md)

## Repo structure

- `ariadne-core/` - kube clients, snapshot resolver, shared graph model/backends, validation, and query-issue classification
- `ariadne-mcp/` - MCP + HTTP server that wires `ariadne-core` into the primary product surface
- `ariadne-cli/` - local GUI/debug harness with optional Memgraph or in-memory execution
- `ariadne-tools/` - schema generation tooling used by `graph_schema`
- `ariadne-cypher/` - Cypher parser, AST, and semantic validation
- `python/agent/` - MCP client, agent experiments, eval harness, and structured-schema consumers

---

## License

See [LICENSE](LICENSE).
