# Ariadne MCP Tools V1 Specification

## Purpose

Define the V1 tool surface for `ariadne-mcp` as a standalone MCP server optimized for consumption by coding agents (Claude Code, Codex, Cursor, etc.) and custom MCP clients.

The guiding principle:

> Ariadne is not a chat agent. It is the best Kubernetes graph MCP that agents want to call.

---

## Non-goals

- Natural language parsing inside MCP. Agents own intent; MCP owns data.
- Multi-step reasoning or diagnosis. MCP returns facts, not conclusions.
- Prompt engineering for downstream LLMs. No `analyze_question` prompt or equivalent.
- Cursor-based pagination. Agents should write narrower queries, not paginate broad ones.

---

## Design rules

1. **Deterministic**: same input produces same output.
2. **Machine-readable everything**: every response is structured JSON, every error includes classification fields.
3. **Small surface**: three tools in V1. Each has a clear, non-overlapping purpose.
4. **Tool descriptions are documentation**: agents choose tools based on the `description` field. Make them specific enough that an agent can use the tool correctly without reading any other docs.
5. **Prefer bounded results**: avoid dumping unbounded data into an agent's context when practical. Hard enforcement is deferred from V1.
6. **Instructions field over prompts**: the `ServerInfo.instructions` field is the single place for short guidance. No MCP prompts.

---

## Naming convention

Tool names use **underscores**: `graph_query`, `graph_schema`, `graph_health`. This supersedes the dotted names (`graph.query`) used in the earlier `docs/specs/agent-mcp-integration.md`. Dots in MCP tool names are not universally supported across frameworks and transports. Underscores are safe everywhere.

The earlier integration spec should be updated to match once V1 ships.

---

## Backend scope

V1 targets **Memgraph as the production backend**. The in-memory backend (`InMemoryBackend`) is dev/test-only and is not part of the V1 correctness contract. Specifically:

- Column ordering guarantees apply to the Memgraph backend only (bolt protocol provides authoritative column vectors).
- Readiness probes (`RETURN 1`) target Memgraph.
- Performance characteristics (query execution behavior, response-size control, duration_ms) are validated against Memgraph.

The in-memory backend should continue to work for local development and unit tests, but behavioral differences (e.g., alphabetical column ordering from the default `execute_query_with_columns` implementation) are acceptable and not treated as spec violations.

---

## V1 tool surface

### 1. `graph_query`

Execute a read-only Cypher query against the Kubernetes graph.

#### Request

```json
{
  "query": "MATCH (p:Pod)-[:RunsOn]->(n:Node) WHERE p['metadata']['namespace'] = $ns RETURN p['metadata']['name'], n['metadata']['name']",
  "params": { "ns": "kube-system" },
  "limit": 100
}
```

| Field    | Type                          | Required | Default | Description                                      |
|----------|-------------------------------|----------|---------|--------------------------------------------------|
| `query`  | `string`                      | yes      | —       | Cypher query. Must be read-only.                                                    |
| `params` | `object (string → any)`       | no       | `null`  | Parameterized query bindings (`$name` syntax).   |
| `limit`  | `integer`                     | no       | `100`   | Response-size cap applied **after execution**. Range: 1–1000. Does not bound backend work. |

#### Success response

```json
{
  "columns": ["p['metadata']['name']", "n['metadata']['name']"],
  "rows": [
    ["coredns-abc", "node-1"],
    ["kube-proxy-xyz", "node-2"]
  ],
  "row_count": 2,
  "truncated": false,
  "duration_ms": 12
}
```

| Field         | Type       | Description                                                        |
|---------------|------------|--------------------------------------------------------------------|
| `columns`     | `string[]` | Column names in Cypher projection order (from bolt protocol on Memgraph). |
| `rows`        | `any[][]`  | Result rows. Each row is an array matching `columns` order.        |
| `row_count`   | `integer`  | Number of rows returned after any response truncation.             |
| `truncated`   | `boolean`  | `true` if the MCP layer dropped additional rows from the response due to the top-level `limit`. This does **not** imply bounded backend execution. |
| `duration_ms` | `integer`  | Server-side query execution time in milliseconds.                  |

#### Error response

Errors are returned as MCP `ErrorData` with structured `data` payload:

```json
{
  "code": -32602,
  "message": "Unbound variable: ns.",
  "data": {
    "kind": "scope_error",
    "retryable": false,
    "repairable": true,
    "source": "backend",
    "cypher": "MATCH (p:Pod) WHERE ns.name = 'default' RETURN p"
  }
}
```

| `data` field  | Type      | Description                                                                      |
|---------------|-----------|----------------------------------------------------------------------------------|
| `kind`        | `string`  | One of: `parse_error`, `syntax_error`, `semantic_error`, `schema_error`, `scope_error`, `parameter_error`, `engine_limitation`, `backend_error`. |
| `retryable`   | `boolean` | `true` if the same query may succeed on retry (transient backend issue).         |
| `repairable`  | `boolean` | `true` if the agent can fix the query and resubmit (e.g., schema mismatch).     |
| `source`      | `string`  | `"validator"` (static pre-execution check) or `"backend"` (runtime failure). These are stable protocol values, not implementation names — `"backend"` applies whether the backend is Memgraph or in-memory. |
| `cypher`      | `string`  | The original query that failed, for agent self-correction feedback loops.        |

Error classification is powered by the shared `QueryIssue` model (`ariadne-core/src/query_issue.rs`).

#### Validation pipeline

Queries pass through two stages before results are returned:

1. **Static validation** (before execution via `validate_cypher` in `ariadne-core/src/cypher_validation.rs`):
   - **Parse**: tree-sitter parse of Cypher syntax. Catches `parse_error`.
   - **Read-only semantics**: `validate_query(&query, ValidationMode::ReadOnly)` rejects write statements. Catches `semantic_error`.
   - **Schema checks**: `validate_schema(&query)` verifies node labels exist as known `ResourceType` values and relationship types/directions match the graph schema (`graph_schema::is_known_edge`). Catches `schema_error`.
   - **Not checked statically**: property names, property types, variable scoping. These are caught at execution time.

2. **Execution** (Memgraph or in-memory backend): catches `scope_error`, `syntax_error` (backend parse divergence), `parameter_error`, `engine_limitation`, `backend_error`.

Both stages produce the same `QueryIssue` shape. The MCP layer maps issues to `ErrorData` via `query_issue_to_mcp` (`ariadne-mcp/src/kube_tool.rs`).

After execution, the MCP layer may apply **response-size control** using the optional top-level `limit`: it truncates returned rows for transport/context safety and sets `truncated = true` when rows were omitted. This happens **after** the backend has already produced the full result.

#### Tool description

```
Execute a read-only Cypher query against the Kubernetes graph. Returns rows, columns,
row count, truncation status, and execution time. The optional top-level `limit`
caps response size after execution (default: 100, max: 1000) for transport/context
safety only. Prefer narrow queries and add Cypher `LIMIT` when exploring large result
sets. Errors include structured classification with `kind`
(schema_error, scope_error, etc.) and `repairable`/`retryable` flags for self-correction.
Use `graph_schema` only when labels, properties, or traversal directions are uncertain.
```

---

### 2. `graph_schema`

Return the graph schema in either compact or structured form.

#### Request

```json
{
  "format": "compact"
}
```

| Field    | Type     | Required | Default     | Description |
|----------|----------|----------|-------------|-------------|
| `format` | `string` | no       | `"compact"` | `"compact"` returns a text schema optimized for model consumption. `"structured"` returns the full machine-readable schema. |

#### Response

Default compact response:

```json
{
  "format": "compact",
  "schema_version": "sha256:a1b2c3d4e5f6",
  "schema_text": "# Nodes\nPod(metadata:MAP, spec:MAP, status:MAP)\nContainer(container_type:STRING, metadata:MAP, spec:MAP)\nNode(metadata:MAP, spec:MAP, status:MAP)\n\n# Edges\nEach bracket item expands to an independent directed edge.\nDeployment-[Manages]->[Deployment, ReplicaSet]\nPod-[RunsOn]->Node\nService-[Manages]->EndpointSlice",
  "example_patterns": [
    "MATCH (d:Deployment)-[:Manages]->(rs:ReplicaSet)-[:Manages]->(p:Pod) WHERE d['metadata']['name'] = $name RETURN p['metadata']['name'] AS pod_name LIMIT 25",
    "MATCH (h:Host)-[:IsClaimedBy]->(i:Ingress)-[:DefinesBackend]->(b:IngressServiceBackend)-[:TargetsService]->(s:Service) WHERE h['name'] = $hostname RETURN s['metadata']['name'] AS service_name, s['metadata']['namespace'] AS service_namespace LIMIT 25",
    "MATCH (p:Pod)-[:RunsOn]->(n:Node) WHERE p['metadata']['namespace'] = $ns RETURN n['metadata']['name'] AS node_name, count(p) AS pod_count ORDER BY pod_count DESC LIMIT 25"
  ]
}
```

Structured response (`format = "structured"`):

```json
{
  "format": "structured",
  "schema_version": "sha256:a1b2c3d4e5f6",
  "server_version": "0.8.1",
  "node_labels": [
    {
      "label": "Pod",
      "properties": [
        { "name": "metadata", "type": "MAP" },
        { "name": "spec", "type": "MAP" },
        { "name": "status", "type": "MAP" }
      ]
    }
  ],
  "relationship_types": [
    { "from": "Pod", "edge": "RunsOn", "to": "Node" }
  ],
  "example_patterns": [
    "MATCH (p:Pod)-[:RunsOn]->(n:Node) WHERE p['metadata']['namespace'] = $ns RETURN n['metadata']['name'] AS node_name, count(p) AS pod_count ORDER BY pod_count DESC LIMIT 25"
  ]
}
```

Common fields:

| Field              | Type       | Description |
|--------------------|------------|-------------|
| `format`           | `string`   | `"compact"` or `"structured"`. |
| `schema_version`   | `string`   | Content hash of the schema (truncated SHA-256, prefixed `sha256:`). Clients can cache schema responses and skip re-fetching when this value is stable. |
| `example_patterns` | `string[]` | Representative Cypher patterns showing common traversals, parameterization, and aggregation. |

Compact-only fields:

| Field          | Type     | Description |
|----------------|----------|-------------|
| `schema_text`  | `string` | Compact text schema optimized for model consumption. |

Structured-only fields:

| Field                | Type     | Description |
|----------------------|----------|-------------|
| `server_version`     | `string` | Server build version string. Informational only. |
| `node_labels`        | `array`  | All node labels in the graph with their top-level properties and normalized types. |
| `relationship_types` | `array`  | All directed relationships as `{from, edge, to}` triples. |

#### Implementation notes

- `generate_schema()` in `ariadne-tools` is the source of truth for node labels and top-level properties. Structured responses post-process that output to normalize types, remove properties stripped before storage, and add any missing schema entries such as `PersistentVolumeClaim`.
- Structured schema types normalize raw JSON Schema references (`#/$defs/...`, `#/definitions/...`) and array-wrapped references into simple values like `MAP`, `[MAP]`, `STRING`, and `DATETIME_UTC`.
- Structured `relationship_types` come from `graph_relationships()`, then are filtered so only labels that actually exist in `node_labels` are returned. This prevents edges for absent labels from leaking into the schema response.
- Compact mode is derived from the structured schema. It strips low-value fields such as `apiVersion` and `kind`, groups edges by `(from, edge)` only, and emits a short explanatory sentence before grouped edges.
- `example_patterns` are static code-maintained patterns. They should remain narrow, parameterized where appropriate, and use explicit `AS` aliases for non-trivial `RETURN` expressions.
- `schema_version` is computed from canonically sorted schema data. The hashing function sorts node labels, sorts each label's properties, and sorts relationship triples before hashing, so the value is invariant to caller ordering.
- `server_version` is included only in structured mode.

#### Tool description

```
Return the Kubernetes graph schema. By default this returns a compact text view
optimized for model consumption. Pass `format = "structured"` for the full
machine-readable schema with node labels, properties, and relationship types.
```

---

### 3. `graph_health`

Return server health, graph scope, graph dimensions, and freshness state.

#### Request

```json
{
  "detail": "compact"
}
```

| Field    | Type     | Required | Default     | Description |
|----------|----------|----------|-------------|-------------|
| `detail` | `string` | no       | `"compact"` | `"compact"` returns a small freshness/status summary. `"full"` and `"debug"` return the full diagnostic payload with backend probe details, sync state, rebuild state, and version. |

#### Response

Default compact response:

```json
{
  "detail": "compact",
  "cluster": "production-us-east-1",
  "mode": "live",
  "scope": {
    "kind": "cluster",
    "namespace": null
  },
  "observed_at": "2026-03-27T06:35:10Z",
  "ready": true,
  "data_as_of": "2026-03-27T06:35:05Z",
  "node_count": 1847,
  "edge_count": 5231,
  "sync_lag_ms": 5000,
  "coverage": {
    "degraded_resource_kinds": []
  }
}
```

Full response (`detail = "full"`):

```json
{
  "detail": "full",
  "cluster": "production-us-east-1",
  "backend": "memgraph",
  "mode": "live",
  "scope": {
    "kind": "cluster",
    "namespace": null
  },
  "observed_at": "2026-03-27T06:35:10Z",
  "ready": true,
  "backend_probe_ok": true,
  "backend_probe_duration_ms": 3,
  "data_as_of": "2026-03-27T06:35:05Z",
  "node_count": 1847,
  "edge_count": 5231,
  "version": "0.8.1",
  "sync": {
    "loop_alive": true,
    "poll_interval_seconds": 5,
    "total_attempts": 142,
    "total_successes": 140,
    "last_attempt_at": "2026-03-27T06:35:05Z",
    "last_success_at": "2026-03-27T06:35:05Z",
    "last_write_at": "2026-03-27T06:33:40Z",
    "lag_ms": 5000,
    "last_attempt_duration_ms": 482,
    "last_fetch_duration_ms": 310,
    "last_diff_duration_ms": 7,
    "last_write_duration_ms": null,
    "last_error": null,
    "last_error_at": null,
    "consecutive_errors": 0,
    "last_diff": {
      "added_nodes": 0,
      "removed_nodes": 0,
      "modified_nodes": 0,
      "added_edges": 0,
      "removed_edges": 0
    }
  },
  "rebuild": null,
  "coverage": {
    "degraded_resource_kinds": []
  }
}
```

Snapshot full response:

```json
{
  "detail": "full",
  "cluster": "snapshot-production-us-east-1",
  "backend": "memgraph",
  "mode": "snapshot",
  "scope": {
    "kind": "namespace",
    "namespace": "kube-system"
  },
  "observed_at": "2026-03-28T03:00:00Z",
  "ready": true,
  "backend_probe_ok": true,
  "backend_probe_duration_ms": 2,
  "data_as_of": "2026-03-27T23:58:41Z",
  "node_count": 412,
  "edge_count": 1198,
  "version": "0.8.1",
  "sync": null,
  "rebuild": null,
  "coverage": {
    "degraded_resource_kinds": []
  }
}
```

Top-level fields:

| Field                       | Type              | Description |
|----------------------------|-------------------|-------------|
| `detail`                   | `string`          | `"compact"`, `"full"`, or `"debug"`. |
| `cluster`                  | `string`          | Cluster name as passed via `--cluster` / `CLUSTER`. |
| `mode`                     | `string`          | `"live"` (connected to cluster with sync loop) or `"snapshot"` (loaded from directory). |
| `scope`                    | `object\|null`    | Graph scope. In live mode this is derived from runtime config (`KUBE_NAMESPACE`). In snapshot mode it comes from the snapshot manifest. In namespace mode, this describes the filter for namespaced resources only; cluster-scoped resources may still be present. |
| `observed_at`              | `string`          | ISO 8601 UTC timestamp of when this `graph_health` payload was assembled. |
| `ready`                    | `boolean`         | Query-readiness summary. `true` if the graph has been populated at least once and the backend probe succeeds. A system can be `ready: true` but still degraded. |
| `data_as_of`               | `string\|null`    | ISO 8601 UTC timestamp of when the graph contents were last successfully observed from the source. In live mode this equals `max(sync.last_success_at, rebuild.last_success_at)`. In snapshot mode this equals the snapshot manifest `captured_at`. |
| `node_count`               | `integer`         | Total graph nodes. |
| `edge_count`               | `integer`         | Total graph edges. |
| `sync_lag_ms`              | `integer\|null`   | Compact-mode freshness lag in milliseconds. Present only in compact responses and only in live mode. |
| `coverage`                 | `object`          | Resource-coverage state. See table below. |

Additional fields in `detail = "full"` or `detail = "debug"`:

| Field                       | Type              | Description |
|----------------------------|-------------------|-------------|
| `backend`                  | `string`          | `"memgraph"` or `"in-memory"`. |
| `backend_probe_ok`         | `boolean`         | Result of a lightweight backend probe (`RETURN 1` for Memgraph, no-op for in-memory). |
| `backend_probe_duration_ms`| `integer`         | Probe duration in milliseconds. |
| `version`                  | `string`          | Server build version string. |
| `sync`                     | `object\|null`    | Detailed source-sync state. Present in live mode, `null` in snapshot mode. |
| `rebuild`                  | `object\|null`    | Optional full-rebuild loop state. Present when `ENABLE_FULL_REBUILD_LOOP` is enabled, otherwise `null`. |

`scope` fields:

| Field         | Type            | Description |
|---------------|-----------------|-------------|
| `kind`        | `string`        | `"cluster"` or `"namespace"`. `"namespace"` means namespaced resources are filtered to one namespace, but cluster-scoped resources may still be included for graph context. |
| `namespace`   | `string\|null`  | Namespace name when `kind = "namespace"`, else `null`. This filters namespaced resources only; it does not imply a strictly namespace-only graph. |

`sync` fields:

| Field                         | Type             | Description |
|------------------------------|------------------|-------------|
| `loop_alive`                 | `boolean`        | `true` if the sync loop task is still running. |
| `poll_interval_seconds`      | `integer`        | Poll interval for the source sync loop. |
| `total_attempts`             | `integer`        | Number of sync attempts observed since startup, including the initial load. |
| `total_successes`            | `integer`        | Number of successful sync completions since startup, including the initial load. |
| `last_attempt_at`            | `string\|null`   | ISO 8601 UTC timestamp of the most recent sync attempt, whether it succeeded or failed. |
| `last_success_at`            | `string\|null`   | ISO 8601 UTC timestamp of the most recent successful sync completion. A no-change poll still counts as a success. |
| `last_write_at`              | `string\|null`   | ISO 8601 UTC timestamp of the most recent successful graph write (`create()` or non-empty diff `update()`). |
| `lag_ms`                     | `integer\|null`  | Server-computed lag in milliseconds: `observed_at - last_success_at`. |
| `last_attempt_duration_ms`   | `integer\|null`  | End-to-end duration of the last sync attempt. |
| `last_fetch_duration_ms`     | `integer\|null`  | Duration of the source fetch phase for the last attempt. |
| `last_diff_duration_ms`      | `integer\|null`  | Duration of the diff-computation phase for the last attempt. |
| `last_write_duration_ms`     | `integer\|null`  | Duration of the graph-write phase for the last attempt. |
| `last_error`                 | `object\|null`   | Most recent sync error. See table below. |
| `last_error_at`              | `string\|null`   | ISO 8601 UTC timestamp of the most recent sync error. |
| `consecutive_errors`         | `integer`        | Number of failed sync attempts since the last success. Resets to `0` on success. |
| `last_diff`                  | `object\|null`   | Summary of the most recent successful diff-loop iteration. |

`sync.last_error` fields:

| Field       | Type     | Description |
|-------------|----------|-------------|
| `stage`     | `string` | One of: `initial_load`, `kube_fetch`, `diff`, `graph_write`. Backend probe failures are reported via the top-level probe fields, not here. |
| `message`   | `string` | Human-readable error message. |

`sync.last_diff` fields:

| Field            | Type      | Description |
|-----------------|-----------|-------------|
| `added_nodes`    | `integer` | Nodes added in the last successful diff-loop iteration. |
| `removed_nodes`  | `integer` | Nodes removed in the last successful diff-loop iteration. |
| `modified_nodes` | `integer` | Nodes modified in the last successful diff-loop iteration. |
| `added_edges`    | `integer` | Edges added in the last successful diff-loop iteration. |
| `removed_edges`  | `integer` | Edges removed in the last successful diff-loop iteration. |

`rebuild` fields:

| Field                   | Type             | Description |
|------------------------|------------------|-------------|
| `loop_alive`           | `boolean`        | `true` if the rebuild fallback loop task is still running. |
| `poll_interval_seconds`| `integer`        | Poll interval for the rebuild loop. |
| `total_attempts`       | `integer`        | Number of rebuild attempts since startup. |
| `total_successes`      | `integer`        | Number of successful rebuild attempts since startup. |
| `last_attempt_at`      | `string\|null`   | ISO 8601 UTC timestamp of the most recent rebuild attempt. |
| `last_success_at`      | `string\|null`   | ISO 8601 UTC timestamp of the most recent successful rebuild attempt. |
| `last_duration_ms`     | `integer\|null`  | Duration of the most recent rebuild attempt. |
| `last_error`           | `object\|null`   | Most recent rebuild error. See table below. |
| `last_error_at`        | `string\|null`   | ISO 8601 UTC timestamp of the most recent rebuild error. |
| `consecutive_errors`   | `integer`        | Number of failed rebuild attempts since the last successful rebuild. |

`rebuild.last_error` fields:

| Field       | Type     | Description |
|-------------|----------|-------------|
| `stage`     | `string` | One of: `state_read`, `graph_write`. |
| `message`   | `string` | Human-readable error message. |

`coverage` fields:

| Field                       | Type       | Description |
|----------------------------|------------|-------------|
| `degraded_resource_kinds`  | `string[]` | Resource kinds that are currently known to be incomplete, skipped, or degraded due to RBAC denial or best-effort fetch fallback. |

#### Implementation notes

- `graph_health()` defaults to the compact summary. `detail = "full"` and `detail = "debug"` currently return the same verbose payload; `debug` is reserved for future expansion.
- Compact mode intentionally omits backend probe fields, version, and nested sync/rebuild state. It keeps only the fields most likely to matter for routine agent turns.
- `cluster`: from the runtime cluster name.
- `backend`: from the configured graph backend kind.
- `mode`: `"snapshot"` if `KUBE_SNAPSHOT_DIR` is set, else `"live"`.
- `scope`: in live mode, derived from runtime config. In snapshot mode, read from `snapshot_manifest.json`.
- `observed_at`: current UTC timestamp at the moment the payload is assembled.
- `backend_probe_ok` / `backend_probe_duration_ms`: measured by a lightweight probe (`RETURN 1` for Memgraph, no-op for in-memory). These are exposed in full/debug mode.
- `ready`: computed summary. In live mode: `ready = sync.last_success_at.is_some() && backend_probe_ok`. In snapshot mode: `ready = initial_load_succeeded && backend_probe_ok`.
- `sync` is the source-sync domain only. The initial load counts as an attempt and a success. Snapshot mode does not expose a sync loop, so `sync` is `null` there.
- `sync.last_success_at` advances on any successful sync iteration, even when the diff is empty. `last_write_at` advances only when a graph write occurred.
- Compact `sync_lag_ms` and verbose `sync.lag_ms` are both computed server-side as `observed_at - sync.last_success_at`.
- `rebuild` tracks the optional full rebuild loop when `ENABLE_FULL_REBUILD_LOOP=true`. A successful rebuild re-fetches from the K8s API and advances `data_as_of`, but it does not advance `sync.last_success_at`.
- `data_as_of`: in live mode, equals `max(sync.last_success_at, rebuild.last_success_at)`. In snapshot mode, equals the snapshot manifest `captured_at`.
- `snapshot_manifest.json` carries snapshot provenance:

  ```json
  {
    "captured_at": "2026-03-27T23:58:41Z",
    "scope": { "kind": "namespace", "namespace": "kube-system" }
  }
  ```

- `cluster.json` remains the raw serialized `Cluster` object. The loader reads `cluster.json` for cluster payload and `snapshot_manifest.json` for provenance.
- `coverage.degraded_resource_kinds` is the union of resource kinds skipped due to RBAC/access checks and resource kinds whose fetches were degraded to empty on error.

#### Tool description

```
Return Kubernetes graph health. By default this returns a compact freshness/status
summary optimized for model consumption. Pass `detail = "full"` or `detail = "debug"`
for the full diagnostic payload with backend probe details, sync and rebuild state,
and version.
```

---

## Removals in V1

### Drop: `analyze_question` MCP prompt

**Current**: `list_prompts` and `get_prompt` handlers in `kube_tool.rs:260-311` expose an `analyze_question` prompt that bundles the full system prompt + schema + user question.

**Why remove**: MCP prompts are not used by major coding agents. Claude Code, Codex, and Cursor invoke tools, not prompts. The prompt also injects a large static text block (~3K tokens) that duplicates what `graph_schema` provides in structured form.

**Action**: Remove `list_prompts`, `get_prompt` handlers, the `PROMPT_CACHE` static, and the `current_prompt()` function from `kube_tool.rs`. Remove `enable_prompts()` from `ServerCapabilities`.

### Drop: MCP resources capability

**Current**: `ServerCapabilities` advertises `enable_resources()` at `kube_tool.rs:242` but no resource handlers are implemented.

**Why remove**: Advertising capabilities without implementing them is a broken contract. If a client enumerates resources, it gets nothing.

**Action**: Remove `enable_resources()` from `ServerCapabilities::builder()`.

### Rename: `execute_cypher_query` → `graph_query`

**Current**: `execute_cypher_query` (imperative, verbose).

**Why rename**: Prefer stable, obvious, noun-based names consistent across the tool surface. `graph_query` / `graph_schema` / `graph_health` form a coherent family.

### Rename: `get_graph_schema` → `graph_schema`

Same rationale.

---

## `ServerInfo.instructions` field

Replace the current instructions (`kube_tool.rs:229-236`) with a concise version:

```
Read-only MCP server for Kubernetes cluster {cluster_name}.
Three tools: graph_query, graph_schema, graph_health.
Use graph_query directly when the query shape is already known. graph_schema defaults to a compact text schema; request format=structured only when full machine-readable details are needed. graph_health defaults to a compact freshness/status summary; request detail=full or detail=debug only when the extra diagnostics matter.
All queries are read-only Cypher. Prefer LIMIT in exploratory queries. Use parameterized queries ($var) when filtering. Alias non-trivial RETURN expressions with AS to keep result columns unique and stable.
Errors include structured classification; check `repairable` to decide whether to fix and retry.
```

Keep it under 4 lines. Agents parse tool descriptions; the instructions field is a fallback orientation.

---

## Changes to `KubeTool` struct

The V1 implementation keeps the following state on `KubeTool`:

```rust
#[derive(Debug, Clone)]
pub struct KubeTool {
    cluster_name: String,
    backend_kind: String,           // "memgraph" | "in-memory"
    mode: String,                   // "live" | "snapshot"
    scope: Option<GraphScope>,
    snapshot_captured_at: Option<String>,
    cluster_state: SharedClusterState,
    graph: Arc<dyn GraphBackend>,
    initial_load_succeeded: Arc<AtomicBool>,
    source_sync: Arc<Mutex<SyncHealth>>,
    rebuild: Arc<Mutex<Option<RebuildHealth>>>,
    coverage: SharedCoverage,
    tool_router: ToolRouter<Self>,
}
```

- `SharedClusterState` is needed for `graph_health` (node/edge counts, `data_as_of`).
- `backend_kind` and `mode` are static configuration strings passed at construction time from `main.rs`.
- `scope` stores the live runtime scope or snapshot-provenance scope when available. In live namespace mode, this means namespaced resources are filtered to that namespace while cluster-scoped resources remain present.
- `snapshot_captured_at` carries the snapshot manifest timestamp used for `graph_health.data_as_of` in snapshot mode.
- `initial_load_succeeded` is used for `ready` in both live and snapshot mode.
- `source_sync` stores source observation timestamps, durations, errors, counters, and the last diff summary. It is updated by the initial source load and by each diff-loop iteration.
- `rebuild` stores the optional full-rebuild loop status when `ENABLE_FULL_REBUILD_LOOP` is enabled.
- `coverage` stores degraded or skipped resource kinds for `graph_health.coverage`.

---

## `GraphBackend` trait changes

`GraphBackend` supports a separate column-aware query path alongside the original row-only method.

### Additive method for column metadata

The workspace uses plain `serde_json = "1"` (`Cargo.toml:50`) without the `preserve_order` feature, so `serde_json::Map` is backed by `BTreeMap` — key iteration order is alphabetical, not insertion order. This means the MCP layer **cannot** reliably reconstruct column projection order from row keys alone, even though the Memgraph backend inserts keys in bolt column order at `ariadne-core/src/backends/memgraph.rs:717`.

`GraphBackend` uses an **additive** method with a default implementation:

```rust
#[async_trait]
pub trait GraphBackend: Send + Sync + std::fmt::Debug {
    // Existing methods unchanged
    async fn create(&self, cluster_state: SharedClusterState) -> Result<()>;
    async fn update(&self, diff: ClusterStateDiff) -> Result<()>;
    async fn execute_query(
        &self,
        query: String,
        params: Option<HashMap<String, Value>>,
    ) -> Result<Vec<Value>>;
    async fn shutdown(&self);

    // New: returns (columns_in_projection_order, rows)
    async fn execute_query_with_columns(
        &self,
        query: String,
        params: Option<HashMap<String, Value>>,
    ) -> Result<(Vec<String>, Vec<Value>)> {
        let rows = self.execute_query(query, params).await?;
        let columns = rows.first()
            .and_then(|v| v.as_object())
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default();
        Ok((columns, rows))
    }
}
```

This is **not a breaking change** — the default implementation preserves existing behavior for all consumers. `MemgraphAsync` overrides it to return the bolt column vector from `connection.execute()` (`ariadne-core/src/backends/memgraph.rs:390`).

The MCP layer calls `execute_query_with_columns` and uses the returned column vector directly:

```rust
async fn graph_query(&self, request: GraphQueryRequest) -> Result<CallToolResult, ErrorData> {
    let response_limit = request.limit.unwrap_or(100).min(1000);
    let start = Instant::now();
    let (columns, raw_rows) = self
        .graph
        .execute_query_with_columns(request.query, request.params)
        .await?;
    let duration_ms = start.elapsed().as_millis() as u64;

    let rows = to_columnar(&raw_rows, &columns); // [{k:v}...] → [[v]...] in column order
    let truncated = rows.len() > response_limit as usize;
    let rows: Vec<Vec<Value>> = rows.into_iter().take(response_limit as usize).collect();
    let row_count = rows.len();

    // ...serialize response with columns, rows, row_count, truncated, duration_ms
}
```

**Row conversion**: `to_columnar` looks up each column name in the row's `Value::Object` map and emits values in the provided column order. Missing keys emit `Value::Null`.

**Response-size control**: the optional top-level `limit` is applied **after** `execute_query_with_columns` returns. This keeps MCP responses and agent context smaller, but does not reduce database work or bolt transport volume.

**MemgraphAsync override**: This works across three layers:

1. **`GraphConnection` trait** (`ariadne-core/src/graph/actor.rs:14`): Add `execute_query_with_columns(&mut self, query, params) -> Result<(Vec<String>, Vec<Value>)>` with a default that delegates to `execute_query`. The `Memgraph` impl overrides to return the bolt column vector from `connection.execute()` (`ariadne-core/src/backends/memgraph.rs:390`).
2. **`GraphActor` / `Command` enum** (`ariadne-core/src/graph/actor.rs:24`): Add an `ExecuteQueryWithColumns` command variant carrying `resp: oneshot::Sender<Result<(Vec<String>, Vec<Value>)>>`. Wire the actor loop to dispatch it to the new `GraphConnection` method.
3. **`MemgraphAsync`** (`ariadne-core/src/backends/memgraph_async.rs:94`): Add `execute_query_with_columns` that sends the new command and awaits the response. The `GraphBackend` trait's default `execute_query_with_columns` is then overridden to delegate here.

**In-memory backend**: Uses the default implementation (alphabetical key order). This is acceptable because in-memory is dev/test-only (see **Backend scope** below).

---

## Implementation summary

The V1 implementation now does the following:

1. `graph_query` uses `execute_query_with_columns` and applies top-level response-size control after execution.
2. `graph_schema` defaults to compact text and exposes the full machine-readable schema behind `format = "structured"`.
3. `graph_health` defaults to a compact freshness/status summary and exposes the full diagnostic payload behind `detail = "full"` or `detail = "debug"`.
4. Sync state, rebuild state, and degraded coverage are tracked explicitly and surfaced through `graph_health`.
5. Snapshot provenance is stored in a sidecar `snapshot_manifest.json` instead of extending `cluster.json`.
6. Python MCP clients explicitly request structured schema when they need full machine-readable details.

---

## Validation as a future tool (V2 candidate)

`graph_validate` is intentionally excluded from V1. Rationale:

- `graph_query` already validates before execution and returns the same `QueryIssue` classification.
- A separate validate tool adds a round-trip for marginal benefit — agents will call `graph_query` regardless.
- If V1 adoption shows agents frequently submitting invalid queries without executing, `graph_validate` becomes justified in V2.

The implementation path is straightforward when needed: expose `validate_cypher()` (`ariadne-core/src/cypher_validation.rs:14`) as a tool returning `{ ok: bool, issues: QueryIssue[] }`.

---

## Limit enforcement

This section is intentionally kept because the underlying constraints matter, but **limit enforcement is deferred from V1**.

### V1 behavior

V1 does **not** rewrite Cypher queries, append limits automatically, or reject otherwise valid queries just because they lack `LIMIT`.

- `graph_query` accepts an optional top-level request `limit` (default 100, max 1000) for **response-size control**.
- After the backend returns the full result, the MCP layer truncates returned rows to that limit and sets `truncated` accordingly.
- This keeps responses smaller for transport and agent-context purposes, but does **not** reduce backend work or bolt transport volume.
- Agents should still prefer narrow queries and include `LIMIT` when exploring large subgraphs or listing raw nodes.

This means V1 does **not** fully realize the "prefer bounded results" design goal yet. It is a deliberate scope cut.

### Deferred design: automatic limit injection

If V2 upgrades the top-level request `limit` into a **true server-side row cap**, or introduces automatic query rewriting, enforcement still needs to happen **before or during execution**, not after. Post-query truncation would let broad queries consume unbounded backend time, memory, and bolt transport bandwidth before the MCP layer drops rows.

That future design likely requires:

- richer query-structure support in `ariadne-cypher` so rewrites are safe around `WITH`, `ORDER BY`, `SKIP`, aggregation, and any future multi-branch constructs;
- a clear contract for how Cypher-authored limits interact with any server-authored limit; and
- dedicated tests for both semantics and failure cases before reintroducing rewrite-based caps.

---

## Testing strategy

1. **Unit tests**: Each tool method in `KubeTool` tested with a mock `GraphBackend` (the in-memory backend already exists).
2. **Response-shape tests**: Assert compact and opt-in verbose JSON shapes for `graph_query`, `graph_schema`, and `graph_health`.
3. **Error classification tests**: Existing tests in `kube_tool.rs` and `query_issue.rs` cover error mapping. Extend for the new response envelope and verify `data.source` remains `"backend"`.
4. **Column ordering tests** (Memgraph): Verify `columns` matches Cypher RETURN projection order via bolt column vector. Test with aliased (`AS`), un-aliased, and mixed projections. Verify `MemgraphAsync::execute_query_with_columns` preserves bolt column order.
5. **Response-size control tests**: Verify valid read-only queries both with and without Cypher `LIMIT` execute successfully, top-level request `limit` truncates returned rows after execution, and `truncated` is set correctly.
6. **Readiness tests**: Verify `ready: false` before initial load, `ready: true` after the first successful load, and `ready: false` when backend probe fails (e.g., Memgraph connection dropped).
7. **Freshness and lag tests**: Verify `observed_at` is present, `sync.lag_ms` is computed server-side, `data_as_of` equals `sync.last_success_at` in live mode, and `sync.last_write_at` can remain older than `sync.last_success_at` after a no-change poll.
8. **Rebuild loop tests**: Verify `ENABLE_FULL_REBUILD_LOOP` populates `rebuild`, successful rebuilds advance both `rebuild.last_success_at` and `data_as_of` (since rebuild re-fetches from K8s), but do **not** advance `sync.last_success_at` — the two loops are independent health domains.
9. **Sync degradation tests**: Verify failed source-sync attempts populate `sync.last_error`, increment `consecutive_errors`, keep `loop_alive` true, and reset `consecutive_errors` on the next success.
10. **Coverage tests**: Verify RBAC-denied or best-effort-degraded resource kinds appear in `coverage.degraded_resource_kinds`.
11. **Snapshot provenance tests**: Verify snapshot export writes a sidecar manifest containing both `captured_at` and `scope`, snapshot load surfaces them as `data_as_of` and `scope`, `cluster.json` remains compatible with the raw `Cluster` type, and `sync` is `null` in snapshot mode.
12. **Integration test**: Start `ariadne-mcp` with a snapshot, connect an MCP client, call all three tools in sequence, verify end-to-end.
