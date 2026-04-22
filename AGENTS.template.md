# Ariadne Agent Guide Template

This is a template/example `AGENTS.md`, not the active instruction file for this repository.
Copy or adapt it into a real `AGENTS.md` only in the environment where you want these instructions to apply.

Use the Ariadne MCP server for Kubernetes cluster questions when the question is about live cluster state or relationships between resources. Do not use it for codebase-only tasks.

## Token-Efficient Workflow

Default to the cheapest path that can answer the question correctly.

For routine cluster questions, prefer:
1. Reuse prior successful Ariadne results from the same conversation when still relevant.
2. Call `graph_query` directly if you already know the labels, relationships, and property paths needed.
3. Call `graph_schema` only when query shape is uncertain, when using unfamiliar parts of the graph, or after a schema/semantic failure.
4. Call `graph_health` only when freshness, readiness, sync lag, coverage, or backend health materially affects the answer.

Do not call `graph_schema` on every turn.
Do not call `graph_health` on every turn.

Reuse a recent successful `graph_health` result within the same conversation unless:
- The user asks about freshness, staleness, sync, or health
- The previous health result showed lag, errors, or degraded coverage
- A query fails in a way that suggests stale or unhealthy graph state
- Enough time has passed that freshness is now relevant again

Call `graph_schema` when:
- Writing the first non-trivial query in an unfamiliar area of the graph
- You are unsure about label names, relationship directions, or property names
- A previous `graph_query` failed with schema or semantic issues

Skip `graph_schema` when:
- Reusing a pattern that already worked earlier in the conversation
- Asking a routine question that matches known graph patterns
- The answer can be obtained with a straightforward query over already-established labels and edges

Prefer one good `graph_query` over a sequence of sample queries when the final query is already clear.

## Key Traversal Paths

These are the graph-specific edges and decomposed nodes that differ from standard K8s API structure. Properties follow standard K8s API structure (e.g., `p['metadata']['name']`, `p['spec']['containers']`).

DNS/hostname to pods:
  `Host-[IsClaimedBy]->Ingress-[DefinesBackend]->IngressServiceBackend-[TargetsService]->Service`

Service to backing pods:
  `Service-[Manages]->EndpointSlice-[ContainsEndpoint]->Endpoint-[HasAddress]->EndpointAddress-[IsAddressOf]->Pod`

Deployment to pods:
  `Deployment-[Manages]->ReplicaSet-[Manages]->Pod`

Other workload owners to pods:
  `StatefulSet-[Manages]->Pod`, `DaemonSet-[Manages]->Pod`, `Job-[Manages]->Pod`, `Node-[Manages]->Pod`

Storage chain:
  `Pod-[ClaimsVolume]->PersistentVolumeClaim-[BoundTo]->PersistentVolume-[UsesStorageClass]->StorageClass`

Container resources:
  `Container-[Runs]->Pod` (resources at `c['spec']['resources']`)

Do not assume `-[BelongsTo]->Namespace` or `-[PartOf]->Cluster` exists for every label.
If those edges matter and you are not reusing a known-good pattern, confirm them with `graph_schema`.

## Query Rules

When using `graph_query`:
- Prefer parameterized Cypher with `$var` parameters.
- Prefer narrow queries and include Cypher `LIMIT` for exploratory reads.
- Alias non-trivial `RETURN` expressions with `AS`.
- Reuse previously successful query patterns before consulting `graph_schema` again.
- Treat `graph_query` errors with `repairable = true` as signals to fix and retry the query.
- If a query fails because labels, edges, or property names are uncertain, then call `graph_schema`.

Avoid these Memgraph-specific pitfalls:
- **No pattern predicates**: `WHERE NOT (s)-[:Manages]->(:EndpointSlice)` fails. Use OPTIONAL MATCH + count instead (see absence pattern below).
- **UNWIND + WHERE**: After UNWIND, add a WITH before any WHERE that filters the unwound variable.
- **No CALL clauses**: Avoid `CALL { ... }` subqueries and procedure calls; the current Ariadne validator rejects them.
- **No inline property filters**: `MATCH (p:Pod {metadata: {name: 'x'}})` fails. Always use WHERE.
- **Single quotes for map keys**: `p['metadata']['name']` not `p["metadata"]["name"]`.
- **Cartesian explosions**: Avoid multiple OPTIONAL MATCH clauses when aggregating across labels. Prefer a single OPTIONAL MATCH plus WITH-based aggregation and filtering.

Examples:

```cypher
-- Pods per node in a namespace
MATCH (p:Pod)-[:RunsOn]->(n:Node)
WHERE p['metadata']['namespace'] = $namespace
RETURN n['metadata']['name'] AS node_name, count(p) AS pod_count
ORDER BY pod_count DESC
LIMIT 25
```

```cypher
-- Hostname to backend services
MATCH (h:Host)-[:IsClaimedBy]->(:Ingress)
  -[:DefinesBackend]->(:IngressServiceBackend)
  -[:TargetsService]->(s:Service)
WHERE h['name'] = $hostname
RETURN
  s['metadata']['name'] AS service_name,
  s['metadata']['namespace'] AS service_namespace
LIMIT 25
```

```cypher
-- Find resources without a relationship (absence pattern)
MATCH (s:Service)
OPTIONAL MATCH (s)-[:Manages]->(es:EndpointSlice)
WITH s, count(es) AS es_count WHERE es_count = 0
RETURN s['metadata']['namespace'] AS namespace, s['metadata']['name'] AS name
LIMIT 100
```

## Answer Style

When answering from Ariadne MCP:
- Mention `graph_health` only when freshness or health is relevant to the answer.
- Do not repeat `ready`, `lag`, `data_as_of`, or version details unless they materially affect trust in the result.
- Distinguish graph facts from inferences.
- If `graph_health` shows lag or degraded coverage, mention that as a caveat.
- Prefer short factual answers over long tool-by-tool narration.
