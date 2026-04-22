---
name: ariadne-kubernetes-graph
description: Use for Ariadne MCP-backed Kubernetes state questions: translate operational questions into Cypher, repair graph_query failures, and decide when graph_schema or graph_health is needed. Best for live or snapshot-backed graph data about resource relationships; do not use for codebase-only tasks or generic Kubernetes explanations.
---

# Ariadne Kubernetes Graph

## Overview

Use this skill when the task is about Kubernetes state that is already available through Ariadne MCP.

The goal is to answer cluster questions with the cheapest reliable sequence of Ariadne tool calls, while avoiding unnecessary schema and health fetches.

## When To Use

Use this skill for:

- live cluster questions answered through Ariadne MCP
- snapshot-backed cluster questions answered through Ariadne MCP
- translating operational questions into `graph_query`
- debugging failed or ambiguous Ariadne queries
- deciding when `graph_schema` or `graph_health` is actually needed

Do not use this skill for:

- codebase-only tasks
- generic Kubernetes explanations that do not depend on Ariadne data
- tasks where the user already provided the exact Cypher to run and no Ariadne-specific reasoning is needed

## Default Workflow

1. Decide whether the question is really about Ariadne-backed cluster state.
2. Reuse prior successful Ariadne results from the same conversation when still relevant.
3. Call `graph_query` directly if the labels, edges, and property paths are already known.
4. Call `graph_schema` only when query shape is uncertain, when working in an unfamiliar part of the graph, or after a schema or semantic failure.
5. Call `graph_health` only when freshness, readiness, coverage, or sync lag materially affects the answer.
6. Keep queries narrow and use Cypher `LIMIT` for exploratory reads.
7. Present results in a concise, operator-friendly format instead of dumping raw tool output.

## Response Style

When answering through this skill:

- start with the direct answer or next action, not with tool narration
- prefer short headings only when they add clarity
- use flat bullets for multi-part findings
- summarize the result before listing rows or examples
- use fenced `cypher` blocks only when the query is useful to show or debug
- avoid dumping raw `graph_health` JSON unless the user explicitly asked for it
- mention freshness, degraded coverage, or sync lag only when it affects trust in the answer
- if a query was repaired, explain the fix in one short sentence
- if the result is inconclusive, say what is missing and what narrower follow-up query would resolve it

## Ariadne-Specific Model Notes

Ariadne exposes both native Kubernetes resources and graph-friendly logical nodes. The most important Ariadne-specific logical nodes are:

- `Container`
- `Endpoint`
- `EndpointAddress`
- `Host`
- `IngressServiceBackend`
- `Cluster`

These are often where the graph differs from a direct Kubernetes API mental model.

For detailed traversals and Cypher patterns, read [references/query-patterns.md](references/query-patterns.md).

## Query Rules

When using `graph_query`:

- prefer parameterized Cypher with `$var` parameters
- prefer narrow queries and include Cypher `LIMIT` for exploration
- alias non-trivial `RETURN` expressions with `AS`
- reuse known-good patterns before asking for schema again
- treat `repairable = true` query errors as signals to fix and retry
- prefer one precise follow-up query over multiple broad exploratory queries

Important Memgraph and Ariadne pitfalls:

- no pattern predicates; use `OPTIONAL MATCH + count(...)` for absence checks
- after `UNWIND`, add `WITH` before a filtering `WHERE`
- avoid `CALL { ... }` subqueries and procedures
- do not use inline property filters like `(:Pod {metadata: ...})`
- use single quotes for map keys: `p['metadata']['name']`
- avoid cartesian explosions from multiple broad `OPTIONAL MATCH` clauses

## Health And Trust

Use `graph_health` when:

- the user asks about freshness, lag, sync, readiness, or degraded coverage
- a query failure suggests stale or unhealthy graph state
- enough time has passed that freshness is part of the answer quality

When reporting Ariadne results:

- distinguish graph facts from inferences
- mention degraded coverage only when it affects trust in the answer
- avoid repeating health metadata when it is not relevant
- do not over-qualify straightforward graph facts that came directly from query results

## References

- For traversal paths, examples, and failure-repair patterns, read [references/query-patterns.md](references/query-patterns.md).
