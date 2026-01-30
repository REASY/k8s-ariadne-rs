# K8s Graph + MCP + ADK Agent Integration

## Goal
Provide a clear separation of responsibilities so the system stays debuggable, testable, and easy to extend. The K8s graph is the source of truth, MCP is the thin access layer, and the ADK agent handles intent and reasoning.

## System Mental Model
- **K8s graph service (Rust)**: owns data ingestion, normalization, graph schema, and refresh cadence.
- **MCP server**: exposes a small, stable, typed tool surface for querying the graph.
- **ADK agent (Python)**: interprets user intent, plans queries, and synthesizes answers for SRE/developer workflows.

## Responsibilities by Layer

### 1) K8s Graph Service (Rust)
- Owns: collection, normalization, schema, correctness, refresh cadence.
- Exposes: Cypher query interface and minimal metadata (e.g., last sync time, cluster id, schema version).
- Avoids: natural language parsing or business logic.

### 2) MCP Server (Thin Adapter)
- Owns: authentication, authorization, request validation, and stable tool definitions.
- Exposes: deterministic tools that map directly to graph queries.
- Avoids: domain reasoning and multi-step diagnosis.

### 3) ADK Agent (Python)
- Owns: user intent understanding, query planning, multi-step reasoning, and summarization.
- Uses MCP tools as capabilities.
- Maintains conversation state and asks clarifying questions when needed.

## What Belongs in MCP
Keep MCP composable and deterministic. Suggested tool surface:

### Core Tools
1) `graph.query(cypher: string, params?: object)`
   - Executes Cypher and returns rows + schema metadata.
2) `graph.schema()`
   - Returns node labels, relationship types, and key properties.
3) `graph.health()`
   - Returns sync state, last updated timestamp, and coverage notes.
4) `graph.explain(cypher: string)` (optional)
   - Returns query validation or cost hints.

### Optional Helpers
- `graph.validate(cypher: string)`
- `graph.samples(label: string, limit?: number)`

### What NOT to Put in MCP
- Natural language parsing
- Heuristic diagnosis or reasoning
- Narrative response formatting

## What the ADK Agent Does
- Interprets the user request.
- Selects and executes MCP tools (one or more graph queries).
- Synthesizes findings into clear SRE/developer‑focused responses.
- Asks for clarification when requests are ambiguous.
- Applies domain reasoning (incident patterns, likely causes, next steps).

## Example Flow
User: "Why is service checkout slow?"

1) Agent infers likely root causes and required graph views.
2) MCP call: service -> pods -> nodes -> metrics bindings.
3) MCP call: recent restarts/events for relevant workloads.
4) Agent synthesizes: observed facts, likely cause, and next checks.

## Data Contracts and Metadata
For each MCP response, include:
- `rows`: data payload
- `columns`: names and types
- `meta`: cluster id, last sync time, query duration, partial coverage notes

## Design Principles
- **Deterministic MCP**: same input -> same output.
- **Structured responses**: minimal parsing by the agent.
- **Agent owns "why"**: MCP returns "what" only.
- **Stable tool surface**: small API set, versioned when changed.

## Next Steps (Implementation)
- Define MCP tool schemas and response payloads.
- Add graph schema metadata endpoints in the Rust service.
- Implement ADK planner that maps intent -> Cypher -> synthesis.
- Establish an SRE response template (facts, impact, likely cause, next actions).
