# Constrained Decoding for LLM-Generated GraphDB Queries

## Why this document exists

When LLMs generate queries over **strict schemas** (GraphDB, SQL, APIs), they can produce output that is:
- **Syntactically valid**
- **Semantically plausible**
- **Structurally wrong**

The most dangerous failure mode is **silent correctness failure**:
- The query executes
- Returns zero (or partial) results
- No error is raised

This document explains **what constrained decoding actually means**, how it applies to a **GraphDB schema**, and **how to design a production-safe system** that avoids hallucinated edges, directions, and paths.

---

## Core problem

LLMs optimize for *plausibility*, not *schema correctness*.

Example failure:
```cypher
EndpointAddress -[:ListedIn]-> Endpoint
```

Looks reasonable, but:
- The edge exists, but it connects **EndpointAddress → EndpointSlice**
- The node type is wrong (Endpoint vs EndpointSlice)
- Query executes and returns **0 rows**

This is not an LLM bug. It is a **missing constraint problem**.

---

## What is constrained decoding (plain English)

**Constrained decoding means the model is physically unable to emit invalid structures.**

Not:
> "Please only use valid edges"

But:
> "These are the only edges you are allowed to output. Anything else is impossible."

---

## Levels of constraint

### 1. Prompt-only constraints (weak)
Helps but does not prevent hallucinations.

### 2. Enum / choice-based constraints (practical)
Force edge and node choices via enums and compile Cypher yourself.

### 3. Grammar-constrained decoding (strong)
Only grammar-valid tokens can be emitted. Without schema-aware token constraints, it still allows invalid edges/directions.

### 4. AST-first generation (recommended)

```json
{
  "path": [
    { "node": "Ingress" },
    { "edge": "DefinesBackend" },
    { "node": "Service" },
    { "edge": "Manages" },
    { "node": "EndpointSlice" },
    { "edge": "ContainsEndpoint" },
    { "node": "Endpoint" },
    { "edge": "HasAddress" },
    { "node": "EndpointAddress" },
    { "edge": "IsAddressOf" },
    { "node": "Pod" }
  ],
  "filters": {
    "Host.name": "litmus.qa.xyz.is"
  }
}
```

---

## Using your GraphDB schema as constraints

Valid relationships:
```
Endpoint        -[:HasAddress]-> EndpointAddress
EndpointAddress -[:IsAddressOf]-> Pod
EndpointAddress -[:ListedIn]-> EndpointSlice
```

Canonical path:
```
Host → Ingress → Service → EndpointSlice → Endpoint → EndpointAddress → Pod
```

Property constraints (also a common source of silent zeros):
- `metadata.name` vs `name` (object metadata is usually nested)
- `metadata.namespace` vs `namespace`
- `spec.rules[].host` (Ingress host lives under rules)
- `status.podIP` vs `status.pod_ip` (exact property names)

---

## Production-safe architecture

```
User question
  ↓
LLM (planner)
  ↓
QueryPlan (structured)
  ↓
Schema Validator
  ↓
Cypher Compiler
  ↓
GraphDB
```

---

## Guardrails vs constraints vs validation

| Mechanism    | Role |
|-------------|------|
| Guardrails  | Reduce error frequency |
| Constraints | Prevent invalid output |
| Validation  | Catch remaining failures |

---

## Final takeaway

> **LLMs are probabilistic planners, not trustworthy executors.**

Treat schemas like type systems.
Fail loud on schema violations; warn on suspicious empty results.
