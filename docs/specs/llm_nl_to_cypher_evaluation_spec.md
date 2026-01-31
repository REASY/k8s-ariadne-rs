# LLM Evaluation Spec for NL → Cypher Translation over GraphDB

## Purpose

This document defines a **practical, production-oriented evaluation spec** for benchmarking LLMs that translate **natural language questions into Cypher queries** over a **strict GraphDB schema** (e.g., Kubernetes graph).

The goal is **not** to measure general intelligence, but to answer:

> Which LLMs most reliably follow a schema, converge under validation feedback, and produce correct results at acceptable cost?

---

## Non-goals

This evaluation explicitly does **not** attempt to:
- Measure chain-of-thought quality
- Benchmark abstract reasoning or math skills
- Produce academic confidence intervals
- Rank models by verbosity or explanation quality

---

## Core principle

> **LLMs are probabilistic planners, not trustworthy executors.**

All correctness decisions must be made by **deterministic systems**:
- AST/schema validator
- Frozen database execution
- Expected result comparison (where available)

---

## Evaluation modes

Each model is evaluated in the following modes:

### 1. Single-shot
- One NL → Cypher attempt
- No retry
- Validator applied once

### 2. Validator + retry (baseline)
- Max 2 attempts
- Structured validator feedback injected on failure (see **Retry feedback contract**)
- This is the **primary production mode**

### 3. Optional: constrained variants (future)
- Structured plan output
- Enum-constrained edges
- Grammar-constrained decoding

Results from mode (2) are used for leaderboard ranking.

---

## Models under test

### Allowed
- Normal / standard LLMs
- Temperature = 0 (or lowest supported)
- No chain-of-thought
- No hidden reasoning channels

### Disallowed for baseline
- Chain-of-thought prompts
- Free-form reasoning output
- Self-reflection or “thinking aloud”

### Optional (separate experiments)
- Reasoning models as **planners only**
- Planner output must not include schema names or Cypher

---

## Dataset requirements

### Question set
- Minimum: 50 questions
- Recommended: 100–200
- Stored in YAML or JSON

Each question includes:
- ID
- Natural language question
- Tags (e.g., ingress, dns, pod, filter)
- Optional expected result
- Optional deterministic flag (if the snapshot guarantees a single stable answer)
- Optional reference Cypher (gold) for debugging and shape guidance (not primary scoring)

Example:
```yaml
- id: q001
  question: "What are the pods backing DNS name litmus.qa.agoda.is?"
  deterministic: true
  reference_cypher: >
    MATCH (h:Host)-[:IsClaimedBy]->(i:Ingress)-[:DefinesBackend]->(b:IngressServiceBackend)
    -[:TargetsService]->(s:Service)-[:Manages]->(es:EndpointSlice)-[:ContainsEndpoint]->(e:Endpoint)
    -[:HasAddress]->(ea:EndpointAddress)-[:IsAddressOf]->(p:Pod)
    WHERE h.name = 'litmus.qa.agoda.is'
    RETURN DISTINCT p.metadata.namespace AS namespace, p.metadata.name AS pod_name
  expected:
    columns: [namespace, pod_name]
    rows:
      - ["litmus", "chaos-litmus-frontend-..."]
  tags: [dns, ingress, endpointslice, pod]
```

### Dataset composition
- Easy: direct paths
- Medium: filters + joins
- Hard: aggregations, grouping
- Adversarial: tempting schema violations

---

## Environment control

To ensure meaningful comparison:
- Use a **frozen GraphDB snapshot**
- Same schema for all runs
- Same system prompt/spec
- Same retry policy
- Same timeout limits
- Record version identifiers: schema hash, snapshot ID, prompt version, validator version, and model parameters

---

## Validation and execution pipeline

```
User question
  ↓
LLM
  ↓
Cypher
  ↓
AST + schema validator
  ↓
(optional retry with structured feedback)
  ↓
GraphDB execution
  ↓
Result comparison
```

### Validator responsibilities
- Relationship existence
- Direction correctness
- Node–edge compatibility
- Illegal patterns (where possible)

Validation failure = hard error (no silent fallback).

### Retry feedback contract (baseline)
On validation failure, the retry prompt **must** include a compact, structured payload:
- Error category (e.g., `wrong_direction`, `unknown_edge`, `label_mismatch`, `parse_error`)
- Human hint (one sentence)
- Invalid Cypher (verbatim)
- Optional schema excerpt limited to relevant relationship(s)

This payload is versioned and **must not change** within a benchmark run.

### Post-processing / rewrites policy (baseline)
Allowed: **minimal, deterministic repairs** with explicit accounting.
- Examples: insert missing `WITH` after `UNWIND`, normalize `EXISTS` into supported form,
  reorder equivalent MATCH clauses.
- Disallowed: schema-guessing rewrites, adding/removing semantic filters, or altering
  relationship directions.

All repairs must be logged and scored separately as **valid-after-repair** (not first-attempt).

---

## Metrics collected

### A. Correctness metrics (primary)
- Valid on first attempt (%)
- Valid after retry (%)
- Unrecoverable failure rate (%)
- End-to-end result match (%)

### B. Stability metrics
- Attempts per question
- Ever-failed rate (did it fail at least once?)
- Retry convergence rate

### C. Efficiency metrics
- Tokens per attempt
- Total tokens per question
- Latency per attempt and total
- Estimated cost

**Token and cost accounting:** record tokenizer/model-specific counts, include system prompt and retry prompts, and document any provider-side token deltas (e.g., tool metadata). Costs must specify pricing version.

### D. Failure analysis
- Unknown edge
- Wrong direction
- Wrong node labels
- Missing required hops
- Parse error
- Illegal function or pattern
- Execution error (DB runtime)
- Timeout

---

## Repetition policy (statistical guidance)

- Default: **3–5 runs per (model, question)**
- If temperature = 0: 3 runs usually sufficient
- Adversarial questions: up to 10 runs
- Stop early if outcome is identical 3 times in a row

Binary outcomes are preferred over averages.

Deterministic questions (as tagged in the dataset) are used for flakiness checks.

---

## LLM-as-judge policy

### When to use
- Only as a **secondary signal**
- For questions with ambiguous intent
- For ranking query *quality* among already-correct queries

### When NOT to use
- Schema correctness
- Result correctness
- Primary model ranking

### Judge setup
- Blind pairwise comparison
- Question + schema summary + query A/B
- 3 judge samples or 2 judge models
- Majority vote

---

## Scoring and ranking

### Primary ranking criteria
1. End-to-end correctness rate
2. Valid-after-retry rate
3. Unrecoverable failure rate

### Secondary criteria
- First-attempt validity
- Avg attempts
- Cost and latency

A model that fails even once on deterministic questions is considered **flaky**.

---

## Reporting format

Each run produces structured JSON:

```json
{
  "model": "openai/gpt-5.2",
  "question_id": "q001",
  "attempts": [
    {"valid": false, "error": "Invalid relationship", "tokens": 17387},
    {"valid": true, "tokens": 34944}
  ],
  "final": {"valid": true, "result_match": true},
  "metrics": {"attempts": 2, "total_tokens": 52331}
}
```

### Result matching rules
When `expected` is present, compare results using:
- Column name matching (case-sensitive)
- Row ordering ignored unless explicitly marked `ordered`
- Duplicate rows compared by multiset equality
- Numeric tolerances defined per dataset (default: exact match)

When `expected` is **absent**, compute validity/execution metrics only; result correctness is **not** scored.

### Gold Cypher usage (recommended)
If `reference_cypher` is present:
- Use it to debug failures and to document intended shape.
- Do **not** require model output to match it exactly.

Final report includes:
- Per-model summary table
- Failure breakdown
- Cost/correctness tradeoffs
- Worst-case behaviors observed

---

## Explicit exclusions

- No chain-of-thought evaluation
- No explanation scoring in baseline
- No confidence-interval optimization

---

## Final takeaway

> **If correctness depends on schema obedience, add constraints — not thoughts.**

Deterministic validation + retries beat reasoning every time for structured query translation.
