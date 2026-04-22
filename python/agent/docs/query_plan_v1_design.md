# QueryPlanV1 Design

Initial validation results for the design and the first IR smoke gates
are recorded in
[query_plan_v1_initial_results.md](/work/python/agent/docs/query_plan_v1_initial_results.md).

## Motivation

The current NL→Cypher pipeline asks LLMs to solve multiple problems in
one shot: understand the question semantics, pick the right graph
traversal, use correct Memgraph bracket-notation property access, get
relationship directions right, and produce valid Cypher syntax. When a
model fails, it is usually on one of these — not all.

Eval data from 18 models on 162 questions (rescored March 2026) shows:

- The top model (gpt-5.4) reaches 90.7% correctness, but most models
  cluster around 75–83%. The gap is dominated by property access errors,
  relationship naming mistakes, and Memgraph dialect issues — not by
  failure to understand the question.
- Claude Sonnet 4-6 understands 86.4% of queries correctly (matched via
  projection) but only gets 35% exact-column match — it picks the right
  entities and traversals but writes Cypher with different column names
  or supplementary properties.
- Gemini models gained +14 to +24 matched questions after removing
  over-specified expected columns, confirming they were producing
  semantically correct queries penalized only for surface-level
  differences.
- DSPy prompt optimization improved gemini-2.5-flash from 34/54 to
  44/54 on the dev set, but hit a ceiling: the single dspy.Predict
  module has too little optimization surface. The taxonomy-aware
  scorer does distinguish failure modes (ordering, missing rows,
  wrong semantics, etc.), but the single-module program structure
  limits how much the optimizer can exploit that signal.
- OpenAI gpt-5 models show limited DSPy responsiveness — the API
  forces temperature=1.0, making optimization signal noisy. gpt-5.4-mini
  gained +5 (possibly lucky given the noise), but gpt-5-mini and the
  nano-tier models were flat.

These findings point to the same conclusion: **the LLM's semantic
understanding is ahead of its Cypher-generation ability.** The models
know what to query but produce mechanically incorrect code.

An intermediate representation separates these concerns:
- The LLM handles **what** to query (entities, relationships, filters,
  return columns) — the part it is already good at.
- A deterministic compiler handles **how** to write valid Memgraph
  Cypher — property resolution, bracket notation, relationship
  direction, dialect-specific syntax.

This may also improve DSPy effectiveness: the IR is structured JSON
that could be decomposed into multiple optimization targets in a future
multi-module program. However, the immediate value is in the compiler —
eliminating mechanical errors that the current NL→Cypher pipeline
cannot fix regardless of prompt optimization.

## Design goal

A typed IR for K8s graph queries that:
1. Is expressive enough to represent every query in the gold dataset
2. Is easier for LLMs to generate correctly than raw Memgraph Cypher
3. Can be deterministically compiled to valid Memgraph Cypher
4. Provides validation before execution — both structural (JSON Schema)
   and **semantic** (a plan validator that checks relationship legality,
   variable scope after stages, alias existence, per-entity property
   resolution, and negation variable scoping). JSON Schema alone is
   insufficient; the semantic checks are the hard guarantees.

## IR Schema (JSON)

### TranslatorOutput envelope

The LLM produces a `TranslatorOutput` — a discriminated union that
either contains a compiled query plan or a raw Cypher fallback:

```json
{
  "mode": "plan" | "cypher",
  "plan": QueryPlanV1 | null,
  "cypher": string | null,
  "reason": string | null
}
```

- When `mode` is `"plan"`: `plan` is required, `cypher` and `reason`
  are absent. The compiler validates and compiles the plan.
- When `mode` is `"cypher"`: `cypher` is required, `reason` is
  required (must name the Cypher feature the IR cannot express),
  `plan` is absent. See [Fallback to raw Cypher](#fallback-to-raw-cypher)
  for the execution policy and gating rules.

The JSON Schema for structured LLM output (recommended next step 3)
must define this envelope as the root object, with `QueryPlanV1` as
a nested definition.

### QueryPlanV1

```json
{
  "$schema": "QueryPlanV1",
  "match": [ MatchStep ],
  "where": [ FilterExpr ],
  "unwind": UnwindStep | null,
  "stages": [ AggregationStage ],
  "return": [ ReturnExpr ],
  "order_by": [ OrderSpec ],
  "limit": int | null,
  "distinct": bool
}
```

The compiler emits Cypher in this fixed order:

1. `match` → MATCH / OPTIONAL MATCH clauses (with step-scoped WHERE)
2. `where` → WITH * WHERE (global post-match filter, if present)
3. `unwind` → UNWIND
4. `stages` → WITH ... WHERE chains (aggregation pipeline)
5. `return` → RETURN
6. `order_by` → ORDER BY
7. `limit` → LIMIT
8. `distinct` → RETURN DISTINCT (modifies step 5)

Field descriptions:

- `match`: the match steps (required)
- `where`: optional global post-match filters. The compiler emits a
  `WITH *` barrier before these predicates to prevent them from being
  absorbed by a preceding `OPTIONAL MATCH`. This compiles to:
  `WITH * WHERE <predicates>`, ensuring true row-level filtering after
  all graph joins have settled. Distinct from MatchStep.filter (which
  is scoped to a single step's MATCH clause). Not needed by the
  current 60-question gold set, but included for completeness — see
  [Design notes](#design-notes) issue 4.
- `unwind` through `distinct`: as described in their respective sections

### MatchStep

A MatchStep binds an entity variable, optionally connecting it to a
previously bound variable via a relationship. Steps execute in order;
later steps can reference variables from earlier steps.

```json
{
  "entity": EntityType,
  "bind": string | null,
  "from": RelationshipSpec | null,
  "filter": [ FilterExpr ],
  "not_exists": [ NegationClause ],
  "property_join": PropertyJoin | null,
  "optional": bool                     // default: false
}
```

- `entity`: one of the EntityType enum values (see below)
- `bind`: variable name used in later steps, stages, and return.
  **Optional** — when omitted, the entity is anonymous in the compiled
  Cypher (e.g., `(:EndpointSlice)` instead of `(es:EndpointSlice)`).
  Omit `bind` for intermediate hops that are not referenced elsewhere.
- `from`: connects this step to a previously bound variable via a
  relationship; the compiler infers direction from the schema
- `filter`: zero or more filter predicates applied to this entity

#### Predicate placement for optional steps

When `optional` is true, all predicates on this step — `filter`,
`property_join`, and `not_exists` — are compiled **inside** the same
`OPTIONAL MATCH ... WHERE` clause, not as a later global `WHERE`.

This is semantically critical: an optional match with a predicate
means "find this entity if it exists and satisfies the predicate;
return NULL if it doesn't exist." Moving the predicate to a global
WHERE would filter out NULL rows and break the optional semantics.

Example (m14): Container is required, Logs is optional with a
property_join predicate:

```
step 1: { entity: Container, bind: c, filter: [ns=litmus] }         → MATCH (c:Container) WHERE ...
step 2: { entity: Logs, bind: l, optional: true,
          property_join: { local: container_uid, remote_var: c,
                           remote_prop: uid } }                       → OPTIONAL MATCH (l:Logs) WHERE l[...] = c[...]
```

The property_join on step 2 compiles inside `OPTIONAL MATCH ... WHERE`,
not after the OPTIONAL MATCH. The compiler rule: **all predicates on
an optional step are part of that step's OPTIONAL MATCH clause.**

Non-optional steps compile their predicates into the immediately
following WHERE clause of their MATCH. When consecutive non-optional
steps share the same MATCH clause (e.g., a multi-hop chain), their
filters are ANDed in the shared WHERE.

#### Optional chains (multi-hop optional)

When consecutive MatchSteps are all marked `optional: true` and form
a connected chain (each step's `from` references the previous step),
the compiler emits them as a **single** `OPTIONAL MATCH` clause, not
separate OPTIONAL MATCHes per step.

Example (m07 — Service → optional 6-hop chain → Pod):

```json
[
  { "entity": "Namespace", "bind": "ns", "filter": [{"property": "name", "op": "eq", "value": "litmus"}] },
  { "entity": "Service", "bind": "s",
    "from": { "variable": "ns", "relationship": "BelongsTo" } },
  { "entity": "EndpointSlice", "optional": true,
    "from": { "variable": "s", "relationship": "Manages" } },
  { "entity": "Endpoint", "optional": true,
    "from": { "relationship": "ContainsEndpoint" } },
  { "entity": "EndpointAddress", "optional": true,
    "from": { "relationship": "HasAddress" } },
  { "entity": "Pod", "bind": "p", "optional": true,
    "from": { "relationship": "IsAddressOf" } }
]
```

Steps 2–5 are all optional and chained. Steps 3–5 use
RelationshipSpec form 2 (no `variable` — chains to preceding step).
Steps 2–4 omit `bind` since they are anonymous intermediaries.

The compiler emits:

```cypher
MATCH (s:Service)-[:BelongsTo]->(ns:Namespace)
WHERE ns['metadata']['name'] = 'litmus'
OPTIONAL MATCH (s)-[:Manages]->(:EndpointSlice)-[:ContainsEndpoint]->(:Endpoint)-[:HasAddress]->(:EndpointAddress)-[:IsAddressOf]->(p:Pod)
```

This is a single OPTIONAL MATCH — if any hop in the chain fails, the
entire chain returns NULL, preserving the Service row.

The chaining rule depends on the RelationshipSpec form:

- **Form 2** (no `variable` — implicit chain to preceding step):
  **continues** the current OPTIONAL MATCH chain. The compiler
  appends this step to the same clause.
- **Form 1** (explicit `variable`): **always starts a new clause**,
  even if the named variable was bound by the immediately preceding
  step.

This distinction applies **only to optional steps**. It is what
separates m07 (single all-or-nothing chain) from h18 (independent
optional queries):

- In m07, steps 3–5 (Endpoint, EndpointAddress, Pod) use form 2 →
  they chain into one OPTIONAL MATCH. Step 2 (EndpointSlice) uses
  form 1 (`variable: "s"`) → it starts the OPTIONAL MATCH from `s`.
  The result is one OPTIONAL MATCH clause.
- In h18, both steps 1 and 2 (Service, EndpointSlice) use form 1 →
  each starts a separate OPTIONAL MATCH clause.

A non-optional step also breaks the chain and starts a new clause.

**Important**: for non-optional steps, the form 1 vs form 2 distinction
does **not** affect clause boundaries. Non-optional steps follow a
simpler rule — see [Non-optional chain compilation](#non-optional-chain-compilation-multi-match)
below.

#### Passthrough WITH barriers between OPTIONAL MATCHes

When an optional step uses form 1 (explicit `variable`) after a
preceding optional step, the compiler emits a **passthrough WITH**
barrier before the new OPTIONAL MATCH. This ensures the first
OPTIONAL MATCH settles before the second starts.

Example (h18 — two independent OPTIONAL MATCHes):

```json
[
  { "entity": "Namespace", "bind": "ns" },
  { "entity": "Service", "bind": "s", "optional": true,
    "from": { "variable": "ns", "relationship": "BelongsTo" } },
  { "entity": "EndpointSlice", "bind": "es", "optional": true,
    "from": { "variable": "s", "relationship": "Manages" } }
]
```

Step 2 uses form 1 (`variable: "ns"`) → starts an OPTIONAL MATCH.
Step 3 uses form 1 (`variable: "s"`) → starts a **new** OPTIONAL
MATCH. The compiler inserts a passthrough WITH between them:

```cypher
MATCH (ns:Namespace)
OPTIONAL MATCH (s:Service)-[:BelongsTo]->(ns)
WITH ns, s
OPTIONAL MATCH (s)-[:Manages]->(es:EndpointSlice)
```

The `WITH ns, s` barrier is inserted automatically. The compiler rule:
**a form 1 `from` on an optional step after a preceding optional
step triggers a passthrough `WITH` listing all in-scope variables
before the new OPTIONAL MATCH.**

#### Non-optional chain compilation (multi-MATCH)

Non-optional steps use a **different, simpler** chaining rule than
optional steps. The form 1 vs form 2 distinction does NOT affect
clause boundaries for non-optional steps:

**Rule**: consecutive non-optional steps compile into one MATCH clause
as long as each step's `from` references a variable bound by any
earlier step in the current chain. A step with no `from`, or a step
that is optional, starts a new clause.

Both forms work identically for non-optional chaining:
- Form 1 (`{ "variable": "p", "relationship": "ClaimsVolume" }`) —
  chains to `p` regardless of whether `p` is the immediately
  preceding step
- Form 2 (`{ "relationship": "ClaimsVolume" }`) — chains to the
  immediately preceding step

Example (h05 — 7 consecutive non-optional steps). The compiler emits
one MATCH clause:

```cypher
MATCH (ns:Namespace)<-[:BelongsTo]-(i:Ingress)-[:DefinesBackend]->(isb:IngressServiceBackend)-[:TargetsService]->(s:Service)-[:Manages]->(es:EndpointSlice)-[:ContainsEndpoint]->(e:Endpoint)-[:HasAddress]->(:EndpointAddress)
WHERE ns['metadata']['name'] = 'litmus'
```

Example (h14 — 3 non-optional steps, each using form 1):

```
step 0: { entity: Namespace, bind: ns }
step 1: { entity: Pod, bind: p, from: { variable: ns, relationship: BelongsTo } }
step 2: { entity: PersistentVolumeClaim, bind: pvc, from: { variable: p, relationship: ClaimsVolume } }
step 3: { entity: PersistentVolume, bind: pv, from: { variable: pvc, relationship: BoundTo } }
```

Steps 0–1 form one chain (ns → p). Step 2 references `p` (from
step 1, the immediately preceding step), so it continues that chain.
Step 3 references `pvc` (step 2, preceding), continuing further.

However, the compiled pattern has a **branch**: `p` connects to both
`ns` (via BelongsTo) and `pvc` (via ClaimsVolume). A single MATCH
clause cannot express a branch from the same variable in two
directions using one linear pattern. The compiler handles this by
emitting **separate MATCH clauses for each branch**:

```cypher
MATCH (p:Pod)-[:BelongsTo]->(ns:Namespace)
MATCH (p)-[:ClaimsVolume]->(pvc:PersistentVolumeClaim)-[:BoundTo]->(pv:PersistentVolume)
WHERE ns['metadata']['name'] = 'litmus'
```

The branching rule: **when a non-optional step's `from` references
a variable that is not the tail of the current MATCH pattern (i.e.,
the step branches back to an earlier variable), the compiler starts
a new MATCH clause.** Steps that continue linearly from the tail of
the current pattern stay in the same clause.

In this example, step 0–1 form the first MATCH (ns ← p). Step 2
references `p` which IS the tail of the first MATCH, so it could
continue. But the relationship goes in a different direction from `p`
(ClaimsVolume, not BelongsTo). The compiler starts a new MATCH from
`p` for clarity and correctness. Step 3 continues linearly from
`pvc` in that second MATCH.

For non-optional patterns, one MATCH vs two MATCHes is semantically
equivalent — the result set is identical. The compiler's concrete
rule ensures deterministic output.

**Summary of chaining rules**:

| Step type | Form 1 (explicit variable) | Form 2 (preceding step) |
|-----------|---------------------------|------------------------|
| Non-optional | Chains if target is tail of current pattern; **new MATCH** if branching back | Chains (always linear) |
| Optional | **Starts new** OPTIONAL MATCH (with WITH barrier) | Continues current chain |

- `not_exists`: zero or more negation paths anchored at this entity
- `property_join`: join this entity to a previously bound variable via
  property equality rather than a graph relationship
- `optional`: if true, compiles to OPTIONAL MATCH (entity may not exist)

### EntityType (enum)

```
Pod | Deployment | StatefulSet | ReplicaSet | DaemonSet | Job |
Service | Ingress | EndpointSlice | NetworkPolicy |
ConfigMap | Container | Logs |
Node | Namespace | Cluster |
ServiceAccount | Event |
PersistentVolume | PersistentVolumeClaim | StorageClass | Provisioner |
IngressServiceBackend | Endpoint | EndpointAddress | Host |
AWX
```

### RelationshipType (enum)

```
BelongsTo | Manages | RunsOn | Runs |
DefinesBackend | TargetsService | IsClaimedBy |
ContainsEndpoint | HasAddress | IsAddressOf | ListedIn |
ClaimsVolume | BoundTo | UsesStorageClass | UsesProvisioner |
MountsConfig | InjectsConfig | UsesIdentity |
AppliesTo | Concerns | PartOf
```

### RelationshipSpec

Connects the current MatchStep entity to another step. Two forms:

**1. Named variable** (reference a previously bound variable by name):

```json
{
  "variable": string,
  "relationship": RelationshipType
}
```

**2. Previous step** (chain to the immediately preceding step):

```json
{
  "relationship": RelationshipType
}
```

When `variable` is omitted, the compiler connects this step to the
entity from the immediately preceding MatchStep in the array. This
avoids forcing the LLM to invent bind names for anonymous
intermediaries.

The compiler **infers direction** from the schema's relationship table.
Given the two entity types and the relationship type, there is exactly
one valid direction. The LLM does not specify direction.

Example: if the current entity is `Pod` and the bound variable is
`Namespace` with relationship `BelongsTo`, the schema says
`BelongsTo: Pod → Namespace`, so the compiler emits
`(p)-[:BelongsTo]->(ns)`.

If the relationship does not connect the two entity types in either
direction, the compiler rejects the query with a validation error.

### FilterExpr

A predicate used in three contexts:
1. **MatchStep.filter**: predicates on the step's entity properties
2. **AggregationStage.having**: predicates on stage outputs (aliases,
   grouped variables, or variable-is-not-null checks)
3. **Top-level `where`**: predicates on any bound variable's properties
   (requires explicit variable scoping via form 6)

Six forms:

**1. Property-vs-literal** (entity property compared to a constant):

```json
{
  "property": PropertyRef,
  "op": FilterOp,
  "value": string | number | bool | null
}
```

Used in MatchStep.filter. The compiler resolves the property via
the entity's property map.

**2. Property-vs-property** (cross-property comparison, used by h08):

```json
{
  "property": PropertyRef,
  "op": FilterOp,
  "value_ref": { "variable": string, "property": PropertyRef }
}
```

Compiles to: `d['status']['readyReplicas'] < d['spec']['replicas']`

**3. Alias-vs-literal** (stage output compared to a constant):

```json
{
  "alias": string,
  "op": FilterOp,
  "value": string | number | bool | null
}
```

Used in AggregationStage.having. `alias` references a ComputeExpr alias
from the same stage.

Examples:
- h04/h11/h12: `{ "alias": "endpoint_slice_count", "op": "eq", "value": 0 }`
- h02: `{ "alias": "pod_count", "op": "gt", "value": 50 }`
- h15: `{ "alias": "namespace_count", "op": "gt", "value": 1 }`

Compiles to: `WHERE endpoint_slice_count = 0`

**4. Variable-is-not-null** (null check on a grouped variable):

```json
{
  "variable": string,
  "op": "is_not_null" | "is_null"
}
```

Used in AggregationStage.having for patterns like h18 where an
OPTIONAL MATCH variable must be checked for existence.

Example (h18): `{ "variable": "s", "op": "is_not_null" }`

Compiles to: `WHERE s IS NOT NULL`

**5. Boolean composition** (used by h14):

```json
{
  "or": [ FilterExpr, FilterExpr, ... ]
}
```

```json
{
  "and": [ FilterExpr, FilterExpr, ... ]
}
```

Compiles to: `(expr1 OR expr2)` / `(expr1 AND expr2)`

A top-level `filter`, `having`, or `where` array is implicitly ANDed.

**6. Variable-scoped property filter** (used in top-level `where`):

```json
{
  "variable": string,
  "property": PropertyRef,
  "op": FilterOp,
  "value": string | number | bool | null
}
```

Like form 1 but with an explicit `variable` field. The compiler
resolves the property using the entity type of the named variable.
Required in top-level `where` because there is no "current step"
entity context.

Example: `{ "variable": "p", "property": "phase", "op": "eq", "value": "Running" }`

Compiles to: `p['status']['phase'] = 'Running'`

Note: quantity-aware comparisons (memory sizes, CPU) require compiler
intrinsics and are deferred to V2. Raw string comparison with `gt` on
K8s quantity strings like "10Gi" would be lexicographic, not numeric.

In MatchStep.filter, form 1 (without `variable`) is preferred since
the entity is implicit. Form 6 is only needed in top-level `where`
and can also be used in boolean composition (form 5) when mixing
predicates across different variables.

### FilterOp (enum)

```
eq | neq | gt | lt | gte | lte |
starts_with | ends_with | contains |
is_null | is_not_null
```

`is_null` and `is_not_null` are unary — they ignore `value`/`value_ref`.

All string comparisons (`eq`, `starts_with`, `ends_with`, `contains`)
are **case-sensitive** by default. This matches K8s conventions — resource
names, namespaces, and labels are case-sensitive. The current gold
dataset has no case-insensitive queries. If needed in the future, a
`case_insensitive: true` flag can be added to FilterExpr, compiling to
`toLower(x) = toLower(value)` in Cypher.

### PropertyRef

A logical property name resolved by the compiler to a Cypher property
access path based on the entity type.

See the [Property Resolution](#property-resolution) section for the
complete per-entity mapping. Examples:

- `"name"` → `x['metadata']['name']` for most entity types,
  but `x['name']` for Host
- `"storage_class_name"` → `x['spec']['storageClassName']` for PV and PVC
- `"container_uid"` → `x['metadata']['uid']` for Container,
  `x['container_uid']` for Logs

### NegationClause

A `NOT EXISTS` pattern expressed as a list of MatchSteps — the same
construct the LLM already uses for the top-level `match` array. This
means the LLM does not need to learn a separate negation mini-language.

```json
{
  "match": [ MatchStep ]
}
```

The `match` array inside a NegationClause follows the exact same rules
as the top-level `match`, except:
- The first step can reference a previously bound variable from the
  outer query via its `from` field (anchoring the negation).
- Variables bound inside the negation are not visible outside it.
- The `bind` field on steps inside a negation may use a temporary name
  (e.g., `"_d"`) or be omitted if the entity is anonymous.

The compiler wraps the inner match chain in
`NOT EXISTS { MATCH ... }`.

**Example (h09 — pods not managed by any Deployment):**

```json
{
  "match": [
    { "entity": "Deployment", "bind": "_d" },
    { "entity": "ReplicaSet", "bind": "_rs",
      "from": { "variable": "_d", "relationship": "Manages" } },
    { "entity": "Pod", "bind": "p",
      "from": { "variable": "_rs", "relationship": "Manages" } }
  ]
}
```

Compiles to: `NOT EXISTS { MATCH (:Deployment)-[:Manages]->(:ReplicaSet)-[:Manages]->(p) }`

The last step references the outer bound variable `p` — the compiler
recognizes that `p` is already bound and emits it as a reference, not
a new binding. Direction is inferred from the schema as usual.

**Example (h13 — PVCs not bound to any PV):**

```json
{
  "match": [
    { "entity": "PersistentVolume", "bind": "_pv",
      "from": { "variable": "pvc", "relationship": "BoundTo" } }
  ]
}
```

Compiles to: `NOT EXISTS((pvc)-[:BoundTo]->(:PersistentVolume))`

The single step's `from` references the outer bound variable `pvc`
directly — no need to re-bind it inside the negation. This is cleaner
than a two-step version and proves that prefix-anchored negation works
natively with `from` pointing to an outer variable.

**Why reuse MatchStep**: The review correctly identified that the
earlier NegationPath construct (pairwise linked list with ref/entity
steps) forced the LLM to learn a separate mechanical syntax —
contradicting the IR's goal of simplifying what the LLM produces. By
reusing MatchStep, the LLM uses the same construct for both positive
and negative patterns. The compiler handles the NOT EXISTS wrapping
and variable scoping.

### PropertyJoin

Joins the current entity to a previously bound variable via property
equality, rather than a graph relationship. Used when the graph has no
direct edge (e.g., m14: Logs → Container via container_uid).

```json
{
  "local_property": PropertyRef,
  "remote_variable": string,
  "remote_property": PropertyRef
}
```

Compiles to: `WHERE l['container_uid'] = c['metadata']['uid']`

### UnwindStep

Iterates over a nested array property of a bound variable. The unwound
alias is **not** a graph entity — it is a raw JSON map. Property access
on it is **not** resolved via the entity property map; it uses a
separate unwind-specific property map.

```json
{
  "source_variable": string,
  "source_property": PropertyRef,
  "element_type": UnwindElementType,
  "as": string
}
```

- `source_variable`: a bound entity variable
- `source_property`: the array property to unwind (resolved via the
  entity's property map)
- `element_type`: declares the type of each unwound element, which
  determines what properties the compiler can access on the alias
- `as`: the alias for each unwound element

### UnwindElementType (enum)

Each element type has its own property map for compiler resolution.

**`k8s_container_spec`** — an element of `pod.spec.containers`:

| Logical name | Cypher access on alias |
|-------------|----------------------|
| `resources.requests.memory` | `container['resources']['requests']['memory']` |
| `resources.limits.memory` | `container['resources']['limits']['memory']` |
| `resources.requests.cpu` | `container['resources']['requests']['cpu']` |
| `resources.limits.cpu` | `container['resources']['limits']['cpu']` |
| `name` | `container['name']` |
| `image` | `container['image']` |

This is not a graph entity — it is the raw K8s container spec from the
Pod JSON. The property paths are direct bracket access on the unwound
map, not metadata-wrapped like graph entities.

Example (h03):
```json
{
  "source_variable": "p",
  "source_property": "spec.containers",
  "element_type": "k8s_container_spec",
  "as": "container"
}
```

Compiles to: `UNWIND p['spec']['containers'] AS container`

The `sum_memory_mib` intrinsic knows that its input comes from a
`k8s_container_spec` alias and accesses `container['resources']['requests']['memory']`
and `container['resources']['limits']['memory']`. The intrinsic's
ComputeExpr references the alias and the logical property name;
the compiler uses the element_type to resolve the access path.

Example ComputeExpr for h03:
```json
{
  "fn": "sum_memory_mib",
  "input": "container",
  "input_property": "resources.requests.memory",
  "alias": "total_requested_memory_mib"
}
```

The compiler looks up `"container"` → bound by UnwindStep with
`element_type: k8s_container_spec`, then resolves
`"resources.requests.memory"` → `container['resources']['requests']['memory']`
using the k8s_container_spec property map.

Future element types can be added as new UNWIND sources appear
(e.g., `k8s_volume_spec` for `pod.spec.volumes`).

Note: V1 supports exactly one UNWIND per query (the `unwind` field is
singular, not an array). A query needing two UNWINDs (e.g., unwind
both containers and volumes) cannot be represented. The gold set only
has h03 (one UNWIND). If multi-UNWIND is needed in V2, changing to
`"unwind": [UnwindStep]` is a backward-compatible schema change.

### AggregationStage

Each stage compiles to a `WITH` clause. Multiple stages can be chained
for multi-stage aggregation (e.g., h18).

```json
{
  "group_by": [ GroupKey ],
  "compute": [ ComputeExpr ],
  "having": [ FilterExpr ]
}
```

- `group_by`: keys to group by (variables, properties, or prior aliases)
- `compute`: aggregation expressions computed per group
- `having`: post-aggregation filters (compiled as `WHERE` after `WITH`)

#### Variable scope rules

In Cypher, `WITH` acts as a projection — any variable not listed is
dropped from scope. The compiler enforces these rules:

1. After an AggregationStage, only the following are in scope:
   - Variables and aliases listed in `group_by`
   - Aliases created by `compute`
2. Subsequent stages and the `return` block can only reference
   variables that survived the most recent stage's projection.
3. The compiler **automatically forwards** group_by variables into the
   WITH clause. It does NOT auto-forward all bound variables — only
   those explicitly listed in group_by.
4. If a ReturnExpr references a variable that was dropped by a stage,
   the compiler rejects the query with a validation error naming the
   dropped variable and the stage that dropped it.

This means the LLM must include every variable it needs later in the
`group_by` array. This is intentional — it matches Cypher's semantics
and prevents the compiler from silently generating queries with
unintended grouping behavior.

#### Aggregation-in-WITH vs aggregation-in-RETURN

Many reference queries aggregate directly in RETURN (e.g., h16:
`RETURN d['metadata']['name'] AS deployment, count(DISTINCT p) AS pod_count`).
The IR forces all aggregation through stages (WITH clauses). This
produces semantically equivalent but structurally different Cypher:

- Reference: `RETURN d['metadata']['name'], count(DISTINCT p)`
  — groups by the name string
- Compiled: `WITH d, count(DISTINCT p) AS pod_count
  RETURN d['metadata']['name'] AS deployment, pod_count`
  — groups by the node identity

**For the current gold dataset** these are equivalent: every
aggregation query either filters to a single namespace (where names
are unique) or groups by entity identity anyway. The compiled form
(WITH-based) is preferred because it makes grouping explicit and
composable with multi-stage pipelines.

**Semantic note**: because the IR always groups by entity identity
(via `group_by: [{ "variable": "d" }]`), the compiled Cypher is
strictly more granular than a raw `RETURN name, count(...)` which
groups by the name string. Identity-grouping never merges rows that
should be separate — it can only preserve distinctions that
property-grouping would collapse.

For the current gold dataset this distinction is invisible: every
aggregation query either filters to a single namespace (h01, h04,
h16) or groups by an entity type where identity and name are
equivalent in context (m19, m20). Identity-grouping produces
identical results to property-grouping for all 60 base questions.

For future cross-namespace queries where duplicate names exist across
namespaces (e.g., "count deployments named nginx across all
namespaces"), identity-grouping preserves per-entity rows while
property-grouping would merge them. If the user intends
property-level grouping, the LLM must use GroupKey form 2
(`{ "variable": "d", "property": "name", "alias": "deployment" }`).
The compiler does **not** reject form 1 plans — identity-grouping
is always a safe default. This is a documented behavioral difference,
not a validation error, because rejecting form 1 would block the
majority of gold-set aggregation queries (h01, h02, h04, m19, m20,
etc.) that correctly use identity-grouping.

### GroupKey

A group key can be one of three forms:

**1. Variable** (group by the entity itself):

```json
{ "variable": "ns" }
```

Compiles to: `ns` in the WITH/GROUP BY. Most common form — groups by
the bound entity, then returns properties of it.

**2. Property expression** (group by a specific property value):

```json
{ "variable": "p", "property": "phase", "alias": "phase" }
```

Compiles to: `p['status']['phase'] AS phase` in the WITH clause.
Used by h20 which groups by `(service, pod_phase)`.

**3. Alias** (group by a prior stage's output):

```json
{ "alias": "endpoint_slice_count" }
```

Used in second-stage aggregation where a prior computed value
becomes a grouping key.

### ComputeExpr

An aggregation or derived computation. The `alias` field is **optional**
on all forms. When omitted, the compiler generates a canonical alias
using these deterministic rules (no alternatives):

| fn | Canonical alias | Example |
|----|----------------|---------|
| `count` | `{entity_type}_count` | `pod_count` |
| `count_distinct` | `{entity_type}_count` | `namespace_count` |
| `collect` | `{entity_type}_list` | `pod_list` |
| `collect_distinct` | `{entity_type}_list` | `endpoint_address_list` |
| `sum` | `total_{entity_type}` | `total_pod` |
| `sum_memory_mib` | `total_{input_property}_mib` | `total_resources_requests_memory_mib` |
| `size` | `{source_alias}_size` | `endpoint_address_list_size` |

Where `{entity_type}` is the snake_cased entity type name of the
input variable (e.g., `EndpointSlice` → `endpoint_slice`), not the
bind name. This makes aliases independent of LLM bind-name choices.

#### Stage-alias uniqueness

All aliases within a single AggregationStage must be unique. The
alias namespace for a stage includes:

- **GroupKey variable** names (form 1: `{ "variable": "ns" }` — the
  variable name `ns` is carried through the WITH)
- **GroupKey property aliases** (form 2: `{ "variable": "p",
  "property": "phase", "alias": "phase" }`)
- **GroupKey forwarded aliases** (form 3: `{ "alias": "pod_count" }`
  — carries a prior stage alias into this stage's scope)
- **ComputeExpr aliases** (explicit or canonical)

All four sources share a single namespace within the stage's WITH
clause. The compiler enforces uniqueness across all of them.

Collision scenarios and resolution:

1. **Two ComputeExprs produce the same canonical alias** (e.g.,
   `count(p)` and `count_distinct(p)` both default to `pod_count`):
   **validation error**. The LLM must provide an explicit `alias` on
   at least one to disambiguate. Auto-suffixing (`pod_count_2`) was
   considered but rejected — later references in `having`,
   `stage_ref`, and `order_by` would need to predict the suffixed
   name, leaking compiler internals back into the LLM's output.
2. **A GroupKey alias collides with a ComputeExpr alias**: validation
   error (same reasoning).
3. **A GroupKey variable name collides with a ComputeExpr alias**:
   validation error.
4. **Cross-stage shadowing**: not an error. Each stage projects a
   new scope, so `pod_count` in stage 1 and `pod_count` in stage 2
   are independent. The second stage's alias shadows the first.

Rule: **all alias collisions within a single stage are validation
errors.** The compiler never auto-renames aliases. If a canonical
alias collides, the LLM must provide an explicit `alias` override
on at least one of the colliding expressions.

If the LLM provides explicit `alias` values that collide, the
compiler rejects with a validation error naming both expressions.

Three forms:

**1. Simple aggregation** (most common):

```json
{
  "fn": AggregationFn,
  "input": string,
  "alias": string | null
}
```

`input` is a variable name. The compiler resolves what to aggregate:
- For `count` / `count_distinct`: counts the entity
- For `collect` / `collect_distinct`: collects the entity's `name` property
- For `sum`: sums the variable (must be numeric)

**2. Aggregation over a property**:

```json
{
  "fn": AggregationFn,
  "input": string,
  "input_property": PropertyRef,
  "alias": string | null
}
```

Used when aggregating a specific property rather than the entity itself.
Example: `collect_distinct` of `ea.address` (h07).

**3. Derived computation** (post-aggregation):

```json
{
  "fn": DerivedFn,
  "input": string,
  "alias": string | null
}
```

Used for functions applied to aggregation results. Example: `size` of a
previously collected list (h07).

### AggregationFn (enum)

```
count | count_distinct | collect | collect_distinct | sum | sum_memory_mib
```

`sum_memory_mib` is a compiler intrinsic that expands to a 30-line CASE
expression for K8s memory unit conversion.

### DerivedFn (enum)

```
size
```

Note: `coalesce` is intentionally NOT a DerivedFn. It requires
multiple inputs from different variables, which doesn't fit the
single-input ComputeExpr shape. It is modeled as ReturnExpr form 3
(coalesce expression) instead.

### ReturnExpr

What to include in the RETURN clause. The `alias` field is **optional**
on all forms except coalesce. When omitted, the compiler generates a
canonical alias using these deterministic rules:

**Canonical alias generation** (when `alias` is omitted):

All canonical aliases are derived from the **entity type name**
(lowercased, snake_cased), never from the bind name. This makes
output shape independent of arbitrary LLM bind-name choices.

1. **Entity property return** (`variable` + `property`):
   - `name` property: alias is the entity type name snake_cased
     (`Namespace` → `namespace`, `Service` → `service`,
     `Pod` → `pod`, `PersistentVolume` → `persistent_volume`,
     `EndpointSlice` → `endpoint_slice`,
     `IngressServiceBackend` → `ingress_service_backend`)
   - Other properties: `{entity_type}_{property}`
     (`Pod.phase` → `pod_phase`, `Deployment.ready_replicas` →
     `deployment_ready_replicas`)
   - Collision resolution: if two return columns would get the same
     canonical alias (e.g., two different Services both returning
     `name`), append a numeric suffix: `service`, `service_2`.
     This is deterministic based on return-column order.
2. **Stage reference return** (stage alias reference):
   inherits the ComputeExpr alias (which itself may be generated;
   see ComputeExpr canonical alias table)
3. **Coalesce**: requires explicit `alias` (no reasonable default)

This reduces answer-shape drift for the common case. Bind names like
`s`, `p`, `_es` have no effect on output column names.

**Limitation**: canonical aliases do not cover all gold column names.
Non-canonical names appear across all difficulty tiers:
- Easy: `node_name` (e07), `service_account` (e10),
  `persistent_volume_claim` (e16), `container_name` (e20)
- Medium: `replica_set` (m01), `daemon_set` (m03),
  `stateful_set` (m04), `pvc_claims` (m12)
- Hard: `service_name`/`pod_name` (h06),
  `distinct_backing_pod_ip_count` (h07), `backend_service` (h19)

For these, the LLM must provide explicit `alias` overrides. The
canonical defaults handle the simplest cases (single-entity list
queries like e01–e06, e08, e11–e15, e17–e19); the rest require
model-chosen aliases.

This is an acceptable tradeoff for V1 — the alternative (forcing all
aliases to be explicit) would increase IR verbosity for every query.
Eval should track the explicit-vs-canonical alias ratio to measure
whether the canonical defaults are pulling their weight.

Three forms:

**1. Entity property**:

```json
{
  "variable": string,
  "property": PropertyRef,
  "alias": string | null
}
```

Compiles to: `ns['metadata']['name'] AS namespace`

**2. Stage reference** (reference to a stage output — either a
ComputeExpr alias or a GroupKey alias):

```json
{
  "stage_ref": string,
  "alias": string | null
}
```

`stage_ref` can reference any alias produced by an AggregationStage:
- ComputeExpr aliases (e.g., `pod_count`, `endpoint_slice_count`)
- GroupKey property aliases (e.g., `phase` from
  `{ "variable": "p", "property": "phase", "alias": "phase" }`)

Compiles to: `pod_count` or `pod_count AS count_of_pods`

**3. Coalesce expression** (multi-input derived value):

```json
{
  "coalesce": [
    { "variable": string, "property": PropertyRef },
    { "variable": string, "property": PropertyRef }
  ],
  "alias": string
}
```

Used by h10: `coalesce(pvc['spec']['storageClassName'], pv['spec']['storageClassName'])`.

### OrderSpec

```json
{
  "column": string,
  "direction": "asc" | "desc"
}
```

`column` references a ReturnExpr alias.

## Compiler Intrinsics

Domain-specific functions the compiler expands into Cypher.

| Intrinsic | Used by | Description | Cypher expansion |
|-----------|---------|-------------|-----------------|
| `sum_memory_mib` | h03 | Sum K8s memory strings as MiB | 30-line CASE + sum() |

Note: `coalesce` is NOT an intrinsic — it is modeled as ReturnExpr
form 3 (coalesce expression). See the ReturnExpr section.

### Why sum_memory_mib is an intrinsic

The CASE expression for K8s memory unit conversion
(Gi/Mi/Ki/Ti/G/M/k/bytes → MiB) is 30 lines of Cypher that every
correct answer must include identically. Making the LLM generate this
each time is wasteful and error-prone. The IR says "sum memory in MiB"
and the compiler emits the correct CASE.

### sum_memory_mib multi-stage expansion

`sum_memory_mib` is not a simple inline function — it requires the
compiler to inject **intermediate WITH clauses** that are not
represented in the IR's `stages` array. The expansion happens between
the `unwind` step and the user-defined `stages`.

Given an AggregationStage with `sum_memory_mib`:

```json
{
  "unwind": { "source_variable": "p", "source_property": "spec.containers",
              "element_type": "k8s_container_spec", "as": "container" },
  "stages": [{
    "group_by": [{ "variable": "ns" }],
    "compute": [
      { "fn": "sum_memory_mib", "input": "container",
        "input_property": "resources.requests.memory",
        "alias": "total_requested_memory_mib" },
      { "fn": "sum_memory_mib", "input": "container",
        "input_property": "resources.limits.memory",
        "alias": "total_limit_memory_mib" }
    ]
  }]
}
```

The compiler emits:

```cypher
-- from unwind:
UNWIND p['spec']['containers'] AS container
-- injected intermediate WITH (per-row unit conversion):
WITH ns,
  CASE
    WHEN container['resources']['requests']['memory'] IS NULL THEN 0.0
    WHEN container['resources']['requests']['memory'] ENDS WITH 'Gi' THEN ...
    ...
  END AS _req_mib,
  CASE
    WHEN container['resources']['limits']['memory'] IS NULL THEN 0.0
    ...
  END AS _lim_mib
-- from stages[0] (aggregation):
RETURN ns['metadata']['name'] AS namespace,
       sum(_req_mib) AS total_requested_memory_mib,
       sum(_lim_mib) AS total_limit_memory_mib
ORDER BY namespace
```

The compiler rule: **when a stage contains `sum_memory_mib` compute
expressions, the compiler inserts an intermediate WITH clause before
the stage that converts each memory string to a numeric MiB value
per-row. The stage's `sum_memory_mib` then compiles to a plain
`sum()` over the intermediate alias.** The intermediate aliases use
a `_` prefix to avoid collision with user aliases.

This injection is implicit — the IR author writes `sum_memory_mib`
and the compiler handles the two-layer expansion. The intermediate
WITH carries forward all `group_by` variables from the stage.

Future intrinsics (not needed for current dataset):
- `sum_cpu_millicores`: K8s CPU string conversion (same two-layer pattern)
- `parse_duration_seconds`: K8s duration string conversion

## Property Resolution

The compiler resolves logical property names to Cypher access paths.
Resolution is **per-entity-type**, not a global alias table.

### Resolution rules

1. Look up `(entity_type, logical_name)` in the typed property map
2. If found, use the entity-specific Cypher path
3. If not found, reject as a validation error

### Extensibility

The property maps below are **minimal** — they include only properties
needed by the current gold dataset. Real-world queries will request
properties not yet mapped (e.g., Pod.node_name, Deployment.strategy,
Node.capacity).

Adding a new property requires a single entry in the relevant entity's
property map: `(logical_name, cypher_access_path)`. No schema version
bump is needed — the property map is a compiler configuration, not
part of the IR schema.

The compiler should log a structured warning when rejecting an unknown
property, including the entity type and the requested logical name.
This makes it easy to identify which properties to add as usage
patterns evolve. In a future version, a passthrough mode could allow
raw bracket-path access for unmapped properties, but V1 prioritizes
validation strictness over flexibility.

### Per-entity property map

**Shared (all entity types with metadata)**:

| Logical name | Cypher access | Entities |
|-------------|--------------|----------|
| `name` | `x['metadata']['name']` | all except Host |
| `namespace` | `x['metadata']['namespace']` | all namespaced resources |
| `uid` | `x['metadata']['uid']` | all |

**Host** (no metadata wrapper):

| Logical name | Cypher access |
|-------------|--------------|
| `name` | `x['name']` |

**Pod**:

| Logical name | Cypher access |
|-------------|--------------|
| `phase` | `x['status']['phase']` |
| `spec.containers` | `x['spec']['containers']` |

**Deployment**:

| Logical name | Cypher access |
|-------------|--------------|
| `replicas` | `x['spec']['replicas']` |
| `ready_replicas` | `x['status']['readyReplicas']` |

**StatefulSet**:

| Logical name | Cypher access |
|-------------|--------------|
| `replicas` | `x['spec']['replicas']` |

**Container**:

| Logical name | Cypher access |
|-------------|--------------|
| `container_type` | `x['container_type']` |
| `pod_name` | `x['pod_name']` |
| `container_uid` | `x['metadata']['uid']` |

**Logs**:

| Logical name | Cypher access |
|-------------|--------------|
| `content` | `x['content']` |
| `container_uid` | `x['container_uid']` |

Note: `container_uid` resolves differently for Container vs Logs.
This is intentional — Container stores it in metadata, Logs stores it
as a top-level property.

**EndpointSlice**:

| Logical name | Cypher access |
|-------------|--------------|
| `address_type` | `x['addressType']` |

**EndpointAddress**:

| Logical name | Cypher access |
|-------------|--------------|
| `address` | `x['address']` |

**PersistentVolume**:

| Logical name | Cypher access |
|-------------|--------------|
| `phase` | `x['status']['phase']` |
| `storage_class_name` | `x['spec']['storageClassName']` |
| `capacity_storage` | `x['spec']['capacity']['storage']` |

**PersistentVolumeClaim**:

| Logical name | Cypher access |
|-------------|--------------|
| `phase` | `x['status']['phase']` |
| `storage_class_name` | `x['spec']['storageClassName']` |
| `volume_name` | `x['spec']['volumeName']` |

**Node**:

| Logical name | Cypher access |
|-------------|--------------|
| `phase` | `x['status']['phase']` |
| `provider_id` | `x['spec']['providerID']` |

Note: the gold query for m08 filters node name with an OR across two
physical paths: `n['metadata']['name'] = X OR n['name'] = X`. This is
a legacy data issue — Node entities in the graph have `name` at both
paths. The compiler handles this by emitting an OR filter when
filtering Node by `name`. m08 has empty expected rows, so this does
not affect current eval scoring, but the compiler must handle it.

**Event**:

| Logical name | Cypher access |
|-------------|--------------|
| `type` | `x['type']` |
| `reason` | `x['reason']` |
| `note` | `x['note']` |
| `event_time` | `x['eventTime']` |

### Relationship Direction Resolution

The compiler validates relationship endpoints and knows the canonical direction:

| Relationship | From | To |
|-------------|------|-----|
| BelongsTo | * (namespaced) | Namespace |
| Manages | Deployment | ReplicaSet |
| Manages | ReplicaSet | Pod |
| Manages | StatefulSet | Pod |
| Manages | DaemonSet | Pod |
| Manages | Job | Pod |
| Manages | Service | EndpointSlice |
| RunsOn | Pod | Node |
| Runs | Container | Pod |
| DefinesBackend | Ingress | IngressServiceBackend |
| TargetsService | IngressServiceBackend | Service |
| IsClaimedBy | Host | Ingress |
| ContainsEndpoint | EndpointSlice | Endpoint |
| HasAddress | Endpoint | EndpointAddress |
| IsAddressOf | EndpointAddress | Pod |
| ClaimsVolume | Pod | PersistentVolumeClaim |
| BoundTo | PersistentVolumeClaim | PersistentVolume |
| UsesStorageClass | PersistentVolume | StorageClass |
| UsesProvisioner | StorageClass | Provisioner |
| Concerns | Event | * (see below) |
| ListedIn | EndpointAddress | EndpointSlice |
| MountsConfig | Pod | ConfigMap |
| InjectsConfig | Pod | ConfigMap |
| UsesIdentity | Pod | ServiceAccount |
| AppliesTo | NetworkPolicy | Pod |
| PartOf | Node | Cluster |  (topology membership)

Note: `ListedIn` connects EndpointAddress → EndpointSlice (the
address is listed in the slice). This is the reverse direction of the
`ContainsEndpoint → HasAddress` chain. Neither `ListedIn` nor `PartOf`
are used by the current gold dataset; both are included for
completeness.

#### Wildcard entity sets

**BelongsTo: * → Namespace** — the `*` stands for all namespaced entity
types:

```
Pod | Deployment | StatefulSet | ReplicaSet | DaemonSet | Job |
Service | Ingress | EndpointSlice | NetworkPolicy |
ConfigMap | Container | ServiceAccount | Event |
PersistentVolumeClaim
```

Not namespaced (excluded from BelongsTo): Node, Namespace, Cluster,
StorageClass, Provisioner, PersistentVolume, IngressServiceBackend,
Endpoint, EndpointAddress, Host, Logs.

**Concerns: Event → \*** — Event can concern any entity type. The gold
dataset uses Event → Pod (h17) and Event → Service (m17). The compiler
accepts any `(Event, X, Concerns)` pair without restricting `X`.

## Representability Checklist

**Status: sketch.** This checklist summarizes which constructs each
question requires. It is NOT a proof of representability — that
requires the hand-written IR audit against `reference_cypher`
(recommended next step 1). The checklist patterns below are
abbreviated and may omit columns that the reference query returns.

### Easy (e01–e20)

| ID | Question | Repr. | IR pattern | Constructs used |
|----|----------|:---:|-----------|----------------|
| e01 | List all namespaces | ✅ | match Namespace, return name | MatchStep, ReturnExpr.property |
| e02 | Count namespaces | ✅ | match Namespace, stage count | AggregationStage, ComputeExpr.simple |
| e03 | Pods in ns litmus | ✅ | match Pod→BelongsTo→Namespace, filter name=litmus | MatchStep.from, FilterExpr.literal |
| e04 | Services in ns litmus | ✅ | same pattern, Service | |
| e05 | Ingresses in ns litmus | ✅ | same pattern, Ingress | |
| e06 | EndpointSlices in ns litmus | ✅ | same pattern, EndpointSlice | |
| e07 | All nodes | ✅ | match Node, return name+phase+provider_id | Node.phase, Node.provider_id |
| e08 | All storage classes | ✅ | match StorageClass, return name | |
| e09 | All PVs | ✅ | match PV, return name (gold also returns phase, storage_class, capacity) | PV.phase, PV.storage_class_name, PV.capacity_storage |
| e10 | ServiceAccounts in ns litmus | ✅ | same pattern, ServiceAccount | |
| e11 | Deployments in ns litmus | ✅ | same pattern, Deployment | |
| e12 | ReplicaSets in ns litmus | ✅ | same pattern, ReplicaSet | |
| e13 | StatefulSets in ns litmus | ✅ | same pattern, StatefulSet | |
| e14 | DaemonSets in ns litmus | ✅ | same pattern, DaemonSet | |
| e15 | Jobs in ns litmus | ✅ | same pattern, Job | |
| e16 | PVCs in ns litmus | ✅ | same pattern, PVC | |
| e17 | Count pods in ns litmus | ✅ | match + stage count_distinct | ComputeExpr.simple |
| e18 | ConfigMaps in ns litmus | ✅ | same pattern, ConfigMap | |
| e19 | NetworkPolicies in ns litmus | ✅ | same pattern, NetworkPolicy | |
| e20 | Containers in ns litmus | ✅ | match Container→BelongsTo→Namespace, return name+pod_name | |

**Coverage: 20/20**

### Medium (m01–m20)

| ID | Question | Repr. | IR pattern | Constructs used |
|----|----------|:---:|-----------|----------------|
| m01 | Deployments → RS in litmus | ✅ | multi-step match chain | MatchStep.from (chained) |
| m02 | RS → Pods (collect) | ✅ | match + stage collect_distinct | ComputeExpr.simple |
| m03 | DaemonSets → Pods | ✅ | multi-step match | |
| m04 | StatefulSets → Pods | ✅ | multi-step match | |
| m05 | Jobs → Pods | ✅ | multi-step match | |
| m06 | Services → ES (collect) | ✅ | match + stage collect_distinct | |
| m07 | Services → backing Pods | ✅ | optional 6-hop chain, collect | MatchStep.optional |
| m08 | Pods on node | ✅ | match + filter | |
| m09 | PVCs → PVs | ✅ | multi-step match | |
| m10 | Events → Pods | ✅ | multi-step match | |
| m11 | Pods → Nodes | ✅ | multi-step match | |
| m12 | Pods → PVCs (optional, collect) | ✅ | optional match + collect | MatchStep.optional |
| m13 | Containers → Pods | ✅ | multi-step match | |
| m14 | Containers → Logs | ✅ | optional + property_join | MatchStep.property_join, MatchStep.optional |
| m15 | Ingresses → Services | ✅ | 4-hop chain through ISB + distinct | distinct |
| m16 | ES → Pods (via endpoints) | ✅ | 4-hop chain + collect | |
| m17 | Events → Services | ✅ | multi-step match | |
| m18 | PVs → StorageClasses | ✅ | multi-step match | |
| m19 | Per-node pod count | ✅ | optional + count_distinct | AggregationStage |
| m20 | Per-namespace service count | ✅ | match + stage count_distinct | AggregationStage |

**Coverage: 20/20**

### Hard (h01–h20)

| ID | Question | Repr. | IR pattern | Constructs used |
|----|----------|:---:|-----------|----------------|
| h01 | Top-5 ns by pod count | ✅ | match + stage count + limit 5 | limit |
| h02 | Nodes with >50 pods | ✅ | match + stage count + having >50 | having with FilterExpr.alias |
| h03 | Memory per namespace | ✅ | unwind + stage sum_memory_mib | UnwindStep, intrinsic |
| h04 | Services with no ES | ✅ | optional + stage count + having =0 | having with FilterExpr.alias |
| h05 | Ingresses with endpoints | ✅ | multi-hop chain | |
| h06 | Host → services + pods | ✅ | 8-hop chain from Host | |
| h07 | Per-svc distinct pod IPs | ✅ | collect_distinct property + size | ComputeExpr.property, DerivedFn.size |
| h08 | Deployments: ready < desired | ✅ | property-vs-property filter | FilterExpr.value_ref |
| h09 | Orphan pods | ✅ | 5× not_exists paths | NegationClause (suffix-anchored) |
| h10 | PVCs + PV + storage class | ✅ | optional + coalesce return | ReturnExpr.coalesce |
| h11 | Services with no ES (litmus) | ✅ | same as h04, different ns | |
| h12 | Ingress backends with no ES | ✅ | chain + optional + having =0 | |
| h13 | Unbound PVCs | ✅ | not_exists PVC→BoundTo→PV | NegationClause (prefix-anchored) |
| h14 | Pods + PVC + SC-less PV | ✅ | filter with OR(is_null, eq='') + distinct | FilterExpr.or, distinct |
| h15 | Nodes with >1 ns | ✅ | chain + count_distinct + having | having with FilterExpr.alias |
| h16 | Deployment pod count via RS | ✅ | chain + count_distinct | |
| h17 | Pods with Warning events | ✅ | filter type=Warning + distinct | distinct |
| h18 | Per-ns count services with 0 ES | ✅ | two-stage aggregation | multi AggregationStage, FilterExpr.variable_null, FilterExpr.alias |
| h19 | Hosts → backend services | ✅ | 4-hop chain | |
| h20 | Per-svc pods by phase | ✅ | optional chain + group by (svc, phase) | GroupKey.property for phase |

**Coverage: 20/20**

## Constructs Required (summary)

| Construct | Defined in | Used by |
|-----------|-----------|---------|
| MatchStep with RelationshipSpec | MatchStep, RelationshipSpec | e03–e20, m01–m20, h01–h20 |
| MatchStep.optional (single step) | MatchStep | m12, m14, h04, h10–h12 |
| MatchStep.optional (multi-hop chain) | MatchStep, optional chains | m07, h18–h20 |
| MatchStep.not_exists + NegationClause (suffix-anchored) | NegationClause | h09 |
| MatchStep.not_exists + NegationClause (prefix-anchored) | NegationClause | h13 |
| MatchStep.property_join | PropertyJoin | m14 |
| FilterExpr (literal) | FilterExpr form 1 | e03–e20, h17 |
| FilterExpr (property-vs-property) | FilterExpr form 2 | h08 |
| FilterExpr (boolean OR) | FilterExpr form 5 (boolean composition) | h14 |
| AggregationStage (single) | AggregationStage | e02, e17, m02, m06, m19–m20, h01–h02, h04, h07, h11–h12, h15–h16 |
| AggregationStage (multi-stage) | AggregationStage (chained) | h18 |
| AggregationStage.having (FilterExpr.alias) | FilterExpr form 3 (alias-vs-literal) | h02, h04, h11–h12, h15, h18 |
| AggregationStage.having (FilterExpr.variable_null) | FilterExpr form 4 (variable-null) | h18 |
| GroupKey.variable | GroupKey form 1 | all aggregation questions |
| GroupKey.property | GroupKey form 2 | h20 |
| ComputeExpr (simple: fn + input) | ComputeExpr form 1 | e02, e17, m02, m19–m20, h01–h02, h04, h16, h18 |
| ComputeExpr (property: fn + input + input_property) | ComputeExpr form 2 | h07 (collect_distinct ea.address) |
| ComputeExpr (derived: size of prior alias) | ComputeExpr form 3 | h07 |
| Intrinsic: sum_memory_mib | AggregationFn | h03 |
| ReturnExpr (entity property) | ReturnExpr form 1 | all questions |
| ReturnExpr (stage reference) | ReturnExpr form 2 | all aggregation questions |
| ReturnExpr (coalesce) | ReturnExpr form 3 | h10 |
| UnwindStep | UnwindStep | h03 |
| OrderSpec | OrderSpec | most questions |
| limit | top-level | h01 |
| distinct | top-level | m15, h05, h06, h14, h17 |
| Per-entity property resolution | Property Resolution | all questions |

## What's NOT needed

These Cypher features are absent from the gold dataset and can be omitted from V1:

- UNION / UNION ALL
- MERGE / CREATE / SET / DELETE (read-only IR)
- Regular expressions
- String functions beyond what intrinsics cover
- Path variables / shortestPath
- CALL procedures
- Subqueries (WITH ... AS) beyond what stages model
- FOREACH
- Generic expression trees (all needed expressions fit the defined forms)

## Fallback to raw Cypher

The IR has a hard expressiveness ceiling — it cannot represent UNION,
regex, CALL procedures, path variables, or any Cypher feature not
modeled by the schema above. For queries that exceed V1 capabilities,
the system must provide a fallback.

**Design**: The LLM output is a discriminated union (see
[TranslatorOutput](#translatoroutput-envelope) in the schema section):

```json
{ "mode": "plan", "plan": { "$schema": "QueryPlanV1", ... } }
```

or:

```json
{ "mode": "cypher", "cypher": "MATCH ... RETURN ...", "reason": "requires UNION" }
```

### Execution policy

The runtime processes the output in three phases:

**Phase 1 — plan-first.** If `mode` is `"plan"`, the compiler
validates the IR (structural + semantic checks). If validation passes,
the compiled Cypher is executed. If validation fails, the runtime
proceeds to phase 2.

**Phase 2 — retry.** The runtime re-prompts the LLM once with the
validation error, asking it to fix the plan. If the second attempt
passes, the compiled Cypher is executed. If it also fails, the
runtime falls back to raw Cypher generation (a fresh `mode: "cypher"`
call). This retry prevents premature escape to raw Cypher for queries
that were representable but had a minor IR error.

**Phase 3 — structural Cypher gate.** When the runtime receives
`mode: "cypher"` (either from the LLM's first response or after a
phase 2 fallback), the `reason` field is **required**. The runtime
does NOT trust the reason string alone. Instead, it performs two
checks on the supplied Cypher:

**Check A — write-operation rejection.** The runtime scans for
`CREATE`, `MERGE`, `SET`, `DELETE`, and `DETACH DELETE`. If any are
found, the query is **rejected unconditionally** — the system is
read-only. This is not a bypass feature; it is a safety boundary.
The LLM is re-prompted with an error explaining that write operations
are not permitted.

**Check B — bypass-feature scan.** The runtime scans for features
that are outside V1 scope but are legitimate read-only Cypher:

Allowed bypass features (the Cypher must actually contain one):
- `UNION` / `UNION ALL`
- Regular expression operators (`=~`)
- `CALL` procedure invocations (`CALL db.` or `CALL { ... }`)
- `shortestPath` / `allShortestPaths`
- `FOREACH`
- `REDUCE` / list comprehensions (`[x IN ... | ...]`)
- Variable-length path patterns (`*` inside a relationship bracket,
  e.g., `-[:REL*1..3]->`)
- Path variable bindings (`p = (a)-[...]->(b)` — the `var =`
  before a pattern)

The scan is purely syntactic — each feature maps to a specific token
or token pattern that can be detected by regex without context. String
functions (`replace`, `toLower`, `toUpper`, `split`, etc.) are
intentionally **not** in the bypass list. They appear in gold-set
queries (h03 uses `replace` as part of the memory-conversion CASE
that `sum_memory_mib` handles), so accepting them as bypass tokens
would let the LLM escape the compiler for IR-representable queries.
If a raw-Cypher query uses string functions for something genuinely
outside V1, it will also contain one of the structural tokens above
(e.g., a `REDUCE`, list comprehension, or `CALL`).

If the Cypher does not contain any bypass features and passes check A,
the runtime **rejects the cypher mode**. What happens next depends on
the call budget (see below).

#### Call budget and control flow

The runtime enforces a **hard cap of 3 total LLM calls** per query.
Every phase 1/2/3 prompt counts against this budget. Re-prompting
(plan retry, cypher rejection → plan mode, write rejection) only
occurs **if calls remain in the budget**. Once the budget is
exhausted, the runtime returns a terminal failure with the last
error — no further re-prompting.

Typical flows:
- Happy path: plan → success (1 call)
- Plan retry: plan → fail → plan retry → success (2 calls)
- Plan fallback: plan → fail → retry fail → cypher accepted (3 calls)
- Cypher rejected: cypher → rejected → plan retry → success (2 calls)
- Terminal: plan → fail → retry fail → cypher rejected → terminal
  failure (3 calls exhausted, no execution)

The budget prevents oscillation between plan and cypher modes. A
query that exhausts all 3 calls is logged as `terminal_failure`.

### Metrics

Eval tracks:
- `plan_success`: plan mode succeeded (1 call)
- `plan_retry_success`: plan failed, retry succeeded (2 calls)
- `plan_fallback`: plan failed twice, cypher accepted (3 calls)
- `cypher_accepted`: cypher mode, bypass feature confirmed
- `cypher_rejected`: cypher mode rejected (no bypass feature found)
- `terminal_failure`: call budget exhausted with no execution

The `cypher_rejected` rate measures unnecessary bypass attempts.
The `terminal_failure` rate measures queries beyond the system's
combined plan+fallback capability.

## Conclusion

**60/60 base questions believed representable. Pending confirmation
by the hand-written IR audit (recommended next step 1).**

The representability checklist is a sketch that identifies which
constructs each question needs. It is not a proof until each question's
IR is compiled and verified against `reference_cypher`. Known edge
cases: m08 has a Node name dual-path issue requiring a compiler
special case; ~20% of return columns need explicit alias overrides
that the canonical defaults cannot derive.

Every construct referenced in the representability table is formally
defined in the schema section above. The schema has:

- 6 forms of FilterExpr (property-vs-literal, property-vs-property,
  alias-vs-literal, variable-null-check, boolean composition,
  variable-scoped property)
- 3 forms of GroupKey (variable, property expression, alias)
- 3 forms of ComputeExpr (simple, property, derived)
- 3 forms of ReturnExpr (property, stage reference, coalesce) with
  optional aliases and compiler-generated canonical defaults
- NegationClause reusing MatchStep (no separate negation syntax)
- Optional chains for multi-hop OPTIONAL MATCH compilation
- PropertyJoin for non-relationship joins
- Per-entity typed property resolution (not a global alias table)
- Pairwise relationship legality table (Manages expanded to 6 rows)
- Semantic plan validator (not just JSON Schema)
- 1 compiler intrinsic (sum_memory_mib)

## Design notes

Responses to the [query_plan_v1_review.md](query_plan_v1_review.md):

### 1. 60 vs 162 question gap

The representability table covers the 60 base questions. The 102
expanded variants are namespace/host substitutions of the same query
patterns — they use identical IR constructs with different literal
values in filters. The compiler handles this naturally (a filter
`name = "litmus"` vs `name = "pyroscope"` is the same construct).

Empty-result variants also use the same IR — the query structure is
identical; only the result set differs. No new constructs are needed.

Validation against the expanded 162-question set should be done during
implementation (step 3 below) but does not require schema changes.

### 2. NegationPath → NegationClause (accepted)

The original NegationPath with its pairwise ref/entity linked list was
a separate mechanical syntax that contradicted the IR's goal. Replaced
with NegationClause, which reuses the MatchStep construct the LLM
already knows. See the NegationClause section above.

### 3. Direction removed from RelationshipSpec (accepted)

The compiler infers direction from the schema's (from, to) table.
Forcing the LLM to specify direction was an unnecessary failure mode.
See the RelationshipSpec section above.

### 4. OPTIONAL MATCH / global WHERE distinction

Added a top-level `where: [FilterExpr]` field for post-match global
filters. This is not needed by the current 60-question gold set
(all filters are either step-scoped or stage-scoped), but it future-
proofs the IR for queries that need row-level filtering after all
MATCH/OPTIONAL MATCH clauses have settled — e.g., "find pods,
optionally their PVCs, but only return rows where the pod phase is
Running or the PVC is null."

The predicate placement rule for optional steps is documented: all
predicates on an optional MatchStep compile inside that step's
`OPTIONAL MATCH ... WHERE` clause, preserving null rows.

### 5. Scope dropping in WITH (accepted)

Added explicit variable scope rules to AggregationStage. The compiler
auto-forwards group_by variables but does NOT auto-forward all bound
variables. ReturnExpr references to dropped variables are rejected
with a validation error. See AggregationStage above.

### 6. sum_memory_mib as filter

Currently `sum_memory_mib` is only usable as an AggregationFn. If a
future question needs "find pods requesting more than 1Gi memory",
it would require a per-row memory conversion, not an aggregation.

This could be handled by:
- Adding a `convert_memory_mib` DerivedFn usable in ComputeExpr
- Or adding a `memory_mib` intrinsic to FilterExpr

Not needed for the current dataset. Deferred to V2 if the gold set
expands to include per-row memory filtering.

### 7. String matching case sensitivity

Documented: all string comparisons are case-sensitive by default,
matching K8s conventions. A `case_insensitive` flag can be added to
FilterExpr if needed in the future. See FilterOp section above.

## Recommended next steps

1. **Hand-write IR for all 60 base questions** against the actual
   `reference_cypher`, not just the expected-row scorer. The audit
   must confirm the compiled Cypher is semantically equivalent to
   the reference, including column shape and property access paths.
2. **Smoke-test LLM IR generation early** (10 questions, mixed
   difficulty). The central assumption — that LLMs generate the IR
   more accurately than raw Cypher — is untested. The IR schema is
   substantial (6 FilterExpr forms, 3 GroupKey forms, 3 ComputeExpr
   forms, NegationClause, etc.). A quick smoke test validates that
   the schema is learnable before committing to the full compiler.
   Run this in parallel with step 1, not after step 4.
3. Define the JSON Schema for TranslatorOutput (the root envelope
   with QueryPlanV1 as a nested definition) for structured LLM output
4. Implement the compiler in Python (Cypher string builder from IR)
5. Validate: compile each hand-written IR, execute via MCP, verify
   match against expected rows for all 162 questions
6. Measure plan/cypher fallback split in eval to track IR coverage
