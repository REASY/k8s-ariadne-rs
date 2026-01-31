# English-to-Cypher translator issue: nondeterministic edge selection

## Summary
When asked the exact same natural-language question twice, the English-to-Cypher translator produced two different Cypher queries. The first query returned **0 rows** because it used an invalid edge (`EndpointAddress <-[:ListedIn]- Endpoint`) that does **not** exist in the graph schema. The second query used the correct edge (`Endpoint -[:HasAddress]-> EndpointAddress`) and returned results.

This is not a data inconsistency. It is a **translator variability + schema mismatch** issue.

## Symptom
Two consecutive runs of the same question:

Question:
```
What are the pods backing DNS name litmus.qa.xyz.is?
```

### Run #1 (incorrect)
Returned 0 rows.

Problematic fragment:
```
... (es:EndpointSlice)-[:ContainsEndpoint]->(e:Endpoint)
<-[:ListedIn]-(ea:EndpointAddress)-[:IsAddressOf]->(p:Pod)
```

This assumes `EndpointAddress -[:ListedIn]-> Endpoint`, which is **not** part of the schema.

### Run #2 (correct)
Returned expected pods.

Correct fragment:
```
... (es:EndpointSlice)-[:ContainsEndpoint]->(e:Endpoint)
-[:HasAddress]->(ea:EndpointAddress)-[:IsAddressOf]->(p:Pod)
```

## Actual schema relationships (relevant)
From the application schema:
- `Endpoint -[:HasAddress]-> EndpointAddress`
- `EndpointAddress -[:ListedIn]-> EndpointSlice`
- `EndpointAddress -[:IsAddressOf]-> Pod`

So `ListedIn` connects **EndpointAddress → EndpointSlice**, not **EndpointAddress → Endpoint**.

## Root cause
The translator is **nondeterministic** and sometimes chooses incorrect edge types or directions. This creates syntactically valid Cypher that returns empty results, even though the intent is correct.

## Impact
- Same question can yield different results
- Incorrect queries can silently return empty sets
- Makes trust in the translator brittle

## Mitigations / next steps
1. **Determinize generation**: set temperature = 0 for the translation model.
2. **Schema-aware validation**: reject queries that contain edges not present in the schema, then retry translation with feedback.
3. **Add canonical path hints**: include explicit “Ingress → Service → EndpointSlice → Pod” path guidance in the prompt.

## References
- Graph schema excerpt: `python/agent/adk_config/k8s_graph_agent/root_agent.yaml`
- Type/edge definitions: `ariadne-core/src/types/mod.rs`
