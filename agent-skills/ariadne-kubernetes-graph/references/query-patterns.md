# Ariadne Query Patterns

Use this reference when writing or repairing Ariadne queries. It contains the high-signal graph paths and a few proven query patterns.

## Core Traversal Paths

These are the graph-specific edges and decomposed nodes that differ from standard Kubernetes API structure.

DNS / hostname to backend service:

`Host-[IsClaimedBy]->Ingress-[DefinesBackend]->IngressServiceBackend-[TargetsService]->Service`

Service to backing pods:

`Service-[Manages]->EndpointSlice-[ContainsEndpoint]->Endpoint-[HasAddress]->EndpointAddress-[IsAddressOf]->Pod`

Deployment to pods:

`Deployment-[Manages]->ReplicaSet-[Manages]->Pod`

Other workload owners to pods:

- `StatefulSet-[Manages]->Pod`
- `DaemonSet-[Manages]->Pod`
- `Job-[Manages]->Pod`
- `Node-[Manages]->Pod`

Storage chain:

`Pod-[ClaimsVolume]->PersistentVolumeClaim-[BoundTo]->PersistentVolume-[UsesStorageClass]->StorageClass`

Container resources:

`Container-[Runs]->Pod`

Do not assume `-[BelongsTo]->Namespace` or `-[PartOf]->Cluster` exists for every label. If those edges matter and you are not reusing a known-good pattern, confirm them with `graph_schema`.

## Example Queries

### Pods per node in a namespace

```cypher
MATCH (p:Pod)-[:RunsOn]->(n:Node)
WHERE p['metadata']['namespace'] = $namespace
RETURN n['metadata']['name'] AS node_name, count(p) AS pod_count
ORDER BY pod_count DESC
LIMIT 25
```

### Hostname to backend services

```cypher
MATCH (h:Host)-[:IsClaimedBy]->(:Ingress)
  -[:DefinesBackend]->(:IngressServiceBackend)
  -[:TargetsService]->(s:Service)
WHERE h['name'] = $hostname
RETURN
  s['metadata']['name'] AS service_name,
  s['metadata']['namespace'] AS service_namespace
LIMIT 25
```

### Services without EndpointSlices

```cypher
MATCH (s:Service)
OPTIONAL MATCH (s)-[:Manages]->(es:EndpointSlice)
WITH s, count(es) AS es_count
WHERE es_count = 0
RETURN
  s['metadata']['namespace'] AS namespace,
  s['metadata']['name'] AS name
LIMIT 100
```

## Repair Heuristics

If `graph_query` fails:

- if the error is schema or semantic ambiguity, fetch `graph_schema`
- if the error is marked `repairable`, adjust and retry
- if the result seems stale or missing expected objects, fetch `graph_health`

If a broad exploratory query returns obvious noise:

- narrow with namespace, workload name, label filters, or exact relationship paths
- exclude Kubernetes-injected defaults when the user wants application-level wiring
- prefer a more specific second query over repeated shape-sampling queries

## Schema Use

Use `graph_schema` when:

- writing the first non-trivial query in an unfamiliar area
- unsure about label names, edge directions, or property paths
- repairing a failed query after a schema-related error

Skip `graph_schema` when:

- reusing a pattern that already worked in the conversation
- asking a routine question over known labels and edges
- the final query shape is already clear
