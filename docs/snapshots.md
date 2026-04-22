# Snapshots

Export a snapshot from a live cluster:

```bash
CLUSTER=<cluster> KUBE_CONTEXT=<context> \
  cargo run --release -p ariadne-mcp -- snapshot export --output-dir ./snapshot
```

This export is a raw snapshot. It is intended for internal debugging and offline graph work. It is not publication-safe as-is.

Load a snapshot instead of talking to K8s:

```bash
CLUSTER=<cluster> KUBE_SNAPSHOT_DIR=./snapshot \
  cargo run --release -p ariadne-mcp
```

Planned public-sharing workflow:

```bash
CLUSTER=<cluster> KUBE_CONTEXT=<context> \
  cargo run --release -p ariadne-mcp -- snapshot export --output-dir ./snapshot/raw

cargo run --release -p ariadne-mcp -- \
  snapshot sanitize \
  --input-dir ./snapshot/raw \
  --output-dir ./snapshot/public \
  --profile public-v1
```

`snapshot sanitize` is specified in [docs/specs/snapshot_sanitizer_v1.md](./specs/snapshot_sanitizer_v1.md). Until that command exists, do not publish raw exports.

Snapshot directory format (JSON files per kind):
```
cluster.json
namespaces.json
pods.json
deployments.json
statefulsets.json
replicasets.json
daemonsets.json
jobs.json
ingresses.json
services.json
endpointslices.json
networkpolicies.json
configmaps.json
storageclasses.json
persistentvolumes.json
persistentvolumeclaims.json
nodes.json
serviceaccounts.json
events.json
```

Sanitized snapshots keep the same file layout and add `sanitization_manifest.json`.
