# Snapshots

Export a snapshot from a live cluster:

```bash
CLUSTER=<cluster> KUBE_CONTEXT=<context> \
  cargo run --release -p ariadne-app -- snapshot export --output-dir ./snapshot
```

Load a snapshot instead of talking to K8s:

```bash
CLUSTER=<cluster> KUBE_SNAPSHOT_DIR=./snapshot \
  cargo run --release -p ariadne-app
```

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
