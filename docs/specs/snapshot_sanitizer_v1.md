# Ariadne Snapshot Sanitizer V1 Specification

## Purpose

Define a stable, publication-safe sanitization pipeline for Ariadne Kubernetes snapshots.

The current `snapshot export` command writes a faithful copy of the observed Kubernetes state. That raw export is appropriate for internal debugging and offline graph work, but it is not safe to publish publicly. The sanitizer defined here produces a derived snapshot that:

- keeps Ariadne's snapshot directory layout,
- remains loadable by `SnapshotKubeClient`,
- preserves Ariadne's current graph topology,
- removes or rewrites sensitive operational data.

This specification defines the first stable public profile: `public-v1`.

---

## Background

Today, raw snapshot export is a direct serialization of `ObservedClusterSnapshot`:

- `ariadne-mcp` exposes `snapshot export`
- `ariadne-core` writes the in-memory observed snapshot to disk
- `SnapshotKubeClient::from_dir` loads the same file layout back

That is the correct design for a canonical raw export, but it means raw snapshots contain real Kubernetes object payloads, including:

- `ConfigMap.data`
- environment variable values
- internal URLs and DNS names
- ingress hosts
- internal IP addresses
- node names and hostnames
- image references and tags
- secret references and service-account wiring
- duplicated manifests embedded in annotations such as:
  - `kubectl.kubernetes.io/last-applied-configuration`
  - `kapp.k14s.io/original`

The sanitizer exists to derive a separate publication-safe artifact from that raw snapshot.

---

## Design Decisions

### 1. Sanitization is a separate transformation

`snapshot export` remains the canonical raw writer.

Sanitization is a second step:

1. export a raw snapshot
2. sanitize the raw snapshot into a separate output directory
3. publish only the sanitized directory

This preserves a clear semantic boundary:

- raw export: fidelity
- sanitized export: publication safety

The sanitizer must never modify the input snapshot in place.

### 2. Core logic lives in `ariadne-core`

The sanitization logic belongs in `ariadne-core`, because it operates on snapshot data structures and must be reusable by:

- `ariadne-mcp`
- future CLI or tooling
- unit and integration tests

`ariadne-mcp` owns the command surface only.

### 3. Sanitized output keeps the snapshot file layout

The sanitized snapshot must use the same per-kind JSON files as raw snapshots so that:

- `SnapshotKubeClient::from_dir` can still load it
- existing offline flows keep working
- diffing and fixtures stay simple

The sanitizer may add extra manifest files, but it must not replace the core file layout with a new format.

### 4. `public-v1` optimizes for publication safety, not replayability

The sanitized snapshot is intended for:

- public demos
- bug reports
- documentation
- offline graph exploration

It is explicitly not intended to be:

- replayed into a live Kubernetes cluster
- applied with `kubectl`
- restored into etcd
- treated as a complete control-plane backup

### 5. Determinism is guaranteed per input snapshot

Given the same input directory and the same profile, the sanitizer must produce byte-for-byte identical output.

`public-v1` does not guarantee stable aliases across different snapshots of the same cluster. Its determinism requirement is scoped to the same input snapshot contents.

---

## Goals

- Provide a stable `public-v1` sanitization profile.
- Remove raw secret-bearing and operationally sensitive data.
- Preserve Ariadne snapshot loadability.
- Preserve Ariadne's current graph topology.
- Make the policy explicit and testable.
- Prevent accidental leakage through duplicated annotations.

## Non-goals

- Reconstructing a runnable Kubernetes cluster.
- Preserving every original object field.
- Hiding cluster size or object counts.
- Providing cryptographic anonymity guarantees against all linkage attacks.
- Reversible anonymization.
- A general-purpose Kubernetes redaction framework outside Ariadne's snapshot model.

---

## Terminology

- **Raw snapshot**: the faithful output of `snapshot export`.
- **Sanitized snapshot**: the derived output of `snapshot sanitize`.
- **Profile**: a named sanitization policy, for example `public-v1`.
- **Graph topology**: the set of Ariadne graph nodes and edges produced from a snapshot, measured by resource and edge types, not by original labels or payload values.
- **Rewrite**: replace a value with a deterministic alias.
- **Drop**: remove a field entirely or replace it with an empty/default value.

---

## Command Surface

### Raw export

Raw export remains unchanged:

```bash
cargo run --release -p ariadne-mcp -- \
  --cluster <cluster> \
  --kube-context <context> \
  snapshot export \
  --output-dir ./snapshot/raw
```

### Sanitization

The sanitizer command surface is:

```bash
cargo run --release -p ariadne-mcp -- \
  snapshot sanitize \
  --input-dir ./snapshot/raw \
  --output-dir ./snapshot/public \
  --profile public-v1
```

#### Required behavior

- `--input-dir` and `--output-dir` must be different paths.
- Sanitization must fail if `output-dir` is the same directory as `input-dir`.
- Sanitization must fail if `output-dir` already contains conflicting snapshot files.
- The command must reject unknown profile names.
- The command must never mutate `input-dir`.

#### Future convenience

A future convenience form may be added:

```bash
snapshot export --output-dir ./snapshot/raw --public-output-dir ./snapshot/public --sanitize-profile public-v1
```

If added later, it must still execute as two distinct internal phases: raw export first, sanitization second.

---

## Output Layout

The sanitized directory must contain the standard snapshot files:

```text
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

If the input contains `snapshot_manifest.json`, the sanitizer must copy it to the output unchanged.

The sanitizer must also write:

```text
sanitization_manifest.json
```

### `sanitization_manifest.json`

The manifest records what policy produced the public artifact without storing any reversible mapping data.

Example:

```json
{
  "version": 1,
  "profile": "public-v1",
  "sanitized_at": "2026-04-06T11:00:00Z",
  "source": {
    "captured_at": "2026-04-06T10:24:43Z",
    "scope": {
      "kind": "cluster",
      "namespace": null
    }
  },
  "guarantees": {
    "snapshot_layout_compatible": true,
    "preserves_current_graph_topology": true,
    "replayable_into_kubernetes": false,
    "in_place_mutation": false
  },
  "counts": {
    "namespaces": 20,
    "pods": 337,
    "deployments": 26,
    "statefulsets": 21,
    "replicasets": 126,
    "daemonsets": 13,
    "jobs": 0,
    "ingresses": 3,
    "services": 57,
    "endpointslices": 54,
    "networkpolicies": 3,
    "configmaps": 114,
    "storageclasses": 2,
    "persistentvolumes": 27,
    "persistentvolumeclaims": 27,
    "nodes": 22,
    "serviceaccounts": 83,
    "events": 52
  }
}
```

The manifest must not contain:

- original values
- mapping tables
- unhashed source payload excerpts
- copies of removed annotations

---

## Current Graph Dependencies

`public-v1` is defined against Ariadne's current graph builder.

The current implementation relies on a limited set of fields for node and edge creation:

- resource `metadata.uid`
- resource `metadata.name`
- resource `metadata.namespace`
- `ownerReferences` for `Manages`
- pod container names for derived `Container` nodes
- pod `spec.nodeName` for `RunsOn`
- pod PVC volumes for `ClaimsVolume`
- PV `spec.storageClassName` and `spec.claimRef` for storage edges
- Ingress hostnames and backend service names for host and backend derivation
- EndpointSlice addresses and pod target references for endpoint derivation
- event `regarding` references for `Concerns`

This dependency set comes from the current `ClusterStateResolver` logic. If the graph builder later starts depending on additional fields, the sanitizer profile must be reviewed and may require a new version.

---

## Rewrite Strategy

`public-v1` uses deterministic aliasing. The exact implementation is internal, but the output must satisfy the following rules.

### General rules

- Rewritten values must be deterministic for a given input snapshot.
- Rewritten names must be valid Kubernetes-compatible strings where the field normally expects a Kubernetes object name.
- Rewritten values must not preserve original internal hostnames, domains, image names, URLs, UIDs, or IP addresses.
- Rewritten values must be stable within a single sanitized snapshot so that all references continue to resolve.

### Recommended alias shapes

- Cluster names: `cluster-001`
- Namespace names: `ns-001`
- Pod names: `pod-001`
- Deployment names: `deploy-001`
- StatefulSet names: `sts-001`
- ReplicaSet names: `rs-001`
- DaemonSet names: `ds-001`
- Job names: `job-001`
- Service names: `svc-001`
- Ingress names: `ing-001`
- ConfigMap names: `cm-001`
- StorageClass names: `sc-001`
- PersistentVolume names: `pv-001`
- PersistentVolumeClaim names: `pvc-001`
- ServiceAccount names: `sa-001`
- Node names: `node-001`
- Event names: `event-001`
- Provisioners: `provisioner-001`
- UIDs: `uid-<kind>-001`
- Hostnames: `host-001.example.invalid`
- URLs: preserve scheme when useful, replace hosts with `.invalid`
- IPv4 addresses: deterministic values from `198.18.0.0/15`
- IPv6 addresses: deterministic values from `2001:db8::/32`

### Metadata normalization

For all resource kinds, unless explicitly preserved later:

- preserve `apiVersion`
- preserve `kind`
- rewrite `metadata.uid`
- rewrite `metadata.name`
- rewrite `metadata.namespace`
- drop `metadata.annotations`
- drop `metadata.managedFields`
- drop `metadata.labels`
- drop `metadata.finalizers`
- drop `metadata.resourceVersion`
- drop `metadata.generation`
- drop `metadata.creationTimestamp`
- drop `metadata.deletionTimestamp`
- drop `metadata.deletionGracePeriodSeconds`
- drop `metadata.ownerReferences` unless needed for graph edges

When `ownerReferences` are needed:

- preserve `kind`
- rewrite `name`
- rewrite `uid`
- preserve `controller` if present
- preserve `blockOwnerDeletion` if present
- drop other fields unless required for serialization

### Annotation handling

All annotations are removed before any other transformation.

This requirement is absolute because annotations may embed full unsanitized manifests. Known examples include:

- `kubectl.kubernetes.io/last-applied-configuration`
- `kapp.k14s.io/original`
- `kapp.k14s.io/original-diff-md5`

No profile in V1 may preserve annotations for public output.

---

## `public-v1` Per-Resource Policy

### Cluster (`cluster.json`)

#### Preserve

- `apiVersion`, if present
- `kind`, if present

#### Rewrite

- `metadata.uid`
- `metadata.name`
- top-level `name`
- `cluster_url` to a synthetic placeholder such as `https://cluster.invalid:443/`

#### Drop or reset

- detailed `info` build metadata must be replaced with defaults or clearly redacted placeholders

#### Rationale

The cluster node exists in Ariadne's graph, but exact API server build details and internal endpoints are not needed for public graph exploration.

### Namespaces

#### Preserve

- `apiVersion`
- `kind`

#### Rewrite

- `metadata.uid`
- `metadata.name`

#### Drop or reset

- all annotations, labels, status, and other metadata noise

### Nodes

#### Preserve

- `apiVersion`
- `kind`

#### Rewrite

- `metadata.uid`
- `metadata.name`

#### Drop or reset

- `spec`
- `status`
- provider IDs
- addresses
- image inventories
- capacity and allocatable
- labels and annotations

#### Rationale

Current graph topology only needs node identity and pod-to-node resolution by name.

### Pods

#### Preserve

- `apiVersion`
- `kind`
- rewritten `ownerReferences`
- `spec.nodeName`, rewritten
- `spec.containers[]` cardinality and ordering
- `spec.initContainers[]` cardinality and ordering
- PVC-backed volume references only

#### Rewrite

- `metadata.uid`
- `metadata.name`
- `metadata.namespace`
- `spec.nodeName`
- container names
- init-container names
- PVC `claimName`
- PVC volume names

#### Drop or reset

- `status`
- `spec.serviceAccountName`
- `spec.imagePullSecrets`
- `spec.hostNetwork`
- `spec.hostPID`
- `spec.hostIPC`
- `spec.priorityClassName`
- `spec.affinity`
- `spec.tolerations`
- `spec.securityContext`
- `spec.restartPolicy`
- `spec.dnsPolicy`
- all non-PVC volumes
- container `image`
- container `env`
- container `envFrom`
- container `command`
- container `args`
- container `ports`
- container `resources`
- container `livenessProbe`
- container `readinessProbe`
- container `startupProbe`
- container `lifecycle`
- container `securityContext`
- container `volumeMounts`
- container `workingDir`
- projected service-account tokens
- secret-backed volumes
- secret refs of all kinds

#### Required minimal container shape

Each preserved container entry must retain at least:

- `name`

All other container fields may be cleared.

#### Required minimal volume shape

For each preserved PVC-backed volume, retain only:

- rewritten `name`
- `persistentVolumeClaim.claimName`, rewritten

### Deployments, StatefulSets, ReplicaSets, DaemonSets, Jobs

#### Preserve

- `apiVersion`
- `kind`
- rewritten `ownerReferences`

#### Rewrite

- `metadata.uid`
- `metadata.name`
- `metadata.namespace`

#### Drop or reset

- `spec`
- `status`
- annotations
- labels

#### Rationale

Current Ariadne topology for these objects is driven by resource identity plus `ownerReferences`.

### Services

#### Preserve

- `apiVersion`
- `kind`

#### Rewrite

- `metadata.uid`
- `metadata.name`
- `metadata.namespace`

#### Drop or reset

- `spec`
- `status`
- cluster IPs
- ports
- selectors
- annotations

#### Rationale

Current topology only needs service identity so ingress backend nodes can target services by rewritten name.

### Ingresses

#### Preserve

- `apiVersion`
- `kind`
- rewritten `ownerReferences`
- `spec.rules` cardinality
- per-rule backend cardinality

#### Rewrite

- `metadata.uid`
- `metadata.name`
- `metadata.namespace`
- `spec.rules[].host`
- backend service names
- TLS hostnames if TLS entries are preserved

#### Normalize

- `path` may be normalized to `/`
- `pathType` may be normalized to `Prefix`

#### Drop or reset

- `status`
- `spec.ingressClassName`
- `spec.tls[].secretName`
- annotations

#### Rationale

Current graph derivation needs hostnames and backend service names only.

### EndpointSlices

#### Preserve

- `apiVersion`
- `kind`
- rewritten `ownerReferences`
- `addressType`
- `endpoints[]` cardinality
- `endpoints[].targetRef.kind` when present

#### Rewrite

- `metadata.uid`
- `metadata.name`
- `metadata.namespace`
- `endpoints[].addresses[]`
- `endpoints[].targetRef.uid`
- `endpoints[].targetRef.name`
- `endpoints[].targetRef.namespace`

#### Drop or reset

- `endpoints[].nodeName`
- `endpoints[].hostname`
- `endpoints[].zone`
- `endpoints[].conditions`
- `endpoints[].hints`
- all annotations and labels

#### Rationale

Current endpoint derivation requires addresses plus pod target references. Internal node and locality data are not needed in public output.

### NetworkPolicies

#### Preserve

- `apiVersion`
- `kind`
- rewritten `ownerReferences`

#### Rewrite

- `metadata.uid`
- `metadata.name`
- `metadata.namespace`

#### Drop or reset

- `spec`
- `status`
- annotations

#### Rationale

Current graph implementation keeps these as nodes but does not depend on policy rule bodies.

### ConfigMaps

#### Preserve

- `apiVersion`
- `kind`

#### Rewrite

- `metadata.uid`
- `metadata.name`
- `metadata.namespace`

#### Drop or reset

- `data`
- `binaryData`
- annotations
- labels

#### Rationale

Raw `ConfigMap.data` is one of the primary leakage vectors. Current graph topology does not depend on config contents.

### StorageClasses

#### Preserve

- `apiVersion`
- `kind`

#### Rewrite

- `metadata.uid`
- `metadata.name`
- `provisioner`

#### Drop or reset

- `parameters`
- `mountOptions`
- `allowedTopologies`
- annotations

#### Rationale

Current storage topology depends on storage class identity plus provisioner identity.

### PersistentVolumes

#### Preserve

- `apiVersion`
- `kind`
- minimal `spec` with storage binding references

#### Rewrite

- `metadata.uid`
- `metadata.name`
- `spec.storageClassName`
- `spec.claimRef.uid`
- `spec.claimRef.name`
- `spec.claimRef.namespace`

#### Drop or reset

- all backend-specific volume source details
- capacity
- node affinity
- mount options
- reclaim policy
- status
- annotations

#### Required minimal `spec`

Retain only what is needed for Ariadne storage edges:

- `storageClassName`
- `claimRef.kind`
- `claimRef.uid`
- `claimRef.name`
- `claimRef.namespace`

### PersistentVolumeClaims

#### Preserve

- `apiVersion`
- `kind`
- rewritten `ownerReferences`

#### Rewrite

- `metadata.uid`
- `metadata.name`
- `metadata.namespace`

#### Drop or reset

- `spec`
- `status`
- annotations

#### Rationale

Current graph only requires PVC identity and references from pods/PVs.

### ServiceAccounts

#### Preserve

- `apiVersion`
- `kind`

#### Rewrite

- `metadata.uid`
- `metadata.name`
- `metadata.namespace`

#### Drop or reset

- `secrets`
- `imagePullSecrets`
- annotations
- labels

#### Rationale

Current graph keeps service accounts as nodes but does not use their secret payloads.

### Events

#### Preserve

- `apiVersion`
- `kind`
- `regarding.kind` when present

#### Rewrite

- `metadata.uid`
- `metadata.name`
- `metadata.namespace`
- `regarding.uid`
- `regarding.name`
- `regarding.namespace`
- `related.uid`
- `related.name`
- `related.namespace`

#### Drop or reset

- `note`
- `reason`
- `action`
- `type`
- `reportingController`
- `reportingInstance`
- `series`
- timestamps
- annotations

#### Rationale

This preserves current `Event -> Concerns -> Resource` graph connectivity without leaking free-text event content.

---

## Fields That Must Never Survive `public-v1`

The following classes of fields must be absent from the sanitized output:

- `metadata.annotations`
- `metadata.managedFields`
- `status.*`
- `ConfigMap.data`
- `ConfigMap.binaryData`
- container `image`
- container `env`
- container `envFrom`
- container `command`
- container `args`
- internal URLs and DNS names
- raw hostnames and ingress hosts
- raw internal IP addresses
- `secretName`
- `secretRef`
- `imagePullSecrets`
- projected service-account token volumes
- API keys, bearer tokens, or token file paths copied from raw config

This rule is policy, not best effort. If a field falls into one of these classes and still appears in output, that is a sanitizer bug.

---

## Graph Compatibility Contract

`public-v1` must remain compatible with Ariadne's snapshot reader and current graph builder.

### Required compatibility guarantees

1. `SnapshotKubeClient::from_dir` can load the sanitized directory.
2. Ariadne can build a `ClusterState` from the sanitized directory.
3. Graph node counts by `ResourceType` must match the raw snapshot.
4. Graph edge counts by `Edge` must match the raw snapshot.

### Interpretation

This contract is about topology, not original payload equality.

The following are allowed to differ:

- object attributes
- names
- UIDs
- URLs
- addresses
- free-text content
- detailed specs and statuses

The following are not allowed to differ:

- whether a pod exists
- whether a controller manages a workload
- whether a pod runs on a node
- whether a PVC is claimed by a pod
- whether a PV is bound to a PVC
- whether an ingress backend targets a service
- whether an endpoint address targets a pod
- whether an event concerns a resource

---

## Failure Handling

The sanitizer must fail loudly if it cannot preserve required graph references.

Examples:

- a pod references a PVC name that no longer resolves after rewriting
- an ingress backend references a service name with no rewritten target
- an EndpointSlice target ref points to a pod UID that was not rewritten consistently
- an owner reference cannot be rewritten because the referenced object class is unsupported

Silent data loss is not acceptable for graph-critical relationships.

---

## Testing Requirements

Implementation must include tests for the following.

### Unit tests

- per-kind sanitization strips forbidden fields
- deterministic rewrite behavior for the same input
- owner-reference rewriting remains consistent
- ingress service rewriting remains consistent
- PVC and PV reference rewriting remains consistent
- endpoint target-ref rewriting remains consistent
- annotation removal catches manifest-carrying annotations

### Integration tests

- load raw snapshot fixture
- sanitize to output directory
- load sanitized snapshot through `SnapshotKubeClient`
- build `ClusterState` from raw and sanitized snapshots
- assert equal node counts by `ResourceType`
- assert equal edge counts by `Edge`

### Regression scans

Fixture-based string scans must assert that sanitized output no longer contains patterns such as:

- `agoda`
- `.local`
- `.svc`
- `api_key`
- `bearer_token`
- `secretName`
- `kubectl.kubernetes.io/last-applied-configuration`
- `kapp.k14s.io/original`

The exact denylist can grow over time, but the presence of known leak patterns must fail tests.

---

## Versioning

`public-v1` is a stable policy identifier.

Any change that affects one of the following requires a new profile version, for example `public-v2`:

- preserved graph topology contract
- deterministic rewrite rules
- per-resource field preservation/removal semantics
- manifest format in a breaking way
- the privacy model of the output

Additive metadata in `sanitization_manifest.json` is allowed without a profile version bump if existing consumers remain compatible.

---

## Reserved Future Profiles

These profiles are intentionally out of scope for V1 but may be added later:

- `internal-lite-v1`
  - lighter redaction for internal sharing
- `replay-v1`
  - best-effort Kubernetes-applyable sanitized fixtures
- `topology-only-v1`
  - extreme minimization for graph demos only

These are names only. This specification does not define their behavior.

---

## Summary

`public-v1` is a strict, graph-preserving publication profile:

- raw export stays raw
- sanitization is a separate step
- logic lives in `ariadne-core`
- CLI lives in `ariadne-mcp`
- output keeps the snapshot layout
- sensitive payloads are dropped or rewritten
- duplicated annotations are always removed
- current Ariadne graph topology is preserved
- the result is safe for public sharing, but not for Kubernetes replay
