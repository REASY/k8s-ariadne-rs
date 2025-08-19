use crate::state::ClusterState;
use crate::state_resolver::ObservedClusterSnapshot;
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{
    ConfigMap, Namespace, Node, PersistentVolume, PersistentVolumeClaim, Pod, Service,
    ServiceAccount,
};
use k8s_openapi::api::discovery::v1::EndpointSlice;
use k8s_openapi::api::events::v1::Event;
use k8s_openapi::api::networking::v1::{Ingress, NetworkPolicy};
use k8s_openapi::api::storage::v1::StorageClass;
use kube::ResourceExt;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

#[derive(Debug)]
pub struct Diff<'a, T> {
    pub added: Vec<&'a T>,
    pub removed: Vec<&'a T>,
    pub modified: Vec<&'a T>,
}

#[derive(Debug)]
pub struct ObservedClusterSnapshotDiff<'a> {
    pub namespaces: Diff<'a, Namespace>,
    pub pods: Diff<'a, Pod>,
    pub deployments: Diff<'a, Deployment>,
    pub stateful_sets: Diff<'a, StatefulSet>,
    pub replica_sets: Diff<'a, ReplicaSet>,
    pub daemon_sets: Diff<'a, DaemonSet>,
    pub jobs: Diff<'a, Job>,
    pub ingresses: Diff<'a, Ingress>,
    pub services: Diff<'a, Service>,
    pub endpoint_slices: Diff<'a, EndpointSlice>,
    pub network_policies: Diff<'a, NetworkPolicy>,
    pub config_maps: Diff<'a, ConfigMap>,
    pub storage_classes: Diff<'a, StorageClass>,
    pub persistent_volumes: Diff<'a, PersistentVolume>,
    pub persistent_volume_claims: Diff<'a, PersistentVolumeClaim>,
    pub nodes: Diff<'a, Node>,
    pub service_accounts: Diff<'a, ServiceAccount>,
    pub events: Diff<'a, Event>,
}

impl<'a> fmt::Display for ObservedClusterSnapshotDiff<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut first = true;
        write_diff_section(f, &mut first, "Namespaces", &self.namespaces)?;
        write_diff_section(f, &mut first, "Pods", &self.pods)?;
        write_diff_section(f, &mut first, "Deployments", &self.deployments)?;
        write_diff_section(f, &mut first, "StatefulSets", &self.stateful_sets)?;
        write_diff_section(f, &mut first, "ReplicaSets", &self.replica_sets)?;
        write_diff_section(f, &mut first, "DaemonSets", &self.daemon_sets)?;
        write_diff_section(f, &mut first, "Jobs", &self.jobs)?;
        write_diff_section(f, &mut first, "Ingresses", &self.ingresses)?;
        write_diff_section(f, &mut first, "Services", &self.services)?;
        write_diff_section(f, &mut first, "EndpointSlices", &self.endpoint_slices)?;
        write_diff_section(f, &mut first, "NetworkPolicies", &self.network_policies)?;
        write_diff_section(f, &mut first, "ConfigMaps", &self.config_maps)?;
        write_diff_section(f, &mut first, "StorageClasses", &self.storage_classes)?;
        write_diff_section(f, &mut first, "PersistentVolumes", &self.persistent_volumes)?;
        write_diff_section(
            f,
            &mut first,
            "PersistentVolumeClaims",
            &self.persistent_volume_claims,
        )?;
        write_diff_section(f, &mut first, "Nodes", &self.nodes)?;
        write_diff_section(f, &mut first, "ServiceAccounts", &self.service_accounts)?;
        write_diff_section(f, &mut first, "Events", &self.events)?;

        Ok(())
    }
}

fn write_diff_section<T: fmt::Debug>(
    f: &mut Formatter<'_>,
    first: &mut bool,
    name: &str,
    diff: &Diff<'_, T>,
) -> fmt::Result {
    if !diff.added.is_empty() || !diff.removed.is_empty() || !diff.modified.is_empty() {
        if !*first {
            writeln!(f)?;
        }
        *first = false;
        writeln!(f, "{}:", name)?;
        if !diff.added.is_empty() {
            writeln!(f, "  Added: {}", diff.added.len())?;
        }
        if !diff.removed.is_empty() {
            writeln!(f, "  Removed: {}", diff.removed.len())?;
        }
        if !diff.modified.is_empty() {
            writeln!(f, "  Modified: {}", diff.modified.len())?;
        }
    }
    Ok(())
}

impl<'a> ObservedClusterSnapshotDiff<'a> {
    pub fn new(current: &'a ObservedClusterSnapshot, prev: &'a ObservedClusterSnapshot) -> Self {
        ObservedClusterSnapshotDiff {
            namespaces: diff_slices(&current.namespaces, &prev.namespaces),
            pods: diff_slices(&current.pods, &prev.pods),
            deployments: diff_slices(&current.deployments, &prev.deployments),
            stateful_sets: diff_slices(&current.stateful_sets, &prev.stateful_sets),
            replica_sets: diff_slices(&current.replica_sets, &prev.replica_sets),
            daemon_sets: diff_slices(&current.daemon_sets, &prev.daemon_sets),
            jobs: diff_slices(&current.jobs, &prev.jobs),
            ingresses: diff_slices(&current.ingresses, &prev.ingresses),
            services: diff_slices(&current.services, &prev.services),
            endpoint_slices: diff_slices(&current.endpoint_slices, &prev.endpoint_slices),
            network_policies: diff_slices(&current.network_policies, &prev.network_policies),
            config_maps: diff_slices(&current.config_maps, &prev.config_maps),
            storage_classes: diff_slices(&current.storage_classes, &prev.storage_classes),
            persistent_volumes: diff_slices(&current.persistent_volumes, &prev.persistent_volumes),
            persistent_volume_claims: diff_slices(
                &current.persistent_volume_claims,
                &prev.persistent_volume_claims,
            ),
            nodes: diff_slices(&current.nodes, &prev.nodes),
            service_accounts: diff_slices(&current.service_accounts, &prev.service_accounts),
            events: diff_slices(&current.events, &prev.events),
        }
    }
}

fn diff_slices<'b, T>(current: &'b [Arc<T>], prev: &'b [Arc<T>]) -> Diff<'b, T>
where
    T: ResourceExt,
{
    // Build maps keyed by UID (borrowed from the object's metadata)
    let mut prev_map: HashMap<&'b str, (&'b T, Option<&'b str>)> = HashMap::new();
    for item in prev {
        if let Some(uid) = item.meta().uid.as_deref() {
            prev_map.insert(uid, (&**item, item.meta().resource_version.as_deref()));
        }
    }

    let mut current_uids: HashSet<&'b str> = HashSet::new();
    let mut added: Vec<&'b T> = Vec::new();
    let mut modified: Vec<&'b T> = Vec::new();

    for item in current {
        if let Some(uid) = item.meta().uid.as_deref() {
            current_uids.insert(uid);
            match prev_map.get(uid) {
                None => {
                    // New object
                    added.push(&**item);
                }
                Some((prev_item, prev_rv)) => {
                    let cur_rv = item.meta().resource_version.as_deref();
                    if cur_rv != *prev_rv {
                        // Changed resourceVersion -> treat as modified
                        let _ = prev_item; // keep for clarity; not used further
                        modified.push(&**item);
                    }
                }
            }
        }
    }

    // Removed => in prev but not in current
    let mut removed: Vec<&'b T> = Vec::new();
    for item in prev {
        if let Some(uid) = item.meta().uid.as_deref() {
            if !current_uids.contains(uid) {
                removed.push(&**item);
            }
        }
    }

    Diff {
        added,
        removed,
        modified,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::Namespace;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use kube::Resource;
    use std::sync::Arc;

    fn uids(items: &[&Namespace]) -> HashSet<String> {
        items.iter().filter_map(|i| i.meta().uid.clone()).collect()
    }

    fn create_test_resource(uid: &str, rv: &str) -> Arc<Namespace> {
        Arc::new(Namespace {
            metadata: ObjectMeta {
                uid: Some(uid.to_string()),
                resource_version: Some(rv.to_string()),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    #[test]
    fn test_diff_empty_slices() {
        let current: Vec<Arc<Namespace>> = vec![];
        let prev: Vec<Arc<Namespace>> = vec![];

        let diff = diff_slices(&current, &prev);

        assert!(diff.added.is_empty());
        assert!(diff.removed.is_empty());
        assert!(diff.modified.is_empty());
    }

    #[test]
    fn test_diff_added_resources() {
        let current = vec![
            create_test_resource("uid1", "rv1"),
            create_test_resource("uid2", "rv1"),
        ];
        let prev: Vec<Arc<Namespace>> = vec![];

        let diff = diff_slices(&current, &prev);

        assert_eq!(diff.added.len(), 2);
        assert!(diff.removed.is_empty());
        assert!(diff.modified.is_empty());
        let expected: HashSet<String> = ["uid1", "uid2"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(uids(&diff.added), expected);
        assert!(uids(&diff.removed).is_empty());
        assert!(uids(&diff.modified).is_empty());
    }

    #[test]
    fn test_diff_removed_resources() {
        let current: Vec<Arc<Namespace>> = vec![];
        let prev = vec![
            create_test_resource("uid1", "rv1"),
            create_test_resource("uid2", "rv1"),
        ];

        let diff = diff_slices(&current, &prev);

        assert!(diff.added.is_empty());
        assert_eq!(diff.removed.len(), 2);
        assert!(diff.modified.is_empty());
        let expected: HashSet<String> = ["uid1", "uid2"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        assert!(uids(&diff.added).is_empty());
        assert_eq!(uids(&diff.removed), expected);
        assert!(uids(&diff.modified).is_empty());
    }

    #[test]
    fn test_diff_modified_resources() {
        let current = vec![create_test_resource("uid1", "rv2")];
        let prev = vec![create_test_resource("uid1", "rv1")];

        let diff = diff_slices(&current, &prev);

        assert!(diff.added.is_empty());
        assert!(diff.removed.is_empty());
        assert_eq!(diff.modified.len(), 1);
        let expected: HashSet<String> = ["uid1"].into_iter().map(|s| s.to_string()).collect();
        assert!(uids(&diff.added).is_empty());
        assert!(uids(&diff.removed).is_empty());
        assert_eq!(uids(&diff.modified), expected);
    }

    #[test]
    fn test_diff_unchanged_resources() {
        let current = vec![create_test_resource("uid1", "rv1")];
        let prev = vec![create_test_resource("uid1", "rv1")];

        let diff = diff_slices(&current, &prev);

        assert!(diff.added.is_empty());
        assert!(diff.removed.is_empty());
        assert!(diff.modified.is_empty());
    }

    #[test]
    fn test_diff_mixed_changes() {
        let current = vec![
            create_test_resource("uid1", "rv2"), // modified
            create_test_resource("uid3", "rv1"), // added
        ];
        let prev = vec![
            create_test_resource("uid1", "rv1"), // modified
            create_test_resource("uid2", "rv1"), // removed
        ];

        let diff = diff_slices(&current, &prev);

        assert_eq!(diff.added.len(), 1);
        assert_eq!(diff.removed.len(), 1);
        assert_eq!(diff.modified.len(), 1);
        let expected_added: HashSet<String> = ["uid3"].into_iter().map(|s| s.to_string()).collect();
        let expected_removed: HashSet<String> =
            ["uid2"].into_iter().map(|s| s.to_string()).collect();
        let expected_modified: HashSet<String> =
            ["uid1"].into_iter().map(|s| s.to_string()).collect();
        assert_eq!(uids(&diff.added), expected_added);
        assert_eq!(uids(&diff.removed), expected_removed);
        assert_eq!(uids(&diff.modified), expected_modified);
    }
}
