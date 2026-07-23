//! Materialization of Kubernetes resources into graph nodes.
//!
//! All primary and derived nodes are created before relationship derivation runs.

use crate::create_generic_object;
use crate::state_resolver::{
    AugmentedClusterSnapshot, ClusterState, ClusterStateResolver, HashMap, HashSet, ResourceExt,
};
use crate::types::{
    Edge, GenericObject, ObjectIdentifier, Provisioner, ResourceAttributes, ResourceType,
};
use tracing::warn;

impl ClusterStateResolver {
    pub(super) fn create_state(augmented: &AugmentedClusterSnapshot) -> ClusterState {
        let snapshot = &augmented.observed;
        let mut state = ClusterState::new(snapshot.cluster.clone());
        let cluster_uid: String = {
            let obj_id = ObjectIdentifier {
                uid: snapshot.cluster.metadata.uid.as_ref().unwrap().to_string(),
                name: snapshot.cluster.metadata.name.as_ref().unwrap().to_string(),
                namespace: None,
                resource_version: None,
            };
            let cluster_node = GenericObject {
                id: obj_id.clone(),
                resource_type: ResourceType::Cluster,
                attributes: Some(Box::new(ResourceAttributes::Cluster {
                    cluster: Box::new(snapshot.cluster.clone()),
                })),
            };
            state.add_node(cluster_node);
            obj_id.uid.clone()
        };

        // Namespaces
        for item in &snapshot.namespaces {
            let node = create_generic_object!(item.clone(), Namespace, Namespace, namespace);
            state.add_node(node);

            state.add_edge(
                item.metadata.uid.as_ref().unwrap(),
                ResourceType::Namespace,
                cluster_uid.as_str(),
                ResourceType::Cluster,
                Edge::PartOf,
            );
        }
        let namespace_name_to_uid: HashMap<&str, &str> =
            Self::name_to_uid(snapshot.namespaces.iter().map(|x| &x.metadata));

        // Core Workloads
        for item in &snapshot.pods {
            let node = create_generic_object!(item.clone(), Pod, Pod, pod);
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                ResourceType::Pod,
                item.metadata.namespace.as_deref(),
            );
        }
        for item in &augmented.derived.containers {
            let obj_id = ObjectIdentifier {
                uid: item.metadata.uid.as_ref().unwrap().clone(),
                name: item.metadata.name.as_ref().unwrap().clone(),
                namespace: item.metadata.namespace.clone(),
                resource_version: None,
            };
            state.add_node(GenericObject {
                id: obj_id.clone(),
                resource_type: ResourceType::Container,
                attributes: Some(Box::new(ResourceAttributes::Container {
                    container: item.clone(),
                })),
            });

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                ResourceType::Container,
                item.metadata.namespace.as_deref(),
            );

            let container_uid = item.metadata.uid.as_ref().unwrap().to_string();
            state.add_edge(
                container_uid.as_str(),
                ResourceType::Container,
                item.pod_uid.as_str(),
                ResourceType::Pod,
                Edge::Runs,
            );
        }

        for item in &snapshot.deployments {
            let node = create_generic_object!(item.clone(), Deployment, Deployment, deployment);
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                ResourceType::Deployment,
                item.metadata.namespace.as_deref(),
            );
        }
        for item in &snapshot.stateful_sets {
            let node = create_generic_object!(item.clone(), StatefulSet, StatefulSet, stateful_set);
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                ResourceType::StatefulSet,
                item.metadata.namespace.as_deref(),
            );
        }
        for item in &snapshot.replica_sets {
            let node = create_generic_object!(item.clone(), ReplicaSet, ReplicaSet, replica_set);
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                ResourceType::ReplicaSet,
                item.metadata.namespace.as_deref(),
            );
        }
        for item in &snapshot.daemon_sets {
            let node = create_generic_object!(item.clone(), DaemonSet, DaemonSet, daemon_set);
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                ResourceType::DaemonSet,
                item.metadata.namespace.as_deref(),
            );
        }
        for item in &snapshot.jobs {
            let node = create_generic_object!(item.clone(), Job, Job, job);
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                ResourceType::Job,
                item.metadata.namespace.as_deref(),
            );
        }

        // Networking & Discovery
        for item in &snapshot.ingresses {
            let node = create_generic_object!(item.clone(), Ingress, Ingress, ingress);
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                ResourceType::Ingress,
                item.metadata.namespace.as_deref(),
            );
        }
        for item in &snapshot.services {
            let node = create_generic_object!(item.clone(), Service, Service, service);
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                ResourceType::Service,
                item.metadata.namespace.as_deref(),
            );
        }
        for item in &snapshot.endpoint_slices {
            let node =
                create_generic_object!(item.clone(), EndpointSlice, EndpointSlice, endpoint_slice);
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                ResourceType::EndpointSlice,
                item.metadata.namespace.as_deref(),
            );
        }
        for item in &snapshot.network_policies {
            let node =
                create_generic_object!(item.clone(), NetworkPolicy, NetworkPolicy, network_policy);
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                ResourceType::NetworkPolicy,
                item.metadata.namespace.as_deref(),
            );
        }

        // Configuration
        for item in &snapshot.config_maps {
            let node = create_generic_object!(item.clone(), ConfigMap, ConfigMap, config_map);
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                ResourceType::ConfigMap,
                item.metadata.namespace.as_deref(),
            );
        }

        let mut unique_provisoners: HashSet<&str> = HashSet::new();
        // Storage
        for item in &snapshot.storage_classes {
            let provisoner = &item.provisioner;
            if unique_provisoners.insert(&item.provisioner) {
                let obj_id = ObjectIdentifier {
                    uid: provisoner.clone(),
                    name: provisoner.clone(),
                    namespace: item.metadata.namespace.clone(),
                    resource_version: None,
                };
                state.add_node(GenericObject {
                    id: obj_id.clone(),
                    resource_type: ResourceType::Provisioner,
                    attributes: Some(Box::new(ResourceAttributes::Provisioner {
                        provisioner: Box::new(Provisioner::new(&obj_id, provisoner.as_str())),
                    })),
                });

                Self::connect_part_of_and_belongs_to(
                    &mut state,
                    &namespace_name_to_uid,
                    cluster_uid.as_str(),
                    obj_id.uid.as_str(),
                    ResourceType::Provisioner,
                    obj_id.namespace.as_deref(),
                );
            }
            let node =
                create_generic_object!(item.clone(), StorageClass, StorageClass, storage_class);
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                ResourceType::StorageClass,
                item.metadata.namespace.as_deref(),
            );

            state.add_edge(
                item.metadata.uid.as_ref().unwrap(),
                ResourceType::StorageClass,
                provisoner,
                ResourceType::Provisioner,
                Edge::UsesProvisioner,
            );
        }
        for item in &snapshot.persistent_volumes {
            let node = create_generic_object!(item.clone(), PersistentVolume, PersistentVolume, pv);
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                ResourceType::PersistentVolume,
                item.metadata.namespace.as_deref(),
            );
        }
        for item in &snapshot.persistent_volume_claims {
            let node = create_generic_object!(
                item.clone(),
                PersistentVolumeClaim,
                PersistentVolumeClaim,
                pvc
            );
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                ResourceType::PersistentVolumeClaim,
                item.metadata.namespace.as_deref(),
            );
        }

        // Cluster Infrastructure
        for item in &snapshot.nodes {
            let node = create_generic_object!(item.clone(), Node, Node, node);
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                ResourceType::Node,
                item.metadata.namespace.as_deref(),
            );
        }

        // Identity & Access Control
        for item in &snapshot.service_accounts {
            let node = create_generic_object!(
                item.clone(),
                ServiceAccount,
                ServiceAccount,
                service_account
            );
            state.add_node(node);

            Self::connect_part_of_and_belongs_to(
                &mut state,
                &namespace_name_to_uid,
                cluster_uid.as_str(),
                item.metadata.uid.as_deref().unwrap(),
                ResourceType::ServiceAccount,
                item.metadata.namespace.as_deref(),
            );
        }

        Self::set_manages_edge_all(snapshot, &mut state);
        Self::pod_to_service_account(&snapshot.pods, &snapshot.service_accounts, &mut state);
        Self::pod_to_config_maps(&snapshot.pods, &snapshot.config_maps, &mut state);
        Self::network_policies_to_pods(&snapshot.network_policies, &snapshot.pods, &mut state);

        let pvc_namespace_name_to_uid: HashMap<(&str, &str), &str> = snapshot
            .persistent_volume_claims
            .iter()
            .filter_map(|claim| {
                Some((
                    (
                        claim.metadata.namespace.as_deref()?,
                        claim.metadata.name.as_deref()?,
                    ),
                    claim.metadata.uid.as_deref()?,
                ))
            })
            .collect();

        for pod in &snapshot.pods {
            pod.metadata.uid.as_ref().inspect(|pod_uid| {
                pod.spec
                    .as_ref()
                    .map(|s| s.volumes.as_ref())
                    .iter()
                    .flatten()
                    .for_each(|volumes| {
                        volumes.iter().for_each(|v| {
                            v.persistent_volume_claim.as_ref().inspect(|pvc| {
                                let claim_name = pvc.claim_name.as_str();
                                let pod_namespace = pod.metadata.namespace.as_deref();
                                match pod_namespace.and_then(|namespace| {
                                    pvc_namespace_name_to_uid.get(&(namespace, claim_name))
                                }) {
                                    Some(pvc_uid) => {
                                        state.add_edge(
                                            pod_uid,
                                            ResourceType::Pod,
                                            pvc_uid,
                                            ResourceType::PersistentVolumeClaim,
                                            Edge::ClaimsVolume,
                                        );
                                    }
                                    None => {
                                        warn!(
                                            pod_namespace = pod_namespace.unwrap_or(""),
                                            pod_name = pod.metadata.name.as_deref().unwrap_or(""),
                                            claim_name,
                                            "Skipping unresolved Pod to PersistentVolumeClaim relationship"
                                        );
                                    }
                                }
                            });
                        });
                    });
            });
        }
        Self::set_runs_on_edge(&snapshot.nodes, &snapshot.pods, &mut state);

        let storage_class_name_to_uid: HashMap<&str, &str> =
            Self::name_to_uid(snapshot.storage_classes.iter().map(|x| &x.metadata));
        Self::persistent_volume_claims_to_storage_classes(
            &snapshot.persistent_volume_claims,
            &storage_class_name_to_uid,
            &mut state,
        );
        Self::pvc_to_pv(
            &snapshot.persistent_volumes,
            &storage_class_name_to_uid,
            &mut state,
        );

        Self::ingress_to_service(
            &snapshot.services,
            &augmented.derived.ingress_service_backends,
            &mut state,
        );
        Self::connect_hosts(&augmented.derived.hosts, &mut state);

        Self::endpoint_to_pod(
            &snapshot.endpoint_slices,
            &augmented.derived.endpoints,
            &augmented.derived.endpoint_addresses,
            &mut state,
        );

        for item in &snapshot.events {
            item.metadata.uid.as_ref().inspect(|uid| {
                state.add_node(GenericObject {
                    id: ObjectIdentifier {
                        uid: uid.to_string(),
                        name: item.metadata.name.as_ref().unwrap().clone(),
                        namespace: item.metadata.namespace.clone(),
                        resource_version: None,
                    },
                    resource_type: ResourceType::Event,
                    attributes: Some(Box::new(ResourceAttributes::Event {
                        event: item.clone(),
                    })),
                })
            });

            let uid = item.metadata.uid.as_ref().unwrap();
            item.regarding.as_ref().inspect(|regarding| {
                regarding.uid.as_ref().inspect(|regarding_uid| {
                    if let Some(kind) = &regarding.kind {
                        match ResourceType::try_new(kind.as_str()) {
                            Ok(regarding_resource_type) => {
                                state.add_edge(
                                    uid,
                                    ResourceType::Event,
                                    regarding_uid,
                                    regarding_resource_type,
                                    Edge::Concerns,
                                );
                            }
                            Err(err) => {
                                warn!(
                                    "Failed to parse resource type from event regarding {:?}: {}",
                                    regarding, err
                                );
                            }
                        }
                    }
                });
            });
        }

        state
    }
}
