use super::*;

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

        let pvc_name_to_uid: HashMap<&str, &str> = Self::name_to_uid(
            snapshot
                .persistent_volume_claims
                .iter()
                .map(|x| &x.metadata),
        );

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
                                let pvc_uid = pvc_name_to_uid
                                    .get(claim_name)
                                    .unwrap_or_else(|| panic!("PVC `{claim_name}` not found"));
                                state.add_edge(
                                    pod_uid,
                                    ResourceType::Pod,
                                    pvc_uid,
                                    ResourceType::PersistentVolumeClaim,
                                    Edge::ClaimsVolume,
                                );
                            });
                        });
                    });
            });
        }
        Self::set_runs_on_edge(&snapshot.nodes, &snapshot.pods, &mut state);

        let storage_class_name_to_uid: HashMap<&str, &str> =
            Self::name_to_uid(snapshot.storage_classes.iter().map(|x| &x.metadata));
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

    pub(super) fn set_manages_edge_all(
        snapshot: &ObservedClusterSnapshot,
        state: &mut ClusterState,
    ) {
        Self::set_manages_edge(&snapshot.pods, ResourceType::Pod, state);
        Self::set_manages_edge(&snapshot.replica_sets, ResourceType::ReplicaSet, state);
        Self::set_manages_edge(&snapshot.stateful_sets, ResourceType::StatefulSet, state);
        Self::set_manages_edge(&snapshot.daemon_sets, ResourceType::DaemonSet, state);
        Self::set_manages_edge(&snapshot.deployments, ResourceType::Deployment, state);
        Self::set_manages_edge(
            &snapshot.endpoint_slices,
            ResourceType::EndpointSlice,
            state,
        );
        Self::set_manages_edge(
            &snapshot.persistent_volume_claims,
            ResourceType::PersistentVolumeClaim,
            state,
        );
        Self::set_manages_edge(&snapshot.ingresses, ResourceType::Ingress, state);
    }

    pub(super) fn set_runs_on_edge(
        nodes: &[Arc<Node>],
        pods: &[Arc<Pod>],
        state: &mut ClusterState,
    ) {
        let node_name_to_node = Self::name_to_uid(nodes.iter().map(|n| &n.metadata));
        for pod in pods {
            let node_uid = pod.spec.as_ref().and_then(|s| s.node_name.as_deref());
            match node_uid {
                None => {}
                Some(node_name) => {
                    node_name_to_node
                        .get(node_name)
                        .as_ref()
                        .inspect(|node_uid| {
                            pod.metadata.uid.as_deref().inspect(|pod_uid| {
                                state.add_edge(
                                    pod_uid,
                                    ResourceType::Pod,
                                    node_uid,
                                    ResourceType::Node,
                                    Edge::RunsOn,
                                );
                            });
                        });
                }
            }
        }
    }

    pub(super) fn set_manages_edge<T: Resource + ResourceExt>(
        objs: &Vec<Arc<T>>,
        resource_type: ResourceType,
        cluster_state: &mut ClusterState,
    ) {
        for item in objs {
            if let Some(item_uid) = item.uid() {
                for owner in item.owner_references() {
                    match ResourceType::try_new(owner.kind.as_str()) {
                        Ok(owner_resource_type) => {
                            cluster_state.add_edge(
                                owner.uid.as_ref(),
                                owner_resource_type,
                                item_uid.as_ref(),
                                resource_type.clone(),
                                Edge::Manages,
                            );
                        }
                        Err(err) => {
                            trace!(
                                "Unable to parse resource type of {:?} from owner reference: {}",
                                owner, err
                            );
                        }
                    }
                }
            }
        }
    }

    pub(super) fn pvc_to_pv(
        pvs: &[Arc<PersistentVolume>],
        storage_class_name_to_uid: &HashMap<&str, &str>,
        state: &mut ClusterState,
    ) {
        for pv in pvs {
            pv.spec.as_ref().inspect(|spec| {
                pv.metadata.uid.as_ref().inspect(|pv_id| {
                    spec.storage_class_name.as_ref().inspect(|sc_name| {
                        storage_class_name_to_uid
                            .get(sc_name.as_str())
                            .inspect(|sc_id| {
                                state.add_edge(
                                    pv_id,
                                    ResourceType::PersistentVolume,
                                    sc_id,
                                    ResourceType::StorageClass,
                                    Edge::UsesStorageClass,
                                );
                            });
                    });

                    spec.claim_ref.as_ref().inspect(|claim_ref| {
                        claim_ref.uid.as_ref().inspect(|pvc_id| {
                            state.add_edge(
                                pvc_id,
                                ResourceType::PersistentVolumeClaim,
                                pv_id,
                                ResourceType::PersistentVolume,
                                Edge::BoundTo,
                            );
                        });
                    });
                });
            });
        }
    }

    pub(super) fn ingress_to_service(
        services: &[Arc<Service>],
        ingress_service_backends: &[Arc<IngressServiceBackend>],
        state: &mut ClusterState,
    ) {
        let service_name_to_id = Self::name_to_uid(services.iter().map(|s| &s.metadata));
        for ingress_service_backend in ingress_service_backends {
            // Prepare for the edges:
            // 1. (Ingress) -[:DefinesBackend]-> (IngressBackend)
            // 2. (IngressBackend) [:TargetsService]-> Service
            let obj_id = ObjectIdentifier {
                uid: ingress_service_backend
                    .metadata
                    .uid
                    .as_ref()
                    .unwrap()
                    .clone(),
                name: ingress_service_backend.name.to_string(),
                namespace: ingress_service_backend.metadata.namespace.clone(),
                resource_version: ingress_service_backend.metadata.resource_version.clone(),
            };

            state.add_node(GenericObject {
                id: obj_id.clone(),
                resource_type: ResourceType::IngressServiceBackend,
                attributes: Some(Box::new(ResourceAttributes::IngressServiceBackend {
                    ingress_service_backend: ingress_service_backend.clone(),
                })),
            });
            state.add_edge(
                ingress_service_backend.ingress_uid.as_ref(),
                ResourceType::Ingress,
                &obj_id.uid,
                ResourceType::IngressServiceBackend,
                Edge::DefinesBackend,
            );

            service_name_to_id
                .get(ingress_service_backend.name.as_str())
                .inspect(|svc_id| {
                    state.add_edge(
                        &obj_id.uid,
                        ResourceType::IngressServiceBackend,
                        svc_id,
                        ResourceType::Service,
                        Edge::TargetsService,
                    );
                });
        }
    }

    pub(super) fn connect_hosts(hosts: &Vec<Arc<Host>>, state: &mut ClusterState) {
        for host in hosts {
            let obj_id = ObjectIdentifier {
                uid: host.metadata.uid.as_ref().unwrap().clone(),
                name: host.name.to_string(),
                namespace: host.metadata.namespace.clone(),
                resource_version: None,
            };
            state.add_node(GenericObject {
                id: obj_id.clone(),
                resource_type: ResourceType::Host,
                attributes: Some(Box::new(ResourceAttributes::Host { host: host.clone() })),
            });
            state.add_edge(
                &obj_id.uid,
                ResourceType::Host,
                host.ingress_uid.as_ref(),
                ResourceType::Ingress,
                Edge::IsClaimedBy,
            );
        }
    }

    pub(super) fn endpoint_to_pod(
        _endpoints_slices: &[Arc<EndpointSlice>],
        endpoints: &[Arc<Endpoint>],
        endpoint_addresses: &[Arc<EndpointAddress>],
        state: &mut ClusterState,
    ) {
        for endpoint in endpoints {
            let endpoint_uid = endpoint.metadata.uid.as_ref().unwrap().to_string();
            let obj_id = ObjectIdentifier {
                uid: endpoint_uid.clone(),
                name: endpoint.metadata.name.as_ref().unwrap().to_string(),
                namespace: endpoint.metadata.namespace.clone(),
                resource_version: endpoint.metadata.resource_version.clone(),
            };
            state.add_node(GenericObject {
                id: obj_id,
                resource_type: ResourceType::Endpoint,
                attributes: Some(Box::new(ResourceAttributes::Endpoint {
                    endpoint: endpoint.clone(),
                })),
            });
            // (EndpointSlice) -[:ContainsEndpoint]-> (Endpoint)
            state.add_edge(
                endpoint.endpoint_slice_id.as_str(),
                ResourceType::EndpointSlice,
                endpoint_uid.as_str(),
                ResourceType::Endpoint,
                Edge::ContainsEndpoint,
            );
        }

        for endpoint_address in endpoint_addresses {
            let obj_id = ObjectIdentifier {
                uid: endpoint_address.metadata.uid.as_ref().unwrap().to_string(),
                name: endpoint_address.metadata.name.as_ref().unwrap().to_string(),
                namespace: endpoint_address.metadata.namespace.clone(),
                resource_version: endpoint_address.metadata.resource_version.clone(),
            };
            state.add_node(GenericObject {
                id: obj_id.clone(),
                resource_type: ResourceType::EndpointAddress,
                attributes: Some(Box::new(ResourceAttributes::EndpointAddress {
                    endpoint_address: endpoint_address.clone(),
                })),
            });

            let endpoint_address_uid = endpoint_address.metadata.uid.as_ref().unwrap().as_str();

            // (Endpoint) -[:HasAddress]-> (EndpointAddress)
            state.add_edge(
                endpoint_address.endpoint_uid.as_str(),
                ResourceType::Endpoint,
                endpoint_address_uid,
                ResourceType::EndpointAddress,
                Edge::HasAddress,
            );

            // (EndpointAddress) -[:ListedIn]-> (EndpointSlice)
            state.add_edge(
                endpoint_address_uid,
                ResourceType::EndpointAddress,
                endpoint_address.endpoint_slice_uid.as_str(),
                ResourceType::EndpointSlice,
                Edge::ListedIn,
            );

            if let Some(pod_uid) = endpoint_address.pod_uid.as_ref() {
                // (EndpointAddress) -[:IsAddressOf]-> (Pod)
                state.add_edge(
                    endpoint_address_uid,
                    ResourceType::EndpointAddress,
                    pod_uid.as_str(),
                    ResourceType::Pod,
                    Edge::IsAddressOf,
                );
            };
        }
    }

    pub(super) fn connect_part_of_and_belongs_to(
        state: &mut ClusterState,
        namespace_name_to_uid: &HashMap<&str, &str>,
        cluster_uid: &str,
        item_uid: &str,
        item_resource_type: ResourceType,
        namespace: Option<&str>,
    ) {
        state.add_edge(
            item_uid,
            item_resource_type.clone(),
            cluster_uid,
            ResourceType::Cluster,
            Edge::PartOf,
        );

        namespace.inspect(|ns| {
            namespace_name_to_uid.get(*ns).inspect(|ns_uid| {
                state.add_edge(
                    item_uid,
                    item_resource_type,
                    ns_uid,
                    ResourceType::Namespace,
                    Edge::BelongsTo,
                );
            });
        });
    }

    pub(super) fn get_containers(pods: &[Arc<Pod>]) -> Result<Vec<Arc<Container>>> {
        let mut containers: Vec<Arc<Container>> = Vec::new();
        for pod in pods {
            if let Some(name) = pod.metadata.name.as_ref()
                && let Some(ns) = pod.metadata.namespace.as_ref()
                && let Some(uid) = pod.metadata.uid.as_ref()
                && let Some(spec) = pod.spec.as_ref()
            {
                if let Some(inits) = spec.init_containers.as_ref() {
                    for c in inits {
                        let container =
                            Container::new(ns, name, uid, c.clone(), ContainerType::Init);
                        containers.push(Arc::new(container));
                    }
                }
                for c in &spec.containers {
                    let container =
                        Container::new(ns, name, uid, c.clone(), ContainerType::Standard);
                    containers.push(Arc::new(container));
                }
            }
        }
        Ok(containers)
    }

    pub(super) fn name_to_uid<'a, I>(items: I) -> HashMap<&'a str, &'a str>
    where
        I: Iterator<Item = &'a ObjectMeta>,
    {
        items
            .filter_map(|n| {
                let name = n.name.as_ref()?.as_str();
                let uid = n.uid.as_ref()?.as_str();
                Some((name, uid))
            })
            .collect()
    }

    #[allow(unused)]
    pub(super) fn uid_to_name<'a, I>(items: I) -> HashMap<&'a str, &'a str>
    where
        I: Iterator<Item = &'a ObjectMeta>,
    {
        items
            .filter_map(|n| {
                let uid = n.uid.as_ref()?.as_str();
                let name = n.name.as_ref()?.as_str();
                Some((uid, name))
            })
            .collect()
    }

    pub(super) fn get_derived_from_ingress(ingresses: &[Arc<Ingress>]) -> Result<IngressDerived> {
        let mut hosts: Vec<Arc<Host>> = Vec::new();
        let mut ingress_service_backends: Vec<Arc<IngressServiceBackend>> = Vec::new();

        for ingress in ingresses {
            ingress.metadata.uid.as_ref().inspect(|ingress_id| {
                ingress.spec.as_ref().inspect(|spec| {
                    spec.rules.as_ref().inspect(|rules| {
                        rules.iter().for_each(|rule| {
                            rule.host.as_ref().inspect(|host| {
                                let host_uid = format!("Host:{ingress_id}:{host}");
                                let obj_id = ObjectIdentifier {
                                    uid: host_uid.clone(),
                                    name: (*host).clone(),
                                    namespace: ingress.metadata.namespace.clone(),
                                    resource_version: None,
                                };
                                hosts.push(Arc::new(Host::new(&obj_id, host, ingress_id.as_ref())));
                            });

                            rule.http.as_ref().inspect(|http| {
                                http.paths.iter().for_each(|p| {
                                    p.backend.service.as_ref().inspect(|s| {
                                        let service_name = s.name.as_str();
                                        let ingress_svc_backend_uid = format!(
                                            "IngressServiceBackend:{ingress_id}:{service_name}"
                                        );
                                        // Prepare for the edges:
                                        // 1. (Ingress) -[:DefinesBackend]-> (IngressBackend)
                                        // 2. (IngressBackend) [:TargetsService]-> Service
                                        let obj_id = ObjectIdentifier {
                                            uid: ingress_svc_backend_uid.clone(),
                                            name: service_name.to_string(),
                                            namespace: ingress.metadata.namespace.clone(),
                                            resource_version: None,
                                        };

                                        ingress_service_backends.push(Arc::new(
                                            IngressServiceBackend::new(
                                                &obj_id,
                                                s,
                                                ingress_id.as_str(),
                                            ),
                                        ));
                                    });
                                });
                            });
                        })
                    });
                });
            });
        }

        Ok((hosts, ingress_service_backends))
    }

    pub(super) fn get_derived_from_endpoints_slices(
        endpoints_slices: &[Arc<EndpointSlice>],
    ) -> Result<EndpointSliceDerived> {
        let mut endpoints: Vec<Arc<Endpoint>> = Vec::new();
        let mut endpoint_addresss: Vec<Arc<EndpointAddress>> = Vec::new();

        for slice in endpoints_slices {
            if let Some(endpoint_slice_id) = slice.metadata.uid.as_ref() {
                slice.endpoints.iter().for_each(|endpoint| {
                    let obj_hash = endpoint.get_hash();
                    let endpoint_uid = format!(
                        "Endpoint:{}:{}:{}",
                        endpoint_slice_id, slice.address_type, obj_hash
                    );
                    let endpoint_id = ObjectIdentifier {
                        uid: endpoint_uid.clone(),
                        name: "".to_string(),
                        namespace: slice.metadata.namespace.clone(),
                        resource_version: None,
                    };
                    endpoints.push(Arc::new(Endpoint::new(
                        &endpoint_id,
                        endpoint.clone(),
                        endpoint_slice_id.as_str(),
                    )));

                    let pod_uid = endpoint.target_ref.as_ref().and_then(|target_ref| {
                        if let (Some(kind), Some(uid)) = (target_ref.kind.as_ref(), target_ref.uid.as_ref()) {
                            match ResourceType::try_new(kind) {
                                Ok(resource_type) => {
                                    match resource_type {
                                        ResourceType::Pod => {
                                            Some(uid.clone())
                                        }
                                        resource_type => {
                                            warn!("Unknown endpoint target kind {} for EndpointSlice [{}]: {}",
                                                        resource_type,
                                                        target_ref.kind.as_deref().unwrap_or(""),
                                                        endpoint_slice_id
                                                    );
                                            None
                                        }
                                    }
                                }
                                Err(err) => {
                                    warn!(
                                                "Failed to parse resource type from endpoint target {:?}: {}",
                                                target_ref, err
                                            );
                                    None
                                }
                            }
                        }
                        else {
                            None
                        }
                    });

                    endpoint.addresses.iter().for_each(|address| {
                        let endpoint_address_uid =
                            format!("EndpointAddress:{endpoint_uid}:{address}");
                        let endpoint_address_id = ObjectIdentifier {
                            uid: endpoint_address_uid.clone(),
                            name: address.clone(),
                            namespace: slice.metadata.namespace.clone(),
                            resource_version: None,
                        };
                        endpoint_addresss.push(Arc::new(EndpointAddress::new(
                            &endpoint_address_id,
                            address.clone(),
                            endpoint_uid.as_str(),
                            endpoint_slice_id.as_str(),
                            pod_uid.clone()
                        )));
                    });
                });
            };
        }

        Ok((endpoints, endpoint_addresss))
    }
}
