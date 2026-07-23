//! Derivation of graph relationships and secondary endpoint/ingress objects.
//!
//! Every relationship is emitted only after both endpoint identifiers are known.

use crate::state_resolver::{
    Arc, ClusterState, ClusterStateResolver, ConfigMap, EndpointSlice, EndpointSliceDerived,
    HashMap, HashSet, Ingress, IngressDerived, Node, ObjectMeta, ObservedClusterSnapshot,
    PersistentVolume, Pod, Resource, ResourceExt, Result, Service, ServiceAccount,
};
use crate::types::{
    Container, ContainerType, Edge, Endpoint, EndpointAddress, EndpointIdentity, GenericObject,
    Host, IngressServiceBackend, ObjectIdentifier, ResourceAttributes, ResourceType,
};
use k8s_openapi::api::core::v1::{EnvFromSource, EnvVar};
use tracing::{trace, warn};

const ENDPOINT_SLICE_SERVICE_NAME_LABEL: &str = "kubernetes.io/service-name";

impl ClusterStateResolver {
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
        Self::set_service_endpoint_slice_edges(
            &snapshot.services,
            &snapshot.endpoint_slices,
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

    fn set_service_endpoint_slice_edges(
        services: &[Arc<Service>],
        endpoint_slices: &[Arc<EndpointSlice>],
        state: &mut ClusterState,
    ) {
        let service_namespace_name_to_uid: HashMap<(&str, &str), &str> = services
            .iter()
            .filter_map(|service| {
                Some((
                    (
                        service.metadata.namespace.as_deref()?,
                        service.metadata.name.as_deref()?,
                    ),
                    service.metadata.uid.as_deref()?,
                ))
            })
            .collect();

        for endpoint_slice in endpoint_slices {
            let Some(endpoint_slice_uid) = endpoint_slice.metadata.uid.as_deref() else {
                continue;
            };
            let Some(namespace) = endpoint_slice.metadata.namespace.as_deref() else {
                continue;
            };
            let Some(service_name) = endpoint_slice
                .metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get(ENDPOINT_SLICE_SERVICE_NAME_LABEL))
            else {
                continue;
            };

            match service_namespace_name_to_uid.get(&(namespace, service_name.as_str())) {
                Some(service_uid) => state.add_edge(
                    service_uid,
                    ResourceType::Service,
                    endpoint_slice_uid,
                    ResourceType::EndpointSlice,
                    Edge::Manages,
                ),
                None => warn!(
                    endpoint_slice_uid,
                    endpoint_slice_namespace = namespace,
                    service_name,
                    "Skipping unresolved EndpointSlice to Service relationship"
                ),
            }
        }
    }

    pub(super) fn pod_to_service_account(
        pods: &[Arc<Pod>],
        service_accounts: &[Arc<ServiceAccount>],
        state: &mut ClusterState,
    ) {
        let service_account_namespace_name_to_uid: HashMap<(&str, &str), &str> = service_accounts
            .iter()
            .filter_map(|service_account| {
                Some((
                    (
                        service_account.metadata.namespace.as_deref()?,
                        service_account.metadata.name.as_deref()?,
                    ),
                    service_account.metadata.uid.as_deref()?,
                ))
            })
            .collect();

        for pod in pods {
            let Some(pod_uid) = pod.metadata.uid.as_deref() else {
                continue;
            };
            let Some(namespace) = pod.metadata.namespace.as_deref() else {
                continue;
            };
            let Some(spec) = pod.spec.as_ref() else {
                continue;
            };
            let service_account_name = spec
                .service_account_name
                .as_deref()
                .filter(|name| !name.is_empty())
                .or_else(|| {
                    spec.service_account
                        .as_deref()
                        .filter(|name| !name.is_empty())
                })
                .unwrap_or("default");

            match service_account_namespace_name_to_uid.get(&(namespace, service_account_name)) {
                Some(service_account_uid) => state.add_edge(
                    pod_uid,
                    ResourceType::Pod,
                    service_account_uid,
                    ResourceType::ServiceAccount,
                    Edge::UsesIdentity,
                ),
                None => warn!(
                    pod_uid,
                    pod_namespace = namespace,
                    service_account_name,
                    "Skipping unresolved Pod to ServiceAccount relationship"
                ),
            }
        }
    }

    pub(super) fn pod_to_config_maps(
        pods: &[Arc<Pod>],
        config_maps: &[Arc<ConfigMap>],
        state: &mut ClusterState,
    ) {
        let config_map_namespace_name_to_uid: HashMap<(&str, &str), &str> = config_maps
            .iter()
            .filter_map(|config_map| {
                Some((
                    (
                        config_map.metadata.namespace.as_deref()?,
                        config_map.metadata.name.as_deref()?,
                    ),
                    config_map.metadata.uid.as_deref()?,
                ))
            })
            .collect();

        for pod in pods {
            let Some(pod_uid) = pod.metadata.uid.as_deref() else {
                continue;
            };
            let Some(namespace) = pod.metadata.namespace.as_deref() else {
                continue;
            };
            let Some(spec) = pod.spec.as_ref() else {
                continue;
            };

            let mut mounted_names = HashMap::new();
            for volume in spec.volumes.as_deref().into_iter().flatten() {
                if let Some(config_map) = volume.config_map.as_ref() {
                    Self::record_config_map_reference(
                        &mut mounted_names,
                        &config_map.name,
                        config_map.optional,
                    );
                }
                for source in volume
                    .projected
                    .as_ref()
                    .and_then(|projected| projected.sources.as_deref())
                    .into_iter()
                    .flatten()
                {
                    if let Some(config_map) = source.config_map.as_ref() {
                        Self::record_config_map_reference(
                            &mut mounted_names,
                            &config_map.name,
                            config_map.optional,
                        );
                    }
                }
            }

            let mut injected_names = HashMap::new();
            for container in &spec.containers {
                Self::collect_injected_config_map_names(
                    container.env_from.as_deref(),
                    container.env.as_deref(),
                    &mut injected_names,
                );
            }
            for container in spec.init_containers.as_deref().into_iter().flatten() {
                Self::collect_injected_config_map_names(
                    container.env_from.as_deref(),
                    container.env.as_deref(),
                    &mut injected_names,
                );
            }
            for container in spec.ephemeral_containers.as_deref().into_iter().flatten() {
                Self::collect_injected_config_map_names(
                    container.env_from.as_deref(),
                    container.env.as_deref(),
                    &mut injected_names,
                );
            }

            for (edge, config_map_names) in [
                (Edge::MountsConfig, mounted_names),
                (Edge::InjectsConfig, injected_names),
            ] {
                for (config_map_name, required) in config_map_names {
                    match config_map_namespace_name_to_uid.get(&(namespace, config_map_name)) {
                        Some(config_map_uid) => state.add_edge(
                            pod_uid,
                            ResourceType::Pod,
                            config_map_uid,
                            ResourceType::ConfigMap,
                            edge.clone(),
                        ),
                        None if required => {
                            warn!(
                                pod_uid,
                                pod_namespace = namespace,
                                config_map_name,
                                relationship = %edge,
                                "Skipping unresolved required Pod to ConfigMap relationship"
                            );
                        }
                        None => {
                            trace!(
                                pod_uid,
                                pod_namespace = namespace,
                                config_map_name,
                                relationship = %edge,
                                "Skipping unresolved optional Pod to ConfigMap relationship"
                            );
                        }
                    }
                }
            }
        }
    }

    fn collect_injected_config_map_names<'a>(
        env_from: Option<&'a [EnvFromSource]>,
        env: Option<&'a [EnvVar]>,
        names: &mut HashMap<&'a str, bool>,
    ) {
        for source in env_from.into_iter().flatten() {
            if let Some(config_map) = source.config_map_ref.as_ref() {
                Self::record_config_map_reference(names, &config_map.name, config_map.optional);
            }
        }
        for variable in env.into_iter().flatten() {
            if let Some(config_map) = variable
                .value_from
                .as_ref()
                .and_then(|source| source.config_map_key_ref.as_ref())
            {
                Self::record_config_map_reference(names, &config_map.name, config_map.optional);
            }
        }
    }

    fn record_config_map_reference<'a>(
        references: &mut HashMap<&'a str, bool>,
        name: &'a str,
        optional: Option<bool>,
    ) {
        if name.is_empty() {
            return;
        }
        let required = optional != Some(true);
        references
            .entry(name)
            .and_modify(|existing| *existing |= required)
            .or_insert(required);
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
        let service_namespace_name_to_uid: HashMap<(&str, &str), &str> = services
            .iter()
            .filter_map(|service| {
                Some((
                    (
                        service.metadata.namespace.as_deref()?,
                        service.metadata.name.as_deref()?,
                    ),
                    service.metadata.uid.as_deref()?,
                ))
            })
            .collect();
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

            let backend_namespace = ingress_service_backend.metadata.namespace.as_deref();
            match backend_namespace.and_then(|namespace| {
                service_namespace_name_to_uid
                    .get(&(namespace, ingress_service_backend.name.as_str()))
            }) {
                Some(service_uid) => {
                    state.add_edge(
                        &obj_id.uid,
                        ResourceType::IngressServiceBackend,
                        service_uid,
                        ResourceType::Service,
                        Edge::TargetsService,
                    );
                }
                None => {
                    warn!(
                        ingress_namespace = backend_namespace.unwrap_or(""),
                        ingress_uid = ingress_service_backend.ingress_uid,
                        service_name = ingress_service_backend.name,
                        "Skipping unresolved Ingress backend to Service relationship"
                    );
                }
            }
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
        let mut seen_backend_uids = HashSet::new();

        // TODO: Derive a backend from `spec.default_backend` as well as rule paths.
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
                                        let port_identity = match s.port.as_ref() {
                                            Some(port) => match (port.name.as_deref(), port.number) {
                                                (Some(name), None) => format!("name:{name}"),
                                                (None, Some(number)) => format!("number:{number}"),
                                                (Some(name), Some(number)) => {
                                                    format!("invalid:name:{name}:number:{number}")
                                                }
                                                (None, None) => "unspecified".to_string(),
                                            },
                                            None => "unspecified".to_string(),
                                        };
                                        let ingress_svc_backend_uid =
                                            format!("IngressServiceBackend:{ingress_id}:{service_name}:{port_identity}");
                                        if !seen_backend_uids
                                            .insert(ingress_svc_backend_uid.clone())
                                        {
                                            return;
                                        }
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
                    let identity_digest = endpoint.identity_digest();
                    let endpoint_uid = format!(
                        "Endpoint:{}:{}:{}",
                        endpoint_slice_id, slice.address_type, identity_digest
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
