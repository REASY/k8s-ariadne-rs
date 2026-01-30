use k8s_openapi::api::authorization::v1::{
    ResourceAttributes, SelfSubjectAccessReview, SelfSubjectAccessReviewSpec,
};
use kube::api::PostParams;
use kube::{Api, Client};
use tracing::warn;

#[derive(Clone, Copy, Debug)]
pub(crate) enum ResourceScope {
    Namespaced,
    Cluster,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ResourceDescriptor {
    kind: &'static str,
    group: Option<&'static str>,
    resource: &'static str,
    scope: ResourceScope,
}

impl ResourceDescriptor {
    const fn namespaced(
        kind: &'static str,
        group: Option<&'static str>,
        resource: &'static str,
    ) -> Self {
        Self {
            kind,
            group,
            resource,
            scope: ResourceScope::Namespaced,
        }
    }

    const fn cluster(
        kind: &'static str,
        group: Option<&'static str>,
        resource: &'static str,
    ) -> Self {
        Self {
            kind,
            group,
            resource,
            scope: ResourceScope::Cluster,
        }
    }

    fn fq_resource(self) -> String {
        match self.group {
            Some(group) => format!("{}.{}", self.resource, group),
            None => self.resource.to_string(),
        }
    }
}

pub(crate) const RESOURCE_NAMESPACE: ResourceDescriptor =
    ResourceDescriptor::cluster("Namespace", None, "namespaces");
pub(crate) const RESOURCE_POD: ResourceDescriptor =
    ResourceDescriptor::namespaced("Pod", None, "pods");
pub(crate) const RESOURCE_DEPLOYMENT: ResourceDescriptor =
    ResourceDescriptor::namespaced("Deployment", Some("apps"), "deployments");
pub(crate) const RESOURCE_STATEFUL_SET: ResourceDescriptor =
    ResourceDescriptor::namespaced("StatefulSet", Some("apps"), "statefulsets");
pub(crate) const RESOURCE_REPLICA_SET: ResourceDescriptor =
    ResourceDescriptor::namespaced("ReplicaSet", Some("apps"), "replicasets");
pub(crate) const RESOURCE_DAEMON_SET: ResourceDescriptor =
    ResourceDescriptor::namespaced("DaemonSet", Some("apps"), "daemonsets");
pub(crate) const RESOURCE_JOB: ResourceDescriptor =
    ResourceDescriptor::namespaced("Job", Some("batch"), "jobs");
pub(crate) const RESOURCE_INGRESS: ResourceDescriptor =
    ResourceDescriptor::namespaced("Ingress", Some("networking.k8s.io"), "ingresses");
pub(crate) const RESOURCE_SERVICE: ResourceDescriptor =
    ResourceDescriptor::namespaced("Service", None, "services");
pub(crate) const RESOURCE_ENDPOINT_SLICE: ResourceDescriptor =
    ResourceDescriptor::namespaced("EndpointSlice", Some("discovery.k8s.io"), "endpointslices");
pub(crate) const RESOURCE_NETWORK_POLICY: ResourceDescriptor = ResourceDescriptor::namespaced(
    "NetworkPolicy",
    Some("networking.k8s.io"),
    "networkpolicies",
);
pub(crate) const RESOURCE_CONFIG_MAP: ResourceDescriptor =
    ResourceDescriptor::namespaced("ConfigMap", None, "configmaps");
pub(crate) const RESOURCE_STORAGE_CLASS: ResourceDescriptor =
    ResourceDescriptor::cluster("StorageClass", Some("storage.k8s.io"), "storageclasses");
pub(crate) const RESOURCE_PERSISTENT_VOLUME: ResourceDescriptor =
    ResourceDescriptor::cluster("PersistentVolume", None, "persistentvolumes");
pub(crate) const RESOURCE_PERSISTENT_VOLUME_CLAIM: ResourceDescriptor =
    ResourceDescriptor::namespaced("PersistentVolumeClaim", None, "persistentvolumeclaims");
pub(crate) const RESOURCE_NODE: ResourceDescriptor =
    ResourceDescriptor::cluster("Node", None, "nodes");
pub(crate) const RESOURCE_SERVICE_ACCOUNT: ResourceDescriptor =
    ResourceDescriptor::namespaced("ServiceAccount", None, "serviceaccounts");
pub(crate) const RESOURCE_EVENT: ResourceDescriptor =
    ResourceDescriptor::namespaced("Event", Some("events.k8s.io"), "events");

pub(crate) struct AccessChecker {
    client: Client,
    namespace: Option<String>,
}

impl AccessChecker {
    pub(crate) fn new(client: Client, maybe_ns: Option<&str>) -> Self {
        Self {
            client,
            namespace: maybe_ns.map(|ns| ns.to_string()),
        }
    }

    pub(crate) async fn can_read(&self, descriptor: ResourceDescriptor) -> bool {
        let list_ok = match self.check(descriptor, "list").await {
            Ok(allowed) => allowed,
            Err(err) => {
                self.log_check_error(descriptor, "list", &err);
                return false;
            }
        };
        let watch_ok = match self.check(descriptor, "watch").await {
            Ok(allowed) => allowed,
            Err(err) => {
                self.log_check_error(descriptor, "watch", &err);
                return false;
            }
        };
        if list_ok && watch_ok {
            return true;
        }
        self.log_denied(descriptor, list_ok, watch_ok);
        false
    }

    async fn check(
        &self,
        descriptor: ResourceDescriptor,
        verb: &'static str,
    ) -> std::result::Result<bool, kube::Error> {
        let namespace = match descriptor.scope {
            ResourceScope::Namespaced => self.namespace.as_deref(),
            ResourceScope::Cluster => None,
        };
        let attributes = ResourceAttributes {
            group: descriptor.group.map(|group| group.to_string()),
            resource: Some(descriptor.resource.to_string()),
            verb: Some(verb.to_string()),
            namespace: namespace.map(|ns| ns.to_string()),
            ..Default::default()
        };
        let review = SelfSubjectAccessReview {
            spec: SelfSubjectAccessReviewSpec {
                resource_attributes: Some(attributes),
                ..Default::default()
            },
            ..Default::default()
        };
        let api: Api<SelfSubjectAccessReview> = Api::all(self.client.clone());
        let response = api.create(&PostParams::default(), &review).await?;
        Ok(response
            .status
            .map(|status| status.allowed)
            .unwrap_or(false))
    }

    fn log_denied(&self, descriptor: ResourceDescriptor, list_ok: bool, watch_ok: bool) {
        let mut missing_verbs = Vec::new();
        if !list_ok {
            missing_verbs.push("list");
        }
        if !watch_ok {
            missing_verbs.push("watch");
        }
        let verbs = missing_verbs.join(", ");
        let scope = self.scope_label(descriptor);
        warn!(
            "RBAC: skipping {} (missing {} on {} at {})",
            descriptor.kind,
            verbs,
            descriptor.fq_resource(),
            scope
        );
    }

    fn log_check_error(
        &self,
        descriptor: ResourceDescriptor,
        verb: &'static str,
        err: &kube::Error,
    ) {
        let scope = self.scope_label(descriptor);
        warn!(
            "RBAC: failed to check {} permission for {} on {} at {}: {:?}. Skipping.",
            verb,
            descriptor.kind,
            descriptor.fq_resource(),
            scope,
            err
        );
    }

    fn scope_label(&self, descriptor: ResourceDescriptor) -> String {
        match descriptor.scope {
            ResourceScope::Cluster => "cluster scope".to_string(),
            ResourceScope::Namespaced => match self.namespace.as_deref() {
                Some(ns) => format!("namespace \"{ns}\""),
                None => "all namespaces".to_string(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{Request, Response, StatusCode};
    use kube::client::Body;
    use kube::Client;
    use serde_json::json;
    use std::convert::Infallible;
    use std::sync::{Arc, Mutex};
    use tower::service_fn;

    fn ssar_response(allowed: bool) -> Response<Body> {
        let body = json!({
            "status": {
                "allowed": allowed
            }
        });
        Response::builder()
            .status(StatusCode::CREATED)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string().into_bytes()))
            .expect("response")
    }

    fn error_response(status: StatusCode) -> Response<Body> {
        let body = json!({
            "kind": "Status",
            "apiVersion": "v1",
            "status": "Failure",
            "message": "forbidden",
            "reason": "Forbidden",
            "code": status.as_u16()
        });
        Response::builder()
            .status(status)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string().into_bytes()))
            .expect("response")
    }

    fn test_client(
        responses: Arc<Mutex<Vec<Response<Body>>>>,
        requests: Arc<Mutex<Vec<serde_json::Value>>>,
    ) -> Client {
        let service = service_fn(move |req: Request<Body>| {
            let responses = responses.clone();
            let requests = requests.clone();
            async move {
                let body = req
                    .into_body()
                    .collect_bytes()
                    .await
                    .expect("collect request body");
                if !body.is_empty() {
                    let value: serde_json::Value =
                        serde_json::from_slice(&body).expect("parse request body");
                    requests.lock().expect("lock requests").push(value);
                }
                let response = responses
                    .lock()
                    .expect("lock responses")
                    .remove(0);
                Ok::<_, Infallible>(response)
            }
        });
        Client::new(service, "default")
    }

    #[tokio::test]
    async fn can_read_returns_true_when_list_and_watch_allowed() {
        let responses = Arc::new(Mutex::new(vec![ssar_response(true), ssar_response(true)]));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let client = test_client(responses, requests.clone());
        let access = AccessChecker::new(client, None);

        let allowed = access.can_read(RESOURCE_STORAGE_CLASS).await;
        assert!(allowed);

        let captured = requests.lock().expect("lock requests");
        assert_eq!(captured.len(), 2);
        for (idx, verb) in ["list", "watch"].iter().enumerate() {
            let attrs = &captured[idx]["spec"]["resourceAttributes"];
            assert_eq!(attrs["verb"], *verb);
            assert_eq!(attrs["resource"], "storageclasses");
            assert_eq!(attrs["group"], "storage.k8s.io");
            assert!(attrs["namespace"].is_null());
        }
    }

    #[tokio::test]
    async fn can_read_returns_false_when_watch_denied() {
        let responses = Arc::new(Mutex::new(vec![ssar_response(true), ssar_response(false)]));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let client = test_client(responses, requests.clone());
        let access = AccessChecker::new(client, Some("team-a"));

        let allowed = access.can_read(RESOURCE_POD).await;
        assert!(!allowed);

        let captured = requests.lock().expect("lock requests");
        assert_eq!(captured.len(), 2);
        for (idx, verb) in ["list", "watch"].iter().enumerate() {
            let attrs = &captured[idx]["spec"]["resourceAttributes"];
            assert_eq!(attrs["verb"], *verb);
            assert_eq!(attrs["resource"], "pods");
            assert!(attrs["group"].is_null());
            assert_eq!(attrs["namespace"], "team-a");
        }
    }

    #[tokio::test]
    async fn can_read_returns_false_when_list_errors() {
        let responses = Arc::new(Mutex::new(vec![error_response(StatusCode::FORBIDDEN)]));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let client = test_client(responses, requests.clone());
        let access = AccessChecker::new(client, Some("team-a"));

        let allowed = access.can_read(RESOURCE_POD).await;
        assert!(!allowed);

        let captured = requests.lock().expect("lock requests");
        assert_eq!(captured.len(), 1);
        let attrs = &captured[0]["spec"]["resourceAttributes"];
        assert_eq!(attrs["verb"], "list");
        assert_eq!(attrs["resource"], "pods");
        assert_eq!(attrs["namespace"], "team-a");
    }
}
