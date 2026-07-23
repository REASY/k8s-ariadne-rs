use crate::types::ResourceType;
use serde_json::{Map, Value};
use std::sync::LazyLock;

pub(crate) const REDACT_ENV_VALUES_ENV: &str = "ARIADNE_REDACT_ENV_VALUES";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RedactionPolicy {
    redact_env_values: bool,
}

impl RedactionPolicy {
    fn from_env_value(value: Option<&str>) -> Self {
        let redact_env_values = !matches!(
            value.map(str::trim).map(str::to_ascii_lowercase),
            Some(value) if matches!(value.as_str(), "false" | "0" | "no" | "off")
        );
        Self { redact_env_values }
    }
}

static REDACTION_POLICY: LazyLock<RedactionPolicy> = LazyLock::new(|| {
    RedactionPolicy::from_env_value(std::env::var(REDACT_ENV_VALUES_ENV).ok().as_deref())
});

pub(crate) fn redact_kubernetes_value(resource_type: &ResourceType, value: &mut Value) {
    redact_kubernetes_value_with_policy(resource_type, value, *REDACTION_POLICY);
}

fn redact_kubernetes_value_with_policy(
    resource_type: &ResourceType,
    value: &mut Value,
    policy: RedactionPolicy,
) {
    redact_nested(value, policy);

    let Some(root) = value.as_object_mut() else {
        return;
    };
    match resource_type {
        ResourceType::ConfigMap => {
            root.remove("data");
            root.remove("binaryData");
        }
        ResourceType::Event => {
            root.remove("note");
            root.remove("deprecatedMessage");
        }
        _ => {}
    }
}

fn redact_nested(value: &mut Value, policy: RedactionPolicy) {
    match value {
        Value::Array(values) => {
            for value in values {
                redact_nested(value, policy);
            }
        }
        Value::Object(object) => {
            redact_metadata(object);
            if policy.redact_env_values {
                redact_literal_environment_values(object);
            }
            object.remove("volumeAttributes");
            for value in object.values_mut() {
                redact_nested(value, policy);
            }
        }
        _ => {}
    }
}

fn redact_metadata(object: &mut Map<String, Value>) {
    let Some(Value::Object(metadata)) = object.get_mut("metadata") else {
        return;
    };
    metadata.remove("managedFields");
    metadata.remove("annotations");
}

fn redact_literal_environment_values(object: &mut Map<String, Value>) {
    let Some(Value::Array(environment)) = object.get_mut("env") else {
        return;
    };
    for variable in environment {
        if let Value::Object(variable) = variable {
            variable.remove("value");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn environment_value_policy_is_secure_by_default_and_accepts_opt_out_values() {
        assert_eq!(
            RedactionPolicy::from_env_value(None),
            RedactionPolicy {
                redact_env_values: true
            }
        );
        for value in ["false", "FALSE", "0", "no", "off"] {
            assert_eq!(
                RedactionPolicy::from_env_value(Some(value)),
                RedactionPolicy {
                    redact_env_values: false
                }
            );
        }
        for value in ["true", "1", "yes", "on", "invalid"] {
            assert_eq!(
                RedactionPolicy::from_env_value(Some(value)),
                RedactionPolicy {
                    redact_env_values: true
                }
            );
        }
    }

    #[test]
    fn redacts_sensitive_fields_but_preserves_topology_references() {
        let mut pod = json!({
            "metadata": {
                "name": "api",
                "labels": {"app": "api"},
                "annotations": {"token": "secret"},
                "managedFields": [{"manager": "kubectl"}]
            },
            "spec": {
                "containers": [{
                    "name": "api",
                    "env": [
                        {"name": "PASSWORD", "value": "secret"},
                        {"name": "FROM_SECRET", "valueFrom": {
                            "secretKeyRef": {"name": "credentials", "key": "password"}
                        }}
                    ]
                }],
                "volumes": [{"configMap": {"name": "settings"}}]
            }
        });

        redact_kubernetes_value(&ResourceType::Pod, &mut pod);

        assert!(pod.pointer("/metadata/annotations").is_none());
        assert!(pod.pointer("/metadata/managedFields").is_none());
        assert_eq!(pod.pointer("/metadata/labels/app"), Some(&json!("api")));
        assert!(pod.pointer("/spec/containers/0/env/0/value").is_none());
        assert_eq!(
            pod.pointer("/spec/containers/0/env/1/valueFrom/secretKeyRef/name"),
            Some(&json!("credentials"))
        );
        assert_eq!(
            pod.pointer("/spec/volumes/0/configMap/name"),
            Some(&json!("settings"))
        );
    }

    #[test]
    fn redacts_resource_specific_payloads() {
        let mut config_map = json!({
            "metadata": {"name": "settings"},
            "data": {"password": "secret"},
            "binaryData": {"archive": "c2VjcmV0"}
        });
        redact_kubernetes_value(&ResourceType::ConfigMap, &mut config_map);
        assert!(config_map.get("data").is_none());
        assert!(config_map.get("binaryData").is_none());

        let mut volume = json!({
            "spec": {"csi": {
                "driver": "example.csi",
                "volumeHandle": "volume-1",
                "volumeAttributes": {"password": "secret"}
            }}
        });
        redact_kubernetes_value(&ResourceType::PersistentVolume, &mut volume);
        assert!(volume.pointer("/spec/csi/volumeAttributes").is_none());
        assert_eq!(
            volume.pointer("/spec/csi/volumeHandle"),
            Some(&json!("volume-1"))
        );

        let mut event = json!({"reason": "Failed", "note": "token=secret"});
        redact_kubernetes_value(&ResourceType::Event, &mut event);
        assert!(event.get("note").is_none());
        assert_eq!(event.get("reason"), Some(&json!("Failed")));
    }

    #[test]
    fn environment_value_opt_out_preserves_literals_but_keeps_other_redaction() {
        let mut pod = json!({
            "metadata": {"annotations": {"token": "secret"}},
            "spec": {"containers": [{
                "name": "api",
                "env": [{"name": "FEATURE_FLAG", "value": "enabled"}]
            }]}
        });

        redact_kubernetes_value_with_policy(
            &ResourceType::Pod,
            &mut pod,
            RedactionPolicy {
                redact_env_values: false,
            },
        );

        assert_eq!(
            pod.pointer("/spec/containers/0/env/0/value"),
            Some(&json!("enabled"))
        );
        assert!(pod.pointer("/metadata/annotations").is_none());
    }
}
