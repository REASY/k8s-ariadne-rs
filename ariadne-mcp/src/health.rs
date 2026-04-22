use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

pub const SNAPSHOT_MANIFEST_FILE: &str = "snapshot_manifest.json";

pub type SharedCoverage = Arc<Mutex<BTreeSet<String>>>;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum GraphHealthDetail {
    #[default]
    Compact,
    Full,
    Debug,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GraphScopeKind {
    Cluster,
    Namespace,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphScope {
    pub kind: GraphScopeKind,
    pub namespace: Option<String>,
}

impl GraphScope {
    pub fn cluster() -> Self {
        Self {
            kind: GraphScopeKind::Cluster,
            namespace: None,
        }
    }

    pub fn namespace(namespace: impl Into<String>) -> Self {
        Self {
            kind: GraphScopeKind::Namespace,
            namespace: Some(namespace.into()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotManifest {
    pub captured_at: String,
    pub scope: GraphScope,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoverageResponse {
    pub degraded_resource_kinds: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct DiffSummary {
    pub added_nodes: usize,
    pub removed_nodes: usize,
    pub modified_nodes: usize,
    pub added_edges: usize,
    pub removed_edges: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiffSummaryResponse {
    pub added_nodes: usize,
    pub removed_nodes: usize,
    pub modified_nodes: usize,
    pub added_edges: usize,
    pub removed_edges: usize,
}

impl From<DiffSummary> for DiffSummaryResponse {
    fn from(value: DiffSummary) -> Self {
        Self {
            added_nodes: value.added_nodes,
            removed_nodes: value.removed_nodes,
            modified_nodes: value.modified_nodes,
            added_edges: value.added_edges,
            removed_edges: value.removed_edges,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HealthError {
    pub stage: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthErrorResponse {
    pub stage: String,
    pub message: String,
}

impl From<HealthError> for HealthErrorResponse {
    fn from(value: HealthError) -> Self {
        Self {
            stage: value.stage,
            message: value.message,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SyncHealth {
    pub loop_alive: bool,
    pub poll_interval_seconds: u64,
    pub total_attempts: u64,
    pub total_successes: u64,
    pub last_attempt_at: Option<SystemTime>,
    pub last_success_at: Option<SystemTime>,
    pub last_write_at: Option<SystemTime>,
    pub last_attempt_duration: Option<Duration>,
    pub last_fetch_duration: Option<Duration>,
    pub last_diff_duration: Option<Duration>,
    pub last_write_duration: Option<Duration>,
    pub last_error: Option<HealthError>,
    pub last_error_at: Option<SystemTime>,
    pub consecutive_errors: u64,
    pub last_diff: Option<DiffSummary>,
}

impl SyncHealth {
    pub fn bootstrap(at: SystemTime) -> Self {
        Self {
            loop_alive: false,
            poll_interval_seconds: 0,
            total_attempts: 1,
            total_successes: 1,
            last_attempt_at: Some(at),
            last_success_at: Some(at),
            last_write_at: Some(at),
            last_attempt_duration: None,
            last_fetch_duration: None,
            last_diff_duration: None,
            last_write_duration: None,
            last_error: None,
            last_error_at: None,
            consecutive_errors: 0,
            last_diff: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncHealthResponse {
    pub loop_alive: bool,
    pub poll_interval_seconds: u64,
    pub total_attempts: u64,
    pub total_successes: u64,
    pub last_attempt_at: Option<String>,
    pub last_success_at: Option<String>,
    pub last_write_at: Option<String>,
    pub lag_ms: Option<u64>,
    pub last_attempt_duration_ms: Option<u64>,
    pub last_fetch_duration_ms: Option<u64>,
    pub last_diff_duration_ms: Option<u64>,
    pub last_write_duration_ms: Option<u64>,
    pub last_error: Option<HealthErrorResponse>,
    pub last_error_at: Option<String>,
    pub consecutive_errors: u64,
    pub last_diff: Option<DiffSummaryResponse>,
}

#[derive(Debug, Clone, Default)]
pub struct RebuildHealth {
    pub loop_alive: bool,
    pub poll_interval_seconds: u64,
    pub total_attempts: u64,
    pub total_successes: u64,
    pub last_attempt_at: Option<SystemTime>,
    pub last_success_at: Option<SystemTime>,
    pub last_duration: Option<Duration>,
    pub last_error: Option<HealthError>,
    pub last_error_at: Option<SystemTime>,
    pub consecutive_errors: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebuildHealthResponse {
    pub loop_alive: bool,
    pub poll_interval_seconds: u64,
    pub total_attempts: u64,
    pub total_successes: u64,
    pub last_attempt_at: Option<String>,
    pub last_success_at: Option<String>,
    pub last_duration_ms: Option<u64>,
    pub last_error: Option<HealthErrorResponse>,
    pub last_error_at: Option<String>,
    pub consecutive_errors: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphHealthCompactResponse {
    pub detail: GraphHealthDetail,
    pub cluster: String,
    pub mode: String,
    pub scope: Option<GraphScope>,
    pub observed_at: String,
    pub ready: bool,
    pub data_as_of: Option<String>,
    pub node_count: usize,
    pub edge_count: usize,
    pub sync_lag_ms: Option<u64>,
    pub coverage: CoverageResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphHealthResponse {
    pub detail: GraphHealthDetail,
    pub cluster: String,
    pub backend: String,
    pub mode: String,
    pub scope: Option<GraphScope>,
    pub observed_at: String,
    pub ready: bool,
    pub backend_probe_ok: bool,
    pub backend_probe_duration_ms: u64,
    pub data_as_of: Option<String>,
    pub node_count: usize,
    pub edge_count: usize,
    pub version: String,
    pub sync: Option<SyncHealthResponse>,
    pub rebuild: Option<RebuildHealthResponse>,
    pub coverage: CoverageResponse,
}

pub fn now() -> SystemTime {
    SystemTime::now()
}

pub fn format_timestamp(time: SystemTime) -> String {
    let dt: DateTime<Utc> = time.into();
    dt.to_rfc3339_opts(SecondsFormat::Secs, true)
}

pub fn format_timestamp_opt(time: Option<SystemTime>) -> Option<String> {
    time.map(format_timestamp)
}

pub fn duration_ms_opt(duration: Option<Duration>) -> Option<u64> {
    duration.map(|value| value.as_millis() as u64)
}

pub fn lag_ms(observed_at: SystemTime, last_success_at: Option<SystemTime>) -> Option<u64> {
    let success = last_success_at?;
    observed_at
        .duration_since(success)
        .ok()
        .map(|duration| duration.as_millis() as u64)
}

pub fn coverage_response(coverage: &SharedCoverage) -> CoverageResponse {
    let degraded_resource_kinds = coverage
        .lock()
        .expect("coverage lock poisoned")
        .iter()
        .cloned()
        .collect();
    CoverageResponse {
        degraded_resource_kinds,
    }
}

pub fn sync_response(sync: &SyncHealth, observed_at: SystemTime) -> SyncHealthResponse {
    SyncHealthResponse {
        loop_alive: sync.loop_alive,
        poll_interval_seconds: sync.poll_interval_seconds,
        total_attempts: sync.total_attempts,
        total_successes: sync.total_successes,
        last_attempt_at: format_timestamp_opt(sync.last_attempt_at),
        last_success_at: format_timestamp_opt(sync.last_success_at),
        last_write_at: format_timestamp_opt(sync.last_write_at),
        lag_ms: lag_ms(observed_at, sync.last_success_at),
        last_attempt_duration_ms: duration_ms_opt(sync.last_attempt_duration),
        last_fetch_duration_ms: duration_ms_opt(sync.last_fetch_duration),
        last_diff_duration_ms: duration_ms_opt(sync.last_diff_duration),
        last_write_duration_ms: duration_ms_opt(sync.last_write_duration),
        last_error: sync.last_error.clone().map(Into::into),
        last_error_at: format_timestamp_opt(sync.last_error_at),
        consecutive_errors: sync.consecutive_errors,
        last_diff: sync.last_diff.clone().map(Into::into),
    }
}

pub fn rebuild_response(rebuild: &RebuildHealth) -> RebuildHealthResponse {
    RebuildHealthResponse {
        loop_alive: rebuild.loop_alive,
        poll_interval_seconds: rebuild.poll_interval_seconds,
        total_attempts: rebuild.total_attempts,
        total_successes: rebuild.total_successes,
        last_attempt_at: format_timestamp_opt(rebuild.last_attempt_at),
        last_success_at: format_timestamp_opt(rebuild.last_success_at),
        last_duration_ms: duration_ms_opt(rebuild.last_duration),
        last_error: rebuild.last_error.clone().map(Into::into),
        last_error_at: format_timestamp_opt(rebuild.last_error_at),
        consecutive_errors: rebuild.consecutive_errors,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coverage_response_returns_sorted_kinds() {
        let coverage: SharedCoverage = Arc::new(Mutex::new(BTreeSet::new()));
        let mut guard = coverage.lock().expect("coverage lock poisoned");
        guard.insert("Pod".to_string());
        guard.insert("Namespace".to_string());
        guard.insert("ConfigMap".to_string());
        drop(guard);

        let response = coverage_response(&coverage);
        assert_eq!(
            response.degraded_resource_kinds,
            vec!["ConfigMap", "Namespace", "Pod"]
        );
    }

    #[test]
    fn lag_ms_returns_none_when_last_success_is_in_the_future() {
        let observed_at = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let future_success = SystemTime::UNIX_EPOCH + Duration::from_secs(11);
        assert_eq!(lag_ms(observed_at, Some(future_success)), None);
    }

    #[test]
    fn sync_response_maps_lag_diff_and_error_fields() {
        let mut sync = SyncHealth::bootstrap(SystemTime::UNIX_EPOCH + Duration::from_secs(2));
        sync.last_success_at = Some(SystemTime::UNIX_EPOCH + Duration::from_secs(8));
        sync.last_attempt_duration = Some(Duration::from_millis(15));
        sync.last_fetch_duration = Some(Duration::from_millis(6));
        sync.last_diff_duration = Some(Duration::from_millis(5));
        sync.last_write_duration = Some(Duration::from_millis(4));
        sync.last_error = Some(HealthError {
            stage: "graph_write".to_string(),
            message: "write failed".to_string(),
        });
        sync.last_diff = Some(DiffSummary {
            added_nodes: 1,
            removed_nodes: 2,
            modified_nodes: 3,
            added_edges: 4,
            removed_edges: 5,
        });

        let observed_at = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let response = sync_response(&sync, observed_at);
        assert_eq!(response.lag_ms, Some(2000));
        assert_eq!(response.last_attempt_duration_ms, Some(15));
        assert_eq!(response.last_fetch_duration_ms, Some(6));
        assert_eq!(response.last_diff_duration_ms, Some(5));
        assert_eq!(response.last_write_duration_ms, Some(4));
        assert_eq!(
            response
                .last_error
                .as_ref()
                .expect("missing last_error")
                .stage,
            "graph_write"
        );
        let last_diff = response.last_diff.expect("missing last_diff");
        assert_eq!(last_diff.added_nodes, 1);
        assert_eq!(last_diff.removed_nodes, 2);
        assert_eq!(last_diff.modified_nodes, 3);
        assert_eq!(last_diff.added_edges, 4);
        assert_eq!(last_diff.removed_edges, 5);
    }
}
