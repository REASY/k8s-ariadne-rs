use super::*;

pub(super) fn describe_source_sync_metrics() {
    SOURCE_SYNC_METRICS_INIT.call_once(|| {
        describe_counter!(
            SOURCE_SYNC_ATTEMPTS_TOTAL,
            Unit::Count,
            "Total number of source sync loop attempts."
        );
        describe_histogram!(
            SOURCE_SYNC_ATTEMPT_DURATION_MS,
            Unit::Milliseconds,
            "End-to-end duration of a source sync attempt."
        );
        describe_histogram!(
            SOURCE_SYNC_FETCH_DURATION_MS,
            Unit::Milliseconds,
            "Time spent reading the current Kubernetes snapshot for a source sync attempt."
        );
        describe_histogram!(
            SOURCE_SYNC_DIFF_DURATION_MS,
            Unit::Milliseconds,
            "Time spent diffing the current snapshot against the previous graph state."
        );
        describe_histogram!(
            SOURCE_SYNC_WRITE_DURATION_MS,
            Unit::Milliseconds,
            "Time spent applying a non-empty source sync diff to the graph backend."
        );
        describe_gauge!(
            SOURCE_SYNC_LAST_ATTEMPT_DURATION_MS,
            Unit::Milliseconds,
            "Duration of the most recent source sync attempt."
        );
        describe_gauge!(
            SOURCE_SYNC_LAST_FETCH_DURATION_MS,
            Unit::Milliseconds,
            "Fetch duration of the most recent source sync attempt."
        );
        describe_gauge!(
            SOURCE_SYNC_LAST_DIFF_DURATION_MS,
            Unit::Milliseconds,
            "Diff duration of the most recent source sync attempt."
        );
        describe_gauge!(
            SOURCE_SYNC_LAST_WRITE_DURATION_MS,
            Unit::Milliseconds,
            "Write duration of the most recent source sync attempt, or zero if no graph write occurred."
        );
    });
}

fn record_source_sync_metrics(
    status: &'static str,
    total_duration: Duration,
    fetch_duration: Duration,
    diff_duration: Option<Duration>,
    write_duration: Option<Duration>,
) {
    counter!(SOURCE_SYNC_ATTEMPTS_TOTAL, "status" => status).increment(1);
    histogram!(SOURCE_SYNC_ATTEMPT_DURATION_MS, "status" => status)
        .record(duration_ms_f64(total_duration));
    histogram!(SOURCE_SYNC_FETCH_DURATION_MS, "status" => status)
        .record(duration_ms_f64(fetch_duration));
    gauge!(SOURCE_SYNC_LAST_ATTEMPT_DURATION_MS).set(duration_ms_f64(total_duration));
    gauge!(SOURCE_SYNC_LAST_FETCH_DURATION_MS).set(duration_ms_f64(fetch_duration));

    let diff_ms = diff_duration.unwrap_or(Duration::ZERO);
    histogram!(SOURCE_SYNC_DIFF_DURATION_MS, "status" => status).record(duration_ms_f64(diff_ms));
    gauge!(SOURCE_SYNC_LAST_DIFF_DURATION_MS).set(duration_ms_f64(diff_ms));

    let write_ms = write_duration.unwrap_or(Duration::ZERO);
    histogram!(SOURCE_SYNC_WRITE_DURATION_MS, "status" => status).record(duration_ms_f64(write_ms));
    gauge!(SOURCE_SYNC_LAST_WRITE_DURATION_MS).set(duration_ms_f64(write_ms));
}

fn sync_stage_label(stage: SourceSyncStage) -> &'static str {
    match stage {
        SourceSyncStage::KubeFetch => "kube_fetch",
        SourceSyncStage::Diff => "diff",
        SourceSyncStage::GraphWrite => "graph_write",
    }
}

fn rebuild_stage_label(stage: RebuildStage) -> &'static str {
    match stage {
        RebuildStage::StateRead => "state_read",
        RebuildStage::GraphWrite => "graph_write",
    }
}

fn diff_summary(summary: StateDiffSummary) -> DiffSummary {
    DiffSummary {
        added_nodes: summary.added_nodes,
        removed_nodes: summary.removed_nodes,
        modified_nodes: summary.modified_nodes,
        added_edges: summary.added_edges,
        removed_edges: summary.removed_edges,
    }
}

pub(super) async fn run_source_sync_loop(
    resolver: Arc<ClusterStateResolver>,
    graph: Arc<dyn GraphBackend>,
    sync_health: Arc<Mutex<SyncHealth>>,
    token: CancellationToken,
    poll_interval: Duration,
) -> errors::Result<()> {
    info!("Starting source sync loop with poll_interval {poll_interval:?}");
    {
        let mut sync = sync_health.lock().expect("source_sync lock poisoned");
        sync.loop_alive = true;
    }
    let mut iterations: usize = 0;

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                break;
            }
            _ = sleep(poll_interval) => {
                let attempt_started_at = SystemTime::now();
                match resolver.sync_from_source(graph.clone()).await {
                    Ok(outcome) => {
                        let completed_at = SystemTime::now();
                        let total_duration = outcome.fetch_duration
                            + outcome.diff_duration
                            + outcome.write_duration.unwrap_or(Duration::ZERO);
                        record_source_sync_metrics(
                            "success",
                            total_duration,
                            outcome.fetch_duration,
                            Some(outcome.diff_duration),
                            outcome.write_duration,
                        );
                        info!(
                            total_ms = duration_ms(total_duration),
                            snapshot_read_ms = duration_ms(outcome.fetch_duration),
                            diff_ms = duration_ms(outcome.diff_duration),
                            write_ms = outcome.write_duration.map(duration_ms).unwrap_or(0),
                            added_nodes = outcome.diff.added_nodes,
                            removed_nodes = outcome.diff.removed_nodes,
                            modified_nodes = outcome.diff.modified_nodes,
                            added_edges = outcome.diff.added_edges,
                            removed_edges = outcome.diff.removed_edges,
                            "source sync completed"
                        );
                        let mut sync = sync_health.lock().expect("source_sync lock poisoned");
                        sync.total_attempts += 1;
                        sync.total_successes += 1;
                        sync.last_attempt_at = Some(attempt_started_at);
                        sync.last_success_at = Some(completed_at);
                        if outcome.write_duration.is_some() {
                            sync.last_write_at = Some(completed_at);
                        }
                        sync.last_attempt_duration = Some(total_duration);
                        sync.last_fetch_duration = Some(outcome.fetch_duration);
                        sync.last_diff_duration = Some(outcome.diff_duration);
                        sync.last_write_duration = outcome.write_duration;
                        sync.last_error = None;
                        sync.last_error_at = None;
                        sync.consecutive_errors = 0;
                        sync.last_diff = Some(diff_summary(outcome.diff));
                    }
                    Err(err) => {
                        let completed_at = SystemTime::now();
                        let total_duration = sum_durations(&[
                            err.fetch_duration,
                            err.diff_duration,
                            err.write_duration,
                        ])
                        .unwrap_or(Duration::ZERO);
                        record_source_sync_metrics(
                            "error",
                            total_duration,
                            err.fetch_duration.unwrap_or(Duration::ZERO),
                            err.diff_duration,
                            err.write_duration,
                        );
                        warn!(
                            stage = sync_stage_label(err.stage),
                            total_ms = duration_ms(total_duration),
                            snapshot_read_ms = err.fetch_duration.map(duration_ms).unwrap_or(0),
                            diff_ms = err.diff_duration.map(duration_ms).unwrap_or(0),
                            write_ms = err.write_duration.map(duration_ms).unwrap_or(0),
                            error = %err.message,
                            "source sync iteration failed"
                        );
                        let mut sync = sync_health.lock().expect("source_sync lock poisoned");
                        sync.total_attempts += 1;
                        sync.last_attempt_at = Some(attempt_started_at);
                        sync.last_attempt_duration = sum_durations(&[
                            err.fetch_duration,
                            err.diff_duration,
                            err.write_duration,
                        ]);
                        sync.last_fetch_duration = err.fetch_duration;
                        sync.last_diff_duration = err.diff_duration;
                        sync.last_write_duration = err.write_duration;
                        sync.last_error = Some(HealthError {
                            stage: sync_stage_label(err.stage).to_string(),
                            message: err.message,
                        });
                        sync.last_error_at = Some(completed_at);
                        sync.consecutive_errors += 1;
                    }
                }
                iterations += 1;
            }
        }
    }

    {
        let mut sync = sync_health.lock().expect("source_sync lock poisoned");
        sync.loop_alive = false;
    }
    info!("Stopped source sync loop, number of loops {iterations}");
    Ok(())
}

pub(super) async fn run_rebuild_loop(
    resolver: Arc<ClusterStateResolver>,
    graph: Arc<dyn GraphBackend>,
    rebuild_health: Arc<Mutex<Option<RebuildHealth>>>,
    token: CancellationToken,
    poll_interval: Duration,
) -> errors::Result<()> {
    info!("Starting full rebuild loop with poll_interval {poll_interval:?}");
    {
        let mut rebuild = rebuild_health.lock().expect("rebuild lock poisoned");
        if let Some(rebuild) = rebuild.as_mut() {
            rebuild.loop_alive = true;
        }
    }
    let mut iterations: usize = 0;

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                break;
            }
            _ = sleep(poll_interval) => {
                let attempt_started_at = SystemTime::now();
                match resolver.rebuild_from_source(graph.clone()).await {
                    Ok(outcome) => {
                        let completed_at = SystemTime::now();
                        let mut rebuild = rebuild_health.lock().expect("rebuild lock poisoned");
                        if let Some(rebuild) = rebuild.as_mut() {
                            rebuild.total_attempts += 1;
                            rebuild.total_successes += 1;
                            rebuild.last_attempt_at = Some(attempt_started_at);
                            rebuild.last_success_at = Some(completed_at);
                            rebuild.last_duration = Some(outcome.fetch_duration + outcome.write_duration);
                            rebuild.last_error = None;
                            rebuild.last_error_at = None;
                            rebuild.consecutive_errors = 0;
                        }
                    }
                    Err(err) => {
                        let completed_at = SystemTime::now();
                        warn!(
                            stage = rebuild_stage_label(err.stage),
                            error = %err.message,
                            "full rebuild iteration failed"
                        );
                        let mut rebuild = rebuild_health.lock().expect("rebuild lock poisoned");
                        if let Some(rebuild) = rebuild.as_mut() {
                            rebuild.total_attempts += 1;
                            rebuild.last_attempt_at = Some(attempt_started_at);
                            rebuild.last_duration =
                                sum_durations(&[err.fetch_duration, err.write_duration]);
                            rebuild.last_error = Some(HealthError {
                                stage: rebuild_stage_label(err.stage).to_string(),
                                message: err.message,
                            });
                            rebuild.last_error_at = Some(completed_at);
                            rebuild.consecutive_errors += 1;
                        }
                    }
                }
                iterations += 1;
            }
        }
    }

    {
        let mut rebuild = rebuild_health.lock().expect("rebuild lock poisoned");
        if let Some(rebuild) = rebuild.as_mut() {
            rebuild.loop_alive = false;
        }
    }
    info!("Stopped full rebuild loop, number of loops {iterations}");
    Ok(())
}
