use std::sync::Arc;
use std::time::Duration;

use crate::backend::access::transam::xact::{
    FIRST_NORMAL_TRANSACTION_ID, TransactionId, transaction_id_is_normal,
};
use crate::backend::executor::ExecError;
use crate::pgrust::cluster::{Cluster, ClusterShared};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AutovacuumConfig {
    pub enabled: bool,
    pub naptime: Duration,
    pub vacuum_threshold: i64,
    pub vacuum_max_threshold: i64,
    pub vacuum_scale_factor: f64,
    pub vacuum_insert_threshold: i64,
    pub vacuum_insert_scale_factor: f64,
    pub analyze_threshold: i64,
    pub analyze_scale_factor: f64,
    pub freeze_max_age: TransactionId,
}

impl AutovacuumConfig {
    pub fn production_default() -> Self {
        Self {
            enabled: true,
            naptime: Duration::from_secs(60),
            vacuum_threshold: 50,
            vacuum_max_threshold: 100_000_000,
            vacuum_scale_factor: 0.2,
            vacuum_insert_threshold: 1_000,
            vacuum_insert_scale_factor: 0.2,
            analyze_threshold: 50,
            analyze_scale_factor: 0.1,
            freeze_max_age: 200_000_000,
        }
    }

    pub fn test_default() -> Self {
        Self {
            enabled: false,
            ..Self::production_default()
        }
    }

    pub fn value_for_show(&self, name: &str) -> Option<String> {
        match name {
            "autovacuum" => Some(if self.enabled { "on" } else { "off" }.into()),
            "autovacuum_naptime" => Some(format_duration_for_show(self.naptime)),
            "autovacuum_vacuum_threshold" => Some(self.vacuum_threshold.to_string()),
            "autovacuum_vacuum_max_threshold" => Some(self.vacuum_max_threshold.to_string()),
            "autovacuum_vacuum_scale_factor" => Some(self.vacuum_scale_factor.to_string()),
            "autovacuum_vacuum_insert_threshold" => Some(self.vacuum_insert_threshold.to_string()),
            "autovacuum_vacuum_insert_scale_factor" => {
                Some(self.vacuum_insert_scale_factor.to_string())
            }
            "autovacuum_analyze_threshold" => Some(self.analyze_threshold.to_string()),
            "autovacuum_analyze_scale_factor" => Some(self.analyze_scale_factor.to_string()),
            "autovacuum_freeze_max_age" => Some(self.freeze_max_age.to_string()),
            // :HACK: Worker count, work memory, and cost throttling GUCs are
            // recognized and rejected at runtime, but their behavior is
            // intentionally deferred for the single-worker MVP.
            _ => None,
        }
    }
}

fn format_duration_for_show(duration: Duration) -> String {
    let secs = duration.as_secs();
    if secs % 60 == 0 {
        format!("{}min", secs / 60)
    } else {
        format!("{secs}s")
    }
}

pub(crate) fn is_autovacuum_guc(name: &str) -> bool {
    matches!(
        name,
        "autovacuum"
            | "autovacuum_naptime"
            | "autovacuum_vacuum_threshold"
            | "autovacuum_vacuum_max_threshold"
            | "autovacuum_vacuum_scale_factor"
            | "autovacuum_vacuum_insert_threshold"
            | "autovacuum_vacuum_insert_scale_factor"
            | "autovacuum_analyze_threshold"
            | "autovacuum_analyze_scale_factor"
            | "autovacuum_freeze_max_age"
            | "autovacuum_max_workers"
            | "autovacuum_work_mem"
            | "autovacuum_vacuum_cost_delay"
            | "autovacuum_vacuum_cost_limit"
    )
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct AutovacuumRelationInput {
    pub reltuples: f64,
    pub relpages: i32,
    pub relallfrozen: i32,
    pub relfrozenxid: TransactionId,
    pub next_xid: TransactionId,
    pub dead_tuples: i64,
    pub mod_since_analyze: i64,
    pub ins_since_vacuum: i64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct AutovacuumDecision {
    pub vacuum: bool,
    pub analyze: bool,
    pub wraparound: bool,
}

pub(crate) fn relation_needs_vacanalyze(
    input: AutovacuumRelationInput,
    config: AutovacuumConfig,
) -> AutovacuumDecision {
    let reltuples = input.reltuples.max(0.0);
    let wraparound = transaction_id_is_normal(input.relfrozenxid)
        && input
            .next_xid
            .saturating_sub(input.relfrozenxid)
            .max(FIRST_NORMAL_TRANSACTION_ID)
            > config.freeze_max_age;

    let mut vacuum_threshold =
        config.vacuum_threshold as f64 + config.vacuum_scale_factor * reltuples;
    if config.vacuum_max_threshold >= 0 {
        vacuum_threshold = vacuum_threshold.min(config.vacuum_max_threshold as f64);
    }

    let unfrozen_fraction = if input.relpages > 0 && input.relallfrozen > 0 {
        let all_frozen = input.relallfrozen.min(input.relpages) as f64;
        1.0 - (all_frozen / input.relpages as f64)
    } else {
        1.0
    };
    let insert_threshold = config.vacuum_insert_threshold as f64
        + config.vacuum_insert_scale_factor * reltuples * unfrozen_fraction;
    let analyze_threshold =
        config.analyze_threshold as f64 + config.analyze_scale_factor * reltuples;

    AutovacuumDecision {
        vacuum: wraparound
            || (input.dead_tuples as f64) > vacuum_threshold
            || (config.vacuum_insert_threshold >= 0
                && (input.ins_since_vacuum as f64) > insert_threshold),
        analyze: (input.mod_since_analyze as f64) > analyze_threshold,
        wraparound,
    }
}

pub(crate) struct AutovacuumRuntime {
    #[cfg(not(target_arch = "wasm32"))]
    shutdown: std::sync::atomic::AtomicBool,
    #[cfg(not(target_arch = "wasm32"))]
    state: parking_lot::Mutex<()>,
    #[cfg(not(target_arch = "wasm32"))]
    cv: parking_lot::Condvar,
    #[cfg(not(target_arch = "wasm32"))]
    handle: parking_lot::Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl AutovacuumRuntime {
    pub(crate) fn new() -> Self {
        Self {
            #[cfg(not(target_arch = "wasm32"))]
            shutdown: std::sync::atomic::AtomicBool::new(false),
            #[cfg(not(target_arch = "wasm32"))]
            state: parking_lot::Mutex::new(()),
            #[cfg(not(target_arch = "wasm32"))]
            cv: parking_lot::Condvar::new(),
            #[cfg(not(target_arch = "wasm32"))]
            handle: parking_lot::Mutex::new(None),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn start(
        self: &Arc<Self>,
        shared: std::sync::Weak<ClusterShared>,
        config: AutovacuumConfig,
    ) {
        if !config.enabled {
            return;
        }
        let runtime = Arc::clone(self);
        let handle = std::thread::Builder::new()
            .name("autovacuum launcher".into())
            .spawn(move || runtime.worker_main(shared, config))
            .expect("failed to spawn autovacuum launcher thread");
        *self.handle.lock() = Some(handle);
    }

    #[cfg(target_arch = "wasm32")]
    pub(crate) fn start(
        self: &Arc<Self>,
        _shared: std::sync::Weak<ClusterShared>,
        _config: AutovacuumConfig,
    ) {
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn shutdown_and_join(&self) {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::Relaxed);
        self.cv.notify_all();
        if let Some(handle) = self.handle.lock().take() {
            if handle.thread().id() == std::thread::current().id() {
                return;
            }
            let _ = handle.join();
        }
    }

    #[cfg(target_arch = "wasm32")]
    pub(crate) fn shutdown_and_join(&self) {}

    #[cfg(not(target_arch = "wasm32"))]
    fn worker_main(
        self: Arc<Self>,
        shared: std::sync::Weak<ClusterShared>,
        config: AutovacuumConfig,
    ) {
        loop {
            if self.shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }
            if let Some(shared) = shared.upgrade() {
                let cluster = Cluster::from_shared(shared);
                let _ = run_autovacuum_cluster_once(&cluster);
            } else {
                break;
            }

            if self.shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }
            let mut guard = self.state.lock();
            if self.shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }
            self.cv.wait_for(&mut guard, config.naptime);
        }
    }
}

pub(crate) fn run_autovacuum_cluster_once(cluster: &Cluster) -> Result<(), ExecError> {
    let rows = cluster
        .shared()
        .shared_catalog
        .read()
        .catcache()
        .map_err(ExecError::from)?
        .database_rows();
    for row in rows {
        if !row.datallowconn {
            continue;
        }
        let db = cluster.connect_database(&row.datname)?;
        db.run_autovacuum_once()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> AutovacuumConfig {
        AutovacuumConfig {
            enabled: false,
            naptime: Duration::from_secs(60),
            vacuum_threshold: 50,
            vacuum_max_threshold: 100_000_000,
            vacuum_scale_factor: 0.2,
            vacuum_insert_threshold: 1_000,
            vacuum_insert_scale_factor: 0.2,
            analyze_threshold: 50,
            analyze_scale_factor: 0.1,
            freeze_max_age: 200_000_000,
        }
    }

    #[test]
    fn autovacuum_decision_uses_dead_tuple_threshold() {
        let decision = relation_needs_vacanalyze(
            AutovacuumRelationInput {
                reltuples: 100.0,
                dead_tuples: 80,
                mod_since_analyze: 0,
                ins_since_vacuum: 0,
                relpages: 10,
                relallfrozen: 0,
                relfrozenxid: 3,
                next_xid: 10,
            },
            config(),
        );
        assert!(decision.vacuum);
        assert!(!decision.analyze);
    }

    #[test]
    fn autovacuum_decision_uses_analyze_threshold() {
        let decision = relation_needs_vacanalyze(
            AutovacuumRelationInput {
                reltuples: 100.0,
                dead_tuples: 0,
                mod_since_analyze: 70,
                ins_since_vacuum: 0,
                relpages: 10,
                relallfrozen: 0,
                relfrozenxid: 3,
                next_xid: 10,
            },
            config(),
        );
        assert!(!decision.vacuum);
        assert!(decision.analyze);
    }

    #[test]
    fn autovacuum_decision_uses_insert_threshold_and_all_frozen_fraction() {
        let mut mostly_frozen = config();
        mostly_frozen.vacuum_insert_threshold = 1_000;
        mostly_frozen.vacuum_insert_scale_factor = 0.2;

        let decision = relation_needs_vacanalyze(
            AutovacuumRelationInput {
                reltuples: 10_000.0,
                dead_tuples: 0,
                mod_since_analyze: 0,
                ins_since_vacuum: 1_201,
                relpages: 100,
                relallfrozen: 90,
                relfrozenxid: 3,
                next_xid: 10,
            },
            mostly_frozen,
        );
        assert!(decision.vacuum);

        let not_frozen = relation_needs_vacanalyze(
            AutovacuumRelationInput {
                reltuples: 10_000.0,
                dead_tuples: 0,
                mod_since_analyze: 0,
                ins_since_vacuum: 1_201,
                relpages: 100,
                relallfrozen: 0,
                relfrozenxid: 3,
                next_xid: 10,
            },
            mostly_frozen,
        );
        assert!(!not_frozen.vacuum);
    }

    #[test]
    fn autovacuum_decision_applies_vacuum_max_threshold() {
        let mut config = config();
        config.vacuum_max_threshold = 10;
        let decision = relation_needs_vacanalyze(
            AutovacuumRelationInput {
                reltuples: 1_000_000.0,
                dead_tuples: 11,
                mod_since_analyze: 0,
                ins_since_vacuum: 0,
                relpages: 100,
                relallfrozen: 0,
                relfrozenxid: 3,
                next_xid: 10,
            },
            config,
        );
        assert!(decision.vacuum);
    }

    #[test]
    fn autovacuum_decision_forces_wraparound_vacuum() {
        let decision = relation_needs_vacanalyze(
            AutovacuumRelationInput {
                reltuples: 1.0,
                dead_tuples: 0,
                mod_since_analyze: 0,
                ins_since_vacuum: 0,
                relpages: 1,
                relallfrozen: 0,
                relfrozenxid: 3,
                next_xid: 300_000_005,
            },
            config(),
        );
        assert!(decision.vacuum);
        assert!(decision.wraparound);
    }
}
