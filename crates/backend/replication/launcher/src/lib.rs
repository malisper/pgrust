#![allow(non_snake_case)]

use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

use guc_tables::{vars, GucVarAccessors};

static MAX_LOGICAL_REPLICATION_WORKERS: AtomicI32 = AtomicI32::new(4);
static MAX_SYNC_WORKERS_PER_SUBSCRIPTION: AtomicI32 = AtomicI32::new(2);
static MAX_PARALLEL_APPLY_WORKERS_PER_SUBSCRIPTION: AtomicI32 = AtomicI32::new(2);

// C divergence: bgworker.c's registry is unported, so the registration is
// recorded here and never launched — nothing consults it until a subscription exists.
static LAUNCHER_REGISTERED: AtomicBool = AtomicBool::new(false);

thread_local! {
    static ON_COMMIT_LAUNCHER_WAKEUP: Cell<bool> = const { Cell::new(false) };
}

pub fn max_logical_replication_workers() -> i32 {
    MAX_LOGICAL_REPLICATION_WORKERS.load(Ordering::Relaxed)
}

pub fn ApplyLauncherRegister() {
    if max_logical_replication_workers() == 0 || init_small::globals::IsBinaryUpgrade() {
        return;
    }
    LAUNCHER_REGISTERED.store(true, Ordering::Relaxed);
}

pub fn apply_launcher_registered() -> bool {
    LAUNCHER_REGISTERED.load(Ordering::Relaxed)
}

// GetLeaderApplyWorkerPid (launcher.c): scans LogicalRepCtx->workers for a
// parallel apply worker owning `pid`. No logical-rep worker pool exists here
// (workers are never launched), so the scan is empty: always InvalidPid.
pub fn GetLeaderApplyWorkerPid(_pid: i32) -> i32 {
    -1
}

pub fn AtEOXact_ApplyLauncher(is_commit: bool) {
    if is_commit && ON_COMMIT_LAUNCHER_WAKEUP.get() {
        panic!("AtEOXact_ApplyLauncher: ApplyLauncherWakeup unported (backend-replication-logical-launcher)");
    }
    ON_COMMIT_LAUNCHER_WAKEUP.set(false);
}

pub fn init_seams() {
    vars::max_logical_replication_workers.install(GucVarAccessors {
        get: max_logical_replication_workers,
        set: |v| MAX_LOGICAL_REPLICATION_WORKERS.store(v, Ordering::Relaxed),
    });
    vars::max_sync_workers_per_subscription.install(GucVarAccessors {
        get: || MAX_SYNC_WORKERS_PER_SUBSCRIPTION.load(Ordering::Relaxed),
        set: |v| MAX_SYNC_WORKERS_PER_SUBSCRIPTION.store(v, Ordering::Relaxed),
    });
    vars::max_parallel_apply_workers_per_subscription.install(GucVarAccessors {
        get: || MAX_PARALLEL_APPLY_WORKERS_PER_SUBSCRIPTION.load(Ordering::Relaxed),
        set: |v| MAX_PARALLEL_APPLY_WORKERS_PER_SUBSCRIPTION.store(v, Ordering::Relaxed),
    });
    launcher_seams::apply_launcher_register::set(ApplyLauncherRegister);
    launcher_seams::get_leader_apply_worker_pid::set(GetLeaderApplyWorkerPid);
}
