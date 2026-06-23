//! Cost-based vacuum delay/limit balancing across autovacuum workers
//! (`autovacuum.c` lines 1647-1794).
//!
//! Everything genuinely external (the `vacuum_cost_*`/`VacuumCost*` globals,
//! the `VacuumFailsafeActive` flag, `AutovacuumLock`, the running-worker list,
//! and the `wi_dobalance`/`av_nworkersForBalance` shmem atomics) routes through
//! the ext-seams.

extern crate alloc;

use ::utils_error::{elog, message_level_is_interesting, PgResult};
use ::types_error::{DEBUG2, ERROR};

use crate::core;
use autovacuum_ext_seams as seam;

/// `void VacuumUpdateCosts(void)` (`autovacuum.c` lines 1653-1712).
///
/// Update vacuum cost-based delay-related parameters for autovacuum workers and
/// backends executing VACUUM or ANALYZE using the value of relevant GUCs and
/// global state. Called during vacuum setup and after every config reload.
pub fn VacuumUpdateCosts() -> PgResult<()> {
    if core::MyWorkerInfo() >= 0 {
        if core::av_storage_param_cost_delay() >= 0.0 {
            seam::set_vacuum_cost_delay::call(core::av_storage_param_cost_delay());
        } else if core::autovacuum_vac_cost_delay() >= 0.0 {
            seam::set_vacuum_cost_delay::call(core::autovacuum_vac_cost_delay());
        } else {
            /* fall back to VacuumCostDelay */
            seam::set_vacuum_cost_delay::call(seam::vacuum_cost_delay_guc::call());
        }

        AutoVacuumUpdateCostLimit()?;
    } else {
        /* Must be explicit VACUUM or ANALYZE */
        seam::set_vacuum_cost_delay::call(seam::vacuum_cost_delay_guc::call());
        seam::set_vacuum_cost_limit::call(seam::vacuum_cost_limit_guc::call());
    }

    /*
     * If configuration changes are allowed to impact VacuumCostActive, make
     * sure it is updated.
     */
    if seam::vacuum_failsafe_active::call() {
        debug_assert!(!seam::vacuum_cost_active::call());
    } else if seam::vacuum_cost_delay::call() > 0.0 {
        seam::set_vacuum_cost_active::call(true);
    } else {
        seam::set_vacuum_cost_active::call(false);
        seam::set_vacuum_cost_balance::call(0);
    }

    /*
     * Since the cost logging requires a lock, avoid rendering the log message
     * in case we are using a message level where the log wouldn't be emitted.
     */
    if core::MyWorkerInfo() >= 0 && message_level_is_interesting(DEBUG2) {
        let my = core::MyWorkerInfo();

        debug_assert!(!seam::autovacuum_lock_held_by_me::call());

        seam::autovacuum_lock_acquire_shared::call()?;
        let dboid = seam::worker_get_dboid::call(my);
        let tableoid = seam::worker_get_tableoid::call(my);
        seam::autovacuum_lock_release::call()?;

        let dobalance = if seam::worker_dobalance_unlocked_test::call(my) {
            "no"
        } else {
            "yes"
        };
        let vacuum_cost_limit = seam::vacuum_cost_limit::call();
        let vacuum_cost_delay = seam::vacuum_cost_delay::call();
        let active = if vacuum_cost_delay > 0.0 { "yes" } else { "no" };
        let failsafe = if seam::vacuum_failsafe_active::call() {
            "yes"
        } else {
            "no"
        };

        elog(
            DEBUG2,
            alloc::format!(
                "Autovacuum VacuumUpdateCosts(db={dboid}, rel={tableoid}, dobalance={dobalance}, \
                 cost_limit={vacuum_cost_limit}, cost_delay={vacuum_cost_delay} active={active} \
                 failsafe={failsafe})"
            ),
        )?;
    }

    Ok(())
}

/// `void AutoVacuumUpdateCostLimit(void)` (`autovacuum.c` lines 1722-1758).
///
/// Update `vacuum_cost_limit` with the correct value for an autovacuum worker,
/// given the value of other relevant cost limit parameters and the number of
/// workers across which the limit must be balanced.
pub fn AutoVacuumUpdateCostLimit() -> PgResult<()> {
    let my = core::MyWorkerInfo();
    if my < 0 {
        return Ok(());
    }

    /*
     * note: in cost_limit, zero also means use value from elsewhere, because
     * zero is not a valid value.
     */

    if core::av_storage_param_cost_limit() > 0 {
        seam::set_vacuum_cost_limit::call(core::av_storage_param_cost_limit());
    } else {
        if core::autovacuum_vac_cost_limit() > 0 {
            seam::set_vacuum_cost_limit::call(core::autovacuum_vac_cost_limit());
        } else {
            seam::set_vacuum_cost_limit::call(seam::vacuum_cost_limit_guc::call());
        }

        /* Only balance limit if no cost-related storage parameters specified */
        if seam::worker_dobalance_unlocked_test::call(my) {
            return Ok(());
        }

        debug_assert!(seam::vacuum_cost_limit::call() > 0);

        let nworkers_for_balance = seam::nworkers_for_balance_read::call() as i32;

        /* There is at least 1 autovac worker (this worker) */
        if nworkers_for_balance <= 0 {
            elog(ERROR, "nworkers_for_balance must be > 0")?;
        }

        let balanced = ::core::cmp::max(seam::vacuum_cost_limit::call() / nworkers_for_balance, 1);
        seam::set_vacuum_cost_limit::call(balanced);
    }

    Ok(())
}

/// `static void autovac_recalculate_workers_for_balance(void)`
/// (`autovacuum.c` lines 1768-1794).
///
/// Recalculate the number of workers to consider, given cost-related storage
/// parameters and the current number of active workers. Caller must hold the
/// `AutovacuumLock` in at least shared mode to access `worker->wi_proc`.
pub fn autovac_recalculate_workers_for_balance() -> PgResult<()> {
    debug_assert!(seam::autovacuum_lock_held_by_me::call());

    let orig_nworkers_for_balance = seam::nworkers_for_balance_read::call() as i32;
    let mut nworkers_for_balance: i32 = 0;

    for worker in seam::running_workers_slots::call() {
        if !seam::worker_proc_is_set::call(worker)
            || seam::worker_dobalance_unlocked_test::call(worker)
        {
            continue;
        }

        nworkers_for_balance += 1;
    }

    if nworkers_for_balance != orig_nworkers_for_balance {
        seam::nworkers_for_balance_write::call(nworkers_for_balance as u32);
    }

    Ok(())
}
