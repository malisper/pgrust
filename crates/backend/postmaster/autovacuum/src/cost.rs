use std::sync::atomic::Ordering::Relaxed;

use commands_vacuum::{
    set_vacuum_cost_delay, set_vacuum_cost_limit, vacuum_cost_delay, vacuum_cost_limit,
    VacuumFailsafeActive,
};
use init_small::globals as g;
use types_error::PgResult;

use crate::shmem::{self, AvLists, AV_STORAGE_PARAM_COST_DELAY, AV_STORAGE_PARAM_COST_LIMIT};
use crate::{autovacuum_vac_cost_delay, autovacuum_vac_cost_limit};

pub fn VacuumUpdateCosts() -> PgResult<()> {
    if shmem::my_worker_slot().is_some() {
        if AV_STORAGE_PARAM_COST_DELAY.get() >= 0.0 {
            set_vacuum_cost_delay(AV_STORAGE_PARAM_COST_DELAY.get());
        } else if autovacuum_vac_cost_delay() >= 0.0 {
            set_vacuum_cost_delay(autovacuum_vac_cost_delay());
        } else {
            set_vacuum_cost_delay(guc_tables::vars::VacuumCostDelay.read());
        }
        AutoVacuumUpdateCostLimit()?;
    } else {
        // Explicit VACUUM or ANALYZE.
        set_vacuum_cost_delay(guc_tables::vars::VacuumCostDelay.read());
        set_vacuum_cost_limit(guc_tables::vars::VacuumCostLimit.read());
    }

    if VacuumFailsafeActive() {
        debug_assert!(!g::VacuumCostActive());
    } else if vacuum_cost_delay() > 0.0 {
        g::SetVacuumCostActive(true);
    } else {
        g::SetVacuumCostActive(false);
        g::SetVacuumCostBalance(0);
    }
    Ok(())
}

pub fn AutoVacuumUpdateCostLimit() -> PgResult<()> {
    let Some(slot) = shmem::my_worker_slot() else {
        return Ok(());
    };

    // In cost_limit, zero also means "use value from elsewhere".
    if AV_STORAGE_PARAM_COST_LIMIT.get() > 0 {
        set_vacuum_cost_limit(AV_STORAGE_PARAM_COST_LIMIT.get());
    } else {
        if autovacuum_vac_cost_limit() > 0 {
            set_vacuum_cost_limit(autovacuum_vac_cost_limit());
        } else {
            set_vacuum_cost_limit(guc_tables::vars::VacuumCostLimit.read());
        }

        // Only balance when no cost-related storage parameters are set.
        if !slot.wi_dobalance.load(Relaxed) {
            return Ok(());
        }
        debug_assert!(vacuum_cost_limit() > 0);

        let nworkers_for_balance = shmem::nworkers_for_balance() as i32;
        if nworkers_for_balance <= 0 {
            return Err(types_error::PgError::error("nworkers_for_balance must be > 0").into());
        }
        set_vacuum_cost_limit((vacuum_cost_limit() / nworkers_for_balance).max(1));
    }
    Ok(())
}

pub fn autovac_recalculate_workers_for_balance(l: &AvLists) {
    let slots = shmem::worker_slots();
    let mut n: u32 = 0;
    for &idx in &l.running_workers {
        let w = &slots[idx];
        if w.wi_proc_pid.load(Relaxed) == 0 || !w.wi_dobalance.load(Relaxed) {
            continue;
        }
        n += 1;
    }
    if n != shmem::nworkers_for_balance() {
        shmem::set_nworkers_for_balance(n);
    }
}
