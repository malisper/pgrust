use std::sync::atomic::Ordering::Relaxed;

use commands_vacuum::{
    set_vacuum_cost_delay, set_vacuum_cost_limit, vacuum_cost_delay, vacuum_cost_limit,
    VacuumFailsafeActive,
};
use init_small::globals as g;
use types_core::Oid;
use types_error::{PgResult, DEBUG2};

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

    // autovacuum.c:1693: the cost trace, rendered only when DEBUG2 would be
    // emitted (C avoids the AutovacuumLock round trip otherwise; the slot's
    // dboid/tableoid are atomics here, readable without it).
    if let Some(slot) = shmem::my_worker_slot() {
        if elog::message_level_is_interesting(DEBUG2) {
            elog::elog(
                DEBUG2,
                vacuum_update_costs_debug_line(
                    slot.wi_dboid.load(Relaxed),
                    slot.wi_tableoid.load(Relaxed),
                    slot.wi_dobalance.load(Relaxed),
                    vacuum_cost_limit(),
                    vacuum_cost_delay(),
                    VacuumFailsafeActive(),
                ),
            )?;
        }
    }
    Ok(())
}

// autovacuum.c:1705: "Autovacuum VacuumUpdateCosts(db=%u, rel=%u,
// dobalance=%s, cost_limit=%d, cost_delay=%g active=%s failsafe=%s)";
// active is vacuum_cost_delay > 0.
fn vacuum_update_costs_debug_line(
    dboid: Oid,
    tableoid: Oid,
    dobalance: bool,
    cost_limit: i32,
    cost_delay: f64,
    failsafe: bool,
) -> String {
    let yn = |b: bool| if b { "yes" } else { "no" };
    format!(
        "Autovacuum VacuumUpdateCosts(db={dboid}, rel={tableoid}, dobalance={}, cost_limit={cost_limit}, cost_delay={} active={} failsafe={})",
        yn(dobalance),
        guc::fmt_g(cost_delay),
        yn(cost_delay > 0.0),
        yn(failsafe)
    )
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

#[cfg(test)]
mod tests {
    use super::*;

    // audit-18.6 b149: the VacuumUpdateCosts DEBUG2 trace (autovacuum.c:1705)
    // byte-matches C's printf rendering, "%g" of cost_delay included — the
    // line a C 18.6 worker logged at autovacuum_vacuum_cost_delay = 20 and
    // cost_limit = 50, the default 2ms delay, and a zero (inactive) delay.
    #[test]
    fn vacuum_update_costs_debug_line_matches_c() {
        assert_eq!(
            vacuum_update_costs_debug_line(5, 16394, true, 50, 20.0, false),
            "Autovacuum VacuumUpdateCosts(db=5, rel=16394, dobalance=yes, cost_limit=50, cost_delay=20 active=yes failsafe=no)"
        );
        assert_eq!(
            vacuum_update_costs_debug_line(1, 2619, false, 200, 2.5, true),
            "Autovacuum VacuumUpdateCosts(db=1, rel=2619, dobalance=no, cost_limit=200, cost_delay=2.5 active=yes failsafe=yes)"
        );
        assert_eq!(
            vacuum_update_costs_debug_line(5, 0, true, 200, 0.0, false),
            "Autovacuum VacuumUpdateCosts(db=5, rel=0, dobalance=yes, cost_limit=200, cost_delay=0 active=no failsafe=no)"
        );
    }
}
