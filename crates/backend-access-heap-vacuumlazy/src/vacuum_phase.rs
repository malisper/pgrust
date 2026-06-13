//! Phases II & III drivers — index vacuuming + failsafe (`vacuumlazy.c`).
//!
//!   * [`lazy_vacuum`] (vacuumlazy.c:2450).
//!   * [`lazy_vacuum_all_indexes`] (vacuumlazy.c:2575).
//!   * [`lazy_check_wraparound_failsafe`] (vacuumlazy.c:2950).
//!
//! The phase-III reaping functions (`lazy_vacuum_heap_rel` /
//! `lazy_vacuum_heap_page` / `heap_page_is_all_visible`) live in
//! [`crate::heap_vacuum`].

use backend_utils_error::{ereport};
use types_error::{ErrorLocation, WARNING};
use types_error::PgResult;

use crate::consts::{
    PROGRESS_VACUUM_INDEXES_PROCESSED, PROGRESS_VACUUM_INDEXES_TOTAL,
    PROGRESS_VACUUM_NUM_INDEX_VACUUMS, PROGRESS_VACUUM_PHASE, PROGRESS_VACUUM_PHASE_VACUUM_INDEX,
};
use crate::core::{parallel_vacuum_is_active, LVRelState, BYPASS_THRESHOLD_PAGES};
use crate::dead_items::dead_items_reset;
use crate::heap_vacuum::lazy_vacuum_heap_rel;
use crate::index::lazy_vacuum_one_index;

use backend_access_heap_vacuumlazy_seams as vl;

fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("vacuumlazy.c", 0, funcname)
}

/// `lazy_vacuum()` (vacuumlazy.c:2450) — remove the dead TIDs collected so far
/// from indexes (phase II) and then from the heap (phase III), unless the bypass
/// or failsafe paths apply.
pub fn lazy_vacuum(vacrel: &mut LVRelState) -> PgResult<()> {
    debug_assert!(vacrel.nindexes > 0);
    debug_assert!(vacrel.lpdead_item_pages > 0);

    if !vacrel.do_index_vacuuming {
        debug_assert!(!vacrel.do_index_cleanup);
        dead_items_reset(vacrel)?;
        return Ok(());
    }

    /*
     * Consider bypassing index vacuuming (and heap vacuuming) entirely when the
     * number of LP_DEAD items for the whole VACUUM is close to zero.
     */
    let mut bypass = false;
    if vacrel.consider_bypass_optimization && vacrel.rel_pages > 0 {
        debug_assert!(vacrel.num_index_scans == 0);
        debug_assert!(vacrel.lpdead_items == vacrel.dead_items_info.num_items);
        debug_assert!(vacrel.do_index_vacuuming);
        debug_assert!(vacrel.do_index_cleanup);

        let threshold = (vacrel.rel_pages as f64 * BYPASS_THRESHOLD_PAGES) as types_core::BlockNumber;
        bypass = vacrel.lpdead_item_pages < threshold
            && vl::tidstore_memory_usage::call(vacrel.dead_items)? < 32 * 1024 * 1024;
    }

    if bypass {
        /* Behave as if there were precisely zero TIDs: bypass index vacuuming. */
        vacrel.do_index_vacuuming = false;
    } else if lazy_vacuum_all_indexes(vacrel)? {
        /* A round of index vacuuming succeeded; do related heap vacuuming now. */
        lazy_vacuum_heap_rel(vacrel)?;
    } else {
        /* Failsafe case: we didn't finish a full round/full index scan. */
        debug_assert!(vl::vacuum_failsafe_active::call()?);
    }

    /* Forget the LP_DEAD items we just vacuumed (or decided not to). */
    dead_items_reset(vacrel)?;
    Ok(())
}

/// `lazy_vacuum_all_indexes()` (vacuumlazy.c:2575) — perform index bulk deletion
/// across every index. Returns `true` iff all indexes were successfully vacuumed.
pub fn lazy_vacuum_all_indexes(vacrel: &mut LVRelState) -> PgResult<bool> {
    let mut allindexes = true;
    let old_live_tuples = vl::relation_get_reltuples::call(vacrel.rel)?;

    debug_assert!(vacrel.nindexes > 0);
    debug_assert!(vacrel.do_index_vacuuming);
    debug_assert!(vacrel.do_index_cleanup);

    /* Precheck for XID wraparound emergencies. */
    if lazy_check_wraparound_failsafe(vacrel)? {
        return Ok(false);
    }

    /* Report that we are now vacuuming indexes and how many. */
    vl::pgstat_progress_update_multi_param::call(
        vec![PROGRESS_VACUUM_PHASE, PROGRESS_VACUUM_INDEXES_TOTAL],
        vec![PROGRESS_VACUUM_PHASE_VACUUM_INDEX, vacrel.nindexes as i64],
    )?;

    if !parallel_vacuum_is_active(vacrel) {
        for idx in 0..vacrel.nindexes as usize {
            let indrel = vacrel.indrels[idx];
            let istat = vacrel.indstats[idx];

            let new_istat = lazy_vacuum_one_index(indrel, istat, old_live_tuples, vacrel)?;
            vacrel.indstats[idx] = new_istat;

            vl::pgstat_progress_update_param::call(
                PROGRESS_VACUUM_INDEXES_PROCESSED,
                (idx + 1) as i64,
            )?;

            if lazy_check_wraparound_failsafe(vacrel)? {
                allindexes = false;
                break;
            }
        }
    } else {
        vl::parallel_vacuum_bulkdel_all_indexes::call(
            vacrel.pvs,
            old_live_tuples,
            vacrel.num_index_scans,
        )?;

        if lazy_check_wraparound_failsafe(vacrel)? {
            allindexes = false;
        }
    }

    debug_assert!(
        vacrel.num_index_scans > 0 || vacrel.dead_items_info.num_items == vacrel.lpdead_items
    );
    debug_assert!(allindexes || vl::vacuum_failsafe_active::call()?);

    /* Increase + report the number of index scans; reset the index counters. */
    vacrel.num_index_scans += 1;
    vl::pgstat_progress_update_multi_param::call(
        vec![
            PROGRESS_VACUUM_INDEXES_TOTAL,
            PROGRESS_VACUUM_INDEXES_PROCESSED,
            PROGRESS_VACUUM_NUM_INDEX_VACUUMS,
        ],
        vec![0, 0, vacrel.num_index_scans as i64],
    )?;

    Ok(allindexes)
}

/// `lazy_check_wraparound_failsafe()` (vacuumlazy.c:2950) — check whether the
/// table's relfrozenxid/relminmxid is dangerously old and, if so, trigger the
/// failsafe. Returns `true` if the failsafe has been triggered.
pub fn lazy_check_wraparound_failsafe(vacrel: &mut LVRelState) -> PgResult<bool> {
    /* Don't warn more than once per VACUUM. */
    if vl::vacuum_failsafe_active::call()? {
        return Ok(true);
    }

    if vl::vacuum_xid_failsafe_check::call(vacrel.cutoffs)? {
        vl::set_vacuum_failsafe_active::call(true)?;

        /*
         * Abandon use of a buffer access strategy to allow use of all of shared
         * buffers.
         */
        vacrel.bstrategy = types_vacuum::vacuumlazy::StrategyHandle::none();

        /* Disable index vacuuming, index cleanup, and heap rel truncation. */
        vacrel.do_index_vacuuming = false;
        vacrel.do_index_cleanup = false;
        vacrel.do_rel_truncate = false;

        /* Reset the progress counters. */
        vl::pgstat_progress_update_multi_param::call(
            vec![PROGRESS_VACUUM_INDEXES_TOTAL, PROGRESS_VACUUM_INDEXES_PROCESSED],
            vec![0, 0],
        )?;

        ereport(WARNING)
            .errmsg(format!(
                "bypassing nonessential maintenance of table \"{}.{}.{}\" as a failsafe after {} index scans",
                vacrel.dbname, vacrel.relnamespace, vacrel.relname, vacrel.num_index_scans
            ))
            .errdetail("The table's relfrozenxid or relminmxid is too far in the past.")
            .errhint(
                "Consider increasing configuration parameter \"maintenance_work_mem\" or \"autovacuum_work_mem\".\n\
                 You might also need to consider other ways for VACUUM to keep up with the allocation of transaction IDs.",
            )
            .finish(here("lazy_check_wraparound_failsafe"))
            .ok();

        /* Stop applying cost limits from this point on. */
        vl::set_vacuum_cost_active::call(false)?;
        vl::set_vacuum_cost_balance::call(0)?;

        return Ok(true);
    }

    Ok(false)
}
