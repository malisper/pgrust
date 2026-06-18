//! Index vacuuming, cleanup, and per-index relstats (`vacuumlazy.c`).
//!
//!   * [`lazy_cleanup_all_indexes`] (vacuumlazy.c:3003).
//!   * [`lazy_vacuum_one_index`] (vacuumlazy.c:3071).
//!   * [`lazy_cleanup_one_index`] (vacuumlazy.c:3120).
//!   * [`update_relstats_all_indexes`] (vacuumlazy.c:3723).

use types_error::DEBUG2;
use types_vacuum::vacuumparallel::{IndexBulkDeleteResult, IndexVacuumInfo};
use types_error::PgResult;

use crate::consts::{
    InvalidMultiXactId, InvalidTransactionId,
    PROGRESS_VACUUM_INDEXES_PROCESSED, PROGRESS_VACUUM_INDEXES_TOTAL, PROGRESS_VACUUM_PHASE,
    PROGRESS_VACUUM_PHASE_INDEX_CLEANUP,
};
use crate::core::{parallel_vacuum_is_active, LVRelState, LVSavedErrInfo, VacErrPhase};
use crate::errcb::{restore_vacuum_error_info, update_vacuum_error_info};

use backend_access_heap_vacuumlazy_seams as vl;

/// `lazy_cleanup_all_indexes()` (vacuumlazy.c:3003) — perform index cleanup
/// across every index (serially or via parallel vacuum).
pub fn lazy_cleanup_all_indexes<'mcx>(vacrel: &mut LVRelState<'mcx>) -> PgResult<()> {
    let reltuples = vacrel.new_rel_tuples;
    let estimated_count = vacrel.scanned_pages < vacrel.rel_pages;

    debug_assert!(vacrel.do_index_cleanup);
    debug_assert!(vacrel.nindexes > 0);

    /* Report that we are now cleaning up indexes and how many. */
    vl::pgstat_progress_update_multi_param::call(
        vec![PROGRESS_VACUUM_PHASE, PROGRESS_VACUUM_INDEXES_TOTAL],
        vec![PROGRESS_VACUUM_PHASE_INDEX_CLEANUP, vacrel.nindexes as i64],
    )?;

    if !parallel_vacuum_is_active(vacrel) {
        for idx in 0..vacrel.nindexes as usize {
            let indrel = vacrel.indrels[idx].alias();
            let istat = vacrel.indstats[idx];

            let new_istat =
                lazy_cleanup_one_index(&indrel, istat, reltuples, estimated_count, vacrel)?;
            vacrel.indstats[idx] = new_istat;

            vl::pgstat_progress_update_param::call(
                PROGRESS_VACUUM_INDEXES_PROCESSED,
                (idx + 1) as i64,
            )?;
        }
    } else {
        vl::parallel_vacuum_cleanup_all_indexes::call(
            vacrel.pvs,
            reltuples,
            vacrel.num_index_scans,
            estimated_count,
        )?;
    }

    /* Reset the progress counters. */
    vl::pgstat_progress_update_multi_param::call(
        vec![PROGRESS_VACUUM_INDEXES_TOTAL, PROGRESS_VACUUM_INDEXES_PROCESSED],
        vec![0, 0],
    )?;
    Ok(())
}

/// `lazy_vacuum_one_index()` (vacuumlazy.c:3071) — run `vac_bulkdel_one_index`
/// for a single index, returning its updated bulk-delete stats.
pub fn lazy_vacuum_one_index<'mcx>(
    indrel: &types_rel::Relation<'mcx>,
    istat: Option<IndexBulkDeleteResult>,
    reltuples: f64,
    vacrel: &mut LVRelState<'mcx>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    let ivinfo = IndexVacuumInfo {
        index: indrel.rd_id,
        heaprel: vacrel.rel.rd_id,
        analyze_only: false,
        report_progress: false,
        estimated_count: true,
        message_level: DEBUG2.0,
        num_heap_tuples: reltuples,
        strategy: strategy_to_types(vacrel.bstrategy),
    };

    /* The index name is saved during this phase and restored after. */
    debug_assert!(vacrel.indname.is_none());
    vacrel.indname = Some(vl::relation_get_relation_name::call(indrel)?);
    let mut saved_err_info = LVSavedErrInfo {
        blkno: 0,
        offnum: 0,
        phase: VacErrPhase::Unknown,
    };
    update_vacuum_error_info(
        vacrel,
        Some(&mut saved_err_info),
        VacErrPhase::VacuumIndex, // re-signed seam takes &Relation
        crate::consts::InvalidBlockNumber,
        crate::consts::InvalidOffsetNumber,
    );

    /* Do bulk deletion. */
    let new_istat = Some(vl::vac_bulkdel_one_index::call(
        ivinfo,
        istat,
        vacrel.dead_items,
        vacrel.dead_items_info,
    )?);

    /* Revert to the previous phase information for error traceback. */
    restore_vacuum_error_info(vacrel, &saved_err_info);
    vacrel.indname = None;

    Ok(new_istat)
}

/// `lazy_cleanup_one_index()` (vacuumlazy.c:3120) — run `vac_cleanup_one_index`
/// for a single index, returning its updated cleanup stats.
pub fn lazy_cleanup_one_index<'mcx>(
    indrel: &types_rel::Relation<'mcx>,
    istat: Option<IndexBulkDeleteResult>,
    reltuples: f64,
    estimated_count: bool,
    vacrel: &mut LVRelState<'mcx>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    let ivinfo = IndexVacuumInfo {
        index: indrel.rd_id,
        heaprel: vacrel.rel.rd_id,
        analyze_only: false,
        report_progress: false,
        estimated_count,
        message_level: DEBUG2.0,
        num_heap_tuples: reltuples,
        strategy: strategy_to_types(vacrel.bstrategy),
    };

    debug_assert!(vacrel.indname.is_none());
    vacrel.indname = Some(vl::relation_get_relation_name::call(indrel)?);
    let mut saved_err_info = LVSavedErrInfo {
        blkno: 0,
        offnum: 0,
        phase: VacErrPhase::Unknown,
    };
    update_vacuum_error_info(
        vacrel,
        Some(&mut saved_err_info),
        VacErrPhase::IndexCleanup,
        crate::consts::InvalidBlockNumber,
        crate::consts::InvalidOffsetNumber,
    );

    let new_istat = vl::vac_cleanup_one_index::call(ivinfo, istat)?;

    restore_vacuum_error_info(vacrel, &saved_err_info);
    vacrel.indname = None;

    Ok(new_istat)
}

/// `update_relstats_all_indexes()` (vacuumlazy.c:3723) — update `pg_class`
/// relstats (relpages, reltuples) for every index using the recorded stats.
pub fn update_relstats_all_indexes<'mcx>(vacrel: &mut LVRelState<'mcx>) -> PgResult<()> {
    debug_assert!(vacrel.do_index_cleanup);

    for idx in 0..vacrel.nindexes as usize {
        let indrel = vacrel.indrels[idx].alias();
        let istat = vacrel.indstats[idx];

        let istat = match istat {
            Some(s) if !s.estimated_count => s,
            _ => continue,
        };

        vl::vac_update_relstats::call(types_vacuum::vacuumlazy::UpdateRelStatsArgs {
            relation: indrel.rd_id,
            num_pages: istat.num_pages,
            num_tuples: istat.num_index_tuples,
            num_all_visible_pages: 0,
            num_all_frozen_pages: 0,
            hasindex: false,
            frozenxid: InvalidTransactionId,
            minmulti: InvalidMultiXactId,
            in_outer_xact: false,
        })?;
    }
    Ok(())
}

#[inline]
fn strategy_to_types(
    h: types_vacuum::vacuumlazy::StrategyHandle,
) -> types_vacuum::vacuumparallel::BufferAccessStrategyHandle {
    types_vacuum::vacuumparallel::BufferAccessStrategyHandle(h.id)
}
