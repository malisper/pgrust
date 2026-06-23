//! Dead-item (TID store) management (`vacuumlazy.c`).
//!
//!   * [`dead_items_alloc`] (vacuumlazy.c:3473).
//!   * [`dead_items_add`] (vacuumlazy.c:3538).
//!   * [`dead_items_reset`] (vacuumlazy.c:3560).
//!   * [`dead_items_cleanup`] (vacuumlazy.c:3582).

use ::utils_error::{ereport};
use ::types_error::{ErrorLocation, DEBUG2, INFO, WARNING};
use ::types_core::{BlockNumber, OffsetNumber};
use ::types_error::PgResult;

use crate::consts::{
    PROGRESS_VACUUM_DEAD_TUPLE_BYTES, PROGRESS_VACUUM_NUM_DEAD_ITEM_IDS,
};
use crate::core::{parallel_vacuum_is_active, LVRelState};

use vacuumlazy_seams as vl;
use tidstore_seams as tidstore_seams;

fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("vacuumlazy.c", 0, funcname)
}

/// `dead_items_alloc()` (vacuumlazy.c:3473) — allocate (or set up parallel) the
/// `dead_items` TID store and `dead_items_info`, sized from the work-mem GUC.
pub fn dead_items_alloc<'mcx>(vacrel: &mut LVRelState<'mcx>, nworkers: i32) -> PgResult<()> {
    let vac_work_mem: i32 = if vl::am_autovacuum_worker_process::call()?
        && vl::autovacuum_work_mem::call()? != -1
    {
        vl::autovacuum_work_mem::call()?
    } else {
        vl::maintenance_work_mem::call()?
    };

    /*
     * Initialize state for a parallel vacuum.  Only invoke parallelism if there
     * are at least two indexes on a table.
     */
    if nworkers >= 0 && vacrel.nindexes > 1 && vacrel.do_index_vacuuming {
        if vl::relation_uses_local_buffers::call(&vacrel.rel)? {
            /* Cannot vacuum temporary tables in parallel. */
            if nworkers > 0 {
                ereport(WARNING)
                    .errmsg(format!(
                        "disabling parallel option of vacuum on \"{}\" --- cannot vacuum temporary tables in parallel",
                        vacrel.relname
                    ))
                    .finish(here("dead_items_alloc"))
                    .ok();
            }
        } else {
            let init = vl::parallel_vacuum_init::call(types_vacuum::vacuumlazy::ParallelVacuumInitArgs {
                rel: vacrel.rel.rd_id,
                indrels: vacrel.indrels.iter().map(|r| r.rd_id).collect(),
                nindexes: vacrel.nindexes,
                nrequested: nworkers,
                vac_work_mem,
                elevel: if vacrel.verbose { INFO.0 } else { DEBUG2.0 },
                bstrategy: vacrel.bstrategy.clone(),
            })?;
            vacrel.pvs = init.pvs;
            vacrel.dead_items = init.dead_items;
            vacrel.dead_items_info = init.dead_items_info;
        }

        /* If parallel mode started, dead_items / dead_items_info live in DSM. */
        if parallel_vacuum_is_active(vacrel) {
            let (ts, info) = vl::parallel_vacuum_get_dead_items::call(vacrel.pvs)?;
            vacrel.dead_items = ts;
            vacrel.dead_items_info = info;
            return Ok(());
        }
    }

    /* Serial VACUUM case. */
    vacrel.dead_items_info.max_bytes = vac_work_mem as usize * 1024usize;
    vacrel.dead_items_info.num_items = 0;

    vacrel.dead_items = tidstore_seams::tidstore_create_local::call(vacrel.dead_items_info.max_bytes, true)?;
    Ok(())
}

/// `dead_items_add()` (vacuumlazy.c:3538) — record the LP_DEAD offsets of one
/// heap block into the TID store and update the progress counters.
pub fn dead_items_add<'mcx>(
    vacrel: &mut LVRelState<'mcx>,
    blkno: BlockNumber,
    offsets: Vec<OffsetNumber>,
) -> PgResult<()> {
    let num_offsets = offsets.len() as i64;

    tidstore_seams::tidstore_set_block_offsets::call(vacrel.dead_items, blkno, offsets)?;
    vacrel.dead_items_info.num_items += num_offsets;

    /* update the progress information */
    let prog_index = vec![PROGRESS_VACUUM_NUM_DEAD_ITEM_IDS, PROGRESS_VACUUM_DEAD_TUPLE_BYTES];
    let prog_val = vec![
        vacrel.dead_items_info.num_items,
        tidstore_seams::tidstore_memory_usage::call(vacrel.dead_items)? as i64,
    ];
    vl::pgstat_progress_update_multi_param::call(prog_index, prog_val)?;
    Ok(())
}

/// `dead_items_reset()` (vacuumlazy.c:3560) — forget all collected dead items so
/// phase I can resume after an intermediate index/heap vacuuming pass.
pub fn dead_items_reset<'mcx>(vacrel: &mut LVRelState<'mcx>) -> PgResult<()> {
    if parallel_vacuum_is_active(vacrel) {
        vl::parallel_vacuum_reset_dead_items::call(vacrel.pvs)?;
        let (ts, info) = vl::parallel_vacuum_get_dead_items::call(vacrel.pvs)?;
        vacrel.dead_items = ts;
        vacrel.dead_items_info = info;
        return Ok(());
    }

    /* Recreate the tidstore with the same max_bytes limitation. */
    tidstore_seams::tidstore_destroy::call(vacrel.dead_items)?;
    vacrel.dead_items = tidstore_seams::tidstore_create_local::call(vacrel.dead_items_info.max_bytes, true)?;

    /* Reset the counter. */
    vacrel.dead_items_info.num_items = 0;
    Ok(())
}

/// `dead_items_cleanup()` (vacuumlazy.c:3582) — perform cleanup for resources
/// allocated in [`dead_items_alloc`]; in the parallel case, tear down parallel
/// vacuum state.
pub fn dead_items_cleanup<'mcx>(vacrel: &mut LVRelState<'mcx>) -> PgResult<()> {
    if !parallel_vacuum_is_active(vacrel) {
        /* Don't bother with pfree here. */
        return Ok(());
    }

    /* End parallel mode. C copies the per-index stats into vacrel->indstats
     * here; the seam returns them so we can store them. */
    vacrel.indstats = vl::parallel_vacuum_end::call(vacrel.pvs)?;
    vacrel.pvs = types_vacuum::vacuumlazy::ParallelVacuumStateHandle::none();
    Ok(())
}
