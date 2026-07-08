// execMain.c + execProcnode.c + nodeResult.c + execAmi.c minimal spine.
// Live: SELECT over Result/scan nodes with a real range table (RTE_RELATION/
// RTE_RESULT, SELECT-only perminfos); every other node type and lane is a
// loud panic naming the owning C file.
#![allow(non_snake_case)]

use std::cell::Cell;

use ::mcx::{Mcx, MemoryContext};
use ::types_error::PgResult;

mod epq;
mod execami;
mod execcurrent;
mod execmain;
mod execparallel;
mod nodegather;
mod nodegathermerge;
mod noderesult;
mod nodeprojectset;
mod nodesubplan;
mod procnode;
mod querydesc;
mod typefromtl;

#[cfg(test)]
mod tests;

pub use execami::{exec_re_scan, exec_re_scan_result, exec_supports_backward_scan};
pub use execmain::{
    exec_check_one_rel_perms, standard_executor_end, standard_executor_finish,
    standard_executor_run, standard_executor_start, tap_executor_end, tap_executor_finish,
    tap_executor_run, tap_executor_start,
};
pub use execparallel::{parallel_query_main, register_parallel_query_main};
pub use nodegather::GatherState;
pub use nodegathermerge::GatherMergeState;
pub use noderesult::ResultState;
pub use nodeprojectset::ProjectSetState;
pub use procnode::{
    exec_end_node, exec_init_node, exec_proc_node, exec_shutdown_node, PlanStateBase,
    PlanStateNode,
};
pub use querydesc::{registry_len, ExecData, ExecutorHandle, QueryDescData};
pub use typefromtl::{exec_clean_type_from_tl, exec_type_from_tl, expr_collation, expr_typmod};

pub fn init_seams() {
    execmain_seams::create_query_desc::set(querydesc::create_query_desc_seam);
    execmain_seams::free_query_desc::set(querydesc::free_query_desc_seam);
    execmain_seams::note_cplan_for_query_desc::set(querydesc::note_cplan_for_query_desc_seam);
    execmain_seams::release_query_desc::set(querydesc::release_query_desc_seam);
    execmain_seams::executor_start::set(execmain::executor_start_seam);
    execmain_seams::executor_run::set(execmain::executor_run_seam);
    execmain_seams::executor_finish::set(execmain::executor_finish_seam);
    execmain_seams::executor_finish_and_park::set(execmain::executor_finish_and_park_seam);
    execmain_seams::executor_rearm::set(execmain::executor_rearm_seam);
    execmain_seams::executor_rewind::set(execmain::executor_rewind_seam);
    execmain_seams::executor_end::set(execmain::executor_end_seam);
    execmain_seams::query_desc_es_processed::set(querydesc::query_desc_es_processed_seam);
    execmain_seams::query_desc_jit_instr::set(querydesc::query_desc_jit_instr_seam);
    execmain_seams::query_desc_snapshot::set(querydesc::query_desc_snapshot_seam);
    execmain_seams::query_desc_result_tupdesc::set(querydesc::query_desc_result_tupdesc_seam);
    execmain_seams::query_desc_operation::set(querydesc::query_desc_operation_seam);
    execmain_seams::query_desc_instrument::set(querydesc::query_desc_instrument_seam);
    execmain_seams::query_desc_prune_result::set(querydesc::query_desc_prune_result_seam);
    execmain_seams::query_desc_rti_unpruned::set(querydesc::query_desc_rti_unpruned_seam);
    execmain_seams::query_desc_agg_instrument::set(querydesc::query_desc_agg_instrument_seam);
    execmain_seams::query_desc_sort_instrument::set(querydesc::query_desc_sort_instrument_seam);
    execmain_seams::query_desc_incsort_instrument::set(
        querydesc::query_desc_incsort_instrument_seam,
    );
    execmain_seams::query_desc_hash_instrument::set(querydesc::query_desc_hash_instrument_seam);
    execmain_seams::query_desc_index_instrument::set(querydesc::query_desc_index_instrument_seam);
    execmain_seams::query_desc_tuplestore_instrument::set(
        querydesc::query_desc_tuplestore_instrument_seam,
    );
    execmain_seams::query_desc_memoize_instrument::set(
        querydesc::query_desc_memoize_instrument_seam,
    );
    execmain_seams::query_desc_bitmap_instrument::set(
        querydesc::query_desc_bitmap_instrument_seam,
    );
    execmain_seams::query_desc_index_searches::set(querydesc::query_desc_index_searches_seam);
    execmain_seams::exec_clean_type_from_tl::set(typefromtl::exec_clean_type_from_tl_seam);
    execmain_seams::exec_check_permissions::set(execmain::exec_check_permissions_over_perminfos);
    execmain_seams::exec_current_of::set(execcurrent::exec_current_of_seam);
    execmain_seams::query_desc_workers_launched::set(
        querydesc::query_desc_workers_launched_seam,
    );
    execmain_seams::query_desc_merge_instrument::set(
        querydesc::query_desc_merge_instrument_seam,
    );
    execmain_seams::query_desc_worker_instrument::set(
        querydesc::query_desc_worker_instrument_seam,
    );
    execmain_seams::query_desc_worker_sort_instrument::set(
        querydesc::query_desc_worker_sort_instrument_seam,
    );
    execmain_seams::query_desc_worker_bitmap_instrument::set(
        querydesc::query_desc_worker_bitmap_instrument_seam,
    );
    execmain_seams::query_desc_worker_incsort_instrument::set(
        querydesc::query_desc_worker_incsort_instrument_seam,
    );
    execparallel::register_parallel_query_main();
    {
        guc_tables::session_guc_bool!(PLP, parallel_leader_participation_stand_in, set_parallel_leader_participation_stand_in, true);
        guc_tables::vars::parallel_leader_participation.install_if_absent(
            guc_tables::GucVarAccessors {
                get: parallel_leader_participation_stand_in,
                set: set_parallel_leader_participation_stand_in,
            },
        );
    }
}

// Divergence from C: result tupdescs die in es_query_cxt there (execMain.c),
// with portals copying theirs before ExecutorEnd (portalcmds.c:354); here they
// are refcount-owned — the Rc strong count is the refcount (tupdesc.c model),
// the executor's references drop by ExecutorEnd, and a portal's clone keeps
// its descriptor alive. This backend-lifetime aset only backs those descs;
// TupleDescData's drop pfrees every byte, so the context stays flat per
// statement (desc_context_stays_flat_across_statements).
pub(crate) fn desc_mcx() -> Mcx<'static> {
    thread_local! {
        static CTX: Cell<Option<&'static MemoryContext>> = const { Cell::new(None) };
    }
    CTX.with(|c| {
        let m = match c.get() {
            Some(m) => m,
            None => {
                let m: &'static MemoryContext =
                    Box::leak(Box::new(MemoryContext::new("ExecutorResultTypes")));
                c.set(Some(m));
                m
            }
        };
        m.mcx()
    })
}

// C's CHECK_FOR_INTERRUPTS: inline flag test, cold out-of-line service.
#[inline(always)]
pub(crate) fn cfi() -> PgResult<()> {
    if init_small::globals::InterruptPending() {
        return cfi_slow();
    }
    Ok(())
}

#[cold]
#[inline(never)]
fn cfi_slow() -> PgResult<()> {
    postgres_seams::check_for_interrupts::call()
}
