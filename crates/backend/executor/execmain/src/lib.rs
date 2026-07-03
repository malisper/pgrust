// execMain.c + execProcnode.c + nodeResult.c + execAmi.c minimal spine.
// Live: SELECT over Result/scan nodes with a real range table (RTE_RELATION/
// RTE_RESULT, SELECT-only perminfos); every other node type and lane is a
// loud panic naming the owning C file.
#![allow(non_snake_case)]

use std::cell::Cell;

use ::mcx::{Mcx, MemoryContext};
use ::types_error::PgResult;

mod execami;
mod execmain;
mod noderesult;
mod procnode;
mod querydesc;
mod typefromtl;

#[cfg(test)]
mod tests;

pub use execami::{exec_re_scan, exec_re_scan_result, exec_supports_backward_scan};
pub use execmain::{
    standard_executor_end, standard_executor_finish, standard_executor_run,
    standard_executor_start,
};
pub use noderesult::ResultState;
pub use procnode::{
    exec_end_node, exec_init_node, exec_proc_node, exec_shutdown_node, PlanStateBase,
    PlanStateNode,
};
pub use querydesc::{registry_len, ExecData, ExecutorHandle, QueryDescData};
pub use typefromtl::{exec_clean_type_from_tl, exec_type_from_tl, expr_collation, expr_typmod};

pub fn init_seams() {
    execmain_seams::create_query_desc::set(querydesc::create_query_desc_seam);
    execmain_seams::free_query_desc::set(querydesc::free_query_desc_seam);
    execmain_seams::release_query_desc::set(querydesc::release_query_desc_seam);
    execmain_seams::executor_start::set(execmain::executor_start_seam);
    execmain_seams::executor_run::set(execmain::executor_run_seam);
    execmain_seams::executor_finish::set(execmain::executor_finish_seam);
    execmain_seams::executor_rewind::set(execmain::executor_rewind_seam);
    execmain_seams::executor_end::set(execmain::executor_end_seam);
    execmain_seams::query_desc_es_processed::set(querydesc::query_desc_es_processed_seam);
    execmain_seams::query_desc_snapshot::set(querydesc::query_desc_snapshot_seam);
    execmain_seams::query_desc_result_tupdesc::set(querydesc::query_desc_result_tupdesc_seam);
    execmain_seams::query_desc_operation::set(querydesc::query_desc_operation_seam);
    execmain_seams::exec_clean_type_from_tl::set(typefromtl::exec_clean_type_from_tl_seam);
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
