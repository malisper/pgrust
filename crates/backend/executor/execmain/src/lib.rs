// execMain.c + execProcnode.c + nodeResult.c + execAmi.c minimal spine.
// Live: SELECT over Result/scan nodes with a real range table (RTE_RELATION/
// RTE_RESULT, SELECT-only perminfos); every other node type and lane is a
// loud panic naming the owning C file.
#![allow(non_snake_case)]

use std::cell::Cell;

use ::mcx::{Mcx, MemoryContext};
use ::types_error::PgResult;

/// De-monomorphization shim for the process-static `OnceLock<T>` env/config
/// gate cluster (execmain TLS/OnceLock bloat, ~5.4% of the crate's pre-opt
/// IR). Each `*CELL.get_or_init(|| ...)` call site otherwise instantiates a
/// fresh closure type `F`, forcing `OnceLock::<T>::get_or_init::<F>` /
/// `get_or_try_init` / `initialize` / `Once::call_once_force` to be
/// monomorphized once *per call site* (~132 copies for ~27 distinct `T`).
/// Routing every Copy-valued gate through this shim passes a *function
/// pointer* `fn() -> T` — one witness type shared by all call sites — so
/// those std helpers collapse to one copy per `T`.
///
/// Behaviour-identical by construction: the gate closures are all
/// non-capturing (a capturing closure cannot coerce to `fn() -> T`, so the
/// compiler rejects any accidental capture), the init runs exactly once with
/// the same value and the same ordering, and `T: Copy` matches the existing
/// deref-copy (`*cell.get_or_init(..)`) these sites already performed.
#[inline]
pub(crate) fn once_val<T: Copy>(cell: &'static std::sync::OnceLock<T>, init: fn() -> T) -> T {
    *cell.get_or_init(init)
}

mod epq;
mod execami;
mod execcurrent;
mod execmain;
mod execparallel;
mod lanev2;
mod nodegather;
mod nodegathermerge;
mod noderesult;
mod nodeprojectset;
mod nodesubplan;
mod procnode;
mod querydesc;
mod slease;
mod typefromtl;

#[cfg(test)]
mod tests;

pub use execami::{exec_re_scan, exec_re_scan_result, plan_implicit_scroll_ok};
pub use execmain::{
    exec_check_one_rel_perms, standard_executor_end, standard_executor_finish,
    standard_executor_run, standard_executor_start, tap_executor_end, tap_executor_finish,
    tap_executor_finish_leave, tap_executor_run, tap_executor_run_leave, tap_executor_start,
};
pub use execparallel::{parallel_query_main, register_parallel_query_main};
// Serial-lease v2 boot surface (GL-SLEASE-2): seams_init installs the armed
// wait-seam wrappers + the ProcessInterrupts admission tap through these.
pub use slease::{
    admission_tap as serial_lease_admission_tap, armed as serial_lease_armed,
    donation_enabled as serial_lease_donation_enabled,
    wait_hook_end as serial_lease_wait_hook_end, wait_hook_start as serial_lease_wait_hook_start,
};
pub use lanev2::coverage::{coverage_snapshot, LANEV2_BUILTINS, PGRUST_FOID_RANGE};
// WS-CB wave-10 (cursors inc-2 contract §8; worklog notes/se-wave10-cb.md
// EX-CB-1): the CA-facing portal seam — pquery must not link lanev2
// internals. Knob face for store arming (§7.3), the §6 assert-arming note,
// and the §3.3 tick face; coverage-export precedent above.
pub use lanev2::{
    ctas_funnel_engagements, cursor_store_armed_note, cursor_store_fill_enabled,
    cursor_store_fill_set_for_tests, funnel_engagements,
};
// GL-STMTTASK-1: the engagement counters (tests/diagnostics; the arming
// face lives in postgres_seams::stmt_task_arm — the seam-boundary crate).
pub use lanev2::stmt_task_engagements;
pub use nodegather::GatherState;
pub use nodegathermerge::GatherMergeState;
pub use noderesult::ResultState;
pub use nodeprojectset::ProjectSetState;
pub use procnode::{
    exec_end_node, exec_init_node, exec_proc_node, exec_shutdown_node, PlanStateBase,
    PlanStateNode,
};
pub use querydesc::{registry_len, with_qd, ExecData, ExecutorHandle, QueryDescData};
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
    execmain_seams::query_desc_runtime_ea_refusals::set(
        querydesc::query_desc_runtime_ea_refusals_seam,
    );
    execmain_seams::query_desc_runtime_ea_pipeline::set(
        querydesc::query_desc_runtime_ea_pipeline_seam,
    );
    execmain_seams::query_desc_engine_events::set(querydesc::query_desc_engine_events_seam);
    execmain_seams::query_desc_foreign_explain::set(querydesc::query_desc_foreign_explain_seam);
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
    // --- WS-CA wave-10 (cursors inc-2, contract §4; escalation EX-CA-1) ---
    execmain_seams::cursor_plan_current_of_eligible::set(
        execcurrent::cursor_plan_current_of_eligible_seam,
    );
    execmain_seams::cursor_capture_current::set(execcurrent::cursor_capture_current_seam);
    // --- end WS-CA wave-10 ---
    // --- SEAM-WIRING (SE10-GATES item 1): the EX-CB-1 faces, seam-installed
    // for the portal layer (production pquery links execmain_seams, not
    // execmain) — single knob cell, §6 assert arming, §3.3 tick face.
    execmain_seams::cursor_store_fill_enabled::set(lanev2::cursor_store_fill_enabled);
    execmain_seams::cursor_store_armed_note::set(lanev2::cursor_store_armed_note);
    // R1a: cursor_fill_tid_capture_refused (reason 41) retired with arm B.
    // --- end SEAM-WIRING ---
    // --- SE-R41 (reason-41 retirement): the §3.1 capture-batchable probe ---
    execmain_seams::cursor_plan_capture_batch_fill::set(
        execcurrent::cursor_plan_capture_batch_fill_seam,
    );
    // --- end SE-R41 ---
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
                    ::mcx::session_root("ExecutorResultTypes");
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
