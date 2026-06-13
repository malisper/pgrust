//! Port of `src/backend/executor/execUtils.c` — miscellaneous executor
//! utility routines.
//!
//! INTERFACE ROUTINES
//! - [`CreateExecutorState`] / [`FreeExecutorState`] — executor working state
//! - [`CreateExprContext`] / [`CreateStandaloneExprContext`] /
//!   [`FreeExprContext`] / [`ReScanExprContext`]
//! - [`ExecAssignExprContext`] etc. — common plan-node init support
//! - [`ExecOpenScanRelation`] — common scan-node init support
//! - [`ExecInitRangeTable`] / [`ExecGetRangeTableRelation`]
//! - [`executor_errposition`]
//! - [`RegisterExprContextCallback`] / [`UnregisterExprContextCallback`]
//! - [`GetAttributeByName`] / [`GetAttributeByNum`]
//!
//! ## Owned-model translation notes
//!
//! - The `EState` is an [`McxOwned`] bundle: the per-query "ExecutorState"
//!   context and the state allocated in it move as one value
//!   ([`ExecutorState`]); `FreeExecutorState` consumes it (the C
//!   `MemoryContextDelete(es_query_cxt)` is the drop).
//! - `ExprContext *` / `ResultRelInfo *` are pool ids ([`EcxtId`]/[`RriId`])
//!   into EState-owned pools — see `types-nodes::execnodes` for why.
//! - `Relation` crosses as a [`types_rel::Relation`] handle: `es_relations`
//!   owns the opens (released at EState teardown or abort-path drop);
//!   `ri_RelationDesc` and returned relations are aliases of those handles.
//! - There is no ambient `CurrentMemoryContext`: C call sites that
//!   `MemoryContextSwitchTo` translate to explicit `Mcx` threading
//!   (docs/mctx-design.md). Functions whose C result is allocated in the
//!   per-tuple context ([`ExecGetInsertedCols`] and friends) take the target
//!   context handle explicitly instead.
//! - C functions returning borrowed `Bitmapset *`s from the perm-info lists
//!   return copies allocated in the caller-supplied context (the owned tree
//!   cannot lend across the later `&mut EStateData` uses).
//!
//! Calls into unported owners (execTuples.c, execExpr.c, execMain.c,
//! nodeModifyTable.c, bitmapset.c, attmap.c/tupconvert.c, relation.c,
//! parse_relation.c, partdesc.c, lmgr.c, typcache.c, mbutils.c, jit.c) go
//! through those owners' seam crates and panic until the owners land.
//! Ported neighbors (table.c, tableam.c) are direct dependencies. Per-backend
//! globals of unported owners (`work_mem`, `IsParallelWorker`,
//! `CurrentUserId`) are explicit parameters — callers read them off their
//! facet/state when the owners land.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// UnregisterExprContextCallback mirrors the C removal-by-(fn, arg) identity;
// see DESIGN_DEBT.md (fn items can merge/duplicate across codegen units).
#![allow(unpredictable_function_pointer_comparisons)]

use backend_access_common_heaptuple::{
    getmissingattr, heap_attisnull, heap_getsysattr, nocachegetattr,
};
use backend_access_common_next_seams as tupconvert_seams;
use backend_access_table_table as table;
use backend_access_table_tableam as tableam;
use backend_executor_execExpr_seams as execExpr_seams;
use backend_executor_execMain_seams as execMain_seams;
use backend_executor_execTuples_seams as execTuples_seams;
use backend_executor_nodeModifyTable_seams as modifytable_seams;
use backend_jit_jit_seams as jit_seams;
use backend_nodes_core_seams as bms_seams;
use backend_parser_relation_seams as parse_relation_seams;
use backend_partitioning_core_seams as partdesc_seams;
use backend_storage_lmgr_lmgr_seams as lmgr_seams;
use backend_utils_cache_typcache_seams as typcache_seams;
use backend_utils_mb_mbutils_seams as mbutils_seams;

use mcx::{alloc_in, vec_with_capacity_in, Mcx, McxOwned, MemoryContext, PgBox, PgVec};
use types_core::primitive::{AttrNumber, Index, InvalidAttrNumber, InvalidOid, Oid};
use types_core::PgResult;
use types_datum::Datum;
use types_error::{PgError, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE};
use types_nodes::bitmapset::Bitmapset;
use types_nodes::execnodes::{
    EStateData, EcxtId, ExprContext, ExprContextCallbackFunction, ExprContext_CB, RriId,
};
use types_nodes::nodes::CMD_UPDATE;
use types_rel::Relation;
use types_nodes::parsenodes::{RangeTblEntry, RTEPermissionInfo, RTE_RELATION};
use types_nodes::primnodes::{Expr, TargetEntry};
use types_nodes::{PlanStateData, ScanStateData, SlotId, TupleSlotKind};
use types_tuple::access::{AccessShareLock, NoLock};
use types_tuple::backend_access_common_heaptuple::{DeformedColumn, FormedTuple, TupleValue};
use types_tuple::heaptuple::{
    HeapTupleData, HeapTupleHeaderGetDatumLength, HeapTupleHeaderGetNatts,
    HeapTupleHeaderGetTypMod, HeapTupleHeaderGetTypeId, ItemPointerData, TupleDescData,
    HEAP_HASNULL,
};

// ===========================================================================
// Constants (executor.h / memutils.h / pg_bitutils.h)
// ===========================================================================

/// `EXEC_FLAG_EXPLAIN_ONLY` (executor.h).
pub const EXEC_FLAG_EXPLAIN_ONLY: i32 = 0x0001;
/// `EXEC_FLAG_WITH_NO_DATA` (executor.h).
pub const EXEC_FLAG_WITH_NO_DATA: i32 = 0x0040;

/// `ALLOCSET_DEFAULT_MINSIZE` (memutils.h).
pub const ALLOCSET_DEFAULT_MINSIZE: usize = 0;
/// `ALLOCSET_DEFAULT_INITSIZE` (memutils.h).
pub const ALLOCSET_DEFAULT_INITSIZE: usize = 8 * 1024;
/// `ALLOCSET_DEFAULT_MAXSIZE` (memutils.h).
pub const ALLOCSET_DEFAULT_MAXSIZE: usize = 8 * 1024 * 1024;

/// `pg_prevpower2_size_t(num)` (pg_bitutils.h): the largest power of two less
/// than or equal to `num`. The C is undefined for 0 (`pg_leftmost_one_pos`
/// asserts non-zero); mirrored as a debug assertion. Private: the helper
/// belongs to port/pg_bitutils.h, not this unit's interface — it moves to
/// the common bit-utils home when one exists.
#[inline]
fn pg_prevpower2_size_t(num: usize) -> usize {
    debug_assert!(num > 0, "pg_prevpower2_size_t is undefined for 0");
    if num == 0 {
        return 0;
    }
    1usize << (usize::BITS - 1 - num.leading_zeros())
}

// ===========================================================================
// Seam installation
// ===========================================================================

/// Install this crate's implementations into its seam slots
/// (`crates/backend-executor-execUtils-seams`).
pub fn init_seams() {
    backend_executor_execUtils_seams::exec_create_scan_slot_from_outer_plan::set(
        ExecCreateScanSlotFromOuterPlan,
    );
}

// ===========================================================================
//				 Executor state and memory management functions
// ===========================================================================

mcx::bind!(pub EStateTy => EStateData<'mcx>);

/// The `EState` together with its per-query "ExecutorState" memory context,
/// movable as one value (the C `EState *` whose node lives inside
/// `es_query_cxt`).
pub type ExecutorState = McxOwned<EStateTy>;

/// `CreateExecutorState` — create and initialize an EState node, the root of
/// working storage for an entire Executor invocation.
///
/// Principally, this creates the per-query memory context that will be used to
/// hold all working data that lives till the end of the query. The per-query
/// context becomes an (accounting) child of `parent` — the C
/// `CurrentMemoryContext` made explicit.
pub fn CreateExecutorState(parent: &MemoryContext) -> PgResult<ExecutorState> {
    // qcontext = AllocSetContextCreate(CurrentMemoryContext, "ExecutorState",
    //                                  ALLOCSET_DEFAULT_SIZES);
    // Make the EState node within the per-query context (no separate pfree at
    // shutdown — the bundle drops together) and initialize all fields.
    McxOwned::try_new(parent.new_child("ExecutorState"), |mcx| {
        Ok(EStateData::new_in(mcx))
    })
}

/// `FreeExecutorState` — release an EState along with all remaining working
/// storage.
///
/// Not responsible for releasing non-memory resources such as open relations
/// or buffer pins, but shuts down any still-active ExprContexts (running
/// their shutdown callbacks) and deallocates JITed expressions — sufficient
/// cleanup where the EState was only used for expression evaluation.
///
/// Fallible: the shutdown callbacks run in ereport-capable code (in C an
/// error here escapes mid-teardown). The per-query context is freed either
/// way (the consumed bundle drops).
pub fn FreeExecutorState(mut estate: ExecutorState) -> PgResult<()> {
    let result = estate.with_mut(|estate| {
        // Shut down and free any remaining ExprContexts, so any remaining
        // shutdown callbacks get called. C: `while (estate->es_exprcontexts)
        // FreeExprContext(linitial(...), true)` — newest first (lcons order);
        // the pool equivalent is highest index first.
        for i in (0..estate.es_exprcontexts.len()).rev() {
            if estate.es_exprcontexts[i].is_some() {
                FreeExprContext(estate, EcxtId(i as u32), true)?;
            }
        }

        // Release JIT context, if allocated.
        if let Some(jit) = estate.es_jit.0.take() {
            jit_seams::jit_release_context::call(jit);
            // estate->es_jit = NULL (the take() above)
        }

        // Release partition directory, if allocated.
        if let Some(pdir) = estate.es_partition_directory.0.take() {
            partdesc_seams::destroy_partition_directory::call(pdir);
        }
        Ok(())
    });

    // Free the per-query memory context, thereby releasing all working
    // memory, including the EState node itself.
    drop(estate);
    result
}

/// Internal implementation for `CreateExprContext()` and
/// `CreateWorkExprContext()` that allows control over the AllocSet
/// parameters.
///
/// The mcx backend has no per-context block-size knobs (the sanctioned
/// divergence table in docs/mctx-design.md), so the size parameters are
/// computed for parity but do not influence the child context.
fn CreateExprContextInternal<'mcx>(
    estate: &mut EStateData<'mcx>,
    _min_context_size: usize,
    _init_block_size: usize,
    _max_block_size: usize,
) -> PgResult<EcxtId> {
    // Create the ExprContext node within the per-query memory context.
    let per_query = estate.es_query_cxt;

    let econtext = ExprContext {
        ecxt_scantuple: None,
        ecxt_innertuple: None,
        ecxt_outertuple: None,
        ecxt_per_query_memory: per_query,
        // Create working memory for expression evaluation in this context:
        // AllocSetContextCreate(estate->es_query_cxt, "ExprContext", sizes).
        ecxt_per_tuple_memory: per_query.context().new_child("ExprContext"),
        // ecxt_param_exec_vals / ecxt_param_list_info: the C copies the
        // EState's pointers; the owned ExprContext does not carry the aliases
        // (readers take them from the explicitly threaded EState).
        ecxt_aggvalues: PgVec::new_in(per_query),
        ecxt_aggnulls: PgVec::new_in(per_query),
        caseValue_datum: Datum::null(),
        caseValue_isNull: true,
        domainValue_datum: Datum::null(),
        domainValue_isNull: true,
        // ecxt_estate: the owned model threads the EState explicitly.
        ecxt_callbacks: None,
    };

    // Link the ExprContext into the EState to ensure it is shut down when the
    // EState is freed. C uses lcons() so shutdowns occur in reverse order of
    // creation; the pool preserves that by iterating highest-index first in
    // FreeExecutorState.
    estate.add_expr_context(econtext)
}

/// `CreateExprContext` — create a context for expression evaluation within an
/// EState.
///
/// An executor run may require multiple ExprContexts (usually one per Plan
/// node, and a separate one for per-output-tuple processing such as
/// constraint checking). Each has its own "per-tuple" memory context.
///
/// Note we make no assumption about the caller's memory context.
pub fn CreateExprContext(estate: &mut EStateData<'_>) -> PgResult<EcxtId> {
    CreateExprContextInternal(
        estate,
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
    )
}

/// `CreateWorkExprContext` — like [`CreateExprContext`], but specifies the
/// AllocSet sizes to be reasonable in proportion to `work_mem`. If the
/// maximum block allocation size is too large, it's easy to skip right past
/// `work_mem` with a single allocation.
///
/// `work_mem_kb` is the C `work_mem` GUC (globals.c, in KB) — an explicit
/// parameter, not an ambient global; the caller reads it off its own
/// facet/state when the GUC owner lands.
pub fn CreateWorkExprContext(estate: &mut EStateData<'_>, work_mem_kb: i32) -> PgResult<EcxtId> {
    let mut max_block_size = pg_prevpower2_size_t(work_mem_kb as usize * 1024 / 16);

    // But no bigger than ALLOCSET_DEFAULT_MAXSIZE
    max_block_size = core::cmp::min(max_block_size, ALLOCSET_DEFAULT_MAXSIZE);

    // and no smaller than ALLOCSET_DEFAULT_INITSIZE
    max_block_size = core::cmp::max(max_block_size, ALLOCSET_DEFAULT_INITSIZE);

    CreateExprContextInternal(
        estate,
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        max_block_size,
    )
}

/// `CreateStandaloneExprContext` — create a context for standalone expression
/// evaluation (expressions containing no Params, subplans, or Var
/// references).
///
/// The ExprContext value lives in the caller's context `mcx` (the C
/// `CurrentMemoryContext`), which also becomes its "per query" context. It is
/// the caller's responsibility to free the ExprContext when done, or at least
/// ensure that any shutdown callbacks have been called
/// ([`ReScanExprContext`] is suitable) — otherwise non-memory resources might
/// be leaked.
pub fn CreateStandaloneExprContext<'mcx>(mcx: Mcx<'mcx>) -> PgResult<ExprContext<'mcx>> {
    Ok(ExprContext {
        ecxt_scantuple: None,
        ecxt_innertuple: None,
        ecxt_outertuple: None,
        ecxt_per_query_memory: mcx,
        // Create working memory for expression evaluation in this context.
        ecxt_per_tuple_memory: mcx.context().new_child("ExprContext"),
        ecxt_aggvalues: PgVec::new_in(mcx),
        ecxt_aggnulls: PgVec::new_in(mcx),
        caseValue_datum: Datum::null(),
        caseValue_isNull: true,
        domainValue_datum: Datum::null(),
        domainValue_isNull: true,
        // ecxt_estate = NULL: a standalone context has no owning EState.
        ecxt_callbacks: None,
    })
}

/// `FreeExprContext` — free an EState-owned expression context, including
/// calling any remaining shutdown callbacks.
///
/// Since we free the temporary context used for expression evaluation, any
/// previously computed pass-by-reference expression result will go away!
///
/// If `is_commit` is false, we are being called in error cleanup, and should
/// not call callbacks but only release memory.
pub fn FreeExprContext(
    estate: &mut EStateData<'_>,
    econtext: EcxtId,
    is_commit: bool,
) -> PgResult<()> {
    // Call any registered callbacks.
    ShutdownExprContext(estate.ecxt_mut(econtext), is_commit)?;
    // And clean up the memory used (MemoryContextDelete of the per-tuple
    // context), unlink self from the owning EState (list_delete_ptr), and
    // delete the ExprContext node (pfree): dropping the pool entry is all
    // three; the tombstone keeps other EcxtIds stable.
    estate.es_exprcontexts[econtext.0 as usize] = None;
    Ok(())
}

/// [`FreeExprContext`] for a standalone context (the C `FreeExprContext` with
/// `econtext->ecxt_estate == NULL`): consumes the value.
pub fn FreeStandaloneExprContext(mut econtext: ExprContext<'_>, is_commit: bool) -> PgResult<()> {
    // Call any registered callbacks.
    ShutdownExprContext(&mut econtext, is_commit)
    // And clean up the memory used / delete the node: the drop.
}

/// `ReScanExprContext` — reset an expression context in preparation for a
/// rescan of its plan node. This requires calling any registered shutdown
/// callbacks, since any partially complete set-returning-functions must be
/// canceled.
pub fn ReScanExprContext(econtext: &mut ExprContext<'_>) -> PgResult<()> {
    // Call any registered callbacks
    ShutdownExprContext(econtext, true)?;
    // And clean up the memory used
    econtext.ecxt_per_tuple_memory.reset();
    Ok(())
}

/// `ResetExprContext(econtext)` (executor.h macro):
/// `MemoryContextReset(econtext->ecxt_per_tuple_memory)`. Does NOT run the
/// shutdown callbacks (that is [`ReScanExprContext`]'s job).
pub fn ResetExprContext(econtext: &mut ExprContext<'_>) {
    econtext.ecxt_per_tuple_memory.reset();
}

/// `MakePerTupleExprContext` — build a per-output-tuple ExprContext for an
/// EState, lazily (normally invoked via the `GetPerTupleExprContext()` macro
/// in C). Returns the context's id.
pub fn MakePerTupleExprContext(estate: &mut EStateData<'_>) -> PgResult<EcxtId> {
    if estate.es_per_tuple_exprcontext.is_none() {
        let id = CreateExprContext(estate)?;
        estate.es_per_tuple_exprcontext = Some(id);
    }
    Ok(estate
        .es_per_tuple_exprcontext
        .expect("just initialized above"))
}

/// `ResetPerTupleExprContext(estate)` (executor.h macro): reset the
/// per-output-tuple context, if it exists.
pub fn ResetPerTupleExprContext(estate: &mut EStateData<'_>) {
    if let Some(id) = estate.es_per_tuple_exprcontext {
        ResetExprContext(estate.ecxt_mut(id));
    }
}

// ===========================================================================
//				 miscellaneous node-init support functions
// ===========================================================================

/// `ExecAssignExprContext` — initialize the `ps_ExprContext` field. Only
/// necessary for nodes which use `ExecQual` or `ExecProject`, because those
/// routines require an econtext.
pub fn ExecAssignExprContext(
    estate: &mut EStateData<'_>,
    planstate: &mut PlanStateData<'_>,
) -> PgResult<()> {
    planstate.ps_ExprContext = Some(CreateExprContext(estate)?);
    Ok(())
}

/// `ExecGetResultType` — the node's result tuple descriptor
/// (`planstate->ps_ResultTupleDesc`).
pub fn ExecGetResultType<'a, 'mcx>(
    planstate: &'a PlanStateData<'mcx>,
) -> Option<&'a TupleDescData<'mcx>> {
    planstate.ps_ResultTupleDesc.as_deref()
}

/// `ExecGetResultSlotOps` — information about the node's type of result slot.
///
/// C compares/returns `const TupleTableSlotOps *` singleton pointers; the
/// owned model carries the identity as the [`TupleSlotKind`] token. The slot
/// pool lives in the EState, so resolving `ps_ResultTupleSlot->tts_ops` takes
/// the (explicitly threaded) `estate`.
pub fn ExecGetResultSlotOps(
    planstate: &PlanStateData<'_>,
    estate: &EStateData<'_>,
    isfixed: Option<&mut bool>,
) -> TupleSlotKind {
    if planstate.resultopsset && planstate.resultops.is_some() {
        if let Some(isfixed) = isfixed {
            *isfixed = planstate.resultopsfixed;
        }
        return planstate.resultops.expect("checked is_some above");
    }

    if let Some(isfixed) = isfixed {
        if planstate.resultopsset {
            *isfixed = planstate.resultopsfixed;
        } else if let Some(slot) = planstate.ps_ResultTupleSlot {
            *isfixed = estate.slot(slot).is_fixed();
        } else {
            *isfixed = false;
        }
    }

    match planstate.ps_ResultTupleSlot {
        None => TupleSlotKind::Virtual,
        Some(slot) => estate.slot(slot).tts_ops,
    }
}

/// `ExecGetCommonSlotOps` — identify common result slot type, if any.
///
/// If all the given PlanState nodes return the same fixed tuple slot type,
/// return that slot type's ops token. Else, return `None` (the C NULL).
pub fn ExecGetCommonSlotOps(
    planstates: &[&PlanStateData<'_>],
    estate: &EStateData<'_>,
) -> Option<TupleSlotKind> {
    if planstates.is_empty() {
        return None;
    }
    let mut isfixed = false;
    let result = ExecGetResultSlotOps(planstates[0], estate, Some(&mut isfixed));
    if !isfixed {
        return None;
    }
    for ps in &planstates[1..] {
        let thisops = ExecGetResultSlotOps(ps, estate, Some(&mut isfixed));
        if !isfixed {
            return None;
        }
        if result != thisops {
            return None;
        }
    }
    Some(result)
}

/// `ExecGetCommonChildSlotOps` — as [`ExecGetCommonSlotOps`], for the
/// PlanState's standard children (`outerPlanState` then `innerPlanState`;
/// the C dereferences both unguarded, so a missing child is a loud panic).
pub fn ExecGetCommonChildSlotOps(
    ps: &PlanStateData<'_>,
    estate: &EStateData<'_>,
) -> Option<TupleSlotKind> {
    let outer = ps
        .lefttree
        .as_deref()
        .expect("ExecGetCommonChildSlotOps: outerPlanState is NULL")
        .ps_head();
    let inner = ps
        .righttree
        .as_deref()
        .expect("ExecGetCommonChildSlotOps: innerPlanState is NULL")
        .ps_head();
    ExecGetCommonSlotOps(&[outer, inner], estate)
}

/// `ExecAssignProjectionInfo` — forms the projection information from the
/// node's targetlist.
///
/// Notes for `input_desc` are same as for `ExecBuildProjectionInfo`: supply
/// it for a relation-scan node, pass `None` for upper-level nodes.
pub fn ExecAssignProjectionInfo<'mcx>(
    planstate: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    input_desc: Option<&TupleDescData<'_>>,
) -> PgResult<()> {
    // ExecBuildProjectionInfo(planstate->plan->targetlist,
    //     planstate->ps_ExprContext, planstate->ps_ResultTupleSlot,
    //     planstate, inputDesc) — the seam takes the node and extracts those.
    let proj = execExpr_seams::exec_build_projection_info::call(planstate, estate, input_desc)?;
    planstate.ps_ProjInfo = Some(proj);
    Ok(())
}

/// `ExecConditionalAssignProjectionInfo` — as [`ExecAssignProjectionInfo`],
/// but store `None` rather than building projection info if no projection is
/// required.
pub fn ExecConditionalAssignProjectionInfo<'mcx>(
    planstate: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    input_desc: &TupleDescData<'_>,
    varno: i32,
) -> PgResult<()> {
    let matched = {
        let tlist = planstate
            .plan
            .as_deref()
            .expect("ExecConditionalAssignProjectionInfo: PlanState has no plan")
            .plan_head()
            .targetlist
            .as_deref()
            .unwrap_or(&[]);
        tlist_matches_tupdesc(tlist, varno, input_desc)
    };

    if matched {
        planstate.ps_ProjInfo = None;
        planstate.resultopsset = planstate.scanopsset;
        planstate.resultopsfixed = planstate.scanopsfixed;
        planstate.resultops = planstate.scanops;
    } else {
        if planstate.ps_ResultTupleSlot.is_none() {
            execTuples_seams::exec_init_result_slot::call(
                planstate,
                estate,
                TupleSlotKind::Virtual,
            )?;
            planstate.resultops = Some(TupleSlotKind::Virtual);
            planstate.resultopsfixed = true;
            planstate.resultopsset = true;
        }
        ExecAssignProjectionInfo(planstate, estate, Some(input_desc))?;
    }
    Ok(())
}

/// `tlist_matches_tupdesc` (static): does this plain-Var target list exactly
/// match the tuple descriptor? (The C also takes the PlanState and varno; the
/// PlanState parameter is unused there too.)
fn tlist_matches_tupdesc(
    tlist: &[TargetEntry<'_>],
    varno: i32,
    tupdesc: &TupleDescData<'_>,
) -> bool {
    let numattrs = tupdesc.natts;
    let mut tlist_item = tlist.iter();

    // Check the tlist attributes
    for attrno in 1..=numattrs {
        let att_tup = tupdesc.attr((attrno - 1) as usize);

        let Some(tle) = tlist_item.next() else {
            return false; // tlist too short
        };
        let var = match tle.expr.as_deref() {
            Some(Expr::Var(var)) => var,
            _ => return false, // tlist item not a Var
        };
        // if these Asserts fail, planner messed up
        debug_assert_eq!(var.varno, varno);
        debug_assert_eq!(var.varlevelsup, 0);
        if var.varattno as i32 != attrno {
            return false; // out of order
        }
        if att_tup.attisdropped {
            return false; // table contains dropped columns
        }
        if att_tup.atthasmissing {
            return false; // table contains cols with missing values
        }

        // Note: usually the Var's type should match the tupdesc exactly, but
        // in situations involving unions of columns that have different
        // typmods, the Var may have come from above the union and hence have
        // typmod -1. This is a legitimate situation since the Var still
        // describes the column, just not as exactly as the tupdesc does.
        if var.vartype != att_tup.atttypid
            || (var.vartypmod != att_tup.atttypmod && var.vartypmod != -1)
        {
            return false; // type mismatch
        }
    }

    if tlist_item.next().is_some() {
        return false; // tlist too long
    }

    true
}

// ===========================================================================
//				  Scan node support
// ===========================================================================

/// `ExecAssignScanType` — set the scan tuple slot's descriptor:
/// `ExecSetSlotDescriptor(scanstate->ss_ScanTupleSlot, tupDesc)`.
pub fn ExecAssignScanType<'mcx>(
    estate: &mut EStateData<'mcx>,
    scanstate: &ScanStateData<'mcx>,
    tup_desc: types_tuple::heaptuple::TupleDesc<'mcx>,
) -> PgResult<()> {
    let slot = scanstate
        .ss_ScanTupleSlot
        .expect("ExecAssignScanType: ss_ScanTupleSlot not initialized");
    execTuples_seams::exec_set_slot_descriptor::call(estate, slot, tup_desc)
}

/// `ExecCreateScanSlotFromOuterPlan` — set up the node's scan tuple slot
/// using the outer plan's result tuple type.
pub fn ExecCreateScanSlotFromOuterPlan<'mcx>(
    estate: &mut EStateData<'mcx>,
    scanstate: &mut ScanStateData<'mcx>,
    tts_ops: TupleSlotKind,
) -> PgResult<()> {
    // outerPlan = outerPlanState(scanstate);
    // tupDesc = ExecGetResultType(outerPlan);
    //
    // The C shares the outer node's descriptor pointer; the owned model
    // copies it into the per-query context before handing it to the slot.
    let mcx = estate.es_query_cxt;
    let tup_desc = {
        let outer_plan = scanstate
            .ps
            .lefttree
            .as_deref()
            .expect("ExecCreateScanSlotFromOuterPlan: outerPlanState is NULL")
            .ps_head();
        match ExecGetResultType(outer_plan) {
            Some(desc) => Some(alloc_in(mcx, desc.clone_in(mcx)?)?),
            None => None,
        }
    };

    execTuples_seams::exec_init_scan_tuple_slot::call(estate, scanstate, tup_desc, tts_ops)
}

/// `ExecRelationIsTargetRelation` — detect whether a relation (identified by
/// rangetable index) is one of the target relations of the query.
///
/// Note: this is currently no longer used in core. We keep it around because
/// FDWs may wish to use it to determine if their foreign table is a target
/// relation.
pub fn ExecRelationIsTargetRelation(estate: &EStateData<'_>, scanrelid: Index) -> bool {
    // list_member_int(estate->es_plannedstmt->resultRelations, scanrelid)
    estate
        .es_plannedstmt
        .as_deref()
        .expect("ExecRelationIsTargetRelation: EState has no PlannedStmt")
        .resultRelations
        .as_deref()
        .unwrap_or(&[])
        .contains(&(scanrelid as i32))
}

/// `ExecOpenScanRelation` — open the heap relation to be scanned by a
/// base-level scan plan node. This should be called during the node's
/// ExecInit routine. Returns an alias of the open relation (the handle is
/// owned by `es_relations`).
///
/// `is_parallel_worker` is the C `IsParallelWorker()` global (parallel.c) —
/// an explicit parameter, passed through to [`ExecGetRangeTableRelation`].
pub fn ExecOpenScanRelation<'mcx>(
    estate: &mut EStateData<'mcx>,
    scanrelid: Index,
    eflags: i32,
    is_parallel_worker: bool,
) -> PgResult<Relation<'mcx>> {
    // Open the relation.
    let rel = ExecGetRangeTableRelation(estate, scanrelid, false, is_parallel_worker)?;

    // Complain if we're attempting a scan of an unscannable relation, except
    // when the query won't actually be run. This is a slightly klugy place
    // to do this, perhaps, but there is no better place.
    if (eflags & (EXEC_FLAG_EXPLAIN_ONLY | EXEC_FLAG_WITH_NO_DATA)) == 0 && !rel.is_scannable() {
        let relname = rel.name();
        return Err(PgError::error(format!(
            "materialized view \"{relname}\" has not been populated"
        ))
        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        .with_hint("Use the REFRESH MATERIALIZED VIEW command."));
    }

    Ok(rel)
}

/// `ExecInitRangeTable` — set up executor's range-table-related data.
///
/// In addition to the range table proper, initialize arrays that are indexed
/// by rangetable index.
pub fn ExecInitRangeTable<'mcx>(
    estate: &mut EStateData<'mcx>,
    range_table: PgVec<'mcx, RangeTblEntry>,
    perm_infos: PgVec<'mcx, RTEPermissionInfo<'mcx>>,
    unpruned_relids: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
) -> PgResult<()> {
    // Remember the range table List as-is
    estate.es_range_table = range_table;

    // ... and the RTEPermissionInfo List too
    estate.es_rteperminfos = perm_infos;

    // Set size of associated arrays
    estate.es_range_table_size = estate.es_range_table.len();

    // Initialize the bitmapset of RT indexes (es_unpruned_relids)
    // representing relations that will be scanned during execution. This set
    // is initially populated by the caller and may be extended later by
    // ExecDoInitialPruning() to include RT indexes of unpruned leaf
    // partitions.
    estate.es_unpruned_relids = unpruned_relids;

    // Allocate an array to store an open Relation corresponding to each
    // rangetable entry, and initialize entries to NULL. Relations are opened
    // and stored here as needed.
    let mcx = estate.es_query_cxt;
    let mut relations = vec_with_capacity_in(mcx, estate.es_range_table_size)?;
    for _ in 0..estate.es_range_table_size {
        relations.push(None);
    }
    estate.es_relations = relations;

    // es_result_relations (and, in C, es_rowmarks — a field with no consumer
    // here yet) are also parallel to es_range_table, but are allocated only
    // if needed.
    estate.es_result_relations = PgVec::new_in(mcx);
    Ok(())
}

/// `exec_rt_fetch(rti, estate)` (executor.h inline):
/// `list_nth(estate->es_range_table, rti - 1)`.
pub fn exec_rt_fetch<'a>(rti: Index, estate: &'a EStateData<'_>) -> &'a RangeTblEntry {
    &estate.es_range_table[(rti - 1) as usize]
}

/// `ExecGetRangeTableRelation` — open the Relation for a range table entry,
/// if not already done. The Relations will be closed in `ExecEndPlan()`.
///
/// If `is_result_rel` is true, the relation is being used as a result
/// relation. Such a relation might have been pruned, which is OK for result
/// relations, but not for scan relations; if `is_result_rel` is false the
/// caller must ensure that `rti` refers to an unpruned relation before
/// calling this function — attempting to open a pruned relation for scanning
/// results in an error.
///
/// `is_parallel_worker` is the C `IsParallelWorker()` global (parallel.c) —
/// an explicit parameter; the caller reads it off its own facet/state when
/// the parallel owner lands.
pub fn ExecGetRangeTableRelation<'mcx>(
    estate: &mut EStateData<'mcx>,
    rti: Index,
    is_result_rel: bool,
    is_parallel_worker: bool,
) -> PgResult<Relation<'mcx>> {
    debug_assert!(rti > 0 && rti as usize <= estate.es_range_table_size);

    if !is_result_rel
        && !bms_seams::bms_is_member::call(rti as i32, estate.es_unpruned_relids.as_deref())
    {
        // elog(ERROR, "trying to open a pruned relation")
        return Err(PgError::error("trying to open a pruned relation"));
    }

    let idx = (rti - 1) as usize;
    if estate.es_relations[idx].is_none() {
        // First time through, so open the relation
        let rte = exec_rt_fetch(rti, estate);
        debug_assert_eq!(rte.rtekind, RTE_RELATION);
        let (relid, rellockmode) = (rte.relid, rte.rellockmode);
        let mcx = estate.es_query_cxt;

        let rel = if !is_parallel_worker {
            // In a normal query, we should already have the appropriate
            // lock, but verify that through an Assert. Since there's already
            // an Assert inside table_open that insists on holding some lock,
            // it seems sufficient to check this only when rellockmode is
            // higher than the minimum.
            let rel = table::table_open(mcx, relid, NoLock)?;
            debug_assert!(
                rellockmode == AccessShareLock
                    || lmgr_seams::check_relation_locked_by_me::call(
                        rel.rd_id, rellockmode, false
                    )
            );
            rel
        } else {
            // If we are a parallel worker, we need to obtain our own local
            // lock on the relation. This ensures sane behavior in case the
            // parent process exits before we do.
            table::table_open(mcx, relid, rellockmode)?
        };

        estate.es_relations[idx] = Some(rel);
    }

    Ok(estate.es_relations[idx]
        .as_ref()
        .expect("just opened above")
        .alias())
}

/// `ExecInitResultRelation` — open the relation given by the passed-in RT
/// index and fill a `ResultRelInfo` for it, saving it in the
/// `estate->es_result_relations` array such that it can be accessed later
/// using the RT index.
///
/// The C takes a caller-allocated `ResultRelInfo *`; the owned model
/// allocates the node in the EState's pool and returns its id.
pub fn ExecInitResultRelation(
    estate: &mut EStateData<'_>,
    rti: Index,
    is_parallel_worker: bool,
) -> PgResult<RriId> {
    let result_relation_desc = ExecGetRangeTableRelation(estate, rti, true, is_parallel_worker)?;

    // InitResultRelInfo(resultRelInfo, resultRelationDesc, rti, NULL,
    //                   estate->es_instrument);
    let rri = estate.add_result_rel(Default::default())?;
    let mcx = estate.es_query_cxt;
    let instrument = estate.es_instrument;
    execMain_seams::init_result_rel_info::call(
        mcx,
        estate.result_rel_mut(rri),
        result_relation_desc,
        rti,
        None,
        instrument,
    )?;

    // if (estate->es_result_relations == NULL)
    //     palloc0(es_range_table_size * sizeof(ResultRelInfo *));
    if estate.es_result_relations.is_empty() {
        let mut arr = vec_with_capacity_in(mcx, estate.es_range_table_size)?;
        arr.resize(estate.es_range_table_size, None);
        estate.es_result_relations = arr;
    }
    estate.es_result_relations[(rti - 1) as usize] = Some(rri);

    // Saving in the list allows to avoid needlessly traversing the whole
    // array when only a few of its entries are possibly non-NULL.
    estate
        .es_opened_result_relations
        .try_reserve(1)
        .map_err(|_| mcx.oom(core::mem::size_of::<RriId>()))?;
    estate.es_opened_result_relations.push(rri);

    Ok(rri)
}

/// `UpdateChangedParamSet` — add changed parameters to a plan node's
/// `chgParam` set. The plan node only depends on params listed in its
/// `allParam` set; don't include anything else.
///
/// `mcx` is the C `CurrentMemoryContext` for the intersection allocation
/// (the per-query context while the executor runs).
pub fn UpdateChangedParamSet<'mcx>(
    node: &mut PlanStateData<'mcx>,
    newchg: Option<&Bitmapset<'_>>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    // parmset = bms_intersect(node->plan->allParam, newchg);
    let parmset = {
        let all_param = node
            .plan
            .as_deref()
            .expect("UpdateChangedParamSet: PlanState has no plan")
            .plan_head()
            .allParam
            .as_deref();
        bms_seams::bms_intersect::call(mcx, all_param, newchg)?
    };
    // node->chgParam = bms_join(node->chgParam, parmset);
    node.chgParam = bms_seams::bms_join::call(node.chgParam.take(), parmset);
    Ok(())
}

/// `executor_errposition` — report an execution-time cursor position, if
/// possible. Expected to be used within an `ereport()` call (here: while an
/// error report is being built on the error stack); the return value is a
/// dummy (always 0, in fact).
///
/// The locations stored in parsetrees are byte offsets into the source
/// string; they are converted to 1-based character indexes for reporting to
/// clients.
pub fn executor_errposition(estate: Option<&EStateData<'_>>, location: i32) -> PgResult<i32> {
    // No-op if location was not provided
    if location < 0 {
        return Ok(0);
    }
    // Can't do anything if source text is not available
    let Some(source_text) = estate.and_then(|e| e.es_sourceText.as_ref()) else {
        return Ok(0);
    };
    // Convert offset to character number
    let pos = mbutils_seams::pg_mbstrlen_with_len::call(source_text.as_str(), location) + 1;
    // And pass it to the ereport mechanism
    backend_utils_error::errposition(pos)?;
    Ok(0)
}

/// `RegisterExprContextCallback` — register a shutdown callback in an
/// ExprContext.
///
/// Shutdown callbacks will be called (in reverse order of registration) when
/// the ExprContext is deleted or rescanned. This provides a hook for
/// functions called in the context to do any cleanup needed — particularly
/// useful for functions returning sets. Note that the callback will *not* be
/// called in the event that execution is aborted by an error.
pub fn RegisterExprContextCallback<'mcx>(
    econtext: &mut ExprContext<'mcx>,
    function: ExprContextCallbackFunction,
    arg: Datum,
) -> PgResult<()> {
    // Save the info in appropriate memory context
    let mut ecxt_callback = alloc_in(
        econtext.ecxt_per_query_memory,
        ExprContext_CB {
            next: None,
            function,
            arg,
        },
    )?;

    // link to front of list for appropriate execution order
    ecxt_callback.next = econtext.ecxt_callbacks.take();
    econtext.ecxt_callbacks = Some(ecxt_callback);
    Ok(())
}

/// `UnregisterExprContextCallback` — deregister a shutdown callback in an
/// ExprContext. Any list entries matching the function and arg will be
/// removed (function pointers compared by address, as the C `==`).
pub fn UnregisterExprContextCallback(
    econtext: &mut ExprContext<'_>,
    function: ExprContextCallbackFunction,
    arg: Datum,
) {
    // C splices matches out in place via a prev_callback pointer-to-pointer;
    // the owned chain is drained and survivors re-linked in order.
    let mut rest = econtext.ecxt_callbacks.take();
    let mut head: Option<PgBox<'_, ExprContext_CB<'_>>> = None;
    let mut tail = &mut head;
    while let Some(mut ecxt_callback) = rest {
        rest = ecxt_callback.next.take();
        if ecxt_callback.function == function && ecxt_callback.arg == arg {
            // *prev_callback = ecxt_callback->next; pfree(ecxt_callback);
            continue;
        }
        *tail = Some(ecxt_callback);
        tail = &mut tail.as_mut().expect("just assigned").next;
    }
    econtext.ecxt_callbacks = head;
}

/// `ShutdownExprContext` (static) — call all the shutdown callbacks
/// registered in an ExprContext, in reverse registration order, emptying the
/// list (important in case this is only a rescan reset, not deletion of the
/// ExprContext).
///
/// If `is_commit` is false, just clean the callback list but don't call 'em.
///
/// The C runs the callbacks inside `ecxt_per_tuple_memory` so any leak is
/// mopped up; here that context is passed to the callback explicitly. A
/// callback may `ereport(ERROR)`: as in C, the error propagates with the
/// already-popped entries gone and the rest of the list still in place.
fn ShutdownExprContext(econtext: &mut ExprContext<'_>, is_commit: bool) -> PgResult<()> {
    // Fast path in normal case where there's nothing to do.
    if econtext.ecxt_callbacks.is_none() {
        return Ok(());
    }

    // Call each callback function in reverse registration order, inside the
    // per-tuple memory context (passed as the callback's allocation target).
    while let Some(mut ecxt_callback) = econtext.ecxt_callbacks.take() {
        econtext.ecxt_callbacks = ecxt_callback.next.take();
        if is_commit {
            (ecxt_callback.function)(econtext.ecxt_per_tuple_memory.mcx(), ecxt_callback.arg)?;
        }
        // pfree(ecxt_callback): drop
    }
    Ok(())
}

// ===========================================================================
//		GetAttributeByName / GetAttributeByNum
// ===========================================================================

/// `heap_getattr(tup, attnum, tupleDesc, isnull)` (htup_details.h inline,
/// localized into this unit by the c2rust build): extract one attribute from
/// a heap tuple, handling missing-attribute defaults and system attributes.
fn heap_getattr<'r>(
    mcx: Mcx<'r>,
    tup: &HeapTupleData<'_>,
    data: &[u8],
    attnum: i32,
    tuple_desc: &TupleDescData<'_>,
) -> PgResult<DeformedColumn<'r>> {
    let header = tup.t_data.as_deref().expect("heap_getattr: no t_data");
    if attnum > 0 {
        if attnum > HeapTupleHeaderGetNatts(header) as i32 {
            // attribute beyond what's stored: missing-value default
            Ok(getmissingattr(tuple_desc, attnum))
        } else {
            // fastgetattr(tup, attnum, tupleDesc, isnull):
            //   HeapTupleNoNulls || !att_isnull -> fetch; else NULL.
            let has_nulls = (header.t_infomask & HEAP_HASNULL) != 0;
            if has_nulls && heap_attisnull(tup, attnum, Some(tuple_desc)) {
                Ok((TupleValue::ByVal(Datum::null()), true))
            } else {
                Ok((nocachegetattr(mcx, tup, attnum, tuple_desc, data)?, false))
            }
        }
    } else {
        heap_getsysattr(mcx, tup, attnum)
    }
}

/// Build the C `tmptup` from a composite-Datum tuple: `heap_getattr` needs a
/// HeapTuple not a bare HeapTupleHeader; all the fields are set just in case
/// the user tries to inspect system columns.
fn tmptup_from_composite<'r>(
    mcx: Mcx<'r>,
    tuple: &FormedTuple<'_>,
) -> PgResult<HeapTupleData<'r>> {
    let header = tuple
        .tuple
        .t_data
        .as_deref()
        .expect("composite Datum has no header");
    Ok(HeapTupleData {
        // tmptup.t_len = HeapTupleHeaderGetDatumLength(tuple);
        t_len: HeapTupleHeaderGetDatumLength(header) as u32,
        // ItemPointerSetInvalid(&(tmptup.t_self));
        t_self: ItemPointerData::new(0xFFFF_FFFF, 0),
        // tmptup.t_tableOid = InvalidOid;
        t_tableOid: InvalidOid,
        // tmptup.t_data = tuple; (the owned model copies the small header)
        t_data: Some(alloc_in(mcx, header.clone_in(mcx)?)?),
        t_user_data: None,
    })
}

/// `GetAttributeByName` — return the value of the named attribute out of the
/// given (composite-Datum) tuple. C functions which take a tuple as an
/// argument are expected to use this. Note: rather slow because it does a
/// typcache lookup on each call.
///
/// Returns `(value, isnull)`; the C's NULL-pointer guards on `attname` /
/// `isNull` are unrepresentable here (`&str` and the returned pair cannot be
/// NULL). A `None` tuple is the C NULL tuple: "kinda bogus but compatible
/// with old behavior", yielding `(0, true)`.
pub fn GetAttributeByName<'r>(
    mcx: Mcx<'r>,
    tuple: Option<&FormedTuple<'_>>,
    attname: &str,
) -> PgResult<(TupleValue<'r>, bool)> {
    let Some(tuple) = tuple else {
        // Kinda bogus but compatible with old behavior...
        return Ok((TupleValue::ByVal(Datum::null()), true));
    };

    let header = tuple
        .tuple
        .t_data
        .as_deref()
        .expect("composite Datum has no header");
    let tup_type = HeapTupleHeaderGetTypeId(header);
    let tup_typmod = HeapTupleHeaderGetTypMod(header);
    let tup_desc = typcache_seams::lookup_rowtype_tupdesc::call(mcx, tup_type, tup_typmod)?;

    let mut attrno: AttrNumber = InvalidAttrNumber;
    for i in 0..tup_desc.natts {
        let att = tup_desc.attr(i as usize);
        // namestrcmp(&(att->attname), attname) == 0 (adt/name.c): NUL-padded
        // NameData vs C string equality.
        if att.attname.name_str() == attname.as_bytes() {
            attrno = att.attnum;
            break;
        }
    }

    if attrno == InvalidAttrNumber {
        // elog(ERROR, "attribute \"%s\" does not exist", attname);
        return Err(PgError::error(format!(
            "attribute \"{attname}\" does not exist"
        )));
    }

    let tmptup = tmptup_from_composite(mcx, tuple)?;
    let result = heap_getattr(mcx, &tmptup, &tuple.data, attrno as i32, &tup_desc)?;

    // ReleaseTupleDesc(tupDesc): the owned copy drops.
    Ok(result)
}

/// `GetAttributeByNum` — as [`GetAttributeByName`], by attribute number.
pub fn GetAttributeByNum<'r>(
    mcx: Mcx<'r>,
    tuple: Option<&FormedTuple<'_>>,
    attrno: AttrNumber,
) -> PgResult<(TupleValue<'r>, bool)> {
    // if (!AttributeNumberIsValid(attrno))
    if attrno == InvalidAttrNumber {
        // elog(ERROR, "invalid attribute number %d", attrno);
        return Err(PgError::error(format!("invalid attribute number {attrno}")));
    }

    let Some(tuple) = tuple else {
        // Kinda bogus but compatible with old behavior...
        return Ok((TupleValue::ByVal(Datum::null()), true));
    };

    let header = tuple
        .tuple
        .t_data
        .as_deref()
        .expect("composite Datum has no header");
    let tup_type = HeapTupleHeaderGetTypeId(header);
    let tup_typmod = HeapTupleHeaderGetTypMod(header);
    let tup_desc = typcache_seams::lookup_rowtype_tupdesc::call(mcx, tup_type, tup_typmod)?;

    let tmptup = tmptup_from_composite(mcx, tuple)?;
    let result = heap_getattr(mcx, &tmptup, &tuple.data, attrno as i32, &tup_desc)?;

    // ReleaseTupleDesc(tupDesc): the owned copy drops.
    Ok(result)
}

/// `ExecTargetListLength` — number of items in a tlist (including any resjunk
/// items!). "This used to be more complex, but fjoins are dead."
pub fn ExecTargetListLength(targetlist: &[TargetEntry<'_>]) -> i32 {
    targetlist.len() as i32
}

/// `ExecCleanTargetListLength` — number of items in a tlist, not including
/// any resjunk items.
pub fn ExecCleanTargetListLength(targetlist: &[TargetEntry<'_>]) -> i32 {
    let mut len = 0;
    for cur_tle in targetlist {
        if !cur_tle.resjunk {
            len += 1;
        }
    }
    len
}

// ===========================================================================
//		Trigger / returning / all-null slots
// ===========================================================================

/// Shared body of the four lazily-created per-ResultRelInfo slots: build an
/// extra slot over the relation's descriptor with its table-AM slot
/// callbacks, allocated in the per-query context (the C
/// `MemoryContextSwitchTo(estate->es_query_cxt)` wrapper).
fn make_rel_extra_slot(estate: &mut EStateData<'_>, rel: &Relation<'_>) -> PgResult<SlotId> {
    let mcx = estate.es_query_cxt;
    // RelationGetDescr(rel) — cloned into the per-query context (the slot
    // owns its copy).
    let tupdesc = Some(alloc_in(mcx, rel.rd_att.clone_in(mcx)?)?);
    let callbacks = tableam::table_slot_callbacks(rel);
    execTuples_seams::exec_init_extra_tuple_slot::call(estate, tupdesc, callbacks)
}

/// `ExecGetTriggerOldSlot` — return a relInfo's tuple slot for a trigger's
/// OLD tuples (lazily created).
pub fn ExecGetTriggerOldSlot(estate: &mut EStateData<'_>, rel_info: RriId) -> PgResult<SlotId> {
    if estate.result_rel(rel_info).ri_TrigOldSlot.is_none() {
        let rel = estate
            .result_rel(rel_info)
            .ri_RelationDesc
            .as_ref()
            .expect("ExecGetTriggerOldSlot: ResultRelInfo has no relation")
            .alias();
        let slot = make_rel_extra_slot(estate, &rel)?;
        estate.result_rel_mut(rel_info).ri_TrigOldSlot = Some(slot);
    }
    Ok(estate
        .result_rel(rel_info)
        .ri_TrigOldSlot
        .expect("just initialized above"))
}

/// `ExecGetTriggerNewSlot` — return a relInfo's tuple slot for a trigger's
/// NEW tuples (lazily created).
pub fn ExecGetTriggerNewSlot(estate: &mut EStateData<'_>, rel_info: RriId) -> PgResult<SlotId> {
    if estate.result_rel(rel_info).ri_TrigNewSlot.is_none() {
        let rel = estate
            .result_rel(rel_info)
            .ri_RelationDesc
            .as_ref()
            .expect("ExecGetTriggerNewSlot: ResultRelInfo has no relation")
            .alias();
        let slot = make_rel_extra_slot(estate, &rel)?;
        estate.result_rel_mut(rel_info).ri_TrigNewSlot = Some(slot);
    }
    Ok(estate
        .result_rel(rel_info)
        .ri_TrigNewSlot
        .expect("just initialized above"))
}

/// `ExecGetReturningSlot` — return a relInfo's tuple slot for processing
/// returning tuples (lazily created).
pub fn ExecGetReturningSlot(estate: &mut EStateData<'_>, rel_info: RriId) -> PgResult<SlotId> {
    if estate.result_rel(rel_info).ri_ReturningSlot.is_none() {
        let rel = estate
            .result_rel(rel_info)
            .ri_RelationDesc
            .as_ref()
            .expect("ExecGetReturningSlot: ResultRelInfo has no relation")
            .alias();
        let slot = make_rel_extra_slot(estate, &rel)?;
        estate.result_rel_mut(rel_info).ri_ReturningSlot = Some(slot);
    }
    Ok(estate
        .result_rel(rel_info)
        .ri_ReturningSlot
        .expect("just initialized above"))
}

/// `ExecGetAllNullSlot` — return a relInfo's all-NULL tuple slot for
/// processing returning tuples (lazily created).
///
/// Note: this slot is intentionally filled with NULLs in every column, and
/// should be considered read-only — the caller must not update it.
pub fn ExecGetAllNullSlot(estate: &mut EStateData<'_>, rel_info: RriId) -> PgResult<SlotId> {
    if estate.result_rel(rel_info).ri_AllNullSlot.is_none() {
        let rel = estate
            .result_rel(rel_info)
            .ri_RelationDesc
            .as_ref()
            .expect("ExecGetAllNullSlot: ResultRelInfo has no relation")
            .alias();
        let slot = make_rel_extra_slot(estate, &rel)?;
        execTuples_seams::exec_store_all_null_tuple::call(estate, slot)?;
        estate.result_rel_mut(rel_info).ri_AllNullSlot = Some(slot);
    }
    Ok(estate
        .result_rel(rel_info)
        .ri_AllNullSlot
        .expect("just initialized above"))
}

// ===========================================================================
//		Tuple conversion maps and updated-column bitmaps
// ===========================================================================

/// `ExecGetChildToRootMap` — return the map needed to convert given child
/// result relation's tuples to the rowtype of the query's main target
/// ("root") relation, computing it on first use. A `None` result is valid
/// and means that no conversion is needed.
///
/// The C reaches the relations through the ResultRelInfo pointers; the owned
/// model takes the EState (which owns the pool and the per-query context).
pub fn ExecGetChildToRootMap<'a, 'mcx>(
    estate: &'a mut EStateData<'mcx>,
    result_rel_info: RriId,
) -> PgResult<Option<&'a types_tuple::tupconvert::TupleConversionMap<'mcx>>> {
    // If we didn't already do so, compute the map for this child.
    if !estate.result_rel(result_rel_info).ri_ChildToRootMapValid {
        let root_rel_info = estate.result_rel(result_rel_info).ri_RootResultRelInfo;
        match root_rel_info {
            Some(root) => {
                let child_rel = estate
                    .result_rel(result_rel_info)
                    .ri_RelationDesc
                    .as_ref()
                    .expect("ExecGetChildToRootMap: ResultRelInfo has no relation")
                    .alias();
                let root_rel = estate
                    .result_rel(root)
                    .ri_RelationDesc
                    .as_ref()
                    .expect("ExecGetChildToRootMap: root ResultRelInfo has no relation")
                    .alias();
                let mcx = estate.es_query_cxt;
                // convert_tuples_by_name(RelationGetDescr(child),
                //                        RelationGetDescr(root))
                let map = tupconvert_seams::convert_tuples_by_name::call(
                    mcx,
                    &child_rel.rd_att,
                    &root_rel.rd_att,
                )?;
                estate.result_rel_mut(result_rel_info).ri_ChildToRootMap = map;
            }
            None => {
                // this isn't a child result rel
                estate.result_rel_mut(result_rel_info).ri_ChildToRootMap = None;
            }
        }
        estate
            .result_rel_mut(result_rel_info)
            .ri_ChildToRootMapValid = true;
    }

    Ok(estate
        .result_rel(result_rel_info)
        .ri_ChildToRootMap
        .as_deref())
}

/// `ExecGetRootToChildMap` — return the map needed to convert given root
/// result relation's tuples to the rowtype of the given child relation,
/// computing it on first use. A `None` result is valid and means that no
/// conversion is needed.
pub fn ExecGetRootToChildMap<'a, 'mcx>(
    estate: &'a mut EStateData<'mcx>,
    result_rel_info: RriId,
) -> PgResult<Option<&'a types_tuple::tupconvert::TupleConversionMap<'mcx>>> {
    // Mustn't get called for a non-child result relation.
    let root = estate
        .result_rel(result_rel_info)
        .ri_RootResultRelInfo
        .expect("ExecGetRootToChildMap: not a child result relation");

    // If we didn't already do so, compute the map for this child.
    if !estate.result_rel(result_rel_info).ri_RootToChildMapValid {
        let child_rel = estate
            .result_rel(result_rel_info)
            .ri_RelationDesc
            .as_ref()
            .expect("ExecGetRootToChildMap: ResultRelInfo has no relation")
            .alias();
        let root_rel = estate
            .result_rel(root)
            .ri_RelationDesc
            .as_ref()
            .expect("ExecGetRootToChildMap: root ResultRelInfo has no relation")
            .alias();
        let mcx = estate.es_query_cxt;

        // When this child table is not a partition (!relispartition), it may
        // have columns that are not present in the root table, which we ask
        // to ignore by passing true for missing_ok.
        let relispartition = child_rel.rd_rel.relispartition;
        let attr_map = tupconvert_seams::build_attrmap_by_name_if_req::call(
            mcx,
            &root_rel.rd_att,
            &child_rel.rd_att,
            !relispartition,
        )?;
        if let Some(attr_map) = attr_map {
            // The owned descriptors move into the map.
            let indesc = Some(alloc_in(mcx, root_rel.rd_att.clone_in(mcx)?)?);
            let outdesc = Some(alloc_in(mcx, child_rel.rd_att.clone_in(mcx)?)?);
            let map = tupconvert_seams::convert_tuples_by_name_attrmap::call(
                mcx, indesc, outdesc, attr_map,
            )?;
            estate.result_rel_mut(result_rel_info).ri_RootToChildMap = Some(map);
        }
        estate
            .result_rel_mut(result_rel_info)
            .ri_RootToChildMapValid = true;
    }

    Ok(estate
        .result_rel(result_rel_info)
        .ri_RootToChildMap
        .as_deref())
}

/// `GetResultRTEPermissionInfo` (static) — look up the `RTEPermissionInfo`
/// for the `ExecGet*Cols()` routines. Returns the 0-based index into
/// `estate.es_rteperminfos` (the C returns the node pointer), or `None`.
fn GetResultRTEPermissionInfo(
    estate: &EStateData<'_>,
    rel_info: RriId,
) -> PgResult<Option<usize>> {
    let relinfo = estate.result_rel(rel_info);

    let rti: Index = if let Some(root) = relinfo.ri_RootResultRelInfo {
        // For inheritance child result relations (a partition routing target
        // of an INSERT or a child UPDATE target), this returns the root
        // parent's RTE to fetch the RTEPermissionInfo because that's the
        // only one that has one assigned.
        estate.result_rel(root).ri_RangeTableIndex
    } else if relinfo.ri_RangeTableIndex != 0 {
        // Non-child result relations have their own RTEPermissionInfo.
        relinfo.ri_RangeTableIndex
    } else {
        // The relation isn't in the range table and it isn't a partition
        // routing target. This ResultRelInfo must've been created only for
        // firing triggers and the relation is not being inserted into. (See
        // ExecGetTriggerResultRel.)
        0
    };

    if rti > 0 {
        let rte = exec_rt_fetch(rti, estate);
        let idx = parse_relation_seams::get_rte_permission_info::call(&estate.es_rteperminfos, rte)?;
        Ok(Some(idx))
    } else {
        Ok(None)
    }
}

/// Copy an optional perminfo bitmapset into the caller's context. The C
/// returns the perminfo's own set by pointer; the owned model copies.
fn copy_cols<'r>(
    mcx: Mcx<'r>,
    cols: Option<&Bitmapset<'_>>,
) -> PgResult<Option<PgBox<'r, Bitmapset<'r>>>> {
    match cols {
        Some(b) => Ok(Some(alloc_in(mcx, b.clone_in(mcx)?)?)),
        None => Ok(None),
    }
}

/// `ExecGetInsertedCols` — return a bitmap representing the columns being
/// inserted, mapped to the child's attribute numbers if this is a child
/// result relation. The result is allocated in `mcx` (the C returns the
/// perminfo's set or allocates in `CurrentMemoryContext`).
pub fn ExecGetInsertedCols<'r>(
    estate: &mut EStateData<'_>,
    rel_info: RriId,
    mcx: Mcx<'r>,
) -> PgResult<Option<PgBox<'r, Bitmapset<'r>>>> {
    let Some(pi) = GetResultRTEPermissionInfo(estate, rel_info)? else {
        return Ok(None);
    };

    // Map the columns to child's attribute numbers if needed.
    if estate.result_rel(rel_info).ri_RootResultRelInfo.is_some() {
        ExecGetRootToChildMap(estate, rel_info)?; // compute the cached map
        if let Some(map) = estate.result_rel(rel_info).ri_RootToChildMap.as_deref() {
            return tupconvert_seams::execute_attr_map_cols::call(
                mcx,
                &map.attrMap,
                estate.es_rteperminfos[pi].insertedCols.as_deref(),
            );
        }
    }

    copy_cols(mcx, estate.es_rteperminfos[pi].insertedCols.as_deref())
}

/// `ExecGetUpdatedCols` — return a bitmap representing the columns being
/// updated (see [`ExecGetInsertedCols`] for the allocation contract).
pub fn ExecGetUpdatedCols<'r>(
    estate: &mut EStateData<'_>,
    rel_info: RriId,
    mcx: Mcx<'r>,
) -> PgResult<Option<PgBox<'r, Bitmapset<'r>>>> {
    let Some(pi) = GetResultRTEPermissionInfo(estate, rel_info)? else {
        return Ok(None);
    };

    // Map the columns to child's attribute numbers if needed.
    if estate.result_rel(rel_info).ri_RootResultRelInfo.is_some() {
        ExecGetRootToChildMap(estate, rel_info)?; // compute the cached map
        if let Some(map) = estate.result_rel(rel_info).ri_RootToChildMap.as_deref() {
            return tupconvert_seams::execute_attr_map_cols::call(
                mcx,
                &map.attrMap,
                estate.es_rteperminfos[pi].updatedCols.as_deref(),
            );
        }
    }

    copy_cols(mcx, estate.es_rteperminfos[pi].updatedCols.as_deref())
}

/// `ExecGetExtraUpdatedCols` — return a bitmap representing the generated
/// columns being updated, computing the info via `ExecInitGenerated` if we
/// didn't already.
pub fn ExecGetExtraUpdatedCols<'r>(
    estate: &mut EStateData<'_>,
    rel_info: RriId,
    mcx: Mcx<'r>,
) -> PgResult<Option<PgBox<'r, Bitmapset<'r>>>> {
    // Compute the info if we didn't already
    if !estate.result_rel(rel_info).ri_extraUpdatedCols_valid {
        modifytable_seams::exec_init_generated::call(estate, rel_info, CMD_UPDATE)?;
    }
    copy_cols(
        mcx,
        estate.result_rel(rel_info).ri_extraUpdatedCols.as_deref(),
    )
}

/// `ExecGetAllUpdatedCols` — return the columns being updated, including
/// generated columns:
/// `bms_union(ExecGetUpdatedCols(...), ExecGetExtraUpdatedCols(...))`.
///
/// The C allocates the result in the per-tuple memory context and notes it's
/// up to the caller to copy it into a different context with the appropriate
/// lifespan; the owned model makes that explicit — the caller supplies the
/// target context `mcx`.
pub fn ExecGetAllUpdatedCols<'r>(
    estate: &mut EStateData<'_>,
    rel_info: RriId,
    mcx: Mcx<'r>,
) -> PgResult<Option<PgBox<'r, Bitmapset<'r>>>> {
    let updated = ExecGetUpdatedCols(estate, rel_info, mcx)?;
    let extra = ExecGetExtraUpdatedCols(estate, rel_info, mcx)?;
    bms_seams::bms_union::call(mcx, updated.as_deref(), extra.as_deref())
}

/// `ExecGetResultRelCheckAsUser` — returns the user to modify the passed-in
/// result relation as, chosen by looking up the relation's or, if a child
/// table, its root parent's RTEPermissionInfo.
///
/// `current_user_id` is the C `GetUserId()` per-backend global (miscinit.c)
/// — an explicit parameter; the caller reads it off its own facet/state when
/// the miscinit owner lands.
pub fn ExecGetResultRelCheckAsUser(
    estate: &EStateData<'_>,
    rel_info: RriId,
    current_user_id: Oid,
) -> PgResult<Oid> {
    let Some(pi) = GetResultRTEPermissionInfo(estate, rel_info)? else {
        // XXX - maybe ok to return GetUserId() in this case?
        let relid = estate
            .result_rel(rel_info)
            .ri_RelationDesc
            .as_ref()
            .map_or(InvalidOid, |r| r.rd_id);
        return Err(PgError::error(format!(
            "no RTEPermissionInfo found for result relation with OID {relid}"
        )));
    };

    let check_as_user = estate.es_rteperminfos[pi].checkAsUser;
    Ok(if check_as_user != InvalidOid {
        check_as_user
    } else {
        current_user_id
    })
}

#[cfg(test)]
mod tests;
