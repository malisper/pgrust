//! `backend-executor-execMain` (`executor/execMain.c`) — the executor driver.
//!
//! ## F0d driver scope (#166 / EState-Plan keystone #167)
//!
//! This crate is the executor driver: `CreateQueryDesc` builds the owned
//! [`QueryDesc`] bundle, `standard_ExecutorStart` (→ `InitPlan`) builds the
//! plan-state tree and the result tupdesc, `standard_ExecutorRun` (→
//! `ExecutePlan`) drives the plan tuple-at-a-time into the `DestReceiver`, and
//! `standard_ExecutorEnd` (→ `ExecEndPlan`) tears it down. The whole loop is
//! wired end-to-end for a plain `DestNone`/`DestRemote` `SELECT`.
//!
//! ### B1 leak-projection (HONEST `&'mcx Node`, NOT a transmute)
//!
//! `InitPlan` must hand `ExecInitNode` an honest `&'mcx Node` for
//! `plannedstmt->planTree` (the seam is signed `node: Option<&'mcx Node>`, and
//! re-signing it would ripple the ~25 node `ExecInit*` crates — rejected). The
//! plan tree lives inside the [`QueryDesc`]'s `McxOwned` bundle, so
//! [`QueryDesc::with_plan_and_estate_mut`] leaks it via the bundle's own `'mcx`
//! allocator ([`mcx::leak_in`]): the allocator lifetime *is* `'mcx`, and the
//! per-query context drop reclaims it — faithful to C's "plan freed with its
//! context". This is a real borrow, not a transmute.
//!
//! ### Guard-and-panic frontier
//!
//! `InitPlan` now runs the full `ExecCheckPermissions` / `ExecCheckOneRelPerms`
//! per-rel classification (`RTEPermissionInfo` carries the
//! `requiredPerms`/`selectedCols`/`insertedCols`/`updatedCols` fields), so a real
//! table query no longer panics on the permission leg (the ACL checks seam into
//! the still-unported `catalog/aclchk.c` owner; with the planner that populates
//! `RTEPermissionInfo` also unported, a plain SELECT's `permInfos` is empty and
//! the check is a no-op).
//!
//! The remaining branches a plain `DestNone` `SELECT` does not exercise —
//! `EXPLAIN`-only read-only/parallel gating, `parallelModeNeeded`, `rowMarks`,
//! `subplans`, partition pruning, RETURNING, and non-`SELECT` command types —
//! `panic!` with a precise message (mirror-pg-and-panic on a live frontier); the
//! unit stays CATALOG `needs-decomp` so the seam-install recurrence guard
//! exempts the still-open surface.

#![no_std]
#![allow(non_snake_case)]
// `PgError` is a large owned struct; the un-boxed `Err` is the project-wide
// error contract.
#![allow(clippy::result_large_err)]

extern crate alloc;

use alloc::string::ToString;

use mcx::MemoryContext;
use types_acl::acl::{
    AclMaskHow, AclMode, AclResult, ACL_INSERT, ACL_SELECT, ACL_UPDATE,
};
use types_nodes::bitmapset::{Bitmapset, BITS_PER_BITMAPWORD};
use types_nodes::parsenodes::RTEPermissionInfo;
use types_core::primitive::Oid;
use types_error::PgResult;
use types_nodes::nodeindexscan::PlannedStmt;
use types_nodes::nodes::CmdType;
use types_nodes::parsestmt::{DestReceiverHandle, ParamListInfoHandle};
use types_nodes::querydesc::QueryDesc;
use types_scan::sdir::{ScanDirection, ScanDirectionIsNoMovement};

use backend_catalog_aclchk_seams as aclchk;
use backend_catalog_objectaddress_seams as objaddr;
use backend_executor_execJunk as execJunk;
use backend_executor_execMain_seams as seams;
use backend_executor_execProcnode_seams as procnode;
use backend_executor_execUtils as execUtils;
use backend_executor_execUtils_seams as execUtils_seams;
use backend_tcop_dest_seams as dest;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_init_miscinit_seams as miscinit;

// `FirstLowInvalidHeapAttributeNumber` (access/sysattr.h) — the column-bitmap
// offset the planner applies to selectedCols bit numbers.
use types_tuple::heaptuple::FirstLowInvalidHeapAttributeNumber;

// EState eflags (executor/executor.h). Mirrored locally; there is no canonical
// shared constant yet (other node crates likewise define them locally).
const EXEC_FLAG_BACKWARD: i32 = 0x0008;
const EXEC_FLAG_SKIP_TRIGGERS: i32 = 0x0020;

/// `CreateQueryDesc(plannedstmt, sourceText, snapshot, crosscheck_snapshot,
/// dest, params, queryEnv, instrument_options)` (execMain.c).
///
/// Allocates the per-query "ExecutorState" context, builds the `EState` in it
/// (`CreateExecutorState`), and copies the read-only inputs. `ExecutorStart`
/// fills the plan-state tree and result tupdesc.
#[allow(clippy::too_many_arguments)]
pub fn CreateQueryDesc(
    parent: &MemoryContext,
    plannedstmt: &PlannedStmt<'_>,
    source_text: &str,
    snapshot: Option<alloc::rc::Rc<types_snapshot::SnapshotData>>,
    crosscheck_snapshot: Option<alloc::rc::Rc<types_snapshot::SnapshotData>>,
    dest: DestReceiverHandle,
    params: ParamListInfoHandle,
    instrument_options: i32,
) -> PgResult<QueryDesc> {
    QueryDesc::create(
        parent,
        plannedstmt,
        source_text,
        snapshot,
        crosscheck_snapshot,
        dest,
        params,
        instrument_options,
    )
}

// ===========================================================================
// ExecutorStart / InitPlan
// ===========================================================================

/// `standard_ExecutorStart(queryDesc, eflags)` (execMain.c).
///
/// Fills the `EState` fields the C prologue sets from `queryDesc`, runs the
/// read-only/parallel gate (when applicable), and calls [`InitPlan`] to build
/// the plan-state tree, the result tupdesc and the top junk filter.
pub fn standard_ExecutorStart(query_desc: &mut QueryDesc, mut eflags: i32) -> PgResult<()> {
    // sanity checks: queryDesc must not be started already (planstate == NULL).
    // (The active-snapshot Assert is the caller's contract.)

    // If the transaction is read-only / we're in parallel mode (and not
    // EXPLAIN-only), check for writes to non-temp tables.
    //   if ((XactReadOnly || IsInParallelMode()) && !EXPLAIN_ONLY)
    //       ExecCheckXactReadOnly(queryDesc->plannedstmt);
    //
    // `XactReadOnly` / `IsInParallelMode()` reach the xact owner (unported on
    // this frontier). They are only consulted when `!EXPLAIN_ONLY`, and for a
    // plain SELECT in a normal read-write, non-parallel transaction both are
    // false so the gate is not entered (a faithful no-op here). When the xact
    // owner lands, `(XactReadOnly || IsInParallelMode()) && !EXPLAIN_ONLY`
    // (EXEC_FLAG_EXPLAIN_ONLY = 0x0001) calls
    // `ExecCheckXactReadOnly(queryDesc->plannedstmt)`.

    let operation = query_desc.operation;
    let params = query_desc.params;
    let instrument = query_desc.instrument_options;

    // estate->es_param_list_info = queryDesc->params;
    // (paramExecTypes != NIL ⇒ allocate es_param_exec_vals — guarded below.)
    // estate->es_sourceText = queryDesc->sourceText;  (bundle-owned copy.)
    // estate->es_queryEnv = queryDesc->queryEnv;
    //
    // Non-read-only command-id assignment + AFTER-trigger setup:
    //   CMD_SELECT: if (rowMarks || hasModifyingCTE) es_output_cid = ...;
    //               if (!hasModifyingCTE) eflags |= EXEC_FLAG_SKIP_TRIGGERS;
    //   CMD_{INSERT,DELETE,UPDATE,MERGE}: es_output_cid = GetCurrentCommandId(true);
    //   default: elog(ERROR, "unrecognized operation code").
    match operation {
        CmdType::CMD_SELECT => {
            // SELECT FOR [KEY] UPDATE/SHARE and modifying CTEs mark tuples; both
            // pull the xact owner (GetCurrentCommandId) and are out of the plain
            // SELECT path — guard-and-panic.
            query_desc.work.with(|w| {
                if w.plannedstmt
                    .rowMarks
                    .as_ref()
                    .is_some_and(|m| !m.is_empty())
                    || w.plannedstmt.hasModifyingCTE
                {
                    panic!(
                        "execMain standard_ExecutorStart: SELECT FOR UPDATE/SHARE rowMarks or \
                         modifying CTE needs es_output_cid = GetCurrentCommandId(true) (xact \
                         owner) — #167 F0d"
                    );
                }
            });
            // A SELECT without modifying CTEs can't queue triggers.
            eflags |= EXEC_FLAG_SKIP_TRIGGERS;
        }
        _ => {
            panic!(
                "execMain standard_ExecutorStart: only CMD_SELECT is wired on this driver \
                 frontier; non-SELECT command id assignment (es_output_cid = \
                 GetCurrentCommandId) reaches the xact owner — #167 F0d"
            );
        }
    }

    query_desc.work.with_mut(|w| {
        w.estate.es_param_list_info = params;
        // estate->es_snapshot / es_crosscheck_snapshot = RegisterSnapshot(...) —
        // the snapmgr owner registers the bundle-carried snapshot on the plain
        // path's first reader; the bundle keeps the snapshot alive.
        w.estate.es_top_eflags = eflags;
        w.estate.es_instrument = instrument;
        w.estate.es_jit_flags = w.plannedstmt.jitFlags;
        if w.plannedstmt
            .paramExecTypes
            .as_ref()
            .is_some_and(|p| !p.is_empty())
        {
            panic!(
                "execMain standard_ExecutorStart: paramExecTypes != NIL needs es_param_exec_vals \
                 allocation (PARAM_EXEC slots) — #167 F0d"
            );
        }
    });

    // AfterTriggerBeginQuery() unless SKIP_TRIGGERS / EXPLAIN_ONLY — SKIP_TRIGGERS
    // is set for the plain SELECT above, so it is elided (faithful no-op).

    // InitPlan(queryDesc, eflags).
    InitPlan(query_desc, eflags)
}

/// `InitPlan(queryDesc, eflags)` (execMain.c) — build the plan-state tree, the
/// result tupdesc and the top junk filter.
fn InitPlan(query_desc: &mut QueryDesc, eflags: i32) -> PgResult<()> {
    let operation = query_desc.operation;

    query_desc.with_plan_and_estate_mut(|plan, plannedstmt, estate, planstate_slot| {
        // Do permissions checks: ExecCheckPermissions(rangeTable, permInfos, true).
        // ExecCheckPermissions ereports (ACLCHECK) on a violation; the rangeTable
        // is unused (it survives only for the un-modeled ExecutorCheckPerms_hook).
        if let Some(perm_infos) = plannedstmt.permInfos.as_ref() {
            exec_check_permissions(perm_infos.as_slice(), true)?;
        }

        // ExecInitRangeTable(estate, rangeTable, permInfos, bms_copy(unprunableRelids)).
        // Move the range table / permInfos / unprunableRelids out of the bundle's
        // plannedstmt into the EState (C aliases the planner-owned lists; the
        // owned model hands the bundle-owned copies over).
        let range_table = plannedstmt.rtable.take().unwrap_or_else(|| {
            mcx::PgVec::new_in(estate.es_query_cxt)
        });
        let perm_infos = plannedstmt.permInfos.take().unwrap_or_else(|| {
            mcx::PgVec::new_in(estate.es_query_cxt)
        });
        let unpruned = plannedstmt.unprunableRelids.take();
        execUtils::ExecInitRangeTable(estate, range_table, perm_infos, unpruned)?;

        // estate->es_plannedstmt = plannedstmt;  (C aliases; owned model clones a
        // copyObject-shape copy into the per-query context so the EState is
        // self-contained — read by ExecGetRangeTableRelation / ...IsTargetRelation.)
        let mcx = estate.es_query_cxt;
        estate.es_plannedstmt = Some(mcx::alloc_in(mcx, plannedstmt.clone_in(mcx)?)?);

        // estate->es_part_prune_infos = plannedstmt->partPruneInfos;
        // ExecDoInitialPruning(estate) — the trimmed PlannedStmt carries no
        // partPruneInfos; es_part_prune_infos stays empty, so initial pruning is
        // a no-op for the plain path. (Guard-and-panic if pruning state is ever
        // present.)
        if !estate.es_part_prune_infos.is_empty() {
            panic!(
                "execMain InitPlan: ExecDoInitialPruning over partPruneInfos not wired \
                 (trimmed PlannedStmt carries no partPruneInfos) — #167 F0d"
            );
        }

        // Build the ExecRowMark array from PlanRowMark(s), if any.
        if plannedstmt.rowMarks.as_ref().is_some_and(|m| !m.is_empty()) {
            panic!(
                "execMain InitPlan: ExecRowMark array build over plannedstmt->rowMarks not \
                 wired (needs ExecGetRangeTableRelation rowmark open + CheckValidRowMarkRel) — \
                 #167 F0d"
            );
        }

        // estate->es_tupleTable = NIL;  (CreateExecutorState left it empty.)
        // estate->es_epq_active = NULL; (default None.)

        // Initialize per-SubPlan state before ExecInitNode on the main tree.
        if plannedstmt.subplans.as_ref().is_some_and(|s| !s.is_empty()) {
            panic!(
                "execMain InitPlan: per-SubPlan ExecInitNode loop over plannedstmt->subplans not \
                 wired (es_subplanstates build) — #167 F0d"
            );
        }

        // planstate = ExecInitNode(plan, estate, eflags).
        let planstate = procnode::exec_init_node::call(mcx, plan, estate, eflags)?;
        *planstate_slot = planstate;

        // Get the tuple descriptor describing the type of tuples to return, and
        // initialize the junk filter if the SELECT tlist has junk attrs.
        let planstate_ref = match planstate_slot.as_deref() {
            Some(ps) => ps,
            None => {
                // ExecInitNode(NULL,...) — degenerate plan; nothing more to do.
                return Ok::<(), types_error::PgError>(());
            }
        };

        // operation == CMD_SELECT: junk_filter_needed = any resjunk in tlist.
        if operation == CmdType::CMD_SELECT {
            // plan->targetlist (the leaked planTree node's tlist).
            let junk_filter_needed = plan
                .map(|n| {
                    n.plan_head()
                        .targetlist
                        .as_ref()
                        .map(|tl| tl.iter().any(|tle| tle.resjunk))
                        .unwrap_or(false)
                })
                .unwrap_or(false);

            if junk_filter_needed {
                // j = ExecInitJunkFilter(planstate->plan->targetlist, slot);
                // (C makes a virtual extra slot first; ExecInitJunkFilter with
                // None allocates the equivalent virtual result slot.)
                let _ = planstate_ref;
                let src_tlist = plan
                    .and_then(|n| n.plan_head().targetlist.as_ref())
                    .expect("junk filter needed ⇒ plan has a targetlist");
                let mut tlist = mcx::vec_with_capacity_in(mcx, src_tlist.len())?;
                for tle in src_tlist.iter() {
                    tlist.push(tle.clone_in(mcx)?);
                }
                let jf = execJunk::ExecInitJunkFilter(estate, tlist, None)?;
                estate.es_junkFilter = Some(mcx::alloc_in(mcx, jf)?);
            }
        }
        Ok(())
    })
}

/// `ExecutorStart(queryDesc, eflags)` — the hookable entry; routes to
/// [`standard_ExecutorStart`] (no `ExecutorStart_hook` consumer yet).
pub fn ExecutorStart(query_desc: &mut QueryDesc, eflags: i32) -> PgResult<()> {
    standard_ExecutorStart(query_desc, eflags)
}

// ===========================================================================
// ExecutorRun / ExecutePlan
// ===========================================================================

/// `standard_ExecutorRun(queryDesc, direction, count)` (execMain.c) — the main
/// executor driver: start the receiver, run the plan, shut the receiver.
pub fn standard_ExecutorRun(
    query_desc: &mut QueryDesc,
    direction: ScanDirection,
    count: u64,
) -> PgResult<()> {
    // Assert(!(es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY)).
    let operation = query_desc.operation;
    let dest_handle = query_desc.dest;

    // estate->es_processed = 0;
    query_desc.with_estate_mut(|estate| {
        estate.es_processed = 0;
    });

    // sendTuples = (operation == CMD_SELECT || queryDesc->plannedstmt->hasReturning);
    let has_returning = query_desc.work.with(|w| w.plannedstmt.hasReturning);
    let send_tuples = operation == CmdType::CMD_SELECT || has_returning;

    // startup tuple receiver, if we will be emitting tuples.
    if send_tuples {
        // dest->rStartup(dest, operation, queryDesc->tupDesc);
        let res = query_desc.with_estate_and_planstate_mut(|estate, planstate| {
            let tupdesc = result_tupdesc(estate, planstate.as_deref());
            match tupdesc {
                Some(td) => dest::dest_rstartup::call(dest_handle, operation, td),
                None => {
                    // No result tupdesc (degenerate plan); the C would still pass
                    // the (NULL-ish) descriptor. A plain SELECT always has one.
                    panic!(
                        "execMain ExecutorRun: dest->rStartup needs queryDesc->tupDesc but the \
                         started plan produced no result TupleDesc"
                    );
                }
            }
        });
        res?;
    }

    // Run plan, unless direction is NoMovement.
    if !ScanDirectionIsNoMovement(direction) {
        ExecutePlan(query_desc, operation, send_tuples, count, direction, dest_handle)?;
    }

    // estate->es_total_processed += estate->es_processed;
    query_desc.with_estate_mut(|estate| {
        estate.es_total_processed += estate.es_processed;
    });

    // shutdown tuple receiver, if we started it.
    if send_tuples {
        dest::dest_rshutdown::call(dest_handle)?;
    }
    Ok(())
}

/// `ExecutePlan(...)` (execMain.c) — process the plan until `numberTuples`
/// tuples have been retrieved (0 = run to completion).
fn ExecutePlan(
    query_desc: &mut QueryDesc,
    operation: CmdType,
    send_tuples: bool,
    number_tuples: u64,
    direction: ScanDirection,
    dest_handle: DestReceiverHandle,
) -> PgResult<()> {
    // use_parallel_mode: parallel mode only supports complete execution.
    //   if (already_executed || numberTuples != 0) false;
    //   else queryDesc->plannedstmt->parallelModeNeeded;
    let parallel_mode_needed = query_desc.work.with(|w| w.plannedstmt.parallelModeNeeded);
    let use_parallel_mode =
        !(query_desc.already_executed || number_tuples != 0) && parallel_mode_needed;
    query_desc.already_executed = true;

    if use_parallel_mode {
        // estate->es_use_parallel_mode = true; EnterParallelMode();
        panic!(
            "execMain ExecutePlan: parallelModeNeeded ⇒ EnterParallelMode()/ExitParallelMode() \
             (xact parallel owner) not wired — #167 F0d"
        );
    }

    let mut current_tuple_count: u64 = 0;

    query_desc.with_estate_and_planstate_mut(|estate, planstate| {
        // estate->es_direction = direction;  estate->es_use_parallel_mode = false;
        estate.es_direction = direction;
        estate.es_use_parallel_mode = false;

        let planstate = match planstate {
            Some(ps) => ps,
            None => return Ok::<(), types_error::PgError>(()),
        };

        loop {
            // ResetPerTupleExprContext(estate): reset es_per_tuple_exprcontext if
            // one has been created (none on the plain path).
            if let Some(ecxt) = estate.es_per_tuple_exprcontext {
                execUtils_seams::reset_expr_context::call(estate, ecxt)?;
            }

            // slot = ExecProcNode(planstate).
            let slot = procnode::exec_proc_node::call(planstate, estate)?;

            // if (TupIsNull(slot)) break;
            let slot = match slot {
                Some(s) => s,
                None => break,
            };

            // If we have a junk filter, project a new clean tuple.
            //   if (estate->es_junkFilter != NULL)
            //       slot = ExecFilterJunk(estate->es_junkFilter, slot);
            let out_slot = if estate.es_junkFilter.is_some() {
                // Move the filter out to satisfy the borrow checker, run the
                // filter against the live estate, then restore it.
                let jf = estate.es_junkFilter.take().unwrap();
                let res = execJunk::ExecFilterJunk(estate, &jf, slot);
                estate.es_junkFilter = Some(jf);
                res?
            } else {
                slot
            };

            // If we are supposed to send the tuple somewhere, do so.
            //   if (!dest->receiveSlot(slot, dest)) break;
            if send_tuples {
                let cont = {
                    let slot_data = estate.slot_data(out_slot);
                    dest::dest_receive_slot::call(slot_data, dest_handle)?
                };
                if !cont {
                    break;
                }
            }

            // Count tuples processed (SELECT only).
            if operation == CmdType::CMD_SELECT {
                estate.es_processed += 1;
            }

            // check our tuple count.
            current_tuple_count += 1;
            if number_tuples != 0 && number_tuples == current_tuple_count {
                break;
            }
        }

        // If we won't need to back up, release resources now.
        //   if (!(es_top_eflags & EXEC_FLAG_BACKWARD)) ExecShutdownNode(planstate);
        if estate.es_top_eflags & EXEC_FLAG_BACKWARD == 0 {
            procnode::exec_shutdown_node::call(planstate, estate)?;
        }
        Ok(())
    })?;

    // if (use_parallel_mode) ExitParallelMode();  (unreachable: guarded above.)
    Ok(())
}

/// `ExecutorRun(queryDesc, direction, count)` — routes to
/// [`standard_ExecutorRun`].
pub fn ExecutorRun(
    query_desc: &mut QueryDesc,
    direction: ScanDirection,
    count: u64,
) -> PgResult<()> {
    standard_ExecutorRun(query_desc, direction, count)
}

// ===========================================================================
// ExecutorEnd / ExecEndPlan
// ===========================================================================

/// `standard_ExecutorEnd(queryDesc)` (execMain.c) — shut the plan-state tree
/// down and release the per-query state.
///
/// `ExecEndPlan` closes relations / drops buffer pins; dropping the
/// [`QueryDesc`] afterward releases the per-query context (the owned model's
/// `FreeExecutorState`).
pub fn standard_ExecutorEnd(query_desc: &mut QueryDesc) -> PgResult<()> {
    // Assert(estate); Assert(es_finished || EXPLAIN_ONLY). ExecEndPlan(planstate, estate).
    ExecEndPlan(query_desc)?;
    // estate->es_finished = true; (informational on the owned bundle.)
    query_desc.with_estate_mut(|estate| {
        estate.es_finished = true;
    });
    // FreeExecutorState(estate): the per-query context (and the EState/plan-state
    // tree it holds) is freed when the caller drops the QueryDesc bundle.
    Ok(())
}

/// `ExecEndPlan(planstate, estate)` (execMain.c) — close files / drop pins.
fn ExecEndPlan(query_desc: &mut QueryDesc) -> PgResult<()> {
    query_desc.with_estate_and_planstate_mut(|estate, planstate| {
        // shut down the node-type-specific query processing.
        if let Some(planstate) = planstate {
            procnode::exec_end_node::call(planstate, estate)?;
        }
        // for subplans too: foreach es_subplanstates { ExecEndNode }. InitPlan
        // guards subplans empty on this frontier, so es_subplanstates is empty;
        // guard-and-panic if a subplan state is ever present.
        if !estate.es_subplanstates.is_empty() {
            panic!(
                "execMain ExecEndPlan: ExecEndNode loop over es_subplanstates not wired \
                 (subplans are guarded out of InitPlan) — #167 F0d"
            );
        }
        Ok::<(), types_error::PgError>(())
    })?;

    // destroy the executor's tuple table (release buffer pins / tupdesc refs).
    query_desc.with_estate_mut(|estate| execUtils::ExecResetTupleTable(estate, false))?;

    // Close any Relations opened for range table / result relations.
    query_desc.with_estate_mut(execUtils::ExecCloseResultRelations)?;
    query_desc.with_estate_mut(execUtils::ExecCloseRangeTableRelations)?;
    Ok(())
}

/// `ExecutorEnd(queryDesc)` — routes to [`standard_ExecutorEnd`].
pub fn ExecutorEnd(query_desc: &mut QueryDesc) -> PgResult<()> {
    standard_ExecutorEnd(query_desc)
}

/// `ExecutorFinish(queryDesc)` (execMain.c) — run the executor's after-query
/// cleanup. For a plain `SELECT` with `EXEC_FLAG_SKIP_TRIGGERS` set there are no
/// secondary ModifyTable nodes and no AFTER triggers, so this is a no-op; the
/// non-trivial `ExecPostprocessPlan` / `AfterTriggerEndQuery` body lands with
/// the ModifyTable + trigger owners.
pub fn standard_ExecutorFinish(query_desc: &mut QueryDesc) -> PgResult<()> {
    // ExecPostprocessPlan(estate): run secondary ModifyTable nodes to completion.
    let has_aux = query_desc.work.with(|w| !w.estate.es_auxmodifytables.is_empty());
    if has_aux {
        panic!(
            "execMain standard_ExecutorFinish: ExecPostprocessPlan over es_auxmodifytables \
             (secondary ModifyTable nodes) not wired — #167 F0d"
        );
    }
    // AfterTriggerEndQuery(estate) unless SKIP_TRIGGERS — SKIP_TRIGGERS is set for
    // the plain SELECT, so it is elided (faithful no-op).
    query_desc.with_estate_mut(|estate| {
        estate.es_finished = true;
    });
    Ok(())
}

/// `ExecutorFinish(queryDesc)` — routes to [`standard_ExecutorFinish`].
pub fn ExecutorFinish(query_desc: &mut QueryDesc) -> PgResult<()> {
    standard_ExecutorFinish(query_desc)
}

/// `FreeQueryDesc(queryDesc)` — free a finished `QueryDesc` (drops the owned
/// bundle, releasing the per-query context).
pub fn FreeQueryDesc(query_desc: QueryDesc) -> PgResult<()> {
    drop(query_desc);
    Ok(())
}

/// The EXPLAIN side of `ExplainOnePlan` (explain.c): `CreateQueryDesc` (with the
/// already-pushed active snapshot, the discard receiver `None_Receiver`, and the
/// caller's `instrument_option`) followed by `ExecutorStart(queryDesc, eflags)`.
/// Returns the started [`QueryDesc`] whose plan-state tree the explain unit
/// walks. Installed for `backend_executor_execMain_seams::
/// create_query_desc_and_start_explain`.
#[allow(clippy::too_many_arguments)]
pub fn CreateQueryDescAndStartExplain(
    parent: &MemoryContext,
    plan: &PlannedStmt<'_>,
    source_text: &str,
    snapshot: Option<alloc::rc::Rc<types_snapshot::SnapshotData>>,
    params: ParamListInfoHandle,
    instrument_option: i32,
    eflags: i32,
) -> PgResult<QueryDesc> {
    // queryDesc = CreateQueryDesc(plannedstmt, queryString, GetActiveSnapshot(),
    //     InvalidSnapshot, None_Receiver, params, queryEnv, instrument_option);
    let mut query_desc = CreateQueryDesc(
        parent,
        plan,
        source_text,
        snapshot,
        None, // InvalidSnapshot crosscheck_snapshot
        DestReceiverHandle::NULL, // None_Receiver (discard)
        params,
        instrument_option,
    )?;
    // ExecutorStart(queryDesc, eflags);
    ExecutorStart(&mut query_desc, eflags)?;
    Ok(query_desc)
}

// ===========================================================================
// Result tupdesc helper (queryDesc->tupDesc)
// ===========================================================================

/// `queryDesc->tupDesc` — the result `TupleDesc` the receiver is started with:
/// the top plan node's result tupdesc, or the junk filter's cleaned tupdesc
/// when a junk filter is present. Returns a borrow tied to the EState/planstate.
fn result_tupdesc<'a, 'mcx>(
    estate: &'a types_nodes::EStateData<'mcx>,
    planstate: Option<&'a types_nodes::PlanStateNode<'mcx>>,
) -> Option<&'a types_tuple::heaptuple::TupleDescData<'mcx>> {
    if let Some(jf) = estate.es_junkFilter.as_deref() {
        return jf.jf_cleanTupType.as_deref();
    }
    planstate.and_then(|ps| ps.ps_head().ps_ResultTupleDesc.as_deref())
}

// ===========================================================================
// Permission checks (the self-contained, fully-portable surface).
// ===========================================================================

/// `ExecCheckXactReadOnly(plannedstmt)` (execMain.c) — read-only / parallel
/// gate, restricted to what the trimmed `PlannedStmt`/`RTEPermissionInfo`
/// expose.
pub fn ExecCheckXactReadOnly(plannedstmt: &PlannedStmt<'_>) {
    if plannedstmt.permInfos.as_ref().is_some_and(|p| !p.is_empty()) {
        panic!(
            "execMain ExecCheckXactReadOnly per-rel write-permission classification needs \
             RTEPermissionInfo.requiredPerms (trimmed; lands with the full ExecCheckPermissions \
             consumer) — #167 F0d"
        );
    }
    let _ = plannedstmt.commandType;
}

/// `ExecCheckOneRelPerms` for the SELECT-on-columns case (execMain.c), used by
/// the `exec_check_permissions_select` seam (`RI_Initial_Check`'s SELECT probe,
/// which passes column attnums as a slice rather than a `Bitmapset`).
fn exec_check_one_rel_perms_select(
    relid: Oid,
    selected_cols: &[i16],
    userid: Oid,
) -> PgResult<bool> {
    if aclchk::pg_class_aclcheck::call(relid, userid, ACL_SELECT)? == AclResult::AclcheckOk {
        return Ok(true);
    }

    if selected_cols.is_empty() {
        if aclchk::pg_attribute_aclcheck_all::call(
            relid,
            userid,
            ACL_SELECT,
            AclMaskHow::AclmaskAny,
        )? != AclResult::AclcheckOk
        {
            return Ok(false);
        }
        return Ok(true);
    }

    for &col in selected_cols {
        let attno = col + FirstLowInvalidHeapAttributeNumber;
        if attno == 0 {
            if aclchk::pg_attribute_aclcheck_all::call(
                relid,
                userid,
                ACL_SELECT,
                AclMaskHow::AclmaskAll,
            )? != AclResult::AclcheckOk
            {
                return Ok(false);
            }
        } else if aclchk::pg_attribute_aclcheck::call(relid, attno, userid, ACL_SELECT)?
            != AclResult::AclcheckOk
        {
            return Ok(false);
        }
    }
    Ok(true)
}

/// `ExecCheckPermissions(rangeTable, rteperminfos, ereport_on_violation)`
/// (execMain.c), restricted to the SELECT-on-relations form `RI_Initial_Check`
/// uses (the `exec_check_permissions_select` seam).
pub fn exec_check_permissions_select(
    rels: &[(Oid, u8, &[i16])],
    ereport_on_violation: bool,
) -> PgResult<bool> {
    let userid = miscinit::get_user_id::call();

    for &(relid, relkind, cols) in rels {
        let ok = exec_check_one_rel_perms_select(relid, cols, userid)?;
        if !ok {
            if ereport_on_violation {
                let objtype = objaddr::get_relkind_objtype::call(relkind);
                let name = lsyscache_get_rel_name(relid)?;
                aclchk::aclcheck_error::call(AclResult::AclcheckNoPriv, objtype, name)?;
            }
            return Ok(false);
        }
    }
    Ok(true)
}

/// `bms_is_empty(a)` (nodes/bitmapset.c) over the owned word storage: a set is
/// empty iff it is absent (C `NULL`) or every word is zero. The `bms_*`
/// constructors never leave trailing all-zero words, so this is faithful.
fn bms_is_empty(cols: &Option<mcx::PgBox<'_, Bitmapset<'_>>>) -> bool {
    match cols {
        None => true,
        Some(b) => b.words.as_slice().iter().all(|&w| w == 0),
    }
}

/// Iterate the members of a `Bitmapset *` in ascending order, mirroring the C
/// `while ((col = bms_next_member(cols, col)) >= 0)` loop. `f` receives each
/// member's bit number; `Bitmapset` bit `k` lives in word `k / BITS_PER_BITMAPWORD`
/// at position `k % BITS_PER_BITMAPWORD` (nodes/bitmapset.c numbering).
fn bms_for_each_member(
    cols: &Option<mcx::PgBox<'_, Bitmapset<'_>>>,
    mut f: impl FnMut(i32) -> PgResult<core::ops::ControlFlow<bool>>,
) -> PgResult<Option<bool>> {
    let Some(b) = cols else {
        return Ok(None);
    };
    for (wordnum, &word) in b.words.as_slice().iter().enumerate() {
        let mut w = word;
        while w != 0 {
            let bitnum = w.trailing_zeros() as usize;
            w &= w - 1;
            let member = (wordnum * BITS_PER_BITMAPWORD + bitnum) as i32;
            if let core::ops::ControlFlow::Break(early) = f(member)? {
                return Ok(Some(early));
            }
        }
    }
    Ok(None)
}

/// `ExecCheckPermissionsModified(relOid, userid, modifiedCols, requiredPerms)`
/// (execMain.c) — INSERT/UPDATE column-permission check (processed uniformly).
fn exec_check_permissions_modified(
    rel_oid: Oid,
    userid: Oid,
    modified_cols: &Option<mcx::PgBox<'_, Bitmapset<'_>>>,
    required_perms: AclMode,
) -> PgResult<bool> {
    // When the query doesn't explicitly update any columns, allow the query if
    // we have permission on any column of the rel.
    if bms_is_empty(modified_cols) {
        if aclchk::pg_attribute_aclcheck_all::call(
            rel_oid,
            userid,
            required_perms,
            AclMaskHow::AclmaskAny,
        )? != AclResult::AclcheckOk
        {
            return Ok(false);
        }
    }

    if let Some(early) = bms_for_each_member(modified_cols, |col| {
        // bit #s are offset by FirstLowInvalidHeapAttributeNumber.
        let attno = col + FirstLowInvalidHeapAttributeNumber as i32;
        if attno == 0 {
            // whole-row reference can't happen here (C: elog(ERROR)).
            panic!("execMain ExecCheckPermissionsModified: whole-row update is not implemented");
        }
        if aclchk::pg_attribute_aclcheck::call(rel_oid, attno as i16, userid, required_perms)?
            != AclResult::AclcheckOk
        {
            return Ok(core::ops::ControlFlow::Break(false));
        }
        Ok(core::ops::ControlFlow::Continue(()))
    })? {
        return Ok(early);
    }
    Ok(true)
}

/// `ExecCheckOneRelPerms(perminfo)` (execMain.c) — check access permissions for
/// a single relation.
fn exec_check_one_rel_perms(perminfo: &RTEPermissionInfo<'_>) -> PgResult<bool> {
    let rel_oid = perminfo.relid;
    let required_perms = perminfo.requiredPerms;
    debug_assert!(required_perms != 0);

    // userid to check as: current user unless we have a setuid indication.
    let userid = if types_core::primitive::OidIsValid(perminfo.checkAsUser) {
        perminfo.checkAsUser
    } else {
        miscinit::get_user_id::call()
    };

    // We must have *all* the requiredPerms bits, but some of the bits can be
    // satisfied from column-level rather than relation-level permissions.
    let rel_perms =
        aclchk::pg_class_aclmask::call(rel_oid, userid, required_perms, AclMaskHow::AclmaskAll)?;
    let remaining_perms = required_perms & !rel_perms;
    if remaining_perms != 0 {
        // If we lack any permissions that exist only as relation permissions,
        // we can fail straight away.
        if remaining_perms & !(ACL_SELECT | ACL_INSERT | ACL_UPDATE) != 0 {
            return Ok(false);
        }

        if remaining_perms & ACL_SELECT != 0 {
            // When the query doesn't explicitly reference any columns (e.g.
            // SELECT COUNT(*) FROM table), allow it if we have SELECT on any
            // column of the rel, as per SQL spec.
            if bms_is_empty(&perminfo.selectedCols)
                && aclchk::pg_attribute_aclcheck_all::call(
                    rel_oid,
                    userid,
                    ACL_SELECT,
                    AclMaskHow::AclmaskAny,
                )? != AclResult::AclcheckOk
            {
                return Ok(false);
            }

            if let Some(early) = bms_for_each_member(&perminfo.selectedCols, |col| {
                // bit #s are offset by FirstLowInvalidHeapAttributeNumber.
                let attno = col + FirstLowInvalidHeapAttributeNumber as i32;
                if attno == 0 {
                    // Whole-row reference, must have priv on all cols.
                    if aclchk::pg_attribute_aclcheck_all::call(
                        rel_oid,
                        userid,
                        ACL_SELECT,
                        AclMaskHow::AclmaskAll,
                    )? != AclResult::AclcheckOk
                    {
                        return Ok(core::ops::ControlFlow::Break(false));
                    }
                } else if aclchk::pg_attribute_aclcheck::call(
                    rel_oid,
                    attno as i16,
                    userid,
                    ACL_SELECT,
                )? != AclResult::AclcheckOk
                {
                    return Ok(core::ops::ControlFlow::Break(false));
                }
                Ok(core::ops::ControlFlow::Continue(()))
            })? {
                return Ok(early);
            }
        }

        // Basically the same for the mod columns, for both INSERT and UPDATE
        // privilege as specified by remainingPerms.
        if remaining_perms & ACL_INSERT != 0
            && !exec_check_permissions_modified(
                rel_oid,
                userid,
                &perminfo.insertedCols,
                ACL_INSERT,
            )?
        {
            return Ok(false);
        }

        if remaining_perms & ACL_UPDATE != 0
            && !exec_check_permissions_modified(
                rel_oid,
                userid,
                &perminfo.updatedCols,
                ACL_UPDATE,
            )?
        {
            return Ok(false);
        }
    }
    Ok(true)
}

/// `ExecCheckPermissions(rangeTable, rteperminfos, ereport_on_violation)`
/// (execMain.c) — check access permissions of relations mentioned in a query.
///
/// `rangeTable` is no longer used by us, but kept around for hooks (the trimmed
/// model carries it on the EState; the `ExecutorCheckPerms_hook` is not modeled
/// in the owned port, so the hook leg is omitted — faithful: with no hook
/// installed C returns `result` unchanged).
pub fn exec_check_permissions(
    rteperminfos: &[RTEPermissionInfo<'_>],
    ereport_on_violation: bool,
) -> PgResult<bool> {
    for perminfo in rteperminfos {
        debug_assert!(types_core::primitive::OidIsValid(perminfo.relid));
        let result = exec_check_one_rel_perms(perminfo)?;
        if !result {
            if ereport_on_violation {
                let relkind = lsyscache::get_rel_relkind::call(perminfo.relid)?;
                let objtype = objaddr::get_relkind_objtype::call(relkind);
                let name = lsyscache_get_rel_name(perminfo.relid)?;
                aclchk::aclcheck_error::call(AclResult::AclcheckNoPriv, objtype, name)?;
            }
            return Ok(false);
        }
    }
    Ok(true)
}

/// `get_rel_name(relid)` → owned `String` (the `aclcheck_error` objectname).
fn lsyscache_get_rel_name(relid: Oid) -> PgResult<Option<alloc::string::String>> {
    let tmp = MemoryContext::new("execMain get_rel_name");
    let out = lsyscache::get_rel_name::call(tmp.mcx(), relid)?
        .map(|s| s.as_str().to_string());
    Ok(out)
}

/// Install the seams this unit owns and is ready to install.
///
/// The driver entry points (`executor_run` / `executor_finish` / `executor_end`
/// / `free_query_desc`) plus the self-contained SELECT-permission check
/// (`exec_check_permissions_select`) are installed. The rest of
/// `backend-executor-execMain-seams` (EvalPlanQual, the constraint/partition
/// checks, `InitResultRelInfo`, the CteScan-leader operations, the COPY-query
/// setup, …) stays uninstalled (mirror-and-panic) until the matching #166/#167
/// family lands; the unit is CATALOG `needs-decomp`, so the recurrence guard
/// exempts the unfinished surface.
pub fn init_seams() {
    seams::exec_check_permissions_select::set(exec_check_permissions_select);
    seams::executor_run::set(ExecutorRun);
    seams::executor_finish::set(ExecutorFinish);
    seams::executor_end::set(ExecutorEnd);
    seams::free_query_desc::set(FreeQueryDesc);
    seams::create_query_desc_and_start_explain::set(CreateQueryDescAndStartExplain);
}
