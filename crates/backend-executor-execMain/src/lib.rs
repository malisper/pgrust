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
//! `InitPlan` now allocates `es_param_exec_vals` from `plannedstmt->paramExecTypes`
//! and runs the per-`SubPlan` `ExecInitNode` loop over `plannedstmt->subplans`,
//! populating `es_subplanstates` (the executor-owned per-init-plan state list);
//! `ExecEndPlan` tears those subplan states down in turn. The top-level subplan
//! list build is faithful; the *consumer* of `es_subplanstates` —
//! `ExecInitSubPlan` reached through a node's `Plan.initPlan` list — is still
//! gated behind the unported `Plan.initPlan` field (a `execProcnode` panic) plus
//! the `SubPlanState.planstate` two-owner re-model, so the two
//! `SubPlanState`-resolving PARAM_EXEC seams (`link_subplan_planstate`,
//! `exec_set_param_plan_for_pending`) stay seam-and-panic until that keystone
//! lands (see `DESIGN_DEBT.md`).
//!
//! The remaining branches a plain `DestNone` `SELECT` does not exercise —
//! `EXPLAIN`-only read-only/parallel gating, `parallelModeNeeded`, `rowMarks`,
//! partition pruning, RETURNING, and non-`SELECT` command types — `panic!` with a
//! precise message (mirror-pg-and-panic on a live frontier); the unit stays
//! CATALOG `needs-decomp` so the seam-install recurrence guard exempts the
//! still-open surface.

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
use types_nodes::params::ParamListInfo;
use types_nodes::parsestmt::DestReceiverHandle;
use types_nodes::querydesc::QueryDesc;
use types_scan::sdir::{ScanDirection, ScanDirectionIsNoMovement};

use backend_catalog_aclchk_seams as aclchk;
use backend_catalog_objectaddress_seams as objaddr;
use backend_nodes_core_seams as bms_seams;
use backend_utils_cache_relcache_seams as relcache;
use backend_executor_execJunk as execJunk;
use backend_executor_execMain_seams as seams;
use backend_executor_execReplication_seams as execReplication_seams;
use backend_executor_execProcnode_seams as procnode;
use backend_executor_execUtils as execUtils;
use backend_executor_execUtils_seams as execUtils_seams;
use backend_tcop_dest_seams as dest;
use backend_access_transam_xact_seams as xact_seams;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_init_miscinit_seams as miscinit;

// `FirstLowInvalidHeapAttributeNumber` (access/sysattr.h) — the column-bitmap
// offset the planner applies to selectedCols bit numbers.
use types_tuple::heaptuple::FirstLowInvalidHeapAttributeNumber;

// EState eflags (executor/executor.h). Mirrored locally; there is no canonical
// shared constant yet (other node crates likewise define them locally).
const EXEC_FLAG_REWIND: i32 = 0x0004;
const EXEC_FLAG_BACKWARD: i32 = 0x0008;
const EXEC_FLAG_MARK: i32 = 0x0010;
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
    params: ParamListInfo,
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
    let params = query_desc.params.clone();
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
    // es_output_cid is set inside the `with_mut` work-bundle borrow below; the
    // command id we need to write (if any) is computed here so the xact-owner
    // call happens outside that borrow.
    let mut es_output_cid: Option<types_core::xact::CommandId> = None;
    match operation {
        CmdType::CMD_SELECT => {
            // SELECT FOR [KEY] UPDATE/SHARE and modifying CTEs need to mark
            // tuples: es_output_cid = GetCurrentCommandId(true).
            let needs_cid = query_desc.work.with(|w| {
                w.plannedstmt
                    .rowMarks
                    .as_ref()
                    .is_some_and(|m| !m.is_empty())
                    || w.plannedstmt.hasModifyingCTE
            });
            let has_modifying_cte =
                query_desc.work.with(|w| w.plannedstmt.hasModifyingCTE);
            if needs_cid {
                es_output_cid = Some(xact_seams::get_current_command_id::call(true)?);
            }
            // A SELECT without modifying CTEs can't queue triggers.
            if !has_modifying_cte {
                eflags |= EXEC_FLAG_SKIP_TRIGGERS;
            }
        }
        CmdType::CMD_INSERT
        | CmdType::CMD_DELETE
        | CmdType::CMD_UPDATE
        | CmdType::CMD_MERGE => {
            es_output_cid = Some(xact_seams::get_current_command_id::call(true)?);
        }
        _ => {
            return Err(types_error::PgError::error(alloc::format!(
                "unrecognized operation code: {}",
                operation as i32
            )));
        }
    }

    // estate->es_snapshot = RegisterSnapshot(queryDesc->snapshot);
    // estate->es_crosscheck_snapshot = RegisterSnapshot(queryDesc->crosscheck_snapshot);
    // (execMain.c standard_ExecutorStart.) The Rc clone is the refcount-bearing
    // registration; the QueryDesc keeps the originals alive for the query's
    // lifetime. Cloned out before borrowing the work bundle.
    let es_snapshot = query_desc.snapshot.clone();
    let es_crosscheck_snapshot = query_desc.crosscheck_snapshot.clone();

    query_desc.work.with_mut(|w| {
        if let Some(cid) = es_output_cid {
            w.estate.es_output_cid = cid;
        }
        w.estate.es_param_list_info = params;
        w.estate.es_snapshot = es_snapshot.clone();
        w.estate.es_crosscheck_snapshot = es_crosscheck_snapshot.clone();
        w.estate.es_top_eflags = eflags;
        w.estate.es_instrument = instrument;
        w.estate.es_jit_flags = w.plannedstmt.jitFlags;
        // if (queryDesc->plannedstmt->paramExecTypes != NIL) {
        //     int nParamExec = list_length(...paramExecTypes);
        //     estate->es_param_exec_vals = palloc0(nParamExec * sizeof(ParamExecData));
        // }
        // The PARAM_EXEC value array is sized from the planner's paramExecTypes
        // list and zero-initialized (each slot's execPlan/value/isnull cleared);
        // the initplan loop (InitPlan) and the param-fetch machinery poke into it
        // by paramid.
        if let Some(param_exec_types) = w.plannedstmt.paramExecTypes.as_ref() {
            let n_param_exec = param_exec_types.len();
            let mcx = w.estate.es_query_cxt;
            let mut vals = mcx::vec_with_capacity_in(mcx, n_param_exec)?;
            for _ in 0..n_param_exec {
                vals.push(types_nodes::ParamExecData::default());
            }
            w.estate.es_param_exec_vals = vals;
        }
        Ok::<(), types_error::PgError>(())
    })?;

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

        // Initialize private state information for each SubPlan.  We must do this
        // before running ExecInitNode on the main query tree, since
        // ExecInitSubPlan expects to be able to find these entries.
        //   Assert(estate->es_subplanstates == NIL);
        //   i = 1;                       /* subplan indices count from 1 */
        //   foreach(l, plannedstmt->subplans) {
        //       Plan *subplan = (Plan *) lfirst(l);
        //       sp_eflags = eflags & ~(EXEC_FLAG_REWIND|EXEC_FLAG_BACKWARD|EXEC_FLAG_MARK);
        //       if (bms_is_member(i, plannedstmt->rewindPlanIDs)) sp_eflags |= EXEC_FLAG_REWIND;
        //       subplanstate = ExecInitNode(subplan, estate, sp_eflags);
        //       estate->es_subplanstates = lappend(estate->es_subplanstates, subplanstate);
        //       i++;
        //   }
        debug_assert!(estate.es_subplanstates.is_empty());
        // A subplan will never need BACKWARD scan nor MARK/RESTORE; we also strip
        // REWIND and only re-add it for parameterless subplans the planner flagged
        // in `rewindPlanIDs`. The trimmed `PlannedStmt` does not model the
        // `rewindPlanIDs` Bitmapset, so no subplan is suggested REWIND here (the
        // common case: initplans/correlated subplans are never in rewindPlanIDs).
        let sp_eflags = eflags & !(EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK);
        if let Some(subplans) = plannedstmt.subplans.take() {
            for subplan in subplans {
                // lfirst(l) — a `Plan *`; an entry can be NULL (a pruned/unused
                // subplan slot), in which case ExecInitNode(NULL, ...) yields a
                // NULL plan-state. Leak the owning box into an honest `&'mcx Node`
                // (it lives until the per-query context drops, faithful to C's
                // "plan freed with its context"), exactly like the main planTree.
                let subnode: Option<&types_nodes::nodes::Node<'_>> =
                    subplan.map(|tree| &*mcx::leak_in(tree));
                // subplanstate = ExecInitNode(subplan, estate, sp_eflags);
                // es_subplanstates = lappend(es_subplanstates, subplanstate).
                // A NULL subplan slot (pruned/unused) lappends a NULL, preserving
                // the 1-based plan_id index.
                let subplanstate =
                    procnode::exec_init_node::call(mcx, subnode, estate, sp_eflags)?;
                estate.es_subplanstates.push(subplanstate);
            }
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
            // dest->rStartup runs in the per-query arena (mcx-vtable keystone):
            // an intorel-style receiver opens its relation/BulkInsertState here.
            let mcx = estate.es_query_cxt;
            let tupdesc = result_tupdesc(estate, planstate.as_deref());
            match tupdesc {
                Some(td) => dest::dest_rstartup::call(mcx, dest_handle, operation, td),
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
    //
    // The receiver's shutdown runs in the per-query arena (mcx-vtable keystone:
    // an intorel-style receiver frees its BulkInsertState / closes its relation
    // here). The McxOwned bundle never lets an `Mcx<'mcx>` escape its closure,
    // so the dispatch is done inside `with_estate_mut`.
    if send_tuples {
        query_desc.with_estate_mut(|estate| {
            let mcx = estate.es_query_cxt;
            dest::dest_rshutdown::call(mcx, dest_handle)
        })?;
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
            // TupIsNull(slot) == (slot == NULL || TTS_EMPTY(slot)): the C
            // ExecProcNode return at end-of-scan is a *non-NULL* but cleared
            // slot (e.g. ExecScan's `return ExecClearTuple(resultslot)`), so the
            // empty-flag check is load-bearing — without it the cleared virtual
            // result slot would be sent to the dest receiver.
            let slot = match slot {
                Some(s) if !estate.slot(s).is_empty() => s,
                _ => break,
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
                    // The per-query arena the receiver works in (mcx-vtable
                    // keystone): an intorel-style receiver drives
                    // `table_tuple_insert(mcx, &rel, slot, …)` here.
                    let mcx = estate.es_query_cxt;
                    let slot_data = estate.slot_data_mut(out_slot);
                    dest::dest_receive_slot::call(mcx, slot_data, dest_handle)?
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

/// `ExecutorRewind(queryDesc)` (execMain.c) — rewind the executor to the start of
/// the query so it can be re-run. It's not sensible to rescan an updating query,
/// so the C asserts `operation == CMD_SELECT`; the work is `ExecReScan(planstate)`
/// in the per-query memory context (the owned bundle's context is already current
/// when the plan-state tree is touched).
pub fn ExecutorRewind(query_desc: &mut QueryDesc) -> PgResult<()> {
    // Assert(queryDesc->operation == CMD_SELECT).
    debug_assert!(query_desc.operation == CmdType::CMD_SELECT);
    // ExecReScan(queryDesc->planstate) — a degenerate (NULL planstate) plan has
    // nothing to rescan (the C dereferences a real planstate; a started SELECT
    // always has one).
    query_desc.with_estate_and_planstate_mut(|estate, planstate| {
        if let Some(planstate) = planstate {
            backend_executor_execAmi_seams::exec_re_scan::call(planstate, estate)?;
        }
        Ok::<(), types_error::PgError>(())
    })
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
/// The "subplan was not initialized" check (nodeSubplan.c:818-827). The
/// `SubPlanState` reaches its child plan state by the subplan's 1-based
/// `plan_id` index into `es_subplanstates`; this verifies that slot exists.
fn link_subplan_planstate(
    estate: &types_nodes::EStateData<'_>,
    plan_id: i32,
) -> PgResult<()> {
    let idx = (plan_id as usize)
        .checked_sub(1)
        .filter(|&i| i < estate.es_subplanstates.len());
    match idx {
        Some(_) => Ok(()),
        None => Err(types_error::PgError::error("subplan was not initialized")),
    }
}

fn ExecEndPlan(query_desc: &mut QueryDesc) -> PgResult<()> {
    query_desc.with_estate_and_planstate_mut(|estate, planstate| {
        // shut down the node-type-specific query processing.
        if let Some(planstate) = planstate {
            procnode::exec_end_node::call(planstate, estate)?;
        }
        // for subplans too:
        //   foreach(l, estate->es_subplanstates) {
        //       PlanState *subplanstate = (PlanState *) lfirst(l);
        //       ExecEndNode(subplanstate);
        //   }
        // es_subplanstates owns each subplan's plan-state tree (the owned model's
        // teardown owner). Move the vec out so each child box can be ended with a
        // live `&mut estate` borrow (no self-alias), then drop the emptied vec.
        let subplanstates = core::mem::replace(
            &mut estate.es_subplanstates,
            mcx::PgVec::new_in(estate.es_query_cxt),
        );
        for subplanstate in subplanstates {
            // ExecEndNode(NULL) is a no-op; only end real (non-pruned) slots.
            let Some(mut subplanstate) = subplanstate else {
                continue;
            };
            procnode::exec_end_node::call(&mut subplanstate, estate)?;
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
    params: ParamListInfo,
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

/// The COPY-(query)-TO executor setup (copyto.c:838-850): `CreateQueryDesc(plan,
/// sourceText, GetActiveSnapshot(), InvalidSnapshot, dest, NULL, NULL, 0)` then
/// `ExecutorStart(queryDesc, 0)`. The COPY-OUT `DestReceiver` is the handle the
/// caller built; the active snapshot is the copied one it has just pushed
/// (copyto.c:830-831). Installed for `create_query_desc_and_start`.
pub fn CreateQueryDescAndStartCopy(
    parent: &MemoryContext,
    plan: &PlannedStmt<'_>,
    source_text: &str,
    copy_receiver: u64,
) -> PgResult<QueryDesc> {
    // GetActiveSnapshot() — the caller pushed PushCopiedSnapshot just before.
    let snapshot = backend_utils_time_snapmgr_seams::get_active_snapshot::call()?;
    let mut query_desc = CreateQueryDesc(
        parent,
        plan,
        source_text,
        snapshot,
        None,                                     // InvalidSnapshot crosscheck_snapshot
        DestReceiverHandle(copy_receiver),        // COPY-OUT receiver
        None,                                     // NULL params
        0,                                        // instrument_options
    )?;
    // ExecutorStart(queryDesc, 0);
    ExecutorStart(&mut query_desc, 0)?;
    Ok(query_desc)
}

/// `ExecutorRun(queryDesc, ForwardScanDirection, 0)` (copyto.c:1104) for the
/// COPY-(query)-TO path. Installed for `executor_run_copy`.
pub fn ExecutorRunCopy(query_desc: &mut QueryDesc) -> PgResult<()> {
    standard_ExecutorRun(query_desc, ScanDirection::ForwardScanDirection, 0)
}

/// The COPY-(query)-TO teardown (copyto.c:1010-1012): `ExecutorFinish` +
/// `ExecutorEnd` + `FreeQueryDesc`. Installed for `end_copy_query`.
pub fn EndCopyQuery(mut query_desc: QueryDesc) -> PgResult<()> {
    ExecutorFinish(&mut query_desc)?;
    ExecutorEnd(&mut query_desc)?;
    FreeQueryDesc(query_desc)
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

// ===========================================================================
// ExecSupportsBackwardScan — execMain owns the seam decl (its caller passes the
// whole PlannedStmt); the body is execAmi's planTree walker.
// ===========================================================================

/// `ExecSupportsBackwardScan(queryDesc->plannedstmt->planTree)` (execAmi.c via
/// the portalcmds caller): does the plan tree support backward scanning?
/// Delegates to execAmi's recursive `Plan`-node walker, handed `planTree`.
pub fn exec_supports_backward_scan(plan: &PlannedStmt<'_>) -> PgResult<bool> {
    backend_executor_execAmi::exec_supports_backward_scan(plan.planTree.as_deref())
}

// ===========================================================================
// ExecUpdateLockMode (execMain.c).
// ===========================================================================

/// `ExecUpdateLockMode(estate, relinfo)` (execMain.c): the row-lock mode to
/// acquire on a conflicting tuple before updating. If no key column has been
/// modified a weaker lock is sufficient (better concurrency).
pub fn ExecUpdateLockMode<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    result_rel_info: types_nodes::RriId,
) -> PgResult<types_tableam::tableam::LockTupleMode> {
    // updatedCols = ExecGetAllUpdatedCols(relinfo, estate);
    // keyCols = RelationGetIndexAttrBitmap(relinfo->ri_RelationDesc,
    //                                      INDEX_ATTR_BITMAP_KEY);
    let mcx = estate.es_query_cxt;
    let updated_cols = execUtils::ExecGetAllUpdatedCols(estate, result_rel_info, mcx)?;

    let rel = estate
        .result_rel(result_rel_info)
        .ri_RelationDesc
        .as_ref()
        .expect("ExecUpdateLockMode: result relation must be open")
        .alias();
    let key_cols = relcache::relation_get_index_attr_bitmap::call(
        mcx,
        &rel,
        relcache::IndexAttrBitmapKind::Keys,
    )?;

    // if (bms_overlap(keyCols, updatedCols)) return LockTupleExclusive;
    if bms_seams::bms_overlap::call(key_cols.as_deref(), updated_cols.as_deref()) {
        return Ok(types_tableam::tableam::LockTupleMode::LockTupleExclusive);
    }
    // return LockTupleNoKeyExclusive;
    Ok(types_tableam::tableam::LockTupleMode::LockTupleNoKeyExclusive)
}

// ===========================================================================
// ExecGetReturningSlot / ExecGetChildToRootMap (execUtils.c) — execMain owns
// the seam decl (consumed by nodeModifyTable); the bodies are execUtils's.
// ===========================================================================

/// `ExecGetReturningSlot(estate, relInfo)` (execUtils.c): get (lazily creating)
/// the per-relation slot used to hold a tuple for RETURNING. Delegates to
/// execUtils.
fn exec_get_returning_slot<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    result_rel_info: types_nodes::RriId,
) -> PgResult<types_nodes::SlotId> {
    execUtils::ExecGetReturningSlot(estate, result_rel_info)
}

/// `ExecGetChildToRootMap(resultRelInfo)` (execUtils.c): compute lazily the
/// child→root tuple-conversion map. The seam reports whether a conversion is
/// needed (the map is `NULL` when the rowtypes already match); the map itself
/// lives on the pooled `ResultRelInfo`. Delegates to execUtils.
fn exec_get_child_to_root_map<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    result_rel_info: types_nodes::RriId,
) -> PgResult<bool> {
    Ok(execUtils::ExecGetChildToRootMap(estate, result_rel_info)?.is_some())
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
/// `InitResultRelInfo(resultRelInfo, resultRelationDesc, resultRelationIndex,
/// partition_root_rri, instrument_options)` (execMain.c) — fill in a
/// `ResultRelInfo` for the given target relation.
///
/// The owned model passes the caller-allocated `ResultRelInfo` (already in the
/// EState pool) by `&mut`, and the relation as an alias handle stored into
/// `ri_RelationDesc`. The C `MemSet(0)` is mirrored by overwriting every
/// modeled field (the trimmed `ResultRelInfo` only carries the subset the
/// ports consume).
/// `ereport(ERROR)` for an unported neighbor/field on the result-relation
/// init path.
fn unported(what: &str) -> types_error::PgError {
    types_error::PgError::error(alloc::format!(
        "backend-executor-execMain: unported neighbor/field: {what}"
    ))
}

#[allow(non_snake_case)]
fn InitResultRelInfo<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    result_rel_info: &mut types_nodes::ResultRelInfo<'mcx>,
    relation: types_rel::Relation<'mcx>,
    result_relation_index: types_core::primitive::Index,
    partition_root_rri: Option<types_nodes::RriId>,
    instrument_options: i32,
) -> PgResult<()> {
    let _ = (mcx, instrument_options);

    // MemSet(resultRelInfo, 0, sizeof(ResultRelInfo)); — start from the
    // zero-initialized shape, then set the fields below.
    *result_rel_info = types_nodes::ResultRelInfo::default();

    // resultRelInfo->ri_RangeTableIndex = resultRelationIndex;
    result_rel_info.ri_RangeTableIndex = result_relation_index;

    // resultRelInfo->ri_needLockTagTuple = IsInplaceUpdateRelation(resultRelationDesc);
    result_rel_info.ri_needLockTagTuple =
        backend_catalog_catalog::IsInplaceUpdateRelation(&relation);

    // resultRelInfo->ri_TrigDesc = CopyTriggerDesc(resultRelationDesc->trigdesc);
    // and (if non-NULL) palloc0 the per-trigger FmgrInfo / ExprState / Instr
    // arrays. The relation has no triggers on the INSERT-into-plain-table path
    // (rd_trigdesc == NULL); the trigger-carrying case needs CopyTriggerDesc
    // (trigger.c) plus the trimmed-away ri_TrigFunctions/ri_TrigWhenExprs/
    // ri_TrigInstrument arrays, which are not modeled here.
    if relation.rd_trigdesc.is_some() {
        return Err(unported(
            "InitResultRelInfo: ResultRelInfo for a relation with triggers \
             (CopyTriggerDesc + ri_TrigFunctions/ri_TrigWhenExprs are not \
             carried on the trimmed ResultRelInfo)",
        ));
    }
    // else: ri_TrigDesc / ri_has_trigdesc / ri_trig_* stay at their default
    // (None / false) — the C NULL trigger desc.

    // if (relkind == RELKIND_FOREIGN_TABLE) ri_FdwRoutine = GetFdwRoutineForRelation(...);
    // The FDW routine vtable is not carried on the trimmed ResultRelInfo;
    // ri_has_fdw_routine stays false. A foreign-table target is rejected in
    // CheckValidResultRel below, so this only matters there.

    // resultRelInfo->ri_RootResultRelInfo = partition_root_rri;
    result_rel_info.ri_RootResultRelInfo = partition_root_rri;

    // The remaining "set later if needed" fields (ri_RowIdAttNo, ri_projectNew,
    // ri_onConflict, ri_MergeActions[*], …) are already at their default
    // (zero / None / NIL) from the MemSet above.

    // ri_RelationDesc holds an alias of the relation es_relations owns (no
    // release authority).
    result_rel_info.ri_RelationDesc = Some(relation.alias());
    // The Relation `relation` is itself an alias handed in by the caller
    // (ExecGetRangeTableRelation / partition routing); drop it here, the alias
    // stored above keeps the shared data reachable.
    drop(relation);

    Ok(())
}

/// `CheckValidResultRel(resultRelInfo, operation, onConflictAction,
/// mergeActions)` (execMain.c) — verify the result relation is a valid target
/// for the command.
///
/// The seam drops `mergeActions` (the partition-routing / nodeModifyTable
/// callers pass NIL on the live paths). MERGE-action validation and the
/// view / materialized-view / foreign-table arms are gated as unported (they
/// need owners — `view_has_instead_trigger`, the FDW vtable — that are off the
/// INSERT-into-table path).
#[allow(non_snake_case)]
fn CheckValidResultRel<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    result_rel_info: types_nodes::RriId,
    operation: CmdType,
    on_conflict_action: types_nodes::nodes::OnConflictAction,
) -> PgResult<()> {
    use types_tuple::access::{
        RELKIND_FOREIGN_TABLE, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
        RELKIND_SEQUENCE, RELKIND_TOASTVALUE, RELKIND_VIEW,
    };

    let mcx = estate.es_query_cxt;
    let rri = estate.result_rel(result_rel_info);
    let result_rel = rri
        .ri_RelationDesc
        .as_ref()
        .expect("CheckValidResultRel: ResultRelInfo has no relation")
        .alias();

    // Assert(ri_needLockTagTuple == IsInplaceUpdateRelation(resultRel)); —
    // InitResultRelInfo set it, so this is an internal consistency check only.
    debug_assert_eq!(
        rri.ri_needLockTagTuple,
        backend_catalog_catalog::IsInplaceUpdateRelation(&result_rel)
    );

    let relname = result_rel.name().to_string();
    let relkind = result_rel.rd_rel.relkind;

    if relkind == RELKIND_RELATION || relkind == RELKIND_PARTITIONED_TABLE {
        // For MERGE, check each action; for others, check the operation itself.
        if operation == CmdType::CMD_MERGE {
            // mergeActions is always NIL at the live call sites (the seam drops
            // it). A non-empty list would require iterating MergeAction nodes.
            return Err(unported(
                "CheckValidResultRel: CMD_MERGE replica-identity check \
                 (mergeActions are not carried on the seam)",
            ));
        } else {
            execReplication_seams::check_cmd_replica_identity::call(mcx, &result_rel, operation)?;
        }

        // INSERT ON CONFLICT DO UPDATE additionally requires UPDATE support.
        if on_conflict_action == types_nodes::nodes::ONCONFLICT_UPDATE {
            execReplication_seams::check_cmd_replica_identity::call(
                mcx,
                &result_rel,
                CmdType::CMD_UPDATE,
            )?;
        }
        Ok(())
    } else if relkind == RELKIND_SEQUENCE {
        Err(types_error::PgError::error(alloc::format!(
            "cannot change sequence \"{relname}\""
        ))
        .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE))
    } else if relkind == RELKIND_TOASTVALUE {
        Err(types_error::PgError::error(alloc::format!(
            "cannot change TOAST relation \"{relname}\""
        ))
        .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE))
    } else if relkind == RELKIND_VIEW {
        // Okay only with a suitable INSTEAD OF trigger (view_has_instead_trigger,
        // rewriteHandler.c); else error_view_not_updatable. Both owners are off
        // the INSERT-into-table path.
        Err(unported(
            "CheckValidResultRel: writable-view target \
             (view_has_instead_trigger is not wired here)",
        ))
    } else if relkind == RELKIND_MATVIEW {
        // Okay only when MatViewIncrementalMaintenanceIsEnabled().
        Err(unported(
            "CheckValidResultRel: materialized-view target \
             (MatViewIncrementalMaintenanceIsEnabled is not wired here)",
        ))
    } else if relkind == RELKIND_FOREIGN_TABLE {
        // Okay only if the FDW supports the operation; the FDW routine vtable is
        // not carried on the trimmed ResultRelInfo.
        Err(unported(
            "CheckValidResultRel: foreign-table target \
             (FdwRoutine vtable is not carried on ResultRelInfo)",
        ))
    } else {
        Err(types_error::PgError::error(alloc::format!(
            "cannot change relation \"{relname}\""
        ))
        .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE))
    }
}

/// `EvalPlanQualInit(epqstate, parentestate, subplan, auxrowmarks, epqParam,
/// resultRelations)` (execMain.c) — initialize an `EPQState` with the data that
/// does not change over its lifetime, leaving the EPQ machinery inactive.
///
/// The owned `EPQState` trims `parentestate` / `plan` / `arowMarks` /
/// `origslot` / `recheckestate` / `recheckplanstate` (the executor threads the
/// EState explicitly and the recheck-exec sub-tree is not built until
/// `EvalPlanQualBegin`). So this records `epqParam` / `resultRelations`,
/// pre-allocates the `relsubs_slot` array (`rtsize` NULL entries), and marks
/// the state inactive (all dynamic arrays `None`).
#[allow(non_snake_case)]
fn EvalPlanQualInit<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    epqstate: &mut types_nodes::modifytable::EPQState<'mcx>,
    parentestate: &mut types_nodes::EStateData<'mcx>,
    epq_param: i32,
    result_relations: &[types_core::primitive::Index],
) -> PgResult<()> {
    // Index rtsize = parentestate->es_range_table_size;
    let rtsize = parentestate.es_range_table_size;

    // epqstate->epqParam = epqParam;
    epqstate.epqParam = epq_param;

    // epqstate->resultRelations = resultRelations; (integer list of RT indexes)
    if result_relations.is_empty() {
        epqstate.resultRelations = None;
    } else {
        let mut rr = mcx::vec_with_capacity_in(mcx, result_relations.len())?;
        for &rti in result_relations {
            rr.push(rti as i32);
        }
        epqstate.resultRelations = Some(rr);
    }

    // epqstate->tuple_table = NIL; (trimmed)
    // epqstate->relsubs_slot = palloc0(rtsize * sizeof(TupleTableSlot *));
    let mut slots = mcx::vec_with_capacity_in(mcx, rtsize)?;
    slots.resize(rtsize, None);
    epqstate.relsubs_slot = Some(slots);

    // epqstate->plan = subplan;            (trimmed — set by EvalPlanQualSetPlan)
    // epqstate->arowMarks = auxrowmarks;   (trimmed)

    // Mark the EPQ state inactive:
    //   origslot/recheckestate/recheckplanstate = NULL; (trimmed)
    //   relsubs_rowmark/relsubs_done/relsubs_blocked = NULL;
    epqstate.relsubs_rowmark = None;
    epqstate.relsubs_done = None;
    epqstate.relsubs_blocked = None;

    Ok(())
}

/// The `arowmarks` build loop + `EvalPlanQualSetPlan(epqstate, subplan,
/// arowmarks)` of `ExecInitModifyTable` (execMain.c).
///
/// `EvalPlanQualSetPlan` shuts down any live EPQ query (`EvalPlanQualEnd` — a
/// no-op on the trimmed `EPQState`, whose `recheckestate`/`tuple_table` are not
/// modeled) and records the recheck plan + aux-rowmark list. The owned
/// `EPQState` trims `plan` and `arowMarks` (rebuilt from the plan in
/// `EvalPlanQualBegin`), so the recording is a no-op residue. Building the
/// aux-rowmark list from a non-empty `rowMarks` needs the `PlanRowMark` plan
/// node and `ExecFindRowMark` / `ExecBuildAuxRowMark` (execMain's EPQ
/// aux-rowmark machinery), gated until that lands; an empty / parent-only /
/// pruned `rowMarks` list is the live INSERT/UPDATE/DELETE path (NIL arowmarks).
#[allow(non_snake_case)]
fn eval_plan_qual_set_plan_with_row_marks<'mcx>(
    _mcx: mcx::Mcx<'mcx>,
    _epqstate: &mut types_nodes::modifytable::EPQState<'mcx>,
    _estate: &mut types_nodes::EStateData<'mcx>,
    row_marks: &[mcx::PgBox<'mcx, types_nodes::nodes::Node<'mcx>>],
    _subplan: Option<&'mcx types_nodes::nodes::Node<'mcx>>,
) -> PgResult<()> {
    // foreach(l, node->rowMarks) { ...ExecFindRowMark / ExecBuildAuxRowMark... }
    if !row_marks.is_empty() {
        return Err(unported(
            "ExecInitModifyTable arowmarks loop: PlanRowMark / ExecFindRowMark / \
             ExecBuildAuxRowMark (EPQ aux-rowmark machinery)",
        ));
    }

    // EvalPlanQualSetPlan(epqstate, subplan, NIL):
    //   EvalPlanQualEnd(epqstate);   — no-op on the trimmed EPQState
    //   epqstate->plan = subplan;    — `plan` is trimmed
    //   epqstate->arowMarks = NIL;   — `arowMarks` is trimmed
    Ok(())
}

/// `ExecBuildSlotValueDescription(reloid, slot, tupdesc, modifiedCols,
/// maxfieldlen)` (execMain.c) — build a "(col, ...) = (val, ...)" description of
/// the slot's contents, limited to columns the current user may SELECT (plus any
/// the caller supplied data for). `Ok(None)` when RLS is active or the user may
/// see no column (the C `NULL`).
///
/// The slot is passed by shared reference, so it is assumed already
/// deconstructed (every repo call site deforms first — logical-replication apply
/// materializes a virtual slot, `ExecConstraints`/`ExecPartitionCheck` evaluate
/// against a materialized scan tuple); the C `slot_getallattrs(slot)` is the
/// idempotent no-op for an already-deformed slot.
fn ExecBuildSlotValueDescription<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    reloid: Oid,
    slot: &types_nodes::TupleTableSlot,
    tupdesc: &types_tuple::heaptuple::TupleDescData<'_>,
    modified_cols: Option<&Bitmapset<'_>>,
    maxfieldlen: i32,
) -> PgResult<Option<mcx::PgString<'mcx>>> {
    use types_acl::acl::CheckEnableRlsResult;

    // If RLS is enabled and active for the relation, return nothing.
    if backend_utils_misc_more_seams::check_enable_rls::call(reloid, types_core::InvalidOid, true)?
        == CheckEnableRlsResult::RlsEnabled
    {
        return Ok(None);
    }

    let mut buf = mcx::PgString::new_in(mcx);
    let mut collist = mcx::PgString::new_in(mcx);
    let mut write_comma = false;
    let mut write_comma_collist = false;
    let mut any_perm = false;

    buf.try_push_str("(")?;

    // Table-level SELECT allows all columns; otherwise check each.
    let aclresult = aclchk::pg_class_aclcheck::call(reloid, miscinit::get_user_id::call(), ACL_SELECT)?;
    let table_perm = aclresult == AclResult::AclcheckOk;
    if table_perm {
        any_perm = true;
    } else {
        collist.try_push_str("(")?;
    }

    let natts = tupdesc.natts;
    let mut i: i32 = 0;
    while i < natts {
        let att = tupdesc.attr(i as usize);

        // Ignore dropped columns.
        if att.attisdropped {
            i += 1;
            continue;
        }

        let mut column_perm = false;
        if !table_perm {
            // No table-level SELECT: include the column only if the user has
            // SELECT on it or provided its data.
            let aclresult = aclchk::pg_attribute_aclcheck::call(
                reloid,
                att.attnum,
                miscinit::get_user_id::call(),
                ACL_SELECT,
            )?;
            if bms_seams::bms_is_member::call(
                att.attnum as i32 - FirstLowInvalidHeapAttributeNumber as i32,
                modified_cols,
            ) || aclresult == AclResult::AclcheckOk
            {
                column_perm = true;
                any_perm = true;
                if write_comma_collist {
                    collist.try_push_str(", ")?;
                } else {
                    write_comma_collist = true;
                }
                collist.try_push_str(&alloc::string::String::from_utf8_lossy(att.attname.name_str()))?;
            }
        }

        if table_perm || column_perm {
            // The value text (a `String`-shaped owned bytes in `mcx`).
            let val: alloc::vec::Vec<u8> = if att.attgenerated == types_tuple::access::ATTRIBUTE_GENERATED_VIRTUAL {
                alloc::vec::Vec::from(&b"virtual"[..])
            } else if slot.tts_isnull.get(i as usize).copied().unwrap_or(true) {
                alloc::vec::Vec::from(&b"null"[..])
            } else {
                let (foutoid, _typisvarlena) =
                    lsyscache::get_type_output_info::call(att.atttypid)?;
                let datum = slot
                    .tts_values
                    .get(i as usize)
                    .ok_or_else(|| unported("ExecBuildSlotValueDescription: slot not deformed"))?;
                let out = backend_utils_fmgr_fmgr_seams::oid_output_function_call::call(mcx, foutoid, datum)?;
                // PgVec<u8> NUL-terminated cstring; strip the trailing NUL if any.
                let mut v: alloc::vec::Vec<u8> = out.iter().copied().collect();
                if v.last() == Some(&0) {
                    v.pop();
                }
                v
            };

            if write_comma {
                buf.try_push_str(", ")?;
            } else {
                write_comma = true;
            }

            // Truncate if needed (by bytes, respecting multibyte boundaries).
            let vallen = val.len() as i32;
            if vallen <= maxfieldlen {
                buf.try_push_str(&alloc::string::String::from_utf8_lossy(&val))?;
            } else {
                let clip = backend_utils_mb_mbutils_seams::pg_mbcliplen::call(&val, vallen, maxfieldlen);
                buf.try_push_str(&alloc::string::String::from_utf8_lossy(&val[..clip as usize]))?;
                buf.try_push_str("...")?;
            }
        }
        i += 1;
    }

    // If we end up with zero columns being returned, return NULL.
    if !any_perm {
        return Ok(None);
    }

    buf.try_push_str(")")?;

    if !table_perm {
        collist.try_push_str(") = ")?;
        // Append buf into collist (copy buf out first to avoid a double borrow).
        let buf_str = buf.as_str().to_string();
        collist.try_push_str(&buf_str)?;
        return Ok(Some(collist));
    }

    Ok(Some(buf))
}

// ===========================================================================
//  Constraint cluster (execMain.c): ExecRelCheck / ExecPartitionCheck /
//  ExecPartitionCheckEmitError / ExecConstraints / ExecRelGenVirtualNotNull /
//  ReportNotNullViolationError, de-handled onto the owned EState slot/RRI
//  pools. The cached constraint `ExprState`s live on the owned `ResultRelInfo`
//  (`ri_CheckConstraintExprs` / `ri_PartitionCheckExpr` /
//  `ri_GenVirtualNotNullConstraintExprs`); the take-evaluate-put-back idiom
//  mirrors nodeModifyTable's `ri_GeneratedExprs*` handling so a `&mut ExprState`
//  and `&mut EStateData` can co-exist (C shares the pointer freely).
//
//  Expression *compilation* (ExecPrepareExpr / ExecPrepareCheck) bottoms out on
//  the unported `expression_planner` (#159) and loud-panics there; the
//  not-null slot-read path (the common COPY/INSERT case for relations without
//  CHECK / partition / virtual-generated constraints) needs no compilation and
//  runs end-to-end.
// ===========================================================================

use backend_executor_execExpr::execExpr_core as execExpr;
use backend_utils_error::ereport;
use types_error::error::{ERRCODE_CHECK_VIOLATION, ERRCODE_NOT_NULL_VIOLATION, ERROR};
use types_tuple::heaptuple::TupleDescData;

/// `ExecRelCheck(resultRelInfo, slot, estate)` (execMain.c): evaluate the
/// relation's CHECK constraints against `slot`. Returns the name of the first
/// failing constraint, or `None` (the C `NULL`) when all pass.
fn ExecRelCheck<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    result_rel_info: types_nodes::RriId,
    slot: types_nodes::SlotId,
) -> PgResult<Option<mcx::PgString<'mcx>>> {
    let mcx = estate.es_query_cxt;

    // Snapshot the relation's check list (ccname / ccbin / ccenforced) and the
    // pg_class relchecks count. The C dereferences ccname/ccbin
    // unconditionally for enforced checks.
    struct CheckInfo<'m> {
        ccname: mcx::PgString<'m>,
        ccbin: mcx::PgString<'m>,
        ccenforced: bool,
    }
    let (checks, num_check, relname): (alloc::vec::Vec<CheckInfo<'mcx>>, i32, alloc::string::String) = {
        let rri = estate.result_rel(result_rel_info);
        let rel = rri
            .ri_RelationDesc
            .as_ref()
            .expect("ExecRelCheck: ri_RelationDesc is NULL");
        let mut v: alloc::vec::Vec<CheckInfo<'mcx>> = alloc::vec::Vec::new();
        let mut num_check = 0i32;
        if let Some(constr) = rel.rd_att.constr.as_ref() {
            num_check = constr.num_check as i32;
            for ck in constr.check.iter() {
                let ccname = ck
                    .ccname
                    .as_ref()
                    .expect("pg_constraint CHECK has ccname")
                    .clone_in(mcx)?;
                let ccbin = ck
                    .ccbin
                    .as_ref()
                    .expect("pg_constraint CHECK has ccbin")
                    .clone_in(mcx)?;
                v.push(CheckInfo {
                    ccname,
                    ccbin,
                    ccenforced: ck.ccenforced,
                });
            }
        }
        (v, num_check, rel.name().to_string())
    };
    let ncheck = checks.len() as i32;

    // CheckNNConstraintFetch let this pass with only a warning; we fail rather
    // than risk not enforcing an important constraint. The owned model's
    // `rd_att->constr` is the parsed source for both counts, so this compares
    // the materialized check list against `constr->num_check`.
    if ncheck != num_check {
        return Err(ereport(ERROR)
            .errmsg_internal(alloc::format!(
                "{} pg_constraint record(s) missing for relation \"{}\"",
                num_check - ncheck,
                relname
            ))
            .into_error());
    }

    // If first time through, build the per-constraint ExprStates (kept in the
    // query-lifespan context, owned by the arena ResultRelInfo).
    if estate
        .result_rel(result_rel_info)
        .ri_CheckConstraintExprs
        .is_none()
        && ncheck > 0
    {
        let rel_alias = estate
            .result_rel(result_rel_info)
            .ri_RelationDesc
            .as_ref()
            .expect("ExecRelCheck: ri_RelationDesc is NULL")
            .alias();
        let mut exprs: mcx::PgVec<'mcx, Option<mcx::PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>>> =
            mcx::vec_with_capacity_in(mcx, ncheck as usize)?;
        for ci in checks.iter() {
            // Skip not-enforced constraints (leaving a None placeholder so the
            // list stays index-aligned with the check list).
            if !ci.ccenforced {
                exprs.push(None);
                continue;
            }
            // checkconstr = stringToNode(check[i].ccbin);
            let node = backend_nodes_core::read::string_to_node(mcx, ci.ccbin.as_str())?;
            let checkconstr_expr = match node.as_expr() {
                Some(e) => e.clone(),
                None => return Err(unported_node("ExecRelCheck: ccbin is not an Expr", &node)),
            };
            // checkconstr = expand_generated_columns_in_expr(checkconstr, rel, 1);
            let expanded = backend_rewrite_rewritehandler::expand_generated_columns_in_expr(
                mcx,
                Some(checkconstr_expr),
                &rel_alias,
                1,
            )?;
            let expanded_expr = expanded
                .expect("expand_generated_columns_in_expr returned NULL for a non-NULL Expr");
            exprs.push(Some(execExpr::exec_prepare_expr(&expanded_expr, estate)?));
        }
        estate
            .result_rel_mut(result_rel_info)
            .ri_CheckConstraintExprs = Some(exprs);
    }

    // econtext = GetPerTupleExprContext(estate); econtext->ecxt_scantuple = slot;
    let econtext = execUtils::MakePerTupleExprContext(estate)?;
    estate.ecxt_mut(econtext).ecxt_scantuple = Some(slot);

    // Evaluate each constraint (NULL counts as success: use ExecCheck).
    for (i, ci) in checks.iter().enumerate() {
        // Take the owned ExprState out, evaluate, put it back.
        let state = estate
            .result_rel_mut(result_rel_info)
            .ri_CheckConstraintExprs
            .as_mut()
            .and_then(|arr| arr.get_mut(i))
            .and_then(|slot| slot.take());
        if let Some(mut state) = state {
            let ok = execExpr::exec_check(Some(&mut state), econtext, estate)?;
            estate
                .result_rel_mut(result_rel_info)
                .ri_CheckConstraintExprs
                .as_mut()
                .expect("ri_CheckConstraintExprs initialized above")[i] = Some(state);
            if !ok {
                return Ok(Some(ci.ccname.clone_in(mcx)?));
            }
        }
    }

    Ok(None)
}

/// `ExecPartitionCheck(resultRelInfo, slot, estate, emitError)` (execMain.c):
/// check that the tuple in `slot` meets the relation's partition constraint.
pub fn ExecPartitionCheck<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    result_rel_info: types_nodes::RriId,
    slot: types_nodes::SlotId,
    emit_error: bool,
) -> PgResult<bool> {
    let mcx = estate.es_query_cxt;

    // If first time through, build the partition-check expression state
    // (owned by the arena ResultRelInfo, query-lifespan).
    if estate
        .result_rel(result_rel_info)
        .ri_PartitionCheckExpr
        .is_none()
    {
        let rel_alias = estate
            .result_rel(result_rel_info)
            .ri_RelationDesc
            .as_ref()
            .expect("ExecPartitionCheck: ri_RelationDesc is NULL")
            .alias();
        // qual = RelationGetPartitionQual(rel);
        let qual = backend_utils_cache_partcache::RelationGetPartitionQual(mcx, &rel_alias)?;
        // ExecPrepareCheck takes an implicit-AND Expr list.
        let mut exprs: alloc::vec::Vec<types_nodes::primnodes::Expr> = alloc::vec::Vec::new();
        for n in qual.iter() {
            match n.as_expr() {
                Some(e) => exprs.push(e.clone()),
                None => {
                    return Err(unported_node(
                        "ExecPartitionCheck: partition qual element is not an Expr",
                        n,
                    ))
                }
            }
        }
        let expr = execExpr::exec_prepare_check(&exprs, estate)?;
        estate
            .result_rel_mut(result_rel_info)
            .ri_PartitionCheckExpr = expr;
    }

    // econtext = GetPerTupleExprContext(estate); econtext->ecxt_scantuple = slot;
    let econtext = execUtils::MakePerTupleExprContext(estate)?;
    estate.ecxt_mut(econtext).ecxt_scantuple = Some(slot);

    // success = ExecCheck(ri_PartitionCheckExpr, econtext); (NULL == success)
    let state = estate
        .result_rel_mut(result_rel_info)
        .ri_PartitionCheckExpr
        .take();
    let success = match state {
        None => execExpr::exec_check(None, econtext, estate)?,
        Some(mut state) => {
            let ok = execExpr::exec_check(Some(&mut state), econtext, estate)?;
            estate
                .result_rel_mut(result_rel_info)
                .ri_PartitionCheckExpr = Some(state);
            ok
        }
    };

    // If asked to emit an error, don't return on failure.
    if !success && emit_error {
        ExecPartitionCheckEmitError(estate, result_rel_info, slot)?;
    }

    Ok(success)
}

/// `ExecPartitionCheckEmitError(resultRelInfo, slot, estate)` (execMain.c):
/// form and emit the error after a failed partition-constraint check.
pub fn ExecPartitionCheckEmitError<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    result_rel_info: types_nodes::RriId,
    slot: types_nodes::SlotId,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;

    // If the tuple was routed, convert it back to the root rowtype so val_desc
    // matches the input tuple.
    let (root_relid, slot, relname) =
        convert_routed_slot_for_error(estate, result_rel_info, slot)?;
    let modified_cols = constraint_modified_cols(estate, result_rel_info)?;

    let val_desc = build_slot_value_desc(estate, root_relid, slot, modified_cols.as_deref())?;

    let mut err = ereport(ERROR)
        .errcode(ERRCODE_CHECK_VIOLATION)
        .errmsg(alloc::format!(
            "new row for relation \"{}\" violates partition constraint",
            relname
        ));
    if let Some(vd) = val_desc {
        err = err.errdetail(alloc::format!("Failing row contains {}.", vd.as_str()));
    }
    Err(err.into_error())
}

/// `ExecConstraints(resultRelInfo, slot, estate)` (execMain.c): check the
/// relation's NOT NULL and CHECK constraints against `slot`. The partition
/// constraint is *not* checked here.
fn ExecConstraints<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    result_rel_info: types_nodes::RriId,
    slot: types_nodes::SlotId,
) -> PgResult<()> {
    // Snapshot the not-null / has-not-null / relchecks shape and per-attribute
    // (attnotnull, attgenerated) flags.
    struct AttInfo {
        attnum: i32,
        attnotnull: bool,
        is_virtual_generated: bool,
    }
    let (has_constr, has_not_null, natts, atts, relchecks): (bool, bool, i32, alloc::vec::Vec<AttInfo>, i32) = {
        let rri = estate.result_rel(result_rel_info);
        let rel = rri
            .ri_RelationDesc
            .as_ref()
            .expect("ExecConstraints: ri_RelationDesc is NULL");
        let tupdesc = &*rel.rd_att;
        let constr = tupdesc.constr.as_ref();
        let has_not_null = constr.map(|c| c.has_not_null).unwrap_or(false);
        let natts = tupdesc.natts;
        let mut atts: alloc::vec::Vec<AttInfo> = alloc::vec::Vec::new();
        if has_not_null {
            for a in 0..natts {
                let att = tupdesc.attr(a as usize);
                atts.push(AttInfo {
                    attnum: (a + 1),
                    attnotnull: att.attnotnull,
                    is_virtual_generated: att.attgenerated
                        == types_tuple::access::ATTRIBUTE_GENERATED_VIRTUAL,
                });
            }
        }
        let num_check = constr.map(|c| c.num_check as i32).unwrap_or(0);
        (constr.is_some(), has_not_null, natts, atts, num_check)
    };

    // We should not be called otherwise.
    debug_assert!(has_constr);

    // Verify not-null constraints (collecting virtual generated columns).
    let mut notnull_virtual_attrs: alloc::vec::Vec<i32> = alloc::vec::Vec::new();
    if has_not_null {
        // Deform the slot once so slot_attisnull reads are valid.
        let nulls = slot_isnulls(estate, slot, natts as usize)?;
        for ai in atts.iter() {
            if ai.attnotnull && ai.is_virtual_generated {
                notnull_virtual_attrs.push(ai.attnum);
            } else if ai.attnotnull && nulls[(ai.attnum - 1) as usize] {
                ReportNotNullViolationError(estate, result_rel_info, slot, ai.attnum)?;
            }
        }
    }

    // Verify not-null constraints on virtual generated columns, if any.
    if !notnull_virtual_attrs.is_empty() {
        let attnum =
            ExecRelGenVirtualNotNull(estate, result_rel_info, slot, &notnull_virtual_attrs)?;
        if attnum != 0 {
            ReportNotNullViolationError(estate, result_rel_info, slot, attnum)?;
        }
    }

    // Verify check constraints.
    if relchecks > 0 {
        if let Some(failed) = ExecRelCheck(estate, result_rel_info, slot)? {
            let orig_relname = {
                let rel = estate
                    .result_rel(result_rel_info)
                    .ri_RelationDesc
                    .as_ref()
                    .expect("ExecConstraints: ri_RelationDesc is NULL");
                rel.name().to_string()
            };
            // If routed, convert back to the root rowtype for the message.
            let (reloid, slot, _relname) =
                convert_routed_slot_for_error(estate, result_rel_info, slot)?;
            let modified_cols = constraint_modified_cols(estate, result_rel_info)?;
            let val_desc =
                build_slot_value_desc(estate, reloid, slot, modified_cols.as_deref())?;

            let mut err = ereport(ERROR)
                .errcode(ERRCODE_CHECK_VIOLATION)
                .errmsg(alloc::format!(
                    "new row for relation \"{}\" violates check constraint \"{}\"",
                    orig_relname,
                    failed.as_str()
                ));
            if let Some(vd) = val_desc {
                err = err.errdetail(alloc::format!("Failing row contains {}.", vd.as_str()));
            }
            return Err(err.into_error());
        }
    }

    Ok(())
}

/// `ExecRelGenVirtualNotNull(resultRelInfo, slot, estate, notnull_virtual_attrs)`
/// (execMain.c): verify not-null constraints on virtual generated columns.
/// Returns `InvalidAttrNumber` (0) when all satisfied, else the violating attnum.
fn ExecRelGenVirtualNotNull<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    result_rel_info: types_nodes::RriId,
    slot: types_nodes::SlotId,
    notnull_virtual_attrs: &[i32],
) -> PgResult<i32> {
    let mcx = estate.es_query_cxt;

    // Build a NullTest ExprState per virtual generated column (cached, owned by
    // the arena ResultRelInfo).
    if estate
        .result_rel(result_rel_info)
        .ri_GenVirtualNotNullConstraintExprs
        .is_none()
        && !notnull_virtual_attrs.is_empty()
    {
        let rel_alias = estate
            .result_rel(result_rel_info)
            .ri_RelationDesc
            .as_ref()
            .expect("ExecRelGenVirtualNotNull: ri_RelationDesc is NULL")
            .alias();
        let mut exprs: mcx::PgVec<'mcx, Option<mcx::PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>>> =
            mcx::vec_with_capacity_in(mcx, notnull_virtual_attrs.len())?;
        for &attnum in notnull_virtual_attrs {
            // "generated_expression IS NOT NULL".
            let arg = backend_rewrite_rewritehandler::build_generation_expression(
                mcx, &rel_alias, attnum,
            )?;
            let nnulltest = types_nodes::primnodes::NullTest {
                arg: Some(alloc::boxed::Box::new(arg)),
                nulltesttype: types_nodes::primnodes::NullTestType::IS_NOT_NULL,
                argisrow: false,
                location: -1,
            };
            let expr = types_nodes::primnodes::Expr::NullTest(nnulltest);
            exprs.push(Some(execExpr::exec_prepare_expr(&expr, estate)?));
        }
        estate
            .result_rel_mut(result_rel_info)
            .ri_GenVirtualNotNullConstraintExprs = Some(exprs);
    }

    // econtext = GetPerTupleExprContext(estate); econtext->ecxt_scantuple = slot;
    let econtext = execUtils::MakePerTupleExprContext(estate)?;
    estate.ecxt_mut(econtext).ecxt_scantuple = Some(slot);

    // Evaluate each virtual-not-null check.
    for (i, &attnum) in notnull_virtual_attrs.iter().enumerate() {
        let state = estate
            .result_rel_mut(result_rel_info)
            .ri_GenVirtualNotNullConstraintExprs
            .as_mut()
            .and_then(|arr| arr.get_mut(i))
            .and_then(|s| s.take());
        let mut state = state.expect("ExecRelGenVirtualNotNull: cached ExprState is NULL");
        let ok = execExpr::exec_check(Some(&mut state), econtext, estate)?;
        estate
            .result_rel_mut(result_rel_info)
            .ri_GenVirtualNotNullConstraintExprs
            .as_mut()
            .expect("ri_GenVirtualNotNullConstraintExprs initialized above")[i] = Some(state);
        if !ok {
            return Ok(attnum);
        }
    }

    Ok(0)
}

/// `ReportNotNullViolationError(resultRelInfo, slot, estate, attnum)`
/// (execMain.c): report an already-detected not-null violation for `attnum`.
fn ReportNotNullViolationError<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    result_rel_info: types_nodes::RriId,
    slot: types_nodes::SlotId,
    attnum: i32,
) -> PgResult<()> {
    debug_assert!(attnum > 0);

    let (orig_relname, attname) = {
        let rel = estate
            .result_rel(result_rel_info)
            .ri_RelationDesc
            .as_ref()
            .expect("ReportNotNullViolationError: ri_RelationDesc is NULL");
        let att = rel.rd_att.attr((attnum - 1) as usize);
        (
            rel.name().to_string(),
            alloc::string::String::from_utf8_lossy(att.attname.name_str()).into_owned(),
        )
    };

    // If routed, convert back to the root rowtype for the message.
    let (reloid, slot, _relname) =
        convert_routed_slot_for_error(estate, result_rel_info, slot)?;
    let modified_cols = constraint_modified_cols(estate, result_rel_info)?;
    let val_desc = build_slot_value_desc(estate, reloid, slot, modified_cols.as_deref())?;

    let mut err = ereport(ERROR)
        .errcode(ERRCODE_NOT_NULL_VIOLATION)
        .errmsg(alloc::format!(
            "null value in column \"{}\" of relation \"{}\" violates not-null constraint",
            attname,
            orig_relname
        ));
    if let Some(vd) = val_desc {
        err = err.errdetail(alloc::format!("Failing row contains {}.", vd.as_str()));
    }
    Err(err.into_error())
}

// --- constraint-cluster helpers -------------------------------------------

fn unported_node(msg: &str, node: &types_nodes::nodes::Node<'_>) -> types_error::PgError {
    unported(&alloc::format!("{msg}: {:?}", core::mem::discriminant(node)))
}

/// Deform `slot` and return its per-attribute null flags (`slot_attisnull`).
fn slot_isnulls<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    slot: types_nodes::SlotId,
    natts: usize,
) -> PgResult<alloc::vec::Vec<bool>> {
    let deformed = backend_executor_execTuples_seams::slot_getallattrs_by_id::call(estate, slot)?;
    let mut nulls = alloc::vec::Vec::with_capacity(natts);
    for (_v, n) in deformed.iter() {
        nulls.push(*n);
    }
    // A short slot reads remaining attributes as NULL (slot_attisnull contract).
    while nulls.len() < natts {
        nulls.push(true);
    }
    Ok(nulls)
}

/// `bms_union(ExecGetInsertedCols(rri, estate), ExecGetUpdatedCols(rri, estate))`
/// over the (root, when routed) result relation for the error `val_desc`.
fn constraint_modified_cols<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    result_rel_info: types_nodes::RriId,
) -> PgResult<Option<mcx::PgBox<'mcx, Bitmapset<'mcx>>>> {
    let mcx = estate.es_query_cxt;
    // C unions over the root RRI when the tuple was routed; otherwise the RRI.
    let target = estate
        .result_rel(result_rel_info)
        .ri_RootResultRelInfo
        .unwrap_or(result_rel_info);
    let inserted = execUtils::ExecGetInsertedCols(estate, target, mcx)?;
    let updated = execUtils::ExecGetUpdatedCols(estate, target, mcx)?;
    backend_nodes_core::bitmapset::bms_union(mcx, inserted.as_deref(), updated.as_deref())
}

/// Build the `val_desc` for a constraint error: deform the slot, then call
/// [`ExecBuildSlotValueDescription`] against the (root, when routed) relation's
/// descriptor.
fn build_slot_value_desc<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    reloid: Oid,
    slot: types_nodes::SlotId,
    modified_cols: Option<&Bitmapset<'_>>,
) -> PgResult<Option<mcx::PgString<'mcx>>> {
    let mcx = estate.es_query_cxt;
    // slot_getallattrs(slot): make the value/null arrays valid for the reader.
    let _ = backend_executor_execTuples_seams::slot_getallattrs_by_id::call(estate, slot)?;
    // Clone the descriptor + a value snapshot so the borrow of `estate` ends
    // before ExecBuildSlotValueDescription reads it (it takes &TupleTableSlot).
    let tupdesc = estate
        .slot(slot)
        .tts_tupleDescriptor
        .as_ref()
        .expect("build_slot_value_desc: slot has no descriptor")
        .clone_in(mcx)?;
    let slot_ref = estate.slot(slot);
    ExecBuildSlotValueDescription(mcx, reloid, slot_ref, &tupdesc, modified_cols, 64)
}

/// If the tuple was routed (the RRI has a root), convert `slot` back to the
/// root table's rowtype so the error `val_desc` matches the input tuple.
/// Returns `(reloid_for_val_desc, slot_to_describe, relname_of_root_or_self)`.
fn convert_routed_slot_for_error<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    result_rel_info: types_nodes::RriId,
    slot: types_nodes::SlotId,
) -> PgResult<(Oid, types_nodes::SlotId, alloc::string::String)> {
    let mcx = estate.es_query_cxt;
    let root = estate.result_rel(result_rel_info).ri_RootResultRelInfo;
    let Some(root) = root else {
        // Not routed: describe against this relation directly.
        let rel = estate
            .result_rel(result_rel_info)
            .ri_RelationDesc
            .as_ref()
            .expect("convert_routed_slot_for_error: ri_RelationDesc is NULL");
        return Ok((rel.rd_id, slot, rel.name().to_string()));
    };

    // map = build_attrmap_by_name_if_req(old_tupdesc, root_tupdesc, false);
    let old_tupdesc: TupleDescData<'mcx> = estate
        .result_rel(result_rel_info)
        .ri_RelationDesc
        .as_ref()
        .expect("convert_routed_slot_for_error: ri_RelationDesc is NULL")
        .rd_att
        .clone_in(mcx)?;
    let (root_relid, root_relname, root_tupdesc): (Oid, alloc::string::String, TupleDescData<'mcx>) = {
        let root_rel = estate
            .result_rel(root)
            .ri_RelationDesc
            .as_ref()
            .expect("convert_routed_slot_for_error: root ri_RelationDesc is NULL");
        (
            root_rel.rd_id,
            root_rel.name().to_string(),
            root_rel.rd_att.clone_in(mcx)?,
        )
    };

    let map = backend_access_common_next::attmap::build_attrmap_by_name_if_req(
        mcx,
        &old_tupdesc,
        &root_tupdesc,
        false,
    )?;

    let out_slot = if let Some(map) = map {
        // slot = execute_attr_map_slot(map, slot, MakeTupleTableSlot(root_tupdesc, &TTSOpsVirtual));
        let new_slot = backend_executor_execTuples::slot_payload_model::MakeTupleTableSlot(
            mcx,
            Some(mcx::alloc_in(mcx, root_tupdesc.clone_in(mcx)?)?),
            types_nodes::TupleSlotKind::Virtual,
        )?;
        let out = estate.push_slot_data(new_slot)?;
        backend_access_common_next::tupconvert::execute_attr_map_slot(estate, &map, slot, out)?
    } else {
        slot
    };

    Ok((root_relid, out_slot, root_relname))
}

pub fn init_seams() {
    seams::exec_constraints::set(ExecConstraints);
    seams::exec_partition_check::set(ExecPartitionCheck);
    seams::exec_partition_check_emit_error::set(ExecPartitionCheckEmitError);
    seams::exec_check_permissions_select::set(exec_check_permissions_select);
    seams::exec_build_slot_value_description::set(ExecBuildSlotValueDescription);
    seams::init_result_rel_info::set(InitResultRelInfo);
    seams::check_valid_result_rel::set(CheckValidResultRel);
    seams::eval_plan_qual_init::set(EvalPlanQualInit);
    seams::eval_plan_qual_set_plan_with_row_marks::set(eval_plan_qual_set_plan_with_row_marks);
    seams::link_subplan_planstate::set(link_subplan_planstate);
    seams::executor_run::set(ExecutorRun);
    seams::executor_finish::set(ExecutorFinish);
    seams::executor_end::set(ExecutorEnd);
    seams::executor_rewind::set(ExecutorRewind);
    seams::free_query_desc::set(FreeQueryDesc);
    seams::create_query_desc_and_start_explain::set(CreateQueryDescAndStartExplain);

    // COPY-(query)-TO executor lifecycle (copyto.c).
    seams::create_query_desc_and_start::set(CreateQueryDescAndStartCopy);
    seams::executor_run_copy::set(ExecutorRunCopy);
    seams::end_copy_query::set(EndCopyQuery);

    // ExecSupportsBackwardScan (body in execAmi) + ExecUpdateLockMode.
    seams::exec_supports_backward_scan::set(exec_supports_backward_scan);
    seams::exec_update_lock_mode::set(ExecUpdateLockMode);

    // ExecGetReturningSlot / ExecGetChildToRootMap bodies live in execUtils;
    // execMain owns these seam decls (consumed by nodeModifyTable) and delegates.
    seams::exec_get_returning_slot::set(exec_get_returning_slot);
    seams::exec_get_child_to_root_map::set(exec_get_child_to_root_map);

    // PARAM_EXEC `execPlan` link plumbing. nodeSubplan parked these executor
    // PARAM_EXEC / `es_subplanstates` seams in the execProcnode-seams crate, but
    // they are not `execProcnode.c` functions: they operate on the EState's param
    // array (`es_param_exec_vals[paramid].execPlan`), which is executor
    // (execMain) machinery. Now that `ParamExecData.execPlan` is modeled (an
    // `ExecPlanLink` identity into `es_subplanstates`), the field-level operations
    // — mark/clear/pending-test — have a real home here.
    seams::mark_param_execplan_pending::set(mark_param_execplan_pending);
    seams::clear_param_execplan::set(clear_param_execplan);
    seams::param_execplan_pending::set(param_execplan_pending);

    // The parallel executor's `execParallel-support` PARAM_EXEC reads/writes and
    // `QueryDesc` lifecycle accessors — all over the owned `EState`/`QueryDesc`
    // this crate drives.
    use backend_executor_execParallel_support_seams as sup;
    sup::param_exec_value_owned::set(param_exec_value_owned);
    sup::set_param_exec_value_owned::set(set_param_exec_value_owned);
    sup::query_desc_source_text_owned::set(|qd| Ok(qd.source_text_owned()));
    sup::set_query_desc_jit_flags_owned::set(|qd, jit_flags| qd.set_jit_flags(jit_flags));
    sup::query_desc_estate_has_jit_owned::set(|qd| qd.estate_has_jit());
}

// ===========================================================================
// PARAM_EXEC `execPlan` link plumbing (execMain owns `es_param_exec_vals`).
// ===========================================================================

/// `&estate->es_param_exec_vals[paramid]`, mutably. The C indexes
/// unconditionally; an out-of-range id is a planner/caller bug, so it surfaces
/// the executor's internal error rather than panicking.
#[inline]
fn exec_param_mut<'a, 'mcx>(
    estate: &'a mut types_nodes::EStateData<'mcx>,
    paramid: i32,
) -> PgResult<&'a mut types_nodes::ParamExecData<'mcx>> {
    estate
        .es_param_exec_vals
        .get_mut(paramid as usize)
        .ok_or_else(|| types_error::PgError::error("PARAM_EXEC id out of range").with_sqlstate(types_error::ERRCODE_INTERNAL_ERROR))
}

/// `prm = &(estate->es_param_exec_vals[paramid]); prm->execPlan = sstate;` —
/// mark the PARAM_EXEC slot as needing lazy evaluation by the subplan whose
/// stable identity is `plan_id` (the 1-based index into `es_subplanstates`).
/// Mirrors nodeSubplan.c `ExecInitSubPlan` / `ExecReScanSetParamPlan`.
fn mark_param_execplan_pending<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    paramid: i32,
    plan_id: i32,
) -> PgResult<()> {
    let prm = exec_param_mut(estate, paramid)?;
    prm.execPlan = Some(types_nodes::ExecPlanLink { plan_id });
    Ok(())
}

/// `prm->execPlan = NULL;` — clear the PARAM_EXEC `execPlan` link after the
/// initplan output has been set (nodeSubplan.c `ExecSetParamPlan`).
fn clear_param_execplan<'mcx>(estate: &mut types_nodes::EStateData<'mcx>, paramid: i32) -> PgResult<()> {
    let prm = exec_param_mut(estate, paramid)?;
    prm.execPlan = None;
    Ok(())
}

/// `econtext->ecxt_param_exec_vals[paramid].execPlan != NULL` — is this param
/// not yet evaluated? (`ExecSetParamPlanMulti`). Reads the `execPlan` link. The C
/// indexes unconditionally; out of range maps to "not pending" (the C would
/// dereference, but the planner never produces an out-of-range PARAM_EXEC id).
fn param_execplan_pending(estate: &types_nodes::EStateData<'_>, paramid: i32) -> bool {
    estate
        .es_param_exec_vals
        .get(paramid as usize)
        .is_some_and(|prm| prm.execPlan.is_some())
}

/// The process-lifetime context backing the `'static` `ParamExecValue` /
/// restored-param the parallel executor's `execParallel-support` seams carry
/// (the serialized value is read out, copied through the DSM chunk, and dropped;
/// its lifetime is unconstrained at the seam boundary). Mirrors the DSA wire
/// path's `dsa_top_mcx`.
fn param_exec_top_mcx() -> mcx::Mcx<'static> {
    use core::sync::atomic::{AtomicPtr, Ordering};
    // Process-lifetime leaked context (no_std: a lazily-initialized atomic
    // pointer in place of a `thread_local!`; PG backends are single-threaded so
    // the init race is benign — a loser leaks one extra context).
    static TOP: AtomicPtr<mcx::MemoryContext> = AtomicPtr::new(core::ptr::null_mut());
    let mut p = TOP.load(Ordering::Acquire);
    if p.is_null() {
        let leaked: &'static mut mcx::MemoryContext = alloc::boxed::Box::leak(
            alloc::boxed::Box::new(mcx::MemoryContext::new("execParallel param_exec")),
        );
        let new = leaked as *mut mcx::MemoryContext;
        match TOP.compare_exchange(core::ptr::null_mut(), new, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => p = new,
            Err(existing) => p = existing,
        }
    }
    // SAFETY: `p` is a non-null pointer to a leaked, never-freed `MemoryContext`.
    unsafe { (*p).mcx() }
}

/// `&estate->es_param_exec_vals[paramid]` value/isnull + the resolved
/// `(typByVal, typLen)` for `list_nth_oid(paramExecTypes, paramid)`
/// (execParallel.c `EstimateParamExecSpace`/`SerializeParamExecParams`). With no
/// type OID, C assumes by-value (`typLen = sizeof(Datum)`, `typByVal = true`),
/// like `copyParamList`.
fn param_exec_value_owned(
    estate: &mut types_nodes::EStateData<'_>,
    paramid: i32,
) -> types_execparallel::ParamExecValue<'static> {
    let prm = estate
        .es_param_exec_vals
        .get(paramid as usize)
        .expect("PARAM_EXEC id in range");
    let value = prm
        .value
        .clone_in(param_exec_top_mcx())
        .expect("clone PARAM_EXEC value");
    let isnull = prm.isnull;

    // typeOid = list_nth_oid(estate->es_plannedstmt->paramExecTypes, paramid);
    let type_oid = estate
        .es_plannedstmt
        .as_ref()
        .and_then(|p| p.paramExecTypes.as_ref())
        .and_then(|types| types.get(paramid as usize).copied())
        .unwrap_or(types_core::InvalidOid);

    let (typ_len, typ_byval) = if type_oid != types_core::InvalidOid {
        backend_utils_cache_lsyscache_seams::get_typlenbyval::call(type_oid)
            .expect("get_typlenbyval")
    } else {
        // No type OID: assume by-value, like copyParamList does.
        (core::mem::size_of::<usize>() as i16, true)
    };

    types_execparallel::ParamExecValue {
        value,
        isnull,
        typ_byval,
        typ_len,
    }
}

/// `prm = &es_param_exec_vals[paramid]; prm->value = ...; prm->isnull = ...;
/// prm->execPlan = NULL;` (execParallel.c `RestoreParamExecParams`).
fn set_param_exec_value_owned<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    paramid: i32,
    restored: types_execparallel::RestoredParam<'mcx>,
) {
    let prm = exec_param_mut(estate, paramid).expect("PARAM_EXEC id in range");
    prm.value = restored.value;
    prm.isnull = restored.isnull;
    prm.execPlan = None;
}
