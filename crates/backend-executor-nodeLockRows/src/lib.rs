//! Port of `nodeLockRows.c` — routines to handle `FOR UPDATE`/`FOR SHARE` row
//! locking.
//!
//! The `LockRows` node sits above a subplan that emits candidate rows with junk
//! `ctid`/`tableoid` columns; for each locking rowmark it takes the requested
//! tuple lock through the table access method, handles every `TM_Result`
//! outcome exactly as C does (skip / serialization error / Halloween-safe
//! self-modified skip / ...), and finally re-checks the row with EvalPlanQual
//! when a lock traversed an update chain.
//!
//! The node-state machine — the `ExecProcNode` callback [`ExecLockRows`], the
//! initializer [`ExecInitLockRows`], the teardown [`ExecEndLockRows`], and the
//! rescan [`ExecReScanLockRows`] — is ported here 1:1. The subsystems below the
//! executor-node layer (the cross-node `ExecProcNode` dispatch + recursive child
//! init/teardown/rescan, the table AM `table_tuple_lock`, the FDW
//! `RefetchForeignRow`, the EvalPlanQual machinery, the result-type/slot-ops
//! setup and junk-attribute fetch, the rowmark lookup/build, `exec_rt_fetch` /
//! `bms_is_member`, interrupt checking, and the `XactIsoLevel` GUC) are reached
//! through the per-owner seams in `backend-executor-nodeLockRows-seams`, each of
//! which panics loudly until its owner installs a real implementation.

#![no_std]
#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

extern crate alloc;

use backend_executor_nodeLockRows_seams as seam;
use mcx::{Mcx, PgBox, PgVec};
use seam::{ForeignRefetch, TupleLockRequest};
use types_core::xact::XACT_REPEATABLE_READ;
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR, ERRCODE_T_R_SERIALIZATION_FAILURE};
use types_nodes::execnodes::EStateData;
use types_nodes::nodelockrows::{
    ExecAuxRowMarkData, ExecRowMark, LockRows, LockRowsStateData, RowMarkRequiresRowShareLock,
    ROW_MARK_EXCLUSIVE, ROW_MARK_KEYSHARE, ROW_MARK_NOKEYEXCLUSIVE, ROW_MARK_SHARE,
};
use types_nodes::parsenodes::RTE_RELATION;
use types_tuple::heaptuple::ItemPointerData;
use types_tableam::{
    LockTupleExclusive, LockTupleKeyShare, LockTupleMode, LockTupleNoKeyExclusive, LockTupleShare,
    TM_FailureData, TM_Result, TUPLE_LOCK_FLAG_FIND_LAST_VERSION,
    TUPLE_LOCK_FLAG_LOCK_UPDATE_IN_PROGRESS,
};

/// `EXEC_FLAG_MARK` (executor.h) — caller needs mark/restore support. A LockRows
/// node never supports mark/restore.
const EXEC_FLAG_MARK: i32 = 0x0010;

/// `RELKIND_FOREIGN_TABLE` (`pg_class.h`) — relkind for a foreign table; the
/// lock path for these is delegated to the FDW.
const RELKIND_FOREIGN_TABLE: u8 = b'f';

/// `IsolationUsesXactSnapshot()` (xact.h) — `XactIsoLevel >= XACT_REPEATABLE_READ`.
#[inline]
fn IsolationUsesXactSnapshot(xact_iso_level: i32) -> bool {
    xact_iso_level >= XACT_REPEATABLE_READ
}

// ===========================================================================
// `ExecLockRows` — the `ExecProcNode` callback (ported 1:1).
// ===========================================================================

/// `ExecLockRows(pstate)` — the `ExecProcNode` callback: fetch locked rows.
///
/// Pulls the next tuple from the subplan, then for each locking rowmark takes
/// the requested tuple lock through the table AM, honoring every `TM_Result`
/// outcome exactly as C does, and finally re-checks the row through EvalPlanQual
/// when any lock traversed an update chain. Returns `Ok(true)` when a row is
/// available (the result tuple is the node's working "outer" slot,
/// `node.lr_curOuterSlot`); `Ok(false)` when the subplan is exhausted.
pub fn ExecLockRows<'mcx>(
    node: &mut LockRowsStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // CHECK_FOR_INTERRUPTS();
    seam::check_for_interrupts::call()?;

    // Get next tuple from subplan, if any. `lnext:` is C's retry target; we
    // model it as a loop with `continue 'lnext` for the `goto lnext` cases and
    // `return Ok(true)` on success.
    'lnext: loop {
        //   slot = ExecProcNode(outerPlan);
        let have_slot = seam::exec_proc_node_outer::call(node, estate)?;

        // if (TupIsNull(slot))
        if !have_slot {
            // Release any resources held by EPQ mechanism before exiting
            seam::eval_plan_qual_end::call(node, estate)?;
            return Ok(false);
        }

        // We don't need EvalPlanQual unless we get updated tuple version(s)
        let mut epq_needed = false;

        // Attempt to lock the source tuple(s).  (Note we only have locking
        // rowmarks in lr_arowMarks.)
        //   foreach(lc, node->lr_arowMarks)
        let nmarks = node.lr_arowMarks.len();
        let mut mark_index = 0;
        while mark_index < nmarks {
            //   ExecAuxRowMark *aerm = lfirst(lc);  ExecRowMark *erm = aerm->rowmark;

            // clear any leftover test tuple for this rel
            //   markSlot = EvalPlanQualSlot(&node->lr_epqstate, erm->relation, erm->rti);
            //   ExecClearTuple(markSlot);
            seam::eval_plan_qual_slot_clear::call(node, estate, mark_index)?;

            // if child rel, must check whether it produced this row
            //   if (erm->rti != erm->prti)
            let (rti, prti, relid) = {
                let erm = rowmark(node, mark_index)?;
                (erm.rti, erm.prti, erm.relid)
            };
            if rti != prti {
                let mut is_null = false;
                //   datum = ExecGetJunkAttribute(slot, aerm->toidAttNo, &isNull);
                let tableoid =
                    seam::exec_get_junk_tableoid::call(node, estate, mark_index, &mut is_null)?;
                // shouldn't ever get a null result...
                if is_null {
                    return Err(elog_internal("tableoid is NULL"));
                }

                // Assert(OidIsValid(erm->relid));
                debug_assert!(relid != 0);
                if tableoid != relid {
                    // this child is inactive right now
                    //   erm->ermActive = false;
                    //   ItemPointerSetInvalid(&(erm->curCtid));
                    let erm = rowmark_mut(node, mark_index)?;
                    erm.ermActive = false;
                    erm.curCtid = item_pointer_set_invalid();
                    mark_index += 1;
                    continue;
                }
            }
            //   erm->ermActive = true;
            rowmark_mut(node, mark_index)?.ermActive = true;

            // fetch the tuple's ctid
            //   datum = ExecGetJunkAttribute(slot, aerm->ctidAttNo, &isNull);
            let mut is_null = false;
            let tid = seam::exec_get_junk_ctid::call(node, estate, mark_index, &mut is_null)?;
            // shouldn't ever get a null result...
            if is_null {
                return Err(elog_internal("ctid is NULL"));
            }

            // requests for foreign tables must be passed to their FDW
            //   if (erm->relation->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
            if seam::relation_get_relkind::call(node, mark_index)? == RELKIND_FOREIGN_TABLE {
                //   fdwroutine = GetFdwRoutineForRelation(erm->relation, false);
                //   if (fdwroutine->RefetchForeignRow == NULL) ereport(ERROR, ...);
                //   fdwroutine->RefetchForeignRow(estate, erm, datum, markSlot, &updated);
                //
                // The FDW-missing-callback error and the foreign-table name read
                // both live in the FDW layer the seam owns. The seam refetches
                // into the node's working "mark" slot and reports both the
                // `updated` out-parameter and whether the slot ended up empty.
                let refetch: ForeignRefetch =
                    seam::refetch_foreign_row::call(node, estate, mark_index, tid)?;
                // if (TupIsNull(markSlot)) { couldn't get the lock, skip this row }
                if refetch.mark_slot_is_null {
                    continue 'lnext;
                }
                // if FDW says tuple was updated before getting locked, we need to
                // perform EPQ testing to see if quals are still satisfied
                if refetch.updated {
                    epq_needed = true;
                }
                mark_index += 1;
                continue;
            }

            // okay, try to lock (and fetch) the tuple
            //   tid = *((ItemPointer) DatumGetPointer(datum));  (decoded above)
            let lockmode: LockTupleMode = match rowmark(node, mark_index)?.markType {
                ROW_MARK_EXCLUSIVE => LockTupleExclusive,
                ROW_MARK_NOKEYEXCLUSIVE => LockTupleNoKeyExclusive,
                ROW_MARK_SHARE => LockTupleShare,
                ROW_MARK_KEYSHARE => LockTupleKeyShare,
                _ => return Err(elog_internal("unsupported rowmark type")),
            };

            let mut lockflags = TUPLE_LOCK_FLAG_LOCK_UPDATE_IN_PROGRESS;
            if !IsolationUsesXactSnapshot(seam::xact_iso_level::call()?) {
                lockflags |= TUPLE_LOCK_FLAG_FIND_LAST_VERSION;
            }

            let mut tmfd = TM_FailureData::default();
            //   test = table_tuple_lock(erm->relation, &tid, estate->es_snapshot,
            //       markSlot, estate->es_output_cid, lockmode, erm->waitPolicy,
            //       lockflags, &tmfd);
            let request = TupleLockRequest {
                lockmode,
                lockflags: lockflags as i32,
            };
            let test: TM_Result =
                seam::table_tuple_lock::call(node, estate, mark_index, tid, request, &mut tmfd)?;

            match test {
                // TM_WouldBlock: couldn't lock tuple in SKIP LOCKED mode
                TM_Result::TM_WouldBlock => continue 'lnext,
                // TM_SelfModified: target already updated/deleted by current
                // command, or a later command in this transaction.  We *must*
                // ignore the tuple (Halloween problem); treat as deleted.
                TM_Result::TM_SelfModified => continue 'lnext,
                // TM_Ok: got the lock; the locked tuple is in markSlot for EPQ.
                TM_Result::TM_Ok => {
                    if tmfd.traversed {
                        epq_needed = true;
                    }
                }
                // TM_Updated
                TM_Result::TM_Updated => {
                    if IsolationUsesXactSnapshot(seam::xact_iso_level::call()?) {
                        return Err(serialization_error());
                    }
                    return Err(elog_internal_fmt("unexpected table_tuple_lock status: ", test));
                }
                // TM_Deleted
                TM_Result::TM_Deleted => {
                    if IsolationUsesXactSnapshot(seam::xact_iso_level::call()?) {
                        return Err(serialization_error());
                    }
                    // tuple was deleted so don't return it
                    continue 'lnext;
                }
                // TM_Invisible
                TM_Result::TM_Invisible => {
                    return Err(elog_internal("attempted to lock invisible tuple"))
                }
                // default (TM_BeingModified and any future codes)
                _ => {
                    return Err(elog_internal_fmt(
                        "unrecognized table_tuple_lock status: ",
                        test,
                    ));
                }
            }

            // Remember locked tuple's TID for EPQ testing and WHERE CURRENT OF
            //   erm->curCtid = tid;
            rowmark_mut(node, mark_index)?.curCtid = tid;

            mark_index += 1;
        }

        // If we need to do EvalPlanQual testing, do so.
        if epq_needed {
            // Initialize EPQ machinery
            seam::eval_plan_qual_begin::call(node, estate)?;

            // To fetch non-locked source rows the EPQ logic needs to access junk
            // columns from the tuple being tested.
            //   EvalPlanQualSetSlot(&node->lr_epqstate, slot);
            seam::eval_plan_qual_set_slot::call(node, estate)?;

            // And finally we can re-evaluate the tuple.
            //   slot = EvalPlanQualNext(&node->lr_epqstate);
            let have_slot = seam::eval_plan_qual_next::call(node, estate)?;
            if !have_slot {
                // Updated tuple fails qual, so ignore it and go on
                continue 'lnext;
            }
        }

        // Got all locks, so return the current tuple
        return Ok(true);
    }
}

// ===========================================================================
// `ExecInitLockRows` — initializer (ported 1:1).
// ===========================================================================

/// `ExecInitLockRows(node, estate, eflags)` — initialize the LockRows node and
/// its subplan.
///
/// `node` is the `LockRows` plan node produced by the planner; `eflags` is the
/// executor flags. Builds the owned [`LockRowsStateData`] (allocated in the
/// executor's per-query context) and returns it by `PgBox`. The
/// genuinely-external setup helpers are reached through the seams; the rowmark
/// partitioning is performed in-crate.
pub fn ExecInitLockRows<'mcx>(
    node: &LockRows<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<PgBox<'mcx, LockRowsStateData<'mcx>>> {
    // check for unsupported flags
    debug_assert!(eflags & EXEC_FLAG_MARK == 0);

    // create state structure: makeNode(LockRowsState)
    let mcx = estate.es_query_cxt;
    let mut lrstate = mcx::alloc_in(mcx, LockRowsStateData::new_in(mcx))?;

    // lrstate->ps.plan = (Plan *) node; lrstate->ps.state = estate;
    // lrstate->ps.ExecProcNode = ExecLockRows;
    //
    // The plan-node link and the ExecProcNode install are wired by the executor
    // node factory (it owns the plan-tree references and the dispatch slot).
    seam::init_plan_state_links::call(&mut lrstate, node)?;

    // Miscellaneous initialization. LockRows nodes never call ExecQual or
    // ExecProject, therefore no ExprContext is needed.

    // Initialize result type.
    //   ExecInitResultTypeTL(&lrstate->ps);
    seam::exec_init_result_type_tl::call(&mut lrstate, estate)?;

    // then initialize outer plan
    //   outerPlanState(lrstate) = ExecInitNode(outerPlan, estate, eflags);
    seam::exec_init_node_outer::call(&mut lrstate, node, estate, eflags)?;

    // node returns unmodified slots from the outer plan
    //   lrstate->ps.resultopsset = true;
    //   lrstate->ps.resultops = ExecGetResultSlotOps(outerPlanState(lrstate),
    //                                                &lrstate->ps.resultopsfixed);
    seam::exec_get_result_slot_ops::call(&mut lrstate)?;

    // LockRows nodes do no projections, so initialize projection info for this
    // node appropriately
    //   lrstate->ps.ps_ProjInfo = NULL;
    lrstate.ps.ps_ProjInfo = None;

    // Locate the ExecRowMark(s) that this node is responsible for, and construct
    // ExecAuxRowMarks for them.  (InitPlan should already have built the global
    // list of ExecRowMarks.)
    //
    // The non-locking aux rowmarks are accumulated into a transient
    // `epq_arowmarks` buffer (C builds it in CurrentMemoryContext, the executor
    // per-query context) and handed to EvalPlanQualInit (the seam re-homes it).
    //   lrstate->lr_arowMarks = NIL;  epq_arowmarks = NIL;
    let mut epq_arowmarks: PgVec<ExecAuxRowMarkData> = PgVec::new_in(mcx);

    // foreach(lc, node->rowMarks)
    if let Some(row_marks) = node.rowMarks.as_ref() {
        // Snapshot the (rti, isParent) pairs first so we can take `&mut estate`
        // for the lookup seams without aliasing the borrowed plan node.
        let nmarks = row_marks.len();
        let mut i = 0usize;
        while i < nmarks {
            let rc_rti = row_marks[i].rti;
            let rc_is_parent = row_marks[i].isParent;
            i += 1;

            // ignore "parent" rowmarks; they are irrelevant at runtime
            if rc_is_parent {
                continue;
            }

            // Also ignore rowmarks belonging to child tables that have been
            // pruned in ExecDoInitialPruning().
            //   RangeTblEntry *rte = exec_rt_fetch(rc->rti, estate);
            //   if (rte->rtekind == RTE_RELATION &&
            //       !bms_is_member(rc->rti, estate->es_unpruned_relids))
            //       continue;
            if seam::exec_rt_fetch_rtekind::call(estate, rc_rti)? == RTE_RELATION
                && !seam::unpruned_relids_is_member::call(estate, rc_rti)?
            {
                continue;
            }

            // find ExecRowMark and build ExecAuxRowMark
            //   erm = ExecFindRowMark(estate, rc->rti, false);
            //   aerm = ExecBuildAuxRowMark(erm, outerPlan->targetlist);
            let erm: PgBox<ExecRowMark> = seam::exec_find_row_mark::call(estate, rc_rti)?;
            let mark_type = erm.markType;
            let aerm: ExecAuxRowMarkData =
                seam::exec_build_aux_row_mark::call(estate, node, erm)?;

            // Only locking rowmarks go into our own list.  Non-locking marks are
            // passed off to the EvalPlanQual machinery.  This is because we don't
            // want to bother fetching non-locked rows unless we actually have to
            // do an EPQ recheck.
            //   if (RowMarkRequiresRowShareLock(erm->markType))
            if RowMarkRequiresRowShareLock(mark_type) {
                //   lrstate->lr_arowMarks = lappend(lrstate->lr_arowMarks, aerm);
                push_in(mcx, &mut lrstate.lr_arowMarks, aerm)?;
            } else {
                //   epq_arowmarks = lappend(epq_arowmarks, aerm);
                push_in(mcx, &mut epq_arowmarks, aerm)?;
            }
        }
    }

    // Now we have the info needed to set up EPQ state
    //   EvalPlanQualInit(&lrstate->lr_epqstate, estate, outerPlan,
    //                    epq_arowmarks, node->epqParam, NIL);
    seam::eval_plan_qual_init::call(&mut lrstate, node, estate, epq_arowmarks)?;

    Ok(lrstate)
}

// ===========================================================================
// `ExecEndLockRows` / `ExecReScanLockRows` — teardown / rescan (ported 1:1).
// ===========================================================================

/// `ExecEndLockRows(node)` — shut down the subplan and free resources.
///
/// ```c
/// void ExecEndLockRows(LockRowsState *node)
/// {
///     /* We may have shut down EPQ already, but no harm in another call */
///     EvalPlanQualEnd(&node->lr_epqstate);
///     ExecEndNode(outerPlanState(node));
/// }
/// ```
pub fn ExecEndLockRows<'mcx>(
    node: &mut LockRowsStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // We may have shut down EPQ already, but no harm in another call
    seam::eval_plan_qual_end::call(node, estate)?;
    seam::exec_end_node_outer::call(node, estate)?;
    Ok(())
}

/// `ExecReScanLockRows(node)` — rescan the relation.
///
/// ```c
/// void ExecReScanLockRows(LockRowsState *node)
/// {
///     PlanState  *outerPlan = outerPlanState(node);
///     if (outerPlan->chgParam == NULL)
///         ExecReScan(outerPlan);
/// }
/// ```
pub fn ExecReScanLockRows<'mcx>(
    node: &mut LockRowsStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // if chgParam of subnode is not null then plan will be re-scanned by first
    // ExecProcNode.
    if seam::outer_chg_param_is_null::call(node)? {
        seam::exec_rescan_outer::call(node, estate)?;
    }
    Ok(())
}

// ===========================================================================
// Crate-local helpers.
// ===========================================================================

/// Borrow the `mark_index`-th rowmark's `ExecRowMark` (the C
/// `((ExecAuxRowMark *) lfirst(lc))->rowmark`). Errors loudly if the aux rowmark
/// has no attached `ExecRowMark` (it always does after `ExecBuildAuxRowMark`).
fn rowmark<'a, 'mcx>(
    node: &'a LockRowsStateData<'mcx>,
    mark_index: usize,
) -> PgResult<&'a ExecRowMark<'mcx>> {
    node.lr_arowMarks[mark_index]
        .rowmark
        .as_deref()
        .ok_or_else(|| elog_internal("aux rowmark has no ExecRowMark"))
}

/// Mutable counterpart of [`rowmark`].
fn rowmark_mut<'a, 'mcx>(
    node: &'a mut LockRowsStateData<'mcx>,
    mark_index: usize,
) -> PgResult<&'a mut ExecRowMark<'mcx>> {
    node.lr_arowMarks[mark_index]
        .rowmark
        .as_deref_mut()
        .ok_or_else(|| elog_internal("aux rowmark has no ExecRowMark"))
}

/// `lappend(list, x)` — an OOM-fallible push into a context-allocated `PgVec`.
fn push_in<'mcx, T>(mcx: Mcx<'mcx>, v: &mut PgVec<'mcx, T>, x: T) -> PgResult<()> {
    v.try_reserve(1)
        .map_err(|_| mcx.oom(core::mem::size_of::<T>()))?;
    v.push(x);
    Ok(())
}

/// `ItemPointerSetInvalid(pointer)` (itemptr.h) — the (InvalidBlock,
/// InvalidOffset) item pointer value to assign into `erm->curCtid`.
#[inline]
fn item_pointer_set_invalid() -> ItemPointerData {
    // BlockIdSet(InvalidBlockNumber); ip_posid = InvalidOffsetNumber(0)
    ItemPointerData::new(types_core::primitive::InvalidBlockNumber, 0)
}

/// `elog(ERROR, "...")` — an internal (untranslated) error.
fn elog_internal(message: &'static str) -> PgError {
    PgError::error(message).with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

/// `elog(ERROR, "... %u", test)` — internal error with the `TM_Result` appended.
fn elog_internal_fmt(prefix: &str, test: TM_Result) -> PgError {
    PgError::error(alloc::format!("{prefix}{}", test as u32))
        .with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

/// `ereport(ERROR, ERRCODE_T_R_SERIALIZATION_FAILURE, "could not serialize
/// access due to concurrent update")`.
fn serialization_error() -> PgError {
    PgError::error("could not serialize access due to concurrent update")
        .with_sqlstate(ERRCODE_T_R_SERIALIZATION_FAILURE)
}

/// Install every seam this crate owns to its real implementation. Called once
/// at startup from `seams-init::init_all()`.
///
/// Every callee of `nodeLockRows.c` lives in a subsystem not yet ported here
/// (the table AM, the FDW, EvalPlanQual, execProcnode/execAmi dispatch,
/// execTuples slot/junk handling, the rowmark build, the xact GUC), so there is
/// no real implementation to install yet — the seams correctly remain
/// uninstalled and panic loudly if reached, which is the faithful behavior
/// until those owners land and install them. This crate therefore installs
/// nothing; its `init_seams()` is the required no-op hook.
pub fn init_seams() {}

#[cfg(test)]
mod tests {
    use super::*;
    use types_core::xact::{XACT_READ_COMMITTED, XACT_SERIALIZABLE};

    #[test]
    fn isolation_uses_xact_snapshot_matches_c() {
        // XACT_READ_COMMITTED(1) < XACT_REPEATABLE_READ(2)
        assert!(!IsolationUsesXactSnapshot(XACT_READ_COMMITTED));
        assert!(IsolationUsesXactSnapshot(XACT_REPEATABLE_READ));
        assert!(IsolationUsesXactSnapshot(XACT_SERIALIZABLE));
    }

    #[test]
    fn item_pointer_set_invalid_matches_c() {
        let ip = item_pointer_set_invalid();
        assert_eq!(ip.ip_posid, 0);
        assert_eq!(
            ip,
            ItemPointerData::new(types_core::primitive::InvalidBlockNumber, 0)
        );
    }

    #[test]
    fn lockmode_mapping_matches_c() {
        let m = |t| -> LockTupleMode {
            match t {
                ROW_MARK_EXCLUSIVE => LockTupleExclusive,
                ROW_MARK_NOKEYEXCLUSIVE => LockTupleNoKeyExclusive,
                ROW_MARK_SHARE => LockTupleShare,
                ROW_MARK_KEYSHARE => LockTupleKeyShare,
                _ => unreachable!("test only exercises the four lockable rowmark types"),
            }
        };
        assert_eq!(m(ROW_MARK_EXCLUSIVE), LockTupleExclusive);
        assert_eq!(m(ROW_MARK_NOKEYEXCLUSIVE), LockTupleNoKeyExclusive);
        assert_eq!(m(ROW_MARK_SHARE), LockTupleShare);
        assert_eq!(m(ROW_MARK_KEYSHARE), LockTupleKeyShare);
    }
}
