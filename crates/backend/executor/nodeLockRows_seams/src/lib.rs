//! Seam declarations for `backend-executor-nodeLockRows`
//! (`executor/nodeLockRows.c`): the row-locking node's calls into subsystems
//! that live in code not yet ported here.
//!
//! A `LockRows` node sits above a subplan and takes `FOR UPDATE`/`FOR SHARE`
//! tuple locks on the rows the subplan emits. The node state machine itself
//! (the `TM_Result` dispatch, the `goto lnext` control flow, the rowmark
//! partitioning, the lock-mode / lock-flag derivation) is ported in
//! `backend-executor-nodeLockRows`; the subsystems *below* the executor-node
//! layer — cross-node `ExecProcNode` dispatch and recursive child
//! init/teardown/rescan (`execProcnode.c`/`execAmi.c`), the table AM
//! `table_tuple_lock` (`tableam.c`), the FDW `RefetchForeignRow`
//! (`foreign/fdwapi.c`), the EvalPlanQual machinery (`execMain.c`), the
//! result-type/slot-ops setup and junk-attribute fetch (`execTuples.c`), the
//! rowmark lookup/build (`execMain.c`), `exec_rt_fetch`/`bms_is_member`,
//! interrupt checking, and the `XactIsoLevel` GUC — are reached through these
//! seams. Each defaults to a loud panic until its owner installs a real
//! implementation.
//!
//! The cross-node recursion `ExecProcNode(child)` follows the child's installed
//! callback (never a direct sibling-crate call); that dispatch and the layers
//! below it are reached through the `exec_*_outer` seams. The per-rowmark
//! slot-bearing seams address the working `TupleTableSlot`s by the rowmark's
//! index into the node's `lr_arowMarks` vector, so this crate never holds a raw
//! slot/relation pointer.

#![allow(non_snake_case)]

use ::types_core::primitive::{Index, Oid};
use ::types_error::PgResult;
use ::nodes::execnodes::EStateData;
use ::nodes::nodelockrows::{ExecAuxRowMarkData, ExecRowMark, LockRows, LockRowsStateData};
use ::nodes::parsenodes::RTEKind;
use types_tableam::{LockTupleMode, TM_FailureData, TM_Result};
use ::types_tuple::heaptuple::ItemPointerData;

/// The lock request `ExecLockRows` passes to `table_tuple_lock`, bundling the
/// scalar arguments the C call site builds inline (`lockmode`, `lockflags`).
/// The relation, snapshot, output CID, wait policy and result slot all live on
/// the addressed rowmark / `EState`, which the table-AM seam owns, so they are
/// not repeated here.
#[derive(Clone, Copy, Debug)]
pub struct TupleLockRequest {
    /// `LockTupleMode lockmode` — derived from `erm->markType`.
    pub lockmode: LockTupleMode,
    /// `int lockflags` — `TUPLE_LOCK_FLAG_*` bit set.
    pub lockflags: i32,
}

/// Result of the FDW `RefetchForeignRow` call. The C node reads two things back
/// after the call: the `updated` out-parameter, and whether the refetch left
/// `markSlot` empty (`TupIsNull(markSlot)` — the lock could not be obtained, so
/// the row is skipped).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ForeignRefetch {
    /// FDW `updated` out-parameter — the tuple was updated before being locked,
    /// so EPQ testing is needed.
    pub updated: bool,
    /// `TupIsNull(markSlot)` after the refetch — the lock could not be obtained
    /// and the row must be skipped (`goto lnext`).
    pub mark_slot_is_null: bool,
}

seam_core::seam!(
    /// `slot = ExecProcNode(outerPlanState(node))` — pull the next tuple from
    /// the subplan through the child's installed callback, stashing its
    /// `SlotId` as the node's working "outer" slot (`node.lr_curOuterSlot`).
    /// Returns `Ok(false)` when the result is `TupIsNull` (subplan exhausted).
    pub fn exec_proc_node_outer<'mcx>(
        node: &mut LockRowsStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `outerPlanState(lrstate) = ExecInitNode(outerPlan, estate, eflags)` —
    /// build and link the subplan's executor state under the node.
    pub fn exec_init_node_outer<'mcx>(
        lrstate: &mut LockRowsStateData<'mcx>,
        node: &'mcx LockRows<'mcx>,
        estate: &mut EStateData<'mcx>,
        eflags: i32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecEndNode(outerPlanState(node))` — recursive subplan teardown.
    pub fn exec_end_node_outer<'mcx>(
        node: &mut LockRowsStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `outerPlan->chgParam == NULL` — whether the subplan has no changed params
    /// (so it must be explicitly rescanned).
    pub fn outer_chg_param_is_null<'mcx>(node: &LockRowsStateData<'mcx>) -> PgResult<bool>
);

seam_core::seam!(
    /// `ExecReScan(outerPlanState(node))` — rescan the subplan.
    pub fn exec_rescan_outer<'mcx>(
        node: &mut LockRowsStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// Wire `lrstate->ps.plan = (Plan *) node` and install the node's
    /// `ExecProcNode` callback. These link the node into the executor's plan
    /// tree and dispatch machinery, which live outside this crate.
    pub fn init_plan_state_links<'mcx>(
        lrstate: &mut LockRowsStateData<'mcx>,
        node: &LockRows<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `CHECK_FOR_INTERRUPTS()`.
    pub fn check_for_interrupts() -> PgResult<()>
);

seam_core::seam!(
    /// `ExecInitResultTypeTL(&lrstate->ps)` — initialize the result tuple type
    /// from the plan's target list.
    pub fn exec_init_result_type_tl<'mcx>(
        lrstate: &mut LockRowsStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `lrstate->ps.resultops = ExecGetResultSlotOps(outerPlanState(lrstate),
    /// &lrstate->ps.resultopsfixed)` — copy the subplan's result slot-ops into
    /// the node (a LockRows node returns the subplan's slots unmodified). Also
    /// sets `lrstate->ps.resultopsset = true`.
    pub fn exec_get_result_slot_ops<'mcx>(lrstate: &mut LockRowsStateData<'mcx>) -> PgResult<()>
);

seam_core::seam!(
    /// `markSlot = EvalPlanQualSlot(&node->lr_epqstate, erm->relation,
    /// erm->rti); ExecClearTuple(markSlot)` for the `mark_index`-th rowmark —
    /// fetch and clear the rowmark's EPQ test slot.
    pub fn eval_plan_qual_slot_clear<'mcx>(
        node: &mut LockRowsStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
        mark_index: usize,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `DatumGetObjectId(ExecGetJunkAttribute(slot, aerm->toidAttNo, &isNull))`
    /// — fetch the tableoid junk column from the working outer slot for the
    /// `mark_index`-th rowmark, decoded to an `Oid`. Sets `is_null` when the
    /// column is SQL NULL.
    pub fn exec_get_junk_tableoid<'mcx>(
        node: &mut LockRowsStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
        mark_index: usize,
        is_null: &mut bool,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `tid = *((ItemPointer) DatumGetPointer(ExecGetJunkAttribute(slot,
    /// aerm->ctidAttNo, &isNull)))` — fetch the ctid junk column from the
    /// working outer slot for the `mark_index`-th rowmark, decoded to an item
    /// pointer. Sets `is_null` when the column is SQL NULL.
    pub fn exec_get_junk_ctid<'mcx>(
        node: &mut LockRowsStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
        mark_index: usize,
        is_null: &mut bool,
    ) -> PgResult<ItemPointerData>
);

seam_core::seam!(
    /// `erm->relation->rd_rel->relkind` for the `mark_index`-th rowmark.
    pub fn relation_get_relkind<'mcx>(
        node: &LockRowsStateData<'mcx>,
        mark_index: usize,
    ) -> PgResult<u8>
);

seam_core::seam!(
    /// `GetFdwRoutineForRelation(erm->relation, false)->RefetchForeignRow(
    /// estate, erm, ctid-datum, markSlot, &updated)` for the `mark_index`-th
    /// rowmark. Errors with `cannot lock rows in foreign table "..."` if the
    /// FDW lacks the callback. Returns both the FDW's `updated` flag and whether
    /// the refetch left the node's working "mark" slot empty.
    pub fn refetch_foreign_row<'mcx>(
        node: &mut LockRowsStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
        mark_index: usize,
        ctid: ItemPointerData,
    ) -> PgResult<ForeignRefetch>
);

seam_core::seam!(
    /// `table_tuple_lock(erm->relation, &tid, estate->es_snapshot, markSlot,
    /// estate->es_output_cid, request.lockmode, erm->waitPolicy,
    /// request.lockflags, &tmfd)` for the `mark_index`-th rowmark. The locked
    /// tuple is left in the node's working "mark" slot; the lock outcome is
    /// returned along with the `tmfd` failure data (its `traversed` flag drives
    /// EPQ).
    pub fn table_tuple_lock<'mcx>(
        node: &mut LockRowsStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
        mark_index: usize,
        tid: ItemPointerData,
        request: TupleLockRequest,
        tmfd: &mut TM_FailureData,
    ) -> PgResult<TM_Result>
);

seam_core::seam!(
    /// `XactIsoLevel` — the current transaction isolation level GUC, used by
    /// `IsolationUsesXactSnapshot()`.
    pub fn xact_iso_level() -> PgResult<i32>
);

seam_core::seam!(
    /// `EvalPlanQualInit(&lrstate->lr_epqstate, estate, outerPlan,
    /// epq_arowmarks, node->epqParam, NIL)` — set up the node's EPQ state,
    /// taking ownership of the non-locking aux rowmarks the init pass
    /// partitioned off.
    pub fn eval_plan_qual_init<'mcx>(
        lrstate: &mut LockRowsStateData<'mcx>,
        node: &'mcx LockRows<'mcx>,
        estate: &mut EStateData<'mcx>,
        epq_arowmarks: mcx::PgVec<'mcx, ExecAuxRowMarkData<'mcx>>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `EvalPlanQualBegin(&node->lr_epqstate)`.
    pub fn eval_plan_qual_begin<'mcx>(
        node: &mut LockRowsStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `EvalPlanQualSetSlot(&node->lr_epqstate, slot)` — make the node's current
    /// working "outer" slot the EPQ origin slot.
    pub fn eval_plan_qual_set_slot<'mcx>(
        node: &mut LockRowsStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `slot = EvalPlanQualNext(&node->lr_epqstate)` — re-evaluate the recheck
    /// plan, storing the result as the node's working "outer" slot. Returns
    /// `Ok(false)` when the result is `TupIsNull`.
    pub fn eval_plan_qual_next<'mcx>(
        node: &mut LockRowsStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `EvalPlanQualEnd(&node->lr_epqstate)` — release EPQ resources.
    pub fn eval_plan_qual_end<'mcx>(
        node: &mut LockRowsStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `exec_rt_fetch(rti, estate)->rtekind` — the range-table entry kind for
    /// the given range-table index (`list_nth(es_range_table, rti-1)`).
    pub fn exec_rt_fetch_rtekind<'mcx>(estate: &EStateData<'mcx>, rti: Index) -> PgResult<RTEKind>
);

seam_core::seam!(
    /// `bms_is_member(rti, estate->es_unpruned_relids)` — whether the
    /// range-table index survived `ExecDoInitialPruning()`.
    pub fn unpruned_relids_is_member<'mcx>(
        estate: &EStateData<'mcx>,
        rti: Index,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `ExecFindRowMark(estate, rc->rti, false)` — locate the per-query
    /// `ExecRowMark` for a range-table index (built by `InitPlan`).
    pub fn exec_find_row_mark<'mcx>(
        estate: &mut EStateData<'mcx>,
        rti: Index,
    ) -> PgResult<mcx::PgBox<'mcx, ExecRowMark<'mcx>>>
);

seam_core::seam!(
    /// `ExecBuildAuxRowMark(erm, outerPlan->targetlist)` — build the
    /// `ExecAuxRowMark` (resjunk column numbers) pairing the rowmark with the
    /// outer plan's target list.
    pub fn exec_build_aux_row_mark<'mcx>(
        estate: &mut EStateData<'mcx>,
        node: &LockRows<'mcx>,
        erm: mcx::PgBox<'mcx, ExecRowMark<'mcx>>,
    ) -> PgResult<ExecAuxRowMarkData<'mcx>>
);
