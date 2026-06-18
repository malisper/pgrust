//! Port of `src/backend/executor/nodeModifyTable.c` — routines that modify a
//! result relation: INSERT, UPDATE, DELETE, and MERGE.
//!
//! INTERFACE ROUTINES
//! - [`lifecycle::ExecInitModifyTable`] - initialize the ModifyTable node.
//! - [`lifecycle::ExecModifyTable`]     - retrieve the next tuple to modify.
//! - [`lifecycle::ExecEndModifyTable`]  - shut down the ModifyTable node.
//! - [`lifecycle::ExecReScanModifyTable`] - rescan the ModifyTable node.
//!
//! The node drives its subplan, and for each subplan tuple performs the
//! INSERT/UPDATE/DELETE/MERGE the plan asks for against one of its
//! `resultRelInfo[]` target relations (an inherited or partitioned target
//! produces several). The owned logic here is the per-operation state machine:
//! junk-attribute extraction, the insert/update/delete/merge prologue →
//! act → epilogue sequence, ON CONFLICT arbitration, cross-partition UPDATE
//! (delete-then-insert with foreign-key bookkeeping), MERGE matched/not-matched
//! action dispatch, RETURNING projection, stored-generated-column computation,
//! and transition-table capture orchestration.
//!
//! Everything below the node layer goes through the owners' seam crates:
//!
//! - heap/table access (`table_tuple_insert` / `table_tuple_update` /
//!   `table_tuple_delete` / `table_tuple_lock` / `table_tuple_fetch_row_version`
//!   / `table_slot_create` / `table_multi_insert`) → `backend-access-table-tableam`;
//! - constraint / WCO / EvalPlanQual / row-mark machinery (`ExecConstraints` /
//!   `ExecWithCheckOptions` / `ExecPartitionCheck` / `EvalPlanQual*` /
//!   `ExecGetReturningSlot` / `ExecInitResultRelation`) → execMain;
//! - expression compile/eval (`ExecInitQual` / `ExecBuildProjectionInfo` /
//!   `ExecBuildUpdateProjection` / `ExecProject` / `ExecQual`) → execExpr;
//! - slot/econtext setup (`ExecAssignExprContext` / `MakeTupleTableSlot` /
//!   `ExecCopySlot` / `ExecClearTuple` / `ExecMaterializeSlot` /
//!   `ExecForceStoreHeapTuple` / `ExecGetRootToChildMap`) → execTuples / execUtils;
//! - child dispatch / teardown / rescan (`ExecProcNode` / `ExecInitNode` /
//!   `ExecEndNode` / `ExecReScan` / `ExecPostprocessPlan`) → execProcnode / execAmi;
//! - tuple routing for partitioned targets (`ExecSetupPartitionTupleRouting` /
//!   `ExecFindPartition` / `ExecCleanupTupleRouting` / `ExecDoInitialPruning`)
//!   → execPartition;
//! - trigger firing & transition capture (`Exec*Triggers` /
//!   `MakeTransitionCaptureState`) → trigger;
//! - index maintenance (`ExecOpenIndices` / `ExecInsertIndexTuples` /
//!   `ExecCheckIndexConstraints`) → execIndexing;
//! - row locking (`LockTuple` / heavyweight locks) → lmgr;
//! - interrupt servicing (`CHECK_FOR_INTERRUPTS`) → tcop/postgres;
//! - function-call value transport (`OidFunctionCall*` / fmgr) → fmgr;
//! - FDW direct modify (`ri_FdwRoutine->ExecForeign*`) dispatches through the
//!   per-relation `FdwRoutine` vtable carried on `ResultRelInfo` (resolved when
//!   the fdwapi type lands).
//!
//! Each function lands in exactly one family module so the body phase can be
//! parallelized:
//! - [`insert`]    — INSERT path (batch insert, ON CONFLICT, TID visibility);
//!   the single-tuple [`insert_exec::ExecInsert`] driver is split out;
//! - [`update`]    — UPDATE path (+ cross-partition update + new-tuple build);
//! - [`delete`]    — DELETE path; the [`delete_exec::ExecDelete`] driver is
//!   split out;
//! - [`merge`]     — MERGE path; the [`merge_matched::ExecMergeMatched`]
//!   dispatch is split out;
//! - [`lifecycle`] — node end/rescan, RETURNING, generated columns,
//!   tuple-routing prep, transition-capture setup, statement-trigger firing;
//!   the [`init::ExecInitModifyTable`] and [`exec::ExecModifyTable`] drivers
//!   are split out.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

pub mod delete;
pub mod delete_exec;
pub mod exec;
pub mod init;
pub mod insert;
pub mod insert_exec;
pub mod lifecycle;
pub mod merge;
pub mod merge_matched;
pub mod partition_init;
pub mod update;

use types_tableam::tableam::{LockTupleMode, TM_FailureData, TU_UpdateIndexes};
use types_nodes::SlotId;

/// `ModifyTableContext` (executor/nodeModifyTable.c) — per-operation working
/// state threaded through the insert/update/delete/merge helpers.
///
/// In C this is a stack struct that also carries back-pointers (`mtstate`,
/// `epqstate`, `estate`); in the owned model those are threaded as explicit
/// `&mut` references by the call sites, so the context carries only the owned
/// per-operation values plus the slot ids.
#[derive(Debug)]
pub struct ModifyTableContext {
    /// `TupleTableSlot *planSlot` — subplan tuple (for junk columns).
    pub planSlot: Option<SlotId>,
    /// `TM_FailureData tmfd` — info about concurrent changes to the target.
    pub tmfd: TM_FailureData,
    /// `TupleTableSlot *cpDeletedSlot` — tuple deleted in a cross-partition
    /// UPDATE whose RETURNING refers to OLD columns (root-rowtype).
    pub cpDeletedSlot: Option<SlotId>,
    /// `TupleTableSlot *cpUpdateReturningSlot` — INSERT RETURNING projection
    /// of a cross-partition UPDATE.
    pub cpUpdateReturningSlot: Option<SlotId>,
}

/// `UpdateContext` (executor/nodeModifyTable.c) — outputs of `ExecUpdateAct`.
#[derive(Debug)]
pub struct UpdateContext {
    /// `bool crossPartUpdate` — was it a cross-partition update?
    pub crossPartUpdate: bool,
    /// `TU_UpdateIndexes updateIndexes` — which index updates are required.
    pub updateIndexes: TU_UpdateIndexes,
    /// `LockTupleMode lockmode` — lock mode to acquire on the latest tuple
    /// version before EvalPlanQual.
    pub lockmode: LockTupleMode,
}

/// Install this unit's seams. nodeModifyTable owns the
/// `backend-executor-nodeModifyTable-seams` declarations (execUtils calls
/// `ExecInitGenerated` through them).
pub fn init_seams() {
    backend_executor_nodeModifyTable_seams::exec_init_generated::set(
        lifecycle::ExecInitGenerated,
    );
    backend_executor_nodeModifyTable_seams::exec_compute_stored_generated::set(
        lifecycle::ExecComputeStoredGenerated,
    );

    // The per-leaf-partition `ResultRelInfo` init blocks of
    // `ExecInitPartitionInfo` (execPartition.c) that read the `ModifyTable` plan
    // node and write ModifyTable-meaning `ResultRelInfo` fields — owned here.
    backend_executor_nodeModifyTable_seams::exec_get_on_conflict_action::set(
        partition_init::ExecGetOnConflictAction,
    );
    backend_executor_nodeModifyTable_seams::exec_open_partition_indices::set(
        partition_init::ExecOpenPartitionIndices,
    );
    backend_executor_nodeModifyTable_seams::exec_init_partition_with_check_options::set(
        partition_init::ExecInitPartitionWithCheckOptions,
    );
    backend_executor_nodeModifyTable_seams::exec_init_partition_returning::set(
        partition_init::ExecInitPartitionReturning,
    );
    backend_executor_nodeModifyTable_seams::exec_init_partition_on_conflict::set(
        partition_init::ExecInitPartitionOnConflict,
    );
    backend_executor_nodeModifyTable_seams::exec_init_partition_merge::set(
        partition_init::ExecInitPartitionMerge,
    );

    // ExecModifyTable's reads of trimmed/now-modeled ResultRelInfo fields,
    // declared in `crate::exec`.
    //
    // `ri_RowIdAttNo` is now carried on the trimmed ResultRelInfo (set up in
    // ExecInitModifyTable for UPDATE/DELETE/MERGE; 0 for INSERT).
    exec::ri_row_id_attno::set(|estate, rri| estate.result_rel(rri).ri_RowIdAttNo as i32);
    // `ri_usesFdwDirectModify` is not carried on the trimmed ResultRelInfo, but
    // it is only ever true for a foreign table whose FDW does direct modify —
    // a target ExecInitModifyTable rejects (fdwDirectModifyPlans unsupported),
    // so it is always false on every reachable path.
    exec::ri_uses_fdw_direct_modify::set(|_estate, _rri| false);

    // `relinfo->ri_projectNew == NULL` — the insert/update new-tuple junk-filter
    // projection presence flag (`ri_has_project_new`, set by
    // exec_build_insert_projection when a projection is built; false for the
    // common no-junk INSERT).
    insert::ri_project_new_is_null::set(|estate, rri| {
        !estate.result_rel(rri).ri_has_project_new
    });

    // `relinfo->ri_newTupleSlot->tts_ops != planSlot->tts_ops` — compare the
    // slot class (kind) of the relation's new-tuple slot against the plan slot.
    insert::ri_new_tuple_slot_ops_differ::set(|estate, rri, plan_slot| {
        let new_slot = estate
            .result_rel(rri)
            .ri_newTupleSlot
            .expect("ExecGetInsertNewTuple: ri_newTupleSlot is NULL");
        estate.slot_data(new_slot).kind() != estate.slot_data(plan_slot).kind()
    });

    // `ExecCopySlot(relinfo->ri_newTupleSlot, planSlot); return ri_newTupleSlot;`
    insert::exec_copy_into_new_tuple_slot::set(|estate, rri, plan_slot| {
        let new_slot = estate
            .result_rel(rri)
            .ri_newTupleSlot
            .expect("ExecGetInsertNewTuple: ri_newTupleSlot is NULL");
        backend_executor_execTuples_seams::exec_copy_slot::call(estate, new_slot, plan_slot)?;
        Ok(new_slot)
    });
}
