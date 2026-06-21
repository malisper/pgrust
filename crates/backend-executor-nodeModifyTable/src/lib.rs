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
    // `ExecGetJunkAttribute(slot, attno, &isNull)` (execJunk.h) is the macro
    // `slot_getattr(slot, attno, isNull)`: the junk attributes ModifyTable reads
    // (the row-ID ctid/wholerow and the tableoid) are positive resnos projected
    // into the plan slot, so they take the regular `slot_getattr` path. The
    // canonical `SlotAttr` is returned whole, so a by-reference ctid image (the
    // 6-byte `ItemPointerData`) crosses as the `Datum::ByRef` arm intact.
    exec::exec_get_junk_attribute::set(|estate, slot, attno| {
        backend_executor_execTuples_seams::slot_getattr_by_id::call(
            estate,
            slot,
            attno as types_core::AttrNumber,
        )
    });

    // `(ItemPointer) DatumGetPointer(datum)` then `*tupleid`: the ctid junk
    // Datum arrives as the canonical `Datum::ByRef` 6-byte `ItemPointerData`
    // image (`PointerGetDatum(&slot->tts_tid)`); decode it back.
    exec::datum_get_item_pointer::set(|datum| {
        backend_access_common_heaptuple::item_pointer_from_bytes(datum.as_ref_bytes())
    });

    // `DatumGetHeapTupleHeader(datum)` + the `oldtupdata` assembly: reconstruct
    // the wholerow OLD tuple from a wholerow junk Datum. C points the composite
    // Datum at a self-describing `HeapTupleHeader` varlena block and reads
    // `t_data`/`t_len` off it; the data-bearing `FormedTuple` carrier mirrors
    // that block (header + user-data area). A `Datum::Composite` already carries
    // the formed tuple (clone it into the query context); any other flat by-ref
    // value is detoasted to its `HeapTupleHeader` image and decoded. C then sets
    //   ItemPointerSetInvalid(&oldtupdata.t_self);
    //   oldtupdata.t_tableOid = (relkind == RELKIND_VIEW) ? InvalidOid
    //                                                     : RelationGetRelid(rel);
    exec::datum_get_wholerow_heap_tuple::set(|mcx, datum, tableoid| {
        use types_tuple::backend_access_common_heaptuple::{Datum, FormedTuple};
        let mut formed = match datum {
            Datum::Composite(t) => t.clone_in(mcx)?,
            other => FormedTuple::from_datum_image(mcx, &other.as_varlena_bytes())?,
        };
        formed.tuple.t_self = types_tuple::heaptuple::ItemPointerData::invalid();
        formed.tuple.t_tableOid = tableoid;
        Ok(formed)
    });

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

    // ON CONFLICT DO UPDATE field-projection seams over the `OnConflictSetState`
    // (`resultRelInfo->ri_onConflict`) built by ExecInitModifyTable.

    // `existing = resultRelInfo->ri_onConflict->oc_Existing;`
    insert::oc_existing_slot::set(|estate, rri| {
        estate
            .result_rel(rri)
            .ri_onConflict
            .as_deref()
            .and_then(|oc| oc.oc_Existing)
            .expect("ExecOnConflictUpdate: ri_onConflict->oc_Existing is NULL")
    });

    // `resultRelInfo->ri_onConflict->oc_ProjSlot`
    insert::oc_proj_slot::set(|estate, rri| {
        estate
            .result_rel(rri)
            .ri_onConflict
            .as_deref()
            .and_then(|oc| oc.oc_ProjSlot)
            .expect("ExecOnConflictUpdate: ri_onConflict->oc_ProjSlot is NULL")
    });

    // econtext->ecxt_scantuple = existing; ecxt_innertuple = excludedSlot;
    // ecxt_outertuple = NULL  (install ON CONFLICT tuples for SET WHERE/projection).
    insert::oc_set_econtext_tuples::set(|estate, mtstate, existing, excluded_slot| {
        let econtext = mtstate
            .ps
            .ps_ExprContext
            .expect("ON CONFLICT DO UPDATE node has an expression context");
        let ecxt = estate.ecxt_mut(econtext);
        ecxt.ecxt_scantuple = Some(existing);
        ecxt.ecxt_innertuple = Some(excluded_slot);
        ecxt.ecxt_outertuple = None;
    });

    // `ExecQual(resultRelInfo->ri_onConflict->oc_WhereClause, econtext)` — a NULL
    // WHERE clause is always-true.
    insert::exec_qual_oc_where::set(|estate, mtstate, rri| {
        let econtext = mtstate
            .ps
            .ps_ExprContext
            .expect("ON CONFLICT DO UPDATE node has an expression context");
        // The pooled ResultRelInfo and the EState are aliased by `&mut estate`,
        // so detach the compiled WHERE-clause ExprState out of the pool to
        // satisfy the borrow checker, run ExecQual, then restore it. A shallow
        // `.clone()` of the ExprState would NOT work: `ExprState::clone` is a
        // handle-only clone that resets the compiled `steps`/`resultslot` to
        // None, so ExecQual would evaluate an empty program. The qual's
        // identity/contents are unchanged by evaluation, so take/restore is
        // sound (same pattern as exec_project_oc below).
        let mut where_clause = estate
            .result_rel_mut(rri)
            .ri_onConflict
            .as_deref_mut()
            .and_then(|oc| oc.oc_WhereClause.take());
        let result = match where_clause.as_mut() {
            Some(state) => {
                backend_executor_execExpr_seams::exec_qual::call(state, econtext, estate)
            }
            // NULL WHERE clause is always-true.
            None => Ok(true),
        };

        if let Some(oc) = estate.result_rel_mut(rri).ri_onConflict.as_deref_mut() {
            oc.oc_WhereClause = where_clause;
        }

        result
    });

    // `ExecProject(resultRelInfo->ri_onConflict->oc_ProjInfo)` — project the new
    // tuple version into oc_ProjSlot, returning that slot.
    insert::exec_project_oc::set(|estate, rri| {
        // The pooled ResultRelInfo and the EState are aliased by `&mut estate`,
        // so detach the compiled projection out of the pool to satisfy the
        // borrow checker, run ExecProject, then restore it. A shallow `.clone()`
        // of the ProjectionInfo would NOT work: `ExprState::clone` is a
        // handle-only clone that resets `resultslot`/`steps` to None (the
        // compiled program is never deep-copied), which would make ExecProject
        // panic with "ProjectionInfo's ExprState has no resultslot". The
        // projection's identity/contents are unchanged by evaluation —
        // ExecProject only fills its result slot.
        let mut proj = estate
            .result_rel_mut(rri)
            .ri_onConflict
            .as_deref_mut()
            .and_then(|oc| oc.oc_ProjInfo.take())
            .expect("ExecOnConflictUpdate: ri_onConflict->oc_ProjInfo is NULL");

        let result = backend_executor_execExpr_seams::exec_project_info::call(&mut proj, estate);

        if let Some(oc) = estate.result_rel_mut(rri).ri_onConflict.as_deref_mut() {
            oc.oc_ProjInfo = Some(proj);
        }

        result
    });

    // `InstrCountFiltered1(&mtstate->ps, n)` — bump nfiltered1 if instrumented.
    insert::instr_count_filtered1::set(|mtstate, n| {
        if let Some(instr) = mtstate.ps.instrument.as_deref_mut() {
            instr.nfiltered1 += n as f64;
        }
    });

    // `resultRelInfo->ri_needLockTagTuple`
    insert::ri_need_lock_tag_tuple::set(|estate, rri| {
        estate.result_rel(rri).ri_needLockTagTuple
    });

    // `resultRelInfo->ri_WithCheckOptions != NIL`
    insert::ri_has_with_check_options::set(|estate, rri| {
        estate
            .result_rel(rri)
            .ri_WithCheckOptions
            .as_ref()
            .map(|l| !l.is_empty())
            .unwrap_or(false)
    });

    // `ExecWithCheckOptions(kind, resultRelInfo, slot, estate)` — delegate to the
    // execMain owner seam.
    insert::exec_with_check_options::set(|estate, kind, rri, slot| {
        backend_executor_execMain_seams::exec_with_check_options::call(estate, kind, rri, slot)
    });

    // `GetCurrentTransactionId()` then `SpeculativeInsertionLockAcquire(xid)`
    // (nodeModifyTable.c ExecInsert ON CONFLICT arbiter path): acquire this
    // backend's speculative-insertion lock and return its token.
    insert_exec::speculative_insertion_lock_acquire::set(|| {
        let xid = backend_access_transam_xact_seams::get_current_transaction_id::call()?;
        backend_storage_lmgr_lmgr_seams::speculative_insertion_lock_acquire::call(xid)
    });

    // `SpeculativeInsertionLockRelease(GetCurrentTransactionId())`.
    insert_exec::speculative_insertion_lock_release::set(|| {
        let xid = backend_access_transam_xact_seams::get_current_transaction_id::call()?;
        backend_storage_lmgr_lmgr_seams::speculative_insertion_lock_release::call(xid)
    });

    // `table_tuple_insert_speculative(rel, slot, cid, options, bistate,
    // specToken)` — insert the slot speculatively, stamped with the token.
    insert_exec::table_tuple_insert_speculative::set(
        |estate, rri, slot, cid, options, spec_token| {
            let rel = crate::exec::relation_alias(estate, rri);
            let mcx = estate.es_query_cxt;
            let inslot = estate.slot_data_mut(slot);
            backend_access_table_tableam::table_tuple_insert_speculative(
                mcx, &rel, inslot, cid, options, None, spec_token,
            )
        },
    );

    // `table_tuple_complete_speculative(rel, slot, specToken, succeeded)` —
    // finish (succeeded) or kill the speculatively inserted tuple.
    insert_exec::table_tuple_complete_speculative::set(
        |estate, rri, slot, spec_token, succeeded| {
            let rel = crate::exec::relation_alias(estate, rri);
            let mcx = estate.es_query_cxt;
            let inslot = estate.slot_data_mut(slot);
            backend_access_table_tableam::table_tuple_complete_speculative(
                mcx, &rel, inslot, spec_token, succeeded,
            )
        },
    );

    // `table_tuple_lock(rel, tid, snapshot, slot, cid, mode, LockWaitBlock, 0,
    // tmfd)` — ON CONFLICT locks the conflicting tuple with no FIND_LAST_VERSION.
    insert::table_tuple_lock::set(|estate, rri, tid, snapshot, slot, cid, mode, tmfd| {
        let rel = crate::exec::relation_alias(estate, rri);
        let mcx = estate.es_query_cxt;
        let inslot = estate.slot_data_mut(slot);
        backend_access_table_tableam::table_tuple_lock(
            mcx,
            &rel,
            tid,
            &snapshot,
            inslot,
            cid,
            mode,
            types_tableam::tableam::LockWaitPolicy::LockWaitBlock,
            0,
            tmfd,
        )
    });

    // `table_tuple_fetch_row_version(rel, tid, SnapshotAny, slot)`
    insert::table_tuple_fetch_row_version_any::set(|estate, rri, tid, slot| {
        let rel = crate::exec::relation_alias(estate, rri);
        let mcx = estate.es_query_cxt;
        let snapshot_any = crate::exec::snapshot_any();
        let inslot = estate.slot_data_mut(slot);
        backend_access_table_tableam::table_tuple_fetch_row_version(
            mcx,
            &rel,
            tid,
            &snapshot_any,
            inslot,
        )
    });

    // Same fetch, used by the DELETE ... RETURNING path (delete_exec).
    delete_exec::table_tuple_fetch_row_version_any::set(|estate, rri, tid, slot| {
        let rel = crate::exec::relation_alias(estate, rri);
        let mcx = estate.es_query_cxt;
        let snapshot_any = crate::exec::snapshot_any();
        let inslot = estate.slot_data_mut(slot);
        backend_access_table_tableam::table_tuple_fetch_row_version(
            mcx,
            &rel,
            tid,
            &snapshot_any,
            inslot,
        )
    });

    // `slot_getsysattr(slot, MinTransactionIdAttributeNumber, &isnull)` then
    // `DatumGetTransactionId` — the slot tuple's xmin.
    insert::slot_get_xmin::set(|estate, slot| {
        let mcx = estate.es_query_cxt;
        let s = estate.slot_data_mut(slot);
        let (datum, isnull) = backend_executor_execTuples_seams::slot_getsysattr::call(
            mcx,
            s,
            types_tuple::heaptuple::MinTransactionIdAttributeNumber,
        )?;
        Ok((datum.as_u32(), isnull))
    });

    // The ON CONFLICT path re-uses the same small EState/slot/xact projections
    // declared in the `insert` module (distinct seam slots from the `de`
    // family); install them with the identical bodies.

    // `context->estate->es_snapshot`
    insert::es_snapshot::set(|estate| estate.es_snapshot.as_deref().cloned());

    // `IsolationUsesXactSnapshot()` (xact.h).
    insert::isolation_uses_xact_snapshot::set(|| {
        backend_access_transam_xact_seams::isolation_uses_xact_snapshot::call()
    });

    // `ExecClearTuple(slot)` (execTuples.c).
    insert::exec_clear_tuple::set(|estate, slot| {
        backend_executor_execTuples_seams::exec_clear_tuple::call(estate, slot)
    });

    // `ExecMaterializeSlot(slot)` (execTuples.c).
    insert::exec_materialize_slot::set(|estate, slot| {
        backend_executor_execTuples_seams::exec_materialize_slot::call(estate, slot)
    });

    // `*returning != NULL && ri_projectReturning->pi_state.flags & EEO_FLAG_HAS_OLD`
    insert::ri_returning_has_old::set(|estate, rri| {
        estate
            .result_rel(rri)
            .ri_projectReturning
            .as_ref()
            .map(|p| (p.pi_state.flags & types_nodes::execexpr::EEO_FLAG_HAS_OLD) != 0)
            .unwrap_or(false)
    });

    install_delete_seams();
}

/// Install the `ExecDelete` (`delete_exec`) within-crate seams — the trimmed
/// `ResultRelInfo`/`EState` field reads and the slot/snapshot/isolation
/// delegations the DELETE driver reaches. Concurrency (EPQ / `table_tuple_lock`)
/// and FDW/cross-partition seams that bottom out on the unported execMain owner
/// stay uninstalled (loud-panic on their genuinely-unreachable paths).
fn install_delete_seams() {
    use delete_exec as de;

    // `resultRelInfo->ri_TrigDesc && ...->trig_delete_instead_row` (reltrigger.h).
    de::ri_has_instead_delete_row::set(|estate, rri| {
        estate
            .result_rel(rri)
            .ri_TrigDesc
            .as_ref()
            .map(|td| td.trig_delete_instead_row)
            .unwrap_or(false)
    });

    // `resultRelInfo->ri_FdwRoutine != NULL` (execnodes.h).
    de::ri_has_fdw_routine::set(|estate, rri| estate.result_rel(rri).ri_has_fdw_routine);

    // `resultRelInfo->ri_projectReturning != NULL` (execnodes.h).
    de::ri_has_project_returning::set(|estate, rri| {
        estate.result_rel(rri).ri_has_project_returning
    });

    // `resultRelInfo->ri_projectReturning->pi_state.flags & EEO_FLAG_HAS_OLD`.
    de::ri_returning_has_old::set(|estate, rri| {
        estate
            .result_rel(rri)
            .ri_projectReturning
            .as_ref()
            .map(|p| {
                (p.pi_state.flags & types_nodes::execexpr::EEO_FLAG_HAS_OLD) != 0
            })
            .unwrap_or(false)
    });

    // `RelationGetRelid(resultRelInfo->ri_RelationDesc)` (rel.h).
    de::ri_relation_relid::set(|estate, rri| {
        estate
            .result_rel(rri)
            .ri_RelationDesc
            .as_ref()
            .expect("ri_relation_relid: ri_RelationDesc must be open")
            .rd_id
    });

    // `IsolationUsesXactSnapshot()` (xact.h).
    de::isolation_uses_xact_snapshot::set(|| {
        backend_access_transam_xact_seams::isolation_uses_xact_snapshot::call()
    });

    // `context->estate->es_snapshot` (execnodes.h).
    de::es_snapshot::set(|estate| estate.es_snapshot.as_deref().cloned());

    // `TTS_EMPTY(slot)` (tuptable.h).
    de::slot_is_empty::set(|estate, slot| estate.slot_data(slot).base().is_empty());

    // `TupIsNull(slot)` — true when the slot is empty/NULL (tuptable.h).
    de::slot_is_null::set(|estate, slot| estate.slot_data(slot).base().is_empty());

    // `ExecStoreAllNullTuple(slot)` (execTuples.c).
    de::exec_store_all_null_tuple::set(|estate, slot| {
        backend_executor_execTuples_seams::exec_store_all_null_tuple::call(estate, slot)
    });

    // `slot->tts_tableOid = relid` (tuptable.h).
    de::slot_set_table_oid::set(|estate, slot, relid| {
        estate.slot_mut(slot).tts_tableOid = relid;
    });

    // `ExecMaterializeSlot(slot)` (execTuples.c).
    de::exec_materialize_slot::set(|estate, slot| {
        backend_executor_execTuples_seams::exec_materialize_slot::call(estate, slot)
    });

    // `ExecClearTuple(slot)` (execTuples.c).
    de::exec_clear_tuple::set(|estate, slot| {
        backend_executor_execTuples_seams::exec_clear_tuple::call(estate, slot)
    });

    // `ExecGetReturningSlot(estate, relinfo)` (execMain.c).
    de::exec_get_returning_slot::set(|estate, rri| {
        backend_executor_execMain_seams::exec_get_returning_slot::call(estate, rri)
    });

    // `ExecLookupResultRelByOid` (nodeModifyTable.c) is homed in this crate
    // (it owns `ModifyTableState`), but its consumer is execPartition's
    // tuple-routing (`ExecFindPartition`), which reaches it through the
    // execMain-seams declaration. Install the real body here.
    backend_executor_execMain_seams::exec_lookup_result_rel_by_oid::set(
        |node, estate, resultoid, missing_ok, update_cache| {
            lifecycle::ExecLookupResultRelByOid(
                node,
                estate,
                resultoid,
                missing_ok,
                update_cache,
            )
        },
    );
}
