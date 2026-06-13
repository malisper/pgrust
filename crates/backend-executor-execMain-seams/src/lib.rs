//! Seam declarations for the `backend-executor-execMain` unit
//! (`executor/execMain.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

// NOTE: the former `epq_relsubs_done` / `epq_relsubs_slot_present` /
// `epq_relsubs_rowmark_present` / `epq_set_relsubs_done` / `epq_load_relsubs_slot`
// / `eval_plan_qual_fetch_row_mark` / `epq_param_is_member_of_ext_param` seams
// took an opaque `EPQStateHandle`. With the real owned `EPQState` now living in
// `EStateData::es_epq_active`, those pure reads/writes are direct field accesses
// on the owned struct (see nodeIndexonlyscan / nodeForeignscan), and the EPQ
// fetch helpers land with the EvalPlanQual (execMain.c) port itself. They are
// removed here per docs/types.md rule 3 (lands with first consumer).

seam_core::seam!(
    /// `EvalPlanQualFetchRowMark(epqstate, rti, slot)` (execMain.c): fetch the
    /// replacement tuple for the non-locking rowmark of relation `rti` (the
    /// scan node's `scanrelid`) into `slot` (the node's scan slot id),
    /// returning `false` when the row no longer exists (the C `return false`).
    /// Reads the active `EPQState` from `estate.es_epq_active`. Owner:
    /// `backend-executor-execMain` (the EvalPlanQual machinery); fallible on
    /// `ereport(ERROR)`.
    pub fn eval_plan_qual_fetch_row_mark<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        rti: types_core::primitive::Index,
        slot: types_nodes::SlotId,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `fetch_cursor_param_value`'s live-state core (execCurrent.c): read
    /// `econtext->ecxt_param_list_info->params[param_id - 1]` (calling the
    /// dynamic `paramFetch` hook when present), and for an OID-valid, non-NULL
    /// param classify its type — decoding the `refcursor` text Datum via
    /// `TextDatumGetCString` (palloc'd in `mcx`) when `ptype == REFCURSOROID`.
    /// `Ok(None)` is the C fall-through to "no value found for parameter" —
    /// i.e. no param list (`paramInfo == NULL`), `param_id` past `numParams`,
    /// or the resolved param is OID-invalid/NULL
    /// (`!OidIsValid(prm->ptype) || prm->isnull`). The caller has already
    /// checked `param_id > 0`; this seam owns the `paramInfo != NULL &&
    /// param_id <= numParams` bound against the live `ParamListInfo`. Owner:
    /// `backend-executor-execMain` (the live `ExprContext`/`ParamListInfo`
    /// navigation + the `paramFetch` hook dispatch).
    pub fn fetch_cursor_param<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        econtext: &types_nodes::ExprContext<'mcx>,
        param_id: i32,
    ) -> types_error::PgResult<Option<types_nodes::FetchedCursorParam<'mcx>>>
);

seam_core::seam!(
    /// `execCurrentOf` plain-scan TID extraction (execCurrent.c): once
    /// `search_plan_tree` has located the scan node and the
    /// `TupIsNull`/`pending_rescan` inactive test has passed, dig the TID out of
    /// the scan's current physical tuple. For an `IndexOnlyScanState`
    /// (`is_index_only`), read `ioss_ScanDesc->xs_heaptid`; otherwise read the
    /// scan tuple slot's `SelfItemPointerAttributeNumber` via `slot_getsysattr`
    /// (with the `USE_ASSERT_CHECKING` tableoid cross-check). Returns the
    /// discriminated [`ScanTidOutcome`] — a null self-ctid is the C
    /// "not a simply updatable scan" path. Owner: `backend-executor-execMain`
    /// (the live concrete scan-node states + execTuples `slot_getsysattr`).
    pub fn scan_node_extract_tid(
        estate: &types_nodes::EStateData,
        scan_tuple_slot: Option<types_nodes::SlotId>,
        is_index_only: bool,
    ) -> types_error::PgResult<types_nodes::ScanTidOutcome>
);

seam_core::seam!(
    /// `InitResultRelInfo(resultRelInfo, resultRelationDesc,
    /// resultRelationIndex, partition_root_rri, instrument_options)`
    /// (execMain.c): fill a `ResultRelInfo` for the given target relation
    /// (an alias handle of the relation `es_relations` owns, stored into
    /// `ri_RelationDesc`). Allocates trigger bookkeeping arrays in `mcx`
    /// when the relation has triggers (fallible on OOM); reads the
    /// relation's trigdesc through the relcache.
    pub fn init_result_rel_info<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        result_rel_info: &mut types_nodes::ResultRelInfo<'mcx>,
        relation: types_rel::Relation<'mcx>,
        result_relation_index: types_core::primitive::Index,
        partition_root_rri: Option<types_nodes::RriId>,
        instrument_options: i32,
    ) -> types_error::PgResult<()>
);

/// Outcome of the EvalPlanQual branch of `ExecScanFetch` (execScan.h).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EpqScanFetch {
    /// The EPQ branch did not apply for this rel (the `scanrelid == 0` /
    /// not-a-pushed-join-descendant fall-through): run the node's access
    /// method instead.
    FallThrough,
    /// The EPQ branch produced a result: return this slot (the node's scan
    /// slot id), or `None` for the C `NULL` (empty result).
    Result(Option<types_nodes::SlotId>),
    /// The EPQ branch wants the caller to apply its access-method recheck to
    /// the node's scan slot, then (if the recheck fails) clear the slot; the
    /// `bool` mirrors the C "would not be returned by scan" clear flag. The
    /// caller returns the scan slot id when recheck passes, else the cleared
    /// slot per the embedded directive.
    Recheck { clear_on_fail: bool },
}

seam_core::seam!(
    /// The EvalPlanQual branch of `ExecScanFetch` (execScan.h): with an active
    /// `EPQState` (`estate.es_epq_active`), decide what the scan should return
    /// for this rel (`scanrelid`) — the replacement/test tuple, an empty slot,
    /// or a fall-through to the access method — performing the
    /// `relsubs_done`/`relsubs_slot`/`relsubs_rowmark` bookkeeping and any
    /// rowmark fetch. Returns an [`EpqScanFetch`] directive; the access-method
    /// recheck stays with the calling node (it owns `recheckMtd`). Fallible on
    /// `ereport(ERROR)`.
    pub fn exec_scan_fetch_epq<'mcx>(
        scanstate: &mut types_nodes::execnodes::ScanStateData<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        scanrelid: types_core::primitive::Index,
    ) -> types_error::PgResult<EpqScanFetch>
);

seam_core::seam!(
    /// `ExecBuildSlotValueDescription(reloid, slot, tupdesc, modifiedCols,
    /// maxfieldlen)` (execMain.c): build a "(col, ...) = (val, ...)"
    /// description of the slot's contents, limited to the columns the current
    /// user has SELECT rights on (all columns when `modified_cols` names only
    /// accessible ones); `Ok(None)` when permissions allow no column (the C
    /// NULL). The string is allocated in `mcx`; column out-functions can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn exec_build_slot_value_description<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        reloid: types_core::Oid,
        slot: &types_nodes::TupleTableSlot,
        tupdesc: &types_tuple::heaptuple::TupleDescData<'_>,
        modified_cols: Option<&types_nodes::Bitmapset<'_>>,
        maxfieldlen: i32,
    ) -> types_error::PgResult<Option<mcx::PgString<'mcx>>>
);

seam_core::seam!(
    /// `ExecPartitionCheck(resultRelInfo, slot, estate, emitError)`
    /// (execMain.c): check the partition constraint of `result_rel_info`
    /// (id into the EState `ResultRelInfo` pool) against the tuple in `slot`
    /// (id into the EState slot pool). With `emit_error = true` a failing
    /// constraint is `ereport(ERROR)` (carried on `Err`) and the bool is
    /// always `true`; with `emit_error = false` it returns whether the
    /// constraint passed. Evaluating the constraint expression can also
    /// `ereport(ERROR)`.
    pub fn exec_partition_check<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        result_rel_info: types_nodes::RriId,
        slot: types_nodes::SlotId,
        emit_error: bool,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `ExecLookupResultRelByOid(node, resultoid, missing_ok, update_cache)`
    /// (execMain.c): find the `ResultRelInfo` already known to the
    /// `ModifyTableState` for the relation `resultoid`, returning its EState
    /// pool id, or `None` (the C `NULL`) when not found and `missing_ok` is
    /// true. With `missing_ok = false` a miss is the C `elog(ERROR, "incorrect
    /// result relation OID %u")`, carried on `Err`.
    pub fn exec_lookup_result_rel_by_oid<'mcx>(
        node: &mut types_nodes::ModifyTableState<'mcx>,
        resultoid: types_core::Oid,
        missing_ok: bool,
        update_cache: bool,
    ) -> types_error::PgResult<Option<types_nodes::RriId>>
);

seam_core::seam!(
    /// `CheckValidResultRel(resultRelInfo, operation, onConflictAction,
    /// mergeActions)` (execMain.c): verify the result relation (id into the
    /// EState pool) is a valid target for the given command, raising the
    /// appropriate `ereport(ERROR)` otherwise (carried on `Err`). The
    /// `merge_actions` list is passed empty by the partition-routing caller.
    pub fn check_valid_result_rel<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        result_rel_info: types_nodes::RriId,
        operation: types_nodes::nodes::CmdType,
        on_conflict_action: types_nodes::nodes::OnConflictAction,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// The COPY-(query)-TO executor setup (copyto.c:838-850):
    /// `CreateQueryDesc(plan, sourceText, GetActiveSnapshot(),
    /// InvalidSnapshot, dest, NULL, NULL, 0)` then `ExecutorStart(queryDesc,
    /// 0)`, which computes the result tupdesc. `copy_receiver` is the COPY-OUT
    /// `DestReceiver` handle the caller built (`CreateCopyDestReceiver`, whose
    /// `cstate` it has already associated). The active snapshot is the copied
    /// one the caller has just pushed (copyto.c:830-831). Returns the started
    /// `QueryDesc` (its `tupDesc` set, `exec_token` the executor's handle).
    /// `Err` carries any `ExecutorStart` `ereport(ERROR)`.
    pub fn create_query_desc_and_start<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        plan: types_nodes::nodeindexscan::PlannedStmt<'mcx>,
        source_text: &str,
        copy_receiver: u64,
    ) -> types_error::PgResult<types_nodes::copy_query::QueryDesc<'mcx>>
);

seam_core::seam!(
    /// `ExecutorRun(queryDesc, ForwardScanDirection, 0)` (copyto.c:1104) for the
    /// COPY-(query)-TO path: run the plan to completion; the COPY-OUT receiver
    /// emits each tuple into copyto's `cstate` (incrementing
    /// `cstate.receiver_processed`, the C `((DR_copy *) dest)->processed`). The
    /// processed count is read by copyto from its own `cstate` after the run, so
    /// it is *not* returned here. `Err` carries execution `ereport(ERROR)`s.
    pub fn executor_run_copy(exec_token: u64) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// The COPY-(query)-TO teardown (copyto.c:1010-1012): `ExecutorFinish` +
    /// `ExecutorEnd` + `FreeQueryDesc` for the started query. `Err` carries any
    /// teardown `ereport(ERROR)`.
    pub fn end_copy_query(exec_token: u64) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecutorRun(queryDesc, direction, count)` (execMain.c) — run the
    /// executor, sending tuples to `queryDesc->dest`. Runs the plan; can
    /// `ereport(ERROR)`. (`once` defaulted to false here, as in
    /// `PersistHoldablePortal`'s call.)
    pub fn executor_run(
        query_desc: &mut types_portal::QueryDesc,
        direction: types_scan::sdir::ScanDirection,
        count: u64,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecutorFinish(queryDesc)` (execMain.c) — run the executor's
    /// after-query cleanup (AFTER triggers etc.). Can `ereport(ERROR)`.
    pub fn executor_finish(query_desc: &mut types_portal::QueryDesc) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecutorEnd(queryDesc)` (execMain.c) — shut down the executor and free
    /// its per-query state. Can `ereport(ERROR)`.
    pub fn executor_end(query_desc: &mut types_portal::QueryDesc) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecutorRewind(queryDesc)` (execMain.c) — rewind the executor to the
    /// start of the query so it can be re-run. Can `ereport(ERROR)`.
    pub fn executor_rewind(query_desc: &mut types_portal::QueryDesc) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `FreeQueryDesc(queryDesc)` (pquery.c, reached through the executor
    /// surface) — free a finished `QueryDesc` (consumes it).
    pub fn free_query_desc(query_desc: types_portal::QueryDesc) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecSupportsBackwardScan(plan)` (execAmi.c) — does the plan tree
    /// support backward scanning? Pure structural inspection (no ereport in
    /// practice, but the index-AM probe path can error), so fallible.
    pub fn exec_supports_backward_scan(
        plan: &types_nodes::nodeindexscan::PlannedStmt<'_>,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `ExecCheckPermissions(rangeTable, rteperminfos, ereport_on_violation)`
    /// (execMain.c) for `RI_Initial_Check`'s SELECT probe on two relations.
    /// Each entry is `(relid, relkind, selected_col_attnums)` for an
    /// `RTE_RELATION` requiring `ACL_SELECT` on the listed columns
    /// (`AccessShareLock`); the owner builds the `RangeTblEntry` /
    /// `RTEPermissionInfo` nodes and runs the access-method-level permission
    /// check. Returns `true` if every check passes; with
    /// `ereport_on_violation = false` a denial is `Ok(false)`. Catalog/ACL
    /// lookups can `ereport(ERROR)`.
    pub fn exec_check_permissions_select(
        rels: &[(types_core::Oid, u8, &[i16])],
        ereport_on_violation: bool,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `resultRelInfo->ri_FdwRoutine->ExecForeignUpdate(estate, resultRelInfo,
    /// slot, planSlot)` (fdwapi): dispatch an UPDATE to the foreign-table FDW
    /// via the per-relation `FdwRoutine` vtable carried on the pooled
    /// `ResultRelInfo`. Returns the (possibly replaced) result slot, or
    /// `Ok(None)` for the FDW "do nothing". Resolved when the fdwapi type
    /// lands; owner-coverage placeholder until then.
    pub fn exec_foreign_update<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        result_rel_info: types_nodes::RriId,
        slot: types_nodes::SlotId,
        plan_slot: Option<types_nodes::SlotId>,
    ) -> types_error::PgResult<Option<types_nodes::SlotId>>
);

seam_core::seam!(
    /// `ExecPartitionCheckEmitError(resultRelInfo, slot, estate)` (execMain.c):
    /// build and raise the partition-constraint-violation error for `slot`.
    /// Always `ereport(ERROR)`s (only called when the constraint is known to
    /// have failed).
    pub fn exec_partition_check_emit_error<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        result_rel_info: types_nodes::RriId,
        slot: types_nodes::SlotId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecConstraints(resultRelInfo, slot, estate)` (execMain.c): check the
    /// not-null and CHECK constraints of the target relation against `slot`,
    /// `ereport(ERROR)`ing on the first violation.
    pub fn exec_constraints<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        result_rel_info: types_nodes::RriId,
        slot: types_nodes::SlotId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecWithCheckOptions(kind, resultRelInfo, slot, estate)` (execMain.c):
    /// evaluate the WITH CHECK OPTION / RLS policies of the given `kind`
    /// (`WCOKind` enum value) on `slot`, `ereport(ERROR)`ing on a violation.
    /// Skips WCOs of other kinds.
    pub fn exec_with_check_options<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        kind: i32,
        result_rel_info: types_nodes::RriId,
        slot: types_nodes::SlotId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecGetReturningSlot(estate, relInfo)` (execMain.c): get (lazily
    /// creating) the per-relation slot used to hold a tuple for RETURNING.
    pub fn exec_get_returning_slot<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        result_rel_info: types_nodes::RriId,
    ) -> types_error::PgResult<types_nodes::SlotId>
);

seam_core::seam!(
    /// `ExecGetChildToRootMap(resultRelInfo)` (execMain.c): compute lazily the
    /// tuple-conversion map from the child partition rowtype to the root's.
    /// `Ok(true)` means a conversion is needed and the map now lives on the
    /// pooled `ResultRelInfo` (`ri_ChildToRootMap`); `Ok(false)` is the C
    /// `NULL` map (rowtypes already match).
    pub fn exec_get_child_to_root_map<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        result_rel_info: types_nodes::RriId,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `ExecGetAncestorResultRels(estate, resultRelInfo)` (execMain.c): return
    /// the chain of ancestor `ResultRelInfo`s (root-ward, inclusive of the
    /// root) for a partition's `ResultRelInfo`, lazily opening them.
    pub fn exec_get_ancestor_result_rels<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        result_rel_info: types_nodes::RriId,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, types_nodes::RriId>>
);

seam_core::seam!(
    /// `ExecUpdateLockMode(estate, relinfo)` (execMain.c): determine the
    /// row-lock mode needed for an UPDATE of `relinfo`, based on which columns
    /// the update touches vs. the relation's key columns.
    pub fn exec_update_lock_mode<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        result_rel_info: types_nodes::RriId,
    ) -> types_error::PgResult<types_tableam::tableam::LockTupleMode>
);

seam_core::seam!(
    /// `EvalPlanQual(epqstate, relation, rti, inputslot)` (execMain.c): run the
    /// EvalPlanQual recheck for a concurrently-updated tuple, returning the
    /// re-projected slot that still passes the quals, or `Ok(None)` when the
    /// row no longer qualifies. The EPQ state lives on the owning
    /// `ModifyTableState`; the owner reads `es_snapshot` etc. off the estate.
    pub fn eval_plan_qual<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        epqstate: &mut types_nodes::modifytable::EPQState<'mcx>,
        result_rel_info: types_nodes::RriId,
        rti: types_core::primitive::Index,
        inputslot: types_nodes::SlotId,
    ) -> types_error::PgResult<Option<types_nodes::SlotId>>
);

seam_core::seam!(
    /// `EvalPlanQualSlot(epqstate, relation, rti)` (execMain.c): get (lazily
    /// creating) the EPQ test slot for the given range-table relation.
    pub fn eval_plan_qual_slot<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        epqstate: &mut types_nodes::modifytable::EPQState<'mcx>,
        result_rel_info: types_nodes::RriId,
        rti: types_core::primitive::Index,
    ) -> types_error::PgResult<types_nodes::SlotId>
);

seam_core::seam!(
    /// `ExecInitResultRelation(estate, resultRelInfo, rti)` (execMain.c): open
    /// the `rti`'th range-table relation (via `ExecGetRangeTableRelation`) and
    /// fill the pooled `ResultRelInfo` for it (`InitResultRelInfo`), recording it
    /// in `es_result_relations[rti-1]` and prepending it to
    /// `es_opened_result_relations`. The `ResultRelInfo` is addressed by its
    /// EState-pool id. Reads `es_relations`/`es_range_table` and the relcache;
    /// fallible on `ereport(ERROR)` and OOM.
    pub fn exec_init_result_relation<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        result_rel_info: types_nodes::RriId,
        rti: types_core::primitive::Index,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `EvalPlanQualInit(epqstate, parentestate, subplan, auxrowmarks, epqParam,
    /// resultRelations)` (execMain.c): initialize the canonical `EPQState` with
    /// dummy subplan data, recording `epqParam` and the `resultRelations` integer
    /// list. The owned model passes the canonical `EPQState` by mutable
    /// reference. Allocates EPQ bookkeeping in the per-query context; fallible
    /// on OOM.
    pub fn eval_plan_qual_init<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        epqstate: &mut types_nodes::modifytable::EPQState<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        epq_param: i32,
        result_relations: &[types_core::primitive::Index],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `EvalPlanQualSetPlan(epqstate, subplan, auxrowmarks)` (execMain.c): record
    /// the recheck plan tree and the aux-rowmark list on the canonical
    /// `EPQState`. The owned model passes the (shared, read-only) subplan node by
    /// borrow and the canonical `EPQState` by mutable reference; the aux-rowmark
    /// list is currently always NIL at the nodeModifyTable call site. Fallible on
    /// OOM.
    pub fn eval_plan_qual_set_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        epqstate: &mut types_nodes::modifytable::EPQState<'mcx>,
        subplan: Option<&'mcx types_nodes::nodes::Node<'mcx>>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// The `arowmarks` build loop of `ExecInitModifyTable` (execMain.c):
    /// `foreach(l, node->rowMarks) { PlanRowMark *rc = ...; if (rc->isParent)
    /// continue; rte = exec_rt_fetch(rc->rti, estate); if (rte->rtekind ==
    /// RTE_RELATION && !bms_is_member(rc->rti, es_unpruned_relids)) continue;
    /// erm = ExecFindRowMark(estate, rc->rti, false); aerm =
    /// ExecBuildAuxRowMark(erm, subplan->targetlist); arowmarks =
    /// lappend(arowmarks, aerm); }`. The `PlanRowMark` plan-node type, the
    /// `ExecFindRowMark`/`ExecBuildAuxRowMark` constructors, and the
    /// `ExecAuxRowMark` list are all execMain's; the owner reads the rowMarks
    /// off the plan node and records the resulting aux-rowmark list directly on
    /// the canonical `EPQState` (the C passes `arowmarks` into
    /// `EvalPlanQualSetPlan`). A no-op when the plan carries no (non-parent,
    /// unpruned) rowMarks. Reads the range table; fallible on `ereport(ERROR)`
    /// and OOM.
    pub fn eval_plan_qual_set_plan_with_row_marks<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        epqstate: &mut types_nodes::modifytable::EPQState<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        row_marks: &[mcx::PgBox<'mcx, types_nodes::nodes::Node<'mcx>>],
        subplan: Option<&'mcx types_nodes::nodes::Node<'mcx>>,
    ) -> types_error::PgResult<()>
);
