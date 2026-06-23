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

// `ExecuteCallStmt` (functioncmds.c) is owned by `backend-commands-functioncmds`
// and installed on `utility_out_seams::execute_call_stmt`, reading
// the live `T_CallStmt` node's transformed `funcexpr`/`outargs` directly (the
// rich call tree transformCallStmt populates). It is not an execMain seam.

// ---------------------------------------------------------------------------
// PARAM_EXEC `execPlan`-link plumbing. These operate on the executor-owned
// `es_param_exec_vals` (the `ParamExecData.execPlan` link, now modeled as an
// `ExecPlanLink` identity into `es_subplanstates`) and `es_subplanstates`, so
// they are execMain machinery — NOT `execProcnode.c` functions. nodeSubplan was
// their first consumer (which is why they were originally parked in
// execProcnode-seams); they live here under their real owner. The three
// field-level ops are installed by execMain::init_seams; the two
// SubPlanState-resolving ops stay seam-and-panic until nodeSubplan's
// SubPlanState-reachability wiring lands.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `prm->execPlan = sstate` — mark the PARAM_EXEC slot `paramid` as needing
    /// evaluation by an initplan (nodeSubplan.c `ExecInitSubPlan` /
    /// `ExecReScanSetParamPlan`). The `execPlan` link is modeled on
    /// `ParamExecData` as an [`ExecPlanLink`](::nodes::ExecPlanLink) — the
    /// marking subplan's stable identity (its 1-based `plan_id`, the index into
    /// `es_subplanstates`). The executor owns the param array, so it installs the
    /// link from the subplan's `plan_id`.
    pub fn mark_param_execplan_pending<'mcx>(
        estate: &mut ::nodes::EStateData<'mcx>,
        paramid: i32,
        plan_id: i32,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `prm->execPlan = NULL` — clear the PARAM_EXEC `execPlan` link after the
    /// initplan output has been set (nodeSubplan.c `ExecSetParamPlan`). The link
    /// is executor-owned. Fallible only structurally.
    pub fn clear_param_execplan<'mcx>(
        estate: &mut ::nodes::EStateData<'mcx>,
        paramid: i32,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `econtext->ecxt_param_exec_vals[paramid].execPlan != NULL` — is the
    /// param not yet evaluated? (`ExecSetParamPlanMulti`). Reads the
    /// executor-owned `execPlan` link. Infallible.
    pub fn param_execplan_pending(estate: &::nodes::EStateData<'_>, paramid: i32) -> bool
);

seam_core::seam!(
    /// `ExecSetParamPlan(prm->execPlan, econtext)` for the not-yet-evaluated
    /// PARAM_EXEC `paramid` (`ExecSetParamPlanMulti`): the executor resolves the
    /// `SubPlanState` whose identity is stashed in the param's `execPlan` link and
    /// re-enters `nodeSubplan::ExecSetParamPlan` over it. Fallible (the subplan's
    /// failure surface). The `econtext` is the id of the expression context to
    /// evaluate any down-passed params in.
    pub fn exec_set_param_plan_for_pending<'mcx>(
        econtext: ::nodes::EcxtId,
        paramid: i32,
        estate: &mut ::nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// The "subplan was not initialized" check (nodeSubplan.c:818-827). The
    /// executor owns `es_subplanstates`; in the owned model the `SubPlanState`
    /// reaches its child plan state by the subplan's 1-based `plan_id` index
    /// (`list_nth(es_subplanstates, plan_id - 1)`) rather than holding an
    /// aliasing box, so this seam only verifies the slot was initialized.
    /// `Err` carries the C `elog(ERROR, "subplan was not initialized")` when the
    /// slot is missing.
    pub fn link_subplan_planstate<'mcx>(
        estate: &::nodes::EStateData<'mcx>,
        plan_id: i32,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `EvalPlanQualFetchRowMark(epqstate, rti, slot)` (execMain.c): fetch the
    /// replacement tuple for the non-locking rowmark of relation `rti` (the
    /// scan node's `scanrelid`) into `slot` (the node's scan slot id),
    /// returning `false` when the row no longer exists (the C `return false`).
    /// Reads the active `EPQState` from `estate.es_epq_active`. Owner:
    /// `backend-executor-execMain` (the EvalPlanQual machinery); fallible on
    /// `ereport(ERROR)`.
    pub fn eval_plan_qual_fetch_row_mark<'mcx>(
        estate: &mut ::nodes::EStateData<'mcx>,
        rti: types_core::primitive::Index,
        slot: ::nodes::SlotId,
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
        econtext: &::nodes::ExprContext<'mcx>,
        param_id: i32,
    ) -> types_error::PgResult<Option<::nodes::FetchedCursorParam<'mcx>>>
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
    pub fn scan_node_extract_tid<'mcx, 'a>(
        mcx: mcx::Mcx<'a>,
        estate: &::nodes::EStateData<'mcx>,
        scan_tuple_slot: Option<::nodes::SlotId>,
        index_only_tid: Option<types_tuple::heaptuple::ItemPointerData>,
    ) -> types_error::PgResult<::nodes::ScanTidOutcome>
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
        result_rel_info: &mut ::nodes::ResultRelInfo<'mcx>,
        relation: rel::Relation<'mcx>,
        result_relation_index: types_core::primitive::Index,
        partition_root_rri: Option<::nodes::RriId>,
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
    Result(Option<::nodes::SlotId>),
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
        scanstate: &mut ::nodes::execnodes::ScanStateData<'mcx>,
        estate: &mut ::nodes::EStateData<'mcx>,
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
        slot: &::nodes::TupleTableSlot,
        tupdesc: &types_tuple::heaptuple::TupleDescData<'_>,
        modified_cols: Option<&::nodes::Bitmapset<'_>>,
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
        estate: &mut ::nodes::EStateData<'mcx>,
        result_rel_info: ::nodes::RriId,
        slot: ::nodes::SlotId,
        emit_error: bool,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// The ATTACH-PARTITION leg of `ATRewriteTable` (tablecmds.c): scan the
    /// relation `relid` and verify every live row satisfies the implicit-AND
    /// `part_constraint` (already mapped to `relid`'s attribute numbers). The
    /// first violating row raises `ereport(ERROR, ERRCODE_CHECK_VIOLATION)`,
    /// carried on `Err`; an empty constraint or empty table is a no-op. Owned by
    /// execMain (it builds a throwaway `EState` + `ExecPrepareCheck` + scan loop
    /// + `ExecCheck`).
    pub fn validate_partition_constraint_scan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relid: types_core::primitive::Oid,
        part_constraint: &[::nodes::primnodes::Expr],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// The per-partition scan leg of `check_default_partition_contents`
    /// (partbounds.c): scan one leaf partition `part_relid` of a default
    /// partition and verify every live row satisfies the revised default-partition
    /// `part_constraint` (already mapped to `part_relid`'s attribute numbers). The
    /// first violating row raises `ereport(ERROR, ERRCODE_CHECK_VIOLATION,
    /// "updated partition constraint for default partition \"%s\" would be violated
    /// by some row")`, where `%s` is `default_relname` (the name reported is the
    /// *default* relation's, not the scanned child's, exactly as C does). An empty
    /// constraint or empty table is a no-op. Owned by execMain (it builds a
    /// throwaway `EState` + `ExecPrepareCheck` + scan loop + `ExecCheck`).
    pub fn validate_default_partition_contents_scan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        default_relname: &str,
        part_relid: types_core::primitive::Oid,
        part_constraint: &[::nodes::primnodes::Expr],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ATRewriteTable(tab, OIDNewHeap)` (tablecmds.c) — the phase-3
    /// scan-and-rewrite of one table's heap, owned by execMain (it builds an
    /// `EState` + `ExprState`s + scan loop, evaluating the queued cast/USING/
    /// default expressions and re-checking constraints). The tablecmds caller
    /// has already done `make_new_heap` (passing the transient heap's OID in
    /// `oid_new_heap`; `InvalidOid` for the scan-only verify path) and will do
    /// `finish_heap_swap` afterward.
    ///
    /// `old_desc` is `tab->oldDesc` (the pre-modification descriptor); `rewrite`
    /// is `tab->rewrite`. `newvals` carries each queued `NewColumnValue` as
    /// `(attnum, planned-expr, is_generated)`; `check_constraints` carries each
    /// `CONSTR_CHECK` `NewConstraint` as `(name, cooked-qual-expr)`.
    /// `verify_new_notnull` / `partition_constraint` / `validate_default` mirror
    /// the `AlteredTableInfo` fields. A constraint violation or NOT NULL
    /// violation raises the matching `ereport(ERROR)`, carried on `Err`.
    #[allow(clippy::too_many_arguments)]
    pub fn at_rewrite_table_scan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relid: types_core::primitive::Oid,
        oid_new_heap: types_core::primitive::Oid,
        old_desc: &types_tuple::heaptuple::TupleDescData<'mcx>,
        rewrite: i32,
        newvals: &[(i16, ::nodes::primnodes::Expr, bool)],
        check_constraints: &[(&str, ::nodes::primnodes::Expr)],
        verify_new_notnull: bool,
        partition_constraint: &[::nodes::primnodes::Expr],
        validate_default: bool,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecLookupResultRelByOid(node, resultoid, missing_ok, update_cache)`
    /// (execMain.c): find the `ResultRelInfo` already known to the
    /// `ModifyTableState` for the relation `resultoid`, returning its EState
    /// pool id, or `None` (the C `NULL`) when not found and `missing_ok` is
    /// true. With `missing_ok = false` a miss is the C `elog(ERROR, "incorrect
    /// result relation OID %u")`, carried on `Err`.
    pub fn exec_lookup_result_rel_by_oid<'mcx>(
        node: &mut ::nodes::ModifyTableState<'mcx>,
        estate: &mut ::nodes::EStateData<'mcx>,
        resultoid: types_core::Oid,
        missing_ok: bool,
        update_cache: bool,
    ) -> types_error::PgResult<Option<::nodes::RriId>>
);

seam_core::seam!(
    /// `CheckValidResultRel(resultRelInfo, operation, onConflictAction,
    /// mergeActions)` (execMain.c): verify the result relation (id into the
    /// EState pool) is a valid target for the given command, raising the
    /// appropriate `ereport(ERROR)` otherwise (carried on `Err`). For a MERGE
    /// (`operation == CMD_MERGE`) `merge_action_cmds` carries the per-result-rel
    /// MergeAction command types (C's `mergeActions`), each of which is
    /// replica-identity checked; for non-MERGE it is empty.
    pub fn check_valid_result_rel<'mcx>(
        estate: &mut ::nodes::EStateData<'mcx>,
        result_rel_info: ::nodes::RriId,
        operation: ::nodes::nodes::CmdType,
        on_conflict_action: ::nodes::nodes::OnConflictAction,
        merge_action_cmds: &[::nodes::nodes::CmdType],
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
    /// owned [`::nodes::querydesc::QueryDesc`] (lifetime-free; its `work`
    /// bundle holds the `EState`/plan-state tree that `ExecutorStart` populated,
    /// so the result tupdesc is readable via
    /// [`::nodes::querydesc::QueryDesc::with_result_tupdesc`]). `parent` is
    /// the `CurrentMemoryContext` the per-query "ExecutorState" context is made
    /// an (accounting) child of. `Err` carries any `ExecutorStart`
    /// `ereport(ERROR)`.
    pub fn create_query_desc_and_start<'mcx>(
        parent: &mcx::MemoryContext,
        plan: &::nodes::nodeindexscan::PlannedStmt<'mcx>,
        source_text: &str,
        copy_receiver: u64,
    ) -> types_error::PgResult<::nodes::querydesc::QueryDesc>
);

seam_core::seam!(
    /// The EXPLAIN side of `ExplainOnePlan` (explain.c): `CreateQueryDesc(plan,
    /// queryString, GetActiveSnapshot(), InvalidSnapshot, None_Receiver, params,
    /// queryEnv, instrument_option)` followed by `ExecutorStart(queryDesc,
    /// eflags)`. The caller has already pushed the active snapshot (explain
    /// `PushCopiedSnapshot(GetActiveSnapshot())`) and passes it in as `snapshot`.
    /// `dest` is the tuple receiver the C body selects (explain.c): the discard
    /// `None_Receiver` for a plain EXPLAIN (passed as `DestReceiverHandle::NULL`,
    /// which the body resolves to `CreateDestReceiver(DestNone)` = `donothingDR`),
    /// or the `DR_intorel` receiver from `CreateIntoRelDestReceiver(into)` for an
    /// `EXPLAIN ... CREATE TABLE AS`. `instrument_option` is derived by the caller
    /// from the EXPLAIN options. Returns the started `QueryDesc` whose plan-state
    /// tree `ExplainPrintPlan` walks. Can `ereport(ERROR)`.
    pub fn create_query_desc_and_start_explain<'mcx>(
        parent: &mcx::MemoryContext,
        plan: &::nodes::nodeindexscan::PlannedStmt<'mcx>,
        source_text: &str,
        snapshot: Option<std::rc::Rc<snapshot::SnapshotData>>,
        params: ::nodes::params::ParamListInfo,
        instrument_option: i32,
        eflags: i32,
        dest: ::nodes::parsestmt::DestReceiverHandle,
    ) -> types_error::PgResult<::nodes::querydesc::QueryDesc>
);

seam_core::seam!(
    /// `ExecutorRun(queryDesc, ForwardScanDirection, 0)` (copyto.c:1104) for the
    /// COPY-(query)-TO path: run the plan to completion; the COPY-OUT receiver
    /// emits each tuple into copyto's `cstate` (incrementing
    /// `cstate.receiver_processed`, the C `((DR_copy *) dest)->processed`). The
    /// processed count is read by copyto from its own `cstate` after the run, so
    /// it is *not* returned here. `Err` carries execution `ereport(ERROR)`s.
    /// Takes the started owned `QueryDesc` by `&mut` (`ExecutorRun` mutates the
    /// `EState` interior — `es_processed`, `already_executed`).
    pub fn executor_run_copy(
        query_desc: &mut ::nodes::querydesc::QueryDesc,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// The COPY-(query)-TO teardown (copyto.c:1010-1012): `ExecutorFinish` +
    /// `ExecutorEnd` + `FreeQueryDesc` for the started query. `Err` carries any
    /// teardown `ereport(ERROR)`. Consumes the started owned `QueryDesc`
    /// (`FreeQueryDesc` frees it; the bundle drops with the value).
    pub fn end_copy_query(
        query_desc: ::nodes::querydesc::QueryDesc,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecutorRun(queryDesc, direction, count)` (execMain.c) — run the
    /// executor, sending tuples to `queryDesc->dest`. Runs the plan; can
    /// `ereport(ERROR)`. (`once` defaulted to false here, as in
    /// `PersistHoldablePortal`'s call.)
    pub fn executor_run(
        query_desc: &mut ::nodes::querydesc::QueryDesc,
        direction: types_scan::sdir::ScanDirection,
        count: u64,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecutorFinish(queryDesc)` (execMain.c) — run the executor's
    /// after-query cleanup (AFTER triggers etc.). Can `ereport(ERROR)`.
    pub fn executor_finish(
        query_desc: &mut ::nodes::querydesc::QueryDesc,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecutorEnd(queryDesc)` (execMain.c) — shut down the executor and free
    /// its per-query state. Can `ereport(ERROR)`.
    pub fn executor_end(
        query_desc: &mut ::nodes::querydesc::QueryDesc,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecutorRewind(queryDesc)` (execMain.c) — rewind the executor to the
    /// start of the query so it can be re-run. Can `ereport(ERROR)`.
    pub fn executor_rewind(
        query_desc: &mut ::nodes::querydesc::QueryDesc,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `FreeQueryDesc(queryDesc)` (pquery.c, reached through the executor
    /// surface) — free a finished `QueryDesc` (consumes it).
    pub fn free_query_desc(
        query_desc: ::nodes::querydesc::QueryDesc,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecSupportsBackwardScan(plan)` (execAmi.c) — does the plan tree
    /// support backward scanning? Pure structural inspection (no ereport in
    /// practice, but the index-AM probe path can error), so fallible.
    pub fn exec_supports_backward_scan(
        plan: &::nodes::nodeindexscan::PlannedStmt<'_>,
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
    /// `ExecCheckOneRelPerms(perminfo)` (execMain.c) for `subquery_planner`'s
    /// view-permission ACL loop (planner.c:866-882). The planner checks
    /// `ACL_SELECT` (etc.) on each `RELKIND_VIEW` RTE's `RTEPermissionInfo`,
    /// because selectivity estimation only checks the view owner's permissions
    /// on the underlying tables, so the invoking user's privilege on the view
    /// itself must be verified here. On a denial this raises
    /// `aclcheck_error(ACLCHECK_NO_PRIV, OBJECT_VIEW, get_rel_name(relid))`
    /// (carried on `Err`); `Ok(())` means the check passed. The owner runs the
    /// access-method-level permission check (`pg_class_aclmask` /
    /// `pg_attribute_aclcheck`), which can `ereport(ERROR)`.
    pub fn exec_check_one_rel_perms_view(
        perminfo: &::nodes::RTEPermissionInfo<'_>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `resultRelInfo->ri_FdwRoutine->ExecForeignUpdate(estate, resultRelInfo,
    /// slot, planSlot)` (fdwapi): dispatch an UPDATE to the foreign-table FDW
    /// via the per-relation `FdwRoutine` vtable carried on the pooled
    /// `ResultRelInfo`. Returns the (possibly replaced) result slot, or
    /// `Ok(None)` for the FDW "do nothing". Resolved when the fdwapi type
    /// lands; owner-coverage placeholder until then.
    pub fn exec_foreign_update<'mcx>(
        estate: &mut ::nodes::EStateData<'mcx>,
        result_rel_info: ::nodes::RriId,
        slot: ::nodes::SlotId,
        plan_slot: Option<::nodes::SlotId>,
    ) -> types_error::PgResult<Option<::nodes::SlotId>>
);

seam_core::seam!(
    /// `ExecPartitionCheckEmitError(resultRelInfo, slot, estate)` (execMain.c):
    /// build and raise the partition-constraint-violation error for `slot`.
    /// Always `ereport(ERROR)`s (only called when the constraint is known to
    /// have failed).
    pub fn exec_partition_check_emit_error<'mcx>(
        estate: &mut ::nodes::EStateData<'mcx>,
        result_rel_info: ::nodes::RriId,
        slot: ::nodes::SlotId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecConstraints(resultRelInfo, slot, estate)` (execMain.c): check the
    /// not-null and CHECK constraints of the target relation against `slot`,
    /// `ereport(ERROR)`ing on the first violation.
    pub fn exec_constraints<'mcx>(
        estate: &mut ::nodes::EStateData<'mcx>,
        result_rel_info: ::nodes::RriId,
        slot: ::nodes::SlotId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecWithCheckOptions(kind, resultRelInfo, slot, estate)` (execMain.c):
    /// evaluate the WITH CHECK OPTION / RLS policies of the given `kind`
    /// (`WCOKind` enum value) on `slot`, `ereport(ERROR)`ing on a violation.
    /// Skips WCOs of other kinds.
    pub fn exec_with_check_options<'mcx>(
        estate: &mut ::nodes::EStateData<'mcx>,
        kind: i32,
        result_rel_info: ::nodes::RriId,
        slot: ::nodes::SlotId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecGetReturningSlot(estate, relInfo)` (execMain.c): get (lazily
    /// creating) the per-relation slot used to hold a tuple for RETURNING.
    pub fn exec_get_returning_slot<'mcx>(
        estate: &mut ::nodes::EStateData<'mcx>,
        result_rel_info: ::nodes::RriId,
    ) -> types_error::PgResult<::nodes::SlotId>
);

seam_core::seam!(
    /// `ExecGetChildToRootMap(resultRelInfo)` (execMain.c): compute lazily the
    /// tuple-conversion map from the child partition rowtype to the root's.
    /// `Ok(true)` means a conversion is needed and the map now lives on the
    /// pooled `ResultRelInfo` (`ri_ChildToRootMap`); `Ok(false)` is the C
    /// `NULL` map (rowtypes already match).
    pub fn exec_get_child_to_root_map<'mcx>(
        estate: &mut ::nodes::EStateData<'mcx>,
        result_rel_info: ::nodes::RriId,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `ExecGetChildToRootMap(resultRelInfo)` (execMain.c), returning the map's
    /// `attrMap` (a copy allocated in `mcx`) and its `outdesc` (the root rowtype
    /// `TupleDesc`, copied in `mcx`) so the caller can re-borrow `estate` to apply
    /// the conversion. `Ok(None)` is the C `NULL` map (rowtypes already match).
    /// `TransitionTableAddTuple` uses this to convert a child-partition tuple into
    /// the root-table transition tuplestore's format via `execute_attr_map_slot`.
    pub fn exec_get_child_to_root_map_full<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        estate: &mut ::nodes::EStateData<'mcx>,
        result_rel_info: ::nodes::RriId,
    ) -> types_error::PgResult<
        Option<(
            mcx::PgBox<'mcx, types_tuple::attmap::AttrMap<'mcx>>,
            types_tuple::heaptuple::TupleDesc<'mcx>,
        )>,
    >
);

seam_core::seam!(
    /// `ExecGetAncestorResultRels(estate, resultRelInfo)` (execMain.c): return
    /// the chain of ancestor `ResultRelInfo`s (root-ward, inclusive of the
    /// root) for a partition's `ResultRelInfo`, lazily opening them.
    pub fn exec_get_ancestor_result_rels<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        estate: &mut ::nodes::EStateData<'mcx>,
        result_rel_info: ::nodes::RriId,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, ::nodes::RriId>>
);

seam_core::seam!(
    /// `ExecUpdateLockMode(estate, relinfo)` (execMain.c): determine the
    /// row-lock mode needed for an UPDATE of `relinfo`, based on which columns
    /// the update touches vs. the relation's key columns.
    pub fn exec_update_lock_mode<'mcx>(
        estate: &mut ::nodes::EStateData<'mcx>,
        result_rel_info: ::nodes::RriId,
    ) -> types_error::PgResult<types_tableam::tableam::LockTupleMode>
);

seam_core::seam!(
    /// `EvalPlanQual(epqstate, relation, rti, inputslot)` (execMain.c): run the
    /// EvalPlanQual recheck for a concurrently-updated tuple, returning the
    /// re-projected slot that still passes the quals, or `Ok(None)` when the
    /// row no longer qualifies. The EPQ state lives on the owning
    /// `ModifyTableState`; the owner reads `es_snapshot` etc. off the estate.
    pub fn eval_plan_qual<'mcx>(
        estate: &mut ::nodes::EStateData<'mcx>,
        epqstate: &mut ::nodes::modifytable::EPQState<'mcx>,
        result_rel_info: ::nodes::RriId,
        rti: types_core::primitive::Index,
        inputslot: ::nodes::SlotId,
    ) -> types_error::PgResult<Option<::nodes::SlotId>>
);

seam_core::seam!(
    /// `EvalPlanQualSlot(epqstate, relation, rti)` (execMain.c): get (lazily
    /// creating) the EPQ test slot for the given range-table relation.
    pub fn eval_plan_qual_slot<'mcx>(
        estate: &mut ::nodes::EStateData<'mcx>,
        epqstate: &mut ::nodes::modifytable::EPQState<'mcx>,
        result_rel_info: ::nodes::RriId,
        rti: types_core::primitive::Index,
    ) -> types_error::PgResult<::nodes::SlotId>
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
        estate: &mut ::nodes::EStateData<'mcx>,
        result_rel_info: ::nodes::RriId,
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
        epqstate: &mut ::nodes::modifytable::EPQState<'mcx>,
        estate: &mut ::nodes::EStateData<'mcx>,
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
        epqstate: &mut ::nodes::modifytable::EPQState<'mcx>,
        subplan: Option<&'mcx ::nodes::nodes::Node<'mcx>>,
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
        epqstate: &mut ::nodes::modifytable::EPQState<'mcx>,
        estate: &mut ::nodes::EStateData<'mcx>,
        row_marks: &[mcx::PgBox<'mcx, ::nodes::nodes::Node<'mcx>>],
        subplan: Option<&'mcx ::nodes::nodes::Node<'mcx>>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `EvalPlanQualBegin(&node->lr_epqstate)` for the LockRows path (execMain.c
    /// / nodeLockRows.c): build (or reset) the recheck estate + plan, then bridge
    /// the parent's locked source tuples (`epqstate.relsubs_slot[i]`) into the
    /// recheck estate's marker so the recheck scans return them. The owned model
    /// passes the parent EState + the LockRows node's canonical `EPQState` by
    /// mutable reference. Fallible on `ereport(ERROR)` / OOM.
    pub fn eval_plan_qual_begin_lockrows<'mcx>(
        parentestate: &mut ::nodes::EStateData<'mcx>,
        epqstate: &mut ::nodes::modifytable::EPQState<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `EvalPlanQualSetSlot(&node->lr_epqstate, slot)` for the LockRows path
    /// (execMain.c / nodeLockRows.c): record `origslot` and bridge the origin
    /// output tuple into a recheck-estate slot so `EvalPlanQualFetchRowMark` can
    /// read its junk attributes. `slot` is the LockRows node's current working
    /// "outer" slot (a PARENT-estate slot). Fallible on `ereport(ERROR)` / OOM.
    pub fn eval_plan_qual_set_slot_lockrows<'mcx>(
        parentestate: &mut ::nodes::EStateData<'mcx>,
        epqstate: &mut ::nodes::modifytable::EPQState<'mcx>,
        slot: ::nodes::SlotId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `EvalPlanQualNext(epqstate)` (execMain.c): run the recheck plan-state tree
    /// under the recheck estate and return the next tuple (a RECHECK-estate
    /// `SlotId`), or `Ok(None)` (the C `TupIsNull`). The owned model passes the
    /// canonical `EPQState` by mutable reference. Fallible on `ereport(ERROR)`.
    pub fn eval_plan_qual_next<'mcx>(
        epqstate: &mut ::nodes::modifytable::EPQState<'mcx>,
    ) -> types_error::PgResult<Option<::nodes::SlotId>>
);

seam_core::seam!(
    /// `EvalPlanQualEnd(epqstate)` (execMain.c): shut down EPQ execution — end
    /// the recheck plan-state tree + subplans, reset the recheck estate's tuple
    /// table, close the result/trigger and range-table relations it opened, and
    /// free the recheck estate. Idempotent (a no-op when EPQ was never started).
    /// The owned model passes the parent EState + the canonical `EPQState` by
    /// mutable reference. Fallible on `ereport(ERROR)` / OOM.
    pub fn eval_plan_qual_end<'mcx>(
        parentestate: &mut ::nodes::EStateData<'mcx>,
        epqstate: &mut ::nodes::modifytable::EPQState<'mcx>,
    ) -> types_error::PgResult<()>
);

// ===========================================================================
// CteScan leader-aliased operations (nodeCtescan.c).
// ===========================================================================
//
// A `CteScanState`'s `leader` is an aliased self-/cross-reference into the
// executor-owned node graph (the C `struct CteScanState *leader`); only the
// leader holds the valid shared `cte_table` / `eof_cte`. A live mutable alias
// into another owned node cannot be held in safe Rust, so every operation that
// crosses that link is reached through these seams. execMain owns the live
// node graph + the `EState.es_param_exec_vals` Param slot that establishes the
// leader and the `es_subplanstates` that hold the CTE subplan, so it installs
// them when it lands; until then a call panics loudly. nodeCtescan keeps the
// node-machine control flow and reaches the leader-aliased state through here.

use ::nodes::nodectescan::{CteScan, CteScanState};

seam_core::seam!(
    /// `ExecInitCteScan` Param-slot leader handshake: read
    /// `prmdata = &estate->es_param_exec_vals[node->cteParam]`,
    /// `Assert(prmdata->execPlan == NULL); Assert(!prmdata->isnull)`, take
    /// `scanstate->leader = castNode(CteScanState, DatumGetPointer(prmdata->value))`.
    /// If the slot was empty (this node is the leader), publish
    /// `prmdata->value = PointerGetDatum(scanstate)` and
    /// `scanstate->leader = scanstate`, returning `true`; otherwise record the
    /// existing leader link and return `false`.
    pub fn cte_resolve_leader<'mcx>(
        scanstate: &mut CteScanState<'mcx>,
        plan: &CteScan<'mcx>,
        estate: &mut ::nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `scanstate->cteplanstate = (PlanState *) list_nth(estate->es_subplanstates,
    /// node->ctePlanId - 1)` (`ExecInitCteScan`): link the already-initialized
    /// CTE subplan from the executor-owned `es_subplanstates`.
    pub fn cte_link_plan_state<'mcx>(
        scanstate: &mut CteScanState<'mcx>,
        plan: &CteScan<'mcx>,
        estate: &mut ::nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// Leader-only store creation (`ExecInitCteScan`):
    /// `scanstate->cte_table = tuplestore_begin_heap(true, false, work_mem);
    /// tuplestore_set_eflags(scanstate->cte_table, scanstate->eflags)`.
    pub fn cte_tuplestore_begin_heap_leader<'mcx>(
        scanstate: &mut CteScanState<'mcx>,
        estate: &mut ::nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// Follower read-pointer setup (`ExecInitCteScan`):
    /// `scanstate->readptr =
    ///   tuplestore_alloc_read_pointer(scanstate->leader->cte_table, scanstate->eflags);
    /// tuplestore_select_read_pointer(...); tuplestore_rescan(...)`. Sets
    /// `scanstate->readptr` to the freshly allocated pointer index.
    pub fn cte_tuplestore_alloc_read_pointer_follower<'mcx>(
        scanstate: &mut CteScanState<'mcx>,
        estate: &mut ::nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `node->leader->eof_cte` (read): the leader's end-of-CTE flag, held in
    /// `EState.es_cte_shared[node.cteParam].eof_cte`.
    pub fn cte_leader_eof_cte<'mcx>(
        node: &CteScanState<'mcx>,
        estate: &::nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `node->leader->eof_cte = value` (write).
    pub fn cte_set_leader_eof_cte<'mcx>(
        node: &mut CteScanState<'mcx>,
        value: bool,
        estate: &mut ::nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `tuplestore_select_read_pointer(node->leader->cte_table, node->readptr)`:
    /// make this node's read pointer active on the shared store.
    pub fn cte_tuplestore_select_read_pointer<'mcx>(
        node: &mut CteScanState<'mcx>,
        estate: &mut ::nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `tuplestore_ateof(node->leader->cte_table)`.
    pub fn cte_tuplestore_ateof<'mcx>(
        node: &CteScanState<'mcx>,
        estate: &::nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `tuplestore_advance(node->leader->cte_table, forward)`: returns `false`
    /// when the store is empty (the C `false`).
    pub fn cte_tuplestore_advance<'mcx>(
        node: &mut CteScanState<'mcx>,
        forward: bool,
        estate: &mut ::nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `tuplestore_gettupleslot(node->leader->cte_table, forward, /*copy*/true,
    /// node->ss.ss_ScanTupleSlot)`: fetch the next tuple from the shared store
    /// into the node's scan slot. Returns `true` when a tuple was fetched.
    pub fn cte_tuplestore_gettupleslot<'mcx>(
        node: &mut CteScanState<'mcx>,
        forward: bool,
        estate: &mut ::nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `tuplestore_puttupleslot(node->leader->cte_table, cteslot)`: append a copy
    /// of the CTE subplan's just-returned tuple to the shared store.
    pub fn cte_tuplestore_puttupleslot<'mcx>(
        node: &mut CteScanState<'mcx>,
        estate: &mut ::nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `tuplestore_rescan(node->leader->cte_table)`.
    pub fn cte_tuplestore_rescan<'mcx>(
        node: &mut CteScanState<'mcx>,
        estate: &mut ::nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `tuplestore_clear(node->leader->cte_table)`.
    pub fn cte_tuplestore_clear<'mcx>(
        node: &mut CteScanState<'mcx>,
        estate: &mut ::nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `node->leader == node` (`ExecEndCteScan` leader-identity test): whether
    /// this node is its own leader. The leader link is an aliased self-/cross-
    /// reference resolved through the owning crate's seams, so the identity test
    /// cannot be expressed in safe Rust at the dispatch site.
    pub fn cte_leader_is_self<'mcx>(
        node: &CteScanState<'mcx>,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `tuplestore_end(node->cte_table)` (leader only, `ExecEndCteScan`): free
    /// the shared store held in `EState.es_cte_shared[node.cteParam]` and clear
    /// that side-entry.
    pub fn cte_tuplestore_end<'mcx>(
        node: &mut CteScanState<'mcx>,
        estate: &mut ::nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `cteslot = ExecProcNode(node->cteplanstate)` then `!TupIsNull(cteslot)`:
    /// run the CTE subplan one tuple. Returns `true` when a non-null tuple was
    /// returned (its slot is stashed for the following puttupleslot / copy);
    /// `false` at end of the subplan.
    pub fn cte_exec_proc_node<'mcx>(
        node: &mut CteScanState<'mcx>,
        estate: &mut ::nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `ExecCopySlot(node->ss.ss_ScanTupleSlot, cteslot)`: copy the CTE subplan's
    /// just-returned tuple into the node's own scan slot (stable across other
    /// readers advancing the subplan).
    pub fn cte_copy_tuple_to_scan_slot<'mcx>(
        node: &mut CteScanState<'mcx>,
        estate: &mut ::nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `node->leader->cteplanstate->chgParam != NULL` (`ExecReScanCteScan`):
    /// whether the underlying CTE needs a fresh scan. Reaches the CTE subplan's
    /// plan-state by `ctePlanId` index into `es_subplanstates`.
    pub fn cte_leader_cteplanstate_chgparam_set<'mcx>(
        node: &CteScanState<'mcx>,
        estate: &::nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<bool>
);

// ===========================================================================
// Executor hooks (execMain.c): ExecutorStart_hook / ExecutorRun_hook /
// ExecutorFinish_hook / ExecutorEnd_hook.
//
// These are the loadable-module interposition points (e.g. pg_stat_statements).
// In C each is a per-process function-pointer global, NULL by default; the
// hookable entry calls `hook ? hook(...) : standard_*(...)`. They are modeled
// exactly like `shmem_request_hook` (miscinit.c): a per-backend thread-local
// `Cell<Option<fn>>` slot, with `set_*_hook` / `*_hook_present` accessors and a
// `call_*_hook` dispatcher. The slots live in this `-seams` crate (a dependency
// of both the execMain owner and any future hook-installing module, e.g.
// pg_stat_statements) so the module can register a hook without a dependency
// cycle. With no hook set, `*_hook_present()` is false and the owner runs the
// `standard_*` path directly — byte-identical to today.
// ===========================================================================

use std::cell::Cell;
use types_error::PgResult;
use ::nodes::querydesc::QueryDesc;
use types_scan::sdir::ScanDirection;

/// `ExecutorStart_hook_type` (executor.h): `void (*)(QueryDesc *queryDesc, int
/// eflags)`. The hook returns a bool in PG18 (`ExecutorStart`'s `bool` return);
/// modeled here as the fallible `PgResult<()>` the owner already threads, with
/// the C bool surfaced as the `start` field on `QueryDesc`'s started state.
pub type ExecutorStartHook = fn(query_desc: &mut QueryDesc, eflags: i32) -> PgResult<()>;
/// `ExecutorRun_hook_type` (executor.h): `void (*)(QueryDesc *queryDesc,
/// ScanDirection direction, uint64 count)`.
pub type ExecutorRunHook =
    fn(query_desc: &mut QueryDesc, direction: ScanDirection, count: u64) -> PgResult<()>;
/// `ExecutorFinish_hook_type` (executor.h): `void (*)(QueryDesc *queryDesc)`.
pub type ExecutorFinishHook = fn(query_desc: &mut QueryDesc) -> PgResult<()>;
/// `ExecutorEnd_hook_type` (executor.h): `void (*)(QueryDesc *queryDesc)`.
pub type ExecutorEndHook = fn(query_desc: &mut QueryDesc) -> PgResult<()>;

thread_local! {
    /// `ExecutorStart_hook_type ExecutorStart_hook = NULL;` (execMain.c).
    static EXECUTOR_START_HOOK: Cell<Option<ExecutorStartHook>> = const { Cell::new(None) };
    /// `ExecutorRun_hook_type ExecutorRun_hook = NULL;` (execMain.c).
    static EXECUTOR_RUN_HOOK: Cell<Option<ExecutorRunHook>> = const { Cell::new(None) };
    /// `ExecutorFinish_hook_type ExecutorFinish_hook = NULL;` (execMain.c).
    static EXECUTOR_FINISH_HOOK: Cell<Option<ExecutorFinishHook>> = const { Cell::new(None) };
    /// `ExecutorEnd_hook_type ExecutorEnd_hook = NULL;` (execMain.c).
    static EXECUTOR_END_HOOK: Cell<Option<ExecutorEndHook>> = const { Cell::new(None) };
}

/// `ExecutorStart_hook != NULL` — whether a module registered an
/// `ExecutorStart` hook.
pub fn executor_start_hook_present() -> bool {
    EXECUTOR_START_HOOK.with(|c| c.get().is_some())
}
/// Register a module's `ExecutorStart_hook` (the `ExecutorStart_hook = my_hook`
/// assignment in `_PG_init`). The hook wraps + calls `standard_ExecutorStart`.
pub fn set_executor_start_hook(hook: Option<ExecutorStartHook>) -> Option<ExecutorStartHook> {
    EXECUTOR_START_HOOK.with(|c| c.replace(hook))
}
/// Invoke the registered `ExecutorStart_hook(queryDesc, eflags)`. Panics if no
/// hook is registered (the call site guards with [`executor_start_hook_present`],
/// mirroring C's `if (ExecutorStart_hook)`).
pub fn call_executor_start_hook(query_desc: &mut QueryDesc, eflags: i32) -> PgResult<()> {
    match EXECUTOR_START_HOOK.with(Cell::get) {
        Some(hook) => hook(query_desc, eflags),
        None => panic!("call_executor_start_hook() called with no hook registered"),
    }
}

/// `ExecutorRun_hook != NULL`.
pub fn executor_run_hook_present() -> bool {
    EXECUTOR_RUN_HOOK.with(|c| c.get().is_some())
}
/// Register a module's `ExecutorRun_hook`. The hook wraps + calls
/// `standard_ExecutorRun`.
pub fn set_executor_run_hook(hook: Option<ExecutorRunHook>) -> Option<ExecutorRunHook> {
    EXECUTOR_RUN_HOOK.with(|c| c.replace(hook))
}
/// Invoke the registered `ExecutorRun_hook(queryDesc, direction, count)`.
pub fn call_executor_run_hook(
    query_desc: &mut QueryDesc,
    direction: ScanDirection,
    count: u64,
) -> PgResult<()> {
    match EXECUTOR_RUN_HOOK.with(Cell::get) {
        Some(hook) => hook(query_desc, direction, count),
        None => panic!("call_executor_run_hook() called with no hook registered"),
    }
}

/// `ExecutorFinish_hook != NULL`.
pub fn executor_finish_hook_present() -> bool {
    EXECUTOR_FINISH_HOOK.with(|c| c.get().is_some())
}
/// Register a module's `ExecutorFinish_hook`. The hook wraps + calls
/// `standard_ExecutorFinish`.
pub fn set_executor_finish_hook(hook: Option<ExecutorFinishHook>) -> Option<ExecutorFinishHook> {
    EXECUTOR_FINISH_HOOK.with(|c| c.replace(hook))
}
/// Invoke the registered `ExecutorFinish_hook(queryDesc)`.
pub fn call_executor_finish_hook(query_desc: &mut QueryDesc) -> PgResult<()> {
    match EXECUTOR_FINISH_HOOK.with(Cell::get) {
        Some(hook) => hook(query_desc),
        None => panic!("call_executor_finish_hook() called with no hook registered"),
    }
}

/// `ExecutorEnd_hook != NULL`.
pub fn executor_end_hook_present() -> bool {
    EXECUTOR_END_HOOK.with(|c| c.get().is_some())
}
/// Register a module's `ExecutorEnd_hook`. The hook wraps + calls
/// `standard_ExecutorEnd`.
pub fn set_executor_end_hook(hook: Option<ExecutorEndHook>) -> Option<ExecutorEndHook> {
    EXECUTOR_END_HOOK.with(|c| c.replace(hook))
}
/// Invoke the registered `ExecutorEnd_hook(queryDesc)`.
pub fn call_executor_end_hook(query_desc: &mut QueryDesc) -> PgResult<()> {
    match EXECUTOR_END_HOOK.with(Cell::get) {
        Some(hook) => hook(query_desc),
        None => panic!("call_executor_end_hook() called with no hook registered"),
    }
}
