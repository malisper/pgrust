//! Seam declarations for the `backend-executor-execMain` unit
//! (`executor/execMain.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `epqstate->relsubs_done[scanrelid - 1]` (execMain.c / EvalPlanQual):
    /// whether the EPQ test tuple for this scan relation has already been
    /// returned. Pure read of the EPQ state.
    pub fn epq_relsubs_done(
        epqstate: types_nodes::EPQStateHandle,
        scanrelid_minus_1: u32,
    ) -> bool
);

seam_core::seam!(
    /// `epqstate->relsubs_slot[scanrelid - 1] != NULL` — is there a
    /// replacement-slot EPQ source for this scan relation?
    pub fn epq_relsubs_slot_present(
        epqstate: types_nodes::EPQStateHandle,
        scanrelid_minus_1: u32,
    ) -> bool
);

seam_core::seam!(
    /// `epqstate->relsubs_rowmark[scanrelid - 1] != NULL` — is there a
    /// non-locking-rowmark EPQ source for this scan relation?
    pub fn epq_relsubs_rowmark_present(
        epqstate: types_nodes::EPQStateHandle,
        scanrelid_minus_1: u32,
    ) -> bool
);

seam_core::seam!(
    /// `epqstate->relsubs_done[scanrelid - 1] = value` (execMain.c): mark
    /// whether the EPQ test tuple has been returned.
    pub fn epq_set_relsubs_done(
        epqstate: types_nodes::EPQStateHandle,
        scanrelid_minus_1: u32,
        value: bool,
    )
);

seam_core::seam!(
    /// Load the EPQ replacement slot (`epqstate->relsubs_slot[scanrelid - 1]`)
    /// into the scan node's scan slot (`ExecCopySlot`-shape), returning whether
    /// a (non-empty) tuple was loaded. Fallible on OOM.
    pub fn epq_load_relsubs_slot<'mcx>(
        epqstate: types_nodes::EPQStateHandle,
        estate: &mut types_nodes::EStateData<'mcx>,
        scanrelid_minus_1: u32,
        dest_slot: types_nodes::SlotId,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `EvalPlanQualFetchRowMark(epqstate, scanrelid, slot)` (execMain.c):
    /// fetch the EPQ replacement tuple for a non-locking rowmark into the scan
    /// slot, returning whether a tuple was produced. Fallible on
    /// `ereport(ERROR)`.
    pub fn eval_plan_qual_fetch_row_mark<'mcx>(
        epqstate: types_nodes::EPQStateHandle,
        estate: &mut types_nodes::EStateData<'mcx>,
        scanrelid: u32,
        dest_slot: types_nodes::SlotId,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// For a `scanrelid == 0` Foreign/Custom scan that pushed a join down,
    /// whether the node's `extParam` set overlaps the EPQ relation set — the
    /// `bms_overlap` test in `ExecScanFetch`'s `scanrelid == 0` branch.
    pub fn epq_param_is_member_of_ext_param(
        epqstate: types_nodes::EPQStateHandle,
        node_ext_param: Option<&types_nodes::Bitmapset<'_>>,
    ) -> bool
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
