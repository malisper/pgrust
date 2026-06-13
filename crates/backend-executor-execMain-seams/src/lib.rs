//! Seam declarations for the `backend-executor-execMain` unit
//! (`executor/execMain.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

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

