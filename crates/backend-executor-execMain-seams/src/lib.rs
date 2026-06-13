//! Seam declarations for the `backend-executor-execMain` unit
//! (`executor/execMain.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

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
