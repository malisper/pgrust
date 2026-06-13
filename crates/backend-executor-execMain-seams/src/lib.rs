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

