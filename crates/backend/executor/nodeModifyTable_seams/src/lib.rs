//! Seam declarations for the `backend-executor-nodeModifyTable` unit
//! (`executor/nodeModifyTable.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `ExecInitGenerated(resultRelInfo, estate, cmdtype)`
    /// (nodeModifyTable.c): initialize the result relation's stored-generated
    ///-column bookkeeping (`ri_GeneratedExprs*` and, for UPDATE,
    /// `ri_extraUpdatedCols` + its valid flag). The `ResultRelInfo` is
    /// addressed by pool id; allocations go to the EState's per-query
    /// context. `Err` carries the C `ereport(ERROR)`s and OOM.
    pub fn exec_init_generated<'mcx>(
        estate: &mut nodes::EStateData<'mcx>,
        result_rel_info: nodes::RriId,
        cmdtype: nodes::nodes::CmdType,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecComputeStoredGenerated(resultRelInfo, estate, slot, cmdtype)`
    /// (nodeModifyTable.c): compute the values of the relation's stored
    /// generated columns for the tuple in `slot` (id into the EState slot
    /// pool) and write them back into the slot. The `ResultRelInfo` is
    /// addressed by pool id; the generated `ExprState`s are read off it. `Err`
    /// carries the C `ereport(ERROR)`s from a generation expression and OOM.
    pub fn exec_compute_stored_generated<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        estate: &mut nodes::EStateData<'mcx>,
        result_rel_info: nodes::RriId,
        slot: nodes::SlotId,
        cmdtype: nodes::nodes::CmdType,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `((ModifyTable *) mtstate->ps.plan)->onConflictAction`
    /// (nodeModifyTable.c view of the plan node): the ON CONFLICT action of
    /// the `ModifyTableState`'s plan node, or `ONCONFLICT_NONE` when the plan
    /// pointer is the C `NULL` (the `node ? ... : ONCONFLICT_NONE` guard in
    /// `ExecFindPartition`). Infallible — a plain field read once the owner
    /// can interpret its own plan node.
    pub fn exec_get_on_conflict_action<'mcx>(
        mtstate: &nodes::ModifyTableState<'mcx>,
    ) -> nodes::nodes::OnConflictAction
);

seam_core::seam!(
    /// The `relhasindex`-gated `ExecOpenIndices` leg of `ExecInitPartitionInfo`
    /// (execPartition.c L543-547): when the partition relation has indexes and
    /// `leaf_part_rri->ri_IndexRelationDescs == NULL`, open the partition's
    /// indices into the leaf `ResultRelInfo` (id into the EState pool), passing
    /// `speculative = (node != NULL && node->onConflictAction != ONCONFLICT_NONE)`
    /// so ExecInsert can perform speculative insertions. A no-op when the
    /// partition has no indexes or they are already open. `relhasindex` and the
    /// `ExecOpenIndices` callee (execIndexing.c) are the unported owner's; `Err`
    /// carries index-open failure and OOM.
    pub fn exec_open_partition_indices<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        mtstate: &nodes::ModifyTableState<'mcx>,
        estate: &mut nodes::EStateData<'mcx>,
        leaf_part_rri: nodes::RriId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// The WITH CHECK OPTION leg of `ExecInitPartitionInfo` (execPartition.c
    /// L549-614): when `node && node->withCheckOptionLists != NIL`, take the
    /// first plan's WCO list as reference, `build_attrmap_by_name` +
    /// `map_variable_attnos` it into the partition `partrel`'s attribute
    /// numbers (relative to `first_varno` / the first result rel), `ExecInitQual`
    /// each `WithCheckOption.qual`, and store `ri_WithCheckOptions` /
    /// `ri_WithCheckOptionExprs` on the leaf `ResultRelInfo` (id into the EState
    /// pool). A no-op when the plan carries no WCO lists. Reads the unported
    /// `ModifyTable` plan node and `WithCheckOption` node type; `Err` carries
    /// the expression-init errors and OOM.
    pub fn exec_init_partition_with_check_options<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        mtstate: &mut nodes::ModifyTableState<'mcx>,
        estate: &mut nodes::EStateData<'mcx>,
        leaf_part_rri: nodes::RriId,
        first_varno: types_core::primitive::Index,
        first_result_rel: nodes::RriId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// The RETURNING leg of `ExecInitPartitionInfo` (execPartition.c L616-679):
    /// when `node && node->returningLists != NIL`, take the first plan's
    /// RETURNING list as reference, `build_attrmap_by_name` +
    /// `map_variable_attnos` it into the partition's attribute numbers, store
    /// `ri_returningList`, and build `ri_projectReturning` via
    /// `ExecBuildProjectionInfo` using `mtstate->ps.ps_ResultTupleSlot` /
    /// `ps_ExprContext`. A no-op when the plan carries no RETURNING lists.
    /// Reads the unported `ModifyTable` plan node; `Err` carries the
    /// projection-build errors and OOM.
    pub fn exec_init_partition_returning<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        mtstate: &mut nodes::ModifyTableState<'mcx>,
        estate: &mut nodes::EStateData<'mcx>,
        leaf_part_rri: nodes::RriId,
        first_varno: types_core::primitive::Index,
        first_result_rel: nodes::RriId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// The ON CONFLICT leg of `ExecInitPartitionInfo` (execPartition.c
    /// L685-862): when `node && node->onConflictAction != ONCONFLICT_NONE`, map
    /// the root's arbiter index list to the partition's (scanning the
    /// partition's index list and matching `get_partition_ancestors`), checking
    /// the `elog(ERROR, "invalid arbiter index list")` length invariant, store
    /// `ri_onConflictArbiterIndexes`, and for `ONCONFLICT_UPDATE` build the
    /// `OnConflictSetState` (existing/proj slots, DO UPDATE SET projection and
    /// WHERE clause, reusing the root's state when the root→child tuple map is
    /// `NULL`). A no-op when the plan has no ON CONFLICT clause. Reads the
    /// unported `ModifyTable` plan node and `OnConflictSetState`; `Err` carries
    /// the arbiter-list `elog`, projection-build errors, and OOM.
    pub fn exec_init_partition_on_conflict<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        mtstate: &mut nodes::ModifyTableState<'mcx>,
        estate: &mut nodes::EStateData<'mcx>,
        leaf_part_rri: nodes::RriId,
        root_result_rel_info: nodes::RriId,
        first_varno: types_core::primitive::Index,
        first_result_rel: nodes::RriId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// The MERGE leg of `ExecInitPartitionInfo` (execPartition.c L877-981):
    /// when `node && node->operation == CMD_MERGE`, take the first plan's
    /// `mergeActionList` as reference, build a per-partition copy converting
    /// attribute numbers (`build_attrmap_by_name`, `map_variable_attnos`,
    /// `adjust_partition_colnos_using_map`), initialize the merge tuple slots
    /// (`ExecInitMergeTupleSlots`) when `!ri_projectNewInfoValid`, build the
    /// join-condition `ExprState` (`ri_MergeJoinCondition`), and for each
    /// action build its `MergeActionState` (INSERT/UPDATE projections, WHEN
    /// qual). A no-op when the operation is not MERGE. Reads the unported
    /// `ModifyTable` plan node and `MergeAction`/`MergeActionState` types;
    /// `Err` carries the `elog`s, projection-build errors, and OOM.
    pub fn exec_init_partition_merge<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        mtstate: &mut nodes::ModifyTableState<'mcx>,
        estate: &mut nodes::EStateData<'mcx>,
        leaf_part_rri: nodes::RriId,
        first_varno: types_core::primitive::Index,
        first_result_rel: nodes::RriId,
    ) -> types_error::PgResult<()>
);
