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
        estate: &mut types_nodes::EStateData<'mcx>,
        result_rel_info: types_nodes::RriId,
        cmdtype: types_nodes::nodes::CmdType,
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
        mtstate: &types_nodes::ModifyTableState<'mcx>,
    ) -> types_nodes::nodes::OnConflictAction
);
