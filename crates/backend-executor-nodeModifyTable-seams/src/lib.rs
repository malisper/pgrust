//! Seam declarations for the `backend-executor-nodeModifyTable` unit
//! (`executor/nodeModifyTable.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

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
