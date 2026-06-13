//! Seam declarations for the `backend-executor-execAmi` unit
//! (`executor/execAmi.c`).
//!
//! The owning crate (`backend-executor-execAmi`) installs these from its
//! `init_seams()`. The C `ExecReScan(PlanState *)` gains the explicit
//! `estate` parameter of the owned-tree model (the C `node->state`
//! back-pointer) and returns `PgResult` (the dispatch's `elog(ERROR,
//! "unrecognized node type")` plus whatever the per-node rescans raise).

#![allow(non_snake_case)]

seam_core::seam!(
    /// `ExecReScan(node)` (execAmi.c): rescan the plan-state subtree (reset
    /// the node so its next `ExecProcNode` call starts over). The node and
    /// estate share the state tree's allocator lifetime.
    pub fn exec_re_scan<'mcx>(
        node: &mut types_nodes::PlanStateNode<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecMarkPos(node)` (execAmi.c): save the current scan position of the
    /// plan-state subtree, dispatching by node type. The C dispatch
    /// `elog(ERROR, "unrecognized node type")` for nodes that do not support
    /// marking, hence `PgResult`.
    pub fn exec_mark_pos<'mcx>(
        node: &mut types_nodes::PlanStateNode<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecRestrPos(node)` (execAmi.c): restore the scan position previously
    /// saved with `ExecMarkPos`, dispatching by node type. `elog(ERROR)` on an
    /// unrecognized node type, hence `PgResult`.
    pub fn exec_restr_pos<'mcx>(
        node: &mut types_nodes::PlanStateNode<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);
