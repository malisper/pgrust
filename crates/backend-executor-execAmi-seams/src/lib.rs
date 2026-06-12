//! Seam declarations for the `backend-executor-execAmi` unit
//! (`executor/execAmi.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `ExecReScan(node)` (execAmi.c): rescan the plan-state subtree (reset
    /// the node so its next `ExecProcNode` call starts over).
    pub fn exec_re_scan(
        node: &mut types_nodes::PlanStateNode,
        estate: &mut types_nodes::EStateData,
    ) -> types_core::PgResult<()>
);
