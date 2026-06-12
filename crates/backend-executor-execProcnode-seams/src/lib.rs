//! Seam declarations for the `backend-executor-execProcnode` unit
//! (`executor/execProcnode.c`): the node-dispatch trio.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. The owned model threads `&mut EStateData`
//! explicitly in place of the C `PlanState.state` back-pointer.

#![allow(non_snake_case)]

extern crate alloc;

seam_core::seam!(
    /// `ExecInitNode(node, estate, eflags)` (execProcnode.c): recursively
    /// initialize the plan subtree, returning its plan-state tree. A `None`
    /// plan yields `None` (the C `if (node == NULL) return NULL;`).
    pub fn exec_init_node(
        node: Option<&types_nodes::nodes::Node>,
        estate: &mut types_nodes::EStateData,
        eflags: i32,
    ) -> types_error::PgResult<Option<alloc::boxed::Box<types_nodes::PlanStateNode>>>
);

seam_core::seam!(
    /// `ExecProcNode(node)` (executor.h/execProcnode.c): pull the next tuple
    /// from the node by dispatching through its installed `ExecProcNode`
    /// callback. Returns the `SlotId` of the produced tuple's slot, or `None`
    /// for the C `NULL` return.
    pub fn exec_proc_node(
        node: &mut types_nodes::PlanStateNode,
        estate: &mut types_nodes::EStateData,
    ) -> types_error::PgResult<Option<types_nodes::SlotId>>
);

seam_core::seam!(
    /// `ExecEndNode(node)` (execProcnode.c): recursively shut down the
    /// plan-state subtree.
    pub fn exec_end_node(
        node: &mut types_nodes::PlanStateNode,
        estate: &mut types_nodes::EStateData,
    ) -> types_error::PgResult<()>
);
