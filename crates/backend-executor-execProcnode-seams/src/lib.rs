//! Seam declarations for the `backend-executor-execProcnode` unit
//! (`executor/execProcnode.c`): the node-dispatch trio.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. The owned model threads `&mut EStateData`
//! explicitly in place of the C `PlanState.state` back-pointer.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `ExecInitNode(node, estate, eflags)` (execProcnode.c): recursively
    /// initialize the plan subtree, returning its plan-state tree. A `None`
    /// plan yields `None` (the C `if (node == NULL) return NULL;`). The state
    /// tree is allocated in `mcx` (C: `makeNode` in `CurrentMemoryContext`,
    /// the per-query context at init time), so the call is fallible on OOM.
    /// The plan tree is shared and read-only: state nodes alias it
    /// (`planstate->plan = (Plan *) node`), so the borrow must outlive the
    /// state tree's `'mcx`.
    pub fn exec_init_node<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        node: Option<&'mcx types_nodes::nodes::Node<'mcx>>,
        estate: &mut types_nodes::EStateData<'mcx>,
        eflags: i32,
    ) -> types_error::PgResult<Option<mcx::PgBox<'mcx, types_nodes::PlanStateNode<'mcx>>>>
);

seam_core::seam!(
    /// `ExecProcNode(node)` (executor.h/execProcnode.c): pull the next tuple
    /// from the node by dispatching through its installed `ExecProcNode`
    /// callback. Returns the `SlotId` of the produced tuple's slot, or `None`
    /// for the C `NULL` return. Allocation during execution comes from
    /// `estate.es_query_cxt` — the node and estate share the tree's `'mcx`.
    pub fn exec_proc_node<'mcx>(
        node: &mut types_nodes::PlanStateNode<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<Option<types_nodes::SlotId>>
);

seam_core::seam!(
    /// `ExecEndNode(node)` (execProcnode.c): recursively shut down the
    /// plan-state subtree.
    pub fn exec_end_node<'mcx>(
        node: &mut types_nodes::PlanStateNode<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecShutdownNode(node)` (execProcnode.c): give the plan-state subtree a
    /// chance to release asynchronous resources (the `planstate_tree_walker`
    /// drives `ExecShutdown*` over `Gather`/`ForeignScan`/`CustomScan`/
    /// `GatherMerge`/`Hash`/`HashJoin`). `ExecutePlan` calls it after the scan
    /// finishes when the plan won't be rescanned backward
    /// (`!EXEC_FLAG_BACKWARD`). Owner: `backend-executor-execProcnode`.
    pub fn exec_shutdown_node<'mcx>(
        node: &mut types_nodes::PlanStateNode<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `MultiExecProcNode(node)` (execProcnode.c): run a node that returns a
    /// bulk result rather than a tuple-at-a-time stream — for bitmap-scan
    /// inputs the child `BitmapIndexScan`/`BitmapAnd`/`BitmapOr` returns a
    /// built `TIDBitmap`. Dispatches through the node's `MultiExecProcNodeMtd`.
    /// The result is allocated during execution (`es_query_cxt`), so the call
    /// is fallible. The caller verifies `IsA(result, TIDBitmap)`.
    pub fn multi_exec_proc_node<'mcx>(
        node: &mut types_nodes::PlanStateNode<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<mcx::PgBox<'mcx, types_tidbitmap::TIDBitmap>>
);

// NOTE: the PARAM_EXEC `execPlan`-link plumbing seams
// (mark_param_execplan_pending / clear_param_execplan / param_execplan_pending /
// exec_set_param_plan_for_pending / link_subplan_planstate) were RELOCATED to
// `backend-executor-execMain-seams`: they are not `execProcnode.c` functions —
// they operate on the executor-owned `es_param_exec_vals` / `es_subplanstates`
// (execMain machinery). nodeSubplan was their first consumer, which is why they
// were parked here; the relocation puts them under their real owner so the
// seam-install guard attributes them correctly.

seam_core::seam!(
    /// `ExecSetTupleBound(tuples_needed, child_node)` (execProcnode.c): inform
    /// a node (and applicable descendants) that it will not need to produce
    /// more than `tuples_needed` tuples. A negative bound means "no limit".
    /// The dispatch recurses into child nodes that may allocate, so it is
    /// fallible (OOM / `ereport(ERROR)`).
    pub fn exec_set_tuple_bound<'mcx>(
        tuples_needed: types_core::int64,
        child: &mut types_nodes::PlanStateNode<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);
