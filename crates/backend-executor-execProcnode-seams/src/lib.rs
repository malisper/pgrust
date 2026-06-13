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
    /// `prm->execPlan = sstate` — mark the PARAM_EXEC slot `paramid` as needing
    /// evaluation by an initplan (nodeSubplan.c `ExecInitSubPlan` /
    /// `ExecReScanSetParamPlan`). The `execPlan` `SubPlanState *` link is not
    /// carried by the trimmed `ParamExecData`; the executor owns the param array
    /// and the `SubPlanState` pool, so it installs the link. The marking subplan
    /// is the one currently being inited/rescanned (the executor tracks it).
    pub fn mark_param_execplan_pending<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        paramid: i32,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `prm->execPlan = NULL` — clear the PARAM_EXEC `execPlan` link after the
    /// initplan output has been set (nodeSubplan.c `ExecSetParamPlan`). The link
    /// is executor-owned. Fallible only structurally.
    pub fn clear_param_execplan<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        paramid: i32,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `econtext->ecxt_param_exec_vals[paramid].execPlan != NULL` — is the
    /// param not yet evaluated? (`ExecSetParamPlanMulti`). Reads the
    /// executor-owned `execPlan` link. Infallible.
    pub fn param_execplan_pending(estate: &types_nodes::EStateData<'_>, paramid: i32) -> bool
);

seam_core::seam!(
    /// `ExecSetParamPlan(prm->execPlan, econtext)` for the not-yet-evaluated
    /// PARAM_EXEC `paramid` (`ExecSetParamPlanMulti`): the executor resolves the
    /// `SubPlanState` stashed in the param's `execPlan` link and re-enters
    /// `nodeSubplan::ExecSetParamPlan` over it. Fallible (the subplan's failure
    /// surface). The `econtext` is the id of the expression context to evaluate
    /// any down-passed params in.
    pub fn exec_set_param_plan_for_pending<'mcx>(
        econtext: types_nodes::EcxtId,
        paramid: i32,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `sstate->planstate = (PlanState *) list_nth(estate->es_subplanstates,
    /// subplan->plan_id - 1)` then the "subplan was not initialized" check
    /// (nodeSubplan.c:818-827). The executor owns `es_subplanstates`, so it
    /// resolves and installs the link from the already-initialized child plan
    /// state into the node, given the subplan's 1-based `plan_id`. `Err` carries
    /// the C `elog(ERROR, "subplan \"%s\" was not initialized")` when the slot
    /// is NULL (the owner reads `node->subplan->plan_name` for the message).
    pub fn link_subplan_planstate<'mcx>(
        node: &mut types_nodes::execexpr::SubPlanState<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        plan_id: i32,
    ) -> types_error::PgResult<()>
);
