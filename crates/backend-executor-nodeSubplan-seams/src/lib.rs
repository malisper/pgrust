//! Seam declarations for the `backend-executor-nodeSubplan` unit
//! (`executor/nodeSubplan.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `UpdateChangedParamSet(node, newchg)` (nodeSubplan.c): add the changed
    /// parameters in `newchg` that the node's plan depends on
    /// (`plan->allParam`) to `node->chgParam`. `mcx` is the per-query context
    /// the set storage grows in (C: `bms_intersect`/`bms_join` palloc in
    /// `CurrentMemoryContext`); fallible on OOM.
    pub fn update_changed_param_set<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        node: &mut types_nodes::PlanStateNode<'mcx>,
        newchg: &types_nodes::Bitmapset<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecReScanSetParamPlan(node, parent)` (nodeSubplan.c): mark an
    /// InitPlan's output params as needing recalculation (set `execPlan` in
    /// the estate's param array, add the params to the parent's `chgParam`).
    /// The C `parent` argument splits into its consumed parts: the parent's
    /// `chgParam` slot (`bms_add_member` may grow or replace the set) and the
    /// threaded `estate` (the C `parent->state`). Errors with the C
    /// sanity-check `elog(ERROR)`s (direct-correlated or paramless subplans)
    /// and on OOM.
    pub fn exec_re_scan_set_param_plan<'mcx>(
        node: &mut types_nodes::SubPlanState<'mcx>,
        parent_chg_param: &mut Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);
