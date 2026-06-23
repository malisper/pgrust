//! Seam declarations for `optimizer/util/paramassign.c` — the PARAM_EXEC slot
//! assignment routines consumed across a dependency cycle, principally by
//! `createplan.c` (which calls `replace_nestloop_param_*`,
//! `process_subquery_nestloop_params`, `identify_current_nestloop_params`, and
//! `assign_special_exec_param` as it builds lower Plan nodes) and by
//! `subselect.c` / `setrefs.c` (the `replace_outer_*` family).
//!
//! The owner is `backend-optimizer-util-paramassign`; it installs every seam
//! here from its `init_seams()`. All entries operate over the lifetime-free
//! arena+handle model of [`::pathnodes::PlannerInfo`]: `Var`/`Param`/
//! `PlaceHolderVar` are real node VALUES; `root->glob->paramExecTypes`,
//! `root->plan_params`, and `root->curOuterParams` are the three planner data
//! structures paramassign manages.
//!
//! Allocating seams take `Mcx<'mcx>` (the placeholder/aggref/returning copies go
//! through `Expr::clone_in`). Failure surface: every routine can `ereport(ERROR)`
//! (the allocating steps plus the explicit `elog(ERROR)` sites), so every seam
//! returns [`types_error::PgResult`].

#![allow(non_snake_case)]

extern crate alloc;

use ::mcx::Mcx;
use ::nodes::primnodes::{Param, PlaceHolderVar, Var};
use ::pathnodes::{NodeId, PlannerInfo, Relids};

seam_core::seam!(
    /// `replace_nestloop_param_var(root, var)` (paramassign.c:413): generate a
    /// `Param` to replace `var`, which is supplied by some upper NestLoop plan
    /// node; record the need in `root->curOuterParams`. Reuses an existing
    /// `NestLoopParam` slot if `var` already appears there.
    pub fn replace_nestloop_param_var(root: &mut PlannerInfo, var: &Var) -> types_error::PgResult<Param>
);

seam_core::seam!(
    /// `replace_nestloop_param_placeholdervar(root, phv)` (paramassign.c:462):
    /// like `replace_nestloop_param_var`, but for a `PlaceHolderVar`. Allocates
    /// (deep-copies the PHV), hence the `Mcx`.
    pub fn replace_nestloop_param_placeholdervar<'mcx>(
        mcx: Mcx<'mcx>,
        root: &mut PlannerInfo,
        phv: &PlaceHolderVar,
    ) -> types_error::PgResult<Param>
);

seam_core::seam!(
    /// `process_subquery_nestloop_params(root, subplan_params)`
    /// (paramassign.c:526): add `root->curOuterParams` entries for a
    /// parameterized subquery-in-FROM's LATERAL references. `subplan_params` is
    /// the list of `PlannerParamItem` arena handles the subquery demanded.
    /// `Err` carries the `elog(ERROR, "non-LATERAL parameter required by
    /// subquery")` / `"unexpected type of subquery parameter"` surface.
    pub fn process_subquery_nestloop_params<'mcx>(
        mcx: Mcx<'mcx>,
        root: &mut PlannerInfo,
        subplan_params: &[NodeId],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `identify_current_nestloop_params(root, leftrelids, outerrelids)`
    /// (paramassign.c:621): remove the `NestLoopParam`s that the current
    /// NestLoop plan node (with the given lefthand + required-outer rels) should
    /// supply from `root->curOuterParams`, and return them as a list of
    /// `NestLoopParam` arena handles. Reads `root->parse->hasSubLinks` via the
    /// planner-run resolver, hence the `&PlannerRun` parameter.
    pub fn identify_current_nestloop_params<'mcx>(
        mcx: Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &::pathnodes::planner_run::PlannerRun<'mcx>,
        leftrelids: &Relids,
        outerrelids: &Relids,
    ) -> types_error::PgResult<alloc::vec::Vec<NodeId>>
);

seam_core::seam!(
    /// `generate_new_exec_param(root, paramtype, paramtypmod, paramcollation)`
    /// (paramassign.c:726): make a fresh PARAM_EXEC `Param` that will not
    /// conflict with any other; records its type in
    /// `root->glob->paramExecTypes`.
    pub fn generate_new_exec_param(
        root: &mut PlannerInfo,
        paramtype: types_core::Oid,
        paramtypmod: i32,
        paramcollation: types_core::Oid,
    ) -> types_error::PgResult<Param>
);

seam_core::seam!(
    /// `assign_special_exec_param(root)` (paramassign.c:753): assign a
    /// (nonnegative) PARAM_EXEC ID for a special signaling parameter (no `Param`
    /// node exists for it). Appends `InvalidOid` to `root->glob->paramExecTypes`.
    pub fn assign_special_exec_param(root: &mut PlannerInfo) -> types_error::PgResult<i32>
);
