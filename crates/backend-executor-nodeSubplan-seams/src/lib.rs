//! Seam declarations for the `backend-executor-nodeSubplan` unit
//! (`executor/nodeSubplan.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

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

seam_core::seam!(
    /// `ExecSetParamPlan(node, econtext)` (nodeSubplan.c): run an InitPlan
    /// subselect and store its output into the estate's PARAM_EXEC slots,
    /// clearing each param's `execPlan` link. Reached lazily from
    /// `ExecEvalParamExec` when a PARAM_EXEC's value is not yet valid. Errors on
    /// the C sanity checks (ANY/ALL/CTE/correlated as initplan) and on
    /// sub-execution `ereport(ERROR)`.
    pub fn exec_set_param_plan<'mcx>(
        node: &mut types_nodes::SubPlanState<'mcx>,
        econtext: types_nodes::EcxtId,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecSubPlan(node, econtext, isNull)` (nodeSubplan.c): process a
    /// sub-select and return its result `Datum`. The C `bool *isNull`
    /// out-parameter is returned alongside the result as `(Datum, bool)`. This
    /// is the entry point the interpreter's `EEOP_SUBPLAN` step calls
    /// (`*op->resvalue = ExecSubPlan(sstate, econtext, op->resnull)`). Can
    /// `ereport(ERROR)` (sanity checks, sub-execution); carried on `Err`.
    ///
    /// Datum-unification: the returned value is the canonical unified value
    /// type (`types_tuple::Datum<'mcx>`). A scalar sub-select result rides the
    /// by-value arm (the C `Datum` machine word); ARRAY/EXPR_SUBLINK results
    /// that C returns as a pointer to a freshly constructed `ArrayType` ride
    /// the by-reference arm, allocated in the per-query `mcx`.
    pub fn exec_sub_plan<'mcx>(
        node: &mut types_nodes::SubPlanState<'mcx>,
        econtext: types_nodes::EcxtId,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<(types_tuple::backend_access_common_heaptuple::Datum<'mcx>, bool)>
);
