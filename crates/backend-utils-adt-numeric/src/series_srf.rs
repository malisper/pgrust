//! Family: `series-srf` — `generate_series(numeric, numeric [, numeric])` and
//! its planner support function, plus the `numeric()` length-coercion planner
//! support function.
//!
//! These four `numeric.c` functions
//! (`generate_series_numeric`, `generate_series_step_numeric`,
//! `generate_series_numeric_support`, `numeric_support`) carry real own logic —
//! the NaN/infinity/zero-step validation, the per-call cross-state advance and
//! step-sign termination, the `floor((stop-start)/step)+1` row estimate, and the
//! length-coercion no-op detection (scale unchanged + precision non-decreasing).
//! That logic lives here and runs over real [`NumericVar`]s.
//!
//! The pieces that are NOT this unit's are the *call frames they run inside*:
//!
//!  * the Set-Returning-Function (SRF) machinery — `SRF_IS_FIRSTCALL` /
//!    `SRF_FIRSTCALL_INIT` / `SRF_PERCALL_SETUP` / `SRF_RETURN_NEXT` /
//!    `SRF_RETURN_DONE`, the `FuncCallContext`'s `multi_call_memory_ctx` and
//!    `user_fctx` cross-call stash — is owned by `funcapi.c` + the executor's
//!    `ExprContext`/fmgr call frame. Those funcapi helpers themselves panic on
//!    the trimmed `FunctionCallInfoBaseData` (no `flinfo`/`fn_extra`/`econtext`);
//!    so this unit reaches the SRF call frame through the seams below, threaded
//!    over a [`SrfCallFrame`] opacity token, exactly as funcapi will once the
//!    fmgr/ExprContext owners widen those shapes.
//!
//!  * the planner `Node` vocabulary — `SupportRequestSimplify` /
//!    `SupportRequestRows`, `FuncExpr`/`Const` inspection, `exprTypmod`,
//!    `estimate_expression_value`, `relabel_to_typmod` — is owned by the
//!    optimizer / nodeFuncs / makefuncs neighbors (genuinely unported, inherited
//!    opacity per docs/types.md rules 6-7). Reached through the seams below over
//!    a [`PlannerNode`] token, the same pattern the ported
//!    `backend-utils-adt-rangetypes` planner-support family uses.

extern crate alloc;

use mcx::Mcx;
use types_tuple::Datum;
use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};

use types_numeric::var::{GenerateSeriesNumericFctx, NumericSign};
use types_numeric::{
    is_valid_numeric_typmod, numeric_is_nan, numeric_is_special, numeric_typmod_precision,
    numeric_typmod_scale,
};

use crate::convert::{make_result, set_var_from_num};
use crate::kernel_transcendental::numericvar_to_double_no_overflow;
use crate::kernel_var::{add_var, cmp_var, const_one, set_var_from_var, sub_var, trunc_var};

// ===========================================================================
// SRF call frame — inherited opacity (owner: funcapi.c + the executor's fmgr
// call frame / ExprContext, still trimmed). `0` is no frame.
// ===========================================================================

/// A `FuncCallContext *` / SRF call frame token. The cross-call state lives in
/// the executor-owned multi-call context this token names; numeric never sees
/// its fields, it only drives the SRF protocol through the seams below.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct SrfCallFrame(pub u64);

seam_core::seam!(
    /// `SRF_IS_FIRSTCALL()` (funcapi.h): is this the first call of the SRF in
    /// the current scan (`fcinfo->flinfo->fn_extra == NULL`).
    pub fn srf_is_firstcall() -> bool
);

seam_core::seam!(
    /// `SRF_FIRSTCALL_INIT()` (funcapi.h): `init_MultiFuncCall(fcinfo)` — create
    /// the multi-call memory context and `FuncCallContext`, returning its frame
    /// token. `Err` carries the `ERRCODE_FEATURE_NOT_SUPPORTED` ereport when the
    /// call site cannot accept a set.
    pub fn srf_firstcall_init() -> PgResult<SrfCallFrame>
);

seam_core::seam!(
    /// `SRF_PERCALL_SETUP()` (funcapi.h): `per_MultiFuncCall(fcinfo)` — recover
    /// the `FuncCallContext` saved for this scan.
    pub fn srf_percall_setup() -> SrfCallFrame
);

seam_core::seam!(
    /// Store the `generate_series_numeric_fctx` cross-call state into
    /// `funcctx->user_fctx`, allocated in `funcctx->multi_call_memory_ctx`. The
    /// state itself (three `NumericVar`s) is this unit's; the long-lived context
    /// it is parked in is the executor's.
    pub fn srf_set_user_fctx<'mcx>(frame: SrfCallFrame, fctx: GenerateSeriesNumericFctx<'mcx>)
);

seam_core::seam!(
    /// Fetch a mutable handle to the `generate_series_numeric_fctx` previously
    /// stored with [`srf_set_user_fctx`] (C `funcctx->user_fctx`). The returned
    /// borrow is tied to `mcx` (the multi-call memory context the owner parked
    /// the state in; passed so the borrow lifetime is constrained by an input).
    pub fn srf_get_user_fctx<'mcx>(
        mcx: Mcx<'mcx>,
        frame: SrfCallFrame,
    ) -> &'mcx mut GenerateSeriesNumericFctx<'mcx>
);

seam_core::seam!(
    /// `SRF_RETURN_NEXT(funcctx, result)` (funcapi.h): bump `call_cntr` and emit
    /// one result `Datum`, returning a value-mode `Datum` for the fmgr return.
    pub fn srf_return_next<'mcx>(frame: SrfCallFrame, result: Datum<'mcx>) -> Datum<'mcx>
);

seam_core::seam!(
    /// `SRF_RETURN_DONE(funcctx)` (funcapi.h): `end_MultiFuncCall(fcinfo,
    /// funcctx)` then set the result-set "done" mode; returns the fmgr null
    /// `Datum`.
    pub fn srf_return_done(frame: SrfCallFrame) -> Datum<'static>
);

seam_core::seam!(
    /// `NumericGetDatum(make_result(var))` materialized in the per-query result
    /// context: turn the freshly built on-disk numeric image into a `Datum` the
    /// SRF can hand back. (The result lives in the executor's per-query context,
    /// not in this unit's `mcx`.)
    pub fn numeric_image_to_datum(image: &[u8]) -> Datum<'static>
);

/// `generate_series_numeric(PG_FUNCTION_ARGS)` (numeric.c:1701): the 2-arg
/// entry; identical to the stepped form with an implicit step of 1.
pub fn generate_series_numeric<'mcx>(
    mcx: Mcx<'mcx>,
    start: &[u8],
    stop: &[u8],
) -> PgResult<Datum<'mcx>> {
    // C: return generate_series_step_numeric(fcinfo);
    generate_series_step_numeric(mcx, start, stop, None)
}

/// `generate_series_step_numeric(PG_FUNCTION_ARGS)` (numeric.c:1708): generate
/// a series of numeric values from `start` to `stop` by `step` (default 1).
///
/// `start`/`stop`/`opt_step` are the on-disk byte args (read via
/// `PG_GETARG_NUMERIC`; `opt_step` is `None` when `PG_NARGS() == 2`). The SRF
/// protocol is driven through the call-frame seams.
pub fn generate_series_step_numeric<'mcx>(
    mcx: Mcx<'mcx>,
    start: &[u8],
    stop: &[u8],
    opt_step: Option<&[u8]>,
) -> PgResult<Datum<'mcx>> {
    if srf_is_firstcall::call() {
        // Reject NaN and infinities in start and stop values.
        if numeric_is_special(start) {
            if numeric_is_nan(start) {
                return Err(PgError::error("start value cannot be NaN")
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            } else {
                return Err(PgError::error("start value cannot be infinity")
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            }
        }
        if numeric_is_special(stop) {
            if numeric_is_nan(stop) {
                return Err(PgError::error("stop value cannot be NaN")
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            } else {
                return Err(PgError::error("stop value cannot be infinity")
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            }
        }

        // steploc = const_one; see if we were given an explicit step size.
        let steploc = if let Some(step) = opt_step {
            if numeric_is_special(step) {
                if numeric_is_nan(step) {
                    return Err(PgError::error("step size cannot be NaN")
                        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
                } else {
                    return Err(PgError::error("step size cannot be infinity")
                        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
                }
            }

            let steploc = set_var_from_num(mcx, step)?;

            // if (cmp_var(&steploc, &const_zero) == 0) error
            if steploc.ndigits() == 0 {
                return Err(PgError::error("step size cannot equal zero")
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            }
            steploc
        } else {
            const_one(mcx)
        };

        // Create a function context for cross-call persistence.
        let frame = srf_firstcall_init::call()?;

        // Switch to memory context appropriate for multiple function calls; the
        // fctx NumericVars are stored in the multi-call context behind the seam.
        // Seed `current` with the original start value (copied, not aliased).
        let fctx = GenerateSeriesNumericFctx {
            current: set_var_from_num(mcx, start)?,
            stop: set_var_from_num(mcx, stop)?,
            step: set_var_from_var(mcx, &steploc)?,
        };
        srf_set_user_fctx::call(frame, fctx);
    }

    // Stuff done on every call of the function.
    let funcctx = srf_percall_setup::call();

    // Get the saved state and use current state as the result of this iteration.
    let fctx: &mut GenerateSeriesNumericFctx<'mcx> = srf_get_user_fctx::call(mcx, funcctx);

    let in_range = (fctx.step.sign == NumericSign::Pos
        && cmp_var(&fctx.current, &fctx.stop) <= core::cmp::Ordering::Equal)
        || (fctx.step.sign == NumericSign::Neg
            && cmp_var(&fctx.current, &fctx.stop) >= core::cmp::Ordering::Equal);

    if in_range {
        let result = make_result(mcx, &fctx.current)?;
        let datum = numeric_image_to_datum::call(result.as_slice());

        // Increment current in preparation for next iteration (in the
        // multi-call context).
        fctx.current = add_var(mcx, &fctx.current, &fctx.step)?;

        // Do when there is more left to send.
        Ok(srf_return_next::call(funcctx, datum))
    } else {
        // Do when there is no more left.
        Ok(srf_return_done::call(funcctx))
    }
}

// ===========================================================================
// Planner support functions — inherited planner Node opacity (owner: the
// optimizer / nodeFuncs / makefuncs neighbors, genuinely unported).
// ===========================================================================

/// A planner `Node *` (`nodes.h`). Inherited opacity (docs/types.md rules 6-7);
/// `0` models C's `NULL`. Resolves to the real node type when the optimizer's
/// node vocabulary lands.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct PlannerNode(pub u64);

impl PlannerNode {
    /// C's `NULL` node.
    pub const NULL: PlannerNode = PlannerNode(0);

    fn is_null(self) -> bool {
        self.0 == 0
    }
}

seam_core::seam!(
    /// `IsA(rawreq, SupportRequestSimplify)` (nodes.h).
    pub fn is_support_request_simplify(node: PlannerNode) -> bool
);

seam_core::seam!(
    /// `IsA(rawreq, SupportRequestRows)` (nodes.h).
    pub fn is_support_request_rows(node: PlannerNode) -> bool
);

seam_core::seam!(
    /// `((SupportRequestSimplify *) req)->fcall` (supportnodes.h): the
    /// `FuncExpr *` of the call to be simplified.
    pub fn support_request_simplify_fcall(req: PlannerNode) -> PlannerNode
);

seam_core::seam!(
    /// `((SupportRequestRows *) req)->root` (supportnodes.h): the `PlannerInfo *`
    /// (may be `NULL`).
    pub fn support_request_rows_root(req: PlannerNode) -> PlannerNode
);

seam_core::seam!(
    /// `((SupportRequestRows *) req)->node` (supportnodes.h): the parse node
    /// invoking the function.
    pub fn support_request_rows_node(req: PlannerNode) -> PlannerNode
);

seam_core::seam!(
    /// `((SupportRequestRows *) req)->rows = rows; ret = (Node *) req;`
    /// (supportnodes.h): store the estimated row count into the request and
    /// return it as the result node.
    pub fn support_request_rows_set(req: PlannerNode, rows: f64) -> PlannerNode
);

seam_core::seam!(
    /// `is_funcclause(node)` (clauses.c): is the node a `FuncExpr`.
    pub fn is_funcclause(node: PlannerNode) -> bool
);

seam_core::seam!(
    /// `list_length(((FuncExpr *) node)->args)` (pg_list.h): the call's argument
    /// count.
    pub fn func_expr_nargs(node: PlannerNode) -> i32
);

seam_core::seam!(
    /// `linitial/lsecond/lthird(((FuncExpr *) node)->args)` (pg_list.h): the
    /// `n`-th (0-based) argument `Expr *` of the function call.
    pub fn func_expr_arg(node: PlannerNode, n: i32) -> PlannerNode
);

seam_core::seam!(
    /// `estimate_expression_value(root, node)` (clauses.c): pre-evaluate the
    /// argument, folding it to a `Const` where possible.
    pub fn estimate_expression_value(root: PlannerNode, node: PlannerNode) -> PlannerNode
);

seam_core::seam!(
    /// `IsA(expr, Const)` (nodes.h).
    pub fn is_const(expr: PlannerNode) -> bool
);

seam_core::seam!(
    /// `((Const *) expr)->constisnull` (primnodes.h).
    pub fn const_is_null(expr: PlannerNode) -> bool
);

seam_core::seam!(
    /// `((Const *) expr)->constvalue` (primnodes.h): the `Const`'s payload
    /// `Datum` (a `numeric` pointer Datum here).
    pub fn const_value(expr: PlannerNode) -> Datum<'static>
);

seam_core::seam!(
    /// `exprTypmod(source)` (nodeFuncs.c): the typmod of the coercion source
    /// expression.
    pub fn expr_typmod(source: PlannerNode) -> i32
);

seam_core::seam!(
    /// `relabel_to_typmod(source, new_typmod)` (nodeFuncs.c): wrap `source` in a
    /// `RelabelType` carrying the new typmod (the length-coercion no-op result).
    pub fn relabel_to_typmod<'mcx>(
        mcx: Mcx<'mcx>,
        source: PlannerNode,
        new_typmod: i32,
    ) -> PlannerNode
);

/// `numeric_support(PG_FUNCTION_ARGS)` (numeric.c:1195): planner support for the
/// `numeric()` length-coercion function. Flattens calls that only widen the
/// allowable precision (scale unchanged, precision non-decreasing, or the
/// destination unconstrained).
pub fn numeric_support<'mcx>(mcx: Mcx<'mcx>, rawreq: PlannerNode) -> PgResult<PlannerNode> {
    let mut ret = PlannerNode::NULL;

    if is_support_request_simplify::call(rawreq) {
        // FuncExpr *expr = req->fcall;
        let expr = support_request_simplify_fcall::call(rawreq);

        // Assert(list_length(expr->args) >= 2);
        debug_assert!(func_expr_nargs::call(expr) >= 2);

        // typmod = (Node *) lsecond(expr->args);
        let typmod = func_expr_arg::call(expr, 1);

        if is_const::call(typmod) && !const_is_null::call(typmod) {
            // source = (Node *) linitial(expr->args);
            let source = func_expr_arg::call(expr, 0);
            let old_typmod = expr_typmod::call(source);
            // new_typmod = DatumGetInt32(((Const *) typmod)->constvalue);
            let new_typmod = const_value::call(typmod).as_usize() as i32;
            let old_scale = numeric_typmod_scale(old_typmod);
            let new_scale = numeric_typmod_scale(new_typmod);
            let old_precision = numeric_typmod_precision(old_typmod);
            let new_precision = numeric_typmod_precision(new_typmod);

            // If new_typmod is invalid, the destination is unconstrained;
            // that's always OK. If old_typmod is valid, the source is
            // constrained, and we're OK if the scale is unchanged and the
            // precision is not decreasing.
            if !is_valid_numeric_typmod(new_typmod)
                || (is_valid_numeric_typmod(old_typmod)
                    && new_scale == old_scale
                    && new_precision >= old_precision)
            {
                ret = relabel_to_typmod::call(mcx, source, new_typmod);
            }
        }
    }

    Ok(ret)
}

/// `generate_series_numeric_support(PG_FUNCTION_ARGS)` (numeric.c:1834): planner
/// support for `generate_series(numeric, numeric [, numeric])` — estimate the
/// number of rows returned via `floor((stop - start) / step) + 1`.
pub fn generate_series_numeric_support<'mcx>(
    mcx: Mcx<'mcx>,
    rawreq: PlannerNode,
) -> PgResult<PlannerNode> {
    let mut ret = PlannerNode::NULL;

    if is_support_request_rows::call(rawreq) {
        let node = support_request_rows_node::call(rawreq);
        let root = support_request_rows_root::call(rawreq);

        if is_funcclause::call(node) {
            // be paranoid
            let nargs = func_expr_nargs::call(node);

            // We can use estimated argument values here.
            let arg1 = estimate_expression_value::call(root, func_expr_arg::call(node, 0));
            let arg2 = estimate_expression_value::call(root, func_expr_arg::call(node, 1));
            let arg3 = if nargs >= 3 {
                estimate_expression_value::call(root, func_expr_arg::call(node, 2))
            } else {
                PlannerNode::NULL
            };

            // If any argument is constant NULL, zero rows are returned.
            // Otherwise, if they're all non-NULL constants, we can compute the
            // row count.
            if (is_const::call(arg1) && const_is_null::call(arg1))
                || (is_const::call(arg2) && const_is_null::call(arg2))
                || (!arg3.is_null() && is_const::call(arg3) && const_is_null::call(arg3))
            {
                ret = support_request_rows_set::call(rawreq, 0.0);
            } else if is_const::call(arg1)
                && is_const::call(arg2)
                && (arg3.is_null() || is_const::call(arg3))
            {
                // If any argument is NaN or infinity, generate_series() will
                // error out, so we needn't produce an estimate.
                let start_bytes = unsafe { numeric_bytes_from_datum(const_value::call(arg1)) };
                let stop_bytes = unsafe { numeric_bytes_from_datum(const_value::call(arg2)) };

                if numeric_is_special(start_bytes) || numeric_is_special(stop_bytes) {
                    return Ok(PlannerNode::NULL);
                }

                // step defaults to const_one.
                let step = if !arg3.is_null() {
                    let step_bytes = unsafe { numeric_bytes_from_datum(const_value::call(arg3)) };
                    if numeric_is_special(step_bytes) {
                        return Ok(PlannerNode::NULL);
                    }
                    set_var_from_num(mcx, step_bytes)?
                } else {
                    const_one(mcx)
                };

                // The number of rows returned is floor((stop - start) / step)
                // + 1, if the sign of step matches the sign of stop - start;
                // otherwise no rows are returned. (cmp_var(&step,&const_zero)!=0
                // i.e. step has digits.)
                if step.ndigits() != 0 {
                    let start = set_var_from_num(mcx, start_bytes)?;
                    let stop = set_var_from_num(mcx, stop_bytes)?;

                    let mut res = sub_var(mcx, &stop, &start)?;

                    if step.sign != res.sign {
                        // no rows will be returned
                        ret = support_request_rows_set::call(rawreq, 0.0);
                    } else {
                        if !arg3.is_null() {
                            // div_var(&res, &step, &res, 0, false, false);
                            res = crate::kernel_var::div_var(mcx, &res, &step, 0, false, false)?;
                        } else {
                            // trunc_var(&res, 0); /* step = 1 */
                            trunc_var(&mut res, 0);
                        }

                        let rows = numericvar_to_double_no_overflow(&res) + 1.0;
                        ret = support_request_rows_set::call(rawreq, rows);
                    }
                }
            }
        }
    }

    Ok(ret)
}

/// Recover the on-disk `numeric` byte image a pointer-bearing `Const` `Datum`
/// refers to (`DatumGetNumeric`). Mirrors `ops_sql::numeric_bytes_from_datum`.
///
/// # Safety
/// `d` must point to a 4-byte-header (`VARATT_IS_4B_U`) `numeric` varlena that
/// outlives the returned slice. The planner const Datums are already-detoasted.
unsafe fn numeric_bytes_from_datum<'a>(d: Datum<'_>) -> &'a [u8] {
    use types_datum::VARHDRSZ;
    let ptr = d.as_usize() as *const u8;
    let header = core::slice::from_raw_parts(ptr, VARHDRSZ);
    let word = u32::from_ne_bytes([header[0], header[1], header[2], header[3]]);
    #[cfg(target_endian = "little")]
    let len = ((word >> 2) & 0x3FFF_FFFF) as usize;
    #[cfg(target_endian = "big")]
    let len = (word & 0x3FFF_FFFF) as usize;
    core::slice::from_raw_parts(ptr, len)
}
