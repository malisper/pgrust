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
//!  * the planner-support *dispatch* — the `SupportRequestSimplify` /
//!    `SupportRequestRows` request nodes the fmgr machinery hands a support
//!    function — is owned by the optimizer's support-call sites
//!    (`clauses.c:simplify_function` and `plancat.c:get_function_rows`). This
//!    codebase models those request nodes *decomposed* (the dispatcher passes
//!    the function call's argument list and per-call scalars and reads back the
//!    simplified `Expr` / estimated row count), mirroring the
//!    `clauses_seam::call_support_simplify` contract. So both support fns run
//!    over the real [`Expr`] / [`PlannerInfo`] node vocabulary and call the real
//!    `exprTypmod` / `relabel_to_typmod` / `estimate_expression_value` owners
//!    directly; only the dispatch entry that routes a `prosupport` OID to the
//!    right support fn is still unported (the `SupportRequestRows` leg in
//!    particular — `get_function_rows` panics workspace-wide).

extern crate alloc;

use mcx::Mcx;
use types_tuple::Datum;
use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};

use types_nodes::primnodes::Expr;
use backend_nodes_core::nodefuncs::{expr_typmod, relabel_to_typmod};
use backend_optimizer_util_clauses::estimate_expression_value;

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
// Planner support functions — re-signed onto the real node vocabulary.
//
// C's `numeric_support` / `generate_series_numeric_support` each receive a
// `SupportRequest*` node via fmgr and read the wrapped `FuncExpr`'s argument
// list. This codebase models that request *decomposed* (the dispatch site
// passes the argument `Expr`s and per-call scalars and reads the result back),
// the same shape as `clauses_seam::call_support_simplify`. So both fns run over
// the real `Expr` / `PlannerInfo` and call the real `exprTypmod`,
// `relabel_to_typmod` (nodeFuncs.c) and `estimate_expression_value` (clauses.c)
// owners directly.
// ===========================================================================

/// `numeric_support(PG_FUNCTION_ARGS)` (numeric.c:1195): planner support for the
/// `numeric()` length-coercion function. Flattens calls that only widen the
/// allowable precision (scale unchanged, precision non-decreasing, or the
/// destination unconstrained).
///
/// The C `SupportRequestSimplify` is decomposed: `args` is the call's argument
/// list (`req->fcall->args`). `Ok(Some(expr))` is the simplified clause (C's
/// `ret`); `Ok(None)` is "no simplification" (C's `NULL`). Reached from the
/// `simplify_function` support-call site once the `prosupport`-OID dispatch
/// lands.
pub fn numeric_support<'mcx>(args: &[Expr<'mcx>]) -> PgResult<Option<Expr<'mcx>>> {
    // FuncExpr *expr = req->fcall; Assert(list_length(expr->args) >= 2);
    debug_assert!(args.len() >= 2);

    // typmod = (Node *) lsecond(expr->args);
    let typmod = &args[1];

    // if (IsA(typmod, Const) && !((Const *) typmod)->constisnull)
    if let Expr::Const(typmod_const) = typmod {
        if !typmod_const.constisnull {
            // source = (Node *) linitial(expr->args);
            let source = &args[0];
            let old_typmod = expr_typmod(Some(source))?;
            // new_typmod = DatumGetInt32(((Const *) typmod)->constvalue);
            let new_typmod = typmod_const.constvalue.as_usize() as i32;
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
                return Ok(Some(relabel_to_typmod(source.clone(), new_typmod)?));
            }
        }
    }

    Ok(None)
}

/// `generate_series_numeric_support(PG_FUNCTION_ARGS)` (numeric.c:1834): planner
/// support for `generate_series(numeric, numeric [, numeric])` — estimate the
/// number of rows returned via `floor((stop - start) / step) + 1`.
///
/// The C `SupportRequestRows` is decomposed: `args` is the invoked call's
/// argument list (`req->node`'s `FuncExpr->args`, already known a funcclause by
/// the dispatcher). `Ok(Some(rows))` stores the estimate into the request (C's
/// `req->rows = rows`); `Ok(None)` declines (C's `NULL`). The `req->root` field
/// is unused by this support function (C never reads it), so it is not threaded.
///
/// The argument folding routes through the real `estimate_expression_value`
/// (clauses.c) and the row arithmetic runs over real `NumericVar`s. The only
/// piece still unported is the `SupportRequestRows` *dispatch* itself
/// (`get_function_rows`, plancat.c, panics workspace-wide) — when it lands it
/// calls this fn with the decomposed request.
pub fn generate_series_numeric_support<'mcx>(
    mcx: Mcx<'mcx>,
    args: &[Expr<'mcx>],
) -> PgResult<Option<f64>> {
    let nargs = args.len();

    // We can use estimated argument values here. estimate_expression_value
    // folds each arg to a Const where possible; it consumes its argument, so
    // pass owned clones (C copies into the SupportRequest's working node).
    let arg1 = estimate_expression_value(mcx, args[0].clone())?;
    let arg2 = estimate_expression_value(mcx, args[1].clone())?;
    let arg3: Option<Expr<'mcx>> = if nargs >= 3 {
        Some(estimate_expression_value(mcx, args[2].clone())?)
    } else {
        None
    };

    // const_of(e): the e as a non-null Const's value bytes, or a null/non-Const
    // classification. Returns (is_const, is_null, bytes-if-const-non-null).
    let arg1c = as_const(&arg1);
    let arg2c = as_const(&arg2);
    let arg3c = arg3.as_ref().map(as_const);

    // If any argument is constant NULL, zero rows are returned. Otherwise, if
    // they're all non-NULL constants, we can compute the row count.
    if matches!(arg1c, ConstArg::Null)
        || matches!(arg2c, ConstArg::Null)
        || matches!(arg3c, Some(ConstArg::Null))
    {
        return Ok(Some(0.0));
    }

    if let (ConstArg::Value(start_dat), ConstArg::Value(stop_dat)) = (&arg1c, &arg2c) {
        // arg3 absent, or a (possibly-null already handled) Const.
        let step_dat: Option<&Datum<'mcx>> = match &arg3c {
            None => None,
            Some(ConstArg::Value(d)) => Some(d),
            // arg3 present but not a plain Const -> can't estimate.
            Some(_) => return Ok(None),
        };
        let have_step = step_dat.is_some();

        // If any argument is NaN or infinity, generate_series() will error out,
        // so we needn't produce an estimate.
        let start_bytes = numeric_bytes_from_datum(start_dat);
        let stop_bytes = numeric_bytes_from_datum(stop_dat);

        if numeric_is_special(start_bytes) || numeric_is_special(stop_bytes) {
            return Ok(None);
        }

        // step defaults to const_one.
        let step = if let Some(d) = step_dat {
            let step_bytes = numeric_bytes_from_datum(d);
            if numeric_is_special(step_bytes) {
                return Ok(None);
            }
            set_var_from_num(mcx, step_bytes)?
        } else {
            const_one(mcx)
        };

        // The number of rows returned is floor((stop - start) / step) + 1, if
        // the sign of step matches the sign of stop - start; otherwise no rows
        // are returned. (cmp_var(&step,&const_zero)!=0 i.e. step has digits.)
        if step.ndigits() != 0 {
            let start = set_var_from_num(mcx, start_bytes)?;
            let stop = set_var_from_num(mcx, stop_bytes)?;

            let mut res = sub_var(mcx, &stop, &start)?;

            if step.sign != res.sign {
                // no rows will be returned
                return Ok(Some(0.0));
            } else {
                if have_step {
                    // div_var(&res, &step, &res, 0, false, false);
                    res = crate::kernel_var::div_var(mcx, &res, &step, 0, false, false)?;
                } else {
                    // trunc_var(&res, 0); /* step = 1 */
                    trunc_var(&mut res, 0);
                }

                let rows = numericvar_to_double_no_overflow(&res) + 1.0;
                return Ok(Some(rows));
            }
        }
    }

    Ok(None)
}

/// `generate_series_numeric_support`'s pg_proc OID (`fmgroids.h`
/// `F_GENERATE_SERIES_NUMERIC_SUPPORT`).
pub const GENERATE_SERIES_NUMERIC_SUPPORT: types_core::Oid = 6357;

/// The [`SupportRowsFn`](backend_optimizer_util_clauses::support_rows::SupportRowsFn)
/// adapter for `generate_series_numeric_support`: decompose the request's
/// (const-folded) `FuncExpr` `node` into its argument list and run the kernel in
/// a transient context. Mirrors the C dispatch reading `((FuncExpr *)
/// req->node)->args`.
fn generate_series_numeric_support_rows(
    _funcid: types_core::Oid,
    node: &Expr,
) -> PgResult<Option<f64>> {
    // if (req->node && IsA(req->node, FuncExpr))
    let Expr::FuncExpr(fexpr) = node else {
        return Ok(None);
    };
    let cx = mcx::MemoryContext::new("generate_series_numeric_support rows");
    // Re-home the call's argument Exprs into the transient context (C reads
    // `((FuncExpr *) req->node)->args` directly; the kernel folds owned clones).
    let mut args: alloc::vec::Vec<Expr> = alloc::vec::Vec::with_capacity(fexpr.args.len());
    for a in fexpr.args.iter() {
        args.push(a.clone_in(cx.mcx())?);
    }
    generate_series_numeric_support(cx.mcx(), &args)
}

/// Register the `generate_series_numeric_support` `SupportRequestRows` kernel
/// under its pg_proc OID. Called from this crate's `init_seams`.
pub fn register_series_support_rows() {
    backend_optimizer_util_clauses::support_rows::register_support_rows(
        GENERATE_SERIES_NUMERIC_SUPPORT,
        generate_series_numeric_support_rows,
    );
}

/// One folded `generate_series` argument, classified as C's
/// `IsA(arg, Const)` / `constisnull` triple needs.
enum ConstArg<'mcx> {
    /// `IsA(arg, Const)` and `constisnull` — zero rows.
    Null,
    /// `IsA(arg, Const)` and not null — the payload `Datum`.
    Value(Datum<'mcx>),
    /// not a `Const` — can't estimate.
    NotConst,
}

/// `IsA(expr, Const)` + null/value split (primnodes.h).
fn as_const<'mcx>(expr: &Expr<'mcx>) -> ConstArg<'mcx> {
    match expr {
        Expr::Const(c) if c.constisnull => ConstArg::Null,
        Expr::Const(c) => ConstArg::Value(c.constvalue.clone()),
        _ => ConstArg::NotConst,
    }
}

/// Recover the on-disk `numeric` byte image a `Const` `Datum` refers to
/// (`DatumGetNumeric`). The planner's `Const` numeric carries its varlena image
/// in the canonical `Datum::ByRef` arm (already-detoasted, 4-byte header), so
/// the bytes are borrowed straight from the Datum — no raw pointer chasing. A
/// legacy `ByVal` pointer-Datum is still handled for the by-value lane.
fn numeric_bytes_from_datum<'a>(d: &'a Datum<'a>) -> &'a [u8] {
    if let Datum::ByRef(_) | Datum::Cstring(_) = d {
        return d.as_ref_bytes();
    }
    // Legacy by-value pointer Datum (the c2rust model): chase the pointer.
    unsafe {
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
}
