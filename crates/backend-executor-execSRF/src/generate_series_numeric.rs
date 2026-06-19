//! `generate_series(numeric, numeric [, numeric])` (OIDs 3259/3260) registered
//! as executor-frame set-returning functions.
//!
//! `numeric.c`'s `generate_series_numeric` / `generate_series_step_numeric`
//! are value-per-call SRFs whose value core (the NaN/infinity/zero-step
//! validation + the per-call `cmp_var`/`add_var` advance over `NumericVar`s) is
//! ported in `backend-utils-adt-numeric::series_srf`, with the underlying
//! arithmetic kernels (`set_var_from_num`, `cmp_var`, `add_var`, `make_result`)
//! in `convert`/`kernel_var`.
//!
//! That core's own SRF driver routes through `funcapi`-shaped seams
//! (`srf_is_firstcall` / `srf_set_user_fctx` / ...) that the trimmed executor
//! frame cannot install (they have no `fcinfo` channel). So — exactly as the
//! int4/int8 `generate_series` adapter does — this unit drives the value-per-call
//! protocol DIRECTLY over the executor frame (`init_MultiFuncCall` /
//! `per_MultiFuncCall` / `end_MultiFuncCall`) and calls the numeric arithmetic
//! kernels for each step. The cross-call state is the canonical on-disk numeric
//! byte images of `current`/`stop`/`step` (lifetime-free, so they live behind the
//! `dyn Any` `user_fctx` carrier); the `NumericVar`s are re-materialized from
//! them each call in the multi-call `Mcx` — the same values, just rebuilt from
//! their canonical image, never aliased.
//!
//! Registered from [`register_generate_series_numeric`] (called by `init_seams`)
//! — the executor-frame `fmgrtab.c` analogue for these SRFs.

use core::any::Any;

use backend_utils_adt_numeric::convert::{make_result, set_var_from_num};
use backend_utils_adt_numeric::kernel_var::{add_var, cmp_var, const_one};
use mcx::{Mcx, PgBox};
use types_core::Oid;
use types_error::error::ERRCODE_INVALID_PARAMETER_VALUE;
use types_error::PgError;
use types_nodes::execexpr::ExprDoneCond;
use types_nodes::fmgr::{FmgrArgRef, FunctionCallInfoBaseData};
use types_numeric::var::NumericSign;
use types_numeric::{numeric_is_nan, numeric_is_special};
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_utils_fmgr_funcapi::srf_support::{
    end_MultiFuncCall, init_MultiFuncCall, per_MultiFuncCall,
};

use crate::register_srf;

/// `generate_series_step_numeric(numeric, numeric, numeric)` (OID 3259).
const GENERATE_SERIES_STEP_NUMERIC: Oid = 3259;
/// `generate_series_numeric(numeric, numeric)` (OID 3260) — the 2-arg form, step 1.
const GENERATE_SERIES_NUMERIC: Oid = 3260;

/// Register the numeric `generate_series` SRFs in the executor-frame table.
pub(crate) fn register_generate_series_numeric() {
    register_srf(GENERATE_SERIES_STEP_NUMERIC, generate_series_step_numeric);
    register_srf(GENERATE_SERIES_NUMERIC, generate_series_step_numeric);
}

/// The lifetime-free cross-call state for numeric `generate_series` (C:
/// `generate_series_numeric_fctx`'s three `NumericVar`s). Stored as canonical
/// on-disk numeric byte images so the state can live behind the `dyn Any`
/// (`Any: 'static`) `user_fctx` carrier across the row series; each `NumericVar`
/// is re-materialized from its image per call (in the multi-call `Mcx`).
struct SeriesNumericFctx {
    /// The next value to emit (advanced by `step` each producing call).
    current: Vec<u8>,
    /// The series end value.
    stop: Vec<u8>,
    /// The step (an explicit non-zero numeric, or one).
    step: Vec<u8>,
    /// The step's sign, decided once at setup (the in-range test depends on it).
    step_is_positive: bool,
}

/// Erase a `'static` cross-call state value into the `FuncCallContext.user_fctx`
/// carrier (C: `funcctx->user_fctx = palloc(...)`).
fn erase_user_fctx<'mcx, T: Any>(mcx: Mcx<'mcx>, v: T) -> PgBox<'mcx, dyn Any> {
    let boxed = mcx::alloc_in(mcx, v).expect("alloc user_fctx");
    let (ptr, alloc) = PgBox::into_raw_with_allocator(boxed);
    // SAFETY: `ptr`/`alloc` came from `into_raw_with_allocator`; the cast only
    // attaches the `dyn Any` vtable.
    unsafe { PgBox::from_raw_in(ptr as *mut dyn Any, alloc) }
}

/// Read a by-reference `numeric` argument `index` as its complete header-ful
/// on-disk varlena image (C: `PG_GETARG_NUMERIC`). The executor frame carries
/// the header-ful varlena image on the by-ref side channel, exactly the form the
/// numeric kernels' `set_var_from_num` / `numeric_is_special` read.
fn arg_numeric_image(fcinfo: &FunctionCallInfoBaseData<'_>, index: usize) -> Vec<u8> {
    match fcinfo.ref_arg(index) {
        Some(FmgrArgRef::Varlena(b)) => b.as_slice().to_vec(),
        _ => panic!("generate_series_numeric: numeric arg {index} missing from by-ref lane"),
    }
}

/// Build the numeric by-reference result `Datum` from a header-ful on-disk image
/// (C: `NumericGetDatum(make_result(&current))`). The image is the complete
/// 4-byte-header varlena, crossing verbatim on the by-ref lane (like `unnest`'s
/// by-ref element).
fn numeric_image_datum<'mcx>(mcx: Mcx<'mcx>, image: &[u8]) -> Datum<'mcx> {
    let mut buf = mcx::PgVec::new_in(mcx);
    buf.try_reserve(image.len())
        .unwrap_or_else(|_| std::panic::panic_any(mcx.oom(image.len())));
    buf.extend_from_slice(image);
    Datum::ByRef(buf)
}

/// `generate_series_step_numeric(PG_FUNCTION_ARGS)` (numeric.c:1708) over the
/// executor frame. Shared by the 2-arg (`generate_series_numeric`, step 1) and
/// 3-arg forms. Drives the value-per-call protocol directly.
fn generate_series_step_numeric<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> Datum<'mcx> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("generate_series_numeric: fn_mcxt set by the SRF caller");

    // C: if (SRF_IS_FIRSTCALL()) { funcctx = SRF_FIRSTCALL_INIT(); ... }
    if fcinfo.fn_extra.is_none() {
        // Validate + seed the cross-call state. The immutable borrow of `fcinfo`
        // (the arg images) must end before the mutable SRF setup calls, so it is
        // scoped. Hard ereports cross the dispatch boundary's catch_unwind.
        let state: SeriesNumericFctx = {
            let start = arg_numeric_image(fcinfo, 0);
            let stop = arg_numeric_image(fcinfo, 1);

            // C: reject NaN/infinity in start and stop.
            check_special(&start, "start value");
            check_special(&stop, "stop value");

            // C: steploc = const_one; if (PG_NARGS() == 3) read the explicit step.
            let (step_image, step_is_positive) = if fcinfo.nargs == 3 {
                let step = arg_numeric_image(fcinfo, 2);
                check_special(&step, "step size");
                // set_var_from_num to decide sign + the zero check.
                let stepvar = set_var_from_num(mcx, &step)
                    .unwrap_or_else(|e| std::panic::panic_any(e));
                // C: if (cmp_var(&steploc, &const_zero) == 0) error.
                if stepvar.ndigits() == 0 {
                    std::panic::panic_any(
                        PgError::error("step size cannot equal zero")
                            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
                    );
                }
                (step, stepvar.sign == NumericSign::Pos)
            } else {
                // steploc = const_one (positive); canonical image of 1.
                let one = const_one(mcx);
                let image = make_result(mcx, &one)
                    .unwrap_or_else(|e| std::panic::panic_any(e));
                (image.as_slice().to_vec(), true)
            };

            // Seed `current` with the original start value (copied via its image).
            SeriesNumericFctx {
                current: start,
                stop,
                step: step_image,
                step_is_positive,
            }
        };

        init_MultiFuncCall(fcinfo).expect("init_MultiFuncCall");
        let fctx = erase_user_fctx(mcx, state);
        let funcctx = per_MultiFuncCall(fcinfo).expect("per_MultiFuncCall");
        funcctx.user_fctx = Some(fctx);
    }

    // C: funcctx = SRF_PERCALL_SETUP(); fctx = funcctx->user_fctx;
    let funcctx = per_MultiFuncCall(fcinfo).expect("per_MultiFuncCall");
    let state: &mut SeriesNumericFctx = funcctx
        .user_fctx
        .as_mut()
        .expect("user_fctx present")
        .downcast_mut::<SeriesNumericFctx>()
        .expect("user_fctx is SeriesNumericFctx");

    // Re-materialize current/stop/step NumericVars from their canonical images.
    let current = set_var_from_num(mcx, &state.current)
        .unwrap_or_else(|e| std::panic::panic_any(e));
    let stop = set_var_from_num(mcx, &state.stop)
        .unwrap_or_else(|e| std::panic::panic_any(e));
    let step = set_var_from_num(mcx, &state.step)
        .unwrap_or_else(|e| std::panic::panic_any(e));

    // C: if ((fctx->step.sign == NUMERIC_POS && cmp_var(&fctx->current, &fctx->stop) <= 0) ||
    //        (fctx->step.sign == NUMERIC_NEG && cmp_var(&fctx->current, &fctx->stop) >= 0))
    let cmp = cmp_var(&current, &stop);
    let in_range = if state.step_is_positive {
        cmp <= core::cmp::Ordering::Equal
    } else {
        cmp >= core::cmp::Ordering::Equal
    };

    if in_range {
        // C: result = make_result(&fctx->current); SRF_RETURN_NEXT(...).
        let result_image = make_result(mcx, &current)
            .unwrap_or_else(|e| std::panic::panic_any(e));
        let datum = numeric_image_datum(mcx, result_image.as_slice());

        // C: add_var(&fctx->current, &fctx->step, &fctx->current); — advance.
        let next = add_var(mcx, &current, &step)
            .unwrap_or_else(|e| std::panic::panic_any(e));
        state.current = make_result(mcx, &next)
            .unwrap_or_else(|e| std::panic::panic_any(e))
            .as_slice()
            .to_vec();

        funcctx.call_cntr += 1;
        set_isdone(fcinfo, ExprDoneCond::ExprMultipleResult);
        fcinfo.isnull = false;
        datum
    } else {
        // SRF_RETURN_DONE(funcctx).
        end_MultiFuncCall(fcinfo).expect("end_MultiFuncCall");
        set_isdone(fcinfo, ExprDoneCond::ExprEndResult);
        fcinfo.isnull = true;
        Datum::null()
    }
}

/// C: the NaN/infinity rejection on a numeric argument. `label` is the C error
/// prefix (`"start value"` / `"stop value"` / `"step size"`).
fn check_special(image: &[u8], label: &str) {
    if numeric_is_special(image) {
        let msg = if numeric_is_nan(image) {
            alloc::format!("{label} cannot be NaN")
        } else {
            alloc::format!("{label} cannot be infinity")
        };
        std::panic::panic_any(
            PgError::error(msg).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        );
    }
}

/// `rsi->isDone = cond` (the `SRF_RETURN_NEXT`/`SRF_RETURN_DONE` write onto the
/// live `ReturnSetInfo` the executor frame carries).
fn set_isdone(fcinfo: &mut FunctionCallInfoBaseData<'_>, cond: ExprDoneCond) {
    fcinfo
        .resultinfo
        .as_mut()
        .expect("resultinfo present for an SRF call")
        .isDone = cond;
}
