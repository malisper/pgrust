//! `generate_series(int4/int8)` registered as executor-frame set-returning
//! functions.
//!
//! `int.c`'s `generate_series_int4` / `generate_series_step_int4` and `int8.c`'s
//! int8 counterparts (`pg_proc` OIDs 1066/1067 int4, 1068/1069 int8) are
//! value-per-call SRFs: their value core (the `generate_series_fctx` cross-call
//! state + the per-call step, including the overflow-stopping final value) is
//! ported in `backend-utils-adt-int::series` / `backend-utils-adt-int8`, and the
//! `SRF_FIRSTCALL_INIT` / `SRF_PERCALL_SETUP` / `SRF_RETURN_NEXT` /
//! `SRF_RETURN_DONE` glue is `backend-utils-fmgr-funcapi::srf_support`.
//!
//! Here those pieces are assembled into [`types_nodes::execexpr::PGFunction`]s
//! (the executor-frame ABI whose call frame carries the LIVE `ReturnSetInfo`)
//! and registered in this unit's executor-frame SRF table from
//! [`register_generate_series`] (called by `init_seams`). This is the
//! executor-frame `fmgrtab.c` analogue for these SRFs: it bypasses the by-OID
//! builtin registry (whose `types_fmgr::PGFunction` frame's `resultinfo` is
//! tag-only — the WONTFIX dual-home) so the function can read/write a live
//! `ReturnSetInfo`. `SELECT * FROM generate_series(1,3)` reaches this via
//! nodeFunctionscan → ExecMakeTableFunctionResult → here.

use core::any::Any;

use backend_utils_adt_int::series::GenerateSeriesInt4;
use backend_utils_fmgr_funcapi::srf_support::{
    end_MultiFuncCall, init_MultiFuncCall, per_MultiFuncCall,
};
use mcx::{Mcx, PgBox};
use types_core::Oid;
use types_nodes::execexpr::ExprDoneCond;
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_tuple::backend_access_common_heaptuple::Datum;

use crate::register_srf;

/// `generate_series_step_int4` (OID 1066) and `generate_series_int4` (OID 1067)
/// share this value core; the 2-arg form defaults step to 1.
const GENERATE_SERIES_INT4_STEP: Oid = 1066;
const GENERATE_SERIES_INT4: Oid = 1067;
/// `generate_series_step_int8` (OID 1068) and `generate_series_int8` (OID 1069).
const GENERATE_SERIES_INT8_STEP: Oid = 1068;
const GENERATE_SERIES_INT8: Oid = 1069;

/// Register the int4/int8 `generate_series` SRFs in the executor-frame table.
pub(crate) fn register_generate_series() {
    register_srf(GENERATE_SERIES_INT4_STEP, generate_series_step_int4);
    register_srf(GENERATE_SERIES_INT4, generate_series_step_int4);
    register_srf(GENERATE_SERIES_INT8_STEP, generate_series_step_int8);
    register_srf(GENERATE_SERIES_INT8, generate_series_step_int8);
}

/// Erase a `'static` cross-call state value into the
/// `FuncCallContext.user_fctx` carrier (C: `funcctx->user_fctx = palloc(...)`).
fn erase_user_fctx<'mcx, T: Any>(mcx: Mcx<'mcx>, v: T) -> PgBox<'mcx, dyn Any> {
    let boxed = mcx::alloc_in(mcx, v).expect("alloc user_fctx");
    let (ptr, alloc) = PgBox::into_raw_with_allocator(boxed);
    // SAFETY: `ptr`/`alloc` came from `into_raw_with_allocator`; the cast only
    // attaches the `dyn Any` vtable.
    unsafe { PgBox::from_raw_in(ptr as *mut dyn Any, alloc) }
}

/// `generate_series_step_int4(PG_FUNCTION_ARGS)` (int.c:1537) over the executor
/// frame. Drives the value-per-call protocol; `SRF_RETURN_NEXT` /
/// `SRF_RETURN_DONE` are the `isDone` writes + the multi-call teardown.
fn generate_series_step_int4<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> Datum<'mcx> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("generate_series_int4: fn_mcxt set by the SRF caller");

    // C: if (SRF_IS_FIRSTCALL()) { funcctx = SRF_FIRSTCALL_INIT(); ... }
    if fcinfo.fn_extra.is_none() {
        let start = fcinfo.args[0].value.as_i32();
        let finish = fcinfo.args[1].value.as_i32();
        // C: step = 1; if (PG_NARGS() == 3) step = PG_GETARG_INT32(2);
        let step = if fcinfo.nargs == 3 {
            fcinfo.args[2].value.as_i32()
        } else {
            1
        };
        // C: GenerateSeriesInt4::new validates step != 0 (ereport on zero).
        // Raise the hard ereport through the PGFunction dispatch boundary
        // (`invoke_pgfunction` catch_unwind) so the structured `PgError` —
        // sqlstate + "step size cannot equal zero" — reaches the client, rather
        // than `.expect()` panicking with a Debug-formatted string.
        let state = match GenerateSeriesInt4::new(start, finish, step) {
            Ok(state) => state,
            Err(e) => std::panic::panic_any(e),
        };
        init_MultiFuncCall(fcinfo).expect("init_MultiFuncCall");
        let fctx = erase_user_fctx(mcx, state);
        let funcctx = per_MultiFuncCall(fcinfo).expect("per_MultiFuncCall");
        funcctx.user_fctx = Some(fctx);
    }

    // C: funcctx = SRF_PERCALL_SETUP(); fctx = funcctx->user_fctx;
    let funcctx = per_MultiFuncCall(fcinfo).expect("per_MultiFuncCall");
    let state: &mut GenerateSeriesInt4 = funcctx
        .user_fctx
        .as_mut()
        .expect("user_fctx present")
        .downcast_mut::<GenerateSeriesInt4>()
        .expect("user_fctx is GenerateSeriesInt4");

    match state.next() {
        Some(result) => {
            // SRF_RETURN_NEXT(funcctx, Int32GetDatum(result)).
            funcctx.call_cntr += 1;
            set_isdone(fcinfo, ExprDoneCond::ExprMultipleResult);
            fcinfo.isnull = false;
            Datum::from_i32(result)
        }
        None => {
            // SRF_RETURN_DONE(funcctx).
            end_MultiFuncCall(fcinfo).expect("end_MultiFuncCall");
            set_isdone(fcinfo, ExprDoneCond::ExprEndResult);
            fcinfo.isnull = true;
            Datum::from_i32(0)
        }
    }
}

/// int8 cross-call state mirroring `generate_series_fctx` for the 64-bit core.
#[derive(Debug)]
struct GenerateSeriesInt8 {
    current: i64,
    finish: i64,
    step: i64,
}

/// `generate_series_step_int8(PG_FUNCTION_ARGS)` (int8.c) over the executor frame.
fn generate_series_step_int8<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> Datum<'mcx> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("generate_series_int8: fn_mcxt set by the SRF caller");

    if fcinfo.fn_extra.is_none() {
        let start = fcinfo.args[0].value.as_i64();
        let finish = fcinfo.args[1].value.as_i64();
        let step = if fcinfo.nargs == 3 {
            fcinfo.args[2].value.as_i64()
        } else {
            1
        };
        // C: if (step == 0) ereport(ERROR, "step size cannot equal zero").
        // This is a hard ereport from a bare-Datum PGFunction frame; raise it
        // through the one dispatch point every PGFunction crosses
        // (`invoke_pgfunction`'s `catch_unwind`), which downcasts the structured
        // `PgError` back into a proper ereport — exactly as `pg_input_error_info`
        // and the fmgr-builtin adapters do. Using `.expect()` here would instead
        // panic with a Debug-formatted string, losing the sqlstate/message and
        // surfacing the raw `PgError { .. }` dump to the client.
        if let Err(e) = backend_utils_adt_int8::generate_series_int8_check_step(step) {
            std::panic::panic_any(e);
        }
        init_MultiFuncCall(fcinfo).expect("init_MultiFuncCall");
        let fctx = erase_user_fctx(
            mcx,
            GenerateSeriesInt8 {
                current: start,
                finish,
                step,
            },
        );
        let funcctx = per_MultiFuncCall(fcinfo).expect("per_MultiFuncCall");
        funcctx.user_fctx = Some(fctx);
    }

    let funcctx = per_MultiFuncCall(fcinfo).expect("per_MultiFuncCall");
    let state: &mut GenerateSeriesInt8 = funcctx
        .user_fctx
        .as_mut()
        .expect("user_fctx present")
        .downcast_mut::<GenerateSeriesInt8>()
        .expect("user_fctx is GenerateSeriesInt8");

    let result = state.current;
    match backend_utils_adt_int8::generate_series_int8_step(state.current, state.finish, state.step)
    {
        Some(next) => {
            // Producing call: advance (or zero the step on overflow → final value).
            match next {
                Some(n) => state.current = n,
                None => state.step = 0,
            }
            funcctx.call_cntr += 1;
            set_isdone(fcinfo, ExprDoneCond::ExprMultipleResult);
            fcinfo.isnull = false;
            Datum::from_i64(result)
        }
        None => {
            end_MultiFuncCall(fcinfo).expect("end_MultiFuncCall");
            set_isdone(fcinfo, ExprDoneCond::ExprEndResult);
            fcinfo.isnull = true;
            Datum::from_i64(0)
        }
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
