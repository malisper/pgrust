//! `generate_series(int4/int8)` registered as executor-frame set-returning
//! functions.
//!
//! `int.c`'s `generate_series_int4` / `generate_series_step_int4` and `int8.c`'s
//! int8 counterparts (`pg_proc` OIDs 1066/1067 int4, 1068/1069 int8) are
//! value-per-call SRFs: their value core (the `generate_series_fctx` cross-call
//! state + the per-call step, including the overflow-stopping final value) is
//! ported in `backend-utils-adt-::int::series` / `backend-utils-adt-int8`, and the
//! `SRF_FIRSTCALL_INIT` / `SRF_PERCALL_SETUP` / `SRF_RETURN_NEXT` /
//! `SRF_RETURN_DONE` glue is `backend-utils-fmgr-::funcapi::srf_support`.
//!
//! Here those pieces are assembled into [`::nodes::execexpr::PGFunction`]s
//! (the executor-frame ABI whose call frame carries the LIVE `ReturnSetInfo`)
//! and registered in this unit's executor-frame SRF table from
//! [`register_generate_series`] (called by `init_seams`). This is the
//! executor-frame `fmgrtab.c` analogue for these SRFs: it bypasses the by-OID
//! builtin registry (whose `fmgr::PGFunction` frame's `resultinfo` is
//! tag-only — the WONTFIX dual-home) so the function can read/write a live
//! `ReturnSetInfo`. `SELECT * FROM generate_series(1,3)` reaches this via
//! nodeFunctionscan → ExecMakeTableFunctionResult → here.

use core::any::Any;

use ::int::series::GenerateSeriesInt4;
use ::funcapi::srf_support::{
    end_MultiFuncCall, init_MultiFuncCall, per_MultiFuncCall,
};
use mcx::{Mcx, PgBox};
use ::types_core::Oid;
use ::types_datetime::Interval;
use ::types_error::error::ERRCODE_INVALID_PARAMETER_VALUE;
use types_error::{PgError, PgResult};
use ::nodes::execexpr::ExprDoneCond;
use ::nodes::fmgr::{FmgrArgRef, FunctionCallInfoBaseData};
use types_tuple::heaptuple::Datum;

use crate::register_srf;

/// `generate_series_step_int4` (OID 1066) and `generate_series_int4` (OID 1067)
/// share this value core; the 2-arg form defaults step to 1.
const GENERATE_SERIES_INT4_STEP: Oid = 1066;
const GENERATE_SERIES_INT4: Oid = 1067;
/// `generate_series_step_int8` (OID 1068) and `generate_series_int8` (OID 1069).
const GENERATE_SERIES_INT8_STEP: Oid = 1068;
const GENERATE_SERIES_INT8: Oid = 1069;
/// `generate_series_timestamp(timestamp, timestamp, interval)` (OID 938).
const GENERATE_SERIES_TIMESTAMP: Oid = 938;
/// `generate_series_timestamptz(timestamptz, timestamptz, interval)` (OID 939).
const GENERATE_SERIES_TIMESTAMPTZ: Oid = 939;
/// `generate_series_timestamptz_at_zone(timestamptz, timestamptz, interval,
/// text)` (OID 6274) — the 4-arg form doing arithmetic in a named zone.
const GENERATE_SERIES_TIMESTAMPTZ_AT_ZONE: Oid = 6274;

/// Register the int4/int8/timestamp `generate_series` SRFs in the executor-frame
/// table.
pub(crate) fn register_generate_series() {
    register_srf(GENERATE_SERIES_INT4_STEP, generate_series_step_int4);
    register_srf(GENERATE_SERIES_INT4, generate_series_step_int4);
    register_srf(GENERATE_SERIES_INT8_STEP, generate_series_step_int8);
    register_srf(GENERATE_SERIES_INT8, generate_series_step_int8);
    register_srf(GENERATE_SERIES_TIMESTAMP, generate_series_timestamp);
    register_srf(GENERATE_SERIES_TIMESTAMPTZ, generate_series_timestamptz);
    register_srf(
        GENERATE_SERIES_TIMESTAMPTZ_AT_ZONE,
        generate_series_timestamptz,
    );
}

/// Erase a `'static` cross-call state value into the
/// `FuncCallContext.user_fctx` carrier (C: `funcctx->user_fctx = palloc(...)`).
fn erase_user_fctx<'mcx, T: Any>(mcx: Mcx<'mcx>, v: T) -> PgBox<'mcx, dyn Any> {
    let boxed = ::mcx::alloc_in(mcx, v).expect("alloc user_fctx");
    let (ptr, alloc) = PgBox::into_raw_with_allocator(boxed);
    // SAFETY: `ptr`/`alloc` came from `into_raw_with_allocator`; the cast only
    // attaches the `dyn Any` vtable.
    unsafe { PgBox::from_raw_in(ptr as *mut dyn Any, alloc) }
}

/// `generate_series_step_int4(PG_FUNCTION_ARGS)` (int.c:1537) over the executor
/// frame. Drives the value-per-call protocol; `SRF_RETURN_NEXT` /
/// `SRF_RETURN_DONE` are the `isDone` writes + the multi-call teardown.
fn generate_series_step_int4<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
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
        // The structured `PgError` — sqlstate + "step size cannot equal zero" —
        // propagates as the Result error to the SRF caller.
        let state = GenerateSeriesInt4::new(start, finish, step)?;
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
            Ok(Datum::from_i32(result))
        }
        None => {
            // SRF_RETURN_DONE(funcctx).
            end_MultiFuncCall(fcinfo).expect("end_MultiFuncCall");
            set_isdone(fcinfo, ExprDoneCond::ExprEndResult);
            fcinfo.isnull = true;
            Ok(Datum::from_i32(0))
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
fn generate_series_step_int8<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
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
        // The structured `PgError` propagates as the Result error to the SRF
        // caller, carrying the sqlstate/message.
        int8::generate_series_int8_check_step(step)?;
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
    match int8::generate_series_int8_step(state.current, state.finish, state.step)
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
            Ok(Datum::from_i64(result))
        }
        None => {
            end_MultiFuncCall(fcinfo).expect("end_MultiFuncCall");
            set_isdone(fcinfo, ExprDoneCond::ExprEndResult);
            fcinfo.isnull = true;
            Ok(Datum::from_i64(0))
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

// ---------------------------------------------------------------------------
// generate_series(timestamp/timestamptz) — timestamp.c
// ---------------------------------------------------------------------------

/// Decode the by-reference `interval` argument `index` (C: `PG_GETARG_INTERVAL_P`).
/// The boundary carries the 16-byte `Interval` POD image
/// (`time:i64, day:i32, month:i32`, little-endian, no alignment padding) on the
/// by-ref side channel, exactly as the fmgr-builtin adapters marshal it.
fn arg_interval(fcinfo: &FunctionCallInfoBaseData<'_>, index: usize) -> Interval {
    let image = match fcinfo.ref_arg(index) {
        Some(FmgrArgRef::Varlena(b)) => b.as_slice(),
        _ => panic!("generate_series_timestamp: interval arg {index} missing from by-ref lane"),
    };
    Interval {
        time: i64::from_le_bytes(image[0..8].try_into().expect("interval image >= 16 bytes")),
        day: i32::from_le_bytes(image[8..12].try_into().expect("interval image >= 16 bytes")),
        month: i32::from_le_bytes(image[12..16].try_into().expect("interval image >= 16 bytes")),
    }
}

/// Decode the by-reference `text` argument `index` into an owned `String`
/// (C: `PG_GETARG_TEXT_PP` + `text_to_cstring`). The boundary carries the
/// header-ful varlena image; skip the 4-byte length word to reach `VARDATA_ANY`.
fn arg_text(fcinfo: &FunctionCallInfoBaseData<'_>, index: usize) -> String {
    let image = match fcinfo.ref_arg(index) {
        Some(FmgrArgRef::Varlena(b)) => b.as_slice(),
        _ => panic!("generate_series_timestamp: text arg {index} missing from by-ref lane"),
    };
    let payload = if image.len() >= 4 { &image[4..] } else { image };
    core::str::from_utf8(payload)
        .expect("generate_series_timestamp: invalid UTF-8 text arg")
        .to_owned()
}

/// Validate a `generate_series` step interval (C: the `interval_sign(&step) == 0`
/// and `INTERVAL_NOT_FINITE(&step)` guards in `generate_series_timestamp`),
/// raising the structured `PgError` through the dispatch boundary's
/// `catch_unwind` (as the int4/int8 cores do for their zero-step error).
fn check_series_step(step: &Interval) -> PgResult<i32> {
    let sign = adt_datetime::interval_sign(step);
    if sign == 0 {
        return Err(PgError::error("step size cannot equal zero")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }
    if adt_datetime::INTERVAL_NOT_FINITE(step) {
        return Err(PgError::error("step size cannot be infinite")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }
    Ok(sign)
}

/// Cross-call state for `generate_series_timestamp` /
/// `generate_series_timestamptz_internal` (C: `generate_series_timestamp_fctx`).
/// `attimezone` carries the optional named zone for the at-zone variant; `None`
/// is the session zone (`PG_NARGS() != 4`, where C stores `session_timezone`).
#[derive(Debug)]
struct GenerateSeriesTimestamp {
    current: i64,
    finish: i64,
    step: Interval,
    step_sign: i32,
    attimezone: Option<String>,
}

/// `generate_series_timestamp(PG_FUNCTION_ARGS)` (timestamp.c:6668) over the
/// executor frame. `current`/`finish` are `Timestamp` (local time); the step is
/// added with `timestamp_pl_interval`.
fn generate_series_timestamp<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    series_timestamp(fcinfo, false)
}

/// `generate_series_timestamptz_internal(fcinfo)` (timestamp.c:6752) over the
/// executor frame. Shared by `generate_series_timestamptz` (OID 939) and
/// `generate_series_timestamptz_at_zone` (OID 6274); the 4-arg form decodes a
/// named zone, the 3-arg form uses the session zone. Arithmetic uses
/// `timestamptz_pl_interval{,_at_zone}` (TZ-aware addition).
fn generate_series_timestamptz<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    series_timestamp(fcinfo, true)
}

/// Shared driver for the timestamp / timestamptz `generate_series`. `tz` selects
/// the TZ-aware (`timestamptz`) arithmetic; otherwise local `timestamp`
/// arithmetic.
fn series_timestamp<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
    tz: bool,
) -> PgResult<Datum<'mcx>> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("generate_series_timestamp: fn_mcxt set by the SRF caller");

    // C: if (SRF_IS_FIRSTCALL()) { ... }
    if fcinfo.fn_extra.is_none() {
        let start = fcinfo.args[0].value.as_i64();
        let finish = fcinfo.args[1].value.as_i64();
        let step = arg_interval(fcinfo, 2);
        // C: zone = (PG_NARGS() == 4) ? PG_GETARG_TEXT_PP(3) : NULL;
        let attimezone = if fcinfo.nargs == 4 {
            Some(arg_text(fcinfo, 3))
        } else {
            None
        };
        let step_sign = check_series_step(&step)?;

        init_MultiFuncCall(fcinfo).expect("init_MultiFuncCall");
        let fctx = erase_user_fctx(
            mcx,
            GenerateSeriesTimestamp {
                current: start,
                finish,
                step,
                step_sign,
                attimezone,
            },
        );
        let funcctx = per_MultiFuncCall(fcinfo).expect("per_MultiFuncCall");
        funcctx.user_fctx = Some(fctx);
    }

    let funcctx = per_MultiFuncCall(fcinfo).expect("per_MultiFuncCall");
    let state: &mut GenerateSeriesTimestamp = funcctx
        .user_fctx
        .as_mut()
        .expect("user_fctx present")
        .downcast_mut::<GenerateSeriesTimestamp>()
        .expect("user_fctx is GenerateSeriesTimestamp");

    let result = state.current;
    let cmp = adt_datetime::timestamp_cmp_internal(result, state.finish);
    let in_range = if state.step_sign > 0 { cmp <= 0 } else { cmp >= 0 };

    if in_range {
        // C: fctx->current = ... timestamp(tz)_pl_interval(current, &step) ...
        let next = if tz {
            match &state.attimezone {
                Some(zone) => adt_datetime::timestamptz_pl_interval_at_zone(
                    state.current,
                    &state.step,
                    zone,
                ),
                None => adt_datetime::timestamptz_pl_interval(
                    state.current,
                    &state.step,
                ),
            }
        } else {
            adt_datetime::timestamp_pl_interval(state.current, &state.step)
        };
        state.current = next?;
        funcctx.call_cntr += 1;
        set_isdone(fcinfo, ExprDoneCond::ExprMultipleResult);
        fcinfo.isnull = false;
        Ok(Datum::from_i64(result))
    } else {
        end_MultiFuncCall(fcinfo).expect("end_MultiFuncCall");
        set_isdone(fcinfo, ExprDoneCond::ExprEndResult);
        fcinfo.isnull = true;
        Ok(Datum::from_i64(0))
    }
}
