//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the
//! `to_char(int4/int8/float4/float8, text)` overloads from `formatting.c`,
//! whose argument/result types are expressible at the current fmgr boundary
//! (a by-value scalar + a by-reference `text` format string -> by-reference
//! `text` result).
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, hands them to the matching [`crate::fmgr_boundary`] marshal
//! entry (which calls the ported [`crate::num_entry`] value core and re-encodes
//! the `text` result), and writes the result image back on the by-reference
//! lane. [`register_formatting_builtins`] registers every row into the
//! fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID dispatch resolves
//! them. OIDs / nargs / strict / retset are transcribed exactly from
//! `pg_proc.dat` (rows 1773-1776).
//!
//! The `numeric` overloads (`to_char(numeric, text)` 1772, `to_number` 1777)
//! are NOT registered here: their argument and/or result is `numeric`, a
//! by-reference on-disk image whose fmgr-frame encoding (`numeric` varlena ->
//! `NumericVar`) is not the by-value scalar lane these `fc_` adapters cover; the
//! `*_boundary` entries already marshal them for the executor, but the
//! registry-frame `numeric` carrier is out of scope for this row set.

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FmgrArg, FunctionCallInfoBaseData};

use mcx::Mcx;
use types_core::Oid;
use types_error::PgError;

use crate::fmgr_boundary::{
    float4_to_char_boundary, float8_to_char_boundary, int4_to_char_boundary,
    int8_to_char_boundary, interval_to_char_boundary, timestamp_to_char_boundary,
    timestamptz_to_char_boundary,
};
use types_datetime::Interval;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_INT32(i)`: the low 32 bits of arg `i`'s by-value word.
#[inline]
fn arg_int32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.arg(i).expect("to_char fn: missing arg").value.as_i32()
}

/// `PG_GETARG_INT64(i)`: arg `i`'s by-value word as a signed 64-bit int.
#[inline]
fn arg_int64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo.arg(i).expect("to_char fn: missing arg").value.as_i64()
}

/// `PG_GETARG_FLOAT4(i)`: arg `i`'s by-value word, bit-cast to `float4`.
#[inline]
fn arg_float4(fcinfo: &FunctionCallInfoBaseData, i: usize) -> f32 {
    fcinfo.arg(i).expect("to_char fn: missing arg").value.as_f32()
}

/// `PG_GETARG_FLOAT8(i)`: arg `i`'s by-value word, bit-cast to `float8`.
#[inline]
fn arg_float8(fcinfo: &FunctionCallInfoBaseData, i: usize) -> f64 {
    fcinfo.arg(i).expect("to_char fn: missing arg").value.as_f64()
}

/// `PG_GETARG_TIMESTAMP(i)` / `PG_GETARG_TIMESTAMPTZ(i)`: arg `i`'s by-value
/// word as a signed 64-bit `Timestamp` (microseconds since the PG epoch).
#[inline]
fn arg_timestamp(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo.arg(i).expect("to_char fn: missing arg").value.as_i64()
}

/// `PG_GETARG_INTERVAL_P(i)`: decode the by-reference `interval` image. The
/// boundary byte image is the C struct's little-endian field layout with no
/// alignment padding: `time:i64, day:i32, month:i32` (16 bytes), matching
/// `backend-utils-adt-datetime`'s `interval_from_bytes`.
#[inline]
fn arg_interval(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Interval {
    let payload = arg_text_payload(fcinfo, i);
    let b = match payload {
        RefPayload::Varlena(ref b) => b.clone(),
        _ => panic!("to_char fn: interval arg missing from by-ref lane"),
    };
    Interval {
        time: i64::from_le_bytes(b[0..8].try_into().expect("interval image >= 16 bytes")),
        day: i32::from_le_bytes(b[8..12].try_into().expect("interval image >= 16 bytes")),
        month: i32::from_le_bytes(b[12..16].try_into().expect("interval image >= 16 bytes")),
    }
}

/// `PG_GETARG_TEXT_PP(i)`: take an owned copy of the `text` format argument off
/// the by-ref lane, so the [`FmgrArg::Ref`] the `*_boundary` entries consume can
/// borrow this owned local (leaving `fcinfo` free for the by-ref result write).
#[inline]
fn arg_text_payload(fcinfo: &FunctionCallInfoBaseData, i: usize) -> RefPayload {
    fcinfo
        .ref_arg(i)
        .expect("to_char fn: text arg missing from by-ref lane")
        .clone()
}

/// A scratch context for the result `text` allocation (C charges it to
/// `CurrentMemoryContext`; this repo carries no ambient context).
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("to_char fmgr scratch")
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: PgError) -> ! {
    std::panic::panic_any(err);
}

/// Drive one `to_char(scalar, text)` overload: take the owned `text` format
/// payload off the by-ref lane, hand it (plus the scalar `value` and the
/// `fncollation`) to the matching `*_boundary` marshal entry, copy the produced
/// varlena image into an owned `Vec<u8>`, and write it back on the by-ref result
/// lane (`PG_RETURN_TEXT_P`). The scratch `Mcx` outlives the boundary call's
/// `PgVec`, which is flattened to an owned `Vec` before the context drops.
#[inline]
fn run<F>(fcinfo: &mut FunctionCallInfoBaseData, boundary: F) -> Datum
where
    F: for<'mcx> FnOnce(
        Mcx<'mcx>,
        &FmgrArg<'_, '_>,
        Oid,
    ) -> Result<mcx::PgVec<'mcx, u8>, PgError>,
{
    let collid = fcinfo.fncollation;
    let fmt_payload = arg_text_payload(fcinfo, 1);
    let m = scratch_mcx();
    let bytes: Vec<u8> = {
        let fmt = FmgrArg::Ref(&fmt_payload);
        match boundary(m.mcx(), &fmt, collid) {
            Ok(image) => image.as_slice().to_vec(),
            Err(e) => raise(e),
        }
    };
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::from_usize(0)
}

/// Like [`run`], but the boundary entry may return SQL NULL (`None`, the C
/// `PG_RETURN_NULL()` arm for an empty format or a non-finite datetime input).
/// On `None` the call frame's `isnull` flag is set and a 0 `Datum` returned.
#[inline]
fn run_opt<F>(fcinfo: &mut FunctionCallInfoBaseData, boundary: F) -> Datum
where
    F: for<'mcx> FnOnce(
        Mcx<'mcx>,
        &FmgrArg<'_, '_>,
        Oid,
    ) -> Result<Option<mcx::PgVec<'mcx, u8>>, PgError>,
{
    let collid = fcinfo.fncollation;
    let fmt_payload = arg_text_payload(fcinfo, 1);
    let m = scratch_mcx();
    let result: Option<Vec<u8>> = {
        let fmt = FmgrArg::Ref(&fmt_payload);
        match boundary(m.mcx(), &fmt, collid) {
            Ok(Some(image)) => Some(image.as_slice().to_vec()),
            Ok(None) => None,
            Err(e) => raise(e),
        }
    };
    match result {
        Some(bytes) => {
            fcinfo.set_ref_result(RefPayload::Varlena(bytes));
            Datum::from_usize(0)
        }
        None => {
            fcinfo.isnull = true;
            Datum::from_usize(0)
        }
    }
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

fn fc_int4_to_char(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let value = arg_int32(fcinfo, 0);
    run(fcinfo, |mcx, fmt, collid| {
        int4_to_char_boundary(mcx, value, fmt, collid)
    })
}

fn fc_int8_to_char(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let value = arg_int64(fcinfo, 0);
    run(fcinfo, |mcx, fmt, collid| {
        int8_to_char_boundary(mcx, value, fmt, collid)
    })
}

fn fc_float4_to_char(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let value = arg_float4(fcinfo, 0);
    run(fcinfo, |mcx, fmt, collid| {
        float4_to_char_boundary(mcx, value, fmt, collid)
    })
}

fn fc_float8_to_char(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let value = arg_float8(fcinfo, 0);
    run(fcinfo, |mcx, fmt, collid| {
        float8_to_char_boundary(mcx, value, fmt, collid)
    })
}

fn fc_timestamp_to_char(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let dt = arg_timestamp(fcinfo, 0);
    run_opt(fcinfo, move |mcx, fmt, collid| {
        timestamp_to_char_boundary(mcx, dt, fmt, collid)
    })
}

fn fc_timestamptz_to_char(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let dt = arg_timestamp(fcinfo, 0);
    run_opt(fcinfo, move |mcx, fmt, collid| {
        timestamptz_to_char_boundary(mcx, dt, fmt, collid)
    })
}

fn fc_interval_to_char(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let it = arg_interval(fcinfo, 0);
    run_opt(fcinfo, move |mcx, fmt, collid| {
        interval_to_char_boundary(mcx, &it, fmt, collid)
    })
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    strict: bool,
    retset: bool,
    func: fn(&mut FunctionCallInfoBaseData) -> Datum,
) -> BuiltinFunction {
    BuiltinFunction {
        foid,
        name: name.to_string(),
        nargs,
        strict,
        retset,
        func: Some(func),
    }
}

/// Register the `to_char(scalar, text)` `formatting.c` builtins (their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
/// OIDs / nargs / strict / retset transcribed from `pg_proc.dat` rows
/// 1773-1776 (all `proisstrict` is the catalog default `'t'`; none is retset;
/// each takes 2 args).
pub fn register_formatting_builtins() {
    backend_utils_fmgr_core::register_builtins([
        builtin(1773, "int4_to_char", 2, true, false, fc_int4_to_char),
        builtin(1774, "int8_to_char", 2, true, false, fc_int8_to_char),
        builtin(1775, "float4_to_char", 2, true, false, fc_float4_to_char),
        builtin(1776, "float8_to_char", 2, true, false, fc_float8_to_char),
        builtin(1768, "interval_to_char", 2, true, false, fc_interval_to_char),
        builtin(1770, "timestamptz_to_char", 2, true, false, fc_timestamptz_to_char),
        builtin(2049, "timestamp_to_char", 2, true, false, fc_timestamp_to_char),
    ]);
}
