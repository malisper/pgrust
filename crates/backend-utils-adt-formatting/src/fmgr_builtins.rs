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
//! are registered via [`fc_numeric_to_char`] / [`fc_numeric_to_number`], which
//! read the by-reference `numeric`/`text` images off the by-ref lane and hand
//! them to the `*_boundary` marshal entries (whose `numeric` varlena ->
//! `NumericVar` decode is the same one the executor uses).

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FmgrArg, FunctionCallInfoBaseData, PgFnNative};

use mcx::Mcx;
use types_core::Oid;
use types_error::{PgError, PgResult};

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

/// Drive one `to_char(scalar, text)` overload: take the owned `text` format
/// payload off the by-ref lane, hand it (plus the scalar `value` and the
/// `fncollation`) to the matching `*_boundary` marshal entry, copy the produced
/// varlena image into an owned `Vec<u8>`, and write it back on the by-ref result
/// lane (`PG_RETURN_TEXT_P`). The scratch `Mcx` outlives the boundary call's
/// `PgVec`, which is flattened to an owned `Vec` before the context drops.
#[inline]
fn run<F>(fcinfo: &mut FunctionCallInfoBaseData, boundary: F) -> PgResult<Datum>
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
        boundary(m.mcx(), &fmt, collid)?.as_slice().to_vec()
    };
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Ok(Datum::from_usize(0))
}

/// Like [`run`], but the boundary entry may return SQL NULL (`None`, the C
/// `PG_RETURN_NULL()` arm for an empty format or a non-finite datetime input).
/// On `None` the call frame's `isnull` flag is set and a 0 `Datum` returned.
#[inline]
fn run_opt<F>(fcinfo: &mut FunctionCallInfoBaseData, boundary: F) -> PgResult<Datum>
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
        boundary(m.mcx(), &fmt, collid)?.map(|image| image.as_slice().to_vec())
    };
    match result {
        Some(bytes) => {
            fcinfo.set_ref_result(RefPayload::Varlena(bytes));
            Ok(Datum::from_usize(0))
        }
        None => {
            fcinfo.isnull = true;
            Ok(Datum::from_usize(0))
        }
    }
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

fn fc_int4_to_char(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let value = arg_int32(fcinfo, 0);
    run(fcinfo, |mcx, fmt, collid| {
        int4_to_char_boundary(mcx, value, fmt, collid)
    })
}

fn fc_int8_to_char(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let value = arg_int64(fcinfo, 0);
    run(fcinfo, |mcx, fmt, collid| {
        int8_to_char_boundary(mcx, value, fmt, collid)
    })
}

fn fc_float4_to_char(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let value = arg_float4(fcinfo, 0);
    run(fcinfo, |mcx, fmt, collid| {
        float4_to_char_boundary(mcx, value, fmt, collid)
    })
}

fn fc_float8_to_char(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let value = arg_float8(fcinfo, 0);
    run(fcinfo, |mcx, fmt, collid| {
        float8_to_char_boundary(mcx, value, fmt, collid)
    })
}

fn fc_timestamp_to_char(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let dt = arg_timestamp(fcinfo, 0);
    run_opt(fcinfo, move |mcx, fmt, collid| {
        timestamp_to_char_boundary(mcx, dt, fmt, collid)
    })
}

fn fc_timestamptz_to_char(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let dt = arg_timestamp(fcinfo, 0);
    run_opt(fcinfo, move |mcx, fmt, collid| {
        timestamptz_to_char_boundary(mcx, dt, fmt, collid)
    })
}

fn fc_interval_to_char(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let it = arg_interval(fcinfo, 0);
    run_opt(fcinfo, move |mcx, fmt, collid| {
        interval_to_char_boundary(mcx, &it, fmt, collid)
    })
}

/// `VARDATA_ANY` of a by-reference `text` argument: the payload bytes after the
/// 4-byte uncompressed length header (`RefPayload::Cstring` is verbatim).
#[inline]
fn arg_text_body(fcinfo: &FunctionCallInfoBaseData, i: usize) -> PgResult<Vec<u8>> {
    let payload = fcinfo
        .ref_arg(i)
        .expect("to_date/to_timestamp fn: text arg missing from by-ref lane");
    Ok(match payload {
        RefPayload::Varlena(b) => {
            let img = b.as_slice();
            if img.len() >= 4 {
                img[4..].to_vec()
            } else {
                Vec::new()
            }
        }
        RefPayload::Cstring(s) => s.as_bytes().to_vec(),
        _ => {
            return Err(PgError::error(
                "to_date/to_timestamp fmgr arg: expected a by-reference text varlena",
            ))
        }
    })
}

/// `to_date(text, text) -> date` (formatting.c). Both args are by-reference
/// `text`; the `date` result is the by-value `DateADT` (int32).
fn fc_to_date(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let date_txt = arg_text_body(fcinfo, 0)?;
    let fmt = arg_text_body(fcinfo, 1)?;
    let collid = fcinfo.fncollation;
    let m = scratch_mcx();
    Ok(Datum::from_i32(crate::to_date(m.mcx(), &date_txt, &fmt, collid)?))
}

/// `to_timestamp(text, text) -> timestamptz` (formatting.c). Both args are
/// by-reference `text`; the `timestamptz` result is the by-value `Timestamp`
/// (int64).
fn fc_to_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let date_txt = arg_text_body(fcinfo, 0)?;
    let fmt = arg_text_body(fcinfo, 1)?;
    let collid = fcinfo.fncollation;
    let m = scratch_mcx();
    Ok(Datum::from_i64(
        crate::to_timestamp(m.mcx(), &date_txt, &fmt, collid)?.timestamp,
    ))
}

/// `numeric_to_char(numeric, text) -> text` (oid 1772). Arg 0 is the by-ref
/// `numeric` image, arg 1 the by-ref `text` format; the `text` result goes back
/// on the by-ref lane.
fn fc_numeric_to_char(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let collid = fcinfo.fncollation;
    let num_payload = arg_text_payload(fcinfo, 0);
    let fmt_payload = arg_text_payload(fcinfo, 1);
    let m = scratch_mcx();
    let bytes: Vec<u8> = {
        let num = FmgrArg::Ref(&num_payload);
        let fmt = FmgrArg::Ref(&fmt_payload);
        crate::fmgr_boundary::numeric_to_char_boundary(m.mcx(), &num, &fmt, collid)?
            .as_slice()
            .to_vec()
    };
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Ok(Datum::from_usize(0))
}

/// `to_number(text, text) -> numeric` (oid 1777). Both args are by-ref `text`;
/// the `numeric` result goes back on the by-ref lane, or SQL NULL for the C
/// `PG_RETURN_NULL()` empty/oversized-format arm.
fn fc_numeric_to_number(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let collid = fcinfo.fncollation;
    let value_payload = arg_text_payload(fcinfo, 0);
    let fmt_payload = arg_text_payload(fcinfo, 1);
    let m = scratch_mcx();
    let result: Option<Vec<u8>> = {
        let value = FmgrArg::Ref(&value_payload);
        let fmt = FmgrArg::Ref(&fmt_payload);
        crate::fmgr_boundary::numeric_to_number_boundary(m.mcx(), &value, &fmt, collid)?
            .map(|image| image.as_slice().to_vec())
    };
    match result {
        Some(bytes) => {
            fcinfo.set_ref_result(RefPayload::Varlena(bytes));
            Ok(Datum::from_usize(0))
        }
        None => {
            fcinfo.isnull = true;
            Ok(Datum::from_usize(0))
        }
    }
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
    native: PgFnNative,
) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.to_string(),
            nargs,
            strict,
            retset,
            func: None,
        },
        native,
    )
}

/// Register the `to_char(scalar, text)` `formatting.c` builtins (their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
/// OIDs / nargs / strict / retset transcribed from `pg_proc.dat` rows
/// 1773-1776 (all `proisstrict` is the catalog default `'t'`; none is retset;
/// each takes 2 args).
pub fn register_formatting_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        builtin(1773, "int4_to_char", 2, true, false, fc_int4_to_char),
        builtin(1774, "int8_to_char", 2, true, false, fc_int8_to_char),
        builtin(1775, "float4_to_char", 2, true, false, fc_float4_to_char),
        builtin(1776, "float8_to_char", 2, true, false, fc_float8_to_char),
        builtin(1768, "interval_to_char", 2, true, false, fc_interval_to_char),
        builtin(1770, "timestamptz_to_char", 2, true, false, fc_timestamptz_to_char),
        builtin(2049, "timestamp_to_char", 2, true, false, fc_timestamp_to_char),
        builtin(1780, "to_date", 2, true, false, fc_to_date),
        builtin(1778, "to_timestamp", 2, true, false, fc_to_timestamp),
        builtin(1772, "numeric_to_char", 2, true, false, fc_numeric_to_char),
        builtin(1777, "numeric_to_number", 2, true, false, fc_numeric_to_number),
    ]);
}
