//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! functions in `cash.c` — the `money` (`Cash` = `int64`, pass-by-value) type's
//! I/O, comparison, arithmetic, the cross-type `mul`/`div` operators against
//! `float8`/`float4`/`int8`/`int4`/`int2`, `cashlarger`/`cashsmaller`,
//! `cash_words`, and the `numeric`/`int4`/`int8` casts.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core in this crate, and writes back the
//! result word / by-reference payload. [`register_cash_builtins`] registers
//! every row into the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID
//! dispatch resolves them. OIDs / nargs / strict / retset are transcribed from
//! `pg_proc.dat` (all rows here are `proisstrict => 't'` and not `proretset`).

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

// ---------------------------------------------------------------------------
// Argument readers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_CASH(i)`: the `money` word reinterpreted as `int64`.
#[inline]
fn arg_cash(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo.arg(i).expect("cash fn: missing arg").value.as_i64()
}
#[inline]
fn arg_f64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> f64 {
    fcinfo.arg(i).expect("cash fn: missing arg").value.as_f64()
}
#[inline]
fn arg_f32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> f32 {
    fcinfo.arg(i).expect("cash fn: missing arg").value.as_f32()
}
#[inline]
fn arg_i64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo.arg(i).expect("cash fn: missing arg").value.as_i64()
}
#[inline]
fn arg_i32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.arg(i).expect("cash fn: missing arg").value.as_i32()
}
#[inline]
fn arg_i16(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i16 {
    fcinfo.arg(i).expect("cash fn: missing arg").value.as_i16()
}
/// `PG_GETARG_CSTRING(i)`: the input text on the by-ref lane.
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("cash fn: cstring arg missing from by-ref lane")
}
/// `PG_GETARG_POINTER(i)` as a wire/varlena buffer: the `recv` byte image / a
/// `numeric` varlena image on the by-ref lane (full image, header included).
#[inline]
fn arg_varlena<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("cash fn: by-ref buffer arg missing from by-ref lane")
}

// ---------------------------------------------------------------------------
// Result writers.
// ---------------------------------------------------------------------------

#[inline]
fn ret_cash(v: i64) -> Datum {
    Datum::from_i64(v)
}
#[inline]
fn ret_f64(v: f64) -> Datum {
    Datum::from_f64(v)
}
#[inline]
fn ret_i32(v: i32) -> Datum {
    Datum::from_i32(v)
}
#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}
/// Set a `cstring` (`_out`) result on the by-ref lane and return the dummy word.
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}
/// Set a `bytea` (`_send`) / `numeric` (`cash_numeric`) varlena image result on
/// the by-ref lane. The bytes are the full varlena image including any header
/// the core already wrote (numeric carries one; the bytea wire payload does not).
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::from_usize(0)
}
/// Set a `text` result (`cash_words`) on the by-ref lane. The fmgr boundary
/// carries `text` results header-stripped (the canonical `Varlena` image at
/// this boundary is the payload bytes without the `VARHDRSZ` length word; the
/// printtup/`textout` consumer re-wraps the header), mirroring the
/// `backend-utils-adt-varlena` text-result convention. C: `cstring_to_text`.
#[inline]
fn ret_text(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(s.into_bytes()));
    Datum::from_usize(0)
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    let chars = types_error::unpack_sqlstate(err.sqlstate());
    let code = core::str::from_utf8(&chars).unwrap_or("XX000");
    std::panic::panic_any(format!("PGRUST-SQLSTATE:{code}:{}", err.message()));
}
/// Unwrap a `PgResult<T>`, raising the error through `raise` on `Err`.
#[inline]
fn ok<T>(r: types_error::PgResult<T>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => raise(e),
    }
}
/// A scratch context for cores that allocate their result through `Mcx`. The
/// resulting bytes are copied onto the by-ref lane before it is dropped (C: the
/// palloc'd result lives in the caller's context; here it crosses by value).
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("cash fmgr scratch")
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

// ---- I/O ----
fn fc_cash_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // C: cash_in(cstring). Hard error context (no soft ErrorSaveContext is
    // modeled on the fmgr frame), matching every adt *in.
    let s = arg_cstring(fcinfo, 0).as_bytes().to_vec();
    ret_cash(ok(crate::cash_in(&s, None)))
}
fn fc_cash_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_cstring(fcinfo, crate::cash_out(arg_cash(fcinfo, 0)))
}
fn fc_cash_recv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let buf = arg_varlena(fcinfo, 0);
    ret_cash(ok(crate::cash_recv(buf)))
}
fn fc_cash_send(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let v = arg_cash(fcinfo, 0);
    let m = scratch_mcx();
    let bytes = ok(crate::cash_send(m.mcx(), v));
    ret_varlena(fcinfo, bytes.as_slice().to_vec())
}

// ---- comparisons ----
fn fc_cash_eq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::cash_eq(arg_cash(fcinfo, 0), arg_cash(fcinfo, 1)))
}
fn fc_cash_ne(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::cash_ne(arg_cash(fcinfo, 0), arg_cash(fcinfo, 1)))
}
fn fc_cash_lt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::cash_lt(arg_cash(fcinfo, 0), arg_cash(fcinfo, 1)))
}
fn fc_cash_le(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::cash_le(arg_cash(fcinfo, 0), arg_cash(fcinfo, 1)))
}
fn fc_cash_gt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::cash_gt(arg_cash(fcinfo, 0), arg_cash(fcinfo, 1)))
}
fn fc_cash_ge(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::cash_ge(arg_cash(fcinfo, 0), arg_cash(fcinfo, 1)))
}
fn fc_cash_cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::cash_cmp(arg_cash(fcinfo, 0), arg_cash(fcinfo, 1)))
}

// ---- arithmetic ----
fn fc_cash_pl(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_cash(ok(crate::cash_pl(arg_cash(fcinfo, 0), arg_cash(fcinfo, 1))))
}
fn fc_cash_mi(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_cash(ok(crate::cash_mi(arg_cash(fcinfo, 0), arg_cash(fcinfo, 1))))
}
fn fc_cash_div_cash(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_f64(ok(crate::cash_div_cash(arg_cash(fcinfo, 0), arg_cash(fcinfo, 1))))
}

// ---- mul/div against float8 ----
fn fc_cash_mul_flt8(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_cash(ok(crate::cash_mul_flt8(arg_cash(fcinfo, 0), arg_f64(fcinfo, 1))))
}
fn fc_flt8_mul_cash(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_cash(ok(crate::flt8_mul_cash(arg_f64(fcinfo, 0), arg_cash(fcinfo, 1))))
}
fn fc_cash_div_flt8(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_cash(ok(crate::cash_div_flt8(arg_cash(fcinfo, 0), arg_f64(fcinfo, 1))))
}

// ---- mul/div against float4 ----
fn fc_cash_mul_flt4(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_cash(ok(crate::cash_mul_flt4(arg_cash(fcinfo, 0), arg_f32(fcinfo, 1))))
}
fn fc_flt4_mul_cash(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_cash(ok(crate::flt4_mul_cash(arg_f32(fcinfo, 0), arg_cash(fcinfo, 1))))
}
fn fc_cash_div_flt4(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_cash(ok(crate::cash_div_flt4(arg_cash(fcinfo, 0), arg_f32(fcinfo, 1))))
}

// ---- mul/div against int8 ----
fn fc_cash_mul_int8(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_cash(ok(crate::cash_mul_int8(arg_cash(fcinfo, 0), arg_i64(fcinfo, 1))))
}
fn fc_int8_mul_cash(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_cash(ok(crate::int8_mul_cash(arg_i64(fcinfo, 0), arg_cash(fcinfo, 1))))
}
fn fc_cash_div_int8(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_cash(ok(crate::cash_div_int8(arg_cash(fcinfo, 0), arg_i64(fcinfo, 1))))
}

// ---- mul/div against int4 ----
fn fc_cash_mul_int4(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_cash(ok(crate::cash_mul_int4(arg_cash(fcinfo, 0), arg_i32(fcinfo, 1))))
}
fn fc_int4_mul_cash(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_cash(ok(crate::int4_mul_cash(arg_i32(fcinfo, 0), arg_cash(fcinfo, 1))))
}
fn fc_cash_div_int4(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_cash(ok(crate::cash_div_int4(arg_cash(fcinfo, 0), arg_i32(fcinfo, 1))))
}

// ---- mul/div against int2 ----
fn fc_cash_mul_int2(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_cash(ok(crate::cash_mul_int2(arg_cash(fcinfo, 0), arg_i16(fcinfo, 1))))
}
fn fc_int2_mul_cash(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_cash(ok(crate::int2_mul_cash(arg_i16(fcinfo, 0), arg_cash(fcinfo, 1))))
}
fn fc_cash_div_int2(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_cash(ok(crate::cash_div_int2(arg_cash(fcinfo, 0), arg_i16(fcinfo, 1))))
}

// ---- larger/smaller ----
fn fc_cashlarger(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_cash(crate::cashlarger(arg_cash(fcinfo, 0), arg_cash(fcinfo, 1)))
}
fn fc_cashsmaller(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_cash(crate::cashsmaller(arg_cash(fcinfo, 0), arg_cash(fcinfo, 1)))
}

// ---- words ----
fn fc_cash_words(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_text(fcinfo, crate::cash_words(arg_cash(fcinfo, 0)))
}

// ---- casts ----
fn fc_cash_numeric(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let money = arg_cash(fcinfo, 0);
    let m = scratch_mcx();
    let image = ok(crate::cash_numeric(m.mcx(), money));
    ret_varlena(fcinfo, image.as_slice().to_vec())
}
fn fc_numeric_cash(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = arg_varlena(fcinfo, 0);
    let m = scratch_mcx();
    ret_cash(ok(crate::numeric_cash(m.mcx(), num)))
}
fn fc_int4_cash(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_cash(ok(crate::int4_cash(arg_i32(fcinfo, 0))))
}
fn fc_int8_cash(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_cash(ok(crate::int8_cash(arg_i64(fcinfo, 0))))
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

/// Register every `cash.c` builtin into the fmgr-core builtin table (C:
/// `fmgr_builtins[]`), so by-OID dispatch resolves them. Called from this
/// crate's `init_seams()`. OIDs/nargs/strict/retset transcribed from
/// `pg_proc.dat` (all rows: `proisstrict => 't'`, none `proretset`).
pub fn register_cash_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // ---- I/O ----
        builtin(886, "cash_in", 1, true, false, fc_cash_in),
        builtin(887, "cash_out", 1, true, false, fc_cash_out),
        builtin(2492, "cash_recv", 1, true, false, fc_cash_recv),
        builtin(2493, "cash_send", 1, true, false, fc_cash_send),
        // ---- comparisons ----
        builtin(888, "cash_eq", 2, true, false, fc_cash_eq),
        builtin(889, "cash_ne", 2, true, false, fc_cash_ne),
        builtin(890, "cash_lt", 2, true, false, fc_cash_lt),
        builtin(891, "cash_le", 2, true, false, fc_cash_le),
        builtin(892, "cash_gt", 2, true, false, fc_cash_gt),
        builtin(893, "cash_ge", 2, true, false, fc_cash_ge),
        builtin(377, "cash_cmp", 2, true, false, fc_cash_cmp),
        // ---- arithmetic ----
        builtin(894, "cash_pl", 2, true, false, fc_cash_pl),
        builtin(895, "cash_mi", 2, true, false, fc_cash_mi),
        builtin(3822, "cash_div_cash", 2, true, false, fc_cash_div_cash),
        // ---- mul/div float8 ----
        builtin(896, "cash_mul_flt8", 2, true, false, fc_cash_mul_flt8),
        builtin(897, "cash_div_flt8", 2, true, false, fc_cash_div_flt8),
        builtin(919, "flt8_mul_cash", 2, true, false, fc_flt8_mul_cash),
        // ---- mul/div float4 ----
        builtin(846, "cash_mul_flt4", 2, true, false, fc_cash_mul_flt4),
        builtin(847, "cash_div_flt4", 2, true, false, fc_cash_div_flt4),
        builtin(848, "flt4_mul_cash", 2, true, false, fc_flt4_mul_cash),
        // ---- mul/div int8 ----
        builtin(3344, "cash_mul_int8", 2, true, false, fc_cash_mul_int8),
        builtin(3345, "cash_div_int8", 2, true, false, fc_cash_div_int8),
        builtin(3399, "int8_mul_cash", 2, true, false, fc_int8_mul_cash),
        // ---- mul/div int4 ----
        builtin(864, "cash_mul_int4", 2, true, false, fc_cash_mul_int4),
        builtin(865, "cash_div_int4", 2, true, false, fc_cash_div_int4),
        builtin(862, "int4_mul_cash", 2, true, false, fc_int4_mul_cash),
        // ---- mul/div int2 ----
        builtin(866, "cash_mul_int2", 2, true, false, fc_cash_mul_int2),
        builtin(867, "cash_div_int2", 2, true, false, fc_cash_div_int2),
        builtin(863, "int2_mul_cash", 2, true, false, fc_int2_mul_cash),
        // ---- larger/smaller ----
        builtin(898, "cashlarger", 2, true, false, fc_cashlarger),
        builtin(899, "cashsmaller", 2, true, false, fc_cashsmaller),
        // ---- words ----
        builtin(935, "cash_words", 1, true, false, fc_cash_words),
        // ---- casts ----
        builtin(3823, "cash_numeric", 1, true, false, fc_cash_numeric),
        builtin(3824, "numeric_cash", 1, true, false, fc_numeric_cash),
        builtin(3811, "int4_cash", 1, true, false, fc_int4_cash),
        builtin(3812, "int8_cash", 1, true, false, fc_int8_cash),
    ]);
}
