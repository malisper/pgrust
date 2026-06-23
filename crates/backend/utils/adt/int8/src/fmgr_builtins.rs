//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for every SQL-callable
//! function in `int8.c` whose argument/result types are expressible at the
//! current fmgr boundary (scalar `int8`/`int4`/`int2`/`oid`/`float8`/`float4`
//! plus the `int8` text I/O, comparison operators, arithmetic, bit ops, casts,
//! `in_range`, and the `gcd`/`lcm`/`mod` SQL aliases).
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame (by-val words for scalars; the by-ref lane for the `cstring`
//! input of `int8in`), calls the matching value core in [`crate`], and writes
//! back the result word / by-reference payload, exactly as the sibling `oid.c`
//! port does. [`register_int8_builtins`] registers every row into the
//! fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID dispatch resolves
//! them. OIDs / nargs / strict / retset are transcribed exactly from
//! `pg_proc.dat` (every listed row is `proisstrict => 't'`, none retset).
//!
//! The binary-wire `int8recv` / `int8send`, the aggregate fast path of
//! `int8inc` / `int8dec`, the set-returning `generate_series_int8` family and
//! the planner *prosupport* functions are NOT registered here — see the crate
//! docs for why (they are the pqformat / executor / planner layers' frames).

use ::datum::Datum;
use ::fmgr::boundary::RefPayload;
use fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

use ::types_core::Oid;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_INT64(i)`: arg `i`'s word as a signed 64-bit integer.
#[inline]
fn arg_i64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo.arg(i).expect("int8 fn: missing arg").value.as_i64()
}
/// `PG_GETARG_INT32(i)`.
#[inline]
fn arg_i32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.arg(i).expect("int8 fn: missing arg").value.as_i32()
}
/// `PG_GETARG_INT16(i)`.
#[inline]
fn arg_i16(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i16 {
    fcinfo.arg(i).expect("int8 fn: missing arg").value.as_i16()
}
/// `PG_GETARG_OID(i)`.
#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Oid {
    fcinfo.arg(i).expect("int8 fn: missing arg").value.as_oid()
}
/// `PG_GETARG_FLOAT8(i)`.
#[inline]
fn arg_f64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> f64 {
    fcinfo.arg(i).expect("int8 fn: missing arg").value.as_f64()
}
/// `PG_GETARG_FLOAT4(i)`.
#[inline]
fn arg_f32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> f32 {
    fcinfo.arg(i).expect("int8 fn: missing arg").value.as_f32()
}
/// `PG_GETARG_BOOL(i)`.
#[inline]
fn arg_bool(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo.arg(i).expect("int8 fn: missing arg").value.as_bool()
}
/// `PG_GETARG_CSTRING(i)`: the input text on the by-ref lane (`int8in`).
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("int8 fn: cstring arg missing from by-ref lane")
}

#[inline]
fn ret_i64(v: i64) -> Datum {
    Datum::from_i64(v)
}
#[inline]
fn ret_i32(v: i32) -> Datum {
    Datum::from_i32(v)
}
#[inline]
fn ret_i16(v: i16) -> Datum {
    Datum::from_i16(v)
}
#[inline]
fn ret_oid(v: Oid) -> Datum {
    Datum::from_oid(v)
}
#[inline]
fn ret_f64(v: f64) -> Datum {
    Datum::from_f64(v)
}
#[inline]
fn ret_f32(v: f32) -> Datum {
    Datum::from_f32(v)
}
#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}
/// Set a `cstring` (`int8out`) result on the by-ref lane and return the dummy
/// word.
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}

/// Map a `PgResult<i64>` core to the Result-native fmgr return: its
/// `ereport(ERROR)` travels as `Err(PgError)` straight back to the dispatch
/// (`invoke_builtin`), with no panic / `catch_unwind`.
#[inline]
fn try_i64(r: types_error::PgResult<i64>) -> types_error::PgResult<Datum> {
    r.map(ret_i64)
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

// ---- I/O ----

fn fc_int8in(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C: `int8in` does `pg_strtoint64_safe(str, fcinfo->context)`, forwarding the
    // frame's soft `ErrorSaveContext` (installed by `InputFunctionCallSafe`) so a
    // recoverable parse failure `ereturn`s into the sink instead of throwing. We
    // own-copy the cstring to release the immutable arg borrow before taking the
    // mutable escontext borrow off the same frame.
    let s = arg_cstring(fcinfo, 0).to_string();
    let escontext = fcinfo.escontext_mut();
    match crate::int8in(&s, escontext) {
        Ok(v) => Ok(ret_i64(v)),
        Err(e) => Err(e),
    }
}

fn fc_int8out(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = arg_i64(fcinfo, 0);
    Ok(ret_cstring(fcinfo, crate::int8out(v)))
}

/// `int8send(int8) -> bytea` (pg_proc.dat oid 2409). C: `pq_begintypsend(&buf);
/// pq_sendint64(&buf, arg1); PG_RETURN_BYTEA_P(pq_endtypsend(&buf))` — the value's
/// 8 big-endian bytes. The `Varlena` payload is just the wire bytes; the libpq /
/// fmgr boundary wraps the varlena framing.
fn fc_int8send(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = arg_i64(fcinfo, 0);
    fcinfo.set_ref_result(RefPayload::Varlena(v.to_be_bytes().to_vec()));
    Ok(Datum::from_usize(0))
}

// ---- comparison operators (int8 / int84 / int48 / int82 / int28) ----

fn fc_int8eq(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::int8eq(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1))))
}
fn fc_int8ne(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::int8ne(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1))))
}
fn fc_int8lt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::int8lt(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1))))
}
fn fc_int8gt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::int8gt(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1))))
}
fn fc_int8le(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::int8le(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1))))
}
fn fc_int8ge(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::int8ge(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1))))
}

fn fc_int84eq(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::int84eq(arg_i64(fcinfo, 0), arg_i32(fcinfo, 1))))
}
fn fc_int84ne(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::int84ne(arg_i64(fcinfo, 0), arg_i32(fcinfo, 1))))
}
fn fc_int84lt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::int84lt(arg_i64(fcinfo, 0), arg_i32(fcinfo, 1))))
}
fn fc_int84gt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::int84gt(arg_i64(fcinfo, 0), arg_i32(fcinfo, 1))))
}
fn fc_int84le(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::int84le(arg_i64(fcinfo, 0), arg_i32(fcinfo, 1))))
}
fn fc_int84ge(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::int84ge(arg_i64(fcinfo, 0), arg_i32(fcinfo, 1))))
}

fn fc_int48eq(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::int48eq(arg_i32(fcinfo, 0), arg_i64(fcinfo, 1))))
}
fn fc_int48ne(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::int48ne(arg_i32(fcinfo, 0), arg_i64(fcinfo, 1))))
}
fn fc_int48lt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::int48lt(arg_i32(fcinfo, 0), arg_i64(fcinfo, 1))))
}
fn fc_int48gt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::int48gt(arg_i32(fcinfo, 0), arg_i64(fcinfo, 1))))
}
fn fc_int48le(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::int48le(arg_i32(fcinfo, 0), arg_i64(fcinfo, 1))))
}
fn fc_int48ge(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::int48ge(arg_i32(fcinfo, 0), arg_i64(fcinfo, 1))))
}

fn fc_int82eq(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::int82eq(arg_i64(fcinfo, 0), arg_i16(fcinfo, 1))))
}
fn fc_int82ne(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::int82ne(arg_i64(fcinfo, 0), arg_i16(fcinfo, 1))))
}
fn fc_int82lt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::int82lt(arg_i64(fcinfo, 0), arg_i16(fcinfo, 1))))
}
fn fc_int82gt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::int82gt(arg_i64(fcinfo, 0), arg_i16(fcinfo, 1))))
}
fn fc_int82le(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::int82le(arg_i64(fcinfo, 0), arg_i16(fcinfo, 1))))
}
fn fc_int82ge(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::int82ge(arg_i64(fcinfo, 0), arg_i16(fcinfo, 1))))
}

fn fc_int28eq(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::int28eq(arg_i16(fcinfo, 0), arg_i64(fcinfo, 1))))
}
fn fc_int28ne(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::int28ne(arg_i16(fcinfo, 0), arg_i64(fcinfo, 1))))
}
fn fc_int28lt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::int28lt(arg_i16(fcinfo, 0), arg_i64(fcinfo, 1))))
}
fn fc_int28gt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::int28gt(arg_i16(fcinfo, 0), arg_i64(fcinfo, 1))))
}
fn fc_int28le(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::int28le(arg_i16(fcinfo, 0), arg_i64(fcinfo, 1))))
}
fn fc_int28ge(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::int28ge(arg_i16(fcinfo, 0), arg_i64(fcinfo, 1))))
}

// ---- unary / arithmetic on int8 ----

fn fc_int8um(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    try_i64(crate::int8um(arg_i64(fcinfo, 0)))
}
fn fc_int8up(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_i64(crate::int8up(arg_i64(fcinfo, 0))))
}
fn fc_int8pl(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    try_i64(crate::int8pl(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_int8mi(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    try_i64(crate::int8mi(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_int8mul(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    try_i64(crate::int8mul(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_int8div(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    try_i64(crate::int8div(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_int8abs(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    try_i64(crate::int8abs(arg_i64(fcinfo, 0)))
}
fn fc_int8mod(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    try_i64(crate::int8mod(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_int8gcd(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    try_i64(crate::int8gcd(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_int8lcm(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    try_i64(crate::int8lcm(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_int8inc(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    try_i64(crate::int8inc(arg_i64(fcinfo, 0)))
}
fn fc_int8dec(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    try_i64(crate::int8dec(arg_i64(fcinfo, 0)))
}
fn fc_int8inc_any(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C: int8inc_any(fcinfo) { return int8inc(fcinfo); } — the count(any) transfn:
    // increments arg0 (the running int8 count), ignoring the (any-typed) input.
    try_i64(crate::int8inc_any(arg_i64(fcinfo, 0)))
}
fn fc_int8inc_float8_float8(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C: int8inc_float8_float8(fcinfo) { return int8inc(fcinfo); } — ignores the
    // two trailing float8 args and increments arg0 in place.
    try_i64(crate::int8inc_float8_float8(arg_i64(fcinfo, 0)))
}
fn fc_int8dec_any(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C: int8dec_any(fcinfo) { return int8dec(fcinfo); } — the count(*)/count(any)
    // inverse-transition: decrements arg0 (the running int8 count), ignoring the
    // (any-typed) second input.
    try_i64(crate::int8dec_any(arg_i64(fcinfo, 0)))
}
fn fc_int8larger(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_i64(crate::int8larger(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1))))
}
fn fc_int8smaller(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_i64(crate::int8smaller(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1))))
}

// ---- mixed-width arithmetic (int84 / int48 / int82 / int28) ----

fn fc_int84pl(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    try_i64(crate::int84pl(arg_i64(fcinfo, 0), arg_i32(fcinfo, 1)))
}
fn fc_int84mi(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    try_i64(crate::int84mi(arg_i64(fcinfo, 0), arg_i32(fcinfo, 1)))
}
fn fc_int84mul(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    try_i64(crate::int84mul(arg_i64(fcinfo, 0), arg_i32(fcinfo, 1)))
}
fn fc_int84div(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    try_i64(crate::int84div(arg_i64(fcinfo, 0), arg_i32(fcinfo, 1)))
}

fn fc_int48pl(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    try_i64(crate::int48pl(arg_i32(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_int48mi(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    try_i64(crate::int48mi(arg_i32(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_int48mul(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    try_i64(crate::int48mul(arg_i32(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_int48div(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    try_i64(crate::int48div(arg_i32(fcinfo, 0), arg_i64(fcinfo, 1)))
}

fn fc_int82pl(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    try_i64(crate::int82pl(arg_i64(fcinfo, 0), arg_i16(fcinfo, 1)))
}
fn fc_int82mi(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    try_i64(crate::int82mi(arg_i64(fcinfo, 0), arg_i16(fcinfo, 1)))
}
fn fc_int82mul(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    try_i64(crate::int82mul(arg_i64(fcinfo, 0), arg_i16(fcinfo, 1)))
}
fn fc_int82div(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    try_i64(crate::int82div(arg_i64(fcinfo, 0), arg_i16(fcinfo, 1)))
}

fn fc_int28pl(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    try_i64(crate::int28pl(arg_i16(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_int28mi(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    try_i64(crate::int28mi(arg_i16(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_int28mul(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    try_i64(crate::int28mul(arg_i16(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_int28div(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    try_i64(crate::int28div(arg_i16(fcinfo, 0), arg_i64(fcinfo, 1)))
}

// ---- bit operators ----

fn fc_int8and(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_i64(crate::int8and(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1))))
}
fn fc_int8or(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_i64(crate::int8or(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1))))
}
fn fc_int8xor(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_i64(crate::int8xor(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1))))
}
fn fc_int8not(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_i64(crate::int8not(arg_i64(fcinfo, 0))))
}
fn fc_int8shl(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_i64(crate::int8shl(arg_i64(fcinfo, 0), arg_i32(fcinfo, 1))))
}
fn fc_int8shr(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_i64(crate::int8shr(arg_i64(fcinfo, 0), arg_i32(fcinfo, 1))))
}

// ---- casts ----

fn fc_int84(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    match crate::int84(arg_i64(fcinfo, 0)) {
        Ok(v) => Ok(ret_i32(v)),
        Err(e) => Err(e),
    }
}
fn fc_int48(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_i64(crate::int48(arg_i32(fcinfo, 0))))
}
fn fc_int82(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    match crate::int82(arg_i64(fcinfo, 0)) {
        Ok(v) => Ok(ret_i16(v)),
        Err(e) => Err(e),
    }
}
fn fc_int28(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_i64(crate::int28(arg_i16(fcinfo, 0))))
}
fn fc_i8tod(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::i8tod(arg_i64(fcinfo, 0))))
}
fn fc_dtoi8(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    match crate::dtoi8(arg_f64(fcinfo, 0)) {
        Ok(v) => Ok(ret_i64(v)),
        Err(e) => Err(e),
    }
}
fn fc_i8tof(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f32(crate::i8tof(arg_i64(fcinfo, 0))))
}
fn fc_ftoi8(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    match crate::ftoi8(arg_f32(fcinfo, 0)) {
        Ok(v) => Ok(ret_i64(v)),
        Err(e) => Err(e),
    }
}
fn fc_i8tooid(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    match crate::i8tooid(arg_i64(fcinfo, 0)) {
        Ok(v) => Ok(ret_oid(v)),
        Err(e) => Err(e),
    }
}
fn fc_oidtoi8(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_i64(crate::oidtoi8(arg_oid(fcinfo, 0))))
}

// ---- window in_range ----

fn fc_in_range_int8_int8(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    match crate::in_range_int8_int8(
        arg_i64(fcinfo, 0),
        arg_i64(fcinfo, 1),
        arg_i64(fcinfo, 2),
        arg_bool(fcinfo, 3),
        arg_bool(fcinfo, 4),
    ) {
        Ok(v) => Ok(ret_bool(v)),
        Err(e) => Err(e),
    }
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

/// Build one Result-native builtin row: the [`BuiltinFunction`] metadata (with
/// `func: None` — the legacy callable is unused; dispatch goes through the native
/// overlay) paired with its [`PgFnNative`] body.
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

/// Register every scalar `int8.c` builtin (C: their `fmgr_builtins[]` rows) as
/// **Result-native** (the panic→Result migration; see
/// `docs/proposals/panic-to-result-migration.md`). Called from this crate's
/// [`crate::init_seams`]. OIDs / nargs / strict / retset transcribed exactly from
/// `pg_proc.dat` (all `proisstrict => 't'`, none retset).
pub fn register_int8_builtins() {
    fmgr_core::register_builtins_native([
        // ---- I/O ----
        builtin(460, "int8in", 1, true, false, fc_int8in),
        builtin(461, "int8out", 1, true, false, fc_int8out),
        builtin(2409, "int8send", 1, true, false, fc_int8send),
        // ---- comparison operators ----
        builtin(467, "int8eq", 2, true, false, fc_int8eq),
        builtin(468, "int8ne", 2, true, false, fc_int8ne),
        builtin(469, "int8lt", 2, true, false, fc_int8lt),
        builtin(470, "int8gt", 2, true, false, fc_int8gt),
        builtin(471, "int8le", 2, true, false, fc_int8le),
        builtin(472, "int8ge", 2, true, false, fc_int8ge),
        builtin(474, "int84eq", 2, true, false, fc_int84eq),
        builtin(475, "int84ne", 2, true, false, fc_int84ne),
        builtin(476, "int84lt", 2, true, false, fc_int84lt),
        builtin(477, "int84gt", 2, true, false, fc_int84gt),
        builtin(478, "int84le", 2, true, false, fc_int84le),
        builtin(479, "int84ge", 2, true, false, fc_int84ge),
        builtin(852, "int48eq", 2, true, false, fc_int48eq),
        builtin(853, "int48ne", 2, true, false, fc_int48ne),
        builtin(854, "int48lt", 2, true, false, fc_int48lt),
        builtin(855, "int48gt", 2, true, false, fc_int48gt),
        builtin(856, "int48le", 2, true, false, fc_int48le),
        builtin(857, "int48ge", 2, true, false, fc_int48ge),
        builtin(1856, "int82eq", 2, true, false, fc_int82eq),
        builtin(1857, "int82ne", 2, true, false, fc_int82ne),
        builtin(1858, "int82lt", 2, true, false, fc_int82lt),
        builtin(1859, "int82gt", 2, true, false, fc_int82gt),
        builtin(1860, "int82le", 2, true, false, fc_int82le),
        builtin(1861, "int82ge", 2, true, false, fc_int82ge),
        builtin(1850, "int28eq", 2, true, false, fc_int28eq),
        builtin(1851, "int28ne", 2, true, false, fc_int28ne),
        builtin(1852, "int28lt", 2, true, false, fc_int28lt),
        builtin(1853, "int28gt", 2, true, false, fc_int28gt),
        builtin(1854, "int28le", 2, true, false, fc_int28le),
        builtin(1855, "int28ge", 2, true, false, fc_int28ge),
        // ---- unary / arithmetic on int8 ----
        builtin(462, "int8um", 1, true, false, fc_int8um),
        builtin(1910, "int8up", 1, true, false, fc_int8up),
        builtin(463, "int8pl", 2, true, false, fc_int8pl),
        builtin(464, "int8mi", 2, true, false, fc_int8mi),
        builtin(465, "int8mul", 2, true, false, fc_int8mul),
        builtin(466, "int8div", 2, true, false, fc_int8div),
        builtin(1230, "int8abs", 1, true, false, fc_int8abs),
        builtin(1396, "int8abs", 1, true, false, fc_int8abs),
        builtin(945, "int8mod", 2, true, false, fc_int8mod),
        builtin(947, "int8mod", 2, true, false, fc_int8mod),
        builtin(5045, "int8gcd", 2, true, false, fc_int8gcd),
        builtin(5047, "int8lcm", 2, true, false, fc_int8lcm),
        builtin(1219, "int8inc", 1, true, false, fc_int8inc),
        builtin(2804, "int8inc_any", 2, true, false, fc_int8inc_any),
        builtin(3546, "int8dec", 1, true, false, fc_int8dec),
        builtin(3547, "int8dec_any", 2, true, false, fc_int8dec_any),
        builtin(
            2805,
            "int8inc_float8_float8",
            3,
            true,
            false,
            fc_int8inc_float8_float8,
        ),
        builtin(1236, "int8larger", 2, true, false, fc_int8larger),
        builtin(1237, "int8smaller", 2, true, false, fc_int8smaller),
        // ---- mixed-width arithmetic ----
        builtin(1274, "int84pl", 2, true, false, fc_int84pl),
        builtin(1275, "int84mi", 2, true, false, fc_int84mi),
        builtin(1276, "int84mul", 2, true, false, fc_int84mul),
        builtin(1277, "int84div", 2, true, false, fc_int84div),
        builtin(1278, "int48pl", 2, true, false, fc_int48pl),
        builtin(1279, "int48mi", 2, true, false, fc_int48mi),
        builtin(1280, "int48mul", 2, true, false, fc_int48mul),
        builtin(1281, "int48div", 2, true, false, fc_int48div),
        builtin(837, "int82pl", 2, true, false, fc_int82pl),
        builtin(838, "int82mi", 2, true, false, fc_int82mi),
        builtin(839, "int82mul", 2, true, false, fc_int82mul),
        builtin(840, "int82div", 2, true, false, fc_int82div),
        builtin(841, "int28pl", 2, true, false, fc_int28pl),
        builtin(942, "int28mi", 2, true, false, fc_int28mi),
        builtin(943, "int28mul", 2, true, false, fc_int28mul),
        builtin(948, "int28div", 2, true, false, fc_int28div),
        // ---- bit operators ----
        builtin(1904, "int8and", 2, true, false, fc_int8and),
        builtin(1905, "int8or", 2, true, false, fc_int8or),
        builtin(1906, "int8xor", 2, true, false, fc_int8xor),
        builtin(1907, "int8not", 1, true, false, fc_int8not),
        builtin(1908, "int8shl", 2, true, false, fc_int8shl),
        builtin(1909, "int8shr", 2, true, false, fc_int8shr),
        // ---- casts ----
        builtin(480, "int84", 1, true, false, fc_int84),
        builtin(481, "int48", 1, true, false, fc_int48),
        builtin(714, "int82", 1, true, false, fc_int82),
        builtin(754, "int28", 1, true, false, fc_int28),
        builtin(482, "i8tod", 1, true, false, fc_i8tod),
        builtin(483, "dtoi8", 1, true, false, fc_dtoi8),
        builtin(652, "i8tof", 1, true, false, fc_i8tof),
        builtin(653, "ftoi8", 1, true, false, fc_ftoi8),
        builtin(1287, "i8tooid", 1, true, false, fc_i8tooid),
        builtin(1288, "oidtoi8", 1, true, false, fc_oidtoi8),
        // ---- window in_range ----
        builtin(4126, "in_range_int8_int8", 5, true, false, fc_in_range_int8_int8),
    ]);
}
