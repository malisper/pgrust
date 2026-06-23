//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! functions in `float.c` whose argument/result types are expressible at the
//! current fmgr boundary (the scalar `float4`/`float8` I/O, arithmetic, the
//! cross-type `float48`/`float84` operators, the math/trig functions, the
//! conversion casts to/from `int2`/`int4`, the btree comparators, `in_range`
//! and `width_bucket_float8`).
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core in this crate, and writes back the
//! result word / by-reference payload. [`register_float_builtins`] registers
//! every row into the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID
//! dispatch resolves them. OIDs / nargs / strict / retset are transcribed
//! exactly from `pg_proc.dat` (all rows here are `proisstrict => 't'` and not
//! `proretset`).

use datum::Datum;
use fmgr::boundary::RefPayload;
use fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_FLOAT4(i)`: the `float4` word reinterpreted as `f32`.
#[inline]
fn arg_f32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> f32 {
    fcinfo.arg(i).expect("float fn: missing arg").value.as_f32()
}

/// `PG_GETARG_FLOAT8(i)`: the `float8` word reinterpreted as `f64`.
#[inline]
fn arg_f64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> f64 {
    fcinfo.arg(i).expect("float fn: missing arg").value.as_f64()
}

/// `PG_GETARG_INT16(i)`.
#[inline]
fn arg_i16(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i16 {
    fcinfo.arg(i).expect("float fn: missing arg").value.as_i16()
}

/// `PG_GETARG_INT32(i)`.
#[inline]
fn arg_i32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.arg(i).expect("float fn: missing arg").value.as_i32()
}

/// `PG_GETARG_BOOL(i)`.
#[inline]
fn arg_bool(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo.arg(i).expect("float fn: missing arg").value.as_bool()
}

/// `PG_GETARG_CSTRING(i)`: the input text on the by-ref lane.
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("float fn: cstring arg missing from by-ref lane")
}

/// `PG_GETARG_POINTER(i)` as a `StringInfo` wire buffer: the `recv` byte image
/// on the by-ref lane.
#[inline]
fn arg_varlena<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("float fn: by-ref buffer arg missing from by-ref lane")
}

#[inline]
fn ret_f32(v: f32) -> Datum {
    Datum::from_f32(v)
}
#[inline]
fn ret_f64(v: f64) -> Datum {
    Datum::from_f64(v)
}
#[inline]
fn ret_i16(v: i16) -> Datum {
    Datum::from_i16(v)
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

/// Set a `bytea` (`_send`) result on the by-ref lane. Under the
/// header-ful-everywhere convention the `RefPayload::Varlena` carrier must hold a
/// COMPLETE `struct varlena *` image (4-byte length header + payload), exactly
/// what a downstream `byteaout`/`VARDATA_ANY` reader skips the header off. The
/// `_send` core returns the bare payload bytes, so frame them with the 4-byte
/// header here (`SET_VARSIZE`); handing back a headerless payload makes a reader
/// mis-parse the first payload byte as a short varlena header and drop bytes.
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    const VARHDRSZ: usize = 4;
    let mut image = Vec::with_capacity(bytes.len() + VARHDRSZ);
    image.extend_from_slice(&datum::varlena::set_varsize_4b(bytes.len() + VARHDRSZ));
    image.extend_from_slice(&bytes);
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    Datum::from_usize(0)
}

/// Set a `float8[]` transition-state result on the by-ref lane. The aggregate
/// transition/combine cores return a COMPLETE `ArrayType` varlena image (built by
/// `construct_array`, 4-byte header + ARR_* body), so the `RefPayload::Varlena`
/// lane carries it verbatim — no extra header framing (unlike [`ret_varlena`]).
#[inline]
fn ret_array_raw(fcinfo: &mut FunctionCallInfoBaseData, image: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    Datum::from_usize(0)
}

/// Write the `Option<f64>` result of a simple aggregate final function: `None`
/// is SQL NULL, `Some(v)` is the `float8` word.
#[inline]
fn ret_opt_f64(fcinfo: &mut FunctionCallInfoBaseData, v: Option<f64>) -> Datum {
    match v {
        Some(x) => ret_f64(x),
        None => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
    }
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

// ---- I/O ----
fn fc_float4in(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C: `float4in` forwards `fcinfo->context` for soft `pg_input_is_valid`.
    let s = arg_cstring(fcinfo, 0).to_string();
    let escontext = fcinfo.escontext_mut();
    Ok(ret_f32(crate::float4in(&s, escontext)?))
}
fn fc_float4out(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = arg_f32(fcinfo, 0);
    Ok(ret_cstring(fcinfo, crate::float4out(v)))
}
fn fc_float8in(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C: `float8in` forwards `fcinfo->context` for soft `pg_input_is_valid`.
    let s = arg_cstring(fcinfo, 0).to_string();
    let escontext = fcinfo.escontext_mut();
    Ok(ret_f64(crate::float8in(&s, escontext)?))
}
fn fc_float8out(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = arg_f64(fcinfo, 0);
    Ok(ret_cstring(fcinfo, crate::float8out(v)))
}
fn fc_float4send(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = arg_f32(fcinfo, 0);
    Ok(ret_varlena(fcinfo, crate::float4send(v)))
}
fn fc_float8send(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = arg_f64(fcinfo, 0);
    Ok(ret_varlena(fcinfo, crate::float8send(v)))
}
fn fc_float4recv(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let buf = arg_varlena(fcinfo, 0);
    Ok(ret_f32(crate::float4recv(buf)?))
}
fn fc_float8recv(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let buf = arg_varlena(fcinfo, 0);
    Ok(ret_f64(crate::float8recv(buf)?))
}

// ---- same-type arithmetic (float4) ----
fn fc_float4pl(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f32(crate::float4_pl(
        arg_f32(fcinfo, 0),
        arg_f32(fcinfo, 1),
    )?))
}
fn fc_float4mi(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f32(crate::float4_mi(
        arg_f32(fcinfo, 0),
        arg_f32(fcinfo, 1),
    )?))
}
fn fc_float4mul(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f32(crate::float4_mul(
        arg_f32(fcinfo, 0),
        arg_f32(fcinfo, 1),
    )?))
}
fn fc_float4div(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f32(crate::float4_div(
        arg_f32(fcinfo, 0),
        arg_f32(fcinfo, 1),
    )?))
}

// ---- same-type arithmetic (float8) ----
fn fc_float8pl(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::float8_pl(
        arg_f64(fcinfo, 0),
        arg_f64(fcinfo, 1),
    )?))
}
fn fc_float8mi(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::float8_mi(
        arg_f64(fcinfo, 0),
        arg_f64(fcinfo, 1),
    )?))
}
fn fc_float8mul(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::float8_mul(
        arg_f64(fcinfo, 0),
        arg_f64(fcinfo, 1),
    )?))
}
fn fc_float8div(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::float8_div(
        arg_f64(fcinfo, 0),
        arg_f64(fcinfo, 1),
    )?))
}

// ---- same-type comparisons (float4) ----
fn fc_float4eq(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::float4_eq(arg_f32(fcinfo, 0), arg_f32(fcinfo, 1))))
}
fn fc_float4ne(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::float4_ne(arg_f32(fcinfo, 0), arg_f32(fcinfo, 1))))
}
fn fc_float4lt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::float4_lt(arg_f32(fcinfo, 0), arg_f32(fcinfo, 1))))
}
fn fc_float4le(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::float4_le(arg_f32(fcinfo, 0), arg_f32(fcinfo, 1))))
}
fn fc_float4gt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::float4_gt(arg_f32(fcinfo, 0), arg_f32(fcinfo, 1))))
}
fn fc_float4ge(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::float4_ge(arg_f32(fcinfo, 0), arg_f32(fcinfo, 1))))
}

// ---- same-type comparisons (float8) ----
fn fc_float8eq(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::float8_eq(arg_f64(fcinfo, 0), arg_f64(fcinfo, 1))))
}
fn fc_float8ne(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::float8_ne(arg_f64(fcinfo, 0), arg_f64(fcinfo, 1))))
}
fn fc_float8lt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::float8_lt(arg_f64(fcinfo, 0), arg_f64(fcinfo, 1))))
}
fn fc_float8le(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::float8_le(arg_f64(fcinfo, 0), arg_f64(fcinfo, 1))))
}
fn fc_float8gt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::float8_gt(arg_f64(fcinfo, 0), arg_f64(fcinfo, 1))))
}
fn fc_float8ge(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::float8_ge(arg_f64(fcinfo, 0), arg_f64(fcinfo, 1))))
}

// ---- unary float4 ----
fn fc_float4um(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f32(crate::float4um(arg_f32(fcinfo, 0))))
}
fn fc_float4up(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f32(crate::float4up(arg_f32(fcinfo, 0))))
}
fn fc_float4abs(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f32(crate::float4abs(arg_f32(fcinfo, 0))))
}
fn fc_float4larger(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f32(crate::float4larger(arg_f32(fcinfo, 0), arg_f32(fcinfo, 1))))
}
fn fc_float4smaller(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f32(crate::float4smaller(arg_f32(fcinfo, 0), arg_f32(fcinfo, 1))))
}

// ---- unary float8 ----
fn fc_float8um(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::float8um(arg_f64(fcinfo, 0))))
}
fn fc_float8up(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::float8up(arg_f64(fcinfo, 0))))
}
fn fc_float8abs(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::float8abs(arg_f64(fcinfo, 0))))
}
fn fc_float8larger(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::float8larger(arg_f64(fcinfo, 0), arg_f64(fcinfo, 1))))
}
fn fc_float8smaller(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::float8smaller(arg_f64(fcinfo, 0), arg_f64(fcinfo, 1))))
}

// ---- math (float8) ----
fn fc_dround(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::dround(arg_f64(fcinfo, 0))))
}
fn fc_dtrunc(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::dtrunc(arg_f64(fcinfo, 0))))
}
fn fc_dceil(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::dceil(arg_f64(fcinfo, 0))))
}
fn fc_dfloor(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::dfloor(arg_f64(fcinfo, 0))))
}
fn fc_dsign(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::dsign(arg_f64(fcinfo, 0))))
}
fn fc_dsqrt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::dsqrt(arg_f64(fcinfo, 0))?))
}
fn fc_dcbrt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::dcbrt(arg_f64(fcinfo, 0))?))
}
fn fc_dpow(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::dpow(
        arg_f64(fcinfo, 0),
        arg_f64(fcinfo, 1),
    )?))
}
fn fc_dexp(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::dexp(arg_f64(fcinfo, 0))?))
}
fn fc_dlog1(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::dlog1(arg_f64(fcinfo, 0))?))
}
fn fc_dlog10(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::dlog10(arg_f64(fcinfo, 0))?))
}

// ---- trig (radians) ----
fn fc_dasin(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::dasin(arg_f64(fcinfo, 0))?))
}
fn fc_dacos(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::dacos(arg_f64(fcinfo, 0))?))
}
fn fc_datan(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::datan(arg_f64(fcinfo, 0))?))
}
fn fc_datan2(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::datan2(
        arg_f64(fcinfo, 0),
        arg_f64(fcinfo, 1),
    )?))
}
fn fc_dsin(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::dsin(arg_f64(fcinfo, 0))?))
}
fn fc_dcos(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::dcos(arg_f64(fcinfo, 0))?))
}
fn fc_dtan(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::dtan(arg_f64(fcinfo, 0))?))
}
fn fc_dcot(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::dcot(arg_f64(fcinfo, 0))?))
}
fn fc_degrees(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::degrees(arg_f64(fcinfo, 0))?))
}
fn fc_radians(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::radians(arg_f64(fcinfo, 0))?))
}
fn fc_dpi(_fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::dpi()))
}

// ---- trig (degrees) ----
fn fc_dasind(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::dasind(arg_f64(fcinfo, 0))?))
}
fn fc_dacosd(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::dacosd(arg_f64(fcinfo, 0))?))
}
fn fc_datand(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::datand(arg_f64(fcinfo, 0))?))
}
fn fc_datan2d(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::datan2d(
        arg_f64(fcinfo, 0),
        arg_f64(fcinfo, 1),
    )?))
}
fn fc_dsind(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::dsind(arg_f64(fcinfo, 0))?))
}
fn fc_dcosd(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::dcosd(arg_f64(fcinfo, 0))?))
}
fn fc_dtand(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::dtand(arg_f64(fcinfo, 0))?))
}
fn fc_dcotd(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::dcotd(arg_f64(fcinfo, 0))?))
}

// ---- hyperbolic ----
fn fc_dsinh(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::dsinh(arg_f64(fcinfo, 0))))
}
fn fc_dcosh(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::dcosh(arg_f64(fcinfo, 0))?))
}
fn fc_dtanh(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::dtanh(arg_f64(fcinfo, 0))?))
}
fn fc_dasinh(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::dasinh(arg_f64(fcinfo, 0))))
}
fn fc_dacosh(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::dacosh(arg_f64(fcinfo, 0))?))
}
fn fc_datanh(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::datanh(arg_f64(fcinfo, 0))?))
}

// ---- erf / gamma ----
fn fc_derf(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::derf(arg_f64(fcinfo, 0))?))
}
fn fc_derfc(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::derfc(arg_f64(fcinfo, 0))?))
}
fn fc_dgamma(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::dgamma(arg_f64(fcinfo, 0))?))
}
fn fc_dlgamma(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::dlgamma(arg_f64(fcinfo, 0))?))
}

// ---- conversions ----
fn fc_i2tod(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::i2tod(arg_i16(fcinfo, 0))))
}
fn fc_i2tof(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f32(crate::i2tof(arg_i16(fcinfo, 0))))
}
fn fc_dtoi2(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_i16(crate::dtoi2(arg_f64(fcinfo, 0))?))
}
fn fc_ftoi2(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_i16(crate::ftoi2(arg_f32(fcinfo, 0))?))
}
fn fc_i4tod(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::i4tod(arg_i32(fcinfo, 0))))
}
fn fc_i4tof(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f32(crate::i4tof(arg_i32(fcinfo, 0))))
}
fn fc_dtoi4(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_i32(crate::dtoi4(arg_f64(fcinfo, 0))?))
}
fn fc_ftoi4(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_i32(crate::ftoi4(arg_f32(fcinfo, 0))?))
}
fn fc_ftod(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::ftod(arg_f32(fcinfo, 0))))
}
fn fc_dtof(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f32(crate::dtof(arg_f64(fcinfo, 0))?))
}

// ---- float48 (float4 op float8) arithmetic ----
fn fc_float48mul(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::float48mul(
        arg_f32(fcinfo, 0),
        arg_f64(fcinfo, 1),
    )?))
}
fn fc_float48div(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::float48div(
        arg_f32(fcinfo, 0),
        arg_f64(fcinfo, 1),
    )?))
}
fn fc_float48pl(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::float48pl(
        arg_f32(fcinfo, 0),
        arg_f64(fcinfo, 1),
    )?))
}
fn fc_float48mi(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::float48mi(
        arg_f32(fcinfo, 0),
        arg_f64(fcinfo, 1),
    )?))
}

// ---- float84 (float8 op float4) arithmetic ----
fn fc_float84mul(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::float84mul(
        arg_f64(fcinfo, 0),
        arg_f32(fcinfo, 1),
    )?))
}
fn fc_float84div(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::float84div(
        arg_f64(fcinfo, 0),
        arg_f32(fcinfo, 1),
    )?))
}
fn fc_float84pl(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::float84pl(
        arg_f64(fcinfo, 0),
        arg_f32(fcinfo, 1),
    )?))
}
fn fc_float84mi(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_f64(crate::float84mi(
        arg_f64(fcinfo, 0),
        arg_f32(fcinfo, 1),
    )?))
}

// ---- float48 comparisons ----
fn fc_float48eq(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::float48eq(arg_f32(fcinfo, 0), arg_f64(fcinfo, 1))))
}
fn fc_float48ne(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::float48ne(arg_f32(fcinfo, 0), arg_f64(fcinfo, 1))))
}
fn fc_float48lt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::float48lt(arg_f32(fcinfo, 0), arg_f64(fcinfo, 1))))
}
fn fc_float48le(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::float48le(arg_f32(fcinfo, 0), arg_f64(fcinfo, 1))))
}
fn fc_float48gt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::float48gt(arg_f32(fcinfo, 0), arg_f64(fcinfo, 1))))
}
fn fc_float48ge(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::float48ge(arg_f32(fcinfo, 0), arg_f64(fcinfo, 1))))
}

// ---- float84 comparisons ----
fn fc_float84eq(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::float84eq(arg_f64(fcinfo, 0), arg_f32(fcinfo, 1))))
}
fn fc_float84ne(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::float84ne(arg_f64(fcinfo, 0), arg_f32(fcinfo, 1))))
}
fn fc_float84lt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::float84lt(arg_f64(fcinfo, 0), arg_f32(fcinfo, 1))))
}
fn fc_float84le(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::float84le(arg_f64(fcinfo, 0), arg_f32(fcinfo, 1))))
}
fn fc_float84gt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::float84gt(arg_f64(fcinfo, 0), arg_f32(fcinfo, 1))))
}
fn fc_float84ge(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::float84ge(arg_f64(fcinfo, 0), arg_f32(fcinfo, 1))))
}

// ---- btree comparators ----
fn fc_btfloat4cmp(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_i32(crate::btfloat4cmp(arg_f32(fcinfo, 0), arg_f32(fcinfo, 1))))
}
fn fc_btfloat8cmp(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_i32(crate::btfloat8cmp(arg_f64(fcinfo, 0), arg_f64(fcinfo, 1))))
}
fn fc_btfloat48cmp(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_i32(crate::btfloat48cmp(arg_f32(fcinfo, 0), arg_f64(fcinfo, 1))))
}
fn fc_btfloat84cmp(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_i32(crate::btfloat84cmp(arg_f64(fcinfo, 0), arg_f32(fcinfo, 1))))
}

// ---- in_range / width_bucket ----
fn fc_in_range_float8_float8(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::in_range_float8_float8(
        arg_f64(fcinfo, 0),
        arg_f64(fcinfo, 1),
        arg_f64(fcinfo, 2),
        arg_bool(fcinfo, 3),
        arg_bool(fcinfo, 4),
    )?))
}
fn fc_in_range_float4_float8(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::in_range_float4_float8(
        arg_f32(fcinfo, 0),
        arg_f32(fcinfo, 1),
        arg_f64(fcinfo, 2),
        arg_bool(fcinfo, 3),
        arg_bool(fcinfo, 4),
    )?))
}
fn fc_width_bucket_float8(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_i32(crate::width_bucket_float8(
        arg_f64(fcinfo, 0),
        arg_f64(fcinfo, 1),
        arg_f64(fcinfo, 2),
        arg_i32(fcinfo, 3),
    )?))
}

// ---- aggregate transition / combine / final functions (float.c) ----
//
// The running state is a 3-element (variance) or 6-element (regression) float8[]
// ArrayType passed/returned on the by-ref lane. `arg_varlena(0)` reads the full
// image; the core deconstructs it and `construct_array` rebuilds a fresh image
// each call, written back raw via `ret_array_raw`. All are `proisstrict => 't'`,
// so the fmgr dispatcher already shortcuts a NULL input before reaching here.

fn fc_float8_accum(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = mcx::MemoryContext::new("float8_accum");
    let transarray = arg_varlena(fcinfo, 0);
    let newval = arg_f64(fcinfo, 1);
    let out = crate::float8_accum(m.mcx(), transarray, newval)?;
    Ok(ret_array_raw(fcinfo, out.as_slice().to_vec()))
}
fn fc_float4_accum(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = mcx::MemoryContext::new("float4_accum");
    let transarray = arg_varlena(fcinfo, 0);
    let newval = arg_f32(fcinfo, 1);
    let out = crate::float4_accum(m.mcx(), transarray, newval)?;
    Ok(ret_array_raw(fcinfo, out.as_slice().to_vec()))
}
fn fc_float8_combine(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = mcx::MemoryContext::new("float8_combine");
    let a = arg_varlena(fcinfo, 0);
    let b = arg_varlena(fcinfo, 1);
    let out = crate::float8_combine(m.mcx(), a, b)?;
    Ok(ret_array_raw(fcinfo, out.as_slice().to_vec()))
}
fn fc_float8_regr_accum(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = mcx::MemoryContext::new("float8_regr_accum");
    let transarray = arg_varlena(fcinfo, 0);
    let newval_y = arg_f64(fcinfo, 1);
    let newval_x = arg_f64(fcinfo, 2);
    let out =
        crate::float8_regr_accum(m.mcx(), transarray, newval_y, newval_x)?;
    Ok(ret_array_raw(fcinfo, out.as_slice().to_vec()))
}
fn fc_float8_regr_combine(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = mcx::MemoryContext::new("float8_regr_combine");
    let a = arg_varlena(fcinfo, 0);
    let b = arg_varlena(fcinfo, 1);
    let out = crate::float8_regr_combine(m.mcx(), a, b)?;
    Ok(ret_array_raw(fcinfo, out.as_slice().to_vec()))
}

macro_rules! fc_final {
    ($fc:ident, $core:ident) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
            let transarray = arg_varlena(fcinfo, 0);
            let v = crate::$core(transarray)?;
            Ok(ret_opt_f64(fcinfo, v))
        }
    };
}
fc_final!(fc_float8_avg, float8_avg);
fc_final!(fc_float8_var_pop, float8_var_pop);
fc_final!(fc_float8_var_samp, float8_var_samp);
fc_final!(fc_float8_stddev_pop, float8_stddev_pop);
fc_final!(fc_float8_stddev_samp, float8_stddev_samp);
fc_final!(fc_float8_regr_sxx, float8_regr_sxx);
fc_final!(fc_float8_regr_syy, float8_regr_syy);
fc_final!(fc_float8_regr_sxy, float8_regr_sxy);
fc_final!(fc_float8_regr_avgx, float8_regr_avgx);
fc_final!(fc_float8_regr_avgy, float8_regr_avgy);
fc_final!(fc_float8_regr_r2, float8_regr_r2);
fc_final!(fc_float8_regr_slope, float8_regr_slope);
fc_final!(fc_float8_regr_intercept, float8_regr_intercept);
fc_final!(fc_float8_covar_pop, float8_covar_pop);
fc_final!(fc_float8_covar_samp, float8_covar_samp);
fc_final!(fc_float8_corr, float8_corr);

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

/// Register every `float.c` builtin whose types cross the current fmgr
/// boundary (C: their `fmgr_builtins[]` rows). Called from this crate's
/// `init_seams()`. OIDs/nargs/strict/retset transcribed from `pg_proc.dat`
/// (all rows: `proisstrict => 't'`, none `proretset`).
pub fn register_float_builtins() {
    fmgr_core::register_builtins_native([
        // ---- I/O ----
        builtin(200, "float4in", 1, true, false, fc_float4in),
        builtin(201, "float4out", 1, true, false, fc_float4out),
        builtin(214, "float8in", 1, true, false, fc_float8in),
        builtin(215, "float8out", 1, true, false, fc_float8out),
        builtin(2425, "float4send", 1, true, false, fc_float4send),
        builtin(2427, "float8send", 1, true, false, fc_float8send),
        builtin(2424, "float4recv", 1, true, false, fc_float4recv),
        builtin(2426, "float8recv", 1, true, false, fc_float8recv),
        // ---- same-type arithmetic ----
        builtin(202, "float4mul", 2, true, false, fc_float4mul),
        builtin(203, "float4div", 2, true, false, fc_float4div),
        builtin(204, "float4pl", 2, true, false, fc_float4pl),
        builtin(205, "float4mi", 2, true, false, fc_float4mi),
        builtin(216, "float8mul", 2, true, false, fc_float8mul),
        builtin(217, "float8div", 2, true, false, fc_float8div),
        builtin(218, "float8pl", 2, true, false, fc_float8pl),
        builtin(219, "float8mi", 2, true, false, fc_float8mi),
        // ---- same-type comparisons ----
        builtin(287, "float4eq", 2, true, false, fc_float4eq),
        builtin(288, "float4ne", 2, true, false, fc_float4ne),
        builtin(289, "float4lt", 2, true, false, fc_float4lt),
        builtin(290, "float4le", 2, true, false, fc_float4le),
        builtin(291, "float4gt", 2, true, false, fc_float4gt),
        builtin(292, "float4ge", 2, true, false, fc_float4ge),
        builtin(293, "float8eq", 2, true, false, fc_float8eq),
        builtin(294, "float8ne", 2, true, false, fc_float8ne),
        builtin(295, "float8lt", 2, true, false, fc_float8lt),
        builtin(296, "float8le", 2, true, false, fc_float8le),
        builtin(297, "float8gt", 2, true, false, fc_float8gt),
        builtin(298, "float8ge", 2, true, false, fc_float8ge),
        // ---- unary float4 ----
        builtin(206, "float4um", 1, true, false, fc_float4um),
        builtin(207, "float4abs", 1, true, false, fc_float4abs),
        builtin(209, "float4larger", 2, true, false, fc_float4larger),
        builtin(211, "float4smaller", 2, true, false, fc_float4smaller),
        builtin(1913, "float4up", 1, true, false, fc_float4up),
        builtin(1394, "float4abs", 1, true, false, fc_float4abs),
        // ---- unary float8 ----
        builtin(220, "float8um", 1, true, false, fc_float8um),
        builtin(221, "float8abs", 1, true, false, fc_float8abs),
        builtin(223, "float8larger", 2, true, false, fc_float8larger),
        builtin(224, "float8smaller", 2, true, false, fc_float8smaller),
        builtin(1914, "float8up", 1, true, false, fc_float8up),
        builtin(1395, "float8abs", 1, true, false, fc_float8abs),
        // ---- math (float8) ----
        builtin(228, "dround", 1, true, false, fc_dround),
        builtin(229, "dtrunc", 1, true, false, fc_dtrunc),
        builtin(230, "dsqrt", 1, true, false, fc_dsqrt),
        builtin(231, "dcbrt", 1, true, false, fc_dcbrt),
        builtin(232, "dpow", 2, true, false, fc_dpow),
        builtin(233, "dexp", 1, true, false, fc_dexp),
        builtin(234, "dlog1", 1, true, false, fc_dlog1),
        builtin(1194, "dlog10", 1, true, false, fc_dlog10),
        builtin(1339, "dlog10", 1, true, false, fc_dlog10),
        builtin(1340, "dlog10", 1, true, false, fc_dlog10),
        builtin(1341, "dlog1", 1, true, false, fc_dlog1),
        builtin(1342, "dround", 1, true, false, fc_dround),
        builtin(1343, "dtrunc", 1, true, false, fc_dtrunc),
        builtin(1344, "dsqrt", 1, true, false, fc_dsqrt),
        builtin(1345, "dcbrt", 1, true, false, fc_dcbrt),
        builtin(1346, "dpow", 2, true, false, fc_dpow),
        builtin(1347, "dexp", 1, true, false, fc_dexp),
        builtin(1368, "dpow", 2, true, false, fc_dpow),
        builtin(2308, "dceil", 1, true, false, fc_dceil),
        builtin(2309, "dfloor", 1, true, false, fc_dfloor),
        builtin(2310, "dsign", 1, true, false, fc_dsign),
        builtin(2320, "dceil", 1, true, false, fc_dceil),
        // ---- trig (radians) ----
        builtin(1600, "dasin", 1, true, false, fc_dasin),
        builtin(1601, "dacos", 1, true, false, fc_dacos),
        builtin(1602, "datan", 1, true, false, fc_datan),
        builtin(1603, "datan2", 2, true, false, fc_datan2),
        builtin(1604, "dsin", 1, true, false, fc_dsin),
        builtin(1605, "dcos", 1, true, false, fc_dcos),
        builtin(1606, "dtan", 1, true, false, fc_dtan),
        builtin(1607, "dcot", 1, true, false, fc_dcot),
        builtin(1608, "degrees", 1, true, false, fc_degrees),
        builtin(1609, "radians", 1, true, false, fc_radians),
        builtin(1610, "dpi", 0, true, false, fc_dpi),
        // ---- trig (degrees) ----
        builtin(2731, "dasind", 1, true, false, fc_dasind),
        builtin(2732, "dacosd", 1, true, false, fc_dacosd),
        builtin(2733, "datand", 1, true, false, fc_datand),
        builtin(2734, "datan2d", 2, true, false, fc_datan2d),
        builtin(2735, "dsind", 1, true, false, fc_dsind),
        builtin(2736, "dcosd", 1, true, false, fc_dcosd),
        builtin(2737, "dtand", 1, true, false, fc_dtand),
        builtin(2738, "dcotd", 1, true, false, fc_dcotd),
        // ---- hyperbolic ----
        builtin(2462, "dsinh", 1, true, false, fc_dsinh),
        builtin(2463, "dcosh", 1, true, false, fc_dcosh),
        builtin(2464, "dtanh", 1, true, false, fc_dtanh),
        builtin(2465, "dasinh", 1, true, false, fc_dasinh),
        builtin(2466, "dacosh", 1, true, false, fc_dacosh),
        builtin(2467, "datanh", 1, true, false, fc_datanh),
        // ---- erf / gamma ----
        builtin(6219, "derf", 1, true, false, fc_derf),
        builtin(6220, "derfc", 1, true, false, fc_derfc),
        builtin(6383, "dgamma", 1, true, false, fc_dgamma),
        builtin(6384, "dlgamma", 1, true, false, fc_dlgamma),
        // ---- conversions ----
        builtin(235, "i2tod", 1, true, false, fc_i2tod),
        builtin(236, "i2tof", 1, true, false, fc_i2tof),
        builtin(237, "dtoi2", 1, true, false, fc_dtoi2),
        builtin(238, "ftoi2", 1, true, false, fc_ftoi2),
        builtin(311, "ftod", 1, true, false, fc_ftod),
        builtin(312, "dtof", 1, true, false, fc_dtof),
        builtin(316, "i4tod", 1, true, false, fc_i4tod),
        builtin(317, "dtoi4", 1, true, false, fc_dtoi4),
        builtin(318, "i4tof", 1, true, false, fc_i4tof),
        builtin(319, "ftoi4", 1, true, false, fc_ftoi4),
        // ---- float48 arithmetic ----
        builtin(279, "float48mul", 2, true, false, fc_float48mul),
        builtin(280, "float48div", 2, true, false, fc_float48div),
        builtin(281, "float48pl", 2, true, false, fc_float48pl),
        builtin(282, "float48mi", 2, true, false, fc_float48mi),
        // ---- float84 arithmetic ----
        builtin(283, "float84mul", 2, true, false, fc_float84mul),
        builtin(284, "float84div", 2, true, false, fc_float84div),
        builtin(285, "float84pl", 2, true, false, fc_float84pl),
        builtin(286, "float84mi", 2, true, false, fc_float84mi),
        // ---- float48 comparisons ----
        builtin(299, "float48eq", 2, true, false, fc_float48eq),
        builtin(300, "float48ne", 2, true, false, fc_float48ne),
        builtin(301, "float48lt", 2, true, false, fc_float48lt),
        builtin(302, "float48le", 2, true, false, fc_float48le),
        builtin(303, "float48gt", 2, true, false, fc_float48gt),
        builtin(304, "float48ge", 2, true, false, fc_float48ge),
        // ---- float84 comparisons ----
        builtin(305, "float84eq", 2, true, false, fc_float84eq),
        builtin(306, "float84ne", 2, true, false, fc_float84ne),
        builtin(307, "float84lt", 2, true, false, fc_float84lt),
        builtin(308, "float84le", 2, true, false, fc_float84le),
        builtin(309, "float84gt", 2, true, false, fc_float84gt),
        builtin(310, "float84ge", 2, true, false, fc_float84ge),
        // ---- btree comparators ----
        builtin(354, "btfloat4cmp", 2, true, false, fc_btfloat4cmp),
        builtin(355, "btfloat8cmp", 2, true, false, fc_btfloat8cmp),
        builtin(2194, "btfloat48cmp", 2, true, false, fc_btfloat48cmp),
        builtin(2195, "btfloat84cmp", 2, true, false, fc_btfloat84cmp),
        // ---- in_range / width_bucket ----
        builtin(4139, "in_range_float8_float8", 5, true, false, fc_in_range_float8_float8),
        builtin(4140, "in_range_float4_float8", 5, true, false, fc_in_range_float4_float8),
        builtin(320, "width_bucket_float8", 4, true, false, fc_width_bucket_float8),
        // ---- aggregate transition / combine / final functions ----
        builtin(208, "float4_accum", 2, true, false, fc_float4_accum),
        builtin(222, "float8_accum", 2, true, false, fc_float8_accum),
        builtin(276, "float8_combine", 2, true, false, fc_float8_combine),
        builtin(1830, "float8_avg", 1, true, false, fc_float8_avg),
        builtin(1831, "float8_var_samp", 1, true, false, fc_float8_var_samp),
        builtin(1832, "float8_stddev_samp", 1, true, false, fc_float8_stddev_samp),
        builtin(2512, "float8_var_pop", 1, true, false, fc_float8_var_pop),
        builtin(2513, "float8_stddev_pop", 1, true, false, fc_float8_stddev_pop),
        builtin(2806, "float8_regr_accum", 3, true, false, fc_float8_regr_accum),
        builtin(2807, "float8_regr_sxx", 1, true, false, fc_float8_regr_sxx),
        builtin(2808, "float8_regr_syy", 1, true, false, fc_float8_regr_syy),
        builtin(2809, "float8_regr_sxy", 1, true, false, fc_float8_regr_sxy),
        builtin(2810, "float8_regr_avgx", 1, true, false, fc_float8_regr_avgx),
        builtin(2811, "float8_regr_avgy", 1, true, false, fc_float8_regr_avgy),
        builtin(2812, "float8_regr_r2", 1, true, false, fc_float8_regr_r2),
        builtin(2813, "float8_regr_slope", 1, true, false, fc_float8_regr_slope),
        builtin(2814, "float8_regr_intercept", 1, true, false, fc_float8_regr_intercept),
        builtin(2815, "float8_covar_pop", 1, true, false, fc_float8_covar_pop),
        builtin(2816, "float8_covar_samp", 1, true, false, fc_float8_covar_samp),
        builtin(2817, "float8_corr", 1, true, false, fc_float8_corr),
        builtin(3342, "float8_regr_combine", 2, true, false, fc_float8_regr_combine),
    ]);
}
