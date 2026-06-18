//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `numeric.c` functions whose argument/result types are expressible at the
//! current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word /
//! by-reference payload. [`register_numeric_builtins`] registers every row into
//! the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID dispatch
//! resolves them. OIDs / nargs / strict / retset are transcribed exactly from
//! `pg_proc.dat`.
//!
//! # The by-reference `numeric` convention
//!
//! `numeric` is a pass-by-reference (varlena) type. Its values cross the fmgr
//! boundary on the by-reference side channel: a `numeric` ARG arrives as
//! `fcinfo.ref_arg(i) == Some(RefPayload::Varlena(image))` and a `numeric`
//! RESULT is set via `fcinfo.set_ref_result(RefPayload::Varlena(image))`. The
//! bare by-value word is meaningless for these (it is the null/dummy word, exactly
//! as the canonical->ABI bridge `datum_to_ref_arg`/`ref_out_to_datum` in
//! fmgr-core arranges: a `ByRef` canonical Datum becomes `(null word,
//! Some(Varlena(bytes)))` and vice-versa).
//!
//! Unlike the `text`/`bytea` family (`backend-utils-adt-varlena`), which strips
//! the 4-byte varlena header at the boundary, the `image` here is the COMPLETE
//! numeric varlena byte image INCLUDING its `VARHDRSZ` header. That is the form
//! `numeric.c`'s codec produces and consumes: `set_var_from_num` /
//! `numeric_data_from_bytes` validate `image[0..4]` against
//! `SET_VARSIZE(image.len())`, and `make_result` writes the header into
//! `buf[..VARHDRSZ]`. The bridge carries `ByRef` bytes verbatim (no strip), so a
//! `numeric` `ByRef` value's bytes ARE its full varlena image, symmetric on the
//! arg and result lanes.

use std::cmp::Ordering;

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_NUMERIC(i)`: a `numeric` arg's full varlena byte image, read from
/// the by-reference side channel (the boundary carries it un-stripped).
#[inline]
fn arg_numeric<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("numeric fn: by-ref `numeric` arg missing from by-ref lane")
}

/// `PG_GETARG_CSTRING(i)`: the input cstring on the by-ref lane.
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("numeric fn: cstring arg missing from by-ref lane")
}

/// `PG_GETARG_INT32(i)`: the low 32 bits of arg `i`'s word, sign-extended.
#[inline]
fn arg_int32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.arg(i).expect("numeric fn: missing arg").value.as_i32()
}

/// `PG_GETARG_INT64(i)`: arg `i`'s full word as a signed 64-bit int.
#[inline]
fn arg_int64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo.arg(i).expect("numeric fn: missing arg").value.as_i64()
}

/// Set a `numeric` (by-reference) result on the by-ref lane and return the dummy
/// by-value word. The bytes are the full numeric varlena image (with header).
#[inline]
fn ret_numeric(fcinfo: &mut FunctionCallInfoBaseData, image: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    Datum::from_usize(0)
}

/// Set a `cstring` (`_out`) result on the by-ref lane and return the dummy word.
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}

#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}

#[inline]
fn ret_i32(v: i32) -> Datum {
    Datum::from_i32(v)
}

#[inline]
fn ret_i64(v: i64) -> Datum {
    Datum::from_i64(v)
}

/// A scratch context for cores that allocate their result through `Mcx`. The
/// resulting bytes are copied onto the by-ref lane before it is dropped (C: the
/// palloc'd result lives in the caller's context; here it crosses by value).
pub(crate) fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("numeric fmgr scratch")
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    std::panic::panic_any(err);
}

/// Unwrap a `PgResult`, re-raising its error through `raise`.
#[inline]
fn ok<T>(r: types_error::PgResult<T>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => raise(e),
    }
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `numeric_in(cstring, oid, int4) -> numeric` (oid 1701). The `typelem` oid arg
/// (arg 1) is unused by `numeric_in`, exactly as in C; the typmod is arg 2.
fn fc_numeric_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = arg_cstring(fcinfo, 0);
    let typmod = arg_int32(fcinfo, 2);
    let m = scratch_mcx();
    let image = ok(crate::io::numeric_in(m.mcx(), s, typmod));
    ret_numeric(fcinfo, image.as_slice().to_vec())
}

/// `numeric_out(numeric) -> cstring` (oid 1702).
fn fc_numeric_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = arg_numeric(fcinfo, 0);
    let m = scratch_mcx();
    let s = ok(crate::io::numeric_out(m.mcx(), num));
    ret_cstring(fcinfo, s)
}

/// Body of a unary `numeric -> numeric` builtin around a `fn(Mcx, &[u8]) ->
/// PgResult<PgVec<u8>>` core.
macro_rules! fc_unary_numeric {
    ($fc:ident, $core:path) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let num = arg_numeric(fcinfo, 0);
            let m = scratch_mcx();
            let image = ok($core(m.mcx(), num));
            ret_numeric(fcinfo, image.as_slice().to_vec())
        }
    };
}

fc_unary_numeric!(fc_numeric_abs, crate::ops_sql::numeric_abs);
fc_unary_numeric!(fc_numeric_uminus, crate::ops_sql::numeric_uminus);
fc_unary_numeric!(fc_numeric_uplus, crate::ops_sql::numeric_uplus);

/// Body of a binary `(numeric, numeric) -> numeric` builtin around a
/// `fn(Mcx, &[u8], &[u8]) -> PgResult<PgVec<u8>>` core.
macro_rules! fc_binary_numeric {
    ($fc:ident, $core:path) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let a = arg_numeric(fcinfo, 0);
            let b = arg_numeric(fcinfo, 1);
            let m = scratch_mcx();
            let image = ok($core(m.mcx(), a, b));
            ret_numeric(fcinfo, image.as_slice().to_vec())
        }
    };
}

fc_binary_numeric!(fc_numeric_add, crate::ops_sql::numeric_add);
fc_binary_numeric!(fc_numeric_sub, crate::ops_sql::numeric_sub);
fc_binary_numeric!(fc_numeric_mul, crate::ops_sql::numeric_mul);
fc_binary_numeric!(fc_numeric_div, crate::ops_sql::numeric_div);

/// Body of a binary `(numeric, numeric) -> bool` comparison builtin around a
/// `fn(&[u8], &[u8]) -> bool` (pure) core.
macro_rules! fc_cmp_bool {
    ($fc:ident, $core:path) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let a = arg_numeric(fcinfo, 0);
            let b = arg_numeric(fcinfo, 1);
            ret_bool($core(a, b))
        }
    };
}

fc_cmp_bool!(fc_numeric_eq, crate::ops_sql::numeric_eq);
fc_cmp_bool!(fc_numeric_ne, crate::ops_sql::numeric_ne);
fc_cmp_bool!(fc_numeric_lt, crate::ops_sql::numeric_lt);
fc_cmp_bool!(fc_numeric_le, crate::ops_sql::numeric_le);
fc_cmp_bool!(fc_numeric_gt, crate::ops_sql::numeric_gt);
fc_cmp_bool!(fc_numeric_ge, crate::ops_sql::numeric_ge);

/// `numeric_cmp(numeric, numeric) -> int4` (oid 1769): -1/0/1.
fn fc_numeric_cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = arg_numeric(fcinfo, 0);
    let b = arg_numeric(fcinfo, 1);
    let c = match crate::ops_sql::numeric_cmp(a, b) {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    };
    ret_i32(c)
}

/// `hash_numeric(numeric) -> int4` (oid 432).
fn fc_hash_numeric(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = arg_numeric(fcinfo, 0);
    // C: PG_RETURN_INT32 of a uint32 hash word (reinterpret, not numeric range).
    ret_i32(crate::aggregate::hash_numeric(num) as i32)
}

/// `hash_numeric_extended(numeric, int8) -> int8` (oid 780).
fn fc_hash_numeric_extended(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = arg_numeric(fcinfo, 0);
    let seed = arg_int64(fcinfo, 1) as u64;
    ret_i64(crate::aggregate::hash_numeric_extended(num, seed) as i64)
}

/// A `bytea`/`internal StringInfo` arg's raw byte payload, read from the by-ref
/// Varlena lane. For `numeric_recv` this is the binary-protocol message body
/// (the boundary delivers the header-less bytes).
#[inline]
fn arg_varlena<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("numeric fn: by-ref varlena arg missing from by-ref lane")
}

/// Set a `bytea` result (header-less payload) on the by-ref Varlena lane and
/// return the dummy by-value word.
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::from_usize(0)
}

// --- Unary numeric -> numeric (additional ported cores). ---
fc_unary_numeric!(fc_numeric_sign, crate::ops_sql::numeric_sign);
fc_unary_numeric!(fc_numeric_inc, crate::ops_sql::numeric_inc);
fc_unary_numeric!(fc_numeric_ceil, crate::ops_sql::numeric_ceil);
fc_unary_numeric!(fc_numeric_floor, crate::ops_sql::numeric_floor);
fc_unary_numeric!(fc_numeric_sqrt, crate::ops_sql::numeric_sqrt);
fc_unary_numeric!(fc_numeric_exp, crate::ops_sql::numeric_exp);
fc_unary_numeric!(fc_numeric_ln, crate::ops_sql::numeric_ln);
fc_unary_numeric!(fc_numeric_trim_scale, crate::ops_sql::numeric_trim_scale);

// --- Binary (numeric, numeric) -> numeric (additional ported cores). ---
fc_binary_numeric!(fc_numeric_mod, crate::ops_sql::numeric_mod);
fc_binary_numeric!(fc_numeric_div_trunc, crate::ops_sql::numeric_div_trunc);
fc_binary_numeric!(fc_numeric_log, crate::ops_sql::numeric_log);
fc_binary_numeric!(fc_numeric_power, crate::ops_sql::numeric_power);
fc_binary_numeric!(fc_numeric_gcd, crate::ops_sql::numeric_gcd);
fc_binary_numeric!(fc_numeric_lcm, crate::ops_sql::numeric_lcm);

/// Body of a `(numeric, int4) -> numeric` builtin (round/trunc) around a
/// `fn(Mcx, &[u8], i32) -> PgResult<PgVec<u8>>` core.
macro_rules! fc_numeric_scale_arg {
    ($fc:ident, $core:path) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let num = arg_numeric(fcinfo, 0);
            let scale = arg_int32(fcinfo, 1);
            let m = scratch_mcx();
            let image = ok($core(m.mcx(), num, scale));
            ret_numeric(fcinfo, image.as_slice().to_vec())
        }
    };
}

fc_numeric_scale_arg!(fc_numeric_round, crate::ops_sql::numeric_round);
fc_numeric_scale_arg!(fc_numeric_trunc, crate::ops_sql::numeric_trunc);
// `numeric(numeric, int4)` — the typmod-application length-coercion cast.
fc_numeric_scale_arg!(fc_numeric, crate::ops_sql::numeric);

/// `numeric_scale(numeric) -> int4` (oid 3281): the display scale. C returns
/// SQL NULL for special (NaN/Inf) inputs.
fn fc_numeric_scale(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = arg_numeric(fcinfo, 0);
    if types_numeric::numeric_is_special(num) {
        fcinfo.set_result_null(true);
        return Datum::from_usize(0);
    }
    ret_i32(ok(crate::ops_sql::numeric_scale(num)))
}

/// `width_bucket_numeric(numeric, numeric, numeric, int4) -> int4` (oid 2170).
/// The `count` arg is int4 in `pg_proc`; the core decodes it from a numeric byte
/// image, so re-encode the int4 to a numeric for the core call.
fn fc_width_bucket_numeric(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let operand = arg_numeric(fcinfo, 0);
    let bound1 = arg_numeric(fcinfo, 1);
    let bound2 = arg_numeric(fcinfo, 2);
    let count = arg_int32(fcinfo, 3);
    let m = scratch_mcx();
    let count_num = ok(crate::convert::int64_to_numeric(m.mcx(), count as i64));
    ret_i32(ok(crate::ops_sql::width_bucket_numeric(
        operand,
        bound1,
        bound2,
        count_num.as_slice(),
    )))
}

/// `in_range_numeric_numeric(numeric, numeric, numeric, bool, bool) -> bool`
/// (oid 4141): the window `RANGE` offset predicate.
fn fc_in_range_numeric_numeric(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let val = arg_numeric(fcinfo, 0);
    let base = arg_numeric(fcinfo, 1);
    let offset = arg_numeric(fcinfo, 2);
    let sub = fcinfo.arg(3).expect("missing arg").value.as_bool();
    let less = fcinfo.arg(4).expect("missing arg").value.as_bool();
    ret_bool(ok(crate::ops_sql::in_range_numeric_numeric(
        val, base, offset, sub, less,
    )))
}

/// `numeric_recv(internal, oid, int4) -> numeric` (oid 2460). Arg 0 is the
/// binary message buffer (StringInfo); arg 1 (typelem oid) is unused; arg 2 is
/// the typmod.
fn fc_numeric_recv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let buf = arg_varlena(fcinfo, 0);
    let typmod = arg_int32(fcinfo, 2);
    let m = scratch_mcx();
    let image = ok(crate::io::numeric_recv(m.mcx(), buf, typmod));
    ret_numeric(fcinfo, image.as_slice().to_vec())
}

/// `numeric_send(numeric) -> bytea` (oid 2461): binary wire form.
fn fc_numeric_send(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = arg_numeric(fcinfo, 0);
    let m = scratch_mcx();
    let bytes = ok(crate::io::numeric_send(m.mcx(), num));
    ret_varlena(fcinfo, bytes.as_slice().to_vec())
}

// ---------------------------------------------------------------------------
// Cross-type casts: int{2,4,8} <-> numeric and float{4,8} <-> numeric.
// (numeric.c int4_numeric/numeric_int4/int8_numeric/numeric_int8/
//  int2_numeric/numeric_int2/float8_numeric/numeric_float8/
//  float4_numeric/numeric_float4.)
// ---------------------------------------------------------------------------

/// `int4_numeric(int4) -> numeric` (oid 1740). C widens to int64 then calls
/// `int64_to_numeric`.
fn fc_int4_numeric(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let val = arg_int32(fcinfo, 0) as i64;
    let m = scratch_mcx();
    let image = ok(crate::convert::int64_to_numeric(m.mcx(), val));
    ret_numeric(fcinfo, image.as_slice().to_vec())
}

/// `int2_numeric(int2) -> numeric` (oid 1782).
fn fc_int2_numeric(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let val = fcinfo.arg(0).expect("missing arg").value.as_i16() as i64;
    let m = scratch_mcx();
    let image = ok(crate::convert::int64_to_numeric(m.mcx(), val));
    ret_numeric(fcinfo, image.as_slice().to_vec())
}

/// `int8_numeric(int8) -> numeric` (oid 1781).
fn fc_int8_numeric(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let val = arg_int64(fcinfo, 0);
    let m = scratch_mcx();
    let image = ok(crate::convert::int64_to_numeric(m.mcx(), val));
    ret_numeric(fcinfo, image.as_slice().to_vec())
}

/// `numeric_int4(numeric) -> int4` (oid 1744): round to nearest, range-checked.
fn fc_numeric_int4(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = arg_numeric(fcinfo, 0);
    ret_i32(ok(crate::ops_sql::seam_numeric_int4(num)))
}

/// `numeric_int2(numeric) -> int2` (oid 1783).
fn fc_numeric_int2(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = arg_numeric(fcinfo, 0);
    Datum::from_i16(ok(crate::ops_sql::seam_numeric_int2(num)))
}

/// `numeric_int8(numeric) -> int8` (oid 1779).
fn fc_numeric_int8(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = arg_numeric(fcinfo, 0);
    ret_i64(ok(crate::ops_sql::seam_numeric_int8(num)))
}

/// `float8_numeric(float8) -> numeric` (oid 1743).
fn fc_float8_numeric(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let val = fcinfo.arg(0).expect("missing arg").value.as_f64();
    let m = scratch_mcx();
    let image = ok(crate::convert::float8_to_numeric(m.mcx(), val));
    ret_numeric(fcinfo, image.as_slice().to_vec())
}

/// `float4_numeric(float4) -> numeric` (oid 1742). C widens `float4` to
/// `float8` before the decimal rendering (`float4_numeric` -> `(float8) val`).
fn fc_float4_numeric(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let val = fcinfo.arg(0).expect("missing arg").value.as_f32() as f64;
    let m = scratch_mcx();
    let image = ok(crate::convert::float8_to_numeric(m.mcx(), val));
    ret_numeric(fcinfo, image.as_slice().to_vec())
}

/// `numeric_float8(numeric) -> float8` (oid 1746).
fn fc_numeric_float8(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = arg_numeric(fcinfo, 0);
    Datum::from_f64(ok(crate::convert::numeric_to_float8(num)))
}

/// `numeric_float4(numeric) -> float4` (oid 1745).
fn fc_numeric_float4(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = arg_numeric(fcinfo, 0);
    Datum::from_f32(ok(crate::convert::numeric_to_float4(num)))
}

/// `numeric_fac(int8) -> numeric` (oid 1376): `factorial(int8)`.
fn fc_numeric_fac(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let n = arg_int64(fcinfo, 0);
    let m = scratch_mcx();
    let image = ok(crate::ops_sql::numeric_factorial(m.mcx(), n));
    ret_numeric(fcinfo, image.as_slice().to_vec())
}

/// `numeric_min_scale(numeric) -> int4` (oid 5042). C returns SQL NULL for a
/// special (NaN/Inf) input; a finite value yields its minimum representable
/// scale.
fn fc_numeric_min_scale(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = arg_numeric(fcinfo, 0);
    if types_numeric::numeric_is_special(num) {
        fcinfo.set_result_null(true);
        return Datum::from_usize(0);
    }
    ret_i32(crate::ops_sql::get_min_scale(num))
}

// ---------------------------------------------------------------------------
// Aggregate transition functions for sum(int2)/sum(int4)/sum(int8).
//
// These are NON-STRICT (`proisstrict => 'f'`): they receive the running
// transition value in arg 0 (which is NULL until the first non-null input) and
// the new input in arg 1 (which may be NULL). The strict-shim is therefore NOT
// applied; each adapter inspects `PG_ARGISNULL` itself.
//
// This is the 64-bit (`USE_FLOAT8_BYVAL`) build, so the `AggCheckCallContext`
// in-place leg of `int2_sum`/`int4_sum` in `numeric.c` is `#ifndef`'d out: int8
// is pass-by-value, the running sum cannot be modified through a pointer, and
// the function simply returns the new transition value.
// ---------------------------------------------------------------------------

/// `PG_ARGISNULL(i)`: whether arg `i` carries a NULL on the call frame.
#[inline]
fn arg_is_null(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo
        .arg(i)
        .map(|d| d.isnull)
        .unwrap_or(true)
}

/// `PG_RETURN_NULL()`: set the result-null flag and return a dummy word.
#[inline]
fn ret_null(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    fcinfo.set_result_null(true);
    Datum::from_usize(0)
}

/// `int2_sum(int8, int2) -> int8` (oid 1840). NON-STRICT aggregate transition
/// function for `sum(int2)`. The transtype is `int8`; arg 0 is the running sum
/// (NULL until the first non-null input), arg 1 the new `int2` input.
fn fc_int2_sum(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    if arg_is_null(fcinfo, 0) {
        // No non-null input seen so far...
        if arg_is_null(fcinfo, 1) {
            return ret_null(fcinfo); // still no non-null
        }
        // This is the first non-null input.
        let newval = arg_int32(fcinfo, 1) as i64; // PG_GETARG_INT16 widened
        return ret_i64(newval);
    }

    let oldsum = arg_int64(fcinfo, 0);

    // Leave sum unchanged if new input is null.
    if arg_is_null(fcinfo, 1) {
        return ret_i64(oldsum);
    }

    // OK to do the addition. (int2 arg is delivered on the by-val word, low
    // bits sign-extended; read it as i32 and widen, matching PG_GETARG_INT16.)
    let newval = oldsum + arg_int32(fcinfo, 1) as i64;
    ret_i64(newval)
}

/// `int4_sum(int8, int4) -> int8` (oid 1841). NON-STRICT aggregate transition
/// function for `sum(int4)`. Same shape as `int2_sum` with an `int4` input.
fn fc_int4_sum(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    if arg_is_null(fcinfo, 0) {
        if arg_is_null(fcinfo, 1) {
            return ret_null(fcinfo);
        }
        let newval = arg_int32(fcinfo, 1) as i64;
        return ret_i64(newval);
    }

    let oldsum = arg_int64(fcinfo, 0);

    if arg_is_null(fcinfo, 1) {
        return ret_i64(oldsum);
    }

    let newval = oldsum + arg_int32(fcinfo, 1) as i64;
    ret_i64(newval)
}

/// `int8_sum(numeric, int8) -> numeric` (oid 1842). NON-STRICT aggregate
/// transition function. (Obsolete; no longer used for `sum(int8)`, but still a
/// registered builtin.) The transtype is `numeric`; arg 0 is the running sum
/// (NULL until the first non-null input), arg 1 the new `int8` input.
fn fc_int8_sum(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    if arg_is_null(fcinfo, 0) {
        if arg_is_null(fcinfo, 1) {
            return ret_null(fcinfo);
        }
        // First non-null input: int64_to_numeric(PG_GETARG_INT64(1)).
        let m = scratch_mcx();
        let image = ok(crate::convert::int64_to_numeric(m.mcx(), arg_int64(fcinfo, 1)));
        return ret_numeric(fcinfo, image.as_slice().to_vec());
    }

    let oldsum = arg_numeric(fcinfo, 0).to_vec();

    // Leave sum unchanged if new input is null.
    if arg_is_null(fcinfo, 1) {
        return ret_numeric(fcinfo, oldsum);
    }

    // numeric_add(oldsum, int64_to_numeric(PG_GETARG_INT64(1))).
    let m = scratch_mcx();
    let addend = ok(crate::convert::int64_to_numeric(m.mcx(), arg_int64(fcinfo, 1)));
    let sum = ok(crate::ops_sql::numeric_add(m.mcx(), &oldsum, addend.as_slice()));
    let image = sum.as_slice().to_vec();
    ret_numeric(fcinfo, image)
}

// ---------------------------------------------------------------------------
// avg(int2)/avg(int4) array transition + final functions (Int8TransTypeData).
// The transition value is an int8[2] {count, sum} array crossing the fmgr
// boundary on the by-reference (Varlena) lane; the input is by-value int2/int4.
// These are STRICT (no `proisstrict => 'f'` in pg_proc.dat), so the strict-shim
// short-circuits NULL args upstream — each adapter sees both args present.
// ---------------------------------------------------------------------------

/// `int2_avg_accum(_int8, int2) -> _int8` (oid 1962).
fn fc_int2_avg_accum(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let transarray = arg_varlena(fcinfo, 0).to_vec();
    let newval = fcinfo.arg(1).expect("missing arg").value.as_i16();
    let m = scratch_mcx();
    let out = ok(crate::aggregate::int2_avg_accum(m.mcx(), &transarray, newval));
    ret_varlena(fcinfo, out.as_slice().to_vec())
}

/// `int4_avg_accum(_int8, int4) -> _int8` (oid 1963).
fn fc_int4_avg_accum(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let transarray = arg_varlena(fcinfo, 0).to_vec();
    let newval = arg_int32(fcinfo, 1);
    let m = scratch_mcx();
    let out = ok(crate::aggregate::int4_avg_accum(m.mcx(), &transarray, newval));
    ret_varlena(fcinfo, out.as_slice().to_vec())
}

/// `int4_avg_combine(_int8, _int8) -> _int8` (oid 3324). Shared by
/// avg(int2)/avg(int4).
fn fc_int4_avg_combine(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let t1 = arg_varlena(fcinfo, 0).to_vec();
    let t2 = arg_varlena(fcinfo, 1).to_vec();
    let m = scratch_mcx();
    let out = ok(crate::aggregate::int4_avg_combine(m.mcx(), &t1, &t2));
    ret_varlena(fcinfo, out.as_slice().to_vec())
}

/// `int2_avg_accum_inv(_int8, int2) -> _int8` (oid 3570).
fn fc_int2_avg_accum_inv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let transarray = arg_varlena(fcinfo, 0).to_vec();
    let newval = fcinfo.arg(1).expect("missing arg").value.as_i16();
    let m = scratch_mcx();
    let out = ok(crate::aggregate::int2_avg_accum_inv(m.mcx(), &transarray, newval));
    ret_varlena(fcinfo, out.as_slice().to_vec())
}

/// `int4_avg_accum_inv(_int8, int4) -> _int8` (oid 3571).
fn fc_int4_avg_accum_inv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let transarray = arg_varlena(fcinfo, 0).to_vec();
    let newval = arg_int32(fcinfo, 1);
    let m = scratch_mcx();
    let out = ok(crate::aggregate::int4_avg_accum_inv(m.mcx(), &transarray, newval));
    ret_varlena(fcinfo, out.as_slice().to_vec())
}

/// `int8_avg(_int8) -> numeric` (oid 1964): AVG(int2)/AVG(int4) final.
fn fc_int8_avg(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let transarray = arg_varlena(fcinfo, 0).to_vec();
    let m = scratch_mcx();
    let image = ok(crate::aggregate::int8_avg(m.mcx(), &transarray)).map(|v| v.as_slice().to_vec());
    match image {
        Some(image) => ret_numeric(fcinfo, image),
        None => ret_null(fcinfo),
    }
}

/// `int2int4_sum(_int8) -> int8` (oid 3572): SUM(int2)/SUM(int4) final in
/// moving-aggregate mode (both return int8).
fn fc_int2int4_sum(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let transarray = arg_varlena(fcinfo, 0).to_vec();
    match ok(crate::aggregate::int2int4_sum(&transarray)) {
        Some(sum) => ret_i64(sum),
        None => ret_null(fcinfo),
    }
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

pub(crate) fn builtin(
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

/// Register every expressible scalar `numeric.c` builtin (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
/// OIDs/nargs/strict/retset transcribed exactly from `pg_proc.dat`
/// (all of these are `proisstrict => 't'` default and none `proretset`).
pub fn register_numeric_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // I/O: cstring <-> numeric.
        builtin(1701, "numeric_in", 3, true, false, fc_numeric_in),
        builtin(1702, "numeric_out", 1, true, false, fc_numeric_out),
        // Length coercion (typmod application): numeric(numeric, int4) -> numeric.
        builtin(1703, "numeric", 2, true, false, fc_numeric),
        // Unary numeric -> numeric.
        builtin(1704, "numeric_abs", 1, true, false, fc_numeric_abs),
        builtin(1771, "numeric_uminus", 1, true, false, fc_numeric_uminus),
        builtin(1915, "numeric_uplus", 1, true, false, fc_numeric_uplus),
        // Binary numeric arithmetic -> numeric.
        builtin(1724, "numeric_add", 2, true, false, fc_numeric_add),
        builtin(1725, "numeric_sub", 2, true, false, fc_numeric_sub),
        builtin(1726, "numeric_mul", 2, true, false, fc_numeric_mul),
        builtin(1727, "numeric_div", 2, true, false, fc_numeric_div),
        // Comparison -> bool.
        builtin(1718, "numeric_eq", 2, true, false, fc_numeric_eq),
        builtin(1719, "numeric_ne", 2, true, false, fc_numeric_ne),
        builtin(1722, "numeric_lt", 2, true, false, fc_numeric_lt),
        builtin(1723, "numeric_le", 2, true, false, fc_numeric_le),
        builtin(1720, "numeric_gt", 2, true, false, fc_numeric_gt),
        builtin(1721, "numeric_ge", 2, true, false, fc_numeric_ge),
        // 3-way comparison -> int4.
        builtin(1769, "numeric_cmp", 2, true, false, fc_numeric_cmp),
        // Hash.
        builtin(432, "hash_numeric", 1, true, false, fc_hash_numeric),
        builtin(
            780,
            "hash_numeric_extended",
            2,
            true,
            false,
            fc_hash_numeric_extended,
        ),
        // typmod in/out.
        builtin(2917, "numerictypmodin", 1, true, false, fc_numerictypmodin),
        builtin(2918, "numerictypmodout", 1, true, false, fc_numerictypmodout),
        // Additional unary numeric -> numeric (each distinct pg_proc OID).
        builtin(1705, "numeric_abs", 1, true, false, fc_numeric_abs),
        builtin(1706, "numeric_sign", 1, true, false, fc_numeric_sign),
        builtin(1764, "numeric_inc", 1, true, false, fc_numeric_inc),
        builtin(1711, "numeric_ceil", 1, true, false, fc_numeric_ceil),
        builtin(2167, "numeric_ceil", 1, true, false, fc_numeric_ceil),
        builtin(1712, "numeric_floor", 1, true, false, fc_numeric_floor),
        builtin(1730, "numeric_sqrt", 1, true, false, fc_numeric_sqrt),
        builtin(1731, "numeric_sqrt", 1, true, false, fc_numeric_sqrt),
        builtin(1732, "numeric_exp", 1, true, false, fc_numeric_exp),
        builtin(1733, "numeric_exp", 1, true, false, fc_numeric_exp),
        builtin(1734, "numeric_ln", 1, true, false, fc_numeric_ln),
        builtin(1735, "numeric_ln", 1, true, false, fc_numeric_ln),
        builtin(5043, "numeric_trim_scale", 1, true, false, fc_numeric_trim_scale),
        // (numeric, int4) -> numeric.
        builtin(1707, "numeric_round", 2, true, false, fc_numeric_round),
        builtin(1709, "numeric_trunc", 2, true, false, fc_numeric_trunc),
        // Binary (numeric, numeric) -> numeric (each distinct pg_proc OID).
        builtin(1728, "numeric_mod", 2, true, false, fc_numeric_mod),
        builtin(1729, "numeric_mod", 2, true, false, fc_numeric_mod),
        builtin(1973, "numeric_div_trunc", 2, true, false, fc_numeric_div_trunc),
        builtin(1980, "numeric_div_trunc", 2, true, false, fc_numeric_div_trunc),
        builtin(1736, "numeric_log", 2, true, false, fc_numeric_log),
        builtin(1737, "numeric_log", 2, true, false, fc_numeric_log),
        builtin(1738, "numeric_power", 2, true, false, fc_numeric_power),
        builtin(1739, "numeric_power", 2, true, false, fc_numeric_power),
        builtin(2169, "numeric_power", 2, true, false, fc_numeric_power),
        builtin(5048, "numeric_gcd", 2, true, false, fc_numeric_gcd),
        builtin(5049, "numeric_lcm", 2, true, false, fc_numeric_lcm),
        // numeric_scale (numeric) -> int4.
        builtin(3281, "numeric_scale", 1, true, false, fc_numeric_scale),
        // width_bucket_numeric (numeric, numeric, numeric, int4) -> int4.
        builtin(2170, "width_bucket_numeric", 4, true, false, fc_width_bucket_numeric),
        // in_range_numeric_numeric (numeric, numeric, numeric, bool, bool) -> bool.
        builtin(4141, "in_range_numeric_numeric", 5, true, false, fc_in_range_numeric_numeric),
        // recv/send.
        builtin(2460, "numeric_recv", 3, true, false, fc_numeric_recv),
        builtin(2461, "numeric_send", 1, true, false, fc_numeric_send),
        // NON-STRICT aggregate transition functions (sum(int2/int4/int8)).
        builtin(1840, "int2_sum", 2, false, false, fc_int2_sum),
        builtin(1841, "int4_sum", 2, false, false, fc_int4_sum),
        builtin(1842, "int8_sum", 2, false, false, fc_int8_sum),
        // avg(int2)/avg(int4) int8[2] {count,sum} array transition + finals.
        // STRICT (per pg_proc.dat).
        builtin(1962, "int2_avg_accum", 2, true, false, fc_int2_avg_accum),
        builtin(1963, "int4_avg_accum", 2, true, false, fc_int4_avg_accum),
        builtin(3324, "int4_avg_combine", 2, true, false, fc_int4_avg_combine),
        builtin(3570, "int2_avg_accum_inv", 2, true, false, fc_int2_avg_accum_inv),
        builtin(3571, "int4_avg_accum_inv", 2, true, false, fc_int4_avg_accum_inv),
        builtin(1964, "int8_avg", 1, true, false, fc_int8_avg),
        builtin(3572, "int2int4_sum", 1, true, false, fc_int2int4_sum),
        // Cross-type casts int{2,4,8} <-> numeric.
        builtin(1740, "int4_numeric", 1, true, false, fc_int4_numeric),
        builtin(1782, "int2_numeric", 1, true, false, fc_int2_numeric),
        builtin(1781, "int8_numeric", 1, true, false, fc_int8_numeric),
        builtin(1744, "numeric_int4", 1, true, false, fc_numeric_int4),
        builtin(1783, "numeric_int2", 1, true, false, fc_numeric_int2),
        builtin(1779, "numeric_int8", 1, true, false, fc_numeric_int8),
        // Cross-type casts float{4,8} <-> numeric.
        builtin(1743, "float8_numeric", 1, true, false, fc_float8_numeric),
        builtin(1742, "float4_numeric", 1, true, false, fc_float4_numeric),
        builtin(1746, "numeric_float8", 1, true, false, fc_numeric_float8),
        builtin(1745, "numeric_float4", 1, true, false, fc_numeric_float4),
        // factorial(int8) and min_scale(numeric).
        builtin(1376, "numeric_fac", 1, true, false, fc_numeric_fac),
        builtin(5042, "numeric_min_scale", 1, true, false, fc_numeric_min_scale),
    ]);
}

/// `numerictypmodin(cstring[]) -> int4` (oid 2917): arg 0 is the typmod array's
/// varlena image on the by-reference lane, decoded via ArrayGetIntegerTypmods.
fn fc_numerictypmodin(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let ta = arg_varlena(fcinfo, 0);
    let m = scratch_mcx();
    let tl = match backend_utils_adt_arrayutils_seams::array_get_integer_typmods::call(m.mcx(), ta)
    {
        Ok(tl) => tl,
        Err(e) => raise(e),
    };
    match crate::ops_sql::numerictypmodin(&tl) {
        Ok(t) => ret_i32(t),
        Err(e) => raise(e),
    }
}

/// `numerictypmodout(int4) -> cstring` (oid 2918): the typmod output function,
/// producing "(prec,scale)" or "". The core allocates a NUL-terminated cstring
/// byte buffer through `Mcx`; we strip the trailing NUL and decode to a `String`
/// for the by-ref `cstring` lane.
fn fc_numerictypmodout(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let typmod = arg_int32(fcinfo, 0);
    let m = scratch_mcx();
    let s = match crate::ops_sql::numerictypmodout(m.mcx(), typmod) {
        Ok(bytes) => {
            // Drop the trailing NUL terminator produced by PG_RETURN_CSTRING.
            let raw = bytes.as_slice();
            let body = raw.strip_suffix(&[0u8]).unwrap_or(raw);
            String::from_utf8_lossy(body).into_owned()
        }
        Err(e) => raise(e),
    };
    ret_cstring(fcinfo, s)
}

// ===========================================================================
// End-to-end proof: a by-reference `numeric` builtin is genuinely callable
// through the fmgr registry.
// ===========================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use types_datum::NullableDatum;
    use types_fmgr::FunctionCallInfoBaseData;

    /// Build a fresh numeric varlena image from its decimal text via the
    /// registered `numeric_in` path (proving the in-function too).
    fn numeric_image(s: &str) -> Vec<u8> {
        register_numeric_builtins();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 3, 0, None, None);
        fcinfo.args = vec![
            NullableDatum::value(Datum::null()),   // cstring (by-ref)
            NullableDatum::value(Datum::from_u32(0)), // typelem oid (unused)
            NullableDatum::value(Datum::from_i32(-1)), // typmod = -1
        ];
        fcinfo.ref_args = vec![Some(RefPayload::Cstring(s.to_string())), None, None];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(1701)
            .expect("numeric_in registered");
        (entry.func.unwrap())(&mut fcinfo);
        match fcinfo.take_ref_result().expect("numeric_in produced a result") {
            RefPayload::Varlena(b) => b,
            other => panic!("numeric_in: unexpected result lane {other:?}"),
        }
    }

    /// Invoke a registered by-ref numeric builtin by OID through the fmgr
    /// registry, passing `numeric` args on the by-ref lane and reading the
    /// `numeric` result back off the by-ref lane.
    fn call_binary_numeric(oid: u32, a: &[u8], b: &[u8]) -> Vec<u8> {
        register_numeric_builtins();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 2, 0, None, None);
        fcinfo.args = vec![
            NullableDatum::value(Datum::null()),
            NullableDatum::value(Datum::null()),
        ];
        fcinfo.ref_args = vec![
            Some(RefPayload::Varlena(a.to_vec())),
            Some(RefPayload::Varlena(b.to_vec())),
        ];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(oid)
            .expect("builtin registered");
        (entry.func.unwrap())(&mut fcinfo);
        match fcinfo.take_ref_result().expect("numeric op produced a result") {
            RefPayload::Varlena(b) => b,
            other => panic!("numeric op: unexpected result lane {other:?}"),
        }
    }

    fn call_cmp_bool(oid: u32, a: &[u8], b: &[u8]) -> bool {
        register_numeric_builtins();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 2, 0, None, None);
        fcinfo.args = vec![
            NullableDatum::value(Datum::null()),
            NullableDatum::value(Datum::null()),
        ];
        fcinfo.ref_args = vec![
            Some(RefPayload::Varlena(a.to_vec())),
            Some(RefPayload::Varlena(b.to_vec())),
        ];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(oid)
            .expect("builtin registered");
        let d = (entry.func.unwrap())(&mut fcinfo);
        d.as_bool()
    }

    /// Render a numeric image back to text through the registered `numeric_out`.
    fn numeric_text(image: &[u8]) -> String {
        register_numeric_builtins();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(image.to_vec()))];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(1702)
            .expect("numeric_out registered");
        (entry.func.unwrap())(&mut fcinfo);
        match fcinfo.take_ref_result().expect("numeric_out produced a result") {
            RefPayload::Cstring(s) => s,
            other => panic!("numeric_out: unexpected result lane {other:?}"),
        }
    }

    /// THE PROOF: `1::numeric + 2::numeric == 3::numeric`, computed entirely
    /// through the fmgr registry by OID, with `numeric` args/result crossing on
    /// the by-reference lane.
    #[test]
    fn byref_numeric_add_through_registry() {
        let one = numeric_image("1");
        let two = numeric_image("2");
        // numeric_add oid 1724.
        let sum = call_binary_numeric(1724, &one, &two);
        assert_eq!(numeric_text(&sum), "3");
    }

    #[test]
    fn byref_numeric_arithmetic_and_compare() {
        let six = numeric_image("6");
        let four = numeric_image("4");
        // 6 - 4 = 2, 6 * 4 = 24, 6 / 4 = 1.5 (full dscale per numeric_div).
        assert_eq!(numeric_text(&call_binary_numeric(1725, &six, &four)), "2");
        assert_eq!(numeric_text(&call_binary_numeric(1726, &six, &four)), "24");
        assert_eq!(
            numeric_text(&call_binary_numeric(1727, &six, &four)),
            "1.5000000000000000"
        );
        // Comparisons (oids 1718 eq, 1722 lt, 1720 gt).
        assert!(!call_cmp_bool(1718, &six, &four)); // 6 == 4 -> false
        assert!(!call_cmp_bool(1722, &six, &four)); // 6 < 4  -> false
        assert!(call_cmp_bool(1720, &six, &four)); // 6 > 4  -> true
        assert!(call_cmp_bool(1718, &six, &six)); // 6 == 6 -> true
    }

    #[test]
    fn byref_numeric_unary_and_cmp_int() {
        let neg = numeric_image("-7");
        // numeric_abs oid 1704 -> 7; numeric_uminus oid 1771 -> 7.
        assert_eq!(numeric_text(&call_unary(1704, &neg)), "7");
        assert_eq!(numeric_text(&call_unary(1771, &neg)), "7");

        // numeric_cmp oid 1769: cmp(-7, 7) = -1, cmp(7,7)=0, cmp(7,-7)=1.
        let pos = numeric_image("7");
        assert_eq!(call_cmp_i32(1769, &neg, &pos), -1);
        assert_eq!(call_cmp_i32(1769, &pos, &pos), 0);
        assert_eq!(call_cmp_i32(1769, &pos, &neg), 1);
    }

    fn call_unary(oid: u32, a: &[u8]) -> Vec<u8> {
        register_numeric_builtins();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(a.to_vec()))];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(oid)
            .expect("builtin registered");
        (entry.func.unwrap())(&mut fcinfo);
        match fcinfo.take_ref_result().expect("unary numeric produced a result") {
            RefPayload::Varlena(b) => b,
            other => panic!("unary numeric: unexpected result lane {other:?}"),
        }
    }

    fn call_cmp_i32(oid: u32, a: &[u8], b: &[u8]) -> i32 {
        register_numeric_builtins();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 2, 0, None, None);
        fcinfo.args = vec![
            NullableDatum::value(Datum::null()),
            NullableDatum::value(Datum::null()),
        ];
        fcinfo.ref_args = vec![
            Some(RefPayload::Varlena(a.to_vec())),
            Some(RefPayload::Varlena(b.to_vec())),
        ];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(oid)
            .expect("builtin registered");
        let d = (entry.func.unwrap())(&mut fcinfo);
        d.as_i32()
    }
}
