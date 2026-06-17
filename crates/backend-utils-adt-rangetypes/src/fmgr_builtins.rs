//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the `rangetypes.c`
//! SQL-callable functions whose argument/result types are expressible at the
//! current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core (in
//! [`crate::range_canonical_subdiff_hash`]), and writes back the result word.
//! [`register_rangetypes_builtins`] registers every row into the fmgr-core
//! builtin table (C: `fmgr_builtins[]`), so by-OID dispatch resolves them.
//! OIDs / nargs / strict / retset are transcribed exactly from `pg_proc.dat`.
//!
//! Currently the two `*_subdiff` opclass support functions over scalar
//! `int4`/`int8` args returning `float8` are registered. Their cores take plain
//! `i32`/`i64` and return `f64`; the `numrange`/`daterange`/`tsrange` subdiff
//! variants are NOT registered here — those ride a different (Datum-seam or
//! by-ref) arg surface and belong to their own crates' builtin layers.

use types_datum::Datum;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_INT32(i)` → `DatumGetInt32`: the low 32 bits of arg `i`'s word.
#[inline]
fn arg_int32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo
        .arg(i)
        .expect("rangetypes fn: missing arg")
        .value
        .as_i32()
}

/// `PG_GETARG_INT64(i)` → `DatumGetInt64`: arg `i`'s word as a signed 64-bit int.
#[inline]
fn arg_int64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo
        .arg(i)
        .expect("rangetypes fn: missing arg")
        .value
        .as_i64()
}

/// `PG_RETURN_FLOAT8(v)` → `Float8GetDatum`: the IEEE-754 bits in the word.
#[inline]
fn ret_float8(v: f64) -> Datum {
    Datum::from_f64(v)
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `int4range_subdiff(int4, int4) -> float8` (rangetypes.c:1685).
fn fc_int4range_subdiff(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let v1 = arg_int32(fcinfo, 0);
    let v2 = arg_int32(fcinfo, 1);
    ret_float8(crate::range_canonical_subdiff_hash::int4range_subdiff(
        v1, v2,
    ))
}

/// `int8range_subdiff(int8, int8) -> float8` (rangetypes.c:1693).
fn fc_int8range_subdiff(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let v1 = arg_int64(fcinfo, 0);
    let v2 = arg_int64(fcinfo, 1);
    ret_float8(crate::range_canonical_subdiff_hash::int8range_subdiff(
        v1, v2,
    ))
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

/// Register the scalar `rangetypes.c` builtins (C: their `fmgr_builtins[]`
/// rows). Called from this crate's `init_seams()`. OIDs/nargs from
/// `pg_proc.dat`; both rows take the `proisstrict` default (`t`) and are not
/// `proretset`.
pub fn register_rangetypes_builtins() {
    backend_utils_fmgr_core::register_builtins([
        builtin(3922, "int4range_subdiff", 2, true, false, fc_int4range_subdiff),
        builtin(3923, "int8range_subdiff", 2, true, false, fc_int8range_subdiff),
    ]);
}
