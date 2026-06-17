//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! functions in `pseudorandomfuncs.c` whose argument/result types are
//! expressible at the current fmgr boundary (the scalar `float8` / `int4` /
//! `int8` ranges).
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core in [`crate`], and writes back the
//! result word. [`register_pseudorandomfuncs_builtins`] registers every row
//! into the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID dispatch
//! resolves them. OIDs / nargs / strict / retset are transcribed exactly from
//! `pg_proc.dat`.
//!
//! `numeric_random` (oid 6341) is NOT registered here: its arguments and result
//! are on-disk `Numeric` Datums, which are not expressible at the current
//! by-value fmgr boundary (the systemic fmgr/Datum `Numeric` deferral the crate
//! docs describe).

use types_datum::Datum;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_FLOAT8(i)` → `DatumGetFloat8`: the IEEE-754 bits of arg `i`'s word.
#[inline]
fn arg_f64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> f64 {
    fcinfo
        .arg(i)
        .expect("pseudorandom fn: missing arg")
        .value
        .as_f64()
}

/// `PG_GETARG_INT32(i)` → `DatumGetInt32`: the low 32 bits of arg `i`'s word.
#[inline]
fn arg_i32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo
        .arg(i)
        .expect("pseudorandom fn: missing arg")
        .value
        .as_i32()
}

/// `PG_GETARG_INT64(i)` → `DatumGetInt64`: arg `i`'s word as a 64-bit integer.
#[inline]
fn arg_i64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo
        .arg(i)
        .expect("pseudorandom fn: missing arg")
        .value
        .as_i64()
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
fn ret_i64(v: i64) -> Datum {
    Datum::from_i64(v)
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    let chars = types_error::unpack_sqlstate(err.sqlstate());
    let code = core::str::from_utf8(&chars).unwrap_or("XX000");
    std::panic::panic_any(format!("PGRUST-SQLSTATE:{code}:{}", err.message()));
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `drandom(PG_FUNCTION_ARGS)` — `random()` (no args), returns `float8`.
fn fc_drandom(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_f64(crate::drandom())
}

/// `drandom_normal(PG_FUNCTION_ARGS)` — `random_normal(mean, stddev)`,
/// returns `float8`.
fn fc_drandom_normal(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let mean = arg_f64(fcinfo, 0);
    let stddev = arg_f64(fcinfo, 1);
    ret_f64(crate::drandom_normal(mean, stddev))
}

/// `int4random(PG_FUNCTION_ARGS)` — `random(int4 min, int4 max)`, returns `int4`.
fn fc_int4random(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let rmin = arg_i32(fcinfo, 0);
    let rmax = arg_i32(fcinfo, 1);
    match crate::int4random(rmin, rmax) {
        Ok(v) => ret_i32(v),
        Err(e) => raise(e),
    }
}

/// `int8random(PG_FUNCTION_ARGS)` — `random(int8 min, int8 max)`, returns `int8`.
fn fc_int8random(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let rmin = arg_i64(fcinfo, 0);
    let rmax = arg_i64(fcinfo, 1);
    match crate::int8random(rmin, rmax) {
        Ok(v) => ret_i64(v),
        Err(e) => raise(e),
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

/// Register every scalar `pseudorandomfuncs.c` builtin (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
/// OIDs / nargs / strict / retset transcribed exactly from `pg_proc.dat`
/// (none set `proisstrict`/`proretset` explicitly, so all take the BKI
/// defaults: `proisstrict => 't'`, `proretset => 'f'`).
pub fn register_pseudorandomfuncs_builtins() {
    backend_utils_fmgr_core::register_builtins([
        builtin(1598, "drandom", 0, true, false, fc_drandom),
        builtin(6212, "drandom_normal", 2, true, false, fc_drandom_normal),
        builtin(6339, "int4random", 2, true, false, fc_int4random),
        builtin(6340, "int8random", 2, true, false, fc_int8random),
    ]);
}
