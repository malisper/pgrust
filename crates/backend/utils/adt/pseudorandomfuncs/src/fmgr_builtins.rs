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
//! `numeric_random` (oid 6341, SQL name `random`) takes two on-disk `numeric`
//! Datums and returns one; like the rest of the `numeric.c` family these cross
//! the fmgr boundary as header-ful varlena images on the by-ref
//! `RefPayload::Varlena` lane (the established `numeric` convention), so it IS
//! registered here.

use datum::Datum;
use types_error::PgResult;
use fmgr::boundary::RefPayload;
use fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

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

/// `PG_GETARG_NUMERIC(i)`: the full header-ful `numeric` varlena image on the
/// by-ref lane (the `numeric` core reads from `VARHDRSZ`; it crosses verbatim).
#[inline]
fn arg_numeric(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Vec<u8> {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("pseudorandom fn: by-ref `numeric` arg missing from by-ref lane")
        .to_vec()
}

/// `PG_RETURN_NUMERIC(image)`: set the header-ful `numeric` result on the by-ref
/// lane and return the dummy by-value word.
#[inline]
fn ret_numeric(fcinfo: &mut FunctionCallInfoBaseData, image: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    Datum::from_usize(0)
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

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `drandom(PG_FUNCTION_ARGS)` — `random()` (no args), returns `float8`.
fn fc_drandom(_fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(ret_f64(crate::drandom()))
}

/// `drandom_normal(PG_FUNCTION_ARGS)` — `random_normal(mean, stddev)`,
/// returns `float8`.
fn fc_drandom_normal(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let mean = arg_f64(fcinfo, 0);
    let stddev = arg_f64(fcinfo, 1);
    Ok(ret_f64(crate::drandom_normal(mean, stddev)))
}

/// `int4random(PG_FUNCTION_ARGS)` — `random(int4 min, int4 max)`, returns `int4`.
fn fc_int4random(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let rmin = arg_i32(fcinfo, 0);
    let rmax = arg_i32(fcinfo, 1);
    Ok(ret_i32(crate::int4random(rmin, rmax)?))
}

/// `setseed(PG_FUNCTION_ARGS)` — `setseed(float8)`, returns `void`.
///
/// C reads `PG_GETARG_FLOAT8(0)`, calls `setseed`, then `PG_RETURN_VOID()`
/// (which is `(Datum) 0`).
fn fc_setseed(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let seed = arg_f64(fcinfo, 0);
    crate::setseed(seed)?;
    Ok(Datum::from_usize(0))
}

/// `int8random(PG_FUNCTION_ARGS)` — `random(int8 min, int8 max)`, returns `int8`.
fn fc_int8random(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let rmin = arg_i64(fcinfo, 0);
    let rmax = arg_i64(fcinfo, 1);
    Ok(ret_i64(crate::int8random(rmin, rmax)?))
}

/// `numeric_random(PG_FUNCTION_ARGS)` — `random(numeric min, numeric max)`,
/// returns a `numeric`. The bounds and result cross as header-ful varlena
/// images on the by-ref lane.
fn fc_numeric_random(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let rmin = arg_numeric(fcinfo, 0);
    let rmax = arg_numeric(fcinfo, 1);
    let m = mcx::MemoryContext::new("numeric_random fmgr scratch");
    let image = crate::numeric_random(m.mcx(), &rmin, &rmax)?;
    Ok(ret_numeric(fcinfo, image.to_vec()))
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

/// Register every scalar `pseudorandomfuncs.c` builtin (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
/// OIDs / nargs / strict / retset transcribed exactly from `pg_proc.dat`
/// (none set `proisstrict`/`proretset` explicitly, so all take the BKI
/// defaults: `proisstrict => 't'`, `proretset => 'f'`).
pub fn register_pseudorandomfuncs_builtins() {
    fmgr_core::register_builtins_native([
        builtin(1598, "drandom", 0, true, false, fc_drandom),
        builtin(1599, "setseed", 1, true, false, fc_setseed),
        builtin(6212, "drandom_normal", 2, true, false, fc_drandom_normal),
        builtin(6339, "int4random", 2, true, false, fc_int4random),
        builtin(6340, "int8random", 2, true, false, fc_int8random),
        builtin(6341, "numeric_random", 2, true, false, fc_numeric_random),
    ]);
}
