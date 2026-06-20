//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the per-function
//! SQL-callable accessors in `src/backend/utils/adt/pgstatfuncs.c`.
//!
//! `pg_stat_get_function_calls(oid)` reads a function OID, fetches that
//! function's `PgStat_StatFuncEntry` via [`crate::pgstat_fetch_stat_funcentry`],
//! and returns `numcalls` as `int8` — or `NULL` when there is no entry (C:
//! `if (funcentry == NULL) PG_RETURN_NULL();`).
//!
//! `pg_stat_get_function_total_time` / `_self_time` follow the
//! `PG_STAT_GET_FUNCENTRY_FLOAT8_MS` macro: `NULL` when no entry, otherwise the
//! microsecond counter converted to milliseconds (`/ 1000.0`) as `float8`.

use types_core::Oid;
use types_datum::Datum;
use types_error::PgError;
use types_fmgr::resolution::BuiltinFunction;
use types_fmgr::FunctionCallInfoBaseData;
use types_pgstat::activity_pgstat::PgStat_StatFuncEntry;

/// `PG_GETARG_OID(0)` → `DatumGetObjectId`: the function OID argument.
#[inline]
fn arg_funcid(fcinfo: &FunctionCallInfoBaseData) -> Oid {
    fcinfo
        .arg(0)
        .expect("pg_stat_get_function_*: missing funcid arg")
        .value
        .as_oid()
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: PgError) -> ! {
    std::panic::panic_any(err);
}

/// Fetch `funcid`'s function-stats entry (C:
/// `pgstat_fetch_stat_funcentry(funcid)`). `None` ⇒ no stats (`funcentry == NULL`).
#[inline]
fn funcentry(funcid: Oid) -> Option<PgStat_StatFuncEntry> {
    match crate::pgstat_fetch_stat_funcentry(funcid) {
        Ok(e) => e,
        Err(e) => raise(e),
    }
}

/// `pg_stat_get_function_calls(oid)` — `int8`, `NULL` when no entry.
fn fc_function_calls(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let funcid = arg_funcid(fcinfo);
    match funcentry(funcid) {
        None => {
            fcinfo.set_result_null(true);
            Datum::from_i64(0)
        }
        Some(e) => Datum::from_i64(e.numcalls),
    }
}

/// `PG_STAT_GET_FUNCENTRY_FLOAT8_MS(stat)`: `float8`, `NULL` when no entry,
/// otherwise `(double) funcentry->stat / 1000.0` (microsec → millisec).
#[inline]
fn funcentry_float8_ms(
    fcinfo: &mut FunctionCallInfoBaseData,
    f: fn(&PgStat_StatFuncEntry) -> i64,
) -> Datum {
    let funcid = arg_funcid(fcinfo);
    match funcentry(funcid) {
        None => {
            fcinfo.set_result_null(true);
            Datum::from_f64(0.0)
        }
        Some(e) => Datum::from_f64(f(&e) as f64 / 1000.0),
    }
}

fn fc_function_total_time(fc: &mut FunctionCallInfoBaseData) -> Datum {
    funcentry_float8_ms(fc, |e| e.total_time)
}
fn fc_function_self_time(fc: &mut FunctionCallInfoBaseData) -> Datum {
    funcentry_float8_ms(fc, |e| e.self_time)
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

fn builtin(
    foid: u32,
    name: &str,
    func: fn(&mut FunctionCallInfoBaseData) -> Datum,
) -> BuiltinFunction {
    BuiltinFunction {
        foid,
        name: name.to_string(),
        nargs: 1,
        strict: true,
        retset: false,
        func: Some(func),
    }
}

/// Register every per-function `pg_stat_get_function_*` builtin (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
pub fn register_pgstat_function_builtins() {
    backend_utils_fmgr_core::register_builtins([
        builtin(2978, "pg_stat_get_function_calls", fc_function_calls),
        builtin(2979, "pg_stat_get_function_total_time", fc_function_total_time),
        builtin(2980, "pg_stat_get_function_self_time", fc_function_self_time),
    ]);
}
