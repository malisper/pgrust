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
use types_error::PgResult;
use types_fmgr::resolution::BuiltinFunction;
use types_fmgr::{FunctionCallInfoBaseData, PgFnNative};
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

/// Fetch `funcid`'s function-stats entry (C:
/// `pgstat_fetch_stat_funcentry(funcid)`). `None` ⇒ no stats (`funcentry == NULL`).
#[inline]
fn funcentry(funcid: Oid) -> PgResult<Option<PgStat_StatFuncEntry>> {
    crate::pgstat_fetch_stat_funcentry(funcid)
}

/// `pg_stat_get_function_calls(oid)` — `int8`, `NULL` when no entry.
fn fc_function_calls(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let funcid = arg_funcid(fcinfo);
    match funcentry(funcid)? {
        None => {
            fcinfo.set_result_null(true);
            Ok(Datum::from_i64(0))
        }
        Some(e) => Ok(Datum::from_i64(e.numcalls)),
    }
}

/// `PG_STAT_GET_FUNCENTRY_FLOAT8_MS(stat)`: `float8`, `NULL` when no entry,
/// otherwise `(double) funcentry->stat / 1000.0` (microsec → millisec).
#[inline]
fn funcentry_float8_ms(
    fcinfo: &mut FunctionCallInfoBaseData,
    f: fn(&PgStat_StatFuncEntry) -> i64,
) -> PgResult<Datum> {
    let funcid = arg_funcid(fcinfo);
    match funcentry(funcid)? {
        None => {
            fcinfo.set_result_null(true);
            Ok(Datum::from_f64(0.0))
        }
        Some(e) => Ok(Datum::from_f64(f(&e) as f64 / 1000.0)),
    }
}

fn fc_function_total_time(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    funcentry_float8_ms(fc, |e| e.total_time)
}
fn fc_function_self_time(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    funcentry_float8_ms(fc, |e| e.self_time)
}

/// `pg_stat_force_next_flush()` (`pgstatfuncs.c:1860`) — `pgstat_force_next_flush();
/// PG_RETURN_VOID();`. Forces this backend's pending cumulative stats to be
/// flushed on the next `pgstat_report_stat()` call (used for writing tests).
/// Takes no arguments and returns `void`.
fn fc_force_next_flush(_fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    backend_utils_activity_pgstat::pgstat_core::pgstat_force_next_flush();
    // PG_RETURN_VOID(): the returned Datum is ignored for a void function.
    Ok(Datum::from_i64(0))
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

fn builtin(foid: u32, name: &str, native: PgFnNative) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.to_string(),
            nargs: 1,
            strict: true,
            retset: false,
            func: None,
        },
        native,
    )
}

/// Register every per-function `pg_stat_get_function_*` builtin (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
pub fn register_pgstat_function_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        builtin(2978, "pg_stat_get_function_calls", fc_function_calls),
        builtin(2979, "pg_stat_get_function_total_time", fc_function_total_time),
        builtin(2980, "pg_stat_get_function_self_time", fc_function_self_time),
        // pg_stat_force_next_flush() — fmgr_builtins[] row { 2137, 0, false,
        // false, ... }: 0 args, non-strict (proisstrict='f'), returns void.
        (
            BuiltinFunction {
                foid: 2137,
                name: "pg_stat_force_next_flush".to_string(),
                nargs: 0,
                strict: false,
                retset: false,
                func: None,
            },
            fc_force_next_flush as PgFnNative,
        ),
    ]);
}
