//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the per-relation
//! SQL-callable accessors in `src/backend/utils/adt/pgstatfuncs.c`.
//!
//! Each `pg_stat_get_<stat>(oid)` reads a relation OID, fetches that relation's
//! `PgStat_StatTabEntry` via [`crate::pgstat_fetch_stat_tabentry`], and projects
//! one counter field. The three C macro families map directly:
//!
//!   * `PG_STAT_GET_RELENTRY_INT64`      — `int8`, returns `0` when no entry;
//!   * `PG_STAT_GET_RELENTRY_FLOAT8`     — `float8`, returns `0` when no entry;
//!   * `PG_STAT_GET_RELENTRY_TIMESTAMPTZ`— `timestamptz`, returns `NULL` when no
//!     entry OR when the stored value is `0`.
//!
//! [`register_pgstat_relation_builtins`] registers every row into the fmgr-core
//! builtin table (C: `fmgr_builtins[]`) so by-OID dispatch resolves them. OIDs /
//! nargs / strict are transcribed exactly from `pg_proc.dat` (all take one `oid`
//! arg, all strict, none retset).

use types_core::{Oid, TimestampTz};
use types_datum::Datum;
use types_error::PgResult;
use types_fmgr::resolution::BuiltinFunction;
use types_fmgr::{FunctionCallInfoBaseData, PgFnNative};
use types_pgstat::activity_pgstat::PgStat_StatTabEntry;

/// `PG_GETARG_OID(0)` → `DatumGetObjectId`: the relation OID argument.
#[inline]
fn arg_relid(fcinfo: &FunctionCallInfoBaseData) -> Oid {
    fcinfo
        .arg(0)
        .expect("pg_stat_get_*: missing relid arg")
        .value
        .as_oid()
}

/// Fetch `relid`'s table-stats entry (C: `pgstat_fetch_stat_tabentry(relid)`).
/// `None` ⇒ no stats for this relation (the `tabentry == NULL` branch).
#[inline]
fn tabentry(relid: Oid) -> PgResult<Option<PgStat_StatTabEntry>> {
    crate::pgstat_fetch_stat_tabentry(relid)
}

/// `PG_STAT_GET_RELENTRY_INT64(stat)`: `int8`, `(int64) tabentry->stat`, or `0`.
#[inline]
fn relentry_int64(
    fcinfo: &FunctionCallInfoBaseData,
    f: fn(&PgStat_StatTabEntry) -> i64,
) -> PgResult<Datum> {
    let relid = arg_relid(fcinfo);
    let result = tabentry(relid)?.map(|t| f(&t)).unwrap_or(0);
    Ok(Datum::from_i64(result))
}

/// `PG_STAT_GET_RELENTRY_FLOAT8(stat)`: `float8`, `(double) tabentry->stat`, or
/// `0`.
#[inline]
fn relentry_float8(
    fcinfo: &FunctionCallInfoBaseData,
    f: fn(&PgStat_StatTabEntry) -> i64,
) -> PgResult<Datum> {
    let relid = arg_relid(fcinfo);
    let result = tabentry(relid)?.map(|t| f(&t)).unwrap_or(0) as f64;
    Ok(Datum::from_f64(result))
}

/// `PG_STAT_GET_RELENTRY_TIMESTAMPTZ(stat)`: `timestamptz`. `NULL` when no entry
/// OR the stored value is `0`; otherwise the timestamp.
#[inline]
fn relentry_timestamptz(
    fcinfo: &mut FunctionCallInfoBaseData,
    f: fn(&PgStat_StatTabEntry) -> TimestampTz,
) -> PgResult<Datum> {
    let relid = arg_relid(fcinfo);
    let result = tabentry(relid)?.map(|t| f(&t)).unwrap_or(0);
    if result == 0 {
        fcinfo.set_result_null(true);
        Ok(Datum::from_i64(0))
    } else {
        Ok(Datum::from_i64(result))
    }
}

// ---------------------------------------------------------------------------
// INT64 accessors.
// ---------------------------------------------------------------------------

fn fc_numscans(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    relentry_int64(fc, |t| t.numscans)
}
fn fc_tuples_returned(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    relentry_int64(fc, |t| t.tuples_returned)
}
fn fc_tuples_fetched(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    relentry_int64(fc, |t| t.tuples_fetched)
}
fn fc_tuples_inserted(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    relentry_int64(fc, |t| t.tuples_inserted)
}
fn fc_tuples_updated(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    relentry_int64(fc, |t| t.tuples_updated)
}
fn fc_tuples_deleted(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    relentry_int64(fc, |t| t.tuples_deleted)
}
fn fc_tuples_hot_updated(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    relentry_int64(fc, |t| t.tuples_hot_updated)
}
fn fc_tuples_newpage_updated(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    relentry_int64(fc, |t| t.tuples_newpage_updated)
}
fn fc_live_tuples(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    relentry_int64(fc, |t| t.live_tuples)
}
fn fc_dead_tuples(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    relentry_int64(fc, |t| t.dead_tuples)
}
fn fc_mod_since_analyze(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    relentry_int64(fc, |t| t.mod_since_analyze)
}
fn fc_ins_since_vacuum(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    relentry_int64(fc, |t| t.ins_since_vacuum)
}
fn fc_blocks_fetched(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    relentry_int64(fc, |t| t.blocks_fetched)
}
fn fc_blocks_hit(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    relentry_int64(fc, |t| t.blocks_hit)
}
fn fc_vacuum_count(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    relentry_int64(fc, |t| t.vacuum_count)
}
fn fc_autovacuum_count(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    relentry_int64(fc, |t| t.autovacuum_count)
}
fn fc_analyze_count(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    relentry_int64(fc, |t| t.analyze_count)
}
fn fc_autoanalyze_count(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    relentry_int64(fc, |t| t.autoanalyze_count)
}

// ---------------------------------------------------------------------------
// FLOAT8 accessors (times stored in milliseconds).
// ---------------------------------------------------------------------------

fn fc_total_vacuum_time(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    relentry_float8(fc, |t| t.total_vacuum_time)
}
fn fc_total_autovacuum_time(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    relentry_float8(fc, |t| t.total_autovacuum_time)
}
fn fc_total_analyze_time(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    relentry_float8(fc, |t| t.total_analyze_time)
}
fn fc_total_autoanalyze_time(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    relentry_float8(fc, |t| t.total_autoanalyze_time)
}

// ---------------------------------------------------------------------------
// TIMESTAMPTZ accessors (NULL when 0).
// ---------------------------------------------------------------------------

fn fc_lastscan(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    relentry_timestamptz(fc, |t| t.lastscan)
}
fn fc_last_vacuum_time(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    relentry_timestamptz(fc, |t| t.last_vacuum_time)
}
fn fc_last_autovacuum_time(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    relentry_timestamptz(fc, |t| t.last_autovacuum_time)
}
fn fc_last_analyze_time(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    relentry_timestamptz(fc, |t| t.last_analyze_time)
}
fn fc_last_autoanalyze_time(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    relentry_timestamptz(fc, |t| t.last_autoanalyze_time)
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

/// Register every per-relation `pg_stat_get_*` builtin (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
pub fn register_pgstat_relation_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        // INT64
        builtin(1928, "pg_stat_get_numscans", fc_numscans),
        builtin(1929, "pg_stat_get_tuples_returned", fc_tuples_returned),
        builtin(1930, "pg_stat_get_tuples_fetched", fc_tuples_fetched),
        builtin(1931, "pg_stat_get_tuples_inserted", fc_tuples_inserted),
        builtin(1932, "pg_stat_get_tuples_updated", fc_tuples_updated),
        builtin(1933, "pg_stat_get_tuples_deleted", fc_tuples_deleted),
        builtin(1972, "pg_stat_get_tuples_hot_updated", fc_tuples_hot_updated),
        builtin(6217, "pg_stat_get_tuples_newpage_updated", fc_tuples_newpage_updated),
        builtin(2878, "pg_stat_get_live_tuples", fc_live_tuples),
        builtin(2879, "pg_stat_get_dead_tuples", fc_dead_tuples),
        builtin(3177, "pg_stat_get_mod_since_analyze", fc_mod_since_analyze),
        builtin(5053, "pg_stat_get_ins_since_vacuum", fc_ins_since_vacuum),
        builtin(1934, "pg_stat_get_blocks_fetched", fc_blocks_fetched),
        builtin(1935, "pg_stat_get_blocks_hit", fc_blocks_hit),
        builtin(3054, "pg_stat_get_vacuum_count", fc_vacuum_count),
        builtin(3055, "pg_stat_get_autovacuum_count", fc_autovacuum_count),
        builtin(3056, "pg_stat_get_analyze_count", fc_analyze_count),
        builtin(3057, "pg_stat_get_autoanalyze_count", fc_autoanalyze_count),
        // FLOAT8
        builtin(6358, "pg_stat_get_total_vacuum_time", fc_total_vacuum_time),
        builtin(6359, "pg_stat_get_total_autovacuum_time", fc_total_autovacuum_time),
        builtin(6360, "pg_stat_get_total_analyze_time", fc_total_analyze_time),
        builtin(6361, "pg_stat_get_total_autoanalyze_time", fc_total_autoanalyze_time),
        // TIMESTAMPTZ
        builtin(6310, "pg_stat_get_lastscan", fc_lastscan),
        builtin(2781, "pg_stat_get_last_vacuum_time", fc_last_vacuum_time),
        builtin(2782, "pg_stat_get_last_autovacuum_time", fc_last_autovacuum_time),
        builtin(2783, "pg_stat_get_last_analyze_time", fc_last_analyze_time),
        builtin(2784, "pg_stat_get_last_autoanalyze_time", fc_last_autoanalyze_time),
    ]);
}
