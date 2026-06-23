//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the per-database
//! SQL-callable accessors in `src/backend/utils/adt/pgstatfuncs.c`.
//!
//! Each `pg_stat_get_db_<stat>(oid)` reads a database OID, fetches that
//! database's `PgStat_StatDBEntry` via [`crate::pgstat_fetch_stat_dbentry`], and
//! projects one counter field. The C macro families map directly:
//!
//!   * `PG_STAT_GET_DBENTRY_INT64(stat)`    — `int8`, returns `0` when no entry;
//!   * `PG_STAT_GET_DBENTRY_FLOAT8_MS(stat)` — `float8`, microsec→millisec
//!     (`(double) stat / 1000.0`), returns `0` when no entry;
//!
//! plus hand-written functions: `numbackends` (scans the local backend-status
//! table), `stat_reset_time`/`checksum_last_failure` (timestamptz, NULL when 0),
//! `checksum_failures`/`checksum_last_failure` (NULL unless checksums enabled),
//! and `conflict_all` (sum of the six conflict counters).
//!
//! [`register_pgstat_database_builtins`] registers every row into the fmgr-core
//! builtin table. OIDs / nargs / strict are transcribed exactly from
//! `pg_proc.dat` (all take one `oid` arg, all strict, none retset).

use types_core::{Oid, TimestampTz};
use datum::Datum;
use types_error::PgResult;
use fmgr::resolution::BuiltinFunction;
use fmgr::{FunctionCallInfoBaseData, PgFnNative};
use types_pgstat::activity_pgstat::PgStat_StatDBEntry;

/// `PG_GETARG_OID(0)` → `DatumGetObjectId`: the database OID argument.
#[inline]
fn arg_dbid(fcinfo: &FunctionCallInfoBaseData) -> Oid {
    fcinfo
        .arg(0)
        .expect("pg_stat_get_db_*: missing dbid arg")
        .value
        .as_oid()
}

/// Fetch `dbid`'s database-stats entry (C: `pgstat_fetch_stat_dbentry(dbid)`).
/// `None` ⇒ no stats for this database (the `dbentry == NULL` branch).
#[inline]
fn dbentry(dbid: Oid) -> PgResult<Option<PgStat_StatDBEntry>> {
    crate::pgstat_fetch_stat_dbentry(dbid)
}

/// `PG_STAT_GET_DBENTRY_INT64(stat)`: `int8`, `(int64) dbentry->stat`, or `0`.
#[inline]
fn dbentry_int64(
    fcinfo: &FunctionCallInfoBaseData,
    f: fn(&PgStat_StatDBEntry) -> i64,
) -> PgResult<Datum> {
    let dbid = arg_dbid(fcinfo);
    let result = dbentry(dbid)?.map(|d| f(&d)).unwrap_or(0);
    Ok(Datum::from_i64(result))
}

/// `PG_STAT_GET_DBENTRY_FLOAT8_MS(stat)`: `float8`, microsec counter converted
/// to millisec (`(double) dbentry->stat / 1000.0`), or `0`.
#[inline]
fn dbentry_float8_ms(
    fcinfo: &FunctionCallInfoBaseData,
    f: fn(&PgStat_StatDBEntry) -> i64,
) -> PgResult<Datum> {
    let dbid = arg_dbid(fcinfo);
    let result = dbentry(dbid)?.map(|d| f(&d) as f64 / 1000.0).unwrap_or(0.0);
    Ok(Datum::from_f64(result))
}

/// timestamptz accessor: `NULL` when no entry OR the stored value is `0`.
#[inline]
fn dbentry_timestamptz(
    fcinfo: &mut FunctionCallInfoBaseData,
    f: fn(&PgStat_StatDBEntry) -> TimestampTz,
) -> PgResult<Datum> {
    let dbid = arg_dbid(fcinfo);
    let result = dbentry(dbid)?.map(|d| f(&d)).unwrap_or(0);
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

fn fc_db_blocks_fetched(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dbentry_int64(fc, |d| d.blocks_fetched)
}
fn fc_db_blocks_hit(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dbentry_int64(fc, |d| d.blocks_hit)
}
fn fc_db_conflict_bufferpin(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dbentry_int64(fc, |d| d.conflict_bufferpin)
}
fn fc_db_conflict_lock(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dbentry_int64(fc, |d| d.conflict_lock)
}
fn fc_db_conflict_snapshot(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dbentry_int64(fc, |d| d.conflict_snapshot)
}
fn fc_db_conflict_startup_deadlock(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dbentry_int64(fc, |d| d.conflict_startup_deadlock)
}
fn fc_db_conflict_tablespace(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dbentry_int64(fc, |d| d.conflict_tablespace)
}
fn fc_db_conflict_logicalslot(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dbentry_int64(fc, |d| d.conflict_logicalslot)
}
fn fc_db_deadlocks(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dbentry_int64(fc, |d| d.deadlocks)
}
fn fc_db_sessions(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dbentry_int64(fc, |d| d.sessions)
}
fn fc_db_sessions_abandoned(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dbentry_int64(fc, |d| d.sessions_abandoned)
}
fn fc_db_sessions_fatal(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dbentry_int64(fc, |d| d.sessions_fatal)
}
fn fc_db_sessions_killed(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dbentry_int64(fc, |d| d.sessions_killed)
}
fn fc_db_parallel_workers_to_launch(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dbentry_int64(fc, |d| d.parallel_workers_to_launch)
}
fn fc_db_parallel_workers_launched(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dbentry_int64(fc, |d| d.parallel_workers_launched)
}
fn fc_db_temp_bytes(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dbentry_int64(fc, |d| d.temp_bytes)
}
fn fc_db_temp_files(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dbentry_int64(fc, |d| d.temp_files)
}
fn fc_db_tuples_deleted(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dbentry_int64(fc, |d| d.tuples_deleted)
}
fn fc_db_tuples_fetched(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dbentry_int64(fc, |d| d.tuples_fetched)
}
fn fc_db_tuples_inserted(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dbentry_int64(fc, |d| d.tuples_inserted)
}
fn fc_db_tuples_returned(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dbentry_int64(fc, |d| d.tuples_returned)
}
fn fc_db_tuples_updated(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dbentry_int64(fc, |d| d.tuples_updated)
}
fn fc_db_xact_commit(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dbentry_int64(fc, |d| d.xact_commit)
}
fn fc_db_xact_rollback(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dbentry_int64(fc, |d| d.xact_rollback)
}

// ---------------------------------------------------------------------------
// FLOAT8 accessors (microsec counters → millisec for display).
// ---------------------------------------------------------------------------

fn fc_db_active_time(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dbentry_float8_ms(fc, |d| d.active_time)
}
fn fc_db_blk_read_time(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dbentry_float8_ms(fc, |d| d.blk_read_time)
}
fn fc_db_blk_write_time(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dbentry_float8_ms(fc, |d| d.blk_write_time)
}
fn fc_db_idle_in_transaction_time(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dbentry_float8_ms(fc, |d| d.idle_in_transaction_time)
}
fn fc_db_session_time(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dbentry_float8_ms(fc, |d| d.session_time)
}

// ---------------------------------------------------------------------------
// TIMESTAMPTZ accessor (NULL when 0).
// ---------------------------------------------------------------------------

fn fc_db_stat_reset_time(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dbentry_timestamptz(fc, |d| d.stat_reset_timestamp)
}

// ---------------------------------------------------------------------------
// Hand-written functions.
// ---------------------------------------------------------------------------

/// `pg_stat_get_db_conflict_all`: sum of the six per-database conflict counters,
/// or `0` when no entry.
fn fc_db_conflict_all(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let dbid = arg_dbid(fc);
    let result = dbentry(dbid)?
        .map(|d| {
            d.conflict_tablespace
                + d.conflict_lock
                + d.conflict_snapshot
                + d.conflict_logicalslot
                + d.conflict_bufferpin
                + d.conflict_startup_deadlock
        })
        .unwrap_or(0);
    Ok(Datum::from_i64(result))
}

/// `pg_stat_get_db_checksum_failures`: `int8` count, but `NULL` if data-page
/// checksums are not enabled in this cluster.
fn fc_db_checksum_failures(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    if !transam_xlog_seams::data_checksums_enabled::call() {
        fc.set_result_null(true);
        return Ok(Datum::from_i64(0));
    }
    let dbid = arg_dbid(fc);
    let result = dbentry(dbid)?.map(|d| d.checksum_failures).unwrap_or(0);
    Ok(Datum::from_i64(result))
}

/// `pg_stat_get_db_checksum_last_failure`: `timestamptz`, `NULL` if checksums
/// off OR no entry OR the stored value is `0`.
fn fc_db_checksum_last_failure(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    if !transam_xlog_seams::data_checksums_enabled::call() {
        fc.set_result_null(true);
        return Ok(Datum::from_i64(0));
    }
    let dbid = arg_dbid(fc);
    let result = dbentry(dbid)?.map(|d| d.last_checksum_failure).unwrap_or(0);
    if result == 0 {
        fc.set_result_null(true);
        Ok(Datum::from_i64(0))
    } else {
        Ok(Datum::from_i64(result))
    }
}

/// `pg_stat_get_db_numbackends`: `int4` count of sessions in the local
/// backend-status table whose `st_databaseid` matches `dbid`.
fn fc_db_numbackends(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let dbid = arg_dbid(fc);
    let tot_backends = status::pgstat_fetch_stat_numbackends();
    let mut result: i32 = 0;
    let mut idx: i32 = 1;
    while idx <= tot_backends {
        if let Some(local_beentry) =
            status::pgstat_get_local_beentry_by_index(idx)
        {
            if local_beentry.backend_status.st_databaseid == dbid {
                result += 1;
            }
        }
        idx += 1;
    }
    Ok(Datum::from_i32(result))
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

/// Register every per-database `pg_stat_get_db_*` builtin (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
pub fn register_pgstat_database_builtins() {
    fmgr_core::register_builtins_native([
        // INT64
        builtin(1944, "pg_stat_get_db_blocks_fetched", fc_db_blocks_fetched),
        builtin(1945, "pg_stat_get_db_blocks_hit", fc_db_blocks_hit),
        builtin(3068, "pg_stat_get_db_conflict_bufferpin", fc_db_conflict_bufferpin),
        builtin(3066, "pg_stat_get_db_conflict_lock", fc_db_conflict_lock),
        builtin(3067, "pg_stat_get_db_conflict_snapshot", fc_db_conflict_snapshot),
        builtin(
            3069,
            "pg_stat_get_db_conflict_startup_deadlock",
            fc_db_conflict_startup_deadlock,
        ),
        builtin(3065, "pg_stat_get_db_conflict_tablespace", fc_db_conflict_tablespace),
        builtin(6309, "pg_stat_get_db_conflict_logicalslot", fc_db_conflict_logicalslot),
        builtin(3152, "pg_stat_get_db_deadlocks", fc_db_deadlocks),
        builtin(6188, "pg_stat_get_db_sessions", fc_db_sessions),
        builtin(6189, "pg_stat_get_db_sessions_abandoned", fc_db_sessions_abandoned),
        builtin(6190, "pg_stat_get_db_sessions_fatal", fc_db_sessions_fatal),
        builtin(6191, "pg_stat_get_db_sessions_killed", fc_db_sessions_killed),
        builtin(
            6355,
            "pg_stat_get_db_parallel_workers_to_launch",
            fc_db_parallel_workers_to_launch,
        ),
        builtin(
            6356,
            "pg_stat_get_db_parallel_workers_launched",
            fc_db_parallel_workers_launched,
        ),
        builtin(3151, "pg_stat_get_db_temp_bytes", fc_db_temp_bytes),
        builtin(3150, "pg_stat_get_db_temp_files", fc_db_temp_files),
        builtin(2762, "pg_stat_get_db_tuples_deleted", fc_db_tuples_deleted),
        builtin(2759, "pg_stat_get_db_tuples_fetched", fc_db_tuples_fetched),
        builtin(2760, "pg_stat_get_db_tuples_inserted", fc_db_tuples_inserted),
        builtin(2758, "pg_stat_get_db_tuples_returned", fc_db_tuples_returned),
        builtin(2761, "pg_stat_get_db_tuples_updated", fc_db_tuples_updated),
        builtin(1942, "pg_stat_get_db_xact_commit", fc_db_xact_commit),
        builtin(1943, "pg_stat_get_db_xact_rollback", fc_db_xact_rollback),
        // FLOAT8 (microsec → millisec)
        builtin(6186, "pg_stat_get_db_active_time", fc_db_active_time),
        builtin(2844, "pg_stat_get_db_blk_read_time", fc_db_blk_read_time),
        builtin(2845, "pg_stat_get_db_blk_write_time", fc_db_blk_write_time),
        builtin(
            6187,
            "pg_stat_get_db_idle_in_transaction_time",
            fc_db_idle_in_transaction_time,
        ),
        builtin(6185, "pg_stat_get_db_session_time", fc_db_session_time),
        // TIMESTAMPTZ
        builtin(3074, "pg_stat_get_db_stat_reset_time", fc_db_stat_reset_time),
        // Hand-written
        builtin(3070, "pg_stat_get_db_conflict_all", fc_db_conflict_all),
        builtin(3426, "pg_stat_get_db_checksum_failures", fc_db_checksum_failures),
        builtin(
            3428,
            "pg_stat_get_db_checksum_last_failure",
            fc_db_checksum_last_failure,
        ),
        builtin(1941, "pg_stat_get_db_numbackends", fc_db_numbackends),
    ]);
}
