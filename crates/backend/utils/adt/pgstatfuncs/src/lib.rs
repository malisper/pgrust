//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! functions in `pgstatfuncs.c` whose argument/result types are expressible at
//! the current fmgr boundary and whose value cores are ported.
//!
//! Scope: the deterministic, non-set-returning cluster — `pg_backend_pid`, the
//! snapshot accessors (`pg_stat_get_snapshot_timestamp` / `pg_stat_have_stats`
//! / `pg_stat_clear_snapshot` / `pg_stat_force_next_flush`), and the counter
//! reset family (`pg_stat_reset` / `pg_stat_reset_shared` / `pg_stat_reset_slru`
//! / `pg_stat_reset_single_table_counters` / `pg_stat_reset_single_function_counters`
//! / `pg_stat_reset_replication_slot` / `pg_stat_reset_subscription_stats`).
//! Each delegates to a faithfully-ported pgstat core function.
//!
//! Argument lanes: a `text` arg arrives on the by-ref lane as `RefPayload::
//! Varlena` carrying a header-ful image; the 4-byte varlena header is stripped
//! to read the bytes (C `text_to_cstring(PG_GETARG_TEXT_PP)`). An `oid`/`int4`
//! arg is the low bits of its by-value word. `PG_ARGISNULL(i)` reads the arg's
//! `isnull` flag. Result lanes: `int4`/`int8`/`TimestampTz` are by-value words;
//! a `void` result is the dummy `0` word; `PG_RETURN_NULL` sets the result-null
//! flag.
//!
//! NOT registered here (genuinely subsystem-gated, left in the gap baseline):
//! the set-returning rows (`pg_stat_get_activity`, `pg_stat_get_io`,
//! `pg_stat_get_archiver`, `pg_stat_get_replication_slot`,
//! `pg_stat_get_subscription_stats`, the backend-status scalar accessors that
//! need `pgstat_get_beentry_by_proc_number`), and `pg_stat_reset_backend_stats`
//! (needs `BackendPidGetProc` / `pgstat_tracks_backend_bktype`, unported).

use datum::Datum;
use types_error::PgResult;
use fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

use types_pgstat::activity_pgstat::{
    PGSTAT_KIND_ARCHIVER, PGSTAT_KIND_BGWRITER, PGSTAT_KIND_CHECKPOINTER, PGSTAT_KIND_FUNCTION,
    PGSTAT_KIND_IO, PGSTAT_KIND_RELATION, PGSTAT_KIND_SLRU, PGSTAT_KIND_SUBSCRIPTION,
    PGSTAT_KIND_WAL,
};

use activity_pgstat::pgstat_core;

mod composite;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_ARGISNULL(i)`.
#[inline]
fn arg_isnull(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo.arg(i).map(|d| d.isnull).unwrap_or(true)
}

/// `PG_GETARG_OID(i)` → low 32 bits of arg `i`'s word.
#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> types_core::Oid {
    fcinfo
        .arg(i)
        .expect("pgstatfuncs fn: missing oid arg")
        .value
        .as_oid()
}

/// `PG_GETARG_INT32(i)` → low 32 bits of arg `i`'s word.
#[inline]
fn arg_i32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo
        .arg(i)
        .expect("pgstatfuncs fn: missing int4 arg")
        .value
        .as_i32()
}

/// `text_to_cstring(PG_GETARG_TEXT_PP(i))` — read a `text` arg's payload off the
/// by-ref lane (header-ful varlena image, strip the 4-byte header) as a String.
#[inline]
fn arg_text(fcinfo: &FunctionCallInfoBaseData, i: usize) -> String {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("pgstatfuncs fn: text arg missing from by-ref lane");
    String::from_utf8_lossy(&image[datum::varlena::VARHDRSZ..]).into_owned()
}

/// `PG_RETURN_INT32(v)`.
#[inline]
fn ret_i32(v: i32) -> Datum {
    Datum::from_i32(v)
}

/// `PG_RETURN_VOID()` — the dummy `0` word.
#[inline]
fn ret_void() -> Datum {
    Datum::from_usize(0)
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `pg_backend_pid()` → `PG_RETURN_INT32(MyProcPid)`.
fn fc_pg_backend_pid(_fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(ret_i32(init_small_seams::my_proc_pid::call()))
}

/// `pg_stat_get_snapshot_timestamp()` → the snapshot timestamp, or NULL when no
/// consistent snapshot is held.
fn fc_pg_stat_get_snapshot_timestamp(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    match pgstat_core::pgstat_get_stat_snapshot_timestamp() {
        Some(ts) => Ok(Datum::from_i64(ts)),
        None => {
            fc.set_result_null(true);
            Ok(ret_void())
        }
    }
}

/// `pg_stat_clear_snapshot()` → `PG_RETURN_VOID`.
fn fc_pg_stat_clear_snapshot(_fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    pgstat_core::pgstat_clear_snapshot();
    Ok(ret_void())
}

/// `pg_stat_force_next_flush()` → `PG_RETURN_VOID`.
fn fc_pg_stat_force_next_flush(_fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    pgstat_core::pgstat_force_next_flush();
    Ok(ret_void())
}

/// `pg_stat_have_stats(stats_type text, dboid oid, objid int8)` →
/// `pgstat_have_entry(pgstat_get_kind_from_str(stats_type), dboid, objid)`.
fn fc_pg_stat_have_stats(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let stats_type = arg_text(fc, 0);
    let dboid = arg_oid(fc, 1);
    let objid = fc.arg(2).expect("pg_stat_have_stats: objid").value.as_i64() as u64;

    let kind = pgstat_core::pgstat_get_kind_from_str(&stats_type)?;
    Ok(Datum::from_bool(pgstat_core::pgstat_have_entry(
        kind, dboid, objid,
    )?))
}

/// `pg_stat_reset()` → `pgstat_reset_counters()`.
fn fc_pg_stat_reset(_fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    pgstat_core::pgstat_reset_counters()?;
    Ok(ret_void())
}

/// `pg_stat_reset_shared(target text)` → reset some shared cluster-wide counters
/// (all when the arg is NULL).
fn fc_pg_stat_reset_shared(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    if arg_isnull(fc, 0) {
        // Reset all the statistics when nothing is specified.
        pgstat_core::pgstat_reset_of_kind(PGSTAT_KIND_ARCHIVER)?;
        pgstat_core::pgstat_reset_of_kind(PGSTAT_KIND_BGWRITER)?;
        pgstat_core::pgstat_reset_of_kind(PGSTAT_KIND_CHECKPOINTER)?;
        pgstat_core::pgstat_reset_of_kind(PGSTAT_KIND_IO)?;
        xlogprefetcher::XLogPrefetchResetStats();
        pgstat_core::pgstat_reset_of_kind(PGSTAT_KIND_SLRU)?;
        pgstat_core::pgstat_reset_of_kind(PGSTAT_KIND_WAL)?;
        return Ok(ret_void());
    }

    let target = arg_text(fc, 0);
    match target.as_str() {
        "archiver" => pgstat_core::pgstat_reset_of_kind(PGSTAT_KIND_ARCHIVER)?,
        "bgwriter" => pgstat_core::pgstat_reset_of_kind(PGSTAT_KIND_BGWRITER)?,
        "checkpointer" => pgstat_core::pgstat_reset_of_kind(PGSTAT_KIND_CHECKPOINTER)?,
        "io" => pgstat_core::pgstat_reset_of_kind(PGSTAT_KIND_IO)?,
        "recovery_prefetch" => xlogprefetcher::XLogPrefetchResetStats(),
        "slru" => pgstat_core::pgstat_reset_of_kind(PGSTAT_KIND_SLRU)?,
        "wal" => pgstat_core::pgstat_reset_of_kind(PGSTAT_KIND_WAL)?,
        _ => {
            return Err(types_error::PgError::error(format!(
                "unrecognized reset target: \"{target}\""
            ))
            .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE)
            .with_hint(
                "Target must be \"archiver\", \"bgwriter\", \"checkpointer\", \"io\", \
                 \"recovery_prefetch\", \"slru\", or \"wal\".",
            ));
        }
    }
    Ok(ret_void())
}

/// `pg_stat_reset_single_table_counters(taboid oid)` → reset one relation's
/// stats (shared relations live under `InvalidOid`, others under
/// `MyDatabaseId`).
fn fc_pg_stat_reset_single_table_counters(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let taboid = arg_oid(fc, 0);
    let dboid = if catalog_seams::is_shared_relation::call(taboid) {
        types_core::InvalidOid
    } else {
        init_small_seams::my_database_id::call()
    };
    pgstat_core::pgstat_reset(PGSTAT_KIND_RELATION, dboid, taboid as u64)?;
    Ok(ret_void())
}

/// `pg_stat_reset_single_function_counters(funcoid oid)` → reset one function's
/// stats under `MyDatabaseId`.
fn fc_pg_stat_reset_single_function_counters(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let funcoid = arg_oid(fc, 0);
    let dboid = init_small_seams::my_database_id::call();
    pgstat_core::pgstat_reset(PGSTAT_KIND_FUNCTION, dboid, funcoid as u64)?;
    Ok(ret_void())
}

/// `pg_stat_reset_slru(target text)` → reset SLRU counters (a specific one, or
/// all when the arg is NULL).
fn fc_pg_stat_reset_slru(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    if arg_isnull(fc, 0) {
        pgstat_core::pgstat_reset_of_kind(PGSTAT_KIND_SLRU)?;
    } else {
        let target = arg_text(fc, 0);
        pgstat_slru::pgstat_reset_slru(&target)?;
    }
    Ok(ret_void())
}

/// `pg_stat_reset_replication_slot(target text)` → reset replication-slot stats
/// (a specific one, or all when the arg is NULL).
fn fc_pg_stat_reset_replication_slot(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    if arg_isnull(fc, 0) {
        pgstat_core::pgstat_reset_of_kind(
            types_pgstat::activity_pgstat::PGSTAT_KIND_REPLSLOT,
        )?;
    } else {
        let target = arg_text(fc, 0);
        pgstat_replslot::pgstat_reset_replslot(&target)?;
    }
    Ok(ret_void())
}

/// `pg_stat_reset_subscription_stats(subid oid)` → reset subscription stats
/// (a specific one, or all when the arg is NULL).
fn fc_pg_stat_reset_subscription_stats(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    if arg_isnull(fc, 0) {
        // Clear all subscription stats.
        pgstat_core::pgstat_reset_of_kind(PGSTAT_KIND_SUBSCRIPTION)?;
    } else {
        let subid = arg_oid(fc, 0);
        if !types_core::OidIsValid(subid) {
            return Err(types_error::PgError::error(format!(
                "invalid subscription OID {subid}"
            ))
            .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE));
        }
        pgstat_core::pgstat_reset(PGSTAT_KIND_SUBSCRIPTION, types_core::InvalidOid, subid as u64)?;
    }
    Ok(ret_void())
}

/// `pg_stat_get_backend_pid(procNumber int4)` (pgstatfuncs.c:696) →
/// `pgstat_get_beentry_by_proc_number(procNumber)->st_procpid`, or NULL when the
/// proc number has no live beentry.
fn fc_pg_stat_get_backend_pid(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    // int32 procNumber = PG_GETARG_INT32(0);
    let proc_number = arg_i32(fc, 0);

    // if ((beentry = pgstat_get_beentry_by_proc_number(procNumber)) == NULL)
    //     PG_RETURN_NULL();
    match status::pgstat_get_beentry_by_proc_number(proc_number) {
        Some(beentry) => Ok(ret_i32(beentry.st_procpid)),
        None => {
            fc.set_result_null(true);
            Ok(ret_void())
        }
    }
}

/// `pg_stat_reset_backend_stats(backend_pid int4)` (pgstatfuncs.c:1956) → reset
/// one backend's cumulative `PGSTAT_KIND_BACKEND` statistics, identified by pid.
/// `PG_RETURN_VOID`.
fn fc_pg_stat_reset_backend_stats(fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let backend_pid = arg_i32(fc, 0);
    pgstat_backend::pgstat_reset_backend_stats(backend_pid)?;
    Ok(ret_void())
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

/// Build one builtin row paired with its native body. OIDs / nargs / strict /
/// retset are transcribed exactly from `pg_proc.dat`.
fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    strict: bool,
    native: PgFnNative,
) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.to_string(),
            nargs,
            strict,
            retset: false,
            func: None,
        },
        native,
    )
}

/// Register the deterministic `pgstatfuncs.c` builtins (C: their
/// `fmgr_builtins[]` rows). Called from this crate's [`init_seams`].
pub fn register_pgstatfuncs_builtins() {
    fmgr_core::register_builtins_native([
        builtin(2026, "pg_backend_pid", 0, true, fc_pg_backend_pid),
        builtin(
            1937,
            "pg_stat_get_backend_pid",
            1,
            true,
            fc_pg_stat_get_backend_pid,
        ),
        builtin(
            6387,
            "pg_stat_reset_backend_stats",
            1,
            true,
            fc_pg_stat_reset_backend_stats,
        ),
        builtin(
            3788,
            "pg_stat_get_snapshot_timestamp",
            0,
            true,
            fc_pg_stat_get_snapshot_timestamp,
        ),
        builtin(
            2230,
            "pg_stat_clear_snapshot",
            0,
            false,
            fc_pg_stat_clear_snapshot,
        ),
        builtin(
            2137,
            "pg_stat_force_next_flush",
            0,
            false,
            fc_pg_stat_force_next_flush,
        ),
        builtin(6230, "pg_stat_have_stats", 3, true, fc_pg_stat_have_stats),
        builtin(2274, "pg_stat_reset", 0, false, fc_pg_stat_reset),
        builtin(
            3775,
            "pg_stat_reset_shared",
            1,
            false,
            fc_pg_stat_reset_shared,
        ),
        builtin(
            3776,
            "pg_stat_reset_single_table_counters",
            1,
            true,
            fc_pg_stat_reset_single_table_counters,
        ),
        builtin(
            3777,
            "pg_stat_reset_single_function_counters",
            1,
            true,
            fc_pg_stat_reset_single_function_counters,
        ),
        builtin(2307, "pg_stat_reset_slru", 1, false, fc_pg_stat_reset_slru),
        builtin(
            6170,
            "pg_stat_reset_replication_slot",
            1,
            false,
            fc_pg_stat_reset_replication_slot,
        ),
        builtin(
            6232,
            "pg_stat_reset_subscription_stats",
            1,
            false,
            fc_pg_stat_reset_subscription_stats,
        ),
        // Single-composite-row (non-set-returning) accessors reachable through
        // the scalar fmgr call path (target-list position). See `composite.rs`.
        builtin(
            6169,
            "pg_stat_get_replication_slot",
            1,
            true,
            crate::composite::fc_pg_stat_get_replication_slot,
        ),
        builtin(
            6231,
            "pg_stat_get_subscription_stats",
            1,
            true,
            crate::composite::fc_pg_stat_get_subscription_stats,
        ),
    ]);
}

/// Install this crate's builtin registrations. (No `-seams` crate: the
/// registration writes directly into the fmgr-core builtin table.)
pub fn init_seams() {
    register_pgstatfuncs_builtins();
}
