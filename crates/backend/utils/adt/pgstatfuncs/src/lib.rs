#![allow(non_snake_case)]

use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::{PgError, PgResult};
use ::types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

use pgstat::pending::{
    PGSTAT_KIND_ARCHIVER, PGSTAT_KIND_BGWRITER, PGSTAT_KIND_CHECKPOINTER, PGSTAT_KIND_FUNCTION,
    PGSTAT_KIND_IO, PGSTAT_KIND_RELATION, PGSTAT_KIND_REPLSLOT, PGSTAT_KIND_SLRU,
    PGSTAT_KIND_SUBSCRIPTION, PGSTAT_KIND_WAL,
};

mod activity;
mod backend;
mod stats;
pub use activity::fc_pg_stat_get_activity;

pub(crate) fn arg_text_str<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<&'a str> {
    // SAFETY: catalog arg type text — non-null varlena (null-checked by caller
    // for non-strict functions).
    let v = unsafe { fcinfo.arg_varlena_packed(i) }?;
    core::str::from_utf8(v.data())
        .map_err(|_| Box::new(PgError::error("invalid UTF-8 in text argument")))
}

fn tabentry(fcinfo: &Fcinfo) -> Option<pgstat::PgStat_StatTabEntry> {
    pgstat::pgstat_fetch_stat_tabentry(fcinfo.args_n::<1>()[0].value.as_oid())
}

fn dbentry(fcinfo: &Fcinfo) -> Option<pgstat::database::PgStat_StatDBEntry> {
    pgstat::pgstat_fetch_stat_dbentry(fcinfo.args_n::<1>()[0].value.as_oid())
}

macro_rules! rel_i64 {
    ($($fc:ident $field:ident;)*) => {$(
        pub fn $fc(_fl: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            Ok(Datum::from_i64(tabentry(fcinfo).map_or(0, |t| t.$field)))
        }
    )*};
}

macro_rules! rel_f8 {
    ($($fc:ident $field:ident;)*) => {$(
        pub fn $fc(_fl: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            Ok(Datum::from_f64(tabentry(fcinfo).map_or(0.0, |t| t.$field as f64)))
        }
    )*};
}

macro_rules! rel_tstz {
    ($($fc:ident $field:ident;)*) => {$(
        pub fn $fc(_fl: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            match tabentry(fcinfo).map_or(0, |t| t.$field) {
                0 => Ok(fcinfo.return_null()),
                ts => Ok(Datum::from_i64(ts)),
            }
        }
    )*};
}

macro_rules! db_i64 {
    ($($fc:ident $field:ident;)*) => {$(
        pub fn $fc(_fl: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            Ok(Datum::from_i64(dbentry(fcinfo).map_or(0, |d| d.$field)))
        }
    )*};
}

// C PG_STAT_GET_DBENTRY_FLOAT8_MS: stored microseconds, displayed milliseconds.
macro_rules! db_f8_ms {
    ($($fc:ident $field:ident;)*) => {$(
        pub fn $fc(_fl: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            Ok(Datum::from_f64(dbentry(fcinfo).map_or(0.0, |d| d.$field as f64) / 1000.0))
        }
    )*};
}

rel_i64! {
    fc_pg_stat_get_numscans numscans;
    fc_pg_stat_get_tuples_returned tuples_returned;
    fc_pg_stat_get_tuples_fetched tuples_fetched;
    fc_pg_stat_get_tuples_inserted tuples_inserted;
    fc_pg_stat_get_tuples_updated tuples_updated;
    fc_pg_stat_get_tuples_deleted tuples_deleted;
    fc_pg_stat_get_tuples_hot_updated tuples_hot_updated;
    fc_pg_stat_get_tuples_newpage_updated tuples_newpage_updated;
    fc_pg_stat_get_live_tuples live_tuples;
    fc_pg_stat_get_dead_tuples dead_tuples;
    fc_pg_stat_get_mod_since_analyze mod_since_analyze;
    fc_pg_stat_get_ins_since_vacuum ins_since_vacuum;
    fc_pg_stat_get_blocks_fetched blocks_fetched;
    fc_pg_stat_get_blocks_hit blocks_hit;
    fc_pg_stat_get_vacuum_count vacuum_count;
    fc_pg_stat_get_autovacuum_count autovacuum_count;
    fc_pg_stat_get_analyze_count analyze_count;
    fc_pg_stat_get_autoanalyze_count autoanalyze_count;
}

rel_f8! {
    fc_pg_stat_get_total_vacuum_time total_vacuum_time;
    fc_pg_stat_get_total_autovacuum_time total_autovacuum_time;
    fc_pg_stat_get_total_analyze_time total_analyze_time;
    fc_pg_stat_get_total_autoanalyze_time total_autoanalyze_time;
}

rel_tstz! {
    fc_pg_stat_get_lastscan lastscan;
    fc_pg_stat_get_last_vacuum_time last_vacuum_time;
    fc_pg_stat_get_last_autovacuum_time last_autovacuum_time;
    fc_pg_stat_get_last_analyze_time last_analyze_time;
    fc_pg_stat_get_last_autoanalyze_time last_autoanalyze_time;
}

db_i64! {
    fc_pg_stat_get_db_xact_commit xact_commit;
    fc_pg_stat_get_db_xact_rollback xact_rollback;
    fc_pg_stat_get_db_blocks_fetched blocks_fetched;
    fc_pg_stat_get_db_blocks_hit blocks_hit;
    fc_pg_stat_get_db_tuples_returned tuples_returned;
    fc_pg_stat_get_db_tuples_fetched tuples_fetched;
    fc_pg_stat_get_db_tuples_inserted tuples_inserted;
    fc_pg_stat_get_db_tuples_updated tuples_updated;
    fc_pg_stat_get_db_tuples_deleted tuples_deleted;
    fc_pg_stat_get_db_conflict_tablespace conflict_tablespace;
    fc_pg_stat_get_db_conflict_lock conflict_lock;
    fc_pg_stat_get_db_conflict_snapshot conflict_snapshot;
    fc_pg_stat_get_db_conflict_logicalslot conflict_logicalslot;
    fc_pg_stat_get_db_conflict_bufferpin conflict_bufferpin;
    fc_pg_stat_get_db_conflict_startup_deadlock conflict_startup_deadlock;
    fc_pg_stat_get_db_deadlocks deadlocks;
    fc_pg_stat_get_db_temp_files temp_files;
    fc_pg_stat_get_db_temp_bytes temp_bytes;
    fc_pg_stat_get_db_sessions sessions;
    fc_pg_stat_get_db_sessions_abandoned sessions_abandoned;
    fc_pg_stat_get_db_sessions_fatal sessions_fatal;
    fc_pg_stat_get_db_sessions_killed sessions_killed;
    fc_pg_stat_get_db_parallel_workers_to_launch parallel_workers_to_launch;
    fc_pg_stat_get_db_parallel_workers_launched parallel_workers_launched;
}

db_f8_ms! {
    fc_pg_stat_get_db_blk_read_time blk_read_time;
    fc_pg_stat_get_db_blk_write_time blk_write_time;
    fc_pg_stat_get_db_session_time session_time;
    fc_pg_stat_get_db_active_time active_time;
    fc_pg_stat_get_db_idle_in_transaction_time idle_in_transaction_time;
}

pub fn fc_pg_stat_get_db_conflict_all(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Ok(Datum::from_i64(dbentry(fcinfo).map_or(0, |d| {
        d.conflict_tablespace
            + d.conflict_lock
            + d.conflict_snapshot
            + d.conflict_logicalslot
            + d.conflict_bufferpin
            + d.conflict_startup_deadlock
    })))
}

pub fn fc_pg_stat_get_db_stat_reset_time(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    match dbentry(fcinfo).map_or(0, |d| d.stat_reset_timestamp) {
        0 => Ok(fcinfo.return_null()),
        ts => Ok(Datum::from_i64(ts)),
    }
}

pub fn fc_pg_stat_get_db_checksum_failures(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    if !transam_xlog_seams::data_checksums_enabled::call() {
        return Ok(fcinfo.return_null());
    }
    Ok(Datum::from_i64(dbentry(fcinfo).map_or(0, |d| d.checksum_failures)))
}

pub fn fc_pg_stat_get_db_checksum_last_failure(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    if !transam_xlog_seams::data_checksums_enabled::call() {
        return Ok(fcinfo.return_null());
    }
    match dbentry(fcinfo).map_or(0, |d| d.last_checksum_failure) {
        0 => Ok(fcinfo.return_null()),
        ts => Ok(Datum::from_i64(ts)),
    }
}

pub fn fc_pg_stat_get_db_numbackends(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let dbid = fcinfo.args_n::<1>()[0].value.as_oid();
    let tot = backend_status::pgstat_fetch_stat_numbackends();
    let mut result: i32 = 0;
    for idx in 1..=tot {
        if let Some(local) = backend_status::pgstat_get_local_beentry_by_index(idx) {
            if local.st_databaseid == dbid {
                result += 1;
            }
        }
    }
    Ok(Datum::from_i32(result))
}

pub fn fc_pg_backend_pid(_fl: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i32(init_small::globals::MyProcPid()))
}

pub fn fc_pg_stat_force_next_flush(
    _fl: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    pgstat::pending::pgstat_force_next_flush();
    Ok(Datum::from_usize(0))
}

pub fn fc_pg_stat_clear_snapshot(
    _fl: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    pgstat::pgstat_clear_snapshot();
    Ok(Datum::from_usize(0))
}

macro_rules! xact_rel_i64 {
    ($($fc:ident $field:ident;)*) => {$(
        pub fn $fc(_fl: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let relid = fcinfo.args_n::<1>()[0].value.as_oid();
            Ok(Datum::from_i64(
                pgstat::relation::find_tabstat_entry(relid).map_or(0, |c| c.$field),
            ))
        }
    )*};
}

xact_rel_i64! {
    fc_pg_stat_get_xact_numscans numscans;
    fc_pg_stat_get_xact_tuples_returned tuples_returned;
    fc_pg_stat_get_xact_tuples_fetched tuples_fetched;
    fc_pg_stat_get_xact_tuples_inserted tuples_inserted;
    fc_pg_stat_get_xact_tuples_updated tuples_updated;
    fc_pg_stat_get_xact_tuples_deleted tuples_deleted;
    fc_pg_stat_get_xact_tuples_hot_updated tuples_hot_updated;
    fc_pg_stat_get_xact_tuples_newpage_updated tuples_newpage_updated;
    fc_pg_stat_get_xact_blocks_fetched blocks_fetched;
    fc_pg_stat_get_xact_blocks_hit blocks_hit;
}

pub fn fc_pg_stat_get_function_calls(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let funcid = fcinfo.args_n::<1>()[0].value.as_oid();
    match pgstat::pgstat_fetch_stat_funcentry(funcid) {
        Some(f) => Ok(Datum::from_i64(f.numcalls)),
        None => Ok(fcinfo.return_null()),
    }
}

// C PG_STAT_GET_FUNCENTRY_FLOAT8_MS: stored microseconds, displayed milliseconds.
macro_rules! func_f8_ms {
    ($($fc:ident $field:ident;)*) => {$(
        pub fn $fc(_fl: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let funcid = fcinfo.args_n::<1>()[0].value.as_oid();
            match pgstat::pgstat_fetch_stat_funcentry(funcid) {
                Some(f) => Ok(Datum::from_f64(f.$field as f64 / 1000.0)),
                None => Ok(fcinfo.return_null()),
            }
        }
    )*};
}

func_f8_ms! {
    fc_pg_stat_get_function_total_time total_time;
    fc_pg_stat_get_function_self_time self_time;
}

pub fn fc_pg_stat_get_xact_function_calls(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let funcid = fcinfo.args_n::<1>()[0].value.as_oid();
    match pgstat::find_funcstat_entry(funcid) {
        Some(f) => Ok(Datum::from_i64(f.numcalls)),
        None => Ok(fcinfo.return_null()),
    }
}

// C PG_STAT_GET_XACT_FUNCENTRY_FLOAT8_MS: pending ns ticks, displayed ms.
macro_rules! xact_func_f8_ms {
    ($($fc:ident $field:ident;)*) => {$(
        pub fn $fc(_fl: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let funcid = fcinfo.args_n::<1>()[0].value.as_oid();
            match pgstat::find_funcstat_entry(funcid) {
                Some(f) => Ok(Datum::from_f64(f.$field as f64 / 1_000_000.0)),
                None => Ok(fcinfo.return_null()),
            }
        }
    )*};
}

xact_func_f8_ms! {
    fc_pg_stat_get_xact_function_total_time total_time;
    fc_pg_stat_get_xact_function_self_time self_time;
}

macro_rules! ckpt_i64 {
    ($($fc:ident $field:ident;)*) => {$(
        pub fn $fc(_fl: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            Ok(Datum::from_i64(pgstat::checkpointer::pgstat_fetch_stat_checkpointer().$field))
        }
    )*};
}

ckpt_i64! {
    fc_pg_stat_get_checkpointer_num_timed num_timed;
    fc_pg_stat_get_checkpointer_num_requested num_requested;
    fc_pg_stat_get_checkpointer_num_performed num_performed;
    fc_pg_stat_get_checkpointer_restartpoints_timed restartpoints_timed;
    fc_pg_stat_get_checkpointer_restartpoints_requested restartpoints_requested;
    fc_pg_stat_get_checkpointer_restartpoints_performed restartpoints_performed;
    fc_pg_stat_get_checkpointer_buffers_written buffers_written;
    fc_pg_stat_get_checkpointer_slru_written slru_written;
}

// C: time stored in msec, converted to double for presentation.
pub fn fc_pg_stat_get_checkpointer_write_time(
    _fl: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Ok(Datum::from_f64(pgstat::checkpointer::pgstat_fetch_stat_checkpointer().write_time as f64))
}

pub fn fc_pg_stat_get_checkpointer_sync_time(
    _fl: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Ok(Datum::from_f64(pgstat::checkpointer::pgstat_fetch_stat_checkpointer().sync_time as f64))
}

pub fn fc_pg_stat_get_checkpointer_stat_reset_time(
    _fl: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Ok(Datum::from_i64(
        pgstat::checkpointer::pgstat_fetch_stat_checkpointer().stat_reset_timestamp,
    ))
}

pub fn fc_pg_stat_get_bgwriter_buf_written_clean(
    _fl: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Ok(Datum::from_i64(pgstat::bgwriter::pgstat_fetch_stat_bgwriter().buf_written_clean))
}

pub fn fc_pg_stat_get_bgwriter_maxwritten_clean(
    _fl: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Ok(Datum::from_i64(pgstat::bgwriter::pgstat_fetch_stat_bgwriter().maxwritten_clean))
}

pub fn fc_pg_stat_get_bgwriter_stat_reset_time(
    _fl: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Ok(Datum::from_i64(pgstat::bgwriter::pgstat_fetch_stat_bgwriter().stat_reset_timestamp))
}

pub fn fc_pg_stat_get_buf_alloc(
    _fl: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Ok(Datum::from_i64(pgstat::bgwriter::pgstat_fetch_stat_bgwriter().buf_alloc))
}

pub fn fc_pg_stat_get_snapshot_timestamp(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    match pgstat::pgstat_get_stat_snapshot_timestamp() {
        Some(ts) => Ok(Datum::from_i64(ts)),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_pg_stat_have_stats(_fl: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let kind = pgstat::pgstat_get_kind_from_str(arg_text_str(fcinfo, 0)?)?;
    let dboid = fcinfo.arg(1).as_oid();
    let objid = fcinfo.arg(2).as_i64() as u64;
    Ok(Datum::from_bool(pgstat::pgstat_have_entry(kind.0, dboid, objid)))
}

pub fn fc_pg_stat_reset(_fl: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    pgstat::pgstat_reset_counters();
    Ok(Datum::from_usize(0))
}

pub fn fc_pg_stat_reset_shared(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    if fcinfo.argisnull(0) {
        pgstat::pgstat_reset_of_kind(PGSTAT_KIND_ARCHIVER);
        pgstat::pgstat_reset_of_kind(PGSTAT_KIND_BGWRITER);
        pgstat::pgstat_reset_of_kind(PGSTAT_KIND_CHECKPOINTER);
        pgstat::pgstat_reset_of_kind(PGSTAT_KIND_IO);
        if xlogprefetcher_seams::xlog_prefetch_reset_stats::is_installed() {
            xlogprefetcher_seams::xlog_prefetch_reset_stats::call();
        }
        pgstat::pgstat_reset_of_kind(PGSTAT_KIND_SLRU);
        pgstat::pgstat_reset_of_kind(PGSTAT_KIND_WAL);
        return Ok(Datum::from_usize(0));
    }
    match arg_text_str(fcinfo, 0)? {
        "archiver" => pgstat::pgstat_reset_of_kind(PGSTAT_KIND_ARCHIVER),
        "bgwriter" => pgstat::pgstat_reset_of_kind(PGSTAT_KIND_BGWRITER),
        "checkpointer" => pgstat::pgstat_reset_of_kind(PGSTAT_KIND_CHECKPOINTER),
        "io" => pgstat::pgstat_reset_of_kind(PGSTAT_KIND_IO),
        "recovery_prefetch" => {
            if xlogprefetcher_seams::xlog_prefetch_reset_stats::is_installed() {
                xlogprefetcher_seams::xlog_prefetch_reset_stats::call();
            }
        }
        "slru" => pgstat::pgstat_reset_of_kind(PGSTAT_KIND_SLRU),
        "wal" => pgstat::pgstat_reset_of_kind(PGSTAT_KIND_WAL),
        target => {
            return Err(Box::new(
                PgError::error(format!("unrecognized reset target: \"{target}\""))
                    .with_sqlstate(::types_error::ERRCODE_INVALID_PARAMETER_VALUE)
                    .with_hint(
                        "Target must be \"archiver\", \"bgwriter\", \"checkpointer\", \"io\", \
                         \"recovery_prefetch\", \"slru\", or \"wal\".",
                    ),
            ))
        }
    }
    Ok(Datum::from_usize(0))
}

pub fn fc_pg_stat_reset_single_table_counters(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let taboid = fcinfo.args_n::<1>()[0].value.as_oid();
    let dboid = if catalog_seams::is_shared_relation::call(taboid) {
        types_core::InvalidOid
    } else {
        init_small::globals::MyDatabaseId()
    };
    pgstat::pgstat_reset(PGSTAT_KIND_RELATION, dboid, taboid as u64);
    Ok(Datum::from_usize(0))
}

pub fn fc_pg_stat_reset_single_function_counters(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let funcoid = fcinfo.args_n::<1>()[0].value.as_oid();
    pgstat::pgstat_reset(PGSTAT_KIND_FUNCTION, init_small::globals::MyDatabaseId(), funcoid as u64);
    Ok(Datum::from_usize(0))
}

pub fn fc_pg_stat_reset_slru(_fl: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    if fcinfo.argisnull(0) {
        pgstat::pgstat_reset_of_kind(PGSTAT_KIND_SLRU);
    } else {
        pgstat::slru::pgstat_reset_slru(arg_text_str(fcinfo, 0)?);
    }
    Ok(Datum::from_usize(0))
}

pub fn fc_pg_stat_reset_backend_stats(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let backend_pid = fcinfo.args_n::<1>()[0].value.as_i32();
    let Some(proc_number) = stats::resolve_backend_proc_number(backend_pid) else {
        return Ok(Datum::from_usize(0));
    };
    let Some(beentry) = backend_status::pgstat_get_beentry_by_proc_number(proc_number) else {
        return Ok(Datum::from_usize(0));
    };
    if !pgstat::backend::pgstat_tracks_backend_bktype(beentry.st_backendType) {
        return Ok(Datum::from_usize(0));
    }
    pgstat::backend::pgstat_reset_backend(proc_number);
    Ok(Datum::from_usize(0))
}

pub fn fc_pg_stat_reset_replication_slot(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    if fcinfo.argisnull(0) {
        pgstat::pgstat_reset_of_kind(PGSTAT_KIND_REPLSLOT);
    } else {
        pgstat::replslot::pgstat_reset_replslot(arg_text_str(fcinfo, 0)?)?;
    }
    Ok(Datum::from_usize(0))
}

pub fn fc_pg_stat_reset_subscription_stats(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    if fcinfo.argisnull(0) {
        pgstat::pgstat_reset_of_kind(PGSTAT_KIND_SUBSCRIPTION);
    } else {
        let subid = fcinfo.args_n::<1>()[0].value.as_oid();
        if subid == ::types_core::InvalidOid {
            return Err(Box::new(
                PgError::error(format!("invalid subscription OID {subid}"))
                    .with_sqlstate(::types_error::ERRCODE_INVALID_PARAMETER_VALUE),
            ));
        }
        pgstat::pgstat_reset(PGSTAT_KIND_SUBSCRIPTION, ::types_core::InvalidOid, subid as u64);
    }
    Ok(Datum::from_usize(0))
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

const fn srf_nonstrict(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: false, retset: true, func }
}

const fn srf(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: true, func }
}

const fn nonstrict(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: false, retset: false, func }
}

pub const PGSTATFUNCS_BUILTINS: &[FmgrBuiltin] = &[
    b(1928, "pg_stat_get_numscans", 1, fc_pg_stat_get_numscans),
    b(6310, "pg_stat_get_lastscan", 1, fc_pg_stat_get_lastscan),
    b(1929, "pg_stat_get_tuples_returned", 1, fc_pg_stat_get_tuples_returned),
    b(1930, "pg_stat_get_tuples_fetched", 1, fc_pg_stat_get_tuples_fetched),
    b(1931, "pg_stat_get_tuples_inserted", 1, fc_pg_stat_get_tuples_inserted),
    b(1932, "pg_stat_get_tuples_updated", 1, fc_pg_stat_get_tuples_updated),
    b(1933, "pg_stat_get_tuples_deleted", 1, fc_pg_stat_get_tuples_deleted),
    b(1972, "pg_stat_get_tuples_hot_updated", 1, fc_pg_stat_get_tuples_hot_updated),
    b(6217, "pg_stat_get_tuples_newpage_updated", 1, fc_pg_stat_get_tuples_newpage_updated),
    b(2878, "pg_stat_get_live_tuples", 1, fc_pg_stat_get_live_tuples),
    b(2879, "pg_stat_get_dead_tuples", 1, fc_pg_stat_get_dead_tuples),
    b(3177, "pg_stat_get_mod_since_analyze", 1, fc_pg_stat_get_mod_since_analyze),
    b(5053, "pg_stat_get_ins_since_vacuum", 1, fc_pg_stat_get_ins_since_vacuum),
    b(1934, "pg_stat_get_blocks_fetched", 1, fc_pg_stat_get_blocks_fetched),
    b(1935, "pg_stat_get_blocks_hit", 1, fc_pg_stat_get_blocks_hit),
    b(2781, "pg_stat_get_last_vacuum_time", 1, fc_pg_stat_get_last_vacuum_time),
    b(2782, "pg_stat_get_last_autovacuum_time", 1, fc_pg_stat_get_last_autovacuum_time),
    b(2783, "pg_stat_get_last_analyze_time", 1, fc_pg_stat_get_last_analyze_time),
    b(2784, "pg_stat_get_last_autoanalyze_time", 1, fc_pg_stat_get_last_autoanalyze_time),
    b(3054, "pg_stat_get_vacuum_count", 1, fc_pg_stat_get_vacuum_count),
    b(3055, "pg_stat_get_autovacuum_count", 1, fc_pg_stat_get_autovacuum_count),
    b(3056, "pg_stat_get_analyze_count", 1, fc_pg_stat_get_analyze_count),
    b(3057, "pg_stat_get_autoanalyze_count", 1, fc_pg_stat_get_autoanalyze_count),
    b(6358, "pg_stat_get_total_vacuum_time", 1, fc_pg_stat_get_total_vacuum_time),
    b(6359, "pg_stat_get_total_autovacuum_time", 1, fc_pg_stat_get_total_autovacuum_time),
    b(6360, "pg_stat_get_total_analyze_time", 1, fc_pg_stat_get_total_analyze_time),
    b(6361, "pg_stat_get_total_autoanalyze_time", 1, fc_pg_stat_get_total_autoanalyze_time),
    b(1941, "pg_stat_get_db_numbackends", 1, fc_pg_stat_get_db_numbackends),
    b(1942, "pg_stat_get_db_xact_commit", 1, fc_pg_stat_get_db_xact_commit),
    b(1943, "pg_stat_get_db_xact_rollback", 1, fc_pg_stat_get_db_xact_rollback),
    b(1944, "pg_stat_get_db_blocks_fetched", 1, fc_pg_stat_get_db_blocks_fetched),
    b(1945, "pg_stat_get_db_blocks_hit", 1, fc_pg_stat_get_db_blocks_hit),
    b(2758, "pg_stat_get_db_tuples_returned", 1, fc_pg_stat_get_db_tuples_returned),
    b(2759, "pg_stat_get_db_tuples_fetched", 1, fc_pg_stat_get_db_tuples_fetched),
    b(2760, "pg_stat_get_db_tuples_inserted", 1, fc_pg_stat_get_db_tuples_inserted),
    b(2761, "pg_stat_get_db_tuples_updated", 1, fc_pg_stat_get_db_tuples_updated),
    b(2762, "pg_stat_get_db_tuples_deleted", 1, fc_pg_stat_get_db_tuples_deleted),
    b(3065, "pg_stat_get_db_conflict_tablespace", 1, fc_pg_stat_get_db_conflict_tablespace),
    b(3066, "pg_stat_get_db_conflict_lock", 1, fc_pg_stat_get_db_conflict_lock),
    b(3067, "pg_stat_get_db_conflict_snapshot", 1, fc_pg_stat_get_db_conflict_snapshot),
    b(6309, "pg_stat_get_db_conflict_logicalslot", 1, fc_pg_stat_get_db_conflict_logicalslot),
    b(3068, "pg_stat_get_db_conflict_bufferpin", 1, fc_pg_stat_get_db_conflict_bufferpin),
    b(
        3069,
        "pg_stat_get_db_conflict_startup_deadlock",
        1,
        fc_pg_stat_get_db_conflict_startup_deadlock,
    ),
    b(3070, "pg_stat_get_db_conflict_all", 1, fc_pg_stat_get_db_conflict_all),
    b(3152, "pg_stat_get_db_deadlocks", 1, fc_pg_stat_get_db_deadlocks),
    b(3426, "pg_stat_get_db_checksum_failures", 1, fc_pg_stat_get_db_checksum_failures),
    b(3428, "pg_stat_get_db_checksum_last_failure", 1, fc_pg_stat_get_db_checksum_last_failure),
    b(3074, "pg_stat_get_db_stat_reset_time", 1, fc_pg_stat_get_db_stat_reset_time),
    b(3150, "pg_stat_get_db_temp_files", 1, fc_pg_stat_get_db_temp_files),
    b(3151, "pg_stat_get_db_temp_bytes", 1, fc_pg_stat_get_db_temp_bytes),
    b(2844, "pg_stat_get_db_blk_read_time", 1, fc_pg_stat_get_db_blk_read_time),
    b(2845, "pg_stat_get_db_blk_write_time", 1, fc_pg_stat_get_db_blk_write_time),
    b(6185, "pg_stat_get_db_session_time", 1, fc_pg_stat_get_db_session_time),
    b(6186, "pg_stat_get_db_active_time", 1, fc_pg_stat_get_db_active_time),
    b(6187, "pg_stat_get_db_idle_in_transaction_time", 1, fc_pg_stat_get_db_idle_in_transaction_time),
    b(6188, "pg_stat_get_db_sessions", 1, fc_pg_stat_get_db_sessions),
    b(6189, "pg_stat_get_db_sessions_abandoned", 1, fc_pg_stat_get_db_sessions_abandoned),
    b(6190, "pg_stat_get_db_sessions_fatal", 1, fc_pg_stat_get_db_sessions_fatal),
    b(6191, "pg_stat_get_db_sessions_killed", 1, fc_pg_stat_get_db_sessions_killed),
    b(
        6355,
        "pg_stat_get_db_parallel_workers_to_launch",
        1,
        fc_pg_stat_get_db_parallel_workers_to_launch,
    ),
    b(
        6356,
        "pg_stat_get_db_parallel_workers_launched",
        1,
        fc_pg_stat_get_db_parallel_workers_launched,
    ),
    b(2026, "pg_backend_pid", 0, fc_pg_backend_pid),
    FmgrBuiltin {
        foid: 2137,
        name: "pg_stat_force_next_flush",
        nargs: 0,
        strict: false,
        retset: false,
        func: fc_pg_stat_force_next_flush,
    },
    FmgrBuiltin {
        foid: 2230,
        name: "pg_stat_clear_snapshot",
        nargs: 0,
        strict: false,
        retset: false,
        func: fc_pg_stat_clear_snapshot,
    },
    srf_nonstrict(2022, "pg_stat_get_activity", 1, fc_pg_stat_get_activity),
    srf(1936, "pg_stat_get_backend_idset", 0, backend::fc_pg_stat_get_backend_idset),
    b(1937, "pg_stat_get_backend_pid", 1, backend::fc_pg_stat_get_backend_pid),
    b(1938, "pg_stat_get_backend_dbid", 1, backend::fc_pg_stat_get_backend_dbid),
    b(1939, "pg_stat_get_backend_userid", 1, backend::fc_pg_stat_get_backend_userid),
    b(1940, "pg_stat_get_backend_activity", 1, backend::fc_pg_stat_get_backend_activity),
    b(
        2788,
        "pg_stat_get_backend_wait_event_type",
        1,
        backend::fc_pg_stat_get_backend_wait_event_type,
    ),
    b(2853, "pg_stat_get_backend_wait_event", 1, backend::fc_pg_stat_get_backend_wait_event),
    b(
        2094,
        "pg_stat_get_backend_activity_start",
        1,
        backend::fc_pg_stat_get_backend_activity_start,
    ),
    b(2857, "pg_stat_get_backend_xact_start", 1, backend::fc_pg_stat_get_backend_xact_start),
    b(1391, "pg_stat_get_backend_start", 1, backend::fc_pg_stat_get_backend_start),
    b(1392, "pg_stat_get_backend_client_addr", 1, backend::fc_pg_stat_get_backend_client_addr),
    b(1393, "pg_stat_get_backend_client_port", 1, backend::fc_pg_stat_get_backend_client_port),
    b(3037, "pg_stat_get_xact_numscans", 1, fc_pg_stat_get_xact_numscans),
    b(3038, "pg_stat_get_xact_tuples_returned", 1, fc_pg_stat_get_xact_tuples_returned),
    b(3039, "pg_stat_get_xact_tuples_fetched", 1, fc_pg_stat_get_xact_tuples_fetched),
    b(3040, "pg_stat_get_xact_tuples_inserted", 1, fc_pg_stat_get_xact_tuples_inserted),
    b(3041, "pg_stat_get_xact_tuples_updated", 1, fc_pg_stat_get_xact_tuples_updated),
    b(3042, "pg_stat_get_xact_tuples_deleted", 1, fc_pg_stat_get_xact_tuples_deleted),
    b(3043, "pg_stat_get_xact_tuples_hot_updated", 1, fc_pg_stat_get_xact_tuples_hot_updated),
    b(
        6218,
        "pg_stat_get_xact_tuples_newpage_updated",
        1,
        fc_pg_stat_get_xact_tuples_newpage_updated,
    ),
    b(3044, "pg_stat_get_xact_blocks_fetched", 1, fc_pg_stat_get_xact_blocks_fetched),
    b(3045, "pg_stat_get_xact_blocks_hit", 1, fc_pg_stat_get_xact_blocks_hit),
    b(2978, "pg_stat_get_function_calls", 1, fc_pg_stat_get_function_calls),
    b(2979, "pg_stat_get_function_total_time", 1, fc_pg_stat_get_function_total_time),
    b(2980, "pg_stat_get_function_self_time", 1, fc_pg_stat_get_function_self_time),
    b(3046, "pg_stat_get_xact_function_calls", 1, fc_pg_stat_get_xact_function_calls),
    b(3047, "pg_stat_get_xact_function_total_time", 1, fc_pg_stat_get_xact_function_total_time),
    b(3048, "pg_stat_get_xact_function_self_time", 1, fc_pg_stat_get_xact_function_self_time),
    b(3788, "pg_stat_get_snapshot_timestamp", 0, fc_pg_stat_get_snapshot_timestamp),
    b(6230, "pg_stat_have_stats", 3, fc_pg_stat_have_stats),
    nonstrict(2274, "pg_stat_reset", 0, fc_pg_stat_reset),
    nonstrict(3775, "pg_stat_reset_shared", 1, fc_pg_stat_reset_shared),
    b(
        3776,
        "pg_stat_reset_single_table_counters",
        1,
        fc_pg_stat_reset_single_table_counters,
    ),
    b(
        3777,
        "pg_stat_reset_single_function_counters",
        1,
        fc_pg_stat_reset_single_function_counters,
    ),
    nonstrict(2307, "pg_stat_reset_slru", 1, fc_pg_stat_reset_slru),
    b(6387, "pg_stat_reset_backend_stats", 1, fc_pg_stat_reset_backend_stats),
    b(2769, "pg_stat_get_checkpointer_num_timed", 0, fc_pg_stat_get_checkpointer_num_timed),
    b(
        2770,
        "pg_stat_get_checkpointer_num_requested",
        0,
        fc_pg_stat_get_checkpointer_num_requested,
    ),
    b(
        6377,
        "pg_stat_get_checkpointer_num_performed",
        0,
        fc_pg_stat_get_checkpointer_num_performed,
    ),
    b(
        6327,
        "pg_stat_get_checkpointer_restartpoints_timed",
        0,
        fc_pg_stat_get_checkpointer_restartpoints_timed,
    ),
    b(
        6328,
        "pg_stat_get_checkpointer_restartpoints_requested",
        0,
        fc_pg_stat_get_checkpointer_restartpoints_requested,
    ),
    b(
        6329,
        "pg_stat_get_checkpointer_restartpoints_performed",
        0,
        fc_pg_stat_get_checkpointer_restartpoints_performed,
    ),
    b(
        2771,
        "pg_stat_get_checkpointer_buffers_written",
        0,
        fc_pg_stat_get_checkpointer_buffers_written,
    ),
    b(
        6366,
        "pg_stat_get_checkpointer_slru_written",
        0,
        fc_pg_stat_get_checkpointer_slru_written,
    ),
    b(3160, "pg_stat_get_checkpointer_write_time", 0, fc_pg_stat_get_checkpointer_write_time),
    b(3161, "pg_stat_get_checkpointer_sync_time", 0, fc_pg_stat_get_checkpointer_sync_time),
    b(
        6314,
        "pg_stat_get_checkpointer_stat_reset_time",
        0,
        fc_pg_stat_get_checkpointer_stat_reset_time,
    ),
    b(2772, "pg_stat_get_bgwriter_buf_written_clean", 0, fc_pg_stat_get_bgwriter_buf_written_clean),
    b(2773, "pg_stat_get_bgwriter_maxwritten_clean", 0, fc_pg_stat_get_bgwriter_maxwritten_clean),
    b(3075, "pg_stat_get_bgwriter_stat_reset_time", 0, fc_pg_stat_get_bgwriter_stat_reset_time),
    b(2859, "pg_stat_get_buf_alloc", 0, fc_pg_stat_get_buf_alloc),
    srf(3318, "pg_stat_get_progress_info", 1, stats::fc_pg_stat_get_progress_info),
    b(6107, "pg_stat_get_backend_subxact", 1, stats::fc_pg_stat_get_backend_subxact),
    nonstrict(3195, "pg_stat_get_archiver", 0, stats::fc_pg_stat_get_archiver),
    b(6169, "pg_stat_get_replication_slot", 1, stats::fc_pg_stat_get_replication_slot),
    b(6231, "pg_stat_get_subscription_stats", 1, stats::fc_pg_stat_get_subscription_stats),
    nonstrict(6170, "pg_stat_reset_replication_slot", 1, fc_pg_stat_reset_replication_slot),
    nonstrict(6232, "pg_stat_reset_subscription_stats", 1, fc_pg_stat_reset_subscription_stats),
    srf(6214, "pg_stat_get_io", 0, stats::fc_pg_stat_get_io),
    srf(6386, "pg_stat_get_backend_io", 1, stats::fc_pg_stat_get_backend_io),
    nonstrict(1136, "pg_stat_get_wal", 0, stats::fc_pg_stat_get_wal),
    b(6313, "pg_stat_get_backend_wal", 1, stats::fc_pg_stat_get_backend_wal),
    srf_nonstrict(2306, "pg_stat_get_slru", 0, stats::fc_pg_stat_get_slru),
    nonstrict(3317, "pg_stat_get_wal_receiver", 0, stats::fc_pg_stat_get_wal_receiver),
    srf_nonstrict(3099, "pg_stat_get_wal_senders", 0, stats::fc_pg_stat_get_wal_senders),
];
