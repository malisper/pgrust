#![allow(non_snake_case)]

use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

mod activity;
pub use activity::fc_pg_stat_get_activity;

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

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

const fn srf_nonstrict(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: false, retset: true, func }
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
];
