//! contrib/pg_stat_statements: per-statement plan/exec statistics keyed by
//! (userid, dbid, queryid, toplevel). C's shared-memory hashtable + external
//! query-text file collapse to one process-global mutexed map with inline
//! texts (thread-per-backend; same rendering as pgstat's shmem.rs), so the
//! qtext file, its GC, and the LWLock/spinlock split have no counterpart.

#![allow(non_snake_case)]

use std::cell::Cell;
use std::collections::HashMap;
use std::sync::Mutex;

use datum::Datum;
use rustc_hash::FxBuildHasher;
use types_core::Oid;
use types_error::{PgError, PgResult};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

mod hooks;
mod normalize;
mod srf;
mod store;
#[cfg(test)]
mod tests;

pub(crate) const PGSS_DUMP_FILE: &str = "pg_stat/pg_stat_statements.stat";
pub(crate) const PGSS_FILE_HEADER: u32 = 0x2022_0408;
pub(crate) const PGSS_PG_MAJOR_VERSION: u32 = 1800;

pub(crate) const USAGE_INIT: f64 = 1.0;
pub(crate) const USAGE_EXEC: f64 = 1.0;
pub(crate) const ASSUMED_MEDIAN_INIT: f64 = 10.0;
pub(crate) const ASSUMED_LENGTH_INIT: usize = 1024;
pub(crate) const USAGE_DECREASE_FACTOR: f64 = 0.99;
pub(crate) const STICKY_DECREASE_FACTOR: f64 = 0.50;
pub(crate) const USAGE_DEALLOC_PERCENT: usize = 5;

pub(crate) const PGSS_PLAN: usize = 0;
pub(crate) const PGSS_EXEC: usize = 1;
pub(crate) const PGSS_NUMKIND: usize = 2;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct PgssHashKey {
    pub userid: Oid,
    pub dbid: Oid,
    pub queryid: i64,
    pub toplevel: bool,
}

// Field set and dump-file order mirror C's Counters exactly (all members are
// 8-byte scalars there too).
#[derive(Clone, Copy, Default)]
pub(crate) struct Counters {
    pub calls: [i64; PGSS_NUMKIND],
    pub total_time: [f64; PGSS_NUMKIND],
    pub min_time: [f64; PGSS_NUMKIND],
    pub max_time: [f64; PGSS_NUMKIND],
    pub mean_time: [f64; PGSS_NUMKIND],
    pub sum_var_time: [f64; PGSS_NUMKIND],
    pub rows: i64,
    pub shared_blks_hit: i64,
    pub shared_blks_read: i64,
    pub shared_blks_dirtied: i64,
    pub shared_blks_written: i64,
    pub local_blks_hit: i64,
    pub local_blks_read: i64,
    pub local_blks_dirtied: i64,
    pub local_blks_written: i64,
    pub temp_blks_read: i64,
    pub temp_blks_written: i64,
    pub shared_blk_read_time: f64,
    pub shared_blk_write_time: f64,
    pub local_blk_read_time: f64,
    pub local_blk_write_time: f64,
    pub temp_blk_read_time: f64,
    pub temp_blk_write_time: f64,
    pub usage: f64,
    pub wal_records: i64,
    pub wal_fpi: i64,
    pub wal_bytes: u64,
    pub wal_buffers_full: i64,
    pub jit_functions: i64,
    pub jit_generation_time: f64,
    pub jit_inlining_count: i64,
    pub jit_deform_time: f64,
    pub jit_deform_count: i64,
    pub jit_inlining_time: f64,
    pub jit_optimization_count: i64,
    pub jit_optimization_time: f64,
    pub jit_emission_count: i64,
    pub jit_emission_time: f64,
    pub parallel_workers_to_launch: i64,
    pub parallel_workers_launched: i64,
}

impl Counters {
    pub fn is_sticky(&self) -> bool {
        self.calls[PGSS_PLAN] + self.calls[PGSS_EXEC] == 0
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) struct PgssGlobalStats {
    pub dealloc: i64,
    pub stats_reset: i64,
}

pub(crate) struct PgssEntry {
    pub counters: Counters,
    // C: offset+len into PGSS_TEXT_FILE; inline here (see module comment).
    pub query_text: String,
    pub encoding: i32,
    pub stats_since: i64,
    pub minmax_stats_since: i64,
}

pub(crate) struct PgssShared {
    pub hash: HashMap<PgssHashKey, PgssEntry, FxBuildHasher>,
    pub max: usize,
    pub cur_median_usage: f64,
    pub mean_query_len: usize,
    pub stats: PgssGlobalStats,
}

// pgss + pgss_hash: None until _PG_init runs under shared_preload_libraries.
pub(crate) static PGSS: Mutex<Option<PgssShared>> = Mutex::new(None);

thread_local! {
    // C: static int nesting_level (planner/ExecutorRun/ProcessUtility depth).
    static NESTING_LEVEL: Cell<i32> = const { Cell::new(0) };
}

pub(crate) fn nesting_level() -> i32 {
    NESTING_LEVEL.get()
}

pub(crate) fn nesting_add(d: i32) {
    NESTING_LEVEL.set(NESTING_LEVEL.get() + d);
}

pub(crate) mod gucs {
    guc_tables::session_guc_cluster!(PgssGucs, PGSS_GUCS:
        (pgss_max_cell, i32, pgss_max, set_pgss_max, 5000),
        (pgss_track_cell, i32, pgss_track, set_pgss_track, guc_tables::consts::PGSS_TRACK_TOP),
        (pgss_track_utility_cell, bool, pgss_track_utility, set_pgss_track_utility, true),
        (pgss_track_planning_cell, bool, pgss_track_planning, set_pgss_track_planning, false),
        (pgss_save_cell, bool, pgss_save, set_pgss_save, true),
    );
}

pub(crate) fn pgss_enabled(level: i32) -> bool {
    !parallel::IsParallelWorker()
        && (gucs::pgss_track() == guc_tables::consts::PGSS_TRACK_ALL
            || (gucs::pgss_track() == guc_tables::consts::PGSS_TRACK_TOP && level == 0))
}

pub(crate) fn not_loaded_error() -> Box<PgError> {
    PgError::error("pg_stat_statements must be loaded via \"shared_preload_libraries\"")
        .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        .into()
}

const LIBRARY: &str = "pg_stat_statements";

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "pg_stat_statements_reset" => srf::fc_reset,
        "pg_stat_statements_reset_1_7" => srf::fc_reset_1_7,
        "pg_stat_statements_reset_1_11" => srf::fc_reset_1_11,
        "pg_stat_statements" => srf::fc_pgss_1_0,
        "pg_stat_statements_1_2" => srf::fc_pgss_1_2,
        "pg_stat_statements_1_3" => srf::fc_pgss_1_3,
        "pg_stat_statements_1_8" => srf::fc_pgss_1_8,
        "pg_stat_statements_1_9" => srf::fc_pgss_1_9,
        "pg_stat_statements_1_10" => srf::fc_pgss_1_10,
        "pg_stat_statements_1_11" => srf::fc_pgss_1_11,
        "pg_stat_statements_1_12" => srf::fc_pgss_1_12,
        "pg_stat_statements_info" => srf::fc_pgss_info,
        _ => return None,
    })
}

pub fn init_seams() {
    use guc_tables::GucVarAccessors;
    guc_tables::vars::pgss_max
        .install(GucVarAccessors { get: gucs::pgss_max, set: gucs::set_pgss_max });
    guc_tables::vars::pgss_track
        .install(GucVarAccessors { get: gucs::pgss_track, set: gucs::set_pgss_track });
    guc_tables::vars::pgss_track_utility.install(GucVarAccessors {
        get: gucs::pgss_track_utility,
        set: gucs::set_pgss_track_utility,
    });
    guc_tables::vars::pgss_track_planning.install(GucVarAccessors {
        get: gucs::pgss_track_planning,
        set: gucs::set_pgss_track_planning,
    });
    guc_tables::vars::pgss_save
        .install(GucVarAccessors { get: gucs::pgss_save, set: gucs::set_pgss_save });

    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: Some(pg_init),
    });
}

/// `_PG_init` (pg_stat_statements.c).
fn pg_init() -> PgResult<()> {
    if !miscinit::process_shared_preload_libraries_in_progress() {
        return Ok(());
    }

    queryjumble::EnableQueryId();

    {
        let mut guard = PGSS.lock().unwrap();
        if guard.is_none() {
            let mut shared = PgssShared {
                hash: HashMap::with_hasher(FxBuildHasher),
                max: gucs::pgss_max().max(100) as usize,
                cur_median_usage: ASSUMED_MEDIAN_INIT,
                mean_query_len: ASSUMED_LENGTH_INIT,
                stats: PgssGlobalStats {
                    dealloc: 0,
                    stats_reset: adt_timestamp::GetCurrentTimestamp(),
                },
            };
            if gucs::pgss_save() {
                store::load_dump_file(&mut shared);
            }
            *guard = Some(shared);
        }
    }

    // C: on_shmem_exit(pgss_shmem_shutdown) in the postmaster; here the boot
    // thread IS the postmaster thread, so ExitPostmaster's proc_exit runs it.
    ipc::on_shmem_exit(store::pgss_shmem_shutdown, 0);

    parser_analyze::tap_post_parse_analyze::install(hooks::pgss_post_parse_analyze);
    planner::tap_planner_enter::install(hooks::pgss_planner_enter);
    planner::tap_planner_leave::install(hooks::pgss_planner_leave);
    // Executor hooks go through the exec_hooks chain (not the raw taps): the
    // taps are install-once and auto_explain shares them — C's saved-prev
    // hook chaining, centralized.
    exec_hooks::register(exec_hooks::ExecutorHooks {
        start: Some(hooks::pgss_executor_start),
        run: Some(hooks::pgss_executor_run),
        run_leave: Some(hooks::pgss_executor_run_leave),
        finish: Some(hooks::pgss_executor_finish),
        finish_leave: Some(hooks::pgss_executor_finish_leave),
        end: Some(hooks::pgss_executor_end),
    });
    utility::dispatch::tap_process_utility_enter::install(hooks::pgss_process_utility_enter);
    utility::dispatch::tap_process_utility_leave::install(hooks::pgss_process_utility_leave);

    Ok(())
}

pub(crate) fn text_datum(fcinfo: &Fcinfo, s: &str) -> PgResult<Datum> {
    let img = varlena::cstring_to_text(fcinfo.result_mcx(), s.as_bytes())?;
    Ok(types_fmgr::varlena_result(img))
}

pub(crate) fn resolved_flinfo<'a>(
    flinfo: Option<&'a mut FmgrInfo>,
    what: &str,
) -> &'a mut FmgrInfo {
    flinfo.unwrap_or_else(|| panic!("{what}: resolved FmgrInfo required"))
}
