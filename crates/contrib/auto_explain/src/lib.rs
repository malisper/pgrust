//! `contrib/auto_explain/auto_explain.c` — log the EXPLAIN plan of any query
//! whose execution time crosses `auto_explain.log_min_duration`.
//!
//! Hook-only preload module: no SQL surface, no extension script. It rides
//! the same executor tap seams pg_stat_statements uses, via the exec_hooks
//! chain (C chains `prev_ExecutorStart` hands; the tap slots are
//! install-once, so the chain lives in exec_hooks). When the module is not
//! preloaded nothing installs into the taps and the executor pays only the
//! `call_if` null test — the same not-loaded zero cost as pgss.
//!
//! Threaded-server divergence (documented, C loads per-process): hooks are
//! process-global and can only be installed in the boot window, so hook
//! activation requires `shared_preload_libraries`. A session-level `LOAD
//! 'auto_explain'` still runs `_PG_init` for that session — reserving the
//! GUC prefix exactly like C (the alter_reset corpus path) — but cannot add
//! executor hooks after boot. The GUC gate (`log_min_duration = -1` default,
//! per-session values) keeps per-session enable/disable semantics identical
//! to C under shared preload.

#![allow(non_snake_case)]

use types_error::PgResult;
use types_fmgr::PGFunction;

mod hooks;

const LIBRARY: &str = "auto_explain";

pub(crate) mod gucs {
    guc_tables::session_guc_cluster!(AexGucs, AEX_GUCS:
        (aex_log_min_duration_cell, i32, log_min_duration, set_log_min_duration, -1),
        (aex_log_parameter_max_length_cell, i32, log_parameter_max_length, set_log_parameter_max_length, -1),
        (aex_log_analyze_cell, bool, log_analyze, set_log_analyze, false),
        (aex_log_settings_cell, bool, log_settings, set_log_settings, false),
        (aex_log_verbose_cell, bool, log_verbose, set_log_verbose, false),
        (aex_log_buffers_cell, bool, log_buffers, set_log_buffers, false),
        (aex_log_wal_cell, bool, log_wal, set_log_wal, false),
        (aex_log_triggers_cell, bool, log_triggers, set_log_triggers, false),
        (aex_log_timing_cell, bool, log_timing, set_log_timing, true),
        (aex_log_nested_statements_cell, bool, log_nested_statements, set_log_nested_statements, false),
        (aex_log_format_cell, i32, log_format, set_log_format, guc_tables::consts::EXPLAIN_FORMAT_TEXT),
        (aex_log_level_cell, i32, log_level, set_log_level, guc_tables::consts::LOG),
    );
    guc_tables::session_guc_real!(
        AEX_SAMPLE_RATE,
        sample_rate,
        set_sample_rate,
        1.0
    );
}

fn lookup(_function: &str) -> Option<PGFunction> {
    // auto_explain exposes no SQL functions.
    None
}

pub fn init_seams() {
    use guc_tables::GucVarAccessors;
    guc_tables::vars::aex_log_min_duration.install(GucVarAccessors {
        get: gucs::log_min_duration,
        set: gucs::set_log_min_duration,
    });
    guc_tables::vars::aex_log_parameter_max_length.install(GucVarAccessors {
        get: gucs::log_parameter_max_length,
        set: gucs::set_log_parameter_max_length,
    });
    guc_tables::vars::aex_log_analyze
        .install(GucVarAccessors { get: gucs::log_analyze, set: gucs::set_log_analyze });
    guc_tables::vars::aex_log_settings
        .install(GucVarAccessors { get: gucs::log_settings, set: gucs::set_log_settings });
    guc_tables::vars::aex_log_verbose
        .install(GucVarAccessors { get: gucs::log_verbose, set: gucs::set_log_verbose });
    guc_tables::vars::aex_log_buffers
        .install(GucVarAccessors { get: gucs::log_buffers, set: gucs::set_log_buffers });
    guc_tables::vars::aex_log_wal
        .install(GucVarAccessors { get: gucs::log_wal, set: gucs::set_log_wal });
    guc_tables::vars::aex_log_triggers
        .install(GucVarAccessors { get: gucs::log_triggers, set: gucs::set_log_triggers });
    guc_tables::vars::aex_log_timing
        .install(GucVarAccessors { get: gucs::log_timing, set: gucs::set_log_timing });
    guc_tables::vars::aex_log_nested_statements.install(GucVarAccessors {
        get: gucs::log_nested_statements,
        set: gucs::set_log_nested_statements,
    });
    guc_tables::vars::aex_log_format
        .install(GucVarAccessors { get: gucs::log_format, set: gucs::set_log_format });
    guc_tables::vars::aex_log_level
        .install(GucVarAccessors { get: gucs::log_level, set: gucs::set_log_level });
    guc_tables::vars::aex_sample_rate
        .install(GucVarAccessors { get: gucs::sample_rate, set: gucs::set_sample_rate });

    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: Some(pg_init),
    });
}

/// `_PG_init` (auto_explain.c). C installs its executor hooks from any load
/// context; here hook installation is boot-window-only (see module comment),
/// so the executor-hook registration happens only under
/// shared_preload_libraries. The GUC prefix reservation runs on every load,
/// like C's MarkGUCPrefixReserved after the GUC definitions.
fn pg_init() -> PgResult<()> {
    guc::MarkGUCPrefixReserved("auto_explain");

    if !miscinit::process_shared_preload_libraries_in_progress() {
        return Ok(());
    }

    exec_hooks::register(exec_hooks::ExecutorHooks {
        start: Some(hooks::explain_executor_start),
        run: Some(hooks::explain_executor_run),
        run_leave: Some(hooks::explain_executor_run_leave),
        finish: Some(hooks::explain_executor_finish),
        finish_leave: Some(hooks::explain_executor_finish_leave),
        end: Some(hooks::explain_executor_end),
    });

    Ok(())
}
