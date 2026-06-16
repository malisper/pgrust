//! `tcop/postgres.c` — the PostgreSQL backend's command processor
//! (`src/backend/tcop/postgres.c`, PostgreSQL 18.3).
//!
//! This crate owns the **fireable-now** families of `postgres.c`:
//!
//!   * **F3 — signal / interrupt machinery** ([`interrupt`]):
//!     `ProcessInterrupts`, `die`, `quickdie`, `StatementCancelHandler`,
//!     `FloatExceptionHandler`, `HandleRecoveryConflictInterrupt`,
//!     `ProcessRecoveryConflictInterrupt(s)`, `ProcessClientReadInterrupt`,
//!     `ProcessClientWriteInterrupt`, the `CHECK_FOR_INTERRUPTS` body, and the
//!     interrupt-flag plumbing.
//!   * **F5 — logging / duration / resource usage** ([`logging`]):
//!     `check_log_duration`, `errdetail_recovery_conflict`, `errdetail_abort`,
//!     `ResetUsage`, `ShowUsage`, `log_disconnections`, `log_statement_is_all`,
//!     `log_executor_stats`, `enable_statement_timeout`,
//!     `disable_statement_timeout`.
//!   * **F6 — command-line switches + GUC check/assign hooks** ([`guc`]):
//!     `process_postgres_switches`, `set_debug_options`,
//!     `set_plan_disabling_options`, `get_stats_option_name`,
//!     `forbidden_in_wal_sender`, the postgres.c GUC check/assign hooks, and the
//!     `restrict_nonsystem_relation_kind` int global.
//!
//! The `postgres.c` file-local globals are owned in [`globals`].
//!
//! NOT in this crate (planner-gated, Families F0a/F1/F2): the `PostgresMain` /
//! `PostgresSingleUserMain` main loop, the simple-query (`exec_simple_query`)
//! and extended-query (`exec_parse_message`/`exec_bind_message`/
//! `exec_execute_message`/describe) pipelines, `pg_parse_query`/`pg_plan_query`/
//! `pg_plan_queries`/the `pg_analyze_and_rewrite_*` family, `start_xact_command`/
//! `finish_xact_command`, `InteractiveBackend`/`SocketBackend`/`ReadCommand`,
//! and the F1/F2-coupled logging helpers `check_log_statement`,
//! `errdetail_execute`, `errdetail_params`. Those land with the planner; the
//! seam decls for the ones other units reference (`postgres_main`,
//! `pg_parse_query`, `pg_plan_query`, `set_stack_base`,
//! `install_bgworker_signal_handlers`, the lock-wait/autovac report seams) stay
//! seam-and-panic until then.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

extern crate alloc;

pub mod globals;
pub mod guc;
pub mod interrupt;
pub mod logging;

#[cfg(test)]
mod tests;

use backend_tcop_postgres_seams as s;

/// `fn(i32)` wrapper around [`interrupt::quickdie`] (which is `-> !`) so it
/// matches the `void (*)(int)` `pqsigfunc` shape the postmaster installs.
fn quickdie_handler(signo: i32) {
    interrupt::quickdie(signo)
}

/// Install this crate's implementations into its seam crate, the
/// `restrict_nonsystem_relation_kind` reader into the GUC-tables seam, and the
/// GUC check/assign hook fns into the GUC tables' typed slots.
///
/// Installs the F3/F5/F6 seams that `postgres.c` owns and that other (already
/// merged) units consume — retiring the latent panics they carried while
/// `tcop/postgres.c` was CATALOG `todo`.
pub fn init_seams() {
    // --- F3: interrupt / signal machinery ---
    s::check_for_interrupts::set(interrupt::check_for_interrupts);
    s::process_client_read_interrupt::set(interrupt::ProcessClientReadInterrupt);
    s::process_client_write_interrupt::set(interrupt::ProcessClientWriteInterrupt);
    s::handle_recovery_conflict_interrupt::set(interrupt::HandleRecoveryConflictInterrupt);
    s::die::set(interrupt::die);
    s::statement_cancel_handler::set(interrupt::StatementCancelHandler);

    // --- F5: logging / duration / resource usage ---
    s::log_statement_is_all::set(logging::log_statement_is_all);
    s::log_executor_stats::set(logging::log_executor_stats);
    s::check_log_duration::set(logging::check_log_duration);
    s::reset_usage::set(logging::ResetUsage);
    s::show_usage::set(|title| {
        // `ShowUsage` is `void` in C; its `ereport(LOG, ...)` cannot surface a
        // FATAL+ here, so any propagated error is dropped to keep the seam's
        // infallible contract (mirroring the C void return).
        let _ = logging::ShowUsage(title);
    });

    // --- F3/F5: whereToSendOutput + debug_query_string globals ---
    s::where_to_send_output::set(globals::where_to_send_output);
    s::set_where_to_send_output_none::set(|| {
        globals::set_where_to_send_output(types_dest::dest::CommandDest::None)
    });
    s::reset_debug_query_string::set(|| globals::set_debug_query_string(None));

    // --- F6: command-line switches + GUC reads ---
    s::process_postgres_switches::set(|argv, ctx| {
        // The seam contract returns `PgResult<()>`; the captured dbname is only
        // consumed by `PostgresMain` (F0a) which calls the in-crate
        // `guc::process_postgres_switches` directly once it lands.
        guc::process_postgres_switches(argv, ctx).map(|_dbname| ())
    });
    s::post_auth_delay::set(backend_utils_init_small::globals::post_auth_delay);
    s::set_transaction_timeout_pending::set(
        backend_utils_init_small::globals::SetTransactionTimeoutPending,
    );
    s::set_idle_in_transaction_session_timeout_pending::set(
        backend_utils_init_small::globals::SetIdleInTransactionSessionTimeoutPending,
    );
    s::set_idle_session_timeout_pending::set(
        backend_utils_init_small::globals::SetIdleSessionTimeoutPending,
    );
    s::set_idle_stats_update_timeout_pending::set(
        backend_utils_init_small::globals::SetIdleStatsUpdateTimeoutPending,
    );
    s::set_check_client_connection_pending::set(
        backend_utils_init_small::globals::SetCheckClientConnectionPending,
    );

    // --- F6: GUC check/assign hooks + restrict_nonsystem_relation_kind reader ---
    backend_utils_misc_guc_tables_seams::restrict_nonsystem_relation_kind::set(
        guc::restrict_nonsystem_relation_kind,
    );
    guc::install_guc_hooks();

    // Reference `quickdie_handler` so the `pqsigfunc`-shaped wrapper is kept and
    // available for the postmaster's SIGQUIT install (done by F0a when it lands).
    let _: fn(i32) = quickdie_handler;
}
