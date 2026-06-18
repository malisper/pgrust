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
//! Also landed:
//!
//!   * **F0a — the backend main loop** ([`main_loop`]): `PostgresMain`, the
//!     `for (;;)` ReadCommand loop with the `sigsetjmp`-equivalent
//!     `PgResult`-recovery block, idle-state handling / `ReadyForQuery`, and the
//!     message-tag dispatch — `'Q'`→`exec_simple_query` (landed end-to-end),
//!     `'F'`→fastpath (landed), `'C'`/`'S'`/`'H'` ported, `'X'`/EOF terminate,
//!     COPY-data accepted-and-ignored; the extended-query `'P'`/`'B'`/`'E'`/`'D'`
//!     exec functions seam-panic (F2 plancache path, unported). `ReadCommand` /
//!     `SocketBackend` / `forbidden_in_wal_sender` are here too.
//!   * **F1 — the simple-Query pipeline** ([`simple_query`]): `exec_simple_query`
//!     and `pg_parse_query`/`pg_analyze_and_rewrite_fixedparams`/`pg_rewrite_query`/
//!     `pg_plan_query`/`pg_plan_queries`, `start_xact_command`/`finish_xact_command`,
//!     `check_log_statement`, `drop_unnamed_stmt`.
//!
//!   * **F0b — the single-user standalone entry** ([`single_user`]):
//!     `PostgresSingleUserMain`, the `--single` (`DISPATCH_SINGLE`) driver. It
//!     runs the standalone bootstrap (`InitStandaloneProcess`,
//!     `InitializeGUCOptions`, `process_postgres_switches`, `SelectConfigFiles`,
//!     `checkDataDir`/`ChangeToDataDir`/`CreateDataDirLockFile`,
//!     `InitializeMaxBackends`/`InitializeFastPathLocks`,
//!     `CreateSharedMemoryAndSemaphores`, `InitProcess`) then hands off to
//!     [`main_loop::PostgresMain`]. It seam-panics into the still-unported
//!     `LocalProcessControlFile`/`process_shared_preload_libraries`/
//!     `process_shmem_requests`/`InitializeShmemGUCs`/
//!     `InitializeWalConsistencyChecking`/`PgStartTime` callees.
//!
//! NOT in this crate:
//! `InteractiveBackend` (the single-user stdin reader); the extended-query
//! (`exec_parse_message`/`exec_bind_message`/`exec_execute_message`/describe)
//! pipeline (F2, plancache-gated); the F2-coupled logging helpers
//! `errdetail_execute`/`errdetail_params`. The `pg_parse_query`/`pg_plan_query`
//! seam decls keyed on opaque plancache handles (consumed by IMPORT FOREIGN
//! SCHEMA / SPI) stay seam-and-panic until the handle model retires.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

extern crate alloc;

pub mod globals;
pub mod guc;
pub mod interrupt;
pub mod logging;
pub mod main_loop;
pub mod simple_query;
pub mod single_user;

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
std::thread_local! {
    /// `int client_connection_check_interval = 0` (postgres.c:102) — backing
    /// store for the guc-table slot; PGC_USERSET, boot value 0 (disabled).
    static CLIENT_CONNECTION_CHECK_INTERVAL: core::cell::Cell<i32> =
        const { core::cell::Cell::new(0) };
}

pub fn init_seams() {
    // postgres.c owns the `client_connection_check_interval` GUC global
    // (read by the interrupt machinery). Install the guc-table slot accessors
    // over our backing cell so the GUC engine can read/write it.
    backend_utils_misc_guc_tables::vars::client_connection_check_interval.install(
        backend_utils_misc_guc_tables::GucVarAccessors {
            get: || CLIENT_CONNECTION_CHECK_INTERVAL.with(core::cell::Cell::get),
            set: |v| CLIENT_CONNECTION_CHECK_INTERVAL.with(|c| c.set(v)),
        },
    );

    // --- F3: interrupt / signal machinery ---
    s::check_for_interrupts::set(interrupt::check_for_interrupts);
    // pathnode.c (the optimizer) calls the same `CHECK_FOR_INTERRUPTS()` macro
    // through pathnode-seams' infallible `()` variant. The body is identical; a
    // genuine pending cancel surfaces (mirroring C's longjmp) rather than being
    // silently swallowed, so unwrap rather than drop the error.
    backend_optimizer_util_pathnode_seams::check_for_interrupts::set(|| {
        interrupt::check_for_interrupts().expect("CHECK_FOR_INTERRUPTS")
    });
    // matview.c's CHECK_FOR_INTERRUPTS() reaches the same body via its outward
    // frontier seam crate (fallible variant).
    backend_commands_matview_deps_seams::check_for_interrupts::set(
        interrupt::check_for_interrupts,
    );
    s::process_client_read_interrupt::set(interrupt::ProcessClientReadInterrupt);
    s::process_client_write_interrupt::set(interrupt::ProcessClientWriteInterrupt);
    s::handle_recovery_conflict_interrupt::set(interrupt::HandleRecoveryConflictInterrupt);
    s::die::set(interrupt::die);
    // Returns the `die` SIGTERM-handler fn-pointer so callers can install it via
    // `pqsignal(SIGTERM, ...)` (e.g. the logical-replication launcher).
    s::die_signal_handler::set(|| interrupt::die as fn(i32));
    s::statement_cancel_handler::set(interrupt::StatementCancelHandler);
    // Returns the SIGFPE `FloatExceptionHandler` fn-pointer so callers can
    // install it via `pqsignal(SIGFPE, ...)` (e.g. the slot-sync worker).
    s::float_exception_handler::set(|| interrupt::float_exception_handler_fn as fn(i32));

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

    // `debug_query_string = queryDesc->sourceText` on the parallel executor's
    // `execParallel-support` surface (execParallel.c `ParallelQueryMain`). C
    // assigns the long-lived worker `sourceText` pointer; the owned model carries
    // the text by value, so leak it for process lifetime to obtain the matching
    // `&'static str` (the worker's query text outlives the assignment, as C's
    // does).
    backend_executor_execParallel_support_seams::set_debug_query_string::set(|s| {
        globals::set_debug_query_string(Some(alloc::boxed::Box::leak(s.into_boxed_str())));
    });

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

    // `debug_parallel_query` (GUC, `int` enum) — the developer GUC read by the
    // parallel-worker error-relay (parallel.c) to decide whether to add the
    // "parallel worker" context line (skipped under DEBUG_PARALLEL_REGRESS for
    // test stability) and by the planner's parallel-Gather forcing. In C the
    // backing `int debug_parallel_query` lives in optimizer/plan/planner.c; in
    // this repo it is the `debug_parallel_query` GUC slot (a `GucEnumVar`, i.e.
    // `GucSlot<i32>`) in guc_tables. The accessor reads that slot directly.
    backend_access_transam_parallel_rt_seams::debug_parallel_query::set(|| {
        backend_utils_misc_guc_tables::vars::debug_parallel_query.read()
    });

    // --- F1: simple-Query analyze+rewrite entry (postgres.c owns
    // pg_analyze_and_rewrite_fixedparams; its consumer is copyto.c). COPY passes
    // no parameter types, matching the seam's no-param signature.
    backend_parser_analyze_seams::pg_analyze_and_rewrite_fixedparams::set(
        |mcx, parsetree, query_string| {
            simple_query::pg_analyze_and_rewrite_fixedparams(mcx, parsetree, query_string, &[])
        },
    );

    // --- STEP B-bis: plancache F0 value PRODUCERS. The param-threading value
    // form of pg_analyze_and_rewrite_fixedparams (RevalidateCachedQuery's
    // fixedparams branch passes plansource->param_types, not the empty array
    // COPY uses) and the value pg_plan_queries (BuildCachedPlan). Both reuse the
    // already-ported value bodies in simple_query.
    backend_parser_analyze_seams::pg_analyze_and_rewrite_fixedparams_params::set(
        |mcx, parsetree, query_string, param_types| {
            simple_query::pg_analyze_and_rewrite_fixedparams(
                mcx,
                parsetree,
                query_string,
                param_types,
            )
        },
    );
    // PREPARE's analyze+rewrite-with-varparams entry. postgres.c owns
    // pg_analyze_and_rewrite_varparams (it wraps analyze.c's parse_analyze_var
    // params + the parameter-completeness check + pg_rewrite_query). The PREPARE
    // driver (prepare.c) consumes it through this seam.
    backend_parser_analyze_seams::analyze_and_rewrite_varparams::set(
        simple_query::pg_analyze_and_rewrite_varparams,
    );

    s::pg_plan_queries_value::set(
        |mcx, querytrees, query_string, cursor_options, bound_params| {
            // The value body owns its querytree list (C scribbles on / copies
            // them); clone the borrowed slice into `mcx`. `boundParams` is NULL
            // (None) on the generic-plan / simple-Query path; a Some
            // (custom-plan parameter substitution) is not yet threaded through
            // the value planning stack — mirror PG and panic precisely rather
            // than silently dropping it.
            if bound_params.is_some() {
                panic!(
                    "pg_plan_queries_value: custom-plan boundParams are not yet \
                     threaded through the value planning stack (standard_planner \
                     drops boundParams); only reached for parameterized custom \
                     plans, not the generic-plan / simple-Query path"
                );
            }
            let mut owned: mcx::PgVec<'_, _> = mcx::PgVec::new_in(mcx);
            for q in querytrees.iter() {
                owned.push(q.clone_in(mcx)?);
            }
            simple_query::pg_plan_queries(mcx, owned, query_string, cursor_options)
        },
    );

    // Reference `quickdie_handler` so the `pqsigfunc`-shaped wrapper is kept and
    // available for the postmaster's SIGQUIT install (done by F0a when it lands).
    let _: fn(i32) = quickdie_handler;

    // --- F0a: the backend main loop (PostgresMain). Retires the latent panic
    // the `postgres_main` seam carried; backend-startup's BackendMain hands off
    // here after BackendInitialize + InitProcess.
    s::postgres_main::set(main_loop::PostgresMain);

    // --- F0b: the single-user standalone backend entry
    // (PostgresSingleUserMain). main()'s DISPATCH_SINGLE arm hands off here for
    // `--single`. The driver runs the standalone bootstrap (config files,
    // data-dir lock, shmem create/init, InitProcess) and then PostgresMain. It
    // seam-panics into the still-unported control-file/preload/shmem-request/
    // runtime-GUC/PgStartTime callees (boot gaps #4/#5/#6).
    s::postgres_single_user_main::set(single_user::PostgresSingleUserMain);
}
