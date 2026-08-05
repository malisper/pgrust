// postgres.c — the backend command processor (PG 18.3): exec_simple_query,
// the extended-query protocol (exec_parse/bind/execute/describe_message), and
// PostgresMain's message loop. Fastpath ('F') panics loudly (tcop/fastpath.c
// is a later unit).
#![allow(non_snake_case)]

use core::cell::Cell;

use ::elog::ereport;
use ::types_error::{ErrorLocation, PgResult, ERRCODE_QUERY_CANCELED, ERROR, FATAL};
use ::types_storage::storage::ProcSignalReason;

pub mod extended_query;
pub mod main_loop;
pub mod simple_query;
pub mod single_user;
pub mod stdio_wire;
#[cfg(pgrust_sim)]
mod sim_net;
#[cfg(pgrust_sim)]
mod sim_sched_demo;
pub mod stmt_trace;
pub mod switches;
#[cfg(test)]
mod session_tests;
#[cfg(test)]
mod tests;

pub use extended_query::{
    drop_unnamed_stmt, exec_bind_message, exec_describe_portal_message,
    exec_describe_statement_message, exec_execute_message, exec_parse_message,
    pg_analyze_and_rewrite_varparams,
};
pub use main_loop::PostgresMain;
pub use single_user::PostgresSingleUserMain;
pub use stdio_wire::PostgresStdioWireMain;
#[cfg(pgrust_sim)]
pub use sim_net::PostgresSimNetMain;
pub use simple_query::{
    exec_simple_query, finish_xact_command, pg_analyze_and_rewrite_fixedparams, pg_parse_query,
    pg_plan_queries, pg_plan_query, pg_rewrite_query, start_xact_command,
};

pub fn init_seams() {
    postgres_seams::postgres_main::set(postgres_main_seam);
    postgres_seams::postgres_single_user_main::set(PostgresSingleUserMain);
    postgres_seams::postgres_stdio_wire_main::set(PostgresStdioWireMain);
    #[cfg(pgrust_sim)]
    postgres_seams::postgres_sim_net_main::set(PostgresSimNetMain);
    postgres_seams::check_for_interrupts::set(check_for_interrupts);
    postgres_seams::die::set(die);
    postgres_seams::statement_cancel_handler::set(StatementCancelHandler);
    postgres_seams::quickdie::set(quickdie);
    postgres_seams::float_exception_handler::set(FloatExceptionHandler);
    postgres_seams::handle_recovery_conflict_interrupt::set(HandleRecoveryConflictInterrupt);
    postgres_seams::reset_usage::set(ResetUsage);
    postgres_seams::show_usage::set(ShowUsage);
    postgres_seams::process_client_read_interrupt::set(ProcessClientReadInterrupt);
    postgres_seams::process_client_write_interrupt::set(ProcessClientWriteInterrupt);
    postgres_seams::set_debug_options::set(set_debug_options);
    postgres_seams::set_plan_disabling_options::set(set_plan_disabling_options);
    postgres_seams::get_stats_option_name::set(get_stats_option_name);
    postgres_seams::process_postgres_switches::set(switches::process_postgres_switches);
    guc_tables::hooks::check_restrict_nonsystem_relation_kind
        .install(check_restrict_nonsystem_relation_kind);
    guc_tables::hooks::assign_restrict_nonsystem_relation_kind
        .install(assign_restrict_nonsystem_relation_kind);
    guc_tables::vars::restrict_nonsystem_relation_kind_string.install(
        guc_tables::GucVarAccessors {
            get: restrict_nonsystem_relation_kind_string_get,
            set: restrict_nonsystem_relation_kind_string_set,
        },
    );
}

guc_tables::session_guc_string!(
    RESTRICT_NONSYSTEM_RELATION_KIND_STRING,
    restrict_nonsystem_relation_kind_string_get,
    restrict_nonsystem_relation_kind_string_set,
    Some("")
);

// GUC check_hook for restrict_nonsystem_relation_kind (postgres.c); the
// derived flag word lives in guc_tables::backing for the rewriter.
fn check_restrict_nonsystem_relation_kind(
    newval: &mut Option<String>,
    extra: &mut Option<guc_tables::GucHookExtra>,
    _source: types_guc::GucSource,
) -> PgResult<bool> {
    let value = newval.clone().unwrap_or_default();
    let ctx = mcx::MemoryContext::new("check_restrict_nonsystem_relation_kind");
    let Some(elemlist) = varlena::split_identifier_string(
        ctx.mcx(),
        &value,
        b',',
        mbutils::GetDatabaseEncoding(),
    )?
    else {
        guc::GUC_check_errdetail("List syntax is invalid.");
        return Ok(false);
    };
    let mut flags: i32 = 0;
    for tok in &elemlist {
        if tok.eq_ignore_ascii_case("view") {
            flags |= guc_tables::consts::RESTRICT_RELKIND_VIEW;
        } else if tok.eq_ignore_ascii_case("foreign-table") {
            flags |= guc_tables::consts::RESTRICT_RELKIND_FOREIGN_TABLE;
        } else {
            guc::GUC_check_errdetail(format!("Unrecognized key word: \"{tok}\"."));
            return Ok(false);
        }
    }
    *extra = Some(Box::new(flags));
    Ok(true)
}

fn assign_restrict_nonsystem_relation_kind(
    _newval: Option<&str>,
    extra: Option<&guc_tables::GucHookExtra>,
) {
    let flags = extra.and_then(|e| e.downcast_ref::<i32>()).copied().unwrap_or(0);
    guc_tables::backing::set_restrict_nonsystem_relation_kind(flags);
}

fn postgres_main_seam(dbname: &str, username: &str) -> ! {
    PostgresMain(dbname, username)
}

thread_local! {
    static XACT_STARTED: Cell<bool> = const { Cell::new(false) };
    static DOING_EXTENDED_QUERY_MESSAGE: Cell<bool> = const { Cell::new(false) };
    static IGNORE_TILL_SYNC: Cell<bool> = const { Cell::new(false) };
    static DOING_COMMAND_READ: Cell<bool> = const { Cell::new(false) };
    // EchoQuery / UseSemiNewlineNewline (postgres.c:154-155): the single-user
    // -E and -j switches.
    static ECHO_QUERY: Cell<bool> = const { Cell::new(false) };
    static USE_SEMI_NEWLINE_NEWLINE: Cell<bool> = const { Cell::new(false) };
}

pub(crate) fn echo_query() -> bool {
    ECHO_QUERY.with(Cell::get)
}
pub(crate) fn set_echo_query(v: bool) {
    ECHO_QUERY.with(|c| c.set(v));
}
pub(crate) fn use_semi_newline_newline() -> bool {
    USE_SEMI_NEWLINE_NEWLINE.with(Cell::get)
}
pub(crate) fn set_use_semi_newline_newline(v: bool) {
    USE_SEMI_NEWLINE_NEWLINE.with(|c| c.set(v));
}

pub(crate) fn xact_started() -> bool {
    XACT_STARTED.with(Cell::get)
}
pub(crate) fn set_xact_started(v: bool) {
    XACT_STARTED.with(|c| c.set(v));
}
pub(crate) fn doing_extended_query_message() -> bool {
    DOING_EXTENDED_QUERY_MESSAGE.with(Cell::get)
}
pub(crate) fn set_doing_extended_query_message(v: bool) {
    DOING_EXTENDED_QUERY_MESSAGE.with(|c| c.set(v));
}
pub(crate) fn ignore_till_sync() -> bool {
    IGNORE_TILL_SYNC.with(Cell::get)
}
pub(crate) fn set_ignore_till_sync(v: bool) {
    IGNORE_TILL_SYNC.with(|c| c.set(v));
}
/// client_connection_check_interval (ms), 0 when the owning transport never
/// installed the backing var (sim-net/wasm) — the check is then inert.
pub(crate) fn client_connection_check_interval_ms() -> i32 {
    if guc_tables::vars::client_connection_check_interval.installed() {
        guc_tables::vars::client_connection_check_interval.read()
    } else {
        0
    }
}

pub fn DoingCommandRead() -> bool {
    DOING_COMMAND_READ.with(Cell::get)
}
pub(crate) fn set_doing_command_read(v: bool) {
    DOING_COMMAND_READ.with(|c| c.set(v));
}

pub(crate) fn loc(line: i32, func: &'static str) -> ErrorLocation {
    ErrorLocation::new("postgres.c", line, func)
}

/// A query string that survived `pg_client_to_server` but is not valid UTF-8:
/// the bytes are in the DATABASE encoding (C carries them fine), but the
/// engine processes SQL text as `&str`, so a non-UTF8 database encoding
/// cannot be honored for non-ASCII query text. Fail loudly and honestly
/// (0A000 naming the database encoding) instead of the old misleading
/// XX000 "invalid byte sequence in query string".
#[cold]
#[inline(never)]
pub(crate) fn non_utf8_query_error() -> Box<::types_error::PgError> {
    Box::new(
        ::types_error::PgError::new(
            ERROR,
            format!(
                "query strings with non-ASCII characters are not supported yet in databases \
                 with encoding \"{}\"",
                mbutils::GetDatabaseEncodingName()
            ),
        )
        .with_sqlstate(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
        .with_hint("Use a database with encoding \"UTF8\"."),
    )
}

pub(crate) fn get_current_timestamp() -> types_core::TimestampTz {
    // DST P2 (contract §1.2, census dedupe (c)): the private SystemTime
    // duplicate deleted; the seam is the one GetCurrentTimestamp path.
    timestamp_seams::get_current_timestamp::call()
}


// Per-tuple hot: TLS pointer + Relaxed load + one predictable branch (C's
// CHECK_FOR_INTERRUPTS; the shared flag is how async senders reach us).
// GL-STMTTASK-2 quantum-yield tick: one MORE thread-local bool load +
// predictable branch (armed only inside statement-task spans under the
// DEFAULT-OFF PGRUST_STMT_TASK_YIELD knob; the governor is the executor's
// registered hook).
#[inline(always)]
pub fn check_for_interrupts() -> PgResult<()> {
    if init_small::globals::InterruptPending() {
        return ProcessInterrupts();
    }
    postgres_seams::stmt_yield::tick();
    Ok(())
}

thread_local! {
    // C's RecoveryConflictPending(Reasons) statics as one ProcSignalReason bitmask.
    static RECOVERY_CONFLICT_PENDING_REASONS: Cell<u32> = const { Cell::new(0) };
}

pub fn HandleRecoveryConflictInterrupt(reason: u32) {
    RECOVERY_CONFLICT_PENDING_REASONS.with(|c| c.set(c.get() | (1 << reason)));
    init_small::globals::SetInterruptPending(true);
}

// errdetail_recovery_conflict (postgres.c:2553).
fn errdetail_recovery_conflict(reason: ProcSignalReason) -> &'static str {
    use ProcSignalReason::*;
    match reason {
        PROCSIG_RECOVERY_CONFLICT_BUFFERPIN => "User was holding shared buffer pin for too long.",
        PROCSIG_RECOVERY_CONFLICT_LOCK => "User was holding a relation lock for too long.",
        PROCSIG_RECOVERY_CONFLICT_TABLESPACE => {
            "User was or might have been using tablespace that must be dropped."
        }
        PROCSIG_RECOVERY_CONFLICT_SNAPSHOT => {
            "User query might have needed to see row versions that must be removed."
        }
        PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT => {
            "User was using a logical replication slot that must be invalidated."
        }
        PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK => {
            "User transaction caused buffer deadlock with recovery."
        }
        PROCSIG_RECOVERY_CONFLICT_DATABASE => {
            "User was connected to a database that must be dropped."
        }
        _ => "",
    }
}

// ProcessRecoveryConflictInterrupt (postgres.c:3101) — one conflict reason.
// C's switch-with-fallthroughs rendered as sequential gates.
fn ProcessRecoveryConflictInterrupt(reason: ProcSignalReason) -> PgResult<()> {
    use init_small::globals as g;
    use ProcSignalReason::*;

    // STARTUP_DEADLOCK: if we aren't waiting for a lock we can never deadlock.
    if reason == PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK
        && lock::GetAwaitedLockHashcode().is_none()
    {
        return Ok(());
    }

    if matches!(
        reason,
        PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK | PROCSIG_RECOVERY_CONFLICT_BUFFERPIN
    ) {
        // BUFFERPIN: nothing to do unless we block the Startup process.
        // STARTUP_DEADLOCK: if the startup process is not waiting for a
        // buffer pin (i.e. also waiting for locks), have ProcSleep check
        // for deadlocks.
        if !bufmgr::HoldingBufferPinThatDelaysRecovery() {
            if reason == PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK
                && lmgr_proc::GetStartupBufferPinWaitBufId() < 0
            {
                lmgr_proc::CheckDeadLockAlert();
            }
            return Ok(());
        }
        if let Some(procno) = lmgr_proc::MyProc() {
            lmgr_proc::GetPGProcByNumber(procno)
                .recoveryConflictPending
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        // Fall through to error handling.
    }

    if matches!(
        reason,
        PROCSIG_RECOVERY_CONFLICT_LOCK
            | PROCSIG_RECOVERY_CONFLICT_TABLESPACE
            | PROCSIG_RECOVERY_CONFLICT_SNAPSHOT
    ) && !xact::IsTransactionOrTransactionBlock()
    {
        // No longer in a transaction: ignore.
        return Ok(());
    }

    if reason != PROCSIG_RECOVERY_CONFLICT_DATABASE
        && (reason == PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT || !xact::IsSubTransaction())
    {
        // Not in a subtransaction (or the always-ERROR logical-slot case):
        // an ERROR can resolve the conflict.
        if xact::IsAbortedTransactionBlockState() {
            // Already aborted: no cancel needed. (Aborted subtransactions
            // must still go FATAL, hence the check placement.)
            return Ok(());
        }

        // Idle-in-transaction sessions (DoingCommandRead) drop through to
        // FATAL to dislodge them.
        if !DoingCommandRead() {
            if g::QueryCancelHoldoffCount() != 0 {
                // Mid-message read: re-arm and defer (FE/BE sync), as in
                // ProcessInterrupts' QueryCancelPending arm.
                RECOVERY_CONFLICT_PENDING_REASONS.with(|c| c.set(c.get() | (1 << reason as u32)));
                g::SetInterruptPending(true);
                return Ok(());
            }

            lmgr_proc::LockErrorCleanup()?;
            pgstat::database::pgstat_report_recovery_conflict(reason);
            return Err(ereport(ERROR)
                .errcode(types_error::ERRCODE_T_R_SERIALIZATION_FAILURE)
                .errmsg("canceling statement due to conflict with recovery")
                .errdetail(errdetail_recovery_conflict(reason))
                .into_error()
                .with_error_location(loc(3222, "ProcessRecoveryConflictInterrupt"))
                .into());
        }
    }

    // Retry impossible (database dropped) or ERROR could not resolve it:
    // terminate the session.
    pgstat::database::pgstat_report_recovery_conflict(reason);
    Err(ereport(FATAL)
        .errcode(if reason == PROCSIG_RECOVERY_CONFLICT_DATABASE {
            types_error::ERRCODE_DATABASE_DROPPED
        } else {
            types_error::ERRCODE_T_R_SERIALIZATION_FAILURE
        })
        .errmsg("terminating connection due to conflict with recovery")
        .errdetail(errdetail_recovery_conflict(reason))
        .errhint(
            "In a moment you should be able to reconnect to the database and repeat \
             your command.",
        )
        .into_error()
        .with_error_location(loc(3244, "ProcessRecoveryConflictInterrupt"))
        .into())
}

// ProcessRecoveryConflictInterrupts (postgres.c:3259).
fn ProcessRecoveryConflictInterrupts() -> PgResult<()> {
    debug_assert!(!elog::config::proc_exit_inprogress());
    debug_assert_eq!(init_small::globals::InterruptHoldoffCount(), 0);

    let first = types_storage::storage::PROCSIG_RECOVERY_CONFLICT_FIRST as u32;
    let last = types_storage::storage::PROCSIG_RECOVERY_CONFLICT_LAST as u32;
    for r in first..=last {
        let bit = 1u32 << r;
        if RECOVERY_CONFLICT_PENDING_REASONS.with(Cell::get) & bit != 0 {
            RECOVERY_CONFLICT_PENDING_REASONS.with(|c| c.set(c.get() & !bit));
            // SAFETY: r is within the ProcSignalReason repr range.
            let reason: ProcSignalReason = unsafe { std::mem::transmute(r) };
            ProcessRecoveryConflictInterrupt(reason)?;
        }
    }
    Ok(())
}

#[cold]
#[inline(never)]
pub fn ProcessInterrupts() -> PgResult<()> {
    use init_small::globals as g;

    // C's SIGALRM/kill handlers already ran asynchronously by this point;
    // the thread model runs them here: senders raised InterruptPending, the
    // drains fire the pended timeout handlers and signal dispositions on
    // this thread before the flags below are inspected.
    if timeout_seams::process_timeout_interrupt::is_installed() {
        timeout_seams::process_timeout_interrupt::call();
    }
    procsignal::DrainThreadSignals()?;

    if g::InterruptHoldoffCount() != 0 || g::CritSectionCount() != 0 {
        return Ok(());
    }
    g::SetInterruptPending(false);

    if g::ProcDiePending() {
        g::SetProcDiePending(false);
        g::SetQueryCancelPending(false); /* ProcDie trumps QueryCancel */
        lmgr_proc::LockErrorCleanup()?;
        if elog::config::client_auth_in_progress() {
            if elog::config::where_to_send_output() == types_dest::CommandDest::Remote {
                elog::config::set_where_to_send_output(types_dest::CommandDest::None);
            }
            return Err(ereport(FATAL)
                .errcode(ERRCODE_QUERY_CANCELED)
                .errmsg("canceling authentication due to timeout")
                .into_error()
                .with_error_location(loc(3316, "ProcessInterrupts"))
                .into());
        }
        if miscinit::GetMyBackendType() == types_core::BackendType::WalReceiver {
            return Err(ereport(FATAL)
                .errcode(types_error::ERRCODE_ADMIN_SHUTDOWN)
                .errmsg("terminating walreceiver process due to administrator command")
                .into_error()
                .with_error_location(loc(3339, "ProcessInterrupts"))
                .into());
        }
        // C's other worker-process arms are unreachable: those mains panic at launch.
        return Err(ereport(FATAL)
            .errcode(types_error::ERRCODE_ADMIN_SHUTDOWN)
            .errmsg("terminating connection due to administrator command")
            .into_error()
            .with_error_location(loc(3355, "ProcessInterrupts"))
            .into());
    }

    if g::CheckClientConnectionPending() {
        g::SetCheckClientConnectionPending(false);
        // C: check for a lost connection and re-arm, if still configured,
        // but not once back at DoingCommandRead — idle sessions already
        // detect lost connections at the read. A vacant seam (sim-net/wasm
        // transports) or a reset interval leaves the connection presumed
        // alive and the timeout unarmed.
        let interval = client_connection_check_interval_ms();
        if !DoingCommandRead()
            && interval > 0
            && pqcomm_seams::pq_check_connection::is_installed()
        {
            if !pqcomm_seams::pq_check_connection::call()? {
                g::SetClientConnectionLost(true);
            } else {
                timeout_seams::enable_timeout_after::call(
                    timeout_seams::CLIENT_CONNECTION_CHECK_TIMEOUT,
                    interval,
                )?;
            }
        }
    }
    if g::ClientConnectionLost() {
        g::SetQueryCancelPending(false); /* lost connection trumps QueryCancel */
        lmgr_proc::LockErrorCleanup()?;
        /* don't send to client, we already know the connection to be dead. */
        elog::config::set_where_to_send_output(types_dest::CommandDest::None);
        return Err(ereport(FATAL)
            .errcode(types_error::ERRCODE_CONNECTION_FAILURE)
            .errmsg("connection to client lost")
            .into_error()
            .with_error_location(loc(3386, "ProcessInterrupts"))
            .into());
    }

    if g::QueryCancelPending() && g::QueryCancelHoldoffCount() != 0 {
        // Cancel mustn't fire mid-message-read (FE/BE sync); re-arm for after.
        g::SetInterruptPending(true);
    } else if g::QueryCancelPending() {
        g::SetQueryCancelPending(false);

        // Uninstalled timeout seams are exact here, not a stub: timeout.c is
        // the only writer of these indicators, so absent it they are false.
        let (mut lock_timeout_occurred, stmt_timeout_occurred) =
            if timeout_seams::get_timeout_indicator::is_installed() {
                (
                    timeout_seams::get_timeout_indicator::call(timeout_seams::LOCK_TIMEOUT, true),
                    timeout_seams::get_timeout_indicator::call(
                        timeout_seams::STATEMENT_TIMEOUT,
                        true,
                    ),
                )
            } else {
                (false, false)
            };

        /* both set: report whichever timeout completed earlier; tie = lock */
        if lock_timeout_occurred
            && stmt_timeout_occurred
            && timeout_seams::get_timeout_finish_time::call(timeout_seams::STATEMENT_TIMEOUT)
                < timeout_seams::get_timeout_finish_time::call(timeout_seams::LOCK_TIMEOUT)
        {
            lock_timeout_occurred = false;
        }

        if lock_timeout_occurred {
            lmgr_proc::LockErrorCleanup()?;
            return Err(ereport(ERROR)
                .errcode(types_error::ERRCODE_LOCK_NOT_AVAILABLE)
                .errmsg("canceling statement due to lock timeout")
                .into_error()
                .with_error_location(loc(3438, "ProcessInterrupts"))
                .into());
        }
        if stmt_timeout_occurred {
            lmgr_proc::LockErrorCleanup()?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_QUERY_CANCELED)
                .errmsg("canceling statement due to statement timeout")
                .into_error()
                .with_error_location(loc(3445, "ProcessInterrupts"))
                .into());
        }
        if !DoingCommandRead() {
            lmgr_proc::LockErrorCleanup()?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_QUERY_CANCELED)
                .errmsg("canceling statement due to user request")
                .into_error()
                .with_error_location(loc(3465, "ProcessInterrupts"))
                .into());
        }
    }

    if RECOVERY_CONFLICT_PENDING_REASONS.with(Cell::get) != 0 {
        ProcessRecoveryConflictInterrupts()?;
    }

    if g::IdleInTransactionSessionTimeoutPending() {
        // A GUC reset to zero between firing and here means ignore the signal
        // (the update itself doesn't disable a pending interrupt).
        g::SetIdleInTransactionSessionTimeoutPending(false);
        if lmgr_proc::globals::IdleInTransactionSessionTimeout() > 0 {
            return Err(ereport(FATAL)
                .errcode(types_error::ERRCODE_IDLE_IN_TRANSACTION_SESSION_TIMEOUT)
                .errmsg("terminating connection due to idle-in-transaction timeout")
                .into_error()
                .with_error_location(loc(3486, "ProcessInterrupts"))
                .into());
        }
    }
    if g::TransactionTimeoutPending() {
        g::SetTransactionTimeoutPending(false);
        if lmgr_proc::globals::TransactionTimeout() > 0 {
            return Err(ereport(FATAL)
                .errcode(types_error::ERRCODE_TRANSACTION_TIMEOUT)
                .errmsg("terminating connection due to transaction timeout")
                .into_error()
                .with_error_location(loc(3499, "ProcessInterrupts"))
                .into());
        }
    }
    if g::IdleSessionTimeoutPending() {
        g::SetIdleSessionTimeoutPending(false);
        if lmgr_proc::globals::IdleSessionTimeout() > 0 {
            return Err(ereport(FATAL)
                .errcode(types_error::ERRCODE_IDLE_SESSION_TIMEOUT)
                .errmsg("terminating connection due to idle-session timeout")
                .into_error()
                .with_error_location(loc(3512, "ProcessInterrupts"))
                .into());
        }
    }

    if g::IdleStatsUpdateTimeoutPending()
        && DoingCommandRead()
        && !xact::IsTransactionOrTransactionBlock()
    {
        g::SetIdleStatsUpdateTimeoutPending(false);
        pgstat::pending::pgstat_report_stat(true);
    }

    if g::ProcSignalBarrierPending() {
        procsignal_seams::process_proc_signal_barrier::call()?;
    }

    if g::ParallelMessagePending() {
        parallel_seams::process_parallel_messages::call()?;
    }

    if g::LogMemoryContextPending() {
        mcxt_seams::process_log_memory_context_interrupt::call()?;
    }
    // ParallelApplyMessagePending flag has no storage yet (logical-apply
    // owner unported).

    // Serial-lease v2 safe-point admission (GL-SLEASE-2; pgrust extension):
    // a sweeper-flagged floor crossing acquires its execution permit HERE —
    // the canonical safe point (past the holdoff/crit-section gates, never
    // on the error-raising arms above, which unwind out of the run anyway).
    // Installed only when the lease is armed; a bounded wait inside.
    postgres_seams::tap_serial_lease_admission::call_if(|f| f());

    Ok(())
}

// wasm32: the wasi libc crate exposes no SIG* names; these are the
// thread-signal emulation's Linux-numbered space (procsignal wasm arm).
#[cfg(not(target_family = "wasm"))]
pub(crate) use libc::{
    SIGCHLD, SIGFPE, SIGHUP, SIGINT, SIGKILL, SIGPIPE, SIGQUIT, SIGTERM, SIGUSR1, SIGUSR2,
};
#[cfg(target_family = "wasm")]
mod wasm_signums {
    pub const SIGHUP: i32 = 1;
    pub const SIGINT: i32 = 2;
    pub const SIGQUIT: i32 = 3;
    pub const SIGFPE: i32 = 8;
    pub const SIGKILL: i32 = 9;
    pub const SIGUSR1: i32 = 10;
    pub const SIGUSR2: i32 = 12;
    pub const SIGPIPE: i32 = 13;
    pub const SIGTERM: i32 = 15;
    pub const SIGCHLD: i32 = 17;
}
#[cfg(target_family = "wasm")]
pub(crate) use wasm_signums::*;

// The C pqsignal block at PostgresMain entry (postgres.c:4217-4251), rendered
// as pqsignal_thread dispositions drained at this thread's latch/client-IO
// wakes. am_walsender arm = WalSndSignals' one delta from the regular backend
// set: SIGUSR2 runs WalSndLastCycleHandler (drain WAL up to the shutdown
// checkpoint, then exit) instead of Ignore.
pub fn install_thread_signal_handlers() {
    use procsignal::ThreadSignalHandler::{Fallible, Ignore, Simple};
    procsignal::pqsignal_thread(SIGHUP, Simple(interrupt::SignalHandlerForConfigReload));
    procsignal::pqsignal_thread(SIGINT, Simple(StatementCancelHandler));
    procsignal::pqsignal_thread(SIGTERM, Fallible(die));
    if init_small::globals::IsUnderPostmaster() {
        procsignal::pqsignal_thread(SIGQUIT, Simple(quickdie_handler));
        // No C analog (SIGKILL has no handler): the crash-test injection's
        // kill-9 rendering, reachable only via procsignal::SendThreadKill.
        procsignal::pqsignal_thread(SIGKILL, Simple(kill9_handler));
    } else {
        procsignal::pqsignal_thread(SIGQUIT, Fallible(die));
    }
    procsignal::pqsignal_thread(SIGPIPE, Ignore);
    procsignal::pqsignal_thread(SIGUSR1, Simple(procsignal::procsignal_sigusr1_handler));
    if walsender_seams::am_walsender()
        && walsender_seams::wal_snd_last_cycle_handler::is_installed()
    {
        procsignal::pqsignal_thread(
            SIGUSR2,
            Simple(|| walsender_seams::wal_snd_last_cycle_handler::call()),
        );
    } else {
        procsignal::pqsignal_thread(SIGUSR2, Ignore);
    }
    procsignal::pqsignal_thread(SIGFPE, Fallible(FloatExceptionHandler));
    procsignal::pqsignal_thread(SIGCHLD, Ignore);
}

fn quickdie_handler() {
    quickdie()
}

// SIGKILL semantics: no handler runs in C, so no client message and no exit
// callbacks — the connection just closes and the postmaster reaps
// "terminated by signal 9".
fn kill9_handler() {
    init_small::globals::HoldInterrupts();
    if elog::config::where_to_send_output() == types_dest::CommandDest::Remote {
        elog::config::set_where_to_send_output(types_dest::CommandDest::None);
    }
    elog::clear_emit_context_callbacks();
    ipc::exit_thread_killed(SIGKILL)
}

pub fn die() -> PgResult<()> {
    use init_small::globals as g;
    if !elog::config::proc_exit_inprogress() {
        g::SetInterruptPending(true);
        g::SetProcDiePending(true);
    }

    pgstat::database::pgstat_set_session_end_cause(
        pgstat::database::SessionEndType::DisconnectKilled,
    );

    latch::SetLatch(g::MyLatch().expect("die: MyLatch is not set"));

    // Single-user mode quits immediately (latches can't cover file stdin).
    if DoingCommandRead() && elog::config::where_to_send_output() != types_dest::CommandDest::Remote
    {
        ProcessInterrupts()?;
    }
    Ok(())
}

pub fn StatementCancelHandler() {
    use init_small::globals as g;
    if !elog::config::proc_exit_inprogress() {
        g::SetInterruptPending(true);
        g::SetQueryCancelPending(true);
    }
    latch::SetLatch(g::MyLatch().expect("StatementCancelHandler: MyLatch is not set"));
}

pub fn quickdie() -> ! {
    // C also blocks signals here; no per-thread signal rendering exists.
    init_small::globals::HoldInterrupts();

    if elog::config::client_auth_in_progress()
        && elog::config::where_to_send_output() == types_dest::CommandDest::Remote
    {
        elog::config::set_where_to_send_output(types_dest::CommandDest::None);
    }

    elog::clear_emit_context_callbacks();

    use pmsignal::QuitSignalReason::*;
    let _ = match pmsignal::GetQuitSignalReason() {
        PMQUIT_NOT_SENT => ereport(types_error::WARNING)
            .errcode(types_error::ERRCODE_ADMIN_SHUTDOWN)
            .errmsg("terminating connection because of unexpected SIGQUIT signal")
            .finish(loc(2983, "quickdie")),
        PMQUIT_FOR_CRASH => ereport(types_error::WARNING_CLIENT_ONLY)
            .errcode(types_error::ERRCODE_CRASH_SHUTDOWN)
            .errmsg("terminating connection because of crash of another server process")
            .errdetail(
                "The postmaster has commanded this server process to roll back the \
                 current transaction and exit, because another server process exited \
                 abnormally and possibly corrupted shared memory.",
            )
            .errhint(
                "In a moment you should be able to reconnect to the database and \
                 repeat your command.",
            )
            .finish(loc(2989, "quickdie")),
        PMQUIT_FOR_STOP => ereport(types_error::WARNING_CLIENT_ONLY)
            .errcode(types_error::ERRCODE_ADMIN_SHUTDOWN)
            .errmsg("terminating connection due to immediate shutdown command")
            .finish(loc(3000, "quickdie")),
    };

    // C's _exit(2), thread rendering: exit code 2 without exit callbacks; the
    // postmaster's crash/immediate-shutdown cycle reaps it (WIFEXITED(2)).
    ipc::exit_thread_raw(2)
}

pub fn FloatExceptionHandler() -> PgResult<()> {
    Err(ereport(ERROR)
        .errcode(types_error::ERRCODE_FLOATING_POINT_EXCEPTION)
        .errmsg("floating-point exception")
        .errdetail(
            "An invalid floating-point operation was signaled. This probably means \
             an out-of-range result or an invalid operation, such as division by zero.",
        )
        .into_error()
        .into())
}

pub fn ProcessClientReadInterrupt(blocked: bool) -> PgResult<()> {
    use init_small::globals as g;
    // C's SIGALRM interrupts a blocked client read directly; the thread
    // model's timer wake only sets the latch, and before this drain the
    // STARTUP_PACKET_TIMEOUT could never fire against a backend parked in
    // its startup-packet read — authentication_timeout was a no-op there
    // (a half-open connection held its backend forever).
    if timeout_seams::process_timeout_interrupt::is_installed() {
        timeout_seams::process_timeout_interrupt::call();
    }
    procsignal::DrainThreadSignals()?;
    if DoingCommandRead() {
        check_for_interrupts()?;

        if sinval::catchupInterruptPending() {
            sinval::ProcessCatchupInterrupt()?;
        }
        if commands_async::notifyInterruptPending() {
            commands_async::ProcessNotifyInterrupt(true)?;
        }
    } else if g::ProcDiePending() {
        if blocked {
            check_for_interrupts()?;
        } else {
            latch::SetLatch(g::MyLatch().expect("ProcessClientReadInterrupt: MyLatch is not set"));
        }
    }
    Ok(())
}

pub fn ProcessClientWriteInterrupt(blocked: bool) -> PgResult<()> {
    use init_small::globals as g;
    // Same SIGALRM rendering as ProcessClientReadInterrupt above.
    if timeout_seams::process_timeout_interrupt::is_installed() {
        timeout_seams::process_timeout_interrupt::call();
    }
    procsignal::DrainThreadSignals()?;
    if g::ProcDiePending() {
        if blocked {
            if g::InterruptHoldoffCount() == 0 && g::CritSectionCount() == 0 {
                // No error to client: it could block, and a partial protocol
                // message may already be out.
                if elog::config::where_to_send_output() == types_dest::CommandDest::Remote {
                    elog::config::set_where_to_send_output(types_dest::CommandDest::None);
                }
                check_for_interrupts()?;
            }
        } else {
            latch::SetLatch(g::MyLatch().expect("ProcessClientWriteInterrupt: MyLatch is not set"));
        }
    }
    Ok(())
}


thread_local! {
    static SAVE_RUSAGE: Cell<Option<(libc::rusage, libc::timeval)>> = const { Cell::new(None) };
}

#[cfg(not(target_family = "wasm"))]
fn getrusage_self() -> libc::rusage {
    // SAFETY: plain libc call filling a zeroed out-struct.
    unsafe {
        let mut r: libc::rusage = core::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut r);
        r
    }
}

// wasm32: WASI has no getrusage (wasi-libc defines no symbol; calling would
// fail at link) — zeroed snapshot, the pg_rusage wasm arm's shape.
#[cfg(target_family = "wasm")]
fn getrusage_self() -> libc::rusage {
    // SAFETY: rusage is plain data; a zeroed struct is the documented
    // "no counters on this platform" value.
    unsafe { core::mem::zeroed() }
}

fn gettimeofday_now() -> libc::timeval {
    // SAFETY: plain libc call filling a zeroed out-struct.
    unsafe {
        let mut t: libc::timeval = core::mem::zeroed();
        libc::gettimeofday(&mut t, core::ptr::null_mut());
        t
    }
}

pub fn ResetUsage() {
    SAVE_RUSAGE.with(|s| s.set(Some((getrusage_self(), gettimeofday_now()))));
}

pub fn ShowUsage(title: &str) -> PgResult<()> {
    let (save_r, save_t) = SAVE_RUSAGE
        .with(Cell::get)
        .unwrap_or_else(|| (getrusage_self(), gettimeofday_now()));
    let r = getrusage_self();
    let mut elapse = gettimeofday_now();

    let user = r.ru_utime;
    let sys = r.ru_stime;
    let mut ru = r;
    if elapse.tv_usec < save_t.tv_usec {
        elapse.tv_sec -= 1;
        elapse.tv_usec += 1_000_000;
    }
    if ru.ru_utime.tv_usec < save_r.ru_utime.tv_usec {
        ru.ru_utime.tv_sec -= 1;
        ru.ru_utime.tv_usec += 1_000_000;
    }
    if ru.ru_stime.tv_usec < save_r.ru_stime.tv_usec {
        ru.ru_stime.tv_sec -= 1;
        ru.ru_stime.tv_usec += 1_000_000;
    }

    let mut str_ = String::from("! system usage stats:\n");
    str_.push_str(&format!(
        "!\t{}.{:06} s user, {}.{:06} s system, {}.{:06} s elapsed\n",
        ru.ru_utime.tv_sec - save_r.ru_utime.tv_sec,
        ru.ru_utime.tv_usec - save_r.ru_utime.tv_usec,
        ru.ru_stime.tv_sec - save_r.ru_stime.tv_sec,
        ru.ru_stime.tv_usec - save_r.ru_stime.tv_usec,
        elapse.tv_sec - save_t.tv_sec,
        elapse.tv_usec - save_t.tv_usec,
    ));
    str_.push_str(&format!(
        "!\t[{}.{:06} s user, {}.{:06} s system total]\n",
        user.tv_sec, user.tv_usec, sys.tv_sec, sys.tv_usec,
    ));
    // wasm32: WASI's rusage carries only ru_utime/ru_stime; the counters
    // below don't exist on the type (C's own ShowUsage guards the
    // equivalent section with !defined(WIN32)).
    #[cfg(not(target_family = "wasm"))]
    {
    #[cfg(target_os = "macos")]
    let maxrss = r.ru_maxrss / 1024;
    #[cfg(not(target_os = "macos"))]
    let maxrss = r.ru_maxrss;
    str_.push_str(&format!("!\t{maxrss} kB max resident size\n"));
    str_.push_str(&format!(
        "!\t{}/{} [{}/{}] filesystem blocks in/out\n",
        r.ru_inblock - save_r.ru_inblock,
        r.ru_oublock - save_r.ru_oublock,
        r.ru_inblock,
        r.ru_oublock,
    ));
    str_.push_str(&format!(
        "!\t{}/{} [{}/{}] page faults/reclaims, {} [{}] swaps\n",
        r.ru_majflt - save_r.ru_majflt,
        r.ru_minflt - save_r.ru_minflt,
        r.ru_majflt,
        r.ru_minflt,
        r.ru_nswap - save_r.ru_nswap,
        r.ru_nswap,
    ));
    str_.push_str(&format!(
        "!\t{} [{}] signals rcvd, {}/{} [{}/{}] messages rcvd/sent\n",
        r.ru_nsignals - save_r.ru_nsignals,
        r.ru_nsignals,
        r.ru_msgrcv - save_r.ru_msgrcv,
        r.ru_msgsnd - save_r.ru_msgsnd,
        r.ru_msgrcv,
        r.ru_msgsnd,
    ));
    str_.push_str(&format!(
        "!\t{}/{} [{}/{}] voluntary/involuntary context switches\n",
        r.ru_nvcsw - save_r.ru_nvcsw,
        r.ru_nivcsw - save_r.ru_nivcsw,
        r.ru_nvcsw,
        r.ru_nivcsw,
    ));
    }

    if str_.ends_with('\n') {
        str_.pop();
    }

    ereport(types_error::LOG)
        .errmsg_internal(title.to_string())
        .errdetail_internal(str_)
        .finish(loc(5157, "ShowUsage"))
}


fn guc_context_from_u8(gucctx: u8) -> types_guc::GucContext {
    use types_guc::GucContext::*;
    match gucctx {
        x if x == PGC_INTERNAL as u8 => PGC_INTERNAL,
        x if x == PGC_POSTMASTER as u8 => PGC_POSTMASTER,
        x if x == PGC_SIGHUP as u8 => PGC_SIGHUP,
        x if x == PGC_SU_BACKEND as u8 => PGC_SU_BACKEND,
        x if x == PGC_BACKEND as u8 => PGC_BACKEND,
        x if x == PGC_SUSET as u8 => PGC_SUSET,
        x if x == PGC_USERSET as u8 => PGC_USERSET,
        other => panic!("invalid GucContext discriminant {other}"),
    }
}

fn guc_source_for(ctx: types_guc::GucContext) -> types_guc::GucSource {
    if ctx == types_guc::GucContext::PGC_POSTMASTER {
        types_guc::GucSource::PGC_S_ARGV
    } else {
        types_guc::GucSource::PGC_S_CLIENT
    }
}

pub fn set_debug_options(debug_flag: i32, gucctx: u8) -> PgResult<()> {
    let context = guc_context_from_u8(gucctx);
    let source = guc_source_for(context);

    if debug_flag > 0 {
        let debugstr = format!("debug{debug_flag}");
        guc::SetConfigOption("log_min_messages", Some(&debugstr), context, source)?;
    } else {
        guc::SetConfigOption("log_min_messages", Some("notice"), context, source)?;
    }

    if debug_flag >= 1 && context == types_guc::GucContext::PGC_POSTMASTER {
        guc::SetConfigOption("log_connections", Some("all"), context, source)?;
        guc::SetConfigOption("log_disconnections", Some("true"), context, source)?;
    }
    if debug_flag >= 2 {
        guc::SetConfigOption("log_statement", Some("all"), context, source)?;
    }
    if debug_flag >= 3 {
        guc::SetConfigOption("debug_print_parse", Some("true"), context, source)?;
    }
    if debug_flag >= 4 {
        guc::SetConfigOption("debug_print_plan", Some("true"), context, source)?;
    }
    if debug_flag >= 5 {
        guc::SetConfigOption("debug_print_rewritten", Some("true"), context, source)?;
    }
    Ok(())
}

pub fn set_plan_disabling_options(arg: &str, gucctx: u8) -> PgResult<bool> {
    let context = guc_context_from_u8(gucctx);
    let source = guc_source_for(context);
    let tmp = match arg.as_bytes().first() {
        Some(b's') => Some("enable_seqscan"),
        Some(b'i') => Some("enable_indexscan"),
        Some(b'o') => Some("enable_indexonlyscan"),
        Some(b'b') => Some("enable_bitmapscan"),
        Some(b't') => Some("enable_tidscan"),
        Some(b'n') => Some("enable_nestloop"),
        Some(b'm') => Some("enable_mergejoin"),
        Some(b'h') => Some("enable_hashjoin"),
        _ => None,
    };
    match tmp {
        Some(name) => {
            guc::SetConfigOption(name, Some("false"), context, source)?;
            Ok(true)
        }
        None => Ok(false),
    }
}

pub fn get_stats_option_name(arg: &str) -> Option<&'static str> {
    match arg.as_bytes() {
        [b'p', b'a', ..] => Some("log_parser_stats"),
        [b'p', b'l', ..] => Some("log_planner_stats"),
        [b'e', ..] => Some("log_executor_stats"),
        _ => None,
    }
}
