//! Family F3 — the signal/interrupt machinery of `tcop/postgres.c`.
//!
//! Every line of control flow, every SQLSTATE and message, and every interrupt
//! flag transition is reproduced from `postgres.c`. The flag globals
//! (`InterruptPending`, `ProcDiePending`, …, the holdoff counters, `MyLatch`)
//! live in `globals.c` (`backend-utils-init-small`); `whereToSendOutput`,
//! `RecoveryConflictPending[Reasons]`, `DoingCommandRead` are `postgres.c`
//! file-locals owned here (see [`crate::globals`]). Subsystem boundaries
//! (latch, timeout, lock manager, transaction state, pgstat, the
//! proc-signal-barrier / parallel-message / log-memory handlers, the
//! notify/catchup interrupts, the client connection check) are reached through
//! their owners' seams or `pub fn`s.

#![allow(non_snake_case)]

use backend_utils_error::{errcode, errdetail, errfinish, errhint, errmsg, errstart};
use types_dest::dest::CommandDest;
use types_error::{
    ErrorLevel, PgResult, ERRCODE_ADMIN_SHUTDOWN, ERRCODE_CONNECTION_FAILURE,
    ERRCODE_DATABASE_DROPPED, ERRCODE_FLOATING_POINT_EXCEPTION,
    ERRCODE_IDLE_IN_TRANSACTION_SESSION_TIMEOUT, ERRCODE_IDLE_SESSION_TIMEOUT,
    ERRCODE_LOCK_NOT_AVAILABLE, ERRCODE_QUERY_CANCELED, ERRCODE_T_R_SERIALIZATION_FAILURE,
    ERRCODE_TRANSACTION_TIMEOUT, DEBUG1, ERROR, FATAL,
};
use types_storage::ProcSignalReason;
use types_storage::storage::{PROCSIG_RECOVERY_CONFLICT_FIRST, PROCSIG_RECOVERY_CONFLICT_LAST};
use types_timeout::TimeoutId;

use backend_utils_init_small::globals as g;

use crate::globals;

// `__FILE__` / `__LINE__` / `__func__` for `errfinish`.
macro_rules! here {
    ($func:expr) => {
        (Some(file!()), line!() as i32, Some($func))
    };
}

/// Process-type predicate `MyBackendType == B_xxx` (miscadmin.h).
fn my_backend_type() -> types_core::init::BackendType {
    g::MyBackendType()
}

/// `AmAutoVacuumWorkerProcess()` (miscadmin.h).
fn am_autovacuum_worker_process() -> bool {
    my_backend_type() == types_core::init::BackendType::AutovacWorker
}

/// `AmWalReceiverProcess()` (miscadmin.h).
fn am_wal_receiver_process() -> bool {
    my_backend_type() == types_core::init::BackendType::WalReceiver
}

/// `AmBackgroundWorkerProcess()` (miscadmin.h).
fn am_background_worker_process() -> bool {
    my_backend_type() == types_core::init::BackendType::BgWorker
}

/// `AmIoWorkerProcess()` (miscadmin.h).
fn am_io_worker_process() -> bool {
    my_backend_type() == types_core::init::BackendType::IoWorker
}

/// `SetLatch(MyLatch)` (latch.c) — wake anything waiting on the process latch.
fn set_latch_my_latch() {
    backend_storage_ipc_latch_seams::set_latch_my_latch::call();
}

/// `LockErrorCleanup()` (proc.c).
fn lock_error_cleanup() {
    backend_storage_lmgr_proc_seams::lock_error_cleanup::call();
}

/// `ProcessClientReadInterrupt(bool blocked)` (postgres.c:496) — process any
/// interrupt that arrived while waiting to read from the client.
///
/// Must preserve `errno`; in this port `errno` is not a process-global we touch
/// (the seams below carry their own error surface), so the C save/restore of
/// `save_errno` collapses to nothing.
pub fn ProcessClientReadInterrupt(blocked: bool) -> PgResult<()> {
    if globals::doing_command_read() {
        // Check for general interrupts that arrived before/while reading.
        check_for_interrupts()?;

        // Process sinval catchup interrupts, if any.
        if backend_storage_ipc_sinval::catchupInterruptPending() {
            backend_storage_ipc_sinval::ProcessCatchupInterrupt()?;
        }

        // Process notify interrupts, if any.
        if backend_commands_async::notify_interrupt_pending() {
            backend_commands_async::ProcessNotifyInterrupt(true)?;
        }
    } else if g::ProcDiePending() {
        // We're dying. If there is no data available to read, then it's safe
        // (and sane) to handle that now. If we haven't tried to read yet, make
        // sure the process latch is set, so that if there is no data then we'll
        // come back here and die. If we're done reading, also make sure the
        // process latch is set, as we might've undesirably cleared it while
        // reading.
        if blocked {
            check_for_interrupts()?;
        } else {
            set_latch_my_latch();
        }
    }

    Ok(())
}

/// `ProcessClientWriteInterrupt(bool blocked)` (postgres.c:546) — process any
/// interrupt that arrived while waiting to write to the client.
pub fn ProcessClientWriteInterrupt(blocked: bool) -> PgResult<()> {
    if g::ProcDiePending() {
        // We're dying. If it's not possible to write, then we should handle
        // that immediately, else a stuck client could indefinitely delay our
        // response to the signal. If we haven't tried to write yet, make sure
        // the process latch is set, so that if the write would block then we'll
        // come back here and die. If we're done writing, also make sure the
        // process latch is set, as we might've undesirably cleared it while
        // writing.
        if blocked {
            // Don't mess with whereToSendOutput if ProcessInterrupts wouldn't
            // service ProcDiePending.
            if g::InterruptHoldoffCount() == 0 && g::CritSectionCount() == 0 {
                // We don't want to send the client the error message, as a)
                // that would possibly block again, and b) it would likely lead
                // to loss of protocol sync because we may have already sent a
                // partial protocol message.
                if globals::where_to_send_output() == CommandDest::Remote {
                    globals::set_where_to_send_output(CommandDest::None);
                }

                check_for_interrupts()?;
            }
        } else {
            set_latch_my_latch();
        }
    }

    Ok(())
}

/// `CHECK_FOR_INTERRUPTS()` (miscadmin.h:123) — the out-of-line body is
/// [`ProcessInterrupts`]; the macro is `if (InterruptPending) ProcessInterrupts()`.
/// (`INTERRUPTS_PENDING_CONDITION()` reduces to `InterruptPending` on the
/// non-Windows build.)
pub fn check_for_interrupts() -> PgResult<()> {
    if g::InterruptPending() {
        ProcessInterrupts()?;
    }
    Ok(())
}

/// `quickdie(SIGNAL_ARGS)` (postgres.c:2928) — hard die in response to a
/// SIGQUIT from the postmaster: notify the client if we safely can, then
/// `_exit(2)` without running any cleanup (shared memory may be corrupt).
/// Never returns.
pub fn quickdie(_postgres_signal_arg: i32) -> ! {
    // sigaddset(&BlockSig, SIGQUIT); sigprocmask(SIG_SETMASK, &BlockSig, NULL):
    // prevent nested calls.
    backend_libpq_pqsignal::block_sig_add(libc_sigquit());
    backend_libpq_pqsignal::set_block_sig_mask();

    // Prevent interrupts while exiting; one may have been pending. We don't want
    // a quickdie() downgraded to a mere query cancel.
    g::HoldInterrupts();

    // If we're aborting out of client auth, don't risk trying to send anything
    // to the client.
    if backend_utils_error::config::client_auth_in_progress()
        && globals::where_to_send_output() == CommandDest::Remote
    {
        globals::set_where_to_send_output(CommandDest::None);
    }

    // Clear the error context stack, so that context callbacks are not called.
    // In this port the C `error_context_stack` callback chain is replaced by the
    // PgResult/builder model; there are no ambient context callbacks to NULL out
    // here, so the C `error_context_stack = NULL` collapses to nothing.

    // When responding to a postmaster-issued signal, send the message only to
    // the client.
    match backend_storage_ipc_pmsignal::GetQuitSignalReason() {
        backend_storage_ipc_pmsignal::QuitSignalReason::PMQUIT_NOT_SENT => {
            // Hmm, SIGQUIT arrived out of the blue.
            let _ = quickdie_report(
                types_error::WARNING,
                ERRCODE_ADMIN_SHUTDOWN,
                "terminating connection because of unexpected SIGQUIT signal",
                None,
                None,
            );
        }
        backend_storage_ipc_pmsignal::QuitSignalReason::PMQUIT_FOR_CRASH => {
            // A crash-and-restart cycle is in progress.
            let _ = quickdie_report(
                types_error::WARNING_CLIENT_ONLY,
                types_error::ERRCODE_CRASH_SHUTDOWN,
                "terminating connection because of crash of another server process",
                Some(
                    "The postmaster has commanded this server process to roll back \
                     the current transaction and exit, because another server \
                     process exited abnormally and possibly corrupted shared memory.",
                ),
                Some(
                    "In a moment you should be able to reconnect to the \
                     database and repeat your command.",
                ),
            );
        }
        backend_storage_ipc_pmsignal::QuitSignalReason::PMQUIT_FOR_STOP => {
            // Immediate-mode stop.
            let _ = quickdie_report(
                types_error::WARNING_CLIENT_ONLY,
                ERRCODE_ADMIN_SHUTDOWN,
                "terminating connection due to immediate shutdown command",
                None,
                None,
            );
        }
    }

    // We DO NOT run proc_exit() or atexit() callbacks. Note we do _exit(2) not
    // _exit(0), to force the postmaster into a system reset cycle.
    unsafe { libc::_exit(2) }
}

/// `SIGQUIT` as a `libc::c_int`.
fn libc_sigquit() -> libc::c_int {
    libc::SIGQUIT
}

/// One of `quickdie`'s `ereport(WARNING[_CLIENT_ONLY], ...)` reports.
fn quickdie_report(
    level: ErrorLevel,
    code: types_error::SqlState,
    msg: &str,
    detail: Option<&str>,
    hint: Option<&str>,
) -> PgResult<()> {
    if errstart(level, None) {
        errcode(code)?;
        errmsg(msg)?;
        if let Some(d) = detail {
            errdetail(d)?;
        }
        if let Some(h) = hint {
            errhint(h)?;
        }
        let (f, l, fc) = here!("quickdie");
        errfinish(f, l, fc)?;
    }
    Ok(())
}

/// `die(SIGNAL_ARGS)` (postgres.c:3025) — shutdown signal from postmaster:
/// abort transaction and exit at soonest convenient time. Async-signal-safe.
pub fn die(_postgres_signal_arg: i32) {
    // Don't joggle the elbow of proc_exit.
    if !backend_utils_error::config::proc_exit_inprogress() {
        g::SetInterruptPending(true);
        g::SetProcDiePending(true);
    }

    // For the cumulative stats system: pgStatSessionEndCause = DISCONNECT_KILLED.
    backend_tcop_postgres_seams::set_session_end_cause_killed::call();

    // If we're still here, waken anything waiting on the process latch.
    set_latch_my_latch();

    // If we're in single user mode, we want to quit immediately - we can't rely
    // on latches as they wouldn't work when stdin/stdout is a file. Rather
    // ugly, but it's unlikely to be worthwhile to invest much more effort just
    // for the benefit of single user mode.
    if globals::doing_command_read() && globals::where_to_send_output() != CommandDest::Remote {
        // `die` is an async-signal handler; `ProcessInterrupts` can ereport
        // (longjmp in C). The C signature is `void`, so any error is dropped
        // here exactly as the C handler discards the longjmp target's control
        // flow (it unwinds out of the signal handler frame).
        let _ = ProcessInterrupts();
    }
}

/// `StatementCancelHandler(SIGNAL_ARGS)` (postgres.c:3055) — query-cancel
/// signal from postmaster: abort current transaction at soonest convenient time.
pub fn StatementCancelHandler(_postgres_signal_arg: i32) {
    // Don't joggle the elbow of proc_exit.
    if !backend_utils_error::config::proc_exit_inprogress() {
        g::SetInterruptPending(true);
        g::SetQueryCancelPending(true);
    }

    // If we're still here, waken anything waiting on the process latch.
    set_latch_my_latch();
}

/// `FloatExceptionHandler(SIGNAL_ARGS)` (postgres.c:3072) — signal handler for
/// floating point exception.
pub fn FloatExceptionHandler(_postgres_signal_arg: i32) -> PgResult<()> {
    // We're not returning, so no need to save errno.
    if errstart(ERROR, None) {
        errcode(ERRCODE_FLOATING_POINT_EXCEPTION)?;
        errmsg("floating-point exception")?;
        errdetail(
            "An invalid floating-point operation was signaled. \
             This probably means an out-of-range result or an \
             invalid operation, such as division by zero.",
        )?;
        let (f, l, fc) = here!("FloatExceptionHandler");
        errfinish(f, l, fc)?;
    }
    Ok(())
}

/// `pqsigfunc`-shaped wrapper for [`FloatExceptionHandler`]: the C handler is
/// `void (*)(int)` and `ereport(ERROR)`s (longjmping out via the active
/// `PG_exception_stack`). The Rust body returns `PgResult<()>`; a genuine
/// SIGFPE surfaces the error, so unwrap it (mirroring the C longjmp) rather
/// than swallow it. Returned by the `float_exception_handler` seam so callers
/// can install it with `pqsignal(SIGFPE, ...)`.
pub fn float_exception_handler_fn(postgres_signal_arg: i32) {
    FloatExceptionHandler(postgres_signal_arg).expect("FloatExceptionHandler");
}

/// `HandleRecoveryConflictInterrupt(ProcSignalReason reason)` (postgres.c:3088)
/// — tell the next `CHECK_FOR_INTERRUPTS()` to check for a particular type of
/// recovery conflict. Runs in a SIGUSR1 handler. Async-signal-safe, infallible.
pub fn HandleRecoveryConflictInterrupt(reason: ProcSignalReason) {
    globals::set_recovery_conflict_pending_reason(reason, true);
    globals::set_recovery_conflict_pending(true);
    g::SetInterruptPending(true);
    // latch will be set by procsignal_sigusr1_handler.
}

/// `GetAwaitedLock() == NULL` (lock.c) — whether this backend is waiting on a
/// lock. The proc.c owner expresses the same predicate as
/// `get_awaited_lock_hashcode() != -1`.
fn awaited_lock_is_set() -> bool {
    backend_storage_lmgr_lock_seams::get_awaited_lock_hashcode::call() != -1
}

/// `ProcessRecoveryConflictInterrupt(ProcSignalReason reason)` (postgres.c:3100)
/// — check one individual conflict reason.
fn ProcessRecoveryConflictInterrupt(reason: ProcSignalReason) -> PgResult<()> {
    // The C is a `switch (reason)` with intentional fall-through. We mirror its
    // exact case-label fall-through chain with three flags. The C order of the
    // shared tail is:
    //   case LOCK/TABLESPACE/SNAPSHOT: if(!IsTxnOrBlock) return; -> falls to
    //   case LOGICALSLOT: {reason==LOGICALSLOT || !IsSubTxn() block} -> falls to
    //   case DATABASE: {session cancel}.
    // BUFFERPIN (after setting MyProc->recoveryConflictPending) and
    // STARTUP_DEADLOCK fall into the LOCK case, so they DO run the
    // IsTransactionOrTransactionBlock guard; a *direct* LOGICALSLOT entry does
    // NOT (it lands one case later). `do_fall_lock` = entered the LOCK case
    // (guard applies); `entered_at_logicalslot` = direct LOGICALSLOT entry
    // (skip the LOCK guard, run the LOGICALSLOT block); `do_fall_database` =
    // the final session-cancel block.
    let mut do_fall_lock = false;
    let mut entered_at_logicalslot = false;
    let mut do_fall_database = false;

    match reason {
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK => {
            // If we aren't waiting for a lock we can never deadlock.
            if !awaited_lock_is_set() {
                return Ok(());
            }
            // Intentional fall through to check wait for pin.
            fall_bufferpin(reason, &mut do_fall_lock)?;
        }
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_BUFFERPIN => {
            fall_bufferpin(reason, &mut do_fall_lock)?;
        }
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_LOCK
        | ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_TABLESPACE
        | ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_SNAPSHOT => {
            do_fall_lock = true;
        }
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT => {
            // Enters at `case LOGICALSLOT:` — the LOCK-case
            // IsTransactionOrTransactionBlock guard is NOT run.
            entered_at_logicalslot = true;
        }
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_DATABASE => {
            do_fall_database = true;
        }
        _ => {
            // elog(FATAL, "unrecognized conflict mode: %d", (int) reason)
            if errstart(FATAL, None) {
                errmsg(&alloc_unrecognized(reason))?;
                let (f, l, fc) = here!("ProcessRecoveryConflictInterrupt");
                errfinish(f, l, fc)?;
            }
            return Ok(());
        }
    }

    if do_fall_lock {
        // case PROCSIG_RECOVERY_CONFLICT_LOCK / TABLESPACE / SNAPSHOT:
        //   if (!IsTransactionOrTransactionBlock()) return;
        // This guard runs for everything that reaches the LOCK case body —
        // LOCK/TABLESPACE/SNAPSHOT directly, and BUFFERPIN/STARTUP_DEADLOCK by
        // fall-through — but NOT for a direct LOGICALSLOT entry, which lands at
        // the next case label (handled via `entered_at_logicalslot`).
        if !backend_access_transam_xact_seams::is_transaction_or_transaction_block::call() {
            // If we aren't in a transaction any longer then ignore.
            return Ok(());
        }
        // Fall through to the LOGICALSLOT case body.
        entered_at_logicalslot = true;
    }

    if entered_at_logicalslot {
        // case PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT:
        // If we're not in a subtransaction then we are OK to throw an ERROR to
        // resolve the conflict. Otherwise drop through to the FATAL case.
        if reason == ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT
            || !backend_access_transam_xact_seams::is_sub_transaction::call()
        {
            // If we already aborted then we no longer need to cancel.
            if backend_access_transam_xact_seams::is_aborted_transaction_block_state::call() {
                return Ok(());
            }

            // If a recovery conflict happens while we are waiting for input from
            // the client, the client is presumably just sitting idle in a
            // transaction; drop through to the FATAL case below to dislodge it.
            if !globals::doing_command_read() {
                // Avoid losing sync in the FE/BE protocol.
                if g::QueryCancelHoldoffCount() != 0 {
                    // Re-arm and defer this interrupt until later.
                    globals::set_recovery_conflict_pending_reason(reason, true);
                    globals::set_recovery_conflict_pending(true);
                    g::SetInterruptPending(true);
                    return Ok(());
                }

                // We are cleared to throw an ERROR.
                lock_error_cleanup();
                backend_tcop_postgres_seams::pgstat_report_recovery_conflict::call(reason);
                if errstart(ERROR, None) {
                    errcode(ERRCODE_T_R_SERIALIZATION_FAILURE)?;
                    errmsg("canceling statement due to conflict with recovery")?;
                    errdetail_recovery_conflict(reason)?;
                    let (f, l, fc) = here!("ProcessRecoveryConflictInterrupt");
                    errfinish(f, l, fc)?;
                }
                return Ok(());
            }
        }

        // Intentional fall through to session cancel.
        do_fall_database = true;
    }

    if do_fall_database {
        // case PROCSIG_RECOVERY_CONFLICT_DATABASE:
        // Retrying is not possible because the database is dropped, or we
        // decided above that we couldn't resolve the conflict with an ERROR and
        // fell through. Terminate the session.
        backend_tcop_postgres_seams::pgstat_report_recovery_conflict::call(reason);
        if errstart(FATAL, None) {
            errcode(if reason == ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_DATABASE {
                ERRCODE_DATABASE_DROPPED
            } else {
                ERRCODE_T_R_SERIALIZATION_FAILURE
            })?;
            errmsg("terminating connection due to conflict with recovery")?;
            errdetail_recovery_conflict(reason)?;
            errhint(
                "In a moment you should be able to reconnect to the \
                 database and repeat your command.",
            )?;
            let (f, l, fc) = here!("ProcessRecoveryConflictInterrupt");
            errfinish(f, l, fc)?;
        }
    }

    Ok(())
}

/// The shared `PROCSIG_RECOVERY_CONFLICT_BUFFERPIN` arm (also entered by the
/// STARTUP_DEADLOCK fall-through). Sets `do_fall_lock` when control should
/// fall through to the LOCK/.../session-cancel block.
fn fall_bufferpin(reason: ProcSignalReason, do_fall_lock: &mut bool) -> PgResult<()> {
    // If PROCSIG_RECOVERY_CONFLICT_BUFFERPIN is requested but we aren't blocking
    // the Startup process there is nothing more to do. When
    // PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK is requested, if we're waiting
    // for locks and the startup process is not waiting for buffer pin, we set
    // the flag so that ProcSleep() will check for deadlocks.
    if !backend_storage_buffer_bufmgr_seams::holding_buffer_pin_that_delays_recovery::call() {
        if reason == ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK
            && backend_storage_lmgr_proc::proc_lifecycle::GetStartupBufferPinWaitBufId() < 0
        {
            backend_storage_lmgr_proc_seams::check_dead_lock_alert::call();
        }
        return Ok(());
    }

    backend_storage_lmgr_proc::proc_misc::set_my_proc_recovery_conflict_pending(true);

    // Intentional fall through to error handling (the LOCK/.../session block).
    *do_fall_lock = true;
    Ok(())
}

/// `elog(FATAL, "unrecognized conflict mode: %d", (int) reason)`.
fn alloc_unrecognized(reason: ProcSignalReason) -> alloc::string::String {
    alloc::format!("unrecognized conflict mode: {}", reason as i32)
}

/// `ProcessRecoveryConflictInterrupts(void)` (postgres.c:3258) — check each
/// possible recovery conflict reason.
fn ProcessRecoveryConflictInterrupts() -> PgResult<()> {
    // We don't need to worry about joggling the elbow of proc_exit, because
    // proc_exit_prepare() holds interrupts, so ProcessInterrupts() won't call us.
    debug_assert!(!backend_utils_error::config::proc_exit_inprogress());
    debug_assert_eq!(g::InterruptHoldoffCount(), 0);
    debug_assert!(globals::recovery_conflict_pending());

    globals::set_recovery_conflict_pending(false);

    // C: for (reason = PROCSIG_RECOVERY_CONFLICT_FIRST;
    //         reason <= PROCSIG_RECOVERY_CONFLICT_LAST; reason++)
    // The recovery-conflict reasons are contiguous in declaration order from
    // FIRST (DATABASE) through LAST (STARTUP_DEADLOCK); enumerate them
    // explicitly (the `ProcSignalReason` enum has no integer round-trip).
    debug_assert_eq!(
        PROCSIG_RECOVERY_CONFLICT_FIRST,
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_DATABASE
    );
    debug_assert_eq!(
        PROCSIG_RECOVERY_CONFLICT_LAST,
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK
    );
    for reason in [
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_DATABASE,
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_TABLESPACE,
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_LOCK,
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_SNAPSHOT,
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT,
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_BUFFERPIN,
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK,
    ] {
        if globals::recovery_conflict_pending_reason(reason) {
            globals::set_recovery_conflict_pending_reason(reason, false);
            ProcessRecoveryConflictInterrupt(reason)?;
        }
    }

    Ok(())
}

/// `ProcessInterrupts(void)` (postgres.c:3297) — out-of-line portion of the
/// `CHECK_FOR_INTERRUPTS()` macro. Called only when `InterruptPending` is true.
pub fn ProcessInterrupts() -> PgResult<()> {
    // OK to accept any interrupts now?
    if g::InterruptHoldoffCount() != 0 || g::CritSectionCount() != 0 {
        return Ok(());
    }
    g::SetInterruptPending(false);

    if g::ProcDiePending() {
        g::SetProcDiePending(false);
        g::SetQueryCancelPending(false); // ProcDie trumps QueryCancel
        lock_error_cleanup();
        // As in quickdie, don't risk sending to client during auth.
        if backend_utils_error::config::client_auth_in_progress()
            && globals::where_to_send_output() == CommandDest::Remote
        {
            globals::set_where_to_send_output(CommandDest::None);
        }
        if backend_utils_error::config::client_auth_in_progress() {
            return die_fatal(
                ERRCODE_QUERY_CANCELED,
                "canceling authentication due to timeout".to_string(),
            );
        } else if am_autovacuum_worker_process() {
            return die_fatal(
                ERRCODE_ADMIN_SHUTDOWN,
                "terminating autovacuum process due to administrator command".to_string(),
            );
        } else if backend_tcop_postgres_seams::is_logical_worker::call() {
            return die_fatal(
                ERRCODE_ADMIN_SHUTDOWN,
                "terminating logical replication worker due to administrator command".to_string(),
            );
        } else if backend_replication_logical_launcher_seams::IsLogicalLauncher::call() {
            if errstart(DEBUG1, None) {
                errmsg("logical replication launcher shutting down")?;
                let (f, l, fc) = here!("ProcessInterrupts");
                errfinish(f, l, fc)?;
            }
            // The logical replication launcher can be stopped at any time. Use
            // exit status 1 so the background worker is restarted.
            backend_storage_ipc_ipc_seams::proc_exit::call(1);
        } else if am_wal_receiver_process() {
            return die_fatal(
                ERRCODE_ADMIN_SHUTDOWN,
                "terminating walreceiver process due to administrator command".to_string(),
            );
        } else if am_background_worker_process() {
            let bgw_type = backend_tcop_postgres_seams::my_bgworker_type::call();
            return die_fatal(
                ERRCODE_ADMIN_SHUTDOWN,
                alloc::format!(
                    "terminating background worker \"{}\" due to administrator command",
                    bgw_type
                ),
            );
        } else if am_io_worker_process() {
            if errstart(DEBUG1, None) {
                errmsg("io worker shutting down due to administrator command")?;
                let (f, l, fc) = here!("ProcessInterrupts");
                errfinish(f, l, fc)?;
            }
            backend_storage_ipc_ipc_seams::proc_exit::call(0);
        } else {
            return die_fatal(
                ERRCODE_ADMIN_SHUTDOWN,
                "terminating connection due to administrator command".to_string(),
            );
        }
    }

    if g::CheckClientConnectionPending() {
        g::SetCheckClientConnectionPending(false);

        // Check for lost connection and re-arm, if still configured, but not if
        // we've arrived back at DoingCommandRead state.
        if !globals::doing_command_read() && client_connection_check_interval() > 0 {
            if !backend_libpq_pqcomm::pq_check_connection()? {
                g::SetClientConnectionLost(true);
            } else {
                backend_utils_misc_timeout::enable_timeout_after(
                    TimeoutId::CLIENT_CONNECTION_CHECK_TIMEOUT,
                    client_connection_check_interval(),
                )?;
            }
        }
    }

    if g::ClientConnectionLost() {
        g::SetQueryCancelPending(false); // lost connection trumps QueryCancel
        lock_error_cleanup();
        // don't send to client, we already know the connection to be dead.
        globals::set_where_to_send_output(CommandDest::None);
        return die_fatal(
            ERRCODE_CONNECTION_FAILURE,
            "connection to client lost".to_string(),
        );
    }

    // Don't allow query cancel interrupts while reading input from the client,
    // because we might lose sync in the FE/BE protocol. (Die interrupts are OK.)
    if g::QueryCancelPending() && g::QueryCancelHoldoffCount() != 0 {
        // Re-arm InterruptPending so that we process the cancel request as soon
        // as we're done reading the message.
        g::SetInterruptPending(true);
    } else if g::QueryCancelPending() {
        g::SetQueryCancelPending(false);

        // If LOCK_TIMEOUT and STATEMENT_TIMEOUT indicators are both set, we need
        // to clear both, so always fetch both.
        let mut lock_timeout_occurred =
            backend_utils_misc_timeout::get_timeout_indicator(TimeoutId::LOCK_TIMEOUT, true);
        let stmt_timeout_occurred =
            backend_utils_misc_timeout::get_timeout_indicator(TimeoutId::STATEMENT_TIMEOUT, true);

        // If both were set, report whichever timeout completed earlier; a tie is
        // arbitrarily broken in favor of reporting a lock timeout.
        if lock_timeout_occurred
            && stmt_timeout_occurred
            && backend_utils_misc_timeout::get_timeout_finish_time(TimeoutId::STATEMENT_TIMEOUT)
                < backend_utils_misc_timeout::get_timeout_finish_time(TimeoutId::LOCK_TIMEOUT)
        {
            lock_timeout_occurred = false; // report stmt timeout
        }

        if lock_timeout_occurred {
            lock_error_cleanup();
            return die_error(
                ERRCODE_LOCK_NOT_AVAILABLE,
                "canceling statement due to lock timeout".to_string(),
            );
        }
        if stmt_timeout_occurred {
            lock_error_cleanup();
            return die_error(
                ERRCODE_QUERY_CANCELED,
                "canceling statement due to statement timeout".to_string(),
            );
        }
        if am_autovacuum_worker_process() {
            lock_error_cleanup();
            return die_error(
                ERRCODE_QUERY_CANCELED,
                "canceling autovacuum task".to_string(),
            );
        }

        // If we are reading a command from the client, just ignore the cancel
        // request. Otherwise, go ahead and throw the error.
        if !globals::doing_command_read() {
            lock_error_cleanup();
            return die_error(
                ERRCODE_QUERY_CANCELED,
                "canceling statement due to user request".to_string(),
            );
        }
    }

    if globals::recovery_conflict_pending() {
        ProcessRecoveryConflictInterrupts()?;
    }

    if g::IdleInTransactionSessionTimeoutPending() {
        // If the GUC has been reset to zero, ignore the signal.
        g::SetIdleInTransactionSessionTimeoutPending(false);
        if idle_in_transaction_session_timeout() > 0 {
            // INJECTION_POINT("idle-in-transaction-session-timeout", NULL) — a
            // no-op in the default (injection-points-disabled) build.
            return die_fatal(
                ERRCODE_IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
                "terminating connection due to idle-in-transaction timeout".to_string(),
            );
        }
    }

    if g::TransactionTimeoutPending() {
        // As above, ignore the signal if the GUC has been reset to zero.
        g::SetTransactionTimeoutPending(false);
        if transaction_timeout() > 0 {
            // INJECTION_POINT("transaction-timeout", NULL) — no-op default build.
            return die_fatal(
                ERRCODE_TRANSACTION_TIMEOUT,
                "terminating connection due to transaction timeout".to_string(),
            );
        }
    }

    if g::IdleSessionTimeoutPending() {
        // As above, ignore the signal if the GUC has been reset to zero.
        g::SetIdleSessionTimeoutPending(false);
        if idle_session_timeout() > 0 {
            // INJECTION_POINT("idle-session-timeout", NULL) — no-op default build.
            return die_fatal(
                ERRCODE_IDLE_SESSION_TIMEOUT,
                "terminating connection due to idle-session timeout".to_string(),
            );
        }
    }

    // If there are pending stats updates and we currently are truly idle, report
    // stats now.
    if g::IdleStatsUpdateTimeoutPending()
        && globals::doing_command_read()
        && !backend_access_transam_xact_seams::is_transaction_or_transaction_block::call()
    {
        g::SetIdleStatsUpdateTimeoutPending(false);
        backend_utils_activity_pgstat_seams::pgstat_report_stat::call(true)?;
    }

    // `ProcSignalBarrierPending` is owned by procsignal.c (set by the SIGUSR1
    // handler `HandleProcSignalBarrierInterrupt`, cleared by
    // `ProcessProcSignalBarrier`); it lives as that crate's thread-local, not
    // the `globals.c` duplicate, so read it from the owner. Reading the
    // init-small copy here was always false — nothing sets it — which is why a
    // delivered barrier signal never triggered `ProcessProcSignalBarrier` and
    // `WaitForProcSignalBarrier` (DROP DATABASE) hung forever on its own slot.
    if backend_storage_ipc_procsignal::ProcSignalBarrierPending() {
        backend_storage_ipc_procsignal::ProcessProcSignalBarrier()?;
    }

    if backend_access_transam_parallel::parallel_message_pending() {
        backend_access_transam_parallel::process_parallel_messages()?;
    }

    if g::LogMemoryContextPending() {
        backend_utils_mmgr_mcxt_seams::process_log_memory_context_interrupt::call()?;
    }

    if backend_replication_logical_applyparallelworker::parallel_apply_message_pending() {
        backend_replication_logical_applyparallelworker::ProcessParallelApplyMessages()?;
    }

    Ok(())
}

/// `ereport(FATAL, (errcode(code), errmsg(msg)))`.
fn die_fatal(code: types_error::SqlState, msg: alloc::string::String) -> PgResult<()> {
    if errstart(FATAL, None) {
        errcode(code)?;
        errmsg(&msg)?;
        let (f, l, fc) = here!("ProcessInterrupts");
        errfinish(f, l, fc)?;
    }
    Ok(())
}

/// `ereport(ERROR, (errcode(code), errmsg(msg)))`.
fn die_error(code: types_error::SqlState, msg: alloc::string::String) -> PgResult<()> {
    if errstart(ERROR, None) {
        errcode(code)?;
        errmsg(&msg)?;
        let (f, l, fc) = here!("ProcessInterrupts");
        errfinish(f, l, fc)?;
    }
    Ok(())
}

/// `errdetail_recovery_conflict(reason)` (postgres.c:2552) — add an
/// `errdetail()` line showing the conflict source. C returns `int` (always 0)
/// after calling the ambient `errdetail`; we mirror that against the ambient
/// error stack.
pub fn errdetail_recovery_conflict(reason: ProcSignalReason) -> PgResult<()> {
    match reason {
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_BUFFERPIN => {
            errdetail("User was holding shared buffer pin for too long.")?;
        }
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_LOCK => {
            errdetail("User was holding a relation lock for too long.")?;
        }
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_TABLESPACE => {
            errdetail("User was or might have been using tablespace that must be dropped.")?;
        }
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_SNAPSHOT => {
            errdetail("User query might have needed to see row versions that must be removed.")?;
        }
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT => {
            errdetail("User was using a logical replication slot that must be invalidated.")?;
        }
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK => {
            errdetail("User transaction caused buffer deadlock with recovery.")?;
        }
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_DATABASE => {
            errdetail("User was connected to a database that must be dropped.")?;
        }
        _ => {
            // no errdetail
        }
    }
    Ok(())
}

/// `errdetail_abort(void)` (postgres.c:2538) — add an `errdetail()` line showing
/// abort reason, if any. C returns `int` (always 0).
pub fn errdetail_abort() -> PgResult<()> {
    if backend_storage_lmgr_proc::proc_misc::my_proc_recovery_conflict_pending() {
        errdetail("Abort reason: recovery conflict")?;
    }
    Ok(())
}

// ---- GUC reads used by the interrupt machinery (postgres.c-referenced GUCs) ----

fn client_connection_check_interval() -> i32 {
    backend_utils_misc_guc_tables::vars::client_connection_check_interval.read()
}

fn idle_in_transaction_session_timeout() -> i32 {
    backend_utils_misc_guc_tables::vars::IdleInTransactionSessionTimeout.read()
}

fn transaction_timeout() -> i32 {
    backend_utils_misc_guc_tables::vars::TransactionTimeout.read()
}

fn idle_session_timeout() -> i32 {
    backend_utils_misc_guc_tables::vars::IdleSessionTimeout.read()
}
