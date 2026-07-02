// postgres.c — the backend command processor (PG 18.3): exec_simple_query and
// PostgresMain's message loop. Simple-query protocol is end-to-end; the
// extended protocol ('P'/'B'/'E'/'D') and fastpath ('F') arms panic loudly
// (their exec_* family is a later unit).
#![allow(non_snake_case)]

use core::cell::Cell;

use ::elog::ereport;
use ::types_error::{ErrorLocation, PgResult, ERRCODE_QUERY_CANCELED, ERROR, FATAL};

pub mod main_loop;
pub mod simple_query;
#[cfg(test)]
mod tests;

pub use main_loop::PostgresMain;
pub use simple_query::{
    exec_simple_query, finish_xact_command, pg_analyze_and_rewrite_fixedparams, pg_parse_query,
    pg_plan_queries, pg_plan_query, pg_rewrite_query, start_xact_command,
};

pub fn init_seams() {
    postgres_seams::postgres_main::set(postgres_main_seam);
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
    // postgres_seams::process_postgres_switches stays uninstalled (loud):
    // the getopt surface lands with the single-user/postmaster consumers.
}

fn postgres_main_seam(dbname: &str, username: &str) -> ! {
    PostgresMain(dbname, username)
}

thread_local! {
    static XACT_STARTED: Cell<bool> = const { Cell::new(false) };
    static DOING_EXTENDED_QUERY_MESSAGE: Cell<bool> = const { Cell::new(false) };
    static IGNORE_TILL_SYNC: Cell<bool> = const { Cell::new(false) };
    static DOING_COMMAND_READ: Cell<bool> = const { Cell::new(false) };
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
pub fn DoingCommandRead() -> bool {
    DOING_COMMAND_READ.with(Cell::get)
}
pub(crate) fn set_doing_command_read(v: bool) {
    DOING_COMMAND_READ.with(|c| c.set(v));
}

pub(crate) fn loc(line: i32, func: &'static str) -> ErrorLocation {
    ErrorLocation::new("postgres.c", line, func)
}

pub(crate) fn get_current_timestamp() -> types_core::TimestampTz {
    const PG_EPOCH_OFFSET_US: i64 = 946_684_800_000_000; // 2000-01-01 - 1970-01-01
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before 1970");
    now.as_micros() as i64 - PG_EPOCH_OFFSET_US
}


// Per-tuple hot: one TLS load + one predictable branch (C's CHECK_FOR_INTERRUPTS).
#[inline(always)]
pub fn check_for_interrupts() -> PgResult<()> {
    if init_small::globals::InterruptPending() {
        return ProcessInterrupts();
    }
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

#[cold]
#[inline(never)]
pub fn ProcessInterrupts() -> PgResult<()> {
    use init_small::globals as g;

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
                .into());
        }
        // C's worker-process arms are unreachable: those mains panic at launch.
        return Err(ereport(FATAL)
            .errcode(types_error::ERRCODE_ADMIN_SHUTDOWN)
            .errmsg("terminating connection due to administrator command")
            .into_error()
            .into());
    }

    if g::CheckClientConnectionPending() {
        // pq_check_connection() polling is pqcomm-owner work; the flag can only
        // be set by CLIENT_CONNECTION_CHECK_TIMEOUT (timeout lane, unported).
        panic!("ProcessInterrupts: CheckClientConnectionPending set but pq_check_connection not ported");
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
                .into());
        }
        if stmt_timeout_occurred {
            lmgr_proc::LockErrorCleanup()?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_QUERY_CANCELED)
                .errmsg("canceling statement due to statement timeout")
                .into_error()
                .into());
        }
        if !DoingCommandRead() {
            lmgr_proc::LockErrorCleanup()?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_QUERY_CANCELED)
                .errmsg("canceling statement due to user request")
                .into_error()
                .into());
        }
    }

    let conflict_reasons = RECOVERY_CONFLICT_PENDING_REASONS.with(Cell::get);
    if conflict_reasons != 0 {
        panic!(
            "ProcessInterrupts: RecoveryConflictPending (reasons bitmask {conflict_reasons:#x}) \
             but ProcessRecoveryConflictInterrupts is not ported (standby/recovery lane)"
        );
    }

    // C rechecks each timeout GUC (> 0) here; unwired backing vars = loud arms.
    if g::IdleInTransactionSessionTimeoutPending() {
        g::SetIdleInTransactionSessionTimeoutPending(false);
        panic!(
            "ProcessInterrupts: IdleInTransactionSessionTimeoutPending set but the \
             IdleInTransactionSessionTimeout GUC recheck is not wired (guc lane; FATAL 25P03)"
        );
    }
    if g::TransactionTimeoutPending() {
        g::SetTransactionTimeoutPending(false);
        panic!(
            "ProcessInterrupts: TransactionTimeoutPending set but the TransactionTimeout \
             GUC recheck is not wired (guc lane; FATAL 25P04)"
        );
    }
    if g::IdleSessionTimeoutPending() {
        g::SetIdleSessionTimeoutPending(false);
        panic!(
            "ProcessInterrupts: IdleSessionTimeoutPending set but the IdleSessionTimeout \
             GUC recheck is not wired (guc lane; FATAL 57P05)"
        );
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

    if g::LogMemoryContextPending() {
        mcxt_seams::process_log_memory_context_interrupt::call()?;
    }
    // ParallelMessagePending / ParallelApplyMessagePending flags have no
    // storage yet (parallel/logical-apply owners unported).

    Ok(())
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

    // C's _exit(2): no cleanup callbacks; one address space takes every
    // backend, as C's crash/immediate-shutdown cycle does anyway.
    // SAFETY: _exit has no preconditions.
    unsafe { libc::_exit(2) }
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
    if DoingCommandRead() {
        check_for_interrupts()?;

        if sinval::catchupInterruptPending() {
            sinval::ProcessCatchupInterrupt()?;
        }
        // ProcessNotifyInterrupt: async.c lane; its flag cannot be raised yet.
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

fn getrusage_self() -> libc::rusage {
    // SAFETY: plain libc call filling a zeroed out-struct.
    unsafe {
        let mut r: libc::rusage = core::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut r);
        r
    }
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
