// PostgresMain + ReadCommand/SocketBackend (postgres.c). The C sigsetjmp
// recovery block is the Err/panic arm of each loop iteration.
use ::elog::ereport;
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::stringinfo::StringInfo;
use ::types_dest::CommandDest;
use ::types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_CONNECTION_FAILURE, ERRCODE_PROTOCOL_VIOLATION,
    ERROR, FATAL, LOG,
};

use crate::{
    check_for_interrupts, extended_query, loc, set_doing_command_read,
    set_doing_extended_query_message, set_ignore_till_sync, set_xact_started, simple_query,
    ignore_till_sync,
};

mod pqmsg {
    pub const QUERY: i32 = b'Q' as i32;
    pub const PARSE: i32 = b'P' as i32;
    pub const BIND: i32 = b'B' as i32;
    pub const EXECUTE: i32 = b'E' as i32;
    pub const FUNCTION_CALL: i32 = b'F' as i32;
    pub const CLOSE: i32 = b'C' as i32;
    pub const DESCRIBE: i32 = b'D' as i32;
    pub const FLUSH: i32 = b'H' as i32;
    pub const SYNC: i32 = b'S' as i32;
    pub const TERMINATE: i32 = b'X' as i32;
    pub const COPY_DATA: i32 = b'd' as i32;
    pub const COPY_DONE: i32 = b'c' as i32;
    pub const COPY_FAIL: i32 = b'f' as i32;
    pub const CLOSE_COMPLETE: u8 = b'3';
    pub const BACKEND_KEY_DATA: u8 = b'K';
}

const EOF: i32 = pqcomm::EOF;

const PQ_SMALL_MESSAGE_LIMIT: i32 = 10000;
const PQ_LARGE_MESSAGE_LIMIT: i32 = 0x3fffffff - 1; /* MaxAllocSize - 1 */

fn SocketBackend(in_buf: &mut StringInfo<'_>) -> PgResult<i32> {
    // HOLD_CANCEL_INTERRUPTS() ... RESUME_CANCEL_INTERRUPTS(): a query cancel
    // must not fire mid-read and lose FE/BE sync; the guard resumes on every
    // exit path.
    struct CancelHoldoff;
    impl Drop for CancelHoldoff {
        fn drop(&mut self) {
            init_small::globals::ResumeCancelInterrupts();
        }
    }
    init_small::globals::HoldCancelInterrupts();
    let _holdoff = CancelHoldoff;

    pqcomm::pq_startmsgread()?;
    let qtype = pqcomm::pq_getbyte()?;

    if qtype == EOF {
        if xact::IsTransactionState() {
            ereport(types_error::COMMERROR)
                .errcode(ERRCODE_CONNECTION_FAILURE)
                .errmsg("unexpected EOF on client connection with an open transaction")
                .finish(loc(369, "SocketBackend"))?;
        } else {
            elog::config::set_where_to_send_output(CommandDest::None);
        }
        return Ok(qtype);
    }

    let maxmsglen = match qtype {
        x if x == pqmsg::QUERY || x == pqmsg::FUNCTION_CALL => {
            set_doing_extended_query_message(false);
            PQ_LARGE_MESSAGE_LIMIT
        }
        x if x == pqmsg::TERMINATE => {
            set_doing_extended_query_message(false);
            set_ignore_till_sync(false);
            PQ_SMALL_MESSAGE_LIMIT
        }
        x if x == pqmsg::BIND || x == pqmsg::PARSE => {
            set_doing_extended_query_message(true);
            PQ_LARGE_MESSAGE_LIMIT
        }
        x if x == pqmsg::CLOSE
            || x == pqmsg::DESCRIBE
            || x == pqmsg::EXECUTE
            || x == pqmsg::FLUSH =>
        {
            set_doing_extended_query_message(true);
            PQ_SMALL_MESSAGE_LIMIT
        }
        x if x == pqmsg::SYNC => {
            set_ignore_till_sync(false);
            set_doing_extended_query_message(false);
            PQ_SMALL_MESSAGE_LIMIT
        }
        x if x == pqmsg::COPY_DATA => {
            set_doing_extended_query_message(false);
            PQ_LARGE_MESSAGE_LIMIT
        }
        x if x == pqmsg::COPY_DONE || x == pqmsg::COPY_FAIL => {
            set_doing_extended_query_message(false);
            PQ_SMALL_MESSAGE_LIMIT
        }
        other => {
            return Err(ereport(FATAL)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg(format!("invalid frontend message type {other}"))
                .into_error()
                .into());
        }
    };

    if pqcomm::pq_getmessage(in_buf, maxmsglen)? != 0 {
        return Ok(EOF); /* suitable message already logged */
    }

    Ok(qtype)
}

fn ReadCommand(in_buf: &mut StringInfo<'_>) -> PgResult<i32> {
    if elog::config::where_to_send_output() == CommandDest::Remote {
        SocketBackend(in_buf)
    } else {
        InteractiveBackend(in_buf)
    }
}

// interactive_getc (postgres.c:318): one byte from stdin with the interrupt
// drains around it. C's getc EINTR surface maps to ErrorKind::Interrupted:
// run the read-interrupt drain and retry, as a signal-interrupted getc does.
fn interactive_getc() -> PgResult<Option<u8>> {
    use std::io::Read;
    check_for_interrupts()?;
    loop {
        let mut byte = [0u8; 1];
        let r = std::io::stdin().lock().read(&mut byte);
        crate::ProcessClientReadInterrupt(false)?;
        match r {
            Ok(0) => return Ok(None), /* EOF */
            Ok(_) => return Ok(Some(byte[0])),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => {
                return Err(ereport(FATAL)
                    .errcode(ERRCODE_CONNECTION_FAILURE)
                    .errmsg(format!("could not read from standard input: {e}"))
                    .into_error()
                    .into());
            }
        }
    }
}

// InteractiveBackend (postgres.c:236): stdin is the query source; the
// interactive delimiter is newline (or ";\n\n" under -j).
fn InteractiveBackend(in_buf: &mut StringInfo<'_>) -> PgResult<i32> {
    use std::io::Write;

    /* display a prompt and obtain input from the user */
    print!("backend> ");
    let _ = std::io::stdout().flush();

    in_buf.reset();

    /* Read characters until EOF or the appropriate delimiter is seen. */
    let mut saw_eof = true;
    while let Some(c) = interactive_getc()? {
        if c == b'\n' {
            if crate::use_semi_newline_newline() {
                /*
                 * In -j mode, semicolon followed by two newlines ends the
                 * command; otherwise treat newline as regular character.
                 */
                if in_buf.len() > 1
                    && in_buf.as_bytes()[in_buf.len() - 1] == b'\n'
                    && in_buf.as_bytes()[in_buf.len() - 2] == b';'
                {
                    /* might as well drop the second newline */
                    saw_eof = false;
                    break;
                }
            } else {
                /*
                 * In plain mode, newline ends the command unless preceded by
                 * backslash.
                 */
                if in_buf.len() > 0 && in_buf.as_bytes()[in_buf.len() - 1] == b'\\' {
                    /* discard backslash from inBuf */
                    in_buf.truncate(in_buf.len() - 1);
                    /* discard newline too */
                    continue;
                }
                /* keep the newline character, but end the command */
                in_buf.append_byte(b'\n')?;
                saw_eof = false;
                break;
            }
        }

        /* Not newline, or newline treated as regular character */
        in_buf.append_byte(c)?;
    }

    /* No input before EOF signal means time to quit. */
    if saw_eof && in_buf.is_empty() {
        return Ok(EOF);
    }

    /* Add '\0' to make it look the same as message case. */
    in_buf.append_byte(0)?;

    /* if the query echo flag was given, print the query.. */
    if crate::echo_query() {
        let query = &in_buf.as_bytes()[..in_buf.len() - 1];
        println!("statement: {}", String::from_utf8_lossy(query));
    }
    let _ = std::io::stdout().flush();

    Ok(pqmsg::QUERY)
}

pub(crate) struct LoopState {
    pub(crate) send_ready_for_query: bool,
    pub(crate) idle_in_transaction_timeout_enabled: bool,
    pub(crate) idle_session_timeout_enabled: bool,
}

pub(crate) fn error_recovery(
    err: &PgError,
    state: &mut LoopState,
    first_attempt: bool,
) -> PgResult<()> {
    use init_small::globals as g;

    /* error_context_stack = NULL: the ambient callback chain is Err-carried. */

    // C's elog.c ERROR path resets the holdoff counters before longjmp; here
    // the catching frame does it (an unbalanced HOLD_INTERRUPTS on the unwind
    // path would otherwise leak forever).
    g::SetInterruptHoldoffCount(0);
    g::SetQueryCancelHoldoffCount(0);
    g::SetCritSectionCount(0);

    // GL-ERRFIX-1 (E-4 + E-5) — EMIT THE REPORT ONCE, BEFORE ANY FALLIBLE STEP.
    // Two defects, one placement fixes both:
    //   E-4: the report used to sit AFTER `disable_all_timeouts` (the first
    //        fallible `?`). If that step failed, the original error reached
    //        neither the client nor the log — the connection then died after 16
    //        silent retries with a generic "failed to settle" FATAL and nothing
    //        anywhere said why. Emitting ahead of every `?` means no recovery
    //        step can swallow the original error.
    //   E-5: the keeper retries recovery up to MAX_RECOVERY_ATTEMPTS times, and
    //        on each retry the pending error is the recovery-step failure, so
    //        the client collected up to 16 reports for one original error.
    //        `first_attempt` is false on every retry, so the report is emitted
    //        at most once per original error.
    if first_attempt {
        elog::emit_error_report_for(err);
    }

    timeout_seams::disable_all_timeouts::call(false)?; /* do first to avoid race */
    g::SetQueryCancelPending(false);
    state.idle_in_transaction_timeout_enabled = false;
    state.idle_session_timeout_enabled = false;

    set_doing_command_read(false);

    pqcomm::pq_comm_reset();

    // (the report emission moved to the top of this function — GL-ERRFIX-1 E-4/E-5.)


    xact::AbortCurrentTransaction()?;

    if walsender_seams::am_walsender() {
        walsender_seams::wal_snd_error_cleanup::call()?;
    }

    portalmem::PortalErrorCleanup()?;

    // Slots can't be released inside AbortTransaction (C keeps them across
    // sub-xacts); top-level error recovery is the C release point
    // (postgres.c:4457).
    if slot::MyReplicationSlot().is_some() {
        slot::ReplicationSlotRelease()?;
    }
    slot::ReplicationSlotCleanup(false)?;


    elog::FlushErrorState();

    if crate::doing_extended_query_message() {
        set_ignore_till_sync(true);
    }

    set_xact_started(false);

    if pqcomm::pq_is_reading_msg() {
        return Err(ereport(FATAL)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("terminating connection because protocol synchronization was lost")
            .into_error()
            .into());
    }

    Ok(())
}

// An earlier-or-equal TRANSACTION_TIMEOUT subsumes the idle-in-transaction timer.
fn start_idle_in_transaction_timer(state: &mut LoopState) -> PgResult<()> {
    let idle_in_transaction_timeout = lmgr_proc::globals::IdleInTransactionSessionTimeout();
    let transaction_timeout = lmgr_proc::globals::TransactionTimeout();
    if idle_in_transaction_timeout > 0
        && (idle_in_transaction_timeout < transaction_timeout || transaction_timeout == 0)
    {
        state.idle_in_transaction_timeout_enabled = true;
        timeout_seams::enable_timeout_after::call(
            timeout_seams::IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
            idle_in_transaction_timeout,
        )?;
    }
    Ok(())
}

fn ready_state(mcx: Mcx<'_>, state: &mut LoopState) -> PgResult<()> {
    use backend_status_seams::BackendState;
    crate::stmt_trace::probe("ready.begin");

    if xact::IsAbortedTransactionBlockState() {
        ps_status_seams::set_ps_display::call("idle in transaction (aborted)");
        backend_status_seams::pgstat_report_activity::call(
            BackendState::STATE_IDLEINTRANSACTION_ABORTED,
            None,
        );
        start_idle_in_transaction_timer(state)?;
    } else if xact::IsTransactionOrTransactionBlock() {
        ps_status_seams::set_ps_display::call("idle in transaction");
        backend_status_seams::pgstat_report_activity::call(
            BackendState::STATE_IDLEINTRANSACTION,
            None,
        );
        start_idle_in_transaction_timer(state)?;
    } else {
        if commands_async::notifyInterruptPending() {
            commands_async::ProcessNotifyInterrupt(false)?;
        }

        let stats_timeout = pgstat::pending::pgstat_report_stat(false);
        if stats_timeout > 0 {
            if !timeout_seams::get_timeout_active::call(timeout_seams::IDLE_STATS_UPDATE_TIMEOUT) {
                timeout_seams::enable_timeout_after::call(
                    timeout_seams::IDLE_STATS_UPDATE_TIMEOUT,
                    stats_timeout as i32,
                )?;
            }
        } else if timeout_seams::get_timeout_active::call(timeout_seams::IDLE_STATS_UPDATE_TIMEOUT)
        {
            timeout_seams::disable_timeout::call(timeout_seams::IDLE_STATS_UPDATE_TIMEOUT, false)?;
        }

        ps_status_seams::set_ps_display::call("idle");
        backend_status_seams::pgstat_report_activity::call(BackendState::STATE_IDLE, None);

        let idle_session_timeout = lmgr_proc::globals::IdleSessionTimeout();
        if idle_session_timeout > 0 {
            state.idle_session_timeout_enabled = true;
            timeout_seams::enable_timeout_after::call(
                timeout_seams::IDLE_SESSION_TIMEOUT,
                idle_session_timeout,
            )?;
        }
    }

    guc::report::report_changed_guc_options();

    log_connection_ready_durations()?;

    crate::stmt_trace::probe("ready.pre_rfq");
    tcop_dest::ReadyForQuery(mcx, elog::config::where_to_send_output())?;
    crate::stmt_trace::probe("rfq.flushed");
    crate::stmt_trace::flush();
    state.send_ready_for_query = false;

    Ok(())
}

// postgres.c: first ReadyForQuery of an external-connection backend logs the
// connection-establishment durations under log_connections=setup_durations.
fn log_connection_ready_durations() -> PgResult<()> {
    use types_core::init::BackendType;

    let timing = backend_startup::conn_timing::get();
    // TIMESTAMP_MINUS_INFINITY (types_startup) is i64::MIN.
    if timing.ready_for_use != i64::MIN
        || backend_startup::log_connections::get()
            & backend_startup::LOG_CONNECTION_SETUP_DURATIONS
            == 0
        || !matches!(
            miscinit::GetMyBackendType(),
            BackendType::Backend | BackendType::WalSender
        )
    {
        return Ok(());
    }

    let now = crate::get_current_timestamp();
    backend_startup::conn_timing::set_ready_for_use(now);

    let diff_us = |start: i64, stop: i64| -> u64 {
        if start >= stop { 0 } else { (stop - start) as u64 }
    };
    let total = diff_us(timing.socket_create, now);
    let fork = diff_us(timing.fork_start, timing.fork_end);
    let auth = diff_us(timing.auth_start, timing.auth_end);

    ereport(LOG)
        .errmsg(format!(
            "connection ready: setup total={:.3} ms, fork={:.3} ms, authentication={:.3} ms",
            total as f64 / 1000.0,
            fork as f64 / 1000.0,
            auth as f64 / 1000.0,
        ))
        .finish(types_error::ErrorLocation::new(
            "src/backend/tcop/postgres.c",
            4677,
            "PostgresMain",
        ))
}

// postgres.c:4760 walsender 'Q' arm; #[cold] keeps the serial query path's
// icache untouched (layout-hygiene convention).
#[cold]
#[inline(never)]
fn walsender_query<'mcx>(mcx: Mcx<'mcx>, query_string: &'mcx str) -> PgResult<()> {
    if !walsender_seams::exec_replication_command::call(query_string)? {
        simple_query::exec_simple_query(mcx, query_string)?;
    }
    Ok(())
}

// forbidden_in_wal_sender (postgres.c:5030): a replication connection speaks
// only the simple-query protocol.
fn forbidden_in_wal_sender(firstchar: i32) -> PgResult<()> {
    if walsender_seams::am_walsender() {
        forbidden_in_wal_sender_error(firstchar)?;
    }
    Ok(())
}

#[cold]
#[inline(never)]
fn forbidden_in_wal_sender_error(firstchar: i32) -> PgResult<()> {
    let msg = if firstchar == pqmsg::FUNCTION_CALL {
        "fastpath function calls not supported in a replication connection"
    } else {
        "extended query protocol not supported in a replication connection"
    };
    ereport(ERROR)
        .errcode(ERRCODE_PROTOCOL_VIOLATION)
        .errmsg(msg)
        .finish(loc(5036, "forbidden_in_wal_sender"))
}

fn dispatch_message<'mcx>(
    mcx: Mcx<'mcx>,
    firstchar: i32,
    input_message: &mut StringInfo<'mcx>,
    state: &mut LoopState,
) -> PgResult<()> {
    match firstchar {
        x if x == pqmsg::QUERY => {
            xact::SetCurrentStatementStartTimestamp();

            let query_string: &'mcx str = {
                let s = pqformat::pq_getmsgstring(mcx, input_message)?;
                leak_str_in(mcx, s.as_bytes())?
            };
            pqformat::pq_getmsgend(input_message)?;

            if walsender_seams::am_walsender() {
                walsender_query(mcx, query_string)?;
            } else {
                simple_query::exec_simple_query(mcx, query_string)?;
            }

            state.send_ready_for_query = true;
        }

        x if x == pqmsg::PARSE => {
            forbidden_in_wal_sender(firstchar)?;

            xact::SetCurrentStatementStartTimestamp();

            let stmt_name = extended_query::owned_msg_string(mcx, input_message)?;
            let query_string = extended_query::owned_msg_string(mcx, input_message)?;
            let num_params = pqformat::pq_getmsgint(input_message, 2)? as usize;
            let mut param_types: PgVec<'_, types_core::Oid> = PgVec::new_in(mcx);
            param_types
                .try_reserve_exact(num_params)
                .map_err(|_| mcx.oom(num_params))?;
            for _ in 0..num_params {
                param_types.push(pqformat::pq_getmsgint(input_message, 4)?);
            }
            pqformat::pq_getmsgend(input_message)?;

            extended_query::exec_parse_message(
                mcx,
                query_string.as_str(),
                stmt_name.as_str(),
                &param_types,
            )?;
        }

        x if x == pqmsg::BIND => {
            forbidden_in_wal_sender(firstchar)?;

            xact::SetCurrentStatementStartTimestamp();

            /* this message is complex enough that the field extraction is
             * out-of-line, as in C */
            extended_query::exec_bind_message(mcx, input_message)?;
        }

        x if x == pqmsg::EXECUTE => {
            forbidden_in_wal_sender(firstchar)?;

            xact::SetCurrentStatementStartTimestamp();

            let portal_name = extended_query::owned_msg_string(mcx, input_message)?;
            let max_rows = pqformat::pq_getmsgint(input_message, 4)? as i32;
            pqformat::pq_getmsgend(input_message)?;

            extended_query::exec_execute_message(mcx, portal_name.as_str(), max_rows as i64)?;
        }

        x if x == pqmsg::DESCRIBE => {
            forbidden_in_wal_sender(firstchar)?;

            xact::SetCurrentStatementStartTimestamp();

            let describe_type = pqformat::pq_getmsgbyte(input_message)?;
            let describe_target = extended_query::owned_msg_string(mcx, input_message)?;
            pqformat::pq_getmsgend(input_message)?;

            match describe_type as u8 {
                b'S' => {
                    extended_query::exec_describe_statement_message(
                        mcx,
                        describe_target.as_str(),
                    )?;
                }
                b'P' => {
                    extended_query::exec_describe_portal_message(
                        mcx,
                        describe_target.as_str(),
                    )?;
                }
                other => {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_PROTOCOL_VIOLATION)
                        .errmsg(format!("invalid DESCRIBE message subtype {other}"))
                        .into_error()
                        .into());
                }
            }
        }

        x if x == pqmsg::FUNCTION_CALL => {
            use backend_status_seams::BackendState;

            forbidden_in_wal_sender(firstchar)?;

            xact::SetCurrentStatementStartTimestamp();

            backend_status_seams::pgstat_report_activity::call(BackendState::STATE_FASTPATH, None);
            ps_status_seams::set_ps_display::call("<FASTPATH>");

            simple_query::start_xact_command()?;

            let was_logged = fastpath::HandleFunctionRequest(mcx, input_message)?;

            match simple_query::check_log_duration(was_logged) {
                (1, msec_str) => {
                    ereport(LOG)
                        .errmsg(format!("duration: {msec_str} ms"))
                        .errhidestmt(true)
                        .finish(ErrorLocation::new("fastpath.c", 312, "HandleFunctionRequest"))?;
                }
                (2, msec_str) => {
                    ereport(LOG)
                        .errmsg(format!("duration: {msec_str} ms  fastpath function call"))
                        .errhidestmt(true)
                        .finish(ErrorLocation::new("fastpath.c", 316, "HandleFunctionRequest"))?;
                }
                _ => {}
            }

            simple_query::finish_xact_command()?;

            state.send_ready_for_query = true;
        }

        x if x == pqmsg::CLOSE => {
            forbidden_in_wal_sender(firstchar)?;

            let close_type = pqformat::pq_getmsgbyte(input_message)?;
            let close_target = {
                let s = pqformat::pq_getmsgrawstring(input_message)?;
                core::str::from_utf8(s)
                    .map_err(|_| {
                        Box::new(PgError::new(ERROR, "invalid string in message".to_string()))
                    })?
                    .to_string()
            };
            pqformat::pq_getmsgend(input_message)?;

            match close_type as u8 {
                b'S' => {
                    if !close_target.is_empty() {
                        prepare_seams::drop_prepared_statement::call(&close_target, false)?;
                    } else {
                        extended_query::drop_unnamed_stmt();
                    }
                }
                b'P' => {
                    if let Some(portal) = portalmem::GetPortalByName(Some(&close_target)) {
                        portalmem::PortalDrop(&portal, false)?;
                    }
                }
                other => {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_PROTOCOL_VIOLATION)
                        .errmsg(format!("invalid CLOSE message subtype {other}"))
                        .into_error()
                        .into());
                }
            }

            if elog::config::where_to_send_output() == CommandDest::Remote {
                pqformat::pq_putemptymessage(pqmsg::CLOSE_COMPLETE)?;
            }
        }

        x if x == pqmsg::FLUSH => {
            pqformat::pq_getmsgend(input_message)?;
            if elog::config::where_to_send_output() == CommandDest::Remote {
                pqcomm::pq_flush()?;
            }
        }

        x if x == pqmsg::SYNC => {
            pqformat::pq_getmsgend(input_message)?;
            xact::EndImplicitTransactionBlock();
            simple_query::finish_xact_command()?;
            crate::stmt_trace::probe("s.commit");
            state.send_ready_for_query = true;
        }

        x if x == EOF || x == pqmsg::TERMINATE => {
            if elog::config::where_to_send_output() == CommandDest::Remote {
                elog::config::set_where_to_send_output(CommandDest::None);
            }
            ipc_seams::proc_exit::call(0, init_small::globals::MyProcPid());
        }

        x if x == pqmsg::COPY_DATA || x == pqmsg::COPY_DONE || x == pqmsg::COPY_FAIL => {
        }

        other => {
            return Err(ereport(FATAL)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg(format!("invalid frontend message type {other}"))
                .into_error()
                .into());
        }
    }

    Ok(())
}

fn leak_str_in<'mcx>(mcx: Mcx<'mcx>, bytes: &[u8]) -> PgResult<&'mcx str> {
    let mut v: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    mcx::vec_append_bytes(&mut v, bytes)?;
    let slice: &'mcx mut [u8] = v.leak();
    // Invalid bytes for the database encoding were already rejected by
    // pq_getmsgstring's pg_client_to_server (pg_verify_mbstr, C-shaped
    // 22021); reaching here non-UTF8 means the database encoding itself
    // is one the UTF-8 engine cannot honor.
    core::str::from_utf8(slice).map_err(|_| crate::non_utf8_query_error())
}

// on_proc_exit handler to log end of session (postgres.c log_disconnections).
// Runs on every backend exit once registered: clean Terminate/EOF, FATAL
// teardown, and admin termination all pass through proc_exit's callback walk.
fn log_disconnections(_code: i32, _arg: usize) {
    let now = crate::get_current_timestamp();
    let start = init_small::globals::MyStartTimestamp();
    let diff_us = (now - start).max(0);
    let mut secs = diff_us / 1_000_000;
    let usecs = diff_us % 1_000_000;
    let msecs = usecs / 1000;

    let hours = secs / 3600;
    secs %= 3600;
    let minutes = secs / 60;
    let seconds = secs % 60;

    let msg = init_small::globals::WithMyProcPort(|port| {
        format!(
            "disconnection: session time: {hours}:{minutes:02}:{seconds:02}.{msecs:03} \
             user={} database={} host={}{}{}",
            port.user_name.as_deref().unwrap_or(""),
            port.database_name.as_deref().unwrap_or(""),
            port.remote_host,
            if port.remote_port.is_empty() { "" } else { " port=" },
            port.remote_port,
        )
    });
    let _ = ereport(LOG).errmsg(msg).finish(loc(5168, "log_disconnections"));
}

pub fn PostgresMain(dbname: &str, username: &str) -> ! {
    let outcome = postgres_main_inner(dbname, username);
    if let Err(err) = outcome {
        elog::emit_error_report_for(&err);
    }
    ipc_seams::proc_exit::call(1, init_small::globals::MyProcPid())
}

fn postgres_main_inner(dbname: &str, username: &str) -> PgResult<()> {
    assert!(!dbname.is_empty() || !username.is_empty());

    crate::install_thread_signal_handlers();

    timeout_seams::initialize_timeouts::call(); /* establishes SIGALRM handler */

    if elog::config::where_to_send_output() == CommandDest::Remote {
        let len = init_small::globals::WithMyProcPort(|port| {
            const PG_PROTOCOL_3_2: u32 = (3 << 16) | 2;
            if port.proto >= PG_PROTOCOL_3_2 {
                types_core::primitive::MAX_CANCEL_KEY_LENGTH
            } else {
                4
            }
        });
        let mut key = [0u8; types_core::primitive::MAX_CANCEL_KEY_LENGTH];
        pg_strong_random(&mut key[..len])?;
        init_small::globals::SetMyCancelKey(key);
        init_small::globals::SetMyCancelKeyLength(len as i32);
    }

    postinit::BaseInit()?;

    // Honor session_preload_libraries if not dealing with a WAL sender.
    let am_walsender = walsender_seams::am_walsender();
    let top = MemoryContext::new("PostgresMainInit");
    postinit::InitPostgres(
        top.mcx(),
        Some(dbname),
        types_core::primitive::InvalidOid,
        Some(username),
        types_core::primitive::InvalidOid,
        if am_walsender {
            0
        } else {
            postinit::INIT_PG_LOAD_SESSION_LIBS
        },
        None,
    )?;
    drop(top);


    miscinit::SetProcessingMode(types_core::ProcessingMode::NormalProcessing);

    guc::report::begin_reporting_guc_options();

    // C postgres.c:4313: set up the session-end logger only now — we have to
    // wait till here to be sure Log_disconnections has its final value
    // (PGC_SU_BACKEND: startup-packet options are applied by InitPostgres).
    if init_small::globals::IsUnderPostmaster() && guc_tables::vars::Log_disconnections.read() {
        ipc::on_proc_exit(log_disconnections, 0);
    }

    pgstat::database::pgstat_report_connect(init_small::globals::MyDatabaseId());

    if am_walsender {
        walsender_seams::init_wal_sender::call();
    }

    if elog::config::where_to_send_output() == CommandDest::Remote {
        let key = init_small::globals::MyCancelKey();
        let len = init_small::globals::MyCancelKeyLength() as usize;
        debug_assert!(len > 0);
        let scratch = MemoryContext::new("CancelKeyMsg");
        let mut buf = pqformat::pq_beginmessage(scratch.mcx(), pqmsg::BACKEND_KEY_DATA)?;
        pqformat::pq_sendint32(&mut buf, init_small::globals::MyProcPid() as u32)?;
        pqformat::pq_sendbytes(&mut buf, &key[..len])?;
        pqformat::pq_endmessage(buf)?;
    }

    /* Welcome banner for standalone case (postgres.c:4341) */
    if elog::config::where_to_send_output() == CommandDest::Debug {
        println!("\nPostgreSQL stand-alone backend 18.3");
    }

    let mut message_context = MemoryContext::new_bump("MessageContext");

    // C postgres.c:4369: fire login event triggers before the main loop; a
    // trigger error here aborts the connection, as C's pre-setjmp ERROR does.
    {
        let scratch = MemoryContext::new("EventTriggerOnLogin");
        event_trigger_seams::event_trigger_on_login::call(scratch.mcx())?;
    }

    let mut state = LoopState {
        send_ready_for_query: true,
        idle_in_transaction_timeout_enabled: false,
        idle_session_timeout_enabled: false,
    };

    loop {
        /*
         * Release storage left over from prior query cycle. C resets the
         * long-lived MessageContext; this is that reset. Stmt-list registry
         * handles are NOT reset here: extended-protocol portals carry them
         * across messages, and PortalDrop frees them.
         */
        message_context.reset();
        let mcx = message_context.mcx();

        let iteration = run_one_iteration(mcx, &mut state);

        match iteration {
            Ok(()) => {}
            Err(err) => {
                if err.level() >= FATAL {
                    return Err(err);
                }
                let mut pending = err;
                const MAX_RECOVERY_ATTEMPTS: u32 = 16;
                let mut settled = false;
                let mut first_attempt = true;
                for _ in 0..MAX_RECOVERY_ATTEMPTS {
                    let attempt_is_first = first_attempt;
                    first_attempt = false;
                    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        error_recovery(&pending, &mut state, attempt_is_first)
                    })) {
                        Ok(Ok(())) => {
                            settled = true;
                            break;
                        }
                        Ok(Err(next)) => {
                            if next.level() >= FATAL {
                                return Err(next);
                            }
                            pending = next;
                        }
                        Err(payload) => {
                            pending = Box::new(pg_error_from_panic(payload));
                        }
                    }
                }
                if !settled {
                    return Err(ereport(FATAL)
                        .errmsg_internal(
                            "error recovery failed to settle the transaction; terminating backend",
                        )
                        .into_error()
                        .into());
                }
                if !ignore_till_sync() {
                    state.send_ready_for_query = true; /* initially, or after error */
                }
            }
        }
    }
}

// One iteration of the C for(;;) body (postgres.c:4516-5021), Err = the
// sigsetjmp path. Panics from unported seams are mapped to ERROR-level errors
// so the backend recovers as C does from ereport(ERROR).
pub(crate) fn run_one_iteration(mcx: Mcx<'_>, state: &mut LoopState) -> PgResult<()> {
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_one_iteration_inner(mcx, state)
    }));
    match outcome {
        Ok(r) => r,
        Err(payload) => Err(Box::new(pg_error_from_panic(payload))),
    }
}

fn pg_error_from_panic(payload: Box<dyn std::any::Any + Send>) -> PgError {
    // proc_exit unwinds ProcExitThread; converting it to an ERROR turns
    // backend exit into an infinite recovery loop (client EOF -> proc_exit(0)
    // -> "recovered" -> ReadCommand panic, ~850/s). Re-raise it.
    // KilledBySignal (SIGKILL crash-test injection) must reach the thread top
    // as a crash, exactly like ProcExitThread/PanicExitThread — demoting it to
    // ERROR would make a SIGKILL'd backend recover as FATAL(1) and skip the
    // postmaster crash cascade.
    if payload.is::<ipc::ProcExitThread>()
        || payload.is::<types_error::PanicExitThread>()
        || payload.is::<ipc::KilledBySignal>()
    {
        std::panic::resume_unwind(payload);
    }
    match ::types_error::pg_error_from_panic(payload) {
        Ok(e) => e,
        Err(payload) => {
            let msg = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "backend panicked".to_string());
            PgError::new(ERROR, msg)
        }
    }
}

fn run_one_iteration_inner<'mcx>(mcx: Mcx<'mcx>, state: &mut LoopState) -> PgResult<()> {
    set_doing_extended_query_message(false);

    let mut input_message = StringInfo::new_in(mcx)?;

    snapmgr::InvalidateCatalogSnapshotConditionally();

    if state.send_ready_for_query {
        ready_state(mcx, state)?;
    }

    set_doing_command_read(true);

    let firstchar = ReadCommand(&mut input_message)?;
    crate::stmt_trace::probe_read(firstchar);

    if state.idle_in_transaction_timeout_enabled {
        timeout_seams::disable_timeout::call(
            timeout_seams::IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
            false,
        )?;
        state.idle_in_transaction_timeout_enabled = false;
    }
    if state.idle_session_timeout_enabled {
        timeout_seams::disable_timeout::call(timeout_seams::IDLE_SESSION_TIMEOUT, false)?;
        state.idle_session_timeout_enabled = false;
    }

    check_for_interrupts()?;
    set_doing_command_read(false);

    if interrupt::ConfigReloadPending() {
        interrupt::SetConfigReloadPending(false);
        guc_file::ProcessConfigFile(types_guc_context_sighup())?;
    }

    if ignore_till_sync() && firstchar != EOF {
        return Ok(());
    }

    let r = dispatch_message(mcx, firstchar, &mut input_message, state);
    crate::stmt_trace::probe("cycle.end");
    r
}

fn types_guc_context_sighup() -> types_guc::GucContext {
    types_guc::GucContext::PGC_SIGHUP
}

fn pg_strong_random(buf: &mut [u8]) -> PgResult<()> {
    if !pg_strong_random::pg_strong_random(buf) {
        return Err(ereport(ERROR)
            .errmsg("could not generate random cancel key")
            .into_error()
            .into());
    }
    Ok(())
}
