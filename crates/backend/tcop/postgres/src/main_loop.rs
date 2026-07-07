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
        panic!("ReadCommand (postgres.c:487): InteractiveBackend (single-user mode) not ported");
    }
}

pub(crate) struct LoopState {
    pub(crate) send_ready_for_query: bool,
    pub(crate) idle_in_transaction_timeout_enabled: bool,
    pub(crate) idle_session_timeout_enabled: bool,
}

pub(crate) fn error_recovery(err: &PgError, state: &mut LoopState) -> PgResult<()> {
    use init_small::globals as g;

    /* error_context_stack = NULL: the ambient callback chain is Err-carried. */

    // C's elog.c ERROR path resets the holdoff counters before longjmp; here
    // the catching frame does it (an unbalanced HOLD_INTERRUPTS on the unwind
    // path would otherwise leak forever).
    g::SetInterruptHoldoffCount(0);
    g::SetQueryCancelHoldoffCount(0);
    g::SetCritSectionCount(0);

    timeout_seams::disable_all_timeouts::call(false)?; /* do first to avoid race */
    g::SetQueryCancelPending(false);
    state.idle_in_transaction_timeout_enabled = false;
    state.idle_session_timeout_enabled = false;

    set_doing_command_read(false);

    pqcomm::pq_comm_reset();

    elog::emit_error_report_for(err);


    xact::AbortCurrentTransaction()?;

    /* am_walsender WalSndErrorCleanup: walsender unported. */

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


    crate::stmt_trace::probe("ready.pre_rfq");
    tcop_dest::ReadyForQuery(mcx, elog::config::where_to_send_output())?;
    crate::stmt_trace::probe("rfq.flushed");
    crate::stmt_trace::flush();
    state.send_ready_for_query = false;

    Ok(())
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

            simple_query::exec_simple_query(mcx, query_string)?;

            state.send_ready_for_query = true;
        }

        x if x == pqmsg::PARSE => {
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
            xact::SetCurrentStatementStartTimestamp();

            /* this message is complex enough that the field extraction is
             * out-of-line, as in C */
            extended_query::exec_bind_message(mcx, input_message)?;
        }

        x if x == pqmsg::EXECUTE => {
            xact::SetCurrentStatementStartTimestamp();

            let portal_name = extended_query::owned_msg_string(mcx, input_message)?;
            let max_rows = pqformat::pq_getmsgint(input_message, 4)? as i32;
            pqformat::pq_getmsgend(input_message)?;

            extended_query::exec_execute_message(mcx, portal_name.as_str(), max_rows as i64)?;
        }

        x if x == pqmsg::DESCRIBE => {
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
    core::str::from_utf8(slice)
        .map_err(|_| Box::new(PgError::new(ERROR, "invalid byte sequence in query string".to_string())))
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

    let top = MemoryContext::new("PostgresMainInit");
    postinit::InitPostgres(
        top.mcx(),
        Some(dbname),
        types_core::primitive::InvalidOid,
        Some(username),
        types_core::primitive::InvalidOid,
        postinit::INIT_PG_LOAD_SESSION_LIBS,
        None,
    )?;
    drop(top);


    miscinit::SetProcessingMode(types_core::ProcessingMode::NormalProcessing);

    guc::report::begin_reporting_guc_options();


    pgstat::database::pgstat_report_connect(init_small::globals::MyDatabaseId());

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
                for _ in 0..MAX_RECOVERY_ATTEMPTS {
                    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        error_recovery(&pending, &mut state)
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
    if payload.is::<ipc::ProcExitThread>() || payload.is::<types_error::PanicExitThread>() {
        std::panic::resume_unwind(payload);
    }
    match payload.downcast::<PgError>() {
        Ok(e) => *e,
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
