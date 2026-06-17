//! `tcop/postgres.c` — the backend's main processing loop (F0a).
//!
//! This module ports the post-auth backend driver:
//!
//!   * [`PostgresMain`]   — postgres.c:4184 (the `for (;;)` ReadCommand loop +
//!     error-recovery block + idle-state handling + message-tag dispatch)
//!   * [`ReadCommand`]    — postgres.c:479
//!   * [`SocketBackend`]  — postgres.c:351 (frontend message read)
//!   * [`forbidden_in_wal_sender`] — postgres.c:5031
//!
//! The simple-Query (`'Q'`) path is landed end-to-end:
//! [`PostgresMain`] reads the `'Q'` message, extracts the query string into the
//! per-message `MessageContext`, and calls
//! [`crate::simple_query::exec_simple_query`], whose parse→analyze→rewrite→plan
//! →portal→run pipeline is complete.
//!
//! # Sanctioned divergences (audit against these)
//!
//! 1. **`sigsetjmp` → `PgResult` recovery.** The C `if (sigsetjmp(...))` outer
//!    error handler becomes: the per-command work runs in a helper returning
//!    `PgResult`; on `Err` the loop runs the recovery block ([`error_recovery`])
//!    — `EmitErrorReport`/`FlushErrorState`/`AbortCurrentTransaction` — exactly
//!    as the C catch arm does, then continues the loop. This is the
//!    backend-utils-error sanctioned model (PG_exception_stack does not exist).
//! 2. **Per-iteration `MessageContext`.** C resets a long-lived `MessageContext`
//!    once per loop; here each iteration creates a child `MemoryContext` off the
//!    backend top context and lets it drop at end of iteration (the `'mcx`
//!    region is the iteration body). The query string is copied into it (it
//!    points into `MessageContext` in C too).
//! 3. **Unported message paths seam-and-panic.** The extended-query protocol
//!    (`'P'`/`'B'`/`'E'`/`'D'`) exec functions (`exec_parse_message` etc.) are
//!    the F2 family and exist nowhere yet; their dispatch arms panic with a
//!    precise rationale (the simple-Query target never reaches them). The
//!    `'C'` Close and `'S'` Sync arms are ported (their deps exist). Fastpath
//!    (`'F'`) is landed (backend-tcop-fastpath). COPY-protocol data messages
//!    are accepted-and-ignored per spec.

#![allow(non_snake_case)]

extern crate alloc;

use alloc::string::String;

use mcx::{Mcx, MemoryContext};
use types_dest::dest::CommandDest;
use types_error::{PgResult, ERROR, FATAL};
use types_stringinfo::StringInfo;
use types_timeout::TimeoutId;

use backend_utils_error::ereport;

use crate::globals;

// Seam crate aliases.
use backend_access_transam_xact_seams as xact_seams;
use backend_tcop_dest_seams as dest_seams;
use backend_utils_activity_status_seams as status_seams;
use backend_utils_misc_more_seams as more_seams;

// Owner crates called directly for entry points with no consumable seam
// (acyclic: none depends on this crate — fastpath/pquery dep only the
// `*-seams` leaves, verified at the Cargo level).
use backend_access_transam_xact as xact;
use backend_libpq_pqcomm as pqcomm;
use backend_libpq_pqformat as pqformat;
use backend_postmaster_interrupt as pm_interrupt;
use backend_tcop_fastpath as fastpath;
use backend_utils_misc_timeout as timeout;

// ===========================================================================
// PqMsg_* frontend message type codes (protocol.h)
// ===========================================================================

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
}

/// `EOF` sentinel returned by [`ReadCommand`]/[`SocketBackend`] on a lost
/// connection (C's `EOF == -1`).
const EOF: i32 = -1;

/// `PQ_SMALL_MESSAGE_LIMIT` (libpq.h): cap for short fixed-shape messages.
const PQ_SMALL_MESSAGE_LIMIT: i32 = 10000;
/// `PQ_LARGE_MESSAGE_LIMIT` (libpq.h): `MaxAllocSize - 1`.
const PQ_LARGE_MESSAGE_LIMIT: i32 = mcx::MAX_ALLOC_SIZE as i32 - 1;

// ===========================================================================
// SocketBackend — postgres.c:351
// ===========================================================================

/// `SocketBackend(inBuf)` (postgres.c:351) — read one frontend message,
/// returning its type code and loading the body into `in_buf`. Returns
/// [`EOF`] on a lost connection.
///
/// `HOLD_CANCEL_INTERRUPTS`/`RESUME_CANCEL_INTERRUPTS` bracket the read in C;
/// the cancel-holdoff counter is owned by the interrupt machinery. We mirror
/// the read sequence (`pq_startmsgread`, `pq_getbyte`, validate, `pq_getmessage`)
/// and set the extended-query / skip-till-Sync flags exactly as C does.
fn SocketBackend(in_buf: &mut StringInfo<'_>) -> PgResult<i32> {
    // HOLD_CANCEL_INTERRUPTS();
    // (cancel-holdoff bracket is interrupt-machinery state; the read itself is
    // faithful.)
    pqcomm::pq_startmsgread()?;
    let qtype = pqcomm::pq_getbyte()?;

    if qtype == EOF {
        // frontend disconnected
        if xact::IsTransactionState() {
            return Err(ereport(ERROR)
                .errcode(types_error::error::ERRCODE_CONNECTION_FAILURE)
                .errmsg(
                    "unexpected EOF on client connection with an open transaction",
                )
                .into_error());
        } else {
            // Can't send DEBUG to client now; disconnecting, so don't restore
            // whereToSendOutput.
            globals::set_where_to_send_output(CommandDest::None);
            // ereport(DEBUG1, "unexpected EOF on client connection") — a DEBUG
            // line, dropped (logging-only, below the default threshold).
        }
        return Ok(qtype);
    }

    // Validate the message type code, choose a type-dependent length limit, and
    // set doing_extended_query_message / ignore_till_sync as early as possible.
    let maxmsglen: i32 = match qtype {
        x if x == pqmsg::QUERY => {
            globals::set_doing_extended_query_message(false);
            PQ_LARGE_MESSAGE_LIMIT
        }
        x if x == pqmsg::FUNCTION_CALL => {
            globals::set_doing_extended_query_message(false);
            PQ_LARGE_MESSAGE_LIMIT
        }
        x if x == pqmsg::TERMINATE => {
            globals::set_doing_extended_query_message(false);
            globals::set_ignore_till_sync(false);
            PQ_SMALL_MESSAGE_LIMIT
        }
        x if x == pqmsg::BIND || x == pqmsg::PARSE => {
            globals::set_doing_extended_query_message(true);
            PQ_LARGE_MESSAGE_LIMIT
        }
        x if x == pqmsg::CLOSE
            || x == pqmsg::DESCRIBE
            || x == pqmsg::EXECUTE
            || x == pqmsg::FLUSH =>
        {
            globals::set_doing_extended_query_message(true);
            PQ_SMALL_MESSAGE_LIMIT
        }
        x if x == pqmsg::SYNC => {
            // stop any active skip-till-Sync
            globals::set_ignore_till_sync(false);
            // mark not-extended, so a new error doesn't begin skip
            globals::set_doing_extended_query_message(false);
            PQ_SMALL_MESSAGE_LIMIT
        }
        x if x == pqmsg::COPY_DATA => {
            globals::set_doing_extended_query_message(false);
            PQ_LARGE_MESSAGE_LIMIT
        }
        x if x == pqmsg::COPY_DONE || x == pqmsg::COPY_FAIL => {
            globals::set_doing_extended_query_message(false);
            PQ_SMALL_MESSAGE_LIMIT
        }
        other => {
            // Garbage from the frontend: probably lost message-boundary sync.
            // Fatal, no good recovery.
            return Err(ereport(FATAL)
                .errcode(types_error::error::ERRCODE_PROTOCOL_VIOLATION)
                .errmsg(alloc::format!("invalid frontend message type {other}"))
                .into_error());
        }
    };

    // In protocol v3 every frontend message has a length word after the type
    // code, so the body can be read independently of the type.
    if pqcomm::pq_getmessage(in_buf, maxmsglen)? != 0 {
        return Ok(EOF); // suitable message already logged
    }
    // RESUME_CANCEL_INTERRUPTS();

    Ok(qtype)
}

// ===========================================================================
// ReadCommand — postgres.c:479
// ===========================================================================

/// `ReadCommand(inBuf)` (postgres.c:479) — read a command from the frontend (or
/// standard input), placing it in `in_buf` and returning the message type code.
/// [`EOF`] on end of file.
///
/// `InteractiveBackend` (the `DestRemote != whereToSendOutput` standalone path)
/// reads from stdin; the standalone single-user driver is `PostgresSingleUserMain`
/// (not this F0a unit). Here `whereToSendOutput` is always `DestRemote` (a real
/// connection), so we take the `SocketBackend` arm; the interactive arm panics
/// with a precise rationale if reached.
fn ReadCommand(in_buf: &mut StringInfo<'_>) -> PgResult<i32> {
    if globals::where_to_send_output() == CommandDest::Remote {
        SocketBackend(in_buf)
    } else {
        // InteractiveBackend: only reached from PostgresSingleUserMain (the
        // standalone backend, a separate unported entry). A real connection
        // backend always has whereToSendOutput == DestRemote here.
        panic!(
            "ReadCommand: InteractiveBackend (stdin) path is reached only from \
             PostgresSingleUserMain (standalone single-user backend), which is \
             a separate unported entry; a connection backend always has \
             whereToSendOutput == DestRemote"
        )
    }
}

// ===========================================================================
// forbidden_in_wal_sender — postgres.c:5031
// ===========================================================================

/// `forbidden_in_wal_sender(firstchar)` (postgres.c:5031) — throw if this is a
/// WAL sender process receiving a non-simple-query message.
fn forbidden_in_wal_sender(firstchar: i32) -> PgResult<()> {
    if backend_replication_walsender_seams::am_walsender::call() {
        if firstchar == pqmsg::FUNCTION_CALL {
            return Err(ereport(ERROR)
                .errcode(types_error::error::ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("fastpath function calls not supported in a replication connection")
                .into_error());
        } else {
            return Err(ereport(ERROR)
                .errcode(types_error::error::ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("extended query protocol not supported in a replication connection")
                .into_error());
        }
    }
    Ok(())
}

// ===========================================================================
// GUC reads used by the idle-state handling
// ===========================================================================

fn idle_in_transaction_session_timeout() -> i32 {
    backend_utils_misc_guc_tables::vars::IdleInTransactionSessionTimeout.read()
}

fn idle_session_timeout() -> i32 {
    backend_utils_misc_guc_tables::vars::IdleSessionTimeout.read()
}

fn transaction_timeout() -> i32 {
    backend_utils_misc_guc_tables::vars::TransactionTimeout.read()
}

// ===========================================================================
// The loop's per-message state carried across the sigsetjmp boundary
// ===========================================================================

/// The C `volatile` locals preserved across `longjmp`: the only ones that
/// survive an error are reset in the recovery block, so we carry them in a
/// struct threaded through the loop helpers.
struct LoopState {
    send_ready_for_query: bool,
    idle_in_transaction_timeout_enabled: bool,
    idle_session_timeout_enabled: bool,
}

// ===========================================================================
// Error-recovery block — the `if (sigsetjmp(...)) { ... }` arm, postgres.c:4393
// ===========================================================================

/// The outer error-recovery block (postgres.c:4393-4504), run when a command
/// (or the idle-state work / ReadCommand) returns `Err`. Mirrors the C catch
/// arm: forget the cancel request, disable timeouts, emit the error, abort the
/// transaction, flush error state, and arm skip-till-Sync if we were mid
/// extended-query.
///
/// `mcx` is the (about-to-be-reset) MessageContext; the C switches into it
/// before `FlushErrorState`.
fn error_recovery(mcx: Mcx<'_>, err: types_error::PgError, state: &mut LoopState) -> PgResult<()> {
    // error_context_stack = NULL — the ambient callback chain is retired
    // (backend-utils-error divergence #10); nothing to reset.

    // HOLD_INTERRUPTS() — the interrupt-holdoff bracket; the abort/cleanup
    // below cannot itself be interrupted in C. The holdoff counter lives in the
    // interrupt machinery; the cleanup here is straight-line.

    // Forget any pending QueryCancel and cancel active timeouts. Clearing the
    // statement/lock timeout indicators prevents a future plain cancel from
    // being misreported as a timeout.
    timeout::disable_all_timeouts(false)?; // first, to avoid a race
    backend_utils_init_small::globals::SetQueryCancelPending(false);
    state.idle_in_transaction_timeout_enabled = false;
    state.idle_session_timeout_enabled = false;

    // Not reading from the client anymore.
    globals::set_doing_command_read(false);

    // Make sure libpq is in a good state.
    pqcomm::pq_comm_reset();

    // Report the error to the client and/or server log.
    backend_utils_error::emit_error_report_for(&err);

    // valgrind_report_error_query(debug_query_string) — valgrind-only, skipped.

    // Make sure debug_query_string gets reset before we possibly clobber the
    // storage it points at.
    globals::set_debug_query_string(None);

    // Abort the current transaction in order to recover.
    xact_seams::abort_current_transaction::call()?;

    // if (am_walsender) WalSndErrorCleanup(); — reached only on a replication
    // connection (am_walsender); the WAL-sender error cleanup is a separate
    // unported path. The simple-Query target is never a WAL sender, so this is
    // not reached; mirror PG and panic if it is.
    if backend_replication_walsender_seams::am_walsender::call() {
        panic!(
            "PostgresMain error recovery: WalSndErrorCleanup is unported; only \
             reached on a replication (am_walsender) connection"
        );
    }

    backend_utils_mmgr_portalmem::PortalErrorCleanup()?;

    // Replication-slot release/cleanup on a top-level error: only relevant when
    // a slot is acquired (replication / logical-decoding sessions). Not reached
    // on the simple-Query target. The slot owner is a separate unit; faithfully
    // a no-op here when no slot is held (MyReplicationSlot == NULL and no
    // temporary slots), which is always the case on this path.
    //   if (MyReplicationSlot != NULL) ReplicationSlotRelease();
    //   ReplicationSlotCleanup(false);

    // jit_reset_after_error() — JIT is compiled out by default; no-op.

    // Now return to the MessageContext and clear ErrorContext for next time.
    // (We already operate in `mcx`.)
    backend_utils_error::FlushErrorState();
    let _ = mcx;

    // If we were handling an extended-query message, initiate skip till Sync.
    // This also suppresses ReadyForQuery until we get Sync.
    if globals::doing_extended_query_message() {
        globals::set_ignore_till_sync(true);
    }

    // We don't have a transaction command open anymore.
    globals::set_xact_started(false);

    // If the error occurred while reading a message, we may have lost track of
    // message boundaries; we can't safely read more.
    if pqcomm::pq_is_reading_msg() {
        return Err(ereport(FATAL)
            .errcode(types_error::error::ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("terminating connection because protocol synchronization was lost")
            .into_error());
    }

    // RESUME_INTERRUPTS();
    Ok(())
}

// ===========================================================================
// Idle-state handling — the `if (send_ready_for_query)` block, postgres.c:4565
// ===========================================================================

/// The idle-state work done before each blocking read when
/// `send_ready_for_query` is set (postgres.c:4565-4685): set the PS display /
/// activity state, arm idle timeouts, report changed GUCs, and send
/// `ReadyForQuery`.
fn ready_state(mcx: Mcx<'_>, state: &mut LoopState) -> PgResult<()> {
    let dest = globals::where_to_send_output();

    if xact_seams::is_aborted_transaction_block_state::call() {
        more_seams::set_ps_display::call("idle in transaction (aborted)");
        // pgstat_report_activity(STATE_IDLEINTRANSACTION_ABORTED, NULL): the
        // typed idle-in-transaction(-aborted) BackendState report seams are not
        // modeled (only idle/running variants exist). Monitoring-only; skipped.
        if idle_in_transaction_session_timeout() > 0
            && (idle_in_transaction_session_timeout() < transaction_timeout()
                || transaction_timeout() == 0)
        {
            state.idle_in_transaction_timeout_enabled = true;
            timeout::enable_timeout_after(
                TimeoutId::IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
                idle_in_transaction_session_timeout(),
            )?;
        }
    } else if xact_seams::is_transaction_or_transaction_block::call() {
        more_seams::set_ps_display::call("idle in transaction");
        // pgstat_report_activity(STATE_IDLEINTRANSACTION, NULL): see above.
        if idle_in_transaction_session_timeout() > 0
            && (idle_in_transaction_session_timeout() < transaction_timeout()
                || transaction_timeout() == 0)
        {
            state.idle_in_transaction_timeout_enabled = true;
            timeout::enable_timeout_after(
                TimeoutId::IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
                idle_in_transaction_session_timeout(),
            )?;
        }
    } else {
        // Process incoming notifies (including self-notifies) so the client
        // sees them before ReadyForQuery. notifyInterruptPending /
        // ProcessNotifyInterrupt is the async-notify path (LISTEN/NOTIFY); not
        // reached on the simple-SELECT target (no notifies pending). The
        // notify owner is a separate unit; faithfully nothing to flush here.
        //   if (notifyInterruptPending) ProcessNotifyInterrupt(false);

        // Check if we need to report stats; arm IDLE_STATS_UPDATE_TIMEOUT if
        // pgstat_report_stat() asks us to retry later.
        let stats_timeout =
            backend_utils_activity_pgstat_seams::pgstat_report_stat::call(false)?;
        if stats_timeout > 0 {
            if !timeout::get_timeout_active(TimeoutId::IDLE_STATS_UPDATE_TIMEOUT) {
                timeout::enable_timeout_after(
                    TimeoutId::IDLE_STATS_UPDATE_TIMEOUT,
                    stats_timeout as i32,
                )?;
            }
        } else if timeout::get_timeout_active(TimeoutId::IDLE_STATS_UPDATE_TIMEOUT) {
            timeout::disable_timeout(TimeoutId::IDLE_STATS_UPDATE_TIMEOUT, false);
        }

        more_seams::set_ps_display::call("idle");
        status_seams::pgstat_report_activity_idle::call();

        if idle_session_timeout() > 0 {
            state.idle_session_timeout_enabled = true;
            timeout::enable_timeout_after(
                TimeoutId::IDLE_SESSION_TIMEOUT,
                idle_session_timeout(),
            )?;
        }
    }

    // Report any recently-changed GUC options.
    backend_utils_misc_guc::report::report_changed_guc_options();

    // The first-ready connection-timing LOG line (conn_timing.ready_for_use) is
    // a connection-setup-duration diagnostic; the conn_timing accounting owner
    // is a separate unit. Not threaded here (logging-only).

    dest_seams::ready_for_query::call(mcx, dest)?;
    state.send_ready_for_query = false;

    Ok(())
}

// ===========================================================================
// Per-message dispatch — the `switch (firstchar)` block, postgres.c:4748
// ===========================================================================

/// Process one client message (postgres.c:4748-5020). `firstchar` is the type
/// code; `input_message` holds the body (cursor at the start). Returns the
/// updated `send_ready_for_query` decision; `proc_exit` does not return.
fn dispatch_message<'mcx>(
    mcx: Mcx<'mcx>,
    firstchar: i32,
    input_message: &mut StringInfo<'mcx>,
    state: &mut LoopState,
) -> PgResult<()> {
    match firstchar {
        x if x == pqmsg::QUERY => {
            // Set statement_timestamp().
            xact_seams::set_current_statement_start_timestamp::call();

            // Copy the query string into the MessageContext (`mcx`) so it has
            // the `'mcx` lifetime exec_simple_query needs (it points into
            // MessageContext in C too, outliving the portal). Copy before
            // pq_getmsgend so the mutable message borrow is released.
            let qstr: &'mcx str = {
                let query_string = pqformat::pq_getmsgstring(mcx, input_message)?;
                leak_str_in(mcx, query_string.as_bytes())?
            };
            pqformat::pq_getmsgend(input_message)?;

            if backend_replication_walsender_seams::am_walsender::call() {
                // if (!exec_replication_command(query_string))
                //     exec_simple_query(query_string);
                // exec_replication_command is the WAL-sender replication-command
                // path; not reached on a non-replication connection. Mirror PG
                // and panic if a WAL sender ever drives this loop.
                panic!(
                    "PostgresMain 'Q': exec_replication_command (WAL-sender \
                     replication command path) is unported; only reached on an \
                     am_walsender connection"
                );
            } else {
                crate::simple_query::exec_simple_query(mcx, qstr)?;
            }

            // valgrind_report_error_query — valgrind-only, skipped.
            state.send_ready_for_query = true;
        }

        x if x == pqmsg::PARSE => {
            forbidden_in_wal_sender(firstchar)?;
            // exec_parse_message (extended-query protocol, F2 family) does not
            // exist yet — the whole prepared-statement / plancache extended path
            // is unported. The simple-Query target never sends Parse.
            panic!(
                "PostgresMain 'P' (Parse): exec_parse_message is the unported \
                 extended-query (F2) protocol path (plancache-gated); not \
                 reached on the simple-Query target"
            );
        }

        x if x == pqmsg::BIND => {
            forbidden_in_wal_sender(firstchar)?;
            // exec_bind_message (extended-query protocol, F2) — unported.
            panic!(
                "PostgresMain 'B' (Bind): exec_bind_message is the unported \
                 extended-query (F2) protocol path (plancache-gated); not \
                 reached on the simple-Query target"
            );
        }

        x if x == pqmsg::EXECUTE => {
            forbidden_in_wal_sender(firstchar)?;
            // exec_execute_message (extended-query protocol, F2) — unported.
            panic!(
                "PostgresMain 'E' (Execute): exec_execute_message is the \
                 unported extended-query (F2) protocol path (plancache-gated); \
                 not reached on the simple-Query target"
            );
        }

        x if x == pqmsg::FUNCTION_CALL => {
            forbidden_in_wal_sender(firstchar)?;

            // Set statement_timestamp().
            xact_seams::set_current_statement_start_timestamp::call();

            // Report query to monitoring facilities.
            // pgstat_report_activity(STATE_FASTPATH, NULL): the STATE_FASTPATH
            // BackendState report seam is not modeled (monitoring-only); the PS
            // display is set below.
            more_seams::set_ps_display::call("<FASTPATH>");

            // Start an xact for this function invocation.
            crate::simple_query::start_xact_command()?;

            // Note: we may be inside an aborted transaction here;
            // HandleFunctionRequest checks for that after reading the message.

            // (MemoryContextSwitchTo(MessageContext) — already in `mcx`.)
            fastpath::handle_function_request(mcx, input_message)?;

            // Commit the function-invocation transaction.
            crate::simple_query::finish_xact_command()?;

            state.send_ready_for_query = true;
        }

        x if x == pqmsg::CLOSE => {
            forbidden_in_wal_sender(firstchar)?;

            let close_type = pqformat::pq_getmsgbyte(input_message)?;
            let close_target = pqformat::pq_getmsgstring(mcx, input_message)?;
            let close_target = String::from_utf8_lossy(close_target.as_bytes()).into_owned();
            pqformat::pq_getmsgend(input_message)?;

            match close_type as u8 {
                b'S' => {
                    if !close_target.is_empty() {
                        // DropPreparedStatement: the prepared-statement store is
                        // the extended-query (F2) plancache path. A named
                        // prepared statement can only exist after a Parse, which
                        // is unported, so this is never reached on this target.
                        panic!(
                            "PostgresMain Close 'S': DropPreparedStatement for a \
                             named prepared statement requires the unported \
                             extended-query (Parse) path to have created one"
                        );
                    } else {
                        // special-case the unnamed statement
                        crate::simple_query::drop_unnamed_stmt()?;
                    }
                }
                b'P' => {
                    if let Some(portal) =
                        backend_utils_mmgr_portalmem_seams::get_portal_by_name::call(
                            &close_target,
                        )?
                    {
                        backend_utils_mmgr_portalmem_seams::portal_drop::call(&portal, false)?;
                    }
                }
                other => {
                    return Err(ereport(ERROR)
                        .errcode(types_error::error::ERRCODE_PROTOCOL_VIOLATION)
                        .errmsg(alloc::format!("invalid CLOSE message subtype {other}"))
                        .into_error());
                }
            }

            if globals::where_to_send_output() == CommandDest::Remote {
                pqformat::pq_putemptymessage(pqmsg::CLOSE_COMPLETE)?;
            }
        }

        x if x == pqmsg::DESCRIBE => {
            forbidden_in_wal_sender(firstchar)?;
            // Set statement_timestamp() (needed for xact).
            xact_seams::set_current_statement_start_timestamp::call();
            // exec_describe_statement_message / exec_describe_portal_message are
            // the unported extended-query (F2) describe path.
            panic!(
                "PostgresMain 'D' (Describe): exec_describe_*_message is the \
                 unported extended-query (F2) protocol path; not reached on the \
                 simple-Query target"
            );
        }

        x if x == pqmsg::FLUSH => {
            pqformat::pq_getmsgend(input_message)?;
            if globals::where_to_send_output() == CommandDest::Remote {
                pqcomm::pq_flush()?;
            }
        }

        x if x == pqmsg::SYNC => {
            pqformat::pq_getmsgend(input_message)?;
            // If pipelining was used we may be in an implicit transaction block;
            // close it before finish_xact_command.
            xact::EndImplicitTransactionBlock();
            crate::simple_query::finish_xact_command()?;
            state.send_ready_for_query = true;
        }

        // PqMsg_Terminate: the frontend is closing the socket. EOF: unexpected
        // loss of the connection. Either way, normal shutdown.
        x if x == EOF || x == pqmsg::TERMINATE => {
            // pgStatSessionEndCause = DISCONNECT_CLIENT_EOF (on EOF) — the
            // session-end-cause stat is owned by pgstat; not threaded here.

            // Reset whereToSendOutput so ereport won't try to send more to the
            // client.
            if globals::where_to_send_output() == CommandDest::Remote {
                globals::set_where_to_send_output(CommandDest::None);
            }

            // NOTE: anything to do at shutdown belongs in an on_proc_exit /
            // on_shmem_exit callback, not here.
            backend_storage_ipc_ipc_seams::proc_exit::call(0);
        }

        x if x == pqmsg::COPY_DATA || x == pqmsg::COPY_DONE || x == pqmsg::COPY_FAIL => {
            // Accept but ignore these per protocol spec (probably a failed COPY
            // whose frontend is still sending data).
        }

        other => {
            return Err(ereport(FATAL)
                .errcode(types_error::error::ERRCODE_PROTOCOL_VIOLATION)
                .errmsg(alloc::format!("invalid frontend message type {other}"))
                .into_error());
        }
    }

    Ok(())
}

// ===========================================================================
// PostgresMain — postgres.c:4184
// ===========================================================================

/// `PostgresMain(dbname, username)` (postgres.c:4184) — the regular backend's
/// post-auth main loop. Never returns (exits through `proc_exit`).
///
/// The per-backend signal-handler installation (postgres.c:4213-4252), the
/// cancel-key generation + `BackendKeyData` send (4264-4339), the welcome
/// banner, the `row_description_context` (extended-query RowDescription buffer),
/// and `EventTriggerOnLogin` are setup steps whose owners are separate units;
/// where unported they are skipped-with-note (signal install, banner, login
/// triggers) — none is exercised by the simple-Query end-to-end path. `BaseInit`
/// + `InitPostgres` run here via their seams (the catalog/shmem connection
/// setup C does in this vicinity).
pub fn PostgresMain(dbname: Option<&str>, username: Option<&str>) -> ! {
    match postgres_main_inner(dbname, username) {
        Ok(()) => {
            // The loop only exits via proc_exit (diverging); reaching here means
            // the loop returned Ok, which it never should.
            unreachable!("PostgresMain loop returned without proc_exit")
        }
        Err(err) => {
            // A FATAL escaped the loop's own recovery (e.g. lost protocol sync,
            // or a setup-phase FATAL). Report it and exit, mirroring the C where
            // an unrecoverable FATAL ends the process.
            backend_utils_error::emit_error_report_for(&err);
            backend_storage_ipc_ipc_seams::proc_exit::call(1)
        }
    }
}

/// The body of [`PostgresMain`], returning `PgResult` so a setup-phase or
/// escaped-FATAL error is reported by the `!`-returning wrapper.
fn postgres_main_inner(dbname: Option<&str>, username: Option<&str>) -> PgResult<()> {
    // Assert(dbname != NULL); Assert(username != NULL);
    let dbname = dbname.expect("PostgresMain requires a non-NULL dbname");
    let _username = username; // InitPostgres seam takes only dbname (role resolved internally).

    // --- Per-backend signal-handler setup (postgres.c:4213-4252) ---
    //
    // The postmaster blocked all signals before forking, so the handlers are
    // installed race-free here. Installing them faithfully requires the
    // per-backend handler set (SignalHandlerForConfigReload, StatementCancelHandler,
    // die, quickdie, procsignal_sigusr1_handler, FloatExceptionHandler) wired
    // through pqsignal at the OS level. The handler bodies are owned across
    // several units (postmaster-interrupt, this crate's interrupt module,
    // procsignal); the install wiring is process-environment setup not exercised
    // by the in-process simple-Query path. Skipped-with-note here.
    //   if (am_walsender) WalSndSignals(); else { pqsignal(...); InitializeTimeouts(); ... }
    //
    // InitializeTimeouts() (postgres.c:4232) IS run here: it establishes the
    // timeout module's per-backend slot table (and the SIGALRM handler), which
    // InitPostgres relies on when it RegisterTimeout()s the deadlock /
    // statement / lock timeouts. The rest of the pqsignal wiring above is
    // process-environment setup not exercised by the in-process simple-Query
    // path and stays skipped-with-note.
    backend_utils_misc_timeout_seams::initialize_timeouts::call();

    // --- Early initialization (postgres.c:4255) ---
    // BaseInit(): open the per-backend low-level subsystems (smgr, buffers, ...).
    backend_utils_init_miscinit_seams::base_init::call()?;

    // sigprocmask(SIG_SETMASK, &UnBlockSig, NULL): allow SIGINT etc during the
    // initial transaction. The signal mask is OS state set up by the launcher;
    // not re-applied here.

    // Generate a random cancel key + advertise it (postgres.c:4264). The
    // MyCancelKey storage + advertisement is owned by the proc/cancel-key unit;
    // not threaded here. The BackendKeyData send (below) is likewise skipped.

    // --- General initialization (postgres.c:4289) ---
    // InitPostgres(dbname, ...): connect to the database, load the relcache /
    // catcache, set MyDatabaseId, run session_preload_libraries.
    backend_utils_init_miscinit_seams::init_postgres::call(dbname)?;

    // if (PostmasterContext) { MemoryContextDelete; PostmasterContext = NULL; }
    // — the postmaster-handoff context recycle; that context is owned by the
    // launcher and not modeled as a deletable here.

    // SetProcessingMode(NormalProcessing): now fully connected.
    backend_utils_init_miscinit::SetProcessingMode(
        types_core::init::ProcessingMode::NormalProcessing,
    );

    // BeginReportingGUCOptions(): report GUCs to the client if appropriate.
    backend_utils_misc_guc::report::begin_reporting_guc_options();

    // if (IsUnderPostmaster && Log_disconnections) on_proc_exit(log_disconnections)
    // — the disconnect-log callback; registration is process-exit plumbing,
    // skipped (the body, logging::log_disconnections, is landed).

    // pgstat_report_connect(MyDatabaseId): the connection-establishment stat.
    // The pgstat_report_connect entry is a separate pgstat unit (no seam);
    // skipped-with-note (cumulative-stats only).

    // if (am_walsender) InitWalSender(); — replication-only setup; the
    // simple-Query target is not a WAL sender. Not reached.

    // Send BackendKeyData to the frontend (postgres.c:4328) — the cancel-key
    // advertisement; the cancel-key storage owner is a separate unit. Skipped
    // (the client tolerates its absence for query execution; not exercised by
    // the in-process simple-Query path).

    // Welcome banner for the standalone (DestDebug) case — single-user only.

    // --- The main-loop memory context (postgres.c:4351) ---
    // MessageContext is reset once per loop iteration; here a child of the
    // backend top context, created fresh each iteration.
    let backend_top = MemoryContext::new("MessageContext");

    // row_description_context + row_description_buf (extended-query
    // RowDescription reuse buffer, postgres.c:4361) — used only by
    // exec_describe_statement_message (unported F2). Not created here.

    // EventTriggerOnLogin(): fire login event triggers. The event-trigger
    // engine is a separate unported unit; login triggers are an opt-in feature
    // not present on a fresh cluster, so this is a no-op on the target. Skipped
    // with note (EventTriggerOnLogin is unported).

    // --- The processing loop (postgres.c:4516) ---
    let mut state = LoopState {
        send_ready_for_query: true,
        idle_in_transaction_timeout_enabled: false,
        idle_session_timeout_enabled: false,
    };

    // if (!ignore_till_sync) send_ready_for_query = true; (initially / after error)
    if !globals::ignore_till_sync() {
        state.send_ready_for_query = true;
    }

    loop {
        // Run one full iteration; on Err, run the recovery block and continue.
        // This is the `sigsetjmp` outer handler expressed over PgResult.
        let message_context = backend_top.new_child("MessageContext");
        let iter = run_one_iteration(message_context.mcx(), &mut state);
        if let Err(err) = iter {
            // The C switches into MessageContext before FlushErrorState; we pass
            // the (about-to-be-reset) per-iteration context.
            error_recovery(message_context.mcx(), err, &mut state)?;
        }
        // MemoryContextReset(MessageContext): the per-iteration arena is
        // reclaimed by dropping the child context (all `'mcx` borrows ended).
        drop(message_context);
    }
}

/// One pass of the loop body up to (not including) the recovery block: the
/// idle-state work, the blocking read, the post-read interrupt checks, and the
/// message dispatch (postgres.c:4516-5021).
fn run_one_iteration<'mcx>(mcx: Mcx<'mcx>, state: &mut LoopState) -> PgResult<()> {
    // At top of loop, reset the extended-query flag so an "idle"-state error
    // doesn't provoke skip.
    globals::set_doing_extended_query_message(false);

    // (MemoryContextReset(MessageContext) — handled by the caller's per-iteration
    // child context.)
    let mut input_message = StringInfo::new_in(mcx);

    // Consider releasing the catalog snapshot so it doesn't pin global xmin
    // while we wait for the client.
    backend_utils_time_snapmgr::InvalidateCatalogSnapshotConditionally()?;

    // (1) If idle, tell the frontend we're ready for a new query.
    if state.send_ready_for_query {
        ready_state(mcx, state)?;
    }

    // (2) Allow async signals to run immediately while waiting for input.
    globals::set_doing_command_read(true);

    // (3) Read a command (blocks here).
    let firstchar = ReadCommand(&mut input_message)?;

    // (4) Turn off idle-in-transaction / idle-session timeouts if active. Done
    // before (5) so any last-moment timeout is detected in (5).
    if state.idle_in_transaction_timeout_enabled {
        timeout::disable_timeout(TimeoutId::IDLE_IN_TRANSACTION_SESSION_TIMEOUT, false);
        state.idle_in_transaction_timeout_enabled = false;
    }
    if state.idle_session_timeout_enabled {
        timeout::disable_timeout(TimeoutId::IDLE_SESSION_TIMEOUT, false);
        state.idle_session_timeout_enabled = false;
    }

    // (5) Disable async signal conditions again. Check for interrupts before
    // resetting DoingCommandRead so an idle-arrived cancel is reset (a no-op
    // when no query is in progress).
    crate::interrupt::check_for_interrupts()?;
    globals::set_doing_command_read(false);

    // (6) Other interesting events that happened while we slept.
    if pm_interrupt::ConfigReloadPending() {
        pm_interrupt::SetConfigReloadPending(false);
        backend_utils_misc_guc_seams::process_config_file_sighup::call()?;
    }

    // (7) Process the command, unless skipping till Sync.
    if globals::ignore_till_sync() && firstchar != EOF {
        return Ok(());
    }

    dispatch_message(mcx, firstchar, &mut input_message, state)
}

// ===========================================================================
// Helpers
// ===========================================================================

/// Copy `bytes` (a valid UTF-8 query string from the message buffer) into `mcx`
/// and leak it to a `&'mcx str`, mirroring the C query string that lives in
/// `MessageContext` for the message's lifetime.
fn leak_str_in<'mcx>(mcx: Mcx<'mcx>, bytes: &[u8]) -> PgResult<&'mcx str> {
    let mut v: mcx::PgVec<'mcx, u8> = mcx::PgVec::new_in(mcx);
    v.try_reserve(bytes.len()).map_err(|_| mcx.oom(bytes.len()))?;
    v.extend_from_slice(bytes);
    let leaked: &'mcx [u8] = allocator_api2::boxed::Box::leak(v.into_boxed_slice());
    core::str::from_utf8(leaked).map_err(|_| {
        ereport(ERROR)
            .errcode(types_error::error::ERRCODE_CHARACTER_NOT_IN_REPERTOIRE)
            .errmsg("invalid byte sequence in query string")
            .into_error()
    })
}
