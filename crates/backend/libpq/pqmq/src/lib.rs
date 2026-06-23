//! `libpq/pqmq.c` — glue routines that let a background (parallel) worker
//! redirect its libpq protocol output into a `shm_mq` shared-memory message
//! queue, so the leader can receive the worker's ErrorResponse / NoticeResponse
//! / NotificationResponse messages and re-raise them.
//!
//! This installs an alternate [`PQcommMethods`](::pqcomm::PQcommMethods)
//! table (`PqCommMqMethods`) into pqcomm's pluggable comm-method slot, plus the
//! `pq_redirect_to_shm_mq` / `pq_set_parallel_leader` orchestration seams that
//! `access/transam/parallel.c` calls from `ParallelWorkerMain`, and the
//! `pq_parse_errornotice` parser the leader uses to rebuild the worker's error.
//!
//! ## Handle model
//!
//! The C statics hold a `shm_mq_handle *` (`pq_mq_handle`) and a
//! `dsm_segment *`. In this workspace the backend-private `shm_mq_handle` is an
//! owned `PgBox<ShmMqHandle>` parked in the `shm-mq` owner's process-global
//! registry and named across the seam by a small id
//! ([`ShmMqAttachHandle`](::execparallel::ShmMqAttachHandle), same id the
//! parallel orchestration carries as
//! [`ShmMqHandleHandle`](::types_parallel::ShmMqHandleHandle)). So `pq_mq_handle`
//! becomes a stored registry id, and `mq_putmessage` / `mq_comm_reset` route
//! their work through the `shm-mq` registry seams rather than touching a raw
//! pointer. The `dsm_segment *` is named by
//! [`DsmSegmentHandle`](::types_parallel::DsmSegmentHandle), whose `.0` is the
//! real [`DsmSegmentId`](::dsm_core::dsm::DsmSegmentId) word.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use std::cell::Cell;

use pqcomm::{
    set_pq_comm_methods, PQcommMethods, EOF, PQ_COMM_SOCKET_METHODS,
};
use ::dsm_core::dsm::{on_dsm_detach, DsmSegmentId};
use utils_error::{PgError, ThrowErrorData};
use types_core::{pid_t, ProcNumber, INVALID_PROC_NUMBER};
use ::datum::Datum;
use ::types_dest::CommandDest;
use types_error::{
    ErrorLevel, ErrorLocation, PgResult, SqlState, ERRCODE_PROTOCOL_VIOLATION, PANIC,
    PG_DIAG_COLUMN_NAME, PG_DIAG_CONSTRAINT_NAME, PG_DIAG_CONTEXT, PG_DIAG_DATATYPE_NAME,
    PG_DIAG_INTERNAL_POSITION, PG_DIAG_INTERNAL_QUERY, PG_DIAG_MESSAGE_DETAIL, PG_DIAG_MESSAGE_HINT,
    PG_DIAG_MESSAGE_PRIMARY, PG_DIAG_SCHEMA_NAME, PG_DIAG_SEVERITY, PG_DIAG_SEVERITY_NONLOCALIZED,
    PG_DIAG_SOURCE_FILE, PG_DIAG_SOURCE_FUNCTION, PG_DIAG_SOURCE_LINE, PG_DIAG_SQLSTATE,
    PG_DIAG_STATEMENT_POSITION, PG_DIAG_TABLE_NAME, DEBUG1, ERROR, FATAL, INFO, LOG, NOTICE,
    WARNING,
};
use ::execparallel::ShmMqAttachHandle;
use types_parallel::{DsmSegmentHandle, ShmMqHandleHandle};
use ::types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET};
use ::types_storage::ProcSignalReason;

use parallel_rt_seams as rt;
use latch_seams as latch;
use procsignal_seams as procsignal;
use shm_mq_seams as shmmq;
use postgres_seams as tcop;

/// `PG_PROTOCOL_LATEST` (`pg_protocol.h`) = `PG_PROTOCOL(3, 2)` =
/// `(3 << 16) | 2`. Defined locally to avoid depending on the much
/// higher-level `backend-tcop-backend-startup` crate.
const PG_PROTOCOL_LATEST: u32 = (3u32 << 16) | 2u32;

// ---------------------------------------------------------------------------
// File statics (pqmq.c). Per-backend; thread_local in this build.
// ---------------------------------------------------------------------------

thread_local! {
    /// `static shm_mq_handle *pq_mq_handle` — the queue this backend's protocol
    /// output is redirected into, or `None` for the C NULL.
    static PQ_MQ_HANDLE: Cell<Option<ShmMqAttachHandle>> = const { Cell::new(None) };

    /// `static bool pq_mq_busy` — re-entrancy guard for `mq_putmessage`.
    static PQ_MQ_BUSY: Cell<bool> = const { Cell::new(false) };

    /// `static pid_t pq_mq_parallel_leader_pid` — leader to signal after a
    /// successful send (0 = not set).
    static PQ_MQ_PARALLEL_LEADER_PID: Cell<pid_t> = const { Cell::new(0) };

    /// `static ProcNumber pq_mq_parallel_leader_proc_number`.
    static PQ_MQ_PARALLEL_LEADER_PROC_NUMBER: Cell<ProcNumber> =
        const { Cell::new(INVALID_PROC_NUMBER) };
}

// ---------------------------------------------------------------------------
// The shm_mq-backed PQcommMethods table.
// ---------------------------------------------------------------------------

/// `static const PQcommMethods PqCommMqMethods` (pqmq.c).
pub static PQ_COMM_MQ_METHODS: PQcommMethods = PQcommMethods {
    comm_reset: mq_comm_reset,
    flush: mq_flush,
    flush_if_writable: mq_flush_if_writable,
    is_send_pending: mq_is_send_pending,
    putmessage: mq_putmessage,
    putmessage_noblock: mq_putmessage_noblock,
};

// ---------------------------------------------------------------------------
// Orchestration entry points (called from ParallelWorkerMain via the rt seams).
// ---------------------------------------------------------------------------

/// `pq_redirect_to_shm_mq(dsm_segment *seg, shm_mq_handle *mqh)` — arrange to
/// send protocol messages to the shared-memory message queue, rather than to
/// the FE/BE socket.
pub fn pq_redirect_to_shm_mq(seg: DsmSegmentHandle, mqh: ShmMqHandleHandle) -> PgResult<()> {
    // PqCommMethods = &PqCommMqMethods;
    set_pq_comm_methods(&PQ_COMM_MQ_METHODS);
    // pq_mq_handle = mqh;  (the ShmMqHandleHandle and ShmMqAttachHandle name the
    // same registry id.)
    PQ_MQ_HANDLE.with(|h| h.set(Some(ShmMqAttachHandle(mqh.0))));
    // whereToSendOutput = DestRemote;
    ::utils_error::config::set_where_to_send_output(CommandDest::Remote);
    // FrontendProtocol = PG_PROTOCOL_LATEST;
    init_small::globals::SetFrontendProtocol(PG_PROTOCOL_LATEST);
    // on_dsm_detach(seg, pq_cleanup_redirect_to_shm_mq, (Datum) 0);
    // C `MemoryContextAlloc`s the callback record in `TopMemoryContext`.
    on_dsm_detach(
        DsmSegmentId::from_u64(seg.0 as u64),
        pq_cleanup_redirect_to_shm_mq,
        Datum::null(),
        mcxt_seams::top_memory_context::call(),
    )?;
    Ok(())
}

/// `pq_cleanup_redirect_to_shm_mq(dsm_segment *seg, Datum arg)` — undo the
/// redirection set up by `pq_redirect_to_shm_mq`. Registered as an
/// `on_dsm_detach` callback so the redirection is torn down when the segment
/// goes away.
fn pq_cleanup_redirect_to_shm_mq(_seg: DsmSegmentId, _arg: Datum) -> PgResult<()> {
    // PqCommMethods = &PqCommSocketMethods;
    set_pq_comm_methods(&PQ_COMM_SOCKET_METHODS);
    // pq_mq_handle = NULL;
    PQ_MQ_HANDLE.with(|h| h.set(None));
    Ok(())
}

/// `pq_set_parallel_leader(pid_t pid, ProcNumber procNumber)` — set the
/// `pq_mq_parallel_leader_pid` and `pq_mq_parallel_leader_proc_number` so that
/// we can signal the leader after every message we send through the queue.
pub fn pq_set_parallel_leader(pid: pid_t, procno: ProcNumber) -> PgResult<()> {
    debug_assert!(PQ_MQ_HANDLE.with(Cell::get).is_some());

    PQ_MQ_PARALLEL_LEADER_PID.with(|c| c.set(pid));
    PQ_MQ_PARALLEL_LEADER_PROC_NUMBER.with(|c| c.set(procno));
    Ok(())
}

// ---------------------------------------------------------------------------
// The PQcommMethods callbacks for the shm_mq destination.
// ---------------------------------------------------------------------------

/// `mq_comm_reset()` — there's nothing to reset here (no per-message scratch),
/// so this is a no-op, matching C.
fn mq_comm_reset() {
    // Nothing to do.
}

/// `mq_flush()` — there's nothing to flush, so this just succeeds.
fn mq_flush() -> PgResult<i32> {
    Ok(0)
}

/// `mq_flush_if_writable()` — there's nothing to flush, so this just succeeds.
fn mq_flush_if_writable() -> PgResult<i32> {
    Ok(0)
}

/// `mq_is_send_pending()` — there's never anything pending.
fn mq_is_send_pending() -> bool {
    false
}

/// `mq_putmessage(char msgtype, const char *s, size_t len)` — transmit a libpq
/// protocol message to the shared-memory message queue selected via
/// `pq_redirect_to_shm_mq`. We don't include a length word in the message
/// written to the queue, because the queue itself frames messages.
fn mq_putmessage(msgtype: u8, s: &[u8]) -> PgResult<i32> {
    // If we're sending a message, and we have to wait because the queue is
    // full, and then we get interrupted, and that interrupt results in trying
    // to send another message, we respond by detaching the queue. There's no
    // way to return to the original context, but even if there were, just
    // queueing the message would amount to indefinitely postponing the
    // response to the interrupt. So we do this instead.
    if PQ_MQ_BUSY.with(Cell::get) {
        if let Some(h) = PQ_MQ_HANDLE.with(Cell::get) {
            shmmq::shm_mq_detach::call(h);
        }
        PQ_MQ_HANDLE.with(|h| h.set(None));
        return Ok(EOF);
    }

    // If the message queue is already gone, just ignore the message. This
    // doesn't necessarily indicate a problem; for example, DEBUG messages can
    // be generated late in the shutdown sequence, after all DSMs have already
    // been detached.
    let handle = match PQ_MQ_HANDLE.with(Cell::get) {
        Some(h) => h,
        None => return Ok(0),
    };

    PQ_MQ_BUSY.with(|c| c.set(true));

    // C builds a 2-element iovec (`{&msgtype, 1}`, `{s, len}`) and calls
    // `shm_mq_sendv`. The registry seam exposes the single-buffer `shm_mq_send`,
    // so we gather into one buffer here; the bytes written to the queue are
    // identical (the gather is purely an avoid-a-copy optimization in C).
    let mut payload = alloc::vec::Vec::with_capacity(1 + s.len());
    payload.push(msgtype);
    payload.extend_from_slice(s);

    let result = loop {
        // Immediately notify the receiver by passing force_flush as true so
        // that the shared memory value is updated before we send the parallel
        // message signal right after this.
        //
        // C: `shm_mq_sendv(pq_mq_handle, iov, 2, true, true)` — the `nowait`
        // argument is TRUE. This is load-bearing: when a worker's protocol
        // message (e.g. a long ErrorResponse) is larger than the error queue's
        // ring, a blocking (`nowait = false`) send would park inside
        // `shm_mq_send_bytes` waiting for the leader to drain, and would never
        // reach the `SendProcSignal(PROCSIG_PARALLEL_MESSAGE)` below — so the
        // leader never sets `ParallelMessagePending`, never runs
        // `ProcessParallelMessages`, and never drains the queue → deadlock
        // between worker (blocked sending) and leader (blocked in
        // `gather_readnext`'s WaitLatch). With `nowait = true` the partial send
        // returns `WOULD_BLOCK`, we signal the leader (waking it to drain),
        // wait on our own latch, and retry — exactly as C does.
        let result =
            shmmq::shm_mq_send::call(handle, payload.clone(), true, true)?;

        let leader_pid = PQ_MQ_PARALLEL_LEADER_PID.with(Cell::get);
        if leader_pid != 0 {
            procsignal::send_proc_signal::call(
                leader_pid,
                ProcSignalReason::PROCSIG_PARALLEL_MESSAGE,
                PQ_MQ_PARALLEL_LEADER_PROC_NUMBER.with(Cell::get),
            );
        }

        if result != ::types_parallel::ShmMqResult::WouldBlock {
            break result;
        }

        latch::wait_latch_my_latch::call(
            WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
            0,
            types_pgstat::wait_event::WAIT_EVENT_MESSAGE_QUEUE_PUT_MESSAGE,
        )?;
        latch::reset_latch_my_latch::call();
        tcop::check_for_interrupts::call()?;
    };

    PQ_MQ_BUSY.with(|c| c.set(false));

    if result != ::types_parallel::ShmMqResult::Success {
        return Ok(EOF);
    }
    Ok(0)
}

/// `mq_putmessage_noblock(char msgtype, const char *s, size_t len)` — like
/// `mq_putmessage`, but never blocks.
///
/// Since this is only used for asynchronous protocol messages (e.g.
/// NotifyResponse, NoticeResponse), and could only block if the queue were
/// full and the leader weren't reading, and since it's not catastrophic to
/// drop such a message in that situation, we just elog(ERROR) — we should
/// never reach here, because the relevant code paths always send synchronously.
fn mq_putmessage_noblock(_msgtype: u8, _s: &[u8]) -> PgResult<()> {
    // C: elog(ERROR, "not currently supported");
    Err(PgError::error("not currently supported"))
}

// ---------------------------------------------------------------------------
// pq_parse_errornotice — rebuild the worker's ErrorData from the wire bytes.
// ---------------------------------------------------------------------------

/// The fields of the C `ErrorData` we reconstruct. Only `elevel` and `context`
/// cross the seams (the parallel-apply leader and the parallel-query leader use
/// only those), but we parse every field exactly as C does so a malformed
/// message raises the same protocol-violation errors.
#[derive(Default)]
struct ErrorData {
    elevel: i32,
    sqlerrcode: i32,
    message: Option<String>,
    detail: Option<String>,
    hint: Option<String>,
    context: Option<String>,
    cursorpos: i32,
    internalpos: i32,
    internalquery: Option<String>,
    schema_name: Option<String>,
    table_name: Option<String>,
    column_name: Option<String>,
    datatype_name: Option<String>,
    constraint_name: Option<String>,
    filename: Option<String>,
    lineno: i32,
    funcname: Option<String>,
}

/// A read cursor over a libpq message body, mirroring the `pq_getmsg*` family
/// (lib/pqformat.c) used by `pq_parse_errornotice`. The seam carries only the
/// raw bytes (no `StringInfo` / memory context), so the readers are reproduced
/// here with identical bounds checks and protocol-violation `ereport`s.
struct MsgCursor<'a> {
    data: &'a [u8],
    cursor: usize,
}

impl<'a> MsgCursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        MsgCursor { data, cursor: 0 }
    }

    /// `pq_getmsgbyte` — get a raw byte from the message buffer.
    fn getmsgbyte(&mut self) -> PgResult<u8> {
        if self.cursor >= self.data.len() {
            return Err(protocol_violation("no data left in message"));
        }
        let b = self.data[self.cursor];
        self.cursor += 1;
        Ok(b)
    }

    /// `pq_copymsgbytes` — copy `n` bytes out of the message buffer, raising the
    /// "insufficient data" protocol violation if fewer than `n` remain.
    fn copymsgbytes(&mut self, n: usize) -> PgResult<&'a [u8]> {
        if self.data.len() - self.cursor < n {
            return Err(protocol_violation("insufficient data left in message"));
        }
        let s = &self.data[self.cursor..self.cursor + n];
        self.cursor += n;
        Ok(s)
    }

    /// `pq_getmsgint(msg, 4)` — get a 32-bit integer in network byte order.
    fn getmsgint32(&mut self) -> PgResult<i32> {
        let bytes = self.copymsgbytes(4)?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    /// `pq_getmsgint64(msg)` — get a 64-bit integer in network byte order.
    fn getmsgint64(&mut self) -> PgResult<i64> {
        let bytes = self.copymsgbytes(8)?;
        Ok(i64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    /// `pq_getmsgrawstring` — get a null-terminated string from the message
    /// buffer (no encoding conversion). Returns the bytes before the NUL.
    fn getmsgrawstring(&mut self) -> PgResult<&'a [u8]> {
        let start = self.cursor;
        let mut i = self.cursor;
        while i < self.data.len() && self.data[i] != 0 {
            i += 1;
        }
        if i >= self.data.len() {
            // No null terminator found.
            return Err(protocol_violation("invalid string in message"));
        }
        let s = &self.data[start..i];
        self.cursor = i + 1; // skip the NUL too
        Ok(s)
    }

    /// `pq_getmsgend` — verify the message has been fully consumed.
    fn getmsgend(&self) -> PgResult<()> {
        if self.cursor != self.data.len() {
            return Err(protocol_violation("invalid message format"));
        }
        Ok(())
    }
}

/// The `ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg(...)))`
/// the `pq_getmsg*` readers raise (lib/pqformat.c).
fn protocol_violation(msg: &'static str) -> PgError {
    PgError::error(msg).with_sqlstate(ERRCODE_PROTOCOL_VIOLATION)
}

/// `pg_strtoint32`-style decode for the numeric diagnostic fields. C uses
/// `pg_strtoint32`, which `ereport`s on malformed input; we surface the same
/// failure as an error.
fn parse_int32(value: &[u8]) -> PgResult<i32> {
    let s = core::str::from_utf8(value)
        .map_err(|_| PgError::error("invalid input syntax for type integer"))?;
    s.trim()
        .parse::<i32>()
        .map_err(|_| PgError::error("invalid input syntax for type integer"))
}

fn raw_to_string(value: &[u8]) -> String {
    // C `pstrdup`s the raw bytes verbatim; the values are already in the
    // backend encoding. Lossless byte->String for the fields we carry.
    String::from_utf8_lossy(value).into_owned()
}

/// `pq_parse_errornotice(StringInfo msg, ErrorData *edata)` — parse an
/// ErrorResponse or NoticeResponse protocol message into an `ErrorData`.
fn parse_errornotice(msg: &[u8]) -> PgResult<ErrorData> {
    // Initialize edata with reasonable defaults.
    let mut edata = ErrorData { elevel: ERROR.0, ..ErrorData::default() };

    let mut cur = MsgCursor::new(msg);

    // Loop over fields and extract each one.
    loop {
        let code = cur.getmsgbyte()?;
        if code == 0 {
            cur.getmsgend()?;
            break;
        }
        let value = cur.getmsgrawstring()?;
        let field = ::types_error::ErrorField(code as i32);

        if field == PG_DIAG_SEVERITY {
            // ignore, trusting to PG_DIAG_SEVERITY_NONLOCALIZED
        } else if field == PG_DIAG_SEVERITY_NONLOCALIZED {
            edata.elevel = match value {
                b"DEBUG" => DEBUG1.0, // or some other DEBUG level
                b"LOG" => LOG.0,      // can't be COMMERROR
                b"INFO" => INFO.0,
                b"NOTICE" => NOTICE.0,
                b"WARNING" => WARNING.0,
                b"ERROR" => ERROR.0,
                b"FATAL" => FATAL.0,
                b"PANIC" => PANIC.0,
                _ => {
                    return Err(PgError::error(format!(
                        "unrecognized error severity: \"{}\"",
                        raw_to_string(value)
                    )))
                }
            };
        } else if field == PG_DIAG_SQLSTATE {
            if value.len() != 5 {
                return Err(PgError::error(format!(
                    "invalid SQLSTATE: \"{}\"",
                    raw_to_string(value)
                )));
            }
            // MAKE_SQLSTATE(ch1..ch5): each char contributes 6 bits.
            let mut code: i32 = 0;
            for (i, &ch) in value.iter().enumerate() {
                code |= ((ch as i32) & 0x3F) << (6 * i);
            }
            edata.sqlerrcode = code;
        } else if field == PG_DIAG_MESSAGE_PRIMARY {
            edata.message = Some(raw_to_string(value));
        } else if field == PG_DIAG_MESSAGE_DETAIL {
            edata.detail = Some(raw_to_string(value));
        } else if field == PG_DIAG_MESSAGE_HINT {
            edata.hint = Some(raw_to_string(value));
        } else if field == PG_DIAG_STATEMENT_POSITION {
            edata.cursorpos = parse_int32(value)?;
        } else if field == PG_DIAG_INTERNAL_POSITION {
            edata.internalpos = parse_int32(value)?;
        } else if field == PG_DIAG_INTERNAL_QUERY {
            edata.internalquery = Some(raw_to_string(value));
        } else if field == PG_DIAG_CONTEXT {
            edata.context = Some(raw_to_string(value));
        } else if field == PG_DIAG_SCHEMA_NAME {
            edata.schema_name = Some(raw_to_string(value));
        } else if field == PG_DIAG_TABLE_NAME {
            edata.table_name = Some(raw_to_string(value));
        } else if field == PG_DIAG_COLUMN_NAME {
            edata.column_name = Some(raw_to_string(value));
        } else if field == PG_DIAG_DATATYPE_NAME {
            edata.datatype_name = Some(raw_to_string(value));
        } else if field == PG_DIAG_CONSTRAINT_NAME {
            edata.constraint_name = Some(raw_to_string(value));
        } else if field == PG_DIAG_SOURCE_FILE {
            edata.filename = Some(raw_to_string(value));
        } else if field == PG_DIAG_SOURCE_LINE {
            edata.lineno = parse_int32(value)?;
        } else if field == PG_DIAG_SOURCE_FUNCTION {
            edata.funcname = Some(raw_to_string(value));
        }
        // Anything else is unrecognized and ignored (matching C's switch with
        // no default arm).
    }

    Ok(edata)
}

/// `MAKE_SQLSTATE`-decoded `ErrorData` → owned [`PgError`], reproducing the field
/// copy `ThrowErrorData` performs out of the C stack `ErrorData edata`. Every
/// diagnostic field `pq_parse_errornotice` rebuilt is carried through, so the
/// leader re-raises the worker's error with its original message, SQLSTATE,
/// detail/hint/context, the column/table/schema/datatype/constraint names, the
/// statement/internal positions and internal query, and the source location —
/// nothing is lost to a thin cross-seam projection.
fn error_data_to_pg_error(edata: ErrorData) -> PgError {
    let mut e = PgError::new(ErrorLevel(edata.elevel), edata.message.unwrap_or_default());
    // C: `if (edata->sqlerrcode) edata->sqlerrcode` is always set by errstart;
    // `pq_parse_errornotice` leaves it 0 only if the worker omitted SQLSTATE, in
    // which case ThrowErrorData keeps the errstart default. Mirror that: only
    // override when the worker supplied a code.
    if edata.sqlerrcode != 0 {
        e.sqlstate = SqlState(edata.sqlerrcode);
    }
    e.detail = edata.detail;
    e.hint = edata.hint;
    e.context = edata.context;
    e.internal_query = edata.internalquery;
    e.schema_name = edata.schema_name;
    e.table_name = edata.table_name;
    e.column_name = edata.column_name;
    e.datatype_name = edata.datatype_name;
    e.constraint_name = edata.constraint_name;
    // C `ErrorData` carries these as plain ints with 0 meaning "unset".
    e.cursor_position = (edata.cursorpos != 0).then_some(edata.cursorpos);
    e.internal_position = (edata.internalpos != 0).then_some(edata.internalpos);
    // `errfinish` reports at edata.filename:lineno (funcname). Only present when
    // the worker sent the SOURCE_FILE/LINE/FUNCTION fields.
    if edata.filename.is_some() || edata.lineno != 0 || edata.funcname.is_some() {
        e.location = Some(ErrorLocation {
            filename: edata.filename,
            lineno: edata.lineno,
            funcname: edata.funcname,
        });
    }
    e
}

/// Seam body for `backend-libpq-pqmq-seams::pq_parse_errornotice` (the
/// parallel-apply leader, which reads only `context`).
fn pq_parse_errornotice_apply(
    msg: &[u8],
) -> PgResult<applyparallel::ParsedErrorNotice> {
    let edata = parse_errornotice(msg)?;
    Ok(applyparallel::ParsedErrorNotice { context: edata.context })
}

/// Seam body for `backend-access-transam-parallel-rt-seams::throw_parallel_error_data`
/// — the ErrorResponse/NoticeResponse arm of `ProcessParallelMessage`
/// (parallel.c:1159-1202). `msg` is the raw message body (the 1-byte type already
/// stripped by the leader). We rebuild the worker's full `ErrorData`
/// (`pq_parse_errornotice`), cap `elevel` at `ERROR` (death of a worker isn't
/// enough justification for suicide), optionally append the "parallel worker"
/// context line, and `ThrowErrorData(&edata)` — re-raising (elevel >= ERROR) or
/// logging (NOTICE/WARNING) with the worker's original message, SQLSTATE and
/// every diagnostic field intact. The `ErrorData` is kept local: it is never
/// projected lossily across a seam.
///
/// `_pcxt_error_context_stack` is the leader's saved `error_context_stack` C swaps
/// in around the `ThrowErrorData` call (`error_context_stack = pcxt->
/// error_context_stack`). That global is retired in this tree (context attaches
/// on propagation, not via a saved chain pointer — docs/query-lifecycle-raii.md),
/// so there is nothing to swap; the handle is accepted and ignored.
fn throw_parallel_error_data(
    msg: &[u8],
    append_parallel_worker_context: bool,
    _pcxt_error_context_stack: usize,
) -> PgResult<()> {
    // Parse ErrorResponse or NoticeResponse into the full ErrorData.
    let mut edata = parse_errornotice(msg)?;

    // Death of a worker isn't enough justification for suicide.
    edata.elevel = edata.elevel.min(ERROR.0);

    // If desired, add a context line to show that this is a message propagated
    // from a parallel worker. (Skipped in DEBUG_PARALLEL_REGRESS mode, which the
    // leader signals via `append_parallel_worker_context = false`.)
    if append_parallel_worker_context {
        edata.context = Some(match edata.context.take() {
            Some(ctx) => format!("{ctx}\nparallel worker"),
            None => String::from("parallel worker"),
        });
    }

    // Rethrow error or print notice.
    ThrowErrorData(error_data_to_pg_error(edata))
}

/// Seam body for `backend-access-transam-parallel-rt-seams::parse_notification_response`
/// — the `PqMsg_NotificationResponse` arm of `ProcessParallelMessage`
/// (parallel.c:1205-1219). The leader strips the 1-byte message type before the
/// call, so `msg` is the body: `int32 pid`, `string channel`, `string payload`.
/// Returns `(pid, channel, payload)` for `NotifyMyFrontEnd`. C uses
/// `pq_endmessage` here (which frees rather than verifying full consumption), so
/// no `getmsgend` is performed.
fn parse_notification_response(msg: &[u8]) -> PgResult<(i32, String, String)> {
    let mut cur = MsgCursor::new(msg);
    let pid = cur.getmsgint32()?;
    let channel = raw_to_string(cur.getmsgrawstring()?);
    let payload = raw_to_string(cur.getmsgrawstring()?);
    Ok((pid, channel, payload))
}

/// Seam body for `backend-access-transam-parallel-rt-seams::parse_progress` —
/// the `PqMsg_Progress` arm of `ProcessParallelMessage` (parallel.c:1222-1237).
/// `msg` is the body (type byte stripped): `int32 index`, `int64 incr`. Only
/// incremental progress reporting is supported; `pq_getmsgend` verifies the
/// message was fully consumed.
fn parse_progress(msg: &[u8]) -> PgResult<(i32, i64)> {
    let mut cur = MsgCursor::new(msg);
    let index = cur.getmsgint32()?;
    let incr = cur.getmsgint64()?;
    cur.getmsgend()?;
    Ok((index, incr))
}

// ---------------------------------------------------------------------------
// Seam installation.
// ---------------------------------------------------------------------------

extern crate alloc;
use alloc::string::String;

/// Install this crate's seams. Called from the startup aggregator.
pub fn init_seams() {
    pqmq_seams::pq_parse_errornotice::set(pq_parse_errornotice_apply);
    rt::pq_redirect_to_shm_mq::set(pq_redirect_to_shm_mq);
    rt::pq_set_parallel_leader::set(pq_set_parallel_leader);
    rt::throw_parallel_error_data::set(throw_parallel_error_data);
    rt::parse_notification_response::set(parse_notification_response);
    rt::parse_progress::set(parse_progress);
}
