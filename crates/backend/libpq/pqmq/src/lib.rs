#![allow(non_snake_case, non_upper_case_globals)]

use std::cell::{Cell, RefCell};

use pqcomm::{set_pq_comm_methods, PQcommMethods};
use shm_mq::{ShmMqHandle, ShmMqResult};
use stringinfo::StringInfo;
use types_core::{ProcNumber, INVALID_PROC_NUMBER};
use types_error::{
    make_sqlstate, PgError, PgResult, DEBUG1, ERROR, FATAL, INFO, LOG, NOTICE, PANIC, WARNING,
    PG_DIAG_COLUMN_NAME, PG_DIAG_CONSTRAINT_NAME, PG_DIAG_CONTEXT, PG_DIAG_DATATYPE_NAME,
    PG_DIAG_INTERNAL_POSITION, PG_DIAG_INTERNAL_QUERY, PG_DIAG_MESSAGE_DETAIL,
    PG_DIAG_MESSAGE_HINT, PG_DIAG_MESSAGE_PRIMARY, PG_DIAG_SCHEMA_NAME, PG_DIAG_SEVERITY,
    PG_DIAG_SEVERITY_NONLOCALIZED, PG_DIAG_SOURCE_FILE, PG_DIAG_SOURCE_FUNCTION,
    PG_DIAG_SOURCE_LINE, PG_DIAG_SQLSTATE, PG_DIAG_STATEMENT_POSITION, PG_DIAG_TABLE_NAME,
};
use types_storage::storage::ProcSignalReason;

const EOF: i32 = -1;
const PG_PROTOCOL_LATEST: u32 = (3 << 16) | 2;
// PG_WAIT_IPC | MessageQueuePutMessage (wait_event_names.txt; shm_mq holds
// the neighboring indexes 33/35/36).
const WAIT_EVENT_MESSAGE_QUEUE_PUT_MESSAGE: u32 = 0x0800_0000 + 34;

thread_local! {
    static PQ_MQ_HANDLE: RefCell<Option<ShmMqHandle>> = const { RefCell::new(None) };
    static PQ_MQ_BUSY: Cell<bool> = const { Cell::new(false) };
    static PQ_MQ_PARALLEL_LEADER_PID: Cell<i32> = const { Cell::new(0) };
    static PQ_MQ_PARALLEL_LEADER_PROC_NUMBER: Cell<ProcNumber> =
        const { Cell::new(INVALID_PROC_NUMBER) };
}

static PQ_COMM_MQ_METHODS: PQcommMethods = PQcommMethods {
    comm_reset: mq_comm_reset,
    flush: mq_flush,
    flush_if_writable: mq_flush_if_writable,
    is_send_pending: mq_is_send_pending,
    putmessage: mq_putmessage,
    putmessage_noblock: mq_putmessage_noblock,
};

pub fn pq_redirect_to_shm_mq(mqh: ShmMqHandle) {
    set_pq_comm_methods(&PQ_COMM_MQ_METHODS);
    PQ_MQ_HANDLE.with(|h| *h.borrow_mut() = Some(mqh));
    elog::config::set_where_to_send_output(types_dest::CommandDest::Remote);
    init_small::globals::SetFrontendProtocol(PG_PROTOCOL_LATEST);
}

// C's pq_cleanup_redirect_to_shm_mq runs from on_dsm_detach; there is no dsm
// segment here, so worker teardown calls this directly (the drop detaches).
pub fn pq_stop_redirect_to_shm_mq() {
    PQ_MQ_HANDLE.with(|h| h.borrow_mut().take());
    elog::config::set_where_to_send_output(types_dest::CommandDest::None);
}

pub fn pq_set_parallel_leader(pid: i32, proc_number: ProcNumber) {
    PQ_MQ_PARALLEL_LEADER_PID.with(|c| c.set(pid));
    PQ_MQ_PARALLEL_LEADER_PROC_NUMBER.with(|c| c.set(proc_number));
}

fn mq_comm_reset() {}

fn mq_flush() -> PgResult<i32> {
    Ok(0)
}

fn mq_flush_if_writable() -> PgResult<i32> {
    Ok(0)
}

fn mq_is_send_pending() -> bool {
    false
}

fn mq_putmessage(msgtype: u8, s: &[u8]) -> PgResult<i32> {
    // Re-entered mid-send (interrupt while waiting): detach rather than
    // postpone the interrupt response indefinitely.
    if PQ_MQ_BUSY.with(Cell::get) {
        if let Some(mut h) = PQ_MQ_HANDLE.with(|h| h.borrow_mut().take()) {
            h.detach();
        }
        return Ok(EOF);
    }

    // Queue already gone: ignore (late-shutdown DEBUG messages).
    if PQ_MQ_HANDLE.with(|h| h.borrow().is_none()) {
        return Ok(0);
    }

    PQ_MQ_BUSY.with(|c| c.set(true));

    // C's 2-entry iovec becomes one contiguous copy; cold path (errors,
    // notices), never per-row data.
    let mut buf = Vec::with_capacity(1 + s.len());
    buf.push(msgtype);
    buf.extend_from_slice(s);

    let result: PgResult<ShmMqResult> = PQ_MQ_HANDLE.with(|cell| {
        let mut borrow = cell.borrow_mut();
        let handle = borrow.as_mut().expect("checked non-empty above; backend is one thread");
        loop {
            let res = handle.send(&buf, true, /* force_flush before signal */ true)?;

            let leader_pid = PQ_MQ_PARALLEL_LEADER_PID.with(Cell::get);
            if leader_pid != 0 {
                // pqmq.c:170-178: a logical parallel apply worker wakes its
                // leader with PROCSIG_PARALLEL_APPLY_MESSAGE, any other
                // (parallel-query) worker with PROCSIG_PARALLEL_MESSAGE. The
                // seam is installed by the logical worker crate at boot; a
                // binary without it has no parallel apply workers.
                use logical_worker_seams::is_logical_parallel_apply_worker as is_pa_worker;
                let reason = if is_pa_worker::is_installed() && is_pa_worker::call() {
                    ProcSignalReason::PROCSIG_PARALLEL_APPLY_MESSAGE
                } else {
                    ProcSignalReason::PROCSIG_PARALLEL_MESSAGE
                };
                procsignal::SendProcSignal(
                    leader_pid,
                    reason,
                    PQ_MQ_PARALLEL_LEADER_PROC_NUMBER.with(Cell::get),
                );
            }

            if res != ShmMqResult::WouldBlock {
                return Ok(res);
            }

            shm_mq::wait_on_my_latch(
                init_small::globals::MyLatch(),
                WAIT_EVENT_MESSAGE_QUEUE_PUT_MESSAGE,
            )?;
        }
    });

    match result {
        Ok(res) => {
            PQ_MQ_BUSY.with(|c| c.set(false));
            debug_assert!(matches!(res, ShmMqResult::Success | ShmMqResult::Detached));
            Ok(if res == ShmMqResult::Success { 0 } else { EOF })
        }
        // C parity: pq_mq_busy stays set across an error escape, so the next
        // putmessage detaches the queue.
        Err(e) => Err(e),
    }
}

fn mq_putmessage_noblock(_msgtype: u8, _s: &[u8]) -> PgResult<()> {
    Err(Box::new(PgError::error("not currently supported".to_string())))
}

fn elog_error(msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg))
}

pub fn pq_parse_errornotice(msg: &mut StringInfo<'_>) -> PgResult<PgError> {
    let mut edata = PgError::new(ERROR, String::new());
    // pqmq.c:219 MemSet: no filename/lineno/funcname until the F/L/R fields
    // supply them (PgError::new records this construction site otherwise).
    edata.location = None;
    let mut filename: Option<String> = None;
    let mut lineno: i32 = 0;
    let mut funcname: Option<String> = None;

    loop {
        let code = pqformat::pq_getmsgbyte(msg)?;
        if code == 0 {
            pqformat::pq_getmsgend(msg)?;
            break;
        }
        let raw = pqformat::pq_getmsgrawstring(msg)?;
        let value = String::from_utf8_lossy(raw).into_owned();

        match code {
            // Localized severity: ignored, the nonlocalized version follows.
            _ if code == PG_DIAG_SEVERITY.0 => {}
            _ if code == PG_DIAG_SEVERITY_NONLOCALIZED.0 => {
                edata.level = match value.as_str() {
                    // Exact DEBUG level is unrecoverable; DEBUG1 still
                    // reaches the client.
                    "DEBUG" => DEBUG1,
                    "LOG" => LOG,
                    "INFO" => INFO,
                    "NOTICE" => NOTICE,
                    "WARNING" => WARNING,
                    "ERROR" => ERROR,
                    "FATAL" => FATAL,
                    "PANIC" => PANIC,
                    _ => {
                        return Err(elog_error(format!(
                            "unrecognized error severity: \"{value}\""
                        )))
                    }
                };
            }
            _ if code == PG_DIAG_SQLSTATE.0 => {
                if raw.len() != 5 {
                    return Err(elog_error(format!("invalid SQLSTATE: \"{value}\"")));
                }
                edata.sqlstate = make_sqlstate([raw[0], raw[1], raw[2], raw[3], raw[4]]);
            }
            _ if code == PG_DIAG_MESSAGE_PRIMARY.0 => edata.message = value,
            _ if code == PG_DIAG_MESSAGE_DETAIL.0 => edata.detail = Some(value),
            _ if code == PG_DIAG_MESSAGE_HINT.0 => edata.hint = Some(value),
            _ if code == PG_DIAG_STATEMENT_POSITION.0 => {
                edata.cursor_position = Some(numutils::pg_strtoint32(&value)?)
            }
            _ if code == PG_DIAG_INTERNAL_POSITION.0 => {
                edata.internal_position = Some(numutils::pg_strtoint32(&value)?)
            }
            _ if code == PG_DIAG_INTERNAL_QUERY.0 => edata.internal_query = Some(value),
            _ if code == PG_DIAG_CONTEXT.0 => edata.context = Some(value),
            _ if code == PG_DIAG_SCHEMA_NAME.0 => edata.schema_name = Some(value),
            _ if code == PG_DIAG_TABLE_NAME.0 => edata.table_name = Some(value),
            _ if code == PG_DIAG_COLUMN_NAME.0 => edata.column_name = Some(value),
            _ if code == PG_DIAG_DATATYPE_NAME.0 => edata.datatype_name = Some(value),
            _ if code == PG_DIAG_CONSTRAINT_NAME.0 => edata.constraint_name = Some(value),
            _ if code == PG_DIAG_SOURCE_FILE.0 => filename = Some(value),
            _ if code == PG_DIAG_SOURCE_LINE.0 => lineno = numutils::pg_strtoint32(&value)?,
            _ if code == PG_DIAG_SOURCE_FUNCTION.0 => funcname = Some(value),
            _ => return Err(elog_error(format!("unrecognized error field code: {code}"))),
        }
    }

    if filename.is_some() || lineno != 0 || funcname.is_some() {
        edata.location = Some(types_error::ErrorLocation {
            filename,
            lineno,
            funcname,
        });
    }

    Ok(edata)
}

#[cfg(test)]
mod tests;
