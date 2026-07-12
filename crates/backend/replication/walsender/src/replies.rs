// Inbound standby messages (walsender.c): ProcessRepliesIfAny and the standby
// status-update / hot-standby-feedback / keepalive-reply handlers.
//
// 'r' (standby status update) is fully ported, including the physical slot
// restart_lsn advance. 'h' (hot-standby feedback) is received and its reply
// timestamp recorded, but the xmin holdback it requests is P4 (hot_standby_
// feedback loop); pg_receivewal, the inc-3 oracle, never sends 'h'.
#![allow(non_snake_case)]

use elog::ereport;
use types_core::{InvalidXLogRecPtr, TimestampTz, XLogRecPtr};
use types_error::{PgResult, ErrorLocation, COMMERROR, FATAL};

use crate::streaming::{proc_exit, WalSndKeepalive};

// pq_getmessage maximum body lengths (pqcomm.h).
const PQ_LARGE_MESSAGE_LIMIT: i32 = 0x3fff_ffff;
const PQ_SMALL_MESSAGE_LIMIT: i32 = 10000;
const EOF: i32 = -1;

fn loc(line: i32, func: &'static str) -> ErrorLocation {
    ErrorLocation::new("src/backend/replication/walsender.c", line, func)
}

fn get_ts() -> TimestampTz {
    timestamp_seams::get_current_timestamp::call()
}
fn streaming_done_sending() -> bool {
    crate::STREAMING_DONE_SENDING.with(|c| c.get())
}
fn streaming_done_receiving() -> bool {
    crate::STREAMING_DONE_RECEIVING.with(|c| c.get())
}

// A forward cursor over a received message body, mirroring the pq_getmsg*
// readers (pqformat.c) — big-endian.
struct MsgReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> MsgReader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        MsgReader { buf, pos: 0 }
    }
    fn get_byte(&mut self) -> u8 {
        let b = self.buf[self.pos];
        self.pos += 1;
        b
    }
    fn get_int64(&mut self) -> i64 {
        let mut a = [0u8; 8];
        a.copy_from_slice(&self.buf[self.pos..self.pos + 8]);
        self.pos += 8;
        i64::from_be_bytes(a)
    }
}

// static void ProcessRepliesIfAny(void).
pub fn ProcessRepliesIfAny() -> PgResult<()> {
    let mut received = false;

    let last_processing = get_ts();
    crate::LAST_PROCESSING.with(|c| c.set(last_processing));

    // Once we've received CopyDone, later messages belong to the next command
    // and are left for the main loop.
    while !streaming_done_receiving() {
        pqcomm::pq_startmsgread()?;

        let mut firstchar: u8 = 0;
        let r = pqcomm::pq_getbyte_if_available(&mut firstchar)?;
        if r == EOF {
            let _ = ereport(COMMERROR)
                .errmsg("unexpected EOF on standby connection")
                .finish(loc(2222, "ProcessRepliesIfAny"));
            proc_exit(0);
        }
        if r == 0 {
            pqcomm::pq_endmsgread();
            break;
        }

        let maxmsglen = match firstchar {
            b'd' => PQ_LARGE_MESSAGE_LIMIT,
            b'c' | b'X' => PQ_SMALL_MESSAGE_LIMIT,
            other => {
                return ereport(FATAL)
                    .errmsg(format!("invalid standby message type \"{}\"", other as char))
                    .finish(loc(2245, "ProcessRepliesIfAny"));
            }
        };

        // C's reply_message is a file-static StringInfo reset per message; here
        // a short-lived context lasting this one message's processing.
        let ctx = mcx::MemoryContext::new("reply_message");
        let mut buf = stringinfo::StringInfo::new_in(ctx.mcx())?;
        if pqcomm::pq_getmessage(&mut buf, maxmsglen)? != 0 {
            let _ = ereport(COMMERROR)
                .errmsg("unexpected EOF on standby connection")
                .finish(loc(2258, "ProcessRepliesIfAny"));
            proc_exit(0);
        }

        match firstchar {
            // 'd' — a standby reply wrapped in CopyData.
            b'd' => {
                ProcessStandbyMessage(buf.as_bytes())?;
                received = true;
            }
            // CopyDone — the standby wants to finish; reply with CopyDone if not
            // already sent.
            b'c' => {
                if !streaming_done_sending() {
                    pqcomm::pq_putmessage_noblock(b'c', &[])?;
                    crate::STREAMING_DONE_SENDING.with(|c| c.set(true));
                }
                crate::STREAMING_DONE_RECEIVING.with(|c| c.set(true));
                received = true;
            }
            // 'X' — the standby is closing the socket.
            b'X' => proc_exit(0),
            _ => debug_assert!(false), // NOT REACHED
        }
    }

    if received {
        crate::LAST_REPLY_TIMESTAMP.with(|c| c.set(last_processing));
        crate::WAITING_FOR_PING_RESPONSE.with(|c| c.set(false));
    }
    Ok(())
}

// static void ProcessStandbyMessage(void).
fn ProcessStandbyMessage(body: &[u8]) -> PgResult<()> {
    let mut r = MsgReader::new(body);
    let msgtype = r.get_byte();
    match msgtype {
        b'r' => ProcessStandbyReplyMessage(&mut r),
        b'h' => ProcessStandbyHSFeedbackMessage(&mut r),
        _ => {
            let _ = ereport(COMMERROR)
                .errmsg(format!("unexpected message type \"{}\"", msgtype as char))
                .finish(loc(2288, "ProcessStandbyMessage"));
            proc_exit(0);
        }
    }
}

// static void ProcessStandbyReplyMessage(void).
fn ProcessStandbyReplyMessage(r: &mut MsgReader<'_>) -> PgResult<()> {
    let write_ptr = r.get_int64() as XLogRecPtr;
    let flush_ptr = r.get_int64() as XLogRecPtr;
    let apply_ptr = r.get_int64() as XLogRecPtr;
    let reply_time: TimestampTz = r.get_int64();
    let reply_requested = r.get_byte() != 0;

    // LagTrackerRead: pg_stat_replication lag columns are monitoring-only and
    // deferred; report unknown (-1) lag.
    let (write_lag, flush_lag, apply_lag) = (-1i64, -1i64, -1i64);

    let sent = crate::SENT_PTR.with(|c| c.get());
    let mut clear_lag_times = false;
    if apply_ptr == sent {
        if crate::FULLY_APPLIED_LAST_TIME.with(|c| c.get()) {
            clear_lag_times = true;
        }
        crate::FULLY_APPLIED_LAST_TIME.with(|c| c.set(true));
    } else {
        crate::FULLY_APPLIED_LAST_TIME.with(|c| c.set(false));
    }

    if reply_requested {
        WalSndKeepalive(false, InvalidXLogRecPtr)?;
    }

    crate::my_set_reply(
        write_ptr, flush_ptr, apply_ptr, write_lag, flush_lag, apply_lag, clear_lag_times,
        reply_time,
    );

    if syncrep_seams::sync_rep_release_waiters::is_installed() {
        syncrep_seams::sync_rep_release_waiters::call()?;
    }

    if let Some(s) = slot::MyReplicationSlot() {
        if flush_ptr != InvalidXLogRecPtr {
            if slot::SlotIsLogical(s) {
                panic!(
                    "walsender: LogicalConfirmReceivedLocation unported \
                     (replication-p1 increment 6)"
                );
            }
            PhysicalConfirmReceivedLocation(flush_ptr)?;
        }
    }
    Ok(())
}

// static void PhysicalConfirmReceivedLocation(XLogRecPtr lsn).
fn PhysicalConfirmReceivedLocation(lsn: XLogRecPtr) -> PgResult<()> {
    debug_assert!(lsn != InvalidXLogRecPtr);
    let s = slot::MyReplicationSlot().expect("PhysicalConfirmReceivedLocation: no slot");

    let changed = s.with_mutex(|| {
        let mut d = s.data.get();
        if d.restart_lsn != lsn {
            d.restart_lsn = lsn;
            s.data.set(d);
            true
        } else {
            false
        }
    });

    if changed {
        slot::ReplicationSlotMarkDirty();
        slot::ReplicationSlotsComputeRequiredLSN()?;
        // PhysicalWakeupLogicalWalSnd(): failover / standby_slot_names wakeup is P4.
    }
    // The slot need not be saved to disk here (see the C comment).
    Ok(())
}

// static void ProcessStandbyHSFeedbackMessage(void). Received; the xmin holdback
// it carries (PhysicalReplicationSlotNewXmin / MyProc->xmin) is the P4
// hot_standby_feedback loop and not applied here.
fn ProcessStandbyHSFeedbackMessage(r: &mut MsgReader<'_>) -> PgResult<()> {
    let reply_time: TimestampTz = r.get_int64();
    let _feedback_xmin = r.get_int64(); // xmin + epoch (2x int32)
    let _feedback_catalog = r.get_int64(); // catalog_xmin + epoch (2x int32)

    crate::my_set_reply_time(reply_time);
    Ok(())
}
