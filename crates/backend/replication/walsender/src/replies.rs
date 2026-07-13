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
    fn get_int32(&mut self) -> u32 {
        let mut a = [0u8; 4];
        a.copy_from_slice(&self.buf[self.pos..self.pos + 4]);
        self.pos += 4;
        u32::from_be_bytes(a)
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
                logical::LogicalConfirmReceivedLocation(flush_ptr)?;
            } else {
                PhysicalConfirmReceivedLocation(flush_ptr)?;
            }
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
        crate::PhysicalWakeupLogicalWalSnd();
    }
    // The slot need not be saved to disk here (see the C comment).
    Ok(())
}

fn my_proc() -> &'static ::types_storage::storage::PGPROC {
    lmgr_proc::GetPGProcByNumber(init_small::globals::MyProcNumber())
}

// PhysicalReplicationSlotNewXmin (walsender.c:2522): the new slot xmin
// horizon from standby feedback.
fn PhysicalReplicationSlotNewXmin(
    feedback_xmin: types_core::TransactionId,
    feedback_catalog_xmin: types_core::TransactionId,
) -> PgResult<()> {
    use types_core::xact::TransactionIdPrecedes;
    use types_core::{InvalidTransactionId, FirstNormalTransactionId};

    let slot = slot::MyReplicationSlot().expect("PhysicalReplicationSlotNewXmin: no slot");
    let normal = |x: types_core::TransactionId| x >= FirstNormalTransactionId;
    let changed = slot.with_mutex(|| {
        my_proc().xmin.value.store(InvalidTransactionId, std::sync::atomic::Ordering::Relaxed);
        let mut data = slot.data.get();
        let mut changed = false;
        // Physical replication doesn't need the xmin/effective_xmin
        // interlock (missed increases only cost query cancellations):
        // set both at once.
        if !normal(data.xmin) || !normal(feedback_xmin)
            || TransactionIdPrecedes(data.xmin, feedback_xmin)
        {
            changed = true;
            data.xmin = feedback_xmin;
            slot.effective_xmin.set(feedback_xmin);
        }
        if !normal(data.catalog_xmin) || !normal(feedback_catalog_xmin)
            || TransactionIdPrecedes(data.catalog_xmin, feedback_catalog_xmin)
        {
            changed = true;
            data.catalog_xmin = feedback_catalog_xmin;
            slot.effective_catalog_xmin.set(feedback_catalog_xmin);
        }
        slot.data.set(data);
        changed
    });

    if changed {
        slot::ReplicationSlotMarkDirty();
        slot::ReplicationSlotsComputeRequiredXmin(false)?;
    }
    Ok(())
}

// TransactionIdInRecentPast (walsender.c:2570): not in the future, not
// already wrapped around.
fn transaction_id_in_recent_past(xid: types_core::TransactionId, epoch: u32) -> bool {
    use std::sync::atomic::Ordering::Relaxed;
    let next_full = types_core::FullTransactionId {
        value: procarray::TransamVariables().nextXid.load(Relaxed),
    };
    let next_xid = next_full.xid();
    let next_epoch = (next_full.value >> 32) as u32;

    if xid <= next_xid {
        if epoch != next_epoch {
            return false;
        }
    } else if epoch.wrapping_add(1) != next_epoch {
        return false;
    }
    types_core::xact::TransactionIdPrecedesOrEquals(xid, next_xid)
}

// static void ProcessStandbyHSFeedbackMessage(void) (walsender.c:2602).
fn ProcessStandbyHSFeedbackMessage(r: &mut MsgReader<'_>) -> PgResult<()> {
    use std::sync::atomic::Ordering::Relaxed;
    use types_core::{FirstNormalTransactionId, InvalidTransactionId};

    let reply_time: TimestampTz = r.get_int64();
    let feedback_xmin = r.get_int32();
    let feedback_epoch = r.get_int32();
    let feedback_catalog_xmin = r.get_int32();
    let feedback_catalog_epoch = r.get_int32();

    crate::my_set_reply_time(reply_time);

    let normal = |x: types_core::TransactionId| x >= FirstNormalTransactionId;

    // Invalid feedback values: the downstream turned hot_standby_feedback
    // off — unset our xmins.
    if !normal(feedback_xmin) && !normal(feedback_catalog_xmin) {
        my_proc().xmin.value.store(InvalidTransactionId, Relaxed);
        if slot::MyReplicationSlot().is_some() {
            PhysicalReplicationSlotNewXmin(feedback_xmin, feedback_catalog_xmin)?;
        }
        return Ok(());
    }

    // Ignore insane xmin/epoch pairs (future, or wrapped around).
    if normal(feedback_xmin) && !transaction_id_in_recent_past(feedback_xmin, feedback_epoch) {
        return Ok(());
    }
    if normal(feedback_catalog_xmin)
        && !transaction_id_in_recent_past(feedback_catalog_xmin, feedback_catalog_epoch)
    {
        return Ok(());
    }

    // Reserve the xmin via the slot when we have one, else via our PGPROC
    // entry (which can only track one value: store the lesser).
    if slot::MyReplicationSlot().is_some() {
        PhysicalReplicationSlotNewXmin(feedback_xmin, feedback_catalog_xmin)?;
    } else if normal(feedback_catalog_xmin)
        && types_core::xact::TransactionIdPrecedes(feedback_catalog_xmin, feedback_xmin)
    {
        my_proc().xmin.value.store(feedback_catalog_xmin, Relaxed);
    } else {
        my_proc().xmin.value.store(feedback_xmin, Relaxed);
    }
    Ok(())
}
