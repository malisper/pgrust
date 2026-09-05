// Inbound standby messages (walsender.c): ProcessRepliesIfAny and the standby
// status-update / hot-standby-feedback / keepalive-reply handlers.
//
// 'r' (standby status update) is fully ported, including the physical slot
// restart_lsn advance. 'h' (hot-standby feedback) is received and its reply
// timestamp recorded, but the xmin holdback it requests is P4 (hot_standby_
// feedback loop); pg_receivewal, the inc-3 oracle, never sends 'h'.
#![allow(non_snake_case)]

use elog::{ereport, message_level_is_interesting};
use pqformat::{pq_getmsgbyte, pq_getmsgint, pq_getmsgint64};
use stringinfo::StringInfo;
use types_core::{InvalidXLogRecPtr, TimestampTz, XLogRecPtr};
use types_error::{PgResult, ErrorLocation, COMMERROR, DEBUG2, ERRCODE_PROTOCOL_VIOLATION, FATAL};

use crate::streaming::{proc_exit, WalSndKeepalive};

// SYNC_REP_WAIT_* read-head indices (syncrep.h) into the lag tracker.
const SYNC_REP_WAIT_WRITE: usize = 0;
const SYNC_REP_WAIT_FLUSH: usize = 1;
const SYNC_REP_WAIT_APPLY: usize = 2;

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

// Build a StringInfo over a received CopyData body so the message readers go
// through pqformat's pq_getmsg* helpers (pqformat.c), which validate buffer
// boundaries and raise 08P01 on a truncated packet instead of the raw slice
// panic the hand-rolled reader had.

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
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("unexpected EOF on standby connection")
                .finish(loc(2288, "ProcessRepliesIfAny"));
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
                    .errcode(ERRCODE_PROTOCOL_VIOLATION)
                    .errmsg(format!("invalid standby message type \"{}\"", other as char))
                    .finish(loc(2312, "ProcessRepliesIfAny"));
            }
        };

        // C's reply_message is a file-static StringInfo reset per message; here
        // a short-lived context lasting this one message's processing.
        let ctx = mcx::MemoryContext::new("reply_message");
        let mut buf = stringinfo::StringInfo::new_in(ctx.mcx())?;
        if pqcomm::pq_getmessage(&mut buf, maxmsglen)? != 0 {
            let _ = ereport(COMMERROR)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("unexpected EOF on standby connection")
                .finish(loc(2325, "ProcessRepliesIfAny"));
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

// static void ProcessStandbyMessage(void). The body is the CopyData payload;
// a StringInfo lets the field reads use pqformat's validated pq_getmsg*.
pub(crate) fn ProcessStandbyMessage(body: &[u8]) -> PgResult<()> {
    let ctx = mcx::MemoryContext::new("standby_reply_body");
    let mut msg = StringInfo::new_in(ctx.mcx())?;
    msg.append_bytes(body)?;

    let msgtype = pq_getmsgbyte(&mut msg)?;
    match msgtype as u8 {
        b'r' => ProcessStandbyReplyMessage(&mut msg),
        b'h' => ProcessStandbyHSFeedbackMessage(&mut msg),
        _ => {
            let _ = ereport(COMMERROR)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg(format!("unexpected message type \"{}\"", msgtype as u8 as char))
                .finish(loc(2409, "ProcessStandbyMessage"));
            proc_exit(0);
        }
    }
}

// static void ProcessStandbyReplyMessage(void) (walsender.c:2445).
fn ProcessStandbyReplyMessage(msg: &mut StringInfo<'_>) -> PgResult<()> {
    let write_ptr = pq_getmsgint64(msg)? as XLogRecPtr;
    let flush_ptr = pq_getmsgint64(msg)? as XLogRecPtr;
    let apply_ptr = pq_getmsgint64(msg)? as XLogRecPtr;
    let reply_time: TimestampTz = pq_getmsgint64(msg)?;
    let reply_requested = pq_getmsgbyte(msg)? != 0;

    if message_level_is_interesting(DEBUG2) {
        let reply_time_str = timestamp_seams::timestamptz_to_str::call(reply_time);
        let _ = elog::elog(
            DEBUG2,
            format!(
                "write {:X}/{:X} flush {:X}/{:X} apply {:X}/{:X}{} reply_time {reply_time_str}",
                (write_ptr >> 32) as u32,
                write_ptr as u32,
                (flush_ptr >> 32) as u32,
                flush_ptr as u32,
                (apply_ptr >> 32) as u32,
                apply_ptr as u32,
                if reply_requested { " (reply requested)" } else { "" },
            ),
        );
    }

    // See if we can compute the round-trip lag for these positions
    // (walsender.c:2487 LagTrackerRead per reported head).
    let now = get_ts();
    let write_lag = crate::lag::LagTrackerRead(SYNC_REP_WAIT_WRITE, write_ptr, now);
    let flush_lag = crate::lag::LagTrackerRead(SYNC_REP_WAIT_FLUSH, flush_ptr, now);
    let apply_lag = crate::lag::LagTrackerRead(SYNC_REP_WAIT_APPLY, apply_ptr, now);

    let clear_lag_times =
        reply_clears_lag_times(write_ptr, flush_ptr, apply_ptr, crate::SENT_PTR.with(|c| c.get()));

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

// ProcessStandbyReplyMessage's clear-lag-times decision, kept beside its
// function-static state (C's prevWritePtr/prevFlushPtr/prevApplyPtr): forget
// the measured lag only when the standby reports full replay (flush and apply
// at sentPtr) AND its write/flush/apply positions are unchanged since the
// previous reply -- the wal_receiver_status_interval tick with no activity
// behind it. "apply == sentPtr twice in a row" alone cleared the lag while
// positions were still advancing (logical apply catches up at once).
// upstream 98e96e579b91 (18.4): Fix premature NULL lag reporting in pg_stat_replication
fn reply_clears_lag_times(
    write_ptr: XLogRecPtr,
    flush_ptr: XLogRecPtr,
    apply_ptr: XLogRecPtr,
    sent: XLogRecPtr,
) -> bool {
    let clear_lag_times = apply_ptr == sent
        && flush_ptr == sent
        && write_ptr == crate::PREV_WRITE_PTR.with(|c| c.get())
        && flush_ptr == crate::PREV_FLUSH_PTR.with(|c| c.get())
        && apply_ptr == crate::PREV_APPLY_PTR.with(|c| c.get());
    crate::PREV_WRITE_PTR.with(|c| c.set(write_ptr));
    crate::PREV_FLUSH_PTR.with(|c| c.set(flush_ptr));
    crate::PREV_APPLY_PTR.with(|c| c.set(apply_ptr));
    clear_lag_times
}

// static void PhysicalConfirmReceivedLocation(XLogRecPtr lsn).
fn PhysicalConfirmReceivedLocation(lsn: XLogRecPtr) -> PgResult<()> {
    debug_assert!(lsn != InvalidXLogRecPtr);
    let s = slot::MyReplicationSlot().expect("PhysicalConfirmReceivedLocation: no slot");

    let changed = s.with_mutex(|| {
        let mut d = unsafe { s.data.get() };
        if d.restart_lsn != lsn {
            d.restart_lsn = lsn;
            unsafe { s.data.set(d) };
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
        let mut data = unsafe { slot.data.get() };
        let mut changed = false;
        // Physical replication doesn't need the xmin/effective_xmin
        // interlock (missed increases only cost query cancellations):
        // set both at once.
        if !normal(data.xmin) || !normal(feedback_xmin)
            || TransactionIdPrecedes(data.xmin, feedback_xmin)
        {
            changed = true;
            data.xmin = feedback_xmin;
            unsafe { slot.effective_xmin.set(feedback_xmin) };
        }
        if !normal(data.catalog_xmin) || !normal(feedback_catalog_xmin)
            || TransactionIdPrecedes(data.catalog_xmin, feedback_catalog_xmin)
        {
            changed = true;
            data.catalog_xmin = feedback_catalog_xmin;
            unsafe { slot.effective_catalog_xmin.set(feedback_catalog_xmin) };
        }
        unsafe { slot.data.set(data) };
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

// static void ProcessStandbyHSFeedbackMessage(void) (walsender.c:2633).
fn ProcessStandbyHSFeedbackMessage(msg: &mut StringInfo<'_>) -> PgResult<()> {
    use std::sync::atomic::Ordering::Relaxed;
    use types_core::{FirstNormalTransactionId, InvalidTransactionId};

    let reply_time: TimestampTz = pq_getmsgint64(msg)?;
    let feedback_xmin = pq_getmsgint(msg, 4)?;
    let feedback_epoch = pq_getmsgint(msg, 4)?;
    let feedback_catalog_xmin = pq_getmsgint(msg, 4)?;
    let feedback_catalog_epoch = pq_getmsgint(msg, 4)?;

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

#[cfg(test)]
mod tests {
    use super::reply_clears_lag_times;

    // upstream 98e96e579b91 (18.4): the measured lag is forgotten only when a
    // reply repeats a fully-replayed position (flush and apply at sentPtr,
    // write/flush/apply unchanged since the previous reply) -- the
    // wal_receiver_status_interval tick with nothing behind it. A reply whose
    // positions advanced with sentPtr (logical apply catches up at once) or
    // whose flush still lags keeps the lag; the old "apply == sent twice"
    // rule cleared it there.
    #[test]
    fn lag_cleared_only_on_repeated_fully_replayed_reply() {
        assert!(!reply_clears_lag_times(100, 100, 100, 100));
        assert!(reply_clears_lag_times(100, 100, 100, 100));

        assert!(!reply_clears_lag_times(200, 200, 200, 200), "positions advanced: activity");
        assert!(!reply_clears_lag_times(300, 300, 300, 300), "positions advanced: activity");

        assert!(!reply_clears_lag_times(300, 250, 300, 300), "flush lags sentPtr");
        assert!(!reply_clears_lag_times(300, 250, 300, 300), "flush lags sentPtr");

        assert!(!reply_clears_lag_times(300, 300, 300, 300), "flush just caught up");
        assert!(reply_clears_lag_times(300, 300, 300, 300), "unchanged full replay");
    }
}
