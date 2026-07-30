// Physical streaming replication (walsender.c): StartReplication (PHYSICAL),
// XLogSendPhysical, WalSndLoop, and the keepalive/timeout/sleep helpers.
//
// The send-decision control flow and message framing are ported 1:1. WAL bytes
// are read through the xlogreader `wal_read` seam over an owned reader. Cascading
// (standby-served) streaming needs the walreceiver flush position (P2) and
// historic-timeline streaming needs the timeline-switch machinery (P4); both are
// loud panics. SyncRep and lag-tracking are out of P1 scope (async only).
#![allow(non_snake_case)]

use core::ffi::c_long;

use elog::ereport;
use repl_gram::{ReplicationKind, StartReplicationCmd};
use types_core::{InvalidXLogRecPtr, TimeLineID, TimestampTz, XLogRecPtr};
use types_error::{
    ErrorLocation, PgResult, DEBUG1, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR, LOG,
};
use types_storage::waiteventset::{WL_POSTMASTER_DEATH, WL_SOCKET_READABLE, WL_SOCKET_WRITEABLE};
use xlogreader::XLogReaderState;

use crate::WalSndState;

// MAX_SEND_SIZE (walsender.c:111): XLOG_BLCKSZ * 16.
const MAX_SEND_SIZE: usize = transam_xlog::XLOG_BLCKSZ * 16;

// WAIT_EVENT_WAL_SENDER_MAIN (wait_event_types.h): pg_stat_activity tag only.
const WAIT_EVENT_WAL_SENDER_MAIN: u32 = 0x05000000 | 15;

fn loc(line: i32, func: &'static str) -> ErrorLocation {
    ErrorLocation::new("src/backend/replication/walsender.c", line, func)
}

// --- per-backend state accessors (walsender.c file-static globals) ---
fn get_ts() -> TimestampTz {
    timestamp_seams::get_current_timestamp::call()
}
fn got_stopping() -> bool {
    crate::GOT_STOPPING.with(|c| c.get())
}
fn got_sigusr2() -> bool {
    crate::GOT_SIGUSR2.with(|c| c.get())
}
fn am_cascading() -> bool {
    crate::AM_CASCADING_WALSENDER.with(|c| c.get())
}
fn sent_ptr() -> XLogRecPtr {
    crate::SENT_PTR.with(|c| c.get())
}
fn set_sent_ptr(v: XLogRecPtr) {
    crate::SENT_PTR.with(|c| c.set(v));
}
fn caught_up() -> bool {
    crate::WAL_SND_CAUGHT_UP.with(|c| c.get())
}
fn set_caught_up(v: bool) {
    crate::WAL_SND_CAUGHT_UP.with(|c| c.set(v));
}
fn streaming_done_sending() -> bool {
    crate::STREAMING_DONE_SENDING.with(|c| c.get())
}
fn streaming_done_receiving() -> bool {
    crate::STREAMING_DONE_RECEIVING.with(|c| c.get())
}
fn last_reply_timestamp() -> TimestampTz {
    crate::LAST_REPLY_TIMESTAMP.with(|c| c.get())
}
fn last_processing() -> TimestampTz {
    crate::LAST_PROCESSING.with(|c| c.get())
}
fn waiting_for_ping_response() -> bool {
    crate::WAITING_FOR_PING_RESPONSE.with(|c| c.get())
}
fn set_waiting_for_ping_response(v: bool) {
    crate::WAITING_FOR_PING_RESPONSE.with(|c| c.set(v));
}

pub(crate) fn proc_exit(code: i32) -> ! {
    ipc_seams::proc_exit::call(code, init_small::globals::MyProcPid())
}

// static void WalSndShutdown(void). Terminate the backend; C also resets
// whereToSendOutput to suppress further client writes (a tcop concern not yet
// threaded through this crate).
pub(crate) fn WalSndShutdown() -> ! {
    proc_exit(0)
}

// static void StartReplication(StartReplicationCmd *cmd) — PHYSICAL branch.
pub fn StartReplication(mcx: mcx::Mcx<'_>, cmd: &StartReplicationCmd) -> PgResult<()> {
    if let Some(slotname) = cmd.slotname.as_deref() {
        slot::ReplicationSlotAcquire(slotname, true, true)?;
        let acquired = slot::MyReplicationSlot().expect("StartReplication: no slot acquired");
        if slot::SlotIsLogical(acquired) {
            return ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg("cannot use a logical replication slot for physical replication")
                .finish(loc(920, "StartReplication"));
        }
    }

    let am_cascading = transam_xlog::RecoveryInProgress();
    crate::AM_CASCADING_WALSENDER.with(|c| c.set(am_cascading));

    let mut flush_tli: TimeLineID = 0;
    let flush_ptr: XLogRecPtr = if am_cascading {
        let (ptr, tli) = crate::GetStandbyFlushRecPtr();
        flush_tli = tli;
        ptr
    } else {
        transam_xlog::GetFlushRecPtr(Some(&mut flush_tli))
    };

    if cmd.timeline != 0 {
        crate::SEND_TIME_LINE.with(|c| c.set(cmd.timeline));
        if cmd.timeline == flush_tli {
            crate::SEND_TIME_LINE_IS_HISTORIC.with(|c| c.set(false));
            crate::SEND_TIME_LINE_VALID_UPTO.with(|c| c.set(InvalidXLogRecPtr));
        } else {
            crate::SEND_TIME_LINE_IS_HISTORIC.with(|c| c.set(true));

            // Check that the timeline the client requested exists, and the
            // requested start location is on that timeline (walsender.c:871).
            let switchpoint = {
                let cx = mcx::MemoryContext::new("StartReplication/timeline-history");
                let history = timeline_seams::read_timeline_history::call(cx.mcx(), flush_tli)?;
                let (switchpoint, next_tli) =
                    timeline_seams::tli_switch_point::call(cmd.timeline, &history)?;
                crate::SEND_TIME_LINE_NEXT_TLI.with(|c| c.set(next_tli));
                switchpoint
            };

            // Loose on purpose (C comment): only reject when we forked off
            // the requested timeline before the switchpoint — the client may
            // legitimately ask for the beginning of the switch segment.
            if switchpoint != InvalidXLogRecPtr && switchpoint < cmd.startpoint {
                return ereport(ERROR)
                    .errmsg(format!(
                        "requested starting point {:X}/{:X} on timeline {} is not in this server's history",
                        (cmd.startpoint >> 32) as u32,
                        cmd.startpoint as u32,
                        cmd.timeline,
                    ))
                    .errdetail(format!(
                        "This server's history forked from timeline {} at {:X}/{:X}.",
                        cmd.timeline,
                        (switchpoint >> 32) as u32,
                        switchpoint as u32,
                    ))
                    .finish(loc(908, "StartReplication"));
            }
            crate::SEND_TIME_LINE_VALID_UPTO.with(|c| c.set(switchpoint));
        }
    } else {
        crate::SEND_TIME_LINE.with(|c| c.set(flush_tli));
        crate::SEND_TIME_LINE_VALID_UPTO.with(|c| c.set(InvalidXLogRecPtr));
        crate::SEND_TIME_LINE_IS_HISTORIC.with(|c| c.set(false));
    }

    crate::STREAMING_DONE_SENDING.with(|c| c.set(false));
    crate::STREAMING_DONE_RECEIVING.with(|c| c.set(false));

    // If there is nothing to stream, don't even enter COPY mode.
    if !crate::SEND_TIME_LINE_IS_HISTORIC.with(|c| c.get())
        || cmd.startpoint < crate::SEND_TIME_LINE_VALID_UPTO.with(|c| c.get())
    {
        crate::WalSndSetState(WalSndState::Catchup);

        // CopyBothResponse ('W'): overall copy format = textual (0), natts = 0.
        pqcomm::pq_putmessage(b'W', &[0u8, 0, 0])?;
        pqcomm::pq_flush()?;

        if flush_ptr < cmd.startpoint {
            return ereport(ERROR)
                .errmsg(format!(
                    "requested starting point {:X}/{:X} is ahead of the WAL flush position of this server {:X}/{:X}",
                    (cmd.startpoint >> 32) as u32,
                    cmd.startpoint as u32,
                    (flush_ptr >> 32) as u32,
                    flush_ptr as u32,
                ))
                .finish(loc(990, "StartReplication"));
        }

        set_sent_ptr(cmd.startpoint);
        crate::my_set_sentptr(cmd.startpoint);

        if syncrep_seams::sync_rep_init_config::is_installed() {
            syncrep_seams::sync_rep_init_config::call()?;
        }

        crate::REPLICATION_ACTIVE.with(|c| c.set(true));

        let mut reader = XLogReaderState::allocate(mcx, transam_xlog::wal_segment_size())?;
        let r = WalSndLoop(&mut |()| XLogSendPhysical(&mut reader));

        crate::REPLICATION_ACTIVE.with(|c| c.set(false));
        r?;

        if got_stopping() {
            proc_exit(0);
        }
        crate::WalSndSetState(WalSndState::Startup);

        debug_assert!(streaming_done_sending() && streaming_done_receiving());
    }

    if cmd.slotname.is_some() {
        slot::ReplicationSlotRelease()?;
    }

    // Copy is finished. Send a single-row result set indicating the next
    // timeline (walsender.c:990).
    if crate::SEND_TIME_LINE_IS_HISTORIC.with(|c| c.get()) {
        crate::send_next_timeline_result_set(mcx)?;
    }

    // Send CommandComplete (walsender.c:1029). The dispatch loop then sends
    // its own "START_REPLICATION" completion — C keeps that dupe on purpose
    // ("necessary per libpqrcv_endstreaming"): the first CommandComplete
    // closes the timeline result set (or the plain copy), the second is the
    // COMMAND_OK clients read after it (receivelog.c expects TUPLES_OK then
    // COMMAND_OK on a historic-timeline stream; pg_basebackup/020 subtest 40
    // catches its absence with "unexpected termination of replication
    // stream").
    tcop_dest::EndReplicationCommand(b"START_STREAMING")?;
    Ok(())
}

// static void XLogSendPhysical(void).
pub fn XLogSendPhysical(reader: &mut XLogReaderState<'_>) -> PgResult<()> {
    if got_stopping() {
        crate::WalSndSetState(WalSndState::Stopping);
    }

    if streaming_done_sending() {
        set_caught_up(true);
        return Ok(());
    }

    let send_rqst_ptr: XLogRecPtr = if crate::SEND_TIME_LINE_IS_HISTORIC.with(|c| c.get()) {
        // Streaming an old timeline in this server's history: it can be
        // streamed up to the point where we switched off that timeline.
        crate::SEND_TIME_LINE_VALID_UPTO.with(|c| c.get())
    } else if am_cascading() {
        // Streaming the latest timeline on a cascading standby.
        let (ptr, mut send_rqst_tli) = crate::GetStandbyFlushRecPtr();
        let became_historic = if !transam_xlog::RecoveryInProgress() {
            // We have been promoted; the current timeline became historic.
            send_rqst_tli = transam_xlog::ctl::GetWALInsertionTimeLine();
            crate::AM_CASCADING_WALSENDER.with(|c| c.set(false));
            true
        } else {
            crate::SEND_TIME_LINE.with(|c| c.get()) != send_rqst_tli
        };
        if became_historic {
            // The timeline we were sending has become historic. Read the new
            // timeline's history to find where we forked off the one we were
            // sending (walsender.c XLogSendPhysical).
            let send_tli = crate::SEND_TIME_LINE.with(|c| c.get());
            let (valid_upto, next_tli) = {
                let cx = mcx::MemoryContext::new("XLogSendPhysical/timeline-history");
                let history =
                    timeline_seams::read_timeline_history::call(cx.mcx(), send_rqst_tli)?;
                timeline_seams::tli_switch_point::call(send_tli, &history)?
            };
            debug_assert!(send_tli < next_tli);
            crate::SEND_TIME_LINE_VALID_UPTO.with(|c| c.set(valid_upto));
            crate::SEND_TIME_LINE_NEXT_TLI.with(|c| c.set(next_tli));
            crate::SEND_TIME_LINE_IS_HISTORIC.with(|c| c.set(true));
            valid_upto
        } else {
            ptr
        }
    } else {
        // Streaming the current timeline on a primary: send everything flushed.
        transam_xlog::GetFlushRecPtr(None)
    };

    // LagTrackerWrite: pg_stat_replication lag monitoring is deferred.

    // If this is a historic timeline and we've reached the point where we
    // forked to the next timeline, stop streaming. (sentPtr may legitimately
    // already exceed sendTimeLineValidUpto: a partial WAL record at the
    // switch point may have been half-sent — see the C comment.)
    let sent = sent_ptr();
    if crate::SEND_TIME_LINE_IS_HISTORIC.with(|c| c.get())
        && crate::SEND_TIME_LINE_VALID_UPTO.with(|c| c.get()) <= sent
    {
        // Close the current file.
        if reader.v.seg.ws_file >= 0 {
            // SAFETY: closing the fd this reader opened; C ignores errno too.
            unsafe { libc::close(reader.v.seg.ws_file) };
            reader.v.seg.ws_file = -1;
        }

        // Send CopyDone.
        pqcomm::pq_putmessage_noblock(b'c', &[])?;
        crate::STREAMING_DONE_SENDING.with(|c| c.set(true));
        set_caught_up(true);

        let valid_upto = crate::SEND_TIME_LINE_VALID_UPTO.with(|c| c.get());
        let _ = elog::elog(
            DEBUG1,
            format!(
                "walsender reached end of timeline at {:X}/{:X} (sent up to {:X}/{:X})",
                (valid_upto >> 32) as u32,
                valid_upto as u32,
                (sent >> 32) as u32,
                sent as u32
            ),
        );
        return Ok(());
    }

    debug_assert!(sent <= send_rqst_ptr);
    if send_rqst_ptr <= sent {
        set_caught_up(true);
        return Ok(());
    }

    let startptr: XLogRecPtr = sent;
    let mut endptr: XLogRecPtr = startptr + MAX_SEND_SIZE as u64;
    if send_rqst_ptr <= endptr {
        endptr = send_rqst_ptr;
        // Historic: not caught up until the timeline-end block above fires.
        set_caught_up(!crate::SEND_TIME_LINE_IS_HISTORIC.with(|c| c.get()));
    } else {
        // Round down to a page boundary.
        endptr -= endptr % transam_xlog::XLOG_BLCKSZ as u64;
        set_caught_up(false);
    }

    let nbytes = (endptr - startptr) as usize;
    debug_assert!(nbytes <= MAX_SEND_SIZE);

    xlog_send_physical_emit(reader, startptr, endptr, send_rqst_ptr)?;

    set_sent_ptr(endptr);
    crate::my_set_sentptr(endptr);

    if guc_tables::vars::update_process_title.read() {
        let title = format!("streaming {:X}/{:X}", (endptr >> 32) as u32, endptr as u32);
        ps_status_seams::set_ps_display::call(title.as_str());
    }

    Ok(())
}

// The WAL read + 'w' framing + CopyData put of XLogSendPhysical. WALReadFromBuffers
// is a read-from-shared-buffers optimization; reading the whole slice through
// wal_read (the WALRead fallback) is always correct.
fn xlog_send_physical_emit(
    reader: &mut XLogReaderState<'_>,
    startptr: XLogRecPtr,
    endptr: XLogRecPtr,
    send_rqst_ptr: XLogRecPtr,
) -> PgResult<()> {
    let nbytes = (endptr - startptr) as usize;
    let send_tli: TimeLineID = crate::SEND_TIME_LINE.with(|c| c.get());
    let segsize_u = transam_xlog::wal_segment_size() as u64;

    // WalSndSegmentOpen parity (walsender.c:3031): on a historic timeline the
    // segment containing sendTimeLineValidUpto is read from the NEXT
    // timeline's file (recovery copies the used portion of the old segment
    // into the new timeline's file; the old-TLI file may not exist). Pick the
    // TLI per segment and read segment-sized chunks.
    let historic = crate::SEND_TIME_LINE_IS_HISTORIC.with(|c| c.get());
    let end_seg_no = if historic {
        crate::SEND_TIME_LINE_VALID_UPTO.with(|c| c.get()) / segsize_u
    } else {
        0
    };
    let next_tli = crate::SEND_TIME_LINE_NEXT_TLI.with(|c| c.get());

    let mut wal_buf = vec![0u8; nbytes];
    let mut chunk_start = startptr;
    let mut off = 0usize;
    while chunk_start < endptr {
        let seg_no = chunk_start / segsize_u;
        let seg_end = (seg_no + 1) * segsize_u;
        let chunk_end = seg_end.min(endptr);
        let chunk_len = (chunk_end - chunk_start) as usize;
        let tli = if historic && seg_no == end_seg_no { next_tli } else { send_tli };
        if xlogreader_seams::wal_read::call(
            &mut reader.v,
            &mut wal_buf[off..off + chunk_len],
            chunk_start,
            chunk_len,
            tli,
        )?
        .is_err()
        {
            return wal_read_raise_error();
        }
        chunk_start = chunk_end;
        off += chunk_len;
    }

    crate::OUTPUT_MESSAGE.with(|b| {
        let mut b = b.borrow_mut();
        b.clear();
        b.push(b'w');
        b.extend_from_slice(&startptr.to_be_bytes()); // dataStart
        b.extend_from_slice(&send_rqst_ptr.to_be_bytes()); // walEnd
        b.extend_from_slice(&0i64.to_be_bytes()); // sendtime, filled in last
        b.extend_from_slice(&wal_buf);
        // Fill the send timestamp last so it is taken as late as possible.
        let sendtime = get_ts();
        b[1 + 8 + 8..1 + 8 + 8 + 8].copy_from_slice(&(sendtime as i64).to_be_bytes());
    });

    crate::OUTPUT_MESSAGE.with(|b| pqcomm::pq_putmessage_noblock(b'd', &b.borrow()))?;

    // C checks with xlogreader->seg.ws_tli, the TLI of the last-opened file.
    transam_xlog::CheckXLogRemoved(startptr / segsize_u, reader.v.seg.ws_tli)?;
    Ok(())
}

// WALReadRaiseError(&errinfo).
fn wal_read_raise_error() -> PgResult<()> {
    ereport(ERROR)
        .errcode_for_file_access()
        .errmsg("could not read from WAL: requested WAL segment slice is unavailable")
        .finish(ErrorLocation::new(file!(), line!() as i32, "WALReadRaiseError"))
}

// static void WalSndLoop(WalSndSendDataCallback send_data): shared by the
// physical (XLogSendPhysical) and logical (XLogSendLogical) walsenders; the
// C function pointer is a closure here.
pub(crate) fn WalSndLoop(send_data: &mut dyn FnMut(()) -> PgResult<()>) -> PgResult<()> {
    // WALSENDER_STATS_FLUSH_INTERVAL (walsender.c:100), ms.
    const WALSENDER_STATS_FLUSH_INTERVAL: i64 = 1000;

    // Initialize the last reply timestamp; that enables timeout processing.
    crate::LAST_REPLY_TIMESTAMP.with(|c| c.set(get_ts()));
    set_waiting_for_ping_response(false);
    let mut last_flush: TimestampTz = 0;

    loop {
        reset_my_latch();
        postgres_seams::check_for_interrupts::call()?;

        if interrupt::ConfigReloadPending() {
            interrupt::SetConfigReloadPending(false);
            guc_file::ProcessConfigFile(types_guc::GucContext::PGC_SIGHUP)?;
            if syncrep_seams::sync_rep_init_config::is_installed() {
                syncrep_seams::sync_rep_init_config::call()?;
            }
        }

        crate::replies::ProcessRepliesIfAny()?;

        // CopyDone received + sent + output drained => exit streaming.
        if streaming_done_receiving() && streaming_done_sending() && !pqcomm::pq_is_send_pending() {
            break;
        }

        if !pqcomm::pq_is_send_pending() {
            send_data(())?;
        } else {
            set_caught_up(false);
        }

        if pqcomm::pq_flush_if_writable()? != 0 {
            WalSndShutdown();
        }

        if caught_up() && !pqcomm::pq_is_send_pending() {
            if crate::my_walsnd_state() == WalSndState::Catchup {
                crate::WalSndSetState(WalSndState::Streaming);
            }
            // On SIGUSR2, drain up to the shutdown checkpoint and exit.
            if got_sigusr2() {
                WalSndDone(send_data)?;
            }
        }

        WalSndCheckTimeOut();
        WalSndKeepaliveIfNecessary()?;

        // Block if we have unsent data, or (physical) if caught up: send_data
        // does not block.
        if (caught_up() && !streaming_done_sending()) || pqcomm::pq_is_send_pending() {
            let mut wake_events: u32 = if !streaming_done_receiving() {
                WL_SOCKET_READABLE
            } else {
                0
            };
            let now = get_ts();
            let sleeptime = WalSndComputeSleeptime(now);
            if pqcomm::pq_is_send_pending() {
                wake_events |= WL_SOCKET_WRITEABLE;
            }

            // Report IO statistics, if needed (walsender.c:2923): the wal_read
            // counts otherwise sit in backend-local pending state forever
            // (pg_stat_io's walsender/wal rows stay 0 — 001_stream_rep #35).
            if adt_timestamp::TimestampDifferenceExceeds(
                last_flush,
                now,
                WALSENDER_STATS_FLUSH_INTERVAL as i32,
            ) {
                pgstat::io::pgstat_flush_io(false);
                let _ = pgstat::backend::pgstat_flush_backend(
                    false,
                    pgstat::backend::PGSTAT_BACKEND_FLUSH_IO,
                );
                last_flush = now;
            }

            WalSndWait(wake_events, sleeptime, WAIT_EVENT_WAL_SENDER_MAIN)?;
        }
    }
    Ok(())
}

// static void WalSndDone(WalSndSendDataCallback send_data).
fn WalSndDone(send_data: &mut dyn FnMut(()) -> PgResult<()>) -> PgResult<()> {
    // Let's be real sure we're caught up.
    send_data(())?;

    let replicated: XLogRecPtr = if crate::my_flush() == InvalidXLogRecPtr {
        crate::my_write()
    } else {
        crate::my_flush()
    };

    if caught_up() && sent_ptr() == replicated && !pqcomm::pq_is_send_pending() {
        pqcomm::pq_flush()?;
        proc_exit(0);
    }
    if !waiting_for_ping_response() {
        WalSndKeepalive(true, InvalidXLogRecPtr)?;
    }
    Ok(())
}

// static long WalSndComputeSleeptime(TimestampTz now).
pub(crate) fn WalSndComputeSleeptime(now: TimestampTz) -> c_long {
    let mut sleeptime: c_long = 10000; // 10 s
    let timeout = guc_tables::vars::wal_sender_timeout.read();
    let last_reply = last_reply_timestamp();

    if timeout > 0 && last_reply > 0 {
        let wakeup_time = if !waiting_for_ping_response() {
            last_reply + (timeout / 2) as i64 * 1000
        } else {
            last_reply + timeout as i64 * 1000
        };
        sleeptime = adt_timestamp::TimestampDifferenceMilliseconds(now, wakeup_time) as c_long;
    }
    sleeptime
}

// static void WalSndCheckTimeOut(void).
pub(crate) fn WalSndCheckTimeOut() {
    let timeout_guc = guc_tables::vars::wal_sender_timeout.read();
    let last_reply = last_reply_timestamp();

    if last_reply <= 0 {
        return;
    }
    let timeout = last_reply + timeout_guc as i64 * 1000;
    if timeout_guc > 0 && last_processing() >= timeout {
        // Expiration usually means a communication problem; don't tell the standby.
        let _ = ereport(LOG)
            .errmsg("terminating walsender process due to replication timeout")
            .finish(loc(2536, "WalSndCheckTimeOut"));
        WalSndShutdown();
    }
}

// static void WalSndWait(uint32 socket_events, long timeout, uint32 wait_event).
pub(crate) fn WalSndWait(socket_events: u32, timeout: c_long, wait_event: u32) -> PgResult<()> {
    // ModifyWaitEvent(FeBeWaitSet, FeBeWaitSetSocketPos, socket_events, NULL).
    pqcomm::pq_modify_fe_be_wait_set_socket(socket_events)?;

    // Prepare to sleep on the matching CV so WalSndWakeup() — or a physical
    // walsender's PhysicalWakeupLogicalWalSnd when we wait for the
    // synchronized_standby_slots standbys — can wake us (walsender.c:3760).
    let ctl = crate::WalSndCtl();
    let cv = if wait_event == crate::WAIT_EVENT_WAIT_FOR_STANDBY_CONFIRMATION {
        &ctl.wal_confirm_rcv_cv
    } else {
        match crate::my_kind() {
            ReplicationKind::REPLICATION_KIND_PHYSICAL => &ctl.wal_flush_cv,
            ReplicationKind::REPLICATION_KIND_LOGICAL => &ctl.wal_replay_cv,
        }
    };
    condition_variable::ConditionVariablePrepareToSleep(cv);

    let events = pqcomm::pq_wait_event_set_wait_fe_be(timeout as i64, wait_event)?;
    if events & WL_POSTMASTER_DEATH != 0 {
        condition_variable::ConditionVariableCancelSleep();
        proc_exit(1);
    }
    condition_variable::ConditionVariableCancelSleep();
    Ok(())
}

// static void WalSndKeepalive(bool requestReply, XLogRecPtr writePtr).
pub(crate) fn WalSndKeepalive(request_reply: bool, write_ptr: XLogRecPtr) -> PgResult<()> {
    let sent = sent_ptr();
    let now = get_ts();

    crate::OUTPUT_MESSAGE.with(|b| {
        let mut b = b.borrow_mut();
        b.clear();
        b.push(b'k');
        let wpos = if write_ptr == InvalidXLogRecPtr { sent } else { write_ptr };
        b.extend_from_slice(&wpos.to_be_bytes());
        b.extend_from_slice(&(now as u64).to_be_bytes());
        b.push(u8::from(request_reply));
    });

    crate::OUTPUT_MESSAGE.with(|b| pqcomm::pq_putmessage_noblock(b'd', &b.borrow()))?;

    if request_reply {
        set_waiting_for_ping_response(true);
    }
    Ok(())
}

// static void WalSndKeepaliveIfNecessary(void).
pub(crate) fn WalSndKeepaliveIfNecessary() -> PgResult<()> {
    let timeout = guc_tables::vars::wal_sender_timeout.read();
    let last_reply = last_reply_timestamp();

    if timeout <= 0 || last_reply <= 0 {
        return Ok(());
    }
    if waiting_for_ping_response() {
        return Ok(());
    }

    let ping_time = last_reply + (timeout / 2) as i64 * 1000;
    if last_processing() >= ping_time {
        WalSndKeepalive(true, InvalidXLogRecPtr)?;
        if pqcomm::pq_flush_if_writable()? != 0 {
            WalSndShutdown();
        }
    }
    Ok(())
}

pub(crate) fn reset_my_latch() {
    if let Some(l) = init_small::globals::MyLatch() {
        latch::ResetLatch(l);
    }
}
