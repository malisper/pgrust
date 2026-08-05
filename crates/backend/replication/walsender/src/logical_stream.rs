// The logical walsender (walsender.c): StartLogicalReplication, XLogSendLogical,
// the WalSndPrepareWrite/WalSndWriteData/WalSndUpdateProgress output-plugin
// writer callbacks, WalSndWaitForWal, and the walsender's logical WAL page
// reader. The decode engine underneath (decode.c / reorderbuffer / snapbuild /
// output plugins) is the same one the SQL interface uses.
//
// Out of scope, refused loudly downstream: two-phase decoding, in-progress
// streaming, snapshot export (all stubbed in their owning crates), and the
// synchronized_standby_slots wait (NeedToWaitForStandbys) — the GUC-off path
// is the ported one.
#![allow(non_snake_case)]

use core::ffi::c_long;
use std::cell::Cell;

use elog::ereport;
use logical::{LogicalDecodingContext, OutputPluginContext, PluginOption};
use repl_gram::{ReplOptionArg, StartReplicationCmd};
use types_core::{InvalidXLogRecPtr, TimeLineID, TimestampTz, TransactionId, XLogRecPtr, XLogSegNo};
use types_error::{ErrorLocation, PgResult, ERROR};
use types_storage::waiteventset::{WL_SOCKET_READABLE, WL_SOCKET_WRITEABLE};
use xlogreader_seams::XLogReaderState as ReaderView;
use xlogreader::{XLogReaderRoutine, XLogSegmentRoutine};

use crate::streaming::{
    proc_exit, reset_my_latch, WalSndCheckTimeOut, WalSndComputeSleeptime,
    WalSndKeepaliveIfNecessary, WalSndLoop, WalSndShutdown, WalSndWait,
};
use crate::WalSndState;

// WAIT_EVENT_WAL_SENDER_WAIT_FOR_WAL / _WRITE_DATA (wait_event_types.h, Client section).
// wait_event_names.txt Client section order: ...(5)=SSL_OPEN_SERVER,
// (6)=WAIT_FOR_STANDBY_CONFIRMATION, (7)=WAL_SENDER_WAIT_FOR_WAL,
// (8)=WAL_SENDER_WRITE_DATA.
const WAIT_EVENT_WAL_SENDER_WAIT_WAL: u32 = 0x0600_0000 | 7;
const WAIT_EVENT_WAL_SENDER_WRITE_DATA: u32 = 0x0600_0000 | 8;

fn get_ts() -> TimestampTz {
    timestamp_seams::get_current_timestamp::call()
}

thread_local! {
    // WalSndPrepareWrite's framing header state, consumed by WalSndWriteData
    // (C writes the 'w' header into ctx->out; out is textual here, so the
    // header rides beside it).
    static PENDING_WRITE_LSN: Cell<XLogRecPtr> = const { Cell::new(0) };
    // XLogSendLogical's cached flushPtr (C function-static).
    static LOGICAL_FLUSH_PTR: Cell<XLogRecPtr> = const { Cell::new(InvalidXLogRecPtr) };
    // WalSndWaitForWal's RecentFlushPtr (C function-static).
    static RECENT_FLUSH_PTR: Cell<XLogRecPtr> = const { Cell::new(InvalidXLogRecPtr) };
    // XLogBackgroundFlush's lastflush pacing is caller-owned state since the
    // M4 walwriter migration; each walsender is a persistent thread (the C
    // per-process function-static analog), so one per walsender thread.
    static WAL_FLUSH_PACING: Cell<transam_xlog::WalFlushPacing> =
        const { Cell::new(transam_xlog::WalFlushPacing::new()) };
}

// StartLogicalReplication (walsender.c:1447).
pub(crate) fn StartLogicalReplication(cmd: &StartReplicationCmd) -> PgResult<()> {
    logical::CheckLogicalDecodingRequirements()?;

    debug_assert!(slot::MyReplicationSlot().is_none());
    slot::ReplicationSlotAcquire(cmd.slotname.as_deref().unwrap_or(""), true, true)?;

    // Force a disconnect if we were connected during recovery and the server
    // has been promoted since, so the decoding code doesn't need to care
    // about a mid-stream switch out of recovery (walsender.c:1462). Client
    // code is expected to handle reconnects.
    if crate::am_cascading_walsender_now() && !transam_xlog::RecoveryInProgress() {
        let _ = ereport(types_error::LOG)
            .errmsg("terminating walsender process after promotion")
            .finish(ErrorLocation::new(
                "src/backend/replication/walsender.c",
                1466,
                "StartLogicalReplication",
            ));
        crate::GOT_STOPPING.with(|c| c.set(true));
    }

    // Create our decoding context, starting from the previously ack'ed
    // position. Before CopyBothResponse so errors are reported early.
    // DefElem plugin options -> (name, value) pairs (defGetString rendering).
    let options: Vec<PluginOption> = cmd
        .options
        .iter()
        .map(|o| {
            let val = o.arg.as_ref().map(|a| match a {
                ReplOptionArg::Str(v) => v.clone(),
                ReplOptionArg::Int(i) => i.to_string(),
                ReplOptionArg::Bool(b) => if *b { "true" } else { "false" }.to_string(),
            });
            (o.name.clone(), val)
        })
        .collect();
    let mut ctx = logical::CreateDecodingContext(
        cmd.startpoint,
        options,
        false,
        Some(WalSndPrepareWrite),
        Some(WalSndWriteData),
        Some(WalSndUpdateProgress),
    )?;

    crate::WalSndSetState(WalSndState::Catchup);

    // CopyBothResponse ('W'): overall format textual, no columns.
    pqcomm::pq_putmessage(b'W', &[0u8, 0, 0])?;
    pqcomm::pq_flush()?;

    // Start reading WAL from the oldest required WAL.
    let restart_lsn = ctx.slot.data.get().restart_lsn;
    ctx.reader.XLogBeginRead(restart_lsn);

    // Report the location after which we'll send out further commits as the
    // current sentPtr; shared memory gets the restart position.
    crate::SENT_PTR.with(|c| c.set(ctx.slot.data.get().confirmed_flush));
    crate::my_set_sentptr(restart_lsn);

    crate::REPLICATION_ACTIVE.with(|c| c.set(true));

    if syncrep_seams::sync_rep_init_config::is_installed() {
        syncrep_seams::sync_rep_init_config::call()?;
    }

    let mut routine = LogicalWalSndPageRead;
    let r = WalSndLoop(&mut |()| XLogSendLogical(&mut ctx, &mut routine));

    let free_r = ctx.free();
    let rel_r = slot::ReplicationSlotRelease();
    crate::REPLICATION_ACTIVE.with(|c| c.set(false));
    r?;
    free_r?;
    rel_r?;

    if crate::GOT_STOPPING.with(|c| c.get()) {
        proc_exit(0);
    }
    crate::WalSndSetState(WalSndState::Startup);

    // Get out of COPY mode (CommandComplete "COPY 0"); the dispatch loop adds
    // the START_STREAMING completion, as in C.
    tcop_dest::EndReplicationCommand(b"COPY 0")?;
    Ok(())
}

// XLogSendLogical (walsender.c:3419).
fn XLogSendLogical(
    ctx: &mut LogicalDecodingContext,
    routine: &mut LogicalWalSndPageRead,
) -> PgResult<()> {
    // We'll set WalSndCaughtUp in WalSndWaitForWal if we actually wait, or
    // below when EndRecPtr passes the flush pointer.
    crate::WAL_SND_CAUGHT_UP.with(|c| c.set(false));

    let record = ctx.reader.XLogReadRecord(routine)?;

    if record.is_none() {
        if let Some(errm) = ctx.reader.errormsg() {
            let msg = errm.to_string();
            return elog::elog(
                ERROR,
                format!("could not find record while sending logically-decoded data: {msg}"),
            );
        }
    } else {
        // Lag tracking rides WalSndUpdateProgress via the plugin write API (C
        // comment); pg_stat_replication lag is deferred like the physical path.
        logical_decode::LogicalDecodingProcessRecord(ctx)?;
        crate::SENT_PTR.with(|c| c.set(ctx.reader.v.EndRecPtr));
    }

    // Initialize/advance the cached flush pointer.
    let end = ctx.reader.v.EndRecPtr;
    let mut flush_ptr = LOGICAL_FLUSH_PTR.with(Cell::get);
    if flush_ptr == InvalidXLogRecPtr || end >= flush_ptr {
        // For cascading (standby) logical walsenders we use the replay LSN
        // instead of the flush LSN: standby decoding only processes WAL that
        // has been replayed. This matters especially at shutdown, when no
        // more WAL gets replayed and the last replayed LSN is the furthest
        // point decoding can reach (walsender.c:3461).
        flush_ptr = if crate::am_cascading_walsender_now() {
            xlogrecovery_seams::get_xlog_replay_rec_ptr::call().0
        } else {
            transam_xlog::GetFlushRecPtr(None)
        };
        LOGICAL_FLUSH_PTR.with(|c| c.set(flush_ptr));
    }

    if end >= flush_ptr {
        crate::WAL_SND_CAUGHT_UP.with(|c| c.set(true));
    }

    // Caught up + asked to stop: shut down in an orderly manner.
    if crate::WAL_SND_CAUGHT_UP.with(|c| c.get()) && crate::GOT_STOPPING.with(|c| c.get()) {
        crate::GOT_SIGUSR2.with(|c| c.set(true));
    }

    crate::my_set_sentptr(crate::SENT_PTR.with(|c| c.get()));
    Ok(())
}

// The walsender's logical WAL page reader: logical_read_xlog_page
// (walsender.c:1041) — wait for the WAL to be flushed (processing client
// replies meanwhile), then read it locally.
struct LogicalWalSndPageRead;

impl XLogSegmentRoutine for LogicalWalSndPageRead {
    fn segment_open(
        &mut self,
        v: &mut ReaderView,
        next_seg_no: XLogSegNo,
        tli: &mut TimeLineID,
    ) -> PgResult<()> {
        xlogutils::wal_segment_open(v, next_seg_no, tli)
    }
    fn segment_close(&mut self, v: &mut ReaderView) {
        xlogutils::wal_segment_close(v);
    }
}

impl XLogReaderRoutine for LogicalWalSndPageRead {
    fn page_read(
        &mut self,
        v: &mut ReaderView,
        target_page_ptr: XLogRecPtr,
        req_len: i32,
        _target_rec_ptr: XLogRecPtr,
        cur_page: &mut [u8],
    ) -> PgResult<i32> {
        let mut flushptr = WalSndWaitForWal(target_page_ptr + req_len as u64)?;

        // Fail if not enough WAL (implies we are going to shut down).
        if flushptr < target_page_ptr + req_len as u64 {
            return Ok(-1);
        }

        // Logical decoding is also permitted on a standby, so re-check
        // whether the server is in recovery to decide how to get the current
        // timeline ID — covering promotion and timeline-change cases. This
        // must happen AFTER waiting for the required WAL so it is correct
        // when the walsender wakes up after a promotion (walsender.c:1060).
        let am_cascading = transam_xlog::RecoveryInProgress();
        crate::AM_CASCADING_WALSENDER.set(am_cascading);
        // A slot restarting before a promotion reads pages from a HISTORIC
        // timeline: XLogReadDetermineTimeline picks it, the read is clamped
        // to the switch point, and the read goes to the old timeline's
        // segment (C delegates the same decision to WalSndSegmentOpen via
        // state->currTLI; the local-read guts here mirror
        // read_local_xlog_page, which 010's SQL path already proves).
        let curr_tli = if am_cascading {
            xlogrecovery_seams::get_xlog_replay_rec_ptr::call().1
        } else {
            transam_xlog::ctl::GetWALInsertionTimeLine()
        };
        xlogutils::XLogReadDetermineTimeline(v, target_page_ptr, req_len as u32, curr_tli)?;
        let read_tli = if v.currTLI != curr_tli {
            // Historical timeline: read only up to the switch point.
            flushptr = v.currTLIValidUntil;
            v.currTLI
        } else {
            curr_tli
        };

        let count: i32 = if target_page_ptr + transam_xlog::XLOG_BLCKSZ as u64 <= flushptr {
            transam_xlog::XLOG_BLCKSZ as i32
        } else if target_page_ptr + req_len as u64 > flushptr {
            return Ok(-1);
        } else {
            (flushptr - target_page_ptr) as i32
        };

        if let Err(_errinfo) = xlogreader_seams::wal_read::call(
            v,
            &mut cur_page[..count as usize],
            target_page_ptr,
            count as usize,
            read_tli,
        )? {
            ereport(ERROR)
                .errcode_for_file_access()
                .errmsg("could not read from WAL: requested WAL segment slice is unavailable")
                .finish(ErrorLocation::new(file!(), line!() as i32, "WALReadRaiseError"))?;
        }

        // The segment might have been recycled while we read it.
        let segsize = transam_xlog::wal_segment_size() as u64;
        transam_xlog::CheckXLogRemoved(target_page_ptr / segsize, v.seg.ws_tli)?;

        Ok(count)
    }
}

// WalSndWaitForWal (walsender.c:1813): wait for loc to be flushed, processing
// client replies and keepalives while waiting; with a logical failover slot,
// also wait for the synchronized_standby_slots standbys to confirm receipt
// (NeedToWaitForStandbys).

// NeedToWaitForStandbys (walsender.c:1753): true when the acquired slot is a
// logical failover slot mid-streaming and the listed standbys have not caught
// up to flushed_lsn. After a shutdown signal, dropped/invalidated/inactive
// slots raise ERROR instead of WARNING so the walsender cannot wait forever.
fn need_to_wait_for_standbys(flushed_lsn: XLogRecPtr) -> PgResult<bool> {
    let elevel = if crate::GOT_STOPPING.with(|c| c.get()) {
        types_error::ERROR
    } else {
        types_error::WARNING
    };
    let failover_slot = crate::REPLICATION_ACTIVE.with(|c| c.get())
        && slot::MyReplicationSlot().is_some_and(|s| s.data.get().failover);
    if failover_slot && !slot::StandbySlotsHaveCaughtup(flushed_lsn, elevel)? {
        return Ok(true);
    }
    Ok(false)
}
fn WalSndWaitForWal(loc_: XLogRecPtr) -> PgResult<XLogRecPtr> {
    // Fast path: enough WAL already known to be flushed.
    let recent = RECENT_FLUSH_PTR.with(Cell::get);
    if recent != InvalidXLogRecPtr && loc_ <= recent {
        return Ok(recent);
    }

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

        // If we're shutting down, trigger pending WAL to be written out so we
        // don't wait for WAL the walwriter will never write.
        if crate::GOT_STOPPING.with(|c| c.get()) {
            let mut pacing = WAL_FLUSH_PACING.with(Cell::get);
            transam_xlog::XLogBackgroundFlush(&mut pacing)?;
            WAL_FLUSH_PACING.with(|c| c.set(pacing));
        }

        // Update our idea of the currently flushed position: on a standby
        // WAL is decodable only once REPLAYED, so the wait target is the
        // replay pointer, not the (local, stale-in-recovery) flush pointer
        // (walsender.c:1869).
        let recent_flush = if !transam_xlog::RecoveryInProgress() {
            transam_xlog::GetFlushRecPtr(None)
        } else {
            xlogrecovery_seams::get_xlog_replay_rec_ptr::call().0
        };
        RECENT_FLUSH_PTR.with(|c| c.set(recent_flush));

        // If postmaster asked us to stop and the standby slots have caught
        // up to the flushed position, don't wait anymore (walsender.c:1893).
        let mut wait_for_standby_at_stop = false;
        if crate::GOT_STOPPING.with(|c| c.get()) {
            if need_to_wait_for_standbys(recent_flush)? {
                wait_for_standby_at_stop = true;
            } else {
                break;
            }
        }

        // We only send regular messages for full decoded transactions, but
        // sync rep / walsender shutdown / a client endpos may wait for a
        // later location: BEFORE the enough-WAL break, ping with the flush
        // location (walsender.c:1896). An otherwise-idle receiver replies —
        // and pg_recvlogical --endpos exits on walEnd >= endpos, which is how
        // a rerun from the confirmed position returns nothing (006 test 8).
        let sent = crate::SENT_PTR.with(|c| c.get());
        if crate::my_flush() < sent
            && crate::my_write() < sent
            && !crate::WAITING_FOR_PING_RESPONSE.with(|c| c.get())
        {
            crate::streaming::WalSndKeepalive(false, InvalidXLogRecPtr)?;
        }

        // Exit if already caught up and not waiting for standby slots
        // (NeedToWaitForWal, walsender.c:1926). Track WHY we wait so the
        // sleep below arms the right condition variable.
        let mut wait_event = WAIT_EVENT_WAL_SENDER_WAIT_WAL;
        if !wait_for_standby_at_stop && loc_ <= recent_flush {
            if need_to_wait_for_standbys(recent_flush)? {
                wait_event = crate::WAIT_EVENT_WAIT_FOR_STANDBY_CONFIRMATION;
            } else {
                break;
            }
        } else if wait_for_standby_at_stop {
            wait_event = crate::WAIT_EVENT_WAIT_FOR_STANDBY_CONFIRMATION;
        }

        // Waiting for new WAL: by definition caught up.
        crate::WAL_SND_CAUGHT_UP.with(|c| c.set(true));

        if pqcomm::pq_flush_if_writable()? != 0 {
            WalSndShutdown();
        }

        // Streaming shut down mid-wait: bail so the loop can exit.
        if crate::STREAMING_DONE_SENDING.with(|c| c.get())
            && crate::STREAMING_DONE_RECEIVING.with(|c| c.get())
        {
            break;
        }

        WalSndCheckTimeOut();

        // Send keepalive if the time has come.
        WalSndKeepaliveIfNecessary()?;

        let now = get_ts();
        let sleeptime: c_long = WalSndComputeSleeptime(now);

        let mut wake_events: u32 = 0;
        if !crate::STREAMING_DONE_RECEIVING.with(|c| c.get()) {
            wake_events |= WL_SOCKET_READABLE;
        }
        if pqcomm::pq_is_send_pending() {
            wake_events |= WL_SOCKET_WRITEABLE;
        }

        WalSndWait(wake_events, sleeptime, wait_event)?;
    }

    // Reactivate the latch so WalSndLoop knows to continue.
    if let Some(l) = init_small::globals::MyLatch() {
        latch::SetLatch(l);
    }
    Ok(RECENT_FLUSH_PTR.with(Cell::get))
}

// WalSndPrepareWrite (walsender.c:1540): stage the 'w' header fields. The
// payload body accumulates in ctx.out; the header is prepended in
// WalSndWriteData ('w' dataStart walEnd sendtime payload).
fn WalSndPrepareWrite(
    opc: &mut OutputPluginContext,
    lsn: XLogRecPtr,
    _xid: TransactionId,
    last_write: bool,
) -> PgResult<()> {
    // Don't confuse sync rep by sending the same LSN several times.
    let lsn = if last_write { lsn } else { InvalidXLogRecPtr };
    PENDING_WRITE_LSN.with(|c| c.set(lsn));
    opc.out.clear();
    Ok(())
}

// WalSndWriteData (walsender.c:1567): frame + send the prepared data,
// processing replies and timeouts if the send backs up.
fn WalSndWriteData(
    opc: &mut OutputPluginContext,
    _lsn: XLogRecPtr,
    _xid: TransactionId,
    _last_write: bool,
) -> PgResult<()> {
    let hdr_lsn = PENDING_WRITE_LSN.with(Cell::get);
    let now = get_ts();

    crate::OUTPUT_MESSAGE.with(|b| {
        let mut b = b.borrow_mut();
        b.clear();
        b.push(b'w');
        b.extend_from_slice(&hdr_lsn.to_be_bytes()); // dataStart
        b.extend_from_slice(&hdr_lsn.to_be_bytes()); // walEnd
        b.extend_from_slice(&(now as i64).to_be_bytes()); // sendtime (taken late)
        b.extend_from_slice(opc.out.as_bytes());
    });
    crate::OUTPUT_MESSAGE.with(|b| pqcomm::pq_putmessage_noblock(b'd', &b.borrow()))?;

    postgres_seams::check_for_interrupts::call()?;

    if pqcomm::pq_flush_if_writable()? != 0 {
        WalSndShutdown();
    }

    // Fast path unless we're close to the walsender timeout with output pending.
    let last_reply = crate::LAST_REPLY_TIMESTAMP.with(|c| c.get());
    let wal_sender_timeout = guc_tables::vars::wal_sender_timeout.read() as i64;
    if now < last_reply + (wal_sender_timeout / 2) * 1000 && !pqcomm::pq_is_send_pending() {
        return Ok(());
    }

    ProcessPendingWrites()
}

// ProcessPendingWrites (walsender.c:1607): wait until there is no pending
// write, processing replies and timeouts meanwhile.
fn ProcessPendingWrites() -> PgResult<()> {
    loop {
        crate::replies::ProcessRepliesIfAny()?;
        WalSndCheckTimeOut();
        WalSndKeepaliveIfNecessary()?;

        if !pqcomm::pq_is_send_pending() {
            break;
        }

        let sleeptime = WalSndComputeSleeptime(get_ts());
        WalSndWait(
            WL_SOCKET_WRITEABLE | WL_SOCKET_READABLE,
            sleeptime,
            WAIT_EVENT_WAL_SENDER_WRITE_DATA,
        )?;

        reset_my_latch();
        postgres_seams::check_for_interrupts::call()?;

        if interrupt::ConfigReloadPending() {
            interrupt::SetConfigReloadPending(false);
            guc_file::ProcessConfigFile(types_guc::GucContext::PGC_SIGHUP)?;
            if syncrep_seams::sync_rep_init_config::is_installed() {
                syncrep_seams::sync_rep_init_config::call()?;
            }
        }

        if pqcomm::pq_flush_if_writable()? != 0 {
            WalSndShutdown();
        }
    }

    // Reactivate the latch so WalSndLoop knows to continue.
    if let Some(l) = init_small::globals::MyLatch() {
        latch::SetLatch(l);
    }
    Ok(())
}

// WalSndUpdateProgress (walsender.c:1663): lag tracking is deferred (as on the
// physical path); keep the skipped-transaction keepalive behavior so an idle
// downstream still acks progress.
fn WalSndUpdateProgress(
    _opc: &mut OutputPluginContext,
    _lsn: XLogRecPtr,
    _xid: TransactionId,
    _skipped_xact: bool,
) -> PgResult<()> {
    WalSndKeepaliveIfNecessary()?;
    if pqcomm::pq_is_send_pending() {
        ProcessPendingWrites()?;
    }
    Ok(())
}
