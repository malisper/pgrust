//! The walsender main streaming loop, WAL-wait machinery, and the
//! keepalive/timeout/sleep helpers.
//!
//! 1:1 port of `WalSndLoop`, `WalSndDone`, `WalSndWaitForWal`,
//! `NeedToWaitForStandbys`, `NeedToWaitForWal`, `WalSndComputeSleeptime`,
//! `WalSndCheckTimeOut`, `WalSndWait`, `WalSndKeepalive`,
//! `WalSndKeepaliveIfNecessary`.  The control flow / arithmetic is ported over
//! owned state; the libpq send-side primitives and the FeBe wait-event set are
//! reached through their owner seams.

#![allow(non_snake_case)]

use core::ffi::c_long;

use crate::core::{
    proc_get, with_proc, ReplicationKind, TimestampTz, WalSndSendDataCallback, WalSndState,
    XLogRecPtr, InvalidXLogRecPtr, WALSENDER_STATS_FLUSH_INTERVAL,
};
use crate::{condvar, pgstat_io, timestamp, xlog, xlogrecovery};

// Wait-event identifiers (utils/wait_event_types.h).
const WAIT_EVENT_WAIT_FOR_STANDBY_CONFIRMATION: u32 = 0x06000000 | 6;
const WAIT_EVENT_WAL_SENDER_WAIT_FOR_WAL: u32 = 0x06000000 | 7;
const WAIT_EVENT_WAL_SENDER_MAIN: u32 = 0x05000000 | 15;

// Latch / socket wakeup bits (storage/waiteventset.h).
const WL_SOCKET_READABLE: u32 = 1 << 1;
const WL_SOCKET_WRITEABLE: u32 = 1 << 2;
const WL_POSTMASTER_DEATH: u32 = 1 << 4;

// elevels (utils/elog.h).
const ERROR: i32 = 21;
const WARNING: i32 = 19;

#[inline]
fn timestamptz_plus_ms(tz: TimestampTz, ms: i64) -> TimestampTz {
    tz + ms * 1000
}

/// `static void WalSndLoop(WalSndSendDataCallback send_data)`.
pub fn WalSndLoop(send_data: WalSndSendDataCallback) {
    let mut last_flush: TimestampTz = 0;

    // Initialize the last reply timestamp; that enables timeout processing.
    let now0 = timestamp::get_current_timestamp::call();
    with_proc(|p| {
        p.last_reply_timestamp = now0;
        p.waiting_for_ping_response = false;
    });

    loop {
        crate::reset_latch_my_latch_loop();

        crate::check_for_interrupts();

        if crate::config_reload_pending() {
            crate::clear_config_reload_pending();
            crate::process_config_file_sighup();
            crate::sync_rep_init_config();
        }

        crate::replies::ProcessRepliesIfAny();

        // If we received CopyDone from the client, sent CopyDone ourselves, and
        // the output buffer is empty, it's time to exit streaming.
        let (done_recv, done_send) =
            proc_get(|p| (p.streamingDoneReceiving, p.streamingDoneSending));
        if done_recv && done_send && !crate::pq_is_send_pending() {
            break;
        }

        // If we have no pending output, try to send more; otherwise assume we're
        // not caught up.
        if !crate::pq_is_send_pending() {
            send_data();
        } else {
            with_proc(|p| p.WalSndCaughtUp = false);
        }

        if crate::pq_flush_if_writable() != 0 {
            crate::init::WalSndShutdown();
        }

        let caught_up = proc_get(|p| p.WalSndCaughtUp);
        if caught_up && !crate::pq_is_send_pending() {
            if crate::shmem_array::my_state() == WalSndState::WALSNDSTATE_CATCHUP {
                crate::init::WalSndSetState(WalSndState::WALSNDSTATE_STREAMING);
            }

            // When SIGUSR2 arrives, send any outstanding logs up to the shutdown
            // checkpoint, wait for replication, and exit.
            if proc_get(|p| p.got_SIGUSR2) != 0 {
                WalSndDone(send_data);
            }
        }

        WalSndCheckTimeOut();
        WalSndKeepaliveIfNecessary();

        // Block if we have unsent data.  (For logical replication, let
        // WalSndWaitForWal handle blocking.  For physical, also block if caught
        // up; its send_data does not block.)
        let caught_up = proc_get(|p| p.WalSndCaughtUp);
        let done_send = proc_get(|p| p.streamingDoneSending);
        let is_logical = core::ptr::fn_addr_eq(
            send_data,
            crate::logical::XLogSendLogical as WalSndSendDataCallback,
        );
        if (caught_up && !is_logical && !done_send) || crate::pq_is_send_pending() {
            let mut wakeEvents: u32 = if !proc_get(|p| p.streamingDoneReceiving) {
                WL_SOCKET_READABLE
            } else {
                0
            };

            let now = timestamp::get_current_timestamp::call();
            let sleeptime = WalSndComputeSleeptime(now);

            if crate::pq_is_send_pending() {
                wakeEvents |= WL_SOCKET_WRITEABLE;
            }

            if timestamp::timestamp_difference_exceeds::call(
                last_flush,
                now,
                WALSENDER_STATS_FLUSH_INTERVAL,
            ) {
                pgstat_io::pgstat_flush_io::call(false);
                pgstat_io::pgstat_flush_backend_io::call(false);
                last_flush = now;
            }

            WalSndWait(wakeEvents, sleeptime, WAIT_EVENT_WAL_SENDER_MAIN);
        }
    }
}

/// `static void WalSndDone(WalSndSendDataCallback send_data)`.
pub fn WalSndDone(send_data: WalSndSendDataCallback) {
    // ... let's be real sure we're caught up ...
    send_data();

    // To figure out whether all WAL has been replicated, check flush location if
    // valid, write otherwise.
    let replicatedPtr: XLogRecPtr = if crate::shmem_array::my_flush() == InvalidXLogRecPtr {
        crate::shmem_array::my_write()
    } else {
        crate::shmem_array::my_flush()
    };

    let (caught_up, sent) = proc_get(|p| (p.WalSndCaughtUp, p.sentPtr));
    if caught_up && sent == replicatedPtr && !crate::pq_is_send_pending() {
        // Inform the standby that XLOG streaming is done: flush and exit.
        crate::pq_flush();
        crate::proc_exit(0);
    }
    if !proc_get(|p| p.waiting_for_ping_response) {
        WalSndKeepalive(true, InvalidXLogRecPtr);
    }
}

/// `static XLogRecPtr WalSndWaitForWal(XLogRecPtr loc)`.
pub fn WalSndWaitForWal(loc: XLogRecPtr) -> XLogRecPtr {
    let mut wait_event: u32 = 0;
    let mut last_flush: TimestampTz = 0;

    // Fast path.
    let recent = proc_get(|p| p.WalSndWaitForWal_RecentFlushPtr);
    if recent != InvalidXLogRecPtr && !NeedToWaitForWal(loc, recent, &mut wait_event) {
        return recent;
    }

    loop {
        let mut wait_for_standby_at_stop = false;

        crate::reset_latch_my_latch_loop();
        crate::check_for_interrupts();

        if crate::config_reload_pending() {
            crate::clear_config_reload_pending();
            crate::process_config_file_sighup();
            crate::sync_rep_init_config();
        }

        crate::replies::ProcessRepliesIfAny();

        // If we're shutting down, trigger pending WAL to be written out.
        if proc_get(|p| p.got_STOPPING) != 0 {
            xlog::xlog_background_flush::call();
        }

        // Update our idea of the flushed position only if not waiting for
        // standbys to catch up.
        if wait_event != WAIT_EVENT_WAIT_FOR_STANDBY_CONFIRMATION {
            let flush = if !xlog::recovery_in_progress::call() {
                xlog::get_flush_rec_ptr::call().0
            } else {
                xlogrecovery::get_xlog_replay_rec_ptr::call().0
            };
            with_proc(|p| p.WalSndWaitForWal_RecentFlushPtr = flush);
        }

        let recent = proc_get(|p| p.WalSndWaitForWal_RecentFlushPtr);

        if proc_get(|p| p.got_STOPPING) != 0 {
            if NeedToWaitForStandbys(recent, &mut wait_event) {
                wait_for_standby_at_stop = true;
            } else {
                break;
            }
        }

        // Before sleeping, send a ping containing the flush location so an idle
        // receiver replies and updates the MyWalSnd locations.
        let sent = proc_get(|p| p.sentPtr);
        if crate::shmem_array::my_flush() < sent
            && crate::shmem_array::my_write() < sent
            && !proc_get(|p| p.waiting_for_ping_response)
        {
            WalSndKeepalive(false, InvalidXLogRecPtr);
        }

        // Exit if already caught up and we don't need standby slots.
        if !wait_for_standby_at_stop && !NeedToWaitForWal(loc, recent, &mut wait_event) {
            break;
        }

        // Waiting for new WAL or for standbys to catch up: we're now caught up.
        with_proc(|p| p.WalSndCaughtUp = true);

        if crate::pq_flush_if_writable() != 0 {
            crate::init::WalSndShutdown();
        }

        let (done_recv, done_send) =
            proc_get(|p| (p.streamingDoneReceiving, p.streamingDoneSending));
        if done_recv && done_send && !crate::pq_is_send_pending() {
            break;
        }

        WalSndCheckTimeOut();
        WalSndKeepaliveIfNecessary();

        let now = timestamp::get_current_timestamp::call();
        let sleeptime = WalSndComputeSleeptime(now);

        let mut wakeEvents: u32 = WL_SOCKET_READABLE;
        if crate::pq_is_send_pending() {
            wakeEvents |= WL_SOCKET_WRITEABLE;
        }

        debug_assert!(wait_event != 0);

        if timestamp::timestamp_difference_exceeds::call(
            last_flush,
            now,
            WALSENDER_STATS_FLUSH_INTERVAL,
        ) {
            pgstat_io::pgstat_flush_io::call(false);
            pgstat_io::pgstat_flush_backend_io::call(false);
            last_flush = now;
        }

        WalSndWait(wakeEvents, sleeptime, wait_event);
    }

    // Reactivate latch so WalSndLoop knows to continue.
    crate::set_latch_my_latch();
    proc_get(|p| p.WalSndWaitForWal_RecentFlushPtr)
}

/// `static bool NeedToWaitForStandbys(XLogRecPtr flushed_lsn, uint32 *wait_event)`.
pub fn NeedToWaitForStandbys(flushed_lsn: XLogRecPtr, wait_event: &mut u32) -> bool {
    let elevel = if proc_get(|p| p.got_STOPPING) != 0 {
        ERROR
    } else {
        WARNING
    };

    let failover_slot =
        proc_get(|p| p.replication_active) != 0 && crate::slot::slot_failover::call();

    // After the shutdown signal, an ERROR is reported if any slots are dropped,
    // invalidated, or inactive, to prevent the walsender waiting indefinitely.
    if failover_slot
        && !crate::standby_slots_have_caughtup(flushed_lsn, elevel)
    {
        *wait_event = WAIT_EVENT_WAIT_FOR_STANDBY_CONFIRMATION;
        return true;
    }

    *wait_event = 0;
    false
}

/// `static bool NeedToWaitForWal(...)`.
pub fn NeedToWaitForWal(target_lsn: XLogRecPtr, flushed_lsn: XLogRecPtr, wait_event: &mut u32) -> bool {
    if target_lsn > flushed_lsn {
        *wait_event = WAIT_EVENT_WAL_SENDER_WAIT_FOR_WAL;
        return true;
    }
    NeedToWaitForStandbys(flushed_lsn, wait_event)
}

/// `static long WalSndComputeSleeptime(TimestampTz now)`.
pub fn WalSndComputeSleeptime(now: TimestampTz) -> c_long {
    let mut sleeptime: c_long = 10000; // 10 s

    let (timeout, last_reply, waiting) = proc_get(|p| {
        (p.wal_sender_timeout, p.last_reply_timestamp, p.waiting_for_ping_response)
    });

    if timeout > 0 && last_reply > 0 {
        let mut wakeup_time = timestamptz_plus_ms(last_reply, timeout as i64);

        // If no ping has been sent yet, wake once half of the timeout passed.
        if !waiting {
            wakeup_time = timestamptz_plus_ms(last_reply, (timeout / 2) as i64);
        }

        sleeptime =
            timestamp::timestamp_difference_milliseconds::call(now, wakeup_time) as c_long;
    }

    sleeptime
}

/// `static void WalSndCheckTimeOut(void)`.
pub fn WalSndCheckTimeOut() {
    let (timeout_guc, last_reply, last_processing) =
        proc_get(|p| (p.wal_sender_timeout, p.last_reply_timestamp, p.last_processing));

    if last_reply <= 0 {
        return;
    }

    let timeout = timestamptz_plus_ms(last_reply, timeout_guc as i64);

    if timeout_guc > 0 && last_processing >= timeout {
        // Expiration usually means a communication problem; don't send the error
        // message to the standby.
        crate::ereport_commerror_replication_timeout();
        crate::init::WalSndShutdown();
    }
}

/// `static void WalSndWait(uint32 socket_events, long timeout, uint32 wait_event)`.
pub fn WalSndWait(socket_events: u32, timeout: c_long, wait_event: u32) {
    // ModifyWaitEvent(FeBeWaitSet, FeBeWaitSetSocketPos, socket_events, NULL).
    crate::modify_fe_be_wait_set_socket(socket_events);

    // Prepare to sleep on the appropriate shared-memory CV so WalSndWakeup() can
    // wake us efficiently.  Separate CVs for physical and logical walsenders
    // allow selective wake-ups.
    let ctl = crate::core::wal_snd_ctl();
    if wait_event == WAIT_EVENT_WAIT_FOR_STANDBY_CONFIRMATION {
        condvar::condition_variable_prepare_to_sleep::call(&ctl.wal_confirm_rcv_cv);
    } else {
        match crate::shmem_array::my_kind() {
            ReplicationKind::REPLICATION_KIND_PHYSICAL => {
                condvar::condition_variable_prepare_to_sleep::call(&ctl.wal_flush_cv)
            }
            ReplicationKind::REPLICATION_KIND_LOGICAL => {
                condvar::condition_variable_prepare_to_sleep::call(&ctl.wal_replay_cv)
            }
        }
    }

    let (nevents, events) = crate::wait_event_set_wait_fe_be(timeout, wait_event);
    if nevents == 1 && (events & WL_POSTMASTER_DEATH) != 0 {
        condvar::condition_variable_cancel_sleep::call();
        crate::proc_exit(1);
    }

    condvar::condition_variable_cancel_sleep::call();
}

/// `static void WalSndKeepalive(bool requestReply, XLogRecPtr writePtr)`.
pub fn WalSndKeepalive(requestReply: bool, writePtr: XLogRecPtr) {
    let sent = proc_get(|p| p.sentPtr);
    let now = timestamp::get_current_timestamp::call();

    // Construct the message into the owned `output_message` buffer.
    crate::core::with_output_message(|b| {
        b.clear();
        b.push(b'k');
        let wpos = if writePtr == InvalidXLogRecPtr { sent } else { writePtr };
        b.extend_from_slice(&wpos.to_be_bytes());
        b.extend_from_slice(&(now as u64).to_be_bytes());
        b.push(if requestReply { 1 } else { 0 });
    });

    // ... and send it wrapped in CopyData.
    crate::pq_putmessage_noblock_output_message(b'd');

    if requestReply {
        with_proc(|p| p.waiting_for_ping_response = true);
    }
}

/// `static void WalSndKeepaliveIfNecessary(void)`.
pub fn WalSndKeepaliveIfNecessary() {
    let (timeout, last_reply, waiting, last_processing) = proc_get(|p| {
        (
            p.wal_sender_timeout,
            p.last_reply_timestamp,
            p.waiting_for_ping_response,
            p.last_processing,
        )
    });

    if timeout <= 0 || last_reply <= 0 {
        return;
    }

    if waiting {
        return;
    }

    let ping_time = timestamptz_plus_ms(last_reply, (timeout / 2) as i64);
    if last_processing >= ping_time {
        WalSndKeepalive(true, InvalidXLogRecPtr);

        if crate::pq_flush_if_writable() != 0 {
            crate::init::WalSndShutdown();
        }
    }
}
