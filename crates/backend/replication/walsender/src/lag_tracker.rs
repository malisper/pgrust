//! The replication-lag tracker over the cyclic `WalTimeSample` buffer.
//!
//! 1:1 port of `LagTrackerWrite` / `LagTrackerRead` from `walsender.c`.  Both
//! operate on the process-local `lag_tracker` (`WalSndProc::lag_tracker`) over
//! owned values — no raw pointers.

#![allow(non_snake_case)]

use core::ffi::c_int;

use crate::core::{
    with_proc, TimeOffset, TimestampTz, WalTimeSample, XLogRecPtr, LAG_TRACKER_BUFFER_SIZE,
    NUM_SYNC_REP_WAIT_MODE,
};

/// `static void LagTrackerWrite(XLogRecPtr lsn, TimestampTz local_flush_time)`.
pub fn LagTrackerWrite(lsn: XLogRecPtr, local_flush_time: TimestampTz) {
    with_proc(|p| {
        if !p.am_walsender {
            return;
        }

        let lt = match p.lag_tracker.as_mut() {
            Some(lt) => lt,
            None => return,
        };

        // If the lsn hasn't advanced since last time, do nothing — only record a
        // new sample when new WAL has been written.
        if lt.last_lsn == lsn {
            return;
        }
        lt.last_lsn = lsn;

        // If advancing the write head would crash into a read head, the buffer is
        // full; the slowest reader (presumably apply) controls release of space.
        let new_write_head = (lt.write_head + 1) % LAG_TRACKER_BUFFER_SIZE as c_int;
        let mut i: c_int = 0;
        while i < NUM_SYNC_REP_WAIT_MODE as c_int {
            if new_write_head == lt.read_heads[i as usize] {
                lt.overflowed[i as usize] = lt.buffer[lt.read_heads[i as usize] as usize];
                lt.read_heads[i as usize] = -1;
            }
            i += 1;
        }

        // Store a sample at the current write head position.
        lt.buffer[lt.write_head as usize].lsn = lsn;
        lt.buffer[lt.write_head as usize].time = local_flush_time;
        lt.write_head = new_write_head;
    })
}

/// `static TimeOffset LagTrackerRead(int head, XLogRecPtr lsn, TimestampTz now)`.
pub fn LagTrackerRead(head: c_int, lsn: XLogRecPtr, now: TimestampTz) -> TimeOffset {
    with_proc(|p| {
        let lt = p
            .lag_tracker
            .as_mut()
            .expect("LagTrackerRead before LagTracker init");
        let head = head as usize;
        let mut time: TimestampTz = 0;

        if lt.read_heads[head] == -1 {
            if lt.overflowed[head].lsn > lsn {
                return if now >= lt.overflowed[head].time {
                    now - lt.overflowed[head].time
                } else {
                    -1
                };
            }

            time = lt.overflowed[head].time;
            lt.last_read[head] = lt.overflowed[head];
            lt.read_heads[head] = (lt.write_head + 1) % LAG_TRACKER_BUFFER_SIZE as c_int;
        }

        // Read all unread samples up to this LSN or end of buffer.
        while lt.read_heads[head] != lt.write_head
            && lt.buffer[lt.read_heads[head] as usize].lsn <= lsn
        {
            time = lt.buffer[lt.read_heads[head] as usize].time;
            lt.last_read[head] = lt.buffer[lt.read_heads[head] as usize];
            lt.read_heads[head] = (lt.read_heads[head] + 1) % LAG_TRACKER_BUFFER_SIZE as c_int;
        }

        // If the tracker is empty, the standby processed everything we've sent;
        // clear 'last_read' to avoid using a stale sample for interpolation.
        if lt.read_heads[head] == lt.write_head {
            lt.last_read[head].time = 0;
        }

        if time > now {
            // The clock went backwards; treat as not found.
            return -1;
        } else if time == 0 {
            if lt.read_heads[head] == lt.write_head {
                // No future samples, so we can't interpolate.
                return -1;
            } else if lt.last_read[head].time != 0 {
                // Interpolate between last_read and the next sample.
                let prev: WalTimeSample = lt.last_read[head];
                let next: WalTimeSample = lt.buffer[lt.read_heads[head] as usize];

                if lsn < prev.lsn {
                    return -1;
                }

                debug_assert!(prev.lsn < next.lsn);

                if prev.time > next.time {
                    return -1;
                }

                let fraction: f64 = (lsn - prev.lsn) as f64 / (next.lsn - prev.lsn) as f64;
                time =
                    (prev.time as f64 + (next.time - prev.time) as f64 * fraction) as TimestampTz;
            } else {
                // Only a future sample: report the hypothetical lag.
                time = lt.buffer[lt.read_heads[head] as usize].time;
            }
        }

        debug_assert!(time != 0);
        now - time
    })
}
