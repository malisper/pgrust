// Replication lag tracking (walsender.c:227 LagTracker, 4177 LagTrackerWrite,
// 4235 LagTrackerRead): a circular buffer of (end-of-WAL LSN, local flush
// time) samples written as WAL is sent; standby replies read it to compute
// pg_stat_replication's write_lag / flush_lag / replay_lag. Per backend (C
// allocates it in TopMemoryContext at InitWalSender; one backend = one
// thread, init_small globals pattern), zeroed like MemoryContextAllocZero.
#![allow(non_snake_case)]

use std::cell::RefCell;

use types_core::{TimestampTz, XLogRecPtr};

use crate::NUM_SYNC_REP_WAIT_MODE;

// LAG_TRACKER_BUFFER_SIZE (walsender.c:227).
const LAG_TRACKER_BUFFER_SIZE: usize = 8192;

// WalTimeSample (walsender.c:232).
#[derive(Clone, Copy, Default, Debug)]
struct WalTimeSample {
    lsn: XLogRecPtr,
    time: TimestampTz,
}

// LagTracker (walsender.c:238).
struct LagTracker {
    buffer: Box<[WalTimeSample]>,
    write_head: i32,
    read_heads: [i32; NUM_SYNC_REP_WAIT_MODE],
    last_read: [WalTimeSample; NUM_SYNC_REP_WAIT_MODE],
    overflowed: [WalTimeSample; NUM_SYNC_REP_WAIT_MODE],
    last_lsn: XLogRecPtr,
}

thread_local! {
    // walsender.c:249 `static LagTracker *lag_tracker` — NULL until
    // InitWalSender allocates it.
    static LAG_TRACKER: RefCell<Option<Box<LagTracker>>> = const { RefCell::new(None) };
}

// InitWalSender's `lag_tracker = MemoryContextAllocZero(TopMemoryContext,
// sizeof(LagTracker))` (walsender.c:333).
pub(crate) fn LagTrackerInit() {
    LAG_TRACKER.with(|t| {
        *t.borrow_mut() = Some(Box::new(LagTracker {
            buffer: vec![WalTimeSample::default(); LAG_TRACKER_BUFFER_SIZE].into_boxed_slice(),
            write_head: 0,
            read_heads: [0; NUM_SYNC_REP_WAIT_MODE],
            last_read: [WalTimeSample::default(); NUM_SYNC_REP_WAIT_MODE],
            overflowed: [WalTimeSample::default(); NUM_SYNC_REP_WAIT_MODE],
            last_lsn: 0,
        }));
    });
}

// LagTrackerWrite (walsender.c:4177): record the end of the WAL and the time
// it was flushed locally, so that LagTrackerRead can compute the elapsed time
// (lag) when this WAL location is eventually reported to have been written,
// flushed and applied by the standby in a reply message.
pub(crate) fn LagTrackerWrite(lsn: XLogRecPtr, local_flush_time: TimestampTz) {
    if !walsender_seams::am_walsender() {
        return;
    }
    LAG_TRACKER.with(|t| {
        let mut t = t.borrow_mut();
        let Some(lt) = t.as_mut() else { return };

        // If the lsn hasn't advanced since last time, then do nothing. This
        // way we only record a new sample when new WAL has been written.
        if lt.last_lsn == lsn {
            return;
        }
        lt.last_lsn = lsn;

        // If advancing the write head of the circular buffer would crash
        // into any of the read heads, then the buffer is full. In other
        // words, the slowest reader (presumably apply) is the one that
        // controls the release of space.
        let new_write_head = (lt.write_head + 1) % LAG_TRACKER_BUFFER_SIZE as i32;
        for i in 0..NUM_SYNC_REP_WAIT_MODE {
            // If the buffer is full, move the slowest reader to a separate
            // overflow entry and free its space in the buffer so the write
            // head can advance.
            if new_write_head == lt.read_heads[i] {
                lt.overflowed[i] = lt.buffer[lt.read_heads[i] as usize];
                lt.read_heads[i] = -1;
            }
        }

        // Store a sample at the current write head position.
        let wh = lt.write_head as usize;
        lt.buffer[wh].lsn = lsn;
        lt.buffer[wh].time = local_flush_time;
        lt.write_head = new_write_head;
    })
}

// LagTrackerRead (walsender.c:4235): how much time has elapsed between the
// moment WAL location `lsn` (or the highest known earlier LSN) was flushed
// locally and the time `now`; one read head per reported position
// (SYNC_REP_WAIT_WRITE/FLUSH/APPLY). Returns -1 if no new sample data is
// available, else the elapsed time in microseconds.
pub(crate) fn LagTrackerRead(head: usize, lsn: XLogRecPtr, now: TimestampTz) -> i64 {
    LAG_TRACKER.with(|t| {
        let mut t = t.borrow_mut();
        let Some(lt) = t.as_mut() else { return -1 };
        let mut time: TimestampTz = 0;

        // If 'lsn' has not passed the WAL position stored in the overflow
        // entry, return the elapsed time since the saved local flush time
        // (-1 if that time is in the future, clock drift). Otherwise switch
        // back to using the buffer to control the read head and reset the
        // read head to the oldest entry in the buffer.
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
            lt.read_heads[head] = (lt.write_head + 1) % LAG_TRACKER_BUFFER_SIZE as i32;
        }

        // Read all unread samples up to this LSN or end of buffer.
        while lt.read_heads[head] != lt.write_head
            && lt.buffer[lt.read_heads[head] as usize].lsn <= lsn
        {
            let rh = lt.read_heads[head] as usize;
            time = lt.buffer[rh].time;
            lt.last_read[head] = lt.buffer[rh];
            lt.read_heads[head] = (lt.read_heads[head] + 1) % LAG_TRACKER_BUFFER_SIZE as i32;
        }

        // If the lag tracker is empty, the standby has processed everything
        // we've ever sent: clear 'last_read' so a stale sample is never used
        // for interpolation at the start of the next burst after idleness.
        if lt.read_heads[head] == lt.write_head {
            lt.last_read[head].time = 0;
        }

        if time > now {
            // If the clock somehow went backwards, treat as not found.
            return -1;
        } else if time == 0 {
            // We didn't cross a time. If there is a future sample that we
            // haven't reached yet, and we've already reached at least one
            // sample, interpolate the local flushed time (a stuck apply
            // position shows increasing lag).
            if lt.read_heads[head] == lt.write_head {
                // There are no future samples, so we can't interpolate.
                return -1;
            } else if lt.last_read[head].time != 0 {
                // We can interpolate between last_read and the next sample.
                let prev = lt.last_read[head];
                let next = lt.buffer[lt.read_heads[head] as usize];

                if lsn < prev.lsn {
                    // Reported LSNs shouldn't normally go backwards, but it's
                    // possible when there is a timeline change. Treat as not
                    // found.
                    return -1;
                }

                debug_assert!(prev.lsn < next.lsn);

                if prev.time > next.time {
                    // If the clock somehow went backwards, treat as not found.
                    return -1;
                }

                // See how far we are between the previous and next samples.
                let fraction = (lsn - prev.lsn) as f64 / (next.lsn - prev.lsn) as f64;

                // Scale the local flush time proportionally.
                time = (prev.time as f64 + (next.time - prev.time) as f64 * fraction) as TimestampTz;
            } else {
                // We have only a future sample: we were entirely caught up
                // and now there is a new burst of WAL the standby hasn't
                // processed the first sample of yet. Until it reaches that
                // sample the best we can do is report the hypothetical lag
                // if that sample were to be replayed now.
                time = lt.buffer[lt.read_heads[head] as usize].time;
            }
        }

        // Return the elapsed time since local flush time in microseconds.
        debug_assert!(time != 0);
        now - time
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // The buffer semantics of walsender.c:4177/4235: a reply at a sampled LSN
    // yields the elapsed time since that sample's local flush; a reply that
    // has not reached any sample interpolates / projects; an idle tracker
    // reports -1 (NULL); replies never move a read head past the write head.
    #[test]
    fn write_then_read_measures_elapsed_time_per_head() {
        walsender_seams::set_walsender_flags(false);
        LagTrackerInit();

        // Nothing sampled yet: unknown lag.
        assert_eq!(LagTrackerRead(0, 100, 1_000), -1);

        LagTrackerWrite(100, 1_000);
        LagTrackerWrite(100, 1_500); // same LSN: ignored
        LagTrackerWrite(200, 2_000);

        // Write head confirms 100 at t=1300: 300us since its flush.
        assert_eq!(LagTrackerRead(0, 100, 1_300), 300);
        // Flush head confirms 200 at t=2_700: crosses both samples, uses 200's.
        assert_eq!(LagTrackerRead(1, 200, 2_700), 700);
        // Apply head at 150 (between samples): the first read crosses sample
        // 100 (lag since its flush); the next read, still between the
        // samples, interpolates the flush time (1_500) -> 500us at t=2_000.
        assert_eq!(LagTrackerRead(2, 150, 2_000), 1_000);
        assert_eq!(LagTrackerRead(2, 150, 2_000), 500);
        // Write head reaches 200 at t=3_000; afterwards the buffer is drained
        // for it -> -1 (NULL) until new WAL is sampled.
        assert_eq!(LagTrackerRead(0, 200, 3_000), 1_000);
        assert_eq!(LagTrackerRead(0, 200, 3_500), -1);
        // A sample flushed in the future (clock skew) is not found.
        LagTrackerWrite(300, 9_000);
        assert_eq!(LagTrackerRead(0, 300, 8_000), -1);
        // Only a future sample ahead after a drain: report the hypothetical
        // lag as if that sample were replayed now.
        LagTrackerWrite(400, 9_500);
        assert_eq!(LagTrackerRead(0, 350, 9_600), 100);
    }
}
