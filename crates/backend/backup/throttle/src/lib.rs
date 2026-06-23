//! Port of PostgreSQL's base-backup throttling sink
//! (`src/backend/backup/basebackup_throttle.c`, PostgreSQL 18.3).
//!
//! This [`Bbsink`] forwards data to the next base-backup sink in the chain at
//! a rate no greater than the configured maximum. Data is accounted in a
//! running counter; once a *sample*'s worth of bytes has passed through, the
//! sink sleeps (on `MyLatch`, via `WaitLatch`) for however long is needed so
//! that the elapsed wall-clock time is at least the minimum required to have
//! transferred that many bytes at the requested rate.
//!
//! # What is ported in-crate vs. seamed
//!
//! All of the throttling arithmetic and bookkeeping — the sample size, the
//! minimum-elapsed-time unit, the running counter, the elapsed-time / sleep
//! computation, and the wait loop structure — is ported 1:1 in-crate over the
//! owned [`BbsinkOps`] trait. The cross-subsystem primitives the C code calls
//! are seamed:
//!
//! * `GetCurrentTimestamp()` and `CHECK_FOR_INTERRUPTS()` are declared in this
//!   crate's own outward-seam crate
//!   ([`throttle_seams`]); they have no ported owner yet.
//! * `WaitLatch(MyLatch, …)` and `ResetLatch(MyLatch)` are consumed from
//!   [`latch_seams`]; the latch unit
//!   (`storage/ipc/latch.c`) has landed and installs them.
//!
//! # The C `bbsink_throttle` struct
//!
//! ```c
//! typedef struct bbsink_throttle
//! {
//!     bbsink      base;
//!     uint64      throttling_sample;
//!     int64       throttling_counter;
//!     TimeOffset  elapsed_min_unit;
//!     TimestampTz throttled_last;
//! } bbsink_throttle;
//! ```
//!
//! The `base` member (the forwarding chain and working buffer) is owned by the
//! surrounding [`Bbsink`] this is installed into; [`BbsinkThrottle`] carries
//! the remaining four throttling fields.

#![no_std]
#![forbid(unsafe_code)]

extern crate alloc;

use alloc::boxed::Box;

use sink::{
    bbsink_forward_archive_contents, bbsink_forward_begin_archive, bbsink_forward_begin_backup,
    bbsink_forward_begin_manifest, bbsink_forward_cleanup, bbsink_forward_end_archive,
    bbsink_forward_end_backup, bbsink_forward_end_manifest, bbsink_forward_manifest_contents,
    Bbsink, BbsinkOps, BbsinkState,
};
use throttle_seams::{check_for_interrupts, get_current_timestamp};
use latch_seams::{reset_latch_my_latch, wait_latch_my_latch};
use ::mcx::Mcx;
use ::types_core::primitive::{Size, TimeLineID, TimestampTz, XLogRecPtr};
use types_datetime::{TimeOffset, USECS_PER_SEC};
use ::types_error::PgResult;
use ::types_pgstat::wait_event::WAIT_EVENT_BASE_BACKUP_THROTTLE;
use ::types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};

/// How frequently to throttle, as a fraction of the specified rate-second
/// (C `#define THROTTLING_FREQUENCY 8`).
const THROTTLING_FREQUENCY: i64 = 8;

/// The base-backup throttling sink (C `bbsink_throttle`).
///
/// The forwarding chain and working buffer are owned by the surrounding
/// [`Bbsink`] this is installed into; this struct carries the throttling
/// bookkeeping. Construct the sink with [`bbsink_throttle_new`].
#[derive(Debug, Clone, Copy)]
pub struct BbsinkThrottle {
    /// The actual number of bytes, transfer of which may cause sleep
    /// (C `uint64 throttling_sample`).
    throttling_sample: u64,
    /// Amount of data already transferred but not yet throttled
    /// (C `int64 throttling_counter`).
    throttling_counter: i64,
    /// The minimum time required to transfer `throttling_sample` bytes
    /// (C `TimeOffset elapsed_min_unit`).
    elapsed_min_unit: TimeOffset,
    /// The last check of the transfer rate (C `TimestampTz throttled_last`).
    throttled_last: TimestampTz,
}

/// Create a new basebackup sink that performs throttling and forwards data to
/// a successor sink.
///
/// Mirrors C `bbsink *bbsink_throttle_new(bbsink *next, uint32 maxrate)`.
///
/// `next` is the successor sink to which everything is forwarded
/// (`Assert(next != NULL)` is implicit in the owned `Box<Bbsink>`). `maxrate`
/// is the maximum transfer rate in kilobytes per second (`Assert(maxrate > 0)`
/// in C; the caller guarantees this). `mcx` is the surrounding memory context
/// the new sink is allocated into (the C `palloc0`).
pub fn bbsink_throttle_new<'mcx>(
    mcx: Mcx<'mcx>,
    next: Box<Bbsink<'mcx>>,
    maxrate: u32,
) -> Box<Bbsink<'mcx>> {
    // The number of bytes after which we consider throttling. (C casts maxrate
    // to int64, multiplies by 1024, and divides by THROTTLING_FREQUENCY.)
    let throttling_sample = ((maxrate as i64) * 1024 / THROTTLING_FREQUENCY) as u64;

    // The minimum amount of time for throttling_sample bytes to be transferred.
    let elapsed_min_unit = USECS_PER_SEC / THROTTLING_FREQUENCY;

    let throttle = BbsinkThrottle {
        throttling_sample,
        throttling_counter: 0,
        elapsed_min_unit,
        // Set when streaming begins, in begin_backup.
        throttled_last: 0,
    };

    Box::new(Bbsink::new(mcx, Box::new(throttle), Some(next)))
}

impl BbsinkThrottle {
    /// Increment the network transfer counter by the given number of bytes,
    /// and sleep if necessary to comply with the requested network transfer
    /// rate (C `static void throttle(bbsink_throttle *sink, size_t increment)`).
    fn throttle(&mut self, increment: Size) -> PgResult<()> {
        debug_assert!(self.throttling_counter >= 0);

        self.throttling_counter += increment as i64;
        if (self.throttling_counter as u64) < self.throttling_sample {
            return Ok(());
        }

        // How much time should have elapsed at minimum?
        let elapsed_min: TimeOffset = self.elapsed_min_unit
            * (self.throttling_counter / self.throttling_sample as i64);

        // Since the latch could be set repeatedly because of concurrently WAL
        // activity, sleep in a loop to ensure enough time has passed.
        loop {
            // Time elapsed since the last measurement (and possible wake up).
            let elapsed: TimeOffset = get_current_timestamp::call()? - self.throttled_last;

            // Sleep if the transfer is faster than it should be.
            let sleep: TimeOffset = elapsed_min - elapsed;
            if sleep <= 0 {
                break;
            }

            reset_latch_my_latch::call();

            // We're eating a potentially set latch, so check for interrupts.
            check_for_interrupts::call()?;

            // (TAR_SEND_SIZE / throttling_sample * elapsed_min_unit) should be
            // the maximum time to sleep. Thus the cast to long is safe.
            let wait_result = wait_latch_my_latch::call(
                WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                (sleep / 1000) as i64,
                WAIT_EVENT_BASE_BACKUP_THROTTLE,
            )?;

            if wait_result & WL_LATCH_SET != 0 {
                check_for_interrupts::call()?;
            }

            // Done waiting?
            if wait_result & WL_TIMEOUT != 0 {
                break;
            }
        }

        // As we work with integers, only whole multiple of throttling_sample
        // was processed. The rest will be done during the next call of this
        // function.
        self.throttling_counter %= self.throttling_sample as i64;

        // Time interval for the remaining amount and possible next increments
        // starts now.
        self.throttled_last = get_current_timestamp::call()?;

        Ok(())
    }
}

impl<'mcx> BbsinkOps<'mcx> for BbsinkThrottle {
    /// Record the current time so that it can be used for future calculations
    /// (C `bbsink_throttle_begin_backup`).
    ///
    /// There's no real work to do here, but we need to record the current time.
    fn begin_backup(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        bbsink_forward_begin_backup(sink, state)?;

        // The 'real data' starts now (header was ignored).
        self.throttled_last = get_current_timestamp::call()?;
        Ok(())
    }

    /// First throttle, and then pass archive contents to next sink
    /// (C `bbsink_throttle_archive_contents`).
    fn archive_contents(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()> {
        self.throttle(len)?;

        bbsink_forward_archive_contents(sink, state, len)
    }

    /// First throttle, and then pass manifest contents to next sink
    /// (C `bbsink_throttle_manifest_contents`).
    fn manifest_contents(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()> {
        self.throttle(len)?;

        bbsink_forward_manifest_contents(sink, state, len)
    }

    // The remaining callbacks pure-forward (C `bbsink_forward_*` entries in
    // `bbsink_throttle_ops`).

    fn begin_archive(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        archive_name: &str,
    ) -> PgResult<()> {
        bbsink_forward_begin_archive(sink, state, archive_name)
    }

    fn end_archive(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        bbsink_forward_end_archive(sink, state)
    }

    fn begin_manifest(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        bbsink_forward_begin_manifest(sink, state)
    }

    fn end_manifest(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        bbsink_forward_end_manifest(sink, state)
    }

    fn end_backup(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        endptr: XLogRecPtr,
        endtli: TimeLineID,
    ) -> PgResult<()> {
        bbsink_forward_end_backup(sink, state, endptr, endtli)
    }

    fn cleanup(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        bbsink_forward_cleanup(sink, state)
    }
}

/// Install this crate's seams. The throttle sink is a vtable leaf consumed
/// directly (via the free function [`bbsink_throttle_new`]) by the unported
/// `basebackup.c`, so it has no inward seams of its own to set; it is
/// registered in `seams-init::init_all` for uniformity, mirroring the sibling
/// sinks.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
