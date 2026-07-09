//! Port of the base-backup throttling sink (`basebackup_throttle.c`).

use std::boxed::Box;

use ::sink::{
    bbsink_forward_archive_contents, bbsink_forward_begin_archive, bbsink_forward_begin_backup,
    bbsink_forward_begin_manifest, bbsink_forward_cleanup, bbsink_forward_end_archive,
    bbsink_forward_end_backup, bbsink_forward_end_manifest, bbsink_forward_manifest_contents,
    Bbsink, BbsinkOps, BbsinkState,
};
use ::latch_seams::{reset_latch_my_latch, wait_latch_my_latch};
use ::mcx::Mcx;
use ::postgres_seams::check_for_interrupts;
use ::timestamp_seams::get_current_timestamp;
use ::types_core::{Size, TimeLineID, TimestampTz, XLogRecPtr};
use ::types_error::PgResult;
use ::types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};

type TimeOffset = i64;
const USECS_PER_SEC: i64 = 1_000_000;
const THROTTLING_FREQUENCY: i64 = 8;
// PG_WAIT_TIMEOUT class | event index 0 (timeout section of wait_event_names).
const WAIT_EVENT_BASE_BACKUP_THROTTLE: u32 = 0x0900_0000;

/// C `bbsink_throttle`; the chain and buffer live in the surrounding [`Bbsink`].
#[derive(Debug, Clone, Copy)]
pub struct BbsinkThrottle {
    throttling_sample: u64,
    throttling_counter: i64,
    elapsed_min_unit: TimeOffset,
    throttled_last: TimestampTz,
}

/// Caller guarantees `maxrate > 0` (C `Assert`).
pub fn bbsink_throttle_new<'mcx>(
    mcx: Mcx<'mcx>,
    next: Box<Bbsink<'mcx>>,
    maxrate: u32,
) -> Box<Bbsink<'mcx>> {
    let throttling_sample = ((maxrate as i64) * 1024 / THROTTLING_FREQUENCY) as u64;
    let elapsed_min_unit = USECS_PER_SEC / THROTTLING_FREQUENCY;

    let throttle = BbsinkThrottle {
        throttling_sample,
        throttling_counter: 0,
        elapsed_min_unit,
        throttled_last: 0,
    };

    Box::new(Bbsink::new(mcx, Box::new(throttle), Some(next)))
}

impl BbsinkThrottle {
    fn throttle(&mut self, increment: Size) -> PgResult<()> {
        debug_assert!(self.throttling_counter >= 0);

        self.throttling_counter += increment as i64;
        if (self.throttling_counter as u64) < self.throttling_sample {
            return Ok(());
        }
        let elapsed_min: TimeOffset =
            self.elapsed_min_unit * (self.throttling_counter / self.throttling_sample as i64);
        loop {
            let elapsed: TimeOffset = get_current_timestamp::call() - self.throttled_last;

            let sleep: TimeOffset = elapsed_min - elapsed;
            if sleep <= 0 {
                break;
            }

            reset_latch_my_latch::call();
            check_for_interrupts::call()?;

            let wait_result = wait_latch_my_latch::call(
                WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                sleep / 1000,
                WAIT_EVENT_BASE_BACKUP_THROTTLE,
            );

            if wait_result & WL_LATCH_SET != 0 {
                check_for_interrupts::call()?;
            }

            if wait_result & WL_TIMEOUT != 0 {
                break;
            }
        }
        self.throttling_counter %= self.throttling_sample as i64;

        self.throttled_last = get_current_timestamp::call();

        Ok(())
    }
}

impl<'mcx> BbsinkOps<'mcx> for BbsinkThrottle {
    fn begin_backup(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        bbsink_forward_begin_backup(sink, state)?;
        self.throttled_last = get_current_timestamp::call();
        Ok(())
    }

    fn archive_contents(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()> {
        self.throttle(len)?;
        bbsink_forward_archive_contents(sink, state, len)
    }

    fn manifest_contents(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()> {
        self.throttle(len)?;
        bbsink_forward_manifest_contents(sink, state, len)
    }

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

/// No inward seams; registered for uniformity.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
