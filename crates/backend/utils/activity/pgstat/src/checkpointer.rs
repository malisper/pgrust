// pgstat_checkpointer.c's pending slice: PendingCheckpointerStats and the one
// counter the SLRU lane needs (slru.c's checkpoint-write count). The rest of
// the checkpointer kind (pgstat_report_checkpointer's shared apply) lands
// with backend-utils-activity-small / phase 2.

use core::cell::RefCell;

use types_core::TimestampTz;

use crate::PgStat_Counter;

#[derive(Clone, Copy, Default, PartialEq, Debug)]
pub struct PgStat_CheckpointerStats {
    pub num_timed: PgStat_Counter,
    pub num_requested: PgStat_Counter,
    pub num_performed: PgStat_Counter,
    pub restartpoints_timed: PgStat_Counter,
    pub restartpoints_requested: PgStat_Counter,
    pub restartpoints_performed: PgStat_Counter,
    pub write_time: PgStat_Counter,
    pub sync_time: PgStat_Counter,
    pub buffers_written: PgStat_Counter,
    pub slru_written: PgStat_Counter,
    pub stat_reset_timestamp: TimestampTz,
}

thread_local! {
    static PENDING_CHECKPOINTER_STATS: RefCell<PgStat_CheckpointerStats> =
        const { RefCell::new(PgStat_CheckpointerStats {
            num_timed: 0,
            num_requested: 0,
            num_performed: 0,
            restartpoints_timed: 0,
            restartpoints_requested: 0,
            restartpoints_performed: 0,
            write_time: 0,
            sync_time: 0,
            buffers_written: 0,
            slru_written: 0,
            stat_reset_timestamp: 0,
        }) };
}

pub fn with_pending_checkpointer_stats<R>(f: impl FnOnce(&mut PgStat_CheckpointerStats) -> R) -> R {
    PENDING_CHECKPOINTER_STATS.with(|s| f(&mut s.borrow_mut()))
}

pub fn pgstat_count_checkpointer_slru_written() {
    with_pending_checkpointer_stats(|s| s.slru_written += 1);
}

pub fn pending_checkpointer_stats() -> PgStat_CheckpointerStats {
    PENDING_CHECKPOINTER_STATS.with(|s| *s.borrow())
}
