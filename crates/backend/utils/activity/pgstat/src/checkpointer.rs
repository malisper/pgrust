// pgstat_checkpointer.c — PendingCheckpointerStats plus the shared apply
// (pgstat_report_checkpointer) and reset.

use core::cell::{Cell, RefCell};
use std::sync::Mutex;

use types_core::TimestampTz;

use crate::PgStat_Counter;

#[derive(Clone, Copy, Default, PartialEq, Debug)]
#[repr(C)]
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

// bufmgr.c BufferSync's PendingCheckpointerStats.buffers_written++: one bump
// per buffer a checkpoint actually wrote.
pub fn pgstat_count_checkpointer_buffers_written() {
    with_pending_checkpointer_stats(|s| s.buffers_written += 1);
}

pub fn pending_checkpointer_stats() -> PgStat_CheckpointerStats {
    PENDING_CHECKPOINTER_STATS.with(|s| *s.borrow())
}

static SHARED_CHECKPOINTER: Mutex<PgStat_CheckpointerStats> =
    Mutex::new(PgStat_CheckpointerStats {
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
    });

thread_local! {
    static SNAPSHOT_CHECKPOINTER: Cell<Option<PgStat_CheckpointerStats>> =
        const { Cell::new(None) };
}

pub fn pgstat_report_checkpointer() {
    PENDING_CHECKPOINTER_STATS.with(|s| {
        let mut pending = s.borrow_mut();
        if *pending == PgStat_CheckpointerStats::default() {
            return;
        }
        let mut shared = SHARED_CHECKPOINTER.lock().unwrap();
        shared.num_timed += pending.num_timed;
        shared.num_requested += pending.num_requested;
        shared.num_performed += pending.num_performed;
        shared.restartpoints_timed += pending.restartpoints_timed;
        shared.restartpoints_requested += pending.restartpoints_requested;
        shared.restartpoints_performed += pending.restartpoints_performed;
        shared.write_time += pending.write_time;
        shared.sync_time += pending.sync_time;
        shared.buffers_written += pending.buffers_written;
        shared.slru_written += pending.slru_written;
        *pending = PgStat_CheckpointerStats::default();
    });
}

pub fn pgstat_fetch_stat_checkpointer() -> PgStat_CheckpointerStats {
    pgstat_checkpointer_snapshot_build();
    SNAPSHOT_CHECKPOINTER.with(|s| s.get().expect("checkpointer snapshot built above"))
}

pub(crate) fn pgstat_checkpointer_snapshot_build() {
    crate::shmem::consume_forced_snapshot_clear();
    if crate::pgstat_fetch_consistency() == crate::PGSTAT_FETCH_CONSISTENCY_SNAPSHOT {
        crate::shmem::build_snapshot();
        return;
    }
    let refresh = crate::pgstat_fetch_consistency() == crate::PGSTAT_FETCH_CONSISTENCY_NONE
        || SNAPSHOT_CHECKPOINTER.with(|s| s.get().is_none());
    if refresh {
        pgstat_checkpointer_snapshot_cb();
    }
}

pub(crate) fn pgstat_checkpointer_snapshot_cb() {
    let shared = *SHARED_CHECKPOINTER.lock().unwrap();
    SNAPSHOT_CHECKPOINTER.with(|s| s.set(Some(shared)));
}

pub(crate) fn pgstat_checkpointer_snapshot_clear() {
    SNAPSHOT_CHECKPOINTER.with(|s| s.set(None));
}

pub(crate) fn pgstat_checkpointer_reset_all_cb(ts: TimestampTz) {
    let mut shared = SHARED_CHECKPOINTER.lock().unwrap();
    *shared = PgStat_CheckpointerStats::default();
    shared.stat_reset_timestamp = ts;
}

pub(crate) fn import_checkpointer_stats(v: PgStat_CheckpointerStats) {
    *SHARED_CHECKPOINTER.lock().unwrap() = v;
}

pub(crate) fn export_checkpointer_stats() -> PgStat_CheckpointerStats {
    *SHARED_CHECKPOINTER.lock().unwrap()
}
