// pgstat_bgwriter.c — pending counters written from bufmgr's BgBufferSync,
// applied to the shared struct by pgstat_report_bgwriter.

use core::cell::{Cell, RefCell};
use std::sync::Mutex;

use types_core::TimestampTz;

use crate::PgStat_Counter;

#[derive(Clone, Copy, Default, PartialEq, Debug)]
#[repr(C)]
pub struct PgStat_BgWriterStats {
    pub buf_written_clean: PgStat_Counter,
    pub maxwritten_clean: PgStat_Counter,
    pub buf_alloc: PgStat_Counter,
    pub stat_reset_timestamp: TimestampTz,
}

static SHARED_BGWRITER: Mutex<PgStat_BgWriterStats> = Mutex::new(PgStat_BgWriterStats {
    buf_written_clean: 0,
    maxwritten_clean: 0,
    buf_alloc: 0,
    stat_reset_timestamp: 0,
});

thread_local! {
    static PENDING_BGWRITER_STATS: RefCell<PgStat_BgWriterStats> =
        const { RefCell::new(PgStat_BgWriterStats {
            buf_written_clean: 0,
            maxwritten_clean: 0,
            buf_alloc: 0,
            stat_reset_timestamp: 0,
        }) };
    static SNAPSHOT_BGWRITER: Cell<Option<PgStat_BgWriterStats>> = const { Cell::new(None) };
}

pub fn with_pending_bgwriter_stats<R>(f: impl FnOnce(&mut PgStat_BgWriterStats) -> R) -> R {
    PENDING_BGWRITER_STATS.with(|s| f(&mut s.borrow_mut()))
}

pub fn pgstat_report_bgwriter() {
    PENDING_BGWRITER_STATS.with(|s| {
        let mut pending = s.borrow_mut();
        if *pending == PgStat_BgWriterStats::default() {
            return;
        }
        let mut shared = SHARED_BGWRITER.lock().unwrap();
        shared.buf_written_clean += pending.buf_written_clean;
        shared.maxwritten_clean += pending.maxwritten_clean;
        shared.buf_alloc += pending.buf_alloc;
        *pending = PgStat_BgWriterStats::default();
    });
}

pub fn pgstat_fetch_stat_bgwriter() -> PgStat_BgWriterStats {
    pgstat_bgwriter_snapshot_build();
    SNAPSHOT_BGWRITER.with(|s| s.get().expect("bgwriter snapshot built above"))
}

pub(crate) fn pgstat_bgwriter_snapshot_build() {
    crate::shmem::consume_forced_snapshot_clear();
    if crate::pgstat_fetch_consistency() == crate::PGSTAT_FETCH_CONSISTENCY_SNAPSHOT {
        crate::shmem::build_snapshot();
        return;
    }
    let refresh = crate::pgstat_fetch_consistency() == crate::PGSTAT_FETCH_CONSISTENCY_NONE
        || SNAPSHOT_BGWRITER.with(|s| s.get().is_none());
    if refresh {
        pgstat_bgwriter_snapshot_cb();
    }
}

pub(crate) fn pgstat_bgwriter_snapshot_cb() {
    let shared = *SHARED_BGWRITER.lock().unwrap();
    SNAPSHOT_BGWRITER.with(|s| s.set(Some(shared)));
}

pub(crate) fn pgstat_bgwriter_snapshot_clear() {
    SNAPSHOT_BGWRITER.with(|s| s.set(None));
}

pub(crate) fn pgstat_bgwriter_reset_all_cb(ts: TimestampTz) {
    let mut shared = SHARED_BGWRITER.lock().unwrap();
    *shared = PgStat_BgWriterStats::default();
    shared.stat_reset_timestamp = ts;
}

pub(crate) fn import_bgwriter_stats(v: PgStat_BgWriterStats) {
    *SHARED_BGWRITER.lock().unwrap() = v;
}

pub(crate) fn export_bgwriter_stats() -> PgStat_BgWriterStats {
    *SHARED_BGWRITER.lock().unwrap()
}
