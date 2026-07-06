// pgstat_wal.c — WAL-stat pending is the delta of pgWalUsage (owned by the
// xlog insert path, read through transam_xlog_seams) against the last flush.

use core::cell::Cell;
use std::sync::Mutex;

use types_core::instrument::WalUsage;
use types_core::TimestampTz;

use crate::PgStat_Counter;

#[derive(Clone, Copy, Default, PartialEq, Debug)]
#[repr(C)]
pub struct PgStat_WalCounters {
    pub wal_records: PgStat_Counter,
    pub wal_fpi: PgStat_Counter,
    pub wal_bytes: u64,
    pub wal_buffers_full: PgStat_Counter,
}

#[derive(Clone, Copy, Default, PartialEq, Debug)]
#[repr(C)]
pub struct PgStat_WalStats {
    pub wal_counters: PgStat_WalCounters,
    pub stat_reset_timestamp: TimestampTz,
}

static SHARED_WAL: Mutex<PgStat_WalStats> = Mutex::new(PgStat_WalStats {
    wal_counters: PgStat_WalCounters {
        wal_records: 0,
        wal_fpi: 0,
        wal_bytes: 0,
        wal_buffers_full: 0,
    },
    stat_reset_timestamp: 0,
});

thread_local! {
    static PREV_WAL_USAGE: Cell<WalUsage> = const { Cell::new(WalUsage {
        wal_records: 0,
        wal_fpi: 0,
        wal_bytes: 0,
        wal_buffers_full: 0,
    }) };
    static SNAPSHOT_WAL: Cell<Option<PgStat_WalStats>> = const { Cell::new(None) };
}

fn current_wal_usage() -> Option<WalUsage> {
    transam_xlog_seams::wal_usage::is_installed()
        .then(transam_xlog_seams::wal_usage::call)
}

pub fn pgstat_report_wal(force: bool) {
    let nowait = !force;
    pgstat_wal_flush_cb(nowait);
    crate::backend::pgstat_flush_backend(nowait, crate::backend::PGSTAT_BACKEND_FLUSH_WAL);
    crate::io::pgstat_flush_io(nowait);
    crate::backend::pgstat_flush_backend(nowait, crate::backend::PGSTAT_BACKEND_FLUSH_IO);
}

pub fn pgstat_fetch_stat_wal() -> PgStat_WalStats {
    pgstat_wal_snapshot_build();
    SNAPSHOT_WAL.with(|s| s.get().expect("wal snapshot built above"))
}

pub(crate) fn pgstat_wal_snapshot_build() {
    crate::shmem::consume_forced_snapshot_clear();
    if crate::pgstat_fetch_consistency() == crate::PGSTAT_FETCH_CONSISTENCY_SNAPSHOT {
        crate::shmem::build_snapshot();
        return;
    }
    let refresh = crate::pgstat_fetch_consistency() == crate::PGSTAT_FETCH_CONSISTENCY_NONE
        || SNAPSHOT_WAL.with(|s| s.get().is_none());
    if refresh {
        pgstat_wal_snapshot_cb();
    }
}

pub(crate) fn pgstat_wal_snapshot_cb() {
    let shared = *SHARED_WAL.lock().unwrap();
    SNAPSHOT_WAL.with(|s| s.set(Some(shared)));
}

pub(crate) fn pgstat_wal_snapshot_clear() {
    SNAPSHOT_WAL.with(|s| s.set(None));
}

fn pgstat_wal_have_pending(usage: &WalUsage) -> bool {
    usage.wal_records != PREV_WAL_USAGE.with(|c| c.get()).wal_records
}

pub(crate) fn pgstat_wal_flush_cb(_nowait: bool) -> bool {
    let Some(usage) = current_wal_usage() else {
        return false;
    };
    if !pgstat_wal_have_pending(&usage) {
        return false;
    }
    let prev = PREV_WAL_USAGE.with(|c| c.get());
    {
        let mut shared = SHARED_WAL.lock().unwrap();
        let w = &mut shared.wal_counters;
        w.wal_records += usage.wal_records - prev.wal_records;
        w.wal_fpi += usage.wal_fpi - prev.wal_fpi;
        w.wal_bytes = w.wal_bytes.wrapping_add(usage.wal_bytes.wrapping_sub(prev.wal_bytes));
        w.wal_buffers_full += usage.wal_buffers_full - prev.wal_buffers_full;
    }
    PREV_WAL_USAGE.with(|c| c.set(usage));
    false
}

pub(crate) fn pgstat_wal_init_backend_cb() {
    if let Some(usage) = current_wal_usage() {
        PREV_WAL_USAGE.with(|c| c.set(usage));
    }
}

pub(crate) fn pgstat_wal_reset_all_cb(ts: TimestampTz) {
    let mut shared = SHARED_WAL.lock().unwrap();
    *shared = PgStat_WalStats::default();
    shared.stat_reset_timestamp = ts;
}

pub(crate) fn import_wal_stats(v: PgStat_WalStats) {
    *SHARED_WAL.lock().unwrap() = v;
}

pub(crate) fn export_wal_stats() -> PgStat_WalStats {
    *SHARED_WAL.lock().unwrap()
}
