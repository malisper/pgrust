// pgstat_archiver.c — shared struct + reset only; no archiver process runs
// here yet, so the report half (pgstat_report_archiver) has no caller.

use core::cell::Cell;
use std::sync::Mutex;

use types_core::TimestampTz;

use crate::PgStat_Counter;

// C stores MAX_XFN_CHARS(40)+1 name bytes; padded to 48 so the struct is
// padding-free for the statsfile POD copy.
const XFN_BUF: usize = 48;
pub const MAX_XFN_CHARS: usize = 40;

#[derive(Clone, Copy, PartialEq, Debug)]
#[repr(C)]
pub struct PgStat_ArchiverStats {
    pub archived_count: PgStat_Counter,
    pub last_archived_wal: [u8; XFN_BUF],
    pub last_archived_timestamp: TimestampTz,
    pub failed_count: PgStat_Counter,
    pub last_failed_wal: [u8; XFN_BUF],
    pub last_failed_timestamp: TimestampTz,
    pub stat_reset_timestamp: TimestampTz,
}

const ARCHIVER_ZERO: PgStat_ArchiverStats = PgStat_ArchiverStats {
    archived_count: 0,
    last_archived_wal: [0; XFN_BUF],
    last_archived_timestamp: 0,
    failed_count: 0,
    last_failed_wal: [0; XFN_BUF],
    last_failed_timestamp: 0,
    stat_reset_timestamp: 0,
};

impl Default for PgStat_ArchiverStats {
    fn default() -> Self {
        ARCHIVER_ZERO
    }
}

static SHARED_ARCHIVER: Mutex<PgStat_ArchiverStats> = Mutex::new(ARCHIVER_ZERO);

thread_local! {
    static SNAPSHOT_ARCHIVER: Cell<Option<PgStat_ArchiverStats>> = const { Cell::new(None) };
}

pub fn pgstat_report_archiver(xlog: &str, failed: bool) {
    let now = timestamp_seams::get_current_timestamp::call();
    let mut guard = SHARED_ARCHIVER.lock().unwrap();
    let shared = &mut *guard;
    let (dst, count, ts) = if failed {
        (&mut shared.last_failed_wal, &mut shared.failed_count, &mut shared.last_failed_timestamp)
    } else {
        (
            &mut shared.last_archived_wal,
            &mut shared.archived_count,
            &mut shared.last_archived_timestamp,
        )
    };
    *count += 1;
    *dst = [0; XFN_BUF];
    let n = xlog.len().min(MAX_XFN_CHARS);
    dst[..n].copy_from_slice(&xlog.as_bytes()[..n]);
    *ts = now;
}

pub fn pgstat_fetch_stat_archiver() -> PgStat_ArchiverStats {
    pgstat_archiver_snapshot_build();
    SNAPSHOT_ARCHIVER.with(|s| s.get().expect("archiver snapshot built above"))
}

pub(crate) fn pgstat_archiver_snapshot_build() {
    crate::shmem::consume_forced_snapshot_clear();
    if crate::pgstat_fetch_consistency() == crate::PGSTAT_FETCH_CONSISTENCY_SNAPSHOT {
        crate::shmem::build_snapshot();
        return;
    }
    let refresh = crate::pgstat_fetch_consistency() == crate::PGSTAT_FETCH_CONSISTENCY_NONE
        || SNAPSHOT_ARCHIVER.with(|s| s.get().is_none());
    if refresh {
        pgstat_archiver_snapshot_cb();
    }
}

pub(crate) fn pgstat_archiver_snapshot_cb() {
    let shared = *SHARED_ARCHIVER.lock().unwrap();
    SNAPSHOT_ARCHIVER.with(|s| s.set(Some(shared)));
}

pub(crate) fn pgstat_archiver_snapshot_clear() {
    SNAPSHOT_ARCHIVER.with(|s| s.set(None));
}

pub(crate) fn pgstat_archiver_reset_all_cb(ts: TimestampTz) {
    let mut shared = SHARED_ARCHIVER.lock().unwrap();
    *shared = ARCHIVER_ZERO;
    shared.stat_reset_timestamp = ts;
}

pub(crate) fn import_archiver_stats(v: PgStat_ArchiverStats) {
    *SHARED_ARCHIVER.lock().unwrap() = v;
}

pub(crate) fn export_archiver_stats() -> PgStat_ArchiverStats {
    *SHARED_ARCHIVER.lock().unwrap()
}
