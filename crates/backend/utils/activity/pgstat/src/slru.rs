// pgstat_slru.c — per-backend pending SLRU counters (fixed-numbered kind).
// Counts happen inside critical sections in C, hence static storage: a
// thread-local array here.

use core::cell::{Cell, RefCell};
use std::sync::Mutex;

use types_core::TimestampTz;

use crate::pending;
use crate::PgStat_Counter;

// repr(C), all-i64 fields: statsfile serialization copies these as bytes.
#[derive(Clone, Copy, Default, PartialEq, Debug)]
#[repr(C)]
pub struct PgStat_SLRUStats {
    pub blocks_zeroed: PgStat_Counter,
    pub blocks_hit: PgStat_Counter,
    pub blocks_read: PgStat_Counter,
    pub blocks_written: PgStat_Counter,
    pub blocks_exists: PgStat_Counter,
    pub flush: PgStat_Counter,
    pub truncate: PgStat_Counter,
    pub stat_reset_timestamp: TimestampTz,
}

pub const SLRU_NUM_ELEMENTS: usize = 8;

static SLRU_NAMES: [&str; SLRU_NUM_ELEMENTS] = [
    "commit_timestamp",
    "multixact_member",
    "multixact_offset",
    "notify",
    "serializable",
    "subtransaction",
    "transaction",
    "other",
];

thread_local! {
    static PENDING_SLRU_STATS: RefCell<[PgStat_SLRUStats; SLRU_NUM_ELEMENTS]> =
        const { RefCell::new([PgStat_SLRUStats {
            blocks_zeroed: 0,
            blocks_hit: 0,
            blocks_read: 0,
            blocks_written: 0,
            blocks_exists: 0,
            flush: 0,
            truncate: 0,
            stat_reset_timestamp: 0,
        }; SLRU_NUM_ELEMENTS]) };
    static HAVE_SLRUSTATS: Cell<bool> = const { Cell::new(false) };
}

fn with_slru_entry(slru_idx: i32, f: impl FnOnce(&mut PgStat_SLRUStats)) {
    let idx = usize::try_from(slru_idx).expect("negative SLRU index");
    assert!(idx < SLRU_NUM_ELEMENTS);
    HAVE_SLRUSTATS.with(|c| c.set(true));
    pending::pgstat_report_fixed_set();
    PENDING_SLRU_STATS.with(|s| f(&mut s.borrow_mut()[idx]));
}

pub fn pgstat_count_slru_page_zeroed(slru_idx: i32) {
    with_slru_entry(slru_idx, |e| e.blocks_zeroed += 1);
}

pub fn pgstat_count_slru_page_hit(slru_idx: i32) {
    with_slru_entry(slru_idx, |e| e.blocks_hit += 1);
}

pub fn pgstat_count_slru_page_exists(slru_idx: i32) {
    with_slru_entry(slru_idx, |e| e.blocks_exists += 1);
}

pub fn pgstat_count_slru_page_read(slru_idx: i32) {
    with_slru_entry(slru_idx, |e| e.blocks_read += 1);
}

pub fn pgstat_count_slru_page_written(slru_idx: i32) {
    with_slru_entry(slru_idx, |e| e.blocks_written += 1);
}

pub fn pgstat_count_slru_flush(slru_idx: i32) {
    with_slru_entry(slru_idx, |e| e.flush += 1);
}

pub fn pgstat_count_slru_truncate(slru_idx: i32) {
    with_slru_entry(slru_idx, |e| e.truncate += 1);
}

pub fn pgstat_get_slru_name(slru_idx: i32) -> Option<&'static str> {
    if slru_idx < 0 || slru_idx as usize >= SLRU_NUM_ELEMENTS {
        return None;
    }
    Some(SLRU_NAMES[slru_idx as usize])
}

pub fn pgstat_get_slru_index(name: &str) -> i32 {
    for (i, n) in SLRU_NAMES.iter().enumerate() {
        if *n == name {
            return i as i32;
        }
    }
    // the last entry is the "other" catch-all
    (SLRU_NUM_ELEMENTS - 1) as i32
}

static SHARED_SLRU: Mutex<[PgStat_SLRUStats; SLRU_NUM_ELEMENTS]> = Mutex::new(
    [PgStat_SLRUStats {
        blocks_zeroed: 0,
        blocks_hit: 0,
        blocks_read: 0,
        blocks_written: 0,
        blocks_exists: 0,
        flush: 0,
        truncate: 0,
        stat_reset_timestamp: 0,
    }; SLRU_NUM_ELEMENTS],
);

thread_local! {
    static SNAPSHOT_SLRU: Cell<Option<[PgStat_SLRUStats; SLRU_NUM_ELEMENTS]>> =
        const { Cell::new(None) };
}

pub(crate) fn pgstat_slru_flush_cb(_nowait: bool) -> bool {
    if !HAVE_SLRUSTATS.with(|c| c.get()) {
        return false;
    }
    PENDING_SLRU_STATS.with(|s| {
        let mut pending = s.borrow_mut();
        let mut shared = SHARED_SLRU.lock().unwrap();
        for (dst, src) in shared.iter_mut().zip(pending.iter()) {
            dst.blocks_zeroed += src.blocks_zeroed;
            dst.blocks_hit += src.blocks_hit;
            dst.blocks_read += src.blocks_read;
            dst.blocks_written += src.blocks_written;
            dst.blocks_exists += src.blocks_exists;
            dst.flush += src.flush;
            dst.truncate += src.truncate;
        }
        *pending = [PgStat_SLRUStats::default(); SLRU_NUM_ELEMENTS];
    });
    HAVE_SLRUSTATS.with(|c| c.set(false));
    false
}

pub fn pgstat_fetch_slru() -> [PgStat_SLRUStats; SLRU_NUM_ELEMENTS] {
    pgstat_slru_snapshot_build();
    SNAPSHOT_SLRU.with(|s| s.get().expect("slru snapshot built above"))
}

pub(crate) fn pgstat_slru_snapshot_build() {
    crate::shmem::consume_forced_snapshot_clear();
    if crate::pgstat_fetch_consistency() == crate::PGSTAT_FETCH_CONSISTENCY_SNAPSHOT {
        crate::shmem::build_snapshot();
        return;
    }
    let refresh = crate::pgstat_fetch_consistency() == crate::PGSTAT_FETCH_CONSISTENCY_NONE
        || SNAPSHOT_SLRU.with(|s| s.get().is_none());
    if refresh {
        pgstat_slru_snapshot_cb();
    }
}

pub(crate) fn pgstat_slru_snapshot_cb() {
    let shared = *SHARED_SLRU.lock().unwrap();
    SNAPSHOT_SLRU.with(|s| s.set(Some(shared)));
}

pub(crate) fn pgstat_slru_snapshot_clear() {
    SNAPSHOT_SLRU.with(|s| s.set(None));
}

fn pgstat_reset_slru_counter_internal(index: usize, ts: TimestampTz) {
    let mut shared = SHARED_SLRU.lock().unwrap();
    shared[index] = PgStat_SLRUStats::default();
    shared[index].stat_reset_timestamp = ts;
}

pub fn pgstat_reset_slru(name: &str) {
    let ts = timestamp_seams::get_current_timestamp::call();
    pgstat_reset_slru_counter_internal(pgstat_get_slru_index(name) as usize, ts);
}

pub(crate) fn pgstat_slru_reset_all_cb(ts: TimestampTz) {
    let mut shared = SHARED_SLRU.lock().unwrap();
    for e in shared.iter_mut() {
        *e = PgStat_SLRUStats::default();
        e.stat_reset_timestamp = ts;
    }
}

pub(crate) fn import_slru_stats(v: [PgStat_SLRUStats; SLRU_NUM_ELEMENTS]) {
    *SHARED_SLRU.lock().unwrap() = v;
}

pub(crate) fn export_slru_stats() -> [PgStat_SLRUStats; SLRU_NUM_ELEMENTS] {
    *SHARED_SLRU.lock().unwrap()
}

pub fn pgstat_slru_pending(slru_idx: usize) -> PgStat_SLRUStats {
    PENDING_SLRU_STATS.with(|s| s.borrow()[slru_idx])
}

pub fn pgstat_have_slrustats() -> bool {
    HAVE_SLRUSTATS.with(|c| c.get())
}
