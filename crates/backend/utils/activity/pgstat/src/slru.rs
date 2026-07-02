// pgstat_slru.c — per-backend pending SLRU counters (fixed-numbered kind).
// Counts happen inside critical sections in C, hence static storage: a
// thread-local array here. Flush applies to PgStatShared_SLRU under its
// lwlock: phase 2; the local half drains the pending array.

use core::cell::{Cell, RefCell};

use types_core::TimestampTz;

use crate::pending;
use crate::PgStat_Counter;

#[derive(Clone, Copy, Default, PartialEq, Debug)]
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

pub(crate) fn pgstat_slru_flush_cb(_nowait: bool) -> bool {
    if !HAVE_SLRUSTATS.with(|c| c.get()) {
        return false;
    }
    // apply into PgStatShared_SLRU under its lock: pgstat_shmem.c phase 2
    PENDING_SLRU_STATS.with(|s| {
        *s.borrow_mut() = [PgStat_SLRUStats::default(); SLRU_NUM_ELEMENTS];
    });
    HAVE_SLRUSTATS.with(|c| c.set(false));
    false
}

pub fn pgstat_slru_pending(slru_idx: usize) -> PgStat_SLRUStats {
    PENDING_SLRU_STATS.with(|s| s.borrow()[slru_idx])
}

pub fn pgstat_have_slrustats() -> bool {
    HAVE_SLRUSTATS.with(|c| c.get())
}
