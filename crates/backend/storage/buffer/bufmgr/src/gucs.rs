use core::cell::Cell;

use guc_tables::GucVarAccessors;

// bufmgr.c's GUC variable homes: effective_io_concurrency, io_combine_limit,
// io_combine_limit_guc, backend_flush_after, zero_damaged_pages; plus
// bufpage.c's ignore_checksum_failure (its reader page_is_verified lives in
// this crate).
const DEFAULT_EFFECTIVE_IO_CONCURRENCY: i32 = 16;
const DEFAULT_IO_COMBINE_LIMIT: i32 = 16;
const DEFAULT_BACKEND_FLUSH_AFTER: i32 = 0;

thread_local! {
    static EFFECTIVE_IO_CONCURRENCY: Cell<i32> = const { Cell::new(DEFAULT_EFFECTIVE_IO_CONCURRENCY) };
    static IO_COMBINE_LIMIT: Cell<i32> = const { Cell::new(DEFAULT_IO_COMBINE_LIMIT) };
    static IO_COMBINE_LIMIT_GUC: Cell<i32> = const { Cell::new(DEFAULT_IO_COMBINE_LIMIT) };
    static BACKEND_FLUSH_AFTER: Cell<i32> = const { Cell::new(DEFAULT_BACKEND_FLUSH_AFTER) };
    static ZERO_DAMAGED_PAGES: Cell<bool> = const { Cell::new(false) };
    static IGNORE_CHECKSUM_FAILURE: Cell<bool> = const { Cell::new(false) };
    static TRACK_IO_TIMING: Cell<bool> = const { Cell::new(false) };
    static CHECKPOINT_FLUSH_AFTER: Cell<i32> =
        const { Cell::new(guc_tables::consts::DEFAULT_CHECKPOINT_FLUSH_AFTER) };
    static BGWRITER_FLUSH_AFTER: Cell<i32> =
        const { Cell::new(guc_tables::consts::DEFAULT_BGWRITER_FLUSH_AFTER) };
    static BGWRITER_LRU_MAXPAGES: Cell<i32> = const { Cell::new(100) };
    static BGWRITER_LRU_MULTIPLIER: Cell<f64> = const { Cell::new(2.0) };
}

pub fn bgwriter_flush_after() -> i32 {
    BGWRITER_FLUSH_AFTER.with(|c| c.get())
}

pub(crate) fn bgwriter_lru_maxpages() -> i32 {
    BGWRITER_LRU_MAXPAGES.with(|c| c.get())
}

pub(crate) fn bgwriter_lru_multiplier() -> f64 {
    BGWRITER_LRU_MULTIPLIER.with(|c| c.get())
}

pub fn effective_io_concurrency() -> i32 {
    EFFECTIVE_IO_CONCURRENCY.with(|c| c.get())
}

pub fn io_combine_limit() -> i32 {
    IO_COMBINE_LIMIT.with(|c| c.get())
}

pub fn zero_damaged_pages() -> bool {
    ZERO_DAMAGED_PAGES.with(|c| c.get())
}

// C home bufpage.c (the PageIsVerified knob); the variable lives with its
// reader.
pub fn ignore_checksum_failure() -> bool {
    IGNORE_CHECKSUM_FAILURE.with(|c| c.get())
}

pub fn track_io_timing() -> bool {
    TRACK_IO_TIMING.with(|c| c.get())
}

pub fn backend_flush_after() -> i32 {
    BACKEND_FLUSH_AFTER.with(|c| c.get())
}

pub fn checkpoint_flush_after() -> i32 {
    CHECKPOINT_FLUSH_AFTER.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn set_checkpoint_flush_after(v: i32) {
    CHECKPOINT_FLUSH_AFTER.with(|c| c.set(v));
}

pub(crate) fn install_guc_backing() {
    guc_tables::vars::bgwriter_flush_after.install(GucVarAccessors {
        get: bgwriter_flush_after,
        set: |v| BGWRITER_FLUSH_AFTER.with(|c| c.set(v)),
    });
    guc_tables::vars::bgwriter_lru_maxpages.install(GucVarAccessors {
        get: bgwriter_lru_maxpages,
        set: |v| BGWRITER_LRU_MAXPAGES.with(|c| c.set(v)),
    });
    guc_tables::vars::bgwriter_lru_multiplier.install(GucVarAccessors {
        get: bgwriter_lru_multiplier,
        set: |v| BGWRITER_LRU_MULTIPLIER.with(|c| c.set(v)),
    });
    guc_tables::vars::checkpoint_flush_after.install(GucVarAccessors {
        get: checkpoint_flush_after,
        set: |v| CHECKPOINT_FLUSH_AFTER.with(|c| c.set(v)),
    });
    guc_tables::vars::effective_io_concurrency.install(GucVarAccessors {
        get: effective_io_concurrency,
        set: |v| EFFECTIVE_IO_CONCURRENCY.with(|c| c.set(v)),
    });
    guc_tables::vars::io_combine_limit_guc.install(GucVarAccessors {
        get: || IO_COMBINE_LIMIT_GUC.with(|c| c.get()),
        // C's assign_io_combine_limit clamps by io_max_combine_limit; that GUC
        // still boots at the same default, so the clamp is the identity here.
        set: |v| {
            IO_COMBINE_LIMIT_GUC.with(|c| c.set(v));
            IO_COMBINE_LIMIT.with(|c| c.set(v));
        },
    });
    guc_tables::vars::backend_flush_after.install(GucVarAccessors {
        get: backend_flush_after,
        set: |v| BACKEND_FLUSH_AFTER.with(|c| c.set(v)),
    });
    guc_tables::vars::zero_damaged_pages.install(GucVarAccessors {
        get: zero_damaged_pages,
        set: |v| ZERO_DAMAGED_PAGES.with(|c| c.set(v)),
    });
    guc_tables::vars::ignore_checksum_failure.install(GucVarAccessors {
        get: ignore_checksum_failure,
        set: |v| IGNORE_CHECKSUM_FAILURE.with(|c| c.set(v)),
    });
    guc_tables::vars::track_io_timing.install(GucVarAccessors {
        get: track_io_timing,
        set: |v| TRACK_IO_TIMING.with(|c| c.set(v)),
    });
}
