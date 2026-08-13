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
    // Raw USERSET io_combine_limit value (C: io_combine_limit_guc). The
    // *effective* limit is derived on read as min(this, io_max_combine_limit)
    // — see io_combine_limit().
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

/// Effective io_combine_limit, matching C's bufmgr.c global: the USERSET
/// io_combine_limit clamped to the PGC_POSTMASTER io_max_combine_limit, which
/// sizes the AIO handle-data region. Deriving this on every read (rather than
/// caching an assign-time value) keeps the bound correct regardless of the
/// order in which the two GUCs are applied — in particular it covers a fresh
/// backend booting with io_max_combine_limit below the default io_combine_limit
/// (no SET io_combine_limit ever runs to re-clamp a cached value). Returning an
/// unclamped value here overruns the io_max_combine_limit-sized region in the
/// batched read path (release: out-of-bounds heap write; W5-CFGENC-F1).
pub fn io_combine_limit() -> i32 {
    let raw = IO_COMBINE_LIMIT_GUC.with(|c| c.get());
    // io_max_combine_limit is installed by variable::init_seams on every real
    // server boot; guard so unit tests that arm the read path without wiring it
    // fall back to the raw value rather than panicking on an uninstalled slot.
    if guc_tables::vars::io_max_combine_limit.installed() {
        raw.min(guc_tables::vars::io_max_combine_limit.read()).max(1)
    } else {
        raw
    }
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

// Sets the raw USERSET io_combine_limit value (C: io_combine_limit_guc). The
// effective limit read by the batched-read path is still clamped to
// io_max_combine_limit by io_combine_limit().
#[cfg(test)]
pub(crate) fn set_io_combine_limit_guc(v: i32) {
    IO_COMBINE_LIMIT_GUC.with(|c| c.set(v));
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
        // Store only the raw USERSET value here. The effective limit is
        // min(this, io_max_combine_limit), derived in io_combine_limit() so it
        // stays correct no matter the GUC-apply order (C clamps in both
        // assign_io_combine_limit and assign_io_max_combine_limit).
        set: |v| IO_COMBINE_LIMIT_GUC.with(|c| c.set(v)),
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
