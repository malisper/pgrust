use core::cell::Cell;

use guc_tables::GucVarAccessors;

// bufmgr.c's GUC variable homes: effective_io_concurrency, io_combine_limit,
// io_combine_limit_guc, backend_flush_after, zero_damaged_pages.
const DEFAULT_EFFECTIVE_IO_CONCURRENCY: i32 = 16;
const DEFAULT_IO_COMBINE_LIMIT: i32 = 16;
const DEFAULT_BACKEND_FLUSH_AFTER: i32 = 0;

thread_local! {
    static EFFECTIVE_IO_CONCURRENCY: Cell<i32> = const { Cell::new(DEFAULT_EFFECTIVE_IO_CONCURRENCY) };
    static IO_COMBINE_LIMIT: Cell<i32> = const { Cell::new(DEFAULT_IO_COMBINE_LIMIT) };
    static IO_COMBINE_LIMIT_GUC: Cell<i32> = const { Cell::new(DEFAULT_IO_COMBINE_LIMIT) };
    static BACKEND_FLUSH_AFTER: Cell<i32> = const { Cell::new(DEFAULT_BACKEND_FLUSH_AFTER) };
    static ZERO_DAMAGED_PAGES: Cell<bool> = const { Cell::new(false) };
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

pub fn backend_flush_after() -> i32 {
    BACKEND_FLUSH_AFTER.with(|c| c.get())
}

pub(crate) fn install_guc_backing() {
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
}
