//! GUCs owned by allpaths.c.

use std::sync::atomic::{AtomicI32, Ordering};

macro_rules! int_guc {
    ($cell:ident, $get:ident, $set:ident, $boot:expr) => {
        static $cell: AtomicI32 = AtomicI32::new($boot);
        pub fn $get() -> i32 {
            $cell.load(Ordering::Relaxed)
        }
        pub fn $set(v: i32) {
            $cell.store(v, Ordering::Relaxed);
        }
    };
}

int_guc!(MIN_PARALLEL_TABLE_SCAN_SIZE, min_parallel_table_scan_size, set_min_parallel_table_scan_size, (8 * 1024 * 1024) / guc_tables::consts::BLCKSZ);
int_guc!(MIN_PARALLEL_INDEX_SCAN_SIZE, min_parallel_index_scan_size, set_min_parallel_index_scan_size, (512 * 1024) / guc_tables::consts::BLCKSZ);

pub fn install() {
    use guc_tables::GucVarAccessors;
    guc_tables::vars::min_parallel_table_scan_size.install(GucVarAccessors {
        get: min_parallel_table_scan_size,
        set: set_min_parallel_table_scan_size,
    });
    guc_tables::vars::min_parallel_index_scan_size.install(GucVarAccessors {
        get: min_parallel_index_scan_size,
        set: set_min_parallel_index_scan_size,
    });
}
