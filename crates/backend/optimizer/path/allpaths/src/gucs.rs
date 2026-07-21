//! GUCs owned by allpaths.c. PGC_USERSET: session-scoped backings.

use guc_tables::session_guc_int as int_guc;

// Lowered 8x from C (8MB->1MB table, 512KB->64KB index; must match
// guc_tables::tables boot_vals). Warm-pool setup (~100 cost, was 1000) drops
// the scan size at which parallelism pays by ~10x, so parallelism engages on
// smaller analytical scans. compute_parallel_worker's log3 rule then also
// assigns more workers per size. docs/design/jit-parallel-defaults.md.
int_guc!(MIN_PARALLEL_TABLE_SCAN_SIZE, min_parallel_table_scan_size, set_min_parallel_table_scan_size, (1024 * 1024) / guc_tables::consts::BLCKSZ);
int_guc!(MIN_PARALLEL_INDEX_SCAN_SIZE, min_parallel_index_scan_size, set_min_parallel_index_scan_size, (64 * 1024) / guc_tables::consts::BLCKSZ);

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
