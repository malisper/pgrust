//! GUCs owned by allpaths.c. PGC_USERSET: session-scoped backings.

use guc_tables::session_guc_int as int_guc;

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
