// commands/progress.h, wired producers' sections only (vacuum, analyze);
// other commands' constants land with their producer wiring.

pub const PROGRESS_VACUUM_PHASE: usize = 0;
pub const PROGRESS_VACUUM_TOTAL_HEAP_BLKS: usize = 1;
pub const PROGRESS_VACUUM_HEAP_BLKS_SCANNED: usize = 2;
pub const PROGRESS_VACUUM_HEAP_BLKS_VACUUMED: usize = 3;
pub const PROGRESS_VACUUM_NUM_INDEX_VACUUMS: usize = 4;
pub const PROGRESS_VACUUM_MAX_DEAD_TUPLE_BYTES: usize = 5;
pub const PROGRESS_VACUUM_DEAD_TUPLE_BYTES: usize = 6;
pub const PROGRESS_VACUUM_NUM_DEAD_ITEM_IDS: usize = 7;
pub const PROGRESS_VACUUM_INDEXES_TOTAL: usize = 8;
pub const PROGRESS_VACUUM_INDEXES_PROCESSED: usize = 9;
pub const PROGRESS_VACUUM_DELAY_TIME: usize = 10;

pub const PROGRESS_VACUUM_PHASE_SCAN_HEAP: i64 = 1;
pub const PROGRESS_VACUUM_PHASE_VACUUM_INDEX: i64 = 2;
pub const PROGRESS_VACUUM_PHASE_VACUUM_HEAP: i64 = 3;
pub const PROGRESS_VACUUM_PHASE_INDEX_CLEANUP: i64 = 4;
pub const PROGRESS_VACUUM_PHASE_TRUNCATE: i64 = 5;
pub const PROGRESS_VACUUM_PHASE_FINAL_CLEANUP: i64 = 6;

// Block numbers in a generic relation scan (CREATE INDEX/REINDEX validate).
pub const PROGRESS_SCAN_BLOCKS_TOTAL: usize = 15;
pub const PROGRESS_SCAN_BLOCKS_DONE: usize = 16;

pub const PROGRESS_ANALYZE_PHASE: usize = 0;
pub const PROGRESS_ANALYZE_BLOCKS_TOTAL: usize = 1;
pub const PROGRESS_ANALYZE_BLOCKS_DONE: usize = 2;
pub const PROGRESS_ANALYZE_EXT_STATS_TOTAL: usize = 3;
pub const PROGRESS_ANALYZE_EXT_STATS_COMPUTED: usize = 4;
pub const PROGRESS_ANALYZE_CHILD_TABLES_TOTAL: usize = 5;
pub const PROGRESS_ANALYZE_CHILD_TABLES_DONE: usize = 6;
pub const PROGRESS_ANALYZE_CURRENT_CHILD_TABLE_RELID: usize = 7;
pub const PROGRESS_ANALYZE_DELAY_TIME: usize = 8;

pub const PROGRESS_ANALYZE_PHASE_ACQUIRE_SAMPLE_ROWS: i64 = 1;
pub const PROGRESS_ANALYZE_PHASE_ACQUIRE_SAMPLE_ROWS_INH: i64 = 2;
pub const PROGRESS_ANALYZE_PHASE_COMPUTE_STATS: i64 = 3;
pub const PROGRESS_ANALYZE_PHASE_COMPUTE_EXT_STATS: i64 = 4;
pub const PROGRESS_ANALYZE_PHASE_FINALIZE_ANALYZE: i64 = 5;

// Progress parameters for CLUSTER / VACUUM FULL (progress.h:60-80).
pub const PROGRESS_CLUSTER_COMMAND: usize = 0;
pub const PROGRESS_CLUSTER_PHASE: usize = 1;
pub const PROGRESS_CLUSTER_INDEX_RELID: usize = 2;
pub const PROGRESS_CLUSTER_HEAP_TUPLES_SCANNED: usize = 3;
pub const PROGRESS_CLUSTER_HEAP_TUPLES_WRITTEN: usize = 4;
pub const PROGRESS_CLUSTER_TOTAL_HEAP_BLKS: usize = 5;
pub const PROGRESS_CLUSTER_HEAP_BLKS_SCANNED: usize = 6;
pub const PROGRESS_CLUSTER_INDEX_REBUILD_COUNT: usize = 7;

pub const PROGRESS_CLUSTER_PHASE_SEQ_SCAN_HEAP: i64 = 1;
pub const PROGRESS_CLUSTER_PHASE_INDEX_SCAN_HEAP: i64 = 2;
pub const PROGRESS_CLUSTER_PHASE_SORT_TUPLES: i64 = 3;
pub const PROGRESS_CLUSTER_PHASE_WRITE_NEW_HEAP: i64 = 4;
pub const PROGRESS_CLUSTER_PHASE_SWAP_REL_FILES: i64 = 5;
pub const PROGRESS_CLUSTER_PHASE_REBUILD_INDEX: i64 = 6;
pub const PROGRESS_CLUSTER_PHASE_FINAL_CLEANUP: i64 = 7;

pub const PROGRESS_CLUSTER_COMMAND_CLUSTER: i64 = 1;
pub const PROGRESS_CLUSTER_COMMAND_VACUUM_FULL: i64 = 2;

pub const PROGRESS_COPY_BYTES_PROCESSED: usize = 0;
pub const PROGRESS_COPY_BYTES_TOTAL: usize = 1;
pub const PROGRESS_COPY_TUPLES_PROCESSED: usize = 2;
pub const PROGRESS_COPY_TUPLES_EXCLUDED: usize = 3;
pub const PROGRESS_COPY_COMMAND: usize = 4;
pub const PROGRESS_COPY_TYPE: usize = 5;
pub const PROGRESS_COPY_TUPLES_SKIPPED: usize = 6;

pub const PROGRESS_COPY_COMMAND_FROM: i64 = 1;
pub const PROGRESS_COPY_COMMAND_TO: i64 = 2;

pub const PROGRESS_COPY_TYPE_FILE: i64 = 1;
pub const PROGRESS_COPY_TYPE_PROGRAM: i64 = 2;
pub const PROGRESS_COPY_TYPE_PIPE: i64 = 3;
pub const PROGRESS_COPY_TYPE_CALLBACK: i64 = 4;

// Lock holder wait counts (shared by the CREATE INDEX and CLUSTER views;
// progress.h reserves params 3-5 of both for "waitfor" metrics). Defined in
// the seams crate so lmgr's waiters can name them.
pub use backend_progress_seams::{
    PROGRESS_WAITFOR_CURRENT_PID, PROGRESS_WAITFOR_DONE, PROGRESS_WAITFOR_TOTAL,
};
