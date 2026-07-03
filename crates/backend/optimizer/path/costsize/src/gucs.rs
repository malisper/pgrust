//! Cost parameters + enable_* flags (owned by costsize.c in C).

use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};

macro_rules! real_guc {
    ($cell:ident, $get:ident, $set:ident, $boot:expr) => {
        static $cell: AtomicU64 = AtomicU64::new(($boot as f64).to_bits());
        pub fn $get() -> f64 {
            f64::from_bits($cell.load(Ordering::Relaxed))
        }
        pub fn $set(v: f64) {
            $cell.store(v.to_bits(), Ordering::Relaxed);
        }
    };
}
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
macro_rules! bool_guc {
    ($cell:ident, $get:ident, $set:ident, $boot:expr) => {
        static $cell: AtomicBool = AtomicBool::new($boot);
        pub fn $get() -> bool {
            $cell.load(Ordering::Relaxed)
        }
        pub fn $set(v: bool) {
            $cell.store(v, Ordering::Relaxed);
        }
    };
}

real_guc!(CPU_TUPLE_COST, cpu_tuple_cost, set_cpu_tuple_cost, guc_tables::consts::DEFAULT_CPU_TUPLE_COST);
real_guc!(SEQ_PAGE_COST, seq_page_cost, set_seq_page_cost, guc_tables::consts::DEFAULT_SEQ_PAGE_COST);
real_guc!(RANDOM_PAGE_COST, random_page_cost, set_random_page_cost, guc_tables::consts::DEFAULT_RANDOM_PAGE_COST);
real_guc!(CPU_INDEX_TUPLE_COST, cpu_index_tuple_cost, set_cpu_index_tuple_cost, guc_tables::consts::DEFAULT_CPU_INDEX_TUPLE_COST);
real_guc!(CPU_OPERATOR_COST, cpu_operator_cost, set_cpu_operator_cost, guc_tables::consts::DEFAULT_CPU_OPERATOR_COST);
int_guc!(EFFECTIVE_CACHE_SIZE, effective_cache_size, set_effective_cache_size, guc_tables::consts::DEFAULT_EFFECTIVE_CACHE_SIZE);
bool_guc!(ENABLE_SEQSCAN, enable_seqscan, set_enable_seqscan, true);
bool_guc!(ENABLE_INDEXSCAN, enable_indexscan, set_enable_indexscan, true);
bool_guc!(ENABLE_INDEXONLYSCAN, enable_indexonlyscan, set_enable_indexonlyscan, true);
bool_guc!(ENABLE_BITMAPSCAN, enable_bitmapscan, set_enable_bitmapscan, true);
bool_guc!(ENABLE_HASHAGG, enable_hashagg, set_enable_hashagg, true);
bool_guc!(ENABLE_SORT, enable_sort, set_enable_sort, true);
bool_guc!(ENABLE_NESTLOOP, enable_nestloop, set_enable_nestloop, true);
bool_guc!(ENABLE_HASHJOIN, enable_hashjoin, set_enable_hashjoin, true);
bool_guc!(ENABLE_MERGEJOIN, enable_mergejoin, set_enable_mergejoin, true);
bool_guc!(ENABLE_MATERIAL, enable_material, set_enable_material, true);
bool_guc!(ENABLE_INCREMENTAL_SORT, enable_incremental_sort, set_enable_incremental_sort, true);
bool_guc!(ENABLE_GROUP_BY_REORDERING, enable_group_by_reordering, set_enable_group_by_reordering, true);
bool_guc!(ENABLE_DISTINCT_REORDERING, enable_distinct_reordering, set_enable_distinct_reordering, true);

pub fn install() {
    use guc_tables::GucVarAccessors;
    guc_tables::vars::cpu_tuple_cost
        .install(GucVarAccessors { get: cpu_tuple_cost, set: set_cpu_tuple_cost });
    guc_tables::vars::seq_page_cost
        .install(GucVarAccessors { get: seq_page_cost, set: set_seq_page_cost });
    guc_tables::vars::random_page_cost
        .install(GucVarAccessors { get: random_page_cost, set: set_random_page_cost });
    guc_tables::vars::cpu_index_tuple_cost
        .install(GucVarAccessors { get: cpu_index_tuple_cost, set: set_cpu_index_tuple_cost });
    guc_tables::vars::cpu_operator_cost
        .install(GucVarAccessors { get: cpu_operator_cost, set: set_cpu_operator_cost });
    guc_tables::vars::effective_cache_size
        .install(GucVarAccessors { get: effective_cache_size, set: set_effective_cache_size });
    guc_tables::vars::enable_seqscan
        .install(GucVarAccessors { get: enable_seqscan, set: set_enable_seqscan });
    guc_tables::vars::enable_indexscan
        .install(GucVarAccessors { get: enable_indexscan, set: set_enable_indexscan });
    guc_tables::vars::enable_indexonlyscan
        .install(GucVarAccessors { get: enable_indexonlyscan, set: set_enable_indexonlyscan });
    guc_tables::vars::enable_bitmapscan
        .install(GucVarAccessors { get: enable_bitmapscan, set: set_enable_bitmapscan });
    guc_tables::vars::enable_sort
        .install(GucVarAccessors { get: enable_sort, set: set_enable_sort });
    guc_tables::vars::enable_nestloop
        .install(GucVarAccessors { get: enable_nestloop, set: set_enable_nestloop });
    guc_tables::vars::enable_hashjoin
        .install(GucVarAccessors { get: enable_hashjoin, set: set_enable_hashjoin });
    guc_tables::vars::enable_mergejoin
        .install(GucVarAccessors { get: enable_mergejoin, set: set_enable_mergejoin });
    guc_tables::vars::enable_material
        .install(GucVarAccessors { get: enable_material, set: set_enable_material });
    guc_tables::vars::enable_incremental_sort
        .install(GucVarAccessors { get: enable_incremental_sort, set: set_enable_incremental_sort });
    guc_tables::vars::enable_hashagg
        .install(GucVarAccessors { get: enable_hashagg, set: set_enable_hashagg });
    guc_tables::vars::enable_group_by_reordering
        .install(GucVarAccessors { get: enable_group_by_reordering, set: set_enable_group_by_reordering });
    guc_tables::vars::enable_distinct_reordering
        .install(GucVarAccessors { get: enable_distinct_reordering, set: set_enable_distinct_reordering });
}
