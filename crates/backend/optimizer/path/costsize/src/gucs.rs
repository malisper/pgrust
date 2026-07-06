//! Cost parameters + enable_* flags (owned by costsize.c in C).
//! All PGC_USERSET: backings are session-scoped (guc_tables::session).

use guc_tables::{session_guc_bool as bool_guc, session_guc_int as int_guc, session_guc_real as real_guc};

real_guc!(CPU_TUPLE_COST, cpu_tuple_cost, set_cpu_tuple_cost, guc_tables::consts::DEFAULT_CPU_TUPLE_COST);
real_guc!(SEQ_PAGE_COST, seq_page_cost, set_seq_page_cost, guc_tables::consts::DEFAULT_SEQ_PAGE_COST);
real_guc!(RANDOM_PAGE_COST, random_page_cost, set_random_page_cost, guc_tables::consts::DEFAULT_RANDOM_PAGE_COST);
real_guc!(CPU_INDEX_TUPLE_COST, cpu_index_tuple_cost, set_cpu_index_tuple_cost, guc_tables::consts::DEFAULT_CPU_INDEX_TUPLE_COST);
real_guc!(CPU_OPERATOR_COST, cpu_operator_cost, set_cpu_operator_cost, guc_tables::consts::DEFAULT_CPU_OPERATOR_COST);
real_guc!(RECURSIVE_WORKTABLE_FACTOR, recursive_worktable_factor, set_recursive_worktable_factor, guc_tables::consts::DEFAULT_RECURSIVE_WORKTABLE_FACTOR);
real_guc!(PARALLEL_TUPLE_COST, parallel_tuple_cost, set_parallel_tuple_cost, guc_tables::consts::DEFAULT_PARALLEL_TUPLE_COST);
real_guc!(PARALLEL_SETUP_COST, parallel_setup_cost, set_parallel_setup_cost, guc_tables::consts::DEFAULT_PARALLEL_SETUP_COST);
int_guc!(EFFECTIVE_CACHE_SIZE, effective_cache_size, set_effective_cache_size, guc_tables::consts::DEFAULT_EFFECTIVE_CACHE_SIZE);
bool_guc!(ENABLE_SEQSCAN, enable_seqscan, set_enable_seqscan, true);
bool_guc!(ENABLE_TIDSCAN, enable_tidscan, set_enable_tidscan, true);
bool_guc!(ENABLE_INDEXSCAN, enable_indexscan, set_enable_indexscan, true);
bool_guc!(ENABLE_INDEXONLYSCAN, enable_indexonlyscan, set_enable_indexonlyscan, true);
bool_guc!(ENABLE_BITMAPSCAN, enable_bitmapscan, set_enable_bitmapscan, true);
bool_guc!(ENABLE_HASHAGG, enable_hashagg, set_enable_hashagg, true);
bool_guc!(ENABLE_SORT, enable_sort, set_enable_sort, true);
bool_guc!(ENABLE_NESTLOOP, enable_nestloop, set_enable_nestloop, true);
bool_guc!(ENABLE_HASHJOIN, enable_hashjoin, set_enable_hashjoin, true);
bool_guc!(ENABLE_MERGEJOIN, enable_mergejoin, set_enable_mergejoin, true);
bool_guc!(ENABLE_MATERIAL, enable_material, set_enable_material, true);
bool_guc!(ENABLE_MEMOIZE, enable_memoize, set_enable_memoize, true);
bool_guc!(ENABLE_INCREMENTAL_SORT, enable_incremental_sort, set_enable_incremental_sort, true);
bool_guc!(ENABLE_GROUP_BY_REORDERING, enable_group_by_reordering, set_enable_group_by_reordering, true);
bool_guc!(ENABLE_DISTINCT_REORDERING, enable_distinct_reordering, set_enable_distinct_reordering, true);
bool_guc!(ENABLE_PRESORTED_AGGREGATE, enable_presorted_aggregate, set_enable_presorted_aggregate, true);
bool_guc!(ENABLE_PARTITION_PRUNING, enable_partition_pruning, set_enable_partition_pruning, true);
bool_guc!(ENABLE_PARTITIONWISE_JOIN, enable_partitionwise_join, set_enable_partitionwise_join, false);
bool_guc!(ENABLE_PARTITIONWISE_AGGREGATE, enable_partitionwise_aggregate, set_enable_partitionwise_aggregate, false);
bool_guc!(ENABLE_GATHERMERGE, enable_gathermerge, set_enable_gathermerge, true);
bool_guc!(ENABLE_PARALLEL_HASH, enable_parallel_hash, set_enable_parallel_hash, true);

// Read through the slot: execmain install_if_absent's a stand-in accessor,
// whichever install wins must serve every reader.
bool_guc!(PARALLEL_LEADER_PARTICIPATION, parallel_leader_participation_backing, set_parallel_leader_participation_backing, true);

pub fn parallel_leader_participation() -> bool {
    guc_tables::vars::parallel_leader_participation.read()
}

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
    guc_tables::vars::enable_partition_pruning.install(GucVarAccessors {
        get: enable_partition_pruning,
        set: set_enable_partition_pruning,
    });
    guc_tables::vars::enable_partitionwise_join.install(GucVarAccessors {
        get: enable_partitionwise_join,
        set: set_enable_partitionwise_join,
    });
    guc_tables::vars::enable_partitionwise_aggregate.install(GucVarAccessors {
        get: enable_partitionwise_aggregate,
        set: set_enable_partitionwise_aggregate,
    });
    guc_tables::vars::enable_tidscan
        .install(GucVarAccessors { get: enable_tidscan, set: set_enable_tidscan });
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
    guc_tables::vars::enable_memoize
        .install(GucVarAccessors { get: enable_memoize, set: set_enable_memoize });
    guc_tables::vars::enable_incremental_sort
        .install(GucVarAccessors { get: enable_incremental_sort, set: set_enable_incremental_sort });
    guc_tables::vars::enable_hashagg
        .install(GucVarAccessors { get: enable_hashagg, set: set_enable_hashagg });
    guc_tables::vars::enable_group_by_reordering
        .install(GucVarAccessors { get: enable_group_by_reordering, set: set_enable_group_by_reordering });
    guc_tables::vars::enable_distinct_reordering
        .install(GucVarAccessors { get: enable_distinct_reordering, set: set_enable_distinct_reordering });
    guc_tables::vars::enable_presorted_aggregate
        .install(GucVarAccessors { get: enable_presorted_aggregate, set: set_enable_presorted_aggregate });
    guc_tables::vars::recursive_worktable_factor
        .install(GucVarAccessors { get: recursive_worktable_factor, set: set_recursive_worktable_factor });
    guc_tables::vars::parallel_tuple_cost
        .install(GucVarAccessors { get: parallel_tuple_cost, set: set_parallel_tuple_cost });
    guc_tables::vars::parallel_setup_cost
        .install(GucVarAccessors { get: parallel_setup_cost, set: set_parallel_setup_cost });
    guc_tables::vars::enable_gathermerge
        .install(GucVarAccessors { get: enable_gathermerge, set: set_enable_gathermerge });
    guc_tables::vars::enable_parallel_hash
        .install(GucVarAccessors { get: enable_parallel_hash, set: set_enable_parallel_hash });
    guc_tables::vars::parallel_leader_participation.install_if_absent(GucVarAccessors {
        get: parallel_leader_participation_backing,
        set: set_parallel_leader_participation_backing,
    });
}
