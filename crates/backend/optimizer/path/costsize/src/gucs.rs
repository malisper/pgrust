//! Cost parameters + enable_* flags (owned by costsize.c in C).
//! All PGC_USERSET: backings are session-scoped (guc_tables::session).

use guc_tables::session_guc_cluster;

session_guc_cluster!(CostsizeGucs, COSTSIZE_GUCS:
    (cpu_tuple_cost_cell, f64, cpu_tuple_cost, set_cpu_tuple_cost, guc_tables::consts::DEFAULT_CPU_TUPLE_COST as f64),
    (seq_page_cost_cell, f64, seq_page_cost, set_seq_page_cost, guc_tables::consts::DEFAULT_SEQ_PAGE_COST as f64),
    (random_page_cost_cell, f64, random_page_cost, set_random_page_cost, guc_tables::consts::DEFAULT_RANDOM_PAGE_COST as f64),
    (cpu_index_tuple_cost_cell, f64, cpu_index_tuple_cost, set_cpu_index_tuple_cost, guc_tables::consts::DEFAULT_CPU_INDEX_TUPLE_COST as f64),
    (cpu_operator_cost_cell, f64, cpu_operator_cost, set_cpu_operator_cost, guc_tables::consts::DEFAULT_CPU_OPERATOR_COST as f64),
    (recursive_worktable_factor_cell, f64, recursive_worktable_factor, set_recursive_worktable_factor, guc_tables::consts::DEFAULT_RECURSIVE_WORKTABLE_FACTOR as f64),
    (parallel_tuple_cost_cell, f64, parallel_tuple_cost, set_parallel_tuple_cost, guc_tables::consts::DEFAULT_PARALLEL_TUPLE_COST as f64),
    (parallel_setup_cost_cell, f64, parallel_setup_cost, set_parallel_setup_cost, guc_tables::consts::DEFAULT_PARALLEL_SETUP_COST as f64),
    (effective_cache_size_cell, i32, effective_cache_size, set_effective_cache_size, guc_tables::consts::DEFAULT_EFFECTIVE_CACHE_SIZE),
    (enable_seqscan_cell, bool, enable_seqscan, set_enable_seqscan, true),
    (enable_tidscan_cell, bool, enable_tidscan, set_enable_tidscan, true),
    (enable_indexscan_cell, bool, enable_indexscan, set_enable_indexscan, true),
    (enable_indexonlyscan_cell, bool, enable_indexonlyscan, set_enable_indexonlyscan, true),
    (enable_bitmapscan_cell, bool, enable_bitmapscan, set_enable_bitmapscan, true),
    (enable_hashagg_cell, bool, enable_hashagg, set_enable_hashagg, true),
    (enable_sort_cell, bool, enable_sort, set_enable_sort, true),
    (enable_nestloop_cell, bool, enable_nestloop, set_enable_nestloop, true),
    (enable_hashjoin_cell, bool, enable_hashjoin, set_enable_hashjoin, true),
    (enable_mergejoin_cell, bool, enable_mergejoin, set_enable_mergejoin, true),
    (enable_material_cell, bool, enable_material, set_enable_material, true),
    (enable_memoize_cell, bool, enable_memoize, set_enable_memoize, true),
    (enable_incremental_sort_cell, bool, enable_incremental_sort, set_enable_incremental_sort, true),
    (enable_group_by_reordering_cell, bool, enable_group_by_reordering, set_enable_group_by_reordering, true),
    (enable_distinct_reordering_cell, bool, enable_distinct_reordering, set_enable_distinct_reordering, true),
    (enable_presorted_aggregate_cell, bool, enable_presorted_aggregate, set_enable_presorted_aggregate, true),
    (enable_partition_pruning_cell, bool, enable_partition_pruning, set_enable_partition_pruning, true),
    (enable_partitionwise_join_cell, bool, enable_partitionwise_join, set_enable_partitionwise_join, false),
    (enable_partitionwise_aggregate_cell, bool, enable_partitionwise_aggregate, set_enable_partitionwise_aggregate, false),
    (enable_gathermerge_cell, bool, enable_gathermerge, set_enable_gathermerge, true),
    (enable_parallel_hash_cell, bool, enable_parallel_hash, set_enable_parallel_hash, true),
    (enable_parallel_append_cell, bool, enable_parallel_append, set_enable_parallel_append, true),
    (parallel_leader_participation_backing_cell, bool, parallel_leader_participation_backing, set_parallel_leader_participation_backing, true),
);

// --- cbstore planner knobs (pgrust-only) ---------------------------------
//
// Re-homed from the old cbstore branch's hidden (GUC_NO_SHOW_ALL) SQL GUCs:
// the lane-v2 line deliberately adds NO new GUCs (a new row would break the
// byte-identical `pg_settings` / `SHOW ALL` regression outputs — see
// execmain::lanev2's gating note), so these are compile-time constants with
// an env off-switch where the old surface was a bool. Every consumer is
// gated on the relation/plan actually being cbstore-fed, so heap plans are
// untouched at any value. Provenance / calibration notes for the constants
// live with their original definitions on the old branch
// (guc_tables::consts @ inter-query-scheduling).

// cbstore-path Gather setup: measured one-time thread-native startup
// (~11ms flat) vs C's fork-based ~1-2ms that DEFAULT_PARALLEL_SETUP_COST
// prices. PROVISIONAL — re-measure when lane-v2 parallel over cbstore lands.
pub const DEFAULT_CBSTORE_PARALLEL_SETUP_COST: f64 = 32000.0;
// cbstore-path Gather per-tuple transfer (chunked transport ~27ns/tuple).
pub const DEFAULT_CBSTORE_PARALLEL_TUPLE_COST: f64 = 0.005;
// cbstore no-stats group-key ndistinct ratio (0 disables = C behavior);
// superseded per column once a footer-NDV-backed ANALYZE has run.
pub const DEFAULT_CBSTORE_GROUP_NDISTINCT_RATIO: f64 = 0.05;
// Per-tuple surcharge for a Sort directly over a Gather on cbstore-fed
// plans (denies workers the fused bounded-sort feed).
pub const DEFAULT_CBSTORE_GATHER_SORT_TUPLE_COST: f64 = 30.0;

pub fn cbstore_parallel_setup_cost() -> f64 {
    DEFAULT_CBSTORE_PARALLEL_SETUP_COST
}

pub fn cbstore_parallel_tuple_cost() -> f64 {
    DEFAULT_CBSTORE_PARALLEL_TUPLE_COST
}

pub fn cbstore_group_ndistinct_ratio() -> f64 {
    DEFAULT_CBSTORE_GROUP_NDISTINCT_RATIO
}

pub fn cbstore_gather_sort_tuple_cost() -> f64 {
    DEFAULT_CBSTORE_GATHER_SORT_TUPLE_COST
}

/// Column-fraction seqscan disk costing on cbstore (pgrust-only, the Q38
/// sort-vs-hash costing fix): the disk term of a cbstore seqscan is scaled
/// by the referenced columns' share of the part's on-disk bytes — C's
/// pages*seq_page_cost structure kept, with an honest page count for a
/// columnar AM whose scan open takes a plan-derived column need-set.
/// `PGRUST_CBSTORE_COLFRAC_COST=0`/`off` disables for A/B.
pub fn cbstore_colfrac_cost() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("PGRUST_CBSTORE_COLFRAC_COST").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// Footer sorted-column scan pathkeys (was GUC `cbstore_scan_pathkeys`,
/// default on). `PGRUST_CBSTORE_SCAN_PATHKEYS=0`/`off` disables for A/B.
pub fn cbstore_scan_pathkeys() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("PGRUST_CBSTORE_SCAN_PATHKEYS").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

// Read through the slot: execmain install_if_absent's a stand-in accessor,
// whichever install wins must serve every reader.

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
    guc_tables::vars::enable_parallel_append
        .install(GucVarAccessors { get: enable_parallel_append, set: set_enable_parallel_append });
    guc_tables::vars::parallel_leader_participation.install_if_absent(GucVarAccessors {
        get: parallel_leader_participation_backing,
        set: set_parallel_leader_participation_backing,
    });
}
