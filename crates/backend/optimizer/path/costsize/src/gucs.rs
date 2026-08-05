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

// --- pgrcolumnar planner knobs (pgrust-only) ---------------------------------
//
// Re-homed from the old pgrcolumnar branch's hidden (GUC_NO_SHOW_ALL) SQL GUCs:
// the lane-v2 line deliberately adds NO new GUCs (a new row would break the
// byte-identical `pg_settings` / `SHOW ALL` regression outputs — see
// execmain::lanev2's gating note), so these are compile-time constants with
// an env off-switch where the old surface was a bool. Every consumer is
// gated on the relation/plan actually being pgrcolumnar-fed, so heap plans are
// untouched at any value. Provenance / calibration notes for the constants
// live with their original definitions on the old branch
// (guc_tables::consts @ inter-query-scheduling).

// pgrcolumnar-path Gather setup/transfer pricing — GUC-ANCHORED (runtime
// cost-model step 0a, scratchpad/night/runtime-cost-model-design.md §5).
//
// These shipped as flat compile-time constants (32000.0 / 0.005) that ignored
// parallel_setup_cost/parallel_tuple_cost entirely, which made pgrcolumnar
// Gather pricing un-sweepable: the high-card grouped-agg regression
// probe's parallel_setup_cost 100:1000
// and parallel_tuple_cost 0.01:0.1 A/Bs were byte-identical no-ops
// (scratchpad/night/q33-regression-probe.md, probes A/B). They are now
// MULTIPLIERS of the GUCs, derived so that at the shipped t34 defaults
// (parallel_setup_cost=100, parallel_tuple_cost=0.01 — guc_tables::consts)
// the computed prices reproduce the previous flat values EXACTLY
// (320 * 100 = 32000; 0.5 * 0.01 = 0.005; both products are exact in f64,
// pinned by pgrcolumnar_gather_pricing_anchored_at_defaults below), so
// default-config plans and EXPLAIN costs are byte-identical to the flat era.
//
// Provenance of the anchored values (unchanged from the flat constants):
// setup 32000 = measured one-time LEGACY cold-spawn thread startup (~11ms)
// vs the C fork ~1-2ms that the old parallel_setup_cost=1000 priced.
// PROVISIONAL and known stale for warm-pool sessions (the same measurement
// chain that re-set parallel_setup_cost to 100 at t34 —
// docs/design/jit-parallel-defaults.md); honest re-derivation is step-1
// calibration work — step 0 by design preserves default-equivalent pricing.
// tuple 0.005 = measured chunked transport ~27ns/tuple.
pub const PGRCOLUMNAR_PARALLEL_SETUP_MULTIPLIER: f64 = 320.0;
pub const PGRCOLUMNAR_PARALLEL_TUPLE_MULTIPLIER: f64 = 0.5;
// Default-equivalent values (multiplier * default GUC), kept as the anchoring
// contract for the drift test and external references.
pub const DEFAULT_PGRCOLUMNAR_PARALLEL_SETUP_COST: f64 =
    PGRCOLUMNAR_PARALLEL_SETUP_MULTIPLIER * guc_tables::consts::DEFAULT_PARALLEL_SETUP_COST;
pub const DEFAULT_PGRCOLUMNAR_PARALLEL_TUPLE_COST: f64 =
    PGRCOLUMNAR_PARALLEL_TUPLE_MULTIPLIER * guc_tables::consts::DEFAULT_PARALLEL_TUPLE_COST;
// pgrcolumnar no-stats group-key ndistinct ratio (0 disables = C behavior);
// superseded per column once a footer-NDV-backed ANALYZE has run.
pub const DEFAULT_PGRCOLUMNAR_GROUP_NDISTINCT_RATIO: f64 = 0.05;
// Per-tuple surcharge for a Sort directly over a Gather on pgrcolumnar-fed
// plans (denies workers the fused bounded-sort feed).
pub const DEFAULT_PGRCOLUMNAR_GATHER_SORT_TUPLE_COST: f64 = 30.0;

// GL-GMLEADER-1: the Gather-Merge LEADER-CONSUMPTION floor (per-tuple).
// pgrust's cheap-exchange defaults (parallel_tuple_cost 0.01, pgrcolumnar
// 0.005 — thread workers, shared-memory queues) are legitimate for the
// TRANSPORT, but C's 0.1 empirically also covered the LEADER-SERIAL
// consumption of a Gather Merge stream (the binary-heap merge + a serial
// parent ingesting every row) — work whose throughput does not improve
// with workers. Discounting it 10-20x removes C's guard against the
// raw-row-GM-into-serial-leader-aggregate catastrophe (GL-STRAGG-2
// increment 2: elected even at true NDV; GUC bisection witnessed
// parallel_tuple_cost=0.1 ALONE restores C's election, setup/mpwg do
// not; the elected family runs 1.37-1.43s where the displaced hashagg
// family runs 1.07-1.11s). The floor prices GM rows at no less than C's
// per-row rate; the delta term vanishes at SET parallel_tuple_cost>=0.1
// (session C-parity, the bisection control) and at EXPLICITLY ZEROED
// transport costs (the forced-plan bench seams' zeroed-cost sessions
// keep free parallelism). Plain Gather is untouched — its consumers
// keep the cheap-exchange discount everywhere.
// `PGRUST_GM_LEADER_MIN_TUPLE_COST` overrides (A/B vehicles; 0 disables).
pub const DEFAULT_GM_LEADER_MIN_TUPLE_COST: f64 =
    guc_tables::consts::DEFAULT_PARALLEL_TUPLE_COST * 10.0; // == C's 0.1 default

pub fn gm_leader_min_tuple_cost() -> f64 {
    static V: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("PGRUST_GM_LEADER_MIN_TUPLE_COST")
            .ok()
            .and_then(|v| v.trim().parse::<f64>().ok())
            .filter(|v| *v >= 0.0)
            .unwrap_or(DEFAULT_GM_LEADER_MIN_TUPLE_COST)
    })
}

// GL-Q2829-FIX-1: the PLAIN-Gather leader-consumption floor (per-tuple),
// the GL-GMLEADER-1 floor's raw-row-Gather sibling — DEFAULT ON since the
// regexp-keyed-grouping-class flip (Michael, 2026-07-22: "the flip for
// the clickhouse query q29 sounds good to me"; staged 0.0 before that).
// Derivation: identical to the GM floor — 10x the shipped
// parallel_tuple_cost default == C's 0.1, the rate whose economics C's
// elections empirically rely on for leader-serial consumers. The GM floor
// displaced the raw-row-GM-into-serial-leader catastrophe; at fresh-stats
// estimate regimes the election slides into the PLAIN-Gather variant of
// the same class — ship-every-raw-row-to-the-leader into a serial leader
// aggregation that also evaluates the grouping expression per row (the
// exact family gather_tuple_cost's armed-pool carve already dodges for
// `pgrust.lane_parallel_pool` sessions; this floor is the stock-defaults
// guard). Scope: raw-row Gathers only — partial-agg-fed Gathers hand
// tables by pointer (the §4.4 exchange pricing) and their leader
// consumption is per-group, not per-row. Same self-scoping as the GM
// floor: the delta vanishes at SET parallel_tuple_cost >= the floor
// (C-parity sessions) and at explicitly zeroed transport (forced-plan
// bench seams — incl. the cost-route gate's zeroed conf).
// `PGRUST_GATHER_LEADER_MIN_TUPLE_COST` overrides (A/B vehicles; 0
// disables, restoring the pre-flip election byte-exactly).
pub const DEFAULT_GATHER_LEADER_MIN_TUPLE_COST: f64 =
    guc_tables::consts::DEFAULT_PARALLEL_TUPLE_COST * 10.0; // == C's 0.1 default

pub fn gather_leader_min_tuple_cost() -> f64 {
    static V: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("PGRUST_GATHER_LEADER_MIN_TUPLE_COST")
            .ok()
            .and_then(|v| v.trim().parse::<f64>().ok())
            .filter(|v| *v >= 0.0)
            .unwrap_or(DEFAULT_GATHER_LEADER_MIN_TUPLE_COST)
    })
}

pub fn pgrcolumnar_parallel_setup_cost() -> f64 {
    PGRCOLUMNAR_PARALLEL_SETUP_MULTIPLIER * parallel_setup_cost()
}

pub fn pgrcolumnar_parallel_tuple_cost() -> f64 {
    PGRCOLUMNAR_PARALLEL_TUPLE_MULTIPLIER * parallel_tuple_cost()
}

pub fn pgrcolumnar_group_ndistinct_ratio() -> f64 {
    DEFAULT_PGRCOLUMNAR_GROUP_NDISTINCT_RATIO
}

pub fn pgrcolumnar_gather_sort_tuple_cost() -> f64 {
    DEFAULT_PGRCOLUMNAR_GATHER_SORT_TUPLE_COST
}

/// Column-fraction seqscan disk costing on pgrcolumnar (pgrust-only, the
/// sort-vs-hash grouping costing fix): the disk term of a pgrcolumnar seqscan is scaled
/// by the referenced columns' share of the part's on-disk bytes — C's
/// pages*seq_page_cost structure kept, with an honest page count for a
/// columnar AM whose scan open takes a plan-derived column need-set.
/// `PGRUST_CBSTORE_COLFRAC_COST=0`/`off` disables for A/B.
pub fn pgrcolumnar_colfrac_cost() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("PGRUST_CBSTORE_COLFRAC_COST").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// Footer-NDV group-key estimation on pgrcolumnar (pgrust-only): a never-
/// ANALYZEd pgrcolumnar rel estimates group-key ndistinct from the part
/// footer's ingest-time per-column NDV (the exact numbers a footer-backed
/// ANALYZE would put in pg_statistic) instead of the flat
/// DEFAULT_PGRCOLUMNAR_GROUP_NDISTINCT_RATIO guess. The ratio starves nothing
/// serially, but at high DOP it prices partial aggregation as useless
/// (per-worker groups ~= 5% of the table) and flips grouped parallel plans
/// serial. `PGRUST_CBSTORE_FOOTER_NDV_EST=0`/`off` disables for A/B.
pub fn pgrcolumnar_footer_ndv_est() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("PGRUST_CBSTORE_FOOTER_NDV_EST").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

// --- Step-0b: leader-hashagg spill honesty over Gather ----------------------
//
// Executor-honest per-group footprint multiplier for a leader-side hashed
// Agg fed by a Gather/GatherMerge on a pgrcolumnar-fed plan. C's
// hash_agg_entry_size counts only the tuple + pergroup + transition
// chunks — for the REAL high-cardinality two-int-key grouped shape (with
// count(*) + SUM(IsRefresh) + AVG(ResolutionWidth): Gather width 16,
// THREE transinfos, transitionSpace 48 = MAXALIGN(get_typavgwidth(_int8)
// = 32) + 16 for AVG's by-ref int8-array transvalue — prepagg.rs) that is
// 16 + MAXALIGN(MAXALIGN(15)+16) + 3*16 + (8 + pow2(48)) = 168 B/group,
// pricing a 10M-group leader table at 1.68GB. The leader's REAL working
// set (TupleHash bucket array + palloc chunk rounding + firstTuple
// copies) measured ~3.03GB peak TreeRssAnon in-memory and crossed a 2GB
// budget into spill (scratchpad/night/q33-regression-probe.md, probe E2:
// work_mem 512MB*4 = 2.15GB -> 9.6s spilling; 1GB*4 = 4.29GB -> 0.93s
// in-memory; same plan). 3.03e9 / (1e7 * 168) = 1.80. Both fleet anchors
// are reproduced at 1.8: predicted working set 3.024GB > 2.15GB budget
// (spill priced, margin 1.41x) and < 4.29GB budget (no spill term,
// margin 0.70x); the admissible band from the two anchors alone is
// (1.28, 2.56). Pinned by leader_hashagg_entry_scale_reproduces_q33_
// fleet_anchors at the REAL entry composition.
// CALIBRATION HISTORY (both catches by the GL-COST-step0 byte-identical-
// at-1GB fleet bar, before any merge): first shipped 4.7 against a
// width-12/one-transinfo mis-model (entry 64B -> predicted 7.9GB, flipped
// both arms — job -0557); then 3.2 against a transitionSpace-0 mis-model
// (entry 96B -> predicted 5.4GB, still flipped the 1GB arm — job -07cf).
// The lesson is now a red test: derive against hash_agg_entry_size's REAL
// output for the anchor shape, never a hand model of it.
// Applied ONLY to the honest-Gather spill delta
// (costsize::cost_agg_leader_spill_adjust); C's cost_agg spill block
// keeps the unscaled entry size everywhere, so heap plans and serial
// hashaggs are untouched bit-for-bit.
pub const DEFAULT_PGRCOLUMNAR_LEADER_HASHAGG_ENTRY_SCALE: f64 = 1.8;

/// Entry-scale for the leader-hashagg-over-Gather spill delta.
/// `PGRUST_CBSTORE_LEADER_SPILL_COST=0`/`off` disables (None); a positive
/// numeric value overrides the scale for calibration sweeps.
pub fn pgrcolumnar_leader_hashagg_entry_scale() -> Option<f64> {
    static V: std::sync::OnceLock<Option<f64>> = std::sync::OnceLock::new();
    *V.get_or_init(|| match std::env::var("PGRUST_CBSTORE_LEADER_SPILL_COST").as_deref() {
        Ok("0") | Ok("off") => None,
        Ok(s) => match s.parse::<f64>() {
            Ok(v) if v > 0.0 => Some(v),
            _ => Some(DEFAULT_PGRCOLUMNAR_LEADER_HASHAGG_ENTRY_SCALE),
        },
        Err(_) => Some(DEFAULT_PGRCOLUMNAR_LEADER_HASHAGG_ENTRY_SCALE),
    })
}

/// Footer sorted-column scan pathkeys (was GUC `pgrcolumnar_scan_pathkeys`,
/// default on). `PGRUST_CBSTORE_SCAN_PATHKEYS=0`/`off` disables for A/B.
pub fn pgrcolumnar_scan_pathkeys() -> bool {
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
