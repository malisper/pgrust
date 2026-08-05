//! Stage-4 v1 work-stealing pool arming for lane-owned pgrcolumnar pipelines
//! (docs/design/pgrcolumnar-v2-beat-clickhouse-plan.md §4.1-4.3).
//!
//! The pool rides pgrust's thread-backend parallel query: Gather workers are
//! threads of one process, the pgrcolumnar scan's row-group claim cursor is
//! already a shared atomic (`phs_nallocated.fetch_add` — the global claim
//! over granule ranges the Stage-0.4 prototype validated), per-worker
//! partial hash-agg tables cross to the leader by pointer (nodeagg::merge
//! handoff), and the finalize merge is partition-parallel (256-bucket atomic
//! bucket claim). What was missing is an ARMING path: pgrcolumnar Gather costing
//! carries a deliberate 32k setup surcharge (provisional, pre-pool), so
//! pgrcolumnar plans essentially never go parallel unforced.
//!
//! v1 arming, per the plan's forced-plans posture (no planner shape rules):
//!  - `SET pgrust.lane_parallel_pool = <dop>` (a placeholder customized
//!    option — deliberately NOT a registered GUC: a new `pg_settings` row
//!    would break the byte-identical `SHOW ALL`/`pg_settings` regression
//!    outputs, the same reason lane-v2 knobs are env vars) forces pgrcolumnar
//!    base relations to plan `<dop>` parallel workers and drops the pgrcolumnar
//!    Gather setup surcharge back to `parallel_setup_cost`.
//!  - `PGRUST_LANE_V2_POOL=0`/`off` is the kill switch: arming is refused
//!    regardless of the GUC. Default (unset) allows arming — the pool is
//!    still OFF by default because the GUC defaults to unset.
//!  - Arming also requires the lane master switch (`PGRUST_LANE_V2=1`):
//!    the pool's scope is lane-owned pgrcolumnar pipelines only; heap plans and
//!    lane-off servers keep PG's Gather behavior untouched.
//!  - The DOP is clamped to actually-available cores minus one (the leader
//!    participates; the Stage-0.4 prototype measured 10-60% losses on short
//!    queries from oversubscribing by even one core) and to
//!    `max_parallel_workers_per_gather` at the use site.
//!
//! Scope guards stay where they live today: EXPLAIN ANALYZE refuses at the
//! merge engagement (`es_instrument != 0`), cursors/EPQ never plan the
//! engaged shape, spill-eligible workers fall back to row emission, and the
//! final emit stays serial behind the finalize Agg's pull face.

/// Kill switch + master gate: lane-v2 on and the pool not killed.
///
/// The master gate is the `pgrust.lane_executor` GUC's session backing cell
/// (default ON since 2026-07-14; the `PGRUST_LANE_V2` boot env var only seeds
/// its startup default), read per call because it is USERSET. Only the
/// `PGRUST_LANE_V2_POOL` kill switch is a process-wide env probe.
fn lane_pool_env_ok() -> bool {
    static KILLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    let killed = *KILLED.get_or_init(|| {
        matches!(std::env::var("PGRUST_LANE_V2_POOL").as_deref(), Ok("0") | Ok("off"))
    });
    !killed && crate::backing::pgrust_lane_executor()
}

/// Available-cores clamp (pool sizing must respect actually-available cores;
/// leader participates, so forced workers stay at cores-1).
fn max_forced_workers() -> i32 {
    static N: std::sync::OnceLock<i32> = std::sync::OnceLock::new();
    *N.get_or_init(|| {
        std::thread::available_parallelism().map_or(1, |n| (n.get() as i32 - 1).max(1))
    })
}

/// The armed pool DOP: `pgrust.lane_parallel_pool` clamped to available
/// cores, or 0 when unarmed (GUC unset/invalid/<=0, kill switch, lane off).
/// Read at plan time (leader) and at handoff-install time (workers — the
/// customized option restores into worker sessions with the rest of the GUC
/// state). Callers must additionally gate on the relation/plan being
/// pgrcolumnar-fed; heap plans never consult this.
pub fn lane_parallel_pool_dop() -> i32 {
    if !lane_pool_env_ok() {
        return 0;
    }
    // Uninstalled seam (unit-test binaries without a guc boot): unarmed.
    if !guc_seams::get_config_option_missing_ok::is_installed() {
        return 0;
    }
    let dop = guc_seams::get_config_option_missing_ok::call("pgrust.lane_parallel_pool")
        .ok()
        .flatten()
        .and_then(|v| v.trim().parse::<i32>().ok())
        .unwrap_or(0);
    if dop <= 0 {
        return 0;
    }
    dop.min(max_forced_workers())
}

/// Whether the pool is armed at all (nonzero DOP): the worker-side
/// partitioned-handoff gate.
pub fn lane_parallel_pool_armed() -> bool {
    lane_parallel_pool_dop() > 0
}

// ---------------------------------------------------------------------------
// Stage-4 §4.4 radix exchange for HIGH-cardinality parallel aggregation.
//
// The engaged pool's thread-local partial tables duplicate groups across
// workers at high NDV (per-worker distinct ≈ G, merge input O(T·G) — the
// Stage-0.4 prototype's merge wall), so 16T barely beats serial on
// groupby_high. The exchange bounds each worker's partial table at
// `exchange_cap` entries and, on reaching the cap, installs the table into
// the finalize's handoff radix-partitioned (top-8 hash bits) and resets it:
// the leader's existing 256-bucket atomic bucket-claim merge then gives every
// bucket to exactly ONE claimer — group ownership is disjoint at the final
// aggregation, per-bucket probe tables are G/256-sized (cache-resident), and
// per-worker builds stay L2-resident instead of DRAM-random-probing 75 MB
// tables. Admission is NDV-driven (plan-time `numGroups`, footer-HLL-honest
// since the cbparallelstats lane): low/mid-NDV builds never reach the cap and
// keep the independent-partials path byte-for-byte.
// ---------------------------------------------------------------------------

/// Exchange kill switch: `PGRUST_AGG_EXCHANGE=0` forces the unbounded
/// thread-local-table behavior on otherwise admitted shapes.
pub fn agg_exchange_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_AGG_EXCHANGE").map_or(true, |v| v != "0"))
}

/// Per-worker partial-table entry bound in exchange mode
/// (`PGRUST_AGG_EXCHANGE_CAP` override). Default 65536: table + entry images
/// stay L2-scale, while a cap-sized flush amortizes the install relocation.
pub fn agg_exchange_cap() -> usize {
    static N: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGRUST_AGG_EXCHANGE_CAP")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|&n| n >= 1024)
            .unwrap_or(1 << 16)
    })
}

/// NDV admission floor (`PGRUST_AGG_EXCHANGE_MIN_GROUPS` override): the
/// exchange engages only when the plan-estimated group count says cross-
/// worker duplication would dominate (default 8× the cap). Below it the
/// independent-partials path wins (groupby_low/mid) and stays untouched.
pub fn agg_exchange_min_groups() -> f64 {
    static N: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGRUST_AGG_EXCHANGE_MIN_GROUPS")
            .ok()
            .and_then(|v| v.trim().parse::<f64>().ok())
            .filter(|&n| n >= 1.0)
            .unwrap_or((8 * (1 << 16)) as f64)
    })
}

/// The full exchange admission over a plan-time group estimate: pool armed,
/// exchange not killed, NDV at/above the floor. Shared verbatim by the
/// planner discounts (costsize) and the executor engagement (nodeagg::merge)
/// so the two never disagree on a shape.
pub fn agg_exchange_admits(num_groups: f64) -> bool {
    agg_exchange_enabled() && num_groups >= agg_exchange_min_groups() && lane_parallel_pool_armed()
}
