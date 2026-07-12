//! Stage-4 v1 work-stealing pool arming for lane-owned cbstore pipelines
//! (docs/design/cbstore-v2-beat-clickhouse-plan.md §4.1-4.3).
//!
//! The pool rides pgrust's thread-backend parallel query: Gather workers are
//! threads of one process, the cbstore scan's row-group claim cursor is
//! already a shared atomic (`phs_nallocated.fetch_add` — the global claim
//! over granule ranges the Stage-0.4 prototype validated), per-worker
//! partial hash-agg tables cross to the leader by pointer (nodeagg::merge
//! handoff), and the finalize merge is partition-parallel (256-bucket atomic
//! bucket claim). What was missing is an ARMING path: cbstore Gather costing
//! carries a deliberate 32k setup surcharge (provisional, pre-pool), so
//! cbstore plans essentially never go parallel unforced.
//!
//! v1 arming, per the plan's forced-plans posture (no planner shape rules):
//!  - `SET pgrust.lane_parallel_pool = <dop>` (a placeholder customized
//!    option — deliberately NOT a registered GUC: a new `pg_settings` row
//!    would break the byte-identical `SHOW ALL`/`pg_settings` regression
//!    outputs, the same reason lane-v2 knobs are env vars) forces cbstore
//!    base relations to plan `<dop>` parallel workers and drops the cbstore
//!    Gather setup surcharge back to `parallel_setup_cost`.
//!  - `PGRUST_LANE_V2_POOL=0`/`off` is the kill switch: arming is refused
//!    regardless of the GUC. Default (unset) allows arming — the pool is
//!    still OFF by default because the GUC defaults to unset.
//!  - Arming also requires the lane master switch (`PGRUST_LANE_V2=1`):
//!    the pool's scope is lane-owned cbstore pipelines only; heap plans and
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
fn lane_pool_env_ok() -> bool {
    static OK: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *OK.get_or_init(|| {
        matches!(std::env::var("PGRUST_LANE_V2").as_deref(), Ok("1") | Ok("on"))
            && !matches!(std::env::var("PGRUST_LANE_V2_POOL").as_deref(), Ok("0") | Ok("off"))
    })
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
/// cbstore-fed; heap plans never consult this.
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
