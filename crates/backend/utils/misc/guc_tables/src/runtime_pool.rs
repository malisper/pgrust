//! M1 runtime scan-pipeline arming (docs/design/parallelism-redesign-2026-07
//! §5 M1): the FORCED/explicit engagement knob for executing serial-plan
//! scan→PREWHERE→plain-agg shapes as runtime TaskSets at DOP N.
//!
//! Engagement layering (all three required; absence of any one = exactly
//! today's behavior, byte-for-byte):
//!  1. `PGRUST_RUNTIME=1` — the M0 runtime kill switch (pool spawned at
//!     postmaster start; read once, default OFF).
//!  2. `SET pgrust.runtime_scan_pool = <dop>` — a placeholder customized
//!     option, deliberately NOT a registered GUC (a new pg_settings row
//!     would break the byte-identical SHOW ALL/pg_settings regression
//!     outputs — the lane_pool precedent). Per the force-plans discipline
//!     the plan surface stays the SERIAL plan: unlike
//!     `pgrust.lane_parallel_pool`, this option is never consulted by the
//!     planner and never forces a Gather — the executor arm submits the
//!     serial pipeline to the runtime instead.
//!  3. The lane master switch (`pgrust.lane_executor`) — the arm is a lane
//!     engagement; lane-off servers keep the untouched incumbent oracle.
//!
//! `PGRUST_RUNTIME_SCAN=0`/`off` is the dedicated kill switch for this arm
//! alone (runtime + lane pool untouched).

/// Kill switch + master gates for the runtime scan arm.
fn runtime_scan_env_ok() -> bool {
    static KILLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    let killed = *KILLED.get_or_init(|| {
        matches!(std::env::var("PGRUST_RUNTIME_SCAN").as_deref(), Ok("0") | Ok("off"))
    });
    !killed && crate::backing::pgrust_lane_executor()
}

/// Available-cores clamp. The submitting leader parks (submit-and-park,
/// redesign §2.5 — it does NOT participate), but the helpers execute under
/// the runtime's cores-wide permit cap, so DOP above cores only adds
/// claim-starved threads; cores-1 keeps one core for the (briefly awake)
/// leader and matches the lane pool's clamp for comparable A/B arms.
fn max_runtime_scan_workers() -> i32 {
    static N: std::sync::OnceLock<i32> = std::sync::OnceLock::new();
    *N.get_or_init(|| {
        std::thread::available_parallelism().map_or(1, |n| (n.get() as i32 - 1).max(1))
    })
}

/// The armed runtime scan DOP: `pgrust.runtime_scan_pool` clamped to
/// available cores, or 0 when unarmed (option unset/invalid/<=0, kill
/// switch, lane off). Callers must additionally gate on `PGRUST_RUNTIME=1`
/// (`runtime::runtime_enabled()` + a started pool) and on the shape/binder
/// admission — this function is only the arming request.
pub fn runtime_scan_pool_dop() -> i32 {
    if !runtime_scan_env_ok() {
        return 0;
    }
    // Uninstalled seam (unit-test binaries without a guc boot): unarmed.
    if !guc_seams::get_config_option_missing_ok::is_installed() {
        return 0;
    }
    let dop = guc_seams::get_config_option_missing_ok::call("pgrust.runtime_scan_pool")
        .ok()
        .flatten()
        .and_then(|v| v.trim().parse::<i32>().ok())
        .unwrap_or(0);
    if dop <= 0 {
        return 0;
    }
    dop.min(max_runtime_scan_workers())
}

/// Whether the runtime scan arm is requested at all (nonzero DOP).
pub fn runtime_scan_pool_armed() -> bool {
    runtime_scan_pool_dop() > 0
}

// ---------------------------------------------------------------------------
// M2 aggregation-sink arming (m2-agg-sink lane): the FORCED/explicit knob
// for executing serial-plan GROUP BY aggregations as a runtime ParallelSink
// at DOP N. Same layering as the scan arm: PGRUST_RUNTIME=1 + the customized
// option + the lane master switch; PGRUST_RUNTIME_AGG=0/off is this arm's
// dedicated kill switch.
// ---------------------------------------------------------------------------

/// Kill switch + master gates for the runtime aggregation-sink arm.
fn runtime_agg_env_ok() -> bool {
    static KILLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    let killed = *KILLED.get_or_init(|| {
        matches!(std::env::var("PGRUST_RUNTIME_AGG").as_deref(), Ok("0") | Ok("off"))
    });
    !killed && crate::backing::pgrust_lane_executor()
}

/// The armed runtime aggregation-sink DOP: `pgrust.runtime_agg_pool` clamped
/// to available cores, or 0 when unarmed. Callers additionally gate on
/// `PGRUST_RUNTIME=1` + a started pool + the shape/binder admission.
pub fn runtime_agg_pool_dop() -> i32 {
    if !runtime_agg_env_ok() {
        return 0;
    }
    if !guc_seams::get_config_option_missing_ok::is_installed() {
        return 0;
    }
    let dop = guc_seams::get_config_option_missing_ok::call("pgrust.runtime_agg_pool")
        .ok()
        .flatten()
        .and_then(|v| v.trim().parse::<i32>().ok())
        .unwrap_or(0);
    if dop <= 0 {
        return 0;
    }
    dop.min(max_runtime_scan_workers())
}

/// Whether the runtime aggregation-sink arm is requested (nonzero DOP).
pub fn runtime_agg_pool_armed() -> bool {
    runtime_agg_pool_dop() > 0
}
