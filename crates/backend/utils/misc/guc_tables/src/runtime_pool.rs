//! Runtime engagement arming — THE three-arm surface (m2-integration
//! reconciliation; design: docs/design/parallelism-redesign-2026-07 §5,
//! docs/design/m2-sinks.md).
//!
//! Every runtime engagement arm layers identically (all gates required;
//! absence of any one = exactly today's behavior, byte-for-byte):
//!  1. `PGRUST_RUNTIME=1` — the M0 runtime kill switch (pool spawned at
//!     postmaster start; read once, default OFF). Checked by the arm.
//!  2. A per-arm DOP knob, a placeholder customized option, deliberately
//!     NOT a registered GUC (a new pg_settings row would break the
//!     byte-identical SHOW ALL/pg_settings regression outputs — the
//!     lane_pool precedent). Per the force-plans discipline the plan
//!     surface stays the SERIAL plan: the option is never consulted by the
//!     planner and never forces a Gather — the executor arm submits the
//!     serial pipeline to the runtime instead.
//!  3. The lane master switch (`pgrust.lane_executor`) — every arm is a
//!     lane engagement; lane-off servers keep the untouched incumbent
//!     oracle.
//!  4. A DEDICATED per-arm env kill switch (`=0`/`off`), independent of the
//!     other arms.
//!
//! | arm (entry)                  | DOP option                  | kill switch              |
//! |------------------------------|-----------------------------|--------------------------|
//! | M1 scan  `runtime_scan_pool_dop`     | `pgrust.runtime_scan_pool` | `PGRUST_RUNTIME_SCAN`    |
//! | M2 agg   `runtime_agg_pool_dop`      | `pgrust.runtime_agg_pool`  | `PGRUST_RUNTIME_AGG`     |
//! | M2 distinct `runtime_distinct_pool_dop` | `pgrust.runtime_distinct_pool`, falling back to `pgrust.runtime_scan_pool` | `PGRUST_RUNTIME_DISTINCT` |
//!
//! The distinct arm's fallback keeps the lane's booked instrument/e2e
//! vocabulary (`SET pgrust.runtime_scan_pool=D` armed its curves) while
//! fixing the m2-distinct-sink coupling note: `PGRUST_RUNTIME_SCAN=0` used
//! to disarm the distinct arm too (the scan kill was embedded in the shared
//! DOP read); each kill now disarms exactly its own arm.

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

/// Shared placeholder-option DOP read: parsed + clamped to available cores;
/// 0 when unset/invalid/<=0 or when the guc seam is uninstalled (unit-test
/// binaries without a guc boot: unarmed).
fn pool_option_dop(option: &str) -> i32 {
    if !guc_seams::get_config_option_missing_ok::is_installed() {
        return 0;
    }
    let dop = guc_seams::get_config_option_missing_ok::call(option)
        .ok()
        .flatten()
        .and_then(|v| v.trim().parse::<i32>().ok())
        .unwrap_or(0);
    if dop <= 0 {
        return 0;
    }
    dop.min(max_runtime_scan_workers())
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
    pool_option_dop("pgrust.runtime_scan_pool")
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
    pool_option_dop("pgrust.runtime_agg_pool")
}

/// Whether the runtime aggregation-sink arm is requested (nonzero DOP).
pub fn runtime_agg_pool_armed() -> bool {
    runtime_agg_pool_dop() > 0
}

// ---------------------------------------------------------------------------
// M2 distinct-sink arming (m2-distinct-sink lane; entry added at
// m2-integration): the FORCED/explicit knob for executing serial-plan
// DISTINCT / COUNT(DISTINCT) shapes as a runtime SealedParallelSink at
// DOP N. Same layering as the other arms; PGRUST_RUNTIME_DISTINCT=0/off is
// this arm's dedicated kill switch.
// ---------------------------------------------------------------------------

/// Kill switch + master gates for the runtime distinct-sink arm.
fn runtime_distinct_env_ok() -> bool {
    static KILLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    let killed = *KILLED.get_or_init(|| {
        matches!(std::env::var("PGRUST_RUNTIME_DISTINCT").as_deref(), Ok("0") | Ok("off"))
    });
    !killed && crate::backing::pgrust_lane_executor()
}

/// The armed runtime distinct-sink DOP: `pgrust.runtime_distinct_pool` if
/// set, else `pgrust.runtime_scan_pool` (the lane's booked instrument/e2e
/// vocabulary), clamped to available cores; 0 when unarmed. Callers
/// additionally gate on `PGRUST_RUNTIME=1` + a started pool + the
/// shape/binder admission. Unlike the pre-integration coupling, the scan
/// arm's PGRUST_RUNTIME_SCAN kill does NOT disarm this arm.
pub fn runtime_distinct_pool_dop() -> i32 {
    if !runtime_distinct_env_ok() {
        return 0;
    }
    let own = pool_option_dop("pgrust.runtime_distinct_pool");
    if own > 0 {
        return own;
    }
    pool_option_dop("pgrust.runtime_scan_pool")
}

// ---------------------------------------------------------------------------
// M3 runtime hash-join arming (docs/design/m3-joins.md §9): same layering as
// the scan arm — PGRUST_RUNTIME=1 + `SET pgrust.runtime_hashjoin_pool = <dop>`
// (customized option, planner never consults it) + the lane master switch;
// `PGRUST_RUNTIME_HASHJOIN=0`/`off` is the dedicated arm kill.
// ---------------------------------------------------------------------------

fn runtime_hashjoin_env_ok() -> bool {
    static KILLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    let killed = *KILLED.get_or_init(|| {
        matches!(std::env::var("PGRUST_RUNTIME_HASHJOIN").as_deref(), Ok("0") | Ok("off"))
    });
    !killed && crate::backing::pgrust_lane_executor()
}

/// The armed runtime hash-join DOP: `pgrust.runtime_hashjoin_pool` clamped
/// to available cores, or 0 when unarmed. Callers must additionally gate on
/// `PGRUST_RUNTIME=1` and the shape/binder admission.
pub fn runtime_hashjoin_pool_dop() -> i32 {
    if !runtime_hashjoin_env_ok() {
        return 0;
    }
    pool_option_dop("pgrust.runtime_hashjoin_pool")
}

/// The armed runtime SORT DOP (M3 top-N sink, docs/design/m3-sort.md §9):
/// `pgrust.runtime_sort_pool` clamped to available cores, or 0 when unarmed.
/// Same layering as the scan knob — a customized option the planner never
/// consults; callers additionally gate on `PGRUST_RUNTIME=1` + shape/binder
/// admission. Deliberately does NOT embed any other arm's kill switch (the
/// m2-distinct coupling gotcha: `runtime_scan_pool_dop` folds
/// PGRUST_RUNTIME_SCAN in, so scan-kill disarms its borrowers too) — the
/// sort arm's own kill (`PGRUST_RUNTIME_SORT`) lives with the arm.
pub fn runtime_sort_pool_dop() -> i32 {
    if !crate::backing::pgrust_lane_executor() {
        return 0;
    }
    pool_option_dop("pgrust.runtime_sort_pool")
}

// ---------------------------------------------------------------------------
// M5-0 engine switch (docs/design/m5-planner.md §2.2): `pgrust.parallel_engine`
// selects the PRODUCT engine. Everything above in this file is the per-arm
// DEVELOPER/bench override layer BENEATH that switch and is deliberately
// untouched by it — arm forcing recipes and manifests keep working verbatim
// in BOTH engine settings. Layering, top to bottom:
//   1. pgrust.parallel_engine (session GUC; default legacy = zero change),
//   2. PGRUST_RUNTIME=1 (process master switch; the pool exists),
//   3. per-arm pool GUCs + PGRUST_RUNTIME_* kills (bench/dev overrides),
//   4. the arm's own fail-closed shape/binder admission walk.
// Under engine=runtime the M5-1 unified router consults `pgrust.runtime_dop`
// for covered shapes; absent a pool the engine degrades to legacy with a
// loud-once server-log line. The pool-availability probe and the degrade
// live at the router (execmain::lanev2::router) — this crate cannot see the
// runtime pool.
// ---------------------------------------------------------------------------

/// Whether the session selects the runtime engine
/// (`pgrust.parallel_engine = runtime`). This is ONLY the GUC read — pool
/// availability and the loud-once degrade are the router's
/// (`execmain::lanev2::router::engine_runtime_active`), the sole consumer
/// allowed to act on this.
#[inline]
pub fn parallel_engine_is_runtime() -> bool {
    crate::backing::pgrust_parallel_engine() == crate::consts::PARALLEL_ENGINE_RUNTIME
}

/// The product runtime DOP (`pgrust.runtime_dop`), resolved: 0 = auto
/// (available cores, the design default); explicit values clamped to
/// available cores (the runtime pool's width — its leaders park rather than
/// participate, so DOP above cores only adds claim-starved threads).
/// Consulted ONLY under engine=runtime (the M5-1 router); the per-arm bench
/// getters above never read it, and it never reads them.
pub fn runtime_dop() -> i32 {
    static CORES: std::sync::OnceLock<i32> = std::sync::OnceLock::new();
    let cores = *CORES
        .get_or_init(|| std::thread::available_parallelism().map_or(1, |n| n.get() as i32));
    let v = crate::backing::pgrust_runtime_dop();
    if v <= 0 {
        cores
    } else {
        v.min(cores)
    }
}
