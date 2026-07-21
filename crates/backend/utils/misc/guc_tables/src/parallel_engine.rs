//! `pgrust.parallel_engine` — the M5-3 planner-probe gate over the M5
//! product engine selector (design of record: docs/design/m5-planner.md
//! §2.2/§2.3, branch m5-design-v2).
//!
//! RECONCILED (m5-integration): the M5-3 lane bootstrapped this module with
//! a placeholder customized-option read of `pgrust.parallel_engine` (the
//! runtime_pool.rs pattern) because the product GUC was the router lane's
//! M5-0 deliverable. Both lanes are now merged: the REGISTERED enum GUC
//! (backing::pgrust_parallel_engine, consts::PARALLEL_ENGINE_*) is the one
//! source of truth and this module redirects to its M5-0 reader
//! (`runtime_pool::parallel_engine_is_runtime`) — the placeholder read is
//! deleted, exactly the redirect the bootstrap note promised.
//!
//! Contract (§2.2):
//!   * `pgrust.parallel_engine = legacy | runtime`, default **runtime**
//!     since the M5 boarding flip (§4.4 criteria met; boarded with
//!     coverage-keyed FloorGuards, min_dop=12). `legacy` restores the
//!     pre-M5 planning byte-for-byte.
//!   * `runtime` additionally requires the runtime pool to be LIVE
//!     (spawned; `runtime_pool::runtime_pool_live`, published by
//!     `launch_backend::rtpool::start` — the `pgrust.runtime` GUC is the one
//!     requested-state cell, seeded at boot by `PGRUST_RUNTIME`) and the
//!     lane master switch (`pgrust.lane_executor`). Absent either, the
//!     suppression stays inert with a loud-once log line (§2.2) — the
//!     plan-time twin of the router's executor-side degrade
//!     (execmain::lanev2::router checks `runtime::global()`; this probe
//!     checks the liveness flag the pool exports into guc_tables).
//!   * `PGRUST_M5_SUPPRESS=0|off` is the dedicated kill switch for the
//!     M5-3 coverage-keyed Gather suppression itself, independent of the
//!     engine selector (a runtime-mode server with suppression killed
//!     plans exactly like legacy; the executor router — M5-1 — is gated
//!     separately).

/// The M5-3 suppression kill switch: `PGRUST_M5_SUPPRESS=0|off` disables
/// the planner's coverage-keyed Gather suppression without touching the
/// engine selector (read once per process, like every arm kill).
fn m5_suppress_killed() -> bool {
    static KILLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *KILLED.get_or_init(|| {
        matches!(std::env::var("PGRUST_M5_SUPPRESS").as_deref(), Ok("0") | Ok("off"))
    })
}

/// Whether the runtime worker pool is LIVE in this process — spawned and
/// published by `launch_backend::rtpool::start` (t34-config review, defect 3):
/// the probe must never suppress a Gather the runtime cannot pick up, and only
/// pool liveness proves that. The `pgrust.runtime` GUC cell (the one
/// requested-state authority both this probe and the spawn gate read; the
/// PGRUST_RUNTIME env var only seeds it at boot) is a PGC_POSTMASTER setting,
/// so live ⊆ requested and a separate GUC re-check is redundant here.
fn runtime_pool_live() -> bool {
    crate::runtime_pool::runtime_pool_live()
}

/// §2.2 degrade line, loud-once per process.
fn degrade_loud_once(reason: &str) {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        eprintln!(
            "pgrust: pgrust.parallel_engine=runtime but {reason}; \
             degrading to legacy (Gather suppression inert)"
        );
    });
}

/// The full M5-3 planner-probe gate: engine=runtime selected (the registered
/// `pgrust.parallel_engine` GUC via the M5-0 reader) AND the suppression
/// kill switch is not thrown AND the runtime pool is LIVE (spawned — the
/// condition this probe documents; a suppressed Gather with no pool is
/// silent serial) AND the lane master switch is on. On every legacy-mode
/// server this is one cached-bool load plus one session-GUC TLS read per
/// Gather-generation call site (the sites already early-return before this
/// when no partial paths exist, so serial arms and select1-class queries
/// never reach it).
pub fn m5_gather_suppression_active() -> bool {
    if m5_suppress_killed() {
        return false;
    }
    if !crate::runtime_pool::parallel_engine_is_runtime() {
        return false;
    }
    if !runtime_pool_live() {
        degrade_loud_once(
            "the runtime pool is not live (pgrust.runtime=off, PGRUST_RUNTIME=0, or the \
             pool failed to spawn)",
        );
        return false;
    }
    if !crate::backing::pgrust_lane_executor() {
        degrade_loud_once("the lane executor is off (pgrust.lane_executor)");
        return false;
    }
    true
}
