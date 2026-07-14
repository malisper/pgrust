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
//!   * `pgrust.parallel_engine = legacy | runtime`, default **legacy** —
//!     fail-closed to today's behavior, byte-for-byte.
//!   * `runtime` additionally requires the runtime pool
//!     (`PGRUST_RUNTIME=1`, the M0 master switch) and the lane master
//!     switch (`pgrust.lane_executor`). Absent either, the suppression
//!     stays inert with a loud-once log line (§2.2) — the plan-time twin
//!     of the router's executor-side degrade (execmain::lanev2::router;
//!     guc_tables cannot see the live pool object, so this probe keys off
//!     the process master switch).
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

/// Whether the runtime pool exists in this process (`PGRUST_RUNTIME=1`,
/// the M0 master switch; mirrors `runtime::runtime_enabled` — guc_tables
/// cannot depend on the runtime crate).
fn runtime_pool_env() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var("PGRUST_RUNTIME").is_ok_and(|v| v == "1"))
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
/// kill switch is not thrown AND the runtime pool + lane master switch are
/// live. On every legacy-mode server this is one cached-bool load plus one
/// session-GUC TLS read per Gather-generation call site (the sites already
/// early-return before this when no partial paths exist, so serial arms and
/// select1-class queries never reach it).
pub fn m5_gather_suppression_active() -> bool {
    if m5_suppress_killed() {
        return false;
    }
    if !crate::runtime_pool::parallel_engine_is_runtime() {
        return false;
    }
    if !runtime_pool_env() {
        degrade_loud_once("the runtime pool is absent (PGRUST_RUNTIME != 1)");
        return false;
    }
    if !crate::backing::pgrust_lane_executor() {
        degrade_loud_once("the lane executor is off (pgrust.lane_executor)");
        return false;
    }
    true
}
