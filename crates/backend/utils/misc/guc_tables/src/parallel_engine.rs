//! `pgrust.parallel_engine` — the M5 product engine selector (design of
//! record: docs/design/m5-planner.md §2.2, branch m5-design-v2).
//!
//! M5-3 BOOTSTRAP NOTE (reconciliation seam, deliberate): the engine GUC
//! proper is the M5-0 deliverable of the router lane. Until that lands,
//! this module supplies the smallest read surface the M5-3 planner probe
//! needs, following the standing placeholder-option pattern of
//! `runtime_pool.rs` (NOT a registered GUC — a new `pg_settings` row would
//! break the byte-identical SHOW ALL/pg_settings regression outputs; the
//! lane_pool precedent). When M5-0's product GUC lands, this reader keys
//! off the same name and the merge is a redirect, not a semantic change.
//!
//! Contract (§2.2):
//!   * `pgrust.parallel_engine = legacy | runtime`, default **legacy**.
//!     Anything other than the exact value `runtime` (trimmed,
//!     ASCII-case-insensitive) is legacy — fail-closed to today's
//!     behavior, byte-for-byte.
//!   * `runtime` additionally requires the runtime pool
//!     (`PGRUST_RUNTIME=1`, the M0 master switch) and the lane master
//!     switch (`pgrust.lane_executor`). Absent either, the engine
//!     degrades to legacy with a loud-once log line (§2.2).
//!   * `PGRUST_M5_SUPPRESS=0|off` is the dedicated kill switch for the
//!     M5-3 coverage-keyed Gather suppression itself, independent of the
//!     engine selector (a runtime-mode server with suppression killed
//!     plans exactly like legacy; the executor router — M5-1 — is gated
//!     separately).

/// Session/cluster read of `pgrust.parallel_engine`: true iff the option is
/// set to `runtime`. Unset / unparseable / seam-uninstalled ⇒ legacy.
pub fn parallel_engine_runtime_requested() -> bool {
    if !guc_seams::get_config_option_missing_ok::is_installed() {
        return false;
    }
    guc_seams::get_config_option_missing_ok::call("pgrust.parallel_engine")
        .ok()
        .flatten()
        .is_some_and(|v| v.trim().eq_ignore_ascii_case("runtime"))
}

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

/// The full M5-3 planner-probe gate: engine=runtime requested AND the
/// suppression kill switch is not thrown AND the runtime pool + lane
/// master switch are live. On every legacy-mode server this is one
/// cached-bool load plus one placeholder-option miss per Gather-generation
/// call site (the sites already early-return before this when no partial
/// paths exist, so serial arms and select1-class queries never reach it).
pub fn m5_gather_suppression_active() -> bool {
    if m5_suppress_killed() {
        return false;
    }
    if !parallel_engine_runtime_requested() {
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
