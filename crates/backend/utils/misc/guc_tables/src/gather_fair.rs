//! Gather read-fairness stride (diagnostic knob, default OFF = C parity).
//!
//! C's gather_readnext (nodeGather.c) deliberately keeps reading the same
//! tuple queue until a read would block ("much more efficient to keep
//! reading from the same queue"). On ship-all-rows shapes where the leader
//! is slower than a single producer, that queue never empties and the scan
//! degenerates to one producer + leader (the 2026-07-14 parallelism audit
//! measured 146x worker-row skew on three grouped-agg shapes). pgrust's batched tqueue
//! transport makes rotation nearly free (the leader holds a decoded chunk
//! per reader; switching readers costs no queue traffic), so a bounded
//! per-queue drain is a candidate divergence — but it is a DIVERGENCE from
//! C's ratified behavior and stays opt-in until fleet evidence + sign-off.
//!
//! `SET pgrust.gather_fair_stride = <n>`: after n consecutive tuples from
//! one queue the leader advances its read cursor round-robin. 0/unset =
//! C behavior. A placeholder customized option, deliberately NOT a
//! registered GUC (same reason as pgrust.lane_parallel_pool: a pg_settings
//! row would break byte-identical regression outputs).

/// The stride, read once per Gather startup (leader). 0 = C parity.
pub fn gather_fair_stride() -> i64 {
    // Uninstalled seam (unit-test binaries without a guc boot): C parity.
    if !guc_seams::get_config_option_missing_ok::is_installed() {
        return 0;
    }
    guc_seams::get_config_option_missing_ok::call("pgrust.gather_fair_stride")
        .ok()
        .flatten()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(0)
}
