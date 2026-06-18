//! Install the inward seams this crate owns.
//!
//! `backend-commands-analyze-seams` declares three seams whose bodies live here:
//!   * `analyze_rel` — the ANALYZE entry vacuum.c calls for the ANALYZE leg;
//!   * `std_typanalyze` — the standard typanalyze (array_typanalyze calls it);
//!   * `std_compute_stats` — the standard compute callback, re-invoked by
//!     `compute_array_stats`.
//!
//! The analyze-OWNED outward seams (`backend-commands-analyze-rt-seams`) are NOT
//! installed here: their owners (extended-stats, pgstat report, the FDW analyze
//! hook, ANALYZE-only index cleanup, the block-sampling read stream) are
//! unported / model-unreachable and panic loudly with the precise C rationale.

use backend_commands_analyze_seams as analyze;

pub fn init_seams() {
    analyze::analyze_rel::set(crate::analyze_rel);
    analyze::std_typanalyze::set(crate::std_typanalyze);
    analyze::std_compute_stats::set(crate::std_compute_stats);
    // analyze.c owns the `default_statistics_target` GUC global.
    crate::install_default_statistics_target_guc();
}
