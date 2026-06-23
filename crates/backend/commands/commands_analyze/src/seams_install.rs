//! Install the inward seams this crate owns, plus the analyze-owned outward
//! seams whose real owners are reachable on the value model.
//!
//! `backend-commands-analyze-seams` declares three inward seams whose bodies
//! live here:
//!   * `analyze_rel` — the ANALYZE entry vacuum.c calls for the ANALYZE leg;
//!   * `std_typanalyze` — the standard typanalyze (array_typanalyze calls it);
//!   * `std_compute_stats` — the standard compute callback, re-invoked by
//!     `compute_array_stats`.
//!
//! The analyze-OWNED outward seams (`backend-commands-analyze-rt-seams`) wire to
//! their real owners where one exists on the value model:
//!   * `catalog_tuple_{insert,update}_with_info_pg_statistic` → the generic
//!     `CatalogTupleInsertWithInfo` / `CatalogTupleUpdateWithInfo`
//!     (catalog/indexing.c, ported);
//!   * `compute_ext_statistics_rows` / `build_relation_ext_statistics` → the
//!     extended_stats.c entry points, installed by their real owner crate
//!     `backend-statistics-extended-stats` (real `pg_statistic_ext` scan with
//!     the empty-case early return), NOT here.
//!
//! The remaining outward seams (`pgstat_report_analyze` — owner
//! pgstat_relation.c; the FDW analyze hook; ANALYZE-only `index_vacuum_cleanup`;
//! the block-sampling read stream — now bypassed in the owned model) are
//! installed by their owners (pgstat) or are model-unreachable and panic loudly.

use analyze_rt_seams as rt;
use commands_analyze_seams as analyze;

pub fn init_seams() {
    analyze::analyze_rel::set(crate::analyze_rel);
    analyze::std_typanalyze::set(crate::std_typanalyze);
    analyze::std_compute_stats::set(crate::std_compute_stats);
    analyze::examine_expression::set(crate::examine_expression);
    // analyze.c owns the `default_statistics_target` GUC global.
    crate::install_default_statistics_target_guc();

    // pg_statistic catalog insert/update (update_attstats) → the generic
    // catalog-tuple writers.
    rt::catalog_tuple_insert_with_info_pg_statistic::set(
        |mcx, sd, tup, indstate| {
            indexing::keystone::CatalogTupleInsertWithInfo(mcx, sd, tup, indstate)
        },
    );
    rt::catalog_tuple_update_with_info_pg_statistic::set(
        |mcx, sd, otid, tup, indstate| {
            indexing::keystone::CatalogTupleUpdateWithInfo(mcx, sd, otid, tup, indstate)
        },
    );

    // ComputeExtStatisticsRows / BuildRelationExtStatistics are installed by
    // their real owner crate `backend-statistics-extended-stats` (the
    // extended_stats.c entry-point port: real pg_statistic_ext scan + empty-case
    // early return), not here.
}
