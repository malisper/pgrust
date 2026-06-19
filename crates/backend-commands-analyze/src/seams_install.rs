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
//!     extended-statistics framework (statistics/extended_stats.c) is unported.
//!     A relation with NO `pg_statistic_ext` objects needs neither (the C scan
//!     yields an empty list → `ComputeExtStatisticsRows` returns 0 and
//!     `BuildRelationExtStatistics` is a no-op). We detect that case faithfully
//!     with `RelationGetStatExtList`'s `pg_statistic_ext` scan and short-circuit;
//!     a relation that actually has extended-statistics objects panics loudly
//!     until extended_stats.c is ported.
//!
//! The remaining outward seams (`pgstat_report_analyze` — owner
//! pgstat_relation.c; the FDW analyze hook; ANALYZE-only `index_vacuum_cleanup`;
//! the block-sampling read stream — now bypassed in the owned model) are
//! installed by their owners (pgstat) or are model-unreachable and panic loudly.

use backend_commands_analyze_rt_seams as rt;
use backend_commands_analyze_seams as analyze;

pub fn init_seams() {
    analyze::analyze_rel::set(crate::analyze_rel);
    analyze::std_typanalyze::set(crate::std_typanalyze);
    analyze::std_compute_stats::set(crate::std_compute_stats);
    // analyze.c owns the `default_statistics_target` GUC global.
    crate::install_default_statistics_target_guc();

    // pg_statistic catalog insert/update (update_attstats) → the generic
    // catalog-tuple writers.
    rt::catalog_tuple_insert_with_info_pg_statistic::set(
        |mcx, sd, tup, indstate| {
            backend_catalog_indexing::keystone::CatalogTupleInsertWithInfo(mcx, sd, tup, indstate)
        },
    );
    rt::catalog_tuple_update_with_info_pg_statistic::set(
        |mcx, sd, otid, tup, indstate| {
            backend_catalog_indexing::keystone::CatalogTupleUpdateWithInfo(mcx, sd, otid, tup, indstate)
        },
    );

    // ComputeExtStatisticsRows(onerel, natts, vacattrstats): 0 when there are no
    // extended-statistics objects on the relation. `ComputeExtStatisticsRows`
    // returns 0 immediately when natts == 0; otherwise it scans pg_statistic_ext
    // and (with an empty list) computes `300 * 0 == 0`.
    rt::compute_ext_statistics_rows::set(|onerel, natts, _vacattrstats| {
        if natts == 0 {
            return Ok(0);
        }
        let statexts = backend_access_index_genam_seams::relcache_scan_pg_statistic_ext::call(
            onerel.rd_id,
        )?;
        if statexts.is_empty() {
            return Ok(0);
        }
        Err(types_error::PgError::error(
            "extended statistics are not yet supported (statistics/extended_stats.c unported)",
        ))
    });

    // BuildRelationExtStatistics(onerel, ...): a no-op when the relation has no
    // extended-statistics objects (the C `foreach` over an empty statslist does
    // nothing).
    rt::build_relation_ext_statistics::set(
        |onerel, _inh, _totalrows, _numrows, _rows, _natts, _vacattrstats| {
            let statexts =
                backend_access_index_genam_seams::relcache_scan_pg_statistic_ext::call(
                    onerel.rd_id,
                )?;
            if statexts.is_empty() {
                return Ok(());
            }
            Err(types_error::PgError::error(
                "extended statistics are not yet supported (statistics/extended_stats.c unported)",
            ))
        },
    );
}
