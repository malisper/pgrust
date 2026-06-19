//! Outward seams owned by `backend-commands-analyze` for the genuinely-unported
//! (or model-incompatibly-reachable) externals `analyze.c` calls.
//!
//! Each seam mirrors one C call site that cannot be made directly:
//!   * the extended-statistics build framework (`statistics/extended_stats.c`,
//!     unported — no owner crate exists);
//!   * `pgstat_report_analyze` (cumulative-stats reporting, owner pgstat does
//!     not expose it);
//!   * the FDW analyze hook (`GetFdwRoutineForRelation` + `AnalyzeForeignTable`
//!     — the repo's `get_fdw_routine_for_relation` takes a `ForeignScanState`,
//!     not a bare `Relation`, so the ANALYZE-shaped hook is unreachable);
//!   * `index_vacuum_cleanup` (its `backend-commands-vacuum-seams` form is
//!     uninstalled and keyed on `Oid`, incompatible with the real `Relation`
//!     analyze holds).
//!
//! These are DECLARED here and NOT installed by analyze (the owners are
//! unported / do not expose them); a call panics loudly with the precise C
//! call-site rationale baked into the seam doc, exactly as required.

#![allow(non_snake_case)]

extern crate alloc;

use types_core::primitive::{BlockNumber, Oid};
use types_error::PgResult;
use types_rel::Relation;
use types_statistics::VacAttrStats;

seam_core::seam!(
    /// `ComputeExtStatisticsRows(onerel, attr_cnt, vacattrstats)`
    /// (statistics/extended_stats.c): the number of rows the extended-statistics
    /// objects on `onerel` want sampled (so ANALYZE can widen `targrows`).
    /// Owner `backend/statistics/extended_stats.c` is unported (no owner crate);
    /// a call panics loudly. The owned model passes the live per-column
    /// `VacAttrStats` (the C `VacAttrStats **`).
    pub fn compute_ext_statistics_rows<'mcx>(
        onerel: &Relation<'mcx>,
        natts: i32,
        vacattrstats: &[VacAttrStats<'mcx>],
    ) -> PgResult<i32>
);

seam_core::seam!(
    /// `BuildRelationExtStatistics(onerel, inh, totalrows, numrows, rows,
    /// natts, vacattrstats)` (statistics/extended_stats.c): build and store every
    /// extended-statistics object on `onerel`. Owner unported; a call panics.
    pub fn build_relation_ext_statistics<'mcx>(
        onerel: &Relation<'mcx>,
        inh: bool,
        totalrows: f64,
        numrows: i32,
        rows: &[types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>],
        natts: i32,
        vacattrstats: &[VacAttrStats<'mcx>],
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `pgstat_report_analyze(onerel, totalrows, totaldeadrows, resetcounter,
    /// starttime)` (pgstat_relation.c): report the just-completed ANALYZE to the
    /// cumulative stats system. Owner pgstat_relation.c installs it. The relation
    /// is reduced to the `(relid, relisshared, relkind, pgstat_enabled)` facets
    /// the C reads off `onerel->rd_rel` / `onerel->pgstat_enabled`.
    pub fn pgstat_report_analyze(
        relid: Oid,
        relisshared: bool,
        relkind: u8,
        pgstat_enabled: bool,
        livetuples: f64,
        deadtuples: f64,
        resetcounter: bool,
        starttime: i64,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `GetFdwRoutineForRelation(onerel, false)->AnalyzeForeignTable(onerel,
    /// &acquirefunc, &relpages)` (foreign/foreign.c + the FDW): ask the foreign
    /// table's FDW whether it supports ANALYZE and, if so, obtain its sample-row
    /// acquisition shape. The repo's `get_fdw_routine_for_relation` takes a
    /// `ForeignScanState` (executor shape), so the ANALYZE-time hook over a bare
    /// `Relation` is unreachable; a call panics. Returns `Some(relpages)` on
    /// support, `None` when the FDW cannot analyze (C `ok == false`). The
    /// acquirefunc itself is the FDW's; the owner drives the sample acquisition
    /// behind this seam when it lands.
    pub fn analyze_foreign_table<'mcx>(onerel: &Relation<'mcx>) -> PgResult<Option<BlockNumber>>
);

seam_core::seam!(
    /// `index_vacuum_cleanup(&ivinfo, NULL)` (access/index/indexam.c) in
    /// ANALYZE-only mode (`ivinfo.analyze_only == true`): let the index AM do
    /// post-analyze cleanup (a no-op for every core AM except GIN). The
    /// `backend-commands-vacuum-seams` form is uninstalled and `Oid`-keyed
    /// (incompatible with the real `Relation` analyze holds), so analyze owns
    /// this seam; a call panics until the index-AM cleanup path is reachable for
    /// the value model. `index`/`heaprel` are the open relations; the remaining
    /// `IndexVacuumInfo` fields (analyze_only, estimated_count, message_level,
    /// num_heap_tuples) are fixed by the ANALYZE call site.
    pub fn index_vacuum_cleanup_analyze<'mcx>(
        index: &Relation<'mcx>,
        heaprel: &Relation<'mcx>,
        message_level: i32,
        num_heap_tuples: f64,
    ) -> PgResult<()>
);

// The block-sampling read stream (read_stream.c) is bypassed in the owned
// model: `acquire_sample_rows` pulls each BlockSampler-chosen block straight
// from the sampler and pins it through the bufmgr-owned
// `read_buffer_with_strategy` seam (the same owned posture vacuumlazy uses),
// so no analyze-owned read-stream seam is needed.

seam_core::seam!(
    /// `CatalogTupleInsertWithInfo(sd, tup, indstate)` (catalog/indexing.c) for
    /// the pg_statistic insert in `update_attstats`. The catalog-indexing owner
    /// exposes only per-catalog typed variants (pg_class / pg_largeobject), not a
    /// generic `FormedTuple` insert into pg_statistic, so analyze owns this seam;
    /// a call panics until a generic catalog-tuple insert is exposed.
    pub fn catalog_tuple_insert_with_info_pg_statistic<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        sd: &types_rel::Relation<'mcx>,
        tup: &mut types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
        indstate: &mut types_cluster::CatalogIndexState<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `CatalogTupleUpdateWithInfo(sd, &tup->t_self, tup, indstate)`
    /// (catalog/indexing.c) for the pg_statistic update in `update_attstats`.
    /// Owner exposes only per-catalog typed variants; analyze owns this seam.
    pub fn catalog_tuple_update_with_info_pg_statistic<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        sd: &types_rel::Relation<'mcx>,
        otid: types_tuple::heaptuple::ItemPointerData,
        tup: &mut types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
        indstate: &mut types_cluster::CatalogIndexState<'mcx>,
    ) -> PgResult<()>
);

/// `BuildIndexInfo` is reached through the installed
/// `backend-catalog-index-seams::build_index_info` owner; `find_all_inheritors`
/// and `SetRelationHasSubclass` through `backend-catalog-pg-inherits`; they are
/// NOT seamed here.
const _: () = ();
