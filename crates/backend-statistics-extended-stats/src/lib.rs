//! `statistics/extended_stats.c` — the ANALYZE entry points for extended
//! statistics.
//!
//! Ported here (faithful, 100% logic): the two ANALYZE entry points
//! `ComputeExtStatisticsRows` / `BuildRelationExtStatistics`, the
//! `statext_compute_stattarget` target arithmetic, and
//! `fetch_statentries_for_relation` (the real `pg_statistic_ext` catalog scan
//! that decides whether the relation has any extended-statistics objects).
//!
//! The per-statistics-object build leg (`lookup_var_attr_stats` /
//! `make_build_data` / `statext_{ndistinct,dependencies,mcv}_build` /
//! `statext_store` plus the `StatExtEntry` Form decode: `get_namespace_name`,
//! the `stxkind` char array, `stringToNode`/`eval_const_expressions` for
//! `stxexprs`) bottoms out on the not-yet-ported `StatsBuildData` /
//! multi-sort statistics-build framework (the `VacAttrStats` value-matrix
//! keystone). It is reached ONLY when the catalog scan finds at least one
//! `pg_statistic_ext` row for the relation — i.e. a table created with
//! `CREATE STATISTICS`. Every relation with NO extended statistics (the common
//! case, including all `test_setup` tables) gets an empty scan result and both
//! entry points short-circuit cheaply, exactly as in C.
//!
//! This crate OWNS the two analyze-rt seams `compute_ext_statistics_rows` /
//! `build_relation_ext_statistics` (declared in
//! `backend-commands-analyze-rt-seams`) and installs them in `init_seams()`.

#![allow(non_snake_case)]

use mcx::MemoryContext;
use types_core::primitive::Oid;
use types_error::{PgError, PgResult};
use types_rel::Relation;
use types_statistics::{VacAttrStats, MAX_STATISTICS_TARGET};

use types_catalog::pg_statistic_ext::{
    StatisticExtRelationId, StatisticExtRelidIndexId, Anum_pg_statistic_ext_oid,
    Anum_pg_statistic_ext_stxrelid,
};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::RowExclusiveLock;
use types_tuple::backend_access_common_heaptuple::{Datum, FormedTuple};

use backend_access_common_scankey::ScanKeyInit;
use backend_access_index_genam_seams as genam;
use backend_access_table_table::{table_close, table_open};
use backend_commands_analyze_rt_seams as rt;

use types_core::fmgr::F_OIDEQ;

/// `default_statistics_target` GUC (guc_tables.c). Read from the real GUC slot
/// (installed by analyze), exactly as `statext_compute_stattarget` does in C.
///
/// Used by `statext_compute_stattarget`, which is reached only on the per-object
/// build leg (`build_leg_keystone`); both stay live for when the keystone lands.
#[allow(dead_code)]
fn default_statistics_target() -> i32 {
    backend_utils_misc_guc_tables::vars::default_statistics_target.read()
}

/* ===========================================================================
 * fetch_statentries_for_relation (extended_stats.c:419-516)
 *
 * Scan pg_statistic_ext for entries having stxrelid = this rel. The C function
 * decodes each row into a StatExtEntry (schema/name/columns/types/stattarget/
 * exprs); that decode needs get_namespace_name, the stxkind char-array decode,
 * and stringToNode/eval_const_expressions over the unported planner-arena Node
 * model. Here we run the real scan and return the OIDs of the matching rows:
 * the empty case (no rows) is fully faithful, and a non-empty result hands off
 * to the build leg, which loudly reports the keystone.
 * ======================================================================== */

/// Run the real `pg_statistic_ext` scan for `relid`, returning the OIDs of the
/// matching statistics objects (empty when the relation has no extended
/// statistics). `lockmode` is the lock the caller holds on `pg_statistic_ext`
/// (`RowExclusiveLock` for the build path, matching C — both entry points open
/// it `RowExclusiveLock`).
fn fetch_statentries_for_relation(relid: Oid, lockmode: i32) -> PgResult<Vec<Oid>> {
    let scratch = MemoryContext::new("fetch_statentries_for_relation scan");
    let smcx = scratch.mcx();

    // ScanKeyInit(&skey, Anum_pg_statistic_ext_stxrelid, BTEqualStrategyNumber,
    //             F_OIDEQ, ObjectIdGetDatum(relid));
    let mut skey = [ScanKeyData::empty()];
    ScanKeyInit(
        &mut skey[0],
        Anum_pg_statistic_ext_stxrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(relid),
    )?;

    let pg_statext = table_open(smcx, StatisticExtRelationId, lockmode)?;
    let mut scan =
        genam::systable_beginscan::call(&pg_statext, StatisticExtRelidIndexId, true, None, &skey)?;

    let mut out: Vec<Oid> = Vec::new();
    while let Some(htup) = genam::systable_getnext::call(smcx, scan.desc_mut())? {
        // entry->statOid = ((Form_pg_statistic_ext) GETSTRUCT(htup))->oid;
        let row = backend_access_common_heaptuple::heap_deform_tuple(
            smcx,
            &htup.tuple,
            &pg_statext.rd_att,
            &htup.data,
        )?;
        out.push(row[(Anum_pg_statistic_ext_oid - 1) as usize].0.as_oid());
    }

    scan.end()?;
    table_close(pg_statext, lockmode)?;
    drop(scratch);
    Ok(out)
}

/* ===========================================================================
 * statext_compute_stattarget (extended_stats.c:343-379)
 * ======================================================================== */

/// Compute statistics target for an extended statistic. `stats` is the
/// `VacAttrStats **` array of length `nattrs`. Reached only on the per-object
/// build leg (`build_leg_keystone`); kept live for when the keystone lands.
#[allow(dead_code)]
fn statext_compute_stattarget(mut stattarget: i32, stats: &[&VacAttrStats<'_>]) -> i32 {
    // If there's statistics target set for the statistics object, use it. It
    // may be set to 0 which disables building of that statistic.
    if stattarget >= 0 {
        return stattarget;
    }

    // The target for the statistics object is set to -1; look at the maximum
    // target set for any of the attributes the object is defined on.
    for s in stats {
        if s.attstattarget > stattarget {
            stattarget = s.attstattarget;
        }
    }

    // If still negative, use the global default target.
    if stattarget < 0 {
        stattarget = default_statistics_target();
    }

    debug_assert!((0..=MAX_STATISTICS_TARGET).contains(&stattarget));
    stattarget
}

/* ===========================================================================
 * ComputeExtStatisticsRows (extended_stats.c:261-321)
 * ======================================================================== */

/// Compute number of rows required by extended statistics on a table.
pub fn compute_ext_statistics_rows<'mcx>(
    onerel: &Relation<'mcx>,
    natts: i32,
    vacattrstats: &[VacAttrStats<'mcx>],
) -> PgResult<i32> {
    // If there are no columns to analyze, just return 0.
    if natts == 0 {
        return Ok(0);
    }

    let lstats = fetch_statentries_for_relation(onerel.rd_id, RowExclusiveLock)?;

    // Empty in the common case (no CREATE STATISTICS objects): return 0 rows.
    if lstats.is_empty() {
        return Ok(0);
    }

    // The per-object lookup_var_attr_stats / statext_compute_stattarget walk
    // needs the StatExtEntry decode + the VacAttrStats matrix lookup, which is
    // the unported build-framework keystone.
    let result = build_leg_keystone(onerel, &lstats, true, false, 0.0, 0, &[], natts, vacattrstats)?;

    // compute sample size based on the statistics target.
    Ok(300 * result)
}

/* ===========================================================================
 * BuildRelationExtStatistics (extended_stats.c:110-246)
 * ======================================================================== */

/// Compute requested extended stats, using the rows sampled for the plain
/// (single-column) stats.
pub fn build_relation_ext_statistics<'mcx>(
    onerel: &Relation<'mcx>,
    inh: bool,
    totalrows: f64,
    numrows: i32,
    rows: &[FormedTuple<'mcx>],
    natts: i32,
    vacattrstats: &[VacAttrStats<'mcx>],
) -> PgResult<()> {
    // Do nothing if there are no columns to analyze.
    if natts == 0 {
        return Ok(());
    }

    let statslist = fetch_statentries_for_relation(onerel.rd_id, RowExclusiveLock)?;

    // Empty in the common case: no extended-statistics objects to build.
    if statslist.is_empty() {
        return Ok(());
    }

    // The per-object decode + statext_{ndistinct,dependencies,mcv}_build +
    // statext_store is the unported build-framework keystone.
    build_leg_keystone(
        onerel,
        &statslist,
        false,
        inh,
        totalrows,
        numrows,
        rows,
        natts,
        vacattrstats,
    )?;
    Ok(())
}

/// The per-`pg_statistic_ext`-row decode + build/compute leg of
/// `BuildRelationExtStatistics` / `ComputeExtStatisticsRows`, reached only when
/// the catalog scan found at least one statistics object. The `StatExtEntry`
/// Form decode (`get_namespace_name`, the `stxkind` char array, `stxexprs`
/// `stringToNode`/`eval_const_expressions`), `lookup_var_attr_stats`,
/// `make_build_data`, the `statext_{ndistinct,dependencies,mcv}_build` kernels
/// and `statext_store` all bottom out on the not-yet-ported `StatsBuildData` /
/// multi-sort statistics-build framework; this reports loudly. The empty-stats
/// common case never reaches here.
fn build_leg_keystone<'mcx>(
    _onerel: &Relation<'mcx>,
    _stat_oids: &[Oid],
    _compute_only: bool,
    _inh: bool,
    _totalrows: f64,
    _numrows: i32,
    _rows: &[FormedTuple<'mcx>],
    _natts: i32,
    _vacattrstats: &[VacAttrStats<'mcx>],
) -> PgResult<i32> {
    Err(PgError::error(
        "extended statistics build is unported: the relation has CREATE STATISTICS \
         objects, but the StatsBuildData / multi-sort build framework \
         (lookup_var_attr_stats / make_build_data / statext_*_build / statext_store \
         + the StatExtEntry Form decode) is not yet ported (extended_stats.c build leg)",
    ))
}

/// Install the analyze-rt extended-statistics seams to the real entry points.
pub fn init_seams() {
    rt::compute_ext_statistics_rows::set(compute_ext_statistics_rows);
    rt::build_relation_ext_statistics::set(build_relation_ext_statistics);
}
