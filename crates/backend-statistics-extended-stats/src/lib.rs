//! `statistics/extended_stats.c` — the ANALYZE entry points for extended
//! statistics.
//!
//! Ported here (faithful, 100% logic): the two ANALYZE entry points
//! `ComputeExtStatisticsRows` / `BuildRelationExtStatistics`, the
//! `statext_compute_stattarget` target arithmetic, `fetch_statentries_for_relation`
//! (the real `pg_statistic_ext` catalog scan + `StatExtEntry` decode:
//! `get_namespace_name`, the `stxkeys` int2vector, the `stxkind` char array),
//! `lookup_var_attr_stats`, `make_build_data` (the regular-attribute path), and
//! `statext_store` (serialize + `pg_statistic_ext_data` write). The per-kind
//! build kernels (`statext_ndistinct_build` / `statext_dependencies_build`) and
//! the multi-sort support live in the sibling crates
//! (`backend-statistics-{mvdistinct,dependencies}` + `backend-statistics-core`),
//! driven from the build loop here.
//!
//! Two sub-legs are reported (not silently mis-built) rather than ported, since
//! both bottom out on unported neighbors:
//!   * the `stxexprs` expression-statistics leg (`stringToNode` /
//!     `eval_const_expressions` / `compute_expr_stats` / `serialize_expr_stats`
//!     over the planner-arena Node model);
//!   * the MCV build kernel (`statext_mcv_build`: `build_mss` /
//!     `build_distinct_groups` / `build_column_frequencies`), whose core seam has
//!     no owner yet.
//!
//! Every relation with NO extended statistics (the common case, including all
//! `test_setup` tables) gets an empty scan result and both entry points
//! short-circuit cheaply, exactly as in C.
//!
//! This crate OWNS the two analyze-rt seams `compute_ext_statistics_rows` /
//! `build_relation_ext_statistics` (declared in
//! `backend-commands-analyze-rt-seams`) and installs them in `init_seams()`.

#![allow(non_snake_case)]

use mcx::{Mcx, MemoryContext};
use types_core::primitive::Oid;
use types_core::AttrNumber;
use types_error::{PgError, PgResult};
use types_rel::Relation;
use types_statistics::{StatsBuildData, VacAttrStats, MAX_STATISTICS_TARGET};

use types_catalog::pg_statistic_ext::{
    Anum_pg_statistic_ext_oid, Anum_pg_statistic_ext_stxkeys, Anum_pg_statistic_ext_stxkind,
    Anum_pg_statistic_ext_stxname, Anum_pg_statistic_ext_stxnamespace,
    Anum_pg_statistic_ext_stxrelid, Anum_pg_statistic_ext_stxstattarget,
    Anum_pg_statistic_ext_stxexprs,
    Anum_pg_statistic_ext_data_stxdinherit, Anum_pg_statistic_ext_data_stxdndistinct,
    Anum_pg_statistic_ext_data_stxddependencies, Anum_pg_statistic_ext_data_stxdmcv,
    Anum_pg_statistic_ext_data_stxoid, Natts_pg_statistic_ext_data,
    StatisticExtDataRelationId, StatisticExtRelationId, StatisticExtRelidIndexId,
};
use types_statistics::{STATS_EXT_DEPENDENCIES, STATS_EXT_MCV, STATS_EXT_NDISTINCT};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::RowExclusiveLock;
use types_tuple::backend_access_common_heaptuple::{Datum, FormedTuple};

use backend_access_common_scankey::ScanKeyInit;
use backend_access_index_genam_seams as genam;
use backend_access_table_table::{table_close, table_open};
use backend_commands_analyze_rt_seams as rt;
use backend_utils_error::ereport;
use types_error::{ErrorLocation, WARNING};

/// `ErrorLocation` for this module's `ereport(...).finish(...)`.
fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/statistics/extended_stats.c", 0, funcname)
}

use types_core::fmgr::F_OIDEQ;

/// `default_statistics_target` GUC (guc_tables.c). Read from the real GUC slot
/// (installed by analyze), exactly as `statext_compute_stattarget` does in C.
fn default_statistics_target() -> i32 {
    backend_utils_misc_guc_tables::vars::default_statistics_target.read()
}

/* ===========================================================================
 * fetch_statentries_for_relation (extended_stats.c:419-516)
 *
 * Scan pg_statistic_ext for entries having stxrelid = this rel, decoding each
 * row into a StatExtEntry (schema/name/columns/types/stattarget). The
 * `stxexprs` expression leg (stringToNode / eval_const_expressions over the
 * unported planner-arena Node model) is flagged (`has_exprs`) and reported by
 * the build loop rather than mis-decoded. The empty case (no rows) is the
 * common one.
 * ======================================================================== */

/// `StatExtEntry` (extended_stats.c:54) — one decoded `pg_statistic_ext` row.
///
/// `schema`/`name` are only used in the can't-build WARNING; `columns` is the
/// sorted attnum list (the decoded `stxkeys` int2vector); `types` is the decoded
/// `stxkind` char list; `stattarget` is `stxstattarget` (-1 if NULL). The
/// `exprs` leg (`stxexprs`) requires `stringToNode`/`eval_const_expressions`
/// over the unported planner-arena Node model, so an object carrying
/// expressions is reported (`has_exprs`) rather than silently mis-built.
struct StatExtEntry {
    stat_oid: Oid,
    schema: String,
    name: String,
    /// `columns` — the sorted attnum list (bitmapset members, ascending).
    columns: Vec<i32>,
    /// `types` — the enabled statistics-kind chars (`stxkind`).
    types: Vec<i8>,
    stattarget: i32,
    /// true if `stxexprs` is non-NULL (expression statistics — unported decode).
    has_exprs: bool,
}

/// `fetch_statentries_for_relation` (extended_stats.c:419-516). Scan
/// `pg_statistic_ext` for entries with `stxrelid = relid`, decoding each row
/// into a [`StatExtEntry`]. The empty case is the common one (no `CREATE
/// STATISTICS`); a non-empty result drives the build/compute loop.
fn fetch_statentries_for_relation(
    relid: Oid,
    lockmode: i32,
) -> PgResult<Vec<StatExtEntry>> {
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

    let mut result: Vec<StatExtEntry> = Vec::new();
    while let Some(htup) = genam::systable_getnext::call(smcx, scan.desc_mut())? {
        let row = backend_access_common_heaptuple::heap_deform_tuple(
            smcx,
            &htup.tuple,
            &pg_statext.rd_att,
            &htup.data,
        )?;

        // entry->statOid = staForm->oid;
        let stat_oid = row[(Anum_pg_statistic_ext_oid - 1) as usize].0.as_oid();
        // entry->schema = get_namespace_name(staForm->stxnamespace);
        let stxnamespace = row[(Anum_pg_statistic_ext_stxnamespace - 1) as usize].0.as_oid();
        let schema = backend_utils_cache_lsyscache_seams::get_namespace_name::call(smcx, stxnamespace)?
            .map(|s| s.as_str().to_string())
            .unwrap_or_default();
        // entry->name = pstrdup(NameStr(staForm->stxname));
        let name = decode_name(&row[(Anum_pg_statistic_ext_stxname - 1) as usize].0);

        // for (i = 0; i < staForm->stxkeys.dim1; i++)
        //   entry->columns = bms_add_member(entry->columns, stxkeys.values[i]);
        // (the int2vector members; bms ordering == ascending attnums)
        let columns = decode_int2vector(&row[(Anum_pg_statistic_ext_stxkeys - 1) as usize].0)?;

        // entry->stattarget = isnull ? -1 : DatumGetInt16(datum);
        let (st_d, st_null) = &row[(Anum_pg_statistic_ext_stxstattarget - 1) as usize];
        let stattarget = if *st_null { -1 } else { st_d.as_i16() as i32 };

        // decode the stxkind char array into a list of chars
        let (kind_d, kind_null) = &row[(Anum_pg_statistic_ext_stxkind - 1) as usize];
        if *kind_null {
            return Err(PgError::error("stxkind is null".to_string()));
        }
        let types = decode_char_array(kind_d)?;

        // decode expression (if any): non-NULL stxexprs is the unported leg.
        let has_exprs = !row[(Anum_pg_statistic_ext_stxexprs - 1) as usize].1;

        result.push(StatExtEntry {
            stat_oid,
            schema,
            name,
            columns,
            types,
            stattarget,
            has_exprs,
        });
    }

    scan.end()?;
    table_close(pg_statext, lockmode)?;
    drop(scratch);
    Ok(result)
}

/// `NameStr(staForm->stxname)` — decode a fixed 64-byte `NameData` by-ref Datum
/// (NUL-padded) into a `String`.
fn decode_name(d: &Datum<'_>) -> String {
    let bytes = d.as_ref_bytes();
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}

/// Decode an `int2vector` (`stxkeys`) by-ref Datum into its `i32` member list.
/// Layout (`int2vector`, c.h): 24-byte fixed header
/// (vl_len_ 4, ndim 4, dataoffset 4, elemtype 4, dim1 4, lbound1 4) then `dim1`
/// `int16` values.
fn decode_int2vector(d: &Datum<'_>) -> PgResult<Vec<i32>> {
    let b = d.as_ref_bytes();
    if b.len() < 24 {
        return Err(PgError::error("stxkeys: short int2vector".to_string()));
    }
    let dim1 = i32::from_ne_bytes([b[16], b[17], b[18], b[19]]);
    if dim1 < 0 || 24 + (dim1 as usize) * 2 > b.len() {
        return Err(PgError::error("stxkeys: bad int2vector dim1".to_string()));
    }
    let mut out = Vec::with_capacity(dim1 as usize);
    for i in 0..dim1 as usize {
        let off = 24 + i * 2;
        out.push(i16::from_ne_bytes([b[off], b[off + 1]]) as i32);
    }
    Ok(out)
}

/// Decode a 1-D no-nulls `char[]` `ArrayType` (`stxkind`) by-ref Datum into its
/// `i8` element list. Layout: 16-byte ArrayType header (vl_len_ 4, ndim 4,
/// dataoffset 4, elemtype 4) then for ndim==1 a `dims[1]`/`lbound[1]`
/// (4 + 4 bytes), then `dims[0]` 1-byte char elements (no alignment padding for
/// 1-byte type). Mirrors the C `ARR_NDIM == 1 && !ARR_HASNULL &&
/// ARR_ELEMTYPE == CHAROID` validation.
fn decode_char_array(d: &Datum<'_>) -> PgResult<Vec<i8>> {
    let b = d.as_ref_bytes();
    if b.len() < 16 {
        return Err(PgError::error("stxkind: short array".to_string()));
    }
    let ndim = i32::from_ne_bytes([b[4], b[5], b[6], b[7]]);
    let dataoffset = i32::from_ne_bytes([b[8], b[9], b[10], b[11]]);
    // ARR_NDIM(arr) != 1 || ARR_HASNULL(arr) (dataoffset != 0) -> error
    if ndim != 1 || dataoffset != 0 {
        return Err(PgError::error("stxkind is not a 1-D char array".to_string()));
    }
    if b.len() < 24 {
        return Err(PgError::error("stxkind: short array dims".to_string()));
    }
    let ndims0 = i32::from_ne_bytes([b[16], b[17], b[18], b[19]]);
    // ARR_DATA_PTR for no-bitmap, no-padding 1-byte element type starts at
    // sizeof(ArrayType) + 2 * ndim * sizeof(int) = 16 + 8 = 24.
    let data_start = 24usize;
    if ndims0 < 0 || data_start + ndims0 as usize > b.len() {
        return Err(PgError::error("stxkind: bad array dims".to_string()));
    }
    let mut out = Vec::with_capacity(ndims0 as usize);
    for i in 0..ndims0 as usize {
        out.push(b[data_start + i] as i8);
    }
    Ok(out)
}

/* ===========================================================================
 * statext_compute_stattarget (extended_stats.c:343-379)
 * ======================================================================== */

/// Compute statistics target for an extended statistic. `stats` is the
/// `VacAttrStats **` array of length `nattrs`.
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
/// `ComputeExtStatisticsRows` (extended_stats.c:261-321).
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

    let mut result = 0i32;
    for stat in &lstats {
        // Check if we can build this statistics object based on the columns
        // analyzed. If not, ignore it.
        let stats = match lookup_var_attr_stats(stat, natts, vacattrstats) {
            Some(stats) => stats,
            None => continue,
        };

        // Compute statistics target.
        let stattarget = statext_compute_stattarget_for(stat.stattarget, &stats);

        // Use the largest value for all statistics objects.
        if stattarget > result {
            result = stattarget;
        }
    }

    // compute sample size based on the statistics target.
    Ok(300 * result)
}

/* ===========================================================================
 * BuildRelationExtStatistics (extended_stats.c:110-246)
 * ======================================================================== */

/// Compute requested extended stats, using the rows sampled for the plain
/// (single-column) stats. `BuildRelationExtStatistics` (extended_stats.c:110).
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

    for stat in &statslist {
        // Check if we can build these stats based on the columns analyzed.
        let stats = match lookup_var_attr_stats(stat, natts, vacattrstats) {
            Some(stats) => stats,
            None => {
                // ereport(WARNING, "statistics object could not be computed").
                // (We always report — the autovacuum-suppression check is the
                // only difference in C, and this build path is never run from an
                // autovacuum worker here.)
                // get_namespace_name(onerel->rd_rel->relnamespace) +
                // RelationGetRelationName(onerel) — the relation is reported
                // schema-qualified (extended_stats.c:175-178).
                let relname = onerel.name();
                let nmcx = vacattrstats[0]
                    .anl_context
                    .expect("anl_context must be set");
                let relschema =
                    backend_utils_cache_lsyscache_seams::get_namespace_name::call(
                        nmcx,
                        onerel.rd_rel.relnamespace,
                    )?
                    .ok_or_else(|| {
                        PgError::error(format!(
                            "get_namespace_name: namespace {} not found",
                            onerel.rd_rel.relnamespace
                        ))
                    })?;
                let schema = &stat.schema;
                let name = &stat.name;
                ereport(WARNING)
                    .errmsg(format!(
                        "statistics object \"{schema}.{name}\" could not be computed for relation \"{relschema}.{relname}\""
                    ))
                    .finish(here("build_relation_ext_statistics"))?;
                continue;
            }
        };

        // compute statistics target for this statistics object
        let stattarget = statext_compute_stattarget_for(stat.stattarget, &stats);

        // Don't rebuild statistics objects with statistics target set to 0.
        if stattarget == 0 {
            continue;
        }

        // The expression-statistics build leg (stxexprs decode +
        // build_expr_data / compute_expr_stats / serialize_expr_stats) needs
        // stringToNode / eval_const_expressions over the unported planner-arena
        // Node model. Report rather than silently skip if such an object exists.
        if stat.has_exprs {
            return Err(PgError::error(
                "extended statistics on expressions is unported: stxexprs decode \
                 (stringToNode / eval_const_expressions / compute_expr_stats / \
                 serialize_expr_stats) is not yet ported (extended_stats.c build leg)"
                    .to_string(),
            ));
        }

        // evaluate the build data (no expressions: pure heap-attr extraction).
        // The live `vacattrstats` carry the tuple descriptor (the resolved
        // copies do not); all columns share the relation's descriptor.
        let data = make_build_data(onerel, stat, numrows, rows, &stats, vacattrstats)?;

        // compute statistic of each requested type
        let mcx = data_anl_mcx(&stats);
        let mut ndistinct_bytes: Option<Vec<u8>> = None;
        let mut dependencies_bytes: Option<Vec<u8>> = None;
        let mut mcv_bytes: Option<Vec<u8>> = None;

        for &t in &stat.types {
            if t == STATS_EXT_NDISTINCT {
                let n = backend_statistics_mvdistinct::statext_ndistinct_build(
                    mcx,
                    totalrows,
                    &data,
                    data.nattnums,
                    &data.attnums,
                )?;
                // statext_ndistinct_serialize handles NULL (no items) by
                // returning a serialized empty list; statext_store stores it.
                ndistinct_bytes =
                    Some(backend_statistics_mvdistinct::statext_ndistinct_serialize(mcx, &n)?);
            } else if t == STATS_EXT_DEPENDENCIES {
                let deps = backend_statistics_dependencies::statext_dependencies_build(
                    mcx,
                    &data,
                    data.nattnums,
                    &data.attnums,
                )?;
                // statext_dependencies_build returns None when no dependency had
                // a non-zero degree (C NULL) -> the column is left NULL.
                if let Some(deps) = deps {
                    dependencies_bytes = Some(
                        backend_statistics_dependencies::statext_dependencies_serialize(mcx, &deps)?,
                    );
                }
            } else if t == STATS_EXT_MCV {
                // mcvlist = statext_mcv_build(data, totalrows, stattarget);
                // statext_store(... mcvlist ...)
                let mcvlist = backend_statistics_mcv::statext_mcv_build(
                    &data,
                    totalrows,
                    stattarget,
                )?;
                // serialized = statext_mcv_serialize(mcvlist, stats)  (C only when
                // mcvlist != NULL; otherwise the column is left NULL).
                if let Some(mcvlist) = mcvlist {
                    // Per-dimension type metadata the serializer needs
                    // (stats[dim]->attrtype->typlen/typbyval; attrtypid/attrcollid).
                    let mut dimstats: Vec<backend_statistics_mcv::McvDimStats> =
                        Vec::with_capacity(stats.len());
                    for s in &stats {
                        let (typlen, typbyval) =
                            backend_utils_cache_lsyscache_seams::get_typlenbyval::call(
                                s.attrtypid,
                            )?;
                        dimstats.push(backend_statistics_mcv::McvDimStats {
                            attrtypid: s.attrtypid,
                            attrcollid: s.attrcollid,
                            typlen,
                            typbyval,
                        });
                    }
                    mcv_bytes = Some(backend_statistics_mcv::statext_mcv_serialize(
                        mcx, &mcvlist, &dimstats,
                    )?);
                }
            }
            // STATS_EXT_EXPRESSIONS handled by the has_exprs guard above.
        }

        // store the statistics in the catalog
        statext_store(
            mcx,
            stat.stat_oid,
            inh,
            ndistinct_bytes.as_deref(),
            dependencies_bytes.as_deref(),
            mcv_bytes.as_deref(),
        )?;
    }

    Ok(())
}

/* ===========================================================================
 * lookup_var_attr_stats (extended_stats.c:690-749)
 * ======================================================================== */

/// `lookup_var_attr_stats(attrs, exprs, nvacatts, vacatts)`
/// (extended_stats.c:690). Resolve the `VacAttrStats` for each column the
/// statistics object is defined on (matched by `tupattnum`). Returns `None` (C
/// `NULL`) if any required column was not analyzed. The expression leg is gated
/// by the `has_exprs` check before the build loop, so only the column path runs
/// here. The returned `Vec` is a copy of the per-column type metadata sufficient
/// for the build kernels (see [`VacAttrStats::for_ext_build`]).
fn lookup_var_attr_stats<'mcx>(
    stat: &StatExtEntry,
    nvacatts: i32,
    vacatts: &[VacAttrStats<'mcx>],
) -> Option<Vec<VacAttrStats<'mcx>>> {
    let mut stats: Vec<VacAttrStats<'mcx>> = Vec::with_capacity(stat.columns.len());

    // lookup VacAttrStats info for the requested columns (same attnum)
    for &x in &stat.columns {
        let mut found: Option<&VacAttrStats<'mcx>> = None;
        for j in 0..nvacatts as usize {
            if x == vacatts[j].tupattnum {
                found = Some(&vacatts[j]);
                break;
            }
        }
        match found {
            Some(s) => stats.push(VacAttrStats::for_ext_build(
                s.attstattarget,
                s.attrtypid,
                s.attrtypmod,
                s.attrcollid,
                s.anl_context,
            )),
            // stats were not gathered for one of the required columns
            None => return None,
        }
    }

    Some(stats)
}

/// `statext_compute_stattarget` adaptor: build the `&[&VacAttrStats]` view.
fn statext_compute_stattarget_for(stattarget: i32, stats: &[VacAttrStats<'_>]) -> i32 {
    let refs: Vec<&VacAttrStats<'_>> = stats.iter().collect();
    statext_compute_stattarget(stattarget, &refs)
}

/* ===========================================================================
 * make_build_data (extended_stats.c:2448-...) — no-expression path
 * ======================================================================== */

/// `make_build_data(rel, stat, numrows, rows, stats, stattarget)`
/// (extended_stats.c:2448) — assemble the unified `StatsBuildData` from the
/// sampled rows. Only the regular-attribute path is exercised (the expression
/// path is gated by `has_exprs`): for each analyzed column, extract the value of
/// the column from each sampled `HeapTuple` via `heap_getattr` over the live
/// `VacAttrStats` tuple descriptor.
fn make_build_data<'mcx>(
    _rel: &Relation<'mcx>,
    stat: &StatExtEntry,
    numrows: i32,
    rows: &[FormedTuple<'mcx>],
    stats: &[VacAttrStats<'mcx>],
    vacattrstats: &[VacAttrStats<'mcx>],
) -> PgResult<StatsBuildData<'mcx>> {
    let nkeys = stat.columns.len(); // bms_num_members(columns) + 0 exprs

    // result->attnums[idx] = k (the bitmapset members, ascending).
    let attnums: Vec<AttrNumber> = stat.columns.iter().map(|&k| k as AttrNumber).collect();

    // The per-column VacAttrStats copies (already resolved by lookup_var_attr_stats).
    let out_stats: Vec<VacAttrStats<'mcx>> = stats
        .iter()
        .map(|s| {
            VacAttrStats::for_ext_build(
                s.attstattarget,
                s.attrtypid,
                s.attrtypmod,
                s.attrcollid,
                s.anl_context,
            )
        })
        .collect();

    // result->values[idx][i] = heap_getattr(rows[i], k, stats[idx]->tupDesc, ...)
    // We use the live vacattrstats' tup_desc for the deform (the copies carry
    // none); all columns share the same relation tuple descriptor.
    let mut values: Vec<Vec<types_tuple::Datum<'mcx>>> = Vec::with_capacity(nkeys);
    let mut nulls: Vec<Vec<bool>> = Vec::with_capacity(nkeys);

    // All analyzed columns share the relation's tuple descriptor; take it from
    // the first live VacAttrStats (the resolved copies carry none). C reads
    // stats[idx]->tupDesc, which all point at the same descriptor.
    let tupdesc = vacattrstats
        .iter()
        .find_map(|s| s.tup_desc.as_ref())
        .expect("VacAttrStats.tup_desc must be set during ANALYZE");

    for idx in 0..nkeys {
        let k = stat.columns[idx];

        let mut colvals: Vec<types_tuple::Datum<'mcx>> = Vec::with_capacity(numrows as usize);
        let mut colnulls: Vec<bool> = Vec::with_capacity(numrows as usize);
        for i in 0..numrows as usize {
            let (val, isnull) = backend_access_common_heaptuple::heap_getattr(
                stats[idx].anl_context.expect("anl_context"),
                &rows[i],
                k,
                tupdesc,
            )?;
            colvals.push(val);
            colnulls.push(isnull);
        }
        values.push(colvals);
        nulls.push(colnulls);
    }

    Ok(StatsBuildData {
        numrows,
        nattnums: nkeys as i32,
        attnums,
        stats: out_stats,
        values,
        nulls,
    })
}

/* ===========================================================================
 * statext_store (extended_stats.c:759-826)
 * ======================================================================== */

/// `statext_store(statOid, inh, ndistinct, dependencies, mcv, exprs, stats)`
/// (extended_stats.c:759). Serialize the built statistics and store them in the
/// `pg_statistic_ext_data` tuple (delete the old row, insert the new one). The
/// serialized bytes are pre-built varlena `bytea` images; a `None` leg leaves
/// the corresponding column NULL.
fn statext_store<'mcx>(
    mcx: Mcx<'mcx>,
    stat_oid: Oid,
    inh: bool,
    ndistinct: Option<&[u8]>,
    dependencies: Option<&[u8]>,
    mcv: Option<&[u8]>,
) -> PgResult<()> {
    use backend_access_table_table::table_open as topen;

    let pg_stextdata = topen(mcx, StatisticExtDataRelationId, RowExclusiveLock)?;

    let mut values: Vec<types_tuple::Datum<'mcx>> =
        vec![types_tuple::Datum::null(); Natts_pg_statistic_ext_data];
    let mut nulls: Vec<bool> = vec![true; Natts_pg_statistic_ext_data];

    // basic info
    values[(Anum_pg_statistic_ext_data_stxoid - 1) as usize] = Datum::from_oid(stat_oid);
    nulls[(Anum_pg_statistic_ext_data_stxoid - 1) as usize] = false;

    values[(Anum_pg_statistic_ext_data_stxdinherit - 1) as usize] = Datum::from_bool(inh);
    nulls[(Anum_pg_statistic_ext_data_stxdinherit - 1) as usize] = false;

    if let Some(bytes) = ndistinct {
        values[(Anum_pg_statistic_ext_data_stxdndistinct - 1) as usize] =
            types_tuple::Datum::from_byref_bytes_in(mcx, bytes)?;
        nulls[(Anum_pg_statistic_ext_data_stxdndistinct - 1) as usize] = false;
    }
    if let Some(bytes) = dependencies {
        values[(Anum_pg_statistic_ext_data_stxddependencies - 1) as usize] =
            types_tuple::Datum::from_byref_bytes_in(mcx, bytes)?;
        nulls[(Anum_pg_statistic_ext_data_stxddependencies - 1) as usize] = false;
    }
    if let Some(bytes) = mcv {
        values[(Anum_pg_statistic_ext_data_stxdmcv - 1) as usize] =
            types_tuple::Datum::from_byref_bytes_in(mcx, bytes)?;
        nulls[(Anum_pg_statistic_ext_data_stxdmcv - 1) as usize] = false;
    }

    // Delete the old tuple if it exists, then insert the new one.
    backend_commands_statscmds::RemoveStatisticsDataById(mcx, stat_oid, inh)?;

    // form and insert a new tuple
    let mut stup = backend_access_common_heaptuple::heap_form_tuple(
        mcx,
        &pg_stextdata.rd_att,
        &values,
        &nulls,
    )
    .map_err(|e| PgError::error(format!("statext_store: heap_form_tuple: {e:?}")))?;

    backend_catalog_indexing::keystone::CatalogTupleInsert(mcx, &pg_stextdata, &mut stup)?;

    pg_stextdata.close(RowExclusiveLock)?;
    Ok(())
}

/// The long-lived ANALYZE `MemoryContext` carried by the resolved per-column
/// stats (used to drive the build kernels and serialize the results).
fn data_anl_mcx<'mcx>(stats: &[VacAttrStats<'mcx>]) -> Mcx<'mcx> {
    stats[0].anl_context.expect("anl_context must be set")
}

/// Install the analyze-rt extended-statistics seams to the real entry points.
pub fn init_seams() {
    rt::compute_ext_statistics_rows::set(compute_ext_statistics_rows);
    rt::build_relation_ext_statistics::set(build_relation_ext_statistics);
}
