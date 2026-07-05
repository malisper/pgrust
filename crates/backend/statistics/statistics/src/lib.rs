#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod builtins;
pub mod dependencies;
pub mod expression;
pub mod mcv;
pub mod mvdistinct;
pub mod sortitem;

pub use expression::{ExprStatsCompute, ExprStatsRow};

use backend_progress::progress::{
    PROGRESS_ANALYZE_EXT_STATS_COMPUTED, PROGRESS_ANALYZE_EXT_STATS_TOTAL, PROGRESS_ANALYZE_PHASE,
    PROGRESS_ANALYZE_PHASE_COMPUTE_EXT_STATS,
};
use backend_progress::{pgstat_progress_update_multi_param, pgstat_progress_update_param};
use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::fmgr::F_OIDEQ;
use types_core::{AttrNumber, Oid};
use types_error::{ErrorLocation, PgResult, WARNING};
use types_rel::Relation;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_tuple::{HeapTupleData, TupleDescData};

use elog::ereport;
use sortitem::{ItemStore, MultiSort, SortItem};

pub const STATS_EXT_NDISTINCT: u8 = b'd';
pub const STATS_EXT_DEPENDENCIES: u8 = b'f';
pub const STATS_EXT_MCV: u8 = b'm';
pub const STATS_EXT_EXPRESSIONS: u8 = b'e';

pub const STATS_MAX_DIMENSIONS: usize = 8;
pub const StatisticExtRelationId: Oid = 3381;
pub const StatisticExtDataRelationId: Oid = 3429;
pub const StatisticExtRelidIndexId: Oid = 3379;

pub const Natts_pg_statistic_ext: usize = 9;
pub const Anum_pg_statistic_ext_oid: i32 = 1;
pub const Anum_pg_statistic_ext_stxrelid: i32 = 2;
pub const Anum_pg_statistic_ext_stxname: i32 = 3;
pub const Anum_pg_statistic_ext_stxnamespace: i32 = 4;
pub const Anum_pg_statistic_ext_stxowner: i32 = 5;
pub const Anum_pg_statistic_ext_stxkeys: i32 = 6;
pub const Anum_pg_statistic_ext_stxstattarget: i32 = 7;
pub const Anum_pg_statistic_ext_stxkind: i32 = 8;
pub const Anum_pg_statistic_ext_stxexprs: i32 = 9;

pub const Natts_pg_statistic_ext_data: usize = 6;
pub const Anum_pg_statistic_ext_data_stxoid: i32 = 1;
pub const Anum_pg_statistic_ext_data_stxdinherit: i32 = 2;
pub const Anum_pg_statistic_ext_data_stxdndistinct: i32 = 3;
pub const Anum_pg_statistic_ext_data_stxddependencies: i32 = 4;
pub const Anum_pg_statistic_ext_data_stxdmcv: i32 = 5;
pub const Anum_pg_statistic_ext_data_stxdexpr: i32 = 6;

const WIDTH_THRESHOLD: usize = 1024;

const ROW_EXCLUSIVE_LOCK: types_rel::LOCKMODE = 3;

#[derive(Clone, Copy)]
pub struct ColStats {
    pub tupattnum: i32,
    pub attstattarget: i32,
    pub attrtypid: Oid,
    pub attrcollid: Oid,
    pub typlen: i16,
    pub typbyval: bool,
}

pub struct StatsBuildData<'mcx> {
    pub numrows: usize,
    pub attnums: PgVec<'mcx, AttrNumber>,
    pub stats: PgVec<'mcx, ColStats>,
    pub values: PgVec<'mcx, PgVec<'mcx, Datum>>,
    pub nulls: PgVec<'mcx, PgVec<'mcx, bool>>,
}

pub struct StatExtEntry<'mcx> {
    pub statOid: Oid,
    pub schema: PgVec<'mcx, u8>,
    pub name: PgVec<'mcx, u8>,
    pub columns: PgVec<'mcx, AttrNumber>,
    pub types: PgVec<'mcx, u8>,
    pub stattarget: i32,
    pub exprs: PgVec<'mcx, types_nodes::Node<'mcx>>,
}

fn oid_key(attno: i32, arg: Oid) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno as AttrNumber;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info({F_OIDEQ}) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(arg);
    key
}

fn getattr(tup: &HeapTupleData<'_>, attnum: i32, desc: &TupleDescData<'_>) -> (Datum, bool) {
    let mut isnull = false;
    // SAFETY: tuple comes from the relation described by `desc`.
    let d = unsafe { types_tuple::heap_getattr(tup, attnum, desc, &mut isnull) };
    (d, isnull)
}

// Inline varlena payload; external/compressed never occur for the fresh
// int2vector/char[] catalog values read here.
fn varlena_body<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null varlena datum into a live catalog tuple.
    unsafe {
        let b0 = *p;
        if b0 == 0x01 || (b0 & 0x03) == 0x02 {
            panic!("statistics: unexpected toasted catalog array");
        }
        if b0 & 0x01 != 0 {
            let len = ((b0 as usize) >> 1) & 0x7F;
            core::slice::from_raw_parts(p.add(1), len - 1)
        } else {
            let w = u32::from_ne_bytes(*(p as *const [u8; 4]));
            core::slice::from_raw_parts(p.add(4), ((w as usize) >> 2) - 4)
        }
    }
}

fn read_i32(b: &[u8], off: usize) -> i32 {
    i32::from_ne_bytes(b[off..off + 4].try_into().unwrap())
}

// 1-D no-null array payload: 20-byte header, then elements.
fn array_elems(body: &[u8]) -> (usize, &[u8]) {
    assert_eq!(read_i32(body, 0), 1, "stx array is not 1-D");
    assert_eq!(read_i32(body, 4), 0, "stx array has nulls");
    let n = read_i32(body, 12) as usize;
    (n, &body[20..])
}

pub fn fetch_statentries_for_relation<'mcx>(
    mcx: Mcx<'mcx>,
    pg_statext: &Relation<'mcx>,
    relid: Oid,
) -> PgResult<PgVec<'mcx, StatExtEntry<'mcx>>> {
    let mut result: PgVec<'mcx, StatExtEntry<'mcx>> = PgVec::new_in(mcx);
    let keys = [oid_key(Anum_pg_statistic_ext_stxrelid, relid)];
    let mut scan =
        genam::systable_beginscan(mcx, pg_statext, StatisticExtRelidIndexId, true, None, &keys)?;
    let desc = pg_statext.descr();
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let (oid_d, _) = getattr(tup, Anum_pg_statistic_ext_oid, desc);
        let (nsp_d, _) = getattr(tup, Anum_pg_statistic_ext_stxnamespace, desc);
        let mut schema: PgVec<'mcx, u8> = PgVec::new_in(mcx);
        if let Some(s) = lsyscache::get_namespace_name(mcx, nsp_d.as_oid())? {
            schema.extend_from_slice(s.as_str().as_bytes());
        }
        let (name_d, _) = getattr(tup, Anum_pg_statistic_ext_stxname, desc);
        let mut name: PgVec<'mcx, u8> = PgVec::new_in(mcx);
        // SAFETY: NameData is a 64-byte NUL-padded field inside the tuple.
        unsafe {
            let p = name_d.as_usize() as *const u8;
            let mut n = 0;
            while n < 64 && *p.add(n) != 0 {
                n += 1;
            }
            name.extend_from_slice(core::slice::from_raw_parts(p, n));
        }

        let (keys_d, _) = getattr(tup, Anum_pg_statistic_ext_stxkeys, desc);
        let (nkeys, keydata) = array_elems(varlena_body(keys_d));
        let mut columns: PgVec<'mcx, AttrNumber> = mcx::vec_with_capacity_in(mcx, nkeys)?;
        for i in 0..nkeys {
            columns.push(i16::from_ne_bytes(keydata[i * 2..i * 2 + 2].try_into().unwrap()));
        }

        let (target_d, target_null) = getattr(tup, Anum_pg_statistic_ext_stxstattarget, desc);
        let stattarget = if target_null { -1 } else { target_d.as_i16() as i32 };

        let (kind_d, _) = getattr(tup, Anum_pg_statistic_ext_stxkind, desc);
        let (nkinds, kinddata) = array_elems(varlena_body(kind_d));
        let mut types: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, nkinds)?;
        types.extend_from_slice(&kinddata[..nkinds]);

        let (exprs_d, exprs_null) = getattr(tup, Anum_pg_statistic_ext_stxexprs, desc);
        let exprs = if exprs_null {
            PgVec::new_in(mcx)
        } else {
            expression::decode_stxexprs(mcx, exprs_d)?
        };

        result.push(StatExtEntry {
            statOid: oid_d.as_oid(),
            schema,
            name,
            columns,
            types,
            stattarget,
            exprs,
        });
    }
    genam::systable_endscan(mcx, scan)?;
    Ok(result)
}

fn lookup_var_attr_stats<'mcx>(
    mcx: Mcx<'mcx>,
    columns: &[AttrNumber],
    colstats: &[ColStats],
) -> PgResult<Option<PgVec<'mcx, ColStats>>> {
    let mut stats: PgVec<'mcx, ColStats> = mcx::vec_with_capacity_in(mcx, columns.len())?;
    for &attnum in columns {
        match colstats.iter().find(|s| s.tupattnum == attnum as i32) {
            Some(s) => stats.push(*s),
            None => return Ok(None),
        }
    }
    Ok(Some(stats))
}

fn statext_compute_stattarget(stattarget: i32, stats: &[ColStats]) -> i32 {
    if stattarget >= 0 {
        return stattarget;
    }
    let mut target = stattarget;
    for s in stats {
        if s.attstattarget > target {
            target = s.attstattarget;
        }
    }
    if target < 0 {
        target = guc_tables::vars::default_statistics_target.read();
    }
    target
}

pub fn ComputeExtStatisticsRows(mcx: Mcx<'_>, relid: Oid, colstats: &[ColStats]) -> PgResult<i32> {
    if colstats.is_empty() {
        return Ok(0);
    }
    let pg_stext = table::table_open(mcx, StatisticExtRelationId, ROW_EXCLUSIVE_LOCK)?;
    let lstats = fetch_statentries_for_relation(mcx, &pg_stext, relid)?;
    let mut result = 0;
    for stat in lstats.iter() {
        let Some(stats) = lookup_var_attr_stats(mcx, &stat.columns, colstats)? else {
            continue;
        };
        let stattarget = statext_compute_stattarget(stat.stattarget, &stats);
        if stattarget > result {
            result = stattarget;
        }
    }
    table::table_close(pg_stext, ROW_EXCLUSIVE_LOCK)?;
    Ok(300 * result)
}

pub fn BuildRelationExtStatistics<'mcx, F: ExprStatsCompute<'mcx>>(
    mcx: Mcx<'mcx>,
    onerel: &Relation<'mcx>,
    inh: bool,
    totalrows: f64,
    rows: &[HeapTupleData<'_>],
    colstats: &[ColStats],
    expr_compute: &mut F,
) -> PgResult<()> {
    if colstats.is_empty() {
        return Ok(());
    }
    let pg_stext = table::table_open(mcx, StatisticExtRelationId, ROW_EXCLUSIVE_LOCK)?;
    let statslist = fetch_statentries_for_relation(mcx, &pg_stext, onerel.rd_id)?;

    if !statslist.is_empty() {
        pgstat_progress_update_multi_param(
            &[PROGRESS_ANALYZE_PHASE, PROGRESS_ANALYZE_EXT_STATS_TOTAL],
            &[
                PROGRESS_ANALYZE_PHASE_COMPUTE_EXT_STATS,
                statslist.len() as i64,
            ],
        );
    }

    let mut ext_cnt: i64 = 0;
    for stat in statslist.iter() {
        let bcx = mcx::MemoryContext::new("BuildRelationExtStatistics");
        let bmcx = bcx.mcx();
        let Some(stats) = lookup_var_attr_stats(bmcx, &stat.columns, colstats)? else {
            let nsp = lsyscache::get_namespace_name(bmcx, onerel.rd_rel.relnamespace)?
                .map(|s| s.as_str().to_string())
                .unwrap_or_default();
            ereport(WARNING)
                .errcode(types_error::ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg(format!(
                    "statistics object \"{}.{}\" could not be computed for relation \"{}.{}\"",
                    core::str::from_utf8(&stat.schema).unwrap_or("?"),
                    core::str::from_utf8(&stat.name).unwrap_or("?"),
                    nsp,
                    onerel.name(),
                ))
                .finish(ErrorLocation { filename: None, lineno: 0, funcname: None })?;
            continue;
        };
        let stattarget = statext_compute_stattarget(stat.stattarget, &stats);
        if stattarget == 0 {
            continue;
        }

        let data = make_build_data(mcx, bmcx, onerel, stat, rows, &stats, stattarget)?;

        let mut ndistinct: Option<PgVec<'_, u8>> = None;
        let mut deps: Option<PgVec<'_, u8>> = None;
        let mut mcv_ser: Option<PgVec<'_, u8>> = None;
        let mut exprstats: Option<PgVec<'_, u8>> = None;
        for &t in stat.types.iter() {
            if t == STATS_EXT_NDISTINCT {
                let nd = mvdistinct::statext_ndistinct_build(bmcx, totalrows, &data)?;
                ndistinct = Some(mvdistinct::statext_ndistinct_serialize(bmcx, &nd)?);
            } else if t == STATS_EXT_DEPENDENCIES {
                if let Some(d) = dependencies::statext_dependencies_build(bmcx, &data)? {
                    deps = Some(dependencies::statext_dependencies_serialize(bmcx, &d)?);
                }
            } else if t == STATS_EXT_MCV {
                if let Some(m) = mcv::statext_mcv_build(bmcx, &data, totalrows, stattarget)? {
                    mcv_ser = Some(mcv::statext_mcv_serialize(bmcx, &m, &data.stats)?);
                }
            } else if t == STATS_EXT_EXPRESSIONS {
                assert!(
                    !stat.exprs.is_empty(),
                    "requested expression stats, but there are no expressions"
                );
                let rows_stats = expr_compute.compute(mcx, onerel, &stat.exprs, stattarget, rows)?;
                exprstats = Some(expression::serialize_expr_stats(bmcx, &rows_stats)?);
            }
        }

        statext_store(
            bmcx,
            stat.statOid,
            inh,
            ndistinct.as_deref(),
            deps.as_deref(),
            mcv_ser.as_deref(),
            exprstats.as_deref(),
        )?;

        ext_cnt += 1;
        pgstat_progress_update_param(PROGRESS_ANALYZE_EXT_STATS_COMPUTED, ext_cnt);
    }

    table::table_close(pg_stext, ROW_EXCLUSIVE_LOCK)?;
    Ok(())
}

fn statext_store(
    mcx: Mcx<'_>,
    statOid: Oid,
    inh: bool,
    ndistinct: Option<&[u8]>,
    dependencies: Option<&[u8]>,
    mcv: Option<&[u8]>,
    exprs: Option<&[u8]>,
) -> PgResult<()> {
    let pg_stextdata = table::table_open(mcx, StatisticExtDataRelationId, ROW_EXCLUSIVE_LOCK)?;

    let mut values = [Datum::null(); Natts_pg_statistic_ext_data];
    let mut nulls = [true; Natts_pg_statistic_ext_data];
    values[Anum_pg_statistic_ext_data_stxoid as usize - 1] = Datum::from_oid(statOid);
    nulls[Anum_pg_statistic_ext_data_stxoid as usize - 1] = false;
    values[Anum_pg_statistic_ext_data_stxdinherit as usize - 1] = Datum::from_bool(inh);
    nulls[Anum_pg_statistic_ext_data_stxdinherit as usize - 1] = false;

    if let Some(d) = ndistinct {
        values[Anum_pg_statistic_ext_data_stxdndistinct as usize - 1] =
            Datum::from_usize(d.as_ptr() as usize);
        nulls[Anum_pg_statistic_ext_data_stxdndistinct as usize - 1] = false;
    }
    if let Some(d) = dependencies {
        values[Anum_pg_statistic_ext_data_stxddependencies as usize - 1] =
            Datum::from_usize(d.as_ptr() as usize);
        nulls[Anum_pg_statistic_ext_data_stxddependencies as usize - 1] = false;
    }
    if let Some(d) = mcv {
        values[Anum_pg_statistic_ext_data_stxdmcv as usize - 1] =
            Datum::from_usize(d.as_ptr() as usize);
        nulls[Anum_pg_statistic_ext_data_stxdmcv as usize - 1] = false;
    }
    if let Some(d) = exprs {
        values[Anum_pg_statistic_ext_data_stxdexpr as usize - 1] =
            Datum::from_usize(d.as_ptr() as usize);
        nulls[Anum_pg_statistic_ext_data_stxdexpr as usize - 1] = false;
    }

    statscmds::RemoveStatisticsDataById(mcx, statOid, inh)?;

    let mut stup = heaptuple::heap_form_tuple(mcx, pg_stextdata.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &pg_stextdata, &mut stup)?;

    table::table_close(pg_stextdata, ROW_EXCLUSIVE_LOCK)?;
    Ok(())
}

fn make_build_data<'mcx, 'b>(
    mcx: Mcx<'mcx>,
    bmcx: Mcx<'b>,
    onerel: &Relation<'mcx>,
    stat: &StatExtEntry<'mcx>,
    rows: &[HeapTupleData<'_>],
    stats: &[ColStats],
    stattarget: i32,
) -> PgResult<StatsBuildData<'b>> {
    let nkeys = stat.columns.len() + stat.exprs.len();
    let numrows = rows.len();
    let tupdesc = onerel.descr();

    let mut attnums: PgVec<'b, AttrNumber> = mcx::vec_with_capacity_in(bmcx, nkeys)?;
    let mut statsv: PgVec<'b, ColStats> = mcx::vec_with_capacity_in(bmcx, nkeys)?;
    let mut values: PgVec<'b, PgVec<'b, Datum>> = PgVec::new_in(bmcx);
    let mut nulls: PgVec<'b, PgVec<'b, bool>> = PgVec::new_in(bmcx);

    for (idx, &k) in stat.columns.iter().enumerate() {
        attnums.push(k);
        statsv.push(stats[idx]);
        let mut v: PgVec<'b, Datum> = mcx::vec_with_capacity_in(bmcx, numrows)?;
        let mut n: PgVec<'b, bool> = mcx::vec_with_capacity_in(bmcx, numrows)?;
        for row in rows {
            let (d, isnull) = getattr(row, k as i32, tupdesc);
            v.push(d);
            n.push(isnull);
        }
        values.push(v);
        nulls.push(n);
    }

    // Expression columns carry negative attnums (-1, -2, ...) per C.
    for (j, &e) in stat.exprs.iter().enumerate() {
        attnums.push(-(j as AttrNumber) - 1);
        statsv.push(expression::expr_col_stats(e, stattarget)?);
    }
    if !stat.exprs.is_empty() {
        expression::eval_exprs(mcx, bmcx, onerel, &stat.exprs, rows, &mut values, &mut nulls)?;
    }

    Ok(StatsBuildData { numrows, attnums, stats: statsv, values, nulls })
}

// toast_raw_datum_size (detoast.c), for the WIDTH_THRESHOLD test.
fn raw_datum_size(d: Datum) -> usize {
    let p = d.as_usize() as *const u8;
    // SAFETY: byref varlena datum into live sample-tuple memory.
    unsafe {
        let b0 = *p;
        if b0 == 0x01 {
            let tag = *p.add(1);
            if tag != 18 {
                panic!("raw_datum_size: unsupported vartag {tag}");
            }
            let mut raw = [0u8; 4];
            core::ptr::copy_nonoverlapping(p.add(2), raw.as_mut_ptr(), 4);
            i32::from_ne_bytes(raw) as usize
        } else if b0 & 0x01 != 0 {
            let len = ((b0 as usize) >> 1) & 0x7F;
            len - 1 + 4
        } else if (b0 & 0x03) == 0x02 {
            let ext = u32::from_ne_bytes(*(p.add(4) as *const [u8; 4]));
            (ext & 0x3FFF_FFFF) as usize + 4
        } else {
            let w = u32::from_ne_bytes(*(p as *const [u8; 4]));
            (w >> 2) as usize
        }
    }
}

fn is_plain_inline(d: Datum) -> bool {
    let p = d.as_usize() as *const u8;
    // SAFETY: as above.
    unsafe {
        let b0 = *p;
        b0 != 0x01 && (b0 & 0x03) != 0x02
    }
}

pub fn build_mss(stats: &[ColStats], dims: &[usize]) -> PgResult<MultiSort> {
    let mut mss = MultiSort::init(dims.len());
    for &d in dims {
        mss.add_dimension(stats[d].attrtypid, stats[d].attrcollid)?;
    }
    Ok(mss)
}

// build_sorted_items (extended_stats.c): row-major copy of the selected
// dimensions, too-wide varlenas dropped, remaining varlenas detoasted, then
// the C-exact qsort under the multi-sort comparator.
pub fn build_sorted_items<'mcx>(
    mcx: Mcx<'mcx>,
    data: &StatsBuildData<'_>,
    mss: &mut MultiSort,
    dims: &[usize],
) -> PgResult<Option<(PgVec<'mcx, SortItem>, ItemStore<'mcx>)>> {
    let width = dims.len();
    let numrows = data.numrows;
    let mut values: PgVec<'mcx, Datum> = mcx::vec_with_capacity_in(mcx, numrows * width)?;
    let mut isnull: PgVec<'mcx, bool> = mcx::vec_with_capacity_in(mcx, numrows * width)?;
    let mut keepalive: PgVec<'mcx, PgVec<'mcx, u8>> = PgVec::new_in(mcx);

    let mut nrows: u32 = 0;
    for i in 0..numrows {
        let mut toowide = false;
        let base = values.len();
        for &j in dims {
            let mut value = data.values[j][i];
            let vnull = data.nulls[j][i];
            if !vnull && data.stats[j].typlen == -1 {
                if raw_datum_size(value) > WIDTH_THRESHOLD {
                    toowide = true;
                    break;
                }
                if !is_plain_inline(value) {
                    let p = value.as_usize() as *const u8;
                    // SAFETY: varlena header declares the image length.
                    let src = unsafe {
                        let b0 = *p;
                        let len = if b0 == 0x01 {
                            2 + types_tuple::varatt::vartag_size(*p.add(1))
                        } else {
                            (u32::from_ne_bytes(*(p as *const [u8; 4])) >> 2) as usize
                        };
                        core::slice::from_raw_parts(p, len)
                    };
                    let img = detoast::detoast_attr(mcx, src)?;
                    value = Datum::from_usize(img.as_ptr() as usize);
                    keepalive.push(img);
                }
            }
            values.push(value);
            isnull.push(vnull);
        }
        if toowide {
            values.truncate(base);
            isnull.truncate(base);
            continue;
        }
        nrows += 1;
    }

    if nrows == 0 {
        return Ok(None);
    }

    let mut items: PgVec<'mcx, SortItem> = mcx::vec_with_capacity_in(mcx, nrows as usize)?;
    for off in 0..nrows {
        items.push(SortItem { off, count: 0 });
    }
    let store = ItemStore { values, isnull, width };
    sortitem::pg_qsort(&mut items, |a, b| store.compare(mss, *a, *b));
    // Detoasted images ride mcx until teardown; from ANALYZE this is an
    // exact-accounting Aset that is dropped, never reset (reset would trip
    // the leak assert on these forgotten bytes).
    core::mem::forget(keepalive);
    Ok(Some((items, store)))
}
