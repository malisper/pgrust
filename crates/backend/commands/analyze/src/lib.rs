#![allow(non_snake_case)]

mod range_typanalyze;
pub mod sampling;

use datum::Datum;
use mcx::{Mcx, MemoryContext, PgVec};
use types_core::{AttrNumber, BlockNumber, ForkNumber, InvalidOid, Oid};
use types_core::fmgr::{F_BOOLEQ, F_INT2EQ, F_OIDEQ};
use types_error::{PgError, PgResult};
use types_nodes::parsenodes::{VacuumRelation, VacuumStmt};
use types_rel::{Relation, RELKIND_MATVIEW, RELKIND_RELATION};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_slot::SlotData;
use types_tuple::{FormData_pg_attribute, HeapTupleData, TupleDescData};

const VACOPT_VERBOSE: i32 = 0x04;

const STATISTIC_RELATION_ID: Oid = 2619;
const STATISTIC_NUM_SLOTS: usize = 5;
const STATISTIC_KIND_MCV: i16 = 1;
const STATISTIC_KIND_HISTOGRAM: i16 = 2;
const STATISTIC_KIND_CORRELATION: i16 = 3;

const NATTS_PG_STATISTIC: usize = 31;
const ANUM_PG_STATISTIC_STARELID: usize = 1;
const ANUM_PG_STATISTIC_STAATTNUM: usize = 2;
const ANUM_PG_STATISTIC_STAINHERIT: usize = 3;
const ANUM_PG_STATISTIC_STANULLFRAC: usize = 4;
const ANUM_PG_STATISTIC_STAWIDTH: usize = 5;
const ANUM_PG_STATISTIC_STADISTINCT: usize = 6;
const ANUM_PG_STATISTIC_STAKIND1: usize = 7;
const ANUM_PG_STATISTIC_STAOP1: usize = 12;
const ANUM_PG_STATISTIC_STACOLL1: usize = 17;
const ANUM_PG_STATISTIC_STANUMBERS1: usize = 22;
const ANUM_PG_STATISTIC_STAVALUES1: usize = 27;

const FLOAT4OID: Oid = 700;
const WIDTH_THRESHOLD: usize = 1024;

const SHARE_UPDATE_EXCLUSIVE_LOCK: types_rel::LOCKMODE = 4;
const ROW_EXCLUSIVE_LOCK: types_rel::LOCKMODE = 3;
const NO_LOCK: types_rel::LOCKMODE = 0;

pub struct VacuumParams {
    pub options: i32,
}

enum ComputeStats {
    Scalar,
    Trivial,
    Range { is_multirange: bool },
}

struct StdAnalyzeData {
    eqopr: Oid,
    ltopr: Oid,
}

pub(crate) struct VacAttrStats<'mcx> {
    tupattnum: i32,
    attstattarget: i32,
    attrtypid: Oid,
    attrcollid: Oid,
    typlen: i16,
    typbyval: bool,
    typalign: u8,
    compute: ComputeStats,
    extra: StdAnalyzeData,
    minrows: i32,

    stats_valid: bool,
    stanullfrac: f32,
    stawidth: i32,
    stadistinct: f32,
    stakind: [i16; STATISTIC_NUM_SLOTS],
    staop: [Oid; STATISTIC_NUM_SLOTS],
    stacoll: [Oid; STATISTIC_NUM_SLOTS],
    stanumbers: [PgVec<'mcx, f32>; STATISTIC_NUM_SLOTS],
    stavalues: [PgVec<'mcx, Datum>; STATISTIC_NUM_SLOTS],
    // C's stavalues-vs-NULL distinction: an empty stored array (range length
    // histogram) is not a NULL column.
    stavalues_set: [bool; STATISTIC_NUM_SLOTS],
    statypid: [Oid; STATISTIC_NUM_SLOTS],
    statyplen: [i16; STATISTIC_NUM_SLOTS],
    statypbyval: [bool; STATISTIC_NUM_SLOTS],
    statypalign: [u8; STATISTIC_NUM_SLOTS],
}

pub fn ExecVacuum(mcx: Mcx<'_>, stmt: &VacuumStmt<'_>, is_top_level: bool) -> PgResult<()> {
    if stmt.is_vacuumcmd {
        panic!("ExecVacuum (vacuum.c): VACUUM lane (commands_vacuum unit)");
    }
    let mut verbose = false;
    for opt in stmt.options.iter() {
        let d = opt.as_def_elem().expect("utility option DefElem");
        match d.defname.expect("option name") {
            "verbose" => verbose = def_get_boolean(d)?,
            other => {
                return Err(PgError::error(format!(
                    "unrecognized ANALYZE option \"{other}\""
                ))
                .into())
            }
        }
    }
    let params = VacuumParams {
        options: 0x02 | if verbose { VACOPT_VERBOSE } else { 0 },
    };
    vacuum(mcx, &stmt.rels, &params, is_top_level)
}

fn vacuum(
    mcx: Mcx<'_>,
    rels: &types_nodes::NodeList<'_>,
    params: &VacuumParams,
    is_top_level: bool,
) -> PgResult<()> {
    if rels.is_nil() {
        panic!("vacuum (vacuum.c): get_all_vacuum_rels (database-wide ANALYZE lane)");
    }
    let in_outer_xact = xact::IsInTransactionBlock(is_top_level);
    if rels.iter().count() > 1 && !in_outer_xact {
        panic!("vacuum (vacuum.c): use_own_xacts multi-relation ANALYZE lane");
    }
    for reln in rels.iter() {
        let vrel: &VacuumRelation<'_> = reln.as_vacuum_relation().expect("VacuumRelation");
        let rv = vrel.relation.expect("ANALYZE relation name");
        let rv = rv.as_range_var().expect("RangeVar");
        let rv = rel_vocab::RangeVar {
            catalogname: rv.catalogname,
            schemaname: rv.schemaname,
            relname: rv.relname.expect("relname"),
            inh: rv.inh,
            relpersistence: rv.relpersistence as u8,
            location: rv.location,
        };
        let rel = table::table_openrv(mcx, &rv, SHARE_UPDATE_EXCLUSIVE_LOCK)?;
        let relid = rel.rd_id;
        table::table_close(rel, NO_LOCK)?;
        analyze_rel(mcx, relid, &vrel.va_cols, params, in_outer_xact)?;
    }
    Ok(())
}

pub fn analyze_rel(
    mcx: Mcx<'_>,
    relid: Oid,
    va_cols: &types_nodes::NodeList<'_>,
    params: &VacuumParams,
    in_outer_xact: bool,
) -> PgResult<()> {
    // vacuum_open_relation's try-lock/skip and vacuum_is_permitted_for_relation
    // owner checks are the commands_vacuum unit's; here the open is plain and
    // permission is the caller's.
    let onerel = table::table_open(mcx, relid, SHARE_UPDATE_EXCLUSIVE_LOCK)?;
    if onerel.rd_id == STATISTIC_RELATION_ID {
        table::table_close(onerel, SHARE_UPDATE_EXCLUSIVE_LOCK)?;
        return Ok(());
    }
    let relkind = onerel.rd_rel.relkind;
    if !(relkind == RELKIND_RELATION || relkind == RELKIND_MATVIEW) {
        panic!("analyze_rel (analyze.c): relkind {relkind}; foreign/partitioned ANALYZE lane");
    }
    if onerel.rd_rel.relhassubclass {
        panic!("analyze_rel (analyze.c): inheritance-tree ANALYZE lane");
    }
    let relpages =
        bufmgr_seams::relation_get_number_of_blocks_in_fork::call(&onerel, ForkNumber::MAIN_FORKNUM)?;

    do_analyze_rel(mcx, &onerel, va_cols, params, relpages, in_outer_xact)?;

    table::table_close(onerel, NO_LOCK)?;
    Ok(())
}

fn do_analyze_rel(
    _mcx: Mcx<'_>,
    onerel: &Relation<'_>,
    va_cols: &types_nodes::NodeList<'_>,
    _params: &VacuumParams,
    relpages: BlockNumber,
    in_outer_xact: bool,
) -> PgResult<()> {
    let anl = MemoryContext::new("Analyze");
    let anl_mcx = anl.mcx();

    let indexes = relcache_seams::relation_get_index_list::call(anl_mcx, onerel.rd_id)?;
    if !indexes.is_empty() {
        panic!("do_analyze_rel (analyze.c): vac_open_indexes/compute_index_stats index lane");
    }

    let tupdesc = onerel.descr();
    let mut vacattrstats: PgVec<'_, VacAttrStats<'_>> = PgVec::new_in(anl_mcx);
    if !va_cols.is_nil() {
        let mut seen: PgVec<'_, AttrNumber> = PgVec::new_in(anl_mcx);
        for c in va_cols.iter() {
            let name = c.as_string().expect("column name String").sval;
            let i = (1..=tupdesc.natts)
                .find(|&i| tupdesc.attr(i as usize - 1).attname.name_str() == name.as_bytes());
            let Some(i) = i else {
                return Err(PgError::error(format!(
                    "column \"{name}\" of relation does not exist"
                ))
                .into());
            };
            let i = i as AttrNumber;
            if seen.contains(&i) {
                return Err(PgError::error(format!(
                    "column \"{name}\" of relation appears more than once"
                ))
                .into());
            }
            seen.push(i);
            if let Some(s) = examine_attribute(anl_mcx, onerel, i as i32)? {
                vacattrstats.push(s);
            }
        }
    } else {
        for i in 1..=tupdesc.natts {
            if let Some(s) = examine_attribute(anl_mcx, onerel, i)? {
                vacattrstats.push(s);
            }
        }
    }

    let mut colstats: PgVec<'_, statistics::ColStats> = PgVec::new_in(anl_mcx);
    for s in vacattrstats.iter() {
        colstats.push(statistics::ColStats {
            tupattnum: s.tupattnum,
            attstattarget: s.attstattarget,
            attrtypid: s.attrtypid,
            attrcollid: s.attrcollid,
            typlen: s.typlen,
            typbyval: s.typbyval,
        });
    }

    let mut targrows: i32 = 100;
    for s in vacattrstats.iter() {
        targrows = targrows.max(s.minrows);
    }
    targrows = targrows.max(statistics::ComputeExtStatisticsRows(
        anl_mcx,
        onerel.rd_id,
        &colstats,
    )?);

    let mut totalrows = 0.0f64;
    let mut totaldeadrows = 0.0f64;
    let mut rows: PgVec<'_, HeapTupleData<'_>> = PgVec::new_in(anl_mcx);
    let numrows = acquire_sample_rows(
        anl_mcx,
        onerel,
        &mut rows,
        targrows,
        &mut totalrows,
        &mut totaldeadrows,
    )?;

    if numrows > 0 {
        let mut col_cx = anl.new_child("Analyze Column");
        for s in vacattrstats.iter_mut() {
            match s.compute {
                ComputeStats::Scalar => {
                    compute_scalar_stats(anl_mcx, col_cx.mcx(), s, tupdesc, &rows, numrows, totalrows)?
                }
                ComputeStats::Trivial => {
                    compute_trivial_stats(s, tupdesc, &rows, numrows)?
                }
                ComputeStats::Range { is_multirange } => {
                    range_typanalyze::compute_range_stats(
                        anl_mcx, col_cx.mcx(), s, is_multirange, tupdesc, &rows, numrows,
                        totalrows,
                    )?
                }
            }
            col_cx.reset();
        }
        update_attstats(onerel.rd_id, false, &vacattrstats)?;

        statistics::BuildRelationExtStatistics(
            anl_mcx,
            onerel,
            false,
            totalrows,
            &rows[..numrows as usize],
            &colstats,
        )?;
    }

    let (relallvisible, relallfrozen) = visibilitymap::visibilitymap_count(onerel)?;
    xact::CommandCounterIncrement()?;
    vacuum_seams::vac_update_relstats::call(
        onerel,
        relpages,
        totalrows,
        relallvisible,
        relallfrozen,
        false,
        in_outer_xact,
    )?;

    // pgstat_report_analyze: cumulative-stats lane (autovacuum feeds off it).
    Ok(())
}

fn examine_attribute<'mcx>(
    mcx: Mcx<'mcx>,
    onerel: &Relation<'_>,
    attnum: i32,
) -> PgResult<Option<VacAttrStats<'mcx>>> {
    let attr: &FormData_pg_attribute = onerel.descr().attr(attnum as usize - 1);
    if attr.attisdropped {
        return Ok(None);
    }
    if attr.attgenerated == b'v' as i8 {
        return Ok(None);
    }
    let attstattarget = syscache_seams::lookup_pg_attribute_stattarget::call(
        onerel.rd_id,
        attnum as AttrNumber,
    )?
    .map_or(-1, |t| t as i32);
    if attstattarget == 0 {
        return Ok(None);
    }
    let typanalyze = syscache_seams::pg_type_typanalyze::call(attr.atttypid)?;
    let ty = syscache_seams::lookup_pg_type_shape::call(attr.atttypid)?
        .expect("attribute type row");

    let mut stats = VacAttrStats {
        tupattnum: attnum,
        attstattarget,
        attrtypid: attr.atttypid,
        attrcollid: attr.attcollation,
        typlen: ty.typlen,
        typbyval: ty.typbyval,
        typalign: ty.typalign as u8,
        compute: ComputeStats::Trivial,
        extra: StdAnalyzeData { eqopr: InvalidOid, ltopr: InvalidOid },
        minrows: 0,
        stats_valid: false,
        stanullfrac: 0.0,
        stawidth: 0,
        stadistinct: 0.0,
        stakind: [0; STATISTIC_NUM_SLOTS],
        staop: [InvalidOid; STATISTIC_NUM_SLOTS],
        stacoll: [InvalidOid; STATISTIC_NUM_SLOTS],
        stanumbers: [
            PgVec::new_in(mcx),
            PgVec::new_in(mcx),
            PgVec::new_in(mcx),
            PgVec::new_in(mcx),
            PgVec::new_in(mcx),
        ],
        stavalues: [
            PgVec::new_in(mcx),
            PgVec::new_in(mcx),
            PgVec::new_in(mcx),
            PgVec::new_in(mcx),
            PgVec::new_in(mcx),
        ],
        stavalues_set: [false; STATISTIC_NUM_SLOTS],
        statypid: [attr.atttypid; STATISTIC_NUM_SLOTS],
        statyplen: [ty.typlen; STATISTIC_NUM_SLOTS],
        statypbyval: [ty.typbyval; STATISTIC_NUM_SLOTS],
        statypalign: [ty.typalign as u8; STATISTIC_NUM_SLOTS],
    };

    // Closed-set typanalyze dispatch (rule 4): std, range 3916, multirange
    // 4242; anything else is an unported analyze lane.
    let ok = match typanalyze {
        InvalidOid => std_typanalyze(&mut stats)?,
        3916 => {
            stats.compute = ComputeStats::Range { is_multirange: false };
            range_typanalyze::setup(&mut stats)?
        }
        4242 => {
            stats.compute = ComputeStats::Range { is_multirange: true };
            range_typanalyze::setup(&mut stats)?
        }
        other => {
            panic!("examine_attribute (analyze.c): custom typanalyze {other}; typanalyze lane")
        }
    };
    if !ok {
        return Ok(None);
    }
    Ok(Some(stats))
}

fn std_typanalyze(stats: &mut VacAttrStats<'_>) -> PgResult<bool> {
    if stats.attstattarget < 0 {
        stats.attstattarget = guc_tables::vars::default_statistics_target.read();
    }
    let entry = typcache::lookup_type_cache(
        stats.attrtypid,
        typcache::TYPECACHE_EQ_OPR | typcache::TYPECACHE_LT_OPR,
    )?;
    let eqopr = entry.eq_opr();
    let ltopr = entry.lt_opr();
    stats.extra = StdAnalyzeData { eqopr, ltopr };
    // 300*target sample floor per Chaudhuri/Motwani/Narasayya (analyze.c).
    stats.minrows = 300 * stats.attstattarget;
    if eqopr != InvalidOid && ltopr != InvalidOid {
        stats.compute = ComputeStats::Scalar;
    } else if eqopr != InvalidOid {
        panic!("std_typanalyze (analyze.c): compute_distinct_stats eq-only-type lane");
    } else {
        stats.compute = ComputeStats::Trivial;
    }
    Ok(true)
}

fn acquire_sample_rows<'mcx>(
    mcx: Mcx<'mcx>,
    onerel: &Relation<'mcx>,
    rows: &mut PgVec<'mcx, HeapTupleData<'mcx>>,
    targrows: i32,
    totalrows: &mut f64,
    totaldeadrows: &mut f64,
) -> PgResult<i32> {
    debug_assert!(targrows > 0);
    let mut numrows: i32 = 0;
    let mut samplerows = 0.0f64;
    let mut liverows = 0.0f64;
    let mut deadrows = 0.0f64;
    let mut rowstoskip = -1.0f64;

    let totalblocks =
        bufmgr_seams::relation_get_number_of_blocks_in_fork::call(onerel, ForkNumber::MAIN_FORKNUM)?;
    let oldest_xmin = procarray::GetOldestNonRemovableTransactionId(onerel)?;

    let randseed = pg_prng::global_prng(|p| p.next_u32());
    let (mut bs, _nblocks) = sampling::block_sampler_init(totalblocks, targrows as u32, randseed);
    let mut rstate =
        sampling::reservoir_init_selection_state(pg_prng::global_prng(|p| p.next_u64()), targrows as u32);

    let mut scan = tableam::table_beginscan_analyze(mcx, onerel)?;
    let mut slot = tableam::table_slot_create(mcx, onerel)?;

    let mut next_buffer = |bs: &mut sampling::BlockSamplerData| -> PgResult<types_core::Buffer> {
        if !bs.has_more() {
            return Ok(types_core::InvalidBuffer);
        }
        bufmgr_seams::read_buffer::call(onerel, bs.next())
    };

    loop {
        let buf = next_buffer(&mut bs)?;
        if !tableam::table_scan_analyze_next_block(mcx, &mut scan, &mut || Ok(buf))? {
            break;
        }
        while tableam::table_scan_analyze_next_tuple(
            mcx,
            &mut scan,
            oldest_xmin,
            &mut liverows,
            &mut deadrows,
            &mut slot,
        )? {
            if numrows < targrows {
                rows.push(copy_slot_tuple(mcx, &slot)?);
                numrows += 1;
            } else {
                if rowstoskip < 0.0 {
                    rowstoskip = sampling::reservoir_get_next_s(&mut rstate, samplerows, targrows as u32);
                }
                if rowstoskip <= 0.0 {
                    let k = (targrows as f64 * sampling::sampler_random_fract(&mut rstate.randstate))
                        as usize;
                    debug_assert!(k < targrows as usize);
                    // C heap_freetuple's the replaced copy; here it stays in
                    // the Analyze arena until context teardown.
                    rows[k] = copy_slot_tuple(mcx, &slot)?;
                }
                rowstoskip -= 1.0;
            }
            samplerows += 1.0;
        }
    }
    drop(slot);
    tableam::table_endscan(scan)?;

    if numrows == targrows {
        rows.sort_unstable_by_key(|t| {
            (
                types_tuple::ItemPointerGetBlockNumberNoCheck(&t.t_self),
                types_tuple::ItemPointerGetOffsetNumberNoCheck(&t.t_self),
            )
        });
    }

    if bs.m > 0 {
        *totalrows = ((liverows / bs.m as f64) * totalblocks as f64 + 0.5).floor();
        *totaldeadrows = ((deadrows / bs.m as f64) * totalblocks as f64 + 0.5).floor();
    } else {
        *totalrows = 0.0;
        *totaldeadrows = 0.0;
    }

    Ok(numrows)
}

fn copy_slot_tuple<'mcx>(mcx: Mcx<'mcx>, slot: &SlotData<'mcx>) -> PgResult<HeapTupleData<'mcx>> {
    let SlotData::BufferHeap(s) = slot else {
        panic!("acquire_sample_rows: non-buffer slot from analyze scan")
    };
    let src = s.base.tuple.as_ref().expect("stored sample tuple");
    let owned = heaptuple::heap_copytuple(mcx, src)?;
    let (ptr, len, tid, oid) =
        (owned.image().as_ptr(), owned.as_tuple().t_len, owned.as_tuple().t_self, owned.as_tuple().t_tableOid);
    core::mem::forget(owned);
    // SAFETY: the image was just copied into `mcx` and, forgotten, lives until
    // that context's teardown; nothing else writes it.
    Ok(unsafe { HeapTupleData::from_raw_parts(ptr, len, tid, oid) })
}

pub(crate) fn fetch_attr(
    row: &HeapTupleData<'_>,
    attnum: i32,
    tupdesc: &TupleDescData<'_>,
) -> (Datum, bool) {
    let mut isnull = false;
    // SAFETY: sampled rows are this relation's tuples under its descriptor.
    let d = unsafe { types_tuple::heap_getattr(row, attnum, tupdesc, &mut isnull) };
    (d, isnull)
}

pub(crate) fn varlena_stored_size(d: Datum) -> usize {
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null varlena datum.
    let b0 = unsafe { *p };
    if b0 == 0x01 || (b0 & 0x03) == 0x02 {
        panic!("compute stats (analyze.c): toasted/compressed varlena in sample; detoast lane");
    }
    if b0 & 0x01 != 0 {
        (b0 as usize >> 1) & 0x7F
    } else {
        // SAFETY: 4-byte varlena header.
        let w = unsafe { u32::from_ne_bytes(*(p as *const [u8; 4])) };
        (w as usize) >> 2
    }
}

fn datum_copy_in<'mcx>(mcx: Mcx<'mcx>, d: Datum, typbyval: bool, typlen: i16) -> PgResult<Datum> {
    if typbyval {
        return Ok(d);
    }
    let len = if typlen > 0 { typlen as usize } else { varlena_stored_size(d) };
    let p = d.as_usize() as *const u8;
    // SAFETY: byref datum addresses `len` live bytes.
    let src = unsafe { core::slice::from_raw_parts(p, len) };
    let copy = mcx::slice_borrow_in(mcx, src)?;
    Ok(Datum::from_usize(copy.as_ptr() as usize))
}

fn compute_trivial_stats(
    stats: &mut VacAttrStats<'_>,
    tupdesc: &TupleDescData<'_>,
    rows: &[HeapTupleData<'_>],
    samplerows: i32,
) -> PgResult<()> {
    let is_varlena = !stats.typbyval && stats.typlen == -1;
    let is_varwidth = !stats.typbyval && stats.typlen < 0;
    let mut null_cnt = 0i32;
    let mut nonnull_cnt = 0i32;
    let mut total_width = 0.0f64;
    for row in &rows[..samplerows as usize] {
        let (value, isnull) = fetch_attr(row, stats.tupattnum, tupdesc);
        if isnull {
            null_cnt += 1;
            continue;
        }
        nonnull_cnt += 1;
        if is_varlena {
            total_width += varlena_stored_size(value) as f64;
        } else if is_varwidth {
            panic!("compute_trivial_stats (analyze.c): cstring-width type lane");
        }
    }
    if nonnull_cnt > 0 {
        stats.stats_valid = true;
        stats.stanullfrac = null_cnt as f32 / samplerows as f32;
        stats.stawidth = if is_varwidth {
            (total_width / nonnull_cnt as f64) as i32
        } else {
            stats.typlen as i32
        };
        stats.stadistinct = 0.0;
    } else if null_cnt > 0 {
        stats.stats_valid = true;
        stats.stanullfrac = 1.0;
        stats.stawidth = if is_varwidth { 0 } else { stats.typlen as i32 };
        stats.stadistinct = 0.0;
    }
    Ok(())
}

struct ScalarMCVItem {
    first: i32,
    count: i32,
}

fn compute_scalar_stats<'mcx>(
    anl_mcx: Mcx<'mcx>,
    col_mcx: Mcx<'_>,
    stats: &mut VacAttrStats<'mcx>,
    tupdesc: &TupleDescData<'_>,
    rows: &[HeapTupleData<'_>],
    samplerows: i32,
    totalrows: f64,
) -> PgResult<()> {
    let is_varlena = !stats.typbyval && stats.typlen == -1;
    let is_varwidth = !stats.typbyval && stats.typlen < 0;
    let mut null_cnt = 0i32;
    let mut nonnull_cnt = 0i32;
    let mut toowide_cnt = 0i32;
    let mut total_width = 0.0f64;
    let num_mcv0 = stats.attstattarget;
    let num_bins = stats.attstattarget;

    let mut values: PgVec<'_, (Datum, i32)> = mcx::vec_with_capacity_in(col_mcx, samplerows as usize)?;
    for row in &rows[..samplerows as usize] {
        let (value, isnull) = fetch_attr(row, stats.tupattnum, tupdesc);
        if isnull {
            null_cnt += 1;
            continue;
        }
        nonnull_cnt += 1;
        if is_varlena {
            let sz = varlena_stored_size(value);
            total_width += sz as f64;
            if sz > WIDTH_THRESHOLD {
                toowide_cnt += 1;
                continue;
            }
        } else if is_varwidth {
            panic!("compute_scalar_stats (analyze.c): cstring-width type lane");
        }
        let tupno = values.len() as i32;
        values.push((value, tupno));
    }
    let values_cnt = values.len() as i32;

    if values_cnt > 0 {
        let entry = typcache::lookup_type_cache(stats.attrtypid, typcache::TYPECACHE_CMP_PROC_FINFO)?;
        let collation = stats.attrcollid;
        let cmp = |a: Datum, b: Datum| -> core::cmp::Ordering {
            let mut finfo = entry.cmp_proc_finfo();
            let r = types_fmgr::function_call2_coll(&mut finfo, collation, a, b)
                .unwrap_or_else(|e| panic!("compute_scalar_stats: comparison failed: {e:?}"))
                .as_i32();
            r.cmp(&0)
        };
        // C's compare_scalars piggybacks dup detection on the sort via
        // tupnoLink; here an explicit adjacent-equality pass replaces it
        // (identical output, N-1 extra comparisons, cold path).
        values.sort_unstable_by(|a, b| cmp(a.0, b.0).then(a.1.cmp(&b.1)));

        let mut corr_xysum = 0.0f64;
        let mut ndistinct = 0i32;
        let mut nmultiple = 0i32;
        let mut dups_cnt = 0i32;
        let mut track: PgVec<'_, ScalarMCVItem> = mcx::vec_with_capacity_in(col_mcx, num_mcv0 as usize)?;
        let mut num_mcv = num_mcv0;
        for i in 0..values_cnt {
            corr_xysum += i as f64 * values[i as usize].1 as f64;
            dups_cnt += 1;
            let group_end = i == values_cnt - 1
                || cmp(values[i as usize].0, values[i as usize + 1].0) != core::cmp::Ordering::Equal;
            if group_end {
                ndistinct += 1;
                if dups_cnt > 1 {
                    nmultiple += 1;
                    if (track.len() as i32) < num_mcv
                        || dups_cnt > track[track.len() - 1].count
                    {
                        if (track.len() as i32) < num_mcv {
                            track.push(ScalarMCVItem { first: 0, count: 0 });
                        }
                        let mut j = track.len() - 1;
                        while j > 0 {
                            if dups_cnt <= track[j - 1].count {
                                break;
                            }
                            track[j] = ScalarMCVItem { first: track[j - 1].first, count: track[j - 1].count };
                            j -= 1;
                        }
                        track[j] = ScalarMCVItem { first: i + 1 - dups_cnt, count: dups_cnt };
                    }
                }
                dups_cnt = 0;
            }
        }
        let track_cnt_all = track.len() as i32;

        stats.stats_valid = true;
        stats.stanullfrac = null_cnt as f32 / samplerows as f32;
        stats.stawidth = if is_varwidth {
            (total_width / nonnull_cnt as f64) as i32
        } else {
            stats.typlen as i32
        };

        if nmultiple == 0 {
            stats.stadistinct = -1.0 * (1.0 - stats.stanullfrac);
        } else if toowide_cnt == 0 && nmultiple == ndistinct {
            stats.stadistinct = ndistinct as f32;
        } else {
            // Haas-Stokes Duj1: n*d / (n - f1 + f1*n/N).
            let f1 = (ndistinct - nmultiple + toowide_cnt) as f64;
            let d = f1 + nmultiple as f64;
            let n = (samplerows - null_cnt) as f64;
            let n_total = totalrows * (1.0 - stats.stanullfrac as f64);
            let mut stadistinct = if n_total > 0.0 {
                (n * d) / ((n - f1) + f1 * n / n_total)
            } else {
                0.0
            };
            if stadistinct < d {
                stadistinct = d;
            }
            if stadistinct > n_total {
                stadistinct = n_total;
            }
            stats.stadistinct = (stadistinct + 0.5).floor() as f32;
        }
        if stats.stadistinct as f64 > 0.1 * totalrows {
            stats.stadistinct = -(stats.stadistinct as f64 / totalrows) as f32;
        }

        let mut track_cnt = track_cnt_all;
        if track_cnt == ndistinct
            && toowide_cnt == 0
            && stats.stadistinct > 0.0
            && track_cnt <= num_mcv
        {
            num_mcv = track_cnt;
        } else {
            if num_mcv > track_cnt {
                num_mcv = track_cnt;
            }
            if num_mcv > 0 {
                let mut mcv_counts: PgVec<'_, i32> = mcx::vec_with_capacity_in(col_mcx, num_mcv as usize)?;
                for item in track.iter().take(num_mcv as usize) {
                    mcv_counts.push(item.count);
                }
                num_mcv = analyze_mcv_list(
                    &mcv_counts,
                    num_mcv,
                    stats.stadistinct as f64,
                    stats.stanullfrac as f64,
                    samplerows,
                    totalrows,
                );
            }
        }
        track_cnt = num_mcv;
        let _ = track_cnt;

        let mut slot_idx = 0usize;
        if num_mcv > 0 {
            let mut mcv_values: PgVec<'mcx, Datum> = mcx::vec_with_capacity_in(anl_mcx, num_mcv as usize)?;
            let mut mcv_freqs: PgVec<'mcx, f32> = mcx::vec_with_capacity_in(anl_mcx, num_mcv as usize)?;
            for item in track.iter().take(num_mcv as usize) {
                mcv_values.push(datum_copy_in(
                    anl_mcx,
                    values[item.first as usize].0,
                    stats.typbyval,
                    stats.typlen,
                )?);
                mcv_freqs.push(item.count as f32 / samplerows as f32);
            }
            stats.stakind[slot_idx] = STATISTIC_KIND_MCV;
            stats.staop[slot_idx] = stats.extra.eqopr;
            stats.stacoll[slot_idx] = stats.attrcollid;
            stats.stanumbers[slot_idx] = mcv_freqs;
            stats.stavalues[slot_idx] = mcv_values;
            stats.stavalues_set[slot_idx] = true;
            slot_idx += 1;
        }

        let mut num_hist = ndistinct - num_mcv;
        if num_hist > num_bins {
            num_hist = num_bins + 1;
        }
        if num_hist >= 2 {
            track.truncate(num_mcv as usize);
            track.sort_unstable_by_key(|it| it.first);

            let nvals = if num_mcv > 0 {
                let mut src = 0usize;
                let mut dest = 0usize;
                let mut j = 0usize;
                let values_cnt = values_cnt as usize;
                while src < values_cnt {
                    let ncopy = if j < num_mcv as usize {
                        let first = track[j].first as usize;
                        if src >= first {
                            src = first + track[j].count as usize;
                            j += 1;
                            continue;
                        }
                        first - src
                    } else {
                        values_cnt - src
                    };
                    values.copy_within(src..src + ncopy, dest);
                    src += ncopy;
                    dest += ncopy;
                }
                dest as i32
            } else {
                values_cnt
            };
            debug_assert!(nvals >= num_hist);

            let mut hist_values: PgVec<'mcx, Datum> =
                mcx::vec_with_capacity_in(anl_mcx, num_hist as usize)?;
            let delta = (nvals - 1) / (num_hist - 1);
            let deltafrac = (nvals - 1) % (num_hist - 1);
            let mut pos = 0i32;
            let mut posfrac = 0i32;
            for _ in 0..num_hist {
                hist_values.push(datum_copy_in(
                    anl_mcx,
                    values[pos as usize].0,
                    stats.typbyval,
                    stats.typlen,
                )?);
                pos += delta;
                posfrac += deltafrac;
                if posfrac >= num_hist - 1 {
                    pos += 1;
                    posfrac -= num_hist - 1;
                }
            }
            stats.stakind[slot_idx] = STATISTIC_KIND_HISTOGRAM;
            stats.staop[slot_idx] = stats.extra.ltopr;
            stats.stacoll[slot_idx] = stats.attrcollid;
            stats.stavalues[slot_idx] = hist_values;
            stats.stavalues_set[slot_idx] = true;
            slot_idx += 1;
        }

        if values_cnt > 1 {
            let vc = values_cnt as f64;
            let corr_xsum = (vc - 1.0) * vc / 2.0;
            let corr_x2sum = (vc - 1.0) * vc * (2.0 * vc - 1.0) / 6.0;
            let corr = (vc * corr_xysum - corr_xsum * corr_xsum)
                / (vc * corr_x2sum - corr_xsum * corr_xsum);
            let mut corrs: PgVec<'mcx, f32> = mcx::vec_with_capacity_in(anl_mcx, 1)?;
            corrs.push(corr as f32);
            stats.stakind[slot_idx] = STATISTIC_KIND_CORRELATION;
            stats.staop[slot_idx] = stats.extra.ltopr;
            stats.stacoll[slot_idx] = stats.attrcollid;
            stats.stanumbers[slot_idx] = corrs;
        }
    } else if nonnull_cnt > 0 {
        debug_assert!(nonnull_cnt == toowide_cnt);
        stats.stats_valid = true;
        stats.stanullfrac = null_cnt as f32 / samplerows as f32;
        stats.stawidth = if is_varwidth {
            (total_width / nonnull_cnt as f64) as i32
        } else {
            stats.typlen as i32
        };
        stats.stadistinct = -1.0 * (1.0 - stats.stanullfrac);
    } else if null_cnt > 0 {
        stats.stats_valid = true;
        stats.stanullfrac = 1.0;
        stats.stawidth = if is_varwidth { 0 } else { stats.typlen as i32 };
        stats.stadistinct = 0.0;
    }
    Ok(())
}

fn analyze_mcv_list(
    mcv_counts: &[i32],
    mut num_mcv: i32,
    stadistinct: f64,
    stanullfrac: f64,
    samplerows: i32,
    totalrows: f64,
) -> i32 {
    if samplerows as f64 == totalrows || totalrows <= 1.0 {
        return num_mcv;
    }
    let mut ndistinct_table = stadistinct;
    if ndistinct_table < 0.0 {
        ndistinct_table = -ndistinct_table * totalrows;
    }
    let mut sumcount = 0.0f64;
    for &c in &mcv_counts[..num_mcv as usize - 1] {
        sumcount += c as f64;
    }
    while num_mcv > 0 {
        let mut selec = 1.0 - sumcount / samplerows as f64 - stanullfrac;
        selec = selec.clamp(0.0, 1.0);
        let otherdistinct = ndistinct_table - (num_mcv - 1) as f64;
        if otherdistinct > 1.0 {
            selec /= otherdistinct;
        }
        // Hypergeometric continuity-corrected Wald bound (analyze.c).
        let n_total = totalrows;
        let n = samplerows as f64;
        let k = n_total * mcv_counts[num_mcv as usize - 1] as f64 / n;
        let variance = n * k * (n_total - k) * (n_total - n) / (n_total * n_total * (n_total - 1.0));
        let stddev = variance.sqrt();
        if mcv_counts[num_mcv as usize - 1] as f64 > selec * n + 2.0 * stddev + 0.5 {
            break;
        }
        num_mcv -= 1;
        if num_mcv == 0 {
            break;
        }
        sumcount -= mcv_counts[num_mcv as usize - 1] as f64;
    }
    num_mcv
}

fn update_attstats(relid: Oid, inh: bool, vacattrstats: &[VacAttrStats<'_>]) -> PgResult<()> {
    if vacattrstats.is_empty() {
        return Ok(());
    }
    let scratch = MemoryContext::new("update_attstats");
    let mcx = scratch.mcx();
    let sd = table::table_open(mcx, STATISTIC_RELATION_ID, ROW_EXCLUSIVE_LOCK)?;
    let mut indstate: Option<catalog_indexing::CatalogIndexState<'_>> = None;

    for stats in vacattrstats {
        if !stats.stats_valid {
            continue;
        }
        let mut values = [Datum::null(); NATTS_PG_STATISTIC];
        let mut nulls = [false; NATTS_PG_STATISTIC];
        values[ANUM_PG_STATISTIC_STARELID - 1] = Datum::from_oid(relid);
        values[ANUM_PG_STATISTIC_STAATTNUM - 1] = Datum::from_i16(stats.tupattnum as i16);
        values[ANUM_PG_STATISTIC_STAINHERIT - 1] = Datum::from_bool(inh);
        values[ANUM_PG_STATISTIC_STANULLFRAC - 1] = Datum::from_f32(stats.stanullfrac);
        values[ANUM_PG_STATISTIC_STAWIDTH - 1] = Datum::from_i32(stats.stawidth);
        values[ANUM_PG_STATISTIC_STADISTINCT - 1] = Datum::from_f32(stats.stadistinct);
        for k in 0..STATISTIC_NUM_SLOTS {
            values[ANUM_PG_STATISTIC_STAKIND1 - 1 + k] = Datum::from_i16(stats.stakind[k]);
            values[ANUM_PG_STATISTIC_STAOP1 - 1 + k] = Datum::from_oid(stats.staop[k]);
            values[ANUM_PG_STATISTIC_STACOLL1 - 1 + k] = Datum::from_oid(stats.stacoll[k]);
        }
        let mut images: PgVec<'_, PgVec<'_, u8>> = PgVec::new_in(mcx);
        for k in 0..STATISTIC_NUM_SLOTS {
            let i = ANUM_PG_STATISTIC_STANUMBERS1 - 1 + k;
            if !stats.stanumbers[k].is_empty() {
                let mut dat: PgVec<'_, Datum> =
                    mcx::vec_with_capacity_in(mcx, stats.stanumbers[k].len())?;
                dat.extend(stats.stanumbers[k].iter().map(|&f| Datum::from_f32(f)));
                let img = datum::array_build::construct_array_image(mcx, &dat, FLOAT4OID, 4, true, b'i')?;
                values[i] = Datum::from_usize(img.as_ptr() as usize);
                images.push(img);
            } else {
                nulls[i] = true;
            }
        }
        for k in 0..STATISTIC_NUM_SLOTS {
            let i = ANUM_PG_STATISTIC_STAVALUES1 - 1 + k;
            if !stats.stavalues[k].is_empty() {
                let img = datum::array_build::construct_array_image(
                    mcx,
                    &stats.stavalues[k],
                    stats.statypid[k],
                    stats.statyplen[k],
                    stats.statypbyval[k],
                    stats.statypalign[k],
                )?;
                values[i] = Datum::from_usize(img.as_ptr() as usize);
                images.push(img);
            } else if stats.stavalues_set[k] {
                let img =
                    datum::array_build::construct_empty_array_image(mcx, stats.statypid[k])?;
                values[i] = Datum::from_usize(img.as_ptr() as usize);
                images.push(img);
            } else {
                nulls[i] = true;
            }
        }

        let old = find_stats_tuple(mcx, &sd, relid, stats.tupattnum as i16, inh)?;
        if indstate.is_none() {
            indstate = Some(catalog_indexing::CatalogOpenIndexes(mcx, &sd)?);
        }
        let ind = indstate.as_mut().expect("opened above");
        match old {
            Some((otid, oldtup)) => {
                let replaces = [true; NATTS_PG_STATISTIC];
                let mut newtup = heaptuple::heap_modify_tuple(
                    mcx,
                    &oldtup,
                    sd.descr(),
                    &values,
                    &nulls,
                    &replaces,
                )?;
                catalog_indexing::CatalogTupleUpdateWithInfo(mcx, &sd, &otid, &mut newtup, ind)?;
            }
            None => {
                let mut stup = heaptuple::heap_form_tuple(mcx, sd.descr(), &values, &nulls)?;
                catalog_indexing::CatalogTupleInsertWithInfo(mcx, &sd, &mut stup, ind)?;
            }
        }
    }
    if let Some(ind) = indstate {
        catalog_indexing::CatalogCloseIndexes(ind)?;
    }
    table::table_close(sd, ROW_EXCLUSIVE_LOCK)?;
    Ok(())
}

type FoundStats<'a> = (types_tuple::ItemPointerData, heaptuple::HeapTuple<'a>);

fn find_stats_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    sd: &Relation<'mcx>,
    relid: Oid,
    attnum: i16,
    inh: bool,
) -> PgResult<Option<FoundStats<'mcx>>> {
    let keys = [
        stat_key(ANUM_PG_STATISTIC_STARELID as i32, F_OIDEQ, Datum::from_oid(relid)),
        stat_key(ANUM_PG_STATISTIC_STAATTNUM as i32, F_INT2EQ, Datum::from_i16(attnum)),
        stat_key(ANUM_PG_STATISTIC_STAINHERIT as i32, F_BOOLEQ, Datum::from_bool(inh)),
    ];
    let mut scan = genam::systable_beginscan(mcx, sd, InvalidOid, false, None, &keys)?;
    let found = match genam::systable_getnext(mcx, &mut scan)? {
        Some(tup) => Some((tup.t_self, heaptuple::heap_copytuple(mcx, tup)?)),
        None => None,
    };
    genam::systable_endscan(mcx, scan)?;
    Ok(found)
}

// defGetBoolean (define.c).
fn def_get_boolean(def: &types_nodes::parsenodes::DefElem<'_>) -> PgResult<bool> {
    use types_nodes::NodeTag;
    let Some(arg) = def.arg else {
        return Ok(true);
    };
    if arg.node_tag() == NodeTag::T_Integer {
        match arg.as_integer().unwrap().ival {
            0 => return Ok(false),
            1 => return Ok(true),
            _ => {}
        }
    } else {
        let sval = match arg.node_tag() {
            NodeTag::T_Float => arg.as_float().unwrap().fval,
            NodeTag::T_Boolean => {
                if arg.as_boolean().unwrap().boolval { "true" } else { "false" }
            }
            NodeTag::T_String => arg.as_string().unwrap().sval,
            t => panic!("defGetBoolean (define.c): {t:?} arg unported (define lane)"),
        };
        if sval.eq_ignore_ascii_case("true") || sval.eq_ignore_ascii_case("on") {
            return Ok(true);
        }
        if sval.eq_ignore_ascii_case("false") || sval.eq_ignore_ascii_case("off") {
            return Ok(false);
        }
    }
    Err(PgError::error(format!(
        "{} requires a Boolean value",
        def.defname.unwrap_or("")
    ))
    .into())
}

fn stat_key(attno: i32, func: types_core::primitive::RegProcedure, arg: Datum) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno as AttrNumber;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(func)
        .unwrap_or_else(|e| panic!("fmgr_info({func}) failed: {e:?}"));
    key.sk_argument = arg;
    key
}

#[cfg(test)]
mod tests {
    use super::analyze_mcv_list;

    #[test]
    fn mcv_list_kept_whole_when_sample_is_table() {
        let counts = [5000, 3000, 2000];
        assert_eq!(analyze_mcv_list(&counts, 3, 3.0, 0.0, 10000, 10000.0), 3);
    }

    #[test]
    fn mcv_list_drops_insignificant_tail() {
        // 100 distinct, uniform-ish tail: a value seen twice in 30000 of 1e6
        // rows is not significantly above the non-MCV estimate.
        let counts = [900, 2];
        assert_eq!(analyze_mcv_list(&counts, 2, -0.0001 * 1_000_000.0, 0.0, 30000, 1_000_000.0), 1);
    }

    #[test]
    fn mcv_list_keeps_significant_values() {
        let counts = [15000, 9000, 6000];
        assert_eq!(analyze_mcv_list(&counts, 3, 103.0, 0.0, 30000, 1_000_000.0), 3);
    }
}

static DEFAULT_STATISTICS_TARGET: std::sync::atomic::AtomicI32 =
    std::sync::atomic::AtomicI32::new(100);

pub fn init_seams() {
    use std::sync::atomic::Ordering::Relaxed;
    guc_tables::vars::default_statistics_target.install(guc_tables::GucVarAccessors {
        get: || DEFAULT_STATISTICS_TARGET.load(Relaxed),
        set: |v| DEFAULT_STATISTICS_TARGET.store(v, Relaxed),
    });
}
