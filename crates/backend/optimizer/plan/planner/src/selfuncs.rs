//! selfuncs.c slice: eqsel/scalarineqsel over Var-op-Const with pg_statistic
//! consumption (MCV + histogram), plus btcostestimate/genericcostestimate.

use datum::Datum;
use syscache_seams::{PgStatisticBundle, PgStatisticSlotData};
use types_core::Oid;
use types_error::PgResult;
use types_fmgr::FmgrInfo;
use types_nodes::parsenodes::RTEKind;
use types_nodes::{BoolTestType, Node, NodeTag};
use types_pathnodes::{JoinType, NodeId, PathNode, RelId, RinfoId, SpecialJoinInfo, JOIN_INNER};

use crate::gucs;
use crate::run::PlannerRun;

pub const DEFAULT_EQ_SEL: f64 = 0.005;
pub const DEFAULT_INEQ_SEL: f64 = 0.3333333333333333;
pub const DEFAULT_MATCH_SEL: f64 = 0.005;
pub const DEFAULT_NUM_DISTINCT: f64 = 200.0;
const DEFAULT_PAGE_CPU_MULTIPLIER: f64 = 50.0;
const BOOLOID: u32 = 16;
const SELF_ITEM_POINTER_ATTRIBUTE_NUMBER: i16 = -1;
const TABLE_OID_ATTRIBUTE_NUMBER: i16 = -6;

pub const STATISTIC_KIND_MCV: i16 = 1;
pub const STATISTIC_KIND_HISTOGRAM: i16 = 2;
pub const STATISTIC_KIND_CORRELATION: i16 = 3;

pub(crate) fn clamp_probability(p: f64) -> f64 {
    p.clamp(0.0, 1.0)
}

// VariableStatData (selfuncs.h); `stats` is the decoded statsTuple.
// statistic_proc_security_check (pg_class_aclcheck) reduces to true on this
// single-role substrate, so acl_ok is not modeled.
pub struct VariableStatData<'mcx> {
    pub var: Option<NodeId>,
    pub rel: Option<RelId>,
    pub vartype: u32,
    pub isunique: bool,
    pub stats: Option<PgStatisticBundle<'mcx>>,
}

impl<'mcx> VariableStatData<'mcx> {
    pub(crate) fn nullfrac(&self) -> f64 {
        self.stats.as_ref().map_or(0.0, |s| s.stanullfrac as f64)
    }

    pub(crate) fn slot(&self, kind: i16, reqop: Oid) -> Option<&PgStatisticSlotData<'mcx>> {
        self.stats.as_ref().and_then(|s| {
            s.slots
                .iter()
                .find(|sl| sl.kind == kind && (reqop == 0 || sl.staop == reqop))
        })
    }
}

pub(crate) fn opproc_for(operator: Oid) -> PgResult<FmgrInfo> {
    let opcode = lsyscache::get_opcode(operator)?;
    fmgr_core::fmgr_info(opcode)
}

pub(crate) fn op_test(
    opproc: &mut FmgrInfo,
    collation: Oid,
    slot_value: Datum,
    constval: Datum,
    varonleft: bool,
) -> PgResult<bool> {
    let (a0, a1) = if varonleft { (slot_value, constval) } else { (constval, slot_value) };
    Ok(types_fmgr::function_call2_coll(opproc, collation, a0, a1)?.as_bool())
}

const DEFAULT_UNK_SEL: f64 = 0.005;
const DEFAULT_NOT_UNK_SEL: f64 = 1.0 - DEFAULT_UNK_SEL;

// nulltestsel (selfuncs.c); C's jointype/sjinfo params are unused there too.
pub fn nulltestsel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    is_null: bool,
    arg: Node<'mcx>,
    varrelid: i32,
) -> PgResult<f64> {
    let node_id = run.intern_expr(arg);
    let vardata = examine_variable(run, node_id, arg, varrelid)?;
    let selec = if let Some(stats) = &vardata.stats {
        let freq_null = stats.stanullfrac as f64;
        if is_null {
            freq_null
        } else {
            1.0 - freq_null
        }
    } else if matches!(arg.as_var(), Some(v) if v.varattno < 0) {
        // System attributes are never NULL (C's varattno < 0 arm).
        if is_null {
            0.0
        } else {
            1.0
        }
    } else if is_null {
        DEFAULT_UNK_SEL
    } else {
        DEFAULT_NOT_UNK_SEL
    };
    Ok(clamp_probability(selec))
}

// boolvarsel (selfuncs.c): a boolean Var is the clause V = 't'.
pub fn boolvarsel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    arg: Node<'mcx>,
    varrelid: i32,
) -> PgResult<f64> {
    let node_id = run.intern_expr(arg);
    let vardata = examine_variable(run, node_id, arg, varrelid)?;
    if vardata.stats.is_some() {
        const BOOLEAN_EQUAL_OPERATOR: Oid = 91;
        var_eq_const(
            run,
            &vardata,
            BOOLEAN_EQUAL_OPERATOR,
            0,
            Datum::from_bool(true),
            false,
            true,
            false,
        )
    } else {
        Ok(0.5)
    }
}

// booltestsel (selfuncs.c).
pub fn booltestsel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    booltesttype: BoolTestType,
    arg: Node<'mcx>,
    varrelid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo<'mcx>>,
) -> PgResult<f64> {
    let node_id = run.intern_expr(arg);
    let vardata = examine_variable(run, node_id, arg, varrelid)?;
    let selec = if let Some(stats) = &vardata.stats {
        let freq_null = stats.stanullfrac as f64;
        let mcv = vardata.slot(STATISTIC_KIND_MCV, 0).and_then(|sslot| {
            let values = sslot.values().ok()?;
            let numbers = sslot.numbers().ok()?;
            let first_num = *numbers.first()? as f64;
            Some((values.first()?.as_bool(), first_num))
        });
        if let Some((first_is_true, first_num)) = mcv {
            let freq_true =
                if first_is_true { first_num } else { 1.0 - first_num - freq_null };
            let freq_false = 1.0 - freq_true - freq_null;
            match booltesttype {
                BoolTestType::IS_UNKNOWN => freq_null,
                BoolTestType::IS_NOT_UNKNOWN => 1.0 - freq_null,
                BoolTestType::IS_TRUE => freq_true,
                BoolTestType::IS_NOT_TRUE => 1.0 - freq_true,
                BoolTestType::IS_FALSE => freq_false,
                BoolTestType::IS_NOT_FALSE => 1.0 - freq_false,
            }
        } else {
            match booltesttype {
                BoolTestType::IS_UNKNOWN => freq_null,
                BoolTestType::IS_NOT_UNKNOWN => 1.0 - freq_null,
                BoolTestType::IS_TRUE | BoolTestType::IS_FALSE => (1.0 - freq_null) / 2.0,
                BoolTestType::IS_NOT_TRUE | BoolTestType::IS_NOT_FALSE => {
                    (freq_null + 1.0) / 2.0
                }
            }
        }
    } else {
        match booltesttype {
            BoolTestType::IS_UNKNOWN => DEFAULT_UNK_SEL,
            BoolTestType::IS_NOT_UNKNOWN => DEFAULT_NOT_UNK_SEL,
            BoolTestType::IS_TRUE | BoolTestType::IS_NOT_FALSE => {
                crate::clausesel::clause_selectivity_node(run, arg, varrelid, jointype, sjinfo)?
            }
            BoolTestType::IS_FALSE | BoolTestType::IS_NOT_TRUE => {
                1.0 - crate::clausesel::clause_selectivity_node(
                    run, arg, varrelid, jointype, sjinfo,
                )?
            }
        }
    };
    Ok(clamp_probability(selec))
}

// scalarltsel/scalarlesel/scalargtsel/scalargesel via scalarineqsel_wrapper
// (selfuncs.c).
pub fn scalarineqsel_wrapper<'mcx>(
    run: &mut PlannerRun<'mcx>,
    operator: Oid,
    args: &[NodeId],
    varrelid: i32,
    collation: Oid,
    isgt: bool,
    iseq: bool,
) -> PgResult<f64> {
    let mut operator = operator;
    let mut isgt = isgt;
    let Some((vardata, other, varonleft)) = get_restriction_variable(run, args, varrelid)?
    else {
        return Ok(DEFAULT_INEQ_SEL);
    };
    let Some(c) = other.as_const() else {
        return Ok(DEFAULT_INEQ_SEL);
    };
    if c.constisnull {
        return Ok(0.0);
    }
    if !varonleft {
        operator = lsyscache::get_commutator(operator)?;
        if operator == 0 {
            return Ok(DEFAULT_INEQ_SEL);
        }
        isgt = !isgt;
    }
    scalarineqsel(
        run,
        operator,
        isgt,
        iseq,
        collation,
        &vardata,
        c.constvalue,
        c.consttype,
    )
}

// scalarineqsel (selfuncs.c). The C no-stats CTID arm (block-position
// estimate) keeps this port's pre-existing DEFAULT_INEQ_SEL shape.
fn scalarineqsel<'mcx>(
    run: &PlannerRun<'mcx>,
    operator: Oid,
    isgt: bool,
    iseq: bool,
    collation: Oid,
    vardata: &VariableStatData<'mcx>,
    constval: Datum,
    consttype: Oid,
) -> PgResult<f64> {
    if vardata.stats.is_none() {
        return Ok(DEFAULT_INEQ_SEL);
    }
    let stanullfrac = vardata.nullfrac();
    let mut opproc = opproc_for(operator)?;

    let (mcv_selec, sumcommon) =
        mcv_selectivity(run, vardata, &mut opproc, collation, constval, true)?;
    let hist_selec = ineq_histogram_selectivity(
        run, vardata, operator, &mut opproc, isgt, iseq, collation, constval, consttype,
    )?;

    let mut selec = 1.0 - stanullfrac - sumcommon;
    if hist_selec >= 0.0 {
        selec *= hist_selec;
    } else {
        selec *= 0.5;
    }
    selec += mcv_selec;
    Ok(clamp_probability(selec))
}

// mcv_selectivity (selfuncs.c); returns (mcv_selec, sumcommon).
pub(crate) fn mcv_selectivity<'mcx>(
    run: &PlannerRun<'mcx>,
    vardata: &VariableStatData<'mcx>,
    opproc: &mut FmgrInfo,
    collation: Oid,
    constval: Datum,
    varonleft: bool,
) -> PgResult<(f64, f64)> {
    let mut mcv_selec = 0.0;
    let mut sumcommon = 0.0;
    if let Some(sslot) = vardata.slot(STATISTIC_KIND_MCV, 0) {
        for (i, &v) in sslot.values()?.iter().enumerate() {
            if op_test(opproc, collation, v, constval, varonleft)? {
                mcv_selec += sslot.numbers()?[i] as f64;
            }
            sumcommon += sslot.numbers()?[i] as f64;
        }
    }
    Ok((mcv_selec, sumcommon))
}

// get_actual_variable_range (selfuncs.c). Returns (have_data, min, max);
// an endpoint is Some only when its probe succeeded (C writes through the
// out-pointer exactly then). Partitioned rels are loud upstream in plancat.
fn get_actual_variable_range<'mcx>(
    run: &PlannerRun<'mcx>,
    vardata: &VariableStatData<'mcx>,
    sortop: Oid,
    collation: Oid,
    want_min: bool,
    want_max: bool,
) -> PgResult<(bool, Option<Datum>, Option<Datum>)> {
    const BT_LESS: i32 = 1;
    const BT_GREATER: i32 = 5;
    let Some(rel) = vardata.rel else { return Ok((false, None, None)) };
    if run.root.rel(rel).indexlist.is_empty() {
        return Ok((false, None, None));
    }
    let Some(var_id) = vardata.var else { return Ok((false, None, None)) };
    let var_node = *run.root.expr_node(var_id);

    let nindexes = run.root.rel(rel).indexlist.len();
    for i in 0..nindexes {
        let index = run.root.rel(rel).indexlist[i];
        if index.sortopfamily.is_empty()
            || !index.indpred.is_empty()
            || index.hypothetical
            || !index.canreturn[0]
            || collation != index.indexcollations[0]
            || !crate::indxpath::match_index_to_operand(run, var_node, 0, &index)
        {
            continue;
        }
        // IndexAmTranslateStrategy, btree arm (non-btree loud in plancat).
        let indexscandir = match lsyscache::amop::get_op_opfamily_strategy(
            sortop,
            index.sortopfamily[0],
        )? {
            BT_LESS => {
                if index.reverse_sort[0] { -1 } else { 1 }
            }
            BT_GREATER => {
                if index.reverse_sort[0] { 1 } else { -1 }
            }
            _ => continue,
        };

        let mcx = run.mcx;
        let relid = run.root.rel(rel).relid;
        let reloid = run.rte(relid as usize).relid;
        let heap_rel = table::table_open(mcx, reloid, types_rel::NoLock)?;
        let index_rel = indexam::index_open(mcx, index.indexoid, types_rel::NoLock)?;
        let mut slot = tableam::table_slot_create(mcx, &heap_rel)?;
        let (typlen, typbyval) = lsyscache::typ::get_typlenbyval(vardata.vartype)?;

        let mut scankey = types_scan::scankey::ScanKeyData::empty();
        scankey.sk_flags = types_scan::scankey::SK_ISNULL
            | types_scan::scankey::SK_SEARCHNOTNULL;
        scankey.sk_attno = 1;

        let mut min = None;
        let mut max = None;
        let mut have_data = true;
        if want_min {
            min = get_actual_variable_endpoint(
                run, &heap_rel, &index_rel, indexscandir, &scankey, typlen, typbyval, &mut slot,
            )?;
            have_data = min.is_some();
        }
        if want_max && have_data {
            max = get_actual_variable_endpoint(
                run, &heap_rel, &index_rel, -indexscandir, &scankey, typlen, typbyval, &mut slot,
            )?;
            have_data = max.is_some();
        }

        indexam::index_close(index_rel, types_rel::NoLock)?;
        heap_rel.close(types_rel::NoLock)?;
        return Ok((have_data, min, max));
    }
    Ok((false, None, None))
}

// get_actual_variable_endpoint (selfuncs.c): index-only probe under
// SnapshotNonVacuumable; gives up after VISITED_PAGES_LIMIT dead heap pages.
#[allow(clippy::too_many_arguments)]
fn get_actual_variable_endpoint<'mcx>(
    run: &PlannerRun<'mcx>,
    heap_rel: &types_rel::Relation<'mcx>,
    index_rel: &types_rel::Relation<'mcx>,
    indexscandir: i32,
    scankey: &types_scan::scankey::ScanKeyData,
    typlen: i16,
    typbyval: bool,
    tableslot: &mut types_slot::SlotData<'mcx>,
) -> PgResult<Option<Datum>> {
    const VISITED_PAGES_LIMIT: i32 = 100;
    let mcx = run.mcx;
    let mut snapshot = types_snapshot::SnapshotData::sentinel(
        mcx,
        types_snapshot::SnapshotType::SNAPSHOT_NON_VACUUMABLE,
    );
    snapshot.vistest = procarray_seams::global_vis_test_for::call(heap_rel);
    let mut scan =
        indexam::index_beginscan(mcx, heap_rel, index_rel, std::rc::Rc::new(snapshot), 1, 0)?;
    scan.xs_want_itup = true;
    let keys = [scankey.clone()];
    indexam::index_rescan(&mut scan, Some(&keys), None)?;

    let dir = match indexscandir {
        -1 => types_scan::sdir::ScanDirection::BackwardScanDirection,
        1 => types_scan::sdir::ScanDirection::ForwardScanDirection,
        other => panic!("invalid index scan direction {other}"),
    };
    let mut vmbuffer = visibilitymap::VmBuffer::new();
    let mut last_heap_block = None;
    let mut n_visited_heap_pages = 0;
    let mut result = None;
    while let Some(tid) = indexam::index_getnext_tid(&mut scan, dir)? {
        let block = types_tuple::itemptr::ItemPointerGetBlockNumber(&tid);
        if !visibilitymap::vm_all_visible(heap_rel, block, &mut vmbuffer)? {
            if !indexam::index_fetch_heap(mcx, &mut scan, tableslot)? {
                if last_heap_block != Some(block) {
                    last_heap_block = Some(block);
                    n_visited_heap_pages += 1;
                    if n_visited_heap_pages > VISITED_PAGES_LIMIT {
                        break;
                    }
                }
                continue;
            }
            exectuples::exec_clear_tuple(tableslot, mcx);
        }
        let Some(itup) = scan.xs_itup else {
            panic!("no data returned for index-only scan");
        };
        if scan.xs_recheck {
            break;
        }
        let itupdesc =
            scan.xs_itupdesc.as_deref().expect("amgettuple published xs_itup without xs_itupdesc");
        let mut isnull = false;
        // SAFETY: xs_itup points at the AM's page-copy buffer, live until the
        // next amgettuple/amendscan on this descriptor.
        let value =
            unsafe { nbtree::itup::index_getattr(itup.as_ptr(), 1, itupdesc, &mut isnull) };
        assert!(!isnull, "found unexpected null value in index");
        result = Some(endpoint_datum_copy(mcx, value, typbyval, typlen)?);
        break;
    }
    indexam::index_endscan(scan)?;
    Ok(result)
}

// datumCopy (datum.c): the probed value points into the AM's page buffer and
// must outlive the scan; toast pointers cannot appear in an index key image.
fn endpoint_datum_copy<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    value: Datum,
    typbyval: bool,
    typlen: i16,
) -> PgResult<Datum> {
    if typbyval {
        return Ok(value);
    }
    let p = value.as_usize() as *const u8;
    assert!(!p.is_null());
    let size = match typlen {
        -1 => {
            // SAFETY: non-null by-ref varlena datum.
            unsafe { datum::VarlenaRef::from_ptr(p).varsize() }
        }
        -2 => {
            let mut n = 0usize;
            // SAFETY: non-null NUL-terminated cstring datum.
            while unsafe { *p.add(n) } != 0 {
                n += 1;
            }
            n + 1
        }
        l => {
            debug_assert!(l > 0);
            l as usize
        }
    };
    // SAFETY: `size` bytes readable per the arms above.
    let src = unsafe { core::slice::from_raw_parts(p, size) };
    let out = mcx::slice_in(mcx, src)?;
    Ok(Datum::from_usize(out.leak().as_ptr() as usize))
}

pub(crate) fn histogram_selectivity<'mcx>(
    vardata: &VariableStatData<'mcx>,
    opproc: &mut FmgrInfo,
    collation: Oid,
    constval: Datum,
    varonleft: bool,
    min_hist_size: usize,
    n_skip: usize,
) -> PgResult<(f64, usize)> {
    debug_assert!(min_hist_size > 2 * n_skip);
    let Some(sslot) = vardata.slot(STATISTIC_KIND_HISTOGRAM, 0) else {
        return Ok((-1.0, 0));
    };
    let values = sslot.values()?;
    let hist_size = values.len();
    if hist_size < min_hist_size {
        return Ok((-1.0, hist_size));
    }
    let mut nmatch = 0usize;
    for &v in &values[n_skip..hist_size - n_skip] {
        if op_test(opproc, collation, v, constval, varonleft)? {
            nmatch += 1;
        }
    }
    Ok((nmatch as f64 / (hist_size - 2 * n_skip) as f64, hist_size))
}

// ineq_histogram_selectivity (selfuncs.c); -1 means no usable histogram.
#[allow(clippy::too_many_arguments)]
pub(crate) fn ineq_histogram_selectivity<'mcx>(
    run: &PlannerRun<'mcx>,
    vardata: &VariableStatData<'mcx>,
    opoid: Oid,
    opproc: &mut FmgrInfo,
    isgt: bool,
    iseq: bool,
    collation: Oid,
    constval: Datum,
    consttype: Oid,
) -> PgResult<f64> {
    let mut hist_selec = -1.0f64;
    let Some(sslot) = vardata.slot(STATISTIC_KIND_HISTOGRAM, 0) else {
        return Ok(hist_selec);
    };
    let nvalues = sslot.values()?.len() as i32;
    if nvalues > 1
        && sslot.stacoll == collation
        && lsyscache::comparison_ops_are_compatible(sslot.staop, opoid)?
    {
        // C overwrites sslot.values[0]/[nvalues-1] in place with the probed
        // actual endpoints; the overrides model that without mutating the
        // cached stats bundle.
        let mut min_override: Option<Datum> = None;
        let mut max_override: Option<Datum> = None;
        let mut have_end = false;
        if nvalues == 2 {
            let (ok, min, max) =
                get_actual_variable_range(run, vardata, sslot.staop, collation, true, true)?;
            have_end = ok;
            min_override = min;
            max_override = max;
        }
        let mut lobound = 0i32;
        let mut hibound = nvalues;
        while lobound < hibound {
            let probe = (lobound + hibound) / 2;
            if probe == 0 && nvalues > 2 {
                let (ok, min, _) = get_actual_variable_range(
                    run, vardata, sslot.staop, collation, true, false,
                )?;
                have_end = ok;
                min_override = min;
            } else if probe == nvalues - 1 && nvalues > 2 {
                let (ok, _, max) = get_actual_variable_range(
                    run, vardata, sslot.staop, collation, false, true,
                )?;
                have_end = ok;
                max_override = max;
            }
            let probe_val = if probe == 0 && min_override.is_some() {
                min_override.unwrap()
            } else if probe == nvalues - 1 && max_override.is_some() {
                max_override.unwrap()
            } else {
                sslot.values()?[probe as usize]
            };
            let mut ltcmp = op_test(opproc, collation, probe_val, constval, true)?;
            if isgt {
                ltcmp = !ltcmp;
            }
            if ltcmp {
                lobound = probe + 1;
            } else {
                hibound = probe;
            }
        }

        let histfrac;
        if lobound <= 0 {
            histfrac = 0.0;
        } else if lobound >= nvalues {
            histfrac = 1.0;
        } else {
            let i = lobound;
            let mut eq_selec = 0.0;
            if i == 1 || isgt == iseq {
                let mut otherdistinct = get_variable_numdistinct(run, vardata).0;
                if let Some(mcvslot) = vardata.slot(STATISTIC_KIND_MCV, 0) {
                    otherdistinct -= mcvslot.numbers()?.len() as f64;
                }
                if otherdistinct > 1.0 {
                    eq_selec = 1.0 / otherdistinct;
                }
            }

            let bin_val = |idx: i32| -> PgResult<Datum> {
                if idx == 0 && min_override.is_some() {
                    return Ok(min_override.unwrap());
                }
                if idx == nvalues - 1 && max_override.is_some() {
                    return Ok(max_override.unwrap());
                }
                Ok(sslot.values()?[idx as usize])
            };
            let binfrac = match convert_to_scalar(
                run.mcx,
                constval,
                consttype,
                collation,
                bin_val(i - 1)?,
                bin_val(i)?,
                vardata.vartype,
            ) {
                Some((val, low, high)) => {
                    if high <= low {
                        0.5
                    } else if val <= low {
                        0.0
                    } else if val >= high {
                        1.0
                    } else {
                        let b = (val - low) / (high - low);
                        if b.is_nan() || !(0.0..=1.0).contains(&b) { 0.5 } else { b }
                    }
                }
                None => 0.5,
            };

            let mut frac = (i - 1) as f64 + binfrac;
            frac /= (nvalues - 1) as f64;
            if i == 1 {
                frac += eq_selec * (1.0 - binfrac);
            }
            if isgt == iseq {
                frac -= eq_selec;
            }
            histfrac = frac;
        }

        hist_selec = if isgt { 1.0 - histfrac } else { histfrac };

        if have_end {
            hist_selec = clamp_probability(hist_selec);
        } else {
            let cutoff = 0.01 / (nvalues - 1) as f64;
            hist_selec = hist_selec.clamp(cutoff, 1.0 - cutoff);
        }
    } else if nvalues > 1 {
        let mut nmatch = 0;
        for &v in sslot.values()?.iter() {
            if op_test(opproc, collation, v, constval, true)? {
                nmatch += 1;
            }
        }
        hist_selec = nmatch as f64 / nvalues as f64;
        let cutoff = 0.01 / (nvalues - 1) as f64;
        hist_selec = hist_selec.clamp(cutoff, 1.0 - cutoff);
    }
    Ok(hist_selec)
}

// convert_to_scalar (selfuncs.c), numeric + string categories. bytea/time/
// network categories fall back to None, which lands on C's binfrac=0.5
// failure path — a divergence for those types.
fn convert_to_scalar(
    mcx: mcx::Mcx<'_>,
    value: Datum,
    valuetypid: Oid,
    collid: Oid,
    lobound: Datum,
    hibound: Datum,
    boundstypid: Oid,
) -> Option<(f64, f64, f64)> {
    const CHAROID: Oid = 18;
    const NAMEOID: Oid = 19;
    const TEXTOID: Oid = 25;
    const BPCHAROID: Oid = 1042;
    const VARCHAROID: Oid = 1043;
    match valuetypid {
        CHAROID | BPCHAROID | VARCHAROID | TEXTOID | NAMEOID => {
            let val = convert_string_datum(mcx, value, valuetypid, collid)?;
            let lostr = convert_string_datum(mcx, lobound, boundstypid, collid)?;
            let histr = convert_string_datum(mcx, hibound, boundstypid, collid)?;
            Some(convert_string_to_scalar(val, lostr, histr))
        }
        _ => {
            let v = convert_numeric_to_scalar(value, valuetypid)?;
            let lo = convert_numeric_to_scalar(lobound, boundstypid)?;
            let hi = convert_numeric_to_scalar(hibound, boundstypid)?;
            Some((v, lo, hi))
        }
    }
}

// convert_string_datum (selfuncs.c); the non-C-collation pg_strxfrm leg is
// the locale-aware lane and stays loud.
fn convert_string_datum<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    value: Datum,
    typid: Oid,
    collid: Oid,
) -> Option<&'mcx [u8]> {
    const CHAROID: Oid = 18;
    const NAMEOID: Oid = 19;
    const TEXTOID: Oid = 25;
    const BPCHAROID: Oid = 1042;
    const VARCHAROID: Oid = 1043;
    let bytes: &[u8] = match typid {
        CHAROID => {
            // C builds a 2-byte cstring from the char datum; a single-byte
            // arena slice carries the same information.
            let b = [value.as_u8()];
            mcx::slice_in(mcx, &b).ok()?.leak()
        }
        BPCHAROID | VARCHAROID | TEXTOID => {
            // SAFETY: by-ref text datum living in the planner arena.
            unsafe { datum::VarlenaRef::from_ptr(value.as_usize() as *const u8).data() }
        }
        NAMEOID => {
            let p = value.as_usize() as *const u8;
            let mut n = 0usize;
            // SAFETY: name datum is a NUL-terminated NAMEDATALEN block.
            while n < 63 && unsafe { *p.add(n) } != 0 {
                n += 1;
            }
            // SAFETY: `n` bytes readable per the loop above.
            unsafe { core::slice::from_raw_parts(p, n) }
        }
        _ => return None,
    };
    let locale = pg_locale::pg_newlocale_from_collation(collid)
        .expect("convert_string_datum: collation lookup");
    if !locale.collate_is_c {
        panic!("convert_string_datum (selfuncs.c): pg_strxfrm leg; C-collation lane only");
    }
    Some(bytes)
}

fn convert_string_to_scalar(value: &[u8], lobound: &[u8], hibound: &[u8]) -> (f64, f64, f64) {
    // C reads hibound[0] unconditionally; an empty C string yields NUL.
    let mut rangelo = *hibound.first().unwrap_or(&0) as i32;
    let mut rangehi = rangelo;
    for &c in lobound.iter().chain(hibound.iter()) {
        rangelo = rangelo.min(c as i32);
        rangehi = rangehi.max(c as i32);
    }
    if rangelo <= b'Z' as i32 && rangehi >= b'A' as i32 {
        rangelo = rangelo.min(b'A' as i32);
        rangehi = rangehi.max(b'Z' as i32);
    }
    if rangelo <= b'z' as i32 && rangehi >= b'a' as i32 {
        rangelo = rangelo.min(b'a' as i32);
        rangehi = rangehi.max(b'z' as i32);
    }
    if rangelo <= b'9' as i32 && rangehi >= b'0' as i32 {
        rangelo = rangelo.min(b'0' as i32);
        rangehi = rangehi.max(b'9' as i32);
    }
    if rangehi - rangelo < 9 {
        rangelo = b' ' as i32;
        rangehi = 127;
    }

    let mut p = 0usize;
    while p < lobound.len() {
        if hibound.get(p) != Some(&lobound[p]) || value.get(p) != Some(&lobound[p]) {
            break;
        }
        p += 1;
    }

    (
        convert_one_string_to_scalar(&value[p.min(value.len())..], rangelo, rangehi),
        convert_one_string_to_scalar(&lobound[p..], rangelo, rangehi),
        convert_one_string_to_scalar(&hibound[p.min(hibound.len())..], rangelo, rangehi),
    )
}

fn convert_one_string_to_scalar(value: &[u8], rangelo: i32, rangehi: i32) -> f64 {
    let slen = value.len().min(12);
    if slen == 0 {
        return 0.0;
    }
    let base = (rangehi - rangelo + 1) as f64;
    let mut num = 0.0f64;
    let mut denom = base;
    for &b in &value[..slen] {
        let mut ch = b as i32;
        if ch < rangelo {
            ch = rangelo - 1;
        } else if ch > rangehi {
            ch = rangehi + 1;
        }
        num += (ch - rangelo) as f64 / denom;
        denom *= base;
    }
    num
}

fn convert_numeric_to_scalar(value: Datum, typid: Oid) -> Option<f64> {
    const INT2OID: Oid = 21;
    const INT4OID: Oid = 23;
    const INT8OID: Oid = 20;
    const FLOAT4OID: Oid = 700;
    const FLOAT8OID: Oid = 701;
    const OIDOID: Oid = 26;
    const REGPROCOID: Oid = 24;
    const REGPROCEDUREOID: Oid = 2202;
    const REGOPEROID: Oid = 2203;
    const REGOPERATOROID: Oid = 2204;
    const REGCLASSOID: Oid = 2205;
    const REGTYPEOID: Oid = 2206;
    match typid {
        BOOLOID => Some(value.as_bool() as i32 as f64),
        INT2OID => Some(value.as_i16() as f64),
        INT4OID => Some(value.as_i32() as f64),
        INT8OID => Some(value.as_i64() as f64),
        FLOAT4OID => Some(value.as_f32() as f64),
        FLOAT8OID => Some(value.as_f64()),
        OIDOID | REGPROCOID | REGPROCEDUREOID | REGOPEROID | REGOPERATOROID | REGCLASSOID
        | REGTYPEOID => Some(value.as_u32() as f64),
        _ => None,
    }
}

pub fn eqsel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    operator: u32,
    args: &[NodeId],
    varrelid: i32,
    collation: u32,
) -> PgResult<f64> {
    eqsel_internal(run, operator, args, varrelid, collation, false)
}

pub fn neqsel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    operator: u32,
    args: &[NodeId],
    varrelid: i32,
    collation: u32,
) -> PgResult<f64> {
    eqsel_internal(run, operator, args, varrelid, collation, true)
}

fn eqsel_internal<'mcx>(
    run: &mut PlannerRun<'mcx>,
    mut operator: u32,
    args: &[NodeId],
    varrelid: i32,
    collation: u32,
    negate: bool,
) -> PgResult<f64> {
    if negate {
        // Stats probes run against the negator (the equality operator).
        operator = lsyscache::get_negator(operator)?;
        if operator == 0 {
            return Ok(1.0 - DEFAULT_EQ_SEL);
        }
    }
    let Some((vardata, other, varonleft)) = get_restriction_variable(run, args, varrelid)? else {
        return Ok(if negate { 1.0 - DEFAULT_EQ_SEL } else { DEFAULT_EQ_SEL });
    };
    let selec = match other.as_const() {
        Some(c) => var_eq_const(
            run,
            &vardata,
            operator,
            collation,
            c.constvalue,
            c.constisnull,
            varonleft,
            negate,
        )?,
        None => var_eq_non_const(run, &vardata, negate),
    };
    Ok(selec)
}

// var_eq_non_const (selfuncs.c).
fn var_eq_non_const(run: &PlannerRun<'_>, vardata: &VariableStatData<'_>, negate: bool) -> f64 {
    let nullfrac = vardata.nullfrac();
    let selec = if vardata.isunique
        && vardata.rel.is_some_and(|r| run.root.rel(r).tuples >= 1.0)
    {
        1.0 / run.root.rel(vardata.rel.unwrap()).tuples
    } else if vardata.stats.is_some() {
        let mut selec = 1.0 - nullfrac;
        let nd = get_variable_numdistinct(run, vardata).0;
        if nd > 1.0 {
            selec /= nd;
        }
        if let Some(sslot) = vardata.slot(STATISTIC_KIND_MCV, 0) {
            if let Some(&first) = sslot.numbers().ok().and_then(|n| n.first()) {
                if selec > first as f64 {
                    selec = first as f64;
                }
            }
        }
        selec
    } else {
        1.0 / get_variable_numdistinct(run, vardata).0
    };
    let selec = if negate { 1.0 - selec - nullfrac } else { selec };
    clamp_probability(selec)
}
pub(crate) fn get_restriction_variable<'mcx>(
    run: &mut PlannerRun<'mcx>,
    args: &[NodeId],
    varrelid: i32,
) -> PgResult<Option<(VariableStatData<'mcx>, Node<'mcx>, bool)>> {
    if args.len() != 2 {
        return Ok(None);
    }
    let left = *run.root.expr_node(args[0]);
    let right = *run.root.expr_node(args[1]);
    let vardata = examine_variable(run, args[0], left, varrelid)?;
    let rdata = examine_variable(run, args[1], right, varrelid)?;

    // estimate_expression_value: Consts pass through and a PARAM_EXEC stays a
    // Param (no bound value at plan time); other shapes keep the loud arm.
    if vardata.rel.is_some() && rdata.rel.is_none() {
        if !matches!(right.node_tag(), NodeTag::T_Const | NodeTag::T_Param) {
            panic!("estimate_expression_value (clauses.c): M2 expression lane");
        }
        return Ok(Some((vardata, right, true)));
    }
    if vardata.rel.is_none() && rdata.rel.is_some() {
        if !matches!(left.node_tag(), NodeTag::T_Const | NodeTag::T_Param) {
            panic!("estimate_expression_value (clauses.c): M2 expression lane");
        }
        return Ok(Some((rdata, left, false)));
    }
    Ok(None)
}

// examine_variable (selfuncs.c), plain-Var and pseudo-constant arms.
pub fn examine_variable<'mcx>(
    run: &mut PlannerRun<'mcx>,
    node_id: NodeId,
    node: Node<'mcx>,
    varrelid: i32,
) -> PgResult<VariableStatData<'mcx>> {
    let (vartype, _) = crate::costsize::expr_type_typmod(node);
    let mut vardata =
        VariableStatData { var: None, rel: None, vartype, isunique: false, stats: None };

    if let Some(var) = node.as_var() {
        if varrelid == 0 || varrelid == var.varno {
            let rel = crate::relnode::find_base_rel(&run.root, var.varno);
            vardata.var = Some(node_id);
            vardata.rel = Some(rel);
            vardata.isunique = crate::plancat::has_unique_index(run, rel, var.varattno);
            vardata.stats = examine_simple_variable(run, var.varno, var.varattno)?;
            return Ok(vardata);
        }
        panic!("examine_variable (selfuncs.c): foreign-rel Var; M2 join lane");
    }
    match node.node_tag() {
        NodeTag::T_Const => Ok(vardata),
        // Var-free expressions (HAVING Aggrefs, PARAM_EXEC initplan outputs):
        // C's expression leg finds no relids and returns "don't know".
        NodeTag::T_Aggref | NodeTag::T_Param => Ok(vardata),
        // C's general expression leg: a single-rel expression keeps its rel
        // (tuple-count clamps) but has no stats (extended statistics absent).
        _ => {
            let varnos = vars::pull_varnos(run.mcx, node)?;
            if let Some(v) = varnos.get_singleton_member() {
                if varrelid == 0 || varrelid == v {
                    vardata.var = Some(node_id);
                    vardata.rel = Some(crate::relnode::find_base_rel(&run.root, v));
                }
            }
            Ok(vardata)
        }
    }
}

// examine_simple_variable (selfuncs.c): the STATRELATTINH probe, decoded once.
fn examine_simple_variable<'mcx>(
    run: &PlannerRun<'mcx>,
    varno: i32,
    varattno: i16,
) -> PgResult<Option<PgStatisticBundle<'mcx>>> {
    let rte = run.rte(varno as usize);
    if rte.rtekind != RTEKind::RTE_RELATION {
        panic!("examine_simple_variable (selfuncs.c): {:?}; M2 lane", rte.rtekind);
    }
    syscache_seams::lookup_pg_statistic_bundle::call(run.mcx, rte.relid, varattno, rte.inh)
}

// get_variable_numdistinct (selfuncs.c). Returns (ndistinct, isdefault).
pub fn get_variable_numdistinct(
    run: &PlannerRun<'_>,
    vardata: &VariableStatData<'_>,
) -> (f64, bool) {
    let mut stanullfrac = 0.0f64;
    let mut stadistinct;
    if let Some(stats) = &vardata.stats {
        stadistinct = stats.stadistinct as f64;
        stanullfrac = stats.stanullfrac as f64;
    } else if vardata.vartype == BOOLOID {
        stadistinct = 2.0;
    } else {
        let attno = vardata
            .var
            .and_then(|id| run.root.expr_node(id).as_var().map(|v| v.varattno));
        stadistinct = match attno {
            Some(SELF_ITEM_POINTER_ATTRIBUTE_NUMBER) => -1.0,
            Some(TABLE_OID_ATTRIBUTE_NUMBER) => 1.0,
            _ => 0.0,
        };
    }
    if vardata.isunique {
        stadistinct = -1.0 * (1.0 - stanullfrac);
    }
    if stadistinct > 0.0 {
        return (crate::costsize::clamp_row_est(stadistinct), false);
    }
    let Some(rel) = vardata.rel else {
        return (DEFAULT_NUM_DISTINCT, true);
    };
    let ntuples = run.root.rel(rel).tuples;
    if ntuples <= 0.0 {
        return (DEFAULT_NUM_DISTINCT, true);
    }
    if stadistinct < 0.0 {
        return (crate::costsize::clamp_row_est(-stadistinct * ntuples), false);
    }
    if ntuples < DEFAULT_NUM_DISTINCT {
        return (crate::costsize::clamp_row_est(ntuples), false);
    }
    (DEFAULT_NUM_DISTINCT, true)
}

// var_eq_const (selfuncs.c).
pub(crate) fn var_eq_const<'mcx>(
    run: &PlannerRun<'mcx>,
    vardata: &VariableStatData<'mcx>,
    oproid: Oid,
    collation: Oid,
    constval: Datum,
    constisnull: bool,
    varonleft: bool,
    negate: bool,
) -> PgResult<f64> {
    // NULL const: strict operator never returns TRUE, even for the negator.
    if constisnull {
        return Ok(0.0);
    }
    let nullfrac = vardata.nullfrac();

    let selec = if vardata.isunique
        && vardata.rel.is_some_and(|r| run.root.rel(r).tuples >= 1.0)
    {
        1.0 / run.root.rel(vardata.rel.unwrap()).tuples
    } else if vardata.stats.is_some() {
        match vardata.slot(STATISTIC_KIND_MCV, 0) {
            Some(sslot) => {
                let mut eqproc = opproc_for(oproid)?;
                let mut matched = None;
                for (i, &v) in sslot.values()?.iter().enumerate() {
                    if op_test(&mut eqproc, collation, v, constval, varonleft)? {
                        matched = Some(i);
                        break;
                    }
                }
                match matched {
                    Some(i) => sslot.numbers()?[i] as f64,
                    None => {
                        let sumcommon: f64 =
                            sslot.numbers()?.iter().map(|&n| n as f64).sum();
                        let mut selec =
                            clamp_probability(1.0 - sumcommon - nullfrac);
                        let otherdistinct = get_variable_numdistinct(run, vardata).0
                            - sslot.numbers()?.len() as f64;
                        if otherdistinct > 1.0 {
                            selec /= otherdistinct;
                        }
                        let least = sslot.numbers()?.last().copied().unwrap_or(0.0) as f64;
                        if !sslot.numbers()?.is_empty() && selec > least {
                            selec = least;
                        }
                        selec
                    }
                }
            }
            None => {
                let mut selec = 1.0 - nullfrac;
                // C treats an absent MCV slot as "no info" and still divides
                // the non-null fraction by ndistinct.
                let nd = get_variable_numdistinct(run, vardata).0;
                if nd > 1.0 {
                    selec /= nd;
                }
                selec
            }
        }
    } else {
        1.0 / get_variable_numdistinct(run, vardata).0
    };
    let selec = if negate { 1.0 - selec - nullfrac } else { selec };
    Ok(clamp_probability(selec))
}

pub struct AmCostEstimate {
    pub index_startup_cost: f64,
    pub index_total_cost: f64,
    pub index_selectivity: f64,
    pub index_correlation: f64,
    pub index_pages: f64,
}

// amcostestimate dispatch: closed set over the committed index AMs (rule 4).
pub fn amcostestimate(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    loop_count: f64,
) -> PgResult<AmCostEstimate> {
    let relam = {
        let PathNode::IndexPath(ip) = run.root.path(path_id) else {
            panic!("amcostestimate: not an IndexPath")
        };
        ip.indexinfo.as_ref().expect("indexinfo set").relam
    };
    match types_relscan::IndexAmKind::from_relam(relam) {
        types_relscan::IndexAmKind::Btree => btcostestimate(run, path_id, loop_count),
        types_relscan::IndexAmKind::Hash => hashcostestimate(run, path_id, loop_count),
        types_relscan::IndexAmKind::Gin => gincostestimate(run, path_id, loop_count),
        #[allow(unreachable_patterns)]
        other => panic!("amcostestimate (selfuncs.c): {other:?}; M2 index-AM lane"),
    }
}

struct GenericCosts {
    num_index_tuples: f64,
    num_sa_scans: f64,
    index_startup_cost: f64,
    index_total_cost: f64,
    index_selectivity: f64,
    index_correlation: f64,
    num_index_pages: f64,
}

// genericcostestimate (selfuncs.c); num_sa_scans arrives preset (no SAOP).
fn genericcostestimate(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    loop_count: f64,
    costs: &mut GenericCosts,
) -> PgResult<()> {
    let (index_quals, has_orderbys, index_pages, index_tuples, index_rel, reltablespace) = {
        let PathNode::IndexPath(ip) = run.root.path(path_id) else { unreachable!() };
        let index = ip.indexinfo.as_ref().expect("indexinfo set");
        (
            get_quals_from_indexclauses(run, path_id),
            !ip.indexorderbys.is_empty(),
            index.pages,
            index.tuples,
            index.rel.expect("index rel set"),
            index.reltablespace,
        )
    };
    assert!(!has_orderbys, "genericcostestimate (selfuncs.c): indexorderbys; M2 amcanorderbyop lane");
    let index_rel_relid = run.root.rel(index_rel).relid as i32;
    let index_rel_tuples = run.root.rel(index_rel).tuples;

    // add_predicate_to_index_quals: identity for a non-partial index.
    debug_assert!(costs.num_sa_scans >= 1.0);
    let num_sa_scans = costs.num_sa_scans;

    let index_selectivity = crate::clausesel::clauselist_selectivity(
        run,
        &index_quals,
        index_rel_relid,
        JOIN_INNER,
        None,
    )?;

    let mut num_index_tuples = costs.num_index_tuples;
    if num_index_tuples <= 0.0 {
        num_index_tuples = index_selectivity * index_rel_tuples;
        num_index_tuples = (num_index_tuples / num_sa_scans).round_ties_even();
    }
    if num_index_tuples > index_tuples {
        num_index_tuples = index_tuples;
    }
    if num_index_tuples < 1.0 {
        num_index_tuples = 1.0;
    }

    let num_index_pages = if index_pages > 1 && index_tuples > 1.0 {
        (num_index_tuples * index_pages as f64 / index_tuples).ceil()
    } else {
        1.0
    };

    let (spc_random_page_cost, _) = crate::costsize::get_tablespace_page_costs(reltablespace);

    let num_scans = num_sa_scans * loop_count;
    let mut index_total_cost = if num_scans > 1.0 {
        let pages_fetched = crate::costsize::index_pages_fetched(
            run,
            num_index_pages * num_scans,
            index_pages,
            index_pages as f64,
        );
        (pages_fetched * spc_random_page_cost) / loop_count
    } else {
        num_index_pages * spc_random_page_cost
    };

    let qual_arg_cost = index_other_operands_eval_cost(run, &index_quals)?;
    let qual_op_cost = gucs::cpu_operator_cost() * index_quals.len() as f64;

    let index_startup_cost = qual_arg_cost;
    index_total_cost += qual_arg_cost;
    index_total_cost += num_index_tuples * num_sa_scans * (gucs::cpu_index_tuple_cost() + qual_op_cost);

    costs.index_startup_cost = index_startup_cost;
    costs.index_total_cost = index_total_cost;
    costs.index_selectivity = index_selectivity;
    costs.index_correlation = 0.0;
    costs.num_index_pages = num_index_pages;
    costs.num_index_tuples = num_index_tuples;
    costs.num_sa_scans = num_sa_scans;
    Ok(())
}

// get_quals_from_indexclauses (selfuncs.c).
fn get_quals_from_indexclauses<'mcx>(
    run: &PlannerRun<'mcx>,
    path_id: types_pathnodes::PathId,
) -> mcx::PgVec<'mcx, RinfoId> {
    let PathNode::IndexPath(ip) = run.root.path(path_id) else { unreachable!() };
    let mut out = mcx::PgVec::new_in(run.mcx);
    for ic in ip.indexclauses.iter() {
        for &r in ic.indexquals.iter() {
            out.push(r);
        }
    }
    out
}

// index_other_operands_eval_cost (selfuncs.c).
fn index_other_operands_eval_cost(
    run: &mut PlannerRun<'_>,
    index_quals: &[RinfoId],
) -> PgResult<f64> {
    let mut qual_arg_cost = 0.0;
    for &rid in index_quals {
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        let other_operand = match clause.node_tag() {
            // indexkey is always the left operand of a fixed indexqual.
            NodeTag::T_OpExpr => Some(clause.as_op_expr().unwrap().args.nth(1)),
            NodeTag::T_NullTest => None,
            other => panic!("index_other_operands_eval_cost (selfuncs.c): {other:?}; M2 lane"),
        };
        if let Some(op) = other_operand {
            let cost = crate::costsize::cost_qual_eval_node(op)?;
            qual_arg_cost += cost.startup + cost.per_tuple;
        }
    }
    Ok(qual_arg_cost)
}

// hashcostestimate (selfuncs.c): pure genericcostestimate; no descent costs
// (bucket lookup is O(1); the deliberate C choice is kept verbatim).
fn hashcostestimate(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    loop_count: f64,
) -> PgResult<AmCostEstimate> {
    let mut costs = GenericCosts {
        num_index_tuples: 0.0,
        num_sa_scans: 1.0,
        index_startup_cost: 0.0,
        index_total_cost: 0.0,
        index_selectivity: 0.0,
        index_correlation: 0.0,
        num_index_pages: 0.0,
    };
    genericcostestimate(run, path_id, loop_count, &mut costs)?;
    Ok(AmCostEstimate {
        index_startup_cost: costs.index_startup_cost,
        index_total_cost: costs.index_total_cost,
        index_selectivity: costs.index_selectivity,
        index_correlation: 0.0,
        index_pages: costs.num_index_pages,
    })
}

// btcostestimate (selfuncs.c); the boundary-qual walk sees only OpExprs.
fn btcostestimate(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    loop_count: f64,
) -> PgResult<AmCostEstimate> {
    let (indexclauses, index_unique, index_nkeycolumns, index_tuples, index_tree_height, index_rel, opfamilies) = {
        let PathNode::IndexPath(ip) = run.root.path(path_id) else {
            panic!("btcostestimate: not an IndexPath")
        };
        let index = ip.indexinfo.as_ref().expect("indexinfo set");
        let mut fams = mcx::PgVec::new_in(run.mcx);
        fams.extend(index.opfamily.iter().copied());
        (
            ip.indexclauses.clone(),
            index.unique,
            index.nkeycolumns,
            index.tuples,
            index.tree_height.get(),
            index.rel.expect("index rel set"),
            fams,
        )
    };
    let index_rel_relid = run.root.rel(index_rel).relid as i32;
    let index_rel_tuples = run.root.rel(index_rel).tuples;
    let index_pages = {
        let PathNode::IndexPath(ip) = run.root.path(path_id) else { unreachable!() };
        ip.indexinfo.as_ref().unwrap().pages
    };

    let index_indexkeys = {
        let PathNode::IndexPath(ip) = run.root.path(path_id) else { unreachable!() };
        ip.indexinfo.as_ref().unwrap().indexkeys.clone()
    };
    let index_opcintype = {
        let PathNode::IndexPath(ip) = run.root.path(path_id) else { unreachable!() };
        ip.indexinfo.as_ref().unwrap().opcintype.clone()
    };

    let mut index_bound_quals: mcx::PgVec<'_, RinfoId> = mcx::PgVec::new_in(run.mcx);
    let mut index_skip_quals: mcx::PgVec<'_, RinfoId> = mcx::PgVec::new_in(run.mcx);
    let mut indexcol: i32 = 0;
    let mut eq_qual_here = false;
    let mut found_array = false;
    let mut found_is_null_op = false;
    let mut num_sa_scans = 1.0f64;

    'buildquals: for iclause in indexclauses.iter() {
        if indexcol < iclause.indexcol as i32 {
            // nbtree backfills skip arrays for index columns lacking an '='
            // qual (selfuncs.c:7397 gap arm).
            let num_sa_scans_prev_cols = num_sa_scans;
            if eq_qual_here {
                indexcol += 1;
                index_skip_quals.clear();
            }
            eq_qual_here = false;
            while indexcol < iclause.indexcol as i32 {
                found_array = true;
                let attno = index_indexkeys[indexcol as usize];
                if attno == 0 {
                    panic!("btcostestimate (selfuncs.c): expression index column; M2 lane");
                }
                // examine_indexcol_variable, simple-column arm.
                let rte = run.rte(index_rel_relid as usize);
                let stats = syscache_seams::lookup_pg_statistic_bundle::call(
                    run.mcx,
                    rte.relid,
                    attno as i16,
                    rte.inh,
                )?;
                let vardata = VariableStatData {
                    var: None,
                    rel: Some(index_rel),
                    vartype: index_opcintype[indexcol as usize],
                    isunique: false,
                    stats,
                };
                let (mut ndistinct, isdefault) = get_variable_numdistinct(run, &vardata);
                // btcost_correlation-in-passing arm folds into the shared
                // leading-column correlation block below (same stats row).
                if isdefault {
                    num_sa_scans = num_sa_scans_prev_cols;
                    break 'buildquals;
                }
                if !index_skip_quals.is_empty() {
                    let ndistinctfrac = crate::clausesel::clauselist_selectivity(
                        run,
                        &index_skip_quals,
                        index_rel_relid,
                        JOIN_INNER,
                        None,
                    )?;
                    if ndistinctfrac < 0.005 {  // DEFAULT_RANGE_INEQ_SEL
                        num_sa_scans = num_sa_scans_prev_cols;
                        break 'buildquals;
                    }
                    ndistinct = (ndistinct * ndistinctfrac).round_ties_even().max(1.0);
                }
                if index_skip_quals.is_empty() {
                    ndistinct += 1.0;
                }
                num_sa_scans *= ndistinct;
                if (index_pages as f64) < num_sa_scans {
                    num_sa_scans = num_sa_scans_prev_cols;
                    break 'buildquals;
                }
                indexcol += 1;
                index_skip_quals.clear();
            }
        }
        debug_assert!(indexcol == iclause.indexcol as i32);

        for &rid in iclause.indexquals.iter() {
            let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
            let clause_op = match clause.node_tag() {
                NodeTag::T_OpExpr => clause.as_op_expr().unwrap().opno,
                NodeTag::T_NullTest => {
                    if clause.as_null_test().unwrap().nulltesttype
                        == types_nodes::primnodes::NullTestType::IS_NULL
                    {
                        found_is_null_op = true;
                        // IS NULL is like = for selectivity/skip-scan purposes.
                        eq_qual_here = true;
                    }
                    0
                }
                other => panic!("btcostestimate (selfuncs.c): indexqual {other:?}; M2 lane"),
            };
            if clause_op != 0 {
                let op_strategy =
                    lsyscache::get_op_opfamily_strategy(clause_op, opfamilies[indexcol as usize])?;
                debug_assert!(op_strategy != 0);
                if op_strategy == lsyscache::BTEqualStrategyNumber as i32 {
                    eq_qual_here = true;
                }
            }
            index_bound_quals.push(rid);
            if !eq_qual_here && indexcol < index_nkeycolumns - 1 {
                index_skip_quals.push(rid);
            }
        }
    }

    let num_index_tuples = if index_unique
        && indexcol == index_nkeycolumns - 1
        && eq_qual_here
        && !found_array
        && !found_is_null_op
    {
        1.0
    } else {
        let btree_selectivity = crate::clausesel::clauselist_selectivity(
            run,
            &index_bound_quals,
            index_rel_relid,
            JOIN_INNER,
            None,
        )?;
        let nit = btree_selectivity * index_rel_tuples;
        num_sa_scans = num_sa_scans.min((index_pages as f64 * 0.3333333).ceil()).max(1.0);
        (nit / num_sa_scans).round_ties_even()
    };

    let mut costs = GenericCosts {
        num_index_tuples,
        num_sa_scans,
        index_startup_cost: 0.0,
        index_total_cost: 0.0,
        index_selectivity: 0.0,
        index_correlation: 0.0,
        num_index_pages: 0.0,
    };
    genericcostestimate(run, path_id, loop_count, &mut costs)?;

    let cpu_operator_cost = gucs::cpu_operator_cost();
    if index_tuples > 1.0 {
        let descent_cost = (index_tuples.ln() / 2.0f64.ln()).ceil() * cpu_operator_cost;
        costs.index_startup_cost += descent_cost;
        costs.index_total_cost += costs.num_sa_scans * descent_cost;
    }
    let descent_cost =
        (index_tree_height as f64 + 1.0) * DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost;
    costs.index_startup_cost += descent_cost;
    costs.index_total_cost += costs.num_sa_scans * descent_cost;

    // btcost_correlation over the leading simple column.
    {
        let (attno, opfamily0, opcintype0, reverse0, nkeycols) = {
            let PathNode::IndexPath(ip) = run.root.path(path_id) else { unreachable!() };
            let index = ip.indexinfo.as_ref().unwrap();
            (
                index.indexkeys[0] as i16,
                index.opfamily[0],
                index.opcintype[0],
                index.reverse_sort[0],
                index.nkeycolumns,
            )
        };
        if attno == 0 {
            panic!("btcost_correlation (selfuncs.c): expression index column; M2 lane");
        }
        let rte = run.rte(index_rel_relid as usize);
        if let Some(bundle) =
            syscache_seams::lookup_pg_statistic_bundle::call(run.mcx, rte.relid, attno, rte.inh)?
        {
            let sortop = lsyscache::get_opfamily_member(
                opfamily0,
                opcintype0,
                opcintype0,
                lsyscache::BTLessStrategyNumber,
            )?;
            let slot = bundle
                .slots
                .iter()
                .find(|sl| sl.kind == STATISTIC_KIND_CORRELATION && sl.staop == sortop);
            if let (true, Some(slot)) = (sortop != 0, slot) {
                debug_assert!(slot.numbers()?.len() == 1);
                let mut corr = slot.numbers()?[0] as f64;
                if reverse0 {
                    corr = -corr;
                }
                costs.index_correlation = if nkeycols > 1 { corr * 0.75 } else { corr };
            }
        }
    }
    let _ = index_pages;

    Ok(AmCostEstimate {
        index_startup_cost: costs.index_startup_cost,
        index_total_cost: costs.index_total_cost,
        index_selectivity: costs.index_selectivity,
        index_correlation: costs.index_correlation,
        index_pages: costs.num_index_pages,
    })
}

// estimate_num_groups (selfuncs.c), no-stats Var-only leg; other families
// and multivariate/extended stats are M3 lanes.
pub fn estimate_num_groups<'mcx>(
    run: &mut PlannerRun<'mcx>,
    group_exprs: &[(NodeId, Node<'mcx>)],
    input_rows: f64,
) -> PgResult<f64> {
    estimate_num_groups_pgset(run, group_exprs, input_rows, None)
}

/// C's `pgset` form: a grouping set given as 0-based indexes into
/// `group_exprs`; exprs outside the set are skipped.
pub fn estimate_num_groups_pgset<'mcx>(
    run: &mut PlannerRun<'mcx>,
    group_exprs: &[(NodeId, Node<'mcx>)],
    input_rows: f64,
    pgset: Option<&[i32]>,
) -> PgResult<f64> {
    let input_rows = crate::costsize::clamp_row_est(input_rows);
    if group_exprs.is_empty() || pgset.is_some_and(|s| s.is_empty()) {
        return Ok(1.0);
    }

    struct GroupVarInfo {
        var: NodeId,
        rel: RelId,
        ndistinct: f64,
    }
    let mcx = run.mcx;
    let mut varinfos: mcx::PgVec<'_, GroupVarInfo> = mcx::PgVec::new_in(mcx);
    let mut work: mcx::PgVec<'_, (NodeId, Node<'mcx>)> = mcx::PgVec::new_in(mcx);
    for (listidx, &(id, node)) in group_exprs.iter().enumerate() {
        if pgset.is_some_and(|s| !s.contains(&(listidx as i32))) {
            continue;
        }
        if node.node_tag() == NodeTag::T_Const {
            continue;
        }
        if node.node_tag() == NodeTag::T_Var {
            work.push((id, node));
            continue;
        }
        // C's expression leg: no expression stats, so decompose to the
        // contained Vars (a Var-free volatile expr keeps every row distinct).
        let vars_here = vars::pull_var_clause(mcx, node, 0)?;
        if vars_here.is_nil() {
            if clauses::contain_volatile_functions(node)? {
                return Ok(input_rows);
            }
            continue;
        }
        for v in &vars_here {
            work.push((run.intern_expr(v), v));
        }
    }
    for &(id, node) in work.iter() {
        let v = node.as_var().unwrap();
        let dup = varinfos.iter().any(|vi| {
            let u = run.root.expr_node(vi.var).as_var().unwrap();
            u.varno == v.varno && u.varattno == v.varattno
        });
        if dup {
            continue;
        }
        let vardata = examine_variable(run, id, node, 0)?;
        let (ndistinct, _isdefault) = get_variable_numdistinct(run, &vardata);
        varinfos.push(GroupVarInfo {
            var: id,
            rel: vardata.rel.expect("grouping Var has a base rel"),
            ndistinct,
        });
    }
    if varinfos.is_empty() {
        return Ok(1.0);
    }

    let mut numdistinct = 1.0f64;
    let mut remaining = varinfos;
    while !remaining.is_empty() {
        let rel_id = remaining[0].rel;
        let mut reldistinct = 1.0f64;
        let mut relmaxndistinct = 1.0f64;
        let mut relvarcount = 0usize;
        let mut rest: mcx::PgVec<'_, GroupVarInfo> = mcx::PgVec::new_in(mcx);
        for vi in remaining {
            if vi.rel == rel_id {
                reldistinct *= vi.ndistinct;
                if relmaxndistinct < vi.ndistinct {
                    relmaxndistinct = vi.ndistinct;
                }
                relvarcount += 1;
            } else {
                rest.push(vi);
            }
        }
        let (rel_tuples, rel_rows) = {
            let rel = run.root.rel(rel_id);
            (rel.tuples, rel.rows)
        };
        if rel_tuples > 0.0 {
            let mut clamp = rel_tuples;
            if relvarcount > 1 {
                clamp *= 0.1;
                if clamp < relmaxndistinct {
                    clamp = relmaxndistinct.min(rel_tuples);
                }
            }
            if reldistinct > clamp {
                reldistinct = clamp;
            }
            if reldistinct > 0.0 && rel_rows < rel_tuples {
                // Dell'Era approximation of Yao's formula.
                reldistinct *=
                    1.0 - ((rel_tuples - rel_rows) / rel_tuples).powf(rel_tuples / reldistinct);
            }
            numdistinct *= crate::costsize::clamp_row_est(reldistinct);
        }
        remaining = rest;
    }

    let numdistinct = numdistinct.ceil();
    Ok(numdistinct.clamp(1.0, input_rows))
}

// eqjoinsel (selfuncs.c). C's MCV-x-MCV arms fire only when BOTH sides carry
// MCV lists; that lane is unported and panics. eqjoinsel_inner's else arm:
// (1-nullfrac1)*(1-nullfrac2) / max(nd1, nd2).
pub fn eqjoinsel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    _operator: u32,
    args: &[NodeId],
    jointype: types_pathnodes::JoinType,
    sjinfo: Option<&types_pathnodes::SpecialJoinInfo<'mcx>>,
) -> PgResult<f64> {
    assert!(args.len() == 2, "eqjoinsel (selfuncs.c): non-binary clause");
    let sj_jointype = sjinfo.map_or(jointype, |sj| sj.jointype);
    let left = *run.root.expr_node(args[0]);
    let right = *run.root.expr_node(args[1]);
    let vardata1 = examine_variable(run, args[0], left, 0)?;
    let vardata2 = examine_variable(run, args[1], right, 0)?;
    let (nd1, isdefault1) = get_variable_numdistinct(run, &vardata1);
    let (nd2, isdefault2) = get_variable_numdistinct(run, &vardata2);

    if vardata1.slot(STATISTIC_KIND_MCV, 0).is_some()
        && vardata2.slot(STATISTIC_KIND_MCV, 0).is_some()
    {
        panic!("eqjoinsel_inner (selfuncs.c): MCV-join lane");
    }
    let selec_inner =
        (1.0 - vardata1.nullfrac()) * (1.0 - vardata2.nullfrac()) / nd1.max(nd2);
    let selec = match sj_jointype {
        JOIN_INNER | types_pathnodes::JOIN_LEFT | types_pathnodes::JOIN_FULL => selec_inner,
        types_pathnodes::JOIN_SEMI | types_pathnodes::JOIN_ANTI => {
            let sjinfo = sjinfo.expect("SEMI/ANTI eqjoinsel has an sjinfo");
            let inner_rel = find_join_input_rel(run, &sjinfo.min_righthand);
            let inner_rows = run.root.rel(inner_rel).rows;
            // get_join_variables (selfuncs.c) reversal test.
            let rel_subset = |rel: Option<RelId>, side: &types_pathnodes::Relids<'mcx>| {
                rel.is_some_and(|r| {
                    crate::relnode::relids_is_subset(&run.root.rel(r).relids, side)
                })
            };
            let join_is_reversed = rel_subset(vardata1.rel, &sjinfo.syn_righthand)
                || rel_subset(vardata2.rel, &sjinfo.syn_lefthand);
            let semi = if !join_is_reversed {
                eqjoinsel_semi(run, &vardata1, &vardata2, nd1, nd2, isdefault1, isdefault2, inner_rel)
            } else {
                eqjoinsel_semi(run, &vardata2, &vardata1, nd2, nd1, isdefault2, isdefault1, inner_rel)
            };
            semi.min(inner_rows * selec_inner)
        }
        other => panic!("eqjoinsel (selfuncs.c): jointype {other}"),
    };
    Ok(clamp_probability(selec))
}

// eqjoinsel_semi (selfuncs.c), non-MCV arm (the MCV-x-MCV arm panics above).
#[allow(clippy::too_many_arguments)]
fn eqjoinsel_semi(
    run: &PlannerRun<'_>,
    vardata1: &VariableStatData<'_>,
    vardata2: &VariableStatData<'_>,
    _nd1: f64,
    _nd2: f64,
    isdefault1: bool,
    isdefault2: bool,
    inner_rel: RelId,
) -> f64 {
    let nd1 = _nd1;
    let mut nd2 = _nd2;
    let mut isdefault2 = isdefault2;
    if let Some(rel2) = vardata2.rel {
        let rows2 = run.root.rel(rel2).rows;
        if nd2 >= rows2 {
            nd2 = rows2;
            isdefault2 = false;
        }
    }
    let inner_rows = run.root.rel(inner_rel).rows;
    if nd2 >= inner_rows {
        nd2 = inner_rows;
        isdefault2 = false;
    }
    let nullfrac1 = vardata1.nullfrac();
    if !isdefault1 && !isdefault2 {
        if nd1 <= nd2 || nd2 < 0.0 {
            1.0 - nullfrac1
        } else {
            (nd2 / nd1) * (1.0 - nullfrac1)
        }
    } else {
        0.5 * (1.0 - nullfrac1)
    }
}

// find_join_input_rel (selfuncs.c).
fn find_join_input_rel<'mcx>(
    run: &PlannerRun<'mcx>,
    relids: &types_pathnodes::Relids<'mcx>,
) -> RelId {
    if let Some(relid) = crate::relnode::relids_singleton_member(relids) {
        return crate::relnode::find_base_rel(&run.root, relid);
    }
    for &jr in run.root.join_rel_list.iter() {
        if crate::relnode::relids_equal(&run.root.rel(jr).relids, relids) {
            return jr;
        }
    }
    panic!("could not find join input relation");
}

// estimate_hash_bucket_stats (selfuncs.c), no-stats/no-MCV lane. The MCV bucket
// adjustment (mcvfreq/avgfreq) is the extended-stats lane; without an MCV list
// mcvfreq is 0 and the bucketsize is 1/ndistinct clamped to virtualbuckets.
pub fn estimate_hash_bucket_stats<'mcx>(
    run: &mut PlannerRun<'mcx>,
    hashkey: Node<'mcx>,
    virtualbuckets: f64,
) -> PgResult<(f64, f64)> {
    let node_id = run.intern_expr(hashkey);
    let vardata = examine_variable(run, node_id, hashkey, 0)?;
    let (mut ndistinct, isdefault) = get_variable_numdistinct(run, &vardata);
    let mcvfreq = 0.0;
    if isdefault {
        return Ok((mcvfreq, 0.1f64.max(mcvfreq)));
    }
    // stanullfrac is 0 on the no-stats lane; scale ndistinct by the
    // restriction selectivity as C does.
    if let Some(rel) = vardata.rel {
        let (tuples, rows) = (run.root.rel(rel).tuples, run.root.rel(rel).rows);
        if tuples > 0.0 {
            ndistinct *= rows / tuples;
            ndistinct = crate::costsize::clamp_row_est(ndistinct);
        }
    }
    if ndistinct <= 0.0 {
        ndistinct = 1.0;
    }
    let mut estfract = if ndistinct > virtualbuckets {
        1.0 / virtualbuckets
    } else {
        1.0 / ndistinct
    };
    if estfract < 1.0e-6 {
        estfract = 1.0e-6;
    }
    Ok((mcvfreq, estfract))
}

// mergejoinscansel (selfuncs.c) -> (leftstart, leftend, rightstart, rightend).
// Every "insufficient info" leg (missing operators, no histogram/MCV range)
// lands on C's silent-fail defaults 0.0/1.0, which is also the no-stats arm.
pub fn mergejoinscansel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rinfo: RinfoId,
    opfamily: Oid,
    cmptype: i32,
    nulls_first: bool,
) -> PgResult<(f64, f64, f64, f64)> {
    use types_pathnodes::{COMPARE_GE, COMPARE_GT, COMPARE_LE, COMPARE_LT};

    let mut leftstart = 0.0f64;
    let mut leftend = 1.0f64;
    let mut rightstart = 0.0f64;
    let mut rightend = 1.0f64;
    let fail = Ok((0.0, 1.0, 0.0, 1.0));

    let clause = *run.root.expr_node(run.root.rinfo(rinfo).clause);
    let Some(o) = clause.as_op_expr().filter(|o| o.args.len() == 2) else {
        return fail;
    };
    let (opno, collation) = (o.opno, o.inputcollid);
    let (left, right) = (o.args.nth(0), o.args.nth(1));
    let lid = run.intern_expr(left);
    let rid = run.intern_expr(right);
    let leftvar = examine_variable(run, lid, left, 0)?;
    let rightvar = examine_variable(run, rid, right, 0)?;

    let (op_strategy, op_lefttype, op_righttype) =
        lsyscache::get_op_opfamily_properties(opno, opfamily, false)?;
    debug_assert!(op_strategy == types_pathnodes::COMPARE_EQ);

    let member = |lt: Oid, rt: Oid, cmp: i32| lsyscache::get_opfamily_member_for_cmptype(opfamily, lt, rt, cmp);

    let (isgt, lsortop, rsortop, lstatop, rstatop, ltop, leop, revltop, revleop);
    match cmptype {
        COMPARE_LT => {
            isgt = false;
            ltop = member(op_lefttype, op_righttype, COMPARE_LT)?;
            leop = member(op_lefttype, op_righttype, COMPARE_LE)?;
            if op_lefttype == op_righttype {
                lsortop = ltop;
                rsortop = ltop;
                lstatop = lsortop;
                rstatop = rsortop;
                revltop = ltop;
                revleop = leop;
            } else {
                lsortop = member(op_lefttype, op_lefttype, COMPARE_LT)?;
                rsortop = member(op_righttype, op_righttype, COMPARE_LT)?;
                lstatop = lsortop;
                rstatop = rsortop;
                revltop = member(op_righttype, op_lefttype, COMPARE_LT)?;
                revleop = member(op_righttype, op_lefttype, COMPARE_LE)?;
            }
        }
        COMPARE_GT => {
            isgt = true;
            ltop = member(op_lefttype, op_righttype, COMPARE_GT)?;
            leop = member(op_lefttype, op_righttype, COMPARE_GE)?;
            if op_lefttype == op_righttype {
                lsortop = ltop;
                rsortop = ltop;
                lstatop = member(op_lefttype, op_lefttype, COMPARE_LT)?;
                rstatop = lstatop;
                revltop = ltop;
                revleop = leop;
            } else {
                lsortop = member(op_lefttype, op_lefttype, COMPARE_GT)?;
                rsortop = member(op_righttype, op_righttype, COMPARE_GT)?;
                lstatop = member(op_lefttype, op_lefttype, COMPARE_LT)?;
                rstatop = member(op_righttype, op_righttype, COMPARE_LT)?;
                revltop = member(op_righttype, op_lefttype, COMPARE_GT)?;
                revleop = member(op_righttype, op_lefttype, COMPARE_GE)?;
            }
        }
        _ => return fail,
    }

    if lsortop == 0
        || rsortop == 0
        || lstatop == 0
        || rstatop == 0
        || ltop == 0
        || leop == 0
        || revltop == 0
        || revleop == 0
    {
        return fail;
    }

    let Some((mut leftmin, mut leftmax)) = get_variable_range(run, &leftvar, lstatop, collation)?
    else {
        return fail;
    };
    let Some((mut rightmin, mut rightmax)) =
        get_variable_range(run, &rightvar, rstatop, collation)?
    else {
        return fail;
    };
    if isgt {
        core::mem::swap(&mut leftmin, &mut leftmax);
        core::mem::swap(&mut rightmin, &mut rightmax);
    }

    let selec = scalarineqsel(run, leop, isgt, true, collation, &leftvar, rightmax, op_righttype)?;
    if selec != DEFAULT_INEQ_SEL {
        leftend = selec;
    }
    let selec = scalarineqsel(run, revleop, isgt, true, collation, &rightvar, leftmax, op_lefttype)?;
    if selec != DEFAULT_INEQ_SEL {
        rightend = selec;
    }
    if leftend > rightend {
        leftend = 1.0;
    } else if leftend < rightend {
        rightend = 1.0;
    } else {
        leftend = 1.0;
        rightend = 1.0;
    }

    let selec = scalarineqsel(run, ltop, isgt, false, collation, &leftvar, rightmin, op_righttype)?;
    if selec != DEFAULT_INEQ_SEL {
        leftstart = selec;
    }
    let selec =
        scalarineqsel(run, revltop, isgt, false, collation, &rightvar, leftmin, op_lefttype)?;
    if selec != DEFAULT_INEQ_SEL {
        rightstart = selec;
    }
    if leftstart < rightstart {
        leftstart = 0.0;
    } else if leftstart > rightstart {
        rightstart = 0.0;
    } else {
        leftstart = 0.0;
        rightstart = 0.0;
    }

    if nulls_first {
        if leftvar.stats.is_some() {
            let f = leftvar.nullfrac();
            leftstart = clamp_probability(leftstart + f);
            leftend = clamp_probability(leftend + f);
        }
        if rightvar.stats.is_some() {
            let f = rightvar.nullfrac();
            rightstart = clamp_probability(rightstart + f);
            rightend = clamp_probability(rightend + f);
        }
    }

    if leftstart >= leftend {
        leftstart = 0.0;
        leftend = 1.0;
    }
    if rightstart >= rightend {
        rightstart = 0.0;
        rightend = 1.0;
    }
    Ok((leftstart, leftend, rightstart, rightend))
}

// get_variable_range (selfuncs.c) -> Some((min, max)) or None. The C
// datumCopy is skipped: slot datums live in the planner arena already.
// statistic_proc_security_check reduces to true on this substrate.
fn get_variable_range<'mcx>(
    run: &PlannerRun<'mcx>,
    vardata: &VariableStatData<'mcx>,
    sortop: Oid,
    collation: Oid,
) -> PgResult<Option<(Datum, Datum)>> {
    let Some(stats) = &vardata.stats else {
        return Ok(None);
    };
    let _ = run;
    let opfuncoid = lsyscache::get_opcode(sortop)?;
    let mut opproc: Option<FmgrInfo> = None;
    let mut range: Option<(Datum, Datum)> = None;

    if let Some(sslot) = vardata.slot(STATISTIC_KIND_HISTOGRAM, sortop) {
        if sslot.stacoll == collation && !sslot.values()?.is_empty() {
            range = Some((sslot.values()?[0], sslot.values()?[sslot.values()?.len() - 1]));
        }
    }
    if range.is_none() {
        if let Some(sslot) = vardata.slot(STATISTIC_KIND_HISTOGRAM, 0) {
            get_stats_slot_range(sslot.values()?, opfuncoid, &mut opproc, collation, &mut range)?;
        }
    }
    if let Some(sslot) = vardata.slot(STATISTIC_KIND_MCV, 0) {
        let use_mcvs = if range.is_some() {
            true
        } else {
            let sumcommon: f64 = sslot.numbers()?.iter().map(|&n| n as f64).sum();
            sumcommon + stats.stanullfrac as f64 > 0.99999
        };
        if use_mcvs {
            get_stats_slot_range(sslot.values()?, opfuncoid, &mut opproc, collation, &mut range)?;
        }
    }
    Ok(range)
}

fn get_stats_slot_range(
    values: &[Datum],
    opfuncoid: Oid,
    opproc: &mut Option<FmgrInfo>,
    collation: Oid,
    range: &mut Option<(Datum, Datum)>,
) -> PgResult<()> {
    if values.is_empty() {
        return Ok(());
    }
    if opproc.is_none() {
        *opproc = Some(fmgr_core::fmgr_info(opfuncoid)?);
    }
    let opproc = opproc.as_mut().unwrap();
    for &v in values {
        match range {
            None => *range = Some((v, v)),
            Some((tmin, tmax)) => {
                if types_fmgr::function_call2_coll(opproc, collation, v, *tmin)?.as_bool() {
                    *tmin = v;
                }
                if types_fmgr::function_call2_coll(opproc, collation, *tmax, v)?.as_bool() {
                    *tmax = v;
                }
            }
        }
    }
    Ok(())
}

fn strip_array_coercion<'mcx>(mut node: Node<'mcx>) -> Node<'mcx> {
    while let Some(r) = node.as_relabel_type() {
        node = r.arg;
    }
    node
}

fn expr_collation(node: Node<'_>) -> Oid {
    match node.node_tag() {
        NodeTag::T_Const => node.as_const().unwrap().constcollid,
        NodeTag::T_ArrayExpr => node.as_array_expr().unwrap().array_collid,
        NodeTag::T_Var => node.as_var().unwrap().varcollid,
        _ => 0,
    }
}

// scalararraysel (selfuncs.c). The typcache eq_opr probe only gates
// scalararraysel_containment, whose live precondition (an array-typed
// variable operand) is the loud arm below; the isEquality/isInequality
// flags key off the estimator oid exactly as C's second-chance test does.
pub fn scalararraysel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    node: Node<'mcx>,
    is_join_clause: bool,
    varrelid: i32,
    jointype: types_pathnodes::JoinType,
    sjinfo: Option<&types_pathnodes::SpecialJoinInfo<'mcx>>,
) -> PgResult<f64> {
    const F_EQSEL: Oid = 101;
    const F_NEQSEL: Oid = 102;
    const F_EQJOINSEL: Oid = 105;
    const F_NEQJOINSEL: Oid = 106;

    let clause = node.as_scalar_array_op_expr().expect("ScalarArrayOpExpr");
    let operator = clause.opno;
    let use_or = clause.useOr;
    debug_assert!(clause.args.len() == 2);
    let leftop = clause.args.nth(0);
    let rightop = clause.args.nth(1);

    let (rightop_type, _) = crate::costsize::expr_type_typmod(rightop);
    let nominal_element_type = lsyscache::get_element_type(rightop_type)?;
    if nominal_element_type == 0 {
        return Ok(0.5);
    }
    let nominal_element_collation = expr_collation(rightop);
    let rightop = strip_array_coercion(rightop);

    if rightop.node_tag() == NodeTag::T_Var {
        panic!("scalararraysel_containment (array_selfuncs.c): array-column operand; M2 lane");
    }

    let oprsel = if is_join_clause {
        lsyscache::get_oprjoin(operator)?
    } else {
        lsyscache::get_oprrest(operator)?
    };
    if oprsel == 0 {
        return Ok(0.5);
    }
    let is_equality = oprsel == F_EQSEL || oprsel == F_EQJOINSEL;
    let is_inequality = oprsel == F_NEQSEL || oprsel == F_NEQJOINSEL;

    let left_id = run.intern_expr(leftop);
    let mut elem_sel = |run: &mut PlannerRun<'mcx>,
                        value: Datum,
                        isnull: bool,
                        elmlen: i16,
                        elmbyval: bool|
     -> PgResult<f64> {
        let elem = Node::mk(
            run.mcx,
            types_nodes::primnodes::Const {
                consttype: nominal_element_type,
                consttypmod: -1,
                constcollid: nominal_element_collation,
                constlen: elmlen as i32,
                constvalue: value,
                constisnull: isnull,
                constbyval: elmbyval,
                location: -1,
            },
        )?;
        let elem_id = run.intern_expr(elem);
        let args = [left_id, elem_id];
        if is_join_clause {
            crate::plancat::join_selectivity(
                run,
                operator,
                &args,
                clause.inputcollid,
                jointype,
                sjinfo,
            )
        } else {
            crate::plancat::restriction_selectivity(
                run,
                operator,
                &args,
                clause.inputcollid,
                varrelid,
            )
        }
    };

    let mut s1;
    let mut s1disjoint;
    if let Some(c) = rightop.as_const() {
        if c.constisnull {
            return Ok(0.0);
        }
        let p = c.constvalue.as_usize() as *const u8;
        // SAFETY: non-null array datum; planner consts carry inline 4-byte
        // headers.
        let b0 = unsafe { *p };
        assert!(b0 != 0x01 && b0 & 0x03 == 0, "scalararraysel: toasted/packed array const");
        // SAFETY: 4-byte varlena header verified; image is VARSIZE bytes.
        let img = unsafe {
            core::slice::from_raw_parts(
                p,
                arrayfuncs::arr_size(core::slice::from_raw_parts(p, 4)),
            )
        };
        let elemtype = arrayfuncs::arr_elemtype(img);
        let (elmlen, elmbyval, elmalign) = lsyscache::get_typlenbyvalalign(elemtype)?;
        let (values, nulls) = arrayfuncs::deconstruct_array(
            run.mcx, img, elmlen as i32, elmbyval, elmalign as u8, true,
        )?;

        s1 = if use_or { 0.0 } else { 1.0 };
        s1disjoint = s1;
        for (i, &v) in values.iter().enumerate() {
            let s2 = elem_sel(run, v, nulls[i], elmlen, elmbyval)?;
            if use_or {
                s1 = s1 + s2 - s1 * s2;
                if is_equality {
                    s1disjoint += s2;
                }
            } else {
                s1 *= s2;
                if is_inequality {
                    s1disjoint += s2 - 1.0;
                }
            }
        }
        if (if use_or { is_equality } else { is_inequality })
            && (0.0..=1.0).contains(&s1disjoint)
        {
            s1 = s1disjoint;
        }
    } else if let Some(arrayexpr) =
        rightop.as_array_expr().filter(|a| !a.multidims)
    {
        s1 = if use_or { 0.0 } else { 1.0 };
        s1disjoint = s1;
        for elem in arrayexpr.elements.iter() {
            let elem_id = run.intern_expr(elem);
            let args = [left_id, elem_id];
            let s2 = if is_join_clause {
                crate::plancat::join_selectivity(
                    run,
                    operator,
                    &args,
                    clause.inputcollid,
                    jointype,
                    sjinfo,
                )?
            } else {
                crate::plancat::restriction_selectivity(
                    run,
                    operator,
                    &args,
                    clause.inputcollid,
                    varrelid,
                )?
            };
            if use_or {
                s1 = s1 + s2 - s1 * s2;
                if is_equality {
                    s1disjoint += s2;
                }
            } else {
                s1 *= s2;
                if is_inequality {
                    s1disjoint += s2 - 1.0;
                }
            }
        }
        if (if use_or { is_equality } else { is_inequality })
            && (0.0..=1.0).contains(&s1disjoint)
        {
            s1 = s1disjoint;
        }
    } else {
        // C estimates a dummy CaseTestExpr comparison over 10 elements.
        panic!("scalararraysel (selfuncs.c): non-constant array operand; M2 lane");
    }

    Ok(clamp_probability(s1))
}

// estimate_array_length (selfuncs.c); the pg_statistic DECHIST leg (array
// variables) is unreachable while scalararraysel's array-column arm is loud.
pub fn estimate_array_length(node: Node<'_>) -> f64 {
    let node = strip_array_coercion(node);
    if let Some(c) = node.as_const() {
        if c.constisnull {
            return 0.0;
        }
        let p = c.constvalue.as_usize() as *const u8;
        // SAFETY: non-null inline-header array datum (as scalararraysel).
        let b0 = unsafe { *p };
        assert!(b0 != 0x01 && b0 & 0x03 == 0, "estimate_array_length: toasted array const");
        // SAFETY: 4-byte varlena header verified.
        let img = unsafe {
            core::slice::from_raw_parts(
                p,
                arrayfuncs::arr_size(core::slice::from_raw_parts(p, 4)),
            )
        };
        let ndim = arrayfuncs::arr_ndim(img);
        let mut n = 1f64;
        for i in 0..ndim as usize {
            n *= arrayfuncs::arr_dim(img, i) as f64;
        }
        if ndim == 0 {
            n = 0.0;
        }
        return n;
    }
    if let Some(a) = node.as_array_expr().filter(|a| !a.multidims) {
        return a.elements.len() as f64;
    }
    10.0
}

// generic_restriction_selectivity (selfuncs.c).
fn generic_restriction_selectivity<'mcx>(
    run: &mut PlannerRun<'mcx>,
    oproid: Oid,
    collation: Oid,
    args: &[NodeId],
    varrelid: i32,
    default_selectivity: f64,
) -> PgResult<f64> {
    let Some((vardata, other, varonleft)) = get_restriction_variable(run, args, varrelid)? else {
        return Ok(default_selectivity);
    };

    let mut selec;
    if let Some(c) = other.as_const() {
        if c.constisnull {
            return Ok(0.0);
        }
        let constval = c.constvalue;
        let opcode = lsyscache::get_opcode(oproid)?;
        let mut opproc = fmgr_core::fmgr_info(opcode)?;
        // Matching operators (jsonb @> …) detoast/allocate: arm the frames
        // with a bump scratch (C leaks into the planner context).
        let scratch = ::mcx::MemoryContext::new_bump("generic_restriction_selectivity");
        let smcx = scratch.mcx();
        let armed_test = |opproc: &mut FmgrInfo, v: Datum| -> PgResult<bool> {
            let (a0, a1) = if varonleft { (v, constval) } else { (constval, v) };
            Ok(types_fmgr::function_call2_coll_in(opproc, collation, smcx, a0, a1)?.as_bool())
        };

        let (mut mcvsel, mut mcvsum) = (0.0f64, 0.0f64);
        if let Some(sslot) = vardata.slot(STATISTIC_KIND_MCV, 0) {
            for (i, &v) in sslot.values()?.iter().enumerate() {
                if armed_test(&mut opproc, v)? {
                    mcvsel += sslot.numbers()?[i] as f64;
                }
                mcvsum += sslot.numbers()?[i] as f64;
            }
        }

        let (hist_selec, hist_size) = {
            let mut hs = -1.0f64;
            let mut n = 0usize;
            if let Some(sslot) = vardata.slot(STATISTIC_KIND_HISTOGRAM, 0) {
                let values = sslot.values()?;
                n = values.len();
                if n >= 10 {
                    let mut nmatch = 0usize;
                    for &v in &values[1..n - 1] {
                        if armed_test(&mut opproc, v)? {
                            nmatch += 1;
                        }
                    }
                    hs = nmatch as f64 / (n - 2) as f64;
                }
            }
            (hs, n)
        };
        selec = if hist_selec < 0.0 {
            default_selectivity
        } else if hist_size < 100 {
            let hist_weight = hist_size as f64 / 100.0;
            hist_selec * hist_weight + default_selectivity * (1.0 - hist_weight)
        } else {
            hist_selec
        };

        selec = selec.clamp(0.0001, 0.9999);

        let nullfrac = vardata.nullfrac();
        selec *= 1.0 - nullfrac - mcvsum;
        selec += mcvsel;
    } else {
        selec = default_selectivity;
    }

    Ok(clamp_probability(selec))
}

// matchingsel (selfuncs.c). DEFAULT_MATCHING_SEL = 2 * DEFAULT_EQ_SEL.
pub fn matchingsel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    operator: Oid,
    args: &[NodeId],
    varrelid: i32,
    collation: Oid,
) -> PgResult<f64> {
    const DEFAULT_MATCHING_SEL: f64 = 0.010;
    generic_restriction_selectivity(run, operator, collation, args, varrelid, DEFAULT_MATCHING_SEL)
}

#[derive(Default)]
struct GinQualCounts {
    att_has_full_scan: bool,
    att_has_normal_scan: bool,
    partial_entries: f64,
    exact_entries: f64,
    search_entries: f64,
    array_scans: f64,
}

// gincost_pattern (selfuncs.c), single key column.
fn gincost_pattern(
    opfamily: Oid,
    opcintype: Oid,
    clause_op: Oid,
    query: Datum,
    counts: &mut GinQualCounts,
) -> PgResult<bool> {
    const GIN_SEARCH_MODE_DEFAULT: i32 = 0;
    const GIN_SEARCH_MODE_INCLUDE_EMPTY: i32 = 1;
    let _strategy = lsyscache::amop::get_op_opfamily_strategy(clause_op, opfamily)?;
    let strategy = _strategy as u16;

    let (nentries, search_mode) =
        gin::gincost_extract_query(opfamily, opcintype, query, strategy)?;

    if nentries <= 0 && search_mode == GIN_SEARCH_MODE_DEFAULT {
        return Ok(false);
    }
    // The closed opclass set has no partial matches.
    counts.exact_entries += nentries as f64;
    counts.search_entries += nentries as f64;

    if search_mode == GIN_SEARCH_MODE_DEFAULT {
        counts.att_has_normal_scan = true;
    } else if search_mode == GIN_SEARCH_MODE_INCLUDE_EMPTY {
        counts.att_has_normal_scan = true;
        counts.exact_entries += 1.0;
        counts.search_entries += 1.0;
    } else {
        counts.att_has_full_scan = true;
    }
    Ok(true)
}

// gincostestimate (selfuncs.c).
fn gincostestimate(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    loop_count: f64,
) -> PgResult<AmCostEstimate> {
    let (index_quals, index_pages, index_tuples, index_rel, reltablespace, gin_stats, opfamily0, opcintype0) = {
        let PathNode::IndexPath(ip) = run.root.path(path_id) else { unreachable!() };
        let index = ip.indexinfo.as_ref().expect("indexinfo set");
        debug_assert!(index.indpred.is_empty());
        (
            get_quals_from_indexclauses(run, path_id),
            index.pages,
            index.tuples,
            index.rel.expect("index rel set"),
            index.reltablespace,
            index.gin_stats.expect("gin stats captured at plancat"),
            index.opfamily[0],
            index.opcintype[0],
        )
    };
    let index_rel_relid = run.root.rel(index_rel).relid as i32;

    let mut num_pages = index_pages as f64;
    let num_tuples = index_tuples;

    let num_pending_pages = if (gin_stats.pending_pages as f64) < num_pages {
        gin_stats.pending_pages as f64
    } else {
        0.0
    };

    let num_entry_pages;
    let num_data_pages;
    let mut num_entries;
    if num_pages > 0.0
        && (gin_stats.total_pages as f64) <= num_pages
        && (gin_stats.total_pages as f64) > num_pages / 4.0
        && gin_stats.entry_pages > 0
        && gin_stats.entries > 0
    {
        let scale = num_pages / gin_stats.total_pages as f64;
        let mut ep = (gin_stats.entry_pages as f64 * scale).ceil();
        let mut dp = (gin_stats.data_pages as f64 * scale).ceil();
        num_entries = (gin_stats.entries as f64 * scale).ceil();
        ep = ep.min(num_pages - num_pending_pages);
        dp = dp.min(num_pages - num_pending_pages - ep);
        num_entry_pages = ep;
        num_data_pages = dp;
    } else {
        num_pages = num_pages.max(10.0);
        num_entry_pages = ((num_pages - num_pending_pages) * 0.90).floor();
        num_data_pages = num_pages - num_pending_pages - num_entry_pages;
        num_entries = (num_entry_pages * 100.0).floor();
    }
    if num_entries < 1.0 {
        num_entries = 1.0;
    }

    // add_predicate_to_index_quals: identity for a non-partial index.
    let index_selectivity = crate::clausesel::clauselist_selectivity(
        run,
        &index_quals,
        index_rel_relid,
        JOIN_INNER,
        None,
    )?;

    let (spc_random_page_cost, _) = crate::costsize::get_tablespace_page_costs(reltablespace);

    // Examine quals: search-entry and partial-match counts.
    let mut counts = GinQualCounts {
        array_scans: 1.0,
        ..Default::default()
    };
    let mut match_possible = true;
    'quals: {
        let iclauses = {
            let PathNode::IndexPath(ip) = run.root.path(path_id) else { unreachable!() };
            ip.indexclauses.clone()
        };
        for ic in iclauses.iter() {
            for &rid in ic.indexquals.iter() {
                let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
                match clause.node_tag() {
                    NodeTag::T_OpExpr => {
                        // gincost_opexpr: fixed indexquals put the indexkey on
                        // the left; the operand is args[1].
                        let op = clause.as_op_expr().unwrap();
                        let operand = op.args.nth(1);
                        match operand.as_const() {
                            None => {
                                counts.exact_entries += 1.0;
                                counts.search_entries += 1.0;
                            }
                            Some(c) if c.constisnull => {
                                match_possible = false;
                                break 'quals;
                            }
                            Some(c) => {
                                if !gincost_pattern(
                                    opfamily0,
                                    opcintype0,
                                    op.opno,
                                    c.constvalue,
                                    &mut counts,
                                )? {
                                    match_possible = false;
                                    break 'quals;
                                }
                            }
                        }
                    }
                    NodeTag::T_ScalarArrayOpExpr => {
                        panic!("gincostestimate: ScalarArrayOpExpr GIN qual; arrays lane")
                    }
                    other => panic!("unsupported GIN indexqual type: {other:?}"),
                }
            }
        }
    }

    if !match_possible {
        return Ok(AmCostEstimate {
            index_startup_cost: 0.0,
            index_total_cost: 0.0,
            index_selectivity: 0.0,
            index_correlation: 0.0,
            index_pages: 0.0,
        });
    }

    let full_index_scan = counts.att_has_full_scan && !counts.att_has_normal_scan;
    if full_index_scan || index_quals.is_empty() {
        counts.partial_entries = 0.0;
        counts.exact_entries = num_entries;
        counts.search_entries = num_entries;
    }

    let outer_scans = loop_count;
    let cpu_operator_cost = gucs::cpu_operator_cost();

    let mut entry_pages_fetched = num_pending_pages;
    // C: ceil(searchEntries * rint(pow(numEntryPages, 0.15))).
    entry_pages_fetched +=
        (counts.search_entries * num_entry_pages.powf(0.15).round_ties_even()).ceil();

    let partial_scale = (counts.partial_entries / num_entries).min(1.0);
    entry_pages_fetched += (num_entry_pages * partial_scale).ceil();

    let mut data_pages_fetched = (num_data_pages * partial_scale).ceil();

    let mut index_startup_cost = 0.0;
    let mut index_total_cost = 0.0;

    if num_entries > 1.0 {
        let descent_cost = (num_entries.ln() / 2f64.ln()).ceil() * cpu_operator_cost;
        index_startup_cost += descent_cost * counts.search_entries;
        index_total_cost += counts.array_scans * descent_cost * counts.search_entries;
    }

    index_startup_cost += entry_pages_fetched * DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost;
    index_total_cost +=
        entry_pages_fetched * counts.array_scans * DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost;

    index_startup_cost += DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost * data_pages_fetched;
    index_total_cost += data_pages_fetched
        * (counts.array_scans - 1.0)
        * DEFAULT_PAGE_CPU_MULTIPLIER
        * cpu_operator_cost;

    if outer_scans > 1.0 || counts.array_scans > 1.0 {
        entry_pages_fetched *= outer_scans * counts.array_scans;
        entry_pages_fetched = crate::costsize::index_pages_fetched(
            run,
            entry_pages_fetched,
            num_entry_pages as u32,
            num_entry_pages,
        );
        entry_pages_fetched /= outer_scans;
        data_pages_fetched *= outer_scans * counts.array_scans;
        data_pages_fetched = crate::costsize::index_pages_fetched(
            run,
            data_pages_fetched,
            num_data_pages as u32,
            num_data_pages,
        );
        data_pages_fetched /= outer_scans;
    }

    index_startup_cost += (entry_pages_fetched + data_pages_fetched) * spc_random_page_cost;

    let mut data_pages_fetched =
        (num_data_pages * counts.exact_entries / num_entries).ceil();
    let data_pages_fetched_by_sel =
        (index_selectivity * (num_tuples / (8192.0 / 3.0))).ceil();
    if data_pages_fetched_by_sel > data_pages_fetched {
        data_pages_fetched = data_pages_fetched_by_sel;
    }

    index_startup_cost += DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost * counts.search_entries;
    index_total_cost +=
        data_pages_fetched * counts.array_scans * DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost;

    if outer_scans > 1.0 || counts.array_scans > 1.0 {
        data_pages_fetched *= outer_scans * counts.array_scans;
        data_pages_fetched = crate::costsize::index_pages_fetched(
            run,
            data_pages_fetched,
            num_data_pages as u32,
            num_data_pages,
        );
        data_pages_fetched /= outer_scans;
    }

    index_total_cost += index_startup_cost + data_pages_fetched * spc_random_page_cost;

    let qual_arg_cost = index_other_operands_eval_cost(run, &index_quals)?;
    let qual_op_cost = cpu_operator_cost * index_quals.len() as f64;

    index_startup_cost += qual_arg_cost;
    index_total_cost += qual_arg_cost;
    index_total_cost += counts.search_entries * counts.array_scans * qual_op_cost;
    index_total_cost += num_tuples * index_selectivity * gucs::cpu_index_tuple_cost();

    Ok(AmCostEstimate {
        index_startup_cost,
        index_total_cost,
        index_selectivity,
        index_correlation: 0.0,
        index_pages: data_pages_fetched,
    })
}
