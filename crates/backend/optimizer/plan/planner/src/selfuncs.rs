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
pub use types_pathnodes::DEFAULT_INEQ_SEL;
pub use types_pathnodes::DEFAULT_NUM_DISTINCT;
const DEFAULT_PAGE_CPU_MULTIPLIER: f64 = 50.0;
const BOOLOID: u32 = 16;
const SELF_ITEM_POINTER_ATTRIBUTE_NUMBER: i16 = -1;
const TABLE_OID_ATTRIBUTE_NUMBER: i16 = -6;

pub const STATISTIC_KIND_MCV: i16 = 1;
pub const STATISTIC_KIND_HISTOGRAM: i16 = 2;
pub const STATISTIC_KIND_DECHIST: i16 = 5;
pub const STATISTIC_KIND_CORRELATION: i16 = 3;

// ---------------------------------------------------------------------------
// Per-planning-cycle attribute-stats memo (replanfix2 T1). The callgrind map
// of one custom-plan replan showed the pg_statistic fetch+decode pipeline at
// ~23% of the bind path: the bundle was rebuilt ~9x per cycle (each rebuild
// re-searching the syscache and re-copying/re-deconstructing slot arrays on
// first touch). C pins the syscache tuple per examine_variable and points
// into it; the memo gets the same effect one altitude up — ONE arena-leaked
// bundle per (relid, attnum, inh) per planning cycle, shared by reference,
// so the OnceCell slot decodes also happen once per cycle. COST ONLY: the
// same syscache row feeds every consumer; the only divergence class is a
// concurrent mid-cycle pg_statistic update C's re-searches could observe
// (estimates-only, nondeterministic in C, never exercised by regress).
// Storage lives in PlannerRun.att_stats_memo (opaque there; this module owns
// both sides of the cast). Kill switch: PGRUST_PLANNER_STATS_MEMO=0 restores
// the per-call fetch (bundles still arena-leaked — VariableStatData holds
// references either way).
// ---------------------------------------------------------------------------

fn stats_memo_disabled() -> bool {
    static DISABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *DISABLED.get_or_init(|| {
        matches!(std::env::var("PGRUST_PLANNER_STATS_MEMO").as_deref(), Ok("0") | Ok("off"))
    })
}

pub(crate) fn leak_bundle<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    bundle: PgStatisticBundle<'mcx>,
) -> PgResult<&'mcx PgStatisticBundle<'mcx>> {
    // forget_box_in (ForgetSafe census on the bundle types): drop glue never
    // runs, the plan arena's reset reclaims the bytes — the same discipline
    // as the run itself (ArenaForget in standard_planner).
    Ok(mcx::forget_box_in(mcx, bundle)?)
}

pub(crate) fn get_att_stats<'mcx>(
    run: &PlannerRun<'mcx>,
    relid: Oid,
    attnum: i16,
    inh: bool,
) -> PgResult<Option<&'mcx PgStatisticBundle<'mcx>>> {
    if stats_memo_disabled() {
        return match syscache_seams::lookup_pg_statistic_bundle::call(
            run.mcx, relid, attnum, inh,
        )? {
            Some(b) => Ok(Some(leak_bundle(run.mcx, b)?)),
            None => Ok(None),
        };
    }
    let key = (relid, attnum, inh);
    let hit = run
        .att_stats_memo
        .borrow()
        .iter()
        .find_map(|(k, v)| (*k == key).then_some(*v));
    if let Some(v) = hit {
        // SAFETY: the pointer was created below from &'mcx PgStatisticBundle
        // <'mcx> leaked into run.mcx (this function is the only writer); the
        // arena outlives the run, and planning is single-threaded.
        return Ok(v.map(|p| unsafe { p.cast::<PgStatisticBundle<'mcx>>().as_ref() }));
    }
    let fetched = match syscache_seams::lookup_pg_statistic_bundle::call(
        run.mcx, relid, attnum, inh,
    )? {
        Some(b) => Some(&*leak_bundle(run.mcx, b)?),
        None => None,
    };
    run.att_stats_memo
        .borrow_mut()
        .push((key, fetched.map(|r| core::ptr::NonNull::from(r).cast())));
    Ok(fetched)
}

pub(crate) fn clamp_probability(p: f64) -> f64 {
    p.clamp(0.0, 1.0)
}

// VariableStatData (selfuncs.h); `stats` is the decoded statsTuple, shared
// by reference from the per-cycle memo (or arena-leaked one-offs) so slot
// decodes happen once per planning cycle.
pub struct VariableStatData<'mcx> {
    pub var: Option<NodeId>,
    pub rel: Option<RelId>,
    pub vartype: u32,
    pub isunique: bool,
    pub stats: Option<&'mcx PgStatisticBundle<'mcx>>,
    pub acl_ok: bool,
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

/// opproc_for through the per-cycle operator-shape memo (syscache-memo
/// lane) for the replan-hot selectivity paths that have the run at hand.
pub(crate) fn opproc_for_run(run: &PlannerRun<'_>, operator: Oid) -> PgResult<FmgrInfo> {
    let opcode = crate::syscache_memo::get_opcode(run, operator)?;
    fmgr_core::fmgr_info(opcode)
}

// Armed frame: comparison procs detoast short/packed args into `mcx`
// (C's DatumGetNumeric detoast lands in the planner context).
pub(crate) fn op_test(
    mcx: mcx::Mcx<'_>,
    opproc: &mut FmgrInfo,
    collation: Oid,
    slot_value: Datum,
    constval: Datum,
    varonleft: bool,
) -> PgResult<bool> {
    let (a0, a1) = if varonleft { (slot_value, constval) } else { (constval, slot_value) };
    Ok(types_fmgr::function_call2_coll_in(opproc, collation, mcx, a0, a1)?.as_bool())
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
        operator = crate::syscache_memo::get_commutator(run, operator)?;
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

// scalarineqsel (selfuncs.c).
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
        let is_ctid = vardata
            .var
            .and_then(|id| run.root.expr_node(id).as_var())
            .is_some_and(|v| v.varattno == SELF_ITEM_POINTER_ATTRIBUTE_NUMBER);
        if is_ctid {
            let rel = vardata.rel.expect("ctid Var has a rel");
            let pages = run.root.rel(rel).pages as f64;
            let tuples = run.root.rel(rel).tuples;
            if pages == 0.0 {
                return Ok(1.0);
            }
            // SAFETY: non-null tid datum points at an ItemPointerData.
            let itemptr =
                unsafe { *(constval.as_usize() as *const types_tuple::itemptr::ItemPointerData) };
            let mut block =
                types_tuple::itemptr::ItemPointerGetBlockNumberNoCheck(&itemptr) as f64;
            // The last page averages half full: half density there, half a
            // page's weight in the fractions below.
            let mut density = tuples / (pages - 0.5);
            if block >= pages - 1.0 {
                density *= 0.5;
            }
            if density > 0.0 {
                let offset =
                    types_tuple::itemptr::ItemPointerGetOffsetNumberNoCheck(&itemptr) as f64;
                block += (offset / density).min(1.0);
            }
            let mut selec = block / (pages - 0.5);
            // "<=" so far; one fewer tuple for "<" and ">=" (iseq == isgt).
            if iseq == isgt && tuples >= 1.0 {
                selec -= 1.0 / tuples;
            }
            if isgt {
                selec = 1.0 - selec;
            }
            return Ok(clamp_probability(selec));
        }
        return Ok(DEFAULT_INEQ_SEL);
    }
    let stanullfrac = vardata.nullfrac();
    let mut opproc = opproc_for_run(run, operator)?;

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
    if vardata.stats.is_some() && statistic_proc_security_check(vardata, opproc.fn_oid)? {
        if let Some(sslot) = vardata.slot(STATISTIC_KIND_MCV, 0) {
            // A torn slot (bundle-time kind, arrays re-probed from a
            // rewritten pg_statistic row) can pair values with a shorter or
            // empty numbers array; only the paired prefix carries MCV
            // entries. On any well-formed slot the lengths agree and this is
            // exactly C's nvalues-bounded loop (C reads a pinned tuple copy
            // and can never see the tear).
            for (&v, &n) in sslot.values()?.iter().zip(sslot.numbers()?.iter()) {
                if op_test(run.mcx, opproc, collation, v, constval, varonleft)? {
                    mcv_selec += n as f64;
                }
                sumcommon += n as f64;
            }
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
// must outlive the scan; index_form_tuple packs, so the -1 arm is C's
// VARSIZE_ANY (short 1B headers and inline-compressed images included).
pub(crate) fn endpoint_datum_copy<'mcx>(
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
            // SAFETY: non-null by-ref varlena datum, readable for its
            // header-declared (VARSIZE_ANY) size.
            unsafe {
                let b0 = *p;
                if b0 == 0x01 {
                    2 + match *p.add(1) {
                        18 => 16,
                        1 => 8,
                        2 | 3 => panic!(
                            "endpoint_datum_copy: expanded-object flatten (EOH_flatten_into) unported"
                        ),
                        tag => panic!("endpoint_datum_copy: unknown vartag {tag}"),
                    }
                } else if b0 & 0x01 != 0 {
                    (b0 as usize >> 1) & 0x7F
                } else {
                    datum::VarlenaRef::from_ptr(p).varsize()
                }
            }
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
    mcx: mcx::Mcx<'_>,
    vardata: &VariableStatData<'mcx>,
    opproc: &mut FmgrInfo,
    collation: Oid,
    constval: Datum,
    varonleft: bool,
    min_hist_size: usize,
    n_skip: usize,
) -> PgResult<(f64, usize)> {
    debug_assert!(min_hist_size > 2 * n_skip);
    if vardata.stats.is_none() || !statistic_proc_security_check(vardata, opproc.fn_oid)? {
        return Ok((-1.0, 0));
    }
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
        if op_test(mcx, opproc, collation, v, constval, varonleft)? {
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
    if vardata.stats.is_none() || !statistic_proc_security_check(vardata, opproc.fn_oid)? {
        return Ok(hist_selec);
    }
    let Some(sslot) = vardata.slot(STATISTIC_KIND_HISTOGRAM, 0) else {
        return Ok(hist_selec);
    };
    let nvalues = sslot.values()?.len() as i32;
    if nvalues > 1
        && sslot.stacoll == collation
        && crate::syscache_memo::comparison_ops_are_compatible(run, sslot.staop, opoid)?
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
            let mut ltcmp = op_test(run.mcx, opproc, collation, probe_val, constval, true)?;
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
            if op_test(run.mcx, opproc, collation, v, constval, true)? {
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
    const INETOID: Oid = 869;
    const CIDROID: Oid = 650;
    match valuetypid {
        CHAROID | BPCHAROID | VARCHAROID | TEXTOID | NAMEOID => {
            let val = convert_string_datum(mcx, value, valuetypid, collid)?;
            let lostr = convert_string_datum(mcx, lobound, boundstypid, collid)?;
            let histr = convert_string_datum(mcx, hibound, boundstypid, collid)?;
            Some(convert_string_to_scalar(val, lostr, histr))
        }
        BYTEAOID => {
            if boundstypid != BYTEAOID {
                return None;
            }
            Some(convert_bytea_to_scalar(value, lobound, hibound))
        }
        INETOID | CIDROID => {
            let v = convert_network_to_scalar(value, valuetypid)?;
            let lo = convert_network_to_scalar(lobound, boundstypid)?;
            let hi = convert_network_to_scalar(hibound, boundstypid)?;
            Some((v, lo, hi))
        }
        TIMESTAMPOID | TIMESTAMPTZOID | DATEOID | INTERVALOID | TIMEOID | TIMETZOID => {
            let v = convert_timevalue_to_scalar(value, valuetypid)?;
            let lo = convert_timevalue_to_scalar(lobound, boundstypid)?;
            let hi = convert_timevalue_to_scalar(hibound, boundstypid)?;
            Some((v, lo, hi))
        }
        _ => {
            let v = convert_numeric_to_scalar(value, valuetypid)?;
            let lo = convert_numeric_to_scalar(lobound, boundstypid)?;
            let hi = convert_numeric_to_scalar(hibound, boundstypid)?;
            Some((v, lo, hi))
        }
    }
}

const TIMESTAMPOID: Oid = 1114;
const TIMESTAMPTZOID: Oid = 1184;
const DATEOID: Oid = 1082;
const INTERVALOID: Oid = 1186;
const TIMEOID: Oid = 1083;
const TIMETZOID: Oid = 1266;

// convert_timevalue_to_scalar (selfuncs.c).
fn convert_timevalue_to_scalar(value: Datum, typid: Oid) -> Option<f64> {
    const USECS_PER_DAY: f64 = 86_400_000_000.0;
    match typid {
        TIMESTAMPOID | TIMESTAMPTZOID | TIMEOID => Some(value.as_i64() as f64),
        DATEOID => {
            let d = value.as_i32();
            Some(if d == i32::MIN {
                -f64::MAX
            } else if d == i32::MAX {
                f64::MAX
            } else {
                d as f64 * USECS_PER_DAY
            })
        }
        INTERVALOID => {
            let p = value.as_usize() as *const u8;
            // SAFETY: by-ref 16-byte interval datum {time i64, day i32, month i32}.
            let (time, day, month) = unsafe {
                (
                    p.cast::<i64>().read_unaligned(),
                    p.add(8).cast::<i32>().read_unaligned(),
                    p.add(12).cast::<i32>().read_unaligned(),
                )
            };
            Some(time as f64 + day as f64 * USECS_PER_DAY
                + month as f64 * ((365.25 / 12.0) * USECS_PER_DAY))
        }
        TIMETZOID => {
            let p = value.as_usize() as *const u8;
            // SAFETY: by-ref 12-byte timetz datum {time i64, zone i32}.
            let (time, zone) = unsafe {
                (p.cast::<i64>().read_unaligned(), p.add(8).cast::<i32>().read_unaligned())
            };
            Some(time as f64 + zone as f64 * 1_000_000.0)
        }
        _ => None,
    }
}

// convert_network_to_scalar (selfuncs.c), inet/cidr arm (mac arms deferred).
fn convert_network_to_scalar(value: Datum, typid: Oid) -> Option<f64> {
    const INETOID: Oid = 869;
    const CIDROID: Oid = 650;
    if typid != INETOID && typid != CIDROID {
        return None;
    }
    let ip = crate::network_selfuncs::inet_ref(value);
    let len = if ip.family == adt_network::PGSQL_AF_INET { 4 } else { 16 };
    let mut res = ip.family as f64;
    for i in 0..len {
        res *= 256.0;
        res += ip.addr[i] as f64;
    }
    Some(res)
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
        BPCHAROID | VARCHAROID | TEXTOID => varlena_datum_payload(value),
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

// convert_bytea_to_scalar (selfuncs.c); range is always 0..255.
fn convert_bytea_to_scalar(value: Datum, lobound: Datum, hibound: Datum) -> (f64, f64, f64) {
    let mut valstr = varlena_datum_payload(value);
    let mut lostr = varlena_datum_payload(lobound);
    let mut histr = varlena_datum_payload(hibound);

    let minlen = valstr.len().min(lostr.len()).min(histr.len());
    let mut i = 0;
    while i < minlen {
        if lostr[i] != histr[i] || lostr[i] != valstr[i] {
            break;
        }
        i += 1;
    }
    valstr = &valstr[i..];
    lostr = &lostr[i..];
    histr = &histr[i..];

    (
        convert_one_bytea_to_scalar(valstr),
        convert_one_bytea_to_scalar(lostr),
        convert_one_bytea_to_scalar(histr),
    )
}

fn convert_one_bytea_to_scalar(value: &[u8]) -> f64 {
    if value.is_empty() {
        return 0.0;
    }
    let base = 256.0f64;
    let mut num = 0.0f64;
    let mut denom = base;
    for &ch in &value[..value.len().min(10)] {
        num += ch as f64 / denom;
        denom *= base;
    }
    num
}

fn convert_numeric_to_scalar(value: Datum, typid: Oid) -> Option<f64> {
    const NUMERICOID: Oid = 1700;
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
        NUMERICOID => {
            Some(adt_numeric::numeric_float8_no_overflow_any(varlena_datum_payload(value)))
        }
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

    if vardata.rel.is_some() && rdata.rel.is_none() {
        let other = clauses::estimate_expression_value(run.mcx, right)?;
        return Ok(Some((vardata, other, true)));
    }
    if vardata.rel.is_none() && rdata.rel.is_some() {
        let other = clauses::estimate_expression_value(run.mcx, left)?;
        return Ok(Some((rdata, other, false)));
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
    let mut vardata = VariableStatData {
        var: None,
        rel: None,
        vartype,
        isunique: false,
        stats: None,
        acl_ok: false,
    };

    let (node, node_id) = if run.glob.last_ph_id != 0 && contain_placeholder(node) {
        let stripped = strip_all_phvs_mutator(run.mcx, node)?;
        (stripped, run.intern_expr(stripped))
    } else {
        (node, node_id)
    };

    // C: look inside any binary-compatible relabeling (vartype stays the
    // exposed type; the Var is returned without relabeling). Nested
    // RelabelTypes can be adjacent after PHV stripping.
    let (basenode, node_id) = {
        let mut b = node;
        while let Some(r) = b.as_relabel_type() {
            b = r.arg;
        }
        if b.ptr_eq(node) { (node, node_id) } else { (b, run.intern_expr(b)) }
    };
    let node = basenode;

    if let Some(var) = node.as_var() {
        if varrelid == 0 || varrelid == var.varno {
            let rel = crate::relnode::find_base_rel(&run.root, var.varno);
            vardata.var = Some(node_id);
            vardata.rel = Some(rel);
            vardata.isunique = crate::plancat::has_unique_index(run, rel, var.varattno);
            // A swapped-in rel subroot keeps its parent level visible (C:
            // the child root's parent_root link).
            let suspended = RootAncestors::Suspended(&run.suspended_roots);
            let up = match run.swapped_parent_subroot {
                Some(i) => RootAncestors::Link {
                    parent: &run.rel_subroots[i].root,
                    up: &suspended,
                },
                None => suspended,
            };
            let simple = examine_simple_variable(run, up, &run.root, var.varno, var.varattno)?;
            vardata.stats = simple.stats;
            vardata.isunique |= simple.force_unique;
            vardata.acl_ok = simple.acl_ok;
            return Ok(vardata);
        }
        // A Var of some other rel (varRelid restricts to one rel) falls to
        // the generic expression leg: no rel, no stats.
        return Ok(vardata);
    }
    match node.node_tag() {
        NodeTag::T_Const => Ok(vardata),
        // Var-free expressions (HAVING Aggrefs, PARAM_EXEC initplan outputs,
        // scalararraysel dummies): C's expression leg finds no relids and
        // returns "don't know".
        NodeTag::T_Aggref | NodeTag::T_Param | NodeTag::T_CaseTestExpr => Ok(vardata),
        // C's general expression leg: rel membership is judged net of
        // outer-join relids (basevarnos); a single-base-rel expression keeps
        // its rel and searches expression-index columns for stats, a
        // multi-rel one keeps the join rel.
        _ => {
            use types_pathnodes::relids;
            let mcx = run.mcx;
            let varnos = crate::initsplan::pull_varnos_relids(run, node)?;
            let basevarnos =
                relids::relids_difference(mcx, &varnos, &run.root.outer_join_rels);
            vardata.var = Some(node_id);
            if relids::relids_is_empty(&basevarnos) {
                // pseudo-constant clause
            } else if let Some(relid) = relids::relids_singleton_member(&basevarnos) {
                if varrelid == 0 || varrelid == relid {
                    let onerel = crate::relnode::find_base_rel(&run.root, relid);
                    vardata.rel = Some(onerel);
                    // Nullingrel bits inside the expression would prevent
                    // matching index/extended-stats expressions; strip first.
                    let matchnode =
                        if relids::relids_overlap(&varnos, &run.root.outer_join_rels) {
                            crate::relnode::strip_nulling_relids(
                                mcx,
                                node,
                                &run.root.outer_join_rels,
                            )?
                        } else {
                            node
                        };
                    examine_expression_index_stats(
                        run,
                        &mut vardata,
                        onerel,
                        relid,
                        matchnode,
                    )?;
                }
                // else treat it as a constant
            } else if varrelid == 0 {
                vardata.rel = crate::joinrels::find_join_rel(&run.root, &varnos);
            } else if relids::relids_is_member(varrelid, &varnos) {
                vardata.rel = Some(crate::relnode::find_base_rel(&run.root, varrelid));
            }
            Ok(vardata)
        }
    }
}

struct SimpleVarStats<'mcx> {
    stats: Option<&'mcx PgStatisticBundle<'mcx>>,
    acl_ok: bool,
    force_unique: bool,
}

impl SimpleVarStats<'_> {
    fn none() -> Self {
        SimpleVarStats { stats: None, acl_ok: true, force_unique: false }
    }
}

fn rte_at<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &types_pathnodes::PlannerInfo<'mcx>,
    varno: usize,
) -> &'mcx types_nodes::parsenodes::RangeTblEntry<'mcx> {
    match root.simple_rte_array[varno] {
        types_pathnodes::RangeTblEntryId::Parse { query, index } => run.queries
            [query.0 as usize]
            .rtable
            .nth(index as usize)
            .as_range_tbl_entry()
            .expect("rtable cell is a RangeTblEntry"),
        other => panic!("rte_at({varno}): unresolvable {other:?}"),
    }
}

// C targetIsInSortList with sortop == InvalidOid: pure sortgroupref match.
fn tle_in_sortlist(tle: &types_nodes::primnodes::TargetEntry<'_>, sortlist: &types_nodes::NodeList<'_>) -> bool {
    let tle_ref = tle.ressortgroupref;
    tle_ref != 0
        && sortlist.iter().any(|n| {
            n.as_sort_group_clause().expect("sortlist cell").tleSortGroupRef == tle_ref
        })
}

// examine_variable (selfuncs.c) expression legs: expression-index column
// stats, then extended-statistics expressions via statext_expressions_load.
// nullingrels within the expression aren't stripped before matching
// (PHV/outer-join expression stats keys are unreachable while PHV creation
// is loud upstream).
fn examine_expression_index_stats<'mcx>(
    run: &PlannerRun<'mcx>,
    vardata: &mut VariableStatData<'mcx>,
    onerel: RelId,
    varno: i32,
    node: Node<'mcx>,
) -> PgResult<()> {
    for index in run.root.rel(onerel).indexlist.iter() {
        if index.indexprs.is_empty() {
            continue;
        }
        let mut indexpr_item = 0usize;
        for pos in 0..index.ncolumns as usize {
            if index.indexkeys[pos] != 0 {
                continue;
            }
            assert!(indexpr_item < index.indexprs.len(), "too few entries in indexprs list");
            let mut indexkey = *run.root.expr_node(index.indexprs[indexpr_item]);
            indexpr_item += 1;
            if let Some(r) = indexkey.as_relabel_type() {
                indexkey = r.arg;
            }
            if !types_nodes::equal(node, indexkey) {
                continue;
            }
            if index.unique
                && index.nkeycolumns == 1
                && pos == 0
                && (index.indpred.is_empty() || index.predOK.get())
            {
                vardata.isunique = true;
            }
            // Stats only from non-partial indexes.
            if index.indpred.is_empty() {
                vardata.stats = get_att_stats(run, index.indexoid, (pos + 1) as i16, false)?;
                vardata.acl_ok = if vardata.stats.is_some() {
                    all_rows_selectable(run, &run.root, varno, None)?
                } else {
                    true
                };
            }
            if vardata.stats.is_some() {
                break;
            }
        }
        if vardata.stats.is_some() {
            break;
        }
    }
    if vardata.stats.is_none() {
        let inh = rte_at(run, &run.root, varno as usize).inh;
        'stats: for &sid in run.root.rel(onerel).statlist.iter() {
            let info = run.root.statistic_ext(sid);
            if info.kind != b'e' as i8 || info.inherit != inh {
                continue;
            }
            for (pos, &eid) in info.exprs.iter().enumerate() {
                let mut expr = *run.root.expr_node(eid);
                if let Some(r) = expr.as_relabel_type() {
                    expr = r.arg;
                }
                if types_nodes::equal(node, expr) {
                    let stat_oid = info.stat_oid;
                    // Keyed by statistics object, not (rel, att): not memoized
                    // (rare path); arena-leaked so the ref shape is uniform.
                    vardata.stats = Some(leak_bundle(
                        run.mcx,
                        syscache_seams::statext_expressions_load::call(
                            run.mcx,
                            stat_oid,
                            inh,
                            pos as i32,
                        )?,
                    )?);
                    vardata.acl_ok = all_rows_selectable(run, &run.root, varno, None)?;
                    break 'stats;
                }
            }
        }
    }
    Ok(())
}

// C's parent_root chain for a root under examination: the live planning
// chain is suspended_roots; drilled subroots hang off the root that owns
// their RTE (rel_subroots) or the resolved cteroot (glob subroots).
#[derive(Clone, Copy)]
enum RootAncestors<'a, 'mcx> {
    Suspended(&'a [crate::run::SubrootState<'mcx>]),
    Link {
        parent: &'a types_pathnodes::PlannerInfo<'mcx>,
        up: &'a RootAncestors<'a, 'mcx>,
    },
}

impl<'a, 'mcx> RootAncestors<'a, 'mcx> {
    fn ancestor(
        &self,
        lvl: usize,
    ) -> Option<(&'a types_pathnodes::PlannerInfo<'mcx>, RootAncestors<'a, 'mcx>)> {
        debug_assert!(lvl >= 1);
        match *self {
            RootAncestors::Suspended(s) => {
                if lvl <= s.len() {
                    let i = s.len() - lvl;
                    Some((&s[i].root, RootAncestors::Suspended(&s[..i])))
                } else {
                    None
                }
            }
            RootAncestors::Link { parent, up } => {
                if lvl == 1 {
                    Some((parent, *up))
                } else {
                    up.ancestor(lvl - 1)
                }
            }
        }
    }
}

// examine_simple_variable (selfuncs.c): the STATRELATTINH probe plus the
// subquery/CTE drill into the already-planned subroot's targetlist.
fn examine_simple_variable<'mcx>(
    run: &PlannerRun<'mcx>,
    up: RootAncestors<'_, 'mcx>,
    root: &types_pathnodes::PlannerInfo<'mcx>,
    varno: i32,
    varattno: i16,
) -> PgResult<SimpleVarStats<'mcx>> {
    let rte = rte_at(run, root, varno as usize);
    match rte.rtekind {
        RTEKind::RTE_RELATION => {
            let stats = get_att_stats(run, rte.relid, varattno, rte.inh)?;
            // C: acl_ok = all_rows_selectable when a stats tuple was found,
            // true otherwise (suppress leakproofness checks).
            let acl_ok = if stats.is_some() {
                all_rows_selectable(run, root, varno, Some(&[varattno]))?
            } else {
                true
            };
            Ok(SimpleVarStats { stats, acl_ok, force_unique: false })
        }
        RTEKind::RTE_SUBQUERY if !rte.inh => {
            if varattno == 0 {
                return Ok(SimpleVarStats::none());
            }
            let rel = crate::relnode::find_base_rel(root, varno);
            let Some(idx) = root.rel(rel).subroot_idx else {
                return Ok(SimpleVarStats::none());
            };
            let sub_up = RootAncestors::Link { parent: root, up: &up };
            examine_subroot_output(run, sub_up, rte, &run.rel_subroots[idx].root, varattno)
        }
        RTEKind::RTE_CTE if !rte.self_reference => {
            if varattno == 0 {
                return Ok(SimpleVarStats::none());
            }
            let ctename = rte.ctename.expect("CTE rte has a ctename");
            let levelsup = rte.ctelevelsup as usize;
            let (cteroot, cte_up): (&types_pathnodes::PlannerInfo<'mcx>, _) = if levelsup == 0 {
                (root, up)
            } else {
                up.ancestor(levelsup)
                    .unwrap_or_else(|| panic!("bad levelsup for CTE \"{ctename}\""))
            };
            // cte_list is SS_process_ctes' snapshot: a mid-preprocessing
            // ancestor (CTE cross-reference) has no sealed parse yet.
            let ndx = cteroot
                .cte_list
                .iter()
                .position(|c| {
                    c.as_common_table_expr().expect("cteList cell").ctename == Some(ctename)
                })
                .unwrap_or_else(|| panic!("could not find CTE \"{ctename}\""));
            assert!(
                ndx < cteroot.cte_plan_ids.len(),
                "could not find plan for CTE \"{ctename}\""
            );
            let plan_id = cteroot.cte_plan_ids[ndx];
            assert!(plan_id > 0, "no plan was made for CTE \"{ctename}\"");
            let sub_up = RootAncestors::Link { parent: cteroot, up: &cte_up };
            examine_subroot_output(
                run,
                sub_up,
                rte,
                &run.subroots[(plan_id - 1) as usize].root,
                varattno,
            )
        }
        // C falls through with no stats for every other RTE kind (appendrel
        // subqueries and self-referencing CTEs included).
        RTEKind::RTE_FUNCTION
        | RTEKind::RTE_TABLEFUNC
        | RTEKind::RTE_VALUES
        | RTEKind::RTE_JOIN
        | RTEKind::RTE_SUBQUERY
        | RTEKind::RTE_NAMEDTUPLESTORE
        | RTEKind::RTE_RESULT
        | RTEKind::RTE_GROUP
        | RTEKind::RTE_CTE => Ok(SimpleVarStats::none()),
    }
}

// The RTE_SUBQUERY/RTE_CTE tail of C examine_simple_variable, on the
// planner-mangled subquery parsetree.
fn examine_subroot_output<'mcx>(
    run: &PlannerRun<'mcx>,
    up: RootAncestors<'_, 'mcx>,
    rte: &types_nodes::parsenodes::RangeTblEntry<'mcx>,
    subroot: &types_pathnodes::PlannerInfo<'mcx>,
    varattno: i16,
) -> PgResult<SimpleVarStats<'mcx>> {
    let subquery = run.queries[subroot.parse.0 as usize];
    // Set ops and grouping sets mash underlying columns' stats beyond
    // recognition.
    if subquery.setOperations.is_some() || !subquery.groupingSets.is_nil() {
        return Ok(SimpleVarStats::none());
    }
    let subtlist = if !subquery.returningList.is_nil() {
        &subquery.returningList
    } else {
        &subquery.targetList
    };
    let aliasname =
        || rte.eref.and_then(|e| e.aliasname).unwrap_or("(unnamed)");
    let ste_node = subtlist
        .iter()
        .find(|n| n.as_target_entry().expect("tlist cell").resno == varattno)
        .unwrap_or_else(|| {
            panic!("subquery {} does not have attribute {varattno}", aliasname())
        });
    let ste = ste_node.as_target_entry().unwrap();
    assert!(!ste.resjunk, "subquery {} does not have attribute {varattno}", aliasname());

    // A single-column DISTINCT (or DISTINCT ON) / GROUP BY makes the output
    // unique, but its stats are no longer usable.
    if !subquery.distinctClause.is_nil() {
        let force_unique = subquery.distinctClause.len() == 1
            && tle_in_sortlist(ste, &subquery.distinctClause);
        return Ok(SimpleVarStats { stats: None, acl_ok: true, force_unique });
    }
    if !subquery.groupClause.is_nil() {
        let force_unique =
            subquery.groupClause.len() == 1 && tle_in_sortlist(ste, &subquery.groupClause);
        return Ok(SimpleVarStats { stats: None, acl_ok: true, force_unique });
    }
    if rte.security_barrier {
        return Ok(SimpleVarStats::none());
    }
    if let Some(v) = ste.expr.as_var() {
        if v.varlevelsup == 0 {
            return examine_simple_variable(run, up, subroot, v.varno, v.varattno);
        }
    }
    Ok(SimpleVarStats::none())
}

// all_rows_selectable (selfuncs.c). varattnos carries raw attnos (0 =
// whole-row, negative = system attno) — set semantics match C's
// FirstLowInvalidHeapAttributeNumber-offset Bitmapset; the result is
// iteration-order independent.
pub fn all_rows_selectable<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &types_pathnodes::PlannerInfo<'mcx>,
    varno: i32,
    varattnos: Option<&[i16]>,
) -> PgResult<bool> {
    let mut rte = rte_at(run, root, varno as usize);
    debug_assert!(rte.rtekind == RTEKind::RTE_RELATION);

    let rel = (varno > 0 && varno < root.simple_rel_array_size)
        .then(|| root.simple_rel_array[varno as usize])
        .flatten();
    let mut userid = match rel {
        Some(r) => root.rel(r).userid,
        None => {
            let perminfo = run.queries[root.parse.0 as usize]
                .rteperminfos
                .nth(rte.perminfoindex as usize - 1)
                .as_rte_permission_info()
                .expect("perminfoindex resolves");
            perminfo.checkAsUser
        }
    };
    if userid == 0 {
        userid = miscinit_seams::get_user_id::call();
    }

    let mut varno = varno;
    let mut cur_attnos: Option<mcx::PgVec<'_, i16>> = varattnos.map(|s| {
        let mut v = mcx::PgVec::new_in(run.mcx);
        v.extend(s.iter().copied());
        v
    });
    if !root.append_rel_array.is_empty() {
        let mut appinfo = root.append_rel_array.get(varno as usize).and_then(|a| a.as_ref());
        while let Some(ai) = appinfo {
            if rte_at(run, root, ai.parent_relid as usize).rtekind != RTEKind::RTE_RELATION {
                break;
            }
            if let Some(attnos) = &cur_attnos {
                let mut parent_attnos: mcx::PgVec<'_, i16> = mcx::PgVec::new_in(run.mcx);
                for &attno in attnos.iter() {
                    if attno == 0 {
                        // Whole-row reference: map every child column.
                        for child_attno in 1..=ai.num_child_cols {
                            let parent_attno = ai.parent_colnos[child_attno as usize - 1];
                            if parent_attno == 0 {
                                return Ok(false);
                            }
                            parent_attnos.push(parent_attno);
                        }
                    } else if attno < 0 {
                        parent_attnos.push(attno);
                    } else {
                        if attno as i32 > ai.num_child_cols {
                            return Ok(false);
                        }
                        let parent_attno = ai.parent_colnos[attno as usize - 1];
                        if parent_attno == 0 {
                            return Ok(false);
                        }
                        parent_attnos.push(parent_attno);
                    }
                }
                cur_attnos = Some(parent_attnos);
            }
            varno = ai.parent_relid as i32;
            appinfo = root.append_rel_array.get(varno as usize).and_then(|a| a.as_ref());
        }
        rte = rte_at(run, root, varno as usize);
        debug_assert!(rte.rtekind == RTEKind::RTE_RELATION);
    }

    if !rte.securityQuals.is_nil() {
        return Ok(false);
    }

    if crate::syscache_memo::class_aclmask(run, rte.relid, userid, adt_acl::ACL_SELECT, false)?
        != 0
    {
        return Ok(true);
    }

    let Some(attnos) = &cur_attnos else {
        return Ok(false);
    };

    for &attno in attnos.iter() {
        if attno == 0 {
            if aclchk::pg_attribute_aclcheck_all(
                rte.relid,
                userid,
                adt_acl::ACL_SELECT,
                adt_acl::AclMaskHow::AclmaskAll,
            )? != aclchk::ACLCHECK_OK
            {
                return Ok(false);
            }
        } else if aclchk::pg_attribute_aclcheck(rte.relid, attno, userid, adt_acl::ACL_SELECT)?
            != aclchk::ACLCHECK_OK
        {
            return Ok(false);
        }
    }
    Ok(true)
}

// statistic_proc_security_check (selfuncs.c); C's DEBUG2 log is dropped.
pub(crate) fn statistic_proc_security_check(
    vardata: &VariableStatData<'_>,
    func_oid: Oid,
) -> PgResult<bool> {
    if vardata.acl_ok {
        return Ok(true);
    }
    if func_oid == 0 {
        return Ok(false);
    }
    lsyscache::get_func_leakproof(func_oid)
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
    } else if vardata.stats.is_some()
        && statistic_proc_security_check(
            vardata,
            crate::syscache_memo::get_opcode(run, oproid)?,
        )?
    {
        match vardata.slot(STATISTIC_KIND_MCV, 0) {
            Some(sslot) => {
                let mut eqproc = opproc_for_run(run, oproid)?;
                // Torn slot: only values paired with a frequency are MCV
                // entries — an unpaired match has no frequency to return, so
                // the const falls through to the not-an-MCV arm (whose
                // sumcommon is already nnumbers-bounded, as C). Well-formed
                // slots have equal lengths: exactly C's nvalues loop.
                let mut matched = None;
                for (&v, &n) in sslot.values()?.iter().zip(sslot.numbers()?.iter()) {
                    if op_test(run.mcx, &mut eqproc, collation, v, constval, varonleft)? {
                        matched = Some(n);
                        break;
                    }
                }
                match matched {
                    Some(n) => n as f64,
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

pub use planner_seams::AmCostEstimate;

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
        types_relscan::IndexAmKind::Gist => gistcostestimate(run, path_id, loop_count),
        types_relscan::IndexAmKind::Spgist => spgcostestimate(run, path_id, loop_count),
        types_relscan::IndexAmKind::Brin => brincostestimate(run, path_id, loop_count),
        types_relscan::IndexAmKind::Hnsw => hnswcostestimate(run, path_id, loop_count),
        types_relscan::IndexAmKind::Bloom => blcostestimate(run, path_id, loop_count),
        #[allow(unreachable_patterns)]
        other => panic!("amcostestimate (selfuncs.c): {other:?}; M2 index-AM lane"),
    }
}

// blcostestimate (contrib/bloom blcost.c): every index tuple is visited, so
// numIndexTuples = index->tuples; the rest is the generic estimate.
fn blcostestimate(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    loop_count: f64,
) -> PgResult<AmCostEstimate> {
    let index_tuples = {
        let PathNode::IndexPath(ip) = run.root.path(path_id) else {
            panic!("blcostestimate: not an IndexPath")
        };
        ip.indexinfo.as_ref().expect("indexinfo set").tuples
    };
    let mut costs = GenericCosts {
        num_index_tuples: index_tuples,
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
        index_correlation: costs.index_correlation,
        index_pages: costs.num_index_pages,
    })
}

// hnswcostestimate (pgvector hnsw.c).
fn hnswcostestimate(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    loop_count: f64,
) -> PgResult<AmCostEstimate> {
    let (has_orderbys, index_tuples, indexoid, reltablespace, rel_pages) = {
        let PathNode::IndexPath(ip) = run.root.path(path_id) else {
            panic!("hnswcostestimate: not an IndexPath")
        };
        let index = ip.indexinfo.as_ref().expect("indexinfo set");
        let rel_pages = index
            .rel
            .as_ref()
            .map(|r| run.root.rel(*r).pages as f64)
            .unwrap_or(0.0);
        (
            !ip.indexorderbys.is_empty(),
            index.tuples,
            index.indexoid,
            index.reltablespace,
            rel_pages,
        )
    };

    if !has_orderbys {
        // "On disable_cost" (PG18): never use the index without an order.
        run.root.path_mut(path_id).base_mut().disabled_nodes = 2;
        return Ok(AmCostEstimate {
            index_startup_cost: f64::INFINITY,
            index_total_cost: f64::INFINITY,
            index_selectivity: 0.0,
            index_correlation: 0.0,
            index_pages: 0.0,
        });
    }

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

    let m = {
        let mcx = run.mcx;
        let index_rel = indexam::index_open(mcx, indexoid, types_rel::NoLock)?;
        let meta = pgvector_hnsw::utils::read_meta(&index_rel)?;
        meta.m as f64
    };

    let hnsw_ef_search = guc_tables::vars::hnsw_ef_search.read() as f64;
    let ratio = if index_tuples > 0.0 {
        let scaling_factor = 0.55;
        let entry_level = (index_tuples.ln() * (1.0 / m.ln())) as i32;
        let layer0_tuples_max = (m * 2.0) * hnsw_ef_search;
        let layer0_selectivity =
            scaling_factor * index_tuples.ln() / (m.ln() * (1.0 + hnsw_ef_search.ln()));
        let r = (entry_level as f64 * m + layer0_tuples_max * layer0_selectivity) / index_tuples;
        r.min(1.0)
    } else {
        1.0
    };

    let (spc_random_page_cost, spc_seq_page_cost) = {
        let _ = reltablespace;
        (gucs::random_page_cost(), gucs::seq_page_cost())
    };

    costs.index_startup_cost = costs.index_total_cost * ratio;
    let startup_pages = costs.num_index_pages * ratio;
    if startup_pages > rel_pages && ratio < 0.5 {
        costs.index_startup_cost -= startup_pages * (spc_random_page_cost - spc_seq_page_cost);
        costs.index_startup_cost -= (startup_pages - rel_pages) * spc_seq_page_cost;
    }

    Ok(AmCostEstimate {
        index_startup_cost: costs.index_startup_cost,
        index_total_cost: costs.index_total_cost,
        index_selectivity: costs.index_selectivity,
        index_correlation: costs.index_correlation,
        index_pages: costs.num_index_pages,
    })
}

// gistcostestimate (selfuncs.c): genericcostestimate + log-fanout-100 descent.
fn gistcostestimate(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    loop_count: f64,
) -> PgResult<AmCostEstimate> {
    let (index_tuples, tree_height) = {
        let PathNode::IndexPath(ip) = run.root.path(path_id) else {
            panic!("gistcostestimate: not an IndexPath")
        };
        let index = ip.indexinfo.as_ref().expect("indexinfo set");
        let mut tree_height = index.tree_height.get();
        if tree_height < 0 {
            tree_height = if index.pages > 1 {
                ((index.pages as f64).ln() / 100.0f64.ln()) as i32
            } else {
                0
            };
            index.tree_height.set(tree_height);
        }
        (index.tuples, tree_height)
    };

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

    let cpu_operator_cost = gucs::cpu_operator_cost();
    if index_tuples > 1.0 {
        let descent_cost = index_tuples.ln().ceil() * cpu_operator_cost;
        costs.index_startup_cost += descent_cost;
        costs.index_total_cost = costs.num_sa_scans.mul_add(descent_cost, costs.index_total_cost);
    }
    let descent_cost =
        (tree_height as f64 + 1.0) * DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost;
    costs.index_startup_cost += descent_cost;
    costs.index_total_cost = costs.num_sa_scans.mul_add(descent_cost, costs.index_total_cost);

    Ok(AmCostEstimate {
        index_startup_cost: costs.index_startup_cost,
        index_total_cost: costs.index_total_cost,
        index_selectivity: costs.index_selectivity,
        index_correlation: costs.index_correlation,
        index_pages: costs.num_index_pages,
    })
}

// spgcostestimate (selfuncs.c): identical structure to gistcostestimate.
fn spgcostestimate(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    loop_count: f64,
) -> PgResult<AmCostEstimate> {
    let (index_tuples, tree_height) = {
        let PathNode::IndexPath(ip) = run.root.path(path_id) else {
            panic!("spgcostestimate: not an IndexPath")
        };
        let index = ip.indexinfo.as_ref().expect("indexinfo set");
        let mut tree_height = index.tree_height.get();
        if tree_height < 0 {
            tree_height = if index.pages > 1 {
                ((index.pages as f64).ln() / 100.0f64.ln()) as i32
            } else {
                0
            };
            index.tree_height.set(tree_height);
        }
        (index.tuples, tree_height)
    };

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

    let cpu_operator_cost = gucs::cpu_operator_cost();
    if index_tuples > 1.0 {
        let descent_cost = index_tuples.ln().ceil() * cpu_operator_cost;
        costs.index_startup_cost += descent_cost;
        costs.index_total_cost = costs.num_sa_scans.mul_add(descent_cost, costs.index_total_cost);
    }
    let descent_cost =
        (tree_height as f64 + 1.0) * DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost;
    costs.index_startup_cost += descent_cost;
    costs.index_total_cost = costs.num_sa_scans.mul_add(descent_cost, costs.index_total_cost);

    Ok(AmCostEstimate {
        index_startup_cost: costs.index_startup_cost,
        index_total_cost: costs.index_total_cost,
        index_selectivity: costs.index_selectivity,
        index_correlation: costs.index_correlation,
        index_pages: costs.num_index_pages,
    })
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
// add_predicate_to_index_quals (selfuncs.c): AND the partial-index predicate
// (as fresh RestrictInfos) into a qual list for selectivity purposes.
fn add_predicate_to_index_quals<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_id: types_pathnodes::PathId,
    index_quals: &[RinfoId],
) -> PgResult<mcx::PgVec<'mcx, RinfoId>> {
    let mcx = run.mcx;
    let indpred = {
        let PathNode::IndexPath(ip) = run.root.path(path_id) else { unreachable!() };
        ip.indexinfo.as_ref().expect("indexinfo set").indpred.clone()
    };
    let mut result: mcx::PgVec<'mcx, RinfoId> = mcx::PgVec::new_in(mcx);
    if !indpred.is_empty() {
        let mut qual_nodes: mcx::PgVec<'mcx, Node<'mcx>> = mcx::PgVec::new_in(mcx);
        for &rid in index_quals {
            qual_nodes.push(*run.root.expr_node(run.root.rinfo(rid).clause));
        }
        for &pid in indpred.iter() {
            let pred = *run.root.expr_node(pid);
            if !crate::predtest::predicate_implied_by(mcx, &[pred], &qual_nodes, false)? {
                result.push(crate::initsplan::make_restrictinfo(
                    run, pred, true, false, false, false, 0, crate::relnode::relids_empty(), crate::relnode::relids_empty(), crate::relnode::relids_empty(),
                )?);
            }
        }
    }
    result.extend(index_quals.iter().copied());
    Ok(result)
}

fn genericcostestimate(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    loop_count: f64,
    costs: &mut GenericCosts,
) -> PgResult<()> {
    let (index_quals, index_orderbys, index_pages, index_tuples, index_rel, reltablespace) = {
        let PathNode::IndexPath(ip) = run.root.path(path_id) else { unreachable!() };
        let index = ip.indexinfo.as_ref().expect("indexinfo set");
        let mut orderbys: mcx::PgVec<'_, types_pathnodes::NodeId> = mcx::PgVec::new_in(run.mcx);
        orderbys.extend(ip.indexorderbys.iter().copied());
        (
            get_quals_from_indexclauses(run, path_id),
            orderbys,
            index.pages,
            index.tuples,
            index.rel.expect("index rel set"),
            index.reltablespace,
        )
    };
    let index_rel_relid = run.root.rel(index_rel).relid as i32;
    let index_rel_tuples = run.root.rel(index_rel).tuples;

    debug_assert!(costs.num_sa_scans >= 1.0);
    let num_sa_scans = costs.num_sa_scans;

    let selectivity_quals = add_predicate_to_index_quals(run, path_id, &index_quals)?;
    let index_selectivity = crate::clausesel::clauselist_selectivity(
        run,
        &selectivity_quals,
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

    let qual_arg_cost = index_other_operands_eval_cost(run, &index_quals)?
        + index_orderby_operands_eval_cost(run, &index_orderbys)?;
    let qual_op_cost =
        gucs::cpu_operator_cost() * (index_quals.len() + index_orderbys.len()) as f64;

    let index_startup_cost = qual_arg_cost;
    index_total_cost += qual_arg_cost;
    // mul_add mirrors the C referee's fmadd (GCC fp-contract on aarch64
    // fuses `cost += expr * tuples`); odd numIndexTuples puts the total on a
    // half-cent display boundary, exposing the one-ulp difference.
    index_total_cost = (num_index_tuples * num_sa_scans)
        .mul_add(gucs::cpu_index_tuple_cost() + qual_op_cost, index_total_cost);

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
            NodeTag::T_ScalarArrayOpExpr => {
                Some(clause.as_scalar_array_op_expr().unwrap().args.nth(1))
            }
            NodeTag::T_RowCompareExpr => {
                // C costs the whole rargs List; summing per element is the
                // same walker arithmetic.
                for arg in &clause.as_row_compare_expr().unwrap().rargs {
                    let cost = crate::costsize::cost_qual_eval_node(Some(&mut *run), arg)?;
                    qual_arg_cost += cost.startup + cost.per_tuple;
                }
                None
            }
            NodeTag::T_NullTest => None,
            other => panic!("index_other_operands_eval_cost (selfuncs.c): {other:?}; M2 lane"),
        };
        if let Some(op) = other_operand {
            let cost = crate::costsize::cost_qual_eval_node(Some(&mut *run), op)?;
            qual_arg_cost += cost.startup + cost.per_tuple;
        }
    }
    Ok(qual_arg_cost)
}

// index_other_operands_eval_cost (selfuncs.c), indexorderbys leg: bare
// OpExprs with the index key on the left.
fn index_orderby_operands_eval_cost(
    run: &mut PlannerRun<'_>,
    index_orderbys: &[types_pathnodes::NodeId],
) -> PgResult<f64> {
    let mut qual_arg_cost = 0.0;
    for &nid in index_orderbys {
        let clause = *run.root.expr_node(nid);
        let other_operand = match clause.node_tag() {
            NodeTag::T_OpExpr => clause.as_op_expr().unwrap().args.nth(1),
            other => panic!("index_other_operands_eval_cost (selfuncs.c): {other:?} orderby"),
        };
        let cost = crate::costsize::cost_qual_eval_node(Some(&mut *run), other_operand)?;
        qual_arg_cost += cost.startup + cost.per_tuple;
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

// brincostestimate (selfuncs.c): search behavior completely different from
// other index types.
fn brincostestimate(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    loop_count: f64,
) -> PgResult<AmCostEstimate> {
    let (index_quals, index_pages, index_rel, reltablespace, indexoid, clause_attnums) = {
        let PathNode::IndexPath(ip) = run.root.path(path_id) else {
            panic!("brincostestimate: not an IndexPath")
        };
        let index = ip.indexinfo.as_ref().expect("indexinfo set");
        let mut attnums: Vec<(i16, i16)> = Vec::new();
        for ic in ip.indexclauses.iter() {
            attnums.push((ic.indexcol as i16, index.indexkeys[ic.indexcol as usize] as i16));
        }
        (
            get_quals_from_indexclauses(run, path_id),
            index.pages,
            index.rel.expect("index rel set"),
            index.reltablespace,
            index.indexoid,
            attnums,
        )
    };
    let num_pages = index_pages as f64;
    let baserel_relid = run.root.rel(index_rel).relid as i32;
    let baserel_pages = run.root.rel(index_rel).pages as f64;

    let (spc_random_page_cost, spc_seq_page_cost) =
        crate::costsize::get_tablespace_page_costs(reltablespace);

    // Fetch pagesPerRange/revmapNumPages from the index itself (a lock is
    // already held from plancat).
    let mcx = run.mcx;
    let (pages_per_range, revmap_num_pages) = {
        let index_rel_open = indexam::index_open(mcx, indexoid, types_rel::NoLock)?;
        let stats = brin::brinGetStats(&index_rel_open)?;
        indexam::index_close(index_rel_open, types_rel::NoLock)?;
        (stats.pagesPerRange as f64, stats.revmapNumPages as f64)
    };
    let index_ranges = (baserel_pages / pages_per_range).ceil().max(1.0);

    // Index correlation: the largest absolute correlation among the queried
    // columns (0 when no stats).
    let mut index_correlation = 0.0f64;
    let rte_relid = run.rte(baserel_relid as usize).relid;
    let rte_inh = run.rte(baserel_relid as usize).inh;
    for &(indexcol, attnum) in &clause_attnums {
        // examine_indexcol_variable (selfuncs.c): expression columns read the
        // index's own pg_statistic row.
        let (stat_relid, stat_attnum, stat_inh) = if attnum != 0 {
            (rte_relid, attnum, rte_inh)
        } else {
            (indexoid, (indexcol + 1) as i16, false)
        };
        if let Some(bundle) = get_att_stats(run, stat_relid, stat_attnum, stat_inh)? {
            if let Some(slot) =
                bundle.slots.iter().find(|sl| sl.kind == STATISTIC_KIND_CORRELATION)
            {
                let numbers = slot.numbers()?;
                let var_correlation = if !numbers.is_empty() {
                    (numbers[0] as f64).abs()
                } else {
                    0.0
                };
                if var_correlation > index_correlation {
                    index_correlation = var_correlation;
                }
            }
        }
    }

    let qual_selectivity = crate::clausesel::clauselist_selectivity(
        run,
        &index_quals,
        baserel_relid,
        JOIN_INNER,
        None,
    )?;

    let minimal_ranges = (index_ranges * qual_selectivity).ceil();
    let estimated_ranges = if index_correlation < 1.0e-10 {
        index_ranges
    } else {
        (minimal_ranges / index_correlation).min(index_ranges)
    };

    let selec = clamp_probability(estimated_ranges / index_ranges);

    let qual_arg_cost = index_other_operands_eval_cost(run, &index_quals)?;

    // Startup: read the whole revmap sequentially, plus the qual setup.
    let mut index_startup_cost = spc_seq_page_cost * revmap_num_pages * loop_count;
    index_startup_cost += qual_arg_cost;

    // Total: the rest of the index in random order.
    let mut index_total_cost = index_startup_cost
        + spc_random_page_cost * (num_pages - revmap_num_pages) * loop_count;

    // Small per-matched-range charge, scaled by pages per range (bitmap
    // manipulation cost).
    index_total_cost +=
        0.1 * gucs::cpu_operator_cost() * estimated_ranges * pages_per_range;

    Ok(AmCostEstimate {
        index_startup_cost,
        index_total_cost,
        index_selectivity: selec,
        index_correlation: index_correlation,
        index_pages: num_pages,
    })
}

// btcostestimate (selfuncs.c).
fn btcostestimate(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    loop_count: f64,
) -> PgResult<AmCostEstimate> {
    let (indexclauses, index_unique, index_nkeycolumns, index_tuples, index_tree_height, index_rel, opfamilies, index_indexoid) = {
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
            index.indexoid,
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
    let mut found_row_compare = false;
    let mut num_sa_scans = 1.0f64;

    'buildquals: for iclause in indexclauses.iter() {
        if indexcol < iclause.indexcol as i32 {
            // Skip arrays can't be added after a RowCompare input qual
            // (nbtree limitation; selfuncs.c).
            if found_row_compare {
                break 'buildquals;
            }
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
                // examine_indexcol_variable (selfuncs.c): simple columns read
                // the table's stats; expression columns read the index's own
                // pg_statistic row (colnum = indexcol+1, inh false).
                let stats = if attno != 0 {
                    let (relid, inh) = {
                        let rte = run.rte(index_rel_relid as usize);
                        (rte.relid, rte.inh)
                    };
                    get_att_stats(run, relid, attno as i16, inh)?
                } else {
                    get_att_stats(run, index_indexoid, (indexcol + 1) as i16, false)?
                };
                let vardata = VariableStatData {
                    var: None,
                    rel: Some(index_rel),
                    vartype: index_opcintype[indexcol as usize],
                    isunique: false,
                    stats,
                    acl_ok: false,
                };
                let (mut ndistinct, isdefault) = get_variable_numdistinct(run, &vardata);
                // btcost_correlation-in-passing arm folds into the shared
                // leading-column correlation block below (same stats row).
                if isdefault {
                    num_sa_scans = num_sa_scans_prev_cols;
                    break 'buildquals;
                }
                if !index_skip_quals.is_empty() {
                    let partial_skip_quals =
                        add_predicate_to_index_quals(run, path_id, &index_skip_quals)?;
                    let ndistinctfrac = crate::clausesel::clauselist_selectivity(
                        run,
                        &partial_skip_quals,
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
                NodeTag::T_RowCompareExpr => {
                    found_row_compare = true;
                    clause.as_row_compare_expr().unwrap().opnos.nth(0)
                }
                NodeTag::T_ScalarArrayOpExpr => {
                    let saop = clause.as_scalar_array_op_expr().unwrap();
                    let alength = estimate_array_length(Some(run), saop.args.nth(1))?;
                    found_array = true;
                    if alength > 1.0 {
                        num_sa_scans *= alength;
                    }
                    saop.opno
                }
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
                NodeTag::T_RowCompareExpr => clause.as_row_compare_expr().unwrap().opnos.nth(0),
                other => panic!("btcostestimate (selfuncs.c): indexqual {other:?}; M2 lane"),
            };
            if clause_op != 0 {
                let op_strategy = crate::syscache_memo::get_op_opfamily_strategy(
                    run,
                    clause_op,
                    opfamilies[indexcol as usize],
                )?;
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
        let selectivity_quals = add_predicate_to_index_quals(run, path_id, &index_bound_quals)?;
        let btree_selectivity = crate::clausesel::clauselist_selectivity(
            run,
            &selectivity_quals,
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
        costs.index_total_cost = costs.num_sa_scans.mul_add(descent_cost, costs.index_total_cost);
    }
    let descent_cost =
        (index_tree_height as f64 + 1.0) * DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost;
    costs.index_startup_cost += descent_cost;
    costs.index_total_cost = costs.num_sa_scans.mul_add(descent_cost, costs.index_total_cost);

    // btcost_correlation over the leading column; expression columns read the
    // index's own pg_statistic row (colnum 1, inh false), as C.
    {
        let (attno, indexoid, opfamily0, opcintype0, reverse0, nkeycols) = {
            let PathNode::IndexPath(ip) = run.root.path(path_id) else { unreachable!() };
            let index = ip.indexinfo.as_ref().unwrap();
            (
                index.indexkeys[0] as i16,
                index.indexoid,
                index.opfamily[0],
                index.opcintype[0],
                index.reverse_sort[0],
                index.nkeycolumns,
            )
        };
        let (stat_relid, stat_attno, stat_inh) = if attno != 0 {
            let rte = run.rte(index_rel_relid as usize);
            (rte.relid, attno, rte.inh)
        } else {
            (indexoid, 1, false)
        };
        if let Some(bundle) = get_att_stats(run, stat_relid, stat_attno, stat_inh)? {
            let sortop = crate::syscache_memo::get_opfamily_member(
                run,
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
                // C btcostestimate guards nnumbers > 0 before reading the
                // correlation value — a degenerate/torn stats slot with an
                // empty numbers array is tolerated, not asserted (the
                // unguarded [0] panicked backends under high auto-analyze
                // churn; found by the GL-ELR-XIDWAIT win-table rig, where
                // the armed arm's ~10x update throughput made analyze
                // rewrites of the stats row constant).
                if let Some(&corr0) = slot.numbers()?.first() {
                    let mut corr = corr0 as f64;
                    if reverse0 {
                        corr = -corr;
                    }
                    costs.index_correlation = if nkeycols > 1 { corr * 0.75 } else { corr };
                }
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

/// C's non-NULL `estinfo` form: also reports SELFLAG_USED_DEFAULT.
pub fn estimate_num_groups_estinfo<'mcx>(
    run: &mut PlannerRun<'mcx>,
    group_exprs: &[(NodeId, Node<'mcx>)],
    input_rows: f64,
) -> PgResult<(f64, bool)> {
    let mut used_default = false;
    let n = estimate_num_groups_core(run, group_exprs, input_rows, None, Some(&mut used_default))?;
    Ok((n, used_default))
}

/// C's `pgset` form: a grouping set given as 0-based indexes into
/// `group_exprs`; exprs outside the set are skipped.
pub fn estimate_num_groups_pgset<'mcx>(
    run: &mut PlannerRun<'mcx>,
    group_exprs: &[(NodeId, Node<'mcx>)],
    input_rows: f64,
    pgset: Option<&[i32]>,
) -> PgResult<f64> {
    estimate_num_groups_core(run, group_exprs, input_rows, pgset, None)
}

struct GroupVarInfo<'mcx> {
    node: Node<'mcx>,
    rel: RelId,
    ndistinct: f64,
    isdefault: bool,
}

// add_unique_group_var (selfuncs.c): drop exact duplicates; among known-equal
// vars of different rels keep the one with the smaller ndistinct.
fn add_unique_group_var<'mcx>(
    run: &mut PlannerRun<'mcx>,
    varinfos: &mut mcx::PgVec<'mcx, GroupVarInfo<'mcx>>,
    node: Node<'mcx>,
    vardata: &VariableStatData<'mcx>,
) -> PgResult<()> {
    let (mut ndistinct, isdefault) = get_variable_numdistinct(run, vardata);
    // pgrcolumnar no-stats group-key ndistinct (pgrust-only divergence, scoped to
    // grouping/DISTINCT estimation -- consts::DEFAULT_PGRCOLUMNAR_GROUP_NDISTINCT_RATIO
    // provenance): pgrcolumnar cannot ANALYZE, so a defaulted 200 here starves
    // hash-finalize parallel agg at 100M scale. isdefault is preserved so
    // SELFLAG_USED_DEFAULT consumers still see the truth.
    if isdefault {
        if let Some(rel) = vardata.rel {
            let r = run.root.rel(rel);
            if r.amflags & types_pathnodes::AMFLAG_PGRCOLUMNAR != 0 && r.tuples > 0.0 {
                // Prefer the footer's ingest-time per-column NDV (whole-stream
                // HLL — the same count a footer-backed ANALYZE harvests into
                // stadistinct; plancat stashes it on the rel). Only a plain
                // Var of this rel maps to a footer column; anything else (or
                // a footer-less part / unknown column) keeps the flat-ratio
                // fallback below.
                let footer_ndv = node.as_var().and_then(|v| {
                    (v.varno == r.relid as i32 && v.varlevelsup == 0 && v.varattno >= 1)
                        .then(|| r.pgrcolumnar_col_ndv.get(v.varattno as usize - 1))
                        .flatten()
                        .copied()
                        .filter(|&d| d > 0)
                });
                if let Some(d) = footer_ndv {
                    ndistinct = crate::costsize::clamp_row_est((d as f64).min(r.tuples));
                } else {
                    let ratio = crate::costsize::gucs::pgrcolumnar_group_ndistinct_ratio();
                    if ratio > 0.0 {
                        ndistinct = crate::costsize::clamp_row_est(r.tuples * ratio);
                    }
                }
            }
        }
    }
    // remove_nulling_relids: Vars only carry outer-join nulling relids here,
    // so stripping to empty matches C's outer_join_rels removal. Nulled Vars
    // inside larger grouped expressions keep theirs (such expressions can
    // only reach this list via expression stats, whose match already
    // requires the exact form).
    let node = match node.as_var() {
        Some(v) if !v.varnullingrels.is_empty() => {
            let stripped = types_nodes::primnodes::Var {
                varnullingrels: types_nodes::Bitmapset::empty(),
                ..*v
            };
            Node::mk(run.mcx, stripped)?
        }
        _ => node,
    };
    let rel = vardata.rel.expect("grouping expr has a base rel");
    let mut i = 0;
    while i < varinfos.len() {
        if types_nodes::equal(node, varinfos[i].node) {
            return Ok(());
        }
        if varinfos[i].rel != rel
            && crate::equivclass::exprs_known_equal(run, node, varinfos[i].node, 0)
        {
            if varinfos[i].ndistinct <= ndistinct {
                return Ok(());
            }
            varinfos.remove(i);
            continue;
        }
        i += 1;
    }
    varinfos.push(GroupVarInfo { node, rel, ndistinct, isdefault });
    Ok(())
}

fn estimate_num_groups_core<'mcx>(
    run: &mut PlannerRun<'mcx>,
    group_exprs: &[(NodeId, Node<'mcx>)],
    input_rows: f64,
    pgset: Option<&[i32]>,
    mut estinfo_used_default: Option<&mut bool>,
) -> PgResult<f64> {
    let input_rows = crate::costsize::clamp_row_est(input_rows);
    if group_exprs.is_empty() || pgset.is_some_and(|s| s.is_empty()) {
        return Ok(1.0);
    }

    let mcx = run.mcx;
    let mut varinfos: mcx::PgVec<'_, GroupVarInfo<'mcx>> = mcx::PgVec::new_in(mcx);
    let mut numdistinct = 1.0f64;
    let mut srf_multiplier = 1.0f64;
    for (listidx, &(id, node)) in group_exprs.iter().enumerate() {
        if pgset.is_some_and(|s| !s.contains(&(listidx as i32))) {
            continue;
        }
        // SRFs are estimated as scalars here; the end result is scaled up by
        // the largest SRF rowcount instead.
        let this_srf_multiplier = crate::costsize::expression_returns_set_rows(node)?;
        if srf_multiplier < this_srf_multiplier {
            srf_multiplier = this_srf_multiplier;
        }
        if crate::costsize::expr_type_typmod(node).0 == BOOLOID {
            numdistinct *= 2.0;
            continue;
        }
        // If examine_variable deduces anything (expression-index stats,
        // provable uniqueness), the whole expression is one variable.
        let vardata = examine_variable(run, id, node, 0)?;
        if vardata.stats.is_some() || vardata.isunique {
            add_unique_group_var(run, &mut varinfos, node, &vardata)?;
            continue;
        }
        let vars_here = vars::pull_var_clause(
            mcx,
            node,
            vars::PVC_RECURSE_AGGREGATES | vars::PVC_RECURSE_WINDOWFUNCS | vars::PVC_RECURSE_PLACEHOLDERS,
        )?;
        if vars_here.is_nil() {
            // A Var-free item is a constant (ignorable) or volatile (every
            // input row is its own group).
            if clauses::contain_volatile_functions(node)? {
                return Ok(input_rows);
            }
            continue;
        }
        for v in &vars_here {
            let vid = run.intern_expr(v);
            let vardata = examine_variable(run, vid, v, 0)?;
            add_unique_group_var(run, &mut varinfos, v, &vardata)?;
        }
    }
    if varinfos.is_empty() {
        let numdistinct = (numdistinct * srf_multiplier).ceil();
        return Ok(numdistinct.clamp(1.0, input_rows));
    }

    let mut remaining = varinfos;
    while !remaining.is_empty() {
        let rel_id = remaining[0].rel;
        let mut reldistinct = 1.0f64;
        let mut relmaxndistinct = 1.0f64;
        let mut relvarcount = 0usize;
        let mut rest: mcx::PgVec<'_, GroupVarInfo<'mcx>> = mcx::PgVec::new_in(mcx);
        let mut relvars: mcx::PgVec<'_, GroupVarInfo<'mcx>> = mcx::PgVec::new_in(mcx);
        for vi in remaining {
            if vi.rel == rel_id {
                relvars.push(vi);
            } else {
                rest.push(vi);
            }
        }
        // estimate_multivariate_ndistinct loop (selfuncs.c): consume vars
        // and expressions covered by ndistinct extended statistics first.
        if relvars.len() > 1 && !run.root.rel(rel_id).statlist.is_empty() {
            while !relvars.is_empty() {
                let nodes: Vec<Node<'mcx>> = relvars.iter().map(|vi| vi.node).collect();
                let Some((mvndistinct, consumed)) =
                    crate::extended_stats::estimate_multivariate_ndistinct(run, rel_id, &nodes)?
                else {
                    break;
                };
                reldistinct *= mvndistinct;
                if relmaxndistinct < mvndistinct {
                    relmaxndistinct = mvndistinct;
                }
                relvarcount += 1;
                let mut kept: mcx::PgVec<'_, GroupVarInfo<'mcx>> = mcx::PgVec::new_in(mcx);
                for (i, vi) in relvars.into_iter().enumerate() {
                    if !consumed[i] {
                        kept.push(vi);
                    }
                }
                relvars = kept;
            }
        }
        for vi in relvars {
            reldistinct *= vi.ndistinct;
            if relmaxndistinct < vi.ndistinct {
                relmaxndistinct = vi.ndistinct;
            }
            relvarcount += 1;
            if vi.isdefault {
                if let Some(flag) = estinfo_used_default.as_deref_mut() {
                    *flag = true;
                }
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

    let numdistinct = (numdistinct * srf_multiplier).ceil();
    Ok(numdistinct.clamp(1.0, input_rows))
}

// eqjoinsel (selfuncs.c).
pub fn eqjoinsel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    operator: u32,
    args: &[NodeId],
    jointype: types_pathnodes::JoinType,
    sjinfo: Option<&types_pathnodes::SpecialJoinInfo<'mcx>>,
    collation: Oid,
) -> PgResult<f64> {
    assert!(args.len() == 2, "eqjoinsel (selfuncs.c): non-binary clause");
    let opfuncoid = lsyscache::get_opcode(operator)?;
    let sj_jointype = sjinfo.map_or(jointype, |sj| sj.jointype);
    let left = *run.root.expr_node(args[0]);
    let right = *run.root.expr_node(args[1]);
    let vardata1 = examine_variable(run, args[0], left, 0)?;
    let vardata2 = examine_variable(run, args[1], right, 0)?;
    let (nd1, isdefault1) = get_variable_numdistinct(run, &vardata1);
    let (nd2, isdefault2) = get_variable_numdistinct(run, &vardata2);

    let get_mcv_stats = vardata1.stats.is_some()
        && vardata2.stats.is_some()
        && vardata1.slot(STATISTIC_KIND_MCV, 0).is_some()
        && vardata2.slot(STATISTIC_KIND_MCV, 0).is_some();
    let have_mcvs1 = get_mcv_stats && statistic_proc_security_check(&vardata1, opfuncoid)?;
    let have_mcvs2 = get_mcv_stats && statistic_proc_security_check(&vardata2, opfuncoid)?;

    let selec_inner = eqjoinsel_inner(
        run,
        opfuncoid,
        collation,
        &vardata1,
        &vardata2,
        nd1,
        nd2,
        have_mcvs1,
        have_mcvs2,
    )?;
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
                eqjoinsel_semi(
                    run, opfuncoid, collation, &vardata1, &vardata2, nd1, nd2, isdefault1,
                    isdefault2, have_mcvs1, have_mcvs2, inner_rel,
                )?
            } else {
                let commop = lsyscache::get_commutator(operator)?;
                let commopfuncoid =
                    if commop != 0 { lsyscache::get_opcode(commop)? } else { 0 };
                eqjoinsel_semi(
                    run, commopfuncoid, collation, &vardata2, &vardata1, nd2, nd1, isdefault2,
                    isdefault1, have_mcvs2, have_mcvs1, inner_rel,
                )?
            };
            semi.min(inner_rows * selec_inner)
        }
        other => panic!("eqjoinsel (selfuncs.c): jointype {other}"),
    };
    Ok(clamp_probability(selec))
}

// eqjoinsel_inner (selfuncs.c).
#[allow(clippy::too_many_arguments)]
fn eqjoinsel_inner(
    run: &PlannerRun<'_>,
    opfuncoid: Oid,
    collation: Oid,
    vardata1: &VariableStatData<'_>,
    vardata2: &VariableStatData<'_>,
    nd1: f64,
    nd2: f64,
    have_mcvs1: bool,
    have_mcvs2: bool,
) -> PgResult<f64> {
    if have_mcvs1 && have_mcvs2 {
        let sslot1 = vardata1.slot(STATISTIC_KIND_MCV, 0).expect("have_mcvs1");
        let sslot2 = vardata2.slot(STATISTIC_KIND_MCV, 0).expect("have_mcvs2");
        // Torn slots can pair values with a shorter numbers array; only the
        // paired prefix carries MCV entries. Well-formed slots have equal
        // lengths, making these exactly C's nvalues-bounded arrays.
        let n1 = sslot1.values()?.len().min(sslot1.numbers()?.len());
        let n2 = sslot2.values()?.len().min(sslot2.numbers()?.len());
        let values1 = &sslot1.values()?[..n1];
        let numbers1 = &sslot1.numbers()?[..n1];
        let values2 = &sslot2.values()?[..n2];
        let numbers2 = &sslot2.numbers()?[..n2];
        let nullfrac1 = vardata1.nullfrac();
        let nullfrac2 = vardata2.nullfrac();

        let mut eqproc = fmgr_core::fmgr_info(opfuncoid)?;
        let mut hasmatch1: mcx::PgVec<'_, bool> = mcx::PgVec::new_in(run.mcx);
        hasmatch1.extend(core::iter::repeat_n(false, values1.len()));
        let mut hasmatch2: mcx::PgVec<'_, bool> = mcx::PgVec::new_in(run.mcx);
        hasmatch2.extend(core::iter::repeat_n(false, values2.len()));

        let mut matchprodfreq = 0.0f64;
        let mut nmatches = 0i32;
        for i in 0..values1.len() {
            for j in 0..values2.len() {
                if hasmatch2[j] {
                    continue;
                }
                if types_fmgr::function_call2_coll(&mut eqproc, collation, values1[i], values2[j])?
                    .as_bool()
                {
                    hasmatch1[i] = true;
                    hasmatch2[j] = true;
                    // C accumulates the float4 product (f32 multiply).
                    matchprodfreq += (numbers1[i] * numbers2[j]) as f64;
                    nmatches += 1;
                    break;
                }
            }
        }
        matchprodfreq = clamp_probability(matchprodfreq);
        let mut matchfreq1 = 0.0f64;
        let mut unmatchfreq1 = 0.0f64;
        for i in 0..values1.len() {
            if hasmatch1[i] {
                matchfreq1 += numbers1[i] as f64;
            } else {
                unmatchfreq1 += numbers1[i] as f64;
            }
        }
        matchfreq1 = clamp_probability(matchfreq1);
        unmatchfreq1 = clamp_probability(unmatchfreq1);
        let mut matchfreq2 = 0.0f64;
        let mut unmatchfreq2 = 0.0f64;
        for j in 0..values2.len() {
            if hasmatch2[j] {
                matchfreq2 += numbers2[j] as f64;
            } else {
                unmatchfreq2 += numbers2[j] as f64;
            }
        }
        matchfreq2 = clamp_probability(matchfreq2);
        unmatchfreq2 = clamp_probability(unmatchfreq2);

        let otherfreq1 = clamp_probability(1.0 - nullfrac1 - matchfreq1 - unmatchfreq1);
        let otherfreq2 = clamp_probability(1.0 - nullfrac2 - matchfreq2 - unmatchfreq2);

        let mut totalsel1 = matchprodfreq;
        if nd2 > values2.len() as f64 {
            totalsel1 += unmatchfreq1 * otherfreq2 / (nd2 - values2.len() as f64);
        }
        if nd2 > nmatches as f64 {
            totalsel1 += otherfreq1 * (otherfreq2 + unmatchfreq2) / (nd2 - nmatches as f64);
        }
        let mut totalsel2 = matchprodfreq;
        if nd1 > values1.len() as f64 {
            totalsel2 += unmatchfreq2 * otherfreq1 / (nd1 - values1.len() as f64);
        }
        if nd1 > nmatches as f64 {
            totalsel2 += otherfreq2 * (otherfreq1 + unmatchfreq1) / (nd1 - nmatches as f64);
        }

        Ok(if totalsel1 < totalsel2 { totalsel1 } else { totalsel2 })
    } else {
        let nullfrac1 = vardata1.nullfrac();
        let nullfrac2 = vardata2.nullfrac();
        let mut selec = (1.0 - nullfrac1) * (1.0 - nullfrac2);
        if nd1 > nd2 {
            selec /= nd1;
        } else {
            selec /= nd2;
        }
        Ok(selec)
    }
}

// neqjoinsel (selfuncs.c).
pub fn neqjoinsel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    operator: u32,
    args: &[NodeId],
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo<'mcx>>,
    collation: Oid,
) -> PgResult<f64> {
    if jointype == types_pathnodes::JOIN_SEMI || jointype == types_pathnodes::JOIN_ANTI {
        let sjinfo = sjinfo.expect("SEMI/ANTI neqjoinsel has an sjinfo");
        let (vardata1, vardata2, reversed) = get_join_variables(run, args, sjinfo)?;
        let nullfrac =
            if reversed { vardata2.nullfrac() } else { vardata1.nullfrac() };
        return Ok(1.0 - nullfrac);
    }
    let eqop = lsyscache::get_negator(operator)?;
    let result = if eqop != 0 {
        eqjoinsel(run, eqop, args, jointype, sjinfo, collation)?
    } else {
        DEFAULT_EQ_SEL
    };
    Ok(1.0 - result)
}

// get_join_variables (selfuncs.c); returns (vardata1, vardata2, reversed).
pub(crate) fn get_join_variables<'mcx>(
    run: &mut PlannerRun<'mcx>,
    args: &[NodeId],
    sjinfo: &SpecialJoinInfo<'mcx>,
) -> PgResult<(VariableStatData<'mcx>, VariableStatData<'mcx>, bool)> {
    assert!(args.len() == 2, "get_join_variables (selfuncs.c): non-binary clause");
    let left = *run.root.expr_node(args[0]);
    let right = *run.root.expr_node(args[1]);
    let vardata1 = examine_variable(run, args[0], left, 0)?;
    let vardata2 = examine_variable(run, args[1], right, 0)?;
    let rel_subset = |rel: Option<RelId>, side: &types_pathnodes::Relids<'mcx>| {
        rel.is_some_and(|r| crate::relnode::relids_is_subset(&run.root.rel(r).relids, side))
    };
    let join_is_reversed = rel_subset(vardata1.rel, &sjinfo.syn_righthand)
        || rel_subset(vardata2.rel, &sjinfo.syn_lefthand);
    Ok((vardata1, vardata2, join_is_reversed))
}

pub const DEFAULT_MATCHING_SEL: f64 = 0.010;

// eqjoinsel_semi (selfuncs.c).
#[allow(clippy::too_many_arguments)]
fn eqjoinsel_semi(
    run: &PlannerRun<'_>,
    opfuncoid: Oid,
    collation: Oid,
    vardata1: &VariableStatData<'_>,
    vardata2: &VariableStatData<'_>,
    _nd1: f64,
    _nd2: f64,
    isdefault1: bool,
    isdefault2: bool,
    have_mcvs1: bool,
    have_mcvs2: bool,
    inner_rel: RelId,
) -> PgResult<f64> {
    let mut nd1 = _nd1;
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

    if have_mcvs1 && have_mcvs2 && opfuncoid != 0 {
        let sslot1 = vardata1.slot(STATISTIC_KIND_MCV, 0).expect("have_mcvs1");
        let sslot2 = vardata2.slot(STATISTIC_KIND_MCV, 0).expect("have_mcvs2");
        // Same torn-slot pairing rule as eqjoinsel_inner; values2 has no
        // frequency reads, so it keeps its full length (C's nvalues2).
        let n1 = sslot1.values()?.len().min(sslot1.numbers()?.len());
        let values1 = &sslot1.values()?[..n1];
        let numbers1 = &sslot1.numbers()?[..n1];
        let values2 = sslot2.values()?;
        let nullfrac1 = vardata1.nullfrac();

        // C's Min(int, double) truncates back to int.
        let clamped_nvalues2 = ((values2.len() as f64).min(nd2)) as usize;

        let mut eqproc = fmgr_core::fmgr_info(opfuncoid)?;
        let mut hasmatch1: mcx::PgVec<'_, bool> = mcx::PgVec::new_in(run.mcx);
        hasmatch1.extend(core::iter::repeat_n(false, values1.len()));
        let mut hasmatch2: mcx::PgVec<'_, bool> = mcx::PgVec::new_in(run.mcx);
        hasmatch2.extend(core::iter::repeat_n(false, clamped_nvalues2));

        let mut nmatches = 0i32;
        for i in 0..values1.len() {
            for j in 0..clamped_nvalues2 {
                if hasmatch2[j] {
                    continue;
                }
                if types_fmgr::function_call2_coll(&mut eqproc, collation, values1[i], values2[j])?
                    .as_bool()
                {
                    hasmatch1[i] = true;
                    hasmatch2[j] = true;
                    nmatches += 1;
                    break;
                }
            }
        }
        let mut matchfreq1 = 0.0f64;
        for i in 0..values1.len() {
            if hasmatch1[i] {
                matchfreq1 += numbers1[i] as f64;
            }
        }
        matchfreq1 = clamp_probability(matchfreq1);

        let uncertainfrac = if !isdefault1 && !isdefault2 {
            nd1 -= nmatches as f64;
            nd2 -= nmatches as f64;
            if nd1 <= nd2 || nd2 < 0.0 {
                1.0
            } else {
                nd2 / nd1
            }
        } else {
            0.5
        };
        let uncertain = clamp_probability(1.0 - matchfreq1 - nullfrac1);
        Ok(matchfreq1 + uncertainfrac * uncertain)
    } else {
        let nullfrac1 = vardata1.nullfrac();
        Ok(if !isdefault1 && !isdefault2 {
            if nd1 <= nd2 || nd2 < 0.0 {
                1.0 - nullfrac1
            } else {
                (nd2 / nd1) * (1.0 - nullfrac1)
            }
        } else {
            0.5 * (1.0 - nullfrac1)
        })
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

// estimate_multivariate_bucketsize (selfuncs.c). Returns (otherclauses,
// innerbucketsize). DIVERGENCE: nullingrels are cleared only on bare Vars,
// not inside larger expressions (C remove_nulling_relids), so outer-join
// hash keys over stat expressions fall back to per-var estimates.
pub fn estimate_multivariate_bucketsize<'mcx>(
    run: &mut PlannerRun<'mcx>,
    _inner: RelId,
    hashclauses: &[RinfoId],
) -> PgResult<(mcx::PgVec<'mcx, RinfoId>, f64)> {
    let mut otherclauses: mcx::PgVec<'mcx, RinfoId> = mcx::PgVec::new_in(run.mcx);
    if hashclauses.len() <= 1 {
        otherclauses.extend(hashclauses.iter().copied());
        return Ok((otherclauses, 1.0));
    }
    let mut ndistinct = 1.0f64;
    let mut clauses: mcx::PgVec<'mcx, RinfoId> = mcx::PgVec::new_in(run.mcx);
    clauses.extend(hashclauses.iter().copied());

    while !clauses.is_empty() {
        let mut group_relid: i32 = -1;
        let mut group_rel: Option<RelId> = None;
        let mut varinfos: mcx::PgVec<'mcx, Node<'mcx>> = mcx::PgVec::new_in(run.mcx);
        let mut origin_rinfos: mcx::PgVec<'mcx, RinfoId> = mcx::PgVec::new_in(run.mcx);
        let mut next_clauses: mcx::PgVec<'mcx, RinfoId> = mcx::PgVec::new_in(run.mcx);

        let cur: mcx::PgVec<'mcx, RinfoId> = clauses;
        for &rid in cur.iter() {
            let (outer_is_left, clause_id, relid_opt) = {
                let ri = run.root.rinfo(rid);
                let relids =
                    if ri.outer_is_left { &ri.right_relids } else { &ri.left_relids };
                (
                    ri.outer_is_left,
                    ri.clause,
                    crate::relnode::relids_singleton_member(relids),
                )
            };
            let has_stats = relid_opt.is_some_and(|relid| {
                run.root.simple_rel_array[relid as usize]
                    .is_some_and(|r| !run.root.rel(r).statlist.is_empty())
            });
            if !has_stats {
                otherclauses.push(rid);
                continue;
            }
            let relid = relid_opt.unwrap();
            if group_relid < 0 {
                let rte = run.rte(relid as usize);
                if !matches!(rte.relkind, b'r' | b'm' | b'f' | b'p') {
                    otherclauses.push(rid);
                    continue;
                }
                group_relid = relid;
                group_rel = Some(crate::relnode::find_base_rel(&run.root, relid));
            } else if group_relid != relid {
                // Not part of the group being formed: retry next iteration.
                next_clauses.push(rid);
                continue;
            }

            let clause = *run.root.expr_node(clause_id);
            let op = clause.as_op_expr().expect("hashclause is an OpExpr");
            let mut expr = op.args.nth(if outer_is_left { 1 } else { 0 });
            // remove_nulling_relids over the hash key: a bare Var is the only
            // shape that can match attno-keyed extended stats, so a cleared
            // copy suffices; non-Var exprs compare raw (never covered below).
            if let Some(v) = expr.as_var() {
                if v.varlevelsup == 0 && !v.varnullingrels.is_empty() {
                    expr = Node::mk(
                        run.mcx,
                        types_nodes::primnodes::Var {
                            varno: v.varno,
                            varattno: v.varattno,
                            vartype: v.vartype,
                            vartypmod: v.vartypmod,
                            varcollid: v.varcollid,
                            varnullingrels: types_nodes::Bitmapset::empty(),
                            varlevelsup: v.varlevelsup,
                            varreturningtype: v.varreturningtype,
                            varnosyn: v.varnosyn,
                            varattnosyn: v.varattnosyn,
                            location: v.location,
                        },
                    )?;
                }
            }
            let mut is_duplicate = false;
            for &vi in varinfos.iter() {
                if types_nodes::equal(expr, vi) {
                    is_duplicate = true;
                    break;
                }
            }
            if is_duplicate {
                continue;
            }
            varinfos.push(expr);
            origin_rinfos.push(rid);
        }
        clauses = next_clauses;

        if varinfos.len() < 2 {
            otherclauses.extend(origin_rinfos.iter().copied());
            continue;
        }
        let group_rel = group_rel.expect("group_rel set with varinfos");

        // estimate_multivariate_ndistinct consumption loop; `estimated`
        // tracks which varinfos a statistics object covered.
        let mut estimated: mcx::PgVec<'mcx, bool> = mcx::PgVec::new_in(run.mcx);
        estimated.extend(core::iter::repeat_n(false, varinfos.len()));
        loop {
            let mut nodes: Vec<Node<'mcx>> = Vec::new();
            let mut node_idx: Vec<usize> = Vec::new();
            for (i, &vi) in varinfos.iter().enumerate() {
                if estimated[i] {
                    continue;
                }
                nodes.push(vi);
                node_idx.push(i);
            }
            let Some((mvndistinct, consumed)) = crate::extended_stats::
                estimate_multivariate_ndistinct(run, group_rel, &nodes)?
            else {
                break;
            };
            if ndistinct < mvndistinct {
                ndistinct = mvndistinct;
            }
            debug_assert!(ndistinct >= 1.0);
            for (k, &i) in node_idx.iter().enumerate() {
                if consumed[k] {
                    estimated[i] = true;
                }
            }
        }
        for (i, &rid) in origin_rinfos.iter().enumerate() {
            if !estimated[i] {
                otherclauses.push(rid);
            }
        }
    }

    Ok((otherclauses, 1.0 / ndistinct))
}

// estimate_hash_bucket_stats (selfuncs.c) -> (mcv_freq, bucketsize_frac).
pub fn estimate_hash_bucket_stats<'mcx>(
    run: &mut PlannerRun<'mcx>,
    hashkey: Node<'mcx>,
    nbuckets: f64,
) -> PgResult<(f64, f64)> {
    let node_id = run.intern_expr(hashkey);
    let vardata = examine_variable(run, node_id, hashkey, 0)?;

    let mut mcv_freq = 0.0f64;
    if let Some(sslot) = vardata.slot(STATISTIC_KIND_MCV, 0) {
        if let Some(&first) = sslot.numbers()?.first() {
            mcv_freq = first as f64;
        }
    }

    let (mut ndistinct, isdefault) = get_variable_numdistinct(run, &vardata);
    if isdefault {
        return Ok((mcv_freq, 0.1f64.max(mcv_freq)));
    }

    let stanullfrac = vardata.nullfrac();
    let avgfreq = (1.0 - stanullfrac) / ndistinct;

    if let Some(rel) = vardata.rel {
        let (tuples, rows) = (run.root.rel(rel).tuples, run.root.rel(rel).rows);
        if tuples > 0.0 {
            ndistinct *= rows / tuples;
            ndistinct = crate::costsize::clamp_row_est(ndistinct);
        }
    }

    let mut estfract = if ndistinct > nbuckets {
        1.0 / nbuckets
    } else {
        1.0 / ndistinct
    };

    if avgfreq > 0.0 && mcv_freq > avgfreq {
        estfract *= mcv_freq / avgfreq;
    }

    if estfract < 1.0e-6 {
        estfract = 1.0e-6;
    } else if estfract > 1.0 {
        estfract = 1.0;
    }
    Ok((mcv_freq, estfract))
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
    if !statistic_proc_security_check(vardata, opfuncoid)? {
        return Ok(None);
    }
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

const TEXTOID: u32 = 25;
const NAMEOID: u32 = 19;
const BPCHAROID: u32 = 1042;
const BYTEAOID: u32 = 17;
const BOOLEAN_EQ_OP: Oid = 91;
pub const DEFAULT_MATCH_SEL: f64 = 0.005;

const PARTIAL_WILDCARD_SEL: f64 = 2.0;


struct PrefixConst {
    consttype: Oid,
    constvalue: Datum,
}

// VARDATA_ANY/VARSIZE_ANY_EXHDR: planner consts and stats values carry 1B or
// 4B-U images (bound-param datumCopy preserves short forms; the asserts keep
// toast forms loud).
pub(crate) fn varlena_datum_payload<'a>(value: Datum) -> &'a [u8] {
    let p = value.as_usize() as *const u8;
    debug_assert!(!p.is_null());
    // SAFETY: by-ref inline varlena datum, readable for its header size.
    unsafe {
        let b0 = *p;
        if b0 & 0x01 == 0x01 {
            assert!(b0 != 0x01, "varlena_datum_payload: external toast datum");
            let total = ((b0 >> 1) & 0x7F) as usize;
            core::slice::from_raw_parts(p.add(1), total - 1)
        } else {
            assert!(b0 & 0x03 == 0, "varlena_datum_payload: compressed datum");
            datum::VarlenaRef::from_ptr(p).data()
        }
    }
}

// PG_DETOAST_DATUM's short-header arm: layout-sensitive readers (array/range
// deserializers) need 4B offsets, so a short const expands into `mcx`.
pub(crate) fn varlena_image_any<'a>(mcx: mcx::Mcx<'a>, value: Datum) -> PgResult<&'a [u8]> {
    let p = value.as_usize() as *const u8;
    debug_assert!(!p.is_null());
    // SAFETY: by-ref inline varlena datum, readable for its header size.
    unsafe {
        let b0 = *p;
        if b0 & 0x01 == 0x01 {
            assert!(b0 != 0x01, "varlena_image_any: external toast datum");
            let total = ((b0 >> 1) & 0x7F) as usize;
            let payload = core::slice::from_raw_parts(p.add(1), total - 1);
            let mut img = mcx::vec_with_capacity_in(mcx, total - 1 + datum::varlena::VARHDRSZ)?;
            mcx::vec_append_bytes(
                &mut img,
                &datum::varlena::set_varsize_4b(total - 1 + datum::varlena::VARHDRSZ),
            )?;
            mcx::vec_append_bytes(&mut img, payload)?;
            Ok(img.leak())
        } else {
            assert!(b0 & 0x03 == 0, "varlena_image_any: compressed datum");
            Ok(datum::VarlenaRef::from_ptr(p).as_bytes())
        }
    }
}


// scalararraysel (selfuncs.c). The typcache eq_opr probe gates the
// scalararraysel_containment try; below that, isEquality/isInequality are
// re-derived from the estimator oid exactly as C's second-chance test does.
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
    // C aggressively reduces both sides to constants.
    let leftop = clauses::estimate_expression_value(run.mcx, clause.args.nth(0))?;
    let rightop = clauses::estimate_expression_value(run.mcx, clause.args.nth(1))?;

    let (rightop_type, _) = crate::costsize::expr_type_typmod(rightop);
    let nominal_element_type = lsyscache::get_base_element_type(rightop_type)?;
    if nominal_element_type == 0 {
        return Ok(0.5);
    }
    let nominal_element_collation = expr_collation(rightop);
    let rightop = strip_array_coercion(rightop);

    // Containment only believes the element type's default btree equality
    // operator (or its negator) — those are what array containment uses.
    let mut is_equality = false;
    let mut is_inequality = false;
    let eq_opr =
        typcache::lookup_type_cache(nominal_element_type, typcache::TYPECACHE_EQ_OPR)?.eq_opr();
    if eq_opr != 0 {
        if operator == eq_opr {
            is_equality = true;
        } else if lsyscache::get_negator(operator)? == eq_opr {
            is_inequality = true;
        }
    }
    if (is_equality || is_inequality) && !is_join_clause {
        let s1 = crate::array_selfuncs::scalararraysel_containment(
            run,
            leftop,
            rightop,
            nominal_element_type,
            is_equality,
            use_or,
            varrelid,
        )?;
        if s1 >= 0.0 {
            return Ok(s1);
        }
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
        let img = varlena_image_any(run.mcx, c.constvalue)?;
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
        // C: a dummy CaseTestExpr rightop, assumed 10 elements; no
        // disjoint-probability shortcut on this arm.
        let dummy = Node::mk(
            run.mcx,
            types_nodes::CaseTestExpr {
                typeId: nominal_element_type,
                typeMod: -1,
                collation: clause.inputcollid,
            },
        )?;
        let dummy_id = run.intern_expr(dummy);
        let args = [left_id, dummy_id];
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
        s1 = if use_or { 0.0 } else { 1.0 };
        for _ in 0..10 {
            if use_or {
                s1 = s1 + s2 - s1 * s2;
            } else {
                s1 *= s2;
            }
        }
    }

    Ok(clamp_probability(s1))
}

fn strip_array_coercion<'mcx>(mut node: Node<'mcx>) -> Node<'mcx> {
    while let Some(r) = node.as_relabel_type() {
        node = r.arg;
    }
    node
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


fn expr_collation(node: Node<'_>) -> Oid {
    match node.node_tag() {
        NodeTag::T_Const => node.as_const().unwrap().constcollid,
        NodeTag::T_ArrayExpr => node.as_array_expr().unwrap().array_collid,
        NodeTag::T_Var => node.as_var().unwrap().varcollid,
        _ => 0,
    }
}


// estimate_array_length (selfuncs.c); run=None mirrors C's NULL root and
// skips the stats arm.
pub fn estimate_array_length<'mcx>(
    run: Option<&mut PlannerRun<'mcx>>,
    node: Node<'mcx>,
) -> PgResult<f64> {
    let node = strip_array_coercion(node);
    if let Some(c) = node.as_const() {
        if c.constisnull {
            return Ok(0.0);
        }
        // Header-relative reads work for 1B and 4B images alike (bound-param
        // array consts can be short-form).
        let body = varlena_datum_payload(c.constvalue);
        let rd = |off: usize| i32::from_ne_bytes(body[off..off + 4].try_into().unwrap());
        let ndim = rd(0);
        let mut n = 1f64;
        for i in 0..ndim as usize {
            n *= rd(12 + 4 * i) as f64;
        }
        if ndim == 0 {
            n = 0.0;
        }
        return Ok(n);
    }
    if let Some(a) = node.as_array_expr().filter(|a| !a.multidims) {
        return Ok(a.elements.len() as f64);
    }
    if let Some(run) = run {
        // The DECHIST slot's last stanumber is the average distinct element
        // count.
        let node_id = run.intern_expr(node);
        let vardata = examine_variable(run, node_id, node, 0)?;
        if vardata.stats.is_some() {
            if let Some(slot) = vardata.slot(STATISTIC_KIND_DECHIST, 0) {
                let numbers = slot.numbers()?;
                if !numbers.is_empty() {
                    let nelem =
                        crate::costsize::clamp_row_est(numbers[numbers.len() - 1] as f64);
                    if nelem > 0.0 {
                        return Ok(nelem);
                    }
                }
            }
        }
    }
    // Default guess; must match scalararraysel.
    Ok(10.0)
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
        // C evaluates via raw fcinfo: a NULL result counts as no-match
        // (jsonb @@ can return NULL), never an error.
        let armed_test = |opproc: &mut FmgrInfo, v: Datum| -> PgResult<bool> {
            let (a0, a1) = if varonleft { (v, constval) } else { (constval, v) };
            let mut fcinfo = types_fmgr::LocalFcinfo::<2>::fresh(collation);
            // SAFETY: smcx outlives this single call.
            unsafe { fcinfo.set_result_mcx(smcx) };
            fcinfo.set_arg(0, a0);
            fcinfo.set_arg(1, a1);
            let result = opproc.invoke(&mut fcinfo)?;
            Ok(!fcinfo.isnull && result.as_bool())
        };

        let stats_usable =
            vardata.stats.is_some() && statistic_proc_security_check(&vardata, opcode)?;
        let (mut mcvsel, mut mcvsum) = (0.0f64, 0.0f64);
        if let Some(sslot) = vardata.slot(STATISTIC_KIND_MCV, 0).filter(|_| stats_usable) {
            // Torn-slot pairing rule (see mcv_selectivity): only values
            // paired with a frequency count.
            for (&v, &n) in sslot.values()?.iter().zip(sslot.numbers()?.iter()) {
                if armed_test(&mut opproc, v)? {
                    mcvsel += n as f64;
                }
            }
        }

        let (hist_selec, hist_size) = {
            let mut hs = -1.0f64;
            let mut n = 0usize;
            if let Some(sslot) = vardata.slot(STATISTIC_KIND_HISTOGRAM, 0).filter(|_| stats_usable)
            {
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

// matchingsel (selfuncs.c); DEFAULT_MATCHING_SEL = 2 * DEFAULT_EQ_SEL.
pub fn matchingsel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    operator: Oid,
    args: &[NodeId],
    varrelid: i32,
    collation: Oid,
) -> PgResult<f64> {
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

    let (nentries, npartial, search_mode) =
        gin::gincost_extract_query(opfamily, opcintype, query, strategy)?;

    if nentries <= 0 && search_mode == GIN_SEARCH_MODE_DEFAULT {
        return Ok(false);
    }
    counts.partial_entries += npartial as f64;
    counts.exact_entries += (nentries - npartial) as f64;
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

// gincost_scalararrayopexpr (selfuncs.c).
fn gincost_scalararrayopexpr<'mcx>(
    run: &mut PlannerRun<'mcx>,
    opfamily: Oid,
    opcintype: Oid,
    clause: Node<'mcx>,
    num_index_entries: f64,
    counts: &mut GinQualCounts,
) -> PgResult<bool> {
    let mcx = run.mcx;
    let saop = clause.as_scalar_array_op_expr().expect("ScalarArrayOpExpr");
    debug_assert!(saop.useOr);
    let clause_op = saop.opno;
    let mut rightop = clauses::estimate_expression_value(mcx, saop.args.nth(1))?;
    if let Some(r) = rightop.as_relabel_type() {
        rightop = r.arg;
    }
    let Some(c) = rightop.as_const() else {
        counts.exact_entries += 1.0;
        counts.search_entries += 1.0;
        counts.array_scans *= estimate_array_length(Some(run), rightop)?;
        return Ok(true);
    };
    if c.constisnull {
        return Ok(false);
    }
    let img = varlena_image_any(mcx, c.constvalue)?;
    let elemtype = arrayfuncs::arr_elemtype(img);
    let (elmlen, elmbyval, elmalign) = lsyscache::get_typlenbyvalalign(elemtype)?;
    let (values, nulls) =
        arrayfuncs::deconstruct_array(mcx, img, elmlen as i32, elmbyval, elmalign as u8, true)?;

    let mut arraycounts = GinQualCounts::default();
    let mut num_possible = 0i32;
    for (i, &v) in values.iter().enumerate() {
        // NULL can't match anything, so ignore, as the executor will.
        if nulls[i] {
            continue;
        }
        let mut elemcounts = GinQualCounts::default();
        if gincost_pattern(opfamily, opcintype, clause_op, v, &mut elemcounts)? {
            num_possible += 1;
            if elemcounts.att_has_full_scan && !elemcounts.att_has_normal_scan {
                elemcounts.partial_entries = 0.0;
                elemcounts.exact_entries = num_index_entries;
                elemcounts.search_entries = num_index_entries;
            }
            arraycounts.partial_entries += elemcounts.partial_entries;
            arraycounts.exact_entries += elemcounts.exact_entries;
            arraycounts.search_entries += elemcounts.search_entries;
        }
    }
    if num_possible == 0 {
        return Ok(false);
    }
    counts.partial_entries += arraycounts.partial_entries / num_possible as f64;
    counts.exact_entries += arraycounts.exact_entries / num_possible as f64;
    counts.search_entries += arraycounts.search_entries / num_possible as f64;
    counts.array_scans *= num_possible as f64;
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

    let selectivity_quals = add_predicate_to_index_quals(run, path_id, &index_quals)?;
    let index_selectivity = crate::clausesel::clauselist_selectivity(
        run,
        &selectivity_quals,
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
                        if !gincost_scalararrayopexpr(
                            run,
                            opfamily0,
                            opcintype0,
                            clause,
                            num_entries,
                            &mut counts,
                        )? {
                            match_possible = false;
                            break 'quals;
                        }
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

// strip_all_phvs_deep / contain_placeholder_walker / strip_all_phvs_mutator
// (selfuncs.c): PHVs are transparent for statistics lookup.
fn contain_placeholder(node: types_nodes::Node<'_>) -> bool {
    struct W {
        found: bool,
    }
    impl<'mcx> nodes_core::NodeWalker<'mcx> for W {
        fn visit(&mut self, node: types_nodes::Node<'mcx>) -> PgResult<bool> {
            if node.node_tag() == types_nodes::NodeTag::T_PlaceHolderVar {
                self.found = true;
                return Ok(true);
            }
            nodes_core::expression_tree_walker(node, self)
        }
    }
    let mut w = W { found: false };
    use nodes_core::NodeWalker as _;
    let _ = w.visit(node);
    w.found
}

fn strip_all_phvs_mutator<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    node: types_nodes::Node<'mcx>,
) -> PgResult<types_nodes::Node<'mcx>> {
    fn mutate<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        node: types_nodes::Node<'mcx>,
    ) -> PgResult<Option<types_nodes::Node<'mcx>>> {
        if let Some(phv) = node.as_place_holder_var() {
            let inner = mutate(mcx, phv.phexpr)?.unwrap_or(phv.phexpr);
            return Ok(Some(inner));
        }
        clauses::expression_tree_mutator(mcx, node, &mut |n| mutate(mcx, n))
    }
    Ok(mutate(mcx, node)?.unwrap_or(node))
}
