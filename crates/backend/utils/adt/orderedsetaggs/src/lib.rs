// orderedsetaggs.c. Divergences from C: the per-query state caches the sort
// metadata recipe in fn_extra (plain std collections; fn_mcxt has no Rust
// analogue), and the tupdesc/slots/equality programs are rebuilt per group in
// the group context; the per-group state is a global-alloc Box freed by the
// AggRegisterCallback shutdown (C pallocs it in gcontext).

use std::rc::Rc;

use ::adt_datetime::Interval;
use ::datum::{Datum, NullableDatum};
use ::mcx::{Mcx, PgVec};
use ::tuplesort::{Tuplesort, TUPLESORT_NONE, TUPLESORT_RANDOMACCESS};
use ::types_core::catalog::{FLOAT8OID, INTERVALOID};
use ::types_core::Oid;
use ::types_error::{PgError, PgResult, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE};
use ::types_fmgr::{
    byref_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, LocalFcinfo,
    PGFunction,
};
use ::types_nodes::primnodes::{Aggref, AGGKIND_HYPOTHETICAL, AGGKIND_NORMAL};
use ::types_slot::{SlotData, TupleSlotKind};
use ::types_tuple::TupleDescData;

const INT4OID: Oid = 23;
// pg_operator.dat: int4lt / int4eq.
const INT4_LESS_OPERATOR: Oid = 97;
const INT4_EQUAL_OPERATOR: Oid = 96;

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: false, retset: false, func }
}

pub const ORDEREDSETAGGS_BUILTINS: &[FmgrBuiltin] = &[
    b(3970, "ordered_set_transition", 2, fc_ordered_set_transition),
    b(3971, "ordered_set_transition_multi", 2, fc_ordered_set_transition_multi),
    b(3973, "percentile_disc_final", 3, fc_percentile_disc_final),
    b(3975, "percentile_cont_float8_final", 2, fc_percentile_cont_float8_final),
    b(3977, "percentile_cont_interval_final", 2, fc_percentile_cont_interval_final),
    b(3979, "percentile_disc_multi_final", 3, fc_percentile_disc_multi_final),
    b(3981, "percentile_cont_float8_multi_final", 2, fc_percentile_cont_float8_multi_final),
    b(3983, "percentile_cont_interval_multi_final", 2, fc_percentile_cont_interval_multi_final),
    b(3985, "mode_final", 2, fc_mode_final),
    b(3987, "hypothetical_rank_final", 2, fc_hypothetical_rank_final),
    b(3989, "hypothetical_percent_rank_final", 2, fc_hypothetical_percent_rank_final),
    b(3991, "hypothetical_cume_dist_final", 2, fc_hypothetical_cume_dist_final),
    b(3993, "hypothetical_dense_rank_final", 2, fc_hypothetical_dense_rank_final),
];

// OSAPerQueryState's allocator-free slice, cached in the transfn's fn_extra
// (std collections justified: once-per-query, boxed by fn_extra itself).
#[derive(Clone)]
struct QMeta {
    rescan_needed: bool,
    is_hypothetical: bool,
    sort_col_type: Oid,
    typ_len: i16,
    typ_by_val: bool,
    typ_align: i8,
    sort_operator: Oid,
    eq_operator: Oid,
    sort_collation: Oid,
    sort_nulls_first: bool,
    sort_col_idx: Vec<i16>,
    sort_ops: Vec<Oid>,
    eq_ops: Vec<Oid>,
    sort_colls: Vec<Oid>,
    sort_nulls: Vec<bool>,
    // (typid, typmod, collation) per aggregated column, for the tupdesc.
    col_recipe: Vec<(Oid, i32, Oid)>,
}

// OSAPerGroupState + the group-context-backed objects C keeps per query.
// The 'static lifetimes are erased aggcontext borrows: the shutdown callback
// frees this Box before nodeagg resets the aggcontext.
struct OsaGroupState {
    q: QMeta,
    gcx: Mcx<'static>,
    sort: Option<Tuplesort>,
    number_of_rows: i64,
    sort_done: bool,
    tupdesc: Option<Rc<TupleDescData<'static>>>,
    insert_slot: Option<SlotData<'static>>,
    fetch_slot: Option<SlotData<'static>>,
    extra_slot: Option<SlotData<'static>>,
    equalfn: Option<FmgrInfo>,
    compare_tuple: Option<::mcx::PgBox<'static, ::execexpr::ExprState<'static>>>,
}

unsafe fn osa_shutdown(arg: *mut ()) {
    // SAFETY: registration passed a Box::into_raw pointer, fired once.
    drop(unsafe { Box::from_raw(arg as *mut OsaGroupState) });
}

#[track_caller]
#[cold]
#[inline(never)]
fn non_agg_context() -> Box<PgError> {
    Box::new(PgError::error("ordered-set aggregate called in non-aggregate context"))
}

#[track_caller]
#[cold]
#[inline(never)]
fn elog_error(msg: &'static str) -> Box<PgError> {
    Box::new(PgError::error(msg))
}

fn sortgroupclause_tle<'a, 'mcx>(
    aggref: &'a Aggref<'mcx>,
    sortgroupref: types_core::Index,
) -> &'a types_nodes::primnodes::TargetEntry<'mcx> {
    aggref
        .args
        .iter()
        .find_map(|n| {
            let t = n.as_target_entry().expect("Aggref.args cell");
            (t.ressortgroupref == sortgroupref).then_some(t)
        })
        .expect("ORDER BY expression not found in Aggref.args")
}

fn build_qmeta(fcinfo: &Fcinfo, use_tuples: bool) -> PgResult<QMeta> {
    // SAFETY: fcinfo.context is nodeagg's live AggStateNode; the cur-agg slot
    // holds a query-lifetime Aggref.
    let aggref = unsafe { ::nodeagg::agg_get_aggref(fcinfo) }.ok_or_else(non_agg_context)?;
    if aggref.aggkind == AGGKIND_NORMAL {
        return Err(elog_error(
            "ordered-set aggregate support function called for non-ordered-set aggregate",
        ));
    }
    // SAFETY: as above.
    let rescan_needed = unsafe { ::nodeagg::agg_state_is_shared(fcinfo) };
    let is_hypothetical = aggref.aggkind == AGGKIND_HYPOTHETICAL;

    let mut q = QMeta {
        rescan_needed,
        is_hypothetical,
        sort_col_type: 0,
        typ_len: 0,
        typ_by_val: false,
        typ_align: 0,
        sort_operator: 0,
        eq_operator: 0,
        sort_collation: 0,
        sort_nulls_first: false,
        sort_col_idx: Vec::new(),
        sort_ops: Vec::new(),
        eq_ops: Vec::new(),
        sort_colls: Vec::new(),
        sort_nulls: Vec::new(),
        col_recipe: Vec::new(),
    };

    if use_tuples {
        for sc_node in &aggref.aggorder {
            let scl = sc_node.as_sort_group_clause().expect("aggorder cell");
            let tle = sortgroupclause_tle(aggref, scl.tleSortGroupRef);
            assert!(scl.sortop != 0, "sortless SortGroupClause survived the parser");
            q.sort_col_idx.push(tle.resno);
            q.sort_ops.push(scl.sortop);
            q.eq_ops.push(scl.eqop);
            q.sort_colls.push(nodes_core::expr_collation(tle.expr));
            q.sort_nulls.push(scl.nulls_first);
        }
        if is_hypothetical {
            q.sort_col_idx.push(aggref.args.len() as i16 + 1);
            q.sort_ops.push(INT4_LESS_OPERATOR);
            q.eq_ops.push(INT4_EQUAL_OPERATOR);
            q.sort_colls.push(0);
            q.sort_nulls.push(false);
        }
        for tle_node in &aggref.args {
            let tle = tle_node.as_target_entry().expect("Aggref.args cell");
            q.col_recipe.push((
                nodes_core::expr_type(tle.expr),
                nodes_core::expr_typmod(tle.expr),
                nodes_core::expr_collation(tle.expr),
            ));
        }
        if is_hypothetical {
            q.col_recipe.push((INT4OID, -1, 0));
        }
    } else {
        if aggref.aggorder.len() != 1 || is_hypothetical {
            return Err(elog_error(
                "ordered-set aggregate support function does not support multiple aggregated \
                 columns",
            ));
        }
        let scl = aggref
            .aggorder
            .nth(0)
            .as_sort_group_clause()
            .expect("aggorder cell");
        let tle = sortgroupclause_tle(aggref, scl.tleSortGroupRef);
        assert!(scl.sortop != 0, "sortless SortGroupClause survived the parser");
        q.sort_col_type = nodes_core::expr_type(tle.expr);
        q.sort_operator = scl.sortop;
        q.eq_operator = scl.eqop;
        q.sort_collation = nodes_core::expr_collation(tle.expr);
        q.sort_nulls_first = scl.nulls_first;
        let (typ_len, typ_by_val, typ_align) = lsyscache::get_typlenbyvalalign(q.sort_col_type)?;
        q.typ_len = typ_len;
        q.typ_by_val = typ_by_val;
        q.typ_align = typ_align;
    }
    Ok(q)
}

fn build_tupdesc<'mcx>(mcx: Mcx<'mcx>, q: &QMeta) -> PgResult<Rc<TupleDescData<'mcx>>> {
    let mut desc = tupdesc::CreateTemplateTupleDesc(mcx, q.col_recipe.len() as i32)?;
    for (i, &(typid, typmod, coll)) in q.col_recipe.iter().enumerate() {
        let resno = i as i16 + 1;
        let name = if q.is_hypothetical && i == q.col_recipe.len() - 1 {
            Some("flag")
        } else {
            None
        };
        tupdesc::TupleDescInitEntry(&mut desc, resno, name, typid, typmod, 0)?;
        tupdesc::TupleDescInitEntryCollation(&mut desc, resno, coll);
    }
    Ok(Rc::new(desc))
}

fn ordered_set_startup(
    flinfo: &mut FmgrInfo,
    fcinfo: &Fcinfo,
    use_tuples: bool,
) -> PgResult<*mut OsaGroupState> {
    // SAFETY: fcinfo.context is nodeagg's live AggStateNode.
    let gcx = unsafe { fcinfo.agg_context() }.ok_or_else(non_agg_context)?;
    if !flinfo.has_fn_extra() {
        let q = build_qmeta(fcinfo, use_tuples)?;
        flinfo.set_fn_extra(q);
    }
    let q: QMeta = flinfo
        .fn_extra_ref::<QMeta>()
        .expect("ordered_set_startup fn_extra holds QMeta")
        .clone();

    // SAFETY: lifetime erasure only; every gcx-backed object in the group
    // state dies in osa_shutdown, which nodeagg fires before the aggcontext
    // resets (AggStateNode::reset contract).
    let gcx: Mcx<'static> = unsafe { core::mem::transmute(gcx) };
    let work_mem = init_small::globals::work_mem();
    let sortopt =
        if q.rescan_needed { TUPLESORT_NONE | TUPLESORT_RANDOMACCESS } else { TUPLESORT_NONE };

    let mut st = OsaGroupState {
        q,
        gcx,
        sort: None,
        number_of_rows: 0,
        sort_done: false,
        tupdesc: None,
        insert_slot: None,
        fetch_slot: None,
        extra_slot: None,
        equalfn: None,
        compare_tuple: None,
    };
    if use_tuples {
        let desc = build_tupdesc(gcx, &st.q)?;
        st.insert_slot = Some(exectuples::make_tuple_table_slot(
            gcx,
            TupleSlotKind::Virtual,
            Some(desc.clone()),
        ));
        st.fetch_slot = Some(exectuples::make_tuple_table_slot(
            gcx,
            TupleSlotKind::MinimalTuple,
            Some(desc.clone()),
        ));
        st.sort = Some(Tuplesort::begin_heap(
            desc.clone(),
            &st.q.sort_col_idx,
            &st.q.sort_ops,
            &st.q.sort_colls,
            &st.q.sort_nulls,
            work_mem,
            sortopt,
        )?);
        st.tupdesc = Some(desc);
    } else {
        st.sort = Some(Tuplesort::begin_datum(
            st.q.sort_col_type,
            st.q.sort_operator,
            st.q.sort_collation,
            st.q.sort_nulls_first,
            work_mem,
            sortopt,
        )?);
    }

    let ptr = Box::into_raw(Box::new(st));
    // SAFETY: ptr stays valid until the callback fires exactly once.
    if let Err(e) = unsafe { ::nodeagg::agg_register_callback(fcinfo, osa_shutdown, ptr.cast()) }
    {
        // SAFETY: registration failed, so this frame is still the sole owner.
        drop(unsafe { Box::from_raw(ptr) });
        return Err(e);
    }
    Ok(ptr)
}

// SAFETY contract shared by every final function: a non-null arg 0 is the
// OsaGroupState pointer our transition functions returned, sole live access.
unsafe fn group_state<'a>(fcinfo: &Fcinfo) -> &'a mut OsaGroupState {
    // SAFETY: caller contract.
    unsafe { &mut *(fcinfo.arg(0).as_usize() as *mut OsaGroupState) }
}

pub fn fc_ordered_set_transition(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("ordered_set_transition: NULL flinfo");
    let st = if fcinfo.argisnull(0) {
        ordered_set_startup(flinfo, fcinfo, false)?
    } else {
        fcinfo.arg(0).as_usize() as *mut OsaGroupState
    };
    // SAFETY: startup's Box, live until the group shutdown callback.
    let st = unsafe { &mut *st };
    if !fcinfo.argisnull(1) {
        st.sort.as_mut().expect("live sortstate").putdatum(fcinfo.arg(1), false)?;
        st.number_of_rows += 1;
    }
    Ok(Datum::from_usize(st as *mut OsaGroupState as usize))
}

pub fn fc_ordered_set_transition_multi(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("ordered_set_transition_multi: NULL flinfo");
    let st = if fcinfo.argisnull(0) {
        ordered_set_startup(flinfo, fcinfo, true)?
    } else {
        fcinfo.arg(0).as_usize() as *mut OsaGroupState
    };
    // SAFETY: startup's Box, live until the group shutdown callback.
    let st = unsafe { &mut *st };
    let nargs = fcinfo.nargs as usize - 1;
    {
        let slot = st.insert_slot.as_mut().expect("tuple-path insert slot");
        exectuples::exec_clear_tuple(slot, st.gcx);
        let base = slot.base_mut();
        for i in 0..nargs {
            base.tts_values[i] = fcinfo.arg(i + 1);
            base.tts_isnull[i] = fcinfo.argisnull(i + 1);
        }
        let mut i = nargs;
        if st.q.is_hypothetical {
            base.tts_values[i] = Datum::from_i32(0);
            base.tts_isnull[i] = false;
            i += 1;
        }
        debug_assert_eq!(i, st.q.col_recipe.len());
        exectuples::exec_store_virtual_tuple(slot);
    }
    {
        let OsaGroupState { sort, insert_slot, gcx, .. } = st;
        sort.as_mut().unwrap().puttupleslot(insert_slot.as_mut().unwrap(), *gcx)?;
    }
    st.number_of_rows += 1;
    Ok(Datum::from_usize(st as *mut OsaGroupState as usize))
}

fn null_result(fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    fcinfo.isnull = true;
    Ok(Datum::null())
}

// C errmsg uses %g; the values a query can produce render identically under
// {} except NaN (C prints "nan").
fn percentile_range_error(p: f64) -> Box<PgError> {
    let rendered = if p.is_nan() { "nan".to_string() } else { format!("{p}") };
    Box::new(
        PgError::error(format!("percentile value {rendered} is not between 0 and 1"))
            .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
    )
}

fn start_scan(st: &mut OsaGroupState) -> PgResult<()> {
    let sort = st.sort.as_mut().expect("live sortstate");
    if !st.sort_done {
        sort.performsort()?;
        st.sort_done = true;
    } else {
        sort.rescan()?;
    }
    Ok(())
}

// tuplesort_getdatum(copy=true): by-ref values are copied into the caller's
// per-tuple memory (C copies too; the sort dies at the group shutdown).
fn getdatum_copied(st: &mut OsaGroupState, mcx: Mcx<'_>) -> PgResult<Option<NullableDatum>> {
    let Some(nd) = st.sort.as_mut().expect("live sortstate").getdatum(true)? else {
        return Ok(None);
    };
    if nd.isnull || st.q.typ_by_val {
        return Ok(Some(nd));
    }
    // SAFETY: non-null by-ref datum in live sort memory.
    let copied = unsafe { ::execexpr::agg_datum_copy(mcx, nd.value, st.q.typ_len)? };
    Ok(Some(NullableDatum { value: copied, isnull: false }))
}

pub fn fc_percentile_disc_final(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    if fcinfo.argisnull(1) {
        return null_result(fcinfo);
    }
    let percentile = fcinfo.arg(1).as_f64();
    if !(0.0..=1.0).contains(&percentile) || percentile.is_nan() {
        return Err(percentile_range_error(percentile));
    }
    if fcinfo.argisnull(0) {
        return null_result(fcinfo);
    }
    // SAFETY: final-arg contract (group_state).
    let st = unsafe { group_state(fcinfo) };
    if st.number_of_rows == 0 {
        return null_result(fcinfo);
    }
    start_scan(st)?;

    // Smallest K with K/N >= percentile: skip K-1 rows, return the next.
    let rownum = (percentile * st.number_of_rows as f64).ceil() as i64;
    debug_assert!(rownum <= st.number_of_rows);
    if rownum > 1 && !st.sort.as_mut().unwrap().skiptuples(rownum - 1, true)? {
        return Err(elog_error("missing row in percentile_disc"));
    }
    let Some(nd) = getdatum_copied(st, fcinfo.result_mcx())? else {
        return Err(elog_error("missing row in percentile_disc"));
    };
    if nd.isnull {
        return null_result(fcinfo);
    }
    Ok(nd.value)
}

// SAFETY contract: `d` is a non-null interval datum (16-byte image).
unsafe fn interval_from_datum(d: Datum) -> Interval {
    let p = d.as_usize() as *const u8;
    // SAFETY: caller contract.
    unsafe {
        Interval {
            time: (p as *const i64).read_unaligned(),
            day: (p.add(8) as *const i32).read_unaligned(),
            month: (p.add(12) as *const i32).read_unaligned(),
        }
    }
}

fn interval_datum(mcx: Mcx<'_>, iv: Interval) -> PgResult<Datum> {
    let mut img = [0u8; 16];
    img[..8].copy_from_slice(&iv.time.to_ne_bytes());
    img[8..12].copy_from_slice(&iv.day.to_ne_bytes());
    img[12..].copy_from_slice(&iv.month.to_ne_bytes());
    byref_result(mcx, &img)
}

enum Lerp {
    Float8,
    Interval,
}

impl Lerp {
    fn apply(&self, mcx: Mcx<'_>, lo: Datum, hi: Datum, pct: f64) -> PgResult<Datum> {
        match self {
            Lerp::Float8 => {
                let (loval, hival) = (lo.as_f64(), hi.as_f64());
                Ok(Datum::from_f64(loval + pct * (hival - loval)))
            }
            Lerp::Interval => {
                // SAFETY: the sort column type is interval (expect_type check).
                let (lo, hi) = unsafe { (interval_from_datum(lo), interval_from_datum(hi)) };
                let diff = ::adt_timestamp::interval::interval_mi(&hi, &lo)?;
                let mul = ::adt_timestamp::interval::interval_mul(&diff, pct)?;
                interval_datum(mcx, ::adt_timestamp::interval::interval_pl(&mul, &lo)?)
            }
        }
    }
}

fn percentile_cont_final_common(
    fcinfo: &mut Fcinfo,
    expect_type: Oid,
    lerp: Lerp,
) -> PgResult<Datum> {
    if fcinfo.argisnull(1) {
        return null_result(fcinfo);
    }
    let percentile = fcinfo.arg(1).as_f64();
    if !(0.0..=1.0).contains(&percentile) || percentile.is_nan() {
        return Err(percentile_range_error(percentile));
    }
    if fcinfo.argisnull(0) {
        return null_result(fcinfo);
    }
    // SAFETY: final-arg contract (group_state).
    let st = unsafe { group_state(fcinfo) };
    if st.number_of_rows == 0 {
        return null_result(fcinfo);
    }
    debug_assert_eq!(expect_type, st.q.sort_col_type);
    start_scan(st)?;

    let first_row = (percentile * (st.number_of_rows - 1) as f64).floor() as i64;
    let second_row = (percentile * (st.number_of_rows - 1) as f64).ceil() as i64;
    debug_assert!(first_row < st.number_of_rows);

    if !st.sort.as_mut().unwrap().skiptuples(first_row, true)? {
        return Err(elog_error("missing row in percentile_cont"));
    }
    let mcx = fcinfo.result_mcx();
    let Some(first) = getdatum_copied(st, mcx)? else {
        return Err(elog_error("missing row in percentile_cont"));
    };
    if first.isnull {
        return null_result(fcinfo);
    }
    if first_row == second_row {
        return Ok(first.value);
    }
    let Some(second) = getdatum_copied(st, mcx)? else {
        return Err(elog_error("missing row in percentile_cont"));
    };
    if second.isnull {
        return null_result(fcinfo);
    }
    let proportion = percentile * (st.number_of_rows - 1) as f64 - first_row as f64;
    lerp.apply(mcx, first.value, second.value, proportion)
}

pub fn fc_percentile_cont_float8_final(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    percentile_cont_final_common(fcinfo, FLOAT8OID, Lerp::Float8)
}

pub fn fc_percentile_cont_interval_final(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    percentile_cont_final_common(fcinfo, INTERVALOID, Lerp::Interval)
}

struct PctInfo {
    first_row: i64,
    second_row: i64,
    proportion: f64,
    idx: usize,
}

fn setup_pct_info<'mcx>(
    mcx: Mcx<'mcx>,
    percentiles: &[Datum],
    nulls: &[bool],
    rowcount: i64,
    continuous: bool,
) -> PgResult<PgVec<'mcx, PctInfo>> {
    let mut out: PgVec<'mcx, PctInfo> = mcx::vec_with_capacity_in(mcx, percentiles.len())?;
    for (i, (&d, &isnull)) in percentiles.iter().zip(nulls.iter()).enumerate() {
        if isnull {
            out.push(PctInfo { first_row: 0, second_row: 0, proportion: 0.0, idx: i });
            continue;
        }
        let p = d.as_f64();
        if !(0.0..=1.0).contains(&p) || p.is_nan() {
            return Err(percentile_range_error(p));
        }
        if continuous {
            let base = p * (rowcount - 1) as f64;
            out.push(PctInfo {
                first_row: 1 + base.floor() as i64,
                second_row: 1 + base.ceil() as i64,
                proportion: base - base.floor(),
                idx: i,
            });
        } else {
            // Smallest K with K/N >= percentile, but not less than 1.
            let row = ((p * rowcount as f64).ceil() as i64).max(1);
            out.push(PctInfo { first_row: row, second_row: row, proportion: 0.0, idx: i });
        }
    }
    out.sort_unstable_by(|a, b| (a.first_row, a.second_row).cmp(&(b.first_row, b.second_row)));
    Ok(out)
}

// The deconstructed float8[] percentile argument plus its detoasted image
// (the output array copies the input's shape).
fn percentile_array<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &Fcinfo,
) -> PgResult<(&'mcx [u8], PgVec<'mcx, Datum>, PgVec<'mcx, bool>)> {
    // SAFETY: a non-null argument is a live array varlena.
    let raw = unsafe {
        let p = fcinfo.arg_ptr(1);
        core::slice::from_raw_parts(p, ::types_tuple::varatt::varsize_any(p))
    };
    let img: &'mcx [u8] = ::detoast_seams::detoast_attr::call(mcx, raw)?.leak();
    let (d, n) = arrayfuncs::construct::deconstruct_array_builtin(mcx, img, FLOAT8OID, true)?;
    Ok((img, d, n))
}

fn array_shape(param: &[u8]) -> (i32, [i32; 6], [i32; 6]) {
    let rd = |off: usize| i32::from_ne_bytes(param[off..off + 4].try_into().unwrap());
    let ndim = rd(4);
    let mut dims = [0i32; 6];
    let mut lbs = [0i32; 6];
    let n = ndim.clamp(0, 6) as usize;
    for i in 0..n {
        dims[i] = rd(16 + 4 * i);
        lbs[i] = rd(16 + 4 * n + 4 * i);
    }
    (ndim, dims, lbs)
}

pub fn fc_percentile_disc_multi_final(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    if fcinfo.argisnull(0) {
        return null_result(fcinfo);
    }
    // SAFETY: final-arg contract (group_state).
    let st = unsafe { group_state(fcinfo) };
    if st.number_of_rows == 0 {
        return null_result(fcinfo);
    }
    if fcinfo.argisnull(1) {
        return null_result(fcinfo);
    }
    let mcx = fcinfo.result_mcx();
    let (param, pd, pn) = percentile_array(mcx, fcinfo)?;
    let num = pd.len();
    if num == 0 {
        let empty = arrayfuncs::construct::construct_empty_array(mcx, st.q.sort_col_type)?;
        return byref_result(mcx, &empty);
    }
    let pct = setup_pct_info(mcx, &pd, &pn, st.number_of_rows, false)?;

    let mut result_datum: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, num)?;
    result_datum.resize(num, Datum::null());
    let mut result_isnull: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, num)?;
    result_isnull.resize(num, false);

    let mut i = 0;
    while i < num && pct[i].first_row == 0 {
        result_isnull[pct[i].idx] = true;
        i += 1;
    }
    if i < num {
        start_scan(st)?;
        let mut rownum: i64 = 0;
        let mut val = NullableDatum::null();
        while i < num {
            let target_row = pct[i].first_row;
            let idx = pct[i].idx;
            if target_row > rownum {
                if !st.sort.as_mut().unwrap().skiptuples(target_row - rownum - 1, true)? {
                    return Err(elog_error("missing row in percentile_disc"));
                }
                let Some(nd) = getdatum_copied(st, mcx)? else {
                    return Err(elog_error("missing row in percentile_disc"));
                };
                val = nd;
                rownum = target_row;
            }
            result_datum[idx] = val.value;
            result_isnull[idx] = val.isnull;
            i += 1;
        }
    }
    let (ndim, dims, lbs) = array_shape(param);
    let out = arrayfuncs::construct::construct_md_array(
        mcx,
        &result_datum,
        Some(&result_isnull),
        ndim,
        &dims[..ndim as usize],
        &lbs[..ndim as usize],
        st.q.sort_col_type,
        st.q.typ_len as i32,
        st.q.typ_by_val,
        st.q.typ_align as u8,
    )?;
    byref_result(mcx, &out)
}

fn percentile_cont_multi_final_common(
    fcinfo: &mut Fcinfo,
    expect_type: Oid,
    typ_len: i32,
    typ_by_val: bool,
    typ_align: u8,
    lerp: Lerp,
) -> PgResult<Datum> {
    if fcinfo.argisnull(0) {
        return null_result(fcinfo);
    }
    // SAFETY: final-arg contract (group_state).
    let st = unsafe { group_state(fcinfo) };
    if st.number_of_rows == 0 {
        return null_result(fcinfo);
    }
    if fcinfo.argisnull(1) {
        return null_result(fcinfo);
    }
    debug_assert_eq!(expect_type, st.q.sort_col_type);
    let mcx = fcinfo.result_mcx();
    let (param, pd, pn) = percentile_array(mcx, fcinfo)?;
    let num = pd.len();
    if num == 0 {
        let empty = arrayfuncs::construct::construct_empty_array(mcx, st.q.sort_col_type)?;
        return byref_result(mcx, &empty);
    }
    let pct = setup_pct_info(mcx, &pd, &pn, st.number_of_rows, true)?;

    let mut result_datum: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, num)?;
    result_datum.resize(num, Datum::null());
    let mut result_isnull: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, num)?;
    result_isnull.resize(num, false);

    let mut i = 0;
    while i < num && pct[i].first_row == 0 {
        result_isnull[pct[i].idx] = true;
        i += 1;
    }
    if i < num {
        start_scan(st)?;
        let mut rownum: i64 = 0;
        let mut first_val = Datum::null();
        let mut second_val = Datum::null();
        while i < num {
            let PctInfo { first_row, second_row, proportion, idx } = pct[i];
            if first_row > rownum {
                if !st.sort.as_mut().unwrap().skiptuples(first_row - rownum - 1, true)? {
                    return Err(elog_error("missing row in percentile_cont"));
                }
                match getdatum_copied(st, mcx)? {
                    Some(nd) if !nd.isnull => first_val = nd.value,
                    _ => return Err(elog_error("missing row in percentile_cont")),
                }
                rownum = first_row;
                second_val = first_val;
            } else if first_row == rownum {
                first_val = second_val;
            }
            if second_row > rownum {
                match getdatum_copied(st, mcx)? {
                    Some(nd) if !nd.isnull => second_val = nd.value,
                    _ => return Err(elog_error("missing row in percentile_cont")),
                }
                rownum += 1;
            }
            debug_assert_eq!(second_row, rownum);
            result_datum[idx] = if second_row > first_row {
                lerp.apply(mcx, first_val, second_val, proportion)?
            } else {
                first_val
            };
            result_isnull[idx] = false;
            i += 1;
        }
    }
    let (ndim, dims, lbs) = array_shape(param);
    let out = arrayfuncs::construct::construct_md_array(
        mcx,
        &result_datum,
        Some(&result_isnull),
        ndim,
        &dims[..ndim as usize],
        &lbs[..ndim as usize],
        expect_type,
        typ_len,
        typ_by_val,
        typ_align,
    )?;
    byref_result(mcx, &out)
}

pub fn fc_percentile_cont_float8_multi_final(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    percentile_cont_multi_final_common(fcinfo, FLOAT8OID, 8, true, b'd', Lerp::Float8)
}

pub fn fc_percentile_cont_interval_multi_final(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    percentile_cont_multi_final_common(fcinfo, INTERVALOID, 16, false, b'd', Lerp::Interval)
}

pub fn fc_mode_final(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    if fcinfo.argisnull(0) {
        return null_result(fcinfo);
    }
    // SAFETY: final-arg contract (group_state).
    let st = unsafe { group_state(fcinfo) };
    if st.number_of_rows == 0 {
        return null_result(fcinfo);
    }
    if st.equalfn.is_none() {
        st.equalfn = Some(fmgr_core::fmgr_info(lsyscache::get_opcode(st.q.eq_operator)?)?);
    }
    start_scan(st)?;

    let mcx = fcinfo.result_mcx();
    let collation = fcinfo.fncollation;
    // Spilled by-ref values live in recycled slab slots (valid until the
    // next fetch): held values need C's datumCopy shape (retained scratch).
    let copy_held = !st.q.typ_by_val && st.sort.as_ref().expect("live sortstate").spilled();
    let typ_len = st.q.typ_len;
    // std Vec (justified): per-final-call scratch freed on return; a bump
    // PgVec would pin the bytes in the agg output context.
    let mut last_buf: Vec<u8> = Vec::new();
    let mut mode_buf: Vec<u8> = Vec::new();
    let mut mode_val = Datum::null();
    let mut mode_freq: i64 = 0;
    let mut last_val = Datum::null();
    let mut last_val_freq: i64 = 0;
    let mut last_val_is_mode = false;
    let mut last_abbrev = Datum::null();

    fn held(buf: &mut Vec<u8>, val: Datum, typ_len: i16) -> Datum {
        let src = val.as_usize() as *const u8;
        // SAFETY: non-null by-ref datum readable for its full size.
        let size = unsafe {
            if typ_len == -1 {
                ::types_tuple::varatt::varsize_any(src)
            } else {
                typ_len as usize
            }
        };
        buf.clear();
        // SAFETY: reserved; src readable; disjoint.
        unsafe {
            buf.reserve(size);
            core::ptr::copy_nonoverlapping(src, buf.as_mut_ptr(), size);
            buf.set_len(size);
        }
        Datum::from_usize(buf.as_ptr() as usize)
    }

    loop {
        let Some((nd, abbrev)) =
            st.sort.as_mut().expect("live sortstate").getdatum_abbrev(true)?
        else {
            break;
        };
        if nd.isnull {
            continue;
        }
        let val = nd.value;
        if last_val_freq == 0 {
            if copy_held {
                last_val = held(&mut last_buf, val, typ_len);
                mode_val = held(&mut mode_buf, val, typ_len);
            } else {
                mode_val = val;
                last_val = val;
            }
            mode_freq = 1;
            last_val_freq = 1;
            last_val_is_mode = true;
            last_abbrev = abbrev;
        } else {
            let equal = abbrev.as_usize() == last_abbrev.as_usize() && {
                let eq = st.equalfn.as_mut().unwrap();
                let mut fc2 = LocalFcinfo::<2>::fresh(collation);
                // SAFETY: the per-tuple context outlives the call.
                unsafe { fc2.set_result_mcx(mcx) };
                fc2.args[0] = NullableDatum { value: val, isnull: false };
                fc2.args[1] = NullableDatum { value: last_val, isnull: false };
                eq.invoke(&mut fc2)?.as_bool()
            };
            if equal {
                if last_val_is_mode {
                    mode_freq += 1;
                } else {
                    last_val_freq += 1;
                    if last_val_freq > mode_freq {
                        mode_val = if copy_held {
                            held(&mut mode_buf, last_val, typ_len)
                        } else {
                            last_val
                        };
                        mode_freq = last_val_freq;
                        last_val_is_mode = true;
                    }
                }
            } else {
                last_val = if copy_held { held(&mut last_buf, val, typ_len) } else { val };
                // Reusing abbreviated keys avoids equality calls (C ditto).
                last_abbrev = abbrev;
                last_val_freq = 1;
                last_val_is_mode = false;
            }
        }
    }

    if mode_freq == 0 {
        return null_result(fcinfo);
    }
    if st.q.typ_by_val {
        return Ok(mode_val);
    }
    // SAFETY: non-null by-ref datum in live sort memory (C copies too).
    unsafe { ::execexpr::agg_datum_copy(mcx, mode_val, st.q.typ_len) }
}

fn hypothetical_check_argtypes(
    flinfo: &FmgrInfo,
    nargs: usize,
    st: &OsaGroupState,
) -> PgResult<()> {
    let recipe = &st.q.col_recipe;
    if recipe.len() != nargs + 1 || recipe[nargs].0 != INT4OID {
        return Err(elog_error("type mismatch in hypothetical-set function"));
    }
    for (i, &(typid, _, _)) in recipe.iter().take(nargs).enumerate() {
        if funcapi::get_fn_expr_argtype(Some(flinfo), i + 1) != typid {
            return Err(elog_error("type mismatch in hypothetical-set function"));
        }
    }
    Ok(())
}

fn insert_hypothetical_row(
    st: &mut OsaGroupState,
    fcinfo: &Fcinfo,
    nargs: usize,
    flag: i32,
) -> PgResult<()> {
    {
        let slot = st.insert_slot.as_mut().expect("tuple-path insert slot");
        exectuples::exec_clear_tuple(slot, st.gcx);
        let base = slot.base_mut();
        for i in 0..nargs {
            base.tts_values[i] = fcinfo.arg(i + 1);
            base.tts_isnull[i] = fcinfo.argisnull(i + 1);
        }
        base.tts_values[nargs] = Datum::from_i32(flag);
        base.tts_isnull[nargs] = false;
        exectuples::exec_store_virtual_tuple(slot);
    }
    let OsaGroupState { sort, insert_slot, gcx, .. } = st;
    sort.as_mut().unwrap().puttupleslot(insert_slot.as_mut().unwrap(), *gcx)
}

// hypothetical_rank_common (orderedsetaggs.c): (rank, number_of_rows).
fn hypothetical_rank_common(
    flinfo: &FmgrInfo,
    fcinfo: &mut Fcinfo,
    flag: i32,
) -> PgResult<(i64, i64)> {
    let nargs = fcinfo.nargs as usize - 1;
    if fcinfo.argisnull(0) {
        return Ok((1, 0));
    }
    // SAFETY: final-arg contract (group_state).
    let st = unsafe { group_state(fcinfo) };
    let number_of_rows = st.number_of_rows;
    if nargs % 2 != 0 {
        return Err(elog_error("wrong number of arguments in hypothetical-set function"));
    }
    let nargs = nargs / 2;
    hypothetical_check_argtypes(flinfo, nargs, st)?;
    assert!(!st.sort_done, "hypothetical-set aggregate cannot share transition state");

    insert_hypothetical_row(st, fcinfo, nargs, flag)?;
    st.sort.as_mut().unwrap().performsort()?;
    st.sort_done = true;

    let mut rank: i64 = 1;
    loop {
        let OsaGroupState { sort, fetch_slot, gcx, .. } = st;
        let slot = fetch_slot.as_mut().expect("tuple-path fetch slot");
        if !sort.as_mut().unwrap().gettupleslot(true, false, slot, *gcx)? {
            break;
        }
        exectuples::slot_getsomeattrs(slot, nargs as i32 + 1);
        let (d, isnull) = {
            let base = slot.base();
            (base.tts_values[nargs], base.tts_isnull[nargs])
        };
        if !isnull && d.as_i32() != 0 {
            break;
        }
        rank += 1;
    }
    {
        let OsaGroupState { fetch_slot, gcx, .. } = st;
        exectuples::exec_clear_tuple(fetch_slot.as_mut().unwrap(), *gcx);
    }
    Ok((rank, number_of_rows))
}

pub fn fc_hypothetical_rank_final(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("hypothetical_rank_final: NULL flinfo");
    let (rank, _) = hypothetical_rank_common(flinfo, fcinfo, -1)?;
    Ok(Datum::from_i64(rank))
}

pub fn fc_hypothetical_percent_rank_final(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("hypothetical_percent_rank_final: NULL flinfo");
    let (rank, rowcount) = hypothetical_rank_common(flinfo, fcinfo, -1)?;
    if rowcount == 0 {
        return Ok(Datum::from_f64(0.0));
    }
    Ok(Datum::from_f64((rank - 1) as f64 / rowcount as f64))
}

pub fn fc_hypothetical_cume_dist_final(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("hypothetical_cume_dist_final: NULL flinfo");
    let (rank, rowcount) = hypothetical_rank_common(flinfo, fcinfo, 1)?;
    Ok(Datum::from_f64(rank as f64 / (rowcount + 1) as f64))
}

pub fn fc_hypothetical_dense_rank_final(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("hypothetical_dense_rank_final: NULL flinfo");
    let nargs = fcinfo.nargs as usize - 1;
    if fcinfo.argisnull(0) {
        return Ok(Datum::from_i64(1));
    }
    // SAFETY: final-arg contract (group_state).
    let st = unsafe { group_state(fcinfo) };
    if nargs % 2 != 0 {
        return Err(elog_error("wrong number of arguments in hypothetical-set function"));
    }
    let nargs = nargs / 2;
    hypothetical_check_argtypes(flinfo, nargs, st)?;

    // Flag column omitted from the comparison: only flag == 0 rows compare.
    let num_distinct_cols = st.q.sort_col_idx.len() - 1;
    if st.compare_tuple.is_none() {
        let mut eqfuncoids: PgVec<'_, Oid> =
            mcx::vec_with_capacity_in(st.gcx, num_distinct_cols)?;
        for &op in st.q.eq_ops.iter().take(num_distinct_cols) {
            eqfuncoids.push(lsyscache::get_opcode(op)?);
        }
        let desc = st.tupdesc.as_ref().expect("tuple-path tupdesc").clone();
        st.compare_tuple = Some(::execexpr::exec_build_grouping_equal(
            st.gcx,
            &desc,
            &desc,
            &st.q.sort_col_idx[..num_distinct_cols],
            &eqfuncoids,
            &st.q.sort_colls[..num_distinct_cols],
        )?);
        if st.extra_slot.is_none() {
            st.extra_slot = Some(exectuples::make_tuple_table_slot(
                st.gcx,
                TupleSlotKind::MinimalTuple,
                Some(desc),
            ));
        }
    }
    assert!(!st.sort_done, "hypothetical-set aggregate cannot share transition state");

    insert_hypothetical_row(st, fcinfo, nargs, -1)?;
    st.sort.as_mut().unwrap().performsort()?;
    st.sort_done = true;

    let mut rank: i64 = 1;
    let mut duplicate_count: i64 = 0;
    let mut have_prev = false;
    // Alternate the two fetch slots so the previous row stays comparable.
    let mut use_extra = false;
    // Spilled reads recycle slab slots on the next fetch: the held previous
    // row needs an owned copy (C passes copy=true here unconditionally).
    let spilled = st.sort.as_ref().unwrap().spilled();
    loop {
        let OsaGroupState { sort, fetch_slot, extra_slot, compare_tuple, gcx, .. } = st;
        let (cur, prev) = if use_extra {
            (extra_slot.as_mut().unwrap(), fetch_slot.as_mut().unwrap())
        } else {
            (fetch_slot.as_mut().unwrap(), extra_slot.as_mut().unwrap())
        };
        if !sort.as_mut().unwrap().gettupleslot(true, spilled, cur, *gcx)? {
            break;
        }
        exectuples::slot_getsomeattrs(cur, nargs as i32 + 1);
        let (d, isnull) = {
            let base = cur.base();
            (base.tts_values[nargs], base.tts_isnull[nargs])
        };
        if !isnull && d.as_i32() != 0 {
            break;
        }
        if have_prev {
            let mut slots = ::execexpr::EvalSlots {
                scan: None,
                inner: Some(&mut *prev),
                outer: Some(&mut *cur),
            };
            if ::execexpr::exec_qual(compare_tuple.as_deref_mut(), &mut slots)? {
                duplicate_count += 1;
            }
        }
        use_extra = !use_extra;
        have_prev = true;
        rank += 1;
    }
    {
        let OsaGroupState { fetch_slot, extra_slot, gcx, .. } = st;
        exectuples::exec_clear_tuple(fetch_slot.as_mut().unwrap(), *gcx);
        if let Some(es) = extra_slot.as_mut() {
            exectuples::exec_clear_tuple(es, *gcx);
        }
    }
    Ok(Datum::from_i64(rank - duplicate_count))
}
