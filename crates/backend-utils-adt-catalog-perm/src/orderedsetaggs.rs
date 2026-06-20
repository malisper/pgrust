//! Ordered-set aggregate support functions (`orderedsetaggs.c`) — datum-sort path.
//!
//! See the crate docs for the scope split. This module ports the
//! single-aggregated-column ordered-set aggregates that sort bare `Datum`s
//! (percentile_disc / percentile_cont(float8) / mode) plus the generic
//! transition / startup / shutdown plumbing they share.
//!
//! ## Owned-model shape of the C pointers
//!
//! C's `OSAPerQueryState` is cached in `fcinfo->flinfo->fn_extra`; we cache it
//! the same way through the generic `FmgrInfo.set_fn_extra` slot (substrate #1).
//! C's `OSAPerGroupState` is the `internal`-transtype transition value
//! (`PG_RETURN_POINTER(osastate)`) AND the argument to the
//! `ordered_set_shutdown` callback. In the owned model the transition value is a
//! `Datum::Internal(Box<OSAPerGroupState>)` that nodeAgg moves in/out of the
//! call frame; the shutdown callback cannot also receive that same box
//! (`Box<dyn Any>` is not `Clone`, and `ShutdownExprContext` clones the callback
//! arg). So the live `Tuplesortstate` — the only resource `ordered_set_shutdown`
//! releases (`tuplesort_end`, for temp files) — is held in a backend-global side
//! table keyed by a small `SortStateId`, and that id (a plain `Datum` word) is
//! the callback arg. Same "index-into-a-side-table instead of a raw pointer"
//! discipline the executor uses for `EcxtId` / `SlotId`.

use alloc::boxed::Box;
use alloc::string::ToString;
use core::any::Any;
use core::cell::RefCell;

use mcx::{alloc_in, MemoryContext, Mcx, PgBox, PgVec};
use types_core::Oid;
use types_error::{PgError, PgResult};
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

/// The fmgr-ABI bare word (`types_datum::Datum`) — what `arg(i).value` carries
/// and what a `PGFunction` returns.
use types_datum::Datum as Word;
/// The canonical value-bearing `Datum` (`types_tuple`) — what the tuplesort
/// seams put/get.
use types_tuple::backend_access_common_heaptuple::Datum as CDatum;

use backend_executor_nodeAgg_aggapi_seams as aggapi;
use backend_utils_sort_tuplesort_seams as tsort;

const FLOAT8OID: Oid = 701;
const INTERVALOID: Oid = 1186;
const AGG_CONTEXT_AGGREGATE: i32 = 1;

pub mod mode;
pub mod multi;
pub mod tuple;

// ===========================================================================
// Small helpers (the PG_* macro surface).
// ===========================================================================

pub(crate) fn raise(err: PgError) -> ! {
    std::panic::panic_any(err)
}

#[inline]
pub(crate) fn ok<T>(r: PgResult<T>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => raise(e),
    }
}

/// `PG_ARGISNULL(i)`.
#[inline]
pub(crate) fn arg_isnull(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo.arg(i).map(|d| d.isnull).unwrap_or(true)
}

/// `PG_GETARG_FLOAT8(i)`.
#[inline]
pub(crate) fn arg_float8(fcinfo: &FunctionCallInfoBaseData, i: usize) -> f64 {
    fcinfo.arg(i).map(|d| d.value.as_f64()).unwrap_or(0.0)
}

/// The raw by-value word of arg `i` (`PG_GETARG_DATUM(i)` for a by-value type).
#[inline]
pub(crate) fn arg_word(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Word {
    fcinfo.arg(i).map(|d| d.value).unwrap_or(Word::from_usize(0))
}

/// `PG_RETURN_NULL()`.
#[inline]
pub(crate) fn ret_null(fcinfo: &mut FunctionCallInfoBaseData) -> Word {
    fcinfo.set_result_null(true);
    Word::from_usize(0)
}

/// `PG_RETURN_POINTER(osastate)` — hand the per-group state back as `internal`.
#[inline]
pub(crate) fn ret_internal(fcinfo: &mut FunctionCallInfoBaseData, state: Box<dyn Any>) -> Word {
    fcinfo.set_ref_result(RefPayload::Internal(state));
    Word::from_usize(0)
}

pub(crate) fn percentile_range_error(p: f64) -> PgError {
    PgError::error(alloc::format!("percentile value {p} is not between 0 and 1"))
        .with_sqlstate(types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

// ===========================================================================
// Live-sortstate side table.
// ===========================================================================

pub(crate) type SortStateId = i64;
pub(crate) type LiveSort = PgBox<'static, types_nodes::Tuplesortstate<'static>>;

thread_local! {
    /// The live `Tuplesortstate`s, addressed by [`SortStateId`]. A `None` slot
    /// is an already-ended (tombstoned) sort. Backend-thread-confined, exactly
    /// like the executor's EState pools.
    static SORTSTATES: RefCell<alloc::vec::Vec<Option<LiveSort>>> =
        const { RefCell::new(alloc::vec::Vec::new()) };
}

pub(crate) fn register_sortstate(s: LiveSort) -> SortStateId {
    SORTSTATES.with(|t| {
        let mut t = t.borrow_mut();
        let id = t.len() as SortStateId;
        t.push(Some(s));
        id
    })
}

/// Run `f` with a mutable borrow of the live sort for `id` (C: dereference
/// `osastate->sortstate`). Panics on an already-ended sort.
pub(crate) fn with_sortstate_mut<R>(
    id: SortStateId,
    f: impl FnOnce(&mut types_nodes::Tuplesortstate<'static>) -> R,
) -> R {
    SORTSTATES.with(|t| {
        let mut t = t.borrow_mut();
        let slot = t
            .get_mut(id as usize)
            .and_then(|s| s.as_mut())
            .expect("ordered-set sortstate used after tuplesort_end");
        f(slot)
    })
}

/// `tuplesort_end(osastate->sortstate); osastate->sortstate = NULL;` — idempotent.
pub(crate) fn end_sortstate(id: SortStateId) -> PgResult<()> {
    let taken = SORTSTATES.with(|t| {
        let mut t = t.borrow_mut();
        t.get_mut(id as usize).and_then(|s| s.take())
    });
    if let Some(s) = taken {
        tsort::tuplesort_end::call(s)?;
    }
    Ok(())
}

// ===========================================================================
// Per-query / per-group state.
// ===========================================================================

/// `OSAPerQueryState` — datum-path fields only.
#[derive(Clone)]
pub(crate) struct OSAPerQueryState {
    pub(crate) rescan_needed: bool,
    pub(crate) sort_col_type: Oid,
    /// `int16 typLen` / `char typAlign` — kept (the C `OSAPerQueryState` fields)
    /// for the shape-preserving `construct_md_array` multi path; the 1-D
    /// `construct_array_builtin_v` used here does not consume them.
    #[allow(dead_code)]
    pub(crate) typ_len: i16,
    pub(crate) typ_by_val: bool,
    #[allow(dead_code)]
    pub(crate) typ_align: i8,
    pub(crate) sort_operator: Oid,
    pub(crate) eq_operator: Oid,
    pub(crate) sort_collation: Oid,
    pub(crate) sort_nulls_first: bool,
    /// Cached `mode()` equality function OID (`get_opcode(eqOperator)`), lazily
    /// resolved on first `mode_final` call. `0` is C's `!OidIsValid(fn_oid)`.
    pub(crate) equal_fn_oid: Oid,
    /// Tuple-path (`use_tuples=true`) per-query state: the multi-column sort
    /// keys. `None` for the single-datum path. Built by `ordered_set_startup`'s
    /// `use_tuples` branch (C `OSAPerQueryState.numSortCols`/`sortColIdx`/...).
    pub(crate) tuple: Option<TupleQueryState>,
    /// `aggref->aggkind == AGGKIND_HYPOTHETICAL`.
    pub(crate) is_hypothetical: bool,
    /// `list_length(aggref->args)` — the number of (non-flag) aggregated
    /// columns; the hypothetical flag column's `sortColIdx` is this + 1.
    pub(crate) num_aggref_args: i32,
}

/// The multi-column sort keys for the `use_tuples` path (`OSAPerQueryState`
/// fields populated only when `use_tuples`).
#[derive(Clone)]
pub(crate) struct TupleQueryState {
    pub(crate) num_sort_cols: i32,
    pub(crate) sort_col_idx: alloc::vec::Vec<types_core::AttrNumber>,
    pub(crate) sort_operators: alloc::vec::Vec<Oid>,
    pub(crate) eq_operators: alloc::vec::Vec<Oid>,
    pub(crate) sort_collations: alloc::vec::Vec<Oid>,
    pub(crate) sort_nulls_firsts: alloc::vec::Vec<bool>,
    /// The aggregated-input columns' `(typid, typmod, collation)` (the
    /// `ExecTypeFromTL(aggref->args)` recipe). For a hypothetical aggregate the
    /// INT4 flag column is appended in `tuple::build_tupdesc`, mirroring the C
    /// `CreateTemplateTupleDesc(natts+1)` hack. Stored per-query, replayed per
    /// group (the owned `TupleDesc` is context-bound and rebuilt each group).
    pub(crate) col_recipe: alloc::vec::Vec<ColRecipe>,
}

/// One aggregated-input column of the tuple-path descriptor recipe.
#[derive(Clone)]
pub(crate) struct ColRecipe {
    pub(crate) typid: Oid,
    pub(crate) typmod: i32,
    pub(crate) collation: Oid,
}

/// `OSAPerGroupState` — the `internal` transition value.
pub(crate) struct OSAPerGroupState {
    pub(crate) qstate: OSAPerQueryState,
    pub(crate) sort_id: SortStateId,
    pub(crate) number_of_rows: i64,
    pub(crate) sort_done: bool,
    /// The standalone input/retrieve slot for the tuple path (C
    /// `qstate->tupslot`, a `MakeSingleTupleTableSlot` slot). `None` on the
    /// datum path. Held in a leaked group-lifespan context like the sort.
    pub(crate) tupslot: Option<Box<types_nodes::tuptable::SlotData<'static>>>,
}

// ===========================================================================
// ordered_set_startup (datum branch) / shutdown.
// ===========================================================================

/// `ordered_set_shutdown(Datum arg)` — release the sort's temp files. The
/// owned-model callback arg is the [`SortStateId`].
pub(crate) fn ordered_set_shutdown<'mcx>(_mcx: Mcx<'mcx>, arg: CDatum<'mcx>) -> PgResult<()> {
    let id = arg.as_usize() as SortStateId;
    end_sortstate(id)
}

/// Build the per-query `OSAPerQueryState` (datum branch), caching it in fn_extra.
fn build_or_get_qstate(fcinfo: &mut FunctionCallInfoBaseData) -> OSAPerQueryState {
    if let Some(flinfo) = fcinfo.flinfo.as_ref() {
        if let Some(q) = flinfo.fn_extra_user_ref::<OSAPerQueryState>() {
            return q.clone();
        }
    }

    let mcx = per_query_mcx();
    let aggref = match ok(aggapi::agg_get_aggref::call(mcx, fcinfo)) {
        Some(a) => a,
        None => raise(PgError::error(
            "ordered-set aggregate called in non-aggregate context",
        )),
    };
    if !types_catalog::pg_aggregate::AGGKIND_IS_ORDERED_SET(aggref.aggkind) {
        raise(PgError::error(
            "ordered-set aggregate support function called for non-ordered-set aggregate",
        ));
    }

    let rescan_needed = aggapi::agg_state_is_shared::call(fcinfo);

    let sortlist = aggref.aggorder.as_ref().map(|v| v.as_slice()).unwrap_or(&[]);
    if sortlist.len() != 1 || aggref.aggkind == types_parsenodes::AGGKIND_HYPOTHETICAL {
        raise(PgError::error(
            "ordered-set aggregate support function does not support multiple aggregated columns",
        ));
    }
    let sortcl = &sortlist[0];
    if sortcl.sortop == 0 {
        raise(PgError::error("ordered-set aggregate: invalid sort operator"));
    }

    // get_sortgroupclause_tle(sortcl, aggref->args): match ressortgroupref.
    let args = aggref
        .args
        .as_ref()
        .expect("ordered_set_startup: aggref->args is NULL");
    let tle = args
        .iter()
        .find(|tle| tle.ressortgroupref == sortcl.tle_sort_group_ref)
        .expect("get_sortgroupclause_tle: no matching TargetEntry");
    let tle_expr = tle
        .expr
        .as_ref()
        .expect("ordered_set_startup: TargetEntry->expr is NULL");

    let ti = ok(backend_nodes_nodeFuncs_seams::expr_type_info::call(tle_expr));
    let sort_col_type = ti.typid;
    let sort_collation = ti.collation;

    let tlbva = ok(backend_utils_cache_lsyscache_seams::get_typlenbyvalalign::call(
        sort_col_type,
    ));

    let qstate = OSAPerQueryState {
        rescan_needed,
        sort_col_type,
        typ_len: tlbva.typlen,
        typ_by_val: tlbva.typbyval,
        typ_align: tlbva.typalign,
        sort_operator: sortcl.sortop,
        eq_operator: sortcl.eqop,
        sort_collation,
        sort_nulls_first: sortcl.nulls_first,
        equal_fn_oid: 0,
        tuple: None,
        is_hypothetical: false,
        num_aggref_args: args.len() as i32,
    };

    if let Some(flinfo) = fcinfo.flinfo.as_mut() {
        flinfo.set_fn_extra(qstate.clone());
    }
    qstate
}

/// `ordered_set_startup(fcinfo, use_tuples=false)` — datum path.
fn ordered_set_startup(fcinfo: &mut FunctionCallInfoBaseData) -> Box<OSAPerGroupState> {
    let (code, _aggcontext) = aggapi::agg_check_call_context::call(fcinfo);
    if code != AGG_CONTEXT_AGGREGATE {
        raise(PgError::error(
            "ordered-set aggregate called in non-aggregate context",
        ));
    }
    let qstate = build_or_get_qstate(fcinfo);
    new_group_state(fcinfo, qstate)
}

/// Create the per-group sort (in a leaked group-lifespan context), register it,
/// and register the shutdown callback.
fn new_group_state(
    fcinfo: &mut FunctionCallInfoBaseData,
    qstate: OSAPerQueryState,
) -> Box<OSAPerGroupState> {
    let gmcx = leak_ctx("ordered-set group sort");

    // TUPLESORT_NONE = 0; TUPLESORT_RANDOMACCESS = 1.
    let tuplesortopt: i32 = if qstate.rescan_needed { 1 } else { 0 };

    let sortstate = ok(tsort::tuplesort_begin_datum::call(
        gmcx,
        qstate.sort_col_type,
        qstate.sort_operator,
        qstate.sort_collation,
        qstate.sort_nulls_first,
        work_mem(),
        tuplesortopt,
    ));
    let boxed = ok(alloc_in(gmcx, sortstate));
    let sort_id = register_sortstate(boxed);

    // AggRegisterCallback(fcinfo, ordered_set_shutdown, PointerGetDatum(osastate)).
    ok(aggapi::agg_register_callback::call(
        fcinfo,
        ordered_set_shutdown,
        CDatum::from_usize(sort_id as usize),
    ));

    Box::new(OSAPerGroupState {
        qstate,
        sort_id,
        number_of_rows: 0,
        sort_done: false,
        tupslot: None,
    })
}

/// `work_mem` GUC — the tuplesort memory budget. The OSA sorts are small; use
/// the standard default (the GUC accessor is not threaded into adt crates).
pub(crate) fn work_mem() -> i32 {
    4096
}

/// A leaked per-query `MemoryContext` mcx (the owned stand-in for `fn_mcxt`).
pub(crate) fn per_query_mcx() -> Mcx<'static> {
    leak_ctx("ordered-set per-query")
}

/// Leak a fresh `MemoryContext` (the owned stand-in for nodeAgg's per-group /
/// per-query contexts, whose reset/free nodeAgg owns; repo-wide by-ref-free TODO)
/// and return its `'static` mcx.
pub(crate) fn leak_ctx(name: &'static str) -> Mcx<'static> {
    let ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new(name)));
    ctx.mcx()
}

// ===========================================================================
// ordered_set_transition.
// ===========================================================================

/// Take the `internal` transition state out of `args[0]`. `None` is first call.
pub(crate) fn take_group_state(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> Option<Box<OSAPerGroupState>> {
    if arg_isnull(fcinfo, 0) {
        return None;
    }
    match fcinfo.take_ref_arg(0) {
        Some(RefPayload::Internal(b)) => Some(b.downcast::<OSAPerGroupState>().unwrap_or_else(|_| {
            panic!("ordered_set fn: args[0] internal state is not an OSAPerGroupState")
        })),
        Some(other) => panic!("ordered_set fn: args[0] is not an internal state ({other:?})"),
        None => None,
    }
}

/// Re-stash the live per-group state onto args[0] so a sibling aggregate sharing
/// the transition value can finalize against it (nodeAgg hands `surviving_arg0`
/// back into the shared pergroup).
pub(crate) fn restash(fcinfo: &mut FunctionCallInfoBaseData, osastate: Box<OSAPerGroupState>) {
    fcinfo.set_ref_arg(0, RefPayload::Internal(osastate));
}

/// `ordered_set_transition(PG_FUNCTION_ARGS)` (3970).
fn fc_ordered_set_transition(fcinfo: &mut FunctionCallInfoBaseData) -> Word {
    let mut osastate = take_group_state(fcinfo).unwrap_or_else(|| ordered_set_startup(fcinfo));

    if !arg_isnull(fcinfo, 1) {
        let val = getarg_sort_cdatum(fcinfo, 1, &osastate.qstate);
        with_sortstate_mut(osastate.sort_id, |s| {
            ok(tsort::tuplesort_putdatum::call(s, val, false));
        });
        osastate.number_of_rows += 1;
    }

    ret_internal(fcinfo, osastate)
}

/// `PG_GETARG_DATUM(i)` for the sort column, as a canonical `CDatum` the
/// tuplesort copies into its own context. By-value → `ByVal(word)`; by-ref →
/// the verbatim image on the by-reference lane.
pub(crate) fn getarg_sort_cdatum<'mcx>(
    fcinfo: &FunctionCallInfoBaseData,
    i: usize,
    qstate: &OSAPerQueryState,
) -> CDatum<'mcx> {
    if qstate.typ_by_val {
        return CDatum::from_usize(arg_word(fcinfo, i).as_usize());
    }
    let mcx = leak_ctx("ordered-set byref arg");
    match fcinfo.ref_arg(i) {
        Some(RefPayload::Varlena(b)) => CDatum::ByRef(vec_in(mcx, b)),
        Some(RefPayload::Cstring(s)) => {
            let mut img = s.clone().into_bytes();
            img.push(0);
            CDatum::ByRef(vec_in(mcx, &img))
        }
        _ => raise(PgError::error(
            "ordered_set_transition: arg has no by-reference payload on the call frame",
        )),
    }
}

pub(crate) fn vec_in<'mcx>(mcx: Mcx<'mcx>, bytes: &[u8]) -> PgVec<'mcx, u8> {
    let mut v = PgVec::new_in(mcx);
    for &b in bytes {
        v.push(b);
    }
    v
}

// ===========================================================================
// percentile_disc_final / percentile_cont_float8_final.
// ===========================================================================

/// Finish the sort, or rescan if we already did.
pub(crate) fn perform_or_rescan(osastate: &mut OSAPerGroupState) {
    if !osastate.sort_done {
        with_sortstate_mut(osastate.sort_id, |s| ok(tsort::tuplesort_performsort::call(s)));
        osastate.sort_done = true;
    } else {
        with_sortstate_mut(osastate.sort_id, |s| ok(tsort::tuplesort_rescan::call(s)));
    }
}

/// `PG_RETURN_DATUM(val)` for the (by-value or by-ref) sort-column result.
pub(crate) fn ret_sort_cdatum(
    fcinfo: &mut FunctionCallInfoBaseData,
    val: CDatum<'_>,
    qstate: &OSAPerQueryState,
) -> Word {
    if qstate.typ_by_val {
        return Word::from_usize(val.as_usize());
    }
    match val {
        CDatum::ByRef(v) => {
            fcinfo.set_ref_result(RefPayload::Varlena(v.iter().copied().collect()));
            Word::from_usize(0)
        }
        CDatum::ByVal(w) => Word::from_usize(w),
        _ => raise(PgError::error(
            "ordered-set finalfn: unexpected result datum shape",
        )),
    }
}

/// `percentile_disc_final(PG_FUNCTION_ARGS)` (3973).
fn fc_percentile_disc_final(fcinfo: &mut FunctionCallInfoBaseData) -> Word {
    if arg_isnull(fcinfo, 1) {
        return ret_null(fcinfo);
    }
    let percentile = arg_float8(fcinfo, 1);
    if percentile < 0.0 || percentile > 1.0 || percentile.is_nan() {
        raise(percentile_range_error(percentile));
    }
    if arg_isnull(fcinfo, 0) {
        return ret_null(fcinfo);
    }
    let mut osastate = take_group_state(fcinfo).expect("percentile_disc: non-null arg0");
    if osastate.number_of_rows == 0 {
        restash(fcinfo, osastate);
        return ret_null(fcinfo);
    }

    perform_or_rescan(&mut osastate);

    let rownum = (percentile * osastate.number_of_rows as f64).ceil() as i64;
    if rownum > 1 {
        let skipped = with_sortstate_mut(osastate.sort_id, |s| {
            ok(tsort::tuplesort_skiptuples::call(s, rownum - 1, true))
        });
        if !skipped {
            raise(PgError::error("missing row in percentile_disc"));
        }
    }
    let (found, val, isnull) =
        with_sortstate_mut(osastate.sort_id, |s| ok(tsort::tuplesort_getdatum::call(s, true, true)));
    if !found {
        raise(PgError::error("missing row in percentile_disc"));
    }
    let result = if isnull {
        ret_null(fcinfo)
    } else {
        ret_sort_cdatum(fcinfo, val, &osastate.qstate)
    };
    restash(fcinfo, osastate);
    result
}

/// `LerpFunc` (C `typedef Datum (*LerpFunc)(Datum lo, Datum hi, double pct)`):
/// interpolate between two consecutive sorted values as canonical `CDatum`s.
type LerpFunc = fn(CDatum<'_>, CDatum<'_>, f64) -> CDatum<'static>;

/// `float8_lerp(lo, hi, pct)` (502): the by-value float8 interpolator.
fn float8_lerp_cdatum(lo: CDatum<'_>, hi: CDatum<'_>, pct: f64) -> CDatum<'static> {
    let v = float8_lerp(lo.as_f64(), hi.as_f64(), pct);
    CDatum::from_usize(Word::from_f64(v).as_usize())
}

/// `interval_lerp(lo, hi, pct)` (511): interpolate two `interval` values via the
/// `timestamp.c` `interval_mi`/`interval_mul`/`interval_pl` arithmetic seam. The
/// by-ref interval is carried as its 16-byte LE image on the `CDatum::ByRef`
/// lane.
fn interval_lerp_cdatum(lo: CDatum<'_>, hi: CDatum<'_>, pct: f64) -> CDatum<'static> {
    let lo_iv = cdatum_to_interval(&lo);
    let hi_iv = cdatum_to_interval(&hi);
    let result = ok(backend_utils_adt_timestamp_seams::interval_lerp::call(
        lo_iv, hi_iv, pct,
    ));
    interval_to_cdatum(&result)
}

/// Decode a 16-byte interval image off the by-ref lane into an `Interval`.
fn cdatum_to_interval(d: &CDatum<'_>) -> types_datetime::Interval {
    match d {
        CDatum::ByRef(v) => {
            let b: alloc::vec::Vec<u8> = v.iter().copied().collect();
            if b.len() < 16 {
                raise(PgError::error("interval image < 16 bytes"));
            }
            types_datetime::Interval {
                time: i64::from_le_bytes(b[0..8].try_into().unwrap()),
                day: i32::from_le_bytes(b[8..12].try_into().unwrap()),
                month: i32::from_le_bytes(b[12..16].try_into().unwrap()),
            }
        }
        _ => raise(PgError::error(
            "percentile_cont(interval): sort value is not a by-reference interval",
        )),
    }
}

/// Encode an `Interval` to its 16-byte LE image on the by-ref lane.
fn interval_to_cdatum(iv: &types_datetime::Interval) -> CDatum<'static> {
    let mcx = leak_ctx("percentile interval result");
    let mut img = alloc::vec::Vec::with_capacity(16);
    img.extend_from_slice(&iv.time.to_le_bytes());
    img.extend_from_slice(&iv.day.to_le_bytes());
    img.extend_from_slice(&iv.month.to_le_bytes());
    CDatum::ByRef(vec_in(mcx, &img))
}

/// `percentile_cont_final_common(fcinfo, expect_type, lerpfunc)` (525) — the
/// shared continuous-percentile finalfn body for both float8 and interval.
fn percentile_cont_final_common(
    fcinfo: &mut FunctionCallInfoBaseData,
    expect_type: Oid,
    lerpfunc: LerpFunc,
) -> Word {
    if arg_isnull(fcinfo, 1) {
        return ret_null(fcinfo);
    }
    let percentile = arg_float8(fcinfo, 1);
    if percentile < 0.0 || percentile > 1.0 || percentile.is_nan() {
        raise(percentile_range_error(percentile));
    }
    if arg_isnull(fcinfo, 0) {
        return ret_null(fcinfo);
    }
    let mut osastate = take_group_state(fcinfo).expect("percentile_cont: non-null arg0");
    if osastate.number_of_rows == 0 {
        restash(fcinfo, osastate);
        return ret_null(fcinfo);
    }
    if osastate.qstate.sort_col_type != expect_type {
        raise(PgError::error("percentile_cont: type mismatch"));
    }

    perform_or_rescan(&mut osastate);

    let n = osastate.number_of_rows as f64;
    let first_row = (percentile * (n - 1.0)).floor() as i64;
    let second_row = (percentile * (n - 1.0)).ceil() as i64;

    let skipped = with_sortstate_mut(osastate.sort_id, |s| {
        ok(tsort::tuplesort_skiptuples::call(s, first_row, true))
    });
    if !skipped {
        raise(PgError::error("missing row in percentile_cont"));
    }
    let (found, first_val, isnull) =
        with_sortstate_mut(osastate.sort_id, |s| ok(tsort::tuplesort_getdatum::call(s, true, true)));
    if !found {
        raise(PgError::error("missing row in percentile_cont"));
    }
    if isnull {
        restash(fcinfo, osastate);
        return ret_null(fcinfo);
    }

    let val: CDatum<'static> = if first_row == second_row {
        clone_cdatum_static(&first_val)
    } else {
        let (found2, second_val, isnull2) = with_sortstate_mut(osastate.sort_id, |s| {
            ok(tsort::tuplesort_getdatum::call(s, true, true))
        });
        if !found2 {
            raise(PgError::error("missing row in percentile_cont"));
        }
        if isnull2 {
            restash(fcinfo, osastate);
            return ret_null(fcinfo);
        }
        let proportion = (percentile * (n - 1.0)) - first_row as f64;
        lerpfunc(
            clone_cdatum_static(&first_val),
            clone_cdatum_static(&second_val),
            proportion,
        )
    };

    let result = ret_sort_cdatum(fcinfo, val, &osastate.qstate);
    restash(fcinfo, osastate);
    result
}

/// Clone a fetched sort `CDatum` to a `'static` value (by-ref images copy into a
/// leaked group context, matching the rest of the datum-path lane).
fn clone_cdatum_static(d: &CDatum<'_>) -> CDatum<'static> {
    match d {
        CDatum::ByVal(w) => CDatum::from_usize(*w),
        CDatum::ByRef(v) => {
            let mcx = leak_ctx("percentile cont value");
            CDatum::ByRef(vec_in(mcx, &v.iter().copied().collect::<alloc::vec::Vec<u8>>()))
        }
        _ => raise(PgError::error("percentile_cont: unexpected datum shape")),
    }
}

/// `percentile_cont_float8_final(PG_FUNCTION_ARGS)` (3975 / C 612).
fn fc_percentile_cont_float8_final(fcinfo: &mut FunctionCallInfoBaseData) -> Word {
    percentile_cont_final_common(fcinfo, FLOAT8OID, float8_lerp_cdatum)
}

/// `percentile_cont_interval_final(PG_FUNCTION_ARGS)` (3977 / C 622).
fn fc_percentile_cont_interval_final(fcinfo: &mut FunctionCallInfoBaseData) -> Word {
    percentile_cont_final_common(fcinfo, INTERVALOID, interval_lerp_cdatum)
}

/// `float8_lerp(lo, hi, pct) = lo + pct * (hi - lo)`.
#[inline]
pub(crate) fn float8_lerp(lo: f64, hi: f64, pct: f64) -> f64 {
    lo + pct * (hi - lo)
}

// ===========================================================================
// Builtin registration.
// ===========================================================================

fn entry(foid: u32, name: &str, nargs: i16, native: types_fmgr::PgFnNative) -> (BuiltinFunction, types_fmgr::PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.to_string(),
            nargs,
            strict: false, // pg_proc.dat: ordered-set support fns are not strict.
            retset: false,
            func: None,
        },
        native,
    )
}

/// Register the datum-path ordered-set builtins into the fmgr-core registry. The
/// tuple/interval/hypothetical functions are intentionally NOT registered (see
/// the crate docs) — they remain genuinely absent rather than stubbed.
pub fn register_orderedset_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        entry(3970, "ordered_set_transition", 2, |fc| Ok(fc_ordered_set_transition(fc))),
        entry(3973, "percentile_disc_final", 3, |fc| Ok(fc_percentile_disc_final(fc))),
        entry(3975, "percentile_cont_float8_final", 2, |fc| {
            Ok(fc_percentile_cont_float8_final(fc))
        }),
        entry(3977, "percentile_cont_interval_final", 2, |fc| {
            Ok(fc_percentile_cont_interval_final(fc))
        }),
        entry(3979, "percentile_disc_multi_final", 3, |fc| {
            Ok(multi::fc_percentile_disc_multi_final(fc))
        }),
        entry(3981, "percentile_cont_float8_multi_final", 2, |fc| {
            Ok(multi::fc_percentile_cont_float8_multi_final(fc))
        }),
        entry(3985, "mode_final", 2, |fc| Ok(mode::fc_mode_final(fc))),
        entry(3971, "ordered_set_transition_multi", 2, |fc| {
            Ok(tuple::fc_ordered_set_transition_multi(fc))
        }),
        entry(3987, "hypothetical_rank_final", 2, |fc| {
            Ok(tuple::fc_hypothetical_rank_final(fc))
        }),
        entry(3989, "hypothetical_percent_rank_final", 2, |fc| {
            Ok(tuple::fc_hypothetical_percent_rank_final(fc))
        }),
        entry(3991, "hypothetical_cume_dist_final", 2, |fc| {
            Ok(tuple::fc_hypothetical_cume_dist_final(fc))
        }),
    ]);
}
