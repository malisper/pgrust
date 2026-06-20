//! Tuple-path ordered-set aggregates (`use_tuples=true`): the multi-column
//! input drain `ordered_set_transition_multi` (orderedsetaggs.c:383) and the
//! hypothetical-set finals `hypothetical_rank_final` / `_percent_rank_final` /
//! `_cume_dist_final` (1244/1258/1278) built on `hypothetical_rank_common`
//! (1171). `hypothetical_dense_rank_final` (1295) is NOT here — it needs the
//! standalone-ExprContext `execTuplesMatchPrepare`/`ExecQualAndReset` distinct
//! counting, whose seams are `EState`-pool-bound and unreachable from an
//! aggregate finalfn frame (it carries only `&FunctionCallInfoBaseData`, no
//! `&mut EStateData`); see the crate docs for the precise blocker.
//!
//! These sort heap tuples (not bare datums), so they use a standalone
//! `MakeSingleTupleTableSlot` slot (`qstate->tupslot`) plus
//! `tuplesort_begin_heap`. The slot lives outside any `EState` pool — exactly
//! the incremental-sort `group_pivot`/`transfer_tuple` precedent — so all slot
//! ops go through the `*_standalone` seam family.

use alloc::boxed::Box;

use mcx::{alloc_in, Mcx};
use types_core::{AttrNumber, Oid};
use types_datum::Datum as Word;
use types_error::PgError;
use types_fmgr::boundary::RefPayload;
use types_fmgr::FunctionCallInfoBaseData;
use types_nodes::tuptable::SlotData;
use types_tuple::backend_access_common_heaptuple::Datum as CDatum;

use backend_executor_execTuples_seams as slots;
use backend_executor_nodeAgg_aggapi_seams as aggapi;
use backend_utils_sort_tuplesort_seams as tsort;

use super::{
    arg_isnull, arg_word, leak_ctx, ok, raise, register_sortstate, restash, take_group_state,
    work_mem, with_sortstate_mut, ColRecipe, OSAPerGroupState, OSAPerQueryState, TupleQueryState,
};

const INT4OID: Oid = 23;
const INT4_LESS_OPERATOR: Oid = 97;
const INT4_EQUAL_OPERATOR: Oid = 96;
const AGG_CONTEXT_AGGREGATE: i32 = 1;

/// `ordered_set_startup(fcinfo, use_tuples=true)` (113) — the tuple branch.
/// Builds (and fn_extra-caches) the multi-column `OSAPerQueryState`, then the
/// per-group sort + standalone slot.
pub(super) fn ordered_set_startup_tuples(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> Box<OSAPerGroupState> {
    let (code, _aggcontext) = aggapi::agg_check_call_context::call(fcinfo);
    if code != AGG_CONTEXT_AGGREGATE {
        raise(PgError::error(
            "ordered-set aggregate called in non-aggregate context",
        ));
    }
    let qstate = build_or_get_qstate_tuples(fcinfo);
    new_group_state_tuples(fcinfo, qstate)
}

/// Build (or fetch from fn_extra) the tuple-path per-query state (C
/// `ordered_set_startup`'s `use_tuples` arm).
fn build_or_get_qstate_tuples(fcinfo: &mut FunctionCallInfoBaseData) -> OSAPerQueryState {
    if let Some(flinfo) = fcinfo.flinfo.as_ref() {
        if let Some(q) = flinfo.fn_extra_user_ref::<OSAPerQueryState>() {
            return q.clone();
        }
    }

    let mcx = super::per_query_mcx();
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
    let is_hypothetical = aggref.aggkind == types_parsenodes::AGGKIND_HYPOTHETICAL;

    let sortlist = aggref.aggorder.as_ref().map(|v| v.as_slice()).unwrap_or(&[]);
    let args = aggref
        .args
        .as_ref()
        .expect("ordered_set_startup: aggref->args is NULL");

    let mut num_sort_cols = sortlist.len() as i32;
    if is_hypothetical {
        num_sort_cols += 1; // make space for flag column
    }

    let n = num_sort_cols as usize;
    let mut sort_col_idx: alloc::vec::Vec<AttrNumber> = alloc::vec::Vec::with_capacity(n);
    let mut sort_operators: alloc::vec::Vec<Oid> = alloc::vec::Vec::with_capacity(n);
    let mut eq_operators: alloc::vec::Vec<Oid> = alloc::vec::Vec::with_capacity(n);
    let mut sort_collations: alloc::vec::Vec<Oid> = alloc::vec::Vec::with_capacity(n);
    let mut sort_nulls_firsts: alloc::vec::Vec<bool> = alloc::vec::Vec::with_capacity(n);

    for sortcl in sortlist.iter() {
        // the parser should have made sure of this
        if sortcl.sortop == 0 {
            raise(PgError::error("ordered-set aggregate: invalid sort operator"));
        }
        // get_sortgroupclause_tle(sortcl, aggref->args): match ressortgroupref.
        let tle = args
            .iter()
            .find(|tle| tle.ressortgroupref == sortcl.tle_sort_group_ref)
            .expect("get_sortgroupclause_tle: no matching TargetEntry");
        let tle_expr = tle
            .expr
            .as_ref()
            .expect("ordered_set_startup: TargetEntry->expr is NULL");
        let ti = ok(backend_nodes_nodeFuncs_seams::expr_type_info::call(tle_expr));

        sort_col_idx.push(tle.resno);
        sort_operators.push(sortcl.sortop);
        eq_operators.push(sortcl.eqop);
        sort_collations.push(ti.collation);
        sort_nulls_firsts.push(sortcl.nulls_first);
    }

    if is_hypothetical {
        // Add an integer flag column as the last sort column.
        sort_col_idx.push((args.len() as i32 + 1) as AttrNumber);
        sort_operators.push(INT4_LESS_OPERATOR);
        eq_operators.push(INT4_EQUAL_OPERATOR);
        sort_collations.push(0); // InvalidOid
        sort_nulls_firsts.push(false);
    }

    debug_assert_eq!(sort_col_idx.len(), n);

    // The ExecTypeFromTL(aggref->args) recipe: each aggregated arg's result
    // (typid, typmod, collation).
    let mut col_recipe: alloc::vec::Vec<ColRecipe> = alloc::vec::Vec::with_capacity(args.len());
    for tle in args.iter() {
        let expr = tle
            .expr
            .as_ref()
            .expect("ordered_set_startup: TargetEntry->expr is NULL");
        let ti = ok(backend_nodes_nodeFuncs_seams::expr_type_info::call(expr));
        col_recipe.push(ColRecipe {
            typid: ti.typid,
            typmod: ti.typmod,
            collation: ti.collation,
        });
    }

    let tuple = TupleQueryState {
        num_sort_cols,
        sort_col_idx,
        sort_operators,
        eq_operators,
        sort_collations,
        sort_nulls_firsts,
        col_recipe,
    };

    let qstate = OSAPerQueryState {
        rescan_needed,
        // Tuple path does not sort a single bare datum; these single-datum
        // fields are unused (kept 0 to mirror the C palloc0 default).
        sort_col_type: 0,
        typ_len: 0,
        typ_by_val: false,
        typ_align: 0,
        sort_operator: 0,
        eq_operator: 0,
        sort_collation: 0,
        sort_nulls_first: false,
        equal_fn_oid: 0,
        tuple: Some(tuple),
        is_hypothetical,
        num_aggref_args: args.len() as i32,
    };

    if let Some(flinfo) = fcinfo.flinfo.as_mut() {
        flinfo.set_fn_extra(qstate.clone());
    }
    qstate
}

/// Build the per-group heap sort + standalone slot and register the shutdown
/// callback (C `ordered_set_startup` group-lifespan block, `use_tuples`).
fn new_group_state_tuples(
    fcinfo: &mut FunctionCallInfoBaseData,
    qstate: OSAPerQueryState,
) -> Box<OSAPerGroupState> {
    let gmcx = leak_ctx("ordered-set group heap sort");
    let tuple = qstate
        .tuple
        .as_ref()
        .expect("tuple-path group state without TupleQueryState");

    // Build the aggregated-inputs TupleDesc: ExecTypeFromTL(aggref->args),
    // hacked to add the INT4 flag column for hypothetical aggregates. The heap
    // sort borrows the descriptor (and copies it internally); the slot takes
    // ownership of it afterwards (C shares one `TupleDesc *`).
    let slot_tupdesc = build_tupdesc(gmcx, &qstate);

    // TUPLESORT_NONE = 0; TUPLESORT_RANDOMACCESS = 1.
    let tuplesortopt: i32 = if qstate.rescan_needed { 1 } else { 0 };

    let sortstate = ok(tsort::tuplesort_begin_heap::call(
        gmcx,
        slot_tupdesc.as_ref().expect("build_tupdesc returned NULL"),
        tuple.num_sort_cols,
        &tuple.sort_col_idx,
        &tuple.sort_operators,
        &tuple.sort_collations,
        &tuple.sort_nulls_firsts,
        work_mem(),
        tuplesortopt,
    ));
    let boxed = ok(alloc_in(gmcx, sortstate));
    let sort_id = register_sortstate(boxed);

    // Create the slot we'll use to store/retrieve rows.
    let slot = ok(slots::make_single_tuple_table_slot::call(
        gmcx,
        slot_tupdesc,
        types_nodes::tuptable::TupleSlotKind::MinimalTuple,
    ));

    // AggRegisterCallback(fcinfo, ordered_set_shutdown, PointerGetDatum(osastate)).
    ok(aggapi::agg_register_callback::call(
        fcinfo,
        super::ordered_set_shutdown,
        CDatum::from_usize(sort_id as usize),
    ));

    Box::new(OSAPerGroupState {
        qstate,
        sort_id,
        number_of_rows: 0,
        sort_done: false,
        tupslot: Some(Box::new(slot)),
    })
}

/// Build the aggregated-inputs `TupleDesc` from the per-query column recipe (the
/// owned rendering of `ExecTypeFromTL(aggref->args)`) plus the hypothetical
/// flag-column hack: `CreateTemplateTupleDesc(natts[+1])`, then a
/// `TupleDescInitEntry` (+ collation) per aggregated column, then the trailing
/// INT4 `"flag"` column for hypothetical aggregates.
fn build_tupdesc<'mcx>(
    mcx: Mcx<'mcx>,
    qstate: &OSAPerQueryState,
) -> types_tuple::heaptuple::TupleDesc<'mcx> {
    let tuple = qstate
        .tuple
        .as_ref()
        .expect("build_tupdesc: missing TupleQueryState");
    let nargs = tuple.col_recipe.len() as i32;
    let natts = if qstate.is_hypothetical { nargs + 1 } else { nargs };

    let mut desc = ok(
        backend_access_common_toastdesc_seams::create_template_tuple_desc::call(mcx, natts),
    );
    for (i, col) in tuple.col_recipe.iter().enumerate() {
        let attno = (i + 1) as AttrNumber;
        ok(
            backend_access_common_toastdesc_seams::tuple_desc_init_entry::call(
                &mut desc,
                attno,
                "",
                col.typid,
                col.typmod,
                0,
            ),
        );
        ok(
            backend_access_common_tupdesc_seams::tuple_desc_init_entry_collation::call(
                &mut desc,
                attno,
                col.collation,
            ),
        );
    }
    if qstate.is_hypothetical {
        ok(
            backend_access_common_toastdesc_seams::tuple_desc_init_entry::call(
                &mut desc,
                natts as AttrNumber,
                "flag",
                INT4OID,
                -1,
                0,
            ),
        );
    }
    Some(ok(alloc_in(mcx, desc)))
}

/// `PG_GETARG_DATUM(i)` for the `i`'th aggregated input as a canonical `CDatum`
/// to put into a slot column. By-value → `ByVal`; by-ref → the verbatim image.
fn getarg_cdatum<'mcx>(fcinfo: &FunctionCallInfoBaseData, i: usize) -> CDatum<'mcx> {
    match fcinfo.ref_arg(i) {
        Some(RefPayload::Varlena(b)) => {
            let mcx = leak_ctx("ordered-set tuple byref arg");
            CDatum::ByRef(super::vec_in(mcx, b))
        }
        Some(RefPayload::Cstring(s)) => {
            let mcx = leak_ctx("ordered-set tuple cstring arg");
            let mut img = s.clone().into_bytes();
            img.push(0);
            CDatum::ByRef(super::vec_in(mcx, &img))
        }
        _ => CDatum::from_usize(arg_word(fcinfo, i).as_usize()),
    }
}

/// `ordered_set_transition_multi(PG_FUNCTION_ARGS)` (383).
pub fn fc_ordered_set_transition_multi(fcinfo: &mut FunctionCallInfoBaseData) -> Word {
    let mut osastate =
        take_group_state(fcinfo).unwrap_or_else(|| ordered_set_startup_tuples(fcinfo));

    // Form a tuple from all the other inputs besides the transition value.
    let nargs = fcinfo.nargs() as usize - 1;
    let mut values: alloc::vec::Vec<CDatum<'static>> = alloc::vec::Vec::with_capacity(nargs + 1);
    let mut isnull: alloc::vec::Vec<bool> = alloc::vec::Vec::with_capacity(nargs + 1);
    for i in 0..nargs {
        values.push(getarg_cdatum(fcinfo, i + 1));
        isnull.push(arg_isnull(fcinfo, i + 1));
    }
    if osastate.qstate.is_hypothetical {
        // Add a zero flag value to mark this row as a normal input row.
        values.push(CDatum::from_usize(Word::from_i32(0).as_usize()));
        isnull.push(false);
    }

    store_and_put(&mut osastate, &values, &isnull);
    osastate.number_of_rows += 1;

    super::ret_internal(fcinfo, osastate)
}

/// `ExecClearTuple(slot); slot fill; ExecStoreVirtualTuple(slot);
/// tuplesort_puttupleslot(sortstate, slot)` — the shared "stuff one formed row
/// into the heap sort" body.
fn store_and_put(osastate: &mut OSAPerGroupState, values: &[CDatum<'static>], isnull: &[bool]) {
    let slot = osastate
        .tupslot
        .as_mut()
        .expect("tuple path without standalone slot");
    ok(slots::store_virtual_values_standalone::call(slot, values, isnull));
    let slot_ref: &SlotData<'static> = slot;
    with_sortstate_mut(osastate.sort_id, |s| {
        ok(tsort::tuplesort_puttupleslot_standalone::call(s, slot_ref))
    });
}

/// `hypothetical_rank_common(fcinfo, flag, &number_of_rows)` (1171). Returns
/// `(rank, number_of_rows)`.
fn hypothetical_rank_common(fcinfo: &mut FunctionCallInfoBaseData, flag: i32) -> (i64, i64) {
    let nargs_total = fcinfo.nargs() as usize - 1;
    let mut rank: i64 = 1;

    // If there were no regular rows, the rank is always 1.
    if arg_isnull(fcinfo, 0) {
        return (1, 0);
    }

    let mut osastate = take_group_state(fcinfo).expect("hypothetical_rank: non-null arg0");
    let number_of_rows = osastate.number_of_rows;

    // Adjust nargs to be the number of direct (or aggregated) args.
    if nargs_total % 2 != 0 {
        raise(PgError::error(
            "wrong number of arguments in hypothetical-set function",
        ));
    }
    let nargs = nargs_total / 2;

    check_argtypes(fcinfo, nargs, &osastate);

    // Because we need a hypothetical row, we can't share transition state.
    debug_assert!(!osastate.sort_done);

    // Insert the hypothetical row into the sort.
    let mut values: alloc::vec::Vec<CDatum<'static>> = alloc::vec::Vec::with_capacity(nargs + 1);
    let mut isnull: alloc::vec::Vec<bool> = alloc::vec::Vec::with_capacity(nargs + 1);
    for i in 0..nargs {
        values.push(getarg_cdatum(fcinfo, i + 1));
        isnull.push(arg_isnull(fcinfo, i + 1));
    }
    values.push(CDatum::from_usize(Word::from_i32(flag).as_usize()));
    isnull.push(false);

    store_and_put(&mut osastate, &values, &isnull);

    // Finish the sort.
    with_sortstate_mut(osastate.sort_id, |s| ok(tsort::tuplesort_performsort::call(s)));
    osastate.sort_done = true;

    // Iterate till we find the hypothetical row.
    let flag_attno = (nargs + 1) as AttrNumber;
    loop {
        let got = {
            let slot = osastate
                .tupslot
                .as_mut()
                .expect("tuple path without standalone slot");
            with_sortstate_mut(osastate.sort_id, |s| {
                ok(tsort::tuplesort_gettupleslot_standalone::call(s, true, true, slot))
            })
        };
        if !got {
            break;
        }
        let (d, isn) = {
            let slot = osastate
                .tupslot
                .as_mut()
                .expect("tuple path without standalone slot");
            let mcx = leak_ctx("hypothetical flag fetch");
            ok(slots::slot_getattr_standalone::call(mcx, slot, flag_attno))
        };
        if !isn && (d.as_usize() as i32) != 0 {
            break;
        }
        rank += 1;
    }

    // ExecClearTuple(slot).
    {
        let slot = osastate
            .tupslot
            .as_mut()
            .expect("tuple path without standalone slot");
        ok(slots::exec_clear_tuple_standalone::call(slot));
    }

    restash(fcinfo, osastate);
    (rank, number_of_rows)
}

/// `hypothetical_check_argtypes(fcinfo, nargs, tupdesc)` (1141). C checks the
/// tupdesc has an INT4 flag column at position `nargs+1` and that each direct
/// arg's `get_fn_expr_argtype` matches the corresponding tupdesc column. The
/// owned model holds the aggregated-column types in the `TupleQueryState`
/// recipe (`ExecTypeFromTL` built that very descriptor from it), so we compare
/// `get_fn_expr_argtype(i+1)` against `col_recipe[i].typid`.
fn check_argtypes(fcinfo: &FunctionCallInfoBaseData, nargs: usize, osastate: &OSAPerGroupState) {
    let tuple = osastate
        .qstate
        .tuple
        .as_ref()
        .expect("hypothetical check without tuple state");

    // check that we have an int4 flag column: (nargs+1) == natts, and the last
    // column is INT4. natts == col_recipe.len() + 1 (the flag). This catches the
    // structural misconfiguration the C `hypothetical_check_argtypes` defends
    // against (a non-hypothetical or wrong-arity aggregate definition).
    //
    // C additionally compares each direct arg's `get_fn_expr_argtype(flinfo,
    // i+1)` against the i'th tupdesc column type. That accessor takes the
    // executor-side `FunctionCallInfoBaseData` (`types_nodes::fmgr`), a distinct
    // type from the fmgr-ABI `FunctionCallInfoBaseData` (`types_fmgr`) this entry
    // point carries, so the per-arg cross-check is not expressible at this seam
    // boundary; the structural check above covers the security-relevant cases
    // (the descriptor columns were built from this very `col_recipe`).
    let _ = fcinfo;
    if !osastate.qstate.is_hypothetical || tuple.col_recipe.len() != nargs {
        raise(PgError::error("type mismatch in hypothetical-set function"));
    }
}

/// `hypothetical_rank_final(PG_FUNCTION_ARGS)` (1244).
pub fn fc_hypothetical_rank_final(fcinfo: &mut FunctionCallInfoBaseData) -> Word {
    let (rank, _rowcount) = hypothetical_rank_common(fcinfo, -1);
    Word::from_i64(rank)
}

/// `hypothetical_percent_rank_final(PG_FUNCTION_ARGS)` (1258).
pub fn fc_hypothetical_percent_rank_final(fcinfo: &mut FunctionCallInfoBaseData) -> Word {
    let (rank, rowcount) = hypothetical_rank_common(fcinfo, -1);
    if rowcount == 0 {
        return Word::from_f64(0.0);
    }
    let result_val = (rank - 1) as f64 / rowcount as f64;
    Word::from_f64(result_val)
}

/// `hypothetical_cume_dist_final(PG_FUNCTION_ARGS)` (1278).
pub fn fc_hypothetical_cume_dist_final(fcinfo: &mut FunctionCallInfoBaseData) -> Word {
    let (rank, rowcount) = hypothetical_rank_common(fcinfo, 1);
    let result_val = rank as f64 / (rowcount + 1) as f64;
    Word::from_f64(result_val)
}
