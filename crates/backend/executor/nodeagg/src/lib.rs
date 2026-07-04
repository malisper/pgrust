// nodeAgg.c, AGG_PLAIN/AGG_SORTED/AGG_HASHED single-grouping-set slice: byval
// and by-ref transtypes (INTERNAL is a byval pointer datum; its state lives in
// the AggStateNode aggcontext the transfn reaches via fcinfo->context; by-ref
// transvalues copy into that aggcontext at C's datumCopy points), finalfn
// via resolve-once peragg carriers; transitions compile into one execexpr
// program (C's evaltrans). Grouping sets (all strategies) live in gsets.rs.
// aggsplit variants and hashagg spill are loud panics.
#![allow(non_snake_case)]

use core::alloc::Layout;
use std::ptr::NonNull;
use std::rc::Rc;

use ::datum::{Datum, NullableDatum};
use ::types_fmgr::{AggStateNode, FmNodePtr, FmgrInfo, LocalFcinfo};
use ::execexpr::{
    exec_build_agg_projection_info_subplans, exec_build_agg_qual_subplans, exec_build_agg_trans,
    exec_build_agg_trans_hashed, exec_eval_expr, exec_project, exec_qual, AggBind,
    AggOrderedSpec, AggPerGroup, AggTransSpec, EvalSlots, ExprState,
};
use ::tuplesort::{Tuplesort, TUPLESORT_NONE};
use ::execgrouping::TupleHashTable;
use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::mcx::{vec_with_capacity_in, Allocator, MemoryContext, PgBox, PgVec};
use ::types_core::catalog::PROCEDURE_RELATION_ID;
use ::types_core::Oid;
use ::types_error::{PgError, PgResult};
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::Agg;
use ::types_nodes::primnodes::{Aggref, AGGKIND_NORMAL};
use ::types_nodes::NodeTag;
use ::types_pathnodes::{AGGSPLIT_SIMPLE, AGG_HASHED, AGG_MIXED, AGG_PLAIN, AGG_SORTED};
use ::types_slot::{SlotData, TupleSlotKind};
use ::types_tuple::TupleDescData;

pub fn init_seams() {}

#[cfg(test)]
mod tests;

mod gsets;

const ACL_EXECUTE: u64 = 1 << 7;
const ACLCHECK_OK: i32 = 0;

pub struct AggStateData<'mcx> {
    pub plan: &'mcx Agg<'mcx>,
    pub ps_ExprContext: EcxtId,
    pub tmpcontext: EcxtId,
    // C's curaggcontext, in the FmNode the transfn fcinfos carry; raw arena
    // cell so the pointer survives self moving (drop: make_agg_state_node).
    agg_node: NonNull<AggStateNode>,
    pub ps_ResultTupleDesc: Option<Rc<TupleDescData<'static>>>,
    pub ps_ResultTupleSlot: ExecSlotId,
    proj: PgBox<'mcx, ExprState<'mcx>>,
    evaltrans: Option<PgBox<'mcx, ExprState<'mcx>>>,
    peragg: PgVec<'mcx, PerAggData<'mcx>>,
    trans_init: PgVec<'mcx, NullableDatum>,
    trans_typ: PgVec<'mcx, TransTyp>,
    // Owners of once-allocated arrays; all element access goes through the
    // *_base pointers so the step-held pointers stay valid (steps.rs note).
    _pergroup: PgVec<'mcx, AggPerGroup>,
    pergroup_base: NonNull<AggPerGroup>,
    agg_values_base: NonNull<Datum>,
    agg_nulls_base: NonNull<bool>,
    agg_done: bool,
    numtrans: usize,
    perhash: Option<PerHashData<'mcx>>,
    persort: Option<PerSortData<'mcx>>,
    gsets: Option<PgBox<'mcx, gsets::GroupingSetsState<'mcx>>>,
    pertrans_sort: PgVec<'mcx, PerTransSortData<'mcx>>,
    qual: Option<PgBox<'mcx, ExprState<'mcx>>>,
}

const MAX_ORDERED_TRANS_ARGS: usize = 8;

// C AggStatePerTransData's non-presorted DISTINCT/ORDER BY slice
// (build_pertrans_for_aggref): the evaltrans program parks each row's args in
// `scratch` and raises `flag`; collect_ordered_input feeds the tuplesort and
// process_ordered_aggregate_{single,multi} replay the transfn at the group
// boundary.
struct PerTransSortData<'mcx> {
    transno: usize,
    num_inputs: usize,
    num_trans_inputs: usize,
    num_distinct_cols: usize,
    sortdesc: Rc<TupleDescData<'mcx>>,
    sort_col_idx: PgVec<'mcx, i16>,
    sort_ops: PgVec<'mcx, Oid>,
    sort_collations: PgVec<'mcx, Oid>,
    sort_nulls_first: PgVec<'mcx, bool>,
    equalfn_one: Option<FmgrInfo>,
    equalfn_multi: Option<PgBox<'mcx, ExprState<'mcx>>>,
    transfn: FmgrInfo,
    agg_collation: Oid,
    scratch: NonNull<NullableDatum>,
    flag: NonNull<bool>,
    sortstate: Option<Tuplesort>,
    insert_slot: Option<SlotData<'mcx>>,
    slot1: Option<SlotData<'mcx>>,
    slot2: Option<SlotData<'mcx>>,
}

fn init_pertrans_sort<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    aggref: &'mcx Aggref<'mcx>,
    transno: usize,
    transfn_oid: Oid,
    agg_collation: Oid,
) -> PgResult<(PerTransSortData<'mcx>, AggOrderedSpec)> {
    let num_inputs = aggref.args.len();
    let num_trans_inputs = aggref.aggargtypes.len();
    assert!(
        num_trans_inputs + 1 <= MAX_ORDERED_TRANS_ARGS,
        "build_pertrans_for_aggref (nodeAgg.c): {num_trans_inputs} ordered trans inputs \
         exceed the replay fcinfo"
    );
    // By construction aggorder is a prefix of aggdistinct
    // (transformDistinctClause).
    let sortlist =
        if !aggref.aggdistinct.is_nil() { &aggref.aggdistinct } else { &aggref.aggorder };
    let num_sort_cols = sortlist.len();
    let num_distinct_cols = aggref.aggdistinct.len();
    debug_assert!(num_sort_cols > 0);
    debug_assert!(num_sort_cols >= aggref.aggorder.len());
    let sortdesc = execscan::exec_type_from_tl(mcx, &aggref.args)?;

    let mut sort_col_idx: PgVec<'mcx, i16> = vec_with_capacity_in(mcx, num_sort_cols)?;
    let mut sort_ops: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, num_sort_cols)?;
    let mut sort_collations: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, num_sort_cols)?;
    let mut sort_nulls_first: PgVec<'mcx, bool> = vec_with_capacity_in(mcx, num_sort_cols)?;
    for sc_node in sortlist {
        let scl = sc_node.as_sort_group_clause().expect("agg sortlist cell");
        let tle = aggref
            .args
            .iter()
            .find_map(|n| {
                let t = n.as_target_entry().expect("Aggref.args cell");
                (t.ressortgroupref == scl.tleSortGroupRef).then_some(t)
            })
            .expect("agg ORDER BY/DISTINCT expression not found in Aggref.args");
        assert!(scl.sortop != 0, "sortless SortGroupClause survived the parser");
        sort_col_idx.push(tle.resno);
        sort_ops.push(scl.sortop);
        sort_collations.push(execscan::expr_collation(tle.expr));
        sort_nulls_first.push(scl.nulls_first);
    }

    let mut equalfn_one = None;
    let mut equalfn_multi = None;
    if num_distinct_cols > 0 {
        let mut eqfuncoids: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, num_distinct_cols)?;
        for sc_node in &aggref.aggdistinct {
            let scl = sc_node.as_sort_group_clause().expect("aggdistinct cell");
            eqfuncoids.push(lsyscache::get_opcode(scl.eqop)?);
        }
        if num_distinct_cols == 1 {
            equalfn_one = Some(fmgr_core::fmgr_info(eqfuncoids[0])?);
        } else {
            equalfn_multi = Some(::execexpr::exec_build_grouping_equal(
                mcx,
                &sortdesc,
                &sortdesc,
                &sort_col_idx[..num_distinct_cols],
                &eqfuncoids,
                &sort_collations[..num_distinct_cols],
            )?);
        }
    }

    let mut transfn = fmgr_core::fmgr_info(transfn_oid)?;
    let mut fnexpr_types: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, num_trans_inputs + 1)?;
    fnexpr_types.push(aggref.aggtranstype);
    for t in aggref.aggargtypes.iter() {
        fnexpr_types.push(t);
    }
    // SAFETY: leaked into the query arena; the replay flinfo dies with the
    // plan it serves — from_node_ref's contract (same carrier as
    // build_agg_trans's AggFnArgTypes).
    let fnexpr_types: &'static [Oid] = unsafe { core::mem::transmute(fnexpr_types.leak()) };
    let carrier =
        ::mcx::alloc_leak_in(mcx, ::types_core::fmgr::AggFnArgTypes(fnexpr_types))?;
    // SAFETY: carrier is arena-backed for the query, see above.
    transfn.fn_expr = Some(unsafe { ::types_core::fmgr::FnExprErased::from_node_ref(carrier) });

    let scratch_layout = Layout::array::<NullableDatum>(num_inputs.max(1))
        .expect("ordered scratch layout");
    let scratch: NonNull<NullableDatum> = ::mcx::Allocator::allocate(&mcx, scratch_layout)
        .map_err(|_| mcx.oom(scratch_layout.size()))?
        .cast();
    // SAFETY: fresh allocation of num_inputs slots.
    unsafe {
        for i in 0..num_inputs {
            scratch.as_ptr().add(i).write(NullableDatum::null());
        }
    }
    let flag_layout = Layout::new::<bool>();
    let flag: NonNull<bool> = ::mcx::Allocator::allocate(&mcx, flag_layout)
        .map_err(|_| mcx.oom(flag_layout.size()))?
        .cast();
    // SAFETY: fresh allocation of the exact layout.
    unsafe { flag.write(false) };

    let (insert_slot, slot1, slot2) = if num_inputs > 1 {
        (
            Some(exectuples::make_tuple_table_slot(
                mcx,
                TupleSlotKind::Virtual,
                Some(sortdesc.clone()),
            )),
            Some(exectuples::make_tuple_table_slot(
                mcx,
                TupleSlotKind::MinimalTuple,
                Some(sortdesc.clone()),
            )),
            (num_distinct_cols > 0).then(|| {
                exectuples::make_tuple_table_slot(
                    mcx,
                    TupleSlotKind::MinimalTuple,
                    Some(sortdesc.clone()),
                )
            }),
        )
    } else {
        (None, None, None)
    };

    let ospec = AggOrderedSpec {
        scratch,
        num_trans_inputs: num_trans_inputs as u16,
        flag,
    };
    Ok((
        PerTransSortData {
            transno,
            num_inputs,
            num_trans_inputs,
            num_distinct_cols,
            sortdesc,
            sort_col_idx,
            sort_ops,
            sort_collations,
            sort_nulls_first,
            equalfn_one,
            equalfn_multi,
            transfn,
            agg_collation,
            scratch,
            flag,
            sortstate: None,
            insert_slot,
            slot1,
            slot2,
        },
        ospec,
    ))
}

// C AggStatePerTransData's transtypeLen/transtypeByVal pair, indexed by
// transno (drives the initval datumCopy at group init).
#[derive(Clone, Copy)]
struct TransTyp {
    len: i16,
    byval: bool,
}

// AGG_SORTED state: firstSlot/grp_firstTuple as two swappable minimal slots
// (the pending slot holds C's grp_firstTuple copy), the grouping-boundary
// program is C's phase->eqfunctions[numCols-1].
struct PerSortData<'mcx> {
    first_slot: SlotData<'mcx>,
    pending_slot: SlotData<'mcx>,
    eq: PgBox<'mcx, ExprState<'mcx>>,
    have_pending: bool,
}

// C AggStatePerHashData, single grouping set (find_hash_columns order:
// grouping cols first, then other needed input cols).
struct PerHashData<'mcx> {
    hashtable: TupleHashTable<'mcx>,
    // C's one minimal-tuple hashslot split in two (same allocation shape):
    // the virtual slot feeds lookups, the minimal one deforms at retrieve.
    hashslot: SlotData<'mcx>,
    retrieve_slot: SlotData<'mcx>,
    first_slot: SlotData<'mcx>,
    num_cols: usize,
    hash_grp_col_idx_input: PgVec<'mcx, i16>,
    largest_grp_col_idx: i32,
    outer_natts: usize,
    // The steps' pergroup indirection cell (exec_build_agg_trans_hashed).
    pergroup_cell: NonNull<NonNull<AggPerGroup>>,
    hash_ngroups_limit: u64,
    hash_ngroups_current: u64,
    table_filled: bool,
    hashiter: usize,
}

// C AggStatePerAggData finalize slice; result copy discipline rides the armed
// result mcx instead of MemoryContextContains.
struct PerAggData<'mcx> {
    transno: u32,
    aggref: &'mcx Aggref<'mcx>,
    trans_shared: bool,
    finalfn: Option<FmgrInfo>,
    num_final_args: u16,
    agg_collation: Oid,
    resulttype_len: i16,
    direct_args: PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>>,
}

fn make_agg_state_node<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    ctx: MemoryContext,
) -> PgResult<NonNull<AggStateNode>> {
    let layout = Layout::new::<AggStateNode>();
    let raw = mcx.allocate(layout).map_err(|_| mcx.oom(layout.size()))?;
    let p: NonNull<AggStateNode> = raw.cast();
    // SAFETY: fresh allocation of the exact layout.
    unsafe { p.write(AggStateNode::new(ctx)) };
    // The node's MemoryContext is droppy inside a no-drop arena: the query
    // context's reset callback is its destructor (docs/no-drop.md guard rule).
    // SAFETY: fires exactly once, before the arena bytes are reclaimed.
    mcx.context()
        .register_reset_callback(move || unsafe { core::ptr::drop_in_place(p.as_ptr()) });
    Ok(p)
}

#[cold]
#[inline(never)]
fn agg_lookup_failed(aggfnoid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!("cache lookup failed for aggregate {aggfnoid}")))
}

#[cold]
#[inline(never)]
fn agg_permission_denied(aggfnoid: Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!("permission denied for aggregate {aggfnoid}"))
            .with_sqlstate(::types_error::ERRCODE_INSUFFICIENT_PRIVILEGE),
    )
}

fn collect_aggrefs<'mcx>(
    node: Node<'mcx>,
    out: &mut PgVec<'mcx, (Node<'mcx>, &'mcx Aggref<'mcx>)>,
) {
    match node.node_tag() {
        NodeTag::T_Aggref => out.push((node, node.as_aggref().unwrap())),
        // GroupingFunc args are never evaluated (EEOP_GROUPING_FUNC reads
        // grouped_cols only).
        NodeTag::T_GroupingFunc => {}
        NodeTag::T_TargetEntry => collect_aggrefs(node.as_target_entry().unwrap().expr, out),
        NodeTag::T_Var | NodeTag::T_Const => {}
        NodeTag::T_FuncExpr => {
            for a in node.as_func_expr().unwrap().args.iter() {
                collect_aggrefs(a, out);
            }
        }
        NodeTag::T_OpExpr => {
            for a in node.as_op_expr().unwrap().args.iter() {
                collect_aggrefs(a, out);
            }
        }
        NodeTag::T_Param
        | NodeTag::T_CaseTestExpr
        | NodeTag::T_SQLValueFunction
        | NodeTag::T_NextValueExpr
        | NodeTag::T_CoerceToDomainValue => {}
        NodeTag::T_RelabelType => collect_aggrefs(node.as_relabel_type().unwrap().arg, out),
        NodeTag::T_CoerceViaIO => collect_aggrefs(node.as_coerce_via_io().unwrap().arg, out),
        NodeTag::T_ArrayCoerceExpr => {
            let a = node.as_array_coerce_expr().unwrap();
            collect_aggrefs(a.arg, out);
            if let Some(e) = a.elemexpr {
                collect_aggrefs(e, out);
            }
        }
        NodeTag::T_ConvertRowtypeExpr => {
            collect_aggrefs(node.as_convert_rowtype_expr().unwrap().arg, out)
        }
        NodeTag::T_CoerceToDomain => {
            collect_aggrefs(node.as_coerce_to_domain().unwrap().arg, out)
        }
        NodeTag::T_BoolExpr => {
            for a in node.as_bool_expr().unwrap().args.iter() {
                collect_aggrefs(a, out);
            }
        }
        NodeTag::T_NullTest => {
            if let Some(a) = node.as_null_test().unwrap().arg {
                collect_aggrefs(a, out);
            }
        }
        NodeTag::T_BooleanTest => {
            if let Some(a) = node.as_boolean_test().unwrap().arg {
                collect_aggrefs(a, out);
            }
        }
        NodeTag::T_DistinctExpr => {
            for a in node.as_distinct_expr().unwrap().args.iter() {
                collect_aggrefs(a, out);
            }
        }
        NodeTag::T_ScalarArrayOpExpr => {
            for a in node.as_scalar_array_op_expr().unwrap().args.iter() {
                collect_aggrefs(a, out);
            }
        }
        NodeTag::T_ArrayExpr => {
            for e in node.as_array_expr().unwrap().elements.iter() {
                collect_aggrefs(e, out);
            }
        }
        NodeTag::T_RowExpr => {
            for a in node.as_row_expr().unwrap().args.iter() {
                collect_aggrefs(a, out);
            }
        }
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().unwrap();
            if let Some(a) = c.arg {
                collect_aggrefs(a, out);
            }
            for w in c.args.iter() {
                let cw = w.as_case_when().expect("CaseWhen");
                collect_aggrefs(cw.expr.expect("CaseWhen.expr"), out);
                collect_aggrefs(cw.result.expect("CaseWhen.result"), out);
            }
            if let Some(d) = c.defresult {
                collect_aggrefs(d, out);
            }
        }
        NodeTag::T_CoalesceExpr => {
            for a in node.as_coalesce_expr().unwrap().args.iter() {
                collect_aggrefs(a, out);
            }
        }
        NodeTag::T_MinMaxExpr => {
            for a in node.as_min_max_expr().unwrap().args.iter() {
                collect_aggrefs(a, out);
            }
        }
        NodeTag::T_JsonValueExpr => {
            let j = node.as_json_value_expr().unwrap();
            for e in [j.raw_expr, j.formatted_expr].into_iter().flatten() {
                collect_aggrefs(e, out);
            }
        }
        NodeTag::T_JsonConstructorExpr => {
            let c = node.as_json_constructor_expr().unwrap();
            for a in c.args.iter() {
                collect_aggrefs(a, out);
            }
            for e in [c.func, c.coercion].into_iter().flatten() {
                collect_aggrefs(e, out);
            }
        }
        NodeTag::T_JsonIsPredicate => {
            if let Some(e) = node.as_json_is_predicate().unwrap().expr {
                collect_aggrefs(e, out);
            }
        }
        NodeTag::T_SubPlan => {
            let sp = node.as_sub_plan().unwrap();
            if let Some(te) = sp.testexpr {
                collect_aggrefs(te, out);
            }
            for a in sp.args.iter() {
                collect_aggrefs(a, out);
            }
        }
        tag => panic!("ExecInitAgg (nodeAgg.c): Agg tlist node family {tag:?} not ported"),
    }
}

// GetAggInitVal (nodeAgg.c): initval text through the transtype's typinput.
// In-function by-ref results ride the resolved carrier's scratch (dead once
// flinfo drops); C's palloc'd result is modeled by the datumCopy into mcx.
fn get_agg_init_val(mcx: ::mcx::Mcx<'_>, text: &str, transtype: Oid) -> PgResult<Datum> {
    let (typinput, typioparam) = lsyscache::getTypeInputInfo(transtype)?;
    let mut flinfo = fmgr_core::fmgr_info(typinput)?;
    let cstr = std::ffi::CString::new(text)
        .expect("agginitval text contains an interior NUL");
    let d = ::types_fmgr::input_function_call(&mut flinfo, Some(&cstr), typioparam, -1, mcx)?;
    let (typlen, typbyval) = lsyscache::get_typlenbyval(transtype)?;
    if typbyval {
        Ok(d)
    } else {
        // SAFETY: non-null by-ref in-function result, live until flinfo drops.
        unsafe { ::execexpr::agg_datum_copy(mcx, d, typlen) }
    }
}

/// `ExecInitAgg` (nodeAgg.c). The caller (execProcnode's T_Agg arm) inits the
/// outer child and passes this node's result type.
pub fn exec_init_agg<'mcx>(
    node: &'mcx Agg<'mcx>,
    estate: &mut EStateData<'mcx>,
    _eflags: i32,
    result_desc: Rc<TupleDescData<'static>>,
    outer_desc: Option<Rc<TupleDescData<'static>>>,
) -> PgResult<AggStateData<'mcx>> {
    let mcx = estate.es_query_cxt;
    let has_grouping_sets = !node.groupingSets.is_nil() || !node.chain.is_nil();
    if node.aggstrategy != AGG_PLAIN
        && node.aggstrategy != AGG_HASHED
        && node.aggstrategy != AGG_SORTED
        && node.aggstrategy != AGG_MIXED
    {
        panic!("ExecInitAgg (nodeAgg.c): aggstrategy {} cannot happen", node.aggstrategy);
    }
    assert!(
        node.aggstrategy != AGG_MIXED || has_grouping_sets,
        "ExecInitAgg (nodeAgg.c): AGG_MIXED outside grouping sets cannot happen"
    );
    if node.aggsplit != AGGSPLIT_SIMPLE {
        panic!("ExecInitAgg (nodeAgg.c): aggsplit {} not ported", node.aggsplit);
    }
    if node.aggstrategy == AGG_PLAIN && node.numCols != 0 {
        panic!("ExecInitAgg (nodeAgg.c): AGG_PLAIN with grouping columns cannot happen");
    }
    assert!(
        node.aggstrategy != AGG_SORTED || node.numCols > 0,
        "ExecInitAgg (nodeAgg.c): AGG_SORTED without grouping columns cannot happen"
    );

    // Hashed: the node context IS the table context (C hands
    // BuildTupleHashTable the same hashcontext memory).
    let agg_ctx_name =
        if node.aggstrategy == AGG_HASHED { "HashAgg hash table" } else { "AggContext" };
    let agg_node = make_agg_state_node(mcx, mcx.context().new_child_bump(agg_ctx_name))?;
    let fm_agg_node: FmNodePtr = Some(agg_node.cast());
    let tmpcontext = estate.create_expr_context();
    let ps_ExprContext = estate.exec_assign_expr_context();
    let ps_ResultTupleSlot =
        estate.exec_init_extra_tuple_slot(Some(result_desc.clone()), TupleSlotKind::Virtual);

    let mut aggrefs: PgVec<'mcx, (Node<'mcx>, &'mcx Aggref<'mcx>)> = PgVec::new_in(mcx);
    for tle in node.plan.targetlist.iter() {
        collect_aggrefs(tle, &mut aggrefs);
    }
    for q in node.plan.qual.iter() {
        collect_aggrefs(q, &mut aggrefs);
    }
    // tlist and qual Aggrefs can share aggnos (find_compatible_agg);
    // numaggs == 0 is C's hashed-DISTINCT shape.
    let numaggs = aggrefs.iter().map(|(_, a)| a.aggno + 1).max().unwrap_or(0) as usize;
    assert!(
        numaggs > 0 || node.aggstrategy == AGG_HASHED || has_grouping_sets,
        "ExecInitAgg: Agg node without Aggrefs outside AGG_HASHED/grouping sets"
    );

    let mut by_aggno: PgVec<'mcx, Option<(Node<'mcx>, &'mcx Aggref<'mcx>)>> =
        vec_with_capacity_in(mcx, numaggs)?;
    by_aggno.resize(numaggs, None);
    let mut numtrans = 0usize;
    for &(anode, aggref) in aggrefs.iter() {
        let (aggno, transno) = (aggref.aggno, aggref.aggtransno);
        assert!(aggno >= 0 && transno >= 0, "Aggref without planner aggno/aggtransno");
        assert!((aggno as usize) < numaggs, "Aggref.aggno out of range");
        if let Some((_, prev)) = by_aggno[aggno as usize] {
            assert!(
                prev.aggfnoid == aggref.aggfnoid && prev.aggtransno == transno,
                "shared aggno with diverging Aggrefs"
            );
        }
        by_aggno[aggno as usize] = Some((anode, aggref));
        numtrans = numtrans.max(transno as usize + 1);
    }

    let userid = miscinit_seams::get_user_id::call();
    // Droppy FmgrInfo carriers: AggStateData's box owns the drops
    // (ExprState.frames precedent), hence no no-drop ctor.
    let mut peragg: PgVec<'mcx, PerAggData<'mcx>> = PgVec::new_in(mcx);
    peragg
        .try_reserve(numaggs)
        .map_err(|_| mcx.oom(numaggs * core::mem::size_of::<PerAggData<'_>>()))?;
    let mut trans_init: PgVec<'mcx, NullableDatum> = vec_with_capacity_in(mcx, numtrans)?;
    trans_init.resize(numtrans, NullableDatum::null());
    let mut trans_aggref: PgVec<'mcx, Option<(Node<'mcx>, &'mcx Aggref<'mcx>)>> =
        vec_with_capacity_in(mcx, numtrans)?;
    trans_aggref.resize(numtrans, None);
    let mut trans_fnoid: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, numtrans)?;
    trans_fnoid.resize(numtrans, 0);
    let mut trans_typ: PgVec<'mcx, TransTyp> = vec_with_capacity_in(mcx, numtrans)?;
    trans_typ.resize(numtrans, TransTyp { len: 0, byval: true });

    let mut pertrans_sort: PgVec<'mcx, PerTransSortData<'mcx>> = PgVec::new_in(mcx);
    let mut ordered_specs: PgVec<'mcx, Option<AggOrderedSpec>> =
        vec_with_capacity_in(mcx, numtrans)?;
    ordered_specs.resize(numtrans, None);
    let mut trans_shared: PgVec<'mcx, bool> = vec_with_capacity_in(mcx, numtrans)?;
    trans_shared.resize(numtrans, false);
    let params = estate.param_bind();
    for aggno in 0..numaggs {
        let (aggref_node, aggref) = by_aggno[aggno].expect("planner aggno numbering has gaps");
        let aclresult = aclchk_seams::object_aclcheck::call(
            PROCEDURE_RELATION_ID,
            aggref.aggfnoid,
            userid,
            ACL_EXECUTE,
        )?;
        if aclresult != ACLCHECK_OK {
            return Err(agg_permission_denied(aggref.aggfnoid));
        }
        let shape = syscache_seams::lookup_pg_aggregate_shape::call(aggref.aggfnoid)?
            .ok_or_else(|| agg_lookup_failed(aggref.aggfnoid))?;
        let is_ordered_set = shape.aggkind != AGGKIND_NORMAL;
        debug_assert!(shape.aggkind == aggref.aggkind);
        if is_ordered_set && has_grouping_sets {
            panic!(
                "ExecInitAgg (nodeAgg.c): ordered-set aggregate under grouping sets \
                 not ported (per-set aggcontexts)"
            );
        }
        if (!aggref.aggorder.is_nil() || !aggref.aggdistinct.is_nil())
            && node.aggstrategy == AGG_HASHED
        {
            panic!("ExecInitAgg (nodeAgg.c): DISTINCT/ORDER BY under AGG_HASHED cannot happen");
        }
        let transtype = aggref.aggtranstype;
        assert!(transtype != 0, "Aggref.aggtranstype unset (planner must resolve it)");
        let (translen, transbyval) = lsyscache::get_typlenbyval(transtype)?;

        let num_direct_args = aggref.aggdirectargs.len();
        let num_final_args = if shape.aggfinalextra {
            aggref.aggargtypes.len() as u16 + 1
        } else {
            num_direct_args as u16 + 1
        };
        let finalfn = if shape.aggfinalfn != 0 {
            // Divergence: C aclchecks as the aggregate owner; differs only
            // under SET ROLE.
            let aclresult = aclchk_seams::object_aclcheck::call(
                PROCEDURE_RELATION_ID,
                shape.aggfinalfn,
                userid,
                ACL_EXECUTE,
            )?;
            if aclresult != ACLCHECK_OK {
                return Err(agg_permission_denied(shape.aggfinalfn));
            }
            let mut flinfo = fmgr_core::fmgr_info(shape.aggfinalfn)?;
            // build_aggregate_finalfn_expr's [transtype, input types..].
            let mut fnexpr_types: PgVec<'mcx, Oid> =
                vec_with_capacity_in(mcx, num_final_args as usize)?;
            fnexpr_types.push(aggref.aggtranstype);
            for t in aggref.aggargtypes.iter().take(num_final_args as usize - 1) {
                fnexpr_types.push(t);
            }
            // SAFETY: leaked into the query arena; the flinfo dies with the
            // plan (init_pertrans_sort's carrier precedent).
            let fnexpr_types: &'static [Oid] =
                unsafe { core::mem::transmute(fnexpr_types.leak()) };
            let carrier =
                ::mcx::alloc_leak_in(mcx, ::types_core::fmgr::AggFnArgTypes(fnexpr_types))?;
            // SAFETY: carrier is arena-backed for the query, see above.
            flinfo.fn_expr =
                Some(unsafe { ::types_core::fmgr::FnExprErased::from_node_ref(carrier) });
            Some(flinfo)
        } else {
            None
        };
        let (resulttype_len, _resulttype_byval) = lsyscache::get_typlenbyval(aggref.aggtype)?;

        let mut direct_args: PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>> = PgVec::new_in(mcx);
        for d in aggref.aggdirectargs.iter() {
            let mut es = ::execexpr::exec_init_expr(mcx, Some(d), params)?
                .expect("aggdirectargs cell is a non-NULL expression");
            // SAFETY: the ps_ExprContext outlives the program (same estate);
            // C evaluates direct args in its per-tuple memory.
            unsafe { es.arm_result_mcx_raw(estate.ecxt(ps_ExprContext).per_tuple_mcx()) };
            direct_args.push(es);
        }

        let transno = aggref.aggtransno as usize;
        peragg.push(PerAggData {
            transno: transno as u32,
            aggref,
            trans_shared: false,
            finalfn,
            num_final_args,
            agg_collation: aggref.inputcollid,
            resulttype_len,
            direct_args,
        });
        match trans_aggref[transno] {
            // find_compatible_trans keys sharing on the transition state.
            Some((_, prev)) => {
                assert!(
                    trans_fnoid[transno] == shape.aggtransfn
                        && prev.aggtranstype == aggref.aggtranstype,
                    "shared transno with diverging transition state"
                );
                trans_shared[transno] = true;
            }
            None => {
                trans_aggref[transno] = Some((aggref_node, aggref));
                trans_fnoid[transno] = shape.aggtransfn;
                trans_typ[transno] = TransTyp { len: translen, byval: transbyval };
                if !is_ordered_set
                    && (!aggref.aggorder.is_nil() || !aggref.aggdistinct.is_nil())
                {
                    // C keeps one sortstate per grouping set; PerTransSortData
                    // is single-set — mis-aggregation, not a missing feature.
                    if has_grouping_sets {
                        panic!(
                            "ExecInitAgg (nodeAgg.c): DISTINCT/ORDER BY aggregate under \
                             grouping sets not ported (per-set sortstates)"
                        );
                    }
                    let (ps, ospec) = init_pertrans_sort(
                        mcx,
                        aggref,
                        transno,
                        shape.aggtransfn,
                        aggref.inputcollid,
                    )?;
                    pertrans_sort.push(ps);
                    ordered_specs[transno] = Some(ospec);
                }
                let initval = syscache_seams::pg_aggregate_agginitval::call(mcx, aggref.aggfnoid)?
                    .ok_or_else(|| agg_lookup_failed(aggref.aggfnoid))?;
                trans_init[transno] = match initval {
                    None => NullableDatum::null(),
                    Some(text) => NullableDatum {
                        value: get_agg_init_val(mcx, &text, transtype)?,
                        isnull: false,
                    },
                };
                if trans_init[transno].isnull
                    && fmgr_core::fmgr_info(shape.aggtransfn)?.fn_strict
                {
                    // C checks the FIRST aggregated input (nodeAgg.c
                    // IsBinaryCoercible gate) — the strict first-value path
                    // copies args[1]; exact-match covers every live agg.
                    let input_type = aggref.aggargtypes.first();
                    if input_type != Some(transtype) {
                        panic!(
                            "ExecInitAgg (nodeAgg.c): strict transfn with NULL initval and \
                             input type {input_type:?} != transtype {transtype} \
                             (IsBinaryCoercible not ported)"
                        );
                    }
                }
            }
        }
    }

    for pa in peragg.iter_mut() {
        pa.trans_shared = trans_shared[pa.transno as usize];
    }

    let mut pergroup: PgVec<'mcx, AggPerGroup> = vec_with_capacity_in(mcx, numtrans)?;
    pergroup.resize(
        numtrans,
        AggPerGroup { trans_value: Datum::null(), trans_value_is_null: true, no_trans_value: true },
    );
    let pergroup_base = NonNull::new(pergroup.as_mut_ptr()).unwrap();

    let (agg_values_base, agg_nulls_base) = {
        let ecxt = estate.ecxt_mut(ps_ExprContext);
        ecxt.ecxt_aggvalues.resize(numaggs, Datum::null());
        ecxt.ecxt_aggnulls.resize(numaggs, true);
        (
            NonNull::new(ecxt.ecxt_aggvalues.as_mut_ptr()).unwrap(),
            NonNull::new(ecxt.ecxt_aggnulls.as_mut_ptr()).unwrap(),
        )
    };

    let mut specs: PgVec<'mcx, AggTransSpec<'mcx, 'mcx>> = vec_with_capacity_in(mcx, numtrans)?;
    for transno in 0..numtrans {
        let (_, aggref) =
            trans_aggref[transno].expect("planner aggtransno numbering has gaps");
        // SAFETY: transno < numtrans elements of the once-allocated pergroup.
        let pg = unsafe { NonNull::new_unchecked(pergroup_base.as_ptr().add(transno)) };
        let is_ordered_set = aggref.aggkind != AGGKIND_NORMAL;
        let num_direct_args = if is_ordered_set { aggref.aggdirectargs.len() } else { 0 };
        let mut arg_types: PgVec<'mcx, Oid> =
            vec_with_capacity_in(mcx, aggref.aggargtypes.len() - num_direct_args + 1)?;
        arg_types.push(aggref.aggtranstype);
        for t in aggref.aggargtypes.iter().skip(num_direct_args) {
            arg_types.push(t);
        }
        let cur_agg =
            is_ordered_set.then(|| (NonNull::from(aggref).cast::<()>(), trans_shared[transno]));
        specs.push(AggTransSpec {
            transfn_oid: trans_fnoid[transno],
            inputcollid: aggref.inputcollid,
            init_value_is_null: trans_init[transno].isnull,
            arg_types: arg_types.leak(),
            args: &aggref.args,
            aggfilter: aggref.aggfilter,
            pergroup: pg,
            transtype_byval: trans_typ[transno].byval,
            transtype_len: trans_typ[transno].len,
            ordered: ordered_specs[transno],
            cur_agg,
        });
    }
    let (mut evaltrans, perhash, persort, gs) = if has_grouping_sets {
        let gs = gsets::init_grouping_sets(
            node, estate, outer_desc, &specs, numtrans, fm_agg_node, params, tmpcontext,
        )?;
        (None, None, None, Some(gs))
    } else if node.aggstrategy == AGG_HASHED {
        let ph = init_perhash(node, estate, numtrans)?;
        let evaltrans =
            exec_build_agg_trans_hashed(mcx, &specs, ph.pergroup_cell, fm_agg_node, params)?;
        (Some(evaltrans), Some(ph), None, None)
    } else {
        let persort = if node.aggstrategy == AGG_SORTED {
            Some(init_persort(node, estate)?)
        } else {
            None
        };
        (Some(exec_build_agg_trans(mcx, &specs, fm_agg_node, params)?), None, persort, None)
    };
    // C invokes transfns in the tmpcontext per-tuple memory; by-ref call
    // results ride the armed result mcx there, reset per tuple (phase
    // programs are armed inside init_grouping_sets).
    if let Some(et) = evaltrans.as_mut() {
        // SAFETY: the tmpcontext ExprContext outlives the program (same estate).
        unsafe { et.arm_result_mcx_raw(estate.ecxt(tmpcontext).per_tuple_mcx()) };
    }
    let bind = AggBind {
        values: agg_values_base,
        nulls: agg_nulls_base,
        naggs: numaggs as u16,
        grouping: gs.as_ref().map(|g| g.grouping_cell()),
    };
    let (proj, qual) = ::executils::with_subplan_compile_env(estate, |env| -> PgResult<_> {
        let env = env.map(|mut e| {
            e.agg = Some(bind);
            e
        });
        let proj = exec_build_agg_projection_info_subplans(
            mcx,
            &node.plan.targetlist,
            None,
            bind,
            params,
            env,
        )?;
        let qual = exec_build_agg_qual_subplans(mcx, &node.plan.qual, bind, params, env)?;
        Ok((proj, qual))
    })?;

    Ok(AggStateData {
        plan: node,
        ps_ExprContext,
        tmpcontext,
        agg_node,
        ps_ResultTupleDesc: Some(result_desc),
        ps_ResultTupleSlot,
        proj,
        evaltrans,
        peragg,
        trans_init,
        trans_typ,
        _pergroup: pergroup,
        pergroup_base,
        agg_values_base,
        agg_nulls_base,
        agg_done: false,
        numtrans,
        perhash,
        persort,
        gsets: gs,
        pertrans_sort,
        qual,
    })
}

// The AGG_SORTED half of ExecInitAgg: outer-format slots + the grouping-
// boundary program (execTuplesMatchPrepare -> ExecBuildGroupingEqual).
fn init_persort<'mcx>(
    node: &'mcx Agg<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PerSortData<'mcx>> {
    let mcx = estate.es_query_cxt;
    let outer_plan = node
        .plan
        .lefttree
        .and_then(Node::as_plan)
        .unwrap_or_else(|| panic!("ExecInitAgg (nodeAgg.c): Agg without an outer plan"));
    let outer_desc = execscan::exec_type_from_tl(mcx, &outer_plan.targetlist)?;

    let num_cols = node.numCols as usize;
    debug_assert!(node.grpColIdx.len() == num_cols && node.grpOperators.len() == num_cols);
    let mut eqfuncoids: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, num_cols)?;
    for &op in node.grpOperators {
        eqfuncoids.push(lsyscache::get_opcode(op)?);
    }
    let eq = ::execexpr::exec_build_grouping_equal(
        mcx,
        &outer_desc,
        &outer_desc,
        node.grpColIdx,
        &eqfuncoids,
        node.grpCollations,
    )?;
    let first_slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(outer_desc.clone()));
    let pending_slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(outer_desc));
    Ok(PerSortData { first_slot, pending_slot, eq, have_pending: false })
}

// find_cols (nodeAgg.c): outer columns referenced outside aggregate args.
fn collect_base_var_cols(node: Node<'_>, out: &mut PgVec<'_, bool>) {
    match node.node_tag() {
        NodeTag::T_Var => {
            let v = node.as_var().unwrap();
            assert!(v.varattno >= 1 && (v.varattno as usize) <= out.len());
            out[(v.varattno - 1) as usize] = true;
        }
        NodeTag::T_Const | NodeTag::T_Aggref | NodeTag::T_GroupingFunc => {}
        NodeTag::T_TargetEntry => {
            collect_base_var_cols(node.as_target_entry().unwrap().expr, out)
        }
        NodeTag::T_FuncExpr => {
            for a in node.as_func_expr().unwrap().args.iter() {
                collect_base_var_cols(a, out);
            }
        }
        NodeTag::T_OpExpr => {
            for a in node.as_op_expr().unwrap().args.iter() {
                collect_base_var_cols(a, out);
            }
        }
        NodeTag::T_Param
        | NodeTag::T_CaseTestExpr
        | NodeTag::T_SQLValueFunction
        | NodeTag::T_NextValueExpr
        | NodeTag::T_CoerceToDomainValue => {}
        NodeTag::T_RelabelType => collect_base_var_cols(node.as_relabel_type().unwrap().arg, out),
        NodeTag::T_CoerceViaIO => collect_base_var_cols(node.as_coerce_via_io().unwrap().arg, out),
        NodeTag::T_ArrayCoerceExpr => {
            let a = node.as_array_coerce_expr().unwrap();
            collect_base_var_cols(a.arg, out);
            if let Some(e) = a.elemexpr {
                collect_base_var_cols(e, out);
            }
        }
        NodeTag::T_ConvertRowtypeExpr => {
            collect_base_var_cols(node.as_convert_rowtype_expr().unwrap().arg, out)
        }
        NodeTag::T_CoerceToDomain => {
            collect_base_var_cols(node.as_coerce_to_domain().unwrap().arg, out)
        }
        NodeTag::T_BoolExpr => {
            for a in node.as_bool_expr().unwrap().args.iter() {
                collect_base_var_cols(a, out);
            }
        }
        NodeTag::T_NullTest => {
            if let Some(a) = node.as_null_test().unwrap().arg {
                collect_base_var_cols(a, out);
            }
        }
        NodeTag::T_BooleanTest => {
            if let Some(a) = node.as_boolean_test().unwrap().arg {
                collect_base_var_cols(a, out);
            }
        }
        NodeTag::T_DistinctExpr => {
            for a in node.as_distinct_expr().unwrap().args.iter() {
                collect_base_var_cols(a, out);
            }
        }
        NodeTag::T_ScalarArrayOpExpr => {
            for a in node.as_scalar_array_op_expr().unwrap().args.iter() {
                collect_base_var_cols(a, out);
            }
        }
        NodeTag::T_ArrayExpr => {
            for e in node.as_array_expr().unwrap().elements.iter() {
                collect_base_var_cols(e, out);
            }
        }
        NodeTag::T_RowExpr => {
            for a in node.as_row_expr().unwrap().args.iter() {
                collect_base_var_cols(a, out);
            }
        }
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().unwrap();
            if let Some(a) = c.arg {
                collect_base_var_cols(a, out);
            }
            for w in c.args.iter() {
                let cw = w.as_case_when().expect("CaseWhen");
                collect_base_var_cols(cw.expr.expect("CaseWhen.expr"), out);
                collect_base_var_cols(cw.result.expect("CaseWhen.result"), out);
            }
            if let Some(d) = c.defresult {
                collect_base_var_cols(d, out);
            }
        }
        NodeTag::T_CoalesceExpr => {
            for a in node.as_coalesce_expr().unwrap().args.iter() {
                collect_base_var_cols(a, out);
            }
        }
        NodeTag::T_MinMaxExpr => {
            for a in node.as_min_max_expr().unwrap().args.iter() {
                collect_base_var_cols(a, out);
            }
        }
        NodeTag::T_JsonValueExpr => {
            let j = node.as_json_value_expr().unwrap();
            for e in [j.raw_expr, j.formatted_expr].into_iter().flatten() {
                collect_base_var_cols(e, out);
            }
        }
        NodeTag::T_JsonConstructorExpr => {
            let c = node.as_json_constructor_expr().unwrap();
            for a in c.args.iter() {
                collect_base_var_cols(a, out);
            }
            for e in [c.func, c.coercion].into_iter().flatten() {
                collect_base_var_cols(e, out);
            }
        }
        NodeTag::T_JsonIsPredicate => {
            if let Some(e) = node.as_json_is_predicate().unwrap().expr {
                collect_base_var_cols(e, out);
            }
        }
        tag => panic!("find_cols (nodeAgg.c): node family {tag:?} not ported"),
    }
}

// find_hash_columns + build_hash_tables (nodeAgg.c), single grouping set.
fn init_perhash<'mcx>(
    node: &'mcx Agg<'mcx>,
    estate: &mut EStateData<'mcx>,
    numtrans: usize,
) -> PgResult<PerHashData<'mcx>> {
    let mcx = estate.es_query_cxt;
    let outer_plan = node
        .plan
        .lefttree
        .and_then(Node::as_plan)
        .unwrap_or_else(|| panic!("ExecInitAgg (nodeAgg.c): Agg without an outer plan"));
    let outer_tlist = &outer_plan.targetlist;
    let outer_natts = outer_tlist.len();
    let num_cols = node.numCols as usize;
    assert!(num_cols > 0 && node.grpColIdx.len() == num_cols);
    assert!(node.numGroups > 0, "Agg.numGroups unset (planner must estimate it)");

    let mut base_cols: PgVec<'mcx, bool> = vec_with_capacity_in(mcx, outer_natts)?;
    base_cols.resize(outer_natts, false);
    for tle in node.plan.targetlist.iter() {
        collect_base_var_cols(tle, &mut base_cols);
    }
    for q in node.plan.qual.iter() {
        collect_base_var_cols(q, &mut base_cols);
    }

    let mut hash_grp_col_idx_input: PgVec<'mcx, i16> =
        vec_with_capacity_in(mcx, outer_natts)?;
    for &attno in node.grpColIdx {
        hash_grp_col_idx_input.push(attno);
        base_cols[(attno - 1) as usize] = false;
    }
    for (i, &needed) in base_cols.iter().enumerate() {
        if needed {
            hash_grp_col_idx_input.push((i + 1) as i16);
        }
    }
    let largest_grp_col_idx =
        hash_grp_col_idx_input.iter().map(|&a| a as i32).max().unwrap_or(0);

    let mut hash_tlist = types_nodes::list::NodeList::nil();
    for &attno in hash_grp_col_idx_input.iter() {
        hash_tlist.lappend(mcx, outer_tlist.nth((attno - 1) as usize))?;
    }
    let hash_desc = execscan::exec_type_from_tl(mcx, &hash_tlist)?;
    let outer_desc = execscan::exec_type_from_tl(mcx, outer_tlist)?;

    let (eqfuncoids, hashfunctions) =
        ::execgrouping::exec_tuples_hash_prepare(mcx, node.grpOperators)?;

    let additionalsize = numtrans * core::mem::size_of::<AggPerGroup>();
    let hashentrysize = hash_agg_entry_size(
        numtrans,
        outer_plan.plan_width.max(0) as usize,
        node.transitionSpace as usize,
    );
    let (mem_limit, hash_ngroups_limit, planned_partitions) =
        hash_agg_set_limits(hashentrysize, node.numGroups as f64, 0);
    estate.es_agg_instrumentation.push((
        node.plan.plan_node_id,
        ::types_core::instrument::AggregateInstrumentation {
            hash_batches_used: 1,
            hash_planned_partitions: planned_partitions as i32,
            ..Default::default()
        },
    ));
    let nbuckets = hash_choose_num_buckets(hashentrysize, node.numGroups, mem_limit);

    let mut key_col_idx: PgVec<'mcx, i16> = vec_with_capacity_in(mcx, num_cols)?;
    for i in 0..num_cols {
        key_col_idx.push((i + 1) as i16);
    }

    let hashtable = ::execgrouping::build_tuple_hash_table(
        mcx,
        &hash_desc,
        &key_col_idx,
        &eqfuncoids,
        &hashfunctions,
        node.grpCollations,
        nbuckets,
        additionalsize,
        false,
    )?;
    let hashslot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(hash_desc.clone()));
    let retrieve_slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(hash_desc));
    let first_slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(outer_desc));

    let cell_layout = Layout::new::<NonNull<AggPerGroup>>();
    let raw = mcx.allocate(cell_layout).map_err(|_| mcx.oom(cell_layout.size()))?;
    let pergroup_cell: NonNull<NonNull<AggPerGroup>> = raw.cast();
    // SAFETY: fresh allocation of the cell's exact layout; repointed before
    // every evaltrans run (lookup_hash_entry).
    unsafe { pergroup_cell.write(NonNull::dangling()) };

    Ok(PerHashData {
        hashtable,
        hashslot,
        retrieve_slot,
        first_slot,
        num_cols,
        hash_grp_col_idx_input,
        largest_grp_col_idx,
        outer_natts,
        pergroup_cell,
        hash_ngroups_limit,
        hash_ngroups_current: 0,
        table_filled: false,
        hashiter: 0,
    })
}

const SIZEOF_MINIMAL_TUPLE_HEADER: usize = 15;
const CHUNKHDRSZ: usize = 16;

const fn maxalign(n: usize) -> usize {
    (n + 7) & !7
}

/// C `hash_agg_entry_size` (nodeAgg.c).
pub fn hash_agg_entry_size(num_trans: usize, tuple_width: usize, transition_space: usize) -> f64 {
    let tuple_size = maxalign(SIZEOF_MINIMAL_TUPLE_HEADER) + tuple_width;
    let tuple_chunk_size = maxalign(tuple_size);
    let pergroup_chunk_size = num_trans * core::mem::size_of::<AggPerGroup>();
    let transition_chunk_size = if transition_space > 0 {
        CHUNKHDRSZ + transition_space.next_power_of_two()
    } else {
        0
    };
    (16 + tuple_chunk_size + pergroup_chunk_size + transition_chunk_size) as f64
}

const HASHAGG_PARTITION_FACTOR: f64 = 1.50;
const HASHAGG_MIN_PARTITIONS: f64 = 4.0;
const HASHAGG_MAX_PARTITIONS: f64 = 1024.0;
const HASHAGG_READ_BUFFER_SIZE: f64 = 8192.0;
const HASHAGG_WRITE_BUFFER_SIZE: f64 = 8192.0;

// C my_log2 (dynahash.c): ceil(log2(num)).
fn my_log2(num: i64) -> u32 {
    if num <= 1 {
        return 0;
    }
    64 - ((num - 1) as u64).leading_zeros()
}

/// C `hash_choose_num_partitions` (nodeAgg.c); the spill machinery itself is
/// unported, this feeds hash_agg_set_limits' buffer-reservation estimate.
fn hash_choose_num_partitions(input_groups: f64, hashentrysize: f64, used_bits: u32) -> usize {
    let hash_mem_limit = ::execgrouping::get_hash_memory_limit() as f64;
    let partition_limit =
        (hash_mem_limit * 0.25 - HASHAGG_READ_BUFFER_SIZE) / HASHAGG_WRITE_BUFFER_SIZE;
    let mem_wanted = HASHAGG_PARTITION_FACTOR * input_groups * hashentrysize;
    let mut dpartitions = 1.0 + (mem_wanted / hash_mem_limit);
    if dpartitions > partition_limit {
        dpartitions = partition_limit;
    }
    dpartitions = dpartitions.clamp(HASHAGG_MIN_PARTITIONS, HASHAGG_MAX_PARTITIONS);
    let mut partition_bits = my_log2(dpartitions as i64);
    if partition_bits + used_bits >= 32 {
        partition_bits = 32 - used_bits;
    }
    1usize << partition_bits
}

/// C `hash_choose_num_buckets` (nodeAgg.c).
fn hash_choose_num_buckets(hashentrysize: f64, ngroups: i64, memory: usize) -> usize {
    let max_nbuckets = ((memory as f64 / hashentrysize) as usize) >> 1;
    (ngroups.max(0) as usize).min(max_nbuckets).max(1)
}

/// C `hash_agg_set_limits` (nodeAgg.c) -> (mem_limit, ngroups_limit,
/// num_partitions).
pub fn hash_agg_set_limits(
    hashentrysize: f64,
    input_groups: f64,
    used_bits: u32,
) -> (usize, u64, usize) {
    let hash_mem_limit = ::execgrouping::get_hash_memory_limit();
    if input_groups * hashentrysize <= hash_mem_limit as f64 {
        return (hash_mem_limit, (hash_mem_limit as f64 / hashentrysize) as u64, 0);
    }
    let npartitions = hash_choose_num_partitions(input_groups, hashentrysize, used_bits);
    let partition_mem =
        (HASHAGG_READ_BUFFER_SIZE + HASHAGG_WRITE_BUFFER_SIZE * npartitions as f64) as usize;
    let mem_limit = if hash_mem_limit > 4 * partition_mem {
        hash_mem_limit - partition_mem
    } else {
        (hash_mem_limit as f64 * 0.75) as usize
    };
    let ngroups_limit =
        if mem_limit as f64 > hashentrysize { (mem_limit as f64 / hashentrysize) as u64 } else { 1 };
    (mem_limit, ngroups_limit, npartitions)
}

// initialize_aggregates (nodeAgg.c), no sortstates; by-ref initvals datumCopy
// into the aggcontext.
fn initialize_aggregates(node: &mut AggStateData<'_>) -> PgResult<()> {
    for ps in node.pertrans_sort.iter_mut() {
        if let Some(old) = ps.sortstate.take() {
            old.end();
        }
        let work_mem = init_small::globals::work_mem();
        ps.sortstate = Some(if ps.num_inputs == 1 {
            Tuplesort::begin_datum(
                ps.sortdesc.attr(0).atttypid,
                ps.sort_ops[0],
                ps.sort_collations[0],
                ps.sort_nulls_first[0],
                work_mem,
                TUPLESORT_NONE,
            )?
        } else {
            // SAFETY: lifetime erasure for the tuplesort API only; the sort
            // ends before the query context resets (group boundary, end,
            // rescan), so the desc outlives every access.
            let desc: Rc<TupleDescData<'static>> =
                unsafe { core::mem::transmute(ps.sortdesc.clone()) };
            Tuplesort::begin_heap(
                desc,
                &ps.sort_col_idx,
                &ps.sort_ops,
                &ps.sort_collations,
                &ps.sort_nulls_first,
                work_mem,
                TUPLESORT_NONE,
            )?
        });
    }
    for (transno, init) in node.trans_init.iter().enumerate() {
        let typ = node.trans_typ[transno];
        let value = if !init.isnull && !typ.byval {
            // SAFETY: node-lifetime initval datum; agg_node is live, no &mut.
            unsafe {
                ::execexpr::agg_datum_copy(
                    node.agg_node.as_ref().aggcontext(),
                    init.value,
                    typ.len,
                )?
            }
        } else {
            init.value
        };
        // SAFETY: transno < the pergroup array's once-allocated length; the
        // base pointer is the sole access path (struct invariant).
        unsafe {
            node.pergroup_base.as_ptr().add(transno).write(AggPerGroup {
                trans_value: value,
                trans_value_is_null: init.isnull,
                no_trans_value: init.isnull,
            });
        }
    }
    Ok(())
}

// The tuplesort feed half of the ordered-trans steps: rows the program
// marked live park their args in scratch until here. Runs before the
// tmpcontext reset — by-ref scratch datums live in per-tuple memory.
fn collect_ordered_input<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    for ps in node.pertrans_sort.iter_mut() {
        // SAFETY: once-allocated cells the trans program writes (steps.rs).
        if !unsafe { ps.flag.read() } {
            continue;
        }
        // SAFETY: as above.
        unsafe { ps.flag.write(false) };
        let sort = ps.sortstate.as_mut().expect("ordered pertrans sort begun");
        if ps.num_inputs == 1 {
            // SAFETY: scratch slot 0 written by the program this row.
            let nd = unsafe { ps.scratch.read() };
            sort.putdatum(nd.value, nd.isnull)?;
        } else {
            let slot = ps.insert_slot.as_mut().expect("multi-input ordered agg has a slot");
            exectuples::exec_clear_tuple(slot, mcx);
            {
                let base = slot.base_mut();
                for i in 0..ps.num_inputs {
                    // SAFETY: i < num_inputs scratch slots.
                    let nd = unsafe { ps.scratch.as_ptr().add(i).read() };
                    base.tts_values[i] = nd.value;
                    base.tts_isnull[i] = nd.isnull;
                }
            }
            exectuples::exec_store_virtual_tuple(slot);
            sort.puttupleslot(slot, mcx)?;
        }
    }
    Ok(())
}

// C advance_transition_function (nodeAgg.c): the sorted-input replay of the
// transfn; by-ref result discipline mirrors execexpr's agg_plain_trans_byref.
fn advance_transition_function(
    fcinfo: &mut LocalFcinfo<MAX_ORDERED_TRANS_ARGS>,
    transfn: &mut FmgrInfo,
    typ: TransTyp,
    num_trans_inputs: usize,
    agg_node: NonNull<AggStateNode>,
    pg: *mut AggPerGroup,
) -> PgResult<()> {
    // SAFETY: pg is the once-allocated pergroup slot, sole live pointer here;
    // agg_node outlives the call (query-lifetime cell).
    unsafe {
        if transfn.fn_strict {
            for i in 1..=num_trans_inputs {
                if fcinfo.args[i].isnull {
                    return Ok(());
                }
            }
            if (*pg).no_trans_value {
                // C ExecAggInitGroup: the first value becomes the transvalue.
                let v = fcinfo.args[1];
                let value = if !typ.byval {
                    ::execexpr::agg_datum_copy(agg_node.as_ref().aggcontext(), v.value, typ.len)?
                } else {
                    v.value
                };
                (*pg) = AggPerGroup {
                    trans_value: value,
                    trans_value_is_null: false,
                    no_trans_value: false,
                };
                return Ok(());
            }
            if (*pg).trans_value_is_null {
                return Ok(());
            }
        }
        fcinfo.args[0] =
            NullableDatum { value: (*pg).trans_value, isnull: (*pg).trans_value_is_null };
        fcinfo.isnull = false;
        let result = transfn.invoke(fcinfo)?;
        let isnull = fcinfo.isnull;
        let new_val = if !typ.byval && result.as_usize() != (*pg).trans_value.as_usize() {
            if !isnull {
                ::execexpr::agg_datum_copy(agg_node.as_ref().aggcontext(), result, typ.len)?
            } else {
                Datum::null()
            }
        } else {
            result
        };
        (*pg).trans_value = new_val;
        (*pg).trans_value_is_null = isnull;
    }
    Ok(())
}

// process_ordered_aggregate_single/multi (nodeAgg.c): drain each pertrans
// sort through the transfn with DISTINCT dedup. Datums/tuples read without
// copy — the in-memory sort images stay live until tuplesort_end.
fn process_ordered_aggregates<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if node.pertrans_sort.is_empty() {
        return Ok(());
    }
    let mcx = estate.es_query_cxt;
    let tmp = node.tmpcontext;
    let AggStateData { pertrans_sort, trans_typ, agg_node, pergroup_base, .. } = node;
    for ps in pertrans_sort.iter_mut() {
        // SAFETY: transno < numtrans of the once-allocated pergroup array.
        let pg = unsafe { pergroup_base.as_ptr().add(ps.transno) };
        let typ = trans_typ[ps.transno];
        let mut fcinfo = LocalFcinfo::<MAX_ORDERED_TRANS_ARGS>::fresh(ps.agg_collation);
        fcinfo.nargs = (ps.num_trans_inputs + 1) as i16;
        fcinfo.context = Some(agg_node.cast());
        // SAFETY: the per-tuple context outlives every call below (resets
        // recycle the same context object).
        unsafe { fcinfo.set_result_mcx(estate.ecxt(tmp).per_tuple_mcx()) };
        let mut sort = ps.sortstate.take().expect("ordered pertrans sort begun");
        sort.performsort()?;
        if ps.num_inputs == 1 {
            let mut old: Option<NullableDatum> = None;
            while let Some(nd) = sort.getdatum(true)? {
                estate.reset_expr_context(tmp);
                if ps.num_distinct_cols > 0 {
                    if let Some(o) = old {
                        let equal = if o.isnull && nd.isnull {
                            true
                        } else if o.isnull != nd.isnull {
                            false
                        } else {
                            let eq = ps.equalfn_one.as_mut().expect("single-col DISTINCT eqfn");
                            let mut fc2 = LocalFcinfo::<2>::fresh(ps.agg_collation);
                            // SAFETY: as the transfn arming above.
                            unsafe { fc2.set_result_mcx(estate.ecxt(tmp).per_tuple_mcx()) };
                            fc2.args[0] = NullableDatum { value: o.value, isnull: false };
                            fc2.args[1] = NullableDatum { value: nd.value, isnull: false };
                            eq.invoke(&mut fc2)?.as_bool()
                        };
                        if equal {
                            continue;
                        }
                    }
                }
                fcinfo.args[1] = nd;
                advance_transition_function(
                    &mut fcinfo,
                    &mut ps.transfn,
                    typ,
                    ps.num_trans_inputs,
                    *agg_node,
                    pg,
                )?;
                old = Some(nd);
            }
            sort.end();
        } else {
            let mut have_old = false;
            loop {
                let got =
                    sort.gettupleslot(true, false, ps.slot1.as_mut().expect("sortslot"), mcx)?;
                if !got {
                    break;
                }
                let matched = if ps.num_distinct_cols > 0 && have_old {
                    let (s1, s2) = (
                        // Two disjoint options; split borrows via as_mut.
                        &mut ps.slot1,
                        &mut ps.slot2,
                    );
                    let mut slots = EvalSlots {
                        scan: None,
                        inner: s2.as_mut().map(|s| &mut *s),
                        outer: s1.as_mut().map(|s| &mut *s),
                    };
                    exec_qual(ps.equalfn_multi.as_deref_mut(), &mut slots)?
                } else {
                    false
                };
                if !matched {
                    {
                        let s1 = ps.slot1.as_mut().unwrap();
                        exectuples::slot_getsomeattrs(s1, ps.num_trans_inputs as i32);
                        let base = s1.base();
                        for i in 0..ps.num_trans_inputs {
                            fcinfo.args[i + 1] = NullableDatum {
                                value: base.tts_values[i],
                                isnull: base.tts_isnull[i],
                            };
                        }
                    }
                    advance_transition_function(
                        &mut fcinfo,
                        &mut ps.transfn,
                        typ,
                        ps.num_trans_inputs,
                        *agg_node,
                        pg,
                    )?;
                    if ps.num_distinct_cols > 0 {
                        core::mem::swap(&mut ps.slot1, &mut ps.slot2);
                        have_old = true;
                    }
                }
                estate.reset_expr_context(tmp);
                exectuples::exec_clear_tuple(ps.slot1.as_mut().unwrap(), mcx);
            }
            exectuples::exec_clear_tuple(ps.slot1.as_mut().unwrap(), mcx);
            if let Some(s2) = ps.slot2.as_mut() {
                exectuples::exec_clear_tuple(s2, mcx);
            }
            sort.end();
        }
    }
    Ok(())
}

/// `ExecAgg` -> `agg_retrieve_direct` (nodeAgg.c), single-group arm: drain the
/// outer child through the transition program, then finalize and project the
/// one result row. Zero input rows still produce a row (C contract).
pub fn exec_agg<'mcx, F>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    mut fetch_outer: F,
) -> PgResult<Option<ExecSlotId>>
where
    F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
{
    if node.agg_done {
        return Ok(None);
    }
    if node.gsets.is_some() {
        return gsets::exec_agg_gsets(node, estate, &mut fetch_outer);
    }
    if node.plan.aggstrategy == AGG_HASHED {
        if !node.perhash.as_ref().expect("hashed Agg has perhash").table_filled {
            agg_fill_hash_table(node, estate, &mut fetch_outer)?;
        }
        return agg_retrieve_hash_table(node, estate);
    }
    if node.plan.aggstrategy == AGG_SORTED {
        return agg_retrieve_sorted(node, estate, &mut fetch_outer);
    }
    initialize_aggregates(node)?;

    while let Some(outer_id) = fetch_outer(estate)? {
        estate.ecxt_mut(node.tmpcontext).ecxt_outertuple = Some(outer_id);
        let outer_slot = estate.slot_mut(outer_id);
        let mut slots = EvalSlots { scan: None, inner: None, outer: Some(outer_slot) };
        exec_eval_expr(node.evaltrans.as_mut().unwrap(), &mut slots)?;
        if !node.pertrans_sort.is_empty() {
            collect_ordered_input(node, estate)?;
        }
        estate.reset_expr_context(node.tmpcontext);
    }
    plain_finish(node, estate)
}

// exec_agg's post-drain tail (finalize + HAVING + project), shared with the
// batched drive.
fn plain_finish<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    process_ordered_aggregates(node, estate)?;
    estate.reset_expr_context(node.ps_ExprContext);
    finalize_aggregates(node, estate, node.pergroup_base)?;
    node.agg_done = true;

    // project_aggregates: the HAVING qual (var-free here) gates the one row.
    if node.proj.has_subplan() || node.qual.as_deref().is_some_and(|q| q.has_subplan()) {
        let ecxt = node.ps_ExprContext;
        if !::executils::exec_qual_with_subplans(node.qual.as_deref_mut(), estate, ecxt)? {
            return Ok(None);
        }
        ::executils::exec_project_with_subplans(
            &mut node.proj,
            estate,
            ecxt,
            node.ps_ResultTupleSlot,
        )?;
        return Ok(Some(node.ps_ResultTupleSlot));
    }
    let mut slots = EvalSlots { scan: None, inner: None, outer: None };
    if !exec_qual(node.qual.as_deref_mut(), &mut slots)? {
        return Ok(None);
    }
    let mcx = estate.es_query_cxt;
    let result_slot = estate.slot_mut(node.ps_ResultTupleSlot);
    let mut slots = EvalSlots { scan: None, inner: None, outer: None };
    exec_project(&mut node.proj, &mut slots, result_slot, mcx)?;
    Ok(Some(node.ps_ResultTupleSlot))
}

/// Page-batch feed for the fused agg-over-scan drive (upstream batch
/// executor design, CF 6176); implemented over the concrete scan node by the
/// dispatcher, which owns both sides.
pub trait AggBatchSource<'mcx> {
    /// Stage the next page batch; 0 = input exhausted.
    fn next_batch(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<u32>;
    /// Store staged tuple `i` into the outer slot and apply the scan qual;
    /// false = filtered out.
    fn fetch_tuple(&mut self, i: u32, estate: &mut EStateData<'mcx>) -> PgResult<bool>;
    fn outer_slot(&self) -> ExecSlotId;
    fn has_qual(&self) -> bool;
    /// True only when `next_batch` counts VISIBLE, qual-passing rows (the
    /// storeless drain never calls `fetch_tuple`). Sources resolving
    /// visibility or quals at fetch time must return false.
    fn storeless_ok(&self) -> bool {
        !self.has_qual()
    }
}

/// Shapes `exec_agg_batched` handles; the dispatcher falls back to the
/// per-tuple drive otherwise.
pub fn agg_batch_drainable(node: &AggStateData<'_>) -> bool {
    node.gsets.is_none()
        && node.pertrans_sort.is_empty()
        && (node.plan.aggstrategy == AGG_PLAIN || node.plan.aggstrategy == AGG_HASHED)
        && node.evaltrans.is_some()
}

/// Outer-slot deform prefix the batched drive reads per row (evaltrans
/// FETCHSOME bound + hashed grouping columns); None = shape unknown, the
/// SoA batch deform stays disarmed.
pub fn agg_batch_outer_prefix(node: &AggStateData<'_>) -> Option<i32> {
    debug_assert!(agg_batch_drainable(node));
    let mut p = node
        .evaltrans
        .as_deref()
        .expect("drainable Agg has evaltrans")
        .max_fetch(::execexpr::SlotSrc::Outer)?;
    if node.plan.aggstrategy == AGG_HASHED {
        p = p.max(node.perhash.as_ref().expect("hashed Agg has perhash").largest_grp_col_idx);
    }
    Some(p)
}

/// `exec_agg` over a page-batch source: identical per-row transition order,
/// minus the per-tuple node recursion (and minus the slot store for
/// input-free transition kernels).
pub fn exec_agg_batched<'mcx, S: AggBatchSource<'mcx>>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    mut src: S,
) -> PgResult<Option<ExecSlotId>> {
    debug_assert!(agg_batch_drainable(node));
    if node.agg_done {
        return Ok(None);
    }
    if node.plan.aggstrategy == AGG_HASHED {
        if !node.perhash.as_ref().expect("hashed Agg has perhash").table_filled {
            agg_fill_hash_table_batched(node, estate, &mut src)?;
        }
        return agg_retrieve_hash_table(node, estate);
    }
    initialize_aggregates(node)?;

    let storeless = src.storeless_ok()
        && matches!(
            node.evaltrans.as_deref().unwrap().kernel(),
            ::execexpr::Kernel::AggTransByVal { .. } | ::execexpr::Kernel::AggTransByValThin { .. }
        );
    loop {
        let n = src.next_batch(estate)?;
        if n == 0 {
            break;
        }
        if storeless {
            for _ in 0..n {
                let mut slots = EvalSlots::default();
                exec_eval_expr(node.evaltrans.as_mut().unwrap(), &mut slots)?;
                estate.reset_expr_context(node.tmpcontext);
            }
        } else {
            for i in 0..n {
                if !src.fetch_tuple(i, estate)? {
                    continue;
                }
                let outer_id = src.outer_slot();
                estate.ecxt_mut(node.tmpcontext).ecxt_outertuple = Some(outer_id);
                let outer_slot = estate.slot_mut(outer_id);
                let mut slots =
                    EvalSlots { scan: None, inner: None, outer: Some(outer_slot) };
                exec_eval_expr(node.evaltrans.as_mut().unwrap(), &mut slots)?;
                estate.reset_expr_context(node.tmpcontext);
            }
        }
    }
    plain_finish(node, estate)
}

fn agg_fill_hash_table_batched<'mcx, S: AggBatchSource<'mcx>>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    src: &mut S,
) -> PgResult<()> {
    loop {
        let n = src.next_batch(estate)?;
        if n == 0 {
            break;
        }
        for i in 0..n {
            if !src.fetch_tuple(i, estate)? {
                continue;
            }
            let outer_id = src.outer_slot();
            estate.ecxt_mut(node.tmpcontext).ecxt_outertuple = Some(outer_id);
            lookup_hash_entry(node, estate, outer_id)?;
            {
                let outer_slot = estate.slot_mut(outer_id);
                let mut slots =
                    EvalSlots { scan: None, inner: None, outer: Some(outer_slot) };
                exec_eval_expr(node.evaltrans.as_mut().unwrap(), &mut slots)?;
            }
            estate.reset_expr_context(node.tmpcontext);
        }
    }
    let ph = node.perhash.as_mut().unwrap();
    ph.table_filled = true;
    ph.hashiter = 0;
    hash_agg_update_metrics(node, estate);
    Ok(())
}

const MAX_FINAL_ARGS: usize = 8;

// finalize_aggregate(s) (nodeAgg.c): finalfn results land in ps_ExprContext's
// per-tuple memory via the armed result mcx (C's MemoryContextContains +
// datumCopy discipline); no finalfn = the byval transvalue itself.
pub(crate) fn finalize_aggregates<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &EStateData<'mcx>,
    pergroup: NonNull<AggPerGroup>,
) -> PgResult<()> {
    let per_tuple = estate.ecxt(node.ps_ExprContext).per_tuple_mcx();
    let AggStateData {
        peragg, trans_typ, agg_node, agg_values_base, agg_nulls_base, persort, ..
    } = node;
    for (aggno, pa) in peragg.iter_mut().enumerate() {
        // SAFETY: transno < the once-allocated pergroup array length; base
        // pointers are the sole access paths (struct invariants).
        let pg = unsafe { &*pergroup.as_ptr().add(pa.transno as usize) };
        // C MakeExpandedObjectReadOnly on the transvalue (both arms).
        // SAFETY: a non-null by-ref transvalue points at a live image.
        let trans_value = if !pg.trans_value_is_null && trans_typ[pa.transno as usize].len == -1
        {
            unsafe {
                datum::expandeddatum::make_expanded_object_read_only_internal(pg.trans_value)
            }
        } else {
            pg.trans_value
        };
        let mut direct: [NullableDatum; MAX_FINAL_ARGS] =
            [NullableDatum::null(); MAX_FINAL_ARGS];
        let mut anynull = false;
        assert!(
            pa.direct_args.len() < MAX_FINAL_ARGS,
            "finalize_aggregate (nodeAgg.c): {} direct args not supported",
            pa.direct_args.len()
        );
        for (i, es) in pa.direct_args.iter_mut().enumerate() {
            let mut slots = EvalSlots {
                scan: None,
                inner: None,
                outer: persort.as_mut().map(|ps| &mut ps.first_slot),
            };
            let nd = exec_eval_expr(es, &mut slots)?;
            direct[i] = nd;
            anynull |= nd.isnull;
        }
        let (value, isnull) = match pa.finalfn.as_mut() {
            None => (trans_value, pg.trans_value_is_null),
            Some(flinfo) => {
                assert!(
                    (pa.num_final_args as usize) <= MAX_FINAL_ARGS,
                    "finalize_aggregate (nodeAgg.c): {} finalfn args not supported",
                    pa.num_final_args
                );
                let mut fcinfo = LocalFcinfo::<MAX_FINAL_ARGS>::fresh(pa.agg_collation);
                fcinfo.nargs = pa.num_final_args as i16;
                fcinfo.context = Some(agg_node.cast());
                // SAFETY: the per-tuple context outlives this stack frame's
                // single call.
                unsafe { fcinfo.set_result_mcx(per_tuple) };
                fcinfo.args[0] =
                    NullableDatum { value: trans_value, isnull: pg.trans_value_is_null };
                for i in 0..pa.direct_args.len() {
                    fcinfo.args[i + 1] = direct[i];
                }
                anynull |= pg.trans_value_is_null
                    || pa.num_final_args as usize > pa.direct_args.len() + 1;
                // SAFETY: query-lifetime node; no &mut lives across the call.
                let agg = unsafe { agg_node.as_ref() };
                agg.set_current_agg(NonNull::from(pa.aggref).cast(), pa.trans_shared);
                let out = if flinfo.fn_strict && anynull {
                    (Datum::null(), true)
                } else {
                    let result = flinfo.invoke(&mut fcinfo)?;
                    let isnull = fcinfo.isnull;
                    // C MakeExpandedObjectReadOnly on the result.
                    // SAFETY: a non-null varlena result points at a live image.
                    let value = if !isnull && pa.resulttype_len == -1 {
                        unsafe {
                            datum::expandeddatum::make_expanded_object_read_only_internal(result)
                        }
                    } else {
                        result
                    };
                    (value, isnull)
                };
                agg.clear_current_agg();
                out
            }
        };
        // SAFETY: aggno < the once-allocated result array lengths.
        unsafe {
            agg_values_base.as_ptr().add(aggno).write(value);
            agg_nulls_base.as_ptr().add(aggno).write(isnull);
        }
    }
    Ok(())
}

// agg_retrieve_direct (nodeAgg.c), AGG_SORTED single-set arm: one group per
// pass; the boundary tuple is copied into the pending slot and swapped in as
// the next group's first tuple. Group copies live in the query context
// (C pfrees each; bump arenas reclaim at query end).
fn agg_retrieve_sorted<'mcx, F>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    fetch_outer: &mut F,
) -> PgResult<Option<ExecSlotId>>
where
    F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
{
    let mcx = estate.es_query_cxt;
    while !node.agg_done {
        estate.reset_expr_context(node.ps_ExprContext);
        // SAFETY: sole access path to the node during the reset (the frames'
        // copies are raw and dormant between evaluations).
        unsafe { node.agg_node.as_mut() }.reset();

        {
            let AggStateData { persort, .. } = node;
            let ps = persort.as_mut().expect("sorted Agg has persort");
            if ps.have_pending {
                core::mem::swap(&mut ps.first_slot, &mut ps.pending_slot);
                ps.have_pending = false;
            } else {
                match fetch_outer(estate)? {
                    Some(outer_id) => {
                        let outer_slot = estate.slot_mut(outer_id);
                        exectuples::exec_copy_slot(&mut ps.first_slot, outer_slot, mcx, mcx)?;
                    }
                    None => {
                        node.agg_done = true;
                        return Ok(None);
                    }
                }
            }
        }
        initialize_aggregates(node)?;
        {
            let AggStateData { persort, evaltrans, .. } = node;
            let ps = persort.as_mut().expect("sorted Agg has persort");
            let mut slots =
                EvalSlots { scan: None, inner: None, outer: Some(&mut ps.first_slot) };
            exec_eval_expr(evaltrans.as_mut().unwrap(), &mut slots)?;
        }
        if !node.pertrans_sort.is_empty() {
            collect_ordered_input(node, estate)?;
        }
        estate.reset_expr_context(node.tmpcontext);
        loop {
            let Some(outer_id) = fetch_outer(estate)? else {
                node.agg_done = true;
                break;
            };
            let AggStateData { persort, evaltrans, .. } = node;
            let ps = persort.as_mut().expect("sorted Agg has persort");
            let outer_slot = estate.slot_mut(outer_id);
            let mut slots = EvalSlots {
                scan: None,
                inner: Some(&mut ps.first_slot),
                outer: Some(&mut *outer_slot),
            };
            let same_group = exec_qual(Some(&mut ps.eq), &mut slots)?;
            if !same_group {
                exectuples::exec_copy_slot(&mut ps.pending_slot, outer_slot, mcx, mcx)?;
                ps.have_pending = true;
                break;
            }
            let mut slots = EvalSlots { scan: None, inner: None, outer: Some(outer_slot) };
            exec_eval_expr(evaltrans.as_mut().unwrap(), &mut slots)?;
            if !node.pertrans_sort.is_empty() {
                collect_ordered_input(node, estate)?;
            }
            estate.reset_expr_context(node.tmpcontext);
        }
        process_ordered_aggregates(node, estate)?;
        finalize_aggregates(node, estate, node.pergroup_base)?;

        {
            let AggStateData { persort, qual, .. } = node;
            let ps = persort.as_mut().expect("sorted Agg has persort");
            let mut slots =
                EvalSlots { scan: None, inner: None, outer: Some(&mut ps.first_slot) };
            if !exec_qual(qual.as_deref_mut(), &mut slots)? {
                continue;
            }
        }
        let result_slot = estate.slot_mut(node.ps_ResultTupleSlot);
        let ps = node.persort.as_mut().unwrap();
        let mut slots = EvalSlots { scan: None, inner: None, outer: Some(&mut ps.first_slot) };
        exec_project(&mut node.proj, &mut slots, result_slot, mcx)?;
        return Ok(Some(node.ps_ResultTupleSlot));
    }
    Ok(None)
}

// agg_fill_hash_table (nodeAgg.c): drain the child through the hash lookup +
// transition program; no spill (lookup panics at the ngroups limit).
fn agg_fill_hash_table<'mcx, F>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    fetch_outer: &mut F,
) -> PgResult<()>
where
    F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
{
    while let Some(outer_id) = fetch_outer(estate)? {
        estate.ecxt_mut(node.tmpcontext).ecxt_outertuple = Some(outer_id);
        lookup_hash_entry(node, estate, outer_id)?;
        {
            let outer_slot = estate.slot_mut(outer_id);
            let mut slots = EvalSlots { scan: None, inner: None, outer: Some(outer_slot) };
            exec_eval_expr(node.evaltrans.as_mut().unwrap(), &mut slots)?;
        }
        estate.reset_expr_context(node.tmpcontext);
    }
    let ph = node.perhash.as_mut().unwrap();
    ph.table_filled = true;
    ph.hashiter = 0;
    hash_agg_update_metrics(node, estate);
    Ok(())
}

// hash_agg_update_metrics (nodeAgg.c), no-spill form: meta = the table's
// entry-array analogue, entry/key mem = the agg context subtree (C's
// hash_tablecxt + hashcontext split lives in one context here).
fn hash_agg_update_metrics(node: &AggStateData<'_>, estate: &mut EStateData<'_>) {
    let ph = node.perhash.as_ref().expect("hashed Agg has perhash");
    // SAFETY: read of the once-allocated node; no &mut is live to it.
    let aggctx = unsafe { node.agg_node.as_ref() }.aggcontext();
    let total = (ph.hashtable.meta_mem() + aggctx.context().subtree_used()) as u64;
    let id = node.plan.plan.plan_node_id;
    let ai = estate
        .es_agg_instrumentation
        .iter_mut()
        .find_map(|(i, ai)| (*i == id).then_some(ai))
        .expect("init_perhash published this node's metrics");
    ai.hash_mem_peak = ai.hash_mem_peak.max(total);
}

// prepare_hash_slot + lookup_hash_entries + initialize_hash_entry
// (nodeAgg.c), single set; repoints the evaltrans pergroup cell.
fn lookup_hash_entry<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    outer_id: ExecSlotId,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let AggStateData { perhash, trans_init, trans_typ, agg_node, .. } = node;
    let ph = perhash.as_mut().expect("hashed Agg has perhash");
    // SAFETY: read of the once-allocated node; no &mut is live to it.
    let table_mcx = unsafe { agg_node.as_ref() }.aggcontext();

    let outer_slot = estate.slot_mut(outer_id);
    exectuples::slot_getsomeattrs(outer_slot, ph.largest_grp_col_idx);
    exectuples::exec_clear_tuple(&mut ph.hashslot, mcx);
    {
        let src = outer_slot.base();
        let dst = ph.hashslot.base_mut();
        for (i, &attno) in ph.hash_grp_col_idx_input.iter().enumerate() {
            let v = (attno - 1) as usize;
            dst.tts_values[i] = src.tts_values[v];
            dst.tts_isnull[i] = src.tts_isnull[v];
        }
    }
    exectuples::exec_store_virtual_tuple(&mut ph.hashslot);

    let hash = ph.hashtable.hash_slot(&mut ph.hashslot)?;
    let (ix, isnew) = ph.hashtable.lookup(&mut ph.hashslot, hash, Some(table_mcx), mcx)?;
    let ix = ix.expect("creating lookup always yields an entry");
    if isnew {
        ph.hash_ngroups_current += 1;
        if ph.hash_ngroups_current > ph.hash_ngroups_limit {
            panic!(
                "hash_agg_check_limits (nodeAgg.c): hash_mem exceeded \
                 ({} groups > limit {}); hashagg spill not ported",
                ph.hash_ngroups_current, ph.hash_ngroups_limit
            );
        }
    }
    if !trans_init.is_empty() {
        let pergroup = ph
            .hashtable
            .entry_additional(ix)
            .expect("numtrans > 0 tables carry additional space")
            .cast::<AggPerGroup>();
        if isnew {
            for (transno, init) in trans_init.iter().enumerate() {
                let typ = trans_typ[transno];
                let value = if !init.isnull && !typ.byval {
                    // SAFETY: node-lifetime initval datum copied into the
                    // table context (C's hashcontext datumCopy).
                    unsafe { ::execexpr::agg_datum_copy(table_mcx, init.value, typ.len)? }
                } else {
                    init.value
                };
                // SAFETY: the entry's additional block holds numtrans
                // AggPerGroup slots, zeroed at creation (execgrouping
                // contract).
                unsafe {
                    pergroup.as_ptr().add(transno).write(AggPerGroup {
                        trans_value: value,
                        trans_value_is_null: init.isnull,
                        no_trans_value: init.isnull,
                    });
                }
            }
        }
        // SAFETY: the cell is a once-allocated live slot the trans steps read.
        unsafe { ph.pergroup_cell.write(pergroup) };
    }
    Ok(())
}

// agg_retrieve_hash_table(_in_memory) (nodeAgg.c): one qual-passing group per
// call, the representative tuple rebuilt into the outer-format first_slot.
fn agg_retrieve_hash_table<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    let mcx = estate.es_query_cxt;
    loop {
        estate.reset_expr_context(node.ps_ExprContext);

        let done = {
            let ph = node.perhash.as_ref().expect("hashed Agg has perhash");
            ph.hashiter >= ph.hashtable.num_entries()
        };
        if done {
            node.agg_done = true;
            return Ok(None);
        }
        let pergroup = {
            let ph = node.perhash.as_mut().expect("hashed Agg has perhash");
            let ix = ph.hashiter as u32;
            ph.hashiter += 1;

            let tup = ph.hashtable.entry_tuple(ix);
            // SAFETY: entry images live in the node's table context for the
            // table's lifetime.
            unsafe { exectuples::exec_store_minimal_tuple_ptr(&mut ph.retrieve_slot, mcx, tup) };
            exectuples::slot_getallattrs(&mut ph.retrieve_slot);

            exectuples::exec_store_all_null_tuple(&mut ph.first_slot, mcx);
            {
                let PerHashData {
                    retrieve_slot: hashslot, first_slot, hash_grp_col_idx_input, ..
                } = &mut *ph;
                let src = hashslot.base();
                let dst = first_slot.base_mut();
                for (i, &attno) in hash_grp_col_idx_input.iter().enumerate() {
                    let v = (attno - 1) as usize;
                    dst.tts_values[v] = src.tts_values[i];
                    dst.tts_isnull[v] = src.tts_isnull[i];
                }
            }
            ph.hashtable.entry_additional(ix).map_or(NonNull::dangling(), |p| p.cast())
        };
        // Written by lookup_hash_entry; unread (and dangling) when peragg is
        // empty.
        finalize_aggregates(node, estate, pergroup)?;

        {
            let AggStateData { perhash, qual, .. } = node;
            let ph = perhash.as_mut().unwrap();
            let mut slots =
                EvalSlots { scan: None, inner: None, outer: Some(&mut ph.first_slot) };
            if !exec_qual(qual.as_deref_mut(), &mut slots)? {
                continue;
            }
        }
        let result_slot = estate.slot_mut(node.ps_ResultTupleSlot);
        let ph = node.perhash.as_mut().unwrap();
        let mut slots = EvalSlots { scan: None, inner: None, outer: Some(&mut ph.first_slot) };
        exec_project(&mut node.proj, &mut slots, result_slot, mcx)?;
        return Ok(Some(node.ps_ResultTupleSlot));
    }
}

/// `ExecEndAgg` node-local half; the caller ends the outer child (contexts
/// are freed with the EState).
pub fn exec_end_agg(node: &mut AggStateData<'_>) {
    node.qual = None;
    node.perhash = None;
    node.persort = None;
    node.gsets = None;
    node.pertrans_sort.clear();
    for pa in node.peragg.iter_mut() {
        pa.finalfn = None;
    }
    node.proj.release_frames();
    if let Some(et) = node.evaltrans.as_mut() {
        et.release_frames();
    }
    node.ps_ResultTupleDesc = None;
}

/// `ExecReScanAgg` (nodeAgg.c) AGG_PLAIN arm; the caller rescans the outer
/// child (chgParam is always NULL until the Param lanes land).
/// ExecReScanAgg (nodeAgg.c), chgParam-nonnull arm: input changed, so hashed
/// results are rebuilt (C reuses only when no params changed in the subtree).
pub fn exec_rescan_agg_chg<'mcx>(node: &mut AggStateData<'mcx>, _estate: &mut EStateData<'mcx>) {
    node.agg_done = false;
    for ps in node.pertrans_sort.iter_mut() {
        if let Some(sort) = ps.sortstate.take() {
            sort.end();
        }
    }
    if let Some(gs) = node.gsets.as_mut() {
        gsets::rescan_grouping_sets(gs).expect("grouping-sets rescan");
        return;
    }
    if let Some(ph) = node.perhash.as_mut() {
        ph.table_filled = false;
        ph.hashiter = 0;
        ph.hash_ngroups_current = 0;
        ph.hashtable.reset();
    }
    if let Some(ps) = node.persort.as_mut() {
        ps.have_pending = false;
    }
    // SAFETY: sole access path to the node during the reset; frees hashed
    // entry images too (they live in aggcontext).
    unsafe { node.agg_node.as_mut() }.reset();
}

pub fn exec_rescan_agg<'mcx>(node: &mut AggStateData<'mcx>, _estate: &mut EStateData<'mcx>) {
    node.agg_done = false;
    for ps in node.pertrans_sort.iter_mut() {
        if let Some(sort) = ps.sortstate.take() {
            sort.end();
        }
    }
    if let Some(gs) = node.gsets.as_mut() {
        // C's no-chgParam AGG_HASHED arm: filled tables are reused, only the
        // iterators reset.
        if !gsets::rescan_hash_reuse(gs) {
            gsets::rescan_grouping_sets(gs).expect("grouping-sets rescan");
        }
        return;
    }
    if let Some(ph) = node.perhash.as_mut() {
        // C's no-chgParam arm: the filled table is reused, only the iterator
        // resets (the caller's child rescan is then redundant but harmless).
        ph.hashiter = 0;
        return;
    }
    if let Some(ps) = node.persort.as_mut() {
        ps.have_pending = false;
    }
    // SAFETY: sole access path to the node during the reset.
    unsafe { node.agg_node.as_mut() }.reset();
}

/// C `AggGetAggref` (nodeAgg.c).
///
/// # Safety
/// `fcinfo.context`, if set, points at a live node outliving `'a`; the
/// cur-agg slot only ever holds `&'query Aggref` pointers.
pub unsafe fn agg_get_aggref<'a>(
    fcinfo: &::types_fmgr::FunctionCallInfoBaseData,
) -> Option<&'a Aggref<'a>> {
    // SAFETY: caller contract.
    let node = unsafe { fcinfo.agg_state_node() }?;
    let (p, _) = node.current_agg()?;
    // SAFETY: writer invariant above.
    Some(unsafe { p.cast::<Aggref<'a>>().as_ref() })
}

/// C `AggStateIsShared` (nodeAgg.c); true (conservative) outside an agg call.
///
/// # Safety
/// As [`agg_get_aggref`].
pub unsafe fn agg_state_is_shared(fcinfo: &::types_fmgr::FunctionCallInfoBaseData) -> bool {
    // SAFETY: caller contract.
    match unsafe { fcinfo.agg_state_node() } {
        Some(node) => node.current_agg().map_or(true, |(_, shared)| shared),
        None => true,
    }
}

/// C `AggRegisterCallback` (nodeAgg.c).
///
/// # Safety
/// As [`agg_get_aggref`], plus `AggStateNode::register_shutdown_callback`'s
/// contract on `func`/`arg`.
pub unsafe fn agg_register_callback(
    fcinfo: &::types_fmgr::FunctionCallInfoBaseData,
    func: unsafe fn(*mut ()),
    arg: *mut (),
) -> PgResult<()> {
    // SAFETY: caller contract.
    match unsafe { fcinfo.agg_state_node() } {
        Some(node) => {
            // SAFETY: caller contract.
            unsafe { node.register_shutdown_callback(func, arg) };
            Ok(())
        }
        None => Err(Box::new(PgError::error(
            "aggregate function cannot register a callback in this context",
        ))),
    }
}

mcx::forget_safe_nodrop!(TransTyp);

// Exempt: all released in exec_end_agg (proj/evaltrans via release_frames).
mcx::forget_safe_struct!(
    PerAggData<'_> { transno, aggref, trans_shared, num_final_args,
        agg_collation, resulttype_len;
        finalfn, direct_args },
    PerSortData<'_> { have_pending; first_slot, pending_slot, eq },
    PerHashData<'_> { num_cols, hash_grp_col_idx_input, largest_grp_col_idx,
        outer_natts, pergroup_cell, hash_ngroups_limit, hash_ngroups_current,
        table_filled, hashiter;
        hashtable, hashslot, retrieve_slot, first_slot },
    AggStateData<'_> { plan, ps_ExprContext, tmpcontext, agg_node,
        ps_ResultTupleSlot, peragg, trans_init, trans_typ, _pergroup,
        pergroup_base, agg_values_base, agg_nulls_base, agg_done, numtrans;
        ps_ResultTupleDesc, proj, evaltrans, perhash, persort, gsets,
        pertrans_sort, qual },
);
