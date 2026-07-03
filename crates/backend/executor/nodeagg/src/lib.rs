// nodeAgg.c, AGG_PLAIN/AGG_SORTED/AGG_HASHED single-grouping-set slice: byval
// transtype only (INTERNAL is a byval pointer datum; its state lives in the
// AggStateNode aggcontext the transfn reaches via fcinfo->context), finalfn
// via resolve-once peragg carriers, no FILTER/DISTINCT/ORDER BY; transitions
// compile into one execexpr program (C's evaltrans). AGG_MIXED, aggsplit
// variants, grouping sets and spill are loud panics.
#![allow(non_snake_case)]

use core::alloc::Layout;
use std::ptr::NonNull;
use std::rc::Rc;

use ::datum::{Datum, NullableDatum};
use ::types_fmgr::{AggStateNode, FmNodePtr, FmgrInfo, LocalFcinfo};
use ::execexpr::{
    exec_build_agg_projection_info, exec_build_agg_qual, exec_build_agg_trans,
    exec_build_agg_trans_hashed, exec_eval_expr, exec_project, exec_qual, AggBind, AggPerGroup,
    AggTransSpec, EvalSlots, ExprState,
};
use ::execgrouping::TupleHashTable;
use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::mcx::{vec_with_capacity_in, Allocator, MemoryContext, PgBox, PgVec};
use ::types_core::catalog::PROCEDURE_RELATION_ID;
use ::types_core::{Oid, INT8OID};
use ::types_error::{PgError, PgResult};
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::Agg;
use ::types_nodes::primnodes::{Aggref, AGGKIND_NORMAL};
use ::types_nodes::NodeTag;
use ::types_pathnodes::{AGGSPLIT_SIMPLE, AGG_HASHED, AGG_PLAIN, AGG_SORTED};
use ::types_slot::{SlotData, TupleSlotKind};
use ::types_tuple::TupleDescData;

pub fn init_seams() {}

#[cfg(test)]
mod tests;

const ACL_EXECUTE: u64 = 1 << 7;
const ACLCHECK_OK: i32 = 0;

pub struct AggStateData<'mcx> {
    pub plan: &'mcx Agg<'mcx>,
    pub ps_ExprContext: EcxtId,
    pub tmpcontext: EcxtId,
    // C's curaggcontext, in the FmNode the transfn fcinfos carry; raw arena
    // cell so the pointer survives self moving (drop: make_agg_state_node).
    agg_node: NonNull<AggStateNode>,
    pub ps_ResultTupleDesc: Rc<TupleDescData<'static>>,
    pub ps_ResultTupleSlot: ExecSlotId,
    proj: PgBox<'mcx, ExprState<'mcx>>,
    evaltrans: PgBox<'mcx, ExprState<'mcx>>,
    peragg: PgVec<'mcx, PerAggData>,
    trans_init: PgVec<'mcx, NullableDatum>,
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
    qual: Option<PgBox<'mcx, ExprState<'mcx>>>,
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
struct PerAggData {
    transno: u32,
    finalfn: Option<FmgrInfo>,
    num_final_args: u16,
    agg_collation: Oid,
    resulttype_len: i16,
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

fn collect_aggrefs<'mcx>(node: Node<'mcx>, out: &mut PgVec<'mcx, &'mcx Aggref<'mcx>>) {
    match node.node_tag() {
        NodeTag::T_Aggref => out.push(node.as_aggref().unwrap()),
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
        tag => panic!("ExecInitAgg (nodeAgg.c): Agg tlist node family {tag:?} not ported"),
    }
}

// GetAggInitVal (nodeAgg.c): C dispatches through the transtype's typinput;
// only the int8 arm is live (count/sum transtypes).
fn get_agg_init_val(text: &str, transtype: Oid) -> PgResult<Datum> {
    if transtype != INT8OID {
        panic!("GetAggInitVal (nodeAgg.c): typinput dispatch for transtype {transtype} not ported");
    }
    Ok(Datum::from_i64(::adt_int8::int8in(text, None)?))
}

/// `ExecInitAgg` (nodeAgg.c). The caller (execProcnode's T_Agg arm) inits the
/// outer child and passes this node's result type.
pub fn exec_init_agg<'mcx>(
    node: &'mcx Agg<'mcx>,
    estate: &mut EStateData<'mcx>,
    _eflags: i32,
    result_desc: Rc<TupleDescData<'static>>,
) -> PgResult<AggStateData<'mcx>> {
    let mcx = estate.es_query_cxt;
    if node.aggstrategy != AGG_PLAIN
        && node.aggstrategy != AGG_HASHED
        && node.aggstrategy != AGG_SORTED
    {
        panic!(
            "ExecInitAgg (nodeAgg.c): aggstrategy {} (AGG_MIXED) not ported",
            node.aggstrategy
        );
    }
    if node.aggsplit != AGGSPLIT_SIMPLE {
        panic!("ExecInitAgg (nodeAgg.c): aggsplit {} not ported", node.aggsplit);
    }
    if !node.groupingSets.is_nil() || !node.chain.is_nil() {
        panic!("ExecInitAgg (nodeAgg.c): grouping sets not ported");
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

    let mut aggrefs: PgVec<'mcx, &'mcx Aggref<'mcx>> = PgVec::new_in(mcx);
    for tle in node.plan.targetlist.iter() {
        collect_aggrefs(tle, &mut aggrefs);
    }
    for q in node.plan.qual.iter() {
        collect_aggrefs(q, &mut aggrefs);
    }
    // tlist and qual Aggrefs can share aggnos (find_compatible_agg);
    // numaggs == 0 is C's hashed-DISTINCT shape.
    let numaggs = aggrefs.iter().map(|a| a.aggno + 1).max().unwrap_or(0) as usize;
    assert!(
        numaggs > 0 || node.aggstrategy == AGG_HASHED,
        "ExecInitAgg: Agg node without Aggrefs outside AGG_HASHED"
    );

    let mut by_aggno: PgVec<'mcx, Option<&'mcx Aggref<'mcx>>> =
        vec_with_capacity_in(mcx, numaggs)?;
    by_aggno.resize(numaggs, None);
    let mut numtrans = 0usize;
    for aggref in aggrefs.iter() {
        let (aggno, transno) = (aggref.aggno, aggref.aggtransno);
        assert!(aggno >= 0 && transno >= 0, "Aggref without planner aggno/aggtransno");
        assert!((aggno as usize) < numaggs, "Aggref.aggno out of range");
        if let Some(prev) = by_aggno[aggno as usize] {
            assert!(
                prev.aggfnoid == aggref.aggfnoid && prev.aggtransno == transno,
                "shared aggno with diverging Aggrefs"
            );
        }
        by_aggno[aggno as usize] = Some(aggref);
        numtrans = numtrans.max(transno as usize + 1);
    }

    let userid = miscinit_seams::get_user_id::call();
    // Droppy FmgrInfo carriers: AggStateData's box owns the drops
    // (ExprState.frames precedent), hence no no-drop ctor.
    let mut peragg: PgVec<'mcx, PerAggData> = PgVec::new_in(mcx);
    peragg
        .try_reserve(numaggs)
        .map_err(|_| mcx.oom(numaggs * core::mem::size_of::<PerAggData>()))?;
    let mut trans_init: PgVec<'mcx, NullableDatum> = vec_with_capacity_in(mcx, numtrans)?;
    trans_init.resize(numtrans, NullableDatum::null());
    let mut trans_aggref: PgVec<'mcx, Option<&'mcx Aggref<'mcx>>> =
        vec_with_capacity_in(mcx, numtrans)?;
    trans_aggref.resize(numtrans, None);
    let mut trans_fnoid: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, numtrans)?;
    trans_fnoid.resize(numtrans, 0);

    for aggno in 0..numaggs {
        let aggref = by_aggno[aggno].expect("planner aggno numbering has gaps");
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
        if shape.aggkind != AGGKIND_NORMAL {
            panic!(
                "ExecInitAgg (nodeAgg.c): ordered-set/hypothetical aggkind {} not ported",
                shape.aggkind
            );
        }
        if !aggref.aggorder.is_nil() || !aggref.aggdistinct.is_nil() {
            panic!("ExecInitAgg (nodeAgg.c): DISTINCT/ORDER BY aggregates not ported");
        }
        if aggref.aggfilter.is_some() {
            panic!("ExecInitAgg (nodeAgg.c): FILTER not ported");
        }
        let transtype = aggref.aggtranstype;
        assert!(transtype != 0, "Aggref.aggtranstype unset (planner must resolve it)");
        let (_len, byval) = lsyscache::get_typlenbyval(transtype)?;
        if !byval {
            panic!(
                "ExecAggCopyTransValue (nodeAgg.c): by-ref transtype {transtype} not ported"
            );
        }

        let finalfn = if shape.aggfinalfn != 0 {
            // Divergence: C aclchecks as the aggregate owner (proowner
            // projection unported); differs only under SET ROLE.
            let aclresult = aclchk_seams::object_aclcheck::call(
                PROCEDURE_RELATION_ID,
                shape.aggfinalfn,
                userid,
                ACL_EXECUTE,
            )?;
            if aclresult != ACLCHECK_OK {
                return Err(agg_permission_denied(shape.aggfinalfn));
            }
            Some(fmgr_core::fmgr_info(shape.aggfinalfn)?)
        } else {
            None
        };
        let num_final_args =
            if shape.aggfinalextra { aggref.args.len() as u16 + 1 } else { 1 };
        let (resulttype_len, _resulttype_byval) = lsyscache::get_typlenbyval(aggref.aggtype)?;

        let transno = aggref.aggtransno as usize;
        peragg.push(PerAggData {
            transno: transno as u32,
            finalfn,
            num_final_args,
            agg_collation: aggref.inputcollid,
            resulttype_len,
        });
        match trans_aggref[transno] {
            Some(prev) => assert!(
                prev.aggfnoid == aggref.aggfnoid,
                "shared transno across different transfns not ported (find_compatible_trans)"
            ),
            None => {
                trans_aggref[transno] = Some(aggref);
                trans_fnoid[transno] = shape.aggtransfn;
                let initval = syscache_seams::pg_aggregate_agginitval::call(mcx, aggref.aggfnoid)?
                    .ok_or_else(|| agg_lookup_failed(aggref.aggfnoid))?;
                trans_init[transno] = match initval {
                    None => NullableDatum::null(),
                    Some(text) => NullableDatum {
                        value: get_agg_init_val(&text, transtype)?,
                        isnull: false,
                    },
                };
            }
        }
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
        let aggref = trans_aggref[transno].expect("planner aggtransno numbering has gaps");
        // SAFETY: transno < numtrans elements of the once-allocated pergroup.
        let pg = unsafe { NonNull::new_unchecked(pergroup_base.as_ptr().add(transno)) };
        specs.push(AggTransSpec {
            transfn_oid: trans_fnoid[transno],
            inputcollid: aggref.inputcollid,
            init_value_is_null: trans_init[transno].isnull,
            args: &aggref.args,
            pergroup: pg,
        });
    }
    let params = estate.param_bind();
    let (evaltrans, perhash) = if node.aggstrategy == AGG_HASHED {
        let ph = init_perhash(node, estate, numtrans)?;
        let evaltrans =
            exec_build_agg_trans_hashed(mcx, &specs, ph.pergroup_cell, fm_agg_node, params)?;
        (evaltrans, Some(ph))
    } else {
        (exec_build_agg_trans(mcx, &specs, fm_agg_node, params)?, None)
    };
    let persort = if node.aggstrategy == AGG_SORTED {
        Some(init_persort(node, estate)?)
    } else {
        None
    };
    let bind = AggBind { values: agg_values_base, nulls: agg_nulls_base, naggs: numaggs as u16 };
    let proj = exec_build_agg_projection_info(mcx, &node.plan.targetlist, None, bind, params)?;
    let qual = exec_build_agg_qual(mcx, &node.plan.qual, bind, params)?;

    Ok(AggStateData {
        plan: node,
        ps_ExprContext,
        tmpcontext,
        agg_node,
        ps_ResultTupleDesc: result_desc,
        ps_ResultTupleSlot,
        proj,
        evaltrans,
        peragg,
        trans_init,
        _pergroup: pergroup,
        pergroup_base,
        agg_values_base,
        agg_nulls_base,
        agg_done: false,
        numtrans,
        perhash,
        persort,
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
        NodeTag::T_Const | NodeTag::T_Aggref => {}
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
    let (mem_limit, hash_ngroups_limit, _) =
        hash_agg_set_limits(hashentrysize, node.numGroups as f64, 0);
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

// initialize_aggregates (nodeAgg.c), byval slice: no datumCopy, no sortstates.
fn initialize_aggregates(node: &mut AggStateData<'_>) {
    for (transno, init) in node.trans_init.iter().enumerate() {
        // SAFETY: transno < the pergroup array's once-allocated length; the
        // base pointer is the sole access path (struct invariant).
        unsafe {
            node.pergroup_base.as_ptr().add(transno).write(AggPerGroup {
                trans_value: init.value,
                trans_value_is_null: init.isnull,
                no_trans_value: init.isnull,
            });
        }
    }
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
    if node.plan.aggstrategy == AGG_HASHED {
        if !node.perhash.as_ref().expect("hashed Agg has perhash").table_filled {
            agg_fill_hash_table(node, estate, &mut fetch_outer)?;
        }
        return agg_retrieve_hash_table(node, estate);
    }
    if node.plan.aggstrategy == AGG_SORTED {
        return agg_retrieve_sorted(node, estate, &mut fetch_outer);
    }
    initialize_aggregates(node);

    while let Some(outer_id) = fetch_outer(estate)? {
        estate.ecxt_mut(node.tmpcontext).ecxt_outertuple = Some(outer_id);
        let outer_slot = estate.slot_mut(outer_id);
        let mut slots = EvalSlots { scan: None, inner: None, outer: Some(outer_slot) };
        exec_eval_expr(&mut node.evaltrans, &mut slots)?;
        estate.reset_expr_context(node.tmpcontext);
    }
    estate.reset_expr_context(node.ps_ExprContext);
    finalize_aggregates(node, estate, node.pergroup_base)?;
    node.agg_done = true;

    // project_aggregates: the HAVING qual (var-free here) gates the one row.
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

const MAX_FINAL_ARGS: usize = 4;

// finalize_aggregate(s) (nodeAgg.c): finalfn results land in ps_ExprContext's
// per-tuple memory via the armed result mcx (C's MemoryContextContains +
// datumCopy discipline); no finalfn = the byval transvalue itself.
fn finalize_aggregates<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &EStateData<'mcx>,
    pergroup: NonNull<AggPerGroup>,
) -> PgResult<()> {
    let per_tuple = estate.ecxt(node.ps_ExprContext).per_tuple_mcx();
    let AggStateData { peragg, agg_node, agg_values_base, agg_nulls_base, .. } = node;
    for (aggno, pa) in peragg.iter_mut().enumerate() {
        // SAFETY: transno < the once-allocated pergroup array length; base
        // pointers are the sole access paths (struct invariants).
        let pg = unsafe { &*pergroup.as_ptr().add(pa.transno as usize) };
        let (value, isnull) = match pa.finalfn.as_mut() {
            None => (pg.trans_value, pg.trans_value_is_null),
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
                    NullableDatum { value: pg.trans_value, isnull: pg.trans_value_is_null };
                let anynull = pg.trans_value_is_null || pa.num_final_args > 1;
                if flinfo.fn_strict && anynull {
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
                }
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
        initialize_aggregates(node);
        {
            let AggStateData { persort, evaltrans, .. } = node;
            let ps = persort.as_mut().expect("sorted Agg has persort");
            let mut slots =
                EvalSlots { scan: None, inner: None, outer: Some(&mut ps.first_slot) };
            exec_eval_expr(evaltrans, &mut slots)?;
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
            exec_eval_expr(evaltrans, &mut slots)?;
            estate.reset_expr_context(node.tmpcontext);
        }
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
            exec_eval_expr(&mut node.evaltrans, &mut slots)?;
        }
        estate.reset_expr_context(node.tmpcontext);
    }
    let ph = node.perhash.as_mut().unwrap();
    ph.table_filled = true;
    ph.hashiter = 0;
    Ok(())
}

// prepare_hash_slot + lookup_hash_entries + initialize_hash_entry
// (nodeAgg.c), single set; repoints the evaltrans pergroup cell.
fn lookup_hash_entry<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    outer_id: ExecSlotId,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let AggStateData { perhash, trans_init, agg_node, .. } = node;
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
                // SAFETY: the entry's additional block holds numtrans
                // AggPerGroup slots, zeroed at creation (execgrouping
                // contract).
                unsafe {
                    pergroup.as_ptr().add(transno).write(AggPerGroup {
                        trans_value: init.value,
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
pub fn exec_end_agg(_node: &mut AggStateData<'_>) {}

/// `ExecReScanAgg` (nodeAgg.c) AGG_PLAIN arm; the caller rescans the outer
/// child (chgParam is always NULL until the Param lanes land).
pub fn exec_rescan_agg<'mcx>(node: &mut AggStateData<'mcx>, _estate: &mut EStateData<'mcx>) {
    node.agg_done = false;
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
