// nodeAgg.c, AGG_PLAIN one-phase slice: single grouping set, byval transtype,
// no finalfn/FILTER/DISTINCT/ORDER BY/HAVING; the transition loop is compiled
// into one execexpr program (C's phase->evaltrans). The outer child stays with
// the ExecProcNode dispatcher via a monomorphized fetch closure (nodesort
// precedent). AGG_SORTED/AGG_HASHED/AGG_MIXED, aggsplit variants, grouping
// sets and the spill machinery are loud panics.
#![allow(non_snake_case)]

use std::ptr::NonNull;
use std::rc::Rc;

use ::datum::{Datum, NullableDatum};
use ::execexpr::{
    exec_build_agg_projection_info, exec_build_agg_trans, exec_eval_expr, exec_project, AggBind,
    AggPerGroup, AggTransSpec, EvalSlots, ExprState,
};
use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::mcx::{vec_with_capacity_in, PgBox, PgVec};
use ::types_core::catalog::PROCEDURE_RELATION_ID;
use ::types_core::{Oid, INT8OID};
use ::types_error::{PgError, PgResult};
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::Agg;
use ::types_nodes::primnodes::{Aggref, AGGKIND_NORMAL};
use ::types_nodes::NodeTag;
use ::types_pathnodes::{AGGSPLIT_SIMPLE, AGG_PLAIN};
use ::types_slot::TupleSlotKind;
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
    pub aggcontext: EcxtId,
    pub ps_ResultTupleDesc: Rc<TupleDescData<'static>>,
    pub ps_ResultTupleSlot: ExecSlotId,
    proj: PgBox<'mcx, ExprState<'mcx>>,
    evaltrans: PgBox<'mcx, ExprState<'mcx>>,
    peragg_transno: PgVec<'mcx, u32>,
    trans_init: PgVec<'mcx, NullableDatum>,
    // Owners of once-allocated arrays; all element access goes through the
    // *_base pointers so the step-held pointers stay valid (steps.rs note).
    _pergroup: PgVec<'mcx, AggPerGroup>,
    pergroup_base: NonNull<AggPerGroup>,
    agg_values_base: NonNull<Datum>,
    agg_nulls_base: NonNull<bool>,
    agg_done: bool,
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
    if node.aggstrategy != AGG_PLAIN {
        panic!(
            "ExecInitAgg (nodeAgg.c): aggstrategy {} (AGG_SORTED/HASHED/MIXED) not ported",
            node.aggstrategy
        );
    }
    if node.aggsplit != AGGSPLIT_SIMPLE {
        panic!("ExecInitAgg (nodeAgg.c): aggsplit {} not ported", node.aggsplit);
    }
    if node.numCols != 0 || !node.groupingSets.is_nil() || !node.chain.is_nil() {
        panic!("ExecInitAgg (nodeAgg.c): grouped aggregation / grouping sets not ported");
    }
    if !node.plan.qual.is_nil() {
        panic!("ExecInitAgg (nodeAgg.c): HAVING qual not ported");
    }

    let aggcontext = estate.create_work_expr_context();
    let tmpcontext = estate.create_expr_context();
    let ps_ExprContext = estate.exec_assign_expr_context();
    let ps_ResultTupleSlot =
        estate.exec_init_extra_tuple_slot(Some(result_desc.clone()), TupleSlotKind::Virtual);

    let mut aggrefs: PgVec<'mcx, &'mcx Aggref<'mcx>> = PgVec::new_in(mcx);
    for tle in node.plan.targetlist.iter() {
        collect_aggrefs(tle, &mut aggrefs);
    }
    let numaggs = aggrefs.len();
    assert!(numaggs > 0, "ExecInitAgg: Agg node without Aggrefs");

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
    let mut peragg_transno: PgVec<'mcx, u32> = vec_with_capacity_in(mcx, numaggs)?;
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
        if shape.aggfinalfn != 0 {
            panic!(
                "finalize_aggregate (nodeAgg.c): finalfn {} arm not ported",
                shape.aggfinalfn
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

        let transno = aggref.aggtransno as usize;
        peragg_transno.push(transno as u32);
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
    let evaltrans = exec_build_agg_trans(mcx, &specs)?;
    let bind = AggBind { values: agg_values_base, nulls: agg_nulls_base, naggs: numaggs as u16 };
    let proj = exec_build_agg_projection_info(mcx, &node.plan.targetlist, None, bind)?;

    Ok(AggStateData {
        plan: node,
        ps_ExprContext,
        tmpcontext,
        aggcontext,
        ps_ResultTupleDesc: result_desc,
        ps_ResultTupleSlot,
        proj,
        evaltrans,
        peragg_transno,
        trans_init,
        _pergroup: pergroup,
        pergroup_base,
        agg_values_base,
        agg_nulls_base,
        agg_done: false,
    })
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
    initialize_aggregates(node);

    while let Some(outer_id) = fetch_outer(estate)? {
        estate.ecxt_mut(node.tmpcontext).ecxt_outertuple = Some(outer_id);
        let outer_slot = estate.slot_mut(outer_id);
        let mut slots = EvalSlots { scan: None, inner: None, outer: Some(outer_slot) };
        exec_eval_expr(&mut node.evaltrans, &mut slots)?;
        estate.reset_expr_context(node.tmpcontext);
    }

    // finalize_aggregates: no finalfn in the live set, so the result is the
    // (byval) transvalue itself.
    for (aggno, transno) in node.peragg_transno.iter().enumerate() {
        // SAFETY: indices bounded by the once-allocated array lengths; base
        // pointers are the sole access paths (struct invariant).
        unsafe {
            let pg = node.pergroup_base.as_ptr().add(*transno as usize);
            node.agg_values_base.as_ptr().add(aggno).write((*pg).trans_value);
            node.agg_nulls_base.as_ptr().add(aggno).write((*pg).trans_value_is_null);
        }
    }

    let mcx = estate.es_query_cxt;
    let result_slot = estate.slot_mut(node.ps_ResultTupleSlot);
    let mut slots = EvalSlots { scan: None, inner: None, outer: None };
    exec_project(&mut node.proj, &mut slots, result_slot, mcx)?;
    node.agg_done = true;
    Ok(Some(node.ps_ResultTupleSlot))
}

/// `ExecEndAgg` node-local half; the caller ends the outer child (contexts
/// are freed with the EState).
pub fn exec_end_agg(_node: &mut AggStateData<'_>) {}

/// `ExecReScanAgg` (nodeAgg.c) AGG_PLAIN arm; the caller rescans the outer
/// child (chgParam is always NULL until the Param lanes land).
pub fn exec_rescan_agg<'mcx>(node: &mut AggStateData<'mcx>, estate: &mut EStateData<'mcx>) {
    node.agg_done = false;
    estate.ecxt_mut(node.aggcontext).rescan();
}
