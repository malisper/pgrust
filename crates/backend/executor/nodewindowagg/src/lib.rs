// nodeWindowAgg.c, default-frame slice: frameheadpos is pinned at 0, no
// inverse transitions, frame end = peer-group boundary. The rank family is
// enum-dispatched (C: fmgr + WindowObject; the set is closed here). Explicit
// frames/exclusion/GROUPS/runCondition/FILTER/other window functions/by-ref
// transtypes are loud panics at init.
#![allow(non_snake_case)]

use std::ptr::NonNull;
use std::rc::Rc;

use ::datum::{Datum, NullableDatum};
use ::execexpr::{
    exec_build_agg_trans, exec_build_grouping_equal, exec_build_window_projection_info,
    exec_eval_expr, exec_project, exec_qual, AggBind, AggPerGroup, AggTransSpec, EvalSlots,
    ExprState, WinBind,
};
use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::mcx::{vec_with_capacity_in, PgBox, PgVec};
use ::tuplestore::Tuplestore;
use ::types_core::catalog::PROCEDURE_RELATION_ID;
use ::types_core::{Oid, INT8OID};
use ::types_error::{PgError, PgResult};
use ::types_nodes::list::NodeList;
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::WindowAgg;
use ::types_nodes::primnodes::WindowFunc;
use ::types_nodes::rawnodes::FRAMEOPTION_DEFAULTS;
use ::types_nodes::NodeTag;
use ::types_slot::{SlotData, TupleSlotKind, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK};
use ::types_tuple::TupleDescData;

pub fn init_seams() {}

#[cfg(test)]
mod tests;

const ACL_EXECUTE: u64 = 1 << 7;
const ACLCHECK_OK: i32 = 0;
const AGGKIND_NORMAL: i8 = b'n' as i8;

const F_WINDOW_ROW_NUMBER: Oid = 3100;
const F_WINDOW_RANK: Oid = 3101;
const F_WINDOW_DENSE_RANK: Oid = 3102;

#[derive(Clone, Copy, PartialEq)]
enum WfKind {
    RowNumber,
    Rank,
    DenseRank,
    PlainAgg { aggno: u16 },
}

// C WindowStatePerFuncData + the WindowObject position fields (markptr is
// bookkeeping only: tuplestore_trim is unported, so no mark read pointer).
struct PerFuncData {
    kind: WfKind,
    wfuncno: u16,
    readptr: i32,
    seekpos: i64,
    markpos: i64,
    rank: i64,
}

pub struct WindowAggStateData<'mcx> {
    plan: &'mcx WindowAgg<'mcx>,
    pub ps_ExprContext: EcxtId,
    tmpcontext: EcxtId,
    pub ps_ResultTupleDesc: Rc<TupleDescData<'static>>,
    pub ps_ResultTupleSlot: ExecSlotId,
    proj: PgBox<'mcx, ExprState<'mcx>>,
    part_eq: Option<PgBox<'mcx, ExprState<'mcx>>>,
    ord_eq: Option<PgBox<'mcx, ExprState<'mcx>>>,
    buffer: Option<Tuplestore>,
    scan_slot: SlotData<'mcx>,
    first_part_slot: SlotData<'mcx>,
    first_part_valid: bool,
    agg_row_slot: SlotData<'mcx>,
    agg_row_valid: bool,
    temp_slot_1: SlotData<'mcx>,
    temp_slot_2: SlotData<'mcx>,
    perfunc: PgVec<'mcx, PerFuncData>,
    evaltrans: Option<PgBox<'mcx, ExprState<'mcx>>>,
    trans_init: PgVec<'mcx, NullableDatum>,
    _pergroup: PgVec<'mcx, AggPerGroup>,
    pergroup_base: NonNull<AggPerGroup>,
    peragg_wfuncno: PgVec<'mcx, u16>,
    agg_saved: PgVec<'mcx, NullableDatum>,
    agg_readptr: i32,
    agg_seekpos: i64,
    agg_values_base: NonNull<Datum>,
    agg_nulls_base: NonNull<bool>,
    numaggs: usize,
    currentpos: i64,
    spooled_rows: i64,
    aggregatedupto: i64,
    partition_spooled: bool,
    more_partitions: bool,
    next_partition: bool,
    done: bool,
}

#[cold]
#[inline(never)]
fn wfunc_lookup_failed(fnoid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!("cache lookup failed for aggregate {fnoid}")))
}

#[cold]
#[inline(never)]
fn wfunc_permission_denied(fnoid: Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!("permission denied for function {fnoid}"))
            .with_sqlstate(::types_error::ERRCODE_INSUFFICIENT_PRIVILEGE),
    )
}

// GetAggInitVal (nodeWindowAgg.c keeps its own copy, as nodeAgg.c does);
// only the int8 arm is live (count/sum transtypes).
fn get_agg_init_val(text: &str, transtype: Oid) -> PgResult<Datum> {
    if transtype != INT8OID {
        panic!(
            "GetAggInitVal (nodeWindowAgg.c): typinput dispatch for transtype {transtype} \
             not ported"
        );
    }
    Ok(Datum::from_i64(::adt_int8::int8in(text, None)?))
}

#[cold]
#[inline(never)]
fn unported_window_function(fnoid: Oid) -> ! {
    let name = match fnoid {
        3103 => "percent_rank",
        3104 => "cume_dist",
        3105 => "ntile",
        3106..=3108 => "lag",
        3109..=3111 => "lead",
        3112 => "first_value",
        3113 => "last_value",
        3114 => "nth_value",
        _ => "unknown",
    };
    panic!(
        "eval_windowfunction (nodeWindowAgg.c): window function {name} (oid {fnoid}) \
         not ported (row_number/rank/dense_rank + plain aggregates only)"
    )
}

fn collect_window_funcs<'mcx>(
    node: Node<'mcx>,
    out: &mut PgVec<'mcx, (Node<'mcx>, &'mcx WindowFunc<'mcx>)>,
) {
    match node.node_tag() {
        NodeTag::T_WindowFunc => out.push((node, node.as_window_func().unwrap())),
        NodeTag::T_TargetEntry => {
            collect_window_funcs(node.as_target_entry().unwrap().expr, out)
        }
        NodeTag::T_Var | NodeTag::T_Const => {}
        NodeTag::T_FuncExpr => {
            for a in node.as_func_expr().unwrap().args.iter() {
                collect_window_funcs(a, out);
            }
        }
        NodeTag::T_OpExpr => {
            for a in node.as_op_expr().unwrap().args.iter() {
                collect_window_funcs(a, out);
            }
        }
        tag => panic!(
            "ExecInitWindowAgg (nodeWindowAgg.c): WindowAgg tlist node family {tag:?} \
             not ported"
        ),
    }
}

/// `ExecInitWindowAgg` minus child linkage: the caller (execProcnode's
/// T_WindowAgg arm) inits the outer child and passes its result type.
pub fn exec_init_window_agg<'mcx>(
    node: &'mcx WindowAgg<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
    outer_desc: &Rc<TupleDescData<'static>>,
    result_desc: Rc<TupleDescData<'static>>,
) -> PgResult<WindowAggStateData<'mcx>> {
    debug_assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);
    let mcx = estate.es_query_cxt;

    if node.frameOptions != FRAMEOPTION_DEFAULTS {
        panic!(
            "ExecInitWindowAgg (nodeWindowAgg.c): frameOptions {:#x} not ported \
             (default frame RANGE UNBOUNDED PRECEDING..CURRENT ROW only)",
            node.frameOptions
        );
    }
    assert!(node.startOffset.is_none() && node.endOffset.is_none());
    assert!(node.startInRangeFunc == 0 && node.endInRangeFunc == 0);
    if !node.runCondition.is_nil() || !node.runConditionOrig.is_nil() {
        panic!("ExecInitWindowAgg (nodeWindowAgg.c): runCondition not ported");
    }
    if !node.plan.qual.is_nil() {
        panic!("ExecInitWindowAgg (nodeWindowAgg.c): top-window qual not ported");
    }

    let tmpcontext = estate.create_expr_context();
    let ps_ExprContext = estate.exec_assign_expr_context();
    let ps_ResultTupleSlot =
        estate.exec_init_extra_tuple_slot(Some(result_desc.clone()), TupleSlotKind::Virtual);

    let mut wfuncs: PgVec<'mcx, (Node<'mcx>, &'mcx WindowFunc<'mcx>)> = PgVec::new_in(mcx);
    for tle in node.plan.targetlist.iter() {
        collect_window_funcs(tle, &mut wfuncs);
    }
    // C dedups equal() non-volatile wfuncs onto one wfuncno; equal() has no
    // WindowFunc arm yet, so duplicates each get their own slot (results
    // identical, duplicated evaluation).
    let numfuncs = wfuncs.len();
    let userid = miscinit_seams::get_user_id::call();

    let mut perfunc: PgVec<'mcx, PerFuncData> = vec_with_capacity_in(mcx, numfuncs)?;
    let mut wfuncnos: PgVec<'mcx, (Node<'mcx>, u16)> = vec_with_capacity_in(mcx, numfuncs)?;
    let mut agg_specs_args: PgVec<'mcx, NodeList<'mcx>> = PgVec::new_in(mcx);
    let mut trans_init: PgVec<'mcx, NullableDatum> = PgVec::new_in(mcx);
    let mut trans_fnoid: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    let mut trans_collid: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    let mut trans_typlen: PgVec<'mcx, i16> = PgVec::new_in(mcx);
    let mut peragg_wfuncno: PgVec<'mcx, u16> = PgVec::new_in(mcx);

    for (wfuncno, &(wnode, wfunc)) in wfuncs.iter().enumerate() {
        if wfunc.winref != node.winref {
            panic!(
                "WindowFunc with winref {} assigned to WindowAgg with winref {}",
                wfunc.winref, node.winref
            );
        }
        if wfunc.aggfilter.is_some() {
            panic!("ExecInitWindowAgg (nodeWindowAgg.c): FILTER not ported");
        }
        if !wfunc.runCondition.is_nil() {
            panic!("ExecInitWindowAgg (nodeWindowAgg.c): WindowFunc runCondition not ported");
        }
        let aclresult = aclchk_seams::object_aclcheck::call(
            PROCEDURE_RELATION_ID,
            wfunc.winfnoid,
            userid,
            ACL_EXECUTE,
        )?;
        if aclresult != ACLCHECK_OK {
            return Err(wfunc_permission_denied(wfunc.winfnoid));
        }
        wfuncnos.push((wnode, wfuncno as u16));

        let kind = if wfunc.winagg {
            let aggno = agg_specs_args.len() as u16;
            initialize_peragg(
                mcx,
                wfunc,
                &mut agg_specs_args,
                &mut trans_init,
                &mut trans_fnoid,
                &mut trans_collid,
                &mut trans_typlen,
            )?;
            peragg_wfuncno.push(wfuncno as u16);
            WfKind::PlainAgg { aggno }
        } else {
            match wfunc.winfnoid {
                F_WINDOW_ROW_NUMBER => WfKind::RowNumber,
                F_WINDOW_RANK => WfKind::Rank,
                F_WINDOW_DENSE_RANK => WfKind::DenseRank,
                other => unported_window_function(other),
            }
        };
        perfunc.push(PerFuncData {
            kind,
            wfuncno: wfuncno as u16,
            readptr: -1,
            seekpos: -1,
            markpos: -1,
            rank: 0,
        });
    }
    let numaggs = agg_specs_args.len();

    let mut pergroup: PgVec<'mcx, AggPerGroup> = vec_with_capacity_in(mcx, numaggs)?;
    pergroup.resize(
        numaggs,
        AggPerGroup { trans_value: Datum::null(), trans_value_is_null: true, no_trans_value: true },
    );
    let pergroup_base = NonNull::new(pergroup.as_mut_ptr()).unwrap();

    let (agg_values_base, agg_nulls_base) = {
        let ecxt = estate.ecxt_mut(ps_ExprContext);
        ecxt.ecxt_aggvalues.resize(numfuncs, Datum::null());
        ecxt.ecxt_aggnulls.resize(numfuncs, true);
        (
            NonNull::new(ecxt.ecxt_aggvalues.as_mut_ptr()).unwrap(),
            NonNull::new(ecxt.ecxt_aggnulls.as_mut_ptr()).unwrap(),
        )
    };

    let params = estate.param_bind();
    let evaltrans = if numaggs > 0 {
        let mut specs: PgVec<'mcx, AggTransSpec<'_, 'mcx>> = vec_with_capacity_in(mcx, numaggs)?;
        for aggno in 0..numaggs {
            // SAFETY: aggno < numaggs elements of the once-allocated pergroup.
            let pg = unsafe { NonNull::new_unchecked(pergroup_base.as_ptr().add(aggno)) };
            specs.push(AggTransSpec {
                transfn_oid: trans_fnoid[aggno],
                inputcollid: trans_collid[aggno],
                init_value_is_null: trans_init[aggno].isnull,
                args: &agg_specs_args[aggno],
                pergroup: pg,
                transtype_byval: true,
                transtype_len: trans_typlen[aggno],
            });
        }
        // C arms fcinfo->context with the WindowAggState; None makes a
        // context-reading transfn fail loud (non-aggregate-context error).
        Some(exec_build_agg_trans(mcx, &specs, None, params)?)
    } else {
        None
    };

    let bind = AggBind { values: agg_values_base, nulls: agg_nulls_base, naggs: numfuncs as u16 };
    let proj = exec_build_window_projection_info(
        mcx,
        &node.plan.targetlist,
        None,
        WinBind { agg: bind, wfuncnos: &wfuncnos },
        params,
    )?;

    let part_eq = build_eq(
        mcx,
        outer_desc,
        node.partNumCols,
        node.partColIdx,
        node.partOperators,
        node.partCollations,
    )?;
    let ord_eq = build_eq(
        mcx,
        outer_desc,
        node.ordNumCols,
        node.ordColIdx,
        node.ordOperators,
        node.ordCollations,
    )?;

    let mut mk_slot = || {
        exectuples::make_tuple_table_slot(
            mcx,
            TupleSlotKind::MinimalTuple,
            Some(outer_desc.clone()),
        )
    };
    let scan_slot = mk_slot();
    let first_part_slot = mk_slot();
    let agg_row_slot = mk_slot();
    let temp_slot_1 = mk_slot();
    let temp_slot_2 = mk_slot();
    let mut agg_saved: PgVec<'mcx, NullableDatum> = vec_with_capacity_in(mcx, numaggs)?;
    agg_saved.resize(numaggs, NullableDatum::null());

    Ok(WindowAggStateData {
        plan: node,
        ps_ExprContext,
        tmpcontext,
        ps_ResultTupleDesc: result_desc,
        ps_ResultTupleSlot,
        proj,
        part_eq,
        ord_eq,
        buffer: None,
        scan_slot,
        first_part_slot,
        first_part_valid: false,
        agg_row_slot,
        agg_row_valid: false,
        temp_slot_1,
        temp_slot_2,
        perfunc,
        evaltrans,
        trans_init,
        _pergroup: pergroup,
        pergroup_base,
        peragg_wfuncno,
        agg_saved,
        agg_readptr: -1,
        agg_seekpos: -1,
        agg_values_base,
        agg_nulls_base,
        numaggs,
        currentpos: 0,
        spooled_rows: 0,
        aggregatedupto: 0,
        partition_spooled: false,
        more_partitions: false,
        next_partition: true,
        done: false,
    })
}

fn build_eq<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    desc: &Rc<TupleDescData<'static>>,
    num_cols: i32,
    col_idx: &[i16],
    operators: &[Oid],
    collations: &[Oid],
) -> PgResult<Option<PgBox<'mcx, ExprState<'mcx>>>> {
    if num_cols == 0 {
        return Ok(None);
    }
    debug_assert!(col_idx.len() == num_cols as usize);
    let mut eqfuncoids: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, num_cols as usize)?;
    for &op in operators {
        eqfuncoids.push(lsyscache::get_opcode(op)?);
    }
    Ok(Some(exec_build_grouping_equal(mcx, desc, desc, col_idx, &eqfuncoids, collations)?))
}

// initialize_peragg (nodeWindowAgg.c), byval no-finalfn slice (nodeAgg
// precedent); invtransfn ignored: the default frame never moves its head.
fn initialize_peragg<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    wfunc: &'mcx WindowFunc<'mcx>,
    agg_specs_args: &mut PgVec<'mcx, NodeList<'mcx>>,
    trans_init: &mut PgVec<'mcx, NullableDatum>,
    trans_fnoid: &mut PgVec<'mcx, Oid>,
    trans_collid: &mut PgVec<'mcx, Oid>,
    trans_typlen: &mut PgVec<'mcx, i16>,
) -> PgResult<()> {
    let shape = syscache_seams::lookup_pg_aggregate_shape::call(wfunc.winfnoid)?
        .ok_or_else(|| wfunc_lookup_failed(wfunc.winfnoid))?;
    if shape.aggkind != AGGKIND_NORMAL {
        panic!(
            "initialize_peragg (nodeWindowAgg.c): ordered-set/hypothetical aggkind {} \
             cannot be a window aggregate",
            shape.aggkind
        );
    }
    if shape.aggfinalfn != 0 {
        panic!(
            "finalize_windowaggregate (nodeWindowAgg.c): finalfn {} arm not ported",
            shape.aggfinalfn
        );
    }
    let transtype = shape.aggtranstype;
    let (translen, byval) = lsyscache::get_typlenbyval(transtype)?;
    trans_typlen.push(translen);
    if !byval {
        panic!(
            "advance_windowaggregate (nodeWindowAgg.c): by-ref transtype {transtype} \
             not ported"
        );
    }
    let initval = syscache_seams::pg_aggregate_agginitval::call(mcx, wfunc.winfnoid)?
        .ok_or_else(|| wfunc_lookup_failed(wfunc.winfnoid))?;
    trans_init.push(match initval {
        None => NullableDatum::null(),
        Some(text) => NullableDatum { value: get_agg_init_val(&text, transtype)?, isnull: false },
    });
    trans_fnoid.push(shape.aggtransfn);
    trans_collid.push(wfunc.inputcollid);

    // WindowFunc args are bare exprs; the shared trans builder consumes
    // Aggref-shaped TargetEntry cells.
    let mut args = NodeList::nil();
    for (i, arg) in wfunc.args.iter().enumerate() {
        args.lappend(mcx, Node::mk_target_entry(mcx, arg, (i + 1) as i16, None, false)?)?;
    }
    agg_specs_args.push(args);
    Ok(())
}

#[derive(Clone, Copy)]
enum WhichSlot {
    AggRow,
    Temp1,
    Temp2,
}

impl<'mcx> WindowAggStateData<'mcx> {
    // prepare_tuplestore (nodeWindowAgg.c), default-frame pointer set: ptr 0
    // is the current row, one forward-only agg pointer (frame head cannot
    // move: no agg mark/backward pointer), a BACKWARD pointer per rank-family
    // function. Mark pointers are position bookkeeping only (no trim).
    fn prepare_tuplestore(&mut self) {
        debug_assert!(self.buffer.is_none());
        let work_mem = init_small::globals::work_mem();
        let mut buffer = Tuplestore::begin_heap(false, false, work_mem);
        buffer.set_eflags(0);
        if self.numaggs > 0 {
            self.agg_readptr = buffer.alloc_read_pointer(0);
        }
        for pf in self.perfunc.iter_mut() {
            if !matches!(pf.kind, WfKind::PlainAgg { .. }) {
                pf.readptr = buffer.alloc_read_pointer(EXEC_FLAG_BACKWARD);
            }
        }
        self.buffer = Some(buffer);
    }

    fn begin_partition<F>(&mut self, estate: &mut EStateData<'mcx>, fetch: &mut F) -> PgResult<()>
    where
        F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
    {
        let mcx = estate.es_query_cxt;
        self.partition_spooled = false;
        self.spooled_rows = 0;
        self.currentpos = 0;
        self.agg_row_valid = false;
        exectuples::exec_clear_tuple(&mut self.agg_row_slot, mcx);

        if !self.first_part_valid {
            match fetch(estate)? {
                Some(outer_id) => {
                    let outer_slot = estate.slot_mut(outer_id);
                    exectuples::exec_copy_slot(&mut self.first_part_slot, outer_slot, mcx, mcx)?;
                    self.first_part_valid = true;
                }
                None => {
                    self.partition_spooled = true;
                    self.more_partitions = false;
                    return Ok(());
                }
            }
        }
        if self.buffer.is_none() {
            self.prepare_tuplestore();
        }
        self.next_partition = false;

        if self.numaggs > 0 {
            self.agg_seekpos = -1;
            self.aggregatedupto = 0;
        }
        for pf in self.perfunc.iter_mut() {
            if !matches!(pf.kind, WfKind::PlainAgg { .. }) {
                pf.seekpos = -1;
                pf.markpos = -1;
                pf.rank = 0;
            }
        }
        self.buffer.as_mut().unwrap().puttupleslot(&mut self.first_part_slot, mcx)?;
        self.spooled_rows += 1;
        Ok(())
    }

    // spool_tuples: pos == -1 spools the whole partition (the pass-through
    // and spilled-store arms are unreachable: no runCondition, no spill).
    fn spool_tuples<F>(
        &mut self,
        estate: &mut EStateData<'mcx>,
        fetch: &mut F,
        pos: i64,
    ) -> PgResult<()>
    where
        F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
    {
        if self.buffer.is_none() || self.partition_spooled {
            return Ok(());
        }
        let mcx = estate.es_query_cxt;
        while self.spooled_rows <= pos || pos == -1 {
            let Some(outer_id) = fetch(estate)? else {
                self.partition_spooled = true;
                self.more_partitions = false;
                break;
            };
            if self.plan.partNumCols > 0 {
                let same = {
                    let outer_slot = estate.slot_mut(outer_id);
                    let mut slots = EvalSlots {
                        scan: None,
                        inner: Some(&mut self.first_part_slot),
                        outer: Some(outer_slot),
                    };
                    exec_qual(self.part_eq.as_deref_mut(), &mut slots)?
                };
                estate.reset_expr_context(self.tmpcontext);
                if !same {
                    let outer_slot = estate.slot_mut(outer_id);
                    exectuples::exec_copy_slot(&mut self.first_part_slot, outer_slot, mcx, mcx)?;
                    self.partition_spooled = true;
                    self.more_partitions = true;
                    break;
                }
            }
            let outer_slot = estate.slot_mut(outer_id);
            self.buffer.as_mut().unwrap().puttupleslot(outer_slot, mcx)?;
            self.spooled_rows += 1;
        }
        Ok(())
    }

    fn release_partition(&mut self, estate: &mut EStateData<'mcx>) {
        let mcx = estate.es_query_cxt;
        // Rank state lives in perfunc (C: partcontext localmem); byval trans
        // values need no aggcontext reset.
        if let Some(buffer) = self.buffer.as_mut() {
            buffer.clear();
        }
        exectuples::exec_clear_tuple(&mut self.scan_slot, mcx);
        self.agg_row_valid = false;
        exectuples::exec_clear_tuple(&mut self.agg_row_slot, mcx);
        self.partition_spooled = false;
        self.next_partition = true;
    }

    // are_peers: no ORDER BY means all partition rows are peers.
    fn are_peers(
        estate: &mut EStateData<'mcx>,
        ord_eq: Option<&mut ExprState<'mcx>>,
        tmpcontext: EcxtId,
        slot1: &mut SlotData<'mcx>,
        slot2: &mut SlotData<'mcx>,
    ) -> PgResult<bool> {
        let Some(ord_eq) = ord_eq else {
            return Ok(true);
        };
        let mut slots = EvalSlots { scan: None, inner: Some(slot2), outer: Some(slot1) };
        let r = exec_qual(Some(ord_eq), &mut slots)?;
        estate.reset_expr_context(tmpcontext);
        Ok(r)
    }

    // window_gettupleslot over (readptr, seekpos): borrowed fetches
    // (copy=false) are sound because the store never spills or trims within
    // a partition (C copies to survive both).
    fn gettupleslot_at<F>(
        &mut self,
        estate: &mut EStateData<'mcx>,
        fetch: &mut F,
        perfunc_ix: Option<usize>,
        pos: i64,
        which_slot: WhichSlot,
    ) -> PgResult<bool>
    where
        F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
    {
        if pos < 0 {
            return Ok(false);
        }
        self.spool_tuples(estate, fetch, pos)?;
        if pos >= self.spooled_rows {
            return Ok(false);
        }
        let (readptr, seekpos, markpos) = match perfunc_ix {
            Some(i) => {
                let pf = &self.perfunc[i];
                (pf.readptr, pf.seekpos, pf.markpos)
            }
            None => (self.agg_readptr, self.agg_seekpos, -1),
        };
        if pos < markpos {
            panic!("cannot fetch row before WindowObject's mark position");
        }
        let mcx = estate.es_query_cxt;
        let buffer = self.buffer.as_mut().unwrap();
        buffer.select_read_pointer(readptr);
        let mut seekpos = seekpos;
        if seekpos < pos - 1 {
            if !buffer.skiptuples(pos - 1 - seekpos, true) {
                panic!("unexpected end of tuplestore");
            }
            seekpos = pos - 1;
        } else if seekpos > pos + 1 {
            if !buffer.skiptuples(seekpos - (pos + 1), false) {
                panic!("unexpected end of tuplestore");
            }
            seekpos = pos + 1;
        } else if seekpos == pos {
            buffer.advance(true);
            seekpos += 1;
        }
        let slot = match which_slot {
            WhichSlot::AggRow => &mut self.agg_row_slot,
            WhichSlot::Temp1 => &mut self.temp_slot_1,
            WhichSlot::Temp2 => &mut self.temp_slot_2,
        };
        if seekpos > pos {
            if !buffer.gettupleslot(false, false, slot, mcx)? {
                panic!("unexpected end of tuplestore");
            }
            seekpos -= 1;
        } else {
            if !buffer.gettupleslot(true, false, slot, mcx)? {
                panic!("unexpected end of tuplestore");
            }
            seekpos += 1;
        }
        debug_assert!(seekpos == pos);
        match perfunc_ix {
            Some(i) => self.perfunc[i].seekpos = seekpos,
            None => self.agg_seekpos = seekpos,
        }
        Ok(true)
    }

    // WinSetMarkPosition minus the mark read pointer (no trim): the read
    // pointer still advances so later fetches never seek before the mark.
    fn set_mark_position(&mut self, perfunc_ix: usize, markpos: i64) {
        let pf = &mut self.perfunc[perfunc_ix];
        if markpos < pf.markpos {
            panic!("cannot move WindowObject's mark position backward");
        }
        pf.markpos = markpos;
        if markpos > pf.seekpos {
            let buffer = self.buffer.as_mut().unwrap();
            buffer.select_read_pointer(pf.readptr);
            buffer.skiptuples(markpos - pf.seekpos, true);
            pf.seekpos = markpos;
        }
    }

    // rank_up (windowfuncs.c): peer check against the prior row, then the
    // mark advances to the current row.
    fn rank_up<F>(
        &mut self,
        estate: &mut EStateData<'mcx>,
        fetch: &mut F,
        perfunc_ix: usize,
    ) -> PgResult<bool>
    where
        F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
    {
        let curpos = self.currentpos;
        let mut up = false;
        if self.perfunc[perfunc_ix].rank == 0 {
            debug_assert!(curpos == 0);
            self.perfunc[perfunc_ix].rank = 1;
        } else {
            debug_assert!(curpos > 0);
            // WinRowsArePeers(curpos - 1, curpos).
            if !self.gettupleslot_at(estate, fetch, Some(perfunc_ix), curpos - 1, WhichSlot::Temp1)?
            {
                panic!("specified position is out of window: {}", curpos - 1);
            }
            if !self.gettupleslot_at(estate, fetch, Some(perfunc_ix), curpos, WhichSlot::Temp2)? {
                panic!("specified position is out of window: {curpos}");
            }
            let Self { ref mut temp_slot_1, ref mut temp_slot_2, ref mut ord_eq, tmpcontext, .. } =
                *self;
            up = !Self::are_peers(
                estate,
                ord_eq.as_deref_mut(),
                tmpcontext,
                temp_slot_1,
                temp_slot_2,
            )?;
        }
        self.set_mark_position(perfunc_ix, curpos);
        Ok(up)
    }

    fn eval_windowfunction<F>(
        &mut self,
        estate: &mut EStateData<'mcx>,
        fetch: &mut F,
        perfunc_ix: usize,
    ) -> PgResult<()>
    where
        F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
    {
        let result = match self.perfunc[perfunc_ix].kind {
            WfKind::RowNumber => {
                let curpos = self.currentpos;
                self.set_mark_position(perfunc_ix, curpos);
                curpos + 1
            }
            WfKind::Rank => {
                let up = self.rank_up(estate, fetch, perfunc_ix)?;
                if up {
                    self.perfunc[perfunc_ix].rank = self.currentpos + 1;
                }
                self.perfunc[perfunc_ix].rank
            }
            WfKind::DenseRank => {
                let up = self.rank_up(estate, fetch, perfunc_ix)?;
                if up {
                    self.perfunc[perfunc_ix].rank += 1;
                }
                self.perfunc[perfunc_ix].rank
            }
            WfKind::PlainAgg { .. } => unreachable!("plain aggs go through eval_windowaggregates"),
        };
        let wfuncno = self.perfunc[perfunc_ix].wfuncno as usize;
        // SAFETY: wfuncno < numfuncs elements of the once-allocated arrays.
        unsafe {
            self.agg_values_base.as_ptr().add(wfuncno).write(Datum::from_i64(result));
            self.agg_nulls_base.as_ptr().add(wfuncno).write(false);
        }
        Ok(())
    }

    // eval_windowaggregates, default-frame arm: aggregates restart only on
    // the partition's first row; row_is_in_frame collapses to the peer test.
    fn eval_windowaggregates<F>(
        &mut self,
        estate: &mut EStateData<'mcx>,
        fetch: &mut F,
    ) -> PgResult<()>
    where
        F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
    {
        // Frame unchanged since the previous row: reuse the saved results.
        if self.aggregatedupto > self.currentpos {
            for aggno in 0..self.numaggs {
                let wfuncno = self.peragg_wfuncno[aggno] as usize;
                let saved = self.agg_saved[aggno];
                // SAFETY: as eval_windowfunction.
                unsafe {
                    self.agg_values_base.as_ptr().add(wfuncno).write(saved.value);
                    self.agg_nulls_base.as_ptr().add(wfuncno).write(saved.isnull);
                }
            }
            return Ok(());
        }

        if self.currentpos == 0 {
            for (aggno, init) in self.trans_init.iter().enumerate() {
                // SAFETY: aggno < the pergroup array's once-allocated length.
                unsafe {
                    self.pergroup_base.as_ptr().add(aggno).write(AggPerGroup {
                        trans_value: init.value,
                        trans_value_is_null: init.isnull,
                        no_trans_value: init.isnull,
                    });
                }
            }
        }

        // Advance until a row past the current peer group (or partition end).
        loop {
            if !self.agg_row_valid {
                if !self.gettupleslot_at(
                    estate,
                    fetch,
                    None,
                    self.aggregatedupto,
                    WhichSlot::AggRow,
                )? {
                    break;
                }
                self.agg_row_valid = true;
            }
            if self.aggregatedupto > self.currentpos {
                let Self {
                    ref mut agg_row_slot,
                    ref mut scan_slot,
                    ref mut ord_eq,
                    tmpcontext,
                    ..
                } = *self;
                if !Self::are_peers(
                    estate,
                    ord_eq.as_deref_mut(),
                    tmpcontext,
                    agg_row_slot,
                    scan_slot,
                )? {
                    // C leaves agg_row_slot holding this row for the next call.
                    break;
                }
            }
            {
                let mut slots =
                    EvalSlots { scan: None, inner: None, outer: Some(&mut self.agg_row_slot) };
                exec_eval_expr(self.evaltrans.as_mut().unwrap(), &mut slots)?;
            }
            estate.reset_expr_context(self.tmpcontext);
            self.aggregatedupto += 1;
            self.agg_row_valid = false;
        }

        // finalize (no finalfn in the live set) + save for frame reuse; all
        // result types byval, so the save is a plain copy.
        for aggno in 0..self.numaggs {
            let wfuncno = self.peragg_wfuncno[aggno] as usize;
            // SAFETY: as the initialize loop above.
            let pg = unsafe { *self.pergroup_base.as_ptr().add(aggno) };
            let result = NullableDatum { value: pg.trans_value, isnull: pg.trans_value_is_null };
            self.agg_saved[aggno] = result;
            // SAFETY: as eval_windowfunction.
            unsafe {
                self.agg_values_base.as_ptr().add(wfuncno).write(result.value);
                self.agg_nulls_base.as_ptr().add(wfuncno).write(result.isnull);
            }
        }
        Ok(())
    }
}

/// `ExecWindowAgg`, WINDOWAGG_RUN-only shape (no runCondition -> no
/// pass-through modes; no qual -> no filter loop).
pub fn exec_window_agg<'mcx, F>(
    state: &mut WindowAggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    mut fetch_outer: F,
) -> PgResult<Option<ExecSlotId>>
where
    F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
{
    if init_small::globals::InterruptPending() {
        postgres_seams::check_for_interrupts::call()?;
    }
    if state.done {
        return Ok(None);
    }
    let fetch = &mut fetch_outer;

    if state.next_partition {
        state.begin_partition(estate, fetch)?;
    } else {
        state.currentpos += 1;
    }
    state.spool_tuples(estate, fetch, state.currentpos)?;
    if state.partition_spooled && state.currentpos >= state.spooled_rows {
        state.release_partition(estate);
        if state.more_partitions {
            state.begin_partition(estate, fetch)?;
            debug_assert!(state.spooled_rows > 0);
        } else {
            state.done = true;
            return Ok(None);
        }
    }

    estate.reset_expr_context(state.ps_ExprContext);

    {
        let mcx = estate.es_query_cxt;
        let buffer = state.buffer.as_mut().unwrap();
        buffer.select_read_pointer(0);
        if !buffer.gettupleslot(true, false, &mut state.scan_slot, mcx)? {
            panic!("unexpected end of tuplestore");
        }
    }

    for i in 0..state.perfunc.len() {
        if !matches!(state.perfunc[i].kind, WfKind::PlainAgg { .. }) {
            state.eval_windowfunction(estate, fetch, i)?;
        }
    }
    if state.numaggs > 0 {
        state.eval_windowaggregates(estate, fetch)?;
    }

    let mcx = estate.es_query_cxt;
    let result_slot = estate.slot_mut(state.ps_ResultTupleSlot);
    let mut slots = EvalSlots { scan: None, inner: None, outer: Some(&mut state.scan_slot) };
    exec_project(&mut state.proj, &mut slots, result_slot, mcx)?;
    Ok(Some(state.ps_ResultTupleSlot))
}

/// `ExecEndWindowAgg` node-local half; the caller ends the outer child.
pub fn exec_end_window_agg(node: &mut WindowAggStateData<'_>) {
    if let Some(buffer) = node.buffer.take() {
        buffer.end();
    }
}

/// `ExecReScanWindowAgg`; the caller (execami) rescans the outer child.
pub fn exec_rescan_window_agg<'mcx>(
    node: &mut WindowAggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) {
    node.done = false;
    node.release_partition(estate);
    let mcx = estate.es_query_cxt;
    exectuples::exec_clear_tuple(&mut node.first_part_slot, mcx);
    node.first_part_valid = false;
    exectuples::exec_clear_tuple(&mut node.temp_slot_1, mcx);
    exectuples::exec_clear_tuple(&mut node.temp_slot_2, mcx);
    let numfuncs = node.perfunc.len();
    let ecxt = estate.ecxt_mut(node.ps_ExprContext);
    for i in 0..numfuncs {
        ecxt.ecxt_aggvalues[i] = Datum::null();
        ecxt.ecxt_aggnulls[i] = false;
    }
}
