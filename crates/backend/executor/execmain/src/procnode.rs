use std::rc::Rc;

use ::execexpr::{EvalSlots, ExprState};
use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::mcx::{Mcx, PgBox};
use ::types_error::PgResult;
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::Plan;
use ::types_nodes::NodeTag;
use ::types_slot::SlotData;
use ::types_tuple::TupleDescData;

use crate::noderesult::{exec_end_result, exec_init_result, exec_result, ResultState};

pub struct PlanStateBase<'mcx> {
    pub plan: &'mcx Plan<'mcx>,
    pub ps_ExprContext: Option<EcxtId>,
    // 'static, not 'mcx: result descriptors are Rc-shared with the QueryDesc
    // (C aliases the same pointer as queryDesc->tupDesc).
    pub ps_ResultTupleDesc: Option<Rc<TupleDescData<'static>>>,
    pub ps_ResultTupleSlot: Option<ExecSlotId>,
    pub ps_ProjInfo: Option<PgBox<'mcx, ExprState<'mcx>>>,
    pub qual: Option<PgBox<'mcx, ExprState<'mcx>>>,
}

// C's ExecProcNodeInstr pointer swap: instrumented trees wrap every node at
// init, so uninstrumented dispatch carries no per-tuple flag test.
pub struct InstrumentedNode<'mcx> {
    pub inner: PlanStateNode<'mcx>,
    pub instr_idx: u32,
}

pub enum PlanStateNode<'mcx> {
    Instrumented(PgBox<'mcx, InstrumentedNode<'mcx>>),
    Result(ResultState<'mcx>),
    SeqScan(::nodeseqscan::SeqScanState<'mcx>),
    IndexScan(::nodeindexscan::IndexScanState<'mcx>),
    IndexOnlyScan(::nodeindexonlyscan::IndexOnlyScanState<'mcx>),
    Agg(PgBox<'mcx, AggPlanState<'mcx>>),
    Sort(SortNode<'mcx>),
    Limit(LimitNode<'mcx>),
    BitmapHeapScan(PgBox<'mcx, BitmapHeapPlanState<'mcx>>),
    BitmapIndexScan(::nodebitmapindexscan::BitmapIndexScanState<'mcx>),
    BitmapAnd(PgBox<'mcx, BitmapCombineState<'mcx>>),
    BitmapOr(PgBox<'mcx, BitmapCombineState<'mcx>>),
    ModifyTable(PgBox<'mcx, ModifyTablePlanState<'mcx>>),
}

// ModifyTable's subplan lives here too (nodesort/nodeagg precedent) —
// exec_proc passes a fetch closure.
pub struct ModifyTablePlanState<'mcx> {
    pub mt: ::nodemodifytable::ModifyTableState<'mcx>,
    pub subplan: PlanStateNode<'mcx>,
}

// The bitmapqual subtree lives here, not in nodebitmapheapscan (crate cycle
// with the node-enum owner; Agg precedent) — exec_proc runs MultiExec on it.
pub struct BitmapHeapPlanState<'mcx> {
    pub scan: ::nodebitmapheapscan::BitmapHeapScanState<'mcx>,
    pub bitmapqual: PlanStateNode<'mcx>,
}

// nodeBitmapAnd.c/nodeBitmapOr.c state: only the subplan list (the And/Or
// MultiExec bodies live in multi_exec_bitmap_node below, next to the
// recursion they need).
pub struct BitmapCombineState<'mcx> {
    pub substates: ::mcx::PgVec<'mcx, PlanStateNode<'mcx>>,
}

// The Agg node's outer child lives here, not in nodeagg (crate cycle with the
// node-enum owner; nodesort precedent) — exec_proc passes a fetch closure.
pub struct AggPlanState<'mcx> {
    pub agg: ::nodeagg::AggStateData<'mcx>,
    pub outer: PlanStateNode<'mcx>,
}

pub struct SortNode<'mcx> {
    pub state: ::nodesort::SortState<'mcx>,
    pub outer: PgBox<'mcx, PlanStateNode<'mcx>>,
    pub outer_desc: Rc<TupleDescData<'static>>,
}

pub struct LimitNode<'mcx> {
    pub state: ::nodelimit::LimitState<'mcx>,
    pub outer: PgBox<'mcx, PlanStateNode<'mcx>>,
}

// Init-time tree node touched by &mut per tuple; rule-9 budget covers the per-row carriers inside.
const _: () = assert!(core::mem::size_of::<PlanStateNode<'static>>() <= 1024);

impl<'mcx> PlanStateNode<'mcx> {
    #[inline]
    pub fn ps_expr_context(&self) -> Option<EcxtId> {
        match self {
            PlanStateNode::Instrumented(w) => w.inner.ps_expr_context(),
            PlanStateNode::Result(rs) => rs.ps.ps_ExprContext,
            PlanStateNode::SeqScan(ss) => Some(ss.ss.ps_ExprContext),
            PlanStateNode::IndexScan(is) => Some(is.ss.ps_ExprContext),
            PlanStateNode::IndexOnlyScan(ios) => Some(ios.ss.ps_ExprContext),
            PlanStateNode::Agg(aps) => Some(aps.agg.ps_ExprContext),
            // C sorts have no ExprContext.
            PlanStateNode::Sort(_) => None,
            PlanStateNode::Limit(l) => Some(l.state.ps_ExprContext),
            PlanStateNode::BitmapHeapScan(b) => Some(b.scan.ss.ps_ExprContext),
            PlanStateNode::BitmapIndexScan(_)
            | PlanStateNode::BitmapAnd(_)
            | PlanStateNode::BitmapOr(_)
            | PlanStateNode::ModifyTable(_) => None,
        }
    }

    /// `ExecGetResultType` (execUtils.c). Scan nodes don't retain a desc when
    /// projection is elided, so the root type is rebuilt from the targetlist
    /// (C's ExecInitResultTypeTL desc, same content).
    pub fn exec_get_result_type(
        &self,
        plan: &Plan<'mcx>,
    ) -> PgResult<Rc<TupleDescData<'static>>> {
        match self {
            PlanStateNode::Instrumented(w) => w.inner.exec_get_result_type(plan),
            PlanStateNode::Result(rs) => Ok(rs
                .ps
                .ps_ResultTupleDesc
                .clone()
                .expect("ResultState without a result type")),
            PlanStateNode::SeqScan(_)
            | PlanStateNode::IndexScan(_)
            | PlanStateNode::IndexOnlyScan(_)
            | PlanStateNode::Limit(_)
            | PlanStateNode::BitmapHeapScan(_) => crate::exec_type_from_tl(&plan.targetlist),
            // No RETURNING: the tlist is NIL and the result type is empty.
            PlanStateNode::ModifyTable(_) => crate::exec_type_from_tl(&plan.targetlist),
            PlanStateNode::Agg(aps) => Ok(aps.agg.ps_ResultTupleDesc.clone()),
            PlanStateNode::Sort(s) => Ok(::nodesort::sort_result_type(&s.state)),
            PlanStateNode::BitmapIndexScan(_)
            | PlanStateNode::BitmapAnd(_)
            | PlanStateNode::BitmapOr(_) => {
                panic!("ExecGetResultType on a bitmap-producing node")
            }
        }
    }
}

macro_rules! unported_nodes {
    ($tag:expr, { $($t:ident => $file:literal),+ $(,)? }) => {
        match $tag {
            $(NodeTag::$t => panic!(concat!(
                "ExecInitNode (execProcnode.c): ", stringify!($t), " (", $file, ") not ported"
            )),)+
            other => panic!("ExecInitNode: unrecognized node type: {other:?}"),
        }
    };
}

/// `ExecInitNode` (execProcnode.c).
pub fn exec_init_node<'mcx>(
    node: Option<Node<'mcx>>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<Option<PlanStateNode<'mcx>>> {
    let Some(node) = node else {
        return Ok(None);
    };
    let result = match node.node_tag() {
        NodeTag::T_Result => PlanStateNode::Result(exec_init_result(
            node.as_result().unwrap(),
            estate,
            eflags,
        )?),
        NodeTag::T_SeqScan => {
            let mcx = estate.es_query_cxt;
            PlanStateNode::SeqScan(::nodeseqscan::exec_init_seq_scan(
                mcx,
                node.as_seq_scan().unwrap(),
                estate,
                eflags,
            )?)
        }
        NodeTag::T_IndexScan => {
            let mcx = estate.es_query_cxt;
            PlanStateNode::IndexScan(::nodeindexscan::exec_init_index_scan(
                mcx,
                node.as_index_scan().unwrap(),
                estate,
                eflags,
            )?)
        }
        NodeTag::T_IndexOnlyScan => {
            let mcx = estate.es_query_cxt;
            PlanStateNode::IndexOnlyScan(::nodeindexonlyscan::exec_init_index_only_scan(
                mcx,
                node.as_index_only_scan().unwrap(),
                estate,
                eflags,
            )?)
        }
        NodeTag::T_BitmapHeapScan => {
            let mcx = estate.es_query_cxt;
            let bhs_plan = node.as_bitmap_heap_scan().unwrap();
            let scan = ::nodebitmapheapscan::exec_init_bitmap_heap_scan(
                mcx, bhs_plan, estate, eflags,
            )?;
            let bitmapqual = exec_init_node(bhs_plan.scan.plan.lefttree, estate, eflags)?
                .unwrap_or_else(|| {
                    panic!("ExecInitBitmapHeapScan: BitmapHeapScan without a bitmapqual subplan")
                });
            PlanStateNode::BitmapHeapScan(::mcx::alloc_in(
                mcx,
                BitmapHeapPlanState { scan, bitmapqual },
            )?)
        }
        NodeTag::T_BitmapIndexScan => {
            let mcx = estate.es_query_cxt;
            PlanStateNode::BitmapIndexScan(::nodebitmapindexscan::exec_init_bitmap_index_scan(
                mcx,
                node.as_bitmap_index_scan().unwrap(),
                estate,
                eflags,
            )?)
        }
        NodeTag::T_BitmapAnd => {
            let mcx = estate.es_query_cxt;
            let plan = node.as_bitmap_and().unwrap();
            PlanStateNode::BitmapAnd(::mcx::alloc_in(
                mcx,
                init_bitmap_combine(&plan.bitmapplans, estate, eflags)?,
            )?)
        }
        NodeTag::T_BitmapOr => {
            let mcx = estate.es_query_cxt;
            let plan = node.as_bitmap_or().unwrap();
            if plan.isshared {
                panic!("ExecInitBitmapOr: isshared (parallel bitmap scan lane) not ported");
            }
            PlanStateNode::BitmapOr(::mcx::alloc_in(
                mcx,
                init_bitmap_combine(&plan.bitmapplans, estate, eflags)?,
            )?)
        }
        NodeTag::T_Sort => {
            let sort_plan = node.as_sort().unwrap();
            let outer = exec_init_node(
                sort_plan.plan.lefttree,
                estate,
                ::nodesort::sort_child_eflags(eflags),
            )?
            .unwrap_or_else(|| panic!("ExecInitSort (nodeSort.c): Sort without an outer plan"));
            let outer_desc = outer
                .exec_get_result_type(sort_plan.plan.lefttree.unwrap().as_plan().unwrap())?;
            let result_desc = crate::exec_type_from_tl(&sort_plan.plan.targetlist)?;
            let state =
                ::nodesort::exec_init_sort(sort_plan, estate, eflags, &outer_desc, result_desc)?;
            PlanStateNode::Sort(SortNode {
                state,
                outer: ::mcx::alloc_in(estate.es_query_cxt, outer)?,
                outer_desc,
            })
        }
        NodeTag::T_Limit => {
            let limit_plan = node.as_limit().unwrap();
            let outer = exec_init_node(limit_plan.plan.lefttree, estate, eflags)?
                .unwrap_or_else(|| {
                    panic!("ExecInitLimit (nodeLimit.c): Limit without an outer plan")
                });
            let state = ::nodelimit::exec_init_limit(limit_plan, estate, eflags)?;
            PlanStateNode::Limit(LimitNode {
                state,
                outer: ::mcx::alloc_in(estate.es_query_cxt, outer)?,
            })
        }
        NodeTag::T_Agg => {
            let mcx = estate.es_query_cxt;
            let agg_plan = node.as_agg().unwrap();
            let outer = exec_init_node(agg_plan.plan.lefttree, estate, eflags)?
                .unwrap_or_else(|| panic!("ExecInitAgg (nodeAgg.c): Agg without an outer plan"));
            let desc = crate::exec_type_from_tl(&agg_plan.plan.targetlist)?;
            let agg = ::nodeagg::exec_init_agg(agg_plan, estate, eflags, desc)?;
            PlanStateNode::Agg(::mcx::alloc_in(mcx, AggPlanState { agg, outer })?)
        }
        NodeTag::T_ModifyTable => {
            let mcx = estate.es_query_cxt;
            let mt_plan = node.as_modify_table().unwrap();
            let mt = ::nodemodifytable::exec_init_modify_table(mt_plan, estate, eflags)?;
            let subplan = exec_init_node(mt_plan.plan.lefttree, estate, eflags)?
                .expect("ModifyTable has a subplan");
            PlanStateNode::ModifyTable(::mcx::alloc_in(
                mcx,
                ModifyTablePlanState { mt, subplan },
            )?)
        }
        tag => unported_nodes!(tag, {
            T_ProjectSet => "nodeProjectSet.c",
            T_Append => "nodeAppend.c",
            T_MergeAppend => "nodeMergeAppend.c",
            T_RecursiveUnion => "nodeRecursiveunion.c",
            T_SampleScan => "nodeSamplescan.c",
            T_TidScan => "nodeTidscan.c",
            T_TidRangeScan => "nodeTidrangescan.c",
            T_SubqueryScan => "nodeSubqueryscan.c",
            T_FunctionScan => "nodeFunctionscan.c",
            T_TableFuncScan => "nodeTableFuncscan.c",
            T_ValuesScan => "nodeValuesscan.c",
            T_CteScan => "nodeCtescan.c",
            T_NamedTuplestoreScan => "nodeNamedtuplestorescan.c",
            T_WorkTableScan => "nodeWorktablescan.c",
            T_ForeignScan => "nodeForeignscan.c",
            T_CustomScan => "nodeCustom.c",
            T_NestLoop => "nodeNestloop.c",
            T_MergeJoin => "nodeMergejoin.c",
            T_HashJoin => "nodeHashjoin.c",
            T_Material => "nodeMaterial.c",
            T_IncrementalSort => "nodeIncrementalSort.c",
            T_Memoize => "nodeMemoize.c",
            T_Group => "nodeGroup.c",
            T_WindowAgg => "nodeWindowAgg.c",
            T_Unique => "nodeUnique.c",
            T_Gather => "nodeGather.c",
            T_GatherMerge => "nodeGatherMerge.c",
            T_Hash => "nodeHash.c",
            T_SetOp => "nodeSetOp.c",
            T_LockRows => "nodeLockRows.c",
        }),
    };
    if !node.as_plan().expect("plan-tree node").initPlan.is_nil() {
        panic!("ExecInitNode (execProcnode.c): initPlan lane (nodeSubplan.c) not ported");
    }
    if estate.es_instrument != 0 {
        return Ok(Some(instrument_node(result, node, estate)?));
    }
    Ok(Some(result))
}

// C: `result->instrument = InstrAlloc(1, estate->es_instrument, ...)`.
fn instrument_node<'mcx>(
    inner: PlanStateNode<'mcx>,
    node: Node<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PlanStateNode<'mcx>> {
    if matches!(
        inner,
        PlanStateNode::BitmapIndexScan(_)
            | PlanStateNode::BitmapAnd(_)
            | PlanStateNode::BitmapOr(_)
    ) {
        panic!(
            "ExecInitNode (execProcnode.c): instrumentation of MultiExec bitmap nodes \
             (nodeBitmapIndexscan.c InstrStopNode arm) not ported"
        );
    }
    let id = node.as_plan().expect("plan-tree node").plan_node_id;
    let idx = usize::try_from(id).expect("plan_node_id is non-negative");
    if estate.es_instrumentation.len() <= idx {
        let grow = idx + 1 - estate.es_instrumentation.len();
        estate
            .es_instrumentation
            .try_reserve(grow)
            .map_err(|_| estate.es_query_cxt.oom(grow))?;
        estate
            .es_instrumentation
            .resize(idx + 1, ::types_core::instrument::Instrumentation::default());
    }
    ::instrument::instr_init(&mut estate.es_instrumentation[idx], estate.es_instrument);
    Ok(PlanStateNode::Instrumented(::mcx::alloc_in(
        estate.es_query_cxt,
        InstrumentedNode { inner, instr_idx: idx as u32 },
    )?))
}

/// `ExecProcNode`: one match over the closed node set, Result arm inlined.
#[inline]
pub fn exec_proc_node<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    match node {
        // ExecProcNodeInstr (execProcnode.c).
        PlanStateNode::Instrumented(w) => {
            let w = &mut **w;
            let idx = w.instr_idx as usize;
            ::instrument::instr_start_node(&mut estate.es_instrumentation[idx]);
            let result = exec_proc_node(&mut w.inner, estate)?;
            let n_tuples = if result.is_some() { 1.0 } else { 0.0 };
            ::instrument::instr_stop_node(&mut estate.es_instrumentation[idx], n_tuples);
            Ok(result)
        }
        PlanStateNode::Result(rs) => exec_result(rs, estate),
        PlanStateNode::SeqScan(ss) => ::nodeseqscan::exec_seq_scan(ss, estate),
        PlanStateNode::IndexScan(is) => ::nodeindexscan::exec_index_scan(is, estate),
        PlanStateNode::IndexOnlyScan(ios) => {
            ::nodeindexonlyscan::exec_index_only_scan(ios, estate)
        }
        PlanStateNode::Agg(aps) => {
            let aps = &mut **aps;
            let outer = &mut aps.outer;
            ::nodeagg::exec_agg(&mut aps.agg, estate, |e| exec_proc_node(outer, e))
        }
        PlanStateNode::Sort(s) => {
            let SortNode { state, outer, outer_desc } = s;
            ::nodesort::exec_sort(state, estate, outer_desc.clone(), |es| {
                exec_proc_node(outer, es)
            })
        }
        PlanStateNode::Limit(l) => {
            let LimitNode { state, outer } = l;
            ::nodelimit::exec_limit(state, &mut **outer, estate)
        }
        PlanStateNode::BitmapHeapScan(b) => {
            let b = &mut **b;
            if !b.scan.initialized {
                let tbm = multi_exec_bitmap_node(&mut b.bitmapqual, estate)?;
                ::nodebitmapheapscan::bitmap_table_scan_setup(&mut b.scan, estate, tbm)?;
            }
            ::nodebitmapheapscan::exec_bitmap_heap_scan(&mut b.scan, estate)
        }
        PlanStateNode::BitmapIndexScan(_)
        | PlanStateNode::BitmapAnd(_)
        | PlanStateNode::BitmapOr(_) => {
            panic!("bitmap-producing node does not support ExecProcNode call convention")
        }
        PlanStateNode::ModifyTable(mps) => {
            let mps = &mut **mps;
            let subplan = &mut mps.subplan;
            ::nodemodifytable::exec_modify_table(&mut mps.mt, estate, |e| {
                exec_proc_node(subplan, e)
            })
        }
    }
}

fn init_bitmap_combine<'mcx>(
    bitmapplans: &::types_nodes::list::NodeList<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<BitmapCombineState<'mcx>> {
    let mut substates: ::mcx::PgVec<'mcx, PlanStateNode<'mcx>> =
        ::mcx::PgVec::new_in(estate.es_query_cxt);
    substates
        .try_reserve_exact(bitmapplans.len())
        .map_err(|_| estate.es_query_cxt.oom(bitmapplans.len()))?;
    for subplan in bitmapplans.iter() {
        let state = exec_init_node(Some(subplan), estate, eflags)?
            .expect("BitmapAnd/BitmapOr subplan list holds plan nodes");
        substates.push(state);
    }
    Ok(BitmapCombineState { substates })
}

/// `MultiExecProcNode` (execProcnode.c), bitmap arms only: every consumer in
/// core is a bitmap combiner or BitmapHeapScan.
pub fn multi_exec_bitmap_node<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<::tidbitmap::TIDBitmap<'mcx>> {
    match node {
        PlanStateNode::BitmapIndexScan(biss) => {
            ::nodebitmapindexscan::multi_exec_bitmap_index_scan(biss, estate)
        }
        // MultiExecBitmapAnd: intersect, stopping early on an empty result.
        PlanStateNode::BitmapAnd(bc) => {
            let mut result: Option<::tidbitmap::TIDBitmap<'mcx>> = None;
            for sub in bc.substates.iter_mut() {
                let subresult = multi_exec_bitmap_node(sub, estate)?;
                match result.as_mut() {
                    None => result = Some(subresult),
                    Some(r) => r.intersect(&subresult),
                }
                if result.as_ref().is_some_and(|r| r.is_empty()) {
                    break;
                }
            }
            Ok(result.expect("BitmapAnd with no subplans"))
        }
        // MultiExecBitmapOr: BitmapIndexScan children add into the shared
        // result (C's biss_result hand-off); other children get unioned.
        PlanStateNode::BitmapOr(bc) => {
            let mut result: Option<::tidbitmap::TIDBitmap<'mcx>> = None;
            for sub in bc.substates.iter_mut() {
                if let PlanStateNode::BitmapIndexScan(biss) = sub {
                    let tbm = result.get_or_insert_with(|| {
                        ::tidbitmap::TIDBitmap::new(
                            estate.es_query_cxt,
                            init_small::globals::work_mem() as usize * 1024,
                        )
                    });
                    ::nodebitmapindexscan::multi_exec_bitmap_index_scan_into(
                        biss, estate, tbm,
                    )?;
                } else {
                    let subresult = multi_exec_bitmap_node(sub, estate)?;
                    match result.as_mut() {
                        None => result = Some(subresult),
                        Some(r) => r.union(&subresult)?,
                    }
                }
            }
            Ok(result.expect("BitmapOr with no subplans"))
        }
        _ => panic!("MultiExecProcNode: node type does not produce a bitmap"),
    }
}

/// `ExecEndNode` (execProcnode.c).
pub fn exec_end_node<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    match node {
        PlanStateNode::Instrumented(w) => exec_end_node(&mut w.inner, estate),
        PlanStateNode::Result(rs) => exec_end_result(rs, estate),
        PlanStateNode::SeqScan(ss) => ::nodeseqscan::exec_end_seq_scan(ss),
        PlanStateNode::IndexScan(is) => ::nodeindexscan::exec_end_index_scan(is),
        PlanStateNode::IndexOnlyScan(ios) => {
            ::nodeindexonlyscan::exec_end_index_only_scan(ios)
        }
        PlanStateNode::Agg(aps) => {
            ::nodeagg::exec_end_agg(&mut aps.agg);
            exec_end_node(&mut aps.outer, estate)
        }
        PlanStateNode::Sort(s) => {
            ::nodesort::exec_end_sort(&mut s.state);
            exec_end_node(&mut s.outer, estate)
        }
        // C ExecEndLimit only ends the child.
        PlanStateNode::Limit(l) => exec_end_node(&mut l.outer, estate),
        PlanStateNode::BitmapHeapScan(b) => {
            let b = &mut **b;
            exec_end_node(&mut b.bitmapqual, estate)?;
            ::nodebitmapheapscan::exec_end_bitmap_heap_scan(&mut b.scan)
        }
        PlanStateNode::BitmapIndexScan(biss) => {
            ::nodebitmapindexscan::exec_end_bitmap_index_scan(biss)
        }
        PlanStateNode::BitmapAnd(bc) | PlanStateNode::BitmapOr(bc) => {
            for sub in bc.substates.iter_mut() {
                exec_end_node(sub, estate)?;
            }
            Ok(())
        }
        PlanStateNode::ModifyTable(mps) => {
            let mps = &mut **mps;
            ::nodemodifytable::exec_end_modify_table(&mut mps.mt);
            exec_end_node(&mut mps.subplan, estate)
        }
    }
}

/// `ExecShutdownNode` (execProcnode.c): per-node arms are all no-ops for the
/// ported set (Gather/ForeignScan/CustomScan/Hash own real shutdowns).
pub fn exec_shutdown_node<'mcx>(node: &mut PlanStateNode<'mcx>) {
    match node {
        PlanStateNode::Instrumented(w) => exec_shutdown_node(&mut w.inner),
        PlanStateNode::Result(rs) => {
            if let Some(outer) = rs.outer.as_deref_mut() {
                exec_shutdown_node(outer);
            }
        }
        PlanStateNode::SeqScan(_)
        | PlanStateNode::IndexScan(_)
        | PlanStateNode::IndexOnlyScan(_)
        | PlanStateNode::BitmapIndexScan(_) => {}
        PlanStateNode::Agg(aps) => exec_shutdown_node(&mut aps.outer),
        PlanStateNode::Sort(s) => exec_shutdown_node(&mut s.outer),
        PlanStateNode::Limit(l) => exec_shutdown_node(&mut l.outer),
        PlanStateNode::BitmapHeapScan(b) => exec_shutdown_node(&mut b.bitmapqual),
        PlanStateNode::BitmapAnd(bc) | PlanStateNode::BitmapOr(bc) => {
            for sub in bc.substates.iter_mut() {
                exec_shutdown_node(sub);
            }
        }
        PlanStateNode::ModifyTable(mps) => exec_shutdown_node(&mut mps.subplan),
    }
}

/// `ExecSetTupleBound` (execProcnode.c): Sort gets the bound, Result passes
/// it through to its child; every other ported variant is C's silent no-op
/// fall-through (Agg included — C only descends Sort/IncrementalSort/
/// MergeAppend/Result/SubqueryScan/Gather/GatherMerge).
pub fn exec_set_tuple_bound<'mcx>(tuples_needed: i64, node: &mut PlanStateNode<'mcx>) {
    match node {
        PlanStateNode::Instrumented(w) => exec_set_tuple_bound(tuples_needed, &mut w.inner),
        PlanStateNode::Sort(s) => ::nodesort::sort_set_tuple_bound(&mut s.state, tuples_needed),
        PlanStateNode::Result(rs) => {
            if let Some(outer) = rs.outer.as_deref_mut() {
                exec_set_tuple_bound(tuples_needed, outer);
            }
        }
        _ => {}
    }
}

impl<'mcx> ::nodelimit::LimitChild<'mcx> for PlanStateNode<'mcx> {
    fn exec_proc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>> {
        exec_proc_node(self, estate)
    }

    fn set_tuple_bound(&mut self, tuples_needed: i64) {
        exec_set_tuple_bound(tuples_needed, self);
    }
}

// ExprContext slot triple + result slot as disjoint &mut borrows of es_tupleTable.
pub(crate) fn with_eval_slots<'mcx, R>(
    estate: &mut EStateData<'mcx>,
    ecxt: EcxtId,
    result: Option<ExecSlotId>,
    f: impl FnOnce(&mut EvalSlots<'_, 'mcx>, Option<&mut SlotData<'mcx>>, Mcx<'mcx>) -> PgResult<R>,
) -> PgResult<R> {
    let mcx = estate.es_query_cxt;
    let (scan, inner, outer) = {
        let e = estate.ecxt(ecxt);
        (e.ecxt_scantuple, e.ecxt_innertuple, e.ecxt_outertuple)
    };
    let table: &mut [SlotData<'mcx>] = &mut estate.es_tupleTable;
    let ids = [scan, inner, outer, result];
    for (i, id) in ids.iter().enumerate() {
        if let Some(a) = id {
            assert!((a.0 as usize) < table.len(), "slot id out of range");
            for later in &ids[i + 1..] {
                assert!(Some(*a) != *later, "aliased slot ids in expression eval");
            }
        }
    }
    let base = table.as_mut_ptr();
    // SAFETY: indices bounds-checked and pairwise-distinct above, so the four
    // derived &mut are disjoint elements of one live slice.
    let get = |id: Option<ExecSlotId>| id.map(|i| unsafe { &mut *base.add(i.0 as usize) });
    let mut slots = EvalSlots {
        scan: get(scan),
        inner: get(inner),
        outer: get(outer),
    };
    f(&mut slots, get(result), mcx)
}
