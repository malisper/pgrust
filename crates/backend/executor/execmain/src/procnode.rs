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

pub enum PlanStateNode<'mcx> {
    Result(ResultState<'mcx>),
    SeqScan(::nodeseqscan::SeqScanState<'mcx>),
    IndexScan(::nodeindexscan::IndexScanState<'mcx>),
    IndexOnlyScan(::nodeindexonlyscan::IndexOnlyScanState<'mcx>),
    Agg(PgBox<'mcx, AggPlanState<'mcx>>),
    BitmapHeapScan(PgBox<'mcx, BitmapHeapPlanState<'mcx>>),
    BitmapIndexScan(::nodebitmapindexscan::BitmapIndexScanState<'mcx>),
    BitmapAnd(PgBox<'mcx, BitmapCombineState<'mcx>>),
    BitmapOr(PgBox<'mcx, BitmapCombineState<'mcx>>),
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

// Init-time tree node touched by &mut per tuple; rule-9 budget covers the per-row carriers inside.
const _: () = assert!(core::mem::size_of::<PlanStateNode<'static>>() <= 1024);

impl<'mcx> PlanStateNode<'mcx> {
    #[inline]
    pub fn ps_expr_context(&self) -> Option<EcxtId> {
        match self {
            PlanStateNode::Result(rs) => rs.ps.ps_ExprContext,
            PlanStateNode::SeqScan(ss) => Some(ss.ss.ps_ExprContext),
            PlanStateNode::IndexScan(is) => Some(is.ss.ps_ExprContext),
            PlanStateNode::IndexOnlyScan(ios) => Some(ios.ss.ps_ExprContext),
            PlanStateNode::Agg(aps) => Some(aps.agg.ps_ExprContext),
            PlanStateNode::BitmapHeapScan(b) => Some(b.scan.ss.ps_ExprContext),
            PlanStateNode::BitmapIndexScan(_)
            | PlanStateNode::BitmapAnd(_)
            | PlanStateNode::BitmapOr(_) => None,
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
            PlanStateNode::Result(rs) => Ok(rs
                .ps
                .ps_ResultTupleDesc
                .clone()
                .expect("ResultState without a result type")),
            PlanStateNode::SeqScan(_)
            | PlanStateNode::IndexScan(_)
            | PlanStateNode::IndexOnlyScan(_)
            | PlanStateNode::BitmapHeapScan(_) => crate::exec_type_from_tl(&plan.targetlist),
            PlanStateNode::Agg(aps) => Ok(aps.agg.ps_ResultTupleDesc.clone()),
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
        NodeTag::T_Agg => {
            let mcx = estate.es_query_cxt;
            let agg_plan = node.as_agg().unwrap();
            let outer = exec_init_node(agg_plan.plan.lefttree, estate, eflags)?
                .unwrap_or_else(|| panic!("ExecInitAgg (nodeAgg.c): Agg without an outer plan"));
            let desc = crate::exec_type_from_tl(&agg_plan.plan.targetlist)?;
            let agg = ::nodeagg::exec_init_agg(agg_plan, estate, eflags, desc)?;
            PlanStateNode::Agg(::mcx::alloc_in(mcx, AggPlanState { agg, outer })?)
        }
        tag => unported_nodes!(tag, {
            T_ProjectSet => "nodeProjectSet.c",
            T_ModifyTable => "nodeModifyTable.c",
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
            T_Sort => "nodeSort.c",
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
            T_Limit => "nodeLimit.c",
        }),
    };
    if !node.as_plan().expect("plan-tree node").initPlan.is_nil() {
        panic!("ExecInitNode (execProcnode.c): initPlan lane (nodeSubplan.c) not ported");
    }
    Ok(Some(result))
}

/// `ExecProcNode`: one match over the closed node set, Result arm inlined.
#[inline]
pub fn exec_proc_node<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    match node {
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
    }
}

/// `ExecShutdownNode` (execProcnode.c): per-node arms are all no-ops for the
/// ported set (Gather/ForeignScan/CustomScan/Hash own real shutdowns).
pub fn exec_shutdown_node<'mcx>(node: &mut PlanStateNode<'mcx>) {
    match node {
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
        PlanStateNode::BitmapHeapScan(b) => exec_shutdown_node(&mut b.bitmapqual),
        PlanStateNode::BitmapAnd(bc) | PlanStateNode::BitmapOr(bc) => {
            for sub in bc.substates.iter_mut() {
                exec_shutdown_node(sub);
            }
        }
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
