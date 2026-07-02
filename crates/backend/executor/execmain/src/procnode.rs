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
}

// Init-time tree node touched by &mut per tuple; rule-9 budget covers the per-row carriers inside.
const _: () = assert!(core::mem::size_of::<PlanStateNode<'static>>() <= 1024);

impl<'mcx> PlanStateNode<'mcx> {
    #[inline]
    pub fn ps_expr_context(&self) -> Option<EcxtId> {
        match self {
            PlanStateNode::Result(rs) => rs.ps.ps_ExprContext,
            PlanStateNode::SeqScan(ss) => Some(ss.ss.ps_ExprContext),
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
            PlanStateNode::SeqScan(_) => crate::exec_type_from_tl(&plan.targetlist),
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
        tag => unported_nodes!(tag, {
            T_ProjectSet => "nodeProjectSet.c",
            T_ModifyTable => "nodeModifyTable.c",
            T_Append => "nodeAppend.c",
            T_MergeAppend => "nodeMergeAppend.c",
            T_RecursiveUnion => "nodeRecursiveunion.c",
            T_BitmapAnd => "nodeBitmapAnd.c",
            T_BitmapOr => "nodeBitmapOr.c",
            T_SampleScan => "nodeSamplescan.c",
            T_IndexScan => "nodeIndexscan.c",
            T_IndexOnlyScan => "nodeIndexonlyscan.c",
            T_BitmapIndexScan => "nodeBitmapIndexscan.c",
            T_BitmapHeapScan => "nodeBitmapHeapscan.c",
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
            T_Agg => "nodeAgg.c",
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
        PlanStateNode::SeqScan(_) => {}
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
