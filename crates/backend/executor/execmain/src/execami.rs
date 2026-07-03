use ::executils::EStateData;
use ::types_error::PgResult;
use ::types_nodes::node_tree::Node;
use ::types_nodes::NodeTag;

use crate::noderesult::ResultState;
use crate::procnode::PlanStateNode;

/// `ExecSupportsBackwardScan` (execAmi.c). Unlanded node types take C's
/// default-false arms; the true-returning unlanded ones (Append, Material,
/// TidScan…) cannot appear in a plan today, so false is never wrongly returned.
pub fn exec_supports_backward_scan(node: Option<Node<'_>>) -> bool {
    let Some(node) = node else { return false };
    let plan = node.as_plan().expect("plan-tree node has a Plan prefix");
    if plan.parallel_aware {
        return false;
    }
    match node.node_tag() {
        NodeTag::T_Result => match plan.lefttree {
            Some(outer) => exec_supports_backward_scan(Some(outer)),
            None => false,
        },
        // amcanbackward: the only live index AM is btree (plancat.c port
        // loud-panics on any other relam before a plan can carry it).
        NodeTag::T_IndexScan | NodeTag::T_IndexOnlyScan => true,
        NodeTag::T_SeqScan | NodeTag::T_Sort => true,
        NodeTag::T_Limit => exec_supports_backward_scan(plan.lefttree),
        _ => false,
    }
}

/// `ExecReScan` (execAmi.c). The chgParam/initPlan/subPlan propagation block
/// is dead until the Param lanes land (their construction panics loudly).
pub fn exec_re_scan<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if let Some(id) = node.ps_expr_context() {
        estate.ecxt_mut(id).rescan();
    }
    match node {
        // C ExecReScan's InstrEndLoop: close the finished cycle, then the
        // recursion runs inner's ecxt reset + node rescan.
        PlanStateNode::Instrumented(w) => {
            ::instrument::instr_end_loop(
                &mut estate.es_instrumentation[w.instr_idx as usize],
            );
            exec_re_scan(&mut w.inner, estate)
        }
        PlanStateNode::Result(rs) => exec_re_scan_result(rs, estate),
        PlanStateNode::SeqScan(ss) => ::nodeseqscan::exec_rescan_seq_scan(ss, estate),
        PlanStateNode::FunctionScan(fs) => {
            ::nodefunctionscan::exec_rescan_function_scan(fs, estate)
        }
        PlanStateNode::ValuesScan(vs) => ::nodevaluesscan::exec_rescan_values_scan(vs, estate),
        PlanStateNode::CteScan(cs) => ::nodectescan::exec_rescan_cte_scan(cs, estate),
        PlanStateNode::IndexScan(is) => ::nodeindexscan::exec_rescan_index_scan(is, estate),
        PlanStateNode::IndexOnlyScan(ios) => {
            ::nodeindexonlyscan::exec_rescan_index_only_scan(ios, estate)
        }
        // ExecReScanAgg: outer child rescanned when chgParam is NULL (always,
        // until the Param lanes land).
        PlanStateNode::Agg(aps) => {
            ::nodeagg::exec_rescan_agg(&mut aps.agg, estate);
            exec_re_scan(&mut aps.outer, estate)
        }
        // ExecReScanWindowAgg: outer child rescanned when chgParam is NULL
        // (always, until the Param lanes land).
        PlanStateNode::WindowAgg(w) => {
            ::nodewindowagg::exec_rescan_window_agg(&mut w.state, estate);
            exec_re_scan(&mut w.outer, estate)
        }
        // ExecReScanSort: child rescanned only when the sort must be redone
        // (chgParam NULL until the Param lanes land).
        PlanStateNode::Sort(s) => {
            if ::nodesort::exec_rescan_sort(&mut s.state, estate) {
                exec_re_scan(&mut s.outer, estate)?;
            }
            Ok(())
        }
        // ExecReScanUnique: outer child rescanned when chgParam is NULL
        // (always, until the Param lanes land).
        PlanStateNode::Unique(u) => {
            ::nodeunique::exec_rescan_unique(&mut u.state, estate);
            exec_re_scan(&mut u.outer, estate)
        }
        PlanStateNode::Limit(l) => {
            let crate::procnode::LimitNode { state, outer } = l;
            ::nodelimit::exec_rescan_limit(state, &mut **outer, estate)?;
            exec_re_scan(outer, estate)
        }
        // ExecReScanBitmapHeapScan: bitmapqual rescanned when chgParam is
        // NULL (always, until the Param lanes land).
        PlanStateNode::BitmapHeapScan(b) => {
            let b = &mut **b;
            ::nodebitmapheapscan::exec_rescan_bitmap_heap_scan(&mut b.scan, estate)?;
            exec_re_scan(&mut b.bitmapqual, estate)
        }
        PlanStateNode::BitmapIndexScan(biss) => {
            ::nodebitmapindexscan::exec_rescan_bitmap_index_scan(biss)
        }
        PlanStateNode::BitmapAnd(bc) | PlanStateNode::BitmapOr(bc) => {
            for sub in bc.substates.iter_mut() {
                exec_re_scan(sub, estate)?;
            }
            Ok(())
        }
        // ExecReScanNestLoop: outer rescanned when its chgParam is NULL
        // (always, until the Param lanes land); the inner is NOT rescanned
        // here -- ExecNestLoop rescans it per outer tuple.
        PlanStateNode::NestLoop(nl) => {
            exec_re_scan(&mut nl.outer, estate)?;
            ::nodenestloop::exec_rescan_nest_loop(&mut nl.state);
            Ok(())
        }
        // ExecReScanHashJoin: single-batch reuse keeps the built table and
        // jumps to HJ_NEED_NEW_OUTER; the outer child is rescanned (chgParam
        // NULL until the Param lanes land). The Hash sub-node's child is NOT
        // rescanned here (the table is reused, not rebuilt).
        PlanStateNode::HashJoin(hj) => {
            let hj = &mut **hj;
            exec_re_scan(&mut hj.outer, estate)?;
            ::nodehashjoin::exec_rescan_hash_join(&mut hj.state, &hj.hash.state);
            Ok(())
        }
        // ExecReScanMergeJoin: both children rescanned (chgParam NULL until the
        // Param lanes land); node-local half clears the marked slot + state.
        PlanStateNode::MergeJoin(mj) => {
            let mj = &mut **mj;
            exec_re_scan(&mut mj.outer, estate)?;
            exec_re_scan(&mut mj.inner, estate)?;
            ::nodemergejoin::exec_rescan_merge_join(&mut mj.state, estate);
            Ok(())
        }
        // execAmi.c has no ModifyTable rescan arm ("node type not supported").
        PlanStateNode::ModifyTable(_) => {
            panic!("ExecReScan (execAmi.c): node type 232 does not support ExecReScan")
        }
    }
}

/// `ExecMarkPos` (execAmi.c): remember `node`'s current scan position. Only the
/// mark-capable ported nodes have arms; the planner routes an unmarkable merge
/// inner through a Sort/Material, so anything else is a loud panic.
pub fn exec_mark_pos<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    match node {
        PlanStateNode::Instrumented(w) => exec_mark_pos(&mut w.inner, estate),
        PlanStateNode::IndexScan(is) => ::nodeindexscan::exec_index_mark_pos(is),
        PlanStateNode::IndexOnlyScan(ios) => ::nodeindexonlyscan::exec_index_only_mark_pos(ios),
        PlanStateNode::Sort(s) => {
            ::nodesort::exec_sort_mark_pos(&mut s.state);
            Ok(())
        }
        _ => panic!("ExecMarkPos (execAmi.c): node type does not support mark/restore"),
    }
}

/// `ExecRestrPos` (execAmi.c): restore `node` to its last marked position.
pub fn exec_restr_pos<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    match node {
        PlanStateNode::Instrumented(w) => exec_restr_pos(&mut w.inner, estate),
        PlanStateNode::IndexScan(is) => ::nodeindexscan::exec_index_restr_pos(is),
        PlanStateNode::IndexOnlyScan(ios) => ::nodeindexonlyscan::exec_index_only_restr_pos(ios),
        PlanStateNode::Sort(s) => {
            ::nodesort::exec_sort_restr_pos(&mut s.state);
            Ok(())
        }
        _ => panic!("ExecRestrPos (execAmi.c): node type does not support mark/restore"),
    }
}

/// `ExecReScanResult` (nodeResult.c).
pub fn exec_re_scan_result<'mcx>(
    node: &mut ResultState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    node.rs_done = false;
    node.rs_checkqual = node.resconstantqual.is_some();
    match node.outer.as_deref_mut() {
        Some(outer) => exec_re_scan(outer, estate),
        None => Ok(()),
    }
}
