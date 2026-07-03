use ::executils::EStateData;
use ::types_error::PgResult;

use crate::noderesult::ResultState;
use crate::procnode::PlanStateNode;

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
        PlanStateNode::Result(rs) => exec_re_scan_result(rs, estate),
        PlanStateNode::SeqScan(ss) => ::nodeseqscan::exec_rescan_seq_scan(ss, estate),
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
        // ExecReScanSort: child rescanned only when the sort must be redone
        // (chgParam NULL until the Param lanes land).
        PlanStateNode::Sort(s) => {
            if ::nodesort::exec_rescan_sort(&mut s.state, estate) {
                exec_re_scan(&mut s.outer, estate)?;
            }
            Ok(())
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
