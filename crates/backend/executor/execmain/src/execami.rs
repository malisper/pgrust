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
