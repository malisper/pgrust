//! execAsync.c, requestee side: dispatch an async request / wait
//! configuration / notification to the child node. The only async-capable
//! requestee is ForeignScan (as C). The requestor side (ExecAsyncResponse ->
//! ExecAsyncAppendResponse) runs inside nodeappend after each dispatch.
//! Divergence: the chgParam-driven ExecReScan arm is absent — this executor
//! does not track per-node chgParam (nodeforeignscan rescan precedent).

use ::executils::{AsyncRequest, AsyncWaitCtx, EStateData};
use ::types_error::PgResult;

use crate::procnode::PlanStateNode;

/// `ExecAsyncRequest` (requestee dispatch + own instrumentation).
pub(crate) fn exec_async_request<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
    areq: &mut AsyncRequest,
) -> PgResult<()> {
    match node {
        PlanStateNode::ForeignScan(fs) => {
            ::nodeforeignscan::exec_async_foreign_scan_request(fs, estate, areq)
        }
        PlanStateNode::Instrumented(w) => {
            let idx = w.instr_idx as usize;
            ::instrument::instr_start_node(&mut estate.es_instrumentation[idx]);
            let r = exec_async_request(&mut w.inner, estate, areq);
            let n = if areq.result.is_some() { 1.0 } else { 0.0 };
            ::instrument::instr_stop_node(&mut estate.es_instrumentation[idx], n);
            r
        }
        _ => panic!("ExecAsyncRequest (execAsync.c): unrecognized requestee node type"),
    }
}

/// `ExecAsyncConfigureWait`: the node registers (at most) one
/// WL_SOCKET_READABLE event carrying its request_index as user_data.
pub(crate) fn exec_async_configure_wait<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
    areq: &mut AsyncRequest,
    wait: &AsyncWaitCtx,
) -> PgResult<()> {
    match node {
        PlanStateNode::ForeignScan(fs) => {
            ::nodeforeignscan::exec_async_foreign_scan_configure_wait(fs, estate, areq, wait)
        }
        PlanStateNode::Instrumented(w) => {
            let idx = w.instr_idx as usize;
            ::instrument::instr_start_node(&mut estate.es_instrumentation[idx]);
            let r = exec_async_configure_wait(&mut w.inner, estate, areq, wait);
            ::instrument::instr_stop_node(&mut estate.es_instrumentation[idx], 0.0);
            r
        }
        _ => panic!("ExecAsyncConfigureWait (execAsync.c): unrecognized requestee node type"),
    }
}

/// `ExecAsyncNotify` (requestee dispatch + own instrumentation).
pub(crate) fn exec_async_notify<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
    areq: &mut AsyncRequest,
) -> PgResult<()> {
    match node {
        PlanStateNode::ForeignScan(fs) => {
            ::nodeforeignscan::exec_async_foreign_scan_notify(fs, estate, areq)
        }
        PlanStateNode::Instrumented(w) => {
            let idx = w.instr_idx as usize;
            ::instrument::instr_start_node(&mut estate.es_instrumentation[idx]);
            let r = exec_async_notify(&mut w.inner, estate, areq);
            let n = if areq.result.is_some() { 1.0 } else { 0.0 };
            ::instrument::instr_stop_node(&mut estate.es_instrumentation[idx], n);
            r
        }
        _ => panic!("ExecAsyncNotify (execAsync.c): unrecognized requestee node type"),
    }
}
