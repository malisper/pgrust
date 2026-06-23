//! Seam declarations for parallel-executor methods on `exec_foreignscan` nodes.
//!
//! Installed by the owning node crate's `init_seams()` when it lands;
//! until then a call panics loudly.
#![allow(unused_doc_comments)]
#![allow(non_snake_case)]

use types_error::PgResult;
use nodes::{AsyncRequestData, ForeignScanState};

// The parallel-scan methods (ExecForeignScanEstimate / InitializeDSM /
// ReInitializeDSM / InitializeWorker) are NOT seamed here: `execParallel`
// dispatches them directly over the value-typed `PlanStateNode::ForeignScan`
// enum arm. Only the execAsync entry points below need an inward seam.

// Async-execution entry points (`nodeForeignscan.c`). The execAsync dispatch
// (re-homed onto the Append node) resolves the requestee `ForeignScanState`
// and calls these; the C reaches the same node via `(ForeignScanState *)
// areq->requestee`. The bodies run the node's `fdwroutine` async callback
// (FDW-extension-owned, so they bottom out at the uninstalled foreign FDW
// seam — sanctioned FLOOR).
seam_core::seam!(
    /// `ExecAsyncForeignScanRequest(areq)` — `node` is the requestee.
    pub fn exec_async_foreignscan_request<'mcx>(
        node: &mut ForeignScanState<'mcx>,
        areq: &mut AsyncRequestData,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `ExecAsyncForeignScanConfigureWait(areq)`.
    pub fn exec_async_foreignscan_configure_wait<'mcx>(
        node: &mut ForeignScanState<'mcx>,
        areq: &mut AsyncRequestData,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `ExecAsyncForeignScanNotify(areq)`.
    pub fn exec_async_foreignscan_notify<'mcx>(
        node: &mut ForeignScanState<'mcx>,
        areq: &mut AsyncRequestData,
    ) -> PgResult<()>
);
