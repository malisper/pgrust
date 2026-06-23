//! Seam declarations for parallel-executor methods on `exec_bitmapindexscan` nodes.
//!
//! Installed by the owning node crate's `init_seams()` when it lands;
//! until then a call panics loudly.
#![allow(unused_doc_comments)]
#![allow(non_snake_case)]

use execparallel::{ParallelContextHandle, ParallelWorkerContextHandle, PlanStateHandle};
use ::types_error::PgResult;

seam_core::seam!(pub fn exec_bitmapindexscan_estimate(node: PlanStateHandle, pcxt: ParallelContextHandle) -> PgResult<()>);
seam_core::seam!(pub fn exec_bitmapindexscan_initialize_dsm(node: PlanStateHandle, pcxt: ParallelContextHandle) -> PgResult<()>);
seam_core::seam!(pub fn exec_bitmapindexscan_initialize_worker(node: PlanStateHandle, pwcxt: ParallelWorkerContextHandle) -> PgResult<()>);
seam_core::seam!(pub fn exec_bitmapindexscan_retrieve_instrumentation(node: PlanStateHandle) -> PgResult<()>);

seam_core::seam!(
    /// `BitmapOr`/`BitmapAnd` special-case child run for a
    /// `BitmapIndexScanState` (nodeBitmapOr.c / nodeBitmapAnd.c):
    ///
    /// ```c
    /// ((BitmapIndexScanState *) subnode)->biss_result = result;
    /// subresult = (TIDBitmap *) MultiExecProcNode(subnode);
    /// if (subresult != result) elog(ERROR, "unrecognized result from subplan");
    /// ```
    ///
    /// The C hands the child the running `result` bitmap through `biss_result`
    /// and lets `MultiExecBitmapIndexScan` OR directly into it, avoiding an
    /// explicit `tbm_union`. The child is one of the `PlanStateNode` subplans;
    /// `result` is borrowed mutably for the duration so the child ORs in place
    /// (the C identity-equality check `subresult == result` is implicit). The
    /// nodeBitmapIndexscan owner installs this; it reaches the child's
    /// `biss_result` field, which is private to that unit.
    pub fn multi_exec_bitmap_index_child<'mcx>(
        subnode: &mut nodes::PlanStateNode<'mcx>,
        result: &mut tidbitmap::TIDBitmap,
        estate: &mut nodes::EStateData<'mcx>,
    ) -> PgResult<()>
);
