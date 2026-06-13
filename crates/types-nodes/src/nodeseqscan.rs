//! Sequential-scan plan-node and state vocabulary (nodes/plannodes.h /
//! executor/execnodes.h).
//!
//! Trimmed to the fields the `nodeSeqscan.c` port consumes. The EvalPlanQual
//! recheck state (`EPQState`) is the canonical owned struct in
//! [`crate::execnodes`]; this module does not redefine it.

use mcx::Mcx;
use types_error::PgResult;

use crate::execnodes::ScanStateData;
use crate::nodeindexscan::Scan;

/// `SeqScan` plan node (plannodes.h):
///
/// ```c
/// typedef struct SeqScan { Scan scan; } SeqScan;
/// ```
#[derive(Debug, Default)]
pub struct SeqScan<'mcx> {
    /// `Scan scan` — the scan base (which embeds `Plan plan` first).
    pub scan: Scan<'mcx>,
}

impl SeqScan<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<SeqScan<'b>> {
        Ok(SeqScan {
            scan: self.scan.clone_in(mcx)?,
        })
    }
}

/// `SeqScanState` (execnodes.h):
///
/// ```c
/// typedef struct SeqScanState {
///     ScanState   ss;             /* its first field is NodeTag */
///     Size        pscan_len;      /* size of parallel heap scan descriptor */
/// } SeqScanState;
/// ```
///
/// The embedded [`ScanStateData`] carries `ss_currentRelation` and
/// `ss_currentScanDesc` (the active table scan descriptor, opaque to the
/// executor).
#[derive(Debug, Default)]
pub struct SeqScanState<'mcx> {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData<'mcx>,
    /// `Size pscan_len` — size of the parallel heap scan descriptor.
    pub pscan_len: usize,
}

impl<'mcx> SeqScanState<'mcx> {
    /// `&node->ss.ps` — the embedded `PlanState` head.
    #[inline]
    pub fn ps(&self) -> &crate::execnodes::PlanStateData<'mcx> {
        &self.ss.ps
    }

    /// `&mut node->ss.ps`.
    #[inline]
    pub fn ps_mut(&mut self) -> &mut crate::execnodes::PlanStateData<'mcx> {
        &mut self.ss.ps
    }
}
