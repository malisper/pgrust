//! Work-table-scan plan-node and state vocabulary (nodes/plannodes.h /
//! executor/execnodes.h), plus the ancestor `RecursiveUnion` executor state the
//! work-table scan reaches into.
//!
//! Trimmed to the fields the `nodeWorktablescan.c` port consumes. The
//! `RecursiveUnionStateData` mirror is included here because a
//! [`WorkTableScanStateData`] holds a `RecursiveUnionState *rustate` back-link;
//! it is the real owned struct that `nodeRecursiveunion.c` will populate when it
//! lands (the work-table scan only ever reads it through seams into that
//! unported owner).

use alloc::boxed::Box;

use crate::execnodes::ScanStateData;
use crate::nodeindexscan::Scan;
pub use crate::noderecursiveunion::RecursiveUnionStateData;

/// `WorkTableScan` plan node (plannodes.h):
///
/// ```c
/// typedef struct WorkTableScan {
///     Scan        scan;
///     int         wtParam;    /* ID of Param representing work table */
/// } WorkTableScan;
/// ```
#[derive(Debug, Default)]
pub struct WorkTableScan<'mcx> {
    /// `Scan scan` — the abstract scan-plan base (embeds `Plan plan`).
    pub scan: Scan<'mcx>,
    /// `int wtParam` — ID of the `Param` representing the work table, indexing
    /// `EState.es_param_exec_vals`.
    pub wtParam: i32,
}

impl WorkTableScan<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying
    /// allocates.
    pub fn clone_in<'b>(
        &self,
        mcx: mcx::Mcx<'b>,
    ) -> types_error::PgResult<WorkTableScan<'b>> {
        Ok(WorkTableScan {
            scan: self.scan.clone_in(mcx)?,
            wtParam: self.wtParam,
        })
    }
}

/// `WorkTableScanState` (execnodes.h):
///
/// ```c
/// typedef struct WorkTableScanState {
///     ScanState   ss;             /* its first field is NodeTag */
///     RecursiveUnionState *rustate;
/// } WorkTableScanState;
/// ```
#[derive(Debug, Default)]
pub struct WorkTableScanStateData<'mcx> {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData<'mcx>,
    /// `RecursiveUnionState *rustate` — the ancestor `RecursiveUnion`'s executor
    /// state, owning the work-table tuplestore. `None` (C `NULL`) until the
    /// first `ExecWorkTableScan` call resolves it from the work-table `Param`
    /// slot.
    pub rustate: Option<Box<RecursiveUnionStateData<'mcx>>>,
}
