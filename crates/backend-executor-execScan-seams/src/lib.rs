//! Seam declarations for the `backend-executor-execScan` unit
//! (`executor/execScan.c`), the generic scan driver.
//!
//! `ExecScan` wraps an access method (return the next candidate tuple) and a
//! recheck method (EvalPlanQual recheck) with qual evaluation, projection, and
//! EPQ handling — logic owned by execScan.c. The executor scan node passes its
//! own in-crate access/recheck functions (concrete `fn` pointers, the C
//! `(ExecScanAccessMtd) TableFuncNext` casts) and its node + estate; the driver
//! re-enters those functions per candidate tuple. The result is the slot id of
//! the produced (possibly projected) tuple, or `None` (the C `NULL`).
//!
//! The signatures are specialized to [`TableFuncScanState`] because that is the
//! caller; when execScan.c lands it installs a single generic implementation
//! and the per-node entry points marshal to it.

#![allow(non_snake_case)]

use types_error::PgResult;
use types_nodes::{EStateData, SlotId, TableFuncScanState};

/// `ExecScanAccessMtd` — the access method `ExecScan` re-enters to get the
/// next candidate tuple. Returns `true` when a tuple is in the node's scan
/// slot, `false` at end-of-scan (the C `TupleTableSlot *` / `NULL`). The node
/// and estate share the state tree's allocator lifetime.
pub type TableFuncScanAccessMtd =
    for<'mcx> fn(&mut TableFuncScanState<'mcx>, &mut EStateData<'mcx>) -> PgResult<bool>;

/// `ExecScanRecheckMtd` — the recheck method for EvalPlanQual.
pub type TableFuncScanRecheckMtd =
    for<'mcx> fn(&mut TableFuncScanState<'mcx>, &mut EStateData<'mcx>) -> PgResult<bool>;

seam_core::seam!(
    /// `ExecScan(&node->ss, accessMtd, recheckMtd)` (execScan.c): run the
    /// generic scan loop — fetch via `access`, qual-filter, project — for a
    /// table-func-scan node. Returns the result slot id, or `None` at end of
    /// scan. `Err` carries qual/projection `ereport(ERROR)`s and OOM.
    pub fn exec_scan<'mcx>(
        node: &mut TableFuncScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
        access: TableFuncScanAccessMtd,
        recheck: TableFuncScanRecheckMtd,
    ) -> PgResult<Option<SlotId>>
);

seam_core::seam!(
    /// `ExecAssignScanProjectionInfo(scanstate)` (execScan.c): set up the
    /// node's projection, comparing the scan tuple type to the result type and
    /// building a `ProjectionInfo` (or leaving `ps_ProjInfo` NULL for the
    /// physical-tlist no-op case). Fallible on OOM / build errors.
    pub fn exec_assign_scan_projection_info<'mcx>(
        scanstate: &mut types_nodes::execnodes::ScanStateData<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecScanReScan(&node->ss)` (execScan.c): reset the generic scan state
    /// (rescan EPQ, clear the result slot) at the start of a rescan.
    pub fn exec_scan_rescan<'mcx>(
        node: &mut TableFuncScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

// --- Index-only-scan-specialized entry points -----------------------------
// Same `execScan.c` driver, specialized to the index-only scan node (its own
// in-crate `IndexOnlyNext`/`IndexOnlyRecheck` access/recheck functions). When
// execScan.c lands it installs one generic implementation; each per-node entry
// point marshals to it.

use types_nodes::nodeindexonlyscan::IndexOnlyScanState;

/// `ExecScanAccessMtd`, specialized to an index-only scan node — returns
/// `true` when a tuple sits in the node's scan slot, `false` at end-of-scan.
pub type IndexOnlyScanAccessMtd =
    for<'mcx> fn(&mut IndexOnlyScanState<'mcx>, &mut EStateData<'mcx>) -> PgResult<bool>;

/// `ExecScanRecheckMtd`, specialized to an index-only scan node.
pub type IndexOnlyScanRecheckMtd =
    for<'mcx> fn(&mut IndexOnlyScanState<'mcx>, &mut EStateData<'mcx>) -> PgResult<bool>;

seam_core::seam!(
    /// `ExecScan(&node->ss, accessMtd, recheckMtd)` (execScan.c): run the
    /// generic scan loop — `ExecScanFetch` (interrupts + the EvalPlanQual
    /// replacement-tuple decision tree), qual-filter, project — for an
    /// index-only scan node. Returns `true` when a qualifying tuple is in the
    /// node's result/scan slot, `false` at end of scan. `Err` carries the
    /// qual/projection `ereport(ERROR)`s and OOM. The EPQ branching is owned by
    /// execScan.c; this node passes its `IndexOnlyNext`/`IndexOnlyRecheck`.
    pub fn exec_scan_indexonly<'mcx>(
        node: &mut IndexOnlyScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
        access: IndexOnlyScanAccessMtd,
        recheck: IndexOnlyScanRecheckMtd,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `ExecScanReScan(&node->ss)` (execScan.c), over the generic scan-state
    /// head: reset the common scan-node state for a rescan — clear the
    /// scan/result tuple slots and reset the EPQ `relsubs_done` flags for the
    /// node's scan relation. Fallible on the slot-clear `ereport(ERROR)` paths.
    pub fn exec_scan_rescan_ss<'mcx>(
        node: &mut types_nodes::execnodes::ScanStateData<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);
