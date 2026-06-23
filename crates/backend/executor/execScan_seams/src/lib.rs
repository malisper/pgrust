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

use ::types_error::PgResult;
use ::nodes::{EStateData, FunctionScanState, SlotId, TableFuncScanState};

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
        scanstate: &mut ::nodes::execnodes::ScanStateData<'mcx>,
        estate: &mut ::nodes::EStateData<'mcx>,
    ) -> ::types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecScanReScan(&node->ss)` (execScan.c): reset the generic scan state
    /// (rescan EPQ, clear the result slot) at the start of a rescan.
    pub fn exec_scan_rescan<'mcx>(
        node: &mut TableFuncScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

// --- Function-scan-specialized entry points --------------------------------
// Same `execScan.c` driver, specialized to the FunctionScan node (its own
// in-crate `FunctionNext`/`FunctionRecheck` access/recheck functions). Unlike
// the relation-scan nodes, `FunctionNext` stores into and returns the node's
// scan slot directly (the C `return scanslot`), so the access method already
// yields a `SlotId` (like the subquery scan), reported here as
// `Option<SlotId>` (`None` is the C empty / `NULL` slot).

/// `ExecScanAccessMtd`, specialized to a FunctionScan node — yields the
/// produced scan slot id (or `None` at end of scan).
pub type FunctionScanAccessMtd =
    for<'mcx> fn(&mut FunctionScanState<'mcx>, &mut EStateData<'mcx>) -> PgResult<Option<SlotId>>;

/// `ExecScanRecheckMtd`, specialized to a FunctionScan node.
pub type FunctionScanRecheckMtd =
    for<'mcx> fn(&mut FunctionScanState<'mcx>, &mut EStateData<'mcx>) -> PgResult<bool>;

seam_core::seam!(
    /// `ExecScan(&node->ss, FunctionNext, FunctionRecheck)` (execScan.c): run
    /// the generic scan loop for a FunctionScan node. Returns the result slot
    /// id, or `None` at end of scan. `Err` carries qual/projection
    /// `ereport(ERROR)`s and OOM.
    pub fn exec_scan_function<'mcx>(
        node: &mut FunctionScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
        access: FunctionScanAccessMtd,
        recheck: FunctionScanRecheckMtd,
    ) -> PgResult<Option<SlotId>>
);

seam_core::seam!(
    /// `ExecScanReScan(&node->ss)` (execScan.c) for a FunctionScan node.
    pub fn exec_scan_rescan_function<'mcx>(
        node: &mut FunctionScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

// --- Index-only-scan-specialized entry points --------------------------------
// Same `execScan.c` driver, specialized to the index-only scan node (its own
// in-crate `IndexOnlyNext`/`IndexOnlyRecheck` access/recheck functions). When
// execScan.c lands it installs one generic implementation; each per-node entry
// point marshals to it.

use ::nodes::nodeindexonlyscan::IndexOnlyScanState;

/// `ExecScanAccessMtd`, specialized to an index-only scan node — returns
/// `true` when a tuple sits in the node's scan slot, `false` at end-of-scan.
pub type IndexOnlyScanAccessMtd =
    for<'mcx> fn(&mut IndexOnlyScanState<'mcx>, &mut EStateData<'mcx>) -> PgResult<bool>;

/// `ExecScanRecheckMtd`, specialized to an index-only scan node.
pub type IndexOnlyScanRecheckMtd =
    for<'mcx> fn(&mut IndexOnlyScanState<'mcx>, &mut EStateData<'mcx>) -> PgResult<bool>;

/// `ExecScanAccessMtd`, specialized to a plain index scan node.
pub type IndexScanAccessMtd =
    for<'mcx> fn(&mut ::nodes::IndexScanState<'mcx>, &mut EStateData<'mcx>) -> PgResult<bool>;

/// `ExecScanRecheckMtd`, specialized to a plain index scan node.
pub type IndexScanRecheckMtd =
    for<'mcx> fn(&mut ::nodes::IndexScanState<'mcx>, &mut EStateData<'mcx>) -> PgResult<bool>;

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
    /// `ExecScan(&node->ss, accessMtd, recheckMtd)` (execScan.c): the same
    /// generic scan driver, specialized to a plain index scan node. This node
    /// passes its `IndexNext`/`IndexNextWithReorder` access method and
    /// `IndexRecheck`.
    pub fn exec_scan_index<'mcx>(
        node: &mut ::nodes::IndexScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
        access: IndexScanAccessMtd,
        recheck: IndexScanRecheckMtd,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `ExecScanReScan(&node->ss)` (execScan.c), over the generic scan-state
    /// head: reset the common scan-node state for a rescan — clear the
    /// scan/result tuple slots and reset the EPQ `relsubs_done` flags for the
    /// node's scan relation. Fallible on the slot-clear `ereport(ERROR)` paths.
    pub fn exec_scan_rescan_ss<'mcx>(
        node: &mut ::nodes::execnodes::ScanStateData<'mcx>,
        estate: &mut ::nodes::EStateData<'mcx>,
    ) -> ::types_error::PgResult<()>
);

// --- Subquery-scan-specialized entry point --------------------------------
// Same `execScan.c` driver, specialized to the subquery scan node (its own
// in-crate `SubqueryNext`/`SubqueryRecheck` access/recheck functions).
//
// Unlike the relation-scan nodes, `SubqueryNext` returns the *subplan's* own
// result slot directly (the C avoids `ExecCopySlot`; the node's own
// `ss_ScanTupleSlot` is used only for EvalPlanQual rechecks), so the access
// method yields the produced `SlotId` rather than a `bool`-into-scan-slot.

use ::nodes::SubqueryScanState;

/// `ExecScanAccessMtd`, specialized to a subquery scan node — returns the
/// produced tuple's `SlotId` (the subplan's result slot), `None` at
/// end-of-scan (the C `TupleTableSlot *` / `NULL`).
pub type SubqueryScanAccessMtd =
    for<'mcx> fn(&mut SubqueryScanState<'mcx>, &mut EStateData<'mcx>) -> PgResult<Option<SlotId>>;

/// `ExecScanRecheckMtd`, specialized to a subquery scan node.
pub type SubqueryScanRecheckMtd =
    for<'mcx> fn(&mut SubqueryScanState<'mcx>, &mut EStateData<'mcx>) -> PgResult<bool>;

seam_core::seam!(
    /// `ExecScan(&node->ss, accessMtd, recheckMtd)` (execScan.c): run the
    /// generic scan loop — fetch via `access`, qual-filter, project, with EPQ
    /// handling — for a subquery scan node. Returns the result slot id, or
    /// `None` at end of scan. `Err` carries qual/projection `ereport(ERROR)`s
    /// and OOM. The node passes its own `SubqueryNext`/`SubqueryRecheck`.
    pub fn exec_scan_subquery<'mcx>(
        node: &mut SubqueryScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
        access: SubqueryScanAccessMtd,
        recheck: SubqueryScanRecheckMtd,
    ) -> PgResult<Option<SlotId>>
);

// --- CteScan-specialized entry points --------------------------------------
// Same `execScan.c` driver, specialized to the CTE scan node (its own in-crate
// `CteScanNext`/`CteScanRecheck` access/recheck functions). When execScan.c
// lands it installs one generic implementation; each per-node entry point
// marshals to it.

use ::nodes::nodectescan::CteScanState;

/// `ExecScanAccessMtd`, specialized to a CTE scan node — returns `true` when a
/// tuple sits in the node's scan slot, `false` at end-of-scan.
pub type CteScanAccessMtd =
    for<'mcx> fn(&mut CteScanState<'mcx>, &mut EStateData<'mcx>) -> PgResult<bool>;

/// `ExecScanRecheckMtd`, specialized to a CTE scan node.
pub type CteScanRecheckMtd =
    for<'mcx> fn(&mut CteScanState<'mcx>, &mut EStateData<'mcx>) -> PgResult<bool>;

seam_core::seam!(
    /// `ExecScan(&node->ss, accessMtd, recheckMtd)` (execScan.c): run the
    /// generic scan loop — fetch via `access`, qual-filter, project — for a CTE
    /// scan node. Returns the result slot id, or `None` at end of scan. `Err`
    /// carries qual/projection `ereport(ERROR)`s and OOM. This node passes its
    /// `CteScanNext`/`CteScanRecheck`.
    pub fn exec_scan_cte<'mcx>(
        node: &mut CteScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
        access: CteScanAccessMtd,
        recheck: CteScanRecheckMtd,
    ) -> PgResult<Option<SlotId>>
);

seam_core::seam!(
    /// `ExecScanReScan(&node->ss)` (execScan.c): reset the generic scan state
    /// (rescan EPQ, clear the result slot) at the start of a CTE-scan rescan.
    pub fn exec_scan_rescan_cte<'mcx>(
        node: &mut CteScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecAssignScanProjectionInfo(&node->ss)` (execScan.c): set up the CTE
    /// scan node's projection, comparing the scan tuple type to the result type.
    pub fn exec_assign_scan_projection_info_cte<'mcx>(
        node: &mut CteScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);
// --- NamedTuplestoreScan-specialized entry point ---------------------------
// Same `execScan.c` driver, specialized to the named-tuplestore-scan node (its
// own in-crate `NamedTuplestoreScanNext`/`NamedTuplestoreScanRecheck`). When
// execScan.c lands it installs one generic implementation; this per-node entry
// point marshals to it.

use ::nodes::nodenamedtuplestorescan::NamedTuplestoreScanState;

/// `ExecScanAccessMtd`, specialized to a named-tuplestore-scan node — returns
/// `true` when a tuple sits in the node's scan slot, `false` at end-of-scan.
pub type NamedTuplestoreScanAccessMtd =
    for<'mcx> fn(&mut NamedTuplestoreScanState<'mcx>, &mut EStateData<'mcx>) -> PgResult<bool>;

/// `ExecScanRecheckMtd`, specialized to a named-tuplestore-scan node.
pub type NamedTuplestoreScanRecheckMtd =
    for<'mcx> fn(&mut NamedTuplestoreScanState<'mcx>, &mut EStateData<'mcx>) -> PgResult<bool>;

seam_core::seam!(
    /// `ExecScan(&node->ss, accessMtd, recheckMtd)` (execScan.c): run the
    /// generic scan loop — `ExecScanFetch` (interrupts + the EvalPlanQual
    /// replacement-tuple decision tree), qual-filter, project — for a
    /// named-tuplestore-scan node. Returns the result slot id of the produced
    /// (possibly projected) tuple, or `None` at end of scan. `Err` carries the
    /// qual/projection `ereport(ERROR)`s and OOM. The EPQ branching is owned by
    /// execScan.c; this node passes its `NamedTuplestoreScanNext` /
    /// `NamedTuplestoreScanRecheck`.
    pub fn exec_scan_namedtuplestore<'mcx>(
        node: &mut NamedTuplestoreScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
        access: NamedTuplestoreScanAccessMtd,
        recheck: NamedTuplestoreScanRecheckMtd,
    ) -> PgResult<Option<SlotId>>
);

// --- WorkTableScan-specialized entry point ---------------------------------
// Same `execScan.c` driver, specialized to the work-table-scan node (its own
// in-crate `WorkTableScanNext`/`WorkTableScanRecheck`). When execScan.c lands it
// installs one generic implementation; this per-node entry point marshals to it.

use ::nodes::nodeworktablescan::WorkTableScanStateData;

/// `ExecScanAccessMtd`, specialized to a work-table-scan node — returns `true`
/// when a tuple sits in the node's scan slot, `false` at end-of-scan.
pub type WorkTableScanAccessMtd =
    for<'mcx> fn(&mut WorkTableScanStateData<'mcx>, &mut EStateData<'mcx>) -> PgResult<bool>;

/// `ExecScanRecheckMtd`, specialized to a work-table-scan node.
pub type WorkTableScanRecheckMtd =
    for<'mcx> fn(&mut WorkTableScanStateData<'mcx>, &mut EStateData<'mcx>) -> PgResult<bool>;

seam_core::seam!(
    /// `ExecScan(&node->ss, accessMtd, recheckMtd)` (execScan.c): run the
    /// generic scan loop — `ExecScanFetch` (interrupts + the EvalPlanQual
    /// replacement-tuple decision tree), qual-filter, project — for a
    /// work-table-scan node. Returns the result slot id of the produced (possibly
    /// projected) tuple, or `None` at end of scan. `Err` carries the
    /// qual/projection `ereport(ERROR)`s and OOM. The EPQ branching is owned by
    /// execScan.c; this node passes its `WorkTableScanNext` / `WorkTableScanRecheck`.
    pub fn exec_scan_worktable<'mcx>(
        node: &mut WorkTableScanStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
        access: WorkTableScanAccessMtd,
        recheck: WorkTableScanRecheckMtd,
    ) -> PgResult<Option<SlotId>>
);

seam_core::seam!(
    /// `ExecScanReScan(&node->ss)` (execScan.c): reset the generic scan state
    /// (rescan EPQ, clear the result slot) at the start of a work-table-scan
    /// rescan.
    pub fn exec_scan_rescan_worktable<'mcx>(
        node: &mut WorkTableScanStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecAssignScanProjectionInfo(&node->ss)` (execScan.c): set up the
    /// work-table-scan node's projection, comparing the scan tuple type to the
    /// result type.
    pub fn exec_assign_scan_projection_info_worktable<'mcx>(
        node: &mut WorkTableScanStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);
