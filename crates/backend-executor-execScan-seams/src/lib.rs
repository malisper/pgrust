//! Seam declarations for the `backend-executor-execScan` unit
//! (`executor/execScan.c`).
//!
//! `ExecScan` itself (the non-inline forwarder) and `ExecScanExtended` /
//! `ExecScanFetch` (the `execScan.h` `static inline` driver) are inlined into
//! each scan node's translation unit, so a scan node reproduces that driver
//! locally; only the genuinely external `execScan.c` entry points that a scan
//! node calls — `ExecAssignScanProjectionInfo` and `ExecScanReScan` — are
//! declared here.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `ExecAssignScanProjectionInfo(node)` (execScan.c): set up projection
    /// info for a scan node, using its scan-tuple-slot descriptor and the
    /// plan's `scanrelid`. Allocates the compiled projection; fallible on OOM
    /// and on `ereport(ERROR)` for unsupported expression shapes.
    pub fn exec_assign_scan_projection_info<'mcx>(
        scanstate: &mut types_nodes::execnodes::ScanStateData<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        scanrelid: types_core::primitive::Index,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecScanReScan(node)` (execScan.c): reset a scan node's common state
    /// for a rescan — rescan the projection result slot and reset the EPQ
    /// tuple/done bookkeeping for this node's rel(s). Fallible on
    /// `ereport(ERROR)`.
    pub fn exec_scan_rescan<'mcx>(
        scanstate: &mut types_nodes::execnodes::ScanStateData<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);
