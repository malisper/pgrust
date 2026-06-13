//! Seam declarations for the `backend-executor-execScan` unit
//! (`executor/execScan.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. (`ExecScan` / `ExecScanExtended` / `ExecScanFetch`
//! themselves are compiled into each scan-node object in C and are reproduced
//! as private functions in the calling node crate; only the separately-linked
//! `ExecScanReScan` is reached through this seam.)

#![allow(non_snake_case)]

seam_core::seam!(
    /// `ExecScanReScan(node)` (execScan.c): reset the common scan-node state
    /// for a rescan — clear the scan/result tuple slots and reset the EPQ
    /// `relsubs_done` flags for the node's scan relation. The owned model lends
    /// the scan-state head and the estate. Fallible on the slot-clear
    /// `ereport(ERROR)` paths.
    pub fn exec_scan_rescan<'mcx>(
        node: &mut types_nodes::execnodes::ScanStateData<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);
