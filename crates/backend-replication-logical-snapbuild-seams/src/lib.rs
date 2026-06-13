//! Seam declarations for the `backend-replication-logical-snapbuild` unit
//! (`replication/logical/snapbuild.c`), as consumed by logical decoding.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

use types_core::primitive::{TransactionId, XLogRecPtr};
use types_logical::{ReorderBufferHandle, SnapBuildHandle};

seam_core::seam!(
    /// `SnapBuildResetExportedSnapshotState()` — reset snapshot-export state
    /// on abort.
    pub fn snap_build_reset_exported_snapshot_state()
);

seam_core::seam!(
    /// `AllocateSnapshotBuilder(reorder, xmin_horizon, start_lsn,
    /// need_full_snapshot, in_create, two_phase_at)`.
    pub fn AllocateSnapshotBuilder(reorder: ReorderBufferHandle, xmin_horizon: TransactionId, start_lsn: XLogRecPtr, need_full_snapshot: bool, in_create: bool, two_phase_at: XLogRecPtr) -> SnapBuildHandle
);
seam_core::seam!(
    /// `FreeSnapshotBuilder(builder)`.
    pub fn FreeSnapshotBuilder(builder: SnapBuildHandle)
);
seam_core::seam!(
    /// `SnapBuildCurrentState(builder)` — the `SnapBuildState` (i32).
    pub fn SnapBuildCurrentState(builder: SnapBuildHandle) -> i32
);
seam_core::seam!(
    /// `SnapBuildSetTwoPhaseAt(builder, lsn)`.
    pub fn SnapBuildSetTwoPhaseAt(builder: SnapBuildHandle, lsn: XLogRecPtr)
);
