//! Seam declarations for the `backend-replication-logical-snapbuild` unit
//! (`replication/logical/snapbuild.c`), as consumed by logical decoding.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

extern crate alloc;
use alloc::vec::Vec;

use ::types_core::primitive::{TransactionId, XLogRecPtr};
use ::types_logical::{ReorderBufferHandle, SnapBuildHandle};

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

// ---------------------------------------------------------------------------
// decode.c entry points (change processing / snapshot generation).
//
// The owning unit implements every one of these (snapbuild.c is landed); they
// are exposed here so `decode.c` can dispatch into the historic-snapshot
// builder over the `SnapBuildHandle` the decoding context carries.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `SnapBuildProcessChange(builder, xid, lsn)` — whether a change at `lsn`
    /// in `xid` should be queued in the reorder buffer (we have a consistent
    /// enough snapshot). Returns the C `bool`.
    pub fn SnapBuildProcessChange(builder: SnapBuildHandle, xid: TransactionId, lsn: XLogRecPtr) -> bool
);
seam_core::seam!(
    /// `SnapBuildProcessNewCid(builder, xid, lsn, xlrec)` — process an
    /// `XLOG_HEAP2_NEW_CID` record, recording the (cmin, cmax) for the tuple so
    /// historic MVCC can later resolve it.
    pub fn SnapBuildProcessNewCid(
        builder: SnapBuildHandle,
        xid: TransactionId,
        lsn: XLogRecPtr,
        xlrec: xlog_records::heapam_xlog::xl_heap_new_cid,
    ) -> types_error::PgResult<()>
);
seam_core::seam!(
    /// `SnapBuildCommitTxn(builder, lsn, xid, nsubxacts, subxacts, xinfo)` —
    /// handle a transaction commit, building/distributing snapshots as needed.
    pub fn SnapBuildCommitTxn(
        builder: SnapBuildHandle,
        lsn: XLogRecPtr,
        xid: TransactionId,
        subxacts: Vec<TransactionId>,
        xinfo: u32,
    )
);
seam_core::seam!(
    /// `SnapBuildProcessRunningXacts(builder, lsn, running)` — process an
    /// `XLOG_RUNNING_XACTS` record (find a consistent point / serialize).
    pub fn SnapBuildProcessRunningXacts(
        builder: SnapBuildHandle,
        lsn: XLogRecPtr,
        running: xlog_records::standbydefs::xl_running_xacts,
        running_xids: Vec<TransactionId>,
    ) -> types_error::PgResult<()>
);
seam_core::seam!(
    /// `SnapBuildGetOrBuildSnapshot(builder)` — the current historic catalog
    /// snapshot, building it on first use.
    pub fn SnapBuildGetOrBuildSnapshot(builder: SnapBuildHandle) -> snapshot::SnapshotData
);
seam_core::seam!(
    /// `SnapBuildXactNeedsSkip(builder, ptr)` — whether decoding should skip a
    /// transaction whose records all precede `start_decoding_at`.
    pub fn SnapBuildXactNeedsSkip(builder: SnapBuildHandle, ptr: XLogRecPtr) -> bool
);
seam_core::seam!(
    /// `SnapBuildGetTwoPhaseAt(builder)` — `builder->two_phase_at`.
    pub fn SnapBuildGetTwoPhaseAt(builder: SnapBuildHandle) -> XLogRecPtr
);
seam_core::seam!(
    /// `SnapBuildSerializationPoint(builder, lsn)` — restore (pre-consistent) or
    /// serialize (consistent) the snapshot at a safe point.
    pub fn SnapBuildSerializationPoint(builder: SnapBuildHandle, lsn: XLogRecPtr) -> types_error::PgResult<()>
);
