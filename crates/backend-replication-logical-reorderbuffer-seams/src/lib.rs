//! Seam declarations for the `backend-replication-logical-reorderbuffer` unit
//! (`replication/logical/reorderbuffer.c`), as consumed by logical decoding.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

extern crate alloc;
use alloc::vec::Vec;

use types_core::primitive::{RepOriginId, TimestampTz, TransactionId, XLogRecPtr};
use types_logical::{ReorderBufferHandle, ReorderBufferStats, TxnHandle};
use types_storage::sinval::SharedInvalidationMessage;
use types_storage::RelFileLocator;
use types_tuple::ItemPointerData;

seam_core::seam!(
    /// `ResolveCminCmaxDuringDecoding(tuplecid_data, snapshot, htup, buffer,
    /// &cmin, &cmax)` (reorderbuffer.c) — look up the actual cmin/cmax for a
    /// tuple seen by a historic (logical-decoding) MVCC snapshot, resolving any
    /// combo CID via the decoded tuplecid hash. `cmin`/`cmax` carry the C
    /// in/out-parameter values; the returned [`ResolveCminCmaxResult`] bundles
    /// the C `bool` return with the resolved out-parameters.
    pub fn resolve_cmin_cmax_during_decoding(
        snapshot: types_snapshot::SnapshotData,
        htup: types_tuple::heaptuple::HeapTupleData<'_>,
        buffer: types_storage::storage::Buffer,
        cmin: types_core::CommandId,
        cmax: types_core::CommandId,
    ) -> types_error::PgResult<types_snapshot::snapshot::ResolveCminCmaxResult>
);

seam_core::seam!(
    /// `ReorderBufferAllocate()`.
    pub fn ReorderBufferAllocate() -> ReorderBufferHandle
);
seam_core::seam!(
    /// `ReorderBufferFree(rb)`.
    pub fn ReorderBufferFree(rb: ReorderBufferHandle)
);
seam_core::seam!(
    /// Wire `rb->private_data = ctx` and install every `*_cb_wrapper`
    /// trampoline (the ReorderBuffer-driven callbacks logical.c owns).
    pub fn wire_reorderbuffer_callbacks(rb: ReorderBufferHandle)
);
seam_core::seam!(
    /// `rb->output_rewrites = value`.
    pub fn set_output_rewrites(rb: ReorderBufferHandle, value: bool)
);
seam_core::seam!(
    /// Read the eight `ReorderBuffer` stat counters (`UpdateDecodingStats`).
    pub fn reorderbuffer_stats(rb: ReorderBufferHandle) -> ReorderBufferStats
);
seam_core::seam!(
    /// Zero the eight `ReorderBuffer` stat counters after reporting.
    pub fn reorderbuffer_reset_stats(rb: ReorderBufferHandle)
);

// ---------------------------------------------------------------------------
// Seams consumed by snapbuild.c (the historic-snapshot builder).
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `ReorderBufferXidHasBaseSnapshot(rb, xid)`.
    pub fn ReorderBufferXidHasBaseSnapshot(rb: ReorderBufferHandle, xid: TransactionId) -> bool
);
seam_core::seam!(
    /// `ReorderBufferSetBaseSnapshot(rb, xid, lsn, snap)` — hands a historic
    /// catalog snapshot to the in-progress transaction `xid`. The reorderbuffer
    /// owner stores its own copy; the builder's refcount bookkeeping is handled
    /// caller-side to mirror the C `SnapBuildSnapIncRefcount` discipline.
    pub fn ReorderBufferSetBaseSnapshot(
        rb: ReorderBufferHandle,
        xid: TransactionId,
        lsn: XLogRecPtr,
        snap: types_snapshot::SnapshotData,
    )
);
seam_core::seam!(
    /// `ReorderBufferXidSetCatalogChanges(rb, xid, lsn)`.
    pub fn ReorderBufferXidSetCatalogChanges(
        rb: ReorderBufferHandle,
        xid: TransactionId,
        lsn: XLogRecPtr,
    )
);
seam_core::seam!(
    /// `ReorderBufferAddNewTupleCids(rb, xid, lsn, locator, tid, cmin, cmax,
    /// combocid)`.
    pub fn ReorderBufferAddNewTupleCids(
        rb: ReorderBufferHandle,
        xid: TransactionId,
        lsn: XLogRecPtr,
        locator: RelFileLocator,
        tid: ItemPointerData,
        cmin: types_core::CommandId,
        cmax: types_core::CommandId,
        combocid: types_core::CommandId,
    )
);
seam_core::seam!(
    /// `ReorderBufferAddNewCommandId(rb, xid, lsn, cid)`.
    pub fn ReorderBufferAddNewCommandId(
        rb: ReorderBufferHandle,
        xid: TransactionId,
        lsn: XLogRecPtr,
        cid: types_core::CommandId,
    )
);
seam_core::seam!(
    /// `ReorderBufferXidHasCatalogChanges(rb, xid)`.
    pub fn ReorderBufferXidHasCatalogChanges(rb: ReorderBufferHandle, xid: TransactionId) -> bool
);
seam_core::seam!(
    /// `ReorderBufferGetOldestXmin(rb)`.
    pub fn ReorderBufferGetOldestXmin(rb: ReorderBufferHandle) -> TransactionId
);
seam_core::seam!(
    /// `ReorderBufferSetRestartPoint(rb, ptr)`.
    pub fn ReorderBufferSetRestartPoint(rb: ReorderBufferHandle, ptr: XLogRecPtr)
);
seam_core::seam!(
    /// `ReorderBufferAddSnapshot(rb, xid, lsn, snap)` — adds an additional
    /// catalog snapshot change to in-progress transaction `xid`.
    pub fn ReorderBufferAddSnapshot(
        rb: ReorderBufferHandle,
        xid: TransactionId,
        lsn: XLogRecPtr,
        snap: types_snapshot::SnapshotData,
    )
);
seam_core::seam!(
    /// `ReorderBufferAddDistributedInvalidations(rb, xid, lsn, nmsgs, msgs)`.
    pub fn ReorderBufferAddDistributedInvalidations(
        rb: ReorderBufferHandle,
        xid: TransactionId,
        lsn: XLogRecPtr,
        msgs: Vec<SharedInvalidationMessage>,
    )
);
seam_core::seam!(
    /// `ReorderBufferGetInvalidations(rb, xid, &msgs)` — returns the
    /// invalidation messages generated by the committed transaction `xid`
    /// (the C `ninvalidations` is the returned vector's length).
    pub fn ReorderBufferGetInvalidations(
        rb: ReorderBufferHandle,
        xid: TransactionId,
    ) -> Vec<SharedInvalidationMessage>
);
seam_core::seam!(
    /// `ReorderBufferGetCatalogChangesXacts(rb)` — the catalog-modifying xids
    /// that are not yet committed (`rb->catchange_txns`), in `xidComparator`
    /// (sorted) order, as the C helper returns.
    pub fn ReorderBufferGetCatalogChangesXacts(
        rb: ReorderBufferHandle,
    ) -> Vec<TransactionId>
);
seam_core::seam!(
    /// `dclist_count(&rb->catchange_txns)`.
    pub fn reorder_buffer_catchange_count(rb: ReorderBufferHandle) -> usize
);
seam_core::seam!(
    /// `rb->current_restart_decoding_lsn`.
    pub fn reorder_buffer_current_restart_decoding_lsn(rb: ReorderBufferHandle) -> XLogRecPtr
);
seam_core::seam!(
    /// Iterate `rb->toplevel_by_lsn`, returning a handle per toplevel
    /// `ReorderBufferTXN` (snapbuild's `SnapBuildDistributeSnapshotAndInval`
    /// walks them in LSN order).
    pub fn reorder_buffer_toplevel_txns(rb: ReorderBufferHandle) -> Vec<TxnHandle>
);
seam_core::seam!(
    /// `ReorderBufferGetOldestTXN(rb)` — the oldest in-progress toplevel txn,
    /// or `None` when there is none.
    pub fn ReorderBufferGetOldestTXN(rb: ReorderBufferHandle) -> Option<TxnHandle>
);
seam_core::seam!(
    /// `txn->xid` of a `ReorderBufferTXN`.
    pub fn reorder_buffer_txn_xid(rb: ReorderBufferHandle, txn: TxnHandle) -> TransactionId
);
seam_core::seam!(
    /// `txn->restart_decoding_lsn` of a `ReorderBufferTXN`.
    pub fn reorder_buffer_txn_restart_decoding_lsn(
        rb: ReorderBufferHandle,
        txn: TxnHandle,
    ) -> XLogRecPtr
);
seam_core::seam!(
    /// `rbtxn_is_prepared(txn)` for a `ReorderBufferTXN`.
    pub fn reorder_buffer_txn_is_prepared(rb: ReorderBufferHandle, txn: TxnHandle) -> bool
);

// ---------------------------------------------------------------------------
// Seams consumed by decode.c (the change-replay entry points).
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `ReorderBufferAssignChild(rb, xid, subxid, lsn)` — record that `subxid`
    /// is a subtransaction of `xid`, as of `lsn`.
    pub fn ReorderBufferAssignChild(
        rb: ReorderBufferHandle,
        xid: TransactionId,
        subxid: TransactionId,
        lsn: XLogRecPtr,
    )
);
seam_core::seam!(
    /// `ReorderBufferCommitChild(rb, xid, subxid, commit_lsn, end_lsn)` —
    /// associate a subtransaction with its toplevel txn at commit time.
    pub fn ReorderBufferCommitChild(
        rb: ReorderBufferHandle,
        xid: TransactionId,
        subxid: TransactionId,
        commit_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr,
    )
);
seam_core::seam!(
    /// `ReorderBufferCommit(rb, xid, commit_lsn, end_lsn, commit_time,
    /// origin_id, origin_lsn)` — replay a committed transaction to the output
    /// plugin.
    pub fn ReorderBufferCommit(
        rb: ReorderBufferHandle,
        xid: TransactionId,
        commit_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr,
        commit_time: TimestampTz,
        origin_id: RepOriginId,
        origin_lsn: XLogRecPtr,
    )
);
