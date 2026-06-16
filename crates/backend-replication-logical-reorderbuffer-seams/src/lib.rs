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

// ---------------------------------------------------------------------------
// Further decode.c entry points (the change-replay / commit-time family).
//
// These are declared here so `decode.c` can be ported against a complete seam
// surface; the owning reorder-buffer families (change replay, spill, streaming,
// cleanup/commit-time) are not yet landed, so the installed bodies panic loudly
// (mirror-PG-and-panic) until those families land.
//
// The decoded-change payload crosses as the pieces `decode.c` assembles: the
// `ReorderBufferChangeKind` discriminant plus the relation locator and the
// owned [`DecodedTuple`] images (the reorder buffer's `ReorderBufferTupleBuf`),
// rather than the owner-private `ReorderBufferChange` struct (which would form a
// crate cycle).
// ---------------------------------------------------------------------------

/// The on-the-wire image of one decoded heap tuple (`ReorderBufferTupleBuf`):
/// the fixed `HeapTupleData` fields plus the contiguous tuple bytes the reorder
/// buffer owns. Mirrors
/// `backend_replication_logical_reorderbuffer::ReorderBufferTupleBuf` without
/// forming a dependency cycle on the owner crate.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DecodedTuple {
    /// `tuple.t_len`.
    pub t_len: u32,
    /// `tuple.t_self`.
    pub t_self: ItemPointerData,
    /// `tuple.t_tableOid`.
    pub t_table_oid: types_core::Oid,
    /// The contiguous tuple image (header + nulls bitmap + user data).
    pub data: Vec<u8>,
}

/// Which heap change `ReorderBufferQueueChange` is recording.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecodedChangeKind {
    /// `REORDER_BUFFER_CHANGE_INSERT`.
    Insert,
    /// `REORDER_BUFFER_CHANGE_UPDATE`.
    Update,
    /// `REORDER_BUFFER_CHANGE_DELETE`.
    Delete,
    /// `REORDER_BUFFER_CHANGE_INTERNAL_SPEC_INSERT`.
    SpecInsert,
    /// `REORDER_BUFFER_CHANGE_INTERNAL_SPEC_CONFIRM`.
    SpecConfirm,
    /// `REORDER_BUFFER_CHANGE_INTERNAL_SPEC_ABORT`.
    SpecAbort,
    /// `REORDER_BUFFER_CHANGE_TRUNCATE`.
    Truncate,
}

seam_core::seam!(
    /// `ReorderBufferProcessXid(rb, xid, lsn)` — note that `xid` produced WAL at
    /// `lsn`, creating the txn entry if it does not exist yet.
    pub fn ReorderBufferProcessXid(rb: ReorderBufferHandle, xid: TransactionId, lsn: XLogRecPtr)
);
seam_core::seam!(
    /// `ReorderBufferQueueChange(rb, xid, lsn, change, toast_insert)` — queue one
    /// decoded heap change. The `change` is conveyed as its discriminant plus the
    /// relation locator and the decoded old/new tuple images.
    pub fn ReorderBufferQueueChange(
        rb: ReorderBufferHandle,
        xid: TransactionId,
        lsn: XLogRecPtr,
        kind: DecodedChangeKind,
        rlocator: RelFileLocator,
        oldtuple: Option<DecodedTuple>,
        newtuple: Option<DecodedTuple>,
        toast_insert: bool,
    )
);
seam_core::seam!(
    /// `ReorderBufferQueueMessage(rb, xid, snapshot_now, lsn, transactional,
    /// prefix, message_size, message)` — queue a logical decoding message.
    pub fn ReorderBufferQueueMessage(
        rb: ReorderBufferHandle,
        xid: TransactionId,
        lsn: XLogRecPtr,
        transactional: bool,
        prefix: Vec<u8>,
        message: Vec<u8>,
    )
);
seam_core::seam!(
    /// `ReorderBufferForget(rb, xid, lsn)` — discard a transaction's changes
    /// without replaying them (e.g. its catalog snapshot was never built).
    pub fn ReorderBufferForget(rb: ReorderBufferHandle, xid: TransactionId, lsn: XLogRecPtr)
);
seam_core::seam!(
    /// `ReorderBufferAbort(rb, xid, lsn, abort_time)` — abort a transaction and
    /// its subtransactions.
    pub fn ReorderBufferAbort(
        rb: ReorderBufferHandle,
        xid: TransactionId,
        lsn: XLogRecPtr,
        abort_time: TimestampTz,
    )
);
seam_core::seam!(
    /// `ReorderBufferAbortOld(rb, oldestRunningXid)` — abort in-progress txns
    /// that started before `oldestRunningXid` (crash recovery cleanup).
    pub fn ReorderBufferAbortOld(rb: ReorderBufferHandle, oldest_running_xid: TransactionId)
);
seam_core::seam!(
    /// `ReorderBufferFinishPrepared(rb, xid, commit_lsn, end_lsn, two_phase_at,
    /// commit_time, origin_id, origin_lsn, gid, is_commit)` — replay the
    /// commit/abort of a previously prepared transaction.
    pub fn ReorderBufferFinishPrepared(
        rb: ReorderBufferHandle,
        xid: TransactionId,
        commit_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr,
        two_phase_at: XLogRecPtr,
        commit_time: TimestampTz,
        origin_id: RepOriginId,
        origin_lsn: XLogRecPtr,
        gid: Vec<u8>,
        is_commit: bool,
    )
);
seam_core::seam!(
    /// `ReorderBufferPrepare(rb, xid, gid)` — replay a PREPARE TRANSACTION.
    pub fn ReorderBufferPrepare(rb: ReorderBufferHandle, xid: TransactionId, gid: Vec<u8>)
);
seam_core::seam!(
    /// `ReorderBufferSkipPrepare(rb, xid)` — mark that the prepare for `xid`
    /// should be skipped (the plugin's `filter_prepare_cb` returned true).
    pub fn ReorderBufferSkipPrepare(rb: ReorderBufferHandle, xid: TransactionId)
);
seam_core::seam!(
    /// `ReorderBufferImmediateInvalidation(rb, ninvalidations, invalidations)` —
    /// execute cache invalidations immediately (XLOG_XACT_INVALIDATIONS outside a
    /// transaction).
    pub fn ReorderBufferImmediateInvalidation(
        rb: ReorderBufferHandle,
        invalidations: Vec<SharedInvalidationMessage>,
    )
);
seam_core::seam!(
    /// `ReorderBufferAddInvalidations(rb, xid, lsn, ninvalidations,
    /// invalidations)` — accumulate cache invalidations for txn `xid`.
    pub fn ReorderBufferAddInvalidations(
        rb: ReorderBufferHandle,
        xid: TransactionId,
        lsn: XLogRecPtr,
        invalidations: Vec<SharedInvalidationMessage>,
    )
);
seam_core::seam!(
    /// `ReorderBufferRememberPrepareInfo(rb, xid, prepare_lsn, end_lsn,
    /// prepare_time, origin_id, origin_lsn)` — stash the metadata needed to later
    /// replay the prepared transaction's commit/abort. Returns the C `bool`
    /// indicating whether the prepare should proceed.
    pub fn ReorderBufferRememberPrepareInfo(
        rb: ReorderBufferHandle,
        xid: TransactionId,
        prepare_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr,
        prepare_time: TimestampTz,
        origin_id: RepOriginId,
        origin_lsn: XLogRecPtr,
    ) -> bool
);
