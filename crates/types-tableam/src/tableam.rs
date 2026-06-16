//! `access/tableam.h` — the table-AM dispatch vocabulary: the `TM_*` result
//! types and the `TableAmRoutine` vtable, trimmed to the callbacks the
//! tableam dispatch unit invokes. Further callbacks are added as their
//! dispatch wrappers are ported.

use std::boxed::Box;
use std::sync::Arc;
use std::vec::Vec;

use mcx::{Mcx, PgVec};
use types_core::xact::CommandId;
use types_core::TransactionId;
use types_error::PgResult;
use types_slot::{SlotData, TupleSlotKind};
use types_rel::Relation;
use types_scan::sdir::ScanDirection;
use types_snapshot::SnapshotData;
use types_storage::RelFileLocator;
use types_tuple::heaptuple::ItemPointerData;

use crate::relscan::{ParallelTableScanDescData, TableScanDesc, TableScanDescData};
use crate::scankey::ScanKeyData;

/// `Snapshot` (`typedef struct SnapshotData *Snapshot`) as it crosses the
/// tableam layer: `None` models the C `SnapshotAny` / `InvalidSnapshot`
/// pointer identities (the only non-payload snapshots these paths pass).
pub type Snapshot = Option<SnapshotData>;

/// `LockTupleMode` (`nodes/lockoptions.h`).
#[repr(i32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LockTupleMode {
    /// `SELECT FOR KEY SHARE`
    LockTupleKeyShare = 0,
    /// `SELECT FOR SHARE`
    LockTupleShare,
    /// `SELECT FOR NO KEY UPDATE`, and UPDATEs that don't modify key columns
    LockTupleNoKeyExclusive,
    /// `SELECT FOR UPDATE`, UPDATEs that modify key columns, and DELETE
    LockTupleExclusive,
}

pub use LockTupleMode::{
    LockTupleExclusive, LockTupleKeyShare, LockTupleNoKeyExclusive, LockTupleShare,
};

/// `LockWaitPolicy` (`nodes/lockoptions.h`) — what to do when a row to be
/// locked is already locked by another transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LockWaitPolicy {
    /// Wait for the lock to become available (default).
    LockWaitBlock = 0,
    /// Skip rows that can't be locked (SKIP LOCKED).
    LockWaitSkip,
    /// Raise an error if a row can't be locked (NOWAIT).
    LockWaitError,
}

/// `TUPLE_LOCK_FLAG_*` (`access/tableam.h`) — `table_tuple_lock` flags.
pub const TUPLE_LOCK_FLAG_LOCK_UPDATE_IN_PROGRESS: u8 = 1 << 0;
pub const TUPLE_LOCK_FLAG_FIND_LAST_VERSION: u8 = 1 << 1;

/// `TM_Result` (`access/tableam.h`) — result codes for `table_tuple_update`
/// and friends.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TM_Result {
    /// signals that the action succeeded
    TM_Ok = 0,
    /// the affected tuple wasn't visible to the relevant snapshot
    TM_Invisible,
    /// the affected tuple was already modified by the calling backend
    TM_SelfModified,
    /// the affected tuple was updated by another transaction
    TM_Updated,
    /// the affected tuple was deleted by another transaction
    TM_Deleted,
    /// the affected tuple is currently being modified by another session
    TM_BeingModified,
    /// lock couldn't be acquired, action skipped (only with skip-locked)
    TM_WouldBlock,
}

/// `TM_FailureData` (`access/tableam.h`) — additional information about a
/// failing tuple modification.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TM_FailureData {
    pub ctid: ItemPointerData,
    pub xmax: TransactionId,
    /// only valid for `TM_SelfModified`
    pub cmax: CommandId,
    pub traversed: bool,
}

/// `TU_UpdateIndexes` (`access/tableam.h`) — which indexes to update after a
/// tuple update.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TU_UpdateIndexes {
    /// No indexed columns were updated (incl. TID addressing of the tuple).
    TU_None = 0,
    /// A non-summarizing indexed column was updated, or the TID changed.
    TU_All,
    /// Only summarized columns were updated (TID unchanged).
    TU_Summarizing,
}

/// `IndexFetchTableData` (`access/tableam.h`) — the AM-private per-index-scan
/// working state. The base member `rel` is an alias handle of the open
/// relation the fetch was begun on; the AM-specific tail (heap's
/// `IndexFetchHeapData`) rides opaquely in `am_private`.
pub struct IndexFetchTableData<'mcx> {
    /// `rel` — the relation the fetch was begun on.
    pub rel: Relation<'mcx>,
    /// The AM-private payload, owned by the access method that created it and
    /// allocated in the scan's `mcx` arena (convention A). The `'mcx`-safe
    /// erased carrier with a tag-checked downcast (see [`crate::amopaque`]).
    pub am_private: Option<mcx::PgBox<'mcx, dyn crate::amopaque::AmOpaque<'mcx> + 'mcx>>,
}

/// `BulkInsertStateData` (`access/hio.h`) — state for bulk inserts, private to
/// `heapam.c` and `hio.c`. The tableam dispatch layer only ever passes this
/// through opaquely (its own callers pass the C `NULL`, i.e. `None`); `hio.c`
/// reads and updates these fields directly.
///
/// If `current_buf` isn't `InvalidBuffer`, we hold an extra pin on that buffer.
///
/// Not `Copy`: the `strategy` field is the backend-private ring handed out by
/// pointer (`Rc<RefCell<BufferAccessStrategyData>>` / `None`), exactly as C's
/// `BulkInsertStateData.strategy` is a `BufferAccessStrategy` pointer — C never
/// copies the struct by value (it passes `BulkInsertState`, a pointer to it).
#[derive(Clone, Debug, Default)]
pub struct BulkInsertStateData {
    /// `strategy` — our BULKWRITE strategy object (NULL == `None`).
    pub strategy: types_storage::buf::BufferAccessStrategy,
    /// `current_buf` — current insertion target page.
    pub current_buf: types_storage::Buffer,
    /// `next_free` — bulk-extension state: next still-unused page from the last
    /// extension (`last_free..next_free` are further unused pages).
    pub next_free: types_core::BlockNumber,
    /// `last_free` — bulk-extension state: last still-unused page.
    pub last_free: types_core::BlockNumber,
    /// `already_extended_by` — pages this bulk insert has extended by so far.
    pub already_extended_by: u32,
}

/// `TableAmRoutine` (`access/tableam.h`) — the table-access-method API
/// vtable, trimmed to the callbacks the dispatch unit (`tableam.c` and the
/// `tableam.h` wrappers it itself uses) invokes. All of these are required
/// callbacks in C (`GetTableAmRoutine` validates them non-NULL), so the
/// fields are plain `fn` pointers. The relation receiver is the open
/// relation's handle; failure surfaces mirror the heap implementations (every one of
/// these can `ereport(ERROR)` except `slot_callbacks` and
/// `parallelscan_estimate`).
#[derive(Clone, Copy)]
pub struct TableAmRoutine {
    /// `slot_callbacks(rel)` — slot implementation suitable for the AM.
    pub slot_callbacks: fn(rel: &Relation<'_>) -> TupleSlotKind,

    /// `scan_begin(mcx, rel, snapshot, nkeys, key, pscan, flags)` — start a
    /// scan. The leading `mcx` (convention A) is the arena the AM allocates the
    /// returned scan descriptor and its scan state in.
    pub scan_begin: for<'mcx> fn(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        snapshot: Snapshot,
        nkeys: i32,
        key: PgVec<'mcx, ScanKeyData<'mcx>>,
        pscan: Option<Arc<ParallelTableScanDescData>>,
        flags: u32,
    ) -> PgResult<TableScanDesc<'mcx>>,

    /// `scan_getnextslot(scan, direction, slot)` — fetch the next tuple of an
    /// in-progress scan into `slot`, returning `true` if a tuple was produced
    /// (`false` at end of scan).
    pub scan_getnextslot: for<'mcx> fn(
        mcx: Mcx<'mcx>,
        scan: &mut TableScanDescData<'mcx>,
        direction: ScanDirection,
        slot: &mut SlotData<'mcx>,
    ) -> PgResult<bool>,

    /// `parallelscan_estimate(rel)` — DSM space needed for the AM's shared
    /// parallel-scan state.
    pub parallelscan_estimate: fn(rel: &Relation<'_>) -> usize,

    /// `parallelscan_initialize(rel, pscan)` — initialize the shared
    /// descriptor; returns the size needed (same as the estimate).
    pub parallelscan_initialize:
        fn(rel: &Relation<'_>, pscan: &mut ParallelTableScanDescData) -> PgResult<usize>,

    /// `parallelscan_reinitialize(rel, pscan)` — reinitialize for a rescan.
    pub parallelscan_reinitialize:
        fn(rel: &Relation<'_>, pscan: &ParallelTableScanDescData) -> PgResult<()>,

    /// `index_fetch_begin(mcx, rel)` — set up index-fetch state in the `mcx`
    /// arena (convention A).
    pub index_fetch_begin:
        for<'mcx> fn(mcx: Mcx<'mcx>, rel: &Relation<'mcx>) -> PgResult<Box<IndexFetchTableData<'mcx>>>,

    /// `index_fetch_reset(data)` — release resources (buffer pins) held by
    /// the index fetch, without ending it.
    pub index_fetch_reset: fn(data: &mut IndexFetchTableData<'_>) -> PgResult<()>,

    /// `index_fetch_end(scan)` — release index-fetch resources.
    pub index_fetch_end: fn(scan: Box<IndexFetchTableData<'_>>) -> PgResult<()>,

    /// `index_fetch_tuple(scan, tid, snapshot, slot, call_again, all_dead)`
    /// — fetch the tuple at `tid` into `slot`, returning true on a
    /// snapshot-visible match.
    pub index_fetch_tuple: for<'mcx> fn(
        mcx: Mcx<'mcx>,
        scan: &mut IndexFetchTableData<'mcx>,
        tid: &ItemPointerData,
        snapshot: &Snapshot,
        slot: &mut SlotData<'mcx>,
        call_again: &mut bool,
        all_dead: Option<&mut bool>,
    ) -> PgResult<bool>,

    /// `scan_end(scan)` — release resources and deallocate the scan
    /// descriptor.
    pub scan_end: fn(scan: TableScanDesc<'_>) -> PgResult<()>,

    /// `scan_rescan(mcx, scan, key, set_params, allow_strat, allow_sync,
    /// allow_pagemode)` — restart a relation scan, optionally with new params.
    /// The leading `mcx` (convention A) is the arena the AM reinitializes the
    /// scan state in (`heap_rescan` re-runs `initscan`, which allocates).
    pub scan_rescan: for<'mcx> fn(
        mcx: Mcx<'mcx>,
        scan: &mut TableScanDescData<'mcx>,
        key: Option<&[ScanKeyData<'mcx>]>,
        set_params: bool,
        allow_strat: bool,
        allow_sync: bool,
        allow_pagemode: bool,
    ) -> PgResult<()>,

    /// `tuple_fetch_row_version(rel, tid, snapshot, slot)` — fetch the tuple at
    /// `tid` into `slot`, after a visibility test against `snapshot`; returns
    /// true if a visible tuple was found.
    pub tuple_fetch_row_version: for<'mcx> fn(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        tid: &ItemPointerData,
        snapshot: &Snapshot,
        slot: &mut SlotData<'mcx>,
    ) -> PgResult<bool>,

    /// `tuple_tid_valid(scan, tid)` — is `tid` potentially valid (within the
    /// relation's current size)?
    pub tuple_tid_valid:
        fn(scan: &mut TableScanDescData<'_>, tid: &ItemPointerData) -> PgResult<bool>,

    /// `tuple_get_latest_tid(mcx, scan, tid)` — chase the latest row version of
    /// the chain starting at `tid`. The leading `mcx` (convention A) is the
    /// arena the AM reads buffers under (`heap_get_latest_tid`).
    pub tuple_get_latest_tid: for<'mcx> fn(
        mcx: Mcx<'mcx>,
        scan: &mut TableScanDescData<'mcx>,
        tid: &mut ItemPointerData,
    ) -> PgResult<()>,

    /// `tuple_insert(mcx, rel, slot, cid, options, bistate)`.
    pub tuple_insert: for<'mcx> fn(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        slot: &mut SlotData<'mcx>,
        cid: CommandId,
        options: i32,
        bistate: Option<&mut BulkInsertStateData>,
    ) -> PgResult<()>,

    /// `tuple_delete(mcx, rel, tid, cid, snapshot, crosscheck, wait, tmfd,
    /// changingPart)`. The leading `mcx` (convention A) is the arena the AM
    /// works in (`heap_delete`).
    pub tuple_delete: for<'mcx> fn(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        tid: &ItemPointerData,
        cid: CommandId,
        snapshot: &Snapshot,
        crosscheck: &Snapshot,
        wait: bool,
        tmfd: &mut TM_FailureData,
        changingPart: bool,
    ) -> PgResult<TM_Result>,

    /// `tuple_update(rel, otid, slot, cid, snapshot, crosscheck, wait, tmfd,
    /// lockmode, update_indexes)`.
    pub tuple_update: for<'mcx> fn(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        otid: &ItemPointerData,
        slot: &mut SlotData<'mcx>,
        cid: CommandId,
        snapshot: &Snapshot,
        crosscheck: &Snapshot,
        wait: bool,
        tmfd: &mut TM_FailureData,
        lockmode: &mut LockTupleMode,
        update_indexes: &mut TU_UpdateIndexes,
    ) -> PgResult<TM_Result>,

    /// `tuple_lock(rel, tid, snapshot, slot, cid, mode, wait_policy, flags,
    /// tmfd)` — lock a tuple in the given mode, fetching it into `slot`.
    #[allow(clippy::type_complexity)]
    pub tuple_lock: for<'mcx> fn(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        tid: &ItemPointerData,
        snapshot: &Snapshot,
        slot: &mut SlotData<'mcx>,
        cid: CommandId,
        mode: LockTupleMode,
        wait_policy: LockWaitPolicy,
        flags: u8,
        tmfd: &mut TM_FailureData,
    ) -> PgResult<TM_Result>,

    /// `relation_set_new_filelocator(rel, newrlocator, persistence,
    /// &freezeXid, &minmulti)` — create storage for the relation's new
    /// relfilelocator (and its init fork if unlogged), handing back the
    /// AM-chosen `relfrozenxid`/`relminmxid` to store in pg_class.
    pub relation_set_new_filelocator: fn(
        rel: &Relation<'_>,
        newrlocator: &RelFileLocator,
        persistence: i8,
    ) -> PgResult<(u32, u32)>,

    /// `scan_analyze_next_block(scan, stream)` (`access/tableam.h`) — the
    /// outer-loop callback of `acquire_sample_rows`: pin and share-lock the next
    /// block to be sampled, leaving it the scan's current page. Returns `false`
    /// when the read stream is exhausted (no more blocks to sample).
    ///
    /// In C the second argument is `ReadStream *`, and the heap callback's only
    /// use of it is `read_stream_next_buffer(stream, NULL)` — pull the next
    /// already-pinned buffer. The read stream lives in `commands/analyze.c`
    /// (which sits far above this types crate), so rather than naming the
    /// higher-layer `ReadStream` type in the vtable we cross it as the
    /// `next_buffer` closure the owner builds over its stream — the same
    /// closure-across-layers technique the index-build callback uses. The
    /// closure returns the next pinned `Buffer`, or `InvalidBuffer` (0) at the
    /// end of the stream.
    pub scan_analyze_next_block: for<'mcx> fn(
        mcx: Mcx<'mcx>,
        scan: &mut TableScanDescData<'mcx>,
        next_buffer: &mut dyn FnMut() -> PgResult<types_storage::buf::Buffer>,
    ) -> PgResult<bool>,

    /// `scan_analyze_next_tuple(scan, OldestXmin, liverows, deadrows, slot)`
    /// (`access/tableam.h`) — the inner-loop callback of `acquire_sample_rows`:
    /// advance over the current block's line pointers, classifying each tuple
    /// for the live/dead counters, and store the next sampleable tuple into
    /// `slot`, returning `true` (leaving the buffer locked) or `false` at the
    /// end of the block (releasing the buffer lock + pin and clearing `slot`).
    /// `liverows`/`deadrows` are the running totals `acquire_sample_rows`
    /// maintains across blocks.
    pub scan_analyze_next_tuple: for<'mcx> fn(
        mcx: Mcx<'mcx>,
        scan: &mut TableScanDescData<'mcx>,
        oldest_xmin: TransactionId,
        liverows: &mut f64,
        deadrows: &mut f64,
        slot: &mut SlotData<'mcx>,
    ) -> PgResult<bool>,
}
