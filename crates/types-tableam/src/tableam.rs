//! `access/tableam.h` — the table-AM dispatch vocabulary: the `TM_*` result
//! types and the `TableAmRoutine` vtable, trimmed to the callbacks the
//! tableam dispatch unit invokes. Further callbacks are added as their
//! dispatch wrappers are ported.

use core::any::Any;
use std::boxed::Box;
use std::sync::Arc;
use std::vec::Vec;

use types_core::primitive::Oid;
use types_core::xact::CommandId;
use types_core::TransactionId;
use types_error::PgResult;
use types_nodes::{TupleSlotKind, TupleTableSlot};
use types_snapshot::SnapshotData;
use types_tuple::heaptuple::ItemPointerData;

use crate::relscan::{ParallelTableScanDescData, TableScanDesc, TableScanDescData};
use crate::scankey::ScanKeyData;

/// `Snapshot` (`typedef struct SnapshotData *Snapshot`) as it crosses the
/// tableam layer: `None` models the C `SnapshotAny` / `InvalidSnapshot`
/// pointer identities (the only non-payload snapshots these paths pass).
pub type Snapshot = Option<SnapshotData>;

/// `LockTupleMode` (`nodes/lockoptions.h`).
pub type LockTupleMode = i32;
pub const LockTupleKeyShare: LockTupleMode = 0;
pub const LockTupleShare: LockTupleMode = 1;
pub const LockTupleNoKeyExclusive: LockTupleMode = 2;
pub const LockTupleExclusive: LockTupleMode = 3;

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
/// working state. The base member `rel` crosses as the relation's `Oid`; the
/// AM-specific tail (heap's `IndexFetchHeapData`) rides opaquely in
/// `am_private`.
pub struct IndexFetchTableData {
    /// `rel` — the relation the fetch was begun on.
    pub rel: Oid,
    /// The AM-private payload, owned by the access method that created it.
    pub am_private: Option<Box<dyn Any>>,
}

/// `BulkInsertStateData` (`access/heapam.h`) — opaque bulk-insert state; the
/// tableam layer only passes it through (the dispatch unit's own callers
/// always pass the C `NULL`, i.e. `None`).
pub struct BulkInsertStateData {
    pub am_private: Option<Box<dyn Any>>,
}

/// `TableAmRoutine` (`access/tableam.h`) — the table-access-method API
/// vtable, trimmed to the callbacks the dispatch unit (`tableam.c` and the
/// `tableam.h` wrappers it itself uses) invokes. All of these are required
/// callbacks in C (`GetTableAmRoutine` validates them non-NULL), so the
/// fields are plain `fn` pointers. The relation receiver crosses as its
/// `Oid`; failure surfaces mirror the heap implementations (every one of
/// these can `ereport(ERROR)` except `slot_callbacks` and
/// `parallelscan_estimate`).
#[derive(Clone, Copy)]
pub struct TableAmRoutine {
    /// `slot_callbacks(rel)` — slot implementation suitable for the AM.
    pub slot_callbacks: fn(rel: Oid) -> TupleSlotKind,

    /// `scan_begin(rel, snapshot, nkeys, key, pscan, flags)` — start a scan.
    pub scan_begin: fn(
        rel: Oid,
        snapshot: Snapshot,
        nkeys: i32,
        key: Vec<ScanKeyData>,
        pscan: Option<Arc<ParallelTableScanDescData>>,
        flags: u32,
    ) -> PgResult<TableScanDesc>,

    /// `parallelscan_estimate(rel)` — DSM space needed for the AM's shared
    /// parallel-scan state.
    pub parallelscan_estimate: fn(rel: Oid) -> usize,

    /// `parallelscan_initialize(rel, pscan)` — initialize the shared
    /// descriptor; returns the size needed (same as the estimate).
    pub parallelscan_initialize:
        fn(rel: Oid, pscan: &mut ParallelTableScanDescData) -> PgResult<usize>,

    /// `parallelscan_reinitialize(rel, pscan)` — reinitialize for a rescan.
    pub parallelscan_reinitialize:
        fn(rel: Oid, pscan: &ParallelTableScanDescData) -> PgResult<()>,

    /// `index_fetch_begin(rel)` — set up index-fetch state.
    pub index_fetch_begin: fn(rel: Oid) -> PgResult<Box<IndexFetchTableData>>,

    /// `index_fetch_end(scan)` — release index-fetch resources.
    pub index_fetch_end: fn(scan: Box<IndexFetchTableData>) -> PgResult<()>,

    /// `index_fetch_tuple(scan, tid, snapshot, slot, call_again, all_dead)`
    /// — fetch the tuple at `tid` into `slot`, returning true on a
    /// snapshot-visible match.
    pub index_fetch_tuple: fn(
        scan: &mut IndexFetchTableData,
        tid: &ItemPointerData,
        snapshot: &Snapshot,
        slot: &mut TupleTableSlot,
        call_again: &mut bool,
        all_dead: Option<&mut bool>,
    ) -> PgResult<bool>,

    /// `tuple_tid_valid(scan, tid)` — is `tid` potentially valid (within the
    /// relation's current size)?
    pub tuple_tid_valid:
        fn(scan: &mut TableScanDescData, tid: &ItemPointerData) -> PgResult<bool>,

    /// `tuple_get_latest_tid(scan, tid)` — chase the latest row version of
    /// the chain starting at `tid`.
    pub tuple_get_latest_tid:
        fn(scan: &mut TableScanDescData, tid: &mut ItemPointerData) -> PgResult<()>,

    /// `tuple_insert(rel, slot, cid, options, bistate)`.
    pub tuple_insert: fn(
        rel: Oid,
        slot: &mut TupleTableSlot,
        cid: CommandId,
        options: i32,
        bistate: Option<&mut BulkInsertStateData>,
    ) -> PgResult<()>,

    /// `tuple_delete(rel, tid, cid, snapshot, crosscheck, wait, tmfd,
    /// changingPart)`.
    pub tuple_delete: fn(
        rel: Oid,
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
    pub tuple_update: fn(
        rel: Oid,
        otid: &ItemPointerData,
        slot: &mut TupleTableSlot,
        cid: CommandId,
        snapshot: &Snapshot,
        crosscheck: &Snapshot,
        wait: bool,
        tmfd: &mut TM_FailureData,
        lockmode: &mut LockTupleMode,
        update_indexes: &mut TU_UpdateIndexes,
    ) -> PgResult<TM_Result>,
}
