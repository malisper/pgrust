//! F0 — the keystone shared-memory model + type expansion every other family
//! builds on (`storage/ipc/procarray.c`).
//!
//! Owns:
//!  - [`ProcArrayStruct`], the in-shmem ProcArray header
//!    (`pgprocnos[FLEXIBLE_ARRAY_MEMBER]` + the KnownAssignedXids ring bounds +
//!    `replication_slot_xmin`/`replication_slot_catalog_xmin`), allocated via
//!    `backend-storage-ipc-shmem` `ShmemInitStruct`/`add_size`/`mul_size`.
//!  - [`ProcArrayProcSlot`], the dense per-slot mirror folding the
//!    `ProcGlobal->{xids,subxidStates,statusFlags}` arrays and the live-PGPROC
//!    `{xid,xmin,subxids,databaseId,tempNamespaceId}` fields the way the
//!    idiomatic crate models a single ProcArray slot.
//!  - the real [`GlobalVisState`] struct (`definitely_needed`/`maybe_needed`
//!    `FullTransactionId` pair) — currently the rest of the tree only holds a
//!    `GlobalVisStateHandle` (`types_vacuum`); this is the real backing struct
//!    the handle resolves to.
//!  - [`ComputeXidHorizonsResult`], the `ComputeXidHorizons()` output the F3
//!    horizon family produces.
//!  - the file-static process-locals (`procArray`, the KnownAssignedXids
//!    cursor, `GlobalVis{Shared,Catalog,Data,Temp}Rels`, `latestObservedXid`,
//!    the `cachedXidIsNotInProgress`/`cachedXidIsRunning` pair) as backend
//!    thread-locals (forked-child convention).
//!
//! The keystone functions (`ProcArrayShmemSize`, `ProcArrayShmemInit`,
//! `GetMaxSnapshotXidCount`, `GetMaxSnapshotSubxidCount`) plus the
//! `FullTransactionId`/`TransactionId` arithmetic helpers shared by the
//! horizon/visibility families live here. Bodies are mirror-pg-and-panic until
//! the fill stage lands the real allocator + arithmetic.

use std::cell::RefCell;

use types_core::{FullTransactionId, Size, TransactionId};
use types_error::PgResult;

/// `ProcArrayStruct` (`storage/ipc/procarray.c`) — the in-shared-memory
/// ProcArray header. The C `pgprocnos[FLEXIBLE_ARRAY_MEMBER]` trailing array is
/// modelled as an owned `Vec<i32>` of indexes into `ProcGlobal->allProcs[]`
/// sized to `PROCARRAY_MAXPROCS` at `ProcArrayShmemInit` time (the FLEXIBLE
/// member is the genuinely-shared payload; the header is a per-process view of
/// the same shmem region).
#[derive(Debug)]
pub struct ProcArrayStruct {
    /// number of valid `pgprocnos` entries.
    pub numProcs: i32,
    /// allocated size of the `pgprocnos` array.
    pub maxProcs: i32,

    // --- Known assigned XIDs ring bounds (hot standby) ---
    /// allocated size of the `KnownAssignedXids` ring.
    pub maxKnownAssignedXids: i32,
    /// current number of valid entries in the ring.
    pub numKnownAssignedXids: i32,
    /// index of the oldest valid element.
    pub tailKnownAssignedXids: i32,
    /// index of the newest element, + 1.
    pub headKnownAssignedXids: i32,

    /// highest subxid removed from `KnownAssignedXids` to prevent overflow, or
    /// `InvalidTransactionId` if none.
    pub lastOverflowedXid: TransactionId,

    /// oldest xmin of any replication slot.
    pub replication_slot_xmin: TransactionId,
    /// oldest catalog xmin of any replication slot.
    pub replication_slot_catalog_xmin: TransactionId,

    /// indexes into `ProcGlobal->allProcs[]` (`pgprocnos[FLEXIBLE_ARRAY_MEMBER]`).
    pub pgprocnos: Vec<i32>,
}

/// The dense per-slot mirror of one ProcArray entry, as the idiomatic crate
/// models a slot: it folds the `ProcGlobal` dense arrays
/// (`ProcGlobal->xids[i]`, `ProcGlobal->subxidStates[i]`,
/// `ProcGlobal->statusFlags[i]`) together with the live-PGPROC fields the
/// snapshot/horizon scans read (`PGPROC->{xid,xmin,subxids,databaseId,
/// tempNamespaceId}`). One of these per `pgxactoff` (== `pgprocnos[index]`).
#[derive(Clone, Copy, Debug, Default)]
pub struct ProcArrayProcSlot {
    /// `ProcGlobal->xids[i]` (== `PGPROC->xid`): the proc's top-level XID, or
    /// `InvalidTransactionId` if not assigned.
    pub xid: TransactionId,
    /// `PGPROC->xmin`: the proc's advertised xmin.
    pub xmin: TransactionId,
    /// `ProcGlobal->subxidStates[i].count`: cached subxid count.
    pub subxid_count: i32,
    /// `ProcGlobal->subxidStates[i].overflowed`: whether the subxid cache
    /// overflowed.
    pub subxid_overflowed: bool,
    /// `ProcGlobal->statusFlags[i]` (mirror of `PGPROC->statusFlags`).
    pub status_flags: u8,
    /// `PGPROC->databaseId`.
    pub databaseId: types_core::Oid,
    /// `PGPROC->tempNamespaceId`.
    pub tempNamespaceId: types_core::Oid,
}

/// `struct GlobalVisState` (`utils/snapmgr.h`, body in procarray.c) — the
/// snapshot-visibility test boundaries. The whole-tree `GlobalVisStateHandle`
/// (`types_vacuum`) resolves to one of these (the four
/// `GlobalVis{Shared,Catalog,Data,Temp}Rels` process-locals below).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GlobalVisState {
    /// XIDs `>=` are considered running by some backend.
    pub definitely_needed: FullTransactionId,
    /// XIDs `<` are not considered running by any backend.
    pub maybe_needed: FullTransactionId,
}

/// `ComputeXidHorizonsResult` (procarray.c) — the output of `ComputeXidHorizons()`
/// consumed by the F3 horizon family and `GetSnapshotData`.
#[derive(Clone, Copy, Debug, Default)]
pub struct ComputeXidHorizonsResult {
    /// `TransamVariables->latestCompletedXid` snapshotted under `ProcArrayLock`.
    pub latest_completed: FullTransactionId,
    /// `procArray->replication_slot_xmin`.
    pub slot_xmin: TransactionId,
    /// `procArray->replication_slot_catalog_xmin`.
    pub slot_catalog_xmin: TransactionId,
    /// oldest xid any backend (incl. VACUUM) might still consider running.
    pub oldest_considered_running: TransactionId,
    /// shared-relation removable cutoff.
    pub shared_oldest_nonremovable: TransactionId,
    /// like `shared_oldest_nonremovable` but ignoring replication slots.
    pub shared_oldest_nonremovable_raw: TransactionId,
    /// catalog-relation removable cutoff.
    pub catalog_oldest_nonremovable: TransactionId,
    /// normal-relation removable cutoff.
    pub data_oldest_nonremovable: TransactionId,
    /// temp-relation removable cutoff (this session only).
    pub temp_oldest_nonremovable: TransactionId,
}

// ---------------------------------------------------------------------------
// File-static process-locals (procarray.c top-of-file `static` declarations).
// Inherited at fork as per-process views; modelled as backend thread-locals.
// ---------------------------------------------------------------------------

thread_local! {
    /// `static ProcArrayStruct *procArray;` — the per-process pointer into the
    /// shared ProcArray header (set by `ProcArrayShmemInit`).
    pub static PROC_ARRAY: RefCell<Option<ProcArrayStruct>> = const { RefCell::new(None) };

    /// `static TransactionId *KnownAssignedXids;` — the hot-standby ring buffer
    /// (lives in the shmem region; the cursor bounds are in `ProcArrayStruct`).
    pub static KNOWN_ASSIGNED_XIDS: RefCell<Vec<TransactionId>> = const { RefCell::new(Vec::new()) };
    /// `static bool *KnownAssignedXidsValid;` — the validity bitmap parallel to
    /// `KnownAssignedXids`.
    pub static KNOWN_ASSIGNED_XIDS_VALID: RefCell<Vec<bool>> = const { RefCell::new(Vec::new()) };

    /// `static GlobalVisState GlobalVisSharedRels;`
    pub static GLOBAL_VIS_SHARED_RELS: RefCell<GlobalVisState> =
        const { RefCell::new(GlobalVisState { definitely_needed: FullTransactionId { value: 0 }, maybe_needed: FullTransactionId { value: 0 } }) };
    /// `static GlobalVisState GlobalVisCatalogRels;`
    pub static GLOBAL_VIS_CATALOG_RELS: RefCell<GlobalVisState> =
        const { RefCell::new(GlobalVisState { definitely_needed: FullTransactionId { value: 0 }, maybe_needed: FullTransactionId { value: 0 } }) };
    /// `static GlobalVisState GlobalVisDataRels;`
    pub static GLOBAL_VIS_DATA_RELS: RefCell<GlobalVisState> =
        const { RefCell::new(GlobalVisState { definitely_needed: FullTransactionId { value: 0 }, maybe_needed: FullTransactionId { value: 0 } }) };
    /// `static GlobalVisState GlobalVisTempRels;`
    pub static GLOBAL_VIS_TEMP_RELS: RefCell<GlobalVisState> =
        const { RefCell::new(GlobalVisState { definitely_needed: FullTransactionId { value: 0 }, maybe_needed: FullTransactionId { value: 0 } }) };

    /// `static TransactionId latestObservedXid;`
    pub static LATEST_OBSERVED_XID: RefCell<TransactionId> = const { RefCell::new(0) };

    /// `static TransactionId cachedXidIsNotInProgress;` — the one-entry
    /// `TransactionIdIsInProgress` negative cache.
    pub static CACHED_XID_IS_NOT_IN_PROGRESS: RefCell<TransactionId> = const { RefCell::new(0) };

    /// `static TransactionId standbySnapshotPendingXmin;` — pending xmin while
    /// waiting for a running-xacts WAL record during standby startup.
    pub static STANDBY_SNAPSHOT_PENDING_XMIN: RefCell<TransactionId> = const { RefCell::new(0) };
}

// ---------------------------------------------------------------------------
// Keystone functions.
// ---------------------------------------------------------------------------

/// `ProcArrayShmemSize(void)` (procarray.c) — shared-memory bytes the ProcArray
/// header + KnownAssignedXids ring need. Uses `add_size`/`mul_size` overflow
/// checks (carried on `Err`).
pub fn ProcArrayShmemSize() -> PgResult<Size> {
    panic!("decomp: ProcArrayShmemSize not yet filled")
}

/// `ProcArrayShmemInit(void)` (procarray.c) — allocate-or-attach the ProcArray
/// header (`ShmemInitStruct`) and the KnownAssignedXids ring; wire the
/// `procArray` process-local.
pub fn ProcArrayShmemInit() -> PgResult<()> {
    panic!("decomp: ProcArrayShmemInit not yet filled")
}

/// `GetMaxSnapshotXidCount(void)` (procarray.c) — the largest possible `xip[]`
/// length (`procArray->maxProcs`).
pub fn GetMaxSnapshotXidCount() -> i32 {
    panic!("decomp: GetMaxSnapshotXidCount not yet filled")
}

/// `GetMaxSnapshotSubxidCount(void)` (procarray.c) — the largest possible
/// `subxip[]` length (`TOTAL_MAX_CACHED_SUBXIDS`).
pub fn GetMaxSnapshotSubxidCount() -> i32 {
    panic!("decomp: GetMaxSnapshotSubxidCount not yet filled")
}

// ---------------------------------------------------------------------------
// FullTransactionId / TransactionId arithmetic helpers shared by the
// horizon (F3) and visibility (F2/F4) families.
// ---------------------------------------------------------------------------

/// `FullTransactionIdAdvance(&dest)` (`access/transam.h`, used pervasively in
/// procarray.c) — advance a `FullTransactionId` past the next-not-special XID,
/// skipping the special XIDs at epoch boundaries.
pub fn FullTransactionIdAdvance(_dest: &mut FullTransactionId) {
    panic!("decomp: FullTransactionIdAdvance not yet filled")
}

/// `TransactionIdOlder(a, b)` (procarray.c static helper) — the older
/// (numerically-smaller-in-xid-order) of two `TransactionId`s, treating
/// `InvalidTransactionId` as "no constraint".
pub fn TransactionIdOlder(_a: TransactionId, _b: TransactionId) -> TransactionId {
    panic!("decomp: TransactionIdOlder not yet filled")
}

/// `FullTransactionIdNewer(a, b)` (procarray.c static helper) — the newer of
/// two `FullTransactionId`s, treating an invalid value as "no constraint".
pub fn FullTransactionIdNewer(
    _a: FullTransactionId,
    _b: FullTransactionId,
) -> FullTransactionId {
    panic!("decomp: FullTransactionIdNewer not yet filled")
}

/// `FullXidRelativeTo(rel, xid)` (procarray.c) — promote a 32-bit
/// `TransactionId` to a `FullTransactionId` relative to the `rel`
/// `FullTransactionId` epoch (handles wraparound around `rel`).
pub fn FullXidRelativeTo(_rel: FullTransactionId, _xid: TransactionId) -> FullTransactionId {
    panic!("decomp: FullXidRelativeTo not yet filled")
}

/// Install the F0-owned inward seams (`backend-storage-ipc-procarray-seams`):
/// the shmem-sizing/init seams and the max-snapshot-count seams.
pub fn init_seams() {
    use backend_storage_ipc_procarray_seams as seams;

    seams::proc_array_shmem_size::set(ProcArrayShmemSize);
    seams::proc_array_shmem_init::set(ProcArrayShmemInit);
    seams::get_max_snapshot_xid_count::set(GetMaxSnapshotXidCount);
    seams::get_max_snapshot_subxid_count::set(GetMaxSnapshotSubxidCount);
}
