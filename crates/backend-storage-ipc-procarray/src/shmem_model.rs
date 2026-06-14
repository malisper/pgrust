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

use types_core::{
    FirstNormalFullTransactionId, FirstNormalTransactionId, FullTransactionId, Size,
    TransactionId, TransactionIdIsValid,
};
use types_error::PgResult;
use types_storage::storage::PGPROC_MAX_CACHED_SUBXIDS;

/// `PROCARRAY_MAXPROCS` (procarray.c) — `(MaxBackends + max_prepared_xacts)`.
/// The allocated size of the `pgprocnos` array and the per-cluster cap on
/// concurrently-running procs.
#[inline]
fn PROCARRAY_MAXPROCS() -> i32 {
    backend_utils_init_small::globals::MaxBackends()
        + backend_utils_misc_guc_tables::vars::max_prepared_xacts.read()
}

/// `TOTAL_MAX_CACHED_SUBXIDS` (procarray.c) —
/// `((PGPROC_MAX_CACHED_SUBXIDS + 1) * PROCARRAY_MAXPROCS)`. The identically
/// sized cap used by `KnownAssignedXids` and every local subxid scratch array.
#[inline]
fn TOTAL_MAX_CACHED_SUBXIDS() -> i32 {
    (PGPROC_MAX_CACHED_SUBXIDS as i32 + 1) * PROCARRAY_MAXPROCS()
}

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
    use backend_storage_ipc_shmem_seams as shmem;

    // Size of the ProcArray structure itself, up to the flexible
    // `pgprocnos[FLEXIBLE_ARRAY_MEMBER]` member: six `int` header words
    // (numProcs, maxProcs, maxKnownAssignedXids, numKnownAssignedXids,
    // tailKnownAssignedXids, headKnownAssignedXids) followed by three
    // `TransactionId` words (lastOverflowedXid, replication_slot_xmin,
    // replication_slot_catalog_xmin) — `offsetof(ProcArrayStruct, pgprocnos)`
    // is 36 on the C struct layout (c2rust verified).
    let mut size: Size = PROC_ARRAY_HEADER_SIZE;
    size = shmem::add_size::call(
        size,
        shmem::mul_size::call(
            core::mem::size_of::<i32>() as Size,
            PROCARRAY_MAXPROCS() as Size,
        )?,
    )?;

    // During Hot Standby processing we have a data structure called
    // KnownAssignedXids, created in shared memory. Local data structures are
    // also created in various backends during GetSnapshotData(),
    // TransactionIdIsInProgress() and GetRunningTransactionData(). All of the
    // main structures created in those functions must be identically sized,
    // since we may at times copy the whole of the data structures around. We
    // refer to this size as TOTAL_MAX_CACHED_SUBXIDS.
    if backend_utils_misc_guc_tables::vars::EnableHotStandby.read() {
        size = shmem::add_size::call(
            size,
            shmem::mul_size::call(
                core::mem::size_of::<TransactionId>() as Size,
                TOTAL_MAX_CACHED_SUBXIDS() as Size,
            )?,
        )?;
        size = shmem::add_size::call(
            size,
            shmem::mul_size::call(
                core::mem::size_of::<bool>() as Size,
                TOTAL_MAX_CACHED_SUBXIDS() as Size,
            )?,
        )?;
    }

    Ok(size)
}

/// `offsetof(ProcArrayStruct, pgprocnos)` (procarray.c) — the fixed header size
/// preceding the flexible `pgprocnos[]` array (six `int` + three
/// `TransactionId` words). The Rust [`ProcArrayStruct`] models the flexible
/// member as an owned `Vec`, so this mirrors the C struct's offset constant
/// (36, c2rust verified) rather than a Rust `offset_of!`.
const PROC_ARRAY_HEADER_SIZE: Size = 36;

/// `ProcArrayShmemInit(void)` (procarray.c) — allocate-or-attach the ProcArray
/// header (`ShmemInitStruct`) and the KnownAssignedXids ring; wire the
/// `procArray` process-local.
pub fn ProcArrayShmemInit() -> PgResult<()> {
    use backend_storage_ipc_shmem_seams as shmem;

    let maxprocs = PROCARRAY_MAXPROCS();
    let total_max_cached_subxids = TOTAL_MAX_CACHED_SUBXIDS();

    // Create or attach to the ProcArray shared structure. The C carves the
    // header + flexible `pgprocnos[PROCARRAY_MAXPROCS]` array from shared
    // memory; this dense model owns the header as a per-process `Vec`-backed
    // view, but we still call `ShmemInitStruct` so the genuinely-shared
    // allocate-or-attach + `found` semantics are honoured.
    let header_size = shmem::add_size::call(
        PROC_ARRAY_HEADER_SIZE,
        shmem::mul_size::call(
            core::mem::size_of::<i32>() as Size,
            maxprocs as Size,
        )?,
    )?;
    let (_addr, found) = shmem::shmem_init_struct::call("Proc Array", header_size)?;

    if !found {
        // We're the first - initialize.
        let proc_array = ProcArrayStruct {
            numProcs: 0,
            maxProcs: maxprocs,
            maxKnownAssignedXids: total_max_cached_subxids,
            numKnownAssignedXids: 0,
            tailKnownAssignedXids: 0,
            headKnownAssignedXids: 0,
            lastOverflowedXid: types_core::InvalidTransactionId,
            replication_slot_xmin: types_core::InvalidTransactionId,
            replication_slot_catalog_xmin: types_core::InvalidTransactionId,
            pgprocnos: vec![0; maxprocs as usize],
        };
        PROC_ARRAY.with(|p| *p.borrow_mut() = Some(proc_array));
        backend_access_transam_varsup_seams::init_xact_completion_count::call();
    }

    // `allProcs = ProcGlobal->allProcs;` — in C this caches the base of the
    // dense PGPROC array. Here the dense per-slot fields are reached through
    // the `backend-storage-lmgr-proc-seams` accessors keyed by `ProcNumber`,
    // so there is no per-process base pointer to cache (no-op).

    // Create or attach to the KnownAssignedXids arrays too, if needed.
    if backend_utils_misc_guc_tables::vars::EnableHotStandby.read() {
        let (_kax, _) = shmem::shmem_init_struct::call(
            "KnownAssignedXids",
            shmem::mul_size::call(
                core::mem::size_of::<TransactionId>() as Size,
                total_max_cached_subxids as Size,
            )?,
        )?;
        let (_kaxv, _) = shmem::shmem_init_struct::call(
            "KnownAssignedXidsValid",
            shmem::mul_size::call(
                core::mem::size_of::<bool>() as Size,
                total_max_cached_subxids as Size,
            )?,
        )?;
        // The ring + validity bitmap themselves are the genuinely-shared
        // payload modelled as the process-local `KNOWN_ASSIGNED_XIDS` /
        // `KNOWN_ASSIGNED_XIDS_VALID` views sized to the same cap.
        KNOWN_ASSIGNED_XIDS
            .with(|k| *k.borrow_mut() = vec![0; total_max_cached_subxids as usize]);
        KNOWN_ASSIGNED_XIDS_VALID
            .with(|k| *k.borrow_mut() = vec![false; total_max_cached_subxids as usize]);
    }

    Ok(())
}

/// `GetMaxSnapshotXidCount(void)` (procarray.c) — the largest possible `xip[]`
/// length (`procArray->maxProcs`).
pub fn GetMaxSnapshotXidCount() -> i32 {
    PROC_ARRAY.with(|p| {
        p.borrow()
            .as_ref()
            .expect("ProcArray accessed before ProcArrayShmemInit")
            .maxProcs
    })
}

/// `GetMaxSnapshotSubxidCount(void)` (procarray.c) — the largest possible
/// `subxip[]` length (`TOTAL_MAX_CACHED_SUBXIDS`).
pub fn GetMaxSnapshotSubxidCount() -> i32 {
    TOTAL_MAX_CACHED_SUBXIDS()
}

// ---------------------------------------------------------------------------
// FullTransactionId / TransactionId arithmetic helpers shared by the
// horizon (F3) and visibility (F2/F4) families.
// ---------------------------------------------------------------------------

/// `FullTransactionIdAdvance(&dest)` (`access/transam.h`, used pervasively in
/// procarray.c) — advance a `FullTransactionId` past the next-not-special XID,
/// skipping the special XIDs at epoch boundaries.
pub fn FullTransactionIdAdvance(dest: &mut FullTransactionId) {
    dest.value += 1;

    // In contrast to 32bit XIDs we don't step over the "actual" special xids.
    // For 64bit xids these can't be reached as part of a wraparound as they
    // can in the 32bit case.
    if FullTransactionIdPrecedes(*dest, FirstNormalFullTransactionId) {
        return;
    }

    // But we do need to step over XIDs that'd appear special only for 32bit
    // XIDs.
    while dest.xid() < FirstNormalTransactionId {
        dest.value += 1;
    }
}

/// `FullTransactionIdPrecedes(a, b)` (`access/transam.h`) — `(a).value <
/// (b).value`. 64-bit FullTransactionIds never wrap, so this is a plain
/// comparison over the fully-owned [`FullTransactionId`] value.
#[inline]
fn FullTransactionIdPrecedes(a: FullTransactionId, b: FullTransactionId) -> bool {
    a.value < b.value
}

/// `FullTransactionIdFollows(a, b)` (`access/transam.h`) — `(a).value >
/// (b).value`.
#[inline]
fn FullTransactionIdFollows(a: FullTransactionId, b: FullTransactionId) -> bool {
    a.value > b.value
}

/// `TransactionIdOlder(a, b)` (procarray.c static helper) — the older
/// (numerically-smaller-in-xid-order) of two `TransactionId`s, treating
/// `InvalidTransactionId` as "no constraint".
pub fn TransactionIdOlder(a: TransactionId, b: TransactionId) -> TransactionId {
    use backend_access_transam_transam_seams as transam;

    if !TransactionIdIsValid(a) {
        return b;
    }
    if !TransactionIdIsValid(b) {
        return a;
    }
    if transam::transaction_id_precedes::call(a, b) {
        return a;
    }
    b
}

/// `FullTransactionIdNewer(a, b)` (procarray.c static helper) — the newer of
/// two `FullTransactionId`s, treating an invalid value as "no constraint".
pub fn FullTransactionIdNewer(
    a: FullTransactionId,
    b: FullTransactionId,
) -> FullTransactionId {
    if !a.is_valid() {
        return b;
    }
    if !b.is_valid() {
        return a;
    }
    if FullTransactionIdFollows(a, b) {
        return a;
    }
    b
}

/// `FullXidRelativeTo(rel, xid)` (procarray.c) — promote a 32-bit
/// `TransactionId` to a `FullTransactionId` relative to the `rel`
/// `FullTransactionId` epoch (handles wraparound around `rel`).
pub fn FullXidRelativeTo(rel: FullTransactionId, xid: TransactionId) -> FullTransactionId {
    let rel_xid: TransactionId = rel.xid();

    debug_assert!(TransactionIdIsValid(xid));
    debug_assert!(TransactionIdIsValid(rel_xid));

    // `FullTransactionIdFromU64(U64FromFullTransactionId(rel)
    //                           + (int32) (xid - rel_xid))`.
    // The 32-bit difference is taken modulo 2^32 then reinterpreted as a
    // signed `i32` and sign-extended into the 64-bit add, mirroring the C
    // `(int32)` cast.
    let delta = xid.wrapping_sub(rel_xid) as i32;
    FullTransactionId::from_u64(rel.value.wrapping_add(delta as i64 as u64))
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
