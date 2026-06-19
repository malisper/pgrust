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
/// ProcArray header. This is the genuinely-shared cluster state: it is carved
/// from the main `MAP_SHARED` segment by `ProcArrayShmemInit` (via
/// `ShmemInitStruct`) and every backend's `procArray` process-local points at
/// the same physical struct, so a mutation by one backend (e.g.
/// `ProcArrayEndTransaction` clearing its xid, `ProcArrayAdd` appending its
/// slot) is immediately visible to every other backend. All accesses happen
/// under `ProcArrayLock`, matching C.
///
/// The header is `#[repr(C)]` so its layout in shmem is stable across
/// processes; the C `pgprocnos[FLEXIBLE_ARRAY_MEMBER]` trailing array lives in
/// the same shmem block immediately after the header (sized to
/// `PROCARRAY_MAXPROCS`), reached through [`ProcArrayStruct::pgprocnos`] /
/// [`ProcArrayStruct::pgprocnos_mut`] rather than an owned `Vec` (a `Vec`'s
/// backing buffer would be process-private heap, defeating cross-backend
/// visibility).
#[repr(C)]
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

    /// `pgprocnos[FLEXIBLE_ARRAY_MEMBER]` — the flexible trailing array of
    /// indexes into `ProcGlobal->allProcs[]`. Zero-length marker; the real
    /// `maxProcs` `i32` slots follow this header in the same shmem block and are
    /// reached via [`ProcArrayStruct::pgprocnos`] / [`pgprocnos_mut`].
    pub pgprocnos: [i32; 0],
}

impl ProcArrayStruct {
    /// `&procArray->pgprocnos[0 .. maxProcs]` — the flexible trailing array in
    /// the same shmem block as the header.
    #[inline]
    pub fn pgprocnos(&self) -> &[i32] {
        // SAFETY: `ProcArrayShmemInit` carved `header + maxProcs * i32` bytes
        // and the flexible member begins right after the `#[repr(C)]` header at
        // the `pgprocnos` field offset. Reads of `maxProcs` slots stay in the
        // allocation.
        unsafe { core::slice::from_raw_parts(self.pgprocnos.as_ptr(), self.maxProcs as usize) }
    }

    /// `&mut procArray->pgprocnos[0 .. maxProcs]`.
    #[inline]
    pub fn pgprocnos_mut(&mut self) -> &mut [i32] {
        // SAFETY: see `pgprocnos`; `&mut self` here is the per-process view of
        // the shared block, and all callers hold `ProcArrayLock` exclusively.
        unsafe {
            core::slice::from_raw_parts_mut(self.pgprocnos.as_mut_ptr(), self.maxProcs as usize)
        }
    }
}

/// Per-process pointer into the shared `ProcArrayStruct` (the realization of
/// C's `static ProcArrayStruct *procArray`). `Deref`/`DerefMut` give field
/// access on the shared struct so the existing
/// `procArray->numProcs` / `procArray->pgprocnos[i]` call sites read and write
/// the one physical struct in shmem.
#[derive(Clone, Copy, Debug)]
pub struct ProcArrayPtr(*mut ProcArrayStruct);

// SAFETY: the pointer addresses the cluster-wide shmem ProcArray, valid for the
// process lifetime; all field access is serialized by `ProcArrayLock` exactly
// as in C. The thread_local that holds it is per-backend.
unsafe impl Send for ProcArrayPtr {}

impl core::ops::Deref for ProcArrayPtr {
    type Target = ProcArrayStruct;
    #[inline]
    fn deref(&self) -> &ProcArrayStruct {
        // SAFETY: `self.0` points at the shared ProcArray header in shmem.
        unsafe { &*self.0 }
    }
}

impl core::ops::DerefMut for ProcArrayPtr {
    #[inline]
    fn deref_mut(&mut self) -> &mut ProcArrayStruct {
        // SAFETY: as above; callers hold `ProcArrayLock` for the mutation.
        unsafe { &mut *self.0 }
    }
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
    /// shared ProcArray header in the `MAP_SHARED` segment (set by
    /// `ProcArrayShmemInit`). The pointer is per-process (correctly forked) but
    /// the struct it addresses is shared, so mutations are visible to every
    /// backend.
    pub static PROC_ARRAY: RefCell<Option<ProcArrayPtr>> = const { RefCell::new(None) };

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
/// `TransactionId` words = 36 on the C layout). Taken from the `#[repr(C)]`
/// [`ProcArrayStruct`] directly so the shmem allocation matches the struct the
/// per-process pointer derefs.
const PROC_ARRAY_HEADER_SIZE: Size = core::mem::offset_of!(ProcArrayStruct, pgprocnos) as Size;

/// `ProcArrayShmemInit(void)` (procarray.c) — allocate-or-attach the ProcArray
/// header (`ShmemInitStruct`) and the KnownAssignedXids ring; wire the
/// `procArray` process-local.
pub fn ProcArrayShmemInit() -> PgResult<()> {
    use backend_storage_ipc_shmem_seams as shmem;

    let maxprocs = PROCARRAY_MAXPROCS();
    let total_max_cached_subxids = TOTAL_MAX_CACHED_SUBXIDS();

    // Create or attach to the ProcArray shared structure. C:
    //   procArray = (ProcArrayStruct *)
    //     ShmemInitStruct("Proc Array",
    //                     add_size(offsetof(ProcArrayStruct, pgprocnos),
    //                              mul_size(sizeof(int), PROCARRAY_MAXPROCS)),
    //                     &found);
    // The header + flexible `pgprocnos[PROCARRAY_MAXPROCS]` array are one block
    // in the main shared-memory segment; `procArray` is the per-process pointer
    // at the single physical struct, so all backends mutate the same array.
    let header_size = shmem::add_size::call(
        PROC_ARRAY_HEADER_SIZE,
        shmem::mul_size::call(
            core::mem::size_of::<i32>() as Size,
            maxprocs as Size,
        )?,
    )?;
    let (addr, found) = shmem::shmem_init_struct::call("Proc Array", header_size)?;
    let proc_array = addr as *mut ProcArrayStruct;

    if !found {
        // We're the first - initialize the shared struct in place.
        // SAFETY: `proc_array` addresses `header_size` writable shmem bytes
        // (header + `maxprocs` `i32` slots) just carved by `ShmemInitStruct`.
        unsafe {
            (*proc_array).numProcs = 0;
            (*proc_array).maxProcs = maxprocs;
            (*proc_array).maxKnownAssignedXids = total_max_cached_subxids;
            (*proc_array).numKnownAssignedXids = 0;
            (*proc_array).tailKnownAssignedXids = 0;
            (*proc_array).headKnownAssignedXids = 0;
            (*proc_array).lastOverflowedXid = types_core::InvalidTransactionId;
            (*proc_array).replication_slot_xmin = types_core::InvalidTransactionId;
            (*proc_array).replication_slot_catalog_xmin = types_core::InvalidTransactionId;
            // Zero the flexible `pgprocnos[]` region.
            core::ptr::write_bytes(
                (*proc_array).pgprocnos.as_mut_ptr(),
                0,
                maxprocs as usize,
            );
        }
        backend_access_transam_varsup_seams::init_xact_completion_count::call();
    }

    // Record this backend's per-process pointer at the shared struct (C's
    // `procArray = ...` assignment runs in every process, allocator or
    // attacher).
    PROC_ARRAY.with(|p| *p.borrow_mut() = Some(ProcArrayPtr(proc_array)));

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

#[cfg(test)]
mod tests {
    use super::*;

    /// The header offset the C `offsetof(ProcArrayStruct, pgprocnos)` reports
    /// (six `int` + three `TransactionId` = 36 on PG's 32-bit-xid layout).
    #[test]
    fn header_offset_matches_c() {
        assert_eq!(PROC_ARRAY_HEADER_SIZE, 36);
    }

    /// Cross-backend visibility: two `ProcArrayPtr`s (conn1's and conn2's
    /// per-process pointers) over the SAME backing block — the realization of
    /// fork sharing one `MAP_SHARED` `ShmemInitStruct` region — must see each
    /// other's mutations. This is exactly the bug: before this change the body
    /// lived in a per-process `Vec`, so conn1's `ProcArrayAdd`/end-of-xact
    /// updates were invisible to conn2's `GetSnapshotData`.
    #[test]
    fn two_pointers_share_one_struct() {
        let maxprocs = 8usize;
        // One shared block: header + flexible pgprocnos[maxprocs], as
        // ShmemInitStruct carves it. A heap `Vec<u8>` stands in for the shmem
        // bytes for this single-process test (a real fork would share the same
        // physical page).
        let block_size = PROC_ARRAY_HEADER_SIZE as usize + maxprocs * core::mem::size_of::<i32>();
        let mut block: Vec<u8> = vec![0u8; block_size + 8];
        // Max-align the base the way ShmemInitStruct does.
        let base = {
            let raw = block.as_mut_ptr() as usize;
            let aligned = (raw + 7) & !7usize;
            aligned as *mut ProcArrayStruct
        };

        // conn1 and conn2: distinct per-process pointers at the SAME struct.
        let mut conn1 = ProcArrayPtr(base);
        let conn2 = ProcArrayPtr(base);

        // First backend initializes the shared header in place.
        conn1.numProcs = 0;
        conn1.maxProcs = maxprocs as i32;
        conn1.lastOverflowedXid = types_core::InvalidTransactionId;
        for slot in conn1.pgprocnos_mut() {
            *slot = 0;
        }

        // conn1 performs a ProcArrayAdd-shaped mutation: append a slot and
        // advance numProcs (mirrors membership.rs writing the shared array).
        conn1.pgprocnos_mut()[0] = 42;
        conn1.numProcs = 1;
        conn1.lastOverflowedXid = 12345;

        // conn2 (a different per-process view) observes conn1's writes — the
        // cross-connection visibility the fix delivers.
        assert_eq!(conn2.numProcs, 1);
        assert_eq!(conn2.maxProcs, maxprocs as i32);
        assert_eq!(conn2.pgprocnos()[0], 42);
        assert_eq!(conn2.lastOverflowedXid, 12345);
    }
}
