// SSI predicate-locking engine (storage/lmgr/predicate.c) over the process-
// global shmem/dynahash/LWLock substrate — same adaptation as lock/shared.rs:
// one address space, structures behind a OnceLock, C's LWLock discipline kept.
//
// LOUD (not ported): DEFERRABLE safe snapshots (GetSafeSnapshot), snapshot
// import (SetSerializableTransactionSnapshot), parallel-query sharing, 2PC
// lock transfer, SLRU summarization (SummarizeOldestCommittedSxact/SerialAdd).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::missing_safety_doc)]

use core::cell::Cell;
use core::ptr;
use std::sync::OnceLock;

use dynahash::{
    get_hash_value, hash_create, hash_destroy, hash_estimate_size, hash_search,
    hash_search_with_hash_value, hash_seq_init, hash_seq_search,
};
use lwlock::{
    main_lock, LWLock, LWLockAcquire, LWLockHeldByMe, LWLockHeldByMeInMode, LWLockInitialize,
    LWLockRelease, LW_EXCLUSIVE, LW_SHARED,
};
use types_core::{
    BlockNumber, InvalidBlockNumber, InvalidTransactionId, OffsetNumber, Oid, ProcNumber, Size,
    TransactionId, TransactionIdEquals, TransactionIdFollows, TransactionIdFollowsOrEquals,
    TransactionIdIsValid, TransactionIdPrecedes, TransactionIdPrecedesOrEquals,
    VirtualTransactionId,
};
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_OUT_OF_MEMORY,
    ERRCODE_T_R_SERIALIZATION_FAILURE,
};
use types_hash::hsearch::{
    HASHCTL, HASH_BLOBS, HASH_ELEM, HASH_ENTER, HASH_ENTER_NULL, HASH_FIND, HASH_FIXED_SIZE,
    HASH_FUNCTION, HASH_PARTITION, HASH_REMOVE, HASH_SEQ_STATUS, HASH_SHARED_MEM, HTAB,
};
use types_snapshot::{IsMVCCSnapshot, SnapshotData};
use types_storage::storage::{
    LOG2_NUM_PREDICATELOCK_PARTITIONS, NUM_PREDICATELOCK_PARTITIONS,
    PREDICATELOCK_MANAGER_LWLOCK_OFFSET, SERIALIZABLE_FINISHED_LIST_LOCK,
    SERIALIZABLE_PREDICATE_LIST_LOCK, SERIALIZABLE_XACT_HASH_LOCK, SERIAL_CONTROL_LOCK,
};
use types_storage::LWTRANCHE_PER_XACT_PREDICATE_LIST;

use crate::ilist::*;
use crate::internals::*;
use crate::serial::{SerialGetMinConflictCommitSeqNo, SerialInit, SerialSetActiveSerXmin};

thread_local! {
    static MY_SERIALIZABLE_XACT: Cell<*mut SERIALIZABLEXACT> = const { Cell::new(ptr::null_mut()) };
    // C's SavedSerializableXact: a leader-stashed partially-released RO_SAFE
    // sxact awaiting end-of-transaction cleanup (workers may still hold it).
    static SAVED_SERIALIZABLE_XACT: Cell<*mut SERIALIZABLEXACT> = const { Cell::new(ptr::null_mut()) };
    static MY_XACT_DID_WRITE: Cell<bool> = const { Cell::new(false) };
    static LOCAL_PREDICATE_LOCK_HASH: Cell<*mut HTAB> = const { Cell::new(ptr::null_mut()) };

    static MAX_PREDICATE_LOCKS_PER_XACT: Cell<i32> = const { Cell::new(64) };
    static MAX_PREDICATE_LOCKS_PER_RELATION: Cell<i32> = const { Cell::new(-2) };
    static MAX_PREDICATE_LOCKS_PER_PAGE: Cell<i32> = const { Cell::new(2) };
}

pub fn max_predicate_locks_per_xact() -> i32 {
    MAX_PREDICATE_LOCKS_PER_XACT.with(|c| c.get())
}
pub fn set_max_predicate_locks_per_xact(v: i32) {
    MAX_PREDICATE_LOCKS_PER_XACT.with(|c| c.set(v));
}
pub fn max_predicate_locks_per_relation() -> i32 {
    MAX_PREDICATE_LOCKS_PER_RELATION.with(|c| c.get())
}
pub fn set_max_predicate_locks_per_relation(v: i32) {
    MAX_PREDICATE_LOCKS_PER_RELATION.with(|c| c.set(v));
}
pub fn max_predicate_locks_per_page() -> i32 {
    MAX_PREDICATE_LOCKS_PER_PAGE.with(|c| c.get())
}
pub fn set_max_predicate_locks_per_page(v: i32) {
    MAX_PREDICATE_LOCKS_PER_PAGE.with(|c| c.set(v));
}
static SCRATCH_TARGET_TAG: PREDICATELOCKTARGETTAG = ZERO_TARGET_TAG;

struct Shared {
    target_hash: *mut HTAB,
    lock_hash: *mut HTAB,
    xid_hash: *mut HTAB,
    pred_xact: PredXactList,
    rw_pool: RWConflictPoolHeader,
    finished: *mut dlist_head,
    old_committed_sxact: *mut SERIALIZABLEXACT,
    scratch_target_tag_hash: u32,
    sxact_slots: i64,
    rw_conflict_slots: i64,
    max_prepared_xacts: i32,
}

// SAFETY: mutation follows C's lock protocol — PredXact/xid_hash under
// SerializableXactHashLock, target/lock hashes under the partition locks +
// SerializablePredicateListLock, finished list under
// SerializableFinishedListLock; dynahash entry alloc uses its freelist locks.
unsafe impl Send for Shared {}
unsafe impl Sync for Shared {}

static SHARED: OnceLock<Shared> = OnceLock::new();

fn shared() -> &'static Shared {
    SHARED
        .get()
        .unwrap_or_else(|| panic!("predicate lock manager shmem not initialized"))
}

#[inline]
fn MySerializableXact() -> *mut SERIALIZABLEXACT {
    MY_SERIALIZABLE_XACT.with(|c| c.get())
}
#[inline]
fn set_MySerializableXact(v: *mut SERIALIZABLEXACT) {
    MY_SERIALIZABLE_XACT.with(|c| c.set(v));
}
#[inline]
fn MyXactDidWrite() -> bool {
    MY_XACT_DID_WRITE.with(|c| c.get())
}
#[inline]
fn set_MyXactDidWrite(v: bool) {
    MY_XACT_DID_WRITE.with(|c| c.set(v));
}
#[inline]
fn LocalPredicateLockHash() -> *mut HTAB {
    LOCAL_PREDICATE_LOCK_HASH.with(|c| c.get())
}

pub(crate) fn my_procno() -> ProcNumber {
    lmgr_proc::MyProc().expect("predicate lock manager entered without a PGPROC")
}

fn my_proc_vxid() -> VirtualTransactionId {
    let proc = lmgr_proc::GetPGProcByNumber(my_procno());
    VirtualTransactionId {
        procNumber: proc.vxid.procNumber.load(std::sync::atomic::Ordering::Relaxed),
        localTransactionId: proc.vxid.lxid.load(std::sync::atomic::Ordering::Relaxed),
    }
}

pub(crate) fn recovery_in_progress() -> bool {
    transam_xlog_seams::recovery_in_progress::call()
}

fn in_parallel_mode() -> bool {
    xact_seams::is_in_parallel_mode::call()
}

fn is_parallel_worker() -> bool {
    parallel_seams::is_parallel_worker::call()
}

#[inline]
fn SerializableXactHashLock() -> &'static LWLock {
    main_lock(SERIALIZABLE_XACT_HASH_LOCK as usize)
}
#[inline]
fn SerializableFinishedListLock() -> &'static LWLock {
    main_lock(SERIALIZABLE_FINISHED_LIST_LOCK as usize)
}
#[inline]
fn SerializablePredicateListLock() -> &'static LWLock {
    main_lock(SERIALIZABLE_PREDICATE_LIST_LOCK as usize)
}
#[inline]
pub(crate) fn SerialControlLock() -> &'static LWLock {
    main_lock(SERIAL_CONTROL_LOCK as usize)
}
#[inline]
fn PredicateLockHashPartitionLock(hashcode: u32) -> &'static LWLock {
    main_lock(
        PREDICATELOCK_MANAGER_LWLOCK_OFFSET as usize
            + (hashcode as usize % NUM_PREDICATELOCK_PARTITIONS as usize),
    )
}
#[inline]
fn PredicateLockHashPartitionLockByIndex(i: i32) -> &'static LWLock {
    main_lock(PREDICATELOCK_MANAGER_LWLOCK_OFFSET as usize + i as usize)
}
#[inline]
fn ScratchPartitionLock() -> &'static LWLock {
    PredicateLockHashPartitionLock(shared().scratch_target_tag_hash)
}

#[inline]
unsafe fn PredicateLockTargetTagHashCode(tag: *const PREDICATELOCKTARGETTAG) -> u32 {
    get_hash_value(shared().target_hash, tag as *const u8)
}

#[inline]
unsafe fn PredicateLockHashCodeFromTargetHashCode(
    predicatelocktag: *const PREDICATELOCKTAG,
    targethash: u32,
) -> u32 {
    targethash
        ^ (((*predicatelocktag).myXact as usize as u32) << LOG2_NUM_PREDICATELOCK_PARTITIONS)
}

fn NPREDICATELOCKTARGETENTS(max_prepared_xacts: i32) -> i64 {
    max_predicate_locks_per_xact() as i64
        * (init_small::globals::MaxBackends() as i64 + max_prepared_xacts as i64)
}

#[track_caller]
#[cold]
fn out_of_shared_memory() -> Box<PgError> {
    Box::new(
        PgError::error("out of shared memory")
            .with_sqlstate(ERRCODE_OUT_OF_MEMORY)
            .with_hint("You might need to increase \"max_pred_locks_per_transaction\"."),
    )
}

#[track_caller]
#[cold]
fn serialization_failure(reason: &str) -> Box<PgError> {
    Box::new(
        PgError::error(
            "could not serialize access due to read/write dependencies among transactions",
        )
        .with_sqlstate(ERRCODE_T_R_SERIALIZATION_FAILURE)
        .with_detail(format!("Reason code: {reason}"))
        .with_hint("The transaction might succeed if retried."),
    )
}

#[inline]
fn predicate_locking_needed(rd_id: Oid, uses_local_buffers: bool) -> bool {
    !(rd_id < types_core::catalog::FirstUnpinnedObjectId || uses_local_buffers)
}

unsafe fn SerializationNeededForRead(
    rd_id: Oid,
    uses_local_buffers: bool,
    snapshot: &SnapshotData<'_>,
) -> PgResult<bool> {
    if MySerializableXact() == InvalidSerializableXact {
        return Ok(false);
    }
    if !IsMVCCSnapshot(snapshot) {
        return Ok(false);
    }
    if SxactIsROSafe(MySerializableXact()) {
        ReleasePredicateLocks(false, true)?;
        return Ok(false);
    }
    if !predicate_locking_needed(rd_id, uses_local_buffers) {
        return Ok(false);
    }
    Ok(true)
}

pub fn SerializableXactActive() -> bool {
    MySerializableXact() != InvalidSerializableXact
}

unsafe fn SerializationNeededForWrite(rd_id: Oid, uses_local_buffers: bool) -> bool {
    if MySerializableXact() == InvalidSerializableXact {
        return false;
    }
    if !predicate_locking_needed(rd_id, uses_local_buffers) {
        return false;
    }
    true
}

unsafe fn CreatePredXact() -> *mut SERIALIZABLEXACT {
    CreatePredXactIn(shared().pred_xact)
}

unsafe fn CreatePredXactIn(px: PredXactList) -> *mut SERIALIZABLEXACT {
    if dlist_is_empty(&raw mut (*px).availableList) {
        return ptr::null_mut();
    }
    let node = dlist_pop_head_node(&raw mut (*px).availableList);
    let sxact = dlist_container!(SERIALIZABLEXACT, xactLink, node);
    dlist_push_tail(&raw mut (*px).activeList, &raw mut (*sxact).xactLink);
    sxact
}

unsafe fn ReleasePredXact(sxact: *mut SERIALIZABLEXACT) {
    dlist_delete(&raw mut (*sxact).xactLink);
    dlist_push_tail(
        &raw mut (*shared().pred_xact).availableList,
        &raw mut (*sxact).xactLink,
    );
}

unsafe fn RWConflictExists(
    reader: *const SERIALIZABLEXACT,
    writer: *const SERIALIZABLEXACT,
) -> bool {
    debug_assert!(reader != writer);

    if SxactIsDoomed(reader)
        || SxactIsDoomed(writer)
        || dlist_is_empty(&raw const (*reader).outConflicts)
        || dlist_is_empty(&raw const (*writer).inConflicts)
    {
        return false;
    }

    let head = &raw const (*reader).outConflicts;
    let mut cur = (*head).head.next;
    while cur != (&raw const (*head).head) as *mut dlist_node {
        let conflict = dlist_container!(RWConflictData, outLink, cur);
        if (*conflict).sxactIn == writer as *mut SERIALIZABLEXACT {
            return true;
        }
        cur = (*cur).next;
    }
    false
}

#[track_caller]
#[cold]
fn rw_conflict_pool_exhausted(potential: bool) -> Box<PgError> {
    let msg = if potential {
        "not enough elements in RWConflictPool to record a potential read/write conflict"
    } else {
        "not enough elements in RWConflictPool to record a read/write conflict"
    };
    Box::new(
        PgError::error(msg)
            .with_sqlstate(ERRCODE_OUT_OF_MEMORY)
            .with_hint(
                "You might need to run fewer transactions at a time or increase \"max_connections\".",
            ),
    )
}

unsafe fn SetRWConflict(
    reader: *mut SERIALIZABLEXACT,
    writer: *mut SERIALIZABLEXACT,
) -> PgResult<()> {
    debug_assert!(reader != writer);
    debug_assert!(!RWConflictExists(reader, writer));

    let pool = shared().rw_pool;
    if dlist_is_empty(&raw const (*pool).availableList) {
        return Err(rw_conflict_pool_exhausted(false));
    }

    let conflict = dlist_container!(RWConflictData, outLink, (*pool).availableList.head.next);
    dlist_delete(&raw mut (*conflict).outLink);

    (*conflict).sxactOut = reader;
    (*conflict).sxactIn = writer;
    dlist_push_tail(&raw mut (*reader).outConflicts, &raw mut (*conflict).outLink);
    dlist_push_tail(&raw mut (*writer).inConflicts, &raw mut (*conflict).inLink);
    Ok(())
}

unsafe fn SetPossibleUnsafeConflict(
    roXact: *mut SERIALIZABLEXACT,
    activeXact: *mut SERIALIZABLEXACT,
) -> PgResult<()> {
    debug_assert!(roXact != activeXact);
    debug_assert!(SxactIsReadOnly(roXact));
    debug_assert!(!SxactIsReadOnly(activeXact));

    let pool = shared().rw_pool;
    if dlist_is_empty(&raw const (*pool).availableList) {
        return Err(rw_conflict_pool_exhausted(true));
    }

    let conflict = dlist_container!(RWConflictData, outLink, (*pool).availableList.head.next);
    dlist_delete(&raw mut (*conflict).outLink);

    (*conflict).sxactOut = activeXact;
    (*conflict).sxactIn = roXact;
    dlist_push_tail(
        &raw mut (*activeXact).possibleUnsafeConflicts,
        &raw mut (*conflict).outLink,
    );
    dlist_push_tail(
        &raw mut (*roXact).possibleUnsafeConflicts,
        &raw mut (*conflict).inLink,
    );
    Ok(())
}

unsafe fn ReleaseRWConflict(conflict: RWConflict) {
    dlist_delete(&raw mut (*conflict).inLink);
    dlist_delete(&raw mut (*conflict).outLink);
    dlist_push_tail(
        &raw mut (*shared().rw_pool).availableList,
        &raw mut (*conflict).outLink,
    );
}

unsafe fn FlagSxactUnsafe(sxact: *mut SERIALIZABLEXACT) {
    debug_assert!(SxactIsReadOnly(sxact));
    debug_assert!(!SxactIsROSafe(sxact));

    (*sxact).flags |= SXACT_FLAG_RO_UNSAFE;

    let head = &raw mut (*sxact).possibleUnsafeConflicts;
    let mut cur = (*head).head.next;
    while cur != (&raw mut (*head).head) as *mut dlist_node {
        let next = (*cur).next;
        let conflict = dlist_container!(RWConflictData, inLink, cur);
        debug_assert!(!SxactIsReadOnly((*conflict).sxactOut));
        debug_assert!(sxact == (*conflict).sxactIn);
        ReleaseRWConflict(conflict);
        cur = next;
    }
}

fn predicatelock_hash(key: &[u8], _keysize: Size) -> u32 {
    // SAFETY: key is a PREDICATELOCKTAG; myTarget points at a live target
    // entry whose tag is immutable for the entry's lifetime.
    unsafe {
        let predicatelocktag = key.as_ptr() as *const PREDICATELOCKTAG;
        let targethash =
            PredicateLockTargetTagHashCode(&raw const (*(*predicatelocktag).myTarget).tag);
        PredicateLockHashCodeFromTargetHashCode(predicatelocktag, targethash)
    }
}

unsafe fn init_pred_xact_list(px: PredXactList, elem_count: i64, first_boot: bool) {
    dlist_init(&raw mut (*px).availableList);
    dlist_init(&raw mut (*px).activeList);
    (*px).SxactGlobalXmin = InvalidTransactionId;
    (*px).SxactGlobalXminCount = 0;
    (*px).WritableSxactCount = 0;
    (*px).LastSxactCommitSeqNo = FirstNormalSerCommitSeqNo - 1;
    (*px).CanPartialClearThrough = 0;
    (*px).HavePartialClearedThrough = 0;
    for i in 0..elem_count {
        let e = (*px).element.add(i as usize);
        if first_boot {
            LWLockInitialize(
                &mut (*e).perXactPredicateListLock,
                LWTRANCHE_PER_XACT_PREDICATE_LIST,
            );
        }
        dlist_push_tail(&raw mut (*px).availableList, &raw mut (*e).xactLink);
    }
    let oc = CreatePredXactIn(px);
    (*px).OldCommittedSxact = oc;
    (*oc).vxid = VirtualTransactionId::invalid();
    (*oc).prepareSeqNo = 0;
    (*oc).commitSeqNo = 0;
    (*oc).SeqNo.lastCommitBeforeSnapshot = 0;
    dlist_init(&raw mut (*oc).outConflicts);
    dlist_init(&raw mut (*oc).inConflicts);
    dlist_init(&raw mut (*oc).predicateLocks);
    dlist_node_init(&raw mut (*oc).finishedLink);
    dlist_init(&raw mut (*oc).possibleUnsafeConflicts);
    (*oc).topXid = InvalidTransactionId;
    (*oc).finishedBefore = InvalidTransactionId;
    (*oc).xmin = InvalidTransactionId;
    (*oc).flags = SXACT_FLAG_COMMITTED;
    (*oc).pid = 0;
    (*oc).pgprocno = types_core::INVALID_PROC_NUMBER;
}

pub fn PredicateLockShmemInit(max_prepared_xacts: i32) -> PgResult<()> {
    if SHARED.get().is_some() {
        return Ok(());
    }
    unsafe {
        let max_table_size_targets = NPREDICATELOCKTARGETENTS(max_prepared_xacts);

        let mut info = HASHCTL::new();
        info.keysize = size_of::<PREDICATELOCKTARGETTAG>();
        info.entrysize = size_of::<PREDICATELOCKTARGET>();
        info.num_partitions = NUM_PREDICATELOCK_PARTITIONS as i64;
        let target_hash = hash_create(
            "PREDICATELOCKTARGET hash",
            max_table_size_targets,
            &info,
            HASH_ELEM | HASH_BLOBS | HASH_PARTITION | HASH_FIXED_SIZE | HASH_SHARED_MEM,
        )?;

        // Reserve the scratch (dummy) entry lock-transfer relies on.
        let mut found = false;
        let _ = hash_search(
            target_hash,
            &raw const SCRATCH_TARGET_TAG as *const u8,
            HASH_ENTER,
            Some(&mut found),
        )?;
        debug_assert!(!found);
        let scratch_hash = get_hash_value(target_hash, &raw const SCRATCH_TARGET_TAG as *const u8);

        let mut info = HASHCTL::new();
        info.keysize = size_of::<PREDICATELOCKTAG>();
        info.entrysize = size_of::<PREDICATELOCK>();
        info.hash = Some(predicatelock_hash);
        info.num_partitions = NUM_PREDICATELOCK_PARTITIONS as i64;
        let lock_hash = hash_create(
            "PREDICATELOCK hash",
            max_table_size_targets * 2,
            &info,
            HASH_ELEM | HASH_FUNCTION | HASH_PARTITION | HASH_FIXED_SIZE | HASH_SHARED_MEM,
        )?;

        let xact_count = (init_small::globals::MaxBackends() + max_prepared_xacts) as i64;
        let elem_count = xact_count * 10;
        let request_size =
            PredXactListDataSize() + elem_count as usize * size_of::<SERIALIZABLEXACT>();
        let (px_ptr, found) = shmem::ShmemInitStruct("PredXactList", request_size)?;
        debug_assert!(!found);
        ptr::write_bytes(px_ptr, 0, request_size);
        let px = px_ptr as PredXactList;
        (*px).element = px_ptr.add(PredXactListDataSize()) as *mut SERIALIZABLEXACT;
        init_pred_xact_list(px, elem_count, true);

        let mut info = HASHCTL::new();
        info.keysize = size_of::<SERIALIZABLEXIDTAG>();
        info.entrysize = size_of::<SERIALIZABLEXID>();
        let xid_hash = hash_create(
            "SERIALIZABLEXID hash",
            xact_count,
            &info,
            HASH_ELEM | HASH_BLOBS | HASH_FIXED_SIZE | HASH_SHARED_MEM,
        )?;

        let conflict_count = elem_count * 5;
        let request_size =
            RWConflictPoolHeaderDataSize() + conflict_count as usize * RWConflictDataSize();
        let (rw_ptr, found) = shmem::ShmemInitStruct("RWConflictPool", request_size)?;
        debug_assert!(!found);
        ptr::write_bytes(rw_ptr, 0, request_size);
        let rw = rw_ptr as RWConflictPoolHeader;
        dlist_init(&raw mut (*rw).availableList);
        (*rw).element = rw_ptr.add(RWConflictPoolHeaderDataSize()) as RWConflict;
        for i in 0..conflict_count {
            let e = (*rw).element.add(i as usize);
            dlist_push_tail(&raw mut (*rw).availableList, &raw mut (*e).outLink);
        }

        let (f_ptr, found) =
            shmem::ShmemInitStruct("FinishedSerializableTransactions", size_of::<dlist_head>())?;
        debug_assert!(!found);
        let finished = f_ptr as *mut dlist_head;
        dlist_init(finished);

        SerialInit()?;

        let _ = SHARED.set(Shared {
            target_hash,
            lock_hash,
            xid_hash,
            pred_xact: px,
            rw_pool: rw,
            finished,
            old_committed_sxact: (*px).OldCommittedSxact,
            scratch_target_tag_hash: scratch_hash,
            sxact_slots: elem_count,
            rw_conflict_slots: conflict_count,
            max_prepared_xacts,
        });
    }
    Ok(())
}

pub fn PredicateLockShmemSize(max_prepared_xacts: i32) -> Size {
    let mut max_table_size = NPREDICATELOCKTARGETENTS(max_prepared_xacts);
    let mut size = hash_estimate_size(max_table_size, size_of::<PREDICATELOCKTARGET>());
    max_table_size *= 2;
    size += hash_estimate_size(max_table_size, size_of::<PREDICATELOCK>());
    size += size / 10;

    let mut max_table_size =
        (init_small::globals::MaxBackends() + max_prepared_xacts) as i64 * 10;
    size += PredXactListDataSize();
    size += max_table_size as usize * size_of::<SERIALIZABLEXACT>();
    size += hash_estimate_size(max_table_size, size_of::<SERIALIZABLEXID>());
    max_table_size *= 5;
    size += RWConflictPoolHeaderDataSize();
    size += max_table_size as usize * RWConflictDataSize();
    size += size_of::<dlist_head>();
    // C adds SerialControlData + the pg_serial SLRU; that store is a loud
    // panic here (serial.rs), so its bytes are not requested.
    size
}

/// Crash-cycle reset in place; sizes are PGC_POSTMASTER-stable.
pub fn PredicateLockShmemResetAfterCrash() {
    let Some(s) = SHARED.get() else { return };
    // SAFETY: crash choreography drained every child; the postmaster thread
    // has exclusive access.
    unsafe {
        dynahash::hash_reset_after_crash(s.target_hash);
        dynahash::hash_reset_after_crash(s.lock_hash);
        dynahash::hash_reset_after_crash(s.xid_hash);
        let mut found = false;
        let _ = hash_search(
            s.target_hash,
            &raw const SCRATCH_TARGET_TAG as *const u8,
            HASH_ENTER,
            Some(&mut found),
        );
        debug_assert!(!found);

        init_pred_xact_list(s.pred_xact, s.sxact_slots, false);

        let rw = s.rw_pool;
        dlist_init(&raw mut (*rw).availableList);
        for i in 0..s.rw_conflict_slots {
            let e = (*rw).element.add(i as usize);
            dlist_push_tail(&raw mut (*rw).availableList, &raw mut (*e).outLink);
        }
        dlist_init(s.finished);
    }
    crate::serial::SerialResetAfterCrash();
}

// ===========================================================================
// Snapshot acquisition.
// ===========================================================================

pub fn GetSerializableTransactionSnapshot<'m>(
    snapshot: &mut SnapshotData<'m>,
    mcx: mcx::Mcx<'m>,
) -> PgResult<()> {
    debug_assert!(xact_seams::isolation_is_serializable::call());

    if recovery_in_progress() {
        return Err(Box::new(
            PgError::error("cannot use serializable mode in a hot standby")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
                .with_detail("\"default_transaction_isolation\" is set to \"serializable\".")
                .with_hint(
                    "You can use \"SET default_transaction_isolation = 'repeatable read'\" to change the default.",
                ),
        ));
    }

    if xact_seams::xact_read_only::call() && xact_seams::xact_deferrable::call() {
        return GetSafeSnapshot(snapshot, mcx);
    }

    GetSerializableTransactionSnapshotInt(snapshot, mcx)
}

// SetSerializableTransactionSnapshot (predicate.c): in a parallel worker the
// leader's SERIALIZABLEXACT arrives via AttachSerializableXact, so there is
// nothing to do here. The snapshot-import arm (SET TRANSACTION SNAPSHOT,
// GetSerializableTransactionSnapshotInt's sourcevxid path) is unported.
pub fn SetSerializableTransactionSnapshot() -> PgResult<()> {
    debug_assert!(xact_seams::isolation_is_serializable::call());

    if is_parallel_worker() {
        return Ok(());
    }

    if xact_seams::xact_read_only::call() && xact_seams::xact_deferrable::call() {
        return Err(Box::new(
            PgError::error("a snapshot-importing transaction must not be READ ONLY DEFERRABLE")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }

    panic!("predicate.c SetSerializableTransactionSnapshot: snapshot import into a serializable transaction is not ported");
}

pub fn ShareSerializableXact() -> usize {
    MySerializableXact() as usize
}

pub fn AttachSerializableXact(handle: usize) -> PgResult<()> {
    debug_assert!(MySerializableXact() == InvalidSerializableXact);
    set_MySerializableXact(handle as *mut SERIALIZABLEXACT);
    if MySerializableXact() != InvalidSerializableXact {
        CreateLocalPredicateLockHash()?;
    }
    Ok(())
}

// PG_WAIT_IPC | SafeSnapshot's index in wait_event_names.txt's IPC section.
const WAIT_EVENT_SAFE_SNAPSHOT: u32 = 0x0800_0000 | 51;

// GetSafeSnapshot (predicate.c:1558): obtain and register a snapshot for a
// READ ONLY DEFERRABLE transaction, waiting out (and if flagged unsafe,
// retrying past) concurrent read-write serializable transactions.
fn GetSafeSnapshot<'m>(snapshot: &mut SnapshotData<'m>, mcx: mcx::Mcx<'m>) -> PgResult<()> {
    debug_assert!(xact_seams::xact_read_only::call() && xact_seams::xact_deferrable::call());

    loop {
        GetSerializableTransactionSnapshotInt(snapshot, mcx)?;

        if MySerializableXact() == InvalidSerializableXact {
            return Ok(()); // no concurrent r/w xacts; it's safe
        }

        let procno = my_procno();
        LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE, procno)?;

        // SAFETY: MySerializableXact is our own active sxact; flag and list
        // mutations happen under SerializableXactHashLock exclusive.
        unsafe {
            let mysx = MySerializableXact();
            (*mysx).flags |= SXACT_FLAG_DEFERRABLE_WAITING;
            while !(dlist_is_empty(&raw const (*mysx).possibleUnsafeConflicts)
                || SxactIsROUnsafe(mysx))
            {
                LWLockRelease(SerializableXactHashLock())?;
                lmgr_proc::ProcWaitForSignal(WAIT_EVENT_SAFE_SNAPSHOT);
                LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE, procno)?;
            }
            (*mysx).flags &= !SXACT_FLAG_DEFERRABLE_WAITING;

            if !SxactIsROUnsafe(mysx) {
                LWLockRelease(SerializableXactHashLock())?;
                break; // success
            }
        }

        LWLockRelease(SerializableXactHashLock())?;

        // Snapshot was unsafe; release and retry with a new one.
        ReleasePredicateLocks(false, false)?;
    }

    debug_assert!(unsafe { SxactIsROSafe(MySerializableXact()) });
    ReleasePredicateLocks(false, true)?;
    Ok(())
}

fn GetSerializableTransactionSnapshotInt<'m>(
    snapshot: &mut SnapshotData<'m>,
    mcx: mcx::Mcx<'m>,
) -> PgResult<()> {
    unsafe {
        debug_assert!(MySerializableXact() == InvalidSerializableXact);
        debug_assert!(!recovery_in_progress());

        if in_parallel_mode() {
            return Err(Box::new(PgError::error(
                "cannot establish serializable snapshot during a parallel operation",
            )));
        }

        let vxid = my_proc_vxid();
        let procno = my_procno();

        LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE, procno)?;
        let sxact = CreatePredXact();
        if sxact.is_null() {
            // C summarizes the oldest committed sxact into the pg_serial SLRU
            // and retries; that store is not ported.
            LWLockRelease(SerializableXactHashLock())?;
            panic!(
                "predicate.c SummarizeOldestCommittedSxact: SERIALIZABLEXACT slots exhausted \
                 and the pg_serial summarization path is not ported"
            );
        }

        procarray::GetSnapshotData(snapshot, mcx)?;

        let px = shared().pred_xact;
        let read_only = xact_seams::xact_read_only::call();

        if read_only && (*px).WritableSxactCount == 0 {
            ReleasePredXact(sxact);
            LWLockRelease(SerializableXactHashLock())?;
            return Ok(());
        }

        (*sxact).vxid = vxid;
        (*sxact).SeqNo.lastCommitBeforeSnapshot = (*px).LastSxactCommitSeqNo;
        (*sxact).prepareSeqNo = InvalidSerCommitSeqNo;
        (*sxact).commitSeqNo = InvalidSerCommitSeqNo;
        dlist_init(&raw mut (*sxact).outConflicts);
        dlist_init(&raw mut (*sxact).inConflicts);
        dlist_init(&raw mut (*sxact).possibleUnsafeConflicts);
        (*sxact).topXid = xact_seams::get_top_transaction_id_if_any::call();
        (*sxact).finishedBefore = InvalidTransactionId;
        (*sxact).xmin = snapshot.xmin;
        (*sxact).pid = init_small::globals::MyProcPid();
        (*sxact).pgprocno = procno;
        dlist_init(&raw mut (*sxact).predicateLocks);
        dlist_node_init(&raw mut (*sxact).finishedLink);
        (*sxact).flags = 0;

        if read_only {
            (*sxact).flags |= SXACT_FLAG_READ_ONLY;

            let head = &raw const (*px).activeList;
            let mut cur = (*head).head.next;
            while cur != (&raw const (*head).head) as *mut dlist_node {
                let othersxact = dlist_container!(SERIALIZABLEXACT, xactLink, cur);
                if !SxactIsCommitted(othersxact)
                    && !SxactIsDoomed(othersxact)
                    && !SxactIsReadOnly(othersxact)
                {
                    // Err propagates with the lock held; the abort path's
                    // LWLockReleaseAll plays C's error-longjmp release.
                    SetPossibleUnsafeConflict(sxact, othersxact)?;
                }
                cur = (*cur).next;
            }

            if dlist_is_empty(&raw const (*sxact).possibleUnsafeConflicts) {
                ReleasePredXact(sxact);
                LWLockRelease(SerializableXactHashLock())?;
                return Ok(());
            }
        } else {
            (*px).WritableSxactCount += 1;
            debug_assert!(
                (*px).WritableSxactCount as i64
                    <= init_small::globals::MaxBackends() as i64
                        + shared().max_prepared_xacts as i64
            );
        }

        if !TransactionIdIsValid((*px).SxactGlobalXmin) {
            debug_assert!((*px).SxactGlobalXminCount == 0);
            (*px).SxactGlobalXmin = snapshot.xmin;
            (*px).SxactGlobalXminCount = 1;
            SerialSetActiveSerXmin(snapshot.xmin)?;
        } else if TransactionIdEquals(snapshot.xmin, (*px).SxactGlobalXmin) {
            debug_assert!((*px).SxactGlobalXminCount > 0);
            (*px).SxactGlobalXminCount += 1;
        } else {
            debug_assert!(TransactionIdFollows(snapshot.xmin, (*px).SxactGlobalXmin));
        }

        set_MySerializableXact(sxact);
        set_MyXactDidWrite(false);

        LWLockRelease(SerializableXactHashLock())?;

        CreateLocalPredicateLockHash()?;
        Ok(())
    }
}

fn CreateLocalPredicateLockHash() -> PgResult<()> {
    debug_assert!(LocalPredicateLockHash().is_null());
    let mut hash_ctl = HASHCTL::new();
    hash_ctl.keysize = size_of::<PREDICATELOCKTARGETTAG>();
    hash_ctl.entrysize = size_of::<LOCALPREDICATELOCK>();
    let h = hash_create(
        "Local predicate lock",
        max_predicate_locks_per_xact() as i64,
        &hash_ctl,
        HASH_ELEM | HASH_BLOBS,
    )?;
    LOCAL_PREDICATE_LOCK_HASH.with(|c| c.set(h));
    Ok(())
}

pub fn RegisterPredicateLockingXid(xid: TransactionId) -> PgResult<()> {
    unsafe {
        if MySerializableXact() == InvalidSerializableXact {
            return Ok(());
        }
        debug_assert!(TransactionIdIsValid(xid));

        LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE, my_procno())?;

        debug_assert!((*MySerializableXact()).topXid == InvalidTransactionId);
        (*MySerializableXact()).topXid = xid;

        let sxidtag = SERIALIZABLEXIDTAG { xid };
        let mut found = false;
        let p = hash_search(
            shared().xid_hash,
            &raw const sxidtag as *const u8,
            HASH_ENTER,
            Some(&mut found),
        )?;
        debug_assert!(!found);
        let sxid = p as *mut SERIALIZABLEXID;
        (*sxid).myXact = MySerializableXact();
        LWLockRelease(SerializableXactHashLock())?;
        Ok(())
    }
}

// ===========================================================================
// Local lock table: existence / parent walk / promotion.
// ===========================================================================

unsafe fn PredicateLockExists(targettag: *const PREDICATELOCKTARGETTAG) -> PgResult<bool> {
    let p = hash_search(
        LocalPredicateLockHash(),
        targettag as *const u8,
        HASH_FIND,
        None,
    )?;
    if p.is_null() {
        return Ok(false);
    }
    Ok((*(p as *mut LOCALPREDICATELOCK)).held)
}

unsafe fn GetParentPredicateLockTag(
    tag: *const PREDICATELOCKTARGETTAG,
    parent: *mut PREDICATELOCKTARGETTAG,
) -> bool {
    match GET_PREDICATELOCKTARGETTAG_TYPE(&*tag) {
        PREDLOCKTAG_RELATION => false,
        PREDLOCKTAG_PAGE => {
            SET_PREDICATELOCKTARGETTAG_RELATION(
                &mut *parent,
                GET_PREDICATELOCKTARGETTAG_DB(&*tag),
                GET_PREDICATELOCKTARGETTAG_RELATION(&*tag),
            );
            true
        }
        PREDLOCKTAG_TUPLE => {
            SET_PREDICATELOCKTARGETTAG_PAGE(
                &mut *parent,
                GET_PREDICATELOCKTARGETTAG_DB(&*tag),
                GET_PREDICATELOCKTARGETTAG_RELATION(&*tag),
                GET_PREDICATELOCKTARGETTAG_PAGE(&*tag),
            );
            true
        }
    }
}

unsafe fn CoarserLockCovers(newtargettag: *const PREDICATELOCKTARGETTAG) -> PgResult<bool> {
    let mut targettag = *newtargettag;
    let mut parenttag = ZERO_TARGET_TAG;
    while GetParentPredicateLockTag(&targettag, &mut parenttag) {
        targettag = parenttag;
        if PredicateLockExists(&targettag)? {
            return Ok(true);
        }
    }
    Ok(false)
}

unsafe fn RemoveScratchTarget(lockheld: bool) -> PgResult<()> {
    debug_assert!(LWLockHeldByMe(SerializablePredicateListLock()));
    if !lockheld {
        LWLockAcquire(ScratchPartitionLock(), LW_EXCLUSIVE, my_procno())?;
    }
    let mut found = false;
    let _ = hash_search_with_hash_value(
        shared().target_hash,
        &raw const SCRATCH_TARGET_TAG as *const u8,
        shared().scratch_target_tag_hash,
        HASH_REMOVE,
        Some(&mut found),
    )?;
    debug_assert!(found);
    if !lockheld {
        LWLockRelease(ScratchPartitionLock())?;
    }
    Ok(())
}

unsafe fn RestoreScratchTarget(lockheld: bool) -> PgResult<()> {
    debug_assert!(LWLockHeldByMe(SerializablePredicateListLock()));
    if !lockheld {
        LWLockAcquire(ScratchPartitionLock(), LW_EXCLUSIVE, my_procno())?;
    }
    let mut found = false;
    let _ = hash_search_with_hash_value(
        shared().target_hash,
        &raw const SCRATCH_TARGET_TAG as *const u8,
        shared().scratch_target_tag_hash,
        HASH_ENTER,
        Some(&mut found),
    )?;
    debug_assert!(!found);
    if !lockheld {
        LWLockRelease(ScratchPartitionLock())?;
    }
    Ok(())
}

unsafe fn RemoveTargetIfNoLongerUsed(
    target: *mut PREDICATELOCKTARGET,
    targettaghash: u32,
) -> PgResult<()> {
    debug_assert!(LWLockHeldByMe(SerializablePredicateListLock()));
    if !dlist_is_empty(&raw const (*target).predicateLocks) {
        return Ok(());
    }
    let rmtarget = hash_search_with_hash_value(
        shared().target_hash,
        &raw const (*target).tag as *const u8,
        targettaghash,
        HASH_REMOVE,
        None,
    )?;
    debug_assert!(rmtarget as *mut PREDICATELOCKTARGET == target);
    let _ = rmtarget;
    Ok(())
}

unsafe fn DeleteChildTargetLocks(newtargettag: *const PREDICATELOCKTARGETTAG) -> PgResult<()> {
    let procno = my_procno();
    LWLockAcquire(SerializablePredicateListLock(), LW_SHARED, procno)?;
    let sxact = MySerializableXact();
    if in_parallel_mode() {
        LWLockAcquire(&(*sxact).perXactPredicateListLock, LW_EXCLUSIVE, procno)?;
    }

    let head = &raw mut (*sxact).predicateLocks;
    let mut cur = (*head).head.next;
    while cur != (&raw mut (*head).head) as *mut dlist_node {
        let next = (*cur).next;
        let predlock = dlist_container!(PREDICATELOCK, xactLink, cur);

        let oldlocktag = (*predlock).tag;
        debug_assert!(oldlocktag.myXact == sxact);
        let oldtarget = oldlocktag.myTarget;
        let oldtargettag = (*oldtarget).tag;

        if TargetTagIsCoveredBy(&oldtargettag, &*newtargettag) {
            let oldtargettaghash = PredicateLockTargetTagHashCode(&oldtargettag);
            let partition_lock = PredicateLockHashPartitionLock(oldtargettaghash);
            LWLockAcquire(partition_lock, LW_EXCLUSIVE, procno)?;

            dlist_delete(&raw mut (*predlock).xactLink);
            dlist_delete(&raw mut (*predlock).targetLink);
            let rmpredlock = hash_search_with_hash_value(
                shared().lock_hash,
                &raw const oldlocktag as *const u8,
                PredicateLockHashCodeFromTargetHashCode(&oldlocktag, oldtargettaghash),
                HASH_REMOVE,
                None,
            )?;
            debug_assert!(rmpredlock as *mut PREDICATELOCK == predlock);
            let _ = rmpredlock;

            RemoveTargetIfNoLongerUsed(oldtarget, oldtargettaghash)?;

            LWLockRelease(partition_lock)?;

            DecrementParentLocks(&oldtargettag)?;
        }
        cur = next;
    }
    if in_parallel_mode() {
        LWLockRelease(&(*sxact).perXactPredicateListLock)?;
    }
    LWLockRelease(SerializablePredicateListLock())?;
    Ok(())
}

unsafe fn MaxPredicateChildLocks(tag: *const PREDICATELOCKTARGETTAG) -> i32 {
    match GET_PREDICATELOCKTARGETTAG_TYPE(&*tag) {
        PREDLOCKTAG_RELATION => {
            if max_predicate_locks_per_relation() < 0 {
                (max_predicate_locks_per_xact() / (-max_predicate_locks_per_relation())) - 1
            } else {
                max_predicate_locks_per_relation()
            }
        }
        PREDLOCKTAG_PAGE => max_predicate_locks_per_page(),
        PREDLOCKTAG_TUPLE => {
            debug_assert!(false);
            0
        }
    }
}

unsafe fn CheckAndPromotePredicateLockRequest(
    reqtag: *const PREDICATELOCKTARGETTAG,
) -> PgResult<bool> {
    let mut promote = false;
    let mut targettag = *reqtag;
    let mut nexttag = ZERO_TARGET_TAG;
    let mut promotiontag = targettag;

    while GetParentPredicateLockTag(&targettag, &mut nexttag) {
        targettag = nexttag;
        let mut found = false;
        let p = hash_search(
            LocalPredicateLockHash(),
            &raw const targettag as *const u8,
            HASH_ENTER,
            Some(&mut found),
        )?;
        let parentlock = p as *mut LOCALPREDICATELOCK;
        if !found {
            (*parentlock).held = false;
            (*parentlock).childLocks = 1;
        } else {
            (*parentlock).childLocks += 1;
        }

        if (*parentlock).childLocks > MaxPredicateChildLocks(&targettag) {
            promotiontag = targettag;
            promote = true;
        }
    }

    if promote {
        PredicateLockAcquire(&promotiontag)?;
        Ok(true)
    } else {
        Ok(false)
    }
}

unsafe fn DecrementParentLocks(targettag: *const PREDICATELOCKTARGETTAG) -> PgResult<()> {
    let mut parenttag = *targettag;
    let mut nexttag = ZERO_TARGET_TAG;

    while GetParentPredicateLockTag(&parenttag, &mut nexttag) {
        parenttag = nexttag;
        let targettaghash = PredicateLockTargetTagHashCode(&parenttag);
        let p = hash_search_with_hash_value(
            LocalPredicateLockHash(),
            &raw const parenttag as *const u8,
            targettaghash,
            HASH_FIND,
            None,
        )?;
        if p.is_null() {
            continue;
        }
        let parentlock = p as *mut LOCALPREDICATELOCK;
        (*parentlock).childLocks -= 1;

        if (*parentlock).childLocks < 0 {
            debug_assert!((*parentlock).held);
            (*parentlock).childLocks = 0;
        }

        if (*parentlock).childLocks == 0 && !(*parentlock).held {
            let rmlock = hash_search_with_hash_value(
                LocalPredicateLockHash(),
                &raw const parenttag as *const u8,
                targettaghash,
                HASH_REMOVE,
                None,
            )?;
            debug_assert!(rmlock as *mut LOCALPREDICATELOCK == parentlock);
            let _ = rmlock;
        }
    }
    Ok(())
}

unsafe fn CreatePredicateLock(
    targettag: *const PREDICATELOCKTARGETTAG,
    targettaghash: u32,
    sxact: *mut SERIALIZABLEXACT,
) -> PgResult<()> {
    let partition_lock = PredicateLockHashPartitionLock(targettaghash);
    let procno = my_procno();

    LWLockAcquire(SerializablePredicateListLock(), LW_SHARED, procno)?;
    if in_parallel_mode() {
        LWLockAcquire(&(*sxact).perXactPredicateListLock, LW_EXCLUSIVE, procno)?;
    }
    LWLockAcquire(partition_lock, LW_EXCLUSIVE, procno)?;

    let mut found = false;
    let tp = hash_search_with_hash_value(
        shared().target_hash,
        targettag as *const u8,
        targettaghash,
        HASH_ENTER_NULL,
        Some(&mut found),
    )?;
    let target = tp as *mut PREDICATELOCKTARGET;
    if target.is_null() {
        LWLockRelease(partition_lock)?;
        if in_parallel_mode() {
            LWLockRelease(&(*sxact).perXactPredicateListLock)?;
        }
        LWLockRelease(SerializablePredicateListLock())?;
        return Err(out_of_shared_memory());
    }
    if !found {
        dlist_init(&raw mut (*target).predicateLocks);
    }

    let locktag = PREDICATELOCKTAG {
        myTarget: target,
        myXact: sxact,
    };
    let mut found = false;
    let lp = hash_search_with_hash_value(
        shared().lock_hash,
        &raw const locktag as *const u8,
        PredicateLockHashCodeFromTargetHashCode(&locktag, targettaghash),
        HASH_ENTER_NULL,
        Some(&mut found),
    )?;
    let lock = lp as *mut PREDICATELOCK;
    if lock.is_null() {
        LWLockRelease(partition_lock)?;
        if in_parallel_mode() {
            LWLockRelease(&(*sxact).perXactPredicateListLock)?;
        }
        LWLockRelease(SerializablePredicateListLock())?;
        return Err(out_of_shared_memory());
    }

    if !found {
        dlist_push_tail(&raw mut (*target).predicateLocks, &raw mut (*lock).targetLink);
        dlist_push_tail(&raw mut (*sxact).predicateLocks, &raw mut (*lock).xactLink);
        (*lock).commitSeqNo = InvalidSerCommitSeqNo;
    }

    LWLockRelease(partition_lock)?;
    if in_parallel_mode() {
        LWLockRelease(&(*sxact).perXactPredicateListLock)?;
    }
    LWLockRelease(SerializablePredicateListLock())?;
    Ok(())
}

unsafe fn PredicateLockAcquire(targettag: *const PREDICATELOCKTARGETTAG) -> PgResult<()> {
    if PredicateLockExists(targettag)? {
        return Ok(());
    }
    if CoarserLockCovers(targettag)? {
        return Ok(());
    }

    let targettaghash = PredicateLockTargetTagHashCode(targettag);

    let mut found = false;
    let p = hash_search_with_hash_value(
        LocalPredicateLockHash(),
        targettag as *const u8,
        targettaghash,
        HASH_ENTER,
        Some(&mut found),
    )?;
    let locallock = p as *mut LOCALPREDICATELOCK;
    (*locallock).held = true;
    if !found {
        (*locallock).childLocks = 0;
    }

    CreatePredicateLock(targettag, targettaghash, MySerializableXact())?;

    if CheckAndPromotePredicateLockRequest(targettag)? {
        // Promoted; the coarser lock subsumed this one and its siblings.
    } else if GET_PREDICATELOCKTARGETTAG_TYPE(&*targettag) != PREDLOCKTAG_TUPLE {
        DeleteChildTargetLocks(targettag)?;
    }
    Ok(())
}

// ===========================================================================
// PredicateLockRelation / Page / TID — over projected Relation fields.
// ===========================================================================

pub fn PredicateLockRelation(
    db_oid: Oid,
    rd_id: Oid,
    uses_local_buffers: bool,
    snapshot: &SnapshotData<'_>,
) -> PgResult<()> {
    unsafe {
        if !SerializationNeededForRead(rd_id, uses_local_buffers, snapshot)? {
            return Ok(());
        }
        let mut tag = ZERO_TARGET_TAG;
        SET_PREDICATELOCKTARGETTAG_RELATION(&mut tag, db_oid, rd_id);
        PredicateLockAcquire(&tag)
    }
}

pub fn PredicateLockPage(
    db_oid: Oid,
    rd_id: Oid,
    uses_local_buffers: bool,
    blkno: BlockNumber,
    snapshot: &SnapshotData<'_>,
) -> PgResult<()> {
    unsafe {
        if !SerializationNeededForRead(rd_id, uses_local_buffers, snapshot)? {
            return Ok(());
        }
        let mut tag = ZERO_TARGET_TAG;
        SET_PREDICATELOCKTARGETTAG_PAGE(&mut tag, db_oid, rd_id, blkno);
        PredicateLockAcquire(&tag)
    }
}

#[allow(clippy::too_many_arguments)]
pub fn PredicateLockTID(
    db_oid: Oid,
    rd_id: Oid,
    uses_local_buffers: bool,
    is_index: bool,
    blkno: BlockNumber,
    offnum: OffsetNumber,
    snapshot: &SnapshotData<'_>,
    tuple_xid: TransactionId,
) -> PgResult<()> {
    unsafe {
        if !SerializationNeededForRead(rd_id, uses_local_buffers, snapshot)? {
            return Ok(());
        }

        if !is_index && xact_seams::transaction_id_is_current_transaction_id::call(tuple_xid) {
            return Ok(());
        }

        let mut tag = ZERO_TARGET_TAG;
        SET_PREDICATELOCKTARGETTAG_RELATION(&mut tag, db_oid, rd_id);
        if PredicateLockExists(&tag)? {
            return Ok(());
        }

        SET_PREDICATELOCKTARGETTAG_TUPLE(&mut tag, db_oid, rd_id, blkno, offnum);
        PredicateLockAcquire(&tag)
    }
}

// ===========================================================================
// Page split/combine lock transfer.
// ===========================================================================

unsafe fn DeleteLockTarget(target: *mut PREDICATELOCKTARGET, targettaghash: u32) -> PgResult<()> {
    debug_assert!(LWLockHeldByMeInMode(
        SerializablePredicateListLock(),
        LW_EXCLUSIVE
    ));
    debug_assert!(LWLockHeldByMe(PredicateLockHashPartitionLock(targettaghash)));

    LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE, my_procno())?;

    let head = &raw mut (*target).predicateLocks;
    let mut cur = (*head).head.next;
    while cur != (&raw mut (*head).head) as *mut dlist_node {
        let next = (*cur).next;
        let predlock = dlist_container!(PREDICATELOCK, targetLink, cur);
        dlist_delete(&raw mut (*predlock).xactLink);
        dlist_delete(&raw mut (*predlock).targetLink);
        let mut found = false;
        let _ = hash_search_with_hash_value(
            shared().lock_hash,
            &raw const (*predlock).tag as *const u8,
            PredicateLockHashCodeFromTargetHashCode(&(*predlock).tag, targettaghash),
            HASH_REMOVE,
            Some(&mut found),
        )?;
        debug_assert!(found);
        cur = next;
    }
    LWLockRelease(SerializableXactHashLock())?;

    RemoveTargetIfNoLongerUsed(target, targettaghash)?;
    Ok(())
}

unsafe fn TransferPredicateLocksToNewTarget(
    oldtargettag: PREDICATELOCKTARGETTAG,
    newtargettag: PREDICATELOCKTARGETTAG,
    removeOld: bool,
) -> PgResult<bool> {
    debug_assert!(LWLockHeldByMeInMode(
        SerializablePredicateListLock(),
        LW_EXCLUSIVE
    ));

    let oldtargettaghash = PredicateLockTargetTagHashCode(&oldtargettag);
    let newtargettaghash = PredicateLockTargetTagHashCode(&newtargettag);
    let oldpartition_lock = PredicateLockHashPartitionLock(oldtargettaghash);
    let newpartition_lock = PredicateLockHashPartitionLock(newtargettaghash);
    let procno = my_procno();

    if removeOld {
        RemoveScratchTarget(false)?;
    }

    let old_addr = oldpartition_lock as *const LWLock as usize;
    let new_addr = newpartition_lock as *const LWLock as usize;
    if old_addr < new_addr {
        LWLockAcquire(
            oldpartition_lock,
            if removeOld { LW_EXCLUSIVE } else { LW_SHARED },
            procno,
        )?;
        LWLockAcquire(newpartition_lock, LW_EXCLUSIVE, procno)?;
    } else if old_addr > new_addr {
        LWLockAcquire(newpartition_lock, LW_EXCLUSIVE, procno)?;
        LWLockAcquire(
            oldpartition_lock,
            if removeOld { LW_EXCLUSIVE } else { LW_SHARED },
            procno,
        )?;
    } else {
        LWLockAcquire(newpartition_lock, LW_EXCLUSIVE, procno)?;
    }

    let mut out_of_shmem = false;

    let otp = hash_search_with_hash_value(
        shared().target_hash,
        &raw const oldtargettag as *const u8,
        oldtargettaghash,
        HASH_FIND,
        None,
    )?;
    let oldtarget = otp as *mut PREDICATELOCKTARGET;

    'exit: {
        if oldtarget.is_null() {
            break 'exit;
        }

        let mut found = false;
        let ntp = hash_search_with_hash_value(
            shared().target_hash,
            &raw const newtargettag as *const u8,
            newtargettaghash,
            HASH_ENTER_NULL,
            Some(&mut found),
        )?;
        let newtarget = ntp as *mut PREDICATELOCKTARGET;
        if newtarget.is_null() {
            out_of_shmem = true;
            break 'exit;
        }
        if !found {
            dlist_init(&raw mut (*newtarget).predicateLocks);
        }

        let mut newpredlocktag = PREDICATELOCKTAG {
            myTarget: newtarget,
            myXact: ptr::null_mut(),
        };

        LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE, procno)?;

        let head = &raw mut (*oldtarget).predicateLocks;
        let mut cur = (*head).head.next;
        while cur != (&raw mut (*head).head) as *mut dlist_node {
            let next = (*cur).next;
            let oldpredlock = dlist_container!(PREDICATELOCK, targetLink, cur);
            let oldCommitSeqNo = (*oldpredlock).commitSeqNo;

            newpredlocktag.myXact = (*oldpredlock).tag.myXact;

            if removeOld {
                dlist_delete(&raw mut (*oldpredlock).xactLink);
                dlist_delete(&raw mut (*oldpredlock).targetLink);
                let mut found = false;
                let _ = hash_search_with_hash_value(
                    shared().lock_hash,
                    &raw const (*oldpredlock).tag as *const u8,
                    PredicateLockHashCodeFromTargetHashCode(
                        &(*oldpredlock).tag,
                        oldtargettaghash,
                    ),
                    HASH_REMOVE,
                    Some(&mut found),
                )?;
                debug_assert!(found);
            }

            let mut found = false;
            let npp = hash_search_with_hash_value(
                shared().lock_hash,
                &raw const newpredlocktag as *const u8,
                PredicateLockHashCodeFromTargetHashCode(&newpredlocktag, newtargettaghash),
                HASH_ENTER_NULL,
                Some(&mut found),
            )?;
            let newpredlock = npp as *mut PREDICATELOCK;
            if newpredlock.is_null() {
                LWLockRelease(SerializableXactHashLock())?;
                DeleteLockTarget(newtarget, newtargettaghash)?;
                out_of_shmem = true;
                break 'exit;
            }
            if !found {
                dlist_push_tail(
                    &raw mut (*newtarget).predicateLocks,
                    &raw mut (*newpredlock).targetLink,
                );
                dlist_push_tail(
                    &raw mut (*newpredlocktag.myXact).predicateLocks,
                    &raw mut (*newpredlock).xactLink,
                );
                (*newpredlock).commitSeqNo = oldCommitSeqNo;
            } else if (*newpredlock).commitSeqNo < oldCommitSeqNo {
                (*newpredlock).commitSeqNo = oldCommitSeqNo;
            }

            debug_assert!((*newpredlock).commitSeqNo != 0);
            debug_assert!(
                (*newpredlock).commitSeqNo == InvalidSerCommitSeqNo
                    || (*newpredlock).tag.myXact == shared().old_committed_sxact
            );
            cur = next;
        }
        LWLockRelease(SerializableXactHashLock())?;

        if removeOld {
            debug_assert!(dlist_is_empty(&raw const (*oldtarget).predicateLocks));
            RemoveTargetIfNoLongerUsed(oldtarget, oldtargettaghash)?;
        }
    }

    if old_addr < new_addr {
        LWLockRelease(newpartition_lock)?;
        LWLockRelease(oldpartition_lock)?;
    } else if old_addr > new_addr {
        LWLockRelease(oldpartition_lock)?;
        LWLockRelease(newpartition_lock)?;
    } else {
        LWLockRelease(newpartition_lock)?;
    }

    if removeOld {
        debug_assert!(!out_of_shmem);
        RestoreScratchTarget(false)?;
    }

    Ok(!out_of_shmem)
}

pub fn PredicateLockPageSplit(
    db_oid: Oid,
    rd_id: Oid,
    uses_local_buffers: bool,
    oldblkno: BlockNumber,
    newblkno: BlockNumber,
) -> PgResult<()> {
    unsafe {
        if !TransactionIdIsValid((*shared().pred_xact).SxactGlobalXmin) {
            return Ok(());
        }
        if !predicate_locking_needed(rd_id, uses_local_buffers) {
            return Ok(());
        }
        debug_assert!(oldblkno != newblkno);

        let mut oldtargettag = ZERO_TARGET_TAG;
        let mut newtargettag = ZERO_TARGET_TAG;
        SET_PREDICATELOCKTARGETTAG_PAGE(&mut oldtargettag, db_oid, rd_id, oldblkno);
        SET_PREDICATELOCKTARGETTAG_PAGE(&mut newtargettag, db_oid, rd_id, newblkno);

        LWLockAcquire(SerializablePredicateListLock(), LW_EXCLUSIVE, my_procno())?;

        let mut success = TransferPredicateLocksToNewTarget(oldtargettag, newtargettag, false)?;
        if !success {
            let r = GetParentPredicateLockTag(&oldtargettag, &mut newtargettag);
            debug_assert!(r);
            success = TransferPredicateLocksToNewTarget(oldtargettag, newtargettag, true)?;
            debug_assert!(success);
            let _ = success;
        }

        LWLockRelease(SerializablePredicateListLock())?;
        Ok(())
    }
}

pub fn PredicateLockPageCombine(
    db_oid: Oid,
    rd_id: Oid,
    uses_local_buffers: bool,
    oldblkno: BlockNumber,
    newblkno: BlockNumber,
) -> PgResult<()> {
    PredicateLockPageSplit(db_oid, rd_id, uses_local_buffers, oldblkno, newblkno)
}

// ===========================================================================
// Transaction xmin maintenance / release at xact end.
// ===========================================================================

unsafe fn SetNewSxactGlobalXmin() -> PgResult<()> {
    debug_assert!(LWLockHeldByMe(SerializableXactHashLock()));
    let px = shared().pred_xact;
    (*px).SxactGlobalXmin = InvalidTransactionId;
    (*px).SxactGlobalXminCount = 0;

    let head = &raw const (*px).activeList;
    let mut cur = (*head).head.next;
    while cur != (&raw const (*head).head) as *mut dlist_node {
        let sxact = dlist_container!(SERIALIZABLEXACT, xactLink, cur);
        if !SxactIsRolledBack(sxact)
            && !SxactIsCommitted(sxact)
            && sxact != shared().old_committed_sxact
        {
            debug_assert!((*sxact).xmin != InvalidTransactionId);
            if !TransactionIdIsValid((*px).SxactGlobalXmin)
                || TransactionIdPrecedes((*sxact).xmin, (*px).SxactGlobalXmin)
            {
                (*px).SxactGlobalXmin = (*sxact).xmin;
                (*px).SxactGlobalXminCount = 1;
            } else if TransactionIdEquals((*sxact).xmin, (*px).SxactGlobalXmin) {
                (*px).SxactGlobalXminCount += 1;
            }
        }
        cur = (*cur).next;
    }

    SerialSetActiveSerXmin((*px).SxactGlobalXmin)?;
    Ok(())
}

pub fn ReleasePredicateLocks(mut isCommit: bool, isReadOnlySafe: bool) -> PgResult<()> {
    unsafe {
        let mut partiallyReleasing = false;

        debug_assert!(!(isCommit && isReadOnlySafe));

        // Non-serializable fast path (every commit/abort lands here): with no
        // sxact and no leader stash, C's worker and leader arms are both
        // no-ops, so skip the is_parallel_worker seam call. Single fused
        // branch: both cells live in one thread_local block.
        if (MySerializableXact() as usize | SAVED_SERIALIZABLE_XACT.with(|c| c.get()) as usize)
            == 0
        {
            debug_assert!(LocalPredicateLockHash().is_null());
            return Ok(());
        }

        if !isReadOnlySafe {
            // Workers must not release predicate locks at end of transaction;
            // the leader owns that (predicate.c ReleasePredicateLocks).
            if is_parallel_worker() {
                ReleasePredicateLocksLocal();
                return Ok(());
            }

            if SAVED_SERIALIZABLE_XACT.with(|c| c.get()) != InvalidSerializableXact {
                debug_assert!(MySerializableXact() == InvalidSerializableXact);
                set_MySerializableXact(SAVED_SERIALIZABLE_XACT.with(|c| c.get()));
                SAVED_SERIALIZABLE_XACT.with(|c| c.set(InvalidSerializableXact));
                debug_assert!(SxactIsPartiallyReleased(MySerializableXact()));
            }
        }

        if MySerializableXact() == InvalidSerializableXact {
            debug_assert!(LocalPredicateLockHash().is_null());
            return Ok(());
        }

        let procno = my_procno();
        LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE, procno)?;

        let mysx = MySerializableXact();

        if isCommit && SxactIsPartiallyReleased(mysx) {
            isCommit = false;
        }

        if isReadOnlySafe && in_parallel_mode() {
            // The leader stashes the sxact for full release at end of
            // transaction; workers may still be referencing it.
            if !is_parallel_worker() {
                SAVED_SERIALIZABLE_XACT.with(|c| c.set(mysx));
            }
            if SxactIsPartiallyReleased(mysx) {
                LWLockRelease(SerializableXactHashLock())?;
                ReleasePredicateLocksLocal();
                return Ok(());
            } else {
                (*mysx).flags |= SXACT_FLAG_PARTIALLY_RELEASED;
                partiallyReleasing = true;
            }
        }
        debug_assert!(!isCommit || SxactIsPrepared(mysx));
        debug_assert!(!isCommit || !SxactIsDoomed(mysx));
        debug_assert!(!SxactIsCommitted(mysx));
        debug_assert!(SxactIsPartiallyReleased(mysx) || !SxactIsRolledBack(mysx));
        debug_assert!((*mysx).pid == 0 || xact_seams::isolation_is_serializable::call());
        debug_assert!(!SxactIsOnFinishedList(mysx));

        let topLevelIsDeclaredReadOnly = SxactIsReadOnly(mysx);

        (*mysx).finishedBefore = varsup_seams::read_next_transaction_id::call()?;

        let px = shared().pred_xact;
        if isCommit {
            (*mysx).flags |= SXACT_FLAG_COMMITTED;
            (*px).LastSxactCommitSeqNo += 1;
            (*mysx).commitSeqNo = (*px).LastSxactCommitSeqNo;
            if !MyXactDidWrite() {
                (*mysx).flags |= SXACT_FLAG_READ_ONLY;
            }
        } else {
            (*mysx).flags |= SXACT_FLAG_DOOMED;
            (*mysx).flags |= SXACT_FLAG_ROLLED_BACK;
            (*mysx).flags &= !SXACT_FLAG_PREPARED;
        }

        if !topLevelIsDeclaredReadOnly {
            debug_assert!((*px).WritableSxactCount > 0);
            (*px).WritableSxactCount -= 1;
            if (*px).WritableSxactCount == 0 {
                (*px).CanPartialClearThrough = (*px).LastSxactCommitSeqNo;
            }
        } else {
            let head = &raw mut (*mysx).possibleUnsafeConflicts;
            let mut cur = (*head).head.next;
            while cur != (&raw mut (*head).head) as *mut dlist_node {
                let next = (*cur).next;
                let puc = dlist_container!(RWConflictData, inLink, cur);
                debug_assert!(!SxactIsReadOnly((*puc).sxactOut));
                debug_assert!(mysx == (*puc).sxactIn);
                ReleaseRWConflict(puc);
                cur = next;
            }
        }

        if isCommit && !SxactIsReadOnly(mysx) && SxactHasSummaryConflictOut(mysx) {
            (*mysx).SeqNo.earliestOutConflictCommit = FirstNormalSerCommitSeqNo;
            (*mysx).flags |= SXACT_FLAG_CONFLICT_OUT;
        }

        let head = &raw mut (*mysx).outConflicts;
        let mut cur = (*head).head.next;
        while cur != (&raw mut (*head).head) as *mut dlist_node {
            let next = (*cur).next;
            let conflict = dlist_container!(RWConflictData, outLink, cur);

            if isCommit && !SxactIsReadOnly(mysx) && SxactIsCommitted((*conflict).sxactIn) {
                if ((*mysx).flags & SXACT_FLAG_CONFLICT_OUT) == 0
                    || (*(*conflict).sxactIn).prepareSeqNo
                        < (*mysx).SeqNo.earliestOutConflictCommit
                {
                    (*mysx).SeqNo.earliestOutConflictCommit = (*(*conflict).sxactIn).prepareSeqNo;
                }
                (*mysx).flags |= SXACT_FLAG_CONFLICT_OUT;
            }

            if !isCommit
                || SxactIsCommitted((*conflict).sxactIn)
                || ((*(*conflict).sxactIn).SeqNo.lastCommitBeforeSnapshot
                    >= (*px).LastSxactCommitSeqNo)
            {
                ReleaseRWConflict(conflict);
            }
            cur = next;
        }

        let head = &raw mut (*mysx).inConflicts;
        let mut cur = (*head).head.next;
        while cur != (&raw mut (*head).head) as *mut dlist_node {
            let next = (*cur).next;
            let conflict = dlist_container!(RWConflictData, inLink, cur);
            if !isCommit
                || SxactIsCommitted((*conflict).sxactOut)
                || SxactIsReadOnly((*conflict).sxactOut)
            {
                ReleaseRWConflict(conflict);
            }
            cur = next;
        }

        if !topLevelIsDeclaredReadOnly {
            let head = &raw mut (*mysx).possibleUnsafeConflicts;
            let mut cur = (*head).head.next;
            while cur != (&raw mut (*head).head) as *mut dlist_node {
                let next = (*cur).next;
                let puc = dlist_container!(RWConflictData, outLink, cur);
                let roXact = (*puc).sxactIn;
                debug_assert!(mysx == (*puc).sxactOut);
                debug_assert!(SxactIsReadOnly(roXact));

                if isCommit
                    && MyXactDidWrite()
                    && SxactHasConflictOut(mysx)
                    && ((*mysx).SeqNo.earliestOutConflictCommit
                        <= (*roXact).SeqNo.lastCommitBeforeSnapshot)
                {
                    FlagSxactUnsafe(roXact);
                } else {
                    ReleaseRWConflict(puc);
                    if dlist_is_empty(&raw const (*roXact).possibleUnsafeConflicts) {
                        (*roXact).flags |= SXACT_FLAG_RO_SAFE;
                    }
                }

                // Wake a waiting DEFERRABLE transaction once it's known safe
                // or conflicted (predicate.c:3616).
                if SxactIsDeferrableWaiting(roXact)
                    && (SxactIsROUnsafe(roXact) || SxactIsROSafe(roXact))
                {
                    lmgr_proc::ProcSendSignal((*roXact).pgprocno)?;
                }
                cur = next;
            }
        }

        let mut needToClear = false;
        if (partiallyReleasing || !SxactIsPartiallyReleased(mysx))
            && TransactionIdEquals((*mysx).xmin, (*px).SxactGlobalXmin)
        {
            debug_assert!((*px).SxactGlobalXminCount > 0);
            (*px).SxactGlobalXminCount -= 1;
            if (*px).SxactGlobalXminCount == 0 {
                SetNewSxactGlobalXmin()?;
                needToClear = true;
            }
        }

        LWLockRelease(SerializableXactHashLock())?;

        LWLockAcquire(SerializableFinishedListLock(), LW_EXCLUSIVE, procno)?;

        if isCommit {
            dlist_push_tail(shared().finished, &raw mut (*mysx).finishedLink);
        }

        if !isCommit {
            ReleaseOneSerializableXact(mysx, isReadOnlySafe && in_parallel_mode(), false)?;
        }

        LWLockRelease(SerializableFinishedListLock())?;

        if needToClear {
            ClearOldPredicateLocks()?;
        }

        ReleasePredicateLocksLocal();
        Ok(())
    }
}

fn ReleasePredicateLocksLocal() {
    set_MySerializableXact(InvalidSerializableXact);
    set_MyXactDidWrite(false);
    let h = LocalPredicateLockHash();
    if !h.is_null() {
        hash_destroy(h);
        LOCAL_PREDICATE_LOCK_HASH.with(|c| c.set(ptr::null_mut()));
    }
}

unsafe fn ClearOldPredicateLocks() -> PgResult<()> {
    let procno = my_procno();
    LWLockAcquire(SerializableFinishedListLock(), LW_EXCLUSIVE, procno)?;
    LWLockAcquire(SerializableXactHashLock(), LW_SHARED, procno)?;

    let px = shared().pred_xact;
    let finished = shared().finished;
    let mut cur = (*finished).head.next;
    while cur != (&raw mut (*finished).head) as *mut dlist_node {
        let next = (*cur).next;
        let finishedSxact = dlist_container!(SERIALIZABLEXACT, finishedLink, cur);

        if !TransactionIdIsValid((*px).SxactGlobalXmin)
            || TransactionIdPrecedesOrEquals((*finishedSxact).finishedBefore, (*px).SxactGlobalXmin)
        {
            LWLockRelease(SerializableXactHashLock())?;
            dlist_delete_thoroughly(&raw mut (*finishedSxact).finishedLink);
            ReleaseOneSerializableXact(finishedSxact, false, false)?;
            LWLockAcquire(SerializableXactHashLock(), LW_SHARED, procno)?;
        } else if (*finishedSxact).commitSeqNo > (*px).HavePartialClearedThrough
            && (*finishedSxact).commitSeqNo <= (*px).CanPartialClearThrough
        {
            LWLockRelease(SerializableXactHashLock())?;
            if SxactIsReadOnly(finishedSxact) {
                dlist_delete_thoroughly(&raw mut (*finishedSxact).finishedLink);
                ReleaseOneSerializableXact(finishedSxact, false, false)?;
            } else {
                ReleaseOneSerializableXact(finishedSxact, true, false)?;
            }
            (*px).HavePartialClearedThrough = (*finishedSxact).commitSeqNo;
            LWLockAcquire(SerializableXactHashLock(), LW_SHARED, procno)?;
        } else {
            break;
        }
        cur = next;
    }
    LWLockRelease(SerializableXactHashLock())?;

    // Predicate locks on the dummy OldCommittedSxact (summarized data).
    LWLockAcquire(SerializablePredicateListLock(), LW_SHARED, procno)?;
    let oc = shared().old_committed_sxact;
    let head = &raw mut (*oc).predicateLocks;
    let mut cur = (*head).head.next;
    while cur != (&raw mut (*head).head) as *mut dlist_node {
        let next = (*cur).next;
        let predlock = dlist_container!(PREDICATELOCK, xactLink, cur);

        LWLockAcquire(SerializableXactHashLock(), LW_SHARED, procno)?;
        debug_assert!((*predlock).commitSeqNo != 0);
        debug_assert!((*predlock).commitSeqNo != InvalidSerCommitSeqNo);
        let canDoPartialCleanup = (*predlock).commitSeqNo <= (*px).CanPartialClearThrough;
        LWLockRelease(SerializableXactHashLock())?;

        if canDoPartialCleanup {
            let tag = (*predlock).tag;
            let target = tag.myTarget;
            let targettag = (*target).tag;
            let targettaghash = PredicateLockTargetTagHashCode(&targettag);
            let partition_lock = PredicateLockHashPartitionLock(targettaghash);

            LWLockAcquire(partition_lock, LW_EXCLUSIVE, procno)?;

            dlist_delete(&raw mut (*predlock).targetLink);
            dlist_delete(&raw mut (*predlock).xactLink);

            hash_search_with_hash_value(
                shared().lock_hash,
                &raw const tag as *const u8,
                PredicateLockHashCodeFromTargetHashCode(&tag, targettaghash),
                HASH_REMOVE,
                None,
            )?;
            RemoveTargetIfNoLongerUsed(target, targettaghash)?;

            LWLockRelease(partition_lock)?;
        }
        cur = next;
    }

    LWLockRelease(SerializablePredicateListLock())?;
    LWLockRelease(SerializableFinishedListLock())?;
    Ok(())
}

unsafe fn ReleaseOneSerializableXact(
    sxact: *mut SERIALIZABLEXACT,
    partial: bool,
    summarize: bool,
) -> PgResult<()> {
    debug_assert!(!sxact.is_null());
    debug_assert!(SxactIsRolledBack(sxact) || SxactIsCommitted(sxact));
    debug_assert!(partial || !SxactIsOnFinishedList(sxact));
    debug_assert!(LWLockHeldByMe(SerializableFinishedListLock()));

    let procno = my_procno();

    LWLockAcquire(SerializablePredicateListLock(), LW_SHARED, procno)?;
    if in_parallel_mode() {
        LWLockAcquire(&(*sxact).perXactPredicateListLock, LW_EXCLUSIVE, procno)?;
    }

    let head = &raw mut (*sxact).predicateLocks;
    let mut cur = (*head).head.next;
    while cur != (&raw mut (*head).head) as *mut dlist_node {
        let next = (*cur).next;
        let mut predlock = dlist_container!(PREDICATELOCK, xactLink, cur);

        let mut tag = (*predlock).tag;
        let target = tag.myTarget;
        let targettag = (*target).tag;
        let targettaghash = PredicateLockTargetTagHashCode(&targettag);
        let partition_lock = PredicateLockHashPartitionLock(targettaghash);

        LWLockAcquire(partition_lock, LW_EXCLUSIVE, procno)?;

        dlist_delete(&raw mut (*predlock).targetLink);

        hash_search_with_hash_value(
            shared().lock_hash,
            &raw const tag as *const u8,
            PredicateLockHashCodeFromTargetHashCode(&tag, targettaghash),
            HASH_REMOVE,
            None,
        )?;

        if summarize {
            tag.myXact = shared().old_committed_sxact;
            let mut found = false;
            let pp = hash_search_with_hash_value(
                shared().lock_hash,
                &raw const tag as *const u8,
                PredicateLockHashCodeFromTargetHashCode(&tag, targettaghash),
                HASH_ENTER_NULL,
                Some(&mut found),
            )?;
            predlock = pp as *mut PREDICATELOCK;
            if predlock.is_null() {
                LWLockRelease(partition_lock)?;
                if in_parallel_mode() {
                    LWLockRelease(&(*sxact).perXactPredicateListLock)?;
                }
                LWLockRelease(SerializablePredicateListLock())?;
                return Err(out_of_shared_memory());
            }
            if found {
                debug_assert!((*predlock).commitSeqNo != 0);
                debug_assert!((*predlock).commitSeqNo != InvalidSerCommitSeqNo);
                if (*predlock).commitSeqNo < (*sxact).commitSeqNo {
                    (*predlock).commitSeqNo = (*sxact).commitSeqNo;
                }
            } else {
                dlist_push_tail(
                    &raw mut (*target).predicateLocks,
                    &raw mut (*predlock).targetLink,
                );
                dlist_push_tail(
                    &raw mut (*shared().old_committed_sxact).predicateLocks,
                    &raw mut (*predlock).xactLink,
                );
                (*predlock).commitSeqNo = (*sxact).commitSeqNo;
            }
        } else {
            RemoveTargetIfNoLongerUsed(target, targettaghash)?;
        }

        LWLockRelease(partition_lock)?;
        cur = next;
    }

    dlist_init(&raw mut (*sxact).predicateLocks);

    if in_parallel_mode() {
        LWLockRelease(&(*sxact).perXactPredicateListLock)?;
    }
    LWLockRelease(SerializablePredicateListLock())?;

    let sxidtag = SERIALIZABLEXIDTAG {
        xid: (*sxact).topXid,
    };
    LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE, procno)?;

    if !partial {
        let head = &raw mut (*sxact).outConflicts;
        let mut cur = (*head).head.next;
        while cur != (&raw mut (*head).head) as *mut dlist_node {
            let next = (*cur).next;
            let conflict = dlist_container!(RWConflictData, outLink, cur);
            if summarize {
                (*(*conflict).sxactIn).flags |= SXACT_FLAG_SUMMARY_CONFLICT_IN;
            }
            ReleaseRWConflict(conflict);
            cur = next;
        }
    }

    let head = &raw mut (*sxact).inConflicts;
    let mut cur = (*head).head.next;
    while cur != (&raw mut (*head).head) as *mut dlist_node {
        let next = (*cur).next;
        let conflict = dlist_container!(RWConflictData, inLink, cur);
        if summarize {
            (*(*conflict).sxactOut).flags |= SXACT_FLAG_SUMMARY_CONFLICT_OUT;
        }
        ReleaseRWConflict(conflict);
        cur = next;
    }

    if !partial {
        if sxidtag.xid != InvalidTransactionId {
            hash_search(
                shared().xid_hash,
                &raw const sxidtag as *const u8,
                HASH_REMOVE,
                None,
            )?;
        }
        ReleasePredXact(sxact);
    }

    LWLockRelease(SerializableXactHashLock())?;
    Ok(())
}

// ===========================================================================
// Conflict detection.
// ===========================================================================

unsafe fn XidIsConcurrent(xid: TransactionId) -> PgResult<bool> {
    debug_assert!(TransactionIdIsValid(xid));
    debug_assert!(!TransactionIdEquals(
        xid,
        xact_seams::get_top_transaction_id_if_any::call()
    ));

    let snap = snapmgr::GetTransactionSnapshot()?;

    if TransactionIdPrecedes(xid, snap.xmin) {
        return Ok(false);
    }
    if TransactionIdFollowsOrEquals(xid, snap.xmax) {
        return Ok(true);
    }
    Ok(snap.xip[..snap.xcnt as usize].contains(&xid))
}

pub fn CheckForSerializableConflictOutNeeded(
    rd_id: Oid,
    uses_local_buffers: bool,
    snapshot: &SnapshotData<'_>,
) -> PgResult<bool> {
    unsafe {
        if !SerializationNeededForRead(rd_id, uses_local_buffers, snapshot)? {
            return Ok(false);
        }
        if SxactIsDoomed(MySerializableXact()) {
            return Err(serialization_failure(
                "Canceled on identification as a pivot, during conflict out checking.",
            ));
        }
        Ok(true)
    }
}

pub fn CheckForSerializableConflictOut(
    rd_id: Oid,
    uses_local_buffers: bool,
    xid: TransactionId,
    snapshot: &SnapshotData<'_>,
) -> PgResult<()> {
    unsafe {
        if !SerializationNeededForRead(rd_id, uses_local_buffers, snapshot)? {
            return Ok(());
        }
        if SxactIsDoomed(MySerializableXact()) {
            return Err(serialization_failure(
                "Canceled on identification as a pivot, during conflict out checking.",
            ));
        }
        debug_assert!(TransactionIdIsValid(xid));

        if TransactionIdEquals(xid, xact_seams::get_top_transaction_id_if_any::call()) {
            return Ok(());
        }

        let sxidtag = SERIALIZABLEXIDTAG { xid };
        let procno = my_procno();
        LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE, procno)?;
        let sp = hash_search(
            shared().xid_hash,
            &raw const sxidtag as *const u8,
            HASH_FIND,
            None,
        )?;
        let sxid = sp as *mut SERIALIZABLEXID;
        let mysx = MySerializableXact();

        if sxid.is_null() {
            let conflictCommitSeqNo = SerialGetMinConflictCommitSeqNo(xid)?;
            if conflictCommitSeqNo != 0 {
                if conflictCommitSeqNo != InvalidSerCommitSeqNo
                    && (!SxactIsReadOnly(mysx)
                        || conflictCommitSeqNo <= (*mysx).SeqNo.lastCommitBeforeSnapshot)
                {
                    LWLockRelease(SerializableXactHashLock())?;
                    return Err(serialization_failure(&format!(
                        "Canceled on conflict out to old pivot {xid}."
                    )));
                }

                if SxactHasSummaryConflictIn(mysx)
                    || !dlist_is_empty(&raw const (*mysx).inConflicts)
                {
                    LWLockRelease(SerializableXactHashLock())?;
                    return Err(serialization_failure(&format!(
                        "Canceled on identification as a pivot, with conflict out to old committed transaction {xid}."
                    )));
                }

                (*mysx).flags |= SXACT_FLAG_SUMMARY_CONFLICT_OUT;
            }

            LWLockRelease(SerializableXactHashLock())?;
            return Ok(());
        }
        let sxact = (*sxid).myXact;
        debug_assert!(TransactionIdEquals((*sxact).topXid, xid));
        if sxact == mysx || SxactIsDoomed(sxact) {
            LWLockRelease(SerializableXactHashLock())?;
            return Ok(());
        }

        if SxactHasSummaryConflictOut(sxact) {
            if !SxactIsPrepared(sxact) {
                (*sxact).flags |= SXACT_FLAG_DOOMED;
                LWLockRelease(SerializableXactHashLock())?;
                return Ok(());
            } else {
                LWLockRelease(SerializableXactHashLock())?;
                return Err(serialization_failure("Canceled on conflict out to old pivot."));
            }
        }

        if SxactIsReadOnly(mysx)
            && SxactIsCommitted(sxact)
            && !SxactHasSummaryConflictOut(sxact)
            && (!SxactHasConflictOut(sxact)
                || (*mysx).SeqNo.lastCommitBeforeSnapshot
                    < (*sxact).SeqNo.earliestOutConflictCommit)
        {
            LWLockRelease(SerializableXactHashLock())?;
            return Ok(());
        }

        if !XidIsConcurrent(xid)? {
            LWLockRelease(SerializableXactHashLock())?;
            return Ok(());
        }

        if RWConflictExists(mysx, sxact) {
            LWLockRelease(SerializableXactHashLock())?;
            return Ok(());
        }

        // FlagRWConflict's failure path releases SerializableXactHashLock
        // itself (C's ereport longjmps past the release below); the release
        // here runs only on the Ok path.
        FlagRWConflict(mysx, sxact)?;
        LWLockRelease(SerializableXactHashLock())?;
        Ok(())
    }
}

unsafe fn CheckTargetForConflictsIn(targettag: *mut PREDICATELOCKTARGETTAG) -> PgResult<()> {
    debug_assert!(MySerializableXact() != InvalidSerializableXact);

    let procno = my_procno();
    let targettaghash = PredicateLockTargetTagHashCode(targettag);
    let partition_lock = PredicateLockHashPartitionLock(targettaghash);
    LWLockAcquire(partition_lock, LW_SHARED, procno)?;
    let tp = hash_search_with_hash_value(
        shared().target_hash,
        targettag as *const u8,
        targettaghash,
        HASH_FIND,
        None,
    )?;
    let target = tp as *mut PREDICATELOCKTARGET;
    if target.is_null() {
        LWLockRelease(partition_lock)?;
        return Ok(());
    }

    let mysx = MySerializableXact();
    let mut mypredlock: *mut PREDICATELOCK = ptr::null_mut();
    let mut mypredlocktag = PREDICATELOCKTAG {
        myTarget: ptr::null_mut(),
        myXact: ptr::null_mut(),
    };

    LWLockAcquire(SerializableXactHashLock(), LW_SHARED, procno)?;

    let head = &raw mut (*target).predicateLocks;
    let mut cur = (*head).head.next;
    while cur != (&raw mut (*head).head) as *mut dlist_node {
        let next = (*cur).next;
        let predlock = dlist_container!(PREDICATELOCK, targetLink, cur);
        let sxact = (*predlock).tag.myXact;

        if sxact == mysx {
            if !xact_seams::is_sub_transaction::call()
                && GET_PREDICATELOCKTARGETTAG_OFFSET(&*targettag) != 0
            {
                mypredlock = predlock;
                mypredlocktag = (*predlock).tag;
            }
        } else if !SxactIsDoomed(sxact)
            && (!SxactIsCommitted(sxact)
                || TransactionIdPrecedes(
                    snapmgr::GetTransactionSnapshot()?.xmin,
                    (*sxact).finishedBefore,
                ))
            && !RWConflictExists(sxact, mysx)
        {
            LWLockRelease(SerializableXactHashLock())?;
            LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE, procno)?;

            if !SxactIsDoomed(sxact)
                && (!SxactIsCommitted(sxact)
                    || TransactionIdPrecedes(
                        snapmgr::GetTransactionSnapshot()?.xmin,
                        (*sxact).finishedBefore,
                    ))
                && !RWConflictExists(sxact, mysx)
            {
                // Failure path: FlagRWConflict released the xact hash lock;
                // the partition lock rides out on LWLockReleaseAll at abort
                // (C's ereport longjmp does the same).
                FlagRWConflict(sxact, mysx)?;
            }

            LWLockRelease(SerializableXactHashLock())?;
            LWLockAcquire(SerializableXactHashLock(), LW_SHARED, procno)?;
        }
        cur = next;
    }
    LWLockRelease(SerializableXactHashLock())?;
    LWLockRelease(partition_lock)?;

    if !mypredlock.is_null() {
        LWLockAcquire(SerializablePredicateListLock(), LW_SHARED, procno)?;
        if in_parallel_mode() {
            LWLockAcquire(
                &(*MySerializableXact()).perXactPredicateListLock,
                LW_EXCLUSIVE,
                procno,
            )?;
        }
        LWLockAcquire(partition_lock, LW_EXCLUSIVE, procno)?;
        LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE, procno)?;

        let predlockhashcode =
            PredicateLockHashCodeFromTargetHashCode(&mypredlocktag, targettaghash);
        let rp = hash_search_with_hash_value(
            shared().lock_hash,
            &raw const mypredlocktag as *const u8,
            predlockhashcode,
            HASH_FIND,
            None,
        )?;
        let mut rmpredlock = rp as *mut PREDICATELOCK;
        if !rmpredlock.is_null() {
            debug_assert!(rmpredlock == mypredlock);

            dlist_delete(&raw mut (*mypredlock).targetLink);
            dlist_delete(&raw mut (*mypredlock).xactLink);

            let rp2 = hash_search_with_hash_value(
                shared().lock_hash,
                &raw const mypredlocktag as *const u8,
                predlockhashcode,
                HASH_REMOVE,
                None,
            )?;
            rmpredlock = rp2 as *mut PREDICATELOCK;
            debug_assert!(rmpredlock == mypredlock);

            RemoveTargetIfNoLongerUsed(target, targettaghash)?;
        }

        LWLockRelease(SerializableXactHashLock())?;
        LWLockRelease(partition_lock)?;
        if in_parallel_mode() {
            LWLockRelease(&(*MySerializableXact()).perXactPredicateListLock)?;
        }
        LWLockRelease(SerializablePredicateListLock())?;

        if !rmpredlock.is_null() {
            hash_search_with_hash_value(
                LocalPredicateLockHash(),
                targettag as *const u8,
                targettaghash,
                HASH_REMOVE,
                None,
            )?;
            DecrementParentLocks(targettag)?;
        }
    }
    Ok(())
}

pub fn CheckForSerializableConflictIn(
    db_oid: Oid,
    rd_id: Oid,
    uses_local_buffers: bool,
    tid: Option<(BlockNumber, OffsetNumber)>,
    blkno: BlockNumber,
) -> PgResult<()> {
    unsafe {
        if !SerializationNeededForWrite(rd_id, uses_local_buffers) {
            return Ok(());
        }
        if SxactIsDoomed(MySerializableXact()) {
            return Err(serialization_failure(
                "Canceled on identification as a pivot, during conflict in checking.",
            ));
        }

        set_MyXactDidWrite(true);

        let mut targettag = ZERO_TARGET_TAG;

        if let Some((tblk, toff)) = tid {
            SET_PREDICATELOCKTARGETTAG_TUPLE(&mut targettag, db_oid, rd_id, tblk, toff);
            CheckTargetForConflictsIn(&mut targettag)?;
        }

        if blkno != InvalidBlockNumber {
            SET_PREDICATELOCKTARGETTAG_PAGE(&mut targettag, db_oid, rd_id, blkno);
            CheckTargetForConflictsIn(&mut targettag)?;
        }

        SET_PREDICATELOCKTARGETTAG_RELATION(&mut targettag, db_oid, rd_id);
        CheckTargetForConflictsIn(&mut targettag)?;
        Ok(())
    }
}

pub fn TransferPredicateLocksToHeapRelation(
    db_id: Oid,
    rel_id: Oid,
    heap_id: Oid,
    uses_local_buffers: bool,
    is_index: bool,
) -> PgResult<()> {
    DropAllPredicateLocksFromTable(db_id, rel_id, heap_id, uses_local_buffers, is_index, true)
}

// Cannot raise a user-facing error: callers may not be serializable (C's
// "can't throw an error from here"); scratch-entry removal guarantees the
// heap-target HASH_ENTER succeeds, and each new lock entry is preceded by a
// removal so lock_hash cannot overflow.
fn DropAllPredicateLocksFromTable(
    db_id: Oid,
    rel_id: Oid,
    heap_id: Oid,
    uses_local_buffers: bool,
    is_index: bool,
    transfer: bool,
) -> PgResult<()> {
    unsafe {
        if !TransactionIdIsValid((*shared().pred_xact).SxactGlobalXmin) {
            return Ok(());
        }
        if !predicate_locking_needed(rel_id, uses_local_buffers) {
            return Ok(());
        }
        debug_assert!(heap_id != types_core::InvalidOid);
        debug_assert!(transfer || !is_index);

        let procno = my_procno();
        LWLockAcquire(SerializablePredicateListLock(), LW_EXCLUSIVE, procno)?;
        for i in 0..NUM_PREDICATELOCK_PARTITIONS {
            LWLockAcquire(PredicateLockHashPartitionLockByIndex(i), LW_EXCLUSIVE, procno)?;
        }
        LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE, procno)?;

        if transfer {
            RemoveScratchTarget(true)?;
        }

        let mut heaptargettaghash: u32 = 0;
        let mut heaptarget: *mut PREDICATELOCKTARGET = ptr::null_mut();

        let mut seqstat = HASH_SEQ_STATUS::new();
        hash_seq_init(&mut seqstat, shared().target_hash)?;

        let mut scan = |seqstat: &mut HASH_SEQ_STATUS| -> PgResult<()> {
            loop {
                let p = hash_seq_search(seqstat)?;
                if p.is_null() {
                    return Ok(());
                }
                let oldtarget = p as *mut PREDICATELOCKTARGET;

                if GET_PREDICATELOCKTARGETTAG_RELATION(&(*oldtarget).tag) != rel_id {
                    continue;
                }
                if GET_PREDICATELOCKTARGETTAG_DB(&(*oldtarget).tag) != db_id {
                    continue;
                }
                if transfer
                    && !is_index
                    && GET_PREDICATELOCKTARGETTAG_TYPE(&(*oldtarget).tag) == PREDLOCKTAG_RELATION
                {
                    continue;
                }

                if transfer && heaptarget.is_null() {
                    let mut heaptargettag = ZERO_TARGET_TAG;
                    SET_PREDICATELOCKTARGETTAG_RELATION(&mut heaptargettag, db_id, heap_id);
                    heaptargettaghash = PredicateLockTargetTagHashCode(&heaptargettag);
                    let mut found = false;
                    let hp = hash_search_with_hash_value(
                        shared().target_hash,
                        &raw const heaptargettag as *const u8,
                        heaptargettaghash,
                        HASH_ENTER,
                        Some(&mut found),
                    )?;
                    heaptarget = hp as *mut PREDICATELOCKTARGET;
                    if !found {
                        dlist_init(&raw mut (*heaptarget).predicateLocks);
                    }
                }

                let head = &raw mut (*oldtarget).predicateLocks;
                let mut cur = (*head).head.next;
                while cur != (&raw mut (*head).head) as *mut dlist_node {
                    let next = (*cur).next;
                    let oldpredlock = dlist_container!(PREDICATELOCK, targetLink, cur);
                    let oldCommitSeqNo = (*oldpredlock).commitSeqNo;
                    let oldXact = (*oldpredlock).tag.myXact;

                    dlist_delete(&raw mut (*oldpredlock).xactLink);
                    // No targetLink delete: the whole target is removed below.
                    let mut found = false;
                    let _ = hash_search(
                        shared().lock_hash,
                        &raw const (*oldpredlock).tag as *const u8,
                        HASH_REMOVE,
                        Some(&mut found),
                    )?;
                    debug_assert!(found);

                    if transfer {
                        let newpredlocktag = PREDICATELOCKTAG {
                            myTarget: heaptarget,
                            myXact: oldXact,
                        };
                        let mut found = false;
                        let npp = hash_search_with_hash_value(
                            shared().lock_hash,
                            &raw const newpredlocktag as *const u8,
                            PredicateLockHashCodeFromTargetHashCode(
                                &newpredlocktag,
                                heaptargettaghash,
                            ),
                            HASH_ENTER,
                            Some(&mut found),
                        )?;
                        let newpredlock = npp as *mut PREDICATELOCK;
                        if !found {
                            dlist_push_tail(
                                &raw mut (*heaptarget).predicateLocks,
                                &raw mut (*newpredlock).targetLink,
                            );
                            dlist_push_tail(
                                &raw mut (*oldXact).predicateLocks,
                                &raw mut (*newpredlock).xactLink,
                            );
                            (*newpredlock).commitSeqNo = oldCommitSeqNo;
                        } else if (*newpredlock).commitSeqNo < oldCommitSeqNo {
                            (*newpredlock).commitSeqNo = oldCommitSeqNo;
                        }

                        debug_assert!((*newpredlock).commitSeqNo != 0);
                        debug_assert!(
                            (*newpredlock).commitSeqNo == InvalidSerCommitSeqNo
                                || (*newpredlock).tag.myXact == shared().old_committed_sxact
                        );
                    }
                    cur = next;
                }

                let mut found = false;
                let _ = hash_search(
                    shared().target_hash,
                    &raw const (*oldtarget).tag as *const u8,
                    HASH_REMOVE,
                    Some(&mut found),
                )?;
                debug_assert!(found);
            }
        };
        if let Err(e) = scan(&mut seqstat) {
            let _ = dynahash::hash_seq_term(&mut seqstat);
            return Err(e);
        }

        if transfer {
            RestoreScratchTarget(true)?;
        }

        LWLockRelease(SerializableXactHashLock())?;
        for i in (0..NUM_PREDICATELOCK_PARTITIONS).rev() {
            LWLockRelease(PredicateLockHashPartitionLockByIndex(i))?;
        }
        LWLockRelease(SerializablePredicateListLock())?;
        Ok(())
    }
}

pub fn CheckTableForSerializableConflictIn(
    db_id: Oid,
    heap_id: Oid,
    rd_id: Oid,
    uses_local_buffers: bool,
) -> PgResult<()> {
    unsafe {
        if !TransactionIdIsValid((*shared().pred_xact).SxactGlobalXmin) {
            return Ok(());
        }
        if !SerializationNeededForWrite(rd_id, uses_local_buffers) {
            return Ok(());
        }
        set_MyXactDidWrite(true);

        let mysx = MySerializableXact();
        let procno = my_procno();

        LWLockAcquire(SerializablePredicateListLock(), LW_EXCLUSIVE, procno)?;
        for i in 0..NUM_PREDICATELOCK_PARTITIONS {
            LWLockAcquire(PredicateLockHashPartitionLockByIndex(i), LW_SHARED, procno)?;
        }
        LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE, procno)?;

        let mut seqstat = HASH_SEQ_STATUS::new();
        hash_seq_init(&mut seqstat, shared().target_hash)?;

        loop {
            let p = hash_seq_search(&mut seqstat)?;
            if p.is_null() {
                break;
            }
            let target = p as *mut PREDICATELOCKTARGET;

            if GET_PREDICATELOCKTARGETTAG_RELATION(&(*target).tag) != heap_id {
                continue;
            }
            if GET_PREDICATELOCKTARGETTAG_DB(&(*target).tag) != db_id {
                continue;
            }

            let head = &raw mut (*target).predicateLocks;
            let mut cur = (*head).head.next;
            while cur != (&raw mut (*head).head) as *mut dlist_node {
                let next = (*cur).next;
                let predlock = dlist_container!(PREDICATELOCK, targetLink, cur);
                if (*predlock).tag.myXact != mysx
                    && !RWConflictExists((*predlock).tag.myXact, mysx)
                {
                    // Failure path: xact hash lock released by FlagRWConflict;
                    // the seq scan must not stay registered past the error.
                    if let Err(e) = FlagRWConflict((*predlock).tag.myXact, mysx) {
                        let _ = dynahash::hash_seq_term(&mut seqstat);
                        return Err(e);
                    }
                }
                cur = next;
            }
        }

        LWLockRelease(SerializableXactHashLock())?;
        for i in (0..NUM_PREDICATELOCK_PARTITIONS).rev() {
            LWLockRelease(PredicateLockHashPartitionLockByIndex(i))?;
        }
        LWLockRelease(SerializablePredicateListLock())?;
        Ok(())
    }
}

unsafe fn FlagRWConflict(
    reader: *mut SERIALIZABLEXACT,
    writer: *mut SERIALIZABLEXACT,
) -> PgResult<()> {
    debug_assert!(reader != writer);

    OnConflict_CheckForSerializationFailure(reader, writer)?;

    if reader == shared().old_committed_sxact {
        (*writer).flags |= SXACT_FLAG_SUMMARY_CONFLICT_IN;
    } else if writer == shared().old_committed_sxact {
        (*reader).flags |= SXACT_FLAG_SUMMARY_CONFLICT_OUT;
    } else {
        SetRWConflict(reader, writer)?;
    }
    Ok(())
}

unsafe fn OnConflict_CheckForSerializationFailure(
    reader: *const SERIALIZABLEXACT,
    writer: *mut SERIALIZABLEXACT,
) -> PgResult<()> {
    debug_assert!(LWLockHeldByMe(SerializableXactHashLock()));

    let mut failure = false;

    if SxactIsCommitted(writer)
        && (SxactHasConflictOut(writer) || SxactHasSummaryConflictOut(writer))
    {
        failure = true;
    }

    if !failure && SxactHasSummaryConflictOut(writer) {
        failure = true;
    } else if !failure {
        let head = &raw const (*writer).outConflicts;
        let mut cur = (*head).head.next;
        while cur != (&raw const (*head).head) as *mut dlist_node {
            let conflict = dlist_container!(RWConflictData, outLink, cur);
            let t2 = (*conflict).sxactIn;

            if SxactIsPrepared(t2)
                && (!SxactIsCommitted(reader) || (*t2).prepareSeqNo <= (*reader).commitSeqNo)
                && (!SxactIsCommitted(writer) || (*t2).prepareSeqNo <= (*writer).commitSeqNo)
                && (!SxactIsReadOnly(reader)
                    || (*t2).prepareSeqNo <= (*reader).SeqNo.lastCommitBeforeSnapshot)
            {
                failure = true;
                break;
            }
            cur = (*cur).next;
        }
    }

    if !failure && SxactIsPrepared(writer) && !SxactIsReadOnly(reader) {
        if SxactHasSummaryConflictIn(reader) {
            failure = true;
        } else {
            let head = &raw const (*reader).inConflicts;
            let mut cur = (*head).head.next;
            while cur != (&raw const (*head).head) as *mut dlist_node {
                let conflict = dlist_container!(RWConflictData, inLink, cur);
                let t0 = (*conflict).sxactOut;

                if !SxactIsDoomed(t0)
                    && (!SxactIsCommitted(t0) || (*t0).commitSeqNo >= (*writer).prepareSeqNo)
                    && (!SxactIsReadOnly(t0)
                        || (*t0).SeqNo.lastCommitBeforeSnapshot >= (*writer).prepareSeqNo)
                {
                    failure = true;
                    break;
                }
                cur = (*cur).next;
            }
        }
    }

    if failure {
        if MySerializableXact() == writer {
            LWLockRelease(SerializableXactHashLock())?;
            return Err(serialization_failure(
                "Canceled on identification as a pivot, during write.",
            ));
        } else if SxactIsPrepared(writer) {
            LWLockRelease(SerializableXactHashLock())?;
            debug_assert!(MySerializableXact() == reader as *mut SERIALIZABLEXACT);
            return Err(serialization_failure(&format!(
                "Canceled on conflict out to pivot {}, during read.",
                (*writer).topXid
            )));
        }
        (*writer).flags |= SXACT_FLAG_DOOMED;
    }
    Ok(())
}

pub fn PreCommit_CheckForSerializationFailure() -> PgResult<()> {
    unsafe {
        if MySerializableXact() == InvalidSerializableXact {
            return Ok(());
        }
        debug_assert!(xact_seams::isolation_is_serializable::call());

        let procno = my_procno();
        LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE, procno)?;

        let mysx = MySerializableXact();
        if SxactIsDoomed(mysx) && !SxactIsPartiallyReleased(mysx) {
            LWLockRelease(SerializableXactHashLock())?;
            return Err(serialization_failure(
                "Canceled on identification as a pivot, during commit attempt.",
            ));
        }

        let head = &raw const (*mysx).inConflicts;
        let mut near = (*head).head.next;
        while near != (&raw const (*head).head) as *mut dlist_node {
            let nearConflict = dlist_container!(RWConflictData, inLink, near);

            if !SxactIsCommitted((*nearConflict).sxactOut)
                && !SxactIsDoomed((*nearConflict).sxactOut)
            {
                let fhead = &raw const (*(*nearConflict).sxactOut).inConflicts;
                let mut far = (*fhead).head.next;
                while far != (&raw const (*fhead).head) as *mut dlist_node {
                    let farConflict = dlist_container!(RWConflictData, inLink, far);
                    if (*farConflict).sxactOut == mysx
                        || (!SxactIsCommitted((*farConflict).sxactOut)
                            && !SxactIsReadOnly((*farConflict).sxactOut)
                            && !SxactIsDoomed((*farConflict).sxactOut))
                    {
                        if SxactIsPrepared((*nearConflict).sxactOut) {
                            LWLockRelease(SerializableXactHashLock())?;
                            return Err(serialization_failure(
                                "Canceled on commit attempt with conflict in from prepared pivot.",
                            ));
                        }
                        (*(*nearConflict).sxactOut).flags |= SXACT_FLAG_DOOMED;
                        break;
                    }
                    far = (*far).next;
                }
            }
            near = (*near).next;
        }

        let px = shared().pred_xact;
        (*px).LastSxactCommitSeqNo += 1;
        (*mysx).prepareSeqNo = (*px).LastSxactCommitSeqNo;
        (*mysx).flags |= SXACT_FLAG_PREPARED;

        LWLockRelease(SerializableXactHashLock())?;
        Ok(())
    }
}

// ===========================================================================
// Two-phase commit support (predicate.c AtPrepare_PredicateLocks,
// PostPrepare_PredicateLocks, PredicateLockTwoPhaseFinish,
// predicatelock_twophase_recover).
// ===========================================================================

// Test-only sxact acquisition: GetSerializableTransactionSnapshotInt's
// registration flow with a caller-supplied xmin instead of GetSnapshotData
// (the tests have no procarray-backed snapshot machinery).
#[cfg(test)]
pub(crate) fn test_acquire_sxact(xmin: TransactionId) -> PgResult<()> {
    unsafe {
        assert!(MySerializableXact() == InvalidSerializableXact);
        let procno = my_procno();
        LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE, procno)?;
        let sxact = CreatePredXact();
        assert!(!sxact.is_null());
        let px = shared().pred_xact;

        (*sxact).vxid = my_proc_vxid();
        (*sxact).SeqNo.lastCommitBeforeSnapshot = (*px).LastSxactCommitSeqNo;
        (*sxact).prepareSeqNo = InvalidSerCommitSeqNo;
        (*sxact).commitSeqNo = InvalidSerCommitSeqNo;
        dlist_init(&raw mut (*sxact).outConflicts);
        dlist_init(&raw mut (*sxact).inConflicts);
        dlist_init(&raw mut (*sxact).possibleUnsafeConflicts);
        (*sxact).topXid = InvalidTransactionId;
        (*sxact).finishedBefore = InvalidTransactionId;
        (*sxact).xmin = xmin;
        (*sxact).pid = init_small::globals::MyProcPid();
        (*sxact).pgprocno = procno;
        dlist_init(&raw mut (*sxact).predicateLocks);
        dlist_node_init(&raw mut (*sxact).finishedLink);
        (*sxact).flags = 0;

        (*px).WritableSxactCount += 1;

        if !TransactionIdIsValid((*px).SxactGlobalXmin) {
            (*px).SxactGlobalXmin = xmin;
            (*px).SxactGlobalXminCount = 1;
            SerialSetActiveSerXmin(xmin)?;
        } else if TransactionIdEquals(xmin, (*px).SxactGlobalXmin) {
            (*px).SxactGlobalXminCount += 1;
        }

        set_MySerializableXact(sxact);
        set_MyXactDidWrite(false);
        LWLockRelease(SerializableXactHashLock())?;
        CreateLocalPredicateLockHash()
    }
}

#[cfg(test)]
pub(crate) fn test_my_sxact() -> *mut SERIALIZABLEXACT {
    MySerializableXact()
}

// TwoPhasePredicateRecord (predicate_internals.h): a 4-byte type tag followed
// by a union sized to the larger member (the 20-byte lock record). The whole
// 24-byte struct is written by RegisterTwoPhaseRecord for BOTH record kinds;
// predicatelock_twophase_recover asserts len == sizeof(record) == 24.
const TWOPHASEPREDICATERECORD_XACT: u32 = 0;
const TWOPHASEPREDICATERECORD_LOCK: u32 = 1;
const SIZEOF_TWOPHASE_PREDICATE_RECORD: usize = 24;

// twophase_rmgr's TWOPHASE_RM_PREDICATELOCK_ID, mirrored locally to avoid a
// predicate -> twophase_rmgr -> predicate dependency cycle (lock/twophase.rs
// mirrors TWOPHASE_RM_LOCK_ID for the same reason).
const TWOPHASE_RM_PREDICATELOCK_ID: u8 = 4;

#[track_caller]
#[cold]
fn recover_out_of_shared_memory() -> Box<PgError> {
    // predicatelock_twophase_recover's plain form (no per-xact-locks hint).
    Box::new(PgError::error("out of shared memory").with_sqlstate(ERRCODE_OUT_OF_MEMORY))
}

// AtPrepare_PredicateLocks: write 2PC statefile records for the current
// SERIALIZABLEXACT and each predicate lock it holds, so a post-crash recovery
// can rebuild the SSI state. The in-memory sxact itself is NOT torn down here;
// it lives on (unowned) for the no-crash COMMIT/ROLLBACK PREPARED path.
pub fn AtPrepare_PredicateLocks() -> PgResult<()> {
    unsafe {
        let sxact = MySerializableXact();
        if sxact == InvalidSerializableXact {
            return Ok(());
        }

        // Per-transaction record. Conflicts (in/out lists) are deliberately not
        // serialized: new conflicts can still form after PREPARE, so recovery
        // makes the conservative summary-conflict assumption instead.
        let mut record = [0u8; SIZEOF_TWOPHASE_PREDICATE_RECORD];
        record[0..4].copy_from_slice(&TWOPHASEPREDICATERECORD_XACT.to_ne_bytes());
        record[4..8].copy_from_slice(&(*sxact).xmin.to_ne_bytes());
        record[8..12].copy_from_slice(&(*sxact).flags.to_ne_bytes());
        twophase_seams::register_two_phase_record::call(
            TWOPHASE_RM_PREDICATELOCK_ID,
            0,
            &record,
        )?;

        // One lock record per predicate lock. Walk the sxact's own lock list
        // (the local lock table is not authoritative). No perXactPredicateListLock
        // needed: no parallel worker can run while we PREPARE.
        let procno = my_procno();
        LWLockAcquire(SerializablePredicateListLock(), LW_SHARED, procno)?;
        debug_assert!(!is_parallel_worker() && !in_parallel_mode());

        let head = &raw const (*sxact).predicateLocks;
        let mut cur = (*head).head.next;
        let mut register_err: PgResult<()> = Ok(());
        while cur != (&raw const (*head).head) as *mut dlist_node {
            let predlock = dlist_container!(PREDICATELOCK, xactLink, cur);
            let target = (*predlock).tag.myTarget;
            let targettag = (*target).tag;

            let mut lrec = [0u8; SIZEOF_TWOPHASE_PREDICATE_RECORD];
            lrec[0..4].copy_from_slice(&TWOPHASEPREDICATERECORD_LOCK.to_ne_bytes());
            lrec[4..8].copy_from_slice(&targettag.locktag_field1.to_ne_bytes());
            lrec[8..12].copy_from_slice(&targettag.locktag_field2.to_ne_bytes());
            lrec[12..16].copy_from_slice(&targettag.locktag_field3.to_ne_bytes());
            lrec[16..20].copy_from_slice(&targettag.locktag_field4.to_ne_bytes());
            // bytes 20..24 = filler, left zero.

            if let Err(e) = twophase_seams::register_two_phase_record::call(
                TWOPHASE_RM_PREDICATELOCK_ID,
                0,
                &lrec,
            ) {
                register_err = Err(e);
                break;
            }
            cur = (*cur).next;
        }

        LWLockRelease(SerializablePredicateListLock())?;
        register_err
    }
}

// PostPrepare_PredicateLocks: clean up local state after a successful PREPARE.
// Unlike the heavyweight lock manager we do NOT transfer locks to a dummy proc
// — the SERIALIZABLEXACT stays around anyway (now unowned: pid/pgprocno cleared)
// and its shared predicate locks keep pointing at it so conflicts still fire.
pub fn PostPrepare_PredicateLocks(_xid: TransactionId) -> PgResult<()> {
    unsafe {
        let sxact = MySerializableXact();
        if sxact == InvalidSerializableXact {
            return Ok(());
        }
        debug_assert!(SxactIsPrepared(sxact));

        (*sxact).pid = 0;
        (*sxact).pgprocno = types_core::INVALID_PROC_NUMBER;

        // hash_destroy(LocalPredicateLockHash) + clear MySerializableXact/MyXactDidWrite.
        ReleasePredicateLocksLocal();
        Ok(())
    }
}

// PredicateLockTwoPhaseFinish: release a prepared transaction's predicate locks
// once it commits or aborts. Finds the recovered/handed-off SERIALIZABLEXACT by
// xid and routes it through ReleasePredicateLocks. The finishing backend's own
// MySerializableXact is clobbered (COMMIT/ROLLBACK PREPARED runs outside any
// serializable transaction, so it is Invalid), exactly as C does.
pub fn PredicateLockTwoPhaseFinish(xid: TransactionId, isCommit: bool) -> PgResult<()> {
    unsafe {
        let sxidtag = SERIALIZABLEXIDTAG { xid };
        let procno = my_procno();

        LWLockAcquire(SerializableXactHashLock(), LW_SHARED, procno)?;
        let p = hash_search(
            shared().xid_hash,
            &raw const sxidtag as *const u8,
            HASH_FIND,
            None,
        )?;
        LWLockRelease(SerializableXactHashLock())?;

        // Not found = it wasn't a serializable transaction; nothing to do.
        if p.is_null() {
            return Ok(());
        }
        let sxid = p as *mut SERIALIZABLEXID;

        set_MySerializableXact((*sxid).myXact);
        set_MyXactDidWrite(true); // conservatively assume it wrote something
        ReleasePredicateLocks(isCommit, false)
    }
}

// predicatelock_twophase_recover: the TWOPHASE_RM_PREDICATELOCK_ID rmgr callback.
// Rebuilds a SERIALIZABLEXACT (per-xact record) and re-acquires each predicate
// lock (per-lock record) at recovery / COMMIT PREPARED / ROLLBACK PREPARED time.
pub fn predicatelock_twophase_recover(
    xid: TransactionId,
    _info: u16,
    recdata: &[u8],
) -> PgResult<()> {
    unsafe {
        debug_assert_eq!(recdata.len(), SIZEOF_TWOPHASE_PREDICATE_RECORD);
        let rtype = u32::from_ne_bytes(recdata[0..4].try_into().unwrap());
        debug_assert!(
            rtype == TWOPHASEPREDICATERECORD_XACT || rtype == TWOPHASEPREDICATERECORD_LOCK
        );
        let procno = my_procno();

        if rtype == TWOPHASEPREDICATERECORD_XACT {
            // Per-transaction record: set up a SERIALIZABLEXACT.
            let xmin = u32::from_ne_bytes(recdata[4..8].try_into().unwrap());
            let flags = u32::from_ne_bytes(recdata[8..12].try_into().unwrap());

            LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE, procno)?;
            let sxact = CreatePredXact();
            if sxact.is_null() {
                LWLockRelease(SerializableXactHashLock())?;
                return Err(recover_out_of_shared_memory());
            }

            // A prepared xact has an invalid vxid (proc gone) but keeps its xid.
            (*sxact).vxid.procNumber = types_core::INVALID_PROC_NUMBER;
            (*sxact).vxid.localTransactionId = xid;
            (*sxact).pid = 0;
            (*sxact).pgprocno = types_core::INVALID_PROC_NUMBER;

            // Hasn't committed yet.
            (*sxact).prepareSeqNo = RecoverySerCommitSeqNo;
            (*sxact).commitSeqNo = InvalidSerCommitSeqNo;
            (*sxact).finishedBefore = InvalidTransactionId;
            (*sxact).SeqNo.lastCommitBeforeSnapshot = RecoverySerCommitSeqNo;

            // No need to track possible-unsafe conflicts across recovery.
            dlist_init(&raw mut (*sxact).possibleUnsafeConflicts);
            dlist_init(&raw mut (*sxact).predicateLocks);
            dlist_node_init(&raw mut (*sxact).finishedLink);

            (*sxact).topXid = xid;
            (*sxact).xmin = xmin;
            (*sxact).flags = flags;
            debug_assert!(SxactIsPrepared(sxact));

            let px = shared().pred_xact;
            if !SxactIsReadOnly(sxact) {
                (*px).WritableSxactCount += 1;
                debug_assert!(
                    (*px).WritableSxactCount as i64
                        <= init_small::globals::MaxBackends() as i64
                            + shared().max_prepared_xacts as i64
                );
            }

            // We don't know the real conflict lists; assume both a conflict in
            // and a conflict out via the summary flags.
            dlist_init(&raw mut (*sxact).outConflicts);
            dlist_init(&raw mut (*sxact).inConflicts);
            (*sxact).flags |= SXACT_FLAG_SUMMARY_CONFLICT_IN;
            (*sxact).flags |= SXACT_FLAG_SUMMARY_CONFLICT_OUT;

            // Register the xid.
            let sxidtag = SERIALIZABLEXIDTAG { xid };
            let mut found = false;
            let sp = hash_search(
                shared().xid_hash,
                &raw const sxidtag as *const u8,
                HASH_ENTER,
                Some(&mut found),
            )?;
            debug_assert!(!sp.is_null());
            debug_assert!(!found);
            let sxid = sp as *mut SERIALIZABLEXID;
            (*sxid).myXact = sxact;

            // Update global xmin. This is the one case where it may go backwards
            // (recovery installs prepared xacts before any new ones start), which
            // is fine — nothing completes or is thrown away until recovery ends.
            if !TransactionIdIsValid((*px).SxactGlobalXmin)
                || TransactionIdFollows((*px).SxactGlobalXmin, (*sxact).xmin)
            {
                (*px).SxactGlobalXmin = (*sxact).xmin;
                (*px).SxactGlobalXminCount = 1;
                SerialSetActiveSerXmin((*sxact).xmin)?;
            } else if TransactionIdEquals((*sxact).xmin, (*px).SxactGlobalXmin) {
                debug_assert!((*px).SxactGlobalXminCount > 0);
                (*px).SxactGlobalXminCount += 1;
            }

            LWLockRelease(SerializableXactHashLock())?;
        } else {
            // Per-lock record: recreate the PREDICATELOCK.
            let mut target = ZERO_TARGET_TAG;
            target.locktag_field1 = u32::from_ne_bytes(recdata[4..8].try_into().unwrap());
            target.locktag_field2 = u32::from_ne_bytes(recdata[8..12].try_into().unwrap());
            target.locktag_field3 = u32::from_ne_bytes(recdata[12..16].try_into().unwrap());
            target.locktag_field4 = u32::from_ne_bytes(recdata[16..20].try_into().unwrap());
            let targettaghash = PredicateLockTargetTagHashCode(&target);

            LWLockAcquire(SerializableXactHashLock(), LW_SHARED, procno)?;
            let sxidtag = SERIALIZABLEXIDTAG { xid };
            let sp = hash_search(
                shared().xid_hash,
                &raw const sxidtag as *const u8,
                HASH_FIND,
                None,
            )?;
            LWLockRelease(SerializableXactHashLock())?;

            debug_assert!(!sp.is_null());
            let sxid = sp as *mut SERIALIZABLEXID;
            let sxact = (*sxid).myXact;
            debug_assert!(sxact != InvalidSerializableXact);

            CreatePredicateLock(&target, targettaghash, sxact)?;
        }
        Ok(())
    }
}

pub struct PredicateLockStatusEntry {
    pub tag: PREDICATELOCKTARGETTAG,
    pub vxid: VirtualTransactionId,
    pub pid: i32,
}

pub fn GetPredicateLockStatusData() -> PgResult<Vec<PredicateLockStatusEntry>> {
    let procno = my_procno();
    // Consistency: all partition locks ascending, then SerializableXactHashLock.
    for i in 0..NUM_PREDICATELOCK_PARTITIONS {
        LWLockAcquire(PredicateLockHashPartitionLockByIndex(i), LW_SHARED, procno)?;
    }
    LWLockAcquire(SerializableXactHashLock(), LW_SHARED, procno)?;

    let els = dynahash::hash_get_num_entries(shared().lock_hash) as usize;
    let mut entries = Vec::with_capacity(els);

    let mut seqstat = HASH_SEQ_STATUS::new();
    hash_seq_init(&mut seqstat, shared().lock_hash)?;
    loop {
        let predlock = hash_seq_search(&mut seqstat)? as *mut PREDICATELOCK;
        if predlock.is_null() {
            break;
        }
        // SAFETY: partition + SerializableXactHashLock held; target and sxact
        // entries referenced by a live PREDICATELOCK are pinned.
        unsafe {
            let sxact = (*predlock).tag.myXact;
            entries.push(PredicateLockStatusEntry {
                tag: (*(*predlock).tag.myTarget).tag,
                vxid: (*sxact).vxid,
                pid: (*sxact).pid,
            });
        }
    }
    debug_assert_eq!(entries.len(), els);

    LWLockRelease(SerializableXactHashLock())?;
    for i in (0..NUM_PREDICATELOCK_PARTITIONS).rev() {
        LWLockRelease(PredicateLockHashPartitionLockByIndex(i))?;
    }
    Ok(entries)
}

pub fn GetSafeSnapshotBlockingPids(blocked_pid: i32, output_size: usize) -> PgResult<Vec<i32>> {
    let procno = my_procno();
    let mut pids = Vec::new();
    LWLockAcquire(SerializableXactHashLock(), LW_SHARED, procno)?;

    // SAFETY: SerializableXactHashLock held shared pins the active list and
    // each sxact's possibleUnsafeConflicts.
    unsafe {
        let px = shared().pred_xact;
        let head = &raw const (*px).activeList;
        let mut cur = (*head).head.next;
        let mut blocking_sxact: *mut SERIALIZABLEXACT = ptr::null_mut();
        while cur != (&raw const (*head).head) as *mut dlist_node {
            let sxact = dlist_container!(SERIALIZABLEXACT, xactLink, cur);
            if (*sxact).pid == blocked_pid {
                blocking_sxact = sxact;
                break;
            }
            cur = (*cur).next;
        }

        if !blocking_sxact.is_null()
            && (*blocking_sxact).flags & SXACT_FLAG_DEFERRABLE_WAITING != 0
        {
            let head = &raw const (*blocking_sxact).possibleUnsafeConflicts;
            let mut cur = (*head).head.next;
            while cur != (&raw const (*head).head) as *mut dlist_node {
                let conflict = dlist_container!(RWConflictData, inLink, cur);
                pids.push((*(*conflict).sxactOut).pid);
                if pids.len() >= output_size {
                    break;
                }
                cur = (*cur).next;
            }
        }
    }

    LWLockRelease(SerializableXactHashLock())?;
    Ok(pids)
}
