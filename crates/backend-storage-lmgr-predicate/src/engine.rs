//! The SSI predicate-locking engine — a faithful, raw-pointer transliteration
//! of `storage/lmgr/predicate.c` over the real shmem/dynahash/LWLock/SLRU
//! substrate. The shmem-resident structs are woven with the intrusive `dlist`
//! helpers from `ilist_inline`; the file-global pointers mirror predicate.c's
//! `static` globals.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::missing_safety_doc)]

use core::cell::Cell;
use core::ptr;

use backend_access_transam_transam::{
    TransactionIdEquals, TransactionIdFollows, TransactionIdFollowsOrEquals, TransactionIdIsValid,
    TransactionIdPrecedes, TransactionIdPrecedesOrEquals,
};
use backend_storage_ipc_shmem::{
    add_size, mul_size, ShmemAddrIsValid, ShmemInitHash, ShmemInitStruct,
};
use backend_storage_lmgr_lwlock::{
    LWLockAcquire, LWLockHeldByMe, LWLockHeldByMeInMode, LWLockInitialize, LWLockRelease,
};
use types_storage::{LWLock, LW_EXCLUSIVE, LW_SHARED};
use backend_utils_hash_dynahash::{
    get_hash_value, hash_create, hash_destroy, hash_estimate_size, hash_get_num_entries,
    hash_search, hash_search_with_hash_value, hash_seq_init, hash_seq_search,
};
use types_core::primitive::{BlockNumber, Oid, Size};
use types_core::xact::InvalidTransactionId;
use types_core::TransactionId;
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERRCODE_OUT_OF_MEMORY, ERRCODE_T_R_SERIALIZATION_FAILURE,
};
use types_hash::hsearch::{
    HASHACTION::{HASH_ENTER, HASH_ENTER_NULL, HASH_FIND, HASH_REMOVE},
    HASHCTL, HASH_BLOBS, HASH_ELEM, HASH_FIXED_SIZE, HASH_FUNCTION, HASH_PARTITION, HASH_SEQ_STATUS,
    HTAB,
};
use types_snapshot::snapshot::{IsMVCCSnapshot, SnapshotData};

use crate::globals::*;
use crate::internals::*;
use crate::serial::{
    SerialAdd, SerialGetMinConflictCommitSeqNo, SerialInit, SerialSetActiveSerXmin,
    SERIAL_CONTROL_FOUND,
};
use backend_lib_ilist::{dlist_head, dlist_node};

// ===========================================================================
// File globals (predicate.c statics).
// ===========================================================================

thread_local! {
    static SERIALIZABLE_XID_HASH: Cell<*mut HTAB> = const { Cell::new(ptr::null_mut()) };
    static PREDICATE_LOCK_TARGET_HASH: Cell<*mut HTAB> = const { Cell::new(ptr::null_mut()) };
    static PREDICATE_LOCK_HASH: Cell<*mut HTAB> = const { Cell::new(ptr::null_mut()) };
    static FINISHED_SERIALIZABLE_TRANSACTIONS: Cell<*mut dlist_head> = const { Cell::new(ptr::null_mut()) };
    static PRED_XACT: Cell<PredXactList> = const { Cell::new(ptr::null_mut()) };
    static RW_CONFLICT_POOL: Cell<RWConflictPoolHeader> = const { Cell::new(ptr::null_mut()) };
    static OLD_COMMITTED_SXACT: Cell<*mut SERIALIZABLEXACT> = const { Cell::new(ptr::null_mut()) };
    static LOCAL_PREDICATE_LOCK_HASH: Cell<*mut HTAB> = const { Cell::new(ptr::null_mut()) };
    static MY_SERIALIZABLE_XACT: Cell<*mut SERIALIZABLEXACT> = const { Cell::new(ptr::null_mut()) };
    static MY_XACT_DID_WRITE: Cell<bool> = const { Cell::new(false) };
    static SAVED_SERIALIZABLE_XACT: Cell<*mut SERIALIZABLEXACT> = const { Cell::new(ptr::null_mut()) };
    static SCRATCH_TARGET_TAG_HASH: Cell<u32> = const { Cell::new(0) };
    static IS_UNDER_POSTMASTER: Cell<bool> = const { Cell::new(false) };
}

#[inline]
fn SerializableXidHash() -> *mut HTAB {
    SERIALIZABLE_XID_HASH.with(|c| c.get())
}
#[inline]
fn PredicateLockTargetHash() -> *mut HTAB {
    PREDICATE_LOCK_TARGET_HASH.with(|c| c.get())
}
#[inline]
fn PredicateLockHash() -> *mut HTAB {
    PREDICATE_LOCK_HASH.with(|c| c.get())
}
#[inline]
fn FinishedSerializableTransactions() -> *mut dlist_head {
    FINISHED_SERIALIZABLE_TRANSACTIONS.with(|c| c.get())
}
#[inline]
fn PredXact() -> PredXactList {
    PRED_XACT.with(|c| c.get())
}
#[inline]
fn RWConflictPool() -> RWConflictPoolHeader {
    RW_CONFLICT_POOL.with(|c| c.get())
}
#[inline]
fn OldCommittedSxact() -> *mut SERIALIZABLEXACT {
    OLD_COMMITTED_SXACT.with(|c| c.get())
}
#[inline]
fn LocalPredicateLockHash() -> *mut HTAB {
    LOCAL_PREDICATE_LOCK_HASH.with(|c| c.get())
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

/// `static const PREDICATELOCKTARGETTAG ScratchTargetTag = {0,0,0,0};`
static SCRATCH_TARGET_TAG: PREDICATELOCKTARGETTAG = PREDICATELOCKTARGETTAG {
    locktag_field1: 0,
    locktag_field2: 0,
    locktag_field3: 0,
    locktag_field4: 0,
};

#[inline]
fn ScratchTargetTagHash() -> u32 {
    SCRATCH_TARGET_TAG_HASH.with(|c| c.get())
}
#[inline]
fn ScratchPartitionLock() -> &'static LWLock {
    PredicateLockHashPartitionLock(ScratchTargetTagHash())
}

// ===========================================================================
// Hash / partition arithmetic macros (predicate.c).
// ===========================================================================

/// `PredicateLockTargetTagHashCode(tag)`.
#[inline]
unsafe fn PredicateLockTargetTagHashCode(tag: *const PREDICATELOCKTARGETTAG) -> u32 {
    get_hash_value(PredicateLockTargetHash(), tag as *const u8)
}

/// `PredicateLockHashCodeFromTargetHashCode(predicatelocktag, targethash)`.
#[inline]
unsafe fn PredicateLockHashCodeFromTargetHashCode(
    predicatelocktag: *const PREDICATELOCKTAG,
    targethash: u32,
) -> u32 {
    targethash ^ (((*predicatelocktag).myXact as usize as u32) << types_storage::LOG2_NUM_PREDICATELOCK_PARTITIONS)
}

/// `NPREDICATELOCKTARGETENTS()`.
fn NPREDICATELOCKTARGETENTS() -> PgResult<Size> {
    mul_size(
        max_predicate_locks_per_xact() as Size,
        add_size(max_backends() as Size, max_prepared_xacts() as Size)?,
    )
}

// ===========================================================================
// errors
// ===========================================================================

fn out_of_shared_memory() -> PgError {
    PgError::error("out of shared memory")
        .with_sqlstate(ERRCODE_OUT_OF_MEMORY)
        .with_hint(format!(
            "You might need to increase \"{}\".",
            "max_pred_locks_per_transaction"
        ))
}

fn serialization_failure(reason: &str) -> PgError {
    PgError::error("could not serialize access due to read/write dependencies among transactions")
        .with_sqlstate(ERRCODE_T_R_SERIALIZATION_FAILURE)
        .with_detail(format!("Reason code: {reason}"))
        .with_hint("The transaction might succeed if retried.")
}

// ===========================================================================
// Predicate-locking gating (PredicateLockingNeededForRelation etc.).
// ===========================================================================

/// `PredicateLockingNeededForRelation(relation)` — over the (dbOid,relOid)
/// already projected, plus the temp-buffer / system-relation tests resolved by
/// the caller. The two flags `rd_id < FirstUnpinnedObjectId` and
/// `RelationUsesLocalBuffers` are passed in pre-computed.
#[inline]
fn predicate_locking_needed(rd_id: Oid, uses_local_buffers: bool) -> bool {
    !(rd_id < types_core::catalog::FirstUnpinnedObjectId || uses_local_buffers)
}

/// `SerializationNeededForRead(relation, snapshot)`. Returns whether predicate
/// locking applies; has the C side-effect of releasing locks when RO-safe.
unsafe fn SerializationNeededForRead(
    rd_id: Oid,
    uses_local_buffers: bool,
    snapshot: &SnapshotData,
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

/// `SerializationNeededForWrite(relation)`.
unsafe fn SerializationNeededForWrite(rd_id: Oid, uses_local_buffers: bool) -> bool {
    if MySerializableXact() == InvalidSerializableXact {
        return false;
    }
    if !predicate_locking_needed(rd_id, uses_local_buffers) {
        return false;
    }
    true
}

// ===========================================================================
// PredXact list (CreatePredXact / ReleasePredXact).
// ===========================================================================

unsafe fn CreatePredXact() -> *mut SERIALIZABLEXACT {
    let px = PredXact();
    if crate::ilist_inline::dlist_is_empty(&raw mut (*px).availableList) {
        return ptr::null_mut();
    }
    let node = crate::ilist_inline::dlist_pop_head_node(&raw mut (*px).availableList);
    let sxact = dlist_container!(SERIALIZABLEXACT, xactLink, node);
    crate::ilist_inline::dlist_push_tail(&raw mut (*px).activeList, &raw mut (*sxact).xactLink);
    sxact
}

unsafe fn ReleasePredXact(sxact: *mut SERIALIZABLEXACT) {
    debug_assert!(ShmemAddrIsValid(sxact as *const u8));
    crate::ilist_inline::dlist_delete(&raw mut (*sxact).xactLink);
    crate::ilist_inline::dlist_push_tail(
        &raw mut (*PredXact()).availableList,
        &raw mut (*sxact).xactLink,
    );
}

// ===========================================================================
// RWConflict pool/list primitives.
// ===========================================================================

unsafe fn RWConflictExists(
    reader: *const SERIALIZABLEXACT,
    writer: *const SERIALIZABLEXACT,
) -> bool {
    debug_assert!(reader != writer);

    if SxactIsDoomed(reader)
        || SxactIsDoomed(writer)
        || crate::ilist_inline::dlist_is_empty(&raw const (*reader).outConflicts as *const dlist_head)
        || crate::ilist_inline::dlist_is_empty(&raw const (*writer).inConflicts as *const dlist_head)
    {
        return false;
    }

    // dlist_foreach(iter, &reader->outConflicts)
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

unsafe fn SetRWConflict(
    reader: *mut SERIALIZABLEXACT,
    writer: *mut SERIALIZABLEXACT,
) -> PgResult<()> {
    debug_assert!(reader != writer);
    debug_assert!(!RWConflictExists(reader, writer));

    let pool = RWConflictPool();
    if crate::ilist_inline::dlist_is_empty(&raw const (*pool).availableList) {
        return Err(PgError::error(
            "not enough elements in RWConflictPool to record a read/write conflict",
        )
        .with_sqlstate(ERRCODE_OUT_OF_MEMORY)
        .with_hint(
            "You might need to run fewer transactions at a time or increase \"max_connections\".",
        ));
    }

    let conflict = dlist_container!(
        RWConflictData,
        outLink,
        (*pool).availableList.head.next
    );
    crate::ilist_inline::dlist_delete(&raw mut (*conflict).outLink);

    (*conflict).sxactOut = reader;
    (*conflict).sxactIn = writer;
    crate::ilist_inline::dlist_push_tail(
        &raw mut (*reader).outConflicts,
        &raw mut (*conflict).outLink,
    );
    crate::ilist_inline::dlist_push_tail(
        &raw mut (*writer).inConflicts,
        &raw mut (*conflict).inLink,
    );
    Ok(())
}

unsafe fn SetPossibleUnsafeConflict(
    roXact: *mut SERIALIZABLEXACT,
    activeXact: *mut SERIALIZABLEXACT,
) -> PgResult<()> {
    debug_assert!(roXact != activeXact);
    debug_assert!(SxactIsReadOnly(roXact));
    debug_assert!(!SxactIsReadOnly(activeXact));

    let pool = RWConflictPool();
    if crate::ilist_inline::dlist_is_empty(&raw const (*pool).availableList) {
        return Err(PgError::error(
            "not enough elements in RWConflictPool to record a potential read/write conflict",
        )
        .with_sqlstate(ERRCODE_OUT_OF_MEMORY)
        .with_hint(
            "You might need to run fewer transactions at a time or increase \"max_connections\".",
        ));
    }

    let conflict = dlist_container!(
        RWConflictData,
        outLink,
        (*pool).availableList.head.next
    );
    crate::ilist_inline::dlist_delete(&raw mut (*conflict).outLink);

    (*conflict).sxactOut = activeXact;
    (*conflict).sxactIn = roXact;
    crate::ilist_inline::dlist_push_tail(
        &raw mut (*activeXact).possibleUnsafeConflicts,
        &raw mut (*conflict).outLink,
    );
    crate::ilist_inline::dlist_push_tail(
        &raw mut (*roXact).possibleUnsafeConflicts,
        &raw mut (*conflict).inLink,
    );
    Ok(())
}

unsafe fn ReleaseRWConflict(conflict: RWConflict) {
    crate::ilist_inline::dlist_delete(&raw mut (*conflict).inLink);
    crate::ilist_inline::dlist_delete(&raw mut (*conflict).outLink);
    crate::ilist_inline::dlist_push_tail(
        &raw mut (*RWConflictPool()).availableList,
        &raw mut (*conflict).outLink,
    );
}

unsafe fn FlagSxactUnsafe(sxact: *mut SERIALIZABLEXACT) {
    debug_assert!(SxactIsReadOnly(sxact));
    debug_assert!(!SxactIsROSafe(sxact));

    (*sxact).flags |= SXACT_FLAG_RO_UNSAFE;

    // dlist_foreach_modify(iter, &sxact->possibleUnsafeConflicts)
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

// ===========================================================================
// Shmem init / sizing.
// ===========================================================================

fn predicatelock_hash(key: &[u8], _keysize: Size) -> u32 {
    // key is a PREDICATELOCKTAG.
    let predicatelocktag = key.as_ptr() as *const PREDICATELOCKTAG;
    unsafe {
        let targethash =
            PredicateLockTargetTagHashCode(&raw const (*(*predicatelocktag).myTarget).tag);
        PredicateLockHashCodeFromTargetHashCode(predicatelocktag, targethash)
    }
}

/// `PredicateLockShmemInit()`.
pub fn PredicateLockShmemInit() -> PgResult<()> {
    unsafe {
        let mut info = HASHCTL::default();
        let max_table_size_targets = NPREDICATELOCKTARGETENTS()? as i64;

        // PREDICATELOCKTARGET hash.
        info.keysize = core::mem::size_of::<PREDICATELOCKTARGETTAG>();
        info.entrysize = core::mem::size_of::<PREDICATELOCKTARGET>();
        info.num_partitions = types_storage::NUM_PREDICATELOCK_PARTITIONS as i64;

        let target_hash = ShmemInitHash(
            "PREDICATELOCKTARGET hash",
            max_table_size_targets,
            max_table_size_targets,
            &mut info,
            HASH_ELEM | HASH_BLOBS | HASH_PARTITION | HASH_FIXED_SIZE,
        )?;
        PREDICATE_LOCK_TARGET_HASH.with(|c| c.set(target_hash));

        // Reserve a dummy (scratch) entry.
        if !IS_UNDER_POSTMASTER.with(|c| c.get()) {
            let (_p, found) = hash_search(
                target_hash,
                &raw const SCRATCH_TARGET_TAG as *const u8,
                HASH_ENTER,
            )?;
            debug_assert!(!found);
        }

        // Pre-calculate the hash of the scratch entry.
        let sh = PredicateLockTargetTagHashCode(&raw const SCRATCH_TARGET_TAG);
        SCRATCH_TARGET_TAG_HASH.with(|c| c.set(sh));

        // PREDICATELOCK hash.
        info.keysize = core::mem::size_of::<PREDICATELOCKTAG>();
        info.entrysize = core::mem::size_of::<PREDICATELOCK>();
        info.hash = Some(predicatelock_hash);
        info.num_partitions = types_storage::NUM_PREDICATELOCK_PARTITIONS as i64;

        let max_table_size_locks = max_table_size_targets * 2;
        let lock_hash = ShmemInitHash(
            "PREDICATELOCK hash",
            max_table_size_locks,
            max_table_size_locks,
            &mut info,
            HASH_ELEM | HASH_FUNCTION | HASH_PARTITION | HASH_FIXED_SIZE,
        )?;
        PREDICATE_LOCK_HASH.with(|c| c.set(lock_hash));
        info.hash = None;

        // PredXactList.
        let xact_count = (max_backends() + max_prepared_xacts()) as i64;
        let elem_count = xact_count * 10;
        let requestSize = add_size(
            PredXactListDataSize() as Size,
            mul_size(
                elem_count as Size,
                core::mem::size_of::<SERIALIZABLEXACT>() as Size,
            )?,
        )?;

        let (px_ptr, found) = ShmemInitStruct("PredXactList", requestSize)?;
        let px = px_ptr.as_ptr() as PredXactList;
        PRED_XACT.with(|c| c.set(px));
        if !found {
            ptr::write_bytes(px_ptr.as_ptr(), 0, requestSize);
            crate::ilist_inline::dlist_init(&raw mut (*px).availableList);
            crate::ilist_inline::dlist_init(&raw mut (*px).activeList);
            (*px).SxactGlobalXmin = InvalidTransactionId;
            (*px).SxactGlobalXminCount = 0;
            (*px).WritableSxactCount = 0;
            (*px).LastSxactCommitSeqNo = FirstNormalSerCommitSeqNo - 1;
            (*px).CanPartialClearThrough = 0;
            (*px).HavePartialClearedThrough = 0;
            (*px).element = (px_ptr.as_ptr().add(PredXactListDataSize())) as *mut SERIALIZABLEXACT;
            for i in 0..elem_count {
                let e = (*px).element.add(i as usize);
                LWLockInitialize(
                    &mut (*e).perXactPredicateListLock,
                    types_storage::LWTRANCHE_PER_XACT_PREDICATE_LIST,
                );
                crate::ilist_inline::dlist_push_tail(
                    &raw mut (*px).availableList,
                    &raw mut (*e).xactLink,
                );
            }
            let oc = CreatePredXact();
            (*px).OldCommittedSxact = oc;
            (*oc).vxid = types_core::VirtualTransactionId::invalid();
            (*oc).prepareSeqNo = 0;
            (*oc).commitSeqNo = 0;
            (*oc).SeqNo.lastCommitBeforeSnapshot = 0;
            crate::ilist_inline::dlist_init(&raw mut (*oc).outConflicts);
            crate::ilist_inline::dlist_init(&raw mut (*oc).inConflicts);
            crate::ilist_inline::dlist_init(&raw mut (*oc).predicateLocks);
            crate::ilist_inline::dlist_node_init(&raw mut (*oc).finishedLink);
            crate::ilist_inline::dlist_init(&raw mut (*oc).possibleUnsafeConflicts);
            (*oc).topXid = InvalidTransactionId;
            (*oc).finishedBefore = InvalidTransactionId;
            (*oc).xmin = InvalidTransactionId;
            (*oc).flags = SXACT_FLAG_COMMITTED;
            (*oc).pid = 0;
            (*oc).pgprocno = INVALID_PROC_NUMBER;
        }
        OLD_COMMITTED_SXACT.with(|c| c.set((*px).OldCommittedSxact));

        // SERIALIZABLEXID hash.
        info.keysize = core::mem::size_of::<SERIALIZABLEXIDTAG>();
        info.entrysize = core::mem::size_of::<SERIALIZABLEXID>();
        let xid_hash = ShmemInitHash(
            "SERIALIZABLEXID hash",
            xact_count,
            xact_count,
            &mut info,
            HASH_ELEM | HASH_BLOBS | HASH_FIXED_SIZE,
        )?;
        SERIALIZABLE_XID_HASH.with(|c| c.set(xid_hash));

        // RWConflictPool.
        let conflict_count = elem_count * 5;
        let requestSize = add_size(
            RWConflictPoolHeaderDataSize() as Size,
            mul_size(conflict_count as Size, RWConflictDataSize() as Size)?,
        )?;
        let (rw_ptr, found) = ShmemInitStruct("RWConflictPool", requestSize)?;
        let rw = rw_ptr.as_ptr() as RWConflictPoolHeader;
        RW_CONFLICT_POOL.with(|c| c.set(rw));
        if !found {
            ptr::write_bytes(rw_ptr.as_ptr(), 0, requestSize);
            crate::ilist_inline::dlist_init(&raw mut (*rw).availableList);
            (*rw).element =
                (rw_ptr.as_ptr().add(RWConflictPoolHeaderDataSize())) as RWConflict;
            for i in 0..conflict_count {
                let e = (*rw).element.add(i as usize);
                crate::ilist_inline::dlist_push_tail(
                    &raw mut (*rw).availableList,
                    &raw mut (*e).outLink,
                );
            }
        }

        // FinishedSerializableTransactions.
        let (f_ptr, found) =
            ShmemInitStruct("FinishedSerializableTransactions", core::mem::size_of::<dlist_head>())?;
        let f = f_ptr.as_ptr() as *mut dlist_head;
        FINISHED_SERIALIZABLE_TRANSACTIONS.with(|c| c.set(f));
        if !found {
            crate::ilist_inline::dlist_init(f);
        }

        // SLRU storage for old committed serializable transactions.
        SerialInit()?;
        let _ = SERIAL_CONTROL_FOUND.with(|c| c.get());
    }
    Ok(())
}

/// `PredicateLockShmemSize()`.
pub fn PredicateLockShmemSize() -> PgResult<Size> {
    let mut size: Size = 0;

    let mut max_table_size = NPREDICATELOCKTARGETENTS()? as i64;
    size = add_size(
        size,
        hash_estimate_size(max_table_size, core::mem::size_of::<PREDICATELOCKTARGET>()),
    )?;

    max_table_size *= 2;
    size = add_size(
        size,
        hash_estimate_size(max_table_size, core::mem::size_of::<PREDICATELOCK>()),
    )?;

    size = add_size(size, size / 10)?;

    let mut max_table_size = (max_backends() + max_prepared_xacts()) as i64;
    max_table_size *= 10;
    size = add_size(size, PredXactListDataSize() as Size)?;
    size = add_size(
        size,
        mul_size(
            max_table_size as Size,
            core::mem::size_of::<SERIALIZABLEXACT>() as Size,
        )?,
    )?;

    size = add_size(
        size,
        hash_estimate_size(max_table_size, core::mem::size_of::<SERIALIZABLEXID>()),
    )?;

    max_table_size *= 5;
    size = add_size(size, RWConflictPoolHeaderDataSize() as Size)?;
    size = add_size(
        size,
        mul_size(max_table_size as Size, RWConflictDataSize() as Size)?,
    )?;

    size = add_size(size, core::mem::size_of::<dlist_head>())?;

    size = add_size(size, core::mem::size_of::<crate::serial::SerialControlData>())?;
    size = add_size(
        size,
        backend_access_transam_slru::SimpleLruShmemSize(serializable_buffers(), 0),
    )?;

    Ok(size)
}

// ===========================================================================
// GetPredicateLockStatusData.
// ===========================================================================

/// `GetPredicateLockStatusData()`.
pub fn GetPredicateLockStatusData() -> PgResult<PredicateLockData> {
    unsafe {
        let procno = my_proc_number();
        for i in 0..types_storage::NUM_PREDICATELOCK_PARTITIONS {
            LWLockAcquire(PredicateLockHashPartitionLockByIndex(i), LW_SHARED, procno)?;
        }
        LWLockAcquire(SerializableXactHashLock(), LW_SHARED, procno)?;

        let els = hash_get_num_entries(PredicateLockHash()) as i32;
        let mut locktags: Vec<PREDICATELOCKTARGETTAG> = Vec::with_capacity(els as usize);
        let mut xacts: Vec<SerializableXactScalars> = Vec::with_capacity(els as usize);

        let mut seqstat = HASH_SEQ_STATUS::default();
        hash_seq_init(&mut seqstat, PredicateLockHash());

        let mut el = 0;
        loop {
            let p = hash_seq_search(&mut seqstat)?;
            if p.is_null() {
                break;
            }
            let predlock = p as *mut PREDICATELOCK;
            locktags.push((*(*predlock).tag.myTarget).tag);
            let x = (*predlock).tag.myXact;
            xacts.push(SerializableXactScalars {
                vxid: (*x).vxid,
                flags: (*x).flags,
                pid: (*x).pid,
                topXid: (*x).topXid,
                xmin: (*x).xmin,
            });
            el += 1;
        }
        debug_assert!(el == els);

        LWLockRelease(SerializableXactHashLock())?;
        for i in (0..types_storage::NUM_PREDICATELOCK_PARTITIONS).rev() {
            LWLockRelease(PredicateLockHashPartitionLockByIndex(i))?;
        }

        Ok(PredicateLockData {
            nelements: els,
            locktags,
            xacts,
        })
    }
}

// ===========================================================================
// SummarizeOldestCommittedSxact.
// ===========================================================================

unsafe fn SummarizeOldestCommittedSxact() -> PgResult<()> {
    LWLockAcquire(SerializableFinishedListLock(), LW_EXCLUSIVE, my_proc_number())?;

    let finished = FinishedSerializableTransactions();
    if crate::ilist_inline::dlist_is_empty(finished) {
        LWLockRelease(SerializableFinishedListLock())?;
        return Ok(());
    }

    let sxact = dlist_container!(SERIALIZABLEXACT, finishedLink, (*finished).head.next);
    crate::ilist_inline::dlist_delete_thoroughly(&raw mut (*sxact).finishedLink);

    if TransactionIdIsValid((*sxact).topXid) && !SxactIsReadOnly(sxact) {
        let seqno = if SxactHasConflictOut(sxact) {
            (*sxact).SeqNo.earliestOutConflictCommit
        } else {
            InvalidSerCommitSeqNo
        };
        SerialAdd((*sxact).topXid, seqno)?;
    }

    ReleaseOneSerializableXact(sxact, false, true)?;

    LWLockRelease(SerializableFinishedListLock())?;
    Ok(())
}

// ===========================================================================
// GetSafeSnapshot / snapshot acquisition.
// ===========================================================================

unsafe fn GetSafeSnapshot(orig_snapshot: SnapshotData) -> PgResult<SnapshotData> {
    debug_assert!(xact_read_only() && xact_deferrable());

    let mut snapshot;
    loop {
        snapshot =
            GetSerializableTransactionSnapshotInt(orig_snapshot.clone(), None, INVALID_PID)?;

        if MySerializableXact() == InvalidSerializableXact {
            return Ok(snapshot); // no concurrent r/w xacts; it's safe
        }

        let procno = my_proc_number();
        LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE, procno)?;

        let mysx = MySerializableXact();
        (*mysx).flags |= SXACT_FLAG_DEFERRABLE_WAITING;
        while !(crate::ilist_inline::dlist_is_empty(&raw const (*mysx).possibleUnsafeConflicts)
            || SxactIsROUnsafe(mysx))
        {
            LWLockRelease(SerializableXactHashLock())?;
            proc_wait_for_signal_safe_snapshot()?;
            LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE, procno)?;
        }
        (*mysx).flags &= !SXACT_FLAG_DEFERRABLE_WAITING;

        if !SxactIsROUnsafe(mysx) {
            LWLockRelease(SerializableXactHashLock())?;
            break; // success
        }

        LWLockRelease(SerializableXactHashLock())?;

        // else, need to retry...
        ReleasePredicateLocks(false, false)?;
    }

    debug_assert!(SxactIsROSafe(MySerializableXact()));
    ReleasePredicateLocks(false, true)?;

    Ok(snapshot)
}

/// `InvalidPid` (== 0).
const INVALID_PID: i32 = 0;

/// `GetSafeSnapshotBlockingPids(blocked_pid, output, output_size)`.
pub fn GetSafeSnapshotBlockingPids(
    blocked_pid: i32,
    output: &mut [i32],
    output_size: i32,
) -> PgResult<i32> {
    unsafe {
        let mut num_written = 0i32;
        let mut blocking_sxact: *mut SERIALIZABLEXACT = ptr::null_mut();

        LWLockAcquire(SerializableXactHashLock(), LW_SHARED, my_proc_number())?;

        // dlist_foreach(iter, &PredXact->activeList)
        let head = &raw const (*PredXact()).activeList;
        let mut cur = (*head).head.next;
        while cur != (&raw const (*head).head) as *mut dlist_node {
            let sxact = dlist_container!(SERIALIZABLEXACT, xactLink, cur);
            if (*sxact).pid == blocked_pid {
                blocking_sxact = sxact;
                break;
            }
            cur = (*cur).next;
        }

        if !blocking_sxact.is_null() && SxactIsDeferrableWaiting(blocking_sxact) {
            let head = &raw const (*blocking_sxact).possibleUnsafeConflicts;
            let mut cur = (*head).head.next;
            while cur != (&raw const (*head).head) as *mut dlist_node {
                let puc = dlist_container!(RWConflictData, inLink, cur);
                output[num_written as usize] = (*(*puc).sxactOut).pid;
                num_written += 1;
                if num_written >= output_size {
                    break;
                }
                cur = (*cur).next;
            }
        }

        LWLockRelease(SerializableXactHashLock())?;
        Ok(num_written)
    }
}

/// `GetSerializableTransactionSnapshot(snapshot)`.
pub fn GetSerializableTransactionSnapshot(snapshot: SnapshotData) -> PgResult<SnapshotData> {
    debug_assert!(isolation_is_serializable());

    if recovery_in_progress() {
        return Err(PgError::error("cannot use serializable mode in a hot standby")
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
            .with_detail("\"default_transaction_isolation\" is set to \"serializable\".")
            .with_hint(
                "You can use \"SET default_transaction_isolation = 'repeatable read'\" to change the default.",
            ));
    }

    if xact_read_only() && xact_deferrable() {
        return unsafe { GetSafeSnapshot(snapshot) };
    }

    GetSerializableTransactionSnapshotInt(snapshot, None, INVALID_PID)
}

/// `SetSerializableTransactionSnapshot(snapshot, sourcevxid, sourcepid)`.
pub fn SetSerializableTransactionSnapshot(
    snapshot: SnapshotData,
    sourcevxid: types_core::VirtualTransactionId,
    sourcepid: i32,
) -> PgResult<()> {
    debug_assert!(isolation_is_serializable());

    if is_parallel_worker() {
        return Ok(());
    }

    if xact_read_only() && xact_deferrable() {
        return Err(
            PgError::error("a snapshot-importing transaction must not be READ ONLY DEFERRABLE")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        );
    }

    GetSerializableTransactionSnapshotInt(snapshot, Some(sourcevxid), sourcepid)?;
    Ok(())
}

fn GetSerializableTransactionSnapshotInt(
    mut snapshot: SnapshotData,
    sourcevxid: Option<types_core::VirtualTransactionId>,
    sourcepid: i32,
) -> PgResult<SnapshotData> {
    unsafe {
        debug_assert!(MySerializableXact() == InvalidSerializableXact);
        debug_assert!(!recovery_in_progress());

        if is_in_parallel_mode() {
            return Err(PgError::error(
                "cannot establish serializable snapshot during a parallel operation",
            ));
        }

        let vxid = my_proc_vxid();
        let procno = my_proc_number();

        LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE, procno)?;
        let mut sxact;
        loop {
            sxact = CreatePredXact();
            if sxact.is_null() {
                LWLockRelease(SerializableXactHashLock())?;
                SummarizeOldestCommittedSxact()?;
                LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE, procno)?;
            } else {
                break;
            }
        }

        // Get the snapshot, or check that it's safe to use.
        match sourcevxid {
            None => {
                snapshot = get_snapshot_data()?;
            }
            Some(svxid) => {
                if !proc_array_install_imported_xmin(snapshot.xmin, svxid)? {
                    ReleasePredXact(sxact);
                    LWLockRelease(SerializableXactHashLock())?;
                    return Err(PgError::error("could not import the requested snapshot")
                        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                        .with_detail(format!(
                            "The source process with PID {sourcepid} is not running anymore."
                        )));
                }
            }
        }

        let px = PredXact();

        if xact_read_only() && (*px).WritableSxactCount == 0 {
            ReleasePredXact(sxact);
            LWLockRelease(SerializableXactHashLock())?;
            return Ok(snapshot);
        }

        // Initialize the structure.
        (*sxact).vxid = vxid;
        (*sxact).SeqNo.lastCommitBeforeSnapshot = (*px).LastSxactCommitSeqNo;
        (*sxact).prepareSeqNo = InvalidSerCommitSeqNo;
        (*sxact).commitSeqNo = InvalidSerCommitSeqNo;
        crate::ilist_inline::dlist_init(&raw mut (*sxact).outConflicts);
        crate::ilist_inline::dlist_init(&raw mut (*sxact).inConflicts);
        crate::ilist_inline::dlist_init(&raw mut (*sxact).possibleUnsafeConflicts);
        (*sxact).topXid = get_top_transaction_id_if_any();
        (*sxact).finishedBefore = InvalidTransactionId;
        (*sxact).xmin = snapshot.xmin;
        (*sxact).pid = my_proc_pid();
        (*sxact).pgprocno = procno;
        crate::ilist_inline::dlist_init(&raw mut (*sxact).predicateLocks);
        crate::ilist_inline::dlist_node_init(&raw mut (*sxact).finishedLink);
        (*sxact).flags = 0;

        if xact_read_only() {
            (*sxact).flags |= SXACT_FLAG_READ_ONLY;

            // dlist_foreach(iter, &PredXact->activeList)
            let head = &raw const (*px).activeList;
            let mut cur = (*head).head.next;
            while cur != (&raw const (*head).head) as *mut dlist_node {
                let othersxact = dlist_container!(SERIALIZABLEXACT, xactLink, cur);
                if !SxactIsCommitted(othersxact)
                    && !SxactIsDoomed(othersxact)
                    && !SxactIsReadOnly(othersxact)
                {
                    SetPossibleUnsafeConflict(sxact, othersxact)?;
                }
                cur = (*cur).next;
            }

            if crate::ilist_inline::dlist_is_empty(&raw const (*sxact).possibleUnsafeConflicts) {
                ReleasePredXact(sxact);
                LWLockRelease(SerializableXactHashLock())?;
                return Ok(snapshot);
            }
        } else {
            (*px).WritableSxactCount += 1;
            debug_assert!((*px).WritableSxactCount <= (max_backends() + max_prepared_xacts()));
        }

        // Maintain serializable global xmin info.
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

        Ok(snapshot)
    }
}

fn CreateLocalPredicateLockHash() -> PgResult<()> {
    debug_assert!(LocalPredicateLockHash().is_null());
    let mut hash_ctl = HASHCTL::default();
    hash_ctl.keysize = core::mem::size_of::<PREDICATELOCKTARGETTAG>();
    hash_ctl.entrysize = core::mem::size_of::<LOCALPREDICATELOCK>();
    let h = hash_create(
        "Local predicate lock",
        max_predicate_locks_per_xact() as i64,
        &hash_ctl,
        HASH_ELEM | HASH_BLOBS,
    )?;
    LOCAL_PREDICATE_LOCK_HASH.with(|c| c.set(h));
    Ok(())
}

/// `RegisterPredicateLockingXid(xid)`.
pub fn RegisterPredicateLockingXid(xid: TransactionId) -> PgResult<()> {
    unsafe {
        if MySerializableXact() == InvalidSerializableXact {
            return Ok(());
        }
        debug_assert!(TransactionIdIsValid(xid));

        LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE, my_proc_number())?;

        debug_assert!((*MySerializableXact()).topXid == InvalidTransactionId);
        (*MySerializableXact()).topXid = xid;

        let mut sxidtag = SERIALIZABLEXIDTAG { xid };
        let (p, found) = hash_search(
            SerializableXidHash(),
            &raw mut sxidtag as *const u8,
            HASH_ENTER,
        )?;
        debug_assert!(!found);
        let sxid = p as *mut SERIALIZABLEXID;
        (*sxid).myXact = MySerializableXact();
        LWLockRelease(SerializableXactHashLock())?;
        Ok(())
    }
}

/// `PageIsPredicateLocked(relation, blkno)` — over already-projected (db, rel).
pub fn PageIsPredicateLocked(db_oid: Oid, rel_id: Oid, blkno: BlockNumber) -> PgResult<bool> {
    unsafe {
        let mut targettag = PREDICATELOCKTARGETTAG {
            locktag_field1: 0,
            locktag_field2: 0,
            locktag_field3: 0,
            locktag_field4: 0,
        };
        SET_PREDICATELOCKTARGETTAG_PAGE(&mut targettag, db_oid, rel_id, blkno);

        let targettaghash = PredicateLockTargetTagHashCode(&targettag);
        let partition_lock = PredicateLockHashPartitionLock(targettaghash);
        LWLockAcquire(partition_lock, LW_SHARED, my_proc_number())?;
        let (target, _found) = hash_search_with_hash_value(
            PredicateLockTargetHash(),
            &raw const targettag as *const u8,
            targettaghash,
            HASH_FIND,
        )?;
        LWLockRelease(partition_lock)?;
        Ok(!target.is_null())
    }
}

// ===========================================================================
// Local lock table: PredicateLockExists / parent walk / promotion.
// ===========================================================================

unsafe fn PredicateLockExists(targettag: *const PREDICATELOCKTARGETTAG) -> PgResult<bool> {
    let (p, _found) = hash_search(LocalPredicateLockHash(), targettag as *const u8, HASH_FIND)?;
    if p.is_null() {
        return Ok(false);
    }
    let lock = p as *mut LOCALPREDICATELOCK;
    Ok((*lock).held)
}

/// `GetParentPredicateLockTag(tag, parent)` — returns true and sets *parent.
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
    let mut parenttag = PREDICATELOCKTARGETTAG {
        locktag_field1: 0,
        locktag_field2: 0,
        locktag_field3: 0,
        locktag_field4: 0,
    };
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
        LWLockAcquire(ScratchPartitionLock(), LW_EXCLUSIVE, my_proc_number())?;
    }
    let (_p, found) = hash_search_with_hash_value(
        PredicateLockTargetHash(),
        &raw const SCRATCH_TARGET_TAG as *const u8,
        ScratchTargetTagHash(),
        HASH_REMOVE,
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
        LWLockAcquire(ScratchPartitionLock(), LW_EXCLUSIVE, my_proc_number())?;
    }
    let (_p, found) = hash_search_with_hash_value(
        PredicateLockTargetHash(),
        &raw const SCRATCH_TARGET_TAG as *const u8,
        ScratchTargetTagHash(),
        HASH_ENTER,
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
    if !crate::ilist_inline::dlist_is_empty(&raw const (*target).predicateLocks) {
        return Ok(());
    }
    let (rmtarget, _found) = hash_search_with_hash_value(
        PredicateLockTargetHash(),
        &raw const (*target).tag as *const u8,
        targettaghash,
        HASH_REMOVE,
    )?;
    debug_assert!(rmtarget as *mut PREDICATELOCKTARGET == target);
    Ok(())
}

unsafe fn DeleteChildTargetLocks(newtargettag: *const PREDICATELOCKTARGETTAG) -> PgResult<()> {
    LWLockAcquire(SerializablePredicateListLock(), LW_SHARED, my_proc_number())?;
    let sxact = MySerializableXact();
    if is_in_parallel_mode() {
        LWLockAcquire(&(*sxact).perXactPredicateListLock, LW_EXCLUSIVE, my_proc_number())?;
    }

    // dlist_foreach_modify(iter, &sxact->predicateLocks)
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
            LWLockAcquire(partition_lock, LW_EXCLUSIVE, my_proc_number())?;

            crate::ilist_inline::dlist_delete(&raw mut (*predlock).xactLink);
            crate::ilist_inline::dlist_delete(&raw mut (*predlock).targetLink);
            let (rmpredlock, _f) = hash_search_with_hash_value(
                PredicateLockHash(),
                &raw const oldlocktag as *const u8,
                PredicateLockHashCodeFromTargetHashCode(&oldlocktag, oldtargettaghash),
                HASH_REMOVE,
            )?;
            debug_assert!(rmpredlock as *mut PREDICATELOCK == predlock);

            RemoveTargetIfNoLongerUsed(oldtarget, oldtargettaghash)?;

            LWLockRelease(partition_lock)?;

            DecrementParentLocks(&oldtargettag)?;
        }
        cur = next;
    }
    if is_in_parallel_mode() {
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
    let mut nexttag = PREDICATELOCKTARGETTAG {
        locktag_field1: 0,
        locktag_field2: 0,
        locktag_field3: 0,
        locktag_field4: 0,
    };
    let mut promotiontag = targettag;

    while GetParentPredicateLockTag(&targettag, &mut nexttag) {
        targettag = nexttag;
        let (p, found) =
            hash_search(LocalPredicateLockHash(), &raw const targettag as *const u8, HASH_ENTER)?;
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
    let mut nexttag = PREDICATELOCKTARGETTAG {
        locktag_field1: 0,
        locktag_field2: 0,
        locktag_field3: 0,
        locktag_field4: 0,
    };

    while GetParentPredicateLockTag(&parenttag, &mut nexttag) {
        parenttag = nexttag;
        let targettaghash = PredicateLockTargetTagHashCode(&parenttag);
        let (p, _found) = hash_search_with_hash_value(
            LocalPredicateLockHash(),
            &raw const parenttag as *const u8,
            targettaghash,
            HASH_FIND,
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
            let (rmlock, _f) = hash_search_with_hash_value(
                LocalPredicateLockHash(),
                &raw const parenttag as *const u8,
                targettaghash,
                HASH_REMOVE,
            )?;
            debug_assert!(rmlock as *mut LOCALPREDICATELOCK == parentlock);
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

    LWLockAcquire(SerializablePredicateListLock(), LW_SHARED, my_proc_number())?;
    if is_in_parallel_mode() {
        LWLockAcquire(&(*sxact).perXactPredicateListLock, LW_EXCLUSIVE, my_proc_number())?;
    }
    LWLockAcquire(partition_lock, LW_EXCLUSIVE, my_proc_number())?;

    let (tp, found) = hash_search_with_hash_value(
        PredicateLockTargetHash(),
        targettag as *const u8,
        targettaghash,
        HASH_ENTER_NULL,
    )?;
    let target = tp as *mut PREDICATELOCKTARGET;
    if target.is_null() {
        LWLockRelease(partition_lock)?;
        if is_in_parallel_mode() {
            LWLockRelease(&(*sxact).perXactPredicateListLock)?;
        }
        LWLockRelease(SerializablePredicateListLock())?;
        return Err(out_of_shared_memory());
    }
    if !found {
        crate::ilist_inline::dlist_init(&raw mut (*target).predicateLocks);
    }

    let mut locktag = PREDICATELOCKTAG {
        myTarget: target,
        myXact: sxact,
    };
    let (lp, found) = hash_search_with_hash_value(
        PredicateLockHash(),
        &raw const locktag as *const u8,
        PredicateLockHashCodeFromTargetHashCode(&locktag, targettaghash),
        HASH_ENTER_NULL,
    )?;
    let lock = lp as *mut PREDICATELOCK;
    if lock.is_null() {
        LWLockRelease(partition_lock)?;
        if is_in_parallel_mode() {
            LWLockRelease(&(*sxact).perXactPredicateListLock)?;
        }
        LWLockRelease(SerializablePredicateListLock())?;
        return Err(out_of_shared_memory());
    }
    let _ = &mut locktag;

    if !found {
        crate::ilist_inline::dlist_push_tail(
            &raw mut (*target).predicateLocks,
            &raw mut (*lock).targetLink,
        );
        crate::ilist_inline::dlist_push_tail(
            &raw mut (*sxact).predicateLocks,
            &raw mut (*lock).xactLink,
        );
        (*lock).commitSeqNo = InvalidSerCommitSeqNo;
    }

    LWLockRelease(partition_lock)?;
    if is_in_parallel_mode() {
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

    let (p, found) = hash_search_with_hash_value(
        LocalPredicateLockHash(),
        targettag as *const u8,
        targettaghash,
        HASH_ENTER,
    )?;
    let locallock = p as *mut LOCALPREDICATELOCK;
    (*locallock).held = true;
    if !found {
        (*locallock).childLocks = 0;
    }

    CreatePredicateLock(targettag, targettaghash, MySerializableXact())?;

    if CheckAndPromotePredicateLockRequest(targettag)? {
        // Promoted; the coarser lock deleted this one and its children.
    } else if GET_PREDICATELOCKTARGETTAG_TYPE(&*targettag) != PREDLOCKTAG_TUPLE {
        DeleteChildTargetLocks(targettag)?;
    }
    Ok(())
}

// ===========================================================================
// PredicateLockRelation / Page / TID — over already-projected fields.
// ===========================================================================

/// `PredicateLockRelation(relation, snapshot)`.
pub fn PredicateLockRelation(
    db_oid: Oid,
    rd_id: Oid,
    uses_local_buffers: bool,
    snapshot: &SnapshotData,
) -> PgResult<()> {
    unsafe {
        if !SerializationNeededForRead(rd_id, uses_local_buffers, snapshot)? {
            return Ok(());
        }
        let mut tag = PREDICATELOCKTARGETTAG {
            locktag_field1: 0,
            locktag_field2: 0,
            locktag_field3: 0,
            locktag_field4: 0,
        };
        SET_PREDICATELOCKTARGETTAG_RELATION(&mut tag, db_oid, rd_id);
        PredicateLockAcquire(&tag)
    }
}

/// `PredicateLockPage(relation, blkno, snapshot)`.
pub fn PredicateLockPage(
    db_oid: Oid,
    rd_id: Oid,
    uses_local_buffers: bool,
    blkno: BlockNumber,
    snapshot: &SnapshotData,
) -> PgResult<()> {
    unsafe {
        if !SerializationNeededForRead(rd_id, uses_local_buffers, snapshot)? {
            return Ok(());
        }
        let mut tag = PREDICATELOCKTARGETTAG {
            locktag_field1: 0,
            locktag_field2: 0,
            locktag_field3: 0,
            locktag_field4: 0,
        };
        SET_PREDICATELOCKTARGETTAG_PAGE(&mut tag, db_oid, rd_id, blkno);
        PredicateLockAcquire(&tag)
    }
}

/// `PredicateLockTID(relation, tid, snapshot, tuple_xid)`. `is_index` ==
/// `relation->rd_index != NULL`.
pub fn PredicateLockTID(
    db_oid: Oid,
    rd_id: Oid,
    uses_local_buffers: bool,
    is_index: bool,
    blkno: BlockNumber,
    offnum: types_core::primitive::OffsetNumber,
    snapshot: &SnapshotData,
    tuple_xid: TransactionId,
) -> PgResult<()> {
    unsafe {
        if !SerializationNeededForRead(rd_id, uses_local_buffers, snapshot)? {
            return Ok(());
        }

        if !is_index {
            // If we wrote it; we already have a write lock.
            if transaction_id_is_current_transaction_id(tuple_xid) {
                return Ok(());
            }
        }

        let mut tag = PREDICATELOCKTARGETTAG {
            locktag_field1: 0,
            locktag_field2: 0,
            locktag_field3: 0,
            locktag_field4: 0,
        };
        SET_PREDICATELOCKTARGETTAG_RELATION(&mut tag, db_oid, rd_id);
        if PredicateLockExists(&tag)? {
            return Ok(());
        }

        SET_PREDICATELOCKTARGETTAG_TUPLE(&mut tag, db_oid, rd_id, blkno, offnum);
        PredicateLockAcquire(&tag)
    }
}

// ===========================================================================
// DeleteLockTarget / TransferPredicateLocksToNewTarget.
// ===========================================================================

unsafe fn DeleteLockTarget(target: *mut PREDICATELOCKTARGET, targettaghash: u32) -> PgResult<()> {
    debug_assert!(LWLockHeldByMeInMode(
        SerializablePredicateListLock(),
        LW_EXCLUSIVE
    ));
    debug_assert!(LWLockHeldByMe(PredicateLockHashPartitionLock(targettaghash)));

    LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE, my_proc_number())?;

    // dlist_foreach_modify(iter, &target->predicateLocks)
    let head = &raw mut (*target).predicateLocks;
    let mut cur = (*head).head.next;
    while cur != (&raw mut (*head).head) as *mut dlist_node {
        let next = (*cur).next;
        let predlock = dlist_container!(PREDICATELOCK, targetLink, cur);
        crate::ilist_inline::dlist_delete(&raw mut (*predlock).xactLink);
        crate::ilist_inline::dlist_delete(&raw mut (*predlock).targetLink);
        let (_p, found) = hash_search_with_hash_value(
            PredicateLockHash(),
            &raw const (*predlock).tag as *const u8,
            PredicateLockHashCodeFromTargetHashCode(&(*predlock).tag, targettaghash),
            HASH_REMOVE,
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
    let procno = my_proc_number();

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

    let (otp, _f) = hash_search_with_hash_value(
        PredicateLockTargetHash(),
        &raw const oldtargettag as *const u8,
        oldtargettaghash,
        HASH_FIND,
    )?;
    let oldtarget = otp as *mut PREDICATELOCKTARGET;

    'exit: {
        if oldtarget.is_null() {
            break 'exit;
        }

        let (ntp, found) = hash_search_with_hash_value(
            PredicateLockTargetHash(),
            &raw const newtargettag as *const u8,
            newtargettaghash,
            HASH_ENTER_NULL,
        )?;
        let newtarget = ntp as *mut PREDICATELOCKTARGET;
        if newtarget.is_null() {
            out_of_shmem = true;
            break 'exit;
        }
        if !found {
            crate::ilist_inline::dlist_init(&raw mut (*newtarget).predicateLocks);
        }

        let mut newpredlocktag = PREDICATELOCKTAG {
            myTarget: newtarget,
            myXact: ptr::null_mut(),
        };

        LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE, procno)?;

        // dlist_foreach_modify(iter, &oldtarget->predicateLocks)
        let head = &raw mut (*oldtarget).predicateLocks;
        let mut cur = (*head).head.next;
        while cur != (&raw mut (*head).head) as *mut dlist_node {
            let next = (*cur).next;
            let oldpredlock = dlist_container!(PREDICATELOCK, targetLink, cur);
            let oldCommitSeqNo = (*oldpredlock).commitSeqNo;

            newpredlocktag.myXact = (*oldpredlock).tag.myXact;

            if removeOld {
                crate::ilist_inline::dlist_delete(&raw mut (*oldpredlock).xactLink);
                crate::ilist_inline::dlist_delete(&raw mut (*oldpredlock).targetLink);
                let (_p, found) = hash_search_with_hash_value(
                    PredicateLockHash(),
                    &raw const (*oldpredlock).tag as *const u8,
                    PredicateLockHashCodeFromTargetHashCode(&(*oldpredlock).tag, oldtargettaghash),
                    HASH_REMOVE,
                )?;
                debug_assert!(found);
            }

            let (npp, found) = hash_search_with_hash_value(
                PredicateLockHash(),
                &raw const newpredlocktag as *const u8,
                PredicateLockHashCodeFromTargetHashCode(&newpredlocktag, newtargettaghash),
                HASH_ENTER_NULL,
            )?;
            let newpredlock = npp as *mut PREDICATELOCK;
            if newpredlock.is_null() {
                LWLockRelease(SerializableXactHashLock())?;
                DeleteLockTarget(newtarget, newtargettaghash)?;
                out_of_shmem = true;
                break 'exit;
            }
            if !found {
                crate::ilist_inline::dlist_push_tail(
                    &raw mut (*newtarget).predicateLocks,
                    &raw mut (*newpredlock).targetLink,
                );
                crate::ilist_inline::dlist_push_tail(
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
                    || (*newpredlock).tag.myXact == OldCommittedSxact()
            );
            cur = next;
        }
        LWLockRelease(SerializableXactHashLock())?;

        if removeOld {
            debug_assert!(crate::ilist_inline::dlist_is_empty(
                &raw const (*oldtarget).predicateLocks
            ));
            RemoveTargetIfNoLongerUsed(oldtarget, oldtargettaghash)?;
        }
    }

    // exit:
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

// ===========================================================================
// DropAllPredicateLocksFromTable / TransferPredicateLocksToHeapRelation.
// ===========================================================================

unsafe fn DropAllPredicateLocksFromTable(
    db_id: Oid,
    rel_id: Oid,
    rd_id: Oid,
    uses_local_buffers: bool,
    rd_index_indrelid: Option<Oid>,
    transfer: bool,
) -> PgResult<()> {
    if !TransactionIdIsValid((*PredXact()).SxactGlobalXmin) {
        return Ok(());
    }
    if !predicate_locking_needed(rd_id, uses_local_buffers) {
        return Ok(());
    }

    let (is_index, heap_id) = match rd_index_indrelid {
        None => (false, rel_id),
        Some(indrelid) => (true, indrelid),
    };
    debug_assert!(heap_id != types_core::primitive::InvalidOid);
    debug_assert!(transfer || !is_index);

    let mut heaptargettaghash: u32 = 0;
    let mut heaptarget: *mut PREDICATELOCKTARGET = ptr::null_mut();
    let procno = my_proc_number();

    LWLockAcquire(SerializablePredicateListLock(), LW_EXCLUSIVE, procno)?;
    for i in 0..types_storage::NUM_PREDICATELOCK_PARTITIONS {
        LWLockAcquire(PredicateLockHashPartitionLockByIndex(i), LW_EXCLUSIVE, procno)?;
    }
    LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE, procno)?;

    if transfer {
        RemoveScratchTarget(true)?;
    }

    let mut seqstat = HASH_SEQ_STATUS::default();
    hash_seq_init(&mut seqstat, PredicateLockTargetHash());

    loop {
        let p = hash_seq_search(&mut seqstat)?;
        if p.is_null() {
            break;
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
            let mut heaptargettag = PREDICATELOCKTARGETTAG {
                locktag_field1: 0,
                locktag_field2: 0,
                locktag_field3: 0,
                locktag_field4: 0,
            };
            SET_PREDICATELOCKTARGETTAG_RELATION(&mut heaptargettag, db_id, heap_id);
            heaptargettaghash = PredicateLockTargetTagHashCode(&heaptargettag);
            let (htp, found) = hash_search_with_hash_value(
                PredicateLockTargetHash(),
                &raw const heaptargettag as *const u8,
                heaptargettaghash,
                HASH_ENTER,
            )?;
            heaptarget = htp as *mut PREDICATELOCKTARGET;
            if !found {
                crate::ilist_inline::dlist_init(&raw mut (*heaptarget).predicateLocks);
            }
        }

        // dlist_foreach_modify(iter, &oldtarget->predicateLocks)
        let head = &raw mut (*oldtarget).predicateLocks;
        let mut cur = (*head).head.next;
        while cur != (&raw mut (*head).head) as *mut dlist_node {
            let next = (*cur).next;
            let oldpredlock = dlist_container!(PREDICATELOCK, targetLink, cur);
            let oldCommitSeqNo = (*oldpredlock).commitSeqNo;
            let oldXact = (*oldpredlock).tag.myXact;

            crate::ilist_inline::dlist_delete(&raw mut (*oldpredlock).xactLink);

            let (_p, found) = hash_search(
                PredicateLockHash(),
                &raw const (*oldpredlock).tag as *const u8,
                HASH_REMOVE,
            )?;
            debug_assert!(found);

            if transfer {
                let mut newpredlocktag = PREDICATELOCKTAG {
                    myTarget: heaptarget,
                    myXact: oldXact,
                };
                let (npp, found) = hash_search_with_hash_value(
                    PredicateLockHash(),
                    &raw const newpredlocktag as *const u8,
                    PredicateLockHashCodeFromTargetHashCode(&newpredlocktag, heaptargettaghash),
                    HASH_ENTER,
                )?;
                let newpredlock = npp as *mut PREDICATELOCK;
                if !found {
                    crate::ilist_inline::dlist_push_tail(
                        &raw mut (*heaptarget).predicateLocks,
                        &raw mut (*newpredlock).targetLink,
                    );
                    crate::ilist_inline::dlist_push_tail(
                        &raw mut (*newpredlocktag.myXact).predicateLocks,
                        &raw mut (*newpredlock).xactLink,
                    );
                    (*newpredlock).commitSeqNo = oldCommitSeqNo;
                } else if (*newpredlock).commitSeqNo < oldCommitSeqNo {
                    (*newpredlock).commitSeqNo = oldCommitSeqNo;
                }
                let _ = &mut newpredlocktag;
                debug_assert!((*newpredlock).commitSeqNo != 0);
                debug_assert!(
                    (*newpredlock).commitSeqNo == InvalidSerCommitSeqNo
                        || (*newpredlock).tag.myXact == OldCommittedSxact()
                );
            }
            cur = next;
        }

        let (_p, found) = hash_search(
            PredicateLockTargetHash(),
            &raw const (*oldtarget).tag as *const u8,
            HASH_REMOVE,
        )?;
        debug_assert!(found);
    }

    if transfer {
        RestoreScratchTarget(true)?;
    }

    LWLockRelease(SerializableXactHashLock())?;
    for i in (0..types_storage::NUM_PREDICATELOCK_PARTITIONS).rev() {
        LWLockRelease(PredicateLockHashPartitionLockByIndex(i))?;
    }
    LWLockRelease(SerializablePredicateListLock())?;
    Ok(())
}

/// `TransferPredicateLocksToHeapRelation(relation)`.
pub fn TransferPredicateLocksToHeapRelation(
    db_id: Oid,
    rel_id: Oid,
    rd_id: Oid,
    uses_local_buffers: bool,
    rd_index_indrelid: Option<Oid>,
) -> PgResult<()> {
    unsafe {
        DropAllPredicateLocksFromTable(
            db_id,
            rel_id,
            rd_id,
            uses_local_buffers,
            rd_index_indrelid,
            true,
        )
    }
}

/// `PredicateLockPageSplit(relation, oldblkno, newblkno)`.
pub fn PredicateLockPageSplit(
    db_oid: Oid,
    rd_id: Oid,
    uses_local_buffers: bool,
    oldblkno: BlockNumber,
    newblkno: BlockNumber,
) -> PgResult<()> {
    unsafe {
        if !TransactionIdIsValid((*PredXact()).SxactGlobalXmin) {
            return Ok(());
        }
        if !predicate_locking_needed(rd_id, uses_local_buffers) {
            return Ok(());
        }
        debug_assert!(oldblkno != newblkno);

        let mut oldtargettag = PREDICATELOCKTARGETTAG {
            locktag_field1: 0,
            locktag_field2: 0,
            locktag_field3: 0,
            locktag_field4: 0,
        };
        let mut newtargettag = PREDICATELOCKTARGETTAG {
            locktag_field1: 0,
            locktag_field2: 0,
            locktag_field3: 0,
            locktag_field4: 0,
        };
        SET_PREDICATELOCKTARGETTAG_PAGE(&mut oldtargettag, db_oid, rd_id, oldblkno);
        SET_PREDICATELOCKTARGETTAG_PAGE(&mut newtargettag, db_oid, rd_id, newblkno);

        LWLockAcquire(SerializablePredicateListLock(), LW_EXCLUSIVE, my_proc_number())?;

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

/// `PredicateLockPageCombine(relation, oldblkno, newblkno)`.
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
// Transaction xmin / release.
// ===========================================================================

unsafe fn SetNewSxactGlobalXmin() -> PgResult<()> {
    debug_assert!(LWLockHeldByMe(SerializableXactHashLock()));
    let px = PredXact();
    (*px).SxactGlobalXmin = InvalidTransactionId;
    (*px).SxactGlobalXminCount = 0;

    // dlist_foreach(iter, &PredXact->activeList)
    let head = &raw const (*px).activeList;
    let mut cur = (*head).head.next;
    while cur != (&raw const (*head).head) as *mut dlist_node {
        let sxact = dlist_container!(SERIALIZABLEXACT, xactLink, cur);
        if !SxactIsRolledBack(sxact) && !SxactIsCommitted(sxact) && sxact != OldCommittedSxact() {
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

/// `ReleasePredicateLocks(isCommit, isReadOnlySafe)`.
pub fn ReleasePredicateLocks(mut isCommit: bool, isReadOnlySafe: bool) -> PgResult<()> {
    unsafe {
        let mut partiallyReleasing = false;

        debug_assert!(!(isCommit && isReadOnlySafe));

        if !isReadOnlySafe {
            if is_parallel_worker() {
                ReleasePredicateLocksLocal();
                return Ok(());
            }
            debug_assert!(!parallel_context_active());

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

        let procno = my_proc_number();
        LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE, procno)?;

        let mysx = MySerializableXact();

        if isCommit && SxactIsPartiallyReleased(mysx) {
            isCommit = false;
        }

        if isReadOnlySafe && is_in_parallel_mode() {
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
        debug_assert!((*mysx).pid == 0 || isolation_is_serializable());
        debug_assert!(!SxactIsOnFinishedList(mysx));

        let topLevelIsDeclaredReadOnly = SxactIsReadOnly(mysx);

        (*mysx).finishedBefore = read_next_transaction_id();

        let px = PredXact();
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
            // dlist_foreach_modify(iter, &MySerializableXact->possibleUnsafeConflicts)
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

        // outConflicts
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
                    (*mysx).SeqNo.earliestOutConflictCommit =
                        (*(*conflict).sxactIn).prepareSeqNo;
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

        // inConflicts
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
            // possibleUnsafeConflicts (outLink)
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
                    if crate::ilist_inline::dlist_is_empty(&raw const (*roXact).possibleUnsafeConflicts)
                    {
                        (*roXact).flags |= SXACT_FLAG_RO_SAFE;
                    }
                }

                if SxactIsDeferrableWaiting(roXact)
                    && (SxactIsROUnsafe(roXact) || SxactIsROSafe(roXact))
                {
                    proc_send_signal((*roXact).pgprocno);
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
            crate::ilist_inline::dlist_push_tail(
                FinishedSerializableTransactions(),
                &raw mut (*mysx).finishedLink,
            );
        }

        if !isCommit {
            ReleaseOneSerializableXact(mysx, isReadOnlySafe && is_in_parallel_mode(), false)?;
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
    let procno = my_proc_number();
    LWLockAcquire(SerializableFinishedListLock(), LW_EXCLUSIVE, procno)?;
    LWLockAcquire(SerializableXactHashLock(), LW_SHARED, procno)?;

    let px = PredXact();
    let finished = FinishedSerializableTransactions();
    let mut cur = (*finished).head.next;
    while cur != (&raw mut (*finished).head) as *mut dlist_node {
        let next = (*cur).next;
        let finishedSxact = dlist_container!(SERIALIZABLEXACT, finishedLink, cur);

        if !TransactionIdIsValid((*px).SxactGlobalXmin)
            || TransactionIdPrecedesOrEquals((*finishedSxact).finishedBefore, (*px).SxactGlobalXmin)
        {
            LWLockRelease(SerializableXactHashLock())?;
            crate::ilist_inline::dlist_delete_thoroughly(&raw mut (*finishedSxact).finishedLink);
            ReleaseOneSerializableXact(finishedSxact, false, false)?;
            LWLockAcquire(SerializableXactHashLock(), LW_SHARED, procno)?;
        } else if (*finishedSxact).commitSeqNo > (*px).HavePartialClearedThrough
            && (*finishedSxact).commitSeqNo <= (*px).CanPartialClearThrough
        {
            LWLockRelease(SerializableXactHashLock())?;
            if SxactIsReadOnly(finishedSxact) {
                crate::ilist_inline::dlist_delete_thoroughly(&raw mut (*finishedSxact).finishedLink);
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

    // Loop through predicate locks on dummy transaction for summarized data.
    LWLockAcquire(SerializablePredicateListLock(), LW_SHARED, procno)?;
    let oc = OldCommittedSxact();
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

            crate::ilist_inline::dlist_delete(&raw mut (*predlock).targetLink);
            crate::ilist_inline::dlist_delete(&raw mut (*predlock).xactLink);

            hash_search_with_hash_value(
                PredicateLockHash(),
                &raw const tag as *const u8,
                PredicateLockHashCodeFromTargetHashCode(&tag, targettaghash),
                HASH_REMOVE,
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

    let procno = my_proc_number();

    LWLockAcquire(SerializablePredicateListLock(), LW_SHARED, procno)?;
    if is_in_parallel_mode() {
        LWLockAcquire(&(*sxact).perXactPredicateListLock, LW_EXCLUSIVE, procno)?;
    }

    // dlist_foreach_modify(iter, &sxact->predicateLocks)
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

        crate::ilist_inline::dlist_delete(&raw mut (*predlock).targetLink);

        hash_search_with_hash_value(
            PredicateLockHash(),
            &raw const tag as *const u8,
            PredicateLockHashCodeFromTargetHashCode(&tag, targettaghash),
            HASH_REMOVE,
        )?;

        if summarize {
            tag.myXact = OldCommittedSxact();
            let (pp, found) = hash_search_with_hash_value(
                PredicateLockHash(),
                &raw const tag as *const u8,
                PredicateLockHashCodeFromTargetHashCode(&tag, targettaghash),
                HASH_ENTER_NULL,
            )?;
            predlock = pp as *mut PREDICATELOCK;
            if predlock.is_null() {
                LWLockRelease(partition_lock)?;
                if is_in_parallel_mode() {
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
                crate::ilist_inline::dlist_push_tail(
                    &raw mut (*target).predicateLocks,
                    &raw mut (*predlock).targetLink,
                );
                crate::ilist_inline::dlist_push_tail(
                    &raw mut (*OldCommittedSxact()).predicateLocks,
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

    crate::ilist_inline::dlist_init(&raw mut (*sxact).predicateLocks);

    if is_in_parallel_mode() {
        LWLockRelease(&(*sxact).perXactPredicateListLock)?;
    }
    LWLockRelease(SerializablePredicateListLock())?;

    let sxidtag = SERIALIZABLEXIDTAG {
        xid: (*sxact).topXid,
    };
    LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE, procno)?;

    if !partial {
        // outConflicts
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

    // inConflicts
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
                SerializableXidHash(),
                &raw const sxidtag as *const u8,
                HASH_REMOVE,
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
    debug_assert!(!TransactionIdEquals(xid, get_top_transaction_id_if_any()));

    let snap = get_transaction_snapshot()?;

    if TransactionIdPrecedes(xid, snap.xmin) {
        return Ok(false);
    }
    if TransactionIdFollowsOrEquals(xid, snap.xmax) {
        return Ok(true);
    }
    Ok(pg_lfind32(xid, &snap.xip, snap.xcnt))
}

/// `CheckForSerializableConflictOutNeeded(relation, snapshot)`.
pub fn CheckForSerializableConflictOutNeeded(
    rd_id: Oid,
    uses_local_buffers: bool,
    snapshot: &SnapshotData,
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

/// `CheckForSerializableConflictOut(relation, xid, snapshot)`.
pub fn CheckForSerializableConflictOut(
    rd_id: Oid,
    uses_local_buffers: bool,
    xid: TransactionId,
    snapshot: &SnapshotData,
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

        if TransactionIdEquals(xid, get_top_transaction_id_if_any()) {
            return Ok(());
        }

        let sxidtag = SERIALIZABLEXIDTAG { xid };
        let procno = my_proc_number();
        LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE, procno)?;
        let (sp, _f) = hash_search(
            SerializableXidHash(),
            &raw const sxidtag as *const u8,
            HASH_FIND,
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
                    || !crate::ilist_inline::dlist_is_empty(&raw const (*mysx).inConflicts)
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
                return Err(serialization_failure(
                    "Canceled on conflict out to old pivot.",
                ));
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

        let r = FlagRWConflict(mysx, sxact);
        LWLockRelease(SerializableXactHashLock())?;
        r
    }
}

unsafe fn CheckTargetForConflictsIn(targettag: *mut PREDICATELOCKTARGETTAG) -> PgResult<()> {
    debug_assert!(MySerializableXact() != InvalidSerializableXact);

    let procno = my_proc_number();
    let targettaghash = PredicateLockTargetTagHashCode(targettag);
    let partition_lock = PredicateLockHashPartitionLock(targettaghash);
    LWLockAcquire(partition_lock, LW_SHARED, procno)?;
    let (tp, _f) = hash_search_with_hash_value(
        PredicateLockTargetHash(),
        targettag as *const u8,
        targettaghash,
        HASH_FIND,
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

    // dlist_foreach_modify(iter, &target->predicateLocks)
    let head = &raw mut (*target).predicateLocks;
    let mut cur = (*head).head.next;
    while cur != (&raw mut (*head).head) as *mut dlist_node {
        let next = (*cur).next;
        let predlock = dlist_container!(PREDICATELOCK, targetLink, cur);
        let sxact = (*predlock).tag.myXact;

        if sxact == mysx {
            if !is_sub_transaction() && GET_PREDICATELOCKTARGETTAG_OFFSET(&*targettag) != 0 {
                mypredlock = predlock;
                mypredlocktag = (*predlock).tag;
            }
        } else if !SxactIsDoomed(sxact)
            && (!SxactIsCommitted(sxact)
                || TransactionIdPrecedes(
                    get_transaction_snapshot()?.xmin,
                    (*sxact).finishedBefore,
                ))
            && !RWConflictExists(sxact, mysx)
        {
            LWLockRelease(SerializableXactHashLock())?;
            LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE, procno)?;

            if !SxactIsDoomed(sxact)
                && (!SxactIsCommitted(sxact)
                    || TransactionIdPrecedes(
                        get_transaction_snapshot()?.xmin,
                        (*sxact).finishedBefore,
                    ))
                && !RWConflictExists(sxact, mysx)
            {
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
        if is_in_parallel_mode() {
            LWLockAcquire(&(*mysx).perXactPredicateListLock, LW_EXCLUSIVE, procno)?;
        }
        LWLockAcquire(partition_lock, LW_EXCLUSIVE, procno)?;
        LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE, procno)?;

        let predlockhashcode =
            PredicateLockHashCodeFromTargetHashCode(&mypredlocktag, targettaghash);
        let (rp, _f) = hash_search_with_hash_value(
            PredicateLockHash(),
            &raw const mypredlocktag as *const u8,
            predlockhashcode,
            HASH_FIND,
        )?;
        let mut rmpredlock = rp as *mut PREDICATELOCK;
        if !rmpredlock.is_null() {
            debug_assert!(rmpredlock == mypredlock);

            crate::ilist_inline::dlist_delete(&raw mut (*mypredlock).targetLink);
            crate::ilist_inline::dlist_delete(&raw mut (*mypredlock).xactLink);

            let (rp2, _f) = hash_search_with_hash_value(
                PredicateLockHash(),
                &raw const mypredlocktag as *const u8,
                predlockhashcode,
                HASH_REMOVE,
            )?;
            rmpredlock = rp2 as *mut PREDICATELOCK;
            debug_assert!(rmpredlock == mypredlock);

            RemoveTargetIfNoLongerUsed(target, targettaghash)?;
        }

        LWLockRelease(SerializableXactHashLock())?;
        LWLockRelease(partition_lock)?;
        if is_in_parallel_mode() {
            LWLockRelease(&(*mysx).perXactPredicateListLock)?;
        }
        LWLockRelease(SerializablePredicateListLock())?;

        if !rmpredlock.is_null() {
            hash_search_with_hash_value(
                LocalPredicateLockHash(),
                targettag as *const u8,
                targettaghash,
                HASH_REMOVE,
            )?;
            DecrementParentLocks(targettag)?;
        }
    }
    Ok(())
}

/// `CheckForSerializableConflictIn(relation, tid, blkno)` — over already-
/// projected fields. `tid` is `Some((blkno, offnum))` when not NULL.
pub fn CheckForSerializableConflictIn(
    db_oid: Oid,
    rd_id: Oid,
    uses_local_buffers: bool,
    tid: Option<(BlockNumber, types_core::primitive::OffsetNumber)>,
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

        let mut targettag = PREDICATELOCKTARGETTAG {
            locktag_field1: 0,
            locktag_field2: 0,
            locktag_field3: 0,
            locktag_field4: 0,
        };

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

/// `CheckTableForSerializableConflictIn(relation)`.
pub fn CheckTableForSerializableConflictIn(
    db_id: Oid,
    heap_id: Oid,
    rd_id: Oid,
    uses_local_buffers: bool,
) -> PgResult<()> {
    unsafe {
        if !TransactionIdIsValid((*PredXact()).SxactGlobalXmin) {
            return Ok(());
        }
        if !SerializationNeededForWrite(rd_id, uses_local_buffers) {
            return Ok(());
        }
        set_MyXactDidWrite(true);

        let mysx = MySerializableXact();
        let procno = my_proc_number();

        LWLockAcquire(SerializablePredicateListLock(), LW_EXCLUSIVE, procno)?;
        for i in 0..types_storage::NUM_PREDICATELOCK_PARTITIONS {
            LWLockAcquire(PredicateLockHashPartitionLockByIndex(i), LW_SHARED, procno)?;
        }
        LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE, procno)?;

        let mut seqstat = HASH_SEQ_STATUS::default();
        hash_seq_init(&mut seqstat, PredicateLockTargetHash());

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

            // dlist_foreach_modify(iter, &target->predicateLocks)
            let head = &raw mut (*target).predicateLocks;
            let mut cur = (*head).head.next;
            while cur != (&raw mut (*head).head) as *mut dlist_node {
                let next = (*cur).next;
                let predlock = dlist_container!(PREDICATELOCK, targetLink, cur);
                if (*predlock).tag.myXact != mysx
                    && !RWConflictExists((*predlock).tag.myXact, mysx)
                {
                    FlagRWConflict((*predlock).tag.myXact, mysx)?;
                }
                cur = next;
            }
        }

        LWLockRelease(SerializableXactHashLock())?;
        for i in (0..types_storage::NUM_PREDICATELOCK_PARTITIONS).rev() {
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

    if reader == OldCommittedSxact() {
        (*writer).flags |= SXACT_FLAG_SUMMARY_CONFLICT_IN;
    } else if writer == OldCommittedSxact() {
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
        // dlist_foreach(iter, &writer->outConflicts)
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
            // dlist_foreach(iter, &reader->inConflicts)
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

/// `PreCommit_CheckForSerializationFailure()`.
pub fn PreCommit_CheckForSerializationFailure() -> PgResult<()> {
    unsafe {
        if MySerializableXact() == InvalidSerializableXact {
            return Ok(());
        }
        debug_assert!(isolation_is_serializable());

        let procno = my_proc_number();
        LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE, procno)?;

        let mysx = MySerializableXact();
        if SxactIsDoomed(mysx) && !SxactIsPartiallyReleased(mysx) {
            LWLockRelease(SerializableXactHashLock())?;
            return Err(serialization_failure(
                "Canceled on identification as a pivot, during commit attempt.",
            ));
        }

        // dlist_foreach(near_iter, &MySerializableXact->inConflicts)
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

        let px = PredXact();
        (*px).LastSxactCommitSeqNo += 1;
        (*mysx).prepareSeqNo = (*px).LastSxactCommitSeqNo;
        (*mysx).flags |= SXACT_FLAG_PREPARED;

        LWLockRelease(SerializableXactHashLock())?;
        Ok(())
    }
}

// ===========================================================================
// Two-phase commit support.
// ===========================================================================

/// `AtPrepare_PredicateLocks()`.
pub fn AtPrepare_PredicateLocks() -> PgResult<()> {
    unsafe {
        let sxact = MySerializableXact();

        if sxact == InvalidSerializableXact {
            return Ok(());
        }

        // Generate an xact record.
        let mut record = TwoPhasePredicateRecord {
            r#type: TWOPHASEPREDICATERECORD_XACT,
            data: TwoPhasePredicateRecordData {
                xactRecord: TwoPhasePredicateXactRecord {
                    xmin: (*sxact).xmin,
                    flags: (*sxact).flags,
                },
            },
        };

        let bytes =
            core::slice::from_raw_parts(
                &raw const record as *const u8,
                core::mem::size_of::<TwoPhasePredicateRecord>(),
            );
        register_two_phase_record(
            backend_access_transam_twophase_rmgr::TWOPHASE_RM_PREDICATELOCK_ID,
            0,
            bytes,
        )?;

        LWLockAcquire(SerializablePredicateListLock(), LW_SHARED, my_proc_number())?;
        debug_assert!(!is_parallel_worker() && !parallel_context_active());

        // dlist_foreach(iter, &sxact->predicateLocks)
        let head = &raw const (*sxact).predicateLocks;
        let mut cur = (*head).head.next;
        while cur != (&raw const (*head).head) as *mut dlist_node {
            let predlock = dlist_container!(PREDICATELOCK, xactLink, cur);

            record.r#type = TWOPHASEPREDICATERECORD_LOCK;
            record.data.lockRecord = TwoPhasePredicateLockRecord {
                target: (*(*predlock).tag.myTarget).tag,
                filler: 0,
            };

            let bytes = core::slice::from_raw_parts(
                &raw const record as *const u8,
                core::mem::size_of::<TwoPhasePredicateRecord>(),
            );
            register_two_phase_record(
                backend_access_transam_twophase_rmgr::TWOPHASE_RM_PREDICATELOCK_ID,
                0,
                bytes,
            )?;
            cur = (*cur).next;
        }

        LWLockRelease(SerializablePredicateListLock())?;
        Ok(())
    }
}

/// `PostPrepare_PredicateLocks(xid)`.
pub fn PostPrepare_PredicateLocks(_xid: TransactionId) -> PgResult<()> {
    unsafe {
        if MySerializableXact() == InvalidSerializableXact {
            return Ok(());
        }
        debug_assert!(SxactIsPrepared(MySerializableXact()));

        (*MySerializableXact()).pid = 0;
        (*MySerializableXact()).pgprocno = INVALID_PROC_NUMBER;

        let h = LocalPredicateLockHash();
        if !h.is_null() {
            hash_destroy(h);
        }
        LOCAL_PREDICATE_LOCK_HASH.with(|c| c.set(ptr::null_mut()));

        set_MySerializableXact(InvalidSerializableXact);
        set_MyXactDidWrite(false);
        Ok(())
    }
}

/// `PredicateLockTwoPhaseFinish(xid, isCommit)`.
pub fn PredicateLockTwoPhaseFinish(xid: TransactionId, isCommit: bool) -> PgResult<()> {
    unsafe {
        let sxidtag = SERIALIZABLEXIDTAG { xid };

        LWLockAcquire(SerializableXactHashLock(), LW_SHARED, my_proc_number())?;
        let (sp, _f) = hash_search(
            SerializableXidHash(),
            &raw const sxidtag as *const u8,
            HASH_FIND,
        )?;
        let sxid = sp as *mut SERIALIZABLEXID;
        LWLockRelease(SerializableXactHashLock())?;

        if sxid.is_null() {
            return Ok(());
        }

        set_MySerializableXact((*sxid).myXact);
        set_MyXactDidWrite(true);
        ReleasePredicateLocks(isCommit, false)
    }
}

/// `predicatelock_twophase_recover(xid, info, recdata, len)`.
pub fn predicatelock_twophase_recover(
    xid: TransactionId,
    _info: u16,
    recdata: &[u8],
) -> PgResult<()> {
    unsafe {
        debug_assert!(recdata.len() == core::mem::size_of::<TwoPhasePredicateRecord>());
        let record = &*(recdata.as_ptr() as *const TwoPhasePredicateRecord);

        debug_assert!(
            record.r#type == TWOPHASEPREDICATERECORD_XACT
                || record.r#type == TWOPHASEPREDICATERECORD_LOCK
        );

        if record.r#type == TWOPHASEPREDICATERECORD_XACT {
            let xactRecord = &record.data.xactRecord;

            let procno = my_proc_number();
            LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE, procno)?;
            let sxact = CreatePredXact();
            if sxact.is_null() {
                return Err(PgError::error("out of shared memory").with_sqlstate(ERRCODE_OUT_OF_MEMORY));
            }

            (*sxact).vxid.procNumber = INVALID_PROC_NUMBER;
            (*sxact).vxid.localTransactionId = xid as types_core::primitive::LocalTransactionId;
            (*sxact).pid = 0;
            (*sxact).pgprocno = INVALID_PROC_NUMBER;

            (*sxact).prepareSeqNo = RecoverySerCommitSeqNo;
            (*sxact).commitSeqNo = InvalidSerCommitSeqNo;
            (*sxact).finishedBefore = InvalidTransactionId;
            (*sxact).SeqNo.lastCommitBeforeSnapshot = RecoverySerCommitSeqNo;

            crate::ilist_inline::dlist_init(&raw mut (*sxact).possibleUnsafeConflicts);
            crate::ilist_inline::dlist_init(&raw mut (*sxact).predicateLocks);
            crate::ilist_inline::dlist_node_init(&raw mut (*sxact).finishedLink);

            (*sxact).topXid = xid;
            (*sxact).xmin = xactRecord.xmin;
            (*sxact).flags = xactRecord.flags;
            debug_assert!(SxactIsPrepared(sxact));
            let px = PredXact();
            if !SxactIsReadOnly(sxact) {
                (*px).WritableSxactCount += 1;
                debug_assert!((*px).WritableSxactCount <= (max_backends() + max_prepared_xacts()));
            }

            crate::ilist_inline::dlist_init(&raw mut (*sxact).outConflicts);
            crate::ilist_inline::dlist_init(&raw mut (*sxact).inConflicts);
            (*sxact).flags |= SXACT_FLAG_SUMMARY_CONFLICT_IN;
            (*sxact).flags |= SXACT_FLAG_SUMMARY_CONFLICT_OUT;

            let sxidtag = SERIALIZABLEXIDTAG { xid };
            let (sp, found) = hash_search(
                SerializableXidHash(),
                &raw const sxidtag as *const u8,
                HASH_ENTER,
            )?;
            let sxid = sp as *mut SERIALIZABLEXID;
            debug_assert!(!sxid.is_null());
            debug_assert!(!found);
            (*sxid).myXact = sxact;

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
        } else if record.r#type == TWOPHASEPREDICATERECORD_LOCK {
            let lockRecord = &record.data.lockRecord;
            let targettaghash = PredicateLockTargetTagHashCode(&lockRecord.target);

            LWLockAcquire(SerializableXactHashLock(), LW_SHARED, my_proc_number())?;
            let sxidtag = SERIALIZABLEXIDTAG { xid };
            let (sp, _f) = hash_search(
                SerializableXidHash(),
                &raw const sxidtag as *const u8,
                HASH_FIND,
            )?;
            let sxid = sp as *mut SERIALIZABLEXID;
            LWLockRelease(SerializableXactHashLock())?;

            debug_assert!(!sxid.is_null());
            let sxact = (*sxid).myXact;
            debug_assert!(sxact != InvalidSerializableXact);

            CreatePredicateLock(&lockRecord.target, targettaghash, sxact)?;
        }
        Ok(())
    }
}

// ===========================================================================
// Parallel query sharing.
// ===========================================================================

/// `SerializableXactHandle` (predicate.h) — opaque `void *` into PredXact.
pub type SerializableXactHandle = *mut SERIALIZABLEXACT;

/// `ShareSerializableXact()`.
pub fn ShareSerializableXact() -> SerializableXactHandle {
    MySerializableXact()
}

/// `AttachSerializableXact(handle)`.
pub fn AttachSerializableXact(handle: SerializableXactHandle) -> PgResult<()> {
    debug_assert!(MySerializableXact() == InvalidSerializableXact);
    set_MySerializableXact(handle);
    if MySerializableXact() != InvalidSerializableXact {
        CreateLocalPredicateLockHash()?;
    }
    Ok(())
}

// ===========================================================================
// Unit tests (pure helpers).
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serial::SerialPagePrecedesLogicallyUnitTests;

    #[test]
    fn tag_set_get_type() {
        let mut t = PREDICATELOCKTARGETTAG {
            locktag_field1: 0,
            locktag_field2: 0,
            locktag_field3: 0,
            locktag_field4: 0,
        };
        SET_PREDICATELOCKTARGETTAG_RELATION(&mut t, 5, 7);
        assert_eq!(GET_PREDICATELOCKTARGETTAG_DB(&t), 5);
        assert_eq!(GET_PREDICATELOCKTARGETTAG_RELATION(&t), 7);
        assert_eq!(GET_PREDICATELOCKTARGETTAG_TYPE(&t), PREDLOCKTAG_RELATION);

        SET_PREDICATELOCKTARGETTAG_PAGE(&mut t, 5, 7, 42);
        assert_eq!(GET_PREDICATELOCKTARGETTAG_PAGE(&t), 42);
        assert_eq!(GET_PREDICATELOCKTARGETTAG_TYPE(&t), PREDLOCKTAG_PAGE);

        SET_PREDICATELOCKTARGETTAG_TUPLE(&mut t, 5, 7, 42, 3);
        assert_eq!(GET_PREDICATELOCKTARGETTAG_OFFSET(&t), 3);
        assert_eq!(GET_PREDICATELOCKTARGETTAG_TYPE(&t), PREDLOCKTAG_TUPLE);
    }

    #[test]
    fn target_tag_is_covered_by() {
        let mut rel = PREDICATELOCKTARGETTAG {
            locktag_field1: 0,
            locktag_field2: 0,
            locktag_field3: 0,
            locktag_field4: 0,
        };
        SET_PREDICATELOCKTARGETTAG_RELATION(&mut rel, 1, 2);
        let mut page = rel;
        SET_PREDICATELOCKTARGETTAG_PAGE(&mut page, 1, 2, 9);
        let mut tup = rel;
        SET_PREDICATELOCKTARGETTAG_TUPLE(&mut tup, 1, 2, 9, 4);

        // page covered by relation
        assert!(TargetTagIsCoveredBy(&page, &rel));
        // tuple covered by page
        assert!(TargetTagIsCoveredBy(&tup, &page));
        // tuple NOT covered by a different page
        let mut page2 = rel;
        SET_PREDICATELOCKTARGETTAG_PAGE(&mut page2, 1, 2, 10);
        assert!(!TargetTagIsCoveredBy(&tup, &page2));
        // relation not covered by a page (covering must be coarser)
        assert!(!TargetTagIsCoveredBy(&rel, &page));
    }

    #[test]
    fn serial_page_precedes_unit_tests() {
        SerialPagePrecedesLogicallyUnitTests();
    }

    #[test]
    fn flag_accessors_smoke() {
        unsafe {
            let mut sx: SERIALIZABLEXACT = core::mem::zeroed();
            sx.flags = SXACT_FLAG_COMMITTED | SXACT_FLAG_READ_ONLY;
            let p = &raw const sx;
            assert!(SxactIsCommitted(p));
            assert!(SxactIsReadOnly(p));
            assert!(!SxactIsDoomed(p));
            assert!(!SxactIsPrepared(p));
        }
    }
}
