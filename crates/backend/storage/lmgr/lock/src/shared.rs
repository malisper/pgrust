use std::mem::offset_of;
use std::ptr::{null_mut, NonNull};
use std::sync::OnceLock;

use dynahash::{get_hash_value, hash_search_with_hash_value};
use types_core::{ProcNumber, Size};
use types_error::PgResult;
use types_hash::hsearch::{
    HASHCTL, HASH_BLOBS, HASH_ELEM, HASH_ENTER_NULL, HASH_FIND, HASH_FIXED_SIZE, HASH_FUNCTION,
    HASH_PARTITION, HASH_REMOVE, HASH_SEQ_STATUS, HASH_SHARED_MEM, HTAB,
};
use types_storage::ilist::{dlist_head, dlist_node};
use types_storage::lock::{
    LockMethod, LOCK, LOCKBIT_OFF, LOCKBIT_ON, LOCKMODE, LOCKTAG, LOCKTAG_RELATION, MAX_LOCKMODES,
    PROCLOCK, PROCLOCKTAG,
};
use types_storage::storage::{
    proclist_head, xl_standby_lock, LOG2_NUM_LOCK_PARTITIONS, NUM_LOCK_PARTITIONS,
};

use crate::waitqueue::ProcLockWakeup;

fn NLOCKENTS(max_prepared_xacts: i32) -> i64 {
    crate::max_locks_per_xact() as i64
        * (init_small::globals::MaxBackends() as i64 + max_prepared_xacts as i64)
}

pub(crate) struct SharedTables {
    pub lock_hash: *mut HTAB,
    pub proclock_hash: *mut HTAB,
}

// SAFETY: post-arming, both tables are fully preallocated and fixed: bucket
// chains are mutated only under the owning partition LWLock (C's discipline)
// and entry free/alloc goes through dynahash's per-freelist spinlocks.
unsafe impl Send for SharedTables {}
unsafe impl Sync for SharedTables {}

static SHARED: OnceLock<SharedTables> = OnceLock::new();

pub(crate) fn shared() -> &'static SharedTables {
    SHARED
        .get()
        .unwrap_or_else(|| panic!("lock manager shmem not initialized"))
}

// C's ShmemInitHash flag set (no HASH_ATTACH: one address space). Divergences
// vs C stand: preallocation is max-size (C: init_size then grow within shmem)
// and the bound is entry-count, not shmem bytes; HASH_ENTER_NULL plays C's
// bounded-shmem exhaustion.
fn shmem_init_hash(
    name: &str,
    max_size: i64,
    info: &HASHCTL,
    hash_flags: i32,
) -> PgResult<*mut HTAB> {
    dynahash::hash_create(
        name,
        max_size,
        info,
        hash_flags | HASH_PARTITION | HASH_SHARED_MEM | HASH_FIXED_SIZE,
    )
}

fn proclock_hash(key: &[u8], _keysize: Size) -> u32 {
    // SAFETY: the first 8 key bytes are PROCLOCKTAG.myLock; an unaligned
    // pointer-typed read keeps its provenance through dynahash's key copies.
    let lock: *mut LOCK = unsafe { (key.as_ptr() as *const *mut LOCK).read_unaligned() };
    let mut procno_bytes = [0u8; 4];
    procno_bytes.copy_from_slice(&key[8..12]);
    let procno = i32::from_ne_bytes(procno_bytes);
    // SAFETY: a non-null myLock in a PROCLOCKTAG points at a live LOCK entry
    // (its tag is immutable for the entry's lifetime; preallocation never
    // hashes, so no synthetic key reaches here).
    let lockhash = LockTagHashCode(unsafe { &(*lock).tag });
    // C xors the PGPROC address; the ProcNumber is this port's PGPROC identity.
    lockhash ^ ((procno as u32) << LOG2_NUM_LOCK_PARTITIONS)
}

pub fn LockTagHashCode(locktag: &LOCKTAG) -> u32 {
    get_hash_value(shared().lock_hash, locktag as *const LOCKTAG as *const u8)
}

// Must match proclock_hash!
pub(crate) fn ProcLockHashCode(proclocktag: &PROCLOCKTAG, hashcode: u32) -> u32 {
    hashcode ^ ((proclocktag.myProc as u32) << LOG2_NUM_LOCK_PARTITIONS)
}

pub fn LockHashPartition(hashcode: u32) -> u32 {
    hashcode % NUM_LOCK_PARTITIONS as u32
}

pub(crate) fn LockHashPartitionLock(hashcode: u32) -> &'static lwlock::LWLock {
    lmgr_proc::LockHashPartitionLock(hashcode)
}

pub(crate) fn LockHashPartitionLockByIndex(i: usize) -> &'static lwlock::LWLock {
    lwlock::main_lock(types_storage::storage::LOCK_MANAGER_LWLOCK_OFFSET as usize + i)
}

pub fn LockManagerShmemSize(max_prepared_xacts: i32) -> Size {
    let mut max_table_size = NLOCKENTS(max_prepared_xacts);
    let mut size = dynahash::hash_estimate_size(max_table_size, size_of::<LOCK>());
    max_table_size *= 2;
    size += dynahash::hash_estimate_size(max_table_size, size_of::<PROCLOCK>());
    size + size / 10
}

pub fn LockManagerShmemInit(max_prepared_xacts: i32) -> PgResult<()> {
    if SHARED.get().is_some() {
        return Ok(());
    }
    let max_table_size = NLOCKENTS(max_prepared_xacts);

    let mut info = HASHCTL::new();
    info.keysize = size_of::<LOCKTAG>();
    info.entrysize = size_of::<LOCK>();
    info.num_partitions = NUM_LOCK_PARTITIONS as i64;
    let lock_hash = shmem_init_hash("LOCK hash", max_table_size, &info, HASH_ELEM | HASH_BLOBS)?;

    let max_table_size = max_table_size * 2;
    let mut info = HASHCTL::new();
    info.keysize = size_of::<PROCLOCKTAG>();
    info.entrysize = size_of::<PROCLOCK>();
    info.hash = Some(proclock_hash);
    info.num_partitions = NUM_LOCK_PARTITIONS as i64;
    let proclock_hash_table = shmem_init_hash(
        "PROCLOCK hash",
        max_table_size,
        &info,
        HASH_ELEM | HASH_FUNCTION,
    )?;

    let _ = SHARED.set(SharedTables {
        lock_hash,
        proclock_hash: proclock_hash_table,
    });
    Ok(())
}

/// Crash-cycle reset in place (notes/crash-restart-design.md); sizes are
/// PGC_POSTMASTER-stable, so emptying both hashes restores the boot image.
pub fn LockManagerShmemResetAfterCrash() {
    let tables = shared();
    // SAFETY: crash choreography drained every child before reset; the
    // postmaster thread has exclusive access to both preallocated tables.
    unsafe {
        dynahash::hash_reset_after_crash(tables.lock_hash);
        dynahash::hash_reset_after_crash(tables.proclock_hash);
    }
    crate::fastpath::reset_strong_locks_after_crash();
}

// Intrusive dlist kernel over PROCLOCK links. NULL-terminated (not C's
// circular sentinel), so deletion takes the list head; an all-None head is
// empty — the representation lmgr_proc's emptiness asserts rely on.

pub(crate) unsafe fn dlist_push_tail(head: *mut dlist_head, node: *mut dlist_node) {
    let h = &mut (*head).head;
    (*node).next = None;
    (*node).prev = h.prev;
    match h.prev {
        Some(tail) => (*tail.as_ptr()).next = NonNull::new(node),
        None => h.next = NonNull::new(node),
    }
    h.prev = NonNull::new(node);
}

pub(crate) unsafe fn dlist_delete(head: *mut dlist_head, node: *mut dlist_node) {
    let h = &mut (*head).head;
    match (*node).prev {
        Some(prev) => (*prev.as_ptr()).next = (*node).next,
        None => h.next = (*node).next,
    }
    match (*node).next {
        Some(next) => (*next.as_ptr()).prev = (*node).prev,
        None => h.prev = (*node).prev,
    }
    (*node).next = None;
    (*node).prev = None;
}

pub(crate) fn dlist_is_empty(head: &dlist_head) -> bool {
    head.head.next.is_none()
}

pub(crate) unsafe fn proclock_from_lock_link(node: *mut dlist_node) -> *mut PROCLOCK {
    node.byte_sub(offset_of!(PROCLOCK, lockLink)) as *mut PROCLOCK
}

pub(crate) unsafe fn proclock_from_proc_link(node: *mut dlist_node) -> *mut PROCLOCK {
    node.byte_sub(offset_of!(PROCLOCK, procLink)) as *mut PROCLOCK
}

// Iterates lock->procLocks, deletion-safe; SAFETY contract: partition LWLock held.
pub unsafe fn foreach_proclock_on_lock(
    lock: *mut LOCK,
    mut body: impl FnMut(*mut PROCLOCK) -> bool,
) {
    let mut cur = (*lock).procLocks.head.next;
    while let Some(node) = cur {
        cur = (*node.as_ptr()).next;
        if !body(proclock_from_lock_link(node.as_ptr())) {
            break;
        }
    }
}

pub(crate) unsafe fn foreach_proclock_on_proc_partition(
    head: *mut dlist_head,
    mut body: impl FnMut(*mut PROCLOCK) -> bool,
) {
    let mut cur = (*head).head.next;
    while let Some(node) = cur {
        cur = (*node.as_ptr()).next;
        if !body(proclock_from_proc_link(node.as_ptr())) {
            break;
        }
    }
}

pub(crate) unsafe fn find_lock(locktag: &LOCKTAG, hashcode: u32) -> PgResult<*mut LOCK> {
    Ok(hash_search_with_hash_value(
        shared().lock_hash,
        locktag as *const LOCKTAG as *const u8,
        hashcode,
        HASH_FIND,
        None,
    )? as *mut LOCK)
}

pub(crate) unsafe fn find_proclock(
    lock: *mut LOCK,
    procno: ProcNumber,
    hashcode: u32,
) -> PgResult<*mut PROCLOCK> {
    let proclocktag = PROCLOCKTAG::new(lock, procno);
    let proclock_hashcode = ProcLockHashCode(&proclocktag, hashcode);
    Ok(hash_search_with_hash_value(
        shared().proclock_hash,
        &proclocktag as *const PROCLOCKTAG as *const u8,
        proclock_hashcode,
        HASH_FIND,
        None,
    )? as *mut PROCLOCK)
}

/// Find or create LOCK and PROCLOCK for a new request; null on shmem
/// exhaustion. SAFETY contract: partition lock for `hashcode` held exclusive.
pub(crate) unsafe fn SetupLockInTable(
    lockMethodTable: LockMethod,
    procno: ProcNumber,
    locktag: &LOCKTAG,
    hashcode: u32,
    lockmode: LOCKMODE,
) -> PgResult<*mut PROCLOCK> {
    let mut found = false;
    let lock = hash_search_with_hash_value(
        shared().lock_hash,
        locktag as *const LOCKTAG as *const u8,
        hashcode,
        HASH_ENTER_NULL,
        Some(&mut found),
    )? as *mut LOCK;
    if lock.is_null() {
        return Ok(null_mut());
    }

    if !found {
        (*lock).grantMask = 0;
        (*lock).waitMask = 0;
        (*lock).procLocks = dlist_head::new();
        (*lock).waitProcs.list = proclist_head::default();
        (*lock).waitProcs.count = 0;
        (*lock).nRequested = 0;
        (*lock).nGranted = 0;
        (*lock).requested = [0; MAX_LOCKMODES];
        (*lock).granted = [0; MAX_LOCKMODES];
    } else {
        debug_assert!((*lock).nRequested >= 0 && (*lock).requested[lockmode as usize] >= 0);
        debug_assert!((*lock).nGranted >= 0 && (*lock).granted[lockmode as usize] >= 0);
        debug_assert!((*lock).nGranted <= (*lock).nRequested);
    }

    let proclocktag = PROCLOCKTAG::new(lock, procno);
    let proclock_hashcode = ProcLockHashCode(&proclocktag, hashcode);

    let mut found = false;
    let proclock = hash_search_with_hash_value(
        shared().proclock_hash,
        &proclocktag as *const PROCLOCKTAG as *const u8,
        proclock_hashcode,
        HASH_ENTER_NULL,
        Some(&mut found),
    )? as *mut PROCLOCK;
    if proclock.is_null() {
        if (*lock).nRequested == 0 {
            // Garbage-collect the new lock object; nothing else would ever
            // release it.
            debug_assert!(dlist_is_empty(&(*lock).procLocks));
            let removed = hash_search_with_hash_value(
                shared().lock_hash,
                locktag as *const LOCKTAG as *const u8,
                hashcode,
                HASH_REMOVE,
                None,
            )?;
            assert!(!removed.is_null(), "lock table corrupted");
        }
        return Ok(null_mut());
    }

    if !found {
        let partition = LockHashPartition(hashcode) as usize;
        let proc = lmgr_proc::GetPGProcByNumber(procno);
        // Safe unlocked read: a process's group leader only changes by the
        // process itself, and fast-path transfer implies the source holds
        // this lock (see C comment in SetupLockInTable).
        let leader = proc
            .lockGroupLeader
            .load(std::sync::atomic::Ordering::Relaxed);
        (*proclock).groupLeader = if leader != types_core::INVALID_PROC_NUMBER {
            leader
        } else {
            procno
        };
        (*proclock).holdMask = 0;
        (*proclock).releaseMask = 0;
        dlist_push_tail(&raw mut (*lock).procLocks, &raw mut (*proclock).lockLink);
        dlist_push_tail(proc.myProcLocks[partition].ptr(), &raw mut (*proclock).procLink);
    } else {
        debug_assert!(((*proclock).holdMask & !(*lock).grantMask) == 0);
    }

    (*lock).nRequested += 1;
    (*lock).requested[lockmode as usize] += 1;
    debug_assert!((*lock).nRequested > 0 && (*lock).requested[lockmode as usize] > 0);

    if (*proclock).holdMask & LOCKBIT_ON(lockmode) != 0 {
        return Err(Box::new(types_error::PgError::new(
            types_error::ERROR,
            format!(
                "lock {} on object {}/{}/{} is already held",
                lockMethodTable.lockModeNames[lockmode as usize],
                (*lock).tag.locktag_field1,
                (*lock).tag.locktag_field2,
                (*lock).tag.locktag_field3
            ),
        )));
    }

    Ok(proclock)
}

/// SAFETY contract: partition lock held exclusive; lock/proclock live.
pub(crate) unsafe fn grant_lock_raw(lock: *mut LOCK, proclock: *mut PROCLOCK, lockmode: LOCKMODE) {
    (*lock).nGranted += 1;
    (*lock).granted[lockmode as usize] += 1;
    (*lock).grantMask |= LOCKBIT_ON(lockmode);
    if (*lock).granted[lockmode as usize] == (*lock).requested[lockmode as usize] {
        (*lock).waitMask &= LOCKBIT_OFF(lockmode);
    }
    (*proclock).holdMask |= LOCKBIT_ON(lockmode);
    debug_assert!((*lock).nGranted > 0 && (*lock).granted[lockmode as usize] > 0);
    debug_assert!((*lock).nGranted <= (*lock).nRequested);
}

pub fn GrantLock(lock: *mut LOCK, proclock: *mut PROCLOCK, lockmode: LOCKMODE) {
    // SAFETY: caller holds the partition lock (C contract).
    unsafe { grant_lock_raw(lock, proclock, lockmode) }
}

/// Returns whether ProcLockWakeup is needed.
/// SAFETY contract: partition lock held exclusive.
pub(crate) unsafe fn UnGrantLock(
    lock: *mut LOCK,
    lockmode: LOCKMODE,
    proclock: *mut PROCLOCK,
    lockMethodTable: LockMethod,
) -> bool {
    debug_assert!((*lock).nRequested > 0 && (*lock).requested[lockmode as usize] > 0);
    debug_assert!((*lock).nGranted > 0 && (*lock).granted[lockmode as usize] > 0);
    debug_assert!((*lock).nGranted <= (*lock).nRequested);

    (*lock).nRequested -= 1;
    (*lock).requested[lockmode as usize] -= 1;
    (*lock).nGranted -= 1;
    (*lock).granted[lockmode as usize] -= 1;

    if (*lock).granted[lockmode as usize] == 0 {
        (*lock).grantMask &= LOCKBIT_OFF(lockmode);
    }

    let wakeup_needed =
        lockMethodTable.conflictTab[lockmode as usize] & (*lock).waitMask != 0;

    (*proclock).holdMask &= LOCKBIT_OFF(lockmode);
    wakeup_needed
}

/// SAFETY contract: partition lock for `hashcode` held exclusive.
pub(crate) unsafe fn CleanUpLock(
    lock: *mut LOCK,
    proclock: *mut PROCLOCK,
    lockMethodTable: LockMethod,
    hashcode: u32,
    wakeupNeeded: bool,
) -> PgResult<()> {
    if (*proclock).holdMask == 0 {
        let proclock_hashcode = ProcLockHashCode(&(*proclock).tag, hashcode);
        let partition = LockHashPartition(hashcode) as usize;
        let proc = lmgr_proc::GetPGProcByNumber((*proclock).tag.myProc);
        dlist_delete(&raw mut (*lock).procLocks, &raw mut (*proclock).lockLink);
        dlist_delete(proc.myProcLocks[partition].ptr(), &raw mut (*proclock).procLink);
        let tag = (*proclock).tag;
        let removed = hash_search_with_hash_value(
            shared().proclock_hash,
            &tag as *const PROCLOCKTAG as *const u8,
            proclock_hashcode,
            HASH_REMOVE,
            None,
        )?;
        assert!(!removed.is_null(), "proclock table corrupted");
    }

    if (*lock).nRequested == 0 {
        debug_assert!(dlist_is_empty(&(*lock).procLocks));
        let tag = (*lock).tag;
        let removed = hash_search_with_hash_value(
            shared().lock_hash,
            &tag as *const LOCKTAG as *const u8,
            hashcode,
            HASH_REMOVE,
            None,
        )?;
        assert!(!removed.is_null(), "lock table corrupted");
    } else if wakeupNeeded {
        ProcLockWakeup(lockMethodTable, lock);
    }
    Ok(())
}

/// True iff the request conflicts with granted locks not our own (nor our
/// lock group's). SAFETY contract: partition lock held.
pub(crate) unsafe fn lock_check_conflicts_raw(
    lockMethodTable: LockMethod,
    lockmode: LOCKMODE,
    lock: *mut LOCK,
    proclock: *mut PROCLOCK,
    procno: ProcNumber,
) -> bool {
    let numLockModes = lockMethodTable.numLockModes;
    let conflictMask = lockMethodTable.conflictTab[lockmode as usize];

    if conflictMask & (*lock).grantMask == 0 {
        return false;
    }

    let myLocks = (*proclock).holdMask;
    let mut conflicts_remaining = [0i32; MAX_LOCKMODES];
    let mut total_conflicts = 0;
    for i in 1..=numLockModes as usize {
        if conflictMask & LOCKBIT_ON(i as LOCKMODE) == 0 {
            continue;
        }
        conflicts_remaining[i] = (*lock).granted[i];
        if myLocks & LOCKBIT_ON(i as LOCKMODE) != 0 {
            conflicts_remaining[i] -= 1;
        }
        total_conflicts += conflicts_remaining[i];
    }

    if total_conflicts == 0 {
        return false;
    }

    let proc = lmgr_proc::GetPGProcByNumber(procno);
    let my_leader = proc
        .lockGroupLeader
        .load(std::sync::atomic::Ordering::Relaxed);
    if (*proclock).groupLeader == procno && my_leader == types_core::INVALID_PROC_NUMBER {
        debug_assert_eq!((*proclock).tag.myProc, procno);
        return true;
    }

    // Relation extension locks conflict even between group members.
    if (*lock).tag.locktag_type == types_storage::lock::LOCKTAG_RELATION_EXTEND {
        return true;
    }

    let mut conflict = true;
    foreach_proclock_on_lock(lock, |other| {
        if other != proclock
            && (*other).groupLeader == (*proclock).groupLeader
            && (*other).holdMask & conflictMask != 0
        {
            let intersect = (*other).holdMask & conflictMask;
            for i in 1..=numLockModes as usize {
                if intersect & LOCKBIT_ON(i as LOCKMODE) != 0 {
                    assert!(conflicts_remaining[i] > 0, "proclocks held do not match lock");
                    conflicts_remaining[i] -= 1;
                    total_conflicts -= 1;
                }
            }
            if total_conflicts == 0 {
                conflict = false;
                return false;
            }
        }
        true
    });
    conflict
}

pub fn LockCheckConflicts(
    lockMethodTable: LockMethod,
    lockmode: LOCKMODE,
    lock: *mut LOCK,
    proclock: *mut PROCLOCK,
) -> bool {
    // SAFETY: caller holds the partition lock (C contract).
    unsafe {
        lock_check_conflicts_raw(
            lockMethodTable,
            lockmode,
            lock,
            proclock,
            (*proclock).tag.myProc,
        )
    }
}

/// Refind a lock in shared memory and release it (2PC postcommit and
/// transferred fast-path releases).
pub(crate) fn LockRefindAndRelease(
    lockMethodTable: LockMethod,
    procno: ProcNumber,
    locktag: &LOCKTAG,
    lockmode: LOCKMODE,
    decrement_strong_lock_count: bool,
) -> PgResult<()> {
    let hashcode = LockTagHashCode(locktag);
    let partition_lock = LockHashPartitionLock(hashcode);

    lwlock::LWLockAcquire(partition_lock, lwlock::LW_EXCLUSIVE, crate::my_procno())?;

    // SAFETY: partition lock held exclusive.
    unsafe {
        let lock = find_lock(locktag, hashcode)?;
        assert!(!lock.is_null(), "failed to re-find shared lock object");
        let proclock = find_proclock(lock, procno, hashcode)?;
        assert!(!proclock.is_null(), "failed to re-find shared proclock object");

        if (*proclock).holdMask & LOCKBIT_ON(lockmode) == 0 {
            lwlock::LWLockRelease(partition_lock)?;
            elog_seams::ereport_msg::call(
                types_error::WARNING,
                format!(
                    "you don't own a lock of type {}",
                    lockMethodTable.lockModeNames[lockmode as usize]
                ),
                None,
            )?;
            return Ok(());
        }

        let wakeup_needed = UnGrantLock(lock, lockmode, proclock, lockMethodTable);
        CleanUpLock(lock, proclock, lockMethodTable, hashcode, wakeup_needed)?;
    }

    lwlock::LWLockRelease(partition_lock)?;

    if decrement_strong_lock_count && crate::fastpath::ConflictsWithRelationFastPath(locktag, lockmode)
    {
        crate::fastpath::decrement_strong_lock_count(hashcode);
    }
    Ok(())
}

pub fn GetRunningTransactionLocks() -> PgResult<Vec<xl_standby_lock>> {
    let procno = crate::my_procno();
    // Must grab LWLocks in partition-number order to avoid LWLock deadlock.
    for i in 0..NUM_LOCK_PARTITIONS as usize {
        lwlock::LWLockAcquire(LockHashPartitionLockByIndex(i), lwlock::LW_SHARED, procno)?;
    }

    let els = dynahash::hash_get_num_entries(shared().proclock_hash);
    let mut accessExclusiveLocks = Vec::with_capacity(els as usize);

    let mut seqstat = HASH_SEQ_STATUS::new();
    dynahash::hash_seq_init(&mut seqstat, shared().proclock_hash)?;

    // A granted relation AccessExclusiveLock has exactly one proclock holder,
    // so no dedup is needed (C's caveat about copying this elsewhere stands).
    loop {
        let proclock = dynahash::hash_seq_search(&mut seqstat)? as *mut PROCLOCK;
        if proclock.is_null() {
            break;
        }
        // SAFETY: all partition locks held; entries and their LOCKs are pinned.
        unsafe {
            if (*proclock).holdMask & LOCKBIT_ON(crate::AccessExclusiveLock) != 0
                && (*(*proclock).tag.myLock).tag.locktag_type == LOCKTAG_RELATION
            {
                let proc = lmgr_proc::GetPGProcByNumber((*proclock).tag.myProc);
                let lock = (*proclock).tag.myLock;
                let xid = proc.xid.read();

                // Skip transactions that have already WAL-logged their commit
                // but not yet zeroed their xid / released the lock.
                if !types_core::TransactionIdIsValid(xid) {
                    continue;
                }

                accessExclusiveLocks.push(xl_standby_lock {
                    xid,
                    dbOid: (*lock).tag.locktag_field1,
                    relOid: (*lock).tag.locktag_field2,
                });
            }
        }
    }

    debug_assert!(accessExclusiveLocks.len() as i64 <= els);

    // Reverse order: anyone needing several partitions locks them in
    // increasing order, and this avoids O(N^2) inside LWLockRelease.
    for i in (0..NUM_LOCK_PARTITIONS as usize).rev() {
        lwlock::LWLockRelease(LockHashPartitionLockByIndex(i))?;
    }

    Ok(accessExclusiveLocks)
}
