//! The lock manager's ambient process-global state (`storage/lmgr/lock.c`
//! file-scope statics).
//!
//! In C these are `static HTAB *LockMethodLockHash` /
//! `LockMethodProcLockHash` (shared memory) / `LockMethodLocalHash`
//! (backend-private), the `FastPathStrongRelationLocks` shmem struct, and the
//! `FastPathLocalUseCounts` / `StrongLockInProgress` / `awaitedLock`
//! backend-private scalars.
//!
//! # Cross-process shared LOCK / PROCLOCK tables
//!
//! C's `LockMethodLockHash` / `LockMethodProcLockHash` are `ShmemInitHash`
//! partitioned SHARED hash tables: a lock another backend holds (or that a 2PC
//! `PREPARE TRANSACTION` transferred to a dummy PGPROC) must be visible to a
//! *different* backend that conflicts on it or issues `COMMIT PREPARED`.
//!
//! These were previously modeled as a per-backend `thread_local
//! RefCell<HashMap>` — a fork-COW process-private copy, so cross-backend lock
//! conflicts and 2PC lock transfers were invisible across processes. This
//! module now places the LOCK and PROCLOCK tables in genuine cross-process
//! `MAP_SHARED` memory (the same `ShmemInitStruct` substrate as the 2PC GXACT
//! array and `MainLWLockArray`): a single flat `#[repr(C)]` arena —
//! [`SharedHeader`] + pools — carved once by `LockManagerShmemInit` in the
//! postmaster (pre-`fork`), inherited COW by every backend at the same virtual
//! address. Because that address names `MAP_SHARED` (not COW-private) memory,
//! every backend now sees the SAME LOCK/PROCLOCK entries. The 16 partition
//! LWLocks (`MainLWLockArray`, already shared) serialize all mutation, exactly
//! as in C.
//!
//! The arena is a hand-rolled chained hash (the dynahash-in-shmem shape): a
//! fixed pool of `#[repr(C)]` LOCK and PROCLOCK entries on a free list, two
//! bucket-head arrays, and intrusive index links for each LOCK's holder list
//! and wait queue. This keeps the `lock.c` grant/release/wait spine logic
//! (`locking.rs` / `recovery.rs`) unchanged at the level of the operations it
//! performs — only the backing store moved from a COW `HashMap` to shmem.

#![allow(dead_code)]

use alloc::vec::Vec;
use core::cell::RefCell;
use core::sync::atomic::{AtomicPtr, Ordering};
use std::collections::HashMap;

use types_core::primitive::INVALID_PROC_NUMBER;
use types_core::ProcNumber;
use types_storage::storage::NUM_LOCK_PARTITIONS;
use types_storage::lock::{LOCALLOCK, LOCALLOCKTAG, LOCKMASK, LOCKTAG, MAX_LOCKMODES};

/// `FAST_PATH_STRONG_LOCK_HASH_BITS` / `FAST_PATH_STRONG_LOCK_HASH_PARTITIONS`
/// (lock.c).
pub const FAST_PATH_STRONG_LOCK_HASH_BITS: u32 = 10;
pub const FAST_PATH_STRONG_LOCK_HASH_PARTITIONS: usize =
    1 << FAST_PATH_STRONG_LOCK_HASH_BITS;

/// `FastPathStrongLockHashPartition(hashcode)` (lock.c).
pub fn fast_path_strong_lock_hash_partition(hashcode: u32) -> usize {
    (hashcode as usize) % FAST_PATH_STRONG_LOCK_HASH_PARTITIONS
}

/// `FastPathStrongRelationLockData` (lock.c) — the per-partition strong-lock
/// counts. The `slock_t mutex` is the spinlock guarding `count[]`; in the
/// single-process model the spinlock is a no-op (uncontended), so only the
/// counts are kept.
#[derive(Debug)]
pub struct FastPathStrongRelationLockData {
    /// `uint32 count[FAST_PATH_STRONG_LOCK_HASH_PARTITIONS]`.
    pub count: [u32; FAST_PATH_STRONG_LOCK_HASH_PARTITIONS],
}

impl Default for FastPathStrongRelationLockData {
    fn default() -> Self {
        FastPathStrongRelationLockData {
            count: [0; FAST_PATH_STRONG_LOCK_HASH_PARTITIONS],
        }
    }
}

/// One backend's relationship to one `LOCK` — the genuine per-PROCLOCK state
/// (`PROCLOCK`'s `holdMask` / `releaseMask` / `groupLeader`). Returned by-value
/// from the shared-table accessors; mutations go through the `proclock_*`
/// methods that write the shmem entry in place.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProcLock {
    /// `proclock->groupLeader` — the holder's lock-group leader (its own
    /// `ProcNumber` when not in a group).
    pub group_leader: ProcNumber,
    /// `proclock->holdMask` — bitmask of lock types currently held.
    pub hold_mask: LOCKMASK,
    /// `proclock->releaseMask` — bitmask of lock types marked for release
    /// (`LockReleaseAll`).
    pub release_mask: LOCKMASK,
}

// ===========================================================================
// Flat #[repr(C)] cross-process arena.
//
// Index sentinel: -1 == NIL (no entry). Both pools and both bucket arrays are
// contiguous inside the single ShmemInitStruct allocation.
// ===========================================================================

/// NIL index sentinel for the intrusive free-lists and hash chains.
const NIL: i32 = -1;

/// `#[repr(C)]` LOCK pool slot. Carries the C `LOCK` body fields plus the
/// intrusive links the shmem arena needs (hash chain, free list, holder list
/// head, wait-queue head). The C `dlist procLocks` / `dclist waitProcs` are
/// realized as index lists rooted at `holders_head` / `wait_head`.
#[repr(C)]
#[derive(Clone, Copy)]
struct ShmLock {
    /// `LOCK.tag` — hash key.
    tag: LOCKTAG,
    /// `LOCK.grantMask`.
    grant_mask: LOCKMASK,
    /// `LOCK.waitMask`.
    wait_mask: LOCKMASK,
    /// `LOCK.requested[MAX_LOCKMODES]`.
    requested: [i32; MAX_LOCKMODES],
    /// `LOCK.nRequested`.
    n_requested: i32,
    /// `LOCK.granted[MAX_LOCKMODES]`.
    granted: [i32; MAX_LOCKMODES],
    /// `LOCK.nGranted`.
    n_granted: i32,
    /// Slot is in use (0 = free).
    used: i32,
    /// Hash-chain / free-list link (next slot index, or NIL).
    next: i32,
    /// Head of this lock's holder list (a PROCLOCK pool index, or NIL).
    holders_head: i32,
    /// Head of this lock's wait queue (a PROCLOCK pool index, or NIL).
    wait_head: i32,
}

impl ShmLock {
    const fn zeroed() -> Self {
        ShmLock {
            tag: LOCKTAG {
                locktag_field1: 0,
                locktag_field2: 0,
                locktag_field3: 0,
                locktag_field4: 0,
                locktag_type: 0,
                locktag_lockmethodid: 0,
            },
            grant_mask: 0,
            wait_mask: 0,
            requested: [0; MAX_LOCKMODES],
            n_requested: 0,
            granted: [0; MAX_LOCKMODES],
            n_granted: 0,
            used: 0,
            next: NIL,
            holders_head: NIL,
            wait_head: NIL,
        }
    }
}

/// `#[repr(C)]` PROCLOCK pool slot. Key is `(myLock tag, myProc ProcNumber)`;
/// carries the per-PROCLOCK state plus the intrusive list links: the hash
/// chain, the free list, the owning LOCK's holder list (`holder_next`), and the
/// wait-queue link (`wait_next` / a membership flag).
#[repr(C)]
#[derive(Clone, Copy)]
struct ShmProcLock {
    /// `PROCLOCKTAG.myLock` tag.
    tag: LOCKTAG,
    /// `PROCLOCKTAG.myProc` proc number.
    holder: ProcNumber,
    /// `PROCLOCK.groupLeader` proc number.
    group_leader: ProcNumber,
    /// `PROCLOCK.holdMask`.
    hold_mask: LOCKMASK,
    /// `PROCLOCK.releaseMask`.
    release_mask: LOCKMASK,
    /// Slot is in use (0 = free).
    used: i32,
    /// PROCLOCK-hash chain / free-list link.
    next: i32,
    /// Owning LOCK's holder list link (`LOCK.procLocks` dlist).
    holder_next: i32,
    /// Owning LOCK's wait-queue link (`LOCK.waitProcs` dclist), or NIL when not
    /// queued.
    wait_next: i32,
    /// 1 when this PROCLOCK's holder is currently on its LOCK's wait queue.
    on_wait_queue: i32,
    /// Holder's per-partition `myProcLocks[partition]` list link (next), or NIL.
    /// Mirrors C's `PROCLOCK.procLink` (the `dlist_node` chained into
    /// `PGPROC.myProcLocks[partition]`). The partition is
    /// `LockHashPartition(LockTagHashCode(tag))`.
    my_proc_next: i32,
    /// Holder's per-partition `myProcLocks` list link (prev), or NIL. Kept so
    /// removal is O(1) like C's doubly-linked `dlist`.
    my_proc_prev: i32,
}

impl ShmProcLock {
    const fn zeroed() -> Self {
        ShmProcLock {
            tag: LOCKTAG {
                locktag_field1: 0,
                locktag_field2: 0,
                locktag_field3: 0,
                locktag_field4: 0,
                locktag_type: 0,
                locktag_lockmethodid: 0,
            },
            holder: INVALID_PROC_NUMBER,
            group_leader: INVALID_PROC_NUMBER,
            hold_mask: 0,
            release_mask: 0,
            used: 0,
            next: NIL,
            holder_next: NIL,
            wait_next: NIL,
            on_wait_queue: 0,
            my_proc_next: NIL,
            my_proc_prev: NIL,
        }
    }
}

/// `#[repr(C)]` header for the shared lock-table arena. Followed in the same
/// `ShmemInitStruct` allocation by the LOCK bucket array, the PROCLOCK bucket
/// array, the LOCK pool, and the PROCLOCK pool (in that order). The handle
/// [`SharedLockTable`] reconstructs the sub-slice pointers from `n_buckets` /
/// `n_locks` / `n_proclocks`.
#[repr(C)]
struct SharedHeader {
    /// Number of hash buckets for each table (same for both).
    n_buckets: i32,
    /// LOCK pool capacity.
    n_locks: i32,
    /// PROCLOCK pool capacity.
    n_proclocks: i32,
    /// Number of PGPROC slots (the holder `ProcNumber` range:
    /// `MaxBackends + NUM_AUXILIARY_PROCS + max_prepared_xacts`). The per-proc
    /// `myProcLocks` head array is `n_procs * NUM_LOCK_PARTITIONS` entries.
    n_procs: i32,
    /// Free-list head for the LOCK pool.
    lock_free_head: i32,
    /// Free-list head for the PROCLOCK pool.
    proclock_free_head: i32,
    /// Live LOCK count (diagnostics / capacity guard).
    n_locks_used: i32,
    /// Live PROCLOCK count.
    n_proclocks_used: i32,
    /// Spinlock word guarding the two pool free-lists (`lock_free_head` /
    /// `proclock_free_head`) and the live counters. The per-tag hash buckets are
    /// already serialized by the tag's partition LWLock (a tag maps to exactly
    /// one partition, and same-bucket implies same-partition because the bucket's
    /// low bits equal the partition index), but the free-lists and counters are
    /// a *single* shared resource crossing all partitions: a backend mutating a
    /// lock in partition A and another in partition B hold different partition
    /// LWLocks yet both pop/push the same free-list. Without this dedicated lock
    /// the concurrent pops lose updates and corrupt the list (parallel-worker
    /// crash: `out of shared PROCLOCK entries` with the list reporting empty
    /// while thousands of slots are free). 0 = unlocked, 1 = locked. Mirrors C
    /// dynahash's per-freelist `mutex` spinlock.
    freelist_lock: i32,
}

/// Round `x` up to `MAXIMUM_ALIGNOF` (8) so each sub-array starts aligned.
const fn maxalign(x: usize) -> usize {
    (x + 7) & !7
}

/// Byte offsets and total size of the flat arena for `n_buckets` buckets,
/// `n_locks` LOCK slots, `n_proclocks` PROCLOCK slots.
struct Offsets {
    lock_buckets: usize,
    proclock_buckets: usize,
    /// Per-proc, per-partition `myProcLocks` list heads
    /// (`n_procs * NUM_LOCK_PARTITIONS` i32 slot indices).
    my_proc_heads: usize,
    lock_pool: usize,
    proclock_pool: usize,
    total: usize,
}

/// Number of per-proc `myProcLocks` head entries for `n_procs` PGPROC slots.
fn my_proc_heads_len(n_procs: usize) -> usize {
    n_procs * (NUM_LOCK_PARTITIONS as usize)
}

fn compute_offsets(
    n_buckets: usize,
    n_locks: usize,
    n_proclocks: usize,
    n_procs: usize,
) -> Offsets {
    let mut off = maxalign(core::mem::size_of::<SharedHeader>());
    let lock_buckets = off;
    off += maxalign(n_buckets * core::mem::size_of::<i32>());
    let proclock_buckets = off;
    off += maxalign(n_buckets * core::mem::size_of::<i32>());
    let my_proc_heads = off;
    off += maxalign(my_proc_heads_len(n_procs) * core::mem::size_of::<i32>());
    let lock_pool = off;
    off += maxalign(n_locks * core::mem::size_of::<ShmLock>());
    let proclock_pool = off;
    off += maxalign(n_proclocks * core::mem::size_of::<ShmProcLock>());
    Offsets {
        lock_buckets,
        proclock_buckets,
        my_proc_heads,
        lock_pool,
        proclock_pool,
        total: off,
    }
}

/// The base pointer of the shared lock-table arena, set once by
/// `lock_manager_shmem_init` in the postmaster (pre-fork) and inherited at the
/// same VA by every forked backend. Reads/writes hit genuine `MAP_SHARED`
/// memory.
static SHARED_BASE: AtomicPtr<u8> = AtomicPtr::new(core::ptr::null_mut());

/// `LockManagerShmemInit` (postmaster, pre-fork): record the arena base and, on
/// first creation, initialize the header / bucket arrays / free lists. `found`
/// mirrors C's `*foundPtr` (true on re-attach; only the creator initializes).
pub(crate) fn shmem_init(
    base: *mut u8,
    found: bool,
    n_locks: usize,
    n_proclocks: usize,
    n_procs: usize,
) {
    SHARED_BASE.store(base, Ordering::Relaxed);
    if found {
        return;
    }
    // Bucket count: round the lock capacity up to a power of two for a cheap
    // mask, with a floor so tiny configs still hash sanely.
    let n_buckets = next_pow2(n_locks.max(16));
    let offs = compute_offsets(n_buckets, n_locks, n_proclocks, n_procs);

    unsafe {
        let hdr = base as *mut SharedHeader;
        (*hdr).n_buckets = n_buckets as i32;
        (*hdr).n_locks = n_locks as i32;
        (*hdr).n_proclocks = n_proclocks as i32;
        (*hdr).n_procs = n_procs as i32;
        (*hdr).n_locks_used = 0;
        (*hdr).n_proclocks_used = 0;
        (*hdr).freelist_lock = 0;

        // Bucket heads = NIL.
        let lb = base.add(offs.lock_buckets) as *mut i32;
        let pb = base.add(offs.proclock_buckets) as *mut i32;
        for i in 0..n_buckets {
            *lb.add(i) = NIL;
            *pb.add(i) = NIL;
        }

        // Per-proc myProcLocks heads = NIL (every proc starts holding nothing).
        let mph = base.add(offs.my_proc_heads) as *mut i32;
        for i in 0..my_proc_heads_len(n_procs) {
            *mph.add(i) = NIL;
        }

        // LOCK pool: zero + thread free list 0 -> 1 -> ... -> NIL.
        let lp = base.add(offs.lock_pool) as *mut ShmLock;
        for i in 0..n_locks {
            *lp.add(i) = ShmLock::zeroed();
            (*lp.add(i)).next = if i + 1 < n_locks { (i + 1) as i32 } else { NIL };
        }
        (*hdr).lock_free_head = if n_locks > 0 { 0 } else { NIL };

        // PROCLOCK pool: zero + thread free list.
        let pp = base.add(offs.proclock_pool) as *mut ShmProcLock;
        for i in 0..n_proclocks {
            *pp.add(i) = ShmProcLock::zeroed();
            (*pp.add(i)).next = if i + 1 < n_proclocks { (i + 1) as i32 } else { NIL };
        }
        (*hdr).proclock_free_head = if n_proclocks > 0 { 0 } else { NIL };
    }
}

fn next_pow2(mut x: usize) -> usize {
    if x == 0 {
        return 1;
    }
    x -= 1;
    x |= x >> 1;
    x |= x >> 2;
    x |= x >> 4;
    x |= x >> 8;
    x |= x >> 16;
    x |= x >> 32;
    x + 1
}

/// Total arena bytes for a given capacity (used by `LockManagerShmemSize`).
pub(crate) fn arena_bytes(n_locks: usize, n_proclocks: usize, n_procs: usize) -> usize {
    let n_buckets = next_pow2(n_locks.max(16));
    compute_offsets(n_buckets, n_locks, n_proclocks, n_procs).total
}

// ===========================================================================
// SharedLockTable — a thin handle over the shmem arena. Reconstructs the
// sub-slice raw pointers from the header each access. Every method serializes
// under the caller-held partition LWLock (the C contract); the handle itself
// holds no Rust borrow across calls, so concurrent backends are free to touch
// other partitions.
// ===========================================================================

/// Hash a LOCKTAG into a bucket index. Reuses `LockTagHashCode` (the same
/// `tag_hash` dynahash uses) so the value is identical across backends.
fn bucket_of(tag: &LOCKTAG, n_buckets: usize) -> usize {
    (crate::LockTagHashCode(tag) as usize) & (n_buckets - 1)
}

/// The lock manager's shared lock-table substrate (the C `LockMethodLockHash` +
/// `LockMethodProcLockHash` shmem hash tables). A thin handle; all state lives
/// in the [`SHARED_BASE`] arena.
pub(crate) struct SharedLockTable {
    base: *mut u8,
}

impl SharedLockTable {
    fn hdr(&self) -> *mut SharedHeader {
        self.base as *mut SharedHeader
    }
    fn n_buckets(&self) -> usize {
        unsafe { (*self.hdr()).n_buckets as usize }
    }
    fn n_locks(&self) -> usize {
        unsafe { (*self.hdr()).n_locks as usize }
    }
    fn n_proclocks(&self) -> usize {
        unsafe { (*self.hdr()).n_proclocks as usize }
    }
    fn n_procs(&self) -> usize {
        unsafe { (*self.hdr()).n_procs as usize }
    }
    fn offsets(&self) -> Offsets {
        compute_offsets(
            self.n_buckets(),
            self.n_locks(),
            self.n_proclocks(),
            self.n_procs(),
        )
    }
    fn my_proc_heads(&self) -> *mut i32 {
        unsafe { self.base.add(self.offsets().my_proc_heads) as *mut i32 }
    }
    /// Index into the `my_proc_heads` array for `(holder, partition)`.
    fn my_proc_head_idx(&self, holder: ProcNumber, partition: i32) -> usize {
        debug_assert!(holder >= 0 && (holder as usize) < self.n_procs());
        debug_assert!(partition >= 0 && partition < NUM_LOCK_PARTITIONS);
        (holder as usize) * (NUM_LOCK_PARTITIONS as usize) + (partition as usize)
    }
    /// The lock partition of a tag (`LockHashPartition(LockTagHashCode(tag))`).
    fn tag_partition(tag: &LOCKTAG) -> i32 {
        (crate::LockTagHashCode(tag) % (NUM_LOCK_PARTITIONS as u32)) as i32
    }
    fn lock_buckets(&self) -> *mut i32 {
        unsafe { self.base.add(self.offsets().lock_buckets) as *mut i32 }
    }
    fn proclock_buckets(&self) -> *mut i32 {
        unsafe { self.base.add(self.offsets().proclock_buckets) as *mut i32 }
    }
    fn lock_pool(&self) -> *mut ShmLock {
        unsafe { self.base.add(self.offsets().lock_pool) as *mut ShmLock }
    }
    fn proclock_pool(&self) -> *mut ShmProcLock {
        unsafe { self.base.add(self.offsets().proclock_pool) as *mut ShmProcLock }
    }

    fn lock_at(&self, idx: i32) -> *mut ShmLock {
        debug_assert!(idx >= 0);
        unsafe { self.lock_pool().add(idx as usize) }
    }
    fn proclock_at(&self, idx: i32) -> *mut ShmProcLock {
        debug_assert!(idx >= 0);
        unsafe { self.proclock_pool().add(idx as usize) }
    }

    /// View of the header's `freelist_lock` word as an atomic (the word lives in
    /// shmem; every backend reaches the SAME bytes at the same VA).
    fn freelist_lock_word(&self) -> &core::sync::atomic::AtomicI32 {
        unsafe {
            let p = core::ptr::addr_of_mut!((*self.hdr()).freelist_lock);
            core::sync::atomic::AtomicI32::from_ptr(p)
        }
    }

    /// Acquire the free-list spinlock (CAS 0 -> 1 with a spin-wait), run `f`,
    /// then release. This is the dedicated lock for the cross-partition free-list
    /// + counter mutations; it is INDEPENDENT of the partition LWLocks (a backend
    /// may already hold a partition lock — that does not exclude another backend
    /// operating in a different partition from touching the shared free-list).
    fn with_freelist_lock<R>(&self, f: impl FnOnce() -> R) -> R {
        let w = self.freelist_lock_word();
        // Spin until we win the 0 -> 1 transition. The critical section is a
        // handful of pointer writes, so unbounded spinning is fine (matches the
        // C dynahash freelist spinlock, which is similarly tiny).
        loop {
            if w
                .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
            core::hint::spin_loop();
        }
        let out = f();
        w.store(0, Ordering::Release);
        out
    }

    /// Find the LOCK slot index for `tag`, or NIL.
    fn find_lock(&self, tag: &LOCKTAG) -> i32 {
        let b = bucket_of(tag, self.n_buckets());
        let mut cur = unsafe { *self.lock_buckets().add(b) };
        while cur != NIL {
            let s = self.lock_at(cur);
            unsafe {
                if (*s).used != 0 && (*s).tag == *tag {
                    return cur;
                }
                cur = (*s).next;
            }
        }
        NIL
    }

    fn find_proclock(&self, tag: &LOCKTAG, holder: ProcNumber) -> i32 {
        let b = bucket_of(tag, self.n_buckets());
        let mut cur = unsafe { *self.proclock_buckets().add(b) };
        while cur != NIL {
            let s = self.proclock_at(cur);
            unsafe {
                if (*s).used != 0 && (*s).tag == *tag && (*s).holder == holder {
                    return cur;
                }
                cur = (*s).next;
            }
        }
        NIL
    }

    // ---- public LOCK accessors -----------------------------------------

    pub(crate) fn lock_exists(&self, tag: &LOCKTAG) -> bool {
        self.find_lock(tag) != NIL
    }

    pub(crate) fn proclock_exists(&self, tag: &LOCKTAG, holder: ProcNumber) -> bool {
        self.find_proclock(tag, holder) != NIL
    }

    /// Create the LOCK entry for `tag` if absent (the C
    /// `hash_search(HASH_ENTER)` for the LOCK, zeroing the body).
    pub(crate) fn lock_get_or_create(&mut self, tag: &LOCKTAG) {
        if self.find_lock(tag) != NIL {
            return;
        }
        let idx = self.alloc_lock();
        let s = self.lock_at(idx);
        unsafe {
            *s = ShmLock::zeroed();
            (*s).tag = *tag;
            (*s).used = 1;
            let b = bucket_of(tag, self.n_buckets());
            (*s).next = *self.lock_buckets().add(b);
            *self.lock_buckets().add(b) = idx;
        }
    }

    fn alloc_lock(&mut self) -> i32 {
        // Pop the free-list head + bump the live counter atomically w.r.t. other
        // backends operating in different partitions (the free-list is shared
        // across all partitions; the partition LWLock does not exclude them).
        self.with_freelist_lock(|| unsafe {
            let head = (*self.hdr()).lock_free_head;
            assert!(head != NIL, "out of shared LOCK entries");
            (*self.hdr()).lock_free_head = (*self.lock_at(head)).next;
            (*self.hdr()).n_locks_used += 1;
            head
        })
    }

    fn free_lock(&mut self, idx: i32) {
        unsafe {
            let s = self.lock_at(idx);
            (*s).used = 0;
        }
        self.with_freelist_lock(|| unsafe {
            let s = self.lock_at(idx);
            (*s).next = (*self.hdr()).lock_free_head;
            (*self.hdr()).lock_free_head = idx;
            (*self.hdr()).n_locks_used -= 1;
        });
    }

    /// Remove the LOCK entry for `tag` (the C `hash_search(HASH_REMOVE)`). The
    /// holder list / wait queue must already be empty.
    pub(crate) fn lock_remove(&mut self, tag: &LOCKTAG) {
        let b = bucket_of(tag, self.n_buckets());
        let mut prev = NIL;
        let mut cur = unsafe { *self.lock_buckets().add(b) };
        while cur != NIL {
            let (used, this_tag, next) = unsafe {
                let s = self.lock_at(cur);
                ((*s).used, (*s).tag, (*s).next)
            };
            if used != 0 && this_tag == *tag {
                unsafe {
                    if prev == NIL {
                        *self.lock_buckets().add(b) = next;
                    } else {
                        (*self.lock_at(prev)).next = next;
                    }
                }
                self.free_lock(cur);
                return;
            }
            prev = cur;
            cur = next;
        }
    }

    // ---- LOCK body field views -----------------------------------------

    pub(crate) fn lock_with<R>(&self, tag: &LOCKTAG, f: impl FnOnce(&LockBody) -> R) -> Option<R> {
        let idx = self.find_lock(tag);
        if idx == NIL {
            return None;
        }
        let body = LockBody { s: self.lock_at(idx) };
        Some(f(&body))
    }

    pub(crate) fn lock_with_mut<R>(
        &mut self,
        tag: &LOCKTAG,
        f: impl FnOnce(&mut LockBodyMut) -> R,
    ) -> Option<R> {
        let idx = self.find_lock(tag);
        if idx == NIL {
            return None;
        }
        let mut body = LockBodyMut { s: self.lock_at(idx) };
        Some(f(&mut body))
    }

    /// `lock->waitMask` (0 if absent).
    pub(crate) fn lock_wait_mask(&self, tag: &LOCKTAG) -> LOCKMASK {
        self.lock_with(tag, |b| b.wait_mask()).unwrap_or(0)
    }
    /// `lock->grantMask` (0 if absent).
    pub(crate) fn lock_grant_mask(&self, tag: &LOCKTAG) -> LOCKMASK {
        self.lock_with(tag, |b| b.grant_mask()).unwrap_or(0)
    }
    /// `lock->nRequested` (0 if absent).
    pub(crate) fn lock_n_requested(&self, tag: &LOCKTAG) -> i32 {
        self.lock_with(tag, |b| b.n_requested()).unwrap_or(0)
    }

    // ---- holder list ---------------------------------------------------

    /// `dlist_push_tail(&lock->procLocks, &proclock->lockLink)`.
    fn holder_push(&mut self, lock_idx: i32, pidx: i32) {
        unsafe {
            (*self.proclock_at(pidx)).holder_next = NIL;
            let head = (*self.lock_at(lock_idx)).holders_head;
            if head == NIL {
                (*self.lock_at(lock_idx)).holders_head = pidx;
                return;
            }
            let mut cur = head;
            while (*self.proclock_at(cur)).holder_next != NIL {
                cur = (*self.proclock_at(cur)).holder_next;
            }
            (*self.proclock_at(cur)).holder_next = pidx;
        }
    }

    fn holder_remove(&mut self, lock_idx: i32, pidx: i32) {
        unsafe {
            let mut prev = NIL;
            let mut cur = (*self.lock_at(lock_idx)).holders_head;
            while cur != NIL {
                if cur == pidx {
                    let next = (*self.proclock_at(cur)).holder_next;
                    if prev == NIL {
                        (*self.lock_at(lock_idx)).holders_head = next;
                    } else {
                        (*self.proclock_at(prev)).holder_next = next;
                    }
                    (*self.proclock_at(pidx)).holder_next = NIL;
                    return;
                }
                prev = cur;
                cur = (*self.proclock_at(cur)).holder_next;
            }
        }
    }

    // ---- per-PGPROC myProcLocks list -----------------------------------
    //
    // Mirrors C's `PGPROC.myProcLocks[NUM_LOCK_PARTITIONS]` doubly-linked list
    // (`dlist_push_tail`/`dlist_delete` of `PROCLOCK.procLink`). It lets
    // `LockReleaseAll` walk only the proclocks THIS backend holds in a
    // partition, instead of seq-scanning the whole shared PROCLOCK slab.
    // Maintained under the same partition LWLock the caller already holds for
    // the proclock's tag (the head lives logically with that partition).

    /// `dlist_push_tail(&proc->myProcLocks[partition], &proclock->procLink)`.
    /// Push tail so the list is iterated in acquisition order, like C.
    fn my_proc_push(&mut self, holder: ProcNumber, partition: i32, pidx: i32) {
        let head_idx = self.my_proc_head_idx(holder, partition);
        unsafe {
            let mph = self.my_proc_heads();
            (*self.proclock_at(pidx)).my_proc_next = NIL;
            let head = *mph.add(head_idx);
            if head == NIL {
                (*self.proclock_at(pidx)).my_proc_prev = NIL;
                *mph.add(head_idx) = pidx;
                return;
            }
            let mut cur = head;
            while (*self.proclock_at(cur)).my_proc_next != NIL {
                cur = (*self.proclock_at(cur)).my_proc_next;
            }
            (*self.proclock_at(cur)).my_proc_next = pidx;
            (*self.proclock_at(pidx)).my_proc_prev = cur;
        }
    }

    /// `dlist_delete(&proclock->procLink)` — unlink from the holder's
    /// per-partition myProcLocks list. O(1) via the prev/next links.
    fn my_proc_remove(&mut self, holder: ProcNumber, partition: i32, pidx: i32) {
        let head_idx = self.my_proc_head_idx(holder, partition);
        unsafe {
            let mph = self.my_proc_heads();
            let prev = (*self.proclock_at(pidx)).my_proc_prev;
            let next = (*self.proclock_at(pidx)).my_proc_next;
            if prev == NIL {
                // Was the head (or not linked; guard via head check).
                if *mph.add(head_idx) == pidx {
                    *mph.add(head_idx) = next;
                }
            } else {
                (*self.proclock_at(prev)).my_proc_next = next;
            }
            if next != NIL {
                (*self.proclock_at(next)).my_proc_prev = prev;
            }
            (*self.proclock_at(pidx)).my_proc_next = NIL;
            (*self.proclock_at(pidx)).my_proc_prev = NIL;
        }
    }

    /// The PROCLOCK tags `holder` holds in `partition`, in list order — the C
    /// `dlist_foreach(&MyProc->myProcLocks[partition])`. O(held), NOT O(slab).
    pub(crate) fn my_proc_lock_tags(
        &self,
        holder: ProcNumber,
        partition: i32,
    ) -> Vec<LOCKTAG> {
        let mut out = Vec::new();
        if holder < 0 || (holder as usize) >= self.n_procs() {
            return out;
        }
        let head_idx = self.my_proc_head_idx(holder, partition);
        let mut cur = unsafe { *self.my_proc_heads().add(head_idx) };
        while cur != NIL {
            unsafe {
                debug_assert!((*self.proclock_at(cur)).used != 0);
                debug_assert_eq!((*self.proclock_at(cur)).holder, holder);
                out.push((*self.proclock_at(cur)).tag);
                cur = (*self.proclock_at(cur)).my_proc_next;
            }
        }
        out
    }

    /// The holder `ProcNumber`s of a LOCK, in list order.
    pub(crate) fn holders(&self, tag: &LOCKTAG) -> Vec<ProcNumber> {
        let mut out = Vec::new();
        let lidx = self.find_lock(tag);
        if lidx == NIL {
            return out;
        }
        let mut cur = unsafe { (*self.lock_at(lidx)).holders_head };
        while cur != NIL {
            unsafe {
                out.push((*self.proclock_at(cur)).holder);
                cur = (*self.proclock_at(cur)).holder_next;
            }
        }
        out
    }

    // ---- PROCLOCK accessors --------------------------------------------

    pub(crate) fn proclock_get(&self, tag: &LOCKTAG, holder: ProcNumber) -> Option<ProcLock> {
        let idx = self.find_proclock(tag, holder);
        if idx == NIL {
            return None;
        }
        let s = self.proclock_at(idx);
        unsafe {
            Some(ProcLock {
                group_leader: (*s).group_leader,
                hold_mask: (*s).hold_mask,
                release_mask: (*s).release_mask,
            })
        }
    }

    /// `proclock->holdMask` (0 if absent).
    pub(crate) fn proclock_hold_mask(&self, tag: &LOCKTAG, holder: ProcNumber) -> LOCKMASK {
        self.proclock_get(tag, holder).map(|p| p.hold_mask).unwrap_or(0)
    }

    /// Insert a PROCLOCK for `(tag, holder)` and chain it into its LOCK's holder
    /// list. The LOCK must already exist (`lock_get_or_create`). If a PROCLOCK
    /// already exists for the key, its fields are overwritten.
    pub(crate) fn proclock_insert(&mut self, tag: &LOCKTAG, holder: ProcNumber, pl: ProcLock) {
        let existing = self.find_proclock(tag, holder);
        if existing != NIL {
            let s = self.proclock_at(existing);
            unsafe {
                (*s).group_leader = pl.group_leader;
                (*s).hold_mask = pl.hold_mask;
                (*s).release_mask = pl.release_mask;
            }
            return;
        }
        let idx = self.alloc_proclock();
        unsafe {
            let s = self.proclock_at(idx);
            *s = ShmProcLock::zeroed();
            (*s).tag = *tag;
            (*s).holder = holder;
            (*s).group_leader = pl.group_leader;
            (*s).hold_mask = pl.hold_mask;
            (*s).release_mask = pl.release_mask;
            (*s).used = 1;
            let b = bucket_of(tag, self.n_buckets());
            (*s).next = *self.proclock_buckets().add(b);
            *self.proclock_buckets().add(b) = idx;
        }
        let lidx = self.find_lock(tag);
        if lidx != NIL {
            self.holder_push(lidx, idx);
        }
        // Chain into the holder's per-partition myProcLocks list (C's
        // `dlist_push_tail(&proc->myProcLocks[partition], &proclock->procLink)`).
        self.my_proc_push(holder, Self::tag_partition(tag), idx);
    }

    fn alloc_proclock(&mut self) -> i32 {
        // See `alloc_lock`: the PROCLOCK free-list + counter cross all partitions
        // and must be mutated under the dedicated free-list spinlock, not merely
        // the (per-partition) lock the caller holds.
        self.with_freelist_lock(|| unsafe {
            let head = (*self.hdr()).proclock_free_head;
            assert!(head != NIL, "out of shared PROCLOCK entries");
            (*self.hdr()).proclock_free_head = (*self.proclock_at(head)).next;
            (*self.hdr()).n_proclocks_used += 1;
            head
        })
    }

    fn free_proclock(&mut self, idx: i32) {
        unsafe {
            let s = self.proclock_at(idx);
            (*s).used = 0;
        }
        self.with_freelist_lock(|| unsafe {
            let s = self.proclock_at(idx);
            (*s).next = (*self.hdr()).proclock_free_head;
            (*self.hdr()).proclock_free_head = idx;
            (*self.hdr()).n_proclocks_used -= 1;
        });
    }

    /// Remove a PROCLOCK, returning its prior `ProcLock` value (the C
    /// `hash_search(HASH_REMOVE)`); also unchains it from its LOCK's holder list
    /// and wait queue.
    pub(crate) fn proclock_remove(&mut self, tag: &LOCKTAG, holder: ProcNumber) -> Option<ProcLock> {
        let b = bucket_of(tag, self.n_buckets());
        let mut prev = NIL;
        let mut cur = unsafe { *self.proclock_buckets().add(b) };
        while cur != NIL {
            let (used, t, h, next) = unsafe {
                let s = self.proclock_at(cur);
                ((*s).used, (*s).tag, (*s).holder, (*s).next)
            };
            if used != 0 && t == *tag && h == holder {
                let prior = self.proclock_get(tag, holder);
                let lidx = self.find_lock(tag);
                if lidx != NIL {
                    self.holder_remove(lidx, cur);
                    self.waitq_remove_idx(lidx, cur);
                }
                // Unchain from the holder's per-partition myProcLocks list
                // (C's `dlist_delete(&proclock->procLink)`).
                self.my_proc_remove(holder, Self::tag_partition(tag), cur);
                unsafe {
                    if prev == NIL {
                        *self.proclock_buckets().add(b) = next;
                    } else {
                        (*self.proclock_at(prev)).next = next;
                    }
                }
                self.free_proclock(cur);
                return prior;
            }
            prev = cur;
            cur = next;
        }
        None
    }

    /// Mutate a PROCLOCK's masks/leader in place. No-op if absent.
    pub(crate) fn proclock_update(
        &mut self,
        tag: &LOCKTAG,
        holder: ProcNumber,
        f: impl FnOnce(&mut ProcLock),
    ) {
        let idx = self.find_proclock(tag, holder);
        if idx == NIL {
            return;
        }
        let s = self.proclock_at(idx);
        unsafe {
            let mut pl = ProcLock {
                group_leader: (*s).group_leader,
                hold_mask: (*s).hold_mask,
                release_mask: (*s).release_mask,
            };
            f(&mut pl);
            (*s).group_leader = pl.group_leader;
            (*s).hold_mask = pl.hold_mask;
            (*s).release_mask = pl.release_mask;
        }
    }

    /// Rekey a PROCLOCK from `(tag, old)` to `(tag, new)` (the recovery
    /// owner-transfer path), applying `f` to its `ProcLock`, rewriting the
    /// holder field. The bucket is keyed on tag only, so no rechaining is
    /// needed. Returns whether the entry existed.
    pub(crate) fn proclock_rekey_holder(
        &mut self,
        tag: &LOCKTAG,
        old: ProcNumber,
        new: ProcNumber,
        f: impl FnOnce(&mut ProcLock),
    ) -> bool {
        let idx = self.find_proclock(tag, old);
        if idx == NIL {
            return false;
        }
        // Move the proclock between the two procs' myProcLocks lists (same
        // partition, since the tag is unchanged). Remove under the OLD holder
        // before rewriting the holder field, then push under the NEW holder.
        let partition = Self::tag_partition(tag);
        self.my_proc_remove(old, partition, idx);
        let s = self.proclock_at(idx);
        unsafe {
            (*s).holder = new;
        }
        self.my_proc_push(new, partition, idx);
        let s = self.proclock_at(idx);
        unsafe {
            let mut pl = ProcLock {
                group_leader: (*s).group_leader,
                hold_mask: (*s).hold_mask,
                release_mask: (*s).release_mask,
            };
            f(&mut pl);
            (*s).group_leader = pl.group_leader;
            (*s).hold_mask = pl.hold_mask;
            (*s).release_mask = pl.release_mask;
        }
        true
    }

    /// All `(tag, holder)` PROCLOCK keys matching `pred` (the C
    /// `hash_seq_search`).
    pub(crate) fn proclock_keys_filtered(
        &self,
        pred: impl Fn(&LOCKTAG, ProcNumber) -> bool,
    ) -> Vec<(LOCKTAG, ProcNumber)> {
        let mut out = Vec::new();
        for i in 0..self.n_proclocks() {
            let s = self.proclock_at(i as i32);
            unsafe {
                if (*s).used != 0 && pred(&(*s).tag, (*s).holder) {
                    out.push(((*s).tag, (*s).holder));
                }
            }
        }
        out
    }

    /// Full PROCLOCK scan returning `(tag, holder, ProcLock)` (the C
    /// `hash_seq_search` over `LockMethodProcLockHash`).
    pub(crate) fn proclock_scan(&self) -> Vec<(LOCKTAG, ProcNumber, ProcLock)> {
        let mut out = Vec::new();
        for i in 0..self.n_proclocks() {
            let s = self.proclock_at(i as i32);
            unsafe {
                if (*s).used != 0 {
                    out.push((
                        (*s).tag,
                        (*s).holder,
                        ProcLock {
                            group_leader: (*s).group_leader,
                            hold_mask: (*s).hold_mask,
                            release_mask: (*s).release_mask,
                        },
                    ));
                }
            }
        }
        out
    }

    // ---- wait queue (per-LOCK list of waiting procs) -------------------

    fn waitq_node_for(&self, lidx: i32, proc_no: ProcNumber) -> i32 {
        unsafe {
            let tag = (*self.lock_at(lidx)).tag;
            self.find_proclock(&tag, proc_no)
        }
    }

    fn waitq_remove_idx(&mut self, lidx: i32, pidx: i32) {
        unsafe {
            if (*self.proclock_at(pidx)).on_wait_queue == 0 {
                return;
            }
            let mut prev = NIL;
            let mut cur = (*self.lock_at(lidx)).wait_head;
            while cur != NIL {
                if cur == pidx {
                    let next = (*self.proclock_at(cur)).wait_next;
                    if prev == NIL {
                        (*self.lock_at(lidx)).wait_head = next;
                    } else {
                        (*self.proclock_at(prev)).wait_next = next;
                    }
                    (*self.proclock_at(pidx)).wait_next = NIL;
                    (*self.proclock_at(pidx)).on_wait_queue = 0;
                    return;
                }
                prev = cur;
                cur = (*self.proclock_at(cur)).wait_next;
            }
        }
    }

    pub(crate) fn waitq_is_empty(&self, tag: &LOCKTAG) -> bool {
        let lidx = self.find_lock(tag);
        if lidx == NIL {
            return true;
        }
        unsafe { (*self.lock_at(lidx)).wait_head == NIL }
    }

    pub(crate) fn waitq_remove(&mut self, tag: &LOCKTAG, proc_no: ProcNumber) {
        let lidx = self.find_lock(tag);
        if lidx == NIL {
            return;
        }
        let pidx = self.waitq_node_for(lidx, proc_no);
        if pidx != NIL {
            self.waitq_remove_idx(lidx, pidx);
        }
    }

    pub(crate) fn waitq_push_tail(&mut self, tag: &LOCKTAG, proc_no: ProcNumber) {
        let lidx = self.find_lock(tag);
        if lidx == NIL {
            return;
        }
        let pidx = self.waitq_node_for(lidx, proc_no);
        if pidx == NIL {
            return;
        }
        self.waitq_remove_idx(lidx, pidx);
        self.waitq_append(lidx, pidx);
    }

    fn waitq_append(&mut self, lidx: i32, pidx: i32) {
        unsafe {
            (*self.proclock_at(pidx)).wait_next = NIL;
            (*self.proclock_at(pidx)).on_wait_queue = 1;
            let head = (*self.lock_at(lidx)).wait_head;
            if head == NIL {
                (*self.lock_at(lidx)).wait_head = pidx;
                return;
            }
            let mut cur = head;
            while (*self.proclock_at(cur)).wait_next != NIL {
                cur = (*self.proclock_at(cur)).wait_next;
            }
            (*self.proclock_at(cur)).wait_next = pidx;
        }
    }

    pub(crate) fn waitq_insert_before(
        &mut self,
        tag: &LOCKTAG,
        insert_before: ProcNumber,
        proc_no: ProcNumber,
    ) {
        let lidx = self.find_lock(tag);
        if lidx == NIL {
            return;
        }
        let pidx = self.waitq_node_for(lidx, proc_no);
        if pidx == NIL {
            return;
        }
        self.waitq_remove_idx(lidx, pidx);
        let before_idx = self.waitq_node_for(lidx, insert_before);
        let before_queued =
            before_idx != NIL && unsafe { (*self.proclock_at(before_idx)).on_wait_queue != 0 };
        if !before_queued {
            // insert_before not queued: push tail.
            self.waitq_append(lidx, pidx);
            return;
        }
        unsafe {
            (*self.proclock_at(pidx)).on_wait_queue = 1;
            let mut prev = NIL;
            let mut cur = (*self.lock_at(lidx)).wait_head;
            while cur != NIL && cur != before_idx {
                prev = cur;
                cur = (*self.proclock_at(cur)).wait_next;
            }
            (*self.proclock_at(pidx)).wait_next = before_idx;
            if prev == NIL {
                (*self.lock_at(lidx)).wait_head = pidx;
            } else {
                (*self.proclock_at(prev)).wait_next = pidx;
            }
        }
    }

    /// Re-thread the wait queue to the exact order in `order` (the deadlock
    /// detector's soft-deadlock resolution rewrites `lock->waitProcs` in place).
    /// `order` must be a permutation of the procs currently queued; any proc not
    /// in `order` is left dequeued. Mirrors C's `dclist_init` +
    /// `dclist_push_tail` over the intrusive queue.
    pub(crate) fn waitq_set_order(&mut self, tag: &LOCKTAG, order: &[ProcNumber]) {
        let lidx = self.find_lock(tag);
        if lidx == NIL {
            return;
        }
        // Detach the existing chain (clear every node's membership), then re-append
        // in the requested order. Use the existing waitq_remove/append helpers so
        // the per-node `on_wait_queue` / `wait_next` bookkeeping stays correct.
        let current = self.waiters(tag);
        for p in current {
            let pidx = self.waitq_node_for(lidx, p);
            if pidx != NIL {
                self.waitq_remove_idx(lidx, pidx);
            }
        }
        for &p in order {
            let pidx = self.waitq_node_for(lidx, p);
            if pidx != NIL {
                self.waitq_append(lidx, pidx);
            }
        }
    }

    /// Snapshot of the wait queue's `ProcNumber`s in order.
    pub(crate) fn waiters(&self, tag: &LOCKTAG) -> Vec<ProcNumber> {
        let mut out = Vec::new();
        let lidx = self.find_lock(tag);
        if lidx == NIL {
            return out;
        }
        let mut cur = unsafe { (*self.lock_at(lidx)).wait_head };
        while cur != NIL {
            unsafe {
                out.push((*self.proclock_at(cur)).holder);
                cur = (*self.proclock_at(cur)).wait_next;
            }
        }
        out
    }
}

/// Read-only view of a LOCK body.
pub(crate) struct LockBody {
    s: *mut ShmLock,
}
impl LockBody {
    pub(crate) fn tag(&self) -> LOCKTAG {
        unsafe { (*self.s).tag }
    }
    pub(crate) fn grant_mask(&self) -> LOCKMASK {
        unsafe { (*self.s).grant_mask }
    }
    pub(crate) fn wait_mask(&self) -> LOCKMASK {
        unsafe { (*self.s).wait_mask }
    }
    pub(crate) fn requested(&self) -> [i32; MAX_LOCKMODES] {
        unsafe { (*self.s).requested }
    }
    pub(crate) fn n_requested(&self) -> i32 {
        unsafe { (*self.s).n_requested }
    }
    pub(crate) fn granted(&self) -> [i32; MAX_LOCKMODES] {
        unsafe { (*self.s).granted }
    }
    pub(crate) fn n_granted(&self) -> i32 {
        unsafe { (*self.s).n_granted }
    }
}

/// Mutable view of a LOCK body.
pub(crate) struct LockBodyMut {
    s: *mut ShmLock,
}
impl LockBodyMut {
    pub(crate) fn tag(&self) -> LOCKTAG {
        unsafe { (*self.s).tag }
    }
    pub(crate) fn grant_mask(&self) -> LOCKMASK {
        unsafe { (*self.s).grant_mask }
    }
    pub(crate) fn set_grant_mask(&mut self, v: LOCKMASK) {
        unsafe { (*self.s).grant_mask = v }
    }
    pub(crate) fn wait_mask(&self) -> LOCKMASK {
        unsafe { (*self.s).wait_mask }
    }
    pub(crate) fn set_wait_mask(&mut self, v: LOCKMASK) {
        unsafe { (*self.s).wait_mask = v }
    }
    pub(crate) fn requested_at(&self, m: usize) -> i32 {
        unsafe { (*self.s).requested[m] }
    }
    pub(crate) fn set_requested_at(&mut self, m: usize, v: i32) {
        unsafe { (*self.s).requested[m] = v }
    }
    pub(crate) fn n_requested(&self) -> i32 {
        unsafe { (*self.s).n_requested }
    }
    pub(crate) fn set_n_requested(&mut self, v: i32) {
        unsafe { (*self.s).n_requested = v }
    }
    pub(crate) fn granted_at(&self, m: usize) -> i32 {
        unsafe { (*self.s).granted[m] }
    }
    pub(crate) fn set_granted_at(&mut self, m: usize, v: i32) {
        unsafe { (*self.s).granted[m] = v }
    }
    pub(crate) fn n_granted(&self) -> i32 {
        unsafe { (*self.s).n_granted }
    }
    pub(crate) fn set_n_granted(&mut self, v: i32) {
        unsafe { (*self.s).n_granted = v }
    }
}

// SAFETY: SharedLockTable's backing store is genuine MAP_SHARED memory whose
// pointer is inherited at the same VA across fork. Every access is serialized
// by the caller-held partition LWLock (the C contract); the handle owns no Rust
// state.
unsafe impl Send for SharedLockTable {}

thread_local! {
    /// `LockMethodLocalHash` (lock.c backend-private LOCALLOCK hash table),
    /// built by `InitLockManagerAccess`. Keyed by `LOCALLOCKTAG`.
    pub(crate) static LOCAL: RefCell<HashMap<LOCALLOCKTAG, alloc::boxed::Box<LOCALLOCK>>> =
        RefCell::new(HashMap::new());

    /// `FastPathStrongRelationLocks` (lock.c shmem struct), built by
    /// `LockManagerShmemInit`.
    pub(crate) static FP_STRONG: RefCell<FastPathStrongRelationLockData> =
        RefCell::new(FastPathStrongRelationLockData::default());

    /// `FastPathLocalUseCounts[FP_LOCK_GROUPS_PER_BACKEND_MAX]` (lock.c
    /// backend-private). Number of fast-path lock slots in use per group.
    pub(crate) static FP_LOCAL_USE_COUNTS: RefCell<Vec<i32>> = RefCell::new(Vec::new());

    /// `StrongLockInProgress` (lock.c backend-private) — the LOCALLOCKTAG of
    /// the strong lock currently being acquired, plus its fast-path partition.
    pub(crate) static STRONG_LOCK_IN_PROGRESS: RefCell<Option<(LOCALLOCKTAG, usize)>> =
        RefCell::new(None);

    /// `awaitedLock` (lock.c backend-private) — the LOCALLOCKTAG of the lock
    /// this backend is currently waiting on (`NULL` when not waiting).
    pub(crate) static AWAITED_LOCK: RefCell<Option<LOCALLOCKTAG>> = RefCell::new(None);

    /// `awaitedOwner` (lock.c backend-private) — the resource owner recorded
    /// for the awaited lock.
    pub(crate) static AWAITED_OWNER: RefCell<Option<types_storage::lock::ResourceOwnerHandle>> =
        RefCell::new(None);

    /// `IsRelationExtensionLockHeld` (lock.c, asserts-only) — whether the
    /// relation-extension lock is currently held by this backend.
    pub(crate) static IS_RELATION_EXTENSION_LOCK_HELD: core::cell::Cell<bool> =
        const { core::cell::Cell::new(false) };
}

/// Run `f` with access to the shared lock table handle. The handle reaches the
/// genuine cross-process arena via [`SHARED_BASE`]; mutual exclusion is the
/// caller-held partition LWLock (the C contract).
pub(crate) fn with_shared<R>(f: impl FnOnce(&mut SharedLockTable) -> R) -> R {
    let base = SHARED_BASE.load(Ordering::Relaxed);
    assert!(
        !base.is_null(),
        "shared lock table accessed before LockManagerShmemInit"
    );
    let mut handle = SharedLockTable { base };
    f(&mut handle)
}

/// Run `f` with mutable access to the backend-local LOCALLOCK table.
pub(crate) fn with_local<R>(
    f: impl FnOnce(&mut HashMap<LOCALLOCKTAG, alloc::boxed::Box<LOCALLOCK>>) -> R,
) -> R {
    LOCAL.with(|c| f(&mut c.borrow_mut()))
}
