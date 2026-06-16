//! The lock manager's ambient process-global state (`storage/lmgr/lock.c`
//! file-scope statics).
//!
//! In C these are `static HTAB *LockMethodLockHash` /
//! `LockMethodProcLockHash` (shared memory) / `LockMethodLocalHash`
//! (backend-private), the `FastPathStrongRelationLocks` shmem struct, and the
//! `FastPathLocalUseCounts` / `StrongLockInProgress` / `awaitedLock`
//! backend-private scalars.
//!
//! Following the repo's single-process shmem model (see proc.c's
//! `PROC_GLOBAL`), every one of these — shared and backend-private alike — is a
//! per-backend `thread_local`. The two shared hash tables are modeled as Rust
//! `HashMap`s keyed by `LOCKTAG` (the LOCK table) and `(LOCKTAG, ProcNumber)`
//! (the PROCLOCK table); the per-lock holder/waiter lists that C threads through
//! intrusive `dlist`/`dclist` links are modeled as the maps themselves plus the
//! `LOCK`'s `wait queue` (a `Vec<ProcNumber>`), so the fine-grained seam bodies
//! proc.c calls can reach them keyed on `(LOCKTAG, ProcNumber)`.

// F0 lands the ambient-table foundation; F1/F2 fill the grant/release/wait
// spine on top of it. A few fast-path / 2PC fields are still consumed only by
// the deferred F3-F5 families; allow dead_code until they land
// (mirror-PG-and-panic frontier).
#![allow(dead_code)]

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::cell::RefCell;
use std::collections::HashMap;

use types_core::ProcNumber;
use types_storage::lock::{LOCALLOCK, LOCALLOCKTAG, LOCK, LOCKMASK, LOCKTAG};

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

/// One backend's relationship to one `LOCK` — the ambient-model rendering of
/// the C `PROCLOCK` (whose `tag.myLock` / `tag.myProc` / `dlist` links are
/// modeled implicitly by this entry's `(LOCKTAG, ProcNumber)` key + the
/// per-`LOCK` holder order). Only the genuine per-PROCLOCK state survives:
/// `holdMask`, `releaseMask`, and the group leader's `ProcNumber`.
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

/// The shared per-lockable-object entry (the C `LOCK` plus its `procLocks` /
/// `waitProcs` lists). The `LOCK` body carries the grant/wait masks and the
/// per-mode counts; the holder/waiter lists that C threads through intrusive
/// `dlist`/`dclist` links are modeled as the maps/vectors here.
#[derive(Debug, Default)]
pub struct LockEntry {
    /// The `LOCK` struct (tag, grant/wait masks, per-mode counts).
    pub lock: LOCK,
    /// `lock->waitProcs` — front-to-back wait queue of `PGPROC`s by ProcNumber.
    pub wait_queue: Vec<ProcNumber>,
    /// `lock->procLocks` — the holders' `ProcNumber`s in dlist (push-tail)
    /// order, paired with their per-PROCLOCK state.
    pub holders: Vec<ProcNumber>,
}

/// The lock manager's shared lock-table substrate (the C
/// `LockMethodLockHash` + `LockMethodProcLockHash` shmem hash tables).
#[derive(Debug, Default)]
pub struct SharedLockTable {
    /// `LockMethodLockHash` — LOCK entries keyed by their `LOCKTAG`.
    pub locks: HashMap<LOCKTAG, Box<LockEntry>>,
    /// `LockMethodProcLockHash` — PROCLOCK entries keyed by `(LOCKTAG, holder
    /// ProcNumber)` (the `PROCLOCKTAG` (`myLock`, `myProc`) pair).
    pub proclocks: HashMap<(LOCKTAG, ProcNumber), ProcLock>,
}

thread_local! {
    /// `LockMethodLockHash` + `LockMethodProcLockHash` (lock.c shmem hash
    /// tables), built by `LockManagerShmemInit`.
    pub(crate) static SHARED: RefCell<SharedLockTable> = RefCell::new(SharedLockTable::default());

    /// `LockMethodLocalHash` (lock.c backend-private LOCALLOCK hash table),
    /// built by `InitLockManagerAccess`. Keyed by `LOCALLOCKTAG`.
    pub(crate) static LOCAL: RefCell<HashMap<LOCALLOCKTAG, Box<LOCALLOCK>>> =
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

/// Run `f` with mutable access to the shared lock table.
pub(crate) fn with_shared<R>(f: impl FnOnce(&mut SharedLockTable) -> R) -> R {
    SHARED.with(|c| f(&mut c.borrow_mut()))
}

/// Run `f` with mutable access to the backend-local LOCALLOCK table.
pub(crate) fn with_local<R>(
    f: impl FnOnce(&mut HashMap<LOCALLOCKTAG, Box<LOCALLOCK>>) -> R,
) -> R {
    LOCAL.with(|c| f(&mut c.borrow_mut()))
}
