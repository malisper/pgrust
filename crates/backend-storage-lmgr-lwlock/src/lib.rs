#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

//! `backend-storage-lmgr-lwlock` — the lightweight-lock manager
//! (`src/backend/storage/lmgr/lwlock.c`).
//!
//! Lightweight locks provide exclusive/shared mutual exclusion over
//! shared-memory data structures, plus the `LWLockWaitForVar` /
//! `LWLockUpdateVar` variable-wait protocol used by the WAL insertion locks.
//!
//! The `LWLock` itself is a shared-memory–resident struct: its `state` word is
//! manipulated exclusively through atomic compare-and-exchange (the wait-free
//! shared-acquisition design described in lwlock.c's header comment), and the
//! `waiters` proclist head/tail are mutated only while the wait-list spinlock
//! bit (`LW_FLAG_LOCKED`) is held. We reinterpret the `pg_atomic_uint32` state
//! field as a real [`AtomicU32`] and run the exact CAS protocol — never
//! substituting a `std::sync` lock for the shared state.
//!
//! Everything genuinely outside lwlock.c — shmem sizing (`add_size` /
//! `mul_size`), the `ShmemLock` spinlock, the PGPROC array fields and process
//! wait semaphores, interrupt holdoff, spin-delay backoff (s_lock.c), and
//! wait-event reporting — is reached through the owning units' crates or
//! seam crates. Foreign per-backend globals C reads ambiently
//! (`MyProcNumber`, `IsUnderPostmaster`,
//! `process_shmem_requests_in_progress`) are explicit parameters of the
//! entry points that need them (AGENTS.md "No ambient-global seams").
//!
//! Compiled-out C surface (`LWLOCK_STATS`, `LOCK_DEBUG`, dtrace probes) is not
//! ported, matching the default build.

use core::cell::RefCell;
use core::ops::ControlFlow;
use core::sync::atomic::{fence, AtomicI32, AtomicU32, AtomicU64, Ordering};

use backend_storage_ipc_shmem_seams as shmem;
use backend_storage_lmgr_proc_seams as proc_s;
use backend_storage_lmgr_s_lock as s_lock;
use backend_utils_activity_waitevent_seams as waitevent;
use backend_utils_error::{elog, PgError, PgResult};
use mcx::Mcx;
use types_error::{ERROR, FATAL, PANIC};
use backend_utils_init_small_seams as globals;
use types_core::{uint16, uint32, ProcNumber, Size, INVALID_PROC_NUMBER, NAMEDATALEN};
use types_pgstat::wait_event::PG_WAIT_LWLOCK;
use types_storage::{
    pg_atomic_uint32, pg_atomic_uint64, proclist_head, proclist_node, LWLock, LWLockMode,
    LWLockPadded, BUFFER_MAPPING_LWLOCK_OFFSET, LOCK_MANAGER_LWLOCK_OFFSET, LW_EXCLUSIVE,
    LW_SHARED, LW_WAIT_UNTIL_FREE, LW_WS_NOT_WAITING, LW_WS_PENDING_WAKEUP, LW_WS_WAITING,
    LWLOCK_PADDED_SIZE, LWTRANCHE_BUFFER_MAPPING, LWTRANCHE_FIRST_USER_DEFINED,
    LWTRANCHE_LOCK_MANAGER, LWTRANCHE_PREDICATE_LOCK_MANAGER, MAX_BACKENDS, NUM_BUFFER_PARTITIONS,
    NUM_FIXED_LWLOCKS, NUM_INDIVIDUAL_LWLOCKS, NUM_LOCK_PARTITIONS, NUM_PREDICATELOCK_PARTITIONS,
    PREDICATELOCK_MANAGER_LWLOCK_OFFSET,
};

// ---------------------------------------------------------------------------
// State-word flag/value constants (lwlock.c lines 104-117).
// ---------------------------------------------------------------------------

pub const LW_FLAG_HAS_WAITERS: uint32 = 1_u32 << 31;
pub const LW_FLAG_RELEASE_OK: uint32 = 1_u32 << 30;
pub const LW_FLAG_LOCKED: uint32 = 1_u32 << 29;
pub const LW_FLAG_BITS: uint32 = 3;
pub const LW_FLAG_MASK: uint32 = ((1 << LW_FLAG_BITS) - 1) << (32 - LW_FLAG_BITS);

/// `LW_VAL_EXCLUSIVE` — assumes `MAX_BACKENDS` is a power of two minus one
/// (asserted below).
pub const LW_VAL_EXCLUSIVE: uint32 = MAX_BACKENDS + 1;
pub const LW_VAL_SHARED: uint32 = 1;
pub const LW_SHARED_MASK: uint32 = MAX_BACKENDS;
pub const LW_LOCK_MASK: uint32 = MAX_BACKENDS | LW_VAL_EXCLUSIVE;

// StaticAssertDecls from lwlock.c lines 119-126.
const _: () = assert!(((MAX_BACKENDS + 1) & MAX_BACKENDS) == 0);
const _: () = assert!((MAX_BACKENDS & LW_FLAG_MASK) == 0);
const _: () = assert!((LW_VAL_EXCLUSIVE & LW_FLAG_MASK) == 0);

/// `MAX_SIMUL_LWLOCKS` — the cap on simultaneously-held LWLocks per backend.
pub const MAX_SIMUL_LWLOCKS: usize = 200;

// ---------------------------------------------------------------------------
// BuiltinTrancheNames[] (lwlock.c lines 146-191): indexed by tranche ID. The
// first NUM_INDIVIDUAL_LWLOCKS slots carry the individually-named LWLock
// names absorbed from lwlocklist.h ("" for the gap ids C leaves NULL); the
// rest are the BuiltinTrancheIds group names. The array-type length is the
// C StaticAssert lengthof(BuiltinTrancheNames) == LWTRANCHE_FIRST_USER_DEFINED.
// ---------------------------------------------------------------------------
const BUILTIN_TRANCHE_NAMES: [&str; LWTRANCHE_FIRST_USER_DEFINED as usize] = [
    "", "ShmemIndex", "OidGen", "XidGen", "ProcArray", "SInvalRead", "SInvalWrite", "WALBufMapping",
    "WALWrite", "ControlFile", "", "", "", "MultiXactGen", "", "", "RelCacheInit",
    "CheckpointerComm", "TwoPhaseState", "TablespaceCreate", "BtreeVacuum", "AddinShmemInit",
    "Autovacuum", "AutovacuumSchedule", "SyncScan", "RelationMapping", "", "NotifyQueue",
    "SerializableXactHash", "SerializableFinishedList", "SerializablePredicateList", "", "SyncRep",
    "BackgroundWorker", "DynamicSharedMemoryControl", "AutoFile", "ReplicationSlotAllocation",
    "ReplicationSlotControl", "", "CommitTs", "ReplicationOrigin", "MultiXactTruncation", "",
    "LogicalRepWorker", "XactTruncation", "", "WrapLimitsVacuum", "NotifyQueueTail",
    "WaitEventCustom", "WALSummarizer", "DSMRegistry", "InjectionPoint", "SerialControl",
    "AioWorkerSubmissionQueue", "XactBuffer", "CommitTsBuffer", "SubtransBuffer",
    "MultiXactOffsetBuffer", "MultiXactMemberBuffer", "NotifyBuffer", "SerialBuffer", "WALInsert",
    "BufferContent", "ReplicationOriginState", "ReplicationSlotIO", "LockFastPath", "BufferMapping",
    "LockManager", "PredicateLockManager", "ParallelHashJoin", "ParallelBtreeScan",
    "ParallelQueryDSA", "PerSessionDSA", "PerSessionRecordType", "PerSessionRecordTypmod",
    "SharedTupleStore", "SharedTidBitmap", "ParallelAppend", "PerXactPredicateList", "PgStatsDSA",
    "PgStatsHash", "PgStatsData", "LogicalRepLauncherDSA", "LogicalRepLauncherHash",
    "DSMRegistryDSA", "DSMRegistryHash", "CommitTsSLRU", "MultiXactMemberSLRU",
    "MultiXactOffsetSLRU", "NotifySLRU", "SerialSLRU", "SubtransSLRU", "XactSLRU",
    "ParallelVacuumDSA", "AioUringCompletion",
];

// ---------------------------------------------------------------------------
// File-scope structs (lwlock.c lines 230-247) + the owned stand-ins for the
// shmem-resident named-tranche metadata.
// ---------------------------------------------------------------------------

/// `NamedLWLockTrancheRequest` (lwlock.c) — a request, recorded during
/// `shmem_request_hook` processing, that the postmaster allocate `num_lwlocks`
/// extra LWLocks under a named tranche. In C the name is a fixed
/// `char tranche_name[NAMEDATALEN]`; here it is the (length-clamped) `String`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamedLWLockTrancheRequest {
    pub tranche_name: String,
    pub num_lwlocks: i32,
}

/// `NamedLWLockTranche` (`storage/lwlock.h`) — the shmem copy of a named
/// tranche's id and name. Kept for the `LWLockShmemSize` sizing math and the
/// `CreateLWLocks` registration loop.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamedLWLockTranche {
    pub trancheId: i32,
    pub trancheName: String,
}

/// Per-process named-tranche placement metadata: the `NamedLWLockTranche`
/// plus the `[start, start+len)` slot range it occupies inside the main
/// LWLock array (placed after the fixed locks), mirroring the
/// `GetNamedLWLockTranche` walk over `NamedLWLockTrancheRequestArray`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamedLWLockTrancheRange {
    pub tranche_name: String,
    pub tranche_id: i32,
    pub start: usize,
    pub len: usize,
}

/// `LWLockHandle` (lwlock.c) — one entry of the backend-local
/// `held_lwlocks[]` array, carrying the same typed `LWLock *` C stores. The
/// pointee is alive for as long as the entry exists: a backend may only
/// record a lock it acquired, and shmem-resident locks outlive the backend.
#[derive(Clone, Copy)]
struct LWLockHandle {
    lock: *const LWLock,
    mode: LWLockMode,
}

/// `held_lwlocks[MAX_SIMUL_LWLOCKS]` + `num_held_lwlocks` — a fixed array,
/// exactly as in C: recording a held lock never allocates and never fails
/// once the `MAX_SIMUL_LWLOCKS` check has passed.
struct HeldLWLocks {
    locks: [LWLockHandle; MAX_SIMUL_LWLOCKS],
    num_held: usize,
}

impl HeldLWLocks {
    const fn new() -> Self {
        Self {
            locks: [LWLockHandle {
                lock: core::ptr::null(),
                mode: LW_EXCLUSIVE,
            }; MAX_SIMUL_LWLOCKS],
            num_held: 0,
        }
    }

    fn has_room(&self) -> bool {
        self.num_held < MAX_SIMUL_LWLOCKS
    }

    fn push(&mut self, lock: *const LWLock, mode: LWLockMode) {
        debug_assert!(self.num_held < MAX_SIMUL_LWLOCKS);
        self.locks[self.num_held] = LWLockHandle { lock, mode };
        self.num_held += 1;
    }

    /// `LWLockDisownInternal`'s search-backwards + shift-down removal.
    fn disown(&mut self, lock: *const LWLock) -> Option<LWLockMode> {
        let i = self.locks[..self.num_held]
            .iter()
            .rposition(|held| core::ptr::eq(held.lock, lock))?;
        let mode = self.locks[i].mode;
        self.num_held -= 1;
        for j in i..self.num_held {
            self.locks[j] = self.locks[j + 1];
        }
        Some(mode)
    }

    fn last(&self) -> Option<*const LWLock> {
        self.num_held
            .checked_sub(1)
            .map(|i| self.locks[i].lock)
    }

    fn contains(&self, lock: *const LWLock) -> bool {
        self.locks[..self.num_held]
            .iter()
            .any(|held| core::ptr::eq(held.lock, lock))
    }

    fn contains_in_mode(&self, lock: *const LWLock, mode: LWLockMode) -> bool {
        self.locks[..self.num_held]
            .iter()
            .any(|held| core::ptr::eq(held.lock, lock) && held.mode == mode)
    }
}

thread_local! {
    /// Backend-local held-lock table (`held_lwlocks[]` / `num_held_lwlocks`).
    static HELD_LWLOCKS: RefCell<HeldLWLocks> = const { RefCell::new(HeldLWLocks::new()) };

    /// `LWLockTrancheNames[]` / `LWLockTrancheNamesAllocated` — dynamic
    /// tranche names known to this process, indexed by
    /// `tranche_id - LWTRANCHE_FIRST_USER_DEFINED`.
    static LWLOCK_TRANCHE_NAMES: RefCell<Vec<Option<String>>> = const { RefCell::new(Vec::new()) };

    /// `NamedLWLockTrancheRequestArray[]` / `NamedLWLockTrancheRequests`.
    static NAMED_LWLOCK_TRANCHE_REQUESTS: RefCell<Vec<NamedLWLockTrancheRequest>> =
        const { RefCell::new(Vec::new()) };
}

/// `*LWLockCounter` — the dynamic tranche-ID counter that in C lives in shared
/// memory just before `MainLWLockArray` and is protected by the `ShmemLock`
/// spinlock. In the threaded-server model shared memory is process memory, so
/// the counter is a process-wide atomic, still accessed only under the
/// `ShmemLock` seam (loads/stores are Relaxed because the spinlock provides
/// the exclusion, exactly as in C).
static LWLOCK_COUNTER: AtomicI32 = AtomicI32::new(LWTRANCHE_FIRST_USER_DEFINED);

// The shared lock state is a real atomic (`pg_atomic_uint32` wraps
// `AtomicU32`); these are plain field reads.

fn atomic_state(lock: &LWLock) -> &AtomicU32 {
    &lock.state.value
}

fn atomic_var(valptr: &pg_atomic_uint64) -> &AtomicU64 {
    &valptr.value
}

/// Exclusive view of the wait list (through its `UnsafeCell`).
///
/// # Safety
///
/// The caller must hold the lock's wait-list spinlock bit (`LW_FLAG_LOCKED`,
/// taken by [`LWLockWaitListLock`] and dropped by [`LWLockWaitListUnlock`]),
/// which is what serializes wait-list access between backends, exactly as in
/// C; the returned borrow must not outlive that critical section.
unsafe fn waiters_mut(lock: &LWLock) -> &mut proclist_head {
    unsafe { &mut *lock.waiters.ptr() }
}

// ---------------------------------------------------------------------------
// proclist helpers (storage/proclist.h) specialized to `lwWaitLink`, 1:1 with
// the `proclist_*_offset` inline helpers. Each PGPROC's `lwWaitLink` node is
// read/written through the proc seams, as the C
// `proclist_node_get(procno, offsetof(PGPROC, lwWaitLink))` macro does.
// ---------------------------------------------------------------------------

fn proclist_init(list: &mut proclist_head) {
    list.head = INVALID_PROC_NUMBER;
    list.tail = INVALID_PROC_NUMBER;
}

fn proclist_is_empty(list: &proclist_head) -> bool {
    list.head == INVALID_PROC_NUMBER
}

fn proclist_push_head(list: &mut proclist_head, procno: ProcNumber) {
    let mut node = proc_s::proc_lw_wait_link::call(procno);
    debug_assert!(node.next == 0 && node.prev == 0);

    if list.head == INVALID_PROC_NUMBER {
        debug_assert!(list.tail == INVALID_PROC_NUMBER);
        node.next = INVALID_PROC_NUMBER;
        list.tail = procno;
    } else {
        node.next = list.head;
        debug_assert!(node.next != INVALID_PROC_NUMBER);
        let mut head_node = proc_s::proc_lw_wait_link::call(node.next);
        head_node.prev = procno;
        proc_s::set_proc_lw_wait_link::call(node.next, head_node);
    }

    node.prev = INVALID_PROC_NUMBER;
    list.head = procno;
    proc_s::set_proc_lw_wait_link::call(procno, node);
}

fn proclist_push_tail(list: &mut proclist_head, procno: ProcNumber) {
    let mut node = proc_s::proc_lw_wait_link::call(procno);
    debug_assert!(node.next == 0 && node.prev == 0);

    if list.tail == INVALID_PROC_NUMBER {
        debug_assert!(list.head == INVALID_PROC_NUMBER);
        node.prev = INVALID_PROC_NUMBER;
        list.head = procno;
    } else {
        node.prev = list.tail;
        debug_assert!(node.prev != INVALID_PROC_NUMBER);
        let mut tail_node = proc_s::proc_lw_wait_link::call(node.prev);
        tail_node.next = procno;
        proc_s::set_proc_lw_wait_link::call(node.prev, tail_node);
    }

    node.next = INVALID_PROC_NUMBER;
    list.tail = procno;
    proc_s::set_proc_lw_wait_link::call(procno, node);
}

fn proclist_delete(list: &mut proclist_head, procno: ProcNumber) {
    let node = proc_s::proc_lw_wait_link::call(procno);

    if node.prev == INVALID_PROC_NUMBER {
        list.head = node.next;
    } else {
        let mut prev_node = proc_s::proc_lw_wait_link::call(node.prev);
        prev_node.next = node.next;
        proc_s::set_proc_lw_wait_link::call(node.prev, prev_node);
    }

    if node.next == INVALID_PROC_NUMBER {
        list.tail = node.prev;
    } else {
        let mut next_node = proc_s::proc_lw_wait_link::call(node.next);
        next_node.prev = node.prev;
        proc_s::set_proc_lw_wait_link::call(node.next, next_node);
    }

    // mark as if not in a list, for debugging
    proc_s::set_proc_lw_wait_link::call(procno, proclist_node { next: 0, prev: 0 });
}

/// `proclist_foreach_modify` (storage/proclist.h) — walk the list starting at
/// head pgprocno `head`, invoking `body(cur)` per node. Exactly like the C
/// macro, the current node's `next` link is cached before the body runs, so
/// the body may `proclist_delete` the current node — including via `&mut` to
/// the very list this walk began from (the traversal never re-reads the list
/// head). `ControlFlow::Break` is a C `break`.
fn proclist_foreach_modify(head: ProcNumber, mut body: impl FnMut(ProcNumber) -> ControlFlow<()>) {
    let mut cur = head;
    while cur != INVALID_PROC_NUMBER {
        let next = proc_s::proc_lw_wait_link::call(cur).next;
        if body(cur).is_break() {
            break;
        }
        cur = next;
    }
}

// ---------------------------------------------------------------------------
// Tranche registry + shmem array setup (lwlock.c lines 427-723).
// ---------------------------------------------------------------------------

/// `NumLWLocksForNamedTranches` (lwlock.c:427) — locks required by named
/// tranches; these are allocated in the main array.
fn NumLWLocksForNamedTranches() -> i32 {
    NAMED_LWLOCK_TRANCHE_REQUESTS.with(|reqs| {
        reqs.borrow()
            .iter()
            .map(|request| request.num_lwlocks)
            .sum()
    })
}

/// `LWLockShmemSize` (lwlock.c:442) — shmem space needed for LWLocks and named
/// tranches. `Err` carries `add_size`/`mul_size`'s overflow `ereport(ERROR)`.
pub fn LWLockShmemSize() -> PgResult<Size> {
    let num_locks = (NUM_FIXED_LWLOCKS + NumLWLocksForNamedTranches()) as Size;

    NAMED_LWLOCK_TRANCHE_REQUESTS.with(|reqs| {
        let reqs = reqs.borrow();

        // Space for the LWLock array.
        let mut size = shmem::mul_size::call(num_locks, core::mem::size_of::<LWLockPadded>())?;

        // Space for dynamic allocation counter, plus room for alignment.
        size = shmem::add_size::call(size, core::mem::size_of::<i32>() + LWLOCK_PADDED_SIZE)?;

        // space for named tranches.
        size = shmem::add_size::call(
            size,
            shmem::mul_size::call(reqs.len(), core::mem::size_of::<NamedLWLockTranche>())?,
        )?;

        // space for name of each tranche.
        for request in reqs.iter() {
            size = shmem::add_size::call(size, request.tranche_name.len() + 1)?;
        }
        Ok(size)
    })
}

/// Process view of the main LWLock array: the owned stand-in for lwlock.c's
/// `MainLWLockArray` pointer plus the named-tranche placement metadata
/// (`NamedLWLockTrancheArray`).
pub struct LWLockTable {
    locks: Vec<LWLockPadded>,
    named_tranches: Vec<NamedLWLockTrancheRange>,
}

impl LWLockTable {
    pub fn locks(&self) -> &[LWLockPadded] {
        &self.locks
    }

    pub fn named_tranches(&self) -> &[NamedLWLockTrancheRange] {
        &self.named_tranches
    }

    pub fn lock(&self, index: usize) -> Option<&LWLock> {
        self.locks.get(index).map(|slot| &slot.lock)
    }
}

/// The published `MainLWLockArray` global (lwlock.c) plus the named-tranche
/// placement metadata. The array lives in main shared memory, which in the
/// threaded-server model is process memory shared by every backend thread —
/// legitimately cross-thread state. (`LWLockTable` is `Sync`: the lock state
/// words are atomics and the `waiters` proclists are `UnsafeCell`s guarded by
/// the `LW_FLAG_LOCKED` wait-list spinlock.)
static MAIN_LWLOCKS: std::sync::OnceLock<LWLockTable> = std::sync::OnceLock::new();

/// `&MainLWLockArray[offset].lock` — panics loudly (like an uninstalled seam)
/// if the array has not been created yet.
fn main_lock(offset: usize) -> &'static LWLock {
    MAIN_LWLOCKS
        .get()
        .expect("MainLWLockArray not published (CreateLWLocks has not run)")
        .lock(offset)
        .expect("main LWLock offset out of range")
}

/// RAII hold on one of the built-in main-array locks, returned by
/// [`LWLockAcquireMain`]. `Drop` is the error-path release (what C leaves to
/// error recovery's `LWLockReleaseAll`); the success path calls
/// [`MainLWLockGuard::release`] where C calls `LWLockRelease`.
pub struct MainLWLockGuard {
    lock: Option<&'static LWLock>,
}

impl MainLWLockGuard {
    /// `LWLockRelease(&MainLWLockArray[offset].lock)` — explicit release,
    /// surfacing the C `elog(ERROR, "lock ... is not held")`.
    pub fn release(mut self) -> PgResult<()> {
        let lock = self.lock.take().expect("MainLWLockGuard released twice");
        LWLockRelease(lock)
    }
}

impl Drop for MainLWLockGuard {
    fn drop(&mut self) {
        if let Some(lock) = self.lock.take() {
            // Unwind-path release. If something already swept the lock away
            // (LWLockReleaseAll during shmem exit), the not-held error is
            // exactly the situation; ignore it.
            let _ = LWLockRelease(lock);
        }
    }
}

/// `LWLockAcquire(&MainLWLockArray[offset].lock, mode)` — acquire a built-in
/// individual lock by its `lwlocklist.h` offset, returning a guard so the
/// hold can never leak across a `?` (AGENTS.md "Locks and held resources").
/// `my_proc_number` is the caller's `MyProcNumber` (explicit parameter for
/// the C ambient per-backend global).
pub fn LWLockAcquireMain(
    offset: usize,
    mode: LWLockMode,
    my_proc_number: ProcNumber,
) -> PgResult<MainLWLockGuard> {
    let lock = main_lock(offset);
    LWLockAcquire(lock, mode, my_proc_number)?;
    Ok(MainLWLockGuard { lock: Some(lock) })
}

/// `CreateLWLocks` (lwlock.c:472) — allocate the main LWLock array, initialize
/// it (postmaster only), publish it as the process's `MainLWLockArray`, and
/// register named extension tranches in the current process.
/// `!is_under_postmaster` (the caller's `IsUnderPostmaster`) ALLOCATES +
/// INITIALIZES (and resets the dynamic tranche counter); a forked backend
/// ATTACHES to the postmaster-built table (in C the pointer is inherited
/// across fork). Allocation failures carry `mcx`'s OOM error (C:
/// `ShmemAlloc`); creating twice panics, like a duplicate seam install.
pub fn CreateLWLocks(mcx: Mcx<'_>, is_under_postmaster: bool) -> PgResult<&'static LWLockTable> {
    let table = if !is_under_postmaster {
        let total_locks = (NUM_FIXED_LWLOCKS + NumLWLocksForNamedTranches()) as usize;

        // Validate the shmem reservation math (C: ShmemAlloc(LWLockShmemSize())).
        let _space_locks = LWLockShmemSize()?;

        // Allocate space.
        let mut locks: Vec<LWLockPadded> = Vec::new();
        locks
            .try_reserve_exact(total_locks)
            .map_err(|_| mcx.oom(total_locks * core::mem::size_of::<LWLockPadded>()))?;
        locks.resize_with(total_locks, LWLockPadded::default);

        // Initialize the dynamic-allocation counter for tranches, which in C
        // is stored just before the first LWLock. (Like C, no spinlock: only
        // the postmaster runs at this point.)
        LWLOCK_COUNTER.store(LWTRANCHE_FIRST_USER_DEFINED, Ordering::Relaxed);

        // Initialize all LWLocks.
        let named_tranches = InitializeLWLocks(mcx, &mut locks)?;

        // Publish the table: this is the shmem segment backends attach to.
        if MAIN_LWLOCKS
            .set(LWLockTable {
                locks,
                named_tranches,
            })
            .is_err()
        {
            panic!("MainLWLockArray published twice");
        }
        MAIN_LWLOCKS.get().expect("just published")
    } else {
        // A forked backend attaches to the postmaster-initialized array.
        MAIN_LWLOCKS.get().expect(
            "CreateLWLocks(is_under_postmaster) before the postmaster created MainLWLockArray",
        )
    };

    // Register named extension LWLock tranches in the current process.
    for range in table.named_tranches() {
        LWLockRegisterTranche(mcx, range.tranche_id, &range.tranche_name)?;
    }

    Ok(table)
}

/// `InitializeLWLocks` (lwlock.c:512) — initialize LWLocks that are fixed and
/// those belonging to named tranches; returns the named-tranche placement
/// metadata (the owned stand-in for filling `NamedLWLockTrancheArray`).
fn InitializeLWLocks(
    mcx: Mcx<'_>,
    locks: &mut [LWLockPadded],
) -> PgResult<Vec<NamedLWLockTrancheRange>> {
    // Initialize all individual LWLocks in main array.
    for id in 0..NUM_INDIVIDUAL_LWLOCKS {
        LWLockInitialize(&mut locks[id as usize].lock, id);
    }

    // Initialize buffer mapping LWLocks in main array.
    for id in 0..NUM_BUFFER_PARTITIONS {
        LWLockInitialize(
            &mut locks[(BUFFER_MAPPING_LWLOCK_OFFSET + id) as usize].lock,
            LWTRANCHE_BUFFER_MAPPING,
        );
    }

    // Initialize lmgrs' LWLocks in main array.
    for id in 0..NUM_LOCK_PARTITIONS {
        LWLockInitialize(
            &mut locks[(LOCK_MANAGER_LWLOCK_OFFSET + id) as usize].lock,
            LWTRANCHE_LOCK_MANAGER,
        );
    }

    // Initialize predicate lmgrs' LWLocks in main array.
    for id in 0..NUM_PREDICATELOCK_PARTITIONS {
        LWLockInitialize(
            &mut locks[(PREDICATELOCK_MANAGER_LWLOCK_OFFSET + id) as usize].lock,
            LWTRANCHE_PREDICATE_LOCK_MANAGER,
        );
    }

    // Copy the info about any named tranches into the placement metadata and
    // initialize the requested LWLocks.
    let requests = NAMED_LWLOCK_TRANCHE_REQUESTS.with(|reqs| reqs.borrow().clone());
    let mut named_tranches: Vec<NamedLWLockTrancheRange> = Vec::new();
    named_tranches
        .try_reserve_exact(requests.len())
        .map_err(|_| mcx.oom(requests.len() * core::mem::size_of::<NamedLWLockTrancheRange>()))?;

    let mut next_lock = NUM_FIXED_LWLOCKS as usize;
    for request in &requests {
        let tranche_id = LWLockNewTrancheId();
        for offset in 0..request.num_lwlocks as usize {
            LWLockInitialize(&mut locks[next_lock + offset].lock, tranche_id);
        }
        named_tranches.push(NamedLWLockTrancheRange {
            tranche_name: request.tranche_name.clone(),
            tranche_id,
            start: next_lock,
            len: request.num_lwlocks as usize,
        });
        next_lock += request.num_lwlocks as usize;
    }

    Ok(named_tranches)
}

/// `InitLWLockAccess` (lwlock.c:579) — initialize backend-local state needed
/// to hold LWLocks. Without `LWLOCK_STATS` this is a no-op, exactly as in C.
pub fn InitLWLockAccess() {}

/// `GetNamedLWLockTranche` (lwlock.c:595) — the LWLock slice for the named
/// tranche `tranche_name` (C returns the base address; callers index the
/// requested number of locks from it).
pub fn GetNamedLWLockTranche<'a>(
    table: &'a LWLockTable,
    tranche_name: &str,
) -> PgResult<&'a [LWLockPadded]> {
    let range = table
        .named_tranches
        .iter()
        .find(|range| range.tranche_name == tranche_name);
    match range {
        Some(range) => {
            let (start, len) = (range.start, range.len);
            Ok(&table.locks[start..start + len])
        }
        None => {
            elog(ERROR, "requested tranche is not registered")?;
            unreachable!("elog(ERROR) returns Err");
        }
    }
}

/// `LWLockNewTrancheId` (lwlock.c:625) — allocate a new tranche ID by
/// incrementing the shared counter under the `ShmemLock` spinlock.
pub fn LWLockNewTrancheId() -> i32 {
    // We use the ShmemLock spinlock to protect LWLockCounter.
    shmem::shmem_lock_acquire::call();
    let result = LWLOCK_COUNTER.load(Ordering::Relaxed);
    LWLOCK_COUNTER.store(result + 1, Ordering::Relaxed);
    shmem::shmem_lock_release::call();
    result
}

/// `LWLockRegisterTranche` (lwlock.c:650) — register a dynamic tranche name in
/// the lookup table of the current process. C stores the caller's pointer
/// (which must be backend-lifetime); the owned port stores a copy, removing
/// that lifetime obligation. `Err` is the allocation failure surface of C's
/// `MemoryContextAllocZero`/`repalloc0_array` growth in `TopMemoryContext`;
/// `mcx` is that target context's handle.
pub fn LWLockRegisterTranche(mcx: Mcx<'_>, tranche_id: i32, tranche_name: &str) -> PgResult<()> {
    // This should only be called for user-defined tranches.
    if tranche_id < LWTRANCHE_FIRST_USER_DEFINED {
        return Ok(());
    }

    // Convert to array index.
    let index = (tranche_id - LWTRANCHE_FIRST_USER_DEFINED) as usize;

    LWLOCK_TRANCHE_NAMES.with(|names| {
        let mut names = names.borrow_mut();
        // If necessary, create or enlarge array.
        if index >= names.len() {
            // newalloc = pg_nextpower2_32(Max(8, tranche_id + 1))
            let newalloc = (index + 1).max(8).next_power_of_two();
            let extra = newalloc - names.len();
            names
                .try_reserve(extra)
                .map_err(|_| mcx.oom(extra * core::mem::size_of::<Option<String>>()))?;
            names.resize(newalloc, None);
        }
        names[index] = Some(tranche_name.to_owned());
        Ok(())
    })
}

/// `RequestNamedLWLockTranche` (lwlock.c:692) — request that extra LWLocks be
/// allocated during postmaster startup. May only be called from a
/// `shmem_request_hook`; calls from elsewhere `elog(FATAL)`.
/// `process_shmem_requests_in_progress` is the caller's view of miscinit.c's
/// flag of that name (explicit parameter per the no-ambient-seams rule).
pub fn RequestNamedLWLockTranche(
    mcx: Mcx<'_>,
    tranche_name: &str,
    num_lwlocks: i32,
    process_shmem_requests_in_progress: bool,
) -> PgResult<()> {
    if !process_shmem_requests_in_progress {
        elog(
            FATAL,
            "cannot request additional LWLocks outside shmem_request_hook",
        )?;
    }

    // C: Assert(strlen(tranche_name) + 1 <= NAMEDATALEN), then
    // strlcpy(request->tranche_name, tranche_name, NAMEDATALEN), i.e. a
    // BYTE-wise truncation to at most NAMEDATALEN - 1 bytes (backed off to
    // the nearest char boundary, since a Rust String cannot hold a split
    // UTF-8 sequence).
    debug_assert!(tranche_name.len() < NAMEDATALEN as usize);
    let mut clamp_len = tranche_name.len().min(NAMEDATALEN as usize - 1);
    while !tranche_name.is_char_boundary(clamp_len) {
        clamp_len -= 1;
    }
    let clamped: String = tranche_name[..clamp_len].to_owned();

    NAMED_LWLOCK_TRANCHE_REQUESTS.with(|reqs| {
        let mut reqs = reqs.borrow_mut();
        reqs.try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<NamedLWLockTrancheRequest>()))?;
        reqs.push(NamedLWLockTrancheRequest {
            tranche_name: clamped,
            num_lwlocks,
        });
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// LWLockInitialize + wait-event reporting + tranche names
// (lwlock.c lines 728-799).
// ---------------------------------------------------------------------------

/// `LWLockInitialize` (lwlock.c:728) — initialize a new lwlock; it's initially
/// unlocked.
pub fn LWLockInitialize(lock: &mut LWLock, tranche_id: i32) {
    // pg_atomic_init_u32(&lock->state, LW_FLAG_RELEASE_OK)
    lock.state = pg_atomic_uint32::new(LW_FLAG_RELEASE_OK);
    lock.tranche = tranche_id as uint16;
    proclist_init(lock.waiters.get_mut());
}

/// `LWLockReportWaitStart` (lwlock.c:747).
fn LWLockReportWaitStart(lock: &LWLock) {
    waitevent::pgstat_report_wait_start::call(PG_WAIT_LWLOCK | lock.tranche as uint32);
}

/// `LWLockReportWaitEnd` (lwlock.c:756).
fn LWLockReportWaitEnd() {
    waitevent::pgstat_report_wait_end::call();
}

/// `GetLWTrancheName` (lwlock.c:765) — the name of an LWLock tranche. C
/// returns a stored `const char *`; the thread-local registry forces an owned
/// copy here. For the builtin gap slots (removed lwlocklist.h ids, NULL in
/// C's array and never assigned to a lock) this returns `"unknown"`.
pub fn GetLWTrancheName(trancheId: uint16) -> String {
    // Built-in tranche or individual LWLock?
    if (trancheId as i32) < LWTRANCHE_FIRST_USER_DEFINED {
        let name = BUILTIN_TRANCHE_NAMES[trancheId as usize];
        return if name.is_empty() { "unknown" } else { name }.to_owned();
    }

    // It's an extension tranche; if never registered in the current process,
    // give up and return "extension".
    let index = (trancheId as i32 - LWTRANCHE_FIRST_USER_DEFINED) as usize;
    LWLOCK_TRANCHE_NAMES.with(|names| {
        names
            .borrow()
            .get(index)
            .and_then(|slot| slot.clone())
            .unwrap_or_else(|| "extension".to_owned())
    })
}

/// `GetLWLockIdentifier` (lwlock.c:789) — identifier for an LWLock based on
/// the wait class and event; the event IDs are just tranche numbers.
pub fn GetLWLockIdentifier(classId: uint32, eventId: uint16) -> String {
    debug_assert!(classId == PG_WAIT_LWLOCK);
    GetLWTrancheName(eventId)
}

/// `T_NAME(lock)`.
fn t_name(lock: &LWLock) -> String {
    GetLWTrancheName(lock.tranche)
}

// ---------------------------------------------------------------------------
// LWLockAttemptLock (lwlock.c:805) — atomic try-acquire, no blocking.
// ---------------------------------------------------------------------------

/// Try to atomically acquire `lock` in `mode`; returns `true` if the lock
/// isn't free and we need to wait.
fn LWLockAttemptLock(lock: &LWLock, mode: LWLockMode) -> bool {
    debug_assert!(mode == LW_EXCLUSIVE || mode == LW_SHARED);

    let state = atomic_state(lock);
    // Read once outside the loop; later iterations get the newer value via
    // compare & exchange.
    let mut old_state = state.load(Ordering::Relaxed);
    loop {
        let mut desired_state = old_state;
        let lock_free = if mode == LW_EXCLUSIVE {
            let free = old_state & LW_LOCK_MASK == 0;
            if free {
                desired_state = desired_state.wrapping_add(LW_VAL_EXCLUSIVE);
            }
            free
        } else {
            let free = old_state & LW_VAL_EXCLUSIVE == 0;
            if free {
                desired_state = desired_state.wrapping_add(LW_VAL_SHARED);
            }
            free
        };

        // We always swap in the value (even when we saw the lock as taken)
        // because the swap doubles as a memory barrier.
        match state.compare_exchange_weak(
            old_state,
            desired_state,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ) {
            Ok(_) => return !lock_free,
            Err(actual) => old_state = actual,
        }
    }
}

// ---------------------------------------------------------------------------
// Wait-list spinlock (lwlock.c lines 876-936).
// ---------------------------------------------------------------------------

/// `LWLockWaitListLock` (lwlock.c:877) — lock the LWLock's wait list against
/// concurrent activity by setting `LW_FLAG_LOCKED`. Non-conflicting lock
/// operations may still happen concurrently.
fn LWLockWaitListLock(lock: &LWLock) {
    let state = atomic_state(lock);
    loop {
        // always try once to acquire lock directly
        let old_state = state.fetch_or(LW_FLAG_LOCKED, Ordering::AcqRel);
        if old_state & LW_FLAG_LOCKED == 0 {
            break; // got lock
        }

        // and then spin without atomic operations until lock is released
        let mut delay_status = s_lock::init_spin_delay(
            Some(file!()),
            line!() as i32,
            Some("LWLockWaitListLock"),
        );
        let mut old_state = old_state;
        while old_state & LW_FLAG_LOCKED != 0 {
            s_lock::perform_spin_delay(&mut delay_status);
            old_state = state.load(Ordering::Relaxed);
        }
        s_lock::finish_spin_delay(&delay_status);

        // Retry; the lock might already be re-acquired by now.
    }
}

/// `LWLockWaitListUnlock` (lwlock.c:929) — clear `LW_FLAG_LOCKED`.
fn LWLockWaitListUnlock(lock: &LWLock) {
    let old_state = atomic_state(lock).fetch_and(!LW_FLAG_LOCKED, Ordering::Release);
    debug_assert!(old_state & LW_FLAG_LOCKED != 0);
}

// ---------------------------------------------------------------------------
// LWLockWakeup (lwlock.c:941).
// ---------------------------------------------------------------------------

/// Wake up all the lockers that currently have a chance to acquire the lock.
fn LWLockWakeup(lock: &LWLock) {
    let mut new_release_ok = true;
    let mut wokeup_somebody = false;
    let mut wakeup = proclist_head::default();
    proclist_init(&mut wakeup);

    // lock wait list while collecting backends to wake up
    LWLockWaitListLock(lock);

    // SAFETY: LW_FLAG_LOCKED is held until the flag-clearing CAS below (the
    // wait-list unlock fused with the flag updates).
    let waiters = unsafe { waiters_mut(lock) };
    proclist_foreach_modify(waiters.head, |cur| {
        let wait_mode = proc_s::proc_lw_wait_mode::call(cur);

        if wokeup_somebody && wait_mode == LW_EXCLUSIVE {
            return ControlFlow::Continue(());
        }

        proclist_delete(waiters, cur);
        proclist_push_tail(&mut wakeup, cur);

        if wait_mode != LW_WAIT_UNTIL_FREE {
            // Prevent additional wakeups until retryer gets to run. Backends
            // that are just waiting for the lock to become free don't retry
            // automatically.
            new_release_ok = false;
            // Don't wakeup (further) exclusive locks.
            wokeup_somebody = true;
        }

        // Signal that the process isn't on the wait list anymore: this allows
        // LWLockDequeueSelf to remove itself with a proclist_delete without
        // checking whether it has already been removed.
        debug_assert!(proc_s::proc_lw_waiting::call(cur) == LW_WS_WAITING);
        proc_s::set_proc_lw_waiting::call(cur, LW_WS_PENDING_WAKEUP);

        // Once we've woken up an exclusive lock, there's no point in waking
        // up anybody else.
        if wait_mode == LW_EXCLUSIVE {
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    });

    debug_assert!(
        proclist_is_empty(&wakeup)
            || atomic_state(lock).load(Ordering::Relaxed) & LW_FLAG_HAS_WAITERS != 0
    );

    // unset required flags, and release lock, in one fell swoop
    {
        let state = atomic_state(lock);
        let mut old_state = state.load(Ordering::Relaxed);
        loop {
            let mut desired_state = old_state;
            if new_release_ok {
                desired_state |= LW_FLAG_RELEASE_OK;
            } else {
                desired_state &= !LW_FLAG_RELEASE_OK;
            }
            if proclist_is_empty(waiters) {
                desired_state &= !LW_FLAG_HAS_WAITERS;
            }
            desired_state &= !LW_FLAG_LOCKED; // release lock
            match state.compare_exchange_weak(
                old_state,
                desired_state,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => old_state = actual,
            }
        }
    }

    // Awaken any waiters I removed from the queue.
    proclist_foreach_modify(wakeup.head, |cur| {
        proclist_delete(&mut wakeup, cur);
        // Guarantee that lwWaiting being unset only becomes visible once the
        // unlink from the list has completed; otherwise the target backend
        // could be woken for another reason and enqueue for a new lock before
        // the unlink happens, corrupting the list. The barrier pairs with
        // LWLockWaitListLock when enqueuing for another lock
        // (pg_write_barrier in C).
        fence(Ordering::Release);
        proc_s::set_proc_lw_waiting::call(cur, LW_WS_NOT_WAITING);
        proc_s::pg_semaphore_unlock::call(cur);
        ControlFlow::Continue(())
    });
}

// ---------------------------------------------------------------------------
// LWLockQueueSelf / LWLockDequeueSelf (lwlock.c lines 1057-1179).
// ---------------------------------------------------------------------------

/// `LWLockQueueSelf` (lwlock.c:1058) — add ourselves to the end of the queue.
/// NB: mode can be `LW_WAIT_UNTIL_FREE` here. The C `elog(PANIC)` paths are
/// `Err(PgError)` at PANIC level, like every other severity.
fn LWLockQueueSelf(lock: &LWLock, mode: LWLockMode, my_proc_number: ProcNumber) -> PgResult<()> {
    // If we don't have a PGPROC structure, there's no way to wait. This
    // should never occur, since MyProc should only be null during shared
    // memory initialization.
    if my_proc_number == INVALID_PROC_NUMBER {
        elog(PANIC, "cannot wait without a PGPROC structure")?;
    }

    if proc_s::proc_lw_waiting::call(my_proc_number) != LW_WS_NOT_WAITING {
        elog(PANIC, "queueing for lock while waiting on another one")?;
    }

    LWLockWaitListLock(lock);

    // setting the flag is protected by the spinlock
    atomic_state(lock).fetch_or(LW_FLAG_HAS_WAITERS, Ordering::AcqRel);

    proc_s::set_proc_lw_waiting::call(my_proc_number, LW_WS_WAITING);
    proc_s::set_proc_lw_wait_mode::call(my_proc_number, mode);

    // LW_WAIT_UNTIL_FREE waiters are always at the front of the queue
    // SAFETY: LW_FLAG_LOCKED is held until LWLockWaitListUnlock below.
    let waiters = unsafe { waiters_mut(lock) };
    if mode == LW_WAIT_UNTIL_FREE {
        proclist_push_head(waiters, my_proc_number);
    } else {
        proclist_push_tail(waiters, my_proc_number);
    }

    // Can release the mutex now
    LWLockWaitListUnlock(lock);
    Ok(())
}

/// `LWLockDequeueSelf` (lwlock.c:1101) — remove ourselves from the waitlist;
/// used when we queued ourselves but discovered we don't need to sleep.
fn LWLockDequeueSelf(lock: &LWLock, my_proc_number: ProcNumber) {
    LWLockWaitListLock(lock);

    // Remove ourselves from the waitlist, unless we've already been removed.
    // The removal happens with the wait list lock held, so there's no race.
    let on_waitlist = proc_s::proc_lw_waiting::call(my_proc_number) == LW_WS_WAITING;
    if on_waitlist {
        // SAFETY: LW_FLAG_LOCKED is held until LWLockWaitListUnlock below.
        proclist_delete(unsafe { waiters_mut(lock) }, my_proc_number);
    }

    // SAFETY: as above — the wait-list spinlock is still held.
    if proclist_is_empty(unsafe { waiters_mut(lock) })
        && (atomic_state(lock).load(Ordering::Relaxed) & LW_FLAG_HAS_WAITERS) != 0
    {
        atomic_state(lock).fetch_and(!LW_FLAG_HAS_WAITERS, Ordering::AcqRel);
    }

    LWLockWaitListUnlock(lock);

    // clear waiting state again, nice for debugging
    if on_waitlist {
        proc_s::set_proc_lw_waiting::call(my_proc_number, LW_WS_NOT_WAITING);
    } else {
        let mut extra_waits = 0_i32;

        // Somebody else dequeued us and has or will wake us up. Deal with
        // the superfluous absorption of a wakeup.

        // Reset RELEASE_OK flag if somebody woke us before we removed
        // ourselves — they'll have set it to false.
        atomic_state(lock).fetch_or(LW_FLAG_RELEASE_OK, Ordering::AcqRel);

        // Now wait for the scheduled wakeup, otherwise our ->lwWaiting would
        // get reset at some inconvenient point later.
        loop {
            proc_s::pg_semaphore_lock::call(my_proc_number);
            if proc_s::proc_lw_waiting::call(my_proc_number) == LW_WS_NOT_WAITING {
                break;
            }
            extra_waits += 1;
        }

        // Fix the process wait semaphore's count for any absorbed wakeups.
        while extra_waits > 0 {
            extra_waits -= 1;
            proc_s::pg_semaphore_unlock::call(my_proc_number);
        }
    }
}

/// The shared "wait until awakened" semaphore loop (lwlock.c:1306-1313 and
/// twins): block on our wait semaphore until `lwWaiting` is cleared,
/// returning the number of absorbed extra wakeups to repay later.
fn wait_until_awakened(my_proc_number: ProcNumber) -> i32 {
    let mut extra_waits = 0;
    loop {
        proc_s::pg_semaphore_lock::call(my_proc_number);
        if proc_s::proc_lw_waiting::call(my_proc_number) == LW_WS_NOT_WAITING {
            break;
        }
        extra_waits += 1;
    }
    extra_waits
}

// ---------------------------------------------------------------------------
// LWLockAcquire family (lwlock.c lines 1190-1533).
// ---------------------------------------------------------------------------

/// `LWLockAcquire` (lwlock.c:1190) — acquire a lightweight lock in the
/// specified mode; if not available, sleep until it is. Returns `true` if the
/// lock was available immediately, `false` if we had to sleep.
///
/// `my_proc_number` is the caller's `MyProcNumber` (C reads the globals.c
/// per-backend global ambiently; here it is an explicit parameter).
///
/// Side effect: cancel/die interrupts are held off until lock release.
pub fn LWLockAcquire(
    lock: &LWLock,
    mode: LWLockMode,
    my_proc_number: ProcNumber,
) -> PgResult<bool> {
    let mut result = true;
    let mut extra_waits = 0_i32;

    debug_assert!(mode == LW_SHARED || mode == LW_EXCLUSIVE);

    // Ensure we will have room to remember the lock.
    if !held_has_room() {
        elog(ERROR, "too many LWLocks taken")?;
    }

    // Lock out cancel/die interrupts until we exit the code section protected
    // by the LWLock. This ensures that interrupts will not interfere with
    // manipulations of data structures in shared memory.
    globals::hold_interrupts::call();

    // Loop here to try to acquire lock after each time we are signaled by
    // LWLockRelease. (Retrying instead of having the releaser grant the lock
    // avoids a process swap per acquisition under contention; see the C
    // comment / pgsql-hackers 29-Dec-01.)
    loop {
        // Try to grab the lock the first time, we're not in the waitqueue
        // yet/anymore.
        let mustwait = LWLockAttemptLock(lock, mode);
        if !mustwait {
            break; // got the lock
        }

        // The lock could long have been released by now; add us to the queue
        // and try to grab the lock again. If we succeed we need to revert the
        // queuing; otherwise the other locker will see our queue entry when
        // releasing, since it existed before we checked for the lock.
        LWLockQueueSelf(lock, mode, my_proc_number)?;

        // we're now guaranteed to be woken up if necessary
        let mustwait = LWLockAttemptLock(lock, mode);

        // ok, grabbed the lock the second time round, need to undo queueing
        if !mustwait {
            LWLockDequeueSelf(lock, my_proc_number);
            break;
        }

        // Wait until awakened. We can get awakened for a reason other than
        // being signaled by LWLockRelease; if so, loop back and wait again.
        LWLockReportWaitStart(lock);
        extra_waits += wait_until_awakened(my_proc_number);

        // Retrying, allow LWLockRelease to release waiters again.
        atomic_state(lock).fetch_or(LW_FLAG_RELEASE_OK, Ordering::AcqRel);

        LWLockReportWaitEnd();

        // Now loop back and try to acquire lock again.
        result = false;
    }

    // Add lock to list of locks held by this backend.
    record_held_lock(lock, mode);

    // Fix the process wait semaphore's count for any absorbed wakeups.
    while extra_waits > 0 {
        extra_waits -= 1;
        proc_s::pg_semaphore_unlock::call(my_proc_number);
    }

    Ok(result)
}

/// `LWLockConditionalAcquire` (lwlock.c:1361) — acquire if available, else
/// return `false` with no side-effects. If successful, cancel/die interrupts
/// are held off until lock release.
pub fn LWLockConditionalAcquire(lock: &LWLock, mode: LWLockMode) -> PgResult<bool> {
    debug_assert!(mode == LW_SHARED || mode == LW_EXCLUSIVE);

    // Ensure we will have room to remember the lock.
    if !held_has_room() {
        elog(ERROR, "too many LWLocks taken")?;
    }

    // Lock out cancel/die interrupts until we exit the code section protected
    // by the LWLock.
    globals::hold_interrupts::call();

    // Check for the lock
    let mustwait = LWLockAttemptLock(lock, mode);

    if mustwait {
        // Failed to get lock, so release interrupt holdoff.
        globals::resume_interrupts::call();
        Ok(false)
    } else {
        // Add lock to list of locks held by this backend.
        record_held_lock(lock, mode);
        Ok(true)
    }
}

/// `LWLockAcquireOrWait` (lwlock.c:1418) — acquire the lock if free (returns
/// `true`); otherwise wait until it is released and return `false` WITHOUT
/// acquiring it. Used for WALWriteLock: a backend flushing WAL flushes many
/// other backends' commit records as a side effect, so those backends only
/// need to wait for the flush to finish, not acquire the lock.
pub fn LWLockAcquireOrWait(
    lock: &LWLock,
    mode: LWLockMode,
    my_proc_number: ProcNumber,
) -> PgResult<bool> {
    let mut extra_waits = 0_i32;

    debug_assert!(mode == LW_SHARED || mode == LW_EXCLUSIVE);

    // Ensure we will have room to remember the lock.
    if !held_has_room() {
        elog(ERROR, "too many LWLocks taken")?;
    }

    // Lock out cancel/die interrupts until we exit the code section protected
    // by the LWLock.
    globals::hold_interrupts::call();

    // NB: nearly the same twice-in-a-row protocol as LWLockAcquire.
    let mut mustwait = LWLockAttemptLock(lock, mode);

    if mustwait {
        LWLockQueueSelf(lock, LW_WAIT_UNTIL_FREE, my_proc_number)?;

        mustwait = LWLockAttemptLock(lock, mode);

        if mustwait {
            // Wait until awakened. Like in LWLockAcquire, be prepared for
            // bogus wakeups.
            LWLockReportWaitStart(lock);
            extra_waits += wait_until_awakened(my_proc_number);
            LWLockReportWaitEnd();
        } else {
            // Got lock in the second attempt, undo queueing. We need to
            // treat this as having successfully acquired the lock, otherwise
            // we'd not necessarily wake up people we've prevented from
            // acquiring the lock.
            LWLockDequeueSelf(lock, my_proc_number);
        }
    }

    // Fix the process wait semaphore's count for any absorbed wakeups.
    while extra_waits > 0 {
        extra_waits -= 1;
        proc_s::pg_semaphore_unlock::call(my_proc_number);
    }

    if mustwait {
        // Failed to get lock, so release interrupt holdoff.
        globals::resume_interrupts::call();
        Ok(false)
    } else {
        // Add lock to list of locks held by this backend.
        record_held_lock(lock, mode);
        Ok(true)
    }
}

// ---------------------------------------------------------------------------
// LWLockConflictsWithVar / LWLockWaitForVar / LWLockUpdateVar
// (lwlock.c lines 1544-1796).
// ---------------------------------------------------------------------------

/// `LWLockConflictsWithVar` (lwlock.c:1544) — does the lwlock in its current
/// state need to wait for the variable value to change? Returns
/// `(mustwait, result)` where `result` is true if the lock was free; on a
/// value mismatch `*newval` receives the current value.
fn LWLockConflictsWithVar(
    lock: &LWLock,
    valptr: &pg_atomic_uint64,
    oldval: u64,
    newval: &mut u64,
) -> (bool, bool) {
    // Test first to see if it the slot is free right now.
    let mustwait = atomic_state(lock).load(Ordering::Relaxed) & LW_VAL_EXCLUSIVE != 0;

    if !mustwait {
        return (false, true); // *result = true
    }

    let result = false;

    // Reading this value atomically is safe even on platforms where uint64
    // cannot be read without observing a torn value.
    let value = atomic_var(valptr).load(Ordering::Relaxed);

    if value != oldval {
        *newval = value;
        (false, result)
    } else {
        (true, result)
    }
}

/// `LWLockWaitForVar` (lwlock.c:1606) — wait until the lock is free (returns
/// `true`), or the lock holder updates `*valptr` away from `oldval` (returns
/// `false` and stores the current value in `*newval`). Ignores shared lock
/// holders. Note: `LWLockConflictsWithVar` has no memory barrier; callers may
/// need an explicit one.
pub fn LWLockWaitForVar(
    lock: &LWLock,
    valptr: &pg_atomic_uint64,
    oldval: u64,
    newval: &mut u64,
    my_proc_number: ProcNumber,
) -> PgResult<bool> {
    let mut extra_waits = 0_i32;
    let mut result;

    // Lock out cancel/die interrupts while we sleep on the lock: there is no
    // cleanup mechanism to remove us from the wait queue if we get
    // interrupted.
    globals::hold_interrupts::call();

    // Loop here to check the lock's status after each time we are signaled.
    loop {
        let (mustwait, free) = LWLockConflictsWithVar(lock, valptr, oldval, newval);
        result = free;

        if !mustwait {
            break; // the lock was free or value didn't match
        }

        // Add myself to wait queue. This is racy (somebody else could wake up
        // before we're finished queuing) — the same twice-in-a-row protocol
        // as LWLockAcquire, except we also check the variable's value.
        LWLockQueueSelf(lock, LW_WAIT_UNTIL_FREE, my_proc_number)?;

        // Set RELEASE_OK flag, to make sure we get woken up as soon as the
        // lock is released.
        atomic_state(lock).fetch_or(LW_FLAG_RELEASE_OK, Ordering::AcqRel);

        // We're now guaranteed to be woken up if necessary. Recheck the lock
        // and variable's state.
        let (mustwait, free) = LWLockConflictsWithVar(lock, valptr, oldval, newval);
        result = free;

        // Ok, no conflict after we queued ourselves. Undo queueing.
        if !mustwait {
            LWLockDequeueSelf(lock, my_proc_number);
            break;
        }

        // Wait until awakened; be prepared for bogus wakeups.
        LWLockReportWaitStart(lock);
        extra_waits += wait_until_awakened(my_proc_number);
        LWLockReportWaitEnd();

        // Now loop back and check the status of the lock again.
    }

    // Fix the process wait semaphore's count for any absorbed wakeups.
    while extra_waits > 0 {
        extra_waits -= 1;
        proc_s::pg_semaphore_unlock::call(my_proc_number);
    }

    // Now okay to allow cancel/die interrupts.
    globals::resume_interrupts::call();

    Ok(result)
}

/// `LWLockUpdateVar` (lwlock.c:1742) — set `*valptr` to `val` and wake up all
/// processes waiting for us with `LWLockWaitForVar`, atomically (the value is
/// updated before waking waiters, so any `LWLockWaitForVar` on the same lock
/// is guaranteed to see the new value). The caller must hold the lock in
/// exclusive mode.
pub fn LWLockUpdateVar(lock: &LWLock, valptr: &pg_atomic_uint64, val: u64) {
    // pg_atomic_exchange_u64 is a full barrier, so the variable is updated
    // before waking up waiters.
    atomic_var(valptr).swap(val, Ordering::SeqCst);

    let mut wakeup = proclist_head::default();
    proclist_init(&mut wakeup);

    LWLockWaitListLock(lock);

    debug_assert!(atomic_state(lock).load(Ordering::Relaxed) & LW_VAL_EXCLUSIVE != 0);

    // See if there are any LW_WAIT_UNTIL_FREE waiters that need to be woken
    // up. They are always in the front of the queue.
    // SAFETY: LW_FLAG_LOCKED is held until LWLockWaitListUnlock below.
    let waiters = unsafe { waiters_mut(lock) };
    proclist_foreach_modify(waiters.head, |cur| {
        if proc_s::proc_lw_wait_mode::call(cur) != LW_WAIT_UNTIL_FREE {
            return ControlFlow::Break(());
        }

        proclist_delete(waiters, cur);
        proclist_push_tail(&mut wakeup, cur);

        // see LWLockWakeup()
        debug_assert!(proc_s::proc_lw_waiting::call(cur) == LW_WS_WAITING);
        proc_s::set_proc_lw_waiting::call(cur, LW_WS_PENDING_WAKEUP);
        ControlFlow::Continue(())
    });

    // We are done updating shared state of the lock itself.
    LWLockWaitListUnlock(lock);

    // Awaken any waiters I removed from the queue.
    proclist_foreach_modify(wakeup.head, |cur| {
        proclist_delete(&mut wakeup, cur);
        // check comment in LWLockWakeup() about this barrier
        fence(Ordering::Release);
        proc_s::set_proc_lw_waiting::call(cur, LW_WS_NOT_WAITING);
        proc_s::pg_semaphore_unlock::call(cur);
        ControlFlow::Continue(())
    });
}

// ---------------------------------------------------------------------------
// Disown / Release family (lwlock.c lines 1815-1975).
// ---------------------------------------------------------------------------

/// `LWLockDisownInternal` (lwlock.c:1816) — stop treating `lock` as held by
/// the current backend and return the mode it was held in. Does NOT
/// RESUME_INTERRUPTS; that's the caller's responsibility.
fn LWLockDisownInternal(lock: &LWLock) -> PgResult<LWLockMode> {
    // Remove lock from list of locks held. Usually, but not always, it will
    // be the latest-acquired lock; so search array backwards.
    match HELD_LWLOCKS.with(|held| held.borrow_mut().disown(lock)) {
        Some(mode) => Ok(mode),
        None => {
            elog(ERROR, format!("lock {} is not held", t_name(lock)))?;
            unreachable!("elog(ERROR) returns Err");
        }
    }
}

/// `LWLockReleaseInternal` (lwlock.c:1846) — release the hold; wake waiters
/// if appropriate. Shared between `LWLockRelease` and `LWLockReleaseDisowned`.
fn LWLockReleaseInternal(lock: &LWLock, mode: LWLockMode) {
    // Release my hold on lock, after that it can immediately be acquired by
    // others, even if we still have to wakeup other waiters.
    let state = atomic_state(lock);
    // pg_atomic_sub_fetch_u32 returns the NEW value.
    let oldstate = if mode == LW_EXCLUSIVE {
        state
            .fetch_sub(LW_VAL_EXCLUSIVE, Ordering::AcqRel)
            .wrapping_sub(LW_VAL_EXCLUSIVE)
    } else {
        state
            .fetch_sub(LW_VAL_SHARED, Ordering::AcqRel)
            .wrapping_sub(LW_VAL_SHARED)
    };

    // nobody else can have that kind of lock
    debug_assert!(oldstate & LW_VAL_EXCLUSIVE == 0);

    // We're still waiting for backends to get scheduled, don't wake them up
    // again.
    let check_waiters = oldstate & (LW_FLAG_HAS_WAITERS | LW_FLAG_RELEASE_OK)
        == (LW_FLAG_HAS_WAITERS | LW_FLAG_RELEASE_OK)
        && oldstate & LW_LOCK_MASK == 0;

    // As waking up waiters requires the spinlock to be acquired, only do so
    // if necessary.
    if check_waiters {
        LWLockWakeup(lock);
    }
}

/// `LWLockDisown` (lwlock.c:1899) — stop treating the lock as held by the
/// current backend without releasing it; the caller must ensure it is later
/// released via [`LWLockReleaseDisowned`], even on error.
pub fn LWLockDisown(lock: &LWLock) -> PgResult<()> {
    LWLockDisownInternal(lock)?;
    globals::resume_interrupts::call();
    Ok(())
}

/// `LWLockRelease` (lwlock.c:1910) — release a previously acquired lock.
pub fn LWLockRelease(lock: &LWLock) -> PgResult<()> {
    let mode = LWLockDisownInternal(lock)?;
    LWLockReleaseInternal(lock, mode);
    // Now okay to allow cancel/die interrupts.
    globals::resume_interrupts::call();
    Ok(())
}

/// `LWLockReleaseDisowned` (lwlock.c:1930) — release a lock previously
/// disowned with [`LWLockDisown`].
pub fn LWLockReleaseDisowned(lock: &LWLock, mode: LWLockMode) {
    LWLockReleaseInternal(lock, mode);
}

/// `LWLockReleaseClearVar` (lwlock.c:1939) — release a previously acquired
/// lock, resetting the variable first.
pub fn LWLockReleaseClearVar(
    lock: &LWLock,
    valptr: &pg_atomic_uint64,
    val: u64,
) -> PgResult<()> {
    // pg_atomic_exchange_u64 is a full barrier, so the variable is updated
    // before releasing the lock.
    atomic_var(valptr).swap(val, Ordering::SeqCst);
    LWLockRelease(lock)
}

/// `LWLockReleaseAll` (lwlock.c:1965) — release all currently-held locks
/// (cleanup after `ereport(ERROR)`). `InterruptHoldoffCount` is deliberately
/// left unchanged: error recovery already set it to the appropriate level, so
/// each iteration re-HOLDs to balance the RESUME inside `LWLockRelease`. Safe
/// to call before the LWLock subsystem is initialized (no locks held → no-op).
pub fn LWLockReleaseAll() -> PgResult<()> {
    while let Some(lock) = last_held_lock() {
        globals::hold_interrupts::call(); // match the upcoming RESUME_INTERRUPTS

        release_held(lock)?;
    }
    Ok(())
}

/// `ForEachLWLockHeldByMe` (lwlock.c:1984) — run a callback for each held
/// lock. Debug support only.
///
/// The callback receives `(&LWLock, LWLockMode)` for C's
/// `(LWLock *, LWLockMode, void *context)`; the `void *context`
/// out-parameter is subsumed by the closure's captures. The table is
/// snapshotted to a fixed-size stack copy first so the callback may re-enter
/// the held-lock table; like C, this allocates nothing.
pub fn ForEachLWLockHeldByMe(mut callback: impl FnMut(&LWLock, LWLockMode)) {
    let (snapshot, n) = HELD_LWLOCKS.with(|held| {
        let held = held.borrow();
        (held.locks, held.num_held)
    });
    for held in &snapshot[..n] {
        // SAFETY: every held entry was recorded from a live `&LWLock` this
        // backend acquired and must keep alive until release (PostgreSQL's
        // shared-memory contract).
        callback(unsafe { &*held.lock }, held.mode);
    }
}

/// `LWLockHeldByMe` (lwlock.c:1999) — does my process hold `lock` in any
/// mode? Debug support only.
pub fn LWLockHeldByMe(lock: &LWLock) -> bool {
    HELD_LWLOCKS.with(|held| held.borrow().contains(lock))
}

/// `LWLockAnyHeldByMe` (lwlock.c:2017) — does my process hold any of an array
/// of locks? Debug support only.
///
/// C takes `(LWLock *lock, int nlocks, size_t stride)` and tests whether any
/// held lock's pointer is a stride-aligned element of that window — i.e.
/// whether it is the `LWLock` embedded at offset 0 of one of the `nlocks`
/// array slots. The slice form expresses the same predicate directly.
pub fn LWLockAnyHeldByMe(locks: &[LWLockPadded]) -> bool {
    HELD_LWLOCKS.with(|held| {
        let held = held.borrow();
        locks.iter().any(|slot| held.contains(&slot.lock))
    })
}

/// `LWLockHeldByMeInMode` (lwlock.c:2043) — does my process hold `lock` in
/// the given mode? Debug support only.
pub fn LWLockHeldByMeInMode(lock: &LWLock, mode: LWLockMode) -> bool {
    HELD_LWLOCKS.with(|held| held.borrow().contains_in_mode(lock, mode))
}

// ---------------------------------------------------------------------------
// Held-lock table helpers.
// ---------------------------------------------------------------------------

/// The `num_held_lwlocks >= MAX_SIMUL_LWLOCKS` room check at the top of every
/// acquire path.
fn held_has_room() -> bool {
    HELD_LWLOCKS.with(|held| held.borrow().has_room())
}

/// `held_lwlocks[num_held_lwlocks++] = ...` — infallible once the room check
/// has passed, exactly like appending to C's fixed stack array.
fn record_held_lock(lock: &LWLock, mode: LWLockMode) {
    HELD_LWLOCKS.with(|held| held.borrow_mut().push(lock, mode));
}

fn last_held_lock() -> Option<*const LWLock> {
    HELD_LWLOCKS.with(|held| held.borrow().last())
}

/// `LWLockRelease(held_lwlocks[num_held_lwlocks - 1].lock)` for the
/// `LWLockReleaseAll` loop: release the given held lock.
fn release_held(lock: *const LWLock) -> PgResult<()> {
    let mode = HELD_LWLOCKS
        .with(|held| held.borrow_mut().disown(lock))
        .ok_or_else(|| PgError::error("held LWLock vanished during release"))?;
    // SAFETY: the held-lock table only carries pointers recorded from live
    // `&LWLock`s the current backend acquired and must keep alive until
    // release (PostgreSQL shared-memory contract).
    let lock: &LWLock = unsafe { &*lock };
    LWLockReleaseInternal(lock, mode);
    globals::resume_interrupts::call();
    Ok(())
}

// ---------------------------------------------------------------------------
// Seam installation.
// ---------------------------------------------------------------------------

/// `lwlock_acquire` seam shape: acquire, then hand back the guard wrapping
/// the still-borrowed lock (Drop = release, the C `LWLockReleaseAll`
/// error-recovery backstop).
fn lwlock_acquire_guard<'l>(
    lock: &'l LWLock,
    mode: LWLockMode,
    my_proc_number: ProcNumber,
) -> PgResult<backend_storage_lmgr_lwlock_seams::LWLockGuard<'l>> {
    let was_free = LWLockAcquire(lock, mode, my_proc_number)?;
    Ok(backend_storage_lmgr_lwlock_seams::LWLockGuard::new(
        lock, was_free,
    ))
}

/// Install every seam declared in `backend-storage-lmgr-lwlock-seams`.
pub fn init_seams() {
    backend_storage_lmgr_lwlock_seams::lwlock_initialize::set(LWLockInitialize);
    backend_storage_lmgr_lwlock_seams::lwlock_acquire::set(lwlock_acquire_guard);
    backend_storage_lmgr_lwlock_seams::lwlock_release::set(LWLockRelease);
}

#[cfg(test)]
mod tests;
