#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

use core::cell::UnsafeCell;
use core::ops::ControlFlow;
use core::sync::atomic::{AtomicI32, AtomicU32, AtomicU64, Ordering};

use lmgr_proc_seams as proc_s;
use lmgr_proc_seams::{proclist_head, proclist_node};
use init_small::globals;
use types_core::{ProcNumber, Size, INVALID_PROC_NUMBER, NAMEDATALEN};
use types_error::{PgError, PgResult, ERROR, FATAL, PANIC};
use waitevent_seams as waitevent;
use shmem_seams as shmem;

pub type LWLockMode = u8;
pub const LW_EXCLUSIVE: LWLockMode = 0;
pub const LW_SHARED: LWLockMode = 1;
pub const LW_WAIT_UNTIL_FREE: LWLockMode = 2;

pub const LW_WS_NOT_WAITING: u8 = 0;
pub const LW_WS_WAITING: u8 = 1;
pub const LW_WS_PENDING_WAKEUP: u8 = 2;

pub const MAX_BACKENDS: u32 = (1 << 18) - 1;

pub const LW_FLAG_HAS_WAITERS: u32 = 1 << 31;
pub const LW_FLAG_RELEASE_OK: u32 = 1 << 30;
pub const LW_FLAG_LOCKED: u32 = 1 << 29;
pub const LW_FLAG_BITS: u32 = 3;
pub const LW_FLAG_MASK: u32 = ((1 << LW_FLAG_BITS) - 1) << (32 - LW_FLAG_BITS);

pub const LW_VAL_EXCLUSIVE: u32 = MAX_BACKENDS + 1;
pub const LW_VAL_SHARED: u32 = 1;
pub const LW_SHARED_MASK: u32 = MAX_BACKENDS;
pub const LW_LOCK_MASK: u32 = MAX_BACKENDS | LW_VAL_EXCLUSIVE;

const _: () = assert!(((MAX_BACKENDS + 1) & MAX_BACKENDS) == 0);
const _: () = assert!((MAX_BACKENDS & LW_FLAG_MASK) == 0);
const _: () = assert!((LW_VAL_EXCLUSIVE & LW_FLAG_MASK) == 0);

pub const MAX_SIMUL_LWLOCKS: usize = 200;

pub const NUM_INDIVIDUAL_LWLOCKS: i32 = 54;
pub const NUM_BUFFER_PARTITIONS: i32 = 128;
pub const NUM_LOCK_PARTITIONS: i32 = 1 << 4;
pub const NUM_PREDICATELOCK_PARTITIONS: i32 = 1 << 4;
pub const BUFFER_MAPPING_LWLOCK_OFFSET: i32 = NUM_INDIVIDUAL_LWLOCKS;
pub const LOCK_MANAGER_LWLOCK_OFFSET: i32 = BUFFER_MAPPING_LWLOCK_OFFSET + NUM_BUFFER_PARTITIONS;
pub const PREDICATELOCK_MANAGER_LWLOCK_OFFSET: i32 =
    LOCK_MANAGER_LWLOCK_OFFSET + NUM_LOCK_PARTITIONS;
pub const NUM_FIXED_LWLOCKS: i32 =
    PREDICATELOCK_MANAGER_LWLOCK_OFFSET + NUM_PREDICATELOCK_PARTITIONS;

pub const LWTRANCHE_XACT_BUFFER: i32 = NUM_INDIVIDUAL_LWLOCKS;
pub const LWTRANCHE_BUFFER_MAPPING: i32 = LWTRANCHE_XACT_BUFFER + 12;
pub const LWTRANCHE_LOCK_MANAGER: i32 = LWTRANCHE_BUFFER_MAPPING + 1;
pub const LWTRANCHE_PREDICATE_LOCK_MANAGER: i32 = LWTRANCHE_LOCK_MANAGER + 1;
pub const LWTRANCHE_FIRST_USER_DEFINED: i32 = LWTRANCHE_XACT_BUFFER + 41;

pub const LWLOCK_PADDED_SIZE: usize = 128;

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

#[repr(C)]
pub struct LWLock {
    pub tranche: u16,
    pub state: AtomicU32,
    // INVARIANT: mutated only while LW_FLAG_LOCKED is held in `state`.
    pub waiters: UnsafeCell<proclist_head>,
}

// SAFETY: `state` is atomic; `waiters` is serialized by the LW_FLAG_LOCKED
// spinlock bit exactly as in C.
unsafe impl Sync for LWLock {}

#[repr(C, align(128))]
pub struct LWLockPadded {
    pub lock: LWLock,
}

const _: () = assert!(core::mem::size_of::<LWLock>() <= LWLOCK_PADDED_SIZE);
const _: () = assert!(core::mem::size_of::<LWLockPadded>() == LWLOCK_PADDED_SIZE);

impl LWLockPadded {
    pub const fn new_unlocked(tranche_id: i32) -> Self {
        Self {
            lock: LWLock {
                tranche: tranche_id as u16,
                state: AtomicU32::new(LW_FLAG_RELEASE_OK),
                waiters: UnsafeCell::new(proclist_head {
                    head: INVALID_PROC_NUMBER,
                    tail: INVALID_PROC_NUMBER,
                }),
            },
        }
    }
}

#[derive(Clone, Copy)]
struct LWLockHandle {
    lock: *const LWLock,
    mode: LWLockMode,
}

struct HeldLWLocks {
    num_held: usize,
    locks: [LWLockHandle; MAX_SIMUL_LWLOCKS],
}

const _: () = assert!(!core::mem::needs_drop::<HeldLWLocks>());

thread_local! {
    // held_lwlocks[]/num_held_lwlocks; UnsafeCell: all accesses are
    // straight-line borrows with no reentrant call inside.
    static HELD_LWLOCKS: UnsafeCell<HeldLWLocks> = const {
        UnsafeCell::new(HeldLWLocks {
            num_held: 0,
            locks: [LWLockHandle { lock: core::ptr::null(), mode: LW_EXCLUSIVE }; MAX_SIMUL_LWLOCKS],
        })
    };
}

#[inline]
fn with_held<R>(f: impl FnOnce(&mut HeldLWLocks) -> R) -> R {
    // SAFETY: backend-local TLS; `f` never re-enters HELD_LWLOCKS.
    HELD_LWLOCKS.with(|held| f(unsafe { &mut *held.get() }))
}

#[inline]
fn record_held(lock: &LWLock, mode: LWLockMode) {
    with_held(|held| {
        debug_assert!(held.num_held < MAX_SIMUL_LWLOCKS);
        // SAFETY: room checked at the top of the acquire path.
        unsafe { *held.locks.get_unchecked_mut(held.num_held) = LWLockHandle { lock, mode } };
        held.num_held += 1;
    });
}

impl HeldLWLocks {
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
}

#[track_caller]
#[cold]
#[inline(never)]
fn err_too_many_lwlocks() -> Box<PgError> {
    Box::new(PgError::new(ERROR, "too many LWLocks taken"))
}

#[track_caller]
#[cold]
#[inline(never)]
fn err_lock_not_held(lock: &LWLock) -> Box<PgError> {
    Box::new(PgError::new(
        ERROR,
        format!("lock {} is not held", GetLWTrancheName(lock.tranche)),
    ))
}

pub struct NamedLWLockTrancheRequest {
    pub tranche_name: [u8; NAMEDATALEN as usize],
    pub num_lwlocks: i32,
}

impl NamedLWLockTrancheRequest {
    pub fn name(&self) -> &str {
        let len = self
            .tranche_name
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(self.tranche_name.len());
        core::str::from_utf8(&self.tranche_name[..len]).unwrap_or("")
    }
}

pub struct NamedLWLockTrancheRange {
    pub tranche_name: [u8; NAMEDATALEN as usize],
    pub tranche_id: i32,
    pub start: usize,
    pub len: usize,
}

impl NamedLWLockTrancheRange {
    pub fn name(&self) -> &str {
        let len = self
            .tranche_name
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(self.tranche_name.len());
        core::str::from_utf8(&self.tranche_name[..len]).unwrap_or("")
    }
}

thread_local! {
    // LWLockTrancheNames[]: C keeps the caller's `const char *` uncopied;
    // `&'static str` is that contract.
    static LWLOCK_TRANCHE_NAMES: UnsafeCell<Vec<Option<&'static str>>> =
        const { UnsafeCell::new(Vec::new()) };

    static NAMED_LWLOCK_TRANCHE_REQUESTS: UnsafeCell<Vec<NamedLWLockTrancheRequest>> =
        const { UnsafeCell::new(Vec::new()) };
}

// *LWLockCounter: C's ShmemLock-guarded shmem int; still only touched
// under the shmem_lock seam.
static LWLOCK_COUNTER: AtomicI32 = AtomicI32::new(LWTRANCHE_FIRST_USER_DEFINED);

fn NumLWLocksForNamedTranches() -> i32 {
    NAMED_LWLOCK_TRANCHE_REQUESTS.with(|reqs| {
        // SAFETY: single-threaded startup TLS, straight-line borrow.
        unsafe { &*reqs.get() }
            .iter()
            .map(|request| request.num_lwlocks)
            .sum()
    })
}

pub fn LWLockShmemSize() -> PgResult<Size> {
    let num_locks = (NUM_FIXED_LWLOCKS + NumLWLocksForNamedTranches()) as Size;

    NAMED_LWLOCK_TRANCHE_REQUESTS.with(|reqs| {
        // SAFETY: single-threaded startup TLS, straight-line borrow.
        let reqs = unsafe { &*reqs.get() };

        let mut size = shmem::mul_size::call(num_locks, core::mem::size_of::<LWLockPadded>())?;
        size = shmem::add_size::call(size, core::mem::size_of::<i32>() + LWLOCK_PADDED_SIZE)?;
        size = shmem::add_size::call(
            size,
            shmem::mul_size::call(reqs.len(), core::mem::size_of::<NamedLWLockTrancheRange>())?,
        )?;
        for request in reqs.iter() {
            size = shmem::add_size::call(size, request.name().len() + 1)?;
        }
        Ok(size)
    })
}

pub struct LWLockTable {
    locks_ptr: *const LWLockPadded,
    locks_len: usize,
    named_tranches: Vec<NamedLWLockTrancheRange>,
}

// SAFETY: `locks_ptr` addresses the shared LWLock array (each slot `Sync`);
// `named_tranches` is written once in CreateLWLocks and read-only after.
unsafe impl Sync for LWLockTable {}
unsafe impl Send for LWLockTable {}

impl LWLockTable {
    pub fn locks(&self) -> &[LWLockPadded] {
        // SAFETY: `locks_ptr`/`locks_len` describe the live shared slot array,
        // allocated for the cluster lifetime.
        unsafe { core::slice::from_raw_parts(self.locks_ptr, self.locks_len) }
    }

    pub fn named_tranches(&self) -> &[NamedLWLockTrancheRange] {
        &self.named_tranches
    }

    pub fn lock(&self, index: usize) -> Option<&LWLock> {
        self.locks().get(index).map(|slot| &slot.lock)
    }
}

static MAIN_LWLOCKS: std::sync::OnceLock<LWLockTable> = std::sync::OnceLock::new();

pub fn main_lock(offset: usize) -> &'static LWLock {
    MAIN_LWLOCKS
        .get()
        .expect("MainLWLockArray not published (CreateLWLocks has not run)")
        .lock(offset)
        .expect("main LWLock offset out of range")
}

pub fn published_lwlock_table() -> Option<&'static LWLockTable> {
    MAIN_LWLOCKS.get()
}

pub fn CreateLWLocks(is_under_postmaster: bool) -> PgResult<&'static LWLockTable> {
    let table = if !is_under_postmaster {
        let total_locks = (NUM_FIXED_LWLOCKS + NumLWLocksForNamedTranches()) as usize;

        let space_locks = LWLockShmemSize()?;
        let raw = shmem::shmem_alloc::call(space_locks)?;

        // Counter int sits just before the array, then align up (slack
        // reserved in LWLockShmemSize).
        // SAFETY: offsets stay inside the `space_locks` reservation.
        let base = unsafe {
            let p = raw.add(core::mem::size_of::<i32>());
            let pad = LWLOCK_PADDED_SIZE - (p as usize) % LWLOCK_PADDED_SIZE;
            p.add(pad) as *mut LWLockPadded
        };

        // SAFETY: `total_locks` aligned slots, single-threaded init.
        for i in 0..total_locks {
            unsafe { core::ptr::write(base.add(i), LWLockPadded::new_unlocked(0)) };
        }

        LWLOCK_COUNTER.store(LWTRANCHE_FIRST_USER_DEFINED, Ordering::Relaxed);

        let locks_slice = unsafe { core::slice::from_raw_parts_mut(base, total_locks) };
        let named_tranches = InitializeLWLocks(locks_slice);

        if MAIN_LWLOCKS
            .set(LWLockTable {
                locks_ptr: base,
                locks_len: total_locks,
                named_tranches,
            })
            .is_err()
        {
            panic!("MainLWLockArray published twice");
        }
        MAIN_LWLOCKS.get().expect("just published")
    } else {
        MAIN_LWLOCKS
            .get()
            .expect("CreateLWLocks(is_under_postmaster) before postmaster created MainLWLockArray")
    };

    for range in table.named_tranches() {
        LWLockRegisterTranche(range.tranche_id, range.name());
    }

    Ok(table)
}

fn InitializeLWLocks(locks: &mut [LWLockPadded]) -> Vec<NamedLWLockTrancheRange> {
    for id in 0..NUM_INDIVIDUAL_LWLOCKS {
        LWLockInitialize(&mut locks[id as usize].lock, id);
    }
    for id in 0..NUM_BUFFER_PARTITIONS {
        LWLockInitialize(
            &mut locks[(BUFFER_MAPPING_LWLOCK_OFFSET + id) as usize].lock,
            LWTRANCHE_BUFFER_MAPPING,
        );
    }
    for id in 0..NUM_LOCK_PARTITIONS {
        LWLockInitialize(
            &mut locks[(LOCK_MANAGER_LWLOCK_OFFSET + id) as usize].lock,
            LWTRANCHE_LOCK_MANAGER,
        );
    }
    for id in 0..NUM_PREDICATELOCK_PARTITIONS {
        LWLockInitialize(
            &mut locks[(PREDICATELOCK_MANAGER_LWLOCK_OFFSET + id) as usize].lock,
            LWTRANCHE_PREDICATE_LOCK_MANAGER,
        );
    }

    NAMED_LWLOCK_TRANCHE_REQUESTS.with(|reqs| {
        // SAFETY: single-threaded startup TLS, straight-line borrow.
        let reqs = unsafe { &*reqs.get() };
        let mut named_tranches = Vec::with_capacity(reqs.len());
        let mut next_lock = NUM_FIXED_LWLOCKS as usize;
        for request in reqs.iter() {
            let tranche_id = LWLockNewTrancheId();
            for offset in 0..request.num_lwlocks as usize {
                LWLockInitialize(&mut locks[next_lock + offset].lock, tranche_id);
            }
            named_tranches.push(NamedLWLockTrancheRange {
                tranche_name: request.tranche_name,
                tranche_id,
                start: next_lock,
                len: request.num_lwlocks as usize,
            });
            next_lock += request.num_lwlocks as usize;
        }
        named_tranches
    })
}

pub fn InitLWLockAccess() {}

/// Crash-cycle reset in place (notes/crash-restart-design.md): re-arm every
/// published lock. Tranche ids are boot-stable (named requests fixed before
/// CreateLWLocks), so only state and wait lists reset.
pub fn LWLockResetAfterCrash() {
    let table = MAIN_LWLOCKS
        .get()
        .expect("LWLockResetAfterCrash before CreateLWLocks");
    for slot in table.locks() {
        slot.lock.state.store(LW_FLAG_RELEASE_OK, Ordering::Relaxed);
        // SAFETY: crash choreography drained every child before reset; the
        // postmaster thread has exclusive access, so no LW_FLAG_LOCKED holder
        // or waiter can exist.
        proclist_init(unsafe { &mut *slot.lock.waiters.get() });
    }
}

pub fn GetNamedLWLockTranche<'a>(
    table: &'a LWLockTable,
    tranche_name: &str,
) -> PgResult<&'a [LWLockPadded]> {
    match table
        .named_tranches
        .iter()
        .find(|range| range.name() == tranche_name)
    {
        Some(range) => Ok(&table.locks()[range.start..range.start + range.len]),
        None => Err(Box::new(PgError::new(
            ERROR,
            "requested tranche is not registered",
        ))),
    }
}

pub fn LWLockNewTrancheId() -> i32 {
    shmem::shmem_lock_acquire::call();
    let result = LWLOCK_COUNTER.load(Ordering::Relaxed);
    LWLOCK_COUNTER.store(result + 1, Ordering::Relaxed);
    shmem::shmem_lock_release::call();
    result
}

pub fn LWLockRegisterTranche(tranche_id: i32, tranche_name: &'static str) {
    if tranche_id < LWTRANCHE_FIRST_USER_DEFINED {
        return;
    }
    let index = (tranche_id - LWTRANCHE_FIRST_USER_DEFINED) as usize;

    LWLOCK_TRANCHE_NAMES.with(|names| {
        // SAFETY: backend-local TLS, straight-line borrow.
        let names = unsafe { &mut *names.get() };
        if index >= names.len() {
            let newalloc = (index + 1).max(8).next_power_of_two();
            names.resize(newalloc, None);
        }
        names[index] = Some(tranche_name);
    });
}

pub fn RequestNamedLWLockTranche(
    tranche_name: &str,
    num_lwlocks: i32,
    process_shmem_requests_in_progress: bool,
) -> PgResult<()> {
    if !process_shmem_requests_in_progress {
        return Err(Box::new(PgError::new(
            FATAL,
            "cannot request additional LWLocks outside shmem_request_hook",
        )));
    }

    debug_assert!(tranche_name.len() < NAMEDATALEN as usize);
    let mut name = [0u8; NAMEDATALEN as usize];
    let mut clamp_len = tranche_name.len().min(NAMEDATALEN as usize - 1);
    while !tranche_name.is_char_boundary(clamp_len) {
        clamp_len -= 1;
    }
    name[..clamp_len].copy_from_slice(&tranche_name.as_bytes()[..clamp_len]);

    NAMED_LWLOCK_TRANCHE_REQUESTS.with(|reqs| {
        // SAFETY: single-threaded startup TLS, straight-line borrow.
        unsafe { &mut *reqs.get() }.push(NamedLWLockTrancheRequest {
            tranche_name: name,
            num_lwlocks,
        });
    });
    Ok(())
}

pub fn LWLockInitialize(lock: &mut LWLock, tranche_id: i32) {
    lock.state = AtomicU32::new(LW_FLAG_RELEASE_OK);
    lock.tranche = tranche_id as u16;
    proclist_init(lock.waiters.get_mut());
}

fn LWLockReportWaitStart(lock: &LWLock) {
    waitevent::pgstat_report_wait_start::call(waitevent::PG_WAIT_LWLOCK | lock.tranche as u32);
}

fn LWLockReportWaitEnd() {
    waitevent::pgstat_report_wait_end::call();
}

pub fn GetLWTrancheName(trancheId: u16) -> &'static str {
    if (trancheId as i32) < LWTRANCHE_FIRST_USER_DEFINED {
        return BUILTIN_TRANCHE_NAMES[trancheId as usize];
    }

    let index = (trancheId as i32 - LWTRANCHE_FIRST_USER_DEFINED) as usize;
    LWLOCK_TRANCHE_NAMES.with(|names| {
        // SAFETY: backend-local TLS, straight-line borrow.
        unsafe { &*names.get() }
            .get(index)
            .copied()
            .flatten()
            .unwrap_or("extension")
    })
}

pub fn GetLWLockIdentifier(classId: u32, eventId: u16) -> &'static str {
    debug_assert!(classId == waitevent::PG_WAIT_LWLOCK);
    GetLWTrancheName(eventId)
}

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
    proc_s::set_proc_lw_wait_link::call(procno, proclist_node { next: 0, prev: 0 });
}

// `next` is cached before the body runs, so the body may delete `cur`.
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

// SAFETY contract: caller holds LW_FLAG_LOCKED; the borrow must not
// outlive that critical section.
unsafe fn waiters_mut(lock: &LWLock) -> &mut proclist_head {
    unsafe { &mut *lock.waiters.get() }
}

fn LWLockAttemptLock(lock: &LWLock, mode: LWLockMode) -> bool {
    debug_assert!(mode == LW_EXCLUSIVE || mode == LW_SHARED);

    if mode == LW_SHARED {
        // DIVERGENCE D1: shared acquire is a plain xadd, not C's CAS loop
        // (Freund's unshipped residual, docs/beat-postgres.md §9/item 7).
        let old_state = lock.state.fetch_add(LW_VAL_SHARED, Ordering::Acquire);
        if old_state & LW_VAL_EXCLUSIVE == 0 {
            return false;
        }
        LWLockAttemptLockBackout(lock);
        return true;
    }

    let mut old_state = lock.state.load(Ordering::Relaxed);
    loop {
        if old_state & LW_LOCK_MASK != 0 {
            // DIVERGENCE D3: C CASes the unchanged value here purely as a
            // barrier; lwlock.c:830 calls skipping it correctness-neutral.
            return true;
        }
        match lock.state.compare_exchange_weak(
            old_state,
            old_state.wrapping_add(LW_VAL_EXCLUSIVE),
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            Ok(_) => return false,
            Err(actual) => old_state = actual,
        }
    }
}

// Undo a failed shared xadd; mirrors LWLockReleaseInternal's wakeup check
// because the real releaser may have seen our transient share and skipped
// its wakeup.
#[cold]
fn LWLockAttemptLockBackout(lock: &LWLock) {
    let oldstate = lock
        .state
        .fetch_sub(LW_VAL_SHARED, Ordering::AcqRel)
        .wrapping_sub(LW_VAL_SHARED);

    let check_waiters = oldstate & (LW_FLAG_HAS_WAITERS | LW_FLAG_RELEASE_OK)
        == (LW_FLAG_HAS_WAITERS | LW_FLAG_RELEASE_OK)
        && oldstate & LW_LOCK_MASK == 0;
    if check_waiters {
        LWLockWakeup(lock);
    }
}

fn LWLockWaitListLock(lock: &LWLock) {
    loop {
        let old_state = lock.state.fetch_or(LW_FLAG_LOCKED, Ordering::Acquire);
        if old_state & LW_FLAG_LOCKED == 0 {
            break;
        }

        let mut delay_status =
            s_lock_seams::SpinDelayStatus::new(file!(), line!() as i32, "LWLockWaitListLock");
        let mut old_state = old_state;
        while old_state & LW_FLAG_LOCKED != 0 {
            s_lock_seams::perform_spin_delay::call(&mut delay_status);
            old_state = lock.state.load(Ordering::Relaxed);
        }
        s_lock_seams::finish_spin_delay::call(&delay_status);
    }
}

fn LWLockWaitListUnlock(lock: &LWLock) {
    let old_state = lock.state.fetch_and(!LW_FLAG_LOCKED, Ordering::Release);
    debug_assert!(old_state & LW_FLAG_LOCKED != 0);
}

fn LWLockWakeup(lock: &LWLock) {
    let mut new_release_ok = true;
    let mut wokeup_somebody = false;
    let mut wakeup = proclist_head::default();
    proclist_init(&mut wakeup);

    LWLockWaitListLock(lock);

    // SAFETY: LW_FLAG_LOCKED held until the flag-clearing CAS below.
    let waiters = unsafe { waiters_mut(lock) };
    proclist_foreach_modify(waiters.head, |cur| {
        let wait_mode = proc_s::proc_lw_wait_mode::call(cur);

        if wokeup_somebody && wait_mode == LW_EXCLUSIVE {
            return ControlFlow::Continue(());
        }

        proclist_delete(waiters, cur);
        proclist_push_tail(&mut wakeup, cur);

        if wait_mode != LW_WAIT_UNTIL_FREE {
            new_release_ok = false;
            wokeup_somebody = true;
        }

        debug_assert!(proc_s::proc_lw_waiting::call(cur) == LW_WS_WAITING);
        proc_s::set_proc_lw_waiting::call(cur, LW_WS_PENDING_WAKEUP);

        if wait_mode == LW_EXCLUSIVE {
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    });

    debug_assert!(
        proclist_is_empty(&wakeup)
            || lock.state.load(Ordering::Relaxed) & LW_FLAG_HAS_WAITERS != 0
    );

    let mut old_state = lock.state.load(Ordering::Relaxed);
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
        desired_state &= !LW_FLAG_LOCKED;
        match lock.state.compare_exchange_weak(
            old_state,
            desired_state,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(actual) => old_state = actual,
        }
    }

    proclist_foreach_modify(wakeup.head, |cur| {
        proclist_delete(&mut wakeup, cur);
        // lwWaiting may only appear unset after the unlink (pg_write_barrier
        // in C). The seam's store is Release and every wait-loop read is
        // Acquire (F-R1-5): the wakee may reach this flag WITHOUT a
        // semaphore edge (stale extraWaits counts), so the flag itself must
        // order the unlink writes before the wakee's re-enqueue link
        // accesses — C leans on the sem syscall barrier + control
        // dependency, which non-atomic Rust cells cannot.
        proc_s::set_proc_lw_waiting::call(cur, LW_WS_NOT_WAITING);
        proc_s::pg_semaphore_unlock::call(cur);
        ControlFlow::Continue(())
    });
}

fn LWLockQueueSelf(lock: &LWLock, mode: LWLockMode, my_proc_number: ProcNumber) -> PgResult<()> {
    if my_proc_number == INVALID_PROC_NUMBER {
        return Err(Box::new(PgError::new(
            PANIC,
            "cannot wait without a PGPROC structure",
        )));
    }
    if proc_s::proc_lw_waiting::call(my_proc_number) != LW_WS_NOT_WAITING {
        return Err(Box::new(PgError::new(
            PANIC,
            "queueing for lock while waiting on another one",
        )));
    }

    LWLockWaitListLock(lock);

    lock.state.fetch_or(LW_FLAG_HAS_WAITERS, Ordering::AcqRel);

    proc_s::set_proc_lw_waiting::call(my_proc_number, LW_WS_WAITING);
    proc_s::set_proc_lw_wait_mode::call(my_proc_number, mode);

    // SAFETY: LW_FLAG_LOCKED held until LWLockWaitListUnlock below.
    let waiters = unsafe { waiters_mut(lock) };
    if mode == LW_WAIT_UNTIL_FREE {
        proclist_push_head(waiters, my_proc_number);
    } else {
        proclist_push_tail(waiters, my_proc_number);
    }

    LWLockWaitListUnlock(lock);
    Ok(())
}

fn LWLockDequeueSelf(lock: &LWLock, my_proc_number: ProcNumber) {
    LWLockWaitListLock(lock);

    let on_waitlist = proc_s::proc_lw_waiting::call(my_proc_number) == LW_WS_WAITING;
    if on_waitlist {
        // SAFETY: LW_FLAG_LOCKED held until LWLockWaitListUnlock below.
        proclist_delete(unsafe { waiters_mut(lock) }, my_proc_number);
    }

    // SAFETY: as above — the wait-list lock is still held.
    if proclist_is_empty(unsafe { waiters_mut(lock) })
        && (lock.state.load(Ordering::Relaxed) & LW_FLAG_HAS_WAITERS) != 0
    {
        lock.state.fetch_and(!LW_FLAG_HAS_WAITERS, Ordering::AcqRel);
    }

    LWLockWaitListUnlock(lock);

    if on_waitlist {
        proc_s::set_proc_lw_waiting::call(my_proc_number, LW_WS_NOT_WAITING);
    } else {
        let mut extra_waits = 0_i32;
        lock.state.fetch_or(LW_FLAG_RELEASE_OK, Ordering::AcqRel);

        loop {
            proc_s::pg_semaphore_lock::call(my_proc_number);
            if proc_s::proc_lw_waiting::call(my_proc_number) == LW_WS_NOT_WAITING {
                break;
            }
            extra_waits += 1;
        }
        while extra_waits > 0 {
            extra_waits -= 1;
            proc_s::pg_semaphore_unlock::call(my_proc_number);
        }
    }
}

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

pub fn LWLockAcquire(
    lock: &LWLock,
    mode: LWLockMode,
    my_proc_number: ProcNumber,
) -> PgResult<bool> {
    debug_assert!(mode == LW_SHARED || mode == LW_EXCLUSIVE);

    // One TLS resolution for probe + record; a raw pointer, not a borrow,
    // crosses the wait path (which can reach seams).
    let held: *mut HeldLWLocks = HELD_LWLOCKS.with(|h| h.get());

    // SAFETY: backend-local TLS, no live borrow (with_held is not re-entered here).
    if unsafe { (*held).num_held } >= MAX_SIMUL_LWLOCKS {
        return Err(err_too_many_lwlocks());
    }

    globals::HoldInterrupts();

    let result = if LWLockAttemptLock(lock, mode) {
        // Contended: queue/sleep/retry lives out of line so the uncontended
        // acquire (the per-page scan path) carries no wait-loop frame — C's
        // small-frame LWLockAcquire shape.
        lwlock_acquire_wait(lock, mode, my_proc_number)?
    } else {
        true
    };

    // SAFETY: as above; room checked before the acquire.
    unsafe {
        let n = (*held).num_held;
        *(*held).locks.get_unchecked_mut(n) = LWLockHandle { lock, mode };
        (*held).num_held = n + 1;
    }
    Ok(result)
}

// The mustwait arm of LWLockAcquire (queue self, re-attempt, sleep, repay
// absorbed semaphore wakeups).
#[cold]
#[inline(never)]
fn lwlock_acquire_wait(
    lock: &LWLock,
    mode: LWLockMode,
    my_proc_number: ProcNumber,
) -> PgResult<bool> {
    let mut result = true;
    let mut extra_waits = 0_i32;

    loop {
        LWLockQueueSelf(lock, mode, my_proc_number)?;

        let mustwait = LWLockAttemptLock(lock, mode);
        if !mustwait {
            LWLockDequeueSelf(lock, my_proc_number);
            break;
        }

        LWLockReportWaitStart(lock);
        extra_waits += wait_until_awakened(my_proc_number);
        lock.state.fetch_or(LW_FLAG_RELEASE_OK, Ordering::AcqRel);
        LWLockReportWaitEnd();

        result = false;

        let mustwait = LWLockAttemptLock(lock, mode);
        if !mustwait {
            break;
        }
    }

    while extra_waits > 0 {
        extra_waits -= 1;
        proc_s::pg_semaphore_unlock::call(my_proc_number);
    }

    Ok(result)
}

pub fn LWLockConditionalAcquire(lock: &LWLock, mode: LWLockMode) -> PgResult<bool> {
    debug_assert!(mode == LW_SHARED || mode == LW_EXCLUSIVE);

    if !with_held(|held| held.num_held < MAX_SIMUL_LWLOCKS) {
        return Err(err_too_many_lwlocks());
    }

    globals::HoldInterrupts();

    let mustwait = LWLockAttemptLock(lock, mode);

    if mustwait {
        globals::ResumeInterrupts();
        Ok(false)
    } else {
        record_held(lock, mode);
        Ok(true)
    }
}

pub fn LWLockAcquireOrWait(
    lock: &LWLock,
    mode: LWLockMode,
    my_proc_number: ProcNumber,
) -> PgResult<bool> {
    let mut extra_waits = 0_i32;

    debug_assert!(mode == LW_SHARED || mode == LW_EXCLUSIVE);

    if !with_held(|held| held.num_held < MAX_SIMUL_LWLOCKS) {
        return Err(err_too_many_lwlocks());
    }

    globals::HoldInterrupts();

    let mut mustwait = LWLockAttemptLock(lock, mode);

    if mustwait {
        LWLockQueueSelf(lock, LW_WAIT_UNTIL_FREE, my_proc_number)?;

        mustwait = LWLockAttemptLock(lock, mode);

        if mustwait {
            LWLockReportWaitStart(lock);
            extra_waits += wait_until_awakened(my_proc_number);
            LWLockReportWaitEnd();
        } else {
            LWLockDequeueSelf(lock, my_proc_number);
        }
    }

    while extra_waits > 0 {
        extra_waits -= 1;
        proc_s::pg_semaphore_unlock::call(my_proc_number);
    }

    if mustwait {
        globals::ResumeInterrupts();
        Ok(false)
    } else {
        record_held(lock, mode);
        Ok(true)
    }
}

fn LWLockConflictsWithVar(
    lock: &LWLock,
    valptr: &AtomicU64,
    oldval: u64,
    newval: &mut u64,
) -> (bool, bool) {
    let mustwait = lock.state.load(Ordering::Relaxed) & LW_VAL_EXCLUSIVE != 0;
    if !mustwait {
        return (false, true);
    }

    let value = valptr.load(Ordering::Relaxed);
    if value != oldval {
        *newval = value;
        (false, false)
    } else {
        (true, false)
    }
}

pub fn LWLockWaitForVar(
    lock: &LWLock,
    valptr: &AtomicU64,
    oldval: u64,
    newval: &mut u64,
    my_proc_number: ProcNumber,
) -> PgResult<bool> {
    let mut extra_waits = 0_i32;
    let mut result;

    globals::HoldInterrupts();

    loop {
        let (mustwait, free) = LWLockConflictsWithVar(lock, valptr, oldval, newval);
        result = free;
        if !mustwait {
            break;
        }

        LWLockQueueSelf(lock, LW_WAIT_UNTIL_FREE, my_proc_number)?;
        lock.state.fetch_or(LW_FLAG_RELEASE_OK, Ordering::AcqRel);

        let (mustwait, free) = LWLockConflictsWithVar(lock, valptr, oldval, newval);
        result = free;
        if !mustwait {
            LWLockDequeueSelf(lock, my_proc_number);
            break;
        }

        LWLockReportWaitStart(lock);
        extra_waits += wait_until_awakened(my_proc_number);
        LWLockReportWaitEnd();
    }

    while extra_waits > 0 {
        extra_waits -= 1;
        proc_s::pg_semaphore_unlock::call(my_proc_number);
    }

    globals::ResumeInterrupts();

    Ok(result)
}

pub fn LWLockUpdateVar(lock: &LWLock, valptr: &AtomicU64, val: u64) {
    // The exchange publishes the value before any waiter can wake (C: full
    // barrier).
    valptr.swap(val, Ordering::AcqRel);

    let mut wakeup = proclist_head::default();
    proclist_init(&mut wakeup);

    LWLockWaitListLock(lock);

    debug_assert!(lock.state.load(Ordering::Relaxed) & LW_VAL_EXCLUSIVE != 0);

    // SAFETY: LW_FLAG_LOCKED held until LWLockWaitListUnlock below.
    let waiters = unsafe { waiters_mut(lock) };
    proclist_foreach_modify(waiters.head, |cur| {
        if proc_s::proc_lw_wait_mode::call(cur) != LW_WAIT_UNTIL_FREE {
            return ControlFlow::Break(());
        }

        proclist_delete(waiters, cur);
        proclist_push_tail(&mut wakeup, cur);

        debug_assert!(proc_s::proc_lw_waiting::call(cur) == LW_WS_WAITING);
        proc_s::set_proc_lw_waiting::call(cur, LW_WS_PENDING_WAKEUP);
        ControlFlow::Continue(())
    });

    LWLockWaitListUnlock(lock);

    proclist_foreach_modify(wakeup.head, |cur| {
        proclist_delete(&mut wakeup, cur);
        // See LWLockWakeup: the seam's Release store carries the
        // unlink-before-flag edge (F-R1-5).
        proc_s::set_proc_lw_waiting::call(cur, LW_WS_NOT_WAITING);
        proc_s::pg_semaphore_unlock::call(cur);
        ControlFlow::Continue(())
    });
}

fn LWLockDisownInternal(lock: &LWLock) -> PgResult<LWLockMode> {
    match with_held(|held| held.disown(lock)) {
        Some(mode) => Ok(mode),
        None => Err(err_lock_not_held(lock)),
    }
}

fn LWLockReleaseInternal(lock: &LWLock, mode: LWLockMode) {
    let sub = if mode == LW_EXCLUSIVE {
        LW_VAL_EXCLUSIVE
    } else {
        LW_VAL_SHARED
    };
    let oldstate = lock.state.fetch_sub(sub, Ordering::Release).wrapping_sub(sub);

    debug_assert!(oldstate & LW_VAL_EXCLUSIVE == 0);

    let check_waiters = oldstate & (LW_FLAG_HAS_WAITERS | LW_FLAG_RELEASE_OK)
        == (LW_FLAG_HAS_WAITERS | LW_FLAG_RELEASE_OK)
        && oldstate & LW_LOCK_MASK == 0;

    if check_waiters {
        LWLockWakeup(lock);
    }
}

pub fn LWLockDisown(lock: &LWLock) -> PgResult<()> {
    LWLockDisownInternal(lock)?;
    globals::ResumeInterrupts();
    Ok(())
}

pub fn LWLockRelease(lock: &LWLock) -> PgResult<()> {
    let mode = LWLockDisownInternal(lock)?;
    LWLockReleaseInternal(lock, mode);
    globals::ResumeInterrupts();
    Ok(())
}

pub fn LWLockReleaseDisowned(lock: &LWLock, mode: LWLockMode) {
    LWLockReleaseInternal(lock, mode);
}

pub fn LWLockReleaseClearVar(lock: &LWLock, valptr: &AtomicU64, val: u64) -> PgResult<()> {
    // Publish before release (C: pg_atomic_exchange_u64 full barrier).
    valptr.swap(val, Ordering::AcqRel);
    LWLockRelease(lock)
}

pub fn LWLockReleaseAll() -> PgResult<()> {
    // Re-HOLD balances the RESUME inside LWLockRelease; error recovery owns
    // the holdoff count, exactly as C.
    loop {
        let last = with_held(|held| {
            held.num_held
                .checked_sub(1)
                .map(|i| held.locks[i].lock)
        });
        let Some(lock) = last else { break };
        globals::HoldInterrupts();
        // SAFETY: held entries were recorded from live `&LWLock`s that must
        // stay alive until release (shared-memory contract).
        LWLockRelease(unsafe { &*lock })?;
    }
    Ok(())
}

pub fn ForEachLWLockHeldByMe(mut callback: impl FnMut(&LWLock, LWLockMode)) {
    let (snapshot, n) = with_held(|held| (held.locks, held.num_held));
    for held in &snapshot[..n] {
        // SAFETY: see LWLockReleaseAll.
        callback(unsafe { &*held.lock }, held.mode);
    }
}

pub fn LWLockHeldByMe(lock: &LWLock) -> bool {
    with_held(|held| {
        held.locks[..held.num_held]
            .iter()
            .any(|h| core::ptr::eq(h.lock, lock))
    })
}

pub fn LWLockAnyHeldByMe(locks: &[LWLockPadded]) -> bool {
    with_held(|held| {
        held.locks[..held.num_held].iter().any(|h| {
            locks
                .iter()
                .any(|slot| core::ptr::eq(h.lock, &slot.lock as *const LWLock))
        })
    })
}

pub fn LWLockHeldByMeInMode(lock: &LWLock, mode: LWLockMode) -> bool {
    with_held(|held| {
        held.locks[..held.num_held]
            .iter()
            .any(|h| core::ptr::eq(h.lock, lock) && h.mode == mode)
    })
}

#[cfg(test)]
mod tests;
