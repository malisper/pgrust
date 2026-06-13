//! Shared-memory sizing and one-time initialization (`storage/lmgr/proc.c`).
//!
//! `InitProcGlobal` runs once in the postmaster: it carves the `PGPROC` array
//! and the dense `ProcGlobal` mirror arrays (`xids`/`subxidStates`/
//! `statusFlags`) out of shared memory, initializes each `PGPROC`'s embedded
//! latch / semaphore / fast-path arrays, and threads every entry onto one of
//! the four freelists (`freeProcs` / `autovacFreeProcs` / `bgworkerFreeProcs`
//! / `walsenderFreeProcs`) by backend class.
//!
//! RECLAIMED here (real algorithm, not a seam): the by-class partitioning of
//! the real `PGPROC` array that `InitProcGlobal` performs (and that
//! `InitProcess` later pops from), over genuine `Vec<PGPROC>` mirror arrays.
//!
//! OUTWARD seams: globals.c (`MaxBackends`/`MaxConnections`/`max_prepared_xacts`
//! /`autovacuum_worker_slots`/`max_worker_processes`/
//! `FastPathLockGroupsPerBackend`), shmem.c (`add_size`/`mul_size` overflow-
//! checked size arithmetic), and lwlock.c (`LWLockInitialize` on each PGPROC's
//! `fpInfoLock`). Each panics loudly until its owner lands.

use core::cell::RefCell;
use core::mem::size_of;

use mcx::Mcx;
use types_core::{Oid, ProcNumber, Size, TransactionId, INVALID_PROC_NUMBER};
use types_error::PgResult;
use types_storage::latch::LatchHandle;
use types_storage::storage::{
    FreeListId, ProcFreeList, XidCacheStatus, FP_LOCK_SLOTS_PER_GROUP, LWTRANCHE_LOCK_FASTPATH,
    NUM_AUXILIARY_PROCS, NUM_LOCK_PARTITIONS, NUM_SPECIAL_WORKER_PROCS, PGPROC, PROC_HDR,
    PROC_WAIT_STATUS_OK,
};

use backend_storage_ipc_shmem_seams as shmem;
use backend_storage_lmgr_lwlock_seams as lwlock;
use backend_utils_init_small_seams as globals;

/// `DEFAULT_SPINS_PER_DELAY` (`storage/s_lock.h`): the initial shared estimate
/// of `spins_per_delay` that `InitProcGlobal` stamps into `ProcGlobal`.
const DEFAULT_SPINS_PER_DELAY: i32 = 100;

/// `MAXIMUM_ALIGNOF` (pg_config.h on every supported target).
const MAXIMUM_ALIGNOF: usize = 8;

/// `MAXALIGN(len)` (c.h): round up to the platform max alignment.
#[inline]
const fn maxalign(len: usize) -> usize {
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// `add_size(s1, s2)` (shmem.c) — overflow-checked size addition. The C
/// `ereport(ERROR)` on overflow is genuinely unreachable for these fixed
/// startup sizes; the size-returning C signature is preserved by surfacing the
/// seam's `Err` as a panic (matching C's abort-on-overflow).
#[inline]
fn add_size(s1: Size, s2: Size) -> Size {
    shmem::add_size::call(s1, s2).expect("proc shmem size overflow")
}

/// `mul_size(s1, s2)` (shmem.c) — overflow-checked size multiplication.
#[inline]
fn mul_size(s1: Size, s2: Size) -> Size {
    shmem::mul_size::call(s1, s2).expect("proc shmem size overflow")
}

/// `FastPathLockSlotsPerBackend()` (proc.h macro):
/// `FP_LOCK_SLOTS_PER_GROUP * FastPathLockGroupsPerBackend`.
#[inline]
fn fast_path_lock_slots_per_backend() -> i32 {
    FP_LOCK_SLOTS_PER_GROUP * globals::fast_path_lock_groups_per_backend::call()
}

/// `TotalProcs = MaxBackends + NUM_AUXILIARY_PROCS + max_prepared_xacts`.
#[inline]
fn total_procs() -> Size {
    add_size(
        globals::max_backends::call() as Size,
        add_size(
            NUM_AUXILIARY_PROCS as Size,
            globals::max_prepared_xacts::call() as Size,
        ),
    )
}

/// `PGProcShmemSize(void)` — bytes for the `PGPROC` array (regular + special
/// worker + aux + prepared-xact dummies) plus the dense mirror arrays.
pub fn PGProcShmemSize() -> Size {
    let mut size: Size = 0;
    let total_procs = total_procs();

    size = add_size(size, mul_size(total_procs, size_of::<PGPROC>()));
    size = add_size(size, mul_size(total_procs, size_of::<TransactionId>()));
    size = add_size(size, mul_size(total_procs, size_of::<XidCacheStatus>()));
    size = add_size(size, mul_size(total_procs, size_of::<u8>()));

    size
}

/// `FastPathLockShmemSize(void)` — bytes for the per-backend fast-path lock
/// bit/relid arrays referenced from each `PGPROC`.
pub fn FastPathLockShmemSize() -> Size {
    let mut size: Size = 0;
    let total_procs = total_procs();

    // Memory needed for PGPROC fast-path lock arrays. Make sure the sizes are
    // nicely aligned in each backend.
    let fp_lock_bits_size =
        maxalign(globals::fast_path_lock_groups_per_backend::call() as usize * size_of::<u64>());
    let fp_rel_id_size = maxalign(fast_path_lock_slots_per_backend() as usize * size_of::<Oid>());

    size = add_size(size, mul_size(total_procs, add_size(fp_lock_bits_size, fp_rel_id_size)));

    size
}

/// `ProcGlobalShmemSize(void)` — total shared memory for the proc subsystem
/// (`PROC_HDR` + [`PGProcShmemSize`] + [`FastPathLockShmemSize`] + semaphores).
pub fn ProcGlobalShmemSize() -> Size {
    let mut size: Size = 0;

    // ProcGlobal + the ProcStructLock spinlock word (`slock_t`).
    size = add_size(size, size_of::<PROC_HDR>());
    size = add_size(size, size_of::<types_storage::storage::Spinlock>());

    size = add_size(size, PGProcShmemSize());
    size = add_size(size, FastPathLockShmemSize());

    size
}

/// `ProcGlobalSemas(void)` — number of PGSemaphores the proc subsystem needs:
/// a sema per backend (including autovacuum), plus one per auxiliary process.
pub fn ProcGlobalSemas() -> i32 {
    globals::max_backends::call() + NUM_AUXILIARY_PROCS
}

thread_local! {
    /// `PROC_HDR *ProcGlobal` (proc.c file-scope global). In C this is a
    /// shared-memory pointer the postmaster sets and every backend inherits;
    /// here the crate owns the `PROC_HDR` value built by `InitProcGlobal`.
    /// Other proc.c family modules reach it through [`with_proc_global`].
    static PROC_GLOBAL: RefCell<Option<PROC_HDR>> = const { RefCell::new(None) };
}

thread_local! {
    /// `slock_t *ProcStructLock` (proc.c file-scope global) — the spinlock that
    /// serializes access to the `ProcGlobal` freelists / `AuxiliaryProcs` slots.
    /// In C this is a separate `ShmemInitStruct` word; here the crate owns the
    /// `Spinlock` word (initialized free, matching `SpinLockInit` in
    /// `InitProcGlobal`), acquired/released through the merged `s_lock.c`
    /// primitive.
    static PROC_STRUCT_LOCK: types_storage::storage::Spinlock =
        types_storage::storage::Spinlock::new();
}

thread_local! {
    /// `ProcNumber MyProcNumber` (proc.c backend-local global): the slot number
    /// of this backend's `PGPROC` in `ProcGlobal->allProcs`, or
    /// `INVALID_PROC_NUMBER` when none is claimed. In C `MyProc` is the
    /// `PGPROC *`; an arena index is the faithful realization (no raw pointer
    /// escapes), so `MyProc != NULL` becomes `MY_PROC_NUMBER.is_some()`.
    static MY_PROC_NUMBER: RefCell<Option<ProcNumber>> = const { RefCell::new(None) };
}

/// Run `f` with mutable access to the cluster-wide `ProcGlobal` (`PROC_HDR`),
/// panicking when it has not yet been built by [`InitProcGlobal`] (mirroring
/// proc.c's `Assert(ProcGlobal != NULL)`).
pub(crate) fn with_proc_global<R>(f: impl FnOnce(&mut PROC_HDR) -> R) -> R {
    PROC_GLOBAL.with(|cell| {
        let mut borrow = cell.borrow_mut();
        let pg = borrow
            .as_mut()
            .expect("proc header uninitialized (InitProcGlobal not run)");
        f(pg)
    })
}

/// Whether [`InitProcGlobal`] has already built `ProcGlobal`.
pub(crate) fn proc_global_initialized() -> bool {
    PROC_GLOBAL.with(|cell| cell.borrow().is_some())
}

/// `SpinLockAcquire(ProcStructLock)` — uncontended test-and-set fast path,
/// falling back to the `s_lock.c` backoff loop on contention.
pub(crate) fn spin_lock_acquire_proc_struct_lock() {
    PROC_STRUCT_LOCK.with(|lock| {
        // SpinLockAcquire: TAS_SPIN; on failure, s_lock() the backoff loop.
        if lock.tas_spin() != 0 {
            backend_storage_lmgr_s_lock::s_lock(lock, Some(file!()), line!() as i32, None);
        }
    });
}

/// `SpinLockRelease(ProcStructLock)` — fence-ordered store of zero.
pub(crate) fn spin_lock_release_proc_struct_lock() {
    PROC_STRUCT_LOCK.with(|lock| lock.unlock());
}

// ---- per-backend MyProc / MyProcNumber / MyProcPid (proc.c backend-locals) ----

/// `MyProc != NULL`.
pub(crate) fn my_proc_is_set() -> bool {
    MY_PROC_NUMBER.with(|c| c.borrow().is_some())
}

/// `MyProc = GetPGProcByNumber(procno); MyProcNumber = procno`. (The single
/// `MyProcNumber` backs both `MyProc` and `MyProcNumber` in C, since `MyProc`
/// is just `&allProcs[MyProcNumber]`.)
pub(crate) fn set_my_proc_number(procno: ProcNumber) {
    MY_PROC_NUMBER.with(|c| *c.borrow_mut() = Some(procno));
}

/// `MyProc = NULL` / `MyProcNumber = INVALID_PROC_NUMBER`.
pub(crate) fn clear_my_proc() {
    MY_PROC_NUMBER.with(|c| *c.borrow_mut() = None);
}

/// `GetNumberFromPGProc(MyProc)` — panics if `MyProc == NULL`.
pub(crate) fn my_proc_number() -> ProcNumber {
    MY_PROC_NUMBER.with(|c| c.borrow().expect("MyProc is NULL (no PGPROC claimed)"))
}

/// Run `f` with mutable access to this backend's claimed `PGPROC`
/// (`&mut *MyProc`), without ever handing out a `&'static mut`. Panics when
/// `MyProc == NULL` or `ProcGlobal` is unbuilt, mirroring the C deref of a
/// `MyProc` that must be non-NULL at the call site.
pub(crate) fn with_my_proc<R>(f: impl FnOnce(&mut PGPROC) -> R) -> R {
    let procno = my_proc_number();
    with_proc_by_number(procno, f)
}

/// Run `f` with shared access to this backend's claimed `PGPROC` (`&*MyProc`).
pub(crate) fn with_my_proc_ref<R>(f: impl FnOnce(&PGPROC) -> R) -> R {
    let procno = my_proc_number();
    with_proc_global(|pg| f(&pg.allProcs[procno as usize]))
}

/// Run `f` with mutable access to `GetPGProcByNumber(procno)` over the owned
/// arena.
pub(crate) fn with_proc_by_number<R>(procno: ProcNumber, f: impl FnOnce(&mut PGPROC) -> R) -> R {
    with_proc_global(|pg| f(&mut pg.allProcs[procno as usize]))
}

/// `GetNumberFromPGProc(proc)` — the slot index of `proc` in
/// `ProcGlobal->allProcs`, the same `proc - ProcGlobal->allProcs` pointer
/// arithmetic the C macro performs. `proc` must point into the owned arena;
/// panics otherwise (a caller bug, mirroring the undefined behaviour of the C
/// macro on a foreign pointer).
pub(crate) fn proc_number_of(proc: &PGPROC) -> ProcNumber {
    with_proc_global(|pg| {
        let base = pg.allProcs.as_ptr();
        let p = proc as *const PGPROC;
        let off = (p as usize)
            .checked_sub(base as usize)
            .map(|bytes| bytes / core::mem::size_of::<PGPROC>())
            .filter(|&i| i < pg.allProcs.len());
        off.expect("GetNumberFromPGProc: PGPROC is not an element of ProcGlobal->allProcs")
            as ProcNumber
    })
}

// ---- freelist operations over ProcGlobal's four heads ----

/// Borrow the [`ProcFreeList`] head named by `list`.
fn freelist_of(pg: &mut PROC_HDR, list: FreeListId) -> &mut ProcFreeList {
    match list {
        FreeListId::Regular => &mut pg.freeProcs,
        FreeListId::Autovac => &mut pg.autovacFreeProcs,
        FreeListId::Bgworker => &mut pg.bgworkerFreeProcs,
        FreeListId::Walsender => &mut pg.walsenderFreeProcs,
    }
}

/// `GetPGProcByNumber(procno)->procgloballist` mapped to its [`FreeListId`].
/// Panics if the slot belongs to no freelist (aux / prepared-xact dummy), which
/// would be a caller bug (the C deref of a NULL `procgloballist`).
pub(crate) fn proc_globallist_of(procno: ProcNumber) -> FreeListId {
    with_proc_global(|pg| {
        pg.allProcs[procno as usize]
            .procgloballist
            .expect("PGPROC has no procgloballist (not a freelist-managed slot)")
    })
}

/// `dlist_container(PGPROC, links, dlist_pop_head_node(<list>))`.
pub(crate) fn freelist_pop_head(list: FreeListId) -> Option<ProcNumber> {
    with_proc_global(|pg| freelist_of(pg, list).pop_head())
}

/// `dlist_push_head(<list>, &GetPGProcByNumber(procno)->links)`.
pub(crate) fn freelist_push_head(list: FreeListId, procno: ProcNumber) {
    with_proc_global(|pg| freelist_of(pg, list).push_head(procno));
}

/// `dlist_push_tail(<list>, &GetPGProcByNumber(procno)->links)`.
pub(crate) fn freelist_push_tail(list: FreeListId, procno: ProcNumber) {
    with_proc_global(|pg| freelist_of(pg, list).push_tail(procno));
}

/// A snapshot of `ProcGlobal->freeProcs` in list order, for `HaveNFreeProcs`'s
/// `dlist_foreach`. (A snapshot — rather than a live iterator — avoids holding
/// the `ProcGlobal` borrow across the caller's loop; the caller holds
/// `ProcStructLock`, so the list cannot change underneath it.)
pub(crate) fn freelist_regular_snapshot() -> Vec<ProcNumber> {
    with_proc_global(|pg| pg.freeProcs.members.iter().copied().collect())
}

// ---- ProcGlobal scalar fields ----

pub(crate) fn spins_per_delay() -> i32 {
    with_proc_global(|pg| pg.spins_per_delay)
}

pub(crate) fn set_spins_per_delay(value: i32) {
    with_proc_global(|pg| pg.spins_per_delay = value);
}

pub(crate) fn startup_buffer_pin_wait_buf_id() -> i32 {
    with_proc_global(|pg| pg.startupBufferPinWaitBufId)
}

pub(crate) fn set_startup_buffer_pin_wait_buf_id(bufid: i32) {
    with_proc_global(|pg| pg.startupBufferPinWaitBufId = bufid);
}

/// `ProcGlobal->statusFlags[pgxactoff]`.
pub(crate) fn status_flags(pgxactoff: i32) -> u8 {
    with_proc_global(|pg| pg.statusFlags[pgxactoff as usize])
}

// ---- AuxiliaryProcs (= &allProcs[MaxBackends..][..NUM_AUXILIARY_PROCS]) ----

/// `GetNumberFromPGProc(&AuxiliaryProcs[proctype])` — the absolute slot number
/// of auxiliary entry `proctype`. In C `AuxiliaryProcs = &allProcs[MaxBackends]`.
pub(crate) fn auxiliary_proc_procno(proctype: i32) -> ProcNumber {
    globals::max_backends::call() + proctype
}

/// `GetNumberFromPGProc(&PreparedXactProcs[i])` — the absolute slot number of
/// prepared-xact dummy `i`. In C `PreparedXactProcs = &allProcs[MaxBackends +
/// NUM_AUXILIARY_PROCS]`, the dummy PGPROCs following the regular + auxiliary
/// slots.
pub(crate) fn prepared_xact_procno(i: i32) -> ProcNumber {
    globals::max_backends::call() + NUM_AUXILIARY_PROCS + i
}

/// Index (`proctype`) of the first `AuxiliaryProcs[i]` with `pid == 0`, or
/// `None`. Caller holds `ProcStructLock`.
pub(crate) fn auxiliary_proc_find_free() -> Option<i32> {
    let base = globals::max_backends::call();
    with_proc_global(|pg| {
        (0..NUM_AUXILIARY_PROCS)
            .find(|&proctype| pg.allProcs[(base + proctype) as usize].pid == 0)
    })
}

// ---- lock-group membership over the arena ----

/// `dlist_push_head(&GetPGProcByNumber(leader)->lockGroupMembers,
/// &GetPGProcByNumber(member)->lockGroupLink)`.
pub(crate) fn lock_group_members_push_head(leader: ProcNumber, member: ProcNumber) {
    with_proc_by_number(leader, |p| p.lockGroupMembers.push_head(member));
}

/// `dlist_push_tail(&GetPGProcByNumber(leader)->lockGroupMembers,
/// &GetPGProcByNumber(member)->lockGroupLink)`.
pub(crate) fn lock_group_members_push_tail(leader: ProcNumber, member: ProcNumber) {
    with_proc_by_number(leader, |p| p.lockGroupMembers.push_tail(member));
}

/// A snapshot of `GetPGProcByNumber(leader)->lockGroupMembers` in list order.
pub(crate) fn lock_group_members_snapshot(leader: ProcNumber) -> Vec<ProcNumber> {
    with_proc_by_number(leader, |p| p.lockGroupMembers.members.iter().copied().collect())
}

/// `dlist_delete(&GetPGProcByNumber(member)->lockGroupLink)` — unlink `member`
/// from its leader's `lockGroupMembers` list. The leader is `member`'s own
/// `lockGroupLeader` (every member, including the leader itself, records it).
pub(crate) fn dlist_delete_lock_group_link(member: ProcNumber) {
    let leader = with_proc_by_number(member, |p| p.lockGroupLeader);
    if let Some(leader) = leader {
        with_proc_by_number(leader, |p| p.lockGroupMembers.remove(member));
    }
}

/// `InitProcGlobal(void)` — postmaster-time setup: build the `PGPROC` array,
/// the dense `ProcGlobal` mirror arrays, the embedded latches/semaphores/
/// fast-path arrays, and the four by-class freelists.
pub fn InitProcGlobal(_mcx: Mcx<'_>) -> PgResult<()> {
    let max_backends = globals::max_backends::call();
    let max_connections = globals::max_connections::call();
    let autovacuum_worker_slots = globals::autovacuum_worker_slots::call();
    let max_worker_processes = globals::max_worker_processes::call();
    let fp_groups = globals::fast_path_lock_groups_per_backend::call();

    let total_procs = (max_backends + NUM_AUXILIARY_PROCS + globals::max_prepared_xacts::call())
        as usize;

    // Create the ProcGlobal shared structure. (C: ShmemInitStruct("Proc
    // Header", ...) + Assert(!found). Here the crate owns the value; a second
    // call would mean the header already existed.)
    if proc_global_initialized() {
        return Err(types_error::PgError::error(
            "InitProcGlobal: \"Proc Header\" already existed",
        ));
    }

    let mut proc_global = PROC_HDR::new_zeroed();

    // Initialize the data structures.
    proc_global.spins_per_delay = DEFAULT_SPINS_PER_DELAY;
    // freeProcs/autovacFreeProcs/bgworkerFreeProcs/walsenderFreeProcs are
    // already dlist_init'd (empty) by PROC_HDR::new_zeroed.
    proc_global.startupBufferPinWaitBufId = -1;
    proc_global.walwriterProc = INVALID_PROC_NUMBER;
    proc_global.checkpointerProc = INVALID_PROC_NUMBER;
    // procArrayGroupFirst/clogGroupFirst are initialized to INVALID_PROC_NUMBER
    // by PROC_HDR::new_zeroed (pg_atomic_init_u32).

    // Create and initialize all the PGPROC structures we'll need. (C carves a
    // single MemSet(0) PGPROC block plus the dense mirror arrays from one
    // ShmemInitStruct; here the owned Vecs are the realization of that block.)
    let mut all_procs: Vec<PGPROC> = Vec::with_capacity(total_procs);

    // Per-backend fast-path array element counts (the C interleaves a uint64
    // fpLockBits group array and an Oid fpRelId slot array per backend).
    let fp_lock_bits_len = fp_groups as usize;
    let fp_rel_id_len = fast_path_lock_slots_per_backend() as usize;

    for i in 0..total_procs {
        let mut proc = PGPROC::new_zeroed();

        // Set the fast-path lock arrays. (C points fpLockBits/fpRelId into the
        // separately-allocated, interleaved "Fast-Path Lock Array" block; here
        // each PGPROC owns its own zeroed slice of the same dimensions.)
        proc.fpLockBits = vec![0u64; fp_lock_bits_len];
        proc.fpRelId = vec![0 as Oid; fp_rel_id_len];

        // Set up per-PGPROC semaphore, latch, and fpInfoLock. Prepared xact
        // dummy PGPROCs don't need these though - they're never associated
        // with a real process.
        if (i as i32) < max_backends + NUM_AUXILIARY_PROCS {
            // proc->sem = PGSemaphoreCreate();
            // PGSemaphoreCreate is owned by the not-yet-ported sysv_sema
            // subsystem; the semaphore is created lazily when the slot is
            // claimed (InitProcess), so InitProcGlobal leaves sem = None here,
            // exactly as the C dummy-vs-real split is recorded by the same
            // index test.
            //
            // InitSharedLatch(&proc->procLatch): mark the embedded latch as a
            // cleared, shared latch.
            proc.procLatch.is_set.store(0, core::sync::atomic::Ordering::SeqCst);
            proc.procLatch
                .maybe_sleeping
                .store(0, core::sync::atomic::Ordering::SeqCst);
            proc.procLatch.is_shared = true;
            proc.procLatch.owner_pid = 0;

            // LWLockInitialize(&proc->fpInfoLock, LWTRANCHE_LOCK_FASTPATH).
            lwlock::lwlock_initialize::call(&mut proc.fpInfoLock, LWTRANCHE_LOCK_FASTPATH);
        }

        // Newly created PGPROCs for normal backends, autovacuum workers,
        // special workers, bgworkers, and walsenders must be queued up on the
        // appropriate free list. Auxiliary processes use a linear search (no
        // free list); prepared-xact PGPROCs are added by TwoPhaseShmemInit().
        //
        // The freelist owning this PGPROC is recorded on the proc itself
        // (procgloballist); InitProcess pops by recomputing the same class. The
        // C does `dlist_push_tail(<freelist>, &proc->links); proc->procgloballist
        // = <freelist>;` for each slot — here the membership is threaded onto the
        // chosen head's index-ordered list and the head is named by FreeListId.
        let freelist = if (i as i32) < max_connections {
            // PGPROC for normal backend, add to freeProcs list.
            Some(FreeListId::Regular)
        } else if (i as i32)
            < max_connections + autovacuum_worker_slots + NUM_SPECIAL_WORKER_PROCS
        {
            // PGPROC for AV or special worker, add to autovacFreeProcs list.
            Some(FreeListId::Autovac)
        } else if (i as i32)
            < max_connections
                + autovacuum_worker_slots
                + NUM_SPECIAL_WORKER_PROCS
                + max_worker_processes
        {
            // PGPROC for bgworker, add to bgworkerFreeProcs list.
            Some(FreeListId::Bgworker)
        } else if (i as i32) < max_backends {
            // PGPROC for walsender, add to walsenderFreeProcs list.
            Some(FreeListId::Walsender)
        } else {
            // Auxiliary / prepared-xact dummy PGPROCs are not on a freelist.
            None
        };
        proc.procgloballist = freelist;
        if let Some(list) = freelist {
            let procno = i as ProcNumber;
            match list {
                FreeListId::Regular => proc_global.freeProcs.push_tail(procno),
                FreeListId::Autovac => proc_global.autovacFreeProcs.push_tail(procno),
                FreeListId::Bgworker => proc_global.bgworkerFreeProcs.push_tail(procno),
                FreeListId::Walsender => proc_global.walsenderFreeProcs.push_tail(procno),
            }
        }

        // Initialize myProcLocks[] shared memory queues. (Already dlist_init'd
        // empty by PGPROC::new_zeroed; assert the dimension matches the C
        // NUM_LOCK_PARTITIONS array.)
        debug_assert_eq!(proc.myProcLocks.len(), NUM_LOCK_PARTITIONS as usize);

        // lockGroupMembers, the atomics (procArrayGroupNext/clogGroupNext) and
        // waitStart are all initialized by PGPROC::new_zeroed
        // (dlist_init/pg_atomic_init_u32(INVALID_PROC_NUMBER==0 here is 0)/
        // pg_atomic_init_u64(0)). NB: proc->procArrayGroupNext/clogGroupNext
        // are initialized to INVALID_PROC_NUMBER in C; INVALID_PROC_NUMBER is
        // -1, so set them explicitly to match.
        proc.procArrayGroupNext =
            types_storage::storage::pg_atomic_uint32::new(INVALID_PROC_NUMBER as u32);
        proc.clogGroupNext =
            types_storage::storage::pg_atomic_uint32::new(INVALID_PROC_NUMBER as u32);

        // waitStatus is PROC_WAIT_STATUS_OK by new_zeroed.
        debug_assert_eq!(proc.waitStatus, PROC_WAIT_STATUS_OK);

        all_procs.push(proc);
    }

    // allProcs excludes prepared-xact dummies in allProcCount, but the array
    // itself spans every PGPROC (regular + aux + prepared) so AuxiliaryProcs /
    // PreparedXactProcs index into it.
    proc_global.allProcs = all_procs;
    // XXX allProcCount isn't really all of them; it excludes prepared xacts.
    proc_global.allProcCount = (max_backends + NUM_AUXILIARY_PROCS) as u32;

    // Allocate the dense ProcGlobal mirror arrays (xids/subxidStates/
    // statusFlags), one element per PGPROC, zeroed like the C MemSet.
    proc_global.xids = vec![0 as TransactionId; total_procs];
    proc_global.subxidStates = vec![XidCacheStatus::default(); total_procs];
    proc_global.statusFlags = vec![0u8; total_procs];

    // ProcStructLock spinlock (C: ShmemInitStruct + SpinLockInit). The proc
    // spinlock is owned by the not-yet-ported s_lock primitive and is acquired
    // through the shmem-lock seam when InitProcess pops a slot; nothing to
    // materialize here.

    PROC_GLOBAL.with(|cell| {
        *cell.borrow_mut() = Some(proc_global);
    });

    Ok(())
}

/// `ProcGlobal->allProcCount` — the total number of `PGPROC` slots in the
/// array. (Owner accessor for [`crate::proc_misc::ProcSendSignal`]'s range
/// check; `ProcGlobal` storage belongs to this module.)
pub(crate) fn all_proc_count() -> u32 {
    with_proc_global(|pg| pg.allProcCount)
}

/// `&ProcGlobal->allProcs[procNumber].procLatch` as a `LatchHandle` — the
/// process latch of the backend owning slot `procNumber`. (Owner accessor for
/// [`crate::proc_misc::ProcSendSignal`]'s `SetLatch`.)
pub(crate) fn proc_latch_handle(procNumber: ProcNumber) -> LatchHandle {
    // The latch unit identifies a per-PGPROC `procLatch` by the owning slot's
    // proc number (`storage/latch.h`: "C call sites that read `&proc->procLatch`
    // translate to an explicit `LatchHandle`, obtained from the caller's own
    // state"). The slot index is exactly that state; `+1` keeps `0` reserved as
    // the never-valid handle.
    LatchHandle::new(procNumber as usize + 1)
}
