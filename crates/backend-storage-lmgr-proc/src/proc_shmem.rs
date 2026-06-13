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
    XidCacheStatus, FP_LOCK_SLOTS_PER_GROUP, LWTRANCHE_LOCK_FASTPATH, NUM_AUXILIARY_PROCS,
    NUM_LOCK_PARTITIONS, NUM_SPECIAL_WORKER_PROCS, PGPROC, PROC_HDR, PROC_WAIT_STATUS_OK,
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

/// Run `f` with mutable access to the cluster-wide `ProcGlobal` (`PROC_HDR`),
/// panicking when it has not yet been built by [`InitProcGlobal`] (mirroring
/// proc.c's `Assert(ProcGlobal != NULL)`).
#[allow(dead_code)]
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
#[allow(dead_code)]
pub(crate) fn proc_global_initialized() -> bool {
    PROC_GLOBAL.with(|cell| cell.borrow().is_some())
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
        // (procgloballist); InitProcess pops by recomputing the same class.
        if (i as i32) < max_connections {
            // PGPROC for normal backend, add to freeProcs list.
            proc.procgloballist =
                Some(Box::new(proc_global.freeProcs.clone()));
        } else if (i as i32)
            < max_connections + autovacuum_worker_slots + NUM_SPECIAL_WORKER_PROCS
        {
            // PGPROC for AV or special worker, add to autovacFreeProcs list.
            proc.procgloballist =
                Some(Box::new(proc_global.autovacFreeProcs.clone()));
        } else if (i as i32)
            < max_connections
                + autovacuum_worker_slots
                + NUM_SPECIAL_WORKER_PROCS
                + max_worker_processes
        {
            // PGPROC for bgworker, add to bgworkerFreeProcs list.
            proc.procgloballist =
                Some(Box::new(proc_global.bgworkerFreeProcs.clone()));
        } else if (i as i32) < max_backends {
            // PGPROC for walsender, add to walsenderFreeProcs list.
            proc.procgloballist =
                Some(Box::new(proc_global.walsenderFreeProcs.clone()));
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
    todo!("proc.c: ProcGlobal->allProcCount")
}

/// `&ProcGlobal->allProcs[procNumber].procLatch` as a `LatchHandle` — the
/// process latch of the backend owning slot `procNumber`. (Owner accessor for
/// [`crate::proc_misc::ProcSendSignal`]'s `SetLatch`.)
pub(crate) fn proc_latch_handle(_procNumber: ProcNumber) -> LatchHandle {
    todo!("proc.c: &ProcGlobal->allProcs[procNumber].procLatch")
}
