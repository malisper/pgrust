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

use types_core::{Oid, ProcNumber, Size, TransactionId, INVALID_PROC_NUMBER};
use types_error::PgResult;
use types_storage::latch::LatchHandle;
use types_storage::storage::{
    FreeListId, XidCacheStatus, FP_LOCK_SLOTS_PER_GROUP, LWTRANCHE_LOCK_FASTPATH,
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

// ---- genuinely-shared per-PGPROC `pid` words + `ProcStructLock` ----
//
// The cluster-wide PGPROC array (`PROC_GLOBAL` above) is owned per-process and
// inherited copy-on-fork, which is sound for its read-mostly fields. But the
// `pid` word and the `ProcStructLock` spinlock are the cross-process
// coordination point of slot assignment: `InitAuxiliaryProcess` scans the
// AuxiliaryProcs[].pid words under ProcStructLock to find a free slot, and
// `ProcKill` zeroes `pid` to release it. If those words were per-process, every
// forked child would see the postmaster's image (all aux pids == 0) and claim
// the *same* free slot, colliding on a single ProcNumber. So — exactly as C
// does (`InitProcGlobal` ShmemInitStruct's the PGPROC block and the
// ProcStructLock into shared memory) — the `pid` words and the ProcStructLock
// live in a genuine shmem segment placed by the postmaster and inherited as a
// true shared mapping by every fork. The base pointers are process-globals (an
// `AtomicPtr`/`AtomicUsize`, set in `InitProcGlobal`, inherited across fork),
// mirroring the `BackendStatusArray` shmem idiom.

use core::sync::atomic::{AtomicPtr, AtomicUsize, Ordering as AtomicOrdering};

/// Base of the genuinely-shared `[i32; total_procs]` `pid` array (the canonical
/// `PGPROC.pid` words for slot coordination). Set by [`InitProcGlobal`], NULL
/// until then. C: part of the `PGPROC` block placed by `ShmemInitStruct`.
static SHARED_PROC_PIDS: AtomicPtr<i32> = AtomicPtr::new(core::ptr::null_mut());

/// `total_procs` — the length of [`SHARED_PROC_PIDS`], recorded for bounds
/// checks. Set alongside the array.
static SHARED_PROC_PID_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Stable base of the per-process `allProcs` `PGPROC` array (the same buffer
/// owned by `ProcGlobal->allProcs` in the [`PROC_GLOBAL`] `RefCell`), recorded
/// by [`InitProcGlobal`] once the array is built. NULL until then.
///
/// In C `&proc->procLatch` is plain pointer arithmetic into the stable
/// `ProcGlobal->allProcs` block: no lock, async-signal-safe, reachable from a
/// `SIGALRM` handler. The Rust port stores the `PGPROC` array inside the
/// [`PROC_GLOBAL`] `RefCell`, so reaching `&proc->procLatch` through
/// [`with_proc_global`] takes a `borrow_mut()` — which is *not* reentrant: a
/// timer (`SIGALRM`) firing while the main thread already holds that borrow
/// would make the handler's `SetLatch -> with_proc_latch` path re-borrow and
/// abort with "RefCell already borrowed". The latch fields are all atomics
/// (`Latch` in types-storage), and the array buffer is allocated once with the
/// final capacity and never reallocated after `InitProcGlobal`, so its address
/// is stable for the process lifetime. Recording that stable base here lets
/// [`with_proc_latch`] reach `&proc->procLatch` through a raw pointer with no
/// `RefCell` borrow — the faithful async-signal-safe `&proc->procLatch`.
static SHARED_ALL_PROCS: AtomicPtr<PGPROC> = AtomicPtr::new(core::ptr::null_mut());

/// Length of the [`SHARED_ALL_PROCS`] array (`total_procs`), for bounds checks.
static SHARED_ALL_PROCS_COUNT: AtomicUsize = AtomicUsize::new(0);

// ---- genuinely-shared per-PGPROC `procLatch` words ----
//
// `with_proc_latch` reaches `&proc->procLatch` to let `SetLatch`/`OwnLatch`/
// `DisownLatch` mutate the all-atomic `Latch` (`is_set`/`maybe_sleeping`/
// `owner_pid`). The C `procLatch` lives in the shared `PGPROC` block, and the
// whole point of `SetLatch` is CROSS-PROCESS wakeup: one backend sets another
// process' latch (`is_set` + signal the `owner_pid`). The `SHARED_ALL_PROCS`
// base above points into the per-process `PROC_GLOBAL` `allProcs` `Vec`, which
// is COW-inherited — fine for read-mostly fields and for async-signal-safety
// (no RefCell re-borrow), but a process-local copy: a write to
// `allProcs[n].procLatch` in one process is invisible to every other. That
// silently breaks every inter-process latch wakeup — most visibly the startup
// process' end-of-recovery `RequestCheckpoint`, whose `SetLatch` of the
// checkpointer's `procLatch` finds `owner_pid == 0` (the checkpointer `OwnLatch`'d
// its OWN private copy) and never signals, hanging recovery.
//
// So the `procLatch` words specifically live in a genuine shmem segment placed
// by the postmaster and inherited as a true shared mapping by every fork
// (exactly like the pid words / freelists / advertised aux procs). The
// PGPROC array's other fields stay process-local; only the latch — the one
// field designed for cross-process mutation — is promoted to real shmem.
static SHARED_PROC_LATCHES: AtomicPtr<types_storage::latch::Latch> =
    AtomicPtr::new(core::ptr::null_mut());

/// Length of [`SHARED_PROC_LATCHES`] (== total_procs), for bounds checks.
static SHARED_PROC_LATCH_COUNT: AtomicUsize = AtomicUsize::new(0);

// ---- genuinely-shared per-PGPROC `cvWaitLink` nodes ----
//
// A `ConditionVariable`'s wait queue is a `proclist` threaded through each
// waiter's `PGPROC.cvWaitLink`. `ConditionVariablePrepareToSleep` (in the
// waiter) pushes its own procno onto the CV's shared list head under the CV
// spinlock; `ConditionVariableBroadcast` (in another process) walks that list
// and `SetLatch`es each waiter. The list *head* lives in the shared
// ConditionVariable (`CheckpointerShmem`), but the per-PGPROC link nodes the
// walk follows must be shared too — otherwise the broadcaster resolves a
// waiter's `cvWaitLink` in its OWN process-local PGPROC copy (where the waiter
// never linked itself), so the traversal sees an inconsistent list and the two
// processes spin forever on the CV mutex. This is exactly what hangs the
// end-of-recovery `RequestCheckpoint`/`CheckpointerMain` CV handshake once the
// shared `procLatch` lets the initial wakeup through. So `cvWaitLink` lives in
// genuine shmem, like the latch words above.
static SHARED_CV_WAIT_LINKS: AtomicPtr<types_storage::proclist_node> =
    AtomicPtr::new(core::ptr::null_mut());

/// Length of [`SHARED_CV_WAIT_LINKS`] (== total_procs), for bounds checks.
static SHARED_CV_WAIT_LINK_COUNT: AtomicUsize = AtomicUsize::new(0);

/// `&proc->cvWaitLink` over the genuinely-shared array (read).
pub(crate) fn cv_wait_link_read(procno: ProcNumber) -> types_storage::proclist_node {
    let base = SHARED_CV_WAIT_LINKS.load(AtomicOrdering::Relaxed);
    let count = SHARED_CV_WAIT_LINK_COUNT.load(AtomicOrdering::Relaxed);
    assert!(
        !base.is_null(),
        "cv_wait_link_read: cvWaitLink base uninitialized (InitProcGlobal not run)"
    );
    let idx = procno as usize;
    assert!(idx < count, "cvWaitLink index {idx} out of range (count {count})");
    // SAFETY: `base` addresses `count` `proclist_node`s of genuine shared
    // memory; `idx < count`. Read under the CV spinlock (caller-held), mirroring
    // C's plain read of `proc->cvWaitLink`.
    unsafe { core::ptr::read(base.add(idx)) }
}

/// `proc->cvWaitLink = node` over the genuinely-shared array (write).
pub(crate) fn cv_wait_link_write(procno: ProcNumber, node: types_storage::proclist_node) {
    let base = SHARED_CV_WAIT_LINKS.load(AtomicOrdering::Relaxed);
    let count = SHARED_CV_WAIT_LINK_COUNT.load(AtomicOrdering::Relaxed);
    assert!(
        !base.is_null(),
        "cv_wait_link_write: cvWaitLink base uninitialized (InitProcGlobal not run)"
    );
    let idx = procno as usize;
    assert!(idx < count, "cvWaitLink index {idx} out of range (count {count})");
    // SAFETY: see `cv_wait_link_read`; written under the CV spinlock.
    unsafe { core::ptr::write(base.add(idx), node) };
}

// ---- genuinely-shared per-PGPROC `lwWaitLink` / `lwWaiting` / `lwWaitMode` ----
//
// An LWLock's wait queue is a `proclist` threaded through each waiter's
// `PGPROC.lwWaitLink`, with the per-waiter `lwWaiting`/`lwWaitMode` bytes. This
// is a cross-process structure exactly like `cvWaitLink` above: a process
// releasing an LWLock (`LWLockWakeup`/`LWLockUpdateVar`) walks `lock->waiters`
// (shared) and reads+writes the `lwWaiting`/`lwWaitLink` of waiters owned by
// OTHER backends. If those per-PGPROC fields stayed in the COW-inherited,
// process-local `PROC_GLOBAL`, the releaser would resolve a waiter's
// `lwWaitLink`/`lwWaiting` in its own private copy (where the waiter never
// queued itself), so the traversal sees an inconsistent list and a proc whose
// shared list-membership says "queued" has a process-local `lwWaiting` that is
// not `LW_WS_WAITING` — firing the `LWLockWakeup` assertion (and then a stuck
// wait-list spinlock as the panic unwinds with `LW_FLAG_LOCKED` held). So these
// three fields live in genuine shmem, mirroring `cvWaitLink`. `lwWaiting` and
// `lwWaitMode` are single bytes (`LWLockWaitState` / `LWLockMode` discriminants).
static SHARED_LW_WAIT_LINKS: AtomicPtr<types_storage::proclist_node> =
    AtomicPtr::new(core::ptr::null_mut());
/// Length of [`SHARED_LW_WAIT_LINKS`] (== total_procs), for bounds checks.
static SHARED_LW_WAIT_LINK_COUNT: AtomicUsize = AtomicUsize::new(0);
/// Base of the genuinely-shared `[u8; total_procs]` `lwWaiting` words.
static SHARED_LW_WAITING: AtomicPtr<u8> = AtomicPtr::new(core::ptr::null_mut());
/// Base of the genuinely-shared `[u8; total_procs]` `lwWaitMode` words.
static SHARED_LW_WAIT_MODE: AtomicPtr<u8> = AtomicPtr::new(core::ptr::null_mut());

/// `&proc->lwWaitLink` over the genuinely-shared array (read).
pub(crate) fn lw_wait_link_read(procno: ProcNumber) -> types_storage::proclist_node {
    let base = SHARED_LW_WAIT_LINKS.load(AtomicOrdering::Relaxed);
    let count = SHARED_LW_WAIT_LINK_COUNT.load(AtomicOrdering::Relaxed);
    assert!(
        !base.is_null(),
        "lw_wait_link_read: lwWaitLink base uninitialized (InitProcGlobal not run)"
    );
    let idx = procno as usize;
    assert!(idx < count, "lwWaitLink index {idx} out of range (count {count})");
    // SAFETY: in-range slot of the shared array; read under the wait-list
    // spinlock (`LW_FLAG_LOCKED`), mirroring C's read of `proc->lwWaitLink`.
    unsafe { core::ptr::read(base.add(idx)) }
}

/// `proc->lwWaitLink = node` over the genuinely-shared array (write).
pub(crate) fn lw_wait_link_write(procno: ProcNumber, node: types_storage::proclist_node) {
    let base = SHARED_LW_WAIT_LINKS.load(AtomicOrdering::Relaxed);
    let count = SHARED_LW_WAIT_LINK_COUNT.load(AtomicOrdering::Relaxed);
    assert!(
        !base.is_null(),
        "lw_wait_link_write: lwWaitLink base uninitialized (InitProcGlobal not run)"
    );
    let idx = procno as usize;
    assert!(idx < count, "lwWaitLink index {idx} out of range (count {count})");
    // SAFETY: see `lw_wait_link_read`; written under the wait-list spinlock.
    unsafe { core::ptr::write(base.add(idx), node) };
}

/// `&proc->lwWaiting` over the genuinely-shared array (read).
pub(crate) fn lw_waiting_read(procno: ProcNumber) -> u8 {
    let base = SHARED_LW_WAITING.load(AtomicOrdering::Relaxed);
    let count = SHARED_LW_WAIT_LINK_COUNT.load(AtomicOrdering::Relaxed);
    assert!(
        !base.is_null(),
        "lw_waiting_read: lwWaiting base uninitialized (InitProcGlobal not run)"
    );
    let idx = procno as usize;
    assert!(idx < count, "lwWaiting index {idx} out of range (count {count})");
    // SAFETY: in-range slot of the shared array.
    unsafe { core::ptr::read(base.add(idx)) }
}

/// `proc->lwWaiting = v` over the genuinely-shared array (write).
pub(crate) fn lw_waiting_write(procno: ProcNumber, v: u8) {
    let base = SHARED_LW_WAITING.load(AtomicOrdering::Relaxed);
    let count = SHARED_LW_WAIT_LINK_COUNT.load(AtomicOrdering::Relaxed);
    assert!(
        !base.is_null(),
        "lw_waiting_write: lwWaiting base uninitialized (InitProcGlobal not run)"
    );
    let idx = procno as usize;
    assert!(idx < count, "lwWaiting index {idx} out of range (count {count})");
    // SAFETY: see `lw_waiting_read`.
    unsafe { core::ptr::write(base.add(idx), v) };
}

/// `&proc->lwWaitMode` over the genuinely-shared array (read).
pub(crate) fn lw_wait_mode_read(procno: ProcNumber) -> u8 {
    let base = SHARED_LW_WAIT_MODE.load(AtomicOrdering::Relaxed);
    let count = SHARED_LW_WAIT_LINK_COUNT.load(AtomicOrdering::Relaxed);
    assert!(
        !base.is_null(),
        "lw_wait_mode_read: lwWaitMode base uninitialized (InitProcGlobal not run)"
    );
    let idx = procno as usize;
    assert!(idx < count, "lwWaitMode index {idx} out of range (count {count})");
    // SAFETY: in-range slot of the shared array.
    unsafe { core::ptr::read(base.add(idx)) }
}

/// `proc->lwWaitMode = v` over the genuinely-shared array (write).
pub(crate) fn lw_wait_mode_write(procno: ProcNumber, v: u8) {
    let base = SHARED_LW_WAIT_MODE.load(AtomicOrdering::Relaxed);
    let count = SHARED_LW_WAIT_LINK_COUNT.load(AtomicOrdering::Relaxed);
    assert!(
        !base.is_null(),
        "lw_wait_mode_write: lwWaitMode base uninitialized (InitProcGlobal not run)"
    );
    let idx = procno as usize;
    assert!(idx < count, "lwWaitMode index {idx} out of range (count {count})");
    // SAFETY: see `lw_wait_mode_read`.
    unsafe { core::ptr::write(base.add(idx), v) };
}

/// Pointer to the genuinely-shared `ProcStructLock` spinlock word. Set by
/// [`InitProcGlobal`], NULL until then. C: `slock_t *ProcStructLock` placed by
/// `ShmemInitStruct`.
static SHARED_PROC_STRUCT_LOCK: AtomicPtr<types_storage::storage::Spinlock> =
    AtomicPtr::new(core::ptr::null_mut());

// ---- genuinely-shared proc freelists (the four `PROC_HDR` dlist heads + the
//      per-PGPROC `links` that thread them) ----
//
// In C the four freelists (`freeProcs`/`autovacFreeProcs`/`bgworkerFreeProcs`/
// `walsenderFreeProcs`) are `dlist_head`s in the shared `PROC_HDR`, threaded
// through each `PGPROC.links` — both live in the genuine shared PGPROC block
// that `InitProcGlobal` ShmemInitStruct's. `InitProcess` pops the head under
// `ProcStructLock`; `ProcKill` pushes head/tail under `ProcStructLock`. If
// these heads/links were process-local (COW-inherited), every forked backend
// would pop the SAME ProcNumber and collide on the genuinely-shared sinval slot
// array. So — exactly like the pid words / ProcStructLock above — the freelist
// heads and the `links` array live in a real shmem segment placed by the
// postmaster and inherited as a true shared mapping by every fork.
//
// Realization of the intrusive dlist over the index-addressed arena: a shared
// `[FreeLink; total_procs]` (next/prev ProcNumber, -1 == none) is the per-PGPROC
// `links`; a shared `[ListHead; 4]` (head/tail ProcNumber, -1 == empty) is the
// four `dlist_head`s. All access is under `ProcStructLock` (held by every caller
// of pop/push), so plain reads/writes mirror C's plain `dlist` mutation.

/// `INVALID_PROC_NUMBER` as the i32 sentinel for an absent list link / empty
/// head — matches C's empty-`dlist` (`head == NULL`) and detached-node state.
const FREE_LINK_NIL: i32 = -1;

// ---- genuinely-shared advertised auxiliary proc numbers ----
//
// `ProcGlobal->checkpointerProc` / `ProcGlobal->walwriterProc` are the slot
// numbers the checkpointer / WAL writer advertise about THEMSELVES at startup
// (`set_checkpointer_proc_to_self` / `set_walwriter_proc_to_self`), so that an
// *unrelated* process (a backend, or the startup process during the
// end-of-recovery `RequestCheckpoint`) can resolve them and `SetLatch` the
// aux process to wake it. In C these words live in the shared `PROC_HDR`.
//
// They MUST be genuinely shared, not part of the COW-inherited `PROC_GLOBAL`
// value: the checkpointer writes `checkpointerProc` in its OWN process after
// being forked, so a process-local copy would never propagate to the reader.
// During crash recovery the startup process is forked from the postmaster's
// image (where `checkpointerProc == INVALID_PROC_NUMBER`, the checkpointer not
// having advertised yet) and would spin in `RequestCheckpoint`'s
// retry-then-error loop forever, never completing the end-of-recovery
// checkpoint handshake. So — exactly like the pid words / freelists above —
// these advertised slot numbers live in a real shmem segment placed by the
// postmaster and inherited as a true shared mapping by every fork.

/// Index into [`SHARED_AUX_PROCS`] for each advertised auxiliary proc number.
#[derive(Clone, Copy)]
enum AuxProcSlot {
    Checkpointer = 0,
    WalWriter = 1,
}

/// Number of advertised auxiliary proc-number words.
const NUM_AUX_PROC_SLOTS: usize = 2;

/// Base of the genuinely-shared `[ProcNumber; NUM_AUX_PROC_SLOTS]` array holding
/// the advertised auxiliary proc numbers. Set by [`InitProcGlobal`], NULL until
/// then. C: the `checkpointerProc` / `walwriterProc` words of the shared
/// `PROC_HDR`.
static SHARED_AUX_PROCS: AtomicPtr<ProcNumber> = AtomicPtr::new(core::ptr::null_mut());

/// `ProcGlobal->{checkpointer,walwriter}Proc` read over the genuinely-shared
/// array. Returns `INVALID_PROC_NUMBER` if the block is not yet initialized
/// (mirrors the pre-`InitProcGlobal` default).
fn aux_proc_read(slot: AuxProcSlot) -> ProcNumber {
    let base = SHARED_AUX_PROCS.load(AtomicOrdering::Relaxed);
    if base.is_null() {
        return INVALID_PROC_NUMBER;
    }
    // SAFETY: `base` addresses `NUM_AUX_PROC_SLOTS` `ProcNumber`s of genuine
    // shared memory; `slot as usize` is in range by construction. A single
    // aligned word read mirrors C's plain read of the `PROC_HDR` field.
    unsafe { core::ptr::read(base.add(slot as usize)) }
}

/// `ProcGlobal->{checkpointer,walwriter}Proc = value` over the genuinely-shared
/// array. No-op (with a debug assert) if the block is not yet initialized.
fn aux_proc_write(slot: AuxProcSlot, value: ProcNumber) {
    let base = SHARED_AUX_PROCS.load(AtomicOrdering::Relaxed);
    debug_assert!(
        !base.is_null(),
        "advertised aux proc block uninitialized (InitProcGlobal not run)"
    );
    if base.is_null() {
        return;
    }
    // SAFETY: see `aux_proc_read`; a single aligned word write mirrors C's plain
    // write of the `PROC_HDR` field.
    unsafe { core::ptr::write(base.add(slot as usize), value) };
}

/// `ProcGlobal->checkpointerProc` (genuinely shared).
pub(crate) fn checkpointer_proc_read() -> ProcNumber {
    aux_proc_read(AuxProcSlot::Checkpointer)
}

/// `ProcGlobal->checkpointerProc = value` (genuinely shared).
pub(crate) fn checkpointer_proc_write(value: ProcNumber) {
    aux_proc_write(AuxProcSlot::Checkpointer, value);
}

/// `ProcGlobal->walwriterProc` (genuinely shared). The reader is the walwriter
/// latch-wakeup in xlog's `XLogSetAsyncXactLSN`, currently a no-op stub
/// (`wake_walwriter`); kept wired so the cross-process word is correct the
/// moment that wakeup is enabled, mirroring `checkpointer_proc_read`.
#[allow(dead_code)]
pub(crate) fn walwriter_proc_read() -> ProcNumber {
    aux_proc_read(AuxProcSlot::WalWriter)
}

/// `ProcGlobal->walwriterProc = value` (genuinely shared).
pub(crate) fn walwriter_proc_write(value: ProcNumber) {
    aux_proc_write(AuxProcSlot::WalWriter, value);
}

/// Per-PGPROC `links` realization: the `next`/`prev` ProcNumber of an intrusive
/// freelist node. `repr(C)` so the in-shmem layout is fixed and plain-int
/// accessible (read/written only under `ProcStructLock`).
#[repr(C)]
#[derive(Clone, Copy)]
struct FreeLink {
    next: i32,
    prev: i32,
}

/// One `dlist_head` realization: head + tail ProcNumber (`-1` == empty).
#[repr(C)]
#[derive(Clone, Copy)]
struct ListHead {
    head: i32,
    tail: i32,
}

/// Base of the genuinely-shared `[FreeLink; total_procs]` array (each PGPROC's
/// `links`). Set by [`InitProcGlobal`], NULL until then.
static SHARED_FREE_LINKS: AtomicPtr<FreeLink> = AtomicPtr::new(core::ptr::null_mut());

/// Length of [`SHARED_FREE_LINKS`] (== total_procs), for bounds checks.
static SHARED_FREE_LINK_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Base of the genuinely-shared `[ListHead; 4]` array (the four freelist
/// `dlist_head`s, indexed by [`FreeListId`] as `usize`). Set by
/// [`InitProcGlobal`], NULL until then.
static SHARED_FREE_HEADS: AtomicPtr<ListHead> = AtomicPtr::new(core::ptr::null_mut());

/// Number of freelist heads (= variants of [`FreeListId`]).
const NUM_FREELISTS: usize = 4;

#[inline]
fn freelist_index(list: FreeListId) -> usize {
    match list {
        FreeListId::Regular => 0,
        FreeListId::Autovac => 1,
        FreeListId::Bgworker => 2,
        FreeListId::Walsender => 3,
    }
}

/// `&proc->links` over the genuinely-shared `links` array. Caller holds
/// `ProcStructLock`. Panics if uninitialized / out of range (caller bug).
fn shared_link(procno: ProcNumber) -> &'static mut FreeLink {
    let base = SHARED_FREE_LINKS.load(AtomicOrdering::Relaxed);
    assert!(
        !base.is_null(),
        "shared PGPROC links array uninitialized (InitProcGlobal not run)"
    );
    let count = SHARED_FREE_LINK_COUNT.load(AtomicOrdering::Relaxed);
    let idx = procno as usize;
    assert!(idx < count, "PGPROC links index {idx} out of range (count {count})");
    // SAFETY: `base` addresses `count` `FreeLink`s of genuine shared memory and
    // `idx < count`. The `&mut` is sound under the single-writer discipline of
    // `ProcStructLock` (every freelist mutator holds it), mirroring C's plain
    // pointer write to `proc->links` inside the spinlock.
    unsafe { &mut *base.add(idx) }
}

/// `&ProcGlobal-><list>` head/tail over the genuinely-shared heads array.
/// Caller holds `ProcStructLock`.
fn shared_head(list: FreeListId) -> &'static mut ListHead {
    let base = SHARED_FREE_HEADS.load(AtomicOrdering::Relaxed);
    assert!(
        !base.is_null(),
        "shared freelist heads uninitialized (InitProcGlobal not run)"
    );
    // SAFETY: `base` addresses `NUM_FREELISTS` `ListHead`s of genuine shared
    // memory; the index is in range by construction. `&mut` is sound under
    // `ProcStructLock` (single writer), mirroring C's `dlist_head` mutation.
    unsafe { &mut *base.add(freelist_index(list)) }
}

/// Place the genuinely-shared freelist `links` array and the four `dlist_head`s
/// into real shared memory, zero-initialized to "all empty / all detached".
/// Idempotent across `found` (EXEC_BACKEND re-attach): existing contents are
/// kept. C: part of the PGPROC `ShmemInitStruct` block (the `links` field) and
/// the `dlist_head`s in the `PROC_HDR` `ShmemInitStruct`.
fn init_shared_freelists(total_procs: usize) -> PgResult<()> {
    // links array
    let links_size = mul_size(total_procs, size_of::<FreeLink>());
    let (links_ptr, links_found) = shmem::shmem_init_struct::call("PGPROC freelist links", links_size)?;
    let links_ptr = links_ptr as *mut FreeLink;
    if !links_found {
        // dlist_node_init on every links: next == prev == NIL (detached).
        for i in 0..total_procs {
            // SAFETY: `links_ptr` addresses `total_procs` writable `FreeLink`s.
            unsafe {
                *links_ptr.add(i) = FreeLink {
                    next: FREE_LINK_NIL,
                    prev: FREE_LINK_NIL,
                };
            }
        }
    }
    SHARED_FREE_LINKS.store(links_ptr, AtomicOrdering::Relaxed);
    SHARED_FREE_LINK_COUNT.store(total_procs, AtomicOrdering::Relaxed);

    // four dlist heads
    let heads_size = mul_size(NUM_FREELISTS, size_of::<ListHead>());
    let (heads_ptr, heads_found) = shmem::shmem_init_struct::call("PROC_HDR freelist heads", heads_size)?;
    let heads_ptr = heads_ptr as *mut ListHead;
    if !heads_found {
        // dlist_init on each head: empty (head == tail == NIL).
        for i in 0..NUM_FREELISTS {
            // SAFETY: `heads_ptr` addresses `NUM_FREELISTS` writable `ListHead`s.
            unsafe {
                *heads_ptr.add(i) = ListHead {
                    head: FREE_LINK_NIL,
                    tail: FREE_LINK_NIL,
                };
            }
        }
    }
    SHARED_FREE_HEADS.store(heads_ptr, AtomicOrdering::Relaxed);

    Ok(())
}

/// `dlist_is_empty(<list>)`.
fn shared_freelist_is_empty(list: FreeListId) -> bool {
    shared_head(list).head == FREE_LINK_NIL
}

/// `dlist_pop_head_node(<list>)` -> `dlist_container(PGPROC, links, …)`: detach
/// and return the head ProcNumber, or `None` if empty. Caller holds
/// `ProcStructLock`.
fn shared_freelist_pop_head(list: FreeListId) -> Option<ProcNumber> {
    let head_node = {
        let h = shared_head(list);
        if h.head == FREE_LINK_NIL {
            return None;
        }
        h.head
    };
    let next = shared_link(head_node).next;
    {
        let h = shared_head(list);
        h.head = next;
        if next == FREE_LINK_NIL {
            // list became empty
            h.tail = FREE_LINK_NIL;
        }
    }
    if next != FREE_LINK_NIL {
        shared_link(next).prev = FREE_LINK_NIL;
    }
    // dlist_node_init(&popped->links): leave it detached.
    let l = shared_link(head_node);
    l.next = FREE_LINK_NIL;
    l.prev = FREE_LINK_NIL;
    Some(head_node)
}

/// `dlist_push_head(<list>, &proc->links)`. Caller holds `ProcStructLock`.
fn shared_freelist_push_head(list: FreeListId, procno: ProcNumber) {
    let old_head = shared_head(list).head;
    {
        let l = shared_link(procno);
        l.prev = FREE_LINK_NIL;
        l.next = old_head;
    }
    if old_head != FREE_LINK_NIL {
        shared_link(old_head).prev = procno;
    }
    let h = shared_head(list);
    h.head = procno;
    if h.tail == FREE_LINK_NIL {
        h.tail = procno;
    }
}

/// `dlist_push_tail(<list>, &proc->links)`. Caller holds `ProcStructLock`.
fn shared_freelist_push_tail(list: FreeListId, procno: ProcNumber) {
    let old_tail = shared_head(list).tail;
    {
        let l = shared_link(procno);
        l.next = FREE_LINK_NIL;
        l.prev = old_tail;
    }
    if old_tail != FREE_LINK_NIL {
        shared_link(old_tail).next = procno;
    }
    let h = shared_head(list);
    h.tail = procno;
    if h.head == FREE_LINK_NIL {
        h.head = procno;
    }
}

/// Snapshot of `<list>` in head→tail order (for `HaveNFreeProcs`'s
/// `dlist_foreach`). Caller holds `ProcStructLock`, so the list is stable.
fn shared_freelist_snapshot(list: FreeListId) -> Vec<ProcNumber> {
    let mut out = Vec::new();
    let mut cur = shared_head(list).head;
    while cur != FREE_LINK_NIL {
        out.push(cur);
        cur = shared_link(cur).next;
    }
    out
}

/// `&ProcGlobal->allProcs[procno].pid` over the genuinely-shared pid array.
/// Panics if `InitProcGlobal` has not run or `procno` is out of range — both
/// caller bugs mirroring the C deref of a slot in the shared PGPROC block.
fn shared_pid_slot(procno: ProcNumber) -> &'static AtomicI32 {
    let base = SHARED_PROC_PIDS.load(AtomicOrdering::Relaxed);
    assert!(
        !base.is_null(),
        "shared PGPROC pid array uninitialized (InitProcGlobal not run)"
    );
    let count = SHARED_PROC_PID_COUNT.load(AtomicOrdering::Relaxed);
    let idx = procno as usize;
    assert!(idx < count, "PGPROC pid index {idx} out of range (count {count})");
    // SAFETY: `base` addresses `count` `i32` words of genuine shared memory and
    // `idx < count`. `i32` and `AtomicI32` share layout (`#[repr(transparent)]`
    // / same size+align), so the word may be accessed atomically — the
    // cross-process discipline mirrors C's plain `pid` int read/written under
    // ProcStructLock, with atomics making the per-word access well-defined under
    // the Rust memory model.
    unsafe { AtomicI32::from_ptr(base.add(idx)) }
}

use core::sync::atomic::AtomicI32;

/// Place the genuinely-shared `pid` array (`[i32; total_procs]`, zeroed) and the
/// `ProcStructLock` spinlock word into real shared memory, recording their base
/// pointers in the process-globals. Idempotent across `found` (EXEC_BACKEND
/// re-attach): the array/lock keep their existing contents when the segment
/// already exists. C: the `pid` words are part of the PGPROC `ShmemInitStruct`
/// block and the lock is its own `ShmemInitStruct` + `SpinLockInit`.
fn init_shared_pid_block(total_procs: usize) -> PgResult<()> {
    // pid array
    let pid_size = mul_size(total_procs, size_of::<i32>());
    let (pid_ptr, pid_found) = shmem::shmem_init_struct::call("PGPROC pid words", pid_size)?;
    let pid_ptr = pid_ptr as *mut i32;
    if !pid_found {
        // MemSet(0): no process has claimed any slot yet.
        // SAFETY: `pid_ptr` addresses `pid_size` writable shmem bytes.
        unsafe { core::ptr::write_bytes(pid_ptr as *mut u8, 0, pid_size) };
    }
    SHARED_PROC_PIDS.store(pid_ptr, AtomicOrdering::Relaxed);
    SHARED_PROC_PID_COUNT.store(total_procs, AtomicOrdering::Relaxed);

    // ProcStructLock spinlock word
    let lock_size = size_of::<types_storage::storage::Spinlock>();
    let (lock_ptr, lock_found) = shmem::shmem_init_struct::call("ProcStructLock", lock_size)?;
    let lock_ptr = lock_ptr as *mut types_storage::storage::Spinlock;
    if !lock_found {
        // SpinLockInit(ProcStructLock): store the free (zero) word.
        // SAFETY: `lock_ptr` addresses a writable `Spinlock` word in shmem.
        unsafe { (*lock_ptr).unlock() };
    }
    SHARED_PROC_STRUCT_LOCK.store(lock_ptr, AtomicOrdering::Relaxed);

    // Advertised auxiliary proc numbers (checkpointerProc / walwriterProc).
    let aux_size = mul_size(NUM_AUX_PROC_SLOTS, size_of::<ProcNumber>());
    let (aux_ptr, aux_found) =
        shmem::shmem_init_struct::call("PROC_HDR advertised aux procs", aux_size)?;
    let aux_ptr = aux_ptr as *mut ProcNumber;
    if !aux_found {
        // No aux process has advertised yet: INVALID_PROC_NUMBER, matching the
        // `PROC_HDR` field defaults (`InitProcGlobal` sets both to
        // INVALID_PROC_NUMBER).
        // SAFETY: `aux_ptr` addresses `NUM_AUX_PROC_SLOTS` writable `ProcNumber`
        // words of shmem.
        for i in 0..NUM_AUX_PROC_SLOTS {
            unsafe { core::ptr::write(aux_ptr.add(i), INVALID_PROC_NUMBER) };
        }
    }
    SHARED_AUX_PROCS.store(aux_ptr, AtomicOrdering::Relaxed);

    // Per-PGPROC `procLatch` words (genuinely shared for cross-process wakeup).
    let latch_size = mul_size(total_procs, size_of::<types_storage::latch::Latch>());
    let (latch_ptr, latch_found) =
        shmem::shmem_init_struct::call("PGPROC procLatch words", latch_size)?;
    let latch_ptr = latch_ptr as *mut types_storage::latch::Latch;
    if !latch_found {
        // Zero the block (C's MemSet of the PGPROC array), then `InitSharedLatch`
        // each one: cleared (is_set=0, maybe_sleeping=0, owner_pid=0) and marked
        // shared (is_shared=true). `OwnLatch` later stamps owner_pid.
        // SAFETY: `latch_ptr` addresses `latch_size` writable shmem bytes; the
        // all-atomic `Latch` is sound to zero-initialize then field-write.
        unsafe { core::ptr::write_bytes(latch_ptr as *mut u8, 0, latch_size) };
        for i in 0..total_procs {
            // SAFETY: in-range slot of the shared latch array.
            let l = unsafe { &*latch_ptr.add(i) };
            l.is_shared.store(true, core::sync::atomic::Ordering::SeqCst);
        }
    }
    SHARED_PROC_LATCHES.store(latch_ptr, AtomicOrdering::Relaxed);
    SHARED_PROC_LATCH_COUNT.store(total_procs, AtomicOrdering::Relaxed);

    // Per-PGPROC `cvWaitLink` nodes (genuinely shared so a CV broadcast in one
    // process walks the same wait queue the waiter linked itself onto).
    let cv_size = mul_size(total_procs, size_of::<types_storage::proclist_node>());
    let (cv_ptr, cv_found) =
        shmem::shmem_init_struct::call("PGPROC cvWaitLink nodes", cv_size)?;
    let cv_ptr = cv_ptr as *mut types_storage::proclist_node;
    if !cv_found {
        // Zero (`proclist_node { next: 0, prev: 0 }`) — not linked into any
        // queue, matching C's MemSet of the PGPROC block.
        // SAFETY: `cv_ptr` addresses `cv_size` writable shmem bytes.
        unsafe { core::ptr::write_bytes(cv_ptr as *mut u8, 0, cv_size) };
    }
    SHARED_CV_WAIT_LINKS.store(cv_ptr, AtomicOrdering::Relaxed);
    SHARED_CV_WAIT_LINK_COUNT.store(total_procs, AtomicOrdering::Relaxed);

    // Per-PGPROC `lwWaitLink` nodes + `lwWaiting`/`lwWaitMode` bytes (genuinely
    // shared so an LWLock release in one process walks the same wait queue the
    // waiter linked itself onto, and reads the waiter's true wait state).
    let lw_size = mul_size(total_procs, size_of::<types_storage::proclist_node>());
    let (lw_ptr, lw_found) =
        shmem::shmem_init_struct::call("PGPROC lwWaitLink nodes", lw_size)?;
    let lw_ptr = lw_ptr as *mut types_storage::proclist_node;
    if !lw_found {
        // Zero (`proclist_node { next: 0, prev: 0 }`), matching C's MemSet.
        // SAFETY: `lw_ptr` addresses `lw_size` writable shmem bytes.
        unsafe { core::ptr::write_bytes(lw_ptr as *mut u8, 0, lw_size) };
    }
    SHARED_LW_WAIT_LINKS.store(lw_ptr, AtomicOrdering::Relaxed);
    SHARED_LW_WAIT_LINK_COUNT.store(total_procs, AtomicOrdering::Relaxed);

    let lww_size = mul_size(total_procs, size_of::<u8>());
    let (lww_ptr, lww_found) =
        shmem::shmem_init_struct::call("PGPROC lwWaiting words", lww_size)?;
    let lww_ptr = lww_ptr as *mut u8;
    if !lww_found {
        // Zero == `LW_WS_NOT_WAITING` (discriminant 0).
        // SAFETY: `lww_ptr` addresses `lww_size` writable shmem bytes.
        unsafe { core::ptr::write_bytes(lww_ptr, 0, lww_size) };
    }
    SHARED_LW_WAITING.store(lww_ptr, AtomicOrdering::Relaxed);

    let lwm_size = mul_size(total_procs, size_of::<u8>());
    let (lwm_ptr, lwm_found) =
        shmem::shmem_init_struct::call("PGPROC lwWaitMode words", lwm_size)?;
    let lwm_ptr = lwm_ptr as *mut u8;
    if !lwm_found {
        // Zero == `LW_EXCLUSIVE` (discriminant 0); reset whenever a proc queues.
        // SAFETY: `lwm_ptr` addresses `lwm_size` writable shmem bytes.
        unsafe { core::ptr::write_bytes(lwm_ptr, 0, lwm_size) };
    }
    SHARED_LW_WAIT_MODE.store(lwm_ptr, AtomicOrdering::Relaxed);

    Ok(())
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
    let lock = proc_struct_lock();
    // SpinLockAcquire: TAS_SPIN; on failure, s_lock() the backoff loop.
    if lock.tas_spin() != 0 {
        backend_storage_lmgr_s_lock::s_lock(lock, Some(file!()), line!() as i32, None);
    }
}

/// `SpinLockRelease(ProcStructLock)` — fence-ordered store of zero.
pub(crate) fn spin_lock_release_proc_struct_lock() {
    proc_struct_lock().unlock();
}

/// `ProcStructLock` — the genuinely-shared spinlock word placed by
/// [`InitProcGlobal`]. Panics if it has not run (caller bug, mirroring the C
/// deref of the `ProcStructLock` pointer before it is set).
fn proc_struct_lock() -> &'static types_storage::storage::Spinlock {
    let p = SHARED_PROC_STRUCT_LOCK.load(AtomicOrdering::Relaxed);
    assert!(
        !p.is_null(),
        "ProcStructLock uninitialized (InitProcGlobal not run)"
    );
    // SAFETY: `p` addresses a `Spinlock` word (`#[repr(transparent)]` over
    // `AtomicI32`) in genuine shared memory, placed and `SpinLockInit`'d by
    // InitProcGlobal in the postmaster, valid for the process lifetime.
    unsafe { &*p }
}

// ---- per-backend MyProc / MyProcNumber / MyProcPid (proc.c backend-locals) ----

/// `MyProc != NULL`.
pub(crate) fn my_proc_is_set() -> bool {
    MY_PROC_NUMBER.with(|c| c.borrow().is_some())
}

/// `MyProc = GetPGProcByNumber(procno); MyProcNumber = procno`. Sets both the
/// owner-private `MyProc != NULL` flag and the globals.c `MyProcNumber` global
/// (via the init-small owner), mirroring proc.c's
/// `MyProcNumber = GetNumberFromPGProc(MyProc);`.
pub(crate) fn set_my_proc_number(procno: ProcNumber) {
    MY_PROC_NUMBER.with(|c| *c.borrow_mut() = Some(procno));
    backend_utils_init_small_seams::set_my_proc_number::call(procno);
}

/// `MyProc = NULL` / `MyProcNumber = INVALID_PROC_NUMBER`.
pub(crate) fn clear_my_proc() {
    MY_PROC_NUMBER.with(|c| *c.borrow_mut() = None);
    backend_utils_init_small_seams::set_my_proc_number::call(INVALID_PROC_NUMBER);
}

/// `MyProcNumber` (globals.c) — the pgprocno of the current backend, or
/// `INVALID_PROC_NUMBER` when no `PGPROC` is attached. This reads the plain
/// global and is tolerant of `MyProc == NULL` (pre-`InitProcess`), exactly like
/// C: it does NOT deref `MyProc`. Code that genuinely needs a live `PGPROC`
/// uses [`my_proc_is_set`] / [`with_my_proc`] instead.
pub(crate) fn my_proc_number() -> ProcNumber {
    backend_utils_init_small_seams::my_proc_number::call()
}

/// `GetNumberFromPGProc(MyProc)` for the strict-deref paths: returns the slot
/// of the live `MyProc`, panicking when `MyProc == NULL`. Mirrors C code that
/// derefs `MyProc` after asserting it is non-NULL (distinct from reading the
/// tolerant `MyProcNumber` global, which may legitimately be -1).
fn my_proc_number_strict() -> ProcNumber {
    MY_PROC_NUMBER.with(|c| c.borrow().expect("MyProc is NULL (no PGPROC claimed)"))
}

/// Run `f` with mutable access to this backend's claimed `PGPROC`
/// (`&mut *MyProc`), without ever handing out a `&'static mut`. Panics when
/// `MyProc == NULL` or `ProcGlobal` is unbuilt, mirroring the C deref of a
/// `MyProc` that must be non-NULL at the call site.
pub(crate) fn with_my_proc<R>(f: impl FnOnce(&mut PGPROC) -> R) -> R {
    let procno = my_proc_number_strict();
    with_proc_by_number(procno, f)
}

/// Run `f` with shared access to this backend's claimed `PGPROC` (`&*MyProc`).
pub(crate) fn with_my_proc_ref<R>(f: impl FnOnce(&PGPROC) -> R) -> R {
    let procno = my_proc_number_strict();
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

// ---- freelist operations over ProcGlobal's four heads (genuine shmem) ----
//
// The freelist heads + the per-PGPROC `links` live in real shared memory (see
// `init_shared_freelists`), so every forked backend pops a DISTINCT ProcNumber.
// Each caller of these holds `ProcStructLock` (the spinlock bracket in
// InitProcess/ProcKill), exactly as C requires for `dlist` mutation.

/// `GetPGProcByNumber(procno)->procgloballist` mapped to its [`FreeListId`].
/// Panics if the slot belongs to no freelist (aux / prepared-xact dummy), which
/// would be a caller bug (the C deref of a NULL `procgloballist`). The
/// `procgloballist` class is stamped once in `InitProcGlobal` and is read-mostly
/// (COW-inherited, never mutated post-fork), so it stays in the process-owned
/// arena.
pub(crate) fn proc_globallist_of(procno: ProcNumber) -> FreeListId {
    with_proc_global(|pg| {
        pg.allProcs[procno as usize]
            .procgloballist
            .expect("PGPROC has no procgloballist (not a freelist-managed slot)")
    })
}

/// `dlist_container(PGPROC, links, dlist_pop_head_node(<list>))` over the
/// genuinely-shared freelist. Caller holds `ProcStructLock`.
pub(crate) fn freelist_pop_head(list: FreeListId) -> Option<ProcNumber> {
    shared_freelist_pop_head(list)
}

/// `dlist_push_head(<list>, &GetPGProcByNumber(procno)->links)` over the
/// genuinely-shared freelist. Caller holds `ProcStructLock`.
pub(crate) fn freelist_push_head(list: FreeListId, procno: ProcNumber) {
    shared_freelist_push_head(list, procno);
}

/// `dlist_push_tail(<list>, &GetPGProcByNumber(procno)->links)` over the
/// genuinely-shared freelist. Caller holds `ProcStructLock`.
pub(crate) fn freelist_push_tail(list: FreeListId, procno: ProcNumber) {
    shared_freelist_push_tail(list, procno);
}

/// A snapshot of `ProcGlobal->freeProcs` in list order, for `HaveNFreeProcs`'s
/// `dlist_foreach`. (A snapshot — rather than a live iterator — keeps the
/// shared-link walk self-contained; the caller holds `ProcStructLock`, so the
/// list cannot change underneath it.)
pub(crate) fn freelist_regular_snapshot() -> Vec<ProcNumber> {
    shared_freelist_snapshot(FreeListId::Regular)
}

/// Whether `<list>` is empty (`dlist_is_empty`), over the genuine shmem head.
#[allow(dead_code)]
pub(crate) fn freelist_is_empty(list: FreeListId) -> bool {
    shared_freelist_is_empty(list)
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

// ---- dense ProcGlobal mirror arrays (procarray.c membership) ----
//
// `ProcGlobal->{xids,subxidStates,statusFlags}` are owned here; procarray's
// membership family reads/writes them (under `ProcArrayLock`) through the
// inward seams, which delegate to these helpers.

/// `ProcGlobal->xids[idx]`.
pub(crate) fn proc_array_xid(idx: i32) -> TransactionId {
    with_proc_global(|pg| pg.xids[idx as usize])
}

/// `ProcGlobal->xids[idx] = xid`.
pub(crate) fn set_proc_array_xid(idx: i32, xid: TransactionId) {
    with_proc_global(|pg| pg.xids[idx as usize] = xid);
}

/// `(ProcGlobal->subxidStates[idx].count, .overflowed)`.
pub(crate) fn proc_array_subxid_state(idx: i32) -> (i32, bool) {
    with_proc_global(|pg| {
        let s = &pg.subxidStates[idx as usize];
        (s.count as i32, s.overflowed)
    })
}

/// `ProcGlobal->subxidStates[idx] = { count, overflowed }`.
pub(crate) fn set_proc_array_subxid_state(idx: i32, count: i32, overflowed: bool) {
    with_proc_global(|pg| {
        let s = &mut pg.subxidStates[idx as usize];
        s.count = count as u8;
        s.overflowed = overflowed;
    });
}

/// `ProcGlobal->statusFlags[idx] = flags`.
pub(crate) fn set_proc_array_status_flags(idx: i32, flags: u8) {
    with_proc_global(|pg| pg.statusFlags[idx as usize] = flags);
}

/// `memmove(&ProcGlobal->xids[dst], &ProcGlobal->xids[src], count * sizeof)`.
pub(crate) fn proc_array_xids_memmove(dst: i32, src: i32, count: i32) {
    with_proc_global(|pg| {
        pg.xids
            .copy_within(src as usize..(src + count) as usize, dst as usize)
    });
}

/// `memmove(&ProcGlobal->subxidStates[dst], ..[src], count * sizeof)`.
pub(crate) fn proc_array_subxid_states_memmove(dst: i32, src: i32, count: i32) {
    with_proc_global(|pg| {
        pg.subxidStates
            .copy_within(src as usize..(src + count) as usize, dst as usize)
    });
}

/// `memmove(&ProcGlobal->statusFlags[dst], ..[src], count * sizeof)`.
pub(crate) fn proc_array_status_flags_memmove(dst: i32, src: i32, count: i32) {
    with_proc_global(|pg| {
        pg.statusFlags
            .copy_within(src as usize..(src + count) as usize, dst as usize)
    });
}

// ---- ProcGlobal->procArrayGroupFirst atomic (procarray.c group-clear) ----

/// `pg_atomic_read_u32(&ProcGlobal->procArrayGroupFirst)`.
pub(crate) fn proc_array_group_first_read() -> u32 {
    with_proc_global(|pg| pg.procArrayGroupFirst.read())
}

/// `pg_atomic_compare_exchange_u32(&ProcGlobal->procArrayGroupFirst, expected,
/// newval)` — returns `(succeeded, value_seen)`.
pub(crate) fn proc_array_group_first_compare_exchange(expected: u32, newval: u32) -> (bool, u32) {
    with_proc_global(|pg| {
        match pg.procArrayGroupFirst.value.compare_exchange(
            expected,
            newval,
            core::sync::atomic::Ordering::SeqCst,
            core::sync::atomic::Ordering::SeqCst,
        ) {
            Ok(prev) => (true, prev),
            Err(seen) => (false, seen),
        }
    })
}

/// `pg_atomic_exchange_u32(&ProcGlobal->procArrayGroupFirst, newval)`.
pub(crate) fn proc_array_group_first_exchange(newval: u32) -> u32 {
    with_proc_global(|pg| {
        pg.procArrayGroupFirst
            .value
            .swap(newval, core::sync::atomic::Ordering::SeqCst)
    })
}

// ---- ProcGlobal->clogGroupFirst atomic (clog.c group XID-status update) ----

/// `pg_atomic_read_u32(&ProcGlobal->clogGroupFirst)`.
pub(crate) fn clog_group_first_read() -> u32 {
    with_proc_global(|pg| pg.clogGroupFirst.read())
}

/// `pg_atomic_compare_exchange_u32(&ProcGlobal->clogGroupFirst, expected,
/// newval)` — returns `(succeeded, value_seen)`.
pub(crate) fn clog_group_first_compare_exchange(expected: u32, newval: u32) -> (bool, u32) {
    with_proc_global(|pg| {
        match pg.clogGroupFirst.value.compare_exchange(
            expected,
            newval,
            core::sync::atomic::Ordering::SeqCst,
            core::sync::atomic::Ordering::SeqCst,
        ) {
            Ok(prev) => (true, prev),
            Err(seen) => (false, seen),
        }
    })
}

/// `pg_atomic_exchange_u32(&ProcGlobal->clogGroupFirst, newval)`.
pub(crate) fn clog_group_first_exchange(newval: u32) -> u32 {
    with_proc_global(|pg| {
        pg.clogGroupFirst
            .value
            .swap(newval, core::sync::atomic::Ordering::SeqCst)
    })
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
    // C: `if (AuxiliaryProcs[proctype].pid == 0) break;` — over the genuinely
    // shared pid words, so a slot another forked aux process already claimed
    // (nonzero pid) is skipped, handing each aux child a distinct ProcNumber.
    (0..NUM_AUXILIARY_PROCS)
        .find(|&proctype| shared_pid(base + proctype) == 0)
}

/// `GetPGProcByNumber(procno)->pid` — read of the genuinely-shared `pid` word.
pub(crate) fn shared_pid(procno: ProcNumber) -> i32 {
    shared_pid_slot(procno).load(AtomicOrdering::Relaxed)
}

/// `GetPGProcByNumber(procno)->pid = pid` — write of the genuinely-shared `pid`
/// word (the cross-process slot-claim / release). Also mirrors the value into
/// the per-process `PGPROC.pid` field so in-process readers (e.g. lock-group
/// leader detection) stay consistent.
pub(crate) fn set_shared_pid(procno: ProcNumber, pid: i32) {
    shared_pid_slot(procno).store(pid, AtomicOrdering::Relaxed);
    with_proc_by_number(procno, |p| p.pid = pid);
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
pub fn InitProcGlobal() -> PgResult<()> {
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

    // Place the genuinely-shared `pid` words and the `ProcStructLock` spinlock
    // in real shared memory (C: part of the "PGPROC structures" + the separate
    // "ProcStructLock" ShmemInitStruct blocks). These are the cross-process
    // slot-coordination state — they MUST be shared, not inherited copy-on-fork,
    // so each forked aux/backend child sees the others' claims and gets a
    // distinct ProcNumber. The rest of the PGPROC arena stays process-owned.
    init_shared_pid_block(total_procs)?;

    // Place the genuinely-shared freelist `links` array and the four
    // `dlist_head`s in real shared memory (same rationale as the pid words: the
    // freelist is mutated on every connect/disconnect and the mutation MUST be
    // visible to the postmaster and all sibling backends, so it cannot be
    // COW-inherited). Zeroed to "all empty / all detached"; threaded below.
    init_shared_freelists(total_procs)?;

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
            proc.procLatch
                .is_shared
                .store(true, core::sync::atomic::Ordering::SeqCst);
            proc.procLatch
                .owner_pid
                .store(0, core::sync::atomic::Ordering::SeqCst);

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
            // `dlist_push_tail(<freelist>, &proc->links)` onto the genuinely-
            // shared freelist (no ProcStructLock needed — InitProcGlobal runs
            // once in the postmaster before any fork, exactly like C).
            let procno = i as ProcNumber;
            shared_freelist_push_tail(list, procno);
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
        // Record the stable base of the now-stored `allProcs` array so
        // `with_proc_latch` can reach `&proc->procLatch` (all-atomic `Latch`)
        // from a signal handler without taking the `PROC_GLOBAL` RefCell
        // borrow. The array was built with the final capacity and is never
        // grown again, so the buffer address is fixed for the process
        // lifetime (the faithful stable `ProcGlobal->allProcs`).
        let pg = cell.borrow();
        let all = &pg.as_ref().expect("just stored").allProcs;
        SHARED_ALL_PROCS.store(all.as_ptr() as *mut PGPROC, AtomicOrdering::Relaxed);
        SHARED_ALL_PROCS_COUNT.store(all.len(), AtomicOrdering::Relaxed);
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
    // A per-PGPROC `procLatch` is named in the latch unit's *proc-tagged*
    // handle space (`LatchHandle::proc`), distinct from the latch unit's own
    // registry: `SetLatch`/`OwnLatch`/`DisownLatch` dispatch the tagged handle
    // back through `with_proc_latch` to this slot's embedded `Latch` (the
    // faithful `&proc->procLatch`), rather than indexing the local registry.
    LatchHandle::proc(procNumber)
}

/// Run `f` over `&ProcGlobal->allProcs[procno].procLatch` — hands the latch
/// unit a shared reference to a backend's embedded `Latch` (the owner accessor
/// behind the `with_proc_latch` seam). The proc unit owns the `allProcs`
/// array; the latch unit applies its own `SetLatch`/`OwnLatch`/`DisownLatch`
/// algorithm inside the callback.
pub(crate) fn with_proc_latch(procno: ProcNumber, f: &mut dyn FnMut(&types_storage::latch::Latch)) {
    // Reach `&proc->procLatch` through the genuinely-shared `SHARED_PROC_LATCHES`
    // array placed in real shmem by InitProcGlobal — NOT the per-process
    // `allProcs` `Vec` (whose `procLatch` writes would be invisible to other
    // processes, silently breaking every cross-process `SetLatch` wakeup, e.g.
    // the end-of-recovery checkpoint handshake). This also keeps the original
    // async-signal-safety property: the `Latch` is all-atomic and the array is
    // a fixed shmem mapping, so `SetLatch` from a `SIGALRM` handler takes no
    // `PROC_GLOBAL` RefCell borrow (no "RefCell already borrowed" abort).
    let base = SHARED_PROC_LATCHES.load(AtomicOrdering::Relaxed);
    let count = SHARED_PROC_LATCH_COUNT.load(AtomicOrdering::Relaxed);
    assert!(
        !base.is_null(),
        "with_proc_latch: procLatch base uninitialized (InitProcGlobal not run)"
    );
    let idx = procno as usize;
    assert!(
        procno >= 0 && idx < count,
        "with_proc_latch: ProcNumber {procno} out of range (0..{count})"
    );
    // SAFETY: `base` addresses the stable, process-lifetime shared `procLatch`
    // array placed by InitProcGlobal; `idx` is bounds-checked against its
    // length. We only touch the all-atomic `Latch` through a shared reference;
    // its fields are mutated concurrently/cross-process exactly as C's
    // `volatile` latch is.
    let latch: &types_storage::latch::Latch = unsafe { &*base.add(idx) };
    f(latch);
}
