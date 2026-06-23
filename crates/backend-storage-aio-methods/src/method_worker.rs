//! `storage/aio/method_worker.c` — perform AIO using worker processes.
//!
//! IO workers consume IOs from a shared-memory submission queue, run
//! traditional synchronous system calls, and perform the shared completion
//! handling immediately. Client code submits most requests by pushing IO handle
//! indices into the submission queue and waits (if necessary) on the per-handle
//! completion condition variable. Some IOs cannot be performed in another
//! process (no infrastructure for reopening the file, or the IO references
//! process-local memory) and must be processed synchronously by the client.
//!
//! This is PG 18's *default* IO method, available in all builds on all OSes.
//!
//! ## Shared-memory model (faithful to C)
//!
//! Two `ShmemInitStruct` allocations live in genuine cross-process shared
//! memory (placed by `pgaio_worker_shmem_init` in the postmaster, before fork,
//! so every forked backend / IO worker reaches the identical mapping at the
//! same VA — exactly like `MainLWLockArray` / `TwoPhaseState`):
//!
//!  * [`PgAioWorkerSubmissionQueue`] — the ring buffer of in-flight IO handle
//!    indices (`sqes[]`), a power-of-two-sized SPMC queue serialized by the
//!    `AioWorkerSubmissionQueue` built-in LWLock.
//!  * [`PgAioWorkerControl`] — the `idle_worker_mask` bitfield + the
//!    per-worker slot array.
//!
//! Both are `#[repr(C)]` flat headers with a flexible trailing array, reached
//! through a raw base pointer (`AIO_WORKER_QUEUE_BASE` / `AIO_WORKER_CONTROL_BASE`)
//! — the 2PC port's `TwoPhaseStateData` idiom.
//!
//! ## The one structural divergence: worker latch is addressed by procno
//!
//! In C `PgAioWorkerSlot.latch` stores the worker's `MyLatch`, which for an aux
//! process IS its PGPROC `procLatch` (a shared latch). A backend wakes a worker
//! with `SetLatch(worker->latch)`. A raw `Latch *` is not portable across the
//! Rust process-local latch registry, so this port stores the worker's
//! **ProcNumber** in the slot and wakes it via `set_latch_for_procno` (which
//! resolves the target PGPROC's `procLatch`). This is exactly the same shared
//! latch the C dereferences, addressed by its owner's procno instead of a raw
//! pointer.

#[cfg(target_family = "wasm")]
#[allow(unused_imports)]
use wasm_libc_shim as libc;
extern crate alloc;

use alloc::vec::Vec;

use core::sync::atomic::{AtomicPtr, Ordering};

use types_core::primitive::Size;
use types_core::init::BackendType;
use types_core::ProcNumber;
use types_error::{PgError, PgResult};
use types_pgstat::wait_event::PG_WAIT_ACTIVITY;
use types_signal::SigHandler;
use types_startup::StartupData;
use types_storage::storage::MAX_IO_WORKERS;
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET};
use types_storage::LW_EXCLUSIVE;

use backend_storage_lmgr_lwlock::{main_lock_ref, LWLockAcquire, LWLockRelease};
use backend_storage_ipc_shmem_seams as ipc_shmem;
use backend_storage_ipc_latch_seams as latch;
use backend_postmaster_interrupt as interrupt;
use backend_utils_init_small_seams as initsmall;
use backend_utils_init_miscinit_seams as misc;
use backend_storage_ipc_dsm_core_seams as ipc;
use backend_utils_misc_guc_seams as guc;
use backend_utils_misc_ps_status_seams as ps;
use backend_storage_aio_completion_seams as completion;

use crate::aio::{
    ioh, pgaio_io_get_id, pgaio_io_prepare_submit, pgaio_io_process_completion,
};
use crate::aio_io::pgaio_io_perform_synchronously;
use crate::aio_target::pgaio_io_can_reopen;
use crate::{IoMethodOps, PgAioHandle, PGAIO_HF_REFERENCES_LOCAL, PGAIO_SUBMIT_BATCH_SIZE};

// ===========================================================================
// Constants (method_worker.c / storage/proc.h)
// ===========================================================================

/// `#define IO_WORKER_WAKEUP_FANOUT 2` — how many idle workers a woken worker
/// may itself wake.
const IO_WORKER_WAKEUP_FANOUT: usize = 2;

/// `static int io_worker_queue_size = 64;` — the configured ring depth (the
/// shmem queue is the next power of two ≥ this).
const IO_WORKER_QUEUE_SIZE: i32 = 64;

/// The `AioWorkerSubmissionQueue` built-in LWLock offset into the main
/// LWLock array (`PG_LWLOCK(53, AioWorkerSubmissionQueue)`, lwlocklist.h).
const AIO_WORKER_SUBMISSION_QUEUE_LOCK: usize = 53;

/// `WAIT_EVENT_IO_WORKER_MAIN` — 7th entry (index 6) of the Activity section of
/// `wait_event_names.txt` (ARCHIVER_MAIN, AUTOVACUUM_MAIN, BGWRITER_HIBERNATE,
/// BGWRITER_MAIN, CHECKPOINTER_MAIN, CHECKPOINTER_SHUTDOWN, IO_WORKER_MAIN), so
/// `PG_WAIT_ACTIVITY + 6`.
const WAIT_EVENT_IO_WORKER_MAIN: u32 = PG_WAIT_ACTIVITY + 6;

/// `INVALID_PROC_NUMBER` (-1): a free worker slot has no owning procno.
const INVALID_PROC_NUMBER: ProcNumber = types_core::primitive::INVALID_PROC_NUMBER;

// ===========================================================================
// The shared-memory ring buffer (PgAioWorkerSubmissionQueue) — repr(C) flat.
// ===========================================================================

/// `typedef struct PgAioWorkerSubmissionQueue` (method_worker.c) — the flat
/// `#[repr(C)]` header; the `int sqes[FLEXIBLE_ARRAY_MEMBER]` ring trails it in
/// the same `ShmemInitStruct` allocation.
#[repr(C)]
struct PgAioWorkerSubmissionQueueHeader {
    /// `uint32 size` — ring capacity (a power of two).
    size: u32,
    /// `uint32 mask` — `size - 1` (the C uses `size - 1` inline; cached here).
    mask: u32,
    /// `uint32 head` — producer cursor.
    head: u32,
    /// `uint32 tail` — consumer cursor.
    tail: u32,
    // int sqes[size] trails immediately (FLEXIBLE_ARRAY_MEMBER).
}

/// `offsetof(PgAioWorkerSubmissionQueue, sqes)`.
const SQ_SQES_OFFSET: usize = core::mem::size_of::<PgAioWorkerSubmissionQueueHeader>();

/// A handle over the genuinely-shared submission queue (a thin wrapper over the
/// `ShmemInitStruct` base, like 2PC's `TwoPhaseStateData`). Every call in any
/// backend reaches the SAME shared bytes; the `AioWorkerSubmissionQueue` LWLock
/// serializes mutation.
struct SubmissionQueue {
    base: *mut u8,
}

impl SubmissionQueue {
    fn header(&self) -> &mut PgAioWorkerSubmissionQueueHeader {
        // SAFETY: `base` is the live `ShmemInitStruct("AioWorkerSubmissionQueue")`
        // allocation, sized to hold the header + ring. Callers hold the
        // AioWorkerSubmissionQueue LWLock for all mutation, exactly as C.
        unsafe { &mut *(self.base as *mut PgAioWorkerSubmissionQueueHeader) }
    }

    /// `&queue->sqes[i]` — the i-th ring slot.
    fn sqe(&self, i: u32) -> *mut i32 {
        // SAFETY: `i < size`; the trailing `int sqes[size]` follows the header.
        unsafe { (self.base.add(SQ_SQES_OFFSET) as *mut i32).add(i as usize) }
    }

    /// `pgaio_worker_submission_queue_insert(ioh)` — enqueue the handle's id at
    /// `head`; returns false when the ring is full.
    fn insert(&self, ioh_index: usize) -> bool {
        let h = self.header();
        let new_head = (h.head + 1) & (h.size - 1);
        if new_head == h.tail {
            // io queue is full
            return false;
        }
        // SAFETY: head < size.
        unsafe {
            *self.sqe(h.head) = pgaio_io_get_id(ioh_index);
        }
        h.head = new_head;
        true
    }

    /// `pgaio_worker_submission_queue_consume()` — dequeue from `tail`; returns
    /// -1 when empty.
    fn consume(&self) -> i32 {
        let h = self.header();
        if h.tail == h.head {
            return -1; // empty
        }
        // SAFETY: tail < size.
        let result = unsafe { *self.sqe(h.tail) };
        h.tail = (h.tail + 1) & (h.size - 1);
        result
    }

    /// `pgaio_worker_submission_queue_depth()` — number of queued IOs.
    fn depth(&self) -> u32 {
        let h = self.header();
        let mut head = h.head;
        let tail = h.tail;
        if tail > head {
            head += h.size;
        }
        debug_assert!(head >= tail);
        head - tail
    }
}

// ===========================================================================
// The shared-memory worker control block (PgAioWorkerControl) — repr(C) flat.
// ===========================================================================

/// `typedef struct PgAioWorkerSlot` (method_worker.c). The C `Latch *latch` is
/// replaced by the owning worker's `ProcNumber` (see the module doc): a worker
/// is woken via `set_latch_for_procno(procno)`, which reaches the same shared
/// `procLatch` the C `SetLatch(worker->latch)` does.
#[repr(C)]
#[derive(Clone, Copy)]
struct PgAioWorkerSlot {
    /// The owning worker's `ProcNumber`, or `INVALID_PROC_NUMBER` when free
    /// (the C `Latch *latch`, addressed by procno here).
    procno: ProcNumber,
    /// `bool in_use`.
    in_use: bool,
}

/// `typedef struct PgAioWorkerControl` (method_worker.c) — the flat `#[repr(C)]`
/// header; the `PgAioWorkerSlot workers[FLEXIBLE_ARRAY_MEMBER]` (sized
/// `MAX_IO_WORKERS`) trails it in the same allocation.
#[repr(C)]
struct PgAioWorkerControlHeader {
    /// `uint64 idle_worker_mask`.
    idle_worker_mask: u64,
    // PgAioWorkerSlot workers[MAX_IO_WORKERS] trails (FLEXIBLE_ARRAY_MEMBER).
}

/// `offsetof(PgAioWorkerControl, workers)`.
const WC_WORKERS_OFFSET: usize = core::mem::size_of::<PgAioWorkerControlHeader>();

/// Handle over the genuinely-shared worker control block.
struct WorkerControl {
    base: *mut u8,
}

impl WorkerControl {
    fn header(&self) -> &mut PgAioWorkerControlHeader {
        // SAFETY: `base` is the live `ShmemInitStruct("AioWorkerControl")`
        // allocation; all mutation runs under the AioWorkerSubmissionQueue LWLock.
        unsafe { &mut *(self.base as *mut PgAioWorkerControlHeader) }
    }

    /// `&io_worker_control->workers[i]`.
    fn worker(&self, i: usize) -> &mut PgAioWorkerSlot {
        debug_assert!(i < MAX_IO_WORKERS as usize);
        // SAFETY: i < MAX_IO_WORKERS; the trailing array follows the header.
        unsafe { &mut *(self.base.add(WC_WORKERS_OFFSET) as *mut PgAioWorkerSlot).add(i) }
    }

    /// `pgaio_worker_choose_idle()` — pop the lowest set bit of
    /// `idle_worker_mask`; returns the worker index or -1 when none idle.
    fn choose_idle(&self) -> i32 {
        let h = self.header();
        if h.idle_worker_mask == 0 {
            return -1;
        }
        // pg_rightmost_one_pos64: trailing-zero count is the lowest set-bit pos.
        let worker = h.idle_worker_mask.trailing_zeros();
        h.idle_worker_mask &= !(1u64 << worker);
        debug_assert!(self.worker(worker as usize).in_use);
        worker as i32
    }
}

// ===========================================================================
// Process-global handles to the two shmem blocks + the consuming-worker id.
// ===========================================================================

/// Base of the shared `PgAioWorkerSubmissionQueue` (the C
/// `io_worker_submission_queue`). Set by [`pgaio_worker_shmem_init`] in the
/// postmaster; inherited by every forked child (same MAP_SHARED VA).
static AIO_WORKER_QUEUE_BASE: AtomicPtr<u8> = AtomicPtr::new(core::ptr::null_mut());

/// Base of the shared `PgAioWorkerControl` (the C `io_worker_control`).
static AIO_WORKER_CONTROL_BASE: AtomicPtr<u8> = AtomicPtr::new(core::ptr::null_mut());

fn submission_queue() -> SubmissionQueue {
    let base = AIO_WORKER_QUEUE_BASE.load(Ordering::Relaxed);
    debug_assert!(!base.is_null(), "AioWorkerSubmissionQueue accessed before shmem init");
    SubmissionQueue { base }
}

fn worker_control() -> WorkerControl {
    let base = AIO_WORKER_CONTROL_BASE.load(Ordering::Relaxed);
    debug_assert!(!base.is_null(), "AioWorkerControl accessed before shmem init");
    WorkerControl { base }
}

thread_local! {
    /// `static int MyIoWorkerId;` — this IO worker's slot index (only meaningful
    /// in a B_IO_WORKER process). `-1` before registration.
    static MY_IO_WORKER_ID: core::cell::Cell<i32> = const { core::cell::Cell::new(-1) };
}

fn my_io_worker_id() -> i32 {
    MY_IO_WORKER_ID.with(|c| c.get())
}

fn set_my_io_worker_id(id: i32) {
    MY_IO_WORKER_ID.with(|c| c.set(id));
}

// ===========================================================================
// Shmem sizing + init (the IoMethodOps shmem_size / shmem_init callbacks).
// ===========================================================================

/// `pg_nextpower2_32(n)` — the smallest power of two ≥ `n` (n > 0).
fn next_power_of_two_32(n: i32) -> u32 {
    debug_assert!(n > 0);
    (n as u32).next_power_of_two()
}

/// `pgaio_worker_queue_shmem_size(&queue_size)` — `offsetof(.., sqes) +
/// sizeof(int) * queue_size`. Returns `(size_bytes, queue_size)`.
fn pgaio_worker_queue_shmem_size() -> (Size, u32) {
    let queue_size = next_power_of_two_32(IO_WORKER_QUEUE_SIZE);
    let sz = SQ_SQES_OFFSET + core::mem::size_of::<i32>() * queue_size as usize;
    (sz, queue_size)
}

/// `pgaio_worker_control_shmem_size()` — `offsetof(.., workers) +
/// sizeof(PgAioWorkerSlot) * MAX_IO_WORKERS`.
fn pgaio_worker_control_shmem_size() -> Size {
    WC_WORKERS_OFFSET + core::mem::size_of::<PgAioWorkerSlot>() * MAX_IO_WORKERS as usize
}

/// `static size_t pgaio_worker_shmem_size(void)` (method_worker.c).
pub(crate) fn pgaio_worker_shmem_size() -> PgResult<Size> {
    let (queue_sz, _) = pgaio_worker_queue_shmem_size();
    let sz = ipc_shmem::add_size::call(queue_sz, pgaio_worker_control_shmem_size())?;
    Ok(sz)
}

/// `static void pgaio_worker_shmem_init(bool first_time)` (method_worker.c).
pub(crate) fn pgaio_worker_shmem_init(_first_time: bool) -> PgResult<()> {
    let (queue_bytes, queue_size) = pgaio_worker_queue_shmem_size();

    let (q_base, q_found) =
        ipc_shmem::shmem_init_struct::call("AioWorkerSubmissionQueue", queue_bytes)?;
    AIO_WORKER_QUEUE_BASE.store(q_base, Ordering::Relaxed);
    if !q_found {
        let queue = SubmissionQueue { base: q_base };
        let h = queue.header();
        h.size = queue_size;
        h.mask = queue_size - 1;
        h.head = 0;
        h.tail = 0;
    }

    let (c_base, c_found) =
        ipc_shmem::shmem_init_struct::call("AioWorkerControl", pgaio_worker_control_shmem_size())?;
    AIO_WORKER_CONTROL_BASE.store(c_base, Ordering::Relaxed);
    if !c_found {
        let control = WorkerControl { base: c_base };
        control.header().idle_worker_mask = 0;
        for i in 0..MAX_IO_WORKERS as usize {
            let slot = control.worker(i);
            slot.procno = INVALID_PROC_NUMBER;
            slot.in_use = false;
        }
    }
    Ok(())
}

// ===========================================================================
// The IoMethodOps callbacks: needs_synchronous_execution + submit.
// ===========================================================================

/// `static bool pgaio_worker_needs_synchronous_execution(PgAioHandle *ioh)`
/// (method_worker.c).
pub(crate) fn pgaio_worker_needs_synchronous_execution(ioh: &PgAioHandle) -> bool {
    // Resolve the handle index from the borrowed reference. The engine passes
    // `&pgaio_ctl->io_handles[idx]`; recover idx by identity within the array.
    let idx = ioh_index_of(ioh);

    !initsmall::is_under_postmaster::call()
        || (ioh.data().flags & PGAIO_HF_REFERENCES_LOCAL) != 0
        || !pgaio_io_can_reopen(idx)
        // PORT BOUNDARY: in C `pgaio_io_can_reopen` is true for the smgr target
        // (its `reopen` vtable entry exists) so worker-executable IOs run in a
        // worker. In this port the worker's per-IO file reopen
        // (`pgaio_io_reopen` -> the smgr-AIO `pgaio_target_smgr.reopen`) is a
        // genuinely-unported seam owned by the buffer-manager/smgr AIO layer
        // (the same layer whose buffered-read completion the port still drives
        // synchronously). Until that seam lands, a real IO handed to a worker
        // would panic in `pgaio_io_reopen`. So treat "reopen seam not installed"
        // exactly as C's "`reopen == NULL` -> can't reopen -> run
        // synchronously": the IO executes inline on the issuing backend
        // (identical to `io_method = sync`), while the worker shmem ring +
        // IoWorkerMain loop + GUC/process supervision remain fully wired and
        // ready for the moment the reopen seam is installed.
        || !backend_storage_aio_completion_seams::pgaio_io_reopen::is_installed()
}

/// `static void pgaio_worker_submit_internal(int num_staged_ios,
/// PgAioHandle **staged_ios)` (method_worker.c). `staged` carries the io-handle
/// indices the engine has staged.
fn pgaio_worker_submit_internal(staged: &[usize]) -> PgResult<()> {
    debug_assert!(staged.len() <= PGAIO_SUBMIT_BATCH_SIZE);

    let mut synchronous_ios: Vec<usize> = Vec::new();
    let mut wakeup: Option<ProcNumber> = None;

    let lock = main_lock_ref(AIO_WORKER_SUBMISSION_QUEUE_LOCK);
    LWLockAcquire(lock, LW_EXCLUSIVE, initsmall::my_proc_number::call())?;

    let queue = submission_queue();
    let control = worker_control();
    for &ioh_index in staged {
        debug_assert!(!pgaio_worker_needs_synchronous_execution(ioh(ioh_index)));
        if !queue.insert(ioh_index) {
            // Full: run it synchronously, but only after sending as many as we
            // can to workers, to maximize concurrency.
            synchronous_ios.push(ioh_index);
            continue;
        }

        if wakeup.is_none() {
            // Choose an idle worker to wake up if we haven't already.
            let worker = control.choose_idle();
            if worker >= 0 {
                wakeup = Some(control.worker(worker as usize).procno);
            }
        }
    }
    LWLockRelease(lock)?;

    if let Some(procno) = wakeup {
        latch::set_latch_for_procno::call(procno);
    }

    // Run whatever is left synchronously.
    for ioh_index in synchronous_ios {
        pgaio_io_perform_synchronously(ioh_index)?;
    }
    Ok(())
}

/// `static int pgaio_worker_submit(uint16 num_staged_ios,
/// PgAioHandle **staged_ios)` (method_worker.c). `staged` is the io-handle index
/// list the engine staged (read from `pgaio_my_backend->staged_ios`).
pub(crate) fn pgaio_worker_submit(staged: &[usize]) -> PgResult<i32> {
    for &ioh_index in staged {
        pgaio_io_prepare_submit(ioh_index)?;
    }
    pgaio_worker_submit_internal(staged)?;
    Ok(staged.len() as i32)
}

/// `const IoMethodOps pgaio_worker_ops` (method_worker.c).
pub(crate) fn pgaio_worker_ops() -> IoMethodOps {
    IoMethodOps {
        // method_worker.c leaves `.wait_on_fd_before_close` 0 (the struct only
        // sets shmem_size/shmem_init/needs_synchronous_execution/submit). The
        // worker reopens the file itself, so the issuer need not wait on FD close.
        wait_on_fd_before_close: false,
        shmem_size: Some(pgaio_worker_shmem_size),
        shmem_init: Some(pgaio_worker_shmem_init),
        init_backend: None,
        needs_synchronous_execution: Some(pgaio_worker_needs_synchronous_execution),
        submit: Some(crate::pgaio_worker_submit_bridge),
        // No `wait_one`: the issuer waits on the handle's completion CV (the
        // worker broadcasts it from `pgaio_io_process_completion`).
        wait_one: None,
    }
}

// ===========================================================================
// Worker registration + the IoWorkerMain aux-process loop.
// ===========================================================================

/// `static void pgaio_worker_die(int code, Datum arg)` (method_worker.c) — the
/// `on_shmem_exit` callback that releases this worker's slot.
fn pgaio_worker_die(_code: i32, _arg: types_tuple::Datum<'static>) -> PgResult<()> {
    let id = my_io_worker_id();
    let lock = main_lock_ref(AIO_WORKER_SUBMISSION_QUEUE_LOCK);
    LWLockAcquire(lock, LW_EXCLUSIVE, initsmall::my_proc_number::call())?;
    let control = worker_control();
    debug_assert!(control.worker(id as usize).in_use);
    control.header().idle_worker_mask &= !(1u64 << id);
    let slot = control.worker(id as usize);
    slot.in_use = false;
    slot.procno = INVALID_PROC_NUMBER;
    LWLockRelease(lock)?;
    Ok(())
}

/// `static void pgaio_worker_register(void)` (method_worker.c) — claim a free
/// worker slot, assign `MyIoWorkerId`, register the release callback.
fn pgaio_worker_register() -> PgResult<()> {
    set_my_io_worker_id(-1);

    let lock = main_lock_ref(AIO_WORKER_SUBMISSION_QUEUE_LOCK);
    LWLockAcquire(lock, LW_EXCLUSIVE, initsmall::my_proc_number::call())?;

    let control = worker_control();
    let my_procno = initsmall::my_proc_number::call();
    for i in 0..MAX_IO_WORKERS as usize {
        if !control.worker(i).in_use {
            debug_assert_eq!(control.worker(i).procno, INVALID_PROC_NUMBER);
            let slot = control.worker(i);
            slot.in_use = true;
            set_my_io_worker_id(i as i32);
            break;
        } else {
            debug_assert_ne!(control.worker(i).procno, INVALID_PROC_NUMBER);
        }
    }

    if my_io_worker_id() == -1 {
        let _ = LWLockRelease(lock);
        return Err(PgError::error("couldn't find a free worker slot"));
    }

    let id = my_io_worker_id() as usize;
    control.header().idle_worker_mask |= 1u64 << id;
    control.worker(id).procno = my_procno;
    LWLockRelease(lock)?;

    // on_shmem_exit(pgaio_worker_die, 0).
    ipc::on_shmem_exit::call(pgaio_worker_die, types_tuple::Datum::null())?;
    Ok(())
}

/// `void IoWorkerMain(const void *startup_data, size_t startup_data_len)`
/// (method_worker.c) — the IO-worker aux-process entry point: register, then
/// loop draining the submission queue (reopen + perform synchronously +
/// complete), waking peers and sleeping on the latch when idle.
///
/// Never returns (ends in `proc_exit`). The C `sigsetjmp` error recovery (mark
/// the in-flight IO failed, then exit so the postmaster restarts the worker) is
/// covered by this crate's `PgResult` propagation to [`io_worker_main`], whose
/// top-level error path runs `proc_exit(1)`.
pub fn io_worker_main(startup_data: &StartupData) -> ! {
    match io_worker_main_inner(startup_data) {
        Ok(()) => ipc::proc_exit::call(0, initsmall::my_proc_pid::call()),
        Err(_e) => {
            // EmitErrorReport()/process-completion-of-the-in-flight-IO is the C
            // recovery; the postmaster starts a replacement worker on exit(1).
            ipc::proc_exit::call(1, initsmall::my_proc_pid::call());
        }
    }
}

fn io_worker_main_inner(startup_data: &StartupData) -> PgResult<()> {
    debug_assert!(matches!(startup_data, StartupData::None));

    // MyBackendType = B_IO_WORKER; AuxiliaryProcessMainCommon().
    initsmall::set_my_backend_type::call(BackendType::IoWorker);
    backend_postmaster_auxprocess_seams::auxiliary_process_main_common::call()?;

    // Signal dispositions (method_worker.c:397-409).
    {
        let pqsignal = port_pqsignal_seams::pqsignal::call;
        // pqsignal(SIGHUP, SignalHandlerForConfigReload);
        fn config_reload(_sig: i32) {
            interrupt::SignalHandlerForConfigReload();
        }
        pqsignal(libc::SIGHUP, SigHandler::Handler(config_reload));
        // pqsignal(SIGINT, die); /* to allow manually triggering worker restart */
        pqsignal(
            libc::SIGINT,
            SigHandler::Handler(backend_tcop_postgres_seams::die::call),
        );
        // pqsignal(SIGTERM, SIG_IGN); /* explicit shutdown via SIGUSR2 later */
        pqsignal(libc::SIGTERM, SigHandler::Ignore);
        // SIGQUIT handler was already set up by InitPostmasterChild.
        // pqsignal(SIGALRM, SIG_IGN);
        pqsignal(libc::SIGALRM, SigHandler::Ignore);
        // pqsignal(SIGPIPE, SIG_IGN);
        pqsignal(libc::SIGPIPE, SigHandler::Ignore);
        // pqsignal(SIGUSR1, procsignal_sigusr1_handler);
        pqsignal(
            libc::SIGUSR1,
            SigHandler::Handler(
                backend_storage_ipc_procsignal::procsignal_sigusr1_handler_signal,
            ),
        );
        // pqsignal(SIGUSR2, SignalHandlerForShutdownRequest);
        fn shutdown_request(_sig: i32) {
            interrupt::SignalHandlerForShutdownRequest();
        }
        pqsignal(libc::SIGUSR2, SigHandler::Handler(shutdown_request));
    }

    // also registers a shutdown callback to unregister
    pgaio_worker_register()?;

    // sprintf(cmd, "%d", MyIoWorkerId); set_ps_display(cmd);
    ps::set_ps_display::call(alloc::format!("{}", my_io_worker_id()));

    // sigprocmask(SIG_SETMASK, &UnBlockSig, NULL).
    backend_libpq_pqsignal_seams::unblock_signals::call();

    while !interrupt::ShutdownRequestPending() {
        let mut latches: [ProcNumber; IO_WORKER_WAKEUP_FANOUT] =
            [INVALID_PROC_NUMBER; IO_WORKER_WAKEUP_FANOUT];
        let mut nlatches = 0usize;
        let io_index: i32;

        // Try to get a job to do. The LWLock acquisition also provides the
        // memory barrier ensuring we don't see outdated data in the handle.
        let lock = main_lock_ref(AIO_WORKER_SUBMISSION_QUEUE_LOCK);
        LWLockAcquire(lock, LW_EXCLUSIVE, initsmall::my_proc_number::call())?;
        {
            let control = worker_control();
            let queue = submission_queue();
            let my_id = my_io_worker_id();
            io_index = queue.consume();
            if io_index == -1 {
                // Nothing to do. Mark self idle.
                control.header().idle_worker_mask |= 1u64 << my_id;
            } else {
                // Got one. Clear idle flag.
                control.header().idle_worker_mask &= !(1u64 << my_id);
                // See if we can wake up some peers.
                let nwakeups = core::cmp::min(queue.depth() as usize, IO_WORKER_WAKEUP_FANOUT);
                for _ in 0..nwakeups {
                    let worker = control.choose_idle();
                    if worker < 0 {
                        break;
                    }
                    latches[nlatches] = control.worker(worker as usize).procno;
                    nlatches += 1;
                }
            }
        }
        LWLockRelease(lock)?;

        for &procno in latches.iter().take(nlatches) {
            latch::set_latch_for_procno::call(procno);
        }

        if io_index != -1 {
            let ioh_index = io_index as usize;

            // Prevent interrupts between reopen and perform_synchronously that
            // could otherwise close the FD in that window.
            misc::hold_interrupts::call();

            // It's very unlikely, but possible, that reopen fails (memory
            // allocation, file permissions). In that case we fail the IO.
            // (C sets error_errno = ENOENT around this.)
            match completion::pgaio_io_reopen::call(ioh_index as u32) {
                Ok(()) => {}
                Err(_) => {
                    // Mark the IO failed (the C sigjmp path:
                    // pgaio_io_process_completion(ioh, -ENOENT)).
                    misc::start_crit_section::call();
                    pgaio_io_process_completion(ioh_index, -libc::ENOENT)?;
                    misc::end_crit_section::call();
                    misc::resume_interrupts::call();
                    continue;
                }
            }

            // We don't expect this to ever fail with ERROR/FATAL;
            // pgaio_io_perform_synchronously contains its own critical section.
            pgaio_io_perform_synchronously(ioh_index)?;

            misc::resume_interrupts::call();
        } else {
            latch::wait_latch_my_latch::call(
                WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
                -1,
                WAIT_EVENT_IO_WORKER_MAIN,
            )?;
            latch::reset_latch_my_latch::call();
        }

        misc::check_for_interrupts::call()?;

        if interrupt::ConfigReloadPending() {
            interrupt::SetConfigReloadPending(false);
            guc::process_config_file_sighup::call()?;
        }
    }

    Ok(())
}

// ===========================================================================
// Index-recovery helper for the borrowed-handle ops callback.
// ===========================================================================

/// Recover the io-handle array index from a borrowed `&PgAioHandle` (the C ops
/// callback receives `PgAioHandle *ioh`; the index is its offset into
/// `pgaio_ctl->io_handles`). Used by `needs_synchronous_execution`.
fn ioh_index_of(handle: &PgAioHandle) -> usize {
    let ctl = crate::pgaio_ctl();
    let base = ctl.io_handles.as_ptr();
    let p = handle as *const PgAioHandle;
    // SAFETY: `handle` is always `&pgaio_ctl->io_handles[idx]`, so the pointer
    // difference is the array index.
    let idx = unsafe { p.offset_from(base) };
    debug_assert!(idx >= 0 && (idx as usize) < ctl.io_handles.len());
    idx as usize
}
