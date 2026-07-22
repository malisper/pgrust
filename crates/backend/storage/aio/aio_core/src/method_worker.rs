//! method_worker.c: the worker IO method. C's worker PROCESSES are
//! postmaster-child THREADS here (BackendType::IoWorker, pmchild-tracked,
//! PGPROC-owning); the worker slot stores the owner's ProcNumber and wakeups
//! go through its shared procLatch — the same latch C stores a raw pointer
//! to.

use std::sync::atomic::Ordering;

use elog::ereport;
use init_small::globals as g;
use latch::{ResetLatch, SetLatch, WaitLatch};
use lwlock::{LWLockAcquire, LWLockRelease, LW_EXCLUSIVE};
use types_core::ProcNumber;
use types_error::{PgResult, ERROR, LOG};
use types_storage::aio::{PGAIO_HF_REFERENCES_LOCAL, PGAIO_SUBMIT_BATCH_SIZE};
use types_storage::latch::LatchHandle;
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET};

const MAX_IO_WORKERS_USIZE: usize = types_storage::storage::MAX_IO_WORKERS as usize;

use crate::handle::loc;
use crate::{ioh, AioCell};

const IO_WORKER_WAKEUP_FANOUT: usize = 2;
const IO_WORKER_QUEUE_SIZE: usize = 64;

// lwlocklist.h PG_LWLOCK(53, AioWorkerSubmissionQueue); pinned by test.
pub(crate) const AIO_WORKER_SUBMISSION_QUEUE_LOCK: usize = 53;

const PG_WAIT_ACTIVITY: u32 = 0x0500_0000;
const WAIT_EVENT_IO_WORKER_MAIN: u32 = PG_WAIT_ACTIVITY + 6;

const INVALID_PROC: ProcNumber = types_core::INVALID_PROC_NUMBER;

// Ring of staged handle ids + worker registry; all fields below are
// Ring + registry fields are serialized by AioWorkerSubmissionQueueLock
// (C keeps them in two
struct WorkerSlot {
    // C stores Latch*; the procno addresses the same shared procLatch.
    procno: ProcNumber,
    in_use: bool,
}

struct WorkerQueue {
    size: usize,
    head: usize,
    tail: usize,
    sqes: [u32; IO_WORKER_QUEUE_SIZE],
    idle_worker_mask: u64,
    workers: [WorkerSlot; MAX_IO_WORKERS_USIZE],
}

static QUEUE: AioCell<WorkerQueue> = AioCell::new(WorkerQueue {
    size: IO_WORKER_QUEUE_SIZE,
    head: 0,
    tail: 0,
    sqes: [0; IO_WORKER_QUEUE_SIZE],
    idle_worker_mask: 0,
    workers: [const { WorkerSlot { procno: INVALID_PROC, in_use: false } }; MAX_IO_WORKERS_USIZE],
});

/// SAFETY: caller holds AioWorkerSubmissionQueueLock.
#[allow(clippy::mut_from_ref)]
unsafe fn queue() -> &'static mut WorkerQueue {
    &mut *QUEUE.get()
}

fn queue_lock() -> &'static lwlock::LWLock {
    lwlock::main_lock(AIO_WORKER_SUBMISSION_QUEUE_LOCK)
}

pub(crate) fn pgaio_worker_shmem_size() -> usize {
    0
}

pub(crate) fn pgaio_worker_shmem_init(_first_time: bool) -> PgResult<()> {
    Ok(())
}

pub(crate) fn pgaio_worker_shmem_reset_after_crash() {
    // Crash reset runs with all children dead: no lock needed.
    // SAFETY: single-threaded (postmaster crash cycle).
    let q = unsafe { queue() };
    q.head = 0;
    q.tail = 0;
    q.idle_worker_mask = 0;
    for w in q.workers.iter_mut() {
        w.procno = INVALID_PROC;
        w.in_use = false;
    }
}

thread_local! {
    static MY_IO_WORKER_ID: std::cell::Cell<i32> = const { std::cell::Cell::new(-1) };
}

pub fn pgaio_workers_enabled() -> bool {
    crate::io_method() == guc_tables::consts::IOMETHOD_WORKER
}

pub(crate) fn pgaio_worker_needs_synchronous_execution(index: u32) -> bool {
    !g::IsUnderPostmaster()
        || ioh(index).flags.load(Ordering::Relaxed) & PGAIO_HF_REFERENCES_LOCAL != 0
        || !crate::target::pgaio_io_can_reopen(index)
}

// SAFETY comment discipline: insert/consume/depth run under the queue lock.
fn queue_insert(q: &mut WorkerQueue, index: u32) -> bool {
    let new_head = (q.head + 1) & (q.size - 1);
    if new_head == q.tail {
        return false; // full
    }
    q.sqes[q.head] = index;
    q.head = new_head;
    true
}

fn queue_consume(q: &mut WorkerQueue) -> Option<u32> {
    if q.tail == q.head {
        return None;
    }
    let result = q.sqes[q.tail];
    q.tail = (q.tail + 1) & (q.size - 1);
    Some(result)
}

fn queue_depth(q: &WorkerQueue) -> usize {
    let mut head = q.head;
    let tail = q.tail;
    if tail > head {
        head += q.size;
    }
    head - tail
}

fn choose_idle(q: &mut WorkerQueue) -> Option<usize> {
    if q.idle_worker_mask == 0 {
        return None;
    }
    let worker = q.idle_worker_mask.trailing_zeros() as usize;
    q.idle_worker_mask &= !(1u64 << worker);
    debug_assert!(q.workers[worker].in_use);
    Some(worker)
}

pub(crate) fn pgaio_worker_submit(staged: &[u32]) -> PgResult<()> {
    for &index in staged {
        crate::handle::pgaio_io_prepare_submit(index);
    }
    pgaio_worker_submit_internal(staged)
}

fn pgaio_worker_submit_internal(staged: &[u32]) -> PgResult<()> {
    debug_assert!(staged.len() <= PGAIO_SUBMIT_BATCH_SIZE);

    let mut synchronous_ios: [u32; PGAIO_SUBMIT_BATCH_SIZE] = [0; PGAIO_SUBMIT_BATCH_SIZE];
    let mut nsync = 0usize;
    let mut wakeup: Option<ProcNumber> = None;

    LWLockAcquire(queue_lock(), LW_EXCLUSIVE, g::MyProcNumber())?;
    // SAFETY: queue lock held.
    let q = unsafe { queue() };
    for &index in staged {
        debug_assert!(!pgaio_worker_needs_synchronous_execution(index));
        if !queue_insert(q, index) {
            synchronous_ios[nsync] = index;
            nsync += 1;
            continue;
        }
        if wakeup.is_none() {
            if let Some(worker) = choose_idle(q) {
                wakeup = Some(q.workers[worker].procno);
            }
        }
    }
    LWLockRelease(queue_lock())?;

    if let Some(procno) = wakeup {
        SetLatch(LatchHandle::proc(procno));
    }

    for &index in &synchronous_ios[..nsync] {
        crate::io::pgaio_io_perform_synchronously(index);
    }
    Ok(())
}

// Registry-slot release (on_shmem_exit); the executed-IOs witness logs
// here because worker exit goes through proc_exit
// (ProcessMainLoopInterrupts), never past the IoWorkerMain loop tail.
fn pgaio_worker_die(_code: i32, _arg: usize) {
    let _ = elog::elog(
        types_error::DEBUG1,
        format!("io worker executed {} IOs", EXECUTED_IOS.get()),
    );
    let id = MY_IO_WORKER_ID.get();
    debug_assert!(id >= 0);
    LWLockAcquire(queue_lock(), LW_EXCLUSIVE, g::MyProcNumber()).expect("pgaio_worker_die");
    // SAFETY: queue lock held.
    let q = unsafe { queue() };
    debug_assert!(q.workers[id as usize].in_use);
    debug_assert!(q.workers[id as usize].procno == g::MyProcNumber());
    q.idle_worker_mask &= !(1u64 << id);
    q.workers[id as usize].in_use = false;
    q.workers[id as usize].procno = INVALID_PROC;
    LWLockRelease(queue_lock()).expect("pgaio_worker_die");
}

// Per-worker executed-IOs count: the e2e's flowed-through-workers witness.
thread_local! {
    static EXECUTED_IOS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

pub fn pgaio_worker_executed_count() -> u64 {
    EXECUTED_IOS.get()
}

pub fn pgaio_worker_register() -> PgResult<()> {
    MY_IO_WORKER_ID.set(-1);

    LWLockAcquire(queue_lock(), LW_EXCLUSIVE, g::MyProcNumber())?;
    // SAFETY: queue lock held.
    let q = unsafe { queue() };
    let mut my_id: i32 = -1;
    for (i, w) in q.workers.iter_mut().enumerate() {
        if !w.in_use {
            debug_assert!(w.procno == INVALID_PROC);
            w.in_use = true;
            my_id = i as i32;
            break;
        }
    }
    if my_id == -1 {
        LWLockRelease(queue_lock())?;
        ereport(ERROR)
            .errmsg_internal("couldn't find a free worker slot")
            .finish(loc("pgaio_worker_register"))?;
    }
    MY_IO_WORKER_ID.set(my_id);
    q.idle_worker_mask |= 1u64 << my_id;
    q.workers[my_id as usize].procno = g::MyProcNumber();
    LWLockRelease(queue_lock())?;

    ipc_seams::on_shmem_exit::call(pgaio_worker_die, 0);
    Ok(())
}

pub fn pgaio_worker_cycle() -> PgResult<()> {
    let mut latches: [ProcNumber; IO_WORKER_WAKEUP_FANOUT] = [INVALID_PROC; 2];
    let mut nlatches = 0usize;

    // C: the lwlock acquire is the barrier making the consumed handle's
    // fields visible.
    LWLockAcquire(queue_lock(), LW_EXCLUSIVE, g::MyProcNumber())?;
    let io_index;
    {
        // SAFETY: queue lock held.
        let q = unsafe { queue() };
        let my_id = MY_IO_WORKER_ID.get() as usize;
        io_index = queue_consume(q);
        match io_index {
            None => {
                q.idle_worker_mask |= 1u64 << my_id;
            }
            Some(_) => {
                q.idle_worker_mask &= !(1u64 << my_id);
                let nwakeups = queue_depth(q).min(IO_WORKER_WAKEUP_FANOUT);
                for _ in 0..nwakeups {
                    match choose_idle(q) {
                        None => break,
                        Some(worker) => {
                            latches[nlatches] = q.workers[worker].procno;
                            nlatches += 1;
                        }
                    }
                }
            }
        }
    }
    LWLockRelease(queue_lock())?;

    for &procno in &latches[..nlatches] {
        SetLatch(LatchHandle::proc(procno));
    }

    match io_index {
        Some(index) => {
            // C: interrupts held so the reopened fd cannot be closed before
            // execution consumes it.
            g::HoldInterrupts();

            if let Err(reopen_err) = crate::target::pgaio_io_reopen(index) {
                elog::emit_error_report_for(&reopen_err);
                let _ = elog::elog(
                    LOG,
                    format!("io worker: reopen failed for io {index}, failing the IO"),
                );
                g::StartCriticalSection();
                crate::handle::pgaio_io_process_completion(index, -libc::ENOENT);
                g::EndCriticalSection();
            } else {
                crate::io::pgaio_io_perform_synchronously(index);
            }
            EXECUTED_IOS.set(EXECUTED_IOS.get() + 1);
            g::ResumeInterrupts();
        }
        None => {
            WaitLatch(
                g::MyLatch(),
                WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
                -1,
                WAIT_EVENT_IO_WORKER_MAIN,
            )?;
            ResetLatch(g::MyLatch().expect("io worker latch"));
        }
    }

    interrupt::ProcessMainLoopInterrupts()
}
