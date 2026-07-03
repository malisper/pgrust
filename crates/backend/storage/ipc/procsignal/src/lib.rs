#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::Cell;
use std::sync::atomic::{
    fence, AtomicBool, AtomicI32, AtomicU32, AtomicU64,
    Ordering::{Acquire, Relaxed, Release, SeqCst},
};
use std::sync::OnceLock;

use ::elog::{elog, ereport};
use init_small::globals as g;
use types_core::{ProcNumber, INVALID_PROC_NUMBER, MAX_CANCEL_KEY_LENGTH};
use types_error::{ErrorLocation, PgResult, DEBUG1, DEBUG2, ERROR, LOG};
use types_storage::storage::{
    ProcSignalBarrierType, ProcSignalReason, Spinlock, SyncCell, NUM_AUXILIARY_PROCS,
    NUM_PROCSIGNALS,
};

// PG_WAIT_IPC | PROC_SIGNAL_BARRIER's index in wait_event_names.txt's IPC section.
const WAIT_EVENT_PROC_SIGNAL_BARRIER: u32 = 0x0800_0000 | 0x2A;

pub struct ProcSignalSlot {
    pss_pid: AtomicI32,
    // [MUTEX] cancel-key fields are protected by pss_mutex.
    pss_cancel_key_len: SyncCell<i32>,
    pss_cancel_key: SyncCell<[u8; MAX_CANCEL_KEY_LENGTH]>,
    pss_signalFlags: [AtomicBool; NUM_PROCSIGNALS],
    // kill(pid,sig)'s thread rendering: bit = signo, drained by the owner
    // thread (no C counterpart; the kernel's pending-signal set).
    pss_pendingThreadSignals: AtomicU32,
    pss_mutex: Spinlock,

    pss_barrierGeneration: AtomicU64,
    pss_barrierCheckMask: AtomicU32,
    // pss_barrierCV storage is owned by the condition_variable unit, keyed by
    // slot index (condition_variable_seams::proc_signal_barrier_cv_*).
}

const _: () = assert!(!core::mem::needs_drop::<ProcSignalSlot>());

impl ProcSignalSlot {
    fn unused() -> ProcSignalSlot {
        ProcSignalSlot {
            pss_pid: AtomicI32::new(0),
            pss_cancel_key_len: SyncCell::new(0),
            pss_cancel_key: SyncCell::new([0; MAX_CANCEL_KEY_LENGTH]),
            pss_signalFlags: std::array::from_fn(|_| AtomicBool::new(false)),
            pss_pendingThreadSignals: AtomicU32::new(0),
            pss_mutex: Spinlock::new(),
            pss_barrierGeneration: AtomicU64::new(u64::MAX),
            pss_barrierCheckMask: AtomicU32::new(0),
        }
    }
}

struct ProcSignalHeader {
    psh_barrierGeneration: AtomicU64,
    psh_slot: &'static [ProcSignalSlot],
}

static PROC_SIGNAL: OnceLock<ProcSignalHeader> = OnceLock::new();

thread_local! {
    static MY_PROC_SIGNAL_SLOT: Cell<Option<usize>> = const { Cell::new(None) };
}

fn proc_signal() -> &'static ProcSignalHeader {
    PROC_SIGNAL
        .get()
        .unwrap_or_else(|| panic!("ProcSignal not initialized (ProcSignalShmemInit not called)"))
}

fn spin_acquire(lock: &Spinlock) {
    if lock.tas() != 0 {
        let mut delay = s_lock_seams::SpinDelayStatus::new(file!(), line!() as i32, "pss_mutex");
        while lock.tas_spin() != 0 {
            s_lock_seams::perform_spin_delay::call(&mut delay);
        }
        s_lock_seams::finish_spin_delay::call(&delay);
    }
}

fn NumProcSignalSlots() -> i32 {
    g::MaxBackends() + NUM_AUXILIARY_PROCS
}

pub fn ProcSignalShmemSize() -> PgResult<usize> {
    let size = shmem_seams::mul_size::call(
        NumProcSignalSlots() as usize,
        core::mem::size_of::<ProcSignalSlot>(),
    )?;
    shmem_seams::add_size::call(size, core::mem::size_of::<AtomicU64>())
}

pub fn ProcSignalShmemInit() {
    assert!(g::MaxBackends() > 0, "MaxBackends not initialized");
    PROC_SIGNAL.get_or_init(|| ProcSignalHeader {
        psh_barrierGeneration: AtomicU64::new(0),
        psh_slot: (0..NumProcSignalSlots() as usize)
            .map(|_| ProcSignalSlot::unused())
            .collect::<Vec<_>>()
            .leak(),
    });
}

fn loc(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("procsignal.c", 0, funcname)
}

pub fn ProcSignalInit(cancel_key: &[u8]) -> PgResult<()> {
    debug_assert!(cancel_key.len() <= MAX_CANCEL_KEY_LENGTH);
    let my_proc_number = g::MyProcNumber();
    if my_proc_number < 0 {
        return elog(ERROR, "MyProcNumber not set");
    }
    let header = proc_signal();
    if my_proc_number as usize >= header.psh_slot.len() {
        return elog(
            ERROR,
            format!(
                "unexpected MyProcNumber {} in ProcSignalInit (max {})",
                my_proc_number,
                header.psh_slot.len()
            ),
        );
    }
    let slot = &header.psh_slot[my_proc_number as usize];

    spin_acquire(&slot.pss_mutex);
    let old_pss_pid = slot.pss_pid.load(Relaxed);
    for flag in &slot.pss_signalFlags {
        flag.store(false, Relaxed);
    }
    // Brand-new process: adopt the latest generation, discard stale bits.
    slot.pss_pendingThreadSignals.store(0, Relaxed);
    slot.pss_barrierCheckMask.store(0, Relaxed);
    let barrier_generation = header.psh_barrierGeneration.load(Relaxed);
    slot.pss_barrierGeneration.store(barrier_generation, Relaxed);
    if !cancel_key.is_empty() {
        let mut key = slot.pss_cancel_key.get();
        key[..cancel_key.len()].copy_from_slice(cancel_key);
        slot.pss_cancel_key.set(key);
    }
    slot.pss_cancel_key_len.set(cancel_key.len() as i32);
    slot.pss_pid.store(g::MyProcPid(), Relaxed);
    slot.pss_mutex.unlock();

    if old_pss_pid != 0 {
        elog(
            LOG,
            format!(
                "process {} taking over ProcSignal slot {}, but it's not empty",
                g::MyProcPid(),
                my_proc_number
            ),
        )?;
    }

    MY_PROC_SIGNAL_SLOT.set(Some(my_proc_number as usize));
    // Every C ProcSignalInit caller pqsignals SIGUSR1 to this handler; the
    // default here covers mains that predate pqsignal_thread registration.
    THREAD_SIGNAL_HANDLERS.with(|t| {
        let mut handlers = t.get();
        if matches!(handlers[libc::SIGUSR1 as usize], ThreadSignalHandler::Unset) {
            handlers[libc::SIGUSR1 as usize] =
                ThreadSignalHandler::Simple(procsignal_sigusr1_handler);
            t.set(handlers);
        }
    });
    ipc_seams::on_shmem_exit::call(CleanupProcSignalState, 0);
    Ok(())
}

pub const NUM_THREAD_SIGNALS: usize = 32;

#[derive(Clone, Copy)]
pub enum ThreadSignalHandler {
    Unset,
    Ignore,
    Simple(fn()),
    Fallible(fn() -> PgResult<()>),
}

thread_local! {
    static THREAD_SIGNAL_HANDLERS: Cell<[ThreadSignalHandler; NUM_THREAD_SIGNALS]> = const {
        assert!(!core::mem::needs_drop::<[ThreadSignalHandler; NUM_THREAD_SIGNALS]>());
        Cell::new([ThreadSignalHandler::Unset; NUM_THREAD_SIGNALS])
    };
}

fn thread_signal_bit(signo: i32) -> u32 {
    assert!(
        signo > 0 && (signo as usize) < NUM_THREAD_SIGNALS,
        "thread signal {signo} out of range"
    );
    1u32 << signo as u32
}

// pqsignal (port/pqsignal.c) for the thread model: dispositions are the
// registering thread's, run by DrainThreadSignals on that thread.
pub fn pqsignal_thread(signo: i32, handler: ThreadSignalHandler) {
    let bit = thread_signal_bit(signo);
    debug_assert!(bit != 0);
    THREAD_SIGNAL_HANDLERS.with(|t| {
        let mut handlers = t.get();
        handlers[signo as usize] = handler;
        t.set(handlers);
    });
}

// kill(pid, signo)'s thread rendering: pend signo on the target's slot and
// wake its procLatch; the target's next drain point runs its registered
// disposition. Contract kept: 0 on match, -1 + errno=ESRCH otherwise.
pub fn SendThreadSignal(pid: i32, signo: i32) -> i32 {
    if signo == libc::SIGKILL || signo == libc::SIGSTOP {
        panic!(
            "SendThreadSignal: signal {signo} has no thread rendering \
             (postmaster SIGKILL-escalation redesign)"
        );
    }
    let bit = thread_signal_bit(signo);
    if pid <= 0 {
        // kill(-pid) process-group fanout: callers signal each member.
        set_errno(libc::ESRCH);
        return -1;
    }
    let header = proc_signal();
    for i in (0..header.psh_slot.len()).rev() {
        let slot = &header.psh_slot[i];
        if slot.pss_pid.load(Relaxed) == pid {
            spin_acquire(&slot.pss_mutex);
            if slot.pss_pid.load(Relaxed) == pid {
                slot.pss_pendingThreadSignals.fetch_or(bit, SeqCst);
                slot.pss_mutex.unlock();
                latch::set_latch(&lmgr_proc::GetPGProcByNumber(i as ProcNumber).procLatch);
                return 0;
            }
            slot.pss_mutex.unlock();
        }
    }
    set_errno(libc::ESRCH);
    -1
}

pub fn DrainThreadSignals() -> PgResult<()> {
    let Some(index) = MY_PROC_SIGNAL_SLOT.get() else {
        return Ok(());
    };
    let slot = &proc_signal().psh_slot[index];
    if slot.pss_pendingThreadSignals.load(Acquire) == 0 {
        return Ok(());
    }
    let mut bits = slot.pss_pendingThreadSignals.swap(0, SeqCst);
    let handlers = THREAD_SIGNAL_HANDLERS.with(Cell::get);
    while bits != 0 {
        let signo = bits.trailing_zeros() as usize;
        bits &= bits - 1;
        let result = match handlers[signo] {
            ThreadSignalHandler::Ignore => Ok(()),
            ThreadSignalHandler::Simple(f) => {
                f();
                Ok(())
            }
            ThreadSignalHandler::Fallible(f) => f(),
            ThreadSignalHandler::Unset => panic!(
                "thread signal {signo} delivered to pid {} with no pqsignal_thread \
                 disposition — its main must install its C pqsignal set at entry \
                 (aux-mains handoff)",
                g::MyProcPid()
            ),
        };
        if let Err(e) = result {
            // Undelivered signos stay pending, as blocked signals do in C.
            if bits != 0 {
                slot.pss_pendingThreadSignals.fetch_or(bits, SeqCst);
            }
            return Err(e);
        }
    }
    Ok(())
}

fn CleanupProcSignalState(_code: i32, _arg: usize) {
    // Clear first so a signal arriving after this point ignores the slot.
    let slot_index = MY_PROC_SIGNAL_SLOT
        .get()
        .expect("CleanupProcSignalState called without a ProcSignal slot");
    MY_PROC_SIGNAL_SLOT.set(None);
    let slot = &proc_signal().psh_slot[slot_index];
    let my_pid = g::MyProcPid();

    spin_acquire(&slot.pss_mutex);
    let old_pid = slot.pss_pid.load(Relaxed);
    if old_pid != my_pid {
        slot.pss_mutex.unlock();
        let _ = elog(
            LOG,
            format!(
                "process {my_pid} releasing ProcSignal slot {slot_index}, but it contains {old_pid}"
            ),
        );
        return;
    }
    slot.pss_pid.store(0, Relaxed);
    slot.pss_cancel_key_len.set(0);
    // Look absorbed-of-everything so no barrier wait blocks on this slot.
    slot.pss_barrierGeneration.store(u64::MAX, Relaxed);
    slot.pss_mutex.unlock();

    // CV unit unported => no sleeper can exist; the uninstalled skip is C's
    // no-waiter broadcast no-op.
    if condition_variable_seams::proc_signal_barrier_cv_broadcast::is_installed() {
        condition_variable_seams::proc_signal_barrier_cv_broadcast::call(slot_index as i32);
    }
}

// C's kill(pid, SIGUSR1). One backend = one thread: the sender cannot run the
// drain (target thread-locals), so pend SIGUSR1 and set the target's procLatch
// (slot index == ProcNumber); the target's drain point runs
// procsignal_sigusr1_handler when it wakes.
fn deliver_sigusr1(slot_index: usize) {
    proc_signal().psh_slot[slot_index]
        .pss_pendingThreadSignals
        .fetch_or(thread_signal_bit(libc::SIGUSR1), SeqCst);
    latch::set_latch(&lmgr_proc::GetPGProcByNumber(slot_index as ProcNumber).procLatch);
}

pub fn SendProcSignal(pid: i32, reason: ProcSignalReason, procNumber: ProcNumber) -> i32 {
    let header = proc_signal();

    if procNumber != INVALID_PROC_NUMBER {
        debug_assert!((procNumber as usize) < header.psh_slot.len());
        let slot = &header.psh_slot[procNumber as usize];
        spin_acquire(&slot.pss_mutex);
        if slot.pss_pid.load(Relaxed) == pid {
            slot.pss_signalFlags[reason as usize].store(true, Release);
            slot.pss_mutex.unlock();
            deliver_sigusr1(procNumber as usize);
            return 0;
        }
        slot.pss_mutex.unlock();
    } else {
        // Search back to front: likely targets (aux procs) sit near the end.
        for i in (0..header.psh_slot.len()).rev() {
            let slot = &header.psh_slot[i];
            if slot.pss_pid.load(Relaxed) == pid {
                spin_acquire(&slot.pss_mutex);
                if slot.pss_pid.load(Relaxed) == pid {
                    slot.pss_signalFlags[reason as usize].store(true, Release);
                    slot.pss_mutex.unlock();
                    deliver_sigusr1(i);
                    return 0;
                }
                slot.pss_mutex.unlock();
            }
        }
    }

    set_errno(libc::ESRCH);
    -1
}

pub fn EmitProcSignalBarrier(barrier_type: ProcSignalBarrierType) -> u64 {
    let flagbit: u32 = 1 << (barrier_type as u32);
    let header = proc_signal();

    // SeqCst RMWs preserve C's pg_atomic full-barrier semantics: the flag
    // sets, the generation bump, and the caller's prior stores stay ordered.
    for slot in header.psh_slot {
        slot.pss_barrierCheckMask.fetch_or(flagbit, SeqCst);
    }
    let generation = header.psh_barrierGeneration.fetch_add(1, SeqCst) + 1;

    for i in (0..header.psh_slot.len()).rev() {
        let slot = &header.psh_slot[i];
        if slot.pss_pid.load(Relaxed) != 0 {
            spin_acquire(&slot.pss_mutex);
            let pid = slot.pss_pid.load(Relaxed);
            if pid != 0 {
                slot.pss_signalFlags[ProcSignalReason::PROCSIG_BARRIER as usize]
                    .store(true, Release);
                slot.pss_mutex.unlock();
                deliver_sigusr1(i);
            } else {
                slot.pss_mutex.unlock();
            }
        }
    }

    generation
}

pub fn WaitForProcSignalBarrier(generation: u64) -> PgResult<()> {
    let header = proc_signal();
    debug_assert!(generation <= header.psh_barrierGeneration.load(Relaxed));

    elog(
        DEBUG1,
        format!("waiting for all backends to process ProcSignalBarrier generation {generation}"),
    )?;

    for i in (0..header.psh_slot.len()).rev() {
        let slot = &header.psh_slot[i];
        // Check only pss_barrierGeneration: check-mask bits clear before the
        // barrier is absorbed, the generation advances only after.
        let mut oldval = slot.pss_barrierGeneration.load(Relaxed);
        while oldval < generation {
            if condition_variable_seams::proc_signal_barrier_cv_timed_sleep::call(
                i as i32,
                5000,
                WAIT_EVENT_PROC_SIGNAL_BARRIER,
            )? {
                ereport(LOG)
                    .errmsg(format!(
                        "still waiting for backend with PID {} to accept ProcSignalBarrier",
                        slot.pss_pid.load(Relaxed)
                    ))
                    .finish(loc("WaitForProcSignalBarrier"))?;
            }
            oldval = slot.pss_barrierGeneration.load(Relaxed);
        }
        condition_variable_seams::condition_variable_cancel_sleep::call();
    }

    elog(
        DEBUG1,
        format!(
            "finished waiting for all backends to process ProcSignalBarrier generation {generation}"
        ),
    )?;

    // pg_memory_barrier(): separate the unlocked generation reads from the
    // caller's subsequent shared-state access.
    fence(SeqCst);
    Ok(())
}

fn HandleProcSignalBarrierInterrupt() {
    g::SetInterruptPending(true);
    g::SetProcSignalBarrierPending(true);
}

pub fn ProcessProcSignalBarrier() -> PgResult<()> {
    debug_assert!(MY_PROC_SIGNAL_SLOT.get().is_some());

    if !g::ProcSignalBarrierPending() {
        return Ok(());
    }
    g::SetProcSignalBarrierPending(false);

    let header = proc_signal();
    let my_index = MY_PROC_SIGNAL_SLOT
        .get()
        .expect("ProcessProcSignalBarrier called without a ProcSignal slot");
    let my_slot = &header.psh_slot[my_index];

    let local_gen = my_slot.pss_barrierGeneration.load(Relaxed);
    let shared_gen = header.psh_barrierGeneration.load(Relaxed);
    debug_assert!(local_gen <= shared_gen);
    if local_gen == shared_gen {
        return Ok(());
    }

    // SeqCst exchange = C's full-barrier pg_atomic_exchange_u32: generation
    // reads above stay ordered before the flag extraction. Bits are cleared
    // BEFORE processing; failures put theirs back (never a late clear race).
    let mut flags = my_slot.pss_barrierCheckMask.swap(0, SeqCst);

    if flags != 0 {
        let mut success = true;
        let result = (|| -> PgResult<()> {
            while flags != 0 {
                let barrier_type = flags.trailing_zeros();
                let processed = if barrier_type
                    == ProcSignalBarrierType::PROCSIGNAL_BARRIER_SMGRRELEASE as u32
                {
                    smgr_seams::process_barrier_smgr_release::call()?
                } else {
                    true
                };
                flags &= !(1u32 << barrier_type);
                if !processed {
                    ResetProcSignalBarrierBits(1u32 << barrier_type);
                    success = false;
                }
            }
            Ok(())
        })();

        if let Err(e) = result {
            // PG_CATCH: `flags` still holds the failing bit; re-arm a retry.
            ResetProcSignalBarrierBits(flags);
            return Err(e);
        }
        if !success {
            return Ok(());
        }
    }

    my_slot.pss_barrierGeneration.store(shared_gen, Release);
    if condition_variable_seams::proc_signal_barrier_cv_broadcast::is_installed() {
        condition_variable_seams::proc_signal_barrier_cv_broadcast::call(my_index as i32);
    }
    Ok(())
}

fn ResetProcSignalBarrierBits(flags: u32) {
    let my_index = MY_PROC_SIGNAL_SLOT
        .get()
        .expect("ResetProcSignalBarrierBits called without a ProcSignal slot");
    proc_signal().psh_slot[my_index]
        .pss_barrierCheckMask
        .fetch_or(flags, SeqCst);
    g::SetProcSignalBarrierPending(true);
    g::SetInterruptPending(true);
}

fn CheckProcSignal(reason: ProcSignalReason) -> bool {
    if let Some(index) = MY_PROC_SIGNAL_SLOT.get() {
        let flag = &proc_signal().psh_slot[index].pss_signalFlags[reason as usize];
        // Don't clear a flag we haven't seen set (reads race senders, as in C).
        if flag.load(Acquire) {
            flag.store(false, Relaxed);
            return true;
        }
    }
    false
}

#[cold]
#[inline(never)]
fn unported_handler(what: &str) -> ! {
    panic!("procsignal reason received but its handler's owner is not ported: {what}");
}

// Allocation-free (signal-dispatch-reachable); each unported arm panics
// loudly rather than dropping the reason.
pub fn procsignal_sigusr1_handler() {
    use ProcSignalReason::*;

    if CheckProcSignal(PROCSIG_CATCHUP_INTERRUPT) {
        sinval_seams::handle_catchup_interrupt::call();
    }
    if CheckProcSignal(PROCSIG_NOTIFY_INTERRUPT) {
        unported_handler("HandleNotifyInterrupt (commands/async.c)");
    }
    if CheckProcSignal(PROCSIG_PARALLEL_MESSAGE) {
        unported_handler("HandleParallelMessageInterrupt (access/transam/parallel.c)");
    }
    if CheckProcSignal(PROCSIG_WALSND_INIT_STOPPING) {
        unported_handler("HandleWalSndInitStopping (replication/walsender.c)");
    }
    if CheckProcSignal(PROCSIG_BARRIER) {
        HandleProcSignalBarrierInterrupt();
    }
    if CheckProcSignal(PROCSIG_LOG_MEMORY_CONTEXT) {
        mcxt_seams::handle_log_memory_context_interrupt::call();
    }
    if CheckProcSignal(PROCSIG_PARALLEL_APPLY_MESSAGE) {
        unported_handler("HandleParallelApplyMessageInterrupt (applyparallelworker.c)");
    }
    for conflict in [
        PROCSIG_RECOVERY_CONFLICT_DATABASE,
        PROCSIG_RECOVERY_CONFLICT_TABLESPACE,
        PROCSIG_RECOVERY_CONFLICT_LOCK,
        PROCSIG_RECOVERY_CONFLICT_SNAPSHOT,
        PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT,
        PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK,
        PROCSIG_RECOVERY_CONFLICT_BUFFERPIN,
    ] {
        if CheckProcSignal(conflict) {
            unported_handler("HandleRecoveryConflictInterrupt (tcop/postgres.c)");
        }
    }

    latch::SetLatch(g::MyLatch().expect("SetLatch(MyLatch): MyLatch is not set"));
}

pub fn SendCancelRequest(backend_pid: i32, cancel_key: &[u8]) {
    if backend_pid == 0 {
        log_never_raises(
            ereport(LOG)
                .errmsg("invalid cancel request with PID 0")
                .finish(loc("SendCancelRequest")),
        );
        return;
    }

    // pss_pid/key reads are racy by design (C accepts the same window).
    let header = proc_signal();
    for slot in header.psh_slot {
        if slot.pss_pid.load(Relaxed) != backend_pid {
            continue;
        }
        spin_acquire(&slot.pss_mutex);
        if slot.pss_pid.load(Relaxed) != backend_pid {
            slot.pss_mutex.unlock();
            continue;
        }
        let key_len = slot.pss_cancel_key_len.get();
        let key = slot.pss_cancel_key.get();
        slot.pss_mutex.unlock();
        let matched = key_len == cancel_key.len() as i32
            && timingsafe_bcmp(&key[..cancel_key.len()], cancel_key) == 0;

        if matched {
            log_never_raises(
                ereport(DEBUG2)
                    .errmsg_internal(format!(
                        "processing cancel request: sending SIGINT to process {backend_pid}"
                    ))
                    .finish(loc("SendCancelRequest")),
            );
            // C: kill(-backendPID, SIGINT); one thread per backend and no
            // parallel workers yet, so the leader is the whole group.
            if SendThreadSignal(backend_pid, libc::SIGINT) < 0 {
                log_never_raises(
                    ereport(LOG)
                        .errmsg(format!(
                            "could not send signal to process {backend_pid}: No such process"
                        ))
                        .finish(loc("SendCancelRequest")),
                );
            }
        } else {
            log_never_raises(
                ereport(LOG)
                    .errmsg(format!("wrong key in cancel request for process {backend_pid}"))
                    .finish(loc("SendCancelRequest")),
            );
        }
        return;
    }

    log_never_raises(
        ereport(LOG)
            .errmsg(format!(
                "PID {backend_pid} in cancel request did not match any process"
            ))
            .finish(loc("SendCancelRequest")),
    );
}

// src/port/timingsafe_bcmp.c (non-OpenSSL arm): constant-time compare.
fn timingsafe_bcmp(b1: &[u8], b2: &[u8]) -> i32 {
    debug_assert_eq!(b1.len(), b2.len());
    let mut ret: i32 = 0;
    for (p1, p2) in b1.iter().zip(b2.iter()) {
        ret |= (p1 ^ p2) as i32;
    }
    (ret != 0) as i32
}

fn set_errno(value: i32) {
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    // SAFETY: __error returns this thread's valid errno location.
    unsafe {
        *libc::__error() = value;
    }
    #[cfg(not(any(target_os = "macos", target_os = "ios")))]
    // SAFETY: __errno_location returns this thread's valid errno location.
    unsafe {
        *libc::__errno_location() = value;
    }
}

fn log_never_raises(result: PgResult<()>) {
    debug_assert!(result.is_ok());
}

pub fn init_seams() {
    procsignal_seams::proc_signal_barrier_pending::set(g::ProcSignalBarrierPending);
    procsignal_seams::process_proc_signal_barrier::set(ProcessProcSignalBarrier);
    procsignal_seams::drain_thread_signals::set(DrainThreadSignals);
}

#[cfg(test)]
mod tests;
