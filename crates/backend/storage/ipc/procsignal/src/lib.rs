#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::Cell;
use std::sync::atomic::{
    fence, AtomicBool, AtomicI32, AtomicU32, AtomicU64, AtomicUsize,
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

// Thread-signal numbers are pure indices/bits here (no kernel signals are
// involved — this crate IS the signal emulation). wasm32: the wasi libc
// crate exposes no SIG* names; Linux/wasi-libc values.
//
// `signums` is public: pqsignal_thread/SendThreadSignal callers (auxjobs,
// tcop) name their dispositions through it so every target agrees on the
// emulation's numbering without each crate re-deriving the wasm arm.
pub mod signums {
    #[cfg(not(target_family = "wasm"))]
    pub use libc::{
        SIGABRT, SIGALRM, SIGCHLD, SIGFPE, SIGHUP, SIGINT, SIGKILL, SIGPIPE, SIGQUIT, SIGSTOP,
        SIGTERM, SIGUSR1, SIGUSR2,
    };
    // Linux-numbered thread-signal emulation space (matches wasi-libc).
    #[cfg(target_family = "wasm")]
    mod wasm_signums {
        pub const SIGHUP: i32 = 1;
        pub const SIGINT: i32 = 2;
        pub const SIGQUIT: i32 = 3;
        pub const SIGABRT: i32 = 6;
        pub const SIGFPE: i32 = 8;
        pub const SIGKILL: i32 = 9;
        pub const SIGUSR1: i32 = 10;
        pub const SIGUSR2: i32 = 12;
        pub const SIGPIPE: i32 = 13;
        pub const SIGALRM: i32 = 14;
        pub const SIGTERM: i32 = 15;
        pub const SIGCHLD: i32 = 17;
        pub const SIGSTOP: i32 = 19;
    }
    #[cfg(target_family = "wasm")]
    pub use wasm_signums::*;
}
use signums::{SIGINT, SIGKILL, SIGSTOP, SIGUSR1};

pub struct ProcSignalSlot {
    pss_pid: AtomicI32,
    // [MUTEX] cancel-key fields are protected by pss_mutex.
    pss_cancel_key_len: SyncCell<i32>,
    pss_cancel_key: SyncCell<[u8; MAX_CANCEL_KEY_LENGTH]>,
    pss_signalFlags: [AtomicBool; NUM_PROCSIGNALS],
    // kill(pid,sig)'s thread rendering: bit = signo, drained by the owner
    // thread (no C counterpart; the kernel's pending-signal set).
    pss_pendingThreadSignals: AtomicU32,
    // Owner's leaked InterruptPending flag: CFI-visible pend delivery.
    pss_interruptFlag: std::sync::atomic::AtomicPtr<AtomicBool>,
    // Extra latch a delivered thread signal must set, beyond the procLatch:
    // renders the wakeup a C signal handler performs itself at delivery time
    // (e.g. the startup process's handlers all call WakeupRecovery(), so a
    // signal interrupts a sleep on recoveryWakeupLatch, not just MyLatch).
    // LatchHandle::as_usize value; valid only while pss_hasExtraWakeLatch.
    pss_extraWakeLatch: AtomicUsize,
    pss_hasExtraWakeLatch: AtomicBool,
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
            pss_interruptFlag: std::sync::atomic::AtomicPtr::new(std::ptr::null_mut()),
            pss_extraWakeLatch: AtomicUsize::new(0),
            pss_hasExtraWakeLatch: AtomicBool::new(false),
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

// ---------------------------------------------------------------------------
// PRE-IDENTITY thread-signal targets (fast-shutdown startup-phase wedge).
//
// deliver_thread_signal routes by ProcSignal slot, and a slot carries a pid
// only from ProcSignalInit — deep into InitPostgres. Every postmaster child
// is signalable by pid BEFORE that (its pmchild slot pid is set at launch/
// dispatch), so a broadcast landing in the window — TerminateChildren's
// SIGTERM at fast shutdown against a backend still reading its startup
// packet — returned ESRCH and was silently dropped. C cannot lose that
// signal (kill() is kernel-queued and runs process_startup_packet_die when
// unblocked); here the drop left the backend parked in secure_read forever:
// PM_WAIT_BACKENDS never drained, the GL-GANGWEDGE-1 watchdog escalated to
// immediate shutdown, and the next start ran crash recovery.
//
// The channel: the LAUNCHER registers `pid -> pending word` BEFORE the pid
// becomes broadcast-visible (postmaster_child_launch / wpool dispatch); the
// child ADOPTS the entry on its own thread (filling in its latch + interrupt
// flag) as soon as it has them; ProcSignalInit consumes the entry, migrating
// any pending bits into the freshly-published slot. Delivery that finds no
// slot falls back here: bits are never lost anywhere in the window.
//
// Drain semantics: a pre-identity thread runs the dispositions it has
// installed; a signo with no disposition yet stays PENDING (C parity — the
// startup sigmask blocks everything a handler hasn't been installed for, and
// pending signals deliver at unblock, i.e. after the main's pqsignal set).
// ---------------------------------------------------------------------------

struct PreIdentityTarget {
    pid: i32,
    bits: std::sync::Arc<AtomicU32>,
    // LatchHandle::as_usize of the owner's latch; 0 until adopted (a
    // not-yet-adopted target is a child that has not parked yet — it drains
    // the word at its first drain point, no wake needed).
    latch_word: AtomicUsize,
    // Owner's leaked InterruptPending flag (CFI-visible pend), null until
    // adopted.
    interrupt_flag: std::sync::atomic::AtomicPtr<AtomicBool>,
}

// pgsync by crate law: locked by the postmaster (launch/deliver) and by
// child threads (adopt/consume) — the CHILD_THREADS precedent.
static PRE_IDENTITY_TARGETS: pgsync::Mutex<Vec<std::sync::Arc<PreIdentityTarget>>> =
    pgsync::Mutex::new(Vec::new());

thread_local! {
    // The calling thread's adopted entry (pid + shared pending word).
    static MY_PRE_IDENTITY: std::cell::RefCell<Option<(i32, std::sync::Arc<AtomicU32>)>> =
        const { std::cell::RefCell::new(None) };
}

fn pre_identity_targets()
-> pgsync::MutexGuard<'static, Vec<std::sync::Arc<PreIdentityTarget>>> {
    PRE_IDENTITY_TARGETS.lock().unwrap_or_else(|e| e.into_inner())
}

/// Launcher side: make `pid` deliverable before the child thread runs (or,
/// for a pooled standby claim, before the task pid becomes broadcast-
/// visible). Idempotent per pid; pids are process-unique (reserve_child_pid).
pub fn PreIdentitySignalPrepare(pid: i32) {
    let mut v = pre_identity_targets();
    if v.iter().any(|e| e.pid == pid) {
        return;
    }
    v.push(std::sync::Arc::new(PreIdentityTarget {
        pid,
        bits: std::sync::Arc::new(AtomicU32::new(0)),
        latch_word: AtomicUsize::new(0),
        interrupt_flag: std::sync::atomic::AtomicPtr::new(std::ptr::null_mut()),
    }));
}

/// Launcher side: the child never came to exist (spawn/dispatch failure).
pub fn PreIdentitySignalDiscard(pid: i32) {
    let mut v = pre_identity_targets();
    if let Some(i) = v.iter().position(|e| e.pid == pid) {
        v.swap_remove(i);
    }
}

/// Child side, on its own thread once MyLatch exists (InitPostmasterChild /
/// the wretain re-key): bind the prepared entry to this thread so delivery
/// can wake it, and remember the pending word for DrainThreadSignals.
/// Also tolerates a missing entry (non-pmchild callers) by creating one.
pub fn PreIdentitySignalAdopt(pid: i32) {
    // A retained-thread re-key adopts a NEW pid; drop any stale binding
    // (its entry was consumed by the previous task's ProcSignalInit).
    let _ = take_pre_identity_registration();
    let entry = {
        let mut v = pre_identity_targets();
        match v.iter().find(|e| e.pid == pid) {
            Some(e) => std::sync::Arc::clone(e),
            None => {
                let e = std::sync::Arc::new(PreIdentityTarget {
                    pid,
                    bits: std::sync::Arc::new(AtomicU32::new(0)),
                    latch_word: AtomicUsize::new(0),
                    interrupt_flag: std::sync::atomic::AtomicPtr::new(std::ptr::null_mut()),
                });
                v.push(std::sync::Arc::clone(&e));
                e
            }
        }
    };
    entry.interrupt_flag.store(
        g::interrupt_pending_flag() as *const _ as *mut AtomicBool,
        Release,
    );
    if let Some(l) = g::MyLatch() {
        entry.latch_word.store(l.as_usize(), Release);
    }
    MY_PRE_IDENTITY.with(|c| {
        *c.borrow_mut() = Some((pid, std::sync::Arc::clone(&entry.bits)));
    });
}

/// Consume the calling thread's registration; returns undrained bits.
fn take_pre_identity_registration() -> u32 {
    let Some((pid, word)) = MY_PRE_IDENTITY.with(|c| c.borrow_mut().take()) else {
        return 0;
    };
    {
        let mut v = pre_identity_targets();
        if let Some(i) = v.iter().position(|e| e.pid == pid) {
            v.swap_remove(i);
        }
    }
    word.swap(0, SeqCst)
}

/// Child side, at any exit path that never reached ProcSignalInit: drop the
/// registration so the registry cannot grow. No-op when already consumed.
pub fn PreIdentitySignalRelease() {
    let _ = take_pre_identity_registration();
}

/// The pre-identity half of DrainThreadSignals: run installed dispositions;
/// keep signos with none PENDING (see the module comment above).
fn drain_pre_identity_signals() -> PgResult<()> {
    let Some(word) =
        MY_PRE_IDENTITY.with(|c| c.borrow().as_ref().map(|(_, w)| std::sync::Arc::clone(w)))
    else {
        return Ok(());
    };
    if word.load(Acquire) == 0 {
        return Ok(());
    }
    let mut bits = word.swap(0, SeqCst);
    let handlers = THREAD_SIGNAL_HANDLERS.with(Cell::get);
    let mut still_blocked: u32 = 0;
    while bits != 0 {
        let signo = bits.trailing_zeros() as usize;
        let bit = 1u32 << signo;
        bits &= bits - 1;
        let result = match handlers[signo] {
            ThreadSignalHandler::Ignore => Ok(()),
            ThreadSignalHandler::Simple(f) => {
                f();
                Ok(())
            }
            ThreadSignalHandler::Fallible(f) => f(),
            // No disposition installed yet: C's startup mask has this signo
            // blocked; it stays pending and migrates into the ProcSignal
            // slot at init, where the main's full pqsignal set handles it.
            ThreadSignalHandler::Unset => {
                still_blocked |= bit;
                Ok(())
            }
        };
        if let Err(e) = result {
            if bits | still_blocked != 0 {
                word.fetch_or(bits | still_blocked, SeqCst);
            }
            return Err(e);
        }
    }
    if still_blocked != 0 {
        word.fetch_or(still_blocked, SeqCst);
    }
    Ok(())
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

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

/// Current shared barrier generation. Retention (wretain) records this at
/// park; a claim-time advance means barrier work was emitted while the
/// thread's slot was released and must be applied before reuse.
pub fn SharedBarrierGeneration() -> u64 {
    proc_signal().psh_barrierGeneration.load(Relaxed)
}

/// Crash-cycle reset in place to the post-ProcSignalShmemInit image
/// (notes/crash-restart-design.md); postmaster thread only, all children dead.
pub fn ProcSignalShmemResetAfterCrash() {
    let header = proc_signal();
    header.psh_barrierGeneration.store(0, Relaxed);
    for slot in header.psh_slot {
        slot.pss_pid.store(0, Relaxed);
        slot.pss_cancel_key_len.set(0);
        slot.pss_cancel_key.set([0; MAX_CANCEL_KEY_LENGTH]);
        for flag in &slot.pss_signalFlags {
            flag.store(false, Relaxed);
        }
        slot.pss_pendingThreadSignals.store(0, Relaxed);
        slot.pss_interruptFlag.store(std::ptr::null_mut(), Relaxed);
        slot.pss_hasExtraWakeLatch.store(false, Relaxed);
        slot.pss_mutex.unlock();
        slot.pss_barrierGeneration.store(u64::MAX, Relaxed);
        slot.pss_barrierCheckMask.store(0, Relaxed);
    }
    if condition_variable_seams::proc_signal_barrier_cvs_reset_after_crash::is_installed() {
        condition_variable_seams::proc_signal_barrier_cvs_reset_after_crash::call();
    }
}

pub fn ProcSignalInit(cancel_key: &[u8]) -> PgResult<()> {
    proc_signal_init_internal(cancel_key, true)
}

/// M2 pool-binding: a STANDING runtime executor re-takes its slot per
/// engagement WITHOUT re-registering the exit callback — its first
/// (connect-time) ProcSignalInit registered once, the thread never
/// proc_exits between engagements, and a per-call registration would grow
/// the exit-callback stack unboundedly over the query stream. Every
/// per-task backend keeps using `ProcSignalInit` (register per task; the
/// task's proc_exit drain consumes it — the wpool retained-thread
/// contract).
pub fn ProcSignalReinitStanding(cancel_key: &[u8]) -> PgResult<()> {
    proc_signal_init_internal(cancel_key, false)
}

fn proc_signal_init_internal(cancel_key: &[u8], register_cleanup: bool) -> PgResult<()> {
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
    slot.pss_hasExtraWakeLatch.store(false, Relaxed);
    slot.pss_interruptFlag.store(
        g::interrupt_pending_flag() as *const _ as *mut AtomicBool,
        Relaxed,
    );
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

    // Identity is now published: consume this thread's pre-identity target
    // and migrate any bits a broadcast pended while we had no slot. Order
    // matters — pid first, then the takedown — so a concurrent delivery
    // always finds at least one of the two channels (double delivery merges
    // by OR; a lost signal cannot happen).
    let pre_identity_pending = take_pre_identity_registration();
    if pre_identity_pending != 0 {
        slot.pss_pendingThreadSignals.fetch_or(pre_identity_pending, SeqCst);
    }

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
        if matches!(handlers[SIGUSR1 as usize], ThreadSignalHandler::Unset) {
            handlers[SIGUSR1 as usize] =
                ThreadSignalHandler::Simple(procsignal_sigusr1_handler);
            t.set(handlers);
        }
    });
    if register_cleanup {
        ipc_seams::on_shmem_exit::call(CleanupProcSignalState, 0);
    }
    Ok(())
}

/// M2 pool-binding: a STANDING runtime executor releases its ProcSignal
/// slot while PARKED (re-initing per engagement): a live slot whose owner
/// never drains signals wedges every WaitForProcSignalBarrier — e.g. the
/// SMGRRELEASE barrier early in DROP DATABASE. A released slot reads as
/// absorbed-of-everything (pss_barrierGeneration = u64::MAX). No-op
/// without a slot.
pub fn ProcSignalRelease() {
    if MY_PROC_SIGNAL_SLOT.get().is_some() {
        CleanupProcSignalState(0, 0);
    }
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

/// M4 bgjobs multi-job seat (docs/design/m4-bgjobs.md §3.2): one dispatcher
/// thread hosts several migrated daemons' signal planes. A job's
/// thread-signal identity — the procsignal slot registration
/// (MY_PROC_SIGNAL_SLOT; the SHARED pending word is already per-job via
/// pss_pid) plus the per-thread handler table (the tables differ between
/// daemons: e.g. bgwriter SIGINT=Ignore, walwriter SIGINT=shutdown) — is
/// stashed here while the job is not the thread's bound identity.
pub struct ThreadSignalIdentity {
    slot: Option<usize>,
    handlers: [ThreadSignalHandler; NUM_THREAD_SIGNALS],
}

impl ThreadSignalIdentity {
    pub const fn unbound() -> ThreadSignalIdentity {
        ThreadSignalIdentity {
            slot: None,
            handlers: [ThreadSignalHandler::Unset; NUM_THREAD_SIGNALS],
        }
    }
}

impl Default for ThreadSignalIdentity {
    fn default() -> Self {
        ThreadSignalIdentity::unbound()
    }
}

/// Exchange the calling thread's signal identity with `saved`.
/// Self-inverse: bind and unbind are the same call (the seat swap
/// discipline — LIFO, RAII-guarded by the caller).
pub fn swap_thread_signal_identity(saved: &mut ThreadSignalIdentity) {
    let cur = MY_PROC_SIGNAL_SLOT.get();
    MY_PROC_SIGNAL_SLOT.set(saved.slot);
    saved.slot = cur;
    THREAD_SIGNAL_HANDLERS.with(|t| {
        let cur = t.get();
        t.set(saved.handlers);
        saved.handlers = cur;
    });
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
    if signo == SIGKILL {
        // The postmaster's ServerLoop SIGKILL escalation (and TerminateChildren
        // during immediate shutdown). No thread can be force-killed; deliver
        // the crash-test kill-9 bit instead — threads with the kill9
        // disposition (backends under postmaster) exit as if killed, matching
        // C's "terminated by signal 9" reap. A thread wedged past its drain
        // points stays wedged, which is the honest single-process limit.
        return SendThreadKill(pid);
    }
    if signo == SIGSTOP {
        panic!("SendThreadSignal: SIGSTOP has no thread rendering");
    }
    deliver_thread_signal(pid, thread_signal_bit(signo))
}

/// Register (or clear) an extra latch that any thread signal delivered to the
/// CURRENT thread must set in addition to its procLatch. Renders the latch
/// wakeups C signal handlers perform at delivery time; the startup process
/// registers recoveryWakeupLatch for the span it can sleep on it.
pub fn SetThreadSignalExtraWakeLatch(latch: Option<types_storage::latch::LatchHandle>) {
    let Some(index) = MY_PROC_SIGNAL_SLOT.get() else {
        return;
    };
    let slot = &proc_signal().psh_slot[index];
    match latch {
        Some(h) => {
            slot.pss_extraWakeLatch.store(h.as_usize(), Relaxed);
            slot.pss_hasExtraWakeLatch.store(true, Release);
        }
        None => slot.pss_hasExtraWakeLatch.store(false, Release),
    }
}

// SIGKILL's crash-test rendering: pend the bit past SendThreadSignal's
// refusal. Callable only from the crash-injection surface — the target's
// SIGKILL disposition dies abruptly (ipc::exit_thread_killed), it is not a
// no-handler process kill.
pub fn SendThreadKill(pid: i32) -> i32 {
    deliver_thread_signal(pid, thread_signal_bit(SIGKILL))
}

fn deliver_thread_signal(pid: i32, bit: u32) -> i32 {
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
                let flag = slot.pss_interruptFlag.load(Relaxed);
                if !flag.is_null() {
                    // SAFETY: only ever a Box::leak'd 'static (never freed).
                    unsafe { &*flag }.store(true, SeqCst);
                }
                slot.pss_mutex.unlock();
                latch::set_latch(&lmgr_proc::GetPGProcByNumber(i as ProcNumber).procLatch);
                // C's handler runs at delivery and may set further latches
                // itself (startup: WakeupRecovery()); the target's registered
                // extra wake latch is that handler-side wakeup, without which
                // a thread sleeping on a non-proc latch never reaches a drain
                // point (048 reload; 030/032/033 shutdown wedges).
                if slot.pss_hasExtraWakeLatch.load(Acquire) {
                    latch::SetLatch(types_storage::latch::LatchHandle::from_raw(
                        slot.pss_extraWakeLatch.load(Relaxed),
                    ));
                }
                return 0;
            }
            slot.pss_mutex.unlock();
        }
    }
    // Pre-identity fallback (see PRE_IDENTITY_TARGETS): the target has no
    // ProcSignal slot yet but the launcher made its pid deliverable. Pend
    // the bit on the shared word and wake whatever the child has bound so
    // far; an unadopted entry has no latch, and that is fine — the child is
    // still running its prelude and drains the word at its first drain
    // point before it can park.
    let found = {
        let v = pre_identity_targets();
        v.iter().find(|e| e.pid == pid).map(|e| {
            e.bits.fetch_or(bit, SeqCst);
            let flag = e.interrupt_flag.load(Acquire);
            if !flag.is_null() {
                // SAFETY: only ever a leaked 'static (interrupt_pending_flag).
                unsafe { &*flag }.store(true, SeqCst);
            }
            e.latch_word.load(Acquire)
        })
    };
    if let Some(latch_word) = found {
        if latch_word != 0 {
            latch::SetLatch(types_storage::latch::LatchHandle::from_raw(latch_word));
        }
        return 0;
    }
    set_errno(libc::ESRCH);
    -1
}

pub fn DrainThreadSignals() -> PgResult<()> {
    let Some(index) = MY_PROC_SIGNAL_SLOT.get() else {
        // No slot yet: drain the pre-identity word instead (the fast-
        // shutdown SIGTERM against a startup-phase backend lands there).
        return drain_pre_identity_signals();
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
    // Tolerate an already-released slot: a standing runtime executor
    // (parallel::standing) releases at every park, so its thread-exit
    // callback usually finds nothing to do.
    let Some(slot_index) = MY_PROC_SIGNAL_SLOT.get() else {
        return;
    };
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
    slot.pss_interruptFlag.store(std::ptr::null_mut(), Relaxed);
    slot.pss_hasExtraWakeLatch.store(false, Relaxed);
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
        .fetch_or(thread_signal_bit(SIGUSR1), SeqCst);
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
        async_seams::handle_notify_interrupt::call();
    }
    if CheckProcSignal(PROCSIG_PARALLEL_MESSAGE) {
        parallel_seams::handle_parallel_message_interrupt::call();
    }
    if CheckProcSignal(PROCSIG_WALSND_INIT_STOPPING) {
        if walsender_seams::handle_walsnd_init_stopping::is_installed() {
            walsender_seams::handle_walsnd_init_stopping::call();
        } else {
            unported_handler("HandleWalSndInitStopping (replication/walsender.c)");
        }
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
            if postgres_seams::handle_recovery_conflict_interrupt::is_installed() {
                postgres_seams::handle_recovery_conflict_interrupt::call(conflict as u32);
            } else {
                unported_handler("HandleRecoveryConflictInterrupt (tcop/postgres.c)");
            }
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
            if SendThreadSignal(backend_pid, SIGINT) < 0 {
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

fn errno() -> i32 {
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    // SAFETY: __error returns this thread's valid errno location.
    unsafe {
        *libc::__error()
    }
    #[cfg(not(any(target_os = "macos", target_os = "ios")))]
    // SAFETY: __errno_location returns this thread's valid errno location.
    unsafe {
        *libc::__errno_location()
    }
}

fn send_thread_signal_errno(pid: i32, signo: i32) -> i32 {
    if SendThreadSignal(pid, signo) < 0 {
        errno()
    } else {
        0
    }
}

pub fn init_seams() {
    procsignal_seams::proc_signal_barrier_pending::set(g::ProcSignalBarrierPending);
    procsignal_seams::process_proc_signal_barrier::set(ProcessProcSignalBarrier);
    procsignal_seams::drain_thread_signals::set(DrainThreadSignals);
    procsignal_seams::send_thread_signal::set(send_thread_signal_errno);
    procsignal_seams::set_thread_signal_extra_wake_latch::set(|raw| {
        SetThreadSignalExtraWakeLatch(raw.map(types_storage::latch::LatchHandle::from_raw))
    });
    procsignal_seams::send_proc_signal::set(SendProcSignal);
}

#[cfg(test)]
mod tests;
