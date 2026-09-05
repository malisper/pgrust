use super::*;
use std::sync::atomic::AtomicUsize;
use std::sync::{Mutex, Once};
use types_storage::storage::NUM_SPECIAL_WORKER_PROCS;

const MAX_CONNECTIONS: i32 = 4;
const MAX_WORKER_PROCESSES: i32 = 2;
const MAX_BACKENDS: i32 = MAX_CONNECTIONS + 3 + MAX_WORKER_PROCESSES + 2 + NUM_SPECIAL_WORKER_PROCS;

static BROADCASTS: Mutex<Vec<i32>> = Mutex::new(Vec::new());
static SMGR_CALLS: AtomicUsize = AtomicUsize::new(0);
static SMGR_RESULTS: Mutex<Vec<PgResult<bool>>> = Mutex::new(Vec::new());
static EXIT_CALLBACKS: Mutex<Vec<(fn(i32, usize), usize)>> = Mutex::new(Vec::new());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn thread_globals(procno: ProcNumber, pid: i32) {
    g::SetMaxConnections(MAX_CONNECTIONS);
    g::set_max_worker_processes(MAX_WORKER_PROCESSES);
    g::SetMaxBackends(MAX_BACKENDS);
    g::SetMyProcNumber(procno);
    g::SetMyProcPid(pid);
}

fn setup() {
    thread_globals(0, 9000);
    static SETUP: Once = Once::new();
    SETUP.call_once(|| {
        s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
        s_lock_seams::finish_spin_delay::set(|_| {});
        s_lock_seams::set_spins_per_delay::set(|_| {});
        s_lock_seams::update_spins_per_delay::set(|v| v);
        shmem_seams::mul_size::set(|a, b| Ok(a.checked_mul(b).expect("mul_size overflow")));
        shmem_seams::add_size::set(|a, b| Ok(a.checked_add(b).expect("add_size overflow")));
        ipc_seams::on_shmem_exit::set(|f, arg| EXIT_CALLBACKS.lock().unwrap().push((f, arg)));
        pg_sema_seams::pg_semaphore_create::set(|_| {});
        condition_variable_seams::proc_signal_barrier_cv_broadcast::set(|slot| {
            BROADCASTS.lock().unwrap().push(slot);
        });
        condition_variable_seams::proc_signal_barrier_cv_timed_sleep::set(|slot, _, _| {
            panic!("unexpected pss_barrierCV sleep on slot {slot}");
        });
        condition_variable_seams::condition_variable_cancel_sleep::set(|| false);
        smgr_seams::process_barrier_smgr_release::set(|| {
            SMGR_CALLS.fetch_add(1, SeqCst);
            SMGR_RESULTS.lock().unwrap().pop().unwrap_or(Ok(true))
        });

        lmgr_proc::InitProcGlobal(&lmgr_proc::ProcGlobalConfig {
            autovacuum_worker_slots: 3,
            max_wal_senders: 2,
            max_prepared_xacts: 2,
            fastpath_lock_groups_per_backend: 1,
        });
        ProcSignalShmemInit();
        init_seams();
    });
}

fn register(procno: ProcNumber, pid: i32, cancel_key: &[u8]) {
    thread_globals(procno, pid);
    let before = EXIT_CALLBACKS.lock().unwrap().len();
    ProcSignalInit(cancel_key).unwrap();
    assert_eq!(EXIT_CALLBACKS.lock().unwrap().len(), before + 1);
}

fn cleanup_current() {
    let (f, arg) = *EXIT_CALLBACKS.lock().unwrap().last().unwrap();
    f(0, arg);
}

// EmitProcSignalBarrier dirties every slot's check mask (unused slots never
// absorb); scrub so shape tests see the init state.
fn scrub_barrier_masks() {
    for s in proc_signal().psh_slot {
        s.pss_barrierCheckMask.store(0, Relaxed);
    }
}

fn slot(procno: ProcNumber) -> &'static ProcSignalSlot {
    &proc_signal().psh_slot[procno as usize]
}

#[test]
fn shmem_shape_matches_c() {
    setup();
    let _guard = serial();
    let n = (MAX_BACKENDS + NUM_AUXILIARY_PROCS) as usize;
    assert_eq!(proc_signal().psh_slot.len(), n);
    assert_eq!(
        ProcSignalShmemSize().unwrap(),
        n * core::mem::size_of::<ProcSignalSlot>() + 8
    );
    let s = slot((n - 1) as ProcNumber);
    assert_eq!(s.pss_pid.load(Relaxed), 0);
    assert_eq!(s.pss_barrierGeneration.load(Relaxed), u64::MAX);
    assert_eq!(s.pss_barrierCheckMask.load(Relaxed), 0);
}

#[test]
fn reset_after_crash_restores_boot_image() {
    setup();
    let _guard = serial();
    register(2, 1002, &[1, 2, 3]);
    let s = slot(2);
    s.pss_signalFlags[ProcSignalReason::PROCSIG_BARRIER as usize].store(true, Release);
    s.pss_pendingThreadSignals.store(1u32 << libc::SIGQUIT as u32, SeqCst);
    proc_signal().psh_barrierGeneration.store(5, Relaxed);
    s.pss_barrierCheckMask.store(3, Relaxed);

    ProcSignalShmemResetAfterCrash();

    assert_eq!(proc_signal().psh_barrierGeneration.load(Relaxed), 0);
    for s in proc_signal().psh_slot {
        assert_eq!(s.pss_pid.load(Relaxed), 0);
        assert_eq!(unsafe { s.pss_cancel_key_len.get() }, 0);
        assert_eq!(unsafe { s.pss_cancel_key.get() }, [0; MAX_CANCEL_KEY_LENGTH]);
        for flag in &s.pss_signalFlags {
            assert!(!flag.load(Relaxed));
        }
        assert_eq!(s.pss_pendingThreadSignals.load(Relaxed), 0);
        assert!(s.pss_mutex.is_free());
        assert_eq!(s.pss_barrierGeneration.load(Relaxed), u64::MAX);
        assert_eq!(s.pss_barrierCheckMask.load(Relaxed), 0);
    }
    MY_PROC_SIGNAL_SLOT.set(None);
}

#[test]
fn init_registers_and_cleanup_releases() {
    setup();
    let _guard = serial();
    register(3, 1003, &[7, 8, 9]);

    let s = slot(3);
    assert_eq!(s.pss_pid.load(Relaxed), 1003);
    assert_eq!(unsafe { s.pss_cancel_key_len.get() }, 3);
    assert_eq!(&unsafe { s.pss_cancel_key.get() }[..3], &[7, 8, 9]);
    assert_eq!(
        s.pss_barrierGeneration.load(Relaxed),
        proc_signal().psh_barrierGeneration.load(Relaxed)
    );

    s.pss_signalFlags[ProcSignalReason::PROCSIG_CATCHUP_INTERRUPT as usize].store(true, Release);
    ProcSignalInit(&[]).unwrap();
    assert!(!s.pss_signalFlags[ProcSignalReason::PROCSIG_CATCHUP_INTERRUPT as usize].load(Acquire));
    assert_eq!(unsafe { s.pss_cancel_key_len.get() }, 0);

    BROADCASTS.lock().unwrap().clear();
    cleanup_current();
    assert_eq!(s.pss_pid.load(Relaxed), 0);
    assert_eq!(s.pss_barrierGeneration.load(Relaxed), u64::MAX);
    assert_eq!(*BROADCASTS.lock().unwrap(), vec![3]);
}

#[test]
fn send_proc_signal_by_procno_sets_flag_and_latch() {
    setup();
    let _guard = serial();
    register(4, 1004, &[]);

    let target_latch = &lmgr_proc::GetPGProcByNumber(4).procLatch;
    target_latch.is_set.store(0, SeqCst);
    assert_eq!(
        SendProcSignal(1004, ProcSignalReason::PROCSIG_CATCHUP_INTERRUPT, 4),
        0
    );
    assert!(slot(4).pss_signalFlags[ProcSignalReason::PROCSIG_CATCHUP_INTERRUPT as usize]
        .load(Acquire));
    assert_eq!(target_latch.is_set.load(SeqCst), 1);

    assert_eq!(
        SendProcSignal(9999, ProcSignalReason::PROCSIG_CATCHUP_INTERRUPT, 4),
        -1
    );
    cleanup_current();
}

#[test]
fn send_proc_signal_searches_by_pid() {
    setup();
    let _guard = serial();
    register(5, 1005, &[]);

    assert_eq!(
        SendProcSignal(
            1005,
            ProcSignalReason::PROCSIG_NOTIFY_INTERRUPT,
            INVALID_PROC_NUMBER
        ),
        0
    );
    let flag = &slot(5).pss_signalFlags[ProcSignalReason::PROCSIG_NOTIFY_INTERRUPT as usize];
    assert!(flag.load(Acquire));
    flag.store(false, Relaxed);
    assert_eq!(
        SendProcSignal(4242, ProcSignalReason::PROCSIG_NOTIFY_INTERRUPT, INVALID_PROC_NUMBER),
        -1
    );
    cleanup_current();
}

fn own_local_my_latch() {
    let handle = latch::allocate_local_latch();
    g::SetMyLatch(Some(handle));
    latch::InitLatch(handle);
}

#[test]
fn barrier_roundtrip_emit_handle_process_wait() {
    setup();
    let _guard = serial();
    register(6, 1006, &[]);
    own_local_my_latch();
    g::SetInterruptPending(false);
    g::SetProcSignalBarrierPending(false);

    let generation = EmitProcSignalBarrier(ProcSignalBarrierType::PROCSIGNAL_BARRIER_SMGRRELEASE);
    assert_eq!(proc_signal().psh_barrierGeneration.load(Relaxed), generation);
    let s = slot(6);
    assert_eq!(s.pss_barrierCheckMask.load(Relaxed), 1);
    assert!(s.pss_signalFlags[ProcSignalReason::PROCSIG_BARRIER as usize].load(Acquire));

    procsignal_sigusr1_handler();
    assert!(!s.pss_signalFlags[ProcSignalReason::PROCSIG_BARRIER as usize].load(Acquire));
    assert!(g::ProcSignalBarrierPending());
    assert!(g::InterruptPending());
    assert!(latch::latch_ref(g::MyLatch().unwrap()).is_set());

    BROADCASTS.lock().unwrap().clear();
    let calls = SMGR_CALLS.load(SeqCst);
    ProcessProcSignalBarrier().unwrap();
    assert_eq!(SMGR_CALLS.load(SeqCst), calls + 1);
    assert_eq!(s.pss_barrierGeneration.load(Relaxed), generation);
    assert!(!g::ProcSignalBarrierPending());
    assert_eq!(*BROADCASTS.lock().unwrap(), vec![6]);

    WaitForProcSignalBarrier(generation).unwrap();
    scrub_barrier_masks();
    cleanup_current();
}

#[test]
fn barrier_failure_and_error_rearm_the_bits() {
    setup();
    let _guard = serial();
    register(7, 1007, &[]);
    own_local_my_latch();

    let s = slot(7);
    let generation = EmitProcSignalBarrier(ProcSignalBarrierType::PROCSIGNAL_BARRIER_SMGRRELEASE);
    procsignal_sigusr1_handler();

    SMGR_RESULTS.lock().unwrap().push(Ok(false));
    ProcessProcSignalBarrier().unwrap();
    assert_eq!(s.pss_barrierCheckMask.load(Relaxed), 1);
    assert!(g::ProcSignalBarrierPending());
    assert!(s.pss_barrierGeneration.load(Relaxed) < generation);

    SMGR_RESULTS
        .lock()
        .unwrap()
        .push(Err(Box::new(types_error::PgError::new(ERROR, "boom"))));
    assert!(ProcessProcSignalBarrier().is_err());
    assert_eq!(s.pss_barrierCheckMask.load(Relaxed), 1);
    assert!(g::ProcSignalBarrierPending());

    ProcessProcSignalBarrier().unwrap();
    assert_eq!(s.pss_barrierCheckMask.load(Relaxed), 0);
    assert_eq!(s.pss_barrierGeneration.load(Relaxed), generation);
    scrub_barrier_masks();
    cleanup_current();
}

#[test]
fn cancel_request_key_checks() {
    setup();
    let _guard = serial();
    register(8, 1008, &[1, 2, 3, 4]);

    SendCancelRequest(0, &[1, 2, 3, 4]);
    SendCancelRequest(555_555, &[1, 2, 3, 4]);
    SendCancelRequest(1008, &[1, 2, 3, 9]);
    SendCancelRequest(1008, &[1, 2, 3]);
    assert_eq!(slot(8).pss_pid.load(Relaxed), 1008);
    assert_eq!(slot(8).pss_pendingThreadSignals.load(Relaxed), 0);

    pqsignal_thread(libc::SIGINT, ThreadSignalHandler::Simple(observe_sigint));
    OBSERVED_SIGINT.store(false, SeqCst);
    SendCancelRequest(1008, &[1, 2, 3, 4]);
    assert_eq!(
        slot(8).pss_pendingThreadSignals.load(Relaxed),
        1 << libc::SIGINT as u32
    );
    DrainThreadSignals().unwrap();
    assert!(OBSERVED_SIGINT.load(SeqCst));
    assert_eq!(slot(8).pss_pendingThreadSignals.load(Relaxed), 0);
    cleanup_current();
}

static OBSERVED_SIGINT: AtomicBool = AtomicBool::new(false);
static OBSERVED_SIGTERM: AtomicBool = AtomicBool::new(false);

fn observe_sigint() {
    OBSERVED_SIGINT.store(true, SeqCst);
}

fn observe_sigterm() {
    OBSERVED_SIGTERM.store(true, SeqCst);
}

#[test]
fn thread_signal_cross_thread_send_wakes_target_drain_runs_handler() {
    setup();
    let _guard = serial();
    register(10, 1010, &[]);
    pqsignal_thread(libc::SIGTERM, ThreadSignalHandler::Simple(observe_sigterm));
    OBSERVED_SIGTERM.store(false, SeqCst);

    let target_latch = &lmgr_proc::GetPGProcByNumber(10).procLatch;
    target_latch.is_set.store(0, SeqCst);

    let sender = std::thread::spawn(|| {
        assert_eq!(SendThreadSignal(4242, libc::SIGTERM), -1);
        SendThreadSignal(1010, libc::SIGTERM)
    });
    assert_eq!(sender.join().unwrap(), 0);
    assert_eq!(target_latch.is_set.load(SeqCst), 1);

    DrainThreadSignals().unwrap();
    assert!(OBSERVED_SIGTERM.load(SeqCst));
    DrainThreadSignals().unwrap(); /* empty mailbox is a no-op */
    cleanup_current();
}

#[test]
fn send_proc_signal_pends_sigusr1_and_drain_reaches_cfi_flags() {
    setup();
    let _guard = serial();
    register(11, 1011, &[]);
    own_local_my_latch();
    g::SetInterruptPending(false);
    g::SetProcSignalBarrierPending(false);

    assert_eq!(SendProcSignal(1011, ProcSignalReason::PROCSIG_BARRIER, 11), 0);
    assert_eq!(
        slot(11).pss_pendingThreadSignals.load(Relaxed),
        1 << libc::SIGUSR1 as u32
    );

    // ProcSignalInit's default SIGUSR1 disposition runs the C handler.
    DrainThreadSignals().unwrap();
    assert!(g::InterruptPending());
    assert!(g::ProcSignalBarrierPending());
    g::SetInterruptPending(false);
    g::SetProcSignalBarrierPending(false);
    cleanup_current();
}

// FUZZ-ROUND regression (run e7967e5a..-59-13): a SIGTERM against a
// slot-published identity whose thread has not installed a disposition for
// that signo — C's kill() can land at ANY time (pg_terminate_backend,
// TerminateOtherDBBackends), and in C such a signo is still blocked by the
// startup sigmask, so the kernel keeps it pending until the main's pqsignal
// set exists. The drain must pend, not panic; installing the disposition
// later runs it on the next drain, exactly once.
#[test]
fn drain_without_disposition_stays_pending_until_installed() {
    setup();
    let _guard = serial();
    register(12, 1012, &[]);

    assert_eq!(SendThreadSignal(1012, libc::SIGHUP), 0);
    let bit = 1u32 << libc::SIGHUP as u32;
    DrainThreadSignals().unwrap(); /* Unset: blocked, stays pending */
    assert_eq!(slot(12).pss_pendingThreadSignals.load(Relaxed), bit);
    DrainThreadSignals().unwrap(); /* still pending, still no panic */
    assert_eq!(slot(12).pss_pendingThreadSignals.load(Relaxed), bit);

    static OBSERVED_SIGHUP: AtomicUsize = AtomicUsize::new(0);
    OBSERVED_SIGHUP.store(0, SeqCst);
    pqsignal_thread(
        libc::SIGHUP,
        ThreadSignalHandler::Simple(|| {
            OBSERVED_SIGHUP.fetch_add(1, SeqCst);
        }),
    );
    DrainThreadSignals().unwrap(); /* delivered at "unblock", exactly once */
    assert_eq!(OBSERVED_SIGHUP.load(SeqCst), 1);
    assert_eq!(slot(12).pss_pendingThreadSignals.load(Relaxed), 0);

    // Restore the Unset disposition for this thread (table is thread-local
    // but be tidy for single-thread test runners).
    pqsignal_thread(libc::SIGHUP, ThreadSignalHandler::Unset);
    cleanup_current();
}

// The Err path must also carry blocked (Unset) bits back into the slot, not
// drop them: SIGHUP (1, Unset) is scanned before SIGINT (2, failing
// Fallible). The failing signo is consumed (delivered, handler threw); the
// blocked one must still be pending after the error.
#[test]
fn drain_error_keeps_blocked_bits_pending_too() {
    setup();
    let _guard = serial();
    register(14, 1014, &[]);
    pqsignal_thread(libc::SIGHUP, ThreadSignalHandler::Unset);
    pqsignal_thread(
        libc::SIGINT,
        ThreadSignalHandler::Fallible(|| Err(Box::new(types_error::PgError::new(ERROR, "boom")))),
    );

    assert_eq!(SendThreadSignal(1014, libc::SIGHUP), 0);
    assert_eq!(SendThreadSignal(1014, libc::SIGINT), 0);
    assert!(DrainThreadSignals().is_err());
    assert_eq!(
        slot(14).pss_pendingThreadSignals.load(Relaxed),
        1u32 << libc::SIGHUP as u32
    );
    cleanup_current();
}

#[test]
fn drain_error_keeps_undelivered_signals_pending() {
    setup();
    let _guard = serial();
    register(13, 1013, &[]);
    pqsignal_thread(
        libc::SIGINT,
        ThreadSignalHandler::Fallible(|| Err(Box::new(types_error::PgError::new(ERROR, "boom")))),
    );
    pqsignal_thread(libc::SIGTERM, ThreadSignalHandler::Simple(observe_sigterm));
    OBSERVED_SIGTERM.store(false, SeqCst);

    assert_eq!(SendThreadSignal(1013, libc::SIGINT), 0);
    assert_eq!(SendThreadSignal(1013, libc::SIGTERM), 0);
    assert!(DrainThreadSignals().is_err()); /* SIGINT (2) drains first */
    assert!(!OBSERVED_SIGTERM.load(SeqCst));
    assert_eq!(
        slot(13).pss_pendingThreadSignals.load(Relaxed),
        1 << libc::SIGTERM as u32
    );

    DrainThreadSignals().unwrap();
    assert!(OBSERVED_SIGTERM.load(SeqCst));
    cleanup_current();
}

#[test]
fn thread_signal_rejects_unrenderable_signals() {
    setup();
    let _guard = serial();
    assert_eq!(SendThreadSignal(-1010, libc::SIGTERM), -1);
    // SIGKILL renders via SendThreadKill (crash-test kill-9 bit) -- delivered
    // like any pend, so an unknown pid is ESRCH, not a panic.
    assert_eq!(SendThreadSignal(1010, libc::SIGKILL), -1);
    let stop = std::panic::catch_unwind(|| SendThreadSignal(1010, libc::SIGSTOP));
    assert!(stop.is_err());
}

#[test]
fn timingsafe_bcmp_matches_c() {
    assert_eq!(timingsafe_bcmp(&[], &[]), 0);
    assert_eq!(timingsafe_bcmp(&[1, 2, 3], &[1, 2, 3]), 0);
    assert_eq!(timingsafe_bcmp(&[1, 2, 3], &[1, 2, 4]), 1);
    assert_eq!(timingsafe_bcmp(&[0xff, 0], &[0, 0xff]), 1);
}

#[test]
fn seams_installed_and_delegate() {
    setup();
    let _guard = serial();
    assert!(procsignal_seams::proc_signal_barrier_pending::is_installed());
    assert!(procsignal_seams::process_proc_signal_barrier::is_installed());
    register(9, 1009, &[]);

    g::SetProcSignalBarrierPending(true);
    assert!(procsignal_seams::proc_signal_barrier_pending::call());
    g::SetProcSignalBarrierPending(false);
    assert!(!procsignal_seams::proc_signal_barrier_pending::call());
    procsignal_seams::process_proc_signal_barrier::call().unwrap();
    cleanup_current();
}

// upstream 1a9b1cc18e06 (18.6): init adopted psh_barrierGeneration before
// publishing pss_pid, so an emitter could bump and skip the pid-0 slot while
// it kept the older generation (WaitForProcSignalBarrier never returns).
// Invariant once both finish: the slot holds the emitted generation or
// carries PROCSIG_BARRIER. The worker sweeps its start across the bump.
#[test]
fn init_publishes_pid_before_adopting_barrier_generation() {
    use std::sync::atomic::AtomicU32;
    use std::sync::Arc;

    setup();
    let _guard = serial();
    // Last slot: the emitter's reverse scan reads its pid right after the bump.
    let procno = (proc_signal().psh_slot.len() - 1) as ProcNumber;
    let s = slot(procno);
    let flag = &s.pss_signalFlags[ProcSignalReason::PROCSIG_BARRIER as usize];
    const ROUNDS: u32 = 100_000;
    const SWEEP: u32 = 512;
    const KEY: [u8; MAX_CANCEL_KEY_LENGTH] = [7; MAX_CANCEL_KEY_LENGTH];

    // Handshake word: main stores odd (init now), the worker stores the next
    // even (init done), u32::MAX ends the worker.
    let phase = Arc::new(AtomicU32::new(0));
    let worker_phase = Arc::clone(&phase);
    let worker = std::thread::spawn(move || {
        thread_globals(procno, 1050);
        loop {
            let p = worker_phase.load(SeqCst);
            if p == u32::MAX {
                break;
            }
            if p & 1 == 1 {
                for _ in 0..((p / 2) % SWEEP) {
                    std::hint::black_box(());
                }
                ProcSignalReinitStanding(&KEY).unwrap();
                worker_phase.store(p + 1, SeqCst);
            } else {
                std::hint::spin_loop();
            }
        }
        ProcSignalRelease();
    });

    let mut violation = None;
    let mut stalled = None;
    // Bounded wait: a worker panic or a wedged init fails instead of hanging.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    for round in 0..ROUNDS {
        s.pss_pid.store(0, Relaxed);
        s.pss_barrierGeneration.store(u64::MAX, Relaxed);
        flag.store(false, Relaxed);
        phase.store(2 * round + 1, SeqCst);
        let generation =
            EmitProcSignalBarrier(ProcSignalBarrierType::PROCSIGNAL_BARRIER_SMGRRELEASE);
        let mut spins: u32 = 0;
        while phase.load(SeqCst) != 2 * round + 2 {
            spins = spins.wrapping_add(1);
            if spins % 1024 == 0 && (worker.is_finished() || std::time::Instant::now() > deadline)
            {
                stalled = Some(round);
                break;
            }
            std::hint::spin_loop();
        }
        if stalled.is_some() {
            break;
        }
        let adopted = s.pss_barrierGeneration.load(Relaxed);
        if adopted < generation && !flag.load(Acquire) {
            violation = Some((round, adopted, generation));
            break;
        }
    }
    phase.store(u32::MAX, SeqCst);
    if let Some(round) = stalled {
        // A panicked worker's payload resurfaces through the join below.
        assert!(worker.is_finished(), "worker stalled in round {round}");
    }
    worker.join().unwrap();
    assert_eq!(s.pss_pid.load(Relaxed), 0);
    scrub_barrier_masks();
    if let Some((round, adopted, generation)) = violation {
        panic!(
            "round {round}: slot adopted generation {adopted}, generation {generation} \
             was emitted without signalling it"
        );
    }
}

// ---------------------------------------------------------------------------
// audit-18.6 batch b118.
// ---------------------------------------------------------------------------

static B118_LOG: std::sync::Mutex<Vec<(i32, String)>> = std::sync::Mutex::new(Vec::new());

fn b118_capture(err: &types_error::PgError, _output_to_server: &mut bool) {
    B118_LOG.lock().unwrap().push((err.level.0, err.message.clone()));
}

// procsignal.c:791: SendCancelRequest fires kill(-pid, SIGINT) and ignores
// the result; a backend gone between the slot match and the signal produces
// no log line (the "could not send signal" wording is signalfuncs.c's).
#[test]
fn cancel_request_signal_failure_is_silent_like_c() {
    setup();
    let _guard = serial();
    register(8, 1008, &[1, 2, 3, 4]);
    // A pid no thread can carry: SendThreadSignal fails with ESRCH, exactly
    // kill(2) on a process that is already gone.
    slot(8).pss_pid.store(-1008, Relaxed);
    let prev = elog::set_emit_log_hook(Some(b118_capture));
    B118_LOG.lock().unwrap().clear();
    SendCancelRequest(-1008, &[1, 2, 3, 4]);
    elog::set_emit_log_hook(prev);
    slot(8).pss_pid.store(1008, Relaxed);
    let log = std::mem::take(&mut *B118_LOG.lock().unwrap());
    let stray: Vec<_> = log.iter().filter(|(lvl, _)| *lvl == types_error::LOG.0).collect();
    assert!(
        stray.is_empty(),
        "C's SendCancelRequest logs nothing when kill() fails: {stray:?}"
    );
    cleanup_current();
}
