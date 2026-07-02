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
fn init_registers_and_cleanup_releases() {
    setup();
    let _guard = serial();
    register(3, 1003, &[7, 8, 9]);

    let s = slot(3);
    assert_eq!(s.pss_pid.load(Relaxed), 1003);
    assert_eq!(s.pss_cancel_key_len.get(), 3);
    assert_eq!(&s.pss_cancel_key.get()[..3], &[7, 8, 9]);
    assert_eq!(
        s.pss_barrierGeneration.load(Relaxed),
        proc_signal().psh_barrierGeneration.load(Relaxed)
    );

    s.pss_signalFlags[ProcSignalReason::PROCSIG_CATCHUP_INTERRUPT as usize].store(true, Release);
    ProcSignalInit(&[]).unwrap();
    assert!(!s.pss_signalFlags[ProcSignalReason::PROCSIG_CATCHUP_INTERRUPT as usize].load(Acquire));
    assert_eq!(s.pss_cancel_key_len.get(), 0);

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

    let delivery = std::panic::catch_unwind(|| SendCancelRequest(1008, &[1, 2, 3, 4]));
    assert!(delivery.is_err(), "matching key must reach the unported delivery panic");
    cleanup_current();
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
