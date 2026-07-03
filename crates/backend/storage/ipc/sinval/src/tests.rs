use super::*;
use init_small::globals as g;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool as StdAtomicBool, AtomicUsize};
use std::sync::{Mutex, Once};
use types_storage::sinval::{SharedInvalRelcacheMsg, SharedInvalSmgrMsg};
use types_storage::storage::NUM_SPECIAL_WORKER_PROCS;
use types_storage::RelFileLocator;

const MAX_CONNECTIONS: i32 = 4;
const MAX_WORKER_PROCESSES: i32 = 2;
const MAX_BACKENDS: i32 = MAX_CONNECTIONS + 3 + MAX_WORKER_PROCESSES + 2 + NUM_SPECIAL_WORKER_PROCS;

static EXIT_CALLBACKS: Mutex<Vec<(fn(i32, usize), usize)>> = Mutex::new(Vec::new());
static ACCEPT_CALLS: AtomicUsize = AtomicUsize::new(0);
static IN_XACT: StdAtomicBool = StdAtomicBool::new(true);

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
        shmem::init_seams();
        s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
        s_lock_seams::finish_spin_delay::set(|_| {});
        s_lock_seams::set_spins_per_delay::set(|_| {});
        s_lock_seams::update_spins_per_delay::set(|v| v);
        ipc_seams::on_shmem_exit::set(|f, arg| EXIT_CALLBACKS.lock().unwrap().push((f, arg)));
        pg_sema_seams::pg_semaphore_create::set(|_| {});
        condition_variable_seams::proc_signal_barrier_cv_broadcast::set(|_| {});
        xact_seams::is_transaction_or_transaction_block::set(|| IN_XACT.load(SeqCst));
        xact_seams::start_transaction_command::set(|| {
            ACCEPT_CALLS.fetch_add(1, SeqCst);
            accept()
        });
        xact_seams::commit_transaction_command::set(|| Ok(()));
        inval_seams::accept_invalidation_messages::set(|| {
            ACCEPT_CALLS.fetch_add(1, SeqCst);
            accept()
        });
        lwlock::CreateLWLocks(false).unwrap();
        lmgr_proc::InitProcGlobal(&lmgr_proc::ProcGlobalConfig {
            autovacuum_worker_slots: 3,
            max_wal_senders: 2,
            max_prepared_xacts: 2,
            fastpath_lock_groups_per_backend: 1,
        });
        procsignal::ProcSignalShmemInit();
        procsignal::init_seams();
        init_seams();
    });
    SharedInvalShmemInit().unwrap();
}

fn accept() -> PgResult<()> {
    ReceiveSharedInvalidMessages(&mut |_| Ok(()), &mut || Ok(()))
}

fn backend_init(procno: ProcNumber, pid: i32, send_only: bool) -> usize {
    thread_globals(procno, pid);
    let mark = EXIT_CALLBACKS.lock().unwrap().len();
    SharedInvalBackendInit(send_only).unwrap();
    mark
}

fn backend_exit(procno: ProcNumber, pid: i32, mark: usize) {
    thread_globals(procno, pid);
    let callbacks: Vec<_> = EXIT_CALLBACKS.lock().unwrap().split_off(mark);
    for (f, arg) in callbacks.into_iter().rev() {
        f(0, arg);
    }
}

fn relcache_msg(rel_id: u32) -> SharedInvalidationMessage {
    SharedInvalidationMessage::Relcache(SharedInvalRelcacheMsg {
        dbId: 5,
        relId: rel_id,
    })
}

fn receive_all() -> (Vec<SharedInvalidationMessage>, bool) {
    let mut got = Vec::new();
    let mut reset = false;
    ReceiveSharedInvalidMessages(
        &mut |msg| {
            got.push(*msg);
            Ok(())
        },
        &mut || {
            reset = true;
            Ok(())
        },
    )
    .unwrap();
    (got, reset)
}

#[test]
fn reset_after_crash_restores_boot_image() {
    let _s = serial();
    setup();
    let seg = current_seg();

    let mark = backend_init(1, 9101, false);
    SendSharedInvalidMessages(&[relcache_msg(41), relcache_msg(42)]).unwrap();
    assert!(seg.hdr().maxMsgNum.load(Relaxed) > 0);

    SharedInvalShmemResetAfterCrash();

    assert_eq!(seg.hdr().minMsgNum.load(Relaxed), 0);
    assert_eq!(seg.hdr().maxMsgNum.load(Relaxed), 0);
    assert_eq!(seg.hdr().nextThreshold.load(Relaxed), CLEANUP_MIN);
    assert!(seg.hdr().msgnumLock.is_free());
    assert_eq!(seg.hdr().numProcs.load(Relaxed), 0);
    for state in seg.proc_states() {
        assert_eq!(state.procPid.load(Relaxed), 0);
        assert_eq!(state.nextMsgNum.load(Relaxed), 0);
        assert!(!state.resetState.load(Relaxed));
        assert!(!state.signaled.load(Relaxed));
        assert!(!state.hasMessages.load(Relaxed));
        assert!(!state.sendOnly.load(Relaxed));
        assert_eq!(state.nextLXID.load(Relaxed), InvalidLocalTransactionId);
    }
    // The dead backend's exit callback must not fire against the reset image.
    EXIT_CALLBACKS.lock().unwrap().truncate(mark);
    LOCAL.with(|st| st.my_procno.set(-1));
}

#[test]
fn backend_lifecycle_and_lxid_handoff() {
    let _s = serial();
    setup();
    let seg = current_seg();

    let procs_before = seg.hdr().numProcs.load(Relaxed);
    let mark = backend_init(0, 9000, false);
    let state = &seg.proc_states()[0];
    assert_eq!(state.procPid.load(Relaxed), 9000);
    assert!(!state.sendOnly.load(Relaxed));
    assert_eq!(seg.hdr().numProcs.load(Relaxed), procs_before + 1);

    let err = SharedInvalBackendInit(false).unwrap_err();
    assert!(err.message().contains("already in use by process 9000"));

    assert_eq!(GetNextLocalTransactionId(), 1);
    assert_eq!(GetNextLocalTransactionId(), 2);

    backend_exit(0, 9000, mark);
    assert_eq!(state.procPid.load(Relaxed), 0);
    assert_eq!(state.nextLXID.load(Relaxed), 3);
    assert_eq!(seg.hdr().numProcs.load(Relaxed), procs_before);

    let mark = backend_init(0, 9000, false);
    assert_eq!(GetNextLocalTransactionId(), 3);
    backend_exit(0, 9000, mark);
}

#[test]
fn send_receive_roundtrip_preserves_order_and_payload() {
    let _s = serial();
    setup();
    let mark = backend_init(0, 9000, false);

    let msgs = [
        SharedInvalidationMessage::Catcache(SharedInvalCatcacheMsg {
            id: 41,
            dbId: 5,
            hashValue: 0xDEAD_BEEF,
        }),
        relcache_msg(16384),
        SharedInvalidationMessage::Smgr(SharedInvalSmgrMsg {
            backend_hi: -1,
            backend_lo: 0xFFFF,
            rlocator: RelFileLocator {
                spcOid: 1663,
                dbOid: 5,
                relNumber: 16385,
            },
        }),
    ];
    let counter_before = SharedInvalidMessageCounter();
    sinval_seams::send_shared_invalid_messages::call(&msgs).unwrap();

    let (got, reset) = receive_all();
    assert!(!reset);
    assert_eq!(got, msgs);
    assert_eq!(SharedInvalidMessageCounter(), counter_before + 3);

    let (got, reset) = receive_all();
    assert!(got.is_empty() && !reset);

    backend_exit(0, 9000, mark);
}

#[test]
fn receive_drains_batches_larger_than_local_buffer() {
    let _s = serial();
    setup();
    let mark = backend_init(0, 9000, false);

    let msgs: Vec<_> = (0..70).map(|i| relcache_msg(20000 + i)).collect();
    SendSharedInvalidMessages(&msgs).unwrap();

    let (got, reset) = receive_all();
    assert!(!reset);
    assert_eq!(got, msgs);

    backend_exit(0, 9000, mark);
}

#[test]
fn overflow_signals_then_resets_lagging_backend() {
    let _s = serial();
    setup();
    let sender_mark = backend_init(7, 9007, true);

    thread_globals(1, 9001);
    let reader_mark = EXIT_CALLBACKS.lock().unwrap().len();
    procsignal::ProcSignalInit(&[7u8; 4]).unwrap();
    SharedInvalBackendInit(false).unwrap();

    let seg = current_seg();
    let reader = &seg.proc_states()[1];
    let latch = &lmgr_proc::GetPGProcByNumber(1).procLatch;

    thread_globals(7, 9007);
    let batch: Vec<_> = (0..64).map(|i| relcache_msg(30000 + i)).collect();
    let mut sent = 0;
    while !reader.signaled.load(Relaxed) {
        SendSharedInvalidMessages(&batch).unwrap();
        sent += batch.len() as i32;
        assert!(sent <= 2 * MAXNUMMESSAGES, "catchup signal never sent");
    }
    assert!(latch.is_set());
    assert!(!reader.resetState.load(Relaxed));

    while !reader.resetState.load(Relaxed) {
        SendSharedInvalidMessages(&batch).unwrap();
        sent += batch.len() as i32;
        assert!(sent <= 4 * MAXNUMMESSAGES, "lagging backend never reset");
    }

    thread_globals(1, 9001);
    let (got, reset) = receive_all();
    assert!(reset && got.is_empty());
    assert!(!reader.resetState.load(Relaxed));
    assert!(!reader.signaled.load(Relaxed));
    assert_eq!(
        reader.nextMsgNum.load(Relaxed),
        seg.hdr().maxMsgNum.load(Relaxed)
    );
    let (got, reset) = receive_all();
    assert!(got.is_empty() && !reset);

    backend_exit(1, 9001, reader_mark);
    backend_exit(7, 9007, sender_mark);
}

#[test]
fn catchup_interrupt_daisy_chain_flags() {
    let _s = serial();
    setup();
    let mark = backend_init(0, 9000, false);

    let my_latch = latch::allocate_local_latch();
    g::SetMyLatch(Some(my_latch));
    latch::InitLatch(my_latch);

    assert!(!catchupInterruptPending());
    sinval_seams::handle_catchup_interrupt::call();
    assert!(catchupInterruptPending());
    assert!(latch::latch_ref(my_latch).is_set());

    IN_XACT.store(true, SeqCst);
    let calls_before = ACCEPT_CALLS.load(SeqCst);
    ProcessCatchupInterrupt().unwrap();
    assert!(!catchupInterruptPending());
    assert_eq!(ACCEPT_CALLS.load(SeqCst), calls_before + 1);

    LOCAL.with(|st| st.catchup_pending.set(true));
    IN_XACT.store(false, SeqCst);
    ProcessCatchupInterrupt().unwrap();
    assert!(!catchupInterruptPending());

    g::SetMyLatch(None);
    backend_exit(0, 9000, mark);
}

#[test]
fn msgnum_wraparound_rebases_all_counters() {
    let _s = serial();
    setup();
    let mark = backend_init(0, 9000, false);

    let seg = current_seg();
    let h = seg.hdr();
    let bias = MSGNUMWRAPAROUND - h.maxMsgNum.load(Relaxed);
    h.maxMsgNum.fetch_add(bias, Relaxed);
    h.minMsgNum.fetch_add(bias, Relaxed);
    seg.proc_states()[0].nextMsgNum.fetch_add(bias, Relaxed);

    SICleanupQueue(false, 0).unwrap();

    assert!(h.maxMsgNum.load(Relaxed) < MSGNUMWRAPAROUND);
    assert!(h.minMsgNum.load(Relaxed) >= 0);
    assert_eq!(
        seg.proc_states()[0].nextMsgNum.load(Relaxed),
        h.minMsgNum.load(Relaxed)
    );
    assert_eq!(h.nextThreshold.load(Relaxed), CLEANUP_MIN);

    SendSharedInvalidMessages(&[relcache_msg(1)]).unwrap();
    let (got, reset) = receive_all();
    assert!(!reset);
    assert_eq!(got, vec![relcache_msg(1)]);

    backend_exit(0, 9000, mark);
}

#[test]
fn shmem_size_matches_layout() {
    let _s = serial();
    setup();
    let slots = num_proc_state_slots();
    assert_eq!(
        SharedInvalShmemSize().unwrap(),
        size_of::<SISegHdr>() + slots * size_of::<ProcState>() + slots * size_of::<i32>()
    );
    assert_eq!(MSGNUMWRAPAROUND, MAXNUMMESSAGES * 262144);
    assert_eq!(CLEANUP_MIN, 2048);
    assert_eq!(CLEANUP_QUANTUM, 256);
    assert_eq!(SIG_THRESHOLD, 2048);
    assert_eq!(WRITE_QUANTUM, 64);
    assert_eq!(MAXINVALMSGS, 32);
}

#[test]
fn backend_thread_attaches_without_local_shmem_init() {
    let _guard = serial();
    setup();
    let mark = std::thread::spawn(|| {
        thread_globals(7, 9700);
        // No SharedInvalShmemInit on this thread: BackendInit must bind the
        // TLS seg from the process-global publication.
        let mark = EXIT_CALLBACKS.lock().unwrap().len();
        SharedInvalBackendInit(false).unwrap();
        let (msgs, reset) = receive_all();
        assert!(msgs.is_empty());
        assert!(!reset);
        mark
    })
    .join()
    .unwrap();
    backend_exit(7, 9700, mark);
}
