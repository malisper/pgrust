use super::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{Condvar, Mutex, Once, OnceLock};

const CFG: ProcGlobalConfig = ProcGlobalConfig {
    autovacuum_worker_slots: 3,
    max_wal_senders: 2,
    max_prepared_xacts: 2,
    fastpath_lock_groups_per_backend: 1,
};
const MAX_CONNECTIONS: i32 = 4;
const MAX_WORKER_PROCESSES: i32 = 2;
const MAX_BACKENDS: i32 = MAX_CONNECTIONS + 3 + MAX_WORKER_PROCESSES + 2 + NUM_SPECIAL_WORKER_PROCS;

static SEMA_CREATED: AtomicUsize = AtomicUsize::new(0);

fn semas() -> &'static (Mutex<HashMap<ProcNumber, i32>>, Condvar) {
    static SEMS: OnceLock<(Mutex<HashMap<ProcNumber, i32>>, Condvar)> = OnceLock::new();
    SEMS.get_or_init(|| (Mutex::new(HashMap::new()), Condvar::new()))
}

// Serializes the tests that observe or mutate the shared freelists.
fn freelist_guard() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn thread_globals(pid: i32) {
    g::SetMaxConnections(MAX_CONNECTIONS);
    g::set_max_worker_processes(MAX_WORKER_PROCESSES);
    g::SetMaxBackends(MAX_BACKENDS);
    g::SetMyProcPid(pid);
}

fn setup() {
    static SETUP: Once = Once::new();
    SETUP.call_once(|| {
        thread_globals(9000);

        pg_sema_seams::pg_semaphore_create::set(|procno| {
            SEMA_CREATED.fetch_add(1, SeqCst);
            semas().0.lock().unwrap().insert(procno, 0);
        });
        pg_sema_seams::pg_semaphore_reset::set(|procno| {
            semas().0.lock().unwrap().insert(procno, 0);
        });
        pg_sema_seams::pg_semaphore_lock::set(|procno| {
            let (map, cv) = semas();
            let mut counts = map.lock().unwrap();
            loop {
                let count = counts.get_mut(&procno).unwrap();
                if *count > 0 {
                    *count -= 1;
                    return;
                }
                counts = cv.wait(counts).unwrap();
            }
        });
        pg_sema_seams::pg_semaphore_unlock::set(|procno| {
            let (map, cv) = semas();
            *map.lock().unwrap().get_mut(&procno).unwrap() += 1;
            cv.notify_all();
        });

        s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
        s_lock_seams::finish_spin_delay::set(|_| {});
        s_lock_seams::set_spins_per_delay::set(|_| {});
        s_lock_seams::update_spins_per_delay::set(|v| v);

        latch_seams::own_latch::set(|latch| latch.owner_pid.store(g::MyProcPid(), SeqCst));
        latch_seams::disown_latch::set(|latch| latch.owner_pid.store(0, SeqCst));
        latch_seams::set_latch::set(|latch| latch.is_set.store(1, SeqCst));
        latch_seams::set_latch_my_latch::set(|| {});
        latch_seams::wait_latch_my_latch::set(|_, _, _| WL_LATCH_SET);
        latch_seams::reset_latch_my_latch::set(|| {});
        miscinit_seams::switch_to_shared_latch::set(|| {});
        miscinit_seams::switch_back_to_local_latch::set(|| {});
        waitevent_seams::pgstat_set_wait_event_storage::set(|_| {});
        waitevent_seams::pgstat_reset_wait_event_storage::set(|| {});
        ipc_seams::on_shmem_exit::set(|_, _| {});
        deadlock_seams::init_dead_lock_checking::set(|| Ok(()));
        pmsignal_seams::register_postmaster_child_active::set(|| {});
        procarray_seams::proc_array_add::set(|_| Ok(()));
        procarray_seams::proc_array_remove::set(|_, _| Ok(()));
        syncrep_seams::sync_rep_cleanup_at_proc_exit::set(|| {});
        condition_variable_seams::condition_variable_cancel_sleep::set(|| false);
        autovacuum_seams::wake_autovacuum_launcher::set(|| {});
        lock_seams::abort_strong_lock_acquire::set(|| {});
        lock_seams::get_awaited_lock_hashcode::set(|| None);
        lock_seams::lock_release_all::set(|_, _| Ok(()));
        timeout_seams::disable_timeouts::set(|_| {});

        shmem_seams::add_size::set(|a, b| Ok(a.checked_add(b).expect("size overflow")));
        shmem_seams::mul_size::set(|a, b| Ok(a.checked_mul(b).expect("size overflow")));
        shmem_seams::shmem_alloc::set(|size| {
            Ok(Box::leak(vec![0u8; size].into_boxed_slice()).as_mut_ptr())
        });

        lwlock::CreateLWLocks(false).unwrap();
        init_seams();
        InitProcGlobal(&CFG);
    });
}

fn freelist_len(id: FreeListId) -> i32 {
    let hdr = ProcGlobal();
    let mut n = 0;
    let mut cur = freelist(hdr, id).get().head;
    while cur != INVALID_PROC_NUMBER {
        n += 1;
        cur = hdr.allProcs[cur as usize].links.get().next;
    }
    n
}

#[test]
fn init_proc_global_shapes() {
    setup();
    let _guard = freelist_guard();
    thread_globals(9001);
    let hdr = ProcGlobal();

    assert_eq!(hdr.allProcs.len(), (MAX_BACKENDS + NUM_AUXILIARY_PROCS + 2) as usize);
    assert_eq!(hdr.allProcCount, (MAX_BACKENDS + NUM_AUXILIARY_PROCS) as u32);
    assert_eq!(hdr.xids.len(), hdr.allProcs.len());
    assert_eq!(hdr.subxidStates.len(), hdr.allProcs.len());
    assert_eq!(hdr.statusFlags.len(), hdr.allProcs.len());
    assert_eq!(hdr.fpLockGroupsPerBackend, 1);
    assert_eq!(hdr.spins_per_delay.get(), DEFAULT_SPINS_PER_DELAY);
    assert_eq!(hdr.startupBufferPinWaitBufId.load(SeqCst), -1);
    assert_eq!(SEMA_CREATED.load(SeqCst), (MAX_BACKENDS + NUM_AUXILIARY_PROCS) as usize);

    assert_eq!(AuxiliaryProcsBase(), MAX_BACKENDS);
    assert_eq!(PreparedXactProcsBase(), MAX_BACKENDS + NUM_AUXILIARY_PROCS);
    for i in 0..NUM_AUXILIARY_PROCS {
        let aux = GetPGProcByNumber(AuxiliaryProcsBase() + i);
        assert!(aux.procgloballist.get().is_none());
        assert!(aux.procLatch.is_shared.load(SeqCst));
    }
    let prepared = GetPGProcByNumber(PreparedXactProcsBase());
    assert!(!prepared.procLatch.is_shared.load(SeqCst));
    assert!(!GetPGProcByNumber(0).fpLockBits.get().is_null());
    assert_eq!(GetPGProcByNumber(0).fpInfoLock.tranche, LWTRANCHE_LOCK_FASTPATH as u16);

    assert_eq!(
        freelist_len(FreeListId::Autovac),
        CFG.autovacuum_worker_slots + NUM_SPECIAL_WORKER_PROCS
    );
    assert_eq!(freelist_len(FreeListId::Walsender), CFG.max_wal_senders);

    assert_eq!(ProcGlobalSemas(), MAX_BACKENDS + NUM_AUXILIARY_PROCS);
    assert!(ProcGlobalShmemSize(&CFG).unwrap() > 0);

    SetStartupBufferPinWaitBufId(7);
    assert_eq!(GetStartupBufferPinWaitBufId(), 7);
    SetStartupBufferPinWaitBufId(-1);
}

#[test]
fn backend_lifecycle_and_lock_groups() {
    setup();
    let _guard = freelist_guard();
    thread_globals(101);

    InitProcess(BackendType::Backend).unwrap();
    let procno = MyProc().unwrap();
    assert!(procno < MAX_CONNECTIONS);
    assert_eq!(g::MyProcNumber(), procno);
    let proc = GetPGProcByNumber(procno);
    assert_eq!(proc.pid.load(SeqCst), 101);
    assert!(proc.isRegularBackend.load(SeqCst));
    assert_eq!(proc.vxid.procNumber.load(SeqCst), procno);
    assert!(proc.links.get().is_detached());
    assert_eq!(
        InitProcess(BackendType::Backend).unwrap_err().message(),
        "you already exist"
    );
    InitProcessPhase2().unwrap();

    BecomeLockGroupLeader().unwrap();
    BecomeLockGroupLeader().unwrap();
    assert_eq!(proc.lockGroupLeader.load(SeqCst), procno);

    let leader_no = procno;
    let worker = std::thread::spawn(move || {
        thread_globals(102);
        InitProcess(BackendType::BgWorker).unwrap();
        let me = MyProc().unwrap();
        assert!(GetPGProcByNumber(me).procgloballist.get() == Some(FreeListId::Bgworker));
        assert!(BecomeLockGroupMember(leader_no, 101).unwrap());
        assert_eq!(GetPGProcByNumber(me).lockGroupLeader.load(SeqCst), leader_no);
        ProcKill(0, 0);
        assert!(MyProc().is_none());
    });
    worker.join().unwrap();
    assert_eq!(freelist_len(FreeListId::Bgworker), MAX_WORKER_PROCESSES);
    assert_eq!(proc.lockGroupLeader.load(SeqCst), procno);

    LockErrorCleanup().unwrap();
    assert_eq!(g::InterruptHoldoffCount(), 0);
    ProcReleaseLocks(true).unwrap();

    let free_before = freelist_len(FreeListId::Regular);
    ProcKill(0, 0);
    assert!(MyProc().is_none());
    assert_eq!(g::MyProcNumber(), INVALID_PROC_NUMBER);
    assert_eq!(proc.pid.load(SeqCst), 0);
    assert_eq!(proc.lockGroupLeader.load(SeqCst), INVALID_PROC_NUMBER);
    assert_eq!(freelist_len(FreeListId::Regular), free_before + 1);

    InitProcess(BackendType::AutovacWorker).unwrap();
    let av = GetPGProcByNumber(MyProc().unwrap());
    assert_eq!(av.statusFlags.load(SeqCst), PROC_IS_AUTOVACUUM);
    assert!(!av.isRegularBackend.load(SeqCst));
    ProcKill(0, 0);
}

#[test]
fn too_many_walsenders_is_fatal_53300() {
    setup();
    let _guard = freelist_guard();
    thread_globals(103);
    let mut claimed = vec![];
    loop {
        std::thread::scope(|s| {
            s.spawn(|| {
                thread_globals(103);
                match InitProcess(BackendType::WalSender) {
                    Ok(()) => {
                        claimed.push(MyProc().unwrap());
                        // Keep the slot claimed past thread exit: no ProcKill.
                    }
                    Err(err) => {
                        assert_eq!(err.sqlstate(), ERRCODE_TOO_MANY_CONNECTIONS);
                        assert!(err.message().contains("max_wal_senders"));
                        claimed.push(INVALID_PROC_NUMBER);
                    }
                }
            });
        });
        if claimed.last() == Some(&INVALID_PROC_NUMBER) {
            break;
        }
    }
    assert_eq!(claimed.len() as i32, CFG.max_wal_senders + 1);
    // Return the claimed slots to the walsender freelist for other tests.
    let hdr = ProcGlobal();
    spin_acquire(&ProcStructLock);
    for &procno in &claimed {
        if procno != INVALID_PROC_NUMBER {
            plist_push_tail(hdr, &hdr.walsenderFreeProcs, procno, links_of);
            GetPGProcByNumber(procno).pid.store(0, SeqCst);
        }
    }
    ProcStructLock.unlock();
}

#[test]
fn aux_lifecycle() {
    setup();
    thread_globals(202);

    InitAuxiliaryProcess().unwrap();
    let procno = MyProc().unwrap();
    assert!(procno >= AuxiliaryProcsBase() && procno < PreparedXactProcsBase());
    let proc = GetPGProcByNumber(procno);
    assert_eq!(proc.pid.load(SeqCst), 202);
    assert_eq!(proc.vxid.procNumber.load(SeqCst), INVALID_PROC_NUMBER);
    assert_eq!(AuxiliaryPidGetProc(202), Some(procno));
    assert_eq!(AuxiliaryPidGetProc(0), None);

    AuxiliaryProcKill(0, (procno - AuxiliaryProcsBase()) as usize);
    assert!(MyProc().is_none());
    assert_eq!(proc.pid.load(SeqCst), 0);
    assert_eq!(AuxiliaryPidGetProc(202), None);
}

#[test]
fn signals_and_deadlock_alert() {
    setup();
    thread_globals(303);

    let target = PreparedXactProcsBase() - 1;
    let latch = &GetPGProcByNumber(target).procLatch;
    latch.is_set.store(0, SeqCst);
    ProcSendSignal(target).unwrap();
    assert_eq!(latch.is_set.load(SeqCst), 1);
    assert!(ProcSendSignal(ProcGlobal().allProcs.len() as ProcNumber).is_err());
    assert!(ProcSendSignal(-1).is_err());

    assert!(!GotDeadlockTimeout());
    CheckDeadLockAlert();
    assert!(GotDeadlockTimeout());

    ProcWaitForSignal(0);
}

#[test]
fn guc_storage_and_installed_seams() {
    setup();
    thread_globals(404);

    assert_eq!(guc_tables::vars::DeadlockTimeout.read(), 1000);
    guc_tables::vars::DeadlockTimeout.write(2500);
    assert_eq!(globals::DeadlockTimeout(), 2500);
    assert!(!guc_tables::vars::log_lock_waits.read());

    let probe = PreparedXactProcsBase();
    assert_eq!(lmgr_proc_seams::proc_lw_waiting::call(probe), 0);
    lmgr_proc_seams::set_proc_lw_waiting::call(probe, 2);
    assert_eq!(lmgr_proc_seams::proc_lw_waiting::call(probe), 2);
    lmgr_proc_seams::set_proc_lw_waiting::call(probe, 0);
    let node = lmgr_proc_seams::proclist_node { next: 5, prev: 6 };
    lmgr_proc_seams::set_proc_lw_wait_link::call(probe, node);
    assert_eq!(lmgr_proc_seams::proc_lw_wait_link::call(probe), node);
    lmgr_proc_seams::set_proc_lw_wait_link::call(
        probe,
        lmgr_proc_seams::proclist_node::default(),
    );

    // The sema delegate reaches the pg_sema owner: unlock then lock returns.
    lmgr_proc_seams::pg_semaphore_unlock::call(0);
    lmgr_proc_seams::pg_semaphore_lock::call(0);
}

#[test]
fn concurrent_backend_claims_are_disjoint() {
    setup();
    let _guard = freelist_guard();
    let procnos: Vec<ProcNumber> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..3)
            .map(|i| {
                s.spawn(move || {
                    thread_globals(500 + i);
                    InitProcess(BackendType::Backend).unwrap();
                    let procno = MyProc().unwrap();
                    std::thread::yield_now();
                    ProcKill(0, 0);
                    procno
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let mut unique = procnos.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(unique.len(), procnos.len());
    assert!(procnos.iter().all(|&p| p < MAX_CONNECTIONS));
}
