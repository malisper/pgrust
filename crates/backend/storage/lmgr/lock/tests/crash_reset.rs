//! Crash-class holder dies without releasing; the reset must leave both
//! shared tables empty. Own process — the unit tests share this state.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering::SeqCst};
use std::sync::{Condvar, Mutex, OnceLock};

use init_small::globals as g;
use types_core::BackendType;
use types_error::PgError;
use types_storage::lock::{
    AccessExclusiveLock, DeadLockState, ShareLock, LOCKACQUIRE_OK, LOCKTAG,
};

const TESTDB: u32 = 7777;
const MAX_CONNECTIONS: i32 = 8;
const MAX_WORKER_PROCESSES: i32 = 2;
const MAX_BACKENDS: i32 = MAX_CONNECTIONS + 3 + MAX_WORKER_PROCESSES + 2 + 2;

const CFG: lmgr_proc::ProcGlobalConfig = lmgr_proc::ProcGlobalConfig {
    autovacuum_worker_slots: 3,
    max_wal_senders: 2,
    max_prepared_xacts: 2,
    fastpath_lock_groups_per_backend: 1,
};

static NEXT_PID: AtomicI32 = AtomicI32::new(9100);

fn semas() -> &'static (Mutex<HashMap<types_core::ProcNumber, i32>>, Condvar) {
    static SEMS: OnceLock<(Mutex<HashMap<types_core::ProcNumber, i32>>, Condvar)> =
        OnceLock::new();
    SEMS.get_or_init(|| (Mutex::new(HashMap::new()), Condvar::new()))
}

fn thread_globals() {
    g::SetMaxConnections(MAX_CONNECTIONS);
    g::set_max_worker_processes(MAX_WORKER_PROCESSES);
    g::SetMaxBackends(MAX_BACKENDS);
    g::SetMyProcPid(NEXT_PID.fetch_add(1, SeqCst));
    g::SetMyDatabaseId(TESTDB);
}

fn setup() {
    thread_globals();

    pg_sema_seams::pg_semaphore_create::set(|procno| {
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
    latch_seams::set_latch_my_latch::set(|| {
        if let Some(procno) = lmgr_proc::MyProc() {
            lmgr_proc::GetPGProcByNumber(procno)
                .procLatch
                .is_set
                .store(1, SeqCst);
        }
    });
    latch_seams::wait_latch_my_latch::set(|_, _, _| {
        let procno = lmgr_proc::MyProc().expect("waiting without a proc");
        let latch = &lmgr_proc::GetPGProcByNumber(procno).procLatch;
        while latch.is_set.load(SeqCst) == 0 {
            std::thread::yield_now();
        }
        types_storage::waiteventset::WL_LATCH_SET
    });
    latch_seams::reset_latch_my_latch::set(|| {
        let procno = lmgr_proc::MyProc().expect("reset without a proc");
        lmgr_proc::GetPGProcByNumber(procno)
            .procLatch
            .is_set
            .store(0, SeqCst);
    });
    miscinit_seams::switch_to_shared_latch::set(|| {});
    miscinit_seams::switch_back_to_local_latch::set(|| {});
    waitevent_seams::pgstat_set_wait_event_storage::set(|_| {});
    waitevent_seams::pgstat_reset_wait_event_storage::set(|| {});
    waitevent_seams::pgstat_report_wait_start::set(|_| {});
    waitevent_seams::pgstat_report_wait_end::set(|| {});
    ipc_seams::on_shmem_exit::set(|_, _| {});
    pmsignal_seams::register_postmaster_child_active::set(|| {});

    deadlock_seams::init_dead_lock_checking::set(|| Ok(()));
    deadlock_seams::dead_lock_check::set(|_| DeadLockState::NoDeadLock);
    deadlock_seams::dead_lock_report::set(|| {
        Err(Box::new(PgError::new(types_error::ERROR, "deadlock detected")))
    });
    deadlock_seams::remember_simple_deadlock::set(|_, _, _, _| {});
    deadlock_seams::get_blocking_autovacuum_procno::set(|| None);

    resowner::init_seams();

    transam_xlog_seams::recovery_in_progress::set(|| false);
    transam_xlog_seams::xlog_standby_info_active::set(|| false);

    timeout_seams::enable_timeout_after::set(|_, _| Ok(()));
    timeout_seams::enable_timeouts::set(|_| Ok(()));
    timeout_seams::disable_timeout::set(|_, _| Ok(()));
    timeout_seams::disable_timeouts::set(|_| {});
    timeout_seams::get_timeout_start_time::set(|_| 0);
    timestamp_seams::get_current_timestamp::set(|| 0);

    ps_status_seams::set_ps_display_suffix::set(|_| {});
    ps_status_seams::set_ps_display_remove_suffix::set(|| {});
    elog_seams::ereport_msg::set(|_, _, _| Ok(()));
    lmgr_seams::describe_lock_tag::set(|tag| format!("{tag:?}"));

    shmem_seams::add_size::set(|a, b| Ok(a.checked_add(b).expect("size overflow")));
    shmem_seams::mul_size::set(|a, b| Ok(a.checked_mul(b).expect("size overflow")));
    shmem_seams::shmem_alloc::set(|size| {
        Ok(Box::leak(vec![0u8; size].into_boxed_slice()).as_mut_ptr())
    });

    lwlock::CreateLWLocks(false).unwrap();
    lmgr_proc::init_seams();
    lock::init_seams();
    lmgr_proc::InitProcGlobal(&CFG);
    lock::LockManagerShmemInit(CFG.max_prepared_xacts).unwrap();
}

fn become_backend() {
    thread_globals();
    lmgr_proc::InitProcess(BackendType::Backend).unwrap();
    let procno = lmgr_proc::MyProc().unwrap();
    lmgr_proc::GetPGProcByNumber(procno)
        .databaseId
        .store(TESTDB, SeqCst);
    lock::InitLockManagerAccess();
    let owner = resowner::ResourceOwnerCreate(types_resowner::ResourceOwner::NULL, "crash reset")
        .unwrap();
    resowner::SetCurrentResourceOwner(owner);
}

fn rel_tag(relid: u32) -> LOCKTAG {
    LOCKTAG::relation(TESTDB, relid)
}

#[test]
fn reset_empties_shared_tables_after_crashed_holder() {
    setup();

    std::thread::spawn(|| {
        become_backend();
        assert_eq!(
            lock::LockAcquire(&rel_tag(6001), AccessExclusiveLock, false, false).unwrap(),
            LOCKACQUIRE_OK
        );
        assert_eq!(
            lock::LockAcquire(&rel_tag(6002), ShareLock, false, false).unwrap(),
            LOCKACQUIRE_OK
        );
    })
    .join()
    .unwrap();

    lock::LockManagerShmemResetAfterCrash();
    lmgr_proc::ProcGlobalResetAfterCrash();

    std::thread::spawn(|| {
        become_backend();
        // dontWait: a stale holder would surface as LOCKACQUIRE_NOT_AVAIL.
        assert_eq!(
            lock::LockAcquire(&rel_tag(6001), AccessExclusiveLock, false, true).unwrap(),
            LOCKACQUIRE_OK
        );
        assert_eq!(
            lock::LockAcquire(&rel_tag(6002), AccessExclusiveLock, false, true).unwrap(),
            LOCKACQUIRE_OK
        );
        assert!(lock::LockRelease(&rel_tag(6001), AccessExclusiveLock, false).unwrap());
        assert!(lock::LockRelease(&rel_tag(6002), AccessExclusiveLock, false).unwrap());
    })
    .join()
    .unwrap();
}
