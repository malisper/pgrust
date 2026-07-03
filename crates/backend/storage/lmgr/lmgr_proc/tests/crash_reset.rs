//! ProcGlobalResetAfterCrash: a backend claims a PGPROC and its thread dies
//! without ProcKill (the crash class); reset must restore the
//! post-InitProcGlobal image, freelists included. Own process — the reset
//! nukes shared state the unit tests run against concurrently.

use std::sync::atomic::Ordering::Relaxed;
use std::sync::mpsc::channel;

use init_small::globals as g;
use types_core::{BackendType, InvalidTransactionId, INVALID_PROC_NUMBER};
use types_storage::storage::NUM_SPECIAL_WORKER_PROCS;

const CFG: lmgr_proc::ProcGlobalConfig = lmgr_proc::ProcGlobalConfig {
    autovacuum_worker_slots: 3,
    max_wal_senders: 2,
    max_prepared_xacts: 2,
    fastpath_lock_groups_per_backend: 1,
};
const MAX_CONNECTIONS: i32 = 4;
const MAX_WORKER_PROCESSES: i32 = 2;
const MAX_BACKENDS: i32 =
    MAX_CONNECTIONS + 3 + MAX_WORKER_PROCESSES + 2 + NUM_SPECIAL_WORKER_PROCS;

fn thread_globals(pid: i32) {
    g::SetMaxConnections(MAX_CONNECTIONS);
    g::set_max_worker_processes(MAX_WORKER_PROCESSES);
    g::SetMaxBackends(MAX_BACKENDS);
    g::SetMyProcPid(pid);
}

#[test]
fn reset_restores_post_init_image() {
    thread_globals(9000);
    pg_sema_seams::pg_semaphore_create::set(|_| {});
    pg_sema_seams::pg_semaphore_reset::set(|_| {});
    s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
    s_lock_seams::finish_spin_delay::set(|_| {});
    s_lock_seams::set_spins_per_delay::set(|_| {});
    s_lock_seams::update_spins_per_delay::set(|v| v);
    latch_seams::own_latch::set(|latch| latch.owner_pid.store(g::MyProcPid(), Relaxed));
    latch_seams::disown_latch::set(|latch| latch.owner_pid.store(0, Relaxed));
    miscinit_seams::switch_to_shared_latch::set(|| {});
    miscinit_seams::switch_back_to_local_latch::set(|| {});
    waitevent_seams::pgstat_set_wait_event_storage::set(|_| {});
    waitevent_seams::pgstat_reset_wait_event_storage::set(|| {});
    ipc_seams::on_shmem_exit::set(|_, _| {});
    pmsignal_seams::register_postmaster_child_active::set(|| {});
    deadlock_seams::init_dead_lock_checking::set(|| Ok(()));

    lmgr_proc::InitProcGlobal(&CFG);
    let hdr = lmgr_proc::ProcGlobal();

    let (tx, rx) = channel();
    std::thread::spawn(move || {
        thread_globals(9001);
        lmgr_proc::InitProcess(BackendType::Backend).unwrap();
        let procno = lmgr_proc::MyProc().unwrap();
        let proc = lmgr_proc::GetPGProcByNumber(procno);
        proc.xid.value.store(77, Relaxed);
        proc.databaseId.store(5, Relaxed);
        tx.send(procno).unwrap();
        // Thread ends without ProcKill: the crash class.
    })
    .join()
    .unwrap();
    let procno = rx.recv().unwrap();

    let proc = lmgr_proc::GetPGProcByNumber(procno);
    assert_eq!(proc.pid.load(Relaxed), 9001);
    hdr.walwriterProc.store(3, Relaxed);
    hdr.spins_per_delay.set(7);
    hdr.startupBufferPinWaitBufId.store(42, Relaxed);
    hdr.xids[procno as usize].value.store(77, Relaxed);

    lmgr_proc::ProcGlobalResetAfterCrash();

    assert_eq!(proc.pid.load(Relaxed), 0);
    assert_eq!(proc.xid.value.load(Relaxed), InvalidTransactionId);
    assert_eq!(proc.databaseId.load(Relaxed), 0);
    assert_eq!(proc.lockGroupLeader.load(Relaxed), INVALID_PROC_NUMBER);
    assert_eq!(hdr.walwriterProc.load(Relaxed), INVALID_PROC_NUMBER);
    assert_eq!(hdr.spins_per_delay.get(), 100);
    assert_eq!(hdr.startupBufferPinWaitBufId.load(Relaxed), -1);
    assert_eq!(hdr.xids[procno as usize].read(), 0);
    let (enough, nfree) = lmgr_proc::HaveNFreeProcs(MAX_CONNECTIONS);
    assert!(enough, "regular freelist rebuilt short: {nfree}");

    // The crashed backend's slot is claimable again, from the head.
    lmgr_proc::InitProcess(BackendType::Backend).unwrap();
    assert_eq!(lmgr_proc::MyProc().unwrap(), 0);
}
