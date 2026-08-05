//! ProcArrayShmemResetAfterCrash: crashed backends leave the array populated;
//! reset must restore the empty boot image with full capacity. Own process —
//! the reset nukes state the unit tests share.

use std::sync::atomic::Ordering::Relaxed;

use init_small::globals as g;
use types_core::ProcNumber;
use types_storage::storage::NUM_SPECIAL_WORKER_PROCS;

const MAX_CONNECTIONS: i32 = 8;
const MAX_WORKER_PROCESSES: i32 = 2;
const MAX_BACKENDS: i32 =
    MAX_CONNECTIONS + 3 + MAX_WORKER_PROCESSES + 2 + NUM_SPECIAL_WORKER_PROCS;

fn add_running(procno: ProcNumber, xid: u32) {
    let proc = lmgr_proc::GetPGProcByNumber(procno);
    proc.xid.value.store(xid, Relaxed);
    proc.pgxactoff.store(-1, Relaxed);
    procarray::ProcArrayAdd(procno).expect("ProcArrayAdd");
}

#[test]
fn reset_restores_empty_array_with_full_capacity() {
    g::SetMaxConnections(MAX_CONNECTIONS);
    g::set_max_worker_processes(MAX_WORKER_PROCESSES);
    g::SetMaxBackends(MAX_BACKENDS);
    g::SetMyProcPid(4242);

    pg_sema_seams::pg_semaphore_create::set(|_| {});
    s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
    s_lock_seams::finish_spin_delay::set(|_| {});
    shmem_seams::add_size::set(|a, b| Ok(a.checked_add(b).expect("size overflow")));
    shmem_seams::mul_size::set(|a, b| Ok(a.checked_mul(b).expect("size overflow")));
    shmem_seams::shmem_alloc::set(|size| {
        Ok(Box::leak(vec![0u8; size].into_boxed_slice()).as_mut_ptr())
    });

    lwlock::CreateLWLocks(false).unwrap();
    lmgr_proc::init_seams();
    lmgr_proc::InitProcGlobal(&lmgr_proc::ProcGlobalConfig {
        autovacuum_worker_slots: 3,
        max_wal_senders: 2,
        max_prepared_xacts: 2,
        fastpath_lock_groups_per_backend: 1,
    });
    varsup::VarsupShmemInit();
    procarray::ProcArrayShmemInit();

    add_running(0, 100);
    add_running(3, 101);
    add_running(5, 102);

    procarray::ProcArrayShmemResetAfterCrash();
    lmgr_proc::ProcGlobalResetAfterCrash();

    // Empty again at full capacity: every backend slot re-adds cleanly.
    for procno in 0..MAX_BACKENDS {
        add_running(procno, 200 + procno as u32);
    }
}
