use std::sync::{Mutex, MutexGuard, Once};

use super::*;

fn btmask(t: BackendType) -> u32 {
    1 << (t as u32)
}

static TEST_LOCK: Mutex<()> = Mutex::new(());

fn bringup() -> MutexGuard<'static, ()> {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        use std::sync::atomic::{AtomicI32, Ordering::Relaxed};
        static WAL_SENDERS: AtomicI32 = AtomicI32::new(10);
        static AV_SLOTS: AtomicI32 = AtomicI32::new(16);
        guc_tables::vars::max_wal_senders.install(guc_tables::GucVarAccessors {
            get: || WAL_SENDERS.load(Relaxed),
            set: |v| WAL_SENDERS.store(v, Relaxed),
        });
        guc_tables::vars::autovacuum_worker_slots.install(guc_tables::GucVarAccessors {
            get: || AV_SLOTS.load(Relaxed),
            set: |v| AV_SLOTS.store(v, Relaxed),
        });
        shmem::init_seams();
        init_seams();
        init_small::globals::SetMaxConnections(100);
        init_small::globals::set_max_worker_processes(8);
        pmchild_seams::init_postmaster_child_slots::call();

        s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
        s_lock_seams::finish_spin_delay::set(|_| {});
        ipc_seams::on_shmem_exit::set(|_, _| {});
        pg_sema_seams::pg_semaphore_create::set(|_| {});
        init_small::globals::SetMaxBackends(
            100 + 3 + 8 + 2 + types_storage::storage::NUM_SPECIAL_WORKER_PROCS,
        );
        lmgr_proc::InitProcGlobal(&lmgr_proc::ProcGlobalConfig {
            autovacuum_worker_slots: 3,
            max_wal_senders: 2,
            max_prepared_xacts: 2,
            fastpath_lock_groups_per_backend: 1,
        });
        procsignal::ProcSignalShmemInit();
    });
    // Idempotent; refreshes pmsignal's thread-local num_child_flags copy.
    pmsignal::PMSignalShmemInit(pmchild_seams::max_live_postmaster_children::call());
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

#[test]
fn slot_count_matches_c_formula() {
    let _g = bringup();
    assert_eq!(MaxLivePostmasterChildren(), 220 + 16 + 8 + 32 + 10);
}

#[test]
fn assign_release_roundtrip_and_masks() {
    let _g = bringup();

    let slot = pmchild_seams::assign_postmaster_child_slot::call(BackendType::Backend)
        .expect("backend pool must not be empty");
    assert!(slot >= 1 && slot <= 220);
    pmchild_seams::set_child_pid::call(slot, 4242);

    assert_eq!(pmchild_seams::count_children::call(btmask(BackendType::Backend)), 1);
    assert_eq!(pmchild_seams::count_children::call(btmask(BackendType::Checkpointer)), 0);
    assert_eq!(FindPostmasterChildByPid(4242).unwrap().child_slot, slot);

    assert!(pmchild_seams::release_postmaster_child_slot::call(slot));
    assert_eq!(pmchild_seams::count_children::call(btmask(BackendType::Backend)), 0);
    assert!(FindPostmasterChildByPid(4242).is_none());

    let again = pmchild_seams::assign_postmaster_child_slot::call(BackendType::Backend).unwrap();
    assert_eq!(again, slot);
    assert!(pmchild_seams::release_postmaster_child_slot::call(again));
}

#[test]
fn singleton_pool_exhausts_to_none() {
    let _g = bringup();
    let s = pmchild_seams::assign_postmaster_child_slot::call(BackendType::Checkpointer).unwrap();
    assert!(pmchild_seams::assign_postmaster_child_slot::call(BackendType::Checkpointer).is_none());
    assert!(pmchild_seams::release_postmaster_child_slot::call(s));
}

#[test]
fn dead_end_children_get_unique_negative_ids() {
    let _g = bringup();
    let a = pmchild_seams::alloc_dead_end_child::call().unwrap();
    let b = pmchild_seams::alloc_dead_end_child::call().unwrap();
    assert!(a < 0 && b < 0 && a != b);
    assert_eq!(
        pmchild_seams::count_children::call(btmask(BackendType::DeadEndBackend)),
        2
    );
    assert!(pmchild_seams::release_postmaster_child_slot::call(a));
    assert!(pmchild_seams::release_postmaster_child_slot::call(b));
    assert_eq!(
        pmchild_seams::count_children::call(btmask(BackendType::DeadEndBackend)),
        0
    );
}

#[test]
fn signal_children_quiet_when_no_match() {
    let _g = bringup();
    assert!(!pmchild_seams::signal_children::call(
        libc_sigterm(),
        btmask(BackendType::WalWriter)
    ));
}

static SIGTERM_OBSERVED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn observe_sigterm() {
    SIGTERM_OBSERVED.store(true, std::sync::atomic::Ordering::SeqCst);
}

#[test]
fn signal_children_delivers_to_registered_child_thread() {
    let _g = bringup();
    let slot = pmchild_seams::assign_postmaster_child_slot::call(BackendType::BgWriter).unwrap();
    pmchild_seams::set_child_pid::call(slot, 5511);

    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let child = std::thread::spawn(move || {
        init_small::globals::SetMyProcNumber(1);
        init_small::globals::SetMyProcPid(5511);
        procsignal::ProcSignalInit(&[]).unwrap();
        procsignal::pqsignal_thread(
            libc_sigterm(),
            procsignal::ThreadSignalHandler::Simple(observe_sigterm),
        );
        ready_tx.send(()).unwrap();
        let latch = &lmgr_proc::GetPGProcByNumber(1).procLatch;
        while !latch.is_set() {
            std::thread::yield_now();
        }
        procsignal::DrainThreadSignals().unwrap();
        done_tx.send(SIGTERM_OBSERVED.load(std::sync::atomic::Ordering::SeqCst)).unwrap();
    });

    ready_rx.recv().unwrap();
    assert!(pmchild_seams::signal_children::call(
        libc_sigterm(),
        btmask(BackendType::BgWriter)
    ));
    assert!(done_rx.recv().unwrap(), "child thread must observe the drained SIGTERM");
    child.join().unwrap();
    assert!(pmchild_seams::release_postmaster_child_slot::call(slot));
}

// A dead-end backend has no ProcSignal slot by design (it never
// ProcSignalInits). C's SignalChildren/signal_child (postmaster.c) signals
// it the same as any other child and tolerates ESRCH -- it never asserts or
// crashes the postmaster over an undeliverable signal to that lane. Matches
// the P1 fix for the boot-loop panic under SIGINT restart with an active
// dead-end backend (backend_startup dead-end lane, pmchild/src/lib.rs).
#[test]
fn signal_children_dead_end_delivery_is_tolerated_not_fatal() {
    let _g = bringup();
    let id = pmchild_seams::alloc_dead_end_child::call().unwrap();
    pmchild_seams::set_child_pid::call(id, 5512);
    let matched = pmchild_seams::signal_children::call(3, btmask(BackendType::DeadEndBackend));
    assert!(matched, "dead-end backend must still count as matched/signaled, not skipped");
    assert!(pmchild_seams::release_postmaster_child_slot::call(id));
}

fn libc_sigterm() -> i32 {
    15
}
