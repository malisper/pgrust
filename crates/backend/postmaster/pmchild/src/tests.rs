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
    });
    // Re-run per test thread: idempotent, refreshes pmsignal's thread-local
    // num_child_flags copy (in production only the postmaster calls Mark*).
    pmsignal::PMSignalShmemInit(pmchild_seams::max_live_postmaster_children::call());
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

#[test]
fn slot_count_matches_c_formula() {
    let _g = bringup();
    // 2*(100+10) + 16 + 8 + MAX_IO_WORKERS + 10 singletons.
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

    // Freshly released slot is granted next (C push_head/pop_head LIFO).
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
fn signal_children_quiet_when_no_match_loud_on_match() {
    let _g = bringup();
    assert!(!pmchild_seams::signal_children::call(
        libc_sigterm(),
        btmask(BackendType::WalWriter)
    ));

    let slot = pmchild_seams::assign_postmaster_child_slot::call(BackendType::WalWriter).unwrap();
    let err = std::panic::catch_unwind(|| {
        pmchild_seams::signal_children::call(libc_sigterm(), btmask(BackendType::WalWriter))
    })
    .expect_err("delivery to a live child thread must fail loud");
    let msg = err.downcast_ref::<String>().cloned().unwrap_or_default();
    assert!(msg.contains("per-thread signal delivery"), "got: {msg}");
    assert!(pmchild_seams::release_postmaster_child_slot::call(slot));
}

fn libc_sigterm() -> i32 {
    15
}
