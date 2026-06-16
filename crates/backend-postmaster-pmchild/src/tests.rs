//! Behavioral tests for the postmaster child-slot pools.
//!
//! These install the small set of GUC seams that `InitPostmasterChildSlots`
//! reads, plus this crate's own `max_live_postmaster_children` seam, and then
//! initialize pmsignal's shared per-slot flag array (which only depends on
//! `max_live_postmaster_children`). That is enough to exercise the full
//! assign / release / find bookkeeping end to end without the postmaster.
//!
//! All tests run on one thread and share a process-local pmchild state plus the
//! global pmsignal `OnceLock`, so they are serialized behind a mutex and the
//! GUCs are fixed once.

use std::sync::{Mutex, Once};

use types_core::init::BackendType;
use types_storage::MAX_IO_WORKERS;

use super::*;

static SETUP: Once = Once::new();
static SERIAL: Mutex<()> = Mutex::new(());

// Fixed GUC values for the test postmaster.
const TEST_MAX_CONNECTIONS: i32 = 10;
const TEST_MAX_WAL_SENDERS: i32 = 3;
const TEST_AUTOVAC_WORKER_SLOTS: i32 = 2;
const TEST_MAX_WORKER_PROCESSES: i32 = 4;

fn install_seams_once() {
    SETUP.call_once(|| {
        // GUCs read by InitPostmasterChildSlots.
        backend_utils_init_small_seams::max_connections::set(|| TEST_MAX_CONNECTIONS);
        backend_replication_walsender_seams::max_wal_senders::set(|| TEST_MAX_WAL_SENDERS);
        backend_postmaster_autovacuum_seams::autovacuum_worker_slots::set(
            || TEST_AUTOVAC_WORKER_SLOTS,
        );
        backend_utils_init_small_seams::max_worker_processes::set(|| TEST_MAX_WORKER_PROCESSES);
        // DEBUG2 log name lookup.
        backend_postmaster_launch_backend_seams::postmaster_child_name::set(|_bt| "test-child");
        // pmsignal needs this crate's MaxLivePostmasterChildren.
        backend_postmaster_pmchild_seams::max_live_postmaster_children::set(
            MaxLivePostmasterChildren,
        );
        // standalone-backend short-circuit guards (SendPostmasterSignal etc.)
        // are not on the paths we exercise, but the Mark* functions need the
        // shared array. Initialize the pools first so MaxLive returns nonzero,
        // then attach pmsignal's shared array.
        InitPostmasterChildSlots();
        backend_storage_ipc_pmsignal::PMSignalShmemInit().unwrap();
    });
}

/// Expected total = sum of all pool sizes for the fixed GUCs.
fn expected_total() -> i32 {
    // B_BACKEND: 2 * (MaxConnections + max_wal_senders)
    2 * (TEST_MAX_CONNECTIONS + TEST_MAX_WAL_SENDERS)
        + TEST_AUTOVAC_WORKER_SLOTS // B_AUTOVAC_WORKER
        + TEST_MAX_WORKER_PROCESSES // B_BG_WORKER
        + MAX_IO_WORKERS // B_IO_WORKER
        // the 10 single-entry pools:
        + 10
}

#[test]
fn init_sets_total_slots() {
    let _g = SERIAL.lock().unwrap();
    install_seams_once();
    assert_eq!(MaxLivePostmasterChildren(), expected_total());
}

#[test]
fn assign_returns_unique_increasing_slots_within_pool() {
    let _g = SERIAL.lock().unwrap();
    install_seams_once();
    // Re-init to a clean state for this test.
    InitPostmasterChildSlots();

    // The autovac-worker pool has exactly TEST_AUTOVAC_WORKER_SLOTS entries.
    let a = AssignPostmasterChildSlot(BackendType::AutovacWorker).unwrap();
    let b = AssignPostmasterChildSlot(BackendType::AutovacWorker).unwrap();
    assert_ne!(a.child_slot, b.child_slot);
    assert!(a.child_slot > 0);
    assert!(b.child_slot > 0);
    assert_eq!(a.bkend_type, BackendType::AutovacWorker);
    assert_eq!(a.bgworker_notify, true);

    // Pool exhausted -> None (C returns NULL).
    assert!(AssignPostmasterChildSlot(BackendType::AutovacWorker).is_none());

    // Releasing one frees a slot back into the pool.
    assert!(ReleasePostmasterChildSlot(a));
    let c = AssignPostmasterChildSlot(BackendType::AutovacWorker).unwrap();
    assert_eq!(c.child_slot, a.child_slot);
}

#[test]
fn single_entry_pool_holds_exactly_one() {
    let _g = SERIAL.lock().unwrap();
    install_seams_once();
    InitPostmasterChildSlots();

    let s = AssignPostmasterChildSlot(BackendType::Checkpointer).unwrap();
    assert!(AssignPostmasterChildSlot(BackendType::Checkpointer).is_none());
    assert!(ReleasePostmasterChildSlot(s));
    assert!(AssignPostmasterChildSlot(BackendType::Checkpointer).is_some());
}

#[test]
fn dead_end_children_are_unbounded_and_have_no_slot() {
    let _g = SERIAL.lock().unwrap();
    install_seams_once();
    InitPostmasterChildSlots();

    let mut kids = Vec::new();
    for _ in 0..100 {
        let k = AllocDeadEndChild().unwrap();
        assert_eq!(k.child_slot, 0);
        assert_eq!(k.bkend_type, BackendType::DeadEndBackend);
        kids.push(k);
    }
    // Releasing a dead-end child reports clean detach (true) and pfrees it.
    for k in kids {
        assert!(ReleasePostmasterChildSlot(k));
    }
}

#[test]
fn find_by_pid_walks_active_list() {
    let _g = SERIAL.lock().unwrap();
    install_seams_once();
    InitPostmasterChildSlots();

    // Assigned children start with pid 0; FindPostmasterChildByPid(0) returns
    // the most recently assigned (head of ActiveChildList).
    let s = AssignPostmasterChildSlot(BackendType::Startup).unwrap();
    let found = FindPostmasterChildByPid(0).expect("a pid-0 child is active");
    assert_eq!(found.child_slot, s.child_slot);

    // A pid that no active child carries is not found.
    assert!(FindPostmasterChildByPid(0x7fff_fffe).is_none());

    ReleasePostmasterChildSlot(s);
}

#[test]
fn set_live_entry_fields_after_assign() {
    let _g = SERIAL.lock().unwrap();
    install_seams_once();
    InitPostmasterChildSlots();

    // Mirror StartBackgroundWorker: assign, then mutate the live entry.
    let bn = AssignPostmasterChildSlot(BackendType::BgWorker).unwrap();
    assert!(SetPostmasterChildPid(&bn, 4242));
    assert!(SetPostmasterChildRw(&bn, Some(7)));
    assert!(SetPostmasterChildBackendType(&bn, BackendType::BgWorker));
    assert!(SetPostmasterChildBgworkerNotify(&bn, false));

    // The live list entry (not the caller's stale Copy) carries the new state.
    let live = FindPostmasterChildByPid(4242).expect("entry now has pid 4242");
    assert_eq!(live.child_slot, bn.child_slot);
    assert_eq!(live.rw, Some(7));
    assert_eq!(live.bgworker_notify, false);

    // A non-active entry is not found (returns false, no panic).
    let bogus = PMChild {
        pid: 0,
        child_slot: 999_999,
        bkend_type: BackendType::BgWorker,
        rw: None,
        bgworker_notify: false,
    };
    assert!(!SetPostmasterChildPid(&bogus, 1));

    ReleasePostmasterChildSlot(live);
}

#[test]
fn for_each_active_child_reads_and_mutates() {
    let _g = SERIAL.lock().unwrap();
    install_seams_once();
    InitPostmasterChildSlots();

    let a = AssignPostmasterChildSlot(BackendType::Backend).unwrap();
    SetPostmasterChildPid(&a, 100);

    // SignalChildren-style relabel + PostmasterMarkPIDForWorkerNotify-style set.
    let mut seen = 0;
    for_each_active_child(|mut r| {
        seen += 1;
        if r.pid() == 100 {
            assert_eq!(r.bkend_type(), BackendType::Backend);
            r.set_bkend_type(BackendType::WalSender);
            r.set_bgworker_notify(true);
        }
    });
    assert_eq!(seen, 1);

    let live = FindPostmasterChildByPid(100).unwrap();
    assert_eq!(live.bkend_type, BackendType::WalSender);
    assert_eq!(live.bgworker_notify, true);

    ReleasePostmasterChildSlot(live);
}

#[test]
fn wal_sender_releases_into_backend_pool() {
    let _g = SERIAL.lock().unwrap();
    install_seams_once();
    InitPostmasterChildSlots();

    // WAL senders start out as regular backends (B_BACKEND pool).
    let mut b = AssignPostmasterChildSlot(BackendType::Backend).unwrap();
    // Postmaster relabels it to a WAL sender after it touches PMChildFlags.
    b.bkend_type = BackendType::WalSender;
    // Release must return the slot to the B_BACKEND pool (its origin), not a
    // (nonexistent) B_WAL_SENDER pool, and must not panic on the pool sanity
    // check.
    assert!(ReleasePostmasterChildSlot(b));
}
