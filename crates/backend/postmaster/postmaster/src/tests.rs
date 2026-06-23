//! Behavioral test for `PostmasterMarkPIDForWorkerNotify` over a real pmchild
//! `ActiveChildList`. We initialize pmchild's slot pools (mirroring the pmchild
//! crate's own test fixture), assign a child, give it a pid, and verify the
//! mark flips `bgworker_notify` for the matching pid and returns the C-faithful
//! found/not-found boolean.

use std::sync::{Mutex, Once};

use pmchild::{
    AssignPostmasterChildSlot, InitPostmasterChildSlots, MaxLivePostmasterChildren, PMChild,
    SetActiveChildPid,
};
use types_core::init::BackendType;

use super::*;

static SETUP: Once = Once::new();
static SERIAL: Mutex<()> = Mutex::new(());

const TEST_MAX_CONNECTIONS: i32 = 10;
const TEST_MAX_WAL_SENDERS: i32 = 3;
const TEST_AUTOVAC_WORKER_SLOTS: i32 = 2;
const TEST_MAX_WORKER_PROCESSES: i32 = 4;

fn setup_once() {
    SETUP.call_once(|| {
        init_small_seams::max_connections::set(|| TEST_MAX_CONNECTIONS);
        walsender_seams::max_wal_senders::set(|| TEST_MAX_WAL_SENDERS);
        autovacuum_seams::autovacuum_worker_slots::set(
            || TEST_AUTOVAC_WORKER_SLOTS,
        );
        init_small_seams::max_worker_processes::set(|| TEST_MAX_WORKER_PROCESSES);
        launch_backend_seams::postmaster_child_name::set(|_bt| "test-child");
        pmchild_seams::max_live_postmaster_children::set(
            MaxLivePostmasterChildren,
        );
        InitPostmasterChildSlots();
        pmsignal::PMSignalShmemInit().unwrap();
    });
}

#[test]
fn marks_matching_pid_and_returns_found() {
    let _g = SERIAL.lock().unwrap();
    setup_once();
    InitPostmasterChildSlots();

    // Assign a bgworker child and give it a pid (as BackendStartup would).
    let child: PMChild = AssignPostmasterChildSlot(BackendType::BgWorker).unwrap();
    assert!(SetActiveChildPid(child.child_slot, 4242));

    // Unknown pid -> false, nothing changed.
    assert!(!PostmasterMarkPIDForWorkerNotify(9999));

    // Matching pid -> true.
    assert!(PostmasterMarkPIDForWorkerNotify(4242));

    // The flag is now set on that entry. (AssignPostmasterChildSlot defaults
    // bgworker_notify to true, so to prove the write we re-init, assign as an
    // autovac worker — which BackendStartup-style callers set notify=false —
    // and verify the mark flips it.)
    InitPostmasterChildSlots();
    let av = AssignPostmasterChildSlot(BackendType::AutovacWorker).unwrap();
    pmchild::SetActiveChildBgworkerInfo(av.child_slot, None, false);
    assert!(SetActiveChildPid(av.child_slot, 5151));

    let before = pmchild::ActiveChildListSnapshot();
    assert!(before
        .iter()
        .any(|c| c.child_slot == av.child_slot && !c.bgworker_notify));

    assert!(PostmasterMarkPIDForWorkerNotify(5151));

    let after = pmchild::ActiveChildListSnapshot();
    assert!(after
        .iter()
        .any(|c| c.child_slot == av.child_slot && c.bgworker_notify));
}
