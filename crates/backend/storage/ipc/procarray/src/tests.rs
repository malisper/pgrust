use super::*;
use init_small::globals as g;
use std::sync::{Mutex, Once};
use types_core::BackendType;

// One backend slot per test thread that calls my_backend(); keep headroom
// over the my_backend() call-site count or InitProcess FATALs mid-suite.
const MAX_CONNECTIONS: i32 = 24;
// Bump when claim_other() call sites grow: the claimable simulated-backend
// range is MAX_BACKENDS - MAX_CONNECTIONS (20 today for 19 claim_other()s).
const MAX_WORKER_PROCESSES: i32 = 13;
const NUM_SPECIAL: i32 = types_storage::storage::NUM_SPECIAL_WORKER_PROCS;
const MAX_BACKENDS: i32 = MAX_CONNECTIONS + 3 + MAX_WORKER_PROCESSES + 2 + NUM_SPECIAL;

fn test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

static RECOVERY: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

// pgstat wait reporting trace: each start pushes its wait_event_info, each
// end pushes 0.
static WAIT_EVENTS: Mutex<Vec<u32>> = Mutex::new(Vec::new());

struct RecoveryOn;
impl RecoveryOn {
    fn new() -> Self {
        RECOVERY.store(true, Relaxed);
        RecoveryOn
    }
}
impl Drop for RecoveryOn {
    fn drop(&mut self) {
        RECOVERY.store(false, Relaxed);
    }
}

fn setup() {
    static SETUP: Once = Once::new();
    SETUP.call_once(|| {
        g::SetMaxConnections(MAX_CONNECTIONS);
        g::set_max_worker_processes(MAX_WORKER_PROCESSES);
        g::SetMaxBackends(MAX_BACKENDS);
        g::SetMyProcPid(4242);

        pg_sema_seams::pg_semaphore_create::set(|_| {});
        pg_sema_seams::pg_semaphore_reset::set(|_| {});
        // A follower in ProcArrayGroupClearXid sleeps on its semaphore until
        // the leader clears its membership; no leader runs in these tests,
        // so the fake semaphore plays the leader for a group member.
        pg_sema_seams::pg_semaphore_lock::set(|procno| {
            let proc = GetPGProcByNumber(procno);
            if proc.procArrayGroupMember.load(Relaxed) {
                proc.procArrayGroupNext
                    .value
                    .store(INVALID_PROC_NUMBER as u32, Relaxed);
                proc.procArrayGroupMember.store(false, Relaxed);
            }
        });
        pg_sema_seams::pg_semaphore_unlock::set(|_| {});
        s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
        s_lock_seams::finish_spin_delay::set(|_| {});
        s_lock_seams::set_spins_per_delay::set(|_| {});
        s_lock_seams::update_spins_per_delay::set(|v| v);
        latch_seams::own_latch::set(|_| {});
        latch_seams::disown_latch::set(|_| {});
        latch_seams::set_latch::set(|_| {});
        latch_seams::set_latch_my_latch::set(|| {});
        latch_seams::wait_latch_my_latch::set(|_, _, _| 0);
        latch_seams::reset_latch_my_latch::set(|| {});
        miscinit_seams::switch_to_shared_latch::set(|| {});
        miscinit_seams::switch_back_to_local_latch::set(|| {});
        waitevent_seams::pgstat_set_wait_event_storage::set(|_| {});
        waitevent_seams::pgstat_report_wait_start::set(|info| {
            WAIT_EVENTS.lock().unwrap().push(info);
        });
        waitevent_seams::pgstat_report_wait_end::set(|| {
            WAIT_EVENTS.lock().unwrap().push(0);
        });
        waitevent_seams::pgstat_reset_wait_event_storage::set(|| {});
        ipc_seams::on_shmem_exit::set(|_, _| {});
        deadlock_seams::init_dead_lock_checking::set(|| Ok(()));
        pmsignal_seams::register_postmaster_child_active::set(|| {});
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

        xact_seams::transaction_id_is_current_transaction_id::set(|_| false);
        transam_xlog_seams::recovery_in_progress::set(|| RECOVERY.load(Relaxed));
        transam_seams::transaction_id_did_abort::set(|_| Ok(false));
        transam_seams::transaction_id_did_commit::set(|_| Ok(false));
        transam_seams::transaction_id_latest::set(|main, subs| {
            let mut latest = main;
            for &s in subs {
                if types_core::TransactionIdFollows(s, latest) {
                    latest = s;
                }
            }
            latest
        });
        subtrans_seams::sub_trans_get_topmost_transaction::set(Ok);
        subtrans_seams::extend_subtrans::set(|_| Ok(()));
        subtrans_seams::sub_trans_set_parent::set(|_, _| Ok(()));
        twophase_seams::standby_transaction_id_is_prepared::set(|_| Ok(false));
        timestamp_seams::get_current_timestamp::set(|| 0);
        procarray_seams::standby_release_old_locks::set(|_| Ok(()));

        lwlock::CreateLWLocks(false).unwrap();
        lmgr_proc::init_seams();
        lmgr_proc::InitProcGlobal(&lmgr_proc::ProcGlobalConfig {
            autovacuum_worker_slots: 3,
            max_wal_senders: 2,
            max_prepared_xacts: 2,
            fastpath_lock_groups_per_backend: 1,
        });
        init_seams();
        varsup::VarsupShmemInit();
        ProcArrayShmemInit();
    });
}

thread_local! {
    static THREAD_PROC: Cell<bool> = const { Cell::new(false) };
}

fn my_backend() -> ProcNumber {
    setup();
    if !THREAD_PROC.get() {
        g::SetMyProcPid(4242);
        lmgr_proc::InitProcess(BackendType::Backend).expect("InitProcess");
        ProcArrayAdd(lmgr_proc::MyProc().unwrap()).expect("ProcArrayAdd");
        THREAD_PROC.set(true);
    }
    lmgr_proc::MyProc().unwrap()
}

// Claim simulated-backend slots from the autovac/bgworker range, which
// InitProcess(Backend) never hands out in these tests.
fn claim_other() -> ProcNumber {
    use std::sync::atomic::AtomicI32;
    static NEXT: AtomicI32 = AtomicI32::new(MAX_CONNECTIONS);
    let p = NEXT.fetch_add(1, Relaxed);
    assert!(p < MAX_BACKENDS);
    p
}

fn other_proc_running(procno: ProcNumber, xid: TransactionId) {
    let proc = GetPGProcByNumber(procno);
    proc.xid.value.store(xid, Relaxed);
    proc.pgxactoff.store(-1, Relaxed);
    ProcArrayAdd(procno).expect("ProcArrayAdd other");
}

fn other_proc_end(procno: ProcNumber, latest: TransactionId) {
    ProcArrayEndTransaction(procno, latest).expect("end xact");
    ProcArrayRemove(procno, InvalidTransactionId).expect("remove");
}

fn take_snapshot(snap: &mut SnapshotData<'static>, mcx: Mcx<'static>) {
    GetSnapshotData(snap, mcx).expect("GetSnapshotData");
}

fn fresh_snapshot(mcx: Mcx<'static>) -> SnapshotData<'static> {
    SnapshotData::sentinel(mcx, types_snapshot::SnapshotType::SNAPSHOT_MVCC)
}

fn leaked_mcx() -> Mcx<'static> {
    Box::leak(Box::new(mcx::MemoryContext::new("procarray-test"))).mcx()
}

#[test]
fn lwlock_ids_match_lwlocklist_h() {
    assert_eq!(XID_GEN_LOCK, 3);
    assert_eq!(PROC_ARRAY_LOCK, 4);
}

#[test]
fn snapshot_includes_running_xacts_and_computes_bounds() {
    let _g = test_lock();
    let me = my_backend();
    let mcx = leaked_mcx();

    let tv = TransamVariables();
    tv.latestCompletedXid
        .store(FullTransactionId::from_epoch_and_xid(0, 100).value, Relaxed);

    // Simulate a concurrent writer with xid 90 on a free PGPROC slot.
    let other = claim_other();
    other_proc_running(other, 90);

    let mut snap = fresh_snapshot(mcx);
    take_snapshot(&mut snap, mcx);

    assert_eq!(snap.xmax, 101);
    assert_eq!(snap.xmin, 90);
    assert_eq!(snap.xcnt, 1);
    assert_eq!(snap.xip[0], 90);
    assert!(!snap.suboverflowed);
    // curcid comes from the direct xact dep now (no seam to stub): outside a
    // transaction the mirror holds xact's boot value.
    assert_eq!(snap.curcid.get(), xact::GetCurrentCommandId(false).unwrap());
    assert_eq!(RecentXmin(), 90);
    assert_eq!(GetPGProcByNumber(me).xmin.read(), 90);

    assert!(TransactionIdIsInProgress(90).unwrap());
    assert!(!TransactionIdIsInProgress(80).unwrap());
    // XIDs past latestCompletedXid are always considered running.
    assert!(TransactionIdIsInProgress(101).unwrap());

    other_proc_end(other, 90);
    GetPGProcByNumber(me).xmin.value.store(0, Relaxed);
    set_transaction_xmin(InvalidTransactionId);
}

#[test]
fn reuse_fastpath_fires_and_invalidates_on_xact_completion() {
    let _g = test_lock();
    let me = my_backend();
    let mcx = leaked_mcx();

    let mut snap = fresh_snapshot(mcx);

    let builds0 = snapshot_full_builds();
    let hits0 = snapshot_reuse_hits();

    take_snapshot(&mut snap, mcx);
    assert_eq!(snapshot_full_builds(), builds0 + 1);
    assert_eq!(snapshot_reuse_hits(), hits0);
    assert_ne!(snap.snapXactCompletionCount, 0);
    let first_count = snap.snapXactCompletionCount;

    // Same struct, no transaction completed since: the reuse path must fire.
    take_snapshot(&mut snap, mcx);
    assert_eq!(snapshot_full_builds(), builds0 + 1, "full rebuild instead of reuse");
    assert_eq!(snapshot_reuse_hits(), hits0 + 1, "reuse fastpath was not CALLED");
    assert_eq!(snap.snapXactCompletionCount, first_count);

    // A write transaction ends: xactCompletionCount moves, reuse must miss.
    let other = claim_other();
    other_proc_running(other, snap.xmax);
    other_proc_end(other, snap.xmax);

    take_snapshot(&mut snap, mcx);
    assert_eq!(snapshot_full_builds(), builds0 + 2);
    assert_eq!(snapshot_reuse_hits(), hits0 + 1);
    assert!(snap.snapXactCompletionCount > first_count);

    GetPGProcByNumber(me).xmin.value.store(0, Relaxed);
    set_transaction_xmin(InvalidTransactionId);
}

#[test]
fn end_transaction_clears_dense_arrays_and_bumps_completion() {
    let _g = test_lock();
    let me = my_backend();
    let proc = GetPGProcByNumber(me);
    let hdr = ProcGlobal();

    let tv = TransamVariables();
    let count0 = tv.xactCompletionCount.load(Relaxed);
    let latest0 = latest_completed_xid().xid();
    let myxid = latest0 + 7;

    proc.xid.value.store(myxid, Relaxed);
    let off = proc.pgxactoff.load(Relaxed) as usize;
    hdr.xids[off].value.store(myxid, Relaxed);

    ProcArrayEndTransaction(me, myxid).unwrap();

    assert_eq!(proc.xid.read(), InvalidTransactionId);
    assert_eq!(hdr.xids[off].read(), InvalidTransactionId);
    assert_eq!(tv.xactCompletionCount.load(Relaxed), count0 + 1);
    assert_eq!(latest_completed_xid().xid(), myxid);
    assert_eq!(proc.xmin.read(), InvalidTransactionId);
}

#[test]
fn add_remove_keeps_pgprocnos_sorted_and_offsets_dense() {
    let _g = test_lock();
    let me = my_backend();
    let arrayP = procArray();
    let hdr = ProcGlobal();

    let base = unsafe { arrayP.numProcs.get() };
    let others: Vec<ProcNumber> = (0..3).map(|_| claim_other()).collect();
    for (i, &p) in others.iter().enumerate() {
        other_proc_running(p, 200 + i as TransactionId);
    }
    assert_eq!(unsafe { arrayP.numProcs.get() }, base + 3);

    let n = unsafe { arrayP.numProcs.get() } as usize;
    for i in 0..n {
        let p = unsafe { arrayP.pgprocnos[i].get() };
        assert_eq!(hdr.allProcs[p as usize].pgxactoff.load(Relaxed), i as i32);
        if i > 0 {
            assert!(unsafe { arrayP.pgprocnos[i - 1].get() } < p);
        }
    }

    // Remove the middle one; offsets must re-densify.
    other_proc_end(others[1], 201);
    let n = unsafe { arrayP.numProcs.get() } as usize;
    assert_eq!(n as i32, base + 2);
    for i in 0..n {
        let p = unsafe { arrayP.pgprocnos[i].get() };
        assert_eq!(hdr.allProcs[p as usize].pgxactoff.load(Relaxed), i as i32);
    }

    other_proc_end(others[0], 200);
    other_proc_end(others[2], 202);
    GetPGProcByNumber(me).xmin.value.store(0, Relaxed);
    set_transaction_xmin(InvalidTransactionId);
}

#[test]
fn in_progress_finds_cached_subxids() {
    let _g = test_lock();
    let me = my_backend();
    let mcx = leaked_mcx();

    let other = claim_other();
    let top: TransactionId = 500;
    let sub: TransactionId = 501;
    {
        let proc = GetPGProcByNumber(other);
        proc.xid.value.store(top, Relaxed);
        proc.pgxactoff.store(-1, Relaxed);
        let mut cache = unsafe { proc.subxids.get() };
        cache.xids[0] = sub;
        unsafe { proc.subxids.set(cache) };
        unsafe { proc.subxidStatus.set(types_storage::storage::XidCacheStatus {
            count: 1,
            overflowed: false,
        }) };
    }
    ProcArrayAdd(other).unwrap();
    TransamVariables()
        .latestCompletedXid
        .store(FullTransactionId::from_epoch_and_xid(0, 502).value, Relaxed);

    // Refresh RecentXmin so the fast bail-out doesn't hide the array walk.
    let mut snap = fresh_snapshot(mcx);
    take_snapshot(&mut snap, mcx);
    assert!(snap.subxip.contains(&sub));

    assert!(TransactionIdIsInProgress(top).unwrap());
    assert!(TransactionIdIsInProgress(sub).unwrap());
    assert!(!TransactionIdIsInProgress(499).unwrap());
    // The not-in-progress result is cached.
    assert!(!TransactionIdIsInProgress(499).unwrap());

    other_proc_end(other, top);
    GetPGProcByNumber(me).xmin.value.store(0, Relaxed);
    set_transaction_xmin(InvalidTransactionId);
}

#[test]
fn running_transaction_data_reports_assigned_xids_and_holds_locks() {
    let _g = test_lock();
    let _me = my_backend();

    let tv = TransamVariables();
    tv.latestCompletedXid
        .store(FullTransactionId::from_epoch_and_xid(0, 200).value, Relaxed);
    tv.nextXid
        .store(FullTransactionId::from_epoch_and_xid(0, 201).value, Relaxed);

    let other = claim_other();
    other_proc_running(other, 190);

    GetRunningTransactionData(|running| {
        assert_eq!(running.xcnt, 1);
        assert_eq!(running.subxcnt, 0);
        assert_eq!(running.xids, &[190]);
        assert!(!running.subxid_overflow);
        assert_eq!(running.next_xid, 201);
        assert_eq!(running.oldest_running_xid, 190);
        assert_eq!(running.latest_completed_xid, 200);
        // The caller-releases contract: both locks are held here.
        lwlock::LWLockRelease(lwlock::main_lock(PROC_ARRAY_LOCK)).expect("PAL held");
        lwlock::LWLockRelease(lwlock::main_lock(XID_GEN_LOCK)).expect("XidGen held");
        Ok(())
    })
    .expect("GetRunningTransactionData");

    other_proc_end(other, 190);
}

#[test]
fn proc_number_transaction_ids_and_pid_lookup() {
    let _g = test_lock();
    let _me = my_backend();

    assert_eq!(
        ProcNumberGetTransactionIds(-1),
        (InvalidTransactionId, InvalidTransactionId, 0, false)
    );
    assert_eq!(
        ProcNumberGetTransactionIds(ProcGlobal().allProcs.len() as ProcNumber),
        (InvalidTransactionId, InvalidTransactionId, 0, false)
    );

    let other = claim_other();
    other_proc_running(other, 700);
    let proc = GetPGProcByNumber(other);
    proc.xmin.value.store(695, Relaxed);
    unsafe { proc.subxidStatus.set(types_storage::storage::XidCacheStatus {
        count: 2,
        overflowed: true,
    }) };

    // pid == 0: dummy PGPROC, ids withheld and PID lookup never matches.
    assert_eq!(
        ProcNumberGetTransactionIds(other),
        (InvalidTransactionId, InvalidTransactionId, 0, false)
    );
    assert!(BackendPidGetProc(0).is_none());

    proc.pid.store(9911, Relaxed);
    assert_eq!(ProcNumberGetTransactionIds(other), (700, 695, 2, true));
    assert!(std::ptr::eq(BackendPidGetProc(9911).unwrap(), proc));
    assert!(BackendPidGetProc(555_555).is_none());

    proc.pid.store(0, Relaxed);
    proc.xmin.value.store(InvalidTransactionId, Relaxed);
    unsafe { proc.subxidStatus.set(Default::default()) };
    other_proc_end(other, 700);
}

#[test]
fn lock_free_reuse_republishes_xmin_at_statement_boundary() {
    let _g = test_lock();
    let me = my_backend();
    let mcx = leaked_mcx();
    let proc = GetPGProcByNumber(me);

    let mut snap = fresh_snapshot(mcx);
    take_snapshot(&mut snap, mcx);
    let hits0 = snapshot_reuse_hits();

    // Statement boundary in READ COMMITTED: snapmgr cleared the proc xmin.
    proc.xmin.value.store(InvalidTransactionId, Relaxed);
    set_transaction_xmin(InvalidTransactionId);

    // Reuse hit must republish the snapshot's xmin (publish-then-verify).
    take_snapshot(&mut snap, mcx);
    assert_eq!(snapshot_reuse_hits(), hits0 + 1);
    assert_eq!(proc.xmin.read(), snap.xmin);
    assert_eq!(TransactionXmin(), snap.xmin);
    assert_eq!(RecentXmin(), snap.xmin);

    // Miss (counter moved): the speculative publish is retracted, then the
    // full build under the lock republishes the fresh xmin.
    proc.xmin.value.store(InvalidTransactionId, Relaxed);
    set_transaction_xmin(InvalidTransactionId);
    let other = claim_other();
    other_proc_running(other, snap.xmax);
    other_proc_end(other, snap.xmax);
    take_snapshot(&mut snap, mcx);
    assert_eq!(snapshot_reuse_hits(), hits0 + 1);
    assert_eq!(proc.xmin.read(), snap.xmin);
    assert_eq!(TransactionXmin(), snap.xmin);

    proc.xmin.value.store(0, Relaxed);
    set_transaction_xmin(InvalidTransactionId);
}

#[test]
fn lock_free_reuse_keeps_older_valid_xmin() {
    let _g = test_lock();
    let me = my_backend();
    let mcx = leaked_mcx();
    let proc = GetPGProcByNumber(me);

    let mut snap = fresh_snapshot(mcx);
    take_snapshot(&mut snap, mcx);
    let hits0 = snapshot_reuse_hits();

    // A registered older snapshot still pins a lower xmin: reuse must not
    // raise it (C: only set when invalid).
    let older = if snap.xmin > 2 { snap.xmin - 1 } else { snap.xmin };
    proc.xmin.value.store(older, Relaxed);
    set_transaction_xmin(older);

    take_snapshot(&mut snap, mcx);
    assert_eq!(snapshot_reuse_hits(), hits0 + 1);
    assert_eq!(proc.xmin.read(), older);
    assert_eq!(TransactionXmin(), older);
    assert_eq!(RecentXmin(), snap.xmin);

    proc.xmin.value.store(0, Relaxed);
    set_transaction_xmin(InvalidTransactionId);
}

/// GL-OLTPREG-1. A reuse attempt that CANNOT succeed must not touch
/// MyProc->xmin and must not execute the SeqCst fence. Born red at
/// 2216f2261e: publish-then-verify wrote and retracted the xmin, and paid a
/// full barrier between them, on EVERY miss — and in a write-heavy workload
/// at high client counts the completion counter moves between essentially
/// every pair of snapshots, so the miss is the common case, not the rare one.
///
/// The counter is the only observable: the write half is retracted before the
/// function returns, so no caller-visible state distinguishes the two shapes.
#[test]
fn doomed_reuse_does_not_speculatively_publish_xmin() {
    let _g = test_lock();
    let me = my_backend();
    let mcx = leaked_mcx();
    let proc = GetPGProcByNumber(me);

    let mut snap = fresh_snapshot(mcx);
    take_snapshot(&mut snap, mcx);

    // Arm the miss: a write transaction completes, so the reuse counter check
    // is guaranteed to fail on the next take.
    let other = claim_other();
    other_proc_running(other, snap.xmax);
    other_proc_end(other, snap.xmax);

    // Arm the publish arm: an INVALID proc xmin is what makes
    // publish-then-verify want to write (the statement-boundary shape of
    // lock_free_reuse_republishes_xmin_at_statement_boundary).
    proc.xmin.value.store(InvalidTransactionId, Relaxed);
    set_transaction_xmin(InvalidTransactionId);

    let hits0 = snapshot_reuse_hits();
    let builds0 = snapshot_full_builds();
    let pubs0 = snapshot_reuse_speculative_publishes();

    take_snapshot(&mut snap, mcx);

    // The reuse really did miss (otherwise the assertion below is vacuous).
    assert_eq!(snapshot_reuse_hits(), hits0, "reuse hit; the miss was not armed");
    assert_eq!(snapshot_full_builds(), builds0 + 1, "no full build happened");
    assert_eq!(
        snapshot_reuse_speculative_publishes(),
        pubs0,
        "a doomed reuse still published MyProc->xmin and fenced"
    );
    // The full build under the lock still published the fresh xmin.
    assert_eq!(proc.xmin.read(), snap.xmin);

    proc.xmin.value.store(0, Relaxed);
    set_transaction_xmin(InvalidTransactionId);
}

/// The pre-check must not cost the HIT its publish: a reachable reuse still
/// runs the full publish-then-verify triple (constraint 1 unchanged).
#[test]
fn reachable_reuse_still_publishes_xmin() {
    let _g = test_lock();
    let me = my_backend();
    let mcx = leaked_mcx();
    let proc = GetPGProcByNumber(me);

    let mut snap = fresh_snapshot(mcx);
    take_snapshot(&mut snap, mcx);

    proc.xmin.value.store(InvalidTransactionId, Relaxed);
    set_transaction_xmin(InvalidTransactionId);

    let hits0 = snapshot_reuse_hits();
    let pubs0 = snapshot_reuse_speculative_publishes();

    take_snapshot(&mut snap, mcx);

    assert_eq!(snapshot_reuse_hits(), hits0 + 1, "reuse did not fire");
    assert_eq!(
        snapshot_reuse_speculative_publishes(),
        pubs0 + 1,
        "the reachable reuse skipped the publish half"
    );
    assert_eq!(proc.xmin.read(), snap.xmin);

    proc.xmin.value.store(0, Relaxed);
    set_transaction_xmin(InvalidTransactionId);
}

use crate::known_assigned::{self, test_support as kax};

fn kax_add(from: TransactionId, to: TransactionId) {
    known_assigned::KnownAssignedXidsAdd(from, to, true).expect("KnownAssignedXidsAdd");
}

#[test]
fn kax_add_get_exists_and_oldest() {
    let _g = test_lock();
    setup();
    kax::kax_reset();

    kax_add(10, 14);
    assert_eq!(kax::get_all(InvalidTransactionId), vec![10, 11, 12, 13, 14]);
    assert_eq!(kax::kax_counts().0, 5);
    assert!(known_assigned::KnownAssignedXidExists(12));
    assert!(!known_assigned::KnownAssignedXidExists(15));
    assert_eq!(known_assigned::KnownAssignedXidsGetOldestXmin(), 10);
}

#[test]
fn kax_out_of_order_insertion_errors() {
    let _g = test_lock();
    setup();
    kax::kax_reset();

    kax_add(20, 20);
    assert!(known_assigned::KnownAssignedXidsAdd(20, 20, true).is_err());
    assert!(known_assigned::KnownAssignedXidsAdd(15, 15, true).is_err());
}

#[test]
fn kax_remove_advances_tail_and_ignores_absent() {
    let _g = test_lock();
    setup();
    kax::kax_reset();

    kax_add(10, 14);
    kax::remove(10);
    kax::remove(11);
    assert_eq!(kax::kax_counts(), (3, 2, 5));
    assert!(!known_assigned::KnownAssignedXidExists(10));
    assert_eq!(known_assigned::KnownAssignedXidsGetOldestXmin(), 12);

    kax::remove(99);
    assert_eq!(kax::kax_counts().0, 3);

    kax::remove(13);
    assert_eq!(kax::get_all(InvalidTransactionId), vec![12, 14]);

    kax::remove(12);
    kax::remove(14);
    assert_eq!(kax::kax_counts(), (0, 0, 0));
}

#[test]
fn kax_remove_tree_removes_top_and_subxids() {
    let _g = test_lock();
    setup();
    kax::kax_reset();

    kax_add(30, 35);
    kax::remove_tree(31, &[33, 34]).unwrap();
    assert_eq!(kax::get_all(InvalidTransactionId), vec![30, 32, 35]);

    kax::remove_tree(InvalidTransactionId, &[30]).unwrap();
    assert_eq!(kax::get_all(InvalidTransactionId), vec![32, 35]);
}

#[test]
fn kax_remove_preceding_prunes_and_clears() {
    let _g = test_lock();
    setup();
    kax::kax_reset();

    kax_add(40, 44);
    kax::remove_preceding(43).unwrap();
    assert_eq!(kax::get_all(InvalidTransactionId), vec![43, 44]);
    assert_eq!(kax::kax_counts().0, 2);
    assert_eq!(known_assigned::KnownAssignedXidsGetOldestXmin(), 43);

    kax::remove_preceding(InvalidTransactionId).unwrap();
    assert_eq!(kax::kax_counts(), (0, 0, 0));
    assert_eq!(
        known_assigned::KnownAssignedXidsGetOldestXmin(),
        InvalidTransactionId
    );
}

#[test]
fn kax_get_and_set_xmin_filters_at_xmax() {
    let _g = test_lock();
    setup();
    kax::kax_reset();

    kax_add(50, 54);
    kax::remove(51);

    let mut out = Vec::new();
    let mut xmin = 100;
    let n = known_assigned::KnownAssignedXidsGetAndSetXmin(|_, x| out.push(x), &mut xmin, 53);
    assert_eq!(n, 2);
    assert_eq!(out, vec![50, 52]);
    assert_eq!(xmin, 50);

    let mut out = Vec::new();
    let mut xmin = 7;
    known_assigned::KnownAssignedXidsGetAndSetXmin(|_, x| out.push(x), &mut xmin, InvalidTransactionId);
    assert_eq!(out, vec![50, 52, 53, 54]);
    assert_eq!(xmin, 7);
}

#[test]
fn kax_add_compresses_when_out_of_space() {
    let _g = test_lock();
    setup();
    kax::kax_reset();

    let max = GetMaxSnapshotSubxidCount() as u32;
    kax_add(10, 10 + max - 1); // fill every slot
    assert!(known_assigned::KnownAssignedXidsAdd(10 + max, 10 + max, true).is_err());

    kax::remove(10);
    kax::remove(12);
    kax_add(10 + max, 10 + max + 1);
    assert_eq!(kax::kax_counts().0 as u32, max);
    assert!(known_assigned::KnownAssignedXidExists(11));
    assert!(known_assigned::KnownAssignedXidExists(10 + max + 1));
    assert!(!known_assigned::KnownAssignedXidExists(12));
    assert_eq!(known_assigned::KnownAssignedXidsGetOldestXmin(), 11);
}

#[test]
fn kax_add_and_search_across_wraparound() {
    let _g = test_lock();
    setup();
    kax::kax_reset();

    kax_add(u32::MAX - 1, 3);
    assert_eq!(kax::kax_counts().0, 3);
    assert_eq!(kax::get_all(InvalidTransactionId), vec![u32::MAX - 1, u32::MAX, 3]);
    assert!(known_assigned::KnownAssignedXidExists(u32::MAX));
    assert!(known_assigned::KnownAssignedXidExists(3));
    assert_eq!(known_assigned::KnownAssignedXidsGetOldestXmin(), u32::MAX - 1);

    let mut out = Vec::new();
    let mut xmin = 10;
    known_assigned::KnownAssignedXidsGetAndSetXmin(|_, x| out.push(x), &mut xmin, 3);
    assert_eq!(out, vec![u32::MAX - 1, u32::MAX]);
    assert_eq!(xmin, u32::MAX - 1);
}

#[test]
fn record_known_assigned_transaction_ids_fills_gaps() {
    let _g = test_lock();
    setup();
    kax::kax_reset();
    xlogutils::set_standby_state(xlogutils::STANDBY_SNAPSHOT_READY);
    kax::set_latest_observed_xid(200);

    RecordKnownAssignedTransactionIds(205).unwrap();
    assert_eq!(kax::get_all(InvalidTransactionId), vec![201, 202, 203, 204, 205]);
    assert_eq!(kax::latest_observed_xid(), 205);
    let next = FullTransactionId::from_u64(TransamVariables().nextXid.load(Relaxed));
    assert!(!TransactionIdPrecedes(next.xid(), 206));

    RecordKnownAssignedTransactionIds(203).unwrap();
    assert_eq!(kax::kax_counts().0, 5);

    xlogutils::set_standby_state(xlogutils::STANDBY_INITIALIZED);
    RecordKnownAssignedTransactionIds(210).unwrap();
    assert_eq!(kax::kax_counts().0, 5);
    assert_eq!(kax::latest_observed_xid(), 210);
    xlogutils::set_standby_state(xlogutils::STANDBY_DISABLED);
}

#[test]
fn expire_tree_removes_and_maintains_latest_completed() {
    let _g = test_lock();
    setup();
    kax::kax_reset();
    xlogutils::set_standby_state(xlogutils::STANDBY_SNAPSHOT_READY);

    let tv = TransamVariables();
    tv.nextXid.store(FullTransactionId::from_epoch_and_xid(0, 400).value, Relaxed);
    tv.latestCompletedXid.store(FullTransactionId::from_epoch_and_xid(0, 300).value, Relaxed);
    let completions = tv.xactCompletionCount.load(Relaxed);

    kax_add(310, 314);
    ExpireTreeKnownAssignedTransactionIds(311, &[313, 314], 314).unwrap();
    assert_eq!(kax::get_all(InvalidTransactionId), vec![310, 312]);
    assert_eq!(FullTransactionId::from_u64(tv.latestCompletedXid.load(Relaxed)).xid(), 314);
    assert_eq!(tv.xactCompletionCount.load(Relaxed), completions + 1);
    xlogutils::set_standby_state(xlogutils::STANDBY_DISABLED);
}

#[test]
fn expire_all_and_old_reset_overflow_state() {
    let _g = test_lock();
    setup();
    kax::kax_reset();

    let tv = TransamVariables();
    tv.nextXid.store(FullTransactionId::from_epoch_and_xid(0, 600).value, Relaxed);
    tv.latestCompletedXid.store(FullTransactionId::from_epoch_and_xid(0, 500).value, Relaxed);

    kax_add(510, 514);
    kax::set_last_overflowed_xid(513);
    ExpireOldKnownAssignedTransactionIds(512).unwrap();
    assert_eq!(kax::get_all(InvalidTransactionId), vec![512, 513, 514]);
    assert_eq!(kax::last_overflowed_xid(), 513);
    ExpireOldKnownAssignedTransactionIds(514).unwrap();
    assert_eq!(kax::last_overflowed_xid(), InvalidTransactionId);
    assert_eq!(kax::get_all(InvalidTransactionId), vec![514]);

    kax::set_last_overflowed_xid(599);
    ExpireAllKnownAssignedTransactionIds().unwrap();
    assert_eq!(kax::kax_counts(), (0, 0, 0));
    assert_eq!(kax::last_overflowed_xid(), InvalidTransactionId);
    assert_eq!(FullTransactionId::from_u64(tv.latestCompletedXid.load(Relaxed)).xid(), 599);
}

#[test]
fn apply_recovery_info_builds_ready_snapshot() {
    let _g = test_lock();
    setup();
    kax::kax_reset();
    xlogutils::set_standby_state(xlogutils::STANDBY_INITIALIZED);

    let tv = TransamVariables();
    tv.nextXid.store(FullTransactionId::from_epoch_and_xid(0, 700).value, Relaxed);
    tv.latestCompletedXid.store(FullTransactionId::from_epoch_and_xid(0, 699).value, Relaxed);

    ProcArrayInitRecovery(700);
    assert_eq!(kax::latest_observed_xid(), 699);

    let mcx = leaked_mcx();
    let mut xids = mcx::vec_with_capacity_in(mcx, 3).unwrap();
    xids.push(705);
    xids.push(701);
    xids.push(705);
    let running = RunningTransactionsData {
        xcnt: 3,
        subxcnt: 0,
        subxid_status: types_storage::storage::SUBXIDS_IN_ARRAY,
        nextXid: 710,
        oldestRunningXid: 701,
        oldestDatabaseRunningXid: 701,
        latestCompletedXid: 706,
        xids,
    };
    ProcArrayApplyRecoveryInfo(&running).unwrap();

    assert_eq!(xlogutils::standby_state(), xlogutils::STANDBY_SNAPSHOT_READY);
    assert_eq!(kax::get_all(InvalidTransactionId), vec![701, 705]);
    assert_eq!(kax::last_overflowed_xid(), InvalidTransactionId);
    assert_eq!(kax::latest_observed_xid(), 709);
    assert_eq!(FullTransactionId::from_u64(tv.latestCompletedXid.load(Relaxed)).xid(), 706);

    let mut xids = mcx::vec_with_capacity_in(mcx, 0).unwrap();
    xids.clear();
    let running = RunningTransactionsData {
        xcnt: 0,
        subxcnt: 0,
        subxid_status: types_storage::storage::SUBXIDS_IN_ARRAY,
        nextXid: 712,
        oldestRunningXid: 705,
        oldestDatabaseRunningXid: 705,
        latestCompletedXid: 707,
        xids,
    };
    ProcArrayApplyRecoveryInfo(&running).unwrap();
    assert_eq!(kax::get_all(InvalidTransactionId), vec![705]);
    xlogutils::set_standby_state(xlogutils::STANDBY_DISABLED);
}

#[test]
fn apply_recovery_info_overflowed_snapshot_goes_pending() {
    let _g = test_lock();
    setup();
    kax::kax_reset();
    xlogutils::set_standby_state(xlogutils::STANDBY_INITIALIZED);

    let tv = TransamVariables();
    tv.nextXid.store(FullTransactionId::from_epoch_and_xid(0, 800).value, Relaxed);
    tv.latestCompletedXid.store(FullTransactionId::from_epoch_and_xid(0, 799).value, Relaxed);

    ProcArrayInitRecovery(800);
    let mcx = leaked_mcx();
    let mut xids = mcx::vec_with_capacity_in(mcx, 1).unwrap();
    xids.push(801);
    let running = RunningTransactionsData {
        xcnt: 1,
        subxcnt: 0,
        subxid_status: types_storage::storage::SUBXIDS_MISSING,
        nextXid: 805,
        oldestRunningXid: 801,
        oldestDatabaseRunningXid: 801,
        latestCompletedXid: 802,
        xids,
    };
    ProcArrayApplyRecoveryInfo(&running).unwrap();

    assert_eq!(xlogutils::standby_state(), xlogutils::STANDBY_SNAPSHOT_PENDING);
    assert_eq!(kax::standby_snapshot_pending_xmin(), 804);
    assert_eq!(kax::last_overflowed_xid(), 804);

    let mut xids = mcx::vec_with_capacity_in(mcx, 1).unwrap();
    xids.push(806);
    let running = RunningTransactionsData {
        xcnt: 1,
        subxcnt: 0,
        subxid_status: types_storage::storage::SUBXIDS_MISSING,
        nextXid: 807,
        oldestRunningXid: 806,
        oldestDatabaseRunningXid: 806,
        latestCompletedXid: 805,
        xids,
    };
    ProcArrayApplyRecoveryInfo(&running).unwrap();
    assert_eq!(xlogutils::standby_state(), xlogutils::STANDBY_SNAPSHOT_READY);
    xlogutils::set_standby_state(xlogutils::STANDBY_DISABLED);
}

#[test]
fn apply_xid_assignment_removes_subxids_and_marks_overflow() {
    let _g = test_lock();
    setup();
    kax::kax_reset();
    xlogutils::set_standby_state(xlogutils::STANDBY_SNAPSHOT_READY);
    kax::set_latest_observed_xid(910);

    kax_add(901, 906);
    ProcArrayApplyXidAssignment(901, &[904, 905, 906]).unwrap();
    assert_eq!(kax::get_all(InvalidTransactionId), vec![901, 902, 903]);
    assert_eq!(kax::last_overflowed_xid(), 906);
    xlogutils::set_standby_state(xlogutils::STANDBY_DISABLED);
}

#[test]
fn snapshot_in_recovery_uses_known_assigned_xids() {
    let _g = test_lock();
    let me = my_backend();
    let mcx = leaked_mcx();
    kax::kax_reset();

    let tv = TransamVariables();
    tv.nextXid.store(FullTransactionId::from_epoch_and_xid(0, 2000).value, Relaxed);
    tv.latestCompletedXid.store(FullTransactionId::from_epoch_and_xid(0, 1000).value, Relaxed);

    kax_add(950, 954);
    kax::remove(951);
    kax::set_last_overflowed_xid(940);

    let proc = GetPGProcByNumber(me);
    proc.xmin.value.store(InvalidTransactionId, Relaxed);
    set_transaction_xmin(InvalidTransactionId);

    let _r = RecoveryOn::new();
    let mut snap = fresh_snapshot(mcx);
    take_snapshot(&mut snap, mcx);

    assert!(snap.takenDuringRecovery);
    assert_eq!(snap.xmax, 1001);
    assert_eq!(snap.xmin, 950);
    assert_eq!(snap.xcnt, 0);
    assert_eq!(snap.subxcnt, 4);
    assert_eq!(&snap.subxip[..], &[950, 952, 953, 954]);
    assert!(!snap.suboverflowed);

    kax::set_last_overflowed_xid(950);
    proc.xmin.value.store(InvalidTransactionId, Relaxed);
    set_transaction_xmin(InvalidTransactionId);
    let mut snap2 = fresh_snapshot(mcx);
    take_snapshot(&mut snap2, mcx);
    assert!(snap2.suboverflowed);

    proc.xmin.value.store(InvalidTransactionId, Relaxed);
    set_transaction_xmin(InvalidTransactionId);
}

#[test]
fn xid_in_progress_in_recovery_consults_known_assigned() {
    let _g = test_lock();
    let _me = my_backend();
    kax::kax_reset();

    let tv = TransamVariables();
    tv.latestCompletedXid.store(FullTransactionId::from_epoch_and_xid(0, 3000).value, Relaxed);

    kax_add(2900, 2903);
    let _r = RecoveryOn::new();

    assert!(TransactionIdIsInProgress(2901).unwrap());
    assert!(!TransactionIdIsInProgress(2950).unwrap());
    assert!(TransactionIdIsInProgress(3050).unwrap());

    kax::set_last_overflowed_xid(2970);
    assert!(!TransactionIdIsInProgress(2960).unwrap());
    kax::set_last_overflowed_xid(InvalidTransactionId);
}

#[test]
fn horizons_in_recovery_fold_in_known_assigned_oldest() {
    let _g = test_lock();
    let me = my_backend();
    kax::kax_reset();

    let proc = GetPGProcByNumber(me);
    proc.xmin.value.store(InvalidTransactionId, Relaxed);
    set_transaction_xmin(InvalidTransactionId);

    let tv = TransamVariables();
    tv.latestCompletedXid.store(FullTransactionId::from_epoch_and_xid(0, 4000).value, Relaxed);

    kax_add(3900, 3901);
    let _r = RecoveryOn::new();
    let running = GetOldestTransactionIdConsideredRunning().unwrap();
    assert_eq!(running, 3900);
}

// MinimumActiveBackends (procarray.c): the commit_delay/commit_siblings
// group-commit gate. Counts OTHER backends with an assigned XID that hold a
// pid (not prepared-xact dummies) and are not blocked on a lock.
#[test]
fn minimum_active_backends_counts_other_active_backends() {
    let _g = test_lock();
    let me = my_backend();
    let my_pgproc = GetPGProcByNumber(me);

    // The installer must publish the gate (the group-commit delay consumer in
    // the WAL flush path only calls through the seam; an uninstalled seam is
    // a silent always-skip — the exact inert-GUC class this port closes).
    assert!(procarray_seams::minimum_active_backends::is_installed());

    // min == 0: always true (C's quick short-circuit).
    assert!(MinimumActiveBackends(0));

    // Myself never counts, even with an assigned XID and a live pid.
    my_pgproc.xid.value.store(4000, Relaxed);
    assert!(my_pgproc.pid.load(Relaxed) != 0);
    assert!(!MinimumActiveBackends(1));

    // One other backend in an active transaction reaches min = 1 only.
    let other = claim_other();
    let op = GetPGProcByNumber(other);
    op.pid.store(9001, Relaxed);
    other_proc_running(other, 4001);
    assert!(MinimumActiveBackends(1));
    assert!(!MinimumActiveBackends(2));

    // pid == 0 marks a prepared-xact dummy: not counted.
    op.pid.store(0, Relaxed);
    assert!(!MinimumActiveBackends(1));
    op.pid.store(9001, Relaxed);
    assert!(MinimumActiveBackends(1));

    // Blocked waiting for a lock: not counted (it cannot run until someone
    // else commits). The pointer is only null-tested, never dereferenced.
    unsafe { op.waitLock.set(core::ptr::NonNull::dangling().as_ptr()) };
    assert!(!MinimumActiveBackends(1));
    unsafe { op.waitLock.set(core::ptr::null_mut()) };
    assert!(MinimumActiveBackends(1));

    // No XID assigned: not counted.
    op.xid.value.store(InvalidTransactionId, Relaxed);
    assert!(!MinimumActiveBackends(1));
    op.xid.value.store(4001, Relaxed);

    // A second active sibling reaches min = 2.
    let other2 = claim_other();
    let op2 = GetPGProcByNumber(other2);
    op2.pid.store(9002, Relaxed);
    other_proc_running(other2, 4002);
    assert!(MinimumActiveBackends(2));
    assert!(!MinimumActiveBackends(3));

    // Cleanup: release the fabricated backends and restore my own proc.
    op2.pid.store(0, Relaxed);
    other_proc_end(other2, 4002);
    op.pid.store(0, Relaxed);
    other_proc_end(other, 4001);
    my_pgproc.xid.value.store(InvalidTransactionId, Relaxed);
}

// ---- CountOtherDBBackends autovacuum SIGTERM (procarray.c:3801-3807) ----

#[test]
fn count_other_db_backends_sigterms_conflicting_autovacuum() {
    use std::sync::atomic::AtomicBool;

    let _g = test_lock();
    let _me = my_backend();

    if !postgres_seams::check_for_interrupts::is_installed() {
        postgres_seams::check_for_interrupts::set(|| Ok(()));
    }
    // Globals are per-thread; setup() sized them only on its own thread.
    g::SetMaxBackends(MAX_BACKENDS);
    procsignal::ProcSignalShmemInit();

    const AV_DB: types_core::Oid = 90777;
    const AV_PID: i32 = 90001;

    // A simulated autovacuum worker connected to AV_DB.
    let other = claim_other();
    let proc = GetPGProcByNumber(other);
    proc.databaseId.store(AV_DB, Relaxed);
    proc.pid.store(AV_PID, Relaxed);
    proc.statusFlags.store(PROC_IS_AUTOVACUUM, Relaxed);
    proc.pgxactoff.store(-1, Relaxed);
    ProcArrayAdd(other).expect("ProcArrayAdd autovac");

    static GOT_SIGTERM: AtomicBool = AtomicBool::new(false);
    static AV_READY: AtomicBool = AtomicBool::new(false);
    GOT_SIGTERM.store(false, Relaxed);
    AV_READY.store(false, Relaxed);

    // The worker's signal plane: registers its procsignal identity, then
    // drains until the SIGTERM lands and "exits" (leaves the database).
    let worker = std::thread::spawn(move || {
        init_small::globals::SetMyProcNumber(other);
        init_small::globals::SetMyProcPid(AV_PID);
        procsignal::ProcSignalInit(&[]).expect("ProcSignalInit");
        procsignal::pqsignal_thread(
            libc::SIGTERM,
            procsignal::ThreadSignalHandler::Simple(|| {
                GOT_SIGTERM.store(true, std::sync::atomic::Ordering::SeqCst);
            }),
        );
        AV_READY.store(true, std::sync::atomic::Ordering::SeqCst);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while std::time::Instant::now() < deadline {
            procsignal::DrainThreadSignals().expect("drain");
            if GOT_SIGTERM.load(std::sync::atomic::Ordering::SeqCst) {
                // Terminated autovacuum: drop out of the target database.
                GetPGProcByNumber(other).databaseId.store(types_core::InvalidOid, Relaxed);
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        procsignal::ProcSignalRelease();
    });
    while !AV_READY.load(std::sync::atomic::Ordering::SeqCst) {
        std::thread::yield_now();
    }

    // DROP DATABASE's interlock: the conflicting autovacuum must be SIGTERMed
    // and, once it exits, the walk comes back clean well before the 5s cap.
    let res = CountOtherDBBackends(AV_DB).expect("CountOtherDBBackends");
    worker.join().unwrap();

    assert!(
        GOT_SIGTERM.load(std::sync::atomic::Ordering::SeqCst),
        "conflicting autovacuum worker never received SIGTERM"
    );
    assert_eq!(res, None, "autovacuum was killed, so no conflicts remain");

    // Cleanup: detach the simulated worker.
    ProcArrayRemove(other, InvalidTransactionId).expect("remove autovac");
    let proc = GetPGProcByNumber(other);
    proc.pid.store(0, Relaxed);
    proc.statusFlags.store(0, Relaxed);
}

// D3.5: arrays start small (SNAPSHOT_ARRAY_INITIAL), grow on observed demand
// with a build restart, and the demand window decays back after the spike.
#[test]
fn snapshot_arrays_demand_grow_and_decay() {
    let _g = test_lock();
    let me = my_backend();
    let mcx = leaked_mcx();

    const NSUB: usize = types_storage::storage::PGPROC_MAX_CACHED_SUBXIDS;
    // Three writers each with a full (non-overflowed) subxid cache: 192
    // subxids > the 64-entry initial subxip, forcing the grow-retry path.
    let mut others = Vec::new();
    let mut expected_subs = Vec::new();
    for i in 0..3u32 {
        let other = claim_other();
        let top: TransactionId = 2000 + i * 100;
        let proc = GetPGProcByNumber(other);
        proc.xid.value.store(top, Relaxed);
        proc.pgxactoff.store(-1, Relaxed);
        let mut cache = unsafe { proc.subxids.get() };
        for j in 0..NSUB {
            cache.xids[j] = top + 1 + j as TransactionId;
            expected_subs.push(top + 1 + j as TransactionId);
        }
        unsafe { proc.subxids.set(cache) };
        unsafe { proc.subxidStatus.set(types_storage::storage::XidCacheStatus {
            count: NSUB as u8,
            overflowed: false,
        }) };
        ProcArrayAdd(other).unwrap();
        others.push((other, top));
    }
    TransamVariables()
        .latestCompletedXid
        .store(FullTransactionId::from_epoch_and_xid(0, 3000).value, Relaxed);

    let mut snap = fresh_snapshot(mcx);
    assert_eq!(snap.subxip.capacity(), 0, "sentinel starts unallocated");
    take_snapshot(&mut snap, mcx);

    // Contents identical to what C's worst-case prealloc would produce.
    assert_eq!(snap.xcnt, 3);
    assert!(!snap.suboverflowed);
    assert_eq!(snap.subxcnt as usize, 3 * NSUB);
    for sub in &expected_subs {
        assert!(snap.subxip.contains(sub), "missing subxid {sub}");
    }
    // The array grew past its initial size, but nowhere near the C worst case.
    assert!(snap.subxip.capacity() >= 3 * NSUB);
    assert!(snap.xip.capacity() <= GetMaxSnapshotXidCount());
    assert!(snap.subxip.capacity() < GetMaxSnapshotSubxidCount());

    // Demand high-water reflects the spike.
    let (_, subxip_target) = SnapshotArrayDecayTargets();
    assert!(subxip_target >= 3 * NSUB, "spike demand not recorded");

    // Spike ends; enough post-spike snapshots + window rotations and the
    // decay target falls back to the floor.
    for (other, top) in others {
        other_proc_end(other, top);
        let proc = GetPGProcByNumber(other);
        unsafe { proc.subxidStatus.set(types_storage::storage::XidCacheStatus {
            count: 0,
            overflowed: false,
        }) };
    }
    let mut floor = (usize::MAX, usize::MAX);
    for _ in 0..(2 * SNAPSHOT_DEMAND_WINDOW_XACTS) {
        take_snapshot(&mut snap, mcx);
        floor = SnapshotArrayDecayTargets();
    }
    assert_eq!(floor.0, SNAPSHOT_ARRAY_INITIAL, "xip target did not decay");
    assert_eq!(floor.1, SNAPSHOT_ARRAY_INITIAL, "subxip target did not decay");

    GetPGProcByNumber(me).xmin.value.store(0, Relaxed);
    set_transaction_xmin(InvalidTransactionId);
}

// procarray.c:1204 — ProcArrayApplyRecoveryInfo sorts the running xids with
// xidLogicalComparator (modular TransactionIdPrecedes order), so a snapshot
// straddling the 2^32 wraparound inserts 4294967294, 4294967295 before 3;
// plain unsigned order puts 3 first and KnownAssignedXidsAdd then rejects
// 4294967294 with "out-of-order XID insertion in KnownAssignedXids".
#[test]
fn apply_recovery_info_orders_wrapped_xids_modularly() {
    let _g = test_lock();
    setup();
    kax::kax_reset();
    xlogutils::set_standby_state(xlogutils::STANDBY_INITIALIZED);

    // The xid counter has wrapped: nextXid is epoch 1 / xid 5.
    let tv = TransamVariables();
    tv.nextXid.store(FullTransactionId::from_epoch_and_xid(1, 5).value, Relaxed);
    tv.latestCompletedXid.store(FullTransactionId::from_epoch_and_xid(1, 4).value, Relaxed);

    ProcArrayInitRecovery(5);
    assert_eq!(kax::latest_observed_xid(), 4);

    let mcx = leaked_mcx();
    let mut xids = mcx::vec_with_capacity_in(mcx, 3).unwrap();
    // Record order is arbitrary (the primary walks the proc array).
    xids.push(3);
    xids.push(4294967295);
    xids.push(4294967294);
    let running = RunningTransactionsData {
        xcnt: 3,
        subxcnt: 0,
        subxid_status: types_storage::storage::SUBXIDS_IN_ARRAY,
        nextXid: 5,
        oldestRunningXid: 4294967294,
        oldestDatabaseRunningXid: 4294967294,
        latestCompletedXid: 4,
        xids,
    };
    ProcArrayApplyRecoveryInfo(&running).expect("wrapped running-xacts snapshot applies");

    assert_eq!(xlogutils::standby_state(), xlogutils::STANDBY_SNAPSHOT_READY);
    assert_eq!(kax::get_all(InvalidTransactionId), vec![4294967294, 4294967295, 3]);
    kax::kax_reset();
    xlogutils::set_standby_state(xlogutils::STANDBY_DISABLED);
}

// procarray.c:3043/3084 + xlog.c CreateCheckPoint — the checkpointer
// snapshots the vxids delaying the checkpoint once
// (GetVirtualXIDsDelayingChkpt) and then waits only for THOSE
// (HaveVirtualXIDsDelayingChkpt(vxids, ...)): a transaction that starts
// delaying after the snapshot never holds up this checkpoint.
#[test]
fn delaying_chkpt_wait_tracks_the_snapshot_not_newcomers() {
    const DELAY_CHKPT_START: i32 = 1 << 0;
    let _g = test_lock();
    let _me = my_backend();
    let a = claim_other();
    let b = claim_other();
    other_proc_running(a, 1200);
    other_proc_running(b, 1201);
    let pa = GetPGProcByNumber(a);
    let pb = GetPGProcByNumber(b);
    pa.vxid.procNumber.store(a, Relaxed);
    pa.vxid.lxid.store(11, Relaxed);
    pb.vxid.procNumber.store(b, Relaxed);
    pb.vxid.lxid.store(12, Relaxed);

    // A is inside its commit critical section when the checkpoint starts.
    pa.delayChkptFlags.store(DELAY_CHKPT_START, Relaxed);
    let snapshot = GetVirtualXIDsDelayingChkpt(DELAY_CHKPT_START);
    assert_eq!(snapshot.len(), 1);
    assert_eq!((snapshot[0].procNumber, snapshot[0].localTransactionId), (a, 11));
    assert!(HaveVirtualXIDsDelayingChkpt(&snapshot, DELAY_CHKPT_START));

    // A leaves; B enters afterwards. C's wait loop, keyed on the snapshot
    // taken above, is done: B is not one of the snapshotted vxids.
    pa.delayChkptFlags.store(0, Relaxed);
    pb.delayChkptFlags.store(DELAY_CHKPT_START, Relaxed);
    assert!(
        !HaveVirtualXIDsDelayingChkpt(&snapshot, DELAY_CHKPT_START),
        "a transaction that started delaying after the snapshot must not delay this checkpoint"
    );
    // A later snapshot sees B (the next checkpoint waits for it).
    assert!(HaveVirtualXIDsDelayingChkpt(
        &GetVirtualXIDsDelayingChkpt(DELAY_CHKPT_START),
        DELAY_CHKPT_START
    ));

    pb.delayChkptFlags.store(0, Relaxed);
    pa.vxid.lxid.store(InvalidLocalTransactionId, Relaxed);
    pb.vxid.lxid.store(InvalidLocalTransactionId, Relaxed);
    other_proc_end(a, 1200);
    other_proc_end(b, 1201);
}

// procarray.c:827/836 — a ProcArrayGroupClearXid follower brackets its
// semaphore sleep with pgstat_report_wait_start(WAIT_EVENT_PROCARRAY_GROUP_UPDATE)
// / pgstat_report_wait_end (pg_stat_activity wait_event ProcarrayGroupUpdate).
#[test]
fn group_clear_xid_follower_reports_procarray_group_update_wait() {
    // wait_event_names.txt IPC row "ProcarrayGroupUpdate" (waitevent row 41).
    const PG_WAIT_IPC: u32 = 0x0800_0000;
    const WAIT_EVENT_PROCARRAY_GROUP_UPDATE: u32 = PG_WAIT_IPC | 41;
    let _g = test_lock();
    let me = my_backend();
    let leader = claim_other();
    other_proc_running(leader, 1300);
    let hdr = ProcGlobal();
    let lp = GetPGProcByNumber(leader);
    lp.procArrayGroupMember.store(true, Relaxed);
    lp.procArrayGroupNext
        .value
        .store(INVALID_PROC_NUMBER as u32, Relaxed);
    hdr.procArrayGroupFirst.value.store(leader as u32, Relaxed);

    let mp = GetPGProcByNumber(me);
    mp.xid.value.store(1301, Relaxed);
    WAIT_EVENTS.lock().unwrap().clear();
    // Joining behind `leader` makes us a follower: we sleep until the (fake)
    // leader clears our membership.
    ProcArrayGroupClearXid(me, 1301).expect("follower returns once cleared");
    let trace = std::mem::take(&mut *WAIT_EVENTS.lock().unwrap());
    assert_eq!(
        trace,
        vec![WAIT_EVENT_PROCARRAY_GROUP_UPDATE, 0],
        "follower must report ProcarrayGroupUpdate around its semaphore wait"
    );
    assert!(!mp.procArrayGroupMember.load(Relaxed));

    // Restore: the fake leader never ran the real clear.
    mp.xid.value.store(InvalidTransactionId, Relaxed);
    hdr.procArrayGroupFirst
        .value
        .store(INVALID_PROC_NUMBER as u32, Relaxed);
    lp.procArrayGroupMember.store(false, Relaxed);
    other_proc_end(leader, 1300);
}

// ---------------------------------------------------------------------------
// audit-18.6 batch b118.
// ---------------------------------------------------------------------------

static B118_LOG: Mutex<Vec<(i32, String)>> = Mutex::new(Vec::new());

fn b118_capture(err: &types_error::PgError, _output_to_server: &mut bool) {
    B118_LOG.lock().unwrap().push((err.level.0, err.message.clone()));
}

// procarray.c:3957: ProcArraySetReplicationSlotXmin logs DEBUG1
// "xmin required by slots: data %u, catalog %u" after installing the xmins.
#[test]
fn set_replication_slot_xmin_logs_debug1_like_c() {
    let _g = test_lock();
    setup();
    let prev = elog::set_emit_log_hook(Some(b118_capture));
    elog::config::set_log_min_messages(types_error::DEBUG1);
    B118_LOG.lock().unwrap().clear();
    ProcArraySetReplicationSlotXmin(700, 690, false).unwrap();
    elog::config::set_log_min_messages(types_error::WARNING);
    elog::set_emit_log_hook(prev);
    assert_eq!(ProcArrayGetReplicationSlotXmin().unwrap(), (700, 690));
    // Leave the horizons as the other tests expect them.
    ProcArraySetReplicationSlotXmin(InvalidTransactionId, InvalidTransactionId, false).unwrap();
    let log = std::mem::take(&mut *B118_LOG.lock().unwrap());
    assert!(
        log.contains(&(
            types_error::DEBUG1.0,
            "xmin required by slots: data 700, catalog 690".to_string()
        )),
        "captured log: {log:?}"
    );
}

// audit-18.6 w2-014: ProcArrayShmemInit is ShmemInitStruct("Proc Array",
// offsetof(ProcArrayStruct, pgprocnos) + sizeof(int) * PROCARRAY_MAXPROCS)
// plus, under hot_standby (the boot default), "KnownAssignedXids" /
// "KnownAssignedXidsValid" over TOTAL_MAX_CACHED_SUBXIDS (procarray.c:424-460),
// so pg_shmem_allocations lists the three blocks with C's sizes. C attaches to
// an existing block by name and size (shmem.c:428-456: found = true; a size
// mismatch is an ERROR), which is the probe used here.
#[test]
fn shmem_init_registers_proc_array_and_known_assigned_xids() {
    setup();
    let array = procArray();
    let max_procs = array.maxProcs as usize;
    let max_kax = array.maxKnownAssignedXids as usize;
    assert_eq!(max_kax, (PGPROC_MAX_CACHED_SUBXIDS + 1) * max_procs);
    // offsetof(ProcArrayStruct, pgprocnos): nine 4-byte fields (procarray.c:71-100).
    let (_, found) = shmem::ShmemInitStruct("Proc Array", 36 + 4 * max_procs).unwrap();
    assert!(found, "\"Proc Array\" is not registered in the ShmemIndex");
    let (_, found) = shmem::ShmemInitStruct("KnownAssignedXids", 4 * max_kax).unwrap();
    assert!(found, "\"KnownAssignedXids\" is not registered in the ShmemIndex");
    let (_, found) = shmem::ShmemInitStruct("KnownAssignedXidsValid", max_kax).unwrap();
    assert!(found, "\"KnownAssignedXidsValid\" is not registered in the ShmemIndex");
}

#[test]
fn replication_horizons_xmin_includes_slot_xmin() {
    let _g = test_lock();
    let me = my_backend();

    let tv = TransamVariables();
    tv.latestCompletedXid.store(
        FullTransactionId::from_epoch_and_xid(0, 4000).value,
        Relaxed,
    );
    let other = claim_other();
    other_proc_running(other, 3900);

    ProcArraySetReplicationSlotXmin(3700, 3600, false).unwrap();
    let (xmin, catalog_xmin) = GetReplicationHorizons().unwrap();
    ProcArraySetReplicationSlotXmin(InvalidTransactionId, InvalidTransactionId, false).unwrap();

    other_proc_end(other, 3900);
    GetPGProcByNumber(me).xmin.value.store(0, Relaxed);
    set_transaction_xmin(InvalidTransactionId);

    assert_eq!(catalog_xmin, 3600);
    assert_eq!(xmin, 3700);
}
