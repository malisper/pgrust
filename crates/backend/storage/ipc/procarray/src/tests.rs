use super::*;
use init_small::globals as g;
use std::sync::{Mutex, Once};
use types_core::BackendType;

// One backend slot per test thread that calls my_backend(); keep headroom
// over the my_backend() call-site count or InitProcess FATALs mid-suite.
const MAX_CONNECTIONS: i32 = 16;
const MAX_WORKER_PROCESSES: i32 = 2;
const NUM_SPECIAL: i32 = types_storage::storage::NUM_SPECIAL_WORKER_PROCS;
const MAX_BACKENDS: i32 = MAX_CONNECTIONS + 3 + MAX_WORKER_PROCESSES + 2 + NUM_SPECIAL;

fn test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
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
        pg_sema_seams::pg_semaphore_lock::set(|_| {});
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
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
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
        transam_xlog_seams::recovery_in_progress::set(|| false);
        transam_seams::transaction_id_did_abort::set(|_| Ok(false));
        subtrans_seams::sub_trans_get_topmost_transaction::set(Ok);

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

    let base = arrayP.numProcs.get();
    let others: Vec<ProcNumber> = (0..3).map(|_| claim_other()).collect();
    for (i, &p) in others.iter().enumerate() {
        other_proc_running(p, 200 + i as TransactionId);
    }
    assert_eq!(arrayP.numProcs.get(), base + 3);

    let n = arrayP.numProcs.get() as usize;
    for i in 0..n {
        let p = arrayP.pgprocnos[i].get();
        assert_eq!(hdr.allProcs[p as usize].pgxactoff.load(Relaxed), i as i32);
        if i > 0 {
            assert!(arrayP.pgprocnos[i - 1].get() < p);
        }
    }

    // Remove the middle one; offsets must re-densify.
    other_proc_end(others[1], 201);
    let n = arrayP.numProcs.get() as usize;
    assert_eq!(n as i32, base + 2);
    for i in 0..n {
        let p = arrayP.pgprocnos[i].get();
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
        let mut cache = proc.subxids.get();
        cache.xids[0] = sub;
        proc.subxids.set(cache);
        proc.subxidStatus.set(types_storage::storage::XidCacheStatus {
            count: 1,
            overflowed: false,
        });
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
    proc.subxidStatus.set(types_storage::storage::XidCacheStatus {
        count: 2,
        overflowed: true,
    });

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
    proc.subxidStatus.set(Default::default());
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
