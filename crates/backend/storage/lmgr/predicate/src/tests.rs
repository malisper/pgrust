use std::sync::atomic::{AtomicI32, AtomicU32, Ordering::SeqCst};
use std::sync::mpsc;
use std::sync::{Mutex, MutexGuard, Once};

use init_small::globals as g;
use types_core::BackendType;

use crate::internals::*;

const TESTDB: u32 = 7777;
const MAX_CONNECTIONS: i32 = 32;
const MAX_BACKENDS: i32 = MAX_CONNECTIONS + 3 + 2 + 2 + 2;
static NEXT_PID: AtomicI32 = AtomicI32::new(9300);
static NEXT_XID: AtomicU32 = AtomicU32::new(1000);
// Shmem (PredXact, serial control) is process-global; the multi-sxact tests
// must not interleave and their xmins must be monotonic (SerialSetActiveSerXmin
// asserts tailXid never regresses).
static CONCURRENCY_GATE: Mutex<()> = Mutex::new(());

// Collector for the 2PC statefile records AtPrepare_PredicateLocks registers
// via the register_two_phase_record seam (installed in setup()).
static REGISTERED: Mutex<Vec<(u8, u16, Vec<u8>)>> = Mutex::new(Vec::new());

fn exclusive() -> (MutexGuard<'static, ()>, u32) {
    let guard = CONCURRENCY_GATE.lock().unwrap_or_else(|e| e.into_inner());
    (guard, NEXT_XID.fetch_add(10, SeqCst))
}

const CFG: lmgr_proc::ProcGlobalConfig = lmgr_proc::ProcGlobalConfig {
    autovacuum_worker_slots: 3,
    max_wal_senders: 2,
    max_prepared_xacts: 2,
    fastpath_lock_groups_per_backend: 1,
};

fn thread_globals() {
    g::SetMaxConnections(MAX_CONNECTIONS);
    g::set_max_worker_processes(2);
    g::SetMaxBackends(MAX_BACKENDS);
    g::SetMyProcPid(NEXT_PID.fetch_add(1, SeqCst));
    g::SetMyDatabaseId(TESTDB);
}

fn setup() {
    static SETUP: Once = Once::new();
    SETUP.call_once(|| {
        thread_globals();

        pg_sema_seams::pg_semaphore_create::set(|_| {});
        pg_sema_seams::pg_semaphore_reset::set(|_| {});
        pg_sema_seams::pg_semaphore_lock::set(|_| {});
        pg_sema_seams::pg_semaphore_unlock::set(|_| {});
        postgres_seams::check_for_interrupts::set(|| Ok(()));
        s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
        s_lock_seams::finish_spin_delay::set(|_| {});
        s_lock_seams::set_spins_per_delay::set(|_| {});
        s_lock_seams::update_spins_per_delay::set(|v| v);
        latch_seams::own_latch::set(|latch| latch.owner_pid.store(g::MyProcPid(), SeqCst));
        latch_seams::disown_latch::set(|latch| latch.owner_pid.store(0, SeqCst));
        latch_seams::set_latch::set(|latch| latch.is_set.store(1, SeqCst));
        latch_seams::set_latch_my_latch::set(|| {});
        latch_seams::wait_latch_my_latch::set(|_, _, _| {
            std::thread::yield_now();
            types_storage::waiteventset::WL_LATCH_SET
        });
        latch_seams::reset_latch_my_latch::set(|| {});
        miscinit_seams::switch_to_shared_latch::set(|| {});
        miscinit_seams::switch_back_to_local_latch::set(|| {});
        waitevent_seams::pgstat_set_wait_event_storage::set(|_| {});
        waitevent_seams::pgstat_reset_wait_event_storage::set(|| {});
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
        ipc_seams::on_shmem_exit::set(|_, _| {});
        pmsignal_seams::register_postmaster_child_active::set(|| {});
        procarray_seams::proc_array_add::set(|_| Ok(()));
        procarray_seams::proc_array_remove::set(|_, _| Ok(()));

        transam_xlog_seams::recovery_in_progress::set(|| false);
        varsup_seams::read_next_transaction_id::set(|| Ok(NEXT_XID.load(SeqCst)));

        xact_seams::is_in_parallel_mode::set(|| false);
        parallel_seams::is_parallel_worker::set(|| false);
        xact_seams::is_sub_transaction::set(|| false);
        xact_seams::isolation_is_serializable::set(|| true);
        xact_seams::xact_read_only::set(|| false);
        xact_seams::xact_deferrable::set(|| false);
        xact_seams::get_top_transaction_id_if_any::set(|| 0);
        xact_seams::transaction_id_is_current_transaction_id::set(|_| false);
        // hash_seq_init (GetPredicateLockStatusData) registers its scan against
        // the current xact nest level; single top-level in these tests.
        xact_seams::get_current_transaction_nest_level::set(|| 1);

        deadlock_seams::init_dead_lock_checking::set(|| Ok(()));
        deadlock_seams::dead_lock_check::set(|_| types_storage::lock::DeadLockState::NoDeadLock);
        timeout_seams::enable_timeout_after::set(|_, _| Ok(()));
        timeout_seams::enable_timeouts::set(|_| Ok(()));
        timeout_seams::disable_timeout::set(|_, _| Ok(()));
        timeout_seams::disable_timeouts::set(|_| {});
        timeout_seams::get_timeout_start_time::set(|_| 0);
        timestamp_seams::get_current_timestamp::set(|| 0);
        ps_status_seams::set_ps_display_suffix::set(|_| {});
        ps_status_seams::set_ps_display_remove_suffix::set(|| {});
        elog_seams::ereport_msg::set(|_, _, _| Ok(()));

        shmem_seams::add_size::set(|a, b| Ok(a.checked_add(b).expect("size overflow")));
        shmem_seams::mul_size::set(|a, b| Ok(a.checked_mul(b).expect("size overflow")));
        shmem_seams::shmem_alloc::set(|size| {
            Ok(Box::leak(vec![0u8; size].into_boxed_slice()).as_mut_ptr())
        });

        twophase_seams::register_two_phase_record::set(|rmid, info, data| {
            REGISTERED.lock().unwrap().push((rmid, info, data.to_vec()));
            Ok(())
        });

        lwlock::CreateLWLocks(false).unwrap();
        lmgr_proc::init_seams();
        lmgr_proc::InitProcGlobal(&CFG);
        crate::engine::PredicateLockShmemInit(CFG.max_prepared_xacts).unwrap();
        crate::init_seams();
    });
}

fn become_backend() {
    setup();
    thread_globals();
    if lmgr_proc::MyProc().is_none() {
        lmgr_proc::InitProcess(BackendType::Backend).unwrap();
    }
}

fn mvcc_snapshot() -> types_snapshot::SnapshotData<'static> {
    let cx: &'static mcx::MemoryContext =
        Box::leak(Box::new(mcx::MemoryContext::new("predicate test")));
    types_snapshot::SnapshotData::sentinel(cx.mcx(), types_snapshot::SNAPSHOT_MVCC)
}

#[test]
fn tag_set_get_type_and_coverage() {
    let mut t = ZERO_TARGET_TAG;
    SET_PREDICATELOCKTARGETTAG_RELATION(&mut t, 5, 7);
    assert_eq!(GET_PREDICATELOCKTARGETTAG_DB(&t), 5);
    assert_eq!(GET_PREDICATELOCKTARGETTAG_RELATION(&t), 7);
    assert_eq!(GET_PREDICATELOCKTARGETTAG_TYPE(&t), PREDLOCKTAG_RELATION);

    SET_PREDICATELOCKTARGETTAG_PAGE(&mut t, 5, 7, 42);
    assert_eq!(GET_PREDICATELOCKTARGETTAG_PAGE(&t), 42);
    assert_eq!(GET_PREDICATELOCKTARGETTAG_TYPE(&t), PREDLOCKTAG_PAGE);

    SET_PREDICATELOCKTARGETTAG_TUPLE(&mut t, 5, 7, 42, 3);
    assert_eq!(GET_PREDICATELOCKTARGETTAG_OFFSET(&t), 3);
    assert_eq!(GET_PREDICATELOCKTARGETTAG_TYPE(&t), PREDLOCKTAG_TUPLE);

    let mut rel = ZERO_TARGET_TAG;
    SET_PREDICATELOCKTARGETTAG_RELATION(&mut rel, 1, 2);
    let mut page = rel;
    SET_PREDICATELOCKTARGETTAG_PAGE(&mut page, 1, 2, 9);
    let mut tup = rel;
    SET_PREDICATELOCKTARGETTAG_TUPLE(&mut tup, 1, 2, 9, 4);
    assert!(TargetTagIsCoveredBy(&page, &rel));
    assert!(TargetTagIsCoveredBy(&tup, &page));
    let mut page2 = rel;
    SET_PREDICATELOCKTARGETTAG_PAGE(&mut page2, 1, 2, 10);
    assert!(!TargetTagIsCoveredBy(&tup, &page2));
    assert!(!TargetTagIsCoveredBy(&rel, &page));
}

#[test]
fn invalid_sxact_fast_paths_and_installs() {
    become_backend();
    predicate_seams::pre_commit_check_for_serialization_failure::call().unwrap();
    predicate_seams::register_predicate_locking_xid::call(100).unwrap();
    predicate_seams::at_prepare_predicate_locks::call().unwrap();
    predicate_seams::post_prepare_predicate_locks::call(100).unwrap();
    predicate_seams::release_predicate_locks::call(true, false).unwrap();
    predicate_seams::check_point_predicate::call().unwrap();
    assert!(predicate_seams::predicate_lock_relation::is_installed());
    assert!(predicate_seams::predicate_lock_page::is_installed());
    assert!(predicate_seams::predicate_lock_tid::is_installed());
    assert!(predicate_seams::check_for_serializable_conflict_out_needed::is_installed());
    assert!(predicate_seams::check_for_serializable_conflict_out::is_installed());
    assert!(predicate_seams::check_for_serializable_conflict_in::is_installed());
    assert!(predicate_seams::check_table_for_serializable_conflict_in::is_installed());
    assert!(predicate_seams::transfer_predicate_locks_to_heap_relation::is_installed());
    assert!(predicate_seams::predicate_lock_page_split::is_installed());
    assert!(predicate_seams::get_serializable_transaction_snapshot::is_installed());
}

// The relations must be above FirstUnpinnedObjectId for predicate locking.
const REL_A: u32 = 30001;
const REL_B: u32 = 30002;

// Classic write-skew: T1 reads A / writes B, T2 reads B / writes A, both
// commit — exactly one aborts with 40001 (first committer dooms the other in
// PreCommit's dangerous-structure walk).
#[test]
fn write_skew_pair_one_aborts_with_40001() {
    become_backend();
    let (_gate, xmin) = exclusive();

    let (to_t2, from_t1) = mpsc::channel::<()>();
    let (to_t1, from_t2) = mpsc::channel::<()>();

    let t2 = std::thread::spawn(move || {
        become_backend();
        let snap = mvcc_snapshot();
        crate::engine::test_acquire_sxact(xmin).unwrap();
        // T2 reads B.
        crate::engine::PredicateLockPage(TESTDB, REL_B, false, 1, &snap).unwrap();
        to_t1.send(()).unwrap();
        from_t1.recv().unwrap();
        // T2 writes A (crossed).
        crate::engine::CheckForSerializableConflictIn(TESTDB, REL_A, false, Some((1, 1)), 1)
            .unwrap();
        to_t1.send(()).unwrap();
        from_t1.recv().unwrap();
        // T1 committed first and doomed T2: commit attempt fails.
        let err = crate::engine::PreCommit_CheckForSerializationFailure().unwrap_err();
        let msg = format!("{err:?}");
        assert!(
            msg.contains("could not serialize access due to read/write dependencies"),
            "unexpected error: {msg}"
        );
        assert!(
            msg.contains(
                "Canceled on identification as a pivot, during commit attempt."
            ),
            "unexpected detail: {msg}"
        );
        crate::engine::ReleasePredicateLocks(false, false).unwrap();
        to_t1.send(()).unwrap();
    });

    let snap = mvcc_snapshot();
    crate::engine::test_acquire_sxact(xmin).unwrap();
    // T1 reads A.
    crate::engine::PredicateLockPage(TESTDB, REL_A, false, 1, &snap).unwrap();
    from_t2.recv().unwrap();
    // T2 has read B; T1 writes B.
    crate::engine::CheckForSerializableConflictIn(TESTDB, REL_B, false, Some((1, 1)), 1).unwrap();
    to_t2.send(()).unwrap();
    from_t2.recv().unwrap();
    // T1 commits first: PreCommit dooms T2, T1 succeeds.
    crate::engine::PreCommit_CheckForSerializationFailure().unwrap();
    crate::engine::ReleasePredicateLocks(true, false).unwrap();
    to_t2.send(()).unwrap();
    from_t2.recv().unwrap();
    t2.join().unwrap();
}

// Tuple-lock promotion: more than max_predicate_locks_per_page tuple locks on
// one page promote to a page lock; a subsequent write to another tuple on the
// page still sees the conflict.
#[test]
fn tuple_locks_promote_to_page() {
    become_backend();
    let (_gate, xmin) = exclusive();

    let (to_t2, from_t1) = mpsc::channel::<()>();
    let (to_t1, from_t2) = mpsc::channel::<()>();

    let t2 = std::thread::spawn(move || {
        become_backend();
        let snap = mvcc_snapshot();
        crate::engine::test_acquire_sxact(xmin).unwrap();
        // Locks 3 tuples on one page (> max_predicate_locks_per_page = 2):
        // promotes to a page-granularity lock.
        for off in 1..=3u16 {
            crate::engine::PredicateLockTID(TESTDB, 30011, false, false, 7, off, &snap, 5)
                .unwrap();
        }
        to_t1.send(()).unwrap();
        from_t1.recv().unwrap();
        crate::engine::ReleasePredicateLocks(false, false).unwrap();
        to_t1.send(()).unwrap();
    });

    from_t2.recv().unwrap();
    crate::engine::test_acquire_sxact(xmin).unwrap();
    // Write a DIFFERENT tuple (offset 9) on the same page: only the promoted
    // page lock can flag this conflict.
    crate::engine::CheckForSerializableConflictIn(TESTDB, 30011, false, Some((7, 9)), 7).unwrap();
    let doomed = unsafe {
        let mysx = crate::engine::test_my_sxact();
        !crate::ilist::dlist_is_empty(&raw const (*mysx).inConflicts)
    };
    assert!(doomed, "promoted page lock did not record the rw-conflict");
    crate::engine::ReleasePredicateLocks(false, false).unwrap();
    to_t2.send(()).unwrap();
    from_t2.recv().unwrap();
    t2.join().unwrap();
}

// A read-only txn overlapping a writer never aborts when the writer commits.
#[test]
fn read_only_txn_never_aborts_in_simple_overlap() {
    become_backend();
    let (_gate, xmin) = exclusive();

    let (to_t2, from_t1) = mpsc::channel::<()>();
    let (to_t1, from_t2) = mpsc::channel::<()>();

    let t2 = std::thread::spawn(move || {
        become_backend();
        let snap = mvcc_snapshot();
        crate::engine::test_acquire_sxact(xmin).unwrap();
        // Reader: reads A only, never writes.
        crate::engine::PredicateLockPage(TESTDB, 30021, false, 1, &snap).unwrap();
        to_t1.send(()).unwrap();
        from_t1.recv().unwrap();
        // Writer committed; reader still commits cleanly.
        crate::engine::PreCommit_CheckForSerializationFailure().unwrap();
        crate::engine::ReleasePredicateLocks(true, false).unwrap();
        to_t1.send(()).unwrap();
    });

    crate::engine::test_acquire_sxact(xmin).unwrap();
    from_t2.recv().unwrap();
    // Writer writes A (conflict in from the reader) and commits.
    crate::engine::CheckForSerializableConflictIn(TESTDB, 30021, false, Some((1, 1)), 1).unwrap();
    crate::engine::PreCommit_CheckForSerializationFailure().unwrap();
    crate::engine::ReleasePredicateLocks(true, false).unwrap();
    to_t2.send(()).unwrap();
    from_t2.recv().unwrap();
    t2.join().unwrap();
}

// Decode a 24-byte TwoPhasePredicateRecord's type tag + payload words.
fn rec_word(rec: &[u8], off: usize) -> u32 {
    u32::from_ne_bytes(rec[off..off + 4].try_into().unwrap())
}

// AtPrepare registers one XACT record then one LOCK record per predicate lock;
// PostPrepare hands the sxact off unowned (clears the thread-local + local
// hash); PredicateLockTwoPhaseFinish then finds it by xid and releases it.
#[test]
fn at_prepare_registers_records_post_prepare_hands_off_finish_releases() {
    become_backend();
    let (_gate, xmin) = exclusive();
    let topxid = xmin + 1;

    let snap = mvcc_snapshot();
    crate::engine::test_acquire_sxact(xmin).unwrap();
    // A real xact registers its xid before commit; needed so Finish can find it.
    crate::engine::RegisterPredicateLockingXid(topxid).unwrap();
    // One SIREAD page lock on REL_A page 7.
    crate::engine::PredicateLockPage(TESTDB, REL_A, false, 7, &snap).unwrap();
    // PreCommit sets prepareSeqNo + SXACT_FLAG_PREPARED (PostPrepare asserts it).
    crate::engine::PreCommit_CheckForSerializationFailure().unwrap();

    REGISTERED.lock().unwrap().clear();
    crate::engine::AtPrepare_PredicateLocks().unwrap();

    let regs = REGISTERED.lock().unwrap().clone();
    assert_eq!(regs.len(), 2, "one xact record + one lock record");
    // Every predicate 2PC record is 24 bytes under rmid TWOPHASE_RM_PREDICATELOCK_ID (4).
    for (rmid, info, data) in &regs {
        assert_eq!(*rmid, 4);
        assert_eq!(*info, 0);
        assert_eq!(data.len(), 24);
    }
    // Record 0: XACT (type 0), payload xmin + flags.
    assert_eq!(rec_word(&regs[0].2, 0), 0, "record 0 is XACT");
    assert_eq!(rec_word(&regs[0].2, 4), xmin, "xact record xmin");
    assert_ne!(rec_word(&regs[0].2, 8) & SXACT_FLAG_PREPARED, 0, "flags carry PREPARED");
    // Record 1: LOCK (type 1), target = (db, REL_A, page 7, offset 0).
    assert_eq!(rec_word(&regs[1].2, 0), 1, "record 1 is LOCK");
    assert_eq!(rec_word(&regs[1].2, 4), TESTDB, "lock target db");
    assert_eq!(rec_word(&regs[1].2, 8), REL_A, "lock target rel");
    assert_eq!(rec_word(&regs[1].2, 12), 7, "lock target page");
    assert_eq!(rec_word(&regs[1].2, 16), 0, "lock target offset (page lock)");

    // PostPrepare hands off: the owning thread no longer references the sxact.
    crate::engine::PostPrepare_PredicateLocks(topxid).unwrap();
    assert_eq!(crate::engine::test_my_sxact(), InvalidSerializableXact);

    // Finish (commit) locates the prepared sxact by xid and releases it.
    crate::engine::PredicateLockTwoPhaseFinish(topxid, true).unwrap();
    assert_eq!(crate::engine::test_my_sxact(), InvalidSerializableXact);
}

// predicatelock_twophase_recover rebuilds a SERIALIZABLEXACT (per-xact record)
// and re-acquires its predicate lock (per-lock record); the rebuilt lock is
// visible in the status view (owner pid 0 = unowned/recovered), and
// PredicateLockTwoPhaseFinish (rollback) releases it cleanly.
#[test]
fn twophase_recover_rebuilds_sxact_and_lock_then_finish_releases() {
    become_backend();
    let (_gate, xmin) = exclusive();
    let recxid = xmin; // the recovered prepared xact's own xid

    // Per-xact record: type 0, xmin, flags = PREPARED.
    let mut xrec = [0u8; 24];
    xrec[4..8].copy_from_slice(&xmin.to_ne_bytes());
    xrec[8..12].copy_from_slice(&SXACT_FLAG_PREPARED.to_ne_bytes());
    crate::engine::predicatelock_twophase_recover(recxid, 0, &xrec).unwrap();

    // Per-lock record: type 1, target = (TESTDB, REL_A, page 5, offset 0).
    let mut lrec = [0u8; 24];
    lrec[0..4].copy_from_slice(&1u32.to_ne_bytes());
    lrec[4..8].copy_from_slice(&TESTDB.to_ne_bytes());
    lrec[8..12].copy_from_slice(&REL_A.to_ne_bytes());
    lrec[12..16].copy_from_slice(&5u32.to_ne_bytes());
    crate::engine::predicatelock_twophase_recover(recxid, 0, &lrec).unwrap();

    // The rebuilt lock is present, owned by the unowned recovered sxact (pid 0).
    let status = crate::engine::GetPredicateLockStatusData().unwrap();
    let found = status.iter().any(|e| {
        e.tag.locktag_field1 == TESTDB
            && e.tag.locktag_field2 == REL_A
            && e.tag.locktag_field3 == 5
            && e.pid == 0
    });
    assert!(found, "recovered predicate lock on REL_A page 5 (pid 0) not found");

    // Rollback-prepared releases the recovered sxact and its lock.
    crate::engine::PredicateLockTwoPhaseFinish(recxid, false).unwrap();
    let status = crate::engine::GetPredicateLockStatusData().unwrap();
    let still = status.iter().any(|e| {
        e.tag.locktag_field1 == TESTDB && e.tag.locktag_field2 == REL_A && e.tag.locktag_field3 == 5
    });
    assert!(!still, "recovered lock should be gone after ROLLBACK PREPARED finish");
}
