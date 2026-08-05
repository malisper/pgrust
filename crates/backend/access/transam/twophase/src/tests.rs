use std::sync::{Mutex, Once};

use types_core::BackendType;

use crate::codec::*;
use crate::state::TwoPhaseState;

fn test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn setup() {
    static SETUP: Once = Once::new();
    SETUP.call_once(|| {
        let dir = std::env::temp_dir().join(format!("twophase-test-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("pg_twophase")).unwrap();
        std::env::set_current_dir(&dir).unwrap();

        init_small::globals::SetMaxConnections(8);
        init_small::globals::set_max_worker_processes(2);
        init_small::globals::SetMaxBackends(17);
        init_small::globals::SetMyProcPid(4242);
        init_small::globals::SetMyDatabaseId(5);

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
        xact_seams::get_current_sub_transaction_id::set(|| 1);
        static WAL_SYNC_METHOD: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(0);
        guc_tables::vars::wal_sync_method.install(guc_tables::GucVarAccessors {
            get: || WAL_SYNC_METHOD.load(std::sync::atomic::Ordering::Relaxed),
            set: |v| WAL_SYNC_METHOD.store(v, std::sync::atomic::Ordering::Relaxed),
        });
        xlog_seams::recovery_in_progress::set(|| false);
        transam_seams::transaction_id_did_abort::set(|_| Ok(false));
        subtrans_seams::sub_trans_get_topmost_transaction::set(Ok);
        superuser_seams::superuser_arg::set(|_| Ok(false));

        twophase_config::init_seams();
        guc_tables::vars::max_prepared_xacts.write(2);

        lwlock::CreateLWLocks(false).unwrap();
        lmgr_proc::init_seams();
        lmgr_proc::InitProcGlobal(&lmgr_proc::ProcGlobalConfig {
            autovacuum_worker_slots: 3,
            max_wal_senders: 2,
            max_prepared_xacts: 2,
            fastpath_lock_groups_per_backend: 1,
        });
        procarray::init_seams();
        varsup::VarsupShmemInit();
        procarray::ProcArrayShmemInit();
        crate::TwoPhaseShmemInit();

        varsup::AdvanceNextFullTransactionIdPastXid(2000).expect("advance nextXid");

        lmgr_proc::InitProcess(BackendType::Backend).expect("InitProcess");
        procarray::ProcArrayAdd(lmgr_proc::MyProc().unwrap()).expect("ProcArrayAdd self");
    });
    miscinit::SetUserIdAndSecContext(721, 0);
}

#[test]
fn header_and_record_codecs_roundtrip() {
    let hdr = TwoPhaseFileHeader {
        magic: TWOPHASE_MAGIC,
        total_len: 456,
        xid: 723,
        database: 5,
        prepared_at: 1234567890123,
        owner: 10,
        nsubxacts: 3,
        ncommitrels: 1,
        nabortrels: 2,
        ncommitstats: 1,
        nabortstats: 0,
        ninvalmsgs: 4,
        initfileinval: true,
        gidlen: 6,
        origin_lsn: 0xABCD_EF01_2345,
        origin_timestamp: 42,
    };
    let bytes = hdr.to_bytes();
    assert_eq!(TwoPhaseFileHeader::from_bytes(&bytes), Some(hdr));

    let rec = TwoPhaseRecordOnDisk { len: 20, rmid: 1, info: 0 };
    assert_eq!(TwoPhaseRecordOnDisk::from_bytes(&rec.to_bytes()), Some(rec));

    let layout = BufferLayout::of(&hdr);
    assert_eq!(layout.gid, 72);
    assert_eq!(layout.children, 80); // gidlen 6 maxaligned to 8
    assert_eq!(layout.commitrels, 96); // 3 subxacts = 12 -> 16
    assert_eq!(layout.abortrels, 112); // 1 rel = 12 -> 16
    assert_eq!(layout.commitstats, 136); // 2 rels = 24
    assert_eq!(layout.abortstats, 152); // 1 stat = 16
    assert_eq!(layout.invalmsgs, 152); // 0 stats
    assert_eq!(layout.records, 216); // 4 msgs = 64
}

#[test]
fn gxact_state_machine() {
    let _l = test_lock();
    setup();
    if lmgr_proc::MyProc().is_none() {
        init_small::globals::SetMyProcPid(4242);
        lmgr_proc::InitProcess(types_core::BackendType::Backend).expect("InitProcess");
        procarray::ProcArrayAdd(lmgr_proc::MyProc().unwrap()).expect("ProcArrayAdd");
    }

    let long_gid = "x".repeat(crate::GIDSIZE);
    let err = crate::MarkAsPreparing(700, &long_gid, 1, 10, 5).unwrap_err();
    assert!(err
        .message()
        .contains(&format!("transaction identifier \"{long_gid}\" is too long")));

    let slot = crate::MarkAsPreparing(701, "gid_a", 111, 10, 5).expect("reserve gid_a");

    let err = crate::MarkAsPreparing(702, "gid_a", 112, 10, 5).unwrap_err();
    assert_eq!(
        err.message(),
        "transaction identifier \"gid_a\" is already in use"
    );

    // Not yet valid: not visible to LockGXact.
    let err = crate::FinishPreparedTransaction("gid_a", true).unwrap_err();
    assert_eq!(
        err.message(),
        "prepared transaction with identifier \"gid_a\" does not exist"
    );

    let _slot_b = crate::MarkAsPreparing(703, "gid_b", 113, 10, 5).expect("reserve gid_b");
    // Table full (max_prepared_transactions = 2).
    let err = crate::MarkAsPreparing(704, "gid_c", 114, 10, 5).unwrap_err();
    assert_eq!(err.message(), "maximum number of prepared transactions reached");
    assert_eq!(
        err.hint(),
        Some("Increase \"max_prepared_transactions\" (currently 2).")
    );

    // Abort releases only OUR locked entry (gid_b, the most recent).
    crate::AtAbort_Twophase();
    crate::MarkAsPreparing(705, "gid_c", 115, 10, 5).expect("slot recycled");
    crate::AtAbort_Twophase();

    // gid_a's entry is still reserved by the first MarkAsPreparing; drop it
    // via the state directly (C's model never interleaves two MarkAsPreparing
    // in one backend, so MY_LOCKED_GXACT pointing at the newer one is fine).
    crate::state::lock_twophase_state(lwlock::LW_EXCLUSIVE);
    crate::core::remove_gxact(slot);
    crate::state::unlock_twophase_state();
    assert_eq!(TwoPhaseState().num_prep_xacts.get(), 0);

    let slot = crate::MarkAsPreparing(801, "gid_vis", 222, 10, 5).expect("reserve");
    crate::core::mark_as_prepared(slot, false).expect("MarkAsPrepared");

    let rows = crate::finish::prepared_xact_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].transaction, 801);
    assert_eq!(rows[0].gid, "gid_vis");
    assert_eq!(rows[0].prepared, 222);
    assert_eq!(rows[0].ownerid, 10);
    assert_eq!(rows[0].dbid, 5);

    let dummy = crate::TwoPhaseGetDummyProcNumber(801, false).expect("dummy proc");
    assert!(dummy >= lmgr_proc::PreparedXactProcsBase());

    // Busy: the entry is still locked by this backend.
    let err = crate::FinishPreparedTransaction("gid_vis", true).unwrap_err();
    assert_eq!(
        err.message(),
        "prepared transaction with identifier \"gid_vis\" is busy"
    );

    crate::PostPrepare_Twophase();

    // Wrong owner (721 != 10) and not superuser.
    let err = crate::FinishPreparedTransaction("gid_vis", true).unwrap_err();
    assert_eq!(err.message(), "permission denied to finish prepared transaction");

    // Cleanup: drop the entry and its procarray membership.
    procarray::ProcArrayRemove(TwoPhaseState().gxact(slot).pgprocno.get(), 801)
        .expect("ProcArrayRemove");
    crate::state::lock_twophase_state(lwlock::LW_EXCLUSIVE);
    TwoPhaseState()
        .gxact(slot)
        .locking_backend
        .set(types_core::INVALID_PROC_NUMBER);
    crate::core::remove_gxact(slot);
    crate::state::unlock_twophase_state();
}

#[test]
fn state_file_roundtrip_and_corruption() {
    let _l = test_lock();
    setup();

    let hdr = TwoPhaseFileHeader {
        magic: TWOPHASE_MAGIC,
        total_len: 0,
        xid: 900,
        database: 5,
        prepared_at: 7,
        owner: 10,
        nsubxacts: 0,
        ncommitrels: 0,
        nabortrels: 0,
        ncommitstats: 0,
        nabortstats: 0,
        ninvalmsgs: 0,
        initfileinval: false,
        gidlen: 2,
        origin_lsn: 0,
        origin_timestamp: 0,
    };
    let mut content = Vec::new();
    content.extend_from_slice(&hdr.to_bytes());
    content.extend_from_slice(b"g\0\0\0\0\0\0\0"); // gid, maxaligned
    content.extend_from_slice(&TwoPhaseRecordOnDisk { len: 0, rmid: 0, info: 0 }.to_bytes());
    let total = (content.len() + 4) as u32;
    content[4..8].copy_from_slice(&total.to_ne_bytes());

    crate::files::recreate_two_phase_file(900, &content).expect("recreate");
    let read = crate::files::read_twophase_file(900, false)
        .expect("read ok")
        .expect("present");
    assert_eq!(&read[..content.len()], &content[..]);
    assert_eq!(read.len(), content.len() + 4);

    assert!(crate::files::twophase_file_exists(900).unwrap());
    assert_eq!(crate::files::scan_twophase_dir().unwrap(), vec![900]);

    // Flip a payload byte: CRC mismatch must be detected.
    let path = crate::files::two_phase_file_path(900);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[16] ^= 1;
    std::fs::write(&path, &bytes).unwrap();
    let err = crate::files::read_twophase_file(900, false).unwrap_err();
    assert!(err.message().contains("calculated CRC checksum does not match"));

    crate::files::remove_two_phase_file(900, true).expect("remove");
    assert!(!crate::files::twophase_file_exists(900).unwrap());
    assert!(crate::files::read_twophase_file(900, true)
        .expect("missing ok")
        .is_none());
}

#[test]
fn gid_helpers() {
    assert_eq!(crate::TwoPhaseTransactionGid(3, 77).unwrap(), "pg_gid_3_77");
    assert!(crate::IsTwoPhaseTransactionGidForSubid(3, "pg_gid_3_77"));
    assert!(!crate::IsTwoPhaseTransactionGidForSubid(4, "pg_gid_3_77"));
    assert!(!crate::IsTwoPhaseTransactionGidForSubid(3, "pg_gid_3_77x"));
    assert!(!crate::IsTwoPhaseTransactionGidForSubid(3, "somegid"));
}

// AtProcExit_Twophase (twophase.c): the before_shmem_exit hook registered by
// MarkAsPreparing/LockGXact must release MyLockedGxact when a backend dies
// abnormally between MarkAsPreparing and EndPrepare — without it the
// never-valid entry wedges its slot and the GID forever (plain-ERROR paths
// are covered by AtAbort_Twophase; this witnesses the exit-drain arm).
#[test]
fn exit_hook_releases_gxact_locked_mid_prepare() {
    let _l = test_lock();
    setup();
    if lmgr_proc::MyProc().is_none() {
        init_small::globals::SetMyProcPid(4242);
        lmgr_proc::InitProcess(types_core::BackendType::Backend).expect("InitProcess");
        procarray::ProcArrayAdd(lmgr_proc::MyProc().unwrap()).expect("ProcArrayAdd");
    }

    // Die between MarkAsPreparing and EndPrepare: entry reserved + locked,
    // never marked valid. (MarkAsPreparing registered the exit hook through
    // the real ipc crate; the seam-stubbed ProcKill-class registrations of
    // this substrate stay out of the drain.)
    let n0 = TwoPhaseState().num_prep_xacts.get();
    let _slot = crate::MarkAsPreparing(901, "gid_exit_hook", 111, 10, 5).expect("reserve");
    assert_eq!(TwoPhaseState().num_prep_xacts.get(), n0 + 1);

    // Abnormal thread death: proc_exit's drain runs the before_shmem_exit
    // stack (at_proc_exit_twophase -> AtAbort_Twophase).
    ipc::shmem_exit(1).unwrap();

    // The never-valid gxact was removed outright and the GID is reusable.
    assert_eq!(TwoPhaseState().num_prep_xacts.get(), n0);
    let _slot2 = crate::MarkAsPreparing(902, "gid_exit_hook", 112, 10, 5)
        .expect("GID reusable after the exit-hook release");
    crate::AtAbort_Twophase();
    assert_eq!(TwoPhaseState().num_prep_xacts.get(), n0);
}
