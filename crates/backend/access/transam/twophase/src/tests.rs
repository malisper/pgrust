use std::sync::{Mutex, Once};

use types_core::BackendType;

use crate::codec::*;
use crate::state::TwoPhaseState;

// The ShmemIndex rows registered through the shmem_init_struct seam during
// setup() (name, size), in registration order.
static SHMEM_INDEX_NAMES: Mutex<Vec<(String, usize)>> = Mutex::new(Vec::new());

// Every pgstat_report_wait_start(info) the crate issued through the
// waitevent seam, in order (wait_event.h PG_WAIT_IO class ids: the
// positions of the waitevent crate's IO name table).
static WAIT_EVENTS: Mutex<Vec<u32>> = Mutex::new(Vec::new());
const PG_WAIT_IO: u32 = 0x0A00_0000;
const WAIT_EVENT_TWOPHASE_FILE_READ: u32 = PG_WAIT_IO | 62;
const WAIT_EVENT_TWOPHASE_FILE_SYNC: u32 = PG_WAIT_IO | 63;
const WAIT_EVENT_TWOPHASE_FILE_WRITE: u32 = PG_WAIT_IO | 64;

fn record_wait_start(info: u32) {
    WAIT_EVENTS.lock().unwrap().push(info);
}

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
        waitevent_seams::pgstat_report_wait_start::set(record_wait_start);
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
        shmem_seams::shmem_init_struct::set(|name, size| {
            SHMEM_INDEX_NAMES.lock().unwrap().push((name.to_owned(), size));
            Ok((Box::leak(vec![0u8; size.max(1)].into_boxed_slice()).as_mut_ptr(), false))
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
        crate::TwoPhaseShmemInit().unwrap();

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

    // A buffer at least as large as the computed layout validates cleanly.
    let layout = BufferLayout::try_of(&hdr, hdr.total_len as usize).expect("valid layout");
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
fn buffer_layout_rejects_hostile_headers() {
    // Baseline: the segments end at `records`, so a buffer exactly that big is
    // the smallest that validates; one byte short must be rejected.
    let base = TwoPhaseFileHeader {
        magic: TWOPHASE_MAGIC,
        total_len: 0,
        xid: 723,
        database: 5,
        prepared_at: 1,
        owner: 10,
        nsubxacts: 1,
        ncommitrels: 0,
        nabortrels: 0,
        ncommitstats: 0,
        nabortstats: 0,
        ninvalmsgs: 0,
        initfileinval: false,
        gidlen: 6,
        origin_lsn: 0,
        origin_timestamp: 0,
    };
    let records = BufferLayout::try_of(&base, usize::MAX)
        .expect("layout fits in an unbounded buffer")
        .records;
    assert!(BufferLayout::try_of(&base, records).is_some());
    assert!(BufferLayout::try_of(&base, records - 1).is_none());

    // gidlen larger than the buffer (the "72-byte body, gidlen=0xFFFF" case).
    let mut h = base;
    h.gidlen = 0xFFFF;
    assert!(BufferLayout::try_of(&h, 72).is_none());

    // gidlen beyond the fixed shared-memory gid slot is corruption.
    let mut h = base;
    h.gidlen = (crate::GIDSIZE + 1) as u16;
    assert!(BufferLayout::try_of(&h, usize::MAX).is_none());

    // Negative counts (e.g. nsubxacts = -1) must never sign-extend into a huge
    // usize; every signed count field is rejected.
    for set in [
        |h: &mut TwoPhaseFileHeader| h.nsubxacts = -1,
        |h: &mut TwoPhaseFileHeader| h.ncommitrels = -1,
        |h: &mut TwoPhaseFileHeader| h.nabortrels = -1,
        |h: &mut TwoPhaseFileHeader| h.ncommitstats = -1,
        |h: &mut TwoPhaseFileHeader| h.nabortstats = -1,
        |h: &mut TwoPhaseFileHeader| h.ninvalmsgs = i32::MIN,
    ] {
        let mut h = base;
        set(&mut h);
        assert!(BufferLayout::try_of(&h, usize::MAX).is_none());
    }

    // A huge positive count runs far past a realistically-sized buffer and is
    // rejected rather than panicking (debug) or wrapping the offset (release).
    let mut h = base;
    h.nsubxacts = i32::MAX;
    assert!(BufferLayout::try_of(&h, 4096).is_none());
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
    assert_eq!(unsafe { TwoPhaseState().num_prep_xacts.get() }, 0);

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
    procarray::ProcArrayRemove(unsafe { TwoPhaseState().gxact(slot).pgprocno.get() }, 801)
        .expect("ProcArrayRemove");
    crate::state::lock_twophase_state(lwlock::LW_EXCLUSIVE);
    unsafe { TwoPhaseState()
        .gxact(slot)
        .locking_backend
        .set(types_core::INVALID_PROC_NUMBER) };
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

// ReadTwoPhaseFile (twophase.c:1328-1333) rejects a too-small state file
// with errmsg_plural, so a 1-byte file says "1 byte"; the read is bracketed
// by WAIT_EVENT_TWOPHASE_FILE_READ (1347/1361) and RecreateTwoPhaseFile's
// write + fsync by WAIT_EVENT_TWOPHASE_FILE_WRITE / _SYNC (1749-1779).
// Audit a186-verified-fp-transam-twophase-a92c3e3abbf434d880d9-1 (+ the
// candidate twin 06d742fc) and a186-candidate-fp-transam-twophase-583bbea9.
#[test]
fn state_file_size_error_and_wait_events_match_c() {
    let _l = test_lock();
    setup();

    let path = crate::files::two_phase_file_path(901);
    std::fs::write(&path, b"x").unwrap();
    let err = crate::files::read_twophase_file(901, false).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_DATA_CORRUPTED);
    assert_eq!(
        err.message(),
        format!("incorrect size of file \"{path}\": 1 byte"),
        "C errmsg_plural picks the singular for a 1-byte file"
    );
    std::fs::remove_file(&path).unwrap();

    let hdr = TwoPhaseFileHeader {
        magic: TWOPHASE_MAGIC,
        total_len: 0,
        xid: 902,
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
    content.extend_from_slice(b"g\0\0\0\0\0\0\0");
    content.extend_from_slice(&TwoPhaseRecordOnDisk { len: 0, rmid: 0, info: 0 }.to_bytes());
    let total = (content.len() + 4) as u32;
    content[4..8].copy_from_slice(&total.to_ne_bytes());

    WAIT_EVENTS.lock().unwrap().clear();
    crate::files::recreate_two_phase_file(902, &content).expect("recreate");
    let after_write = WAIT_EVENTS.lock().unwrap().clone();
    assert!(
        after_write.contains(&WAIT_EVENT_TWOPHASE_FILE_WRITE)
            && after_write.contains(&WAIT_EVENT_TWOPHASE_FILE_SYNC),
        "RecreateTwoPhaseFile must report TwophaseFileWrite + TwophaseFileSync, got {after_write:?}"
    );
    WAIT_EVENTS.lock().unwrap().clear();
    crate::files::read_twophase_file(902, false).expect("read ok").expect("present");
    let after_read = WAIT_EVENTS.lock().unwrap().clone();
    assert!(
        after_read.contains(&WAIT_EVENT_TWOPHASE_FILE_READ),
        "ReadTwoPhaseFile must report TwophaseFileRead, got {after_read:?}"
    );
    crate::files::remove_two_phase_file(902, true).expect("remove");
}

#[test]
fn gid_helpers() {
    assert_eq!(crate::TwoPhaseTransactionGid(3, 77).unwrap(), "pg_gid_3_77");
    assert!(crate::IsTwoPhaseTransactionGidForSubid(3, "pg_gid_3_77").unwrap());
    assert!(!crate::IsTwoPhaseTransactionGidForSubid(4, "pg_gid_3_77").unwrap());
    assert!(!crate::IsTwoPhaseTransactionGidForSubid(3, "pg_gid_3_77x").unwrap());
    assert!(!crate::IsTwoPhaseTransactionGidForSubid(3, "somegid").unwrap());
    // sscanf-shaped: "%u" consumes the digits, the reconstruction rejects
    // the non-canonical spelling; a zero xid raises 08P01 (twophase.c:2688).
    assert!(!crate::IsTwoPhaseTransactionGidForSubid(3, "pg_gid_+3_77").unwrap());
    assert!(!crate::IsTwoPhaseTransactionGidForSubid(3, "pg_gid_3_077").unwrap());
    let err = crate::IsTwoPhaseTransactionGidForSubid(3, "pg_gid_3_0").unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_PROTOCOL_VIOLATION);
    assert_eq!(err.message(), "invalid two-phase transaction ID");
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
    let n0 = unsafe { TwoPhaseState().num_prep_xacts.get() };
    let _slot = crate::MarkAsPreparing(901, "gid_exit_hook", 111, 10, 5).expect("reserve");
    assert_eq!(unsafe { TwoPhaseState().num_prep_xacts.get() }, n0 + 1);

    // Abnormal thread death: proc_exit's drain runs the before_shmem_exit
    // stack (at_proc_exit_twophase -> AtAbort_Twophase).
    ipc::shmem_exit(1).unwrap();

    // The never-valid gxact was removed outright and the GID is reusable.
    assert_eq!(unsafe { TwoPhaseState().num_prep_xacts.get() }, n0);
    let _slot2 = crate::MarkAsPreparing(902, "gid_exit_hook", 112, 10, 5)
        .expect("GID reusable after the exit-hook release");
    crate::AtAbort_Twophase();
    assert_eq!(unsafe { TwoPhaseState().num_prep_xacts.get() }, n0);
}

// idx 61: process_records must reject an attacker-influenced record stream
// (bad rmid, oversized len, or a truncated header) with a catchable
// ERRCODE_DATA_CORRUPTED error rather than an out-of-bounds panic.
#[test]
fn process_records_rejects_malformed_stream() {
    use crate::core::process_records;
    use types_error::ERRCODE_DATA_CORRUPTED;

    // All-None callback table: validation must fire before any dispatch.
    let callbacks: [Option<twophase_rmgr::TwoPhaseCallback>; twophase_rmgr::NUM_TWOPHASE_RM] =
        [None; twophase_rmgr::NUM_TWOPHASE_RM];

    // (1) rmid past the callback table => corruption error, not an OOB index.
    let rec = TwoPhaseRecordOnDisk { len: 0, rmid: 200, info: 0 };
    let err = process_records(&rec.to_bytes(), 0, 42, &callbacks)
        .expect_err("rmid 200 must be rejected");
    assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);

    // (2) Oversized len => corruption error, not an OOB slice.
    let rec = TwoPhaseRecordOnDisk { len: 0xFFFF_FFFF, rmid: 1, info: 0 };
    let err = process_records(&rec.to_bytes(), 0, 42, &callbacks)
        .expect_err("oversized len must be rejected");
    assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);

    // (3) Truncated header (< 8 bytes) => corruption error, not a panic.
    let err = process_records(&[0u8; 4], 0, 42, &callbacks)
        .expect_err("truncated header must be rejected");
    assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);

    // (4) A well-formed END sentinel still terminates cleanly.
    let rec = TwoPhaseRecordOnDisk { len: 0, rmid: twophase_rmgr::TWOPHASE_RM_END_ID, info: 0 };
    process_records(&rec.to_bytes(), 0, 42, &callbacks).expect("END sentinel terminates");
}

// twophase.c:257 TwoPhaseShmemInit registers the table via
// ShmemInitStruct("Prepared Transaction Table", TwoPhaseShmemSize(), &found),
// so pg_shmem_allocations lists the row. Audit
// a186-candidate-fp-transam-twophase-66bb4fc2a6599036522e-1.
#[test]
fn shmem_init_registers_prepared_transaction_table_in_shmem_index() {
    let _g = test_lock();
    setup();
    let rows = SHMEM_INDEX_NAMES.lock().unwrap();
    let row = rows.iter().find(|(n, _)| n == "Prepared Transaction Table");
    let (_, size) = row.expect("Prepared Transaction Table is missing from the ShmemIndex");
    assert_eq!(*size, crate::TwoPhaseShmemSize());
}

// twophase.c:1639-1670: FinishPreparedTransaction runs ProcessRecords and
// PredicateLockTwoPhaseFinish under TwoPhaseStateLock and only THEN
// RemoveGXact. A callback ERROR therefore unwinds with the gxact still in the
// array (valid == false, still locked by us) and AtAbort_Twophase removes it
// exactly once; the HOLD_INTERRUPTS taken before the callbacks is undone by
// the error path. Pre-fix pgrust removed the gxact before evaluating the
// callback result, so the abort path double-removed it and panicked with
// "failed to find gxact ... in GlobalTransaction array".
// Audit a186-candidate-fp-transam-twophase-f9bd389cf1a5396b9731-1.
#[test]
fn finish_callback_error_leaves_gxact_for_at_abort() {
    use twophase_rmgr::{TwoPhaseCallback, NUM_TWOPHASE_RM, TWOPHASE_RM_END_ID, TWOPHASE_RM_LOCK_ID};

    let _l = test_lock();
    setup();
    if lmgr_proc::MyProc().is_none() {
        init_small::globals::SetMyProcPid(4242);
        lmgr_proc::InitProcess(types_core::BackendType::Backend).expect("InitProcess");
        procarray::ProcArrayAdd(lmgr_proc::MyProc().unwrap()).expect("ProcArrayAdd");
    }

    fn failing_callback(
        _xid: types_core::TransactionId,
        _info: u16,
        _data: &[u8],
    ) -> types_error::PgResult<()> {
        Err(Box::new(types_error::PgError::error("twophase test callback failure")))
    }
    let mut callbacks: [Option<TwoPhaseCallback>; NUM_TWOPHASE_RM] = [None; NUM_TWOPHASE_RM];
    callbacks[TWOPHASE_RM_LOCK_ID as usize] = Some(failing_callback);
    // One zero-length lock record, then the END sentinel.
    let mut buf = TwoPhaseRecordOnDisk { len: 0, rmid: TWOPHASE_RM_LOCK_ID, info: 0 }
        .to_bytes()
        .to_vec();
    buf.extend_from_slice(
        &TwoPhaseRecordOnDisk { len: 0, rmid: TWOPHASE_RM_END_ID, info: 0 }.to_bytes(),
    );

    // The finish-time shape: the gxact is in the array, locked by this
    // backend (MY_LOCKED_GXACT) and marked invalid before the callbacks run.
    let n0 = unsafe { TwoPhaseState().num_prep_xacts.get() };
    let slot = crate::MarkAsPreparing(1801, "gid_cb_err", 333, 10, 5).expect("reserve gid_cb_err");
    assert_eq!(crate::state::MY_LOCKED_GXACT.get(), slot);
    assert_eq!(unsafe { TwoPhaseState().num_prep_xacts.get() }, n0 + 1);

    let holdoff = init_small::globals::InterruptHoldoffCount();
    init_small::globals::HoldInterrupts();
    let err = crate::finish::finish_callbacks_and_remove(&buf, 0, 1801, &callbacks, true, slot)
        .expect_err("a post-commit callback error must propagate");
    assert_eq!(err.message(), "twophase test callback failure");
    assert_eq!(
        unsafe { TwoPhaseState().num_prep_xacts.get() },
        n0 + 1,
        "gxact must stay in the array until AtAbort_Twophase: RemoveGXact (twophase.c:1656) runs only after the callbacks succeed"
    );
    assert_eq!(crate::state::MY_LOCKED_GXACT.get(), slot);
    assert_eq!(
        init_small::globals::InterruptHoldoffCount(),
        holdoff,
        "the HOLD_INTERRUPTS taken for the finish must be undone on the error path"
    );

    // AtAbort_Twophase: !valid -> RemoveGXact, once, without panicking.
    crate::AtAbort_Twophase();
    assert_eq!(unsafe { TwoPhaseState().num_prep_xacts.get() }, n0);
    assert_eq!(crate::state::MY_LOCKED_GXACT.get(), crate::state::NO_GXACT);
}

// twophase.c:236-247 TwoPhaseShmemSize: 16-byte header + GlobalTransaction
// pointer array, MAXALIGN, + 256-byte GlobalTransactionData slots
// (pg_shmem_allocations parity: 2656 for max_prepared_transactions = 10).
#[test]
fn shmem_size_matches_c_layout() {
    assert_eq!(crate::state::two_phase_shmem_size_for(0), 16);
    assert_eq!(crate::state::two_phase_shmem_size_for(10), 2656);
    assert_eq!(crate::state::two_phase_shmem_size_for(20), 5296);
}

// twophase.c:2709 sscanf("%u"): strtoul saturates on overflow (sign
// ignored) before the unsigned-int store, so an xid field of 2^64 scans as
// 4294967295 and the GID is merely non-canonical — not the 08P01 that xid
// 0 raises. 2^32 truncates to 0 in C as well.
#[test]
fn gid_for_subid_saturates_like_sscanf() {
    use crate::core::IsTwoPhaseTransactionGidForSubid;
    assert!(!IsTwoPhaseTransactionGidForSubid(1, "pg_gid_1_18446744073709551616").unwrap());
    assert!(!IsTwoPhaseTransactionGidForSubid(1, "pg_gid_1_-18446744073709551616").unwrap());
    assert!(IsTwoPhaseTransactionGidForSubid(1, "pg_gid_1_4294967295").unwrap());
    assert!(IsTwoPhaseTransactionGidForSubid(1, "pg_gid_1_4294967296").is_err());
}

// twophase.c:1359-1362 / 1747-1750: a CloseTransientFile failure reports
// errcode_for_file_access() "could not close file \"%s\": %m" with the close
// errno spliced in; the pre-fix port left a literal "%m". Detail bug_8b0f8979.
#[test]
fn close_failure_message_carries_the_os_error() {
    let err = crate::files::close_two_phase_file(1 << 20, "pg_twophase/000000AC", "ReadTwoPhaseFile")
        .unwrap_err();
    // elog.c errcode_for_file_access: EBADF is the default (internal error) arm.
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);
    assert_eq!(
        err.message(),
        format!("could not close file \"pg_twophase/000000AC\": {}", errno_str(libc::EBADF))
    );
}

fn errno_str(en: i32) -> String {
    // SAFETY: strerror returns a static NUL-terminated string for a known errno.
    unsafe { std::ffi::CStr::from_ptr(libc::strerror(en)) }.to_string_lossy().into_owned()
}
