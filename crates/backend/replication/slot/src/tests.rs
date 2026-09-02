use crate::ondisk::*;
use crate::ReplicationSlotValidateNameInternal;
use types_error::{ERRCODE_INVALID_NAME, ERRCODE_NAME_TOO_LONG};

#[test]
fn validate_name_ok() {
    assert!(ReplicationSlotValidateNameInternal("my_slot_01").is_ok());
    assert!(ReplicationSlotValidateNameInternal(&"a".repeat(63)).is_ok());
}

#[test]
fn validate_name_too_short() {
    let (code, msg, hint) = ReplicationSlotValidateNameInternal("").unwrap_err();
    assert_eq!(code, ERRCODE_INVALID_NAME);
    assert_eq!(msg, "replication slot name \"\" is too short");
    assert!(hint.is_none());
}

#[test]
fn validate_name_too_long() {
    let name = "a".repeat(64);
    let (code, _, hint) = ReplicationSlotValidateNameInternal(&name).unwrap_err();
    assert_eq!(code, ERRCODE_NAME_TOO_LONG);
    assert!(hint.is_none());
}

#[test]
fn validate_name_bad_chars() {
    for bad in ["Slot", "slot-1", "slot 1", "slot.1", "sløt"] {
        let (code, msg, hint) = ReplicationSlotValidateNameInternal(bad).unwrap_err();
        assert_eq!(code, ERRCODE_INVALID_NAME);
        assert_eq!(
            msg,
            format!("replication slot name \"{bad}\" contains invalid character")
        );
        assert_eq!(
            hint.as_deref(),
            Some(
                "Replication slot names may only contain lower case letters, numbers, and the \
                 underscore character."
            )
        );
    }
}

fn sample_data() -> ReplicationSlotPersistentData {
    let mut d = ReplicationSlotPersistentData::default();
    d.name.namestrcpy("kat_slot");
    d.restart_lsn = 0x0102030405060708;
    d.confirmed_flush = 0x1122334455667788;
    d
}

// Known-answer image independently computed with a table-based CRC32C over
// the C struct layout (magic 0x1051CA1, version 5, length 184).
const KAT_HEX: &str = "a11c0501ba08c4f805000000b8000000\
6b61745f736c6f740000000000000000\
00000000000000000000000000000000\
00000000000000000000000000000000\
00000000000000000000000000000000\
00000000000000000000000000000000\
08070605040302010000000000000000\
88776655443322110000000000000000\
00000000000000000000000000000000\
00000000000000000000000000000000\
00000000000000000000000000000000\
00000000000000000000000000000000\
0000000000000000";

fn kat_bytes() -> Vec<u8> {
    (0..KAT_HEX.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&KAT_HEX[i..i + 2], 16).unwrap())
        .collect()
}

#[test]
fn on_disk_known_answer() {
    let image = serialize_state_file(&sample_data());
    assert_eq!(image.len(), 200);
    assert_eq!(header_magic(&image), SLOT_MAGIC);
    assert_eq!(header_version(&image), SLOT_VERSION);
    assert_eq!(header_length(&image), 184);
    assert_eq!(header_checksum(&image), 0xF8C408BA);
    assert_eq!(&image[..], &kat_bytes()[..]);
}

#[test]
fn on_disk_round_trip() {
    let mut d = sample_data();
    d.database = 16384;
    d.persistency = RS_EPHEMERAL;
    d.xmin = 731;
    d.catalog_xmin = 730;
    d.invalidated = RS_INVAL_IDLE_TIMEOUT;
    d.two_phase_at = 0xDEADBEEF;
    d.two_phase = true;
    d.plugin.namestrcpy("test_decoding");
    d.synced = 1;
    d.failover = true;

    let image = serialize_state_file(&d);
    assert_eq!(state_file_checksum(&image), header_checksum(&image));

    let back = deserialize_persistent_data(&image[ON_DISK_CONSTANT_SIZE..]);
    assert_eq!(back.name.data, d.name.data);
    assert_eq!(back.database, d.database);
    assert_eq!(back.persistency, d.persistency);
    assert_eq!(back.xmin, d.xmin);
    assert_eq!(back.catalog_xmin, d.catalog_xmin);
    assert_eq!(back.restart_lsn, d.restart_lsn);
    assert_eq!(back.invalidated, d.invalidated);
    assert_eq!(back.confirmed_flush, d.confirmed_flush);
    assert_eq!(back.two_phase_at, d.two_phase_at);
    assert_eq!(back.two_phase, d.two_phase);
    assert_eq!(back.plugin.data, d.plugin.data);
    assert_eq!(back.synced, d.synced);
    assert_eq!(back.failover, d.failover);
}

#[test]
fn checksum_covers_exact_range() {
    let image = serialize_state_file(&sample_data());
    // Bytes 0..8 (magic + checksum) are outside the checksummed range.
    let mut altered = image;
    altered[0] ^= 0xFF;
    assert_eq!(state_file_checksum(&altered), header_checksum(&image));
    let mut altered = image;
    altered[ON_DISK_SIZE - 1] ^= 0x01;
    assert_ne!(state_file_checksum(&altered), header_checksum(&image));
}

// am_walsender acquired/released log lines (slot.c:702 and the
// ReplicationSlotRelease tail): exact C messages; DEBUG1 unless
// log_replication_commands (default off).
#[test]
fn walsender_slot_log_lines() {
    use crate::{walsender_slot_log_level, walsender_slot_log_message};
    use types_error::{DEBUG1, LOG};

    assert_eq!(
        walsender_slot_log_message(true, true, "s1"),
        "acquired logical replication slot \"s1\""
    );
    assert_eq!(
        walsender_slot_log_message(true, false, "s1"),
        "acquired physical replication slot \"s1\""
    );
    assert_eq!(
        walsender_slot_log_message(false, true, "s1"),
        "released logical replication slot \"s1\""
    );
    assert_eq!(
        walsender_slot_log_message(false, false, "s1"),
        "released physical replication slot \"s1\""
    );

    // In the server the GUC storage is installed by walsender's init_seams;
    // stand in for it here.
    use std::sync::atomic::{AtomicBool, Ordering};
    static LOG_REPLICATION_COMMANDS: AtomicBool = AtomicBool::new(false);
    guc_tables::vars::log_replication_commands.install(guc_tables::GucVarAccessors {
        get: || LOG_REPLICATION_COMMANDS.load(Ordering::Relaxed),
        set: |v| LOG_REPLICATION_COMMANDS.store(v, Ordering::Relaxed),
    });

    // log_replication_commands defaults to off -> DEBUG1; on -> LOG.
    assert_eq!(walsender_slot_log_level(), DEBUG1);
    LOG_REPLICATION_COMMANDS.store(true, Ordering::Relaxed);
    assert_eq!(walsender_slot_log_level(), LOG);
}

#[test]
fn invalidation_cause_names() {
    use crate::{GetSlotInvalidationCause, GetSlotInvalidationCauseName};
    assert_eq!(GetSlotInvalidationCauseName(RS_INVAL_NONE), "none");
    assert_eq!(GetSlotInvalidationCauseName(RS_INVAL_WAL_REMOVED), "wal_removed");
    assert_eq!(GetSlotInvalidationCauseName(RS_INVAL_HORIZON), "rows_removed");
    assert_eq!(
        GetSlotInvalidationCauseName(RS_INVAL_WAL_LEVEL),
        "wal_level_insufficient"
    );
    assert_eq!(GetSlotInvalidationCauseName(RS_INVAL_IDLE_TIMEOUT), "idle_timeout");
    assert_eq!(GetSlotInvalidationCause("rows_removed"), RS_INVAL_HORIZON);
}

// One-shot lwlock / proc / procarray / slot-array bring-up (the twophase
// crate's test setup, minus its own state).
fn shmem_setup() {
    static SETUP: std::sync::Once = std::sync::Once::new();
    SETUP.call_once(|| {
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
        xlog_seams::recovery_in_progress::set(|| false);
        transam_seams::transaction_id_did_abort::set(|_| Ok(false));
        subtrans_seams::sub_trans_get_topmost_transaction::set(Ok);
        superuser_seams::superuser_arg::set(|_| Ok(false));

        walsender_config::init_seams();
        guc_tables::vars::max_replication_slots.write(2);

        lwlock::CreateLWLocks(false).unwrap();
        lmgr_proc::init_seams();
        lmgr_proc::InitProcGlobal(&lmgr_proc::ProcGlobalConfig {
            autovacuum_worker_slots: 3,
            max_wal_senders: 2,
            max_prepared_xacts: 2,
            fastpath_lock_groups_per_backend: 1,
        });
        procarray::init_seams();
        procarray::ProcArrayShmemInit();
        // The slot-drop path stores the required-LSN floor into XLogCtl.
        static XLOG_BUFFERS: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(64);
        guc_tables::vars::XLOGbuffers.install_if_absent(guc_tables::GucVarAccessors {
            get: || XLOG_BUFFERS.load(std::sync::atomic::Ordering::Relaxed),
            set: |v| XLOG_BUFFERS.store(v, std::sync::atomic::Ordering::Relaxed),
        });
        transam_xlog::XLOGShmemInit();
        crate::ReplicationSlotsShmemInit();

        lmgr_proc::InitProcess(types_core::BackendType::Backend).expect("InitProcess");
        procarray::ProcArrayAdd(lmgr_proc::MyProc().unwrap()).expect("ProcArrayAdd self");
    });
}

// upstream f833c92077a1 (18.5): Fix race in ReplicationSlotRelease() for
// ephemeral slots — once dropped, the entry is another backend's to reuse,
// so the release must not write effective_xmin / inactive_since into it.
#[test]
fn release_of_ephemeral_slot_leaves_the_dropped_entry_untouched() {
    use crate::{MyReplicationSlot, ReplicationSlotCtl, ReplicationSlotRelease, SetMyReplicationSlot};

    shmem_setup();
    let s = &ReplicationSlotCtl()[0];

    // An ephemeral slot mid-creation: acquired, data.xmin invalid while
    // effective_xmin holds the temporary horizon, never inactive.
    let mut d = ReplicationSlotPersistentData::default();
    d.name.namestrcpy("eph");
    d.persistency = RS_EPHEMERAL;
    // SAFETY: single-threaded test; nobody else references this entry.
    unsafe {
        s.data.set(d);
        s.in_use.set(true);
        s.active_pid.set(4242);
        s.effective_xmin.set(1234);
        s.inactive_since.set(0);
    }
    SetMyReplicationSlot(Some(s));

    ReplicationSlotRelease().unwrap();

    assert!(MyReplicationSlot().is_none());
    // SAFETY: as above.
    unsafe {
        assert!(!s.in_use.get());
        assert_eq!(s.active_pid.get(), 0);
        // Not written to after the drop (both landed on the dead entry
        // before the fix).
        assert_eq!(s.effective_xmin.get(), 1234);
        assert_eq!(s.inactive_since.get(), 0);
    }
}
