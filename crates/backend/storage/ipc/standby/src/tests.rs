use crate::*;

// standbydefs.h: xcnt 0, subxcnt 4, overflow 8, nextXid 12, oldest 16, latestCompleted 20.
#[test]
fn running_xacts_header_matches_c_layout() {
    let xids = [900u32, 905];
    let running = procarray::RunningTransactions {
        xids: &xids,
        xcnt: 1,
        subxcnt: 1,
        subxid_overflow: false,
        next_xid: 910,
        oldest_running_xid: 900,
        latest_completed_xid: 899,
        oldest_database_running_xid: 900,
    };
    let hdr = running_xacts_header(&running);
    assert_eq!(hdr.len(), 24);
    assert_eq!(i32::from_ne_bytes(hdr[0..4].try_into().unwrap()), 1);
    assert_eq!(i32::from_ne_bytes(hdr[4..8].try_into().unwrap()), 1);
    assert_eq!(hdr[8], 0);
    assert_eq!(&hdr[9..12], &[0, 0, 0]);
    assert_eq!(u32::from_ne_bytes(hdr[12..16].try_into().unwrap()), 910);
    assert_eq!(u32::from_ne_bytes(hdr[16..20].try_into().unwrap()), 900);
    assert_eq!(u32::from_ne_bytes(hdr[20..24].try_into().unwrap()), 899);

    let overflowed = procarray::RunningTransactions {
        subxid_overflow: true,
        subxcnt: 0,
        ..running
    };
    assert_eq!(running_xacts_header(&overflowed)[8], 1);
}

// standbydefs.h xl_standby_lock: xid 0, dbOid 4, relOid 8; 12 bytes each.
#[test]
fn standby_lock_body_matches_c_layout() {
    let locks = [
        xl_standby_lock { xid: 700, dbOid: 5, relOid: 16384 },
        xl_standby_lock { xid: 701, dbOid: 5, relOid: 16385 },
    ];
    let body = standby_locks_body(&locks);
    assert_eq!(body.len(), 2 * SIZE_OF_XL_STANDBY_LOCK);
    assert_eq!(u32::from_ne_bytes(body[0..4].try_into().unwrap()), 700);
    assert_eq!(u32::from_ne_bytes(body[4..8].try_into().unwrap()), 5);
    assert_eq!(u32::from_ne_bytes(body[8..12].try_into().unwrap()), 16384);
    assert_eq!(u32::from_ne_bytes(body[12..16].try_into().unwrap()), 701);
    assert_eq!(u32::from_ne_bytes(body[20..24].try_into().unwrap()), 16385);
}

#[test]
fn recovery_lock_table_bookkeeping_dedupes_and_chains() {
    use crate::recovery::test_support as ts;
    ts::init_lock_tables_only();

    assert!(ts::insert_entry(700, 5, 16384));
    // Checkpoints re-report held locks; the dedupe hash absorbs them.
    assert!(!ts::insert_entry(700, 5, 16384));
    assert!(ts::insert_entry(700, 5, 16385));
    assert!(ts::insert_entry(701, 5, 16384));

    assert_eq!(ts::recovery_lock_table_counts(), (3, 2));
    assert_eq!(ts::chain(700), vec![(5, 16384), (5, 16385)]);
    assert_eq!(ts::chain(701), vec![(5, 16384)]);
    assert_eq!(ts::chain(702), vec![]);
}

// A hostile primary/archive can set arbitrary counts in a RM_STANDBY_ID
// record (the WAL CRC is a checksum, not a MAC). validate_count must reject
// every out-of-range count as a catchable ERRCODE_DATA_CORRUPTED error rather
// than letting it reach slice-index/allocation panics on the redo thread.
#[test]
fn validate_count_rejects_hostile_counts() {
    use types_error::ERRCODE_DATA_CORRUPTED;

    // Well-formed: 2 * 12-byte locks backed by a 4-byte header.
    let ok = validate_count(4 + 2 * SIZE_OF_XL_STANDBY_LOCK, 4, 2, SIZE_OF_XL_STANDBY_LOCK, "t");
    assert_eq!(ok.unwrap(), 2);

    // Negative count (sign-extends to a huge usize in the buggy path).
    let e = validate_count(4, 4, -1, SIZE_OF_XL_STANDBY_LOCK, "t").err().unwrap();
    assert_eq!(e.sqlstate(), ERRCODE_DATA_CORRUPTED);

    // Positive but unbacked by the record (would OOB-index).
    let e = validate_count(4 + SIZE_OF_XL_STANDBY_LOCK, 4, 2, SIZE_OF_XL_STANDBY_LOCK, "t")
        .err()
        .unwrap();
    assert_eq!(e.sqlstate(), ERRCODE_DATA_CORRUPTED);

    // count * elem_size overflows usize (would wrap/allocate absurdly).
    let e = validate_count(64, 16, i32::MAX, SHARED_INVALIDATION_MESSAGE_SIZE, "t")
        .err()
        .unwrap();
    assert_eq!(e.sqlstate(), ERRCODE_DATA_CORRUPTED);

    // Zero-count records are valid (header only).
    assert_eq!(validate_count(16, 16, 0, SHARED_INVALIDATION_MESSAGE_SIZE, "t").unwrap(), 0);
}

#[test]
fn require_len_rejects_truncated_header() {
    use types_error::ERRCODE_DATA_CORRUPTED;
    assert!(require_len(&[0u8; 24], MIN_SIZE_OF_XACT_RUNNING_XACTS, "t").is_ok());
    let e = require_len(&[0u8; 8], MIN_SIZE_OF_XACT_RUNNING_XACTS, "t").err().unwrap();
    assert_eq!(e.sqlstate(), ERRCODE_DATA_CORRUPTED);
}

static FROM_STREAM: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

#[test]
fn standby_limit_time_matches_c_arithmetic() {
    use crate::recovery::test_support as ts;
    use std::sync::atomic::Ordering::Relaxed;
    xlogrecovery_seams::get_xlog_receipt_time::set(|| (1_000_000, FROM_STREAM.load(Relaxed)));

    ts::set_delay_gucs(7_000, 30_000);
    assert_eq!(ts::standby_limit_time(), 1_000_000 + 30_000i64 * 1000);

    // fromStream=false picks the archive delay.
    FROM_STREAM.store(false, Relaxed);
    assert_eq!(ts::standby_limit_time(), 1_000_000 + 7_000i64 * 1000);

    // -1 = wait forever = 0 sentinel.
    ts::set_delay_gucs(-1, -1);
    assert_eq!(ts::standby_limit_time(), 0);
}

// ---------------------------------------------------------------------------
// standby_redo (standby.c:1163) — audit-18.6 batch b118.
// ---------------------------------------------------------------------------

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

static REPORT_STAT_CALLS: AtomicUsize = AtomicUsize::new(0);
static REDO_LOG: Mutex<Vec<(i32, String)>> = Mutex::new(Vec::new());

fn redo_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn redo_capture(err: &types_error::PgError, _output_to_server: &mut bool) {
    REDO_LOG.lock().unwrap().push((err.level.0, err.message.clone()));
}

// The startup-process slice of shared state standby_redo's RUNNING_XACTS arm
// walks (ProcArrayApplyRecoveryInfo -> KnownAssignedXids / TransamVariables
// under ProcArrayLock and XidGenLock), mirroring procarray's own harness.
fn redo_harness() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        use init_small::globals as g;
        g::SetMaxConnections(8);
        g::set_max_worker_processes(2);
        g::SetMaxBackends(8 + 3 + 2 + 2 + types_storage::storage::NUM_SPECIAL_WORKER_PROCS);
        g::SetMyProcPid(4242);
        shmem::init_seams();
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
        pg_sema_seams::pg_semaphore_create::set(|_| {});
        pg_sema_seams::pg_semaphore_reset::set(|_| {});
        pgstat_seams::pgstat_report_stat::set(|force| {
            assert!(force, "standby.c:1206 reports with force = true");
            REPORT_STAT_CALLS.fetch_add(1, Ordering::Relaxed);
            0
        });
        elog::init_seams();
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
        procarray::procarray_seams::standby_release_old_locks::set(|_| Ok(()));
    });
}

fn redo_record(info: u8, main_data: &[u8]) -> xlogreader_seams::XLogReaderState {
    let rec = xlogreader_seams::DecodedXLogRecord {
        xl_info: info,
        max_block_id: -1,
        main_data: if main_data.is_empty() { std::ptr::null() } else { main_data.as_ptr() },
        main_data_len: main_data.len() as u32,
        ..Default::default()
    };
    xlogreader_seams::XLogReaderState { record: Some(rec), ..Default::default() }
}

// standby.c:1206: replaying XLOG_RUNNING_XACTS flushes the startup process's
// pending statistics (pgstat_report_stat(true)) — the running-xacts cadence
// is the only stats-report schedule the startup process has.
#[test]
fn running_xacts_redo_reports_pending_stats() {
    redo_harness();
    let _g = redo_lock();
    xlogutils::set_standby_state(xlogutils::STANDBY_SNAPSHOT_READY);
    let tv = varsup::TransamVariables();
    tv.nextXid.store(
        types_core::FullTransactionId::from_epoch_and_xid(0, 105).value,
        Ordering::Relaxed,
    );
    tv.latestCompletedXid.store(
        types_core::FullTransactionId::from_epoch_and_xid(0, 104).value,
        Ordering::Relaxed,
    );
    let xids = [100u32, 103];
    let running = procarray::RunningTransactions {
        xids: &xids,
        xcnt: 2,
        subxcnt: 0,
        subxid_overflow: false,
        next_xid: 105,
        oldest_running_xid: 100,
        latest_completed_xid: 104,
        oldest_database_running_xid: 100,
    };
    let mut body = running_xacts_header(&running).to_vec();
    for x in xids {
        body.extend_from_slice(&x.to_ne_bytes());
    }
    let before = REPORT_STAT_CALLS.load(Ordering::Relaxed);
    let mut reader = redo_record(XLOG_RUNNING_XACTS, &body);
    standby_redo(&mut reader).expect("running-xacts replay");
    xlogutils::set_standby_state(xlogutils::STANDBY_DISABLED);
    assert_eq!(
        REPORT_STAT_CALLS.load(Ordering::Relaxed),
        before + 1,
        "standby_redo must call pgstat_report_stat(true) after applying XLOG_RUNNING_XACTS"
    );
}

// standby.c:1219: an unknown op code is elog(PANIC, "standby_redo: unknown op
// code %u") — the logged PANIC that unwinds the thread (PanicExitThread),
// never a bare Rust panic that bypasses the server log.
#[test]
fn unknown_op_code_is_a_logged_panic() {
    redo_harness();
    let _g = redo_lock();
    xlogutils::set_standby_state(xlogutils::STANDBY_INITIALIZED);
    let prev = elog::set_emit_log_hook(Some(redo_capture));
    REDO_LOG.lock().unwrap().clear();
    let mut reader = redo_record(0x30, &[]);
    let outcome =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| standby_redo(&mut reader)));
    elog::set_emit_log_hook(prev);
    xlogutils::set_standby_state(xlogutils::STANDBY_DISABLED);
    let payload = outcome.expect_err("unknown op code must PANIC");
    assert!(
        payload.downcast_ref::<types_error::PanicExitThread>().is_some(),
        "C's elog(PANIC) unwinds the thread as PanicExitThread; got a Rust panic: {:?}",
        payload
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| payload.downcast_ref::<String>().cloned())
    );
    let log = std::mem::take(&mut *REDO_LOG.lock().unwrap());
    assert!(
        log.contains(&(types_error::PANIC.0, "standby_redo: unknown op code 48".to_string())),
        "captured log: {log:?}"
    );
}

// standby.c:1378/1386: the DEBUG2 snapshot lines print the LSN with
// LSN_FORMAT_ARGS under "%X/%X" -- no zero padding of the low word at 18.6
// (C: "lsn 0/17A4040"; the padded "lsn 0/017A4040" is not C).
#[test]
fn running_xacts_snapshot_message_formats_lsn_like_c() {
    let xids = [900u32];
    let running = procarray::RunningTransactions {
        xids: &xids,
        xcnt: 0,
        subxcnt: 0,
        subxid_overflow: false,
        next_xid: 754,
        oldest_running_xid: 754,
        latest_completed_xid: 753,
        oldest_database_running_xid: 754,
    };
    assert_eq!(
        running_xacts_snapshot_message(&running, 0x017A_4040),
        "snapshot of 0+0 running transaction ids (lsn 0/17A4040 oldest xid 754 latest complete 753 next xid 754)"
    );
    let overflowed = procarray::RunningTransactions {
        subxid_overflow: true,
        xcnt: 3,
        ..running
    };
    assert_eq!(
        running_xacts_snapshot_message(&overflowed, 0x0000_0001_0000_000Au64 as XLogRecPtr),
        "snapshot of 3 running transactions overflowed (lsn 1/A oldest xid 754 latest complete 753 next xid 754)"
    );
}
