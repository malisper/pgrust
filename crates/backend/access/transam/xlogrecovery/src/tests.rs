//! NATIVE ARM ONLY: these units build fixtures through std::fs, which under
//! `--cfg pgrust_sim` would split across the real disk and the SimVfs
//! namespace; the sim-cfg recovery battery is tests/sim_crash_sweep.rs.
#![cfg(not(pgrust_sim))]

use super::*;
use controldata_utils::{CheckPoint, ControlFileData, SIZEOF_CHECKPOINT};
use transam_xlog::control_file::{
    FirstNormalUnloggedLSN, FLOATFORMAT_VALUE, PG_CONTROL_FILE_SIZE, PG_CONTROL_VERSION,
    TOAST_MAX_CHUNK_SIZE,
};
use transam_xlog::{
    SizeOfXLogLongPHD, SizeOfXLogRecord, DB_SHUTDOWNED, MAXALIGN, RM_XLOG_ID, WAL_LEVEL_REPLICA,
    XLOG_CHECKPOINT_SHUTDOWN, XLP_LONG_HEADER,
};

const SEG: i32 = 16 * 1024 * 1024;
const SYS_ID: u64 = 0x00AA_BB00_CCDD_0011;

// DataDir is process-global: every test that points it at a fixture
// directory holds this while the directory is in use.
static DATADIR_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn datadir_lock() -> std::sync::MutexGuard<'static, ()> {
    DATADIR_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

// init_seams() installs the crate's seam slots and GUC hooks exactly once per
// process (seam/hook installs refuse a second install).
fn install_crate_seams() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(init_seams);
}

// wait_event.h PG_WAIT_IO class, WalRead id (waitevent crate IO name table).
const PG_WAIT_IO: u32 = 0x0A00_0000;
const WAIT_EVENT_WAL_READ: u32 = PG_WAIT_IO | 75;

static WAIT_EVENTS: std::sync::Mutex<Vec<u32>> = std::sync::Mutex::new(Vec::new());

fn record_wait_start(info: u32) {
    WAIT_EVENTS.lock().unwrap().push(info);
}

static EMITTED: std::sync::Mutex<Vec<(types_error::SqlState, String)>> =
    std::sync::Mutex::new(Vec::new());

fn capture_emitted(err: &types_error::PgError, _output_to_server: &mut bool) {
    EMITTED.lock().unwrap().push((err.sqlstate(), err.message().to_string()));
}

static ALLOW_IN_PLACE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn fixture_datadir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pgrust_xlogrecovery_{tag}_{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    init_small::globals::SetDataDir(dir.to_str().unwrap());
    dir
}

// CheckTablespaceDirectory (xlogrecovery.c:2177-2181): AllocateDir failure is
// ReadDir's ereport(ERROR, errcode_for_file_access(), "could not open
// directory \"pg_tblspc\": %m") — the relative name and strerror text. The
// pre-fix port `let Ok(entries) = read_dir(..) else { return Ok(()) }`
// swallowed the failure. Audit
// a186-candidate-fp-transam-xlogrecovery-p1-8d8a7bd3d3071065526c-1.
#[test]
fn check_tablespace_directory_reports_open_failure_like_c() {
    let _g = datadir_lock();
    let dir = fixture_datadir("tblspc_open");
    // A self-referential symlink fails opendir with ELOOP for root too.
    std::os::unix::fs::symlink("pg_tblspc", dir.join("pg_tblspc")).unwrap();
    let err = check_tablespace_directory().expect_err("opendir failure must be an ERROR");
    assert_eq!(err.level, ERROR);
    assert_eq!(
        err.message(),
        format!(
            "could not open directory \"{PG_TBLSPC_DIR}\": {}",
            elog::errno::strerror(libc::ELOOP)
        )
    );
    // C's errcode_for_file_access() default arm for ELOOP.
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);

    // As a non-root user EACCES exercises the 42501 arm as well.
    if unsafe { libc::geteuid() } != 0 {
        std::fs::remove_file(dir.join("pg_tblspc")).unwrap();
        std::fs::create_dir(dir.join("pg_tblspc")).unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.join("pg_tblspc"), std::fs::Permissions::from_mode(0o000))
            .unwrap();
        let err = check_tablespace_directory().expect_err("EACCES must be an ERROR");
        std::fs::set_permissions(dir.join("pg_tblspc"), std::fs::Permissions::from_mode(0o700))
            .unwrap();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_INSUFFICIENT_PRIVILEGE);
        assert_eq!(
            err.message(),
            format!(
                "could not open directory \"{PG_TBLSPC_DIR}\": {}",
                elog::errno::strerror(libc::EACCES)
            )
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

// CheckTablespaceDirectory (xlogrecovery.c:2189-2195): a non-symlink entry is
// ereport(allow_in_place_tablespaces ? WARNING : PANIC, errcode(
// ERRCODE_DATA_CORRUPTED), ...) — SQLSTATE XX001 on the WARNING arm too.
// Audit a186-candidate-fp-transam-xlogrecovery-p1-f8e209eb08abf906f4e1-1.
#[test]
fn check_tablespace_directory_warning_carries_data_corrupted_sqlstate() {
    let _g = datadir_lock();
    let dir = fixture_datadir("tblspc_warn");
    std::fs::create_dir_all(dir.join("pg_tblspc/16385")).unwrap();
    guc_tables::vars::allow_in_place_tablespaces.install_if_absent(guc_tables::GucVarAccessors {
        get: || ALLOW_IN_PLACE.load(Relaxed),
        set: |v| ALLOW_IN_PLACE.store(v, Relaxed),
    });
    guc_tables::vars::allow_in_place_tablespaces.write(true);
    EMITTED.lock().unwrap().clear();
    let prev = elog::set_emit_log_hook(Some(capture_emitted));
    let r = check_tablespace_directory();
    elog::set_emit_log_hook(prev);
    r.expect("allow_in_place_tablespaces=on downgrades the PANIC to a WARNING");
    let emitted = EMITTED.lock().unwrap();
    let msg = format!("unexpected directory entry \"16385\" found in {PG_TBLSPC_DIR}");
    let row = emitted
        .iter()
        .find(|(_, m)| *m == msg)
        .unwrap_or_else(|| panic!("WARNING {msg:?} not emitted; got {emitted:?}"));
    assert_eq!(row.0, types_error::ERRCODE_DATA_CORRUPTED);
    drop(emitted);
    let _ = std::fs::remove_dir_all(&dir);
}

// check_recovery_target / check_recovery_target_name /
// check_recovery_target_timeline (xlogrecovery.c:4846, 4921, 5047) reject
// with GUC_check_errdetail(...) + return false, so guc.c builds the C
// headline 'invalid value for parameter "<name>": "<value>"' with the text
// as DETAIL and SQLSTATE 22023. The pre-fix hooks returned Err(XX000) with
// the text folded into one message. Audit rows 46ea467b/6a4ea0ad,
// 698f6053/89c9f972, a27457d6.
static CHECK_DETAILS: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
// The recorder is process-global (one seam slot): the hook tests run one at
// a time so a concurrent test's detail cannot land in another's vector.
static HOOK_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn install_check_error_recorder() {
    if !guc_seams::guc_check_errdetail::is_installed() {
        guc_seams::guc_check_errdetail::set(|d| CHECK_DETAILS.lock().unwrap().push(d));
    }
}

fn run_check_hook(
    slot: &guc_tables::GucStringCheckHook,
    value: &str,
) -> (PgResult<bool>, Vec<String>) {
    let _serial = HOOK_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    // Install first: the slot is only populated by install_guc_hooks().
    install_crate_seams();
    install_check_error_recorder();
    CHECK_DETAILS.lock().unwrap().clear();
    let hook = slot.get();
    let mut newval = Some(value.to_string());
    let mut extra = None;
    let r = hook(&mut newval, &mut extra, ::types_guc::GucSource::PGC_S_FILE);
    let details = CHECK_DETAILS.lock().unwrap().clone();
    (r, details)
}

#[test]
fn check_recovery_target_rejects_via_guc_check_errdetail() {
    let (r, details) = run_check_hook(&guc_tables::hooks::check_recovery_target, "foo");
    assert!(matches!(r, Ok(false)), "got {r:?}");
    assert_eq!(details, vec!["The only allowed value is \"immediate\".".to_string()]);
    let (r, _) = run_check_hook(&guc_tables::hooks::check_recovery_target, "immediate");
    assert!(matches!(r, Ok(true)));
}

#[test]
fn check_recovery_target_name_rejects_via_guc_check_errdetail() {
    let long = "x".repeat(targets::MAXFNAMELEN);
    let (r, details) = run_check_hook(&guc_tables::hooks::check_recovery_target_name, &long);
    assert!(matches!(r, Ok(false)), "got {r:?}");
    assert_eq!(
        details,
        vec![format!(
            "\"recovery_target_name\" is too long (maximum {} characters).",
            targets::MAXFNAMELEN - 1
        )]
    );
}

#[test]
fn check_recovery_target_timeline_rejects_via_guc_check_errdetail() {
    let (r, details) = run_check_hook(
        &guc_tables::hooks::check_recovery_target_timeline,
        "99999999999999999999999",
    );
    assert!(matches!(r, Ok(false)), "got {r:?}");
    assert_eq!(
        details,
        vec!["\"recovery_target_timeline\" is not a valid number.".to_string()]
    );
}

// check_primary_slot_name (xlogrecovery.c:4796-4806): the slot-name
// validation failure is GUC_check_errcode + GUC_check_errdetail (+ hint) and
// return false; the pre-fix hook raised the slot message as the primary
// ERROR. Audit a186-verified-fp-replication-slot-1194004bffdd0bc4ee67-1 /
// a186-candidate-fp-transam-xlogrecovery-p2-d109376c1637ea0a9551-1.
#[test]
fn check_primary_slot_name_rejects_via_guc_check_errdetail() {
    let (r, details) = run_check_hook(&guc_tables::hooks::check_primary_slot_name, "Bad-Name");
    assert!(matches!(r, Ok(false)), "got {r:?}");
    assert_eq!(
        details,
        vec!["replication slot name \"Bad-Name\" contains invalid character".to_string()]
    );
    let (r, _) = run_check_hook(&guc_tables::hooks::check_primary_slot_name, "good_name");
    assert!(matches!(r, Ok(true)));
}

fn make_checkpoint(loc: XLogRecPtr) -> CheckPoint {
    let mut ckpt = CheckPoint::ZEROED;
    ckpt.redo = loc;
    ckpt.ThisTimeLineID = 1;
    ckpt.PrevTimeLineID = 1;
    ckpt.fullPageWrites = true;
    ckpt.wal_level = WAL_LEVEL_REPLICA;
    ckpt.nextXid = types_core::FullTransactionId::from_epoch_and_xid(0, 731);
    ckpt.oldestXid = 3;
    ckpt.oldestActiveXid = 17;
    ckpt
}

fn write_control_file(dir: &std::path::Path, ckpt_loc: XLogRecPtr, ckpt: &CheckPoint) {
    let mut cf = ControlFileData::ZEROED;
    cf.system_identifier = SYS_ID;
    cf.pg_control_version = PG_CONTROL_VERSION;
    cf.catalog_version_no = controldata_utils::CATALOG_VERSION_NO;
    cf.state = DB_SHUTDOWNED;
    cf.checkPoint = ckpt_loc;
    cf.checkPointCopy = *ckpt;
    cf.unloggedLSN = FirstNormalUnloggedLSN;
    cf.maxAlign = 8;
    cf.floatFormat = FLOATFORMAT_VALUE;
    cf.blcksz = 8192;
    cf.relseg_size = 131072;
    cf.xlog_blcksz = 8192;
    cf.xlog_seg_size = SEG as u32;
    cf.nameDataLen = 64;
    cf.indexMaxKeys = 32;
    cf.toast_max_chunk_size = TOAST_MAX_CHUNK_SIZE;
    cf.loblksize = 2048;
    cf.float8ByVal = true;
    cf.crc = controldata_utils::crc_of_image(&cf.to_disk_bytes());
    let mut image = vec![0u8; PG_CONTROL_FILE_SIZE];
    image[..controldata_utils::SIZEOF_CONTROL_FILE_DATA].copy_from_slice(&cf.to_disk_bytes());
    std::fs::write(dir.join("global/pg_control"), &image).unwrap();
}

fn checkpoint_record_bytes(loc: XLogRecPtr, ckpt: &CheckPoint) -> Vec<u8> {
    let tot_len = SizeOfXLogRecord + 2 + SIZEOF_CHECKPOINT;
    let mut rec = vec![0u8; tot_len];
    rec[0..4].copy_from_slice(&(tot_len as u32).to_ne_bytes());
    rec[8..16].copy_from_slice(&(loc - 0x28).to_ne_bytes());
    rec[16] = XLOG_CHECKPOINT_SHUTDOWN;
    rec[17] = RM_XLOG_ID;
    rec[24] = 255; // XLR_BLOCK_ID_DATA_SHORT
    rec[25] = SIZEOF_CHECKPOINT as u8;
    rec[26..26 + SIZEOF_CHECKPOINT].copy_from_slice(&ckpt.to_bytes());
    let crc = crc32c::fin_crc32c(crc32c::pg_comp_crc32c(
        crc32c::pg_comp_crc32c(crc32c::CRC32C_INIT, &rec[SizeOfXLogRecord..]),
        &rec[..20],
    ));
    rec[20..24].copy_from_slice(&crc.to_ne_bytes());
    rec
}

fn write_segment_with_checkpoint(dir: &std::path::Path, ckpt_loc: XLogRecPtr, ckpt: &CheckPoint) {
    let segno = ckpt_loc / SEG as u64;
    let page_addr = ckpt_loc - ckpt_loc % 8192;
    let mut seg = vec![0u8; SEG as usize];
    seg[0..2].copy_from_slice(&0xD118u16.to_ne_bytes());
    seg[2..4].copy_from_slice(&XLP_LONG_HEADER.to_ne_bytes());
    seg[4..8].copy_from_slice(&1u32.to_ne_bytes());
    seg[8..16].copy_from_slice(&page_addr.to_ne_bytes());
    seg[24..32].copy_from_slice(&SYS_ID.to_ne_bytes());
    seg[32..36].copy_from_slice(&(SEG as u32).to_ne_bytes());
    seg[36..40].copy_from_slice(&8192u32.to_ne_bytes());
    let rec = checkpoint_record_bytes(ckpt_loc, ckpt);
    let off = (ckpt_loc % SEG as u64) as usize;
    seg[off..off + rec.len()].copy_from_slice(&rec);
    let name = transam_xlog::XLogFileName(1, segno, SEG);
    std::fs::write(dir.join("pg_wal").join(name), &seg).unwrap();
}

fn install_timeline_seams() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        timeline::init_seams();
        if !timestamp_seams::get_current_timestamp::is_installed() {
            timestamp_seams::get_current_timestamp::set(|| 0);
        }
    });
}

#[test]
fn timeline_history_helpers() {
    install_timeline_seams();
    let tles = vec![Tle { tli: 3, begin: 0, end: 0 }];
    assert!(tli_in_history(3, &tles));
    assert!(!tli_in_history(2, &tles));
    assert!(tli_in_history(9, &[]));
    assert_eq!(tli_of_point_in_history(0x12345, &tles).unwrap(), 3);
    let split = vec![
        Tle { tli: 2, begin: 0x8000, end: 0 },
        Tle { tli: 1, begin: 0, end: 0x8000 },
    ];
    assert_eq!(tli_of_point_in_history(0x7FFF, &split).unwrap(), 1);
    assert_eq!(tli_of_point_in_history(0x8000, &split).unwrap(), 2);
    assert!(tli_of_point_in_history(1, &[]).is_err());
}

#[test]
fn checkpoint_record_length_constant() {
    assert_eq!(CHECKPOINT_REC_TOT_LEN, 114);
}

// Single process-global e2e: clean-shutdown InitWalRecovery →
// FinishWalRecovery → ShutdownWalRecovery against a fabricated datadir.
#[test]
fn clean_shutdown_boot_path() {
    let _g = datadir_lock();
    let dir = std::env::temp_dir().join(format!("pgrust_xlogrecovery_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    for sub in ["global", "pg_wal"] {
        std::fs::create_dir_all(dir.join(sub)).unwrap();
    }
    init_small::globals::SetDataDir(dir.to_str().unwrap());
    init_small::globals::set_enableFsync(false);
    guc_tables::init_seams();
    transam_xlog::init_seams();
    xlogprefetcher::init_seams();
    xlogprefetcher::XLogPrefetchShmemInit();
    guc_tables::vars::maintenance_io_concurrency
        .install_if_absent(guc_tables::GucVarAccessors { get: || 10, set: |_| {} });
    install_crate_seams();
    install_timeline_seams();
    // XLogPageRead brackets pg_pread with WAIT_EVENT_WAL_READ
    // (xlogrecovery.c:3434/3441/3466); record what the boot reports.
    if !waitevent_seams::pgstat_report_wait_start::is_installed() {
        waitevent_seams::pgstat_report_wait_start::set(record_wait_start);
        waitevent_seams::pgstat_report_wait_end::set(|| {});
    }
    WAIT_EVENTS.lock().unwrap().clear();

    let ckpt_loc: XLogRecPtr = SEG as u64 + SizeOfXLogLongPHD as u64;
    let ckpt = make_checkpoint(ckpt_loc);

    // (A missing/corrupt checkpoint ends in ereport(PANIC), which aborts like
    // C — not testable in-process.)
    // Valid shutdown checkpoint, redo == checkpoint location.
    write_control_file(&dir, ckpt_loc, &ckpt);
    write_segment_with_checkpoint(&dir, ckpt_loc, &ckpt);
    transam_xlog::ReadControlFile().unwrap();

    // upstream 311e66df9cc8 (18.6): a startup process can begin life with a
    // stale reachedConsistency=true (C: inherited from the postmaster on a
    // crash reset after hot standby reached consistency; pgrust: the startup
    // thread shares the process-global outright). InitWalRecovery must clear
    // it, or CheckRecoveryConsistency skips the minRecoveryPoint comparison.
    REACHED_CONSISTENCY.store(true, Relaxed);
    let init = xlogrecovery_seams::init_wal_recovery::call().unwrap();
    assert!(
        !reached_consistency(),
        "InitWalRecovery must clear an inherited reachedConsistency"
    );
    assert!(init.was_shutdown);
    assert!(!init.have_backup_label && !init.have_tblspc_map);
    // The checkpoint record read reported WalRead around its pg_pread.
    // Audit a186-candidate-fp-transam-xlogrecovery-p2-b7d4ad68f5f82b90c195-1.
    assert!(
        WAIT_EVENTS.lock().unwrap().contains(&WAIT_EVENT_WAL_READ),
        "no WAIT_EVENT_WAL_READ reported during the checkpoint read: {:?}",
        WAIT_EVENTS.lock().unwrap()
    );
    assert!(!xlogutils::in_recovery());
    assert_eq!(xlogrecovery_seams::recovery_target_tli::call(), 1);
    assert!(!xlogrecovery_seams::archive_recovery_requested::call());
    assert!(!xlogrecovery_seams::in_archive_recovery::call());
    assert_eq!(xlogrecovery_seams::recovery_oldest_active_xid::call(), 17);

    let info = xlogrecovery_seams::finish_wal_recovery::call().unwrap();
    let end_of_log = ckpt_loc + MAXALIGN(CHECKPOINT_REC_TOT_LEN as usize) as u64;
    assert_eq!(info.lastRec, ckpt_loc);
    assert_eq!(info.lastRecTLI, 1);
    assert_eq!(info.endOfLog, end_of_log);
    assert_eq!(info.endOfLogTLI, 1);
    assert_eq!(info.lastPageBeginPtr, SEG as u64);
    assert_eq!(info.lastPage.len(), (end_of_log % 8192) as usize);
    // The copied partial page carries the long header and the record.
    assert_eq!(&info.lastPage[0..2], &0xD118u16.to_ne_bytes());
    let rec_off = SizeOfXLogLongPHD;
    assert_eq!(
        &info.lastPage[rec_off..rec_off + 4],
        &CHECKPOINT_REC_TOT_LEN.to_ne_bytes()
    );
    assert_eq!(info.abortedRecPtr, InvalidXLogRecPtr);
    assert_eq!(info.missingContrecPtr, InvalidXLogRecPtr);

    // ShutdownWalRecovery (xlogrecovery.c:1641) makes the final
    // XLogPrefetcherComputeStats() pass over pg_stat_recovery_prefetch's
    // instantaneous columns before the prefetcher goes away: a planted
    // pre-state must not survive it. Audit
    // a186-candidate-fp-transam-xlogrecovery-p1-32457ca830b68c7bfe2d-1.
    xlogprefetcher::xlog_prefetch_poke_shared_distances(7, 8, 9);
    xlogrecovery_seams::shutdown_wal_recovery::call().unwrap();
    RECOVERY.with(|c| assert!(c.borrow().is_none()));
    let distances = xlogprefetcher::xlog_prefetch_shared_distances();
    assert_ne!(
        distances,
        (7, 8, 9),
        "ShutdownWalRecovery left pg_stat_recovery_prefetch distances unfinalized"
    );
    assert_eq!(distances.2, 0, "io_depth after the last read is 0 (nothing in flight)");

    // ApplyWalRecord's timeline-switch epilogue (xlogrecovery.c:2092) resets
    // the prefetcher with XLogPrefetchReconfigure(). Audit
    // a186-candidate-fp-transam-xlogrecovery-p1-8fea805aa62fca3ae264-1.
    let reconfigures = xlogprefetcher::xlog_prefetch_reconfigure_count();
    after_timeline_switch(end_of_log, 2).unwrap();
    assert_eq!(
        xlogprefetcher::xlog_prefetch_reconfigure_count(),
        reconfigures + 1,
        "a timeline switch must XLogPrefetchReconfigure()"
    );

    // Promote signal file removal.
    std::fs::write(dir.join(PROMOTE_SIGNAL_FILE), b"").unwrap();
    xlogrecovery_seams::remove_promote_signal_files::call();
    assert!(!dir.join(PROMOTE_SIGNAL_FILE).exists());

    assert!(!xlogrecovery_seams::reached_consistency::call());
    assert!(!xlogrecovery_seams::promote_is_triggered::call());
    assert_eq!(xlogrecovery_seams::get_xlog_replay_rec_ptr::call(), (0, 0));

    let _ = std::fs::remove_dir_all(&dir);
}

// verifyBackupPageConsistency (xlogrecovery.c:2483) is now ported: a WAL
// record carrying XLR_CHECK_CONSISTENCY is cross-checked after redo via
// verify_backup_page_consistency (rm_mask + memcmp, FATAL on real mismatch),
// rather than the earlier skip-with-warning stub. The end-to-end behavior
// (boots + replays clean under wal_consistency_checking=all, planted-divergence
// FATAL) is exercised by scripts/recovery-diff-campaign.sh; here we only pin
// the flag bit the apply loop tests.
#[test]
fn consistency_check_flag_bit_pinned() {
    // xlogrecord.h: #define XLR_CHECK_CONSISTENCY 0x02
    assert_eq!(XLR_CHECK_CONSISTENCY, 0x02);
}

// The mask+compare lane of verify_backup_page_consistency, exercised without
// WAL machinery: pd_lsn parsing, mask-forgives-hint-divergence, and
// mask-preserves-real-divergence (the FATAL trigger condition).
mod mask_compare_lane {
    use super::super::{page_lsn, BLCKSZ};
    use types_core::RmgrIds;
    use types_storage::bufpage::{PageMut, PAI_IS_HEAP};
    use types_tuple::htup::{HeapTupleHeaderData, SizeofHeapTupleHeader};
    use types_tuple::{HEAP_XMAX_COMMITTED, HEAP_XMIN_COMMITTED};

    #[repr(align(8))]
    struct P([u8; BLCKSZ]);

    fn pm(p: &mut P) -> PageMut<'_> {
        let ptr = core::ptr::NonNull::new(p.0.as_mut_ptr()).unwrap();
        // SAFETY: owned MAXALIGNed BLCKSZ image, exclusively borrowed.
        unsafe { PageMut::from_raw(ptr) }
    }

    // PageGetLSN over the two PageXLogRecPtr u32 halves.
    #[test]
    fn page_lsn_matches_page_set_lsn() {
        let mut p = P([0u8; BLCKSZ]);
        let mut page = pm(&mut p);
        page.init(0);
        page.set_lsn(0x1122_3344_5566_7788);
        assert_eq!(page_lsn(&p.0), 0x1122_3344_5566_7788);
        assert_eq!(page_lsn(&[0u8; BLCKSZ]), 0);
    }

    fn heap_page_with_tuple(lsn: u64, infomask: u16, payload: u8) -> P {
        let mut p = P([0u8; BLCKSZ]);
        let mut page = pm(&mut p);
        page.init(0);
        let mut body = [payload; SizeofHeapTupleHeader + 8];
        // Zero the header portion so only t_infomask below differs.
        body[..SizeofHeapTupleHeader].fill(0);
        page.add_item(&body, 0, PAI_IS_HEAP).unwrap();
        page.set_lsn(lsn);
        let off = page.as_ref().item_id(1).lp_off() as usize;
        // SAFETY: the item just added stores a HeapTupleHeaderData at `off`.
        let htup = unsafe { &mut *(page.as_mut_ptr().add(off) as *mut HeapTupleHeaderData) };
        htup.t_infomask = infomask;
        p
    }

    // Replay side and primary FPI differing only in maskable state (pd_lsn,
    // hint bits) must compare equal after rm_mask — no false FATAL.
    #[test]
    fn heap_mask_forgives_lsn_and_hint_bit_divergence() {
        let mask = rmgr::GetRmgr(RmgrIds::RM_HEAP_ID as u8)
            .unwrap()
            .rm_mask
            .expect("heap rmgr has rm_mask");
        let mut replay = heap_page_with_tuple(0x0AAA_0000, HEAP_XMIN_COMMITTED, 0x5A);
        let mut primary =
            heap_page_with_tuple(0x0BBB_0000, HEAP_XMIN_COMMITTED | HEAP_XMAX_COMMITTED, 0x5A);
        assert_ne!(replay.0[..], primary.0[..]);
        mask(&mut replay.0, 7).unwrap();
        mask(&mut primary.0, 7).unwrap();
        assert_eq!(replay.0[..], primary.0[..]);
    }

    // A genuine data divergence (different tuple payload) must survive the
    // mask — this inequality is exactly what raises the C-text FATAL.
    #[test]
    fn heap_mask_preserves_real_data_divergence() {
        let mask = rmgr::GetRmgr(RmgrIds::RM_HEAP_ID as u8)
            .unwrap()
            .rm_mask
            .expect("heap rmgr has rm_mask");
        let mut replay = heap_page_with_tuple(0x0AAA_0000, 0, 0x5A);
        let mut primary = heap_page_with_tuple(0x0AAA_0000, 0, 0xA5);
        mask(&mut replay.0, 7).unwrap();
        mask(&mut primary.0, 7).unwrap();
        assert_ne!(replay.0[..], primary.0[..]);
    }

    // Every rmgr with rm_mask in C 18.3 rmgrlist.h has one here, and only
    // those — an unported mask would silently weaken the consistency check.
    #[test]
    fn mask_coverage_matches_c_rmgrlist() {
        use RmgrIds::*;
        let masked = [
            RM_HEAP2_ID,
            RM_HEAP_ID,
            RM_BTREE_ID,
            RM_HASH_ID,
            RM_GIN_ID,
            RM_GIST_ID,
            RM_SEQ_ID,
            RM_SPGIST_ID,
            RM_BRIN_ID,
            RM_GENERIC_ID,
        ];
        for id in 0..=RM_LOGICALMSG_ID as u8 {
            let has = rmgr::GetRmgr(id).unwrap().rm_mask.is_some();
            let expect = masked.iter().any(|&m| m as u8 == id);
            assert_eq!(has, expect, "rmid {id} mask presence");
        }
    }
}
