//! NATIVE ARM ONLY: these units build fixtures through std::fs, which under
//! `--cfg pgrust_sim` would split across the real disk and the SimVfs
//! namespace; the sim-cfg recovery battery is tests/sim_crash_sweep.rs.
#![cfg(not(pgrust_sim))]

use super::*;
use controldata_utils::{CheckPoint, ControlFileData};
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

// One record with a short main-data chunk (XLR_BLOCK_ID_DATA_SHORT), CRC'd.
fn record_bytes(loc: XLogRecPtr, rmid: u8, info: u8, main_data: &[u8]) -> Vec<u8> {
    assert!(main_data.len() < 256);
    let tot_len = SizeOfXLogRecord + 2 + main_data.len();
    let mut rec = vec![0u8; tot_len];
    rec[0..4].copy_from_slice(&(tot_len as u32).to_ne_bytes());
    rec[8..16].copy_from_slice(&(loc - 0x28).to_ne_bytes());
    rec[16] = info;
    rec[17] = rmid;
    rec[24] = 255; // XLR_BLOCK_ID_DATA_SHORT
    rec[25] = main_data.len() as u8;
    rec[26..26 + main_data.len()].copy_from_slice(main_data);
    let crc = crc32c::fin_crc32c(crc32c::pg_comp_crc32c(
        crc32c::pg_comp_crc32c(crc32c::CRC32C_INIT, &rec[SizeOfXLogRecord..]),
        &rec[..20],
    ));
    rec[20..24].copy_from_slice(&crc.to_ne_bytes());
    rec
}

fn checkpoint_record_bytes(loc: XLogRecPtr, ckpt: &CheckPoint) -> Vec<u8> {
    record_bytes(loc, RM_XLOG_ID, XLOG_CHECKPOINT_SHUTDOWN, &ckpt.to_bytes())
}

// Timeline-1 segment holding `rec` at `loc` (long page header on the first page).
fn write_segment_with_record(dir: &std::path::Path, loc: XLogRecPtr, rec: &[u8]) {
    let segno = loc / SEG as u64;
    let page_addr = loc - loc % 8192;
    let mut seg = vec![0u8; SEG as usize];
    seg[0..2].copy_from_slice(&0xD118u16.to_ne_bytes());
    seg[2..4].copy_from_slice(&XLP_LONG_HEADER.to_ne_bytes());
    seg[4..8].copy_from_slice(&1u32.to_ne_bytes());
    seg[8..16].copy_from_slice(&page_addr.to_ne_bytes());
    seg[24..32].copy_from_slice(&SYS_ID.to_ne_bytes());
    seg[32..36].copy_from_slice(&(SEG as u32).to_ne_bytes());
    seg[36..40].copy_from_slice(&8192u32.to_ne_bytes());
    let off = (loc % SEG as u64) as usize;
    seg[off..off + rec.len()].copy_from_slice(rec);
    let name = transam_xlog::XLogFileName(1, segno, SEG);
    std::fs::write(dir.join("pg_wal").join(name), &seg).unwrap();
}

fn write_segment_with_checkpoint(dir: &std::path::Path, ckpt_loc: XLogRecPtr, ckpt: &CheckPoint) {
    write_segment_with_record(dir, ckpt_loc, &checkpoint_record_bytes(ckpt_loc, ckpt));
}

// One-shot process init shared by the recovery-driving units (seam/hook
// installs refuse a second install).
fn install_boot_seams() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        guc_tables::init_seams();
        transam_xlog::init_seams();
        xlogprefetcher::init_seams();
        xlogprefetcher::XLogPrefetchShmemInit();
        guc_tables::vars::maintenance_io_concurrency
            .install_if_absent(guc_tables::GucVarAccessors { get: || 10, set: |_| {} });
    });
    install_crate_seams();
    install_timeline_seams();
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
    install_boot_seams();
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

// ---------------------------------------------------------------------------
// read_backup_label / read_tablespace_map error surfaces (xlogrecovery.c:
// 1260-1358, 1395-1469). The twelve verified rows of batch
// b192-unit-fp-transam-xlogrecovery-p1-1 were fixed by #1800 (49124e31e1d):
// every "invalid data in file" FATAL carries 55000, read failures carry
// errcode_for_file_access() + strerror (%m), the START TIME / LABEL lines are
// logged at DEBUG1, and the file is read as raw bytes (a non-UTF-8 label byte
// is not a read failure). These are the current-main witnesses per arm.
//
// A FATAL runs errfinish's proc_exit(1) arm (elog.c:600-ish; stack.rs); the
// stubbed proc_exit seam panics so catch_unwind observes it, and the emit
// hook captures the report (level, SQLSTATE, message, DETAIL, HINT) first.

const LABEL_HEAD: &str =
    "START WAL LOCATION: 0/16000028 (file 000000010000000000000016)\nCHECKPOINT LOCATION: 0/16000060\n";
const LABEL_TRAILER: &str =
    "BACKUP METHOD: streamed\nBACKUP FROM: primary\nSTART TIME: 2026-09-03 03:49:34 PDT\nLABEL: fp label\n";

static REPORTS: std::sync::Mutex<Vec<types_error::PgError>> = std::sync::Mutex::new(Vec::new());

fn capture_report(err: &types_error::PgError, _output_to_server: &mut bool) {
    REPORTS.lock().unwrap().push(err.clone());
}

fn install_fatal_exit_seams() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        if !pgstat_seams::pgstat_set_session_end_cause_fatal::is_installed() {
            pgstat_seams::pgstat_set_session_end_cause_fatal::set(|| {});
        }
        if !init_small_seams::my_proc_pid::is_installed() {
            init_small_seams::my_proc_pid::set(|| 4242);
        }
        if !ipc_seams::proc_exit::is_installed() {
            ipc_seams::proc_exit::set(|code, _pid| panic!("proc_exit({code})"));
        }
    });
}

// Runs `f` (a reader that must FATAL) under the capturing emit hook; the
// FATAL report is returned after the proc_exit(1) unwind is caught.
fn expect_fatal(f: impl FnOnce() -> PgResult<()>) -> types_error::PgError {
    install_fatal_exit_seams();
    REPORTS.lock().unwrap().clear();
    let prev = elog::set_emit_log_hook(Some(capture_report));
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    elog::set_emit_log_hook(prev);
    elog::FlushErrorState();
    let payload = result.expect_err("a FATAL report must reach proc_exit(1)");
    assert_eq!(payload.downcast_ref::<String>().map(String::as_str), Some("proc_exit(1)"));
    let err = REPORTS.lock().unwrap().pop().expect("FATAL report was emitted");
    assert_eq!(err.level, FATAL);
    err
}

fn read_label_fatal(dir: &std::path::Path, content: &[u8]) -> types_error::PgError {
    std::fs::write(dir.join(BACKUP_LABEL_FILE), content).unwrap();
    expect_fatal(|| backup_label::read_backup_label().map(drop))
}

fn read_map_fatal(dir: &std::path::Path, content: &[u8]) -> types_error::PgError {
    std::fs::write(dir.join(TABLESPACE_MAP), content).unwrap();
    expect_fatal(|| backup_label::read_tablespace_map().map(drop))
}

// xlogrecovery.c:1278/1285: the START WAL LOCATION and CHECKPOINT LOCATION
// fscanf failures are FATAL errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
// "invalid data in file \"backup_label\"" (pre-#1800 pgrust: XX000). Audit
// rows 6521841a (garbage file) and a5b3dac7 (CHECKPOINT LOCATION: junk).
#[test]
fn backup_label_invalid_data_is_fatal_55000_like_c() {
    let _g = datadir_lock();
    let dir = fixture_datadir("label_invalid");
    for content in [
        &b"garbage\n"[..],
        b"START WAL LOCATION: 0/16000028 (file 000000010000000000000016)\nCHECKPOINT LOCATION: junk\n",
    ] {
        let err = read_label_fatal(&dir, content);
        assert_eq!(err.sqlstate(), types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
        assert_eq!(err.message(), "invalid data in file \"backup_label\"");
        assert_eq!(err.detail(), None);
        assert_eq!(err.hint(), None);
    }
    let _ = std::fs::remove_dir_all(&dir);
}

// xlogrecovery.c:1341-1348: the in-order START TIMELINE cross-check is FATAL
// 55000 with errdetail("Timeline ID parsed is %u, but expected %u."). Audit
// row ced9841a.
#[test]
fn backup_label_timeline_mismatch_is_fatal_55000_with_detail_like_c() {
    let _g = datadir_lock();
    let dir = fixture_datadir("label_tli");
    let content = format!("{LABEL_HEAD}{LABEL_TRAILER}START TIMELINE: 2\n");
    let err = read_label_fatal(&dir, content.as_bytes());
    assert_eq!(err.sqlstate(), types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
    assert_eq!(err.message(), "invalid data in file \"backup_label\"");
    assert_eq!(err.detail(), Some("Timeline ID parsed is 2, but expected 1."));
    let _ = std::fs::remove_dir_all(&dir);
}

// xlogrecovery.c:1355-1359: INCREMENTAL FROM LSN is FATAL 55000 "this is an
// incremental backup, not a data directory" + the pg_combinebackup hint.
// Audit row 56d75d79.
#[test]
fn backup_label_incremental_is_fatal_55000_with_hint_like_c() {
    let _g = datadir_lock();
    let dir = fixture_datadir("label_incr");
    let content = format!(
        "{LABEL_HEAD}{LABEL_TRAILER}START TIMELINE: 1\nINCREMENTAL FROM LSN: 0/2000028\nINCREMENTAL FROM TLI: 1\n"
    );
    let err = read_label_fatal(&dir, content.as_bytes());
    assert_eq!(err.sqlstate(), types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
    assert_eq!(err.message(), "this is an incremental backup, not a data directory");
    assert_eq!(err.hint(), Some("Use pg_combinebackup to reconstruct a valid data directory."));
    let _ = std::fs::remove_dir_all(&dir);
}

// xlogrecovery.c:1262-1266 / 1401-1405: a read failure other than ENOENT is
// FATAL errcode_for_file_access() "could not read file \"%s\": %m" — the
// strerror text, no "(os error N)" suffix (pre-#1800 pgrust: XX000 + io::Error
// Display). ELOOP (self-referential symlink) exercises the default XX000 arm
// for any uid; EACCES (mode 000) the 42501 arm as a non-root user. Audit rows
// a3aac855 (backup_label) and d9f49f36 (tablespace_map).
fn unreadable_file_is_fatal_file_access_like_c(tag: &str, file: &str, read: fn() -> PgResult<()>) {
    let _g = datadir_lock();
    let dir = fixture_datadir(tag);
    std::os::unix::fs::symlink(file, dir.join(file)).unwrap();
    let err = expect_fatal(read);
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);
    assert_eq!(
        err.message(),
        format!("could not read file \"{file}\": {}", elog::errno::strerror(libc::ELOOP))
    );
    if unsafe { libc::geteuid() } != 0 {
        use std::os::unix::fs::PermissionsExt;
        std::fs::remove_file(dir.join(file)).unwrap();
        std::fs::write(dir.join(file), b"").unwrap();
        std::fs::set_permissions(dir.join(file), std::fs::Permissions::from_mode(0o000)).unwrap();
        let err = expect_fatal(read);
        std::fs::set_permissions(dir.join(file), std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_INSUFFICIENT_PRIVILEGE);
        assert_eq!(
            err.message(),
            format!("could not read file \"{file}\": {}", elog::errno::strerror(libc::EACCES))
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn backup_label_unreadable_is_fatal_file_access_like_c() {
    unreadable_file_is_fatal_file_access_like_c("label_eacces", BACKUP_LABEL_FILE, || {
        backup_label::read_backup_label().map(drop)
    });
}

#[test]
fn tablespace_map_unreadable_is_fatal_file_access_like_c() {
    unreadable_file_is_fatal_file_access_like_c("map_eacces", TABLESPACE_MAP, || {
        backup_label::read_tablespace_map().map(drop)
    });
}

// xlogrecovery.c:1432/1441/1461: every tablespace_map reject (no space in the
// line, strtoul trailing junk, unterminated last line) is FATAL 55000
// "invalid data in file \"tablespace_map\"". Audit rows d6e577be, 37b281d6,
// 8eaef6c5.
#[test]
fn tablespace_map_invalid_data_is_fatal_55000_like_c() {
    let _g = datadir_lock();
    let dir = fixture_datadir("map_invalid");
    for content in [&b"nospace\n"[..], b"16384x /tmp/ts\n", b"16385 /tmp/ts"] {
        let err = read_map_fatal(&dir, content);
        assert_eq!(err.sqlstate(), types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
        assert_eq!(err.message(), "invalid data in file \"tablespace_map\"");
        assert_eq!(err.detail(), None);
    }
    let _ = std::fs::remove_dir_all(&dir);
}

// Reads backup_label under log_min_messages=debug1 with the emit hook
// capturing every report; returns the label and the emitted messages.
fn read_label_at_debug1(content: &[u8]) -> (backup_label::BackupLabel, Vec<String>) {
    let dir = fixture_datadir("label_debug1");
    std::fs::write(dir.join(BACKUP_LABEL_FILE), content).unwrap();
    let prev_min = elog::config::log_min_messages();
    elog::config::set_log_min_messages(types_error::DEBUG1);
    REPORTS.lock().unwrap().clear();
    let prev = elog::set_emit_log_hook(Some(capture_report));
    let r = backup_label::read_backup_label();
    elog::set_emit_log_hook(prev);
    elog::config::set_log_min_messages(prev_min);
    let _ = std::fs::remove_dir_all(&dir);
    let label = r.expect("a well-formed backup_label is read").expect("backup_label present");
    let reports = REPORTS.lock().unwrap();
    for e in reports.iter() {
        assert_eq!(e.level, types_error::DEBUG1, "unexpected report {:?}", e.message());
    }
    let msgs = reports.iter().map(|e| e.message().to_string()).collect();
    (label, msgs)
}

// xlogrecovery.c:1327-1335 / 1349-1352: START TIME, LABEL and START TIMELINE
// are each logged at DEBUG1 (errmsg_internal), in that order (pre-#1800
// pgrust had no START TIME / LABEL arm). Audit rows 4497bc1c and 5080bc8c.
#[test]
fn backup_label_start_time_and_label_are_logged_at_debug1_like_c() {
    let _g = datadir_lock();
    let content = format!("{LABEL_HEAD}{LABEL_TRAILER}START TIMELINE: 1\n");
    let (label, msgs) = read_label_at_debug1(content.as_bytes());
    assert!(label.backup_end_required);
    assert_eq!(
        msgs,
        vec![
            "backup time 2026-09-03 03:49:34 PDT in file \"backup_label\"".to_string(),
            "backup label fp label in file \"backup_label\"".to_string(),
            "backup timeline 1 in file \"backup_label\"".to_string(),
        ]
    );
}

// xlogrecovery.c:1260 reads the file bytewise (fscanf): a raw 0xE9 in the
// LABEL line is not a read failure — C logs the label and starts recovery.
// Pre-#1800 pgrust read_to_string()ed the file and FATALed "could not read
// file \"backup_label\": stream did not contain valid UTF-8" (XX000). Audit
// row e063214a. (Residual, outside this crate: the server-log line renders
// the byte as U+FFFD because elog's log writer takes the String rendering —
// PgError.message_raw only feeds the frontend wire — while C writes the raw
// byte.)
#[test]
fn backup_label_with_non_utf8_label_byte_starts_recovery_like_c() {
    let _g = datadir_lock();
    let mut content = format!(
        "{LABEL_HEAD}BACKUP METHOD: streamed\nBACKUP FROM: primary\nSTART TIME: 2026-09-03 03:49:34 PDT\nLABEL: caf"
    )
    .into_bytes();
    content.extend_from_slice(b"\xe9\nSTART TIMELINE: 1\n");
    let (label, msgs) = read_label_at_debug1(&content);
    assert_eq!(label.redo_start_lsn, 0x16000028);
    assert_eq!(label.checkpoint_loc, 0x16000060);
    assert_eq!(label.backup_label_tli, 1);
    assert!(label.backup_end_required);
    assert!(
        msgs.iter()
            .any(|m| m.starts_with("backup label caf") && m.ends_with(" in file \"backup_label\"")),
        "DEBUG1 backup label line missing; got {msgs:?}"
    );
    assert!(msgs.contains(&"backup timeline 1 in file \"backup_label\"".to_string()));
}

// ---------------------------------------------------------------------------
// WAL-source state machine witnesses (audit-18.6 b206): the startup process's
// interrupt/promote/walreceiver periphery as seams, driven in-process.

// ProcessStartupProcInterrupts stand-in: runs the test's hook (the SIGHUP /
// promote work C does there), one hook at a time under DATADIR_LOCK.
static INTERRUPT_HOOK: std::sync::Mutex<Option<fn()>> = std::sync::Mutex::new(None);
static PROMOTE_SIGNALED: AtomicBool = AtomicBool::new(false);
// A walreceiver stand-in: WalRcvStreaming()/WalRcvRunning() while up,
// ShutdownWalRcv() takes it down.
static WALRCV_UP: AtomicBool = AtomicBool::new(false);

fn install_startup_process_seams() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        if !startup_seams::process_startup_proc_interrupts::is_installed() {
            startup_seams::process_startup_proc_interrupts::set(|| {
                let hook = *INTERRUPT_HOOK.lock().unwrap();
                if let Some(hook) = hook {
                    hook();
                }
                Ok(())
            });
        }
        if !startup_seams::is_promote_signaled::is_installed() {
            startup_seams::is_promote_signaled::set(|| PROMOTE_SIGNALED.load(Relaxed));
            startup_seams::reset_promote_signaled::set(|| PROMOTE_SIGNALED.store(false, Relaxed));
        }
        if !walreceiverfuncs_seams::wal_rcv_streaming::is_installed() {
            walreceiverfuncs_seams::wal_rcv_streaming::set(|| WALRCV_UP.load(Relaxed));
            walreceiverfuncs_seams::wal_rcv_running::set(|| WALRCV_UP.load(Relaxed));
            walreceiverfuncs_seams::shutdown_wal_rcv::set(|| {
                WALRCV_UP.store(false, Relaxed);
                Ok(())
            });
            walreceiverfuncs_seams::get_wal_rcv_flush_rec_ptr::set(|| {
                (WALRCV_FLUSHED_UPTO.load(Relaxed), InvalidXLogRecPtr, 1)
            });
            walreceiverfuncs_seams::wal_rcv_force_reply::set(|| {});
        }
    });
}

// A booted-looking data directory: control file read (wal_segment_size),
// empty pg_wal, the crate/timeline/boot seams installed.
fn boot_fixture(tag: &str) -> std::path::PathBuf {
    let dir = fixture_datadir(tag);
    for sub in ["global", "pg_wal"] {
        std::fs::create_dir_all(dir.join(sub)).unwrap();
    }
    init_small::globals::set_enableFsync(false);
    install_boot_seams();
    let ckpt_loc: XLogRecPtr = SEG as u64 + SizeOfXLogLongPHD as u64;
    let ckpt = make_checkpoint(ckpt_loc);
    write_control_file(&dir, ckpt_loc, &ckpt);
    transam_xlog::ReadControlFile().unwrap();
    dir
}

fn restart_log_count() -> usize {
    REPORTS
        .lock()
        .unwrap()
        .iter()
        .filter(|e| e.message() == "WAL receiver process shutdown requested")
        .count()
}

static STREAM_WAKES: AtomicU32 = AtomicU32::new(0);
static DISTANCES_AT_FIRST_WAKE: std::sync::Mutex<Option<(i32, i32, i32)>> =
    std::sync::Mutex::new(None);
// The walreceiver stand-in's flushed-up-to pointer (GetWalRcvFlushRecPtr).
static WALRCV_FLUSHED_UPTO: AtomicU64 = AtomicU64::new(0);
static PENDING_AFTER_ON_STREAM_RELOAD: AtomicBool = AtomicBool::new(false);

// The startup interrupt work of the stream-idle scenario, per wake-up.
fn stream_idle_interrupt() {
    let wake = STREAM_WAKES.fetch_add(1, Relaxed) + 1;
    if wake == 1 {
        // Back from the idle WaitLatch (xlogrecovery.c:4034): what
        // pg_stat_recovery_prefetch showed while the standby waited.
        *DISTANCES_AT_FIRST_WAKE.lock().unwrap() =
            Some(xlogprefetcher::xlog_prefetch_shared_distances());
        // A SIGHUP with changed primary_conninfo while currentSource ==
        // XLOG_FROM_STREAM and the walreceiver is up (StartupRereadConfig ->
        // StartupRequestWalReceiverRestart). The request is observed and
        // cleared: the restart arm itself (XLogShutdownWalRcv +
        // RequestXLogStreaming) needs XLogCtl, which this unit does not boot.
        StartupRequestWalReceiverRestart();
        PENDING_AFTER_ON_STREAM_RELOAD.store(PENDING_WALRCV_RESTART.with(Cell::get), Relaxed);
        PENDING_WALRCV_RESTART.with(|c| c.set(false));
        // WAL arrives: the walreceiver flushed past the requested page.
        WALRCV_FLUSHED_UPTO.store(SEG as u64 + 2 * 8192, Relaxed);
    }
}

struct StreamIdleRun {
    distances_at_first_wake: Option<(i32, i32, i32)>,
    reports: Vec<types_error::PgError>,
    on_stream_reload_logged: usize,
    on_stream_reload_pending: bool,
    off_stream_reloads_logged: usize,
    off_stream_reload_pending: bool,
    stream_result: i32,
    pg_wal_result: i32,
}

// Leg 1 — WaitForWALToBecomeAvailable in standby mode, XLOG_FROM_STREAM with
// the walreceiver up and nothing flushed yet: one idle wait
// (xlogrecovery.c:4006-4037); the interrupt hook then delivers a reload and
// the flush, and the segment opens from pg_wal (XLREAD_SUCCESS).
// Leg 2 — crash recovery (XLOG_FROM_PG_WAL) asking for a segment no timeline
// has: XLogFileReadAnyTLI fails it (XLREAD_FAIL).
// StartupRequestWalReceiverRestart is exercised off-stream before leg 1
// (XLOG_FROM_ANY) and after leg 2 (XLOG_FROM_PG_WAL), and on-stream from the hook.
fn run_stream_idle_scenario() -> StreamIdleRun {
    let _g = datadir_lock();
    let dir = boot_fixture("stream_idle");
    install_startup_process_seams();
    *INTERRUPT_HOOK.lock().unwrap() = Some(stream_idle_interrupt);
    STREAM_WAKES.store(0, Relaxed);
    *DISTANCES_AT_FIRST_WAKE.lock().unwrap() = None;
    WALRCV_FLUSHED_UPTO.store(InvalidXLogRecPtr, Relaxed);
    PENDING_AFTER_ON_STREAM_RELOAD.store(false, Relaxed);
    PROMOTE_SIGNALED.store(false, Relaxed);
    WALRCV_UP.store(true, Relaxed);
    RECOVERY_TARGET_TLI.store(1, Relaxed);
    IN_ARCHIVE_RECOVERY.store(true, Relaxed);
    STANDBY_MODE.store(true, Relaxed);
    PENDING_WALRCV_RESTART.with(|c| c.set(false));
    REPORTS.lock().unwrap().clear();
    let prev_min = elog::config::log_min_messages();
    elog::config::set_log_min_messages(types_error::DEBUG2);
    let prev = elog::set_emit_log_hook(Some(capture_report));

    // No WAL source chosen yet (currentSource == XLOG_FROM_ANY), walreceiver
    // up: a reload requests nothing (xlogrecovery.c:4430).
    StartupRequestWalReceiverRestart();
    let mut off_stream_reloads_logged = restart_log_count();
    let mut off_stream_reload_pending = PENDING_WALRCV_RESTART.with(Cell::get);
    PENDING_WALRCV_RESTART.with(|c| c.set(false));

    latch::OwnLatch(targets::recovery_wakeup_latch()).unwrap();
    latch::SetLatch(targets::recovery_wakeup_latch());
    // A stale pre-state in pg_stat_recovery_prefetch's instantaneous columns.
    xlogprefetcher::xlog_prefetch_poke_shared_distances(4032, 64, 0);
    // The segment the walreceiver is writing (opened once data arrives).
    std::fs::write(
        dir.join("pg_wal").join(transam_xlog::XLogFileName(1, 1, SEG)),
        vec![0u8; 8192],
    )
    .unwrap();
    let mut src = PageSource::new();
    src.cur_source = XLogSource::Stream;
    src.cur_file_tli = 1;
    src.read_seg_no = 1;
    src.replay_tli = 1;
    let rec_ptr = SEG as u64 + 8192;
    let stream_result = src.wait_for_wal(rec_ptr, rec_ptr, InvalidXLogRecPtr, false);
    src.close_read_file();
    let on_stream_reload_logged = restart_log_count() - off_stream_reloads_logged;
    let on_stream_reload_pending = PENDING_AFTER_ON_STREAM_RELOAD.load(Relaxed);

    // Leg 2: crash recovery reading pg_wal, segment 2 absent on every
    // timeline.
    WALRCV_UP.store(false, Relaxed);
    STANDBY_MODE.store(false, Relaxed);
    IN_ARCHIVE_RECOVERY.store(false, Relaxed);
    let mut src = PageSource::new();
    src.read_seg_no = 2;
    src.replay_tli = 1;
    let rec_ptr = 2 * SEG as u64 + 8192;
    let pg_wal_result = src.wait_for_wal(rec_ptr, rec_ptr, InvalidXLogRecPtr, false);

    // Reading pg_wal (currentSource == XLOG_FROM_PG_WAL) with a walreceiver
    // up: C ignores the reload.
    WALRCV_UP.store(true, Relaxed);
    PENDING_WALRCV_RESTART.with(|c| c.set(false));
    let before = restart_log_count();
    StartupRequestWalReceiverRestart();
    off_stream_reloads_logged += restart_log_count() - before;
    off_stream_reload_pending |= PENDING_WALRCV_RESTART.with(Cell::get);
    PENDING_WALRCV_RESTART.with(|c| c.set(false));

    latch::DisownLatch(targets::recovery_wakeup_latch());
    elog::set_emit_log_hook(prev);
    elog::config::set_log_min_messages(prev_min);
    *INTERRUPT_HOOK.lock().unwrap() = None;
    WALRCV_UP.store(false, Relaxed);
    let reports = REPORTS.lock().unwrap().clone();
    let _ = std::fs::remove_dir_all(&dir);
    StreamIdleRun {
        distances_at_first_wake: *DISTANCES_AT_FIRST_WAKE.lock().unwrap(),
        reports,
        on_stream_reload_logged,
        on_stream_reload_pending,
        off_stream_reloads_logged,
        off_stream_reload_pending,
        stream_result: stream_result.expect("leg 1 ends in XLREAD_SUCCESS, not an error"),
        pg_wal_result: pg_wal_result.expect("leg 2 ends in XLREAD_FAIL, not an error"),
    }
}

// xlogrecovery.c:4028-4029: XLogPrefetcherComputeStats(xlogprefetcher) runs
// before every idle WaitLatch for streamed WAL, so pg_stat_recovery_prefetch
// shows the caught-up state (wal_distance 0) while the standby waits; the
// pre-fix port went from KnownAssignedTransactionIdsIdleMaintenance straight
// to the wait and the view kept the last distance-triggered values. Audit
// a186-verified-fp-transam-xlogrecovery-p2-fc4e81b81c20729c4b34-1.
#[test]
fn stream_idle_wait_refreshes_recovery_prefetch_stats_like_c() {
    let run = run_stream_idle_scenario();
    assert_eq!(run.stream_result, XLREAD_SUCCESS);
    let distances = run.distances_at_first_wake.expect("the idle wait was reached");
    assert_ne!(
        distances,
        (4032, 64, 0),
        "pg_stat_recovery_prefetch kept its stale distances across the idle wait"
    );
    assert_eq!(distances.0, 0, "wal_distance while idle and caught up: {distances:?}");
}

// xlogrecovery.c:4430: StartupRequestWalReceiverRestart requests the restart
// only while currentSource == XLOG_FROM_STREAM (and WalRcvRunning()); the
// pre-fix port dropped the source test, so a reload while reading from the
// archive or pg_wal with a walreceiver still up logged "WAL receiver process
// shutdown requested" and set pendingWalRcvRestart. Audit
// a186-candidate-fp-transam-xlogrecovery-p2-f4f3dbf626ae9a41247b-1.
#[test]
fn walreceiver_restart_request_needs_stream_source_like_c() {
    let run = run_stream_idle_scenario();
    assert_eq!(run.stream_result, XLREAD_SUCCESS);
    assert_eq!(run.pg_wal_result, XLREAD_FAIL);
    assert_eq!(
        run.on_stream_reload_logged, 1,
        "on-stream reload with the walreceiver up must log the shutdown request"
    );
    assert!(run.on_stream_reload_pending, "on-stream reload must set pendingWalRcvRestart");
    assert_eq!(
        run.off_stream_reloads_logged, 0,
        "off-stream reloads must not log \"WAL receiver process shutdown requested\""
    );
    assert!(!run.off_stream_reload_pending, "off-stream reload set pendingWalRcvRestart");
}

// xlogrecovery.c:4414-4419: when no expected timeline has the segment,
// XLogFileReadAnyTLI reports ereport(DEBUG2, errcode_for_file_access(),
// "could not open file \"%s\": %m") for the front timeline's path with
// errno = ENOENT before returning -1; the pre-fix port returned silently.
// Audit a186-candidate-fp-transam-xlogrecovery-p2-6238a76d0efbfb3b2ecf-1.
#[test]
fn missing_segment_on_every_timeline_is_logged_at_debug2_like_c() {
    let run = run_stream_idle_scenario();
    assert_eq!(run.pg_wal_result, XLREAD_FAIL);
    let expected = "could not open file \"pg_wal/000000010000000000000002\": No such file or directory";
    let hit = run
        .reports
        .iter()
        .find(|e| e.message() == expected)
        .unwrap_or_else(|| {
            panic!(
                "no {expected:?} report; got {:?}",
                run.reports.iter().map(|e| e.message().to_string()).collect::<Vec<_>>()
            )
        });
    assert_eq!(hit.level, types_error::DEBUG2);
    assert_eq!(hit.sqlstate(), types_error::ERRCODE_UNDEFINED_FILE);
}

static APPLY_DELAY_WAKES: AtomicU32 = AtomicU32::new(0);

fn apply_delay_interrupt() {
    let wake = APPLY_DELAY_WAKES.fetch_add(1, Relaxed) + 1;
    if wake == 1 {
        // The delay wait must not sleep.
        latch::SetLatch(targets::recovery_wakeup_latch());
    } else {
        // "This might change recovery_min_apply_delay" (xlogrecovery.c:3066):
        // a reload that drops the delay ends the loop.
        guc_tables::vars::recovery_min_apply_delay.write(0);
    }
}

// xlogrecovery.c:3087: each pass of recoveryApplyDelay's wait loop logs
// elog(DEBUG2, "recovery apply delay %ld milliseconds", msecs); the pre-fix
// port waited silently. A commit record with xact_time == now and
// recovery_min_apply_delay = 1000 gives one 1000 ms pass. Audit
// a186-candidate-fp-transam-xlogrecovery-p2-b7c3b1b4a1d2a14c9511-1.
#[test]
fn recovery_apply_delay_logs_debug2_like_c() {
    let _g = datadir_lock();
    let dir = boot_fixture("apply_delay");
    install_startup_process_seams();
    WALRCV_UP.store(false, Relaxed);
    STANDBY_MODE.store(false, Relaxed);
    IN_ARCHIVE_RECOVERY.store(false, Relaxed);
    let loc: XLogRecPtr = SEG as u64 + SizeOfXLogLongPHD as u64;
    // xl_xact_commit without XLOG_XACT_HAS_INFO: xact_time only.
    let commit = record_bytes(loc, xact::RM_XACT_ID, xact::XLOG_XACT_COMMIT, &0i64.to_ne_bytes());
    write_segment_with_record(&dir, loc, &commit);
    RECOVERY_TARGET_TLI.store(1, Relaxed);

    let cx = mcx::MemoryContext::new("apply delay witness");
    let mut reader = XLogReaderState::allocate(cx.mcx(), SEG).unwrap();
    reader.system_identifier = SYS_ID;
    reader.XLogReaderSetDecodeBuffer(guc_tables::vars::wal_decode_buffer_size.read() as usize);
    reader.XLogBeginRead(loc);
    let mut src = PageSource::new();
    src.replay_tli = 1;
    assert_eq!(reader.XLogReadRecord(&mut src).unwrap(), Some(loc));
    src.close_read_file();
    assert_eq!(reader.XLogRecGetRmid(), xact::RM_XACT_ID);

    guc_tables::vars::recovery_min_apply_delay.write(1000);
    REACHED_CONSISTENCY.store(true, Relaxed);
    ARCHIVE_RECOVERY_REQUESTED.store(true, Relaxed);
    *INTERRUPT_HOOK.lock().unwrap() = Some(apply_delay_interrupt);
    APPLY_DELAY_WAKES.store(0, Relaxed);
    latch::OwnLatch(targets::recovery_wakeup_latch()).unwrap();
    REPORTS.lock().unwrap().clear();
    let prev_min = elog::config::log_min_messages();
    elog::config::set_log_min_messages(types_error::DEBUG2);
    let prev = elog::set_emit_log_hook(Some(capture_report));

    let delayed = targets::recoveryApplyDelay(&reader);

    elog::set_emit_log_hook(prev);
    elog::config::set_log_min_messages(prev_min);
    latch::DisownLatch(targets::recovery_wakeup_latch());
    *INTERRUPT_HOOK.lock().unwrap() = None;
    guc_tables::vars::recovery_min_apply_delay.write(0);
    REACHED_CONSISTENCY.store(false, Relaxed);
    ARCHIVE_RECOVERY_REQUESTED.store(false, Relaxed);
    let reports = REPORTS.lock().unwrap().clone();
    drop(reader);
    let _ = std::fs::remove_dir_all(&dir);

    assert!(delayed.unwrap(), "the commit record was delayed");
    assert!(APPLY_DELAY_WAKES.load(Relaxed) >= 2, "the wait loop ran at least one pass");
    let expected = "recovery apply delay 1000 milliseconds";
    let hit = reports.iter().find(|e| e.message() == expected).unwrap_or_else(|| {
        panic!(
            "no {expected:?} report; got {:?}",
            reports.iter().map(|e| e.message().to_string()).collect::<Vec<_>>()
        )
    });
    assert_eq!(hit.level, types_error::DEBUG2);
}

// check_recovery_target_time (xlogrecovery.c:4980-5006): ParseDateTime /
// DecodeDateTime must yield DTK_DATE — 'infinity', '-infinity' and 'epoch'
// (DTK_LATE / DTK_EARLY / DTK_EPOCH) are rejected like now/today/tomorrow/
// yesterday, and a tm2timestamp overflow carries GUC_check_errdetail
// "Timestamp out of range: \"%s\".". The pre-fix hook rejected only the four
// literal words and accepted whatever timestamptz_in parsed. Audit
// a186-verified-fp-transam-xlogrecovery-p2-bca11a0cf6e3cad4e15f-1.
#[test]
fn check_recovery_target_time_rejects_non_date_tokens_like_c() {
    for token in ["infinity", "-infinity", "epoch", "now", "today", "tomorrow", "yesterday"] {
        let (r, details) = run_check_hook(&guc_tables::hooks::check_recovery_target_time, token);
        assert!(matches!(r, Ok(false)), "{token}: got {r:?}");
        assert!(details.is_empty(), "{token}: unexpected detail {details:?}");
    }
    let (r, details) = run_check_hook(
        &guc_tables::hooks::check_recovery_target_time,
        "294277-01-01 00:00:00+00",
    );
    assert!(matches!(r, Ok(false)), "got {r:?}");
    assert_eq!(
        details,
        vec!["Timestamp out of range: \"294277-01-01 00:00:00+00\".".to_string()]
    );
    let (r, details) =
        run_check_hook(&guc_tables::hooks::check_recovery_target_time, "2024-01-01 00:00:00+00");
    assert!(matches!(r, Ok(true)), "got {r:?}");
    assert!(details.is_empty());
    let (r, _) = run_check_hook(&guc_tables::hooks::check_recovery_target_time, "");
    assert!(matches!(r, Ok(true)));
}

// ---- rm_redo_error_callback (xlogrecovery.c:1943-1946, 2306-2323) ---------
//
// ApplyWalRecord pushes an error context frame whose callback appends
// "WAL redo at %X/%X for <rmgr>/<identify>: <desc>[; blkref #n: ...]" to
// every report raised while a record is being applied (xlog_outdesc +
// xlog_block_info over the rmgr table), and pops it right after
// verifyBackupPageConsistency (xlogrecovery.c:2030-2031). Audit
// a186-candidate-fp-transam-xlogrecovery-p1-8443be11c090e8e8a810-1.

// One record carrying a block reference (XLR_BLOCK_ID 0: id, fork_flags,
// data_length, RelFileLocator, BlockNumber — no data, no image) followed by
// a short main-data chunk (XLR_BLOCK_ID_DATA_SHORT), CRC'd.
fn record_bytes_with_blkref(
    loc: XLogRecPtr,
    rmid: u8,
    info: u8,
    rlocator: (u32, u32, u32),
    forknum: u8,
    blkno: u32,
    main_data: &[u8],
) -> Vec<u8> {
    assert!(main_data.len() < 256);
    let tot_len = SizeOfXLogRecord + 20 + 2 + main_data.len();
    let mut rec = vec![0u8; tot_len];
    rec[0..4].copy_from_slice(&(tot_len as u32).to_ne_bytes());
    rec[8..16].copy_from_slice(&(loc - 0x28).to_ne_bytes());
    rec[16] = info;
    rec[17] = rmid;
    let mut p = SizeOfXLogRecord;
    rec[p] = 0; // block id
    rec[p + 1] = forknum; // fork_flags: fork number, no BKPBLOCK_* flags
    rec[p + 2..p + 4].copy_from_slice(&0u16.to_ne_bytes()); // data_length
    p += 4;
    rec[p..p + 4].copy_from_slice(&rlocator.0.to_ne_bytes());
    rec[p + 4..p + 8].copy_from_slice(&rlocator.1.to_ne_bytes());
    rec[p + 8..p + 12].copy_from_slice(&rlocator.2.to_ne_bytes());
    rec[p + 12..p + 16].copy_from_slice(&blkno.to_ne_bytes());
    p += 16;
    rec[p] = 255; // XLR_BLOCK_ID_DATA_SHORT
    rec[p + 1] = main_data.len() as u8;
    rec[p + 2..p + 2 + main_data.len()].copy_from_slice(main_data);
    let crc = crc32c::fin_crc32c(crc32c::pg_comp_crc32c(
        crc32c::pg_comp_crc32c(crc32c::CRC32C_INIT, &rec[SizeOfXLogRecord..]),
        &rec[..20],
    ));
    rec[20..24].copy_from_slice(&crc.to_ne_bytes());
    rec
}

// AdvanceNextFullTransactionIdPastXid reads TransamVariables; boot the
// heap image once per test binary (VarsupShmemInit refuses a second call).
// ApplyWalRecord's AllowCascadeReplication() reads max_wal_senders, whose
// owning unit is not part of this binary: a zero stand-in (no walsenders).
fn install_varsup_once() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(varsup::VarsupShmemInit);
    guc_tables::vars::max_wal_senders
        .install_if_absent(guc_tables::GucVarAccessors { get: || 0, set: |_| {} });
}

// ApplyWalRecord advances the shared replay pointers; the boot-path units
// assert them zero (GetXLogReplayRecPtr), so a witness that applied a record
// puts them back.
fn reset_replay_pointers() {
    REPLAY_END_REC_PTR.store(0, Relaxed);
    REPLAY_END_TLI.store(0, Relaxed);
    LAST_REPLAYED_READ_REC_PTR.store(0, Relaxed);
    LAST_REPLAYED_END_REC_PTR.store(0, Relaxed);
    LAST_REPLAYED_TLI.store(0, Relaxed);
}

// Reads the single record at `loc` from the fixture's segment through the
// real reader and wraps it in a Recovery for apply_wal_record.
fn recovery_at_record(loc: XLogRecPtr) -> Recovery {
    let context: &'static mcx::MemoryContext =
        Box::leak(Box::new(mcx::MemoryContext::new("rm_redo errcontext witness")));
    let mut reader = XLogReaderState::allocate(context.mcx(), SEG).unwrap();
    reader.system_identifier = SYS_ID;
    reader.XLogReaderSetDecodeBuffer(guc_tables::vars::wal_decode_buffer_size.read() as usize);
    reader.XLogBeginRead(loc);
    let mut src = PageSource::new();
    src.replay_tli = 1;
    assert_eq!(reader.XLogReadRecord(&mut src).unwrap(), Some(loc));
    src.close_read_file();
    let prefetcher = xlogprefetcher::XLogPrefetcher::XLogPrefetcherAllocate(context.mcx());
    Recovery {
        context,
        reader,
        prefetcher,
        src,
        check_point_loc: loc,
        check_point_tli: 1,
        aborted_rec_ptr: InvalidXLogRecPtr,
        missing_contrec_ptr: InvalidXLogRecPtr,
        oldest_active_xid: types_core::InvalidTransactionId,
        redo_error_arg: Rc::new(RefCell::new(RmRedoErrorArg::default())),
    }
}

// An error thrown by rm_redo (btree_redo's unknown op code, a PANIC-level
// report) carries C's CONTEXT line: identity from rm_identify (UNKNOWN with
// the opcode in %X), rm_desc (empty for an unknown btree opcode) and the
// block reference on a non-MAIN fork ("fork %u").
#[test]
fn apply_wal_record_error_carries_wal_redo_context_like_c() {
    let _g = datadir_lock();
    let dir = boot_fixture("rm_redo_errcontext_err");
    install_startup_process_seams();
    install_varsup_once();
    WALRCV_UP.store(false, Relaxed);
    STANDBY_MODE.store(false, Relaxed);
    IN_ARCHIVE_RECOVERY.store(false, Relaxed);
    RECOVERY_TARGET_TLI.store(1, Relaxed);
    let loc: XLogRecPtr = SEG as u64 + SizeOfXLogLongPHD as u64;
    let bad = record_bytes_with_blkref(
        loc,
        rmgr::RM_BTREE_ID as u8,
        0xF0,
        (1663, 5, 61000),
        types_core::ForkNumber::FSM_FORKNUM as i32 as u8,
        7,
        &[0u8; 4],
    );
    write_segment_with_record(&dir, loc, &bad);
    let mut rec = recovery_at_record(loc);
    assert_eq!(rec.reader.XLogRecGetRmid(), rmgr::RM_BTREE_ID as u8);
    let mut replay_tli: TimeLineID = 1;

    let err = apply_wal_record(&mut rec, &mut replay_tli)
        .expect_err("an unknown btree opcode must not replay silently");

    drop(rec);
    reset_replay_pointers();
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(err.message(), "btree_redo: unknown op code 240");
    assert_eq!(
        err.context(),
        Some(
            "WAL redo at 0/1000028 for Btree/UNKNOWN (F0): ; blkref #0: rel 1663/5/61000, fork 1, blk 7"
        ),
        "rm_redo_error_callback CONTEXT line (xlogrecovery.c:2306-2323)"
    );
}

// A non-ERROR report emitted inside the frame (xlogrecovery_redo's DEBUG1
// "end of backup record reached", xlogrecovery.c:1944-2028 scope) carries the
// CONTEXT line at emit time; a report after ApplyWalRecord returns does not
// (the frame was popped, xlogrecovery.c:2030-2031).
#[test]
fn apply_wal_record_reports_carry_wal_redo_context_like_c() {
    let _g = datadir_lock();
    let dir = boot_fixture("rm_redo_errcontext_emit");
    install_startup_process_seams();
    install_varsup_once();
    WALRCV_UP.store(false, Relaxed);
    STANDBY_MODE.store(false, Relaxed);
    IN_ARCHIVE_RECOVERY.store(false, Relaxed);
    RECOVERY_TARGET_TLI.store(1, Relaxed);
    let loc: XLogRecPtr = SEG as u64 + SizeOfXLogLongPHD as u64;
    let backup_end = record_bytes(
        loc,
        transam_xlog::RM_XLOG_ID,
        transam_xlog::XLOG_BACKUP_END,
        &loc.to_ne_bytes(),
    );
    write_segment_with_record(&dir, loc, &backup_end);
    let mut rec = recovery_at_record(loc);
    rec.src.backup_start_point = loc;
    let mut replay_tli: TimeLineID = 1;

    REPORTS.lock().unwrap().clear();
    let prev_min = elog::config::log_min_messages();
    elog::config::set_log_min_messages(types_error::DEBUG1);
    let prev = elog::set_emit_log_hook(Some(capture_report));

    let applied = apply_wal_record(&mut rec, &mut replay_tli);
    let _ = elog(LOG, "probe after ApplyWalRecord".to_string());

    elog::set_emit_log_hook(prev);
    elog::config::set_log_min_messages(prev_min);
    let reports = REPORTS.lock().unwrap().clone();
    drop(rec);
    reset_replay_pointers();
    let _ = std::fs::remove_dir_all(&dir);

    applied.unwrap();
    let find = |msg: &str| {
        reports.iter().find(|e| e.message() == msg).cloned().unwrap_or_else(|| {
            panic!(
                "no {msg:?} report; got {:?}",
                reports.iter().map(|e| e.message().to_string()).collect::<Vec<_>>()
            )
        })
    };
    let inside = find("end of backup record reached");
    assert_eq!(inside.level, types_error::DEBUG1);
    assert_eq!(
        inside.context(),
        Some("WAL redo at 0/1000028 for XLOG/BACKUP_END: 0/1000028"),
        "reports inside ApplyWalRecord get rm_redo_error_callback's line"
    );
    let outside = find("probe after ApplyWalRecord");
    assert_eq!(outside.context(), None, "the frame is popped with ApplyWalRecord");
}

// xlog_outdesc (xlogrecovery.c:2327-2344) + xlog_block_info (2366-2398) over
// a hand-built decoded record: rm_name/rm_identify ("UNKNOWN (%X): " for an
// unknown opcode, "%s: " otherwise), rm_desc, then every in-use block
// reference in id order (unused ids skipped), MAIN fork without "fork %u",
// other forks with it, " FPW" when the block carries an image.
#[test]
fn xlog_outdesc_and_block_info_render_like_c() {
    let cx = mcx::MemoryContext::new("xlog_outdesc witness");

    let mut unknown = xlogreader_seams::DecodedXLogRecord::default();
    unknown.xl_rmid = rmgr::RM_BTREE_ID as u8;
    unknown.xl_info = 0xF0;
    unknown.max_block_id = 2;
    unknown.blocks[0] = xlogreader_seams::DecodedBkpBlock {
        in_use: true,
        rlocator: types_storage::RelFileLocator::new(1, 2, 3),
        forknum: types_core::ForkNumber::MAIN_FORKNUM,
        blkno: 4,
        has_image: true,
        ..xlogreader_seams::DecodedBkpBlock::EMPTY
    };
    unknown.blocks[2] = xlogreader_seams::DecodedBkpBlock {
        in_use: true,
        rlocator: types_storage::RelFileLocator::new(1, 2, 3),
        forknum: types_core::ForkNumber::VISIBILITYMAP_FORKNUM,
        blkno: 9,
        ..xlogreader_seams::DecodedBkpBlock::EMPTY
    };
    let view = ReaderView { ReadRecPtr: 0x1000028, record: Some(unknown), ..Default::default() };
    let mut buf = StringInfo::new_in(cx.mcx()).unwrap();
    xlog_outdesc(&mut buf, &view).unwrap();
    xlog_block_info(&mut buf, &view).unwrap();
    assert_eq!(
        String::from_utf8_lossy(buf.as_bytes()),
        "Btree/UNKNOWN (F0): ; blkref #0: rel 1/2/3, blk 4 FPW; blkref #2: rel 1/2/3, fork 2, blk 9"
    );

    let startpoint: [u8; 8] = 0x1000028u64.to_ne_bytes();
    let mut known = xlogreader_seams::DecodedXLogRecord::default();
    known.xl_rmid = transam_xlog::RM_XLOG_ID;
    known.xl_info = transam_xlog::XLOG_BACKUP_END;
    known.main_data = startpoint.as_ptr();
    known.main_data_len = startpoint.len() as u32;
    let view = ReaderView { ReadRecPtr: 0x1000028, record: Some(known), ..Default::default() };
    let mut buf = StringInfo::new_in(cx.mcx()).unwrap();
    xlog_outdesc(&mut buf, &view).unwrap();
    xlog_block_info(&mut buf, &view).unwrap();
    assert_eq!(String::from_utf8_lossy(buf.as_bytes()), "XLOG/BACKUP_END: 0/1000028");
}
