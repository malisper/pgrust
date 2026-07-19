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
    init_seams();
    install_timeline_seams();

    let ckpt_loc: XLogRecPtr = SEG as u64 + SizeOfXLogLongPHD as u64;
    let ckpt = make_checkpoint(ckpt_loc);

    // (A missing/corrupt checkpoint ends in ereport(PANIC), which aborts like
    // C — not testable in-process.)
    // Valid shutdown checkpoint, redo == checkpoint location.
    write_control_file(&dir, ckpt_loc, &ckpt);
    write_segment_with_checkpoint(&dir, ckpt_loc, &ckpt);
    transam_xlog::ReadControlFile().unwrap();

    let init = xlogrecovery_seams::init_wal_recovery::call().unwrap();
    assert!(init.was_shutdown);
    assert!(!init.have_backup_label && !init.have_tblspc_map);
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

    xlogrecovery_seams::shutdown_wal_recovery::call().unwrap();
    RECOVERY.with(|c| assert!(c.borrow().is_none()));

    // Promote signal file removal.
    std::fs::write(dir.join(PROMOTE_SIGNAL_FILE), b"").unwrap();
    xlogrecovery_seams::remove_promote_signal_files::call();
    assert!(!dir.join(PROMOTE_SIGNAL_FILE).exists());

    assert!(!xlogrecovery_seams::reached_consistency::call());
    assert!(!xlogrecovery_seams::promote_is_triggered::call());
    assert_eq!(xlogrecovery_seams::get_xlog_replay_rec_ptr::call(), (0, 0));

    let _ = std::fs::remove_dir_all(&dir);
}
