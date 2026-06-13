//! Unit tests for the grounded arithmetic / codec core of the WAL engine: the
//! byte-pos<->LSN conversions, the segment / file-name codec, the
//! checkpoint-distance arithmetic, the `WalConfig` predicates, and the
//! `CheckPoint` C-ABI image. (The shmem-write / WAL driver is the deferred
//! `xlog-driver` core and loud-panics by design, so it is not exercised here.)

use super::*;
use types_control::{CheckPoint, ControlFileData, FullTransactionId};

const SEG: i32 = DEFAULT_XLOG_SEG_SIZE; // 16 MiB

#[test]
fn wal_segment_math_matches_xlog_internal_macros() {
    assert!(IsValidWalSegSize(SEG));
    assert!(!IsValidWalSegSize(3 * 1024 * 1024));
    assert!(!IsValidWalSegSize(WAL_SEG_MIN_SIZE / 2));
    assert!(!IsValidWalSegSize(0));
    assert!(!IsValidWalSegSize(-SEG));
    assert!(IsValidWalSegSize(WAL_SEG_MIN_SIZE));
    assert!(IsValidWalSegSize(WAL_SEG_MAX_SIZE));

    assert_eq!(XLogSegmentsPerXLogId(SEG), 256);
    assert_eq!(XLByteToSeg(0x0100_0000, SEG), 1);
}

#[test]
fn check_wal_segment_size_hook() {
    assert!(check_wal_segment_size(SEG).is_ok());
    assert!(check_wal_segment_size(3 * 1024 * 1024).is_err());
}

#[test]
fn xrecoff_is_valid() {
    assert!(!XRecOffIsValid(0));
    assert!(!XRecOffIsValid((SIZE_OF_XLOG_SHORT_PHD - 1) as u64));
    assert!(XRecOffIsValid(SIZE_OF_XLOG_SHORT_PHD as u64));
    assert!(XRecOffIsValid((SIZE_OF_XLOG_SHORT_PHD + 1) as u64));
}

#[test]
fn usable_bytes_helpers_match_c() {
    assert_eq!(UsableBytesInPage(), 8168);
    let pages_per_seg = (SEG as u64) / (XLOG_BLCKSZ as u64);
    assert_eq!(pages_per_seg, 2048);
    assert_eq!(UsableBytesInSegment(SEG), 2048 * 8168 - 16);
    assert_eq!(UsableBytesInSegment(SEG), 16_728_048);
}

#[test]
fn byte_pos_to_recptr_first_page_offsets_by_long_header() {
    assert_eq!(XLogBytePosToRecPtr(0, SEG), SIZE_OF_XLOG_LONG_PHD as XLogRecPtr);
    assert_eq!(
        XLogBytePosToRecPtr(100, SEG),
        (SIZE_OF_XLOG_LONG_PHD + 100) as XLogRecPtr
    );
}

#[test]
fn byte_pos_to_end_recptr_zero_maps_to_page_start() {
    assert_eq!(XLogBytePosToEndRecPtr(0, SEG), 0);
    assert_eq!(
        XLogBytePosToEndRecPtr(100, SEG),
        (SIZE_OF_XLOG_LONG_PHD + 100) as XLogRecPtr
    );
}

#[test]
fn byte_pos_recptr_round_trips() {
    let usable_page = UsableBytesInPage();
    let usable_seg = UsableBytesInSegment(SEG);

    let cases = [
        0u64,
        1,
        50,
        usable_page - 1,
        usable_page,
        usable_page + 1,
        2 * usable_page,
        usable_seg - 1,
        usable_seg,
        usable_seg + 1,
        usable_seg + usable_page + 123,
        3 * usable_seg + 7 * usable_page + 4567,
        100 * usable_seg + 9_999_999,
    ];

    for &bp in &cases {
        let ptr = XLogBytePosToRecPtr(bp, SEG);
        assert_eq!(XLogRecPtrToBytePos(ptr, SEG), bp, "round-trip failed for bytepos {bp}");
    }
}

#[test]
fn byte_pos_conversion_handles_multi_page_records() {
    let usable_page = UsableBytesInPage();
    let first_overflow = (XLOG_BLCKSZ - SIZE_OF_XLOG_LONG_PHD) as u64;
    let ptr = XLogBytePosToRecPtr(first_overflow, SEG);
    assert_eq!(ptr, (XLOG_BLCKSZ + SIZE_OF_XLOG_SHORT_PHD) as XLogRecPtr);
    assert_eq!(XLogRecPtrToBytePos(ptr, SEG), first_overflow);
    assert_eq!(usable_page, 8168);
}

#[test]
fn wal_file_names_round_trip() {
    let name = XLogFileName(1, 0x123, SEG);
    assert_eq!(name, "000000010000000100000023");
    assert!(IsXLogFileName(&name));
    assert!(!IsXLogFileName("000000010000000100000023.partial"));
    assert!(IsPartialXLogFileName("000000010000000100000023.partial"));
    assert_eq!(XLogFromFileName(&name, SEG).unwrap(), (1, 0x123));
    assert_eq!(XLogFilePath(1, 0x123, SEG), "pg_wal/000000010000000100000023");
    assert_eq!(XLogFileNameById(2, 1, 0x23), "000000020000000100000023");
    assert!(XLogFromFileName("not-a-wal-name-zz", SEG).is_err());
}

#[test]
fn wal_sidecar_file_names_match_postgres_format() {
    assert_eq!(TLHistoryFileName(7), "00000007.history");
    assert!(IsTLHistoryFileName("00000007.history"));
    assert!(!IsTLHistoryFileName("00000007.HISTORY"));
    assert_eq!(TLHistoryFilePath(7), "pg_wal/00000007.history");
    assert_eq!(
        StatusFilePath("000000010000000100000023", ".ready"),
        "pg_wal/archive_status/000000010000000100000023.ready"
    );

    let backup = BackupHistoryFileName(1, 0x123, 0x0100_4567, SEG);
    assert_eq!(backup, "000000010000000100000023.00004567.backup");
    assert!(IsBackupHistoryFileName(&backup));
    assert_eq!(
        BackupHistoryFilePath(1, 0x123, 0x0100_4567, SEG),
        "pg_wal/000000010000000100000023.00004567.backup"
    );
}

#[test]
fn calculate_checkpoint_segments_matches_c() {
    assert_eq!(CalculateCheckpointSegments(1024, SEG, 0.9), 33);
    assert_eq!(CalculateCheckpointSegments(1024, SEG, 0.5), 42);
    assert_eq!(CalculateCheckpointSegments(1, SEG, 0.9), 1);
    assert_eq!(CalculateCheckpointSegments(0, SEG, 0.9), 1);
}

#[test]
fn config_macros_match_xlog_h() {
    let mut config = WalConfig::default();
    assert!(!config.XLogArchivingActive());
    assert!(!config.XLogArchivingAlways());
    assert!(config.XLogIsNeeded());
    assert!(config.XLogStandbyInfoActive());
    assert!(!config.XLogLogicalInfoActive());
    assert!(!config.XLogHintBitIsNeeded(false));
    assert!(config.XLogHintBitIsNeeded(true));

    config.XLogArchiveMode = ArchiveMode::Always;
    config.wal_level = WalLevel::Logical;
    config.wal_log_hints = true;
    assert!(config.XLogArchivingActive());
    assert!(config.XLogArchivingAlways());
    assert!(config.XLogLogicalInfoActive());
    assert!(config.XLogHintBitIsNeeded(false));

    config.wal_level = WalLevel::Minimal;
    assert!(!config.XLogIsNeeded());
    assert!(!config.XLogStandbyInfoActive());
    assert!(!config.XLogLogicalInfoActive());
}

#[test]
fn invalid_recptr_predicate() {
    assert!(XLogRecPtrIsInvalid(0));
    assert!(!XLogRecPtrIsInvalid(1));
}

#[test]
fn checkpoint_record_image_is_c_abi_88_bytes() {
    let mut cp = CheckPoint::default();
    cp.redo = 0x0102_0304_0506_0708;
    cp.ThisTimeLineID = 0x1112_1314;
    cp.PrevTimeLineID = 0x2122_2324;
    cp.fullPageWrites = true;
    cp.wal_level = 0x3132_3334;
    cp.nextXid = FullTransactionId {
        value: 0x4142_4344_4546_4748,
    };
    cp.nextOid = 0x5152_5354;
    cp.nextMulti = 0x6162_6364;
    cp.nextMultiOffset = 0x7172_7374;
    cp.oldestXid = 0x0A0B_0C0D;
    cp.oldestXidDB = 0x1A1B_1C1D;
    cp.oldestMulti = 0x2A2B_2C2D;
    cp.oldestMultiDB = 0x3A3B_3C3D;
    cp.time = 0x4A4B_4C4D_4E4F_5051;
    cp.oldestCommitTsXid = 0x5A5B_5C5D;
    cp.newestCommitTsXid = 0x6A6B_6C6D;
    cp.oldestActiveXid = 0x7A7B_7C7D;

    let b = checkpoint::checkpoint_to_bytes(&cp);
    assert_eq!(b.len(), checkpoint::SIZE_OF_CHECK_POINT);
    assert_eq!(b.len(), 88);

    assert_eq!(&b[0..8], &cp.redo.to_ne_bytes());
    assert_eq!(&b[8..12], &cp.ThisTimeLineID.to_ne_bytes());
    assert_eq!(&b[12..16], &cp.PrevTimeLineID.to_ne_bytes());
    assert_eq!(b[16], 1);
    assert_eq!(&b[17..20], &[0u8; 3]);
    assert_eq!(&b[20..24], &cp.wal_level.to_ne_bytes());
    assert_eq!(&b[24..32], &cp.nextXid.value.to_ne_bytes());
    assert_eq!(&b[32..36], &cp.nextOid.to_ne_bytes());
    assert_eq!(&b[36..40], &cp.nextMulti.to_ne_bytes());
    assert_eq!(&b[40..44], &cp.nextMultiOffset.to_ne_bytes());
    assert_eq!(&b[44..48], &cp.oldestXid.to_ne_bytes());
    assert_eq!(&b[48..52], &cp.oldestXidDB.to_ne_bytes());
    assert_eq!(&b[52..56], &cp.oldestMulti.to_ne_bytes());
    assert_eq!(&b[56..60], &cp.oldestMultiDB.to_ne_bytes());
    assert_eq!(&b[60..64], &[0u8; 4]);
    assert_eq!(&b[64..72], &cp.time.to_ne_bytes());
    assert_eq!(&b[72..76], &cp.oldestCommitTsXid.to_ne_bytes());
    assert_eq!(&b[76..80], &cp.newestCommitTsXid.to_ne_bytes());
    assert_eq!(&b[80..84], &cp.oldestActiveXid.to_ne_bytes());
    assert_eq!(&b[84..88], &[0u8; 4]);
}

#[test]
fn checkpoint_state_seeds_redo_from_control_file() {
    let mut cf = ControlFileData::default();
    cf.checkPointCopy.redo = 0xDEAD_BEEF;
    let st = checkpoint::CheckpointState::new(cf, WalConfig::default());
    assert_eq!(st.RedoRecPtr, 0xDEAD_BEEF);
    assert_eq!(st.LocalXLogInsertAllowed, -1);
}

#[test]
fn xlog_checkpoint_needed_matches_c() {
    // RedoRecPtr at start of segment 10; CheckPointSegments = 5.
    let redo = 10 * (SEG as u64);
    // old_segno = 10; trigger when new_segno >= 10 + (5-1) = 14.
    assert!(!XLogCheckpointNeeded(13, redo, 5, SEG));
    assert!(XLogCheckpointNeeded(14, redo, 5, SEG));
    assert!(XLogCheckpointNeeded(99, redo, 5, SEG));
    // CheckPointSegments == 1 => trigger at old_segno itself.
    assert!(XLogCheckpointNeeded(10, redo, 1, SEG));
}

#[test]
fn xlog_choose_num_buffers_clamps() {
    // NBuffers/32, clamped to [8, SEG/XLOG_BLCKSZ]=[8,2048].
    assert_eq!(XLOGChooseNumBuffers(16, SEG), 8); // 0 -> 8
    assert_eq!(XLOGChooseNumBuffers(1024, SEG), 32); // 32
    assert_eq!(XLOGChooseNumBuffers(1_000_000, SEG), 2048); // capped at SEG/8192
}

#[test]
fn check_wal_buffers_autotune_and_floor() {
    // -1 with XLOGbuffers still -1 stays -1.
    assert_eq!(check_wal_buffers(-1, -1, 1024, SEG), -1);
    // -1 with XLOGbuffers already set substitutes auto-tune.
    assert_eq!(check_wal_buffers(-1, 64, 1024, SEG), 32);
    // manual below 4 clamps to 4.
    assert_eq!(check_wal_buffers(1, 64, 1024, SEG), 4);
    assert_eq!(check_wal_buffers(100, 64, 1024, SEG), 100);
}

#[test]
fn get_sync_bit_maps_methods() {
    const O_SYNC: i32 = 0x1000;
    const O_DSYNC: i32 = 0x2000;
    const DIRECT: i32 = 0x80000000u32 as i32;
    // fsync disabled: only the o_direct flag.
    assert_eq!(get_sync_bit(WalSyncMethod::OpenDsync, DIRECT, false, O_SYNC, O_DSYNC).unwrap(), DIRECT);
    // fsync/fdatasync/writethrough: just o_direct_flag.
    assert_eq!(get_sync_bit(WalSyncMethod::Fsync, 0, true, O_SYNC, O_DSYNC).unwrap(), 0);
    assert_eq!(get_sync_bit(WalSyncMethod::Fdatasync, DIRECT, true, O_SYNC, O_DSYNC).unwrap(), DIRECT);
    assert_eq!(get_sync_bit(WalSyncMethod::FsyncWritethrough, 0, true, O_SYNC, O_DSYNC).unwrap(), 0);
    // open / open_dsync OR in the sync bit.
    assert_eq!(get_sync_bit(WalSyncMethod::Open, 0, true, O_SYNC, O_DSYNC).unwrap(), O_SYNC);
    assert_eq!(get_sync_bit(WalSyncMethod::OpenDsync, DIRECT, true, O_SYNC, O_DSYNC).unwrap(), O_DSYNC | DIRECT);
}
