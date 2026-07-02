use std::mem::{offset_of, size_of};

use crate::control_file::*;
use crate::*;

// Layout ground truth from a C compile of pg_control.h (REL_18_3).
#[test]
fn control_file_layout_matches_c() {
    assert_eq!(size_of::<CheckPoint>(), 88);
    assert_eq!(size_of::<ControlFileData>(), 296);
    assert_eq!(offset_of!(ControlFileData, crc), 292);
    assert_eq!(offset_of!(ControlFileData, state), 16);
    assert_eq!(offset_of!(ControlFileData, time), 24);
    assert_eq!(offset_of!(ControlFileData, checkPointCopy), 40);
    assert_eq!(offset_of!(ControlFileData, unloggedLSN), 128);
    assert_eq!(offset_of!(ControlFileData, mock_authentication_nonce), 257);
    assert_eq!(offset_of!(CheckPoint, nextXid), 24);
    assert_eq!(offset_of!(CheckPoint, time), 64);
    assert_eq!(offset_of!(CheckPoint, oldestActiveXid), 80);
}

#[test]
fn checkpoint_byte_roundtrip() {
    let mut ckpt = CheckPoint::ZEROED;
    ckpt.redo = 0x0123_4567_89AB_CDEF;
    ckpt.ThisTimeLineID = 7;
    ckpt.PrevTimeLineID = 6;
    ckpt.fullPageWrites = true;
    ckpt.wal_level = WAL_LEVEL_REPLICA;
    ckpt.nextXid = types_core::FullTransactionId::from_epoch_and_xid(2, 1234);
    ckpt.nextOid = 24576;
    ckpt.oldestXid = 3;
    ckpt.time = 1_700_000_000;
    ckpt.oldestActiveXid = 99;
    let bytes = ckpt.as_bytes().to_vec();
    assert_eq!(bytes.len(), 88);
    assert_eq!(CheckPoint::from_bytes(&bytes), ckpt);
}

fn with_seg(size: i32, f: impl FnOnce()) {
    set_wal_segment_size(size);
    f();
}

#[test]
fn bytepos_recptr_roundtrip() {
    with_seg(16 * 1024 * 1024, || {
        for bytepos in [
            0u64,
            1,
            (XLOG_BLCKSZ - SizeOfXLogLongPHD) as u64 - 1,
            (XLOG_BLCKSZ - SizeOfXLogLongPHD) as u64,
            (XLOG_BLCKSZ - SizeOfXLogLongPHD) as u64 + 1,
            UsableBytesInPage * 3 + 17,
            UsableBytesInSegment() - 1,
            UsableBytesInSegment(),
            UsableBytesInSegment() + 12345,
            UsableBytesInSegment() * 5 + 7,
        ] {
            let ptr = XLogBytePosToRecPtr(bytepos);
            assert_eq!(XLogRecPtrToBytePos(ptr), bytepos, "bytepos {bytepos}");
        }
    });
}

#[test]
fn bytepos_end_recptr_page_boundary() {
    with_seg(16 * 1024 * 1024, || {
        // End position at exactly a page boundary points before the header.
        let one_page = (XLOG_BLCKSZ - SizeOfXLogLongPHD) as u64;
        let end = XLogBytePosToEndRecPtr(one_page);
        assert_eq!(end % XLOG_BLCKSZ as u64, 0);
        let start = XLogBytePosToRecPtr(one_page);
        assert_eq!(start % XLOG_BLCKSZ as u64, SizeOfXLogShortPHD as u64);
        assert_eq!(XLogBytePosToEndRecPtr(0), 0);
        assert_eq!(XLogBytePosToRecPtr(0), SizeOfXLogLongPHD as u64);
    });
}

#[test]
fn segment_arithmetic() {
    let seg = 16 * 1024 * 1024;
    assert_eq!(XLogSegmentsPerXLogId(seg), 256);
    assert_eq!(XLByteToSeg(seg as u64 * 3 + 5, seg), 3);
    assert_eq!(XLByteToPrevSeg(seg as u64 * 3, seg), 2);
    assert!(XLByteInPrevSeg(seg as u64 * 3, 2, seg));
    assert_eq!(XLogSegmentOffset(seg as u64 + 42, seg), 42);
    assert_eq!(XLogFileName(1, 1, seg), "000000010000000000000001");
    assert_eq!(XLogFileName(1, 256, seg), "000000010000000100000000");
    assert_eq!(XLogFilePath(1, 1, seg), "pg_wal/000000010000000000000001");
    assert!(IsValidWalSegSize(seg));
    assert!(!IsValidWalSegSize(seg - 1));
    assert!(!IsValidWalSegSize(512 * 1024));
}

#[test]
fn insert_freespace_and_align() {
    assert_eq!(INSERT_FREESPACE(0), 0);
    assert_eq!(INSERT_FREESPACE(1), XLOG_BLCKSZ - 1);
    assert_eq!(INSERT_FREESPACE(XLOG_BLCKSZ as u64), 0);
    assert_eq!(MAXALIGN(1), 8);
    assert_eq!(MAXALIGN(8), 8);
    assert_eq!(MAXALIGN64(9), 16);
}

#[test]
fn control_file_crc_detects_corruption() {
    let mut cf = ControlFileData::ZEROED;
    cf.pg_control_version = PG_CONTROL_VERSION;
    cf.system_identifier = 0xDEADBEEF;
    // SAFETY: POD byte view.
    let bytes = unsafe {
        std::slice::from_raw_parts(
            &cf as *const ControlFileData as *const u8,
            offset_of!(ControlFileData, crc),
        )
    };
    let crc = crc32c::fin_crc32c(crc32c::pg_comp_crc32c(crc32c::CRC32C_INIT, bytes));
    cf.crc = crc;
    let mut other = cf;
    other.system_identifier ^= 1;
    // SAFETY: POD byte view.
    let other_bytes = unsafe {
        std::slice::from_raw_parts(
            &other as *const ControlFileData as *const u8,
            offset_of!(ControlFileData, crc),
        )
    };
    let other_crc = crc32c::fin_crc32c(crc32c::pg_comp_crc32c(crc32c::CRC32C_INIT, other_bytes));
    assert_ne!(crc, other_crc);
}

#[test]
fn record_header_offsets() {
    // XLogRecord (xlogrecord.h): tot_len@0 xid@4 prev@8 info@16 rmid@17 crc@20.
    assert_eq!(SizeOfXLogRecord, 24);
}

#[test]
fn xlog_checkpoint_flags_match_c() {
    assert_eq!(CHECKPOINT_IS_SHUTDOWN, 0x0001);
    assert_eq!(CHECKPOINT_END_OF_RECOVERY, 0x0002);
    assert_eq!(CHECKPOINT_IMMEDIATE, 0x0004);
    assert_eq!(CHECKPOINT_FORCE, 0x0008);
    assert_eq!(CHECKPOINT_FLUSH_ALL, 0x0010);
    assert_eq!(CHECKPOINT_WAIT, 0x0020);
    assert_eq!(CHECKPOINT_CAUSE_XLOG, 0x0080);
    assert_eq!(CHECKPOINT_CAUSE_TIME, 0x0100);
}
