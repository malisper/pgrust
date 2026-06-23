//! Tests for the `XLogCtl` shmem layout + control-file codec.

use super::*;
use core::mem::size_of;

#[test]
fn wal_insert_lock_padded_is_one_cache_line() {
    assert_eq!(size_of::<WALInsertLockPadded>(), PG_CACHE_LINE_SIZE);
}

#[test]
fn control_file_image_size_matches_layout() {
    let cf = ControlFileData::default();
    let bytes = control_file_to_bytes(&cf);
    assert_eq!(bytes.len(), SIZE_OF_CONTROL_FILE_DATA);
    // The CRC field starts at offset_of_crc().
    assert_eq!(offset_of_crc(), 292);
}

#[test]
fn control_file_codec_roundtrips() {
    let mut cf = ControlFileData::default();
    cf.system_identifier = 0x0123_4567_89AB_CDEF;
    cf.pg_control_version = PG_CONTROL_VERSION;
    cf.catalog_version_no = CATALOG_VERSION_NO;
    cf.state = DBState::InProduction;
    cf.time = 1_700_000_000;
    cf.checkPoint = 0xDEAD_BEEF;
    cf.checkPointCopy.redo = 0xCAFE;
    cf.checkPointCopy.ThisTimeLineID = 7;
    cf.checkPointCopy.nextXid = FullTransactionId { value: 4242 };
    cf.minRecoveryPoint = 0x9999;
    cf.minRecoveryPointTLI = 3;
    cf.wal_level = 2;
    cf.MaxConnections = 100;
    cf.maxAlign = 8;
    cf.floatFormat = FLOATFORMAT_VALUE;
    cf.blcksz = 8192;
    cf.xlog_seg_size = 16 * 1024 * 1024;
    cf.data_checksum_version = 1;
    cf.default_char_signedness = true;
    for (i, byte) in cf.mock_authentication_nonce.iter_mut().enumerate() {
        *byte = i as u8;
    }

    let bytes = control_file_to_bytes(&cf);
    let back = control_file_from_bytes(&bytes);

    assert_eq!(back.system_identifier, cf.system_identifier);
    assert_eq!(back.pg_control_version, cf.pg_control_version);
    assert_eq!(back.catalog_version_no, cf.catalog_version_no);
    assert_eq!(back.state as u32, cf.state as u32);
    assert_eq!(back.time, cf.time);
    assert_eq!(back.checkPoint, cf.checkPoint);
    assert_eq!(back.checkPointCopy, cf.checkPointCopy);
    assert_eq!(back.minRecoveryPoint, cf.minRecoveryPoint);
    assert_eq!(back.minRecoveryPointTLI, cf.minRecoveryPointTLI);
    assert_eq!(back.wal_level, cf.wal_level);
    assert_eq!(back.MaxConnections, cf.MaxConnections);
    assert_eq!(back.floatFormat, cf.floatFormat);
    assert_eq!(back.blcksz, cf.blcksz);
    assert_eq!(back.xlog_seg_size, cf.xlog_seg_size);
    assert_eq!(back.data_checksum_version, cf.data_checksum_version);
    assert_eq!(back.default_char_signedness, cf.default_char_signedness);
    assert_eq!(back.mock_authentication_nonce, cf.mock_authentication_nonce);
    // `control_file_to_bytes` computes + writes the CRC, so the decoded `crc`
    // is the genuine checksum over the body (the source `cf.crc` was 0).
    let expected_crc = control_file_crc(&bytes[..offset_of_crc()]);
    assert_eq!(back.crc, expected_crc);
    assert!(EQ_CRC32C(back.crc, expected_crc));
}

#[test]
fn control_file_crc_detects_corruption() {
    let cf = ControlFileData::default();
    let mut bytes = control_file_to_bytes(&cf);
    // Flip a byte in the body; the recomputed CRC must no longer match.
    bytes[0] ^= 0xFF;
    let crc = control_file_crc(&bytes[..offset_of_crc()]);
    let stored = get_u32(&bytes, 292);
    assert!(!EQ_CRC32C(crc, stored));
}
