//! Tests for the pure arithmetic / path logic of basebackup_incremental.c.
//! The manifest-ingest and prepare paths drive external owner seams (manifest
//! parser, walsummary, blkreftable, timeline, xlog) that panic until installed,
//! so they are exercised via the workspace integration tests, not here.

use super::*;
use types_catalog::catalog::DEFAULTTABLESPACE_OID;

const BLCKSZ_U: usize = 8192;

#[test]
fn header_size_empty() {
    // Zero blocks: 3 * 4 bytes, not rounded up (no block data).
    assert_eq!(GetIncrementalHeaderSize(0), 12);
}

#[test]
fn header_size_rounds_to_block() {
    // 1 block: 3*4 + 4 = 16 bytes, rounded up to a full BLCKSZ because block
    // data follows.
    assert_eq!(GetIncrementalHeaderSize(1), BLCKSZ_U);
    // Header that is already a multiple of BLCKSZ is not padded further.
    // num = (BLCKSZ - 12) / 4 makes 3*4 + 4*num == BLCKSZ exactly.
    let num = ((BLCKSZ_U - 12) / 4) as u32;
    assert_eq!(GetIncrementalHeaderSize(num), BLCKSZ_U);
}

#[test]
fn file_size_is_header_plus_block_data() {
    let n = 5u32;
    assert_eq!(
        GetIncrementalFileSize(n),
        GetIncrementalHeaderSize(n) + BLCKSZ_U * n as usize
    );
    // Zero blocks: just the unpadded 12-byte header, no block data.
    assert_eq!(GetIncrementalFileSize(0), 12);
}

#[test]
fn compare_block_numbers_three_way() {
    assert_eq!(compare_block_numbers(1, 2), -1);
    assert_eq!(compare_block_numbers(2, 2), 0);
    assert_eq!(compare_block_numbers(3, 2), 1);
    // Unsigned comparison: a high block number is greater, not negative.
    assert_eq!(compare_block_numbers(0xFFFF_FFFF, 0), 1);
}

#[test]
fn incremental_path_no_segment() {
    // base/<db>/<relnum> -> base/<db>/INCREMENTAL.<relnum>
    let p = GetIncrementalFilePath(5, DEFAULTTABLESPACE_OID, 16384, MAIN_FORKNUM, 0);
    assert_eq!(p, "base/5/INCREMENTAL.16384");
}

#[test]
fn incremental_path_with_segment() {
    let p = GetIncrementalFilePath(5, DEFAULTTABLESPACE_OID, 16384, MAIN_FORKNUM, 3);
    assert_eq!(p, "base/5/INCREMENTAL.16384.3");
}

#[test]
fn incremental_path_nonmain_fork() {
    // base/<db>/<relnum>_fsm -> base/<db>/INCREMENTAL.<relnum>_fsm
    let p = GetIncrementalFilePath(5, DEFAULTTABLESPACE_OID, 16384, FSM_FORKNUM, 0);
    assert_eq!(p, "base/5/INCREMENTAL.16384_fsm");
}

#[test]
fn lsn_format() {
    // LSN_FORMAT_ARGS renders high/low 32 bits in hex separated by '/'.
    assert_eq!(lsn(0x0000_0001_2345_6789), "1/23456789");
    assert_eq!(lsn(0), "0/0");
}

#[test]
fn incremental_magic_matches_header() {
    assert_eq!(INCREMENTAL_MAGIC, 0xd3ae_1f0d);
}
