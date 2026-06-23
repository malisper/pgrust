//! Pure-logic tests for the `logtape.c` port. The end-to-end tape round-trips
//! need a real `BufFile` (the fd.c temp-file/temp-tablespace subsystem), which
//! a unit test cannot stand up — mirroring `backend-storage-file-buffile`'s
//! own pure-logic-only test policy. Covered here: the block-trailer codec and
//! the layout constants, which carry the on-disk format fidelity.

use super::*;

#[test]
fn layout_constants_match_c() {
    // logtape.c: TapeBlockPayloadSize = BLCKSZ - sizeof(TapeBlockTrailer).
    assert_eq!(SIZEOF_TAPE_BLOCK_TRAILER, 16);
    assert_eq!(TapeBlockPayloadSize, BLCKSZ - 16);
    assert_eq!(TapeBlockPayloadSize, 8176);
    assert_eq!(TAPE_WRITE_PREALLOC_MIN, 8);
    assert_eq!(TAPE_WRITE_PREALLOC_MAX, 128);
}

#[test]
fn trailer_prev_next_roundtrip() {
    let mut buf = [0u8; BLCKSZ];
    TapeBlockSetPrev(&mut buf, 0x1122_3344_5566_7788);
    TapeBlockSetNext(&mut buf, -7);
    assert_eq!(trailer_prev(&buf), 0x1122_3344_5566_7788);
    assert_eq!(trailer_next(&buf), -7);
}

#[test]
fn last_block_encoding_via_nbytes() {
    // TapeBlockSetNBytes(buf, n) sets next = -(n); a last block has next < 0,
    // and TapeBlockGetNBytes returns -next.
    let mut buf = [0u8; BLCKSZ];
    TapeBlockSetNBytes(&mut buf, 1234);
    assert!(TapeBlockIsLast(&buf));
    assert_eq!(TapeBlockGetNBytes(&buf), 1234);
    assert_eq!(trailer_next(&buf), -1234);
}

#[test]
fn non_last_block_get_nbytes_is_payload_size() {
    // A non-last block (next >= 0) reports the full payload size.
    let mut buf = [0u8; BLCKSZ];
    TapeBlockSetNext(&mut buf, 42); // a real following block number
    assert!(!TapeBlockIsLast(&buf));
    assert_eq!(TapeBlockGetNBytes(&buf), TapeBlockPayloadSize as i64);
}

#[test]
fn min_heap_offset_helpers() {
    // Standard 0-based binary-heap index arithmetic (logtape.c).
    assert_eq!(left_offset(0), 1);
    assert_eq!(right_offset(0), 2);
    assert_eq!(parent_offset(1), 0);
    assert_eq!(parent_offset(2), 0);
    assert_eq!(parent_offset(3), 1);
    assert_eq!(parent_offset(4), 1);
}
