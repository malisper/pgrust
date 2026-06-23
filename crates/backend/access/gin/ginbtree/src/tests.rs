//! Unit tests for the `ginbtree.c` byte/serialization helpers that do not
//! require the descent engine's bufmgr / vtable substrate.

use super::*;

#[test]
fn ginxlog_insert_body_is_flags_le() {
    let body = encode_ginxlog_insert(GIN_INSERT_ISLEAF | GIN_INSERT_ISDATA);
    assert_eq!(body, (GIN_INSERT_ISLEAF | GIN_INSERT_ISDATA).to_ne_bytes());
}

#[test]
fn block_id_bytes_roundtrip() {
    let bytes = block_id_bytes(0x0001_2345);
    let bi_hi = u16::from_ne_bytes([bytes[0], bytes[1]]);
    let bi_lo = u16::from_ne_bytes([bytes[2], bytes[3]]);
    // BlockIdData stores the high 16 bits in bi_hi and low 16 in bi_lo.
    assert_eq!(((bi_hi as u32) << 16) | (bi_lo as u32), 0x0001_2345);
}

#[test]
fn init_seams_is_empty_hook() {
    init_seams();
}
