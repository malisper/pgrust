//! Unit tests for the GiST build engine's pure byte-level helpers.

use crate::gistbuildbuffers::{index_tuple_size, BUFFER_PAGE_DATA_OFFSET, DATA_SIZE};
use types_core::primitive::BLCKSZ;

#[test]
fn buffer_page_data_offset_is_eight() {
    // offsetof(GISTNodeBufferPage, tupledata) == 8 (4-byte prev + 4-byte
    // freespace), MAXALIGN(8) == 8.
    assert_eq!(BUFFER_PAGE_DATA_OFFSET, 8);
    assert_eq!(DATA_SIZE, BLCKSZ - 8);
}

#[test]
fn index_tuple_size_reads_low_13_bits_of_t_info() {
    // t_info is the u16 at byte offset 6; the size is its low 13 bits.
    let mut itup = [0u8; 16];
    let t_info: u16 = 0xE000 | 16; // high flag bits set, size = 16
    itup[6..8].copy_from_slice(&t_info.to_ne_bytes());
    assert_eq!(index_tuple_size(&itup), 16);
}
