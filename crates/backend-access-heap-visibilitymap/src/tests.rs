//! Seam-free unit tests for the visibility-map bit arithmetic and layout
//! constants. The interface routines themselves drive the buffer manager via
//! seams (loud-panic until their owners land), so they are exercised in
//! integration once bufmgr/smgr are installed; here we lock down the pure math
//! that the C `#define`s encode.

use super::*;

#[test]
fn layout_constants_match_default_page() {
    // 8 KiB page, 24-byte header MAXALIGN'd to 24.
    assert_eq!(BLCKSZ, 8192);
    assert_eq!(CONTENTS_OFF, 24);
    assert_eq!(MAPSIZE, 8192 - 24);
    assert_eq!(BITS_PER_HEAPBLOCK, 2);
    assert_eq!(HEAPBLOCKS_PER_BYTE, 4);
    assert_eq!(HEAPBLOCKS_PER_PAGE, MAPSIZE * 4);
    assert_eq!(VISIBLE_MASK8, 0x55);
    assert_eq!(FROZEN_MASK8, 0xaa);
    assert_eq!(VISIBILITYMAP_VALID_BITS, 0x03);
}

#[test]
fn heapblk_macros_decompose_block_addressing() {
    // Block 0: page 0, byte 0, offset 0.
    assert_eq!(HEAPBLK_TO_MAPBLOCK(0), 0);
    assert_eq!(HEAPBLK_TO_MAPBYTE(0), 0);
    assert_eq!(HEAPBLK_TO_OFFSET(0), 0);

    // Within byte 0, the 4 heap blocks sit at offsets 0,2,4,6.
    assert_eq!(HEAPBLK_TO_OFFSET(1), 2);
    assert_eq!(HEAPBLK_TO_OFFSET(2), 4);
    assert_eq!(HEAPBLK_TO_OFFSET(3), 6);
    assert_eq!(HEAPBLK_TO_MAPBYTE(3), 0);

    // Block 4 wraps to byte 1, offset 0.
    assert_eq!(HEAPBLK_TO_MAPBYTE(4), 1);
    assert_eq!(HEAPBLK_TO_OFFSET(4), 0);

    // First block of the second VM page.
    let per_page = HEAPBLOCKS_PER_PAGE;
    assert_eq!(HEAPBLK_TO_MAPBLOCK(per_page), 1);
    assert_eq!(HEAPBLK_TO_MAPBYTE(per_page), 0);
    assert_eq!(HEAPBLK_TO_OFFSET(per_page), 0);
}

#[test]
fn popcount_masked_counts_visible_and_frozen_bits() {
    // One byte holding all-visible+all-frozen for the first heap block (0b11)
    // and all-visible-only for the second (0b01 << 2): 0b00000111 = 0x07.
    let buf = [0x07u8];
    // VISIBLE bits (0x55 mask): bits 0 and 2 set -> 2.
    assert_eq!(pg_popcount_masked(&buf, VISIBLE_MASK8), 2);
    // FROZEN bits (0xaa mask): bit 1 set -> 1.
    assert_eq!(pg_popcount_masked(&buf, FROZEN_MASK8), 1);

    // Empty map -> zero.
    assert_eq!(pg_popcount_masked(&[0u8; 16], VISIBLE_MASK8), 0);
    assert_eq!(pg_popcount_masked(&[0u8; 16], FROZEN_MASK8), 0);

    // Fully-set byte: 4 visible, 4 frozen.
    assert_eq!(pg_popcount_masked(&[0xffu8], VISIBLE_MASK8), 4);
    assert_eq!(pg_popcount_masked(&[0xffu8], FROZEN_MASK8), 4);
}

#[test]
fn map_byte_bounds_are_checked() {
    let mut page = [0u8; BLCKSZ];
    // Writing the last in-range map byte succeeds.
    let last = (MAPSIZE - 1) as usize;
    *map_byte_mut(&mut page, last).unwrap() = 0x03;
    assert_eq!(map_byte(&page, last).unwrap(), 0x03);

    // One past the end is rejected (the promoted bounds check).
    assert!(map_byte(&page, MAPSIZE as usize).is_err());
    assert!(map_byte_mut(&mut page, MAPSIZE as usize).is_err());
}

#[test]
fn xlogrecptr_is_invalid_for_zero() {
    assert!(XLogRecPtrIsInvalid(0));
    assert!(!XLogRecPtrIsInvalid(1));
}
