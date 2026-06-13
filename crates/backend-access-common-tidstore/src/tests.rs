//! Tests for the `tidstore.c` bit math.
//!
//! These drive only the in-crate, allocation-pure logic that needs no radix
//! substrate: the `WORDNUM`/`BITNUM`/`WORDS_PER_PAGE` helpers and constants,
//! the `BlocktableEntry` pack (`from_offsets`, both the inline `full_offsets`
//! header form and the compressed `words` bitmap form), the membership test
//! (`contains`), the offset unpack (`offsets_into`), and the across-seam wire
//! encode/decode round-trip. The radix-tree container ops (`local_ts_*` /
//! `shared_ts_*`) are seamed out to their owner, so the
//! `TidStoreCreate*`/`Set`/`Iterate`/... wrappers panic until that owner lands
//! and are not exercised here.

extern crate alloc;

use super::*;
use alloc::vec;
use alloc::vec::Vec;

#[test]
fn helper_constants() {
    // (8 - 1 - 1)/2 == 3 on a 64-bit pointer / 16-bit OffsetNumber platform.
    assert_eq!(NUM_FULL_OFFSETS, 3);
    assert_eq!(wordnum(0), 0);
    assert_eq!(wordnum(BITS_PER_BITMAPWORD), 1);
    assert_eq!(bitnum(BITS_PER_BITMAPWORD + 5), 5);
    assert_eq!(words_per_page(0), 1);
    assert_eq!(words_per_page(BITS_PER_BITMAPWORD), 2);
    // In practice equals MaxOffsetNumber.
    assert_eq!(MAX_OFFSET_IN_BITMAP, MaxOffsetNumber);
}

#[test]
fn pack_small_uses_header_form() {
    let entry = BlocktableEntry::from_offsets(&[3, 7, 100]).unwrap();
    assert_eq!(entry.nwords, 0);
    assert!(entry.words.is_empty());
    assert_eq!(entry.full_offsets, [3, 7, 100]);

    assert!(entry.contains(3));
    assert!(entry.contains(7));
    assert!(entry.contains(100));
    assert!(!entry.contains(8));
}

#[test]
fn pack_large_uses_bitmap_form() {
    // 4 offsets, > NUM_FULL_OFFSETS, spanning two bitmap words.
    let big = (BITS_PER_BITMAPWORD as u16) + 1; // first bit of word 1
    let mut offsets: Vec<u16> = vec![1, 5, BITS_PER_BITMAPWORD as u16 - 1, big];
    offsets.sort_unstable();
    offsets.dedup();

    let entry = BlocktableEntry::from_offsets(&offsets).unwrap();
    assert!(entry.nwords > 0);
    assert_eq!(
        entry.nwords as usize,
        words_per_page(*offsets.last().unwrap() as usize)
    );

    for &off in &offsets {
        assert!(entry.contains(off), "offset {off} should be a member");
    }
    assert!(!entry.contains(2));
    assert!(!entry.contains(big + 200));
}

#[test]
fn offsets_into_round_trips_header_form() {
    let entry = BlocktableEntry::from_offsets(&[11, 22]).unwrap();
    let mut buf = [0u16; 8];
    let n = entry.offsets_into(&mut buf);
    assert_eq!(n, 2);
    assert_eq!(&buf[..n], &[11, 22]);
}

#[test]
fn offsets_into_round_trips_bitmap_form() {
    let mut offsets: Vec<u16> = vec![1, 2, BITS_PER_BITMAPWORD as u16, 130, 200];
    offsets.sort_unstable();
    offsets.dedup();

    let entry = BlocktableEntry::from_offsets(&offsets).unwrap();
    let mut buf = [0u16; 16];
    let n = entry.offsets_into(&mut buf);
    assert_eq!(n, offsets.len());
    assert_eq!(&buf[..n], offsets.as_slice());
}

#[test]
fn offsets_into_buffer_too_small_returns_required() {
    let entry = BlocktableEntry::from_offsets(&[10, 20, 30, 300, 301]).unwrap();
    // Buffer holds only 2 of 5.
    let mut buf = [0u16; 2];
    let n = entry.offsets_into(&mut buf);
    assert_eq!(n, 5); // total required
    assert_eq!(&buf, &[10, 20]); // first two filled
}

#[test]
fn invalid_offset_rejected() {
    // InvalidOffsetNumber (0) in the header form.
    let err = BlocktableEntry::from_offsets(&[0]).unwrap_err();
    assert!(err.message.starts_with("tuple offset out of range"));

    // A full bitmap-form set up to MAX_OFFSET_IN_BITMAP packs fine.
    assert!(BlocktableEntry::from_offsets(&[1, 2, 3, 64, MAX_OFFSET_IN_BITMAP]).is_ok());
}

#[test]
fn entry_encode_decode_round_trip() {
    // Header form.
    let e1 = BlocktableEntry::from_offsets(&[5, 9, 13]).unwrap();
    let w1 = e1.encode().unwrap();
    assert_eq!(BlocktableEntry::decode(&w1).unwrap(), e1);

    // Bitmap form.
    let e2 = BlocktableEntry::from_offsets(&[1, 2, 3, 64, 250]).unwrap();
    let w2 = e2.encode().unwrap();
    assert_eq!(BlocktableEntry::decode(&w2).unwrap(), e2);
}
