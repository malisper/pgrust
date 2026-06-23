//! Unit tests for the `bufmask.c` port.
//!
//! These build a raw `BLCKSZ` page buffer with hand-written header fields and
//! line pointers (matching the on-disk layout) and assert each mask routine
//! reproduces the C behaviour.

use super::*;
use ::types_storage::bufpage::{LP_NORMAL, PD_ALL_VISIBLE, PD_HAS_FREE_LINES, PD_PAGE_FULL};

const OFF_PD_FLAGS: usize = 10;
const OFF_PD_PRUNE_XID: usize = 20;

fn write_u16_at(page: &mut [u8], off: usize, v: u16) {
    page[off..off + 2].copy_from_slice(&v.to_ne_bytes());
}

fn read_u16_at(page: &[u8], off: usize) -> u16 {
    u16::from_ne_bytes([page[off], page[off + 1]])
}

fn write_u32_at(page: &mut [u8], off: usize, v: u32) {
    page[off..off + 4].copy_from_slice(&v.to_ne_bytes());
}

fn read_u32_at(page: &[u8], off: usize) -> u32 {
    u32::from_ne_bytes([page[off], page[off + 1], page[off + 2], page[off + 3]])
}

fn write_item_id(page: &mut [u8], offnum: u16, lp_off: u16, lp_flags: u32, lp_len: u16) {
    let start = SizeOfPageHeaderData + (offnum as usize - 1) * ITEM_ID_SIZE;
    let raw =
        (lp_off as u32 & 0x7fff) | ((lp_flags & 0x0003) << 15) | ((lp_len as u32 & 0x7fff) << 17);
    page[start..start + ITEM_ID_SIZE].copy_from_slice(&raw.to_ne_bytes());
}

fn read_item_id(page: &[u8], offnum: u16) -> (u16, u32, u16) {
    let start = SizeOfPageHeaderData + (offnum as usize - 1) * ITEM_ID_SIZE;
    let raw = u32::from_ne_bytes(page[start..start + ITEM_ID_SIZE].try_into().unwrap());
    let lp_off = (raw & 0x7fff) as u16;
    let lp_flags = (raw >> 15) & 0x0003;
    let lp_len = ((raw >> 17) & 0x7fff) as u16;
    (lp_off, lp_flags, lp_len)
}

/// Build a 0xff-filled page with two normal line pointers and sane header
/// fields.
fn test_page() -> [u8; BLCKSZ] {
    let mut page = [0xff_u8; BLCKSZ];
    write_u32_at(&mut page, 0, 0x1234);
    write_u32_at(&mut page, 4, 0x5678);
    write_u16_at(&mut page, OFF_PD_CHECKSUM, 0xabcd);
    write_u16_at(
        &mut page,
        OFF_PD_FLAGS,
        PD_PAGE_FULL | PD_HAS_FREE_LINES | PD_ALL_VISIBLE,
    );
    write_u16_at(
        &mut page,
        OFF_PD_LOWER,
        (SizeOfPageHeaderData + 2 * ITEM_ID_SIZE) as u16,
    );
    write_u16_at(&mut page, OFF_PD_UPPER, 128);
    write_u16_at(&mut page, OFF_PD_SPECIAL, BLCKSZ as u16);
    write_u16_at(&mut page, 18, BLCKSZ as u16);
    write_u32_at(&mut page, OFF_PD_PRUNE_XID, 99);
    write_item_id(&mut page, 1, 200, LP_NORMAL, 20);
    write_item_id(&mut page, 2, 220, LP_NORMAL, 20);
    page
}

#[test]
fn masks_lsn_and_checksum() {
    let mut page = test_page();
    mask_page_lsn_and_checksum(&mut page).unwrap();
    assert_eq!(read_u32_at(&page, 0), 0);
    assert_eq!(read_u32_at(&page, 4), 0);
    assert_eq!(read_u16_at(&page, OFF_PD_CHECKSUM), 0);
}

#[test]
fn masks_header_hint_bits() {
    let mut page = test_page();
    mask_page_hint_bits(&mut page).unwrap();
    assert_eq!(read_u32_at(&page, OFF_PD_PRUNE_XID), 0);
    let flags = read_u16_at(&page, OFF_PD_FLAGS);
    assert_eq!(flags & (PD_PAGE_FULL | PD_HAS_FREE_LINES | PD_ALL_VISIBLE), 0);
}

#[test]
fn masks_unused_space() {
    let mut page = test_page();
    let lower = SizeOfPageHeaderData + 2 * ITEM_ID_SIZE;
    mask_unused_space(&mut page).unwrap();
    assert!(page[lower..128].iter().all(|b| *b == 0));
    assert!(page[128..].iter().any(|b| *b == 0xff));
    assert_eq!(read_u16_at(&page, OFF_PD_UPPER), 128);
}

#[test]
fn mask_unused_space_rejects_invalid_header() {
    let mut page = test_page();
    write_u16_at(&mut page, OFF_PD_LOWER, 200);
    write_u16_at(&mut page, OFF_PD_UPPER, 100);
    let err = mask_unused_space(&mut page).unwrap_err();
    assert!(err.message().contains("invalid page pd_lower"));
}

#[test]
fn masks_line_pointer_flags() {
    let mut page = test_page();
    mask_lp_flags(&mut page).unwrap();
    let (off1, flags1, len1) = read_item_id(&page, 1);
    assert_eq!(flags1, LP_UNUSED);
    assert_eq!(off1, 200);
    assert_eq!(len1, 20);
    let (off2, flags2, len2) = read_item_id(&page, 2);
    assert_eq!(flags2, LP_UNUSED);
    assert_eq!(off2, 220);
    assert_eq!(len2, 20);
}

#[test]
fn mask_lp_flags_empty_page_is_noop() {
    let mut page = [0u8; BLCKSZ];
    write_u16_at(&mut page, OFF_PD_LOWER, SizeOfPageHeaderData as u16);
    write_u16_at(&mut page, OFF_PD_UPPER, BLCKSZ as u16);
    write_u16_at(&mut page, OFF_PD_SPECIAL, BLCKSZ as u16);
    let before = page;
    mask_lp_flags(&mut page).unwrap();
    assert_eq!(page, before);
}

#[test]
fn masks_page_content_and_lower_upper() {
    let mut page = test_page();
    mask_page_content(&mut page).unwrap();
    assert!(page[SizeOfPageHeaderData..].iter().all(|b| *b == 0));
    assert_eq!(read_u16_at(&page, OFF_PD_LOWER), 0);
    assert_eq!(read_u16_at(&page, OFF_PD_UPPER), 0);
    assert_eq!(read_u32_at(&page, 0), 0x1234);
}
