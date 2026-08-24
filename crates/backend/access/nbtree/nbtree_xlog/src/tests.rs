use super::*;

fn itup(size: usize, key: u8) -> Vec<u8> {
    assert!(size >= 8 && size % 8 == 0);
    let mut t = vec![key; size];
    t[6..8].copy_from_slice(&(size as u16).to_ne_bytes());
    t
}

#[test]
fn restore_page_reverses_stream_into_offnum_order() {
    let a = itup(16, 0xAA);
    let b = itup(24, 0xBB);
    let c = itup(16, 0xCC);
    // page-memory order: highest offset number first (lowest on the page).
    let stream: Vec<u8> = [c.clone(), b.clone(), a.clone()].concat();

    #[repr(align(8))]
    struct P([u8; BLCKSZ]);
    let mut p = P([0u8; BLCKSZ]);
    // SAFETY: owned aligned scratch page.
    let mut pm =
        unsafe { PageMut::from_raw(core::ptr::NonNull::new(p.0.as_mut_ptr()).unwrap()) };
    bt_pageinit(&mut pm);
    bt_restore_page(&mut pm, &stream).unwrap();

    let r = pm.as_ref();
    assert_eq!(r.max_offset_number(), 3);
    for (off, want) in [(1u16, &a), (2, &b), (3, &c)] {
        let id = r.item_id(off);
        let (ptr, len) = r.item_raw(id);
        let got = unsafe { core::slice::from_raw_parts(ptr, len as usize) };
        assert_eq!(got, &want[..], "offnum {off}");
    }
    // physical order preserved: offnum 3 lowest on the page.
    let off3 = r.item_id(3).lp_off();
    let off1 = r.item_id(1).lp_off();
    assert!(off3 < off1);
    assert_eq!(r.pd_special() as usize - r.pd_upper() as usize, 16 + 24 + 16);
}

// Malformed replayed WAL (truncated main data, truncated/zero-size tuple
// streams, inflated counts) must surface as a catchable ERRCODE_DATA_CORRUPTED
// error on the startup redo thread rather than a slice-index / arithmetic panic.
// See finding: "nbtree redo fixed-offset main-data reads and tuple-stream walks
// panic on short/hostile records".

fn is_corrupted(e: &types_error::PgError) -> bool {
    e.sqlstate() == types_error::ERRCODE_DATA_CORRUPTED
}

#[test]
fn require_len_rejects_short_main_data() {
    // A record with rmid=RM_BTREE_ID and truncated main data (0 is legal).
    let e = require_len(&[], SizeOfBtreeInsert, "t").err().unwrap();
    assert!(is_corrupted(&e));
    let e = require_len(&[0u8; 9], SizeOfBtreeUnlinkPage, "t").err().unwrap();
    assert!(is_corrupted(&e));
    // Exactly the required length succeeds.
    assert!(require_len(&[0u8; SizeOfBtreeSplit], SizeOfBtreeSplit, "t").is_ok());
}

#[test]
fn read_u16_rejects_overrun() {
    assert_eq!(read_u16(&[7, 0, 9], 0, "t").unwrap(), 7);
    assert!(is_corrupted(&read_u16(&[0u8; 3], 2, "t").err().unwrap()));
    assert!(is_corrupted(&read_u16(&[], 0, "t").err().unwrap()));
    // Offset arithmetic near usize::MAX must not overflow-panic.
    assert!(read_u16(&[0u8; 8], usize::MAX, "t").is_err());
}

#[test]
fn itup_size_at_rejects_truncated_header() {
    // A full 8-byte header is required to read the size word.
    assert!(is_corrupted(&itup_size_at(&[0u8; 4], 0).err().unwrap()));
    assert!(is_corrupted(&itup_size_at(&[0u8; 8], 4).err().unwrap()));
    // Valid header returns the masked size word.
    let mut t = [0u8; 8];
    t[6..8].copy_from_slice(&(24u16).to_ne_bytes());
    assert_eq!(itup_size_at(&t, 0).unwrap(), 24);
}

#[test]
fn bt_restore_page_rejects_hostile_streams() {
    #[repr(align(8))]
    struct P([u8; BLCKSZ]);
    fn run(stream: &[u8]) -> PgResult<()> {
        let mut p = P([0u8; BLCKSZ]);
        // SAFETY: owned aligned scratch page.
        let mut pm =
            unsafe { PageMut::from_raw(core::ptr::NonNull::new(p.0.as_mut_ptr()).unwrap()) };
        bt_pageinit(&mut pm);
        bt_restore_page(&mut pm, stream)
    }

    // Truncated stream: a partial item header (< 8 bytes) cannot be decoded.
    assert!(is_corrupted(&run(&[0u8; 4]).err().unwrap()));

    // Zero-size item word: never advances `off` (would spin then over-index).
    let mut zero = [0u8; 8];
    zero[6..8].copy_from_slice(&0u16.to_ne_bytes());
    assert!(is_corrupted(&run(&zero).err().unwrap()));

    // Item size word larger than the remaining stream.
    let mut over = [0u8; 8];
    over[6..8].copy_from_slice(&64u16.to_ne_bytes());
    assert!(is_corrupted(&run(&over).err().unwrap()));

    // More items than a page can hold (each a valid 8-byte header).
    let mut many = Vec::new();
    for _ in 0..(MaxIndexTuplesPerPage + 1) {
        let mut t = [0u8; 8];
        t[6..8].copy_from_slice(&8u16.to_ne_bytes());
        many.extend_from_slice(&t);
    }
    assert!(is_corrupted(&run(&many).err().unwrap()));
}

// Build a minimal posting-list index tuple: `keysize` bytes of key/header
// followed by `norig` 6-byte TIDs.
fn posting_tuple(keysize: usize, norig: u16) -> Vec<u8> {
    let total = keysize + norig as usize * 6;
    let mut t = vec![0u8; total];
    // posting offset (posting_base) split across ip_blkid hi/lo.
    t[0..2].copy_from_slice(&(((keysize >> 16) as u16)).to_ne_bytes());
    t[2..4].copy_from_slice(&((keysize & 0xffff) as u16).to_ne_bytes());
    // ip_posid: BT_IS_POSTING | nposting.
    t[4..6].copy_from_slice(&(types_nbtree::BT_IS_POSTING | norig).to_ne_bytes());
    // t_info: INDEX_ALT_TID_MASK | size.
    t[6..8].copy_from_slice(&(types_nbtree::INDEX_ALT_TID_MASK | total as u16).to_ne_bytes());
    t
}

#[test]
fn xlog_update_posting_rejects_inflated_and_malformed() {
    let mut out = [0u8; BLCKSZ];
    let orig = posting_tuple(8, 3);

    // ndeleted >= norig: nhtids would underflow (panic in debug, wrap in release
    // feeding a huge newsize into out[..newsize]).
    let del_all = [0u8; 6]; // 3 delete offsets
    assert!(is_corrupted(&xlog_update_posting(&orig, &del_all, &mut out).err().unwrap()));

    // ndeleted == 0: nhtids == norig, also rejected (C Assert nhtids < norig).
    assert!(is_corrupted(&xlog_update_posting(&orig, &[], &mut out).err().unwrap()));

    // Target tuple that is not a posting list at all.
    let plain = vec![0u8; 16];
    assert!(is_corrupted(&xlog_update_posting(&plain, &[0u8; 2], &mut out).err().unwrap()));

    // A posting header claiming more TIDs than its body contains.
    let mut short = posting_tuple(8, 5);
    short.truncate(8 + 6); // only room for 1 TID, header says 5
    assert!(is_corrupted(&xlog_update_posting(&short, &[0u8; 2], &mut out).err().unwrap()));

    // A well-formed single-TID delete succeeds.
    let one = [0u8, 0]; // delete posting entry index 0
    let n = xlog_update_posting(&orig, &one, &mut out).unwrap();
    assert!(n > 0);
}

#[test]
fn opcode_constants_match_nbtxlog_h() {
    assert_eq!(XLOG_BTREE_INSERT_LEAF, 0x00);
    assert_eq!(XLOG_BTREE_INSERT_UPPER, 0x10);
    assert_eq!(XLOG_BTREE_INSERT_META, 0x20);
    assert_eq!(XLOG_BTREE_SPLIT_L, 0x30);
    assert_eq!(XLOG_BTREE_SPLIT_R, 0x40);
    assert_eq!(XLOG_BTREE_NEWROOT, 0xA0);
    assert_eq!(core::mem::size_of::<BTMetaPageData>(), 48);
    assert_eq!(SizeOfBtreeOpaque, 16);
    assert_eq!(MaxIndexTuplesPerPage, 408);
}
