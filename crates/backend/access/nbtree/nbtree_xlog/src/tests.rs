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
