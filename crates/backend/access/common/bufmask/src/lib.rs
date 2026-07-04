#![no_std]

use types_core::{uint16, uint32, BLCKSZ};
use types_storage::bufpage::{
    ItemIdData, PageHeaderData, PageMut, SizeOfPageHeaderData, LP_UNUSED,
};

pub const MASK_MARKER: u8 = 0;

#[inline]
fn page_mut(page: &mut [u8]) -> PageMut<'_> {
    assert_eq!(page.len(), BLCKSZ);
    let ptr = core::ptr::NonNull::new(page.as_mut_ptr()).unwrap();
    // SAFETY: `page` is a full BLCKSZ image, exclusively borrowed for the
    // returned value's lifetime.
    unsafe { PageMut::from_raw(ptr) }
}

pub fn mask_page_lsn_and_checksum(page: &mut [u8]) {
    let mut pm = page_mut(page);
    pm.set_lsn(MASK_MARKER as u64);
    let off = core::mem::offset_of!(PageHeaderData, pd_checksum);
    // SAFETY: in-bounds, 2-aligned (header is MAXALIGNed).
    unsafe { pm.as_mut_ptr().add(off).cast::<uint16>().write(MASK_MARKER as uint16) };
}

pub fn mask_page_hint_bits(page: &mut [u8]) {
    let mut pm = page_mut(page);
    pm.set_prune_xid(MASK_MARKER as uint32);
    pm.clear_full();
    pm.clear_has_free_line_pointers();
    pm.clear_all_visible();
}

/// Panics on corrupt page pointers (C: `elog(ERROR, "invalid page ...")`).
pub fn mask_unused_space(page: &mut [u8]) {
    let mut pm = page_mut(page);
    let r = pm.as_ref();
    let pd_lower = r.pd_lower() as usize;
    let pd_upper = r.pd_upper() as usize;
    let pd_special = r.pd_special() as usize;
    assert!(
        pd_lower <= pd_upper
            && pd_upper <= pd_special
            && pd_lower >= SizeOfPageHeaderData
            && pd_special <= BLCKSZ,
        "invalid page pd_lower {pd_lower} pd_upper {pd_upper} pd_special {pd_special}"
    );
    // SAFETY: [pd_lower, pd_upper) validated within the page above.
    unsafe {
        core::ptr::write_bytes(pm.as_mut_ptr().add(pd_lower), MASK_MARKER, pd_upper - pd_lower)
    };
}

pub fn mask_lp_flags(page: &mut [u8]) {
    let mut pm = page_mut(page);
    let maxoff = pm.as_ref().max_offset_number();
    for offnum in 1..=maxoff {
        let id = pm.as_ref().item_id(offnum);
        if id.is_used() {
            pm.set_item_id(offnum, ItemIdData::new(id.lp_off(), LP_UNUSED, id.lp_len()));
        }
    }
}

pub fn mask_page_content(page: &mut [u8]) {
    let mut pm = page_mut(page);
    // SAFETY: [SizeOfPageHeaderData, BLCKSZ) is within the page.
    unsafe {
        core::ptr::write_bytes(
            pm.as_mut_ptr().add(SizeOfPageHeaderData),
            MASK_MARKER,
            BLCKSZ - SizeOfPageHeaderData,
        )
    };
    let lower_off = core::mem::offset_of!(PageHeaderData, pd_lower);
    let upper_off = core::mem::offset_of!(PageHeaderData, pd_upper);
    // SAFETY: header fields, 2-aligned, in-bounds.
    unsafe {
        pm.as_mut_ptr().add(lower_off).cast::<uint16>().write(MASK_MARKER as uint16);
        pm.as_mut_ptr().add(upper_off).cast::<uint16>().write(MASK_MARKER as uint16);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use types_storage::bufpage::PAI_IS_HEAP;

    #[repr(align(8))]
    struct AlignedPage([u8; BLCKSZ]);

    fn temp_page() -> AlignedPage {
        AlignedPage([0u8; BLCKSZ])
    }

    fn page_mut_of(t: &mut AlignedPage) -> PageMut<'_> {
        let ptr = core::ptr::NonNull::new(t.0.as_mut_ptr()).unwrap();
        // SAFETY: owned MAXALIGNed BLCKSZ image, exclusively borrowed.
        unsafe { PageMut::from_raw(ptr) }
    }

    #[test]
    fn lsn_and_checksum_masked() {
        let mut t = temp_page();
        let mut pm = page_mut_of(&mut t);
        pm.init(0);
        pm.set_lsn(0x0102_0304_0506_0708);
        let off = core::mem::offset_of!(PageHeaderData, pd_checksum);
        // SAFETY: in-bounds header write for test setup.
        unsafe { pm.as_mut_ptr().add(off).cast::<uint16>().write(0xBEEF) };
        drop(pm);

        mask_page_lsn_and_checksum(&mut t.0);

        let pm = page_mut_of(&mut t);
        assert_eq!(pm.as_ref().lsn(), 0);
        // SAFETY: in-bounds header read.
        let checksum = unsafe { pm.as_ref().as_ptr().add(off).cast::<uint16>().read() };
        assert_eq!(checksum, 0);
    }

    #[test]
    fn hint_bits_masked() {
        let mut t = temp_page();
        let mut pm = page_mut_of(&mut t);
        pm.init(0);
        pm.set_prune_xid(0xDEAD_BEEF);
        pm.set_full();
        pm.set_has_free_line_pointers();
        pm.set_all_visible();
        drop(pm);

        mask_page_hint_bits(&mut t.0);

        let pm = page_mut_of(&mut t);
        let r = pm.as_ref();
        assert_eq!(r.prune_xid(), 0);
        assert!(!r.is_full());
        assert!(!r.has_free_line_pointers());
        assert!(!r.is_all_visible());
    }

    #[test]
    fn unused_space_masked_between_lower_and_upper() {
        let mut t = temp_page();
        let mut pm = page_mut_of(&mut t);
        pm.init(0);
        let item = [0xAAu8; 32];
        pm.add_item(&item, 0, PAI_IS_HEAP).unwrap();
        let (pd_lower, pd_upper) = (pm.as_ref().pd_lower() as usize, pm.as_ref().pd_upper() as usize);
        // Poison the free region so the test can see it get masked.
        // SAFETY: [pd_lower, pd_upper) is free space within the page.
        unsafe { core::ptr::write_bytes(pm.as_mut_ptr().add(pd_lower), 0xFF, pd_upper - pd_lower) };
        drop(pm);

        mask_unused_space(&mut t.0);

        assert!(t.0[pd_lower..pd_upper].iter().all(|&b| b == 0));
        // Tuple bytes (beyond pd_upper) are untouched.
        assert!(t.0[pd_upper..pd_upper + 32].iter().all(|&b| b == 0xAA));
    }

    #[test]
    #[should_panic(expected = "invalid page")]
    fn unused_space_panics_on_corrupt_pointers() {
        let mut t = temp_page();
        let mut pm = page_mut_of(&mut t);
        pm.init(0);
        pm.set_pd_lower(pm.as_ref().pd_upper() + 1); // pd_lower > pd_upper
        drop(pm);
        mask_unused_space(&mut t.0);
    }

    #[test]
    fn lp_flags_masked_preserving_off_and_len() {
        let mut t = temp_page();
        let mut pm = page_mut_of(&mut t);
        pm.init(16);
        let item = [0u8; 16];
        pm.add_item(&item, 1, 0).unwrap();
        pm.add_item(&item, 2, 0).unwrap();
        let mut dead = pm.as_ref().item_id(2);
        dead.mark_dead();
        pm.set_item_id(2, dead);
        let (id1_before, id2_before) = (pm.as_ref().item_id(1), pm.as_ref().item_id(2));
        drop(pm);

        mask_lp_flags(&mut t.0);

        let pm = page_mut_of(&mut t);
        let (id1, id2) = (pm.as_ref().item_id(1), pm.as_ref().item_id(2));
        assert!(!id1.is_used());
        assert!(!id2.is_used());
        assert_eq!(id1.lp_off(), id1_before.lp_off());
        assert_eq!(id1.lp_len(), id1_before.lp_len());
        assert_eq!(id2.lp_off(), id2_before.lp_off());
        assert_eq!(id2.lp_len(), id2_before.lp_len());
    }

    #[test]
    fn page_content_masked_except_leading_header_fields() {
        let mut t = temp_page();
        let mut pm = page_mut_of(&mut t);
        pm.init(0);
        pm.set_lsn(0x1122_3344_5566_7788);
        let item = [0x77u8; 32];
        pm.add_item(&item, 0, PAI_IS_HEAP).unwrap();
        drop(pm);

        mask_page_content(&mut t.0);

        assert!(t.0[SizeOfPageHeaderData..].iter().all(|&b| b == 0));
        let pm = page_mut_of(&mut t);
        assert_eq!(pm.as_ref().pd_lower(), 0);
        assert_eq!(pm.as_ref().pd_upper(), 0);
        // pd_lsn precedes SizeOfPageHeaderData and mask_page_content never
        // touches it (only mask_page_lsn_and_checksum does).
        assert_eq!(pm.as_ref().lsn(), 0x1122_3344_5566_7788);
    }
}
