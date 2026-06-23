//! `backend/access/common/bufmask.c` — buffer masking.
//!
//! Routines for masking certain bits in a page which can differ between WAL
//! generation and WAL apply. Used by the rmgr `rm_mask` callbacks during
//! WAL-consistency checks.
//!
//! In C a `Page` is a `char *` overlaid with `(PageHeader) page`; here each
//! function takes the page bytes by `&mut [u8]` and operates through the
//! already-ported [`page`] page views. The field/range writes
//! `backend-storage-page` does not expose a public mutator for (`pd_checksum`,
//! `pd_lower`/`pd_upper`, and the content/`pd_lower..pd_upper` `memset`s) are
//! done here as byte writes at the documented `PageHeaderData` offsets — the
//! same fixed native-endian layout `backend-storage-page` uses internally.

#![no_std]
#![forbid(unsafe_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

use alloc::format;

use page::{
    PageClearAllVisible, PageClearFull, PageClearHasFreeLinePointers, PageClearPrunable,
    PageGetItemId, PageGetMaxOffsetNumber, PageMut, PageSetItemId, PageSetLSN,
};
use ::types_core::primitive::{OffsetNumber, BLCKSZ};
use types_error::{PgError, PgResult};
use ::types_storage::bufpage::{ItemIdData, SizeOfPageHeaderData, LP_UNUSED};
use ::types_tuple::heaptuple::FIRST_OFFSET_NUMBER;

/// `MASK_MARKER` (bufmask.h): marker used to mask pages consistently.
pub const MASK_MARKER: u8 = 0;

// Fixed byte offsets of the `PageHeaderData` fields this module writes
// directly, matching the native-endian on-disk layout `backend-storage-page`
// uses internally. `SizeOfPageHeaderData` (== 24) is the offset of the
// line-pointer array `pd_linp`.
const OFF_PD_CHECKSUM: usize = 8;
const OFF_PD_LOWER: usize = 12;
const OFF_PD_UPPER: usize = 14;
const OFF_PD_SPECIAL: usize = 16;

/// `sizeof(ItemIdData)` — every line pointer is 4 bytes (used by tests).
#[allow(dead_code)]
const ITEM_ID_SIZE: usize = core::mem::size_of::<ItemIdData>();

/// `mask_page_lsn_and_checksum(page)`.
///
/// Set `pd_lsn` and `pd_checksum` to `MASK_MARKER`. The LSN of two compared
/// pages differs because of concurrent operations; masking other fields also
/// invalidates the checksum, so it is masked too.
pub fn mask_page_lsn_and_checksum(page: &mut [u8]) -> PgResult<()> {
    let mut page = PageMut::new(page)?;
    PageSetLSN(&mut page, MASK_MARKER as u64);
    write_u16(page.as_mut_bytes(), OFF_PD_CHECKSUM, MASK_MARKER as u16);
    Ok(())
}

/// `mask_page_hint_bits(page)`.
///
/// Mask the page-level hint bits that can be set without emitting any WAL:
/// `pd_prune_xid`, the `PD_PAGE_FULL`/`PD_HAS_FREE_LINES` flags, and
/// `PD_ALL_VISIBLE`.
pub fn mask_page_hint_bits(page: &mut [u8]) -> PgResult<()> {
    let mut page = PageMut::new(page)?;

    // Ignore prune_xid (it's like a hint-bit). C writes `pd_prune_xid =
    // MASK_MARKER` (0), which is exactly `PageClearPrunable`.
    PageClearPrunable(&mut page);

    // Ignore PD_PAGE_FULL and PD_HAS_FREE_LINES flags, they are just hints.
    PageClearFull(&mut page);
    PageClearHasFreeLinePointers(&mut page);

    // During replay, if the page LSN has advanced past our XLOG record's LSN,
    // we don't mark the page all-visible.
    PageClearAllVisible(&mut page);

    Ok(())
}

/// `mask_unused_space(page)`.
///
/// Mask the unused space of a page between `pd_lower` and `pd_upper`.
pub fn mask_unused_space(page: &mut [u8]) -> PgResult<()> {
    let mut page = PageMut::new(page)?;
    let bytes = page.as_bytes();
    let pd_lower = read_u16(bytes, OFF_PD_LOWER) as usize;
    let pd_upper = read_u16(bytes, OFF_PD_UPPER) as usize;
    let pd_special = read_u16(bytes, OFF_PD_SPECIAL) as usize;

    if pd_lower > pd_upper
        || pd_special < pd_upper
        || pd_lower < SizeOfPageHeaderData
        || pd_special > BLCKSZ
    {
        return Err(PgError::error(format!(
            "invalid page pd_lower {pd_lower} pd_upper {pd_upper} pd_special {pd_special}"
        )));
    }

    page.as_mut_bytes()[pd_lower..pd_upper].fill(MASK_MARKER);
    Ok(())
}

/// `mask_lp_flags(page)`.
///
/// In some index AMs, line pointer flags can be modified on the primary without
/// emitting any WAL record; mask each used line pointer's `lp_flags` to
/// `LP_UNUSED`.
pub fn mask_lp_flags(page: &mut [u8]) -> PgResult<()> {
    let mut page = PageMut::new(page)?;

    let maxoff = PageGetMaxOffsetNumber(&page.as_ref());
    let mut offnum: OffsetNumber = FIRST_OFFSET_NUMBER;
    while offnum <= maxoff {
        let item_id = PageGetItemId(&page.as_ref(), offnum)?;
        // C: if (ItemIdIsUsed(itemId)) itemId->lp_flags = LP_UNUSED;
        // Clear only lp_flags, preserving lp_off/lp_len as C does.
        if item_id.lp_flags() != LP_UNUSED {
            let cleared = ItemIdData::new(item_id.lp_off(), LP_UNUSED, item_id.lp_len());
            PageSetItemId(&mut page, offnum, cleared)?;
        }
        offnum += 1;
    }
    Ok(())
}

/// `mask_page_content(page)`.
///
/// In some index AMs, the contents of deleted pages need to be almost
/// completely ignored. Mask everything past `SizeOfPageHeaderData` plus
/// `pd_lower`/`pd_upper`.
pub fn mask_page_content(page: &mut [u8]) -> PgResult<()> {
    let mut page = PageMut::new(page)?;
    let bytes = page.as_mut_bytes();

    bytes[SizeOfPageHeaderData..BLCKSZ].fill(MASK_MARKER);
    write_u16(bytes, OFF_PD_LOWER, MASK_MARKER as u16);
    write_u16(bytes, OFF_PD_UPPER, MASK_MARKER as u16);

    Ok(())
}

#[inline]
fn read_u16(bytes: &[u8], off: usize) -> u16 {
    u16::from_ne_bytes([bytes[off], bytes[off + 1]])
}

#[inline]
fn write_u16(bytes: &mut [u8], off: usize, value: u16) {
    bytes[off..off + 2].copy_from_slice(&value.to_ne_bytes());
}

/// Install this unit's owned seams. Wired into `seams-init::init_all()`.
///
/// The four seams the consumers declared as infallible (fixed-offset writes)
/// surface the page-size invariant violation as a loud panic — a correctly
/// sized `BLCKSZ` page never trips it, and bad input is undefined behaviour in
/// C (`(PageHeader) page` overlay on a short buffer).
pub fn init_seams() {
    bufmask_seams::mask_page_lsn_and_checksum::set(|page| {
        mask_page_lsn_and_checksum(page).expect("mask_page_lsn_and_checksum: malformed page")
    });
    bufmask_seams::mask_page_hint_bits::set(|page| {
        mask_page_hint_bits(page).expect("mask_page_hint_bits: malformed page")
    });
    bufmask_seams::mask_unused_space::set(mask_unused_space);
    bufmask_seams::mask_lp_flags::set(|page| {
        mask_lp_flags(page).expect("mask_lp_flags: malformed page")
    });
    bufmask_seams::mask_page_content::set(|page| {
        mask_page_content(page).expect("mask_page_content: malformed page")
    });
}

#[cfg(test)]
mod tests;
