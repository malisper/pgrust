//! BTDedupStateData (nbtree.h) shared by _bt_dedup_pass (nbtree), _bt_load
//! dedup (nbtsort), btree_xlog_dedup redo (nbtree_xlog). Raw tuple images.

use crate::page::BTMaxItemSize;
use crate::vacuum::BTDedupInterval;
use crate::{BT_IS_POSTING, BT_OFFSET_MASK, INDEX_ALT_TID_MASK};
use types_core::{OffsetNumber, Size, BLCKSZ};
use types_storage::bufpage::{ItemIdData, PageMut, SizeOfPageHeaderData};
use types_tuple::itemptr::{ItemPointerData, InvalidOffsetNumber};

pub const INDEX_SIZE_MASK: u16 = 0x1FFF;
pub const MaxIndexTuplesPerPage: usize = (BLCKSZ - SizeOfPageHeaderData) / (16 + 4);
const _: () = assert!(MaxIndexTuplesPerPage == 408);

const IPD_SIZE: usize = core::mem::size_of::<ItemPointerData>();
const _: () = assert!(IPD_SIZE == 6);

// bound: bottom-up deletion sets maxpostingsize = BLCKSZ (C pallocs that much)
const MAX_HTIDS: usize = BLCKSZ / IPD_SIZE + 2;

const fn maxalign(l: usize) -> usize {
    (l + 7) & !7
}

#[inline]
unsafe fn t_info(itup: *const u8) -> u16 {
    itup.add(6).cast::<u16>().read_unaligned()
}

#[inline]
unsafe fn t_tid_posid(itup: *const u8) -> u16 {
    itup.add(4).cast::<u16>().read_unaligned()
}

#[inline]
pub unsafe fn itup_size(itup: *const u8) -> usize {
    (t_info(itup) & INDEX_SIZE_MASK) as usize
}

#[inline]
pub unsafe fn itup_is_posting(itup: *const u8) -> bool {
    (t_info(itup) & INDEX_ALT_TID_MASK) != 0 && (t_tid_posid(itup) & BT_IS_POSTING) != 0
}

#[inline]
pub unsafe fn itup_is_pivot(itup: *const u8) -> bool {
    (t_info(itup) & INDEX_ALT_TID_MASK) != 0 && (t_tid_posid(itup) & BT_IS_POSTING) == 0
}

#[inline]
pub unsafe fn itup_nposting(itup: *const u8) -> usize {
    (t_tid_posid(itup) & BT_OFFSET_MASK) as usize
}

// posting offset = ip_blkid of the alt TID (bi_hi << 16 | bi_lo).
#[inline]
pub unsafe fn itup_posting_offset(itup: *const u8) -> usize {
    let hi = itup.cast::<u16>().read_unaligned() as u32;
    let lo = itup.add(2).cast::<u16>().read_unaligned() as u32;
    (hi << 16 | lo) as usize
}

pub struct BTDedupState {
    pub deduplicate: bool,
    pub nmaxitems: i32,
    pub maxpostingsize: Size,
    pub base: *const u8,
    pub baseoff: OffsetNumber,
    pub basetupsize: Size,
    htids: [ItemPointerData; MAX_HTIDS],
    pub nhtids: usize,
    pub nitems: usize,
    pub phystupsize: Size,
    pub intervals: [BTDedupInterval; MaxIndexTuplesPerPage],
    pub nintervals: usize,
    scratch: [u8; BTMaxItemSize],
}

impl BTDedupState {
    pub fn new(maxpostingsize: Size) -> BTDedupState {
        // BLCKSZ = bottom-up deletion ("not really deduplicating"): form_posting unreached.
        debug_assert!(maxpostingsize <= BTMaxItemSize || maxpostingsize == BLCKSZ);
        // SAFETY: POD; one zeroed frame-local per pass (C pallocs state + htids).
        let mut state: BTDedupState = unsafe { core::mem::zeroed() };
        state.deduplicate = true;
        state.maxpostingsize = maxpostingsize;
        state.baseoff = InvalidOffsetNumber;
        state
    }

    /// _bt_dedup_start_pending.
    /// # Safety
    /// `base`: live non-pivot tuple, valid until the next `finish_pending`.
    pub unsafe fn start_pending(&mut self, base: *const u8, baseoff: OffsetNumber) {
        debug_assert!(self.nhtids == 0);
        debug_assert!(self.nitems == 0);
        debug_assert!(!itup_is_pivot(base));

        if !itup_is_posting(base) {
            self.htids[0] = base.cast::<ItemPointerData>().read_unaligned();
            self.nhtids = 1;
            self.basetupsize = itup_size(base);
        } else {
            let nposting = itup_nposting(base);
            let src = base.add(itup_posting_offset(base));
            core::ptr::copy_nonoverlapping(
                src,
                self.htids.as_mut_ptr().cast::<u8>(),
                IPD_SIZE * nposting,
            );
            self.nhtids = nposting;
            self.basetupsize = itup_posting_offset(base);
        }

        self.nitems = 1;
        self.base = base;
        self.baseoff = baseoff;
        self.phystupsize = maxalign(itup_size(base)) + core::mem::size_of::<ItemIdData>();
        self.intervals[self.nintervals].baseoff = self.baseoff;
    }

    /// _bt_dedup_save_htid.
    /// # Safety
    /// `itup`: live non-pivot tuple.
    pub unsafe fn save_htid(&mut self, itup: *const u8) -> bool {
        debug_assert!(!itup_is_pivot(itup));

        let (nhtids, htids): (usize, *const u8) = if !itup_is_posting(itup) {
            (1, itup)
        } else {
            (itup_nposting(itup), itup.add(itup_posting_offset(itup)))
        };

        // must match the size formed in finish_pending
        let mergedtupsz =
            maxalign(self.basetupsize + (self.nhtids + nhtids) * IPD_SIZE);

        if mergedtupsz > self.maxpostingsize {
            // counts for single value strategy only past 50 TIDs
            if self.nhtids > 50 {
                self.nmaxitems += 1;
            }
            return false;
        }

        self.nitems += 1;
        debug_assert!(self.nhtids + nhtids <= MAX_HTIDS);
        core::ptr::copy_nonoverlapping(
            htids,
            self.htids.as_mut_ptr().add(self.nhtids).cast::<u8>(),
            IPD_SIZE * nhtids,
        );
        self.nhtids += nhtids;
        self.phystupsize += maxalign(itup_size(itup)) + core::mem::size_of::<ItemIdData>();

        true
    }

    /// _bt_dedup_finish_pending; `Err` = C's failed-to-add-tuple elog(ERROR).
    /// # Safety
    /// `self.base` still live; `newpage` is the temp page.
    pub unsafe fn finish_pending(&mut self, newpage: &mut PageMut<'_>) -> Result<Size, ()> {
        debug_assert!(self.nitems > 0);
        debug_assert!(self.nitems <= self.nhtids);
        debug_assert!(self.intervals[self.nintervals].baseoff == self.baseoff);

        let tupoff = newpage.as_ref().max_offset_number() + 1;
        let spacesaving: Size;
        if self.nitems == 1 {
            let tuplesz = itup_size(self.base);
            debug_assert!(tuplesz == maxalign(tuplesz) && tuplesz <= BTMaxItemSize);
            let item = core::slice::from_raw_parts(self.base, tuplesz);
            if newpage.add_item(item, tupoff, 0).is_none() {
                return Err(());
            }
            spacesaving = 0;
        } else {
            let newsize = self.form_posting_scratch();

            self.intervals[self.nintervals].nitems = self.nitems as u16;

            if newpage.add_item(&self.scratch[..newsize], tupoff, 0).is_none() {
                return Err(());
            }

            spacesaving = self.phystupsize - (newsize + core::mem::size_of::<ItemIdData>());
            self.nintervals += 1;
            debug_assert!(spacesaving > 0 && spacesaving < BLCKSZ);
        }

        self.nhtids = 0;
        self.nitems = 0;
        self.phystupsize = 0;

        Ok(spacesaving)
    }

    // _bt_form_posting into owned scratch (C pallocs the final image)
    unsafe fn form_posting_scratch(&mut self) -> Size {
        let keysize = self.basetupsize;
        debug_assert!(keysize == maxalign(keysize));
        debug_assert!(self.nhtids > 1 && self.nhtids <= u16::MAX as usize);
        let newsize = maxalign(keysize + self.nhtids * IPD_SIZE);
        debug_assert!(newsize <= INDEX_SIZE_MASK as usize && newsize <= self.maxpostingsize);

        core::ptr::copy_nonoverlapping(self.base, self.scratch.as_mut_ptr(), keysize);
        let info = (t_info(self.scratch.as_ptr()) & !INDEX_SIZE_MASK)
            | newsize as u16
            | INDEX_ALT_TID_MASK;
        self.scratch[6..8].copy_from_slice(&info.to_ne_bytes());
        // BTreeTupleSetPosting: alt TID = (postingoffset blkid, nhtids|BT_IS_POSTING)
        self.scratch[0..2].copy_from_slice(&((keysize >> 16) as u16).to_ne_bytes());
        self.scratch[2..4].copy_from_slice(&((keysize & 0xffff) as u16).to_ne_bytes());
        self.scratch[4..6]
            .copy_from_slice(&(self.nhtids as u16 | BT_IS_POSTING).to_ne_bytes());
        core::ptr::copy_nonoverlapping(
            self.htids.as_ptr().cast::<u8>(),
            self.scratch.as_mut_ptr().add(keysize),
            self.nhtids * IPD_SIZE,
        );
        // palloc0 parity: MAXALIGN pad after the TID array is zero
        self.scratch[keysize + self.nhtids * IPD_SIZE..newsize].fill(0);
        newsize
    }

    /// _bt_sort_dedup_finish_pending forming leg: None = single item (caller adds base);
    /// Some((ptr, size, truncextra)) lives in scratch until the next start_pending.
    /// # Safety
    /// `self.base` still live.
    pub unsafe fn sort_finish_pending(&mut self) -> Option<(*const u8, usize, Size)> {
        debug_assert!(self.nitems > 0);
        debug_assert!(self.nitems <= self.nhtids);

        let out = if self.nitems == 1 {
            None
        } else {
            let newsize = self.form_posting_scratch();
            Some((self.scratch.as_ptr(), newsize, newsize - self.basetupsize))
        };

        self.nmaxitems = 0;
        self.nhtids = 0;
        self.nitems = 0;
        self.phystupsize = 0;

        out
    }

    pub fn intervals_bytes(&self) -> &[u8] {
        // SAFETY: BTDedupInterval is repr(C) {u16,u16}, size 4, no padding.
        unsafe {
            core::slice::from_raw_parts(
                self.intervals.as_ptr().cast::<u8>(),
                self.nintervals * core::mem::size_of::<BTDedupInterval>(),
            )
        }
    }
}
