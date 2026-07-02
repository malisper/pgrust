use alloc::vec::Vec;

use ::types_core::{
    uint16, uint32, uint8, BlockNumber, InvalidBlockNumber, OffsetNumber, Size, XLogRecPtr, BLCKSZ,
};
use ::types_error::PgError;

pub type ItemOffset = uint16;
pub type ItemLength = uint16;

pub const LP_UNUSED: u32 = 0;
pub const LP_NORMAL: u32 = 1;
pub const LP_REDIRECT: u32 = 2;
pub const LP_DEAD: u32 = 3;

// C bitfield word (lp_off:15, lp_flags:2, lp_len:15), LSB-first as clang lays
// it out on the supported little-endian targets.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ItemIdData {
    raw: uint32,
}

impl ItemIdData {
    #[inline]
    pub const fn new(lp_off: ItemOffset, lp_flags: u32, lp_len: ItemLength) -> Self {
        Self {
            raw: (lp_off as uint32 & 0x7fff)
                | ((lp_flags & 0x0003) << 15)
                | ((lp_len as uint32 & 0x7fff) << 17),
        }
    }

    #[inline]
    pub const fn lp_off(&self) -> ItemOffset {
        (self.raw & 0x7fff) as ItemOffset
    }

    #[inline]
    pub const fn lp_flags(&self) -> u32 {
        (self.raw >> 15) & 0x0003
    }

    #[inline]
    pub const fn lp_len(&self) -> ItemLength {
        ((self.raw >> 17) & 0x7fff) as ItemLength
    }

    #[inline]
    pub fn set_unused(&mut self) {
        *self = Self::new(0, LP_UNUSED, 0);
    }

    #[inline]
    pub fn set_normal(&mut self, off: ItemOffset, len: ItemLength) {
        *self = Self::new(off, LP_NORMAL, len);
    }

    // PageIndexTupleOverwrite writes lp_off/lp_len preserving lp_flags.
    #[inline]
    pub fn set_storage(&mut self, off: ItemOffset, len: ItemLength) {
        *self = Self::new(off, self.lp_flags(), len);
    }

    #[inline]
    pub fn set_redirect(&mut self, link: OffsetNumber) {
        *self = Self::new(link, LP_REDIRECT, 0);
    }

    #[inline]
    pub fn set_dead(&mut self) {
        *self = Self::new(0, LP_DEAD, 0);
    }

    #[inline]
    pub fn mark_dead(&mut self) {
        *self = Self::new(self.lp_off(), LP_DEAD, self.lp_len());
    }

    #[inline]
    pub const fn is_used(&self) -> bool {
        self.lp_flags() != LP_UNUSED
    }

    #[inline]
    pub const fn is_normal(&self) -> bool {
        self.lp_flags() == LP_NORMAL
    }

    #[inline]
    pub const fn is_redirected(&self) -> bool {
        self.lp_flags() == LP_REDIRECT
    }

    #[inline]
    pub const fn is_dead(&self) -> bool {
        self.lp_flags() == LP_DEAD
    }

    #[inline]
    pub const fn has_storage(&self) -> bool {
        self.lp_len() != 0
    }
}

const _: () = assert!(core::mem::size_of::<ItemIdData>() == 4);

// Two uint32s to avoid alignment assumptions in the on-disk header.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PageXLogRecPtr {
    pub xlogid: uint32,
    pub xrecoff: uint32,
}

impl PageXLogRecPtr {
    #[inline]
    pub const fn from_lsn(lsn: XLogRecPtr) -> Self {
        Self {
            xlogid: (lsn >> 32) as uint32,
            xrecoff: lsn as uint32,
        }
    }

    #[inline]
    pub const fn lsn(&self) -> XLogRecPtr {
        ((self.xlogid as XLogRecPtr) << 32) | self.xrecoff as XLogRecPtr
    }
}

const _: () = assert!(core::mem::size_of::<PageXLogRecPtr>() == 8);

pub const MaxOffsetNumber: OffsetNumber = (BLCKSZ / core::mem::size_of::<ItemIdData>()) as u16;

pub const MovedPartitionsOffsetNumber: OffsetNumber = 0xfffd;
pub const MovedPartitionsBlockNumber: BlockNumber = InvalidBlockNumber;

pub const PD_HAS_FREE_LINES: uint16 = 0x0001;
pub const PD_PAGE_FULL: uint16 = 0x0002;
pub const PD_ALL_VISIBLE: uint16 = 0x0004;
pub const PD_VALID_FLAG_BITS: uint16 = 0x0007;

pub const PG_PAGE_LAYOUT_VERSION: uint8 = 4;

pub const SizeOfPageHeaderData: Size = 24;

pub const PAI_OVERWRITE: i32 = 1 << 0;
pub const PAI_IS_HEAP: i32 = 1 << 1;

pub const PIV_LOG_WARNING: i32 = 1 << 0;
pub const PIV_LOG_LOG: i32 = 1 << 1;
pub const PIV_IGNORE_CHECKSUM_FAILURE: i32 = 1 << 2;

pub const SizeofHeapTupleHeader: usize = 23;

pub const MaxHeapTuplesPerPage: usize =
    (BLCKSZ - SizeOfPageHeaderData) / (24 + core::mem::size_of::<ItemIdData>());

pub const MaxHeapTupleSize: Size = BLCKSZ - {
    let raw = SizeOfPageHeaderData + core::mem::size_of::<ItemIdData>();
    (raw + 7) & !7
};

pub const PG_IO_ALIGN_SIZE: usize = 4096;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PageHeaderData {
    pub pd_lsn: PageXLogRecPtr,
    pub pd_checksum: uint16,
    pub pd_flags: uint16,
    pub pd_lower: uint16,
    pub pd_upper: uint16,
    pub pd_special: uint16,
    pub pd_pagesize_version: uint16,
    pub pd_prune_xid: uint32,
    pub pd_linp: [ItemIdData; 0],
}

const _: () = assert!(core::mem::offset_of!(PageHeaderData, pd_lower) == 12);
const _: () = assert!(core::mem::offset_of!(PageHeaderData, pd_prune_xid) == 20);
const _: () = assert!(core::mem::offset_of!(PageHeaderData, pd_linp) == SizeOfPageHeaderData);

// C's `Page`, read view. All access chains from the raw pointer (no whole-page
// `&[u8]`), so C's tolerated hint-bit stores don't invalidate the view.
#[derive(Clone, Copy)]
pub struct PageRef<'a> {
    ptr: core::ptr::NonNull<u8>,
    _page: core::marker::PhantomData<&'a [u8]>,
}

impl<'a> PageRef<'a> {
    /// # Safety
    /// `ptr` is a live, MAXALIGN-aligned, `BLCKSZ`-readable page image for all
    /// of `'a` (buffer pages: pinned for `'a`); concurrent writes follow C's locking contract.
    #[inline]
    pub unsafe fn from_raw(ptr: core::ptr::NonNull<u8>) -> PageRef<'a> {
        PageRef {
            ptr,
            _page: core::marker::PhantomData,
        }
    }

    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    #[inline]
    fn read_u16(&self, off: usize) -> uint16 {
        debug_assert!(off + 2 <= BLCKSZ && off % 2 == 0);
        // SAFETY: in-bounds, 2-aligned (from_raw contract).
        unsafe { self.ptr.as_ptr().add(off).cast::<uint16>().read() }
    }

    #[inline]
    pub fn max_offset_number(&self) -> OffsetNumber {
        let pd_lower = self.read_u16(core::mem::offset_of!(PageHeaderData, pd_lower)) as usize;
        if pd_lower <= SizeOfPageHeaderData {
            0
        } else {
            ((pd_lower - SizeOfPageHeaderData) / core::mem::size_of::<ItemIdData>())
                as OffsetNumber
        }
    }

    #[inline]
    pub fn is_all_visible(&self) -> bool {
        (self.read_u16(core::mem::offset_of!(PageHeaderData, pd_flags)) & PD_ALL_VISIBLE) != 0
    }

    #[inline]
    pub fn is_new(&self) -> bool {
        self.read_u16(core::mem::offset_of!(PageHeaderData, pd_upper)) == 0
    }

    /// `*PageGetItemId(page, offnum)` by value; hard-bounded to the page image.
    #[inline]
    pub fn item_id(&self, offnum: OffsetNumber) -> ItemIdData {
        let offnum = offnum as usize;
        assert!(
            offnum >= 1
                && SizeOfPageHeaderData + offnum * core::mem::size_of::<ItemIdData>() <= BLCKSZ
        );
        let off = SizeOfPageHeaderData + (offnum - 1) * core::mem::size_of::<ItemIdData>();
        // SAFETY: in-bounds (checked above), 4-aligned (header is MAXALIGNed).
        unsafe { self.ptr.as_ptr().add(off).cast::<ItemIdData>().read() }
    }

    /// `PageGetItem` + `ItemIdGetLength` as raw parts (raw: hint-bit writes stay legal).
    #[inline]
    pub fn item_raw(&self, id: ItemIdData) -> (*const u8, u32) {
        let off = id.lp_off() as usize;
        let len = id.lp_len() as usize;
        assert!(off >= SizeOfPageHeaderData && off + len <= BLCKSZ, "corrupt line pointer");
        // SAFETY: in-bounds (checked above).
        (unsafe { self.ptr.as_ptr().add(off) }, len as u32)
    }
}

// Owned local scratch page: PageGetTempPage*'s palloc(pageSize), always a full
// BLCKSZ buffer even when pd_pagesize is smaller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageTemp {
    bytes: Vec<u8>,
}

impl PageTemp {
    pub fn new(page_size: Size) -> Result<Self, PgError> {
        if page_size == 0 || page_size > BLCKSZ {
            return Err(PgError::error(
                "PageTemp page size is out of range (must be 1..=BLCKSZ)",
            ));
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(BLCKSZ)
            .map_err(|_| PgError::error("PageTemp allocation failed"))?;
        bytes.resize(BLCKSZ, 0);
        Ok(Self { bytes })
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn as_mut_bytes(&mut self) -> &mut [u8] {
        &mut self.bytes
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn item_id_bit_layout_matches_c_bitfield() {
        let id = ItemIdData::new(0x1234, LP_NORMAL, 0x0abc);
        assert_eq!(id.raw, 0x1234 | (1 << 15) | (0x0abc << 17));
        assert_eq!(id.lp_off(), 0x1234);
        assert_eq!(id.lp_flags(), LP_NORMAL);
        assert_eq!(id.lp_len(), 0x0abc);

        let max = ItemIdData::new(0x7fff, LP_DEAD, 0x7fff);
        assert_eq!(max.raw, u32::MAX);
        assert_eq!(max.lp_off(), 0x7fff);
        assert_eq!(max.lp_flags(), LP_DEAD);
        assert_eq!(max.lp_len(), 0x7fff);
    }

    #[test]
    fn item_id_state_transitions() {
        let mut id = ItemIdData::new(100, LP_NORMAL, 60);
        assert!(id.is_used() && id.is_normal() && id.has_storage());
        id.mark_dead();
        assert!(id.is_dead());
        assert_eq!((id.lp_off(), id.lp_len()), (100, 60));
        id.set_storage(200, 80);
        assert!(id.is_dead());
        assert_eq!((id.lp_off(), id.lp_len()), (200, 80));
        id.set_redirect(7);
        assert!(id.is_redirected());
        assert_eq!((id.lp_off(), id.lp_len()), (7, 0));
        id.set_unused();
        assert!(!id.is_used() && !id.has_storage());
    }

    #[test]
    fn page_geometry_matches_headers() {
        assert_eq!(MaxOffsetNumber, 2048);
        assert_eq!(MaxHeapTuplesPerPage, 291);
        assert_eq!(MaxHeapTupleSize, 8160);
        assert_eq!(SizeOfPageHeaderData, 24);
    }

    #[test]
    fn page_lsn_round_trip() {
        let lsn: XLogRecPtr = 0x0102_0304_0506_0708;
        let p = PageXLogRecPtr::from_lsn(lsn);
        assert_eq!(p.xlogid, 0x0102_0304);
        assert_eq!(p.xrecoff, 0x0506_0708);
        assert_eq!(p.lsn(), lsn);
    }

    #[test]
    fn page_temp_bounds() {
        assert!(PageTemp::new(0).is_err());
        assert!(PageTemp::new(BLCKSZ + 1).is_err());
        let p = PageTemp::new(512).unwrap();
        assert_eq!(p.as_bytes().len(), BLCKSZ);
    }
}
