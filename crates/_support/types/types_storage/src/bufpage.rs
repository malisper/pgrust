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

    #[inline]
    pub fn pd_flags(&self) -> uint16 {
        self.read_u16(core::mem::offset_of!(PageHeaderData, pd_flags))
    }

    #[inline]
    pub fn pd_lower(&self) -> uint16 {
        self.read_u16(core::mem::offset_of!(PageHeaderData, pd_lower))
    }

    #[inline]
    pub fn pd_upper(&self) -> uint16 {
        self.read_u16(core::mem::offset_of!(PageHeaderData, pd_upper))
    }

    #[inline]
    pub fn pd_special(&self) -> uint16 {
        self.read_u16(core::mem::offset_of!(PageHeaderData, pd_special))
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        (self.pd_flags() & PD_PAGE_FULL) != 0
    }

    #[inline]
    pub fn has_free_line_pointers(&self) -> bool {
        (self.pd_flags() & PD_HAS_FREE_LINES) != 0
    }

    #[inline]
    pub fn prune_xid(&self) -> uint32 {
        let off = core::mem::offset_of!(PageHeaderData, pd_prune_xid);
        // SAFETY: in-bounds, 4-aligned (from_raw contract).
        unsafe { self.ptr.as_ptr().add(off).cast::<uint32>().read() }
    }

    #[inline]
    pub fn lsn(&self) -> XLogRecPtr {
        // SAFETY: in-bounds; PageXLogRecPtr is two u32s (4-aligned).
        let p = unsafe { self.ptr.as_ptr().cast::<PageXLogRecPtr>().read() };
        p.lsn()
    }

    /// `PageGetFreeSpace`: usable space assuming one new line pointer.
    pub fn free_space(&self) -> Size {
        let space = self.pd_upper() as isize - self.pd_lower() as isize;
        if space < core::mem::size_of::<ItemIdData>() as isize {
            return 0;
        }
        space as Size - core::mem::size_of::<ItemIdData>()
    }

    /// `PageGetExactFreeSpace`.
    pub fn exact_free_space(&self) -> Size {
        let space = self.pd_upper() as isize - self.pd_lower() as isize;
        if space < 0 {
            0
        } else {
            space as Size
        }
    }

    /// `PageGetHeapFreeSpace`: 0 once the heap line-pointer limit is reached
    /// with no recyclable LP_UNUSED slot.
    pub fn heap_free_space(&self) -> Size {
        let space = self.free_space();
        if space > 0 {
            let nline = self.max_offset_number() as usize;
            if nline >= MaxHeapTuplesPerPage {
                if self.has_free_line_pointers() {
                    for off in 1..=nline as OffsetNumber {
                        let id = self.item_id(off);
                        if !id.is_used() {
                            return space;
                        }
                    }
                }
                return 0;
            }
        }
        space
    }
}

// C's `Page`, write view: requires the exclusive content lock (or a local /
// not-yet-visible page). The page-write kernel under safe heap DML.
pub struct PageMut<'a> {
    ptr: core::ptr::NonNull<u8>,
    _page: core::marker::PhantomData<&'a mut [u8]>,
}

impl<'a> PageMut<'a> {
    /// # Safety
    /// `ptr` is a live, MAXALIGN-aligned, `BLCKSZ`-writable page image,
    /// exclusively owned for `'a` (C: exclusive buffer content lock held).
    #[inline]
    pub unsafe fn from_raw(ptr: core::ptr::NonNull<u8>) -> PageMut<'a> {
        PageMut {
            ptr,
            _page: core::marker::PhantomData,
        }
    }

    #[inline]
    pub fn as_ref(&self) -> PageRef<'_> {
        // SAFETY: same image, narrower (read) view for a shorter borrow.
        unsafe { PageRef::from_raw(self.ptr) }
    }

    #[inline]
    fn write_u16(&mut self, off: usize, v: uint16) {
        debug_assert!(off + 2 <= BLCKSZ && off % 2 == 0);
        // SAFETY: in-bounds, 2-aligned (from_raw contract).
        unsafe { self.ptr.as_ptr().add(off).cast::<uint16>().write(v) }
    }

    #[inline]
    pub fn set_pd_lower(&mut self, v: uint16) {
        self.write_u16(core::mem::offset_of!(PageHeaderData, pd_lower), v);
    }

    #[inline]
    pub fn set_pd_upper(&mut self, v: uint16) {
        self.write_u16(core::mem::offset_of!(PageHeaderData, pd_upper), v);
    }

    #[inline]
    pub fn set_pd_flags(&mut self, v: uint16) {
        self.write_u16(core::mem::offset_of!(PageHeaderData, pd_flags), v);
    }

    #[inline]
    pub fn clear_all_visible(&mut self) {
        self.set_pd_flags(self.as_ref().pd_flags() & !PD_ALL_VISIBLE);
    }

    #[inline]
    pub fn set_all_visible(&mut self) {
        self.set_pd_flags(self.as_ref().pd_flags() | PD_ALL_VISIBLE);
    }

    #[inline]
    pub fn set_full(&mut self) {
        self.set_pd_flags(self.as_ref().pd_flags() | PD_PAGE_FULL);
    }

    #[inline]
    pub fn set_lsn(&mut self, lsn: XLogRecPtr) {
        let v = PageXLogRecPtr::from_lsn(lsn);
        // SAFETY: in-bounds at offset 0; two 4-aligned u32 stores.
        unsafe { self.ptr.as_ptr().cast::<PageXLogRecPtr>().write(v) }
    }

    #[inline]
    pub fn set_prune_xid(&mut self, xid: uint32) {
        let off = core::mem::offset_of!(PageHeaderData, pd_prune_xid);
        // SAFETY: in-bounds, 4-aligned.
        unsafe { self.ptr.as_ptr().add(off).cast::<uint32>().write(xid) }
    }

    #[inline]
    pub fn set_item_id(&mut self, offnum: OffsetNumber, id: ItemIdData) {
        let offnum = offnum as usize;
        assert!(
            offnum >= 1
                && SizeOfPageHeaderData + offnum * core::mem::size_of::<ItemIdData>() <= BLCKSZ
        );
        let off = SizeOfPageHeaderData + (offnum - 1) * core::mem::size_of::<ItemIdData>();
        // SAFETY: in-bounds (checked above), 4-aligned.
        unsafe { self.ptr.as_ptr().add(off).cast::<ItemIdData>().write(id) }
    }

    /// `PageInit(page, BLCKSZ, specialSize)`.
    pub fn init(&mut self, special_size: Size) {
        let special_size = (special_size + 7) & !7;
        assert!(special_size < BLCKSZ - SizeOfPageHeaderData);
        // SAFETY: whole-page zero fill within the from_raw contract.
        unsafe { core::ptr::write_bytes(self.ptr.as_ptr(), 0, BLCKSZ) };
        let special = (BLCKSZ - special_size) as uint16;
        self.set_pd_flags(0);
        self.set_pd_lower(SizeOfPageHeaderData as uint16);
        self.set_pd_upper(special);
        self.write_u16(core::mem::offset_of!(PageHeaderData, pd_special), special);
        self.write_u16(
            core::mem::offset_of!(PageHeaderData, pd_pagesize_version),
            BLCKSZ as uint16 | PG_PAGE_LAYOUT_VERSION as uint16,
        );
        self.set_prune_xid(0);
    }

    /// `PageAddItemExtended`; `None` is C's `InvalidOffsetNumber` (the C
    /// WARNING text lives at the caller). Panics on corrupt page pointers
    /// (C ereport PANIC).
    pub fn add_item(
        &mut self,
        item: &[u8],
        offset_number: OffsetNumber,
        flags: i32,
    ) -> Option<OffsetNumber> {
        let overwrite = (flags & PAI_OVERWRITE) != 0;
        let is_heap = (flags & PAI_IS_HEAP) != 0;
        let r = self.as_ref();
        let pd_lower = r.pd_lower() as usize;
        let pd_upper = r.pd_upper() as usize;
        let pd_special = r.pd_special() as usize;
        assert!(
            pd_lower >= SizeOfPageHeaderData
                && pd_lower <= pd_upper
                && pd_upper <= pd_special
                && pd_special <= BLCKSZ,
            "corrupted page pointers: lower = {pd_lower}, upper = {pd_upper}, special = {pd_special}"
        );

        let limit = r.max_offset_number() + 1;
        let mut offset_number = offset_number;
        let mut needshuffle = false;
        if offset_number != 0 {
            if offset_number < limit {
                let id = r.item_id(offset_number);
                if overwrite {
                    if id.is_used() || id.has_storage() {
                        return None;
                    }
                } else {
                    needshuffle = true;
                }
            }
        } else {
            if r.has_free_line_pointers() {
                for off in 1..limit {
                    let id = r.item_id(off);
                    if !id.is_used() && !id.has_storage() {
                        offset_number = off;
                        break;
                    }
                }
                if offset_number == 0 {
                    self.set_pd_flags(r.pd_flags() & !PD_HAS_FREE_LINES);
                }
            }
            if offset_number == 0 {
                offset_number = limit;
            }
        }

        if offset_number > limit {
            return None;
        }
        if is_heap && offset_number as usize > MaxHeapTuplesPerPage {
            return None;
        }

        let lower = if offset_number == limit || needshuffle {
            pd_lower + core::mem::size_of::<ItemIdData>()
        } else {
            pd_lower
        };
        let aligned_size = (item.len() + 7) & !7;
        if pd_upper < aligned_size {
            return None;
        }
        let upper = pd_upper - aligned_size;
        if lower > upper {
            return None;
        }

        if needshuffle {
            let base = SizeOfPageHeaderData;
            let idx = (offset_number - 1) as usize;
            let n = (limit - offset_number) as usize;
            // SAFETY: source and destination line-pointer ranges are within
            // pd_lower (validated above); overlapping move.
            unsafe {
                let src = self.ptr.as_ptr().add(base + idx * 4).cast::<ItemIdData>();
                core::ptr::copy(src, src.add(1), n);
            }
        }

        self.set_item_id(
            offset_number,
            ItemIdData::new(upper as ItemOffset, LP_NORMAL, item.len() as ItemLength),
        );
        // SAFETY: upper + len <= pd_upper(old) <= pd_special <= BLCKSZ; item
        // region is disjoint from the header/line array by lower <= upper.
        unsafe {
            core::ptr::copy_nonoverlapping(item.as_ptr(), self.ptr.as_ptr().add(upper), item.len())
        };
        self.set_pd_lower(lower as uint16);
        self.set_pd_upper(upper as uint16);

        Some(offset_number)
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

    #[repr(align(8))]
    struct AlignedPage([u8; BLCKSZ]);

    fn temp_page() -> alloc::boxed::Box<AlignedPage> {
        alloc::boxed::Box::new(AlignedPage([0u8; BLCKSZ]))
    }

    fn page_mut(t: &mut AlignedPage) -> PageMut<'_> {
        let ptr = core::ptr::NonNull::new(t.0.as_mut_ptr()).unwrap();
        // SAFETY: owned MAXALIGNed BLCKSZ image, exclusively borrowed.
        unsafe { PageMut::from_raw(ptr) }
    }

    #[test]
    fn page_init_layout() {
        let mut t = temp_page();
        let mut pm = page_mut(&mut t);
        pm.init(0);
        let r = pm.as_ref();
        assert_eq!(r.pd_lower() as usize, SizeOfPageHeaderData);
        assert_eq!(r.pd_upper() as usize, BLCKSZ);
        assert_eq!(r.pd_special() as usize, BLCKSZ);
        assert_eq!(r.max_offset_number(), 0);
        assert_eq!(r.free_space(), BLCKSZ - SizeOfPageHeaderData - 4);
        assert!(!r.is_all_visible() && !r.is_full());

        let mut pm = page_mut(&mut t);
        pm.init(16);
        assert_eq!(pm.as_ref().pd_special() as usize, BLCKSZ - 16);
        assert_eq!(pm.as_ref().pd_upper() as usize, BLCKSZ - 16);
    }

    #[test]
    fn add_item_appends_and_copies() {
        let mut t = temp_page();
        let mut pm = page_mut(&mut t);
        pm.init(0);
        let item1 = [0xAAu8; 60];
        let item2 = [0xBBu8; 33];
        let off1 = pm.add_item(&item1, 0, PAI_IS_HEAP).unwrap();
        let off2 = pm.add_item(&item2, 0, PAI_IS_HEAP).unwrap();
        assert_eq!((off1, off2), (1, 2));
        let r = pm.as_ref();
        assert_eq!(r.max_offset_number(), 2);
        let id1 = r.item_id(1);
        let id2 = r.item_id(2);
        assert_eq!(id1.lp_len(), 60);
        assert_eq!(id1.lp_off() as usize, BLCKSZ - 64);
        assert_eq!(id2.lp_len(), 33);
        assert_eq!(id2.lp_off() as usize, BLCKSZ - 64 - 40);
        let (p1, l1) = r.item_raw(id1);
        // SAFETY: item_raw bounds-checked.
        assert_eq!(unsafe { core::slice::from_raw_parts(p1, l1 as usize) }, &item1);
        assert_eq!(
            r.free_space(),
            BLCKSZ - SizeOfPageHeaderData - 2 * 4 - 104 - 4
        );
    }

    #[test]
    fn add_item_rejects_when_full() {
        let mut t = temp_page();
        let mut pm = page_mut(&mut t);
        pm.init(0);
        let big = [0u8; 4000];
        assert!(pm.add_item(&big, 0, PAI_IS_HEAP).is_some());
        assert!(pm.add_item(&big, 0, PAI_IS_HEAP).is_some());
        assert!(pm.add_item(&big, 0, PAI_IS_HEAP).is_none());
        // offnum beyond limit refused
        assert!(pm.add_item(&[0u8; 8], 9, 0).is_none());
    }

    #[test]
    fn add_item_recycles_unused_line_pointer() {
        let mut t = temp_page();
        let mut pm = page_mut(&mut t);
        pm.init(0);
        let item = [0x11u8; 24];
        let o1 = pm.add_item(&item, 0, PAI_IS_HEAP).unwrap();
        let _o2 = pm.add_item(&item, 0, PAI_IS_HEAP).unwrap();
        let mut id = pm.as_ref().item_id(o1);
        id.set_unused();
        pm.set_item_id(o1, id);
        pm.set_pd_flags(pm.as_ref().pd_flags() | PD_HAS_FREE_LINES);
        let o3 = pm.add_item(&item, 0, PAI_IS_HEAP).unwrap();
        assert_eq!(o3, o1);
        assert_eq!(pm.as_ref().max_offset_number(), 2);
    }

    #[test]
    fn add_item_shuffles_line_pointers() {
        let mut t = temp_page();
        let mut pm = page_mut(&mut t);
        pm.init(0);
        let a = [0xAAu8; 16];
        let b = [0xBBu8; 16];
        let c = [0xCCu8; 16];
        pm.add_item(&a, 0, 0).unwrap();
        pm.add_item(&b, 0, 0).unwrap();
        // insert at 1, shifting a/b to 2/3 (index redo shape)
        assert_eq!(pm.add_item(&c, 1, 0), Some(1));
        let r = pm.as_ref();
        assert_eq!(r.max_offset_number(), 3);
        let get = |off| {
            let (p, l) = r.item_raw(r.item_id(off));
            // SAFETY: item_raw bounds-checked.
            (unsafe { core::slice::from_raw_parts(p, l as usize) })[0]
        };
        assert_eq!((get(1), get(2), get(3)), (0xCCu8, 0xAAu8, 0xBBu8));
    }

    #[test]
    fn header_flag_and_lsn_writes() {
        let mut t = temp_page();
        let mut pm = page_mut(&mut t);
        pm.init(0);
        pm.set_all_visible();
        assert!(pm.as_ref().is_all_visible());
        pm.clear_all_visible();
        assert!(!pm.as_ref().is_all_visible());
        pm.set_full();
        assert!(pm.as_ref().is_full());
        pm.set_lsn(0x0102_0304_0506_0708);
        assert_eq!(pm.as_ref().lsn(), 0x0102_0304_0506_0708);
        pm.set_prune_xid(77);
        assert_eq!(pm.as_ref().prune_xid(), 77);
    }

    #[test]
    fn heap_free_space_line_pointer_limit() {
        let mut t = temp_page();
        let mut pm = page_mut(&mut t);
        pm.init(0);
        let tiny = [0u8; 8];
        for _ in 0..MaxHeapTuplesPerPage {
            pm.add_item(&tiny, 0, PAI_IS_HEAP).unwrap();
        }
        let r = pm.as_ref();
        assert!(r.free_space() > 0);
        assert_eq!(r.heap_free_space(), 0);
        assert!(pm.add_item(&tiny, 0, PAI_IS_HEAP).is_none());
        // one recyclable slot restores heap free space
        let mut id = pm.as_ref().item_id(5);
        id.set_unused();
        pm.set_item_id(5, id);
        pm.set_pd_flags(pm.as_ref().pd_flags() | PD_HAS_FREE_LINES);
        assert!(pm.as_ref().heap_free_space() > 0);
        assert_eq!(pm.add_item(&tiny, 0, PAI_IS_HEAP), Some(5));
    }
}
