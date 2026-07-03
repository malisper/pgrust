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

pub const MaxIndexTuplesPerPage: usize =
    (BLCKSZ - SizeOfPageHeaderData) / (16 + core::mem::size_of::<ItemIdData>());

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
        assert!(
            offnum >= 1
                && SizeOfPageHeaderData
                    + offnum as usize * core::mem::size_of::<ItemIdData>()
                    <= BLCKSZ
        );
        // SAFETY: bounds checked above.
        unsafe { self.item_id_unchecked(offnum) }
    }

    /// C's 2-insn `PageGetItemId`.
    /// # Safety
    /// `offnum >= 1` and `SizeOfPageHeaderData + offnum * 4 <= BLCKSZ`: any
    /// `offnum <= max_offset_number()` after ONE per-page `pd_lower` check.
    #[inline]
    pub unsafe fn item_id_unchecked(&self, offnum: OffsetNumber) -> ItemIdData {
        let offnum = offnum as usize;
        debug_assert!(
            offnum >= 1
                && SizeOfPageHeaderData + offnum * core::mem::size_of::<ItemIdData>() <= BLCKSZ
        );
        let off = SizeOfPageHeaderData + (offnum - 1) * core::mem::size_of::<ItemIdData>();
        // SAFETY: in-bounds (caller contract), 4-aligned (header is MAXALIGNed).
        unsafe { self.ptr.as_ptr().add(off).cast::<ItemIdData>().read() }
    }

    /// `PageGetItem` + `ItemIdGetLength` as raw parts (raw: hint-bit writes stay legal).
    #[inline]
    pub fn item_raw(&self, id: ItemIdData) -> (*const u8, u32) {
        let off = id.lp_off() as usize;
        let len = id.lp_len() as usize;
        assert!(off >= SizeOfPageHeaderData && off + len <= BLCKSZ, "corrupt line pointer");
        // SAFETY: bounds checked above.
        unsafe { self.item_raw_unchecked(id) }
    }

    /// C's `PageGetItem`: no per-tuple bounds re-check.
    /// # Safety
    /// `id` came from THIS page with its item within the image — the page
    /// invariant every writer keeps and C reads unchecked (LP_NORMAL).
    #[inline]
    pub unsafe fn item_raw_unchecked(&self, id: ItemIdData) -> (*const u8, u32) {
        let off = id.lp_off() as usize;
        let len = id.lp_len() as usize;
        debug_assert!(
            off >= SizeOfPageHeaderData && off + len <= BLCKSZ,
            "corrupt line pointer"
        );
        // SAFETY: in-bounds (caller contract: page invariant).
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
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
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
    pub fn clear_full(&mut self) {
        self.set_pd_flags(self.as_ref().pd_flags() & !PD_PAGE_FULL);
    }

    #[inline]
    pub fn set_has_free_line_pointers(&mut self) {
        self.set_pd_flags(self.as_ref().pd_flags() | PD_HAS_FREE_LINES);
    }

    #[inline]
    pub fn clear_has_free_line_pointers(&mut self) {
        self.set_pd_flags(self.as_ref().pd_flags() & !PD_HAS_FREE_LINES);
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

    /// `PageIndexTupleOverwrite`; false = new tuple would overflow the page.
    /// Panics on corrupt page/line pointers (C ereport ERROR, DATA_CORRUPTED).
    pub fn index_tuple_overwrite(&mut self, offnum: OffsetNumber, newtup: &[u8]) -> bool {
        let r = self.as_ref();
        let pd_lower = r.pd_lower() as usize;
        let pd_upper = r.pd_upper() as usize;
        let pd_special = r.pd_special() as usize;
        assert!(
            pd_lower >= SizeOfPageHeaderData
                && pd_lower <= pd_upper
                && pd_upper <= pd_special
                && pd_special <= BLCKSZ
                && pd_special == (pd_special + 7) & !7,
            "corrupted page pointers: lower = {pd_lower}, upper = {pd_upper}, special = {pd_special}"
        );

        let itemcount = r.max_offset_number();
        assert!(offnum >= 1 && offnum <= itemcount, "invalid index offnum: {offnum}");

        let tupid = r.item_id(offnum);
        debug_assert!(tupid.has_storage());
        let oldsize = tupid.lp_len() as usize;
        let offset = tupid.lp_off() as usize;
        assert!(
            offset >= pd_upper && offset + oldsize <= pd_special && offset == (offset + 7) & !7,
            "corrupted line pointer: offset = {offset}, size = {oldsize}"
        );

        let oldsize = (oldsize + 7) & !7;
        let alignednewsize = (newtup.len() + 7) & !7;
        if alignednewsize > oldsize + (pd_upper - pd_lower) {
            return false;
        }

        let size_diff = oldsize as isize - alignednewsize as isize;
        if size_diff != 0 {
            // SAFETY: [pd_upper, offset) moved by size_diff stays within
            // [pd_lower, pd_special) by the overflow check above.
            unsafe {
                let addr = self.ptr.as_ptr().add(pd_upper);
                core::ptr::copy(addr, addr.offset(size_diff), offset - pd_upper);
            }
            self.set_pd_upper((pd_upper as isize + size_diff) as uint16);
            for i in 1..=itemcount {
                let mut ii = self.as_ref().item_id(i);
                if ii.has_storage() && (ii.lp_off() as usize) <= offset {
                    ii.set_storage(
                        (ii.lp_off() as isize + size_diff) as ItemOffset,
                        ii.lp_len(),
                    );
                    self.set_item_id(i, ii);
                }
            }
        }

        let mut tupid = self.as_ref().item_id(offnum);
        let newoff = (offset as isize + size_diff) as ItemOffset;
        tupid.set_storage(newoff, newtup.len() as ItemLength);
        self.set_item_id(offnum, tupid);
        // SAFETY: destination [newoff, newoff+len) within tuple space by the
        // checks above.
        unsafe {
            core::ptr::copy_nonoverlapping(
                newtup.as_ptr(),
                self.ptr.as_ptr().add(newoff as usize),
                newtup.len(),
            )
        };
        true
    }

    /// `PageIndexTupleDeleteNoCompact`: unused line pointer instead of
    /// compaction (removed outright only when last). Panics on corruption
    /// (C ereport ERROR, DATA_CORRUPTED).
    pub fn index_tuple_delete_no_compact(&mut self, offnum: OffsetNumber) {
        let r = self.as_ref();
        let pd_lower = r.pd_lower() as usize;
        let pd_upper = r.pd_upper() as usize;
        let pd_special = r.pd_special() as usize;
        assert!(
            pd_lower >= SizeOfPageHeaderData
                && pd_lower <= pd_upper
                && pd_upper <= pd_special
                && pd_special <= BLCKSZ
                && pd_special == (pd_special + 7) & !7,
            "corrupted page pointers: lower = {pd_lower}, upper = {pd_upper}, special = {pd_special}"
        );

        let mut nline = r.max_offset_number();
        assert!(offnum >= 1 && offnum <= nline, "invalid index offnum: {offnum}");

        let tup = r.item_id(offnum);
        debug_assert!(tup.has_storage());
        let size = tup.lp_len() as usize;
        let offset = tup.lp_off() as usize;
        assert!(
            offset >= pd_upper && offset + size <= pd_special && offset == (offset + 7) & !7,
            "corrupted line pointer: offset = {offset}, size = {size}"
        );

        let size = (size + 7) & !7;

        if offnum < nline {
            let mut id = tup;
            id.set_unused();
            self.set_item_id(offnum, id);
        } else {
            self.set_pd_lower((pd_lower - core::mem::size_of::<ItemIdData>()) as uint16);
            nline -= 1;
        }

        if offset > pd_upper {
            // SAFETY: [pd_upper, offset) shifts up by `size`, staying within
            // [pd_upper, pd_special) per the line-pointer check above.
            unsafe {
                let addr = self.ptr.as_ptr().add(pd_upper);
                core::ptr::copy(addr, addr.add(size), offset - pd_upper);
            }
        }
        self.set_pd_upper((pd_upper + size) as uint16);

        for i in 1..=nline {
            let mut ii = self.as_ref().item_id(i);
            if ii.has_storage() && (ii.lp_off() as usize) <= offset {
                ii.set_storage(ii.lp_off() + size as ItemOffset, ii.lp_len());
                self.set_item_id(i, ii);
            }
        }
    }

    /// `PageRepairFragmentation`; caller holds the buffer cleanup lock.
    /// Panics on corruption (in prune's crit section C's ERROR promotes to PANIC).
    pub fn repair_fragmentation(&mut self) {
        // SAFETY: same exclusively-held image; the read view is used only
        // between the interleaved header/line-pointer stores below.
        let r = unsafe { PageRef::from_raw(self.ptr) };
        let pd_lower = r.pd_lower() as usize;
        let pd_upper = r.pd_upper() as usize;
        let pd_special = r.pd_special() as usize;
        assert!(
            pd_lower >= SizeOfPageHeaderData
                && pd_lower <= pd_upper
                && pd_upper <= pd_special
                && pd_special <= BLCKSZ
                && pd_special == (pd_special + 7) & !7,
            "corrupted page pointers: lower = {pd_lower}, upper = {pd_upper}, special = {pd_special}"
        );

        let nline = r.max_offset_number();
        let mut itemidbase = [ItemIdCompact::ZERO; MaxHeapTuplesPerPage];
        let mut nstorage = 0usize;
        let mut nunused = 0usize;
        let mut totallen = 0usize;
        let mut last_offset = pd_special;
        let mut presorted = true;
        let mut finalusedlp: OffsetNumber = 0;

        for i in 1..=nline {
            // SAFETY: i <= max_offset_number.
            let lp = unsafe { r.item_id_unchecked(i) };
            if lp.is_used() {
                if lp.has_storage() {
                    let itemoff = lp.lp_off() as usize;
                    if last_offset > itemoff {
                        last_offset = itemoff;
                    } else {
                        presorted = false;
                    }
                    assert!(
                        itemoff >= pd_upper && itemoff < pd_special,
                        "corrupted line pointer: {itemoff}"
                    );
                    let alignedlen = (lp.lp_len() as usize + 7) & !7;
                    itemidbase[nstorage] = ItemIdCompact {
                        offsetindex: i - 1,
                        itemoff: itemoff as u16,
                        alignedlen: alignedlen as u16,
                    };
                    totallen += alignedlen;
                    nstorage += 1;
                }
                finalusedlp = i;
            } else {
                debug_assert!(!lp.has_storage());
                self.set_item_id(i, ItemIdData::new(0, LP_UNUSED, 0));
                nunused += 1;
            }
        }

        if nstorage == 0 {
            self.set_pd_upper(pd_special as uint16);
        } else {
            assert!(
                totallen <= pd_special - pd_lower,
                "corrupted item lengths: total {totallen}, available space {}",
                pd_special - pd_lower
            );
            self.compactify_tuples(&itemidbase[..nstorage], presorted);
        }

        if finalusedlp != nline {
            // Trailing unused line pointers: truncate the line-pointer array.
            let nunusedend = (nline - finalusedlp) as usize;
            debug_assert!(nunused >= nunusedend && nunusedend > 0);
            nunused -= nunusedend;
            self.set_pd_lower(
                (pd_lower - core::mem::size_of::<ItemIdData>() * nunusedend) as uint16,
            );
        }

        if nunused > 0 {
            self.set_has_free_line_pointers();
        } else {
            self.clear_has_free_line_pointers();
        }
    }

    /// `PageTruncateLinePointerArray`: shorten a trailing run of LP_UNUSED
    /// line pointers, always keeping at least one entry; caller guarantees at
    /// least one LP_UNUSED exists on the page.
    pub fn truncate_line_pointer_array(&mut self) {
        // SAFETY: same exclusively-held image; read view used between stores.
        let r = unsafe { PageRef::from_raw(self.ptr) };
        let mut countdone = false;
        let mut sethint = false;
        let mut nunusedend = 0usize;

        let mut i = r.max_offset_number();
        while i >= 1 {
            let lp = r.item_id(i);
            debug_assert!(lp.is_used() || !lp.has_storage());
            if !countdone && i > 1 {
                if lp.is_used() {
                    countdone = true;
                } else {
                    nunusedend += 1;
                }
            } else if !lp.is_used() {
                sethint = true;
                break;
            }
            i -= 1;
        }

        if nunusedend > 0 {
            let new_lower =
                r.pd_lower() as usize - core::mem::size_of::<ItemIdData>() * nunusedend;
            self.set_pd_lower(new_lower as uint16);
        } else {
            debug_assert!(sethint);
        }

        if sethint {
            self.set_has_free_line_pointers();
        } else {
            self.clear_has_free_line_pointers();
        }
    }

    /// `PageIndexTupleDelete`. Panics on corruption (C ereport ERROR,
    /// DATA_CORRUPTED; promoted inside callers' critical sections).
    pub fn index_tuple_delete(&mut self, offnum: OffsetNumber) {
        let r = self.as_ref();
        let pd_lower = r.pd_lower() as usize;
        let pd_upper = r.pd_upper() as usize;
        let pd_special = r.pd_special() as usize;
        assert!(
            pd_lower >= SizeOfPageHeaderData
                && pd_lower <= pd_upper
                && pd_upper <= pd_special
                && pd_special <= BLCKSZ
                && pd_special == (pd_special + 7) & !7,
            "corrupted page pointers: lower = {pd_lower}, upper = {pd_upper}, special = {pd_special}"
        );

        let nline = r.max_offset_number();
        assert!(offnum >= 1 && offnum <= nline, "invalid index offnum: {offnum}");

        let tup = r.item_id(offnum);
        debug_assert!(tup.has_storage());
        let size = tup.lp_len() as usize;
        let offset = tup.lp_off() as usize;
        assert!(
            offset >= pd_upper && offset + size <= pd_special && offset == (offset + 7) & !7,
            "corrupted line pointer: offset = {offset}, size = {size}"
        );
        let size = (size + 7) & !7;

        let offidx = (offnum - 1) as usize;
        let linp_base = SizeOfPageHeaderData;
        let nbytes = pd_lower - (linp_base + (offidx + 1) * core::mem::size_of::<ItemIdData>());
        if nbytes > 0 {
            // SAFETY: both ranges within pd_lower (validated); overlapping move.
            unsafe {
                let dst = self.ptr.as_ptr().add(linp_base + offidx * 4);
                core::ptr::copy(dst.add(4), dst, nbytes);
            }
        }

        if offset > pd_upper {
            // SAFETY: [pd_upper, offset) shifts up by `size`; the deleted item
            // occupied [offset, offset+size) within pd_special.
            unsafe {
                let addr = self.ptr.as_ptr().add(pd_upper);
                core::ptr::copy(addr, addr.add(size), offset - pd_upper);
            }
        }

        self.set_pd_upper((pd_upper + size) as uint16);
        self.set_pd_lower((pd_lower - core::mem::size_of::<ItemIdData>()) as uint16);

        let nline = nline - 1;
        if self.as_ref().pd_lower() as usize > SizeOfPageHeaderData {
            for i in 1..=nline {
                let mut ii = self.as_ref().item_id(i);
                debug_assert!(ii.has_storage());
                if (ii.lp_off() as usize) <= offset {
                    ii.set_storage((ii.lp_off() as usize + size) as ItemOffset, ii.lp_len());
                    self.set_item_id(i, ii);
                }
            }
        }
    }

    /// `PageIndexMultiDelete`; `itemnos` must be sorted ascending.
    pub fn index_multi_delete(&mut self, itemnos: &[OffsetNumber]) {
        debug_assert!(itemnos.len() <= MaxIndexTuplesPerPage);

        if itemnos.len() <= 2 {
            for &off in itemnos.iter().rev() {
                self.index_tuple_delete(off);
            }
            return;
        }

        let r = self.as_ref();
        let pd_lower = r.pd_lower() as usize;
        let pd_upper = r.pd_upper() as usize;
        let pd_special = r.pd_special() as usize;
        assert!(
            pd_lower >= SizeOfPageHeaderData
                && pd_lower <= pd_upper
                && pd_upper <= pd_special
                && pd_special <= BLCKSZ
                && pd_special == (pd_special + 7) & !7,
            "corrupted page pointers: lower = {pd_lower}, upper = {pd_upper}, special = {pd_special}"
        );

        let nline = r.max_offset_number();
        let mut itemidbase = [ItemIdCompact::ZERO; MaxIndexTuplesPerPage];
        let mut newitemids = [ItemIdData::default(); MaxIndexTuplesPerPage];
        let mut totallen = 0usize;
        let mut nused = 0usize;
        let mut nextitm = 0usize;
        let mut last_offset = pd_special;
        let mut presorted = true;

        for offnum in 1..=nline {
            let lp = r.item_id(offnum);
            debug_assert!(lp.has_storage());
            let size = lp.lp_len() as usize;
            let offset = lp.lp_off() as usize;
            assert!(
                offset >= pd_upper && offset + size <= pd_special && offset == (offset + 7) & !7,
                "corrupted line pointer: offset = {offset}, size = {size}"
            );

            if nextitm < itemnos.len() && offnum == itemnos[nextitm] {
                nextitm += 1;
            } else {
                let alignedlen = (size + 7) & !7;
                if last_offset > offset {
                    last_offset = offset;
                } else {
                    presorted = false;
                }
                itemidbase[nused] = ItemIdCompact {
                    offsetindex: nused as u16,
                    itemoff: offset as u16,
                    alignedlen: alignedlen as u16,
                };
                totallen += alignedlen;
                newitemids[nused] = lp;
                nused += 1;
            }
        }

        assert!(nextitm == itemnos.len(), "incorrect index offsets supplied");
        assert!(
            totallen <= pd_special - pd_lower,
            "corrupted item lengths: total {totallen}, available space {}",
            pd_special - pd_lower
        );

        for (i, id) in newitemids[..nused].iter().enumerate() {
            self.set_item_id((i + 1) as OffsetNumber, *id);
        }
        self.set_pd_lower(
            (SizeOfPageHeaderData + nused * core::mem::size_of::<ItemIdData>()) as uint16,
        );

        if nused > 0 {
            self.compactify_tuples(&itemidbase[..nused], presorted);
        } else {
            self.set_pd_upper(pd_special as uint16);
        }
    }

    // compactify_tuples: close removed-item gaps, restore reverse line-pointer order.
    fn compactify_tuples(&mut self, itemidbase: &[ItemIdCompact], presorted: bool) {
        debug_assert!(!itemidbase.is_empty());
        let nitems = itemidbase.len();
        let pd_special = self.as_ref().pd_special() as usize;
        let page = self.ptr.as_ptr();

        let mut upper = pd_special;
        if presorted {
            #[cfg(debug_assertions)]
            {
                let mut lastoff = pd_special;
                for it in itemidbase {
                    debug_assert!(lastoff > it.itemoff as usize);
                    lastoff = it.itemoff as usize;
                }
            }
            let mut i = 0;
            while i < nitems {
                let it = &itemidbase[i];
                if upper != it.itemoff as usize + it.alignedlen as usize {
                    break;
                }
                upper -= it.alignedlen as usize;
                i += 1;
            }
            if i < nitems {
                let mut copy_tail =
                    itemidbase[i].itemoff as usize + itemidbase[i].alignedlen as usize;
                let mut copy_head = copy_tail;
                for it in &itemidbase[i..] {
                    if copy_head != it.itemoff as usize + it.alignedlen as usize {
                        // SAFETY: all offsets/lengths validated against
                        // pd_upper..pd_special by repair_fragmentation; upper
                        // only decreases from pd_special by the same lengths.
                        unsafe {
                            core::ptr::copy(
                                page.add(copy_head),
                                page.add(upper),
                                copy_tail - copy_head,
                            )
                        };
                        copy_tail = it.itemoff as usize + it.alignedlen as usize;
                    }
                    upper -= it.alignedlen as usize;
                    copy_head = it.itemoff as usize;
                    let mut lp = self.as_ref().item_id(it.offsetindex + 1);
                    lp.set_storage(upper as ItemOffset, lp.lp_len());
                    self.set_item_id(it.offsetindex + 1, lp);
                }
                // SAFETY: as above.
                unsafe {
                    core::ptr::copy(page.add(copy_head), page.add(upper), copy_tail - copy_head)
                };
            }
        } else {
            let mut scratch = [0u8; BLCKSZ];
            let pd_upper = self.as_ref().pd_upper() as usize;
            let mut i = 0;
            if nitems < self.as_ref().max_offset_number() as usize / 4 {
                for it in itemidbase {
                    let off = it.itemoff as usize;
                    // SAFETY: validated item range within the page.
                    unsafe {
                        core::ptr::copy_nonoverlapping(
                            page.add(off),
                            scratch.as_mut_ptr().add(off),
                            it.alignedlen as usize,
                        )
                    };
                }
            } else {
                while i < nitems {
                    let it = &itemidbase[i];
                    if upper != it.itemoff as usize + it.alignedlen as usize {
                        break;
                    }
                    upper -= it.alignedlen as usize;
                    i += 1;
                }
                if i == nitems {
                    self.set_pd_upper(upper as uint16);
                    return;
                }
                // SAFETY: pd_upper..upper is the whole moving region.
                unsafe {
                    core::ptr::copy_nonoverlapping(
                        page.add(pd_upper),
                        scratch.as_mut_ptr().add(pd_upper),
                        upper - pd_upper,
                    )
                };
            }
            let mut copy_tail = itemidbase[i].itemoff as usize + itemidbase[i].alignedlen as usize;
            let mut copy_head = copy_tail;
            for it in &itemidbase[i..] {
                if copy_head != it.itemoff as usize + it.alignedlen as usize {
                    // SAFETY: scratch holds the staged tuples; target range is
                    // within pd_upper..pd_special.
                    unsafe {
                        core::ptr::copy_nonoverlapping(
                            scratch.as_ptr().add(copy_head),
                            page.add(upper),
                            copy_tail - copy_head,
                        )
                    };
                    copy_tail = it.itemoff as usize + it.alignedlen as usize;
                }
                upper -= it.alignedlen as usize;
                copy_head = it.itemoff as usize;
                let mut lp = self.as_ref().item_id(it.offsetindex + 1);
                lp.set_storage(upper as ItemOffset, lp.lp_len());
                self.set_item_id(it.offsetindex + 1, lp);
            }
            // SAFETY: as above.
            unsafe {
                core::ptr::copy_nonoverlapping(
                    scratch.as_ptr().add(copy_head),
                    page.add(upper),
                    copy_tail - copy_head,
                )
            };
        }
        self.set_pd_upper(upper as uint16);
    }
}

// bufpage.c itemIdCompactData.
#[derive(Clone, Copy)]
struct ItemIdCompact {
    offsetindex: u16,
    itemoff: u16,
    alignedlen: u16,
}

impl ItemIdCompact {
    const ZERO: ItemIdCompact = ItemIdCompact { offsetindex: 0, itemoff: 0, alignedlen: 0 };
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

    fn add_n(pm: &mut PageMut<'_>, n: usize, len: usize) {
        for i in 0..n {
            let item = alloc::vec![(i + 1) as u8; len];
            assert_eq!(pm.add_item(&item, 0, PAI_IS_HEAP), Some((i + 1) as OffsetNumber));
        }
    }

    #[test]
    fn repair_fragmentation_presorted() {
        let mut t = temp_page();
        let mut pm = page_mut(&mut t);
        pm.init(0);
        add_n(&mut pm, 4, 28); // offsets 8160, 8128, 8096, 8064
        let mut lp2 = pm.as_ref().item_id(2);
        lp2.set_unused();
        pm.set_item_id(2, lp2);
        pm.repair_fragmentation();

        let pm = page_mut(&mut t);
        let r = pm.as_ref();
        assert_eq!(r.max_offset_number(), 4);
        assert_eq!(r.pd_upper(), (BLCKSZ - 3 * 32) as u16);
        assert!(r.has_free_line_pointers());
        // Tuples 1,3,4 packed against pd_special in line-pointer order.
        for (off, expect_at, tag) in [(1u16, BLCKSZ - 32, 1u8), (3, BLCKSZ - 64, 3), (4, BLCKSZ - 96, 4)] {
            let id = r.item_id(off);
            assert_eq!(id.lp_off() as usize, expect_at);
            let (ptr, len) = r.item_raw(id);
            assert_eq!(len, 28);
            // SAFETY: in-page item.
            assert_eq!(unsafe { *ptr }, tag);
        }
    }

    #[test]
    fn repair_fragmentation_not_presorted_and_trailing_truncation() {
        let mut t = temp_page();
        let mut pm = page_mut(&mut t);
        pm.init(0);
        add_n(&mut pm, 6, 28);
        // Swap the storage of items 1 and 5 so itemoff order isn't descending.
        let (a, b) = (pm.as_ref().item_id(1), pm.as_ref().item_id(5));
        pm.set_item_id(1, ItemIdData::new(b.lp_off(), LP_NORMAL, b.lp_len()));
        pm.set_item_id(5, ItemIdData::new(a.lp_off(), LP_NORMAL, a.lp_len()));
        // Kill 2 and the trailing 5,6: the line-pointer array truncates to 4.
        for off in [2u16, 5, 6] {
            let mut lp = pm.as_ref().item_id(off);
            lp.set_unused();
            pm.set_item_id(off, lp);
        }
        pm.repair_fragmentation();

        let pm = page_mut(&mut t);
        let r = pm.as_ref();
        assert_eq!(r.max_offset_number(), 4);
        assert_eq!(r.pd_upper(), (BLCKSZ - 3 * 32) as u16);
        assert!(r.has_free_line_pointers());
        for (off, tag) in [(1u16, 5u8), (3, 3), (4, 4)] {
            let id = r.item_id(off);
            let (ptr, len) = r.item_raw(id);
            assert_eq!(len, 28);
            // SAFETY: in-page item.
            assert_eq!(unsafe { *ptr }, tag);
        }
        let lasts: alloc::vec::Vec<usize> = (1..=4u16).filter(|&o| r.item_id(o).is_used())
            .map(|o| r.item_id(o).lp_off() as usize).collect();
        let mut sorted = lasts.clone();
        sorted.sort_unstable_by(|x, y| y.cmp(x));
        assert_eq!(lasts, sorted); // reverse line-pointer order restored
    }

    #[test]
    fn repair_fragmentation_empty_page_resets_upper() {
        let mut t = temp_page();
        let mut pm = page_mut(&mut t);
        pm.init(0);
        add_n(&mut pm, 2, 28);
        for off in [1u16, 2] {
            let mut lp = pm.as_ref().item_id(off);
            lp.set_unused();
            pm.set_item_id(off, lp);
        }
        pm.repair_fragmentation();
        let pm = page_mut(&mut t);
        let r = pm.as_ref();
        assert_eq!(r.pd_upper() as usize, BLCKSZ);
        assert_eq!(r.max_offset_number(), 0);
        assert!(!r.has_free_line_pointers());
    }

    fn fill_index_page(pm: &mut PageMut<'_>, n: usize) -> alloc::vec::Vec<[u8; 16]> {
        pm.init(16);
        let mut items = Vec::new();
        for i in 0..n {
            let mut item = [0u8; 16];
            item[0] = i as u8;
            item[8..12].copy_from_slice(&(i as u32).to_ne_bytes());
            assert_eq!(pm.add_item(&item, (i + 1) as OffsetNumber, 0), Some((i + 1) as OffsetNumber));
            items.push(item);
        }
        items
    }

    fn surviving_payloads(pm: &PageMut<'_>) -> alloc::vec::Vec<u8> {
        let r = pm.as_ref();
        (1..=r.max_offset_number())
            .map(|off| {
                let id = r.item_id(off);
                assert!(id.has_storage());
                let (p, l) = r.item_raw(id);
                assert_eq!(l, 16);
                // SAFETY: item_raw bounds-checked.
                unsafe { *p }
            })
            .collect()
    }

    #[test]
    fn index_tuple_delete_shifts_and_repoints() {
        let mut t = temp_page();
        let mut pm = page_mut(&mut t);
        fill_index_page(&mut pm, 5);
        pm.index_tuple_delete(3);
        assert_eq!(surviving_payloads(&pm), [0, 1, 3, 4]);
        pm.index_tuple_delete(1);
        assert_eq!(surviving_payloads(&pm), [1, 3, 4]);
        pm.index_tuple_delete(3);
        assert_eq!(surviving_payloads(&pm), [1, 3]);
    }

    #[test]
    fn index_multi_delete_matches_retail_deletes() {
        // <=2 items go through the retail path; >2 through compactify.
        let cases: [&[OffsetNumber]; 4] =
            [&[2, 5], &[1, 4, 7, 8], &[3], &[1, 2, 3, 4, 5, 6, 7, 8]];
        for dels in cases {
            let mut t1 = temp_page();
            let mut pm1 = page_mut(&mut t1);
            fill_index_page(&mut pm1, 8);
            pm1.index_multi_delete(&dels);

            let mut t2 = temp_page();
            let mut pm2 = page_mut(&mut t2);
            fill_index_page(&mut pm2, 8);
            for &off in dels.iter().rev() {
                pm2.index_tuple_delete(off);
            }

            assert_eq!(surviving_payloads(&pm1), surviving_payloads(&pm2), "dels {dels:?}");
            assert_eq!(
                (pm1.as_ref().pd_lower(), pm1.as_ref().pd_upper()),
                (pm2.as_ref().pd_lower(), pm2.as_ref().pd_upper()),
                "dels {dels:?}"
            );
            // physical layout parity too (both keep remaining tuples in
            // original relative order)
            for off in 1..=pm1.as_ref().max_offset_number() {
                assert_eq!(pm1.as_ref().item_id(off), pm2.as_ref().item_id(off));
            }
        }
    }

    #[test]
    fn index_multi_delete_all_items_empties_page() {
        let mut t = temp_page();
        let mut pm = page_mut(&mut t);
        fill_index_page(&mut pm, 4);
        pm.index_multi_delete(&[1, 2, 3, 4]);
        assert_eq!(pm.as_ref().max_offset_number(), 0);
        assert_eq!(pm.as_ref().pd_upper(), pm.as_ref().pd_special());
    }
}
