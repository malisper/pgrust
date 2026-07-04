use core::alloc::Layout;
use core::ptr::NonNull;

use ::mcx::{check_alloc_size, Allocator, Mcx};
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_tuple::{HeapTupleData, ItemPointerData, MinimalTupleData, MAXIMUM_ALIGNOF};

#[inline]
pub(crate) fn image_layout(size: usize) -> Layout {
    // SAFETY: MAXIMUM_ALIGNOF is a power of two; size passed check_alloc_size.
    unsafe { Layout::from_size_align_unchecked(size, MAXIMUM_ALIGNOF) }
}

// Out of line: the 456B PgError temp must not live in callers' hot frames.
#[cold]
#[inline(never)]
fn oom_boxed(mcx: Mcx<'_>, size: usize) -> alloc::boxed::Box<::types_error::PgError> {
    alloc::boxed::Box::new(mcx.oom(size))
}

#[inline]
pub(crate) fn alloc_image(mcx: Mcx<'_>, size: usize, zeroed: bool) -> PgResult<NonNull<u8>> {
    check_alloc_size(size)?;
    let layout = image_layout(size);
    let res = if zeroed {
        mcx.allocate_zeroed(layout)
    } else {
        mcx.allocate(layout)
    };
    match res {
        Ok(p) => Ok(p.cast()),
        Err(_) => Err(oom_boxed(mcx, size)),
    }
}

/// Owned heap tuple (C single-palloc image); Drop is `heap_freetuple`.
pub struct HeapTuple<'mcx> {
    htup: HeapTupleData<'mcx>,
    alloc_size: u32,
    mcx: Mcx<'mcx>,
}

impl<'mcx> HeapTuple<'mcx> {
    /// # Safety
    /// `image` from `alloc_image(mcx, t_len as usize, _)`, header bytes initialized.
    #[inline]
    pub(crate) unsafe fn from_image(
        mcx: Mcx<'mcx>,
        image: NonNull<u8>,
        t_len: u32,
        t_self: ItemPointerData,
        t_tableOid: Oid,
    ) -> HeapTuple<'mcx> {
        HeapTuple {
            htup: HeapTupleData::from_raw_parts(image.as_ptr(), t_len, t_self, t_tableOid),
            alloc_size: t_len,
            mcx,
        }
    }

    /// Zeroed `t_len`-byte image; the caller initializes header and data
    /// (the heaptoast rebuild path — C's `palloc0` + memcpy'd header).
    pub fn alloc_zeroed(mcx: Mcx<'mcx>, t_len: usize) -> PgResult<HeapTuple<'mcx>> {
        let image = alloc_image(mcx, t_len, true)?;
        // SAFETY: fresh zeroed image of t_len bytes.
        unsafe {
            Ok(HeapTuple::from_image(
                mcx,
                image,
                t_len as u32,
                ItemPointerData::invalid(),
                ::types_core::InvalidOid,
            ))
        }
    }

    #[inline]
    pub fn as_tuple(&self) -> &HeapTupleData<'mcx> {
        &self.htup
    }

    #[inline]
    pub fn as_tuple_mut(&mut self) -> &mut HeapTupleData<'mcx> {
        &mut self.htup
    }

    #[inline]
    pub fn image(&self) -> &[u8] {
        // SAFETY: the image is owned, live, and t_len bytes long.
        unsafe { core::slice::from_raw_parts(self.htup.header_ptr(), self.htup.t_len as usize) }
    }

    /// The size Drop frees with; forget-then-free-by-t_len consumers must
    /// assert `t_len` still equals it.
    #[inline]
    pub fn alloc_size(&self) -> u32 {
        self.alloc_size
    }

    #[inline]
    pub fn image_mut(&mut self) -> &mut [u8] {
        // SAFETY: the image is owned, live, unique, and t_len bytes long.
        unsafe {
            core::slice::from_raw_parts_mut(
                self.htup.header_ptr().cast_mut(),
                self.htup.t_len as usize,
            )
        }
    }
}

impl<'mcx> core::ops::Deref for HeapTuple<'mcx> {
    type Target = HeapTupleData<'mcx>;
    #[inline]
    fn deref(&self) -> &HeapTupleData<'mcx> {
        &self.htup
    }
}

impl<'mcx> core::ops::DerefMut for HeapTuple<'mcx> {
    #[inline]
    fn deref_mut(&mut self) -> &mut HeapTupleData<'mcx> {
        &mut self.htup
    }
}

impl Drop for HeapTuple<'_> {
    fn drop(&mut self) {
        // SAFETY: image allocated by alloc_image with this exact layout.
        unsafe {
            self.mcx.deallocate(
                NonNull::new_unchecked(self.htup.header_ptr().cast_mut()),
                image_layout(self.alloc_size as usize),
            )
        }
    }
}

/// Owned minimal tuple: contiguous C `MinimalTupleData` image, preceded by
/// `extra` caller bytes (C's `palloc(len + extra); tuple = mem + extra`).
pub struct MinimalTuple<'mcx> {
    base: NonNull<u8>,
    extra: u32,
    alloc_size: u32,
    mcx: Mcx<'mcx>,
}

impl<'mcx> MinimalTuple<'mcx> {
    pub(crate) fn alloc(
        mcx: Mcx<'mcx>,
        t_len: usize,
        extra: usize,
        zeroed: bool,
    ) -> PgResult<MinimalTuple<'mcx>> {
        debug_assert!(extra % MAXIMUM_ALIGNOF == 0);
        let size = t_len + extra;
        let base = alloc_image(mcx, size, zeroed)?;
        if !zeroed && extra > 0 {
            // SAFETY: extra <= size bytes owned by this fresh allocation.
            unsafe { core::ptr::write_bytes(base.as_ptr(), 0, extra) };
        }
        Ok(MinimalTuple {
            base,
            extra: extra as u32,
            alloc_size: size as u32,
            mcx,
        })
    }

    #[inline]
    pub fn t_len(&self) -> u32 {
        self.alloc_size - self.extra
    }

    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        // SAFETY: extra <= alloc_size; tuple starts extra bytes into the block.
        unsafe { self.base.as_ptr().add(self.extra as usize) }
    }

    #[inline]
    pub(crate) fn tuple_mut_ptr(&mut self) -> *mut u8 {
        unsafe { self.base.as_ptr().add(self.extra as usize) }
    }

    #[inline]
    pub fn data(&self) -> &MinimalTupleData {
        // SAFETY: header bytes initialized at construction; alignment from alloc_image + MAXALIGN'd extra.
        unsafe { &*self.as_ptr().cast::<MinimalTupleData>() }
    }

    #[inline]
    pub fn data_mut(&mut self) -> &mut MinimalTupleData {
        unsafe { &mut *self.tuple_mut_ptr().cast::<MinimalTupleData>() }
    }

    /// The flat tuple image (`(char *) mtup .. + t_len`), excluding `extra`.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        // SAFETY: t_len owned bytes starting at the tuple.
        unsafe { core::slice::from_raw_parts(self.as_ptr(), self.t_len() as usize) }
    }

    /// The caller-owned `extra` prefix bytes (zeroed at construction).
    #[inline]
    pub fn extra_mut(&mut self) -> &mut [u8] {
        // SAFETY: the first extra bytes of the owned block.
        unsafe { core::slice::from_raw_parts_mut(self.base.as_ptr(), self.extra as usize) }
    }

    /// Whole-allocation pointer (`extra` prefix at offset 0, tuple at
    /// `extra`), full-image provenance — an `extra_mut()`-derived pointer
    /// covers only the prefix and cannot reach the tuple. Forgets self:
    /// the block is bulk-freed with its context (docs/no-drop.md).
    #[inline]
    pub fn forget_base(self) -> NonNull<u8> {
        let p = self.base;
        core::mem::forget(self);
        p
    }
}

impl Drop for MinimalTuple<'_> {
    fn drop(&mut self) {
        // SAFETY: block allocated by alloc_image with this exact layout.
        unsafe {
            self.mcx
                .deallocate(self.base, image_layout(self.alloc_size as usize))
        }
    }
}

