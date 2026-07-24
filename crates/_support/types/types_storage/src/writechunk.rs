//! `WriteChunk` — the storage write path's image argument.
//!
//! C has no counterpart type: `smgrwrite`/`FileWriteV` take a bare `char *`,
//! and `pwritev`'s `iovec` carries no aliasing promise at all. This type is
//! how that same absence of a promise is spelled in Rust.

use core::cell::UnsafeCell;
use core::marker::PhantomData;

/// One contiguous image handed to the storage write path.
///
/// **Why this exists instead of `&[u8]`.** The buffer pool writes a page out
/// while holding only a SHARE content lock (`FlushBuffer` and every caller
/// above it), and hint-bit setters mutate that very same shared page image
/// under that very same SHARE lock. A shared reference whose element type is
/// `Freeze` — `&[u8]` — asserts to the compiler that the bytes do not change
/// for the reference's whole lifetime, so minting one over a live shared page
/// is aliasing undefined behaviour. The *data* outcome is not in question: C
/// hands `pwritev` a bare pointer into the same live page and tolerates a torn
/// hint bit by design (a page written with no checksum cannot be invalidated
/// by one), and so do we. What `&[u8]` adds on top of C's behaviour is a
/// promise to the optimizer that is false.
///
/// A `WriteChunk` carries the same `(base, len)` pair, but is not a reference
/// to `u8`: the `PhantomData<&'a UnsafeCell<u8>>` marker keeps the borrow
/// (and the `!Send`/`!Sync`-ness a pinned page image wants) without claiming
/// the bytes are immutable. Crucially the bytes are then never read *through
/// Rust* on the flush path — the only reader is the kernel, via the `iovec`
/// this lowers to, and a kernel read is not an operation of the Rust abstract
/// machine. So there is no Rust-level read to race with the hint-bit writer.
///
/// The layout is deliberately `iovec`-compatible (`#[repr(C)]`, base then
/// length, both pointer-sized) so `&[WriteChunk]` can be reinterpreted as
/// `&[iovec]` at the syscall boundary — the same reinterpretation the fd layer
/// already performs on `std::io::IoSlice`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct WriteChunk<'a> {
    base: *const u8,
    len: usize,
    _borrow: PhantomData<&'a UnsafeCell<u8>>,
}

impl<'a> WriteChunk<'a> {
    /// A chunk over memory the caller owns exclusively for `'a` (a private
    /// scratch page, a local-buffer block, a test fixture).
    #[inline]
    pub fn from_slice(buf: &'a [u8]) -> Self {
        WriteChunk { base: buf.as_ptr(), len: buf.len(), _borrow: PhantomData }
    }

    /// A chunk over a shared page image that another backend may mutate while
    /// the write is in flight — the buffer pool's flush path, where hint-bit
    /// setters run under the same SHARE content lock.
    ///
    /// # Safety
    /// `base` must be readable for `len` bytes for all of `'a`; on the flush
    /// path that is the caller's buffer pin. Concurrent *writes* by other
    /// backends are explicitly permitted — that is the whole point of the
    /// type — provided no Rust read of the bytes is derived from it. Use
    /// [`WriteChunk::as_slice_unchecked`] only where exclusivity is separately
    /// established.
    #[inline]
    pub unsafe fn from_shared(base: *const u8, len: usize) -> Self {
        WriteChunk { base, len, _borrow: PhantomData }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.base
    }

    /// Drop the first `n` bytes — the short-write resume step, where the
    /// kernel already consumed a prefix of this chunk.
    #[inline]
    pub fn advance(self, n: usize) -> Self {
        debug_assert!(n <= self.len);
        WriteChunk {
            // SAFETY: n <= len, so base+n is in bounds (one-past-the-end at
            // n == len, which is a valid pointer for a zero-length chunk).
            base: unsafe { self.base.add(n) },
            len: self.len - n,
            _borrow: PhantomData,
        }
    }

    /// Read the bytes back as a slice.
    ///
    /// # Safety
    /// Only sound where no other backend can write the image for `'a` — a
    /// private buffer, or a test with one thread. This is exactly the promise
    /// the type exists to avoid making, so every call site must name its own
    /// excluding mechanism; never call it on a shared buffer-pool page.
    #[inline]
    pub unsafe fn as_slice_unchecked(&self) -> &'a [u8] {
        // SAFETY: caller contract (readable for len, and exclusive).
        unsafe { core::slice::from_raw_parts(self.base, self.len) }
    }
}

impl<'a> From<&'a [u8]> for WriteChunk<'a> {
    #[inline]
    fn from(buf: &'a [u8]) -> Self {
        WriteChunk::from_slice(buf)
    }
}

impl core::fmt::Debug for WriteChunk<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // Never print the bytes: they may be changing under us.
        f.debug_struct("WriteChunk").field("len", &self.len).finish_non_exhaustive()
    }
}
