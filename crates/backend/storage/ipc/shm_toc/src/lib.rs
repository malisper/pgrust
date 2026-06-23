#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

//! `backend-storage-ipc-shm-toc` — shared-memory segment table of contents.
//!
//! Port of `src/backend/storage/ipc/shm_toc.c` plus the inline estimator
//! pieces of `src/include/storage/shm_toc.h`.
//!
//! A `shm_toc` is the bootstrap directory a process setting up a dynamic
//! shared-memory segment writes at the head of the segment: it bump-allocates
//! chunks backward from the end of the segment, records the `(key → offset)`
//! of each registered structure in a flexible entry array growing forward from
//! the header, and lets other backends — which may map the same segment at a
//! *different* virtual address — re-discover those structures by key using
//! only segment-relative offsets.
//!
//! The TOC header, its flexible `toc_entry[]` array, and the bump-allocated
//! chunks all live inside the caller's segment; there is no backend-local
//! copy of any of that state. The in-segment layout is the crate-local
//! `#[repr(C)]` [`InSegmentShmToc`] / [`ShmTocEntry`] (matching
//! `offsetof(shm_toc, toc_entry) == 40` and `sizeof(shm_toc_entry) == 16` on
//! the 64-bit target); the header's `toc_mutex` is the real in-segment
//! [`Spinlock`]. [`ShmToc`] is the handle the rest of the engine uses instead
//! of a raw `*mut shm_toc`; it borrows the segment, it does not own it.
//!
//! Matching the C, [`ShmToc::lookup`] acquires no spinlock — only memory
//! barriers. The release fence in [`ShmToc::insert`] (after filling an entry,
//! before bumping `toc_nentry`) pairs with the acquire fence in `lookup`
//! (after reading `toc_nentry`, before examining any entry), so the entry
//! array is safe to scan unlocked.

use core::mem::{align_of, size_of};
use core::ptr::NonNull;
use core::sync::atomic::{fence, Ordering};

use ipc_shmem_seams::{add_size, mul_size};
use s_lock::{s_init_lock, s_lock_macro, s_unlock, Spinlock};
use utils_error::{elog, ereport};
use types_core::{uint64, Size};
use types_error::{PgError, PgResult, ERRCODE_OUT_OF_MEMORY, ERROR};
use ::types_storage::storage::shm_toc_estimator;

/// `ALIGNOF_BUFFER` (`pg_config.h`) — buffer alignment used by `BUFFERALIGN`.
pub const ALIGNOF_BUFFER: Size = 32;

/// `PG_UINT32_MAX`.
pub const PG_UINT32_MAX: Size = u32::MAX as Size;

/// `BUFFERALIGN(LEN)` — round `len` up to the next `ALIGNOF_BUFFER` boundary.
#[inline]
pub const fn BUFFERALIGN(len: Size) -> Size {
    // C's `TYPEALIGN` is `((uintptr_t)(LEN) + (ALIGNVAL - 1)) & ~(ALIGNVAL - 1)`,
    // which wraps on overflow; `wrapping_add` reproduces that exactly (a plain
    // `+` would panic in debug builds where C silently wraps).
    len.wrapping_add(ALIGNOF_BUFFER - 1) & !(ALIGNOF_BUFFER - 1)
}

/// `BUFFERALIGN_DOWN(LEN)` — round `len` down to an `ALIGNOF_BUFFER` boundary.
#[inline]
pub const fn BUFFERALIGN_DOWN(len: Size) -> Size {
    len & !(ALIGNOF_BUFFER - 1)
}

/// `shm_toc_entry` as it lives in the in-segment flexible entry array.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct ShmTocEntry {
    /// `uint64 key` — arbitrary identifier.
    key: uint64,
    /// `Size offset` — offset, in bytes, from the TOC start.
    offset: Size,
}

/// `struct shm_toc` as it lives at the head of the segment, up to (but not
/// including) the flexible `toc_entry[]` array.
#[repr(C)]
struct InSegmentShmToc {
    /// `uint64 toc_magic` — magic number identifying this TOC.
    toc_magic: uint64,
    /// `slock_t toc_mutex` — spinlock for mutual exclusion.
    toc_mutex: Spinlock,
    /// `Size toc_total_bytes` — bytes managed by this TOC.
    toc_total_bytes: Size,
    /// `Size toc_allocated_bytes` — bytes allocated of those managed.
    toc_allocated_bytes: Size,
    /// `uint32 toc_nentry` — number of entries in the TOC.
    toc_nentry: u32,
}

/// `offsetof(shm_toc, toc_entry)` — size of the fixed TOC header, before the
/// flexible `toc_entry[]` array begins.
#[inline]
const fn toc_entry_offset() -> Size {
    size_of::<InSegmentShmToc>()
}

/// `ereport(ERROR, errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of shared memory"))`.
fn out_of_shared_memory() -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_OUT_OF_MEMORY)
        .errmsg("out of shared memory")
        .into_error()
}

/// A handle to a `shm_toc` living at the head of a shared-memory segment —
/// the replacement for the raw `*mut shm_toc` the C passes around. It borrows
/// the segment (it does not own it); the segment's lifetime is the
/// surrounding DSM/shmem discipline's responsibility.
#[derive(Clone, Copy, Debug)]
pub struct ShmToc {
    /// Pointer to the `InSegmentShmToc` header at the start of the segment.
    base: NonNull<u8>,
}

// The handle is just a borrow of a shared segment whose cross-process
// synchronization is the in-segment `toc_mutex` spinlock's responsibility.
unsafe impl Send for ShmToc {}

impl ShmToc {
    /// Pointer to the in-segment header.
    #[inline]
    fn header_ptr(&self) -> *mut InSegmentShmToc {
        self.base.as_ptr().cast::<InSegmentShmToc>()
    }

    /// Shared view of the in-segment header.
    ///
    /// # Safety
    ///
    /// The segment must remain mapped and contain a valid `InSegmentShmToc`.
    #[inline]
    unsafe fn header(&self) -> &InSegmentShmToc {
        // SAFETY: base points at a live InSegmentShmToc for the segment's life.
        unsafe { &*self.header_ptr() }
    }

    /// Mutable view of the in-segment header.
    ///
    /// # Safety
    ///
    /// As [`header`](Self::header); callers must serialize writes to the locked
    /// fields with the `toc_mutex` spinlock as the C does.
    #[inline]
    #[allow(clippy::mut_from_ref)]
    unsafe fn header_mut(&self) -> &mut InSegmentShmToc {
        // SAFETY: as above. The fields mutated under this reference are the
        // shmem-resident header fields the spinlock serializes.
        unsafe { &mut *self.header_ptr() }
    }

    /// The in-segment `toc_mutex` spinlock.
    #[inline]
    fn lock(&self) -> &Spinlock {
        // SAFETY: the toc_mutex field is part of the live in-segment header.
        unsafe { &(*self.header_ptr()).toc_mutex }
    }

    /// `SpinLockAcquire(&toc->toc_mutex)`, returning an RAII release guard.
    fn spin_lock(&self, func: &'static str) -> TocSpinGuard<'_> {
        s_lock_macro(self.lock(), Some(file!()), line!() as i32, Some(func));
        TocSpinGuard { lock: self.lock() }
    }

    /// Raw pointer to entry `index` of the flexible `toc_entry[]` array.
    ///
    /// # Safety
    ///
    /// The segment must have room for at least `index + 1` entries.
    #[inline]
    unsafe fn entry_ptr(&self, index: Size) -> *mut ShmTocEntry {
        // SAFETY: the flexible array begins at toc_entry_offset() bytes past the
        // header; entries are contiguous `ShmTocEntry`s. Caller guarantees room.
        unsafe {
            self.base
                .as_ptr()
                .add(toc_entry_offset())
                .cast::<ShmTocEntry>()
                .add(index)
        }
    }

    /// Initialize a region of shared memory with a table of contents
    /// (`shm_toc_create`).
    ///
    /// # Safety
    ///
    /// `address` must point to at least `nbytes` of writable, properly aligned
    /// shared memory that outlives the returned handle, with
    /// `nbytes > offsetof(shm_toc, toc_entry)`.
    pub unsafe fn create(magic: uint64, address: NonNull<u8>, nbytes: Size) -> Self {
        debug_assert_eq!(
            address.as_ptr() as usize % align_of::<InSegmentShmToc>(),
            0,
            "shm_toc segment must be aligned for the header"
        );
        debug_assert!(nbytes > toc_entry_offset());

        let toc = Self { base: address };
        // SAFETY: caller guarantees `address` is a writable segment of `nbytes`.
        let header = unsafe { toc.header_mut() };
        header.toc_magic = magic;
        s_init_lock(&header.toc_mutex);
        // The alignment code in `allocate` assumes that the starting value is
        // buffer-aligned.
        header.toc_total_bytes = BUFFERALIGN_DOWN(nbytes);
        header.toc_allocated_bytes = 0;
        header.toc_nentry = 0;

        toc
    }

    /// Attach to an existing table of contents (`shm_toc_attach`). If the magic
    /// number found at the target address doesn't match, returns `None`.
    ///
    /// # Safety
    ///
    /// `address` must point to a live `shm_toc` header in shared memory that
    /// outlives the returned handle.
    pub unsafe fn attach(magic: uint64, address: NonNull<u8>) -> Option<Self> {
        let toc = Self { base: address };
        // SAFETY: caller guarantees `address` points at a live header.
        let header = unsafe { toc.header() };

        if header.toc_magic != magic {
            return None;
        }

        debug_assert!(header.toc_total_bytes >= header.toc_allocated_bytes);
        debug_assert!(header.toc_total_bytes > toc_entry_offset());

        Some(toc)
    }

    /// Allocate shared memory from the segment managed by this TOC
    /// (`shm_toc_allocate`).
    ///
    /// This is not a full-blown allocator; there's no way to free memory. It's
    /// just a way of dividing a single physical shared memory segment into
    /// logical chunks that may be used for different purposes. We allocate
    /// backwards from the end of the segment, so that the TOC entries can grow
    /// forward from the start of the segment.
    pub fn allocate(&self, nbytes: Size) -> PgResult<NonNull<u8>> {
        // Make sure request is well-aligned. XXX: MAXALIGN is not enough,
        // because atomic ops might need a wider alignment. We don't have a
        // proper definition for the minimum to make atomic ops safe, but
        // BUFFERALIGN ought to be enough.
        let nbytes = BUFFERALIGN(nbytes);

        let guard = self.spin_lock("shm_toc_allocate");
        // SAFETY: the spinlock is held; the header is live for the segment.
        let header = unsafe { self.header_mut() };

        let total_bytes = header.toc_total_bytes;
        let allocated_bytes = header.toc_allocated_bytes;
        let nentry = header.toc_nentry as Size;
        let toc_bytes = toc_entry_offset() + nentry * size_of::<ShmTocEntry>() + allocated_bytes;

        // Check for memory exhaustion and overflow. C relies on unsigned
        // wrapping for the overflow case (`toc_bytes + nbytes < toc_bytes`);
        // wrapping_add reproduces it without Rust's debug-overflow panic.
        let sum = toc_bytes.wrapping_add(nbytes);
        if sum > total_bytes || sum < toc_bytes {
            drop(guard);
            return Err(out_of_shared_memory());
        }
        header.toc_allocated_bytes += nbytes;

        drop(guard);

        // ((char *) toc) + (total_bytes - allocated_bytes - nbytes)
        // SAFETY: the offset is within the segment (checked above) and the
        // segment outlives the handle. The offset is nonzero (the header
        // itself occupies the first toc_entry_offset() bytes), so the result
        // of offsetting the non-null base is non-null.
        unsafe {
            Ok(NonNull::new_unchecked(
                self.base.as_ptr().add(total_bytes - allocated_bytes - nbytes),
            ))
        }
    }

    /// Return the number of bytes that can still be allocated
    /// (`shm_toc_freespace`).
    pub fn freespace(&self) -> Size {
        let guard = self.spin_lock("shm_toc_freespace");
        // SAFETY: the spinlock is held; the header is live for the segment.
        let header = unsafe { self.header() };
        let total_bytes = header.toc_total_bytes;
        let allocated_bytes = header.toc_allocated_bytes;
        let nentry = header.toc_nentry as Size;
        drop(guard);

        let toc_bytes = toc_entry_offset() + nentry * size_of::<ShmTocEntry>();
        debug_assert!(allocated_bytes + BUFFERALIGN(toc_bytes) <= total_bytes);
        total_bytes - (allocated_bytes + BUFFERALIGN(toc_bytes))
    }

    /// Insert a TOC entry (`shm_toc_insert`).
    ///
    /// The process setting up the shared memory segment registers the
    /// addresses of data structures within the segment under a 64-bit key,
    /// assumed to be a well-known or discoverable integer; other processes
    /// pass the same key to [`ShmToc::lookup`]. Since the segment may be
    /// mapped at different addresses within different backends, we store
    /// relative rather than absolute pointers.
    ///
    /// This won't scale well to a large number of keys. The real idea is just
    /// to give someone mapping a dynamic shared memory segment the ability to
    /// find the bare minimum number of pointers they need to bootstrap.
    ///
    /// # Safety
    ///
    /// `address` must point within this TOC's segment, strictly past the TOC
    /// start.
    pub unsafe fn insert(&self, key: uint64, address: NonNull<u8>) -> PgResult<()> {
        // Relativize pointer.
        debug_assert!(address.as_ptr() as usize > self.base.as_ptr() as usize);
        let offset = (address.as_ptr() as usize) - (self.base.as_ptr() as usize);

        let guard = self.spin_lock("shm_toc_insert");
        // SAFETY: the spinlock is held; the header is live for the segment.
        let header = unsafe { self.header_mut() };

        let total_bytes = header.toc_total_bytes;
        let allocated_bytes = header.toc_allocated_bytes;
        let nentry = header.toc_nentry as Size;
        let toc_bytes = toc_entry_offset() + nentry * size_of::<ShmTocEntry>() + allocated_bytes;

        // Check for memory exhaustion and overflow (wrapping_add reproduces
        // C's unsigned-overflow guard without a debug panic).
        let sum = toc_bytes.wrapping_add(size_of::<ShmTocEntry>());
        if sum > total_bytes || sum < toc_bytes || nentry >= PG_UINT32_MAX {
            drop(guard);
            return Err(out_of_shared_memory());
        }

        debug_assert!(offset < total_bytes);
        // SAFETY: the guards above proved there is room for entry `nentry`.
        let entry = unsafe { self.entry_ptr(nentry) };
        // SAFETY: `entry` points at writable space for one ShmTocEntry.
        unsafe {
            (*entry).key = key;
            (*entry).offset = offset;
        }

        // By placing a write barrier after filling in the entry and before
        // updating the number of entries, we make it safe to read the TOC
        // unlocked.
        fence(Ordering::Release);

        header.toc_nentry += 1;

        drop(guard);
        Ok(())
    }

    /// Look up a TOC entry (`shm_toc_lookup`).
    ///
    /// If the key is not found, returns `Ok(None)` when `no_error` is true,
    /// otherwise throws `elog(ERROR)`.
    ///
    /// Unlike the other functions here, this operation acquires no lock; it
    /// uses only barriers. It probably wouldn't hurt concurrency very much
    /// even if it did get a lock, but since it's reasonably likely that a
    /// group of worker processes could each read a series of entries from the
    /// same TOC right around the same time, there seems to be some value in
    /// avoiding it.
    pub fn lookup(&self, key: uint64, no_error: bool) -> PgResult<Option<NonNull<u8>>> {
        // Read the number of entries before we examine any entry. We assume
        // that reading a uint32 is atomic.
        // SAFETY: the header is live for the segment's lifetime.
        let nentry = unsafe { self.header() }.toc_nentry;
        fence(Ordering::Acquire);

        // Now search for a matching entry.
        for i in 0..nentry {
            // SAFETY: i < nentry <= the number of entries written; the segment
            // outlives the handle.
            let entry = unsafe { self.entry_ptr(i as Size) };
            // SAFETY: `entry` points at a written ShmTocEntry.
            let entry = unsafe { &*entry };
            if entry.key == key {
                // SAFETY: the stored offset addresses a chunk within the
                // segment, strictly past the non-null TOC start.
                let ptr = unsafe { self.base.as_ptr().add(entry.offset) };
                return Ok(NonNull::new(ptr));
            }
        }

        // No matching entry was found.
        if !no_error {
            elog(
                ERROR,
                format!(
                    "could not find key {} in shm TOC at {:p}",
                    key,
                    self.base.as_ptr()
                ),
            )?;
        }
        Ok(None)
    }
}

/// RAII guard for the in-segment `toc_mutex` spinlock: `SpinLockAcquire` on
/// construction (in [`ShmToc::spin_lock`]), `SpinLockRelease` on drop.
struct TocSpinGuard<'a> {
    lock: &'a Spinlock,
}

impl Drop for TocSpinGuard<'_> {
    fn drop(&mut self) {
        // SpinLockRelease(&toc->toc_mutex).
        s_unlock(self.lock);
    }
}

/// `shm_toc_initialize_estimator(e)` (`shm_toc.h`).
pub fn shm_toc_initialize_estimator(e: &mut shm_toc_estimator) {
    e.space_for_chunks = 0;
    e.number_of_keys = 0;
}

/// `shm_toc_estimate_chunk(e, sz)` (`shm_toc.h`).
pub fn shm_toc_estimate_chunk(e: &mut shm_toc_estimator, sz: Size) -> PgResult<()> {
    e.space_for_chunks = add_size::call(e.space_for_chunks, BUFFERALIGN(sz))?;
    Ok(())
}

/// `shm_toc_estimate_keys(e, cnt)` (`shm_toc.h`).
pub fn shm_toc_estimate_keys(e: &mut shm_toc_estimator, cnt: Size) -> PgResult<()> {
    e.number_of_keys = add_size::call(e.number_of_keys, cnt)?;
    Ok(())
}

/// Estimate how much shared memory will be required to store a TOC and its
/// dependent data structures (`shm_toc_estimate`).
pub fn shm_toc_estimate(e: &shm_toc_estimator) -> PgResult<Size> {
    let mut sz = toc_entry_offset();
    sz = add_size::call(sz, mul_size::call(e.number_of_keys, size_of::<ShmTocEntry>())?)?;
    sz = add_size::call(sz, e.space_for_chunks)?;

    Ok(BUFFERALIGN(sz))
}

/// `shm_toc_estimate_chunk(&pcxt->estimator, nbytes)` over the owned
/// `ParallelContext` — the seam form the FDW/custom-scan parallel-estimate
/// entry points (`ExecForeignScanEstimate` / `ExecCustomScanEstimate`) call.
/// Delegates to [`shm_toc_estimate_chunk`] against the context's backend-local
/// `estimator`.
fn shm_toc_estimate_chunk_pcxt(
    pcxt: &mut nodes::ParallelContext,
    nbytes: usize,
) -> PgResult<()> {
    shm_toc_estimate_chunk(&mut pcxt.estimator, nbytes)
}

/// `shm_toc_estimate_keys(&pcxt->estimator, nkeys)` over the owned
/// `ParallelContext`; companion of [`shm_toc_estimate_chunk_pcxt`].
fn shm_toc_estimate_keys_pcxt(
    pcxt: &mut nodes::ParallelContext,
    nkeys: usize,
) -> PgResult<()> {
    shm_toc_estimate_keys(&mut pcxt.estimator, nkeys)
}

/// Installs the `&mut ParallelContext`-keyed estimate seams over the context's
/// real backend-local `estimator` field.
pub fn init_seams() {
    ipc_shm_toc_seams::shm_toc_estimate_chunk::set(shm_toc_estimate_chunk_pcxt);
    ipc_shm_toc_seams::shm_toc_estimate_keys::set(shm_toc_estimate_keys_pcxt);
}

#[cfg(test)]
mod tests;
