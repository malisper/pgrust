//! Resource guards for the buffer pin and content lock (docs/no-drop.md:
//! guards live in owner structures, `Drop` is the abort path). Page bytes are
//! reachable only through [`BufferPin::page`], so every page access is a
//! pin-scoped borrow.

use types_core::{BlockNumber, Buffer, InvalidBuffer};
use types_error::PgResult;
use types_storage::bufpage::PageRef;

use crate::{
    buffer_get_block_number, buffer_get_page, incr_buffer_ref_count, lock_buffer, release_buffer,
    BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_SHARE, BUFFER_LOCK_UNLOCK,
};

/// One pin reference on a shared buffer (C pairs ReadBuffer/ReleaseBuffer by
/// convention; the guard pairs them by construction).
pub struct BufferPin {
    buffer: Buffer,
}

impl BufferPin {
    /// Adopt ownership of one already-taken pin reference (the ReadBuffer
    /// return); `InvalidBuffer` maps to `None`.
    #[inline]
    pub fn adopt(buffer: Buffer) -> Option<BufferPin> {
        if buffer == InvalidBuffer {
            None
        } else {
            Some(BufferPin { buffer })
        }
    }

    #[inline]
    pub fn buffer(&self) -> Buffer {
        self.buffer
    }

    #[inline]
    pub fn block_number(&self) -> BlockNumber {
        buffer_get_block_number::call(self.buffer)
    }

    /// `BufferGetPage`: the borrow is scoped to this pin.
    #[inline]
    pub fn page(&self) -> PageRef<'_> {
        // SAFETY: the pin held for the returned borrow's lifetime keeps the
        // page image live; locking discipline is the caller's C contract.
        unsafe { PageRef::from_raw(buffer_get_page::call(self.buffer)) }
    }

    /// `IncrBufferRefCount`: a second owned pin reference on the same buffer.
    #[inline]
    pub fn incr_clone(&self) -> BufferPin {
        incr_buffer_ref_count::call(self.buffer);
        BufferPin { buffer: self.buffer }
    }

    /// Explicit `ReleaseBuffer` (the ordered-teardown path).
    #[inline]
    pub fn release(self) {
        let _ = release_buffer::call(self.buffer);
        core::mem::forget(self);
    }

    /// `LockBuffer(buf, BUFFER_LOCK_SHARE)`.
    #[inline]
    pub fn lock_share(&self) -> PgResult<ContentLockGuard<'_>> {
        lock_buffer::call(self.buffer, BUFFER_LOCK_SHARE)?;
        Ok(ContentLockGuard { pin: self })
    }

    /// `LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE)`.
    #[inline]
    pub fn lock_exclusive(&self) -> PgResult<ContentLockGuard<'_>> {
        lock_buffer::call(self.buffer, BUFFER_LOCK_EXCLUSIVE)?;
        Ok(ContentLockGuard { pin: self })
    }
}

impl Drop for BufferPin {
    fn drop(&mut self) {
        let _ = release_buffer::call(self.buffer);
    }
}

impl core::fmt::Debug for BufferPin {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "BufferPin({})", self.buffer)
    }
}

/// Buffer content lock held for the guard's lifetime; borrowing the pin makes
/// releasing the pin while locked a compile error.
pub struct ContentLockGuard<'p> {
    pin: &'p BufferPin,
}

impl<'p> ContentLockGuard<'p> {
    #[inline]
    pub fn page(&self) -> PageRef<'_> {
        self.pin.page()
    }

    /// Explicit `LockBuffer(buf, BUFFER_LOCK_UNLOCK)`.
    #[inline]
    pub fn unlock(self) {
        drop(self);
    }
}

impl Drop for ContentLockGuard<'_> {
    fn drop(&mut self) {
        // C's LockBuffer(UNLOCK) has no error surface; the seam's PgResult
        // exists for lock-mode dispatch. Unlock cannot fail.
        let _ = lock_buffer::call(self.pin.buffer, BUFFER_LOCK_UNLOCK);
    }
}
