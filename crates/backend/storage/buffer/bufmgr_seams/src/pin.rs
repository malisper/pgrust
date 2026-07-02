//! Buffer pin + content lock guards (docs/no-drop.md: Drop is the abort
//! path); page bytes are reachable only through pin-scoped borrows.

use types_core::{BlockNumber, Buffer, InvalidBuffer};
use types_error::PgResult;
use types_storage::bufpage::PageRef;

use crate::{
    buffer_get_block_number, buffer_get_page, incr_buffer_ref_count, lock_buffer, release_buffer,
    BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_SHARE, BUFFER_LOCK_UNLOCK,
};

pub struct BufferPin {
    buffer: Buffer,
}

impl BufferPin {
    /// Adopt one already-taken pin reference; `InvalidBuffer` maps to `None`.
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

    #[inline]
    pub fn page(&self) -> PageRef<'_> {
        // SAFETY: the pin held for the borrow keeps the page live; locking is the caller's C contract.
        unsafe { PageRef::from_raw(buffer_get_page::call(self.buffer)) }
    }

    #[inline]
    pub fn incr_clone(&self) -> BufferPin {
        incr_buffer_ref_count::call(self.buffer);
        BufferPin { buffer: self.buffer }
    }

    #[inline]
    pub fn release(self) {
        let _ = release_buffer::call(self.buffer);
        core::mem::forget(self);
    }

    #[inline]
    pub fn lock_share(&self) -> PgResult<ContentLockGuard<'_>> {
        lock_buffer::call(self.buffer, BUFFER_LOCK_SHARE)?;
        Ok(ContentLockGuard { pin: self })
    }

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

// Borrowing the pin makes release-while-locked a compile error.
pub struct ContentLockGuard<'p> {
    pin: &'p BufferPin,
}

impl<'p> ContentLockGuard<'p> {
    #[inline]
    pub fn page(&self) -> PageRef<'_> {
        self.pin.page()
    }

    #[inline]
    pub fn unlock(self) {
        drop(self);
    }
}

impl Drop for ContentLockGuard<'_> {
    fn drop(&mut self) {
        let _ = lock_buffer::call(self.pin.buffer, BUFFER_LOCK_UNLOCK);
    }
}
