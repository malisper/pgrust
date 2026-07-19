#![no_std]

extern crate alloc;

use core::ptr;

use mcx::{Mcx, PgVec, MAX_ALLOC_SIZE};
use types_error::{PgError, PgResult, ERRCODE_PROGRAM_LIMIT_EXCEEDED};

pub const STRINGINFO_DEFAULT_SIZE: usize = 1024;

// Invariants (C stringinfo.h): capacity > len always, and data[len] == b'\0'
// except transiently after append_bytes_nt (as in C).
pub struct StringInfo<'mcx> {
    data: PgVec<'mcx, u8>,
    pub cursor: usize,
}

impl<'mcx> StringInfo<'mcx> {
    #[inline]
    pub fn new_in(mcx: Mcx<'mcx>) -> PgResult<Self> {
        Self::with_capacity_in(mcx, STRINGINFO_DEFAULT_SIZE)
    }

    pub fn with_capacity_in(mcx: Mcx<'mcx>, initsize: usize) -> PgResult<Self> {
        debug_assert!(initsize >= 1 && initsize <= MAX_ALLOC_SIZE);
        let mut data = PgVec::new_in(mcx);
        data.try_reserve_exact(initsize).map_err(|_| mcx.oom(initsize))?;
        // SAFETY: capacity >= initsize >= 1.
        unsafe { *data.as_mut_ptr() = 0 };
        Ok(StringInfo { data, cursor: 0 })
    }

    pub fn from_vec(mut data: PgVec<'mcx, u8>) -> PgResult<Self> {
        if data.capacity() == data.len() {
            let mcx = *data.allocator();
            data.try_reserve(1).map_err(|_| mcx.oom(data.len() + 1))?;
        }
        // SAFETY: capacity > len after the reserve above.
        unsafe { *data.as_mut_ptr().add(data.len()) = 0 };
        Ok(StringInfo { data, cursor: 0 })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    #[inline]
    pub fn allocator(&self) -> Mcx<'mcx> {
        *self.data.allocator()
    }

    #[inline]
    pub fn into_vec(self) -> PgVec<'mcx, u8> {
        self.data
    }

    #[inline]
    pub fn reset(&mut self) {
        debug_assert!(self.data.capacity() > 0);
        self.data.clear();
        // SAFETY: capacity >= 1 (construction invariant).
        unsafe { *self.data.as_mut_ptr() = 0 };
        self.cursor = 0;
    }

    #[inline]
    pub fn enlarge(&mut self, needed: usize) -> PgResult<()> {
        if self.data.capacity().wrapping_sub(self.data.len()) > needed {
            return Ok(());
        }
        self.enlarge_slow(needed)
    }

    #[cold]
    #[inline(never)]
    fn enlarge_slow(&mut self, needed: usize) -> PgResult<()> {
        let len = self.data.len();
        if needed >= MAX_ALLOC_SIZE.saturating_sub(len) {
            return Err(enlarge_error(len, needed));
        }
        let total = len + needed + 1;
        let mut newlen = 2 * self.data.capacity().max(1);
        while total > newlen {
            newlen *= 2;
        }
        if newlen > MAX_ALLOC_SIZE {
            newlen = MAX_ALLOC_SIZE;
        }
        let mcx = *self.data.allocator();
        self.data
            .try_reserve_exact(newlen - len)
            .map_err(|_| mcx.oom(newlen))?;
        // SAFETY: capacity >= newlen > len.
        unsafe { *self.data.as_mut_ptr().add(len) = 0 };
        Ok(())
    }

    #[inline]
    pub fn append_bytes(&mut self, bytes: &[u8]) -> PgResult<()> {
        let n = bytes.len();
        self.enlarge(n)?;
        let len = self.data.len();
        // SAFETY: capacity >= len + n + 1 after enlarge; `bytes` cannot alias
        // the spare capacity written here.
        unsafe {
            let dst = self.data.as_mut_ptr().add(len);
            ptr::copy_nonoverlapping(bytes.as_ptr(), dst, n);
            *dst.add(n) = 0;
            self.data.set_len(len + n);
        }
        Ok(())
    }

    #[inline]
    pub fn append_bytes_nt(&mut self, bytes: &[u8]) -> PgResult<()> {
        let n = bytes.len();
        self.enlarge(n)?;
        let len = self.data.len();
        // SAFETY: as append_bytes.
        unsafe {
            ptr::copy_nonoverlapping(bytes.as_ptr(), self.data.as_mut_ptr().add(len), n);
            self.data.set_len(len + n);
        }
        Ok(())
    }

    // appendBinaryStringInfoNT(buf, s, s.len() + 1): the NUL is counted in len.
    #[inline]
    pub fn append_bytes_z(&mut self, bytes: &[u8]) -> PgResult<()> {
        let n = bytes.len();
        self.enlarge(n + 1)?;
        let len = self.data.len();
        // SAFETY: capacity >= len + n + 2 after enlarge; `bytes` cannot alias
        // the spare capacity written here.
        unsafe {
            let dst = self.data.as_mut_ptr().add(len);
            ptr::copy_nonoverlapping(bytes.as_ptr(), dst, n);
            *dst.add(n) = 0;
            self.data.set_len(len + n + 1);
        }
        Ok(())
    }

    // pq_writeintN shape: space preallocated by an earlier enlarge (C asserts
    // len + N <= maxlen and skips the capacity check in ship builds).
    #[inline]
    pub fn write_fixed<const N: usize>(&mut self, bytes: [u8; N]) {
        let len = self.data.len();
        assert!(N <= self.data.capacity() - len);
        // SAFETY: capacity >= len + N per the check above.
        unsafe {
            let dst = self.data.as_mut_ptr().add(len);
            ptr::copy_nonoverlapping(bytes.as_ptr(), dst, N);
            self.data.set_len(len + N);
        }
    }

    #[inline]
    pub fn mcx(&self) -> Mcx<'mcx> {
        *self.data.allocator()
    }

    pub fn append_str(&mut self, s: &str) -> PgResult<()> {
        self.append_bytes(s.as_bytes())
    }

    // C's `buf->len = n; buf->data[n] = '\0'` roll-back idiom (rmgrdesc).
    #[inline]
    pub fn truncate(&mut self, new_len: usize) {
        if new_len < self.data.len() {
            // SAFETY: new_len < len <= capacity; slot new_len is allocated.
            unsafe {
                self.data.set_len(new_len);
                *self.data.as_mut_ptr().add(new_len) = 0;
            }
        }
    }

    #[inline]
    pub fn append_byte(&mut self, ch: u8) -> PgResult<()> {
        if self.data.capacity().wrapping_sub(self.data.len()) < 2 {
            self.enlarge_slow(1)?;
        }
        let len = self.data.len();
        // SAFETY: capacity >= len + 2.
        unsafe {
            let dst = self.data.as_mut_ptr().add(len);
            *dst = ch;
            *dst.add(1) = 0;
            self.data.set_len(len + 1);
        }
        Ok(())
    }

    pub fn append_spaces(&mut self, count: usize) -> PgResult<()> {
        if count > 0 {
            self.enlarge(count)?;
            let len = self.data.len();
            // SAFETY: capacity >= len + count + 1 after enlarge.
            unsafe {
                let dst = self.data.as_mut_ptr().add(len);
                ptr::write_bytes(dst, b' ', count);
                *dst.add(count) = 0;
                self.data.set_len(len + count);
            }
        }
        Ok(())
    }
}

#[cold]
#[inline(never)]
fn enlarge_error(len: usize, needed: usize) -> alloc::boxed::Box<PgError> {
    PgError::error(alloc::format!(
        "string buffer exceeds maximum allowed length ({MAX_ALLOC_SIZE} bytes)"
    ))
    .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
    .with_detail(alloc::format!(
        "Cannot enlarge string buffer containing {len} bytes by {needed} more bytes."
    ))
    .into()
}

#[cfg(test)]
mod tests;
