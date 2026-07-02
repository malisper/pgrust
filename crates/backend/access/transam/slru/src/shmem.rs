// Raw views into one `ShmemInitStruct` block (C's SlruSharedData arrays).
// The slru.c lock protocol is the synchronization, exactly as in C.
pub(crate) struct ShmemVals<T: Copy> {
    ptr: *mut T,
    len: usize,
}

unsafe impl<T: Copy + Send> Send for ShmemVals<T> {}
unsafe impl<T: Copy + Send> Sync for ShmemVals<T> {}

impl<T: Copy> ShmemVals<T> {
    /// # Safety
    /// `ptr` must be aligned and point at `len` `T`s live for the cluster
    /// lifetime.
    pub(crate) unsafe fn from_raw(ptr: *mut T, len: usize) -> Self {
        Self { ptr, len }
    }

    #[inline]
    pub(crate) fn get(&self, i: usize) -> T {
        assert!(i < self.len);
        // SAFETY: in-bounds; callers hold the covering LWLock (both accessors).
        unsafe { self.ptr.add(i).read() }
    }

    #[inline]
    pub(crate) fn set(&self, i: usize, v: T) {
        assert!(i < self.len);
        unsafe { self.ptr.add(i).write(v) }
    }
}

pub(crate) struct ShmemPages {
    ptr: *mut u8,
    nslots: usize,
}

unsafe impl Send for ShmemPages {}
unsafe impl Sync for ShmemPages {}

impl ShmemPages {
    /// # Safety
    /// `ptr` must point at `nslots * BLCKSZ` live bytes.
    pub(crate) unsafe fn from_raw(ptr: *mut u8, nslots: usize) -> Self {
        Self { ptr, nslots }
    }

    #[inline]
    pub(crate) fn page_ptr(&self, slotno: usize) -> *mut u8 {
        assert!(slotno < self.nslots);
        // SAFETY: in-bounds by the assert.
        unsafe { self.ptr.add(slotno * ::types_core::BLCKSZ) }
    }
}

/// # Safety
/// `ptr` must be aligned at `len` initialized `T`s live for the cluster
/// lifetime; `T` must tolerate shared access (atomics, LWLocks).
pub(crate) unsafe fn shared_slice<'a, T>(ptr: *mut T, len: usize) -> &'a [T] {
    if len == 0 {
        &[]
    } else {
        core::slice::from_raw_parts(ptr, len)
    }
}
