//! Seam declarations for the `backend-utils-mmgr-freepage` unit
//! (`utils/mmgr/freepage.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! A `FreePageManager` lives in shared memory (its real layout is
//! `types_freepage::FreePageManager`), so it crosses the seam as a raw
//! pointer, exactly as the C functions take `FreePageManager *`.

use types_freepage::FreePageManager;

seam_core::seam!(
    /// `FreePageManagerInitialize(FreePageManager *fpm, char *base)` —
    /// initialize a new, empty free page manager whose relative pointers are
    /// based at `base`.
    pub fn free_page_manager_initialize(fpm: *mut FreePageManager, base: *mut u8)
);

seam_core::seam!(
    /// `FreePageManagerGet(FreePageManager *fpm, Size npages, Size *first_page)`
    /// — allocate a run of `npages` consecutive pages; `Some(first_page)` on
    /// success, `None` when no sufficient run exists.
    pub fn free_page_manager_get(fpm: *mut FreePageManager, npages: usize) -> Option<usize>
);

seam_core::seam!(
    /// `FreePageManagerPut(FreePageManager *fpm, Size first_page, Size npages)`
    /// — return a run of pages to the free page map.
    pub fn free_page_manager_put(fpm: *mut FreePageManager, first_page: usize, npages: usize)
);
