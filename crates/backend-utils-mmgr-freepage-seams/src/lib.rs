//! Seam declarations for the `backend-utils-mmgr-freepage` unit
//! (`utils/mmgr/freepage.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! A `FreePageManager` lives in shared memory and its layout belongs to the
//! owner, so it is carried here as a raw byte pointer.

seam_core::seam!(
    /// `FreePageManagerInitialize(FreePageManager *fpm, char *base)` —
    /// initialize a new, empty free page manager whose relative pointers are
    /// based at `base`.
    pub fn free_page_manager_initialize(fpm: *mut u8, base: *mut u8)
);

seam_core::seam!(
    /// `FreePageManagerGet(FreePageManager *fpm, Size npages, Size *first_page)`
    /// — allocate a run of `npages` consecutive pages; `Some(first_page)` on
    /// success, `None` when no sufficient run exists.
    pub fn free_page_manager_get(fpm: *mut u8, npages: usize) -> Option<usize>
);

seam_core::seam!(
    /// `FreePageManagerPut(FreePageManager *fpm, Size first_page, Size npages)`
    /// — return a run of pages to the free page map.
    pub fn free_page_manager_put(fpm: *mut u8, first_page: usize, npages: usize)
);

seam_core::seam!(
    /// `sizeof(FreePageManager)` (`utils/freepage.h`) — the layout is owned by
    /// `freepage.c`; consumers (e.g. `dsm_shmem_init`) need only its size to
    /// reserve space ahead of the managed pages.
    pub fn free_page_manager_size() -> usize
);
