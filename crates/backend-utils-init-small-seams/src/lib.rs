//! Seam declarations for the `backend-utils-init-small` unit
//! (`utils/init/globals.c`, `utils/init/usercontext.c`): backend-global
//! variable reads.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `work_mem` (globals.c): the `work_mem` GUC — per-operation memory
    /// budget in kilobytes.
    pub fn work_mem() -> i32
);

seam_core::seam!(
    /// Read `IsUnderPostmaster` (globals.c): true when this process was
    /// forked by a running postmaster.
    pub fn is_under_postmaster() -> bool
);

seam_core::seam!(
    /// Write `MyBackendType` (globals.c / miscadmin.h): record what kind of
    /// process this backend is (e.g. `B_STARTUP`).
    pub fn set_my_backend_type(backend_type: types_miscadmin::BackendType)
);
