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
    /// `MyStartTime` (globals.c): this process's start time (`pg_time_t`,
    /// seconds since the Unix epoch), set once at process start.
    pub fn my_start_time() -> types_core::pg_time_t
);

seam_core::seam!(
    /// Write `MyBackendType` (globals.c, declared in miscadmin.h): processes
    /// assign their own type at startup (e.g. `MyBackendType = B_LOGGER` in
    /// SysLoggerMain). Per-crate mirrors of this global (e.g. elog's
    /// `am_syslogger`) are updated by the assigning unit itself.
    pub fn set_my_backend_type(backend_type: types_core::init::BackendType)
);
