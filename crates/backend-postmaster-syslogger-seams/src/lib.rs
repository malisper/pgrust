//! Seam declarations for the `backend-postmaster-syslogger` unit
//! (`src/backend/postmaster/syslogger.c`). The owning unit installs these from its `init_seams()`;
//! until then a call panics loudly.

seam_core::seam!(
    /// `SysLoggerMain(startup_data, startup_data_len)` (`src/backend/postmaster/syslogger.c`): child entry
    /// point invoked by `postmaster_child_launch`; never returns.
    pub fn sys_logger_main(startup_data: &types_startup::StartupData) -> !
);
