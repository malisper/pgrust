//! Seam declarations for the `backend-postmaster-startup` unit
//! (`src/backend/postmaster/startup.c`). The owning unit installs these from its `init_seams()`;
//! until then a call panics loudly.

seam_core::seam!(
    /// `StartupProcessMain(startup_data, startup_data_len)` (`src/backend/postmaster/startup.c`): child entry
    /// point invoked by `postmaster_child_launch`; never returns.
    pub fn startup_process_main(startup_data: &[u8]) -> !
);
