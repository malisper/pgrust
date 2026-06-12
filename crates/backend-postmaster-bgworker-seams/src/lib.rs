//! Seam declarations for the `backend-postmaster-bgworker` unit
//! (`src/backend/postmaster/bgworker.c`). The owning unit installs these from its `init_seams()`;
//! until then a call panics loudly.

seam_core::seam!(
    /// `BackgroundWorkerMain(startup_data, startup_data_len)` (`src/backend/postmaster/bgworker.c`): child entry
    /// point invoked by `postmaster_child_launch`; never returns.
    pub fn background_worker_main(startup_data: &[u8]) -> !
);
