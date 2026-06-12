//! Seam declarations for the `backend-postmaster-bgwriter` unit
//! (`src/backend/postmaster/bgwriter.c`). The owning unit installs these from its `init_seams()`;
//! until then a call panics loudly.

seam_core::seam!(
    /// `BackgroundWriterMain(startup_data, startup_data_len)` (`src/backend/postmaster/bgwriter.c`): child entry
    /// point invoked by `postmaster_child_launch`; never returns.
    pub fn background_writer_main(startup_data: &types_startup::StartupData) -> !
);
