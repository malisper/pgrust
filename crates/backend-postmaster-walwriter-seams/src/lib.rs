//! Seam declarations for the `backend-postmaster-walwriter` unit
//! (`src/backend/postmaster/walwriter.c`). The owning unit installs these from its `init_seams()`;
//! until then a call panics loudly.

seam_core::seam!(
    /// `WalWriterMain(startup_data, startup_data_len)` (`src/backend/postmaster/walwriter.c`): child entry
    /// point invoked by `postmaster_child_launch`; never returns.
    pub fn wal_writer_main(startup_data: &[u8]) -> !
);
