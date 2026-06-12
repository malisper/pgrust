//! Seam declarations for the `backend-postmaster-checkpointer` unit
//! (`src/backend/postmaster/checkpointer.c`). The owning unit installs these from its `init_seams()`;
//! until then a call panics loudly.

seam_core::seam!(
    /// `CheckpointerMain(startup_data, startup_data_len)` (`src/backend/postmaster/checkpointer.c`): child entry
    /// point invoked by `postmaster_child_launch`; never returns.
    pub fn checkpointer_main(startup_data: &[u8]) -> !
);
