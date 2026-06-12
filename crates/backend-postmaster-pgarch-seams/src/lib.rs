//! Seam declarations for the `backend-postmaster-pgarch` unit
//! (`src/backend/postmaster/pgarch.c`). The owning unit installs these from its `init_seams()`;
//! until then a call panics loudly.

seam_core::seam!(
    /// `PgArchiverMain(startup_data, startup_data_len)` (`src/backend/postmaster/pgarch.c`): child entry
    /// point invoked by `postmaster_child_launch`; never returns.
    pub fn pg_archiver_main(startup_data: &types_startup::StartupData) -> !
);
