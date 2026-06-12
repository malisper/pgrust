//! Seam declarations for the `backend-storage-aio-methods` unit
//! (`src/backend/storage/aio/method_worker.c`). The owning unit installs these from its `init_seams()`;
//! until then a call panics loudly.

seam_core::seam!(
    /// `IoWorkerMain(startup_data, startup_data_len)` (`src/backend/storage/aio/method_worker.c`): child entry
    /// point invoked by `postmaster_child_launch`; never returns.
    pub fn io_worker_main(startup_data: &types_startup::StartupData) -> !
);
