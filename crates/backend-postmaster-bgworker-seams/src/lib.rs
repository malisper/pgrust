//! Seam declarations for the `backend-postmaster-bgworker` unit
//! (`src/backend/postmaster/bgworker.c`). The owning unit installs these from its
//! `init_seams()`; until then a call panics loudly.

seam_core::seam!(
    /// `BackgroundWorkerMain(startup_data, startup_data_len)` (`src/backend/postmaster/bgworker.c`): child entry
    /// point invoked by `postmaster_child_launch`; never returns.
    pub fn background_worker_main(startup_data: &types_startup::StartupData) -> !
);

seam_core::seam!(
    /// `GetBackgroundWorkerPid(handle, &pid)` (`postmaster/bgworker.c`) —
    /// report the state of the worker named by `handle`; the returned pid is
    /// meaningful only when the status is `Started` (C writes `*pidp` for
    /// `Started`/`Stopped` only). Infallible in C (slot read under the slot
    /// spinlock; no ereport path).
    pub fn get_background_worker_pid(
        handle: types_bgworker::BackgroundWorkerHandle,
    ) -> (types_bgworker::BgwHandleStatus, i32)
);

seam_core::seam!(
    /// `RegisterBackgroundWorker(BackgroundWorker *worker)` (bgworker.c):
    /// register a static (postmaster-start) background worker. Only callable
    /// before shared memory is initialized; can `ereport` on misuse, carried
    /// on `Err`.
    pub fn register_background_worker(
        worker: &types_bgworker::BackgroundWorker,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `RegisterDynamicBackgroundWorker(BackgroundWorker *worker,
    /// BackgroundWorkerHandle **handle)` (bgworker.c): register a dynamic
    /// background worker at runtime. Returns `Some(handle)` on success and
    /// `None` (C `false`, out of bgworker slots) on failure; the handle is used
    /// to poll startup.
    pub fn register_dynamic_background_worker(
        worker: &types_bgworker::BackgroundWorker,
    ) -> types_error::PgResult<Option<types_bgworker::BackgroundWorkerHandle>>
);

seam_core::seam!(
    /// `BackgroundWorkerInitializeConnection(const char *dbname,
    /// const char *username, uint32 flags)` (bgworker.c): establish this
    /// bgworker's connection to a database (the launcher passes NULL/NULL to
    /// connect to nailed catalogs only). Can `ereport(FATAL)`, carried on
    /// `Err`.
    pub fn background_worker_initialize_connection(
        dbname: Option<&str>,
        username: Option<&str>,
        flags: u32,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `BackgroundWorkerUnblockSignals()` (bgworker.c): unblock signals in a
    /// background worker after handlers are installed.
    pub fn background_worker_unblock_signals()
);
