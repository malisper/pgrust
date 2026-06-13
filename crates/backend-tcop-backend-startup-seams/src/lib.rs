//! Seam declarations for the `backend-tcop-backend-startup` unit
//! (`src/backend/tcop/backend_startup.c`) — the `BackendMain` entry point and
//! the `conn_timing` global that `tcop/backend_startup.h` owns. The owning
//! unit installs these from its `init_seams()`; until then a call panics
//! loudly.

seam_core::seam!(
    /// `BackendMain(startup_data, startup_data_len)` (`backend_startup.c`):
    /// entry point for regular (and dead-end) backends invoked by
    /// `postmaster_child_launch`; never returns. The payload is
    /// `StartupData::Backend`.
    pub fn backend_main(startup_data: &types_startup::StartupData) -> !
);

seam_core::seam!(
    /// In the freshly forked child, transfer launch timings into the
    /// `conn_timing` global (`backend_startup.c`): `conn_timing.socket_create`,
    /// `.fork_start`, and `.fork_end`.
    pub fn set_conn_timing_child(
        socket_create: types_core::TimestampTz,
        fork_start: types_core::TimestampTz,
        fork_end: types_core::TimestampTz,
    )
);

// --- backend-utils-init-postinit consumer (backend_startup.c) ---

seam_core::seam!(
    /// `MyCancelKey` / `MyCancelKeyLength` (backend_startup.c globals): the
    /// backend's cancel key bytes, copied into `mcx`. Passed to
    /// `ProcSignalInit`. `Err` carries OOM from the copy.
    pub fn my_cancel_key<'mcx>(
        mcx: mcx::Mcx<'mcx>,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, u8>>
);
