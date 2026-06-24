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

seam_core::seam!(
    /// `conn_timing.auth_start = tstamp` (the `conn_timing` global owned by
    /// `backend_startup.c`): record the authentication start timestamp.
    /// Set by `postinit.c`'s `PerformAuthentication`.
    pub fn set_conn_timing_auth_start(tstamp: types_core::TimestampTz)
);

seam_core::seam!(
    /// `conn_timing.auth_end = tstamp` (the `conn_timing` global owned by
    /// `backend_startup.c`). Set by `postinit.c`'s `PerformAuthentication`.
    pub fn set_conn_timing_auth_end(tstamp: types_core::TimestampTz)
);

seam_core::seam!(
    /// The first-ready connection-setup-durations LOG line (`postgres.c`
    /// `PostgresMain`, the `conn_timing.ready_for_use == TIMESTAMP_MINUS_INFINITY
    /// && (log_connections & LOG_CONNECTION_SETUP_DURATIONS) &&
    /// IsExternalConnectionBackend(MyBackendType)` block). Owned by
    /// `backend_startup.c` because it reads/writes the `conn_timing` global and
    /// the `log_connections` aspect mask; called once from the main loop just
    /// before `ReadyForQuery`.
    pub fn log_connection_ready()
);
