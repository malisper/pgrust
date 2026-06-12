//! Seam declarations for the `backend-tcop-backend-startup` unit
//! (`src/backend/tcop/backend_startup.c`) — the `BackendMain` entry point,
//! plus accessors for the `BackendStartupData` startup-blob layout and the
//! `conn_timing` global that `tcop/backend_startup.h` owns. The owning unit
//! installs these from its `init_seams()`; until then a call panics loudly.

seam_core::seam!(
    /// `BackendMain(startup_data, startup_data_len)` (`backend_startup.c`):
    /// entry point for regular (and dead-end) backends invoked by
    /// `postmaster_child_launch`; never returns.
    pub fn backend_main(startup_data: &[u8]) -> !
);

seam_core::seam!(
    /// `((BackendStartupData *) startup_data)->fork_started = fork_started`:
    /// record the time at which the postmaster initiated process creation in
    /// a connection backend's startup blob.
    pub fn set_backend_startup_data_fork_started(
        startup_data: &mut [u8],
        fork_started: types_core::TimestampTz,
    )
);

seam_core::seam!(
    /// Read `(socket_created, fork_started)` from a connection backend's
    /// `BackendStartupData` startup blob.
    pub fn backend_startup_data_timings(
        startup_data: &[u8],
    ) -> (types_core::TimestampTz, types_core::TimestampTz)
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
