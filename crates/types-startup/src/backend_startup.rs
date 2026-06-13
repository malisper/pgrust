//! Startup data passed from the postmaster to a child process
//! (`tcop/backend_startup.h` and the `postmaster_child_launch` interface in
//! `postmaster/launch_backend.c`).

use types_core::TimestampTz;

/// `CAC_state` (`tcop/backend_startup.h`) — passed from postmaster to the
/// backend process, to indicate whether the connection should be accepted, or
/// if the process should just send an error to the client and close the
/// connection. Note that the connection can fail for various reasons even if
/// postmaster passed `CAC_OK`. Discriminant order matches the C enum.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum CacState {
    /// `CAC_OK`.
    Ok = 0,
    /// `CAC_STARTUP`.
    Startup = 1,
    /// `CAC_SHUTDOWN`.
    Shutdown = 2,
    /// `CAC_RECOVERY`.
    Recovery = 3,
    /// `CAC_NOTHOTSTANDBY`.
    NotHotStandby = 4,
    /// `CAC_TOOMANY`.
    TooMany = 5,
}

/// `BackendStartupData` (`tcop/backend_startup.h`) — information passed from
/// postmaster to backend process in `startup_data`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackendStartupData {
    /// `CAC_state canAcceptConnections`.
    pub can_accept_connections: CacState,
    /// `TimestampTz socket_created` — time at which the connection client
    /// socket is created. Only used for client and wal sender connections.
    pub socket_created: TimestampTz,
    /// `TimestampTz fork_started` — time at which the postmaster initiates
    /// process creation. Only used for client and wal sender connections.
    pub fork_started: TimestampTz,
}

/// The typed currency behind C's `void *startup_data, size_t
/// startup_data_len` pair handed to `postmaster_child_launch` and forwarded
/// to the per-type `*Main` entry point, which immediately resolves it to a
/// concrete type.
///
/// Unix launch sites in C pass either NULL ([`StartupData::None`]; all
/// auxiliary processes and, on non-`EXEC_BACKEND` builds, the syslogger) or a
/// `BackendStartupData` ([`StartupData::Backend`]; `B_BACKEND` /
/// `B_DEAD_END_BACKEND`, `postmaster.c BackendStartup`). The remaining Unix
/// payload — `B_BG_WORKER`'s `BackgroundWorker` (`postmaster.c
/// do_start_bgworker`) — gains its variant when the bgworker/postmaster units
/// port; neither its producer nor its consumer exists in this repo yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartupData {
    /// C `startup_data == NULL, startup_data_len == 0`.
    None,
    /// A `BackendStartupData` (connection backends).
    Backend(BackendStartupData),
}
