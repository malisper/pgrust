//! Postmaster-owned GUC value reads.
//!
//! In C these are direct reads of the GUC globals declared in `postmaster.c`
//! (and a few sibling subsystem globals the postmaster also reads): `EnableSSL`,
//! `ListenAddresses`, `PostPortNumber`, `MaxConnections`,
//! `Unix_socket_directories`, `Logging_collector`, the crash-restart / abort
//! policy bools, etc. The postmaster reads them straight from the variable
//! storage — `*conf->variable` — through the `guc_tables` typed variable slots
//! (`::guc_tables::vars`), which the GUC machinery keeps in
//! sync with `postgresql.conf` / command-line settings.
//!
//! `backend-postmaster-postmaster-seams` declares each of these as a caller-side
//! read seam (so other units that need a postmaster GUC value get a stable seam
//! rather than a direct cross-crate read); [`crate::init_seams`] installs them
//! here. Each is a pure read of the live GUC value, faithful to the C global
//! dereference.

use ::guc_tables::vars;

/// `EnableSSL` (postmaster.c GUC `ssl`).
pub fn enable_ssl() -> bool {
    vars::EnableSSL.read()
}

/// `Logging_collector` (postmaster.c GUC `logging_collector`).
pub fn logging_collector() -> bool {
    vars::Logging_collector.read()
}

/// `restart_after_crash` (postmaster.c GUC).
pub fn restart_after_crash() -> bool {
    vars::restart_after_crash.read()
}

/// `remove_temp_files_after_crash` (postmaster.c GUC).
pub fn remove_temp_files_after_crash() -> bool {
    vars::remove_temp_files_after_crash.read()
}

/// `send_abort_for_crash` (postmaster.c GUC).
pub fn send_abort_for_crash() -> bool {
    vars::send_abort_for_crash.read()
}

/// `send_abort_for_kill` (postmaster.c GUC).
pub fn send_abort_for_kill() -> bool {
    vars::send_abort_for_kill.read()
}

/// `log_hostname` (postmaster.c GUC).
pub fn log_hostname() -> bool {
    vars::log_hostname.read()
}

/// `summarize_wal` (GUC).
pub fn summarize_wal() -> bool {
    vars::summarize_wal.read()
}

/// `EnableHotStandby` (GUC `hot_standby`).
pub fn enable_hot_standby() -> bool {
    vars::EnableHotStandby.read()
}

/// `sync_replication_slots` (GUC).
pub fn sync_replication_slots() -> bool {
    vars::sync_replication_slots.read()
}

/// `PostPortNumber` (postmaster.c GUC `port`). C stores it as `int`; the listen
/// path treats it as the TCP/Unix port number.
pub fn post_port_number() -> u16 {
    vars::PostPortNumber.read() as u16
}

/// `MaxConnections` (postmaster.c GUC `max_connections`).
pub fn max_connections() -> i32 {
    vars::MaxConnections.read()
}

/// `AuthenticationTimeout` (postmaster.c GUC `authentication_timeout`).
pub fn authentication_timeout() -> i32 {
    vars::AuthenticationTimeout.read()
}

/// `PreAuthDelay` (postmaster.c GUC `pre_auth_delay`).
pub fn pre_auth_delay() -> i32 {
    vars::PreAuthDelay.read()
}

/// `io_workers` (GUC) — the configured number of IO worker processes.
pub fn io_workers() -> i32 {
    vars::io_workers.read()
}

/// `ListenAddresses` (postmaster.c GUC `listen_addresses`). `None` for a NULL
/// `char *` (unset).
pub fn listen_addresses() -> Option<String> {
    vars::ListenAddresses.read()
}

/// `Unix_socket_directories` (postmaster.c GUC `unix_socket_directories`).
pub fn unix_socket_directories() -> Option<String> {
    vars::Unix_socket_directories.read()
}
